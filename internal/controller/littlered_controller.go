/*
Copyright 2026 The littlered Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

const (
	finalizerName      = "redis.chuck-chuck-chuck.net/finalizer"
	fieldManager       = "littlered-operator"
	reasonPodsNotReady = "PodsNotReady"
)

// Logging categories
const (
	LogCategoryRecon = "recon" // Steady-state reconciliation
	LogCategoryState = "state" // Observations about cluster state
	LogCategoryAudit = "audit" // Cluster interference actions
)

type SentinelErrorCode int

const (
	SentinelUnreachable SentinelErrorCode = iota
	SentinelNoMaster
	SentinelGhostMaster
)

type SentinelError struct {
	Code    SentinelErrorCode
	Message string
	IP      string
	Err     error
}

func (e *SentinelError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// LittleRedReconciler reconciles a LittleRed object
type LittleRedReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// Sentinel monitoring
	sentinelEvents chan event.GenericEvent
	monitors       map[types.NamespacedName]func()
	monitorsMu     sync.Mutex
}

// event emits a Kubernetes Event for lr, tolerating a nil Recorder (e.g. in unit
// tests that construct the reconciler directly). Events are the operator's only
// human-facing "something notable just happened" channel — used for the destructive
// leaderless rebootstrap and the refuse-and-wait state.
func (r *LittleRedReconciler) event(lr *littleredv1alpha1.LittleRed, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}
	// events/v1 API: (regarding, related, eventtype, reason, action, note, args).
	// We have no distinct "related" object, and reuse reason as the machine-readable
	// action. message is passed as a literal note (%s) so a stray % never formats.
	r.Recorder.Eventf(lr, nil, eventType, reason, reason, "%s", message)
}

// +kubebuilder:rbac:groups=redis.chuck-chuck-chuck.net,resources=littlereds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=redis.chuck-chuck-chuck.net,resources=littlereds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=redis.chuck-chuck-chuck.net,resources=littlereds/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is the main reconciliation loop
func (r *LittleRedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Fetch the LittleRed instance
	littleRed := &littleredv1alpha1.LittleRed{}
	if err := r.Get(ctx, req.NamespacedName, littleRed); err != nil {
		if apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Info("LittleRed resource not found, ignoring", "category", LogCategoryRecon)
			return ctrl.Result{}, nil
		}
		logf.FromContext(ctx).Error(err, "Failed to get LittleRed", "category", LogCategoryRecon)
		return ctrl.Result{}, err
	}

	log := r.getLogger(ctx, littleRed, LogCategoryRecon)

	// Apply defaults
	littleRed.SetDefaults()

	// Validate supported constraints for initial release
	if err := littleRed.Validate(); err != nil {
		return r.setFailedStatus(ctx, littleRed, "UnsupportedConfiguration", err.Error())
	}

	// Handle deletion
	if !littleRed.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, littleRed)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(littleRed, finalizerName) {
		controllerutil.AddFinalizer(littleRed, finalizerName)
		if err := r.Update(ctx, littleRed); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Validate spec
	if err := r.validateSpec(ctx, littleRed); err != nil {
		return r.setFailedStatus(ctx, littleRed, "ValidationFailed", err.Error())
	}

	// Set ConfigValid condition
	meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionConfigValid,
		Status:             metav1.ConditionTrue,
		Reason:             "ConfigurationValid",
		Message:            "Configuration validated successfully",
		LastTransitionTime: metav1.Now(),
	})

	// Reconcile based on mode
	if littleRed.Spec.Mode != ModeSentinel {
		r.stopSentinelMonitor(req.NamespacedName)
	}

	// Initialize BootstrapRequired for Sentinel mode
	if littleRed.Spec.Mode == ModeSentinel && littleRed.Status.Phase == "" && !littleRed.Status.BootstrapRequired {
		log.Info("Initializing new Sentinel cluster: setting bootstrapRequired flag")
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &littleredv1alpha1.LittleRed{}
			if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
				return err
			}
			if latest.Status.Phase != "" || latest.Status.BootstrapRequired {
				return nil // Already initialized by another pass
			}
			latest.Status.BootstrapRequired = true
			return r.Status().Update(ctx, latest)
		})
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to initialize bootstrap flag: %w", err)
		}
		// Re-fetch to continue with the updated object
		if err := r.Get(ctx, req.NamespacedName, littleRed); err != nil {
			return ctrl.Result{}, err
		}
	}

	switch littleRed.Spec.Mode {
	case ModeStandalone:
		return r.reconcileStandalone(ctx, littleRed)
	case ModeSentinel:
		return r.reconcileSentinel(ctx, littleRed)
	case ModeCluster:
		return r.reconcileCluster(ctx, littleRed)
	default:
		return r.setFailedStatus(ctx, littleRed, "InvalidMode", fmt.Sprintf("Unknown mode: %s", littleRed.Spec.Mode))
	}
}

// getLogger returns a logger with standard fields and stripped redundancies
func (r *LittleRedReconciler) getLogger(ctx context.Context, lr *littleredv1alpha1.LittleRed, category string) logr.Logger {
	log := logf.FromContext(ctx).
		WithValues(
			"category", category,
		)

	// Note: name and namespace are already included in context by controller-runtime
	// as top-level fields. We add 'category' to enable filtering.

	return log
}

// reconcileDelete handles cleanup when the resource is deleted
func (r *LittleRedReconciler) reconcileDelete(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	log.Info("Reconciling delete")

	// Update phase
	littleRed.Status.Phase = littleredv1alpha1.PhaseTerminating
	if err := r.Status().Update(ctx, littleRed); err != nil {
		if !apierrors.IsConflict(err) {
			return ctrl.Result{}, err
		}
	}

	// Remove finalizer (owned resources will be garbage collected)
	controllerutil.RemoveFinalizer(littleRed, finalizerName)
	if err := r.Update(ctx, littleRed); err != nil {
		return ctrl.Result{}, err
	}

	// Stop sentinel monitor if running
	r.stopSentinelMonitor(types.NamespacedName{
		Name:      littleRed.Name,
		Namespace: littleRed.Namespace,
	})

	return ctrl.Result{}, nil
}

// validateSpec validates the LittleRed spec
func (r *LittleRedReconciler) validateSpec(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	// Validate auth
	if littleRed.Spec.Auth.Enabled {
		if littleRed.Spec.Auth.ExistingSecret == "" {
			return fmt.Errorf("auth.enabled is true but auth.existingSecret is not set")
		}
		// Verify secret exists
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      littleRed.Spec.Auth.ExistingSecret,
			Namespace: littleRed.Namespace,
		}, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("auth secret %q not found", littleRed.Spec.Auth.ExistingSecret)
			}
			return err
		}
		if _, ok := secret.Data["password"]; !ok {
			return fmt.Errorf("auth secret %q does not contain 'password' key", littleRed.Spec.Auth.ExistingSecret)
		}
	}

	// Validate TLS
	if littleRed.Spec.TLS.Enabled {
		if littleRed.Spec.TLS.ExistingSecret == "" {
			return fmt.Errorf("tls.enabled is true but tls.existingSecret is not set")
		}
		// Verify TLS secret exists
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      littleRed.Spec.TLS.ExistingSecret,
			Namespace: littleRed.Namespace,
		}, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("TLS secret %q not found", littleRed.Spec.TLS.ExistingSecret)
			}
			return err
		}
		if _, ok := secret.Data["tls.crt"]; !ok {
			return fmt.Errorf("TLS secret %q does not contain 'tls.crt' key", littleRed.Spec.TLS.ExistingSecret)
		}
		if _, ok := secret.Data["tls.key"]; !ok {
			return fmt.Errorf("TLS secret %q does not contain 'tls.key' key", littleRed.Spec.TLS.ExistingSecret)
		}

		// Validate client auth
		if littleRed.Spec.TLS.ClientAuth {
			if littleRed.Spec.TLS.CACertSecret == "" {
				return fmt.Errorf("tls.clientAuth is true but tls.caCertSecret is not set")
			}
			caSecret := &corev1.Secret{}
			if err := r.Get(ctx, types.NamespacedName{
				Name:      littleRed.Spec.TLS.CACertSecret,
				Namespace: littleRed.Namespace,
			}, caSecret); err != nil {
				if apierrors.IsNotFound(err) {
					return fmt.Errorf("CA certificate secret %q not found", littleRed.Spec.TLS.CACertSecret)
				}
				return err
			}
			if _, ok := caSecret.Data["ca.crt"]; !ok {
				return fmt.Errorf("CA certificate secret %q does not contain 'ca.crt' key", littleRed.Spec.TLS.CACertSecret)
			}
		}
	}

	// Validate cluster config
	if littleRed.Spec.Mode == ModeCluster {
		if err := r.validateClusterSpec(littleRed); err != nil {
			return err
		}
	}

	if err := r.validatePlacementSpec(littleRed); err != nil {
		return err
	}

	return nil
}

// validatePlacementSpec validates spec.placement. shardAntiAffinity is cluster-mode only
// (sentinel/standalone spread their pods via spec.podTemplate directly), and its
// whenUnsatisfiable must be a valid topology-spread action. The CRD enum already enforces the
// latter at admission; this is defense-in-depth (and catches the mode mismatch, which the CRD
// cannot express here).
func (r *LittleRedReconciler) validatePlacementSpec(littleRed *littleredv1alpha1.LittleRed) error {
	p := littleRed.Spec.Placement
	if p == nil || p.ShardAntiAffinity == nil {
		return nil
	}
	if littleRed.Spec.Mode != ModeCluster {
		return fmt.Errorf("spec.placement.shardAntiAffinity is only supported in cluster mode (mode is %q)", littleRed.Spec.Mode)
	}
	switch p.ShardAntiAffinity.WhenUnsatisfiable {
	case "", corev1.ScheduleAnyway, corev1.DoNotSchedule:
		return nil
	default:
		return fmt.Errorf("spec.placement.shardAntiAffinity.whenUnsatisfiable must be ScheduleAnyway or DoNotSchedule, got %q",
			p.ShardAntiAffinity.WhenUnsatisfiable)
	}
}

// reconcileStandalone reconciles standalone mode
func (r *LittleRedReconciler) reconcileStandalone(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	log.Info("Reconciling standalone mode")

	// Set initial phase
	if littleRed.Status.Phase == "" {
		littleRed.Status.Phase = littleredv1alpha1.PhasePending
	}

	// Reconcile ConfigMap
	if err := r.reconcileConfigMap(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile ConfigMap")
		return ctrl.Result{}, err
	}

	// Reconcile StatefulSet
	if err := r.reconcileStatefulSet(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile StatefulSet")
		return ctrl.Result{}, err
	}

	// Reconcile Service
	if err := r.reconcileService(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Service")
		return ctrl.Result{}, err
	}

	// Reconcile PodDisruptionBudget
	if err := r.reconcilePodDisruptionBudget(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile PodDisruptionBudget")
		return ctrl.Result{}, err
	}

	// Reconcile ServiceMonitor if enabled
	if littleRed.Spec.Metrics.IsEnabled() && littleRed.Spec.Metrics.ServiceMonitor.Enabled {
		if err := r.reconcileServiceMonitor(ctx, littleRed); err != nil {
			log.Error(err, "Failed to reconcile ServiceMonitor")
			// Don't fail reconciliation if ServiceMonitor fails (CRD might not be installed)
		}
	}

	// Update status
	return r.updateStatus(ctx, littleRed)
}

// reconcileConfigMap ensures the ConfigMap exists with the correct content
func (r *LittleRedReconciler) reconcileConfigMap(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildConfigMap(littleRed))
}

// reconcileStatefulSet ensures the StatefulSet exists with the correct spec
func (r *LittleRedReconciler) reconcileStatefulSet(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildStatefulSet(littleRed))
}

// reconcileService ensures the Service exists with the correct spec
func (r *LittleRedReconciler) reconcileService(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildService(littleRed))
}

// reconcileServiceMonitor ensures the ServiceMonitor exists
func (r *LittleRedReconciler) reconcileServiceMonitor(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildServiceMonitor(littleRed))
}

// pdbEnabled returns true if PDB creation should be active for the given LittleRed CR.
// spec.podDisruptionBudget.create is a *bool defaulted to true by the CRD; a nil value
// (e.g. a CR created against an older CRD that lacked the default) is treated as enabled.
func (r *LittleRedReconciler) pdbEnabled(littleRed *littleredv1alpha1.LittleRed) bool {
	create := littleRed.Spec.PodDisruptionBudget.Create
	return create == nil || *create
}

// reconcilePodDisruptionBudget ensures no PodDisruptionBudget exists for standalone mode.
// Standalone runs a single Redis pod; a PDB over a single-pod workload is counter-productive
// (it can only ever block node drains, never protect availability), so we never create one
// regardless of spec.podDisruptionBudget.create. The deletion handles upgrades from earlier
// versions that created a standalone PDB. See PR #92.
func (r *LittleRedReconciler) reconcilePodDisruptionBudget(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.deleteIfExists(ctx, littleRed, &policyv1.PodDisruptionBudget{}, podDisruptionBudgetName(littleRed))
}

// reconcileSentinelRedisPDB creates or deletes the PDB for the Redis StatefulSet in sentinel mode based on spec.
func (r *LittleRedReconciler) reconcileSentinelRedisPDB(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	if r.pdbEnabled(littleRed) {
		return r.apply(ctx, littleRed, buildSentinelRedisPDB(littleRed))
	}
	return r.deleteIfExists(ctx, littleRed, &policyv1.PodDisruptionBudget{}, podDisruptionBudgetName(littleRed))
}

// reconcileSentinelPDB creates or deletes the PDB for the Sentinel StatefulSet based on spec.
func (r *LittleRedReconciler) reconcileSentinelPDB(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	if r.pdbEnabled(littleRed) {
		return r.apply(ctx, littleRed, buildSentinelPDB(littleRed))
	}
	return r.deleteIfExists(ctx, littleRed, &policyv1.PodDisruptionBudget{}, sentinelPodDisruptionBudgetName(littleRed))
}

// deleteIfExists deletes a namespaced resource by name, ignoring not-found errors.
func (r *LittleRedReconciler) deleteIfExists(ctx context.Context, littleRed *littleredv1alpha1.LittleRed, obj client.Object, name string) error {
	obj.SetName(name)
	obj.SetNamespace(littleRed.Namespace)
	if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// updateStatus updates the LittleRed status based on current state
func (r *LittleRedReconciler) updateStatus(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	oldStatus := littleRed.Status.DeepCopy()

	// Get StatefulSet status
	sts := &appsv1.StatefulSet{}
	stsName := fmt.Sprintf("%s-redis", littleRed.Name)
	if err := r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: littleRed.Namespace}, sts); err != nil {
		if apierrors.IsNotFound(err) {
			littleRed.Status.Phase = littleredv1alpha1.PhasePending
			littleRed.Status.Redis.Ready = 0
			littleRed.Status.Redis.Total = 1
		} else {
			return ctrl.Result{}, err
		}
	} else {
		littleRed.Status.Redis.Ready = sts.Status.ReadyReplicas
		littleRed.Status.Redis.Total = *sts.Spec.Replicas

		// Determine phase
		if sts.Status.ReadyReplicas == *sts.Spec.Replicas && sts.Status.ReadyReplicas > 0 {
			littleRed.Status.Phase = littleredv1alpha1.PhaseRunning
			meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionReady,
				Status:             metav1.ConditionTrue,
				Reason:             "AllPodsReady",
				Message:            "All pods are ready",
				LastTransitionTime: metav1.Now(),
			})
			meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionInitialized,
				Status:             metav1.ConditionTrue,
				Reason:             "Initialized",
				Message:            "Redis is initialized",
				LastTransitionTime: metav1.Now(),
			})
		} else if sts.Status.ReadyReplicas > 0 {
			littleRed.Status.Phase = littleredv1alpha1.PhaseInitializing
			meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             reasonPodsNotReady,
				Message:            fmt.Sprintf("%d/%d pods ready", sts.Status.ReadyReplicas, *sts.Spec.Replicas),
				LastTransitionTime: metav1.Now(),
			})
		} else {
			littleRed.Status.Phase = littleredv1alpha1.PhaseInitializing
			meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             reasonPodsNotReady,
				Message:            "Waiting for pods to start",
				LastTransitionTime: metav1.Now(),
			})
		}
	}

	// Update observed generation
	littleRed.Status.ObservedGeneration = littleRed.Generation

	// Update high-level status summary
	if littleRed.Status.Phase == littleredv1alpha1.PhaseRunning {
		littleRed.Status.Status = littleredv1alpha1.ConditionReady
	} else {
		littleRed.Status.Status = string(littleRed.Status.Phase)
	}

	// Update status if changed
	if !reflect.DeepEqual(oldStatus, littleRed.Status) {
		if err := r.Status().Update(ctx, littleRed); err != nil {
			if apierrors.IsConflict(err) {
				log.Info("Status update conflict, requeueing")
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err
		}
	}

	fast, steady := littleRed.GetRequeueIntervals()

	// Requeue if not running to check status
	if littleRed.Status.Phase != littleredv1alpha1.PhaseRunning {
		log.Info("Not yet Running, requeueing",
			"phase", littleRed.Status.Phase,
			"redis", fmt.Sprintf("%d/%d", littleRed.Status.Redis.Ready, littleRed.Status.Redis.Total))
		return ctrl.Result{RequeueAfter: fast}, nil
	}

	return ctrl.Result{RequeueAfter: steady}, nil
}

// reconcileSentinel reconciles sentinel mode
func (r *LittleRedReconciler) reconcileSentinel(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	log.Info("Reconciling sentinel mode")

	// Set initial phase
	if littleRed.Status.Phase == "" {
		littleRed.Status.Phase = littleredv1alpha1.PhasePending
	}

	// Reconcile Redis ConfigMap
	if err := r.reconcileConfigMapSentinel(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Redis ConfigMap")
		return ctrl.Result{}, err
	}

	// Reconcile Sentinel ConfigMap
	if err := r.reconcileSentinelConfigMap(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Sentinel ConfigMap")
		return ctrl.Result{}, err
	}

	// Reconcile headless service for Redis (needed before StatefulSet)
	if err := r.reconcileReplicasService(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile replicas Service")
		return ctrl.Result{}, err
	}

	// Reconcile headless service for Sentinel
	if err := r.reconcileSentinelService(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Sentinel Service")
		return ctrl.Result{}, err
	}

	// Reconcile Redis StatefulSet
	if err := r.reconcileRedisStatefulSetSentinel(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Redis StatefulSet")
		return ctrl.Result{}, err
	}

	// Reconcile Sentinel StatefulSet
	if err := r.reconcileSentinelStatefulSet(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Sentinel StatefulSet")
		return ctrl.Result{}, err
	}

	// Reconcile master Service (points to current master)
	if err := r.reconcileMasterService(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile master Service")
		return ctrl.Result{}, err
	}

	// Bootstrap Sentinel if required
	if littleRed.Status.BootstrapRequired {
		if err := r.bootstrapSentinel(ctx, littleRed); err != nil {
			log.Error(err, "Failed to bootstrap Sentinel")
			return ctrl.Result{}, err
		}
	}

	// Update pod labels to reflect current master
	if err := r.updateMasterLabel(ctx, littleRed); err != nil {
		log.Error(err, "Failed to update master labels")
		// Don't fail - this is best effort
	}

	// Unified Sentinel cluster reconciliation
	if err := r.reconcileSentinelCluster(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Sentinel cluster")
	}

	// Reconcile PodDisruptionBudgets
	if err := r.reconcileSentinelRedisPDB(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Redis PodDisruptionBudget")
		return ctrl.Result{}, err
	}
	if err := r.reconcileSentinelPDB(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Sentinel PodDisruptionBudget")
		return ctrl.Result{}, err
	}

	// Reconcile ServiceMonitor if enabled
	if littleRed.Spec.Metrics.IsEnabled() && littleRed.Spec.Metrics.ServiceMonitor.Enabled {
		if err := r.reconcileServiceMonitor(ctx, littleRed); err != nil {
			log.Error(err, "Failed to reconcile ServiceMonitor")
		}
	}

	// Ensure background sentinel monitoring is running
	r.ensureSentinelMonitor(ctx, littleRed)

	// Update status
	return r.updateSentinelStatus(ctx, littleRed)
}

// reconcileConfigMapSentinel ensures the Redis ConfigMap exists for sentinel mode
func (r *LittleRedReconciler) reconcileConfigMapSentinel(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildConfigMapSentinelMode(littleRed))
}

// reconcileSentinelConfigMap ensures the Sentinel ConfigMap exists
func (r *LittleRedReconciler) reconcileSentinelConfigMap(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildSentinelConfigMap(littleRed))
}

// reconcileReplicasService ensures the headless service for Redis pods exists
func (r *LittleRedReconciler) reconcileReplicasService(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildReplicasHeadlessService(littleRed))
}

// reconcileSentinelService ensures the headless service for Sentinel pods exists
func (r *LittleRedReconciler) reconcileSentinelService(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildSentinelHeadlessService(littleRed))
}

// reconcileRedisStatefulSetSentinel ensures the Redis StatefulSet exists for sentinel mode
func (r *LittleRedReconciler) reconcileRedisStatefulSetSentinel(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildRedisStatefulSetSentinel(littleRed))
}

// reconcileSentinelStatefulSet ensures the Sentinel StatefulSet exists
func (r *LittleRedReconciler) reconcileSentinelStatefulSet(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildSentinelStatefulSet(littleRed))
}

// reconcileMasterService ensures the master Service exists
func (r *LittleRedReconciler) reconcileMasterService(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildMasterService(littleRed))
}

// reconcileSentinelCluster gathers ground truth from all pods (Redis and Sentinel)
// and performs atomic healing of the entire cluster state.
//
//nolint:gocyclo
func (r *LittleRedReconciler) reconcileSentinelCluster(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	// The Sentinel master name is this instance's isolation boundary against every
	// other Sentinel deployment on the pod network — never a shared constant.
	sentinelMasterName := littleRed.SentinelMasterName()
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)

	// Skip if we haven't bootstrapped yet
	if littleRed.Status.BootstrapRequired {
		return nil
	}

	password := r.getRedisPassword(ctx, littleRed)

	// 1. Gather all living pods
	redisPods := &corev1.PodList{}
	if err := r.List(ctx, redisPods, client.InNamespace(littleRed.Namespace), client.MatchingLabels(redisSelectorLabels(littleRed))); err != nil {
		return err
	}

	sentinelPods := &corev1.PodList{}
	if err := r.List(ctx, sentinelPods, client.InNamespace(littleRed.Namespace), client.MatchingLabels(sentinelSelectorLabels(littleRed))); err != nil {
		return err
	}

	redisMap := make(map[string]string)
	for _, p := range redisPods.Items {
		if p.Status.PodIP != "" && p.DeletionTimestamp.IsZero() {
			redisMap[p.Status.PodIP] = p.Name
		}
	}

	sentinelMap := make(map[string]string)
	for _, p := range sentinelPods.Items {
		if p.Status.PodIP != "" && p.DeletionTimestamp.IsZero() {
			sentinelMap[p.Status.PodIP] = p.Name
		}
	}

	// Any pod terminating? (Important for guardrails)
	anyTerminating := false
	for _, p := range append(redisPods.Items, sentinelPods.Items...) {
		if !p.DeletionTimestamp.IsZero() {
			anyTerminating = true
			break
		}
	}

	// 2. Gather Cluster State (Ground Truth)
	g := &operatorGatherer{password: password, tlsEnabled: littleRed.Spec.TLS.Enabled}
	state := redisclient.GatherClusterState(ctx, g, redisMap, sentinelMap)

	// 3. Healing

	// Rule 0: Re-register sentinel pods that started without a master configured.
	// This happens when a sentinel pod restarts with a new IP: bootstrapRequired is
	// already false so bootstrapSentinel() won't run, yet the new pod starts bare.
	// Sentinel gossip cannot self-heal this — a sentinel with no master configured
	// cannot discover the pubsub channel and therefore never joins the cluster.
	// We detect the condition via Reachable && !Monitoring and issue SENTINEL MONITOR
	// directly to the individual pod IP (not via the headless Service, for the same
	// reason as bootstrap: the Service load-balances to a single backend).
	// This action is safe during any transition because adding a monitor to an
	// unconfigured sentinel is non-disruptive to the running cluster.
	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)
	if state.RealMasterIP != "" {
		quorum := 2
		if littleRed.Spec.Sentinel != nil && littleRed.Spec.Sentinel.Quorum > 0 {
			quorum = littleRed.Spec.Sentinel.Quorum
		}
		for ip, sn := range state.SentinelNodes {
			if !sn.Reachable || sn.Monitoring {
				continue
			}
			auditLog.Info("Sentinel pod has no master configured, re-registering",
				"pod", sn.PodName, "ip", ip, "master", state.RealMasterIP)
			podAddr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.SentinelPort)
			podSC := redisclient.NewSentinelClient([]string{podAddr}, password, littleRed.Spec.TLS.Enabled)
			if err := podSC.Monitor(ctx, sentinelMasterName, state.RealMasterIP, littleredv1alpha1.RedisPort, quorum); err != nil {
				auditLog.Error(err, "Failed to re-register sentinel pod", "pod", sn.PodName)
				continue
			}
			if password != "" {
				_ = podSC.Set(ctx, sentinelMasterName, "auth-pass", password)
			}
			applySentinelSettings(ctx, podSC, littleRed.Spec.Sentinel, sentinelMasterName)
		}
	}

	// Rule A: Guardrails
	// We skip ALL healing if:
	// 1. Any pod is terminating (K8s is already working).
	// 2. Sentinel reports an active failover (Sentinel is already working).
	if anyTerminating || state.FailoverActive {
		log.Info("Cluster transition in progress. Skipping healing.",
			"anyTerminating", anyTerminating,
			"failoverActive", state.FailoverActive)
		return nil
	}

	// Note: state.RealMasterIP == "" (leaderless) used to be a hard blocker here.
	// We now allow reconciliation to proceed so that Rule A+ (ghost pruning)
	// can clear stuck sentinels even during leaderless periods.

	// Ghost pruning: only safe if the master Sentinel reports is a living pod
	ghostMasterFound := false
	ghostFound := false
	stateLog := r.getLogger(ctx, littleRed, LogCategoryState)

	quorum := 2
	if littleRed.Spec.Sentinel != nil && littleRed.Spec.Sentinel.Quorum > 0 {
		quorum = littleRed.Spec.Sentinel.Quorum
	}

	for ip, sn := range state.SentinelNodes {
		if !sn.Reachable || !sn.Monitoring {
			continue
		}
		// A sentinel still pointing at a ghost master means it lost its failover
		// notification (e.g. two sentinels raced to lead the failover and the
		// "winner" superseded the elected leader before it could record the
		// switch).
		//
		// We REMOVE and re-MONITOR this individual sentinel so it points to the correct
		// master IP.
		// Safety: We ONLY do this if a living master consensus exists. If the cluster
		// is currently leaderless (RealMasterIP == ""), we MUST NOT intervene,
		// because sentinels might be correctly timing out a recently-deceased master.
		if state.RealMasterIP != "" && state.IsGhost(sn.MasterIP) && !state.IsGhost(state.RealMasterIP) && state.RedisNodes[state.RealMasterIP] != nil && state.RedisNodes[state.RealMasterIP].Reachable {
			auditLog.Info("Sentinel monitoring ghost master; re-registering correct master",
				"pod", sn.PodName, "ghost_master", sn.MasterIP, "correct_master", state.RealMasterIP)
			podAddr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.SentinelPort)
			podSC := redisclient.NewSentinelClient([]string{podAddr}, password, littleRed.Spec.TLS.Enabled)

			_ = podSC.Remove(ctx, sentinelMasterName)
			if err := podSC.Monitor(ctx, sentinelMasterName, state.RealMasterIP, littleredv1alpha1.RedisPort, quorum); err == nil {
				if password != "" {
					_ = podSC.Set(ctx, sentinelMasterName, "auth-pass", password)
				}
				applySentinelSettings(ctx, podSC, littleRed.Spec.Sentinel, sentinelMasterName)
			}

			ghostMasterFound = true
			continue // don't inspect this sentinel's replica list
		}

		// A sentinel monitoring a LIVING but WRONG master must also be re-registered.
		// This requires a consensus on the RealMasterIP.
		if state.RealMasterIP != "" && sn.MasterIP != state.RealMasterIP && !state.IsGhost(state.RealMasterIP) && state.RedisNodes[state.RealMasterIP] != nil && state.RedisNodes[state.RealMasterIP].Reachable {
			auditLog.Info("Sentinel monitoring wrong master IP; re-registering correct master",
				"pod", sn.PodName, "monitored_master", sn.MasterIP, "correct_master", state.RealMasterIP)
			podAddr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.SentinelPort)
			podSC := redisclient.NewSentinelClient([]string{podAddr}, password, littleRed.Spec.TLS.Enabled)

			_ = podSC.Remove(ctx, sentinelMasterName)
			if err := podSC.Monitor(ctx, sentinelMasterName, state.RealMasterIP, littleredv1alpha1.RedisPort, quorum); err == nil {
				if password != "" {
					_ = podSC.Set(ctx, sentinelMasterName, "auth-pass", password)
				}
				applySentinelSettings(ctx, podSC, littleRed.Spec.Sentinel, sentinelMasterName)
			}

			ghostMasterFound = true // using this flag to trigger requeue
			continue
		}

		// Check replicas for ghost IPs (IPs not belonging to any living pod).
		// We accept s_down here — for ghost replicas, s_down is the correct
		// signal. o_down (objectively down) is never set on replicas by Sentinel;
		// it only applies to the master and requires a quorum vote. Requiring
		// o_down for replicas means the condition is permanently dead.
		//
		// Rule A above ensures we skip this block entirely while any pod is
		// Terminating or a failover is active, giving Sentinel time to finish
		// sending REPLICAOF to surviving replicas before we issue RESET.
		for _, replica := range sn.Replicas {
			if state.IsGhost(replica.IP) && (strings.Contains(replica.Flags, "s_down") || strings.Contains(replica.Flags, "o_down")) {
				stateLog.Info("Ghost node detected in Sentinel topology", "ip", replica.IP, "flags", replica.Flags, "sentinel", sn.PodName)
				ghostFound = true
				break
			}
		}
		if ghostFound {
			break
		}
	}

	if ghostMasterFound {
		// Requeue so the next cycle can verify the sentinels have converged.
		return nil
	}

	// If no master is known yet, attempt to break a bootstrap deadlock (all
	// sentinels bare, no reachable master) before giving up for this pass. This is
	// the only rule that operates while leaderless; every other rule requires a
	// consensus master. See ADR-005 (LR-015).
	if state.RealMasterIP == "" {
		// Two mutually-exclusive no-living-master deadlocks are recoverable here. Rule L
		// handles the bare-Sentinel bootstrap deadlock; recoverGhostMasterDeadlock handles
		// the ghost-master failover deadlock (Sentinels pinned to a dead master, no
		// promotable replica). Each no-ops when it is not its case.
		if err := r.recoverLeaderlessDeadlock(ctx, littleRed, state, redisMap, password); err != nil {
			stateLog.Error(err, "leaderless deadlock recovery failed")
		}
		if err := r.recoverGhostMasterDeadlock(ctx, littleRed, state, redisMap, password); err != nil {
			stateLog.Error(err, "ghost-master deadlock recovery failed")
		}
		return nil
	}

	// A consensus master is known — clear any leaderless-deadlock marker left over
	// from a prior recovery attempt. No-op (no API call) when already clear.
	if err := r.clearLeaderlessSince(ctx, littleRed, reasonRecovered, "A consensus master is known again."); err != nil {
		stateLog.Error(err, "failed to clear leaderless marker")
	}
	if err := r.clearGhostMasterStuckSince(ctx, littleRed, reasonRecovered, "A consensus master is known again."); err != nil {
		stateLog.Error(err, "failed to clear ghost-master marker")
	}

	// Rule D (continued): Prune ghost replicas.
	// Safety conditions for issuing SENTINEL RESET:
	// 1. A ghost node was detected
	// 2. The consensus master is living and reachable (not a ghost)
	// 3. At least one living, non-ghost replica is known to Sentinel
	//
	// Condition 3 prevents a race after failover: SENTINEL RESET wipes ALL slave
	// knowledge. If we reset before Sentinel re-discovers the surviving replicas,
	// the next failover attempt fails with "no good slave" because Sentinel has
	// no candidates to promote. Requiring at least 1 healthy replica ensures
	// Sentinel can recover from the RESET.
	// clusterWhole: every expected Redis pod is present and reachable. RESET wipes
	// Sentinel's replica list, so we only prune ghost replicas when the cluster is
	// whole — otherwise a RESET racing a node loss (e.g. a force-deleted master)
	// strands Sentinel with no promotable replica, a permanent deadlock (LR-013).
	// This uses only ground truth already gathered this loop — no extra requests.
	reachableRedis := 0
	for _, rn := range state.RedisNodes {
		if rn.Reachable {
			reachableRedis++
		}
	}
	clusterWhole := reachableRedis == int(littleredv1alpha1.SentinelRedisReplicas)

	if state.GhostReplicaResetSafe(ghostFound, clusterWhole) {
		auditLog.Info("Issuing SENTINEL RESET to clear ghost nodes from topology",
			"master", sentinelMasterName, "reachableRedis", reachableRedis)
		sentinelAddresses := r.getSentinelAddresses(ctx, littleRed)
		sc := redisclient.NewSentinelClient(sentinelAddresses, password, littleRed.Spec.TLS.Enabled)
		_ = sc.Reset(ctx, sentinelMasterName)
	} else if ghostFound {
		log.Info("Ghost replica detected but skipping SENTINEL RESET",
			"master", state.RealMasterIP, "clusterWhole", clusterWhole,
			"reachableRedis", reachableRedis, "expected", littleredv1alpha1.SentinelRedisReplicas)
	}

	// Rule R: Replica Rescue
	// Ensure all living Redis pods that are not the consensus master are actually
	// configured as replicas.
	for ip, rn := range state.RedisNodes {
		if !rn.Reachable || ip == state.RealMasterIP {
			continue
		}
		// If the pod thinks it's a master, or is following the wrong master.
		// We DON'T trigger on LinkStatus == "down" alone, because that could be a
		// transient state during a handshake, and re-issuing SLAVEOF would interrupt it.
		if rn.Role == RoleMaster || rn.MasterHost != state.RealMasterIP {
			auditLog.Info("Redis pod is not following the consensus master, issuing SLAVEOF",
				"pod", rn.PodName, "current_role", rn.Role, "target_master", state.RealMasterIP)
			if err := redisclient.SlaveOf(ctx, fmt.Sprintf("%s:%d", ip, littleredv1alpha1.RedisPort), password, state.RealMasterIP, fmt.Sprintf("%d", littleredv1alpha1.RedisPort), littleRed.Spec.TLS.Enabled); err != nil {
				auditLog.Error(err, "Failed to rescue replica", "pod", rn.PodName)
			}
		}
	}

	return nil
}

// getSentinelAddresses returns a list of Sentinel addresses to try (Service FQDN and pod IPs)
func (r *LittleRedReconciler) getSentinelAddresses(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) []string {
	addresses := []string{
		fmt.Sprintf("%s-sentinel.%s.svc:%d",
			littleRed.Name, littleRed.Namespace, littleredv1alpha1.SentinelPort),
	}

	// Also add pod IPs for resilience, but only for healthy pods.
	// Using dead or terminating IPs causes long connection timeouts that hang reconciliation.
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(littleRed.Namespace),
		client.MatchingLabels(sentinelSelectorLabels(littleRed)),
	}
	if err := r.List(ctx, podList, listOpts...); err == nil {
		for _, pod := range podList.Items {
			// Skip pods that are being deleted or aren't ready yet
			isReady := false
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					isReady = true
					break
				}
			}

			if pod.Status.PodIP != "" && pod.DeletionTimestamp.IsZero() && isReady {
				addresses = append(addresses, fmt.Sprintf("%s:%d", pod.Status.PodIP, littleredv1alpha1.SentinelPort))
			}
		}
	}

	return addresses
}

// applySentinelSettings applies tunable sentinel configuration parameters to a
// specific sentinel pod for the named master. Must be called after SENTINEL MONITOR
// to apply user-configured thresholds. All Set errors are intentionally swallowed —
// a failure to apply settings is non-fatal and will be retried on the next reconcile.
func applySentinelSettings(ctx context.Context, sc *redisclient.SentinelClient, spec *littleredv1alpha1.SentinelSpec, sentinelMasterName string) {
	if spec == nil {
		return
	}
	if spec.DownAfterMilliseconds > 0 {
		_ = sc.Set(ctx, sentinelMasterName, "down-after-milliseconds", fmt.Sprintf("%d", spec.DownAfterMilliseconds))
	}
	if spec.FailoverTimeout > 0 {
		_ = sc.Set(ctx, sentinelMasterName, "failover-timeout", fmt.Sprintf("%d", spec.FailoverTimeout))
	}
	if spec.ParallelSyncs > 0 {
		_ = sc.Set(ctx, sentinelMasterName, "parallel-syncs", fmt.Sprintf("%d", spec.ParallelSyncs))
	}
}

// getMasterPodName queries Sentinel to find the current master pod name.
// Returns an error if Sentinel query fails.
func (r *LittleRedReconciler) getMasterPodName(ctx context.Context, littleRed *littleredv1alpha1.LittleRed, podList *corev1.PodList) (string, error) {
	sentinelMasterName := littleRed.SentinelMasterName()
	// Try to get real master from Sentinel
	addresses := r.getSentinelAddresses(ctx, littleRed)

	password := ""
	if littleRed.Spec.Auth.Enabled {
		password = r.getRedisPassword(ctx, littleRed)
	}

	// Use a short timeout for the check
	checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	sc := redisclient.NewSentinelClient(addresses, password, littleRed.Spec.TLS.Enabled)
	masterInfo, err := sc.GetMaster(checkCtx, sentinelMasterName)
	if err != nil {
		// If Sentinel explicitly says "no master", it's a confirmed state
		if strings.Contains(err.Error(), "redis: nil") {
			return "", &SentinelError{
				Code:    SentinelNoMaster,
				Message: "Sentinel explicitly reported no master monitored",
			}
		}
		// Otherwise it's a connection/unreachable error
		return "", &SentinelError{
			Code:    SentinelUnreachable,
			Message: "Failed to reach any Sentinel or get master info",
			Err:     err,
		}
	}

	// masterInfo.IP MUST be an IP address in our strict identity model.
	reportedIdentity := masterInfo.IP

	// Find pod with matching IP (skip terminating pods — their IPs are stale)
	for _, pod := range podList.Items {
		if pod.Status.PodIP == reportedIdentity && pod.DeletionTimestamp.IsZero() {
			return pod.Name, nil
		}
	}

	// Reported master IP not found in current pod list -> Ghost Master
	return "", &SentinelError{
		Code:    SentinelGhostMaster,
		Message: fmt.Sprintf("Sentinel reported master IP %q not found in pod list", reportedIdentity),
		IP:      reportedIdentity,
	}
}

// updateMasterLabel updates the role labels on Redis pods based on current master
func (r *LittleRedReconciler) updateMasterLabel(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)

	// List all Redis pods
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(littleRed.Namespace),
		client.MatchingLabels(redisSelectorLabels(littleRed)),
	}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		return err
	}

	if len(podList.Items) == 0 {
		return nil // No pods yet
	}

	// Skip label updates during pod transitions to avoid churn during failovers
	for _, pod := range podList.Items {
		if !pod.DeletionTimestamp.IsZero() {
			log.Info("Pod terminating, skipping label update to avoid churn during failover", "pod", pod.Name)
			return nil
		}
	}

	stateLog := r.getLogger(ctx, littleRed, LogCategoryState)
	masterPodName, err := r.getMasterPodName(ctx, littleRed, podList)
	if err != nil {
		var sErr *SentinelError
		if errors.As(err, &sErr) {
			switch sErr.Code {
			case SentinelUnreachable:
				log.Info("Sentinel unreachable, skipping label update to avoid churn", "error", sErr.Err)
				return nil
			case SentinelNoMaster:
				stateLog.Info("Sentinel confirms no master is currently monitored. Ensuring no pod is labeled as master.")
				masterPodName = ""
			case SentinelGhostMaster:
				stateLog.Info("Sentinel reported a ghost master. Ensuring no pod is labeled as master.", "ghost_ip", sErr.IP)
				masterPodName = ""
			}
		} else {
			log.Error(err, "Unexpected error identifying master pod, skipping label update")
			return nil
		}
	}

	// surgical role updates:
	// 1. If we have a masterPodName, ensure ONLY that pod is labeled Master.
	// 2. If we DON'T have a masterPodName, ensure NO pod is labeled Master.
	// 3. We only change Replica/Orphan labels if we are sure of the state.
	//    During failover (masterPodName == ""), we just strip the Master label
	//    from whoever had it and leave others alone.

	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)
	for i := range podList.Items {
		pod := &podList.Items[i]
		currentRole := pod.Labels[LabelRole]
		expectedRole := currentRole // default: stay as you are

		if masterPodName != "" {
			// We have a consensus master. Ensure ALL pods have correct labels.
			if pod.Name == masterPodName {
				expectedRole = RoleMaster
			} else {
				expectedRole = RoleReplica
			}
		} else {
			// No master identified by Sentinel (failover in progress).
			// Be surgical: only strip the Master label if someone has it.
			if currentRole == RoleMaster {
				expectedRole = RoleReplica // downgrade to replica while waiting
			}
		}

		if currentRole != expectedRole {
			auditLog.Info("Updating pod role label", "pod", pod.Name, "old_role", currentRole, "new_role", expectedRole)
			if pod.Labels == nil {
				pod.Labels = make(map[string]string)
			}
			pod.Labels[LabelRole] = expectedRole
			if err := r.Update(ctx, pod); err != nil {
				return err
			}
		}
	}
	return nil
}

// updateSentinelStatus updates the LittleRed status for sentinel mode
//
//nolint:gocyclo
func (r *LittleRedReconciler) updateSentinelStatus(ctx context.Context, lr *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	// The Sentinel master name is this instance's isolation boundary against every
	// other Sentinel deployment on the pod network — never a shared constant.
	sentinelMasterName := lr.SentinelMasterName()
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	stateLog := r.getLogger(ctx, lr, LogCategoryState)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		oldStatus := latest.Status.DeepCopy()

		// Get Redis StatefulSet status
		redisSts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      fmt.Sprintf("%s-redis", latest.Name),
			Namespace: latest.Namespace,
		}, redisSts); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			latest.Status.Redis.Ready = 0
			latest.Status.Redis.Total = 3
		} else {
			latest.Status.Redis.Ready = redisSts.Status.ReadyReplicas
			latest.Status.Redis.Total = *redisSts.Spec.Replicas
		}

		// Get Sentinel StatefulSet status
		sentinelSts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      fmt.Sprintf("%s-sentinel", latest.Name),
			Namespace: latest.Namespace,
		}, sentinelSts); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			if latest.Status.Sentinels == nil {
				latest.Status.Sentinels = &littleredv1alpha1.SentinelStatus{}
			}
			latest.Status.Sentinels.Ready = 0
			latest.Status.Sentinels.Total = 3
		} else {
			if latest.Status.Sentinels == nil {
				latest.Status.Sentinels = &littleredv1alpha1.SentinelStatus{}
			}
			latest.Status.Sentinels.Ready = sentinelSts.Status.ReadyReplicas
			latest.Status.Sentinels.Total = *sentinelSts.Spec.Replicas
		}

		// Set replicas status (Redis pods - 1 master = replicas)
		if latest.Status.Replicas == nil {
			latest.Status.Replicas = &littleredv1alpha1.ReplicaStatus{}
		}
		if latest.Status.Redis.Ready > 0 {
			latest.Status.Replicas.Ready = latest.Status.Redis.Ready - 1
		} else {
			latest.Status.Replicas.Ready = 0
		}
		latest.Status.Replicas.Total = latest.Status.Redis.Total - 1

		// Set master info
		if latest.Status.Master == nil {
			latest.Status.Master = &littleredv1alpha1.MasterStatus{}
		}

		// List Redis pods to find the master IP
		podList := &corev1.PodList{}
		listOpts := []client.ListOption{
			client.InNamespace(latest.Namespace),
			client.MatchingLabels(redisSelectorLabels(latest)),
		}
		_ = r.List(ctx, podList, listOpts...)

		masterPodName, err := r.getMasterPodName(ctx, latest, podList)
		if err != nil {
			stateLog.Info("Sentinel unreachable or master unknown, reporting no master in status", "error", err)
			masterPodName = ""
		}
		latest.Status.Master.PodName = masterPodName

		// Try to get master pod IP
		if masterPodName != "" {
			masterPod := &corev1.Pod{}
			if err := r.Get(ctx, types.NamespacedName{
				Name:      masterPodName,
				Namespace: latest.Namespace,
			}, masterPod); err == nil {
				latest.Status.Master.IP = masterPod.Status.PodIP
			}
		} else {
			latest.Status.Master.IP = ""
		}

		// Count healthy replicas as seen by Sentinel. A slave is "up" if it is
		// not marked s_down, o_down, or disconnected. This prevents reporting
		// Running while Sentinel has not yet polled the master and registered all
		// replicas — which would let a caller (test or operator) trigger a
		// failover before Sentinel knows every slave, leaving replicas stuck on
		// the dead master IP.
		sentinelReplicasOK := 0
		if masterPodName != "" {
			password := r.getRedisPassword(ctx, latest)
			sc := redisclient.NewSentinelClient(r.getSentinelAddresses(ctx, latest), password, latest.Spec.TLS.Enabled)
			if replicas, err := sc.GetReplicas(ctx, sentinelMasterName); err == nil {
				for _, rep := range replicas {
					if !strings.Contains(rep.Flags, "s_down") &&
						!strings.Contains(rep.Flags, "o_down") &&
						!strings.Contains(rep.Flags, "disconnected") {
						sentinelReplicasOK++
					}
				}
			}
		}

		// Determine phase
		expectedReplicas := int(latest.Status.Redis.Total) - 1
		allReady := latest.Status.Redis.Ready == latest.Status.Redis.Total &&
			latest.Status.Sentinels.Ready == latest.Status.Sentinels.Total &&
			latest.Status.Redis.Ready > 0 &&
			masterPodName != "" &&
			sentinelReplicasOK >= expectedReplicas

		if allReady {
			latest.Status.Phase = littleredv1alpha1.PhaseRunning
			// If we reach Running phase, initial bootstrap is definitely complete
			latest.Status.BootstrapRequired = false

			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionReady,
				Status:             metav1.ConditionTrue,
				Reason:             "AllPodsReady",
				Message:            "All Redis and Sentinel pods are ready",
				LastTransitionTime: metav1.Now(),
			})
			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionSentinelReady,
				Status:             metav1.ConditionTrue,
				Reason:             "QuorumEstablished",
				Message:            "Sentinel quorum is established",
				LastTransitionTime: metav1.Now(),
			})
			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionInitialized,
				Status:             metav1.ConditionTrue,
				Reason:             "Initialized",
				Message:            "Redis sentinel cluster is initialized",
				LastTransitionTime: metav1.Now(),
			})
		} else {
			// Build a human-readable breakdown of which condition is blocking Running.
			var notReadyReasons []string
			if latest.Status.Redis.Ready == 0 {
				notReadyReasons = append(notReadyReasons, "no Redis pods ready")
			} else if latest.Status.Redis.Ready != latest.Status.Redis.Total {
				notReadyReasons = append(notReadyReasons, fmt.Sprintf("Redis pods %d/%d ready", latest.Status.Redis.Ready, latest.Status.Redis.Total))
			}
			if latest.Status.Sentinels.Ready != latest.Status.Sentinels.Total {
				notReadyReasons = append(notReadyReasons, fmt.Sprintf("Sentinel pods %d/%d ready", latest.Status.Sentinels.Ready, latest.Status.Sentinels.Total))
			}
			if masterPodName == "" {
				notReadyReasons = append(notReadyReasons, "master not yet known to Sentinel")
			}
			if masterPodName != "" && sentinelReplicasOK < expectedReplicas {
				notReadyReasons = append(notReadyReasons, fmt.Sprintf("Sentinel knows %d/%d replicas as healthy", sentinelReplicasOK, expectedReplicas))
			}
			log.Info("Not yet Running, requeueing", "reasons", strings.Join(notReadyReasons, "; "))

			latest.Status.Phase = littleredv1alpha1.PhaseInitializing
			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             reasonPodsNotReady,
				Message:            fmt.Sprintf("Redis: %d/%d, Sentinels: %d/%d, Sentinel-known replicas: %d/%d", latest.Status.Redis.Ready, latest.Status.Redis.Total, latest.Status.Sentinels.Ready, latest.Status.Sentinels.Total, sentinelReplicasOK, expectedReplicas),
				LastTransitionTime: metav1.Now(),
			})
		}

		// Surface whether this instance has its own Sentinel gossip identity. Runs on
		// every pass, independent of readiness: an unscoped instance is exposed whether
		// or not it is healthy, and it is the only signal a pre-field instance gets.
		r.reconcileSentinelMasterNameCondition(latest)

		// Update observed generation
		latest.Status.ObservedGeneration = latest.Generation

		// Update high-level status summary
		if latest.Status.Phase == littleredv1alpha1.PhaseRunning {
			latest.Status.Status = littleredv1alpha1.ConditionReady
		} else {
			latest.Status.Status = string(latest.Status.Phase)
		}

		// Update status if changed
		if !reflect.DeepEqual(oldStatus, &latest.Status) {
			return r.Status().Update(ctx, latest)
		}
		return nil
	})

	if err != nil {
		return ctrl.Result{}, err
	}

	// Re-fetch to get current phase/annotations for requeue logic
	latest := &littleredv1alpha1.LittleRed{}
	if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
		return ctrl.Result{}, err
	}

	fast, steady := latest.GetRequeueIntervals()

	// Requeue if not running
	if latest.Status.Phase != littleredv1alpha1.PhaseRunning {
		return ctrl.Result{RequeueAfter: fast}, nil
	}

	// Periodically requeue to update master info, unless disabled via annotation
	if latest.Annotations[AnnotationDisablePolling] == annotationValueTrue {
		log.Info("Sentinel polling disabled via annotation")
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: steady}, nil
}

// setFailedStatus sets the LittleRed status to Failed
func (r *LittleRedReconciler) setFailedStatus(ctx context.Context, lr *littleredv1alpha1.LittleRed, reason, message string) (ctrl.Result, error) {
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	log.Error(fmt.Errorf("%s", message), "Validation failed", "reason", reason)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}

		latest.Status.Phase = littleredv1alpha1.PhaseFailed
		latest.Status.ObservedGeneration = latest.Generation
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               littleredv1alpha1.ConditionConfigValid,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
		})
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               littleredv1alpha1.ConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
		})

		return r.Status().Update(ctx, latest)
	})

	if err != nil {
		return ctrl.Result{}, err
	}

	// Don't requeue - wait for spec change
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *LittleRedReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.sentinelEvents = make(chan event.GenericEvent)
	r.monitors = make(map[types.NamespacedName]func())

	return ctrl.NewControllerManagedBy(mgr).
		For(&littleredv1alpha1.LittleRed{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		WatchesRawSource(source.Channel(r.sentinelEvents, &handler.EnqueueRequestForObject{})).
		Named("littlered").
		Complete(r)
}

// apply uses Server-Side Apply to create or update a resource. It sets the
// controller reference and resolves the GVK from the scheme before patching.
// SSA only manages fields the operator explicitly sets, preserving external
// labels, annotations (e.g. kubectl rollout restart), and server-defaulted
// fields like ClusterIP.
func (r *LittleRedReconciler) apply(ctx context.Context, owner *littleredv1alpha1.LittleRed, obj client.Object) error {
	if err := controllerutil.SetControllerReference(owner, obj, r.Scheme); err != nil {
		return err
	}
	gvk, err := apiutil.GVKForObject(obj, r.Scheme)
	if err != nil {
		return err
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: raw}
	return r.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(fieldManager), client.ForceOwnership)
}

// getRedisPassword retrieves the Redis password from the secret if auth is enabled
func (r *LittleRedReconciler) getRedisPassword(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) string {
	if !littleRed.Spec.Auth.Enabled {
		return ""
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      littleRed.Spec.Auth.ExistingSecret,
		Namespace: littleRed.Namespace,
	}, secret); err != nil {
		return ""
	}
	return string(secret.Data["password"])
}

// validateClusterSpec validates cluster-specific configuration
func (r *LittleRedReconciler) validateClusterSpec(littleRed *littleredv1alpha1.LittleRed) error {
	cluster := littleRed.Spec.Cluster
	if cluster == nil {
		return nil
	}

	if cluster.Shards < 3 {
		return fmt.Errorf("cluster.shards must be at least 3, got %d", cluster.Shards)
	}

	if cluster.ReplicasPerShard != nil && *cluster.ReplicasPerShard < 0 {
		return fmt.Errorf("cluster.replicasPerShard cannot be negative, got %d", *cluster.ReplicasPerShard)
	}

	return nil
}

// bootstrapSentinel configures Sentinels to monitor the initial master
func (r *LittleRedReconciler) bootstrapSentinel(ctx context.Context, lr *littleredv1alpha1.LittleRed) error {
	log := r.getLogger(ctx, lr, LogCategoryRecon)

	// 1. Just-in-Time API Check: Re-fetch the object to ensure another worker hasn't already bootstrapped
	latest := &littleredv1alpha1.LittleRed{}
	if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
		return err
	}
	if !latest.Status.BootstrapRequired {
		log.Info("Bootstrap: flag already cleared in latest API version, skipping")
		*lr = *latest // Update local copy
		return nil
	}

	// 2. Ensure redis-0 belongs to the current StatefulSet and has an IP.
	// After a delete-and-recreate, a terminating redis-0 from the old deployment
	// may still exist with a stale IP. Using that IP would poison all sentinels.
	// We verify the pod's controller-revision-hash matches the StatefulSet's
	// currentRevision to guarantee it was created by the current deployment.
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name: statefulSetName(lr), Namespace: lr.Namespace,
	}, sts); err != nil {
		return err
	}

	pod0 := &corev1.Pod{}
	pod0Name := fmt.Sprintf("%s-redis-0", lr.Name)
	if err := r.Get(ctx, types.NamespacedName{Name: pod0Name, Namespace: lr.Namespace}, pod0); err != nil {
		return err
	}

	currentRevision := sts.Status.CurrentRevision
	podRevision := pod0.Labels["controller-revision-hash"]
	if pod0.Status.PodIP == "" || currentRevision == "" || podRevision != currentRevision {
		log.Info("Bootstrap: waiting for redis-0 to be ready",
			"hasIP", pod0.Status.PodIP != "",
			"podRevision", podRevision,
			"stsRevision", currentRevision)
		return nil
	}

	// 3. Configure Sentinel Client
	password := r.getRedisPassword(ctx, lr)

	quorum := 2
	if lr.Spec.Sentinel != nil && lr.Spec.Sentinel.Quorum > 0 {
		quorum = lr.Spec.Sentinel.Quorum
	}
	masterAddr := pod0.Status.PodIP

	// 4. Point every Sentinel pod at the initial master (redis-0).
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	configuredCount, totalPods, serr := r.seedSentinelsWithMaster(ctx, lr, masterAddr, password, quorum)
	if serr != nil {
		return serr
	}
	if configuredCount == 0 {
		log.Info("Bootstrap: no Sentinel pods reachable yet, will retry on next reconcile")
		return nil
	}
	if configuredCount < totalPods {
		log.Info("Bootstrap: not all sentinels configured yet", "configured", configuredCount, "total", totalPods)
		// Continue anyway — the sentinel gossip protocol will propagate the config to the others.
	}

	// 5. Clear bootstrap flag with retry on conflict
	auditLog.Info("Bootstrap: initial master registered, clearing bootstrapRequired flag")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestUpdate := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latestUpdate); err != nil {
			return err
		}
		if !latestUpdate.Status.BootstrapRequired {
			return nil // Already done
		}
		latestUpdate.Status.BootstrapRequired = false
		return r.Status().Update(ctx, latestUpdate)
	})
	if err != nil {
		return fmt.Errorf("failed to clear bootstrap flag: %w", err)
	}

	// Update the local object version to avoid subsequent conflicts in the same reconcile pass
	return r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, lr)
}

// seedSentinelsWithMaster issues SENTINEL MONITOR (+auth + tuning) to every
// reachable Sentinel pod, pointing each directly at masterIP.
//
// It never uses the headless Service VIP: that load-balances to a single backend,
// so only one sentinel would be configured and the quorum would never form. It
// iterates pod IPs instead. Sentinels that already monitor a master are counted
// but left untouched (idempotent). Returns (configured, totalReachablePods).
// Shared by initial bootstrap (bootstrapSentinel) and leaderless recovery
// (recoverLeaderlessDeadlock).
func (r *LittleRedReconciler) seedSentinelsWithMaster(ctx context.Context, lr *littleredv1alpha1.LittleRed, masterIP, password string, quorum int) (configured, total int, err error) {
	// The Sentinel master name is this instance's isolation boundary against every
	// other Sentinel deployment on the pod network — never a shared constant.
	sentinelMasterName := lr.SentinelMasterName()
	sentinelPods := &corev1.PodList{}
	if err := r.List(ctx, sentinelPods,
		client.InNamespace(lr.Namespace),
		client.MatchingLabels(sentinelSelectorLabels(lr)),
	); err != nil {
		return 0, 0, fmt.Errorf("failed to list sentinel pods: %w", err)
	}

	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	for i := range sentinelPods.Items {
		pod := &sentinelPods.Items[i]
		if pod.Status.PodIP == "" || !pod.DeletionTimestamp.IsZero() {
			continue
		}
		total++
		podAddr := fmt.Sprintf("%s:%d", pod.Status.PodIP, littleredv1alpha1.SentinelPort)
		podSC := redisclient.NewSentinelClient([]string{podAddr}, password, lr.Spec.TLS.Enabled)

		// Idempotent guard: leave a sentinel that already monitors the CORRECT master
		// alone (avoids re-MONITOR churn every reconcile). But if it monitors a DIFFERENT
		// master — e.g. a dead ghost master during a failover deadlock (LR-024) — force it
		// onto masterIP by REMOVE-ing the stale entry first: a plain SENTINEL MONITOR is
		// rejected while a same-named master is still configured, so without this the
		// sentinel stays stranded on the ghost and the election never takes effect.
		if info, gerr := podSC.GetMaster(ctx, sentinelMasterName); gerr == nil && info != nil {
			if info.IP == masterIP {
				configured++
				continue
			}
			_ = podSC.Remove(ctx, sentinelMasterName)
		}

		auditLog.Info("Pointing Sentinel at master", "sentinel", pod.Name, "master", masterIP)
		if merr := podSC.Monitor(ctx, sentinelMasterName, masterIP, littleredv1alpha1.RedisPort, quorum); merr != nil {
			auditLog.Error(merr, "Failed to configure sentinel", "sentinel", pod.Name)
			continue // best-effort
		}
		if password != "" {
			_ = podSC.Set(ctx, sentinelMasterName, "auth-pass", password)
		}
		applySentinelSettings(ctx, podSC, lr.Spec.Sentinel, sentinelMasterName)
		configured++
	}
	return configured, total, nil
}

// leaderlessRecoveryCooldown is how long the operator waits after first observing
// a leaderless, all-sentinels-bare state before attempting a rebootstrap. The
// fast requeue interval (2s) re-checks well within this window, so a transient
// startup blip clears the LeaderlessSince marker long before the cooldown expires.
const leaderlessRecoveryCooldown = 30 * time.Second

// recoverLeaderlessDeadlock breaks a Sentinel bootstrap deadlock — the state where
// no Sentinel monitors a master, no reachable Redis node is a master, and thus no
// other healing rule (all of which require a consensus master) can make progress.
// See ADR-005 (LR-015).
//
// It is deliberately conservative:
//   - Only fires when every reachable Sentinel is bare (excludes a recent master
//     death, where Sentinels still monitor the dead master and can fail over).
//   - Requires a reachable Sentinel quorum, so the seed can actually form consensus.
//   - Requires the state to persist past leaderlessRecoveryCooldown, so a brief
//     rollout blip never triggers a rebootstrap.
//   - Is data-aware: if any reachable Redis pod still holds keys it refuses unless
//     the owner opted in via sentinel.allowUnsafeRebootstrapOnDeadlock, in which
//     case it force-elects the most-complete pod (data on the others is discarded).
//
// It is called only when RealMasterIP == "" (nobody knows a living master).
func (r *LittleRedReconciler) recoverLeaderlessDeadlock(
	ctx context.Context,
	lr *littleredv1alpha1.LittleRed,
	state *redisclient.SentinelClusterState,
	redisMap map[string]string,
	password string,
) error {
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)

	quorum := 2
	if lr.Spec.Sentinel != nil && lr.Spec.Sentinel.Quorum > 0 {
		quorum = lr.Spec.Sentinel.Quorum
	}
	allowUnsafe := lr.Spec.Sentinel != nil && lr.Spec.Sentinel.AllowUnsafeRebootstrapOnDeadlock

	var since *time.Time
	if lr.Status.LeaderlessSince != nil {
		since = &lr.Status.LeaderlessSince.Time
	}
	bootstrapMasterIP := r.pickBootstrapMasterIP(lr, redisMap)

	// The decision is a pure function (planLeaderlessRecovery) so every gate and
	// tier is unit-tested without I/O. This method only executes the plan.
	plan := planLeaderlessRecovery(state, quorum, allowUnsafe, bootstrapMasterIP, since, time.Now(), leaderlessRecoveryCooldown)

	switch plan.action {
	case recoveryClearMarker:
		// Not (or no longer) a bare-sentinel deadlock. Clear any stale marker.
		return r.clearLeaderlessSince(ctx, lr, reasonRecovered, "No longer in a bare-Sentinel deadlock.")

	case recoveryStartCooldown:
		log.Info("Leaderless bootstrap deadlock suspected; starting cooldown before recovery",
			"cooldown", leaderlessRecoveryCooldown.String())
		return r.setLeaderlessSince(ctx, lr, metav1.Now())

	case recoveryWait:
		log.Info("Leaderless bootstrap deadlock persists; waiting", "holders", plan.holders,
			"cooldown", leaderlessRecoveryCooldown.String())
		return nil

	case recoverySeedNoData:
		msg := fmt.Sprintf("Leaderless recovery: no data present, seeded %s as master", redisMap[plan.masterIP])
		auditLog.Info(msg, "master", plan.masterIP, "masterPod", redisMap[plan.masterIP])
		if err := r.electMaster(ctx, lr, state, plan.masterIP, password, quorum); err != nil {
			return err
		}
		r.event(lr, corev1.EventTypeNormal, reasonReseeded, msg)
		return r.clearLeaderlessSince(ctx, lr, reasonReseeded, msg)

	case recoveryPromoteSurvivor:
		// The sole data holder — a surviving replica of a dead master. Electing it
		// loses nothing (every other pod is empty or unreachable == empty).
		h := state.RedisNodes[plan.masterIP]
		msg := fmt.Sprintf("Leaderless recovery: %s is the only pod with data (keys=%d); "+
			"promoting it as master — no data discarded", h.PodName, h.Keys)
		auditLog.Info(msg, "master", h.IP, "masterPod", h.PodName, "keys", h.Keys, "offset", h.Offset)
		if err := r.electMaster(ctx, lr, state, h.IP, password, quorum); err != nil {
			return err
		}
		r.event(lr, corev1.EventTypeNormal, reasonReseededFromSurvivor, msg)
		return r.clearLeaderlessSince(ctx, lr, reasonReseededFromSurvivor, msg)

	case recoveryRefuse:
		msg := fmt.Sprintf("Bootstrap deadlock: %d Redis pods hold data. Refusing to rebootstrap "+
			"(would discard data on all but one). Set sentinel.allowUnsafeRebootstrapOnDeadlock=true "+
			"to authorize, or intervene manually.", plan.holders)
		log.Info(msg)
		// Surface loudly and durably; keep LeaderlessSince set so the state stays visible.
		r.event(lr, corev1.EventTypeWarning, reasonRefusedDataPresent, msg)
		return r.setLeaderlessCondition(ctx, lr, metav1.ConditionTrue, reasonRefusedDataPresent, msg)

	case recoveryUnsafeElect:
		best := state.RedisNodes[plan.masterIP]
		msg := fmt.Sprintf("UNSAFE rebootstrap: force-elected %s (keys=%d, offset=%d) as master; data on %d other "+
			"pod(s) will be DISCARDED via full resync", best.PodName, best.Keys, best.Offset, plan.holders-1)
		if plan.diverged {
			msg += ". WARNING: data-holding pods span multiple replication lineages — offsets are not comparable " +
				"and genuinely independent writes will be lost"
		}
		auditLog.Info(msg, "master", best.IP, "masterPod", best.PodName, "keys", best.Keys, "offset", best.Offset,
			"candidates", plan.holders, "divergedLineages", plan.diverged)
		if err := r.electMaster(ctx, lr, state, best.IP, password, quorum); err != nil {
			return err
		}
		r.event(lr, corev1.EventTypeWarning, reasonUnsafeRebootstrap, msg)
		return r.clearLeaderlessSince(ctx, lr, reasonUnsafeRebootstrap, msg)
	}
	return nil
}

// electMaster makes masterIP the master and points every Sentinel at it. If the
// elected pod is a reachable replica (a surviving replica of a dead master, in the
// leaderless case) it is first promoted with REPLICAOF NO ONE — SENTINEL MONITOR
// alone would not promote it, and Rule R skips the elected master, so nothing else
// would. An unreachable / wait-looping elect (the no-data reseed) starts fresh as
// master via its own startup script, so no promotion is issued.
func (r *LittleRedReconciler) electMaster(ctx context.Context, lr *littleredv1alpha1.LittleRed, state *redisclient.SentinelClusterState, masterIP, password string, quorum int) error {
	if needsPromotion(state, masterIP) {
		auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
		auditLog.Info("Promoting elected pod to master (REPLICAOF NO ONE)", "master", masterIP, "wasRole", state.RedisNodes[masterIP].Role)
		addr := fmt.Sprintf("%s:%d", masterIP, littleredv1alpha1.RedisPort)
		if err := redisclient.SlaveOf(ctx, addr, password, "", "", lr.Spec.TLS.Enabled); err != nil {
			return fmt.Errorf("promote elected master %s: %w", masterIP, err)
		}
	}
	_, _, err := r.seedSentinelsWithMaster(ctx, lr, masterIP, password, quorum)
	return err
}

// pickBootstrapMasterIP returns the IP to elect as master for a no-data rebootstrap.
// It prefers redis-0 (the canonical master slot); if redis-0 has no IP yet it falls
// back to the reachable pod with the lowest-ordinal name for determinism.
func (r *LittleRedReconciler) pickBootstrapMasterIP(lr *littleredv1alpha1.LittleRed, redisMap map[string]string) string {
	pod0Name := fmt.Sprintf("%s-redis-0", lr.Name)
	fallbackIP, fallbackName := "", ""
	for ip, name := range redisMap {
		if name == pod0Name {
			return ip
		}
		if fallbackName == "" || name < fallbackName {
			fallbackName, fallbackIP = name, ip
		}
	}
	return fallbackIP
}

// Reasons for the LeaderlessRecovery status condition.
const (
	reasonDeadlockDetected     = "DeadlockDetected"
	reasonRefusedDataPresent   = "RefusedDataPresent"
	reasonReseeded             = "Reseeded"
	reasonReseededFromSurvivor = "ReseededFromSurvivor"
	reasonUnsafeRebootstrap    = "UnsafeRebootstrap"
	reasonRecovered            = "Recovered"

	// Sentinel master-name scoping reasons. The master name is the only isolation
	// boundary Sentinel's gossip protocol has, so an instance that never chose one
	// shares an identity with every other unscoped instance on the pod network.
	reasonSentinelMasterNameUnscoped = "SentinelMasterNameUnscoped"
	reasonSentinelMasterNameScoped   = "SentinelMasterNameScoped"
)

// setLeaderlessSince stamps Status.LeaderlessSince and sets the LeaderlessRecovery
// condition to True/DeadlockDetected (retry on conflict). No-op if already stamped.
func (r *LittleRedReconciler) setLeaderlessSince(ctx context.Context, lr *littleredv1alpha1.LittleRed, t metav1.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.LeaderlessSince != nil {
			return nil
		}
		latest.Status.LeaderlessSince = &t
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    littleredv1alpha1.ConditionLeaderlessRecovery,
			Status:  metav1.ConditionTrue,
			Reason:  reasonDeadlockDetected,
			Message: "All Sentinels are bare and no master is known; waiting out the recovery cooldown.",
		})
		lr.Status.LeaderlessSince = &t
		return r.Status().Update(ctx, latest)
	})
}

// setLeaderlessCondition updates the LeaderlessRecovery condition without touching
// LeaderlessSince (used for the refuse-and-wait state). Retry on conflict.
func (r *LittleRedReconciler) setLeaderlessCondition(ctx context.Context, lr *littleredv1alpha1.LittleRed, status metav1.ConditionStatus, reason, message string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionLeaderlessRecovery, Status: status, Reason: reason, Message: message,
		})
		return r.Status().Update(ctx, latest)
	})
}

// clearLeaderlessSince resets Status.LeaderlessSince once the instance is no longer
// in the bare-sentinel deadlock, recording the outcome on the LeaderlessRecovery
// condition (Status=False). No-op if never deadlocked (LeaderlessSince already nil),
// so healthy instances never grow a spurious condition.
func (r *LittleRedReconciler) clearLeaderlessSince(ctx context.Context, lr *littleredv1alpha1.LittleRed, reason, message string) error {
	if lr.Status.LeaderlessSince == nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.LeaderlessSince == nil {
			return nil
		}
		latest.Status.LeaderlessSince = nil
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionLeaderlessRecovery, Status: metav1.ConditionFalse, Reason: reason, Message: message,
		})
		lr.Status.LeaderlessSince = nil
		return r.Status().Update(ctx, latest)
	})
}
