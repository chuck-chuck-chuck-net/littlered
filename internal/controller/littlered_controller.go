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
	"sort"
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
	reasonAllPodsReady = "AllPodsReady"
	reasonInitialized  = "Initialized"
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

	// APIReader is an UNCACHED reader (mgr.GetAPIReader()) — the same `get pods`
	// permission as Client, just not served from the informer cache. It exists for
	// the one class of decision where a stale cached Status.PodIP is not merely slow
	// but unsafe: introducing an address to the cluster with CLUSTER MEET (LR-043).
	// Used only on the MEET paths (partition healing, bootstrap, migration Meet),
	// never in the steady reconcile loop. SetupWithManager defaults it from the
	// manager, so production cannot omit it; unit/envtest reconcilers that skip
	// SetupWithManager leave it nil and fall back to Client (itself a direct,
	// uncached client there) — see (*LittleRedReconciler).apiReader.
	APIReader client.Reader

	// Background fast-detection monitors. monitorEvents is the mode-agnostic
	// GenericEvent channel wired into SetupWithManager (both the sentinel
	// +switch-master subscriber and the failover-mode master watcher push onto
	// it). monitors holds the sentinel
	// subscribers, failoverMonitors the failover-mode watchers — separate maps
	// because the mode-mismatch stop branches in Reconcile are per-kind (a
	// shared map could not tell WHICH monitor runs under a key across a mode
	// switch). Both share monitorsMu (bookkeeping only, no contention).
	monitorEvents    chan event.GenericEvent
	monitors         map[types.NamespacedName]func()
	failoverMonitors map[types.NamespacedName]func()
	monitorsMu       sync.Mutex

	// stalledFailover remembers which instances have already been reported as
	// carrying a stalled Sentinel failover (LR-060), so the operator says it once
	// per transition rather than on all ~84 passes of the window — LR-042's lesson.
	//
	// In memory, deliberately: this is a report, nothing reads it back, and it
	// gates no decision, so it fails safe in the only direction it can (an operator
	// restart mid-stall costs one repeated line). Persisting it would be a status
	// field for a once-in-a-lifetime event, which LR-050 established is a cost.
	stalledFailover   map[types.NamespacedName]bool
	stalledFailoverMu sync.Mutex
}

// noteStalledFailover reports, once per transition, that a Sentinel is reporting a
// failover that can never progress, and that the operator is therefore no longer
// standing down for it.
//
// This is a genuine Sentinel-side fault rather than a transient: the wedged
// Sentinel is stuck in RECONF_SLAVES with no timer, and sentinelStartFailoverIfNeeded
// refuses to begin another failover for a master that still carries
// SRI_FAILOVER_IN_PROGRESS — so it is also out of action as a failover initiator
// until something RESETs it. The documented remedy is a `SENTINEL RESET`.
func (r *LittleRedReconciler) noteStalledFailover(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, state *redisclient.ReplicationState,
) {
	key := types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}

	var stalled []string
	for _, sn := range state.SentinelNodes {
		if sn.FailoverStalled() {
			stalled = append(stalled, sn.PodName)
		}
	}
	sort.Strings(stalled)

	r.stalledFailoverMu.Lock()
	already := r.stalledFailover[key]
	if r.stalledFailover == nil {
		r.stalledFailover = map[types.NamespacedName]bool{}
	}
	r.stalledFailover[key] = true
	r.stalledFailoverMu.Unlock()
	if already {
		return
	}

	msg := fmt.Sprintf("Sentinel(s) %v report a failover stuck in %q with the promoted replica down. "+
		"It cannot end on its own and that Sentinel cannot start another failover for this master. "+
		"The operator is NOT standing down for this report; healing continues. "+
		"Clear it with `SENTINEL RESET <master-name>` against the named pod(s).",
		stalled, "reconf_slaves")
	r.getLogger(ctx, lr, LogCategoryState).Info("Sentinel failover is stalled; ignoring the report",
		"sentinels", stalled)
	r.event(lr, corev1.EventTypeWarning, "SentinelFailoverStalled", msg)
}

// clearStalledFailover forgets the report so a later stall is announced again.
func (r *LittleRedReconciler) clearStalledFailover(lr *littleredv1alpha1.LittleRed) {
	key := types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}
	r.stalledFailoverMu.Lock()
	delete(r.stalledFailover, key)
	r.stalledFailoverMu.Unlock()
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

	// Reconcile based on mode: stop the fast-detection monitor of any mode
	// this instance is NOT in (mode switch cleanup; per-kind maps, so a
	// sentinel subscriber and a failover watcher can never survive each other).
	if littleRed.Spec.Mode != ModeSentinel {
		r.stopSentinelMonitor(req.NamespacedName)
	}
	if littleRed.Spec.Mode != ModeFailover {
		r.stopFailoverMonitor(req.NamespacedName)
	}

	// Initialize BootstrapRequired for the operator-led-bootstrap modes: sentinel
	// (pillar 3.6) and failover (ADR-011 §3 — same contract, the assignment
	// annotations replace the Sentinel registration).
	if (littleRed.Spec.Mode == ModeSentinel || littleRed.Spec.Mode == ModeFailover) &&
		littleRed.Status.Phase == "" && !littleRed.Status.BootstrapRequired {
		log.Info("Initializing new instance: setting bootstrapRequired flag", "mode", littleRed.Spec.Mode)
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
	case ModeFailover:
		return r.reconcileFailover(ctx, littleRed)
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

	// Stop background monitors if running
	nn := types.NamespacedName{
		Name:      littleRed.Name,
		Namespace: littleRed.Namespace,
	}
	r.stopSentinelMonitor(nn)
	r.stopFailoverMonitor(nn)

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

// requeueAfterNotRunning is the pure decision behind the not-Running requeue cadence,
// shared by every mode-specific status function (LR-045; see also LR-042). While an
// instance is CONVERGING the fast cadence is load-bearing: the healing rules are
// driven by these iterations (LR-012/014/017). The one exception is an instance the
// operator has declared Forsaken — captured by another Sentinel deployment sharing its
// master name, recovery declined by design (ADR-015 §9.2) — which is re-examined at
// the steady cadence instead, since the operator has already stopped managing it and
// polling it fast forever buys nothing but log noise.
//
// Only sentinel mode can ever be Forsaken today, but the predicate is written against
// the generic (phase, conditions) shape so every future mode-specific status path is
// safe by construction rather than by omission.
// A declared heavy operation (ADR-020) is the second exception, and it pulls the other
// way: while one is RUNNING the operator has suppressed the instance's regular healing,
// so it must see the operation finish promptly — every steady interval it waits is an
// extra interval of suppression, and the acknowledgment that ends it can only be written
// on a pass. That is why this clause outranks the phase check: an instance mid-operation
// is frequently Running (its pods are healthy between rollout waves) and would otherwise
// be polled at the steady cadence for the whole window.
//
// It is deliberately narrow — only Running. A Blocked, Stalled or Quarantined operation
// is permanent until a human acts (there is no auto-exit timer, ADR-017), so polling it
// fast forever buys nothing but log noise: LR-042's lesson, in the same shape as
// Forsaken above.
func requeueAfterNotRunning(
	phase littleredv1alpha1.LittleRedPhase, conditions []metav1.Condition, fast, steady time.Duration,
) time.Duration {
	if c := meta.FindStatusCondition(conditions, littleredv1alpha1.ConditionOperationInProgress); c != nil &&
		c.Status == metav1.ConditionTrue && c.Reason == operationReasonRunning {
		return fast
	}
	if phase == littleredv1alpha1.PhaseRunning {
		return steady
	}
	if meta.IsStatusConditionTrue(conditions, littleredv1alpha1.ConditionForsaken) {
		return steady
	}
	return fast
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
				Reason:             reasonAllPodsReady,
				Message:            "All pods are ready",
				LastTransitionTime: metav1.Now(),
			})
			meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionInitialized,
				Status:             metav1.ConditionTrue,
				Reason:             reasonInitialized,
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

	// Requeue if not running to check status. See requeueAfterNotRunning (LR-045,
	// correcting LR-042) for the cadence choice, shared with updateSentinelStatus.
	if littleRed.Status.Phase != littleredv1alpha1.PhaseRunning {
		after := requeueAfterNotRunning(littleRed.Status.Phase, littleRed.Status.Conditions, fast, steady)
		log.Info("Not yet Running, requeueing",
			"phase", littleRed.Status.Phase,
			"redis", fmt.Sprintf("%d/%d", littleRed.Status.Redis.Ready, littleRed.Status.Redis.Total),
			"requeueAfter", after)
		return ctrl.Result{RequeueAfter: after}, nil
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

	// Desired replica counts for BOTH StatefulSets, computed once, here, before either
	// is applied (LR-044 wiring). This ordering is the whole point: r.apply is
	// server-side apply with client.ForceOwnership, so .Spec.Replicas as built below is
	// authoritative every pass. Deciding the quarantine only later (in
	// reconcileSentinelCluster, which needs the gather) would mean these two applies put
	// 3 back on every pass and the healing step took it away again — a 0→3→0 flap that
	// actually schedules pods, which briefly rejoin the captor's quorum: worse than the
	// log churn LR-042 removed. So zero must genuinely BE the desired state.
	//
	// The decision is therefore taken from status alone, which the pure planner supports
	// by design: an armed quarantine is decided from status.quarantinedSince FIRST and
	// without reference to the capture verdict (the verdict provably self-clears once the
	// pods are gone). ARMING still happens after the gather, where the verdict and the
	// data-risk classification live.
	redisReplicas, sentinelReplicas := sentinelDesiredReplicas(littleRed, time.Now())

	// Reconcile Redis StatefulSet
	if err := r.reconcileRedisStatefulSetSentinel(ctx, littleRed, redisReplicas); err != nil {
		log.Error(err, "Failed to reconcile Redis StatefulSet")
		return ctrl.Result{}, err
	}

	// Reconcile Sentinel StatefulSet
	if err := r.reconcileSentinelStatefulSet(ctx, littleRed, sentinelReplicas); err != nil {
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

// reconcileRedisStatefulSetSentinel ensures the Redis StatefulSet exists for sentinel mode,
// at the desired replica count its caller computed (see sentinelDesiredReplicas).
func (r *LittleRedReconciler) reconcileRedisStatefulSetSentinel(
	ctx context.Context, littleRed *littleredv1alpha1.LittleRed, replicas int32,
) error {
	return r.apply(ctx, littleRed, buildRedisStatefulSetSentinel(littleRed, replicas))
}

// reconcileSentinelStatefulSet ensures the Sentinel StatefulSet exists, at the desired
// replica count its caller computed (see sentinelDesiredReplicas).
func (r *LittleRedReconciler) reconcileSentinelStatefulSet(
	ctx context.Context, littleRed *littleredv1alpha1.LittleRed, replicas int32,
) error {
	return r.apply(ctx, littleRed, buildSentinelStatefulSet(littleRed, replicas))
}

// sentinelProcessReplicas is the fixed number of Sentinel processes in sentinel mode.
// Mirrors littleredv1alpha1.SentinelRedisReplicas (3 Redis pods) for the monitoring side:
// the quorum is fixed at three, sentinel HA is not horizontally scalable. Kept local
// because the API package has no such constant and the sentinel StatefulSet is the only
// consumer.
const sentinelProcessReplicas int32 = 3

// sentinelDesiredReplicas is the single source of truth for the sentinel-mode Redis and
// Sentinel StatefulSet replica counts. Pure: the CR plus a clock, no I/O.
//
// Normally it is simply the fixed sentinel shape (3 Redis pods, 3 Sentinels). The one
// thing that changes it is an ARMED quarantine (LR-044), and this function is why zero
// genuinely IS the desired state rather than a value the next apply overwrites: it is
// called before either StatefulSet is applied, and r.apply is server-side apply with
// client.ForceOwnership.
//
// It deliberately passes NO capture verdict to the planner. That is the planner's
// documented pre-gather contract — an armed quarantine is decided from
// status.quarantinedSince first and without reference to the verdict, because the verdict
// provably self-clears once the pods are gone (no reachable monitoring Sentinel ⇒
// planForsaken clause 1 fails). Arming still happens after the gather, in
// reconcileSentinelCluster, where the verdict and the data-risk clauses live.
//
// Two safety properties, both pinned by tests:
//
//   - A fresh instance can never read as quarantined: with status.quarantinedSince unset
//     (nil, or a zero-valued marker) the planner's armed branch is not taken at all, and
//     nothing else in this input can produce ScaleToZero (Captured is false).
//   - Only sentinel mode can ever be scaled down by this. A quarantine marker left in
//     status by a mode change, or hand-edited in, cannot take a cluster-, failover- or
//     standalone-mode instance's pods away — the mode gate is checked here rather than
//     relying on these builders happening to be sentinel-only callers.
func sentinelDesiredReplicas(lr *littleredv1alpha1.LittleRed, now time.Time) (redis, sentinel int32) {
	redis, sentinel = littleredv1alpha1.SentinelRedisReplicas, sentinelProcessReplicas
	if lr.Spec.Mode != ModeSentinel {
		return redis, sentinel
	}
	q := planQuarantine(quarantineInput{
		QuarantinedSince: lr.Status.QuarantinedSince,
		Attempts:         lr.Status.QuarantineAttempts,
		Dangerous:        quarantineConfigDangerous(lr),
		Now:              now,
	})
	if q.ScaleToZero {
		return 0, 0
	}
	return redis, sentinel
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

	// Two sets, because there are two questions (LR-053).
	//
	// redisMap is the LIVE TOPOLOGY: what we probe, and what every "is anything of
	// ours alive there" judgement reads. Terminating pods are filtered out and must
	// stay filtered out — LR-038 established that a reachable, still-mastering
	// terminating pod in the gather reads as a live master to the failover-mode
	// planner and as an election candidate to BestDataHolder.
	//
	// ownedIPs is ATTRIBUTION: every address a pod of ours holds, terminating
	// included, because for "is this address ours?" a terminating pod of ours is
	// still ours. Built from the same two unfiltered pod lists the maps below are
	// built from, so the two views cannot disagree about which pods exist.
	redisMap := make(map[string]string)
	ownedIPs := make(map[string]bool, len(redisPods.Items)+len(sentinelPods.Items))
	for _, p := range redisPods.Items {
		if p.Status.PodIP == "" {
			continue
		}
		ownedIPs[p.Status.PodIP] = true
		if p.DeletionTimestamp.IsZero() {
			redisMap[p.Status.PodIP] = p.Name
		}
	}

	// Kubelet readiness of each Redis pod's redis container, keyed by pod name. This is
	// the data-safety signal the quarantine's "could not be proven empty" clause is keyed
	// on (LR-044 wiring): the kubelet's local probe is authoritative and blackhole-proof
	// where the operator's own remote dial is not (LR-017), and in a pure in-memory
	// instance a not-Ready redis holds no data (LR-023). Built from the pod list, which
	// is the same list the gather is built from, so the two cannot disagree about which
	// pods exist.
	redisReady := make(map[string]bool, len(redisPods.Items))
	for i := range redisPods.Items {
		redisReady[redisPods.Items[i].Name] = redisContainerReady(&redisPods.Items[i])
	}

	sentinelMap := make(map[string]string)
	for _, p := range sentinelPods.Items {
		if p.Status.PodIP == "" {
			continue
		}
		ownedIPs[p.Status.PodIP] = true
		if p.DeletionTimestamp.IsZero() {
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

	// Is our OWN Redis StatefulSet settled? (LR-050)
	//
	// This is the one input that separates "an address that is not one of our pods" from
	// "an address that is not one of our pods ANY MORE". While the StatefulSet is mid-roll
	// — or simply short of its Ready count — a pod of ours has just left and its address is
	// still in the air: its pod OBJECT is gone, so nothing can attribute the address to us,
	// and Sentinel does not flag it down for a whole down-after-milliseconds. That reading is
	// byte-identical to a captor's live master, which is how an ORDINARY rename settled a
	// false capture verdict and quarantined a healthy instance (design §16: the verdict
	// fired at T+30 of a 42.5s window, 12.5s before the instance healed itself).
	//
	// So while this is false the operator does not ATTRIBUTE addresses: it will not arm a
	// new capture verdict (planForsaken) and it will not call a stale entry foreign
	// (Rule N's G5). It is NOT made redundant by LR-053's OwnedIPs: that set covers the
	// pod of ours that is still in the pod list (terminating), this gate covers the one
	// whose object is already GONE — no list can hold an address whose object no longer
	// exists. Both stay. It is deliberately not a licence to stop working — Rule N still runs,
	// and an already-armed verdict is neither advanced nor retracted by a roll of our own
	// making.
	//
	// Read UNCACHED, on LR-047's precedent: the dangerous direction is a stale read that
	// says "settled" while a roll is in flight, and the informer necessarily lags BEHIND —
	// at t0 of a rename it still holds the previous, settled generation. One GET per
	// sentinel pass. A StatefulSet we cannot read at all is treated as not settled, which
	// is the conservative direction for a gate whose only effect is to withhold an
	// accusation.
	rollingSts := &appsv1.StatefulSet{}
	rolling := true
	switch err := r.apiReader().Get(ctx, types.NamespacedName{
		Name: statefulSetName(littleRed), Namespace: littleRed.Namespace,
	}, rollingSts); {
	case err == nil:
		rolling = !statefulSetRolloutSettled(rollingSts)
	case apierrors.IsNotFound(err):
		// A definitive answer, not an unknown: with no StatefulSet there is no rollout
		// of ours in flight and nothing to withhold attribution for. (Unreachable on
		// the normal path — reconcileSentinel applies it well before this function.)
		rolling = false
	default:
		log.V(1).Info("Could not read the Redis StatefulSet uncached; holding address attribution",
			"error", err.Error())
	}

	// 2. Gather Cluster State (Ground Truth)
	g := &operatorGatherer{password: password, tlsEnabled: littleRed.Spec.TLS.Enabled}
	state := redisclient.GatherReplicationState(ctx, g, redisMap, sentinelMap, sentinelMasterName, ownedIPs)

	// 2a. Can we still talk to our own pods? (LR-051)
	//
	// Reported BEFORE the forsaken/quarantine switch below, because that switch
	// returns early on a capture and this is precisely the state an owner needs
	// named when the operator has otherwise gone quiet. A pod that refuses our
	// credential ANSWERED us, so it is a live server whose keyspace is unknown —
	// which is why unprovablyEmptyVeto refuses every action that discards data while
	// this holds. The report is a rendering only; no decision depends on it having
	// been computed.
	if err := r.reportOperatorAuth(ctx, littleRed, replicationAuthFailures(state)); err != nil {
		log.Error(err, "failed to report the operator's authentication state")
	}

	// 2b. Is this instance still ours to manage?
	//
	// If another Sentinel deployment sharing our master name has captured us, every
	// healing rule below is not merely useless but counterproductive: the LR-008
	// ghost-master correction would reissue REMOVE+MONITOR every reconcile and lose to
	// the captor's config epoch every time (ADR-015 §9.2). Recovery is declined by
	// design, so the right move is to stop, say so, and leave the instance loudly
	// broken for a human.
	//
	// This is a verdict about US, from OUR Sentinels' own answers — never a claim about
	// isolation, which is why it is not the controller-side collision check ADR-015
	// rejected. Silence here asserts nothing.
	forsakenLog := r.getLogger(ctx, littleRed, LogCategoryAudit)
	now := time.Now()
	forsakenPlan := planForsaken(state, littleRed.Status.ForsakenSince, now, rolling)

	// 2c. Quarantine lifecycle (LR-044). A forsaken instance is not merely beyond help,
	// it is actively harmful: its pods keep replicating from the CAPTOR's master, so the
	// captor's Sentinel failover-candidate set holds foreign pods it can promote. Taking
	// this instance's pods away removes the cause and lets the captor heal through the
	// ghost-replica pruning it already has. Nothing is reclaimed here — the instance
	// returns empty, which ADR-015 §9.2 names as the only achievable outcome anyway.
	//
	// The decision is pure; this call site only persists it. Note that the verdict above
	// self-clears once the pods are gone (no reachable monitoring Sentinel ⇒ planForsaken
	// clause 1 fails), so status.quarantinedSince and status.quarantineAttempts — not the
	// signature — are what carry the state across the quarantine.
	atRisk, unverified := quarantineDataRisk(state, forsakenPlan.ForeignMaster, redisReady)
	qPlan := planQuarantine(quarantineInput{
		Captured:         forsakenPlan.Captured,
		Forsaken:         forsakenPlan.Forsaken,
		DataAtRisk:       atRisk,
		DataUnverified:   unverified,
		QuarantinedSince: littleRed.Status.QuarantinedSince,
		Attempts:         littleRed.Status.QuarantineAttempts,
		Dangerous:        quarantineConfigDangerous(littleRed),
		Now:              now,
	})

	// 2d. Declared heavy operations (ADR-020).
	//
	// The input is assembled here — after the quarantine DECISION and before any healing
	// — because that is where the two branches part: a quarantined instance reports its
	// held operation (row 1) and returns, while every other instance runs the driver
	// first, since the driver's completion is an INPUT to the decision (rows 7-10)
	// rather than an output of it, and takes the branch below Rule N.
	//
	// Settledness spans BOTH of this instance's StatefulSets and is read uncached. It is
	// deliberately NOT LR-050's `rolling` gate above: that one names a fact about our own
	// churn and therefore also covers the image bump, the drain and the eviction that
	// nobody declares. Unifying the two is the exact mistake ADR-020 exists to prevent
	// (its trap 1), and it is tempting precisely because during a rename they fire
	// together.
	opInput := buildOperationInput(littleRed, r.instanceStatefulSetsSettled(ctx, littleRed), now)

	switch {
	case qPlan.ScaleToZero:
		// Quarantine in force — armed this pass, settling, or latched. Stop managing:
		// with the pods gone there is nothing to heal, and the desired replica count
		// is computed from the marker this records.
		//
		prevReason := ""
		if c := meta.FindStatusCondition(littleRed.Status.Conditions,
			littleredv1alpha1.ConditionForsaken); c != nil {
			prevReason = c.Reason
		}
		if err := r.setForsaken(ctx, littleRed, metav1.Now(),
			forsakenPlan.ForeignMaster, true, qPlan); err != nil {
			forsakenLog.Error(err, "failed to record the quarantine")
		}
		if prevReason != quarantineReason(qPlan.Phase) {
			// Log once per phase transition, not per reconcile.
			forsakenLog.Info("Instance is forsaken and quarantined: captured by another "+
				"Sentinel deployment sharing its master name. Halting management.",
				"foreign_master", forsakenPlan.ForeignMaster, "quarantine", string(qPlan.Phase),
				"attempt", qPlan.NextAttempts, "attempt_limit", qPlan.AttemptLimit)
		}
		// A declared heavy operation is HELD here and still REPORTED (ADR-020 row 1): a
		// replicas:0 StatefulSet reads SETTLED, so an operation allowed to proceed would
		// acknowledge work no pod ever executed — and a change held invisibly is exactly
		// what LR-054 says this mechanism must never do. Quarantined is taken from the
		// quarantine plan rather than from the marker, because on the ARMING pass the
		// marker is written by setForsaken above and the input was built before it.
		opInput.Quarantined = true
		if _, err := r.reconcileOperation(ctx, littleRed, opInput); err != nil {
			forsakenLog.Error(err, "failed to report the held heavy operation")
		}
		return nil
	case forsakenPlan.Captured:
		if err := r.setForsaken(ctx, littleRed, metav1.Now(),
			forsakenPlan.ForeignMaster, forsakenPlan.Forsaken, qPlan); err != nil {
			forsakenLog.Error(err, "failed to record the capture")
		}
		if forsakenPlan.Forsaken {
			// Forsaken but NOT quarantined: the only way here is a data clause
			// refusing (a pod holds data the captor does not have, or a pod could not
			// be proven empty). Log once per transition, not per reconcile.
			if !meta.IsStatusConditionTrue(littleRed.Status.Conditions, littleredv1alpha1.ConditionForsaken) {
				forsakenLog.Info("Instance is forsaken: captured by another Sentinel deployment "+
					"sharing its master name. Halting management.",
					"foreign_master", forsakenPlan.ForeignMaster, "quarantine", string(qPlan.Phase))
			}
			return nil
		}
	default:
		if err := r.clearForsaken(ctx, littleRed, qPlan); err != nil {
			forsakenLog.Error(err, "failed to clear the capture verdict")
		}
	}

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

	// Rule N: prune stale Sentinel master names (LR-048).
	//
	// Placed AFTER Rule 0 so the desired name is already registered on a bare Sentinel in
	// this same pass — that is what makes the two-name window intra-pass rather than
	// multi-pass, and what makes the prune's own precondition (G6) pass on the first
	// attempt.
	//
	// Placed BEFORE Rule A, i.e. it runs while anyTerminating is true, and that is
	// deliberate rather than an oversight. The churn Rule A sits out is EXACTLY when a
	// rename is in flight: editing spec.sentinel.masterName rewrites the Redis pod
	// template, so a pod is terminating from the moment of the edit. Gating on
	// !anyTerminating would hold the two-name window open for the whole multi-minute
	// roll — the one window in which redis-0's baked, stale-name preStop can fire a real
	// `SENTINEL failover <old>` under the old name, which was MEASURED doing exactly that
	// (design §4.1: two names naming two different live pods as master for 56.6s).
	//
	// LR-040's actual lesson applies in full and is discharged rather than inherited: an
	// action that runs during churn must be BOUNDED. Every call this rule makes goes
	// through newBoundedClient (Dial/Read/WriteTimeout = ProbeTimeout) AND carries a
	// per-call context deadline; a ctx alone is inert against go-redis.
	//
	// G0 is fed the verdict this pass ALREADY computed (never re-derived, never computed
	// twice), and it is fed `Captured` rather than `Forsaken` deliberately: a settled
	// Forsaken returns from the switch above long before this line, so passing it would
	// make the gate structurally dead. `Captured` is the reachable — and strictly more
	// conservative — reading of "a capture is in evidence": while one is, Rule N stands
	// down entirely and ADR-016 owns the instance.
	stalePlan, err := r.reconcileStaleMasterNames(ctx, littleRed, state, sentinelMasterName,
		sentinelQuorum(littleRed), password, forsakenPlan.Captured, rolling)
	if err != nil {
		auditLog.Error(err, "failed to reconcile stale Sentinel master names")
	}

	// The declared-operation decision (ADR-020). Rule 0 and Rule N together ARE registry
	// v1's driver — no new healing logic exists anywhere in this mechanism, and the
	// driver's verdict is Rule N's own plan — so the decision is taken here, after them,
	// because a driver's completion is an INPUT to it (rows 7-10) and not an output.
	//
	// THE BOUNDARY IS AUTHORITY ASSIGNMENT, NOT "HEALING" (ADR-020), and getting it wrong
	// cost a measured 145s. An earlier build of this branch returned here, one gate ahead
	// of Rule A, on the reasoning that during a rename a pod is terminating from the
	// moment of the edit so Rule A already returned before every suppressed rule. That
	// holds only while something is TERMINATING. Once the last replacement pod is created
	// and nothing is terminating, Rule A lets healing run and the blanket return did not —
	// a suppression strictly longer than Rule A's, whose extra window is exactly when the
	// instance needs help converging.
	//
	//	During a heavy operation the operator does not ASSIGN AUTHORITY. It may
	//	PROPAGATE an authority decision already made, and it may CLEAN UP DEBRIS.
	//	Everything else runs under its normal guards.
	//
	// Assigning authority means creating a new fact about who holds data — in sentinel
	// mode, which pod the quorum monitors as master. Propagating means making reality
	// match a decision that already exists. So the suppressed set here is exactly two
	// rules, Rule L and the LR-024 recovery, and it is a strict subset of Rule A's.
	//
	// An earlier formulation of this comment said "convergence versus rescue" and is
	// RETRACTED: the classification cannot be applied by reading rule names, and that
	// naming got it backwards — Rule R is literally called "Replica Rescue" and assigns
	// nothing at all. Each rule's classification is therefore declared where the rule
	// lives, with its reason, and never collected into a central list to keep in sync.
	//
	// The narrowing is implemented by NOT returning: every other rule reaches Rule A
	// exactly as it always did and Rule A's own guards (!anyTerminating, !FailoverActive)
	// still apply to them unchanged. Hoisting any of them earlier is not on the table —
	// those guards exist for their own reasons.
	opDone, opBlocked := operationDriverReport(stalePlan)
	opInput.DriverDone, opInput.DriverBlocked = opDone, opBlocked
	opPlan, err := r.reconcileOperation(ctx, littleRed, opInput)
	if err != nil {
		auditLog.Error(err, "failed to record the declared heavy operation")
	}
	operationRunning := opPlan.Run != ""
	if operationRunning {
		log.Info("A declared heavy operation is in progress. Authority-assigning rules "+
			"stand down; everything else runs under its normal guards.",
			"operation", opPlan.Run, "reason", opPlan.Reason, "pending", opPlan.Pending)
	}

	// Rule A: Guardrails
	// We skip ALL healing if:
	// 1. Any pod is terminating (K8s is already working).
	// 2. Sentinel reports a failover that can still PROGRESS (Sentinel is already
	//    working). NOT merely "a failover is reported" — LR-060. A Sentinel latched
	//    in RECONF_SLAVES reports one that can never end, and honouring that report
	//    means standing down forever; FailoverProgressing is false there so the
	//    operator resumes. See FailoverStalled for the predicate and its concessions.
	//
	// Note what this guard does NOT stop any more: Rule R. It is below this line and
	// therefore still governed by clause 1, but clause 2 no longer reaches it — the
	// narrowing is at the Rule R site itself (planReplicaRescue), because Rule R
	// issues the same command at the same target Sentinel's own reconf_slaves is
	// issuing and is therefore incapable of fighting it. ADR-003's amendment of
	// 2026-09-03 carries the argument.
	if anyTerminating || state.FailoverProgressing {
		// ...EXCEPT Rule R, when a reported failover is the only reason to stand
		// down (LR-060). Rule R issues the same command at the same target that
		// Sentinel's own reconf_slaves is issuing, so it cannot fight the failover
		// — and a failover held open by an un-reconfigured replica is one only Rule
		// R can discharge, which is why suppressing it cost a measured 179s of an
		// entirely unmanaged instance. It has to be invoked HERE rather than left in
		// its usual position, because that position is below this return.
		//
		// anyTerminating still suppresses it, unchanged: that clause is about
		// Kubernetes churn, and Rule R's argument says nothing about it.
		if !anyTerminating {
			r.applyReplicaRescue(ctx, littleRed, state, password)
		}
		log.Info("Cluster transition in progress. Skipping healing.",
			"anyTerminating", anyTerminating,
			"failoverProgressing", state.FailoverProgressing,
			"ruleRStillRuns", !anyTerminating)
		return nil
	}

	// LR-060: a reported-but-stalled failover is a real Sentinel-side fault — that
	// Sentinel is wedged in RECONF_SLAVES and, per sentinelStartFailoverIfNeeded,
	// can never start another failover for this master until something RESETs it.
	// The operator now declines to honour the report, so say so ONCE per transition
	// (LR-042's lesson) rather than every pass. Deliberately a log line and an event
	// and NOT a new condition: this is a rare, self-describing state, and LR-050
	// established that status-surface inflation for a once-in-a-lifetime event is a
	// cost rather than a feature.
	if state.FailoverReported && !state.FailoverProgressing {
		r.noteStalledFailover(ctx, littleRed, state)
	} else {
		r.clearStalledFailover(littleRed)
	}

	// Note: state.RealMasterIP == "" (leaderless) used to be a hard early return here.
	// Reconciliation now proceeds past this point so that the leaderless branch below —
	// Rule L (LR-015) and the LR-024 ghost-master recovery — can own that state. It is NOT
	// so that ghost pruning can act while leaderless: Rule D's SENTINEL RESET and the
	// LR-005/LR-008 REMOVE+MONITOR correction are both gated on a living, reachable
	// consensus master, which is precisely what leaderless means we do not have.
	// ("Rule A+" was an early alias for Rule D and is retired; LR-001/LR-007 established
	// by incident that pruning during a leaderless period resets Sentinel's s_down timer
	// and blocks failover indefinitely.)

	// Ghost pruning: only safe if the master Sentinel reports is a living pod
	// CLASSIFICATION of the loop below: the LR-005 / LR-008 ghost-master correction
	// PROPAGATES an existing authority decision (ADR-020) and is NOT gated on
	// operationRunning. It re-points a Sentinel
	// that has lost its failover notification at the master the rest of the quorum already
	// agrees on — our own, living, reachable pod, established by the same gate chain
	// LR-008 wrote. It makes reality match a decision the quorum already made rather than
	// making a new one, so it cannot contradict a declared change; and standing it down would
	// leave a diverged Sentinel diverged for the whole operation, which is how a rename
	// ends up with two quorums (LR-048's measured 56.6s of two live masters).
	//
	// The ghost-REPLICA scan further down this same loop is detection only — it sets
	// ghostFound and issues nothing. The action it feeds is Rule D, gated at its own site.
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
		//
		// CLASSIFICATION: both ASSIGN AUTHORITY (ADR-020) — Rule L seeds redis-0 or
		// promotes a survivor, the LR-024 recovery force-elects one via REMOVE + MONITOR
		// + REPLICAOF NO ONE. That classification is correct and is NOT why they used to
		// be suppressed during a declared operation; the suppression was removed in
		// LR-059 because the boundary was testing the wrong property.
		//
		// A DECLARED OPERATION NO LONGER STANDS THESE DOWN, and the reason generalises
		// LR-058's rule. Suppression has no auto-exit by design (rows 7, 9 and 10 all
		// keep the operation Running, and a Stalled operation is deliberately not
		// auto-exited — ADR-017: a timer is the defect with a delay). So the boundary has
		// to be closed under "forever":
		//
		//	a rule may be stood down by an operation only if the instance can still
		//	reach a SETTLED state with that rule permanently absent.
		//
		// Rule L fails that test on exactly this branch, and the cycle is closed: no
		// master seeded ⇒ the pods park in the startup wait-loop ⇒ never Ready ⇒
		// ReadyReplicas != Replicas ⇒ the StatefulSets never settle ⇒ row 7 withholds
		// the acknowledgment ⇒ the operation stays Running ⇒ Rule L stays suppressed.
		// Measured on t3e: 74s to recover with no rename pending, WEDGED 7m56s with one,
		// 84s once it was reverted (LR-059).
		//
		// Two things make removing it the whole fix rather than a relaxation. The gate
		// lived HERE, inside `RealMasterIP == ""`, and both rules require that state — so
		// its entire domain of effect was the leaderless state, where there is no
		// authority to withhold, only one to establish. And a capture cannot widen that
		// domain: planForsaken's clauses 3 and 4 imply `RealMasterIP == ""` (the agreed
		// address is not ours, so DetermineRealMaster's step 3 cannot match it, and no
		// reachable pod of ours is a master, so step 4's fallback finds nothing), while a
		// settled Forsaken verdict and an armed quarantine both return well above this
		// line. In the one capture state that does reach here — Captured inside its
		// cooldown — both rules already veto themselves (Rule L needs AllSentinelsBare,
		// false while the Sentinels monitor the captor; the LR-024 recovery needs
		// !HasHealthyKnownReplica, and our own live pods sit in the captor's replica
		// list). So the refusal moves from the operation gate to each rule's own gates,
		// which are the ones designed for it.
		//
		// The return stays unconditional: with no consensus master nothing below can run
		// anyway (Rule R would issue SLAVEOF at an empty address).
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

	// CLASSIFICATION: Rule D CLEANS UP DEBRIS. It does NOT assign authority, and that is a
	// ledger fact rather than a judgement call: LR-007 established BY INCIDENT that
	// SENTINEL RESET does not change the monitored master IP — it clears the replica and
	// sentinel lists and nothing else — which is precisely why LR-008 had to introduce
	// REMOVE + MONITOR to repoint a stuck Sentinel. Rule D is therefore STRUCTURALLY
	// INCAPABLE of deciding who is master, so an operation has no reason to stand it down
	// (ADR-020). It reads like a topology operation and is one, but not of the kind that
	// decides.
	//
	// It is consequently NOT gated on operationRunning, and un-gating it is what closed a
	// measured 145s regression. Gated, an ordinary rename settled in ~308s against a
	// ~166s baseline: the early ghost prune (measured at +47s on the pre-M3.1 control) is
	// what stops Sentinel's failover state machine wedging in RECONF_SLAVES, and once it
	// wedges, state.FailoverProgressing pins Rule A above — so every rule below that line,
	// including this one, is unreachable until Sentinel's own timers clear it ~178s
	// later. Rule D cannot BREAK that latch (it sits after Rule A); it prevents it from
	// forming. See LR-055.
	//
	// Its own gate chain is what makes firing it safe here and is unchanged: a living,
	// reachable consensus master (LR-008), at least one healthy known replica (LR-011),
	// and a K8s-grounded whole instance (LR-013). An operation supplies no information
	// those three clauses lack.
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

	// Rule R: Replica Rescue.
	// Ensure all living Redis pods that are not the consensus master are actually
	// configured as replicas.
	//
	// CLASSIFICATION: Rule R PROPAGATES an authority decision that already exists (ADR-020),
	// and THE NAME LIES — "Replica Rescue" reads like a rescue and assigns nothing at all.
	// It issues one idempotent SLAVEOF pointing a pod at the consensus master the quorum
	// has ALREADY chosen. It destroys nothing, elects nothing and decides nothing: the
	// target is an input.
	//
	// It is therefore NOT gated on operationRunning, and that is the whole of the M3.1
	// regression. A replaced pod comes back following the old master with link:down;
	// sentinel-mode readiness needs role:master or link:up (LR-016), so it stays unready,
	// so the StatefulSet stays unsettled, so the operation stays pending — and gating
	// this rule on the operation closes that loop with no exit but Sentinel's own timers
	// (measured: +180s on the first-replaced pod). Rule A's guards above still apply to
	// it exactly as they always have.
	r.applyReplicaRescue(ctx, littleRed, state, password)

	return nil
}

// applyReplicaRescue executes Rule R: point every pod that is not following the
// consensus master at it.
//
// The decision is the pure planReplicaRescue (LR-060). The trigger it carries is
// LR-010's, unchanged — a definitively wrong Role or MasterHost, never LinkStatus
// alone, so a replica mid-sync from the correct master is left alone.
func (r *LittleRedReconciler) applyReplicaRescue(
	ctx context.Context, littleRed *littleredv1alpha1.LittleRed,
	state *redisclient.ReplicationState, password string,
) {
	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)
	for _, rn := range planReplicaRescue(state) {
		auditLog.Info("Redis pod is not following the consensus master, issuing SLAVEOF",
			"pod", rn.PodName, "current_role", rn.Role, "target_master", state.RealMasterIP)
		if err := redisclient.SlaveOf(ctx, fmt.Sprintf("%s:%d", rn.IP, littleredv1alpha1.RedisPort), password, state.RealMasterIP, fmt.Sprintf("%d", littleredv1alpha1.RedisPort), littleRed.Spec.TLS.Enabled); err != nil {
			auditLog.Error(err, "Failed to rescue replica", "pod", rn.PodName)
		}
	}
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

	return r.applyRoleLabels(ctx, littleRed, podList, masterPodName)
}

// applyRoleLabels performs the surgical role-label updates shared by sentinel
// and failover mode (the flip mechanics; only the master SOURCE differs —
// Sentinel consensus vs operator intent):
//  1. If we have a masterPodName, ensure ONLY that pod is labeled Master.
//  2. If we DON'T have a masterPodName, ensure NO pod is labeled Master.
//  3. We only change Replica/Orphan labels if we are sure of the state.
//     During failover (masterPodName == ""), we just strip the Master label
//     from whoever had it and leave others alone.
func (r *LittleRedReconciler) applyRoleLabels(ctx context.Context, littleRed *littleredv1alpha1.LittleRed, podList *corev1.PodList, masterPodName string) error {
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
			// No master identified (failover in progress).
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
		// Redis.Total is 0 while the instance is quarantined (LR-044), so this must not
		// go negative: "master minus one" is meaningless when there is no master.
		latest.Status.Replicas.Total = 0
		if latest.Status.Redis.Total > 0 {
			latest.Status.Replicas.Total = latest.Status.Redis.Total - 1
		}

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
				Reason:             reasonAllPodsReady,
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
				Reason:             reasonInitialized,
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

	// Requeue if not running. See requeueAfterNotRunning (LR-045, correcting LR-042):
	// this is the path every sentinel-mode instance actually returns through, so it
	// must honour the same Forsaken-aware cadence choice as updateStatus, not just the
	// generic non-sentinel path.
	if latest.Status.Phase != littleredv1alpha1.PhaseRunning {
		after := requeueAfterNotRunning(latest.Status.Phase, latest.Status.Conditions, fast, steady)
		return ctrl.Result{RequeueAfter: after}, nil
	}

	// Periodically requeue to update master info, unless disabled via annotation
	if latest.Annotations[AnnotationDisablePolling] == annotationValueTrue {
		log.Info("Sentinel polling disabled via annotation")
		return ctrl.Result{}, nil
	}

	// A Running instance normally polls at the steady cadence — but not while a declared
	// heavy operation is running, because its healing is suppressed until that operation
	// is acknowledged and the acknowledgment can only be written on a pass (ADR-020).
	// Routed through the same shared predicate rather than re-deciding here: a duplicated
	// cadence predicate is literally how LR-045 happened.
	return ctrl.Result{RequeueAfter: requeueAfterNotRunning(
		latest.Status.Phase, latest.Status.Conditions, fast, steady)}, nil
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
	// Default the uncached reader here, where the manager is already in hand, so a
	// production reconciler cannot be constructed without it (LR-043). Leaving it to the
	// wiring site would be the exact shape LR-041 warns about — a required value held as
	// optional-looking construction state, which has no enforcement: drop the assignment
	// in main.go and the MEET guard silently degrades to the cached read (i.e. back to the
	// bug) with every test still green. Every production path goes through
	// SetupWithManager; unit/envtest reconcilers that never call it keep the apiReader()
	// fallback they need. Belt-and-braces with main.go's explicit assignment on purpose —
	// that one documents intent at the wiring site, this one enforces it.
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}

	r.monitorEvents = make(chan event.GenericEvent)
	r.monitors = make(map[types.NamespacedName]func())
	r.failoverMonitors = make(map[types.NamespacedName]func())

	return ctrl.NewControllerManagedBy(mgr).
		For(&littleredv1alpha1.LittleRed{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		WatchesRawSource(source.Channel(r.monitorEvents, &handler.EnqueueRequestForObject{})).
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
	state *redisclient.ReplicationState,
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

	case recoveryRefuseUnverified:
		// LR-051. Distinct from recoveryRefuse above and the difference is the whole
		// point: there we can SEE the holders and there are too many, here we cannot
		// see them at all, and the two want opposite remedies. The marker is kept so
		// the state stays visible; OperatorCannotAuthenticate carries the detail.
		msg := fmt.Sprintf("Bootstrap deadlock: %s. Refusing to rebootstrap — a pod that "+
			"refuses the operator's credential is a live server whose keyspace cannot be read, "+
			"so it cannot be shown to be empty and seeding a master over it may discard the "+
			"entire dataset. Fix the credential (see the OperatorCannotAuthenticate condition); "+
			"allowUnsafeRebootstrapOnDeadlock deliberately does NOT override this.",
			unverifiedPodSummary(plan.unverified))
		log.Info(msg)
		r.event(lr, corev1.EventTypeWarning, reasonRefusedDataUnverified, msg)
		return r.setLeaderlessCondition(ctx, lr, metav1.ConditionTrue, reasonRefusedDataUnverified, msg)

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
func (r *LittleRedReconciler) electMaster(ctx context.Context, lr *littleredv1alpha1.LittleRed, state *redisclient.ReplicationState, masterIP, password string, quorum int) error {
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

	// Forsaken-gated quarantine reasons (LR-044). They are reported on the Forsaken
	// condition, because the quarantine is what the operator DOES about that verdict
	// rather than a separate state of its own.
	reasonCaptured                 = "Captured"
	reasonCaptureSuspected         = "CaptureSuspected"
	reasonNotCaptured              = "NotCaptured"
	reasonQuarantined              = "Quarantined"
	reasonQuarantineLatched        = "QuarantineLatched"
	reasonQuarantineRefusedData    = "QuarantineRefusedDataPresent"
	reasonQuarantineRefusedUnknown = "QuarantineRefusedDataUnknown"
)

// quarantineReason maps a quarantine phase to the Forsaken condition reason it is
// reported under. Quarantined and Settling deliberately share one reason: arming and
// waiting are the same state to an operator, and it keeps the log/event to one per
// transition rather than one per reconcile.
func quarantineReason(p quarantinePhase) string {
	switch p {
	case quarantineStart, quarantineSettling:
		return reasonQuarantined
	case quarantineLatched:
		return reasonQuarantineLatched
	case quarantineHoldDataPresent:
		return reasonQuarantineRefusedData
	case quarantineHoldDataUnknown:
		return reasonQuarantineRefusedUnknown
	default:
		return reasonCaptured
	}
}

// setLeaderlessSince stamps Status.LeaderlessSince and sets the LeaderlessRecovery
// condition to True/DeadlockDetected (retry on conflict). No-op if already stamped.
// setForsaken arms the capture marker, persists the quarantine decision and surfaces
// the Forsaken condition.
//
// Idempotent on both timers: once ForsakenSince is set the cooldown must not be
// restarted, and QuarantinedSince/QuarantineAttempts are written only on the pass the
// pure planner asks for (q.Arm), never on every pass of the same quarantine.
func (r *LittleRedReconciler) setForsaken(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, t metav1.Time, foreign string, forsaken bool,
	q quarantinePlan,
) error {
	status, reason, msg := metav1.ConditionFalse, reasonCaptureSuspected,
		fmt.Sprintf("Sentinels are serving %s, which is not one of this instance's pods; "+
			"waiting out the settling period before declaring the instance forsaken.", foreign)
	switch {
	case q.ScaleToZero:
		status, reason = metav1.ConditionTrue, quarantineReason(q.Phase)
		msg = fmt.Sprintf("This instance has been captured by another Sentinel deployment sharing its "+
			"master name (its Sentinels serve %s, which is not one of its pods) and has been "+
			"QUARANTINED: its Redis and Sentinel pods are scaled to 0 so the capturing instance can "+
			"prune them from its own Sentinel topology. Nothing is recovered by this — the dataset was "+
			"flushed on capture; the pods return EMPTY. Quarantine attempt %d of %d.",
			foreign, q.NextAttempts, q.AttemptLimit)
		if q.Phase == quarantineLatched {
			msg = fmt.Sprintf("This instance has been quarantined %d time(s) as captured by another "+
				"Sentinel deployment sharing its master name, and is now LATCHED at 0 replicas: every "+
				"release has been followed by a recapture, and each recapture also re-pollutes the "+
				"capturing instance. This is a configuration problem — set a unique "+
				"spec.sentinel.masterName and enable spec.auth — not something the operator can retry "+
				"its way out of. Run the capture-recovery runbook in docs/USAGE.md.", q.NextAttempts)
		}
	case forsaken:
		status, reason = metav1.ConditionTrue, quarantineReason(q.Phase)
		msg = fmt.Sprintf("This instance has been captured by another Sentinel deployment sharing its "+
			"master name: every Sentinel is serving %s, which is not one of its pods, and no pod of "+
			"its own is a master. Recovery is declined by design — the dataset was flushed on capture "+
			"and the operator cannot outbid the captor's config epoch. The operator has stopped "+
			"managing this instance; run the capture-recovery runbook in docs/USAGE.md.", foreign)
		switch q.Phase {
		case quarantineHoldDataPresent:
			msg += " It has NOT been quarantined: a reachable pod still holds keys that the capturing " +
				"instance does not have, so scaling to 0 could destroy the only copy."
		case quarantineHoldDataUnknown:
			msg += " It has NOT been quarantined: a pod of this instance did not answer and the " +
				"kubelet still reports its redis container Ready, so it cannot be proven empty and " +
				"scaling to 0 could destroy data."
		}
	}

	prev := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionForsaken)
	changed := prev == nil || prev.Reason != reason

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.ForsakenSince == nil {
			latest.Status.ForsakenSince = &t
			lr.Status.ForsakenSince = &t
		}
		if q.Arm {
			latest.Status.QuarantinedSince = &t
			latest.Status.QuarantineAttempts = q.NextAttempts
			lr.Status.QuarantinedSince = &t
			lr.Status.QuarantineAttempts = q.NextAttempts
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionForsaken, Status: status,
			Reason: reason, Message: msg,
		})
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		// Mirror the condition list we just persisted onto the in-memory object. Without
		// this, the rest of the pass reads a condition list that does not contain what was
		// just written — and updateSentinelStatus, which runs later in the same pass, both
		// reads the Forsaken condition (to pick the steady requeue interval) and writes the
		// whole status back from this object. Self-correcting on the next pass, but it
		// makes the state a human is being asked to act on wrong for one interval, and this
		// condition is now load-bearing for exactly that human.
		lr.Status.Conditions = latest.Status.Conditions
		return nil
	}); err != nil {
		return err
	}

	// One event per transition, not per reconcile. The attempt count is the clearest
	// operational signal this state has: it says the configuration is the problem.
	if changed && status == metav1.ConditionTrue {
		r.event(lr, corev1.EventTypeWarning, reason, msg)
	}
	return nil
}

// quarantineConfigDangerous reports whether this instance's OWN configuration is the
// known-dangerous one: no authentication AND the shared legacy Sentinel master name.
// The master name is the only isolation boundary Sentinel's gossip protocol has, and
// authentication is the only thing that closes the narrower address-adoption path
// (ADR-015 §9.4) — so an instance with neither is expected to be recaptured rather
// than unlucky, and gets no re-roll.
//
// It deliberately does NOT use SentinelMasterNameUnscoped(), which reports only that
// the field is UNSET and (correctly, for its own purpose) does not flag an instance
// that sets "mymaster" explicitly. Here the effective value is what matters: a
// deliberate "mymaster" is exactly as capturable as an omitted one.
func quarantineConfigDangerous(lr *littleredv1alpha1.LittleRed) bool {
	return !lr.Spec.Auth.Enabled &&
		lr.SentinelMasterName() == littleredv1alpha1.LegacySentinelMasterName
}

// clearForsaken retracts the verdict. Reached when a human has run the runbook, when
// the captor has gone, and — routinely — on every pass of a quarantine, because with no
// pods there is no reachable monitoring Sentinel and the capture signature provably
// disappears. That is precisely why this function must NOT touch the attempt counter on
// signature absence: a counter that reset every cycle could never latch.
//
// The two quarantine fields therefore have different lifecycles:
//
//   - QuarantinedSince is cleared when the pure planner says the settling period has
//     elapsed (q.Clear) — the release, which hands the pods back so the existing
//     no-data leaderless reseed can re-bootstrap them empty.
//   - QuarantineAttempts is cleared only on SUCCESS: the instance actually reached a
//     healthy Running state. Not when the signature stops being observable.
//
// No-op, and no API call, when there is nothing to clear.
func (r *LittleRedReconciler) clearForsaken(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, q quarantinePlan,
) error {
	clearAttempts := lr.Status.Phase == littleredv1alpha1.PhaseRunning && lr.Status.QuarantineAttempts != 0
	if lr.Status.ForsakenSince == nil && !q.Clear && !clearAttempts &&
		meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionForsaken) == nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		latest.Status.ForsakenSince = nil
		lr.Status.ForsakenSince = nil
		if q.Clear {
			latest.Status.QuarantinedSince = nil
			lr.Status.QuarantinedSince = nil
		}
		if clearAttempts {
			latest.Status.QuarantinedSince = nil
			latest.Status.QuarantineAttempts = 0
			lr.Status.QuarantinedSince = nil
			lr.Status.QuarantineAttempts = 0
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    littleredv1alpha1.ConditionForsaken,
			Status:  metav1.ConditionFalse,
			Reason:  reasonNotCaptured,
			Message: "This instance's Sentinels are serving its own topology.",
		})
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		lr.Status.Conditions = latest.Status.Conditions // see setForsaken
		return nil
	})
}

// sentinelQuorum is the configured Sentinel quorum, defaulting to 2 (the value the
// three-Sentinel topology bootstraps with).
func sentinelQuorum(lr *littleredv1alpha1.LittleRed) int {
	if lr.Spec.Sentinel != nil && lr.Spec.Sentinel.Quorum > 0 {
		return lr.Spec.Sentinel.Quorum
	}
	return 2
}

// staleNameReport is the caller's half of Rule N's verdict: what to put on the
// condition, and whether the operator is entitled to raise its voice yet.
type staleNameReport struct {
	Status  metav1.ConditionStatus
	Reason  string
	Message string
	// Warn: emit a Warning event on transition. Only ever true for Foreign.
	Warn bool
}

// planStaleMasterNameReport turns the pure plan into the condition the operator
// publishes. It is a rendering, not a decision — and it deliberately has no clock.
//
// It USED to (M3): G5's discriminator — an address that is neither one of our pods nor
// flagged down — is byte-identical to planForsaken clause 3, and WP0 measured that
// predicate going true during an ORDINARY rename, so `Foreign` was held below a
// settling period (`ForeignSuspected`, `status.staleMasterNameForeignSince`) to stop the
// operator warning "this instance may be captured" at the exact moment the owner is
// performing the rename the runbook asked for.
//
// LR-050 removed all three surfaces, because the settle was a MARGIN against a
// user-settable timer (`spec.sentinel.downAfterMilliseconds`, unbounded) and the gate
// that replaced it closes the same window at its source: while our own Redis
// StatefulSet is not settled the planner does not call anything foreign at all, it
// defers naming the rollout gate. There is nothing left to settle, so an extra status
// field and a fifth condition reason for a once-in-an-instance-lifetime operation are
// surface with no job — and the gate covers strictly more, since a departed address of
// ours only exists in states where the StatefulSet is short of its Ready count.
func planStaleMasterNameReport(plan StaleMasterNamePlan) staleNameReport {
	if plan.Reason == staleNamesConverged {
		return staleNameReport{Status: metav1.ConditionFalse, Reason: plan.Reason, Message: plan.Message}
	}
	return staleNameReport{
		Status: metav1.ConditionTrue, Reason: plan.Reason, Message: plan.Message,
		Warn: plan.Reason == staleNamesForeign,
	}
}

// reconcileStaleMasterNames is Rule N: it reconciles the SCOPE of what this instance's
// Sentinels monitor — desired state is "every Sentinel monitors exactly the desired
// name, and nothing else" — and reports what it did.
//
// The decision is the pure planStaleMasterNames; this function executes it and owns the
// two things a pure function cannot: the re-confirmation immediately before a
// destructive command, and the clock behind the Foreign settling period.
//
// Nothing is remembered between passes: no previous name, no phase, no cursor. Anything
// a Sentinel monitors that is not the desired name is stale by definition, which is also
// why this repairs an instance a PREVIOUS botched rename (or a hand-issued MONITOR)
// already broke (R4).
//
// It returns the plan as well as its error, because Rule N is the DRIVER of ADR-020's
// one registered heavy operation and its verdict is that operation's progress report
// (see operationDriverReport, which reads Reason and Gate). Nothing about Rule N's own
// behaviour depends on that — the plan is a value the caller reads, not a gate the rule
// gained.
func (r *LittleRedReconciler) reconcileStaleMasterNames(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, state *redisclient.ReplicationState,
	desired string, quorum int, password string, forsaken, rolling bool,
) (StaleMasterNamePlan, error) {
	plan := planStaleMasterNames(state, desired, quorum, forsaken, rolling)
	audit := r.getLogger(ctx, lr, LogCategoryAudit)

	skippedAtReconfirm := r.executeStaleMasterNamePrune(ctx, lr, plan, desired, password, audit)

	msg := plan.Message
	if len(skippedAtReconfirm) > 0 {
		sort.Strings(skippedAtReconfirm)
		msg += fmt.Sprintf("; skipped at the pre-REMOVE re-confirm (did not confirm %q when asked "+
			"directly, Rule 0 registers it next pass): %s", desired, strings.Join(skippedAtReconfirm, ", "))
	}
	plan.Message = msg

	return plan, r.setStaleMasterNameCondition(ctx, lr, planStaleMasterNameReport(plan))
}

// executeStaleMasterNamePrune issues the REMOVEs the plan asks for and returns the pods
// it skipped at the re-confirm. A non-Pruning plan executes nothing at all.
//
// G6 is re-confirmed HERE, per Sentinel, immediately before acting, with a bounded
// IsMonitoring — the planner's view is a gather that is already milliseconds old, and
// REMOVE is destructive. This is LR-024's electMaster lesson (enforce the invariant, do
// not assume it) applied to the last line of defence: a Sentinel that does not confirm
// the desired name is SKIPPED, never pruned, because removing its only entry would leave
// it bare on purpose. Rule 0 gives it the desired name on a later pass.
func (r *LittleRedReconciler) executeStaleMasterNamePrune(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, plan StaleMasterNamePlan,
	desired, password string, audit logr.Logger,
) []string {
	if plan.Reason != staleNamesPruning {
		return nil
	}
	var skipped []string
	for _, entry := range plan.Prune {
		addr := fmt.Sprintf("%s:%d", entry.SentinelIP, littleredv1alpha1.SentinelPort)
		sc := redisclient.NewSentinelClient([]string{addr}, password, lr.Spec.TLS.Enabled)

		// Bounded twice, and both halves are required (LR-040): IsMonitoring's client
		// carries ProbeTimeout on Dial/Read/Write, and this deadline is what bounds
		// go-redis's dial-retry loop. Rule N runs during churn by design, which is the
		// exact situation that stalled a reconcile ~117s.
		confirmCtx, cancel := context.WithTimeout(ctx, redisclient.ProbeTimeout)
		monitoring, err := sc.IsMonitoring(confirmCtx, addr, desired)
		cancel()
		if err != nil || !monitoring {
			audit.Info("Skipping stale master-name prune: this Sentinel did not confirm the desired "+
				"name when asked directly; Rule 0 registers it next pass",
				"pod", entry.SentinelPod, "address", addr, "desired_name", desired,
				"stale_names", entry.Names, "error", err)
			skipped = append(skipped, entry.SentinelPod)
			continue
		}

		for _, name := range entry.Names {
			if err := sc.Remove(ctx, name); err != nil {
				audit.Error(err, "Failed to remove stale Sentinel master name",
					"pod", entry.SentinelPod, "address", addr,
					"stale_name", name, "desired_name", desired)
				continue
			}
			audit.Info("Removed stale Sentinel master name",
				"pod", entry.SentinelPod, "address", addr,
				"stale_name", name, "desired_name", desired)
		}
	}
	return skipped
}

// setStaleMasterNameCondition persists Rule N's report, and is deliberately quiet when
// nothing changed: this runs on every sentinel pass, and a status write every 2s for an
// unchanged verdict is churn of exactly the kind LR-042 removed.
//
// One event per TRANSITION, never per reconcile — and only a settled Foreign is a
// Warning (see planStaleMasterNameReport).
func (r *LittleRedReconciler) setStaleMasterNameCondition(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, rep staleNameReport,
) error {
	prev := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionStaleMasterName)
	if prev != nil && prev.Status == rep.Status && prev.Reason == rep.Reason &&
		prev.Message == rep.Message {
		return nil
	}
	changed := prev == nil || prev.Reason != rep.Reason

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionStaleMasterName, Status: rep.Status,
			Reason: rep.Reason, Message: rep.Message,
		})
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		// Mirror what was just persisted onto the in-memory object. updateSentinelStatus
		// runs later in the SAME pass and writes the whole status back from this object,
		// so without the mirror it silently reverts what we just wrote — the LR-044
		// defect, which must not be reintroduced.
		lr.Status.Conditions = latest.Status.Conditions
		return nil
	}); err != nil {
		return err
	}

	if changed && rep.Warn {
		r.event(lr, corev1.EventTypeWarning, rep.Reason, rep.Message)
	}
	return nil
}

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
