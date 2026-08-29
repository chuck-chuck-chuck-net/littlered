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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// ============================================================================
// Failover mode reconciliation (ADR-011, M4).
//
// Operator-managed HA without Sentinel: the reconcile loop is the sole
// failure detector and failover decider. The pods' assignment annotations ARE
// the intent record (re-derived every pass, never read back from status —
// ADR-006); the decisions live in the pure seams planMasterDeath /
// planFailover (failover_plan.go) and the intent helpers (failover_intent.go).
// This file only gathers inputs and executes the returned plans.
// ============================================================================

// failoverTransitionCooldown is the short post-transition cooldown (ADR-011
// §6): after the operator stamps a new master intent (epoch bump), no further
// promotion decision is taken for this long, serializing cascading flips. 10s
// spans several fast-requeue passes (2s) — enough for the promoted pod to
// report role:master and the label to flip — while staying well below the
// sentinel-mode failover timers this mode replaces. Anchored on
// status.failover.transitionSince (a monitoring surface: losing it at worst
// skips one cooldown window; the unsettled-transition gate still holds).
const failoverTransitionCooldown = 10 * time.Second

// Event reasons specific to failover mode (ADR-011). The recovery-outcome
// reasons (Reseeded, RefusedDataPresent, UnsafeRebootstrap, Recovered, ...)
// are shared with the sentinel-mode rules.
const (
	// reasonExperimentalMode marks the one-time warning emitted on the first
	// reconcile of a failover-mode instance (ADR-011 §8).
	reasonExperimentalMode = "ExperimentalMode"
	// reasonFailoverPromoted records a completed operator-led failover: a
	// replica was promoted to master and the intent re-stamped.
	reasonFailoverPromoted = "FailoverPromoted"
)

// failoverEngineView is what the assignment/heal engine hands to the label and
// status steps, so they act on the same gather instead of re-probing.
type failoverEngineView struct {
	intent       failoverIntent
	liveMasterIP string
	// masterPodName is the intended master's pod name once it is OBSERVED
	// live (reachable + role:master); empty during transitions, so the label
	// step strips the master label and the status step reports no master.
	masterPodName string
	// replicasLinkedUp counts reachable replicas following the intended
	// master with master_link_status:up (the Running gate input).
	replicasLinkedUp int
	// requeueFast requests the fast requeue interval: a transition, detection
	// window, or healing action is in flight.
	requeueFast bool
}

// reconcileFailover reconciles failover mode. Step sequence mirrors
// reconcileSentinel minus everything Sentinel: ConfigMap -> replicas Service ->
// redis StatefulSet -> master Service -> bootstrap -> assignment/heal engine ->
// master label -> PDB -> ServiceMonitor -> status.
func (r *LittleRedReconciler) reconcileFailover(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	log.Info("Reconciling failover mode")

	if littleRed.Status.Phase == "" {
		littleRed.Status.Phase = littleredv1alpha1.PhasePending
		// Experimental-mode warning (ADR-011 §8; neutral wording from the
		// design note §3.4). Phase is unset only on the very first
		// reconcile(s) of a new CR, so this is a cheap "once" without a
		// persisted flag; the recorder aggregates any repeats.
		r.event(littleRed, corev1.EventTypeWarning, reasonExperimentalMode,
			"mode failover is experimental: operator-managed HA without Sentinel, under active "+
				"validation — see docs for current status and trade-offs vs sentinel")
	}

	// Reconcile Redis ConfigMap (no sentinel.conf sibling in this mode)
	if err := r.reconcileConfigMapFailover(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Redis ConfigMap")
		return ctrl.Result{}, err
	}

	// Reconcile headless service for Redis (needed before StatefulSet)
	if err := r.reconcileReplicasService(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile replicas Service")
		return ctrl.Result{}, err
	}

	// Reconcile Redis StatefulSet
	if err := r.reconcileFailoverStatefulSet(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Redis StatefulSet")
		return ctrl.Result{}, err
	}

	// Reconcile master Service (label-routed to the current master)
	if err := r.reconcileMasterService(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile master Service")
		return ctrl.Result{}, err
	}

	// Bootstrap: stamp the initial assignment set if required
	if littleRed.Status.BootstrapRequired {
		if err := r.bootstrapFailover(ctx, littleRed); err != nil {
			log.Error(err, "Failed to bootstrap failover instance")
			return ctrl.Result{}, err
		}
	}

	// Assignment/heal engine: failure detection, failover decision, straggler
	// repoint, re-authorization. Best-effort like reconcileSentinelCluster —
	// status is still updated below.
	eng, err := r.reconcileFailoverAssignments(ctx, littleRed)
	if err != nil {
		log.Error(err, "Failed to reconcile failover assignments")
	}

	// Update pod role labels from the operator's intent (+observation)
	if err := r.updateFailoverMasterLabel(ctx, littleRed, eng); err != nil {
		log.Error(err, "Failed to update master labels")
		// Don't fail - this is best effort
	}

	// Reconcile PodDisruptionBudget
	if err := r.reconcileFailoverPDB(ctx, littleRed); err != nil {
		log.Error(err, "Failed to reconcile Redis PodDisruptionBudget")
		return ctrl.Result{}, err
	}

	// Reconcile ServiceMonitor if enabled
	if littleRed.Spec.Metrics.IsEnabled() && littleRed.Spec.Metrics.ServiceMonitor.Enabled {
		if err := r.reconcileServiceMonitor(ctx, littleRed); err != nil {
			log.Error(err, "Failed to reconcile ServiceMonitor")
		}
	}

	// Ensure the background master watcher is running (ADR-011 §4 fast path;
	// mirrors reconcileSentinel's ensureSentinelMonitor placement)
	r.ensureFailoverMonitor(ctx, littleRed)

	// Update status
	res, err := r.updateFailoverStatus(ctx, littleRed, eng)
	if err != nil {
		return res, err
	}
	// A transition/detection window is in flight: requeue fast even if the
	// status computation landed on the steady interval.
	if eng != nil && eng.requeueFast {
		fast, _ := littleRed.GetRequeueIntervals()
		if res.RequeueAfter == 0 || res.RequeueAfter > fast {
			res.RequeueAfter = fast
		}
	}
	return res, nil
}

// reconcileConfigMapFailover ensures the Redis ConfigMap exists for failover mode
func (r *LittleRedReconciler) reconcileConfigMapFailover(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildConfigMapFailoverMode(littleRed))
}

// reconcileFailoverStatefulSet ensures the Redis StatefulSet exists for failover mode
func (r *LittleRedReconciler) reconcileFailoverStatefulSet(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildRedisStatefulSetFailover(littleRed))
}

// reconcileFailoverPDB creates or deletes the PDB for the failover-mode data pods.
func (r *LittleRedReconciler) reconcileFailoverPDB(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	if r.pdbEnabled(littleRed) {
		return r.apply(ctx, littleRed, buildFailoverRedisPDB(littleRed))
	}
	return r.deleteIfExists(ctx, littleRed, &policyv1.PodDisruptionBudget{}, podDisruptionBudgetName(littleRed))
}

// bootstrapFailover stamps the initial assignment set: redis-0 as master and
// every other data pod as its replica, all at one epoch (the ADR-011 §3
// replacement for bootstrapSentinel's Sentinel registration). Mirrors
// bootstrapSentinel's guards: JIT re-check of the flag, and redis-0 must have
// an IP and belong to the current StatefulSet revision (a terminating redis-0
// from a deleted deployment would poison the replicas with a stale master IP).
func (r *LittleRedReconciler) bootstrapFailover(ctx context.Context, lr *littleredv1alpha1.LittleRed) error {
	log := r.getLogger(ctx, lr, LogCategoryRecon)

	// 1. Just-in-Time check: another worker may have bootstrapped already.
	latest := &littleredv1alpha1.LittleRed{}
	if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
		return err
	}
	if !latest.Status.BootstrapRequired {
		log.Info("Bootstrap: flag already cleared in latest API version, skipping")
		*lr = *latest
		return nil
	}

	// 2. Ensure redis-0 belongs to the current StatefulSet and has an IP.
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: statefulSetName(lr), Namespace: lr.Namespace}, sts); err != nil {
		return err
	}
	pod0 := &corev1.Pod{}
	pod0Name := fmt.Sprintf("%s-redis-0", lr.Name)
	if err := r.Get(ctx, types.NamespacedName{Name: pod0Name, Namespace: lr.Namespace}, pod0); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Bootstrap: redis-0 not created yet, will retry on next reconcile")
			return nil
		}
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

	// 3. Stamp the full assignment set at one fresh epoch (derived from live
	// pod annotations — normally 0+1=1 on a fresh instance). Pods without an
	// IP yet are picked up by the re-authorization loop once the master lives.
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(lr.Namespace), client.MatchingLabels(redisSelectorLabels(lr))); err != nil {
		return err
	}
	intent := resolveFailoverIntent(buildFailoverPodViews(podList, nil))
	epoch := intent.maxEpoch + 1

	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	auditLog.Info("Bootstrap: stamping initial assignment set",
		"master", pod0Name, "masterIP", pod0.Status.PodIP, "epoch", epoch)
	// authorizeMasterStart: bootstrap is by definition a fresh start with no data
	// anywhere, and redis-0 may already carry a start marker (e.g. its container
	// restarted while we were still waiting to bootstrap). Without the
	// authorization it would park forever (LR-038).
	if err := r.stampFailoverAssignments(ctx, lr, podList, pod0Name, pod0.Status.PodIP, epoch, true); err != nil {
		return err
	}
	if err := r.markFailoverTransition(ctx, lr, epoch); err != nil {
		return err
	}

	// 4. Clear bootstrap flag with retry on conflict.
	auditLog.Info("Bootstrap: initial assignment stamped, clearing bootstrapRequired flag")
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

	// Update the local object to avoid conflicts later in the same pass.
	return r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, lr)
}

// reconcileFailoverAssignments is the failover-mode assignment/heal engine: it
// gathers ground truth, re-derives the intent from the pod annotations, runs
// the pure master-death predicate and failover decision, and executes exactly
// one class of action per pass (stamping one consistent assignment set —
// master + replicas at one epoch — counts as one action; conflicting actions
// prefer a fast requeue over a multi-step blast).
//
//nolint:gocyclo
func (r *LittleRedReconciler) reconcileFailoverAssignments(ctx context.Context, lr *littleredv1alpha1.LittleRed) (*failoverEngineView, error) {
	// Skip until the initial assignment is stamped.
	if lr.Status.BootstrapRequired {
		return nil, nil
	}

	log := r.getLogger(ctx, lr, LogCategoryRecon)
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	password := r.getRedisPassword(ctx, lr)

	// 1. Gather the K8s view.
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(lr.Namespace), client.MatchingLabels(redisSelectorLabels(lr))); err != nil {
		return nil, err
	}
	redisMap := make(map[string]string)
	// Every address a pod of ours holds, terminating included — the attribution set
	// (LR-053). Failover mode asks no attribution question today (there is no capture
	// verdict and no master-name scope), so nothing reads OwnedIPs here; it is built
	// correctly rather than passed as nil so the mode cannot silently inherit the
	// live-topology answer to an "is this ours?" question the day it grows one.
	ownedIPs := make(map[string]bool, len(podList.Items))
	anyTerminating := false
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Status.PodIP != "" {
			ownedIPs[p.Status.PodIP] = true
		}
		if !p.DeletionTimestamp.IsZero() {
			anyTerminating = true
			continue
		}
		if p.Status.PodIP != "" {
			redisMap[p.Status.PodIP] = p.Name
		}
	}

	// 2. Gather Redis ground truth. There are no Sentinels in this mode, so
	// the sentinel map is empty; the state's Sentinel-derived fields
	// (RealMasterIP would fall back to ANY reachable role:master) are
	// deliberately ignored — in failover mode the operator's intent is the
	// sole master authority (determineFailoverLiveMaster).
	g := &operatorGatherer{password: password, tlsEnabled: lr.Spec.TLS.Enabled}
	// No Sentinels in failover mode, so no master name to supply and no Sentinel
	// probe is issued.
	state := redisclient.GatherReplicationState(ctx, g, redisMap, map[string]string{}, "", ownedIPs)

	// 2a. Can we still talk to our own pods? (LR-051, rule §7.11.) Failover mode has
	// the same defect as sentinel mode and reaches it the same way — planFailover's
	// 0-holder seed branch keys on DataHolders(), which filters on Reachable — so it
	// gets the same veto (in planFailover) and the same named condition.
	if err := r.reportOperatorAuth(ctx, lr, replicationAuthFailures(state)); err != nil {
		log.Error(err, "failed to report the operator's authentication state")
	}

	// 3. Re-derive the intent and the live master from live state.
	views := buildFailoverPodViews(podList, state)
	intent := resolveFailoverIntent(views)
	liveMasterIP := determineFailoverLiveMaster(state, intent.masterIP)

	roleLabels := make(map[string]string, len(podList.Items))
	for i := range podList.Items {
		roleLabels[podList.Items[i].Name] = podList.Items[i].Labels[LabelRole]
	}
	settled := failoverTransitionSettled(intent, state, roleLabels)

	eng := &failoverEngineView{intent: intent, liveMasterIP: liveMasterIP}
	if liveMasterIP != "" {
		eng.masterPodName = intent.masterName
	}
	for ip, rn := range state.RedisNodes {
		if rn.Reachable && ip != intent.masterIP && rn.MasterHost == intent.masterIP && rn.LinkStatus == "up" {
			eng.replicasLinkedUp++
		}
	}

	// 4. Resume a half-applied transition (ADR-006: resumable from live state,
	// no persisted cursor): the intent names a master that is reachable but
	// still runs as a replica — the stamp landed but the REPLICAOF NO ONE did
	// not (operator restart / transient error mid-execution). Re-issue it.
	if intent.masterIP != "" && needsPromotion(state, intent.masterIP) {
		auditLog.Info("Resuming unfinished transition: promoting intended master (REPLICAOF NO ONE)",
			"master", intent.masterIP, "masterPod", intent.masterName)
		if err := r.promoteFailoverMaster(ctx, lr, state, intent.masterIP, password); err != nil {
			return eng, err
		}
		eng.requeueFast = true
		return eng, nil
	}

	// 5. Failure detection for the intended master (pure planMasterDeath).
	failover := failoverSpecOrDefault(lr)
	declaredDead := false
	if intent.masterName != "" {
		pv := failoverMasterPodView(podList, intent)
		masterReachable := false
		if rn := state.RedisNodes[intent.masterIP]; rn != nil {
			masterReachable = rn.Reachable
		}
		var replicaLinks []string
		for ip, rn := range state.RedisNodes {
			if rn.Reachable && ip != intent.masterIP && rn.MasterHost == intent.masterIP {
				replicaLinks = append(replicaLinks, rn.LinkStatus)
			}
		}
		var downSince *time.Time
		if lr.Status.Failover != nil && lr.Status.Failover.MasterDownSince != nil {
			downSince = &lr.Status.Failover.MasterDownSince.Time
		}
		downAfter := time.Duration(failover.DownAfterMilliseconds) * time.Millisecond

		switch planMasterDeath(pv, masterReachable, replicaLinks, downSince, time.Now(), downAfter) {
		case masterDeathClearMarker:
			if err := r.clearFailoverMasterDownSince(ctx, lr); err != nil {
				log.Error(err, "failed to clear masterDownSince marker")
			}
		case masterDeathStartWindow:
			log.Info("Master unreachable; starting detection window",
				"master", intent.masterName, "downAfter", downAfter.String())
			if err := r.setFailoverMasterDownSince(ctx, lr, metav1.Now()); err != nil {
				log.Error(err, "failed to set masterDownSince marker")
			}
			eng.requeueFast = true
		case masterDeathWait:
			eng.requeueFast = true
		case masterDeathHold:
			log.Info("Master unreachable past the window but declaration vetoed "+
				"(a replica link is up, or no replica can corroborate); holding",
				"master", intent.masterName)
			eng.requeueFast = true
		case masterDeathDeclareK8s, masterDeathDeclareProbe:
			declaredDead = true
		}
	}

	// 6. The failover decision (pure planFailover): runs when the master is
	// declared dead, or when there is no intended master at all (fresh or
	// deadlock states — the annotations died with their pods).
	if intent.masterName == "" || declaredDead {
		eng.requeueFast = true
		var transitionSince *time.Time
		if lr.Status.Failover != nil && lr.Status.Failover.TransitionSince != nil {
			transitionSince = &lr.Status.Failover.TransitionSince.Time
		}
		bootstrapMasterIP := r.pickBootstrapMasterIP(lr, redisMap)

		// The unsettled gate uses failoverPromotionUnsettled, NOT !settled: a
		// dead intended master (the usual reason we are here) must never block
		// its own replacement — see the helper's doc for the deadlock this avoids.
		unsettled := failoverPromotionUnsettled(intent, state, roleLabels)
		plan := planFailover(state, liveMasterIP, failover.AllowUnsafeRebootstrapOnDeadlock,
			bootstrapMasterIP, unsettled, transitionSince, time.Now(), failoverTransitionCooldown)
		return eng, r.executeFailoverPlan(ctx, lr, state, podList, intent, plan, password)
	}

	// 7. Healthy-path healing. Requires a live master; while the detection
	// window runs there is nothing safe to do.
	if liveMasterIP == "" {
		eng.requeueFast = true
		return eng, nil
	}

	// A live master is known again: record a completed recovery if the
	// FailoverRecovery condition was raised (mirror of the sentinel loop's
	// marker-clearing block). No-op when the condition was never set.
	if err := r.clearFailoverRecoveryCondition(ctx, lr); err != nil {
		log.Error(err, "failed to clear FailoverRecovery condition")
	}

	// 7a. Straggler repoint (Rule R analog). UNGATED as of LR-038 — it previously
	// required `settled && !anyTerminating`, and both halves were inherited from
	// sentinel mode rather than reasoned for this one (pillar 3.5 scope).
	//
	// `!anyTerminating` meant "don't churn the topology while a pod is going
	// away", which protects against a *competing actor* mid-failover. In failover
	// mode there is no competing actor — the operator is the algorithm — so it
	// bought nothing and cost three things: it hid the outgoing master from the
	// fence (the 202-write loss), it kept a freshly promoted master replica-less
	// for extra passes (measured: 60 extra refused writes under
	// minReplicasToWrite:1), and it suppressed `min-replicas-to-write` as a
	// self-fence, since stripping a dying master of its last replica is itself a
	// fencing action.
	//
	// `settled` additionally waited for the master *label* to flip. That is
	// redundant here: reaching this point already requires liveMasterIP != "",
	// i.e. the intended master is reachable AND reporting role:master.
	//
	// Repointing EARLIER is also the safer direction for data, which is the
	// opposite of how the gate reads. A straggler still following the old master
	// can only drift further ahead of the new one while we wait — so waiting
	// enlarges the divergence a resync will discard rather than protecting it. And
	// the live master is by construction not behind: it was chosen either by
	// bootstrap (no data anywhere) or by BestDataHolder (highest offset among
	// reachable holders).
	//
	// A bonus falls out: the outgoing master is itself a straggler by
	// planFailoverRepoints' definition (reachable, role:master, not the live
	// master), so removing the gate demotes it here too — the operator-side fence,
	// arriving for free on any path where its pod object still exists.
	//
	// Step 7b (re-authorization) keeps `settled`: it stamps annotations that
	// release parked pods, and releasing a pod into a role that is about to change
	// is a different risk from redirecting a running replica.
	for _, ip := range planFailoverRepoints(state, liveMasterIP) {
		rn := state.RedisNodes[ip]
		auditLog.Info("Redis pod is not following the intended master, issuing SLAVEOF",
			"pod", rn.PodName, "current_role", rn.Role, "target_master", liveMasterIP,
			"settled", settled, "anyTerminating", anyTerminating)
		if err := r.slaveOfBounded(ctx, lr, ip, liveMasterIP, password); err != nil {
			auditLog.Error(err, "Failed to repoint straggler", "pod", rn.PodName)
		}
		eng.requeueFast = true
	}

	// 7b. Re-authorization loop (ADR-011 §3): release parked pods (their
	// stamped epoch is consumed) and stamp brand-new pods. Annotation stamps
	// are inert metadata, so no terminating-pods gate; still gated on a
	// settled transition so re-auth never races a master flip.
	if settled {
		for _, s := range planFailoverReauth(views, intent, liveMasterIP) {
			auditLog.Info("Re-authorizing pod with a fresh replica assignment",
				"pod", s.podName, "master", s.masterIP, "epoch", s.epoch)
			if err := r.stampFailoverPod(ctx, lr, s); err != nil {
				auditLog.Error(err, "Failed to stamp pod assignment", "pod", s.podName)
			}
			eng.requeueFast = true
		}
	}

	return eng, nil
}

// executeFailoverPlan executes a planFailover decision. Stamping one consistent
// assignment set (master + replicas at one epoch) counts as ONE action; the
// promotion of the elected pod is issued in the same pass and is resumable from
// live state if it fails (step 4 of the engine).
func (r *LittleRedReconciler) executeFailoverPlan(
	ctx context.Context,
	lr *littleredv1alpha1.LittleRed,
	state *redisclient.ReplicationState,
	podList *corev1.PodList,
	intent failoverIntent,
	plan failoverPlan,
	password string,
) error {
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)

	switch plan.action {
	case failoverNone:
		// Declared dead by K8s evidence but observably still mastering (e.g. a
		// readiness flap on a reachable role:master). Nothing safe to do.
		return nil

	case failoverWait:
		log.Info("No live master but not acting this pass (unsettled transition, cooldown, or no candidate yet)",
			"intendedMaster", intent.masterName, "holders", plan.holders)
		return nil

	case failoverSeed:
		epoch := intent.maxEpoch + 1
		masterPod := podNameForIP(podList, plan.masterIP)
		msg := fmt.Sprintf("Failover recovery: no data present, seeded %s as master (epoch %d)", masterPod, epoch)
		auditLog.Info(msg, "master", plan.masterIP, "masterPod", masterPod, "epoch", epoch)
		// authorizeMasterStart: seeding runs only with ZERO data holders, and the
		// seeded pod may be parked by the start gate after a restart, so it needs
		// explicit permission to come up as master (LR-038).
		if err := r.stampFailoverAssignments(ctx, lr, podList, masterPod, plan.masterIP, epoch, masterStartAuthorizedFor(plan.action)); err != nil {
			return err
		}
		if err := r.markFailoverTransition(ctx, lr, epoch); err != nil {
			return err
		}
		r.event(lr, corev1.EventTypeNormal, reasonReseeded, msg)
		return nil

	case failoverPromote, failoverUnsafeElect:
		epoch := intent.maxEpoch + 1
		masterPod := podNameForIP(podList, plan.masterIP)
		var rn *redisclient.RedisNodeState
		if n := state.RedisNodes[plan.masterIP]; n != nil {
			rn = n
		} else {
			rn = &redisclient.RedisNodeState{IP: plan.masterIP, PodName: masterPod}
		}

		// ADR-011 §5 execution order: stamp the new intent first (it is the
		// durable record a crashed operator resumes from), then promote.
		// authorizeMasterStart=false, deliberately: BestDataHolder only ever returns
		// a REACHABLE node (DataHolders requires Reachable && Keys > 0), so this is
		// an in-place promotion of a running process that already holds the data.
		// Authorizing a master START here would hand a future kill-9 of this very
		// pod permission to return as an EMPTY master — the 352-of-1145 wipe this
		// guard exists to prevent (LR-038).
		if err := r.stampFailoverAssignments(ctx, lr, podList, masterPod, plan.masterIP, epoch, masterStartAuthorizedFor(plan.action)); err != nil {
			return err
		}
		if err := r.promoteFailoverMaster(ctx, lr, state, plan.masterIP, password); err != nil {
			// The intent is stamped; the resume step re-issues the promotion
			// on the next (fast) requeue.
			return err
		}

		// Fence the outgoing master (ADR-011 §7 amendment): demote it so it stops
		// accepting writes. On a graceful delete it is still alive and still
		// mastering for the rest of its preStop window, and an established client
		// connection through the master Service is NOT re-routed by the label
		// flip — so without this it keeps ACKing writes that die with the pod
		// (measured: 202 of 1171 lost, silently). Best-effort and idempotent: it
		// is a convergence step the straggler repoint would eventually perform
		// anyway, so a failure here is retried by the next pass, never fatal.
		//
		// Views are built with a nil state ON PURPOSE. The gather omits
		// terminating pods, so the outgoing master is missing from state.RedisNodes
		// exactly when it needs fencing; keying the fence on the gather made it
		// silently inert. Widening the gather is not the answer either — the dying
		// master would then feed determineFailoverLiveMaster and BestDataHolder.
		if fenceIP := planFailoverFence(buildFailoverPodViews(podList, nil), intent.masterIP, plan.masterIP); fenceIP != "" {
			auditLog.Info("Fencing outgoing master: demoting it so it can no longer accept writes",
				"outgoingMaster", fenceIP, "outgoingPod", podNameForIP(podList, fenceIP), "newMaster", plan.masterIP)
			if err := r.slaveOfBounded(ctx, lr, fenceIP, plan.masterIP, password); err != nil {
				auditLog.Error(err, "Failed to fence outgoing master; writes may be lost until it dies",
					"outgoingMaster", fenceIP)
			}
		}

		if err := r.markFailoverTransition(ctx, lr, epoch); err != nil {
			return err
		}

		if plan.action == failoverUnsafeElect {
			msg := fmt.Sprintf("UNSAFE failover: force-elected %s (keys=%d, offset=%d) as master; divergent data on %d "+
				"other holder(s) will be DISCARDED via full resync", masterPod, rn.Keys, rn.Offset, plan.holders-1)
			auditLog.Info(msg, "master", plan.masterIP, "epoch", epoch, "divergedLineages", plan.diverged)
			r.event(lr, corev1.EventTypeWarning, reasonUnsafeRebootstrap, msg)
			return nil
		}
		msg := fmt.Sprintf("Failover: promoted %s (keys=%d, offset=%d) to master at epoch %d — single replication "+
			"lineage, no data discarded", masterPod, rn.Keys, rn.Offset, epoch)
		auditLog.Info(msg, "master", plan.masterIP, "epoch", epoch, "holders", plan.holders)
		r.event(lr, corev1.EventTypeNormal, reasonFailoverPromoted, msg)
		return nil

	case failoverRefuse:
		msg := fmt.Sprintf("No live master and %d pods hold data across divergent replication lineages. "+
			"Refusing to elect (would discard independent writes). Set failover.allowUnsafeRebootstrapOnDeadlock=true "+
			"to authorize, or intervene manually.", plan.holders)
		log.Info(msg)
		r.event(lr, corev1.EventTypeWarning, reasonRefusedDataPresent, msg)
		return r.setFailoverRecoveryCondition(ctx, lr, metav1.ConditionTrue, reasonRefusedDataPresent, msg)

	case failoverRefuseUnverified:
		// LR-051, the failover-mode twin of Rule L's branch. The SEED branch above is
		// the dangerous one here: it would elect a FRESH pod — reachable precisely
		// because it restarted onto the current credential, and therefore empty —
		// while the pods holding the only copy are invisible to us.
		msg := fmt.Sprintf("No live master, and %s. Refusing to elect — a pod that refuses the "+
			"operator's credential is a live server whose keyspace cannot be read, so it cannot "+
			"be shown to be empty and electing around it may discard the entire dataset. Fix the "+
			"credential (see the OperatorCannotAuthenticate condition); "+
			"failover.allowUnsafeRebootstrapOnDeadlock deliberately does NOT override this.",
			unverifiedPodSummary(plan.unverified))
		log.Info(msg)
		r.event(lr, corev1.EventTypeWarning, reasonRefusedDataUnverified, msg)
		return r.setFailoverRecoveryCondition(ctx, lr, metav1.ConditionTrue, reasonRefusedDataUnverified, msg)
	}
	return nil
}

// promoteFailoverMaster issues REPLICAOF NO ONE to the elected pod when it is a
// reachable replica (needsPromotion — reused from the sentinel rules; an
// unreachable/parked elect starts fresh as master via its startup script, and a
// pod already reporting role:master needs nothing). This is the promotion
// primitive of sentinel-mode electMaster WITHOUT its Sentinel half
// (seedSentinelsWithMaster) — in failover mode there is nothing to point at the
// new master; the stamped annotations and the label flip carry the intent.
func (r *LittleRedReconciler) promoteFailoverMaster(ctx context.Context, lr *littleredv1alpha1.LittleRed, state *redisclient.ReplicationState, masterIP, password string) error {
	if !needsPromotion(state, masterIP) {
		return nil
	}
	auditLog := r.getLogger(ctx, lr, LogCategoryAudit)
	auditLog.Info("Promoting elected pod to master (REPLICAOF NO ONE)",
		"master", masterIP, "wasRole", state.RedisNodes[masterIP].Role)
	pctx, cancel := context.WithTimeout(ctx, redisclient.ProbeTimeout)
	defer cancel()
	addr := fmt.Sprintf("%s:%d", masterIP, littleredv1alpha1.RedisPort)
	if err := redisclient.SlaveOf(pctx, addr, password, "", "", lr.Spec.TLS.Enabled); err != nil {
		return fmt.Errorf("promote elected master %s: %w", masterIP, err)
	}
	return nil
}

// slaveOfBounded issues SLAVEOF <masterIP> to one pod with a hard ProbeTimeout
// deadline (LR-017 discipline: no unbounded dials in the reconcile path).
func (r *LittleRedReconciler) slaveOfBounded(ctx context.Context, lr *littleredv1alpha1.LittleRed, ip, masterIP, password string) error {
	pctx, cancel := context.WithTimeout(ctx, redisclient.ProbeTimeout)
	defer cancel()
	addr := fmt.Sprintf("%s:%d", ip, littleredv1alpha1.RedisPort)
	return redisclient.SlaveOf(pctx, addr, password, masterIP, fmt.Sprintf("%d", littleredv1alpha1.RedisPort), lr.Spec.TLS.Enabled)
}

// stampFailoverAssignments stamps one consistent assignment set: masterName
// gets assigned-role=master, every other non-terminating pod with an IP gets
// assigned-role=replica pointing at masterIP, all at the same epoch. Pods
// already carrying the exact target assignment are skipped (idempotent).
func (r *LittleRedReconciler) stampFailoverAssignments(ctx context.Context, lr *littleredv1alpha1.LittleRed, podList *corev1.PodList, masterName, masterIP string, epoch int64, authorizeMasterStart bool) error {
	var firstErr error
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Status.PodIP == "" || !p.DeletionTimestamp.IsZero() {
			continue
		}
		s := failoverStamp{podName: p.Name, role: RoleReplica, masterIP: masterIP, epoch: epoch}
		if p.Name == masterName {
			s.role = RoleMaster
			s.masterIP = ""
			s.authorizeMasterStart = authorizeMasterStart
		}
		if err := r.stampFailoverPod(ctx, lr, s); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// stampFailoverPod patches one pod's assignment annotations (the ADR-011 §3
// assignment channel; the kubelet re-renders the downward-API projection). A
// pod already carrying the exact target values is left untouched.
func (r *LittleRedReconciler) stampFailoverPod(ctx context.Context, lr *littleredv1alpha1.LittleRed, s failoverStamp) error {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Name: s.podName, Namespace: lr.Namespace}, pod); err != nil {
		return err
	}
	epochStr := strconv.FormatInt(s.epoch, 10)
	authSatisfied := !s.authorizeMasterStart ||
		pod.Annotations[AnnotationMasterStartAuthorizedEpoch] == epochStr
	if pod.Annotations[AnnotationAssignedRole] == s.role &&
		pod.Annotations[AnnotationAssignedMasterIP] == s.masterIP &&
		pod.Annotations[AnnotationAssignmentEpoch] == epochStr &&
		authSatisfied {
		return nil
	}
	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[AnnotationAssignedRole] = s.role
	pod.Annotations[AnnotationAssignedMasterIP] = s.masterIP
	pod.Annotations[AnnotationAssignmentEpoch] = epochStr
	if s.authorizeMasterStart {
		// Explicit permission for a RESTARTED pod to start a fresh process as
		// master (LR-038). Never stamped by an in-place promotion, so it cannot
		// authorize a future kill-9 to return as an empty master.
		pod.Annotations[AnnotationMasterStartAuthorizedEpoch] = epochStr
	}
	return r.Patch(ctx, pod, patch)
}

// buildFailoverPodViews resolves the pure per-pod inputs from the K8s pod list
// and (optionally) the gathered ground truth.
func buildFailoverPodViews(podList *corev1.PodList, state *redisclient.ReplicationState) []failoverPodView {
	views := make([]failoverPodView, 0, len(podList.Items))
	for i := range podList.Items {
		p := &podList.Items[i]
		ready, restarted := redisContainerStatus(p)
		v := failoverPodView{
			name:        p.Name,
			ip:          p.Status.PodIP,
			terminating: !p.DeletionTimestamp.IsZero(),
			ready:       ready,
			restarted:   restarted,
		}
		if state != nil && v.ip != "" {
			if rn := state.RedisNodes[v.ip]; rn != nil {
				v.reachable = rn.Reachable
			}
		}
		role := p.Annotations[AnnotationAssignedRole]
		epochStr := p.Annotations[AnnotationAssignmentEpoch]
		if role != "" && epochStr != "" {
			if e, err := strconv.ParseInt(epochStr, 10, 64); err == nil {
				v.hasAssignment = true
				v.assignedRole = role
				v.assignedMasterIP = p.Annotations[AnnotationAssignedMasterIP]
				v.epoch = e
			}
		}
		views = append(views, v)
	}
	return views
}

// failoverMasterPodView resolves the K8s view of the intended master pod for
// planMasterDeath. Present requires name AND IP to match the intent (strict IP
// identity, ADR-001 — a replaced pod is not present; in practice a replaced
// pod also loses its annotations, so the intent itself vanishes with it).
func failoverMasterPodView(podList *corev1.PodList, intent failoverIntent) masterPodView {
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Name != intent.masterName || p.Status.PodIP != intent.masterIP {
			continue
		}
		ready, _ := redisContainerStatus(p)
		return masterPodView{
			present:     true,
			ready:       ready,
			terminating: !p.DeletionTimestamp.IsZero(),
		}
	}
	return masterPodView{}
}

// redisContainerStatus returns the kubelet-reported readiness and whether the
// redis container has restarted at least once.
func redisContainerStatus(pod *corev1.Pod) (ready, restarted bool) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == ComponentRedis {
			return cs.Ready, cs.RestartCount > 0
		}
	}
	return false, false
}

// podNameForIP resolves a pod name from its IP ("" if unknown).
func podNameForIP(podList *corev1.PodList, ip string) string {
	for i := range podList.Items {
		if podList.Items[i].Status.PodIP == ip {
			return podList.Items[i].Name
		}
	}
	return ""
}

// updateFailoverMasterLabel updates the role labels from the operator's intent:
// the intended master pod, once OBSERVED role:master (eng.masterPodName), gets
// the master label; the flip mechanics are shared with sentinel mode
// (applyRoleLabels), including the terminating-pod churn guard.
func (r *LittleRedReconciler) updateFailoverMasterLabel(ctx context.Context, littleRed *littleredv1alpha1.LittleRed, eng *failoverEngineView) error {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)

	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(littleRed.Namespace), client.MatchingLabels(redisSelectorLabels(littleRed))); err != nil {
		return err
	}
	if len(podList.Items) == 0 {
		return nil
	}
	for _, pod := range podList.Items {
		if !pod.DeletionTimestamp.IsZero() {
			log.Info("Pod terminating, skipping label update to avoid churn during failover", "pod", pod.Name)
			return nil
		}
	}
	masterPodName := ""
	if eng != nil {
		masterPodName = eng.masterPodName
	}
	return r.applyRoleLabels(ctx, littleRed, podList, masterPodName)
}

// updateFailoverStatus updates the LittleRed status for failover mode. It
// mirrors updateSentinelStatus minus everything Sentinel: Running is gated on
// StatefulSet readiness, an observed intended master, and every replica
// reporting master_link_status:up in the engine's gather (instead of the
// Sentinel-known-replicas count). ConditionSentinelReady is never set.
//
//nolint:gocyclo
func (r *LittleRedReconciler) updateFailoverStatus(ctx context.Context, lr *littleredv1alpha1.LittleRed, eng *failoverEngineView) (ctrl.Result, error) {
	log := r.getLogger(ctx, lr, LogCategoryRecon)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		oldStatus := latest.Status.DeepCopy()

		failover := failoverSpecOrDefault(latest)
		expectedTotal := 1 + *failover.Replicas

		// Redis StatefulSet status
		redisSts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Name: statefulSetName(latest), Namespace: latest.Namespace}, redisSts); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			latest.Status.Redis.Ready = 0
			latest.Status.Redis.Total = expectedTotal
		} else {
			latest.Status.Redis.Ready = redisSts.Status.ReadyReplicas
			latest.Status.Redis.Total = *redisSts.Spec.Replicas
		}

		// Replica status (data pods minus the master)
		if latest.Status.Replicas == nil {
			latest.Status.Replicas = &littleredv1alpha1.ReplicaStatus{}
		}
		if latest.Status.Redis.Ready > 0 {
			latest.Status.Replicas.Ready = latest.Status.Redis.Ready - 1
		} else {
			latest.Status.Replicas.Ready = 0
		}
		latest.Status.Replicas.Total = latest.Status.Redis.Total - 1

		// Master info: the operator's intent, once observed live.
		if latest.Status.Master == nil {
			latest.Status.Master = &littleredv1alpha1.MasterStatus{}
		}
		masterPodName := ""
		replicasLinkedUp := 0
		if eng != nil {
			masterPodName = eng.masterPodName
			replicasLinkedUp = eng.replicasLinkedUp
		}
		latest.Status.Master.PodName = masterPodName
		if masterPodName != "" {
			latest.Status.Master.IP = eng.intent.masterIP
		} else {
			latest.Status.Master.IP = ""
		}

		// Failover monitoring mirrors: the assignment epoch (masterDownSince /
		// transitionSince are maintained by the engine's marker helpers).
		if latest.Status.Failover == nil {
			latest.Status.Failover = &littleredv1alpha1.FailoverStatus{}
		}
		if eng != nil {
			latest.Status.Failover.AssignmentEpoch = eng.intent.maxEpoch
		}

		// Phase: Running iff every pod is ready AND the intended master is
		// observed live AND every replica's link is up in the gather.
		expectedReplicas := int(latest.Status.Redis.Total) - 1
		allReady := latest.Status.Redis.Ready == latest.Status.Redis.Total &&
			latest.Status.Redis.Ready > 0 &&
			masterPodName != "" &&
			replicasLinkedUp >= expectedReplicas

		if allReady {
			latest.Status.Phase = littleredv1alpha1.PhaseRunning
			latest.Status.BootstrapRequired = false

			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionReady,
				Status:             metav1.ConditionTrue,
				Reason:             reasonAllPodsReady,
				Message:            "All Redis pods are ready and replicating from the intended master",
				LastTransitionTime: metav1.Now(),
			})
			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionInitialized,
				Status:             metav1.ConditionTrue,
				Reason:             reasonInitialized,
				Message:            "Redis failover instance is initialized",
				LastTransitionTime: metav1.Now(),
			})
		} else {
			var notReadyReasons []string
			if latest.Status.Redis.Ready == 0 {
				notReadyReasons = append(notReadyReasons, "no Redis pods ready")
			} else if latest.Status.Redis.Ready != latest.Status.Redis.Total {
				notReadyReasons = append(notReadyReasons, fmt.Sprintf("Redis pods %d/%d ready", latest.Status.Redis.Ready, latest.Status.Redis.Total))
			}
			if masterPodName == "" {
				notReadyReasons = append(notReadyReasons, "intended master not observed live yet")
			} else if replicasLinkedUp < expectedReplicas {
				notReadyReasons = append(notReadyReasons, fmt.Sprintf("replica links up %d/%d", replicasLinkedUp, expectedReplicas))
			}
			log.Info("Not yet Running, requeueing", "reasons", strings.Join(notReadyReasons, "; "))

			latest.Status.Phase = littleredv1alpha1.PhaseInitializing
			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               littleredv1alpha1.ConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             reasonPodsNotReady,
				Message:            fmt.Sprintf("Redis: %d/%d, replica links up: %d/%d", latest.Status.Redis.Ready, latest.Status.Redis.Total, replicasLinkedUp, expectedReplicas),
				LastTransitionTime: metav1.Now(),
			})
		}

		latest.Status.ObservedGeneration = latest.Generation
		if latest.Status.Phase == littleredv1alpha1.PhaseRunning {
			latest.Status.Status = littleredv1alpha1.ConditionReady
		} else {
			latest.Status.Status = string(latest.Status.Phase)
		}

		if !reflect.DeepEqual(oldStatus, &latest.Status) {
			return r.Status().Update(ctx, latest)
		}
		return nil
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	// Re-fetch for requeue logic
	latest := &littleredv1alpha1.LittleRed{}
	if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
		return ctrl.Result{}, err
	}

	fast, steady := latest.GetRequeueIntervals()
	if latest.Status.Phase != littleredv1alpha1.PhaseRunning {
		return ctrl.Result{RequeueAfter: fast}, nil
	}
	if latest.Annotations[AnnotationDisablePolling] == annotationValueTrue {
		log.Info("Failover polling disabled via annotation")
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: steady}, nil
}

// --- status marker / condition helpers (mirror the Rule L pattern) ----------

// setFailoverMasterDownSince stamps status.failover.masterDownSince (the
// detection-window anchor). No-op if already stamped.
func (r *LittleRedReconciler) setFailoverMasterDownSince(ctx context.Context, lr *littleredv1alpha1.LittleRed, t metav1.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.Failover != nil && latest.Status.Failover.MasterDownSince != nil {
			lr.Status.Failover = latest.Status.Failover
			return nil
		}
		if latest.Status.Failover == nil {
			latest.Status.Failover = &littleredv1alpha1.FailoverStatus{}
		}
		latest.Status.Failover.MasterDownSince = &t
		lr.Status.Failover = latest.Status.Failover
		return r.Status().Update(ctx, latest)
	})
}

// clearFailoverMasterDownSince resets the detection-window marker once the
// master is reachable again. No-op (no API call) when already clear.
func (r *LittleRedReconciler) clearFailoverMasterDownSince(ctx context.Context, lr *littleredv1alpha1.LittleRed) error {
	if lr.Status.Failover == nil || lr.Status.Failover.MasterDownSince == nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.Failover == nil || latest.Status.Failover.MasterDownSince == nil {
			lr.Status.Failover = latest.Status.Failover
			return nil
		}
		latest.Status.Failover.MasterDownSince = nil
		lr.Status.Failover = latest.Status.Failover
		return r.Status().Update(ctx, latest)
	})
}

// markFailoverTransition records a completed intent stamp: transitionSince
// anchors the post-transition cooldown, assignmentEpoch mirrors the stamped
// epoch, and masterDownSince is cleared (the failover this window detected is
// now answered). All monitoring surfaces — nothing here is load-bearing.
func (r *LittleRedReconciler) markFailoverTransition(ctx context.Context, lr *littleredv1alpha1.LittleRed, epoch int64) error {
	now := metav1.Now()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.Failover == nil {
			latest.Status.Failover = &littleredv1alpha1.FailoverStatus{}
		}
		latest.Status.Failover.TransitionSince = &now
		latest.Status.Failover.AssignmentEpoch = epoch
		latest.Status.Failover.MasterDownSince = nil
		lr.Status.Failover = latest.Status.Failover
		return r.Status().Update(ctx, latest)
	})
}

// setFailoverRecoveryCondition updates the FailoverRecovery condition (retry on
// conflict). Used for the refuse-and-wait state and its Recovered clearance.
func (r *LittleRedReconciler) setFailoverRecoveryCondition(ctx context.Context, lr *littleredv1alpha1.LittleRed, status metav1.ConditionStatus, reason, message string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionFailoverRecovery, Status: status, Reason: reason, Message: message,
		})
		lr.Status.Conditions = latest.Status.Conditions
		return r.Status().Update(ctx, latest)
	})
}

// clearFailoverRecoveryCondition records a completed recovery on the
// FailoverRecovery condition. No-op (no API call) unless the condition is
// currently True, so healthy instances never grow a spurious condition.
func (r *LittleRedReconciler) clearFailoverRecoveryCondition(ctx context.Context, lr *littleredv1alpha1.LittleRed) error {
	c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionFailoverRecovery)
	if c == nil || c.Status != metav1.ConditionTrue {
		return nil
	}
	return r.setFailoverRecoveryCondition(ctx, lr, metav1.ConditionFalse, reasonRecovered, "A live master is known again.")
}
