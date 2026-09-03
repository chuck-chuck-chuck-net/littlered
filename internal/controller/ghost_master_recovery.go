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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// ghostMasterRecoveryCooldown is how long the ghost-master deadlock signature must persist
// before the operator intervenes, so a recent master death gets its full Sentinel
// down-after + election window first. Mirrors leaderlessRecoveryCooldown.
const ghostMasterRecoveryCooldown = 30 * time.Second

// planGhostMasterRecovery is the pure decision function for the ghost-master failover
// deadlock — the sibling of planLeaderlessRecovery. It reuses the same recoveryAction /
// leaderlessPlan vocabulary; only the detection gate and the safety gate differ.
//
// The deadlock: a majority of Sentinels are pinned to a *ghost* master IP (a dead pod)
// with no promotable replica, so failover aborts `no-good-slave` forever, while living
// survivor pods hold the data. LR-008 ghost-master correction cannot act (no living
// consensus master to MONITOR) and Rule L cannot act (the Sentinels are not bare). The
// operator breaks it (pillar 3.5, External Knowledge) by electing the best survivor.
//
// Guard order:
//  1. Not the ghost-master deadlock (no majority-ghost master, OR Sentinel still knows a
//     healthy replica so a legitimate failover is imminent, OR below quorum) -> clear the
//     marker, do nothing. The healthy-replica check is what keeps us from stealing a
//     failover that Sentinel is about to perform on its own after a *recent* master death.
//  2. First observation -> start the cooldown.
//  3. Within the cooldown -> wait (give Sentinel its full down-after + election window).
//  4. Cooldown elapsed -> act by data-holder set: 0 -> seed the bootstrap master; >=1 ->
//     elect the most-complete survivor (BestDataHolder). Unlike Rule L, the safety gate is
//     REPLICATION LINEAGE, not holder count: same-master survivors (one replid) are safe to
//     elect from with no opt-in (it is exactly what Sentinel would have done); only genuinely
//     DIVERGED lineages require the unsafe opt-in.
//
// The caller invokes this only while state.RealMasterIP == "" (majority-ghost implies it).
func planGhostMasterRecovery(
	state *redisclient.ReplicationState,
	quorum int,
	allowUnsafe bool,
	bootstrapMasterIP string,
	stuckSince *time.Time,
	now time.Time,
	cooldown time.Duration,
) leaderlessPlan {
	// 1. Detection gate. Not the ghost-master deadlock if no majority-ghost master, or
	// Sentinel still knows a healthy replica (a legitimate failover is imminent — do not
	// steal it), or too few Sentinels are reachable to act safely.
	if !state.SentinelsMonitorGhostMaster() || state.HasHealthyKnownReplica() {
		return leaderlessPlan{action: recoveryClearMarker}
	}
	if state.ReachableSentinels() < quorum {
		return leaderlessPlan{action: recoveryClearMarker}
	}

	// 2-3. Persistence gate: observe, then wait out the cooldown so Sentinel gets its full
	// down-after + election window before the operator intervenes.
	if stuckSince == nil {
		return leaderlessPlan{action: recoveryStartCooldown}
	}
	if now.Sub(*stuckSince) < cooldown {
		return leaderlessPlan{action: recoveryWait}
	}

	// 4. LR-051: the same veto as Rule L's, for the same reason — everything below
	// this line elects a master, and a pod that refused our credential is a live
	// server whose keyspace we cannot read. The 0-holder branch is the dangerous one
	// here too: it seeds the bootstrap master over what may be the whole dataset.
	if plan, veto := unprovablyEmptyVeto(state); veto {
		return plan
	}

	// 5. Act by data-holder set.
	holders := state.DataHolders()
	if len(holders) == 0 {
		if bootstrapMasterIP == "" {
			return leaderlessPlan{action: recoveryWait}
		}
		return leaderlessPlan{action: recoverySeedNoData, masterIP: bootstrapMasterIP}
	}

	best, diverged, forked := state.BestDataHolder()
	if best == nil { // defensive; holders is non-empty so this cannot happen
		return leaderlessPlan{action: recoveryWait, holders: len(holders)}
	}
	// Safety gate: replication LINEAGE, not holder count. Same-master survivors (one replid)
	// are safe to elect from — the losers re-sync from the elected master with no divergent
	// writes lost — so no opt-in. Only genuinely diverged lineages need the unsafe opt-in.
	//
	// LR-057: lineage alone is NOT sufficient, and forked is the missing half. A promotion
	// rotates the replid into replid2, so the union-find connects a promotion CHAIN and a
	// promotion FORK identically — and only the chain carries LR-024's premise that "the
	// survivors are replicas of the same dead master". Two holders that have each been
	// writable have appended to their own streams, so BestDataHolder's offset comparison is
	// across two units and electing the higher discards the other's acknowledged writes.
	// Refusing LOUDLY into the opt-in that already exists for exactly this decision is
	// LR-047's asymmetry (a loud stall beats silent destruction); it is not LR-043's
	// permanent-stall hazard, because the knob is a documented way out and the state cannot
	// be produced by the empty-master transient (an empty pod is never a DataHolder).
	if diverged || forked {
		if !allowUnsafe {
			// diverged is deliberately NOT set here: the pre-existing decision-table
			// rows pin it false on a refusal, and reusing it to carry the REASON
			// would edit their contract (LR-048's K2b). forked is a new field, so it
			// can carry the distinction the message needs without touching them.
			return leaderlessPlan{action: recoveryRefuse, forked: forked, holders: len(holders)}
		}
		return leaderlessPlan{action: recoveryUnsafeElect, masterIP: best.IP, diverged: diverged, forked: forked, holders: len(holders)}
	}
	return leaderlessPlan{action: recoveryPromoteSurvivor, masterIP: best.IP, holders: len(holders)}
}

// recoverGhostMasterDeadlock executes the plan from planGhostMasterRecovery. It mirrors
// recoverLeaderlessDeadlock but keys on Status.GhostMasterStuckSince and the
// GhostMasterRecovery condition. The caller invokes it only while RealMasterIP == "".
func (r *LittleRedReconciler) recoverGhostMasterDeadlock(
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
	if lr.Status.GhostMasterStuckSince != nil {
		since = &lr.Status.GhostMasterStuckSince.Time
	}
	bootstrapMasterIP := r.pickBootstrapMasterIP(lr, redisMap)

	plan := planGhostMasterRecovery(state, quorum, allowUnsafe, bootstrapMasterIP, since, time.Now(), ghostMasterRecoveryCooldown)

	switch plan.action {
	case recoveryClearMarker:
		return r.clearGhostMasterStuckSince(ctx, lr, reasonRecovered, "No longer stuck on a ghost master.")

	case recoveryStartCooldown:
		log.Info("Ghost-master failover deadlock suspected; starting cooldown before recovery",
			"cooldown", ghostMasterRecoveryCooldown.String())
		return r.setGhostMasterStuckSince(ctx, lr, metav1.Now())

	case recoveryWait:
		log.Info("Ghost-master failover deadlock persists; waiting", "holders", plan.holders,
			"cooldown", ghostMasterRecoveryCooldown.String())
		return nil

	case recoverySeedNoData:
		msg := fmt.Sprintf("Ghost-master recovery: no data present, seeded %s as master", redisMap[plan.masterIP])
		auditLog.Info(msg, "master", plan.masterIP, "masterPod", redisMap[plan.masterIP])
		if err := r.electMaster(ctx, lr, state, plan.masterIP, password, quorum); err != nil {
			return err
		}
		r.event(lr, corev1.EventTypeNormal, reasonReseeded, msg)
		return r.clearGhostMasterStuckSince(ctx, lr, reasonReseeded, msg)

	case recoveryPromoteSurvivor:
		// Sentinel is pinned to a dead master with no promotable replica; the survivors are
		// same-lineage replicas of that master, so electing the most-complete one discards
		// nothing (the others resync from it). Safe, no opt-in.
		h := state.RedisNodes[plan.masterIP]
		msg := fmt.Sprintf("Ghost-master recovery: Sentinel stuck on a dead master with no promotable replica; "+
			"electing survivor %s (keys=%d, offset=%d) via REMOVE+MONITOR — no data discarded", h.PodName, h.Keys, h.Offset)
		auditLog.Info(msg, "master", h.IP, "masterPod", h.PodName, "keys", h.Keys, "offset", h.Offset, "holders", plan.holders)
		if err := r.electMaster(ctx, lr, state, h.IP, password, quorum); err != nil {
			return err
		}
		r.event(lr, corev1.EventTypeNormal, reasonReseededFromSurvivor, msg)
		return r.clearGhostMasterStuckSince(ctx, lr, reasonReseededFromSurvivor, msg)

	case recoveryRefuse:
		msg := fmt.Sprintf("Ghost-master deadlock: %d survivors hold data across divergent replication lineages. "+
			"Refusing to elect (would discard independent writes). Set sentinel.allowUnsafeRebootstrapOnDeadlock=true "+
			"to authorize, or intervene manually.", plan.holders)
		if plan.forked {
			// LR-057. Different situation, different sentence: these two SHARE a
			// history — the union-find sees one lineage — but each has been writable
			// since they parted, so their replication offsets are not comparable and
			// the higher one is not a superset. Telling the owner "divergent
			// lineages" here would point them at the wrong diagnosis.
			msg = fmt.Sprintf("Ghost-master deadlock: %d survivors hold data and MORE THAN ONE of them "+
				"is a master, so each has accepted writes independently since they parted. Their "+
				"replication offsets are not comparable, so electing the highest would silently "+
				"discard the other's acknowledged writes. Refusing to elect. Identify which pod "+
				"holds the writes you need, or set sentinel.allowUnsafeRebootstrapOnDeadlock=true "+
				"to authorize discarding the rest.", plan.holders)
		}
		log.Info(msg)
		r.event(lr, corev1.EventTypeWarning, reasonRefusedDataPresent, msg)
		return r.setGhostMasterCondition(ctx, lr, metav1.ConditionTrue, reasonRefusedDataPresent, msg)

	case recoveryRefuseUnverified:
		// LR-051, the twin of Rule L's branch. See there.
		msg := fmt.Sprintf("Ghost-master deadlock: %s. Refusing to elect — a pod that refuses "+
			"the operator's credential is a live server whose keyspace cannot be read, so it "+
			"cannot be shown to be empty and electing around it may discard the entire dataset. "+
			"Fix the credential (see the OperatorCannotAuthenticate condition); "+
			"allowUnsafeRebootstrapOnDeadlock deliberately does NOT override this.",
			unverifiedPodSummary(plan.unverified))
		log.Info(msg)
		r.event(lr, corev1.EventTypeWarning, reasonRefusedDataUnverified, msg)
		return r.setGhostMasterCondition(ctx, lr, metav1.ConditionTrue, reasonRefusedDataUnverified, msg)

	case recoveryUnsafeElect:
		best := state.RedisNodes[plan.masterIP]
		msg := fmt.Sprintf("UNSAFE ghost-master recovery: force-elected %s (keys=%d, offset=%d); divergent data on %d "+
			"other survivor(s) will be DISCARDED via full resync", best.PodName, best.Keys, best.Offset, plan.holders-1)
		auditLog.Info(msg, "master", best.IP, "masterPod", best.PodName, "keys", best.Keys, "offset", best.Offset,
			"candidates", plan.holders, "divergedLineages", plan.diverged)
		if err := r.electMaster(ctx, lr, state, best.IP, password, quorum); err != nil {
			return err
		}
		r.event(lr, corev1.EventTypeWarning, reasonUnsafeRebootstrap, msg)
		return r.clearGhostMasterStuckSince(ctx, lr, reasonUnsafeRebootstrap, msg)
	}
	return nil
}

// setGhostMasterStuckSince stamps Status.GhostMasterStuckSince and sets the
// GhostMasterRecovery condition to True/DeadlockDetected (retry on conflict). No-op if
// already stamped. Mirrors setLeaderlessSince.
func (r *LittleRedReconciler) setGhostMasterStuckSince(ctx context.Context, lr *littleredv1alpha1.LittleRed, t metav1.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.GhostMasterStuckSince != nil {
			return nil
		}
		latest.Status.GhostMasterStuckSince = &t
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:    littleredv1alpha1.ConditionGhostMasterRecovery,
			Status:  metav1.ConditionTrue,
			Reason:  reasonDeadlockDetected,
			Message: "A majority of Sentinels are pinned to a dead master with no promotable replica; waiting out the recovery cooldown.",
		})
		lr.Status.GhostMasterStuckSince = &t
		return r.Status().Update(ctx, latest)
	})
}

// setGhostMasterCondition updates the GhostMasterRecovery condition without touching the
// marker (used for the refuse-and-wait state). Retry on conflict.
func (r *LittleRedReconciler) setGhostMasterCondition(ctx context.Context, lr *littleredv1alpha1.LittleRed, status metav1.ConditionStatus, reason, message string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionGhostMasterRecovery, Status: status, Reason: reason, Message: message,
		})
		return r.Status().Update(ctx, latest)
	})
}

// clearGhostMasterStuckSince resets Status.GhostMasterStuckSince once the instance is no
// longer stuck on a ghost master, recording the outcome on the GhostMasterRecovery
// condition (Status=False). No-op if never stuck. Mirrors clearLeaderlessSince.
func (r *LittleRedReconciler) clearGhostMasterStuckSince(ctx context.Context, lr *littleredv1alpha1.LittleRed, reason, message string) error {
	if lr.Status.GhostMasterStuckSince == nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if latest.Status.GhostMasterStuckSince == nil {
			return nil
		}
		latest.Status.GhostMasterStuckSince = nil
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionGhostMasterRecovery, Status: metav1.ConditionFalse, Reason: reason, Message: message,
		})
		lr.Status.GhostMasterStuckSince = nil
		return r.Status().Update(ctx, latest)
	})
}
