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
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// reshardConsolidated is the LR-018 repair step (Step 3b). It restores a distinct
// slot-owning master per shard when the cluster has drifted into the consolidated
// state — one master owning more than one shard range while other masters sit empty —
// which no other repair branch can heal (see docs/CLUSTER_CONSOLIDATED_SHARD_RECOVERY.md).
//
// The decision is the pure PlanReshard seam; this method only executes one move per
// reconcile and returns acted=true (with a fast requeue) whenever it drives or waits on
// a migration, so the caller returns immediately. It preserves keys.
//
// Returns (result, acted): acted=true means the caller should return result now.
// Transient failures are logged and turned into a fast requeue rather than a hard error
// (matching the rest of repairCluster), so there is no error return.
func (r *LittleRedReconciler) reshardConsolidated(
	ctx context.Context,
	littleRed *littleredv1alpha1.LittleRed,
	gt *redisclient.ClusterGroundTruth,
	clusterClient *redisclient.ClusterClient,
) (ctrl.Result, bool) {
	fast, _ := littleRed.GetRequeueIntervals()
	stateLog := r.getLogger(ctx, littleRed, LogCategoryState)
	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)

	plan := redisclient.PlanReshard(gt, littleRed.Spec.Cluster.Shards)
	if len(plan.Moves) == 0 {
		// Nothing to do, or not actionable (e.g. no empty master yet). Only log the
		// non-trivial deferrals; "one master per shard" is the healthy steady state.
		if plan.Reason != "" && !strings.HasPrefix(plan.Reason, "one master per shard") {
			stateLog.Info("Consolidated-shard reshard deferred", "reason", plan.Reason)
		}
		return ctrl.Result{}, false
	}

	// One move per reconcile; the next gather observes progress and re-plans.
	move := plan.Moves[0]

	auditLog.Info("Consolidated-shard reshard: relocating surplus range (LR-018)",
		"range", fmt.Sprintf("%d-%d", move.Start, move.End),
		"source", move.Source.PodName, "dest", move.Dest.PodName,
		"atomicSlotMigration", gt.AtomicSlotMigration)

	// Drive one pass of the move via the shared executor (ASM or dance by capability).
	// A move was planned, so we always acted this pass (acted=true); the caller returns
	// the requeue and the next gather observes progress and re-plans. Transient failures
	// are already turned into a fast requeue inside moveSlotRange (no hard error surfaces).
	_, res, err := r.moveSlotRange(ctx, littleRed, gt, clusterClient, &move)
	if err != nil {
		auditLog.Error(err, "Consolidated-shard reshard move failed; will retry",
			"range", fmt.Sprintf("%d-%d", move.Start, move.End))
		return ctrl.Result{RequeueAfter: fast}, true
	}
	return res, true
}

// moveSlotRange executes ONE pass of moving a single slot range (move.Source → move.Dest),
// choosing native atomic slot migration (Redis 8.4+) or the pre-8.4 key-preserving MIGRATE
// dance by the gather-time capability probe (gt.AtomicSlotMigration). It is the shared
// executor behind both the consolidated-shard reshard (LR-018, reshardConsolidated) and the
// legacy→per-shard migration Draining phase (ADR-013, migrateLegacyCluster). It preserves
// keys and is resumable from on-node markers; transient Redis failures are logged and turned
// into a fast requeue (never a hard error), matching the rest of the repair loop, so err is
// nil in practice.
//
// done reports whether this pass completed the move: true only on the dance's ownership-flip
// pass (the range is now on Dest). For native ASM, completion is instead observed by the next
// gather's re-plan (Dest owns the range), so done stays false while ASM drives/waits — both
// callers requeue and re-plan regardless, so done is advisory. res is the requeue result the
// caller should return.
//
// err is part of the shared-executor contract (both callers propagate it), but is nil in
// practice today: transient Redis failures are intentionally logged and turned into a fast
// requeue, matching the rest of the repair loop, rather than surfaced as hard errors.
//
//nolint:unparam // err reserved by the executor contract; transient failures requeue, not error.
func (r *LittleRedReconciler) moveSlotRange(
	ctx context.Context,
	littleRed *littleredv1alpha1.LittleRed,
	gt *redisclient.ClusterGroundTruth,
	clusterClient *redisclient.ClusterClient,
	move *redisclient.ReshardMove,
) (done bool, res ctrl.Result, err error) {
	fast, _ := littleRed.GetRequeueIntervals()
	stateLog := r.getLogger(ctx, littleRed, LogCategoryState)
	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)

	destAddr := fmt.Sprintf("%s:%d", move.Dest.PodIP, littleredv1alpha1.RedisPort)

	if !gt.AtomicSlotMigration {
		// Pre-8.4 / non-ASM engine: key-preserving incremental MIGRATE dance.
		danceDone, danceRes := r.reshardViaDance(ctx, littleRed, gt, clusterClient, *move)
		return danceDone, danceRes, nil
	}

	// Native atomic slot migration (Redis 8.4+). Re-entrant: if a task is already in
	// flight on the destination, wait for it (the gather shows completion when the
	// destination owns the range) rather than relaunching IMPORT.
	inFlight, qErr := clusterClient.ClusterMigrationInFlight(ctx, destAddr)
	if qErr != nil {
		auditLog.Error(qErr, "Failed to query atomic slot migration status; will retry", "dest", move.Dest.PodName)
		return false, ctrl.Result{RequeueAfter: fast}, nil
	}
	if inFlight {
		stateLog.Info("Atomic slot migration in progress; waiting", "dest", move.Dest.PodName,
			"range", fmt.Sprintf("%d-%d", move.Start, move.End))
		return false, ctrl.Result{RequeueAfter: fast}, nil
	}

	taskID, iErr := clusterClient.ClusterMigrationImport(ctx, destAddr, [][2]int{{move.Start, move.End}})
	if iErr != nil {
		auditLog.Error(iErr, "Failed to start atomic slot migration; will retry",
			"dest", move.Dest.PodName, "range", fmt.Sprintf("%d-%d", move.Start, move.End))
		return false, ctrl.Result{RequeueAfter: fast}, nil
	}
	auditLog.Info("Started atomic slot migration", "taskID", taskID,
		"dest", move.Dest.PodName, "range", fmt.Sprintf("%d-%d", move.Start, move.End))
	return false, ctrl.Result{RequeueAfter: fast}, nil
}

// reshardViaDance is the pre-8.4 key-preserving executor: the classic
// IMPORTING/MIGRATING → MIGRATE key batches → SETSLOT NODE dance, made incremental
// across reconciles. Slot ownership flips only once the whole range is drained, so
// the source keeps owning the range in gossip throughout — PlanReshard keeps emitting
// the same move and this method resumes it (state lives in the cluster's slot markers,
// not in the operator). Bounded to ReshardMaxKeysPerReconcile keys per pass so it never
// hogs the single reconcile worker. See LR-018 §7.2.
// Returns (done, res): done is true only on the ownership-flip pass (the range is fully
// drained and now owned by Dest); res is the fast requeue the caller returns each pass.
func (r *LittleRedReconciler) reshardViaDance(
	ctx context.Context,
	littleRed *littleredv1alpha1.LittleRed,
	gt *redisclient.ClusterGroundTruth,
	clusterClient *redisclient.ClusterClient,
	move redisclient.ReshardMove,
) (done bool, res ctrl.Result) {
	fast, _ := littleRed.GetRequeueIntervals()
	stateLog := r.getLogger(ctx, littleRed, LogCategoryState)
	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)

	batch := littleRed.Spec.Cluster.ReshardKeyBatchSize
	if batch <= 0 {
		batch = 128
	}
	maxKeys := littleRed.Spec.Cluster.ReshardMaxKeysPerReconcile
	if maxKeys <= 0 {
		maxKeys = 2000
	}
	migTimeout := littleRed.Spec.Cluster.ReshardMigrateTimeoutMillis
	if migTimeout <= 0 {
		migTimeout = 5000
	}

	srcAddr := fmt.Sprintf("%s:%d", move.Source.PodIP, littleredv1alpha1.RedisPort)
	destAddr := fmt.Sprintf("%s:%d", move.Dest.PodIP, littleredv1alpha1.RedisPort)

	slots := make([]int, 0, move.End-move.Start+1)
	for s := move.Start; s <= move.End; s++ {
		slots = append(slots, s)
	}

	// 1. Mark importing (dest) + migrating (source), idempotent. Enables ASK
	//    redirection and lets the destination accept MIGRATE'd keys.
	if err := clusterClient.ClusterSetSlotsImporting(ctx, destAddr, slots, move.Source.NodeID); err != nil {
		auditLog.Error(err, "reshard dance: mark importing failed; will retry", "dest", move.Dest.PodName)
		return false, ctrl.Result{RequeueAfter: fast}
	}
	if err := clusterClient.ClusterSetSlotsMigrating(ctx, srcAddr, slots, move.Dest.NodeID); err != nil {
		auditLog.Error(err, "reshard dance: mark migrating failed; will retry", "source", move.Source.PodName)
		return false, ctrl.Result{RequeueAfter: fast}
	}

	// 2. Which slots still hold keys on the source?
	counts, err := clusterClient.ClusterCountKeysInSlots(ctx, srcAddr, slots)
	if err != nil {
		auditLog.Error(err, "reshard dance: count keys failed; will retry", "source", move.Source.PodName)
		return false, ctrl.Result{RequeueAfter: fast}
	}
	toDrain := redisclient.SlotsNeedingDrain(counts)

	// 3. Fully drained → flip ownership on every reachable master (converges gossip and
	//    clears the importing/migrating markers). Only now does the range change hands.
	if len(toDrain) == 0 {
		for _, addr := range reachableMasterAddrs(gt) {
			if err := clusterClient.ClusterSetSlotsNode(ctx, addr, slots, move.Dest.NodeID); err != nil {
				auditLog.Error(err, "reshard dance: flip ownership failed; will retry", "addr", addr)
				return false, ctrl.Result{RequeueAfter: fast}
			}
		}
		auditLog.Info("reshard dance: drain complete, ownership flipped to dest",
			"range", fmt.Sprintf("%d-%d", move.Start, move.End), "dest", move.Dest.PodName)
		return true, ctrl.Result{RequeueAfter: fast}
	}

	// 4. Drain up to maxKeys keys this reconcile, then yield (resume next pass).
	moved := 0
	for _, slot := range toDrain {
		for moved < maxKeys {
			keys, err := clusterClient.ClusterGetKeysInSlot(ctx, srcAddr, slot, batch)
			if err != nil {
				auditLog.Error(err, "reshard dance: GETKEYSINSLOT failed; will retry", "slot", slot)
				return false, ctrl.Result{RequeueAfter: fast}
			}
			if len(keys) == 0 {
				break
			}
			if err := clusterClient.MigrateKeys(ctx, srcAddr, move.Dest.PodIP, littleredv1alpha1.RedisPort, migTimeout, keys...); err != nil {
				// A single un-migratable key (e.g. a value too large to move within the
				// MIGRATE timeout) stalls the reshard here. Never silent: log loudly with
				// the slot so the field can see it, and retry next reconcile.
				auditLog.Error(err, "reshard dance: MIGRATE failed (possible oversized key); will retry",
					"slot", slot, "batch", len(keys), "migrateTimeoutMs", migTimeout)
				return false, ctrl.Result{RequeueAfter: fast}
			}
			moved += len(keys)
		}
		if moved >= maxKeys {
			break
		}
	}
	stateLog.Info("reshard dance: migrated key batch this pass (incremental)",
		"range", fmt.Sprintf("%d-%d", move.Start, move.End),
		"movedThisPass", moved, "slotsRemaining", len(toDrain))
	return false, ctrl.Result{RequeueAfter: fast}
}

// reachableMasterAddrs returns host:port for every reachable master in the ground
// truth — the set to broadcast the final SETSLOT NODE flip to for prompt convergence.
func reachableMasterAddrs(gt *redisclient.ClusterGroundTruth) []string {
	var addrs []string
	for _, n := range gt.Nodes {
		if n.Reachable && n.Role == RoleMaster {
			addrs = append(addrs, fmt.Sprintf("%s:%d", n.PodIP, littleredv1alpha1.RedisPort))
		}
	}
	return addrs
}
