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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// reconcileCluster reconciles cluster mode
func (r *LittleRedReconciler) reconcileCluster(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	log.Info("Reconciling cluster mode")

	// Set initial phase
	if littleRed.Status.Phase == "" {
		littleRed.Status.Phase = littleredv1alpha1.PhasePending
	}

	// Legacy single-STS migration (ADR-013). When a pre-0.3.0 single {name}-cluster
	// StatefulSet is present, the operator drives an in-place, online, data-safe migration
	// into the per-shard layout (drain slots old→new, then delete the emptied legacy
	// workload) instead of refusing forever. While a migration is in flight the driver owns
	// the reconcile (handled=true) and the steady-state repair loop below is fully suspended
	// (ADR-013 §6). A non-shape-preserving legacy topology is still refused (terminal),
	// inside the driver. This must run before ensureClusterResources so the per-shard
	// StatefulSets are stood up beside the legacy one under migration control, not blindly.
	if res, handled, err := r.migrateLegacyCluster(ctx, littleRed); handled || err != nil {
		return res, err
	}

	// Shard scale-down guard. Reducing spec.cluster.shards would orphan the high-index
	// shard StatefulSets and drop their slots; with EmptyDir storage that is data loss,
	// and there is no reshard-away path. We never delete data by default, so we refuse
	// and wait rather than remove the orphaned shards. See ADR per-shard StatefulSets.
	if orphans, err := r.detectOrphanedShardStatefulSets(ctx, littleRed); err != nil {
		return ctrl.Result{}, err
	} else if len(orphans) > 0 {
		return r.reportShardScaleDown(ctx, littleRed, orphans)
	}

	// 1. Ensure resources (ConfigMap, Services, per-shard StatefulSets)
	if err := r.ensureClusterResources(ctx, littleRed); err != nil {
		log.Error(err, "Failed to ensure cluster resources")
		return ctrl.Result{}, err
	}

	cluster := littleRed.Spec.Cluster
	if cluster == nil {
		cluster = &littleredv1alpha1.ClusterSpec{}
		cluster.SetDefaults()
	}

	// 2. Aggregate readiness across all per-shard StatefulSets.
	_, allShardsExist, readyReplicas, totalSpecReplicas, err := r.clusterShardStatefulSets(ctx, littleRed)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !allShardsExist {
		log.Info("Shard StatefulSet(s) not yet created, requeueing")
		fast, _ := littleRed.GetRequeueIntervals()
		return ctrl.Result{RequeueAfter: fast}, nil
	}

	expectedReplicas := int32(cluster.GetTotalNodes())
	allPodsReady := readyReplicas == expectedReplicas

	// 3. If not all pods ready, wait (update status to Initializing)
	if !allPodsReady {
		log.Info("Waiting for all pods to be ready",
			"ready", readyReplicas,
			"expected", expectedReplicas)

		// Total-/partial-wipe deadlock recovery. Pods stuck not-Ready and crash-looping
		// (redis down ⇒ no data in a pure in-memory cluster) can never become Ready on their
		// own — e.g. a mass container crash where every restarted master parks in the startup
		// yield loop with no live replica to fail over to. The operator recycles exactly those
		// pods (delete ⇒ StatefulSet reschedules fresh) after a cooldown, never touching a
		// Ready data holder, and the normal repair loop then re-bootstraps. Mutates the
		// WipeDeadlockSince cooldown marker, persisted by the Status().Update below. ADR-008.
		if err := r.recoverClusterWipeDeadlock(ctx, littleRed); err != nil {
			return ctrl.Result{}, err
		}

		littleRed.Status.Phase = littleredv1alpha1.PhaseInitializing
		littleRed.Status.Redis.Ready = readyReplicas
		littleRed.Status.Redis.Total = totalSpecReplicas

		if err := r.Status().Update(ctx, littleRed); err != nil {
			return ctrl.Result{}, err
		}

		fast, _ := littleRed.GetRequeueIntervals()
		return ctrl.Result{RequeueAfter: fast}, nil
	}

	// 4. All pods are ready. Gather Ground Truth.
	gt := r.gatherGroundTruth(ctx, littleRed)

	// Analyze state
	isHealthy := gt.IsHealthy(expectedReplicas, int32(cluster.Shards))

	// If cluster is not fully healthy or has topology issues, run repair loop
	if !isHealthy || gt.HasPartitions() || gt.HasGhostNodes() || gt.HasOrphanedSlots() || gt.HasEmptyMasters() {
		r.getLogger(ctx, littleRed, LogCategoryState).Info("Cluster not healthy or topology issues detected, running repair",
			"partitions", len(gt.Partitions),
			"ghosts", len(gt.GhostNodes),
			"orphanedSlots", gt.HasOrphanedSlots(),
			"emptyMasters", gt.HasEmptyMasters(),
			"masters", gt.CountMasters(),
			"allNodesView", len(gt.AllNodeIDs))
		return r.repairCluster(ctx, littleRed, gt)
	}

	// 5. Cluster is healthy and stable — pass gt through to avoid a second gather
	return r.updateClusterStatus(ctx, littleRed, gt)
}

// repairCluster handles healing: partitions, ghost nodes, slot restoration, and replication topology
//
//nolint:gocyclo
func (r *LittleRedReconciler) repairCluster(ctx context.Context, littleRed *littleredv1alpha1.LittleRed, gt *redisclient.ClusterGroundTruth) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	fast, _ := littleRed.GetRequeueIntervals()

	password := r.getRedisPassword(ctx, littleRed)
	clusterClient := redisclient.NewClusterClient(password, littleRed.Spec.TLS.Enabled)

	stateLog := r.getLogger(ctx, littleRed, LogCategoryState)
	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)

	// 0. Quorum Recovery (High Priority)
	// If we have lost quorum (majority of masters), the cluster cannot heal itself.
	// We must manually promote replicas whose masters are gone.
	shards := littleRed.Spec.Cluster.Shards
	votingMasters := gt.CountMasters()

	// Quorum is lost if available voting masters are <= shards / 2
	if votingMasters <= shards/2 {
		stateLog.Info("Quorum loss detected", "votingMasters", votingMasters, "targetShards", shards)

		// Build set of live node IDs for fast lookup
		liveNodes := make(map[string]bool)
		for _, n := range gt.Nodes {
			liveNodes[n.NodeID] = true
		}

		promotedCount := 0
		for _, node := range gt.Nodes {
			if node.Role == RoleReplica {
				// Skip if master is known/live or if master ID is invalid
				if node.MasterNodeID == "" || node.MasterNodeID == "-" || liveNodes[node.MasterNodeID] {
					continue
				}

				auditLog.Info("Promoting orphan replica during quorum loss",
					"pod", node.PodName,
					"missingMaster", node.MasterNodeID)

				addr := fmt.Sprintf("%s:%d", node.PodIP, littleredv1alpha1.RedisPort)
				if err := clusterClient.ClusterFailoverTakeover(ctx, addr); err != nil {
					auditLog.Error(err, "Failed to force takeover", "pod", node.PodName)
				} else {
					promotedCount++
				}
			}
		}

		if promotedCount > 0 {
			auditLog.Info("Promoted replicas to restore quorum", "count", promotedCount)
			// Wait for cluster state to settle
			return ctrl.Result{RequeueAfter: fast}, nil
		}
	}

	// 1. Heal Partitions (CLUSTER MEET)
	if gt.HasPartitions() {
		// Check for orphaned replicas whose master is a ghost.
		// Allow natural failover for a grace period, then force-promote if stuck.
		gracePeriod := 15 // default
		if littleRed.Spec.Cluster != nil && littleRed.Spec.Cluster.FailoverGracePeriod > 0 {
			gracePeriod = littleRed.Spec.Cluster.FailoverGracePeriod
		}
		orphanTimeout := time.Duration(littleRed.Spec.Cluster.ClusterNodeTimeout)*time.Millisecond +
			time.Duration(gracePeriod)*time.Second

		// Build lookup sets
		liveNodes := make(map[string]bool)
		for _, n := range gt.Nodes {
			liveNodes[n.NodeID] = true
		}
		ghostSet := make(map[string]bool)
		for _, g := range gt.GhostNodes {
			ghostSet[g] = true
		}

		// Reconcile orphan tracking: detect new orphans, check timeouts on existing ones
		now := metav1.Now()
		existingOrphans := make(map[string]*littleredv1alpha1.OrphanedReplicaInfo)
		if littleRed.Status.Cluster != nil {
			for i := range littleRed.Status.Cluster.OrphanedReplicas {
				o := &littleRed.Status.Cluster.OrphanedReplicas[i]
				existingOrphans[o.PodName] = o
			}
		}

		var currentOrphans []littleredv1alpha1.OrphanedReplicaInfo
		hasBlockingOrphans := false
		promotedCount := 0

		for _, node := range gt.Nodes {
			if node.Role != RoleReplica {
				continue
			}
			if node.MasterNodeID == "" || node.MasterNodeID == "-" || liveNodes[node.MasterNodeID] {
				continue
			}
			if !ghostSet[node.MasterNodeID] {
				continue // Master unknown — might be in transition
			}

			// This is an orphaned replica whose master is a ghost
			orphanInfo, tracked := existingOrphans[node.PodName]
			if !tracked {
				// New orphan — start tracking
				orphanInfo = &littleredv1alpha1.OrphanedReplicaInfo{
					PodName:      node.PodName,
					NodeID:       node.NodeID,
					MasterNodeID: node.MasterNodeID,
					DetectedAt:   now,
				}
			}

			age := now.Sub(orphanInfo.DetectedAt.Time)
			if age >= orphanTimeout {
				// Timeout exceeded — force-promote
				auditLog.Info("Force-promoting stuck orphan replica",
					"pod", node.PodName, "orphanAge", age, "timeout", orphanTimeout)
				addr := fmt.Sprintf("%s:%d", node.PodIP, littleredv1alpha1.RedisPort)
				if err := clusterClient.ClusterFailoverTakeover(ctx, addr); err != nil {
					auditLog.Error(err, "Failed to force takeover", "pod", node.PodName)
				} else {
					promotedCount++
				}
			} else {
				// Still within grace period — track and wait
				log.Info("Waiting for natural failover",
					"pod", node.PodName, "orphanAge", age, "timeout", orphanTimeout)
				currentOrphans = append(currentOrphans, *orphanInfo)
				hasBlockingOrphans = true
			}
		}

		// Persist orphan tracking (removes resolved orphans, adds new ones)
		if littleRed.Status.Cluster == nil {
			littleRed.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{}
		}

		// Only update if changes occurred to avoid unnecessary status updates
		if len(littleRed.Status.Cluster.OrphanedReplicas) != len(currentOrphans) || promotedCount > 0 {
			littleRed.Status.Cluster.OrphanedReplicas = currentOrphans
			if err := r.Status().Update(ctx, littleRed); err != nil {
				if !apierrors.IsConflict(err) {
					return ctrl.Result{}, err
				}
				return ctrl.Result{Requeue: true}, nil
			}
		}

		if promotedCount > 0 || hasBlockingOrphans {
			return ctrl.Result{RequeueAfter: fast}, nil
		}

		auditLog.Info("Healing partitions", "count", len(gt.Partitions))
		// CLUSTER MEET is the one Redis operation that creates a *fresh* identity
		// binding — it validates nothing about the target's cluster membership, and the
		// initiator adopts whatever node ID the responder reports. Pod IPs here come
		// from the cache-backed pod read, which LR-012 showed can be stale during churn,
		// and LR-039 proved pod IPs really are recycled across unrelated instances on a
		// shared pod network. So every address (targets AND the seed the command is
		// issued at) must first be attributed to this instance. See LR-043.
		plan := gt.PlanPartitionMeets()
		// Report every suppressed MEET — a silently skipped one is what would make a
		// future partition-healing bug undiagnosable. These are the no-evidence verdicts
		// only (nothing answered, or answered without a gossip view); they are routine
		// churn and self-clearing, so one state line per pass, not per node.
		var unknown []string
		for _, skip := range plan.Skipped {
			unknown = append(unknown, fmt.Sprintf("%s@%s (%s)", skip.PodName, skip.PodIP, skip.Verdict))
		}
		if len(unknown) > 0 {
			stateLog.Info("Skipping CLUSTER MEET: target not identified this pass",
				"targets", strings.Join(unknown, ", "))
		}
		// Addresses the cluster bus could not attribute to us. NOT a veto any more: the
		// API-server confirmation below is the stronger evidence and decides. Recorded so
		// that a genuine cross-instance merge — the LR-043 hazard — stays diagnosable,
		// and keyed by pod name so the warning is emitted next to the MEET it describes.
		unattributed := make(map[string]string, len(plan.Unattributed))
		for _, w := range plan.Unattributed {
			unattributed[w.PodName] = w.NodeID
		}
		if plan.Seed == nil {
			auditLog.Info("Skipping partition healing: no MEET seed identified this pass",
				"verdict", plan.SeedVerdict)
			return ctrl.Result{RequeueAfter: fast}, nil
		}
		// The deciding guard: confirm each address against the API server (uncached)
		// before introducing it. The gather's pod IPs come from the informer cache, which
		// is where the whole hazard originates; one uncached pod GET per MEET candidate
		// answers "is this still OUR live pod's address, right now" exactly, instead of
		// inferring it from Redis-side state. MEET path only — never the steady loop.
		if ok, why := r.confirmPodIP(ctx, littleRed.Namespace, plan.Seed.PodName, plan.Seed.PodIP); !ok {
			auditLog.Info("Skipping partition healing: MEET seed address is no longer confirmed as this pod's",
				"pod", plan.Seed.PodName, "seed", plan.Seed.PodIP, "reason", why)
			return ctrl.Result{RequeueAfter: fast}, nil
		}
		if plan.SeedVerdict == redisclient.MeetDenyUnattributed {
			auditLog.Info("MEET seed is confirmed as this pod's address but its cluster view names no node of ours; seeding anyway",
				"pod", plan.Seed.PodName, "seed", plan.Seed.PodIP, "nodeID", plan.Seed.NodeID)
		}
		seedAddr := fmt.Sprintf("%s:%d", plan.Seed.PodIP, littleredv1alpha1.RedisPort)
		for _, node := range plan.Targets {
			if ok, why := r.confirmPodIP(ctx, littleRed.Namespace, node.PodName, node.PodIP); !ok {
				auditLog.Info("Skipping CLUSTER MEET: address is no longer confirmed as this pod's",
					"pod", node.PodName, "target", node.PodIP, "reason", why)
				continue
			}
			if nodeID, ok := unattributed[node.PodName]; ok {
				auditLog.Info("CLUSTER MEET target is confirmed as this pod's address but its cluster view names no node of ours; meeting anyway",
					"pod", node.PodName, "target", node.PodIP, "nodeID", nodeID)
			}
			auditLog.Info("Meeting node", "seed", seedAddr, "target", node.PodIP, "pod", node.PodName)
			_ = clusterClient.ClusterMeet(ctx, seedAddr, node.PodIP, littleredv1alpha1.RedisPort)
		}
		return ctrl.Result{RequeueAfter: fast}, nil
	}

	// 2. Forget Ghost Nodes (With Safety Check)
	if gt.HasGhostNodes() {
		// Belt-and-suspenders (ADR-013 §6): never FORGET a node while a legacy {name}-cluster
		// StatefulSet exists. During migration the steady gather enumerates only the NEW pods,
		// so live legacy nodes appear as ghosts here; the migration driver (which normally owns
		// the reconcile via handled=true) is the sole authority over legacy-node lifecycle.
		// A stray steady-state pass must not evict a legacy node mid-migration.
		if legacyExists, _ := r.detectLegacyClusterStatefulSet(ctx, littleRed); legacyExists {
			stateLog.Info("Legacy cluster StatefulSet present; skipping ghost FORGET (migration owns legacy-node lifecycle)")
			return ctrl.Result{RequeueAfter: fast}, nil
		}
		// Safety: Don't forget a ghost if it is the master of a live replica.
		// We should wait for the replica to be promoted (Step 0) instead.
		protectedMasters := make(map[string]bool)
		for _, n := range gt.Nodes {
			if n.Role == RoleReplica && n.MasterNodeID != "" && n.MasterNodeID != "-" {
				protectedMasters[n.MasterNodeID] = true
			}
		}

		ghostsToRemove := make([]string, 0)
		for _, ghostID := range gt.GhostNodes {
			if protectedMasters[ghostID] {
				stateLog.Info("Skipping removal of ghost node because it is still a master of a live replica", "ghost", ghostID)
				continue
			}
			ghostsToRemove = append(ghostsToRemove, ghostID)
		}

		if len(ghostsToRemove) > 0 {
			stateLog.Info("Removing ghost nodes", "count", len(ghostsToRemove))
			for _, ghostID := range ghostsToRemove {
				auditLog.Info("Forgetting ghost node", "id", ghostID)
				for _, node := range gt.Nodes {
					// Only issue FORGET to live nodes; dialing an unreachable pod
					// would block the loop on dial retries for no benefit (LR-012).
					if !node.Reachable {
						continue
					}
					addr := fmt.Sprintf("%s:%d", node.PodIP, littleredv1alpha1.RedisPort)
					if err := clusterClient.ClusterForget(ctx, addr, ghostID); err != nil {
						log.Info("Failed to forget node (might already be gone)", "node", addr, "ghost", ghostID, "error", err)
					}
				}
			}
			return ctrl.Result{RequeueAfter: fast}, nil
		}
	}

	// 3. Recover Missing Shards (Strict Shard Validation)
	// We assume we never fragment shards. Verify assignments and restore missing shards.
	expectedRanges := redisclient.GenerateSlotRanges(shards)

	// Map shard index to the NodeID that holds it
	shardOwners := make([]string, shards)

	// Validate current assignments
	for _, node := range gt.Nodes {
		for _, slotStr := range node.Slots {
			start, end, err := redisclient.ParseSlotRange(slotStr)
			if err != nil {
				stateLog.Error(err, "Failed to parse slot range", "node", node.PodName, "range", slotStr)
				continue
			}

			// Check if this range matches exactly one of our expected shards
			matchedShardIdx := -1
			for i, r := range expectedRanges {
				if r.Start == start && r.End == end {
					matchedShardIdx = i
					break
				}
			}

			if matchedShardIdx == -1 {
				// Mismatch! Found a slot range that doesn't align with our shard definition.
				// This implies fragmentation or external manipulation.
				stateLog.Error(nil, "Cluster slot topology mismatch detected. Found fragmented or non-aligned slot range. Refusing to reconcile to avoid data loss.",
					"node", node.PodName,
					"foundRange", fmt.Sprintf("%d-%d", start, end),
					"expectedShards", shards)
				return ctrl.Result{RequeueAfter: fast}, nil // Retry later, maybe transient? Or stuck.
			}

			// Valid range found
			shardOwners[matchedShardIdx] = node.NodeID
		}
	}

	// Check for missing shards
	var missingShardIndices []int
	for i, owner := range shardOwners {
		if owner == "" {
			missingShardIndices = append(missingShardIndices, i)
		}
	}

	if len(missingShardIndices) > 0 {
		stateLog.Info("Detected missing shards", "count", len(missingShardIndices), "indices", missingShardIndices)

		// Find the intended master for each missing shard (strict: pod N owns shard N).
		// Never assign a shard to a different master — that causes split-ownership
		// and "Slot already busy" errors. If the intended master isn't available, wait.
		//
		// LR-018 hardening: the intended master must be a reachable EMPTY master.
		// Assigning a missing shard's range to a pod that already owns a *different*
		// range consolidates two shards onto one node and creates the consolidated-
		// shard deadlock (one master owning >1 range while others sit empty). When
		// roles have drifted so that pod N already owns some other shard, we defer
		// rather than pile a second range on it; PlanReshard (Step 3b) untangles an
		// already-consolidated cluster.
		intendedMasters := make(map[int]*redisclient.ClusterNodeState) // shardIdx -> Node
		for i := range shards {
			podName := shardMasterPodName(littleRed.Name, i)
			if node, ok := gt.Nodes[podName]; ok && redisclient.SafeMissingShardTarget(node) {
				intendedMasters[i] = node
			}
		}

		ops := 0
		for _, shardIdx := range missingShardIndices {
			targetNode := intendedMasters[shardIdx]
			if targetNode == nil {
				log.Info("Intended master for shard not available, waiting",
					"shardIdx", shardIdx,
					"expectedPod", shardMasterPodName(littleRed.Name, shardIdx))
				continue
			}

			targetRange := expectedRanges[shardIdx]
			addr := fmt.Sprintf("%s:%d", targetNode.PodIP, littleredv1alpha1.RedisPort)

			auditLog.Info("Assigning missing shard to master",
				"shardIdx", shardIdx,
				"range", fmt.Sprintf("%d-%d", targetRange.Start, targetRange.End),
				"target", targetNode.PodName)

			slots, _ := redisclient.ExpandSlotRange(redisclient.FormatSlotRange(targetRange.Start, targetRange.End))
			if err := clusterClient.ClusterAddSlots(ctx, addr, slots...); err != nil {
				auditLog.Error(err, "Failed to assign shard", "shardIdx", shardIdx)
			} else {
				ops++
			}
		}

		if ops > 0 {
			return ctrl.Result{RequeueAfter: fast}, nil
		}
	}

	// 3b. Consolidated-Shard Reshard (LR-018).
	// When all slots are assigned but a single master owns more than one shard range
	// (leaving other masters empty), no other step heals it: Step 3 sees no *missing*
	// range, and Step 4 has no under-replicated slot-master to attach the empties to.
	// PlanReshard detects this and relocates the surplus range onto an empty master,
	// preserving keys. Runs before Step 4 so the freshly-created third master exists
	// for the remaining empty master(s) to be reattached as its replica.
	if res, acted := r.reshardConsolidated(ctx, littleRed, gt, clusterClient); acted {
		return res, nil
	}

	// 4. Replication Repair (Non-Zero Replica Mode)
	isZeroReplicaMode := littleRed.Spec.Cluster.ReplicasPerShard != nil && *littleRed.Spec.Cluster.ReplicasPerShard == 0

	if !isZeroReplicaMode {
		emptyMasters := gt.GetEmptyMasters()
		shardsWithReplicas := gt.GetMastersWithReplicas()

		if len(emptyMasters) > 0 {
			stateLog.Info("Detected masters with no slots in replication mode, attempting to assign as replicas")

			expectedReplicas := 1
			if littleRed.Spec.Cluster.ReplicasPerShard != nil {
				expectedReplicas = *littleRed.Spec.Cluster.ReplicasPerShard
			}

			// Candidate masters, snapshotted once for a deterministic, shard-aware choice.
			candidates := make([]*redisclient.ClusterNodeState, 0, len(gt.Nodes))
			for _, m := range gt.Nodes {
				candidates = append(candidates, m)
			}

			for _, em := range emptyMasters {
				// Reattach to an under-replicated master in the empty pod's OWN shard,
				// keeping the Redis shard inside its shard StatefulSet (ADR-007); a
				// shard-blind choice here decouples shards from StatefulSets and defeats
				// single-domain-loss survivability. Falls back cross-shard only if no
				// same-shard master needs a replica (logged loudly below).
				targetMaster := chooseReattachTarget(em.PodName, candidates, shardsWithReplicas, expectedReplicas)

				if targetMaster != nil {
					if redisclient.ShardIndexFromPodName(targetMaster.PodName) != redisclient.ShardIndexFromPodName(em.PodName) {
						stateLog.Info("Reattaching empty pod cross-shard (no same-shard master needs a replica); shard/STS pairing may drift",
							"pod", em.PodName, "targetPod", targetMaster.PodName, "targetNodeID", targetMaster.NodeID)
					}
					// CLUSTER REPLICATE is executed on the empty master and requires it
					// to already know the target's NodeID; right after a CLUSTER MEET that
					// knowledge may not have propagated yet, and the command would fail with
					// "ERR Unknown node". Defer rather than issue a doomed command (pillar
					// 3.5); IsHealthy keeps us on the fast cadence (LR-014), so the next loop
					// retries within ~2s once gossip converges.
					if !gt.NodeKnows(em.NodeID, targetMaster.NodeID) {
						stateLog.Info("Empty master does not yet know its target master; deferring reattach until gossip converges",
							"pod", em.PodName, "targetNodeID", targetMaster.NodeID)
						continue
					}
					auditLog.Info("Assigning empty master as replica", "pod", em.PodName, "masterNodeID", targetMaster.NodeID)
					addr := fmt.Sprintf("%s:%d", em.PodIP, littleredv1alpha1.RedisPort)
					if err := clusterClient.ClusterReplicate(ctx, addr, targetMaster.NodeID); err != nil {
						auditLog.Error(err, "Failed to replicate", "pod", em.PodName)
					} else {
						return ctrl.Result{RequeueAfter: fast}, nil
					}
				}
			}
		}
	}

	if gt.TotalSlots == 0 {
		// Safety Guard: Only bootstrap if the cluster is truly empty (no slots AND no replicas).
		// If we have replicas, it implies a previous state existed, and we shouldn't overwrite it.
		hasReplicas := false
		for _, n := range gt.Nodes {
			if n.Role == RoleReplica {
				hasReplicas = true
				break
			}
		}

		if !hasReplicas {
			return r.bootstrapCluster(ctx, littleRed)
		}

		stateLog.Info("Cluster has 0 slots but contains replicas. Refusing to bootstrap to avoid data loss.", "replicas_detected", true)
		// Fall through to update status (will likely show as unhealthy/initializing)
	}

	return r.updateClusterStatus(ctx, littleRed, nil)
}

// Reasons confirmPodIP reports when an address cannot be confirmed as a pod's current IP.
const (
	podIPGone     = "pod-gone"
	podIPChanged  = "ip-changed"
	podIPUnknown  = "unknown-pod"
	podIPNoAnswer = "confirm-failed"
	// podIPTerminating: the pod object exists and still reports the address, but it is
	// under deletion. The kubelet writes Status.PodIP, so a terminating pod's object can
	// keep naming an address the CNI has already released and handed on — the one window
	// in which "our pod claims this IP" is not the same as "this IP is ours". Denying it
	// is what makes confirmPodIP strong enough to be the sole authority on ownership.
	// See the regression section of changelog LR-043.
	podIPTerminating = "pod-terminating"
)

// apiReader returns the uncached reader for the MEET paths, falling back to the cached
// Client when APIReader was not wired. The only production construction site
// (cmd/littlered/main.go) always sets it; the fallback exists for unit and envtest
// reconcilers, whose Client is itself a direct, uncached client — so the fallback is
// never a silent downgrade there.
func (r *LittleRedReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// confirmPodIP asks the API server directly (no informer cache) whether podName still
// holds ip. It returns false plus a short reason otherwise.
//
// This is the PRIMARY guard for CLUSTER MEET (LR-043), and it is a different kind of
// evidence from AttributeMeetTarget: Kubernetes holds at most one live pod per IP, so if
// our own pod object still reports this address then whatever answers there IS our pod —
// no inference from Redis-side state needed. Conversely a recycled IP is, by definition,
// no longer our pod's IP.
//
// It is also the DECIDING guard: bus-state attribution informs but does not overrule a
// positive answer here (see MeetVerdict.AdmissibleWhenConfirmed). That is only defensible
// because this check denies a TERMINATING pod, which is the sole window in which the pod
// object can name an address the CNI has handed on. See changelog LR-043, regression
// section.
func (r *LittleRedReconciler) confirmPodIP(ctx context.Context, namespace, podName, ip string) (bool, string) {
	if podName == "" || ip == "" {
		return false, podIPUnknown
	}
	pod := &corev1.Pod{}
	if err := r.apiReader().Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, podIPGone
		}
		// Could not confirm. Deny: an unconfirmed address is exactly what must not be
		// introduced to the cluster, and the fast requeue retries within ~2s.
		return false, podIPNoAnswer
	}
	if pod.DeletionTimestamp != nil {
		return false, podIPTerminating
	}
	if pod.Status.PodIP != ip {
		return false, podIPChanged
	}
	return true, ""
}

// gatherGroundTruth queries all pods to build a view of the cluster
func (r *LittleRedReconciler) gatherGroundTruth(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) *redisclient.ClusterGroundTruth {
	cluster := littleRed.Spec.Cluster
	password := r.getRedisPassword(ctx, littleRed)

	clusterPods := make(map[string]string)
	for _, ref := range ClusterPodRefs(littleRed.Name, cluster.Shards, clusterReplicasPerShard(cluster)) {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: littleRed.Namespace}, pod); err == nil && pod.Status.PodIP != "" {
			clusterPods[pod.Status.PodIP] = ref.Name
		}
	}

	g := &operatorGatherer{password: password, tlsEnabled: littleRed.Spec.TLS.Enabled}
	return redisclient.GatherClusterGroundTruth(ctx, g, clusterPods)
}

// bootstrapCluster initializes a new Redis Cluster
func (r *LittleRedReconciler) bootstrapCluster(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)
	auditLog.Info("Bootstrapping/Healing cluster")

	cluster := littleRed.Spec.Cluster
	if cluster == nil {
		cluster = &littleredv1alpha1.ClusterSpec{}
		cluster.SetDefaults()
	}

	password := r.getRedisPassword(ctx, littleRed)
	clusterClient := redisclient.NewClusterClient(password, littleRed.Spec.TLS.Enabled)
	refs := ClusterPodRefs(littleRed.Name, cluster.Shards, clusterReplicasPerShard(cluster))

	// Verify pods belong to the current revision of their shard StatefulSet before using
	// their IPs. After a delete-and-recreate, terminating pods from the old deployment may
	// still exist with stale IPs and the same names; using those IPs would poison the
	// cluster. With per-shard StatefulSets each shard carries its own revision, so gate
	// each pod against its own shard's CurrentRevision.
	shardSTSs, allShardsExist, _, _, err := r.clusterShardStatefulSets(ctx, littleRed)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !allShardsExist {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Gather all Pod IPs and Node IDs, keyed by pod name.
	podIPs := make(map[string]string, len(refs))
	nodeIDs := make(map[string]string, len(refs))
	// Per-pod attribution evidence for the MEET round below (LR-043): each address's own
	// CLUSTER NODES view and the slots it claims. The revision gate above bounds *which
	// deployment* a pod object belongs to, not how fresh its cached Status.PodIP is, so
	// these addresses carry the same recycled-IP exposure as the repair loop's.
	candidates := make(map[string]redisclient.MeetCandidate, len(refs))

	for _, ref := range refs {
		pod := &corev1.Pod{}
		// UNCACHED read (LR-043). Bootstrap MEETs every one of these addresses, and the
		// revision gate below bounds which *deployment* the pod object is from, not how
		// fresh its cached Status.PodIP is — so a pod recreated at an unchanged STS
		// revision would pass the gate carrying a stale IP that may now belong to
		// another instance's pod. Reading uncached here replaces the cached read rather
		// than adding to it: zero extra requests, and the IP we go on to MEET is by
		// construction the one the API server reports for this pod right now.
		if err := r.apiReader().Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: littleRed.Namespace}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("Bootstrap: pod not found (yet)", "pod", ref.Name)
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err
		}
		// A terminating pod's object can still report an address the CNI has released
		// and handed on — the one window in which the uncached read is not proof of
		// ownership. Treat it exactly like "not ready" and requeue. Same reasoning as
		// confirmPodIP's podIPTerminating; see changelog LR-043, regression section.
		if pod.DeletionTimestamp != nil {
			log.Info("Bootstrap: pod is terminating; its address is not confirmable", "pod", ref.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		currentRevision := shardSTSs[ref.ShardIdx].Status.CurrentRevision
		podRevision := pod.Labels["controller-revision-hash"]
		if pod.Status.PodIP == "" || currentRevision == "" || podRevision != currentRevision {
			log.Info("Bootstrap: pod not ready (no IP or stale revision)",
				"pod", ref.Name, "podRevision", podRevision, "stsRevision", currentRevision)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		podIPs[ref.Name] = pod.Status.PodIP

		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, littleredv1alpha1.RedisPort)
		id, err := clusterClient.GetMyID(ctx, addr)
		if err != nil {
			auditLog.Error(err, "Failed to get Node ID", "pod", ref.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		nodeIDs[ref.Name] = id

		view, viewErr := clusterClient.GetClusterNodes(ctx, addr)
		if viewErr != nil {
			// No view ⇒ no attribution evidence. Record the address as identified but
			// view-less so AttributeMeetTarget denies it rather than reading it as an
			// isolated fresh pod.
			log.Info("Bootstrap: could not read node's own cluster view; it will not be MEETed this pass",
				"pod", ref.Name, "error", viewErr)
			candidates[ref.Name] = redisclient.MeetCandidate{
				PodName: ref.Name, PodIP: pod.Status.PodIP, NodeID: id, Identified: true,
			}
			continue
		}
		candidates[ref.Name] = redisclient.MeetCandidateFromNodes(ref.Name, pod.Status.PodIP, id, view)
	}

	// 1. CLUSTER MEET: everyone meets shard 0's master (the seed node), but only at
	// addresses attributable to this instance (LR-043).
	if !r.bootstrapMeetRound(ctx, littleRed, clusterClient, refs, podIPs, nodeIDs, candidates) {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Wait for gossip to propagate slightly
	time.Sleep(2 * time.Second)

	// 2. Assign Slots to Masters (shard K's master is {name}-shard-K-0).
	if littleRed.Annotations[AnnotationDebugSkipSlotAssignment] == annotationValueTrue {
		auditLog.Info("DEBUG: Skipping slot assignment due to annotation")
	} else {
		slotRanges := redisclient.GenerateSlotRanges(cluster.Shards)

		for k := range cluster.Shards {
			masterName := shardMasterPodName(littleRed.Name, k)
			masterAddr := fmt.Sprintf("%s:%d", podIPs[masterName], littleredv1alpha1.RedisPort)
			masterID := nodeIDs[masterName]

			nodes, err := clusterClient.GetClusterNodes(ctx, masterAddr)
			if err == nil {
				hasSlots := false
				for _, n := range nodes {
					if n.NodeID == masterID && len(n.Slots) > 0 {
						hasSlots = true
						break
					}
				}
				if hasSlots {
					log.Info("Node already has slots, skipping assignment", "shard", k, "pod", masterName)
					continue
				}
			}

			auditLog.Info("Assigning slots to master", "shard", k, "pod", masterName, "slots", fmt.Sprintf("%d-%d", slotRanges[k].Start, slotRanges[k].End))
			slots, _ := redisclient.ExpandSlotRange(redisclient.FormatSlotRange(slotRanges[k].Start, slotRanges[k].End))
			if err := clusterClient.ClusterAddSlots(ctx, masterAddr, slots...); err != nil {
				auditLog.Error(err, "Failed to add slots", "shard", k, "pod", masterName)
			}
		}
	}

	// 3. Assign Replicas: each shard's -1..R replicate that shard's master.
	for _, ref := range refs {
		if ref.IsMaster {
			continue
		}
		masterName := shardMasterPodName(littleRed.Name, ref.ShardIdx)
		masterID := nodeIDs[masterName]

		replicaAddr := fmt.Sprintf("%s:%d", podIPs[ref.Name], littleredv1alpha1.RedisPort)

		nodes, err := clusterClient.GetClusterNodes(ctx, replicaAddr)
		alreadyCorrect := false
		if err == nil {
			for _, n := range nodes {
				if n.NodeID == nodeIDs[ref.Name] && n.MasterID == masterID {
					alreadyCorrect = true
					break
				}
			}
		}

		if !alreadyCorrect {
			auditLog.Info("Assigning replica to master", "replica", ref.Name, "master", masterName)
			if err := clusterClient.ClusterReplicate(ctx, replicaAddr, masterID); err != nil {
				auditLog.Error(err, "Failed to replicate", "replica", ref.Name, "master", masterName)
			}
		}
	}

	return r.updateClusterStatus(ctx, littleRed, nil)
}

// bootstrapMeetRound introduces every bootstrap pod to shard 0's master. It returns
// false when the seed itself could not be attributed to this instance, in which case
// the caller must requeue without touching the cluster.
//
// LR-043. CLUSTER MEET creates a fresh identity binding with no membership validation on
// either side — `clusterStartHandshake` checks only the address syntax, the receiver
// trusts an inbound MEET's gossip section, and the initiator adopts whatever node ID the
// responder reports — so an address belonging to another instance merges two clusters,
// bidirectionally and transitively via gossip. Two guards, in this order:
//
//   - the addresses come from the caller's UNCACHED pod read, so each one is confirmed as
//     that pod's current IP at the API server (the primary guard: Kubernetes holds at most
//     one live pod per IP, so no separate confirm read is needed here);
//   - attribution as an ADVISORY second layer. It reports a node that names peers of which
//     none are ours (an established foreign cluster), but it no longer vetoes: the caller's
//     read is uncached AND refuses a terminating pod, which closes the only window in which
//     a pod object can name an address that is not its own. A veto here can permanently
//     deny a legitimate own node — a mass container-crash that leaves stale nodes.conf peers
//     on some pods while others are recycled with new node IDs produces exactly that, and a
//     refused SEED stalls bootstrap forever. See changelog LR-043, regression section.
func (r *LittleRedReconciler) bootstrapMeetRound(
	ctx context.Context,
	littleRed *littleredv1alpha1.LittleRed,
	clusterClient *redisclient.ClusterClient,
	refs []ClusterPodRef,
	podIPs, nodeIDs map[string]string,
	candidates map[string]redisclient.MeetCandidate,
) bool {
	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)

	seedName := shardMasterPodName(littleRed.Name, 0)
	seedAddr := fmt.Sprintf("%s:%d", podIPs[seedName], littleredv1alpha1.RedisPort)
	ourIDs := make(map[string]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		ourIDs[id] = true
	}

	if v := redisclient.AttributeMeetTarget(candidates[seedName], ourIDs); !v.AdmissibleWhenConfirmed() {
		auditLog.Info("Bootstrap: refusing to seed CLUSTER MEET from an unidentified address",
			"pod", seedName, "seed", seedAddr, "verdict", v)
		return false
	} else if v == redisclient.MeetDenyUnattributed {
		auditLog.Info("Bootstrap: MEET seed's cluster view names no node of ours; its address is API-server-confirmed, seeding anyway",
			"pod", seedName, "seed", seedAddr, "nodeID", candidates[seedName].NodeID)
	}

	for _, ref := range refs {
		if ref.Name == seedName {
			continue
		}
		if v := redisclient.AttributeMeetTarget(candidates[ref.Name], ourIDs); !v.AdmissibleWhenConfirmed() {
			auditLog.Info("Bootstrap: skipping CLUSTER MEET, target not identified this pass",
				"pod", ref.Name, "target", podIPs[ref.Name], "verdict", v)
			continue
		} else if v == redisclient.MeetDenyUnattributed {
			auditLog.Info("Bootstrap: MEET target's cluster view names no node of ours; its address is API-server-confirmed, meeting anyway",
				"pod", ref.Name, "target", podIPs[ref.Name], "nodeID", candidates[ref.Name].NodeID)
		}
		auditLog.Info("Meeting node", "node", ref.Name, "target", seedName)
		if err := clusterClient.ClusterMeet(ctx, seedAddr, podIPs[ref.Name], littleredv1alpha1.RedisPort); err != nil {
			auditLog.Error(err, "Failed to meet node", "node", ref.Name)
		}
	}
	return true
}

// updateClusterStatus updates the LittleRed status for cluster mode.
// gt may be passed in from the caller to avoid a redundant gather; pass nil to gather fresh.
func (r *LittleRedReconciler) updateClusterStatus(ctx context.Context, littleRed *littleredv1alpha1.LittleRed, gt *redisclient.ClusterGroundTruth) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	oldStatus := littleRed.Status.DeepCopy()

	// Aggregate readiness/total across all per-shard StatefulSets.
	_, _, readyReplicas, totalSpecReplicas, err := r.clusterShardStatefulSets(ctx, littleRed)
	if err != nil {
		return ctrl.Result{}, err
	}
	littleRed.Status.Redis.Ready = readyReplicas
	littleRed.Status.Redis.Total = totalSpecReplicas

	clusterShards := int32(littleredv1alpha1.DefaultClusterShards)
	replicasPerShard := 0
	if littleRed.Spec.Cluster != nil {
		clusterShards = int32(littleRed.Spec.Cluster.Shards)
		replicasPerShard = clusterReplicasPerShard(littleRed.Spec.Cluster)
	}

	// Gather ground truth to get node details for status.
	// Reuse the caller's gt if provided (happy path); gather fresh otherwise (post-repair).
	if gt == nil {
		gt = r.gatherGroundTruth(ctx, littleRed)
	}
	clusterOK := false
	if gt != nil {
		if littleRed.Status.Cluster == nil {
			littleRed.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{}
		}
		littleRed.Status.Cluster.State = gt.ClusterState
		clusterOK = gt.IsHealthy(littleRed.Status.Redis.Total, clusterShards)

		// Populate node details
		nodeStates := make([]littleredv1alpha1.ClusterNodeState, 0)
		for _, ref := range ClusterPodRefs(littleRed.Name, int(clusterShards), replicasPerShard) {
			podName := ref.Name
			if node, ok := gt.Nodes[podName]; ok {
				nodeStates = append(nodeStates, littleredv1alpha1.ClusterNodeState{
					PodName:      podName,
					NodeID:       node.NodeID,
					Role:         node.Role,
					MasterNodeID: node.MasterNodeID,
					SlotRanges:   strings.Join(node.Slots, ","),
				})
			}
		}
		littleRed.Status.Cluster.Nodes = nodeStates
	}

	// Determine high level phase
	if littleRed.Status.Redis.Ready == littleRed.Status.Redis.Total && clusterOK {
		littleRed.Status.Phase = littleredv1alpha1.PhaseRunning
		littleRed.Status.Status = littleredv1alpha1.ConditionReady

		meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
			Type:               littleredv1alpha1.ConditionReady,
			Status:             metav1.ConditionTrue,
			Reason:             "ClusterHealthy",
			Message:            "All pods ready and cluster state is ok",
			LastTransitionTime: metav1.Now(),
		})
	} else {
		if littleRed.Status.Phase != littleredv1alpha1.PhasePending {
			littleRed.Status.Phase = littleredv1alpha1.PhaseInitializing
		}
		littleRed.Status.Status = "Initializing"

		log.Info("Not yet Running, requeueing",
			"redis", fmt.Sprintf("%d/%d", littleRed.Status.Redis.Ready, littleRed.Status.Redis.Total),
			"clusterHealthy", clusterOK)

		meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
			Type:               littleredv1alpha1.ConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             "ClusterNotReady",
			Message:            "Waiting for cluster to be healthy",
			LastTransitionTime: metav1.Now(),
		})
	}

	// Update status if changed
	if !reflect.DeepEqual(oldStatus, &littleRed.Status) {
		if err := r.Status().Update(ctx, littleRed); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
	}

	fast, steady := littleRed.GetRequeueIntervals()
	if littleRed.Status.Phase == littleredv1alpha1.PhaseRunning {
		return ctrl.Result{RequeueAfter: steady}, nil
	}
	return ctrl.Result{RequeueAfter: fast}, nil
}

// ensureClusterResources creates/updates all resources for cluster mode
func (r *LittleRedReconciler) ensureClusterResources(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	if err := r.reconcileClusterConfigMap(ctx, littleRed); err != nil {
		return err
	}
	if err := r.reconcileClusterHeadlessService(ctx, littleRed); err != nil {
		return err
	}
	if err := r.reconcileClusterStatefulSet(ctx, littleRed); err != nil {
		return err
	}
	if err := r.reconcileClusterClientService(ctx, littleRed); err != nil {
		return err
	}
	if err := r.reconcileClusterPDB(ctx, littleRed); err != nil {
		return err
	}

	// Reconcile ServiceMonitor if enabled
	if littleRed.Spec.Metrics.IsEnabled() && littleRed.Spec.Metrics.ServiceMonitor.Enabled {
		if err := r.reconcileServiceMonitor(ctx, littleRed); err != nil {
			// Don't fail reconciliation if ServiceMonitor fails (CRD might not be installed)
			log := r.getLogger(ctx, littleRed, LogCategoryRecon)
			log.Error(err, "Failed to reconcile ServiceMonitor")
		}
	}

	return nil
}

// reconcileClusterConfigMap ensures the ConfigMap exists
func (r *LittleRedReconciler) reconcileClusterConfigMap(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildClusterConfigMap(littleRed))
}

// reconcileClusterHeadlessService ensures the headless Service exists
func (r *LittleRedReconciler) reconcileClusterHeadlessService(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildClusterHeadlessService(littleRed))
}

// reconcileClusterStatefulSet ensures one StatefulSet per shard exists ({name}-shard-K),
// and serializes template *updates* across shards so an operator-driven change never
// restarts more than one shard at a time (LR-021).
//
// Missing shard StatefulSets are created immediately and in parallel — a fresh bootstrap has
// no data to protect and no reason to stage pod creation. For an existing shard whose applied
// pod-template hash differs from desired, the operator rolls that one shard and returns,
// deferring every later shard until this one has fully settled (clusterShardRolloutSettled).
// This restores the global one-pod-at-a-time serialization that the single pre-0.3.0
// StatefulSet gave for free; without it, applying a new template to all N shards at once rolls
// them in parallel and takes every shard's master down in one wave (the availability dip
// observed in the first e2e run — see changelog LR-021).
//
// Note: this governs only rollouts the operator triggers (a spec/config change that rewrites
// the pod template). A manual `kubectl rollout restart` of the shard StatefulSets bypasses the
// operator and is not serialized — roll shards one at a time by hand instead.
func (r *LittleRedReconciler) reconcileClusterStatefulSet(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	shards := clusterShardCount(littleRed)
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	for k := range shards {
		desired := buildClusterShardStatefulSet(littleRed, k)
		existing := &appsv1.StatefulSet{}
		err := r.Get(ctx, types.NamespacedName{
			Name:      clusterShardStatefulSetName(littleRed, k),
			Namespace: littleRed.Namespace,
		}, existing)
		if apierrors.IsNotFound(err) {
			// Create-missing: immediate and parallel (bootstrap).
			if err := r.apply(ctx, littleRed, desired); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}

		// Serialize updates. If the applied template differs from desired, roll only this
		// shard and stop until it settles. The hash lives on the pod template (so changing
		// it is itself part of the roll) and is compared cache-safely as a stored value.
		desiredHash := desired.Spec.Template.Annotations[AnnotationPodSpecHash]
		appliedHash := existing.Spec.Template.Annotations[AnnotationPodSpecHash]
		if appliedHash != desiredHash {
			if err := r.apply(ctx, littleRed, desired); err != nil {
				return err
			}
			log.Info("Serialized cluster rollout: rolling shard, deferring later shards until it settles",
				"shard", k, "sts", desired.Name)
			return nil
		}

		// Template already desired. If it is still converging (including the window right
		// after our own apply, where ObservedGeneration lags Generation), wait before the
		// next shard so at most one shard rolls at a time.
		if !clusterShardRolloutSettled(existing) {
			log.Info("Serialized cluster rollout: waiting for shard to settle before rolling the next",
				"shard", k, "sts", existing.Name)
			return nil
		}

		// Settled at desired: keep non-template fields in sync (idempotent) and advance.
		if err := r.apply(ctx, littleRed, desired); err != nil {
			return err
		}
	}
	return nil
}

// reconcileClusterClientService ensures the client Service exists
func (r *LittleRedReconciler) reconcileClusterClientService(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	return r.apply(ctx, littleRed, buildClusterClientService(littleRed))
}

// reconcileClusterPDB creates or deletes one PDB per shard based on spec. A per-shard PDB
// is only created when the cluster has redundancy (replicasPerShard >= 1). With
// replicasPerShard == 0 every pod is the sole owner of its slots, so a PDB cannot protect
// availability without blocking node drains — we never create one in that case.
func (r *LittleRedReconciler) reconcileClusterPDB(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	shards := clusterShardCount(littleRed)
	create := r.pdbEnabled(littleRed) && clusterHasReplicas(littleRed)
	for k := range shards {
		if create {
			if err := r.apply(ctx, littleRed, buildClusterShardPDB(littleRed, k)); err != nil {
				return err
			}
			continue
		}
		if err := r.deleteIfExists(ctx, littleRed, &policyv1.PodDisruptionBudget{}, clusterShardPDBName(littleRed, k)); err != nil {
			return err
		}
	}
	// Also clean up the pre-0.3.0 single cluster PDB if it lingers.
	return r.deleteIfExists(ctx, littleRed, &policyv1.PodDisruptionBudget{}, clusterPodDisruptionBudgetName(littleRed))
}

// clusterShardCount returns the number of shards, defaulting a nil spec.
func clusterShardCount(lr *littleredv1alpha1.LittleRed) int {
	if lr.Spec.Cluster == nil {
		return littleredv1alpha1.DefaultClusterShards
	}
	return lr.Spec.Cluster.Shards
}

// clusterShardStatefulSets fetches every per-shard StatefulSet. It returns the fetched
// StatefulSets keyed by shard index (missing shards omitted), whether all expected shard
// StatefulSets exist, and the summed ready and spec replica counts across all shards.
func (r *LittleRedReconciler) clusterShardStatefulSets(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) (byShard map[int]*appsv1.StatefulSet, allExist bool, ready, total int32, err error) {
	shards := clusterShardCount(littleRed)
	byShard = make(map[int]*appsv1.StatefulSet, shards)
	allExist = true
	for k := range shards {
		sts := &appsv1.StatefulSet{}
		if getErr := r.Get(ctx, types.NamespacedName{
			Name:      clusterShardStatefulSetName(littleRed, k),
			Namespace: littleRed.Namespace,
		}, sts); getErr != nil {
			if apierrors.IsNotFound(getErr) {
				allExist = false
				continue
			}
			return nil, false, 0, 0, getErr
		}
		byShard[k] = sts
		ready += sts.Status.ReadyReplicas
		if sts.Spec.Replicas != nil {
			total += *sts.Spec.Replicas
		}
	}
	return byShard, allExist, ready, total, nil
}

// isLegacyClusterStatefulSet is the pure legacy-shape identification behind
// detectLegacyClusterStatefulSet. A name-only trigger ({name}-cluster exists) risks a
// false-positive auto-migration against any StatefulSet that merely shares the name; this is a
// positive identification of a genuine pre-0.3.0 single-STS cluster (ADR-013 §5). Every
// discriminator must hold:
//   - name == {name}-cluster — guaranteed by the Get that fetched sts, asserted defensively;
//   - carries component=cluster — the shard-agnostic label the pre-0.3 builder stamped
//     (buildClusterStatefulSet @ 85e1a93^);
//   - does NOT carry the LabelShard key — the single strongest discriminator vs a 0.3
//     per-shard STS ({name}-shard-K), which always stamps it;
//   - Replicas == shards*(1+replicasPerShard) — the old whole-cluster sizing (a per-shard STS
//     is sized 1+replicasPerShard);
//   - is controller-owned by this CR — the pre-0.3 builder set a controller OwnerReference via
//     SetControllerReference, so a genuine legacy STS holds one.
//
// Any check failing ⇒ not a legacy cluster we should auto-migrate (returns false).
func isLegacyClusterStatefulSet(sts *appsv1.StatefulSet, lr *littleredv1alpha1.LittleRed) bool {
	if sts == nil {
		return false
	}
	// Defensive: the Get keys on this name, but a name-only trigger is exactly the hazard we
	// are hardening against — assert it explicitly.
	if sts.Name != clusterStatefulSetName(lr) {
		return false
	}
	// component=cluster — the shard-agnostic label the pre-0.3 builder stamped.
	if sts.Labels[labelAppComponent] != ComponentCluster {
		return false
	}
	// A 0.3 per-shard STS carries the shard label; a legacy single STS never did.
	if _, hasShard := sts.Labels[LabelShard]; hasShard {
		return false
	}
	// Whole-cluster sizing: shards*(1+replicasPerShard).
	cluster := lr.Spec.Cluster
	if cluster == nil {
		cluster = &littleredv1alpha1.ClusterSpec{}
		cluster.SetDefaults()
	}
	if sts.Spec.Replicas == nil || int(*sts.Spec.Replicas) != cluster.GetTotalNodes() {
		return false
	}
	// The pre-0.3 builder owned the STS via SetControllerReference (controller OwnerReference).
	if !metav1.IsControlledBy(sts, lr) {
		return false
	}
	return true
}

// detectLegacyClusterStatefulSet reports whether a genuine pre-0.3.0 single cluster StatefulSet
// ({name}-cluster) still exists. Presence of a legacy-shaped STS triggers the in-place
// legacy→per-shard migration (see migrateLegacyCluster, ADR-013) so we never fork the cluster
// or wipe data. It fetches {name}-cluster and defers the legacy-vs-not decision to the pure
// isLegacyClusterStatefulSet — a StatefulSet that merely shares the name (e.g. a 0.3 artifact,
// or a hand-created STS) does NOT trigger migration.
func (r *LittleRedReconciler) detectLegacyClusterStatefulSet(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) (bool, error) {
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      clusterStatefulSetName(littleRed),
		Namespace: littleRed.Namespace,
	}, sts)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return isLegacyClusterStatefulSet(sts, littleRed), nil
}

// reportLegacyMigrationRefused is the terminal refuse for a legacy cluster the operator
// cannot safely migrate (ADR-013 §5 — a non-shape-preserving topology). It sets Phase=Failed
// with the LegacyClusterTopology condition carrying the given reason/message and emits a
// warning event. It never deletes the legacy workload (EmptyDir storage — deleting destroys
// data; LittleRed never deletes data by default). It replaces ADR-007's blanket terminal
// refuse: the healthy legacy cluster now migrates automatically (migrateLegacyCluster); only
// the genuinely unsupported cases land here.
func (r *LittleRedReconciler) reportLegacyMigrationRefused(ctx context.Context, littleRed *littleredv1alpha1.LittleRed, reason, msg string) (ctrl.Result, error) {
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	log.Info("Refusing legacy cluster migration", "reason", reason)
	r.event(littleRed, corev1.EventTypeWarning, "LegacyClusterTopology", msg)

	littleRed.Status.Phase = littleredv1alpha1.PhaseFailed
	littleRed.Status.Status = "LegacyClusterTopology"
	meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
	if err := r.Status().Update(ctx, littleRed); err != nil && !apierrors.IsConflict(err) {
		return ctrl.Result{}, err
	}
	_, steady := littleRed.GetRequeueIntervals()
	return ctrl.Result{RequeueAfter: steady}, nil
}

// detectOrphanedShardStatefulSets returns the names of existing cluster shard
// StatefulSets whose shard index is >= the desired shard count — i.e. shards left behind
// by a scale-down of spec.cluster.shards. It lists by the shard-agnostic cluster labels
// and reads the per-shard identity label to recover each shard index.
func (r *LittleRedReconciler) detectOrphanedShardStatefulSets(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) ([]string, error) {
	shards := clusterShardCount(littleRed)
	list := &appsv1.StatefulSetList{}
	if err := r.List(ctx, list,
		client.InNamespace(littleRed.Namespace),
		client.MatchingLabels(clusterSelectorLabels(littleRed)),
	); err != nil {
		return nil, err
	}
	var orphans []string
	for i := range list.Items {
		sts := &list.Items[i]
		val, ok := sts.Labels[LabelShard]
		if !ok {
			continue
		}
		shardIdx, err := strconv.Atoi(val)
		if err != nil {
			continue
		}
		if shardIdx >= shards {
			orphans = append(orphans, sts.Name)
		}
	}
	return orphans, nil
}

// reportShardScaleDown surfaces a refused shard scale-down and waits. Like the legacy
// guard, it does NOT delete the orphaned StatefulSets (that would destroy their data).
func (r *LittleRedReconciler) reportShardScaleDown(ctx context.Context, littleRed *littleredv1alpha1.LittleRed, orphans []string) (ctrl.Result, error) {
	msg := fmt.Sprintf("Refusing to reduce cluster.shards to %d: shard StatefulSet(s) %s "+
		"would be orphaned and their slots (and data) lost. There is no reshard-away path "+
		"and LittleRed never deletes data by default. Restore cluster.shards or migrate the "+
		"data and remove the shard(s) manually.", clusterShardCount(littleRed), strings.Join(orphans, ", "))
	log := r.getLogger(ctx, littleRed, LogCategoryRecon)
	log.Info("Refusing cluster shard scale-down", "orphans", orphans)
	r.event(littleRed, corev1.EventTypeWarning, "ShardScaleDownRefused", msg)

	littleRed.Status.Phase = littleredv1alpha1.PhaseFailed
	littleRed.Status.Status = "ShardScaleDownRefused"
	meta.SetStatusCondition(&littleRed.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             "ShardScaleDownRefused",
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
	if err := r.Status().Update(ctx, littleRed); err != nil && !apierrors.IsConflict(err) {
		return ctrl.Result{}, err
	}
	_, steady := littleRed.GetRequeueIntervals()
	return ctrl.Result{RequeueAfter: steady}, nil
}

// clusterHasReplicas reports whether cluster mode runs with at least one replica per shard.
// The CRD default for replicasPerShard is 1, so a nil spec is treated as redundant.
func clusterHasReplicas(lr *littleredv1alpha1.LittleRed) bool {
	if lr.Spec.Cluster == nil || lr.Spec.Cluster.ReplicasPerShard == nil {
		return true
	}
	return *lr.Spec.Cluster.ReplicasPerShard > 0
}
