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

package redis

import (
	"fmt"
	"sort"
)

// MigrationPhase is the phase of the in-place legacy→per-shard cluster migration
// (ADR-013). It is re-derived from live ground truth every reconcile pass and never
// read back from status (ADR-006).
type MigrationPhase string

const (
	// MigrationStandup: the new {name}-shard-K pods are not yet cluster members and no
	// address is known to MEET them (they are still coming up).
	MigrationStandup MigrationPhase = "Standup"
	// MigrationMeet: new pods have addresses but some are not yet in the cluster mesh.
	MigrationMeet MigrationPhase = "Meet"
	// MigrationReplicate (LR-025): every new pod is a cluster member, but some new pod is not
	// yet a link-up replica of the node currently owning its shard's range (the legacy master
	// pre-failover, {name}-shard-K-0 post-failover). The new nodes full-sync as slot-less replicas.
	MigrationReplicate MigrationPhase = "Replicate"
	// MigrationFailover (LR-025): every new pod is a synced (link-up) replica, but some
	// {name}-shard-K-0 does not yet own its range. One coordinated CLUSTER FAILOVER per pass
	// promotes the lowest such K, atomically flipping ownership to the already-synced new master.
	MigrationFailover MigrationPhase = "Failover"
	// MigrationDecommission: everything is on the new layout; FORGET the drained legacy
	// nodes and delete the legacy StatefulSet.
	MigrationDecommission MigrationPhase = "Decommission"
	// MigrationComplete: no legacy nodes remain in the cluster.
	MigrationComplete MigrationPhase = "Complete"
)

// totalClusterSlots is the fixed Redis Cluster slot count (0..16383). Matches the
// literal used by IsHealthy and GenerateSlotRanges.
const totalClusterSlots = 16384

// ReplicaAttach describes one CLUSTER REPLICATE the driver should issue: make the
// node at ReplicaAddr replicate the new-shard master identified by MasterID.
type ReplicaAttach struct {
	ReplicaAddr string
	MasterID    string
}

// LegacyFacts carries the migration inputs the plan needs but cannot derive from
// live ground truth alone. It is assembled by the driver from the pod list + gt and
// is strictly pure data (strings/ints/maps, no k8s types) so the plan stays testable.
//
//   - SeedAddrs: reachable legacy master addresses usable as a MEET seed. A MEET can
//     only be issued when a seed exists; the plan holds in Standup otherwise.
//   - LegacyNodeIDs: NodeIDs of the pre-0.3 {name}-cluster-N nodes. A gt node whose
//     NodeID is here is legacy; used to FORGET at Decommission and to detect Complete.
//   - NewPodAddrs: each new {name}-shard-K-M pod name → its dial address (host:port).
//     The driver fills it once the new pods have IPs; a missing/empty entry means the
//     pod is not up yet (⇒ Standup, not Meet). Keyed by pod name so it uniformly serves
//     both MEET (masters + replicas) and replica-attach without fragile ordering.
type LegacyFacts struct {
	SeedAddrs     []string
	LegacyNodeIDs []string
	NewPodAddrs   map[string]string
}

// MigrationPlan is the pure output of planClusterMigration for one reconcile pass.
// It carries the derived Phase plus at most one action set for that phase (idempotent,
// resumable from live state); Reason is a short human log line.
type MigrationPlan struct {
	Phase        MigrationPhase
	Meets        []string         // new-pod addrs to MEET (via a legacy seed)
	Replicates   []ReplicaAttach  // {ReplicaAddr, MasterID}: attach a new pod onto its shard's current range owner
	Failovers    []FailoverAction // {name}-shard-K-0 to promote this pass (at most one, for determinism)
	Forgets      []string         // legacy node IDs to FORGET (sorted)
	DeleteLegacy bool             // decommission: delete {name}-cluster STS + PDB
	ShardsMoved  int
	TotalShards  int
	Reason       string
}

// FailoverAction promotes a synced new master {name}-shard-K-0 (LR-025). The failover is
// a coordinated CLUSTER FAILOVER by default (an atomic ownership flip to an already-synced
// replica; the current owner demotes to a live replica of it). Force (⇒ CLUSTER FAILOVER
// TAKEOVER) is set ONLY on the legacy-master-died-mid-migration edge (§7): the range owner
// is unreachable AND this replica is confirmed link-up/synced (it holds the data). A replica
// that is not confirmed synced is never Force-promoted.
type FailoverAction struct {
	Addr  string // {name}-shard-K-0 dial addr (the replica to promote)
	Force bool
}

// PlanClusterMigration derives the migration phase and the single action set for this pass
// PURELY from live ground truth + LegacyFacts (ADR-013 §7, LR-025 replicate-then-failover).
// Phase is never read back from status; the driver re-plans every reconcile. Derivation is
// strict-precedence: it reports the LEAST-advanced phase that still has work and emits only
// that phase's actions, so ALL replication finishes before ANY failover (every slot has ≥2
// live copies before a single handoff).
//
//	Complete     — no legacy nodes remain in gt.
//	Standup/Meet  — some new pod is not yet a cluster member (MEET it via a legacy seed).
//	Replicate     — every new pod is MET, but some is not yet a link-up replica of the node
//	                currently owning its shard's range (legacy master pre-failover, K-0 post).
//	                Defers (counts, does not emit) an attach the replica does not yet NodeKnows.
//	Failover      — every new pod is a synced (link-up) replica, but some {name}-shard-K-0 does
//	                not own its range; emit ONE coordinated CLUSTER FAILOVER for the lowest such K.
//	Decommission  — every K-0 owns its range and every new replica is a link-up replica of its
//	                own new master; FORGET all legacy nodes and delete the legacy STS.
//
// Exported so the controller migration driver (cluster_migration.go) can drive it; the logic
// stays a pure seam (no I/O). No slot move is ever emitted (LR-025 removed the reshard path).
func PlanClusterMigration(gt *ClusterGroundTruth, shards, replicasPerShard int, name string,
	legacy LegacyFacts) MigrationPlan {
	plan := MigrationPlan{TotalShards: shards}
	if shards <= 0 {
		plan.Reason = "invalid shard count"
		return plan
	}
	ranges := GenerateSlotRanges(shards)
	plan.ShardsMoved = countShardsOnNewMasters(gt, name, ranges)

	// Complete: no legacy nodes remain in the cluster.
	if len(presentLegacyNodes(gt, legacy.LegacyNodeIDs)) == 0 {
		plan.Phase = MigrationComplete
		plan.Reason = "no legacy nodes remain; migration complete"
		return plan
	}

	// Standup / Meet: not all new pods are cluster members yet.
	if !allNewPodsMet(gt, name, shards, replicasPerShard) {
		return planStandupOrMeet(plan, gt, name, shards, replicasPerShard, legacy)
	}

	// Replicate: some new pod is not yet a link-up replica of its shard's current range owner.
	replicates, deferred, unsynced := planReplicates(gt, name, shards, replicasPerShard, ranges, legacy.NewPodAddrs)
	if unsynced {
		plan.Phase = MigrationReplicate
		plan.Replicates = replicates
		if deferred > 0 {
			plan.Reason = fmt.Sprintf("replicating %d new pod(s) onto their range owner; %d deferred (owner not yet known via gossip)",
				len(replicates), deferred)
		} else {
			plan.Reason = fmt.Sprintf("replicating %d new pod(s) onto their range owner", len(replicates))
		}
		return plan
	}

	// Failover: all new pods synced, but some {name}-shard-K-0 does not yet own its range.
	if fo, k, ok := nextFailover(gt, name, shards, ranges, legacy.NewPodAddrs); ok {
		plan.Phase = MigrationFailover
		plan.Failovers = []FailoverAction{fo}
		mode := "coordinated"
		if fo.Force {
			mode = "forced TAKEOVER (range owner unreachable, replica synced)"
		}
		plan.Reason = fmt.Sprintf("promoting %s-shard-%d-0 to own range %s (%s)",
			name, k, FormatSlotRange(ranges[k].Start, ranges[k].End), mode)
		return plan
	}

	// Decommission: every new master owns its range and every new replica is a link-up replica
	// of its new master (the redundancy gate) — FORGET the demoted legacy nodes and delete the STS.
	plan.Phase = MigrationDecommission
	plan.Forgets = presentLegacyIDsSorted(gt, legacy.LegacyNodeIDs)
	plan.DeleteLegacy = !anyLegacyOwnsSlots(gt, legacy.LegacyNodeIDs)
	plan.Reason = fmt.Sprintf("decommissioning %d legacy node(s)", len(plan.Forgets))
	return plan
}

// planStandupOrMeet decides between Standup (nothing MET-able yet) and Meet (addresses
// known + a seed exists, so emit MEETs for the not-yet-joined new pods).
func planStandupOrMeet(plan MigrationPlan, gt *ClusterGroundTruth, name string, shards, rps int,
	legacy LegacyFacts) MigrationPlan {
	meets := missingNewPodMeets(gt, name, shards, rps, legacy.NewPodAddrs)
	if len(meets) > 0 && len(legacy.SeedAddrs) > 0 {
		plan.Phase = MigrationMeet
		plan.Meets = meets
		plan.Reason = fmt.Sprintf("MEET %d new pod(s) into the cluster", len(meets))
		return plan
	}
	plan.Phase = MigrationStandup
	plan.Reason = "waiting for new shard pods to come up"
	return plan
}

// LegacyMigrationReady is the health gate (ADR-013 §2). Migration begins only when the
// legacy cluster is safe to rewrite: cluster_state ok, all 16384 slots assigned, all
// legacy pods Ready (kubelet readiness, injected as a bool — blackhole-proof per
// LR-017/023), and a reachable master quorum. Exported for the migration driver.
func LegacyMigrationReady(gt *ClusterGroundTruth, allLegacyPodsReady bool) bool {
	if gt.ClusterState != "ok" {
		return false
	}
	if gt.TotalSlots != totalClusterSlots {
		return false
	}
	if !allLegacyPodsReady {
		return false
	}
	reachable, total := countReachableMasters(gt)
	return total > 0 && reachable*2 > total
}

// LegacyShapePreserved reports whether the legacy cluster is shape-preserving (ADR-013
// §5): exactly `shards` slot-owning masters, each owning exactly one aligned
// GenerateSlotRanges(shards)[K] range, and a member count of shards×(1+replicasPerShard).
// Anything else is refused (the 1:1 range mapping only holds for the identical shape).
// Exported for the migration driver.
func LegacyShapePreserved(gt *ClusterGroundTruth, shards, replicasPerShard int) bool {
	if shards <= 0 {
		return false
	}
	if len(gt.Nodes) != shards*(1+replicasPerShard) {
		return false
	}
	ranges := GenerateSlotRanges(shards)
	ownedIdx := make(map[int]bool, shards)
	masters := 0
	for _, n := range gt.Nodes {
		if n.Role != roleMaster || len(n.Slots) == 0 {
			continue
		}
		masters++
		if len(n.Slots) != 1 {
			return false // an aligned shard owns exactly one contiguous range
		}
		idx := alignedRangeIndex(ranges, n.Slots[0])
		if idx == -1 || ownedIdx[idx] {
			return false // non-aligned/fragmented range, or a duplicate owner
		}
		ownedIdx[idx] = true
	}
	return masters == shards && len(ownedIdx) == shards
}

// --- pure helpers ---

// ownerOfRange returns the node that owns exactly the aligned range [start,end] (regardless
// of reachability — the Force-failover edge needs an unreachable owner), or nil if none does.
func ownerOfRange(gt *ClusterGroundTruth, start, end int) *ClusterNodeState {
	for _, n := range gt.Nodes {
		if nodeOwnsRange(n, start, end) {
			return n
		}
	}
	return nil
}

// IsLinkUpReplicaOf is the LR-025 "synced" gate: rep is a replica of masterNodeID with its
// replication link reported up. A replica whose link is still down is not yet synced, so it
// is neither Failover-promotable nor a satisfied redundancy copy.
//
// Exported because the state-gated rolling update (ADR-017, internal/controller) asks the same
// question of a replaced shard pod before the StatefulSet is allowed to take the next one down.
// One definition, deliberately: the rollout gate and the migration planner must not be able to
// disagree about what "synced" means. LinkStatus here is INFO's master_link_status (see
// gatherNodeIdentities), i.e. the replication link, not the cluster-bus link — so a replica
// mid-full-sync reads down, which is exactly the state both callers must wait out.
func IsLinkUpReplicaOf(rep *ClusterNodeState, masterNodeID string) bool {
	return rep != nil && rep.Role == roleReplica && rep.MasterNodeID == masterNodeID && rep.LinkStatus == "up"
}

func newMasterPodName(name string, k int) string { return fmt.Sprintf("%s-shard-%d-0", name, k) }
func newReplicaPodName(name string, k, m int) string {
	return fmt.Sprintf("%s-shard-%d-%d", name, k, m)
}

// expectedNewPods enumerates the new per-shard pod names in deterministic order
// (shard K ascending, then master, then replicas 1..rps).
func expectedNewPods(name string, shards, rps int) []string {
	pods := make([]string, 0, shards*(1+rps))
	for k := range shards {
		pods = append(pods, newMasterPodName(name, k))
		for m := 1; m <= rps; m++ {
			pods = append(pods, newReplicaPodName(name, k, m))
		}
	}
	return pods
}

func allNewPodsMet(gt *ClusterGroundTruth, name string, shards, rps int) bool {
	for _, p := range expectedNewPods(name, shards, rps) {
		if gt.Nodes[p] == nil {
			return false
		}
	}
	return true
}

// missingNewPodMeets returns, in enumeration order, the dial addresses of new pods that
// are not yet cluster members and whose address is known.
func missingNewPodMeets(gt *ClusterGroundTruth, name string, shards, rps int,
	addrs map[string]string) []string {
	var meets []string
	for _, p := range expectedNewPods(name, shards, rps) {
		if gt.Nodes[p] != nil {
			continue
		}
		if addr := addrs[p]; addr != "" {
			meets = append(meets, addr)
		}
	}
	return meets
}

func nodeOwnsRange(n *ClusterNodeState, start, end int) bool {
	for _, s := range n.Slots {
		st, en, err := ParseSlotRange(s)
		if err == nil && st == start && en == end {
			return true
		}
	}
	return false
}

// alignedRangeIndex returns the shard index whose aligned range equals slotStr, or -1
// if slotStr is fragmented / not one of the expected ranges.
func alignedRangeIndex(ranges []struct{ Start, End int }, slotStr string) int {
	st, en, err := ParseSlotRange(slotStr)
	if err != nil {
		return -1
	}
	for i, r := range ranges {
		if r.Start == st && r.End == en {
			return i
		}
	}
	return -1
}

func countShardsOnNewMasters(gt *ClusterGroundTruth, name string, ranges []struct{ Start, End int }) int {
	count := 0
	for k, r := range ranges {
		if m := gt.Nodes[newMasterPodName(name, k)]; m != nil && nodeOwnsRange(m, r.Start, r.End) {
			count++
		}
	}
	return count
}

// planReplicates scans every new pod (master-to-be and its replicas) and, for those not yet a
// link-up replica of the node currently owning their shard's range, emits a CLUSTER REPLICATE
// (or defers it if the executing node does not yet NodeKnows the owner, avoiding ERR Unknown
// node). unsynced is true if ANY new pod is not yet settled (⇒ stay in the Replicate phase).
//
// Per-pod "settled" means: a {name}-shard-K-0 that already owns range K (post-failover, done),
// OR a link-up replica of ownerOfRange(range K). A pod already replicating the right owner but
// with its link still down is NOT settled (keeps us in Replicate) yet is NOT re-emitted (that
// would restart the in-flight full-sync).
func planReplicates(gt *ClusterGroundTruth, name string, shards, rps int,
	ranges []struct{ Start, End int }, addrs map[string]string) (replicates []ReplicaAttach, deferred int, unsynced bool) {
	for k := range shards {
		r := ranges[k]
		owner := ownerOfRange(gt, r.Start, r.End)
		for _, podName := range shardNewPods(name, k, rps) {
			node := gt.Nodes[podName]
			if node == nil {
				continue // not MET (allNewPodsMet guards this; defensive)
			}
			// A new pod that already owns this shard's range is on the new side and settled for
			// Replicate: it can't (and needn't) replicate itself — a node is never its own owner.
			// Normally that's {name}-shard-K-0 after its coordinated failover; a restart-during-
			// migration native failover can instead promote a new *replica* pod (e.g. K-1) to own
			// the range (MIGRATION_CHAOS_SELF_REPLICATE_DEADLOCK). Either way, never emit REPLICATE
			// <self> (ERR Can't replicate myself). The Failover phase then reconciles which K-0 is
			// master; roles are fluid in cluster mode.
			if nodeOwnsRange(node, r.Start, r.End) {
				continue
			}
			if IsLinkUpReplicaOf(node, ownerNodeID(owner)) {
				continue // already a synced replica of the current owner
			}
			unsynced = true
			if owner == nil {
				continue // no owner to attach to this pass; wait (cannot emit)
			}
			if node.Role == roleReplica && node.MasterNodeID == owner.NodeID {
				continue // already replicating the owner, link just not up yet — do not re-issue
			}
			if !gt.NodeKnows(node.NodeID, owner.NodeID) {
				deferred++
				continue
			}
			replicates = append(replicates, ReplicaAttach{ReplicaAddr: addrs[podName], MasterID: owner.NodeID})
		}
	}
	return replicates, deferred, unsynced
}

// nextFailover returns the FailoverAction for the lowest shard K whose new master
// {name}-shard-K-0 does not yet own range K. It is only called once planReplicates reports
// fully synced, so that {name}-shard-K-0 is by construction a link-up replica of the range
// owner — a coordinated CLUSTER FAILOVER is a lossless atomic ownership flip. Force (⇒
// TAKEOVER) is set only on the §7 edge: the range owner is unreachable AND {name}-shard-K-0
// is confirmed link-up/synced (it holds the data). ok=false means every K-0 owns its range.
func nextFailover(gt *ClusterGroundTruth, name string, shards int,
	ranges []struct{ Start, End int }, addrs map[string]string) (FailoverAction, int, bool) {
	for k := range shards {
		r := ranges[k]
		masterName := newMasterPodName(name, k)
		m0 := gt.Nodes[masterName]
		if m0 != nil && nodeOwnsRange(m0, r.Start, r.End) {
			continue // already owns its range
		}
		owner := ownerOfRange(gt, r.Start, r.End)
		force := owner != nil && !owner.Reachable && IsLinkUpReplicaOf(m0, owner.NodeID)
		return FailoverAction{Addr: addrs[masterName], Force: force}, k, true
	}
	return FailoverAction{}, 0, false
}

// shardNewPods enumerates the new pod names of shard k: the master {name}-shard-k-0 first,
// then its replicas 1..rps.
func shardNewPods(name string, k, rps int) []string {
	pods := []string{newMasterPodName(name, k)}
	for m := 1; m <= rps; m++ {
		pods = append(pods, newReplicaPodName(name, k, m))
	}
	return pods
}

// ownerNodeID returns owner.NodeID, or "" when owner is nil (so IsLinkUpReplicaOf, which
// requires a non-empty MasterNodeID match, cleanly reports "not settled").
func ownerNodeID(owner *ClusterNodeState) string {
	if owner == nil {
		return ""
	}
	return owner.NodeID
}

func presentLegacyNodes(gt *ClusterGroundTruth, legacyIDs []string) []*ClusterNodeState {
	idset := make(map[string]bool, len(legacyIDs))
	for _, id := range legacyIDs {
		idset[id] = true
	}
	var out []*ClusterNodeState
	for _, n := range gt.Nodes {
		if idset[n.NodeID] {
			out = append(out, n)
		}
	}
	return out
}

func presentLegacyIDsSorted(gt *ClusterGroundTruth, legacyIDs []string) []string {
	present := presentLegacyNodes(gt, legacyIDs)
	ids := make([]string, 0, len(present))
	for _, n := range present {
		ids = append(ids, n.NodeID)
	}
	sort.Strings(ids)
	return ids
}

func anyLegacyOwnsSlots(gt *ClusterGroundTruth, legacyIDs []string) bool {
	for _, n := range presentLegacyNodes(gt, legacyIDs) {
		if len(n.Slots) > 0 {
			return true
		}
	}
	return false
}

func countReachableMasters(gt *ClusterGroundTruth) (reachable, total int) {
	for _, n := range gt.Nodes {
		if n.Role == roleMaster && len(n.Slots) > 0 {
			total++
			if n.Reachable {
				reachable++
			}
		}
	}
	return reachable, total
}
