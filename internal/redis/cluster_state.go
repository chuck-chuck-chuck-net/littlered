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

import "slices"

const (
	roleMaster  = "master"
	roleReplica = "replica"

	// flagFail is the CLUSTER NODES gossip flag marking a node objectively down (FAIL).
	flagFail = "fail"
	// flagPFail is the CLUSTER NODES gossip flag marking a node subjectively down
	// (PFAIL) — a single node's suspicion, not yet the cluster-wide FAIL verdict.
	flagPFail = "fail?"

	// ClusterStateFail is the cluster_state a node reports when it has fail-stopped in the
	// unconditional minority check (fewer than (size/2)+1 reachable slot-serving masters).
	// It is the first-hand, survivor-vantage signal a multi-site site loss has exhausted
	// native failover (ADR-012 condition 2).
	ClusterStateFail = "fail"
)

// ClusterNodeState represents the state of a single node in the Redis Cluster
type ClusterNodeState struct {
	PodName      string
	PodIP        string
	NodeID       string
	Slots        []string
	Role         string // "master" or "replica"
	MasterNodeID string // "-" if master
	LinkStatus   string // "up" or "down"
	Reachable    bool

	// --- Survivor-vantage fields (multi-site site-loss takeover, ADR-012) ---
	// These carry data already on the wire in the gather (this node's own CLUSTER INFO
	// and CLUSTER NODES, its replication INFO) that was previously discarded. They give
	// the operator the SURVIVORS' OWN view of the cluster — the vantage ADR-012 requires,
	// because the hub's dial reachability is transport, not evidence (LR-017). They are a
	// §7-parity gather addition: single-site gather populates them too, but no single-site
	// decision reads them.

	// ClusterState is this node's own cluster_state (from its CLUSTER INFO): "ok" when it
	// can reach a slot-serving-master majority, "fail" when it has fail-stopped in the
	// unconditional minority check (the direct, first-hand signal of a quorum loss —
	// ADR-012 condition 2). Empty when its CLUSTER INFO was not gathered.
	ClusterState string
	// FailFlagged is the set of NodeIDs this node's CLUSTER NODES view marks objectively
	// down (FAIL). PFailFlagged is the set it marks subjectively down (PFAIL). Together
	// they are the survivors' corroboration of which peers they have given up on.
	FailFlagged  []string
	PFailFlagged []string
	// MasterReplid / MasterReplid2 / ReplOffset are this node's replication lineage
	// identity and progress (INFO replication). Used to pick the most-complete surviving
	// replica of an orphaned shard WITHIN one lineage (LR-024: union-find over
	// {replid, replid2}, never keyed on replid alone), mirroring BestDataHolder.
	MasterReplid  string
	MasterReplid2 string
	ReplOffset    int64
}

// ClusterGroundTruth represents the combined view of the Redis Cluster topology
type ClusterGroundTruth struct {
	Nodes        map[string]*ClusterNodeState // Map PodName -> ClusterNodeState
	Partitions   [][]string                   // Sets of NodeIDs that see each other
	GhostNodes   []string                     // NodeIDs present in cluster but not in K8s
	ClusterState string                       // "ok" if ANY node says ok, else "fail" or "unknown"
	TotalSlots   int                          // Max slots assigned reported by any node
	AllNodeIDs   map[string]bool              // Set of all NodeIDs seen in the mesh
	// KnownNodes maps a reachable node's NodeID to the set of NodeIDs it directly
	// knows in its own CLUSTER NODES view (excluding fail/noaddr/handshake). It is
	// the same adjacency used to compute Partitions, retained so the operator can
	// avoid issuing CLUSTER REPLICATE before gossip has propagated the target
	// master's NodeID to the executing node (which returns ERR Unknown node).
	KnownNodes map[string][]string
	// AtomicSlotMigration is true iff EVERY reachable node reported support for
	// Redis 8.4+ native atomic slot migration in its CLUSTER INFO. It is a
	// transient, gather-time capability verdict (never persisted): the reshard
	// executor uses it to pick native ASM over the pre-8.4 migrate dance, and a
	// mixed-version cluster mid rolling-upgrade falls back to the dance. Unknown
	// or any node lacking support ⇒ false ⇒ baseline dance. See LR-018 §7.3.
	AtomicSlotMigration bool
}

// NewClusterGroundTruth initializes a new cluster ground truth
func NewClusterGroundTruth() *ClusterGroundTruth {
	return &ClusterGroundTruth{
		Nodes:      make(map[string]*ClusterNodeState),
		AllNodeIDs: make(map[string]bool),
	}
}

func (gt *ClusterGroundTruth) IsHealthy(expectedNodes, expectedShards int32) bool {
	if len(gt.Nodes) < int(expectedNodes) {
		return false
	}
	if len(gt.AllNodeIDs) != int(expectedNodes) {
		return false
	}
	if gt.HasPartitions() {
		return false
	}
	if gt.CountMasters() != int(expectedShards) {
		return false
	}
	// An empty master (a node that is a master with no slots) means a shard is
	// under-replicated and a node is dead weight — not a healthy steady state.
	// Reporting it as healthy lets updateClusterStatus declare Phase=Running and
	// drop to the slow steady requeue cadence, which can stall the empty-master
	// reattach (Step 4) past the e2e topology-sync window. Step 4 always has a
	// reattach target while an empty master exists, so this clause is operator-
	// actionable and cannot deadlock. See RECONCILIATION_ALGORITHM_CHANGELOG.md (LR-014).
	if gt.HasEmptyMasters() {
		return false
	}
	return gt.ClusterState == "ok" && gt.TotalSlots == 16384
}

func (gt *ClusterGroundTruth) HasPartitions() bool {
	return len(gt.Partitions) > 1
}

func (gt *ClusterGroundTruth) HasGhostNodes() bool {
	return len(gt.GhostNodes) > 0
}

func (gt *ClusterGroundTruth) HasOrphanedSlots() bool {
	return gt.TotalSlots < 16384
}

func (gt *ClusterGroundTruth) CountMasters() int {
	count := 0
	for _, n := range gt.Nodes {
		if n.Role == roleMaster && len(n.Slots) > 0 {
			count++
		}
	}
	return count
}

func (gt *ClusterGroundTruth) GetLargestPartitionSeed() *ClusterNodeState {
	if len(gt.Partitions) == 0 {
		for _, n := range gt.Nodes {
			return n
		}
		return nil
	}

	maxIdx := 0
	maxLen := 0
	for i, p := range gt.Partitions {
		if len(p) > maxLen {
			maxLen = len(p)
			maxIdx = i
		}
	}

	targetID := gt.Partitions[maxIdx][0]
	for _, n := range gt.Nodes {
		if n.NodeID == targetID {
			return n
		}
	}
	return nil
}

func (gt *ClusterGroundTruth) GetEmptyMasters() []*ClusterNodeState {
	var empty []*ClusterNodeState
	for _, n := range gt.Nodes {
		if n.Role == roleMaster && len(n.Slots) == 0 {
			empty = append(empty, n)
		}
	}
	return empty
}

// HasEmptyMasters returns true if any node is a master with no slots assigned.
func (gt *ClusterGroundTruth) HasEmptyMasters() bool {
	for _, n := range gt.Nodes {
		if n.Role == roleMaster && len(n.Slots) == 0 {
			return true
		}
	}
	return false
}

// NodeKnows reports whether the node identified by observerID directly knows
// targetID in its own gossip view. Returns false when the observer's view was
// not gathered (e.g. it was unreachable, or KnownNodes was never populated) —
// the safe default for gating CLUSTER REPLICATE, which fails with ERR Unknown
// node if the executing node does not yet know the target.
func (gt *ClusterGroundTruth) NodeKnows(observerID, targetID string) bool {
	return slices.Contains(gt.KnownNodes[observerID], targetID)
}

func (gt *ClusterGroundTruth) GetMastersWithReplicas() map[string][]string {
	m := make(map[string][]string)
	for _, n := range gt.Nodes {
		if n.Role == roleReplica && n.MasterNodeID != "-" {
			m[n.MasterNodeID] = append(m[n.MasterNodeID], n.NodeID)
		}
	}
	return m
}
