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
	"reflect"
	"testing"
)

// Red-first table tests for the ADR-013 pure migration seam, reworked for the LR-025
// replicate-then-failover mechanism. They assert the exact MigrationPlan for every phase
// transition of PlanClusterMigration, and the truth table of the gate predicates. Authored
// against the still-reshard-based body (Draining/ReplicasAttached, nil Failovers) and
// observed to fail for exactly that reason before the new body landed (see the milestone report).

// mgt builds a ClusterGroundTruth keyed by PodName from the given nodes. Health
// fields (ClusterState/TotalSlots) default to a healthy legacy cluster; tests that
// exercise the health gate override them explicitly.
func mgt(nodes ...*ClusterNodeState) *ClusterGroundTruth {
	gt := NewClusterGroundTruth()
	gt.ClusterState = "ok"
	gt.TotalSlots = 16384
	for _, n := range nodes {
		gt.Nodes[n.PodName] = n
		gt.AllNodeIDs[n.NodeID] = true
	}
	return gt
}

// rReplicaUp is a reachable, link-UP replica of masterID (the LR-025 "synced" gate:
// IsLinkUpReplicaOf requires LinkStatus == "up"). The reshard-era rReplica leaves
// LinkStatus == "" (not synced), which the new plan treats as still-syncing.
func rReplicaUp(pod, id, masterID string) *ClusterNodeState {
	n := rReplica(pod, id, masterID)
	n.LinkStatus = "up"
	return n
}

const (
	seedAddr    = "10.0.0.1:6379"
	addrShard00 = "10.1.0.1:6379"
	addrShard01 = "10.1.0.2:6379"
	addrShard10 = "10.1.0.3:6379"
	addrShard11 = "10.1.0.4:6379"
	nodeN00     = "N00"
	nodeN01     = "N01"
	nodeN10     = "N10"
	nodeN11     = "N11"
)

// legacyFacts for the shards=2, replicasPerShard=1 fixtures below.
func mFacts() LegacyFacts {
	return LegacyFacts{
		SeedAddrs:     []string{seedAddr},
		LegacyNodeIDs: []string{"L0", "L1", "L2", "L3"},
		NewPodAddrs: map[string]string{
			"mr-shard-0-0": addrShard00,
			"mr-shard-0-1": addrShard01,
			"mr-shard-1-0": addrShard10,
			"mr-shard-1-1": addrShard11,
		},
	}
}

// knows wires up the KnownNodes gossip adjacency (observer -> targets it knows) so
// Replicate can emit (rather than defer) a CLUSTER REPLICATE.
func knows(gt *ClusterGroundTruth, m map[string][]string) *ClusterGroundTruth {
	gt.KnownNodes = m
	return gt
}

func TestPlanClusterMigration(t *testing.T) {
	const name, shards, rps = "mr", 2, 1
	seedOnly := LegacyFacts{SeedAddrs: []string{seedAddr}, LegacyNodeIDs: []string{"L0", "L1", "L2", "L3"}}

	// Shared node fixtures.
	legNodesFull := func(m0slots, m1slots []string) []*ClusterNodeState {
		return []*ClusterNodeState{
			rMaster("mr-cluster-0", "L0", m0slots...),
			rMaster("mr-cluster-1", "L1", m1slots...),
			rReplica("mr-cluster-2", "L2", "L0"),
			rReplica("mr-cluster-3", "L3", "L1"),
		}
	}
	// The intact legacy cluster (L0 owns range 0, L1 owns range 1).
	legFull := func() []*ClusterNodeState {
		return legNodesFull([]string{"0-8191"}, []string{"8192-16383"})
	}
	// knowsOwners: every new pod knows its legacy range owner (so Replicate emits, not defers).
	knowsOwners := map[string][]string{
		nodeN00: {"L0"}, nodeN01: {"L0"}, nodeN10: {"L1"}, nodeN11: {"L1"},
	}

	tests := []struct {
		name  string
		gt    *ClusterGroundTruth
		facts LegacyFacts
		want  MigrationPlan
	}{
		{
			name:  "standup: new pods not up yet (no addrs)",
			gt:    mgt(legFull()...),
			facts: seedOnly,
			want:  MigrationPlan{Phase: MigrationStandup, TotalShards: 2},
		},
		{
			name:  "meet: all new pods up, none MET",
			gt:    mgt(legFull()...),
			facts: mFacts(),
			want: MigrationPlan{
				Phase:       MigrationMeet,
				Meets:       []string{addrShard00, addrShard01, addrShard10, addrShard11},
				TotalShards: 2,
			},
		},
		{
			name: "meet: partially MET (shard 0 joined, shard 1 not)",
			gt: mgt(append(legFull(),
				rEmpty("mr-shard-0-0", nodeN00), rEmpty("mr-shard-0-1", nodeN01))...),
			facts: mFacts(),
			want: MigrationPlan{
				Phase:       MigrationMeet,
				Meets:       []string{addrShard10, addrShard11},
				TotalShards: 2,
			},
		},
		{
			name: "replicate: all MET as empty masters, none replicating yet",
			gt: knows(mgt(append(legFull(),
				rEmpty("mr-shard-0-0", nodeN00), rEmpty("mr-shard-0-1", nodeN01),
				rEmpty("mr-shard-1-0", nodeN10), rEmpty("mr-shard-1-1", nodeN11))...), knowsOwners),
			facts: mFacts(),
			want: MigrationPlan{
				Phase: MigrationReplicate,
				Replicates: []ReplicaAttach{
					{ReplicaAddr: addrShard00, MasterID: "L0"},
					{ReplicaAddr: addrShard01, MasterID: "L0"},
					{ReplicaAddr: addrShard10, MasterID: "L1"},
					{ReplicaAddr: addrShard11, MasterID: "L1"},
				},
				TotalShards: 2,
			},
		},
		{
			name: "replicate: one replica deferred (does not NodeKnows its owner)",
			gt: knows(mgt(append(legFull(),
				rEmpty("mr-shard-0-0", nodeN00), rEmpty("mr-shard-0-1", nodeN01),
				rEmpty("mr-shard-1-0", nodeN10), rEmpty("mr-shard-1-1", nodeN11))...),
				map[string][]string{nodeN00: {"L0"}, nodeN10: {"L1"}, nodeN11: {"L1"}}), // N01 does not know L0
			facts: mFacts(),
			want: MigrationPlan{
				Phase: MigrationReplicate,
				Replicates: []ReplicaAttach{
					{ReplicaAddr: addrShard00, MasterID: "L0"},
					{ReplicaAddr: addrShard10, MasterID: "L1"},
					{ReplicaAddr: addrShard11, MasterID: "L1"},
				},
				TotalShards: 2,
			},
		},
		{
			name: "replicate: master synced, one replica still an empty master",
			gt: knows(mgt(append(legFull(),
				rReplicaUp("mr-shard-0-0", nodeN00, "L0"), // synced (link-up) replica of L0
				rEmpty("mr-shard-0-1", nodeN01),           // not replicating yet
				rReplicaUp("mr-shard-1-0", nodeN10, "L1"),
				rReplicaUp("mr-shard-1-1", nodeN11, "L1"))...), knowsOwners),
			facts: mFacts(),
			want: MigrationPlan{
				Phase:       MigrationReplicate,
				Replicates:  []ReplicaAttach{{ReplicaAddr: addrShard01, MasterID: "L0"}},
				TotalShards: 2,
			},
		},
		{
			// Chaos regression (MIGRATION_CHAOS_SELF_REPLICATE_DEADLOCK): a restart-during-migration
			// crash of the intended new master mr-shard-0-0 let Redis natively fail shard 0's range
			// over to the *new replica* pod mr-shard-0-1 (N01), which now OWNS 0-8191. The Replicate
			// planner must recognise N01 as already on the new side (a node can't replicate itself)
			// and must NOT emit REPLICATE <self>; it attaches the crashed-and-restarted mr-shard-0-0
			// (N00) onto the new owner instead and stays in Replicate. Pre-fix it also emitted
			// {10.1.0.2 -> N01} (ERR Can't replicate myself) and deadlocked forever.
			name: "replicate CHAOS: native failover promoted a new replica to own the range (no REPLICATE self)",
			gt: knows(mgt(
				rMaster("mr-shard-0-1", nodeN01, "0-8191"), // promoted by native failover; owns range 0
				rEmpty("mr-shard-0-0", nodeN00),            // intended master, crashed+restarted, re-MET
				rMaster("mr-cluster-1", "L1", "8192-16383"),
				rReplicaUp("mr-shard-1-0", nodeN10, "L1"),
				rReplicaUp("mr-shard-1-1", nodeN11, "L1")),
				map[string][]string{nodeN00: {nodeN01}, nodeN01: {nodeN01}}), // N01 knows itself (gossip includes self)
			facts: mFacts(),
			want: MigrationPlan{
				Phase:       MigrationReplicate, // stay in Replicate: attach N00 onto the new owner N01
				Replicates:  []ReplicaAttach{{ReplicaAddr: addrShard00, MasterID: nodeN01}},
				TotalShards: 2,
			},
		},
		{
			name: "INVARIANT (i): K-0 not a link-up replica of owner must NOT emit a Failover",
			gt: knows(mgt(
				rMaster("mr-cluster-0", "L0", "0-8191"),
				rMaster("mr-cluster-1", "L1", "8192-16383"),
				rReplica("mr-cluster-2", "L2", "L0"),
				rReplica("mr-cluster-3", "L3", "L1"),
				rEmpty("mr-shard-0-0", nodeN00), // K-0 still an empty master (NOT synced)
				rReplicaUp("mr-shard-0-1", nodeN01, "L0"),
				rReplicaUp("mr-shard-1-0", nodeN10, "L1"),
				rReplicaUp("mr-shard-1-1", nodeN11, "L1")), knowsOwners),
			facts: mFacts(),
			want: MigrationPlan{
				Phase:       MigrationReplicate, // stays in Replicate; no Failover for shard 0
				Replicates:  []ReplicaAttach{{ReplicaAddr: addrShard00, MasterID: "L0"}},
				TotalShards: 2,
			},
		},
		{
			name: "failover: all new pods synced replicas, no K-0 owns its range -> one, lowest K",
			gt: mgt(
				rMaster("mr-cluster-0", "L0", "0-8191"),
				rMaster("mr-cluster-1", "L1", "8192-16383"),
				rReplica("mr-cluster-2", "L2", "L0"),
				rReplica("mr-cluster-3", "L3", "L1"),
				rReplicaUp("mr-shard-0-0", nodeN00, "L0"),
				rReplicaUp("mr-shard-0-1", nodeN01, "L0"),
				rReplicaUp("mr-shard-1-0", nodeN10, "L1"),
				rReplicaUp("mr-shard-1-1", nodeN11, "L1")),
			facts: mFacts(),
			want: MigrationPlan{
				Phase:       MigrationFailover,
				Failovers:   []FailoverAction{{Addr: addrShard00, Force: false}},
				TotalShards: 2,
			},
		},
		{
			name: "failover FORCE edge: range owner unreachable + K-0 synced -> TAKEOVER",
			gt: mgt(
				func() *ClusterNodeState { n := rMaster("mr-cluster-0", "L0", "0-8191"); n.Reachable = false; return n }(),
				rMaster("mr-cluster-1", "L1", "8192-16383"),
				rReplica("mr-cluster-2", "L2", "L0"),
				rReplica("mr-cluster-3", "L3", "L1"),
				rReplicaUp("mr-shard-0-0", nodeN00, "L0"), // confirmed synced before L0 died
				rReplicaUp("mr-shard-0-1", nodeN01, "L0"),
				rReplicaUp("mr-shard-1-0", nodeN10, "L1"),
				rReplicaUp("mr-shard-1-1", nodeN11, "L1")),
			facts: mFacts(),
			want: MigrationPlan{
				Phase:       MigrationFailover,
				Failovers:   []FailoverAction{{Addr: addrShard00, Force: true}},
				TotalShards: 2,
			},
		},
		{
			name: "failover: shard 0 done (owns range), shard 1 pending -> failover shard 1",
			gt: mgt(
				rMaster("mr-shard-0-0", nodeN00, "0-8191"), // shard 0 already promoted
				rReplicaUp("mr-shard-0-1", nodeN01, nodeN00),
				rReplicaUp("mr-shard-1-0", nodeN10, "L1"),
				rReplicaUp("mr-shard-1-1", nodeN11, "L1"),
				rReplica("mr-cluster-0", "L0", nodeN00), // demoted legacy master, now replica of N00
				rMaster("mr-cluster-1", "L1", "8192-16383"),
				rReplica("mr-cluster-2", "L2", "L0"),
				rReplica("mr-cluster-3", "L3", "L1")),
			facts: mFacts(),
			want: MigrationPlan{
				Phase:       MigrationFailover,
				Failovers:   []FailoverAction{{Addr: addrShard10, Force: false}},
				ShardsMoved: 1,
				TotalShards: 2,
			},
		},
		{
			name: "INVARIANT (ii): a new replica not link-up on its new master must NOT emit legacy Forgets",
			gt: knows(mgt(
				rMaster("mr-shard-0-0", nodeN00, "0-8191"),
				rEmpty("mr-shard-0-1", nodeN01), // NOT yet a link-up replica of its new master N00
				rMaster("mr-shard-1-0", nodeN10, "8192-16383"),
				rReplicaUp("mr-shard-1-1", nodeN11, nodeN10),
				rReplica("mr-cluster-0", "L0", nodeN00),
				rReplica("mr-cluster-1", "L1", nodeN10),
				rReplica("mr-cluster-2", "L2", nodeN00),
				rReplica("mr-cluster-3", "L3", nodeN10)),
				map[string][]string{nodeN01: {nodeN00}}),
			facts: mFacts(),
			want: MigrationPlan{
				Phase:       MigrationReplicate, // reparent the lagging replica; do NOT Forget yet
				Replicates:  []ReplicaAttach{{ReplicaAddr: addrShard01, MasterID: nodeN00}},
				ShardsMoved: 2,
				TotalShards: 2,
			},
		},
		{
			name: "decommission: all masters own ranges, all new replicas synced, legacy demoted+present",
			gt: mgt(
				rMaster("mr-shard-0-0", nodeN00, "0-8191"),
				rReplicaUp("mr-shard-0-1", nodeN01, nodeN00),
				rMaster("mr-shard-1-0", nodeN10, "8192-16383"),
				rReplicaUp("mr-shard-1-1", nodeN11, nodeN10),
				rReplica("mr-cluster-0", "L0", nodeN00), // demoted legacy masters (slot-less)
				rReplica("mr-cluster-1", "L1", nodeN10),
				rReplica("mr-cluster-2", "L2", nodeN00),
				rReplica("mr-cluster-3", "L3", nodeN10)),
			facts: mFacts(),
			want: MigrationPlan{
				Phase:        MigrationDecommission,
				Forgets:      []string{"L0", "L1", "L2", "L3"},
				DeleteLegacy: true,
				ShardsMoved:  2,
				TotalShards:  2,
			},
		},
		{
			name: "complete: no legacy nodes remain",
			gt: mgt(
				rMaster("mr-shard-0-0", nodeN00, "0-8191"),
				rReplicaUp("mr-shard-0-1", nodeN01, nodeN00),
				rMaster("mr-shard-1-0", nodeN10, "8192-16383"),
				rReplicaUp("mr-shard-1-1", nodeN11, nodeN10)),
			facts: mFacts(),
			want:  MigrationPlan{Phase: MigrationComplete, ShardsMoved: 2, TotalShards: 2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PlanClusterMigration(tc.gt, shards, rps, name, tc.facts)
			assertPlanEqual(t, got, tc.want)
		})
	}
}

// TestPlanClusterMigrationRPS0 exercises the replicasPerShard=0 path: no new replicas, so
// the Decommission redundancy gate is vacuous — Decommission is reached as soon as every
// {name}-shard-K-0 owns its range.
func TestPlanClusterMigrationRPS0(t *testing.T) {
	const name, shards, rps = "mr", 2, 0
	facts := LegacyFacts{
		SeedAddrs:     []string{seedAddr},
		LegacyNodeIDs: []string{"L0", "L1"},
		NewPodAddrs: map[string]string{
			"mr-shard-0-0": addrShard00,
			"mr-shard-1-0": addrShard10,
		},
	}
	// Intact rps=0 legacy: two masters, no legacy replicas.
	legFull := func() []*ClusterNodeState {
		return []*ClusterNodeState{
			rMaster("mr-cluster-0", "L0", "0-8191"),
			rMaster("mr-cluster-1", "L1", "8192-16383"),
		}
	}

	tests := []struct {
		name string
		gt   *ClusterGroundTruth
		want MigrationPlan
	}{
		{
			name: "replicate rps=0: both new masters replicate their legacy owner",
			gt: knows(mgt(append(legFull(),
				rEmpty("mr-shard-0-0", nodeN00), rEmpty("mr-shard-1-0", nodeN10))...),
				map[string][]string{nodeN00: {"L0"}, nodeN10: {"L1"}}),
			want: MigrationPlan{
				Phase: MigrationReplicate,
				Replicates: []ReplicaAttach{
					{ReplicaAddr: addrShard00, MasterID: "L0"},
					{ReplicaAddr: addrShard10, MasterID: "L1"},
				},
				TotalShards: 2,
			},
		},
		{
			name: "failover rps=0: both synced, no K-0 owns range -> failover shard 0",
			gt: mgt(append(legFull(),
				rReplicaUp("mr-shard-0-0", nodeN00, "L0"), rReplicaUp("mr-shard-1-0", nodeN10, "L1"))...),
			want: MigrationPlan{
				Phase:       MigrationFailover,
				Failovers:   []FailoverAction{{Addr: addrShard00, Force: false}},
				TotalShards: 2,
			},
		},
		{
			name: "decommission rps=0: both K-0 own ranges (vacuous redundancy gate), legacy demoted",
			gt: mgt(
				rMaster("mr-shard-0-0", nodeN00, "0-8191"),
				rMaster("mr-shard-1-0", nodeN10, "8192-16383"),
				rReplica("mr-cluster-0", "L0", nodeN00),
				rReplica("mr-cluster-1", "L1", nodeN10)),
			want: MigrationPlan{
				Phase:        MigrationDecommission,
				Forgets:      []string{"L0", "L1"},
				DeleteLegacy: true,
				ShardsMoved:  2,
				TotalShards:  2,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := PlanClusterMigration(tc.gt, shards, rps, name, facts)
			assertPlanEqual(t, got, tc.want)
		})
	}
}

// assertPlanEqual compares the load-bearing fields of a MigrationPlan, ignoring Reason
// (a human string). Move is no longer part of the LR-025 migration plan.
func assertPlanEqual(t *testing.T, got, want MigrationPlan) {
	t.Helper()
	if got.Phase != want.Phase {
		t.Errorf("Phase = %q, want %q", got.Phase, want.Phase)
	}
	if !reflect.DeepEqual(got.Meets, want.Meets) {
		t.Errorf("Meets = %v, want %v", got.Meets, want.Meets)
	}
	if !reflect.DeepEqual(got.Replicates, want.Replicates) {
		t.Errorf("Replicates = %v, want %v", got.Replicates, want.Replicates)
	}
	if !reflect.DeepEqual(got.Failovers, want.Failovers) {
		t.Errorf("Failovers = %v, want %v", got.Failovers, want.Failovers)
	}
	if !reflect.DeepEqual(got.Forgets, want.Forgets) {
		t.Errorf("Forgets = %v, want %v", got.Forgets, want.Forgets)
	}
	if got.DeleteLegacy != want.DeleteLegacy {
		t.Errorf("DeleteLegacy = %v, want %v", got.DeleteLegacy, want.DeleteLegacy)
	}
	if got.ShardsMoved != want.ShardsMoved {
		t.Errorf("ShardsMoved = %d, want %d", got.ShardsMoved, want.ShardsMoved)
	}
	if got.TotalShards != want.TotalShards {
		t.Errorf("TotalShards = %d, want %d", got.TotalShards, want.TotalShards)
	}
}

// TestOwnerOfRange asserts ownerOfRange returns the node owning exactly the given aligned
// range (regardless of reachability), or nil when none does.
func TestOwnerOfRange(t *testing.T) {
	gt := mgt(
		rMaster("mr-cluster-0", "L0", "0-8191"),
		rMaster("mr-cluster-1", "L1", "8192-16383"),
		rReplica("mr-cluster-2", "L2", "L0"),
	)
	if got := ownerOfRange(gt, 0, 8191); got == nil || got.NodeID != "L0" {
		t.Errorf("ownerOfRange(0,8191) = %+v, want NodeID L0", got)
	}
	if got := ownerOfRange(gt, 8192, 16383); got == nil || got.NodeID != "L1" {
		t.Errorf("ownerOfRange(8192,16383) = %+v, want NodeID L1", got)
	}
	if got := ownerOfRange(gt, 100, 200); got != nil {
		t.Errorf("ownerOfRange(100,200) = %+v, want nil (no exact owner)", got)
	}
	// An unreachable owner is still the owner (used by the Force-failover edge).
	gt.Nodes["mr-cluster-0"].Reachable = false
	if got := ownerOfRange(gt, 0, 8191); got == nil || got.NodeID != "L0" {
		t.Errorf("ownerOfRange with unreachable owner = %+v, want NodeID L0", got)
	}
}

// TestIsLinkUpReplicaOf asserts the synced-replica gate: role replica, of the given master,
// with link status "up". Anything else is not yet synced.
func TestIsLinkUpReplicaOf(t *testing.T) {
	tests := []struct {
		name string
		node *ClusterNodeState
		want bool
	}{
		{"link-up replica of target", rReplicaUp("p", "R", "M"), true},
		{"replica of target but link down", rReplica("p", "R", "M"), false},
		{"link-up replica of a different master", rReplicaUp("p", "R", "OTHER"), false},
		{"a master, not a replica", func() *ClusterNodeState { n := rMaster("p", "R"); n.LinkStatus = "up"; return n }(), false},
		{"nil node", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsLinkUpReplicaOf(tc.node, "M"); got != tc.want {
				t.Errorf("IsLinkUpReplicaOf = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPlanReplicateNeverTargetsSelf encodes the hard invariant from the chaos deadlock
// (MIGRATION_CHAOS_SELF_REPLICATE_DEADLOCK): PlanClusterMigration must NEVER emit a CLUSTER
// REPLICATE whose target master NodeID is the replica pod's OWN NodeID (ERR Can't replicate
// myself). A mid-migration native failover can promote any new-side pod to own its shard's
// range; the planner must treat that pod as settled, not try to make it replicate itself.
func TestPlanReplicateNeverTargetsSelf(t *testing.T) {
	const name, shards, rps = "mr", 2, 1
	facts := mFacts()

	// Two chaos shapes: the promoted owner is the new replica pod (N01), and — same class —
	// the promoted owner is the intended new master pod itself before it owns via the normal path.
	fixtures := []struct {
		name string
		gt   *ClusterGroundTruth
	}{
		{
			name: "native failover promoted new replica mr-shard-0-1 to own range 0",
			gt: knows(mgt(
				rMaster("mr-shard-0-1", nodeN01, "0-8191"),
				rEmpty("mr-shard-0-0", nodeN00),
				rMaster("mr-cluster-1", "L1", "8192-16383"),
				rReplicaUp("mr-shard-1-0", nodeN10, "L1"),
				rReplicaUp("mr-shard-1-1", nodeN11, "L1")),
				map[string][]string{nodeN00: {nodeN01}, nodeN01: {nodeN01}}),
		},
		{
			name: "new replica mr-shard-1-1 owns range 1 while its shard is otherwise un-synced",
			gt: knows(mgt(
				rMaster("mr-shard-0-0", nodeN00, "0-8191"),
				rReplicaUp("mr-shard-0-1", nodeN01, nodeN00),
				rEmpty("mr-shard-1-0", nodeN10),                // intended master, not owner
				rMaster("mr-shard-1-1", nodeN11, "8192-16383"), // promoted to own range 1
				rEmpty("mr-cluster-1", "L1")),                  // a lingering (demoted) legacy node (keeps us pre-Complete)
				map[string][]string{nodeN10: {nodeN11}, nodeN11: {nodeN11}}),
		},
	}

	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			plan := PlanClusterMigration(f.gt, shards, rps, name, facts)

			addrToID := map[string]string{}
			for podName, addr := range facts.NewPodAddrs {
				if n := f.gt.Nodes[podName]; n != nil {
					addrToID[addr] = n.NodeID
				}
			}
			for _, ra := range plan.Replicates {
				if selfID, ok := addrToID[ra.ReplicaAddr]; ok && selfID == ra.MasterID {
					t.Fatalf("plan emitted REPLICATE self: replica %s (node %s) -> master %s (phase %s)",
						ra.ReplicaAddr, selfID, ra.MasterID, plan.Phase)
				}
			}
		})
	}
}

func TestLegacyMigrationReady(t *testing.T) {
	healthy := func() *ClusterGroundTruth {
		return mgt(
			rMaster("mr-cluster-0", "L0", "0-8191"),
			rMaster("mr-cluster-1", "L1", "8192-16383"),
			rReplica("mr-cluster-2", "L2", "L0"),
			rReplica("mr-cluster-3", "L3", "L1"),
		)
	}

	tests := []struct {
		name      string
		mutate    func(gt *ClusterGroundTruth)
		podsReady bool
		want      bool
	}{
		{name: "healthy", mutate: func(*ClusterGroundTruth) {}, podsReady: true, want: true},
		{name: "missing slots", mutate: func(gt *ClusterGroundTruth) { gt.TotalSlots = 16000 }, podsReady: true, want: false},
		{name: "state not ok", mutate: func(gt *ClusterGroundTruth) { gt.ClusterState = "fail" }, podsReady: true, want: false},
		{name: "a legacy pod not ready", mutate: func(*ClusterGroundTruth) {}, podsReady: false, want: false},
		{
			name: "no master quorum",
			mutate: func(gt *ClusterGroundTruth) {
				gt.Nodes["mr-cluster-1"].Reachable = false // 1 of 2 masters reachable
			},
			podsReady: true,
			want:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gt := healthy()
			tc.mutate(gt)
			if got := LegacyMigrationReady(gt, tc.podsReady); got != tc.want {
				t.Errorf("legacyMigrationReady = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLegacyShapePreserved(t *testing.T) {
	const shards, rps = 2, 1
	exact := func() *ClusterGroundTruth {
		return mgt(
			rMaster("mr-cluster-0", "L0", "0-8191"),
			rMaster("mr-cluster-1", "L1", "8192-16383"),
			rReplica("mr-cluster-2", "L2", "L0"),
			rReplica("mr-cluster-3", "L3", "L1"),
		)
	}

	tests := []struct {
		name string
		gt   *ClusterGroundTruth
		want bool
	}{
		{name: "exact shape", gt: exact(), want: true},
		{
			name: "wrong master count (consolidated)",
			gt: mgt(
				rMaster("mr-cluster-0", "L0", "0-8191", "8192-16383"),
				rReplica("mr-cluster-2", "L2", "L0"),
				rReplica("mr-cluster-3", "L3", "L0"),
				rReplica("mr-cluster-1", "L1", "L0"),
			),
			want: false,
		},
		{
			name: "fragmented / non-aligned range",
			gt: mgt(
				rMaster("mr-cluster-0", "L0", "0-4000"),
				rMaster("mr-cluster-1", "L1", "8192-16383"),
				rReplica("mr-cluster-2", "L2", "L0"),
				rReplica("mr-cluster-3", "L3", "L1"),
			),
			want: false,
		},
		{
			name: "wrong member count",
			gt: mgt(
				rMaster("mr-cluster-0", "L0", "0-8191"),
				rMaster("mr-cluster-1", "L1", "8192-16383"),
				rReplica("mr-cluster-2", "L2", "L0"),
				rReplica("mr-cluster-3", "L3", "L1"),
				rReplica("mr-cluster-4", "L4", "L0"),
			),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LegacyShapePreserved(tc.gt, shards, rps); got != tc.want {
				t.Errorf("legacyShapePreserved = %v, want %v", got, tc.want)
			}
		})
	}
}
