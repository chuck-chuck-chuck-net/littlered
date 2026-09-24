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

import "testing"

// master returns a slot-owning master node with the given id.
func master(id string, slots ...string) *ClusterNodeState {
	return &ClusterNodeState{NodeID: id, Role: roleMaster, MasterNodeID: "-", Slots: slots}
}

// emptyMaster returns a master node with no slots (a cold-start / restarted pod).
func emptyMaster(id string) *ClusterNodeState {
	return &ClusterNodeState{NodeID: id, Role: roleMaster, MasterNodeID: "-"}
}

// replica returns a replica node following the given master id.
func replica(id, masterID string) *ClusterNodeState {
	return &ClusterNodeState{NodeID: id, Role: roleReplica, MasterNodeID: masterID}
}

// gtFrom builds a ClusterGroundTruth from a set of nodes, deriving AllNodeIDs and
// a single-partition view. TotalSlots/ClusterState are set to a healthy baseline
// so tests can isolate the topology predicates.
func gtFrom(nodes ...*ClusterNodeState) *ClusterGroundTruth {
	gt := NewClusterGroundTruth()
	for _, n := range nodes {
		// IsHealthy keys off counts and node roles, not pod names, so keying the
		// map by NodeID here is sufficient.
		gt.Nodes[n.NodeID] = n
		gt.AllNodeIDs[n.NodeID] = true
	}
	gt.ClusterState = "ok"
	gt.TotalSlots = 16384
	return gt
}

func TestNodeKnows(t *testing.T) {
	gt := NewClusterGroundTruth()
	gt.KnownNodes = map[string][]string{
		"empty": {"m0", "m1"}, // freshly MEETed node knows m0/m1 but not yet m2
	}

	if !gt.NodeKnows("empty", "m0") {
		t.Error("expected empty to know m0")
	}
	if gt.NodeKnows("empty", "m2") {
		t.Error("expected empty NOT to know m2 (gossip not yet converged)")
	}
	// Observer whose view was never gathered is treated as knowing nothing — the
	// safe default that defers CLUSTER REPLICATE.
	if gt.NodeKnows("ungathered", "m0") {
		t.Error("expected ungathered observer to know nothing")
	}
}

func TestIsHealthy(t *testing.T) {
	// Canonical healthy 3-shard / 1-replica topology: 3 slot-owning masters,
	// each with exactly one replica.
	healthyNodes := func() []*ClusterNodeState {
		return []*ClusterNodeState{
			master("m0", "0-5460"),
			master("m1", "5461-10922"),
			master("m2", "10923-16383"),
			replica("r0", "m0"),
			replica("r1", "m1"),
			replica("r2", "m2"),
		}
	}

	tests := []struct {
		name          string
		nodes         []*ClusterNodeState
		expectedNodes int32
		expectedShard int32
		state         string
		totalSlots    int
		want          bool
	}{
		{
			name:          "healthy 3x1 topology",
			nodes:         healthyNodes(),
			expectedNodes: 6,
			expectedShard: 3,
			state:         "ok",
			totalSlots:    16384,
			want:          true,
		},
		{
			name: "empty master is not healthy (LR-014)",
			// One shard's restarted pod came back as an empty master; its
			// promoted shard therefore has zero replicas. Counts still add up
			// to 6 nodes / 3 slot-masters / 16384 slots, so the pre-LR-014
			// checks all pass — only HasEmptyMasters() catches it.
			nodes: []*ClusterNodeState{
				master("m0", "0-5460"),
				master("m1", "5461-10922"),
				master("m2", "10923-16383"),
				replica("r0", "m0"),
				replica("r1", "m1"),
				emptyMaster("e2"),
			},
			expectedNodes: 6,
			expectedShard: 3,
			state:         "ok",
			totalSlots:    16384,
			want:          false,
		},
		{
			name:          "healthy zero-replica topology",
			nodes:         []*ClusterNodeState{master("m0", "0-5460"), master("m1", "5461-10922"), master("m2", "10923-16383")},
			expectedNodes: 3,
			expectedShard: 3,
			state:         "ok",
			totalSlots:    16384,
			want:          true,
		},
		{
			name: "replica maldistribution is tolerated (no rebalance step exists)",
			// 2 replicas on m0, 1 on m1, 0 on m2, no empty master. Not the
			// intended shape, but no repair step rebalances replicas, so
			// gating health on it would deadlock. Tracked separately.
			nodes: []*ClusterNodeState{
				master("m0", "0-5460"),
				master("m1", "5461-10922"),
				master("m2", "10923-16383"),
				replica("r0a", "m0"),
				replica("r0b", "m0"),
				replica("r1", "m1"),
			},
			expectedNodes: 6,
			expectedShard: 3,
			state:         "ok",
			totalSlots:    16384,
			want:          true,
		},
		{
			name:          "missing node is not healthy",
			nodes:         healthyNodes()[:5],
			expectedNodes: 6,
			expectedShard: 3,
			state:         "ok",
			totalSlots:    16384,
			want:          false,
		},
		{
			name:          "incomplete slots is not healthy",
			nodes:         healthyNodes(),
			expectedNodes: 6,
			expectedShard: 3,
			state:         "ok",
			totalSlots:    16383,
			want:          false,
		},
		{
			name:          "cluster state not ok is not healthy",
			nodes:         healthyNodes(),
			expectedNodes: 6,
			expectedShard: 3,
			state:         flagFail,
			totalSlots:    16384,
			want:          false,
		},
		{
			name: "wrong master count is not healthy",
			nodes: []*ClusterNodeState{
				master("m0", slotsLowerHalf),
				master("m1", slotsUpperHalf),
				replica("r0", "m0"),
				replica("r1", "m1"),
				replica("r2", "m0"),
				replica("r3", "m1"),
			},
			expectedNodes: 6,
			expectedShard: 3,
			state:         "ok",
			totalSlots:    16384,
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gt := gtFrom(tt.nodes...)
			gt.ClusterState = tt.state
			gt.TotalSlots = tt.totalSlots
			if got := gt.IsHealthy(tt.expectedNodes, tt.expectedShard); got != tt.want {
				t.Errorf("IsHealthy(%d, %d) = %v, want %v", tt.expectedNodes, tt.expectedShard, got, tt.want)
			}
		})
	}
}
