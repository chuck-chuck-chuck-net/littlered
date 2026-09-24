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
	"context"
	"reflect"
	"testing"
)

const (
	ghostDeadID    = "deadX"
	ghostSuspectID = "suspect"
	ipSiteB0       = "10.0.0.12"
	ipSiteB1       = "10.0.0.13"
)

// TestDeriveGhostNodes pins the ghost criterion (LR-030): a ghost is a NodeID that
// reachable peers' gossip marks objectively FAIL (Redis's own quorum verdict), NOT a
// node the hub simply cannot dial. PFAIL (subjective single-node suspicion) is not a
// ghost. Red-first: against the nil stub the "is a ghost" cases (genuinely-down,
// fail-flagged-by-one, dedup/sort) fail; the "not a ghost" cases are the semantic
// guarantee and are additionally pinned by mutation checks (see report).
func TestDeriveGhostNodes(t *testing.T) {
	// reachable builds a reachable node carrying its gossip verdicts on peers.
	reachable := func(id string, fail, pfail []string) *ClusterNodeState {
		return &ClusterNodeState{NodeID: id, Reachable: true, FailFlagged: fail, PFailFlagged: pfail}
	}

	tests := []struct {
		name  string
		nodes map[string]*ClusterNodeState
		want  []string
	}{
		{
			// The headline bug (multi-site hub-blind data cut): a live site-B master the
			// hub cannot dial, but which its peers gossip-reach and do NOT fail-flag, must
			// NOT be a ghost. No reachable node fail-flags it → not a ghost.
			name: "multisite_hub_blind_live_node_not_ghost",
			nodes: map[string]*ClusterNodeState{
				"p0": reachable("m1", nil, nil),
				"p1": reachable("m2", nil, nil),
			},
			want: nil,
		},
		{
			// LR-017 class, single-site: a node that blackholes/ RSTs the operator's dial
			// but is reachable to its peers is not fail-flagged → not a ghost.
			name: "single_site_blackhole_to_operator_only_not_ghost",
			nodes: map[string]*ClusterNodeState{
				"p0": reachable("m1", nil, nil),
			},
			want: nil,
		},
		{
			// A genuinely-down node: reachable peers agree it is FAIL → ghost.
			name: "genuinely_down_is_ghost",
			nodes: map[string]*ClusterNodeState{
				"p0": reachable("m1", []string{ghostDeadID}, nil),
				"p1": reachable("m2", []string{ghostDeadID}, nil),
			},
			want: []string{ghostDeadID},
		},
		{
			// FAIL is quorum-propagated: one reachable node fail-flagging is sufficient.
			name: "fail_flagged_by_one_of_many_is_ghost",
			nodes: map[string]*ClusterNodeState{
				"p0": reachable("m1", []string{"g"}, nil),
				"p1": reachable("m2", nil, nil),
			},
			want: []string{"g"},
		},
		{
			// PFAIL (subjective) is NOT a ghost — only FAIL is the objective verdict.
			name: "pfail_only_not_ghost",
			nodes: map[string]*ClusterNodeState{
				"p0": reachable("m1", nil, []string{ghostSuspectID}),
				"p1": reachable("m2", nil, []string{ghostSuspectID}),
			},
			want: nil,
		},
		{
			// Union across peers, deduplicated and sorted for deterministic FORGET order.
			name: "dedup_and_sort",
			nodes: map[string]*ClusterNodeState{
				"p0": reachable("m1", []string{"b", "a"}, nil),
				"p1": reachable("m2", []string{"a"}, nil),
			},
			want: []string{"a", "b"},
		},
		{
			// Only a REACHABLE node's verdict counts. An unreachable node never has its
			// gossip probed (FailFlagged stays empty in practice); guard the criterion
			// wording by ignoring any FailFlagged an unreachable node might carry.
			name: "unreachable_node_verdict_ignored",
			nodes: map[string]*ClusterNodeState{
				"p0": reachable("m1", nil, nil),
				"p1": {NodeID: "m2", Reachable: false, FailFlagged: []string{"z"}},
			},
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveGhostNodes(tc.nodes)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("deriveGhostNodes = %v, want %v", got, tc.want)
			}
		})
	}
}

// hubBlindGossipHealthyGossip is a CLUSTER NODES view in which every shard master is a
// healthy (non-fail-flagged) master. The reachable survivors return this view: their
// peers on the unreachable site are gossip-healthy, NOT flagged FAIL.
func hubBlindGossipHealthyGossip() []ClusterNodeInfo {
	return []ClusterNodeInfo{
		{NodeID: "m1", Flags: []string{roleMaster}, MasterID: "-", Slots: []string{"0-4095"}},
		{NodeID: "m2", Flags: []string{roleMaster}, MasterID: "-", Slots: []string{"4096-8191"}},
		{NodeID: "m3", Flags: []string{roleMaster}, MasterID: "-", Slots: []string{"8192-12287"}},
		{NodeID: "m4", Flags: []string{roleMaster}, MasterID: "-", Slots: []string{"12288-16383"}},
	}
}

// TestGatherClusterGroundTruth_HubBlindGossipHealthy is the integration repro of the
// LR-030 bug against the real gather pipeline. Two site-B masters (m3, m4) are NOT
// dial-reachable to the hub, but the reachable survivors (m1, m2) gossip-reach them and
// do NOT flag them FAIL. Under the old dial-based derivation ghosts = AllNodeIDs −
// dial-reachable = {m3, m4} (the operator would then FORGET the live nodes and
// manufacture the very outage it "repairs"). Under the FailFlagged criterion no reachable
// node flags m3/m4, so there are no ghosts. Red against the dial-based loop; green after.
func TestGatherClusterGroundTruth_HubBlindGossipHealthy(t *testing.T) {
	// pod2/pod3 (site B) are dead to the hub's dial; pod0/pod1 (site A) are reachable.
	g := &fakeGatherer{
		nodeID: map[string]string{ipPod0: "m1", ipPod1: "m2"},
		dead:   map[string]bool{ipSiteB0: true, ipSiteB1: true},
		gossip: hubBlindGossipHealthyGossip(),
	}
	clusterPods := map[string]string{
		ipPod0:   namePod0,
		ipPod1:   namePod1,
		ipSiteB0: namePod2,
		ipSiteB1: "pod-3",
	}

	gt := GatherClusterGroundTruth(context.Background(), g, clusterPods)

	// The live-but-hub-unreachable site-B masters must NOT be classified as ghosts.
	if len(gt.GhostNodes) != 0 {
		t.Fatalf("hub-blind but gossip-healthy: expected no ghosts, got %v", gt.GhostNodes)
	}
	// AllNodeIDs is unchanged (still needed by IsHealthy): all four masters are seen.
	if len(gt.AllNodeIDs) != 4 {
		t.Errorf("expected 4 NodeIDs in the mesh, got %d: %v", len(gt.AllNodeIDs), gt.AllNodeIDs)
	}
}
