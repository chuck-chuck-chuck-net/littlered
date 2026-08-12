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
	"errors"
	"slices"
	"testing"
)

// perIPGatherer is a Gatherer returning a DISTINCT CLUSTER INFO / CLUSTER NODES / INFO
// replication per pod IP, so the survivor-vantage gather fields (ADR-012) can be exercised
// with a realistic mid-loss topology: one survivor reporting cluster_state:fail and its
// CLUSTER NODES flagging a dead master FAIL and a suspect peer PFAIL.
type perIPGatherer struct {
	nodeID   map[string]string
	info     map[string]*ClusterInfo
	nodes    map[string][]ClusterNodeInfo
	repl     map[string]*RedisNodeState
	deadDial map[string]bool
}

func (g *perIPGatherer) GetRedisState(_ context.Context, podName, ip string) (*RedisNodeState, error) {
	if g.deadDial[ip] {
		return nil, errors.New("dial timeout")
	}
	rs := g.repl[ip]
	if rs == nil {
		return &RedisNodeState{PodName: podName, IP: ip, Reachable: true}, nil
	}
	cp := *rs
	cp.PodName, cp.IP, cp.Reachable = podName, ip, true
	return &cp, nil
}

func (g *perIPGatherer) GetSentinelState(_ context.Context, _, _ string) (*SentinelNodeState, error) {
	return nil, errors.New("not used")
}

func (g *perIPGatherer) GetClusterID(_ context.Context, _, ip string) (string, error) {
	if g.deadDial[ip] {
		return "", errors.New("dial timeout")
	}
	return g.nodeID[ip], nil
}

func (g *perIPGatherer) GetClusterInfo(_ context.Context, _, ip string) (*ClusterInfo, error) {
	if g.deadDial[ip] {
		return nil, errors.New("dial timeout")
	}
	return g.info[ip], nil
}

func (g *perIPGatherer) GetClusterNodes(_ context.Context, _, ip string) ([]ClusterNodeInfo, error) {
	if g.deadDial[ip] {
		return nil, errors.New("dial timeout")
	}
	return g.nodes[ip], nil
}

// TestGatherClusterGroundTruth_SurvivorVantageFields asserts the ADR-012 survivor-vantage
// fields are populated from data already on the wire: a survivor's own cluster_state:fail,
// its FAIL/PFAIL peer verdicts, and its replication offset/lineage.
func TestGatherClusterGroundTruth_SurvivorVantageFields(t *testing.T) {
	const (
		survivorIP = "10.0.0.1" // a surviving replica; has fail-stopped (minority)
		deadIP     = "10.0.0.2" // the dead master's stale IP (dial blackholes)
	)
	// The survivor's CLUSTER NODES: itself (a replica of the dead master), the dead master
	// flagged FAIL, and a suspect peer flagged PFAIL.
	survivorGossip := []ClusterNodeInfo{
		{NodeID: "surv", Flags: []string{flagSlave}, MasterID: "deadm"},
		{NodeID: "deadm", Flags: []string{roleMaster, flagFail}, MasterID: "-", Slots: []string{slotsLowerHalf}},
		{NodeID: "suspect", Flags: []string{roleMaster, flagPFail}, MasterID: "-", Slots: []string{slotsUpperHalf}},
	}

	g := &perIPGatherer{
		nodeID:   map[string]string{survivorIP: "surv"},
		deadDial: map[string]bool{deadIP: true},
		info: map[string]*ClusterInfo{
			survivorIP: {State: flagFail, SlotsAssigned: 16384},
		},
		nodes: map[string][]ClusterNodeInfo{
			survivorIP: survivorGossip,
		},
		repl: map[string]*RedisNodeState{
			survivorIP: {Role: "slave", Offset: 987654, Replid: "REPLID_NEW", Replid2: "REPLID_OLD"},
		},
	}

	clusterPods := map[string]string{survivorIP: "inst-shard-0-1", deadIP: "inst-shard-0-0"}
	gt := GatherClusterGroundTruth(context.Background(), g, clusterPods)

	surv := gt.Nodes["inst-shard-0-1"]
	if surv == nil {
		t.Fatalf("survivor node missing from ground truth")
	}
	if surv.ClusterState != flagFail {
		t.Errorf("survivor ClusterState = %q, want fail (first-hand minority fail-stop)", surv.ClusterState)
	}
	if !slices.Contains(surv.FailFlagged, "deadm") {
		t.Errorf("survivor FailFlagged = %v, want it to contain deadm", surv.FailFlagged)
	}
	if !slices.Contains(surv.PFailFlagged, "suspect") {
		t.Errorf("survivor PFailFlagged = %v, want it to contain suspect", surv.PFailFlagged)
	}
	if slices.Contains(surv.FailFlagged, "suspect") {
		t.Errorf("PFAIL peer must NOT appear in FailFlagged: %v", surv.FailFlagged)
	}
	if surv.ReplOffset != 987654 {
		t.Errorf("survivor ReplOffset = %d, want 987654", surv.ReplOffset)
	}
	if surv.MasterReplid != "REPLID_NEW" || surv.MasterReplid2 != "REPLID_OLD" {
		t.Errorf("survivor lineage = (%q,%q), want (REPLID_NEW,REPLID_OLD)", surv.MasterReplid, surv.MasterReplid2)
	}

	// The dead master (dial blackholes) is unreachable and carries none of the fields.
	if dead := gt.Nodes["inst-shard-0-0"]; dead == nil || dead.Reachable {
		t.Errorf("dead master should be present and unreachable, got %+v", dead)
	}
}
