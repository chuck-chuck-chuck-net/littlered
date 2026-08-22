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
	"sync"
	"testing"
	"time"
)

// fakeGatherer is a programmable Gatherer for exercising GatherClusterGroundTruth
// without a live cluster. Each pod IP maps to a canned identity; unreachable IPs
// return an error after an optional delay (to simulate a dead pod's dial timeout).
type fakeGatherer struct {
	// nodeID returns the CLUSTER MYID for a reachable pod IP.
	nodeID map[string]string
	// dead marks IPs whose probes fail (stale/deleted pods).
	dead map[string]bool
	// probeDelay is slept before every probe, used to assert parallelism.
	probeDelay time.Duration
	// gossip is the CLUSTER NODES view returned by every reachable node.
	gossip []ClusterNodeInfo
	// concurrency tracks the max number of in-flight GetClusterID calls.
	mu        sync.Mutex
	inFlight  int
	maxFlight int
}

// enter/leave track the max number of concurrently in-flight probes, so tests can
// assert that a gather fans out instead of dialing pods one at a time.
func (f *fakeGatherer) enter() {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxFlight {
		f.maxFlight = f.inFlight
	}
	f.mu.Unlock()
}

func (f *fakeGatherer) leave() {
	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()
}

func (f *fakeGatherer) GetRedisState(_ context.Context, podName, ip string) (*RedisNodeState, error) {
	f.enter()
	defer f.leave()
	if f.probeDelay > 0 {
		time.Sleep(f.probeDelay)
	}
	if f.dead[ip] {
		return nil, errors.New("dial timeout")
	}
	return &RedisNodeState{PodName: podName, IP: ip, Reachable: true, LinkStatus: "up"}, nil
}

func (f *fakeGatherer) GetSentinelState(_ context.Context, podName, ip, _ string) (*SentinelNodeState, error) {
	f.enter()
	defer f.leave()
	if f.probeDelay > 0 {
		time.Sleep(f.probeDelay)
	}
	if f.dead[ip] {
		return nil, errors.New("dial timeout")
	}
	return &SentinelNodeState{PodName: podName, IP: ip, Reachable: true, Monitoring: false}, nil
}

func (f *fakeGatherer) GetClusterID(_ context.Context, _, ip string) (string, error) {
	f.enter()
	defer f.leave()
	if f.probeDelay > 0 {
		time.Sleep(f.probeDelay)
	}
	if f.dead[ip] {
		return "", errors.New("dial timeout")
	}
	return f.nodeID[ip], nil
}

func (f *fakeGatherer) GetClusterInfo(_ context.Context, _, ip string) (*ClusterInfo, error) {
	if f.dead[ip] {
		return nil, errors.New("dial timeout")
	}
	return &ClusterInfo{State: "ok", SlotsAssigned: 16384}, nil
}

func (f *fakeGatherer) GetClusterNodes(_ context.Context, _, ip string) ([]ClusterNodeInfo, error) {
	if f.dead[ip] {
		return nil, errors.New("dial timeout")
	}
	return f.gossip, nil
}

// twoMasterOneReplicaGossip builds a CLUSTER NODES view: two masters, one replica,
// plus one ghost node (present in gossip but with no backing pod).
func twoMasterOneReplicaGossip() []ClusterNodeInfo {
	return []ClusterNodeInfo{
		{NodeID: "m1", Flags: []string{roleMaster}, MasterID: "-", Slots: []string{rangeForeign}},
		{NodeID: "m2", Flags: []string{roleMaster}, MasterID: "-", Slots: []string{"8192-16383"}},
		{NodeID: "r1", Flags: []string{flagSlave}, MasterID: "m1"},
		{NodeID: "ghost", Flags: []string{roleMaster, flagFail}, MasterID: "-"},
	}
}

const (
	ipPod0   = "10.0.0.1"
	ipPod1   = "10.0.0.2"
	ipPod2   = "10.0.0.3"
	namePod0 = "pod-0"
	namePod1 = "pod-1"
	namePod2 = "pod-2"
)

func TestGatherClusterGroundTruth_TopologyAndGhost(t *testing.T) {
	g := &fakeGatherer{
		nodeID: map[string]string{ipPod0: "m1", ipPod1: "m2", ipPod2: "r1"},
		dead:   map[string]bool{},
		gossip: twoMasterOneReplicaGossip(),
	}
	clusterPods := map[string]string{
		ipPod0: namePod0,
		ipPod1: namePod1,
		ipPod2: namePod2,
	}

	gt := GatherClusterGroundTruth(context.Background(), g, clusterPods)

	if len(gt.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(gt.Nodes))
	}
	if gt.ClusterState != "ok" {
		t.Errorf("expected ClusterState ok, got %q", gt.ClusterState)
	}
	if gt.TotalSlots != 16384 {
		t.Errorf("expected TotalSlots 16384, got %d", gt.TotalSlots)
	}

	// Roles propagated from gossip onto the backing pods.
	if got := gt.Nodes[namePod0].Role; got != roleMaster {
		t.Errorf("pod-0 role = %q, want master", got)
	}
	if got := gt.Nodes[namePod2].Role; got != roleReplica {
		t.Errorf("pod-2 role = %q, want replica", got)
	}
	if got := gt.Nodes[namePod2].MasterNodeID; got != "m1" {
		t.Errorf("pod-2 masterNodeID = %q, want m1", got)
	}
	if got := gt.Nodes[namePod0].LinkStatus; got != "up" {
		t.Errorf("pod-0 linkStatus = %q, want up", got)
	}

	// "ghost" is in gossip but has no backing pod → flagged as ghost.
	if len(gt.GhostNodes) != 1 || gt.GhostNodes[0] != "ghost" {
		t.Errorf("expected single ghost node 'ghost', got %v", gt.GhostNodes)
	}

	// All three live nodes see each other → a single partition.
	if len(gt.Partitions) != 1 {
		t.Errorf("expected 1 partition, got %d: %v", len(gt.Partitions), gt.Partitions)
	}
}

func TestGatherClusterGroundTruth_DeadIPMarkedUnreachable(t *testing.T) {
	g := &fakeGatherer{
		nodeID: map[string]string{ipPod0: "m1", ipPod1: "m2"},
		dead:   map[string]bool{"10.0.0.9": true}, // stale IP from a deleted pod
		gossip: twoMasterOneReplicaGossip(),
	}
	clusterPods := map[string]string{
		ipPod0:     namePod0,
		ipPod1:     namePod1,
		"10.0.0.9": namePod2, // cache handed us a dead IP for pod-2
	}

	gt := GatherClusterGroundTruth(context.Background(), g, clusterPods)

	if gt.Nodes[namePod2].Reachable {
		t.Errorf("pod-2 (dead IP) should be marked unreachable")
	}
	if gt.Nodes[namePod2].NodeID != "" {
		t.Errorf("pod-2 (dead IP) should have no NodeID, got %q", gt.Nodes[namePod2].NodeID)
	}
	if !gt.Nodes[namePod0].Reachable || !gt.Nodes[namePod1].Reachable {
		t.Errorf("live pods should be reachable")
	}
}

func TestGatherClusterGroundTruth_ProbesRunConcurrently(t *testing.T) {
	const n = 6
	const delay = 120 * time.Millisecond

	nodeID := make(map[string]string, n)
	clusterPods := make(map[string]string, n)
	for i := range n {
		ip := "10.0.0." + string(rune('1'+i))
		nodeID[ip] = "id" + string(rune('1'+i))
		clusterPods[ip] = "pod-" + string(rune('0'+i))
	}
	g := &fakeGatherer{nodeID: nodeID, dead: map[string]bool{}, probeDelay: delay, gossip: nil}

	start := time.Now()
	GatherClusterGroundTruth(context.Background(), g, clusterPods)
	elapsed := time.Since(start)

	// Serial would be >= n*delay (720ms). Parallel should be a small multiple of a
	// single delay. Allow generous headroom to stay non-flaky on loaded CI.
	if elapsed >= n*delay {
		t.Fatalf("gather appears serial: elapsed %v >= serial bound %v", elapsed, n*delay)
	}
	if g.maxFlight < 2 {
		t.Fatalf("expected concurrent probes, max in-flight was %d", g.maxFlight)
	}
}

// TestGatherReplicationState_ProbesRunConcurrently is the sentinel-mode analogue of the
// cluster-mode concurrency guard above. It is a red-first regression test for the
// leaderless-recovery stall observed on a managed cloud (op-logs: a single reconcile
// blocked ~146s dialing freshly-killed Sentinel/Redis pod IPs that blackholed —
// i/o timeout / no route to host — one after another). The stall froze status at
// stale bootstrap values, so Rule L never ran inside the test window.
//
// The defect is that GatherReplicationState probes every Redis pod and then every
// Sentinel pod sequentially, so N unreachable endpoints cost N × (dial timeout ×
// retries) instead of ~one. A blocking fakeGatherer makes that serial cost
// observable: against the current sequential implementation this test is RED
// (elapsed ≈ total-pods × delay, max in-flight = 1); a concurrent gather turns it
// green. Locally/on kubeadm the real bug hides because dead IPs RST fast (~0 delay);
// the delay here stands in for the cloud's blackhole timeout.
func TestGatherReplicationState_ProbesRunConcurrently(t *testing.T) {
	const delay = 120 * time.Millisecond

	redisPods := map[string]string{
		ipPod0: "redis-0",
		ipPod1: "redis-1",
		ipPod2: "redis-2",
	}
	sentinelPods := map[string]string{
		"10.0.1.1": "sentinel-0",
		"10.0.1.2": "sentinel-1",
		"10.0.1.3": "sentinel-2",
	}
	totalPods := len(redisPods) + len(sentinelPods)

	// Every probe is slow (stands in for a blackholing dead IP); none is marked dead
	// so the timing, not an early error, is what the assertion turns on.
	g := &fakeGatherer{nodeID: map[string]string{}, dead: map[string]bool{}, probeDelay: delay}

	start := time.Now()
	GatherReplicationState(context.Background(), g, redisPods, sentinelPods, "ns.inst")
	elapsed := time.Since(start)

	// Serial would be >= totalPods*delay (720ms). Concurrent should be a small
	// multiple of a single delay. Generous headroom keeps this non-flaky on loaded CI.
	if elapsed >= time.Duration(totalPods)*delay {
		t.Fatalf("sentinel gather appears serial: elapsed %v >= serial bound %v",
			elapsed, time.Duration(totalPods)*delay)
	}
	if g.maxFlight < 2 {
		t.Fatalf("expected concurrent probes, max in-flight was %d", g.maxFlight)
	}
}
