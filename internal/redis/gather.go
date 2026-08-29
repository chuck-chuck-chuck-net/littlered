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
	"sync"
)

// Gatherer is an interface for collecting state from Redis and Sentinel nodes.
// This allows the Operator (direct TCP) and the CLI (K8s Exec) to share logic.
type Gatherer interface {
	GetRedisState(ctx context.Context, podName, ip string) (*RedisNodeState, error)
	// GetSentinelState probes one Sentinel pod for masterName. The name is a
	// PARAMETER rather than implementation state on purpose (LR-041): as
	// construction state it was silently omissible, and an unset required string
	// zero-values to "" — which Sentinel answers exactly like an unknown name, so
	// every sentinel read as reachable-but-bare and every Monitoring-gated rule
	// went quietly dead. As a parameter the compiler asks for it at every call site.
	GetSentinelState(ctx context.Context, podName, ip, masterName string) (*SentinelNodeState, error)

	// Cluster mode
	GetClusterID(ctx context.Context, podName, ip string) (string, error)
	GetClusterInfo(ctx context.Context, podName, ip string) (*ClusterInfo, error)
	GetClusterNodes(ctx context.Context, podName, ip string) ([]ClusterNodeInfo, error)
}

// GatherReplicationState uses a Gatherer to populate a ReplicationState.
//
// Every Redis and Sentinel pod is probed concurrently: a single unreachable IP —
// e.g. a stale pod IP the K8s cache hands us during pod churn — must not
// serialize-block the whole gather behind its dial timeout, or the reconcile loop
// cannot heal fast enough. This is the sentinel-mode analogue of the cluster-mode
// concurrent gather (see gatherNodeIdentities / LR-012); it was previously a plain
// sequential loop, and the same blackhole-dial stall then bit sentinel mode on a
// managed cloud. See the cross-mode-parity rule in CLAUDE.md.
// masterName is the instance's Sentinel master name (LittleRed.SentinelMasterName()).
// It is required whenever sentinelPods is non-empty; callers with no Sentinels
// (failover mode) pass "" and no Sentinel probe is issued.
//
// ownedIPs is EVERY address this instance's pods hold, TERMINATING INCLUDED, and it
// is a parameter rather than a set the gather could derive (LR-053). It cannot be
// derived here: redisPods/sentinelPods arrive already filtered to the live topology
// — a terminating pod of ours never reaches this function — which is exactly why
// `ValidIPs` could only ever mean "live topology" and the attribution question had
// no set of its own to read. It is in the signature for LR-041's reason: a required
// value held as construction state has no enforcement, and the compiler now asks
// every caller.
//
// Its zero value fails SAFE by construction: the probed addresses are unioned in, so
// a nil ownedIPs degrades to exactly the pre-split behaviour (OwnedIPs ==
// LiveTopologyIPs) rather than to "nothing is ours", which would make every
// monitored address read as foreign and manufacture capture verdicts everywhere.
func GatherReplicationState(
	ctx context.Context, g Gatherer, redisPods, sentinelPods map[string]string,
	masterName string, ownedIPs map[string]bool,
) *ReplicationState {
	state := NewReplicationState()

	// Recorded first, so the union below can only ever widen: every probed address is
	// also an owned one (the LiveTopologyIPs subset OwnedIPs invariant), and any
	// address of ours the caller knows about but did not hand us to probe — a
	// terminating pod — is owned and nothing more.
	for ip := range ownedIPs {
		state.AddOwnedIP(ip)
	}

	type redisResult struct {
		ip string
		rs *RedisNodeState
	}
	type sentinelResult struct {
		ip string
		ss *SentinelNodeState
	}

	redisResults := make([]redisResult, 0, len(redisPods))
	sentinelResults := make([]sentinelResult, 0, len(sentinelPods))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for ip, name := range redisPods {
		state.AddLiveTopologyIP(ip)
		wg.Add(1)
		go func(ip, name string) {
			defer wg.Done()
			rs, err := g.GetRedisState(ctx, name, ip)
			if err != nil {
				// The error is CLASSIFIED, not discarded (LR-051). Dropping it made a
				// credential mismatch byte-identical to a dial timeout, and because
				// DataHolders() filters on Reachable, an AuthFailed pod then read as
				// "holds no data" while it could be holding the only copy.
				rs = &RedisNodeState{
					PodName: name, IP: ip, Reachable: false,
					ProbeFailure: ClassifyProbeError(err), ProbeError: DescribeProbeError(err),
				}
			}
			mu.Lock()
			redisResults = append(redisResults, redisResult{ip: ip, rs: rs})
			mu.Unlock()
		}(ip, name)
	}

	for ip, name := range sentinelPods {
		state.AddLiveTopologyIP(ip)
		wg.Add(1)
		go func(ip, name string) {
			defer wg.Done()
			ss, err := g.GetSentinelState(ctx, name, ip, masterName)
			if err != nil {
				ss = &SentinelNodeState{
					PodName: name, IP: ip, Reachable: false,
					ProbeFailure: ClassifyProbeError(err), ProbeError: DescribeProbeError(err),
				}
			}
			mu.Lock()
			sentinelResults = append(sentinelResults, sentinelResult{ip: ip, ss: ss})
			mu.Unlock()
		}(ip, name)
	}

	wg.Wait()

	// Assemble the maps single-threaded after the barrier (maps are not concurrency-safe).
	for _, r := range redisResults {
		state.RedisNodes[r.ip] = r.rs
	}
	for _, r := range sentinelResults {
		state.SentinelNodes[r.ip] = r.ss
	}

	state.DetermineRealMaster()
	return state
}

// GatherClusterGroundTruth queries all cluster pods to build a view of the cluster topology.
//
// Probes are issued concurrently (see gatherNodeIdentities / gatherTopology): a single
// unreachable IP — e.g. a stale pod IP handed to us by the K8s cache during pod churn —
// must not serialize-block the whole gather, or the reconcile loop cannot heal fast
// enough (LR-012).
func GatherClusterGroundTruth(ctx context.Context, g Gatherer, clusterPods map[string]string) *ClusterGroundTruth {
	gt := NewClusterGroundTruth()

	// 1. Identify every pod (CLUSTER MYID + replication state) and index live nodes.
	nodeIDtoPod := make(map[string]string)
	for _, ns := range gatherNodeIdentities(ctx, g, clusterPods) {
		gt.Nodes[ns.PodName] = ns
		if ns.Reachable {
			nodeIDtoPod[ns.NodeID] = ns.PodName
		}
	}

	// 2. Query topology (CLUSTER INFO + NODES) from all reachable nodes and merge.
	adj := gatherTopology(ctx, g, gt)
	// Retain the per-node adjacency (who each node directly knows) so repair can
	// gate CLUSTER REPLICATE on the empty master actually knowing its target.
	gt.KnownNodes = adj

	// Detect ghosts: NodeIDs seen in the mesh that have no backing pod.
	for nodeID := range gt.AllNodeIDs {
		if _, hasPod := nodeIDtoPod[nodeID]; !hasPod {
			gt.GhostNodes = append(gt.GhostNodes, nodeID)
		}
	}

	if gt.ClusterState == "" {
		gt.ClusterState = "unknown"
	}

	// 3. Compute partitions over the live nodes' adjacency graph.
	gt.Partitions = computePartitions(nodeIDtoPod, adj)

	return gt
}

// gatherNodeIdentities probes every pod concurrently for its CLUSTER MYID and
// replication LinkStatus. Each goroutine writes its own pre-allocated slot, so no
// locking is needed; unreachable pods come back with Reachable=false.
func gatherNodeIdentities(ctx context.Context, g Gatherer, clusterPods map[string]string) []*ClusterNodeState {
	type podProbe struct{ ip, name string }
	probes := make([]podProbe, 0, len(clusterPods))
	for ip, name := range clusterPods {
		probes = append(probes, podProbe{ip: ip, name: name})
	}

	states := make([]*ClusterNodeState, len(probes))
	var wg sync.WaitGroup
	for i := range probes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := probes[i]
			id, err := g.GetClusterID(ctx, p.name, p.ip)
			if err != nil {
				// Classified, not discarded — the cross-mode half of LR-051 (rule §7.11).
				// No cluster-mode decision keys on it today: the wipe recovery already
				// takes its data-safety signal from the kubelet (LR-023), not from this
				// dial. It is carried so the "operator cannot authenticate" report is
				// mode-complete, and so the next rule that reaches for this state finds
				// the reason already on it rather than having to rediscover the defect.
				states[i] = &ClusterNodeState{
					PodName: p.name, PodIP: p.ip, Reachable: false,
					ProbeFailure: ClassifyProbeError(err), ProbeError: DescribeProbeError(err),
				}
				return
			}
			ns := &ClusterNodeState{PodName: p.name, PodIP: p.ip, NodeID: id, Reachable: true}
			// Replication state is only queried for reachable nodes, so it never hits a dead IP.
			if rs, err := g.GetRedisState(ctx, p.name, p.ip); err == nil {
				ns.LinkStatus = rs.LinkStatus
			}
			states[i] = ns
		}(i)
	}
	wg.Wait()
	return states
}

// topoView is one reachable node's contribution to the merged topology.
type topoView struct {
	state         string
	haveState     bool
	slotsAssigned int
	seenIDs       []string // every NodeID seen in CLUSTER NODES (for ghost detection)
	known         []string // non-failed neighbor IDs
	hasNodes      bool     // CLUSTER NODES succeeded → record adjacency
	asm           bool     // node's CLUSTER INFO reported cluster_slot_migration_* (ASM support)
}

// gatherTopology queries CLUSTER INFO + NODES from all reachable nodes concurrently,
// mutates each node's own role/slots, and merges the shared fields (ClusterState,
// TotalSlots, AllNodeIDs) single-threaded after the barrier. Returns the adjacency map.
func gatherTopology(ctx context.Context, g Gatherer, gt *ClusterGroundTruth) map[string][]string {
	reachable := make([]*ClusterNodeState, 0, len(gt.Nodes))
	for _, n := range gt.Nodes {
		if n.Reachable {
			reachable = append(reachable, n)
		}
	}

	views := make([]*topoView, len(reachable))
	var wg sync.WaitGroup
	for i := range reachable {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			views[i] = probeNodeTopology(ctx, g, reachable[i])
		}(i)
	}
	wg.Wait()

	// ASM capability is the AND over all reachable nodes: use native atomic slot
	// migration only if every reachable node reported support (a mixed-version
	// cluster mid rolling-upgrade falls back to the baseline dance). An empty mesh
	// or any node lacking support ⇒ false ⇒ dance. See LR-018 §7.3.
	asmAll := len(reachable) > 0

	adj := make(map[string][]string)
	for i, v := range views {
		if v == nil {
			asmAll = false
			continue
		}
		if !v.haveState || !v.asm {
			asmAll = false
		}
		if v.haveState {
			if v.state == "ok" {
				gt.ClusterState = "ok"
			} else if gt.ClusterState == "" || gt.ClusterState == "unknown" {
				gt.ClusterState = v.state
			}
			if v.slotsAssigned > gt.TotalSlots {
				gt.TotalSlots = v.slotsAssigned
			}
		}
		for _, id := range v.seenIDs {
			gt.AllNodeIDs[id] = true
		}
		if v.hasNodes {
			adj[reachable[i].NodeID] = v.known
		}
	}
	gt.AtomicSlotMigration = asmAll
	return adj
}

// probeNodeTopology queries one node's CLUSTER INFO + NODES and folds its own
// role/slots in place. It mutates only the node it was given, so it is safe to run
// concurrently across distinct nodes.
func probeNodeTopology(ctx context.Context, g Gatherer, n *ClusterNodeState) *topoView {
	v := &topoView{}

	if info, err := g.GetClusterInfo(ctx, n.PodName, n.PodIP); err == nil {
		v.haveState = true
		v.state = info.State
		v.slotsAssigned = info.SlotsAssigned
		v.asm = info.AtomicSlotMigration
	}

	nodes, err := g.GetClusterNodes(ctx, n.PodName, n.PodIP)
	if err != nil {
		return v
	}
	v.hasNodes = true

	for _, knownNode := range nodes {
		v.seenIDs = append(v.seenIDs, knownNode.NodeID)

		if !nodeFlagsFailed(knownNode.Flags) {
			v.known = append(v.known, knownNode.NodeID)
		}

		if knownNode.NodeID == n.NodeID {
			n.Slots = knownNode.Slots
			n.Role = roleMaster
			if knownNode.IsReplica() {
				n.Role = roleReplica
			}
			n.MasterNodeID = knownNode.MasterID
		}
	}
	return v
}

// computePartitions groups live nodes into connected components over the adjacency
// graph built from each node's CLUSTER NODES view.
func computePartitions(nodeIDtoPod map[string]string, adj map[string][]string) [][]string {
	// At most one partition per node (fully disconnected graph).
	partitions := make([][]string, 0, len(nodeIDtoPod))
	visited := make(map[string]bool)
	for id := range nodeIDtoPod {
		if visited[id] {
			continue
		}

		var partition []string
		queue := []string{id}
		visited[id] = true

		for len(queue) > 0 {
			curr := queue[0]
			queue = queue[1:]
			partition = append(partition, curr)

			for _, neighbor := range adj[curr] {
				if _, valid := nodeIDtoPod[neighbor]; valid && !visited[neighbor] {
					visited[neighbor] = true
					queue = append(queue, neighbor)
				}
			}
		}
		partitions = append(partitions, partition)
	}
	return partitions
}
