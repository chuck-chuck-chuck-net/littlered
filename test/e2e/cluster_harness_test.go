//go:build e2e
// +build e2e

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

package e2e

// cluster_harness_test.go is the DE-DUP LEVER for the cluster e2e suite. The topology-agnostic
// cluster behaviours (bring-up to healthy, basic ops, master-pod-failure recovery) are the SAME
// Redis Cluster behaviours whether the cluster lives in one Kubernetes cluster or is stretched
// across several sites — the only thing that differs is WHICH kube-context a given pod lives in.
//
// clusterHarness captures exactly the pod-level operations the shared behaviour specs
// (clusterBehaviourSpecs, cluster_behaviour_test.go) need, at the "pod-level ops any cluster
// supports" altitude:
//
//   - CR lifecycle (apply a redundant cluster CR / wait Running / delete),
//   - a pods-with-PER-POD-CONTEXT inventory (podHandle carries its own kube-context),
//   - exec-redis / delete-pod routed to the right context,
//   - ground-truth master resolution by slot (topology-agnostic CLUSTER NODES parse),
//   - a topology-appropriate healthy-topology assertion, and
//   - a chaos client wired to reach the instance's pods.
//
// Two implementations:
//   - singleSiteHarness — today's ambient-context helpers (one Kubernetes cluster, one namespace).
//   - multiSiteHarness  — built on the M4b registry (multisite_utils_test.go): hub + per-site
//     contexts, per-site namespace/RBAC/Secret provisioning, per-pod site context.
//
// The disruption helpers that are INHERENTLY multi-site (hypervisor site kill, Cilium partition) stay in
// multisite_takeover_test.go and are deliberately NOT part of this interface — the shared body
// only exercises pod-level failures every cluster supports.

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/chuck-chuck-chuck-net/littlered/test/chaos"
)

// --- Topology gating ---------------------------------------------------------------------

// topologyEnabled reports whether the given topology ("single"|"multi") should register its
// behaviour specs, per E2E_TOPOLOGY (default "single" — the historical behaviour of the suite).
//
//	E2E_TOPOLOGY=single   only single-site (default)
//	E2E_TOPOLOGY=multi    only multi-site
//	E2E_TOPOLOGY=both     both topologies
func topologyEnabled(topo string) bool {
	v := os.Getenv("E2E_TOPOLOGY")
	if v == "" {
		v = "single"
	}
	return v == "both" || v == topo
}

// --- Pod handle --------------------------------------------------------------------------

// podHandle identifies one Redis pod AND the kube-context to reach it through. In multi-site the
// Kubernetes object name ({cr}-shard-K-O) is only unique within a site (the same name exists in
// every hosting site as a different Redis node — site_placement.go), so a bare name is never a
// node identity: a handle carries its site context too.
type podHandle struct {
	ctx  string // kube-context; "" = ambient (single-site)
	name string // Kubernetes pod object name
	site string // logical site name; "" for single-site (display only)
}

func (p podHandle) String() string {
	if p.site == "" {
		return p.name
	}
	return p.site + "/" + p.name
}

// chaosDeployment is a deployed chaos client, remembering the context+namespace it lives in.
type chaosDeployment struct {
	ctx string
	ns  string
	pod string
}

// --- Harness interface -------------------------------------------------------------------

// clusterHarness abstracts the pod-level operations of a Redis Cluster instance so ONE shared
// behaviour spec body runs under both single-site and multi-site topologies.
type clusterHarness interface {
	// topology is "single" or "multi" — used in spec descriptions and logs.
	topology() string
	// instanceNS is the namespace the CR and its pods live in.
	instanceNS() string

	// applyRedundantCR renders + applies a cluster CR sized so every shard has cross-node
	// redundancy for this topology (single: 1 replica/shard; multi: sitesPerShard=2, so each
	// shard's cross-site peer IS the redundancy) and returns any apply error.
	applyRedundantCR(crName string) error
	// waitRunning blocks until the CR reports phase Running.
	waitRunning(crName string, timeout time.Duration)
	// deleteCR deletes the CR (and, multi-site, sweeps the spokes).
	deleteCR(crName string)

	// expectedMasters / expectedReplicas / expectedTotal describe the settled topology shape
	// for the redundant CR (both default topologies land on 3 masters + 3 replicas = 6 nodes).
	expectedMasters() int
	expectedReplicas() int
	expectedTotal() int

	// queryPod returns a reachable pod to run cluster-wide reads (CLUSTER INFO/NODES) against.
	queryPod(crName string) podHandle
	// resolveMasterForSlot finds the CURRENT master serving the given slot and returns its
	// handle (with the right context) — the ground-truth victim selector.
	resolveMasterForSlot(crName string, slot int) (podHandle, error)

	// execRedis runs redis-cli <args> in the pod's redis container, in the pod's context.
	execRedis(h podHandle, args ...string) (string, error)
	// deletePod deletes the pod (graceful or force) in the pod's context.
	deletePod(h podHandle, graceful bool) error

	// verifyHealthy asserts a topology-appropriate healthy end state (single: operator-status ⇄
	// Redis topology sync incl. shard colocation; multi: cross-site invariant + all slots served).
	verifyHealthy(crName string)

	// deployChaos launches a chaos client seeded to reach the instance's pods, for `duration`.
	deployChaos(crName, name string, duration time.Duration) (chaosDeployment, error)
	// waitChaos / chaosMetrics / deleteChaos operate on a chaosDeployment in its own context.
	waitChaos(d chaosDeployment, timeout time.Duration) error
	chaosMetrics(d chaosDeployment) (*chaos.MetricsSnapshot, error)
	deleteChaos(d chaosDeployment)
}

// --- Slot → shard helpers (topology-agnostic) --------------------------------------------

// slotForShard returns a representative slot in shard K's range under the operator's even
// contiguous split of 16384 slots across clusterShards shards. Used to name a victim shard's
// master independent of topology.
func slotForShard(shard int) int {
	per := 16384 / clusterShards
	return shard*per + per/2
}

// masterIPForSlot parses CLUSTER NODES output and returns the IP of the master serving the given
// slot (empty if unassigned). Topology-agnostic: CLUSTER NODES is a cluster-wide view.
func masterIPForSlot(nodesOutput string, slot int) string {
	for _, line := range strings.Split(strings.TrimSpace(nodesOutput), "\n") {
		f := strings.Fields(line)
		if len(f) < 9 {
			continue
		}
		flags := f[2]
		if !strings.Contains(flags, "master") || strings.Contains(flags, "fail") {
			continue
		}
		for _, sp := range f[8:] {
			var start, end int
			if strings.Contains(sp, "-") {
				if strings.HasPrefix(sp, "[") { // importing/migrating marker — not owned
					continue
				}
				_, _ = fmt.Sscanf(sp, "%d-%d", &start, &end)
			} else {
				_, _ = fmt.Sscanf(sp, "%d", &start)
				end = start
			}
			if slot >= start && slot <= end {
				hostport := strings.Split(f[1], "@")[0]
				return strings.Split(hostport, ":")[0]
			}
		}
	}
	return ""
}
