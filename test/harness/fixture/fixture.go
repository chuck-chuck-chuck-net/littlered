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

// Package fixture holds the named multi-site topology fixtures of
// docs/MULTI_SITE_E2E_TEST_MATRIX.md §3 as DATA, plus the arithmetic derived
// from them.
//
// Why this is a seam and not four env vars in a work list: a multi-site work
// item that stands up the WRONG topology is not a failing test — it is a
// passing test that measured something else. `sitesPerShard: 1` is still a
// valid CR; `shards: 3` over two sites is still healthy. Both silently stop
// being the cell anybody meant. So the topology is named once, the arithmetic
// that makes it meaningful (pods, the initial master split, the quorum, and
// LR-031's two thresholds) is derived rather than restated, and the whole thing
// is validated at LAUNCH time — before an overnight array spends an hour
// converging the wrong cluster.
//
// The fixtures are deliberately the whole §3 table, not only the one M3 runs:
// the table is what the unit test checks the arithmetic against, and later
// milestones (M3.5 onward) select the rest by name with no code change.
package fixture

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Frugal sizing (matrix §3): "sized frugally (100m CPU / 128Mi per redis) so
// several non-destructive fixtures coexist". Cheap to repeat is the property
// that makes an array of these economic at all — one fixture at the operator's
// defaults would dominate the lab's capacity.
const (
	FrugalCPU    = "100m"
	FrugalMemory = "128Mi"
)

// The matrix §3 fixture names, as constants: they are referenced from the
// registry, from work lists and from tests, and a typo in any of those places
// is a topology that silently is not the cell anybody meant.
const (
	F2K4  = "F2-K4"
	F2K4R = "F2-K4R"
	F2K3  = "F2-K3"
	F2K6  = "F2-K6"
	F3K3  = "F3-K3"
	F3K4  = "F3-K4"
	F3K6  = "F3-K6"
	F3K3M = "F3-K3-M"
)

// Fixture is one named topology from matrix §3.
//
// Sites is how many workload Kubernetes clusters it needs, NOT which ones: the
// mapping to actual lab clusters belongs to the lab inventory (package lab), so
// the same fixture runs on any pair of sites.
type Fixture struct {
	// Name is the matrix §3 identifier ("F2-K4").
	Name string
	// Sites is the number of workload sites the fixture spans (S).
	Sites int
	// Shards is the Redis Cluster shard count (K).
	Shards int
	// SitesPerShard is spec.multiSite.sitesPerShard — the copy factor. Required
	// and never defaulted since LR-033.
	SitesPerShard int
	// ReplicasPerShard is spec.cluster.replicasPerShard: the INTRA-site replicas.
	// Zero still yields cross-site replicas (pillar 3.15) — a reading trap the
	// Pods() arithmetic below exists to keep out of the work list.
	ReplicasPerShard int
	// CPU / Memory size one redis container.
	CPU, Memory string
	// Why is the matrix's own one-line reason the fixture exists.
	Why string
}

// registry is the matrix §3 table, verbatim. Ordered arbitrarily; Names sorts.
var registry = map[string]Fixture{
	F2K4: {
		Name: F2K4, Sites: 2, Shards: 4, SitesPerShard: 2, ReplicasPerShard: 0,
		Why: "the recommended dual-site: an even 2+2 split, so a loss in either " +
			"direction is fence-safe and auto-takes-over. The primary fixture.",
	},
	F2K4R: {
		Name: F2K4R, Sites: 2, Shards: 4, SitesPerShard: 2, ReplicasPerShard: 1,
		Why: "F2-K4 plus an intra-site replica per shard — proves native intra-site " +
			"failover is preferred over the cross-site one.",
	},
	F2K3: {
		Name: F2K3, Sites: 2, Shards: 3, SitesPerShard: 2, ReplicasPerShard: 0,
		Why: "the asymmetric \"1+2\" people will actually run, and the ONLY " +
			"ReducedSiteLossResilience regime (two sites, odd K).",
	},
	F2K6: {
		Name: F2K6, Sites: 2, Shards: 6, SitesPerShard: 2, ReplicasPerShard: 0,
		Why: "multi-orphan takeover: three concurrent no-consensus epoch bumps.",
	},
	F3K3: {
		Name: F3K3, Sites: 3, Shards: 3, SitesPerShard: 2, ReplicasPerShard: 0,
		Why: "Tier C reference: any single-site loss is native, planSiteTakeover " +
			"should barely be reached.",
	},
	F3K4: {
		Name: F3K4, Sites: 3, Shards: 4, SitesPerShard: 2, ReplicasPerShard: 0,
		Why: "the former \"headline trap\", superseded by LR-031: 2+1+1 sits AT the " +
			"takeover floor, so every single-site loss auto-survives.",
	},
	F3K6: {
		Name: F3K6, Sites: 3, Shards: 6, SitesPerShard: 2, ReplicasPerShard: 0,
		Why: "the tightest balance that reaches the NATIVE target (S x target = K).",
	},
	F3K3M: {
		Name: F3K3M, Sites: 3, Shards: 3, SitesPerShard: 3, ReplicasPerShard: 0,
		Why: "full mesh (sitesPerShard == S): a two-site loss still leaves a replica.",
	},
}

// Pods is the total number of redis pods the fixture stands up:
// K x sitesPerShard x (replicasPerShard + 1) — every hosting site runs a shard
// StatefulSet of `replicasPerShard + 1` pods (pillar 3.15). Stated as
// arithmetic, never as a literal, because the reading trap it guards
// ("replicasPerShard: 1 dual-site = 4 members, not 2") is exactly the kind of
// thing a hand-written table gets wrong once and then nobody re-derives.
func (f Fixture) Pods() int {
	return f.Shards * f.SitesPerShard * (f.ReplicasPerShard + 1)
}

// MasterSplit is the INITIAL slot-serving-master count per site, in site order.
// Shard K's initial master lives in site `K mod S` (matrix §3, the round-robin
// placement of site_placement.go).
//
// Initial, and only initial: roles are fluid in multi-site (pillar 3.15), so
// this is what bringup produces, not an invariant. It is what a bringup cell
// asserts and what the rebalancer's drift is measured against.
func (f Fixture) MasterSplit() []int {
	if f.Sites <= 0 {
		return nil
	}
	split := make([]int, f.Sites)
	for k := range f.Shards {
		split[k%f.Sites]++
	}
	return split
}

// Quorum is Redis Cluster's master majority, (K/2)+1 — the unconditional
// minority check in clusterUpdateState() (ADR-012 D1, verified in Redis 8.4
// cluster_legacy.c). A side holding fewer fail-stops itself.
func (f Fixture) Quorum() int { return f.Shards/2 + 1 }

// TakeoverSurvivalFloor is ADR-012 D2's HARD floor, floor(K/2) (LR-031): the
// most masters a site may hold and still have its loss recoverable unattended.
// At or below it the survivors keep a blocking minority, so planSiteTakeover's
// fence holds; above it, losing that site hits the drift-refusal.
func (f Fixture) TakeoverSurvivalFloor() int { return f.Shards / 2 }

// NativeSurvivalTarget is ADR-012 D2's STANDARD, ceil(K/2)-1 (LR-031): at or
// below it the survivors keep quorum and Redis elects on its own, with the hub
// uninvolved. Breaching it only downgrades recovery to an operator takeover.
//
// The two coincide for odd K and differ by exactly one for even K. That gap is
// the whole LR-031 correction, so both are exposed rather than one being
// derived from the other at the call site.
func (f Fixture) NativeSurvivalTarget() int { return (f.Shards+1)/2 - 1 }

// Validate reports whether the fixture is a coherent multi-site topology. Run
// at launch: an incoherent fixture is a work list typo, and the cheapest place
// to catch it is before any cluster is touched.
func (f Fixture) Validate() error {
	if f.Sites < 2 {
		return fmt.Errorf("fixture %q: sites = %d, but a stretched cluster needs at least 2", f.Name, f.Sites)
	}
	if f.Shards < 3 {
		return fmt.Errorf("fixture %q: shards = %d, but Redis Cluster needs at least 3 masters "+
			"(and the e2e suite refuses CLUSTER_SHARDS < 3)", f.Name, f.Shards)
	}
	if f.SitesPerShard < 2 {
		return fmt.Errorf("fixture %q: sitesPerShard = %d, but a shard must span at least 2 sites "+
			"or losing a site loses data", f.Name, f.SitesPerShard)
	}
	if f.SitesPerShard > f.Sites {
		return fmt.Errorf("fixture %q: sitesPerShard = %d exceeds sites = %d — there is no "+
			"placement for it", f.Name, f.SitesPerShard, f.Sites)
	}
	if f.ReplicasPerShard < 0 {
		return fmt.Errorf("fixture %q: replicasPerShard = %d must not be negative", f.Name, f.ReplicasPerShard)
	}
	return nil
}

// Env is the environment the e2e suite needs to realise this fixture. These are
// the suite's OWN knob names; getting one wrong is invisible at runtime,
// because the suite simply falls back to its default and stands up a different
// topology that still passes. TestEnvCarriesTheWholeTopology pins the set.
func (f Fixture) Env() map[string]string {
	return map[string]string{
		"E2E_TOPOLOGY":          "multi",
		"CLUSTER_SHARDS":        strconv.Itoa(f.Shards),
		"MS_SITES_PER_SHARD":    strconv.Itoa(f.SitesPerShard),
		"MS_REPLICAS_PER_SHARD": strconv.Itoa(f.ReplicasPerShard),
		"MS_REDIS_CPU":          f.CPU,
		"MS_REDIS_MEMORY":       f.Memory,
	}
}

// Lookup resolves a fixture by name. The error lists every known name: a
// refusal that does not say what IS valid only moves the dead end.
func Lookup(name string) (Fixture, error) {
	f, ok := registry[strings.TrimSpace(name)]
	if !ok {
		return Fixture{}, fmt.Errorf("unknown topology fixture %q; matrix §3 defines: %s",
			name, strings.Join(Names(), ", "))
	}
	return f, nil
}

// Names lists every known fixture name, sorted (it is user-facing output).
func Names() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Every shipped fixture is frugal. Set here rather than repeated in each table
// entry so the sizing is one decision, and so a new fixture cannot forget it.
func init() {
	for name, f := range registry {
		f.CPU, f.Memory = FrugalCPU, FrugalMemory
		registry[name] = f
	}
}
