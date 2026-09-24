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

package multisite

import (
	"fmt"
	"strings"

	"github.com/chuck-chuck-chuck-net/littlered/internal/mesh"
)

// Deriving spec.multiSite from the lab inventory, and why a generator earns its place here
// rather than being a convenience.
//
// The split is slaptain's (docs/MULTI-SITE.md): its mesh-derive-topology.sh generates the
// topology from lab.yaml and leaves the directory — "choices, not facts about the lab" — to
// the human. The same line falls here between which clusters exist, in what order (FACTS,
// derived below) and how many shards with how many copies (CHOICES, which the caller states).
//
// It matters more here than there, because three decisions make a mistake in this block
// unrecoverable rather than merely wrong:
//
//   - spec.multiSite.sites is listType=atomic and a site's POSITION is its placement index —
//     shard K lives in sites (K+j) mod S (LR-033). Reordering re-places every shard.
//   - the topology is IMMUTABLE after create (LR-032), enforced by CEL transition rules.
//   - sitesPerShard is required and never defaulted (LR-033), because it is the copy factor
//     of a RAM-resident store: every candidate default is wrong for somebody.
//
// So the remedy for a hand-written mistake is delete-and-recreate, which under EmptyDir
// (pillar 3.1) is the data. This generator removes the class.

// TopologyChoices are the values no inventory can supply, because they are decisions about
// the deployment rather than facts about the lab.
type TopologyChoices struct {
	// SitesPerShard is the copy factor: each shard lives in this many sites, so a shard has
	// SitesPerShard*(ReplicasPerShard+1) members. Required, and deliberately never guessed —
	// the CRD refuses to default it for the same reason (LR-033), and a generator that
	// invented one would reintroduce exactly what that entry removed.
	SitesPerShard int
	// Shards is needed for the resilience arithmetic even though it lives outside
	// spec.multiSite. Without it the two ADR-012 D2 thresholds cannot be computed, and those
	// are the half of this report a human cannot do in their head.
	Shards int
}

// TopologySite is one entry of the generated spec.multiSite.sites list. Order is significant.
type TopologySite struct {
	Name string
	// KubeconfigSecretName is empty for a HUB-LOCAL site: the operator reaches its own
	// cluster through its manager's client, and the ABSENCE of kubeconfigSecretRef is what
	// marks a site hub-local (hubSiteClient). Emitting one there would be wrong, not merely
	// redundant.
	KubeconfigSecretName string
}

// ShardPlacement records where one shard's members land under the round-robin rule. Derived
// and printed because it is the one property of the CR a reader cannot check by eye, and the
// one that a reordered site list silently changes.
type ShardPlacement struct {
	Shard int
	Sites []string
}

// Resilience is the ADR-012 D2 reading of the topology. The two thresholds coincide for odd
// shard counts and differ by exactly one for even ones — the off-by-one LR-031 corrected,
// which had left a K=4 dual-site topology permanently drifted and unrecoverable.
type Resilience struct {
	Shards int
	Sites  int
	// NativeSurvivalTarget: at or below this, the SURVIVORS keep quorum and Redis elects on
	// its own with the operator uninvolved.
	NativeSurvivalTarget int
	// TakeoverSurvivalFloor: at or below this, the LOST side lacks quorum, so the survivors
	// hold a blocking minority and planSiteTakeover's fence holds. Breaching this is what
	// makes a site's loss unrecoverable unattended.
	TakeoverSurvivalFloor int
	// SiteLossResilient is whether the floor is STRUCTURALLY reachable at all. False is the
	// state the operator reports as ReducedSiteLossResilience.
	SiteLossResilient bool
	Summary           string
}

// Topology is the generated spec.multiSite plus the two things worth saying about it.
type Topology struct {
	Sites         []TopologySite
	SitesPerShard int
	Placement     []ShardPlacement
	Resilience    Resilience
}

// SitesForShard mirrors the operator's sitesForShard (site_placement.go): shard K lives in
// sites (K+j) mod S for j < sitesPerShard, and the FIRST is the shard's primary, whose
// initial master lives there.
//
// It CLAMPS sitesPerShard to numSites rather than refusing, exactly as the operator does.
// That branch is unreachable from DeriveTopology, which refuses the over-request outright
// with a better message — but the mirror's job is that the two DEFINITIONS agree, not that
// the caller never reaches the difference. The drift guard caught this on its first run,
// against a first version of this function that returned nil there.
//
// A mirror rather than an import because cmd/lrctl does not import internal/controller; the
// drift guard sits with the threshold mirrors' in internal/controller/site_rebalance_test.go,
// which is what keeps this from becoming a second, quietly-diverging definition (LR-045).
func SitesForShard(shardIdx, numSites, sitesPerShard int) []int {
	if numSites <= 0 || sitesPerShard <= 0 {
		return nil
	}
	if sitesPerShard > numSites {
		sitesPerShard = numSites
	}
	out := make([]int, 0, sitesPerShard)
	for j := 0; j < sitesPerShard; j++ {
		out = append(out, (shardIdx+j)%numSites)
	}
	return out
}

// DeriveTopology builds spec.multiSite from the inventory's workload sites, in inventory
// order, and reports what that topology survives. It refuses rather than guesses: every
// refusal below is a state whose consequence is unrecoverable after create.
func DeriveTopology(in *mesh.Inventory, c TopologyChoices) (*Topology, error) {
	if in == nil {
		return nil, fmt.Errorf("no inventory")
	}
	ws := in.WorkloadSites()
	if len(ws) < 2 {
		return nil, fmt.Errorf("a stretched cluster needs at least 2 workload sites; the "+
			"inventory declares %d (a site with no `roles` is a workload site)", len(ws))
	}
	if c.SitesPerShard <= 0 {
		return nil, fmt.Errorf("sitesPerShard is required and is never guessed: it is the COPY " +
			"FACTOR of an in-memory store — every shard gets sitesPerShard*(replicasPerShard+1) " +
			"members — so every candidate default is wrong for somebody, which is why the CRD " +
			"refuses to default it either (LR-033). State it with --sites-per-shard")
	}
	if c.SitesPerShard < 2 {
		return nil, fmt.Errorf("sitesPerShard must be at least 2: with one site per shard a "+
			"whole-site loss takes that shard's only copies, which is the property multi-site "+
			"exists for (got %d)", c.SitesPerShard)
	}
	if c.SitesPerShard > len(ws) {
		return nil, fmt.Errorf("sitesPerShard %d exceeds the %d workload site(s) in the inventory",
			c.SitesPerShard, len(ws))
	}
	if c.Shards < 3 {
		return nil, fmt.Errorf("shards must be at least 3 (Redis Cluster's own minimum); got %d",
			c.Shards)
	}

	hubCtx := in.HubContext()
	t := &Topology{SitesPerShard: c.SitesPerShard}
	names := make([]string, 0, len(ws))
	for _, s := range ws {
		ts := TopologySite{Name: s.Name}
		// A hub-local workload site carries NO kubeconfigSecretRef — its absence is the
		// marker. Only a remote site gets one.
		if !s.HasRole(mesh.RoleHub) || s.KubeContext() != hubCtx {
			ts.KubeconfigSecretName = s.KubeconfigSecretName()
		}
		t.Sites = append(t.Sites, ts)
		names = append(names, s.Name)
	}

	for k := range c.Shards {
		var on []string
		for _, i := range SitesForShard(k, len(ws), c.SitesPerShard) {
			on = append(on, names[i])
		}
		t.Placement = append(t.Placement, ShardPlacement{Shard: k, Sites: on})
	}

	t.Resilience = evaluateResilience(c.Shards, len(ws))
	return t, nil
}

func evaluateResilience(shards, sites int) Resilience {
	r := Resilience{
		Shards:                shards,
		Sites:                 sites,
		NativeSurvivalTarget:  NativeSurvivalTarget(shards),
		TakeoverSurvivalFloor: TakeoverSurvivalFloor(shards),
		SiteLossResilient:     SiteLossResilient(shards, sites),
	}
	switch {
	case !r.SiteLossResilient:
		// Given Redis Cluster's three-master minimum this holds in exactly one regime:
		// two sites with an odd shard count (LR-031). No rebalancing fixes it.
		r.Summary = fmt.Sprintf(
			"%d shards over %d sites CANNOT auto-survive an arbitrary single-site loss: "+
				"%d x floor(%d/2)=%d < %d, so some site must hold more masters than the takeover "+
				"floor and losing THAT site strands the survivors below quorum. The instance is "+
				"valid and will serve; recovery of that site's loss needs a human or "+
				"multiSite.allowUnsafeSiteTakeover. The operator reports this as "+
				"ReducedSiteLossResilience. Use an EVEN shard count, or a third site.",
			shards, sites, sites, shards, r.TakeoverSurvivalFloor, shards)
	case sites == 2:
		r.Summary = fmt.Sprintf(
			"%d shards over 2 sites auto-survives any single-site loss by OPERATOR TAKEOVER "+
				"(floor %d, at the %d:%d master split). Two sites never survive natively for any "+
				"shard count — the heavier side always holds ceil(K/2) — so recovery here is the "+
				"quorum-arithmetic takeover plus its cooldown, not a native Redis election.",
			shards, r.TakeoverSurvivalFloor, shards/2, shards-shards/2)
	default:
		r.Summary = fmt.Sprintf(
			"%d shards over %d sites auto-survives any single-site loss (floor %d, native "+
				"target %d). A site at or below the target recovers by a native Redis election; "+
				"between target and floor it recovers by operator takeover, which is a slower "+
				"recovery rather than an outage.",
			shards, sites, r.TakeoverSurvivalFloor, r.NativeSurvivalTarget)
	}
	return r
}

// MultiSiteYAML renders the generated spec.multiSite block, indented to sit under `spec:`.
func (t *Topology) MultiSiteYAML() string {
	var b strings.Builder
	b.WriteString("  multiSite:\n")
	fmt.Fprintf(&b, "    sitesPerShard: %d\n", t.SitesPerShard)
	b.WriteString("    # ORDER IS SIGNIFICANT: a site's position is its placement index\n")
	b.WriteString("    # (shard K -> sites (K+j) mod S), and the list is immutable after\n")
	b.WriteString("    # create. Reordering it would re-place every shard.\n")
	b.WriteString("    sites:\n")
	for _, s := range t.Sites {
		fmt.Fprintf(&b, "      - name: %s\n", s.Name)
		if s.KubeconfigSecretName == "" {
			b.WriteString("        # hub-local: no kubeconfigSecretRef, which is what marks it so\n")
			continue
		}
		fmt.Fprintf(&b, "        kubeconfigSecretRef:\n          name: %s\n", s.KubeconfigSecretName)
	}
	return b.String()
}
