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
	"strings"
	"testing"

	"github.com/chuck-chuck-chuck-net/littlered/internal/mesh"
)

func inv(sites ...mesh.Site) *mesh.Inventory { return &mesh.Inventory{Sites: sites} }

func hub(name string) mesh.Site {
	return mesh.Site{Name: name, Context: name, Roles: []string{mesh.RoleHub}}
}

func workload(name, secret string) mesh.Site {
	return mesh.Site{Name: name, Context: name, Endpoint: "https://" + name + ":6443",
		KubeconfigSecret: secret}
}

// DeriveTopology turns the lab's FACTS (which clusters exist, in what order) into the
// spec.multiSite block, and refuses the CHOICES it cannot make. The split is slaptain's —
// its mesh-derive-topology.sh generates the topology values and leaves the directory to the
// human — and it matters more here than there, because of three decisions already recorded:
// the site list is listType=atomic and POSITION is the placement index (LR-033), the topology
// is immutable after create (LR-032), and sitesPerShard is required-and-never-defaulted
// because it is the copy factor of a RAM-resident store (LR-033). A mistake in this block is
// therefore not editable — it is a delete-and-recreate, which under EmptyDir is the data.
func TestDeriveTopology(t *testing.T) {
	three := inv(hub("scm-cp"), workload("scm-s1", "site-scm-s1"),
		workload("scm-s2", "site-scm-s2"), workload("scm-s3", "site-scm-s3"))

	t.Run("emits the workload sites in inventory order, hub excluded", func(t *testing.T) {
		got, err := DeriveTopology(three, TopologyChoices{SitesPerShard: 2, Shards: 3})
		if err != nil {
			t.Fatalf("DeriveTopology: %v", err)
		}
		names := make([]string, 0, len(got.Sites))
		for _, s := range got.Sites {
			names = append(names, s.Name)
		}
		if strings.Join(names, ",") != "scm-s1,scm-s2,scm-s3" {
			t.Errorf("sites = %v, want the three workload sites in inventory order", names)
		}
		if got.Sites[0].KubeconfigSecretName != "site-scm-s1" {
			t.Errorf("kubeconfigSecretRef = %q, want the inventory's", got.Sites[0].KubeconfigSecretName)
		}
		if got.SitesPerShard != 2 {
			t.Errorf("sitesPerShard = %d, want the value the caller had to state", got.SitesPerShard)
		}
	})

	t.Run("a hub that also carries pods IS a site of the topology", func(t *testing.T) {
		h := hub("scm-cp")
		h.Roles = []string{mesh.RoleHub, mesh.RoleWorkload}
		got, err := DeriveTopology(inv(h, workload("scm-s1", "site-scm-s1")),
			TopologyChoices{SitesPerShard: 2, Shards: 3})
		if err != nil {
			t.Fatalf("DeriveTopology: %v", err)
		}
		if len(got.Sites) != 2 || got.Sites[0].Name != "scm-cp" {
			t.Fatalf("sites = %+v, want the hub first (it carries pods)", got.Sites)
		}
		// Hub-local: the operator reaches it through its own client, so it must carry NO
		// kubeconfigSecretRef — that absence is exactly what marks it hub-local.
		if got.Sites[0].KubeconfigSecretName != "" {
			t.Errorf("hub-local site got kubeconfigSecretRef %q, want none",
				got.Sites[0].KubeconfigSecretName)
		}
	})

	t.Run("sitesPerShard is never guessed", func(t *testing.T) {
		_, err := DeriveTopology(three, TopologyChoices{Shards: 3})
		if err == nil || !strings.Contains(err.Error(), "sitesPerShard") {
			t.Fatalf("err = %v, want a refusal naming sitesPerShard", err)
		}
	})

	t.Run("sitesPerShard above the site count is refused", func(t *testing.T) {
		_, err := DeriveTopology(inv(hub("h"), workload("a", "sa"), workload("b", "sb")),
			TopologyChoices{SitesPerShard: 3, Shards: 3})
		if err == nil || !strings.Contains(err.Error(), "2 workload site") {
			t.Fatalf("err = %v, want a refusal naming the site count", err)
		}
	})

	t.Run("fewer than two workload sites is not a mesh", func(t *testing.T) {
		_, err := DeriveTopology(inv(hub("h"), workload("a", "sa")),
			TopologyChoices{SitesPerShard: 2, Shards: 3})
		if err == nil || !strings.Contains(err.Error(), "at least 2") {
			t.Fatalf("err = %v, want a refusal", err)
		}
	})
}

// The resilience report is the half a human cannot compute in their head, and it is the one
// LR-031 spent a whole entry getting right: the two thresholds coincide for odd shard counts
// and differ by exactly one for even ones, which is why a naive single bound left a K=4
// dual-site topology permanently drifted and unrecoverable.
func TestDeriveTopologyResilienceReport(t *testing.T) {
	tests := []struct {
		name          string
		sites, shards int
		wantResilient bool
		wantFloor     int
		wantTarget    int
	}{
		// The D3 recommendation: even K over two sites lives on the floor, reachable at
		// the K/2:K/2 split, so it auto-survives by takeover.
		{"two sites, even shards — the recommended dual-site", 2, 4, true, 2, 1},
		// The one regime where the structural warning fires at all (LR-031): two sites,
		// odd K. No rebalancing fixes it.
		{"two sites, odd shards — cannot auto-survive", 2, 3, false, 1, 1},
		// LR-031 SUPERSEDED the "K=4 over 3 sites is a trap" reading: 2+1+1 loses at most
		// floor(4/2)=2, which is fence-safe.
		{"three sites, four shards — NOT a trap", 3, 4, true, 2, 1},
		{"three sites, three shards", 3, 3, true, 1, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sites := []mesh.Site{hub("h")}
			for i := range tc.sites {
				n := string(rune('a' + i))
				sites = append(sites, workload(n, "s-"+n))
			}
			got, err := DeriveTopology(inv(sites...),
				TopologyChoices{SitesPerShard: 2, Shards: tc.shards})
			if err != nil {
				t.Fatalf("DeriveTopology: %v", err)
			}
			r := got.Resilience
			if r.SiteLossResilient != tc.wantResilient {
				t.Errorf("SiteLossResilient = %v, want %v", r.SiteLossResilient, tc.wantResilient)
			}
			if r.TakeoverSurvivalFloor != tc.wantFloor || r.NativeSurvivalTarget != tc.wantTarget {
				t.Errorf("floor/target = %d/%d, want %d/%d",
					r.TakeoverSurvivalFloor, r.NativeSurvivalTarget, tc.wantFloor, tc.wantTarget)
			}
			// A non-resilient topology must SAY so — the warning is the whole reason the
			// report exists, and the operator raises ReducedSiteLossResilience for it.
			if !tc.wantResilient && !strings.Contains(strings.ToLower(r.Summary), "cannot") {
				t.Errorf("Summary = %q, want it to state the topology cannot auto-survive", r.Summary)
			}
		})
	}
}

// Placement is index-based and IMMUTABLE after create, so the one thing a human cannot check
// by reading the CR is where each shard actually lands. Deriving it is free — the rule is
// arithmetic — and printing it is what makes the order visibly load-bearing.
func TestDeriveTopologyPlacement(t *testing.T) {
	got, err := DeriveTopology(inv(hub("h"), workload("a", "sa"), workload("b", "sb"), workload("c", "sc")),
		TopologyChoices{SitesPerShard: 2, Shards: 3})
	if err != nil {
		t.Fatalf("DeriveTopology: %v", err)
	}
	// shard K -> sites (K+j) mod S, j < sitesPerShard
	want := map[int]string{0: "a,b", 1: "b,c", 2: "c,a"}
	if len(got.Placement) != 3 {
		t.Fatalf("Placement = %+v, want one entry per shard", got.Placement)
	}
	for _, p := range got.Placement {
		if got := strings.Join(p.Sites, ","); got != want[p.Shard] {
			t.Errorf("shard %d lands on %q, want %q", p.Shard, got, want[p.Shard])
		}
	}
}

// SitesForShard is the CLI mirror of the operator's placement rule. Its drift guard lives
// beside the threshold mirrors' (internal/controller/site_rebalance_test.go), which is what
// keeps this from becoming a second, quietly-diverging definition (LR-045).
func TestSitesForShardMirror(t *testing.T) {
	// A shard never lands on the same site twice, and always gets sitesPerShard sites.
	for s := 2; s <= 5; s++ {
		for sps := 2; sps <= s; sps++ {
			for k := range 12 {
				got := SitesForShard(k, s, sps)
				if len(got) != sps {
					t.Fatalf("SitesForShard(%d,%d,%d) = %v, want %d sites", k, s, sps, got, sps)
				}
				seen := map[int]bool{}
				for _, i := range got {
					if seen[i] {
						t.Fatalf("SitesForShard(%d,%d,%d) = %v repeats a site", k, s, sps, got)
					}
					seen[i] = true
				}
			}
		}
	}
	// Over-requesting CLAMPS to the site count, matching the operator. This assertion
	// originally said "want nil" — the drift guard in internal/controller failed on its
	// first run and said otherwise, which is the guard doing its job.
	if got := SitesForShard(0, 3, 4); len(got) != 3 {
		t.Errorf("SitesForShard(0,3,4) = %v, want it clamped to 3 sites", got)
	}
}
