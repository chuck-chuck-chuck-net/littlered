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

package controller

import (
	"fmt"
	"slices"
	"testing"
)

func TestSitesForShard(t *testing.T) {
	tests := []struct {
		name          string
		shardIdx      int
		numSites      int
		sitesPerShard int
		want          []int
	}{
		{"2 sites, shard 0", 0, 2, 2, []int{0, 1}},
		{"2 sites, shard 1", 1, 2, 2, []int{1, 0}},
		{"2 sites, shard 2 wraps", 2, 2, 2, []int{0, 1}},
		{"3 sites, sitesPerShard 2, shard 0", 0, 3, 2, []int{0, 1}},
		{"3 sites, sitesPerShard 2, shard 1", 1, 3, 2, []int{1, 2}},
		{"3 sites, sitesPerShard 2, shard 2 wraps", 2, 3, 2, []int{2, 0}},
		{"3 sites, sitesPerShard 3, shard 1", 1, 3, 3, []int{1, 2, 0}},
		{"sitesPerShard clamped to numSites", 0, 2, 5, []int{0, 1}},
		{"degenerate zero sites", 0, 0, 2, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sitesForShard(tc.shardIdx, tc.numSites, tc.sitesPerShard)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("sitesForShard(%d,%d,%d) = %v, want %v", tc.shardIdx, tc.numSites, tc.sitesPerShard, got, tc.want)
			}
		})
	}
}

func TestMultiSiteClusterPodRefsMemberCounts(t *testing.T) {
	tests := []struct {
		name             string
		sites            []string
		shards           int
		replicasPerShard int
		sitesPerShard    int
	}{
		{"dual-site rp1 sps2", []string{"a", "b"}, 3, 1, 2},
		{"dual-site rp0 sps2", []string{"a", "b"}, 3, 0, 2},
		{"tri-site rp1 sps2", []string{"a", "b", "c"}, 3, 1, 2},
		{"tri-site rp1 sps3", []string{"a", "b", "c"}, 4, 1, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			refs := multiSiteClusterPodRefs("cache", tc.sites, tc.shards, tc.replicasPerShard, tc.sitesPerShard)

			perShardTotal := tc.sitesPerShard * (tc.replicasPerShard + 1)
			if len(refs) != tc.shards*perShardTotal {
				t.Fatalf("total members = %d, want %d", len(refs), tc.shards*perShardTotal)
			}

			// Per shard: exactly one initial master, sitesPerShard distinct sites (>= 2),
			// perShardTotal members.
			membersByShard := map[int]int{}
			mastersByShard := map[int]int{}
			sitesByShard := map[int]map[int]bool{}
			mastersPerSite := map[int]int{}
			keys := map[string]bool{}
			for _, ref := range refs {
				membersByShard[ref.ShardIdx]++
				if sitesByShard[ref.ShardIdx] == nil {
					sitesByShard[ref.ShardIdx] = map[int]bool{}
				}
				sitesByShard[ref.ShardIdx][ref.SiteIdx] = true
				if keys[ref.Key] {
					t.Fatalf("duplicate ground-truth key %q", ref.Key)
				}
				keys[ref.Key] = true
				if ref.IsInitialMaster {
					mastersByShard[ref.ShardIdx]++
					mastersPerSite[ref.SiteIdx]++
				}
			}
			for k := 0; k < tc.shards; k++ {
				if membersByShard[k] != perShardTotal {
					t.Fatalf("shard %d members = %d, want %d", k, membersByShard[k], perShardTotal)
				}
				if mastersByShard[k] != 1 {
					t.Fatalf("shard %d initial masters = %d, want exactly 1", k, mastersByShard[k])
				}
				if got := len(sitesByShard[k]); got != tc.sitesPerShard {
					t.Fatalf("shard %d spans %d sites, want %d", k, got, tc.sitesPerShard)
				}
				if len(sitesByShard[k]) < 2 {
					t.Fatalf("shard %d spans < 2 sites: %v", k, sitesByShard[k])
				}
			}

			// Initial masters spread across sites as evenly as arithmetic allows: max-min <= 1.
			minM, maxM := 1<<30, 0
			for s := range tc.sites {
				c := mastersPerSite[s]
				if c < minM {
					minM = c
				}
				if c > maxM {
					maxM = c
				}
			}
			if maxM-minM > 1 {
				t.Fatalf("initial master spread uneven: per-site counts %v (max-min=%d)", mastersPerSite, maxM-minM)
			}
		})
	}
}

func TestMultiSiteClusterPodRefsMapping(t *testing.T) {
	// Dual-site, 2 shards, rp1, sps2. Shard 0 -> sites [0,1]; shard 1 -> sites [1,0].
	refs := multiSiteClusterPodRefs("cache", []string{"alpha", "beta"}, 2, 1, 2)

	// Initial master of shard 0 is alpha/cache-shard-0-0; shard 1 is beta/cache-shard-1-0.
	wantMaster := map[int]string{
		0: "alpha/cache-shard-0-0",
		1: "beta/cache-shard-1-0",
	}
	got := map[int]string{}
	for _, ref := range refs {
		if ref.IsInitialMaster {
			got[ref.ShardIdx] = ref.Key
		}
	}
	for k, want := range wantMaster {
		if got[k] != want {
			t.Fatalf("shard %d initial master = %q, want %q", k, got[k], want)
		}
	}

	// ObjName collides across sites but Key does not: alpha and beta both host cache-shard-0-... only shard's sites.
	if multiSiteInitialMasterKey("cache", []string{"alpha", "beta"}, 1) != "beta/cache-shard-1-0" {
		t.Fatalf("multiSiteInitialMasterKey mismatch: %q", multiSiteInitialMasterKey("cache", []string{"alpha", "beta"}, 1))
	}
}

func TestPlanMultiSiteBootstrap(t *testing.T) {
	sites := []string{"alpha", "beta"}
	refs := multiSiteClusterPodRefs("cache", sites, 3, 1, 2)
	plan := planMultiSiteBootstrap(refs)

	if len(plan.MastersByShard) != 3 {
		t.Fatalf("masters = %d, want 3", len(plan.MastersByShard))
	}

	// Every non-master member maps to its shard's master; no master appears as a replica.
	masterKeys := map[string]int{} // masterKey -> shardIdx
	for shard, key := range plan.MastersByShard {
		masterKeys[key] = shard
	}
	for _, ref := range refs {
		if ref.IsInitialMaster {
			if _, isReplica := plan.ReplicaToMaster[ref.Key]; isReplica {
				t.Fatalf("initial master %q wrongly listed as replica", ref.Key)
			}
			continue
		}
		masterKey, ok := plan.ReplicaToMaster[ref.Key]
		if !ok {
			t.Fatalf("replica %q has no master assignment", ref.Key)
		}
		if masterKeys[masterKey] != ref.ShardIdx {
			t.Fatalf("replica %q (shard %d) points at master %q of shard %d", ref.Key, ref.ShardIdx, masterKey, masterKeys[masterKey])
		}
	}

	// Cross-site replica exists: each master has at least one replica in a different site.
	masterSite := map[string]string{}
	for _, ref := range refs {
		if ref.IsInitialMaster {
			masterSite[ref.Key] = ref.SiteName
		}
	}
	crossSite := map[string]bool{}
	for _, ref := range refs {
		if ref.IsInitialMaster {
			continue
		}
		if ref.SiteName != masterSite[plan.ReplicaToMaster[ref.Key]] {
			crossSite[plan.ReplicaToMaster[ref.Key]] = true
		}
	}
	if len(crossSite) != 3 {
		t.Fatalf("only %d masters have a cross-site replica, want 3", len(crossSite))
	}
}

// TestMultiSiteShardHostingSites pins the per-shard site placement the resource loop uses.
func TestMultiSiteShardHostingSites(t *testing.T) {
	got := multiSiteShardHostingSites(3, 3, 2)
	want := [][]int{{0, 1}, {1, 2}, {2, 0}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("multiSiteShardHostingSites = %v, want %v", got, want)
	}
}
