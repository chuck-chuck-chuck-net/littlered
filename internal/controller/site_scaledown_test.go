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
	"sort"
	"testing"
)

// observedFromTopology enumerates the per-(site, shard) StatefulSets that exist under a given
// (siteNames, shards, sitesPerShard) topology — i.e. the layout the previous spec applied. Used
// to model "what exists on the cluster now" so a diff against the NEW desired set reproduces a
// real scale-down.
func observedFromTopology(instance string, siteNames []string, shards, sitesPerShard int) []observedShardSTS {
	var out []observedShardSTS
	numSites := len(siteNames)
	if sitesPerShard > numSites {
		sitesPerShard = numSites
	}
	for shardIdx, siteIdxs := range multiSiteShardHostingSites(numSites, shards, sitesPerShard) {
		for _, si := range siteIdxs {
			out = append(out, observedShardSTS{
				Site:  siteNames[si],
				Shard: shardIdx,
				Name:  fmt.Sprintf("%s-shard-%d", instance, shardIdx),
			})
		}
	}
	return out
}

func orphanKeys(orphans []multiSiteOrphanSTS) []string {
	out := make([]string, 0, len(orphans))
	for _, o := range orphans {
		out = append(out, fmt.Sprintf("%s#%d", o.Site, o.Shard))
	}
	sort.Strings(out)
	return out
}

// TestPlanMultiSiteScaleDown covers the three shrink dimensions and combinations: an orphan is
// any observed per-(site, shard) StatefulSet the desired placement no longer includes.
func TestPlanMultiSiteScaleDown(t *testing.T) {
	const inst = "cache"
	three := []string{siteName0, siteName1, siteName2}
	two := []string{siteName0, siteName1}

	tests := []struct {
		name string
		// old topology (what exists on the cluster)
		oldSites []string
		oldShard int
		oldSPS   int
		// new/desired topology (current spec)
		newSites []string
		newShard int
		newSPS   int
		want     []string // "site#shard" of expected orphans
	}{
		{
			name:     "no change: steady state, no orphans",
			oldSites: three, oldShard: 3, oldSPS: 2,
			newSites: three, newShard: 3, newSPS: 2,
			want: nil,
		},
		{
			name:     "shards decrease 3->2 orphans shard-2's STSs",
			oldSites: three, oldShard: 3, oldSPS: 2,
			newSites: three, newShard: 2, newSPS: 2,
			// shard2 hosted in sites (2,0) under sps2 -> orphaned in site2 and site0
			want: []string{keySite0Shard2STS, keySite2Shard2STS},
		},
		{
			name:     "sitesPerShard decrease 2->1 orphans each shard's second site",
			oldSites: three, oldShard: 3, oldSPS: 2,
			newSites: three, newShard: 3, newSPS: 1,
			// old hosting: 0->{0,1} 1->{1,2} 2->{2,0}; new: 0->{0} 1->{1} 2->{2}
			// orphans: site1#0, site2#1, site0#2
			want: []string{keySite0Shard2STS, "site1#0", keySite2Shard1STS},
		},
		{
			name:     "site removed (site2 gone) orphans every shard STS that lived in site2",
			oldSites: three, oldShard: 3, oldSPS: 2,
			newSites: two, newShard: 3, newSPS: 2,
			// old hosting over 3 sites: 0->{s0,s1} 1->{s1,s2} 2->{s2,s0}. site2 hosted shard1 and
			// shard2. New placement over 2 sites: 0->{s0,s1} 1->{s1,s0} 2->{s0,s1}. So desired
			// covers all of site0/site1's (site,shard) pairs; only site2's two STSs orphan.
			want: []string{keySite2Shard1STS, keySite2Shard2STS},
		},
		{
			name:     "combination: shards 3->2 AND site2 removed",
			oldSites: three, oldShard: 3, oldSPS: 2,
			newSites: two, newShard: 2, newSPS: 2,
			// old STSs: shard0 in {0,1}, shard1 in {1,2}, shard2 in {2,0}.
			// new desired over 2 sites, 2 shards, sps2: shard0->{0,1}, shard1->{1,0}.
			// desired pairs: site0#0, site1#0, site1#1, site0#1.
			// observed pairs: site0#0, site1#0, site1#1, site2#1, site2#2, site0#2.
			// orphans: site2#1, site2#2, site0#2.
			want: []string{keySite0Shard2STS, keySite2Shard1STS, keySite2Shard2STS},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			observed := observedFromTopology(inst, tc.oldSites, tc.oldShard, tc.oldSPS)
			desired := desiredMultiSiteShardSet(tc.newSites, tc.newShard, tc.newSPS)
			got := orphanKeys(planMultiSiteScaleDown(desired, observed))

			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			if len(got) != len(want) {
				t.Fatalf("orphans = %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("orphans = %v, want %v", got, want)
				}
			}
		})
	}
}
