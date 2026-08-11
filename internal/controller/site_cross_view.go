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
	"sort"
	"strings"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// splitSiteQualifiedKey splits a site-qualified ground-truth key (site/objName)
// back into (site, objName). A key without "/" is treated as having no site,
// returning ("", key) — the inverse of siteQualifiedKey.
func splitSiteQualifiedKey(key string) (site, objName string) {
	if before, after, ok := strings.Cut(key, "/"); ok {
		return before, after
	}
	return "", key
}

// buildCrossSiteView projects the union ground truth onto the compact,
// site-attributed cross-site summary the operator publishes to
// status.multiSite for tier-1 (read-only-hub) consumers. It is the operator
// counterpart of lrctl's live checks: per shard it records the master's site,
// the replicas' sites, whether the shard spans >= 2 sites (the failure-domain
// guarantee), and whether any replica link is down. crossSiteOK is true only
// when at least one shard exists and every shard is cross-site.
//
// It also returns the current per-site slot-serving-master distribution
// (masterCounts, ADR-012 D2): the count of masters holding >= 1 slot in each
// site, mirroring buildMasterRebalanceInput's master criterion. This is the
// tier-1 published input for the balance-invariant live health (per-site count
// vs the ceil(K/2)-1 target), sorted by site for determinism.
//
// Site + shard are read from each node's site-qualified ground-truth key
// (site/objName; ShardIndexFromPodName tolerates the site prefix). Nodes whose
// key carries no shard index are skipped — there is nothing to attribute.
func buildCrossSiteView(gt *redisclient.ClusterGroundTruth) (state string, slots int32, crossSiteOK bool, shards []littleredv1alpha1.ShardSiteView, masterCounts []littleredv1alpha1.SiteMasterCount) {
	state = gt.ClusterState
	slots = int32(gt.TotalSlots)

	type acc struct {
		masterSite   string
		replicaSites map[string]bool
		linkDown     bool
	}
	byShard := map[int]*acc{}
	mastersPerSite := map[string]int32{}
	for _, n := range gt.Nodes {
		shard := redisclient.ShardIndexFromPodName(n.PodName)
		if shard < 0 {
			continue
		}
		site, _ := splitSiteQualifiedKey(n.PodName)
		// Slot-serving master distribution (D2): count only masters holding >= 1 slot,
		// matching buildMasterRebalanceInput / buildSiteTakeoverInput (a fresh empty
		// master is not yet a Domain-M vote).
		if n.Role == RoleMaster && len(n.Slots) > 0 && site != "" {
			mastersPerSite[site]++
		}
		a := byShard[shard]
		if a == nil {
			a = &acc{replicaSites: map[string]bool{}}
			byShard[shard] = a
		}
		switch n.Role {
		case RoleMaster:
			if site != "" {
				a.masterSite = site
			}
		case RoleReplica:
			if site != "" {
				a.replicaSites[site] = true
			}
			if n.LinkStatus == "down" {
				a.linkDown = true
			}
		}
	}

	masterCounts = make([]littleredv1alpha1.SiteMasterCount, 0, len(mastersPerSite))
	for site, c := range mastersPerSite {
		masterCounts = append(masterCounts, littleredv1alpha1.SiteMasterCount{Site: site, Masters: c})
	}
	sort.Slice(masterCounts, func(i, j int) bool { return masterCounts[i].Site < masterCounts[j].Site })

	shardIdxs := make([]int, 0, len(byShard))
	for k := range byShard {
		shardIdxs = append(shardIdxs, k)
	}
	sort.Ints(shardIdxs)

	crossSiteOK = len(shardIdxs) > 0
	for _, k := range shardIdxs {
		a := byShard[k]
		replicaSites := make([]string, 0, len(a.replicaSites))
		for s := range a.replicaSites {
			replicaSites = append(replicaSites, s)
		}
		sort.Strings(replicaSites)

		cross := false
		for _, rs := range replicaSites {
			if a.masterSite != "" && rs != a.masterSite {
				cross = true
				break
			}
		}
		if !cross {
			crossSiteOK = false
		}
		shards = append(shards, littleredv1alpha1.ShardSiteView{
			Shard:        k,
			MasterSite:   a.masterSite,
			ReplicaSites: replicaSites,
			CrossSite:    cross,
			LinkDown:     a.linkDown,
		})
	}
	return state, slots, crossSiteOK, shards, masterCounts
}
