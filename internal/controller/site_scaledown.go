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

// LR-032: the topology-change POLICY now lives at admission — CRD transition rules pin
// spec.multiSite.sites (names + order), spec.multiSite.sitesPerShard, spec.cluster.shards (while
// multiSite is set) and the presence of the multiSite block itself, so none of the shrink
// dimensions below — nor the additive site growth this diff never caught — can reach the
// reconciler through the API. This guard is retained as DEFENCE IN DEPTH for drift admission
// cannot see: hand-created/hand-edited StatefulSets, an instance whose CRD predates the rules, or
// a cluster without CEL validation. It is a drift net, not the policy. Runtime is deliberately the
// wrong layer for the policy: a runtime detector would have to tell "a site was added" apart from
// "a site is temporarily unobservable", and would risk refusing during the very site outage
// recoverSiteLoss exists to handle. See docs/MULTI_SITE_DESIGN.md §2/§7.
//
// site_scaledown.go holds the PURE, I/O-free scale-down / topology-shrink refusal detector for
// multi-site cluster mode (docs/MULTI_SITE_DESIGN.md §10 item 7, changelog LR-028). Reducing
// spec.cluster.shards, lowering spec.multiSite.sitesPerShard, or removing a site from
// spec.multiSite.sites leaves per-site shard StatefulSets the desired placement no longer
// includes. Cluster storage is EmptyDir, so those StatefulSets still hold their shard's slot
// data; LittleRed never deletes data by default (pillar 3.13), so the operator must REFUSE
// (surface a condition, wait) rather than silently leave them as un-managed drift or auto-delete
// them. This is the multi-site analogue of the single-site detectOrphanedShardStatefulSets /
// reportShardScaleDown guard.
//
// All three shrink dimensions collapse to one predicate — "a per-(site, shard) StatefulSet
// exists that the desired placement set does not want" — so a single orphan-set diff covers
// shards↓, sitesPerShard↓, and a removed site. The one dimension the operator CANNOT catch is a
// site fully removed from the spec: observation requires the site's SiteSpec (its kubeconfig
// ref), which is gone once the entry is removed, so the operator no longer builds a client for
// it and never lists its StatefulSets. Refusal requires observability; that limitation is
// documented (see the wiring in site_cluster_reconcile.go and MULTI_SITE_DESIGN.md §10). The
// pure diff below is nonetheless fully general — given an observation of a removed site it would
// flag it — so the limitation is a property of the I/O layer, not of this decision seam.

// reasonMultiSiteScaleDownRefused is the status/condition Reason and event reason for a refused
// multi-site topology shrink (the multi-site sibling of the single-site "ShardScaleDownRefused").
const reasonMultiSiteScaleDownRefused = "MultiSiteScaleDownRefused"

// observedShardSTS is one per-site shard StatefulSet observed in a site this pass: the site it
// lives in, the shard index recovered from its LabelShard label, and its object name.
type observedShardSTS struct {
	Site  string
	Shard int
	Name  string
}

// multiSiteOrphanSTS identifies one observed per-site shard StatefulSet that the desired
// (shards, sitesPerShard, sites) topology no longer includes — orphaned by a scale-down.
type multiSiteOrphanSTS struct {
	Site  string
	Shard int
	Name  string
}

// desiredSiteShard keys the desired-placement set by (site name, shard index). Keyed by NAME,
// not index, so the set stays correct across a change to the site list.
type desiredSiteShard struct {
	Site  string
	Shard int
}

// desiredMultiSiteShardSet returns the set of (site, shard) per-site StatefulSets the CURRENT
// topology wants: shard K hosts in the round-robin sites (K+j) mod S for j < sitesPerShard
// (the placement SSOT, sitesForShard / multiSiteShardHostingSites). Keyed by site NAME.
func desiredMultiSiteShardSet(siteNames []string, shards, sitesPerShard int) map[desiredSiteShard]bool {
	desired := map[desiredSiteShard]bool{}
	numSites := len(siteNames)
	if numSites == 0 || shards <= 0 || sitesPerShard <= 0 {
		return desired
	}
	if sitesPerShard > numSites {
		sitesPerShard = numSites
	}
	for shardIdx, siteIdxs := range multiSiteShardHostingSites(numSites, shards, sitesPerShard) {
		for _, si := range siteIdxs {
			desired[desiredSiteShard{Site: siteNames[si], Shard: shardIdx}] = true
		}
	}
	return desired
}

// planMultiSiteScaleDown is the pure orphan-set diff: every observed per-site shard StatefulSet
// whose (site, shard) is NOT in the desired set is an orphan the operator must refuse (never
// delete). Fully general over all three shrink dimensions — an orphan is simply "observed but
// not desired". Input order is preserved for stable messages and tests.
func planMultiSiteScaleDown(desired map[desiredSiteShard]bool, observed []observedShardSTS) []multiSiteOrphanSTS {
	orphans := make([]multiSiteOrphanSTS, 0, len(observed))
	for _, o := range observed {
		if desired[desiredSiteShard{Site: o.Site, Shard: o.Shard}] {
			continue
		}
		orphans = append(orphans, multiSiteOrphanSTS(o))
	}
	return orphans
}
