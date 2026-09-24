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

// site_placement.go holds the PURE, I/O-free placement model for multi-site cluster
// mode (docs/MULTI_SITE_DESIGN.md §4). It is the single source of truth for "which
// pods exist in which site, which shard each belongs to, and which is a shard's
// initial (bootstrap) master." Every site-aware reconcile surface consumes it, so the
// round-robin, sizing invariant, and even-master-spread live in one testable place.

// siteQualifiedKey is the operator's in-memory ground-truth key for a multi-site pod.
// The Kubernetes object name ({name}-shard-K-O) is only unique WITHIN a site — the same
// name exists in every hosting site's Kubernetes cluster (design §4, "names may collide
// across sites") — so the operator qualifies it with the site name to key its
// ground-truth maps unambiguously. Pod IPs are globally unique (routed pod CIDRs), so
// the IP->key direction never collides; this handles the key->identity direction.
func siteQualifiedKey(siteName, objName string) string {
	return siteName + "/" + objName
}

// sitesForShard returns the ordered site indices hosting shard shardIdx under the
// deterministic round-robin rule (design §4): shard K -> sites (K+j) mod numSites for
// j in 0..sitesPerShard-1. Evens out capacity when shards do not divide evenly by sites.
// The FIRST returned site (K mod numSites) is the shard's primary — its initial master
// lives there, which distributes initial masters across sites as evenly as arithmetic
// allows.
func sitesForShard(shardIdx, numSites, sitesPerShard int) []int {
	if numSites <= 0 || sitesPerShard <= 0 {
		return nil
	}
	if sitesPerShard > numSites {
		sitesPerShard = numSites
	}
	out := make([]int, 0, sitesPerShard)
	for j := range sitesPerShard {
		out = append(out, (shardIdx+j)%numSites)
	}
	return out
}

// multiSitePodRef identifies one pod of a stretched (multi-site) cluster, resolved to the
// site that hosts it. It is the multi-site analogue of ClusterPodRef (single-site SSOT),
// with a site dimension and a site-qualified ground-truth Key.
type multiSitePodRef struct {
	// SiteIdx is the site's index in the ordered spec site list.
	SiteIdx int
	// SiteName is the site's name from spec.multiSite.sites.
	SiteName string
	// ObjName is the Kubernetes pod object name in the site ({name}-shard-K-O). Not
	// globally unique (collides across sites) — use Key for ground-truth maps.
	ObjName string
	// Key is the site-qualified ground-truth key (siteName/objName), globally unique.
	Key string
	// ShardIdx is the shard this pod belongs to (0..shards-1).
	ShardIdx int
	// Ordinal is the pod's ordinal within its per-site shard StatefulSet (0..replicasPerShard).
	Ordinal int
	// IsInitialMaster is true iff this pod is its shard's intended bootstrap master:
	// ordinal 0 in the shard's primary hosting site (K mod numSites). Roles are fluid
	// afterwards (design §4) — this only seeds the initial replication topology.
	IsInitialMaster bool
}

// multiSiteClusterPodRefs enumerates every pod of a stretched cluster in a stable order:
// shard 0's pods (each hosting site in sitesForShard order, ordinals 0..replicasPerShard),
// then shard 1's, and so on. It is the multi-site pod-enumeration SSOT (pillar 3.12).
//
// Per shard there are sitesPerShard StatefulSets (one per hosting site), each sized
// replicasPerShard+1, so total members per shard = sitesPerShard x (replicasPerShard + 1)
// (design §4 — the reading trap the docs must state prominently). Exactly one pod per
// shard is the initial master; every other member is an initial replica of it, and since
// a shard spans >= 2 sites, each master has at least one cross-site replica.
func multiSiteClusterPodRefs(instance string, siteNames []string, shards, replicasPerShard, sitesPerShard int) []multiSitePodRef {
	numSites := len(siteNames)
	if numSites == 0 {
		return nil
	}
	if sitesPerShard > numSites {
		sitesPerShard = numSites
	}
	refs := make([]multiSitePodRef, 0, shards*sitesPerShard*(replicasPerShard+1))
	for shardIdx := range shards {
		primarySite := shardIdx % numSites
		for _, siteIdx := range sitesForShard(shardIdx, numSites, sitesPerShard) {
			siteName := siteNames[siteIdx]
			for ordinal := 0; ordinal <= replicasPerShard; ordinal++ {
				objName := clusterShardPodName(instance, shardIdx, ordinal)
				refs = append(refs, multiSitePodRef{
					SiteIdx:         siteIdx,
					SiteName:        siteName,
					ObjName:         objName,
					Key:             siteQualifiedKey(siteName, objName),
					ShardIdx:        shardIdx,
					Ordinal:         ordinal,
					IsInitialMaster: siteIdx == primarySite && ordinal == 0,
				})
			}
		}
	}
	return refs
}

// multiSiteShardHostingSites returns, per shard index, the ordered site indices hosting
// that shard (the placement the resource loop applies StatefulSets to). Convenience over
// sitesForShard for the whole shard range.
func multiSiteShardHostingSites(numSites, shards, sitesPerShard int) [][]int {
	out := make([][]int, shards)
	for k := range shards {
		out[k] = sitesForShard(k, numSites, sitesPerShard)
	}
	return out
}

// multiSiteBootstrapPlan is the initial replication topology for a stretched cluster's
// bootstrap: which pod is each shard's initial master, and every replica's target master.
// Keys are site-qualified ground-truth keys (multiSitePodRef.Key).
type multiSiteBootstrapPlan struct {
	// MastersByShard maps shard index -> that shard's initial-master Key.
	MastersByShard map[int]string
	// ReplicaToMaster maps a replica's Key -> its shard master's Key.
	ReplicaToMaster map[string]string
}

// planMultiSiteBootstrap derives the initial replication wiring from the pod refs: each
// shard's IsInitialMaster pod becomes the master; every other member of that shard
// replicates it. Because a shard spans >= 2 sites (validated sitesPerShard >= 2), every
// master ends up with at least one cross-site replica — the whole point of multi-site.
func planMultiSiteBootstrap(refs []multiSitePodRef) multiSiteBootstrapPlan {
	plan := multiSiteBootstrapPlan{
		MastersByShard:  make(map[int]string),
		ReplicaToMaster: make(map[string]string),
	}
	for _, ref := range refs {
		if ref.IsInitialMaster {
			plan.MastersByShard[ref.ShardIdx] = ref.Key
		}
	}
	for _, ref := range refs {
		if ref.IsInitialMaster {
			continue
		}
		if master, ok := plan.MastersByShard[ref.ShardIdx]; ok {
			plan.ReplicaToMaster[ref.Key] = master
		}
	}
	return plan
}

// multiSiteInitialMasterKey returns the site-qualified key of shard shardIdx's intended
// bootstrap master (ordinal 0 in the primary hosting site). Used by the missing-shard
// repair step to know which node a shard's slots belong to, mirroring the single-site
// shardMasterPodName rule.
func multiSiteInitialMasterKey(instance string, siteNames []string, shardIdx int) string {
	numSites := len(siteNames)
	if numSites == 0 {
		return ""
	}
	primarySite := shardIdx % numSites
	return siteQualifiedKey(siteNames[primarySite], clusterShardPodName(instance, shardIdx, 0))
}
