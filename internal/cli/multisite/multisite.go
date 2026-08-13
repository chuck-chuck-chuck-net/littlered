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

// Package multisite holds the PURE, I/O-free logic behind lrctl's multi-site
// cluster support (docs/MULTI_SITE_DESIGN.md §10 item 6): building the union
// IP->site-qualified-key map the shared gather runs over, resolving each site's
// name to a workstation-routable kube-context, and the cross-site placement
// (failure-domain) interpretation of the gathered ground truth. It mirrors the
// operator's per-site union (internal/controller unionClusterPods /
// siteQualifiedKey) without importing the controller.
package multisite

import (
	"fmt"
	"sort"
	"strings"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// SiteQualifiedKey is lrctl's ground-truth key for a multi-site pod. A pod object
// name ({name}-shard-K-O) is only unique WITHIN a site — the same name exists in
// every hosting site's Kubernetes cluster (design §4, "names may collide across
// sites") — so it is qualified with the site name. Pod IPs are globally unique
// (routed pod CIDRs), so the IP->key direction never collides; this handles the
// key->identity direction. Format matches the operator's siteQualifiedKey.
func SiteQualifiedKey(site, objName string) string {
	return site + "/" + objName
}

// SplitSiteKey splits a site-qualified key back into (site, objName). A key with
// no "/" is treated as having no site (single-site / legacy), returning
// ("", key).
func SplitSiteKey(key string) (site, objName string) {
	if before, after, ok := strings.Cut(key, "/"); ok {
		return before, after
	}
	return "", key
}

// PodRef is one observed cluster pod in a site: its Kubernetes object name and
// pod IP (empty while pending).
type PodRef struct {
	ObjName string
	IP      string
}

// SitePods is the set of this instance's cluster pods observed in one site.
type SitePods struct {
	Site string
	Pods []PodRef
}

// UnionClusterPods builds the shared gather's IP->site-qualified-key map from the
// union of every site's observed pods (with an assigned IP). Mirrors the
// operator's unionClusterPods: pod IPs are routed and globally unique, so one
// gather over the union is correct, and a site that contributed no reachable pods
// simply adds nothing.
func UnionClusterPods(sites []SitePods) map[string]string {
	clusterPods := make(map[string]string)
	for _, s := range sites {
		for _, p := range s.Pods {
			if p.IP != "" {
				clusterPods[p.IP] = SiteQualifiedKey(s.Site, p.ObjName)
			}
		}
	}
	return clusterPods
}

// ParseSiteContextFlag parses the --site-context value "site=ctx,site2=ctx2" into
// a site->context map. An empty value yields an empty (non-nil) map. A pair
// without "=" or with an empty side, or a duplicated site, is an error.
func ParseSiteContextFlag(value string) (map[string]string, error) {
	out := make(map[string]string)
	value = strings.TrimSpace(value)
	if value == "" {
		return out, nil
	}
	for pair := range strings.SplitSeq(value, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		site, ctx, ok := strings.Cut(pair, "=")
		site = strings.TrimSpace(site)
		ctx = strings.TrimSpace(ctx)
		if !ok || site == "" || ctx == "" {
			return nil, fmt.Errorf("invalid --site-context entry %q: want site=context", pair)
		}
		if _, dup := out[site]; dup {
			return nil, fmt.Errorf("duplicate --site-context entry for site %q", site)
		}
		out[site] = ctx
	}
	return out, nil
}

// UnresolvedSite records a site whose kube-context could not be determined, with
// a human-readable reason.
type UnresolvedSite struct {
	Site   string
	Reason string
}

// SiteContextResolution is the result of ResolveSiteContexts.
type SiteContextResolution struct {
	// Resolved maps site name -> kube-context name.
	Resolved map[string]string
	// Unresolved lists sites that could not be mapped to an available context.
	Unresolved []UnresolvedSite
}

// ResolveSiteContexts maps each site name to a workstation-routable kube-context.
// The operator's per-site kubeconfig Secrets embed the replication-net endpoint,
// which is reachable only from hub pods (docs/MULTI_SITE_DESIGN.md §8, two
// distinct network paths), so lrctl on a workstation cannot reuse them; it gathers
// each site through the user's OWN kube-contexts (the admin/site-net path).
//
// Resolution per site: an explicit --site-context mapping wins (but errors if it
// names a context absent from the kubeconfig); otherwise the CONVENTION site name
// == context name applies when such a context exists; otherwise the site is
// unresolved with a reason. Deterministic: sites are processed in the given order.
func ResolveSiteContexts(siteNames []string, explicit map[string]string, available []string) SiteContextResolution {
	avail := make(map[string]bool, len(available))
	for _, c := range available {
		avail[c] = true
	}
	res := SiteContextResolution{Resolved: make(map[string]string)}
	for _, site := range siteNames {
		if ctx, ok := explicit[site]; ok {
			if !avail[ctx] {
				res.Unresolved = append(res.Unresolved, UnresolvedSite{
					Site:   site,
					Reason: fmt.Sprintf("--site-context maps it to %q, which is not a context in the kubeconfig", ctx),
				})
				continue
			}
			res.Resolved[site] = ctx
			continue
		}
		if avail[site] {
			res.Resolved[site] = site
			continue
		}
		res.Unresolved = append(res.Unresolved, UnresolvedSite{
			Site:   site,
			Reason: fmt.Sprintf("no --site-context mapping and no kube-context named %q (pass --site-context %s=<context>)", site, site),
		})
	}
	return res
}

// ShardMember is one gathered cluster node projected onto the cross-site
// placement inputs: which shard it belongs to and which site it lives in.
type ShardMember struct {
	Key       string // site-qualified ground-truth key
	Site      string // owning site (from the qualified key)
	Shard     int    // shard index, -1 if the name is not in per-shard form
	Role      string // "master" or "replica"
	Reachable bool
}

// MembersFromGroundTruth projects the gathered ground truth onto the placement
// inputs, deriving each node's site from its site-qualified key and its shard
// from the pod name suffix (reusing the shared ShardIndexFromPodName). Only nodes
// whose key carries a site and whose name is in per-shard form contribute.
func MembersFromGroundTruth(gt *redisclient.ClusterGroundTruth) []ShardMember {
	out := make([]ShardMember, 0, len(gt.Nodes))
	for _, n := range gt.Nodes {
		site, _ := SplitSiteKey(n.PodName)
		out = append(out, ShardMember{
			Key:       n.PodName,
			Site:      site,
			Shard:     redisclient.ShardIndexFromPodName(n.PodName),
			Role:      n.Role,
			Reachable: n.Reachable,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// roleMaster is the ground-truth Role string for a master node (redisclient's own constant
// is unexported); replicated here for the render-side master-per-site distribution.
const roleMaster = "master"

// NativeSurvivalTarget mirrors the operator's nativeSurvivalTarget (ADR-012 D2): the maximum
// slot-serving masters a site may hold for its loss to leave the survivors holding quorum, so
// Redis recovers natively — ceil(shards/2)-1. Exceeding it downgrades recovery to an operator
// takeover; it is not an outage.
//
// Re-implemented here because the controller symbol is unexported and its package is too heavy
// to import into the CLI; a controller-side drift-guard test asserts the definitions agree.
func NativeSurvivalTarget(shards int) int { return (shards+1)/2 - 1 }

// TakeoverSurvivalFloor mirrors the operator's takeoverSurvivalFloor (ADR-012 D2): the maximum
// slot-serving masters a site may hold for its loss to leave the LOST side short of quorum, so
// the survivors hold a blocking minority and the operator's takeover is fence-safe —
// floor(shards/2). Exceeding it is an availability exposure: that site's loss needs a human or
// multiSite.allowUnsafeSiteTakeover.
func TakeoverSurvivalFloor(shards int) int { return shards / 2 }

// SiteLossResilient mirrors the operator's siteLossResilient (ADR-012 D2): whether the takeover
// FLOOR is *structurally reachable* for K=shards over S=numSites — i.e. K masters can be spread
// so no site exceeds TakeoverSurvivalFloor(K). False means the topology cannot auto-survive an
// arbitrary single-site loss (pigeonhole forces a site over the floor); given K >= 3 that is
// exactly the two-sites-with-odd-K regime.
func SiteLossResilient(shards, numSites int) bool {
	return numSites*TakeoverSurvivalFloor(shards) >= shards
}

// SiteMasterCount is one site's current slot-serving-master count, for the master-per-site
// distribution view (ADR-012 D2).
type SiteMasterCount struct {
	Site    string
	Masters int
}

// MasterCountsFromGroundTruth counts slot-serving masters (role master with >= 1 owned slot)
// per site from a gathered ground truth, deriving each node's site from its site-qualified
// key. Sorted by site. Mirrors the operator's buildCrossSiteView / buildMasterRebalanceInput
// master criterion so the live (tier 2/3) view matches the operator's published (tier 1) one.
func MasterCountsFromGroundTruth(gt *redisclient.ClusterGroundTruth) []SiteMasterCount {
	perSite := map[string]int{}
	for _, n := range gt.Nodes {
		if n.Role != roleMaster || len(n.Slots) == 0 {
			continue
		}
		site, _ := SplitSiteKey(n.PodName)
		if site == "" {
			continue
		}
		perSite[site]++
	}
	out := make([]SiteMasterCount, 0, len(perSite))
	for s, c := range perSite {
		out = append(out, SiteMasterCount{Site: s, Masters: c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Site < out[j].Site })
	return out
}

// MasterDistribution is the ADR-012 D2 balance-invariant health assessment: the current
// per-site slot-serving-master distribution plus its structural and live verdicts. It is
// informational — imbalance never fails a verdict (the rebalancer corrects it); the structural
// non-resilience is the same warning the operator surfaces as ReducedSiteLossResilience.
type MasterDistribution struct {
	Shards   int
	NumSites int
	// Target is NativeSurvivalTarget(Shards): the max masters a site may hold for its loss to
	// still be recoverable by a native Redis election.
	Target int
	// Floor is TakeoverSurvivalFloor(Shards): the max masters a site may hold for its loss to
	// be recoverable AT ALL (by the operator's fence-safe takeover).
	Floor int
	// Counts is the per-site master count, sorted by site.
	Counts []SiteMasterCount
	// OverFloor lists sites whose master count exceeds Floor, sorted — an actionable
	// availability exposure: that site's loss hits the drift-refusal.
	OverFloor []string
	// OverTarget lists sites whose master count exceeds Target but not Floor, sorted — a
	// recovery downgrade only (takeover instead of a native election), not an exposure.
	OverTarget []string
	// Resilient is the structural verdict SiteLossResilient(Shards, NumSites).
	Resilient bool
}

// EvaluateMasterDistribution builds the D2 balance-invariant health from the shard/site counts
// and the current per-site master distribution. Pure and deterministic. A site currently over
// Target is flagged Imbalanced (informational drift); Resilient is the structural predicate.
func EvaluateMasterDistribution(shards, numSites int, counts []SiteMasterCount) MasterDistribution {
	target, floor := NativeSurvivalTarget(shards), TakeoverSurvivalFloor(shards)
	sorted := make([]SiteMasterCount, len(counts))
	copy(sorted, counts)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Site < sorted[j].Site })
	var overFloor, overTarget []string
	for _, c := range sorted {
		switch {
		case c.Masters > floor:
			overFloor = append(overFloor, c.Site)
		case c.Masters > target:
			overTarget = append(overTarget, c.Site)
		}
	}
	sort.Strings(overFloor)
	sort.Strings(overTarget)
	return MasterDistribution{
		Shards:     shards,
		NumSites:   numSites,
		Target:     target,
		Floor:      floor,
		Counts:     sorted,
		OverFloor:  overFloor,
		OverTarget: overTarget,
		Resilient:  SiteLossResilient(shards, numSites),
	}
}

// SiteSpreadViolation records a shard whose reachable members do not span at
// least two sites — a single site loss would take the whole shard with it,
// defeating the multi-site failure-domain guarantee (design §1/§4). In
// multi-site the shard-colocation invariant inverts: single-site wants a shard's
// master and replica in one shard StatefulSet, multi-site wants them in DIFFERENT
// SITES.
type SiteSpreadViolation struct {
	Shard int
	Sites []string // the (one) site all members share
}

// CheckSiteSpread flags every shard whose reachable members all live in a single
// site. Pure and deterministic (violations sorted by shard). Members whose shard
// or site could not be determined are skipped — there is nothing to assert for
// them. A shard with a single reachable member is still a violation: it has no
// cross-site copy.
func CheckSiteSpread(members []ShardMember) []SiteSpreadViolation {
	// shard -> set of sites among its reachable, well-formed members
	sitesByShard := make(map[int]map[string]bool)
	for _, m := range members {
		if !m.Reachable || m.Shard < 0 || m.Site == "" {
			continue
		}
		if sitesByShard[m.Shard] == nil {
			sitesByShard[m.Shard] = make(map[string]bool)
		}
		sitesByShard[m.Shard][m.Site] = true
	}

	violations := make([]SiteSpreadViolation, 0, len(sitesByShard))
	for shard, sites := range sitesByShard {
		if len(sites) >= 2 {
			continue
		}
		names := make([]string, 0, len(sites))
		for s := range sites {
			names = append(names, s)
		}
		sort.Strings(names)
		violations = append(violations, SiteSpreadViolation{Shard: shard, Sites: names})
	}
	sort.Slice(violations, func(i, j int) bool { return violations[i].Shard < violations[j].Shard })
	return violations
}
