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

package cmd

import (
	"context"
	"fmt"
	"sort"
	"strings"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/multisite"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// verifyMultiSiteCluster aggregates ground truth across ALL sites of a stretched
// cluster and runs the shared checks over the union — fixing the single-context
// false-FAIL (the hub, where the CR lives, hosts zero data pods). It gathers each
// site through the user's own kube-contexts (design §8), unions the per-site pods
// into one IP->site-qualified-key map, and reuses GatherClusterGroundTruth +
// IsHealthy + CheckShardColocation, adding the cross-site placement (failure
// domain) check that is the whole point of multi-site.
func verifyMultiSiteCluster(ctx context.Context, lr *littleredv1alpha1.LittleRed) error {
	gathers, _, err := resolveMultiSiteGathers(ctx, lr)
	if err != nil {
		return err
	}

	fmt.Println("Gathering Cluster Ground Truth across sites...")

	// Site reachability summary + any site we could not gather.
	fmt.Println("\nSites:")
	var siteProblems []string
	for _, sg := range gathers {
		switch {
		case sg.reachable:
			fmt.Printf("  - %s (context %s): reachable, %d pod(s)\n", sg.name, sg.context, len(sg.pods))
		default:
			fmt.Printf("  - %s: [FAIL] UNREACHABLE — %v\n", sg.name, sg.err)
			siteProblems = append(siteProblems, sg.name)
		}
	}

	g := newMultiSiteGatherer(gathers)
	clusterPods := unionSitePods(gathers)
	gt := redisclient.GatherClusterGroundTruth(ctx, g, clusterPods)

	fmt.Printf("\nCluster State: %s\n", gt.ClusterState)
	fmt.Printf("Total Slots Assigned: %d / 16384\n", gt.TotalSlots)

	printMultiSiteNodeStatus(gt)

	if len(gt.GhostNodes) > 0 {
		fmt.Println("\n[!] Ghost Nodes Detected (present in cluster but not in K8s):")
		for _, id := range gt.GhostNodes {
			fmt.Printf("  - %s\n", id)
		}
	}
	if gt.HasPartitions() {
		fmt.Println("\n[!] Network Partitions Detected:")
		for i, p := range gt.Partitions {
			fmt.Printf("  Partition %d: %s\n", i, strings.Join(p, ", "))
		}
	}

	printMultiSiteTopology(gt)

	// Cross-site placement: each shard must span >= 2 sites (master and replica in
	// different sites) — the multi-site inversion of the single-site shard-colocation
	// invariant. A same-site-only shard is a real failure-domain defect (FAIL).
	spread := multisite.CheckSiteSpread(multisite.MembersFromGroundTruth(gt))
	fmt.Println("\nCross-Site Placement:")
	if len(spread) == 0 {
		fmt.Println("  [OK] every shard spans >= 2 sites (a shard's master and replica live in different sites)")
	} else {
		for _, v := range spread {
			fmt.Printf("  [FAIL] shard %d has all members in a single site %v — "+
				"a site loss takes the whole shard (no cross-site copy)\n", v.Shard, v.Sites)
		}
	}

	// Shard-colocation (same-shard) invariant is still meaningful across sites: a
	// replica must follow a master in its OWN shard K (reused unchanged, LR-020).
	colocation := gt.CheckShardColocation()
	for _, v := range colocation {
		fmt.Printf("  [FAIL] Replica %s (shard %d) follows a master in shard %d (%s) — Redis shard spans two shard groups\n",
			v.ReplicaPod, v.ReplicaShard, v.MasterShard, v.MasterPod)
	}

	fmt.Printf("\nSummary:\n")
	expectedNodes := int32(len(clusterPods))
	expectedShards := int32(gt.CountMasters())
	healthy := gt.IsHealthy(expectedNodes, expectedShards)

	if len(siteProblems) > 0 {
		fmt.Printf("  [FAIL] %d site(s) could not be gathered (%s) — verification is incomplete\n",
			len(siteProblems), strings.Join(siteProblems, ", "))
	}

	if !healthy || len(spread) > 0 || len(colocation) > 0 || len(siteProblems) > 0 {
		fmt.Println("  [FAIL] Stretched cluster has topology, placement, or health issues!")
		return fmt.Errorf("multi-site cluster %s/%s has topology, placement, or health issues", lr.Namespace, lr.Name)
	}

	// Degraded (reduced redundancy): a replica whose replication link is down.
	var linkDown []string
	for _, n := range gt.Nodes {
		if n.Role == roleReplica && n.LinkStatus == "down" {
			linkDown = append(linkDown, n.PodName)
		}
	}
	sort.Strings(linkDown)
	if len(linkDown) > 0 {
		for _, pod := range linkDown {
			fmt.Printf("  [WARN] Replica %s link:down — reduced redundancy (may be a transient resync)\n", pod)
		}
		fmt.Printf("  [DEGRADED] Stretched cluster is functional but %d replica link(s) are down; "+
			"not fully healthy.\n", len(linkDown))
		return nil
	}

	fmt.Println("  [OK] Stretched cluster is healthy and consistent across all sites.")
	return nil
}

// multiSiteSiteJSON is one site's reachability in the JSON verify output.
type multiSiteSiteJSON struct {
	Name      string `json:"name"`
	Context   string `json:"context,omitempty"`
	Reachable bool   `json:"reachable"`
	Pods      int    `json:"pods"`
	Error     string `json:"error,omitempty"`
}

// multiSiteNodeJSON is a site-attributed cluster node.
type multiSiteNodeJSON struct {
	Key          string   `json:"key"`
	Site         string   `json:"site"`
	PodIP        string   `json:"podIP,omitempty"`
	NodeID       string   `json:"nodeID"`
	Role         string   `json:"role,omitempty"`
	MasterNodeID string   `json:"masterNodeID,omitempty"`
	Slots        []string `json:"slots,omitempty"`
	LinkStatus   string   `json:"linkStatus,omitempty"`
	Reachable    bool     `json:"reachable"`
}

// multiSiteSpreadJSON is one cross-site placement violation.
type multiSiteSpreadJSON struct {
	Shard int      `json:"shard"`
	Sites []string `json:"sites"`
}

// multiSiteVerifyJSON is the --json shape for a multi-site cluster verify.
type multiSiteVerifyJSON struct {
	Name             string                `json:"name"`
	Namespace        string                `json:"namespace"`
	Mode             string                `json:"mode"`
	MultiSite        bool                  `json:"multiSite"`
	ClusterState     string                `json:"clusterState"`
	TotalSlots       int                   `json:"totalSlots"`
	Sites            []multiSiteSiteJSON   `json:"sites"`
	Nodes            []multiSiteNodeJSON   `json:"nodes"`
	GhostNodes       []string              `json:"ghostNodes"`
	SiteSpreadIssues []multiSiteSpreadJSON `json:"siteSpreadViolations"`
	ColocationIssues []string              `json:"colocationViolations"`
	UnreachableSites []string              `json:"unreachableSites"`
	Healthy          bool                  `json:"healthy"`
}

// verifyMultiSiteClusterJSON gathers the stretched cluster ground truth across
// sites and returns the JSON-serialisable verdict without printing.
func verifyMultiSiteClusterJSON(ctx context.Context, lr *littleredv1alpha1.LittleRed) multiSiteVerifyJSON {
	out := multiSiteVerifyJSON{
		Name:             lr.Name,
		Namespace:        lr.Namespace,
		Mode:             modeCluster,
		MultiSite:        true,
		Sites:            []multiSiteSiteJSON{},
		Nodes:            []multiSiteNodeJSON{},
		GhostNodes:       []string{},
		SiteSpreadIssues: []multiSiteSpreadJSON{},
		ColocationIssues: []string{},
		UnreachableSites: []string{},
	}

	gathers, _, err := resolveMultiSiteGathers(ctx, lr)
	if err != nil {
		out.Healthy = false
		return out
	}

	for _, sg := range gathers {
		entry := multiSiteSiteJSON{Name: sg.name, Context: sg.context, Reachable: sg.reachable, Pods: len(sg.pods)}
		if sg.err != nil {
			entry.Error = sg.err.Error()
		}
		out.Sites = append(out.Sites, entry)
		if !sg.reachable {
			out.UnreachableSites = append(out.UnreachableSites, sg.name)
		}
	}

	g := newMultiSiteGatherer(gathers)
	clusterPods := unionSitePods(gathers)
	gt := redisclient.GatherClusterGroundTruth(ctx, g, clusterPods)

	out.ClusterState = gt.ClusterState
	out.TotalSlots = gt.TotalSlots
	if gt.GhostNodes != nil {
		out.GhostNodes = gt.GhostNodes
	}

	keys := make([]string, 0, len(gt.Nodes))
	for k := range gt.Nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		n := gt.Nodes[k]
		site, _ := multisite.SplitSiteKey(n.PodName)
		masterID := n.MasterNodeID
		if masterID == "-" {
			masterID = ""
		}
		out.Nodes = append(out.Nodes, multiSiteNodeJSON{
			Key: n.PodName, Site: site, PodIP: n.PodIP, NodeID: n.NodeID, Role: n.Role,
			MasterNodeID: masterID, Slots: n.Slots, LinkStatus: n.LinkStatus, Reachable: n.Reachable,
		})
	}

	for _, v := range multisite.CheckSiteSpread(multisite.MembersFromGroundTruth(gt)) {
		out.SiteSpreadIssues = append(out.SiteSpreadIssues, multiSiteSpreadJSON{Shard: v.Shard, Sites: v.Sites})
	}
	for _, v := range gt.CheckShardColocation() {
		out.ColocationIssues = append(out.ColocationIssues,
			fmt.Sprintf("replica %s (shard %d) follows master %s (shard %d)",
				v.ReplicaPod, v.ReplicaShard, v.MasterPod, v.MasterShard))
	}

	expectedNodes := int32(len(clusterPods))
	out.Healthy = gt.IsHealthy(expectedNodes, int32(gt.CountMasters())) &&
		len(out.SiteSpreadIssues) == 0 && len(out.ColocationIssues) == 0 && len(out.UnreachableSites) == 0
	return out
}

// printMultiSiteNodeStatus renders each node with its owning site attributed.
func printMultiSiteNodeStatus(gt *redisclient.ClusterGroundTruth) {
	fmt.Println("\nNode Status:")
	keys := make([]string, 0, len(gt.Nodes))
	for k := range gt.Nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		node := gt.Nodes[k]
		site, _ := multisite.SplitSiteKey(node.PodName)
		if !node.Reachable {
			fmt.Printf("  - Pod %s [site %s]: [!] UNREACHABLE\n", node.PodName, site)
			continue
		}
		details := ""
		if node.Role == roleMaster {
			details = fmt.Sprintf("slots:%s", strings.Join(node.Slots, ","))
		} else {
			details = fmt.Sprintf("following:%s, link:%s", node.MasterNodeID, node.LinkStatus)
		}
		fmt.Printf("  - Pod %s [site %s]: role:%s, id:%s, %s\n", node.PodName, site, node.Role, node.NodeID, details)
	}
}

// printMultiSiteTopology renders the master->replica tree with per-node site
// attribution, making cross-site placement visible at a glance.
func printMultiSiteTopology(gt *redisclient.ClusterGroundTruth) {
	fmt.Println("\nCluster Topology (site-attributed):")
	masters := make([]*redisclient.ClusterNodeState, 0)
	for _, n := range gt.Nodes {
		if n.Role == roleMaster {
			masters = append(masters, n)
		}
	}
	sort.Slice(masters, func(i, j int) bool { return masters[i].PodName < masters[j].PodName })

	for _, m := range masters {
		mSite, _ := multisite.SplitSiteKey(m.PodName)
		fmt.Printf("  Master: %s [site %s] (%s)\n", m.PodName, mSite, m.NodeID)
		if len(m.Slots) > 0 {
			fmt.Printf("    Slots: %s\n", strings.Join(m.Slots, " "))
		} else {
			fmt.Printf("    [!] NO SLOTS ASSIGNED\n")
		}
		replicas := make([]*redisclient.ClusterNodeState, 0)
		for _, r := range gt.Nodes {
			if r.Role == roleReplica && r.MasterNodeID == m.NodeID {
				replicas = append(replicas, r)
			}
		}
		sort.Slice(replicas, func(i, j int) bool { return replicas[i].PodName < replicas[j].PodName })
		for _, r := range replicas {
			rSite, _ := multisite.SplitSiteKey(r.PodName)
			crossSite := ""
			if rSite != mSite {
				crossSite = ", cross-site"
			} else {
				crossSite = ", SAME-SITE"
			}
			fmt.Printf("    └── Replica: %s [site %s] (%s, link:%s%s)\n", r.PodName, rSite, r.NodeID, r.LinkStatus, crossSite)
		}
	}
}
