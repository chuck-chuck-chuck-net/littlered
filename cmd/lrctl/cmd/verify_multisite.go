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
	"time"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	"github.com/chuck-chuck-chuck-net/littlered/internal/cli/multisite"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// verifyMultiSite is the tier-aware entry point for `lrctl verify` on a
// multi-site cluster. It resolves the transport (tier 2 live-via-hub-Job by
// default; tier 1 --from-status; tier 3 --direct/--site-context) and renders the
// same cross-site verdict over whichever source produced the ground truth. The
// selected tier is announced in the output so the reader knows how the data was
// obtained.
func verifyMultiSite(
	ctx context.Context, k8sClient client.Client, coreClient *kubernetes.Clientset, lr *littleredv1alpha1.LittleRed,
) error {
	tier, err := multisite.SelectTier("verify", currentTierFlags())
	if err != nil {
		return err
	}
	fmt.Printf("Transport: %s\n", tier.Label())
	switch tier {
	case multisite.TierStatus:
		return verifyMultiSiteFromStatus(lr)
	case multisite.TierDirect:
		return verifyMultiSiteDirect(ctx, lr)
	default: // TierLive
		return verifyMultiSiteLive(ctx, k8sClient, coreClient, lr)
	}
}

// verifyMultiSiteDirect is tier 3: gather every site through the user's own
// per-site kube-contexts and render. This was the M5 default; it is now the
// explicit power-user fallback (e.g. the operator is gone) selected by --direct
// or --site-context. It unions the per-site pods into one IP->site-qualified-key
// map and reuses GatherClusterGroundTruth + the shared checks.
func verifyMultiSiteDirect(ctx context.Context, lr *littleredv1alpha1.LittleRed) error {
	gathers, _, err := resolveMultiSiteGathers(ctx, lr)
	if err != nil {
		return err
	}

	fmt.Println("Gathering Cluster Ground Truth across sites (direct contexts)...")

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
	return renderMultiSiteVerdict(lr, gt, int32(len(clusterPods)), siteProblems)
}

// verifyMultiSiteLive is tier 2: obtain the ground truth from a throwaway hub
// Job (the operator gathers every site and returns JSON over the control path),
// then render exactly as the direct path does — no site access on the
// workstation. This is the default for verify.
func verifyMultiSiteLive(
	ctx context.Context, k8sClient client.Client, coreClient *kubernetes.Clientset, lr *littleredv1alpha1.LittleRed,
) error {
	result, err := gatherViaHubJob(ctx, k8sClient, coreClient, lr, operatorNamespace, operatorDeployment)
	if err != nil {
		return err
	}
	fmt.Printf("(gathered %s)\n", result.GatheredAt.Format(time.RFC3339))

	fmt.Println("\nSites:")
	var siteProblems []string
	reachablePods := 0
	for _, s := range result.Sites {
		if s.Reachable {
			fmt.Printf("  - %s: reachable, %d pod(s)\n", s.Name, s.Pods)
			reachablePods += s.Pods
		} else {
			fmt.Printf("  - %s: [FAIL] UNREACHABLE — %s\n", s.Name, s.Error)
			siteProblems = append(siteProblems, s.Name)
		}
	}
	return renderMultiSiteVerdict(lr, result.GroundTruth, int32(reachablePods), siteProblems)
}

// verifyMultiSiteFromStatus is tier 1: render the cross-site verdict from the
// view the operator published to status.multiSite. Needs only get littlered on
// the hub — no site access, no gather. Freshness is the last reconcile.
func verifyMultiSiteFromStatus(lr *littleredv1alpha1.LittleRed) error {
	ms := lr.Status.MultiSite
	if ms == nil || ms.LastReconcileTime == nil {
		fmt.Println("\n[!] The operator has not yet published a cross-site view for this instance.")
		fmt.Println("    (Is it a fresh/initializing instance? Try tier 2 without --from-status for a live gather.)")
		return fmt.Errorf("no published cross-site view in status for %s/%s", lr.Namespace, lr.Name)
	}
	age := time.Since(ms.LastReconcileTime.Time).Round(time.Second)
	fmt.Printf("(as of last reconcile %s ago, at %s)\n", age, ms.LastReconcileTime.Format(time.RFC3339))

	fmt.Printf("\nCluster State: %s\n", valueOrNone(ms.ClusterState))
	fmt.Printf("Total Slots Assigned: %d / 16384\n", ms.SlotsAssigned)

	var unreachable []string
	fmt.Println("\nSites:")
	for _, s := range ms.Sites {
		reach := "reachable"
		if !s.Reachable {
			reach = "UNREACHABLE"
			unreachable = append(unreachable, s.Name)
		}
		pods := ""
		if s.Pods != nil {
			pods = fmt.Sprintf(", pods %d/%d Ready", s.Pods.Ready, s.Pods.Total)
		}
		fmt.Printf("  - %s: %s%s\n", s.Name, reach, pods)
	}

	shardViews := make([]multisite.ShardView, 0, len(ms.Shards))
	fmt.Println("\nCross-Site Placement (per shard):")
	for _, sh := range ms.Shards {
		marker := "[OK] cross-site"
		if !sh.CrossSite {
			marker = "[FAIL] single-site"
		}
		link := ""
		if sh.LinkDown {
			link = ", [WARN] a replica link is down"
		}
		fmt.Printf("  - shard %d: master@%s, replicas@%v %s%s\n",
			sh.Shard, valueOrNone(sh.MasterSite), sh.ReplicaSites, marker, link)
		shardViews = append(shardViews, multisite.ShardView{
			Shard: sh.Shard, MasterSite: sh.MasterSite, ReplicaSites: sh.ReplicaSites,
			CrossSite: sh.CrossSite, LinkDown: sh.LinkDown,
		})
	}

	// Master-per-site distribution + balance-invariant health (ADR-012 D2), from the
	// operator's published counts. Informational — never changes the verdict.
	msCounts := make([]multisite.SiteMasterCount, 0, len(ms.MasterCounts))
	for _, mc := range ms.MasterCounts {
		msCounts = append(msCounts, multisite.SiteMasterCount{Site: mc.Site, Masters: int(mc.Masters)})
	}
	printMasterDistribution(multisite.EvaluateMasterDistribution(len(ms.Shards), len(ms.Sites), msCounts))

	v := multisite.StatusVerdict(ms.ClusterState, int(ms.SlotsAssigned), shardViews, unreachable)
	fmt.Printf("\nSummary:\n")
	for _, r := range v.Reasons {
		marker := "[FAIL]"
		if strings.Contains(r, "link:down") {
			marker = tokenWarn
		}
		fmt.Printf("  %s %s\n", marker, r)
	}
	switch {
	case !v.OK:
		fmt.Println("  [FAIL] Stretched cluster has topology, placement, or health issues (per published status)!")
		return fmt.Errorf("multi-site cluster %s/%s has issues (from operator status)", lr.Namespace, lr.Name)
	case v.Degraded:
		fmt.Println("  [DEGRADED] Stretched cluster is functional but not fully healthy (per published status).")
		return nil
	default:
		fmt.Println("  [OK] Stretched cluster is healthy and cross-site consistent (per published status).")
		return nil
	}
}

// renderMultiSiteVerdict prints the cluster state, node status, ghosts,
// partitions, topology, cross-site placement, colocation and health summary from
// a gathered ground truth — shared by tiers 2 and 3, which differ only in how
// the ground truth was obtained.
func renderMultiSiteVerdict(
	lr *littleredv1alpha1.LittleRed, gt *redisclient.ClusterGroundTruth, expectedNodes int32, siteProblems []string,
) error {
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

	// Master-per-site distribution + balance-invariant health (ADR-012 D2). Informational —
	// [IMBALANCED] drift (the rebalancer corrects it) and the structural reduced-resilience
	// [WARN] never change the verdict/exit code below. numSites counts every site the
	// deployment spans (reachable nodes' sites + unreachable ones), so the structural verdict
	// is not skewed by a site being down this pass.
	numSites := distinctMultiSiteSites(gt, siteProblems)
	printMasterDistribution(multisite.EvaluateMasterDistribution(gt.CountMasters(), numSites,
		multisite.MasterCountsFromGroundTruth(gt)))

	fmt.Printf("\nSummary:\n")
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
	Tier             string                `json:"tier"`
	AsOf             string                `json:"asOf,omitempty"`
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

func newMultiSiteVerifyJSON(lr *littleredv1alpha1.LittleRed, tier multisite.Tier) multiSiteVerifyJSON {
	return multiSiteVerifyJSON{
		Name:             lr.Name,
		Namespace:        lr.Namespace,
		Mode:             modeCluster,
		MultiSite:        true,
		Tier:             tier.Label(),
		Sites:            []multiSiteSiteJSON{},
		Nodes:            []multiSiteNodeJSON{},
		GhostNodes:       []string{},
		SiteSpreadIssues: []multiSiteSpreadJSON{},
		ColocationIssues: []string{},
		UnreachableSites: []string{},
	}
}

// verifyMultiSiteJSON returns the JSON-serialisable verdict, choosing the
// transport tier exactly like the text path.
func verifyMultiSiteJSON(
	ctx context.Context, k8sClient client.Client, coreClient *kubernetes.Clientset, lr *littleredv1alpha1.LittleRed,
) multiSiteVerifyJSON {
	tier, err := multisite.SelectTier("verify", currentTierFlags())
	if err != nil {
		out := newMultiSiteVerifyJSON(lr, multisite.TierLive)
		out.Healthy = false
		return out
	}
	switch tier {
	case multisite.TierStatus:
		return verifyMultiSiteJSONFromStatus(lr)
	case multisite.TierDirect:
		return verifyMultiSiteJSONDirect(ctx, lr)
	default:
		return verifyMultiSiteJSONLive(ctx, k8sClient, coreClient, lr)
	}
}

// verifyMultiSiteJSONDirect is the tier-3 JSON builder (direct site contexts).
func verifyMultiSiteJSONDirect(ctx context.Context, lr *littleredv1alpha1.LittleRed) multiSiteVerifyJSON {
	out := newMultiSiteVerifyJSON(lr, multisite.TierDirect)
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
	fillMultiSiteVerifyJSONFromGT(&out, gt, int32(len(clusterPods)))
	return out
}

// verifyMultiSiteJSONLive is the tier-2 JSON builder (live via hub Job).
func verifyMultiSiteJSONLive(
	ctx context.Context, k8sClient client.Client, coreClient *kubernetes.Clientset, lr *littleredv1alpha1.LittleRed,
) multiSiteVerifyJSON {
	out := newMultiSiteVerifyJSON(lr, multisite.TierLive)
	result, err := gatherViaHubJob(ctx, k8sClient, coreClient, lr, operatorNamespace, operatorDeployment)
	if err != nil {
		out.Healthy = false
		out.UnreachableSites = append(out.UnreachableSites, err.Error())
		return out
	}
	out.AsOf = result.GatheredAt.Format(time.RFC3339)
	reachablePods := 0
	for _, s := range result.Sites {
		out.Sites = append(out.Sites, multiSiteSiteJSON{Name: s.Name, Reachable: s.Reachable, Pods: s.Pods, Error: s.Error})
		if s.Reachable {
			reachablePods += s.Pods
		} else {
			out.UnreachableSites = append(out.UnreachableSites, s.Name)
		}
	}
	fillMultiSiteVerifyJSONFromGT(&out, result.GroundTruth, int32(reachablePods))
	return out
}

// fillMultiSiteVerifyJSONFromGT populates the node/placement/colocation/health
// fields from a gathered ground truth — shared by tiers 2 and 3.
func fillMultiSiteVerifyJSONFromGT(out *multiSiteVerifyJSON, gt *redisclient.ClusterGroundTruth, expectedNodes int32) {
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
	out.Healthy = gt.IsHealthy(expectedNodes, int32(gt.CountMasters())) &&
		len(out.SiteSpreadIssues) == 0 && len(out.ColocationIssues) == 0 && len(out.UnreachableSites) == 0
}

// verifyMultiSiteJSONFromStatus is the tier-1 JSON builder (published status).
func verifyMultiSiteJSONFromStatus(lr *littleredv1alpha1.LittleRed) multiSiteVerifyJSON {
	out := newMultiSiteVerifyJSON(lr, multisite.TierStatus)
	ms := lr.Status.MultiSite
	if ms == nil || ms.LastReconcileTime == nil {
		out.Healthy = false
		return out
	}
	out.AsOf = ms.LastReconcileTime.Format(time.RFC3339)
	out.ClusterState = ms.ClusterState
	out.TotalSlots = int(ms.SlotsAssigned)
	var unreachable []string
	for _, s := range ms.Sites {
		entry := multiSiteSiteJSON{Name: s.Name, Reachable: s.Reachable}
		if s.Pods != nil {
			entry.Pods = int(s.Pods.Ready)
		}
		out.Sites = append(out.Sites, entry)
		if !s.Reachable {
			out.UnreachableSites = append(out.UnreachableSites, s.Name)
			unreachable = append(unreachable, s.Name)
		}
	}
	shardViews := make([]multisite.ShardView, 0, len(ms.Shards))
	for _, sh := range ms.Shards {
		if !sh.CrossSite {
			out.SiteSpreadIssues = append(out.SiteSpreadIssues,
				multiSiteSpreadJSON{Shard: sh.Shard, Sites: []string{sh.MasterSite}})
		}
		shardViews = append(shardViews, multisite.ShardView{
			Shard: sh.Shard, MasterSite: sh.MasterSite, ReplicaSites: sh.ReplicaSites,
			CrossSite: sh.CrossSite, LinkDown: sh.LinkDown,
		})
	}
	out.Healthy = multisite.StatusVerdict(ms.ClusterState, int(ms.SlotsAssigned), shardViews, unreachable).OK
	return out
}

// printMasterDistribution renders the ADR-012 D2 master-per-site distribution and the live
// health of both balance thresholds: each site's slot-serving-master count, an [OVER FLOOR]
// marker when it exceeds the takeover floor (floor(K/2) — that site's loss could not be fenced,
// an actionable availability exposure), an [OVER TARGET] marker when it exceeds only the native
// target (ceil(K/2)-1 — its loss recovers by operator takeover instead of a Redis election, a
// downgrade), and the structural reduced-resilience [WARN] when no placement can auto-survive an
// arbitrary single-site loss. All of it is INFORMATIONAL — drift is transient and the operator's
// rebalancer corrects it, and reduced resilience is a surfaced warning, not a refusal — so none
// of it changes the verify verdict or exit code.
func printMasterDistribution(md multisite.MasterDistribution) {
	fmt.Printf("\nMaster Distribution (%d shards; native target %d, takeover floor %d master(s)/site, ADR-012 D2):\n",
		md.Shards, md.Target, md.Floor)
	overFloor := make(map[string]bool, len(md.OverFloor))
	for _, s := range md.OverFloor {
		overFloor[s] = true
	}
	overTarget := make(map[string]bool, len(md.OverTarget))
	for _, s := range md.OverTarget {
		overTarget[s] = true
	}
	if len(md.Counts) == 0 {
		fmt.Println("  (no slot-serving masters observed)")
	}
	for _, c := range md.Counts {
		marker := tokenOK
		switch {
		case overFloor[c.Site]:
			marker = fmt.Sprintf("[OVER FLOOR] (exceeds takeover floor %d)", md.Floor)
		case overTarget[c.Site]:
			marker = fmt.Sprintf("[OVER TARGET] (exceeds native target %d)", md.Target)
		}
		fmt.Printf("  - site %s: %d master(s) %s\n", c.Site, c.Masters, marker)
	}
	switch {
	case !md.Resilient:
		fmt.Printf("  [WARN] reduced site-loss resilience: %d sites x %d (floor(%d/2)) = %d < %d shards — "+
			"the topology cannot auto-survive an arbitrary single-site loss; the heavier-master site's loss "+
			"needs manual or multiSite.allowUnsafeSiteTakeover recovery\n",
			md.NumSites, md.Floor, md.Shards, md.NumSites*md.Floor, md.Shards)
	case len(md.OverFloor) > 0:
		fmt.Println("  [WARN] master drift past the takeover floor: losing that site would leave the lost side " +
			"holding a master majority, so the takeover cannot be fenced safely and the loss would need manual " +
			"or multiSite.allowUnsafeSiteTakeover recovery. The operator's rebalancer corrects this with a " +
			"graceful failover (no data at risk)")
	case len(md.OverTarget) > 0:
		fmt.Println("  [OK] recoverable, but not natively: a site exceeds the native target, so its loss recovers " +
			"by operator takeover rather than a Redis election. The rebalancer corrects it where the topology allows")
	default:
		fmt.Println("  [OK] balanced: every site is at or below the native target; any single-site loss recovers " +
			"by a native Redis election")
	}
}

// distinctMultiSiteSites counts the sites the deployment spans: every site attributed to a
// gathered node plus any additional (e.g. unreachable) sites the caller knows about. Used to
// derive the structural resilience denominator so a site being down this pass does not skew it.
func distinctMultiSiteSites(gt *redisclient.ClusterGroundTruth, extra []string) int {
	set := make(map[string]bool)
	for _, n := range gt.Nodes {
		if site, _ := multisite.SplitSiteKey(n.PodName); site != "" {
			set[site] = true
		}
	}
	for _, s := range extra {
		set[s] = true
	}
	return len(set)
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
