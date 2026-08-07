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

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/littlered-operator/littlered-operator/internal/cli/multisite"
)

// inspectMultiSiteTiered is the tier-aware entry for `lrctl inspect` on a
// multi-site cluster. Default (tier 2) renders the site-attributed topology from
// a hub gather Job — operator-mediated, no workstation site access. --direct
// (tier 3) does the full per-pod raw exec dump. --from-status (tier 1) renders
// the operator's published node view. Full raw per-pod command output is a
// tier-3-only capability (it needs site exec); the tier is announced so the
// reader knows which they got.
func inspectMultiSiteTiered(
	ctx context.Context, k8sClient client.Client, coreClient *kubernetes.Clientset, lr *littleredv1alpha1.LittleRed,
) (inspectJSON, error) {
	tier, err := multisite.SelectTier("inspect", currentTierFlags())
	if err != nil {
		return inspectJSON{}, err
	}
	if !jsonOutput {
		fmt.Printf("Transport: %s\n", tier.Label())
	}
	switch tier {
	case multisite.TierDirect:
		return inspectMultiSite(ctx, lr)
	case multisite.TierStatus:
		return inspectMultiSiteFromStatus(lr), nil
	default:
		return inspectMultiSiteLive(ctx, k8sClient, coreClient, lr)
	}
}

// inspectMultiSiteLive is tier 2: render the site-attributed topology from a hub
// gather Job. It does not reproduce per-pod raw redis-cli output (that needs site
// exec — use --direct); it presents the structured ground truth the operator saw.
func inspectMultiSiteLive(
	ctx context.Context, k8sClient client.Client, coreClient *kubernetes.Clientset, lr *littleredv1alpha1.LittleRed,
) (inspectJSON, error) {
	res := inspectJSON{Name: lr.Name, Namespace: lr.Namespace, Mode: modeCluster}
	result, err := gatherViaHubJob(ctx, k8sClient, coreClient, lr, operatorNamespace, operatorDeployment)
	if err != nil {
		return res, err
	}
	gt := result.GroundTruth

	keys := make([]string, 0, len(gt.Nodes))
	for k := range gt.Nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		n := gt.Nodes[k]
		res.Redis = append(res.Redis, redisPodJSON{
			Pod: n.PodName, IP: n.PodIP,
			ClusterInfo: map[string]string{
				"role":         n.Role,
				"masterNodeID": n.MasterNodeID,
				"slots":        strings.Join(n.Slots, ","),
				"linkStatus":   n.LinkStatus,
			},
		})
	}

	if !jsonOutput {
		fmt.Printf("Deep Inspect (Multi-Site, live via hub Job): %s/%s (%d sites, gathered %s)\n",
			res.Namespace, res.Name, len(lr.Spec.MultiSite.Sites), result.GatheredAt.Format("2006-01-02T15:04:05Z07:00"))
		fmt.Println(strings.Repeat("-", 40))
		printMultiSiteNodeStatus(gt)
		printMultiSiteTopology(gt)
		fmt.Println("\n(For full per-pod raw redis-cli output, re-run with --direct.)")
	}
	return res, nil
}

// inspectMultiSiteFromStatus is tier 1: render the node view the operator
// published to status (site-attributed), no gather.
func inspectMultiSiteFromStatus(lr *littleredv1alpha1.LittleRed) inspectJSON {
	res := inspectJSON{Name: lr.Name, Namespace: lr.Namespace, Mode: modeCluster}
	if lr.Status.Cluster != nil {
		for _, n := range lr.Status.Cluster.Nodes {
			res.Redis = append(res.Redis, redisPodJSON{
				Pod: n.PodName,
				ClusterInfo: map[string]string{
					"role":         n.Role,
					"masterNodeID": n.MasterNodeID,
					"slots":        n.SlotRanges,
					"nodeID":       n.NodeID,
				},
			})
		}
	}
	if !jsonOutput {
		fmt.Printf("Deep Inspect (Multi-Site, from operator status): %s/%s\n", res.Namespace, res.Name)
		if lr.Status.MultiSite != nil && lr.Status.MultiSite.LastReconcileTime != nil {
			fmt.Printf("(as of last reconcile at %s)\n", lr.Status.MultiSite.LastReconcileTime.Format(timeFormat))
		}
		fmt.Println(strings.Repeat("-", 40))
		for _, r := range res.Redis {
			fmt.Printf("Node %s: role=%s, master=%s, slots=%s\n",
				r.Pod, valueOrNone(r.ClusterInfo["role"]),
				valueOrNone(r.ClusterInfo["masterNodeID"]), valueOrNone(r.ClusterInfo["slots"]))
		}
	}
	return res
}

// inspectMultiSite deep-inspects every pod of a stretched cluster, executing the
// cluster diagnostic commands in each pod through its OWNING site's Kubernetes
// API (design §8: sites are reached over the user's own kube-contexts, not the
// operator's hub-pod-only site Secrets). Output is site-attributed. Returns the
// aggregated JSON payload for --json.
func inspectMultiSite(ctx context.Context, lr *littleredv1alpha1.LittleRed) (inspectJSON, error) {
	res := inspectJSON{Name: lr.Name, Namespace: lr.Namespace, Mode: modeCluster}

	gathers, _, err := resolveMultiSiteGathers(ctx, lr)
	if err != nil {
		return res, err
	}

	if !jsonOutput {
		fmt.Printf("Deep Inspect (Multi-Site): %s/%s (Mode: cluster, %d sites)\n",
			res.Namespace, res.Name, len(lr.Spec.MultiSite.Sites))
		fmt.Println(strings.Repeat("-", 40))
	}

	for i := range gathers {
		sg := &gathers[i]
		if !jsonOutput {
			fmt.Printf("Site: %s (context %s)\n", sg.name, sg.context)
		}
		if !sg.reachable {
			if !jsonOutput {
				fmt.Printf("  [!] UNREACHABLE: %v\n\n", sg.err)
			}
			continue
		}
		for _, pod := range sg.cCtx.RedisPods {
			key := multisite.SiteQualifiedKey(sg.name, pod.Name)
			entry := redisPodJSON{Pod: key, IP: pod.Status.PodIP}
			cmdArgs := []string{"sh", "-c", "redis-cli cluster nodes && echo --- && redis-cli cluster info"}
			stdout, stderr, execErr := k8s.Exec(
				ctx, sg.coreClient, sg.config, sg.cCtx.Namespace, pod.Name, sg.cCtx.RedisContainer, cmdArgs)
			if execErr != nil {
				entry.Error = fmt.Sprintf("%v (stderr: %q)", execErr, stderr)
			} else {
				entry.raw = stdout
				parts := strings.SplitN(stdout, "\n---\n", 2)
				entry.ClusterNodes = parseClusterNodesJSON(parts[0])
				if len(parts) > 1 {
					entry.ClusterInfo = parseInfoKV(parts[1])
				}
			}
			res.Redis = append(res.Redis, entry)

			if !jsonOutput {
				fmt.Printf("  Redis Pod: %s (IP: %s)\n", key, pod.Status.PodIP)
				if entry.Error != "" {
					fmt.Printf("    [!] Error: %s\n", entry.Error)
				} else {
					for line := range strings.SplitSeq(strings.TrimSpace(entry.raw), "\n") {
						fmt.Printf("    %s\n", line)
					}
				}
				fmt.Println()
			}
		}
	}
	return res, nil
}
