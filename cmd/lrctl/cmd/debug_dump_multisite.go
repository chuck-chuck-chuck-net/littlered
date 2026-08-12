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
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/littlered-operator/littlered-operator/internal/cli/multisite"
)

// collectMultiSiteArtifactsTiered routes debug-dump's multi-site collection by
// tier. Tier 2 (default) runs the operator's own hub gather Job and saves the
// site-attributed ground truth as JSON — operator-mediated, so it works even
// when the workstation has no site access (and even when the operator pod is
// unhealthy). Tier 3 (--direct) does the full per-site log/resource/exec
// collection through the user's kube-contexts. Tier 1 (--from-status) leaves the
// already-collected CR+status as the artifact.
func collectMultiSiteArtifactsTiered(
	ctx context.Context, dir string, k8sClient client.Client, coreClient *kubernetes.Clientset,
	lr *littleredv1alpha1.LittleRed,
) {
	tier, err := multisite.SelectTier("debug-dump", currentTierFlags())
	if err != nil {
		writeFile(dir, "multisite-error.txt", err.Error())
		return
	}
	switch tier {
	case multisite.TierDirect:
		collectMultiSiteArtifacts(ctx, dir, lr)
	case multisite.TierStatus:
		writeFile(dir, "multisite-note.txt",
			"tier 1 (--from-status): only the hub-side CR and status were collected.\n"+
				"Re-run with --direct for full per-site logs/resources/exec, or without --from-status for the live hub gather.\n")
	default: // tier 2 (live via hub Job)
		fmt.Println("  Collecting multi-site ground truth via hub gather Job (tier 2)...")
		result, gerr := gatherViaHubJob(ctx, k8sClient, coreClient, lr, operatorNamespaceFlag, operatorDeployment)
		if gerr != nil {
			writeFile(dir, "multisite-gather-error.txt", gerr.Error())
			return
		}
		raw, merr := json.MarshalIndent(result, "", "  ")
		if merr != nil {
			writeFile(dir, "multisite-gather-error.txt", merr.Error())
			return
		}
		writeFile(dir, "multisite-gather.json", string(raw))
		writeFile(dir, "multisite-note.txt",
			"tier 2 (live via hub Job): multisite-gather.json holds the operator's site-attributed ground truth.\n"+
				"For full per-site pod logs / k8s resources / raw redis-cli, re-run with --direct.\n")
	}
}

// collectMultiSiteArtifacts collects pod logs, live Redis cluster state and
// Kubernetes resources from EVERY site of a stretched cluster, into per-site
// subdirectories site-<name>/. Each site is reached through its own kube-context
// (design §8); an unreachable/unresolved site is noted and skipped.
func collectMultiSiteArtifacts(ctx context.Context, dir string, lr *littleredv1alpha1.LittleRed) {
	fmt.Println("  Collecting multi-site artifacts (per site)...")
	gathers, resolution, err := resolveMultiSiteGathers(ctx, lr)
	if err != nil {
		writeFile(dir, "multisite-error.txt", fmt.Sprintf("failed to resolve site gathers: %v", err))
		return
	}

	// Record the site->context resolution used, for the reader.
	var summary strings.Builder
	summary.WriteString("Site context resolution:\n")
	for _, u := range resolution.Unresolved {
		fmt.Fprintf(&summary, "  %s: UNRESOLVED — %s\n", u.Site, u.Reason)
	}
	for site, cxt := range resolution.Resolved {
		fmt.Fprintf(&summary, "  %s -> context %s\n", site, cxt)
	}
	writeFile(dir, "multisite-sites.txt", summary.String())

	authShell := `AUTH=""; [ -n "$REDIS_PASSWORD" ] && AUTH="-a $REDIS_PASSWORD --no-auth-warning";`

	for i := range gathers {
		sg := &gathers[i]
		siteDir := filepath.Join(dir, "site-"+sg.name)
		if !sg.reachable {
			writeFile(siteDir, "unreachable.txt", fmt.Sprintf("site %s not gathered: %v", sg.name, sg.err))
			continue
		}
		fmt.Printf("    site %s (context %s): %d pod(s)\n", sg.name, sg.context, len(sg.pods))

		// K8s resources via the site's context.
		writeFile(siteDir, "pods.txt",
			kubectlContext(sg.context, "get", "pods", "-n", lr.Namespace,
				"-l", "app.kubernetes.io/instance="+lr.Name, "-o", "wide",
				"-L", "redis.chuck-chuck-chuck.net/shard", "-L", "redis.chuck-chuck-chuck.net/role"))
		writeFile(siteDir, "statefulsets.yaml",
			kubectlContext(sg.context, "get", "statefulsets", "-n", lr.Namespace,
				"-l", "app.kubernetes.io/instance="+lr.Name, "-o", "yaml"))
		writeFile(siteDir, "services.yaml",
			kubectlContext(sg.context, "get", "services", "-n", lr.Namespace,
				"-l", "app.kubernetes.io/instance="+lr.Name, "-o", "yaml"))
		writeFile(siteDir, "events.txt",
			kubectlContext(sg.context, "get", "events", "-n", lr.Namespace, "--sort-by=.lastTimestamp"))

		for _, pod := range sg.cCtx.RedisPods {
			// Pod logs (current + previous).
			logs := kubectlContext(sg.context, "logs", pod.Name, "-n", lr.Namespace,
				"-c", containerRedis, "--tail=2000", "--timestamps")
			writeFile(siteDir, fmt.Sprintf("pod-%s-redis.log", pod.Name), logs)

			// Live cluster state via exec.
			clusterCmd := authShell + ` redis-cli $AUTH INFO replication` +
				` && echo "---" && redis-cli $AUTH CLUSTER NODES` +
				` && echo "---" && redis-cli $AUTH CLUSTER INFO`
			out, _, _ := k8s.Exec(ctx, sg.coreClient, sg.config, lr.Namespace, pod.Name, containerRedis,
				[]string{"sh", "-c", clusterCmd})
			writeFile(siteDir, fmt.Sprintf("redis-%s-cluster-state.txt", pod.Name), out)
		}
	}
}
