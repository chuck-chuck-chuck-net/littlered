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
	"path/filepath"
	"strings"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
)

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
				"-c", "redis", "--tail=2000", "--timestamps")
			writeFile(siteDir, fmt.Sprintf("pod-%s-redis.log", pod.Name), logs)

			// Live cluster state via exec.
			clusterCmd := authShell + ` redis-cli $AUTH INFO replication` +
				` && echo "---" && redis-cli $AUTH CLUSTER NODES` +
				` && echo "---" && redis-cli $AUTH CLUSTER INFO`
			out, _, _ := k8s.Exec(ctx, sg.coreClient, sg.config, lr.Namespace, pod.Name, "redis",
				[]string{"sh", "-c", clusterCmd})
			writeFile(siteDir, fmt.Sprintf("redis-%s-cluster-state.txt", pod.Name), out)
		}
	}
}
