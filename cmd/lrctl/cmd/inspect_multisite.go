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
	"strings"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/littlered-operator/littlered-operator/internal/cli/multisite"
)

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
