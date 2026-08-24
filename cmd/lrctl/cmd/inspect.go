package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/littlered-operator/littlered-operator/internal/cli/discovery"
	clifailover "github.com/littlered-operator/littlered-operator/internal/cli/failover"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/spf13/cobra"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect [name]",
	Short: "Perform a deep-dive diagnostic of a Redis instance (omit name to inspect all in namespace)",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		k8sClient, coreClient, config, defaultNS, err := k8s.NewClient(kubeconfig)
		if err != nil {
			return err
		}

		targetNS := namespace
		if targetNS == "" {
			targetNS = defaultNS
		}

		ctx := context.Background()

		if unmanaged && len(args) == 0 {
			return fmt.Errorf("a resource name is required when using --unmanaged")
		}

		targets, err := resolveTargets(ctx, k8sClient, args, targetNS)
		if err != nil {
			return err
		}
		if len(targets) == 0 {
			if allNamespaces {
				fmt.Println("No LittleRed resources found in any namespace")
			} else {
				fmt.Printf("No LittleRed resources found in namespace %q\n", targetNS)
			}
			return nil
		}

		var jsonResults []inspectJSON
		errCount := 0
		textIdx := 0

		for _, key := range targets {
			cCtx, err := discovery.GetContext(ctx, k8sClient, key.Namespace, key.Name, kind, unmanaged)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %s/%s: %v\n", key.Namespace, key.Name, err)
				errCount++
				continue
			}

			res := inspectJSON{Name: key.Name, Namespace: key.Namespace, Mode: cCtx.Mode}

			// ── collect sentinel pods ───────────────────────────────────────
			for _, pod := range cCtx.SentinelPods {
				entry := sentinelPodJSON{Pod: pod.Name, IP: pod.Status.PodIP}
				cmdArgs := redisCliArgs("-p", "26379", modeSentinel, roleMaster, masterNameOf(cCtx))
				stdout, stderr, err := k8s.Exec(ctx, coreClient, config, cCtx.Namespace, pod.Name, cCtx.SentinelContainer, cmdArgs)
				if err != nil {
					entry.Error = fmt.Sprintf("%v (stderr: %q)", err, stderr)
				} else {
					entry.raw = stdout
					entry.MasterInfo = parseAlternatingKV(stdout)
				}
				res.Sentinels = append(res.Sentinels, entry)
			}

			// ── collect redis pods ──────────────────────────────────────────
			for _, pod := range cCtx.RedisPods {
				entry := redisPodJSON{Pod: pod.Name, IP: pod.Status.PodIP}
				if cCtx.Mode == modeFailover {
					// The assignment annotations ARE the operator's intent
					// record (ADR-011) — surface them alongside the live view.
					entry.Assignment = map[string]string{
						"assigned-role":      pod.Annotations[clifailover.AnnotationAssignedRole],
						"assigned-master-ip": pod.Annotations[clifailover.AnnotationAssignedMasterIP],
						"assignment-epoch":   pod.Annotations[clifailover.AnnotationAssignmentEpoch],
						"role-label":         pod.Labels[clifailover.LabelRole],
					}
				}
				var cmdArgs []string
				if cCtx.Mode == modeCluster {
					cmdArgs = redisCliChainArgs(
						[]string{clusterSubcommand, "nodes"},
						[]string{clusterSubcommand, infoSubcommand},
					)
				} else {
					cmdArgs = redisCliArgs(infoSubcommand, "replication")
				}
				stdout, stderr, err := k8s.Exec(ctx, coreClient, config, cCtx.Namespace, pod.Name, cCtx.RedisContainer, cmdArgs)
				if err != nil {
					entry.Error = fmt.Sprintf("%v (stderr: %q)", err, stderr)
				} else {
					entry.raw = stdout
					if cCtx.Mode == modeCluster {
						parts := strings.SplitN(stdout, "\n---\n", 2)
						entry.ClusterNodes = parseClusterNodesJSON(parts[0])
						if len(parts) > 1 {
							entry.ClusterInfo = parseInfoKV(parts[1])
						}
					} else {
						entry.Replication = parseInfoKV(stdout)
					}
				}
				res.Redis = append(res.Redis, entry)
			}

			// ── render ──────────────────────────────────────────────────────
			if jsonOutput {
				jsonResults = append(jsonResults, res)
				continue
			}

			if textIdx > 0 {
				fmt.Println(strings.Repeat("=", 40))
			}
			textIdx++
			fmt.Printf("Deep Inspect: %s/%s (Mode: %s)\n", res.Namespace, res.Name, res.Mode)
			fmt.Println(strings.Repeat("-", 40))

			for _, s := range res.Sentinels {
				fmt.Printf("Sentinel Pod: %s (IP: %s)\n", s.Pod, s.IP)
				if s.Error != "" {
					fmt.Printf("  [!] Error: %s\n", s.Error)
				} else {
					printLines(s.raw)
				}
				fmt.Println()
			}
			for _, r := range res.Redis {
				fmt.Printf("Redis Pod: %s (IP: %s)\n", r.Pod, r.IP)
				if r.Assignment != nil {
					fmt.Printf("  Assignment: role=%s, master-ip=%s, epoch=%s (label: %s)\n",
						valueOrNone(r.Assignment["assigned-role"]),
						valueOrNone(r.Assignment["assigned-master-ip"]),
						valueOrNone(r.Assignment["assignment-epoch"]),
						valueOrNone(r.Assignment["role-label"]))
				}
				if r.Error != "" {
					fmt.Printf("  [!] Error: %s\n", r.Error)
				} else {
					printLines(r.raw)
				}
				fmt.Println()
			}
		}

		if jsonOutput {
			if err := printJSON(jsonResults); err != nil {
				return err
			}
		}
		if errCount > 0 {
			return fmt.Errorf("%d of %d resource(s) not found or inaccessible", errCount, len(targets))
		}
		return nil
	},
}

// valueOrNone renders an empty annotation/label value as <none>.
func valueOrNone(v string) string {
	if v == "" {
		return "<none>"
	}
	return v
}

func printLines(stdout string) {
	lines := strings.SplitSeq(strings.TrimSpace(stdout), "\n")
	for line := range lines {
		fmt.Printf("  %s\n", line)
	}
}

func init() {
	rootCmd.AddCommand(inspectCmd)
	inspectCmd.ValidArgsFunction = completeLittleRedNames
}
