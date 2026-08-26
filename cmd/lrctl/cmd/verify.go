package cmd

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/discovery"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/littlered-operator/littlered-operator/internal/cli/types"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

var verifyCmd = &cobra.Command{
	Use:   "verify [name]",
	Short: "Verify consistency of a Redis instance (omit name to verify all in namespace)",
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

		var jsonResults []any
		errCount := 0

		for i, key := range targets {
			cCtx, err := discovery.GetContext(ctx, k8sClient, key.Namespace, key.Name, kind, unmanaged)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %s/%s: %v\n", key.Namespace, key.Name, err)
				errCount++
				continue
			}

			if jsonOutput {
				switch cCtx.Mode {
				case modeSentinel:
					jsonResults = append(jsonResults,
						verifySentinelJSON(ctx, coreClient, config, cCtx, key.Name, key.Namespace))
				case modeCluster:
					jsonResults = append(jsonResults,
						verifyClusterJSON(ctx, coreClient, config, cCtx, key.Name, key.Namespace))
				case modeFailover:
					jsonResults = append(jsonResults,
						verifyFailoverJSON(ctx, coreClient, config, cCtx, key.Name, key.Namespace))
				default:
					fmt.Fprintf(os.Stderr,
						"error: %s/%s: JSON output for mode %q not yet implemented\n",
						key.Namespace, key.Name, cCtx.Mode)
					errCount++
				}
				continue
			}

			if i > 0 {
				fmt.Println(strings.Repeat("=", 40))
			}
			fmt.Printf("Verifying Cluster: %s/%s (Mode: %s)\n", cCtx.Namespace, cCtx.Name, cCtx.Mode)

			// Surface an in-progress in-place legacy→per-shard cluster migration
			// (ADR-013). Read-only, best-effort: the migration phase lives on the CR
			// status, which --unmanaged targets don't have. A fetch error is non-fatal
			// (verify's job is live health, not migration reporting).
			if !unmanaged {
				lr := &littleredv1alpha1.LittleRed{}
				if err := k8sClient.Get(ctx, key, lr); err == nil {
					if banner := migrationBanner(clusterMigration(lr)); banner != "" {
						fmt.Println(banner)
					}
				}
			}

			var verifyErr error
			switch cCtx.Mode {
			case modeSentinel:
				verifyErr = verifySentinel(ctx, coreClient, config, cCtx)
			case modeCluster:
				verifyErr = verifyCluster(ctx, coreClient, config, cCtx)
			case modeFailover:
				verifyErr = verifyFailover(ctx, coreClient, config, cCtx)
			default:
				fmt.Printf("Verification for mode %q not yet fully implemented\n", cCtx.Mode)
			}
			if verifyErr != nil {
				errCount++ // [FAIL] details already printed by verifySentinel / verifyCluster
			}
		}

		if jsonOutput {
			if err := printJSON(jsonResults); err != nil {
				return err
			}
		}
		if errCount > 0 {
			return fmt.Errorf("%d of %d resource(s) failed verification", errCount, len(targets))
		}
		return nil
	},
}

func verifyCluster(
	ctx context.Context, coreClient *kubernetes.Clientset, config *rest.Config,
	cCtx *types.ClusterContext,
) error {
	clusterPods := make(map[string]string)
	for _, p := range cCtx.RedisPods {
		if p.Status.PodIP != "" {
			clusterPods[p.Status.PodIP] = p.Name
		}
	}

	fmt.Println("Gathering Cluster Ground Truth...")
	g := &cliGatherer{coreClient: coreClient, config: config, cCtx: cCtx}
	gt := redisclient.GatherClusterGroundTruth(ctx, g, clusterPods)

	fmt.Printf("\nCluster State: %s\n", gt.ClusterState)
	fmt.Printf("Total Slots Assigned: %d / 16384\n", gt.TotalSlots)

	fmt.Println("\nNode Status:")
	for _, pod := range cCtx.RedisPods {
		node, ok := gt.Nodes[pod.Name]
		if !ok || !node.Reachable {
			fmt.Printf("  - Pod %s: [!] UNREACHABLE\n", pod.Name)
			continue
		}

		role := node.Role
		details := ""
		if role == roleMaster {
			details = fmt.Sprintf("slots:%s", strings.Join(node.Slots, ","))
		} else {
			details = fmt.Sprintf("following:%s, link:%s", node.MasterNodeID, node.LinkStatus)
		}
		fmt.Printf("  - Pod %s: role:%s, id:%s, %s\n", pod.Name, role, node.NodeID, details)
	}

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

	fmt.Println("\nCluster Topology:")
	// Build ID to PodName map for display
	idToName := make(map[string]string)
	for _, n := range gt.Nodes {
		if n.NodeID != "" {
			idToName[n.NodeID] = n.PodName
		}
	}

	for _, n := range gt.Nodes {
		if n.Role != roleMaster {
			continue
		}
		fmt.Printf("  Master: %s (%s)\n", n.PodName, n.NodeID)
		if len(n.Slots) > 0 {
			fmt.Printf("    Slots: %s\n", strings.Join(n.Slots, " "))
		} else {
			fmt.Printf("    [!] NO SLOTS ASSIGNED\n")
		}

		// Find replicas
		for _, r := range gt.Nodes {
			if r.Role == "replica" && r.MasterNodeID == n.NodeID {
				fmt.Printf("    └── Replica: %s (%s, link:%s)\n", r.PodName, r.NodeID, r.LinkStatus)
			}
		}
	}

	fmt.Printf("\nSummary:\n")
	expectedNodes := int32(len(cCtx.RedisPods))
	expectedShards := int32(gt.CountMasters())
	healthy := gt.IsHealthy(expectedNodes, expectedShards)

	// Shard-colocation invariant (ADR-007): each Redis shard must live inside one shard
	// StatefulSet. A cross-STS pairing is a real (non-transient) topology defect — the
	// operator's shard-pinning has broken — so it fails verification.
	violations := gt.CheckShardColocation()
	for _, v := range violations {
		fmt.Printf("  [FAIL] Replica %s (shard %d) follows a master in shard %d (%s) — Redis shard spans two StatefulSets\n",
			v.ReplicaPod, v.ReplicaShard, v.MasterShard, v.MasterPod)
	}

	if !healthy || len(violations) > 0 {
		fmt.Println("  [FAIL] Cluster has topology or health issues!")
		return fmt.Errorf("cluster %s/%s has topology or health issues", cCtx.Namespace, cCtx.Name)
	}

	// Degraded (functional but reduced redundancy): a replica whose replication link is
	// down is not currently receiving its master's stream. This is often a transient
	// resync, so it warns rather than fails — but the cluster is not fully healthy.
	var linkDown []string
	for _, n := range gt.Nodes {
		if n.Role == "replica" && n.LinkStatus == "down" {
			linkDown = append(linkDown, n.PodName)
		}
	}
	sort.Strings(linkDown)
	if len(linkDown) > 0 {
		for _, pod := range linkDown {
			fmt.Printf("  [WARN] Replica %s link:down — reduced redundancy (may be a transient resync)\n", pod)
		}
		fmt.Printf("  [DEGRADED] Cluster is functional but %d replica link(s) are down; not fully healthy.\n", len(linkDown))
		return nil
	}

	fmt.Println("  [OK] Cluster is healthy and consistent.")
	return nil
}

func verifySentinel(
	ctx context.Context, coreClient *kubernetes.Clientset, config *rest.Config,
	cCtx *types.ClusterContext,
) error {
	redisMap := make(map[string]string)
	for _, p := range cCtx.RedisPods {
		if p.Status.PodIP != "" {
			redisMap[p.Status.PodIP] = p.Name
		}
	}

	sentinelMap := make(map[string]string)
	for _, p := range cCtx.SentinelPods {
		if p.Status.PodIP != "" {
			sentinelMap[p.Status.PodIP] = p.Name
		}
	}

	fmt.Println("Gathering Cluster Ground Truth...")
	g := &cliGatherer{coreClient: coreClient, config: config, cCtx: cCtx}
	state := redisclient.GatherReplicationState(ctx, g, redisMap, sentinelMap, masterNameOf(cCtx))

	fmt.Println("\nSentinel Status:")
	for _, sn := range state.SentinelNodes {
		status := "idle"
		if sn.Monitoring {
			status = fmt.Sprintf("monitoring %s", sn.MasterIP)
		}
		if !sn.Reachable {
			status = statusUnreachable
		}
		fmt.Printf("  - Sentinel %s: %s\n", sn.PodName, status)
	}

	fmt.Println("\nRedis Status:")
	for _, rn := range state.RedisNodes {
		status := fmt.Sprintf("role:%s", rn.Role)
		if rn.Role == roleSlave {
			status += fmt.Sprintf(", following:%s, link:%s", rn.MasterHost, rn.LinkStatus)
		}
		status += fmt.Sprintf(", keys:%d", rn.Keys)
		if !rn.Reachable {
			status = statusUnreachable
		}
		fmt.Printf("  - Redis %s: %s\n", rn.PodName, status)
	}

	fmt.Printf("\nGround Truth Summary:\n")
	if state.RealMasterIP != "" {
		masterName := redisMap[state.RealMasterIP]
		if masterName == "" {
			masterName = "GHOST(" + state.RealMasterIP + ")"
		}
		fmt.Printf("  [OK] Authority Master: %s (%s)\n", masterName, state.RealMasterIP)
	} else {
		fmt.Printf("  [FAIL] Authority Master: NONE (Split Brain or Cluster not initialized)\n")
	}

	if state.FailoverActive {
		fmt.Printf("  [!] Sentinel reports failover in progress!\n")
	}

	// A Sentinel monitoring a name other than the CR's fails verification. This is
	// the check the rename runbook's verification step needs: before it, `verify`
	// asked only about the desired name, so a two-name instance — two `sentinel
	// monitor` lines, two failover state machines over the same pods — reported as
	// entirely healthy (LR-048).
	staleNames := reportCrossInstance(state, cCtx)

	actions := state.GetHealActions(masterNameOf(cCtx))
	if len(actions) > 0 {
		fmt.Println("\nRecommended Healing Actions:")
		for _, a := range actions {
			fmt.Printf("  - %s\n", a)
		}
	} else if state.RealMasterIP != "" && !staleNames {
		fmt.Println("\n[OK] Cluster configuration is consistent.")
	}

	return sentinelVerifyFailure(cCtx.Namespace, cCtx.Name, masterNameOf(cCtx),
		state.RealMasterIP, len(actions), staleNames)
}

// sentinelVerifyFailure turns the three sentinel-mode findings into the error verify
// returns, or nil. A non-nil error becomes errCount, then the RunE error, then
// main.go's os.Exit(1) — so this function IS the exit code.
//
// It is a separate, pure function for exactly one reason: the printed verdict and the
// process's exit status must not be able to disagree. A script trusts the exit code,
// so a `[FAIL]` line beside an exit 0 would be worse than no check at all — the check
// would be actively misleading rather than merely absent. Keeping the decision here,
// driven by the same booleans that drove the printing, makes that a unit-testable
// property instead of a reviewer's promise.
func sentinelVerifyFailure(namespace, name, masterName, realMasterIP string,
	healActions int, staleNames bool,
) error {
	switch {
	case realMasterIP == "" || healActions > 0:
		return fmt.Errorf("cluster %s/%s is not healthy or consistent", namespace, name)
	case staleNames:
		// Named separately: an instance that is otherwise healthy but carries a
		// second master name is a specific, actionable defect, and "not healthy or
		// consistent" would send the reader looking for the wrong thing.
		return fmt.Errorf("cluster %s/%s does not monitor exactly one Sentinel master name (%q)",
			namespace, name, masterName)
	}
	return nil
}

func init() {
	rootCmd.AddCommand(verifyCmd)
	verifyCmd.ValidArgsFunction = completeLittleRedNames
}

// verifySentinelJSON gathers sentinel cluster state and returns it as a
// JSON-serialisable struct without printing anything.
func verifySentinelJSON(
	ctx context.Context, coreClient *kubernetes.Clientset, config *rest.Config,
	cCtx *types.ClusterContext, name, namespace string,
) sentinelVerifyJSON {
	redisMap := make(map[string]string)
	for _, p := range cCtx.RedisPods {
		if p.Status.PodIP != "" {
			redisMap[p.Status.PodIP] = p.Name
		}
	}
	sentinelMap := make(map[string]string)
	for _, p := range cCtx.SentinelPods {
		if p.Status.PodIP != "" {
			sentinelMap[p.Status.PodIP] = p.Name
		}
	}
	g := &cliGatherer{coreClient: coreClient, config: config, cCtx: cCtx}
	state := redisclient.GatherReplicationState(ctx, g, redisMap, sentinelMap, masterNameOf(cCtx))
	return buildSentinelVerifyJSON(name, namespace, redisMap, state, masterNameOf(cCtx),
		cCtx.SentinelMasterName, len(cCtx.SentinelPods), max(len(cCtx.RedisPods)-1, 0))
}

// verifyClusterJSON gathers cluster ground truth and returns it as a
// JSON-serialisable struct without printing anything.
func verifyClusterJSON(
	ctx context.Context, coreClient *kubernetes.Clientset, config *rest.Config,
	cCtx *types.ClusterContext, name, namespace string,
) clusterVerifyJSON {
	clusterPods := make(map[string]string)
	for _, p := range cCtx.RedisPods {
		if p.Status.PodIP != "" {
			clusterPods[p.Status.PodIP] = p.Name
		}
	}
	g := &cliGatherer{coreClient: coreClient, config: config, cCtx: cCtx}
	gt := redisclient.GatherClusterGroundTruth(ctx, g, clusterPods)
	return buildClusterVerifyJSON(name, namespace, gt)
}
