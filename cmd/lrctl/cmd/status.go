package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/spf13/cobra"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var statusCmd = &cobra.Command{
	Use:   "status [name]",
	Short: "Show status of a LittleRed instance (omit name to list all in namespace)",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		k8sClient, _, _, defaultNS, err := k8s.NewClientForContext(kubeconfig, kubeContext)
		if err != nil {
			return err
		}

		targetNS := namespace
		if targetNS == "" {
			targetNS = defaultNS
		}

		ctx := context.Background()

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

		var jsonResults []statusJSON
		errCount := 0
		textIdx := 0

		for _, key := range targets {
			lr := &littleredv1alpha1.LittleRed{}
			if err := k8sClient.Get(ctx, key, lr); err != nil {
				fmt.Fprintf(os.Stderr, "error: %s/%s: %v\n", key.Namespace, key.Name, err)
				errCount++
				continue
			}
			if jsonOutput {
				jsonResults = append(jsonResults, lrToStatusJSON(lr))
			} else {
				if textIdx > 0 {
					fmt.Println()
				}
				printStatus(lr)
				textIdx++
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

func printStatus(lr *littleredv1alpha1.LittleRed) {
	fmt.Printf("Cluster: %s\n", lr.Name)
	fmt.Printf("Namespace: %s\n", lr.Namespace)
	fmt.Printf("Phase: %s\n", lr.Status.Phase)
	fmt.Printf("Mode: %s\n", lr.Spec.Mode)

	if lr.Status.Master != nil {
		fmt.Printf("Master: %s (IP: %s)\n", lr.Status.Master.PodName, lr.Status.Master.IP)
	} else {
		fmt.Printf("Master: <none>\n")
	}

	// Multi-site: the operator aggregates per-site observed state into
	// status.multiSite (docs/MULTI_SITE_DESIGN.md §8). Surface it here — the
	// single "Master" line is meaningless for a stretched cluster (roles are
	// fluid per shard, pillar 3.5).
	if lr.Spec.MultiSite != nil {
		printMultiSiteStatus(lr)
	}

	if lr.Status.Sentinels != nil {
		fmt.Printf("Sentinels: %d/%d Ready\n", lr.Status.Sentinels.Ready, lr.Status.Sentinels.Total)
	}
	if lr.Spec.Mode == modeFailover && lr.Status.Replicas != nil {
		fmt.Printf("Replicas: %d/%d Ready\n", lr.Status.Replicas.Ready, lr.Status.Replicas.Total)
	}
	fmt.Printf("Redis Nodes: %d/%d Ready\n", lr.Status.Redis.Ready, lr.Status.Redis.Total)

	// Surface an in-progress in-place legacy→per-shard cluster migration (ADR-013).
	// One line, only while migrating (nil / Complete render nothing, so non-migrating
	// output is unchanged).
	if banner := migrationBanner(clusterMigration(lr)); banner != "" {
		fmt.Println(banner)
	}

	// Failover-mode monitoring surfaces (ADR-011): assignment epoch, plus the
	// detection-window and post-transition markers when set.
	if fo := lr.Status.Failover; fo != nil {
		fmt.Printf("Assignment Epoch: %d\n", fo.AssignmentEpoch)
		if fo.MasterDownSince != nil {
			fmt.Printf("Master Down Since: [!] %s (detection window running)\n",
				fo.MasterDownSince.Format(timeFormat))
		}
		if fo.TransitionSince != nil {
			fmt.Printf("Last Transition: %s\n", fo.TransitionSince.Format(timeFormat))
		}
	}

	// Surface the failover-mode refuse-and-wait condition (ADR-011).
	if c := apimeta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionFailoverRecovery); c != nil {
		printRecoveryCondition("Failover Recovery", c)
	}

	// Surface the leaderless bootstrap-deadlock condition (ADR-005 / LR-015).
	if c := apimeta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionLeaderlessRecovery); c != nil {
		printRecoveryCondition("Leaderless Recovery", c)
	} else if lr.Status.LeaderlessSince != nil {
		since := lr.Status.LeaderlessSince.Format(timeFormat)
		fmt.Printf("Leaderless Recovery: [!] bare-Sentinel deadlock since %s\n", since)
	}
}

// printMultiSiteStatus renders the multi-site observed state from the CR status
// (the operator's aggregated per-site view) plus the site-loss takeover marker.
func printMultiSiteStatus(lr *littleredv1alpha1.LittleRed) {
	ms := lr.Spec.MultiSite
	sitesPerShard := len(ms.Sites)
	if ms.SitesPerShard > 0 {
		sitesPerShard = ms.SitesPerShard
	}
	fmt.Printf("Multi-Site: %d sites, sitesPerShard=%d, network=%s\n",
		len(ms.Sites), sitesPerShard, valueOrNone(ms.Network.Mode))
	if lr.Status.Cluster != nil && lr.Status.Cluster.State != "" {
		fmt.Printf("Cluster State: %s\n", lr.Status.Cluster.State)
	}

	fmt.Println("Sites:")
	if lr.Status.MultiSite != nil {
		for _, s := range lr.Status.MultiSite.Sites {
			reach := "unreachable"
			if s.Reachable {
				reach = "reachable"
			}
			pods := ""
			if s.Pods != nil {
				pods = fmt.Sprintf(", pods %d/%d Ready", s.Pods.Ready, s.Pods.Total)
			}
			fmt.Printf("  - %s: %s%s\n", s.Name, reach, pods)
		}
	} else {
		for _, s := range ms.Sites {
			fmt.Printf("  - %s: (no observed state yet)\n", s.Name)
		}
	}

	// Tier-1 cross-site view: the operator's published, site-attributed topology
	// (docs/LRCTL.md "Multi-Site Clusters"). Rendered from status alone — no site
	// access — with its freshness (the last reconcile).
	if mss := lr.Status.MultiSite; mss != nil && mss.LastReconcileTime != nil {
		fmt.Printf("Slots Assigned: %d / 16384\n", mss.SlotsAssigned)
		crossVerdict := "[OK] every shard spans >= 2 sites"
		if !mss.CrossSiteOK {
			crossVerdict = "[!] one or more shards are NOT cross-site"
		}
		fmt.Printf("Cross-Site Placement: %s\n", crossVerdict)
		for _, sh := range mss.Shards {
			marker := "cross-site"
			if !sh.CrossSite {
				marker = "[!] SINGLE-SITE"
			}
			link := ""
			if sh.LinkDown {
				link = " [WARN] a replica link is down"
			}
			fmt.Printf("  - shard %d: master@%s, replicas@%v (%s)%s\n",
				sh.Shard, valueOrNone(sh.MasterSite), sh.ReplicaSites, marker, link)
		}
		fmt.Printf("View freshness: as of last reconcile %s ago (at %s)\n",
			time.Since(mss.LastReconcileTime.Time).Round(time.Second),
			mss.LastReconcileTime.Format(timeFormat))
	}

	if lr.Status.MultiSite != nil && lr.Status.MultiSite.TakeoverPendingSince != nil {
		fmt.Printf("Site-Loss Takeover: [!] pending since %s (cooldown armed)\n",
			lr.Status.MultiSite.TakeoverPendingSince.Format(timeFormat))
	}
	if c := apimeta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionMultiSiteTakeover); c != nil {
		printRecoveryCondition("Site-Loss Takeover", c)
	}
}

// printRecoveryCondition renders a recovery/takeover condition with a consistent
// marker: [recovered] when it has cleared, an action-required marker while True.
func printRecoveryCondition(label string, c *metav1.Condition) {
	marker := markerRecovered
	if c.Status == metav1.ConditionTrue {
		marker = markerActionRequired
	}
	fmt.Printf("%s: %s %s: %s\n", label, marker, c.Reason, c.Message)
}

const (
	markerRecovered      = "[recovered]"
	markerActionRequired = "[!] ACTION MAY BE REQUIRED"
)

// timeFormat is the timestamp layout used by printStatus (RFC3339).
const timeFormat = "2006-01-02T15:04:05Z07:00"

func init() {
	rootCmd.AddCommand(statusCmd)
	statusCmd.ValidArgsFunction = completeLittleRedNames
}
