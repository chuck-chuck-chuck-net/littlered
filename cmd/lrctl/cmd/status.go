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
)

var statusCmd = &cobra.Command{
	Use:   "status [name]",
	Short: "Show status of a LittleRed instance (omit name to list all in namespace)",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		k8sClient, _, _, defaultNS, err := k8s.NewClient(kubeconfig)
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

	if lr.Status.Sentinels != nil {
		fmt.Printf("Sentinels: %d/%d Ready\n", lr.Status.Sentinels.Ready, lr.Status.Sentinels.Total)
	}
	if lr.Spec.Mode == modeFailover && lr.Status.Replicas != nil {
		fmt.Printf("Replicas: %d/%d Ready\n", lr.Status.Replicas.Ready, lr.Status.Replicas.Total)
	}
	fmt.Printf("Redis Nodes: %d/%d Ready\n", lr.Status.Redis.Ready, lr.Status.Redis.Total)

	// Surface a declared heavy operation (ADR-020) first among the extras: it is the
	// mechanism that decides whether the operator is healing or standing down, so it must
	// be impossible to miss. Absent when nothing is in flight — a permanent line here
	// would be noise on every instance in the fleet.
	for _, l := range renderOperationStatus(operationViewOf(lr), time.Now()) {
		fmt.Println(l)
	}

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
		marker := "[recovered]"
		if c.Status == "True" {
			marker = "[!] ACTION MAY BE REQUIRED"
		}
		fmt.Printf("Failover Recovery: %s %s: %s\n", marker, c.Reason, c.Message)
	}

	// Surface the leaderless bootstrap-deadlock condition (ADR-005 / LR-015).
	if c := apimeta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionLeaderlessRecovery); c != nil {
		marker := "[recovered]"
		if c.Status == "True" {
			marker = "[!] ACTION MAY BE REQUIRED"
		}
		fmt.Printf("Leaderless Recovery: %s %s: %s\n", marker, c.Reason, c.Message)
	} else if lr.Status.LeaderlessSince != nil {
		since := lr.Status.LeaderlessSince.Format(timeFormat)
		fmt.Printf("Leaderless Recovery: [!] bare-Sentinel deadlock since %s\n", since)
	}
}

// timeFormat is the timestamp layout used by printStatus (RFC3339).
const timeFormat = "2006-01-02T15:04:05Z07:00"

func init() {
	rootCmd.AddCommand(statusCmd)
	statusCmd.ValidArgsFunction = completeLittleRedNames
}
