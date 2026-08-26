package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	debugDumpOutputDir    string
	operatorNamespaceFlag string
)

var debugDumpCmd = &cobra.Command{
	Use:   "debug-dump <name>",
	Short: "Collect debug artifacts for a LittleRed instance into a local directory",
	Long: `Collects logs, resource state, and live Redis diagnostics for a LittleRed
instance into a timestamped directory. Useful for sharing with maintainers
when reporting issues.

Collected artifacts:
  - LittleRed CR (full YAML including status and conditions)
  - Operator logs (last 10 minutes, filtered to the instance)
  - Pod logs (all containers, current + previous for crash loops)
  - Redis state (INFO replication, SENTINEL MASTER/REPLICAS/MASTERS,
    CLUSTER NODES/INFO)
  - Kubernetes resources (pods, statefulsets, services, PDBs, events)
  - Failover-mode assignment annotations (assigned-role / assigned-master-ip /
    assignment-epoch per data pod, failover mode only)

Secrets are never collected.`,
	Args: cobra.ExactArgs(1),
	RunE: runDebugDump,
}

func runDebugDump(_ *cobra.Command, args []string) error {
	k8sClient, coreClient, config, defaultNS, err := k8s.NewClient(kubeconfig)
	if err != nil {
		return err
	}

	targetNS := namespace
	if targetNS == "" {
		targetNS = defaultNS
	}

	ctx := context.Background()
	name := args[0]

	// Verify the CR exists.
	lr := &littleredv1alpha1.LittleRed{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: targetNS}, lr); err != nil {
		return fmt.Errorf("LittleRed %q not found in namespace %q: %w", name, targetNS, err)
	}

	// Create output directory.
	dir := debugDumpOutputDir
	if dir == "" {
		dir = fmt.Sprintf("debug-dump-%s-%s", name, time.Now().Format("20060102-150405"))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}
	fmt.Printf("Collecting debug artifacts for %s/%s into %s/\n", targetNS, name, dir)

	// Collect everything. Errors are logged but don't stop collection.
	collectCR(ctx, dir, name, targetNS)
	collectOperatorLogsForCR(dir, name, targetNS)
	collectPodLogsForCR(ctx, k8sClient, dir, name, targetNS)
	collectRedisState(ctx, coreClient, config, k8sClient, dir, lr)
	collectK8sResources(dir, name, targetNS)
	if lr.Spec.Mode == modeFailover {
		collectFailoverAssignments(dir, name, targetNS)
	}

	fmt.Printf("Done. Artifacts written to %s/\n", dir)
	return nil
}

// --- CR ---

func collectCR(_ context.Context, dir, name, ns string) {
	fmt.Println("  Collecting LittleRed CR...")
	out := kubectl("get", "littlered", name, "-n", ns, "-o", "yaml")
	writeFile(dir, "littlered-cr.yaml", out)
}

// --- Operator Logs ---

func collectOperatorLogsForCR(dir, crName, crNamespace string) {
	fmt.Println("  Collecting operator logs...")
	opNS := operatorNamespaceFlag

	// Find operator pod.
	podName := strings.TrimSpace(kubectl("get", "pods", "-n", opNS,
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}"))
	if podName == "" {
		writeFile(dir, "operator-logs.txt", "ERROR: operator pod not found in namespace "+opNS)
		return
	}

	// Get last 10 minutes of logs, then grep for the CR name to reduce noise.
	allLogs := kubectl("logs", podName, "-n", opNS, "-c", "manager", "--since=10m", "--timestamps")
	var filtered []string
	for line := range strings.SplitSeq(allLogs, "\n") {
		if strings.Contains(line, crName) || strings.Contains(line, crNamespace) ||
			strings.Contains(line, "error") || strings.Contains(line, "Error") {
			filtered = append(filtered, line)
		}
	}
	if len(filtered) == 0 {
		// Fall back to unfiltered tail.
		writeFile(dir, "operator-logs.txt", "# No lines matched CR name; showing last 500 lines\n"+
			kubectl("logs", podName, "-n", opNS, "-c", "manager", "--tail=500", "--timestamps"))
	} else {
		writeFile(dir, "operator-logs.txt", strings.Join(filtered, "\n"))
	}
}

// --- Pod Logs ---

func collectPodLogsForCR(ctx context.Context, k8sClient client.Client, dir, name, ns string) {
	fmt.Println("  Collecting pod logs...")

	podList := &corev1.PodList{}
	if err := k8sClient.List(ctx, podList, client.InNamespace(ns), client.MatchingLabels{
		"app.kubernetes.io/instance": name,
	}); err != nil {
		writeFile(dir, "pod-logs-error.txt", fmt.Sprintf("failed to list pods: %v", err))
		return
	}

	for _, pod := range podList.Items {
		for _, container := range pod.Spec.Containers {
			// Current logs.
			logs := kubectl("logs", pod.Name, "-n", ns, "-c", container.Name, "--tail=2000", "--timestamps")
			writeFile(dir, fmt.Sprintf("pod-%s-%s.log", pod.Name, container.Name), logs)

			// Previous logs (crash loop).
			prev := kubectl("logs", pod.Name, "-n", ns, "-c", container.Name, "--previous", "--tail=2000", "--timestamps")
			if prev != "" && !strings.Contains(prev, "previous terminated container") {
				writeFile(dir, fmt.Sprintf("pod-%s-%s-previous.log", pod.Name, container.Name), prev)
			}
		}
	}
}

// --- Live Redis State ---

func collectRedisState(
	ctx context.Context,
	coreClient *kubernetes.Clientset,
	config *rest.Config,
	k8sClient client.Client,
	dir string,
	lr *littleredv1alpha1.LittleRed,
) {
	fmt.Println("  Collecting live Redis state...")

	podList := &corev1.PodList{}
	if err := k8sClient.List(ctx, podList, client.InNamespace(lr.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance": lr.Name,
	}); err != nil {
		return
	}

	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		component := pod.Labels["app.kubernetes.io/component"]

		switch component {
		case "redis":
			// Sentinel-mode Redis pod.
			out, _, _ := k8s.Exec(ctx, coreClient, config, lr.Namespace, pod.Name, "redis",
				redisCliArgs(infoSubcommand, "replication"))
			writeFile(dir, fmt.Sprintf("redis-%s-info-replication.txt", pod.Name), out)

		case modeCluster:
			// Cluster-mode Redis pod.
			out, _, _ := k8s.Exec(ctx, coreClient, config, lr.Namespace, pod.Name, "redis",
				redisCliChainArgs(
					[]string{infoSubcommand, "replication"},
					[]string{clusterSubcommand, "nodes"},
					[]string{clusterSubcommand, infoSubcommand},
				))
			writeFile(dir, fmt.Sprintf("redis-%s-cluster-state.txt", pod.Name), out)

		case modeSentinel:
			// Sentinel pod.
			// Per-instance master name: a hardcoded one would report "No such master"
			// on any correctly-scoped instance.
			//
			// `SENTINEL masters` is dumped alongside them because the two
			// single-name calls can only confirm or deny the name we passed: a
			// Sentinel carrying a leftover name answers `master <mn>` perfectly
			// well, and the leftover — two names over one instance, two failover
			// state machines — is invisible in their output (LR-048).
			mn := lr.SentinelMasterName()
			out, _, _ := k8s.Exec(ctx, coreClient, config, lr.Namespace, pod.Name, "sentinel",
				redisCliChainArgs(
					[]string{"-p", "26379", modeSentinel, roleMaster, mn},
					[]string{"-p", "26379", modeSentinel, "replicas", mn},
					[]string{"-p", "26379", modeSentinel, "masters"},
				))
			writeFile(dir, fmt.Sprintf("sentinel-%s-state.txt", pod.Name), out)
		}
	}
}

// --- Failover-Mode Assignment Annotations ---

// collectFailoverAssignments dumps the operator-stamped assignment annotations
// (the failover-mode intent record, ADR-011) plus the role label per data pod.
func collectFailoverAssignments(dir, name, ns string) {
	fmt.Println("  Collecting failover assignment annotations...")

	const jsonpath = `{range .items[*]}{.metadata.name}` +
		`{"\tlabel-role="}{.metadata.labels.redis\.chuck-chuck-chuck\.net/role}` +
		`{"\tassigned-role="}{.metadata.annotations.redis\.chuck-chuck-chuck\.net/assigned-role}` +
		`{"\tassigned-master-ip="}{.metadata.annotations.redis\.chuck-chuck-chuck\.net/assigned-master-ip}` +
		`{"\tassignment-epoch="}{.metadata.annotations.redis\.chuck-chuck-chuck\.net/assignment-epoch}` +
		`{"\n"}{end}`

	out := kubectl("get", "pods", "-n", ns,
		"-l", "app.kubernetes.io/instance="+name,
		"-o", "jsonpath="+jsonpath)
	writeFile(dir, "failover-assignments.txt", out)
}

// --- Kubernetes Resources ---

func collectK8sResources(dir, name, ns string) {
	fmt.Println("  Collecting Kubernetes resources...")

	// Server and client versions. Behaviour that a dump is used to explain can depend
	// on the API server version — CRD validation ratcheting, for one, changes whether
	// a newly-required field blocks writes on pre-existing objects, and it shifted
	// across 1.29/1.30/1.33. Without this a reader has to go and ask.
	writeFile(dir, "kubernetes-version.txt", kubectl("version"))
	writeFile(dir, "nodes.txt", kubectl("get", "nodes", "-o", "wide"))

	writeFile(dir, "pods.txt",
		kubectl("get", "pods", "-n", ns,
			"-l", "app.kubernetes.io/instance="+name,
			"-o", "wide",
			"-L", "redis.chuck-chuck-chuck.net/role"))

	writeFile(dir, "statefulsets.yaml",
		kubectl("get", "statefulsets", "-n", ns,
			"-l", "app.kubernetes.io/instance="+name,
			"-o", "yaml"))

	writeFile(dir, "services.yaml",
		kubectl("get", "services", "-n", ns,
			"-l", "app.kubernetes.io/instance="+name,
			"-o", "yaml"))

	writeFile(dir, "pdb.yaml",
		kubectl("get", "pdb", "-n", ns,
			"-l", "app.kubernetes.io/instance="+name,
			"-o", "yaml"))

	writeFile(dir, "events.txt",
		kubectl("get", "events", "-n", ns,
			"--sort-by=.lastTimestamp",
			"--field-selector=involvedObject.name="+name))

	// Also get events for all pods (covers StatefulSet pod events).
	writeFile(dir, "pod-events.txt",
		kubectl("get", "events", "-n", ns,
			"--sort-by=.lastTimestamp"))
}

// --- Helpers ---

// kubectl runs a kubectl command and returns stdout. Errors are silently swallowed
// (the output will contain the error text if kubectl prints to stdout).
func kubectl(args ...string) string {
	cmd := exec.Command("kubectl", args...)
	out, _ := cmd.CombinedOutput()
	return string(out)
}

func writeFile(dir, name, content string) {
	if content == "" {
		return
	}
	// Redact potential password leaks in log files.
	if strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".txt") {
		content = redactSecrets(content)
	}
	path := filepath.Join(dir, name)
	_ = os.WriteFile(path, []byte(content), 0o644)
}

// redactSecrets replaces known password patterns in log output.
// Catches: -a <password>, --requirepass <password>, --masterauth <password>,
// sentinel-pass <password>, AUTH_ARGS=..., SENTINEL_AUTH_ARGS=...
func redactSecrets(s string) string {
	const redacted = `${1}[REDACTED]`
	patterns := []struct{ re, repl string }{
		{`(-a\s+)\S+`, redacted},
		{`(--requirepass\s+)\S+`, redacted},
		{`(--masterauth\s+)\S+`, redacted},
		{`(sentinel-pass\s+)\S+`, redacted},
		{`(AUTH_ARGS=)\S+`, redacted},
		{`(SENTINEL_AUTH_ARGS=)\S+`, redacted},
	}
	for _, p := range patterns {
		re := regexp.MustCompile(p.re)
		s = re.ReplaceAllString(s, p.repl)
	}
	return s
}

func init() {
	rootCmd.AddCommand(debugDumpCmd)
	debugDumpCmd.ValidArgsFunction = completeLittleRedNames
	debugDumpCmd.Flags().StringVarP(&debugDumpOutputDir, "output", "o", "",
		"Output directory (default: debug-dump-<name>-<timestamp>)")
	debugDumpCmd.Flags().StringVar(&operatorNamespaceFlag, "operator-namespace", "littlered-system",
		"Namespace where the LittleRed operator is deployed")
}
