//go:build e2e
// +build e2e

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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/littlered-operator/littlered-operator/test/utils"
)

var (
	// testStartTime tracks when the current test started
	testStartTime time.Time

	// suiteStartTime is when this test binary started. It is a package-level
	// initializer rather than a BeforeSuite assignment so it cannot be forgotten by a
	// wiring site, and because process start is what we actually want: everything the
	// operator has done to any instance in this run happened after it.
	suiteStartTime = time.Now()
)

// operatorLogFloor is the minimum log window collected for a failing spec, whatever
// the arithmetic below produces. See operatorLogWindow.
const operatorLogFloor = 30 * time.Minute

// operatorLogWindow returns the `kubectl logs --since` window for a failing spec.
//
// It used to be derived from the *spec's* elapsed time (`int(time.Since(testStartTime)
// .Minutes()) + 2`), which collapses for exactly the specs that most need artifacts: a
// spec that fails 0.4s in yielded `--since 2m`. In the LR-046 investigation that window
// opened **33 seconds after the pod under investigation was created and well after the
// stall had begun** — the whole early rollout, and the reconcile pass that chose the MEET
// seed, were simply absent, so only one of four observed dial cycles could be attributed
// to a named call site. Spec elapsed time is the wrong unit anyway: an instance created
// in an `Ordered` container's `BeforeAll` outlives every individual spec in it.
//
// So: cover the whole run (process start plus a 2m buffer for clock skew and the
// pre-spec deploy), with a generous floor so a fast failure early in the suite still
// gets useful history. It stays bounded — the log is also capped by `--tail`, and a
// window measured in tens of minutes of one operator's output is megabytes, not
// gigabytes.
func operatorLogWindow(now time.Time) time.Duration {
	window := now.Sub(suiteStartTime) + 2*time.Minute
	if window < operatorLogFloor {
		return operatorLogFloor
	}
	return window
}

// CollectDebugArtifacts collects logs and state information for debugging failed tests.
// It creates a timestamped directory with all relevant information.
//
// Parameters:
//   - testNamespace: The Kubernetes namespace where the test resources exist
//   - crName: The name of the LittleRed CR being tested
//   - chaosPodName: Optional chaos client pod name (empty string if not a chaos test)
func CollectDebugArtifacts(testNamespace, crName, chaosPodName string) {
	if !debugOnFailure {
		return
	}

	// Generate a unique directory name
	timestamp := time.Now().Format("20060102-150405")
	specText := CurrentSpecReport().LeafNodeText
	// Sanitize the spec text for use in a directory name
	sanitized := sanitizeForFilename(specText)
	debugDir := fmt.Sprintf("./debug-artifacts-%s-%s", timestamp, sanitized)

	// Create the debug directory
	if err := os.MkdirAll(debugDir, 0755); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to create debug directory %s: %v\n", debugDir, err)
		return
	}

	_, _ = fmt.Fprintf(GinkgoWriter, "\n=== Collecting debug artifacts to %s ===\n", debugDir)

	// Collect operator logs
	collectOperatorLogs(debugDir)

	// Collect LittleRed CR status
	if crName != "" {
		collectCRStatus(debugDir, testNamespace, crName)
	}

	// Collect pod logs (Redis/Valkey/Sentinel)
	collectPodLogs(debugDir, testNamespace, crName)

	// Write logs captured before pod deletions (not available via kubectl after replacement)
	writePreDeleteLogs(debugDir)

	// Copy streaming logs (dying-pod output including preStop) captured by background followers
	copyStreamingLogsToDir(debugDir)

	// Collect chaos client logs if applicable
	if chaosPodName != "" {
		collectChaosClientLogs(debugDir, testNamespace, chaosPodName)
	}

	// Collect general cluster state
	collectClusterState(debugDir, testNamespace)

	// Collect each pod's OWN view of the topology, plus the computed verdict when
	// an lrctl binary is available. See debug_ground_truth.go for why neither the
	// CR status nor the pod logs can substitute for this.
	collectRedisGroundTruth(debugDir, testNamespace)
	collectLrctlVerify(debugDir, testNamespace, crName)

	// Write test metadata
	writeTestMetadata(debugDir, testNamespace, crName, chaosPodName)

	_, _ = fmt.Fprintf(GinkgoWriter, "=== Debug artifacts collected to %s ===\n\n", debugDir)
}

// sanitizeForFilename removes characters that are problematic in filenames
func sanitizeForFilename(s string) string {
	// Replace spaces and special chars with hyphens
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	sanitized := re.ReplaceAllString(s, "-")
	// Trim hyphens from start/end
	sanitized = strings.Trim(sanitized, "-")
	// Limit length
	if len(sanitized) > 80 {
		sanitized = sanitized[:80]
	}
	return sanitized
}

// collectOperatorLogs collects logs from the operator pod since test start
func collectOperatorLogs(debugDir string) {
	_, _ = fmt.Fprintf(GinkgoWriter, "Collecting operator logs...\n")

	// Find operator pod
	cmd := exec.Command("kubectl", "get", "pods",
		"-n", operatorNamespace,
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	podName, err := utils.Run(cmd)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to find operator pod: %v\n", err)
		return
	}

	if podName == "" {
		_, _ = fmt.Fprintf(GinkgoWriter, "No operator pod found\n")
		return
	}

	// Cover the whole run, not just this spec — see operatorLogWindow.
	durationStr := fmt.Sprintf("%dm", int(operatorLogWindow(time.Now()).Minutes())+1)

	_, _ = fmt.Fprintf(GinkgoWriter, "Fetching operator logs for pod %s (--since %s)...\n", podName, durationStr)

	// Try with --since duration (more reliable than --since-time)
	cmd = exec.Command("kubectl", "logs",
		"-n", operatorNamespace,
		podName,
		"-c", "manager",
		"--since", durationStr,
		"--timestamps",
		// Safety cap. Raised with the window: --tail keeps the LAST N lines, so a
		// 10k cap on a suite-length window would re-truncate exactly the early
		// history the wider window was opened to capture. 50k lines of the
		// operator's JSON output is tens of megabytes, still an artifact rather
		// than a dump.
		"--tail", "50000")
	logs, err := cmd.CombinedOutput()

	if err != nil || len(logs) == 0 {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get operator logs with --since, trying without time filter: %v\n", err)
		// Fallback: get all recent logs (last 1000 lines)
		cmd = exec.Command("kubectl", "logs",
			"-n", operatorNamespace,
			podName,
			"-c", "manager",
			"--timestamps",
			"--tail", "1000")
		logs, err = cmd.CombinedOutput()
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get operator logs (fallback): %v\n", err)
		}
	}

	logFile := filepath.Join(debugDir, "operator-logs.txt")
	if err := os.WriteFile(logFile, logs, 0644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to write operator logs: %v\n", err)
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "Wrote %d bytes to %s\n", len(logs), logFile)
	}
}

// collectCRStatus collects the YAML representation of the LittleRed CR
func collectCRStatus(debugDir, namespace, crName string) {
	if crName == "" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CR status collection (no CR name provided)\n")
		return
	}

	_, _ = fmt.Fprintf(GinkgoWriter, "Collecting CR status for %s/%s...\n", namespace, crName)

	cmd := exec.Command("kubectl", "get", "littlered", crName,
		"-n", namespace,
		"-o", "yaml")
	output, err := cmd.CombinedOutput()
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get CR status: %v (output: %s)\n", err, string(output))
		return
	}

	statusFile := filepath.Join(debugDir, fmt.Sprintf("cr-%s.yaml", crName))
	if err := os.WriteFile(statusFile, output, 0644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to write CR status: %v\n", err)
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "Wrote %d bytes to %s\n", len(output), statusFile)
	}
}

// collectPodLogs collects logs from all pods in the namespace
func collectPodLogs(debugDir, namespace, crName string) {
	_, _ = fmt.Fprintf(GinkgoWriter, "Collecting pod logs from namespace %s...\n", namespace)

	// Get all pods in the namespace
	cmd := exec.Command("kubectl", "get", "pods",
		"-n", namespace,
		"-o", "jsonpath={.items[*].metadata.name}")
	output, err := utils.Run(cmd)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to list pods: %v\n", err)
		return
	}

	podNames := strings.Fields(output)
	for _, podName := range podNames {
		// Skip chaos client pods (they're collected separately)
		if strings.Contains(podName, "chaos-client") {
			continue
		}

		collectSinglePodLogs(debugDir, namespace, podName)
	}
}

// collectSinglePodLogs collects logs from a single pod (all containers)
func collectSinglePodLogs(debugDir, namespace, podName string) {
	// Get container names
	cmd := exec.Command("kubectl", "get", "pod", podName,
		"-n", namespace,
		"-o", "jsonpath={.spec.containers[*].name}")
	output, err := utils.Run(cmd)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get containers for pod %s: %v\n", podName, err)
		return
	}

	containers := strings.Fields(output)
	for _, container := range containers {
		cmd = exec.Command("kubectl", "logs",
			"-n", namespace,
			podName,
			"-c", container,
			"--tail", "1000",
			"--timestamps")
		logs, err := cmd.CombinedOutput()
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get logs for %s/%s: %v\n", podName, container, err)
			continue
		}

		logFile := filepath.Join(debugDir, fmt.Sprintf("pod-%s-%s.log", podName, container))
		if err := os.WriteFile(logFile, logs, 0644); err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to write logs for %s/%s: %v\n", podName, container, err)
		}
	}

	// Also collect previous logs if pod restarted
	cmd = exec.Command("kubectl", "logs",
		"-n", namespace,
		podName,
		"--previous",
		"--tail", "1000",
		"--timestamps")
	logs, err := cmd.CombinedOutput()
	if err == nil && len(logs) > 0 {
		logFile := filepath.Join(debugDir, fmt.Sprintf("pod-%s-previous.log", podName))
		_ = os.WriteFile(logFile, logs, 0644)
	}
}

// writePreDeleteLogs writes logs that were captured synchronously before pod deletions.
// These logs are not retrievable via kubectl after the pod has been replaced.
func writePreDeleteLogs(debugDir string) {
	preDeleteLogsMu.Lock()
	snapshot := make(map[string][]byte, len(preDeleteLogs))
	for k, v := range preDeleteLogs {
		snapshot[k] = v
	}
	preDeleteLogsMu.Unlock()

	if len(snapshot) == 0 {
		return
	}

	_, _ = fmt.Fprintf(GinkgoWriter, "Writing %d pre-deletion log snapshots...\n", len(snapshot))

	for key, content := range snapshot {
		// key is "podName/containerName"
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}
		podName, container := parts[0], parts[1]
		filename := fmt.Sprintf("pod-%s-%s-pre-delete.log", podName, container)
		logFile := filepath.Join(debugDir, filename)
		if err := os.WriteFile(logFile, content, 0644); err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to write pre-delete logs for %s/%s: %v\n", podName, container, err)
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter, "Wrote %d bytes to %s\n", len(content), logFile)
		}
	}
}

// collectChaosClientLogs collects logs and metrics from chaos client pod
func collectChaosClientLogs(debugDir, namespace, chaosPodName string) {
	if chaosPodName == "" {
		return
	}

	_, _ = fmt.Fprintf(GinkgoWriter, "Collecting chaos client logs for %s...\n", chaosPodName)

	// Get chaos client logs
	cmd := exec.Command("kubectl", "logs",
		"-n", namespace,
		chaosPodName,
		"--timestamps")
	logs, err := cmd.CombinedOutput()
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get chaos client logs: %v\n", err)
	} else {
		logFile := filepath.Join(debugDir, fmt.Sprintf("chaos-client-%s.log", chaosPodName))
		if err := os.WriteFile(logFile, logs, 0644); err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to write chaos client logs: %v\n", err)
		}
	}

	// Try to get metrics JSON if available
	cmd = exec.Command("kubectl", "exec",
		"-n", namespace,
		chaosPodName,
		"--",
		"cat", "/tmp/metrics.json")
	metrics, err := cmd.CombinedOutput()
	if err == nil && len(metrics) > 0 {
		metricsFile := filepath.Join(debugDir, fmt.Sprintf("chaos-metrics-%s.json", chaosPodName))
		_ = os.WriteFile(metricsFile, metrics, 0644)
	}
}

// collectClusterState collects general cluster state information
func collectClusterState(debugDir, namespace string) {
	_, _ = fmt.Fprintf(GinkgoWriter, "Collecting cluster state for namespace %s...\n", namespace)

	// Get all pods
	cmd := exec.Command("kubectl", "get", "pods",
		"-n", namespace,
		"-L", "redis.chuck-chuck-chuck.net/role",
		"-o", "wide")
	output, err := cmd.CombinedOutput()
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Warning: failed to get pods: %v\n", err)
	}
	_ = os.WriteFile(filepath.Join(debugDir, "pods.txt"), output, 0644)

	// Get events
	cmd = exec.Command("kubectl", "get", "events",
		"-n", namespace,
		"--sort-by", ".lastTimestamp")
	output, err = cmd.CombinedOutput()
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Warning: failed to get events: %v\n", err)
	}
	_ = os.WriteFile(filepath.Join(debugDir, "events.txt"), output, 0644)

	// Get all LittleRed CRs
	cmd = exec.Command("kubectl", "get", "littlered",
		"-n", namespace,
		"-o", "yaml")
	output, err = cmd.CombinedOutput()
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Warning: failed to get littlered CRs: %v\n", err)
	}
	_ = os.WriteFile(filepath.Join(debugDir, "littlered-crs.yaml"), output, 0644)

	// Get StatefulSets
	cmd = exec.Command("kubectl", "get", "statefulsets",
		"-n", namespace,
		"-o", "yaml")
	output, err = cmd.CombinedOutput()
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Warning: failed to get statefulsets: %v\n", err)
	}
	_ = os.WriteFile(filepath.Join(debugDir, "statefulsets.yaml"), output, 0644)

	// Get Services
	cmd = exec.Command("kubectl", "get", "services",
		"-n", namespace,
		"-o", "yaml")
	output, err = cmd.CombinedOutput()
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Warning: failed to get services: %v\n", err)
	}
	_ = os.WriteFile(filepath.Join(debugDir, "services.yaml"), output, 0644)

	_, _ = fmt.Fprintf(GinkgoWriter, "Cluster state collection complete\n")
}

// writeTestMetadata writes test execution metadata
func writeTestMetadata(debugDir, namespace, crName, chaosPodName string) {
	spec := CurrentSpecReport()

	// Get failure message and node text
	failureMsg := spec.FailureMessage()
	if failureMsg == "" {
		failureMsg = "(no failure message)"
	}

	failureLocation := spec.FailureLocation().String()
	if failureLocation == "" {
		failureLocation = "(no failure location)"
	}

	metadata := fmt.Sprintf(`Test Failure Debug Artifacts
==============================

Test: %s
Full Path: %s
Failed: %v
State: %s

Namespace: %s
CR Name: %s
Chaos Pod: %s

Test Start Time: %s
Collection Time: %s
Test Duration: %v
Time Since Test Start: %v

Failure Location:
%s

Failure Message:
%s

Stack Trace:
%s
`,
		spec.LeafNodeText,
		spec.FullText(),
		spec.Failed(),
		spec.State.String(),
		namespace,
		crName,
		chaosPodName,
		testStartTime.Format(time.RFC3339),
		time.Now().Format(time.RFC3339),
		spec.RunTime,
		time.Since(testStartTime),
		failureLocation,
		failureMsg,
		spec.Failure.Location.FullStackTrace,
	)

	metadataFile := filepath.Join(debugDir, "test-metadata.txt")
	if err := os.WriteFile(metadataFile, []byte(metadata), 0644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to write test metadata: %v\n", err)
	}
}
