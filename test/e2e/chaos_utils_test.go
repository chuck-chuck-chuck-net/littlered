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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/littlered-operator/littlered-operator/test/chaos"
	"github.com/littlered-operator/littlered-operator/test/utils"
)

// getChaosClientImage returns the chaos client image to use
func getChaosClientImage() string {
	if img := os.Getenv("CHAOS_CLIENT_IMAGE"); img != "" {
		return img
	}
	return "ghcr.io/littlered-operator/littlered-chaos-client:latest"
}

// deployChaosClient deploys a chaos test client pod and returns the pod name
func deployChaosClient(namespace, name, addresses, keyPrefix string, clusterMode bool, duration time.Duration) (string, error) {
	podName := fmt.Sprintf("chaos-client-%s", name)

	image := getChaosClientImage()

	clusterArg := ""
	if clusterMode {
		clusterArg = "\n    - \"-cluster\""
	}

	pod := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    app: chaos-client
    test: %s
spec:
  restartPolicy: Never
  containers:
  - name: chaos-client
    image: %s
    imagePullPolicy: Always
    args:
    - "-addrs=%s"
    - "-prefix=%s"
    - "-duration=%s"
    - "-status-interval=5s"
    - "-write-rate=100ms"
    - "-timeout=500ms"%s
`, podName, namespace, name, image, addresses, keyPrefix, duration.String(), clusterArg)

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(pod)
	_, err := utils.Run(cmd)
	return podName, err
}

// chaosClientLogTailLines is how much of the chaos client's log rides along in a
// failure message. The client prints its configuration, then one line per
// connection attempt, then its final results — so the last ~20 lines carry either
// the metrics block or the reason it never got that far, which is precisely the
// distinction a bare "METRICS_JSON not found" erases.
const chaosClientLogTailLines = 20

// chaosClientLogTail returns the last chaosClientLogTailLines of the pod's log,
// prefixed for embedding in an error message. It never fails: a diagnostic that
// can itself error is a diagnostic that gets dropped from the error path.
func chaosClientLogTail(namespace, podName string) string {
	cmd := exec.Command("kubectl", "logs", podName, "-n", namespace,
		"--tail", strconv.Itoa(chaosClientLogTailLines), "--timestamps")
	out, err := cmd.CombinedOutput()
	if err != nil && len(out) == 0 {
		return fmt.Sprintf("\n--- last %d log lines of %s: unavailable (%v) ---",
			chaosClientLogTailLines, podName, err)
	}
	return fmt.Sprintf("\n--- last %d log lines of %s ---\n%s--- end of log tail ---",
		chaosClientLogTailLines, podName, string(out))
}

// chaosClientTerminationDetail describes how the pod's container exited, for the
// failure message. Best-effort: an empty string when kubectl cannot tell us.
func chaosClientTerminationDetail(namespace, podName string) string {
	cmd := exec.Command("kubectl", "get", "pod", podName, "-n", namespace, "-o",
		"jsonpath={.status.containerStatuses[0].state.terminated.exitCode}"+
			"{\" \"}{.status.containerStatuses[0].state.terminated.reason}")
	out, err := utils.Run(cmd)
	if err != nil || strings.TrimSpace(out) == "" {
		return ""
	}
	return " (container exit: " + strings.TrimSpace(out) + ")"
}

// waitForChaosClientComplete waits for the chaos client pod to finish and reports
// whether it finished *successfully*.
//
// Succeeded and Failed are deliberately NOT collapsed. A client that exited
// non-zero is a different fact from one that completed its run, and treating them
// alike is what turned a client-side connectivity timeout into the opaque
// "METRICS_JSON not found in pod logs" from the caller's next line — the real
// cause (`Failed to create client: timeout waiting for Redis connectivity`) was
// sitting in the pod's log the whole time, so it now rides along in the error.
//
// The client exits non-zero in exactly two ways, and both are genuine spec
// failures: 1 when it never reached the store, 2 when it detected data
// corruption. The corruption case prints METRICS_JSON *before* exiting, so the
// log tail below carries the metrics block as well as "CRITICAL: Data corruption
// detected!" — strictly more than the caller's DataCorruptions assertion would
// have reported.
func waitForChaosClientComplete(namespace, podName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("kubectl", "get", "pod", podName,
			"-n", namespace, "-o", "jsonpath={.status.phase}")
		output, err := utils.Run(cmd)
		if err != nil {
			return err
		}
		switch output {
		case "Succeeded":
			return nil
		case "Failed":
			return fmt.Errorf("chaos client pod %s exited unsuccessfully%s%s",
				podName,
				chaosClientTerminationDetail(namespace, podName),
				chaosClientLogTail(namespace, podName))
		}
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("timeout waiting for pod %s to complete%s",
		podName, chaosClientLogTail(namespace, podName))
}

// getChaosClientMetrics retrieves metrics from a completed chaos client pod
func getChaosClientMetrics(namespace, podName string) (*chaos.MetricsSnapshot, error) {
	cmd := exec.Command("kubectl", "logs", podName, "-n", namespace)
	output, err := utils.Run(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}

	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "METRICS_JSON:") {
			jsonStr := strings.TrimPrefix(line, "METRICS_JSON:")
			var metrics chaos.MetricsSnapshot
			if err := json.Unmarshal([]byte(jsonStr), &metrics); err != nil {
				return nil, fmt.Errorf("failed to parse metrics JSON: %w", err)
			}
			return &metrics, nil
		}
	}
	// The client only ever omits this line by dying before its traffic window
	// closed, so the log tail is the diagnosis. Deliberately NOT fixed by making
	// the client emit a zeroed METRICS_JSON on connect failure: MetricsSnapshot's
	// WriteAvailability() returns 1.0 when WriteAttempts == 0, so a zeroed record
	// satisfies every `>= 0.99` availability assertion in the suite and converts
	// this red into a FALSE GREEN. An opaque error is bad; a passing test for a
	// client that never connected is far worse. Fix legibility here, never there.
	return nil, fmt.Errorf("METRICS_JSON not found in pod logs%s",
		chaosClientLogTail(namespace, podName))
}

// deleteChaosClient deletes the chaos client pod
func deleteChaosClient(namespace, podName string) {
	cmd := exec.Command("kubectl", "delete", "pod", podName, "-n", namespace, "--ignore-not-found")
	_, _ = utils.Run(cmd)
}
