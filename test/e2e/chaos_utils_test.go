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

// kubectlCtx runs kubectl against the given kube-context, omitting the --context flag
// entirely when ctx is "" so the ambient (single-site) call is byte-for-byte what it was
// before the multi-site harness existed. Multi-site callers pass a per-pod site context.
func kubectlCtx(ctx string, args ...string) (string, error) {
	if ctx != "" {
		args = append([]string{"--context", ctx}, args...)
	}
	return utils.Run(exec.Command("kubectl", args...))
}

// deployChaosClient deploys a chaos test client pod and returns the pod name (ambient context).
func deployChaosClient(namespace, name, addresses, keyPrefix string, clusterMode bool, duration time.Duration) (string, error) {
	return deployChaosClientCtx("", namespace, name, addresses, keyPrefix, clusterMode, duration)
}

// deployChaosClientCtx deploys a chaos test client pod in the given kube-context (""=ambient)
// and returns the pod name. Multi-site runs deploy the client into a site that can reach the
// stretched cluster's routed pod IPs.
func deployChaosClientCtx(ctx, namespace, name, addresses, keyPrefix string, clusterMode bool, duration time.Duration) (string, error) {
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

	if ctx != "" {
		full := []string{"--context", ctx, "apply", "-f", "-"}
		cmd := exec.Command("kubectl", full...)
		cmd.Stdin = strings.NewReader(pod)
		_, err := utils.Run(cmd)
		return podName, err
	}
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(pod)
	_, err := utils.Run(cmd)
	return podName, err
}

// waitForChaosClientComplete waits for the chaos client pod to complete (ambient context).
func waitForChaosClientComplete(namespace, podName string, timeout time.Duration) error {
	return waitForChaosClientCompleteCtx("", namespace, podName, timeout)
}

// waitForChaosClientCompleteCtx waits for the chaos client pod to complete in the given context.
func waitForChaosClientCompleteCtx(ctx, namespace, podName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		output, err := kubectlCtx(ctx, "get", "pod", podName,
			"-n", namespace, "-o", "jsonpath={.status.phase}")
		if err != nil {
			return err
		}
		if output == "Succeeded" || output == "Failed" {
			return nil
		}
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("timeout waiting for pod %s to complete", podName)
}

// getChaosClientMetrics retrieves metrics from a completed chaos client pod (ambient context).
func getChaosClientMetrics(namespace, podName string) (*chaos.MetricsSnapshot, error) {
	return getChaosClientMetricsCtx("", namespace, podName)
}

// getChaosClientMetricsCtx retrieves metrics from a completed chaos client pod in the given context.
func getChaosClientMetricsCtx(ctx, namespace, podName string) (*chaos.MetricsSnapshot, error) {
	output, err := kubectlCtx(ctx, "logs", podName, "-n", namespace)
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
	return nil, fmt.Errorf("METRICS_JSON not found in pod logs")
}

// deleteChaosClient deletes the chaos client pod (ambient context).
func deleteChaosClient(namespace, podName string) {
	deleteChaosClientCtx("", namespace, podName)
}

// deleteChaosClientCtx deletes the chaos client pod in the given context.
func deleteChaosClientCtx(ctx, namespace, podName string) {
	_, _ = kubectlCtx(ctx, "delete", "pod", podName, "-n", namespace, "--ignore-not-found")
}
