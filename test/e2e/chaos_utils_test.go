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

// waitForChaosClientComplete waits for the chaos client pod to complete
func waitForChaosClientComplete(namespace, podName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("kubectl", "get", "pod", podName,
			"-n", namespace, "-o", "jsonpath={.status.phase}")
		output, err := utils.Run(cmd)
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
	return nil, fmt.Errorf("METRICS_JSON not found in pod logs")
}

// deleteChaosClient deletes the chaos client pod
func deleteChaosClient(namespace, podName string) {
	cmd := exec.Command("kubectl", "delete", "pod", podName, "-n", namespace, "--ignore-not-found")
	_, _ = utils.Run(cmd)
}
