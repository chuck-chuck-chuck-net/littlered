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
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive // dot-import is the Ginkgo/Gomega convention in tests
	. "github.com/onsi/gomega"    //nolint:revive

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/test/utils"
)

// =============================================================================
// Failover-mode e2e helpers (ADR-011, M6).
//
// Failover mode has NO Sentinels, so the ground truth for topology assertions
// is `INFO replication` on the data pods themselves — never any SENTINEL
// command. verifyFailoverTopologySync is the failover-mode sibling of
// verifySentinelTopologySync (cluster_utils_test.go).
// =============================================================================

// failoverAnnRole / failoverAnnMasterIP / failoverAnnEpoch are the operator's
// assignment-channel annotation keys (ADR-011 §3; mirror of the constants in
// internal/controller/resources.go).
const (
	failoverAnnRole     = "redis.chuck-chuck-chuck.net/assigned-role"
	failoverAnnMasterIP = "redis.chuck-chuck-chuck.net/assigned-master-ip"
	failoverAnnEpoch    = "redis.chuck-chuck-chuck.net/assignment-epoch"
)

// failoverCR renders a mode:failover LittleRed CR manifest, mirroring the
// sentinel-mode YAML shape used across the suite. downAfterMs 3000 matches the
// sentinel failover tier's fast-detection setting. metaAnnotations lands under
// metadata.annotations (e.g. the disable-event-monitoring kill switch).
func failoverCR(name string, replicas, downAfterMs int, metaAnnotations map[string]string, extraFailoverFields string) string {
	ann := ""
	if len(metaAnnotations) > 0 {
		ann = "  annotations:\n"
		for k, v := range metaAnnotations {
			ann += fmt.Sprintf("    %s: %q\n", k, v)
		}
	}
	return fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
%sspec:
  mode: failover
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
  failover:
    replicas: %d
    downAfterMilliseconds: %d
%s`, name, testNamespace, ann, replicas, downAfterMs, extraFailoverFields)
}

// deployFailover applies a failover-mode CR and waits for phase Running.
func deployFailover(crName string, replicas, downAfterMs int, metaAnnotations map[string]string) {
	AddReportEntry("cr:" + crName)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(failoverCR(crName, replicas, downAfterMs, metaAnnotations, ""))
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())

	By("waiting for the failover instance to reach Running")
	Eventually(func(g Gomega) {
		g.Expect(getPhase(crName)).To(Equal("Running"))
	}, 3*time.Minute, 5*time.Second).Should(Succeed())
}

// cleanupFailoverCR deletes the CR unless a failure should be kept for debugging.
func cleanupFailoverCR(crName string) {
	if debugOnFailure && suiteOrSpecFailed() {
		By("skipping failover CR cleanup to allow debugging")
		return
	}
	cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=60s")
	_, _ = utils.Run(cmd)
}

// failoverDataPods returns the instance's data pod names (failover mode has
// only data pods — component=redis).
func failoverDataPods(crName string) []string {
	out, _ := utils.Run(exec.Command("kubectl", "get", "pods", "-n", testNamespace,
		"-l", "app.kubernetes.io/instance="+crName+",app.kubernetes.io/component=redis",
		"-o", "jsonpath={.items[*].metadata.name}"))
	return strings.Fields(out)
}

// getPodAnnotation returns one metadata.annotation of a pod ("" on error/absent).
func getPodAnnotation(namespace, pod, key string) string {
	jp := fmt.Sprintf("jsonpath={.metadata.annotations.%s}", strings.ReplaceAll(key, ".", `\.`))
	out, err := utils.Run(exec.Command("kubectl", "get", "pod", pod, "-n", namespace, "-o", jp))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// getPodRoleLabel returns the operator-managed role label of a pod.
func getPodRoleLabel(namespace, pod string) string {
	out, _ := utils.Run(exec.Command("kubectl", "get", "pod", pod, "-n", namespace,
		"-o", `jsonpath={.metadata.labels.redis\.chuck-chuck-chuck\.net/role}`))
	return strings.TrimSpace(out)
}

// podContainerLogContains reports whether the CURRENT container log of
// pod/container contains substr (false on any kubectl error). Used for
// mechanism OBSERVATIONS (e.g. the kill-9 epoch-gate park log, a
// timing-dependent transient) — never for assertions.
func podContainerLogContains(namespace, pod, container, substr string) bool {
	out, err := utils.Run(exec.Command("kubectl", "logs", pod, "-n", namespace, "-c", container))
	return err == nil && strings.Contains(out, substr)
}

// replicationView is the parsed `INFO replication` of one data pod.
type replicationView struct {
	role       string // role:master|slave
	masterHost string // master_host (replicas only)
	linkStatus string // master_link_status (replicas only)
}

// getReplicationView runs INFO replication on a pod and parses the fields the
// topology assertions need. redis-cli output uses \r\n line endings.
func getReplicationView(namespace, pod string) (replicationView, error) {
	var v replicationView
	out, err := redisExec(namespace, pod, "INFO", "replication")
	if err != nil {
		return v, err
	}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "role:"):
			v.role = strings.TrimPrefix(line, "role:")
		case strings.HasPrefix(line, "master_host:"):
			v.masterHost = strings.TrimPrefix(line, "master_host:")
		case strings.HasPrefix(line, "master_link_status:"):
			v.linkStatus = strings.TrimPrefix(line, "master_link_status:")
		}
	}
	return v, nil
}

// verifyFailoverTopologySync cross-validates the operator's reported status,
// the K8s role labels, and the ACTUAL replication topology read via
// `INFO replication` on every data pod (there are no Sentinels to ask in this
// mode). It asserts:
//
//   - exactly 1+expectedReplicas data pods exist;
//   - exactly ONE reachable pod reports role:master;
//   - every other pod reports role:slave, following the master's IP with
//     master_link_status:up;
//   - the K8s role labels match the observed roles;
//   - CR status.master.{podName,ip} matches the observed master;
//   - NO sentinel resources exist for the instance (no sentinel pods, no
//     {name}-sentinel Service, no {name}-sentinel StatefulSet).
func verifyFailoverTopologySync(namespace, crName string, expectedReplicas int) {
	By(fmt.Sprintf("verifying operator status for %s matches the actual replication topology (INFO replication)", crName))

	Eventually(func(g Gomega) {
		pods := failoverDataPods(crName)
		g.Expect(pods).To(HaveLen(1+expectedReplicas),
			fmt.Sprintf("expected %d data pods, got %v", 1+expectedReplicas, pods))

		// 1. Ground truth: INFO replication on every data pod.
		views := make(map[string]replicationView, len(pods))
		ips := make(map[string]string, len(pods))
		var masters []string
		for _, pod := range pods {
			v, err := getReplicationView(namespace, pod)
			g.Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("pod %s not reachable for INFO replication", pod))
			views[pod] = v
			ipOut, err := utils.Run(exec.Command("kubectl", "get", "pod", pod, "-n", namespace,
				"-o", "jsonpath={.status.podIP}"))
			g.Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("failed to get IP of pod %s", pod))
			ips[pod] = strings.TrimSpace(ipOut)
			if v.role == "master" {
				masters = append(masters, pod)
			}
		}
		g.Expect(masters).To(HaveLen(1), fmt.Sprintf("expected exactly one role:master, got %v", masters))
		masterPod := masters[0]
		masterIP := ips[masterPod]
		g.Expect(masterIP).NotTo(BeEmpty())

		// 2. Every other pod is a replica of the master with its link up.
		for _, pod := range pods {
			if pod == masterPod {
				continue
			}
			v := views[pod]
			g.Expect(v.role).To(Equal("slave"), fmt.Sprintf("pod %s: expected role:slave, got %q", pod, v.role))
			g.Expect(v.masterHost).To(Equal(masterIP),
				fmt.Sprintf("pod %s follows %q, expected the master IP %s", pod, v.masterHost, masterIP))
			g.Expect(v.linkStatus).To(Equal("up"), fmt.Sprintf("pod %s: master_link_status %q", pod, v.linkStatus))
		}

		// 3. K8s role labels match the observation.
		for _, pod := range pods {
			role := getPodRoleLabel(namespace, pod)
			if pod == masterPod {
				g.Expect(role).To(Equal("master"), fmt.Sprintf("pod %s is master but labeled %q", pod, role))
			} else {
				g.Expect(role).To(Equal("replica"), fmt.Sprintf("pod %s should be labeled replica, got %q", pod, role))
			}
		}

		// 4. CR status matches the observation.
		out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName, "-n", namespace, "-o", "json"))
		g.Expect(err).NotTo(HaveOccurred(), "failed to get LittleRed CR")
		var lr littleredv1alpha1.LittleRed
		g.Expect(json.Unmarshal([]byte(out), &lr)).To(Succeed())
		g.Expect(lr.Status.Master).NotTo(BeNil(), "CR status.master is nil")
		g.Expect(lr.Status.Master.PodName).To(Equal(masterPod), "status.master.podName mismatch")
		g.Expect(lr.Status.Master.IP).To(Equal(masterIP), "status.master.ip mismatch")

		// 5. No Sentinel resources of any kind for this instance (ADR-011 §2).
		sentinelPods, _ := utils.Run(exec.Command("kubectl", "get", "pods", "-n", namespace,
			"-l", "app.kubernetes.io/instance="+crName+",app.kubernetes.io/component=sentinel",
			"-o", "jsonpath={.items[*].metadata.name}"))
		g.Expect(strings.TrimSpace(sentinelPods)).To(BeEmpty(), "failover mode must not run sentinel pods")
		_, err = utils.Run(exec.Command("kubectl", "get", "service", crName+"-sentinel", "-n", namespace))
		g.Expect(err).To(HaveOccurred(), "the {name}-sentinel Service must not exist in failover mode")
		_, err = utils.Run(exec.Command("kubectl", "get", "statefulset", crName+"-sentinel", "-n", namespace))
		g.Expect(err).To(HaveOccurred(), "the {name}-sentinel StatefulSet must not exist in failover mode")
	}, 2*time.Minute, 5*time.Second).Should(Succeed(),
		"operator status, labels, and INFO-replication topology should converge")

	By("Failover topology sync validation passed")
}
