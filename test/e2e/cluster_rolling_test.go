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
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/littlered-operator/littlered-operator/test/utils"
)

var _ = Describe("Cluster Mode Rolling Update", Label("cluster"), Ordered, func() {
	const crName = "roll-cluster"

	BeforeAll(func() {
		AddReportEntry("cr:" + crName)
		totalNodes := clusterTotalNodes(clusterReplicasPerShard)
		By(fmt.Sprintf("creating a %d-shard cluster with %d replica(s) per shard for rolling update testing",
			clusterShards, clusterReplicasPerShard))
		cr := clusterCR(crName, clusterReplicasPerShard, "", `  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
`)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the cluster to be running")
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.phase}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).To(Equal("Running"))
		}, 4*time.Minute, 5*time.Second).Should(Succeed())

		_ = totalNodes // used in tests below
	})

	AfterAll(func() {
		if debugOnFailure && suiteOrSpecFailed() {
			return
		}
		cmd := exec.Command("kubectl", "delete", "littlered", crName,
			"-n", testNamespace, "--ignore-not-found", "--timeout=2m")
		_, _ = utils.Run(cmd)
	})

	It("should create a cluster for rolling update testing", func() {
		// Write test keys that spread across different hash slots, covering all
		// shards. redis-cli -c follows MOVED redirects automatically.
		testKeys := []struct{ key, value string }{
			{"roll-a", "val-a"},
			{"roll-b", "val-b"},
			{"roll-c", "val-c"},
			{"roll-d", "val-d"},
			{"roll-e", "val-e"},
		}
		for _, kv := range testKeys {
			cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "-c", "SET", kv.key, kv.value)
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("OK"))
		}
	})

	It("should perform rolling update when resources are changed", func() {
		totalNodes := clusterTotalNodes(clusterReplicasPerShard)
		podNames := clusterPodNames(crName, clusterShards, clusterReplicasPerShard)

		By("recording current pod UIDs before the update")
		oldUIDs := make([]string, totalNodes)
		for i, pod := range podNames {
			cmd := exec.Command("kubectl", "get", "pod", pod,
				"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
			uid, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			oldUIDs[i] = strings.TrimSpace(uid)
		}

		By("applying updated resource limits to trigger rolling update")
		cr := clusterCR(crName, clusterReplicasPerShard, "", `  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "200m"
      memory: "192Mi"
`)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for rolling update to begin (at least one pod replaced)")
		Eventually(func(g Gomega) {
			replacedCount := 0
			for i, pod := range podNames {
				cmd := exec.Command("kubectl", "get", "pod", pod,
					"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
				uid, _ := utils.Run(cmd)
				if strings.TrimSpace(uid) != oldUIDs[i] {
					replacedCount++
				}
			}
			g.Expect(replacedCount).To(BeNumerically(">", 0))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("waiting for all %d cluster pods to be replaced", totalNodes))
		Eventually(func(g Gomega) {
			replacedCount := 0
			for i, pod := range podNames {
				cmd := exec.Command("kubectl", "get", "pod", pod,
					"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
				uid, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				if strings.TrimSpace(uid) != oldUIDs[i] {
					replacedCount++
				}
			}
			g.Expect(replacedCount).To(Equal(totalNodes),
				fmt.Sprintf("All %d pods should have been replaced by the rolling update", totalNodes))
		}, time.Duration(totalNodes)*90*time.Second, 10*time.Second).Should(Succeed())

		By("verifying cluster is healthy after rolling update")
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "CLUSTER", "INFO")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).To(ContainSubstring("cluster_state:ok"))
			g.Expect(output).To(ContainSubstring("cluster_slots_assigned:16384"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("should preserve data after rolling update", func() {
		// All keys written before the rolling update must survive.
		// When each master pod was replaced: its replica held all data, was promoted
		// to master during the node-timeout window, then the old master rejoined as
		// a fresh replica and received a full resync from the promoted master.
		// The data chain was never broken.
		testKeys := []struct{ key, value string }{
			{"roll-a", "val-a"},
			{"roll-b", "val-b"},
			{"roll-c", "val-c"},
			{"roll-d", "val-d"},
			{"roll-e", "val-e"},
		}
		for _, kv := range testKeys {
			cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "-c", "GET", kv.key)
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal(kv.value),
				"key %q should still hold its value after rolling update", kv.key)
		}
	})

	It("should have cluster status Running after rolling update", func() {
		totalNodes := clusterTotalNodes(clusterReplicasPerShard)

		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.phase}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).To(Equal("Running"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying operator topology matches actual cluster nodes after rolling update")
		verifyClusterTopologySync(testNamespace, crName, totalNodes)
	})
})
