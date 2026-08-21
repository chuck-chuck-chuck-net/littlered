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

var _ = Describe("Cluster Mode Functional Testing", Label("cluster"), Ordered, func() {

	Context("Basic Operations (Masters + Replicas)", Ordered, func() {
		const crName = "func-cluster-basic"

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
			totalNodes := clusterTotalNodes(clusterReplicasPerShard)
			By(fmt.Sprintf("creating a Redis Cluster with %d shards and %d replica(s) per shard (%d nodes total)",
				clusterShards, clusterReplicasPerShard, totalNodes))
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

			By("waiting for cluster to be Running")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup due to failure and DEBUG_ON_FAILURE=true")
				return
			}
			By("cleaning up cluster CR")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
		})

		It("should have correct topology and state", func() {
			By("checking each shard StatefulSet has all replicas ready")
			for k := 0; k < clusterShards; k++ {
				cmd := exec.Command("kubectl", "get", "statefulset",
					fmt.Sprintf("%s-shard-%d", crName, k),
					"-n", testNamespace, "-o", "jsonpath={.status.readyReplicas}")
				output, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
				Expect(output).To(Equal(fmt.Sprintf("%d", 1+clusterReplicasPerShard)))
			}

			By("checking cluster state is ok")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "INFO")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("cluster_state:ok"))
				g.Expect(output).To(ContainSubstring("cluster_slots_assigned:16384"))
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			// Role flags propagate across the gossip bus a beat after the operator
			// reattaches the last replica: CLUSTER REPLICATE flips a node's own role
			// to slave instantly (so the operator's self-view gather declares Running),
			// but a single node's CLUSTER NODES view can still show that replica as a
			// master for ~1-2s while its sync completes and gossip converges. Poll
			// until cluster-0's view reflects the settled topology, mirroring the
			// cluster_state Eventually above.
			By(fmt.Sprintf("verifying %d masters and %d replicas", clusterShards, clusterShards*clusterReplicasPerShard))
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "NODES")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				lines := strings.Split(strings.TrimSpace(output), "\n")
				masterCount := 0
				replicaCount := 0
				for _, line := range lines {
					if strings.Contains(line, "master") {
						masterCount++
					}
					if strings.Contains(line, "slave") {
						replicaCount++
					}
				}
				g.Expect(masterCount).To(Equal(clusterShards))
				g.Expect(replicaCount).To(Equal(clusterShards * clusterReplicasPerShard))
			}, 1*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should store and retrieve data with redirection", func() {
			By("setting keys")
			keys := []string{"key1", "key2", "key3"}
			for _, key := range keys {
				cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "-c", "SET", key, "value-"+key)
				_, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			}

			By("getting keys")
			for _, key := range keys {
				cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 2),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "-c", "GET", key)
				output, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
				Expect(strings.TrimSpace(output)).To(Equal("value-" + key))
			}
		})

		It("should track status correctly in the CR", func() {
			totalNodes := clusterTotalNodes(clusterReplicasPerShard)

			By("checking status.cluster.state")
			cmd := exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.cluster.state}")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(Equal("ok"))

			By("checking node list in status")
			cmd = exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.cluster.nodes[*].podName}")
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			nodes := strings.Fields(output)
			Expect(len(nodes)).To(Equal(totalNodes))

			verifyClusterTopologySync(testNamespace, crName, totalNodes)
		})
	})

	Context("0-Replica Mode Self-Healing", Ordered, func() {
		const crName = "func-cluster-0replica"

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
			By(fmt.Sprintf("creating a %d-shard cluster with no replicas", clusterShards))
			cr := clusterCR(crName, 0, "", "")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for cluster to be ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 4*time.Minute, 5*time.Second).Should(Succeed())
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup due to failure and DEBUG_ON_FAILURE=true")
				return
			}
			By("cleaning up cluster")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
		})

		It("should heal cluster after master pod deletion in 0-replica mode", func() {
			totalNodes := clusterTotalNodes(0)

			// In 0-replica mode, deleting a pod means losing data and slots.
			// The operator should re-assign slots to the new node.
			victimPod := clusterMasterPod(crName, 1)

			By(fmt.Sprintf("recording initial NodeID of victim pod %s", victimPod))
			oldNodeID, err := getPodNodeID(testNamespace, victimPod)
			Expect(err).NotTo(HaveOccurred())

			By(fmt.Sprintf("deleting master pod %s", victimPod))
			_, err = deletePod(testNamespace, victimPod)
			Expect(err).NotTo(HaveOccurred())

			// Wait for shard master to change (slot 8000 is in the middle of shard 1),
			// querying via shard 0's master.
			waitForShardMasterChange(testNamespace, clusterMasterPod(crName, 0), 8000, oldNodeID)

			By("waiting for cluster to stabilize")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "INFO")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("cluster_state:ok"))
				g.Expect(output).To(ContainSubstring("cluster_slots_assigned:16384"))
				g.Expect(output).To(ContainSubstring(fmt.Sprintf("cluster_known_nodes:%d", totalNodes)))
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			verifyClusterTopologySync(testNamespace, crName, totalNodes)
		})
	})

	Context("Failover Recovery (Masters + Replicas)", Ordered, func() {
		const crName = "func-cluster-failover"

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
			totalNodes := clusterTotalNodes(clusterReplicasPerShard)
			By(fmt.Sprintf("creating a %d-shard cluster with %d replica(s) per shard (%d nodes total)",
				clusterShards, clusterReplicasPerShard, totalNodes))
			cr := clusterCR(crName, clusterReplicasPerShard, "", "")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for cluster to be ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 4*time.Minute, 5*time.Second).Should(Succeed())
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup due to failure and DEBUG_ON_FAILURE=true")
				return
			}
			By("cleaning up cluster")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
		})

		It("should promote replica when master pod is deleted", func() {
			totalNodes := clusterTotalNodes(clusterReplicasPerShard)

			By("identifying initial master for shard 0")
			victimPod := clusterMasterPod(crName, 0)
			oldNodeID, err := getPodNodeID(testNamespace, victimPod)
			Expect(err).NotTo(HaveOccurred())

			By(fmt.Sprintf("deleting master pod %s", victimPod))
			_, err = deletePod(testNamespace, victimPod)
			Expect(err).NotTo(HaveOccurred())

			// Wait for shard master to change (slot 0 is in shard 0), querying via shard 1's master
			waitForShardMasterChange(testNamespace, clusterMasterPod(crName, 1), 0, oldNodeID)

			By("waiting for cluster to become ok again")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 1),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "INFO")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("cluster_state:ok"))
			}, 4*time.Minute, 5*time.Second).Should(Succeed())

			By(fmt.Sprintf("verifying topology is recovered (%d nodes, %d masters)", totalNodes, clusterShards))
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 1),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "NODES")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				lines := strings.Split(strings.TrimSpace(out), "\n")

				mastersWithSlots := 0
				for _, line := range lines {
					if strings.Contains(line, "master") {
						fields := strings.Fields(line)
						if len(fields) > 8 {
							mastersWithSlots++
						}
					}
				}
				g.Expect(mastersWithSlots).To(Equal(clusterShards))
				g.Expect(len(lines)).To(Equal(totalNodes))
				g.Expect(out).NotTo(ContainSubstring("fail"))
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			verifyClusterTopologySync(testNamespace, crName, totalNodes)
		})

		It("should reassign replica when replica pod is deleted", func() {
			totalNodes := clusterTotalNodes(clusterReplicasPerShard)

			// Target shard 0's first replica
			victimPod := clusterReplicaPod(crName, 0, 1)
			By(fmt.Sprintf("recording initial NodeID of replica pod %s", victimPod))
			oldNodeID, err := getPodNodeID(testNamespace, victimPod)
			Expect(err).NotTo(HaveOccurred())

			By(fmt.Sprintf("deleting replica pod %s", victimPod))
			_, err = deletePod(testNamespace, victimPod)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for new replica to join with a different NodeID")
			Eventually(func(g Gomega) {
				newNodeID, err := getPodNodeID(testNamespace, victimPod)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to get new NodeID for replica")
				g.Expect(newNodeID).NotTo(Equal(oldNodeID), "Replica NodeID has not changed yet")
			}, 3*time.Minute, 10*time.Second).Should(Succeed())

			By("waiting for cluster to stabilize")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "INFO")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("cluster_state:ok"))
				g.Expect(output).To(ContainSubstring(fmt.Sprintf("cluster_known_nodes:%d", totalNodes)))
			}, 4*time.Minute, 5*time.Second).Should(Succeed())

			verifyClusterTopologySync(testNamespace, crName, totalNodes)
		})
	})

	Context("Cluster Cleanup", Ordered, func() {
		const crName = "func-cluster-cleanup"

		It("should clean up all resources when CR is deleted", func() {
			AddReportEntry("cr:" + crName)
			By("creating cluster for cleanup")
			cr := clusterCR(crName, 0, "", "")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for Running state")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 4*time.Minute, 5*time.Second).Should(Succeed())

			By("deleting the CR")
			cmd = exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("verifying resources are gone")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "statefulset", crName+"-shard-0", "-n", testNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred())
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

	Context("Custom Configuration", Ordered, func() {
		const crName = "func-cluster-config"

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
			By("creating cluster with custom configuration")
			cr := clusterCR(crName, clusterReplicasPerShard,
				"    clusterNodeTimeout: 10000\n",
				`  config:
    maxmemory: "256Mi"
    maxmemoryPolicy: noeviction
`)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for Running phase")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 4*time.Minute, 5*time.Second).Should(Succeed())
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup due to failure and DEBUG_ON_FAILURE=true")
				return
			}
			By("cleaning up cluster")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
		})

		It("should apply custom redis configuration to all nodes", func() {
			for _, podName := range clusterPodNames(crName, clusterShards, clusterReplicasPerShard) {
				cmd := exec.Command("kubectl", "exec", podName,
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CONFIG", "GET", "maxmemory-policy")
				output, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
				Expect(output).To(ContainSubstring("noeviction"))

				cmd = exec.Command("kubectl", "exec", podName,
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CONFIG", "GET", "cluster-node-timeout")
				output, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
				Expect(output).To(ContainSubstring("10000"))
			}
		})
	})

	Context("Debug Research Mode (No slots)", Ordered, func() {
		const crName = "func-cluster-debug"

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
			By("Creating an empty cluster with debug-skip-slot-assignment annotation")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
  annotations:
    redis.chuck-chuck-chuck.net/debug-skip-slot-assignment: "true"
spec:
  mode: cluster
  cluster:
    shards: %d
    replicasPerShard: 0
`, crName, testNamespace, clusterShards)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup due to failure and DEBUG_ON_FAILURE=true")
				return
			}
			By("cleaning up cluster")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=1m")
			_, _ = utils.Run(cmd)
		})

		It("should stay in 'fail' state without slots assigned", func() {
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "INFO")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("cluster_state:fail"))
				g.Expect(output).To(ContainSubstring("cluster_slots_assigned:0"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})
	})
})
