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
	"math/rand"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/littlered-operator/littlered-operator/test/utils"
)

var _ = Describe("Cluster Mode Chaos Testing", Label("cluster"), Ordered, func() {

	Context("Baseline Stability (Masters, 0 Replicas)", Ordered, func() {
		const crName = "chaos-cluster-stable"
		var chaosPodName string
		const testDuration = 30 * time.Second

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
			By(fmt.Sprintf("creating a %d-shard cluster with no replicas", clusterShards))
			cr := clusterCR(crName, 0, "", "")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("deploying chaos client pod simultaneously")
			chaosPodName, err = deployChaosClient(testNamespace, "stable", crName+":6379", "chaos-stable", true, testDuration)
			Expect(err).NotTo(HaveOccurred())
			AddReportEntry("chaos:" + chaosPodName)

			By("waiting for cluster to be ready and ok")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))

				cmd = exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "INFO")
				output, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("cluster_state:ok"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("waiting 10 seconds for baseline traffic")
			time.Sleep(10 * time.Second)
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				return
			}
			deleteChaosClient(testNamespace, chaosPodName)
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=2m")
			_, _ = utils.Run(cmd)
		})

		It("should maintain 100% availability under stable conditions", func() {
			err := waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
			Expect(err).NotTo(HaveOccurred())

			metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
			Expect(err).NotTo(HaveOccurred())

			Expect(metrics.WriteAvailability()).To(BeNumerically(">=", 0.99))
			Expect(metrics.DataCorruptions).To(Equal(int64(0)))
		})
	})

	Context("Resilience with Replicas (Masters + Replicas)", Ordered, func() {
		var currentCRName string

		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				return
			}
			if currentCRName == "" {
				return
			}
			By("cleaning up cluster " + currentCRName)
			cmd := exec.Command("kubectl", "delete", "littlered", currentCRName, "-n", testNamespace, "--ignore-not-found", "--timeout=2m")
			_, _ = utils.Run(cmd)
			// Wait for all pods to fully terminate before the next test starts.
			cmd = exec.Command("kubectl", "wait", "--for=delete", "pod",
				"-l", "app.kubernetes.io/instance="+currentCRName,
				"-n", testNamespace, "--timeout=3m")
			_, _ = utils.Run(cmd)
			currentCRName = ""
		})

		for _, mode := range restartModes {
			mode := mode // capture range variable

			It(fmt.Sprintf("should maintain high availability during master failure (%s)", mode.Name), func() {
				crName := fmt.Sprintf("chaos-cluster-master-%s", mode.Name)
				currentCRName = crName
				totalNodes := clusterTotalNodes(clusterReplicasPerShard)
				const testDuration = 60 * time.Second

				AddReportEntry("cr:" + crName)
				By(fmt.Sprintf("creating a %d-shard cluster with %d replica(s) per shard",
					clusterShards, clusterReplicasPerShard))
				cr := clusterCR(crName, clusterReplicasPerShard, "", "")
				cmd := exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(cr)
				_, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())

				By("deploying chaos client pod simultaneously")
				chaosPodName, err := deployChaosClient(testNamespace, "master-"+mode.Name, crName+":6379", "chaos-master", true, testDuration)
				Expect(err).NotTo(HaveOccurred())
				AddReportEntry("chaos:" + chaosPodName)
				defer func() {
					if debugOnFailure && suiteOrSpecFailed() {
						return
					}
					deleteChaosClient(testNamespace, chaosPodName)
				}()

				By("waiting for cluster to be ready and ok")
				Eventually(func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "littlered", crName,
						"-n", testNamespace, "-o", "jsonpath={.status.phase}")
					output, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).To(Equal("Running"))

					cmd = exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
						"-n", testNamespace, "-c", "redis", "--",
						"redis-cli", "CLUSTER", "INFO")
					output, err = utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).To(ContainSubstring("cluster_state:ok"))
				}, 5*time.Minute, 5*time.Second).Should(Succeed())

				By("waiting 10 seconds for baseline traffic")
				time.Sleep(10 * time.Second)

				victimPod := clusterMasterPod(crName, 0)
				victimNodeID, _ := getPodNodeID(testNamespace, victimPod)

				By(fmt.Sprintf("deleting master pod %s (%s)", victimPod, mode.Name))
				_, err = deletePodMode(testNamespace, victimPod, mode.Graceful)
				Expect(err).NotTo(HaveOccurred())

				// Wait for cluster to detect failure via shard-1's master (a surviving node in another shard)
				waitForClusterFailureDetected(testNamespace, crName, clusterMasterPod(crName, 1), totalNodes, []string{victimNodeID})

				err = waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
				Expect(err).NotTo(HaveOccurred())

				metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
				Expect(err).NotTo(HaveOccurred())
				GinkgoWriter.Printf("Master Failure Availability (%s): %.2f%%\n", mode.Name, metrics.WriteAvailability()*100)

				Expect(metrics.DataCorruptions).To(Equal(int64(0)))
				Expect(metrics.WriteAvailability()).To(BeNumerically(">=", 0.80))
			})

			It(fmt.Sprintf("should maintain 100%% availability during replica failure (%s)", mode.Name), func() {
				crName := fmt.Sprintf("chaos-cluster-replica-%s", mode.Name)
				currentCRName = crName
				totalNodes := clusterTotalNodes(clusterReplicasPerShard)
				const testDuration = 60 * time.Second

				AddReportEntry("cr:" + crName)
				By(fmt.Sprintf("creating a %d-shard cluster with %d replica(s) per shard",
					clusterShards, clusterReplicasPerShard))
				cr := clusterCR(crName, clusterReplicasPerShard, "", "")
				cmd := exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(cr)
				_, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())

				By("deploying chaos client pod simultaneously")
				chaosPodName, err := deployChaosClient(testNamespace, "replica-"+mode.Name, crName+":6379", "chaos-replica", true, testDuration)
				Expect(err).NotTo(HaveOccurred())
				AddReportEntry("chaos:" + chaosPodName)
				defer func() {
					if debugOnFailure && suiteOrSpecFailed() {
						return
					}
					deleteChaosClient(testNamespace, chaosPodName)
				}()

				By("waiting for cluster to be ready and ok")
				Eventually(func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "littlered", crName,
						"-n", testNamespace, "-o", "jsonpath={.status.phase}")
					output, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).To(Equal("Running"))

					cmd = exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
						"-n", testNamespace, "-c", "redis", "--",
						"redis-cli", "CLUSTER", "INFO")
					output, err = utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).To(ContainSubstring("cluster_state:ok"))
				}, 5*time.Minute, 5*time.Second).Should(Succeed())

				By("waiting 10 seconds for baseline traffic")
				time.Sleep(10 * time.Second)

				// Target shard 0's first replica
				victimPod := clusterReplicaPod(crName, 0, 1)
				victimNodeID, _ := getPodNodeID(testNamespace, victimPod)

				By(fmt.Sprintf("deleting replica pod %s (%s)", victimPod, mode.Name))
				_, err = deletePodMode(testNamespace, victimPod, mode.Graceful)
				Expect(err).NotTo(HaveOccurred())

				// Wait for cluster to detect failure via shard-0's master
				waitForClusterFailureDetected(testNamespace, crName, clusterMasterPod(crName, 0), totalNodes, []string{victimNodeID})

				err = waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
				Expect(err).NotTo(HaveOccurred())

				metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
				Expect(err).NotTo(HaveOccurred())

				Expect(metrics.DataCorruptions).To(Equal(int64(0)))
				Expect(metrics.WriteAvailability()).To(BeNumerically(">=", 0.99))
			})
		}

		It("should maintain data integrity during rolling restart", func() {
			const crName = "chaos-cluster-rolling"
			currentCRName = crName
			totalNodes := clusterTotalNodes(clusterReplicasPerShard)
			const testDuration = 90 * time.Second

			AddReportEntry("cr:" + crName)
			By(fmt.Sprintf("creating a %d-shard cluster with %d replica(s) per shard",
				clusterShards, clusterReplicasPerShard))
			cr := clusterCR(crName, clusterReplicasPerShard, "", "")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("deploying chaos client pod simultaneously")
			chaosPodName, err := deployChaosClient(testNamespace, "rolling", crName+":6379", "chaos-rolling", true, testDuration)
			Expect(err).NotTo(HaveOccurred())
			AddReportEntry("chaos:" + chaosPodName)
			defer func() {
				if debugOnFailure && suiteOrSpecFailed() {
					return
				}
				deleteChaosClient(testNamespace, chaosPodName)
			}()

			By("waiting for cluster to be ready and ok")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))

				cmd = exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "INFO")
				output, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("cluster_state:ok"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("waiting 10 seconds for baseline traffic")
			time.Sleep(10 * time.Second)

			By("triggering an operator-mediated rolling update (pod-template change via the CR)")
			// A manual `kubectl rollout restart` of the shard StatefulSets would roll all
			// shards in parallel and bypass the operator. Instead we change the CR's pod
			// template so the OPERATOR drives the rollout, which serializes it one shard at
			// a time (LR-021) — only one shard's master is ever down at a time, so
			// availability stays high.
			updated := clusterCR(crName, clusterReplicasPerShard, "", `  resources:
    requests:
      cpu: "100m"
      memory: "160Mi"
    limits:
      cpu: "100m"
      memory: "160Mi"
`)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(updated)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the rolling update to begin")
			time.Sleep(5 * time.Second)

			err = waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
			Expect(err).NotTo(HaveOccurred())

			metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
			Expect(err).NotTo(HaveOccurred())

			Expect(metrics.DataCorruptions).To(Equal(int64(0)))
			// The operator-serialized rolling update keeps only one shard's master down at
			// a time, so availability must stay high (LR-021). A parallel roll would take
			// all masters down in one wave and drop this well below 0.95.
			Expect(metrics.ReadAvailability()).To(BeNumerically(">=", 0.95))

			By("waiting for the serialized rolling update to fully complete")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("Running"))
			}, 8*time.Minute, 10*time.Second).Should(Succeed())

			By("verifying final cluster topology (no lost shards)")
			cmd = exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "CLUSTER", "INFO")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(ContainSubstring("cluster_state:ok"))
			Expect(output).To(ContainSubstring("cluster_slots_assigned:16384"))

			verifyClusterTopologySync(testNamespace, crName, totalNodes)
		})
	})

	Context("Continuous Multi-Pod Failure Resilience", Ordered, func() {
		const crName = "chaos-cluster-multipod"
		var chaosPodName string
		// 5 iterations usually complete in ~3-4 minutes.
		const testDuration = 5 * time.Minute

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

			By("deploying chaos client pod simultaneously")
			chaosPodName, err = deployChaosClient(testNamespace, "multipod", crName+":6379", "chaos-multi", true, testDuration)
			Expect(err).NotTo(HaveOccurred())
			AddReportEntry("chaos:" + chaosPodName)

			By("waiting for cluster to be ready and ok")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))

				cmd = exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "INFO")
				output, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("cluster_state:ok"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("waiting 10 seconds for baseline traffic")
			time.Sleep(10 * time.Second)
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				return
			}
			deleteChaosClient(testNamespace, chaosPodName)
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=2m")
			_, _ = utils.Run(cmd)
		})

		It("should survive multiple rounds of random pod deletions without data loss", func() {
			totalNodes := clusterTotalNodes(clusterReplicasPerShard)
			const iterations = 5
			for i := 1; i <= iterations; i++ {
				By(fmt.Sprintf("=== Chaos Round %d/%d ===", i, iterations))

				shardGroups, err := getShardGroups(testNamespace, crName, totalNodes)
				Expect(err).NotTo(HaveOccurred())

				victims := make([]string, 0)
				victimNodeIDs := make([]string, 0)
				for _, group := range shardGroups {
					if len(group) < 2 {
						continue
					}
					// 50% chance to kill a pod in this shard
					if rand.Intn(2) == 0 {
						victimIdx := rand.Intn(len(group))
						vPod := group[victimIdx]
						victims = append(victims, vPod)
						vID, _ := getPodNodeID(testNamespace, vPod)
						if vID != "" {
							victimNodeIDs = append(victimNodeIDs, vID)
						}
					}
				}

				if len(victims) == 0 {
					vPod := shardGroups[0][0]
					victims = append(victims, vPod)
					vID, _ := getPodNodeID(testNamespace, vPod)
					if vID != "" {
						victimNodeIDs = append(victimNodeIDs, vID)
					}
				}

				By(fmt.Sprintf("Deleting victims: %v", victims))
				for _, v := range victims {
					_, err = deletePod(testNamespace, v)
					Expect(err).NotTo(HaveOccurred())
				}

				// Wait for cluster to detect failure via any survivor
				survivor := ""
				victimMap := make(map[string]bool)
				for _, v := range victims {
					victimMap[v] = true
				}
				for _, p := range clusterPodNames(crName, clusterShards, clusterReplicasPerShard) {
					if !victimMap[p] {
						survivor = p
						break
					}
				}
				if survivor != "" {
					waitForClusterFailureDetected(testNamespace, crName, survivor, totalNodes, victimNodeIDs)
				}

				By("Waiting for cluster to recover")
				Eventually(func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "littlered", crName,
						"-n", testNamespace, "-o", "jsonpath={.status.phase}")
					output, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).To(Equal("Running"))

					var info string
					for _, podName := range clusterPodNames(crName, clusterShards, clusterReplicasPerShard) {
						cmd = exec.Command("kubectl", "exec", podName,
							"-n", testNamespace, "-c", "redis", "--",
							"redis-cli", "CLUSTER", "INFO")
						out, err := utils.Run(cmd)
						if err == nil {
							info = out
							break
						}
					}
					g.Expect(info).To(ContainSubstring("cluster_state:ok"))
					g.Expect(info).To(ContainSubstring("cluster_slots_assigned:16384"))
				}, 5*time.Minute, 5*time.Second).Should(Succeed())

				verifyClusterTopologySync(testNamespace, crName, totalNodes)

				By("Stabilizing for 5 seconds")
				time.Sleep(5 * time.Second)
			}

			err := waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
			Expect(err).NotTo(HaveOccurred())

			metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
			Expect(err).NotTo(HaveOccurred())

			GinkgoWriter.Printf("Final Multi-Pod Chaos Metrics:\n%s\n", metrics.String())

			Expect(metrics.DataCorruptions).To(Equal(int64(0)))
			Expect(metrics.WriteAvailability()).To(BeNumerically(">=", 0.80))
			Expect(metrics.ReadAvailability()).To(BeNumerically(">=", 0.80))
		})
	})
})
