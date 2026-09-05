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

// Kill-9 in-pod process crash tests.
//
// These tests exercise the "super-hard" restart scenario: the Redis process inside
// a running pod is killed with SIGKILL. The pod is NOT deleted — the container
// restarts in-place, preserving the pod IP.
//
// Sentinel mode (see ADR-001): the startup script detects "I am the known master
// with a stored run-id" and suppresses Redis start until Sentinel completes
// failover, then joins as a replica. Data is preserved on the surviving replica.
//
// Cluster mode (see ADR-001): the startup script deletes nodes.conf before
// starting redis-server, giving the restarted process a fresh node ID. The
// cluster treats it as a stranger, the replica is promoted, and the restarted
// node rejoins as a new replica. Data is preserved on the surviving replica.

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

var _ = Describe("Kill-9 In-Pod Process Crash", Ordered, func() {

	// -------------------------------------------------------------------------
	// Smoke test: verify killPodProcess actually works
	// -------------------------------------------------------------------------
	//
	// Uses standalone mode (simplest topology) to confirm that:
	//   1. kill -9 restarts the container without replacing the pod (IP preserved).
	//   2. The run-id changes, proving a new Redis process is running.
	//   3. All in-memory data is wiped — the store returned empty after restart.
	//
	// This test is intentionally trivial. Its only job is to validate the
	// killPodProcess helper before the sentinel/cluster tests rely on it.

	Context("Standalone Mode — Kill-9 Smoke Test", Label("standalone"), Ordered, func() {
		It("should kill the Redis process and confirm the kill took effect", func() {
			const crName = "kill9-smoke"
			AddReportEntry("cr:" + crName)

			By("creating a standalone LittleRed")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: standalone
`, crName, testNamespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			defer func() {
				if debugOnFailure && suiteOrSpecFailed() {
					return
				}
				cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
				_, _ = utils.Run(cmd)
			}()

			By("waiting for standalone to be running")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			const pod = crName + "-redis-0"

			By("writing a key before kill-9")
			cmd = exec.Command("kubectl", "exec", pod,
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "SET", "kill9-smoke-key", "smoke-value")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("OK"))

			By("confirming the key is present before kill-9")
			cmd = exec.Command("kubectl", "exec", pod,
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "GET", "kill9-smoke-key")
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("smoke-value"))

			By("recording run-id and pod UID before kill-9")
			oldRunID, err := getPodRunID(testNamespace, pod)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "get", "pod", pod,
				"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
			podUID, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			podUID = strings.TrimSpace(podUID)

			By("kill -9 the Redis process")
			killPodProcess(testNamespace, pod)

			By("verifying the pod UID is unchanged (only the container restarted, not the pod)")
			Consistently(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", pod,
					"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
				uid, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(uid)).To(Equal(podUID))
			}, 15*time.Second, 3*time.Second).Should(Succeed())

			By("verifying the redis container restarted (restart count > 0)")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", pod,
					"-n", testNamespace,
					"-o", `jsonpath={.status.containerStatuses[?(@.name=="redis")].restartCount}`)
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				var count int
				fmt.Sscan(strings.TrimSpace(out), &count)
				g.Expect(count).To(BeNumerically(">", 0))
			}, 30*time.Second, 2*time.Second).Should(Succeed())

			By("verifying run-id changed (a fresh Redis process is running)")
			Eventually(func(g Gomega) {
				newRunID, err := getPodRunID(testNamespace, pod)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(newRunID).NotTo(Equal(oldRunID))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the key is gone (in-memory store wiped on process restart)")
			// Use Eventually: Redis may still be starting up right after the container restart.
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", pod,
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "GET", "kill9-smoke-key")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				// redis-cli returns empty string (not "(nil)") for missing keys when
				// not attached to a TTY (kubectl exec without -t).
				g.Expect(strings.TrimSpace(output)).To(BeEmpty())
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

	// -------------------------------------------------------------------------
	// Sentinel mode
	// -------------------------------------------------------------------------

	Context("Sentinel Mode — Master Process Crash", Label("sentinel"), Ordered, func() {
		It("should recover from master kill-9 without data loss", func() {
			const crName = "kill9-sentinel"
			AddReportEntry("cr:" + crName)
			const testDuration = 120 * time.Second

			By("creating Sentinel cluster and chaos client simultaneously")
			cr := e2eAuthPreamble(crName) + fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
%s  sentinel:
    masterName: %s
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace, e2eAuthSpecYAML(crName), e2eMasterName(testNamespace, crName))
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			chaosPodName, err := deployChaosClient(testNamespace, "kill9-sentinel", crName+":6379", "kill9-sent", false, testDuration)
			Expect(err).NotTo(HaveOccurred())
			AddReportEntry("chaos:" + chaosPodName)

			defer func() {
				if debugOnFailure && suiteOrSpecFailed() {
					return
				}
				cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
				_, _ = utils.Run(cmd)
			}()
			defer func() {
				if debugOnFailure && suiteOrSpecFailed() {
					return
				}
				deleteChaosClient(testNamespace, chaosPodName)
			}()

			By("waiting for sentinel cluster to be running")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))

				cmd = exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
				master, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(master).NotTo(BeEmpty())

				cmd = exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.bootstrapRequired}")
				bootstrap, _ := utils.Run(cmd)
				g.Expect(bootstrap).To(Or(Equal("false"), Equal("")))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			verifySentinelTopologySync(testNamespace, crName, 3, 2)

			By("waiting 10 seconds for baseline traffic")
			time.Sleep(10 * time.Second)

			By("identifying the current master pod")
			cmd = exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
			masterPod, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			masterPod = strings.TrimSpace(masterPod)

			oldRunID, err := getPodRunID(testNamespace, masterPod)
			Expect(err).NotTo(HaveOccurred())

			By("recording master pod UID — kill-9 must NOT replace the pod")
			cmd = exec.Command("kubectl", "get", "pod", masterPod,
				"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
			podUID, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			podUID = strings.TrimSpace(podUID)

			By(fmt.Sprintf("kill -9 on master pod %s (in-pod process crash, IP stays the same)", masterPod))
			killPodProcess(testNamespace, masterPod)

			By("verifying pod UID is unchanged — the pod was not replaced, only the container")
			Consistently(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", masterPod,
					"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
				uid, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(uid)).To(Equal(podUID), "Pod UID changed — this is a pod replacement, not an in-pod crash")
			}, 15*time.Second, 3*time.Second).Should(Succeed())

			By("waiting for the redis container to restart (restart count > 0)")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", masterPod,
					"-n", testNamespace,
					"-o", `jsonpath={.status.containerStatuses[?(@.name=="redis")].restartCount}`)
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				var count int
				fmt.Sscan(strings.TrimSpace(out), &count)
				g.Expect(count).To(BeNumerically(">", 0), "Container restart count should be > 0 after kill-9")
			}, 30*time.Second, 2*time.Second).Should(Succeed())

			// At this point the startup script is running inside the restarted container.
			// It detected the non-empty run-id stored by Sentinel and set YIELD_MASTER=true.
			// It is now sleeping (not starting Redis), waiting for Sentinel to detect SDOWN
			// (after downAfterMilliseconds=5s) and complete failover.

			By("waiting for Sentinel to elect a new master (failover from the yielding pod)")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
				newMaster, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				newMaster = strings.TrimSpace(newMaster)
				g.Expect(newMaster).NotTo(BeEmpty())
				g.Expect(newMaster).NotTo(Equal(masterPod), "Original master pod must have lost the master role")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the original master pod rejoined as a replica")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", masterPod,
					"-n", testNamespace,
					"-o", "jsonpath={.metadata.labels['redis\\.chuck-chuck-chuck\\.net/role']}")
				role, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(role)).To(Equal("replica"),
					"Original master pod should have rejoined as a replica after yielding master role")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the run-id changed (a new Redis process is running)")
			Eventually(func(g Gomega) {
				newRunID, err := getPodRunID(testNamespace, masterPod)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(newRunID).NotTo(Equal(oldRunID), "Run-id must change: the process was replaced")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying full Sentinel topology is consistent")
			verifySentinelTopologySync(testNamespace, crName, 3, 2)

			err = waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
			Expect(err).NotTo(HaveOccurred())

			metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
			Expect(err).NotTo(HaveOccurred())
			GinkgoWriter.Printf("Sentinel Kill-9 Metrics:\n%s\n", metrics.String())

			// Downtime window: SDOWN detection (~5 s) + failover (~5–10 s) + replica startup.
			// Over the 120 s chaos window, write availability should remain above 40%.
			Expect(metrics.DataCorruptions).To(Equal(int64(0)), "Data corruption detected!")
			Expect(metrics.WriteAvailability()).To(BeNumerically(">", 0.40))

			// DURABILITY. `VerifyAll` has always computed FinalMissing for this tier and
			// this tier has always PRINTED IT AND DISCARDED IT — which is exactly the
			// shape LR-038 started from: a number computed, reported, never asserted,
			// while 202 of 1171 acknowledged writes vanished under assertions that all
			// passed. DataCorruptions cannot catch it, because the keys are GONE rather
			// than wrong, and WriteAvailability cannot either, because the writes were
			// ACKed.
			//
			// The bound is the one the failover tier derives, and it is a design claim
			// rather than a fitted number: a kill-9'd master loses only what was ACKed
			// inside the replication lag of the kill — ~1 per failover at 10 writes/s,
			// so ~2 for this spec. 5 is generous against that and tight against any real
			// regression, which loses tens to hundreds.
			//
			// Evidence it is achievable HERE, rather than borrowed from the other mode:
			// LR-038's six-cell matrix ran this disruption shape 10 consecutive times
			// and measured 0 MISSING in every block (sentinel kill-9, 43-74% write
			// availability — the availability is poor and the durability is perfect,
			// which is precisely why the availability bar cannot stand in for this one).
			Expect(metrics.FinalChecked).To(BeNumerically(">", 500),
				"final verification sweep did not check a meaningful number of keys, so a "+
					"FinalMissing of 0 would be vacuous")
			Expect(metrics.FinalUnreadable).To(BeNumerically("<", metrics.FinalChecked/10),
				"too many keys unreadable at sweep time to trust the durability verdict")
			Expect(metrics.FinalMissing).To(BeNumerically("<=", 5),
				"acknowledged writes were LOST: %d of %d. The kill-9 guard exists to make "+
					"this 0 — see LR-038's start-gate, where a kill-9 of a PROMOTED master "+
					"destroyed 352 of 1145.", metrics.FinalMissing, metrics.FinalChecked)
		})
	})

	// -------------------------------------------------------------------------
	// Cluster mode
	// -------------------------------------------------------------------------

	Context("Cluster Mode — Master Process Crash", Label("cluster"), Ordered, func() {
		It("should recover from master shard kill-9 without data loss", func() {
			const crName = "kill9-cluster"
			AddReportEntry("cr:" + crName)
			const testDuration = 120 * time.Second

			By(fmt.Sprintf("creating a %d-shard cluster with %d replica(s) per shard",
				clusterShards, clusterReplicasPerShard))
			cr := clusterCR(crName, clusterReplicasPerShard,
				"    clusterNodeTimeout: 5000\n", "")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			chaosPodName, err := deployChaosClient(testNamespace, "kill9-cluster", crName+":6379", "kill9-clust", true, testDuration)
			Expect(err).NotTo(HaveOccurred())
			AddReportEntry("chaos:" + chaosPodName)

			defer func() {
				if debugOnFailure && suiteOrSpecFailed() {
					return
				}
				cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=2m")
				_, _ = utils.Run(cmd)
			}()
			defer func() {
				if debugOnFailure && suiteOrSpecFailed() {
					return
				}
				deleteChaosClient(testNamespace, chaosPodName)
			}()

			By("waiting for cluster to be running")
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

			// shard-0's master (pod {crName}-shard-0-0, assigned slots 0–5460).
			victimPod := clusterMasterPod(crName, 0)

			By(fmt.Sprintf("recording node ID and pod UID of %s before kill-9", victimPod))
			oldNodeID, err := getPodNodeID(testNamespace, victimPod)
			Expect(err).NotTo(HaveOccurred())

			cmd = exec.Command("kubectl", "get", "pod", victimPod,
				"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
			podUID, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			podUID = strings.TrimSpace(podUID)

			By(fmt.Sprintf("kill -9 on master pod %s (in-pod process crash, IP stays the same)", victimPod))
			killPodProcess(testNamespace, victimPod)

			By("verifying pod UID is unchanged — the pod was not replaced, only the container")
			Consistently(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", victimPod,
					"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
				uid, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(uid)).To(Equal(podUID), "Pod UID changed — this is a pod replacement, not an in-pod crash")
			}, 15*time.Second, 3*time.Second).Should(Succeed())

			By("waiting for the redis container to restart (restart count > 0)")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", victimPod,
					"-n", testNamespace,
					"-o", `jsonpath={.status.containerStatuses[?(@.name=="redis")].restartCount}`)
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				var count int
				fmt.Sscan(strings.TrimSpace(out), &count)
				g.Expect(count).To(BeNumerically(">", 0), "Container restart count should be > 0 after kill-9")
			}, 30*time.Second, 2*time.Second).Should(Succeed())

			// At this point the startup script deleted nodes.conf and started redis-server
			// with a fresh node ID. The cluster treats the restarted node as a stranger.
			// After clusterNodeTimeout (5 s) the replica is promoted to master.

			By("waiting for shard-0 failover: slot 0 must transfer to the former replica")
			// Query via shard-1's master (a surviving node in a different shard).
			waitForShardMasterChange(testNamespace, clusterMasterPod(crName, 1), 0, oldNodeID)

			By("verifying the restarted pod has a new node ID (nodes.conf was deleted)")
			Eventually(func(g Gomega) {
				newNodeID, err := getPodNodeID(testNamespace, victimPod)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(newNodeID).NotTo(Equal(oldNodeID),
					"Node ID must change after kill-9: startup script deleted nodes.conf")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("waiting for cluster to be fully healthy")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))

				// Query via shard-1's master (shard-0's master may still be joining).
				cmd = exec.Command("kubectl", "exec", clusterMasterPod(crName, 1),
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CLUSTER", "INFO")
				output, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("cluster_state:ok"))
				g.Expect(output).To(ContainSubstring("cluster_slots_assigned:16384"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			verifyClusterTopologySync(testNamespace, crName, clusterTotalNodes(clusterReplicasPerShard))

			err = waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
			Expect(err).NotTo(HaveOccurred())

			metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
			Expect(err).NotTo(HaveOccurred())
			GinkgoWriter.Printf("Cluster Kill-9 Metrics:\n%s\n", metrics.String())

			// Slot-0 shard is unavailable for up to clusterNodeTimeout (5 s) + election.
			// Over the 120 s chaos window, write availability should remain >= 80%.
			Expect(metrics.DataCorruptions).To(Equal(int64(0)), "Data corruption detected!")
			Expect(metrics.WriteAvailability()).To(BeNumerically(">=", 0.80))
		})
	})
})
