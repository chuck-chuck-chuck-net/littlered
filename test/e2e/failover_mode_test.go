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

// Failover-mode e2e suite (ADR-011, M6).
//
// mode:failover is operator-managed HA without Sentinel: the operator is the
// sole failure detector and failover decider (assignment annotations + downward
// API + epoch fencing). This suite is the failover-mode analog of the sentinel
// functional/failover/kill-9/chaos/deadlock tiers — the graduation-gate
// scenarios from docs/FAILOVER_MODE_DESIGN.md §4, including the hybrid
// double-failover that spawned LR-007/LR-008/LR-024 in sentinel mode.
//
// Ground truth is always `INFO replication` on the data pods
// (verifyFailoverTopologySync) — there are no Sentinels to ask.

package e2e

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive // dot-import is the Ginkgo/Gomega convention in tests
	. "github.com/onsi/gomega"    //nolint:revive

	"github.com/littlered-operator/littlered-operator/test/utils"
)

var _ = Describe("Failover Mode", Label("failover-mode"), func() {

	// -------------------------------------------------------------------------
	// FUNCTIONAL: resources, status, assignment channel, replication.
	// -------------------------------------------------------------------------
	Context("Functional", Ordered, func() {
		const crName = "failover-func"

		BeforeAll(func() {
			By("Test ID: FO-001 - applying the LittleRed CR with failover mode")
			deployFailover(crName, 2, 3000, nil)
		})

		AfterAll(func() { cleanupFailoverCR(crName) })

		It("should create the data StatefulSet and label-routed Services, and NO sentinel resources", func() {
			By("Test ID: FO-002 - checking the data StatefulSet")
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "statefulset", crName+"-redis",
					"-n", testNamespace, "-o", "jsonpath={.status.readyReplicas}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("3"), "1 master + 2 replicas expected")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("checking the master service")
			_, err := utils.Run(exec.Command("kubectl", "get", "service", crName, "-n", testNamespace))
			Expect(err).NotTo(HaveOccurred())

			By("checking the replicas service")
			_, err = utils.Run(exec.Command("kubectl", "get", "service", crName+"-replicas", "-n", testNamespace))
			Expect(err).NotTo(HaveOccurred())

			By("checking that NO sentinel Service exists")
			_, err = utils.Run(exec.Command("kubectl", "get", "service", crName+"-sentinel", "-n", testNamespace))
			Expect(err).To(HaveOccurred(), "failover mode must not create a sentinel Service")

			By("checking that NO sentinel StatefulSet exists")
			_, err = utils.Run(exec.Command("kubectl", "get", "statefulset", crName+"-sentinel", "-n", testNamespace))
			Expect(err).To(HaveOccurred(), "failover mode must not create a sentinel StatefulSet")
		})

		It("should report master info in status and clear the bootstrap flag", func() {
			By("Test ID: FO-003")
			Eventually(func(g Gomega) {
				g.Expect(getMasterPod(crName)).NotTo(BeEmpty())

				out, _ := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.bootstrapRequired}"))
				g.Expect(out).To(Or(Equal("false"), Equal("")), "bootstrapRequired must be cleared once Running")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should stamp assignment annotations: exactly one master, all epochs >= 1", func() {
			By("Test ID: FO-004 - reading the operator-stamped assignment channel (ADR-011 §3)")
			Eventually(func(g Gomega) {
				pods := failoverDataPods(crName)
				g.Expect(pods).To(HaveLen(3))

				masterIP, _ := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.master.ip}"))
				masterIP = strings.TrimSpace(masterIP)
				g.Expect(masterIP).NotTo(BeEmpty())

				masterCount := 0
				for _, pod := range pods {
					role := getPodAnnotation(testNamespace, pod, failoverAnnRole)
					epochStr := getPodAnnotation(testNamespace, pod, failoverAnnEpoch)
					g.Expect(role).To(Or(Equal("master"), Equal("replica")),
						fmt.Sprintf("pod %s has no valid assigned-role annotation", pod))
					epoch, err := strconv.ParseInt(epochStr, 10, 64)
					g.Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("pod %s has no parsable assignment-epoch", pod))
					g.Expect(epoch).To(BeNumerically(">=", 1))

					if role == "master" {
						masterCount++
						g.Expect(getPodAnnotation(testNamespace, pod, failoverAnnMasterIP)).To(BeEmpty(),
							"the master assignment must carry no master IP")
					} else {
						g.Expect(getPodAnnotation(testNamespace, pod, failoverAnnMasterIP)).To(Equal(masterIP),
							fmt.Sprintf("replica %s must be assigned to the current master IP", pod))
					}
				}
				g.Expect(masterCount).To(Equal(1), "exactly one pod must carry assigned-role=master")

				By("checking the status mirror of the assignment epoch")
				epochOut, _ := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.failover.assignmentEpoch}"))
				statusEpoch, err := strconv.ParseInt(strings.TrimSpace(epochOut), 10, 64)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(statusEpoch).To(BeNumerically(">=", 1))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should emit the experimental-mode warning event on the CR", func() {
			By("Test ID: FO-005 (ADR-011 §8)")
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "events", "-n", testNamespace,
					"--field-selector", "involvedObject.name="+crName+",reason=ExperimentalMode",
					"-o", "jsonpath={.items[*].message}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("experimental"),
					"the one-time ExperimentalMode warning event must be present")
			}, 1*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should have a consistent topology: roles, links, labels, and status", func() {
			By("Test ID: FO-006")
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})

		It("should replicate writes from master to replicas", func() {
			By("Test ID: FO-010 - writing to the master")
			masterPod := getMasterPod(crName)
			Expect(masterPod).NotTo(BeEmpty())

			out, err := redisExec(testNamespace, masterPod, "SET", "fo-repl-key", "fo-repl-value")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("OK"))

			By("reading from a replica")
			replicas := otherRedisPods(crName, masterPod)
			Expect(replicas).NotTo(BeEmpty())
			Eventually(func(g Gomega) {
				out, err := redisExec(testNamespace, replicas[0], "GET", "fo-repl-key")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("fo-repl-value"))
			}, 30*time.Second, 2*time.Second).Should(Succeed())
		})
	})

	// -------------------------------------------------------------------------
	// FAILOVER: graceful + crash master deletion (the SEN-011 analog, asserted
	// by pod-instance UID per the a26efa4 convention).
	// -------------------------------------------------------------------------
	Context("Failover", Ordered, func() {
		const crName = "failover-ha"

		BeforeAll(func() {
			By("Test ID: FO-011 - deploying the failover instance with fast detection")
			deployFailover(crName, 2, 3000, nil)
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})

		AfterAll(func() { cleanupFailoverCR(crName) })

		for _, mode := range restartModes {
			mode := mode // capture range variable
			It(fmt.Sprintf("should elect a new master after master pod deletion (%s)", mode.Name), func() {
				By("Test ID: FO-012 - getting the current master pod")
				originalMaster := getMasterPod(crName)
				Expect(originalMaster).NotTo(BeEmpty())
				_, _ = fmt.Fprintf(GinkgoWriter, "Original master: %s\n", originalMaster)

				// Assert on the pod INSTANCE (UID), not the name: a StatefulSet
				// reuses the name, so only the UID proves a real failover/recovery.
				originalMasterUID := podUID(testNamespace, originalMaster)
				Expect(originalMasterUID).NotTo(BeEmpty())
				originalRunID, err := getPodRunID(testNamespace, originalMaster)
				Expect(err).NotTo(HaveOccurred())

				key := "fo-failover-key-" + mode.Name
				By("writing test data to the master before failover")
				out, err := redisExec(testNamespace, originalMaster, "SET", key, "fo-failover-value")
				Expect(err).NotTo(HaveOccurred())
				Expect(strings.TrimSpace(out)).To(Equal("OK"))

				By("waiting for at least one replica to hold the data")
				replicas := otherRedisPods(crName, originalMaster)
				Expect(replicas).NotTo(BeEmpty())
				Eventually(func(g Gomega) {
					out, _ := redisExec(testNamespace, replicas[0], "GET", key)
					g.Expect(strings.TrimSpace(out)).To(Equal("fo-failover-value"))
				}, 30*time.Second, 2*time.Second).Should(Succeed())

				By(fmt.Sprintf("deleting the master pod to trigger failover (%s mode)", mode.Name))
				_, err = deletePodMode(testNamespace, originalMaster, mode.Graceful)
				Expect(err).NotTo(HaveOccurred())

				By("waiting for a NEW master pod instance (UID + RunID must change)")
				var newMaster string
				Eventually(func(g Gomega) {
					newMaster = getMasterPod(crName)
					g.Expect(newMaster).NotTo(BeEmpty())
					// Label-agreement guard: right after a (force) delete, status can
					// briefly still name the dead master while its recreated fresh-UID
					// pod already exists — require the label to agree so the UID check
					// below proves a real completed failover, not the stale window.
					g.Expect(getPodRoleLabel(testNamespace, newMaster)).To(Equal("master"),
						"status.master must agree with the role label")
					newMasterUID := podUID(testNamespace, newMaster)
					g.Expect(newMasterUID).NotTo(BeEmpty())
					g.Expect(newMasterUID).NotTo(Equal(originalMasterUID),
						"master must be a new pod instance (a real failover occurred), not the same instance")
					newRunID, err := getPodRunID(testNamespace, newMaster)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(newRunID).NotTo(Equal(originalRunID),
						"the new master must be a different Redis process (RunID changed)")
				}, 90*time.Second, 3*time.Second).Should(Succeed())
				_, _ = fmt.Fprintf(GinkgoWriter, "New master: %s\n", newMaster)

				By("Test ID: FO-013 - verifying data is preserved after failover")
				Eventually(func(g Gomega) {
					out, err := redisExec(testNamespace, getMasterPod(crName), "GET", key)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(strings.TrimSpace(out)).To(Equal("fo-failover-value"), "data was lost during failover")
				}, 60*time.Second, 3*time.Second).Should(Succeed())

				By("verifying the full topology re-converges with both replicas re-joined")
				verifyFailoverTopologySync(testNamespace, crName, 2)
			})
		}
	})

	// -------------------------------------------------------------------------
	// EVENT-PATH latency: with default annotations the crash of the master must
	// move the master label to a different pod instance in < 15s (the ADR-011
	// §8 watcher-path bar; K8s pod events + the fast watcher both accelerate).
	// -------------------------------------------------------------------------
	Context("Event-Path Detection", Ordered, func() {
		var crName string

		BeforeAll(func() {
			crName = fmt.Sprintf("failover-event-%d", time.Now().Unix())
			By("Test ID: FO-020 - deploying with default annotations (event fast path active)")
			deployFailover(crName, 2, 3000, nil)
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})

		AfterAll(func() { cleanupFailoverCR(crName) })

		It("should move the master label to a new pod instance in under 15 seconds (crash)", func() {
			By("Step 1: identify the initial master instance")
			initialMaster := getMasterPod(crName)
			Expect(initialMaster).NotTo(BeEmpty())
			initialUID := podUID(testNamespace, initialMaster)
			Expect(initialUID).NotTo(BeEmpty())

			By(fmt.Sprintf("Step 2: crash-delete the master %s", initialMaster))
			_, err := deletePodMode(testNamespace, initialMaster, false)
			Expect(err).NotTo(HaveOccurred())

			By("Step 3: wait for the master label to land on a different pod instance")
			start := time.Now()
			Eventually(func(g Gomega) {
				out, _ := utils.Run(exec.Command("kubectl", "get", "pods", "-n", testNamespace,
					"-l", "redis.chuck-chuck-chuck.net/role=master,app.kubernetes.io/instance="+crName,
					"-o", "jsonpath={.items[*].metadata.name}"))
				labeled := strings.Fields(strings.TrimSpace(out))
				g.Expect(labeled).To(HaveLen(1), "exactly one pod must carry the master label")
				uid := podUID(testNamespace, labeled[0])
				g.Expect(uid).NotTo(BeEmpty())
				g.Expect(uid).NotTo(Equal(initialUID),
					"the master label must move to a DIFFERENT pod instance")
			}, 45*time.Second, 1*time.Second).Should(Succeed(), "operator failed to move the master label")

			duration := time.Since(start)
			_, _ = fmt.Fprintf(GinkgoWriter, "Event-path failover took: %v\n", duration)
			Expect(duration).To(BeNumerically("<", 15*time.Second),
				"event-path failover was too slow (fast path not effective)")

			verifyFailoverTopologySync(testNamespace, crName, 2)
		})
	})

	// -------------------------------------------------------------------------
	// POLLING-ONLY: with the watcher disabled via annotation, recovery must
	// still complete on reconcile cadence, and the operator must log that the
	// watcher is off.
	// -------------------------------------------------------------------------
	Context("Polling-Only Recovery", Ordered, func() {
		var crName string

		BeforeAll(func() {
			crName = fmt.Sprintf("failover-polling-%d", time.Now().Unix())
			By("Test ID: FO-021 - deploying with the event watcher disabled")
			deployFailover(crName, 2, 3000, map[string]string{
				"redis.chuck-chuck-chuck.net/disable-event-monitoring": "true",
			})
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})

		AfterAll(func() { cleanupFailoverCR(crName) })

		It("should recover within 60 seconds via reconcile polling with the watcher disabled", func() {
			startTime := time.Now().Add(-5 * time.Second)

			By("Step 1: identify the initial master instance")
			initialMaster := getMasterPod(crName)
			Expect(initialMaster).NotTo(BeEmpty())
			initialUID := podUID(testNamespace, initialMaster)

			By(fmt.Sprintf("Step 2: crash-delete the master %s", initialMaster))
			_, err := deletePodMode(testNamespace, initialMaster, false)
			Expect(err).NotTo(HaveOccurred())

			By("Step 3: wait for the master label to move (polling cadence)")
			Eventually(func(g Gomega) {
				out, _ := utils.Run(exec.Command("kubectl", "get", "pods", "-n", testNamespace,
					"-l", "redis.chuck-chuck-chuck.net/role=master,app.kubernetes.io/instance="+crName,
					"-o", "jsonpath={.items[*].metadata.name}"))
				labeled := strings.Fields(strings.TrimSpace(out))
				g.Expect(labeled).To(HaveLen(1))
				uid := podUID(testNamespace, labeled[0])
				g.Expect(uid).NotTo(BeEmpty())
				g.Expect(uid).NotTo(Equal(initialUID))
			}, 60*time.Second, 2*time.Second).Should(Succeed(), "operator failed to recover on polling cadence")

			By("Step 4: verify operator logs show the watcher was disabled")
			Eventually(func(g Gomega) {
				since := startTime.Format(time.RFC3339Nano)
				cmd := exec.Command("sh", "-c",
					fmt.Sprintf("kubectl logs -n %s -l control-plane=controller-manager --tail=-1 --since-time=%s | grep %s",
						operatorNamespace, since, crName))
				logs, _ := utils.Run(cmd)
				g.Expect(logs).To(ContainSubstring("Failover event monitoring disabled via annotation"))
				g.Expect(logs).NotTo(ContainSubstring("Starting failover master watcher"))
			}, 30*time.Second, 2*time.Second).Should(Succeed())

			verifyFailoverTopologySync(testNamespace, crName, 2)
		})
	})

	// -------------------------------------------------------------------------
	// HYBRID double-failover — THE graduation scenario (design note §4): a
	// graceful failover immediately followed by a crash of the new master on
	// the same instance. This exact sequence spawned LR-007/LR-008/LR-024 in
	// sentinel mode; failover mode must clear it without deadlocking.
	// -------------------------------------------------------------------------
	Context("Hybrid Double-Failover", Ordered, func() {
		var crName string

		BeforeAll(func() {
			crName = fmt.Sprintf("failover-hybrid-%d", time.Now().Unix())
			By("Test ID: FO-030 - deploying the graduation-scenario instance")
			deployFailover(crName, 2, 3000, nil)
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})

		AfterAll(func() { cleanupFailoverCR(crName) })

		It("should survive a graceful failover immediately followed by a crash of the new master", func() {
			By("Step 1: identify master #1 and replicate test data everywhere")
			master1 := getMasterPod(crName)
			Expect(master1).NotTo(BeEmpty())
			uid1 := podUID(testNamespace, master1)
			Expect(uid1).NotTo(BeEmpty())

			out, err := redisExec(testNamespace, master1, "SET", "fo-hybrid-key", "fo-hybrid-value")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("OK"))
			for _, r := range otherRedisPods(crName, master1) {
				Eventually(func(g Gomega) {
					out, _ := redisExec(testNamespace, r, "GET", "fo-hybrid-key")
					g.Expect(strings.TrimSpace(out)).To(Equal("fo-hybrid-value"))
				}, 30*time.Second, 2*time.Second).Should(Succeed(), "replica %s never received the data", r)
			}

			By(fmt.Sprintf("Step 2: GRACEFUL delete of master #1 (%s)", master1))
			_, err = deletePodMode(testNamespace, master1, true)
			Expect(err).NotTo(HaveOccurred())

			By("Step 3: wait for master #2 (a new pod instance)")
			var master2, uid2 string
			Eventually(func(g Gomega) {
				master2 = getMasterPod(crName)
				g.Expect(master2).NotTo(BeEmpty())
				// Guard against a stale status window: the pod named in status
				// must actually carry the master label before we crash it.
				g.Expect(getPodRoleLabel(testNamespace, master2)).To(Equal("master"),
					"status.master must agree with the role label before the second failover")
				uid2 = podUID(testNamespace, master2)
				g.Expect(uid2).NotTo(BeEmpty())
				g.Expect(uid2).NotTo(Equal(uid1), "master #2 must be a new pod instance")
			}, 90*time.Second, 2*time.Second).Should(Succeed())
			_, _ = fmt.Fprintf(GinkgoWriter, "Master #2: %s\n", master2)

			By(fmt.Sprintf("Step 4: IMMEDIATE crash (force delete) of master #2 (%s)", master2))
			_, err = deletePodMode(testNamespace, master2, false)
			Expect(err).NotTo(HaveOccurred())

			By("Step 5: wait for master #3 — a third distinct pod instance")
			var master3 string
			Eventually(func(g Gomega) {
				master3 = getMasterPod(crName)
				g.Expect(master3).NotTo(BeEmpty())
				// Label-agreement guard (as for master #2): a force-delete leaves
				// a ~1-2s stale-status window in which status still names the
				// crashed master while its recreated (fresh-UID) pod already
				// exists — without this guard the UID checks below can pass
				// spuriously before the third master even exists.
				g.Expect(getPodRoleLabel(testNamespace, master3)).To(Equal("master"),
					"status.master must agree with the role label")
				uid3 := podUID(testNamespace, master3)
				g.Expect(uid3).NotTo(BeEmpty())
				g.Expect(uid3).NotTo(Equal(uid2), "master #3 must not be the crashed master #2 instance")
				g.Expect(uid3).NotTo(Equal(uid1), "master #3 must not be the long-gone master #1 instance")
			}, 90*time.Second, 3*time.Second).Should(Succeed(),
				"no third master emerged — the double-failover deadlock class (LR-007/LR-024) is back")
			_, _ = fmt.Fprintf(GinkgoWriter, "Master #3: %s\n", master3)

			By("Step 6: data must have survived both failovers")
			Eventually(func(g Gomega) {
				out, err := redisExec(testNamespace, getMasterPod(crName), "GET", "fo-hybrid-key")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("fo-hybrid-value"), "data was lost across the double failover")
			}, 60*time.Second, 3*time.Second).Should(Succeed())

			By("Step 7: full topology re-convergence")
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})
	})

	// -------------------------------------------------------------------------
	// KILL-9 in-place: the ADR-001 same-IP hazard, re-owned by the epoch gate
	// (ADR-011 §3). The pod is NOT replaced. Asserted is the INVARIANT the ADR
	// guarantees: the ex-master never re-enters service as master on its stale
	// authorization; the operator promotes a replica; the ex-master rejoins as
	// a replica at a strictly greater epoch. Whether the restarted container
	// visibly PARKS ("already consumed" log) is a kubelet-timing race with two
	// equally correct outcomes — recorded as an observation, never asserted.
	// -------------------------------------------------------------------------
	Context("Kill-9 In-Place Master Crash", Ordered, func() {
		It("should yield mastership via the epoch gate and recover without data loss", func() {
			const crName = "kill9-failover"
			AddReportEntry("cr:" + crName)
			const testDuration = 120 * time.Second

			By("Test ID: FO-040 - creating the failover instance and chaos client simultaneously")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(failoverCR(crName, 2, 3000, nil, ""))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			chaosPodName, err := deployChaosClient(testNamespace, "kill9-failover", crName+":6379", "kill9-fo", false, testDuration)
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

			By("waiting for the failover instance to be running")
			Eventually(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
				g.Expect(getMasterPod(crName)).NotTo(BeEmpty())
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			verifyFailoverTopologySync(testNamespace, crName, 2)

			By("waiting 10 seconds for baseline traffic")
			time.Sleep(10 * time.Second)

			By("identifying the current master pod")
			masterPod := getMasterPod(crName)
			Expect(masterPod).NotTo(BeEmpty())
			oldRunID, err := getPodRunID(testNamespace, masterPod)
			Expect(err).NotTo(HaveOccurred())
			masterUID := podUID(testNamespace, masterPod)
			Expect(masterUID).NotTo(BeEmpty())

			By("capturing the pre-kill assignment epoch (the stale authorization)")
			preKillEpoch, err := strconv.ParseInt(
				getPodAnnotation(testNamespace, masterPod, failoverAnnEpoch), 10, 64)
			Expect(err).NotTo(HaveOccurred(),
				"master pod must carry a parsable assignment-epoch before the kill")

			By(fmt.Sprintf("kill -9 on master pod %s (in-pod process crash, pod and IP stay)", masterPod))
			killPodProcess(testNamespace, masterPod)

			By("waiting for the redis container restart to become visible (invariant-window start)")
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "pod", masterPod,
					"-n", testNamespace,
					"-o", `jsonpath={.status.containerStatuses[?(@.name=="redis")].restartCount}`))
				g.Expect(err).NotTo(HaveOccurred())
				var count int
				_, _ = fmt.Sscan(strings.TrimSpace(out), &count)
				g.Expect(count).To(BeNumerically(">", 0))
			}, 30*time.Second, 1*time.Second).Should(Succeed())

			// THE INVARIANT (ADR-011 §3): after the in-place restart, the
			// ex-master must never again SERVE as master on its stale
			// authorization — i.e. never simultaneously (a) carry the
			// role=master label (traffic routing) and (b) report role:master
			// via INFO (actually serving). HOW it gets there is a
			// kubelet-timing race with two equally correct outcomes: either
			// its first annotation read precedes the downward-API sync of the
			// operator's bumped epoch and it PARKS ("already consumed" log),
			// or the fresh epoch is already visible and it starts directly as
			// a replica (no park log). So the mechanism is only observed
			// (parkLogSeen, reported below), never asserted.
			//
			// The window starts strictly AFTER the restart is visible: before
			// it, label==master && role:master was the legitimate pre-kill
			// state. The pod-UID check rides along for the whole window — an
			// unchanged UID proves this stayed an in-pod crash, not a pod
			// replacement.
			parkLogSeen := false
			By("verifying the ex-master never re-enters service as master on the stale epoch")
			Consistently(func(g Gomega) {
				g.Expect(podUID(testNamespace, masterPod)).To(Equal(masterUID),
					"pod UID changed — this is a pod replacement, not an in-pod crash")

				label := getPodRoleLabel(testNamespace, masterPod)
				v, infoErr := getReplicationView(testNamespace, masterPod)
				// While parked, INFO is unreachable — a legal (yielding) state;
				// only label-routed AND serving as master violates the yield.
				servesAsMaster := infoErr == nil && v.role == "master"
				g.Expect(label == "master" && servesAsMaster).To(BeFalse(),
					"the restarted ex-master serves as labeled master again on its stale epoch — the kill-9 yield (ADR-011 §3) is broken")

				if !parkLogSeen {
					parkLogSeen = podContainerLogContains(testNamespace, masterPod, "redis", "already consumed")
				}
			}, 30*time.Second, 2*time.Second).Should(Succeed())

			By("waiting for the operator to promote a replica (new master, different pod)")
			var newMaster string
			Eventually(func(g Gomega) {
				newMaster = getMasterPod(crName)
				g.Expect(newMaster).NotTo(BeEmpty())
				g.Expect(newMaster).NotTo(Equal(masterPod),
					"a data-holding replica must be promoted; the empty restarted ex-master must not reclaim mastership")
				// Label-agreement guard (a26efa4 convention): only a labeled
				// master is a completed promotion (and a safe IP source below).
				g.Expect(getPodRoleLabel(testNamespace, newMaster)).To(Equal("master"),
					"status.master must agree with the role label")
			}, 2*time.Minute, 3*time.Second).Should(Succeed())

			newMasterIPRaw, err := utils.Run(exec.Command("kubectl", "get", "pod", newMaster,
				"-n", testNamespace, "-o", "jsonpath={.status.podIP}"))
			Expect(err).NotTo(HaveOccurred())
			newMasterIP := strings.TrimSpace(newMasterIPRaw)
			Expect(newMasterIP).NotTo(BeEmpty())

			By("verifying the ex-master rejoins as a replica of the NEW master at a strictly greater epoch")
			Eventually(func(g Gomega) {
				// The operator's re-authorization: a replica assignment whose
				// epoch strictly exceeds the consumed pre-kill epoch — the only
				// thing that may release the yielded pod (ADR-011 §3).
				g.Expect(getPodAnnotation(testNamespace, masterPod, failoverAnnRole)).To(Equal("replica"),
					"old master must be re-assigned as replica")
				epoch, err := strconv.ParseInt(
					getPodAnnotation(testNamespace, masterPod, failoverAnnEpoch), 10, 64)
				g.Expect(err).NotTo(HaveOccurred(), "old master must carry a parsable assignment-epoch")
				g.Expect(epoch).To(BeNumerically(">", preKillEpoch),
					"the re-authorization must carry a STRICTLY greater epoch than the consumed pre-kill one")

				g.Expect(getPodRoleLabel(testNamespace, masterPod)).To(Equal("replica"),
					"old master must be re-labeled replica")
				v, err := getReplicationView(testNamespace, masterPod)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(v.role).To(Equal("slave"), "old master must be running as a replica")
				g.Expect(v.masterHost).To(Equal(newMasterIP),
					"old master must follow the NEW master's IP")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the run-id changed (a fresh Redis process is running)")
			Eventually(func(g Gomega) {
				newRunID, err := getPodRunID(testNamespace, masterPod)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(newRunID).NotTo(Equal(oldRunID))
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			// Race-frequency observation only (see the invariant comment above):
			// did the epoch gate visibly PARK this run? The container survives
			// its exec, so the park lines — if any — are still in its log.
			if !parkLogSeen {
				parkLogSeen = podContainerLogContains(testNamespace, masterPod, "redis", "already consumed")
			}
			GinkgoWriter.Printf("Epoch-gate park log ('already consumed') observed this run: %v\n", parkLogSeen)

			By("verifying full topology consistency")
			verifyFailoverTopologySync(testNamespace, crName, 2)

			err = waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
			Expect(err).NotTo(HaveOccurred())

			metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
			Expect(err).NotTo(HaveOccurred())
			GinkgoWriter.Printf("Failover Kill-9 Metrics:\n%s\n", metrics.String())

			// Same bars as the sentinel kill-9 tier (design note §4 graduation gate).
			Expect(metrics.DataCorruptions).To(Equal(int64(0)), "Data corruption detected!")
			Expect(metrics.WriteAvailability()).To(BeNumerically(">", 0.40))
		})
	})

	// -------------------------------------------------------------------------
	// ROLLING UPDATE: config change rolls all pods one at a time with the
	// operator-led graceful handover (ADR-011 §7); the topology invariant (at
	// most one labeled master) holds throughout and data survives the roll.
	// -------------------------------------------------------------------------
	Context("Rolling Update", Ordered, func() {
		const crName = "failover-rolling"

		BeforeAll(func() {
			By("Test ID: FO-050 - deploying the rolling-update instance")
			deployFailover(crName, 2, 3000, nil)
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})

		AfterAll(func() { cleanupFailoverCR(crName) })

		It("should roll all pods on a config change, preserving data and the topology invariant", func() {
			By("writing test data before the update")
			masterPod := getMasterPod(crName)
			Expect(masterPod).NotTo(BeEmpty())
			out, err := redisExec(testNamespace, masterPod, "SET", "fo-rolling-key", "fo-rolling-value")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("OK"))
			for _, r := range otherRedisPods(crName, masterPod) {
				Eventually(func(g Gomega) {
					out, _ := redisExec(testNamespace, r, "GET", "fo-rolling-key")
					g.Expect(strings.TrimSpace(out)).To(Equal("fo-rolling-value"))
				}, 30*time.Second, 2*time.Second).Should(Succeed())
			}

			By("recording the pre-update pod instances (UIDs)")
			pods := failoverDataPods(crName)
			Expect(pods).To(HaveLen(3))
			oldUIDs := map[string]string{}
			for _, p := range pods {
				oldUIDs[p] = podUID(testNamespace, p)
			}

			By("applying a config change (maxmemory-policy) to trigger the roll")
			cr := failoverCR(crName, 2, 3000, nil, "") +
				"  config:\n    maxmemoryPolicy: volatile-lru\n"
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for every pod to be replaced, holding the one-labeled-master invariant throughout")
			Eventually(func(g Gomega) {
				// Topology invariant during the roll: never more than one pod of
				// this instance labeled master.
				labeledOut, _ := utils.Run(exec.Command("kubectl", "get", "pods", "-n", testNamespace,
					"-l", "redis.chuck-chuck-chuck.net/role=master,app.kubernetes.io/instance="+crName,
					"-o", "jsonpath={.items[*].metadata.name}"))
				labeled := strings.Fields(strings.TrimSpace(labeledOut))
				g.Expect(len(labeled)).To(BeNumerically("<=", 1),
					fmt.Sprintf("more than one pod labeled master during the roll: %v", labeled))

				for pod, oldUID := range oldUIDs {
					newUID := podUID(testNamespace, pod)
					g.Expect(newUID).NotTo(BeEmpty(), fmt.Sprintf("pod %s not found", pod))
					g.Expect(newUID).NotTo(Equal(oldUID), fmt.Sprintf("pod %s not yet replaced", pod))
				}
			}, 8*time.Minute, 5*time.Second).Should(Succeed(), "rolling update did not replace all pods")

			By("waiting for the instance to be Running again")
			Eventually(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the new config is live on the master")
			Eventually(func(g Gomega) {
				out, err := redisExec(testNamespace, getMasterPod(crName), "CONFIG", "GET", "maxmemory-policy")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("volatile-lru"))
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying data survived the roll (replication carried it across pod replacements)")
			Eventually(func(g Gomega) {
				out, err := redisExec(testNamespace, getMasterPod(crName), "GET", "fo-rolling-key")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("fo-rolling-value"), "data was lost during the rolling update")
			}, 1*time.Minute, 3*time.Second).Should(Succeed())

			verifyFailoverTopologySync(testNamespace, crName, 2)
		})
	})
})

// =============================================================================
// DEADLOCK tiers — the failover-mode analog of the sentinel leaderless tiers
// (leaderless_recovery_test.go), adapted: there are NO sentinels, and the
// safety gate is replication LINEAGE, not holder count (ADR-011 §5) — so the
// multi-holder tier expects an ORDINARY promotion, never a refuse.
// =============================================================================
var _ = Describe("Failover Mode Deadlock Recovery", Label("failover-mode"), func() {

	// --- Tier 1: total loss — every pod deleted at once ----------------------
	Context("Total-loss deadlock", Ordered, func() {
		var crName string
		BeforeAll(func() {
			crName = fmt.Sprintf("fo-deadlock-total-%d", time.Now().Unix())
			deployFailover(crName, 2, 3000, nil)
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})
		AfterAll(func() { cleanupFailoverCR(crName) })

		It("self-heals to Running after every pod is deleted (no opt-in required)", func() {
			By("Test ID: FO-060 - deleting ALL pods of the instance (assignments die with the pods)")
			_, err := deletePodsWithLabel(testNamespace, "app.kubernetes.io/instance="+crName)
			Expect(err).NotTo(HaveOccurred())

			By("the operator must re-seed and return to Running on its own")
			Eventually(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed(), "operator did not self-heal the total-loss deadlock")

			verifyFailoverTopologySync(testNamespace, crName, 2)
			By("a master must be named again")
			Expect(getMasterPod(crName)).NotTo(BeEmpty())
		})
	})

	// --- Tier 2: a single surviving replica still holds the data -------------
	Context("Single-survivor deadlock", Ordered, func() {
		var crName string
		BeforeAll(func() {
			crName = fmt.Sprintf("fo-deadlock-survivor-%d", time.Now().Unix())
			deployFailover(crName, 2, 3000, nil)
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})
		AfterAll(func() { cleanupFailoverCR(crName) })

		It("promotes the sole data holder and preserves its data (no opt-in required)", func() {
			By("Test ID: FO-061")
			master := getMasterPod(crName)
			Expect(master).NotTo(BeEmpty())

			By("writing data to the master")
			_, err := redisExec(testNamespace, master, "SET", "fo-survivor-key", "fo-survivor-value")
			Expect(err).NotTo(HaveOccurred())

			replicas := otherRedisPods(crName, master)
			Expect(replicas).To(HaveLen(2))
			survivor, doomedReplica := replicas[0], replicas[1]

			By("waiting for the survivor replica to receive the data")
			Eventually(func(g Gomega) {
				out, _ := redisExec(testNamespace, survivor, "GET", "fo-survivor-key")
				g.Expect(strings.TrimSpace(out)).To(Equal("fo-survivor-value"))
			}, 30*time.Second, 2*time.Second).Should(Succeed())

			By("force-killing the master AND one replica — keeping only the survivor")
			_, _ = deletePodMode(testNamespace, doomedReplica, false)
			_, err = deletePodMode(testNamespace, master, false)
			Expect(err).NotTo(HaveOccurred())

			By("the operator must promote the survivor and return to Running")
			Eventually(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("the data must have survived on the new master")
			newMaster := getMasterPod(crName)
			Expect(newMaster).NotTo(BeEmpty())
			Eventually(func(g Gomega) {
				out, _ := redisExec(testNamespace, newMaster, "GET", "fo-survivor-key")
				g.Expect(strings.TrimSpace(out)).To(Equal("fo-survivor-value"))
			}, 1*time.Minute, 3*time.Second).Should(Succeed(), "data was lost — the survivor was not promoted correctly")

			verifyFailoverTopologySync(testNamespace, crName, 2)
		})
	})

	// --- Tier 3: multiple same-lineage holders — ordinary failover, NO refuse.
	// This is where failover mode deliberately DIFFERS from sentinel Rule L:
	// the gate is lineage (holdersDiverged over {replid, replid2}), and two
	// replicas of one dead master are ONE lineage — promote the best holder
	// with no opt-in (ADR-011 §5, the LR-024 lesson).
	Context("Multi-holder same-lineage failover", Ordered, func() {
		var crName string
		BeforeAll(func() {
			crName = fmt.Sprintf("fo-deadlock-multi-%d", time.Now().Unix())
			deployFailover(crName, 2, 3000, nil)
			verifyFailoverTopologySync(testNamespace, crName, 2)
		})
		AfterAll(func() { cleanupFailoverCR(crName) })

		It("promotes the best same-lineage holder without refusing and preserves the data", func() {
			By("Test ID: FO-062")
			master := getMasterPod(crName)
			Expect(master).NotTo(BeEmpty())
			masterUID := podUID(testNamespace, master)
			Expect(masterUID).NotTo(BeEmpty())

			By("writing data and waiting for BOTH replicas to hold it")
			_, err := redisExec(testNamespace, master, "SET", "fo-multi-key", "fo-multi-value")
			Expect(err).NotTo(HaveOccurred())
			replicas := otherRedisPods(crName, master)
			Expect(replicas).To(HaveLen(2))
			for _, r := range replicas {
				Eventually(func(g Gomega) {
					out, _ := redisExec(testNamespace, r, "GET", "fo-multi-key")
					g.Expect(strings.TrimSpace(out)).To(Equal("fo-multi-value"))
				}, 30*time.Second, 2*time.Second).Should(Succeed(), "replica %s never received the data", r)
			}

			By("force-killing the master only — both replicas survive as same-lineage holders")
			_, err = deletePodMode(testNamespace, master, false)
			Expect(err).NotTo(HaveOccurred())

			By("the operator must treat this as an ORDINARY failover: promote, never refuse")
			Eventually(func(g Gomega) {
				// Refusing same-lineage holders would be the sentinel Rule L
				// holder-count gate leaking into failover mode — fail immediately.
				reason, _ := getConditionField(crName, "FailoverRecovery", "reason")
				Expect(reason).NotTo(Equal("RefusedDataPresent"),
					"failover mode must NOT refuse same-lineage holders (lineage gate, ADR-011 §5)")

				newMaster := getMasterPod(crName)
				g.Expect(newMaster).NotTo(BeEmpty())
				// Label-agreement guard against the post-force-delete stale-status
				// window (see the hybrid tier).
				g.Expect(getPodRoleLabel(testNamespace, newMaster)).To(Equal("master"),
					"status.master must agree with the role label")
				newUID := podUID(testNamespace, newMaster)
				g.Expect(newUID).NotTo(BeEmpty())
				g.Expect(newUID).NotTo(Equal(masterUID), "a promotion must have happened (new pod instance)")
				g.Expect(getPhase(crName)).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed(), "operator did not promote a same-lineage holder")

			By("the data must be intact on the new master")
			Eventually(func(g Gomega) {
				out, _ := redisExec(testNamespace, getMasterPod(crName), "GET", "fo-multi-key")
				g.Expect(strings.TrimSpace(out)).To(Equal("fo-multi-value"))
			}, 1*time.Minute, 3*time.Second).Should(Succeed(), "data was lost during multi-holder recovery")

			verifyFailoverTopologySync(testNamespace, crName, 2)
		})
	})
})

// =============================================================================
// Minimum topology: replicas: 1 (LR-038 / handover gap 3)
//
// `replicas: 1` is the CRD MINIMUM and therefore a supported, reachable topology,
// and until now nothing exercised it — unit or e2e. Every other failover spec runs
// the default 2. That gap mattered more than it looked:
//
//   - it is the only topology with a SINGLE witness, so it is where the
//     "is one witness enough corroboration to declare a master dead?" question
//     becomes real rather than academic (answered YES, pinned in the arity rows of
//     TestPlanMasterDeath)
//   - it is where the promoted master has ZERO replicas until the old pod returns,
//     which is what makes the new minReplicasToWrite: 1 default harshest — hence
//     this spec sets it to 0, the documented opt-out
//   - it is where the start-gate bug would have wiped the ONLY copy rather than
//     one of three
//
// Deliberately ONE spec, not a parameterised re-run of the whole tier. There is no
// count-dependent arithmetic anywhere in failover mode (no quorum, no majority):
// planMasterDeath branches on "any witness disagrees", planFailover on set-based
// lineage. So re-running 18 specs at another count would exercise identical
// branches with longer slices — the lowest-yield coverage available, and precisely
// the coverage that rots because only the default ever gets run.
//
// `replicas: 3` is DECLINED for the same reason, recorded here so it is not
// re-proposed as an obvious gap: it adds a pod and a few minutes of suite time and
// cannot reach a branch that 2 does not already reach. Arity 3 is covered where it
// costs nothing — the pure tables.
// =============================================================================
var _ = Describe("Failover Mode Minimum Topology", Label("failover-mode"), Ordered, func() {
	crName := "fo-min-topology"

	BeforeAll(func() {
		AddReportEntry("cr:" + crName)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		// minReplicasToWrite: 0 — the documented opt-out for this topology. With
		// the default 1, a promoted master here has no replica until the old pod
		// resyncs, so writes would be refused for the whole recovery rather than
		// just the handover. Setting it makes the spec assert the OPT-OUT path,
		// which is the one a replicas: 1 user is told to take.
		//
		// DELIBERATELY AUTH-FREE — do not "finish the flip" here.
		//
		// Every other failover-mode fixture in this suite defaults to auth-ON
		// (auth_utils_test.go), because in this mode the password is a real
		// mesh-isolation control. That default would leave the NO-AUTH path — which
		// is what `spec.auth.enabled: false`, the CRD default, gives every user who
		// has not opted in — with no functional coverage outside security_test.go's
		// single negative/positive PING pair. This tier is the one that keeps it:
		// it stands an instance up, replicates, kills the master, promotes and
		// asserts the data survived, all with no credential anywhere. It is also the
		// cheapest place to spend that coverage (2 pods, and it was already here).
		cmd.Stdin = strings.NewReader(
			failoverCRWithAuth(crName, 1, 5000, nil, "    minReplicasToWrite: 0\n", false))
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			g.Expect(getPhase(crName)).To(Equal("Running"))
			g.Expect(getMasterPod(crName)).NotTo(BeEmpty())
		}, 5*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() { cleanupFailoverCR(crName) })

	It("should stand up 2 pods and report a consistent topology", func() {
		cmd := exec.Command("kubectl", "get", "statefulset", crName+"-redis",
			"-n", testNamespace, "-o", "jsonpath={.status.readyReplicas}")
		out, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("2"), "replicas: 1 must yield 1 + 1 = 2 data pods")

		verifyFailoverTopologySync(testNamespace, crName, 1)
	})

	It("should fail over to its ONLY replica without losing data", func() {
		originalMaster := getMasterPod(crName)
		Expect(originalMaster).NotTo(BeEmpty())
		originalUID := podUID(testNamespace, originalMaster)
		Expect(originalUID).NotTo(BeEmpty())

		By("writing data and waiting for the sole replica to hold it")
		const key, value = "fo-min-key", "fo-min-value"
		out, err := redisExec(testNamespace, originalMaster, "SET", key, value)
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("OK"))

		replicas := otherRedisPods(crName, originalMaster)
		Expect(replicas).To(HaveLen(1), "replicas: 1 must have exactly one non-master data pod")
		soleReplica := replicas[0]
		Eventually(func(g Gomega) {
			out, _ := redisExec(testNamespace, soleReplica, "GET", key)
			g.Expect(strings.TrimSpace(out)).To(Equal(value))
		}, 30*time.Second, 2*time.Second).Should(Succeed(),
			"the sole replica never received the data — there is no second copy in this topology")

		By("killing the master, leaving exactly ONE witness and ONE data holder")
		_, err = deletePodMode(testNamespace, originalMaster, false)
		Expect(err).NotTo(HaveOccurred())

		By("the sole replica must be promoted (a single witness IS sufficient corroboration)")
		Eventually(func(g Gomega) {
			m := getMasterPod(crName)
			g.Expect(m).To(Equal(soleReplica),
				"the only surviving data holder must become master")
			g.Expect(getPodRoleLabel(testNamespace, m)).To(Equal("master"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("the data must survive — this was the only copy")
		Eventually(func(g Gomega) {
			out, _ := redisExec(testNamespace, getMasterPod(crName), "GET", key)
			g.Expect(strings.TrimSpace(out)).To(Equal(value))
		}, 1*time.Minute, 3*time.Second).Should(Succeed(),
			"data lost at replicas: 1 — there was no other holder to recover from")

		verifyFailoverTopologySync(testNamespace, crName, 1)
	})
})
