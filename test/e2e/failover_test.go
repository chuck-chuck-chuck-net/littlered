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

var _ = Describe("Sentinel Advanced Failover", func() {

	Context("Event-Driven Label Updates", Ordered, func() {
		var crName string

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping event-driven CR cleanup to allow debugging")
				return
			}
			By("cleaning up event-driven CR")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		BeforeAll(func() {
			crName = fmt.Sprintf("event-driven-%d", time.Now().Unix())
			AddReportEntry("cr:" + crName)
			By(fmt.Sprintf("deploying cluster %s with polling disabled", crName))
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
  annotations:
    redis.chuck-chuck-chuck.net/disable-polling: "true"
spec:
  mode: sentinel
  sentinel:
    masterName: %s
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace, e2eMasterName(testNamespace, crName))
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
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should update master role label immediately via Sentinel event", func() {
			By("Step 1: Identify initial master and its RunID")
			cmd := exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
			initialMaster, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			initialMaster = strings.TrimSpace(initialMaster)

			oldRunID, err := getPodRunID(testNamespace, initialMaster)
			Expect(err).NotTo(HaveOccurred())

			By("Step 1.5: Wait for Sentinel full discovery")
			verifySentinelTopologySync(testNamespace, crName, 3, 2)

			By(fmt.Sprintf("Step 2: Kill the Master %s", initialMaster))
			_, err = deletePod(testNamespace, initialMaster)
			Expect(err).NotTo(HaveOccurred())

			By("Step 3: Wait for new master label and verify different RunID")
			start := time.Now()
			Eventually(func(g Gomega) {
				// We check the K8s label directly to see how fast the Operator reacted
				cmd := exec.Command("kubectl", "get", "pods", "-n", testNamespace, "-l", "redis.chuck-chuck-chuck.net/role=master", "-o", "jsonpath={.items[*].metadata.name}")
				out, _ := utils.Run(cmd)

				// Must contain the CR name (belong to this test) and NOT be the old master
				if strings.Contains(out, crName) && !strings.Contains(out, initialMaster) {
					// Found a different pod name, but let's be double sure via RunID
					newMaster := strings.TrimSpace(out)
					// Handle multiple pods returned (shouldn't happen with strict labels but possible during transition)
					for _, p := range strings.Fields(newMaster) {
						if strings.Contains(p, crName) && p != initialMaster {
							newRunID, err := getPodRunID(testNamespace, p)
							if err == nil {
								g.Expect(newRunID).NotTo(Equal(oldRunID), "New master must have a different RunID")
								return
							}
						}
					}
				}
				g.Expect(out).To(And(ContainSubstring(crName), Not(ContainSubstring(initialMaster))),
					fmt.Sprintf("New master label not yet applied or still points to old master. Current masters found: %q", out))
			}, 45*time.Second, 1*time.Second).Should(Succeed(), "Operator failed to update master label")

			duration := time.Since(start)
			fmt.Fprintf(GinkgoWriter, "Event-driven failover took: %v\n", duration)
			Expect(duration).To(BeNumerically("<", 15*time.Second), "Event-driven failover was too slow (likely fell back to other mechanisms)")

			verifySentinelTopologySync(testNamespace, crName, 3, 2)
		})
	})

	Context("Polling-Only Recovery", Ordered, func() {
		var crName string

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				return
			}
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		BeforeAll(func() {
			crName = fmt.Sprintf("polling-only-%d", time.Now().Unix())
			AddReportEntry("cr:" + crName)
			By(fmt.Sprintf("deploying cluster %s with events disabled", crName))
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
  annotations:
    redis.chuck-chuck-chuck.net/disable-event-monitoring: "true"
spec:
  mode: sentinel
  sentinel:
    masterName: %s
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace, e2eMasterName(testNamespace, crName))
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			utils.Run(cmd)

			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName, "-n", testNamespace, "-o", "jsonpath={.status.phase}")
				out, _ := utils.Run(cmd)
				g.Expect(out).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should eventually update master label via periodic polling", func() {
			startTime := time.Now().Add(-5 * time.Second)
			By("Step 1: Identify initial master and its RunID")
			cmd := exec.Command("kubectl", "get", "littlered", crName, "-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
			initialMaster, _ := utils.Run(cmd)
			initialMaster = strings.TrimSpace(initialMaster)

			oldRunID, _ := getPodRunID(testNamespace, initialMaster)

			By("Step 1.5: Wait for Sentinel full discovery")
			verifySentinelTopologySync(testNamespace, crName, 3, 2)

			By(fmt.Sprintf("Step 2: Kill the Master %s", initialMaster))
			_, err := deletePod(testNamespace, initialMaster)
			Expect(err).NotTo(HaveOccurred())

			By("Step 3: Wait for new master label and verify different RunID")
			start := time.Now()
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "-n", testNamespace, "-l", "redis.chuck-chuck-chuck.net/role=master", "-o", "jsonpath={.items[*].metadata.name}")
				out, _ := utils.Run(cmd)
				if strings.Contains(out, crName) && !strings.Contains(out, initialMaster) {
					// Check RunID
					newMaster := strings.Fields(strings.TrimSpace(out))[0]
					newRunID, err := getPodRunID(testNamespace, newMaster)
					if err == nil {
						g.Expect(newRunID).NotTo(Equal(oldRunID), "New master must have a different RunID")
						return
					}
				}
				g.Expect(out).To(And(ContainSubstring(crName), Not(ContainSubstring(initialMaster))),
					fmt.Sprintf("New master label not yet applied. Current masters found: %q", out))
			}, 60*time.Second, 2*time.Second).Should(Succeed(), "Operator failed to update master label")

			duration := time.Since(start)
			fmt.Fprintf(GinkgoWriter, "Polling-based failover took: %v\n", duration)
			By("Step 4: Verify logs show event monitoring was disabled")
			Eventually(func(g Gomega) {
				since := startTime.Format(time.RFC3339Nano)
				cmd = exec.Command("sh", "-c", fmt.Sprintf("kubectl logs -n littlered-system -l control-plane=controller-manager --tail=-1 --since-time=%s | grep %s", since, crName))
				logs, _ := utils.Run(cmd)
				g.Expect(logs).To(ContainSubstring("Sentinel event monitoring disabled via annotation"))
				g.Expect(logs).NotTo(ContainSubstring("Triggering reconciliation via Sentinel event"))
			}, 30*time.Second, 2*time.Second).Should(Succeed())

			verifySentinelTopologySync(testNamespace, crName, 3, 2)
		})
	})

	Context("Hybrid (Production) Mode", Ordered, func() {
		var crName string

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				return
			}
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		BeforeAll(func() {
			crName = fmt.Sprintf("hybrid-%d", time.Now().Unix())
			AddReportEntry("cr:" + crName)
			By(fmt.Sprintf("deploying cluster %s with standard settings", crName))
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
  sentinel:
    masterName: %s
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace, e2eMasterName(testNamespace, crName))
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			utils.Run(cmd)

			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName, "-n", testNamespace, "-o", "jsonpath={.status.phase}")
				out, _ := utils.Run(cmd)
				g.Expect(out).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
		})

		for _, mode := range restartModes {
			mode := mode // capture range variable
			It(fmt.Sprintf("should recover correctly with both mechanisms active (%s)", mode.Name), func() {
				By("Step 1: Identify initial master and its RunID")
				cmd := exec.Command("kubectl", "get", "littlered", crName, "-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
				initialMaster, _ := utils.Run(cmd)
				initialMaster = strings.TrimSpace(initialMaster)

				oldRunID, _ := getPodRunID(testNamespace, initialMaster)

				By(fmt.Sprintf("Step 2: Kill the Master %s (%s mode)", initialMaster, mode.Name))
				_, err := deletePodMode(testNamespace, initialMaster, mode.Graceful)
				Expect(err).NotTo(HaveOccurred())

				By("Step 3: Wait for new master label and verify different RunID")
				Eventually(func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "pods", "-n", testNamespace, "-l", "redis.chuck-chuck-chuck.net/role=master", "-o", "jsonpath={.items[*].metadata.name}")
					out, _ := utils.Run(cmd)
					if strings.Contains(out, crName) && !strings.Contains(out, initialMaster) {
						// Check RunID
						newMaster := strings.Fields(strings.TrimSpace(out))[0]
						newRunID, err := getPodRunID(testNamespace, newMaster)
						if err == nil {
							g.Expect(newRunID).NotTo(Equal(oldRunID), "New master must have a different RunID")
							return
						}
					}
					g.Expect(out).To(And(ContainSubstring(crName), Not(ContainSubstring(initialMaster))),
						fmt.Sprintf("New master label not yet applied. Current masters found: %q", out))
				}, 45*time.Second, 1*time.Second).Should(Succeed(), "Operator failed to update master label")

				verifySentinelTopologySync(testNamespace, crName, 3, 2)
			})
		}
	})

	Context("Sentinel Pod Resilience", Ordered, func() {
		var crName string

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				return
			}
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		BeforeAll(func() {
			crName = fmt.Sprintf("sentinel-death-%d", time.Now().Unix())
			AddReportEntry("cr:" + crName)
			By(fmt.Sprintf("deploying cluster %s for sentinel death testing", crName))
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
  sentinel:
    masterName: %s
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace, e2eMasterName(testNamespace, crName))
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			utils.Run(cmd)

			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName, "-n", testNamespace, "-o", "jsonpath={.status.phase}")
				out, _ := utils.Run(cmd)
				g.Expect(out).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should still perform failover after a sentinel pod is restarted", func() {
			By("Step 1: Identify initial master and its RunID")
			cmd := exec.Command("kubectl", "get", "littlered", crName, "-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
			initialMaster, _ := utils.Run(cmd)
			initialMaster = strings.TrimSpace(initialMaster)

			oldRunID, _ := getPodRunID(testNamespace, initialMaster)

			sentinelPod := fmt.Sprintf("%s-sentinel-0", crName)

			By("Step 2: Kill the Sentinel pod")
			// This tests if the Operator's background monitor handles connection loss and reconnects
			_, err := deletePod(testNamespace, sentinelPod)
			Expect(err).NotTo(HaveOccurred())

			By("Step 3: Wait for sentinel pod to be recreated and ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", sentinelPod, "-n", testNamespace, "-o", "jsonpath={.status.phase}")
				out, _ := utils.Run(cmd)
				g.Expect(out).To(Equal("Running"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("Step 4: Kill the Redis Master")
			_, err = deletePod(testNamespace, initialMaster)
			Expect(err).NotTo(HaveOccurred())

			By("Step 5: Verify failover still happens and RunID changed")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "-n", testNamespace, "-l", "redis.chuck-chuck-chuck.net/role=master", "-o", "jsonpath={.items[*].metadata.name}")
				out, _ := utils.Run(cmd)
				if strings.Contains(out, crName) && !strings.Contains(out, initialMaster) {
					// Check RunID
					newMaster := strings.Fields(strings.TrimSpace(out))[0]
					newRunID, err := getPodRunID(testNamespace, newMaster)
					if err == nil {
						g.Expect(newRunID).NotTo(Equal(oldRunID), "New master must have a different RunID")
						return
					}
				}
				g.Expect(out).To(And(ContainSubstring(crName), Not(ContainSubstring(initialMaster))),
					fmt.Sprintf("Failover failed after sentinel restart. Current masters: %q", out))
			}, 45*time.Second, 1*time.Second).Should(Succeed(), "Operator failed to update master label after sentinel restart")

			verifySentinelTopologySync(testNamespace, crName, 3, 2)
		})
	})
})
