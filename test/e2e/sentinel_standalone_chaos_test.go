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

var _ = Describe("Sentinel and Standalone Chaos Testing", Ordered, func() {

	Context("Sentinel Resilience", Label("sentinel"), Ordered, func() {
		for _, d := range chaosDisruptions {
			d := d // capture range variable
			It(fmt.Sprintf("should maintain availability during rapid double failover (%s)", d.Name), func() {
				crName := fmt.Sprintf("chaos-sentinel-%s-%d", d.Name, time.Now().Unix())
				// Add dynamic labels for the artifact collector
				AddReportEntry("cr:" + crName)
				const testDuration = 120 * time.Second

				By(fmt.Sprintf("creating Sentinel cluster %s and chaos client simultaneously", crName))
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

				chaosPodName, err := deployChaosClient(testNamespace, "sentinel-chaos", crName+":6379", "chaos-sent", false, testDuration)
				Expect(err).NotTo(HaveOccurred())
				AddReportEntry("chaos:" + chaosPodName)

				// Cleanup defers - these will run in LIFO order.
				// We want artifact collection to happen BEFORE cleanup.
				// Suite-level AfterEach runs after these defers.
				defer func() {
					if debugOnFailure && suiteOrSpecFailed() {
						By("skipping Sentinel cluster cleanup to allow debugging")
						return
					}
					By("cleaning up Sentinel cluster")
					cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
					_, _ = utils.Run(cmd)
				}()
				defer func() {
					if debugOnFailure && suiteOrSpecFailed() {
						By("skipping chaos client cleanup to allow debugging")
						return
					}
					deleteChaosClient(testNamespace, chaosPodName)
				}()

				By("waiting for sentinel cluster to be ready")
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
					g.Expect(bootstrap).To(Or(Equal("false"), Equal("")), "bootstrapRequired flag should be cleared")
				}, 5*time.Minute, 5*time.Second).Should(Succeed())

				// Full cross-check of all sentinels and pod role labels
				verifySentinelTopologySync(testNamespace, crName, 3, 2)

				By("waiting 10 seconds for baseline traffic")
				time.Sleep(10 * time.Second)

				// --- Failover 1 ---
				By("identifying and killing first master")
				cmd = exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
				master1, _ := utils.Run(cmd)
				master1 = strings.TrimSpace(master1)

				oldRunID1, _ := getPodRunID(testNamespace, master1)

				d.Apply(testNamespace, master1)

				By("waiting for failover to complete (approx 20s)")
				time.Sleep(20 * time.Second)

				// --- Failover 2 ---
				By("identifying and killing second master")
				var master2 string
				var oldRunID2 string
				Eventually(func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "littlered", crName,
						"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
					out, _ := utils.Run(cmd)
					master2 = strings.TrimSpace(out)
					g.Expect(master2).NotTo(Equal(master1), "Master should have changed")
					g.Expect(master2).NotTo(BeEmpty())

					runID, err := getPodRunID(testNamespace, master2)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(runID).NotTo(Equal(oldRunID1), "New master must have a different RunID")
					oldRunID2 = runID
				}, 1*time.Minute, 2*time.Second).Should(Succeed())

				d.Apply(testNamespace, master2)

				By("verifying third master eventually emerges with different RunID")
				Eventually(func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "littlered", crName,
						"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
					out, _ := utils.Run(cmd)
					master3 := strings.TrimSpace(out)
					g.Expect(master3).NotTo(Equal(master2), "Master should have changed again")
					g.Expect(master3).NotTo(BeEmpty())

					runID, err := getPodRunID(testNamespace, master3)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(runID).NotTo(Equal(oldRunID2), "Third master must have a different RunID")
				}, 1*time.Minute, 2*time.Second).Should(Succeed())

				By("Step 6: Final topology synchronization verification")
				verifySentinelTopologySync(testNamespace, crName, 3, 2)

				err = waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
				Expect(err).NotTo(HaveOccurred())

				metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
				Expect(err).NotTo(HaveOccurred())
				// Printed so this tier can be compared against the failover-mode mirror
				// in failover_chaos_test.go. Without it the numbers are computed, asserted
				// against a deliberately loose bar, and discarded — which is how a 20-point
				// graceful-vs-crash spread in the other mode had no baseline to be read
				// against. Every other chaos tier already prints.
				GinkgoWriter.Printf("Sentinel Rapid-Double-Failover Metrics (%s):\n%s\n", d.Name, metrics.String())

				Expect(metrics.DataCorruptions).To(Equal(int64(0)), "Data corruption detected!")
				Expect(metrics.WriteAvailability()).To(BeNumerically(">", 0.40))

				// The sweep must actually have run, or FinalMissing == 0 is vacuous.
				Expect(metrics.FinalChecked).To(BeNumerically(">", 500),
					"final verification sweep did not check a meaningful number of keys")
				Expect(metrics.FinalUnreadable).To(BeNumerically("<", metrics.FinalChecked/10),
					"too many keys unreadable at sweep time to trust the durability verdict")

				// DURABILITY — same bound and same derivation as the failover-mode
				// mirror (LR-038), so the two tiers assert symmetrically rather than
				// only reporting symmetrically. A master either hands over before
				// dying, or dies with a guard that stops an emptied restart from
				// reclaiming mastership — sentinel mode's guard being the yield on
				// Sentinel's stored run-id. Neither leaves a window in which
				// acknowledged writes vanish, so the only writes at risk are those
				// ACKed within the replication lag of the promotion instant: ~1 per
				// failover at 10 writes/s, ~2 for this spec's two.
				//
				// Measured 0 on graceful and force-delete across four runs on two
				// clusters. NEW and unmeasured for kill-9 at the time of writing —
				// asserted as a PREDICTION from the yield mechanism, so a red here is
				// a finding about sentinel mode, not a flaky bound.
				Expect(metrics.FinalMissing).To(BeNumerically("<=", 5),
					"acknowledged writes were LOST (%s): %d of %d. DataCorruptions cannot "+
						"catch this — the writes are gone, not wrong.",
					d.Name, metrics.FinalMissing, metrics.FinalChecked)
			})
		}
	})

	Context("Standalone Mode Resilience", Label("standalone"), Ordered, func() {
		It("should recover after pod restart", func() {
			crName := "chaos-standalone"
			const testDuration = 60 * time.Second

			By("creating standalone and chaos client simultaneously")
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

			chaosPodName, err := deployChaosClient(testNamespace, "standalone-restart", crName+":6379", "chaos-stand", false, testDuration)
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				if debugOnFailure && suiteOrSpecFailed() {
					return
				}
				deleteChaosClient(testNamespace, chaosPodName)
			}()
			defer func() {
				cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
				_, _ = utils.Run(cmd)
			}()

			By("waiting for standalone to be ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("waiting 10 seconds for baseline traffic")
			time.Sleep(10 * time.Second)

			By("recording initial RunID")
			oldRunID, _ := getPodRunID(testNamespace, crName+"-redis-0")

			By("deleting the standalone pod")
			_, err = deletePod(testNamespace, crName+"-redis-0")
			Expect(err).NotTo(HaveOccurred())

			By("verifying pod restarts with different RunID")
			Eventually(func(g Gomega) {
				newRunID, err := getPodRunID(testNamespace, crName+"-redis-0")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(newRunID).NotTo(Equal(oldRunID), "Pod should have a different RunID after restart")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			err = waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
			Expect(err).NotTo(HaveOccurred())

			metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
			Expect(err).NotTo(HaveOccurred())

			Expect(metrics.DataCorruptions).To(Equal(int64(0)))
		})
	})
})
