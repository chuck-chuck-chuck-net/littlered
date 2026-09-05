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

// Failover-mode chaos tier — the deliberate counterpart of the sentinel-mode
// "rapid double failover" pair in sentinel_standalone_chaos_test.go.
//
// WHY THIS EXISTS AS A MIRROR, NOT A VARIATION: ADR-011 offers failover mode as
// an alternative to sentinel mode, and its graduation gate asks whether it is at
// least as good under load. That question is only answerable if both modes are
// measured on the SAME yardstick, so this tier deliberately reuses the sentinel
// tier's shape verbatim — same 120s instrumented window, same two-failovers-20s-
// apart cadence, same downAfterMilliseconds (5000), same chaos client, and the
// same two assertions at the same thresholds. Divergences would be read as
// mode differences when they were really test differences.
//
// It is NOT redundant with the existing failover specs: "Hybrid Double-Failover"
// asserts correctness invariants (UID/RunID/label agreement) with no traffic
// flowing, and the standalone kill-9 tier measures a SINGLE in-place crash. This
// is the only failover spec that measures availability and durability across a
// rapid mastership cascade, and the only one that measures all three disruption
// shapes on one yardstick.
//
// Cost note: like its sentinel twin this is NOT labelled "extended" (parity —
// an opt-in mirror of a default-tier test would not get run alongside it), so it
// adds three ~3min specs to a default run. Skip with LABEL_FILTER='!failover-mode'.
var _ = Describe("Failover Mode Chaos Testing", Label("failover-mode"), Ordered, func() {

	Context("Failover Resilience", Ordered, func() {
		for _, d := range chaosDisruptions {
			d := d // capture range variable
			It(fmt.Sprintf("should maintain availability during rapid double failover (%s)", d.Name), func() {
				crName := fmt.Sprintf("chaos-failover-%s-%d", d.Name, time.Now().Unix())
				// Add dynamic labels for the artifact collector
				AddReportEntry("cr:" + crName)
				const testDuration = 120 * time.Second

				By(fmt.Sprintf("creating failover instance %s and chaos client simultaneously", crName))
				// downAfterMilliseconds 5000 matches the sentinel tier's setting so
				// the two modes' detection windows are comparable.
				cmd := exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(failoverCR(crName, 2, 5000, nil, ""))
				_, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())

				// Traffic targets the label-routed master Service ({crName}), which in
				// failover mode is the operator's role:master label selector — i.e. the
				// chaos client measures exactly what a real writer would experience.
				chaosPodName, err := deployChaosClient(testNamespace, "failover-chaos", crName+":6379", "chaos-fo", false, testDuration)
				Expect(err).NotTo(HaveOccurred())
				AddReportEntry("chaos:" + chaosPodName)

				// Cleanup defers - these will run in LIFO order.
				// We want artifact collection to happen BEFORE cleanup.
				// Suite-level AfterEach runs after these defers.
				defer func() {
					if debugOnFailure && suiteOrSpecFailed() {
						By("skipping failover instance cleanup to allow debugging")
						return
					}
					By("cleaning up failover instance")
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

				By("waiting for the failover instance to be ready")
				Eventually(func(g Gomega) {
					g.Expect(getPhase(crName)).To(Equal("Running"))
					g.Expect(getMasterPod(crName)).NotTo(BeEmpty())

					cmd := exec.Command("kubectl", "get", "littlered", crName,
						"-n", testNamespace, "-o", "jsonpath={.status.bootstrapRequired}")
					bootstrap, _ := utils.Run(cmd)
					g.Expect(bootstrap).To(Or(Equal("false"), Equal("")), "bootstrapRequired flag should be cleared")
				}, 5*time.Minute, 5*time.Second).Should(Succeed())

				// Full cross-check of the annotation intent, INFO replication and role labels.
				verifyFailoverTopologySync(testNamespace, crName, 2)

				By("waiting 10 seconds for baseline traffic")
				time.Sleep(10 * time.Second)

				// --- Failover 1 ---
				By("identifying and killing first master")
				master1 := getMasterPod(crName)
				Expect(master1).NotTo(BeEmpty())

				oldRunID1, _ := getPodRunID(testNamespace, master1)

				d.Apply(testNamespace, master1)

				// 20s comfortably clears the 10s post-transition cooldown that serializes
				// cascades (ADR-011), so failover 2 is a genuine second event and not a
				// request the operator is still holding off on from the first.
				By("waiting for failover to complete (approx 20s)")
				time.Sleep(20 * time.Second)

				// --- Failover 2 ---
				By("identifying and killing second master")
				var master2 string
				var oldRunID2 string
				Eventually(func(g Gomega) {
					master2 = getMasterPod(crName)
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
					master3 := getMasterPod(crName)
					g.Expect(master3).NotTo(Equal(master2), "Master should have changed again")
					g.Expect(master3).NotTo(BeEmpty())

					runID, err := getPodRunID(testNamespace, master3)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(runID).NotTo(Equal(oldRunID2), "Third master must have a different RunID")
				}, 1*time.Minute, 2*time.Second).Should(Succeed())

				By("Final topology synchronization verification")
				verifyFailoverTopologySync(testNamespace, crName, 2)

				err = waitForChaosClientComplete(testNamespace, chaosPodName, testDuration+2*time.Minute)
				Expect(err).NotTo(HaveOccurred())

				metrics, err := getChaosClientMetrics(testNamespace, chaosPodName)
				Expect(err).NotTo(HaveOccurred())
				GinkgoWriter.Printf("Failover Rapid-Double-Failover Metrics (%s):\n%s\n", d.Name, metrics.String())

				// Same two assertions, same thresholds, as the sentinel tier. Corruption
				// is the hard invariant; the availability bar is deliberately loose
				// because the point is comparability between the modes, not an SLO.
				Expect(metrics.DataCorruptions).To(Equal(int64(0)), "Data corruption detected!")
				// Per-cell, derived from LR-038's 10-pass matrix — see
				// chaosWriteAvailabilityFloor for the distribution each bar sits under.
				floor := chaosWriteAvailabilityFloor("failover", d.Name)
				Expect(metrics.WriteAvailability()).To(BeNumerically(">", floor),
					"write availability %.2f%% is below the %s/%s bar of %.0f%%: read this as a "+
						"behaviour change in that cell, not as flakiness",
					metrics.WriteAvailability()*100, "failover", d.Name, floor*100)

				// The sweep must actually have run, or FinalMissing == 0 is vacuous.
				Expect(metrics.FinalChecked).To(BeNumerically(">", 500),
					"final verification sweep did not check a meaningful number of keys")
				Expect(metrics.FinalUnreadable).To(BeNumerically("<", metrics.FinalChecked/10),
					"too many keys unreadable at sweep time to trust the durability verdict")

				// DURABILITY, on the graceful path only.
				//
				// A graceful master delete is a PLANNED handover, so an acknowledged
				// write should not silently vanish. The only writes a correct handover
				// can lose are those ACKed within the replication lag of the promotion
				// instant — at 10 writes/s that is ~1 per failover, so ~2 for this
				// spec's two failovers. The bound is 5, generous against that and
				// tight against the failure mode: today the dying master keeps serving
				// writes for its whole ~10s preStop window (resources_failover.go:408)
				// while its replica is never repointed away (the !anyTerminating gate,
				// failover_reconcile.go:454), and an established TCP connection through
				// the master Service is not re-routed by the label flip. That is
				// ~10s x 10 writes/s x 2 failovers = ~200 lost keys, not 5.
				//
				// The crash path is deliberately NOT bounded here: a kill -9 loses the
				// unreplicated tail by construction (async replication), and asserting
				// a number we have not measured would be tuning, not a check.
				// DURABILITY — every shape, not just the planned one.
				//
				// The bound is a design claim, not a fitted number: a master either
				// gets to fence itself before dying (graceful, force-delete) or dies
				// instantly with nothing in flight to lose (kill-9). Neither leaves a
				// window in which acknowledged writes can vanish, so the only writes
				// at risk are those ACKed within the replication lag of the promotion
				// instant — ~1 per failover at 10 writes/s, so ~2 here.
				//
				// Measured 0 on all three shapes. This is the assertion most likely to
				// teach us something if it ever fails: it went red at 202 of 1171 when
				// the terminating master was never fenced at all.
				Expect(metrics.FinalMissing).To(BeNumerically("<=", 5),
					"acknowledged writes were LOST (%s): %d of %d. DataCorruptions cannot "+
						"catch this — the writes are gone, not wrong.",
					d.Name, metrics.FinalMissing, metrics.FinalChecked)

				if d.Planned {

					// AVAILABILITY, graceful path only — the other half of the handover.
					//
					// A planned handover should cost only as long as the handover
					// actually takes. Two failovers, each costing the operator's
					// notice-plus-promote latency (~1-2s at a 2s fast requeue), is
					// ~2-4s of the 120s window, so >0.90 leaves generous headroom
					// while still catching the failure mode: a pod that holds its
					// grace window open for a fixed 10s keeps the client's pinned
					// connection on a fenced master for ~9s per failover, which is
					// ~20s of refused writes and lands near 0.82. Sentinel mode,
					// whose preStop exits as soon as the master address changes,
					// measures ~0.95 on the identical harness — so this bar also
					// keeps the mode comparison honest in the direction the fence
					// made worse.
					Expect(metrics.WriteAvailability()).To(BeNumerically(">", 0.90),
						"planned handover cost far more write availability than the handover itself: "+
							"%d of %d writes failed", metrics.WriteFailures, metrics.WriteAttempts)
				}
			})
		}
	})
})
