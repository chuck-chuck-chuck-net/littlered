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

// cluster_behaviour_test.go holds the ONE shared body of topology-agnostic cluster behaviours,
// run under BOTH single-site and multi-site topologies via the clusterHarness abstraction
// (cluster_harness_test.go). The point: a stretched multi-site cluster IS just a Redis Cluster —
// bring-up, basic ops, and pod-failure recovery must all hold when it is stretched across sites,
// with data intact. The specs assert OUTCOMES (data intact + all 16384 slots served + converges
// healthy), not the mechanism — multi-site pod failure may recover via native cross-site failover
// OR (below quorum, the takeover suite's job) the operator, and the shared body must not care which.
//
// Topology matrix (env E2E_TOPOLOGY: single|multi|both; default single = the suite's historical
// behaviour). Single-site-only cluster specs that DON'T generalise stay in their own files:
//   - cluster_totalwipe_test.go  — kill-9-all / nodes.conf-on-EmptyDir semantics are per-site;
//     the multi-site analogue is the ADR-008 per-site wipe recovery, exercised inside the sites.
//   - cluster_placement_test.go  — the per-shard topologySpreadConstraint is one cluster's node
//     spread; the multi-site analogue is the SITE dimension, covered by M4a/M4b placement asserts.
//   - the 0-replica self-heal / custom-config / debug-no-slots / rolling-update / multipod specs
//     in cluster_functional_test.go + cluster_chaos_test.go stay single-site (not in the matrix).

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// clusterBehaviourSpecs registers the shared, topology-agnostic cluster behaviour Contexts against
// the given harness. Call it once per topology from a topology-gated Describe.
func clusterBehaviourSpecs(h clusterHarness) {
	topo := h.topology()

	// --- Bring-up + basic operations -----------------------------------------------------
	Context("Bring-up and basic operations", Ordered, func() {
		crName := fmt.Sprintf("beh-%s-basic", topo)

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
			By(fmt.Sprintf("[%s] applying a redundant cluster CR %q", topo, crName))
			Expect(h.applyRedundantCR(crName)).To(Succeed())
			By(fmt.Sprintf("[%s] waiting for %q to be Running", topo, crName))
			h.waitRunning(crName, 8*time.Minute)
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup due to failure and DEBUG_ON_FAILURE=true")
				return
			}
			h.deleteCR(crName)
		})

		It("forms a healthy cluster serving all 16384 slots", func() {
			By("checking cluster_state:ok and all slots assigned")
			Eventually(func(g Gomega) {
				out, err := h.execRedis(h.queryPod(crName), "CLUSTER", "INFO")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("cluster_state:ok"))
				g.Expect(out).To(ContainSubstring("cluster_slots_assigned:16384"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By(fmt.Sprintf("verifying %d masters and %d replicas via CLUSTER NODES",
				h.expectedMasters(), h.expectedReplicas()))
			Eventually(func(g Gomega) {
				masters, replicas := countMastersReplicas(h, crName, g)
				g.Expect(masters).To(Equal(h.expectedMasters()))
				g.Expect(replicas).To(Equal(h.expectedReplicas()))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("stores and retrieves data across shards with redirection", func() {
			keys := []string{"beh-key1", "beh-key2", "beh-key3"}
			By("setting keys through the cluster (with -c redirection)")
			for _, k := range keys {
				_, err := h.execRedis(h.queryPod(crName), "-c", "SET", k, "value-"+k)
				Expect(err).NotTo(HaveOccurred())
			}
			By("getting keys back")
			for _, k := range keys {
				out, err := h.execRedis(h.queryPod(crName), "-c", "GET", k)
				Expect(err).NotTo(HaveOccurred())
				Expect(strings.TrimSpace(out)).To(Equal("value-" + k))
			}
		})

		It("converges to a topology-appropriate healthy end state", func() {
			h.verifyHealthy(crName)
		})
	})

	// --- Master-pod-failure recovery under chaos -----------------------------------------
	// Isolated instance per scenario (no shared Ordered state — CLAUDE.md §10 LR-024).
	Context("Master pod-failure recovery under chaos", func() {
		for _, mode := range restartModes {
			It(fmt.Sprintf("recovers from a master-pod failure keeping data intact (%s)", mode.Name), func() {
				crName := fmt.Sprintf("beh-%s-chaos-%s", topo, mode.Name)
				const testDuration = 60 * time.Second
				AddReportEntry("cr:" + crName)

				By(fmt.Sprintf("[%s] applying a redundant cluster CR %q", topo, crName))
				Expect(h.applyRedundantCR(crName)).To(Succeed())
				defer func() {
					if debugOnFailure && suiteOrSpecFailed() {
						return
					}
					h.deleteCR(crName)
				}()

				By("waiting for the cluster to be Running and ok")
				h.waitRunning(crName, 8*time.Minute)
				Eventually(func(g Gomega) {
					out, err := h.execRedis(h.queryPod(crName), "CLUSTER", "INFO")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(out).To(ContainSubstring("cluster_state:ok"))
				}, 3*time.Minute, 5*time.Second).Should(Succeed())

				// Deploy the chaos client only AFTER the cluster is Running: the multi-site
				// harness seeds the client with the stretched cluster's routed POD IPs, which
				// do not exist until the pods are scheduled (a pre-Running deploy would seed an
				// empty address list). Single-site seeds a Service name and tolerates either
				// order, so post-Running is correct for both.
				By("deploying the chaos client against the live cluster")
				chaos, err := h.deployChaos(crName, topo+"-chaos-"+mode.Name, testDuration)
				Expect(err).NotTo(HaveOccurred())
				AddReportEntry("chaos:" + chaos.pod)
				defer func() {
					if debugOnFailure && suiteOrSpecFailed() {
						return
					}
					h.deleteChaos(chaos)
				}()

				By("waiting 10s for baseline traffic")
				time.Sleep(10 * time.Second)

				By("resolving shard 0's current master (ground truth) as the victim")
				victim, err := h.resolveMasterForSlot(crName, slotForShard(0))
				Expect(err).NotTo(HaveOccurred())
				// A survivor in a different shard for post-kill cluster-wide reads.
				survivor, err := h.resolveMasterForSlot(crName, slotForShard(1))
				Expect(err).NotTo(HaveOccurred())

				By(fmt.Sprintf("deleting master pod %s (%s)", victim, mode.Name))
				Expect(h.deletePod(victim, mode.Graceful)).To(Succeed())

				By("waiting for the chaos client to finish")
				Expect(h.waitChaos(chaos, testDuration+3*time.Minute)).To(Succeed())

				metrics, err := h.chaosMetrics(chaos)
				Expect(err).NotTo(HaveOccurred())
				GinkgoWriter.Printf("[%s] master-failure(%s) write availability: %.2f%%, corruptions: %d\n",
					topo, mode.Name, metrics.WriteAvailability()*100, metrics.DataCorruptions)

				// HARD invariant, both topologies: no corrupted data across the failover.
				Expect(metrics.DataCorruptions).To(Equal(int64(0)))
				// Soft availability floor. Cross-site native failover can take up to
				// cluster-node-timeout (15s), so multi-site tolerates a lower floor; single-site
				// keeps its historical 0.80.
				floor := 0.80
				if topo == "multi" {
					floor = 0.50
				}
				Expect(metrics.WriteAvailability()).To(BeNumerically(">=", floor))

				By("asserting the cluster reconverges: all slots served AND redundancy restored")
				Eventually(func(g Gomega) {
					out, err := h.execRedis(survivor, "CLUSTER", "INFO")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(out).To(ContainSubstring("cluster_state:ok"))
					g.Expect(out).To(ContainSubstring("cluster_slots_assigned:16384"))
					masters, replicas := countMastersReplicas(h, crName, g)
					g.Expect(masters).To(Equal(h.expectedMasters()))
					g.Expect(replicas).To(Equal(h.expectedReplicas()))
				}, 6*time.Minute, 10*time.Second).Should(Succeed())

				By("verifying a topology-appropriate healthy end state with data intact")
				h.verifyHealthy(crName)
			})
		}
	})
}

// countMastersReplicas queries CLUSTER NODES from a reachable pod and counts masters/replicas
// cluster-wide (topology-agnostic). It queries the primary-shard-0 pod first, then falls back to
// any master handle it can resolve, so it survives the victim being unreachable mid-recovery.
func countMastersReplicas(h clusterHarness, crName string, g Gomega) (masters, replicas int) {
	out, err := h.execRedis(h.queryPod(crName), "CLUSTER", "NODES")
	if err != nil {
		// Fall back to a survivor in another shard.
		if surv, e2 := h.resolveMasterForSlot(crName, slotForShard(1)); e2 == nil {
			out, err = h.execRedis(surv, "CLUSTER", "NODES")
		}
	}
	g.Expect(err).NotTo(HaveOccurred(), "CLUSTER NODES from a reachable pod")
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 3 {
			continue
		}
		flags := f[2]
		if strings.Contains(flags, "fail") {
			continue
		}
		if strings.Contains(flags, "slave") {
			replicas++
		} else if strings.Contains(flags, "master") {
			masters++
		}
	}
	return masters, replicas
}

// --- Topology-gated Describe registrations ------------------------------------------------

var _ = Describe("Cluster Behaviour Matrix [single-site]", Label("cluster-behaviour"), func() {
	if !topologyEnabled("single") {
		return
	}
	clusterBehaviourSpecs(newSingleSiteHarness())
})

var _ = Describe("Cluster Behaviour Matrix [multi-site]", Ordered, ContinueOnFailure, Label("multisite", "cluster-behaviour"), func() {
	if !topologyEnabled("multi") {
		return
	}
	// The instance namespace follows TEST_NAMESPACE when a run sets one, so the
	// in-cluster harness can give every work item its own (and so a scoped
	// operator, whose watch list the harness derives from the same value,
	// actually reconciles it). Unset => the historical name.
	ns := msInstanceNamespace("ms-behaviour")
	reg := msRegistryFromEnv()
	h := newMultiSiteHarness(reg, ns)

	BeforeAll(func() {
		By("multi-site behaviour: provisioning per-site namespace/RBAC/Secrets")
		msSetupSites(reg, ns)
	})

	AfterAll(func() {
		if debugOnFailure && suiteOrSpecFailed() {
			By("skipping multi-site behaviour teardown due to failure and DEBUG_ON_FAILURE=true")
			return
		}
		By("multi-site behaviour: tearing down namespace + hub site Secrets")
		for _, s := range reg.orderedSites {
			_, _ = kctl(s.context, "delete", "ns", ns, "--ignore-not-found", "--wait=false")
			_, _ = kctl(reg.hubContext, "-n", reg.operatorNS, "delete", "secret", msSiteSecretName(ns, s.name), "--ignore-not-found")
		}
		_, _ = kctl(reg.hubContext, "delete", "ns", ns, "--ignore-not-found", "--wait=false")
	})

	clusterBehaviourSpecs(h)
})
