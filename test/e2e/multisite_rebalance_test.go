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

// multisite_rebalance_test.go carries matrix cells G1, G4 and G5 (§MS-G) — the master
// rebalancer.
//
// THIS DOMAIN IS FIRST CONTACT, NOT REGRESSION. `rebalanceMasters → pickRebalanceReplica →
// ClusterFailover` had never executed against real Redis anywhere, in any topology, when these
// cells were written. Pillar 3.15 records the rebalancer as unit/envtest-verified with live
// validation pending, and LR-031 changed the planner's thresholds without touching the executor.
// LR-031 also raised the stakes: it switched the rebalancer ON for topologies where it was
// previously inert (K=4 over 2 or 3 sites, including the D3-recommended dual-site), so existing
// instances carrying drift start moving mastership after an upgrade. These cells are the first
// evidence that the actuation works at all.
//
// DRIFT IS INDUCED BY A SINGLE POD DELETE, which is why MS-G needs no fault primitive and lands
// in M3.5. At rps:0 with sps:2 every shard has exactly one replica, in the other hosting site —
// so deleting a shard's master hands mastership to a known site, deterministically, via Redis's
// own native failover. The operator is not involved in creating the drift, only in correcting it.
//
// WHAT IS ASSERTED, and why the event matters (LR-037). Before this work the rebalancer emitted
// no event: its only outputs were a master-count distribution (from which a MOVE must be inferred
// after the fact) and an audit log line. Counting moves by grepping audit prose silently reports
// zero the moment someone rewords the message, so the cells would have been unfalsifiable in the
// most dangerous direction. LR-037 added one Normal `MasterRebalanced` event per move; these
// cells assert on it, which is what makes "exactly ONE move, ≥30s apart" a real claim.
//
// SCOPE, stated rather than implied. The matrix's G4 key assertion also asks for the arithmetic
// CONSEQUENCE — that a subsequent loss of the over-mastered site then Takes over instead of
// refusing. That requires the whole-site kill primitive and belongs with the MS-A cells, which
// own it; asserting it here would make a no-fault-primitive cell destructive and duplicate A8/A9.
// What these cells own is the half that has never run: the executor, its serialization, and the
// two-tier planner's choice of destination. The threshold ARITHMETIC itself is already guarded by
// site_balance_property_test.go, which drives the real planSiteTakeover over every (K, loss) pair.
//
// Red-first (CLAUDE.md tier-3): the executor is new behaviour, so its red is "has never run" —
// there is nothing to break first. The repeatable guards are the LR-037 unit tests
// (TestRebalanceMastersFires: one failover per reconcile, one event, none on failure) and the
// LR-031 cross-seam property test. These cells are the integration evidence.

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// msRebalanceEventReason is the operator's own constant (reasonMasterRebalanced, LR-037). Pinned
// here because a wrong reason string makes every event assertion below silently vacuous.
const msRebalanceEventReason = "MasterRebalanced"

// msCountEvents returns how many Events the CR has emitted with the given reason. Unlike
// msHasEvent this can distinguish "one move" from "a burst", which is the whole point of the
// serialization assertion.
func msCountEvents(reg *msRegistry, ns, crName, reason string) int {
	out, err := kctl(reg.hubContext, "-n", ns, "get", "events",
		"--field-selector", "involvedObject.name="+crName+",reason="+reason, "-o", "name")
	if err != nil {
		return 0
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return 0
	}
	return len(strings.Split(trimmed, "\n"))
}

// msMasterCounts reads the operator's published master-per-site distribution
// (status.multiSite.masterCounts). This is the surface a human reads and lrctl renders, so the
// cells assert on it rather than on a re-derivation from CLUSTER NODES.
func msMasterCounts(reg *msRegistry, ns, crName string) map[string]int {
	out := map[string]int{}
	lr, err := msGetCR(reg, ns, crName)
	if err != nil || lr.Status.MultiSite == nil {
		return out
	}
	for _, mc := range lr.Status.MultiSite.MasterCounts {
		out[mc.Site] = int(mc.Masters)
	}
	return out
}

// msSortedCounts renders a distribution as a stable, comparable string like "s1=2,s2=1,s3=1",
// so a failure message shows the actual shape rather than an unordered map dump.
func msSortedCounts(reg *msRegistry, counts map[string]int) string {
	var parts []string
	for _, s := range reg.orderedSites {
		parts = append(parts, s.name+"="+strconv.Itoa(counts[s.name]))
	}
	return strings.Join(parts, ",")
}

// msPodIPToPod maps pod IP → (site, pod name) across every site, so a master identified by IP in
// CLUSTER NODES can be turned into something deletable.
func msPodIPToPod(reg *msRegistry, ns string) map[string][2]string {
	out := map[string][2]string{}
	for _, s := range reg.orderedSites {
		raw, err := kctl(s.context, "-n", ns, "get", "pods", "-l", "app.kubernetes.io/component=cluster",
			"-o", "jsonpath={range .items[*]}{.status.podIP}={.metadata.name}{\"\\n\"}{end}")
		if err != nil {
			continue
		}
		for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
			kv := strings.SplitN(line, "=", 2)
			if len(kv) == 2 && kv[0] != "" {
				out[kv[0]] = [2]string{s.name, kv[1]}
			}
		}
	}
	return out
}

// msDrainOneMaster moves exactly one master OFF fromSite by force-deleting it, letting Redis's
// own native failover promote its cross-site replica. Returns the site that gained mastership.
//
// This is the drift primitive, and it is deliberately Redis-driven: the operator must not be
// involved in CREATING the imbalance, or the cell would be testing the operator against itself.
func msDrainOneMaster(reg *msRegistry, ns, seedSite, seedPod, fromSite string) (gained string) {
	rows, err := msClusterNodes(reg.site(seedSite).context, ns, seedPod)
	Expect(err).NotTo(HaveOccurred(), "CLUSTER NODES for drift induction")

	ipPod := msPodIPToPod(reg, ns)

	// Find a slot-owning master in fromSite whose replica lives elsewhere: deleting it hands
	// mastership to that replica's site.
	for _, r := range rows {
		if r.role != "master" || strings.TrimSpace(r.slots) == "" {
			continue
		}
		loc, ok := ipPod[r.ip]
		if !ok || loc[0] != fromSite {
			continue
		}
		// Locate this master's replica and the site it is in.
		for _, rep := range rows {
			if rep.role != "replica" || rep.masterID != r.id {
				continue
			}
			repLoc, ok := ipPod[rep.ip]
			if !ok || repLoc[0] == fromSite {
				continue
			}
			By(fmt.Sprintf("inducing drift: force-deleting master %s in %s (its replica is in %s)",
				loc[1], fromSite, repLoc[0]))
			_, err := kctl(reg.site(fromSite).context, "-n", ns, "delete", "pod", loc[1],
				"--force", "--grace-period=0")
			Expect(err).NotTo(HaveOccurred(), "deleting master pod %s", loc[1])
			return repLoc[0]
		}
	}
	Fail(fmt.Sprintf("no slot-owning master in %s with a cross-site replica: cannot induce drift", fromSite))
	return ""
}

// msRebalanceScenario is the shared body of G1/G4/G5: they differ only in topology and in the
// distribution the rebalancer must restore.
type msRebalanceScenario struct {
	ns, crName string
	sites      []string
	shards     int
	// drainFrom is the site a master is moved OFF, creating the drift.
	drainFrom string
	// wantAfter is the distribution the rebalancer must restore, as msSortedCounts renders it.
	// Derived from the ADR-012 D2 arithmetic BEFORE the run (matrix §2), never adjusted to fit.
	wantAfter string
	// why records the arithmetic, so a failure says which tier was expected to plan.
	why string
}

var _ = Describe("Multi-Site Master Rebalancer (G1, G4, G5)", Label("multisite"), Label("multisite-rebalance"), func() {
	var reg *msRegistry

	BeforeEach(func() {
		reg = msRegistryFromEnv()
	})

	runRebalance := func(sc msRebalanceScenario) {
		setMSDebugTarget(reg, sc.ns, sc.crName)
		msSetupSites(reg, sc.ns)
		msRecordInstance(reg, sc.ns, sc.crName)

		By(fmt.Sprintf("standing up %v shards:%d sitesPerShard:2 replicasPerShard:0 — %s",
			sc.sites, sc.shards, sc.why))
		_, err := kctlIn(reg.hubContext,
			msClusterCR(reg, sc.ns, sc.crName, sc.sites, sc.shards, 0, 2, 15000), "apply", "-f", "-")
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			lr, err := msGetCR(reg, sc.ns, sc.crName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(string(lr.Status.Phase)).To(Equal(string(littleredv1alpha1.PhaseRunning)))
		}, "10m", "10s").Should(Succeed())

		seedSite, seedPod := sc.sites[0], clusterMasterPod(sc.crName, 0)
		msVerifyCrossSiteTopology(reg, sc.ns, sc.crName, seedSite, seedPod, true)

		By("writing a data anchor on every shard, so a lossy rebalance cannot pass")
		// The rebalancer's primitive is a GRACEFUL CLUSTER FAILOVER (LR-025's no-data-loss
		// primitive), so zero loss is a hard requirement, not a nice-to-have.
		anchors := map[string]string{}
		for i := range sc.shards {
			key := fmt.Sprintf("g:anchor:%d", i)
			val := fmt.Sprintf("shard-%d-value", i)
			writeKV(reg.site(seedSite).context, sc.ns, seedPod, key, val)
			anchors[key] = val
		}

		before := msMasterCounts(reg, sc.ns, sc.crName)
		By("baseline master distribution: " + msSortedCounts(reg, before))

		// Events emitted before the drift are not ours to count.
		baselineEvents := msCountEvents(reg, sc.ns, sc.crName, msRebalanceEventReason)

		gained := msDrainOneMaster(reg, sc.ns, seedSite, seedPod, sc.drainFrom)

		By("waiting for the drift to appear in the published distribution (native failover moved a master to " + gained + ")")
		Eventually(func(g Gomega) {
			counts := msMasterCounts(reg, sc.ns, sc.crName)
			g.Expect(counts[sc.drainFrom]).To(BeNumerically("<", before[sc.drainFrom]),
				"the drained site must have lost a master; distribution is %s", msSortedCounts(reg, counts))
		}, "5m", "5s").Should(Succeed())

		By("asserting the rebalancer corrects the drift to " + sc.wantAfter + " — " + sc.why)
		// Generous: the healthy-path gate needs the cluster quiet and the replaced pod
		// re-synced before a move is planned at all, plus the 30s settle cooldown.
		Eventually(func(g Gomega) {
			counts := msMasterCounts(reg, sc.ns, sc.crName)
			g.Expect(msSortedCounts(reg, counts)).To(Equal(sc.wantAfter),
				"the rebalancer must restore this distribution; %s", sc.why)
		}, "10m", "10s").Should(Succeed())

		By("asserting the correction took exactly ONE graceful failover (LR-021-style serialization)")
		moves := msCountEvents(reg, sc.ns, sc.crName, msRebalanceEventReason) - baselineEvents
		Expect(moves).To(Equal(1),
			"the rebalancer must move ONE master per reconcile and stop once the threshold is "+
				"reached: %d %s events for a one-move correction means it either burst (restarting "+
				"several masters in one wave) or thrashed past the target", moves, msRebalanceEventReason)

		By("asserting it then STOPS — no thrash once the distribution is satisfied")
		// The no-thrash half of the claim. Two settle cooldowns is long enough for a
		// rebalancer that had not converged to issue another move.
		settled := msCountEvents(reg, sc.ns, sc.crName, msRebalanceEventReason)
		Consistently(func() int {
			return msCountEvents(reg, sc.ns, sc.crName, msRebalanceEventReason)
		}, "90s", "15s").Should(Equal(settled),
			"a satisfied distribution must produce no further moves")

		By("asserting zero data loss across the graceful failover")
		for key, want := range anchors {
			Eventually(func() string {
				out, err := kctl(reg.site(seedSite).context, "-n", sc.ns, "exec", seedPod, "-c", "redis",
					"--", "redis-cli", "-c", "GET", key)
				if err != nil {
					return "ERR: " + err.Error()
				}
				return strings.TrimSpace(out)
			}, "2m", "5s").Should(Equal(want),
				"key %s lost across the rebalance: a graceful CLUSTER FAILOVER is the "+
					"no-data-loss primitive (LR-025), so any loss here is a defect, not a trade-off", key)
		}

		By("asserting the cluster is still healthy and cross-site placement is intact")
		msVerifyCrossSiteTopology(reg, sc.ns, sc.crName, seedSite, seedPod, true)
	}

	// --- G1: the reference 3-site topology; any single-site loss is native. -----------------
	Context("G1: F3-K3 drift 1+1+1 → 2+0+1 is corrected back to 1+1+1", func() {
		sc := msRebalanceScenario{
			ns: "ms-e2e-g1", crName: "ms-g1",
			sites:  []string{"scm-s1", "scm-s2", "scm-s3"},
			shards: 3, drainFrom: "scm-s1",
			wantAfter: "scm-s1=1,scm-s2=1,scm-s3=1",
			why: "K=3 over S=3: nativeSurvivalTarget(3) = ⌈3/2⌉−1 = 1 and S·target = 3 = K, so the " +
				"TARGET tier is reachable and every site must end at 1 — the only distribution " +
				"where a single-site loss is survived natively, with the hub uninvolved",
		}
		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + sc.ns + "/" + sc.crName + " in place")
				return
			}
			msTeardownInstance(reg, sc.ns, sc.crName)
		})
		It("restores the native-survival target with one graceful failover and no data loss", func() {
			runRebalance(sc)
		})
	})

	// --- G4: the D3-recommended dual-site, and the topology LR-031 switched the rebalancer ON for.
	Context("G4: F2-K4 drift 2+2 → 3+1 is corrected back to 2+2", func() {
		sc := msRebalanceScenario{
			ns: "ms-e2e-g4", crName: "ms-g4",
			sites:  []string{"scm-s1", "scm-s2"},
			shards: 4, drainFrom: "scm-s2",
			wantAfter: "scm-s1=2,scm-s2=2",
			why: "K=4 over S=2: nativeSurvivalTarget(4) = 1 is UNREACHABLE (S·target = 2 < 4 = K), " +
				"so the two-tier planner falls to takeoverSurvivalFloor(4) = ⌊4/2⌋ = 2 and must " +
				"restore the even 2+2 split. This is the exact case LR-031 fixed: the old single " +
				"bound judged the target unreachable and returned nil under the no-partial-churn " +
				"rule, so a 3+1 drift was NEVER corrected — and losing the 3-master site then hit " +
				"the drift-refusal instead of taking over",
		}
		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + sc.ns + "/" + sc.crName + " in place")
				return
			}
			msTeardownInstance(reg, sc.ns, sc.crName)
		})
		It("falls to the floor tier and restores the fence-safe even split", func() {
			runRebalance(sc)
		})
	})

	// --- G5: the former "headline trap", now fence-safe (LR-031). ---------------------------
	Context("G5: F3-K4 drift 2+1+1 → 3+1+0 is corrected back to 2+1+1", func() {
		sc := msRebalanceScenario{
			ns: "ms-e2e-g5", crName: "ms-g5",
			sites:  []string{"scm-s1", "scm-s2", "scm-s3"},
			shards: 4, drainFrom: "scm-s3",
			wantAfter: "scm-s1=2,scm-s2=1,scm-s3=1",
			why: "K=4 over S=3: target 1 is unreachable (S·target = 3 < 4), so the floor tier " +
				"(⌊4/2⌋ = 2) plans and 2+1+1 must be restored — the distribution LR-031 showed is " +
				"fence-safe (a 2-master site's loss is within the floor, so it takes over) and " +
				"which the pigeonhole makes the best K=4-over-3-sites can do. The recipient is " +
				"constrained to a site holding an in-sync replica of the moved shard (sps=2)",
		}
		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + sc.ns + "/" + sc.crName + " in place")
				return
			}
			msTeardownInstance(reg, sc.ns, sc.crName)
		})
		It("restores the fence-safe 2+1+1 with one graceful failover", func() {
			runRebalance(sc)
		})
	})

	// --- The no-thrash claim on its own (G2's cheap half). ----------------------------------
	Context("G2 (short form): a balanced cluster issues no failovers at all", func() {
		const (
			ns     = "ms-e2e-g2"
			crName = "ms-g2"
		)
		sites := []string{"scm-s1", "scm-s2", "scm-s3"}

		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + ns + "/" + crName + " in place")
				return
			}
			msTeardownInstance(reg, ns, crName)
		})

		// The matrix's G2 is a 30-minute idle soak (Tier 3). This is the same claim at a
		// length that fits Tier 2: the rebalancer must be inert on a cluster that is already
		// balanced. It is cheap and it guards the direction that would be worst in production —
		// an operator that gracefully fails masters over for no reason.
		It("issues zero rebalance moves while balanced", func() {
			setMSDebugTarget(reg, ns, crName)
			msSetupSites(reg, ns)
			msRecordInstance(reg, ns, crName)

			_, err := kctlIn(reg.hubContext, msClusterCR(reg, ns, crName, sites, 3, 0, 2, 15000), "apply", "-f", "-")
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				lr, err := msGetCR(reg, ns, crName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(string(lr.Status.Phase)).To(Equal(string(littleredv1alpha1.PhaseRunning)))
			}, "10m", "10s").Should(Succeed())

			msVerifyCrossSiteTopology(reg, ns, crName, sites[0], clusterMasterPod(crName, 0), true)
			Expect(msSortedCounts(reg, msMasterCounts(reg, ns, crName))).
				To(Equal("scm-s1=1,scm-s2=1,scm-s3=1"), "bringup must already be balanced for this cell to mean anything")

			By("holding idle for several settle cooldowns and asserting the rebalancer stays inert")
			start := time.Now()
			Consistently(func() int {
				return msCountEvents(reg, ns, crName, msRebalanceEventReason)
			}, "3m", "20s").Should(Equal(0),
				"a balanced cluster must produce NO rebalance moves; any move here is churn "+
					"against a healthy serving instance")
			By(fmt.Sprintf("stayed inert for %s", time.Since(start).Round(time.Second)))
		})
	})
})
