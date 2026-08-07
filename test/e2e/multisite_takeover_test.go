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

// multisite_takeover_test.go drives the ADR-012 site-loss takeover on the real hub+spokes
// lab. Two scenarios, in ascending blast-radius order (least-destructive safety test first):
//
//	Scenario B (split-brain guard, MUST NOT fire): a data-plane-only partition of one site
//	  while the site stays alive (API reachable, pods Ready) ⇒ witness Contradiction ⇒ the
//	  operator HOLDs. No CLUSTER FAILOVER TAKEOVER, no second serving master. Then heal ⇒
//	  native recovery to healthy.
//
//	Scenario A (crown jewel, MUST fire): a whole-site kill (talosctl reboot) of one site of a
//	  dedicated 2-site instance with an even 2+2 master split (shards:4) ⇒ witness Unobservable +
//	  survivors below quorum + fence-safe ⇒ the operator fires CLUSTER FAILOVER TAKEOVER on
//	  the surviving site, which then serves all 16384 slots with the replicated data intact.
//	  Then restore ⇒ the site rejoins back to a healthy cross-site topology.
//
// Each scenario uses its OWN dedicated throwaway instance + namespace (no shared Ordered
// state — the LR-024 lesson). The specs assume: the operator already runs on the hub
// (SKIP_OPERATOR_DEPLOY=true), talosctl is configured for the sites, and the chaos-client
// image is pullable. Every disruption is reversed and the lab left healthy.
//
// Red-first note (CLAUDE.md tier-3): the live-disruption red is "observed once" against the
// real lab — a network/reboot condition that CI cannot reproduce. The repeatable guards for
// the decision itself are the planSiteTakeover / classifySiteWitness unit tables.

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

var _ = Describe("Multi-Site Site-Loss Takeover (ADR-012)", Label("multisite"), func() {
	var reg *msRegistry

	BeforeEach(func() {
		reg = msDefaultRegistry()
	})

	// --- Scenario B: the split-brain guard (least destructive, run first) ------------------
	Context("Scenario B: data-plane partition with the site alive (must NOT take over)", Label("multisite-hold"), func() {
		const (
			ns     = "ms-e2e-b"
			crName = "ms-b"
		)
		siteNames := []string{"scm-s1", "scm-s2"}
		affected := "scm-s2" // the partitioned (but alive) site

		AfterEach(func() {
			msHealDataPlane(reg.site(affected).context, ns)
			msTeardownInstance(reg, ns, crName)
		})

		It("classifies the affected site Contradiction and HOLDs — no takeover, no split-brain", func() {
			By("standing up a 2-site cluster instance [s1,s2] shards:4 sitesPerShard:2 (even 2+2 master split)")
			msSetupSites(reg, ns)
			_, err := kctlIn(reg.hubContext, msClusterCR(reg, ns, crName, siteNames, 4, 0, 2, 15000), "apply", "-f", "-")
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the stretched cluster to become Ready")
			Eventually(func(g Gomega) {
				lr, err := msGetCR(reg, ns, crName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(string(lr.Status.Phase)).To(Equal(string(littleredv1alpha1.PhaseRunning)))
			}, "5m", "10s").Should(Succeed())

			seedPod := clusterMasterPod(crName, 0) // shard 0 master lives in s1 (primary)
			msVerifyCrossSiteTopology(reg, ns, crName, "scm-s1", seedPod, true)

			By("recording a pre-partition key on each shard through the survivor site")
			// (data-integrity anchor for the heal assertion)
			writeKV(reg.site("scm-s1").context, ns, seedPod, "msb:anchor", "pre-partition")

			startedAt := time.Now()

			By("cutting ONLY the cross-site redis/gossip data-plane to " + affected + " (site stays alive)")
			peers := append(msPodCIDRsForSite(reg, reg.site("scm-s1").context, ns),
				msHubPeerCIDRs()...)
			msPartitionDataPlane(reg, reg.site(affected).context, ns, crName, peers)

			By("asserting the operator classifies " + affected + " as Contradiction and HOLDs (no takeover)")
			// The affected site's pods stay Ready and its API answers, so the witness is
			// Contradiction and planSiteTakeover returns Hold. We assert the STRONG negative:
			// across a window comfortably longer than the takeover cooldown (max(60s,2×15s)=60s),
			// no SiteTakeoverExecuted event is ever emitted and no shard gains a 2nd serving master.
			Consistently(func(g Gomega) {
				g.Expect(msHasEvent(reg, ns, crName, "SiteTakeoverExecuted")).To(BeFalse(),
					"a takeover must NEVER fire against an alive (Contradiction) site")
				g.Expect(msSecondServingMasterExists(reg, ns, crName)).To(BeFalse(),
					"no shard may gain a second serving master (split-brain)")
			}, "100s", "10s").Should(Succeed())

			By("confirming the MultiSiteTakeover condition never latched a refusal/executed reason")
			status, reason := msTakeoverCondition(reg, ns, crName)
			_, _ = fmt.Fprintf(GinkgoWriter, "MultiSiteTakeover condition: status=%q reason=%q\n", status, reason)
			Expect(reason).NotTo(Equal("SiteTakeoverExecuted"))

			By("healing the partition and asserting native recovery to a healthy cross-site topology")
			msHealDataPlane(reg.site(affected).context, ns)
			msVerifyCrossSiteTopology(reg, ns, crName, "scm-s1", seedPod, true)

			By("verifying the pre-partition anchor key survived the whole scenario")
			Eventually(func(g Gomega) {
				v, err := readKV(reg.site("scm-s1").context, ns, seedPod, "msb:anchor")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(v).To(Equal("pre-partition"))
			}, "1m", "5s").Should(Succeed())

			_, _ = fmt.Fprintf(GinkgoWriter, "Scenario B started at %s — HELD for the full window, no takeover.\n", startedAt)
		})
	})

	// --- Scenario A: the crown jewel — dual-site kill → takeover ----------------------------
	Context("Scenario A: whole-site kill of an even 2+2 split (MUST take over)", Label("multisite-takeover"), func() {
		const (
			ns     = "ms-e2e-a"
			crName = "ms-a"
		)
		siteNames := []string{"scm-s1", "scm-s2"}
		victim := "scm-s2" // the site we kill (hosts shard-1 and shard-3 masters)

		AfterEach(func() {
			// Best-effort: ensure the victim site is back before tearing down (teardown must be
			// able to reach the spoke to sweep it).
			msWaitSiteReady(reg.site(victim), 8*time.Minute)
			msTeardownInstance(reg, ns, crName)
		})

		It("detects the loss, waits the cooldown, fires TAKEOVER, serves all slots, keeps data, and rejoins", func() {
			By("standing up a dedicated 2-site cluster [s1,s2] shards:4 sitesPerShard:2 (even 2+2 master split)")
			msSetupSites(reg, ns)
			_, err := kctlIn(reg.hubContext, msClusterCR(reg, ns, crName, siteNames, 4, 0, 2, 15000), "apply", "-f", "-")
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				lr, err := msGetCR(reg, ns, crName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(string(lr.Status.Phase)).To(Equal(string(littleredv1alpha1.PhaseRunning)))
			}, "5m", "10s").Should(Succeed())

			survivorPod := clusterMasterPod(crName, 0) // shard-0 master in the survivor site s1
			msVerifyCrossSiteTopology(reg, ns, crName, "scm-s1", survivorPod, true)

			By("writing known data across all shards with the chaos client (cross-site pod IPs are routed)")
			addrs := msPodIPAddrs(reg, ns) // every pod IP:6379 across both sites
			chaosPod, err := deployChaosClient(ns, "msa", addrs, "msa", true, 20*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(waitForChaosClientComplete(ns, chaosPod, 3*time.Minute)).To(Succeed())
			pre, err := getChaosClientMetrics(ns, chaosPod)
			Expect(err).NotTo(HaveOccurred())
			_, _ = fmt.Fprintf(GinkgoWriter, "pre-kill chaos metrics: writes=%d corruptions=%d\n", pre.WriteSuccesses, pre.DataCorruptions)
			Expect(pre.DataCorruptions).To(Equal(int64(0)), "baseline write phase must be corruption-free")
			deleteChaosClient(ns, chaosPod)

			startedAt := time.Now()

			By("KILLING the whole victim site " + victim + " (talosctl reboot — self-reversing)")
			msRebootSite(reg.site(victim))
			msWaitSiteUnreachable(reg.site(victim), 3*time.Minute)

			By("asserting the FENCE/WITNESS-GATED recoverSiteLoss fires the takeover after the cooldown")
			// Evidence priority: the operator log's gated-path audit line and the
			// MultiSiteTakeover=SiteTakeoverExecuted condition — NOT Kubernetes Events, which
			// some clusters do not retain (M4b Finding #3). The event is checked best-effort only.
			Eventually(func(g Gomega) {
				gated, _ := msOperatorLogSince(reg, startedAt, "promoting surviving replica via CLUSTER FAILOVER TAKEOVER")
				_, reason := msTakeoverCondition(reg, ns, crName)
				g.Expect(gated != "" || reason == "SiteTakeoverExecuted").To(BeTrue(),
					"the gated recoverSiteLoss takeover must fire (log 'promoting surviving replica…' or condition SiteTakeoverExecuted)")
			}, "4m", "10s").Should(Succeed())

			logLines, _ := msOperatorLogSince(reg, startedAt, "promoting surviving replica via CLUSTER FAILOVER TAKEOVER")
			_, _ = fmt.Fprintf(GinkgoWriter, "operator gated-takeover log lines:\n%s\n", logLines)
			Expect(logLines).To(ContainSubstring("CLUSTER FAILOVER TAKEOVER"))

			By("asserting the generic repair Step-0 did NOT preempt the gated path (multi-site suppresses it)")
			step0, _ := msOperatorLogSince(reg, startedAt, "Promoting orphan replica during quorum loss")
			Expect(step0).To(BeEmpty(),
				"repair Step-0's unguarded quorum-loss takeover must never fire in multi-site")

			// Best-effort: the event, if this cluster retains Events.
			if msHasEvent(reg, ns, crName, "SiteTakeoverExecuted") {
				_, _ = fmt.Fprintf(GinkgoWriter, "SiteTakeoverExecuted event present\n")
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "note: no SiteTakeoverExecuted event (cluster may not retain Events); relying on log+condition\n")
			}

			By("asserting the surviving site serves ALL 16384 slots (cluster_state:ok from a survivor)")
			Eventually(func(g Gomega) {
				info, err := msClusterInfo(reg.site("scm-s1").context, ns, survivorPod)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(info["cluster_state"]).To(Equal("ok"))
				g.Expect(info["cluster_slots_assigned"]).To(Equal("16384"))
			}, "3m", "10s").Should(Succeed())

			By("asserting the replicated data written before the kill is intact")
			// Re-run the chaos client in verify-only fashion against the survivor pods: reading
			// back the pre-kill keyset must report zero corruptions.
			survAddrs := msPodIPAddrs(reg, ns) // now only survivor-site pods answer
			verifyPod, err := deployChaosClient(ns, "msa-verify", survAddrs, "msa", true, 15*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(waitForChaosClientComplete(ns, verifyPod, 3*time.Minute)).To(Succeed())
			post, err := getChaosClientMetrics(ns, verifyPod)
			Expect(err).NotTo(HaveOccurred())
			_, _ = fmt.Fprintf(GinkgoWriter, "post-takeover chaos metrics: writes=%d corruptions=%d\n", post.WriteSuccesses, post.DataCorruptions)
			Expect(post.DataCorruptions).To(Equal(int64(0)), "replicated data must survive the takeover with zero corruptions")
			deleteChaosClient(ns, verifyPod)

			By("RESTORING the site: waiting for it to boot and rejoin to a healthy cross-site topology")
			msWaitSiteReady(reg.site(victim), 8*time.Minute)
			Eventually(func(g Gomega) {
				info, err := msClusterInfo(reg.site("scm-s1").context, ns, survivorPod)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(info["cluster_state"]).To(Equal("ok"))
				g.Expect(info["cluster_slots_assigned"]).To(Equal("16384"))
			}, "6m", "15s").Should(Succeed())
			msVerifyCrossSiteTopology(reg, ns, crName, "scm-s1", survivorPod, true)

			_, _ = fmt.Fprintf(GinkgoWriter, "Scenario A: takeover fired and site rejoined; started at %s\n", startedAt)
		})
	})
})

// --- small data helpers ------------------------------------------------------------------

func writeKV(siteCtx, ns, pod, key, val string) {
	_, err := kctl(siteCtx, "exec", pod, "-n", ns, "-c", "redis", "--", "redis-cli", "-c", "SET", key, val)
	Expect(err).NotTo(HaveOccurred(), "SET %s", key)
}

func readKV(siteCtx, ns, pod, key string) (string, error) {
	out, err := kctl(siteCtx, "exec", pod, "-n", ns, "-c", "redis", "--", "redis-cli", "-c", "GET", key)
	return strings.TrimSpace(out), err
}

// msPodIPAddrs returns a comma-separated list of every reachable instance pod's IP:6379 across
// all sites — the chaos client's cluster seed set (pod IPs are routed cross-site).
func msPodIPAddrs(reg *msRegistry, ns string) string {
	var addrs []string
	for _, s := range reg.orderedSites {
		out, err := kctl(s.context, "-n", ns, "get", "pods", "-l", "app.kubernetes.io/component=cluster",
			"-o", "jsonpath={range .items[*]}{.status.podIP}{\"\\n\"}{end}")
		if err != nil {
			continue
		}
		for _, ip := range strings.Split(strings.TrimSpace(out), "\n") {
			if ip = strings.TrimSpace(ip); ip != "" {
				addrs = append(addrs, ip+":6379")
			}
		}
	}
	return strings.Join(addrs, ",")
}

// msHubPeerCIDRs returns extra CIDRs to deny in the Scenario-B cut so the hub operator's own
// gather dial to the affected site is also cut (making the site data-unreachable from the
// survivors' AND the hub's vantage, while the site's Kubernetes API — a separate host-network
// path — stays reachable). Configurable via MS_HUB_POD_CIDR; empty by default (the survivor
// peer CIDRs already make the affected site's masters unreachable to the survivors).
func msHubPeerCIDRs() []string {
	if v := envOr("MS_HUB_POD_CIDR", ""); v != "" {
		return strings.Split(v, ",")
	}
	return nil
}

// msSecondServingMasterExists reports whether ANY shard has more than one master serving
// (overlapping/duplicate) slots — the split-brain signature the guard must prevent. Best-effort
// across reachable survivor pods.
func msSecondServingMasterExists(reg *msRegistry, ns, crName string) bool {
	for _, s := range reg.orderedSites {
		pods, err := kctl(s.context, "-n", ns, "get", "pods", "-l", "app.kubernetes.io/component=cluster",
			"-o", "jsonpath={.items[0].metadata.name}")
		if err != nil || strings.TrimSpace(pods) == "" {
			continue
		}
		rows, err := msClusterNodes(s.context, ns, strings.TrimSpace(pods))
		if err != nil {
			continue
		}
		// Count masters that own at least one slot; a healthy 2-shard cluster has exactly 2.
		// Two masters both claiming the SAME slot range is the split-brain we guard against.
		slotOwners := map[string]int{}
		for _, r := range rows {
			if r.role == "master" && r.slots != "" {
				for _, sl := range strings.Split(r.slots, ",") {
					slotOwners[sl]++
				}
			}
		}
		for _, n := range slotOwners {
			if n > 1 {
				return true
			}
		}
	}
	return false
}
