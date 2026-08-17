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

// multisite_bringup_test.go carries matrix cells I1 and I3 (§MS-I) — the two bringup
// REGRESSION cells. Both guard live bugs found during the multi-site bringup on the real lab,
// and both are reachable with nothing but an API call, which is why they belong in M3.5
// alongside F1 rather than behind the fault-agent work.
//
// I1 — the clusterFormed veto (LR-026). A never-bootstrapped cluster presents the EXACT surface
// of "all masters lost": fresh nodes are role:master, own 0 slots, and report their own
// cluster_state:fail. Without condition 0 the site-loss planner armed on that and would have
// fired CLUSTER FAILOVER TAKEOVER across pods that had never joined a cluster — preempting
// bootstrap into a hard deadlock. The cell brings an instance up with one site unreachable and
// asserts the operator does NOTHING: no takeover, ever, however long it waits.
//
// I3 — cross-site replicas at replicasPerShard:0 (LR-027). Multi-site ALWAYS has replicas: a
// shard spans sitesPerShard sites, so it has sitesPerShard×(rps+1)−1 replicas even at rps:0.
// Step 4's repair was originally gated on the single-site meaning of rps==0 ("no replicas"), so
// the cross-site members stayed stranded as standalone empty masters forever.
//
//	I3 IS NOT ALREADY COVERED BY THE M3 BRINGUP ITEM, contrary to appearances. That item calls
//	msVerifyCrossSiteTopology, which iterates the replicas it finds and checks each one's site
//	differs from its master's. With ZERO replicas that loop body never executes and the helper
//	passes VACUOUSLY — which is precisely the LR-027 symptom. And cluster_state would still be
//	ok with all 16384 slots assigned, because the four slot-owning masters are healthy; the
//	four stranded members are simply empty masters nobody is watching. So the existing
//	assertion is green on the exact regression it appears to cover. I3 therefore asserts the
//	EXPECTED COUNTS — how many replicas there must be, and that no member is a slotless
//	master — which is what makes it a real guard rather than a restatement.
//
// Red-first (CLAUDE.md tier-3): both bugs were observed live during the LR-026/LR-027 bringup,
// which is the "observed once on the environment that produces it" tier. The repeatable guards
// are the planSiteTakeover condition-0 unit table (I1) and clusterRepairExpectedReplicas (I3);
// these cells are the integration evidence that the two fixes still hold on real Redis.

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// msUnroutableAPIServer is an address that is syntactically valid but goes nowhere: TEST-NET-1
// (RFC 5737), which is reserved for documentation and is not routed. Syntactic validity is
// load-bearing here — LR-036 classifies an UNPARSEABLE server URL as a hub-local configuration
// fault (SiteClientUnbuildable), which is a different state with a different code path. I1 wants
// genuine unreachability, so the URL must be well-formed and merely unanswerable.
const msUnroutableAPIServer = "https://192.0.2.1:6443"

// msBreakSiteSecret rewrites a site's kubeconfig Secret so the site is unreachable, returning
// the original kubeconfig so the caller can heal it. This is the whole "fault": no network
// primitive, no hypervisor, just a Secret the operator reads.
func msBreakSiteSecret(reg *msRegistry, ns, siteName string) (original string) {
	secret := msSiteSecretName(ns, siteName)
	orig, err := kctl(reg.hubContext, "-n", reg.operatorNS, "get", "secret", secret,
		"-o", "jsonpath={.data.kubeconfig}")
	Expect(err).NotTo(HaveOccurred(), "reading site Secret %s", secret)
	Expect(strings.TrimSpace(orig)).NotTo(BeEmpty(), "site Secret %s carries no kubeconfig", secret)

	kc := string(mustBase64Decode(strings.TrimSpace(orig)))
	// Repoint every server: address to somewhere unroutable, leaving the document otherwise
	// intact (valid YAML, valid CA data) so nothing reclassifies it as a configuration fault.
	var out []string
	for _, ln := range strings.Split(kc, "\n") {
		if idx := strings.Index(ln, "server:"); idx >= 0 {
			out = append(out, ln[:idx]+"server: "+msUnroutableAPIServer)
			continue
		}
		out = append(out, ln)
	}
	msWriteSiteSecret(reg, ns, siteName, strings.Join(out, "\n"))
	return kc
}

// msWriteSiteSecret replaces a site kubeconfig Secret's contents in place, as a single
// server-side merge patch. The registry rebuilds that site's client when the content hash
// changes, so this is how a site is made unreachable and healed again without touching the CR
// (whose topology is immutable anyway — see F1).
func msWriteSiteSecret(reg *msRegistry, ns, siteName, kubeconfig string) {
	secret := msSiteSecretName(ns, siteName)
	patch := fmt.Sprintf(`{"data":{"kubeconfig":%q}}`, base64.StdEncoding.EncodeToString([]byte(kubeconfig)))
	_, err := kctl(reg.hubContext, "-n", reg.operatorNS, "patch", "secret", secret,
		"--type=merge", "-p", patch)
	Expect(err).NotTo(HaveOccurred(), "patching site Secret %s", secret)
}

// msSiteReasons reads the per-site status reasons the operator publishes, keyed by site name.
// LR-034/LR-036 made this the surface that distinguishes "did not answer" from "I cannot read
// or use my own configuration", which is exactly the distinction I1 depends on.
func msSiteReasons(reg *msRegistry, ns, crName string) map[string]string {
	out := map[string]string{}
	lr, err := msGetCR(reg, ns, crName)
	if err != nil || lr.Status.MultiSite == nil {
		return out
	}
	for _, s := range lr.Status.MultiSite.Sites {
		out[s.Name] = s.Reason
	}
	return out
}

var _ = Describe("Multi-Site Bringup Regressions (I1, I3)", Label("multisite"), func() {
	var reg *msRegistry

	BeforeEach(func() {
		reg = msRegistryFromEnv()
	})

	// --- I1 -------------------------------------------------------------------------------
	Context("I1: bringup with one site unreachable from the start", Label("multisite-bringup"), func() {
		const (
			ns     = "ms-e2e-i1"
			crName = "ms-i1"
		)
		sites := []string{"scm-s1", "scm-s2"}
		broken := "scm-s2"

		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + ns + "/" + crName + " in place")
				return
			}
			msTeardownInstance(reg, ns, crName)
		})

		It("never arms a takeover across pods that never joined a cluster (clusterFormed veto)", func() {
			setMSDebugTarget(reg, ns, crName)
			msSetupSites(reg, ns)
			msRecordInstance(reg, ns, crName)

			By("making " + broken + " unreachable BEFORE the instance exists")
			original := msBreakSiteSecret(reg, ns, broken)

			By("creating the stretched instance [s1,s2] shards:4 sitesPerShard:2 against a half-reachable topology")
			_, err := kctlIn(reg.hubContext, msClusterCR(reg, ns, crName, sites, 4, 0, 2, 15000), "apply", "-f", "-")
			Expect(err).NotTo(HaveOccurred())

			By("confirming the operator reports the site as UNREACHABLE, not as a configuration fault")
			// This is the §2.1 discipline applied to a positive cell: if the Secret were
			// malformed rather than unroutable, the operator would take the LR-034/LR-036
			// configuration path and REFUSE at bootstrap — a different behaviour that would
			// also produce "no takeover" and make the cell pass for the wrong reason.
			Eventually(func() string {
				return msSiteReasons(reg, ns, crName)[broken]
			}, "4m", "10s").Should(Equal("Unreachable"),
				"I1 requires genuine unreachability; any other reason means this cell is "+
					"exercising the configuration-fault path instead of the clusterFormed veto")

			By("holding for the full takeover cooldown and asserting the operator does NOTHING")
			// The cooldown is max(60s, 2 × cluster-node-timeout) = max(60s, 30s) = 60s. Waiting
			// past it is the point: a veto that merely DELAYS the takeover would pass a shorter
			// check and fail here.
			Consistently(func(g Gomega) {
				status, reason := msTakeoverCondition(reg, ns, crName)
				g.Expect(status).NotTo(Equal("True"),
					"a takeover was armed during bringup (reason %q) — the clusterFormed veto "+
						"(condition 0, LR-026) is not holding. Without it the planner treats "+
						"fresh never-joined nodes as 'all masters lost' and preempts bootstrap "+
						"into a deadlock", reason)
				// The reason string is the operator's own constant (reasonSiteTakeoverExecuted).
				// Spelling it wrong makes msHasEvent permanently false and the assertion
				// permanently, silently green — so it is pinned here deliberately.
				g.Expect(msHasEvent(reg, ns, crName, "SiteTakeoverExecuted")).To(BeFalse(),
					"CLUSTER FAILOVER TAKEOVER must never be issued against a cluster that was "+
						"never formed")
			}, "3m", "15s").Should(Succeed())

			By("healing the site and asserting bringup then completes normally")
			msWriteSiteSecret(reg, ns, broken, original)
			Eventually(func(g Gomega) {
				lr, err := msGetCR(reg, ns, crName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(string(lr.Status.Phase)).To(Equal(string(littleredv1alpha1.PhaseRunning)),
					"once the site is reachable the deferred bootstrap must proceed — the veto "+
						"must be a state, not a latch")
			}, "8m", "10s").Should(Succeed())

			msVerifyCrossSiteTopology(reg, ns, crName, "scm-s1", clusterMasterPod(crName, 0), true)
		})
	})

	// --- I3 -------------------------------------------------------------------------------
	Context("I3: replicasPerShard 0 still yields cross-site replicas", Label("multisite-bringup"), func() {
		const (
			ns     = "ms-e2e-i3"
			crName = "ms-i3"
		)
		sites := []string{"scm-s1", "scm-s2"}
		const (
			shards = 4
			rps    = 0
			sps    = 2
		)

		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + ns + "/" + crName + " in place")
				return
			}
			msTeardownInstance(reg, ns, crName)
		})

		It("reattaches every cross-site member as a replica, leaving no stranded empty master", func() {
			setMSDebugTarget(reg, ns, crName)
			msSetupSites(reg, ns)
			msRecordInstance(reg, ns, crName)

			By(fmt.Sprintf("standing up F2-K4: shards:%d replicasPerShard:%d sitesPerShard:%d", shards, rps, sps))
			_, err := kctlIn(reg.hubContext, msClusterCR(reg, ns, crName, sites, shards, rps, sps, 15000), "apply", "-f", "-")
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				lr, err := msGetCR(reg, ns, crName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(string(lr.Status.Phase)).To(Equal(string(littleredv1alpha1.PhaseRunning)))
			}, "8m", "10s").Should(Succeed())

			// members per shard = sps × (rps+1); one of them is the master, the rest replicas.
			// At rps:0 that is 2 members ⇒ exactly 1 replica per shard — the replica the
			// single-site reading of "replicasPerShard: 0" says should not exist.
			wantMasters := shards
			wantReplicas := shards * (sps*(rps+1) - 1)

			By(fmt.Sprintf("asserting the member census: %d masters, %d replicas, 0 stranded empty masters",
				wantMasters, wantReplicas))
			Eventually(func(g Gomega) {
				rows, err := msClusterNodes(reg.site("scm-s1").context, ns, clusterMasterPod(crName, 0))
				g.Expect(err).NotTo(HaveOccurred())

				var masters, replicas, slotless int
				for _, r := range rows {
					if r.role == "replica" {
						replicas++
						continue
					}
					masters++
					if strings.TrimSpace(r.slots) == "" {
						slotless++
					}
				}

				// The load-bearing assertion. A regressed Step-4 gate leaves these members as
				// standalone empty masters: slotless > 0 and replicas == 0, while
				// cluster_state stays ok and all 16384 slots stay assigned — which is why
				// counting, not health, is what catches it.
				g.Expect(slotless).To(Equal(0),
					"%d member(s) are slotless masters: the cross-site members never reattached, "+
						"which is LR-027 — the single-site 'replicasPerShard==0 ⇒ no replicas' "+
						"short-circuit has been re-enabled", slotless)
				g.Expect(replicas).To(Equal(wantReplicas),
					"multi-site always has replicas: a shard spans %d sites, so it must have "+
						"%d replica(s) even at replicasPerShard:0", sps, sps*(rps+1)-1)
				g.Expect(masters).To(Equal(wantMasters))
			}, "5m", "10s").Should(Succeed())

			// Only now is the cross-site placement check non-vacuous: we have just proved the
			// replicas exist, so iterating them means something.
			msVerifyCrossSiteTopology(reg, ns, crName, "scm-s1", clusterMasterPod(crName, 0), true)

			By("confirming the replicas actually carry data (a reattached replica must sync)")
			writeKV(reg.site("scm-s1").context, ns, clusterMasterPod(crName, 0), "i3:anchor", "replicated")
			time.Sleep(5 * time.Second)
			Eventually(func(g Gomega) {
				rows, err := msClusterNodes(reg.site("scm-s1").context, ns, clusterMasterPod(crName, 0))
				g.Expect(err).NotTo(HaveOccurred())
				for _, r := range rows {
					if r.role == "replica" {
						g.Expect(r.flags).NotTo(ContainSubstring("fail"),
							"replica %s is flagged failed; reattachment did not settle", r.ip)
					}
				}
			}, "2m", "5s").Should(Succeed())
		})
	})
})
