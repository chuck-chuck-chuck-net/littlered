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

// multisite_immutability_test.go is matrix cell F1 (docs/MULTI_SITE_E2E_TEST_MATRIX.md §MS-F):
// the multi-site topology is immutable for an instance's lifetime, enforced by four CRD CEL
// transition rules at ADMISSION (LR-032/LR-033). F1 is the cheapest cell in the matrix — pure
// API calls, no fault primitive, no outage — and it collapses the five topology-change cells
// MS-F used to carry, because they are now all the same thing: the API server rejects the edit.
//
// TWO assertions per case, and the second is the one that matters:
//
//  1. the Update is rejected, and rejected for the reason UNDER TEST (§2.1);
//  2. no per-site shard StatefulSet was created or deleted in ANY site — i.e. growth cannot
//     half-succeed. This is the actual LR-032 failure mode: with sitesPerShard unset the old
//     dynamic default made the desired set a strict superset, so the LR-028 orphan guard found
//     nothing to refuse and the operator went on to half-build an undesigned topology.
//
// WHY THREE FIXTURES, not the one the matrix first assumed. §2.1: "a fixture that is invalid for
// two reasons proves nothing about either." Three CRD bounds collide with the mutations —
// sites MinItems=2, sitesPerShard Minimum=2, and the CEL rule sitesPerShard <= sites.size() — so
// several of the obvious edits would be rejected by a BOUND rather than by immutability, and the
// cell would pass while testing nothing:
//
//	mutation            on F2-K4 (2 sites, sps 2)     ⇒ fixture actually needed
//	sitesPerShard ↑     2→3 also breaks sps<=|sites|  ⇒ F3-K3  (3 sites, sps 2 → 3)
//	sitesPerShard ↓     2→1 also breaks Minimum=2     ⇒ F3-K3-M (3 sites, sps 3 → 2)
//	remove a site       2→1 also breaks MinItems=2    ⇒ F3-K3  (3 sites → 2, sps 2 still ≤ 2)
//
// and site removal specifically may NOT use F3-K3-M, because dropping to 2 sites there would
// leave sps=3 > 2 and again reject for the bound. Each case below is therefore run on a fixture
// where immutability is the ONLY applicable rule, and asserts that rule's own message.
//
// This was MEASURED against a real API server (envtest), not reasoned about. Every case below
// produces exactly one rule violation; the two rejected alternatives produce several:
//
//	sps 2→3 on 2 sites   ⇒ [sitesPerShard cannot exceed the number of sites,
//	                        sitesPerShard is immutable]                        — 2 reasons
//	remove site on 2 sites ⇒ [sites should have at least 2 items,
//	                        sitesPerShard cannot exceed the number of sites,
//	                        sites is immutable]                                — 3 reasons
//
// Note what that implies: a ContainSubstring assertion would still PASS on both, because the
// immutability message is present among the others. §2.1 compliance is therefore invisible to
// the assertion and is enforced only by the choice of fixture — which is why the mapping above
// is spelled out rather than left to whoever next edits this file.
//
// READINESS IS NOT REQUIRED. A CEL transition rule fires on any Update regardless of what the
// data plane is doing, and assertion (2) only needs the StatefulSet objects to exist. So these
// specs wait for the per-site STSs to appear, never for Redis to form a cluster — which is what
// keeps F1 cheap enough to run anywhere. Fixtures are created and torn down one at a time, so
// the peak footprint is a single fixture rather than all three.
//
// Red-first (CLAUDE.md tier-3 / matrix "Teeth"): the repeatable guards for the rules themselves
// are the envtest transition specs that landed with LR-032/033 (9 specs, 7 observed red first).
// This cell's own teeth are a mutant: delete one XValidation line from the type, regenerate, and
// exactly the case naming that rule must go green-to-red. §10 records that as the F1 mutant.

import (
	"fmt"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The four CEL transition rules' distinguishing message fragments. Asserting on these rather
// than on "the update failed" is what makes each case prove its own rule (§2.1).
const (
	msImmutableSites     = "spec.multiSite.sites is immutable"
	msImmutableSPS       = "spec.multiSite.sitesPerShard is immutable"
	msImmutableShards    = "spec.cluster.shards is immutable while spec.multiSite is set"
	msImmutableMultiSite = "cannot be added to or removed from an existing instance"
)

// msSingleSiteClusterCR renders an ordinary single-site cluster CR — the "before" state for the
// one case that has to start WITHOUT a multiSite block (adding one is what must be rejected).
func msSingleSiteClusterCR(ns, name string, shards int) string {
	cpu := msEnvStr("MS_REDIS_CPU", "128m")
	mem := msEnvStr("MS_REDIS_MEMORY", "256Mi")
	return withRedisImage(fmt.Sprintf(`apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: cluster
  cluster:
    shards: %d
    replicasPerShard: 0
  resources:
    requests: {memory: %s, cpu: %s}
    limits: {memory: %s}
  config:
    maxmemoryPolicy: noeviction
`, name, ns, shards, mem, cpu, mem))
}

// msStripMultiSite removes the whole multiSite block from a rendered CR, producing the "remove
// the block" mutation. kubectl apply turns the absent field into a deletion patch, so the
// has(self.multiSite) == has(oldSelf.multiSite) rule is what rejects it.
func msStripMultiSite(cr string) string {
	lines := strings.Split(cr, "\n")
	out := make([]string, 0, len(lines))
	inBlock := false
	for _, ln := range lines {
		switch {
		case strings.HasPrefix(ln, "  multiSite:"):
			inBlock = true
			continue
		case inBlock && (strings.HasPrefix(ln, "    ") || strings.TrimSpace(ln) == ""):
			continue
		default:
			inBlock = false
		}
		out = append(out, ln)
	}
	return strings.Join(out, "\n")
}

// msSTSNamesIn lists the StatefulSet names in one cluster's instance namespace. The namespace is
// dedicated to the instance, so a bare list is both sufficient and immune to label drift.
func msSTSNamesIn(context, ns string) []string {
	raw, err := kctl(context, "-n", ns, "get", "statefulsets",
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	Expect(err).NotTo(HaveOccurred(), "listing StatefulSets in %s", context)
	names := []string{}
	for _, ln := range strings.Split(strings.TrimSpace(raw), "\n") {
		if ln = strings.TrimSpace(ln); ln != "" {
			names = append(names, ln)
		}
	}
	sort.Strings(names)
	return names
}

// msShardSTSNames snapshots the StatefulSet names in the instance namespace of every SITE, so a
// rejected edit can be shown to have changed nothing anywhere. Deliberately site-only: a
// single-site instance's workloads live on the HUB, which is a different cluster and is read
// with msSTSNamesIn(reg.hubContext, ...) instead.
func msShardSTSNames(reg *msRegistry, ns string) map[string][]string {
	out := map[string][]string{}
	for _, s := range reg.orderedSites {
		out[s.name] = msSTSNamesIn(s.context, ns)
	}
	return out
}

// msWaitShardSTSs waits until `want` StatefulSets exist across all sites combined. F1 needs the
// objects, not a healthy cluster, so this is deliberately not a readiness wait.
func msWaitShardSTSs(reg *msRegistry, ns string, want int) {
	Eventually(func() int {
		n := 0
		for _, names := range msShardSTSNames(reg, ns) {
			n += len(names)
		}
		return n
	}, "4m", "5s").Should(Equal(want),
		"the operator must create every per-site shard StatefulSet before the immutability "+
			"attempts, otherwise 'nothing was created or deleted' is vacuously true")
}

// msRejectedUpdate applies a mutated CR and asserts the API server refused it FOR THE REASON
// UNDER TEST. A bare "it failed" would pass for a CR that is invalid on some unrelated bound,
// which §2.1 exists to prevent.
func msRejectedUpdate(reg *msRegistry, mutated, wantMsg string) {
	out, err := kctlIn(reg.hubContext, mutated, "apply", "-f", "-")
	Expect(err).To(HaveOccurred(),
		"the API server must REJECT this edit; it was accepted, which means the topology is "+
			"mutable again and shard placement can be silently re-mapped. Output: %s", out)
	Expect(out).To(ContainSubstring(wantMsg),
		"rejected, but not for the reason under test — this case proves nothing about its own "+
			"rule (matrix §2.1). Wanted %q in: %s", wantMsg, out)
}

var _ = Describe("Multi-Site Topology Immutability (F1)", Label("multisite"), Label("multisite-immutability"), func() {
	var reg *msRegistry

	BeforeEach(func() {
		reg = msRegistryFromEnv()
	})

	// One mutation attempt: a name, the mutated CR, and the rule that must reject it.
	type mutation struct {
		what    string
		cr      string
		wantMsg string
	}

	// runFixture stands a fixture up, waits for its StatefulSets, runs every mutation against it,
	// and asserts the per-site StatefulSet set is untouched throughout.
	runFixture := func(ns, crName string, sites []string, shards, rps, sps int, mutations func() []mutation) {
		setMSDebugTarget(reg, ns, crName)
		msSetupSites(reg, ns)
		msRecordInstance(reg, ns, crName)

		By(fmt.Sprintf("standing up %d site(s) %v shards:%d sitesPerShard:%d", len(sites), sites, shards, sps))
		_, err := kctlIn(reg.hubContext, msClusterCR(reg, ns, crName, sites, shards, rps, sps, 15000), "apply", "-f", "-")
		Expect(err).NotTo(HaveOccurred())

		// Per-site shard STSs = one per (hosting site, shard) = shards × sitesPerShard.
		msWaitShardSTSs(reg, ns, shards*sps)
		before := msShardSTSNames(reg, ns)

		for _, m := range mutations() {
			By("rejecting: " + m.what)
			msRejectedUpdate(reg, m.cr, m.wantMsg)
		}

		// The assertion that matters: an attempted growth must not have half-succeeded anywhere.
		// Checked once after all attempts and given a moment for any (erroneous) reconcile to act
		// — an immediate read could pass simply by outrunning the operator.
		time.Sleep(10 * time.Second)
		Expect(msShardSTSNames(reg, ns)).To(Equal(before),
			"a REJECTED topology edit must leave every site's StatefulSets exactly as they were; "+
				"a difference means the edit half-landed, which is the LR-032 failure mode")
	}

	// --- F2-K4: the recommended dual-site. Shards both ways, site addition, reorder, block removal.
	Context("F2-K4 (2 sites, shards 4, sitesPerShard 2)", func() {
		const (
			ns     = "ms-e2e-f1-k4"
			crName = "ms-f1-k4"
		)
		sites := []string{"scm-s1", "scm-s2"}

		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + ns + "/" + crName + " in place")
				return
			}
			msTeardownInstance(reg, ns, crName)
		})

		It("rejects shard scale-out and scale-in, site addition, site reordering, and removing the multiSite block", func() {
			runFixture(ns, crName, sites, 4, 0, 2, func() []mutation {
				return []mutation{
					{
						// 4→5. Above the Minimum=3 bound, so immutability is the only rule that applies.
						what:    "shards 4 → 5 (scale-out)",
						cr:      msClusterCR(reg, ns, crName, sites, 5, 0, 2, 15000),
						wantMsg: msImmutableShards,
					},
					{
						// 4→3 is still ≥ Minimum=3: again a single reason. The LR-028 runtime guard
						// would also refuse this, but it must never be REACHED — admission is first.
						what:    "shards 4 → 3 (scale-in)",
						cr:      msClusterCR(reg, ns, crName, sites, 3, 0, 2, 15000),
						wantMsg: msImmutableShards,
					},
					{
						// sps=2 ≤ 3 sites, so the bound is satisfied and only immutability rejects.
						// This is the case LR-032 called out as fail-open before the CEL rules.
						what:    "adding a third site",
						cr:      msClusterCR(reg, ns, crName, append(append([]string{}, sites...), "scm-s3"), 4, 0, 2, 15000),
						wantMsg: msImmutableSites,
					},
					{
						// The case that only exists because listType went map→atomic (LR-033):
						// under listType=map, CEL list equality is order-blind and this reorder
						// would have been ACCEPTED while silently re-placing every shard.
						what:    "reordering the site list (same names)",
						cr:      msClusterCR(reg, ns, crName, []string{sites[1], sites[0]}, 4, 0, 2, 15000),
						wantMsg: msImmutableSites,
					},
					{
						what:    "removing the multiSite block (stretched → single-site)",
						cr:      msStripMultiSite(msClusterCR(reg, ns, crName, sites, 4, 0, 2, 15000)),
						wantMsg: msImmutableMultiSite,
					},
				}
			})
		})
	})

	// --- F3-K3: 3 sites at sps=2 — the only fixture where sps↑ and site REMOVAL each have exactly
	// one applicable rule.
	Context("F3-K3 (3 sites, shards 3, sitesPerShard 2)", func() {
		const (
			ns     = "ms-e2e-f1-k3"
			crName = "ms-f1-k3"
		)
		sites := []string{"scm-s1", "scm-s2", "scm-s3"}

		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + ns + "/" + crName + " in place")
				return
			}
			msTeardownInstance(reg, ns, crName)
		})

		It("rejects raising sitesPerShard and removing a site", func() {
			runFixture(ns, crName, sites, 3, 0, 2, func() []mutation {
				return []mutation{
					{
						// 2→3 with 3 sites satisfies sitesPerShard <= sites.size(), so the ONLY
						// reason left is immutability. On a 2-site fixture this same edit would
						// also break the bound and prove nothing.
						what:    "sitesPerShard 2 → 3 (raising the copy factor)",
						cr:      msClusterCR(reg, ns, crName, sites, 3, 0, 3, 15000),
						wantMsg: msImmutableSPS,
					},
					{
						// 3→2 sites keeps MinItems=2 and keeps sps=2 ≤ 2. Single reason.
						what:    "removing a site",
						cr:      msClusterCR(reg, ns, crName, sites[:2], 3, 0, 2, 15000),
						wantMsg: msImmutableSites,
					},
				}
			})
		})
	})

	// --- F3-K3-M: full mesh (sps = S). The only fixture where sitesPerShard can be LOWERED
	// without tripping Minimum=2.
	Context("F3-K3-M (3 sites, shards 3, sitesPerShard 3 — full mesh)", func() {
		const (
			ns     = "ms-e2e-f1-mesh"
			crName = "ms-f1-mesh"
		)
		sites := []string{"scm-s1", "scm-s2", "scm-s3"}

		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + ns + "/" + crName + " in place")
				return
			}
			msTeardownInstance(reg, ns, crName)
		})

		It("rejects lowering sitesPerShard", func() {
			runFixture(ns, crName, sites, 3, 0, 3, func() []mutation {
				return []mutation{
					{
						// 3→2 stays ≥ Minimum=2, so immutability is the only applicable rule.
						// Lowering is the direction that would ORPHAN per-site StatefulSets (the
						// LR-028 subject), which is why it gets its own case rather than being
						// assumed symmetric with raising it.
						what:    "sitesPerShard 3 → 2 (lowering the copy factor)",
						cr:      msClusterCR(reg, ns, crName, sites, 3, 0, 2, 15000),
						wantMsg: msImmutableSPS,
					},
				}
			})
		})
	})

	// --- The converse direction: a single-site instance must not gain a multiSite block.
	Context("single-site cluster", func() {
		const (
			ns     = "ms-e2e-f1-single"
			crName = "ms-f1-single"
		)
		sites := []string{"scm-s1", "scm-s2"}

		AfterEach(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("DEBUG_ON_FAILURE: leaving " + ns + "/" + crName + " in place")
				return
			}
			msTeardownInstance(reg, ns, crName)
		})

		It("rejects adding a multiSite block to a running single-site instance", func() {
			setMSDebugTarget(reg, ns, crName)
			// The site Secrets must exist even though this instance does not use them: the
			// mutated CR references them, and a dangling secretRef would be a second reason for
			// the CR to be wrong (§2.1) — though not, as it happens, one the API server checks.
			msSetupSites(reg, ns)
			msRecordInstance(reg, ns, crName)

			By("standing up an ordinary single-site cluster on the hub")
			_, err := kctlIn(reg.hubContext, msSingleSiteClusterCR(ns, crName, 3), "apply", "-f", "-")
			Expect(err).NotTo(HaveOccurred())

			By("waiting for its shard StatefulSets on the HUB (a single-site instance is hub-local)")
			Eventually(func() int {
				return len(msSTSNamesIn(reg.hubContext, ns))
			}, "4m", "5s").Should(Equal(3),
				"the single-site instance must have built before the conversion is attempted")

			hubBefore := msSTSNamesIn(reg.hubContext, ns)
			By("rejecting: adding a multiSite block (single-site → stretched)")
			msRejectedUpdate(reg, msClusterCR(reg, ns, crName, sites, 3, 0, 2, 15000), msImmutableMultiSite)

			time.Sleep(10 * time.Second)
			Expect(msSTSNamesIn(reg.hubContext, ns)).To(Equal(hubBefore),
				"a rejected conversion must not have disturbed the hub-local StatefulSets")
			// The conversion named two sites; neither may have gained anything. This is the
			// half-succeeded-growth assertion for the single-site → stretched direction.
			for site, names := range msShardSTSNames(reg, ns) {
				Expect(names).To(BeEmpty(),
					"site %s must hold no StatefulSets: the conversion that would have created "+
						"them was rejected", site)
			}
		})
	})
})
