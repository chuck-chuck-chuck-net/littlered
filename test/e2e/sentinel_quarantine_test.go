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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/littlered-operator/littlered-operator/test/utils"
)

// Forsaken-gated quarantine, end to end (LR-042 verdict + LR-044 quarantine).
//
// These are the FIRST specs that make the operator execute either of those two
// decisions. LR-042 shipped with no e2e coverage at all, and the LR-039 isolation
// tier — the only other place a capture is staged — deliberately PAUSES the operator
// (`scaleOperator(0)`), because it measures what Sentinel does with an injected hello
// and the operator's LR-008 correction heals a 1-of-3 injection sub-second (LR-041).
// That pause is exactly why the verdict had no coverage. Here the operator runs
// throughout: it is the subject, not a confound.
//
// The staging recipe (proven live in LR-044 milestone 4a on t3e):
//
//   - Two instances in one namespace SHARING a master name. That shared name is the
//     whole mechanism: sentinelProcessHelloMessage() looks the advertised name up and
//     discards anything it does not know, so the name is Sentinel's only isolation
//     boundary (LR-039).
//   - A hello PUBLISHed at the victim's sentinel port, advertising the CAPTOR's LIVE
//     master at a DERIVED config epoch. Live, not TEST-NET: an unroutable address goes
//     s_down after down-after-milliseconds and then reads as ordinary dead debris,
//     which planForsaken clause 3 correctly refuses to call a capture. Derived, not
//     hardcoded: Sentinel acts only on a STRICTLY greater epoch, so a stale constant
//     silently makes the injection a no-op.
//   - Injected into ALL THREE of the victim's Sentinels. planForsaken clause 2 requires
//     unanimity among reachable monitoring Sentinels, so a 1-of-3 injection reads as a
//     transition, not a verdict — and it is also the shape the operator heals
//     sub-second, because two Sentinels still holding the right master give the LR-008
//     correction a living consensus master to aim at. With all three captured there is
//     no consensus master, RealMasterIP is "" (LR-004), and nothing can race the
//     verdict.
//   - Same Redis image on both instances (neither CR pins one, so both take the
//     operator default). Matching RDB versions mean the victim's sync SUCCEEDS, which
//     reproduces the SILENT capture — the common case, and the one where every victim
//     pod holds the captor's keyspace. That is precisely the shape LR-044's data
//     clauses were written for.
//
// Timeouts here are deliberately generous, and the reason is LR-045: a forsaken
// instance is now polled at the STEADY interval (30s), so the arming edge and the
// release edge each carry up to one steady interval MORE latency than milestone 4a
// measured (it ran on the pre-LR-045 build, where the poll was still fast). M4a's own
// figures — capture→verdict ~30s, verdict→armed 31s, armed→release 120-122s,
// release→serving ~38s, capture→serving ~3m55s — are therefore lower bounds, not
// budgets. Every Eventually below carries roughly one extra steady interval per edge
// on top of them.
//
// NOT COVERED HERE — quarantineHoldDataUnknown / reason QuarantineRefusedDataUnknown.
// That state needs a victim Redis pod that is READY per the kubelet while being
// UNREACHABLE from the operator, which is the whole point of the clause: LR-023
// established kubelet readiness as the blackhole-proof data-safety signal, and LR-017
// showed the operator's own dial is not, so only Ready-but-unreachable may block the
// quarantine. Staging that combination requires traffic shaping — the kubelet's local
// exec probe must keep passing while operator→pod traffic is dropped — i.e. an e2e
// harness capability (NetworkPolicy enforcement or equivalent) that does not exist in
// this suite. Deferred to the `feat/e2e-harness` branch rather than pre-built here;
// the decision matrix itself stays covered by TestQuarantineDataRisk /
// TestPlanQuarantine.
//
// ============================ DELIBERATELY AUTH-FREE ==========================
//
// Every other sentinel-mode fixture in this suite defaults to auth-ON
// (auth_utils_test.go). All THREE tiers here must stay auth-free, and a future
// sweep of "the last few stragglers" must leave them alone. Three independent
// reasons, any one of which is sufficient:
//
//  1. AUTH IS ONE OF THE CONDITIONS THAT PREVENTS A CAPTURE. Every tier stages a
//     real one by PUBLISHing a hello at the victim's sentinel port; with
//     `requirepass` set that connection answers NOAUTH before the payload reaches
//     sentinelProcessHelloMessage(), so the capture never lands and every tier
//     below silently degrades into asserting a non-event.
//  2. The Latched tier is deterministic ONLY because the configuration is
//     dangerous: quarantineConfigDangerous is `auth disabled` AND the legacy
//     shared master name, which sets the attempt budget to 1. Enabling auth moves
//     the budget to 2 and that tier stops being reachable at all.
//  3. The HoldDataPresent tier stages its permanently-failing sync with a bogus
//     `masterauth` against a foreign master that has NO password ("Client sent
//     AUTH, but no password is set"). Give the foreign master a real password and
//     the mechanism changes underneath the tier.
//
// ==============================================================================
var _ = Describe("Sentinel Forsaken-Gated Quarantine", Label("sentinel"), func() {

	// --- helpers -------------------------------------------------------------
	//
	// The capture machinery below is DUPLICATED from the "Sentinel Cross-Instance
	// Isolation" Describe rather than lifted to file scope. That tier was just
	// repaired after going red on a frozen-status bug and carries load-bearing
	// warning comments about why it pauses the operator and why its assertions must
	// avoid status; hoisting its closures would mix a mechanical refactor of a
	// freshly-fixed fixture into this change. These are three short readers and one
	// exec wrapper, and this file needs them against ALL THREE sentinels rather than
	// just sentinel-0, so the duplicates are not even quite the same shape.

	// sentinelCmd runs redis-cli against one named sentinel pod's sentinel port.
	sentinelCmd := func(pod string, args ...string) (string, error) {
		full := append([]string{"exec", pod, "-n", testNamespace,
			"-c", "sentinel", "--", "redis-cli", "-p", "26379"}, args...)
		return utils.Run(exec.Command("kubectl", full...))
	}

	// sentinelField reads a value out of redis-cli's flat key/value output.
	sentinelField := func(out, key string) string {
		lines := strings.Split(strings.TrimSpace(out), "\n")
		for i, l := range lines {
			if strings.TrimSpace(l) == key && i+1 < len(lines) {
				return strings.TrimSpace(lines[i+1])
			}
		}
		return ""
	}

	// nextEpoch returns a config epoch comfortably above the one the target Sentinel
	// currently holds. Sentinel acts on an injected hello only when
	// master_config_epoch is STRICTLY greater than the master's own config epoch, so a
	// hardcoded constant is a trap: once real activity pushes the epoch past it, the
	// injection silently becomes a no-op and the spec fails as "did not land" with no
	// hint that the payload, not the code, is stale.
	nextEpoch := func(mastersOut string) uint64 {
		cur, _ := strconv.ParseUint(sentinelField(mastersOut, "config-epoch"), 10, 64)
		return cur + 1000
	}

	sentinelPods := func(crName string) []string {
		return []string{crName + "-sentinel-0", crName + "-sentinel-1", crName + "-sentinel-2"}
	}
	redisPods := func(crName string) []string {
		return []string{crName + "-redis-0", crName + "-redis-1", crName + "-redis-2"}
	}

	// stsSpecReplicas reads a StatefulSet's DESIRED replica count. Spec, not status:
	// the quarantine's claim is that zero is the desired state at build time (SSA with
	// ForceOwnership makes the builder authoritative every pass), so the assertion has
	// to be on what the operator asked for, not on how far the kubelet got.
	stsSpecReplicas := func(sts string) string {
		out, err := utils.Run(exec.Command("kubectl", "get", "statefulset", sts,
			"-n", testNamespace, "-o", "jsonpath={.spec.replicas}"))
		if err != nil {
			return "err:" + strings.TrimSpace(out)
		}
		return strings.TrimSpace(out)
	}

	quarantinedSince := func(crName string) string {
		out, _ := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
			"-n", testNamespace, "-o", "jsonpath={.status.quarantinedSince}"))
		return strings.TrimSpace(out)
	}

	quarantineAttempts := func(crName string) string {
		out, _ := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
			"-n", testNamespace, "-o", "jsonpath={.status.quarantineAttempts}"))
		return strings.TrimSpace(out)
	}

	forsakenReason := func(crName string) string {
		r, _ := getConditionField(crName, "Forsaken", "reason")
		return r
	}

	// sentinelCounts reports what one Sentinel believes about the shared master: how
	// many replicas and how many peer Sentinels it knows. These two numbers ARE the
	// captor-side damage a capture does — the victim's Redis pods land in the replica
	// list (so they are failover candidates) and the victim's Sentinels land in the
	// peer list (so they vote).
	sentinelCounts := func(pod, masterName string) (slaves, peers string) {
		out, err := sentinelCmd(pod, "SENTINEL", "master", masterName)
		if err != nil {
			return "err", "err"
		}
		return sentinelField(out, "num-slaves"), sentinelField(out, "num-other-sentinels")
	}

	// expectSentinelCounts asserts every Sentinel of an instance agrees on the counts.
	expectSentinelCounts := func(crName, masterName, slaves, peers string, timeout time.Duration) {
		Eventually(func(g Gomega) {
			for _, sp := range sentinelPods(crName) {
				s, p := sentinelCounts(sp, masterName)
				g.Expect([]string{s, p}).To(Equal([]string{slaves, peers}),
					"%s reports num-slaves=%s num-other-sentinels=%s, want %s/%s",
					sp, s, p, slaves, peers)
			}
		}, timeout, 5*time.Second).Should(Succeed())
	}

	quarantineCR := func(crName, masterName string) string {
		// No spec.image, deliberately: both instances take the operator default, so
		// their RDB versions match and the victim's sync from the foreign master
		// SUCCEEDS. That is the silent capture — the common case, and the only one
		// that exercises the data clauses honestly (a version mismatch leaves the
		// victim at 0 keys, which LR-044 records as the luck the field incident had).
		//
		// No spec.auth either, and that is load-bearing rather than an omission —
		// see the DELIBERATELY AUTH-FREE block at the top of this file. In short:
		// auth disabled is half of quarantineConfigDangerous, so the attempt limit
		// is decided purely by the master name (see the Latched tier), and an
		// authenticated Sentinel would refuse the injected hello outright.
		return fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
  sentinel:
    masterName: %s
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace, masterName)
	}

	deploy := func(crName, masterName string) {
		AddReportEntry("cr:" + crName)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(quarantineCR(crName, masterName))
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			g.Expect(getPhase(crName)).To(Equal("Running"))
		}, 4*time.Minute, 5*time.Second).Should(Succeed(), "%s never reached Running", crName)
	}

	cleanup := func(names ...string) {
		if debugOnFailure && suiteOrSpecFailed() {
			By("skipping cleanup to allow debugging")
			return
		}
		for _, n := range names {
			_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", n,
				"-n", testNamespace, "--ignore-not-found"))
		}
	}

	// capture injects the cross-instance capture and returns the foreign master IP it
	// advertised. It asserts its own preconditions and its own landing, so a spec that
	// gets past it has a genuine capture rather than a dud payload.
	capture := func(captor, victim, masterName string) string {
		By("reading the captor's live master address")
		captorMasters, err := sentinelCmd(captor+"-sentinel-0", "SENTINEL", "masters")
		Expect(err).NotTo(HaveOccurred())
		Expect(sentinelField(captorMasters, "name")).To(Equal(masterName),
			"the two instances must share a master name for the hello to be accepted")
		foreign := sentinelField(captorMasters, "ip")
		Expect(foreign).NotTo(BeEmpty())
		AddReportEntry("foreign master (captor's)", foreign)

		By("checking no Sentinel of the victim already monitors that address")
		for _, sp := range sentinelPods(victim) {
			out, err := sentinelCmd(sp, "SENTINEL", "masters")
			Expect(err).NotTo(HaveOccurred())
			Expect(sentinelField(out, "ip")).NotTo(Equal(foreign),
				"%s already monitors the foreign master before any injection", sp)
		}

		By("injecting a hello for the captor's master into ALL THREE of the victim's Sentinels")
		// Injecting into all three is what planForsaken clause 2 needs (unanimity among
		// reachable monitoring Sentinels) — but the three are not independent: Sentinel
		// propagates a higher-epoch config to its peers in its own hellos, so by the time
		// the second or third injection is issued that Sentinel may ALREADY have converged
		// on its own. Observed live: sentinel-2 was on the foreign master before it was
		// injected. So a peer that has already converged is skipped rather than asserted
		// about — asserting otherwise is a race against Sentinel's gossip. At least one
		// injection must still be accepted, which is what keeps the payload's
		// positive control (a PUBLISH reply of 1) load-bearing.
		injected := 0
		for _, sp := range sentinelPods(victim) {
			before, err := sentinelCmd(sp, "SENTINEL", "masters")
			Expect(err).NotTo(HaveOccurred())
			if sentinelField(before, "ip") == foreign {
				AddReportEntry("converged before injection", sp)
				continue
			}
			epoch := nextEpoch(before)

			// ip,port,runid,current_epoch,master_name,master_ip,master_port,master_config_epoch
			// The advertised sender is the captor's own sentinel-0 address — the
			// introduction a recycled pod IP made in the field. One synthetic runid for
			// all three so they learn the same peer rather than three conflicting ones.
			hello := fmt.Sprintf("%s,26379,%s,%d,%s,%s,6379,%d",
				podIP(captor+"-sentinel-0"),
				"ca7e0000000000000000000000000000deadbee1",
				epoch, masterName, foreign, epoch)
			out, err := sentinelCmd(sp, "PUBLISH", "__sentinel__:hello", hello)
			Expect(err).NotTo(HaveOccurred(), "PUBLISH output: %s", out)
			// redis-cli exits 0 on a Redis error reply, so check the reply itself.
			// sentinelPublishCommand answers 1; anything else means the hello never
			// reached the processor and this spec would go on to test nothing.
			Expect(strings.TrimSpace(out)).To(Equal("1"),
				"%s refused the injected hello", sp)
			injected++
		}
		Expect(injected).To(BeNumerically(">=", 1),
			"no hello was injected at all, so nothing below is attributable to the payload")

		By("the victim's whole Sentinel quorum now serves the foreign master")
		Eventually(func(g Gomega) {
			for _, sp := range sentinelPods(victim) {
				out, err := sentinelCmd(sp, "SENTINEL", "masters")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(sentinelField(out, "ip")).To(Equal(foreign),
					"%s still monitors %s", sp, sentinelField(out, "ip"))
			}
		}, 90*time.Second, 3*time.Second).Should(Succeed())

		By("and every victim Redis pod follows it — no pod of its own is a master")
		// This is planForsaken clause 4. Until it holds there is still something to heal
		// the instance back toward, and the verdict is correctly withheld.
		Eventually(func(g Gomega) {
			for _, rp := range redisPods(victim) {
				out, err := redisExec(testNamespace, rp, "INFO", "replication")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("role:slave"), "%s is not a replica", rp)
				g.Expect(out).To(ContainSubstring("master_host:"+foreign),
					"%s does not follow the foreign master", rp)
			}
		}, 2*time.Minute, 3*time.Second).Should(Succeed())

		return foreign
	}

	// --- The full cycle ----------------------------------------------------
	//
	// The regression guard, and the spec that closes LR-042's coverage gap: capture →
	// Forsaken fires → both StatefulSets reach 0 → the CAPTOR's Sentinel replica list
	// returns to what the captor deployed → release → the victim re-bootstraps empty
	// with a master of its own, captor data intact throughout.
	//
	// The captor-side assertion is the load-bearing one. LR-044 shipped the claim "the
	// captor then heals itself through Rule D" as an INFERENCE from three
	// independently-documented gates (LR-008 living master, LR-011 healthy known
	// replica, LR-013 K8s wholeness). M4a confirmed it live twice; this is what keeps
	// it confirmed.
	Context("Full cycle", Ordered, func() {
		var captor, victim, masterName string

		BeforeAll(func() {
			stamp := time.Now().Unix()
			captor = fmt.Sprintf("q-captor-%d", stamp)
			victim = fmt.Sprintf("q-victim-%d", stamp)
			// A DISTINCT shared name, not the legacy "mymaster": sharing it is what
			// makes the capture possible, but "mymaster" plus auth-off is
			// quarantineConfigDangerous, which drops the attempt limit to 1 and LATCHES
			// on the first quarantine — making the release edge (and everything after
			// it) unobservable. The Latched tier below uses "mymaster" for exactly that
			// reason.
			masterName = fmt.Sprintf("q.shared.%d", stamp)
			deploy(captor, masterName)
			deploy(victim, masterName)
		})

		AfterAll(func() { cleanup(captor, victim) })

		It("quarantines the victim, lets the captor heal, then re-bootstraps the victim empty", func() {
			By("writing data to the captor's master")
			captorMaster := getMasterPod(captor)
			Expect(captorMaster).NotTo(BeEmpty())
			_, err := redisExec(testNamespace, captorMaster,
				"MSET", "cap-1", "cv1", "cap-2", "cv2", "cap-3", "cv3")
			Expect(err).NotTo(HaveOccurred())

			By("writing data to the victim's master (it will be flushed by the capture)")
			victimMaster := getMasterPod(victim)
			Expect(victimMaster).NotTo(BeEmpty())
			_, err = redisExec(testNamespace, victimMaster, "MSET", "vic-1", "vv1", "vic-2", "vv2")
			Expect(err).NotTo(HaveOccurred())

			By("baseline: the captor's Sentinels know exactly what the captor deployed")
			expectSentinelCounts(captor, masterName, "2", "2", 2*time.Minute)

			foreign := capture(captor, victim, masterName)

			By("the victim now serves the CAPTOR's keyspace — the silent capture")
			// Not a decoration: this is the shape that makes the data clauses
			// load-bearing. Keys > 0 on every victim pod, all of it the captor's, so a
			// literal "all pods hold 0 keys" gate would be inert here (LR-044).
			Eventually(func(g Gomega) {
				for _, rp := range redisPods(victim) {
					out, err := redisExec(testNamespace, rp, "GET", "cap-1")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(strings.TrimSpace(out)).To(Equal("cv1"),
						"%s does not hold the captor's data", rp)
					got, err := redisExec(testNamespace, rp, "GET", "vic-1")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(strings.TrimSpace(got)).To(BeEmpty(),
						"%s still holds its own data; the capture did not flush it", rp)
				}
			}, 90*time.Second, 3*time.Second).Should(Succeed())

			By("the captor is polluted: it now counts the victim's pods and Sentinels as its own")
			// The positive control for the healing assertion further down. Without it,
			// "the captor reports 2/2" at the end would be indistinguishable from a
			// capture that never touched the captor at all.
			Eventually(func(g Gomega) {
				s, p := sentinelCounts(captor+"-sentinel-0", masterName)
				g.Expect(s).To(Equal("5"), "captor num-slaves = %s, want 5 (2 its own + 3 the victim's)", s)
				g.Expect(p).To(Equal("5"), "captor num-other-sentinels = %s, want 5", p)
			}, 2*time.Minute, 3*time.Second).Should(Succeed())

			By("the operator declares the victim forsaken and quarantines it")
			// Budget: up to one steady interval before the capture is even observed,
			// + forsakenCooldown (30s), + one more steady interval to commit the
			// verdict, + LR-045's steady polling on every pass after that. 5 minutes is
			// ~3x the measured 61s (M4a: 30s + 31s) and deliberately loose, because
			// every edge here is quantised by a 30s poll.
			Eventually(func(g Gomega) {
				g.Expect(forsakenReason(victim)).To(Equal("Quarantined"))
				st, _ := getConditionField(victim, "Forsaken", "status")
				g.Expect(st).To(Equal("True"))
				g.Expect(quarantinedSince(victim)).NotTo(BeEmpty())
				g.Expect(quarantineAttempts(victim)).To(Equal("1"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("both StatefulSets reach 0 desired replicas")
			// The arming pass still applies 3 (the StatefulSets are reconciled before
			// the gather, so the marker this pass writes is read by the NEXT one) — the
			// monotone 3→3→0 ordering the wiring half predicted. Hence one more steady
			// interval of slack.
			Eventually(func(g Gomega) {
				g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("0"))
				g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("0"))
			}, 3*time.Minute, 2*time.Second).Should(Succeed())

			By("and HOLDS there — no 0→3→0 flap while settling")
			// The failure mode the wiring half was built against: an out-of-band scale
			// fought by the next server-side apply would put the pods back every pass,
			// re-polluting the captor. Sampled fast (2s) for a full minute inside the
			// 120s settle, because a flap is an interleaving, not a state.
			Consistently(func(g Gomega) {
				g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("0"))
				g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("0"))
			}, 60*time.Second, 2*time.Second).Should(Succeed())

			By("the victim's pods are gone")
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", testNamespace,
					"-l", "app.kubernetes.io/instance="+victim,
					"-o", "jsonpath={.items[*].metadata.name}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(BeEmpty(), "victim pods still present: %s", out)
			}, 2*time.Minute, 3*time.Second).Should(Succeed())

			By("THE LOAD-BEARING ONE: the captor prunes them and returns to its own topology")
			// Rule D's ghost-replica SENTINEL RESET, whose gate chain LR-044 could only
			// argue would pass. M4a measured ~5-12s from pods-gone; a whole minute of
			// budget is a formality.
			expectSentinelCounts(captor, masterName, "2", "2", 3*time.Minute)

			By("the captor's own data was never touched")
			for _, rp := range redisPods(captor) {
				out, err := redisExec(testNamespace, rp, "GET", "cap-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(strings.TrimSpace(out)).To(Equal("cv1"), "%s lost the captor's data", rp)
			}

			By("the release hands the pods back")
			// quarantineSettlePeriod (120s) + up to one steady interval to notice
			// (LR-045) + the apply. M4a measured 120-122s on the pre-LR-045 build, so
			// ~150s is the expected figure now; 5 minutes is the budget.
			Eventually(func(g Gomega) {
				g.Expect(quarantinedSince(victim)).To(BeEmpty())
				g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("3"))
				g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("3"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("and Rule L re-bootstraps the victim EMPTY with a master of its own")
			// Rule L's no-data reseed signature needs no opt-in: all Sentinels come back
			// bare, no master anywhere, zero data holders (LR-015). Budget: pod start +
			// Rule L's own 30s cooldown + its cooldown passes.
			Eventually(func(g Gomega) {
				g.Expect(getPhase(victim)).To(Equal("Running"))
				g.Expect(getMasterPod(victim)).NotTo(BeEmpty())
			}, 6*time.Minute, 5*time.Second).Should(Succeed())

			// Eventually, not a bare Expect: the reset LAGS the phase by at least one
			// reconcile pass, by construction. clearForsaken (littlered_controller.go)
			// runs inside reconcileSentinelCluster and reads lr.Status.Phase — the value
			// PERSISTED BY THE PREVIOUS PASS — while the phase itself is written at the
			// tail of the same pass by updateSentinelStatus. So the pass that first
			// reports Running cannot also clear the counter; the next one does. Measured
			// on t3e 2026-08-23: phase Running at 13:02:31Z, counter still 1 when read
			// 0.9s later, cleared by the pass at 13:02:33Z. A bare Expect here is a race
			// against a ~2s window sampled by a 5s poll, and it lost. The upper bound is
			// one steady interval (30s, LR-045) if no watch event arrives sooner, hence
			// 90s — still tight enough to fail a counter that never clears.
			Eventually(func(g Gomega) {
				g.Expect(quarantineAttempts(victim)).To(BeEmpty())
			}, 90*time.Second, 2*time.Second).Should(Succeed(),
				"the attempt counter must be reset once the instance is Running again")

			By("the victim came back empty, and monitoring its OWN master")
			for _, rp := range redisPods(victim) {
				out, err := redisExec(testNamespace, rp, "DBSIZE")
				Expect(err).NotTo(HaveOccurred())
				Expect(strings.TrimSpace(out)).To(Equal("0"), "%s came back with data", rp)
			}
			for _, sp := range sentinelPods(victim) {
				out, err := sentinelCmd(sp, "SENTINEL", "masters")
				Expect(err).NotTo(HaveOccurred())
				Expect(sentinelField(out, "ip")).NotTo(Equal(foreign),
					"%s still monitors the foreign master after re-bootstrap", sp)
			}
		})
	})

	// --- HoldDataPresent (a refusal) ---------------------------------------
	//
	// The quarantine deletes pods, so it carries its own data clause on top of the
	// verdict: a reachable pod holding keys that are NOT the captor's replicated copy
	// may be holding the only copy in existence, and the operator must refuse.
	//
	// Staging it means producing "reachable, role:slave, master_link_status NOT up,
	// keys > 0" on a victim pod while the capture verdict still stands. Three mechanisms
	// were tried; only the third is race-free.
	//
	//   - REPLICAOF NO ONE would make the pod role:master, which breaks planForsaken
	//     clause 4 and dissolves the verdict entirely — the spec would then be testing
	//     nothing.
	//   - REPLICAOF <blackhole> does produce the state, but the pod's master ADDRESS
	//     then disagrees with what the victim's Sentinels believe, and Sentinel's own
	//     +fix-slave-config repoints it back within ~failoverTimeout — after which it
	//     full-resyncs and the keys are the captor's again. The refusal would be a race.
	//   - A bogus masterauth, set BEFORE the injection, keeps master_host pointing at
	//     the foreign master (so Sentinel sees nothing to fix) while the handshake
	//     Sentinel's own SLAVEOF triggers fails on AUTH forever against a master that
	//     has no password. The dataset is retained, because a flush only happens on a
	//     SUCCESSFUL resync — so this pod keeps the VICTIM's own keys, which really are
	//     the only copy in existence.
	//
	// Pre-arming it, rather than breaking the link after the capture has landed, is not
	// tidiness: a first attempt broke the link afterwards and lost the race — the
	// operator had already armed the quarantine and the pod was GONE (`pods
	// "…-redis-1" not found`) before the precondition could even be read. forsakenCooldown
	// is 30s and staging a capture takes longer than that. Pre-arming means the clause is
	// true on the very first gather that sees the capture, so the quarantine is never
	// armed at all — which is also the behaviour under test.
	//
	// The preconditions are asserted from INFO replication + DBSIZE before the refusal
	// is asserted, and re-asserted inside the Consistently, because an assertion that
	// the operator refused is worthless if the state it should have refused on was
	// never actually produced.
	Context("Refusal when a victim pod holds data the captor does not have", Ordered, func() {
		var captor, victim, masterName string

		BeforeAll(func() {
			stamp := time.Now().Unix()
			captor = fmt.Sprintf("q-hdp-captor-%d", stamp)
			victim = fmt.Sprintf("q-hdp-victim-%d", stamp)
			masterName = fmt.Sprintf("q.hdp.%d", stamp)
			deploy(captor, masterName)
			deploy(victim, masterName)
		})

		AfterAll(func() { cleanup(captor, victim) })

		It("refuses to quarantine and leaves both StatefulSets at 3", func() {
			By("writing data to the captor's master")
			captorMaster := getMasterPod(captor)
			Expect(captorMaster).NotTo(BeEmpty())
			_, err := redisExec(testNamespace, captorMaster, "MSET", "cap-1", "cv1", "cap-2", "cv2")
			Expect(err).NotTo(HaveOccurred())

			By("writing data to the victim's own master and letting it replicate")
			victimMaster := getMasterPod(victim)
			Expect(victimMaster).NotTo(BeEmpty())
			_, err = redisExec(testNamespace, victimMaster, "MSET", "vic-1", "vv1", "vic-2", "vv2")
			Expect(err).NotTo(HaveOccurred())

			// A replica of the victim's own master, so it holds a replicated copy of the
			// victim's data rather than being the pod Sentinel is about to demote first.
			others := otherRedisPods(victim, victimMaster)
			Expect(others).NotTo(BeEmpty())
			pinned := others[0]
			AddReportEntry("pinned victim pod", pinned)

			Eventually(func(g Gomega) {
				out, err := redisExec(testNamespace, pinned, "GET", "vic-1")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("vv1"))
			}, 60*time.Second, 2*time.Second).Should(Succeed())

			By("pre-arming " + pinned + " so its sync from the foreign master can never succeed")
			// A wrong masterauth against a master with no password fails the handshake
			// permanently ("Client sent AUTH, but no password is set"). The existing link
			// to the victim's own master is unaffected until it reconnects, which is
			// exactly the SLAVEOF Sentinel is about to issue.
			_, err = redisExec(testNamespace, pinned, "CONFIG", "SET", "masterauth", "wrong-on-purpose")
			Expect(err).NotTo(HaveOccurred())

			foreign := capture(captor, victim, masterName)

			By("confirming the precondition: slave, link NOT up, keys > 0")
			Eventually(func(g Gomega) {
				info, err := redisExec(testNamespace, pinned, "INFO", "replication")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(info).To(ContainSubstring("role:slave"))
				g.Expect(info).To(ContainSubstring("master_host:" + foreign))
				g.Expect(info).NotTo(ContainSubstring("master_link_status:up"),
					"the replication link is still up:\n%s", info)
				size, err := redisExec(testNamespace, pinned, "DBSIZE")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(size)).NotTo(Equal("0"),
					"%s holds no keys, so there is nothing for the clause to protect", pinned)
				// And they are the VICTIM's own keys, not a replicated copy of the
				// captor's — i.e. genuinely the only copy in existence, which is what
				// the clause exists to protect.
				own, err := redisExec(testNamespace, pinned, "GET", "vic-1")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(own)).To(Equal("vv1"),
					"%s no longer holds the victim's own data", pinned)
			}, 90*time.Second, 3*time.Second).Should(Succeed())

			By("the operator declares the capture but REFUSES to quarantine")
			// Same budget as the arming edge in tier 1: the refusal reason only appears
			// once forsakenCooldown has elapsed (before that the condition reads
			// False/CaptureSuspected regardless of the data clauses).
			Eventually(func(g Gomega) {
				g.Expect(forsakenReason(victim)).To(Equal("QuarantineRefusedDataPresent"))
				st, _ := getConditionField(victim, "Forsaken", "status")
				g.Expect(st).To(Equal("True"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("no quarantine is armed and the pods stay put")
			Consistently(func(g Gomega) {
				g.Expect(quarantinedSince(victim)).To(BeEmpty(), "a quarantine was armed")
				g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("3"))
				g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("3"))

				// Re-assert the precondition, so a green cannot be earned by the state
				// quietly decaying into "link up, captor's copy, safe to discard".
				info, err := redisExec(testNamespace, pinned, "INFO", "replication")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(info).NotTo(ContainSubstring("master_link_status:up"),
					"the staged precondition decayed; this refusal proves nothing")
				size, err := redisExec(testNamespace, pinned, "DBSIZE")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(size)).NotTo(Equal("0"))
			}, 90*time.Second, 5*time.Second).Should(Succeed())
		})
	})

	// --- Latched (the deterministic, dangerous-config case) ----------------
	//
	// Made deterministic by the DANGEROUS configuration rather than by running two
	// cycles: auth disabled AND the effective master name being the shared legacy
	// "mymaster" is quarantineConfigDangerous, which sets the attempt limit to 1. The
	// first quarantine therefore spends the budget, and the next pass latches instead
	// of releasing.
	//
	// The shared legacy name is used ON PURPOSE here — do not "fix" this fixture to a
	// scoped name, and do not fold it into the tiers above. A scoped shared name gives
	// a limit of 2 and the instance releases, which is what makes tier 1's cycle
	// observable and what makes THIS tier impossible; "mymaster" is what makes this one
	// deterministic and tier 1 impossible.
	Context("Latched after the attempt budget is spent", Ordered, func() {
		var captor, victim string
		// The legacy shared name. quarantineConfigDangerous reads the EFFECTIVE name,
		// so setting it explicitly counts exactly as omitting it would — a deliberate
		// "mymaster" is exactly as capturable.
		const masterName = "mymaster"

		BeforeAll(func() {
			stamp := time.Now().Unix()
			captor = fmt.Sprintf("q-latch-captor-%d", stamp)
			victim = fmt.Sprintf("q-latch-victim-%d", stamp)
			deploy(captor, masterName)
			deploy(victim, masterName)
		})

		AfterAll(func() { cleanup(captor, victim) })

		It("stays at 0 replicas without ever releasing", func() {
			captorMaster := getMasterPod(captor)
			Expect(captorMaster).NotTo(BeEmpty())
			_, err := redisExec(testNamespace, captorMaster, "MSET", "cap-1", "cv1")
			Expect(err).NotTo(HaveOccurred())

			capture(captor, victim, masterName)

			By("the quarantine arms and immediately latches — the budget is 1, not 2")
			Eventually(func(g Gomega) {
				g.Expect(forsakenReason(victim)).To(Equal("QuarantineLatched"))
				st, _ := getConditionField(victim, "Forsaken", "status")
				g.Expect(st).To(Equal("True"))
				g.Expect(quarantinedSince(victim)).NotTo(BeEmpty())
				g.Expect(quarantineAttempts(victim)).To(Equal("1"))
				g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("0"))
				g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("0"))
			}, 6*time.Minute, 5*time.Second).Should(Succeed())

			By("and it does NOT come back — held well past the 120s settling period")
			// The whole claim of this tier is a non-event, so the window has to outlast
			// the timer that would have produced the event: quarantineSettlePeriod
			// (120s) plus a steady interval of granularity (LR-045), with margin.
			Consistently(func(g Gomega) {
				g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("0"))
				g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("0"))
				g.Expect(quarantinedSince(victim)).NotTo(BeEmpty(),
					"the marker was cleared — the instance was released despite the latch")
				g.Expect(forsakenReason(victim)).To(Equal("QuarantineLatched"))
			}, 200*time.Second, 5*time.Second).Should(Succeed())

			By("the captor is healthy and holds its own data")
			expectSentinelCounts(captor, masterName, "2", "2", 3*time.Minute)
			for _, rp := range redisPods(captor) {
				out, err := redisExec(testNamespace, rp, "GET", "cap-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(strings.TrimSpace(out)).To(Equal("cv1"))
			}
		})
	})
})
