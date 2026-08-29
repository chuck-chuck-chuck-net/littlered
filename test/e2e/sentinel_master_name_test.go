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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/littlered-operator/littlered-operator/test/utils"
)

// e2eMasterName derives an instance's Sentinel master name exactly the way the
// documentation tells users to: "<namespace>.<name>".
//
// Every e2e sentinel instance uses it, and that is deliberate rather than tidiness.
// A single shared literal across the suite would make the e2e instances themselves a
// cross-instance collision — the very defect under test — and two concurrent suite
// runs against one cluster would merge each other's quorums. Deriving from BOTH the
// namespace and the name (not the name alone) keeps the entropy the recommendation
// itself carries, so the suite is a live demonstration of the advice.
func e2eMasterName(namespace, name string) string {
	return fmt.Sprintf("%s.%s", namespace, name)
}

var _ = Describe("Sentinel Master Name Scoping", Label("sentinel"), func() {

	applyCR := func(manifest string) (string, error) {
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(manifest)
		return utils.Run(cmd)
	}

	// Auth-ON, like every other sentinel-mode fixture in this suite
	// (auth_utils_test.go). The awkward-name spec below therefore queries Sentinel
	// with a credential; the point it proves — that an awkward but legal master
	// name survives sentinel.conf, the startup script and the preStop hook — is
	// independent of auth, and running it in the suite's default posture means it
	// also proves the two do not interact.
	sentinelCR := func(crName, masterNameLine string) string {
		return e2eAuthPreamble(crName) + fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
%s  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
  sentinel:
%s    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace, e2eAuthSpecYAML(crName), masterNameLine)
	}

	// --- Admission -----------------------------------------------------------
	//
	// The CRD-schema layer is already covered red-first by envtest specs. What only
	// e2e can show is that the *shipped* CRD — the one applied by `make deploy` from
	// config/crd/bases, which the Helm chart mirrors — actually carries the rule. This
	// catches a regeneration or packaging slip, not a logic bug.
	Context("Admission", func() {
		It("rejects a sentinel instance created without spec.sentinel.masterName", func() {
			crName := fmt.Sprintf("mn-missing-%d", time.Now().Unix())
			out, err := applyCR(sentinelCR(crName, ""))

			Expect(err).To(HaveOccurred(),
				"the shipped CRD must require spec.sentinel.masterName; apply output: %s", out)
			Expect(out).To(ContainSubstring("masterName"),
				"the rejection must name the field so the message is actionable")

			// Nothing should have been created.
			cmd := exec.Command("kubectl", "get", "littlered", crName, "-n", testNamespace)
			_, getErr := utils.Run(cmd)
			Expect(getErr).To(HaveOccurred(), "a rejected CR must not exist")
		})

		It("accepts an awkward but legal master name", func() {
			crName := fmt.Sprintf("mn-awkward-%d", time.Now().Unix())
			// Mixed case, dots, dashes, digits, and long — everything the pattern and
			// MaxLength allow. Proves nothing downstream (sentinel.conf, the startup
			// script's redis-cli invocation, the preStop hook, lrctl) chokes on shape.
			awkward := "Aa0." + strings.Repeat("x-y.", 20) + "Zz9"
			Expect(len(awkward)).To(BeNumerically("<=", 128))

			out, err := applyCR(sentinelCR(crName, fmt.Sprintf("    masterName: %s\n", awkward)))
			Expect(err).NotTo(HaveOccurred(), "apply output: %s", out)
			defer func() {
				cmd := exec.Command("kubectl", "delete", "littlered", crName,
					"-n", testNamespace, "--ignore-not-found")
				_, _ = utils.Run(cmd)
			}()

			By("reaching Running with that name in use")
			Eventually(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("confirming Sentinel actually monitors it")
			Eventually(func(g Gomega) {
				out, err := sentinelPortExec(testNamespace, crName+"-sentinel-0",
					"SENTINEL", "get-master-addr-by-name", awkward)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).NotTo(BeEmpty(),
					"Sentinel does not know master %q", awkward)
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})
	})
})

// --- Cross-instance isolation ------------------------------------------------
//
// The property under test: a Sentinel hello naming a master this instance does not
// know is discarded. That lookup — sentinelGetMasterByName(token[4]) in
// sentinelProcessHelloMessage() — is the ONLY isolation boundary the gossip protocol
// has, so a per-instance master name is what stops one deployment absorbing another.
//
// Two co-located instances do NOT merge spontaneously: each sentinel only subscribes
// to the hello channel of instances it already monitors, so a merge needs an
// *introduction*. In the field that introduction was a recycled pod IP inheriting a
// stale known-sentinel entry. Waiting for one in a test would be neither fast nor
// deterministic, so this injects the introduction directly — a hello PUBLISHed at
// instance A's sentinel port, which Sentinel accepts from anyone
// (sentinelPublishCommand feeds it straight to the hello processor).
//
// The injected payload advertises instance B's master at a high config epoch. The
// only thing that decides whether A swallows it is whether B's master NAME is one A
// knows. Before per-instance naming both instances answered to "mymaster", so this
// payload flipped A onto B's master and A's replicas flushed to resync from it. That
// is the red this spec must produce against pre-fix code.
//
// ============================ DELIBERATELY AUTH-FREE ==========================
//
// Every other sentinel-mode fixture in this suite defaults to auth-ON
// (auth_utils_test.go). This Describe must NOT be flipped, and a future sweep of
// "the last few stragglers" must leave it alone.
//
// AUTHENTICATION IS ONE OF THE CONDITIONS THAT PREVENTS A CAPTURE. Both specs
// below work by PUBLISHing a hello straight at a Sentinel's port
// (`redis-cli -p 26379 PUBLISH __sentinel__:hello ...`). With `requirepass` set,
// that connection is answered with NOAUTH before the payload ever reaches
// sentinelProcessHelloMessage(), so:
//
//   - the isolation spec would pass having tested NOTHING (it asserts a
//     non-event, and a rejected connection looks exactly like a discarded hello);
//   - the positive control — the thing that makes the isolation result
//     attributable at all — could not land, and would fail on its PUBLISH reply.
//
// Auth would also be the wrong variable to hold: LR-039 records auth as the
// remaining mitigation for the ADDRESS-ADOPTION path, whereas what these specs
// measure is the master NAME closing the gossip-fusion path. Turning auth on here
// would confound the two and destroy the coverage.
// ==============================================================================
var _ = Describe("Sentinel Cross-Instance Isolation", Label("sentinel"), Ordered, func() {
	var instA, instB, lrctlBin string

	// lrctlVerify runs the REAL CLI against the cluster. The detector behind it is
	// unit-tested; what only this can show is the whole plumbing — CLI gatherer to
	// evidence to rendered output — producing the text a human actually reads. Its
	// exit status is ignored: verify exits non-zero on an unhealthy instance, which is
	// precisely the case under test.
	lrctlVerify := func(crName string) string {
		out, _ := utils.Run(exec.Command(lrctlBin, "verify", crName, "-n", testNamespace))
		AddReportEntry("lrctl verify "+crName, out)
		return out
	}

	sentinelExec := func(crName string, args ...string) (string, error) {
		full := append([]string{"exec", crName + "-sentinel-0", "-n", testNamespace,
			"-c", "sentinel", "--", "redis-cli", "-p", "26379"}, args...)
		return utils.Run(exec.Command("kubectl", full...))
	}

	// field reads a value out of redis-cli's flat key/value output.
	field := func(out, key string) string {
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
	// hardcoded constant is a trap: the moment real activity pushes the epoch past it,
	// the injection silently becomes a no-op and the assertion fails as "did not land"
	// with no hint that the payload, not the code, is stale. Derived from the target's
	// own reply, the payload is always credible.
	nextEpoch := func(mastersOut string) uint64 {
		cur, _ := strconv.ParseUint(field(mastersOut, "config-epoch"), 10, 64)
		return cur + 1000
	}

	podIP := func(pod string) string {
		out, err := utils.Run(exec.Command("kubectl", "get", "pod", pod, "-n", testNamespace,
			"-o", "jsonpath={.status.podIP}"))
		Expect(err).NotTo(HaveOccurred())
		return strings.TrimSpace(out)
	}

	deploy := func(crName string) {
		AddReportEntry("cr:" + crName)
		cr := fmt.Sprintf(`
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
`, crName, testNamespace, e2eMasterName(testNamespace, crName))
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			g.Expect(getPhase(crName)).To(Equal("Running"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	}

	BeforeAll(func() {
		// bin/lrctl is guaranteed fresh by make (BeforeSuite already asserted it
		// exists) — the suite never builds its own copy. See lrctlBinPath.
		var err error
		lrctlBin, err = lrctlBinPath()
		Expect(err).NotTo(HaveOccurred())

		stamp := time.Now().Unix()
		instA, instB = fmt.Sprintf("iso-a-%d", stamp), fmt.Sprintf("iso-b-%d", stamp)
		deploy(instA)
		deploy(instB)

		By("pausing the operator for the duration of the injections")
		// Both specs below measure what SENTINEL does with an injected hello. The
		// operator is a third party to that question and, since LR-041 restored the
		// gather, an actively interfering one: the advertised foreign master is not
		// one of the receiving instance's pods, so it reads as a ghost master and the
		// LR-008 correction issues REMOVE + MONITOR back to the real master. Measured
		// on a live cluster, that lands in the SAME SECOND as the PUBLISH, so a poll
		// loop never observes the capture at all.
		//
		// This is not only about making the positive control observable. It is what
		// makes the ISOLATION spec mean anything: with the operator healing captures
		// sub-second, that spec would pass whether the master name protected instance
		// A or not, and its conclusion would be unattributable. Pausing removes the
		// confound from both. (Found exactly this way — the positive control went red
		// while the isolation spec still passed, which is the failure mode it exists
		// to catch.)
		scaleOperator(0)
		// From here to AfterAll the CR status is FROZEN at whatever the operator last
		// wrote, which is not necessarily healthy: deploy() waits for phase Running
		// once, and Ready flaps back to False while Sentinel is still learning the
		// second replica — so the freeze can capture a transient. Measured: A sat at
		// Ready=False for a whole spec and went True 4s after the operator returned.
		// Assert on the data plane or on `lrctl verify` (which gathers live), never on
		// status, for the rest of this Describe.
	})

	AfterAll(func() {
		// Unconditionally FIRST, before any early return: an operator left at 0
		// replicas silently breaks every later spec and the next run of the whole
		// suite. Mirrors the reshard tier's same discipline.
		scaleOperator(1)

		if debugOnFailure && suiteOrSpecFailed() {
			By("skipping cleanup to allow debugging")
			return
		}
		for _, n := range []string{instA, instB} {
			_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", n,
				"-n", testNamespace, "--ignore-not-found"))
		}
	})

	It("does not absorb a foreign instance's Sentinel configuration", func() {
		By("recording instance A's own pod IPs")
		aIPs := map[string]bool{}
		for _, p := range []string{instA + "-redis-0", instA + "-redis-1", instA + "-redis-2"} {
			aIPs[podIP(p)] = true
		}

		By("reading instance B's master identity as B's own Sentinels report it")
		// Read at runtime rather than assuming: on pre-fix code this is "mymaster"
		// for BOTH instances, which is exactly the condition that makes the injection
		// land and this spec go red.
		bMasters, err := sentinelExec(instB, "SENTINEL", "masters")
		Expect(err).NotTo(HaveOccurred())
		bMasterName := field(bMasters, "name")
		bMasterIP := field(bMasters, "ip")
		Expect(bMasterName).NotTo(BeEmpty())
		Expect(bMasterIP).NotTo(BeEmpty())
		AddReportEntry("instB master name:" + bMasterName)

		aBefore, err := sentinelExec(instA, "SENTINEL", "masters")
		Expect(err).NotTo(HaveOccurred())
		aMasterName := field(aBefore, "name")
		AddReportEntry("instA master name:" + aMasterName)

		By("injecting a Sentinel hello for B's master into A, at a high config epoch")
		// ip,port,runid,current_epoch,master_name,master_ip,master_port,master_config_epoch
		epoch := nextEpoch(aBefore)
		hello := fmt.Sprintf("%s,26379,%s,%d,%s,%s,6379,%d",
			podIP(instB+"-sentinel-0"),
			"f0000000000000000000000000000000deadbeef",
			epoch, bMasterName, bMasterIP, epoch)
		out, err := sentinelExec(instA, "PUBLISH", "__sentinel__:hello", hello)
		Expect(err).NotTo(HaveOccurred(), "PUBLISH output: %s", out)
		// redis-cli exits 0 on a Redis error reply, so check the reply itself:
		// sentinelPublishCommand answers 1, anything else means the hello never
		// reached the processor and this spec would pass having tested nothing.
		Expect(strings.TrimSpace(out)).To(Equal("1"), "Sentinel refused the injected hello")

		By("holding instance A to its own topology")
		// Generous window: the epoch bump and +switch-master are processed inside the
		// single PUBLISH, and any resulting SLAVEOF follows within ~10s.
		Consistently(func(g Gomega) {
			masters, err := sentinelExec(instA, "SENTINEL", "masters")
			g.Expect(err).NotTo(HaveOccurred())

			// Every failure carries the raw reply. Without it a red is unreadable:
			// the compared value alone cannot distinguish "captured" from "observed
			// mid-reset", and those demand opposite conclusions.
			raw := func(what string) string {
				return fmt.Sprintf("%s\nA's SENTINEL masters:\n%s", what, masters)
			}

			g.Expect(field(masters, "name")).To(Equal(aMasterName),
				raw("instance A's master name changed"))
			g.Expect(aIPs).To(HaveKey(field(masters, "ip")),
				raw(fmt.Sprintf("instance A is monitoring %s, which is not one of its own pods "+
					"— it has been captured", field(masters, "ip"))))
			// Checked last, and reported rather than asserted on its own: a
			// +switch-master wipes this list, so a bare count is ambiguous in
			// isolation. The master-IP assertion above is the authoritative one.
			g.Expect(field(masters, "num-other-sentinels")).To(Equal("2"),
				raw("instance A's known-sentinel count changed; if the master IP above is "+
					"still an A pod this is a reset artifact, not (yet) a capture"))
		}, 45*time.Second, 5*time.Second).Should(Succeed())

		By("confirming A's data pods never followed a foreign master")
		for _, p := range []string{instA + "-redis-0", instA + "-redis-1", instA + "-redis-2"} {
			out, err := utils.Run(exec.Command("kubectl", "exec", p, "-n", testNamespace,
				"-c", "redis", "--", "redis-cli", "INFO", "replication"))
			Expect(err).NotTo(HaveOccurred())
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, "master_host:") {
					host := strings.TrimSpace(strings.TrimPrefix(line, "master_host:"))
					Expect(aIPs).To(HaveKey(host),
						"%s replicates from %s, which is not an instance-A pod", p, host)
				}
			}
		}

		By("confirming instance A is still serving — checked on the data plane, not via status")
		// NOT `getPhase(instA)`. The operator is paused for the duration of the
		// injections (see BeforeAll), so the CR status is frozen at whatever it held
		// at pause time and can say nothing at all about the injection. Asserting on
		// it was a false signal in both directions: it could fail on a healthy
		// instance frozen mid-flap, and it could equally pass on a captured one.
		//
		// "Still serving" is a data-plane claim, so make it against the data plane:
		// exactly one of A's pods is a master, and it accepts a write and returns it.
		aMasters := 0
		for _, p := range []string{instA + "-redis-0", instA + "-redis-1", instA + "-redis-2"} {
			out, err := utils.Run(exec.Command("kubectl", "exec", p, "-n", testNamespace,
				"-c", "redis", "--", "redis-cli", "INFO", "replication"))
			Expect(err).NotTo(HaveOccurred())
			if !strings.Contains(out, "role:master") {
				continue
			}
			aMasters++
			key := "iso-serving-" + instA
			set, err := utils.Run(exec.Command("kubectl", "exec", p, "-n", testNamespace,
				"-c", "redis", "--", "redis-cli", "SET", key, "ok"))
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(set)).To(Equal("OK"), "%s is master but refused a write", p)
			got, err := utils.Run(exec.Command("kubectl", "exec", p, "-n", testNamespace,
				"-c", "redis", "--", "redis-cli", "GET", key))
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(got)).To(Equal("ok"))
		}
		Expect(aMasters).To(Equal(1),
			"instance A should have exactly one master of its own; found %d", aMasters)

		By("lrctl verify reports the scoped name and no foreign contact")
		report := lrctlVerify(instA)
		Expect(report).To(ContainSubstring("Master name: " + e2eMasterName(testNamespace, instA)))
		Expect(report).To(ContainSubstring("No foreign Sentinel contact observed"))
		Expect(report).NotTo(ContainSubstring("Evidence of another Sentinel deployment"))
	})

	// Positive control for the spec above. Without it, a payload that never reaches
	// the hello processor — wrong token count, a Redis error reply, a future change
	// to the wire format — would make the isolation assertion pass having proved
	// nothing, because "nothing happened" is also what success looks like.
	//
	// Same payload shape, same injection path, one variable changed: the master name
	// now matches the receiving instance's own. If Sentinel acts on this one, then it
	// would have acted on the other too had the name matched, and the isolation above
	// is attributable to the name rather than to a dud payload.
	//
	// Deliberately destructive to instance B, which is torn down straight after. The
	// advertised master is a TEST-NET-1 address (RFC 5737) so nothing can attach to
	// it and instance A is left undisturbed.
	It("proves the injection path is live by capturing an instance that shares the name", func() {
		// The advertised master is instance A's real master: a LIVE address. An
		// unroutable one (TEST-NET) would work for the capture itself, but Sentinel
		// flags it s_down after down-after-milliseconds and the diagnostic then
		// correctly reclassifies it as ordinary dead debris — making any assertion on
		// verify's output a race against a 5 s timer. A live foreign master is also
		// the faithful scenario: this is now a genuine cross-instance capture.
		//
		// Deliberately destructive to BOTH instances; they are torn down immediately
		// after, and instance A's own assertions have already completed above.
		foreignMaster := podIP(instA + "-redis-0")

		before, err := sentinelExec(instB, "SENTINEL", "masters")
		Expect(err).NotTo(HaveOccurred())
		bMasterName := field(before, "name")
		Expect(field(before, "ip")).NotTo(Equal(foreignMaster))

		epoch := nextEpoch(before)
		hello := fmt.Sprintf("%s,26379,%s,%d,%s,%s,6379,%d",
			podIP(instB+"-sentinel-0"),
			"f1111111111111111111111111111111deadbeef",
			epoch, bMasterName, foreignMaster, epoch)
		out, err := sentinelExec(instB, "PUBLISH", "__sentinel__:hello", hello)
		Expect(err).NotTo(HaveOccurred(), "PUBLISH output: %s", out)
		Expect(strings.TrimSpace(out)).To(Equal("1"))

		By("instance B follows the injected configuration, because the name matched")
		Eventually(func(g Gomega) {
			masters, err := sentinelExec(instB, "SENTINEL", "masters")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(field(masters, "ip")).To(Equal(foreignMaster),
				"the injection did not land; the isolation spec above proves nothing")
		}, 30*time.Second, 2*time.Second).Should(Succeed())

		By("lrctl verify names the capture rather than reporting a healthy instance")
		Eventually(func(g Gomega) {
			report := lrctlVerify(instB)
			g.Expect(report).To(ContainSubstring("Evidence of another Sentinel deployment"))
			g.Expect(report).To(ContainSubstring(foreignMaster),
				"the diagnostic must name the foreign master, not merely flag a problem")
			g.Expect(report).NotTo(ContainSubstring("No foreign Sentinel contact observed"))
		}, 60*time.Second, 5*time.Second).Should(Succeed())
	})
})

// =============================================================================
// In-place Sentinel master-name RENAME (LR-048 / ADR-017, design §10 WP6)
// =============================================================================
//
// The feature under test: editing `spec.sentinel.masterName` on a healthy sentinel
// instance re-points its Sentinels at the new name, REMOVES the old one, rolls the
// Redis pods so their baked scripts carry it, and preserves the dataset.
//
// The bug this is the regression guard for (design §4, measured live as WP0 on t3e):
// before Rule N the rename "converged" and the data survived, but every Sentinel was
// left monitoring BOTH names — permanently. Two `sentinel monitor` lines, two config
// epochs, two independent failover state machines over the same three pods, which is
// exactly the hazard LR-039 named. WP0 measured both names still present 12m39s after
// the patch on a `Running`/`Ready=True` instance, and measured the master's baked
// old-name preStop firing a REAL `SENTINEL failover <old>`, leaving the two names
// naming two different LIVE pods as master for 56.6s.
//
// Tier 1's `SENTINEL masters` length-of-1 assertion is that defect's red, and it was
// observed red against a pre-fix operator image before this file was believed.
//
// BUDGETS ARE MEASURED, NOT GUESSED (design §7.1a, WP0 on t3e, 1s sampling):
//   both names present on all three Sentinels  t0+0.8s   (the prune lands in pass 1)
//   redis-2 Ready under the new name           t0+12.9s
//   redis-1 deleted / Ready                    t0+47.6s / t0+55.5s
//   redis-0 (the master) deleted               t0+89.1s
//   final sustained Running/Ready=True         t0+176.8s (2m57s)
// Post-fix the `redis-0` edge costs ~30s MORE: with the stale entry gone, the master's
// baked-old-name preStop `SENTINEL failover <old>` errors instead of handing over, so
// the desired name's quorum waits out down-after-milliseconds before electing. Every
// Eventually below is sized off those numbers plus honest margin.
//
// TWO THINGS THESE ASSERTIONS DELIBERATELY DO NOT DO:
//
//  1. They never assert on the TRANSIENT. Design §7.1b: a healthy rename transiently
//     presents the capture signature (WP0 measured `Forsaken=False/CaptureSuspected` at
//     t0+89.1s, cleared by the 30s cooldown), and the CR flaps
//     Running → Initializing → Running several times during the roll. So every
//     steady-state claim is an `Eventually` on the SETTLED value, and `Consistently` is
//     used only where the design says a state must hold.
//  2. The failover step is never allowed to become a data-loss test that passes by luck
//     (LR-017's discipline). The key sweep is exact — every written key, by value — and
//     it is asserted after the failover whatever shape the failover took.

// sentinelMasterNames returns every master name one Sentinel monitors, in reply order.
//
// This is the assertion the whole feature turns on, so it reads the LIST rather than
// asking about one name: `SENTINEL master <desired>` answering happily is exactly what
// a two-name instance looks like, which is how the defect stayed invisible to
// `lrctl verify` for as long as it did (design §10 WP5). redis-cli renders
// `SENTINEL masters` as flat key/value lines, one master's fields after another's, so
// each line equal to "name" opens a master entry and the line after it is its name.
func sentinelMasterNames(out string) []string {
	var names []string
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for i, l := range lines {
		if strings.TrimSpace(l) == "name" && i+1 < len(lines) {
			names = append(names, strings.TrimSpace(lines[i+1]))
		}
	}
	return names
}

// sentinelMasterIPs is sentinelMasterNames' sibling: the address each monitored name
// points at, keyed by name. Used to prove a stale FOREIGN entry is still present
// (tier 2) rather than merely that some entry is.
func sentinelMasterIPs(out string) map[string]string {
	byName := map[string]string{}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	cur := ""
	for i, l := range lines {
		switch strings.TrimSpace(l) {
		case "name":
			if i+1 < len(lines) {
				cur = strings.TrimSpace(lines[i+1])
			}
		case "ip":
			if cur != "" && i+1 < len(lines) {
				byName[cur] = strings.TrimSpace(lines[i+1])
			}
		}
	}
	return byName
}

// renameMasterName performs the runbook's step 4 verbatim (design §12): a merge patch
// of the one field. Nothing else is touched, which is the point — the operator has to
// derive the whole operation from the spec edit alone.
func renameMasterName(crName, newName string) {
	patch := fmt.Sprintf(`{"spec":{"sentinel":{"masterName":%q}}}`, newName)
	out, err := utils.Run(exec.Command("kubectl", "patch", "littlered", crName,
		"-n", testNamespace, "--type=merge", "-p", patch))
	Expect(err).NotTo(HaveOccurred(), "patch output: %s", out)
}

var _ = Describe("Sentinel Master Name Rename", Label("sentinel"), func() {

	sentinelPods := func(crName string) []string {
		return []string{crName + "-sentinel-0", crName + "-sentinel-1", crName + "-sentinel-2"}
	}
	// monitoredNames asks ONE Sentinel what it monitors. Errors are folded into the
	// returned slice as a single sentinel value so an Eventually reports the exec
	// failure rather than an unexplained empty list.
	monitoredNames := func(pod string) []string {
		out, err := sentinelPortExec(testNamespace, pod, "SENTINEL", "masters")
		if err != nil {
			return []string{"exec-error:" + strings.TrimSpace(out)}
		}
		return sentinelMasterNames(out)
	}

	// expectExactlyOneName is R3 ("every Sentinel monitors exactly one master name — the
	// desired one — no leftover entry, EVER") as an assertion over the whole quorum.
	expectExactlyOneName := func(g Gomega, crName, want string) {
		for _, sp := range sentinelPods(crName) {
			names := monitoredNames(sp)
			g.Expect(names).To(Equal([]string{want}),
				"%s monitors %v, want exactly [%s]", sp, names, want)
		}
	}

	// expectNeverForsaken is the K9 regression guard the design makes MANDATORY, and
	// which nothing else in this suite carries.
	//
	// §7.1b measured a healthy rename transiently presenting the whole capture
	// signature — the quorum unanimous on a JUST-REPLACED pod's address, which is no
	// longer in ValidIPs and not yet flagged down, with no pod of ours a master. All
	// four planForsaken clauses hold in that window, and the design says plainly that
	// "what saved it was the 30s forsakenCooldown, not the clauses". K9 rates a
	// spurious quarantine of a healthy instance mid-rename as "strictly worse than the
	// bug being fixed", because the quarantine DELETES THE PODS and storage is EmptyDir
	// (pillar 3.1) — on an instance holding data, that is data loss on a supported
	// operation.
	//
	// So the rename window must be asserted to produce no capture verdict, not merely
	// to converge. Note what this does NOT assert: a transient False/CaptureSuspected
	// is legitimate and expected (§7.1b measured exactly one such sample), so only the
	// settled True verdict is a failure.
	expectNeverForsaken := func(g Gomega, crName string) {
		st, _ := getConditionField(crName, "Forsaken", "status")
		reason, _ := getConditionField(crName, "Forsaken", "reason")
		g.Expect(st).NotTo(Equal("True"),
			"the operator declared this instance FORSAKEN (%s) during an ordinary rename — "+
				"there is no other Sentinel deployment here, so this is the K9 false positive: "+
				"a quarantine deletes the pods and EmptyDir means that is data loss", reason)
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

	// --- Tier 1: the full rename, with the dataset preserved -----------------
	//
	// Auth-ON, like every other sentinel-mode fixture in this suite. It is not
	// incidental here: Rule N's REMOVE, the pre-check IsMonitoring and Rule 0's
	// re-registration all go through the authenticated path, so running the tier in
	// the suite's default posture is what proves the rename works with a credential
	// rather than only in the auth-free capture fixtures.
	Context("Full rename with the dataset preserved", Ordered, func() {
		var crName, newName string
		// The legacy shared name, on purpose: the instances the runbook is written for
		// are the ones that never chose a name, and `mymaster` is what they carry.
		const oldName = "mymaster"
		// Enough keys that a lost dataset is unmistakable and few enough that a full
		// byte-for-byte sweep is one round trip.
		const keyCount = 500

		// verifyKeys sweeps EVERY written key by value and returns how many are missing
		// or wrong. Not a DBSIZE check and not a sample: LR-038's lesson is that a
		// sampled counter cannot carry a durability claim (one lost key read five times
		// looks like five), and DBSIZE cannot tell a preserved dataset from a
		// same-sized wrong one.
		const sweep = `local bad=0 for i=1,tonumber(ARGV[1]) do ` +
			`if redis.call('GET','rn:'..i) ~= 'v'..i then bad=bad+1 end end return bad`

		BeforeAll(func() {
			crName = fmt.Sprintf("rn-full-%d", time.Now().Unix())
			newName = e2eMasterName(testNamespace, crName)
			AddReportEntry("cr:" + crName)

			By("deploying a healthy sentinel instance under the legacy name " + oldName)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(sentinelRenameCR(crName, oldName))
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "apply output: %s", out)

			Eventually(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed(), "%s never reached Running", crName)
		})

		AfterAll(func() { cleanup(crName) })

		It("leaves exactly one monitored name, the data intact, and a working failover", func() {
			By("the precondition the runbook demands: healthy, and monitoring exactly " + oldName)
			Eventually(func(g Gomega) {
				expectExactlyOneName(g, crName, oldName)
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By(fmt.Sprintf("writing %d distinguishable keys to the master", keyCount))
			master := getMasterPod(crName)
			Expect(master).NotTo(BeEmpty())
			_, err := redisExec(testNamespace, master, "EVAL",
				`for i=1,tonumber(ARGV[1]) do redis.call('SET','rn:'..i,'v'..i) end return 1`,
				"0", strconv.Itoa(keyCount))
			Expect(err).NotTo(HaveOccurred())

			By("asserting the write actually REPLICATED before anything is disrupted")
			// LR-016's precondition lesson: a tier that disrupts an instance without
			// first establishing that the data reached the replicas can fail (or pass)
			// for a reason that has nothing to do with what it claims to test.
			replicas := otherRedisPods(crName, master)
			Expect(replicas).To(HaveLen(2))
			Eventually(func(g Gomega) {
				for _, rp := range replicas {
					size, err := redisExec(testNamespace, rp, "DBSIZE")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(strings.TrimSpace(size)).To(Equal(strconv.Itoa(keyCount)),
						"%s has not received the dataset", rp)
				}
			}, 2*time.Minute, 3*time.Second).Should(Succeed())

			By("renaming: kubectl patch spec.sentinel.masterName -> " + newName)
			renameMasterName(crName, newName)

			// ---- THE REGRESSION GUARD ----------------------------------------
			//
			// Both halves in one assertion, because the defect is precisely that the
			// first half passes while the second does not: on pre-fix code every
			// Sentinel monitors the new name within ~0.1s (Rule 0 registers it) and
			// ALSO still monitors `mymaster`, forever. Asserting only "the new name is
			// monitored" is the assertion `lrctl verify` was already making, and it
			// reported a two-name instance as entirely healthy.
			By("every Sentinel monitors the new name and NOTHING else — no `" + oldName + "` left")
			Eventually(func(g Gomega) {
				expectExactlyOneName(g, crName, newName)
			}, 3*time.Minute, 3*time.Second).Should(Succeed())

			By("and it STAYS at one name across the whole Redis rollout")
			// The roll takes ~3 minutes and replaces all three Redis pods, each of
			// which re-registers with Sentinel on the way back. A stale entry
			// re-appearing later would be the same defect arriving by a different door,
			// and only a sustained window can see it. Sampled across the window in which
			// WP0 measured redis-1 and redis-0 being replaced.
			Consistently(func(g Gomega) {
				expectExactlyOneName(g, crName, newName)
				expectNeverForsaken(g, crName)
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("the instance settles back to Running / Ready=True under the new name")
			// WP0 measured 2m57s to the final sustained Running, plus ~30s for the
			// post-fix redis-0 edge (the baked old-name preStop can no longer hand over,
			// so the quorum waits out down-after-milliseconds). The CR legitimately
			// flaps Running → Initializing → Running several times on the way (§7.1b),
			// so this is an Eventually on the settled value followed by a short
			// Consistently, never a bare read.
			//
			// AND THE SETTLE IS EXPLICIT, because an `Eventually` alone does not wait for
			// a flap to FINISH — it is satisfied by the first `Running` sample it sees,
			// including one taken during a mid-roll interlude, and then hands a
			// still-flapping instance to the `Consistently` below. Measured (LR-050,
			// t3e): exactly that, `Initializing` 5.2s into the window, ~1 run in 5. So
			// the window only opens once `Running`/`Ready=True` has held CONTINUOUSLY for
			// crSettleWindow — see crSettleTracker for why that number is 60s and not a
			// guess. The `Consistently` that follows is unchanged: the claim "and it
			// STAYS there" is still asserted, on top of the settle rather than instead
			// of it.
			settle := &crSettleTracker{settleFor: crSettleWindow}
			var lastPhase, lastReady string
			Eventually(func(g Gomega) {
				lastPhase = getPhase(crName)
				lastReady, _ = getConditionField(crName, "Ready", "status")
				g.Expect(settle.observe(lastPhase, lastReady, time.Now())).To(BeTrue(),
					"instance has not held Running/Ready=True for %s yet (currently %s/Ready=%s)",
					crSettleWindow, lastPhase, lastReady)
			}, 10*time.Minute, 5*time.Second).Should(Succeed())
			Consistently(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
			}, 30*time.Second, 5*time.Second).Should(Succeed())

			By("StaleMasterName reports the quiet steady state, False/Converged")
			Eventually(func(g Gomega) {
				st, _ := getConditionField(crName, "StaleMasterName", "status")
				reason, _ := getConditionField(crName, "StaleMasterName", "reason")
				g.Expect([]string{st, reason}).To(Equal([]string{"False", "Converged"}))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("SentinelMasterNameUnscoped is not raised")
			// Disclosed rather than dressed up: this condition can only ever be True for
			// an instance whose masterName field is UNSET, and LR-039 made the field
			// REQUIRED on create, so it is unreachable through the API for anything this
			// suite can deploy. The assertion is therefore that the rename does not
			// somehow raise it — a guard against a regression in the accessor, not a
			// demonstration of clearing.
			st, err := getConditionField(crName, "SentinelMasterNameUnscoped", "status")
			Expect(err).NotTo(HaveOccurred())
			Expect(st).To(Equal("False"))

			By("every written key survived the rename — swept exactly, on the master")
			postMaster := getMasterPod(crName)
			Expect(postMaster).NotTo(BeEmpty())
			bad, err := redisExec(testNamespace, postMaster, "EVAL", sweep, "0", strconv.Itoa(keyCount))
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(bad)).To(Equal("0"),
				"%s keys are missing or wrong on %s after the rename", strings.TrimSpace(bad), postMaster)

			By("and they are readable through the {name} Service, which is what a client sees")
			Eventually(func(g Gomega) {
				bad, err := serviceExec(crName, "EVAL", sweep, "0", strconv.Itoa(keyCount))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(bad)).To(Equal("0"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			// ---- The proof that we left ONE healthy state machine behind ------
			//
			// One monitored name and intact data would both be true of an instance
			// whose Sentinel quorum can no longer elect anything. Killing the master and
			// requiring a real election under the NEW name is what separates "converged"
			// from "quiesced".
			By("a failover still works under the new name: deleting master " + postMaster)
			_, err = utils.Run(exec.Command("kubectl", "delete", "pod", postMaster,
				"-n", testNamespace, "--wait=false"))
			Expect(err).NotTo(HaveOccurred())

			By("a different pod is elected master")
			Eventually(func(g Gomega) {
				m := getMasterPod(crName)
				g.Expect(m).NotTo(BeEmpty())
				g.Expect(m).NotTo(Equal(postMaster), "no failover happened; still %s", m)
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("still exactly one monitored name afterwards")
			Eventually(func(g Gomega) {
				expectExactlyOneName(g, crName, newName)
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("and the dataset survived the failover too — the same exact sweep")
			// Asserted unconditionally, whatever shape the failover took (LR-017: a
			// tier that accepts more than one outcome must still ALWAYS assert no data
			// loss, so a false negative can never become a false positive).
			Eventually(func(g Gomega) {
				m := getMasterPod(crName)
				g.Expect(m).NotTo(BeEmpty())
				bad, err := redisExec(testNamespace, m, "EVAL", sweep, "0", strconv.Itoa(keyCount))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(bad)).To(Equal("0"),
					"%s keys are missing or wrong on the new master %s", strings.TrimSpace(bad), m)
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

	// --- Tier 3: idempotence -------------------------------------------------
	//
	// Two renames in quick succession, so the second lands while the first is still
	// rolling. It exercises the design's "two stale names on one Sentinel" row for
	// real: the quorum can transiently carry `mymaster`, the intermediate name AND the
	// final one, and Rule N must converge on exactly the last-requested value without
	// thrashing between them.
	//
	// Cheap by construction — it is the tier-1 fixture with no data and no failover —
	// and it is the only place the "no remembered previous name" property (R4) is
	// visible end to end: the operator never learns that name A existed, it simply
	// removes everything that is not name B.
	Context("Renamed twice in quick succession", Ordered, func() {
		var crName, nameA, nameB string

		BeforeAll(func() {
			crName = fmt.Sprintf("rn-twice-%d", time.Now().Unix())
			nameA = e2eMasterName(testNamespace, crName) + ".a"
			nameB = e2eMasterName(testNamespace, crName) + ".b"
			AddReportEntry("cr:" + crName)

			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(sentinelRenameCR(crName, "mymaster"))
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "apply output: %s", out)
			Eventually(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())
		})

		AfterAll(func() { cleanup(crName) })

		It("converges on the last name with exactly one entry and no thrash", func() {
			Eventually(func(g Gomega) {
				expectExactlyOneName(g, crName, "mymaster")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("renaming to " + nameA)
			renameMasterName(crName, nameA)

			By("waiting only long enough for the first rename to be in flight, then renaming to " + nameB)
			// One pass, not one convergence: WP0 measured Rule 0 registering the new
			// name at t0+0.1s and the prune landing in the same pass, so ~10s is several
			// passes in and the Redis roll is definitely under way (redis-2 is deleted at
			// t0+0.6s). Deliberately NOT waiting for nameA to converge — that would be
			// two sequential renames, which tier 1 already covers.
			time.Sleep(10 * time.Second)
			renameMasterName(crName, nameB)

			By("converging on exactly " + nameB)
			Eventually(func(g Gomega) {
				expectExactlyOneName(g, crName, nameB)
			}, 5*time.Minute, 3*time.Second).Should(Succeed())

			By("and holding there while both rollouts finish")
			Consistently(func(g Gomega) {
				expectExactlyOneName(g, crName, nameB)
				expectNeverForsaken(g, crName)
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("the instance is healthy under the final name")
			Eventually(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
				st, _ := getConditionField(crName, "Ready", "status")
				g.Expect(st).To(Equal("True"))
			}, 10*time.Minute, 5*time.Second).Should(Succeed())
		})
	})
})

// sentinelRenameCR renders a healthy 3-pod sentinel instance under a given master name.
//
// Auth-ON (the suite default, auth_utils_test.go) and that is load-bearing rather than
// inherited: the rename's whole execution path — Rule 0's MONITOR + auth-pass, Rule N's
// bounded IsMonitoring pre-check and its REMOVE — runs through the authenticated client,
// and the deliberately auth-free capture fixtures elsewhere in this file cannot exercise
// it. downAfterMilliseconds is left at the product default rather than shortened to 5s:
// the post-rename `redis-0` edge is DEFINED by that timer (the baked old-name preStop can
// no longer hand over, so the quorum has to time the master out), and shrinking it would
// hide the very cost the design documents.
func sentinelRenameCR(crName, masterName string) string {
	return e2eAuthPreamble(crName) + fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
%s  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
  sentinel:
    masterName: %s
    quorum: 2
`, crName, testNamespace, e2eAuthSpecYAML(crName), masterName)
}

// serviceExec runs redis-cli against the label-routed `{name}` Service from inside one of
// the instance's own pods — i.e. by the path a client actually takes.
//
// It matters that this is not another pod-local read: the Service selects on the
// operator-managed `role: master` label, so it also proves the operator moved that label
// to whichever pod the rename's failover left as master. The command is issued from
// sentinel-0's sentinel container purely because that pod is never the Redis pod being
// rolled; the target is the Service.
func serviceExec(crName string, args ...string) (string, error) {
	full := []string{"exec", crName + "-sentinel-0", "-n", testNamespace,
		"-c", "sentinel", "--", "redis-cli", "-h", crName, "-p", "6379"}
	full = append(full, redisCliAuthArgs(crName)...)
	full = append(full, args...)
	return utils.Run(exec.Command("kubectl", full...))
}

// =============================================================================
// Tier 2 — renaming to escape a capture must NOT defeat the quarantine
// =============================================================================
//
// The trap the design calls out (§7.3, N6, risk K2): an owner who reads the
// LR-039 → LR-042 → LR-044 runbook chain will try exactly this — "we were captured, so
// let's give the instance a unique name."
//
// Before WP4b that turned a diagnosed, self-healing capture into an undiagnosed
// leaderless refusal. `planForsaken` evaluated only the DESIRED name, so the moment the
// rename landed the victim's Sentinels were monitoring the foreign master under a name
// the verdict no longer looked at: clause 1 failed, the capture verdict EVAPORATED, and
// with it ADR-016's quarantine — the thing that heals the captor in ~4 minutes. Making
// the verdict name-agnostic is the actual repair; Rule N refusing to prune is the
// diagnostic that sits on top of it.
//
// So the load-bearing assertion here is the FIRST one: `Forsaken` still holds after the
// rename. The rest — no REMOVE, the foreign entry still present, and finally the
// quarantine firing and both sides healing — is what makes "the verdict survived" mean
// something operationally.
//
// STAGING, and why it is shaped this way. A plain capture arms the quarantine within
// ~60s and takes the victim's pods away, which makes "the foreign entry is still there"
// unobservable for any useful window. So this tier PRE-ARMS the HoldDataPresent data
// clause (LR-044) exactly as the quarantine tier does — a bogus `masterauth` on one
// victim pod, set BEFORE the injection — which holds the quarantine open indefinitely
// while leaving the capture verdict fully in force. That buys a deterministic window in
// which to rename and assert. Releasing the clause afterwards (restoring `masterauth`)
// lets the pod resync from the foreign master, `atRisk` clears, and the quarantine runs
// for real — so both halves of the tier are exercised on one instance, in order.
//
// The pinned pod is deliberately NOT the highest ordinal. The rename rewrites the Redis
// pod template, so the StatefulSet immediately deletes `redis-2`; its replacement parks
// in the startup wait-loop (nothing monitors the new name, because a forsaken instance
// returns before Rule 0), which stalls the roll there and leaves the lower ordinals —
// including the pinned pod and its staged state — untouched for the whole window.
//
// ============================ DELIBERATELY AUTH-FREE ==========================
//
// Same reason as the two capture-staging Describes it borrows from: the capture is
// staged by PUBLISHing a hello at the victim's sentinel port, and with `requirepass`
// set that connection answers NOAUTH before the payload ever reaches
// sentinelProcessHelloMessage() — so the capture would never land and every assertion
// below would silently degrade into asserting a non-event. The shared name here is a
// SCOPED one rather than `mymaster` on purpose: `mymaster` + auth-off is
// quarantineConfigDangerous, which drops the attempt budget to 1 and LATCHES on the
// first quarantine, making the release-and-heal half of this tier unobservable.
// ==============================================================================
var _ = Describe("Sentinel Master Name Rename Under Capture", Label("sentinel"), Ordered, func() {
	var captor, victim, sharedName, newName, foreign, pinned string

	sentinelCmd := func(pod string, args ...string) (string, error) {
		full := append([]string{"exec", pod, "-n", testNamespace,
			"-c", "sentinel", "--", "redis-cli", "-p", "26379"}, args...)
		return utils.Run(exec.Command("kubectl", full...))
	}
	sentinelField := func(out, key string) string {
		lines := strings.Split(strings.TrimSpace(out), "\n")
		for i, l := range lines {
			if strings.TrimSpace(l) == key && i+1 < len(lines) {
				return strings.TrimSpace(lines[i+1])
			}
		}
		return ""
	}
	nextEpoch := func(mastersOut string) uint64 {
		cur, _ := strconv.ParseUint(sentinelField(mastersOut, "config-epoch"), 10, 64)
		return cur + 1000
	}
	sentinelPodsOf := func(crName string) []string {
		return []string{crName + "-sentinel-0", crName + "-sentinel-1", crName + "-sentinel-2"}
	}
	redisPodsOf := func(crName string) []string {
		return []string{crName + "-redis-0", crName + "-redis-1", crName + "-redis-2"}
	}
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
	sentinelCounts := func(pod, masterName string) (slaves, peers string) {
		out, err := sentinelCmd(pod, "SENTINEL", "master", masterName)
		if err != nil {
			return "err", "err"
		}
		return sentinelField(out, "num-slaves"), sentinelField(out, "num-other-sentinels")
	}

	// The capture-staging machinery is DUPLICATED from sentinel_quarantine_test.go
	// rather than hoisted, following the precedent that file set (and documents) when
	// it duplicated from the isolation Describe above: those fixtures carry
	// load-bearing warning comments about the operator being paused and about avoiding
	// status assertions, and a mechanical refactor of two freshly-fixed Describes does
	// not belong in the change that adds a third. What is reused is the KNOWLEDGE, not
	// the closures — including both of LR-044's staging findings: the precondition is
	// asserted over all three Sentinels BEFORE any injection, and the PUBLISH reply is
	// asserted to be `1` so the payload's positive control stays load-bearing.
	capture := func() string {
		By("reading the captor's live master address")
		captorMasters, err := sentinelCmd(captor+"-sentinel-0", "SENTINEL", "masters")
		Expect(err).NotTo(HaveOccurred())
		Expect(sentinelField(captorMasters, "name")).To(Equal(sharedName))
		f := sentinelField(captorMasters, "ip")
		Expect(f).NotTo(BeEmpty())
		AddReportEntry("foreign master (captor's)", f)

		By("asserting the precondition over ALL THREE of the victim's Sentinels first")
		for _, sp := range sentinelPodsOf(victim) {
			out, err := sentinelCmd(sp, "SENTINEL", "masters")
			Expect(err).NotTo(HaveOccurred())
			Expect(sentinelField(out, "ip")).NotTo(Equal(f),
				"%s already monitors the foreign master before any injection", sp)
		}

		By("injecting a hello for the captor's master into all three")
		injected := 0
		for _, sp := range sentinelPodsOf(victim) {
			before, err := sentinelCmd(sp, "SENTINEL", "masters")
			Expect(err).NotTo(HaveOccurred())
			if sentinelField(before, "ip") == f {
				// Sentinel propagates a higher-epoch config to its peers in its own
				// hellos, so a peer may already have converged (LR-044 observed exactly
				// this). Skipping it is correct; asserting about it is a race.
				AddReportEntry("converged before injection", sp)
				continue
			}
			epoch := nextEpoch(before)
			hello := fmt.Sprintf("%s,26379,%s,%d,%s,%s,6379,%d",
				podIP(captor+"-sentinel-0"),
				"ca7e0000000000000000000000000000deadbee2",
				epoch, sharedName, f, epoch)
			out, err := sentinelCmd(sp, "PUBLISH", "__sentinel__:hello", hello)
			Expect(err).NotTo(HaveOccurred(), "PUBLISH output: %s", out)
			Expect(strings.TrimSpace(out)).To(Equal("1"), "%s refused the injected hello", sp)
			injected++
		}
		Expect(injected).To(BeNumerically(">=", 1),
			"no hello was injected at all, so nothing below is attributable to the payload")

		By("the victim's whole Sentinel quorum now serves the foreign master")
		Eventually(func(g Gomega) {
			for _, sp := range sentinelPodsOf(victim) {
				out, err := sentinelCmd(sp, "SENTINEL", "masters")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(sentinelField(out, "ip")).To(Equal(f),
					"%s still monitors %s", sp, sentinelField(out, "ip"))
			}
		}, 90*time.Second, 3*time.Second).Should(Succeed())

		By("and no Redis pod of the victim is a master any more (planForsaken clause 4)")
		Eventually(func(g Gomega) {
			for _, rp := range redisPodsOf(victim) {
				out, err := redisExec(testNamespace, rp, "INFO", "replication")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("role:slave"), "%s is not a replica", rp)
			}
		}, 2*time.Minute, 3*time.Second).Should(Succeed())

		return f
	}

	quarantineCR := func(crName string) string {
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
`, crName, testNamespace, sharedName)
	}

	BeforeAll(func() {
		stamp := time.Now().Unix()
		captor = fmt.Sprintf("rn-cap-captor-%d", stamp)
		victim = fmt.Sprintf("rn-cap-victim-%d", stamp)
		sharedName = fmt.Sprintf("rn.shared.%d", stamp)
		newName = e2eMasterName(testNamespace, victim)
		AddReportEntry("cr:" + victim)

		for _, n := range []string{captor, victim} {
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(quarantineCR(n))
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "apply output: %s", out)
		}
		for _, n := range []string{captor, victim} {
			Eventually(func(g Gomega) {
				g.Expect(getPhase(n)).To(Equal("Running"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed(), "%s never reached Running", n)
		}
	})

	AfterAll(func() {
		if debugOnFailure && suiteOrSpecFailed() {
			By("skipping cleanup to allow debugging")
			return
		}
		for _, n := range []string{captor, victim} {
			_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", n,
				"-n", testNamespace, "--ignore-not-found"))
		}
	})

	It("keeps the capture verdict, prunes nothing, and still quarantines and heals", func() {
		By("writing data to the captor's master")
		captorMaster := getMasterPod(captor)
		Expect(captorMaster).NotTo(BeEmpty())
		_, err := redisExec(testNamespace, captorMaster, "MSET", "cap-1", "cv1", "cap-2", "cv2")
		Expect(err).NotTo(HaveOccurred())

		By("baseline: the captor's Sentinels know exactly what the captor deployed")
		// The positive control for the healing assertion at the end: without it,
		// "the captor reports 2/2" would be indistinguishable from a capture that never
		// touched the captor at all.
		Eventually(func(g Gomega) {
			for _, sp := range sentinelPodsOf(captor) {
				s, p := sentinelCounts(sp, sharedName)
				g.Expect([]string{s, p}).To(Equal([]string{"2", "2"}),
					"%s reports %s/%s", sp, s, p)
			}
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("writing data to the victim's own master and letting it replicate")
		victimMaster := getMasterPod(victim)
		Expect(victimMaster).NotTo(BeEmpty())
		_, err = redisExec(testNamespace, victimMaster, "MSET", "vic-1", "vv1", "vic-2", "vv2")
		Expect(err).NotTo(HaveOccurred())

		// A replica of the victim's own master, and never the highest ordinal: the
		// rename deletes redis-2 first and its replacement parks, which is what keeps
		// the roll away from this pod for the whole assertion window.
		for _, rp := range []string{victim + "-redis-0", victim + "-redis-1"} {
			if rp != victimMaster {
				pinned = rp
				break
			}
		}
		Expect(pinned).NotTo(BeEmpty())
		AddReportEntry("pinned victim pod", pinned)
		Eventually(func(g Gomega) {
			out, err := redisExec(testNamespace, pinned, "GET", "vic-1")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("vv1"))
		}, 90*time.Second, 2*time.Second).Should(Succeed())

		By("pre-arming " + pinned + " so its sync from the foreign master can never succeed")
		// A wrong masterauth against a password-less master fails the handshake forever
		// ("Client sent AUTH, but no password is set") while master_host still points at
		// the foreign master, so Sentinel sees nothing to fix (+fix-slave-config never
		// fires) and the dataset is retained — a flush only happens on a SUCCESSFUL
		// resync. The pod therefore keeps the VICTIM's own keys, genuinely the only copy
		// in existence, which is exactly what LR-044's atRisk clause exists to protect.
		_, err = redisExec(testNamespace, pinned, "CONFIG", "SET", "masterauth", "wrong-on-purpose")
		Expect(err).NotTo(HaveOccurred())

		foreign = capture()

		By("the operator declares the capture and REFUSES to quarantine (data at risk)")
		Eventually(func(g Gomega) {
			st, _ := getConditionField(victim, "Forsaken", "status")
			reason, _ := getConditionField(victim, "Forsaken", "reason")
			g.Expect(st).To(Equal("True"))
			g.Expect(reason).To(Equal("QuarantineRefusedDataPresent"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		// ---- THE PANICKED RENAME -----------------------------------------
		By("the owner panics and renames the captured instance to " + newName)
		renameMasterName(victim, newName)

		By("the capture verdict SURVIVES the rename — this is WP4b, and it is the repair")
		// Before planForsaken was made name-agnostic this is where the verdict
		// evaporated: the Sentinels went on monitoring the foreign master under the now
		// STALE name, clause 1 stopped finding a monitoring Sentinel for the DESIRED
		// name, and ADR-016's quarantine — the only thing that heals the captor — was
		// never armed. The reason may legitimately move between the two data refusals
		// during the roll (redis-2 is replaced and parks, so it can read as unverifiable
		// rather than as holding data), so the assertion is on the verdict itself plus
		// the family of refusals, not on one reason string.
		Consistently(func(g Gomega) {
			st, _ := getConditionField(victim, "Forsaken", "status")
			reason, _ := getConditionField(victim, "Forsaken", "reason")
			g.Expect(st).To(Equal("True"), "the capture verdict was lost after the rename")
			g.Expect(reason).To(BeElementOf(
				"QuarantineRefusedDataPresent", "QuarantineRefusedDataUnknown"),
				"unexpected Forsaken reason %q", reason)

			// No REMOVE was issued: the stale, foreign entry is still on every Sentinel.
			for _, sp := range sentinelPodsOf(victim) {
				out, err := sentinelCmd(sp, "SENTINEL", "masters")
				g.Expect(err).NotTo(HaveOccurred())
				byName := sentinelMasterIPs(out)
				g.Expect(byName).To(HaveKeyWithValue(sharedName, foreign),
					"%s no longer carries the stale entry %s -> %s; Rule N pruned a capture: %v",
					sp, sharedName, foreign, byName)
			}

			// No quarantine was armed, so the pods are still there.
			g.Expect(quarantinedSince(victim)).To(BeEmpty())
			g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("3"))
			g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("3"))

			// Re-assert the staged precondition, so this refusal cannot pass by decay.
			info, err := redisExec(testNamespace, pinned, "INFO", "replication")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(info).NotTo(ContainSubstring("master_link_status:up"),
				"the staged precondition decayed; this refusal proves nothing")
			own, err := redisExec(testNamespace, pinned, "GET", "vic-1")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(own)).To(Equal("vv1"))
		}, 60*time.Second, 5*time.Second).Should(Succeed())

		By("StaleMasterName reports True/Foreign — Rule N stood down, it did not converge")
		// Read once rather than in the window above. The reason is EXACT, not a family:
		// G0 (`forsaken`) is checked FIRST and UNCONDITIONALLY in `planStaleMasterNames`
		// — ahead of the ground-truth check, ahead of G1, and ahead of the per-entry
		// survey — so while the capture verdict holds Rule N returns `Foreign` and
		// nothing else. The `Consistently` above has just asserted `Forsaken=True` over
		// this very window, so that precondition is established rather than assumed.
		//
		// In particular LR-050's rollout gate cannot make this `Deferred` here, even
		// though the rename genuinely is rolling the Redis StatefulSet: the gate acts on
		// G5's per-entry attribution, which G0 returns before ever reaching.
		//
		// This assertion USED to read BeElementOf("Foreign", "ForeignSuspected"). That
		// second reason was DELETED by LR-050 — the rollout gate closed §9.2's window at
		// its source, so the settle, its cooldown and `status.staleMasterNameForeignSince`
		// all came out with it. Accepting a reason the product can no longer emit is a
		// guard that would silently pass a regression reintroducing exactly the surface
		// that was removed, which is the one thing an assertion in this position must not
		// do. `TestStaleMasterNameHasNoSuspicionReason` fails if it ever comes back.
		//
		// What this must never say is False/Converged — "the operator thinks this
		// instance is fine" — but asserting only that would be weaker than the code
		// warrants.
		st, err := getConditionField(victim, "StaleMasterName", "status")
		Expect(err).NotTo(HaveOccurred())
		reason, _ := getConditionField(victim, "StaleMasterName", "reason")
		AddReportEntry("StaleMasterName after the rename", st+"/"+reason)
		Expect(st).To(Equal("True"))
		Expect(reason).To(Equal("Foreign"),
			"Rule N must stand down as Foreign while the capture verdict holds (G0), not %q", reason)

		// ---- and ADR-016 still works -------------------------------------
		By("releasing the staged data risk, so the quarantine may proceed")
		// Restoring masterauth lets the pinned pod's handshake succeed; it resyncs from
		// the foreign master, which flushes the victim's own keys and replaces them with
		// the captor's — at which point atRisk is false for the right reason (the keys
		// are the captor's copy and the original still exists on the captor).
		_, err = redisExec(testNamespace, pinned, "CONFIG", "SET", "masterauth", "")
		Expect(err).NotTo(HaveOccurred())

		By("the quarantine fires despite the rename — a panicked rename no longer defeats ADR-016")
		Eventually(func(g Gomega) {
			reason, _ := getConditionField(victim, "Forsaken", "reason")
			g.Expect(reason).To(Equal("Quarantined"))
			g.Expect(quarantinedSince(victim)).NotTo(BeEmpty())
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("both StatefulSets reach 0 desired replicas and the victim's pods go away")
		Eventually(func(g Gomega) {
			g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("0"))
			g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("0"))
		}, 3*time.Minute, 2*time.Second).Should(Succeed())
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", testNamespace,
				"-l", "app.kubernetes.io/instance="+victim,
				"-o", "jsonpath={.items[*].metadata.name}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(BeEmpty(), "victim pods still present: %s", out)
		}, 3*time.Minute, 3*time.Second).Should(Succeed())

		By("the captor heals itself through Rule D, exactly as it would have without the rename")
		Eventually(func(g Gomega) {
			for _, sp := range sentinelPodsOf(captor) {
				s, p := sentinelCounts(sp, sharedName)
				g.Expect([]string{s, p}).To(Equal([]string{"2", "2"}),
					"%s reports %s/%s", sp, s, p)
			}
		}, 4*time.Minute, 5*time.Second).Should(Succeed())

		By("the captor's own data was never touched")
		for _, rp := range redisPodsOf(captor) {
			out, err := redisExec(testNamespace, rp, "GET", "cap-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("cv1"), "%s lost the captor's data", rp)
		}

		By("the victim is released and Rule L re-bootstraps it empty — under the NEW name")
		// The silver lining the design records (§7.2): Rule L's electMaster issues
		// REMOVE + MONITOR with the name the operator currently WANTS, so the rename
		// completes out of the wreckage. The victim comes back with exactly one
		// monitored name and it is the one the owner asked for.
		Eventually(func(g Gomega) {
			g.Expect(getPhase(victim)).To(Equal("Running"))
			g.Expect(getMasterPod(victim)).NotTo(BeEmpty())
		}, 8*time.Minute, 5*time.Second).Should(Succeed())

		Eventually(func(g Gomega) {
			for _, sp := range sentinelPodsOf(victim) {
				out, err := sentinelCmd(sp, "SENTINEL", "masters")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(sentinelMasterNames(out)).To(Equal([]string{newName}),
					"%s monitors %v, want exactly [%s]", sp, sentinelMasterNames(out), newName)
			}
		}, 4*time.Minute, 5*time.Second).Should(Succeed())

		By("and it came back empty")
		for _, rp := range redisPodsOf(victim) {
			out, err := redisExec(testNamespace, rp, "DBSIZE")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("0"), "%s came back with data", rp)
		}
	})
})

// =============================================================================
// The settle tracker — and why the tier needs one
// =============================================================================
//
// Plain table tests, not Ginkgo specs, needing no cluster. They live here only because
// the code they guard carries the e2e build tag (the `auth_utils_unit_test.go`
// precedent). Run them on their own — an unfiltered `go test -tags e2e ./test/e2e/...`
// starts the whole suite:
//
//	go test -tags e2e ./test/e2e/ -run 'TestCRSettle'

// crSettleTracker folds a sequence of CR polls into one question: has this instance held
// `Running`/`Ready=True` CONTINUOUSLY for `settleFor`?
//
// It exists because "the CR reads Running" and "the CR has finished flapping" are
// different claims, and tier 1 was making the first while asserting the second. Design
// §7.1a: a rename replaces all three Redis pods, and the CR legitimately flaps
// `Running → Initializing → Running` twice more on the way. An `Eventually` is satisfied
// by the FIRST `Running` sample it sees, so on an unlucky poll alignment it returns
// during a mid-roll interlude and hands a still-flapping instance to the `Consistently`
// that follows — which then reads `Initializing` and fails. Measured live (LR-050, t3e):
// exactly that, 5.2s into the window, roughly 1 run in 5.
//
// THE SETTLE BUDGET IS DERIVED, NOT TUNED. The mid-roll `Running` interludes are not
// arbitrary: the Redis StatefulSet carries `minReadySeconds: 35` (`resources.go`), so
// after a replaced pod goes Ready the StatefulSet waits exactly that long before deleting
// the next one — and the CR reads `Running` for that whole wait. WP0's measured trace is
// this arithmetic to the second: `redis-2` Ready at t0+12.9s, `redis-1` deleted at
// t0+47.6s. So `minReadySeconds` IS the structural upper bound on a false settle, and the
// tracker's window must exceed it. 60s = 35s + margin for the operator's own observation
// lag, and it is well inside the tier's existing 10-minute budget: WP0 measured the final
// sustained `Running` at t0+176.8s, plus ~30s for the post-fix `redis-0` edge.
//
// Note what this deliberately does NOT do: it does not shorten or replace the tier's
// `Consistently`. The claim "and it STAYS Running" is still asserted, on top of the
// settle, exactly as before.
type crSettleTracker struct {
	settleFor time.Duration
	since     time.Time // zero when the last sample was NOT the settled state
}

// crSettleWindow is the derived number above: it must exceed the Redis StatefulSet's
// minReadySeconds, which is the structural upper bound on a mid-roll `Running` interlude,
// with margin for the operator's own observation lag.
//
// ⚠ IT IS KEYED TO THE DEFAULT 35s, NOT TO WHAT THE CR CARRIES. `minReadySeconds` is
// user-settable — `resources.go` reads `lr.Spec.UpdateStrategy.MinReadySeconds` and only
// falls back to `int32(35)` — so this constant is a margin against a value the fixture
// happens not to set. That is the shape that has bitten this branch twice: LR-049 (a
// constant timeout against a real one) and LR-050, whose whole reason for rejecting a
// longer `forsakenCooldown` was that it would be a margin against the user-settable,
// unbounded `spec.sentinel.downAfterMilliseconds`. Setting
// `updateStrategy.minReadySeconds` in `sentinelRenameCR` without revisiting this number
// silently restores the flake — so `TestRenameFixtureDoesNotOverrideMinReadySeconds`
// fails the moment the fixture grows that field, rather than leaving the next reader a
// fresh mystery.
//
// The 25s of margin over 35s covers the operator's own observation lag between a pod
// going Ready and the CR reading `Running`, which is REASONED, NOT MEASURED. If this ever
// settles falsely inside a 60s interlude, that lag is the number to go and measure — not
// this constant to bump.
const crSettleWindow = 60 * time.Second

// crSettleMinReady is the `resources.go` default for the StatefulSet's
// `minReadySeconds` field, as a Duration — named so the drift guard below and the
// derivation above cannot disagree about the number they are both keyed to.
const crSettleMinReady = 35 * time.Second

// observe folds one poll in and reports whether the settled state has now held for the
// whole window. Any non-settled sample resets the streak — a flap is an interleaving, not
// a state, so it can only be seen by keeping the sequence.
func (t *crSettleTracker) observe(phase, ready string, now time.Time) bool {
	if phase != "Running" || ready != "True" {
		t.since = time.Time{}
		return false
	}
	if t.since.IsZero() {
		t.since = now
	}
	return now.Sub(t.since) >= t.settleFor
}

// TestCRSettleTrackerRejectsAWindowOpenedMidFlap replays the rename's MEASURED phase
// trace (§7.1a, WP0 on t3e) at the tier's own 5s poll cadence and asserts the two
// properties the tier needs, in the order that matters:
//
//  1. the tracker must NOT report settled during either mid-roll `Running` interlude —
//     this is the flake, and it is the row the pre-fix logic fails;
//  2. it must report settled once the instance has genuinely finished — otherwise the fix
//     is an unconditional-fail, which is no better than an unconditional-pass.
//
// The trace: `redis-2` rolls (Ready t0+12.9s), `minReadySeconds: 35` elapses, `redis-1`
// rolls (Ready t0+55.5s), 35s more, then the master `redis-0` goes at t0+89.1s and the
// post-fix edge costs down-after-milliseconds on top, settling at ~t0+207s.
func TestCRSettleTrackerRejectsAWindowOpenedMidFlap(t *testing.T) {
	const settleFor = 60 * time.Second
	const finalSettleAt = 210 * time.Second // first sample of the terminal Running run

	// running reports the CR's phase at time d, from the measured trace.
	running := func(d time.Duration) bool {
		switch {
		case d < 15*time.Second: // redis-2 replaced
			return false
		case d < 50*time.Second: // INTERLUDE 1 — 35s of Running (minReadySeconds)
			return true
		case d < 60*time.Second: // redis-1 replaced
			return false
		case d < 90*time.Second: // INTERLUDE 2 — 30s of Running
			return true
		case d < finalSettleAt: // redis-0 (the master) + the post-fix down-after edge
			return false
		default:
			return true
		}
	}

	tr := &crSettleTracker{settleFor: settleFor}
	base := time.Unix(0, 0)

	var firstSettledAt time.Duration = -1
	for d := time.Duration(0); d <= 6*time.Minute; d += 5 * time.Second {
		phase, ready := "Initializing", "False"
		if running(d) {
			phase, ready = "Running", "True"
		}
		if tr.observe(phase, ready, base.Add(d)) && firstSettledAt < 0 {
			firstSettledAt = d
		}
	}

	if firstSettledAt < 0 {
		t.Fatalf("the tracker never reported settled; the instance was Running from %v onwards", finalSettleAt)
	}
	// The window may only open once the terminal Running run has itself lasted settleFor.
	// Anything earlier is a mid-flap window — the flake this guards.
	if want := finalSettleAt + settleFor; firstSettledAt < want {
		t.Errorf("tracker reported settled at %v, want no earlier than %v — "+
			"it opened the consistency window during a mid-roll Running interlude, "+
			"which is exactly the flake (LR-050: read Initializing 5.2s into the window)",
			firstSettledAt, want)
	}
}

// TestCRSettleTrackerResetsOnASingleBadSample pins the other half: one non-settled poll
// must discard the whole streak. Without it the tracker degrades into "Running for
// settleFor in total", which a flapping instance satisfies just as easily.
func TestCRSettleTrackerResetsOnASingleBadSample(t *testing.T) {
	tr := &crSettleTracker{settleFor: 30 * time.Second}
	base := time.Unix(0, 0)

	for d := time.Duration(0); d < 25*time.Second; d += 5 * time.Second {
		if tr.observe("Running", "True", base.Add(d)) {
			t.Fatalf("reported settled at %v, before settleFor elapsed", d)
		}
	}
	// One blip at t=25s, then Running again: the clock must restart from t=30s, so t=50s
	// is only 20s into the new streak.
	if tr.observe("Initializing", "False", base.Add(25*time.Second)) {
		t.Fatal("reported settled on an Initializing sample")
	}
	for _, d := range []time.Duration{30, 40, 50} {
		if tr.observe("Running", "True", base.Add(d*time.Second)) {
			t.Errorf("reported settled at t=%vs — the streak was not reset by the blip", d)
		}
	}
	if !tr.observe("Running", "True", base.Add(60*time.Second)) {
		t.Error("never reported settled at t=60s, 30s into an unbroken streak")
	}
}

// TestCRSettleTrackerRequiresReadyTrue pins that the settle asserts BOTH halves of the
// tier's claim. `Running` with `Ready=False` is not the settled state, and accepting it
// would silently weaken what tier 1 proves.
func TestCRSettleTrackerRequiresReadyTrue(t *testing.T) {
	tr := &crSettleTracker{settleFor: 10 * time.Second}
	base := time.Unix(0, 0)
	for d := time.Duration(0); d <= 60*time.Second; d += 5 * time.Second {
		if tr.observe("Running", "False", base.Add(d)) {
			t.Fatalf("reported settled at %v on Ready=False", d)
		}
	}
}

// TestRenameFixtureDoesNotOverrideMinReadySeconds is the drift guard for crSettleWindow.
//
// The settle window is a margin over the Redis StatefulSet's `minReadySeconds`, and that
// value is USER-SETTABLE (`resources.go` reads `lr.Spec.UpdateStrategy.MinReadySeconds`
// and only falls back to 35s). The window is therefore derived from what the fixture
// happens NOT to set, which is exactly the trap LR-049 and LR-050 record: a margin
// against a configurable timer is correct only for the configuration it was measured on.
// Deriving the window from the live StatefulSet instead would mean a cluster read on a
// path that must stay pure, so the cheap enforcement is here: raise
// `updateStrategy.minReadySeconds` in the fixture and this fails, naming the constant to
// revisit, rather than silently restoring the flake for the next reader to rediscover.
func TestRenameFixtureDoesNotOverrideMinReadySeconds(t *testing.T) {
	if got := sentinelRenameCR("drift-guard", "mymaster"); strings.Contains(got, "minReadySeconds") {
		t.Errorf("sentinelRenameCR now sets updateStrategy.minReadySeconds, but crSettleWindow (%s) "+
			"is derived from the resources.go DEFAULT of %s. Re-derive the window from the new value "+
			"before landing this — the settle must exceed minReadySeconds or tier 1's consistency "+
			"window can open during a mid-roll Running interlude again (LR-050).",
			crSettleWindow, crSettleMinReady)
	}
	if crSettleWindow <= crSettleMinReady {
		t.Errorf("crSettleWindow = %s, which does not exceed minReadySeconds = %s — "+
			"a mid-roll Running interlude can satisfy the settle", crSettleWindow, crSettleMinReady)
	}
}

// TestStaleMasterNameHasNoSuspicionReason is the local half of the tier-2 assertion above.
//
// That assertion reads `Equal("Foreign")` and its teeth are CLUSTER-ONLY: it can only
// fail against a running operator emitting a different reason, so there is no honest red
// for it here. What IS checkable without a cluster is the half that made the old
// `BeElementOf("Foreign", "ForeignSuspected")` dangerous — that the second reason still
// does not exist. LR-050 deleted it (the rollout gate closed §9.2's window at its source,
// taking the settle, its cooldown and `status.staleMasterNameForeignSince` with it), and
// an e2e that accepts a reason the product cannot emit would pass a regression
// reintroducing precisely the surface that was removed.
//
// It keys on a constant DECLARATION rather than the bare string, because
// `stale_master_name_plan.go` and `littlered_controller.go` both mention the name in
// historical comments explaining the removal — those must not trip the guard, and
// deleting them to satisfy it would erase the record of why the reason is gone.
func TestStaleMasterNameHasNoSuspicionReason(t *testing.T) {
	// Anchored to THIS source file, never to the process working directory.
	// test/utils.Run() calls os.Chdir(GetProjectDir()) and never restores it, so by
	// the time the plain Go tests run, any Ginkgo spec that shelled out has already
	// moved the process to the repo root — and a "../../" path then resolves one
	// level above the repo. The guard failed with a bare "no such file or directory"
	// in every full `make test-e2e` run and passed when run alone, which reads as a
	// flake and trains the reader to dismiss a guard that is supposed to be load-
	// bearing. A source-anchored path cannot be moved out from under it.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed; cannot locate the source tree")
	}
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "internal", "controller")
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("cannot read %s: %v", dir, err)
	}
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
			continue
		}
		src, err := os.ReadFile(dir + "/" + f.Name())
		if err != nil {
			t.Fatalf("cannot read %s: %v", f.Name(), err)
		}
		if strings.Contains(string(src), `= "ForeignSuspected"`) {
			t.Errorf("%s declares a ForeignSuspected reason again. LR-050 deleted it along with "+
				"staleMasterNameForeignCooldown and status.staleMasterNameForeignSince. If it is "+
				"genuinely coming back, revisit the tier-2 assertion on StaleMasterName's reason "+
				"(it now requires exactly \"Foreign\") before landing this.", f.Name())
		}
	}
}
