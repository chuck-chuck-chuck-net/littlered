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

	sentinelCR := func(crName, masterNameLine string) string {
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
%s    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace, masterNameLine)
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
				cmd := exec.Command("kubectl", "exec", crName+"-sentinel-0", "-n", testNamespace,
					"-c", "sentinel", "--", "redis-cli", "-p", "26379",
					"SENTINEL", "get-master-addr-by-name", awkward)
				out, err := utils.Run(cmd)
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
