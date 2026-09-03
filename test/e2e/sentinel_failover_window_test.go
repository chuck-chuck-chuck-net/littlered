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

// LR-060 — the operator must keep managing an instance while Sentinel is reporting
// a failover that is going to take its full `failover_timeout`.
//
// THE MECHANISM, because the staging below is meaningless without it. Sentinel ends
// a failover in `sentinelFailoverDetectEnd`, which needs `not_reconfigured == 0`. A
// replica is counted unless it is PROMOTED, RECONF_DONE or S_DOWN, and the reconf
// ladder has exactly one rung with no timeout of its own:
//
//	RECONF_SENT   -> the leader sent SLAVEOF                  10s force-done
//	RECONF_INPROG -> the replica's INFO names the new master  NO TIMEOUT
//	RECONF_DONE   -> the replica's INFO says link up          --
//
// So a replica that ACCEPTS its SLAVEOF but whose link never comes up parks on the
// middle rung: not DONE (link down), not S_DOWN (it answers PING perfectly well),
// and outside the 10s escape, which is guarded on RECONF_SENT alone. The failover
// then runs to the whole-failover force-end — `failoverTimeout`, 180s by default.
//
// Measured on t3e 2026-09-03 against operator 47154d7: `reconf_slaves` for 179
// samples, the victim on `reconf_inprog` for 178, and Rule A suppressing ALL healing
// for 84 consecutive passes. A healthy failover on the same rig occupies ONE 1s
// sample, so this is not a slow failover — it is a stuck bookkeeping entry holding a
// report open, and the operator standing down for the whole of it.
//
// WHAT THIS TIER ASSERTS, and why it is Rule R specifically. During that window Rule
// R is the rule that matters: it points a pod at the consensus master, which is the
// same command at the same target Sentinel's own reconf_slaves is issuing, so it
// cannot fight the failover — and where the stuck link is fixable it is the only
// thing that can END the failover. Pre-fix it was unreachable, because Rule A returns
// above it.
//
// STAGING — every element is load-bearing:
//
//   - PRODUCT DEFAULTS for downAfterMilliseconds (30000) and failoverTimeout
//     (180000). Shortening either would hide the very cost this tier exists to
//     measure; at `failoverTimeout: 10000` the window closes before the operator
//     could plausibly be observed doing anything.
//   - `replica-priority` decides WHO gets promoted. compareSlavesForPromotion sorts
//     on slave_priority BEFORE offset and runid, so the victim is pinned OUT of the
//     promotion at 200 and the intended target sits at 100. Without this the first
//     attempt at this fixture promoted the victim itself, and there was no stuck
//     replica at all.
//   - A BOGUS `masterauth` on the victim, set BEFORE the failover. Against this
//     password-less instance the handshake then fails forever with `ERR Client sent
//     AUTH, but no password is set`, so `master_host` stays correct — the replica
//     reaches RECONF_INPROG — while the link can never come up. That is the rung,
//     staged exactly, and it is the same mechanism the LR-044 quarantine tiers use.
//   - `SENTINEL failover`, not a pod delete. No churn, no ghosts, no rollout: the
//     reconf mechanism in isolation, and `anyTerminating` stays false so Rule A's
//     FIRST clause cannot be what suppresses anything.
//   - A DELIBERATELY MIS-POINTED third pod, created after the promotion. Rule R does
//     not fire on the victim (its MasterHost is correct — LR-010 excludes link:down
//     alone), so without a genuinely wrong-mastered pod there is nothing for Rule R
//     to do and the tier would pass vacuously against any build.
var _ = Describe("Sentinel Failover Window", Label("sentinel"), Ordered, func() {
	const (
		victimOrdinal = 2 // pinned OUT of the promotion, holds the failover open
		strayOrdinal  = 0 // deliberately mis-pointed, the pod Rule R must repoint
	)

	var (
		crName     string
		masterName string
		newMaster  string
	)

	redisExec := func(ordinal int, args ...string) (string, error) {
		full := []string{"exec", fmt.Sprintf("%s-redis-%d", crName, ordinal), "-n", testNamespace,
			"-c", "redis", "--", "redis-cli"}
		full = append(full, args...)
		return utils.Run(exec.Command("kubectl", full...))
	}
	sentinelExec := func(args ...string) (string, error) {
		full := []string{"exec", crName + "-sentinel-0", "-n", testNamespace,
			"-c", "sentinel", "--", "redis-cli", "-p", "26379"}
		full = append(full, args...)
		return utils.Run(exec.Command("kubectl", full...))
	}
	podIP := func(ordinal int) string {
		out, err := utils.Run(exec.Command("kubectl", "get", "pod",
			fmt.Sprintf("%s-redis-%d", crName, ordinal), "-n", testNamespace,
			"-o", "jsonpath={.status.podIP}"))
		Expect(err).NotTo(HaveOccurred())
		return strings.TrimSpace(out)
	}
	infoField := func(ordinal int, field string) string {
		out, err := redisExec(ordinal, "info", "replication")
		if err != nil {
			return ""
		}
		for _, line := range strings.Split(strings.ReplaceAll(out, "\r", ""), "\n") {
			if strings.HasPrefix(line, field+":") {
				return strings.TrimSpace(strings.TrimPrefix(line, field+":"))
			}
		}
		return ""
	}
	failoverState := func() string {
		out, err := sentinelExec("sentinel", "master", masterName)
		if err != nil {
			return ""
		}
		lines := strings.Split(strings.ReplaceAll(out, "\r", ""), "\n")
		for i := 0; i < len(lines)-1; i++ {
			if strings.TrimSpace(lines[i]) == "failover-state" {
				return strings.TrimSpace(lines[i+1])
			}
		}
		return ""
	}

	AfterAll(func() {
		if debugOnFailure && suiteOrSpecFailed() {
			By("skipping cleanup to allow debugging")
			return
		}
		By("cleaning up")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", crName,
			"-n", testNamespace, "--ignore-not-found"))
	})

	BeforeAll(func() {
		crName = fmt.Sprintf("fw-%d", time.Now().Unix())
		masterName = e2eMasterName(testNamespace, crName)
		AddReportEntry("cr:" + crName)

		By("deploying a sentinel instance at PRODUCT DEFAULT failover timers")
		cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
  sentinel:
    masterName: %s
    quorum: 2
    downAfterMilliseconds: 30000
    failoverTimeout: 180000
    parallelSyncs: 1
`, crName, testNamespace, masterName)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the instance to serve")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.phase}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("keeps healing an instance whose failover is held open for a full failoverTimeout", func() {
		By("Step 1: pin the promotion target with replica-priority")
		// The victim must not be promoted, or there is no stuck replica at all.
		for ord := 0; ord < 3; ord++ {
			prio := "100"
			if ord == victimOrdinal {
				prio = "200"
			}
			_, err := redisExec(ord, "config", "set", "replica-priority", prio)
			Expect(err).NotTo(HaveOccurred())
		}
		By("waiting one sentinel_info_period so Sentinel reads the new priorities")
		time.Sleep(12 * time.Second)

		By("Step 2: make the victim's replication link unrecoverable")
		// A wrong masterauth against a password-less master: master_host stays
		// correct (so the replica reaches RECONF_INPROG) while every handshake
		// fails on AUTH forever (so it can never reach RECONF_DONE).
		_, err := redisExec(victimOrdinal, "config", "set", "masterauth", "definitely-wrong-password")
		Expect(err).NotTo(HaveOccurred())

		oldMasterIP := strings.TrimSpace(func() string {
			out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.master.podName}"))
			Expect(err).NotTo(HaveOccurred())
			return out
		}())
		AddReportEntry("master-before:" + oldMasterIP)

		By("Step 3: force a failover")
		_, err = sentinelExec("sentinel", "failover", masterName)
		Expect(err).NotTo(HaveOccurred())

		By("Step 4: PRECONDITION — the failover is genuinely stuck in reconf_slaves")
		// Asserted, not assumed: if this fixture ever stops producing the stuck rung
		// the assertion below would pass vacuously against any build.
		Eventually(failoverState, 90*time.Second, 2*time.Second).
			Should(Equal("reconf_slaves"), "the fixture must produce a failover held in reconf_slaves")
		Eventually(func() string { return infoField(victimOrdinal, "master_link_status") },
			60*time.Second, 2*time.Second).
			Should(Equal("down"), "the victim must be unable to complete its resync")

		By("Step 5: identify the promoted master")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.master.podName}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).NotTo(BeEmpty())
			g.Expect(strings.TrimSpace(out)).NotTo(Equal(oldMasterIP))
			newMaster = strings.TrimSpace(out)
		}, 90*time.Second, 2*time.Second).Should(Succeed())
		AddReportEntry("master-after:" + newMaster)

		By("Step 6: mis-point a third pod, the one Rule R must repoint")
		// Rule R does NOT fire on the victim: its MasterHost is correct and LR-010
		// excludes link:down alone. So a genuinely wrong-mastered pod is required, or
		// this tier proves nothing.
		Expect(fmt.Sprintf("%s-redis-%d", crName, strayOrdinal)).NotTo(Equal(newMaster),
			"the stray pod must not be the promoted master")
		strayTarget := podIP(victimOrdinal)
		_, err = redisExec(strayOrdinal, "slaveof", strayTarget, "6379")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() string { return infoField(strayOrdinal, "master_host") },
			30*time.Second, 2*time.Second).
			Should(Equal(strayTarget), "precondition: the stray pod is following the wrong master")

		By("Step 7: THE ASSERTION — the operator repoints it WHILE the failover is still reported")
		// Pre-fix this fails: Rule A returns above Rule R for the whole
		// failoverTimeout, so the stray pod stays mis-pointed for ~179s and this
		// Eventually times out. Post-fix Rule R runs during the window and corrects
		// it within a couple of reconcile passes.
		newMasterIP := ""
		for ord := 0; ord < 3; ord++ {
			if fmt.Sprintf("%s-redis-%d", crName, ord) == newMaster {
				newMasterIP = podIP(ord)
			}
		}
		Expect(newMasterIP).NotTo(BeEmpty())

		Eventually(func(g Gomega) {
			g.Expect(infoField(strayOrdinal, "master_host")).To(Equal(newMasterIP),
				"Rule R must repoint the stray pod at the consensus master")
			// The window must still be open, or we proved only that the operator
			// heals AFTER the failover ends — which it always did.
			g.Expect(failoverState()).To(Equal("reconf_slaves"),
				"the failover must still be reported while Rule R acts")
		}, 60*time.Second, 2*time.Second).Should(Succeed())

		By("Step 8: the promoted master was never demoted by the operator")
		// The `promoted` skip: in the seconds before the quorum's majority catches
		// up, the promoted pod reports role:master while RealMasterIP is still the
		// outgoing one, and an unguarded Rule R would SLAVEOF it back.
		Consistently(func() string {
			for ord := 0; ord < 3; ord++ {
				if fmt.Sprintf("%s-redis-%d", crName, ord) == newMaster {
					return infoField(ord, "role")
				}
			}
			return ""
		}, 20*time.Second, 2*time.Second).Should(Equal("master"),
			"the operator must never demote the pod Sentinel promoted")
	})
})
