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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/littlered-operator/littlered-operator/test/utils"
)

// =============================================================================
// Declared operations, end to end (ADR-020 / LR-058)
// =============================================================================
//
// The mechanism under test: a bounded, documented set of HEAVY spec fields whose
// edit the operator records as an operation, carries out, and acknowledges ON
// COMPLETION — while standing down only the rules that would ASSIGN AUTHORITY.
// Registry v1 has exactly one member, `spec.sentinel.masterName`, whose driver is
// the code that already ships (Rule 0 + Rule N, LR-048).
//
// WHAT THESE TIERS ARE FOR, AND WHAT THEY ARE NOT. The decision itself —
// planOperation's ten rows and its three mutants — is pinned deterministically by
// `internal/controller/operation_plan_test.go`, and the wiring by the envtest tier.
// Nothing here re-litigates a row. What only a cluster can show is the ORDERING
// against a real StatefulSet rollout and a real Sentinel quorum: that the
// acknowledgment is withheld until the pods have actually settled, that an operator
// death in the middle loses nothing, and that the two ways this mechanism could
// detonate in the field — declaring an operation nobody asked for, and declaring one
// for every instance in a fleet the moment the operator is upgraded — do not happen.
//
// ASSERT THE ORDERING, NOT THE DURATIONS. LR-058 already measured this live: the
// driver reported `Converged` at T+4.6s while the acknowledgment was withheld for
// 148s until both StatefulSets settled. These tiers make that repeatable, so they
// assert the RELATION (driver done ⇒ still declared; ack ⇒ settled) and never a
// number. Every budget below is a generous upper bound taken from LR-048's measured
// rename trace (prune at t0+1.4s, sustained Running at t0+176.8s, plus ~30s at the
// redis-0 edge) and is not itself an assertion.
//
// MODE LABEL. Every tier here is sentinel mode and carries exactly one mode label,
// per test/e2e/mode_labels_test.go. The Stalled tier additionally carries
// `extended`: StallAfter is 15 minutes and is NOT configurable, so that tier cannot
// be made cheap and must not slow the default suite.

// heavyOpRename is the registry v1 operation name. It appears in status.operation,
// in status.acknowledgedOperations and in the condition, so the tiers spell it once.
//
// It is a deliberate DUPLICATE of the Go constant in
// internal/controller/operation_registry.go rather than an import: the e2e package
// cannot import the controller, and the name is an API surface (ADR-020 says
// renaming one is an API change), so pinning the string the CR actually carries is
// the right assertion. If the registry renames it, these tiers go red — which is
// the intended behaviour for an API change.
const heavyOpRename = "SentinelMasterNameRename"

// --- readers -----------------------------------------------------------------
//
// All four read the CR rather than the operator's log. The condition and
// status.operation ARE the mechanism's published surface (docs/USAGE.md tells an
// owner to read exactly these), so asserting on them is asserting the contract; a
// log line is an implementation detail that could be reworded without breaking a
// user.

// operationCondStatus returns the OperationInProgress condition's status ("True",
// "False", or "" when the condition is absent).
func operationCondStatus(crName string) string {
	st, _ := getConditionField(crName, "OperationInProgress", "status")
	return st
}

// operationCondReason returns the OperationInProgress condition's reason — one of
// Converged / Running / Blocked / Stalled / Quarantined / Seeded, or "" when absent.
func operationCondReason(crName string) string {
	r, _ := getConditionField(crName, "OperationInProgress", "reason")
	return r
}

// operationStatusField reads one field of status.operation. Empty when the whole
// object is absent, which is what "nothing is declared" looks like on the wire.
func operationStatusField(crName, field string) string {
	out, _ := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
		"-n", testNamespace, "-o", "jsonpath={.status.operation."+field+"}"))
	return strings.TrimSpace(out)
}

// renameAck returns the acknowledged fingerprint for the rename operation, or "" if
// there is no row.
//
// THE FINGERPRINT IS OPAQUE ON PURPOSE and these tiers treat it that way. It is an
// HMAC keyed on the instance UID precisely so that nothing can read it back as "the
// previous master name" (ADR-020: that is what structurally enforces ADR-018's
// refusal to remember one). So every assertion below is about whether the row
// CHANGED, never about what it says — which is also the only thing the mechanism
// promises.
func renameAck(crName string) string {
	jp := fmt.Sprintf("jsonpath={.status.acknowledgedOperations[?(@.name==%q)].fingerprint}", heavyOpRename)
	out, _ := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
		"-n", testNamespace, "-o", jp))
	return strings.TrimSpace(out)
}

// e2eStsSettled mirrors the operator's own statefulSetRolloutSettled
// (internal/controller/cluster_rollout.go) over one StatefulSet, from the API server.
//
// It is a DUPLICATE of a predicate the e2e package cannot import, and it is used for
// exactly one purpose: to assert the ORDER of two observable events (the ack landing,
// and the rollout finishing). It is never used to decide anything, so a small
// divergence would weaken an assertion rather than corrupt a verdict — and the
// clauses are copied verbatim from the source so a reader can diff them by eye.
//
// A StatefulSet that cannot be read counts as NOT settled, matching the operator's
// own conservative direction.
func e2eStsSettled(sts string) bool {
	out, err := utils.Run(exec.Command("kubectl", "get", "statefulset", sts, "-n", testNamespace,
		"-o", "jsonpath={.spec.replicas} {.metadata.generation} {.status.observedGeneration} "+
			"{.status.updateRevision} {.status.currentRevision} "+
			"{.status.updatedReplicas} {.status.readyReplicas} {.status.replicas}"))
	if err != nil {
		return false
	}
	f := strings.Fields(out)
	if len(f) != 8 {
		return false
	}
	want := f[0]
	return f[1] == f[2] && // observedGeneration == generation
		f[3] != "" && f[3] == f[4] && // updateRevision == currentRevision, non-empty
		f[5] == want && f[6] == want && f[7] == want
}

// instanceSettled is the operator's instanceStatefulSetsSettled: BOTH StatefulSets a
// sentinel instance owns. Both, and not just the Redis one — the Sentinel StatefulSet
// is what carries the master name the rename is about, so acknowledging while it is
// still being replaced would call the operation complete on pods that never ran under
// the new value.
func instanceSettled(crName string) bool {
	return e2eStsSettled(crName+"-redis") && e2eStsSettled(crName+"-sentinel")
}

// --- the tiers ---------------------------------------------------------------

var _ = Describe("Sentinel Declared Operations", Label("sentinel"), func() {

	// sweep counts how many of the written keys are missing or wrong. Byte-exact, not
	// DBSIZE and not a sample — LR-038's lesson is that a sampled counter cannot carry
	// a durability claim, and DBSIZE cannot tell a preserved dataset from a same-sized
	// wrong one.
	const sweep = `local bad=0 for i=1,tonumber(ARGV[1]) do ` +
		`if redis.call('GET','op:'..i) ~= 'v'..i then bad=bad+1 end end return bad`
	const keyCount = 200

	writeKeys := func(crName string) {
		master := getMasterPod(crName)
		Expect(master).NotTo(BeEmpty())
		_, err := redisExec(testNamespace, master, "EVAL",
			`for i=1,tonumber(ARGV[1]) do redis.call('SET','op:'..i,'v'..i) end return 1`,
			"0", strconv.Itoa(keyCount))
		Expect(err).NotTo(HaveOccurred())

		// LR-016's precondition lesson: a tier that disrupts an instance without first
		// establishing that the data reached the replicas can pass or fail for a reason
		// unrelated to what it claims to test.
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
	}

	expectKeysIntact := func(crName string) {
		Eventually(func(g Gomega) {
			m := getMasterPod(crName)
			g.Expect(m).NotTo(BeEmpty())
			bad, err := redisExec(testNamespace, m, "EVAL", sweep, "0", strconv.Itoa(keyCount))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(bad)).To(Equal("0"),
				"%s keys are missing or wrong on %s", strings.TrimSpace(bad), m)
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	}

	// expectQuietOperation is the steady state the mechanism must return to: nothing
	// declared. The condition is False and status.operation is absent entirely.
	expectQuietOperation := func(g Gomega, crName string) {
		g.Expect(operationCondStatus(crName)).NotTo(Equal("True"),
			"an operation is declared (%s) when none should be", operationCondReason(crName))
		g.Expect(operationStatusField(crName, "name")).To(BeEmpty(),
			"status.operation is populated when nothing should be declared")
	}

	// deploySettled brings up a healthy sentinel instance and waits until the
	// declared-operation mechanism has SEEDED it — i.e. the ack row exists, which is
	// what row 3 does on the first post-bootstrap pass. Every tier below starts from
	// this state, because "no ack row yet" and "an ack row that differs" are different
	// inputs and mixing them would make a tier's baseline ambiguous.
	deploySettled := func(crName, masterName string) string {
		AddReportEntry("cr:" + crName)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(sentinelRenameCR(crName, masterName))
		out, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "apply output: %s", out)

		Eventually(func(g Gomega) {
			g.Expect(getPhase(crName)).To(Equal("Running"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed(), "%s never reached Running", crName)

		var ack string
		Eventually(func(g Gomega) {
			ack = renameAck(crName)
			g.Expect(ack).NotTo(BeEmpty(),
				"the instance was never seeded: no acknowledgedOperations row for %s", heavyOpRename)
			expectQuietOperation(g, crName)
		}, 3*time.Minute, 3*time.Second).Should(Succeed())
		AddReportEntry("seeded ack for "+crName, ack)
		return ack
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

	// --- TIER 1: a rename runs as a declared operation ----------------------
	//
	// The headline claim, and it is an ORDERING claim: the acknowledgment lands only
	// after the StatefulSets settle, NOT when the driver converges. LR-058 measured
	// that gap at 148s live (driver `Converged` at T+4.6s), which is row 7 — the
	// transition guard — doing its whole job. Acknowledging at the driver's word would
	// hand the exit edge straight into the churn LR-050 is about.
	//
	// So the tier samples the two published surfaces together and asserts the relation
	// between them, never a duration:
	//
	//   (a) the operation is genuinely declared     — True/Running observed;
	//   (b) it is still declared while the instance
	//       is unsettled                            — the operation spans the rollout;
	//   (c) the DRIVER reports Converged while the
	//       operation is STILL declared             — row 7, the load-bearing one;
	//   (d) at the sample where the ack first
	//       changes, both StatefulSets are settled  — the ack waited for the pods.
	//
	// (c) and (d) are the two halves of "acknowledge on COMPLETION, not on
	// observation". A 2s sampler cannot miss (c): LR-048 measured the prune landing
	// 1.4s after the patch and the instance settling at +176.8s, so the window is
	// ~150-175s wide. If it ever were zero-length, row 7 would be inert and this tier
	// SHOULD complain — which is why (c) is asserted rather than merely reported.
	Context("Rename under a declared operation", Ordered, func() {
		var crName, newName, ack0 string
		const oldName = "mymaster"

		BeforeAll(func() {
			crName = fmt.Sprintf("op-rn-%d", time.Now().Unix())
			newName = e2eMasterName(testNamespace, crName)
			ack0 = deploySettled(crName, oldName)
		})

		AfterAll(func() { cleanup(crName) })

		It("declares the operation, withholds the ack until the pods settle, and keeps the data", func() {
			By("the baseline is quiet: seeded, nothing declared")
			Expect(ack0).NotTo(BeEmpty())
			Expect(operationCondStatus(crName)).To(Equal("False"))

			writeKeys(crName)

			By("renaming: kubectl patch spec.sentinel.masterName -> " + newName)
			renameMasterName(crName, newName)

			// One sampler, because every claim here is about an INTERLEAVING and no
			// single-pass read can see one. Sampled at 2s, well inside every window
			// LR-048 and LR-058 measured.
			var (
				sawDeclared   bool // (a)
				sawUnsettled  bool // (b)
				sawDriverDone bool // (c)
				settledAtAck  bool // (d)
				ackChanged    bool
				ackAt         time.Duration
				lastReason    string
				forsakenAt    string
			)
			start := time.Now()
			deadline := start.Add(10 * time.Minute)
			for time.Now().Before(deadline) {
				opSt, opReason := operationCondStatus(crName), operationCondReason(crName)
				staleSt, _ := getConditionField(crName, "StaleMasterName", "status")
				staleReason, _ := getConditionField(crName, "StaleMasterName", "reason")
				settled := instanceSettled(crName)
				ack := renameAck(crName)
				lastReason = opReason

				// K9 (LR-050): a healthy rename must never settle a capture verdict.
				// Asserted inside the sampler rather than after it, because the verdict
				// is transient by construction and a post-hoc read would miss it.
				if st, _ := getConditionField(crName, "Forsaken", "status"); st == "True" {
					forsakenAt = fmt.Sprintf("%s at +%s", operationCondReason(crName),
						time.Since(start).Truncate(time.Second))
				}

				if opSt == "True" {
					sawDeclared = sawDeclared || opReason == "Running"
					if !settled {
						sawUnsettled = true
					}
					// Rule N has converged — every Sentinel monitors exactly the desired
					// name — and the operation is STILL declared. That is row 7.
					if staleSt == "False" && staleReason == "Converged" {
						sawDriverDone = true
					}
				}
				if !ackChanged && ack != "" && ack != ack0 {
					ackChanged = true
					ackAt = time.Since(start).Truncate(time.Second)
					// RE-READ, deliberately: `settled` above was read SEVERAL kubectl
					// round-trips before `ack`, so the two are not simultaneous and pairing
					// them is a read skew. A soak run caught exactly that — the StatefulSet
					// was mid-roll when `settled` was read, settled a few hundred ms later,
					// and the operator acked before `renameAck` ran, producing
					// settledAtAck=false on a perfectly correct rename.
					//
					// Re-reading AFTER the ack is sound in the direction that matters, and
					// this is what the original comment claimed but did not do. Nothing
					// rolls once the operation completes, so a post-ack read can show
					// settled only if it genuinely settled; and a product that acked EARLY
					// would still be mid-roll here, so the real red is preserved. The
					// asymmetry is the point: the old code was safe against a stale-SETTLED
					// read (which would hide a defect) and open to a stale-UNSETTLED one
					// (which invents one).
					settledAtAck = instanceSettled(crName)
					break
				}
				time.Sleep(2 * time.Second)
			}

			AddReportEntry("operation sampler", fmt.Sprintf(
				"declared=%v spannedRollout=%v driverDoneWhileDeclared=%v ackChanged=%v at=%s settledAtAck=%v lastReason=%q",
				sawDeclared, sawUnsettled, sawDriverDone, ackChanged, ackAt, settledAtAck, lastReason))

			Expect(forsakenAt).To(BeEmpty(),
				"the operator declared this instance FORSAKEN (%s) during an ordinary rename — "+
					"there is no other Sentinel deployment here, so this is LR-050's false positive, "+
					"and a quarantine deletes the pods on EmptyDir", forsakenAt)

			// ORDER MATTERS HERE, and it was chosen from an observed mutant run. The
			// acknowledge-on-sight mutant fails EVERY one of these, and the first one
			// to fire is the message a future reader gets — so the sharpest claim is
			// asserted first. Assert sawDeclared first and the report reads "the edit
			// was not recognised as a heavy operation at all", which is a true
			// observation and the wrong diagnosis: the edit WAS recognised, and then
			// acknowledged in the same pass, so the declaration existed for one
			// reconcile interval instead of for the whole rollout. (LR-047 Addendum 2:
			// a message that describes the wrong thing sends the reader to the wrong
			// place.)
			Expect(ackChanged).To(BeTrue(),
				"the acknowledgment never changed within 10 minutes; the rename never completed")
			Expect(settledAtAck).To(BeTrue(),
				"the acknowledgment landed at +%s while a StatefulSet was still rolling. "+
					"Row 7 was not enforced and the exit edge was handed into the churn LR-050 "+
					"is about. With declared=%v driverDone=%v and an ack at +%s this is the "+
					"ACKNOWLEDGE-ON-SIGHT signature: the record says the work is finished at "+
					"the moment it is noticed, not when it is done (D1)",
				ackAt, sawDeclared, sawDriverDone, ackAt)
			Expect(sawDeclared).To(BeTrue(),
				"OperationInProgress never went True/Running for the rename — the edit was "+
					"either not recognised as a declared heavy operation, or it was recognised "+
					"and acknowledged in the same pass, so the declaration never outlived one "+
					"reconcile interval")
			Expect(sawUnsettled).To(BeTrue(),
				"the operation was never observed while the instance was unsettled, so this run "+
					"proves nothing about the transition guard")
			Expect(sawDriverDone).To(BeTrue(),
				"never observed StaleMasterName=False/Converged while OperationInProgress was still "+
					"True — the driver's completion and the operation's completion were "+
					"indistinguishable, which is exactly what row 7 exists to separate (D1: "+
					"acknowledge on COMPLETION, not on observation)")

			By("the operation goes quiet and the rename converged")
			Eventually(func(g Gomega) {
				expectQuietOperation(g, crName)
				g.Expect(operationCondReason(crName)).To(Equal("Converged"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("every Sentinel monitors exactly " + newName)
			Eventually(func(g Gomega) {
				for _, sp := range []string{crName + "-sentinel-0", crName + "-sentinel-1", crName + "-sentinel-2"} {
					out, err := sentinelPortExec(testNamespace, sp, "SENTINEL", "masters")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(sentinelMasterNames(out)).To(Equal([]string{newName}),
						"%s monitors %v", sp, sentinelMasterNames(out))
				}
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("every written key survived — swept exactly")
			expectKeysIntact(crName)
		})
	})

	// --- TIER 2: a non-heavy edit declares nothing --------------------------
	//
	// This is the tier that pins ADR-020's rejection of Alternative C
	// (`generation != observedGeneration`). That mechanism is 100% accurate for "some
	// spec change is unreconciled" and free, and it is REJECTED because it cannot tell
	// a masterName change from a resources change. Editing `spec.resources` bumps the
	// CR's generation and rolls both StatefulSets — the full churn a coarse mechanism
	// would read as an operation — and nothing here may declare one.
	//
	// "Healing is never suppressed" is asserted through the condition rather than
	// through the operator's log, and that is a derivation rather than a proxy: the
	// suppression in reconcileSentinelCluster is `operationRunning := opPlan.Run != ""`,
	// and Run != "" is exactly the state that publishes True/Running. A condition that
	// never goes True is a suppression that never happened.
	Context("A non-heavy field edit declares nothing", Ordered, func() {
		var crName, ack0 string

		BeforeAll(func() {
			crName = fmt.Sprintf("op-nh-%d", time.Now().Unix())
			ack0 = deploySettled(crName, e2eMasterName(testNamespace, crName))
		})

		AfterAll(func() { cleanup(crName) })

		It("rolls the instance without ever declaring an operation", func() {
			writeKeys(crName)

			By("editing spec.resources — a real, rolling, NON-heavy change")
			// 128Mi -> 192Mi on both request and limit, which keeps pillar 3.3's
			// limits==requests posture intact while guaranteeing a pod-template change.
			patch := `{"spec":{"resources":{"requests":{"memory":"192Mi"},"limits":{"memory":"192Mi"}}}}`
			out, err := utils.Run(exec.Command("kubectl", "patch", "littlered", crName,
				"-n", testNamespace, "--type=merge", "-p", patch))
			Expect(err).NotTo(HaveOccurred(), "patch output: %s", out)

			By("the edit really reached the workload — otherwise this tier asserts a non-event")
			// The positive control. Without it, "no operation was declared" would pass
			// just as happily against an edit that never touched anything.
			Eventually(func(g Gomega) {
				mem, err := utils.Run(exec.Command("kubectl", "get", "statefulset", crName+"-redis",
					"-n", testNamespace, "-o",
					"jsonpath={.spec.template.spec.containers[?(@.name=='redis')].resources.limits.memory}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(mem)).To(Equal("192Mi"))
			}, 2*time.Minute, 3*time.Second).Should(Succeed())

			By("and no operation is declared for the whole rollout")
			// 4 minutes covers the measured full three-pod rename rollout (LR-048:
			// sustained Running at t0+176.8s) with margin, so the window spans the
			// entire period a generation-keyed mechanism would have been reporting one.
			Consistently(func(g Gomega) {
				expectQuietOperation(g, crName)
				g.Expect(renameAck(crName)).To(Equal(ack0),
					"the acknowledgment moved on a non-heavy edit; the fingerprint is keyed on "+
						"the wrong thing")
			}, 4*time.Minute, 5*time.Second).Should(Succeed())

			By("the instance converges under its own healing, which was never stood down")
			Eventually(func(g Gomega) {
				g.Expect(getPhase(crName)).To(Equal("Running"))
				st, _ := getConditionField(crName, "Ready", "status")
				g.Expect(st).To(Equal("True"))
				expectQuietOperation(g, crName)
			}, 8*time.Minute, 5*time.Second).Should(Succeed())

			expectKeysIntact(crName)
		})
	})

	// --- TIER 3: the operator dies mid-rename -------------------------------
	//
	// D1's central claim, end to end: the record says "the work this fingerprint
	// stands for is FINISHED", so it survives operator death and is idempotent across
	// restarts. Acknowledging on OBSERVATION instead fails the mechanism's own 100%
	// bar in the forbidden direction — the operator writes the ack, dies before acting,
	// and the intent is lost silently with nothing left to say work is outstanding.
	//
	// The load-bearing assertion is the middle one: while the operator is DOWN, the
	// acknowledgment must still be the pre-rename value. Against an
	// acknowledge-on-observation build the ack has already moved by then, and this
	// tier goes red on the one line that distinguishes the two designs.
	//
	// The operator is stopped only AFTER the operation has been observed declared,
	// rather than after a blind sleep: that makes the kill land inside the window the
	// tier is about, deterministically, instead of racing the first reconcile pass.
	Context("Operator killed mid-rename", Ordered, func() {
		var crName, newName, ack0 string
		const oldName = "mymaster"

		BeforeAll(func() {
			crName = fmt.Sprintf("op-kill-%d", time.Now().Unix())
			newName = e2eMasterName(testNamespace, crName)
			ack0 = deploySettled(crName, oldName)
		})

		AfterAll(func() {
			// Unconditionally FIRST, before any early return: an operator left at 0
			// replicas silently breaks every later spec and the next run of the whole
			// suite. Mirrors the reshard and isolation tiers' discipline.
			scaleOperator(1)
			cleanup(crName)
		})

		It("resumes the operation after the operator returns, and completes it", func() {
			writeKeys(crName)

			By("renaming to " + newName)
			renameMasterName(crName, newName)

			By("waiting until the operator has actually declared the operation")
			// The ack is read alongside so that a TIMEOUT here can say why. Against an
			// acknowledge-on-observation build the operation is declared and
			// acknowledged in the same pass, so this times out having never caught the
			// declaration — and without the ack in the message that reads as "the edit
			// was not recognised", which is the wrong diagnosis. Observed exactly that.
			Eventually(func(g Gomega) {
				g.Expect(operationCondStatus(crName)).To(Equal("True"),
					"reason=%q ack-already-moved=%v — if the ack has ALREADY moved then the "+
						"operation was acknowledged on OBSERVATION rather than on completion, "+
						"and the declaration never outlived one reconcile interval (D1)",
					operationCondReason(crName), renameAck(crName) != ack0)
				g.Expect(operationCondReason(crName)).To(Equal("Running"))
				g.Expect(operationStatusField(crName, "name")).To(Equal(heavyOpRename))
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By("killing the operator mid-rename")
			scaleOperator(0)

			// ---- THE ASSERTION THIS TIER EXISTS FOR --------------------------
			//
			// With no operator running the CR status is frozen, so what this window
			// measures is what the operator had PERSISTED at the moment it died. Under
			// acknowledge-on-completion that is: the operation still declared, and the
			// acknowledgment still pointing at the pre-rename value — i.e. the record
			// says there is unfinished work, which is what makes the resume possible.
			// Under acknowledge-on-observation the ack has already moved and the intent
			// is gone; this Consistently is where that build fails.
			By("the operation is still declared and NOTHING was acknowledged")
			Consistently(func(g Gomega) {
				g.Expect(operationCondStatus(crName)).To(Equal("True"),
					"the operation was retracted while the operator was down")
				g.Expect(renameAck(crName)).To(Equal(ack0),
					"the rename was acknowledged BEFORE it completed — the operator died holding "+
						"a record that says the work is finished when it is not (D1: acknowledge "+
						"on COMPLETION, never on observation)")
			}, 45*time.Second, 5*time.Second).Should(Succeed())

			By("bringing the operator back")
			scaleOperator(1)

			By("the operation resumes and completes: exactly one monitored name")
			Eventually(func(g Gomega) {
				for _, sp := range []string{crName + "-sentinel-0", crName + "-sentinel-1", crName + "-sentinel-2"} {
					out, err := sentinelPortExec(testNamespace, sp, "SENTINEL", "masters")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(sentinelMasterNames(out)).To(Equal([]string{newName}),
						"%s monitors %v", sp, sentinelMasterNames(out))
				}
			}, 6*time.Minute, 5*time.Second).Should(Succeed())

			By("and the acknowledgment finally lands, on completion")
			Eventually(func(g Gomega) {
				ack := renameAck(crName)
				g.Expect(ack).NotTo(BeEmpty())
				g.Expect(ack).NotTo(Equal(ack0), "the acknowledgment never moved to the new value")
				g.Expect(instanceSettled(crName)).To(BeTrue(),
					"the ack landed while a StatefulSet was still rolling")
				expectQuietOperation(g, crName)
			}, 8*time.Minute, 5*time.Second).Should(Succeed())

			By("no capture verdict was settled across the outage")
			st, _ := getConditionField(crName, "Forsaken", "status")
			Expect(st).NotTo(Equal("True"))

			expectKeysIntact(crName)
		})
	})

	// --- TIER 5: Stalled --------------------------------------------------
	//
	// `StallAfter` is 15 minutes for the rename and is NOT configurable, so this tier
	// is unavoidably long and carries `extended`. The default suite runs
	// `--ginkgo.label-filter='!extended'`, so it is opt-in via `make test-e2e-all` or
	// `LABEL_FILTER=extended`.
	//
	// The stall is manufactured the way ADR-020's own plan suggests: make the
	// replacement pod unschedulable, so the StatefulSets never settle and row 7 keeps
	// the operation Running past its budget. One patch changes BOTH the master name and
	// the nodeSelector, which is legal — the CEL transition rule counts CHANGED HEAVY
	// fields, and nodeSelector is not one.
	//
	// The three claims, and the second is the whole point:
	//
	//   1. it reports Stalled after StallAfter;
	//   2. there is NO AUTO-EXIT — it stays Stalled, the ack never lands, and the
	//      operator does not "give up and proceed" (ADR-017: a timer would be the
	//      defect with a delay);
	//   3. no data action is taken — the surviving pods keep serving, the dataset is
	//      intact, and no quarantine is armed.
	//
	// The instance stays AVAILABLE throughout, which is the intended failure direction:
	// a StatefulSet rolls in reverse-ordinal order, so only the highest ordinal is
	// stranded Pending and the master keeps serving.
	Context("Stalled", Ordered, Label("extended"), func() {
		var crName, newName, ack0 string
		const oldName = "mymaster"

		BeforeAll(func() {
			crName = fmt.Sprintf("op-stall-%d", time.Now().Unix())
			newName = e2eMasterName(testNamespace, crName)
			ack0 = deploySettled(crName, oldName)
		})

		AfterAll(func() { cleanup(crName) })

		It("reports Stalled after the budget, never auto-exits, and touches no data", func() {
			writeKeys(crName)
			masterBefore := getMasterPod(crName)
			Expect(masterBefore).NotTo(BeEmpty())

			By("renaming AND wedging the rollout in one apply")
			// One patch, deliberately: two applies would work equally well here, but a
			// single one also demonstrates that the admission rule counts changed HEAVY
			// fields rather than changed fields.
			patch := fmt.Sprintf(
				`{"spec":{"sentinel":{"masterName":%q},"podTemplate":{"nodeSelector":`+
					`{"littlered.e2e/unschedulable":"true"}}}}`, newName)
			out, err := utils.Run(exec.Command("kubectl", "patch", "littlered", crName,
				"-n", testNamespace, "--type=merge", "-p", patch))
			Expect(err).NotTo(HaveOccurred(), "patch output: %s", out)

			By("the rollout really is wedged — otherwise the stall would prove nothing")
			// The positive control: a pod must actually be stuck Pending. Without it a
			// Stalled report could come from any slow path and the tier would not know.
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", testNamespace,
					"-l", "app.kubernetes.io/instance="+crName,
					"-o", "jsonpath={range .items[*]}{.metadata.name}={.status.phase} {end}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("=Pending"),
					"no pod is Pending, so the rollout is not wedged: %s", out)
			}, 4*time.Minute, 5*time.Second).Should(Succeed())

			By("the operation is declared and runs")
			Eventually(func(g Gomega) {
				g.Expect(operationCondStatus(crName)).To(Equal("True"))
				g.Expect(operationStatusField(crName, "name")).To(Equal(heavyOpRename))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("and after StallAfter (15m) it reports Stalled")
			// 15m budget + the reconcile that notices it + generous margin. The reason
			// is EXACT: row 10 is checked before rows 7 and 9, so a wedged rollout past
			// the budget must read Stalled and not Running or Blocked.
			Eventually(func(g Gomega) {
				g.Expect(operationCondReason(crName)).To(Equal("Stalled"))
				g.Expect(operationStatusField(crName, "reason")).To(Equal("Stalled"))
			}, 22*time.Minute, 15*time.Second).Should(Succeed())

			By("NO AUTO-EXIT: it stays Stalled, unacknowledged, and takes no data action")
			// The whole claim is a NON-EVENT, so the window has to be long enough that a
			// hypothetical auto-exit timer would have fired inside it. There is no such
			// timer by design, so any length is arbitrary; 3 minutes is 12% of the budget
			// that has already elapsed and comfortably more than the operator's own
			// cadence.
			Consistently(func(g Gomega) {
				g.Expect(operationCondReason(crName)).To(Equal("Stalled"),
					"the operation left Stalled on its own — there must be no auto-exit")
				g.Expect(operationCondStatus(crName)).To(Equal("True"))
				g.Expect(renameAck(crName)).To(Equal(ack0),
					"the stalled operation was acknowledged; work that never completed was "+
						"recorded as finished")

				// No data action: the quarantine never armed, and the pods that were
				// serving are still there.
				qs, _ := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.quarantinedSince}"))
				g.Expect(strings.TrimSpace(qs)).To(BeEmpty(), "a quarantine was armed on a stalled operation")
				fst, _ := getConditionField(crName, "Forsaken", "status")
				g.Expect(fst).NotTo(Equal("True"))
				for _, p := range []string{crName + "-redis-0", crName + "-redis-1"} {
					_, err := utils.Run(exec.Command("kubectl", "get", "pod", p,
						"-n", testNamespace, "-o", "jsonpath={.metadata.name}"))
					g.Expect(err).NotTo(HaveOccurred(), "%s was deleted during a stalled operation", p)
				}
			}, 3*time.Minute, 10*time.Second).Should(Succeed())

			By("and the dataset is untouched")
			// Swept against the CURRENT master rather than the pod that held the role
			// before the patch. A StatefulSet rolls in reverse ordinal, so the wedged
			// replacement is the HIGHEST ordinal and the master — seeded as redis-0 at
			// bootstrap — is never reached; that is why masterBefore survived every
			// observed run. But it is a property of the seeding, not an invariant of
			// this tier, and pinning the sweep to a pod that a failover could have
			// moved would make the assertion fail for a reason that has nothing to do
			// with the stall.
			expectKeysIntact(crName)
			Expect(masterBefore).NotTo(BeEmpty())
		})
	})
})

// =============================================================================
// TIER 4 — a quarantined instance advances nothing
// =============================================================================
//
// Row 1 of planOperation, and it exists because of a trap that is easy to walk into:
// a `replicas: 0` StatefulSet reads SETTLED. So an operation allowed to proceed over
// a quarantined instance would satisfy row 7's completion condition and acknowledge
// work that NO POD EVER EXECUTED — the acknowledge-on-sight failure arriving through
// a side door.
//
// The other half of the row is LR-054's standing requirement: anything this mechanism
// withholds must SAY that it withheld. So a pending change over a quarantined instance
// is not dropped and not run — it is reported, with reason `Quarantined`, which is
// exactly what docs/USAGE.md promises an owner ("recorded and held, not run... picked
// up once the instance is released and serving again"). Both halves are asserted here.
//
// ============================ DELIBERATELY AUTH-FREE ==========================
//
// This tier stages a REAL capture, by PUBLISHing a hello at the victim's sentinel
// port. With `requirepass` set that connection answers NOAUTH before the payload
// reaches sentinelProcessHelloMessage(), so the capture would never land and every
// assertion below would silently degrade into asserting a non-event. Same reasoning,
// verbatim, as the two capture-staging Describes this borrows the recipe from.
//
// The shared name is a SCOPED one rather than `mymaster` on purpose: `mymaster` plus
// auth-off is quarantineConfigDangerous, which drops the attempt budget to 1 and
// LATCHES on the first quarantine — which would make the release half of this tier
// unobservable.
// ==============================================================================
var _ = Describe("Sentinel Declared Operations Under Quarantine", Label("sentinel"), Ordered, func() {
	var captor, victim, sharedName, newName, ack0 string

	// The capture machinery is DUPLICATED from sentinel_quarantine_test.go rather than
	// hoisted, following the precedent that file set (and documents) when it duplicated
	// from the isolation Describe: those fixtures carry load-bearing warning comments
	// about the operator being paused and about avoiding status assertions, and a
	// mechanical refactor of them does not belong in the change that adds a fourth
	// consumer. What is reused is the KNOWLEDGE, including both of LR-044's staging
	// findings — the precondition is asserted over all three Sentinels BEFORE any
	// injection, and the PUBLISH reply is asserted to be `1`.
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
	forsakenReasonOf := func(crName string) string {
		r, _ := getConditionField(crName, "Forsaken", "reason")
		return r
	}
	quarantinedSince := func(crName string) string {
		out, _ := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
			"-n", testNamespace, "-o", "jsonpath={.status.quarantinedSince}"))
		return strings.TrimSpace(out)
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

	capture := func() string {
		By("reading the captor's live master address")
		captorMasters, err := sentinelCmd(captor+"-sentinel-0", "SENTINEL", "masters")
		Expect(err).NotTo(HaveOccurred())
		Expect(sentinelField(captorMasters, "name")).To(Equal(sharedName))
		foreign := sentinelField(captorMasters, "ip")
		Expect(foreign).NotTo(BeEmpty())
		AddReportEntry("foreign master (captor's)", foreign)

		By("asserting the precondition over ALL THREE of the victim's Sentinels first")
		for _, sp := range sentinelPodsOf(victim) {
			out, err := sentinelCmd(sp, "SENTINEL", "masters")
			Expect(err).NotTo(HaveOccurred())
			Expect(sentinelField(out, "ip")).NotTo(Equal(foreign),
				"%s already monitors the foreign master before any injection", sp)
		}

		By("injecting a hello for the captor's master into all three")
		injected := 0
		for _, sp := range sentinelPodsOf(victim) {
			before, err := sentinelCmd(sp, "SENTINEL", "masters")
			Expect(err).NotTo(HaveOccurred())
			if sentinelField(before, "ip") == foreign {
				// Sentinel propagates a higher-epoch config to its peers in its own
				// hellos, so a peer may already have converged (LR-044 observed exactly
				// this). Skipping it is correct; asserting about it is a race.
				AddReportEntry("converged before injection", sp)
				continue
			}
			epoch := nextEpoch(before)
			hello := fmt.Sprintf("%s,26379,%s,%d,%s,%s,6379,%d",
				podIP(captor+"-sentinel-0"),
				"ca7e0000000000000000000000000000deadbee3",
				epoch, sharedName, foreign, epoch)
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
				g.Expect(sentinelField(out, "ip")).To(Equal(foreign),
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

		return foreign
	}

	BeforeAll(func() {
		stamp := time.Now().Unix()
		captor = fmt.Sprintf("op-q-captor-%d", stamp)
		victim = fmt.Sprintf("op-q-victim-%d", stamp)
		sharedName = fmt.Sprintf("opq.shared.%d", stamp)
		// See the rename step for why this is the legacy name and not a scoped one.
		newName = "mymaster"
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
		Eventually(func(g Gomega) {
			ack0 = renameAck(victim)
			g.Expect(ack0).NotTo(BeEmpty(), "the victim was never seeded")
		}, 3*time.Minute, 3*time.Second).Should(Succeed())
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

	It("holds a pending rename and reports it as Quarantined", func() {
		capture()

		By("the operator quarantines the victim")
		Eventually(func(g Gomega) {
			g.Expect(quarantinedSince(victim)).NotTo(BeEmpty())
			g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("0"))
			g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("0"))
		}, 5*time.Minute, 3*time.Second).Should(Succeed())

		// ---- THE RENAME LANDS ON AN INSTANCE WITH NO PODS ------------------
		//
		// THE TARGET NAME IS THE LEGACY `mymaster`, AND THAT IS A DELIBERATE DEVICE
		// RATHER THAN A SCENARIO. The claim under test — a quarantined instance
		// advances nothing — is indifferent to WHICH name is asked for; what it needs
		// is a window long enough to be evidence. A scoped target leaves the attempt
		// budget at 2, so the quarantine releases 120s after arming and every
		// assertion here races that timer (a first draft did, and it also opened its
		// Consistently 0.096s after the patch, before the operator could possibly have
		// reconciled — LR-050's own e2e flake, repeated).
		//
		// Auth is off in this fixture, so renaming TO `mymaster` makes the effective
		// name the legacy shared one, which is `quarantineConfigDangerous`: the budget
		// drops to 1, the single spent attempt LATCHES, and the instance stays at zero
		// replicas indefinitely. The window becomes unbounded. This is the same device
		// the Latched tier in sentinel_quarantine_test.go uses and documents, for the
		// same reason.
		By("renaming the quarantined instance to " + newName)
		renameMasterName(victim, newName)

		By("the held change is REPORTED rather than dropped, and the latch engages")
		// Eventually first, because this is a TRANSITION: the operator has to observe
		// the patch, and while Forsaken holds it is polled at the STEADY interval
		// (LR-045), so the state cannot be read the instant kubectl returns.
		Eventually(func(g Gomega) {
			g.Expect(operationCondStatus(victim)).To(Equal("True"),
				"a pending heavy change over a quarantined instance must be REPORTED, not "+
					"silently withheld (LR-054: anything this mechanism withholds must say so)")
			g.Expect(operationCondReason(victim)).To(Equal("Quarantined"))
			g.Expect(operationStatusField(victim, "name")).To(Equal(heavyOpRename))
			g.Expect(forsakenReasonOf(victim)).To(Equal("QuarantineLatched"),
				"the latch did not engage, so the window below would race the 120s settle")
		}, 3*time.Minute, 3*time.Second).Should(Succeed())

		By("and NOTHING advances, held past the timer that would otherwise have released it")
		// 150s outlasts quarantineSettlePeriod (120s) plus a steady interval of
		// granularity, so this is not merely "nothing happened yet": it is "nothing
		// happened across the whole window in which the release would have fired".
		Consistently(func(g Gomega) {
			g.Expect(operationCondReason(victim)).To(Equal("Quarantined"))

			// The trap this row exists for: a replicas:0 StatefulSet reads SETTLED, so a
			// mechanism that let the operation proceed here would satisfy row 7's
			// completion condition and acknowledge work no pod ever executed.
			g.Expect(renameAck(victim)).To(Equal(ack0),
				"the rename was acknowledged on an instance with ZERO pods — no pod ever "+
					"carried it out")

			// Re-assert the staged precondition, so a green cannot be earned by the
			// quarantine quietly ending.
			g.Expect(quarantinedSince(victim)).NotTo(BeEmpty())
			g.Expect(stsSpecReplicas(victim + "-redis")).To(Equal("0"))
			g.Expect(stsSpecReplicas(victim + "-sentinel")).To(Equal("0"))
		}, 150*time.Second, 5*time.Second).Should(Succeed())

		// ---- WHAT THIS TIER DELIBERATELY DOES NOT ASSERT — LR-059 ----------
		//
		// docs/USAGE.md promises the held change "is picked up once the instance is
		// released and serving again", and a first draft of this tier asserted exactly
		// that. **The product does not deliver it, and that is the defect this tier
		// found**: on release the instance comes back leaderless with bare Sentinels,
		// the pending rename is picked up IMMEDIATELY (it is no longer quarantined),
		// and Rule L — the only thing that can give it a master — is in the suppressed
		// set. So the pods never become Ready, the StatefulSets never settle, row 7
		// never acknowledges, and the operation runs forever.
		//
		// Measured, and NOT specific to the quarantine: a hand-staged leaderless
		// instance with a pending rename sat wedged for 7m56s with zero leaderless
		// lines in the operator log, while an identically-staged instance with no
		// rename recovered in 74s, and reverting the rename recovered the first in 84s.
		// The deterministic reproduction and its positive control are the
		// committed-and-skipped tier below. Re-add the pickup assertion here WITH the
		// LR-059 fix — asserting it now would be asserting a defect-free world.
	})
})

// =============================================================================
// LR-059 — a pending heavy operation on a LEADERLESS instance wedges forever
// =============================================================================
//
// ⚠ COMMITTED AND SKIPPED. This is a live product defect found by the tier above,
// deferred by decision rather than fixed here, and it follows the LR-056/LR-057
// precedent exactly: the reproduction is committed so the fix has a red to turn
// green, and it is NOT inverted into a characterisation of the current behaviour —
// a test that asserts the defect is correct has to be un-written by whoever fixes
// it, and until then it actively defends the defect.
//
// THE FINDING. ADR-020 states the rule that this violates, in its own words:
//
//	"an operation must never suppress the healing its own completion condition
//	 depends on"
//
// LR-058 generalized that for Rule R (measured 311s against 162s) and stopped
// there. It does not hold for **Rule L**, and Rule L is in the suppressed set on
// purpose, because Rule L assigns authority. The authority boundary is right as a
// SAFETY rule and it opens a LIVENESS hole in exactly the case where there is no
// authority to protect: with zero data holders, Rule L's no-data reseed is the only
// thing that can produce the living master the operation's completion depends on.
//
// The loop, every step of it a documented decision working as designed:
//
//	rename pending  ⇒ operation Running
//	              ⇒ Rule L suppressed (it assigns authority)
//	              ⇒ no master, pods park in the startup wait-loop, never Ready
//	              ⇒ StatefulSets never settle
//	              ⇒ row 7 withholds the acknowledgment
//	              ⇒ the operation stays Running  ⇒ Rule L stays suppressed
//
// MEASURED ON t3e (2026-09-01, operator 48120e9), one variable, three directions:
//
//	ctrl  — leaderless, NO rename pending        recovered in 74s
//	        ("Leaderless bootstrap deadlock suspected" -> "seeded ctrl-redis-0")
//	diag  — leaderless, rename pending           WEDGED 7m56s, still going
//	        (only "A declared heavy operation is in progress" every ~4s;
//	         ZERO leaderless lines — Rule L never even started its cooldown)
//	diag  — the rename REVERTED                  recovered in 84s
//	        (op False/Converged in 10s, then DeadlockDetected -> Reseeded)
//
// SEVERITY: availability, not durability. The instance is empty by construction on
// the observed path, and an instance that DID hold data is protected by Rule L's own
// >=2-holder refusal anyway. It is loud (Ready=False, phase Initializing,
// OperationInProgress=True) but it never reaches `Stalled` for 15 minutes, and
// `Stalled` has no auto-exit either. The escape hatch is to revert the spec edit,
// which is exactly the operation the owner was trying to perform.
//
// REACHABLE, and by a route the documentation invites: any leaderless deadlock that
// coincides with a pending rename. The quarantine release above is one route (and
// USAGE tells owners a held rename is picked up there); LR-015's original
// mass-restart incident is another, and a rename issued shortly before node
// maintenance composes the two.
//
// FIXED 2026-09-04 (LR-059), and NOT by the narrowing this header proposed. The fix is
// the criterion under ADR-020's boundary: because a suppression has no auto-exit, a rule
// may be stood down by an operation only if the instance can still reach a SETTLED state
// with that rule permanently absent. Rule L fails that here, and since both
// authority-assigning rules require `RealMasterIP == ""`, the gate's whole domain of
// effect was this branch — so sentinel mode's suppressed set is now empty.
//
// TWO TIERS, because the run against the fixed operator found a SECOND cycle underneath
// this one (**LR-061**): pods left on the OLD pod template ask Sentinel for the OLD
// master name, which Rule N has correctly pruned, so they never start, never become
// Ready, and the rolling update that would re-bake them cannot advance. The first tier
// below therefore asserts exactly what LR-059 delivers (a master, from Rule L's reseed,
// named in the CR) and stops; the second stages the IN-SCOPE ordering — rename first, so
// the pods return on the current template, which is what ADR-016's quarantine release
// produces — and asserts full recovery plus the rename completing.
var _ = Describe("Sentinel Declared Operations On A Leaderless Instance",
	Label("sentinel"), Ordered, func() {

		var wedged, control, inScope string

		leaderlessCR := func(crName string) string {
			return sentinelRenameCR(crName, "mymaster")
		}

		// forceLeaderless reproduces LR-015's deadlock deterministically: with the
		// operator paused, force-delete every pod. Sentinel storage is EmptyDir
		// (pillar 3.1), so the Sentinels come back BARE and the Redis pods park in the
		// startup wait-loop with no master — RealMasterIP == "", which is Rule L's
		// entire precondition.
		forceLeaderless := func(names ...string) {
			for _, n := range names {
				_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", "-n", testNamespace,
					"-l", "app.kubernetes.io/instance="+n, "--grace-period=0", "--force"))
			}
			for _, n := range names {
				Eventually(func(g Gomega) {
					for _, sp := range []string{n + "-sentinel-0", n + "-sentinel-1", n + "-sentinel-2"} {
						out, err := sentinelPortExec(testNamespace, sp, "SENTINEL", "masters")
						g.Expect(err).NotTo(HaveOccurred())
						g.Expect(sentinelMasterNames(out)).To(BeEmpty(),
							"%s is not bare: %v", sp, sentinelMasterNames(out))
					}
				}, 3*time.Minute, 5*time.Second).Should(Succeed())
			}
		}

		BeforeAll(func() {
			stamp := time.Now().Unix()
			wedged = fmt.Sprintf("op-ll-wedge-%d", stamp)
			control = fmt.Sprintf("op-ll-ctrl-%d", stamp)
			inScope = fmt.Sprintf("op-ll-inscope-%d", stamp)
			for _, n := range []string{wedged, control, inScope} {
				AddReportEntry("cr:" + n)
			}
		})

		AfterAll(func() {
			// Unconditionally FIRST: an operator left at 0 replicas silently breaks
			// every later spec and the next run of the whole suite.
			scaleOperator(1)
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup to allow debugging")
				return
			}
			for _, n := range []string{wedged, control, inScope} {
				_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", n,
					"-n", testNamespace, "--ignore-not-found"))
			}
		})

		It("recovers through Rule L even with a rename pending", func() {
			// UN-SKIPPED with the LR-059 fix. It was committed skipped because it
			// asserts what the operator SHOULD do, and inverting it into a
			// characterisation would have pinned the wedge as correct.
			//
			// The fix is not the narrowing this header proposed ("let Rule L run when
			// there are zero data holders"). It is the criterion underneath: because a
			// suppression has no auto-exit, a rule may be stood down by an operation
			// only if the instance can still reach a SETTLED state with that rule
			// permanently absent. Rule L fails that on the leaderless branch, and since
			// both suppressed rules require `RealMasterIP == ""`, the gate's entire
			// domain of effect was that branch — so sentinel mode's suppressed set is
			// now empty and each rule's own gates carry the refusals. ADR-020, LR-059.
			//
			// ⚠ SPLIT 2026-09-04, AND THE REASON IS THE INTERESTING PART. Run against
			// the fixed operator, this tier still failed — at `phase == Running`, NOT at
			// the master assertion, which passed. Rule L had seeded
			// (`LeaderlessRecovery=False/Reseeded`) and Rule N had converged
			// (`StaleMasterName=False/Converged`); what remained was a SECOND, unrelated
			// cycle, now recorded as **LR-061**:
			//
			//	the Sentinels monitor only the NEW name (Rule N pruned mymaster, which
			//	is ADR-018's R3 working) -> redis-0 and redis-1 are still on the OLD
			//	pod template and their wait-loop asks `get-master-addr-by-name
			//	mymaster` -> they never start ("Sentinel has no master info. Waiting...",
			//	redis-cli refused) -> never Ready -> the rolling update cannot advance
			//	past the one pod it already replaced -> they are never re-baked with the
			//	new name. Measured: sts `ready=0 updated=1`, currentRevision !=
			//	updateRevision, and the operation reaching Stalled without auto-exiting.
			//
			// THIS TIER'S STAGING IS STRONGER THAN LR-059'S CLAIM, which is why the two
			// separate cleanly: it force-deletes the pods BEFORE patching the name, so
			// they return on the OLD template — i.e. it stages "rename a degraded
			// instance", which LR-048 explicitly scopes out ("Rule L is the safety net
			// AND the wedge"). LR-059's own documented route, ADR-016's quarantine
			// release, hands the pods back from the CURRENT template, so they speak the
			// new name and the seed wakes them; that route is covered by the sibling
			// tier below, which is the in-scope variant.
			//
			// So what stays asserted here is exactly what LR-059's fix delivers and what
			// went red→green: a master is assigned and the reseed is named in the CR.
			// Everything downstream of a pod being able to START belongs to LR-061 and
			// is asserted in the sibling tier instead — asserting it here would pin
			// behaviour that does not hold, which is what the skip existed to avoid.

			By("deploying two identical instances")
			for _, n := range []string{wedged, control} {
				cmd := exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(leaderlessCR(n))
				out, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "apply output: %s", out)
			}
			for _, n := range []string{wedged, control} {
				Eventually(func(g Gomega) {
					g.Expect(getPhase(n)).To(Equal("Running"))
					g.Expect(renameAck(n)).NotTo(BeEmpty(), "%s was never seeded", n)
				}, 6*time.Minute, 5*time.Second).Should(Succeed())
			}

			By("pausing the operator and driving BOTH into the leaderless deadlock")
			// Paused, so neither instance can be healed while the state is staged and
			// the two enter it together — which is what makes the pair an A/B rather
			// than two runs.
			scaleOperator(0)
			forceLeaderless(wedged, control)

			By("THE ONE VARIABLE: a rename is pending on one of them only")
			renameMasterName(wedged, e2eMasterName(testNamespace, wedged))

			By("resuming the operator")
			scaleOperator(1)

			// The POSITIVE CONTROL, and it is what makes the failure attributable:
			// the identically-staged instance with no pending operation must recover.
			// Without it, "the other one did not recover" could be a broken fixture.
			By("the control recovers through Rule L")
			Eventually(func(g Gomega) {
				g.Expect(getPhase(control)).To(Equal("Running"))
				g.Expect(getMasterPod(control)).NotTo(BeEmpty())
				st, _ := getConditionField(control, "LeaderlessRecovery", "reason")
				g.Expect(st).To(Equal("Reseeded"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("and so must the one with a rename pending — the LR-059 assertion")
			// THE LR-059 GUARD, and deliberately no more than this. Pre-fix these two
			// were unreachable: Rule L was suppressed for the whole window and the
			// operator logged only "A declared heavy operation is in progress" (LR-059
			// measured ZERO leaderless lines). Post-fix both hold, and the CR NAMES the
			// mechanism rather than leaving it to be inferred.
			Eventually(func(g Gomega) {
				g.Expect(getMasterPod(wedged)).NotTo(BeEmpty(),
					"the instance is still leaderless with a rename pending: Rule L was "+
						"suppressed by the operation, and the operation cannot complete until "+
						"Rule L gives it a master (LR-059)")
				reason, _ := getConditionField(wedged, "LeaderlessRecovery", "reason")
				g.Expect(reason).To(Equal("Reseeded"),
					"the master must come from Rule L's no-data reseed, not from some other "+
						"path — naming it is what makes this a guard for LR-059 rather than "+
						"for 'something eventually assigned a master'")
			}, 6*time.Minute, 5*time.Second).Should(Succeed())

			// ⚠ DELIBERATELY NOT ASSERTED HERE — BLOCKED ON LR-061, and do NOT
			// re-add it without that fix. On this staging (pods force-deleted BEFORE
			// the name patch, so they return on the OLD template) the instance cannot
			// reach `Running` or finish the rename however long it is given: the two
			// old-template pods ask Sentinel for `mymaster`, which Rule N has correctly
			// pruned, so they never start, never become Ready, and the rolling update
			// that would re-bake them cannot advance. Measured on t3e 2026-09-04 against
			// the fixed operator: `ready=0 updated=1`, currentRevision != updateRevision,
			// operation Stalled with no auto-exit. The in-scope route — a rename already
			// in the template when the pods return, which is what ADR-016's quarantine
			// release produces — is the sibling tier below, and it asserts full recovery.
		})

		// The IN-SCOPE variant of the tier above, and the one that exercises LR-059's
		// documented reachability path end to end.
		//
		// The difference is one line of ordering: the rename is patched BEFORE the pods
		// are forced out, so they come back from the CURRENT pod template and speak the
		// name the operator will seed. That is exactly what ADR-016's quarantine release
		// hands back (empty, leaderless, current template), which is the route LR-059
		// names as reachable "by a path docs/USAGE.md was actively inviting".
		//
		// So this tier asserts what the fix is FOR: with a heavy operation pending and no
		// master anywhere, the instance recovers on Rule L's ordinary path AND the
		// operation then completes. It carries no LR-061 exposure, because no pod is left
		// on a superseded template.
		It("recovers and completes the rename when the pods return on the new template", func() {
			By("deploying one instance")
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(leaderlessCR(inScope))
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "apply output: %s", out)
			Eventually(func(g Gomega) {
				g.Expect(getPhase(inScope)).To(Equal("Running"))
				g.Expect(renameAck(inScope)).NotTo(BeEmpty(), "%s was never seeded", inScope)
			}, 6*time.Minute, 5*time.Second).Should(Succeed())

			By("renaming FIRST, so the new name is in the pod template")
			renameMasterName(inScope, e2eMasterName(testNamespace, inScope))

			By("then pausing the operator and driving it leaderless")
			// The operation is pending across the whole window: the rename is patched
			// but unacknowledged, so `OperationInProgress` is True throughout, which is
			// the precondition this tier exists to test against.
			scaleOperator(0)
			forceLeaderless(inScope)
			scaleOperator(1)

			By("Rule L recovers it despite the pending operation")
			Eventually(func(g Gomega) {
				g.Expect(getMasterPod(inScope)).NotTo(BeEmpty())
				reason, _ := getConditionField(inScope, "LeaderlessRecovery", "reason")
				g.Expect(reason).To(Equal("Reseeded"))
				g.Expect(getPhase(inScope)).To(Equal("Running"))
			}, 8*time.Minute, 5*time.Second).Should(Succeed())

			By("and the rename then completes and goes quiet")
			Eventually(func(g Gomega) {
				expectQuietOperationOn(g, inScope)
				for _, sp := range []string{inScope + "-sentinel-0", inScope + "-sentinel-1", inScope + "-sentinel-2"} {
					so, serr := sentinelPortExec(testNamespace, sp, "SENTINEL", "masters")
					g.Expect(serr).NotTo(HaveOccurred())
					g.Expect(sentinelMasterNames(so)).To(
						Equal([]string{e2eMasterName(testNamespace, inScope)}))
				}
			}, 6*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

// expectQuietOperationOn is the package-level twin of the closure inside the main
// Describe, for the tier above which lives outside it.
func expectQuietOperationOn(g Gomega, crName string) {
	g.Expect(operationCondStatus(crName)).NotTo(Equal("True"),
		"an operation is declared (%s) when none should be", operationCondReason(crName))
	g.Expect(operationStatusField(crName, "name")).To(BeEmpty(),
		"status.operation is populated when nothing should be declared")
}

// =============================================================================
// =============================================================================
// TIER 6 — an operator upgrade over an existing fleet declares NOTHING
// =============================================================================
//
// The worst regression this feature can ship, and the cheapest tier here.
//
// Without per-candidate seeding (planOperation row 3) EVERY instance in an existing
// fleet declares an operation the moment the operator is upgraded: the spec value
// differs from a nonexistent acknowledgment, which reads as unfinished work. The
// blast radius is the whole installed base at once, and every one of those instances
// would then stand down its authority-assigning healing for a change nobody made.
//
// Row 3 is PER CANDIDATE, not per instance, and that is the whole of it: the
// fleet-upgrade case falls out as the special case where every candidate is missing,
// rather than being a second rule. Keying it on "the ack list is empty" is a
// whole-list heuristic doing a per-row job — correct for a one-entry registry, and it
// silently re-runs a completed operation the moment there are two.
//
// THE RIG. This needs an operator that predates the wiring, so the instances are
// created with NO acknowledgment rows at all — which is exactly the state a real
// fleet is in on the day of the upgrade. `1d915e4` is the last commit before the
// branch was wired. Building and pushing it is out-of-band (a different working
// tree), so the tier takes the image reference from the environment and self-skips
// when it is absent, mirroring LEGACY_OPERATOR_IMAGE in cluster_migration_test.go.
//
// It swaps the cluster's operator, so it restores OPERATOR_IMAGE unconditionally in
// AfterAll — an operator left on an old image silently breaks every later spec and
// the next run of the whole suite.
var _ = Describe("Sentinel Declared Operations Across an Operator Upgrade",
	Label("sentinel"), Ordered, func() {

		var instances []string
		var churn string
		var preOpsImage, currentImage string

		// preOperationsOperatorImage returns the pre-ADR-020 operator image, or skips.
		preOperationsOperatorImage := func() string {
			ref := os.Getenv("PRE_OPERATIONS_OPERATOR_IMAGE")
			if ref == "" {
				Skip("PRE_OPERATIONS_OPERATOR_IMAGE not set — this tier needs an operator image " +
					"built from a commit BEFORE the declared-operations wiring (1d915e4), so the " +
					"fleet it creates carries no acknowledgedOperations rows. Build and push it " +
					"out-of-band and set PRE_OPERATIONS_OPERATOR_IMAGE=<repo>:<tag> to run this tier.")
			}
			return ref
		}

		BeforeAll(func() {
			preOpsImage = preOperationsOperatorImage()
			currentImage = os.Getenv("OPERATOR_IMAGE")
			if currentImage == "" {
				Skip("OPERATOR_IMAGE not set — cannot determine the image to upgrade TO " +
					"(normally exported by `make run-test-e2e`).")
			}

			stamp := time.Now().Unix()
			// Three settled instances plus one deliberately UNSETTLED at the moment of
			// the upgrade — see the churn step below for why the fourth is what makes
			// this tier able to fail at all.
			for i := 0; i < 3; i++ {
				instances = append(instances, fmt.Sprintf("op-fleet-%d-%d", stamp, i))
			}
			churn = fmt.Sprintf("op-fleet-%d-churn", stamp)
			instances = append(instances, churn)
			for _, n := range instances {
				AddReportEntry("cr:" + n)
			}
		})

		AfterAll(func() {
			// Unconditionally FIRST: restore the suite's own operator before anything
			// else can go wrong or return early.
			if currentImage != "" {
				deployOperatorImage(currentImage)
			}
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping cleanup to allow debugging")
				return
			}
			for _, n := range instances {
				_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", n,
					"-n", testNamespace, "--ignore-not-found"))
			}
		})

		It("seeds every instance and declares no operation for any of them", func() {
			By("deploying the PRE-operations operator " + preOpsImage)
			deployOperatorImage(preOpsImage)

			By("creating a fleet of healthy sentinel instances under it")
			for _, n := range instances {
				cmd := exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(sentinelRenameCR(n, e2eMasterName(testNamespace, n)))
				out, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "apply output: %s", out)
			}
			for _, n := range instances {
				Eventually(func(g Gomega) {
					g.Expect(getPhase(n)).To(Equal("Running"))
				}, 8*time.Minute, 5*time.Second).Should(Succeed(), "%s never reached Running", n)
			}

			By("the precondition that makes this tier mean anything: NO acknowledgment rows")
			// The positive control. If the old operator did write acks, the upgrade
			// would be a no-op and "nothing was declared" would prove nothing at all.
			for _, n := range instances {
				Expect(renameAck(n)).To(BeEmpty(),
					"%s already carries an acknowledgment row, so %s is not a pre-operations "+
						"operator and this tier is testing nothing", n, preOpsImage)
				Expect(operationCondStatus(n)).To(BeEmpty(),
					"%s already carries an OperationInProgress condition", n)
			}

			// ---- ONE INSTANCE MUST BE UNSETTLED ACROSS THE UPGRADE -----------
			//
			// THIS STEP IS WHAT MAKES THE TIER ABLE TO FAIL, and it was added because
			// the mutation check said so rather than because it was reasoned. Against
			// a build with row 3 REMOVED the three settled instances go green: with no
			// ack row the candidate reads as pending, the driver (Rule N) has nothing
			// stale to prune so it reports Converged on the very first pass, the
			// StatefulSets are already settled, and row 8 therefore acknowledges in
			// that same pass with Report empty — so the condition goes straight to
			// False/Converged and NOTHING is ever observed declared. The regression
			// ADR-020 warns about is invisible on a quiet fleet.
			//
			// It is not invisible on a fleet that is mid-anything, which is what a real
			// upgrade meets: with the StatefulSets unsettled, row 8's completion
			// condition is false, so row 7 keeps the operation RUNNING and healing
			// stands down — for every instance in the fleet, until each one settles.
			// On a leaderless one that is LR-059's permanent wedge.
			//
			// So one instance is wedged unsettled first, with a nodeSelector nothing
			// matches. Only the highest ordinal is stranded Pending, so the instance
			// keeps its master and stays a legitimate member of the fleet — it is
			// simply not settled, which is the state under test.
			By("wedging " + churn + " unsettled, so the upgrade meets a fleet that is mid-roll")
			out, err := utils.Run(exec.Command("kubectl", "patch", "littlered", churn,
				"-n", testNamespace, "--type=merge", "-p",
				`{"spec":{"podTemplate":{"nodeSelector":{"littlered.e2e/unschedulable":"true"}}}}`))
			Expect(err).NotTo(HaveOccurred(), "patch output: %s", out)
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", testNamespace,
					"-l", "app.kubernetes.io/instance="+churn,
					"-o", "jsonpath={range .items[*]}{.metadata.name}={.status.phase} {end}"))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("=Pending"),
					"no pod of %s is Pending, so it is not unsettled and the tier is back to "+
						"testing only the quiet case: %s", churn, out)
			}, 4*time.Minute, 5*time.Second).Should(Succeed())

			By("UPGRADING to " + currentImage)
			deployOperatorImage(currentImage)

			By("every instance is SEEDED — an ack row appears, written without running anything")
			for _, n := range instances {
				Eventually(func(g Gomega) {
					g.Expect(renameAck(n)).NotTo(BeEmpty(),
						"%s was never seeded after the upgrade", n)
				}, 4*time.Minute, 3*time.Second).Should(Succeed())
			}

			By("and NO instance ever declares an operation")
			// Sampled rather than read once: the failure this guards is an operation
			// declared for a few passes and then acknowledged, which a single read
			// would miss entirely. 3 minutes is many multiples of the steady interval
			// and covers the whole post-upgrade settling period.
			Consistently(func(g Gomega) {
				for _, n := range instances {
					g.Expect(operationCondStatus(n)).NotTo(Equal("True"),
						"%s declared an operation (%s) on an operator upgrade — nobody edited "+
							"its spec. Without per-candidate seeding this happens to EVERY "+
							"instance in the fleet at once (ADR-020 row 3)",
						n, operationCondReason(n))
					g.Expect(operationStatusField(n, "name")).To(BeEmpty(),
						"%s populated status.operation on an operator upgrade", n)
				}
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("and the settled part of the fleet is still healthy under the new operator")
			// churn is excluded by construction: it was wedged on purpose and cannot be
			// Ready. Its claim is the one above — that it declared no operation while
			// unsettled — which is the whole reason it is here.
			for _, n := range instances {
				if n == churn {
					continue
				}
				Eventually(func(g Gomega) {
					g.Expect(getPhase(n)).To(Equal("Running"))
					st, _ := getConditionField(n, "Ready", "status")
					g.Expect(st).To(Equal("True"))
				}, 5*time.Minute, 5*time.Second).Should(Succeed())
			}
		})
	})
