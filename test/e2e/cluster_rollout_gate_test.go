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

// Cluster Rolling Update Redundancy Gate — the regression guard for LR-047 / ADR-017
// ("state-gated cluster rolling updates").
//
// WHAT THIS GUARDS
// ----------------
// Cluster mode is one StatefulSet per shard ({name}-shard-K, sized 1+replicasPerShard;
// pod -K-0 is the shard's intended master, -K-1..R its replicas). The shard STS is built
// with `UpdateStrategy: {Type: RollingUpdate}` and NO `rollingUpdate.partition`
// (`internal/controller/resources.go`, buildClusterShardStatefulSet), so the StatefulSet
// controller — not the operator — owns the intra-shard sequence: delete the highest
// ordinal, wait until it is Running+Ready and has been available for `minReadySeconds`
// (30s for a redundant shard), then delete the next ordinal. I.e. the replica first,
// then the shard's master.
//
// BOTH of those gates are blind to redundancy:
//
//   - Readiness (buildClusterReadinessProbe) is `[ ! -f /data/bootstrap-in-progress ] &&
//     redis-cli ping` — a LOCAL ping. It says nothing about cluster membership, slot
//     ownership or replication.
//   - `minReadySeconds: 30` is wall-clock. Its own comment in resources.go lists
//     "buffer for operator reconciliation" as a justification.
//
// A replaced pod returns with a wiped EmptyDir (pillar 3.1) and therefore a NEW cluster
// node ID, so it must be CLUSTER FORGET-ed (old ID), MEET-ed, REPLICATE-d by the operator
// and then full-sync before it is a copy of anything. The operator's repair loop is gated
// on `allPodsReady` (cluster_reconcile.go), so its entire window to restore the shard's
// redundancy is `minReadySeconds` after the fresh pod passes its local PING. Miss that
// window and the StatefulSet deletes the shard's master anyway; the master's preStop looks
// for a healthy replica of itself, finds none, logs "No healthy replica found to take over.
// Proceeding with restart." and exits 0 — and the last copy of that shard's slot range dies
// with it.
//
// Measured in the field on 2026-08-23: `roll-c`/`roll-d` lost in a rolling update that
// reported complete success — every shard STS at observedGeneration == generation,
// currentRevision == updateRevision, all six pods replaced, `cluster_state:ok`, 16384 slots
// assigned — and shard 1's data gone. The operator then correctly healed an ALREADY-DEAD
// shard (Step 3 SafeMissingShardTarget assigns the orphaned range to a fresh empty master),
// which is exactly why the topology read healthy. Shards 0 and 2 used 15s and 19s of their
// 36s budget: this has been passing on margin, not on an invariant.
//
// WHY THIS TIER PAUSES THE OPERATOR
// ---------------------------------
// The existing `Cluster Mode Rolling Update > should preserve data after rolling update`
// tier races that margin, so it flakes rather than fails. This tier makes the margin
// UNAVAILABLE instead of racing it: it scales the operator to 0 the moment a shard's
// replica pod starts being replaced, and holds it down until the StatefulSet has deleted
// that shard's MASTER too. With the operator down nothing can FORGET/MEET/REPLICATE the
// fresh replica, so the shard provably has zero synced copies when its master is destroyed.
// The loss becomes deterministic instead of a race.
//
// That also isolates this defect (B, the rollout gate) from the LR-046 half (probe latency):
// with the operator down, no amount of bounded dialling helps. The two were found on the
// same run; LR-046 closes only the latency half and says so.
//
// This tier is expected to be RED until ADR-017's fix lands (operator-owned
// `spec.updateStrategy.rollingUpdate.partition`, lowered per shard only once the fresh
// replica is a link-`up` replica of that shard's master). Note that a naive preStop refusal
// is NOT a fix — the kubelet SIGKILLs after terminationGracePeriodSeconds, so refusing only
// delays the loss.
//
// WHAT MAKES THE ASSERTION MEANINGFUL (the positive controls)
// -----------------------------------------------------------
// A data-survival assertion is vacuous if nothing actually happened. This tier therefore
// asserts, as first-class expectations rather than as setup:
//
//  1. every key is attributed to a named shard by CRC16 (CLUSTER KEYSLOT) and is proven to
//     be served by that shard's own master with a direct, non-redirecting GET, BEFORE the
//     update starts (so a later miss names the shard the way the field report did);
//  2. the rollout really started on a named shard (its replica pod entered replacement);
//  3. the operator really was absent for the WHOLE pause window — availableReplicas is
//     re-asserted every second across it, not sampled once at the end;
//  4. the rollout really completed afterwards (every pod replaced, CR Running).
//
// Whether the shard's MASTER was replaced during the pause is RECORDED, not asserted:
// pre-fix it is (T+37.2s, measured on t3e) and that is the defect; post-fix it is not, and
// that is the fix. Asserting it would make the fixed build time out instead of pass. The
// tier's verdict is the data assertion, in both worlds.
//
// The final topology capture is deliberately NOT an assertion about health but a record of
// the silence: `cluster_state:ok` + 16384 slots + Running is exactly what the field run
// reported while a shard's data was gone. A green topology is not evidence.
var _ = Describe("Cluster Rolling Update Redundancy Gate (LR-047)", Label("cluster", "rollout-gate"), Ordered, func() {
	const crName = "rollgate-b"

	// operatorPauseWindow bounds how long the operator is held at 0 replicas. It has to be
	// comfortably longer than a shard's own intra-shard budget (`minReadySeconds` 30s plus
	// the fresh replica's start-up), because pre-fix that budget is exactly what the
	// StatefulSet spends before deleting the shard's master — measured at T+37.2s on t3e.
	// It also has to be bounded rather than "wait until the master is gone", so a FIXED
	// build (where the master is never deleted while the operator is away) expires the
	// window instead of timing out. Pausing the operator pauses every instance in its
	// scope, so it is no longer than it needs to be, and the loop exits early once the
	// master has actually been replaced.
	const operatorPauseWindow = 90 * time.Second

	// operatorDownHold keeps the operator away for a moment AFTER the target shard's master
	// has been observed replaced, so the fresh master also comes up with nothing to seed it.
	const operatorDownHold = 5 * time.Second

	type shardKey struct {
		shard int
		key   string
		value string
		slot  int
		lo    int
		hi    int
	}
	var keys []shardKey

	// targetShard is the shard whose redundancy window we make unavailable; chosen at
	// runtime as whichever shard the operator's LR-021 serialization rolls first.
	targetShard := -1

	BeforeAll(func() {
		AddReportEntry("cr:" + crName)
		Expect(clusterReplicasPerShard).To(BeNumerically(">=", 1),
			"this tier is about intra-shard redundancy during a roll; it needs replicasPerShard >= 1")

		By(fmt.Sprintf("creating a %d-shard cluster with %d replica(s) per shard",
			clusterShards, clusterReplicasPerShard))
		cr := clusterCR(crName, clusterReplicasPerShard, "", `  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
`)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the cluster to be Running")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.phase}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}, 6*time.Minute, 5*time.Second).Should(Succeed())

		By("waiting for a whole cluster (all slots assigned, every shard master owning a range)")
		Eventually(func(g Gomega) {
			info, err := redisExec(testNamespace, clusterMasterPod(crName, 0), "CLUSTER", "INFO")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(info).To(ContainSubstring("cluster_state:ok"))
			g.Expect(info).To(ContainSubstring("cluster_slots_assigned:16384"))
			g.Expect(clusterSize(clusterMasterPod(crName, 0))).To(Equal(clusterShards))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		// UNCONDITIONAL: never leave the operator scaled down, whatever happened above.
		// A paused operator would silently break every later tier in the run.
		scaleOperator(1)

		if debugOnFailure && suiteOrSpecFailed() {
			By("skipping cleanup due to failure and DEBUG_ON_FAILURE=true")
			return
		}
		_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", crName,
			"-n", testNamespace, "--ignore-not-found", "--timeout=2m"))
	})

	It("attributes one key to each shard and proves it is served by that shard's own master", func() {
		// Positive control #1. The field report's whole diagnostic value came from CRC16
		// attribution — "shard 0 loaded 2 keys, shard 2 loaded 1, shard 1 loaded 0". A tier
		// that writes five keys through `redis-cli -c` and reads them back the same way can
		// only ever say "some keys are gone"; it can never name the shard. So: derive each
		// shard's owned range from that shard's own master (ground truth, not a recomputed
		// GenerateSlotRanges), find a hash tag routing into it, and pin the key to the shard.
		keys = nil
		for k := 0; k < clusterShards; k++ {
			master := clusterMasterPod(crName, k)
			lo, hi := firstOwnedSlotRange(master)
			tag := findHashTagInSlotRange(master, lo, hi)
			sk := shardKey{
				shard: k,
				key:   fmt.Sprintf("rollgate%s-shard%d", tag, k),
				value: fmt.Sprintf("val-shard%d", k),
				lo:    lo,
				hi:    hi,
			}
			sk.slot = keySlot(master, sk.key)
			Expect(sk.slot).To(BeNumerically(">=", lo))
			Expect(sk.slot).To(BeNumerically("<=", hi))
			keys = append(keys, sk)
		}

		By("writing the attributed keys")
		for _, sk := range keys {
			out, err := redisExec(testNamespace, clusterMasterPod(crName, 0), "-c", "SET", sk.key, sk.value)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("OK"))
		}

		// The decisive precondition: a DIRECT (non-`-c`, non-redirecting) GET on the shard's
		// own master. If the key were not physically on that master this would answer MOVED,
		// and the post-update assertion below would be measuring the wrong pod.
		By("proving each key is served directly by its own shard master")
		for _, sk := range keys {
			master := clusterMasterPod(crName, sk.shard)
			out, err := redisExec(testNamespace, master, "GET", sk.key)
			Expect(err).NotTo(HaveOccurred(),
				"key %q (slot %d) is not served by %s", sk.key, sk.slot, master)
			Expect(strings.TrimSpace(out)).To(Equal(sk.value),
				"key %q (slot %d) should be readable directly from %s before the update", sk.key, sk.slot, master)
			AddReportEntry(fmt.Sprintf("attribution: shard %d slots %d-%d key %s slot %d -> %s",
				sk.shard, sk.lo, sk.hi, sk.key, sk.slot, master))
			_, _ = fmt.Fprintf(GinkgoWriter, "attribution: shard %d slots %d-%d | key %s slot %d | master %s\n",
				sk.shard, sk.lo, sk.hi, sk.key, sk.slot, master)
		}
	})

	It("must not lose a shard when the rollout's redundancy window is unavailable", func() {
		podNames := clusterPodNames(crName, clusterShards, clusterReplicasPerShard)
		oldUIDs := map[string]string{}
		for _, p := range podNames {
			oldUIDs[p] = podUID(testNamespace, p)
			Expect(oldUIDs[p]).NotTo(BeEmpty())
		}

		By("triggering an OPERATOR-mediated rolling update (a CR pod-template change)")
		// Never `kubectl rollout restart`: that bypasses the operator entirely, so it rolls
		// every shard in parallel and is explicitly out of LR-021's scope. The defect under
		// test is in the rollout the operator DOES govern.
		updated := clusterCR(crName, clusterReplicasPerShard, "", `  resources:
    requests:
      cpu: "100m"
      memory: "160Mi"
    limits:
      cpu: "100m"
      memory: "160Mi"
`)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(updated)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("watching for the first shard whose REPLICA pod starts being replaced")
		// The StatefulSet controller rolls the highest ordinal first, so a shard's roll
		// starts at its replica. Detect on deletionTimestamp (or an outright UID change),
		// which is strictly EARLIER than the fresh pod passing its local PING — that head
		// start is what lets the operator be gone before it could have MEET-ed anything.
		Eventually(func(g Gomega) {
			for k := 0; k < clusterShards; k++ {
				replica := clusterReplicaPod(crName, k, clusterReplicasPerShard)
				if podTerminating(replica) || podUID(testNamespace, replica) != oldUIDs[replica] {
					targetShard = k
					return
				}
			}
			g.Expect(targetShard).To(BeNumerically(">=", 0), "no shard has begun rolling yet")
		}, 6*time.Minute, 500*time.Millisecond).Should(Succeed())

		tReplica := time.Now()
		targetReplica := clusterReplicaPod(crName, targetShard, clusterReplicasPerShard)
		targetMaster := clusterMasterPod(crName, targetShard)
		By(fmt.Sprintf("shard %d is rolling (replica %s) — pausing the operator", targetShard, targetReplica))
		AddReportEntry(fmt.Sprintf("target shard: %d (replica %s, master %s)", targetShard, targetReplica, targetMaster))

		// Fire the scale-down first and only then wait for it, so the operator starts
		// terminating at the earliest possible instant.
		_, err = utils.Run(exec.Command("kubectl", "scale", "deployment/littlered",
			"-n", operatorNamespace, "--replicas=0"))
		Expect(err).NotTo(HaveOccurred())
		scaleOperator(0)
		tOperatorDown := time.Now()
		_, _ = fmt.Fprintf(GinkgoWriter, "operator down %.1fs after the replica roll began\n",
			tOperatorDown.Sub(tReplica).Seconds())

		By(fmt.Sprintf("holding the operator down while shard %d's roll proceeds", targetShard))
		// This window is BOUNDED and must stay bounded, because the tier has to be able to
		// go green once ADR-017 lands. Pre-fix the StatefulSet deletes the shard's master
		// inside it (nothing gates that deletion on the fresh replica being a synced copy —
		// only on a local PING plus `minReadySeconds` of stillness). Post-fix the operator
		// owns `rollingUpdate.partition` and, being down, never lowers it, so the master is
		// NOT deleted and the window simply expires. Waiting for the master's deletion as a
		// precondition would therefore turn the fixed build into a 5-minute timeout — a red
		// for the wrong reason. So: sample, record, and move on either way.
		//
		// Sampling every second also gives the operator-absence control its teeth: it is a
		// sequence assertion over the whole window (this suite's convention), not a single
		// reading at the end that a brief unnoticed restart could slip past.
		masterReplacedAt := time.Duration(-1)
		deadline := time.Now().Add(operatorPauseWindow)
		for time.Now().Before(deadline) {
			Expect(operatorAvailableReplicas()).To(Equal(0),
				"the operator came back up during the pause window — the repro did not hold, "+
					"so neither a pass nor a failure below would mean anything")
			if masterReplacedAt < 0 && podUID(testNamespace, targetMaster) != oldUIDs[targetMaster] {
				masterReplacedAt = time.Since(tReplica)
				// Give the fresh master a moment to come up with no operator to seed it,
				// then stop early: the damage (if any) is done and a longer operator
				// outage only slows the run and every other instance in its scope.
				time.Sleep(operatorDownHold)
				Expect(operatorAvailableReplicas()).To(Equal(0))
				break
			}
			time.Sleep(time.Second)
		}

		// Recorded, NOT asserted. Pre-fix this is the defect (the master is destroyed with
		// zero synced copies of its range); post-fix its absence is the fix working. The
		// tier's verdict is the data assertion at the end, in both worlds.
		if masterReplacedAt >= 0 {
			_, _ = fmt.Fprintf(GinkgoWriter,
				"shard %d: replica roll at T+0, operator down at T+%.1fs, MASTER REPLACED at T+%.1fs "+
					"— the shard had zero operator-attached copies for that whole window\n",
				targetShard, tOperatorDown.Sub(tReplica).Seconds(), masterReplacedAt.Seconds())
			AddReportEntry(fmt.Sprintf("window: operator down T+%.1fs, master replaced T+%.1fs",
				tOperatorDown.Sub(tReplica).Seconds(), masterReplacedAt.Seconds()))
		} else {
			_, _ = fmt.Fprintf(GinkgoWriter,
				"shard %d: replica roll at T+0, operator down at T+%.1fs, master NOT replaced within %s "+
					"— the intra-shard roll held while the operator was absent\n",
				targetShard, tOperatorDown.Sub(tReplica).Seconds(), operatorPauseWindow)
			AddReportEntry(fmt.Sprintf("window: operator down T+%.1fs, master NOT replaced within %s",
				tOperatorDown.Sub(tReplica).Seconds(), operatorPauseWindow))
		}

		By("restoring the operator")
		scaleOperator(1)

		By("waiting for every cluster pod to be replaced (the rollout really did complete)")
		// Positive control #4. Without this the tier could pass simply by not rolling.
		Eventually(func(g Gomega) {
			for _, p := range podNames {
				g.Expect(podUID(testNamespace, p)).NotTo(Equal(oldUIDs[p]), "pod %s was never replaced", p)
			}
		}, 12*time.Minute, 5*time.Second).Should(Succeed())

		By("waiting for the CR to report Running again")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.phase}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}, 12*time.Minute, 5*time.Second).Should(Succeed())

		// --- the silence, captured BEFORE the data assertion -------------------------
		// This is what made the field incident invisible, and it is recorded rather than
		// asserted-upon: the operator heals an already-dead shard (Step 3
		// SafeMissingShardTarget assigns the orphaned range to a fresh empty master), so
		// every topology signal reads healthy while a shard's keyspace is gone.
		By("recording the post-rollout topology — a green topology is NOT evidence of data survival")
		Eventually(func(g Gomega) {
			info, err := redisExec(testNamespace, clusterMasterPod(crName, 0), "CLUSTER", "INFO")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(info).To(ContainSubstring("cluster_state:ok"))
			g.Expect(info).To(ContainSubstring("cluster_slots_assigned:16384"))
		}, 6*time.Minute, 5*time.Second).Should(Succeed())
		logClusterGroundTruth(crName)

		// --- the actual check --------------------------------------------------------
		By("asserting every shard still holds its attributed key")
		var missing []string
		for _, sk := range keys {
			out, err := redisExec(testNamespace, clusterMasterPod(crName, 0), "-c", "GET", sk.key)
			got := strings.TrimSpace(out)
			if err != nil || got != sk.value {
				missing = append(missing, fmt.Sprintf(
					"shard %d (slots %d-%d): key %q slot %d — want %q, got %q (err %v)",
					sk.shard, sk.lo, sk.hi, sk.key, sk.slot, sk.value, got, err))
			}
		}
		Expect(missing).To(BeEmpty(),
			"a rolling update that reported complete success destroyed a shard's data "+
				"(LR-047 / ADR-017: the intra-shard roll is time-gated on minReadySeconds + a local "+
				"PING, not state-gated on the fresh replica actually being a link-up replica of its "+
				"shard's master):\n  %s", strings.Join(missing, "\n  "))
	})
})

// --- helpers -----------------------------------------------------------------

// podTerminating reports whether a pod has a deletionTimestamp. This fires EARLIER than a
// UID change (the replacement is only created once the old pod is gone), which is what
// gives this tier its head start on the fresh pod's readiness.
func podTerminating(pod string) bool {
	out, err := utils.Run(exec.Command("kubectl", "get", "pod", pod, "-n", testNamespace,
		"-o", "jsonpath={.metadata.deletionTimestamp}"))
	if err != nil {
		return false
	}
	return strings.TrimSpace(out) != ""
}

// operatorAvailableReplicas returns the operator Deployment's availableReplicas (0 when the
// field is absent). Used as a positive control that the pause window actually held.
func operatorAvailableReplicas() int {
	out, err := utils.Run(exec.Command("kubectl", "get", "deployment/littlered",
		"-n", operatorNamespace, "-o", "jsonpath={.status.availableReplicas}"))
	if err != nil {
		return -1
	}
	s := strings.TrimSpace(out)
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return -1
	}
	return n
}

// logClusterGroundTruth dumps per-pod CLUSTER INFO / CLUSTER NODES / DBSIZE to the Ginkgo
// output, so a failing run carries the same evidence the field report was built from
// (CLAUDE.md §7 rule 8 — `lrctl verify` is the designated tool and is collected by the
// debug-artifact path; this is the inline, always-present companion).
func logClusterGroundTruth(crName string) {
	_, _ = fmt.Fprintf(GinkgoWriter, "\n===== cluster ground truth (%s) =====\n", crName)
	for k := 0; k < clusterShards; k++ {
		for o := 0; o <= clusterReplicasPerShard; o++ {
			pod := fmt.Sprintf("%s-shard-%d-%d", crName, k, o)
			info, _ := redisExec(testNamespace, pod, "CLUSTER", "INFO")
			dbsize, _ := redisExec(testNamespace, pod, "DBSIZE")
			myid, _ := redisExec(testNamespace, pod, "CLUSTER", "MYID")
			_, _ = fmt.Fprintf(GinkgoWriter, "--- %s (id %s, dbsize %s) ---\n%s\n",
				pod, strings.TrimSpace(myid), strings.TrimSpace(dbsize), firstLines(info, 6))
		}
	}
	nodes, _ := redisExec(testNamespace, clusterMasterPod(crName, 0), "CLUSTER", "NODES")
	_, _ = fmt.Fprintf(GinkgoWriter, "--- CLUSTER NODES ---\n%s\n", nodes)
	_, _ = fmt.Fprintf(GinkgoWriter, "===== end cluster ground truth =====\n\n")
}

func firstLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}
