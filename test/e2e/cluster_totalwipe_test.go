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

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
	"github.com/littlered-operator/littlered-operator/test/utils"
)

// Cluster total-wipe re-bootstrap — the cluster analog of the sentinel leaderless
// bootstrap deadlock (Rule L / LR-015, see leaderless_recovery_test.go).
//
// The question under test: when EVERY cluster pod is lost at once — the whole
// instance, not a rolling update (LR-021 already closed the operator-driven rolling
// path) — does the operator get the cluster back to a healthy, serving state on its
// own, or does it stall like the sentinel bare-quorum deadlock did?
//
// Two genuinely different wipe shapes, because cluster /data is an EmptyDir and the
// startup script (buildClusterRedisContainer) branches on whether nodes.conf survives:
//
//	A. All pods DELETED at once (node-pool recycle / mass eviction). EmptyDir is gone,
//	   so every pod returns FRESH: no nodes.conf, a new node ID, no slots, isolated.
//	   Code trace says this self-heals through repairCluster without ever hitting
//	   bootstrapCluster: Step 1 MEETs all nodes into one cluster (seed = largest
//	   partition — no majority needed), Step 3 assigns each missing range to its fresh
//	   intended -shard-K-0 master (SafeMissingShardTarget accepts a reachable empty
//	   master), Step 4 reattaches the empty replicas shard-aware. This test is the
//	   regression guard for that path.
//
//	B. All redis processes KILL-9'd at once (OOM storm / mass container crash). The
//	   pod, its IP, and its EmptyDir survive, so nodes.conf survives and the startup
//	   script takes its RESTART_DETECTED=true branch (STEP 3 "yield until a peer
//	   confirms I no longer own slots"). With a single victim this resolves — the
//	   replica fails over and the returning empty master is confirmed demoted (proven
//	   by the kill-9 cluster test). With EVERY node down at once there may be no
//	   reachable peer that can confirm demotion, so this is where a mutual-yield
//	   deadlock could live. This test is the arbiter of whether that gap is real.
//
// Data safety is NOT the axis here (unlike sentinel Rule L). A cluster total-wipe
// leaves ZERO survivors holding data — in cluster mode data lives only in owned slots,
// so "all slots gone" means "no data anywhere" — hence there is no ≥2-holder /
// allowUnsafeRebootstrap dilemma to model. Data loss on a full wipe is expected by
// design (pure in-memory, EmptyDir — pillar 3.1); the property under test is
// AVAILABILITY RECOVERY, not durability. Each test writes a canary only to prove the
// cluster was functional beforehand, and asserts fresh writes work after recovery.
var _ = Describe("Cluster Total-Wipe Re-Bootstrap", Label("cluster"), func() {

	expectedNodes := clusterTotalNodes(clusterReplicasPerShard)

	deployCluster := func(crName string) {
		AddReportEntry("cr:" + crName)
		By(fmt.Sprintf("creating a %d-shard / %d-replica cluster (%d nodes)",
			clusterShards, clusterReplicasPerShard, expectedNodes))
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
			g.Expect(getPhase(crName)).To(Equal("Running"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())
		verifyClusterTopologySync(testNamespace, crName, expectedNodes)
	}

	cleanup := func(crName string) {
		if debugOnFailure && suiteOrSpecFailed() {
			By("skipping cleanup to allow debugging")
			return
		}
		cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace,
			"--ignore-not-found", "--timeout=1m")
		_, _ = utils.Run(cmd)
	}

	// writeCanary sets a key through a shard-0 master (with -c redirection) to prove the
	// cluster is serving before the wipe.
	writeCanary := func(crName, key, val string) {
		cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
			"-n", testNamespace, "-c", "redis", "--", "redis-cli", "-c", "SET", key, val)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
	}

	// expectRecovered asserts the operator drove the wiped cluster back to a healthy,
	// serving state: Phase=Running, cluster_state:ok with all 16384 slots, the operator's
	// status matches Redis ground truth (incl. shard colocation), and fresh writes work.
	expectRecovered := func(crName string, recoveryTimeout time.Duration) {
		By("the operator must re-bootstrap the wiped cluster back to Running on its own")
		Eventually(func(g Gomega) {
			g.Expect(getPhase(crName)).To(Equal("Running"))
		}, recoveryTimeout, 10*time.Second).Should(Succeed(),
			"operator did not re-bootstrap the cluster after a total wipe")

		By("cluster_state must be ok with all slots assigned")
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
				"-n", testNamespace, "-c", "redis", "--", "redis-cli", "CLUSTER", "INFO")
			out, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("cluster_state:ok"))
			g.Expect(out).To(ContainSubstring("cluster_slots_assigned:16384"))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		verifyClusterTopologySync(testNamespace, crName, expectedNodes)

		By("the recovered cluster must serve fresh writes across slots")
		for _, key := range []string{"postwipe-a", "postwipe-b", "postwipe-c"} {
			cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
				"-n", testNamespace, "-c", "redis", "--", "redis-cli", "-c", "SET", key, "v-"+key)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			cmd = exec.Command("kubectl", "exec", clusterMasterPod(crName, clusterShards-1),
				"-n", testNamespace, "-c", "redis", "--", "redis-cli", "-c", "GET", key)
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("v-" + key))
		}
	}

	// --- Flavor A: all pods deleted at once (fresh EmptyDir, no nodes.conf) --------
	Context("All pods deleted at once (node-pool recycle)", Ordered, func() {
		var crName string
		BeforeAll(func() { crName = fmt.Sprintf("wipe-delete-%d", time.Now().Unix()); deployCluster(crName) })
		AfterAll(func() { cleanup(crName) })

		It("re-bootstraps from a fully-fresh wipe (expected to self-heal)", func() {
			writeCanary(crName, "canary-delete", "pre-wipe")

			By("force-deleting ALL pods simultaneously (grace-period=0 → clean all-empty state)")
			// Force (non-graceful): every EmptyDir is wiped at once and no pod lingers to
			// act as an accidental survivor, so the cluster genuinely returns all-fresh —
			// the strongest form of the wipe and the one that most cleanly tests re-bootstrap.
			_, err := deletePodsWithLabelMode(testNamespace, "app.kubernetes.io/instance="+crName, false)
			Expect(err).NotTo(HaveOccurred())

			expectRecovered(crName, 6*time.Minute)
		})
	})

	// --- Flavor B: all redis processes kill-9'd at once (nodes.conf survives) ------
	Context("All redis processes kill-9'd at once (mass container crash)", Ordered, func() {
		var crName string
		BeforeAll(func() { crName = fmt.Sprintf("wipe-kill9-%d", time.Now().Unix()); deployCluster(crName) })
		AfterAll(func() { cleanup(crName) })

		It("re-bootstraps from a mass container crash (operator recycles the parked pods)", func() {
			writeCanary(crName, "canary-kill9", "pre-wipe")

			By("kill -9 every redis process (pods/IPs/EmptyDir survive → nodes.conf survives)")
			// Sequential per pod (killPodProcess launches a privileged hostPID helper each),
			// but all land within the startup script's 60s STEP-3 yield window, so every master
			// re-enters RESTART_DETECTED=true with no healthy peer to confirm demotion and parks
			// → CrashLoopBackOff (the mutual-yield deadlock). The pods never become Ready on
			// their own, so the operator's wipe-recovery (recoverClusterWipeDeadlock) must, after
			// the cooldown, recycle the stuck redis-down pods → they reschedule fresh → the
			// normal repair loop re-bootstraps. Without that recovery this deadlocks forever.
			for _, pod := range clusterPodNames(crName, clusterShards, clusterReplicasPerShard) {
				killPodProcess(testNamespace, pod)
			}

			// Budget covers the ~120s recovery cooldown plus reschedule + re-bootstrap.
			expectRecovered(crName, 8*time.Minute)
		})
	})

	// --- Partial wipe: a surviving data-holder must NOT be recycled (LR-003 guard) ----
	// The safety property behind recycling stuck pods: recycle ONLY not-Ready, crash-looping
	// (redis-down ⇒ dataless) pods, NEVER a Ready pod that may hold the only copy of a shard's
	// data. Here we keep exactly one shard's replica alive (Ready, holding replicated data)
	// while kill-9'ing everything else, and assert that shard's data survives recovery — i.e.
	// the operator promoted the survivor and never recycled it. This is the explicit regression
	// guard for the ADR-001 / LR-003 crash-protection the fix must not undo.
	Context("Partial wipe keeps a surviving data-holder", Ordered, func() {
		var crName string
		BeforeAll(func() { crName = fmt.Sprintf("wipe-partial-%d", time.Now().Unix()); deployCluster(crName) })
		AfterAll(func() { cleanup(crName) })

		It("preserves the surviving replica's data and never recycles it", func() {
			const key = "survivor-shard-key"
			const val = "must-survive"

			By("writing a key and locating the shard that owns it")
			cmd := exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
				"-n", testNamespace, "-c", "redis", "--", "redis-cli", "-c", "SET", key, val)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			slotOut, err := utils.Run(exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
				"-n", testNamespace, "-c", "redis", "--", "redis-cli", "CLUSTER", "KEYSLOT", key))
			Expect(err).NotTo(HaveOccurred())
			slot, err := strconv.Atoi(strings.TrimSpace(slotOut))
			Expect(err).NotTo(HaveOccurred())

			survivorShard := -1
			for i, rng := range redisclient.GenerateSlotRanges(clusterShards) {
				if slot >= rng.Start && slot <= rng.End {
					survivorShard = i
					break
				}
			}
			Expect(survivorShard).To(BeNumerically(">=", 0), "could not map slot to a shard")
			survivor := clusterReplicaPod(crName, survivorShard, 1)
			AddReportEntry("survivor-pod", survivor)

			By(fmt.Sprintf("waiting for the survivor replica %s to hold the replicated data", survivor))
			Eventually(func(g Gomega) {
				out, _ := utils.Run(exec.Command("kubectl", "exec", survivor,
					"-n", testNamespace, "-c", "redis", "--", "redis-cli", "DBSIZE"))
				g.Expect(strings.TrimSpace(out)).NotTo(Equal("0"))
			}, 30*time.Second, 2*time.Second).Should(Succeed(), "survivor replica never received the data")

			By("kill -9 every pod EXCEPT the survivor replica (it stays Ready, holding data)")
			for _, pod := range clusterPodNames(crName, clusterShards, clusterReplicasPerShard) {
				if pod == survivor {
					continue
				}
				killPodProcess(testNamespace, pod)
			}

			expectRecovered(crName, 8*time.Minute)

			By("the surviving shard's data must be intact after recovery (survivor promoted, never recycled)")
			Eventually(func(g Gomega) {
				out, err := utils.Run(exec.Command("kubectl", "exec", clusterMasterPod(crName, 0),
					"-n", testNamespace, "-c", "redis", "--", "redis-cli", "-c", "GET", key))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal(val))
			}, 1*time.Minute, 3*time.Second).Should(Succeed(),
				"surviving replica's data was lost — it was wrongly recycled or not promoted before slot assignment")
		})
	})
})
