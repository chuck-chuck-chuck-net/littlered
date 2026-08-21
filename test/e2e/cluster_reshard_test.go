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

// LR-018: the operator must recover a *consolidated-shard* cluster — one master owning
// more than one shard's slot range while another master sits slotless (empty) — by
// resharding the surplus range onto the empty master, PRESERVING KEYS. It picks the
// mechanism by capability: native atomic slot migration on Redis 8.4+, else the
// incremental MIGRATE dance.
//
// The Step 3 hardening means the operator will no longer *create* this state itself, so
// the test injects it: consolidate a shard onto another master (which demotes the emptied
// source to a replica), then turn a node into a fresh empty master (CLUSTER RESET SOFT +
// MEET, mimicking an EmptyDir-restarted node). The operator is paused during injection so
// the multi-step setup is deterministic.
//
// Runs by default (Label "reshard", not "extended") — this is exactly the class of defect
// unit tests cannot reach: it exercises the gather parsing a real mid-migration topology,
// which is where LR-018's latent ParseClusterNodes bug hid.
var _ = Describe("Cluster Mode Consolidated-Shard Reshard Recovery (LR-018)", Label("reshard", "cluster"), Ordered, func() {
	Context("native atomic slot migration (Redis 8.4+)", Ordered, func() {
		// Default image (8.4.x) → ASM path. 300 keys migrate atomically.
		consolidatedShardReshardSpecs("reshard-asm", "", 300, 0, true)
	})

	Context("pre-8.4 incremental dance", Ordered, func() {
		// Redis 7.4.0 → no ASM → dance path. A low per-reconcile key budget forces a
		// genuine multi-pass drain (300 keys / 100 per pass = 3 passes), exercising the
		// resume-from-markers path across reconciles.
		consolidatedShardReshardSpecs("reshard-dance", "7.4.0", 300, 100, false)
	})
})

// consolidatedShardReshardSpecs installs the BeforeAll/It/AfterAll for one tier. imageTag
// "" uses the operator default (ASM-capable); a value overrides spec.image.tag. When
// maxKeysPerReconcile > 0 it is set on the CR (advanced field) to bound the dance.
func consolidatedShardReshardSpecs(crName, imageTag string, numKeys, maxKeysPerReconcile int, wantASM bool) {
	sourcePod := clusterMasterPod(crName, 2) // shard-2's master ({crName}-shard-2-0) on a fresh bootstrap
	destPod := clusterMasterPod(crName, 0)   // shard-0's master; consolidation target during injection
	neutralPod := clusterMasterPod(crName, 1)

	BeforeAll(func() {
		AddReportEntry("cr:" + crName)

		extraCluster := ""
		if maxKeysPerReconcile > 0 {
			extraCluster = fmt.Sprintf("    reshardMaxKeysPerReconcile: %d\n", maxKeysPerReconcile)
		}
		extraSpec := `  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      memory: "256Mi"
`
		if imageTag != "" {
			extraSpec += fmt.Sprintf(`  image:
    registry: docker.io
    path: library/redis
    tag: %q
`, imageTag)
		}

		By(fmt.Sprintf("creating a %d-shard cluster (image tag %q)", clusterShards, imageTag))
		cr := clusterCR(crName, clusterReplicasPerShard, extraCluster, extraSpec)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for cluster to be Running")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.phase}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("confirming the engine's atomic-slot-migration capability matches the tier")
		info, err := redisExec(testNamespace, destPod, "CLUSTER", "INFO")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Contains(info, "cluster_slot_migration")).To(Equal(wantASM),
			"expected ASM support == %v for this tier", wantASM)
	})

	AfterAll(func() {
		// Always make sure we did not leave the operator scaled down.
		scaleOperator(1)
		if debugOnFailure && suiteOrSpecFailed() {
			By("skipping cleanup due to failure and DEBUG_ON_FAILURE=true")
			return
		}
		By("cleaning up cluster CR")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", crName,
			"-n", testNamespace, "--ignore-not-found", "--timeout=2m"))
	})

	It("reshards a consolidated shard back onto the empty master, preserving keys", func() {
		By("locating shard-2's slot range and a hash tag that routes into it")
		lo, hi := firstOwnedSlotRange(sourcePod)
		tag := findHashTagInSlotRange(sourcePod, lo, hi)
		slotCount := hi - lo + 1

		By(fmt.Sprintf("writing %d keys into shard-2 (tag %s, slots %d-%d)", numKeys, tag, lo, hi))
		writeCmd := fmt.Sprintf("for i in $(seq 1 %d); do echo \"SET %s:$i v:$i\"; done | redis-cli --pipe", numKeys, tag)
		_, err := utils.Run(exec.Command("kubectl", "exec", sourcePod, "-n", testNamespace, "-c", "redis", "--", "sh", "-c", writeCmd))
		Expect(err).NotTo(HaveOccurred())
		Expect(countKeysInSlot(sourcePod, keySlot(sourcePod, tag))).To(Equal(numKeys))

		sourceID, err := getPodNodeID(testNamespace, sourcePod)
		Expect(err).NotTo(HaveOccurred())
		destID, err := getPodNodeID(testNamespace, destPod)
		Expect(err).NotTo(HaveOccurred())

		By("pausing the operator to inject the consolidated state deterministically")
		scaleOperator(0)

		By("consolidating shard-2's slots (with data) onto shard-0's master via redis-cli --cluster reshard")
		// Connect via a neutral node's real IP (never 127.0.0.1, which makes the tool
		// address the destination as loopback and MIGRATE to the source itself).
		reshard := exec.Command("kubectl", "exec", destPod, "-n", testNamespace, "-c", "redis", "--",
			"redis-cli", "--cluster", "reshard", podIP(neutralPod)+":6379",
			"--cluster-from", sourceID, "--cluster-to", destID,
			"--cluster-slots", strconv.Itoa(slotCount), "--cluster-yes")
		out, err := utils.Run(reshard)
		Expect(err).NotTo(HaveOccurred(), "reshard failed: %s", out)

		By("turning shard-2's master into a fresh empty master (RESET SOFT + MEET)")
		_, err = redisExec(testNamespace, sourcePod, "CLUSTER", "RESET", "SOFT")
		Expect(err).NotTo(HaveOccurred())
		_, _ = redisExec(testNamespace, sourcePod, "FLUSHALL")
		_, err = redisExec(testNamespace, destPod, "CLUSTER", "MEET", podIP(sourcePod), "6379")
		Expect(err).NotTo(HaveOccurred())
		_, _ = redisExec(testNamespace, neutralPod, "CLUSTER", "MEET", podIP(sourcePod), "6379")

		By("verifying the injected consolidated state (one fewer slot-owning master, an empty master)")
		Eventually(func(g Gomega) {
			g.Expect(clusterSize(destPod)).To(Equal(clusterShards - 1))
		}, 30*time.Second, 2*time.Second).Should(Succeed())

		By("resuming the operator and letting Step 3b reshard the surplus range back")
		scaleOperator(1)

		By("waiting for a healthy, fully-sharded cluster again")
		Eventually(func(g Gomega) {
			g.Expect(clusterSize(neutralPod)).To(Equal(clusterShards))
			out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.phase}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying the surplus range and all its keys landed on the resharded master (no data loss)")
		// The only empty master was shard-2's, so PlanReshard relocates shard-2 back to it.
		Eventually(func(g Gomega) {
			g.Expect(countKeysInSlot(sourcePod, keySlot(sourcePod, tag))).To(Equal(numKeys))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("spot-checking key values survived the migration")
		for _, i := range []int{1, numKeys} {
			v, err := redisExec(testNamespace, destPod, "-c", "GET", fmt.Sprintf("%s:%d", tag, i))
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(v)).To(Equal(fmt.Sprintf("v:%d", i)))
		}

		By("verifying no migrating/importing markers remain (clean flip)")
		nodes, err := redisExec(testNamespace, sourcePod, "CLUSTER", "NODES")
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).NotTo(ContainSubstring("["), "leftover IMPORTING/MIGRATING markers")

		verifyClusterTopologySync(testNamespace, crName, clusterTotalNodes(clusterReplicasPerShard))
	})
}

// --- helpers -----------------------------------------------------------------

// scaleOperator scales the operator Deployment and waits for the change to take effect.
// Used to pause the operator while a consolidated topology is injected by hand.
func scaleOperator(replicas int) {
	_, err := utils.Run(exec.Command("kubectl", "scale", "deployment/littlered",
		"-n", operatorNamespace, fmt.Sprintf("--replicas=%d", replicas)))
	Expect(err).NotTo(HaveOccurred())
	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("kubectl", "get", "deployment/littlered",
			"-n", operatorNamespace, "-o", "jsonpath={.status.availableReplicas}"))
		g.Expect(err).NotTo(HaveOccurred())
		got := strings.TrimSpace(out)
		if got == "" {
			got = "0"
		}
		g.Expect(got).To(Equal(strconv.Itoa(replicas)))
	}, 90*time.Second, 2*time.Second).Should(Succeed())
}

// podIP returns a pod's status.podIP.
func podIP(pod string) string {
	out, err := utils.Run(exec.Command("kubectl", "get", "pod", pod, "-n", testNamespace,
		"-o", "jsonpath={.status.podIP}"))
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(out)
}

// firstOwnedSlotRange parses the pod's own CLUSTER NODES line and returns its first owned
// slot range [lo,hi] (single slots return lo==hi), skipping [importing]/[migrating] tokens.
func firstOwnedSlotRange(pod string) (int, int) {
	out, err := redisExec(testNamespace, pod, "CLUSTER", "NODES")
	Expect(err).NotTo(HaveOccurred())
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "myself") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) <= 8 {
			break
		}
		for _, f := range fields[8:] {
			if strings.HasPrefix(f, "[") {
				continue
			}
			if lo, hi, ok := strings.Cut(f, "-"); ok {
				a, _ := strconv.Atoi(lo)
				b, _ := strconv.Atoi(hi)
				return a, b
			}
			n, _ := strconv.Atoi(f)
			return n, n
		}
	}
	Fail("no owned slot range found on " + pod)
	return 0, 0
}

// findHashTagInSlotRange returns a hash tag "{tN}" whose CLUSTER KEYSLOT falls in [lo,hi],
// so keys written with it concentrate on the master owning that range.
func findHashTagInSlotRange(pod string, lo, hi int) string {
	for i := 1; i <= 512; i++ {
		tag := fmt.Sprintf("{t%d}", i)
		if s := keySlot(pod, tag); s >= lo && s <= hi {
			return tag
		}
	}
	Fail(fmt.Sprintf("no hash tag found routing into slots %d-%d", lo, hi))
	return ""
}

func keySlot(pod, key string) int {
	out, err := redisExec(testNamespace, pod, "CLUSTER", "KEYSLOT", key)
	Expect(err).NotTo(HaveOccurred())
	n, err := strconv.Atoi(strings.TrimSpace(out))
	Expect(err).NotTo(HaveOccurred())
	return n
}

func countKeysInSlot(pod string, slot int) int {
	out, err := redisExec(testNamespace, pod, "CLUSTER", "COUNTKEYSINSLOT", strconv.Itoa(slot))
	Expect(err).NotTo(HaveOccurred())
	n, err := strconv.Atoi(strings.TrimSpace(out))
	Expect(err).NotTo(HaveOccurred())
	return n
}

func clusterSize(pod string) int {
	out, err := redisExec(testNamespace, pod, "CLUSTER", "INFO")
	Expect(err).NotTo(HaveOccurred())
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "cluster_size:"); ok {
			n, _ := strconv.Atoi(strings.TrimSpace(v))
			return n
		}
	}
	return -1
}
