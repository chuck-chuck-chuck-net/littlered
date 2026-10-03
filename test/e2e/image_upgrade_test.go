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

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	"github.com/chuck-chuck-chuck-net/littlered/test/utils"
)

// Redis image upgrades must not lose data (#95).
//
// Storage is EmptyDir (pillar 3.1), so an image upgrade is a rolling replacement of every
// pod and the dataset survives only through replication: each replaced pod comes back
// empty and full-syncs from a live peer, and a master is replaced only after a replica
// has taken over. Nothing short of doing it proves that the two chains below hold for the
// two upgrade shapes a user actually performs:
//
//   - a patch upgrade inside one line (7.4.3 -> 7.4.8, the issue's own example), and
//   - a major upgrade (7.4.8 -> the operator default, 8.4.2).
//
// The major step is the one with teeth. A replica on the NEWER version can load the
// OLDER master's RDB, so rolling the replicas first and the master last works; the
// reverse (a downgrade) does not, and the operator does not guard against it — see
// USAGE "Upgrading the Redis image". Each step asserts: every pod was replaced (UID),
// every pod runs the new version (INFO server), the instance is Running and its topology
// is sound, and every seeded key still reads back with its exact value.
const (
	imageUpgradeFrom  = "7.4.3"
	imageUpgradePatch = "7.4.8"
	// imageUpgradeMajor is the operator default, so the chain ends where a user who
	// drops the pin would land.
	imageUpgradeMajor = littleredv1alpha1.DefaultImageTag

	imageUpgradeSentinelKeys        = 300
	imageUpgradeClusterKeysPerShard = 20

	// A rollout replaces every pod serially (sentinel: two StatefulSets; cluster: one
	// shard at a time, state-gated per ADR-017) and each replacement full-syncs. Generous.
	imageRolloutBound = 12 * time.Minute
)

var _ = Describe("Redis Image Upgrade — Sentinel Mode", Label("sentinel", "image-upgrade"), Ordered, func() {
	var (
		crName string
		data   map[string]string
	)

	BeforeAll(func() {
		crName = fmt.Sprintf("img-sentinel-%d", time.Now().Unix())
		AddReportEntry("cr:" + crName)
		registerE2EAuth(crName)
		cr := e2eAuthSecretDoc(crName) + fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
  image:
    tag: %q
%s  sentinel:
    masterName: %s
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace, imageUpgradeFrom, e2eAuthSpecYAML(crName), e2eMasterName(testNamespace, crName))
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the instance to be Running on " + imageUpgradeFrom)
		Eventually(func(g Gomega) { g.Expect(getPhase(crName)).To(Equal("Running")) },
			4*time.Minute, 5*time.Second).Should(Succeed())
		verifySentinelTopologySync(testNamespace, crName, 3, 2)
		expectRedisVersionOnPods(crName, sentinelModePods(crName), imageUpgradeFrom)

		By("seeding the dataset on the master")
		data = seedSentinelDataset(getMasterPod(crName), imageUpgradeSentinelKeys)
	})

	AfterAll(func() {
		if debugOnFailure && suiteOrSpecFailed() {
			return
		}
		_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace,
			"--ignore-not-found", "--timeout=2m"))
	})

	It("keeps every key across a patch upgrade "+imageUpgradeFrom+" -> "+imageUpgradePatch, func() {
		upgradeImageAndAssert(crName, sentinelModePods(crName), imageUpgradePatch, func() {
			verifySentinelTopologySync(testNamespace, crName, 3, 2)
			verifySentinelDataset(getMasterPod(crName), data)
		})
	})

	It("keeps every key across a major upgrade "+imageUpgradePatch+" -> "+imageUpgradeMajor, func() {
		upgradeImageAndAssert(crName, sentinelModePods(crName), imageUpgradeMajor, func() {
			verifySentinelTopologySync(testNamespace, crName, 3, 2)
			verifySentinelDataset(getMasterPod(crName), data)
		})
	})
})

var _ = Describe("Redis Image Upgrade — Cluster Mode", Label("cluster", "image-upgrade"), Ordered, func() {
	var (
		crName string
		data   map[string]string
		pods   []string
	)

	BeforeAll(func() {
		crName = fmt.Sprintf("img-cluster-%d", time.Now().Unix())
		AddReportEntry("cr:" + crName)
		pods = clusterPodNames(crName, clusterShards, clusterReplicasPerShard)
		cr := clusterCR(crName, clusterReplicasPerShard, "", fmt.Sprintf("  image:\n    tag: %q\n", imageUpgradeFrom))
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the cluster to be Running on " + imageUpgradeFrom)
		Eventually(func(g Gomega) { g.Expect(getPhase(crName)).To(Equal("Running")) },
			5*time.Minute, 5*time.Second).Should(Succeed())
		expectRedisVersionOnPods(crName, pods, imageUpgradeFrom)

		By("seeding a dataset that spans every shard")
		data = writeDatasetSpanningShards(clusterMasterPod(crName, 0), imageUpgradeClusterKeysPerShard)
	})

	AfterAll(func() {
		if debugOnFailure && suiteOrSpecFailed() {
			return
		}
		_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace,
			"--ignore-not-found", "--timeout=2m"))
	})

	It("keeps every key across a patch upgrade "+imageUpgradeFrom+" -> "+imageUpgradePatch, func() {
		upgradeImageAndAssert(crName, pods, imageUpgradePatch, func() {
			expectClusterServingAllSlots(crName)
			verifyDataset(clusterMasterPod(crName, 0), data)
		})
	})

	It("keeps every key across a major upgrade "+imageUpgradePatch+" -> "+imageUpgradeMajor, func() {
		upgradeImageAndAssert(crName, pods, imageUpgradeMajor, func() {
			expectClusterServingAllSlots(crName)
			verifyDataset(clusterMasterPod(crName, 0), data)
		})
	})
})

// --- helpers -----------------------------------------------------------------

// upgradeImageAndAssert patches spec.image.tag, waits until every listed pod has been
// replaced and the instance is Running again, asserts every pod runs the new version, and
// then runs the mode-specific checks (topology + dataset). The rollout time is reported.
func upgradeImageAndAssert(crName string, pods []string, newTag string, modeChecks func()) {
	By("recording the pre-upgrade pod UIDs")
	before := make(map[string]string, len(pods))
	for _, p := range pods {
		before[p] = podUID(testNamespace, p)
		Expect(before[p]).NotTo(BeEmpty(), "pod %s has no UID (missing?)", p)
	}

	By("setting spec.image.tag to " + newTag)
	t0 := time.Now()
	_, err := utils.Run(exec.Command("kubectl", "patch", "littlered", crName, "-n", testNamespace,
		"--type=merge", "-p", fmt.Sprintf(`{"spec":{"image":{"tag":%q}}}`, newTag)))
	Expect(err).NotTo(HaveOccurred())

	By("waiting for every pod to be replaced")
	Eventually(func(g Gomega) {
		for _, p := range pods {
			uid := podUID(testNamespace, p)
			g.Expect(uid).NotTo(BeEmpty(), "pod %s not present", p)
			g.Expect(uid).NotTo(Equal(before[p]), "pod %s not yet replaced", p)
		}
	}, imageRolloutBound, 5*time.Second).Should(Succeed(), "image rollout did not replace every pod")

	By("waiting for the instance to be Running again")
	Eventually(func(g Gomega) { g.Expect(getPhase(crName)).To(Equal("Running")) },
		5*time.Minute, 5*time.Second).Should(Succeed())
	rollout := time.Since(t0).Round(time.Second)
	AddReportEntry("image-upgrade:"+crName+":"+newTag, rollout.String())
	_, _ = fmt.Fprintf(GinkgoWriter, "image-upgrade: %s -> %s rolled %d pods in %s\n", crName, newTag, len(pods), rollout)

	By("every pod runs " + newTag)
	expectRedisVersionOnPods(crName, pods, newTag)

	modeChecks()
}

// sentinelModePods lists the six pods of a sentinel instance: three data pods and three
// Sentinels. Both StatefulSets share spec.image, so both roll on a tag change.
func sentinelModePods(crName string) []string {
	pods := make([]string, 0, 6)
	for i := range 3 {
		pods = append(pods, fmt.Sprintf("%s-redis-%d", crName, i))
	}
	for i := range 3 {
		pods = append(pods, fmt.Sprintf("%s-sentinel-%d", crName, i))
	}
	return pods
}

// expectRedisVersionOnPods asserts INFO server reports the given version on every pod,
// reading the sentinel port on Sentinel pods and the redis port everywhere else. The
// running process is the ground truth; the pod's image field only says what was asked for.
// A Sentinel pod is recognised by the Sentinel StatefulSet's name prefix, not by a bare
// substring: a CR named "img-sentinel-…" puts "-sentinel-" into every pod name.
func expectRedisVersionOnPods(crName string, pods []string, version string) {
	want := "redis_version:" + version
	sentinelPrefix := crName + "-sentinel-"
	Eventually(func(g Gomega) {
		for _, p := range pods {
			var out string
			var err error
			if strings.HasPrefix(p, sentinelPrefix) {
				out, err = sentinelPortExec(testNamespace, p, "INFO", "server")
			} else {
				out, err = redisExec(testNamespace, p, "INFO", "server")
			}
			g.Expect(err).NotTo(HaveOccurred(), "INFO server on %s", p)
			g.Expect(out).To(ContainSubstring(want), "pod %s is not running %s", p, version)
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// seedSentinelDataset writes n keys through the master in one MSET and returns them.
func seedSentinelDataset(masterPod string, n int) map[string]string {
	data := make(map[string]string, n)
	args := []string{"MSET"}
	for i := 1; i <= n; i++ {
		k := fmt.Sprintf("img:%d", i)
		v := fmt.Sprintf("value-%d", i)
		data[k] = v
		args = append(args, k, v)
	}
	out, err := redisExec(testNamespace, masterPod, args...)
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.TrimSpace(out)).To(Equal("OK"))
	// The seed is only a baseline once it is on a replica too: a master replaced before
	// its replicas have the keys would lose them for reasons that are not an upgrade's.
	Eventually(func(g Gomega) {
		info, err := redisExec(testNamespace, masterPod, "INFO", "replication")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(info).To(ContainSubstring("connected_slaves:2"))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
	return data
}

// verifySentinelDataset asserts every seeded key still exists with its exact value, read
// through the current master. Presence is counted with one EXISTS over all keys — a single
// integer that survives any output trimming and names exactly how many keys were lost —
// and values are compared with one MGET only once every key is present, when MGET's
// output has no nil lines left to be collapsed by the exec helper.
func verifySentinelDataset(masterPod string, data map[string]string) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	Eventually(func(g Gomega) {
		out, err := redisExec(testNamespace, masterPod, append([]string{"EXISTS"}, keys...)...)
		g.Expect(err).NotTo(HaveOccurred())
		present, err := strconv.Atoi(strings.TrimSpace(out))
		g.Expect(err).NotTo(HaveOccurred(), "EXISTS did not return a count: %q", out)
		g.Expect(present).To(Equal(len(keys)), "%d of %d keys lost across the upgrade", len(keys)-present, len(keys))

		out, err = redisExec(testNamespace, masterPod, append([]string{"MGET"}, keys...)...)
		g.Expect(err).NotTo(HaveOccurred())
		got := strings.Split(strings.TrimSpace(out), "\n")
		g.Expect(got).To(HaveLen(len(keys)), "MGET returned %d values for %d keys", len(got), len(keys))
		changed := 0
		for i, k := range keys {
			if strings.TrimSpace(got[i]) != data[k] {
				changed++
			}
		}
		g.Expect(changed).To(BeZero(), "%d of %d keys changed value across the upgrade", changed, len(keys))
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// expectClusterServingAllSlots asserts cluster_state:ok with all 16384 slots assigned.
func expectClusterServingAllSlots(crName string) {
	Eventually(func(g Gomega) {
		out, err := redisExec(testNamespace, clusterMasterPod(crName, 0), "CLUSTER", "INFO")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(ContainSubstring("cluster_state:ok"))
		g.Expect(out).To(ContainSubstring("cluster_slots_assigned:16384"))
	}, 3*time.Minute, 5*time.Second).Should(Succeed())
}
