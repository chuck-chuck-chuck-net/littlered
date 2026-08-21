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

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
	"github.com/littlered-operator/littlered-operator/test/utils"
)

// schedulableNodeCount returns the number of Ready nodes that carry no NoSchedule taint —
// i.e. nodes a shard pod could actually land on. The per-shard spread is only demonstrable
// when there are at least as many domains as a shard has pods.
func schedulableNodeCount() int {
	// Names of Ready=True nodes.
	cmd := exec.Command("kubectl", "get", "nodes",
		"-o", `jsonpath={range .items[?(@.status.conditions[-1].type=="Ready")]}{.metadata.name}{"\n"}{end}`)
	out, err := utils.Run(cmd)
	if err != nil {
		return 0
	}
	count := 0
	for _, name := range strings.Fields(out) {
		taints, _ := utils.Run(exec.Command("kubectl", "get", "node", name,
			"-o", `jsonpath={range .spec.taints[?(@.effect=="NoSchedule")]}{.key}{" "}{end}`))
		if strings.TrimSpace(taints) == "" {
			count++
		}
	}
	return count
}

var _ = Describe("Cluster Mode Per-Shard Placement", Label("placement", "cluster"), Ordered, func() {
	const crName = "place-cluster"

	AfterAll(func() {
		if debugOnFailure && suiteOrSpecFailed() {
			return
		}
		cmd := exec.Command("kubectl", "delete", "littlered", crName,
			"-n", testNamespace, "--ignore-not-found", "--timeout=2m")
		_, _ = utils.Run(cmd)
	})

	It("spreads each shard's pods across distinct nodes when shardAntiAffinity is set", func() {
		AddReportEntry("cr:" + crName)

		// A hard (DoNotSchedule) per-shard hostname spread needs at least 1+replicasPerShard
		// schedulable nodes for a shard's pods to occupy distinct nodes. Skip on clusters too
		// small to demonstrate it (the soft default would never Pending, but distinct-node
		// placement can't be asserted deterministically there).
		needed := 1 + clusterReplicasPerShard
		if n := schedulableNodeCount(); n < needed {
			Skip(fmt.Sprintf("need >= %d schedulable nodes to demonstrate per-shard spread, have %d", needed, n))
		}

		By("deploying a cluster with a hard per-shard hostname anti-affinity")
		cr := clusterCR(crName, clusterReplicasPerShard, "", `  placement:
    shardAntiAffinity:
      topologyKey: kubernetes.io/hostname
      whenUnsatisfiable: DoNotSchedule
`)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the cluster to be running (pods must schedule despite the hard constraint)")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.phase}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Running"))
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying each shard's pods landed on distinct nodes")
		// pod -> node
		podNames := clusterPodNames(crName, clusterShards, clusterReplicasPerShard)
		nodeByShard := make(map[int][]string)
		for _, pod := range podNames {
			node, err := utils.Run(exec.Command("kubectl", "get", "pod", pod,
				"-n", testNamespace, "-o", "jsonpath={.spec.nodeName}"))
			Expect(err).NotTo(HaveOccurred())
			node = strings.TrimSpace(node)
			Expect(node).NotTo(BeEmpty(), "pod %s has no node assigned", pod)
			shard := redisclient.ShardIndexFromPodName(pod)
			Expect(shard).To(BeNumerically(">=", 0), "pod %s not in per-shard form", pod)
			nodeByShard[shard] = append(nodeByShard[shard], node)
		}
		for shard, nodes := range nodeByShard {
			seen := make(map[string]bool)
			for _, n := range nodes {
				Expect(seen[n]).To(BeFalse(),
					"shard %d has two pods on the same node %s — per-shard isolation violated", shard, n)
				seen[n] = true
			}
		}

		By("confirming Redis-shard colocation is still intact (lrctl verify)")
		verifyClusterTopologySync(testNamespace, crName, clusterTotalNodes(clusterReplicasPerShard))
	})
})
