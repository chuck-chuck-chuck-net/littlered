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
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
	"github.com/littlered-operator/littlered-operator/test/utils"
)

// clusterMasterPod returns shard K's master pod name ({crName}-shard-K-0).
// In the per-shard-StatefulSet model (0.3.0) each shard is its own StatefulSet
// {crName}-shard-K with pod ordinal 0 the master and 1..R the replicas.
func clusterMasterPod(crName string, shard int) string {
	return fmt.Sprintf("%s-shard-%d-0", crName, shard)
}

// clusterReplicaPod returns shard K's replica pod name for replica ordinal 1..R.
func clusterReplicaPod(crName string, shard, replicaOrdinal int) string {
	return fmt.Sprintf("%s-shard-%d-%d", crName, shard, replicaOrdinal)
}

// clusterPodNames enumerates every cluster pod name in shard/ordinal order:
// shard 0's pods (0..R), then shard 1's, and so on.
func clusterPodNames(crName string, shards, replicasPerShard int) []string {
	names := make([]string, 0, shards*(1+replicasPerShard))
	for k := 0; k < shards; k++ {
		for o := 0; o <= replicasPerShard; o++ {
			names = append(names, fmt.Sprintf("%s-shard-%d-%d", crName, k, o))
		}
	}
	return names
}

// clusterPodNamesForTotal enumerates cluster pod names given a total node count,
// deriving replicasPerShard from the suite-wide clusterShards.
func clusterPodNamesForTotal(crName string, totalNodes int) []string {
	replicasPerShard := totalNodes/clusterShards - 1
	return clusterPodNames(crName, clusterShards, replicasPerShard)
}

// verifyClusterTopologySync cross-validates the Operator's reported Status
// against the actual ground truth from Redis 'CLUSTER NODES'.
func verifyClusterTopologySync(namespace, crName string, expectedNodes int) {
	By(fmt.Sprintf("verifying that Operator status for %s matches actual Redis topology", crName))

	Eventually(func(g Gomega) {
		// 1. Get ground truth from any available pod
		var clusterNodesOutput string
		var success bool
		for _, podName := range clusterPodNamesForTotal(crName, expectedNodes) {
			cmd := exec.Command("kubectl", "exec", podName, "-n", namespace, "-c", "redis", "--", "redis-cli", "CLUSTER", "NODES")
			output, err := utils.Run(cmd)
			if err == nil {
				clusterNodesOutput = output
				success = true
				break
			}
		}
		g.Expect(success).To(BeTrue(), "Failed to execute CLUSTER NODES on any pod")

		// 2. Parse Redis output into a map for easy comparison
		redisNodes := make(map[string]struct {
			role     string
			masterID string
			slots    string
		})

		lines := strings.Split(strings.TrimSpace(clusterNodesOutput), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 4 {
				continue
			}
			id := fields[0]
			flags := fields[2]
			masterID := fields[3]

			role := "master"
			if strings.Contains(flags, "slave") {
				role = "replica"
			}

			slots := ""
			if len(fields) > 8 {
				slots = strings.Join(fields[8:], ",")
			}

			redisNodes[id] = struct {
				role     string
				masterID string
				slots    string
			}{role: role, masterID: masterID, slots: slots}
		}

		// 3. Wait for Operator Status to match ground truth
		cmd := exec.Command("kubectl", "get", "littlered", crName, "-n", namespace, "-o", "json")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get LittleRed CR")

		var lr littleredv1alpha1.LittleRed
		err = json.Unmarshal([]byte(output), &lr)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to parse LittleRed JSON")

		g.Expect(lr.Status.Cluster).NotTo(BeNil(), "CR Status.Cluster is nil")
		g.Expect(len(lr.Status.Cluster.Nodes)).To(Equal(expectedNodes),
			fmt.Sprintf("Number of nodes in Status (%d) doesn't match expected (%d)", len(lr.Status.Cluster.Nodes), expectedNodes))

		for _, nodeStatus := range lr.Status.Cluster.Nodes {
			actual, exists := redisNodes[nodeStatus.NodeID]
			g.Expect(exists).To(BeTrue(), fmt.Sprintf("Status reports NodeID %s (pod %s) but it's missing from CLUSTER NODES output", nodeStatus.NodeID, nodeStatus.PodName))
			g.Expect(nodeStatus.Role).To(Equal(actual.role), fmt.Sprintf("Role mismatch for node %s", nodeStatus.PodName))

			if nodeStatus.Role == "replica" {
				g.Expect(nodeStatus.MasterNodeID).To(Equal(actual.masterID), fmt.Sprintf("MasterID mismatch for replica %s", nodeStatus.PodName))
			} else {
				g.Expect(nodeStatus.SlotRanges).NotTo(BeEmpty(), fmt.Sprintf("Master %s has no slots in Status", nodeStatus.PodName))
			}
		}

		// Shard-colocation invariant (ADR-007 / LR-020): each Redis shard must live inside
		// ONE shard StatefulSet — every replica's master must be a pod in the same shard
		// (…-shard-K-…). The shard-blind Step 4 reattach used to scramble this at bootstrap
		// (debug-artifacts-20260730); this is the guard that goes red on that regression.
		idToPod := make(map[string]string, len(lr.Status.Cluster.Nodes))
		for _, n := range lr.Status.Cluster.Nodes {
			idToPod[n.NodeID] = n.PodName
		}
		for _, n := range lr.Status.Cluster.Nodes {
			if n.Role != "replica" {
				continue
			}
			masterPod, ok := idToPod[n.MasterNodeID]
			g.Expect(ok).To(BeTrue(), fmt.Sprintf("replica %s: master %s not among nodes", n.PodName, n.MasterNodeID))
			rShard := redisclient.ShardIndexFromPodName(n.PodName)
			mShard := redisclient.ShardIndexFromPodName(masterPod)
			g.Expect(rShard).To(BeNumerically(">=", 0), fmt.Sprintf("replica %s is not in per-shard form", n.PodName))
			g.Expect(mShard).To(Equal(rShard), fmt.Sprintf(
				"shard-colocation violated: replica %s (shard %d) follows master %s (shard %d) — Redis shard spans two StatefulSets",
				n.PodName, rShard, masterPod, mShard))
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Operator status should eventually match Redis cluster topology")

	By("Topology sync validation passed")
}

// verifySentinelTopologySync cross-validates the Operator's reported Sentinel Status
// against the ground truth from the Sentinel nodes.
func verifySentinelTopologySync(namespace, crName string, expectedSentinels, expectedReplicas int) {
	By(fmt.Sprintf("verifying that Operator status for %s matches actual Sentinel topology", crName))

	Eventually(func(g Gomega) {
		// 1. Check master information from ALL sentinels to verify broadcast/sync
		var masterIPs []string
		for i := 0; i < expectedSentinels; i++ {
			sentinelPod := fmt.Sprintf("%s-sentinel-%d", crName, i)
			sentinelOutput, err := sentinelPortExec(namespace, sentinelPod, "SENTINEL", "master", e2eMasterName(namespace, crName))
			g.Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Failed to execute SENTINEL master on %s", sentinelPod))

			// Parse Sentinel output
			sentinelData := make(map[string]string)
			lines := strings.Split(strings.TrimSpace(sentinelOutput), "\n")
			for j := 0; j < len(lines)-1; j += 2 {
				sentinelData[lines[j]] = lines[j+1]
			}
			masterIPs = append(masterIPs, sentinelData["ip"])
		}

		// Verify all sentinels agree on the same master
		for i := 1; i < len(masterIPs); i++ {
			g.Expect(masterIPs[i]).To(Equal(masterIPs[0]), "Sentinel nodes disagree on master identity")
		}

		actualMasterIP := masterIPs[0]

		// 1b. Get replicas info from one sentinel to count only healthy ones
		sentinelPod := fmt.Sprintf("%s-sentinel-0", crName)
		replicasOutput, err := sentinelPortExec(namespace, sentinelPod, "SENTINEL", "replicas", e2eMasterName(namespace, crName))
		g.Expect(err).NotTo(HaveOccurred(), "Failed to execute SENTINEL replicas on sentinel pod")

		// Parse replicas output more robustly
		actualNumUpReplicas := 0
		// Sentinel output for replicas is a list of key-value pairs separated by name\n
		replicaBlocks := strings.Split(replicasOutput, "\nname\n")
		for i, block := range replicaBlocks {
			// First block might contain "name\n" at the start, handle it
			content := block
			if i == 0 {
				content = strings.TrimPrefix(content, "name\n")
			}
			if content == "" {
				continue
			}

			// A replica is UP if it doesn't have s_down or o_down flags
			if !strings.Contains(content, "s_down") && !strings.Contains(content, "o_down") {
				actualNumUpReplicas++
			}
		}

		g.Expect(actualNumUpReplicas).To(Equal(expectedReplicas),
			fmt.Sprintf("Sentinel reports %d up replicas, but expected %d", actualNumUpReplicas, expectedReplicas))

		// 2. Get Operator Status
		cmd := exec.Command("kubectl", "get", "littlered", crName, "-n", namespace, "-o", "json")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get LittleRed CR")

		var lr littleredv1alpha1.LittleRed
		err = json.Unmarshal([]byte(output), &lr)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to parse LittleRed JSON")

		g.Expect(lr.Status.Master).NotTo(BeNil(), "CR Status.Master is nil")

		// Sentinel reports IP in our strict identity model.
		masterPodName := lr.Status.Master.PodName
		g.Expect(masterPodName).NotTo(BeEmpty(), "Status.Master.PodName is empty")

		g.Expect(lr.Status.Master.IP).To(Equal(actualMasterIP), "Master IP mismatch in Status")

		g.Expect(int(lr.Status.Sentinels.Total)).To(Equal(expectedSentinels), "Sentinel total count mismatch")
		g.Expect(int(lr.Status.Replicas.Total)).To(Equal(expectedReplicas), "Replica total count mismatch")

		// 3. Verify K8s Pod Labels correctly reflect roles
		cmd = exec.Command("kubectl", "get", "pods", "-n", namespace, "-l", "app.kubernetes.io/instance="+crName+",app.kubernetes.io/component=redis", "-o", "jsonpath={.items[*].metadata.name}")
		allPodsOutput, _ := utils.Run(cmd)
		allPods := strings.Fields(allPodsOutput)

		for _, pod := range allPods {
			cmd = exec.Command("kubectl", "get", "pod", pod, "-n", namespace, "-o", "jsonpath={.metadata.labels['redis\\.chuck-chuck-chuck\\.net/role']}")
			role, _ := utils.Run(cmd)
			if pod == masterPodName {
				g.Expect(role).To(Equal("master"), fmt.Sprintf("Pod %s is master but lacks master label", pod))
			} else {
				g.Expect(role).To(Equal("replica"), fmt.Sprintf("Pod %s should be replica but label is %s", pod, role))
			}
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed(), "Operator status and labels should eventually match Sentinel topology")

	By("Sentinel topology sync validation passed")
}

// getPodNodeID returns the current Redis Cluster NodeID of a pod.
func getPodNodeID(namespace, podName string) (string, error) {
	cmd := exec.Command("kubectl", "exec", podName, "-n", namespace, "-c", "redis", "--", "redis-cli", "CLUSTER", "MYID")
	out, err := utils.Run(cmd)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// getPodRunID returns the current Redis RunID of a pod.
// This is useful for Standalone and Sentinel modes where NodeID is not available.
func getPodRunID(namespace, podName string) (string, error) {
	output, err := redisExec(namespace, podName, "INFO", "server")
	if err != nil {
		return "", err
	}

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "run_id:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "run_id:")), nil
		}
	}
	return "", fmt.Errorf("run_id not found in INFO server output")
}

// waitForShardMasterChange waits until the master NodeID for a specific shard (identified by a slot)
// changes from the oldMasterNodeID. This is a robust way to verify failover or healing.
func waitForShardMasterChange(namespace, queryPod string, slot int, oldMasterNodeID string) {
	By(fmt.Sprintf("waiting for shard master for slot %d to change from %s", slot, oldMasterNodeID))
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "exec", queryPod,
			"-n", namespace, "-c", "redis", "--",
			"redis-cli", "CLUSTER", "NODES")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get CLUSTER NODES")

		var currentMasterID string
		lines := strings.Split(strings.TrimSpace(output), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 8 {
				continue
			}

			// Field 2 is flags, must contain "master" and not "fail"
			flags := fields[2]
			if !strings.Contains(flags, "master") || strings.Contains(flags, "fail") {
				continue
			}

			// Fields from 8 onwards are slot ranges
			for j := 8; j < len(fields); j++ {
				slotPart := fields[j]
				// Handle range (e.g. "0-5460") or single slot (e.g. "123")
				var start, end int
				if strings.Contains(slotPart, "-") {
					fmt.Sscanf(slotPart, "%d-%d", &start, &end)
				} else {
					fmt.Sscanf(slotPart, "%d", &start)
					end = start
				}

				if slot >= start && slot <= end {
					currentMasterID = fields[0]
					break
				}
			}
			if currentMasterID != "" {
				break
			}
		}

		g.Expect(currentMasterID).NotTo(BeEmpty(), fmt.Sprintf("Slot %d is currently unassigned (shard master missing)", slot))
		g.Expect(currentMasterID).NotTo(Equal(oldMasterNodeID), "Shard master has not changed yet")
	}, 3*time.Minute, 5*time.Second).Should(Succeed(), "Shard master should change within 3 minutes")
}

// waitForClusterFailureDetected waits for:
// 1. At least one node to be marked as fail or pfail
// 2. OR for the number of known nodes to be less than expected
// 3. OR for any of the specific victimNodeIDs to disappear from the mesh (if provided)
// This prevents false-positive recovery checks against stale pre-failure state.
func waitForClusterFailureDetected(namespace, crName string, queryPod string, expectedNodes int, victimNodeIDs []string) {
	By(fmt.Sprintf("waiting for cluster to detect failure via pod %s (expecting < %d nodes, fail flag, or some victims gone)", queryPod, expectedNodes))

	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "exec", queryPod,
			"-n", namespace, "-c", "redis", "--",
			"redis-cli", "CLUSTER", "NODES")
		output, err := utils.Run(cmd)
		if err != nil {
			return // Retry on connection failures
		}

		lines := strings.Split(strings.TrimSpace(output), "\n")
		currentCount := len(lines)

		// Success if count decreased (Operator already healed)
		if currentCount < expectedNodes {
			return
		}

		// Success if any victim ID is gone
		if len(victimNodeIDs) > 0 {
			allVictimsFound := true
			for _, vid := range victimNodeIDs {
				foundThisVictim := false
				for _, line := range lines {
					if strings.Contains(line, vid) {
						foundThisVictim = true
						break
					}
				}
				if !foundThisVictim {
					// At least one victim is gone! Success.
					return
				}
			}
			_ = allVictimsFound
		}

		// Success if any node is in fail/pfail state
		foundFail := false
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				flags := fields[2]
				if strings.Contains(flags, "fail") || strings.Contains(flags, "pfail") {
					foundFail = true
					break
				}
			}
		}
		g.Expect(foundFail).To(BeTrue(), "Expected failure detection (fail flag, count decrease, or victim gone)")
	}, 45*time.Second, 2*time.Second).Should(Succeed(),
		"Cluster should detect node failure within 45s")
}

// getShardGroups returns a list of pod names grouped by their shard.
func getShardGroups(namespace, crName string, totalNodes int) ([][]string, error) {
	nodeIDToPodName := make(map[string]string)

	podNames := clusterPodNamesForTotal(crName, totalNodes)
	for _, podName := range podNames {
		cmd := exec.Command("kubectl", "exec", podName, "-n", namespace, "-c", "redis", "--", "redis-cli", "CLUSTER", "MYID")
		output, err := utils.Run(cmd)
		if err != nil {
			return nil, fmt.Errorf("failed to get ID for pod %s: %w", podName, err)
		}
		nodeIDToPodName[strings.TrimSpace(output)] = podName
	}

	var clusterNodesOutput string
	for _, podName := range podNames {
		cmd := exec.Command("kubectl", "exec", podName, "-n", namespace, "-c", "redis", "--", "redis-cli", "CLUSTER", "NODES")
		var err error
		clusterNodesOutput, err = utils.Run(cmd)
		if err == nil {
			break
		}
	}
	if clusterNodesOutput == "" {
		return nil, fmt.Errorf("failed to get CLUSTER NODES from any pod")
	}

	shardMap := make(map[string][]string)

	type nodeInfo struct {
		id       string
		role     string
		masterID string
	}
	nodes := []nodeInfo{}

	lines := strings.Split(strings.TrimSpace(clusterNodesOutput), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		id := fields[0]
		flags := fields[2]
		masterID := fields[3]

		role := "master"
		if strings.Contains(flags, "slave") {
			role = "replica"
		}
		nodes = append(nodes, nodeInfo{id: id, role: role, masterID: masterID})
	}

	for _, n := range nodes {
		podName, ok := nodeIDToPodName[n.id]
		if !ok {
			continue
		}

		targetShardID := n.id
		if n.role == "replica" {
			targetShardID = n.masterID
		}
		shardMap[targetShardID] = append(shardMap[targetShardID], podName)
	}

	result := make([][]string, 0, len(shardMap))
	for _, group := range shardMap {
		result = append(result, group)
	}

	return result, nil
}

// clusterTotalNodes returns the total node count for a cluster with the given replicas per shard.
func clusterTotalNodes(replicasPerShard int) int {
	return clusterShards * (1 + replicasPerShard)
}

// clusterCR returns a LittleRed cluster CR YAML string with the configured shard count.
// extraClusterFields adds fields under spec.cluster (e.g. "    clusterNodeTimeout: 10000\n").
// extraSpecFields adds fields under spec (e.g. "  config:\n    maxmemory: ...\n").
func clusterCR(name string, replicasPerShard int, extraClusterFields, extraSpecFields string) string {
	cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: cluster
  cluster:
    shards: %d
    replicasPerShard: %d
`, name, testNamespace, clusterShards, replicasPerShard)
	if extraClusterFields != "" {
		cr += extraClusterFields
	}
	if extraSpecFields != "" {
		cr += extraSpecFields
	}
	return cr
}
