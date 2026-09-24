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

	. "github.com/onsi/gomega" //nolint:revive // Gomega dot-import convention

	"github.com/chuck-chuck-chuck-net/littlered/test/chaos"
	"github.com/chuck-chuck-chuck-net/littlered/test/utils"
)

// singleSiteHarness drives a Redis Cluster in one Kubernetes cluster (the suite's ambient,
// pinned kube-context) and one namespace (testNamespace) — the historical shape of the suite.
// Every pod handle carries the empty context (== ambient), so its helpers are byte-for-byte the
// kubectl calls the suite always made.
type singleSiteHarness struct{}

func newSingleSiteHarness() *singleSiteHarness { return &singleSiteHarness{} }

func (h *singleSiteHarness) topology() string   { return "single" }
func (h *singleSiteHarness) instanceNS() string { return testNamespace }

func (h *singleSiteHarness) applyRedundantCR(crName string) error {
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
	return err
}

func (h *singleSiteHarness) waitRunning(crName string, timeout time.Duration) {
	Eventually(func(g Gomega) {
		out, err := kubectlCtx("", "get", "littlered", crName, "-n", testNamespace,
			"-o", "jsonpath={.status.phase}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal("Running"))
	}, timeout, 5*time.Second).Should(Succeed())
}

func (h *singleSiteHarness) deleteCR(crName string) {
	_, _ = kubectlCtx("", "delete", "littlered", crName, "-n", testNamespace,
		"--ignore-not-found", "--timeout=2m")
	_, _ = kubectlCtx("", "wait", "--for=delete", "pod",
		"-l", "app.kubernetes.io/instance="+crName, "-n", testNamespace, "--timeout=3m")
}

func (h *singleSiteHarness) expectedMasters() int  { return clusterShards }
func (h *singleSiteHarness) expectedReplicas() int { return clusterShards * clusterReplicasPerShard }
func (h *singleSiteHarness) expectedTotal() int    { return clusterTotalNodes(clusterReplicasPerShard) }

func (h *singleSiteHarness) queryPod(crName string) podHandle {
	return podHandle{name: clusterMasterPod(crName, 0)}
}

func (h *singleSiteHarness) execRedis(p podHandle, args ...string) (string, error) {
	full := append([]string{"exec", p.name, "-n", testNamespace, "-c", "redis", "--", "redis-cli"}, args...)
	return kubectlCtx(p.ctx, full...)
}

func (h *singleSiteHarness) deletePod(p podHandle, graceful bool) error {
	_, err := deletePodMode(testNamespace, p.name, graceful)
	return err
}

// resolveMasterForSlot maps the slot's serving-master IP back to a pod object name via the
// component=cluster pod list.
func (h *singleSiteHarness) resolveMasterForSlot(crName string, slot int) (podHandle, error) {
	nodes, err := h.execRedis(h.queryPod(crName), "CLUSTER", "NODES")
	if err != nil {
		return podHandle{}, err
	}
	ip := masterIPForSlot(nodes, slot)
	if ip == "" {
		return podHandle{}, fmt.Errorf("slot %d has no serving master", slot)
	}
	out, err := kubectlCtx("", "-n", testNamespace, "get", "pods",
		"-l", "app.kubernetes.io/component=cluster",
		"-o", "jsonpath={range .items[*]}{.status.podIP}={.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return podHandle{}, err
	}
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		kv := strings.SplitN(l, "=", 2)
		if len(kv) == 2 && kv[0] == ip {
			return podHandle{name: kv[1]}, nil
		}
	}
	return podHandle{}, fmt.Errorf("no pod found for master IP %s (slot %d)", ip, slot)
}

func (h *singleSiteHarness) verifyHealthy(crName string) {
	verifyClusterTopologySync(testNamespace, crName, h.expectedTotal())
}

func (h *singleSiteHarness) deployChaos(crName, name string, duration time.Duration) (chaosDeployment, error) {
	pod, err := deployChaosClient(testNamespace, name, crName+":6379", "chaos-"+name, true, duration)
	return chaosDeployment{ns: testNamespace, pod: pod}, err
}

func (h *singleSiteHarness) waitChaos(d chaosDeployment, timeout time.Duration) error {
	return waitForChaosClientCompleteCtx(d.ctx, d.ns, d.pod, timeout)
}

func (h *singleSiteHarness) chaosMetrics(d chaosDeployment) (*chaos.MetricsSnapshot, error) {
	return getChaosClientMetricsCtx(d.ctx, d.ns, d.pod)
}

func (h *singleSiteHarness) deleteChaos(d chaosDeployment) {
	deleteChaosClientCtx(d.ctx, d.ns, d.pod)
}
