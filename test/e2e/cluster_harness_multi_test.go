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
	"strings"
	"time"

	. "github.com/onsi/gomega" //nolint:revive // Gomega dot-import convention

	"github.com/littlered-operator/littlered-operator/test/chaos"
)

// multiSiteHarness drives a STRETCHED Redis Cluster across the M4b hub+spokes lab. Every pod
// handle carries its site's kube-context, so exec/delete route to the right cluster. The
// redundant CR uses sitesPerShard=2 with replicasPerShard=0: each shard's master and its single
// cross-site peer live in two DIFFERENT sites, so the shard survives a single-pod (or, in the
// takeover suite, a whole-site) loss — the multi-site analogue of single-site's 1 replica/shard.
type multiSiteHarness struct {
	reg           *msRegistry
	ns            string
	sitesPerShard int
	nodeTimeoutMs int
	siteNames     []string
}

func newMultiSiteHarness(reg *msRegistry, ns string) *multiSiteHarness {
	var names []string
	for _, s := range reg.orderedSites {
		names = append(names, s.name)
	}
	return &multiSiteHarness{
		reg:           reg,
		ns:            ns,
		sitesPerShard: 2,
		nodeTimeoutMs: 15000,
		siteNames:     names,
	}
}

func (h *multiSiteHarness) topology() string   { return "multi" }
func (h *multiSiteHarness) instanceNS() string { return h.ns }

func (h *multiSiteHarness) applyRedundantCR(crName string) error {
	cr := msClusterCR(h.reg, h.ns, crName, h.siteNames, clusterShards, 0, h.sitesPerShard, h.nodeTimeoutMs)
	_, err := kctlIn(h.reg.hubContext, cr, "apply", "-f", "-")
	return err
}

func (h *multiSiteHarness) waitRunning(crName string, timeout time.Duration) {
	Eventually(func(g Gomega) {
		out, err := kctl(h.reg.hubContext, "-n", h.ns, "get", "littlered", crName,
			"-o", "jsonpath={.status.phase}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal("Running"))
	}, timeout, 10*time.Second).Should(Succeed())
}

func (h *multiSiteHarness) deleteCR(crName string) {
	_, _ = kctl(h.reg.hubContext, "-n", h.ns, "delete", "littlered", crName,
		"--ignore-not-found", "--timeout=3m")
	// The multi-site finalizer sweeps the spokes; wait for each site's pods to drain so the
	// next scenario starts clean.
	for _, s := range h.reg.orderedSites {
		_, _ = kctl(s.context, "-n", h.ns, "wait", "--for=delete", "pod",
			"-l", "app.kubernetes.io/instance="+crName, "--timeout=3m")
	}
}

func (h *multiSiteHarness) expectedMasters() int  { return clusterShards }
func (h *multiSiteHarness) expectedReplicas() int { return clusterShards * (h.sitesPerShard - 1) }
func (h *multiSiteHarness) expectedTotal() int    { return clusterShards * h.sitesPerShard }

// primarySiteOf returns the ordered-site index that hosts shard K's initial master (K mod numSites).
func (h *multiSiteHarness) primarySiteOf(shard int) msSite {
	return h.reg.orderedSites[shard%len(h.reg.orderedSites)]
}

func (h *multiSiteHarness) queryPod(crName string) podHandle {
	s := h.primarySiteOf(0)
	return podHandle{ctx: s.context, name: clusterMasterPod(crName, 0), site: s.name}
}

func (h *multiSiteHarness) execRedis(p podHandle, args ...string) (string, error) {
	full := append([]string{"exec", p.name, "-n", h.ns, "-c", "redis", "--", "redis-cli"}, args...)
	return kubectlCtx(p.ctx, full...)
}

func (h *multiSiteHarness) deletePod(p podHandle, graceful bool) error {
	args := []string{"-n", h.ns, "delete", "pod", p.name}
	if !graceful {
		args = append(args, "--grace-period=0", "--force")
	}
	_, err := kctl(p.ctx, args...)
	return err
}

// ipToHandle builds a globally-unique pod-IP → handle map across every reachable site (pod IPs
// are routed cross-site, so they never collide — the basis for attributing a CLUSTER NODES row,
// which carries only an IP, back to the site that hosts it).
func (h *multiSiteHarness) ipToHandle() map[string]podHandle {
	out := map[string]podHandle{}
	for _, s := range h.reg.orderedSites {
		res, err := kctl(s.context, "-n", h.ns, "get", "pods", "-l", "app.kubernetes.io/component=cluster",
			"-o", "jsonpath={range .items[*]}{.status.podIP}={.metadata.name}{\"\\n\"}{end}")
		if err != nil {
			continue
		}
		for _, l := range strings.Split(strings.TrimSpace(res), "\n") {
			kv := strings.SplitN(l, "=", 2)
			if len(kv) == 2 && kv[0] != "" {
				out[kv[0]] = podHandle{ctx: s.context, name: kv[1], site: s.name}
			}
		}
	}
	return out
}

func (h *multiSiteHarness) resolveMasterForSlot(crName string, slot int) (podHandle, error) {
	nodes, err := h.execRedis(h.queryPod(crName), "CLUSTER", "NODES")
	if err != nil {
		return podHandle{}, err
	}
	ip := masterIPForSlot(nodes, slot)
	if ip == "" {
		return podHandle{}, fmt.Errorf("slot %d has no serving master", slot)
	}
	if hnd, ok := h.ipToHandle()[ip]; ok {
		return hnd, nil
	}
	return podHandle{}, fmt.Errorf("no pod found for master IP %s (slot %d)", ip, slot)
}

func (h *multiSiteHarness) verifyHealthy(crName string) {
	s := h.primarySiteOf(0)
	msVerifyCrossSiteTopology(h.reg, h.ns, crName, s.name, clusterMasterPod(crName, 0), true)
}

func (h *multiSiteHarness) deployChaos(crName, name string, duration time.Duration) (chaosDeployment, error) {
	// Deploy into the first site (a workload cluster that routes to every stretched pod IP).
	ctx := h.reg.orderedSites[0].context
	addrs := msPodIPAddrs(h.reg, h.ns)
	pod, err := deployChaosClientCtx(ctx, h.ns, name, addrs, "chaos-"+name, true, duration)
	return chaosDeployment{ctx: ctx, ns: h.ns, pod: pod}, err
}

func (h *multiSiteHarness) waitChaos(d chaosDeployment, timeout time.Duration) error {
	return waitForChaosClientCompleteCtx(d.ctx, d.ns, d.pod, timeout)
}

func (h *multiSiteHarness) chaosMetrics(d chaosDeployment) (*chaos.MetricsSnapshot, error) {
	return getChaosClientMetricsCtx(d.ctx, d.ns, d.pod)
}

func (h *multiSiteHarness) deleteChaos(d chaosDeployment) {
	deleteChaosClientCtx(d.ctx, d.ns, d.pod)
}
