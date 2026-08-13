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

// debug_artifacts_multisite.go extends the failure-time debug collector to multi-site
// (stretched-cluster) tests. The single-site collector (debug_artifacts.go) assumes ONE
// kube-context (the pinned hub) and ONE namespace (testNamespace) — but a multi-site instance
// lives in its own namespace with pods spread across the SITE clusters, none of which the
// single-context collector can reach. So a failed multi-site run collected only the (empty) hub
// default namespace and none of the cross-site ground truth that actually explains the failure
// (see project debugging of the LR-030 Scenario B cut, 2026-08-12).
//
// A multi-site spec publishes its target via setMSDebugTarget(reg, ns, crName); the suite-level
// AfterEach's CollectDebugArtifacts then routes to collectMultiSiteDebugArtifacts, which gathers
// the hub-side CR/events plus, FOR EACH SITE, the per-site pods/workloads/CiliumNetworkPolicy,
// per-pod redis logs, and the redis ground truth (CLUSTER NODES + CLUSTER INFO) from that site's
// own vantage — the union view the mode's reconcile actually decides on.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
)

// msDebugTarget is the multi-site instance a failing spec wants collected on failure: the harness
// registry (hub + site kube-contexts), the instance namespace, and the CR name.
type msDebugTarget struct {
	reg    *msRegistry
	ns     string
	crName string
}

// activeMSDebug is set by a multi-site spec (setMSDebugTarget) while it runs, and read by the
// suite-level CollectDebugArtifacts on failure. It is reset before every spec (resetMSDebugTarget,
// called from the global BeforeEach) so a single-site spec never inherits a stale multi-site
// target. Ginkgo runs specs serially, so a plain package var is safe here.
var activeMSDebug *msDebugTarget

// setMSDebugTarget marks the current spec as multi-site for the failure-time collector. Call it as
// soon as the registry + instance identity are known (before the assertions that may fail), so even
// a setup/bring-up failure is collected across sites. It must NOT be cleared in the spec's own
// AfterEach: the suite-level collector runs in a LATER (outer) AfterEach, so the target has to
// survive until then — resetMSDebugTarget in the global BeforeEach clears it before the next spec.
func setMSDebugTarget(reg *msRegistry, ns, crName string) {
	activeMSDebug = &msDebugTarget{reg: reg, ns: ns, crName: crName}
}

// resetMSDebugTarget clears any published multi-site target. Called from the global BeforeEach.
func resetMSDebugTarget() { activeMSDebug = nil }

// collectMultiSiteDebugArtifacts gathers hub-side + per-site state for a failed multi-site spec.
// Everything is best-effort: a failure to reach one site (e.g. a killed site in Scenario A, or a
// data-plane cut) must not abort collection of the others — that partial view is often the whole
// point. Errors are noted in the artifact file and on GinkgoWriter, never fatal.
func collectMultiSiteDebugArtifacts(debugDir string, t *msDebugTarget) {
	reg, ns, crName := t.reg, t.ns, t.crName
	_, _ = fmt.Fprintf(GinkgoWriter,
		"Collecting MULTI-SITE debug artifacts for %s/%s across %d site(s)...\n", ns, crName, len(reg.orderedSites))

	// --- Hub side: the CR (spec+status), its events, and the site Secrets that wire the sites. ---
	dumpKubectl(debugDir, "hub-cr.yaml", reg.hubContext, "-n", ns, "get", "littlered", crName, "-o", "yaml")
	dumpKubectl(debugDir, "hub-events.txt", reg.hubContext, "-n", ns, "get", "events", "--sort-by", ".lastTimestamp")
	dumpKubectl(debugDir, "hub-site-secrets.txt", reg.hubContext, "-n", reg.operatorNS, "get", "secrets", "-o", "name")

	// Chaos-client pods, if any are alive at failure time (they are short-lived and deleted
	// mid-spec, so this captures them only for a failure DURING a write/verify phase — exactly the
	// data-integrity failures where the per-op log matters). Discovered by label, no bookkeeping.
	collectMultiSiteChaosLogs(debugDir, ns, reg)

	// --- Per site: workloads, the cut policy (if any), per-pod logs, and redis ground truth. ---
	for _, s := range reg.orderedSites {
		p := "site-" + s.name + "-"
		dumpKubectl(debugDir, p+"pods.txt", s.context, "-n", ns, "get", "pods", "-o", "wide",
			"-L", "redis.chuck-chuck-chuck.net/shard", "-L", "redis.chuck-chuck-chuck.net/role")
		dumpKubectl(debugDir, p+"statefulsets.yaml", s.context, "-n", ns, "get", "statefulsets", "-o", "yaml")
		dumpKubectl(debugDir, p+"events.txt", s.context, "-n", ns, "get", "events", "--sort-by", ".lastTimestamp")
		// The CiliumNetworkPolicy is the data-plane cut in the network scenarios — its presence and
		// exact scope is usually the crux (a deny-only policy silently over-blocks, LR-030 debug).
		dumpKubectl(debugDir, p+"ciliumnetworkpolicies.yaml", s.context, "-n", ns, "get", "ciliumnetworkpolicy", "-o", "yaml")

		collectSitePodArtifacts(debugDir, p, s.context, ns)
	}
	_, _ = fmt.Fprintf(GinkgoWriter, "Multi-site debug artifacts collected.\n")
}

// collectSitePodArtifacts dumps, for every cluster pod on one site, its redis log (+ previous, if
// it restarted) and its redis ground truth (CLUSTER NODES + CLUSTER INFO from that pod's vantage).
func collectSitePodArtifacts(debugDir, prefix, siteCtx, ns string) {
	out, err := kctl(siteCtx, "-n", ns, "get", "pods", "-l", "app.kubernetes.io/component=cluster",
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "  [%s] could not list cluster pods: %v\n", strings.TrimRight(prefix, "-"), err)
		return
	}
	for _, pod := range strings.Fields(out) {
		dumpKubectl(debugDir, prefix+"pod-"+pod+"-redis.log", siteCtx, "-n", ns, "logs", pod,
			"-c", "redis", "--tail", "1000", "--timestamps")
		// Previous logs only exist if the container restarted; write only when non-empty.
		if prev, perr := kctl(siteCtx, "-n", ns, "logs", pod, "--previous", "--tail", "1000", "--timestamps"); perr == nil && strings.TrimSpace(prev) != "" {
			writeArtifact(debugDir, prefix+"pod-"+pod+"-redis-previous.log", prev)
		}
		// Redis ground truth from this pod's OWN gossip vantage — the per-site view the multi-site
		// reconcile gathers as a union and decides on (site-loss takeover, ghost FORGET, rebalance).
		dumpKubectl(debugDir, prefix+"pod-"+pod+"-cluster-nodes.txt", siteCtx, "exec", pod, "-n", ns,
			"-c", "redis", "--", "redis-cli", "CLUSTER", "NODES")
		dumpKubectl(debugDir, prefix+"pod-"+pod+"-cluster-info.txt", siteCtx, "exec", pod, "-n", ns,
			"-c", "redis", "--", "redis-cli", "CLUSTER", "INFO")
	}
}

// collectMultiSiteChaosLogs dumps the logs of any chaos-client pod (label app=chaos-client) found
// in the instance namespace. An ambient chaos deploy lands on the hub; a *Ctx deploy could land on
// a site — so scan both. The chaos metrics (writes / corruptions) are embedded in the pod log as a
// METRICS_JSON line, so the log is the complete artifact. Best-effort: no pod ⇒ nothing written.
func collectMultiSiteChaosLogs(debugDir, ns string, reg *msRegistry) {
	type ctxRef struct{ label, kctx string }
	refs := []ctxRef{{"hub", reg.hubContext}}
	for _, s := range reg.orderedSites {
		refs = append(refs, ctxRef{s.name, s.context})
	}
	for _, r := range refs {
		out, err := kctl(r.kctx, "-n", ns, "get", "pods", "-l", "app=chaos-client",
			"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
		if err != nil || strings.TrimSpace(out) == "" {
			continue
		}
		for _, pod := range strings.Fields(out) {
			dumpKubectl(debugDir, "chaos-"+r.label+"-"+pod+".log", r.kctx, "-n", ns, "logs", pod, "--timestamps")
		}
	}
}

// dumpKubectl runs a context-scoped kubectl and writes its output (plus any error) to filename.
// Best-effort: a non-zero exit still produces an artifact carrying whatever output + the error, so
// an unreachable site or a missing resource is recorded rather than silently dropped.
func dumpKubectl(debugDir, filename, siteCtx string, args ...string) {
	out, err := kctl(siteCtx, args...)
	if err != nil {
		out = strings.TrimRight(out, "\n") + fmt.Sprintf("\n[collection error: %v]\n", err)
	}
	writeArtifact(debugDir, filename, out)
}

// writeArtifact writes content to debugDir/filename, reporting (never failing) on a write error.
func writeArtifact(debugDir, filename, content string) {
	f := filepath.Join(debugDir, filename)
	if err := os.WriteFile(f, []byte(content), 0644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to write %s: %v\n", f, err)
	}
}
