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
	"path/filepath"
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"

	"github.com/littlered-operator/littlered-operator/test/utils"
)

// Per-pod Redis ground truth for the debug artifacts.
//
// Why this file exists: in the chaos-cluster-stable investigation (2026-08-23) the
// strongest conclusion — that one of three cluster masters never reached
// cluster_state:ok while the operator reported the instance healthy — had to be
// inferred from the *absence* of a "Cluster state changed: ok" line in one pod's
// log, corroborated only by a statistical argument about which node a client's
// round-robin must have been pinned to. One `CLUSTER INFO` per pod in the dump
// would have made it a fact.
//
// It is also the artifact that no other collected file can substitute for. The CR
// status cannot: the operator's own ClusterState is an OR over reachable nodes
// ("ok" if ANY node says ok) and its TotalSlots a MAX, so a single node with a
// partial slot view is invisible there by construction. Pod logs cannot: Redis
// logs state *transitions*, so a node that never converged is diagnosed by
// silence. This is the per-node view, one node at a time, on the record.
//
// Everything here is read-only and best-effort. It runs on the failure path, so no
// probe may abort collection: an unreachable pod, a NOAUTH reply on an
// auth-enabled instance, or a missing container all get recorded verbatim as the
// probe's output and the sweep continues.

// groundTruthProbe is one read-only command executed inside a pod.
type groundTruthProbe struct {
	// label heads the section in the output file.
	label string
	// container is the pod container to exec into.
	container string
	// args is the command line, starting with the binary.
	args []string
}

// redisDataProbes are run in every pod that has a "redis" container, in any mode.
//
// INFO replication is the mode-agnostic per-pod view: role, master link state,
// replication offset and replid lineage — the fields every LR-nnn replication
// investigation has needed (holder counts in LR-015, replid/replid2 divergence in
// LR-024, link:up in LR-038). CLUSTER INFO is harmless on a non-cluster node,
// where every CLUSTER subcommand answers "ERR This instance has cluster support
// disabled" — recorded verbatim, which positively identifies the mode rather than
// leaving a gap. CLUSTER NODES is chained off that reply, see clusterEnabled.
var redisDataProbes = []groundTruthProbe{
	{label: "INFO replication", container: "redis", args: []string{"redis-cli", "INFO", "replication"}},
	{label: "CLUSTER INFO", container: "redis", args: []string{"redis-cli", "CLUSTER", "INFO"}},
}

// sentinelProbes are run in every pod that has a "sentinel" container.
//
// SENTINEL masters is used rather than `SENTINEL master <name>` on purpose: it
// needs no master name, so this stays mode-agnostic and cannot repeat LR-041's
// mistake of querying by a name the call site failed to supply. It reports the
// monitored master's address and flags per sentinel, which is the whole subject of
// LR-005/LR-008 (a sentinel pinned to a wrong or dead master) and of the bare
// sentinel that defines Rule L's precondition.
var sentinelProbes = []groundTruthProbe{
	{label: "SENTINEL masters", container: "sentinel", args: []string{"redis-cli", "-p", "26379", "SENTINEL", "masters"}},
	{label: "INFO sentinel", container: "sentinel", args: []string{"redis-cli", "-p", "26379", "INFO", "sentinel"}},
}

// collectRedisGroundTruth writes one file holding every managed pod's own view of
// the topology, pod by pod, so per-node disagreement is directly readable.
func collectRedisGroundTruth(debugDir, namespace string) {
	_, _ = fmt.Fprintf(GinkgoWriter, "Collecting per-pod Redis ground truth...\n")

	pods, err := podsWithContainers(namespace)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to list pods for ground truth: %v\n", err)
		return
	}

	var b strings.Builder
	b.WriteString("Per-pod Redis ground truth\n")
	b.WriteString("==========================\n\n")
	b.WriteString("Each section is ONE pod's own opinion, gathered directly from that pod.\n")
	b.WriteString("Compare the sections against each other before trusting any aggregate:\n")
	b.WriteString("the operator's status.cluster.state is an OR over nodes and its slot count\n")
	b.WriteString("a MAX, so one node with a partial view cannot show up there.\n\n")
	b.WriteString("lrctl verify inherits that blindness: its \"Cluster State\" line is the same\n")
	b.WriteString("OR. On 2026-08-23 it would have reported ok while one of three masters sat\n")
	b.WriteString("at cluster_state:fail for 122s, refusing the traffic a client sent it. So\n")
	b.WriteString("when this file and lrctl-verify-*.txt disagree about whether the instance is\n")
	b.WriteString("whole, THIS file is the one to trust: lrctl adds the computed verdict\n")
	b.WriteString("(authority master, ghosts, partitions, colocation), not per-node dissent.\n")

	probed := 0
	for _, pod := range pods {
		probes := probesFor(pod)
		if len(probes) == 0 {
			continue
		}
		probed++
		fmt.Fprintf(&b, "\n\n########## %s ##########\n", pod.name)
		for _, p := range probes {
			out := execInPod(namespace, pod.name, p)
			fmt.Fprintf(&b, "\n----- %s (%s) -----\n%s\n", p.label, p.container, out)

			// CLUSTER NODES only makes sense where cluster support is on. Chained
			// off the reply we already have rather than off the pod's labels or the
			// CR's mode, so it stays correct for a pod whose mode the collector was
			// never told.
			if p.label == "CLUSTER INFO" && clusterEnabled(out) {
				nodes := execInPod(namespace, pod.name, groundTruthProbe{
					label:     "CLUSTER NODES",
					container: "redis",
					args:      []string{"redis-cli", "CLUSTER", "NODES"},
				})
				fmt.Fprintf(&b, "\n----- CLUSTER NODES (redis) -----\n%s\n", nodes)
			}
		}
	}

	if probed == 0 {
		b.WriteString("\n(no pod with a redis or sentinel container found in the namespace)\n")
	}

	outFile := filepath.Join(debugDir, "redis-ground-truth.txt")
	if err := os.WriteFile(outFile, []byte(b.String()), 0644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to write Redis ground truth: %v\n", err)
		return
	}
	_, _ = fmt.Fprintf(GinkgoWriter, "Wrote %d pods' ground truth to %s\n", probed, outFile)
}

// clusterEnabled reports whether a CLUSTER INFO reply came from a cluster-enabled
// node.
//
// It keys on cluster_state, NOT on cluster_enabled: Redis 8.4.2's CLUSTER INFO
// reply does not carry a cluster_enabled field at all (verified against the
// shipped default image — the reply opens with cluster_state), so a probe gated on
// that field would be permanently and silently inert. A non-cluster node cannot
// produce this line either, because every CLUSTER subcommand there answers
// "ERR This instance has cluster support disabled".
func clusterEnabled(clusterInfo string) bool {
	return strings.Contains(clusterInfo, "cluster_state:")
}

// podContainers pairs a pod name with its container names.
type podContainers struct {
	name       string
	containers []string
}

// podsWithContainers lists the namespace's pods and their containers in one call,
// so the sweep costs one kubectl invocation plus the probes themselves.
func podsWithContainers(namespace string) ([]podContainers, error) {
	cmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-o",
		`jsonpath={range .items[*]}{.metadata.name}{"\t"}{range .spec.containers[*]}{.name}{" "}{end}{"\n"}{end}`)
	out, err := utils.Run(cmd)
	if err != nil {
		return nil, err
	}

	var pods []podContainers
	for line := range strings.SplitSeq(out, "\n") {
		name, containers, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if !ok || name == "" {
			continue
		}
		pods = append(pods, podContainers{name: name, containers: strings.Fields(containers)})
	}
	return pods, nil
}

// probesFor selects the probes a pod can answer, from its container set alone.
// Container membership is the right discriminator here rather than a label or the
// CR's mode: it is exactly the question "can this pod answer this command", and it
// keeps the sweep correct for the chaos client and any unmanaged pod sharing the
// namespace (neither has a redis or sentinel container, so both are skipped).
func probesFor(pod podContainers) []groundTruthProbe {
	var probes []groundTruthProbe
	if slices.Contains(pod.containers, "redis") {
		probes = append(probes, redisDataProbes...)
	}
	if slices.Contains(pod.containers, "sentinel") {
		probes = append(probes, sentinelProbes...)
	}
	return probes
}

// execInPod runs one probe and returns its output, or a rendering of why it could
// not run. It never returns an error: on the failure path a probe that cannot
// answer is itself a finding worth recording (a pod that refuses redis-cli is
// usually a pod parked in a startup wait-loop).
func execInPod(namespace, podName string, p groundTruthProbe) string {
	args := append([]string{"exec", podName, "-n", namespace, "-c", p.container, "--"}, p.args...)
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	text := strings.TrimRight(string(out), "\n")
	if err != nil {
		if text == "" {
			return fmt.Sprintf("(probe failed: %v)", err)
		}
		return fmt.Sprintf("%s\n(probe exited with %v)", text, err)
	}
	if text == "" {
		return "(empty reply)"
	}
	return text
}

// collectLrctlVerify captures `lrctl verify`, the project's designated ground-truth
// tool (CLAUDE.md §7 rule 8): it gathers the operator-side view, computes the
// authority master and flags ghosts, partitions and cross-shard colocation
// breakage — conclusions no raw CLUSTER NODES dump states outright.
//
// It is opportunistic by design. lrctl is a separate binary that the suite does
// not build for every spec, and the collector must not build it: this runs on the
// failure path, where a two-second artifact sweep turning into a Go build that can
// itself fail is a bad trade. So: use bin/lrctl if a previous `make lrctl` or
// `make build` left one there, and otherwise record the one command that would
// have produced it.
func collectLrctlVerify(debugDir, namespace, crName string) {
	if crName == "" {
		return
	}

	projectDir, err := utils.GetProjectDir()
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping lrctl verify (project dir unknown): %v\n", err)
		return
	}
	bin := filepath.Join(projectDir, "bin", "lrctl")

	outFile := filepath.Join(debugDir, fmt.Sprintf("lrctl-verify-%s.txt", crName))
	if _, statErr := os.Stat(bin); statErr != nil {
		note := fmt.Sprintf(`lrctl verify was not captured: no binary at %s.

Build it and re-run to get this artifact on the next failure:

    make lrctl

The per-pod view in redis-ground-truth.txt is the raw equivalent; lrctl adds the
computed verdict (authority master, ghosts, partitions, shard colocation).
`, bin)
		_ = os.WriteFile(outFile, []byte(note), 0644)
		return
	}

	_, _ = fmt.Fprintf(GinkgoWriter, "Collecting lrctl verify for %s...\n", crName)
	out, _ := exec.Command(bin, "verify", crName, "-n", namespace).CombinedOutput()
	if err := os.WriteFile(outFile, out, 0644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to write lrctl verify output: %v\n", err)
	}
}
