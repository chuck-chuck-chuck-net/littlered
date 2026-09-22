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

package cmd

import (
	corev1 "k8s.io/api/core/v1"

	"fmt"
	"strings"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	"github.com/chuck-chuck-chuck-net/littlered/internal/cli/types"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// reportCrossInstance prints the Sentinel master name in use, EVERY master name each
// Sentinel monitors, and any evidence that another Sentinel deployment shares the
// name. It reports whether what it found fails verification.
//
// Two things are deliberately kept apart here. The master-name SCOPE is a local,
// exact fact — these are the names our own Sentinels carry — and a name other than
// the CR's is a defect whatever else is true, so it fails. The cross-instance
// EVIDENCE is an observation, not a verdict: a clean result says "nothing visible
// from this vantage" and never "isolated", because we see only what our own Sentinels
// report and a deployment we have not merged with yet is invisible by construction.
// That is why this lives in `verify`, run by someone already suspicious, rather than
// in the controller, where silence would be read as an all-clear it cannot give
// (ADR-015 Alternative E).
//
// A capture is reported once, not twice: the foreign-name finding and the foreign
// contact evidence are separate observations of one state, so they are printed in one
// block under one heading and share a single pointer to the recovery runbook.
func reportCrossInstance(state *redisclient.ReplicationState, cCtx *types.ClusterContext) bool {
	masterName := masterNameOf(cCtx)
	expectedSentinels := len(cCtx.SentinelPods)
	expectedReplicas := max(len(cCtx.RedisPods)-1, 0)

	fmt.Printf("\nSentinel Identity:\n")
	fmt.Printf("  Master name: %s\n", masterName)
	if masterName == littleredv1alpha1.LegacySentinelMasterName {
		fmt.Printf("  [WARN] This is the historic shared default. Every LittleRed instance using it\n")
		fmt.Printf("         on this pod network shares one Sentinel identity and can absorb this\n")
		fmt.Printf("         instance's topology. Set spec.sentinel.masterName (e.g. %s.%s).\n",
			cCtx.Namespace, cCtx.Name)
	}

	// The scope check needs a name we KNOW is wanted. With --unmanaged there is no CR
	// to read it from and masterNameOf falls back to the legacy constant — a guess —
	// so surveying against it would accuse a correctly-named foreign instance of
	// carrying a stale name. Accusing on a guess is the class of mistake this project
	// keeps recording; say what is missing instead.
	var scopeFail bool
	if cCtx.SentinelMasterName == "" {
		fmt.Printf("  [WARN] The wanted master name is not known (no CR was read), so the\n")
		fmt.Printf("         monitored-name check is skipped. Run without --unmanaged to check it.\n")
	} else {
		scopeLines, fail := renderMasterNameScope(state.SurveyMonitoredNames(masterName), masterName)
		for _, l := range scopeLines {
			fmt.Println(l)
		}
		scopeFail = fail
	}

	ev := state.DetectCrossInstance(expectedSentinels, expectedReplicas)
	if !ev.Any() {
		// Printed even when the name scope failed, and deliberately: the two are
		// different questions, and "the leftover name is OURS and nothing foreign is
		// in contact" is exactly what separates a botched rename from a capture.
		fmt.Printf("  [OK] No foreign Sentinel contact observed (%d sentinels, %d replicas expected).\n",
			expectedSentinels, expectedReplicas)
		return scopeFail
	}

	fmt.Printf("  [FAIL] Evidence of another Sentinel deployment sharing this master name:\n")
	if len(ev.ForeignMasterIPs) > 0 {
		fmt.Printf("         - monitored master is not one of this instance's pods, and is alive: %s\n",
			strings.Join(ev.ForeignMasterIPs, ", "))
	}
	if len(ev.ForeignReplicaIPs) > 0 {
		fmt.Printf("         - Sentinel knows live replicas that are not this instance's pods: %s\n",
			strings.Join(ev.ForeignReplicaIPs, ", "))
	}
	for _, c := range ev.PeerSurplus {
		fmt.Printf("         - %s reports %d other sentinels; %d were deployed\n",
			c.PodName, c.Reported, c.Expected)
	}
	for _, c := range ev.ReplicaSurplus {
		fmt.Printf("         - %s reports %d replicas; %d were deployed\n",
			c.PodName, c.Reported, c.Expected)
	}
	// The deployed count is the same denominator the operator uses for its own
	// wholeness judgements (LR-013, LR-056: key on what we DEPLOYED, never on what
	// answered), and sentinel mode's is fixed. It is passed as 0 under --unmanaged,
	// where there is no CR and a foreign deployment may legitimately run any number
	// of pods — accusing on a guess is the mistake this file already avoids once.
	expectedRedis, expectedSentinel := 0, 0
	if cCtx.SentinelMasterName != "" {
		expectedRedis = int(littleredv1alpha1.SentinelRedisReplicas)
		expectedSentinel = int(littleredv1alpha1.SentinelProcessReplicas)
	}
	for _, l := range crossInstanceChurnCaveat(
		churnGroup{kind: containerNameRedis, pods: cCtx.RedisPods,
			container: cCtx.RedisContainer, expected: expectedRedis},
		churnGroup{kind: containerNameSentinel, pods: cCtx.SentinelPods,
			container: cCtx.SentinelContainer, expected: expectedSentinel},
	) {
		fmt.Println(l)
	}
	fmt.Printf("         This instance's data may already have been overwritten. See the\n")
	fmt.Printf("         \"Recovering a sentinel instance captured by another Sentinel deployment\"\n")
	fmt.Printf("         runbook in docs/USAGE.md.\n")
	return scopeFail
}

// churnGroup is one group of pods this instance deployed — the Redis pods or the
// Sentinel pods — with the count we DEPLOYED rather than the count that answered
// (LR-013, LR-056). Both groups matter and for different evidence: a departed Redis
// pod inflates the replica surplus and leaves an unattributable address behind, while
// a departed SENTINEL pod inflates `num-other-sentinels`, because a stale
// known-sentinel entry never ages out on its own (LR-039). Measured on t3e
// 2026-09-22: the Sentinel side was the larger half of the false-evidence window.
type churnGroup struct {
	kind      string
	pods      []corev1.Pod
	container string
	expected  int
}

// containerNameRedis is the default name of the Redis container, used when discovery
// could not name one (--unmanaged). It mirrors discovery's own default rather than
// importing it: that constant is unexported and lives in another package.
const containerNameRedis = "redis"

// containerNameSentinel is the same for the Sentinel container.
const containerNameSentinel = "sentinel"

// crossInstanceChurnCaveat qualifies the cross-instance evidence when this instance's
// OWN pods are in churn, naming the pods that make the addresses above unattributable.
//
// It qualifies and never suppresses, and that is the whole of the design. The evidence
// is the floorless diagnostic LR-039 built to fire on a PARTIAL capture — before a
// takeover completes — and LR-056 deliberately left it without the quorum floor it
// gave the verdict, on the ground that only one of the two deletes pods. LR-054 goes
// further and makes it load-bearing: for a victim still holding its own data the
// operator can no longer arm a verdict at all, and this report is the remaining signal.
// So churn changes what the reader is told about the evidence, never whether they are
// told.
//
// WHY THE ADDRESS SETS CANNOT ANSWER THIS. A pod of ours that has just been REPLACED
// holds an address that no set can contain: OwnedIPs covers the pod still in the pod
// list, terminating included (LR-053), and LR-050's gate covers the pod whose object
// is already gone — and neither is reachable from a gathered address, because the
// object that would attribute it no longer exists. Meanwhile Sentinel keeps listing
// that address, unflagged, for a whole down-after-milliseconds, which is byte-identical
// to a captor's live replica. Measured on t3e 2026-09-22: `verify` named our own
// just-replaced redis-2 as foreign five seconds before the operator logged the same
// address as an ordinary ghost.
//
// The signal is kubelet readiness of the redis CONTAINER — LR-023's blackhole-proof
// evidence, not the operator's dial (LR-017), and the container rather than the pod
// condition (the distinction LR-047's mutation table pins). It is deliberately ONE
// clause rather than a re-implementation of the operator's statefulSetRolloutSettled:
// that predicate answers two questions at once and is pending the R5 split LR-054
// names, so a second copy of it here would be LR-045's duplicated predicate in a new
// place. What this one clause buys is stated in its limits below.
//
// Limits, stated rather than left to be discovered. It does not fire for a rollout
// whose replacement has already gone Ready while the departed address is still listed,
// so a clean caveat is not a guarantee of attribution. And it fires for as long as any
// pod is unready, including forever — which for a caveat is the safe direction, and is
// precisely NOT LR-054's hole, because nothing here is withheld.
func crossInstanceChurnCaveat(groups ...churnGroup) []string {
	var reasons []string
	for _, g := range groups {
		container := g.container
		if container == "" {
			container = g.kind
		}

		var churning []string
		for _, p := range g.pods {
			if podRedisInChurn(p, container) {
				churning = append(churning, p.Name)
			}
		}

		// A pod that is GONE is the other half, and on the live measurement it was
		// the larger one: with the departed pod absent from the list every pod still
		// listed reads Ready, and the list is also where the expected counts come
		// from, so the evidence arrives as a count surplus rather than as an address.
		// The readiness clause is structurally blind to it.
		if g.expected > 0 && len(g.pods) < g.expected {
			reasons = append(reasons, fmt.Sprintf("%d of %d %s pods listed",
				len(g.pods), g.expected, g.kind))
		}
		if len(churning) > 0 {
			reasons = append(reasons, "not Ready: "+strings.Join(churning, ", "))
		}
	}
	if len(reasons) == 0 {
		return nil
	}

	return []string{
		fmt.Sprintf("         [!] But this instance's OWN pods are in churn (%s), so an address or",
			strings.Join(reasons, "; ")),
		"             count above may be explained by a pod of ours that has just been replaced",
		"             rather than by a stranger: its address leaves the pod list at once — taking",
		"             the expected-replica count with it — while Sentinel goes on listing it,",
		"             unflagged, for a whole down-after-milliseconds. The operator withholds this",
		"             same attribution while the instance is unsettled (LR-050), so it will report",
		"             nothing here. Let the pods settle and re-run before following the runbook.",
	}
}

// podRedisInChurn reports whether this pod is in a state in which an address that was
// recently its own may still be in Sentinel's view while the pod no longer accounts
// for it. A pod on its way out counts (lrctl's pod list carries terminating pods —
// it applies no deletionTimestamp filter, LR-053), and so does one whose redis
// container has not reported a status yet, which is the same window one moment earlier.
func podRedisInChurn(p corev1.Pod, redisContainer string) bool {
	if p.DeletionTimestamp != nil {
		return true
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Name == redisContainer {
			return !cs.Ready
		}
	}
	return true
}
