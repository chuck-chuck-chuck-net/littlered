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
	"fmt"
	"strings"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/types"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
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
	fmt.Printf("         This instance's data may already have been overwritten. See the\n")
	fmt.Printf("         \"Recovering a sentinel instance captured by another Sentinel deployment\"\n")
	fmt.Printf("         runbook in docs/USAGE.md.\n")
	return scopeFail
}
