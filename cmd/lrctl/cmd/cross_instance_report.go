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

	"github.com/littlered-operator/littlered-operator/internal/cli/types"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// reportCrossInstance prints the Sentinel master name in use and any evidence that
// another Sentinel deployment shares it.
//
// Deliberately worded as an observation, not a verdict. A clean result says "nothing
// visible from this vantage" and never "isolated": we see only what this instance's
// own Sentinels report, and a deployment we have not merged with yet is invisible by
// construction. That is why this lives in `verify`, run by someone already suspicious,
// rather than in the controller, where silence would be read as an all-clear it cannot
// give (ADR-015 Alternative E).
func reportCrossInstance(state *redisclient.ReplicationState, cCtx *types.ClusterContext) {
	masterName := masterNameOf(cCtx)
	expectedSentinels := len(cCtx.SentinelPods)
	expectedReplicas := max(len(cCtx.RedisPods)-1, 0)

	fmt.Printf("\nSentinel Identity:\n")
	fmt.Printf("  Master name: %s\n", masterName)
	if masterName == "mymaster" {
		fmt.Printf("  [WARN] This is the historic shared default. Every LittleRed instance using it\n")
		fmt.Printf("         on this pod network shares one Sentinel identity and can absorb this\n")
		fmt.Printf("         instance's topology. Set spec.sentinel.masterName (e.g. %s.%s).\n",
			cCtx.Namespace, cCtx.Name)
	}

	ev := state.DetectCrossInstance(expectedSentinels, expectedReplicas)
	if !ev.Any() {
		fmt.Printf("  [OK] No foreign Sentinel contact observed (%d sentinels, %d replicas expected).\n",
			expectedSentinels, expectedReplicas)
		return
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
}
