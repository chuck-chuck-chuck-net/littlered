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

package redis

import (
	"sort"
	"strings"
)

// SentinelCount records a Sentinel reporting more peers or replicas than deployed.
type SentinelCount struct {
	PodName  string
	Reported int
	Expected int
}

// CrossInstanceEvidence is what a diagnostic can honestly say about contact with a
// Sentinel deployment that is not ours.
//
// It is EVIDENCE, never a clean bill of health. An empty result means "nothing visible
// from this vantage", not "isolated" — we can only see what our own Sentinels report,
// and a deployment we have not merged with yet is invisible by construction. This is
// why the check lives in `lrctl verify`, run by someone already suspicious, rather than
// in the controller, where silence would be read as an all-clear it cannot give
// (ADR-015 Alternative E).
type CrossInstanceEvidence struct {
	// ForeignMasterIPs are monitored master addresses that are not this instance's
	// pods AND are not flagged down — i.e. alive, and someone else's.
	ForeignMasterIPs []string
	// ForeignReplicaIPs are Sentinel-known replica addresses that are not this
	// instance's pods and are not flagged down.
	ForeignReplicaIPs []string
	// PeerSurplus / ReplicaSurplus are Sentinels reporting more peers or replicas
	// than this instance deployed.
	PeerSurplus    []SentinelCount
	ReplicaSurplus []SentinelCount
}

// Any reports whether anything at all was observed.
func (e CrossInstanceEvidence) Any() bool {
	return len(e.ForeignMasterIPs) > 0 || len(e.ForeignReplicaIPs) > 0 ||
		len(e.PeerSurplus) > 0 || len(e.ReplicaSurplus) > 0
}

// flaggedDown reports whether a Sentinel flags string marks the instance as down.
// This is the discriminator that keeps ordinary post-failover debris — a dead
// ex-master, a dead ex-replica — from being reported as a foreign deployment. An
// address that is not ours AND is reported healthy means something else is alive
// there, which is the captured state.
func flaggedDown(flags string) bool {
	return strings.Contains(flags, "s_down") || strings.Contains(flags, "o_down")
}

// DetectCrossInstance inspects the gathered Sentinel view for signs that another
// Sentinel deployment shares this instance's master name.
//
// expectedSentinels and expectedReplicas are what this instance deployed; a Sentinel
// reporting MORE peers or replicas than that has learned about somebody else. A
// deficit is deliberately ignored — that is a partition or a restart, a different
// problem, and must not masquerade as a collision.
//
// Only reachable, monitoring Sentinels contribute: an unreachable one has no view, and
// counting it would manufacture evidence out of a gather failure.
func (s *SentinelClusterState) DetectCrossInstance(expectedSentinels, expectedReplicas int) CrossInstanceEvidence {
	var ev CrossInstanceEvidence
	masters := map[string]bool{}
	replicas := map[string]bool{}

	// num-other-sentinels excludes the reporting Sentinel itself.
	expectedPeers := max(expectedSentinels-1, 0)

	for _, sn := range s.SentinelNodes {
		if sn == nil || !sn.Reachable || !sn.Monitoring {
			continue
		}

		if sn.MasterIP != "" && s.IsGhost(sn.MasterIP) && !flaggedDown(sn.MasterFlags) {
			masters[sn.MasterIP] = true
		}
		for _, r := range sn.Replicas {
			if r.IP != "" && s.IsGhost(r.IP) && !flaggedDown(r.Flags) {
				replicas[r.IP] = true
			}
		}
		if sn.NumOtherSentinels > expectedPeers {
			ev.PeerSurplus = append(ev.PeerSurplus, SentinelCount{
				PodName: sn.PodName, Reported: sn.NumOtherSentinels, Expected: expectedPeers,
			})
		}
		if sn.NumSlaves > expectedReplicas {
			ev.ReplicaSurplus = append(ev.ReplicaSurplus, SentinelCount{
				PodName: sn.PodName, Reported: sn.NumSlaves, Expected: expectedReplicas,
			})
		}
	}

	ev.ForeignMasterIPs = sortedKeys(masters)
	ev.ForeignReplicaIPs = sortedKeys(replicas)
	// Map iteration order is random; sort so output is stable across runs.
	sort.Slice(ev.PeerSurplus, func(i, j int) bool { return ev.PeerSurplus[i].PodName < ev.PeerSurplus[j].PodName })
	sort.Slice(ev.ReplicaSurplus, func(i, j int) bool {
		return ev.ReplicaSurplus[i].PodName < ev.ReplicaSurplus[j].PodName
	})
	return ev
}

func sortedKeys(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
