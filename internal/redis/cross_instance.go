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
func (s *ReplicationState) DetectCrossInstance(expectedSentinels, expectedReplicas int) CrossInstanceEvidence {
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

// =============================================================================
// Monitored master-name scope
// =============================================================================

// The three classes a monitored master name can fall into, from the vantage of an
// instance that wants exactly one name. They are the rendering half of Rule N's
// per-entry discriminator (design §7.3 / §9 gate G5) and must stay identical to it:
// an address in ValidIPs, or an address Sentinel flags down, is ordinary debris of
// OURS; anything else is somebody else's live master.
const (
	// MasterNameDesired is the name the CR asks for.
	MasterNameDesired = "desired"
	// MasterNameStale is a leftover entry of ours — the address is one of our pods,
	// or it is flagged down (a dead ex-master, LR-024's subject).
	MasterNameStale = "stale"
	// MasterNameForeign is a name pointing at an address that is neither one of our
	// pods nor flagged down: something else is alive there. A different and more
	// serious finding than a stale one — it is the signature of a capture (LR-039).
	MasterNameForeign = "foreign"
)

// ClassifyMonitoredName places one monitored master name in its class, against the
// name the CR asks for and the set of this instance's own pod addresses.
//
// It is the one definition of the distinction, shared by the survey below and by
// `lrctl inspect`, which has pod addresses but no gathered ReplicationState. The
// discriminator is Rule N's G5 / planForsaken clause 3, and must stay identical to
// them: an address of ours, or an address Sentinel flags down, is debris of ours;
// anything else — including an entry with no address at all, which cannot be
// attributed to us — is somebody else's live master.
func ClassifyMonitoredName(name, ip, flags, desired string, ourIPs map[string]bool) string {
	switch {
	case name == desired:
		return MasterNameDesired
	case ourIPs[ip] || flaggedDown(flags):
		return MasterNameStale
	default:
		return MasterNameForeign
	}
}

// MonitoredNameFinding is one (Sentinel, monitored master name) pair as that Sentinel
// reports it, plus the class it falls into.
type MonitoredNameFinding struct {
	SentinelPod string
	Name        string
	IP          string
	Flags       string
	Class       string
}

// MasterNameScope is what every reachable Sentinel monitors, and how it classifies.
//
// It answers the question `lrctl verify` structurally could not ask before: not "is
// this Sentinel monitoring the name we want" (which any Sentinel carrying a leftover
// name alongside the desired one answers yes to) but "does it monitor ONLY that
// name". A half-finished master-name change leaves two `sentinel monitor` lines and
// two independent failover state machines over the same three pods (LR-048), and
// nothing that asks about a single name can see the second one.
type MasterNameScope struct {
	// Findings is every monitored name of every reachable Sentinel, ordered by
	// Sentinel pod then name so an unchanged topology renders identically twice.
	Findings []MonitoredNameFinding
	// Stale and Foreign are the distinct names of each class, sorted.
	Stale   []string
	Foreign []string
	// Unreported names the reachable Sentinels whose master list could not be read.
	// An empty list means "no evidence", never "monitors nothing" (LR-041), so it is
	// reported rather than silently rendered as convergence.
	Unreported []string
}

// Converged reports whether every reachable Sentinel monitors the desired name and
// nothing else. It is deliberately not "healthy": a Sentinel whose list could not be
// read is not evidence either way, and is surfaced separately.
func (s MasterNameScope) Converged() bool {
	return len(s.Stale) == 0 && len(s.Foreign) == 0
}

// SurveyMonitoredNames classifies every master name every reachable Sentinel
// monitors, against the name this instance wants.
//
// Pure, and deliberately NOT gated on sn.Monitoring: mid-rename every Sentinel reads
// `Monitoring: false` for the new name while still carrying the old entry (measured,
// design §9.1 item 2), which is exactly the state this survey exists to see.
func (s *ReplicationState) SurveyMonitoredNames(desired string) MasterNameScope {
	var scope MasterNameScope
	// With no name to compare against there is nothing to say. Classifying every
	// entry as stale would be the "prune everything" failure mode read out loud.
	if s == nil || desired == "" {
		return scope
	}

	stale := map[string]bool{}
	foreign := map[string]bool{}
	for _, sn := range s.SentinelNodes {
		if sn == nil || !sn.Reachable {
			continue
		}
		if len(sn.MonitoredMasters) == 0 {
			scope.Unreported = append(scope.Unreported, sn.PodName)
			continue
		}
		for _, m := range sn.MonitoredMasters {
			f := MonitoredNameFinding{
				SentinelPod: sn.PodName, Name: m.Name, IP: m.IP, Flags: m.Flags,
			}
			f.Class = ClassifyMonitoredName(m.Name, m.IP, m.Flags, desired, s.ValidIPs)
			switch f.Class {
			case MasterNameStale:
				stale[m.Name] = true
			case MasterNameForeign:
				foreign[m.Name] = true
			}
			scope.Findings = append(scope.Findings, f)
		}
	}

	// The gather is a map, so without an explicit order an unchanged topology
	// renders differently every run and a reader cannot tell a change from a
	// re-render (design §9.1 item 5).
	sort.Slice(scope.Findings, func(i, j int) bool {
		if scope.Findings[i].SentinelPod != scope.Findings[j].SentinelPod {
			return scope.Findings[i].SentinelPod < scope.Findings[j].SentinelPod
		}
		return scope.Findings[i].Name < scope.Findings[j].Name
	})
	sort.Strings(scope.Unreported)
	scope.Stale = sortedKeys(stale)
	scope.Foreign = sortedKeys(foreign)
	return scope
}
