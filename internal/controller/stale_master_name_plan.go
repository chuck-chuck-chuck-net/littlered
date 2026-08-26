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

package controller

import (
	"fmt"
	"sort"
	"strings"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// The four reasons of the StaleMasterName condition (design §8). Polarity: True is bad,
// matching Forsaken and LegacyClusterTopology, so Converged is the quiet steady state
// and the other three are True.
const (
	// staleNamesConverged: every reachable Sentinel monitors exactly the desired name.
	staleNamesConverged = "Converged"
	// staleNamesPruning: stale names observed and being removed this pass.
	staleNamesPruning = "Pruning"
	// staleNamesDeferred: stale names observed but a gate refuses. The message names
	// WHICH gate — a refusal that does not say why is indistinguishable from a stall.
	staleNamesDeferred = "Deferred"
	// staleNamesForeign: the stale entry is somebody else's master, or the instance is
	// already known to be captured. Never prune; warn.
	staleNamesForeign = "Foreign"
)

// staleEntry is one Sentinel's worth of work: the master names THAT Sentinel monitors
// which are not the desired one. The desired name is never among Names — re-pointing it
// at a different address is LR-005/LR-008's job and stays there.
type staleEntry struct {
	SentinelIP  string
	SentinelPod string
	Names       []string
}

// StaleMasterNamePlan is Rule N's decision: what to REMOVE, from whom, and what to say.
type StaleMasterNamePlan struct {
	// Prune is per Sentinel, ordered by pod name, names sorted. Empty unless Reason is
	// Pruning.
	Prune []staleEntry
	// Skipped names the Sentinels that carry a stale name but do not yet carry the
	// desired one (G6). They are reported rather than silently dropped: R3 is "no
	// leftover entry, EVER", so "lagging by a pass" must be distinguishable from
	// "permanently stuck". Their convergence is Rule 0's job, not this rule's, and
	// nothing here bounds how long it may take — Rule 0 has no convergence bound
	// either, and inventing a timeout at this layer would be pretending otherwise.
	Skipped []string
	Reason  string
	Message string
}

// planStaleMasterNames decides which stale Sentinel master-name entries may be removed.
//
// The operator reconciles the SCOPE of what its Sentinels monitor: desired state is
// "every Sentinel monitors exactly the desired name, and nothing else". Anything else a
// Sentinel monitors is by definition stale — no previous name is remembered, no phase is
// persisted, and an instance a botched rename (or a hand-issued MONITOR) already broke is
// repaired by the same evidence.
//
// It is pure: no I/O, no clock, no logging. Everything comes from the four parameters,
// and the caller re-confirms G6 with a bounded IsMonitoring immediately before each
// REMOVE — the planner decides, the caller verifies its own precondition (LR-024's
// electMaster lesson: enforce the invariant, do not assume it).
//
// Nearly all of the value is in the refusals. REMOVE is destructive and the only thing
// aiming it is this predicate:
//
//   - G0 forsaken: a capture verdict stands the whole rule down. Passed IN, never
//     re-derived — it is computed once per pass, before Rule N.
//   - G1 the desired name is non-empty (LR-041: a required string's zero value is a
//     plausible input, not an obvious error — and with desired == "" EVERY name reads as
//     stale, so the failure mode is "prune everything").
//   - G2 a living, reachable master of OURS exists to keep monitoring. LR-008's gate,
//     reused: pruning without it manufactures the LR-015 leaderless deadlock.
//   - G3 no monitored master, under ANY name, reports an in-flight failover.
//   - G4 a Sentinel quorum is reachable — do not operate on a minority.
//   - G5 every stale entry's address is one of our pods or is flagged down. Otherwise
//     something else is alive there and the entry is evidence of a capture, not debris.
//   - G6 per Sentinel, the desired name is already present on THAT Sentinel.
func planStaleMasterNames(
	state *redisclient.ReplicationState,
	desired string,
	quorum int,
	forsaken bool,
) StaleMasterNamePlan {
	// G0 — first, and unconditionally. It is checked ahead of everything (including
	// "is there anything to do at all") because it is a stand-down, not a gate on an
	// action: while the instance is captured, ADR-016's quarantine owns it and Rule N
	// must not touch its Sentinels or claim they are converged. It exists because G5
	// is blind to a captor whose master is transiently s_down — at that moment every
	// per-entry test passes, the prune fires, and the capture becomes both undiagnosed
	// and unrecoverable. Only a verdict that does not depend on the name closes that.
	if forsaken {
		return StaleMasterNamePlan{
			Reason: staleNamesForeign,
			Message: "the instance is Forsaken (captured by another Sentinel deployment): " +
				"no master-name pruning while the capture verdict holds. Do not rename to escape " +
				"a capture — let the quarantine complete first, then rename the empty instance.",
		}
	}
	if state == nil {
		return deferStaleNames("G2", "no ground truth was gathered this pass", nil)
	}

	// G1. Deliberately before the survey: with an empty desired name every monitored
	// name would classify as stale, so the failure mode is not "do nothing", it is
	// "prune everything".
	if desired == "" {
		return deferStaleNames("G1", "the desired master name is empty", nil)
	}

	// Survey, in one pass over the gathered Sentinels: what is stale, who already has
	// the desired name, is anything failing over, and does any stale entry point
	// somewhere that is not ours.
	var (
		entries       []staleEntry
		skipped       []string
		allStale      = map[string]bool{}
		failoverUnder []string
		foreign       []string
	)
	for _, sn := range state.SentinelNodes {
		if sn == nil || !sn.Reachable {
			continue
		}
		// An EMPTY MonitoredMasters list means "we could not read it", not "this
		// Sentinel monitors nothing" — the extra round trip degrades to empty rather
		// than to Reachable:false. So it yields no stale names and no skip, which is
		// the correct reading of "no evidence" (LR-041's class of mistake).
		var names []string
		hasDesired := false
		for _, m := range sn.MonitoredMasters {
			if m.FailoverInProgress() {
				failoverUnder = append(failoverUnder, m.Name)
			}
			if m.Name == desired {
				hasDesired = true
				continue
			}
			names = append(names, m.Name)
			allStale[m.Name] = true
			// G5's discriminator, same as planForsaken clause 3: an address that is
			// not ours and is NOT flagged down means something else is alive there.
			// A dead ex-master of ours (ordinary post-failover debris) is flagged
			// down; a captor's master answers with clean flags, which is exactly why
			// no failover ever fires on it.
			//
			// An entry with no address at all lands here too, and deliberately: it
			// cannot be attributed to one of our pods, and refusing to prune is the
			// safe direction for an entry we cannot read.
			if !state.ValidIPs[m.IP] && !flaggedDown(m.Flags) {
				foreign = append(foreign, fmt.Sprintf("%q (at %s)", m.Name, addrOrUnknown(m.IP)))
			}
		}
		if len(names) == 0 {
			continue
		}
		sort.Strings(names)
		if !hasDesired {
			// G6: Rule 0 registers the desired name on this Sentinel next pass; until
			// it has, removing its only entry would leave it bare on purpose.
			skipped = append(skipped, sn.PodName)
			continue
		}
		entries = append(entries, staleEntry{SentinelIP: sn.IP, SentinelPod: sn.PodName, Names: names})
	}

	if len(allStale) == 0 {
		return StaleMasterNamePlan{
			Reason:  staleNamesConverged,
			Message: fmt.Sprintf("every reachable Sentinel monitors exactly %q", desired),
		}
	}
	staleList := sortedKeys(allStale)
	sort.Slice(entries, func(i, j int) bool { return entries[i].SentinelPod < entries[j].SentinelPod })
	sort.Strings(skipped)

	// G5 is evaluated before G2/G3/G4 although §9 numbers it after them, because it is
	// the only verdict that is ALSO a diagnosis. The §7.3 trap — renaming to escape a
	// capture — produces a state in which G2 fails too (no pod of ours is a master, so
	// RealMasterIP is empty), and reporting that as a generic "Deferred: no living
	// master" would hand the owner the least useful of the two true statements. Both
	// outcomes prune nothing, so this changes only which sentence the operator reads.
	if len(foreign) > 0 {
		return StaleMasterNamePlan{
			Reason: staleNamesForeign,
			Message: fmt.Sprintf(
				"stale master name(s) %s point at an address that is not one of our pods and is not "+
					"flagged down — something else is alive there and this instance may be captured. "+
					"Nothing was removed. Do not rename to escape a capture; let the quarantine "+
					"complete first, then rename the empty instance.",
				strings.Join(dedupSorted(foreign), ", ")),
		}
	}

	// G2 — LR-008's gate. All three clauses: a consensus master exists, it is one of
	// our pods, and its own Redis view says it is a reachable master. Checking only
	// RealMasterIP != "" is the easy mis-implementation and it is not the gate.
	master := state.RedisNodes[state.RealMasterIP]
	switch {
	case state.RealMasterIP == "":
		return deferStaleNames("G2", "no living master of ours is known", staleList)
	case !state.ValidIPs[state.RealMasterIP]:
		return deferStaleNames("G2",
			fmt.Sprintf("the consensus master %s is not one of our pods", state.RealMasterIP), staleList)
	case master == nil || !master.Reachable || master.Role != RoleMaster:
		return deferStaleNames("G2",
			fmt.Sprintf("the consensus master %s does not report itself a reachable master",
				state.RealMasterIP), staleList)
	}

	// G3 — the per-entry test is the load-bearing half and covers every monitored name,
	// stale ones included: a failover under the stale name is still a real state machine
	// reconfiguring our pods. state.FailoverActive is OR-ed in for form only; it is
	// permanently false in sentinel mode today (design §15b, a pre-existing dead-key
	// defect tracked separately), so nothing may rest on it.
	if len(failoverUnder) > 0 || state.FailoverActive {
		return deferStaleNames("G3",
			fmt.Sprintf("a failover is in flight under master name(s) %s",
				quoteJoin(dedupSorted(failoverUnder))), staleList)
	}

	// G4.
	if reachable := state.ReachableSentinels(); reachable < quorum || reachable == 0 {
		return deferStaleNames("G4",
			fmt.Sprintf("only %d Sentinel(s) are reachable, quorum is %d", reachable, quorum), staleList)
	}

	if len(entries) == 0 {
		// Every Sentinel carrying a stale name is waiting on Rule 0 to give it the
		// desired one. Nothing to do this pass, and saying "Pruning" with an empty
		// plan would be a lie.
		return deferStaleNames("G6",
			fmt.Sprintf("no Sentinel carries %q yet; Rule 0 registers it next pass", desired), staleList)
	}

	msg := fmt.Sprintf("removing stale master name(s) %s from: %s",
		quoteJoin(staleList), describeStaleEntries(entries))
	if len(skipped) > 0 {
		msg += fmt.Sprintf("; skipped (does not carry %q yet, Rule 0 registers it next pass): %s",
			desired, strings.Join(skipped, ", "))
	}
	return StaleMasterNamePlan{Prune: entries, Skipped: skipped, Reason: staleNamesPruning, Message: msg}
}

// deferStaleNames builds the Deferred verdict. The gate is named in the message because
// a refusal that does not say which gate refused is indistinguishable from a stall.
func deferStaleNames(gate, why string, stale []string) StaleMasterNamePlan {
	msg := fmt.Sprintf("deferred by gate %s: %s", gate, why)
	if len(stale) > 0 {
		msg += fmt.Sprintf("; stale master name(s) still present: %s", quoteJoin(stale))
	}
	return StaleMasterNamePlan{Reason: staleNamesDeferred, Message: msg}
}

func describeStaleEntries(entries []staleEntry) string {
	parts := make([]string, 0, len(entries))
	for _, e := range entries {
		parts = append(parts, fmt.Sprintf("%s=%s", e.SentinelPod, quoteJoin(e.Names)))
	}
	return strings.Join(parts, ", ")
}

func quoteJoin(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, n := range names {
		quoted = append(quoted, fmt.Sprintf("%q", n))
	}
	return "[" + strings.Join(quoted, " ") + "]"
}

// dedupSorted makes a survey's findings stable: the gather is a map, so without this
// an unchanged topology would render a different message every pass and an operator
// could not tell a new event from a re-render of the old one.
func dedupSorted(in []string) []string {
	set := make(map[string]bool, len(in))
	for _, s := range in {
		set[s] = true
	}
	return sortedKeys(set)
}

func addrOrUnknown(ip string) string {
	if ip == "" {
		return "an address Sentinel did not report"
	}
	return ip
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
