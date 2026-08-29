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
//     something else is alive there and the entry is evidence of a capture, not debris —
//     UNLESS `rolling`, in which case it is unattributable rather than foreign (below).
//   - G6 per Sentinel, the desired name is already present on THAT Sentinel.
func planStaleMasterNames(
	state *redisclient.ReplicationState,
	desired string,
	quorum int,
	forsaken bool,
	rolling bool,
) StaleMasterNamePlan {
	// `rolling` — our own Redis StatefulSet is not settled (LR-050). G5's discriminator
	// is byte-identical to planForsaken clause 3, and it goes true during an ORDINARY
	// rename: a pod we have just replaced has no pod object left to attribute its
	// address to — so not even LR-053's OwnedIPs can hold it — and is not flagged down
	// for a whole down-after-milliseconds. Reported as `Foreign` that emits a
	// Warning reading "this instance may be captured — do not rename to escape a
	// capture" at the exact moment the owner is performing the rename the runbook asked
	// for.
	//
	// So while rolling such an address is UNATTRIBUTABLE, not foreign: `Deferred`,
	// naming the gate. What does NOT change is the refusal itself — an entry we cannot
	// attribute is never pruned, in either reading. Only the sentence differs, and
	// whether the operator raises its voice.
	//
	// Note the asymmetry, which is deliberate: Rule N still RUNS during churn (§7.5 —
	// that is the whole point of it sitting before Rule A, so the two-name window stays
	// intra-pass), it just stops ATTRIBUTING. An entry whose address IS one of our pods
	// is attributable whatever the StatefulSet is doing, and is pruned as usual.
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
		entries        []staleEntry
		skipped        []string
		allStale       = map[string]bool{}
		failoverUnder  []string
		foreign        []string
		unattributable []string
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
			// "Ours" is the OWNED set — terminating pods included, LR-053 — because a
			// pod we have just deleted still holds its address and still answers on it;
			// reading that as somebody else's is what made this gate accuse the owner
			// mid-handover.
			// A dead ex-master of ours (ordinary post-failover debris) is flagged
			// down; a captor's master answers with clean flags, which is exactly why
			// no failover ever fires on it.
			//
			// An entry with no address at all lands here too, and deliberately: it
			// cannot be attributed to one of our pods, and refusing to prune is the
			// safe direction for an entry we cannot read.
			if !state.IsOurs(m.IP) && !flaggedDown(m.Flags) {
				entry := fmt.Sprintf("%q (at %s)", m.Name, addrOrUnknown(m.IP))
				if rolling {
					unattributable = append(unattributable, entry)
				} else {
					foreign = append(foreign, entry)
				}
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
	if len(unattributable) > 0 {
		return deferStaleNames("G5", fmt.Sprintf(
			"stale master name(s) %s point at an address that is not one of our pods and is not "+
				"flagged down, but this instance's own Redis StatefulSet is mid-rollout — a pod we "+
				"have just replaced looks exactly like this until down-after-milliseconds elapses, "+
				"so the address is unattributable rather than foreign. Nothing was removed",
			strings.Join(dedupSorted(unattributable), ", ")), staleList)
	}

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
	case !state.LiveTopologyIPs[state.RealMasterIP]:
		// LIVE topology, deliberately, and this is the one clause in Rule N that is
		// NOT the attribution question (LR-053). G2 asks whether there is a master to
		// keep monitoring after the prune; a terminating pod of ours is ours but is
		// not that, and admitting it would let a destructive REMOVE be anchored on a
		// pod that is leaving — the LR-015 leaderless deadlock this gate exists to
		// prevent. The clause below reads RedisNodes, which is keyed on the same live
		// set, so any other choice would make the three clauses disagree.
		return deferStaleNames("G2",
			fmt.Sprintf("the consensus master %s is not one of our live pods", state.RealMasterIP), staleList)
	case master == nil || !master.Reachable || master.Role != RoleMaster:
		return deferStaleNames("G2",
			fmt.Sprintf("the consensus master %s does not report itself a reachable master",
				state.RealMasterIP), staleList)
	}

	// G3 — the per-entry test is the load-bearing half and covers every monitored name,
	// stale ones included: a failover under the stale name is still a real state machine
	// reconfiguring our pods.
	//
	// state.FailoverActive is no longer along for the ride. It was permanently false
	// when this was written (a dead wire key, LR-052) and the comment here said so; it
	// is now live, and it genuinely fires where the per-entry test cannot: failoverUnder
	// is collected from MonitoredMasters, which degrades to an EMPTY list on a read
	// failure (LR-041's deliberate choice), whereas FailoverActive comes from the
	// desired name's own successful probe. So `SENTINEL MASTERS` failing while
	// `SENTINEL master <desired>` succeeds is exactly the case the OR covers — a
	// strengthening in the refusal direction, which is G3's safe direction.
	//
	// That case is also why the message is built rather than formatted straight: with
	// failoverUnder empty it rendered `under master name(s) []`, naming nothing.
	if len(failoverUnder) > 0 || state.FailoverActive {
		return deferStaleNames("G3", failoverInFlightMessage(dedupSorted(failoverUnder)), staleList)
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

// failoverInFlightMessage renders G3's refusal.
//
// It exists because the name list can legitimately be EMPTY: G3 also refuses on
// state.FailoverActive, which comes from the desired name's own probe, and that case
// is reachable exactly when `SENTINEL MASTERS` could not be read at all — so there
// are no per-entry names to quote (LR-052 found this rendering, LR-053 fixes it).
// `a failover is in flight under master name(s) []` names nothing and reads like a
// bug in the operator rather than a state of the instance.
func failoverInFlightMessage(names []string) string {
	if len(names) == 0 {
		return "a Sentinel reports a failover in flight for " + "the desired master name " +
			"(the full monitored-master list could not be read this pass, so no other name can be named)"
	}
	return fmt.Sprintf("a failover is in flight under master name(s) %s", quoteJoin(names))
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
