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
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// forsakenCooldown holds the verdict below a settling period, so a single stale or
// mid-transition gather cannot park a live instance. It does not need to cover a
// failover: a legitimate failover moves mastership to one of OUR pods, so it can
// never produce the signature below. What it covers is a bad read.
const forsakenCooldown = 30 * time.Second

// forsakenMonitoringFloor is the minimum number of reachable, MONITORING Sentinels
// whose agreement may carry a capture verdict (LR-056).
//
// Clause 2 is documented as unanimity, and LR-044's live procedure states the
// consequence it relied on: an injection had to hit ALL THREE of the victim's
// Sentinels, because "a 1-of-3 injection reads as a transition, not a verdict". That
// is true only while all three are monitoring, and nothing checked it. Both of
// forsakenSignature's `continue`s — unreachable, and no observations — drop a Sentinel
// from the DENOMINATOR rather than from the vote alone, so "everyone who spoke agrees"
// degrades to "someone spoke" as the speakers fall silent. With two peers bare, one
// Sentinel's word armed a verdict that takes both StatefulSets to zero on EmptyDir.
//
// The predicate was thereby NON-MONOTONE in evidence, which is what makes it
// indefensible rather than merely unlucky: two peers still naming a master of ours
// REFUSE the verdict (clause 3), while the same two peers saying nothing — strictly
// less evidence, same stranger — armed it.
//
// The denominator has to come from what we DEPLOYED rather than from what answered,
// which is LR-013's gate for Rule D arriving here: `reachableRedis ==
// SentinelRedisReplicas`. Hence sentinelProcessReplicas, which is fixed at three
// (sentinel HA is not horizontally scalable), and NOT spec.sentinel.quorum:
//
//   - `quorum` carries no Minimum, so `quorum: 1` is representable and would make this
//     floor vacuous — a guard defeated by a user setting, which is LR-050's rejected
//     shape one field over. At every default the two coincide at 2.
//   - It is a constant rather than a parameter, deliberately against LR-041's "put
//     mandatory values in the signature". That rule guards against a call site
//     forgetting one — and here the forgotten value's zero IS the defect, while there
//     is no per-instance value to thread. A constant cannot be zero-valued by omission.
//
// The floor is a CLAUSE, not an arming-only gate like LR-050's `rolling`. That gate
// names a fact about our own churn; this names how much evidence we have, and LR-044's
// lifecycle rests on the verdict self-clearing once the evidence is gone. So when a
// monitoring majority is no longer visible the operator stops asserting, exactly as
// every other clause does.
//
// The failure direction is chosen and it is LR-047's, not LR-043's. Withholding a
// verdict from a genuine capture costs the CAPTOR its automated cleanup and leaves the
// victim loudly `Ready=False` — bounded, non-destructive, and still diagnosed, because
// `lrctl verify`'s DetectCrossInstance deliberately has no floor (LR-039 built it to
// fire on a PARTIAL capture). Admitting a false one deletes six pods on EmptyDir, and
// pillar 3.15 makes that unrecoverable: `status.quarantinedSince` is the sole authority
// on scale-to-zero from the moment it is set, so the arming pass is the only moment any
// evidence is ever weighed.
const forsakenMonitoringFloor = int(sentinelProcessReplicas)/2 + 1

// forsakenPlan is the verdict on whether this instance has been captured by another
// Sentinel deployment sharing its master name.
type forsakenPlan struct {
	// Captured: the signature is present in THIS gather.
	Captured bool
	// Forsaken: present, and present for longer than forsakenCooldown.
	Forsaken bool
	// ForeignMaster is the live non-pod address our Sentinels are serving.
	ForeignMaster string
}

// planForsaken decides whether the instance is forsaken — captured, and beyond the
// operator's help by design (ADR-015 §9.2: nothing survives to salvage, and the
// operator structurally cannot win the reclaim).
//
// The verdict is not a repair. Its only purpose is to let the operator STOP: stop
// healing an instance whose topology is no longer its own, and stop polling it at the
// converging cadence. The instance stays loudly broken (Ready=False) until a human
// runs the runbook, which is the intended end state.
//
// Conservatism runs one way on purpose. A false positive parks a live instance, which
// is far worse than a false negative — that merely leaves today's behaviour in place.
// So EVERY clause must hold:
//
//  1. At least one reachable Sentinel that monitors SOMETHING — otherwise we know
//     nothing. (Bare Sentinels are Rule L's business, not this one's.)
//  2. Every master every reachable Sentinel monitors is at ONE address. Disagreement
//     is a transition, and transitions are not verdicts.
//  3. That address is not one of our pods, and is NOT flagged down. The down flag is
//     the discriminator that keeps ordinary post-failover debris — a dead ex-master —
//     out of this: an address that is not ours and is answering means something else
//     is alive there. Same discriminator the lrctl diagnostic uses.
//  4. No reachable Redis pod of ours is a master. While one of ours still is, the
//     instance has something to be healed back toward and the existing rules own it.
//
// The clauses are NAME-AGNOSTIC: they range over every master a Sentinel monitors,
// not only the one we currently want. Keying them on the desired name — which is
// what sn.MasterIP/sn.MasterFlags are, the single-name probe's answer — made the
// verdict evaporate the moment an owner renamed a captured instance, which is
// precisely what the LR-039/LR-042 runbook tempts them into ("we were captured, so
// let's give it a unique name"). The Sentinels keep serving the captor under the OLD
// name, the new name reads bare, clause 1 fails, and with the verdict goes ADR-016's
// quarantine — the thing that heals both sides. A capture under a stale name is a
// capture. See the rename design §7.3.
//
// The `rolling` parameter is the ONE thing the four clauses cannot see, and without it
// they are structurally unable to tell "an address that is not one of our pods" from "an
// address that is not one of our pods ANY MORE" (LR-050). A pod of ours that has just been
// replaced has no pod object left to attribute its address to — so not even LR-053's
// OwnedIPs can hold it — and is not flagged down for a whole `down-after-milliseconds`, so
// for that window it is byte-identical to a captor's live master. The two fixes are
// complementary and neither subsumes the other: OwnedIPs covers the pod that is still in
// the list (terminating); this covers the pod whose object is already gone. That is not hypothetical: it quarantined a healthy instance
// during an ordinary supported rename, at T+30 of a 42.5s window, 12.5s before the
// instance healed itself.
//
// It gates ARMING — and only arming. The two halves of that are both load-bearing:
//
//   - While rolling, a signature observed for the first time is NOT a verdict. There is
//     no evidence here that distinguishes it from our own churn, so the honest answer is
//     to hold, not to accuse.
//   - While rolling, an ALREADY-ARMED verdict is evaluated exactly as before: the gate
//     does not touch it. The caller's switch treats "not captured" as "clear it"
//     (`clearForsaken`), so a naive "return no verdict while rolling" would make a
//     panicked rename of a genuinely captured instance dissolve the verdict — which is
//     exactly the §7.3 trap the name-agnostic clauses exist to close, and with it
//     ADR-016's quarantine, the only thing that heals the CAPTOR. That mutant is what
//     the invariant row in the table guards against.
//
// So: a rollout cannot START a capture verdict, and it never CLEARS one either — only
// the ordinary clauses do, on the ordinary evidence, exactly as before. The stronger
// reading (hold an armed verdict up against an ABSENT signature while rolling) was
// implemented first and wedged the quarantine release live; see the note at the
// `!captured` return below.
//
// It is deliberately not a timer. The alternatives considered were all margins against
// `spec.sentinel.downAfterMilliseconds`, which is user-settable and unbounded, so no
// value of `forsakenCooldown` can be correct for every instance. The StatefulSet's own
// settledness is config-independent and needs no new state — and it covers strictly more
// than a rename, since a departed address of ours exists only in states where the
// StatefulSet is short of its Ready count.
//
// Accepted hole, stated rather than hidden: a permanently STUCK rollout means the gate
// never lifts, so a genuine capture arriving in that window goes undetected. The owner
// accepted it — "we don't fix on operator level if something's broken below". An instance
// whose roll is stuck is already `Ready=False` and visibly broken, and the quarantine
// exists to heal the CAPTOR, which it cannot do for an instance that cannot roll.
//
// This is a widening of what the clauses observe, and NOTHING else: their intent,
// order and conservatism are untouched, and every input that produced a verdict on
// the desired name alone still produces the same one. Two consequences of the wider
// observation set are worth stating, because both run toward the safe direction:
//
//   - A Sentinel monitoring only a stale name now counts for clause 1. That is the
//     §7.3 case itself.
//   - Two names naming two different addresses is now a clause-2 disagreement. An
//     ordinary rename transiently does exactly that (WP0 measured 88.5s of it, 56.6s
//     of which named two different LIVE pods), so the widening removes a suspicion
//     the desired-name view raised on its own rather than adding one.
func planForsaken(
	state *redisclient.ReplicationState, since *metav1.Time, now time.Time, rolling bool,
) forsakenPlan {
	captured, foreign := forsakenSignature(state)

	armed := since != nil && !since.IsZero()
	if rolling && !armed {
		// Cannot ARM: mid-roll the operator does not attribute addresses, so a
		// signature seen here is not a verdict.
		return forsakenPlan{}
	}
	if !captured {
		// The gate is a ONE-WAY suppression of arming, and never asserts a verdict the
		// evidence does not support. It deliberately does NOT hold an armed verdict up
		// against an absent signature while rolling, and the first implementation that
		// did was wedged by a live run within minutes: the quarantine RELEASE scales
		// this instance's StatefulSets 0 -> 3, which reads as unsettled, while the pods
		// come back with bare Sentinels and no signature at all. Carrying the verdict
		// through that returned Captured for a state with zero evidence, the call site
		// returned before clearForsaken, and the instance never left quarantine (LR-044
		// is explicit that the whole lifecycle rests on the verdict self-clearing once
		// the pods are gone). Absence of evidence must not become evidence.
		//
		// What that costs is nothing the §7.3 trap needs: a captured instance being
		// renamed goes on presenting the signature under the STALE name, because the
		// clauses are name-agnostic (WP4b) — verified live, the verdict survives the
		// panicked rename and the quarantine still fires. So "a roll cannot clear a
		// verdict" holds in the only sense that is true: the gate never clears one; the
		// ordinary clauses do, exactly as they did before this change.
		return forsakenPlan{}
	}

	p := forsakenPlan{Captured: true, ForeignMaster: foreign}
	if armed && now.Sub(since.Time) >= forsakenCooldown {
		p.Forsaken = true
	}
	return p
}

// forsakenSignature is the four clauses, unchanged: is the capture signature present in
// THIS gather, and at which address.
func forsakenSignature(state *redisclient.ReplicationState) (bool, string) {
	var foreign string
	monitoring := 0

	for _, sn := range state.SentinelNodes {
		if sn == nil || !sn.Reachable {
			continue
		}
		observed := monitoredAddresses(sn)
		if len(observed) == 0 {
			continue
		}
		monitoring++
		for _, m := range observed {
			if m.IP == "" {
				return false, ""
			}
			if foreign == "" {
				foreign = m.IP
			} else if foreign != m.IP {
				return false, "" // clause 2: no consensus, no verdict
			}
			// clause 3: alive, and not ours. "Ours" is the OWNED set, which includes
			// our terminating pods (LR-053): keyed on IsGhost — "is anything of ours
			// ALIVE there" — a pod we deleted a second ago answered this clause as a
			// captor, because it leaves the live topology the instant its object
			// gains a deletionTimestamp while it goes on holding its address and
			// answering on it for the whole preStop window.
			if state.IsOurs(m.IP) || flaggedDown(m.Flags) {
				return false, ""
			}
		}
	}
	// Clause 1, with the LR-056 floor: not "somebody spoke" but "a majority of the
	// Sentinels we deployed spoke, and agreed". A Sentinel that is unreachable or bare
	// contributed no observation and is correctly not counted as DISAGREEMENT above —
	// the defect was letting it also not count as EXISTING.
	if monitoring < forsakenMonitoringFloor {
		return false, ""
	}

	// clause 4
	for _, rn := range state.RedisNodes {
		if rn != nil && rn.Reachable && rn.Role == RoleMaster {
			return false, ""
		}
	}
	return true, foreign
}

// flaggedDown reports whether a Sentinel flags string marks the instance down. Mirrors
// the unexported discriminator in internal/redis used by the lrctl diagnostic; kept
// local because it is one line and the redis-side one is not exported.
func flaggedDown(flags string) bool {
	return strings.Contains(flags, "s_down") || strings.Contains(flags, "o_down")
}

// monitoredAddresses is every (address, flags) this Sentinel currently monitors,
// under any master name.
//
// Two sources, deliberately BOTH:
//
//   - The desired name's dedicated probe (sn.MasterIP / sn.MasterFlags, gated on
//     sn.Monitoring). This is what planForsaken read before it went name-agnostic,
//     and it stays the authority on the name we want, so behaviour on that name is
//     unchanged in every case.
//   - sn.MonitoredMasters — the full `SENTINEL MASTERS` list. The desired name
//     appears in it too and is not filtered out: the same address twice agrees with
//     itself, and if the two reads DISAGREE (they are separate round trips) that is
//     a transition clause 2 should refuse anyway.
//
// An EMPTY MonitoredMasters list means "we could not read it", never "this Sentinel
// monitors nothing" — the extra round trip degrades to empty rather than to
// Reachable:false. So it contributes no observations and the verdict falls back to
// the desired-name view alone, which is the correct reading of no evidence and is
// LR-041's class of mistake avoided.
func monitoredAddresses(sn *redisclient.SentinelNodeState) []redisclient.MonitoredMaster {
	observed := make([]redisclient.MonitoredMaster, 0, len(sn.MonitoredMasters)+1)
	if sn.Monitoring {
		observed = append(observed, redisclient.MonitoredMaster{IP: sn.MasterIP, Flags: sn.MasterFlags})
	}
	observed = append(observed, sn.MonitoredMasters...)
	return observed
}
