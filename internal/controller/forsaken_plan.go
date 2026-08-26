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
	state *redisclient.ReplicationState, since *metav1.Time, now time.Time,
) forsakenPlan {
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
				return forsakenPlan{}
			}
			if foreign == "" {
				foreign = m.IP
			} else if foreign != m.IP {
				return forsakenPlan{} // clause 2: no consensus, no verdict
			}
			// clause 3: alive, and not ours
			if !state.IsGhost(m.IP) || flaggedDown(m.Flags) {
				return forsakenPlan{}
			}
		}
	}
	if monitoring == 0 { // clause 1
		return forsakenPlan{}
	}

	// clause 4
	for _, rn := range state.RedisNodes {
		if rn != nil && rn.Reachable && rn.Role == RoleMaster {
			return forsakenPlan{}
		}
	}

	p := forsakenPlan{Captured: true, ForeignMaster: foreign}
	if since != nil && !since.IsZero() && now.Sub(since.Time) >= forsakenCooldown {
		p.Forsaken = true
	}
	return p
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
