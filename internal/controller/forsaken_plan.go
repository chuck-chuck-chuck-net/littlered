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
//  1. At least one reachable, monitoring Sentinel — otherwise we know nothing. (Bare
//     Sentinels are Rule L's business, not this one's.)
//  2. Every reachable monitoring Sentinel agrees on ONE master address. Disagreement
//     is a transition, and transitions are not verdicts.
//  3. That address is not one of our pods, and is NOT flagged down. The down flag is
//     the discriminator that keeps ordinary post-failover debris — a dead ex-master —
//     out of this: an address that is not ours and is answering means something else
//     is alive there. Same discriminator the lrctl diagnostic uses.
//  4. No reachable Redis pod of ours is a master. While one of ours still is, the
//     instance has something to be healed back toward and the existing rules own it.
func planForsaken(
	state *redisclient.ReplicationState, since *metav1.Time, now time.Time,
) forsakenPlan {
	var foreign string
	monitoring := 0

	for _, sn := range state.SentinelNodes {
		if sn == nil || !sn.Reachable || !sn.Monitoring {
			continue
		}
		monitoring++
		if sn.MasterIP == "" {
			return forsakenPlan{}
		}
		if foreign == "" {
			foreign = sn.MasterIP
		} else if foreign != sn.MasterIP {
			return forsakenPlan{} // clause 2: no consensus, no verdict
		}
		// clause 3: alive, and not ours
		if !state.IsGhost(sn.MasterIP) || flaggedDown(sn.MasterFlags) {
			return forsakenPlan{}
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
