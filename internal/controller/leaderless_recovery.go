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
	"time"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// recoveryAction is what Rule L (leaderless bootstrap-deadlock recovery) should do
// for the current ground truth. It is the output of the pure decision function
// planLeaderlessRecovery, so the full gate/tier matrix can be unit-tested without
// any Kubernetes or Redis I/O.
type recoveryAction int

const (
	// recoveryClearMarker: not (or no longer) a bare-sentinel deadlock. Clear any
	// LeaderlessSince marker and take no recovery action.
	recoveryClearMarker recoveryAction = iota
	// recoveryStartCooldown: deadlock just observed; stamp LeaderlessSince and wait.
	recoveryStartCooldown
	// recoveryWait: deadlock persists but the cooldown has not elapsed, or there is
	// no eligible master to seed yet. Do nothing this pass.
	recoveryWait
	// recoverySeedNoData: no reachable pod holds data; seed redis-0 as master.
	recoverySeedNoData
	// recoveryPromoteSurvivor: exactly one reachable pod holds data; promote it.
	recoveryPromoteSurvivor
	// recoveryRefuse: two or more pods hold data and the unsafe opt-in is off; refuse.
	recoveryRefuse
	// recoveryUnsafeElect: two or more pods hold data and the opt-in is on; force-
	// elect the most-complete pod, discarding the rest.
	recoveryUnsafeElect
	// recoveryRefuseUnverified: at least one Redis pod REFUSED the operator's
	// credential (LR-051). Such a pod is a LIVE server whose keyspace we cannot
	// read, so no election can be shown to be lossless — refuse, loudly, until a
	// human fixes the credential. Distinct from recoveryRefuse, which means "we can
	// see the holders and there are too many": here we cannot see them at all, and
	// the two want opposite remedies.
	recoveryRefuseUnverified
)

// leaderlessPlan is the decision returned by planLeaderlessRecovery.
type leaderlessPlan struct {
	action   recoveryAction
	masterIP string // pod to elect (seed/promote/unsafe actions only)
	diverged bool   // unsafe action: holders span multiple replication lineages
	holders  int    // number of reachable data holders (for messaging)
	// unverified: pods that refused the operator's credential, so their keyspace is
	// unknown (recoveryRefuseUnverified only). Carried for the message — naming the
	// pods and what the server said is what turns this from "stuck" into "fix the
	// Secret" (auth design §3.5a Path B).
	unverified []*redisclient.RedisNodeState
}

// unprovablyEmptyVeto is the shared LR-051 guard for both recovery planners.
//
// It sits AFTER the detection and cooldown gates and BEFORE the act step, and that
// placement is the whole of it: the veto is about ACTIONS THAT DISCARD DATA, not
// about observing. Hoisting it above the gates would strand the LeaderlessSince
// marker (the clear-marker branch is a no-op, not an action) and would make a
// credential problem look like a deadlock; putting it after the act step would be
// no veto at all.
//
// It is deliberately NOT overridable by allowUnsafeRebootstrapOnDeadlock. That
// opt-in authorizes discarding a set of holders the owner could SEE — the message
// it is answering names them and their key counts. Under a credential mismatch the
// operator cannot see the set at all, so the authorization was never given with
// knowledge of what it authorizes, and BestDataHolder would be choosing among the
// pods that happen to still answer rather than among the pods that hold data. The
// remedy is trivial, always available and non-destructive (fix the Secret, or
// delete and recreate the CR — ADR-015 §9.2's accepted fallback), so a knob that
// overrides "I cannot see" buys nothing and costs the dataset.
func unprovablyEmptyVeto(state *redisclient.ReplicationState) (leaderlessPlan, bool) {
	unverified := state.AuthFailedRedisNodes()
	if len(unverified) == 0 {
		return leaderlessPlan{}, false
	}
	return leaderlessPlan{action: recoveryRefuseUnverified, unverified: unverified}, true
}

// planLeaderlessRecovery is the pure decision function for Rule L. It performs no
// I/O: given the gathered ground truth and timing, it returns what the operator
// should do. The guard order matters and each step is independently testable:
//
//  1. Not a bare-sentinel deadlock (some sentinel is monitoring, or too few are
//     reachable to form a quorum) -> clear the marker, do nothing.
//  2. First observation -> start the cooldown.
//  3. Within the cooldown -> wait.
//  4. Cooldown elapsed -> act by data-holder count: 0 -> seed redis-0; 1 -> promote
//     the sole holder (safe, no data discarded); >=2 -> refuse unless opted in, then
//     force-elect the most-complete holder.
//
// bootstrapMasterIP is the pre-resolved redis-0 IP for the no-data case ("" if none
// is eligible yet). now/leaderlessSince/cooldown drive the persistence gate.
func planLeaderlessRecovery(
	state *redisclient.ReplicationState,
	quorum int,
	allowUnsafe bool,
	bootstrapMasterIP string,
	leaderlessSince *time.Time,
	now time.Time,
	cooldown time.Duration,
) leaderlessPlan {
	bare, reachable := state.AllSentinelsBare()
	if !bare || reachable < quorum {
		return leaderlessPlan{action: recoveryClearMarker}
	}
	if leaderlessSince == nil {
		return leaderlessPlan{action: recoveryStartCooldown}
	}
	if now.Sub(*leaderlessSince) < cooldown {
		return leaderlessPlan{action: recoveryWait}
	}

	// LR-051: every branch below this line either elects a master or refuses to. A
	// pod that refused our credential is a live server whose keyspace is unknown, so
	// none of them can be shown to be lossless.
	if plan, veto := unprovablyEmptyVeto(state); veto {
		return plan
	}

	holders := state.DataHolders()
	switch {
	case len(holders) == 0:
		if bootstrapMasterIP == "" {
			return leaderlessPlan{action: recoveryWait}
		}
		return leaderlessPlan{action: recoverySeedNoData, masterIP: bootstrapMasterIP}
	case len(holders) == 1:
		return leaderlessPlan{action: recoveryPromoteSurvivor, masterIP: holders[0].IP, holders: 1}
	default:
		if !allowUnsafe {
			return leaderlessPlan{action: recoveryRefuse, holders: len(holders)}
		}
		best, diverged := state.BestDataHolder()
		if best == nil { // defensive; holders is non-empty so this cannot happen
			return leaderlessPlan{action: recoveryWait, holders: len(holders)}
		}
		return leaderlessPlan{action: recoveryUnsafeElect, masterIP: best.IP, diverged: diverged, holders: len(holders)}
	}
}

// needsPromotion reports whether the elected master must be promoted with
// REPLICAOF NO ONE before it can serve as master: it is a *reachable* pod that is
// currently a replica (following a now-dead master). An unreachable / wait-looping
// elect starts fresh as master via its startup script, and an elect already
// reporting role:master needs nothing.
func needsPromotion(state *redisclient.ReplicationState, masterIP string) bool {
	rn := state.RedisNodes[masterIP]
	return rn != nil && rn.Reachable && rn.Role != RoleMaster
}
