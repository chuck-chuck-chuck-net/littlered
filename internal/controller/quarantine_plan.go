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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// quarantinePhase is the decided state of the forsaken-gated quarantine lifecycle
// (LR-044). It is a decision, not persisted state: every value is re-derived each
// pass from the capture verdict plus the two status fields the lifecycle needs
// (status.quarantinedSince, status.quarantineAttempts).
type quarantinePhase string

const (
	// quarantineNone: nothing to do — the instance is not captured and no quarantine
	// is in flight. Desired replicas are whatever the CR asks for.
	quarantineNone quarantinePhase = ""
	// quarantineHoldSuspected: captured, but the verdict has not cleared
	// forsakenCooldown yet. A suspicion is not a verdict, and this planner deletes
	// pods, so it waits for planForsaken to commit.
	quarantineHoldSuspected quarantinePhase = "HoldSuspected"
	// quarantineHoldDataPresent: forsaken, but a reachable pod holds data that is not
	// the captor's replicated copy. Never quarantined.
	quarantineHoldDataPresent quarantinePhase = "HoldDataPresent"
	// quarantineHoldDataUnknown: forsaken, but a pod of ours could not be proven
	// empty (we could not dial it and the kubelet still calls its redis Ready).
	// Never quarantined.
	quarantineHoldDataUnknown quarantinePhase = "HoldDataUnknown"
	// quarantineStart: the pass that arms the quarantine — take the pods away and
	// count the attempt.
	quarantineStart quarantinePhase = "Quarantined"
	// quarantineSettling: quarantined and inside quarantineSettlePeriod. Stay at zero.
	quarantineSettling quarantinePhase = "Settling"
	// quarantineRelease: the settling period elapsed and attempts remain — let the
	// pods back so Rule L's no-data reseed re-bootstraps them.
	quarantineRelease quarantinePhase = "Released"
	// quarantineLatched: the attempt budget is spent. Stay at zero indefinitely; the
	// instance's configuration, not its luck, is the problem.
	quarantineLatched quarantinePhase = "Latched"
)

const (
	// quarantineSettlePeriod is how long the instance stays at zero replicas before
	// its pods are allowed back. The number is DERIVED, not picked:
	//
	//   - The captor is Running, so it reconciles on the STEADY interval (30s). Every
	//     step of its recovery is observed on those passes, not on a fast loop.
	//   - Its recovery needs, in order: Sentinel to re-read its master's INFO and stop
	//     seeing the victim's pods as replicas; those departed entries to become
	//     ordinary s_down ghost replicas (a dead replica never ages out, LR-024); and
	//     Rule D's gate chain — living+reachable consensus master (LR-008), >=1 healthy
	//     known replica (LR-011), K8s-grounded wholeness (LR-013) — to pass. That is a
	//     couple of steady passes, not one.
	//   - It also shrinks the warm-IP window: while the captor still lists the victim's
	//     OLD addresses as s_down replicas, a fresh victim pod landing on one of those
	//     recycled IPs is the very coincidence that starts a capture (LR-039).
	//
	// 120s covers ~4 steady passes and matches the existing cluster-mode precedent
	// (status.cluster.wipeDeadlockSince, LR-023). It is deliberately NOT the "five
	// minutes" shape LR-042 rejected as "a guess with no relationship to anything
	// real".
	quarantineSettlePeriod = 120 * time.Second

	// quarantineAttemptLimit is N: how many quarantine attempts an instance gets
	// before the operator latches it down. 2 means one re-roll and then stop.
	//
	// Bounded rather than unbounded, because every recapture re-pollutes the captor:
	// the victim's pods re-attach to its master and its Sentinel candidate set is
	// dirty again until its own Rule D cleans up. An unbounded retry does not merely
	// fail to fix the victim, it repeatedly degrades a healthy neighbour.
	quarantineAttemptLimit int32 = 2

	// quarantineAttemptLimitDangerous is N when the instance's OWN configuration is
	// known-dangerous: auth disabled AND the effective master name is the shared
	// legacy one. Both conditions are what make a capture reachable in the first
	// place (the name is Sentinel's only isolation boundary; auth is the only thing
	// closing the address-adoption path — ADR-015 §9.4), so a recapture is the
	// expected outcome rather than bad luck. Such an instance gets no re-roll.
	quarantineAttemptLimitDangerous int32 = 1
)

// quarantineInput is everything planQuarantine decides from. All of it is either this
// pass's verdict or persisted status; there is no I/O and no clock read inside.
type quarantineInput struct {
	// Captured / Forsaken come from planForsaken on THIS pass. Both false is also the
	// legitimate "no gather available yet" input: an already-armed quarantine is
	// decided from QuarantinedSince alone, so a caller that runs before the gather
	// (the StatefulSet reconcile, which happens before reconcileSentinelCluster) gets
	// the correct ScaleToZero without needing a verdict.
	Captured, Forsaken bool

	// DataAtRisk / DataUnverified come from quarantineDataRisk. Either one refuses.
	DataAtRisk, DataUnverified bool

	// QuarantinedSince is status.quarantinedSince — the arming marker. It is the ONLY
	// thing that survives the quarantine, and that is load-bearing: once the pods are
	// gone there is no reachable monitoring Sentinel, so planForsaken clause 1 fails
	// and the capture verdict self-clears. The signature cannot hold the state; the
	// marker and the counter must.
	QuarantinedSince *metav1.Time

	// Attempts is status.quarantineAttempts — how many quarantines this instance has
	// already been through.
	Attempts int32

	// Dangerous selects quarantineAttemptLimitDangerous. See its comment.
	Dangerous bool

	Now time.Time
}

// quarantinePlan is the decision. Arm/Clear/NextAttempts are what the caller must
// persist; ScaleToZero is what M3's replica computation consumes.
type quarantinePlan struct {
	Phase quarantinePhase

	// ScaleToZero: the desired Redis AND Sentinel replica count is 0 this pass.
	// Sentinel is not optional — the victim's *sentinels* publish hellos on the
	// captor's master's channel under the shared name, so the captor learns them as
	// peers. That is the num-other-sentinels inflation, which distorts the captor's
	// quorum math and puts foreign sentinels in its elections.
	ScaleToZero bool

	// Arm: persist QuarantinedSince = now and Attempts = NextAttempts.
	// Clear: persist QuarantinedSince = nil (the counter is NOT cleared here).
	Arm, Clear bool

	// NextAttempts is the attempt count that should be persisted after this pass.
	NextAttempts int32

	// AttemptLimit is the N in effect, reported so the operational signal can say
	// "attempt 2 of 2" without re-deriving the policy.
	AttemptLimit int32
}

// planQuarantine decides the quarantine lifecycle of a forsaken instance (LR-044).
//
// Why quarantine at all, given ADR-015 §9.2 DECLINED automated recovery: this is not a
// reversal of that decision, because it reclaims nothing. A capture has two sides and
// only one is loud. The victim sits at Ready=False; the CAPTOR reports Running/Ready
// with the victim's pods in its Sentinel replica list, so its failover-candidate set is
// poisoned and its next master death can promote a foreign pod. The captor must not be
// operated on directly — its Sentinels are not confused, the victim's pods are
// GENUINELY replicating from its master, so a SENTINEL RESET there clears a list that
// repopulates seconds later (and RESET is the LR-024 hazard). Taking the victim's pods
// away removes the cause, and the captor then heals through Rule D, which already
// exists and whose gates all pass once the foreign replicas are merely dead entries.
//
// The victim itself comes back EMPTY — which §9.2 names as the only achievable outcome
// and equates to delete-and-recreate. That is the whole claim: a worthless instance is
// stopped so a healthy neighbour recovers, then re-bootstrapped empty by Rule L.
//
// Decision order matters. An armed quarantine is decided FIRST and without reference
// to the verdict, because the verdict provably self-clears while it is in force.
func planQuarantine(in quarantineInput) quarantinePlan {
	limit := quarantineAttemptLimit
	if in.Dangerous {
		limit = quarantineAttemptLimitDangerous
	}
	p := quarantinePlan{NextAttempts: in.Attempts, AttemptLimit: limit}

	if in.QuarantinedSince != nil && !in.QuarantinedSince.IsZero() {
		switch {
		case in.Attempts >= limit:
			// The budget is spent. Latching is the point: releasing again would hand
			// the same pods back to the same captor and dirty it a third time.
			p.Phase, p.ScaleToZero = quarantineLatched, true
		case in.Now.Sub(in.QuarantinedSince.Time) < quarantineSettlePeriod:
			p.Phase, p.ScaleToZero = quarantineSettling, true
		default:
			// Release. Rule L sees all Sentinels bare, no master and zero data
			// holders — its no-data reseed signature, which needs no opt-in
			// (LR-015). No bootstrap machinery is re-armed.
			p.Phase, p.Clear = quarantineRelease, true
		}
		return p
	}

	switch {
	case !in.Captured:
		p.Phase = quarantineNone
	case in.DataAtRisk:
		p.Phase = quarantineHoldDataPresent
	case in.DataUnverified:
		p.Phase = quarantineHoldDataUnknown
	case !in.Forsaken:
		p.Phase = quarantineHoldSuspected
	default:
		p.Phase, p.ScaleToZero, p.Arm = quarantineStart, true, true
		p.NextAttempts = in.Attempts + 1
	}
	return p
}

// quarantineDataRisk classifies whether taking this instance's pods away could destroy
// data, from the gather alone.
//
//   - atRisk: a reachable pod holds keys that are NOT explained by the capture. Keys on
//     a pod that is a link-up replica of the captor's master are the CAPTOR's dataset,
//     replicated in — the original is still on the captor, so discarding the copy loses
//     nothing. Keys anywhere else may be the only copy in existence. That covers the one
//     path ADR-015 §9.2 could not rule out on timing alone ("replication blocked before
//     the sync starts"), and it covers it whatever the timing, which is why it is
//     load-bearing rather than belt-and-braces.
//
//   - unverified: a pod of ours did not answer AND the kubelet still reports its redis
//     container Ready, so it cannot be proven empty. The operator's own dial is not
//     blackhole-proof (LR-017), so such a pod may be serving clients perfectly well while
//     being invisible to us. Refusing is the safe direction: a false negative merely
//     leaves today's behaviour, while a false positive deletes the last copy.
//
//     Readiness, not reachability, is what this clause is keyed on — the correction M2
//     deferred to the wiring. Keying it on the gather made a permanently crash-looping or
//     blackholing pod "unverified" forever, holding the quarantine open indefinitely and
//     so keeping the CAPTOR dirty for exactly as long: a pod that can never answer would
//     have vetoed the one action that helps the healthy neighbour. LR-023 established the
//     right signal for precisely this judgement — the kubelet's local readiness probe is
//     authoritative and blackhole-proof, and in a pure in-memory (EmptyDir) instance a
//     not-Ready redis holds no data, so discarding it loses nothing by construction. A
//     pod the kubelet has no view of (absent from the map) is treated like a Ready one:
//     unknown readiness is not evidence of emptiness.
//
// Both are computed over pods the gather knows about, i.e. pods with an IP that are not
// terminating. A TERMINATING pod holding data is therefore invisible here — the guard is
// only as good as what the ground truth is allowed to contain (LR-038) — and that is
// judged harmless rather than overlooked: a terminating pod's RAM is gone whatever this
// planner decides, so the quarantine adds no harm to it. Do not "fix" it by widening the
// gather; LR-038 records why that is worse than the gap (a terminating pod in the gather
// reads as live topology to every other rule).
func quarantineDataRisk(
	state *redisclient.ReplicationState, foreignMaster string, redisReady map[string]bool,
) (atRisk, unverified bool) {
	if state == nil {
		return false, true
	}
	for _, rn := range state.RedisNodes {
		if rn == nil {
			continue
		}
		if !rn.Reachable {
			// Not-Ready per the kubelet ⇒ redis is down ⇒ no data (LR-023). Only a pod
			// the kubelet still calls Ready — or one it has no view of — is unverified.
			if ready, known := redisReady[rn.PodName]; !known || ready {
				unverified = true
			}
			continue
		}
		if rn.Keys <= 0 {
			continue
		}
		captorsCopy := foreignMaster != "" && rn.MasterHost == foreignMaster && rn.LinkStatus == "up"
		if !captorsCopy {
			atRisk = true
		}
	}
	return atRisk, unverified
}
