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
	"testing"
	"time"
)

// site_balance_property_test.go binds the ADR-012 D2 balance THRESHOLDS to the D1 takeover
// DECISION they exist to predict. D2's thresholds are a placement rule; D1's planner is the
// thing that actually recovers a site loss. Nothing connected the two, which is how a bound
// derived for the NATIVE path (survivors keep quorum, ceil(K/2)-1) came to gate the TAKEOVER
// path (lost side lacks quorum, floor(K/2)) inside a single ADR — off by one for even K,
// squarely on the recommended dual-site topology (LR-031).
//
// A hand-written expectation table cannot catch that class: authored beside the
// implementation, it restates the implementation's assumption, bug included. These tests
// instead drive the REAL planSiteTakeover over every (K, loss) pair and assert the
// predicates agree with what it does.

// takeoverInputForLoss synthesises the planner input for "a site holding l of the K
// slot-serving masters was lost", with every non-arithmetic gate open (formed, quiet,
// cooldown elapsed, no unsafe opt-in) so the arithmetic alone decides.
//
// survivorsClusterFail is derived, not chosen: a survivor reports its own cluster_state:fail
// exactly when the reachable masters are below the majority — Redis's unconditional minority
// check. Hard-coding it would let the test open a gate reality keeps shut.
func takeoverInputForLoss(k, l int) siteTakeoverInput {
	return siteTakeoverInput{
		clusterFormed:        true,
		size:                 k,
		reachableMasters:     k - l,
		survivorsClusterFail: k-l < majorityOf(k),
		quiet:                true,
		// One orphaned shard with a promotable survivor: condition 1 is about the loss
		// having something to promote, which is placement (sitesPerShard >= 2), not
		// arithmetic. Keeping it satisfied isolates the thresholds under test.
		orphanTargets: []orphanTarget{{shard: 0, key: "siteA/x-shard-0-1", ip: ipMaster, site: "siteA"}},
	}
}

// planLoss runs the real planner on a synthesised loss of l masters, past the cooldown.
func planLoss(k, l int) siteTakeoverPlan {
	armed := time.Now().Add(-10 * time.Minute)
	return planSiteTakeover(takeoverInputForLoss(k, l), &armed, time.Now(), time.Minute)
}

// TestTakeoverFloorPredictsRecoverability asserts the D2 FLOOR is exactly the boundary of
// what planSiteTakeover can recover unattended: a loss of l masters auto-survives (fires a
// takeover, or is still natively electable) iff l <= takeoverSurvivalFloor(K). Above the
// floor the planner refuses — the drift-refusal needing a human or allowUnsafeSiteTakeover.
//
// This is the assertion that fails against the pre-LR-031 bound: for even K the floor was
// one too low, so K=4 at 2+2 (the D3-recommended dual-site placement) was declared
// unsurvivable while the planner fires on it with no opt-in.
func TestTakeoverFloorPredictsRecoverability(t *testing.T) {
	for k := 3; k <= 10; k++ {
		for l := 1; l <= k; l++ {
			plan := planLoss(k, l)
			recovered := plan.action != siteTakeoverRefuse
			within := l <= takeoverSurvivalFloor(k)
			if recovered != within {
				t.Errorf("K=%d lost=%d: planSiteTakeover action=%v (recovered=%v) but "+
					"l <= takeoverSurvivalFloor(%d)=%d says %v",
					k, l, plan.action, recovered, k, takeoverSurvivalFloor(k), within)
			}
		}
	}
}

// TestNativeTargetPredictsNativeElection asserts the D2 TARGET is exactly the boundary of
// what Redis still resolves on its own: the survivors keep quorum — so the planner stands
// down (WaitNative) rather than intervening — iff l <= nativeSurvivalTarget(K).
func TestNativeTargetPredictsNativeElection(t *testing.T) {
	for k := 3; k <= 10; k++ {
		for l := 1; l <= k; l++ {
			plan := planLoss(k, l)
			native := plan.action == siteTakeoverWaitNative
			within := l <= nativeSurvivalTarget(k)
			if native != within {
				t.Errorf("K=%d lost=%d: planSiteTakeover action=%v (native=%v) but "+
					"l <= nativeSurvivalTarget(%d)=%d says %v",
					k, l, plan.action, native, k, nativeSurvivalTarget(k), within)
			}
		}
	}
}

// TestThresholdOrdering pins the relationship the two-threshold model rests on (ADR-012 D2):
// the takeover condition is the WEAKER one, so the floor is never below the target, and the
// two coincide exactly for odd K — the placement-side reading of D1's blocking-minority
// window (empty for odd K, one value wide for even K).
func TestThresholdOrdering(t *testing.T) {
	for k := 1; k <= 20; k++ {
		target, floor := nativeSurvivalTarget(k), takeoverSurvivalFloor(k)
		if target > floor {
			t.Errorf("K=%d: nativeSurvivalTarget=%d exceeds takeoverSurvivalFloor=%d; "+
				"the takeover condition must be the weaker one", k, target, floor)
		}
		if k%2 == 1 && target != floor {
			t.Errorf("K=%d (odd): thresholds must coincide, got target=%d floor=%d", k, target, floor)
		}
		if k%2 == 0 && floor-target != 1 {
			t.Errorf("K=%d (even): thresholds must differ by exactly one, got target=%d floor=%d", k, target, floor)
		}
	}
}

// TestSiteLossResilientMatchesReachableFloor asserts the structural predicate behind the
// ReducedSiteLossResilience warning is exactly "some placement of K masters over S sites
// keeps every site at or below the floor" — verified by construction (spread the masters as
// evenly as possible, which minimises the maximum) rather than by restating the formula.
//
// Consequence recorded in ADR-012: given Redis Cluster's three-master minimum the warning
// fires in exactly one regime, two sites with an odd K.
func TestSiteLossResilientMatchesReachableFloor(t *testing.T) {
	for k := 3; k <= 12; k++ {
		for s := 2; s <= 6; s++ {
			// The evenly-spread placement minimises max_i m_i: ceil(K/S).
			bestMax := (k + s - 1) / s
			want := bestMax <= takeoverSurvivalFloor(k)
			if got := siteLossResilient(k, s); got != want {
				t.Errorf("siteLossResilient(shards=%d, sites=%d) = %v, want %v "+
					"(best achievable max_i m_i = %d, floor = %d)",
					k, s, got, want, bestMax, takeoverSurvivalFloor(k))
			}
			// The regime statement in ADR-012's Consequences.
			if wantRegime := s != 2 || k%2 == 0; siteLossResilient(k, s) != wantRegime {
				t.Errorf("siteLossResilient(shards=%d, sites=%d) = %v; ADR-012 says the warning "+
					"fires iff two sites with odd K", k, s, siteLossResilient(k, s))
			}
		}
	}
}
