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

const testTakeoverCooldown = 60 * time.Second

// oneOrphan is the canonical single orphaned-shard target (shard 1's surviving replica in
// a surviving site).
func oneOrphan() []orphanTarget {
	return []orphanTarget{{shard: 1, key: "alpha/inst-shard-1-1", ip: ipNode9, site: "alpha", offset: 100, replid: "L1"}}
}

// TestPlanSiteTakeover is the red-first gate table for ADR-012 decision D1: the witness-free
// takeover whose safety rests entirely on Domain-M quorum arithmetic — the reachable-master
// majority check (condition 2) and the unreachable-master minority self-fence (the fence),
// corroborated by the survivors' own cluster_state:fail. There is no Kubernetes witness and
// no Contradiction/Hold-on-alive branch anymore.
func TestPlanSiteTakeover(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	armed := now.Add(-2 * testTakeoverCooldown) // well past the cooldown
	fresh := now.Add(-testTakeoverCooldown / 2) // within the cooldown

	cases := []struct {
		name        string
		in          siteTakeoverInput
		since       *time.Time
		wantAction  siteTakeoverAction
		wantTargets int // -1 = don't assert
	}{
		{
			name: "never-bootstrapped cluster (no slots anywhere) => None even under the all-masters-lost signature (LR-026)",
			in: siteTakeoverInput{
				// The bringup signature: fresh nodes classified as promotable survivors,
				// zero slot-serving masters, survivors report cluster_state:fail (uncovered),
				// quiet, past cooldown — every other gate would fire. Only clusterFormed=false
				// must veto it, so bootstrap can proceed.
				clusterFormed: false,
				size:          3, reachableMasters: 0, orphanTargets: oneOrphan(),
				survivorsClusterFail: true, quiet: true,
			},
			since: &armed, wantAction: siteTakeoverNone, wantTargets: -1,
		},
		{
			name: "no orphaned survivor => None (condition 1 fails)",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          2, reachableMasters: 1, orphanTargets: nil,
				survivorsClusterFail: true, quiet: true,
			},
			since: &armed, wantAction: siteTakeoverNone, wantTargets: -1,
		},
		{
			name: "survivors hold majority => WaitNative (condition 2 fails)",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          3, reachableMasters: 2, orphanTargets: oneOrphan(),
				survivorsClusterFail: false, quiet: true,
			},
			since: &armed, wantAction: siteTakeoverWaitNative, wantTargets: -1,
		},
		{
			name: "below quorum arithmetically but survivors do NOT report fail => WaitNative (no corroboration)",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          2, reachableMasters: 1, orphanTargets: oneOrphan(),
				survivorsClusterFail: false, quiet: true,
			},
			since: &armed, wantAction: siteTakeoverWaitNative, wantTargets: -1,
		},
		{
			// THE D1 BEHAVIORAL CHANGE (red-first anchor): an isolated-alive MINORITY site
			// loss — even split (unreachable 1 < majority 2), survivors report their own
			// cluster_state:fail, quiet, past cooldown. Under the OLD witness design this
			// physical situation (site API reachable, its pods Ready) graded Contradiction
			// and the planner HELD. Under D1 there is no witness: the fence proves the lost
			// side has itself fail-stopped (a master minority), so the takeover is provably
			// split-brain-free and FIRES.
			name: "isolated-alive MINORITY (even split), survivors fail, quiet, past cooldown => Takeover (D1: was Hold-on-Contradiction)",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          2, reachableMasters: 1, orphanTargets: oneOrphan(),
				survivorsClusterFail: true, quiet: true,
			},
			since: &armed, wantAction: siteTakeover, wantTargets: 1,
		},
		{
			name: "not quiet => Hold (condition 5)",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          2, reachableMasters: 1, orphanTargets: oneOrphan(),
				survivorsClusterFail: true, quiet: false,
			},
			since: &armed, wantAction: siteTakeoverHold, wantTargets: -1,
		},
		{
			name: "first observation => Arm (condition 4, not fire)",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          2, reachableMasters: 1, orphanTargets: oneOrphan(),
				survivorsClusterFail: true, quiet: true,
			},
			since: nil, wantAction: siteTakeoverArm, wantTargets: -1,
		},
		{
			name: "within cooldown => Wait (condition 4)",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          2, reachableMasters: 1, orphanTargets: oneOrphan(),
				survivorsClusterFail: true, quiet: true,
			},
			since: &fresh, wantAction: siteTakeoverWait, wantTargets: -1,
		},
		{
			name: "fence-safe even split past cooldown => Takeover with target",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          2, reachableMasters: 1, orphanTargets: oneOrphan(),
				survivorsClusterFail: true, quiet: true,
			},
			since: &armed, wantAction: siteTakeover, wantTargets: 1,
		},
		{
			name: "drift-majority unreachable, no opt-in => Refuse (fence unsafe)",
			in: siteTakeoverInput{
				// size 3, reachable 1 => unreachable 2 >= majority 2: the lost side could hold
				// the master majority, so an isolated-but-alive site may still be serving.
				clusterFormed: true,
				size:          3, reachableMasters: 1, orphanTargets: oneOrphan(),
				survivorsClusterFail: true, quiet: true, allowUnsafe: false,
			},
			since: &armed, wantAction: siteTakeoverRefuse, wantTargets: -1,
		},
		{
			name: "drift-majority unreachable, WITH opt-in => TakeoverUnsafe",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          3, reachableMasters: 1, orphanTargets: oneOrphan(),
				survivorsClusterFail: true, quiet: true, allowUnsafe: true,
			},
			since: &armed, wantAction: siteTakeoverUnsafe, wantTargets: 1,
		},
		{
			name: "multi-orphaned-shard even split past cooldown => Takeover with both targets",
			in: siteTakeoverInput{
				clusterFormed: true,
				size:          4, reachableMasters: 2, orphanTargets: []orphanTarget{
					{shard: 2, key: "alpha/inst-shard-2-1", ip: "10.0.0.7", offset: 50, replid: "L2"},
					{shard: 3, key: "alpha/inst-shard-3-1", ip: "10.0.0.8", offset: 70, replid: "L3"},
				},
				survivorsClusterFail: true, quiet: true,
			},
			since: &armed, wantAction: siteTakeover, wantTargets: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var since *time.Time
			if tc.since != nil {
				s := *tc.since
				since = &s
			}
			got := planSiteTakeover(tc.in, since, now, testTakeoverCooldown)
			if got.action != tc.wantAction {
				t.Fatalf("action = %s, want %s", got.action, tc.wantAction)
			}
			if tc.wantTargets >= 0 && len(got.targets) != tc.wantTargets {
				t.Fatalf("targets = %d, want %d", len(got.targets), tc.wantTargets)
			}
		})
	}
}

// TestPlanSiteTakeover_CooldownClearsWhenStateClears locks that the actions that carry no
// cooldown (None / WaitNative / Hold) are exactly the ones the executor uses to CLEAR the
// marker — i.e. the marker is only ever held by Arm/Wait/Refuse/fire.
func TestPlanSiteTakeover_CooldownClearsWhenStateClears(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	armed := now.Add(-2 * testTakeoverCooldown)

	// State cleared: survivors regained a majority (native path back).
	in := siteTakeoverInput{
		clusterFormed: true,
		size:          3, reachableMasters: 2, orphanTargets: oneOrphan(),
		survivorsClusterFail: false, quiet: true,
	}
	got := planSiteTakeover(in, &armed, now, testTakeoverCooldown)
	if got.action != siteTakeoverWaitNative {
		t.Fatalf("recovered state should WaitNative (clear marker), got %s", got.action)
	}
}

// TestPickShardSurvivor is the red-first table for per-shard target selection: highest
// offset within the dominant replication lineage (LR-024 primitives).
func TestPickShardSurvivor(t *testing.T) {
	t.Run("no candidates => not found", func(t *testing.T) {
		if _, ok := pickShardSurvivor(1, nil); ok {
			t.Fatalf("expected no survivor for an empty candidate set")
		}
	})

	t.Run("single candidate => that one", func(t *testing.T) {
		got, ok := pickShardSurvivor(1, []orphanCandidate{{key: "a", offset: 5, replid: "X"}})
		if !ok || got.key != "a" {
			t.Fatalf("got (%+v, %v), want key a", got, ok)
		}
	})

	t.Run("same lineage => highest offset wins", func(t *testing.T) {
		got, ok := pickShardSurvivor(1, []orphanCandidate{
			{key: "a", offset: 10, replid: "X"},
			{key: "b", offset: 42, replid: "X"},
		})
		if !ok || got.key != "b" {
			t.Fatalf("got (%+v, %v), want key b (offset 42)", got, ok)
		}
	})

	t.Run("promotion chain (replid2 connects lineage) => highest offset", func(t *testing.T) {
		// b was promoted: its replid rotated to Y and the old X moved to replid2, so a and b
		// are ONE lineage. Keyed on replid alone they would look divergent.
		got, ok := pickShardSurvivor(1, []orphanCandidate{
			{key: "a", offset: 10, replid: "X"},
			{key: "b", offset: 42, replid: "Y", replid2: "X"},
		})
		if !ok || got.key != "b" {
			t.Fatalf("got (%+v, %v), want key b (same lineage, offset 42)", got, ok)
		}
	})

	t.Run("divergent outlier ignored: pick highest offset in the DOMINANT lineage", func(t *testing.T) {
		// a is a lone divergent lineage with the highest offset; b and c share the dominant
		// lineage. Offsets are not comparable across lineages, so the dominant lineage's best
		// (c) is chosen, not the higher-offset outlier a.
		got, ok := pickShardSurvivor(1, []orphanCandidate{
			{key: "a", offset: 100, replid: "X"},
			{key: "b", offset: 50, replid: "Y"},
			{key: "c", offset: 60, replid: "Y"},
		})
		if !ok || got.key != "c" {
			t.Fatalf("got (%+v, %v), want key c (dominant lineage Y, offset 60)", got, ok)
		}
	})
}

// TestPlanFencedRejoinRecycle is the red-first table for the fenced-alive rejoin recycle
// signal (ADR-012 action 3): recycle isolated, never-restarted pods only once the serving
// cluster already holds a master majority.
func TestPlanFencedRejoinRecycle(t *testing.T) {
	isolatedFresh := recycleCandidate{key: "beta/inst-shard-0-0", restartCount: 0, isolated: true}
	isolatedRestarted := recycleCandidate{key: "beta/inst-shard-1-0", restartCount: 3, isolated: true}
	joinedFresh := recycleCandidate{key: "alpha/inst-shard-0-1", restartCount: 0, isolated: false}

	t.Run("no majority => never recycle (bootstrap/takeover in progress)", func(t *testing.T) {
		if got := planFencedRejoinRecycle([]recycleCandidate{isolatedFresh}, false); len(got) != 0 {
			t.Fatalf("expected no recycle without a serving majority, got %v", got)
		}
	})

	t.Run("isolated + never-restarted + majority => recycle", func(t *testing.T) {
		got := planFencedRejoinRecycle([]recycleCandidate{isolatedFresh, joinedFresh}, true)
		if len(got) != 1 || got[0] != isolatedFresh.key {
			t.Fatalf("expected to recycle only %s, got %v", isolatedFresh.key, got)
		}
	})

	t.Run("isolated but restarted => not recycled (crash/reschedule path, not a returnee)", func(t *testing.T) {
		if got := planFencedRejoinRecycle([]recycleCandidate{isolatedRestarted}, true); len(got) != 0 {
			t.Fatalf("expected no recycle for a restarted pod, got %v", got)
		}
	})

	t.Run("joined pod => not recycled", func(t *testing.T) {
		if got := planFencedRejoinRecycle([]recycleCandidate{joinedFresh}, true); len(got) != 0 {
			t.Fatalf("expected no recycle for a joined pod, got %v", got)
		}
	})
}
