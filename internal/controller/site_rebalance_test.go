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
	"context"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	climultisite "github.com/littlered-operator/littlered-operator/internal/cli/multisite"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// TestBalanceArithmeticDriftGuard pins the operator's balance arithmetic against lrctl's
// render-side copy (internal/cli/multisite): the CLI re-implements masterBalanceTarget /
// siteLossResilient because the controller symbols are unexported and its package is too
// heavy to import into the CLI. This test asserts the two definitions never drift, so the
// operator's published condition and lrctl's rendered verdict always agree.
func TestBalanceArithmeticDriftGuard(t *testing.T) {
	for k := 1; k <= 20; k++ {
		if got, want := climultisite.NativeSurvivalTarget(k), nativeSurvivalTarget(k); got != want {
			t.Errorf("NativeSurvivalTarget drift at shards=%d: cli=%d controller=%d", k, got, want)
		}
		if got, want := climultisite.TakeoverSurvivalFloor(k), takeoverSurvivalFloor(k); got != want {
			t.Errorf("TakeoverSurvivalFloor drift at shards=%d: cli=%d controller=%d", k, got, want)
		}
		for s := 1; s <= 8; s++ {
			if got, want := climultisite.SiteLossResilient(k, s), siteLossResilient(k, s); got != want {
				t.Errorf("SiteLossResilient drift at shards=%d sites=%d: cli=%v controller=%v", k, s, got, want)
			}
		}
	}
}

// TestBalanceThresholds locks the two ADR-012 D2 thresholds. The NATIVE TARGET ceil(K/2)-1 is
// the bound for the survivors keeping quorum (Redis elects on its own); the TAKEOVER FLOOR
// floor(K/2) is the bound for the LOST side lacking quorum (the survivors hold a blocking
// minority, so planSiteTakeover is fence-safe). They coincide for odd K and differ by one for
// even K — see site_balance_property_test.go, which binds both to the planner's real decisions.
func TestBalanceThresholds(t *testing.T) {
	cases := []struct{ shards, target, floor int }{
		{shards: 1, target: 0, floor: 0},
		{shards: 2, target: 0, floor: 1}, // moot: Redis Cluster's 3-master minimum rejects K=2
		{shards: 3, target: 1, floor: 1},
		{shards: 4, target: 1, floor: 2}, // the D3-recommended dual-site 2+2 sits AT the floor
		{shards: 5, target: 2, floor: 2},
		{shards: 6, target: 2, floor: 3},
		{shards: 7, target: 3, floor: 3},
		{shards: 8, target: 3, floor: 4},
	}
	for _, c := range cases {
		if got := nativeSurvivalTarget(c.shards); got != c.target {
			t.Errorf("nativeSurvivalTarget(%d) = %d, want %d", c.shards, got, c.target)
		}
		if got := takeoverSurvivalFloor(c.shards); got != c.floor {
			t.Errorf("takeoverSurvivalFloor(%d) = %d, want %d", c.shards, got, c.floor)
		}
	}
}

// TestSiteLossResilient locks the D2 structural resilience predicate (ADR-012 "Reduced-
// resilience warning"): a topology of K shards over S sites can auto-survive an arbitrary
// single-site loss iff S·takeoverSurvivalFloor(K) >= K — the FLOOR is reachable, so no site is
// forced over it by pigeonhole. Given K >= 3 that is false in exactly one regime: two sites
// with an odd K.
func TestSiteLossResilient(t *testing.T) {
	cases := []struct {
		shards, sites int
		want          bool
	}{
		{shards: 3, sites: 3, want: true},  // 3*1=3 >= 3
		{shards: 3, sites: 2, want: false}, // 2*1=2 < 3  — odd K over two sites, the whole warned regime
		{shards: 5, sites: 2, want: false}, // 2*2=4 < 5
		{shards: 4, sites: 2, want: true},  // 2*2=4 >= 4 — the D3-recommended dual-site topology
		{shards: 4, sites: 3, want: true},  // 3*2=6 >= 4 — LR-031: the former "headline trap" is resilient
		{shards: 6, sites: 2, want: true},  // 2*3=6 >= 6
		{shards: 5, sites: 3, want: true},  // 3*2=6 >= 5
	}
	for _, c := range cases {
		if got := siteLossResilient(c.shards, c.sites); got != c.want {
			t.Errorf("siteLossResilient(shards=%d, sites=%d) = %v, want %v", c.shards, c.sites, got, c.want)
		}
	}
}

// TestPlanMasterRebalance is the red-first table for the D2 master-balance seam: it returns
// the minimal, deterministic list of {shard, toSite} graceful promotions that restores the
// invariant max_i m_i <= ceil(K/2)-1, or nil when already satisfied or unsatisfiable.
func TestPlanMasterRebalance(t *testing.T) {
	cases := []struct {
		name string
		in   masterRebalanceInput
		want []masterMove
	}{
		{
			name: "empty input => nil",
			in:   masterRebalanceInput{},
			want: nil,
		},
		{
			name: "K=4 over 4 sites, already balanced (1,1,1,1) => nil",
			in: masterRebalanceInput{
				masterSite:   []string{"A", "B", "C", "D"},
				replicaSites: [][]string{{"B"}, {"C"}, {"D"}, {"A"}},
			},
			want: nil,
		},
		{
			name: "K=5 over 3 sites, counts (2,2,1) all <= target 2 => nil",
			in: masterRebalanceInput{
				masterSite:   []string{"A", "A", "B", "B", "C"},
				replicaSites: [][]string{{"B"}, {"C"}, {"A"}, {"C"}, {"A"}},
			},
			want: nil,
		},
		{
			name: "K=2 => nil (unsatisfiable: target 0, two masters must live somewhere)",
			in: masterRebalanceInput{
				masterSite:   []string{"A", "B"},
				replicaSites: [][]string{{"B"}, {"A"}},
			},
			want: nil,
		},
		{
			name: "K=3 over 2 sites => nil (odd K on two sites: BOTH thresholds are 1 and neither is reachable)",
			in: masterRebalanceInput{
				masterSite:   []string{"A", "B", "A"},
				replicaSites: [][]string{{"B"}, {"A"}, {"B"}},
			},
			want: nil,
		},
		{
			name: "K=4 over 3 sites, 2+1+1 => nil (target 1 unreachable, but already AT the floor 2)",
			in: masterRebalanceInput{
				// LR-031: this is the former "headline trap". 2+1+1 satisfies the floor, so
				// losing any single site is recoverable — nothing to correct.
				masterSite:   []string{"A", "B", "C", "A"},
				replicaSites: [][]string{{"B", "C"}, {"A"}, {"A"}, {"B", "C"}},
			},
			want: nil,
		},
		{
			name: "K=4 over 2 sites, drift 3+1 => one move to the floor (2+2)",
			in: masterRebalanceInput{
				// LR-031 regression: the native target (1) is unreachable for K=4 over 2 sites, so
				// the old single-threshold planner returned nil and left 3+1 standing — and losing
				// the 3-master site then REFUSES. The floor (2) is reachable, and 2+2 is exactly
				// the D3-recommended fence-safe placement.
				masterSite:   []string{"A", "A", "A", "B"},
				replicaSites: [][]string{{"B"}, {"B"}, {"B"}, {"A"}},
			},
			want: []masterMove{{shard: 0, toSite: "B"}},
		},
		{
			name: "K=4 over 3 sites, drift 3+1+0 => falls back to the floor rather than discarding progress",
			in: masterRebalanceInput{
				// The target (1) is unreachable: one move gets to 2+1+1 and then sticks. The old
				// planner threw that move away as "partial churn" and left a 3-master site — over
				// the floor, so its loss refused. The floor tier reaches 2+1+1 in one move.
				masterSite:   []string{"A", "A", "A", "B"},
				replicaSites: [][]string{{"B", "C"}, {"B"}, {"C"}, {"A"}},
			},
			want: []masterMove{{shard: 0, toSite: "C"}},
		},
		{
			name: "K=4 over 4 sites, drift (2,1,1,0) with a replica in the empty site => one move",
			in: masterRebalanceInput{
				// A=2 (shards 0,3), B=1, C=1, D=0. target 1. shard0 has a replica in D.
				masterSite:   []string{"A", "B", "C", "A"},
				replicaSites: [][]string{{"B", "D"}, {"C"}, {"D"}, {"C"}},
			},
			want: []masterMove{{shard: 0, toSite: "D"}},
		},
		{
			name: "K=6 over 3 sites, drift (4,1,1) => two deterministic moves onto B then C",
			in: masterRebalanceInput{
				// A=4 (shards 0,1,2,3), B=1 (shard 4), C=1 (shard 5). target 2.
				masterSite:   []string{"A", "A", "A", "A", "B", "C"},
				replicaSites: [][]string{{"B", "C"}, {"B", "C"}, {"A"}, {"A"}, {"C"}, {"B"}},
			},
			// shard0 -> B (lowest count tie broken by name), then shard1 -> C (B now full).
			want: []masterMove{{shard: 0, toSite: "B"}, {shard: 1, toSite: "C"}},
		},
		{
			name: "drift but the donor's shards have no replica in a receivable site => nil",
			in: masterRebalanceInput{
				// A=2 (shards 0,3), B=1,C=1,D=0. target 1. But shards 0 and 3 only replicate to
				// B and C, both already at target (cannot receive). D is empty but has no replica.
				masterSite:   []string{"A", "B", "C", "A"},
				replicaSites: [][]string{{"B", "C"}, {"D"}, {"D"}, {"B", "C"}},
			},
			want: nil,
		},
		{
			name: "never target a site lacking a promotable replica: pick the shard that CAN move",
			in: masterRebalanceInput{
				// A=2 (shards 0,3). shard0 can only go to B/C (full); shard3 can go to D (empty).
				masterSite:   []string{"A", "B", "C", "A"},
				replicaSites: [][]string{{"B", "C"}, {"C"}, {"D"}, {"D"}},
			},
			want: []masterMove{{shard: 3, toSite: "D"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planMasterRebalance(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("planMasterRebalance() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// rbNode is a compact constructor for a gathered cluster node used by the input-builder test.
func rbNode(key, role string, slots []string, reachable bool, link string) *redisclient.ClusterNodeState {
	return &redisclient.ClusterNodeState{
		PodName:    key, // multi-site ground truth is keyed by site-qualified pod name
		PodIP:      "10.0." + key,
		Role:       role,
		Slots:      slots,
		Reachable:  reachable,
		LinkStatus: link,
	}
}

// TestBuildMasterRebalanceInput is the red-first assertion for the pure input-builder that
// projects the gather onto the D2 rebalance planner input: per shard, the site of its reachable
// slot-serving master and the sites of its reachable, IN-SYNC (link:up) promotable replicas.
// It models the "K=4 over 4 sites, drift (2,1,1,0)" case (masters A,B,C,A; a promotable replica
// of shard0 in the empty site D) so the derived input feeds planMasterRebalance -> [{0,D}], plus
// the exclusion rules (a link:down replica and an unreachable replica are NOT promotable).
func TestBuildMasterRebalanceInput(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	slots := []string{"0-4095"}
	add := func(n *redisclient.ClusterNodeState) { gt.Nodes[n.PodName] = n }

	// Masters: A holds shard0 and shard3, B holds shard1, C holds shard2 (counts 2,1,1,0).
	add(rbNode("A/cache-shard-0-0", RoleMaster, slots, true, "up"))
	add(rbNode("A/cache-shard-3-0", RoleMaster, slots, true, "up"))
	add(rbNode("B/cache-shard-1-0", RoleMaster, slots, true, "up"))
	add(rbNode("C/cache-shard-2-0", RoleMaster, slots, true, "up"))

	// In-sync promotable replicas: shard0 in B and D; shard1 in C; shard2 in D; shard3 in C.
	add(rbNode("B/cache-shard-0-1", RoleReplica, nil, true, "up"))
	add(rbNode("D/cache-shard-0-2", RoleReplica, nil, true, "up"))
	add(rbNode("C/cache-shard-1-1", RoleReplica, nil, true, "up"))
	add(rbNode("D/cache-shard-2-1", RoleReplica, nil, true, "up"))
	add(rbNode("C/cache-shard-3-1", RoleReplica, nil, true, "up"))

	// NOT promotable: a link:down replica of shard1 in E, and an unreachable replica of shard2 in E.
	add(rbNode("E/cache-shard-1-2", RoleReplica, nil, true, "down"))
	add(rbNode("E/cache-shard-2-2", RoleReplica, nil, false, "up"))

	in := buildMasterRebalanceInput(gt, 4)

	wantMasterSite := []string{"A", "B", "C", "A"}
	if !reflect.DeepEqual(in.masterSite, wantMasterSite) {
		t.Fatalf("masterSite = %v, want %v", in.masterSite, wantMasterSite)
	}

	// replicaSites are compared as sorted sets per shard (builder order is not contractual).
	wantReplicaSites := [][]string{{"B", "D"}, {"C"}, {"D"}, {"C"}}
	if len(in.replicaSites) != len(wantReplicaSites) {
		t.Fatalf("replicaSites len = %d, want %d (%v)", len(in.replicaSites), len(wantReplicaSites), in.replicaSites)
	}
	for k := range wantReplicaSites {
		got := append([]string(nil), in.replicaSites[k]...)
		sort.Strings(got)
		if !reflect.DeepEqual(got, wantReplicaSites[k]) {
			t.Fatalf("replicaSites[%d] = %v, want %v (link:down/unreachable replicas must be excluded)", k, got, wantReplicaSites[k])
		}
	}

	// The built input must feed the planner to the same single move the pure table asserts.
	if moves := planMasterRebalance(in); !reflect.DeepEqual(moves, []masterMove{{shard: 0, toSite: "D"}}) {
		t.Fatalf("planMasterRebalance(buildMasterRebalanceInput(...)) = %+v, want [{0 D}]", moves)
	}
}

// balancedGT builds a K-shard stretched-cluster gather with exactly one reachable slot-serving
// master per shard, each in its own site (counts all 1 -> invariant holds, no moves).
func balancedGT(shards int) *redisclient.ClusterGroundTruth {
	gt := redisclient.NewClusterGroundTruth()
	for k := range shards {
		key := fmtSiteShard(k, k, 0) // site k hosts shard k master
		gt.Nodes[key] = rbNode(key, RoleMaster, []string{"0-1"}, true, "up")
	}
	return gt
}

// driftGT builds a K=4 gather with drift (2,1,1,0): site0 masters shards 0 and 3; site1 shard1;
// site2 shard2; and shard0 has an in-sync replica in the empty site3 (a promotable move target).
func driftGT() *redisclient.ClusterGroundTruth {
	gt := redisclient.NewClusterGroundTruth()
	m := func(site, shard int) {
		key := fmtSiteShard(site, shard, 0)
		gt.Nodes[key] = rbNode(key, RoleMaster, []string{"0-1"}, true, "up")
	}
	rep := func(site, shard, ord int) {
		key := fmtSiteShard(site, shard, ord)
		gt.Nodes[key] = rbNode(key, RoleReplica, nil, true, "up")
	}
	m(0, 0)
	m(0, 3)
	m(1, 1)
	m(2, 2)
	rep(3, 0, 1) // shard0 replica in the empty site3 -> planner move {0, site3}
	return gt
}

func fmtSiteShard(site, shard, ord int) string {
	return "site" + strconv.Itoa(site) + "/cache-shard-" + strconv.Itoa(shard) + "-" + strconv.Itoa(ord)
}

// TestRebalanceMastersGating exercises the executor's non-I/O gates — the paths that return
// BEFORE any Redis dial. The fire path (drift past cooldown -> ClusterFailover) dials a real pod
// and is left to e2e (untested plumbing, disclosed). Here: quiet-veto holds, a balanced cluster
// clears the marker, and drift within the post-move cooldown neither re-evaluates nor re-issues.
func TestRebalanceMastersGating(t *testing.T) {
	t.Run("terminating survivor pod vetoes rebalance (quiet gate)", func(t *testing.T) {
		lr := multiSiteClusterCR(4, 2, 4, 1)
		r := &LittleRedReconciler{Client: newStatusFakeClient(t, lr)}
		obs := []siteObservation{{
			site: resolvedSite{name: siteName0}, reachable: true,
			pods: []sitePodObservation{{key: keySite0Shard00, terminating: true}},
		}}
		acted, err := r.rebalanceMasters(context.Background(), lr, driftGT(), obs)
		if err != nil || acted {
			t.Fatalf("quiet-veto: acted=%v err=%v, want acted=false err=nil", acted, err)
		}
	})

	t.Run("balanced cluster clears the pending marker", func(t *testing.T) {
		lr := multiSiteClusterCR(4, 2, 4, 1)
		now := metav1.Now()
		lr.Status.MultiSite = &littleredv1alpha1.MultiSiteStatus{RebalancePendingSince: &now}
		r := &LittleRedReconciler{Client: newStatusFakeClient(t, lr)}
		acted, err := r.rebalanceMasters(context.Background(), lr, balancedGT(4), nil)
		if err != nil || acted {
			t.Fatalf("balanced: acted=%v err=%v, want acted=false err=nil", acted, err)
		}
		if lr.Status.MultiSite.RebalancePendingSince != nil {
			t.Fatalf("balanced cluster must clear RebalancePendingSince, got %v", lr.Status.MultiSite.RebalancePendingSince)
		}
	})

	t.Run("drift within cooldown waits, does not re-issue", func(t *testing.T) {
		lr := multiSiteClusterCR(4, 2, 4, 1)
		recent := metav1.NewTime(time.Now().Add(-masterRebalanceCooldown / 2))
		lr.Status.MultiSite = &littleredv1alpha1.MultiSiteStatus{RebalancePendingSince: &recent}
		r := &LittleRedReconciler{Client: newStatusFakeClient(t, lr)}
		acted, err := r.rebalanceMasters(context.Background(), lr, driftGT(), nil)
		if err != nil || acted {
			t.Fatalf("cooldown-wait: acted=%v err=%v, want acted=false err=nil", acted, err)
		}
		// Marker must be untouched (still set) — no dial, no re-stamp/clear.
		if lr.Status.MultiSite.RebalancePendingSince == nil {
			t.Fatalf("cooldown-wait must leave the pending marker set")
		}
	})
}
