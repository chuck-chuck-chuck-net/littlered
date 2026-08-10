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
	"reflect"
	"testing"
)

// TestMasterBalanceTarget locks the D2 invariant threshold ceil(K/2)-1 (the maximum number
// of slot-serving masters any single site may hold so that NO site holds a Redis master
// majority (K/2)+1). It is the arithmetic the rebalancer restores.
func TestMasterBalanceTarget(t *testing.T) {
	cases := map[int]int{
		1: 0, // ceil(0.5)-1 = 1-1 = 0
		2: 0, // ceil(1)-1   = 1-1 = 0  (dual-site is unsatisfiable: 2 masters, target 0)
		3: 1, // ceil(1.5)-1 = 2-1 = 1
		4: 1, // ceil(2)-1   = 2-1 = 1
		5: 2, // ceil(2.5)-1 = 3-1 = 2
		6: 2, // ceil(3)-1   = 3-1 = 2
		7: 3,
	}
	for k, want := range cases {
		if got := masterBalanceTarget(k); got != want {
			t.Errorf("masterBalanceTarget(%d) = %d, want %d", k, got, want)
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
			name: "K=3 over 2 sites => nil (unsatisfiable: 3 masters, 2 sites, target 1)",
			in: masterRebalanceInput{
				masterSite:   []string{"A", "B", "A"},
				replicaSites: [][]string{{"B"}, {"A"}, {"B"}},
			},
			want: nil,
		},
		{
			name: "K=4 over 3 sites => nil (unsatisfiable: 4 masters, 3 sites, target 1)",
			in: masterRebalanceInput{
				// counts A=2,B=1,C=1; even with a replica in a fuller site, capacity < excess.
				masterSite:   []string{"A", "B", "C", "A"},
				replicaSites: [][]string{{"B", "C"}, {"A"}, {"A"}, {"B", "C"}},
			},
			want: nil,
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
