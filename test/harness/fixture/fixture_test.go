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

package fixture

import (
	"slices"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// The matrix §3 table, transcribed. Every column that is DERIVED (pods, the
// initial master split, q) is asserted against the arithmetic rather than
// re-stated in the implementation, so a slip in either one shows up here.
//
// This is the whole point of a fixture seam: an e2e that stands up the wrong
// topology is not a failing test, it is a test that measured something else.
// ---------------------------------------------------------------------------

func TestMatrixSection3Table(t *testing.T) {
	cases := []struct {
		name        string
		sites       int
		shards      int
		sps         int
		rps         int
		pods        int
		masterSplit []int
		quorum      int
	}{
		{F2K4, 2, 4, 2, 0, 8, []int{2, 2}, 3},
		{F2K4R, 2, 4, 2, 1, 16, []int{2, 2}, 3},
		{F2K3, 2, 3, 2, 0, 6, []int{2, 1}, 2},
		{F2K6, 2, 6, 2, 0, 12, []int{3, 3}, 4},
		{F3K3, 3, 3, 2, 0, 6, []int{1, 1, 1}, 2},
		{F3K4, 3, 4, 2, 0, 8, []int{2, 1, 1}, 3},
		{F3K6, 3, 6, 2, 0, 12, []int{2, 2, 2}, 4},
		{F3K3M, 3, 3, 3, 0, 9, []int{1, 1, 1}, 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Lookup(tc.name)
			if err != nil {
				t.Fatalf("Lookup(%q): %v", tc.name, err)
			}
			if f.Sites != tc.sites {
				t.Errorf("Sites = %d, matrix §3 says %d", f.Sites, tc.sites)
			}
			if f.Shards != tc.shards {
				t.Errorf("Shards = %d, matrix §3 says %d", f.Shards, tc.shards)
			}
			if f.SitesPerShard != tc.sps {
				t.Errorf("SitesPerShard = %d, matrix §3 says %d", f.SitesPerShard, tc.sps)
			}
			if f.ReplicasPerShard != tc.rps {
				t.Errorf("ReplicasPerShard = %d, matrix §3 says %d", f.ReplicasPerShard, tc.rps)
			}
			if got := f.Pods(); got != tc.pods {
				t.Errorf("Pods() = %d, matrix §3 says %d", got, tc.pods)
			}
			if got := f.MasterSplit(); !slices.Equal(got, tc.masterSplit) {
				t.Errorf("MasterSplit() = %v, matrix §3 says %v", got, tc.masterSplit)
			}
			if got := f.Quorum(); got != tc.quorum {
				t.Errorf("Quorum() = %d, matrix §3 says %d", got, tc.quorum)
			}
			if err := f.Validate(); err != nil {
				t.Errorf("Validate() = %v, want nil for a shipped fixture", err)
			}
		})
	}
}

// The two LR-031 thresholds. They coincide for odd K and differ by exactly one
// for even K — the off-by-one that LR-031 corrected. Asserting both here keeps
// the fixture seam honest about which fixtures live ON the floor.
func TestLR031Thresholds(t *testing.T) {
	cases := []struct {
		name         string
		floor        int // takeoverSurvivalFloor(K) = floor(K/2)
		nativeTarget int // nativeSurvivalTarget(K) = ceil(K/2) - 1
	}{
		{F2K3, 1, 1},
		{F2K4, 2, 1},
		{F2K6, 3, 2},
		{F3K3, 1, 1},
		{F3K4, 2, 1},
		{F3K6, 3, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Lookup(tc.name)
			if err != nil {
				t.Fatalf("Lookup: %v", err)
			}
			if got := f.TakeoverSurvivalFloor(); got != tc.floor {
				t.Errorf("TakeoverSurvivalFloor() = %d, want %d", got, tc.floor)
			}
			if got := f.NativeSurvivalTarget(); got != tc.nativeTarget {
				t.Errorf("NativeSurvivalTarget() = %d, want %d", got, tc.nativeTarget)
			}
		})
	}
}

// F2-K4's initial split sits exactly ON the takeover floor in both sites: that
// IS the property that makes it "the recommended dual-site" (matrix §3, ADR-012
// D3). If the fixture ever drifted off it, the primary fixture would silently
// stop being fence-safe.
func TestF2K4IsFenceSafeInBothDirections(t *testing.T) {
	f, err := Lookup(F2K4)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	for site, masters := range f.MasterSplit() {
		if masters > f.TakeoverSurvivalFloor() {
			t.Errorf("site %d holds %d masters, above the takeover floor %d — "+
				"losing it would hit the drift-refusal, not the automated takeover",
				site, masters, f.TakeoverSurvivalFloor())
		}
	}
}

func TestLookupRejectsUnknown(t *testing.T) {
	_, err := Lookup("F9-K99")
	if err == nil {
		t.Fatal("Lookup of an unknown fixture must fail: a typo in a work list would " +
			"otherwise stand up a silently different topology")
	}
	// The error must name the alternatives, or the refusal just moves the dead end.
	for _, want := range Names() {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not list the known fixture %q", err, want)
		}
	}
}

func TestNamesCoversTheRegistry(t *testing.T) {
	names := Names()
	if len(names) < 8 {
		t.Fatalf("Names() returned %d fixtures, matrix §3 defines 8", len(names))
	}
	if !slices.IsSorted(names) {
		t.Errorf("Names() = %v, want sorted (it is user-facing output)", names)
	}
	for _, n := range names {
		if _, err := Lookup(n); err != nil {
			t.Errorf("Names() lists %q but Lookup rejects it: %v", n, err)
		}
	}
}

// Every shipped fixture must be frugal (matrix §3: "sized frugally, 100m/128Mi,
// so several non-destructive fixtures coexist"). A fixture that quietly asked
// for the operator default would make an array uneconomic to repeat.
func TestShippedFixturesAreFrugal(t *testing.T) {
	for _, n := range Names() {
		f, err := Lookup(n)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", n, err)
		}
		if f.CPU != "100m" || f.Memory != "128Mi" {
			t.Errorf("%s: cpu/memory = %s/%s, matrix §3 sizes fixtures at 100m/128Mi",
				n, f.CPU, f.Memory)
		}
	}
}

func TestValidateRejectsIncoherentTopologies(t *testing.T) {
	cases := []struct {
		name    string
		f       Fixture
		wantErr string
	}{
		{
			name:    "sitesPerShard above the site count",
			f:       Fixture{Name: "x", Sites: 2, Shards: 3, SitesPerShard: 3},
			wantErr: "sitesPerShard",
		},
		{
			name:    "sitesPerShard below 2 is not multi-site",
			f:       Fixture{Name: "x", Sites: 2, Shards: 3, SitesPerShard: 1},
			wantErr: "sitesPerShard",
		},
		{
			name:    "fewer than 3 shards is not a Redis Cluster",
			f:       Fixture{Name: "x", Sites: 2, Shards: 2, SitesPerShard: 2},
			wantErr: "shards",
		},
		{
			name:    "a single site is not multi-site",
			f:       Fixture{Name: "x", Sites: 1, Shards: 3, SitesPerShard: 2},
			wantErr: "sites",
		},
		{
			name:    "negative replicas",
			f:       Fixture{Name: "x", Sites: 2, Shards: 3, SitesPerShard: 2, ReplicasPerShard: -1},
			wantErr: "replicasPerShard",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.f.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Validate() = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

// Env is what actually reaches the e2e suite. Getting a key name wrong here is
// invisible at runtime — the suite falls back to its default and stands up a
// DIFFERENT topology that still passes.
func TestEnvCarriesTheWholeTopology(t *testing.T) {
	f, err := Lookup(F2K4)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	env := f.Env()
	want := map[string]string{
		"E2E_TOPOLOGY":          "multi",
		"CLUSTER_SHARDS":        "4",
		"MS_SITES_PER_SHARD":    "2",
		"MS_REPLICAS_PER_SHARD": "0",
		"MS_REDIS_CPU":          "100m",
		"MS_REDIS_MEMORY":       "128Mi",
	}
	for k, v := range want {
		if got := env[k]; got != v {
			t.Errorf("Env()[%q] = %q, want %q", k, got, v)
		}
	}
	if len(env) != len(want) {
		t.Errorf("Env() has %d keys %v, want exactly %d — an unexpected key is a knob "+
			"nobody documented", len(env), env, len(want))
	}
}

// The suite refuses CLUSTER_SHARDS < 3 (e2e_suite_test.go). A fixture that
// emitted a smaller K would fail in BeforeSuite of every item, minutes apart.
func TestEveryFixtureSatisfiesTheSuiteShardFloor(t *testing.T) {
	for _, n := range Names() {
		f, _ := Lookup(n)
		if f.Shards < 3 {
			t.Errorf("%s: CLUSTER_SHARDS=%d, but the e2e suite requires >= 3", n, f.Shards)
		}
	}
}
