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

package watchscope_test

import (
	"reflect"
	"testing"

	"github.com/chuck-chuck-chuck-net/littlered/internal/watchscope"
)

const (
	nsTeamA   = "team-a"
	nsStaging = "staging"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name            string
		watch           string
		ignore          string
		wantErr         bool
		wantMode        watchscope.Mode
		wantNS          []string
		wantLeaseIsBase bool // true => LeaderElectionID must equal BaseLeaderElectionID
	}{
		{
			name:            "neither set is cluster-scoped with base lease",
			watch:           "",
			ignore:          "",
			wantMode:        watchscope.ModeNone,
			wantNS:          nil,
			wantLeaseIsBase: true,
		},
		{
			name:    "both set is a fatal error",
			watch:   "a",
			ignore:  "b",
			wantErr: true,
		},
		{
			name:     "allow single namespace",
			watch:    nsTeamA,
			wantMode: watchscope.ModeAllow,
			wantNS:   []string{nsTeamA},
		},
		{
			name:     "allow multi namespace sorted and deduped",
			watch:    "team-b, team-a ,team-b,,team-c",
			wantMode: watchscope.ModeAllow,
			wantNS:   []string{nsTeamA, "team-b", "team-c"},
		},
		{
			name:     "deny single namespace",
			ignore:   nsStaging,
			wantMode: watchscope.ModeDeny,
			wantNS:   []string{nsStaging},
		},
		{
			name:     "deny multi namespace sorted and deduped",
			ignore:   " prod ,staging,prod",
			wantMode: watchscope.ModeDeny,
			wantNS:   []string{"prod", nsStaging},
		},
		{
			name:            "whitespace-only collapses to none",
			watch:           "  ,  , ",
			wantMode:        watchscope.ModeNone,
			wantNS:          nil,
			wantLeaseIsBase: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := watchscope.Parse(tt.watch, tt.ignore)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Parse(%q,%q) = nil error, want error", tt.watch, tt.ignore)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q,%q) unexpected error: %v", tt.watch, tt.ignore, err)
			}
			if got.Mode != tt.wantMode {
				t.Errorf("Mode = %q, want %q", got.Mode, tt.wantMode)
			}
			if !reflect.DeepEqual(got.Namespaces, tt.wantNS) {
				t.Errorf("Namespaces = %#v, want %#v", got.Namespaces, tt.wantNS)
			}
			if got.LeaderElectionID == "" {
				t.Errorf("LeaderElectionID must never be empty")
			}
			if tt.wantLeaseIsBase && got.LeaderElectionID != watchscope.BaseLeaderElectionID {
				t.Errorf("LeaderElectionID = %q, want base %q", got.LeaderElectionID, watchscope.BaseLeaderElectionID)
			}
			if !tt.wantLeaseIsBase && got.LeaderElectionID == watchscope.BaseLeaderElectionID {
				t.Errorf("scoped LeaderElectionID must differ from base %q", watchscope.BaseLeaderElectionID)
			}
		})
	}
}

// TestLeaseIDDeterministic: same input yields the same lease ID across calls.
func TestLeaseIDDeterministic(t *testing.T) {
	a, err := watchscope.Parse("team-a,team-b", "")
	if err != nil {
		t.Fatal(err)
	}
	b, err := watchscope.Parse("team-b,team-a", "") // different order, same set
	if err != nil {
		t.Fatal(err)
	}
	if a.LeaderElectionID != b.LeaderElectionID {
		t.Errorf("lease ID not stable across input order: %q != %q", a.LeaderElectionID, b.LeaderElectionID)
	}
}

// TestLeaseIDDistinct: none, allow[a], and deny[a] must all yield distinct lease IDs.
func TestLeaseIDDistinct(t *testing.T) {
	none, _ := watchscope.Parse("", "")
	allowA, _ := watchscope.Parse("a", "")
	denyA, _ := watchscope.Parse("", "a")

	ids := map[string]string{
		"none":   none.LeaderElectionID,
		"allowA": allowA.LeaderElectionID,
		"denyA":  denyA.LeaderElectionID,
	}
	seen := map[string]string{}
	for name, id := range ids {
		if prev, dup := seen[id]; dup {
			t.Errorf("lease ID collision: %s and %s both = %q", prev, name, id)
		}
		seen[id] = name
	}
}

// TestCacheOptions_None: unscoped mode leaves the cache options unscoped.
func TestCacheOptions_None(t *testing.T) {
	cfg, _ := watchscope.Parse("", "")
	opts := cfg.CacheOptions()
	if opts.DefaultNamespaces != nil {
		t.Errorf("none mode must not set DefaultNamespaces, got %#v", opts.DefaultNamespaces)
	}
	if opts.DefaultFieldSelector != nil {
		t.Errorf("none mode must not set DefaultFieldSelector, got %v", opts.DefaultFieldSelector)
	}
}

// TestCacheOptions_Allow: allow mode maps to a DefaultNamespaces entry per namespace.
func TestCacheOptions_Allow(t *testing.T) {
	cfg, _ := watchscope.Parse("team-a,team-b", "")
	opts := cfg.CacheOptions()
	if opts.DefaultFieldSelector != nil {
		t.Errorf("allow mode must not set DefaultFieldSelector")
	}
	if len(opts.DefaultNamespaces) != 2 {
		t.Fatalf("DefaultNamespaces = %#v, want 2 entries", opts.DefaultNamespaces)
	}
	for _, ns := range []string{nsTeamA, "team-b"} {
		if _, ok := opts.DefaultNamespaces[ns]; !ok {
			t.Errorf("DefaultNamespaces missing %q", ns)
		}
	}
}

// TestCacheOptions_Deny: deny mode maps to an AND of metadata.namespace!=ns field selectors.
func TestCacheOptions_Deny(t *testing.T) {
	cfg, _ := watchscope.Parse("", "prod,staging")
	opts := cfg.CacheOptions()
	if opts.DefaultNamespaces != nil {
		t.Errorf("deny mode must not set DefaultNamespaces")
	}
	if opts.DefaultFieldSelector == nil {
		t.Fatal("deny mode must set DefaultFieldSelector")
	}
	got := opts.DefaultFieldSelector.String()
	want := "metadata.namespace!=prod,metadata.namespace!=staging"
	if got != want {
		t.Errorf("DefaultFieldSelector = %q, want %q", got, want)
	}
}
