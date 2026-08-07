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

package multisite

import (
	"reflect"
	"testing"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

func TestSiteQualifiedKeyRoundTrip(t *testing.T) {
	key := SiteQualifiedKey("alpha", "store-shard-0-0")
	if key != "alpha/store-shard-0-0" {
		t.Fatalf("SiteQualifiedKey = %q", key)
	}
	site, obj := SplitSiteKey(key)
	if site != "alpha" || obj != "store-shard-0-0" {
		t.Fatalf("SplitSiteKey(%q) = %q, %q", key, site, obj)
	}
	// A key with no site prefix is single-site / legacy.
	if s, o := SplitSiteKey("store-shard-0-0"); s != "" || o != "store-shard-0-0" {
		t.Fatalf("SplitSiteKey(no-slash) = %q, %q", s, o)
	}
}

func TestUnionClusterPods(t *testing.T) {
	sites := []SitePods{
		{Site: "alpha", Pods: []PodRef{{ObjName: "s-shard-0-0", IP: "10.0.0.1"}, {ObjName: "s-shard-1-0", IP: ""}}},
		{Site: "beta", Pods: []PodRef{{ObjName: "s-shard-0-0", IP: "10.1.0.1"}}},
	}
	got := UnionClusterPods(sites)
	want := map[string]string{
		"10.0.0.1": "alpha/s-shard-0-0",
		"10.1.0.1": "beta/s-shard-0-0",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UnionClusterPods = %v, want %v", got, want)
	}
	// Pod names collide across sites, but the IP->key direction disambiguates.
	if got["10.0.0.1"] == got["10.1.0.1"] {
		t.Fatalf("colliding pod names must map to distinct site-qualified keys")
	}
}

func TestParseSiteContextFlag(t *testing.T) {
	tests := []struct {
		in      string
		want    map[string]string
		wantErr bool
	}{
		{in: "", want: map[string]string{}},
		{in: "alpha=scm-s1,beta=scm-s2", want: map[string]string{"alpha": "scm-s1", "beta": "scm-s2"}},
		{in: " alpha = scm-s1 ", want: map[string]string{"alpha": "scm-s1"}},
		{in: "alpha", wantErr: true},
		{in: "alpha=", wantErr: true},
		{in: "=scm-s1", wantErr: true},
		{in: "alpha=scm-s1,alpha=scm-s2", wantErr: true},
	}
	for _, tt := range tests {
		got, err := ParseSiteContextFlag(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseSiteContextFlag(%q): want error, got %v", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSiteContextFlag(%q): unexpected error %v", tt.in, err)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("ParseSiteContextFlag(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestResolveSiteContexts(t *testing.T) {
	siteNames := []string{"scm-s1", "scm-s2", "gamma"}
	available := []string{"scm-cp", "scm-s1", "scm-s2", "ctxG"}

	// Convention (name==context) resolves s1/s2; gamma needs an explicit mapping.
	res := ResolveSiteContexts(siteNames, map[string]string{"gamma": "ctxG"}, available)
	want := map[string]string{"scm-s1": "scm-s1", "scm-s2": "scm-s2", "gamma": "ctxG"}
	if !reflect.DeepEqual(res.Resolved, want) {
		t.Fatalf("Resolved = %v, want %v", res.Resolved, want)
	}
	if len(res.Unresolved) != 0 {
		t.Fatalf("Unresolved = %v, want none", res.Unresolved)
	}

	// Unresolved: no mapping and no same-named context.
	res = ResolveSiteContexts([]string{"gamma"}, nil, available)
	if len(res.Unresolved) != 1 || res.Unresolved[0].Site != "gamma" {
		t.Fatalf("expected gamma unresolved, got %+v", res.Unresolved)
	}
	if _, ok := res.Resolved["gamma"]; ok {
		t.Fatalf("gamma must not be resolved")
	}

	// Explicit mapping to a non-existent context is an error (unresolved with reason).
	res = ResolveSiteContexts([]string{"scm-s1"}, map[string]string{"scm-s1": "nope"}, available)
	if len(res.Unresolved) != 1 || res.Unresolved[0].Site != "scm-s1" {
		t.Fatalf("expected scm-s1 unresolved for bad mapping, got %+v", res)
	}
}

func TestCheckSiteSpread(t *testing.T) {
	// Shard 0 spans two sites (good); shard 1 is entirely in one site (bad).
	members := []ShardMember{
		{Key: "scm-s1/x-shard-0-0", Site: "scm-s1", Shard: 0, Role: "master", Reachable: true},
		{Key: "scm-s2/x-shard-0-0", Site: "scm-s2", Shard: 0, Role: "replica", Reachable: true},
		{Key: "scm-s2/x-shard-1-0", Site: "scm-s2", Shard: 1, Role: "master", Reachable: true},
		{Key: "scm-s2/x-shard-1-1", Site: "scm-s2", Shard: 1, Role: "replica", Reachable: true},
	}
	got := CheckSiteSpread(members)
	if len(got) != 1 {
		t.Fatalf("CheckSiteSpread = %+v, want 1 violation (shard 1)", got)
	}
	if got[0].Shard != 1 || !reflect.DeepEqual(got[0].Sites, []string{"scm-s2"}) {
		t.Fatalf("violation = %+v, want shard 1 in [scm-s2]", got[0])
	}
}

func TestCheckSiteSpreadSkipsUnreachableAndMalformed(t *testing.T) {
	members := []ShardMember{
		// Shard 0's cross-site copy is unreachable — do NOT synthesize a spread from a dead node.
		{Key: "scm-s1/x-shard-0-0", Site: "scm-s1", Shard: 0, Role: "master", Reachable: true},
		{Key: "scm-s2/x-shard-0-0", Site: "scm-s2", Shard: 0, Role: "replica", Reachable: false},
		// Legacy / non-per-shard name (shard -1) and no-site key are skipped entirely.
		{Key: "legacy-pod", Site: "", Shard: -1, Role: "master", Reachable: true},
	}
	got := CheckSiteSpread(members)
	if len(got) != 1 || got[0].Shard != 0 {
		t.Fatalf("CheckSiteSpread = %+v, want shard 0 flagged (only reachable member in scm-s1)", got)
	}
}

func TestMembersFromGroundTruth(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.Nodes["scm-s1/x-shard-0-0"] = &redisclient.ClusterNodeState{
		PodName: "scm-s1/x-shard-0-0", Role: "master", Reachable: true,
	}
	gt.Nodes["scm-s2/x-shard-0-0"] = &redisclient.ClusterNodeState{
		PodName: "scm-s2/x-shard-0-0", Role: "replica", Reachable: true,
	}
	members := MembersFromGroundTruth(gt)
	if len(members) != 2 {
		t.Fatalf("MembersFromGroundTruth len = %d, want 2", len(members))
	}
	// Deterministic order (by key).
	if members[0].Site != "scm-s1" || members[0].Shard != 0 || members[0].Role != "master" {
		t.Fatalf("member[0] = %+v", members[0])
	}
	if members[1].Site != "scm-s2" || members[1].Shard != 0 {
		t.Fatalf("member[1] = %+v", members[1])
	}
}
