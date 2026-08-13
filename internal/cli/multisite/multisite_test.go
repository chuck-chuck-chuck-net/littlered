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
	key := SiteQualifiedKey(siteAlpha, "store-shard-0-0")
	if key != "alpha/store-shard-0-0" {
		t.Fatalf("SiteQualifiedKey = %q", key)
	}
	site, obj := SplitSiteKey(key)
	if site != siteAlpha || obj != "store-shard-0-0" {
		t.Fatalf("SplitSiteKey(%q) = %q, %q", key, site, obj)
	}
	// A key with no site prefix is single-site / legacy.
	if s, o := SplitSiteKey("store-shard-0-0"); s != "" || o != "store-shard-0-0" {
		t.Fatalf("SplitSiteKey(no-slash) = %q, %q", s, o)
	}
}

func TestUnionClusterPods(t *testing.T) {
	sites := []SitePods{
		{Site: siteAlpha, Pods: []PodRef{{ObjName: "s-shard-0-0", IP: "10.0.0.1"}, {ObjName: "s-shard-1-0", IP: ""}}},
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
		{in: "alpha=scm-s1,beta=scm-s2", want: map[string]string{siteAlpha: ctxS1, "beta": ctxS2}},
		{in: " alpha = scm-s1 ", want: map[string]string{siteAlpha: ctxS1}},
		{in: siteAlpha, wantErr: true},
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
	siteNames := []string{ctxS1, ctxS2, siteGamma}
	available := []string{"scm-cp", ctxS1, ctxS2, ctxG}

	// Convention (name==context) resolves s1/s2; gamma needs an explicit mapping.
	res := ResolveSiteContexts(siteNames, map[string]string{siteGamma: ctxG}, available)
	want := map[string]string{ctxS1: ctxS1, ctxS2: ctxS2, siteGamma: ctxG}
	if !reflect.DeepEqual(res.Resolved, want) {
		t.Fatalf("Resolved = %v, want %v", res.Resolved, want)
	}
	if len(res.Unresolved) != 0 {
		t.Fatalf("Unresolved = %v, want none", res.Unresolved)
	}

	// Unresolved: no mapping and no same-named context.
	res = ResolveSiteContexts([]string{siteGamma}, nil, available)
	if len(res.Unresolved) != 1 || res.Unresolved[0].Site != siteGamma {
		t.Fatalf("expected gamma unresolved, got %+v", res.Unresolved)
	}
	if _, ok := res.Resolved[siteGamma]; ok {
		t.Fatalf("gamma must not be resolved")
	}

	// Explicit mapping to a non-existent context is an error (unresolved with reason).
	res = ResolveSiteContexts([]string{ctxS1}, map[string]string{ctxS1: "nope"}, available)
	if len(res.Unresolved) != 1 || res.Unresolved[0].Site != ctxS1 {
		t.Fatalf("expected scm-s1 unresolved for bad mapping, got %+v", res)
	}
}

func TestCheckSiteSpread(t *testing.T) {
	// Shard 0 spans two sites (good); shard 1 is entirely in one site (bad).
	members := []ShardMember{
		{Key: keyS1Shard00, Site: ctxS1, Shard: 0, Role: roleMaster, Reachable: true},
		{Key: keyS2Shard00, Site: ctxS2, Shard: 0, Role: roleReplica, Reachable: true},
		{Key: "scm-s2/x-shard-1-0", Site: ctxS2, Shard: 1, Role: roleMaster, Reachable: true},
		{Key: "scm-s2/x-shard-1-1", Site: ctxS2, Shard: 1, Role: roleReplica, Reachable: true},
	}
	got := CheckSiteSpread(members)
	if len(got) != 1 {
		t.Fatalf("CheckSiteSpread = %+v, want 1 violation (shard 1)", got)
	}
	if got[0].Shard != 1 || !reflect.DeepEqual(got[0].Sites, []string{ctxS2}) {
		t.Fatalf("violation = %+v, want shard 1 in [scm-s2]", got[0])
	}
}

func TestCheckSiteSpreadSkipsUnreachableAndMalformed(t *testing.T) {
	members := []ShardMember{
		// Shard 0's cross-site copy is unreachable — do NOT synthesize a spread from a dead node.
		{Key: keyS1Shard00, Site: ctxS1, Shard: 0, Role: roleMaster, Reachable: true},
		{Key: keyS2Shard00, Site: ctxS2, Shard: 0, Role: roleReplica, Reachable: false},
		// Legacy / non-per-shard name (shard -1) and no-site key are skipped entirely.
		{Key: "legacy-pod", Site: "", Shard: -1, Role: roleMaster, Reachable: true},
	}
	got := CheckSiteSpread(members)
	if len(got) != 1 || got[0].Shard != 0 {
		t.Fatalf("CheckSiteSpread = %+v, want shard 0 flagged (only reachable member in scm-s1)", got)
	}
}

// TestMasterCountsFromGroundTruth checks the D2 slot-serving-master-per-site count: only
// masters with >= 1 owned slot count (a fresh empty master is not yet a Domain-M vote), and
// the result is site-attributed and sorted.
func TestMasterCountsFromGroundTruth(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	// s1 hosts two slot-serving masters (drift).
	gt.Nodes["s1/x-shard-0-0"] = &redisclient.ClusterNodeState{PodName: "s1/x-shard-0-0", Role: roleMaster, Slots: []string{"0-100"}, Reachable: true}
	gt.Nodes["s1/x-shard-1-0"] = &redisclient.ClusterNodeState{PodName: "s1/x-shard-1-0", Role: roleMaster, Slots: []string{"101-200"}, Reachable: true}
	// s2 hosts one slot-serving master.
	gt.Nodes["s2/x-shard-2-0"] = &redisclient.ClusterNodeState{PodName: "s2/x-shard-2-0", Role: roleMaster, Slots: []string{"201-300"}, Reachable: true}
	// s2 also hosts a replica (not counted) and an EMPTY master (no slots — not counted).
	gt.Nodes["s2/x-shard-0-1"] = &redisclient.ClusterNodeState{PodName: "s2/x-shard-0-1", Role: roleReplica, Reachable: true}
	gt.Nodes["s3/x-shard-3-0"] = &redisclient.ClusterNodeState{PodName: "s3/x-shard-3-0", Role: roleMaster, Slots: nil, Reachable: true}

	got := MasterCountsFromGroundTruth(gt)
	want := []SiteMasterCount{{Site: "s1", Masters: 2}, {Site: "s2", Masters: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MasterCountsFromGroundTruth = %+v, want %+v", got, want)
	}
}

// TestEvaluateMasterDistribution checks the D2 balance-invariant live health: the balance
// target ceil(K/2)-1, the informational per-site over-target flag, and the structural
// resilience verdict.
func TestEvaluateMasterDistribution(t *testing.T) {
	// K=4 over 3 sites at 2+1+1: target 1, floor 2. s1 holds 2 — over the native target (its
	// loss recovers by takeover, not a native election) but AT the floor, so it is not an
	// availability exposure. LR-031: the topology IS structurally resilient (3*2 >= 4); the
	// former "K=4-over-3-sites trap" was an artefact of deriving this from the native bound.
	md := EvaluateMasterDistribution(4, 3, []SiteMasterCount{
		{Site: "s2", Masters: 1}, {Site: "s1", Masters: 2}, {Site: "s3", Masters: 1},
	})
	if md.Target != 1 || md.Floor != 2 {
		t.Errorf("Target = %d, Floor = %d, want 1 and 2", md.Target, md.Floor)
	}
	if len(md.OverFloor) != 0 {
		t.Errorf("OverFloor = %v, want none (2+1+1 sits at the floor)", md.OverFloor)
	}
	if !reflect.DeepEqual(md.OverTarget, []string{"s1"}) {
		t.Errorf("OverTarget = %v, want [s1]", md.OverTarget)
	}
	if !md.Resilient {
		t.Errorf("K=4/3-sites must be structurally resilient (floor 2 reachable)")
	}
	// Counts must come back sorted by site.
	if len(md.Counts) != 3 || md.Counts[0].Site != "s1" || md.Counts[2].Site != "s3" {
		t.Errorf("Counts not sorted by site: %+v", md.Counts)
	}

	// K=4 over 2 sites drifted to 3+1: s1 is over the FLOOR (2), so losing it leaves the lost
	// side holding a master majority and the takeover cannot be fenced — an actionable exposure.
	md2 := EvaluateMasterDistribution(4, 2, []SiteMasterCount{
		{Site: "s1", Masters: 3}, {Site: "s2", Masters: 1},
	})
	if !reflect.DeepEqual(md2.OverFloor, []string{"s1"}) {
		t.Errorf("OverFloor = %v, want [s1]", md2.OverFloor)
	}
	if !md2.Resilient {
		t.Errorf("K=4/2-sites is structurally resilient (2+2 reachable); the 3+1 drift is live, not structural")
	}

	// K=4 over 4 sites, one master each: at the native target, nothing flagged.
	md3 := EvaluateMasterDistribution(4, 4, []SiteMasterCount{
		{Site: "s1", Masters: 1}, {Site: "s2", Masters: 1}, {Site: "s3", Masters: 1}, {Site: "s4", Masters: 1},
	})
	if len(md3.OverFloor) != 0 || len(md3.OverTarget) != 0 || !md3.Resilient {
		t.Errorf("K=4/4-sites balanced: overFloor=%v overTarget=%v resilient=%v",
			md3.OverFloor, md3.OverTarget, md3.Resilient)
	}
}

func TestMembersFromGroundTruth(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.Nodes[keyS1Shard00] = &redisclient.ClusterNodeState{
		PodName: keyS1Shard00, Role: roleMaster, Reachable: true,
	}
	gt.Nodes[keyS2Shard00] = &redisclient.ClusterNodeState{
		PodName: keyS2Shard00, Role: roleReplica, Reachable: true,
	}
	members := MembersFromGroundTruth(gt)
	if len(members) != 2 {
		t.Fatalf("MembersFromGroundTruth len = %d, want 2", len(members))
	}
	// Deterministic order (by key).
	if members[0].Site != ctxS1 || members[0].Shard != 0 || members[0].Role != roleMaster {
		t.Fatalf("member[0] = %+v", members[0])
	}
	if members[1].Site != ctxS2 || members[1].Shard != 0 {
		t.Fatalf("member[1] = %+v", members[1])
	}
}
