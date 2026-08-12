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

package multisitegather

import (
	"encoding/json"
	"testing"
	"time"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// TestResultRoundTrip is the operator-emits / lrctl-parses contract: a Result
// marshalled to JSON and read back must carry the identical site-attributed
// ground truth. NOTE: this is largely green-from-birth (encoding/json handles
// exported fields), so it is pinned by TestResultRoundTripFieldTags below, which
// fails if a load-bearing field stops serialising.
const (
	nodeIDMaster = "abc"
	roleMaster   = "master"
)

func TestResultRoundTrip(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.ClusterState = "ok"
	gt.TotalSlots = 16384
	gt.Nodes["scm-s1/ms-smoke-shard-0-0"] = &redisclient.ClusterNodeState{
		PodName: "scm-s1/ms-smoke-shard-0-0", PodIP: "10.1.0.5", NodeID: nodeIDMaster,
		Role: roleMaster, MasterNodeID: "-", Slots: []string{"0-8191"}, Reachable: true,
	}
	gt.Nodes["scm-s2/ms-smoke-shard-0-1"] = &redisclient.ClusterNodeState{
		PodName: "scm-s2/ms-smoke-shard-0-1", PodIP: "10.2.0.7", NodeID: "def",
		Role: "replica", MasterNodeID: nodeIDMaster, LinkStatus: "up", Reachable: true,
	}
	in := Result{
		Instance:   "ms-smoke",
		Namespace:  "ms-smoke",
		GatheredAt: time.Now().UTC().Truncate(time.Second),
		Sites: []SiteObservation{
			{Name: "scm-s1", Reachable: true, Pods: 1},
			{Name: "scm-s2", Reachable: true, Pods: 1},
			{Name: "scm-s3", Reachable: false, Pods: 0, Error: "context deadline exceeded"},
		},
		GroundTruth: gt,
	}

	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Result
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.Instance != in.Instance || out.Namespace != in.Namespace {
		t.Errorf("identity lost: %+v", out)
	}
	if !out.GatheredAt.Equal(in.GatheredAt) {
		t.Errorf("gatheredAt lost: %v != %v", out.GatheredAt, in.GatheredAt)
	}
	if len(out.Sites) != 3 || out.Sites[2].Error == "" || out.Sites[2].Reachable {
		t.Errorf("site observations lost: %+v", out.Sites)
	}
	if out.GroundTruth == nil {
		t.Fatalf("ground truth nil after round-trip")
	}
	if out.GroundTruth.ClusterState != "ok" || out.GroundTruth.TotalSlots != 16384 {
		t.Errorf("cluster state/slots lost: %+v", out.GroundTruth)
	}
	m := out.GroundTruth.Nodes["scm-s1/ms-smoke-shard-0-0"]
	if m == nil || m.Role != roleMaster || len(m.Slots) != 1 || m.Slots[0] != "0-8191" {
		t.Errorf("master node lost: %+v", m)
	}
	r := out.GroundTruth.Nodes["scm-s2/ms-smoke-shard-0-1"]
	if r == nil || r.Role != "replica" || r.MasterNodeID != nodeIDMaster || r.LinkStatus != "up" {
		t.Errorf("replica node lost: %+v", r)
	}
}

// TestResultRoundTripFieldTags gives the round-trip its teeth: it asserts the
// serialised JSON actually contains the load-bearing keys. If a field were
// dropped (json:"-") or renamed, the operator/lrctl contract would silently
// break; this catches it.
func TestResultRoundTripFieldTags(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.Nodes["scm-s1/x-shard-0-0"] = &redisclient.ClusterNodeState{PodName: "scm-s1/x-shard-0-0", Role: roleMaster}
	raw, err := json.Marshal(Result{Instance: "i", Namespace: "ns", GroundTruth: gt})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"instance"`, `"namespace"`, `"sites"`, `"groundTruth"`, `"Nodes"`, `"PodName"`, `"Role"`} {
		if !containsSub(string(raw), key) {
			t.Errorf("expected serialised JSON to contain %s; got %s", key, raw)
		}
	}
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
