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

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// gtNode is a tiny helper to add a site-qualified node to a ground truth.
func gtNode(gt *redisclient.ClusterGroundTruth, key, role, masterID, link string, slots ...string) {
	gt.Nodes[key] = &redisclient.ClusterNodeState{
		PodName: key, Role: role, MasterNodeID: masterID, LinkStatus: link, Slots: slots, Reachable: true,
	}
}

func TestBuildCrossSiteView_healthyCrossSite(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.ClusterState = "ok"
	gt.TotalSlots = 16384
	// shard 0: master in s1, replica in s2 (cross-site, link up)
	gtNode(gt, "scm-s1/ms-shard-0-0", "master", "-", "", "0-8191")
	gtNode(gt, "scm-s2/ms-shard-0-1", "replica", "m0", "up")
	// shard 1: master in s2, replica in s3 (cross-site, link up)
	gtNode(gt, "scm-s2/ms-shard-1-0", "master", "-", "", "8192-16383")
	gtNode(gt, "scm-s3/ms-shard-1-1", "replica", "m1", "up")

	state, slots, crossSiteOK, shards, _ := buildCrossSiteView(gt)
	if state != "ok" || slots != 16384 {
		t.Errorf("state/slots = %q/%d, want ok/16384", state, slots)
	}
	if !crossSiteOK {
		t.Errorf("want crossSiteOK true")
	}
	if len(shards) != 2 {
		t.Fatalf("want 2 shards, got %d: %+v", len(shards), shards)
	}
	if shards[0].Shard != 0 || shards[0].MasterSite != "scm-s1" ||
		len(shards[0].ReplicaSites) != 1 || shards[0].ReplicaSites[0] != "scm-s2" || !shards[0].CrossSite {
		t.Errorf("shard0 wrong: %+v", shards[0])
	}
	if shards[1].Shard != 1 || shards[1].MasterSite != "scm-s2" || !shards[1].CrossSite {
		t.Errorf("shard1 wrong: %+v", shards[1])
	}
}

func TestBuildCrossSiteView_sameSiteShardIsNotCrossSite(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.ClusterState = "ok"
	gt.TotalSlots = 16384
	// shard 0: master AND replica both in s1 → not cross-site
	gtNode(gt, "scm-s1/ms-shard-0-0", "master", "-", "", "0-16383")
	gtNode(gt, "scm-s1/ms-shard-0-1", "replica", "m0", "up")

	_, _, crossSiteOK, shards, _ := buildCrossSiteView(gt)
	if crossSiteOK {
		t.Errorf("want crossSiteOK false for single-site shard")
	}
	if len(shards) != 1 || shards[0].CrossSite {
		t.Errorf("shard0 should be flagged not cross-site: %+v", shards)
	}
}

func TestBuildCrossSiteView_linkDownFlagged(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.ClusterState = "ok"
	gt.TotalSlots = 16384
	gtNode(gt, "scm-s1/ms-shard-0-0", "master", "-", "", "0-16383")
	gtNode(gt, "scm-s2/ms-shard-0-1", "replica", "m0", "down")

	_, _, _, shards, _ := buildCrossSiteView(gt)
	if len(shards) != 1 || !shards[0].LinkDown {
		t.Errorf("want shard0 LinkDown true: %+v", shards)
	}
}

// TestBuildCrossSiteView_masterCountsPerSite checks the D2 master-per-site distribution: only
// slot-serving masters (role master + >= 1 slot) are counted, attributed to their site and
// sorted. An empty (no-slot) master and replicas are excluded.
func TestBuildCrossSiteView_masterCountsPerSite(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.ClusterState = "ok"
	// scm-s1 holds two slot-serving masters (drift); scm-s2 holds one; scm-s3 holds only an
	// empty master + a replica (neither counts).
	gtNode(gt, "scm-s1/ms-shard-0-0", "master", "-", "", "0-5460")
	gtNode(gt, "scm-s1/ms-shard-1-0", "master", "-", "", "5461-10922")
	gtNode(gt, "scm-s2/ms-shard-2-0", "master", "-", "", "10923-16383")
	gtNode(gt, "scm-s2/ms-shard-0-1", "replica", "m0", "up")
	gtNode(gt, "scm-s3/ms-shard-3-0", "master", "-", "") // empty master: no slots

	_, _, _, _, masterCounts := buildCrossSiteView(gt)
	want := []littleredv1alpha1.SiteMasterCount{{Site: "scm-s1", Masters: 2}, {Site: "scm-s2", Masters: 1}}
	if len(masterCounts) != len(want) {
		t.Fatalf("masterCounts = %+v, want %+v", masterCounts, want)
	}
	for i := range want {
		if masterCounts[i] != want[i] {
			t.Fatalf("masterCounts[%d] = %+v, want %+v", i, masterCounts[i], want[i])
		}
	}
}

func TestBuildCrossSiteView_replicaSitesSortedDeduped(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.ClusterState = "ok"
	gt.TotalSlots = 16384
	gtNode(gt, "scm-s2/ms-shard-0-0", "master", "-", "", "0-16383")
	gtNode(gt, "scm-s3/ms-shard-0-1", "replica", "m0", "up")
	gtNode(gt, "scm-s1/ms-shard-0-2", "replica", "m0", "up")
	gtNode(gt, "scm-s1/ms-shard-0-3", "replica", "m0", "up") // duplicate site s1

	_, _, _, shards, _ := buildCrossSiteView(gt)
	if len(shards) != 1 {
		t.Fatalf("want 1 shard, got %d", len(shards))
	}
	got := shards[0].ReplicaSites
	if len(got) != 2 || got[0] != "scm-s1" || got[1] != "scm-s3" {
		t.Errorf("replica sites should be sorted+deduped [scm-s1 scm-s3], got %v", got)
	}
}
