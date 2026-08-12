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

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// TestBuildSiteTakeoverInput exercises the executor's projection from gather + per-site
// witness onto the pure planner input, for the canonical dual-site even-split loss: two
// sites, one shard's master in each; the second site is lost. It then feeds the projected
// input through the real planner to confirm the end-to-end verdict.
func TestBuildSiteTakeoverInput(t *testing.T) {
	lr := multiSiteClusterCR(2, 2, 2, 0) // 2 sites, sitesPerShard 2, 2 shards, 0 replicas/shard
	r := &LittleRedReconciler{}

	gt := redisclient.NewClusterGroundTruth()
	// Survivor site (site0): shard0's master (serving) and shard1's surviving replica.
	gt.Nodes[keySite0Shard00] = &redisclient.ClusterNodeState{
		PodName: keySite0Shard00, PodIP: "10.0.0.1", NodeID: "m0", Reachable: true,
		Role: RoleMaster, Slots: []string{"0-8191"}, ClusterState: "fail",
	}
	gt.Nodes[keySite0Shard10] = &redisclient.ClusterNodeState{
		PodName: keySite0Shard10, PodIP: "10.0.0.2", NodeID: "r1", Reachable: true,
		Role: RoleReplica, ReplOffset: 500, MasterReplid: "L1", ClusterState: "fail",
	}
	// Lost site (site1): both its pods unreachable.
	gt.Nodes["site1/cache-shard-0-0"] = &redisclient.ClusterNodeState{PodName: "site1/cache-shard-0-0", Reachable: false}
	gt.Nodes["site1/cache-shard-1-0"] = &redisclient.ClusterNodeState{PodName: "site1/cache-shard-1-0", Reachable: false}

	obs := []siteObservation{
		{
			site: resolvedSite{name: siteName0, client: &recordingSiteClient{name: siteName0}}, reachable: true,
			pods: []sitePodObservation{
				{objName: podCacheShard00, key: keySite0Shard00, ready: true},
				{objName: "cache-shard-1-0", key: keySite0Shard10, ready: true},
			},
		},
		{site: resolvedSite{name: siteName1}, reachable: false}, // API unreachable ⇒ lost site
	}

	in := r.buildSiteTakeoverInput(lr, gt, obs, 2)

	if in.size != 2 || in.reachableMasters != 1 {
		t.Fatalf("size/reachableMasters = %d/%d, want 2/1", in.size, in.reachableMasters)
	}
	if !in.survivorsClusterFail {
		t.Errorf("expected survivorsClusterFail=true (site0 reports cluster_state:fail)")
	}
	if !in.quiet {
		t.Errorf("expected quiet=true (no terminating survivor)")
	}
	if len(in.orphanTargets) != 1 || in.orphanTargets[0].shard != 1 || in.orphanTargets[0].key != keySite0Shard10 {
		t.Fatalf("orphanTargets = %+v, want single shard-1 target site0/cache-shard-1-0", in.orphanTargets)
	}

	// End-to-end (D1): even split (unreachable 1 < majority 2), survivors report their own
	// cluster_state:fail, quiet, past cooldown ⇒ a provably split-brain-free takeover — no
	// Kubernetes witness consulted.
	now := time.Unix(1_800_000_000, 0)
	armed := now.Add(-2 * time.Minute)
	plan := planSiteTakeover(in, &armed, now, 60*time.Second)
	if plan.action != siteTakeover {
		t.Fatalf("end-to-end action = %s, want Takeover", plan.action)
	}
}
