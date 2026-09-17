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

	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

func TestClusterPodRefs(t *testing.T) {
	tests := []struct {
		name             string
		instance         string
		shards           int
		replicasPerShard int
		want             []ClusterPodRef
	}{
		{
			name:             "3 shards x 1 replica",
			instance:         "my-cache",
			shards:           3,
			replicasPerShard: 1,
			want: []ClusterPodRef{
				{Name: "my-cache-shard-0-0", ShardIdx: 0, Ordinal: 0, IsMaster: true},
				{Name: "my-cache-shard-0-1", ShardIdx: 0, Ordinal: 1, IsMaster: false},
				{Name: "my-cache-shard-1-0", ShardIdx: 1, Ordinal: 0, IsMaster: true},
				{Name: "my-cache-shard-1-1", ShardIdx: 1, Ordinal: 1, IsMaster: false},
				{Name: "my-cache-shard-2-0", ShardIdx: 2, Ordinal: 0, IsMaster: true},
				{Name: "my-cache-shard-2-1", ShardIdx: 2, Ordinal: 1, IsMaster: false},
			},
		},
		{
			name:             "3 shards x 0 replicas (master only)",
			instance:         "c",
			shards:           3,
			replicasPerShard: 0,
			want: []ClusterPodRef{
				{Name: stsCShard00, ShardIdx: 0, Ordinal: 0, IsMaster: true},
				{Name: stsCShard10, ShardIdx: 1, Ordinal: 0, IsMaster: true},
				{Name: "c-shard-2-0", ShardIdx: 2, Ordinal: 0, IsMaster: true},
			},
		},
		{
			name:             "3 shards x 2 replicas",
			instance:         "big",
			shards:           3,
			replicasPerShard: 2,
			want: []ClusterPodRef{
				{Name: "big-shard-0-0", ShardIdx: 0, Ordinal: 0, IsMaster: true},
				{Name: "big-shard-0-1", ShardIdx: 0, Ordinal: 1, IsMaster: false},
				{Name: "big-shard-0-2", ShardIdx: 0, Ordinal: 2, IsMaster: false},
				{Name: "big-shard-1-0", ShardIdx: 1, Ordinal: 0, IsMaster: true},
				{Name: "big-shard-1-1", ShardIdx: 1, Ordinal: 1, IsMaster: false},
				{Name: "big-shard-1-2", ShardIdx: 1, Ordinal: 2, IsMaster: false},
				{Name: "big-shard-2-0", ShardIdx: 2, Ordinal: 0, IsMaster: true},
				{Name: "big-shard-2-1", ShardIdx: 2, Ordinal: 1, IsMaster: false},
				{Name: "big-shard-2-2", ShardIdx: 2, Ordinal: 2, IsMaster: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClusterPodRefs(tt.instance, tt.shards, tt.replicasPerShard)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClusterPodRefs(%q, %d, %d):\n got: %#v\nwant: %#v",
					tt.instance, tt.shards, tt.replicasPerShard, got, tt.want)
			}
		})
	}
}

// TestChooseReattachTarget pins the invariant that keeps a Redis shard inside one shard
// StatefulSet: an empty pod must reattach to the under-replicated slot-master in ITS OWN
// shard, not an arbitrary one. The fixture is the exact bootstrap scramble observed in
// debug-artifacts-20260730 (shard-K-1 was wrongly welded to a different shard's master).
func TestChooseReattachTarget(t *testing.T) {
	master := func(pod, id, slots string) *redisclient.ClusterNodeState {
		return &redisclient.ClusterNodeState{PodName: pod, NodeID: id, Role: RoleMaster, Slots: []string{slots}}
	}
	nodes := []*redisclient.ClusterNodeState{
		master(stsCShard00, testNodeID0, "0-5461"),
		master(stsCShard10, "id1", "5462-10922"),
		master("c-shard-2-0", "id2", "10923-16383"),
	}
	noReplicas := map[string][]string{}

	// Same-shard preference: each empty replica pod must pick its own shard's master.
	wantByPod := map[string]string{
		"c-shard-0-1": testNodeID0,
		"c-shard-1-1": "id1",
		"c-shard-2-1": "id2",
	}
	for pod, wantID := range wantByPod {
		got := chooseReattachTarget(pod, nodes, noReplicas, 1)
		if got == nil || got.NodeID != wantID {
			t.Errorf("chooseReattachTarget(%q): got %v, want master %s", pod, got, wantID)
		}
	}

	// Already-satisfied master is skipped even if same-shard.
	full := map[string][]string{testNodeID0: {"someReplica"}}
	if got := chooseReattachTarget("c-shard-0-1", nodes, full, 1); got == nil || got.NodeID == testNodeID0 {
		t.Errorf("expected shard-0 master skipped (already has its replica), got %v", got)
	}

	// Fallback: no same-shard master → lowest-PodName under-replicated master, deterministically.
	if got := chooseReattachTarget("c-shard-9-1", nodes, noReplicas, 1); got == nil || got.NodeID != testNodeID0 {
		t.Errorf("fallback: got %v, want lowest-PodName master id0", got)
	}
}

// TestShardMasterPodName pins the master-identity convention that the bootstrap and
// missing-shard-recovery paths depend on: shard K's master is always {name}-shard-K-0.
func TestShardMasterPodName(t *testing.T) {
	cases := map[int]string{
		0: "my-cache-shard-0-0",
		1: "my-cache-shard-1-0",
		2: "my-cache-shard-2-0",
	}
	for shardIdx, want := range cases {
		if got := shardMasterPodName("my-cache", shardIdx); got != want {
			t.Errorf("shardMasterPodName(my-cache, %d) = %q, want %q", shardIdx, got, want)
		}
	}
}
