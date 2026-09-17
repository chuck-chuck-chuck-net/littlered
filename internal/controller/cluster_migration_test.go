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
	"sort"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// Red-first table tests for the ADR-013 migration driver's pure helpers (the decisions the
// driver makes that are not already in M1's plan). Each was authored against a zero-value
// stub and observed failing before the real body landed (see the milestone report).

func TestIsLegacyClusterPod(t *testing.T) {
	tests := []struct {
		pod, name string
		want      bool
	}{
		{stsMrCluster0, "mr", true},
		{"mr-cluster-5", "mr", true},
		{stsMrShard00, "mr", false},
		{"mr-shard-1-2", "mr", false},
		{"mr-cluster-x", "mr", false},    // non-integer ordinal
		{"mr-cluster-", "mr", false},     // missing ordinal
		{"other-cluster-0", "mr", false}, // wrong instance
		{"mr-cluster-0-0", "mr", false},  // extra segment (not a legacy pod)
	}
	for _, tc := range tests {
		if got := isLegacyClusterPod(tc.pod, tc.name); got != tc.want {
			t.Errorf("isLegacyClusterPod(%q,%q) = %v, want %v", tc.pod, tc.name, got, tc.want)
		}
	}
}

func TestIsNewShardPod(t *testing.T) {
	tests := []struct {
		pod, name string
		want      bool
	}{
		{stsMrShard00, "mr", true},
		{"mr-shard-1-2", "mr", true},
		{stsMrCluster0, "mr", false},
		{"mr-shard-0", "mr", false},      // missing ordinal
		{"mr-shard-a-0", "mr", false},    // non-integer shard
		{"mr-shard-0-b", "mr", false},    // non-integer ordinal
		{"other-shard-0-0", "mr", false}, // wrong instance
	}
	for _, tc := range tests {
		if got := isNewShardPod(tc.pod, tc.name); got != tc.want {
			t.Errorf("isNewShardPod(%q,%q) = %v, want %v", tc.pod, tc.name, got, tc.want)
		}
	}
}

func TestAllLegacyPodsReady(t *testing.T) {
	tests := []struct {
		name string
		pods []migPodFact
		want bool
	}{
		{
			name: "all legacy ready",
			pods: []migPodFact{
				{Name: stsMrCluster0, RedisReady: true},
				{Name: stsMrCluster1, RedisReady: true},
			},
			want: true,
		},
		{
			name: "one legacy not ready",
			pods: []migPodFact{
				{Name: stsMrCluster0, RedisReady: true},
				{Name: stsMrCluster1, RedisReady: false},
			},
			want: false,
		},
		{
			name: "new pods ignored; legacy all ready",
			pods: []migPodFact{
				{Name: stsMrCluster0, RedisReady: true},
				{Name: stsMrShard00, RedisReady: false}, // new pod not counted
			},
			want: true,
		},
		{
			name: "no legacy pods -> not ready",
			pods: []migPodFact{{Name: stsMrShard00, RedisReady: true}},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := allLegacyPodsReady(tc.pods, "mr"); got != tc.want {
				t.Errorf("allLegacyPodsReady = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildLegacyFactsFromPods(t *testing.T) {
	// Ground truth: two reachable legacy masters + replicas; no new pods MET yet.
	gt := redisclient.NewClusterGroundTruth()
	add := func(pod, id, ip string, reachable bool) {
		gt.Nodes[pod] = &redisclient.ClusterNodeState{PodName: pod, NodeID: id, PodIP: ip, Reachable: reachable}
	}
	add(stsMrCluster0, "L0", ipMaster, true)
	add(stsMrCluster1, "L1", ipReplica, true)
	add("mr-cluster-2", "L2", ipNode3, true)
	add("mr-cluster-3", "L3", "10.0.0.4", true)

	pods := []migPodFact{
		{Name: stsMrCluster0, IP: ipMaster, RedisReady: true},
		{Name: stsMrCluster1, IP: ipReplica, RedisReady: true},
		{Name: "mr-cluster-2", IP: ipNode3, RedisReady: true},
		{Name: "mr-cluster-3", IP: "10.0.0.4", RedisReady: true},
		// new pods up (have IPs) but not yet MET (absent from gt.Nodes)
		{Name: stsMrShard00, IP: "10.1.0.1", RedisReady: true},
		{Name: "mr-shard-0-1", IP: "10.1.0.2", RedisReady: true},
		{Name: "mr-shard-1-0", IP: "", RedisReady: false}, // not up yet (no IP)
	}

	facts, allReady, newExist := buildLegacyFactsFromPods(pods, gt, "mr")

	if !allReady {
		t.Errorf("allLegacyReady = false, want true")
	}
	if !newExist {
		t.Errorf("newPodsExist = false, want true")
	}

	// LegacyNodeIDs = the four legacy node IDs (order-independent).
	gotIDs := append([]string(nil), facts.LegacyNodeIDs...)
	sort.Strings(gotIDs)
	if !reflect.DeepEqual(gotIDs, []string{"L0", "L1", "L2", "L3"}) {
		t.Errorf("LegacyNodeIDs = %v, want [L0 L1 L2 L3]", gotIDs)
	}

	// NewPodAddrs: only new pods with an IP, keyed by pod name.
	wantAddrs := map[string]string{
		stsMrShard00:   "10.1.0.1:6379",
		"mr-shard-0-1": "10.1.0.2:6379",
	}
	if !reflect.DeepEqual(facts.NewPodAddrs, wantAddrs) {
		t.Errorf("NewPodAddrs = %v, want %v", facts.NewPodAddrs, wantAddrs)
	}

	// SeedAddrs: reachable legacy pods' addrs (order-independent).
	gotSeeds := append([]string(nil), facts.SeedAddrs...)
	sort.Strings(gotSeeds)
	wantSeeds := []string{"10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379", "10.0.0.4:6379"}
	if !reflect.DeepEqual(gotSeeds, wantSeeds) {
		t.Errorf("SeedAddrs = %v, want %v", gotSeeds, wantSeeds)
	}
}

func TestBuildLegacyFactsFromPods_NoNewPods(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.Nodes[stsMrCluster0] = &redisclient.ClusterNodeState{PodName: stsMrCluster0, NodeID: "L0", PodIP: ipMaster, Reachable: true}
	pods := []migPodFact{{Name: stsMrCluster0, IP: ipMaster, RedisReady: true}}

	_, _, newExist := buildLegacyFactsFromPods(pods, gt, "mr")
	if newExist {
		t.Errorf("newPodsExist = true, want false (no shard pods present)")
	}
}

func TestRestrictToLegacyMesh(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	add := func(pod, id string) {
		gt.Nodes[pod] = &redisclient.ClusterNodeState{PodName: pod, NodeID: id, Reachable: true}
	}
	add(stsMrCluster0, "L0")
	add(stsMrCluster1, "L1")
	add(stsMrShard00, "N00")   // MET: in legacy partition
	add("mr-shard-1-0", "N10") // un-MET: its own partition

	// Legacy cluster + the MET new node form one partition; the un-MET node is alone.
	gt.Partitions = [][]string{
		{"L0", "L1", "N00"},
		{"N10"},
	}

	restrictToLegacyMesh(gt, []string{"L0", "L1"}, "mr")

	if _, ok := gt.Nodes["mr-shard-1-0"]; ok {
		t.Errorf("un-MET new pod mr-shard-1-0 should have been removed from gt.Nodes")
	}
	if _, ok := gt.Nodes[stsMrShard00]; !ok {
		t.Errorf("MET new pod mr-shard-0-0 must be retained")
	}
	if _, ok := gt.Nodes[stsMrCluster0]; !ok {
		t.Errorf("legacy pod mr-cluster-0 must never be removed")
	}
}

func TestRestrictToLegacyMesh_NoNewPods(t *testing.T) {
	gt := redisclient.NewClusterGroundTruth()
	gt.Nodes[stsMrCluster0] = &redisclient.ClusterNodeState{PodName: stsMrCluster0, NodeID: "L0", Reachable: true}
	gt.Partitions = [][]string{{"L0"}}
	restrictToLegacyMesh(gt, []string{"L0"}, "mr")
	if len(gt.Nodes) != 1 {
		t.Errorf("no-op expected when no new pods; got %d nodes", len(gt.Nodes))
	}
}

// TestIsLegacyClusterStatefulSet is the WS3 hardening of the migration trigger (ADR-013 §5):
// detectLegacyClusterStatefulSet must not fire on any StatefulSet that merely shares the
// {name}-cluster name — it must positively identify a genuine pre-0.3.0 single-STS cluster.
// Authored red-first: with the pure helper stubbed to return false the genuine case fails
// (want true); stubbed to return true every negative case fails (want false); the real body
// makes all cases green (see the milestone report for the two observed red runs).
func TestIsLegacyClusterStatefulSet(t *testing.T) {
	int32Ptr := func(i int32) *int32 { return &i }
	boolPtr := func(b bool) *bool { return &b }
	intPtr := func(i int) *int { return &i }

	const crUID = types.UID("cr-uid-abc123")

	// shards=3, replicasPerShard=1 ⇒ GetTotalNodes() == 6 (whole-cluster sizing).
	newLR := func() *littleredv1alpha1.LittleRed {
		return &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: "mr", Namespace: "ns", UID: crUID},
			Spec: littleredv1alpha1.LittleRedSpec{
				Cluster: &littleredv1alpha1.ClusterSpec{Shards: 3, ReplicasPerShard: intPtr(1)},
			},
		}
	}

	// A genuine pre-0.3.0 single-STS cluster: name {name}-cluster, component=cluster, NO shard
	// label, replicas == shards*(1+replicasPerShard) == 6, controller-owned by the CR.
	genuine := func() *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mr-cluster",
				Namespace: "ns",
				Labels:    map[string]string{labelAppComponent: ComponentCluster},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: littleredv1alpha1.GroupVersion.String(),
					Kind:       "LittleRed",
					Name:       "mr",
					UID:        crUID,
					Controller: boolPtr(true),
				}},
			},
			Spec: appsv1.StatefulSetSpec{Replicas: int32Ptr(6)},
		}
	}

	tests := []struct {
		name   string
		mutate func(*appsv1.StatefulSet)
		want   bool
	}{
		{"genuine legacy single-STS cluster", func(*appsv1.StatefulSet) {}, true},
		{"per-shard STS: carries shard label", func(s *appsv1.StatefulSet) {
			s.Labels[LabelShard] = clusterShardLabelValue(0)
		}, false},
		{"wrong replica count (per-shard sizing 1+rps)", func(s *appsv1.StatefulSet) {
			s.Spec.Replicas = int32Ptr(2)
		}, false},
		{"nil replica count", func(s *appsv1.StatefulSet) {
			s.Spec.Replicas = nil
		}, false},
		{"missing component label", func(s *appsv1.StatefulSet) {
			delete(s.Labels, labelAppComponent)
		}, false},
		{"wrong component label", func(s *appsv1.StatefulSet) {
			s.Labels[labelAppComponent] = ComponentRedis
		}, false},
		{"not controller-owned: foreign UID", func(s *appsv1.StatefulSet) {
			s.OwnerReferences[0].UID = types.UID("someone-else")
		}, false},
		{"not controller-owned: no owner references", func(s *appsv1.StatefulSet) {
			s.OwnerReferences = nil
		}, false},
		{"owned but not controller (Controller=false)", func(s *appsv1.StatefulSet) {
			s.OwnerReferences[0].Controller = boolPtr(false)
		}, false},
		{"wrong name (not {name}-cluster)", func(s *appsv1.StatefulSet) {
			s.Name = "mr-shard-0"
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sts := genuine()
			tc.mutate(sts)
			if got := isLegacyClusterStatefulSet(sts, newLR()); got != tc.want {
				t.Errorf("isLegacyClusterStatefulSet() = %v, want %v", got, tc.want)
			}
		})
	}
}
