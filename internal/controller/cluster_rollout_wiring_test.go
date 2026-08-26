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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// rolloutOwnerLR is the instance name the shard-pod-name fixtures below are built from.
const rolloutOwnerLR = "my-cache"

func clusterLRWithReplicas(rps int) *littleredv1alpha1.LittleRed {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeCluster
	lr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{Shards: 3, ReplicasPerShard: &rps}
	return lr
}

// TestBuildClusterShardStatefulSetRendersPartition pins the ADR-017 builder contract: the
// builder RENDERS the rollout partition it is handed and never computes one (LR-044's
// "a builder renders a decision, it does not make one"), and nil means "no rollingUpdate
// block at all" — today's shape, and the replicasPerShard: 0 case, byte-identical to what
// shipped before this change.
func TestBuildClusterShardStatefulSetRendersPartition(t *testing.T) {
	lr := clusterLRWithReplicas(1)

	t.Run("nil emits no rollingUpdate block", func(t *testing.T) {
		us := buildClusterShardStatefulSet(lr, 0, nil).Spec.UpdateStrategy
		if us.Type != appsv1.RollingUpdateStatefulSetStrategyType {
			t.Errorf("update strategy type = %q, want RollingUpdate", us.Type)
		}
		if us.RollingUpdate != nil {
			t.Errorf("nil partition must emit NO rollingUpdate block, got %+v", us.RollingUpdate)
		}
	})

	for _, want := range []int32{0, 1, 2} {
		t.Run("renders the value it is given", func(t *testing.T) {
			us := buildClusterShardStatefulSet(lr, 0, &want).Spec.UpdateStrategy
			if us.RollingUpdate == nil || us.RollingUpdate.Partition == nil {
				t.Fatalf("partition %d: expected a rollingUpdate.partition, got %+v", want, us.RollingUpdate)
			}
			if got := *us.RollingUpdate.Partition; got != want {
				t.Errorf("partition = %d, want %d", got, want)
			}
		})
	}
}

// TestClusterShardPartitionIsOutsideThePodTemplateHash is the ADR-017 Consequences claim,
// pinned rather than asserted in prose: `partition` lives in spec.updateStrategy, OUTSIDE
// the pod template that AnnotationPodSpecHash covers. If it were inside, then (a) upgrading
// to this build would trigger a rolling update of every cluster instance purely from the
// partition appearing, and (b) worse, every lowering of the partition would itself change
// the desired hash and restart the rollout — the gate would drive an endless roll.
//
// Green from birth (it asserts a structural property of code that already exists). Its
// teeth were shown with a mutation: folding the rendered rollingUpdate block into the
// template the hash is computed over made all three rows below fail, each with a different
// hash — d048fbc0 / c694b352 / 9f6dee88 against the baseline d3454c86.
func TestClusterShardPartitionIsOutsideThePodTemplateHash(t *testing.T) {
	lr := clusterLRWithReplicas(1)

	hashFor := func(p *int32) string {
		return buildClusterShardStatefulSet(lr, 0, p).Spec.Template.Annotations[AnnotationPodSpecHash]
	}

	base := hashFor(nil)
	if base == "" {
		t.Fatal("no pod-spec hash stamped at all")
	}
	for _, p := range []int32{0, 1, 2} {
		if got := hashFor(&p); got != base {
			t.Errorf("partition %d changed the pod-template hash (%q != %q); the rollout gate "+
				"would restart its own rollout on every step", p, got, base)
		}
	}
}

// --- buildShardRolloutInput: live objects → the seam's facts ------------------------

func rolloutSTS(appliedHash string, gen, observed int64, cur, upd string, partition *int32) *appsv1.StatefulSet {
	sts := &appsv1.StatefulSet{}
	sts.Name = "s"
	sts.Generation = gen
	sts.Spec.Template.Annotations = map[string]string{AnnotationPodSpecHash: appliedHash}
	sts.Status.ObservedGeneration = observed
	sts.Status.CurrentRevision = cur
	sts.Status.UpdateRevision = upd
	if partition != nil {
		p := *partition
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &p}
	}
	return sts
}

func rolloutPodObj(name, revision string, redisReady bool, readySince time.Time) *corev1.Pod {
	pod := &corev1.Pod{}
	pod.Name = name
	pod.Labels = map[string]string{labelControllerRevisionHash: revision}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: ComponentRedis, Ready: redisReady}}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodReady, LastTransitionTime: metav1.NewTime(readySince),
	}}
	return pod
}

// TestShardSlotOwnerResolvesTheServingNode pins what "the shard's slot owner" means: the
// master actually serving shard K's range as the cluster reports it, NOT the pod the
// operator intends to be the master. After the master ordinal has been replaced and Redis
// has failed the range over, the owner is a different pod, and the gate must key on the
// live answer or it waits for a replication link to a node that owns nothing.
func TestShardSlotOwnerResolvesTheServingNode(t *testing.T) {
	// GenerateSlotRanges(3) = 0-5461 / 5462-10922 / 10923-16383.
	node := func(shard, ord int, id, role, masterID string, slots ...string) (string, *redisclient.ClusterNodeState) {
		name := clusterShardPodName(rolloutOwnerLR, shard, ord)
		return name, &redisclient.ClusterNodeState{
			PodName: name, NodeID: id, Role: role, MasterNodeID: masterID, Slots: slots,
		}
	}
	nodes := map[string]*redisclient.ClusterNodeState{}
	for _, f := range []func() (string, *redisclient.ClusterNodeState){
		func() (string, *redisclient.ClusterNodeState) { return node(0, 0, "A", RoleMaster, "", "0-5461") },
		func() (string, *redisclient.ClusterNodeState) { return node(0, 1, "a", RoleReplica, "A") },
		// Shard 1's range is served by the REPLICA ordinal after a failover.
		func() (string, *redisclient.ClusterNodeState) { return node(1, 0, "B", RoleReplica, "b") },
		func() (string, *redisclient.ClusterNodeState) { return node(1, 1, "b", RoleMaster, "", "5462-10922") },
		// Shard 2 owns a FRAGMENTED range that merely contains its start slot.
		func() (string, *redisclient.ClusterNodeState) {
			return node(2, 0, "C", RoleMaster, "", "10923-12000", "12001-16383")
		},
	} {
		name, n := f()
		nodes[name] = n
	}
	gt := &redisclient.ClusterGroundTruth{Nodes: nodes}

	for _, tc := range []struct {
		shard int
		want  string
	}{
		{0, "A"},
		{1, "b"},
		{2, "C"},
	} {
		got := shardSlotOwner(gt, rolloutOwnerLR, 3, 1, tc.shard)
		if got == nil {
			t.Fatalf("shard %d: no owner resolved; clause (c) could never be satisfied and the rollout would stall forever", tc.shard)
		}
		if got.NodeID != tc.want {
			t.Errorf("shard %d owner = %q, want %q", tc.shard, got.NodeID, tc.want)
		}
	}

	if shardSlotOwner(nil, rolloutOwnerLR, 3, 1, 0) != nil {
		t.Error("a nil ground truth must resolve no owner, not panic")
	}
	if shardSlotOwner(gt, rolloutOwnerLR, 3, 1, 7) != nil {
		t.Error("an out-of-range shard index must resolve no owner")
	}
}

// TestBuildShardRolloutInputRedundancy is the wiring the seam's doc comment specifies in two
// lines, exercised against the shapes that matter: only a link-UP replica of the owner is
// Synced (LR-025), an attached-but-link-down replica is Attached-not-Synced (a full sync in
// flight — progress, never reported blocked), and a fresh empty master is neither.
func TestBuildShardRolloutInputRedundancy(t *testing.T) {
	lr := clusterLRWithReplicas(2)
	lr.Name = rolloutOwnerLR
	owner := &redisclient.ClusterNodeState{NodeID: "A", Role: RoleMaster, Slots: []string{"0-5461"}}
	upReplica := &redisclient.ClusterNodeState{NodeID: "a1", Role: RoleReplica, MasterNodeID: "A", LinkStatus: "up"}
	syncing := &redisclient.ClusterNodeState{NodeID: "a2", Role: RoleReplica, MasterNodeID: "A", LinkStatus: "down"}
	gt := &redisclient.ClusterGroundTruth{Nodes: map[string]*redisclient.ClusterNodeState{}}
	now := time.Now()
	pods := map[string]*corev1.Pod{}
	for ord, n := range []*redisclient.ClusterNodeState{owner, upReplica, syncing} {
		name := clusterShardPodName(rolloutOwnerLR, 0, ord)
		gt.Nodes[name] = n
		pods[name] = rolloutPodObj(name, "rev2", true, now.Add(-time.Minute))
	}

	in := buildShardRolloutInput(lr, 0, 2, "want", rolloutSTS("have", 3, 2, "rev1", "rev2", nil), pods, gt, now)

	if len(in.Pods) != 3 {
		t.Fatalf("expected 3 pods, got %d", len(in.Pods))
	}
	// IsOwner is what keeps a shard's own owner out of the STALL survey: it fails clause (c)
	// structurally (a master is nobody's replica) and must not be reported as blocked.
	want := []struct{ attached, synced, isOwner bool }{
		{false, false, true}, // ordinal 0 IS the owner — a master is nobody's replica
		{true, true, false},  // ordinal 1: link up
		{true, false, false}, // ordinal 2: attached, mid full sync
	}
	for i, w := range want {
		p := in.Pods[i]
		if p.Ordinal != i {
			t.Fatalf("pods must be in ordinal order; got %d at index %d", p.Ordinal, i)
		}
		if p.AttachedToOwner != w.attached || p.SyncedWithOwner != w.synced || p.IsOwner != w.isOwner {
			t.Errorf("ordinal %d: attached=%v synced=%v isOwner=%v, want attached=%v synced=%v isOwner=%v",
				i, p.AttachedToOwner, p.SyncedWithOwner, p.IsOwner, w.attached, w.synced, w.isOwner)
		}
	}
}

// TestBuildShardRolloutInputStructuralFacts pins the rest of the translation: the applied
// partition IS the cursor (read off the live object, nil when absent), the revision comes
// from the pod's controller-revision-hash, readiness is the REDIS CONTAINER's per the
// kubelet (not the pod-level condition), and a missing ordinal is simply absent.
func TestBuildShardRolloutInputStructuralFacts(t *testing.T) {
	lr := clusterLRWithReplicas(1)
	lr.Name = rolloutOwnerLR
	now := time.Now()
	readySince := now.Add(-90 * time.Second)

	// Pod 0 is Ready at the pod level but its REDIS container is not — the gate must not
	// take that as clause (b) satisfied. Pod 1 is missing entirely.
	pod0 := rolloutPodObj(clusterShardPodName(rolloutOwnerLR, 0, 0), "revX", false, readySince)
	pod0.Status.Conditions[0].Status = corev1.ConditionTrue

	in := buildShardRolloutInput(lr, 0, 1, "want",
		rolloutSTS("have", 5, 4, "rev1", "rev2", new(int32(1))),
		map[string]*corev1.Pod{pod0.Name: pod0}, nil, now)

	if in.AppliedPartition == nil || *in.AppliedPartition != 1 {
		t.Errorf("applied partition = %v, want 1 (the cursor is the StatefulSet's own field)", in.AppliedPartition)
	}
	if in.AppliedHash != "have" || in.DesiredHash != "want" {
		t.Errorf("hashes = %q/%q, want have/want", in.AppliedHash, in.DesiredHash)
	}
	if in.Generation != 5 || in.ObservedGeneration != 4 || in.CurrentRevision != "rev1" || in.UpdateRevision != "rev2" {
		t.Errorf("statefulset facts not carried through: %+v", in)
	}
	if len(in.Pods) != 1 || in.Pods[0].Ordinal != 0 {
		t.Fatalf("expected only ordinal 0 to be present, got %+v", in.Pods)
	}
	if in.Pods[0].Ready {
		t.Error("clause (b) must be the kubelet's verdict on the REDIS container, not the pod-level Ready condition")
	}
	if in.Pods[0].Revision != "revX" {
		t.Errorf("revision = %q, want revX", in.Pods[0].Revision)
	}
	if !in.Pods[0].ReadySince.Equal(readySince) {
		t.Errorf("readySince = %v, want %v", in.Pods[0].ReadySince, readySince)
	}

	// No StatefulSet at all: no cursor, no facts, no panic.
	if empty := buildShardRolloutInput(lr, 0, 1, "want", nil, nil, nil, now); empty.AppliedPartition != nil {
		t.Error("a nil StatefulSet must yield a nil cursor")
	}
}

// TestPreGatherPlanOnlyHoldsOrRaises is the ADR-017 pre/post-gather split, pinned. The
// step-1 apply runs before the gather, so it has NO redundancy facts; it must therefore be
// unable to lower the partition. If it could, a shard mid-rollout would have its master
// released on every pass by the very apply that is supposed to be holding it — the LR-044
// flap, on the one field where flapping is the defect itself.
func TestPreGatherPlanOnlyHoldsOrRaises(t *testing.T) {
	lr := clusterLRWithReplicas(2) // highest ordinal 2

	preGather := func(sts *appsv1.StatefulSet) shardRolloutPlan {
		return planShardRolloutPartition(buildShardRolloutInput(lr, 0, 2, "want", sts, nil, nil, time.Now()))
	}

	// Template change: gate at the highest ordinal. The one legal raise.
	if p := preGather(rolloutSTS("have", 3, 3, "rev1", "rev1", nil)); p.Verdict != rolloutStart ||
		p.Partition == nil || *p.Partition != 2 {
		t.Errorf("first sight of a template change: verdict=%v partition=%v, want Started/2", p.Verdict, p.Partition)
	}
	// Mid-rollout at the desired template: re-emit the cursor UNCHANGED.
	for _, cursor := range []int32{2, 1} {
		p := preGather(rolloutSTS("want", 4, 4, "rev1", "rev2", &cursor))
		if p.Verdict != rolloutHold || p.Partition == nil || *p.Partition != cursor {
			t.Errorf("cursor %d: verdict=%v partition=%v, want Holding at %d", cursor, p.Verdict, p.Partition, cursor)
		}
	}
	// Settled: nothing left to gate.
	if p := preGather(rolloutSTS("want", 4, 4, "rev2", "rev2", new(int32(2)))); p.Verdict != rolloutComplete ||
		p.Partition == nil || *p.Partition != 0 {
		t.Errorf("settled: verdict=%v partition=%v, want Complete/0", p.Verdict, p.Partition)
	}
	// replicasPerShard 0: no partition field at all, and the caller's Warning verdict.
	zero := planShardRolloutPartition(buildShardRolloutInput(clusterLRWithReplicas(0), 0, 0, "want",
		rolloutSTS("have", 1, 1, "rev1", "rev1", nil), nil, nil, time.Now()))
	if zero.Verdict != rolloutUngated || zero.Partition != nil {
		t.Errorf("replicasPerShard 0: verdict=%v partition=%v, want Ungated/nil", zero.Verdict, zero.Partition)
	}
}
