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

	appsv1 "k8s.io/api/apps/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

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
