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
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
)

// availabilityGatedBuilders are the three Parallel-pod-management StatefulSets whose rollout
// the operator gates on availability (LR-065). Every one must take the partition as a
// parameter and render it, exactly as the cluster shard builder does.
func availabilityGatedBuilders() map[string]func(p *int32) *appsv1.StatefulSet {
	sentinelLR := sentinelModeLR()
	failoverLR := failoverModeLR()
	return map[string]func(p *int32) *appsv1.StatefulSet{
		"sentinel-mode Redis": func(p *int32) *appsv1.StatefulSet {
			return buildRedisStatefulSetSentinel(sentinelLR, littleredv1alpha1.SentinelRedisReplicas, p)
		},
		"Sentinel processes": func(p *int32) *appsv1.StatefulSet {
			return buildSentinelStatefulSet(sentinelLR, sentinelProcessReplicas, p)
		},
		"failover-mode Redis": func(p *int32) *appsv1.StatefulSet {
			return buildRedisStatefulSetFailover(failoverLR, p)
		},
	}
}

func sentinelModeLR() *littleredv1alpha1.LittleRed {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	return lr
}

func failoverModeLR() *littleredv1alpha1.LittleRed { return newFailoverTestLittleRed() }

func TestAvailabilityGatedBuildersRenderPartition(t *testing.T) {
	for name, build := range availabilityGatedBuilders() {
		t.Run(name, func(t *testing.T) {
			if ru := build(nil).Spec.UpdateStrategy.RollingUpdate; ru != nil {
				t.Errorf("nil partition rendered a rollingUpdate block %+v; nil must mean no block at all", ru)
			}
			for _, p := range []int32{0, 2} {
				ru := build(&p).Spec.UpdateStrategy.RollingUpdate
				if ru == nil || ru.Partition == nil || *ru.Partition != p {
					t.Errorf("partition %d rendered as %+v", p, ru)
				}
			}
		})
	}
}

// TestAvailabilityGatedTemplateHashIsOnTheStatefulSet pins where the change detector lives for
// these StatefulSets, and why. It is stamped on the StatefulSet's OWN metadata, not on the pod
// template: a new pod-template annotation would itself be a template change, and every existing
// sentinel/failover instance would roll once on upgrade for nothing. And it must not move with
// the partition, or each step of the gate would read as a fresh template change and re-raise.
func TestAvailabilityGatedTemplateHashIsOnTheStatefulSet(t *testing.T) {
	for name, build := range availabilityGatedBuilders() {
		t.Run(name, func(t *testing.T) {
			sts := build(nil)
			if _, onTemplate := sts.Spec.Template.Annotations[AnnotationPodSpecHash]; onTemplate {
				t.Errorf("pod-spec hash stamped on the POD TEMPLATE: upgrading would roll every instance")
			}
			got := sts.Annotations[AnnotationPodSpecHash]
			if want := computePodTemplateHash(&sts.Spec.Template); got == "" || got != want {
				t.Errorf("StatefulSet annotation %s = %q, want the template's hash %q", AnnotationPodSpecHash, got, want)
			}
			for _, p := range []int32{0, 1, 2} {
				if h := build(&p).Annotations[AnnotationPodSpecHash]; h != got {
					t.Errorf("partition %d moved the hash (%q != %q): the gate would restart its own rollout", p, h, got)
				}
			}
		})
	}
}

// --- buildStatefulSetRolloutInput: live objects → the seam's facts --------------------------

func gatedPod(sts string, ordinal int, rev string, ready bool, since time.Time, container string) *corev1.Pod {
	pod := &corev1.Pod{}
	pod.Name = fmt.Sprintf("%s-%d", sts, ordinal)
	pod.Labels = map[string]string{labelControllerRevisionHash: rev}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodReady, Status: status, LastTransitionTime: metav1.NewTime(since),
	}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: container, Ready: ready}}
	return pod
}

func TestBuildStatefulSetRolloutInput(t *testing.T) {
	min := int32(35)
	desired := buildSentinelStatefulSet(sentinelModeLR(), sentinelProcessReplicas, nil)
	desired.Spec.MinReadySeconds = min

	live := desired.DeepCopy()
	live.Annotations[AnnotationPodSpecHash] = "applied"
	live.Generation = 4
	live.Status.ObservedGeneration = 3
	live.Status.CurrentRevision = rolloutRevOld
	live.Status.UpdateRevision = rolloutRevNew
	live.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: new(int32(1))}

	since := rolloutNow.Add(-time.Minute)
	pods := map[string]*corev1.Pod{
		// A Sentinel pod has no "redis" container: readiness must come from the POD's
		// Ready condition — the signal the StatefulSet controller itself uses.
		desired.Name + "-0": gatedPod(desired.Name, 0, rolloutRevOld, true, since, "sentinel"),
		desired.Name + "-2": gatedPod(desired.Name, 2, rolloutRevNew, false, since, "sentinel"),
	}

	in := buildStatefulSetRolloutInput(desired, live, pods, rolloutNow)

	if in.HighestOrdinal != int(sentinelProcessReplicas)-1 {
		t.Errorf("HighestOrdinal = %d, want %d", in.HighestOrdinal, sentinelProcessReplicas-1)
	}
	if in.MinReadySeconds != min {
		t.Errorf("MinReadySeconds = %d, want %d (the desired StatefulSet's)", in.MinReadySeconds, min)
	}
	if !in.SkipRedundancyClause {
		t.Error("SkipRedundancyClause = false: these StatefulSets have no slot owner, every pod would hold forever")
	}
	if in.DesiredHash == "" || in.DesiredHash != desired.Annotations[AnnotationPodSpecHash] {
		t.Errorf("DesiredHash = %q, want the desired StatefulSet's %q", in.DesiredHash, desired.Annotations[AnnotationPodSpecHash])
	}
	if in.AppliedHash != "applied" {
		t.Errorf("AppliedHash = %q, want the LIVE StatefulSet's annotation", in.AppliedHash)
	}
	if in.Generation != 4 || in.ObservedGeneration != 3 ||
		in.CurrentRevision != rolloutRevOld || in.UpdateRevision != rolloutRevNew {
		t.Errorf("structural facts not copied from the live object: %+v", in)
	}
	if in.AppliedPartition == nil || *in.AppliedPartition != 1 {
		t.Errorf("AppliedPartition = %v, want 1 (the cursor is the live object's own field)", in.AppliedPartition)
	}
	if !in.Now.Equal(rolloutNow) {
		t.Errorf("Now = %v, want %v", in.Now, rolloutNow)
	}

	if len(in.Pods) != 2 {
		t.Fatalf("Pods = %+v, want ordinals 0 and 2 (1 is absent)", in.Pods)
	}
	byOrd := map[int]shardRolloutPod{}
	for _, p := range in.Pods {
		byOrd[p.Ordinal] = p
	}
	if p := byOrd[0]; p.Revision != rolloutRevOld || !p.Ready || !p.ReadySince.Equal(since) {
		t.Errorf("ordinal 0 = %+v, want old revision, Ready (pod condition) since %v", p, since)
	}
	if p := byOrd[2]; p.Revision != rolloutRevNew || p.Ready {
		t.Errorf("ordinal 2 = %+v, want new revision, not Ready", p)
	}
}
