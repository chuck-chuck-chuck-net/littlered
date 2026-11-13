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
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
)

// ---- availability-gated rollouts of the sentinel/failover StatefulSets (ADR-017 Amendment, LR-065) ----
//
// The sentinel-mode Redis and Sentinel StatefulSets and the failover-mode Redis StatefulSet
// use Parallel pod management, and their rolling updates relied on the StatefulSet controller
// replacing one pod at a time: delete the highest ordinal, wait until its replacement is Ready
// and has been for minReadySeconds, delete the next. From Kubernetes v1.37 the controller
// instead deletes, in one pass and with no budget check, every old-revision pod that is not
// yet available — so a template change made while pods are young (shortly after creation, a
// second edit mid-roll, a mass restart) replaces the whole instance at once, and on EmptyDir
// that is the dataset. maxUnavailable does not bound that pass.
//
// The operator therefore owns spec.updateStrategy.rollingUpdate.partition on these three
// StatefulSets, with the cluster shard cursor's planner (planShardRolloutPartition) and two
// differences, both in the input: the redundancy clause is dropped (there is no slot owner),
// and the availability clause is added (Ready for the StatefulSet's minReadySeconds) — the
// gate the controller itself applied before v1.37. Every input is Kubernetes state, so the
// whole decision is taken at apply time: unlike the cluster gate there is no Redis-level
// clause that would need a gather, hence no post-gather half.

// withAvailabilityGate stamps the change detector and renders the partition on one of the
// three availability-gated StatefulSets.
//
// The pod-template hash goes on the StatefulSet's OWN metadata, not on the pod template the
// way a cluster shard carries it: a new pod-template annotation is itself a template change,
// so every existing sentinel/failover instance would roll once on upgrade for nothing. The
// hash is computed over the template as built, and the partition lives outside the template,
// so a step of the gate never reads as a template change. nil renders no rollingUpdate block,
// which is what a StatefulSet that does not exist yet is created with.
func withAvailabilityGate(sts *appsv1.StatefulSet, partition *int32) *appsv1.StatefulSet {
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[AnnotationPodSpecHash] = computePodTemplateHash(&sts.Spec.Template)
	if partition != nil {
		p := *partition
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &p}
	}
	return sts
}

// buildStatefulSetRolloutInput assembles planShardRolloutPartition's input for one of the
// availability-gated StatefulSets. Pure — the caller does the reads.
//
// desired is the StatefulSet as this pass builds it (its hash, replica count and
// minReadySeconds are what is being applied); live is the object on the API server (its hash,
// generation, revisions and partition are the state the cursor moves from). pods is keyed by
// name and may be missing entries; it is empty when no rollout is in flight.
//
// Readiness is the POD's Ready condition, not one container's: it is the signal the
// StatefulSet controller decides availability by, and a Sentinel pod has no "redis" container.
func buildStatefulSetRolloutInput(
	desired, live *appsv1.StatefulSet, pods map[string]*corev1.Pod, now time.Time,
) shardRolloutInput {
	replicas := int32(0)
	if desired.Spec.Replicas != nil {
		replicas = *desired.Spec.Replicas
	}
	in := shardRolloutInput{
		HighestOrdinal:       int(replicas) - 1,
		MinReadySeconds:      desired.Spec.MinReadySeconds,
		SkipRedundancyClause: true,
		DesiredHash:          desired.Annotations[AnnotationPodSpecHash],
		Now:                  now,
	}
	if live != nil {
		in.AppliedHash = live.Annotations[AnnotationPodSpecHash]
		in.Generation = live.Generation
		in.ObservedGeneration = live.Status.ObservedGeneration
		in.UpdateRevision = live.Status.UpdateRevision
		in.CurrentRevision = live.Status.CurrentRevision
		if ru := live.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil {
			p := *ru.Partition
			in.AppliedPartition = &p
		}
	}
	for ord := 0; ord <= in.HighestOrdinal; ord++ {
		pod := pods[fmt.Sprintf("%s-%d", desired.Name, ord)]
		if pod == nil {
			continue
		}
		p := shardRolloutPod{Ordinal: ord, Revision: pod.Labels[labelControllerRevisionHash]}
		for i := range pod.Status.Conditions {
			if c := &pod.Status.Conditions[i]; c.Type == corev1.PodReady {
				p.Ready = c.Status == corev1.ConditionTrue
				p.ReadySince = c.LastTransitionTime.Time
				break
			}
		}
		in.Pods = append(in.Pods, p)
	}
	return in
}

// applyAvailabilityGated applies one of the three availability-gated StatefulSets with the
// partition this pass decides. build renders the StatefulSet for a given partition; it is
// called once with nil to learn what is being applied, and once more with the decision —
// the builder renders a decision, it does not make one (LR-044, ADR-017).
//
// The decision may raise the partition (first sight of a template change: gate at the highest
// ordinal), lower it by one (every pod at or above it is updated and available), or hold it.
// Server-side apply with ForceOwnership makes the built value authoritative on every pass, so
// the cursor is the live object's own partition and nothing new is persisted (ADR-006).
//
// The cursor is read UNCACHED while a rollout is in flight, and the pods with it, for LR-047's
// reason: a stale-low partition, or a cached pod that still reads Ready after it was deleted,
// releases the next pod early — the defect, through a cached read. The steady loop keeps the
// cached read (LR-043's cost argument). Errors are returned and fail the pass, which leaves
// the live partition where it is: holding is the safe direction.
func (r *LittleRedReconciler) applyAvailabilityGated(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, build func(partition *int32) *appsv1.StatefulSet,
) error {
	desired := build(nil)
	live := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, live)
	if apierrors.IsNotFound(err) {
		// Create: no rollout to gate and no cursor to preserve. Created with no partition,
		// so bootstrap is unaffected.
		return r.apply(ctx, lr, desired)
	}
	if err != nil {
		return err
	}

	inFlight := live.Annotations[AnnotationPodSpecHash] != desired.Annotations[AnnotationPodSpecHash] ||
		!statefulSetRolloutSettled(live)
	if live, err = r.refreshStatefulSetForRollout(ctx, live, inFlight); err != nil {
		return err
	}
	var pods map[string]*corev1.Pod
	if inFlight {
		if pods, err = r.rolloutPodsUncached(ctx, live.Namespace, live.Name, int(ptrInt32(desired.Spec.Replicas))-1); err != nil {
			return err
		}
	}

	plan := planShardRolloutPartition(buildStatefulSetRolloutInput(desired, live, pods, time.Now()))
	// A hold (or the ungated quarantine) restates a persisting state on every pass, so it is
	// debug-level; a start and each step are transitions and are logged.
	gateLog := r.getLogger(ctx, lr, LogCategoryRecon)
	switch plan.Verdict {
	case rolloutComplete:
	case rolloutStart, rolloutAdvance:
		gateLog.Info("StatefulSet rollout gate",
			"sts", desired.Name, "verdict", plan.Verdict, "partition", partitionValue(plan.Partition))
	default:
		gateLog.V(1).Info("StatefulSet rollout gate",
			"sts", desired.Name, "verdict", plan.Verdict, "partition", partitionValue(plan.Partition),
			"hold", plan.Hold, "holdPod", plan.HoldPod)
	}
	if plan.Verdict == rolloutAdvance {
		r.getLogger(ctx, lr, LogCategoryAudit).Info(
			"StatefulSet rollout gate: every pod at or above the partition is updated and available, releasing the next pod",
			"sts", desired.Name, "partition", partitionValue(plan.Partition))
	}
	return r.apply(ctx, lr, build(plan.Partition))
}

func ptrInt32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}
