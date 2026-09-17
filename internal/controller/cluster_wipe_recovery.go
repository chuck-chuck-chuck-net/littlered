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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
)

// clusterWipeRecoveryCooldown is how long the wipe-deadlock signature must persist before
// the operator recycles the stuck pods. It is set safely above the startup script's ~60s
// STEP-3 yield (plus its liveness-kill cycle), so a pod that would self-recover on its own
// is never preempted; only a genuinely stuck, redis-down cluster is acted on.
const clusterWipeRecoveryCooldown = 120 * time.Second

// containerReasonOOMKilled is the container LastTerminationState reason for an OOM kill.
const containerReasonOOMKilled = "OOMKilled"

// clusterWipeAction is what the operator should do about a suspected cluster
// total-/partial-wipe deadlock, given the observed pod health and timing. It is the
// output of the pure decision function planClusterWipeRecovery, so the whole
// gate/cooldown matrix is unit-testable without any Kubernetes or Redis I/O.
type clusterWipeAction int

const (
	// wipeClearMarker: no pod matches the recyclable signature (or no longer does).
	// Clear any WipeDeadlockSince marker and take no action.
	wipeClearMarker clusterWipeAction = iota
	// wipeStartCooldown: the signature was just observed; stamp WipeDeadlockSince and wait.
	wipeStartCooldown
	// wipeWait: the signature persists but the cooldown has not elapsed. Do nothing this pass.
	wipeWait
	// wipeRecycle: the cooldown has elapsed and the signature still holds; delete the
	// listed pods so their StatefulSets reschedule them fresh (clean EmptyDir → new node
	// identity), letting the operator's normal repair loop re-bootstrap.
	wipeRecycle
)

// clusterPodHealth is the minimal, K8s-API-derived view of one cluster pod's redis
// container that the wipe-recovery decision needs. It is built from pod status only
// (no Redis dial): the kubelet's local readiness probe is the authoritative,
// blackhole-proof signal that redis is genuinely down — and in a pure in-memory
// (EmptyDir) cluster, a redis that is down holds no data, so recycling such a pod
// cannot lose data. A pod whose redis is Ready is a possible data holder and is never
// a candidate.
type clusterPodHealth struct {
	Name string
	// RedisReady is the kubelet readiness of the redis container (local PING).
	RedisReady bool
	// Restarted is true once the redis container has restarted at least once
	// (RestartCount >= 1) — the crash/park-and-be-killed cycle of the wipe deadlock.
	Restarted bool
	// OOMKilled is true when the redis container's last termination was an OOM kill.
	// Excluded from recycling: recycling would not fix an OOM (it would just churn),
	// and it is a distinct failure mode from the wipe deadlock. Data-safe either way.
	OOMKilled bool
}

// recyclable reports whether this pod matches the wipe-deadlock signature: redis is
// down (not Ready) and has been crash-looping (restarted), and it is not an OOM kill.
func (h clusterPodHealth) recyclable() bool {
	return !h.RedisReady && h.Restarted && !h.OOMKilled
}

// clusterWipePlan is the decision returned by planClusterWipeRecovery.
type clusterWipePlan struct {
	action  clusterWipeAction
	recycle []string // pod names to delete (wipeRecycle only)
}

// planClusterWipeRecovery is the pure decision function for cluster total-/partial-wipe
// recovery. It performs no I/O. The caller invokes it only when the instance is NOT
// making progress (not all pods Ready), so being here already means the cluster is not
// healthy; this function adds the pod-level signature and the cooldown gate on top.
//
//  1. No pod matches the recyclable signature -> clear the marker, do nothing.
//  2. First observation of the signature -> start the cooldown.
//  3. Within the cooldown -> wait (a transient blip / a pod that self-recovers clears it).
//  4. Cooldown elapsed and the signature still holds -> recycle every recyclable pod.
//
// now/since/cooldown drive the persistence gate (mirrors the sentinel LeaderlessSince).
func planClusterWipeRecovery(pods []clusterPodHealth, since *time.Time, now time.Time, cooldown time.Duration) clusterWipePlan {
	var recycle []string
	for _, p := range pods {
		if p.recyclable() {
			recycle = append(recycle, p.Name)
		}
	}
	if len(recycle) == 0 {
		return clusterWipePlan{action: wipeClearMarker}
	}
	if since == nil {
		return clusterWipePlan{action: wipeStartCooldown}
	}
	if now.Sub(*since) < cooldown {
		return clusterWipePlan{action: wipeWait}
	}
	return clusterWipePlan{action: wipeRecycle, recycle: recycle}
}

// recyclableNames returns the names of the pods that match the recyclable signature
// (used for logging the cooldown/wait phases before the plan surfaces them).
func recyclableNames(pods []clusterPodHealth) []string {
	var names []string
	for _, p := range pods {
		if p.recyclable() {
			names = append(names, p.Name)
		}
	}
	return names
}

// gatherClusterPodHealth builds the K8s-API-only health view of every cluster pod. It reads
// the redis container's status (readiness, restart count, last-termination reason) — no Redis
// dial — so the decision rests on the kubelet's authoritative local view (blackhole-proof),
// not the operator's remote reachability. A missing pod (not yet created) is not recyclable.
func (r *LittleRedReconciler) gatherClusterPodHealth(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) []clusterPodHealth {
	cluster := littleRed.Spec.Cluster
	if cluster == nil {
		return nil
	}
	refs := ClusterPodRefs(littleRed.Name, cluster.Shards, clusterReplicasPerShard(cluster))
	health := make([]clusterPodHealth, 0, len(refs))
	for _, ref := range refs {
		h := clusterPodHealth{Name: ref.Name}
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: littleRed.Namespace}, pod); err == nil {
			for i := range pod.Status.ContainerStatuses {
				cs := &pod.Status.ContainerStatuses[i]
				if cs.Name != ComponentRedis {
					continue
				}
				h.RedisReady = cs.Ready
				h.Restarted = cs.RestartCount >= 1
				if cs.LastTerminationState.Terminated != nil &&
					cs.LastTerminationState.Terminated.Reason == containerReasonOOMKilled {
					h.OOMKilled = true
				}
			}
		}
		health = append(health, h)
	}
	return health
}

// recoverClusterWipeDeadlock detects and, past the cooldown, breaks a cluster total-/partial-
// wipe deadlock: cluster pods stuck not-Ready and crash-looping (redis down → holding no data
// in a pure in-memory cluster) that the operator cannot otherwise reach or repair. It mutates
// littleRed.Status.Cluster.WipeDeadlockSince (persisted by the caller's Status().Update) to
// arm/clear the cooldown, and when the cooldown has elapsed it deletes the stuck pods so their
// StatefulSets reschedule them fresh (clean EmptyDir → new node identity), after which the
// normal repair loop re-bootstraps. It NEVER touches a Ready pod (a possible data holder). It
// is called only from the not-all-Ready branch of reconcileCluster. See ADR-008.
func (r *LittleRedReconciler) recoverClusterWipeDeadlock(ctx context.Context, littleRed *littleredv1alpha1.LittleRed) error {
	health := r.gatherClusterPodHealth(ctx, littleRed)

	if littleRed.Status.Cluster == nil {
		littleRed.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{}
	}
	var since *time.Time
	if littleRed.Status.Cluster.WipeDeadlockSince != nil {
		t := littleRed.Status.Cluster.WipeDeadlockSince.Time
		since = &t
	}

	now := time.Now()
	plan := planClusterWipeRecovery(health, since, now, clusterWipeRecoveryCooldown)

	stateLog := r.getLogger(ctx, littleRed, LogCategoryState)
	auditLog := r.getLogger(ctx, littleRed, LogCategoryAudit)

	switch plan.action {
	case wipeClearMarker:
		littleRed.Status.Cluster.WipeDeadlockSince = nil
	case wipeStartCooldown:
		stateLog.Info("Cluster wipe-deadlock signature observed; arming recovery cooldown",
			"stuckPods", recyclableNames(health), "cooldown", clusterWipeRecoveryCooldown.String())
		t := metav1.NewTime(now)
		littleRed.Status.Cluster.WipeDeadlockSince = &t
	case wipeWait:
		stateLog.Info("Cluster wipe-deadlock persists; waiting out recovery cooldown",
			"stuckPods", recyclableNames(health))
	case wipeRecycle:
		auditLog.Info("Breaking cluster wipe deadlock: recycling stuck redis-down pods so their StatefulSets reschedule them fresh",
			"pods", plan.recycle)
		for _, name := range plan.recycle {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: littleRed.Namespace}}
			if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
				auditLog.Error(err, "Failed to recycle stuck pod", "pod", name)
				return err
			}
		}
		littleRed.Status.Cluster.WipeDeadlockSince = nil
	}
	return nil
}
