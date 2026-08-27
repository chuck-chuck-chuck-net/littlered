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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// labelControllerRevisionHash is the label the StatefulSet controller stamps on each pod
// with the ControllerRevision (pod template revision) that pod was created from. Named
// here rather than repeated as a literal because the rollout gate's clause (a) — "this pod
// has actually been replaced" — is exactly a comparison of this label against the
// StatefulSet's UpdateRevision.
const labelControllerRevisionHash = "controller-revision-hash"

// gtNode is a nil-safe lookup into a gather's node map. The rollout gate's input builder is
// deliberately callable with gt == nil (the pre-gather apply), so this must not panic.
func gtNode(gt *redisclient.ClusterGroundTruth, podName string) *redisclient.ClusterNodeState {
	if gt == nil {
		return nil
	}
	return gt.Nodes[podName]
}

// statefulSetRolloutSettled reports whether a StatefulSet has fully converged on its desired
// pod template: the controller has observed the latest spec (ObservedGeneration ==
// Generation), no rollout is in progress (UpdateRevision == CurrentRevision), and every
// replica is updated and ready.
//
// Cluster mode uses it to serialize rollouts across shards (LR-021): it only rolls the next
// shard once the current one is settled, so an operator-driven template change never restarts
// more than one shard's pods at a time — restoring the global one-pod-at-a-time serialization
// the single pre-0.3.0 StatefulSet gave for free.
//
// Sentinel mode uses the same predicate for a different question (LR-050): while our own
// Redis StatefulSet is not settled, the operator does not ATTRIBUTE addresses, because that
// is precisely the window in which an address that is no longer one of our pods may be a pod
// of ours that has just left. Note that it is deliberately broader than "a template rollout
// is in flight": a deleted, crash-looping or merely not-yet-Ready pod also fails the replica
// clauses, and those are the same states in which a departed address of ours is in the air.
// It is NOT mode-specific — hence the mode-neutral name; it was `statefulSetRolloutSettled`
// until sentinel mode needed it, and a second copy would have been the LR-045 mistake.
func statefulSetRolloutSettled(sts *appsv1.StatefulSet) bool {
	if sts == nil || sts.Spec.Replicas == nil {
		return false
	}
	want := *sts.Spec.Replicas
	st := sts.Status
	return st.ObservedGeneration == sts.Generation &&
		st.UpdateRevision != "" &&
		st.UpdateRevision == st.CurrentRevision &&
		st.UpdatedReplicas == want &&
		st.ReadyReplicas == want &&
		st.Replicas == want
}

// ---- state-gated intra-shard rollout (ADR-017, LR-047) ----

type shardRolloutVerdict string

// shardRolloutVerdict is what this pass decided about one shard's intra-shard rollout.
const (
	// rolloutUngated: replicasPerShard == 0, so no rollout can be made safe and gating is
	// vacuous. The caller warns that the update will lose each shard's data, and proceeds.
	rolloutUngated shardRolloutVerdict = "Ungated"
	// rolloutStart: a template change was seen for the first time — gate at the highest
	// ordinal. The only pass on which the partition may RISE.
	rolloutStart shardRolloutVerdict = "Started"
	// rolloutAdvance: every pod at or above the partition is updated, Ready and a synced
	// copy, so exactly one step down.
	rolloutAdvance shardRolloutVerdict = "Advanced"
	// rolloutHold: re-emit the partition unchanged. Availability-safe: the old pods serve on.
	rolloutHold shardRolloutVerdict = "Holding"
	// rolloutComplete: the shard has settled on the desired template; nothing left to gate.
	rolloutComplete shardRolloutVerdict = "Complete"
)

type shardRolloutHold string

// shardRolloutHold names the clause a hold is waiting on, for the operational signal.
const (
	holdNone shardRolloutHold = ""
	// holdPodAbsent: the StatefulSet has deleted this ordinal and not recreated it yet.
	holdPodAbsent shardRolloutHold = "PodAbsent"
	// holdRevision: clause (a) — not yet at UpdateRevision, i.e. not yet replaced.
	holdRevision shardRolloutHold = "PodNotUpdated"
	// holdReadiness: clause (b) — the kubelet does not call this pod's redis Ready.
	holdReadiness shardRolloutHold = "PodNotReady"
	// holdRedundancy: clause (c) — not a link-up replica of the shard's slot owner. THE
	// clause: this is the state the 2026-08-23 run took a shard's master down in.
	holdRedundancy shardRolloutHold = "PodNotSyncedReplica"
	// holdSettling: every clause is satisfied at partition 0 and the StatefulSet is finishing.
	holdSettling shardRolloutHold = "AwaitingRolloutCompletion"
)

// clusterRolloutReattachBudget is how long a replaced pod may sit Ready with NO attachment to
// its shard's slot owner before the hold is reported as a stall. It governs REPORTING ONLY —
// acting on a timer is precisely what ADR-017 removes — so being wrong about it costs a
// condition message and never a pod.
//
// It is bounded against something real rather than picked. What has to happen in this window is
// the operator's own reattach: FORGET the replaced pod's old node ID, MEET the new one,
// CLUSTER REPLICATE it. All of that runs on the FAST 2s cadence for the whole rollout, because
// a not-yet-reattached pod is an empty master, so HasEmptyMasters() is true, so IsHealthy is
// false — LR-014's clause, doing exactly the job it was added for — with gossip converging
// ~1-2s after the MEET. 120s is therefore ~60 passes of a loop that needs a handful, and it
// matches the existing cluster-mode precedent status.cluster.wipeDeadlockSince (LR-023).
//
// What it deliberately does NOT have to cover is the full sync, which is dataset-dependent and
// genuinely unbounded. That case is excluded structurally instead of by inflating the number:
// an attached-but-link-down pod is never reported blocked. See podStalled.
const clusterRolloutReattachBudget = 120 * time.Second

// shardRolloutPod is one shard pod's facts, as data. The two redundancy booleans are computed
// by the caller against the gathered ClusterGroundTruth and passed in, so this seam stays pure:
//
//	owner := ownerOfRange(gt, shard's range)                       // internal/redis
//	SyncedWithOwner = redisclient.IsLinkUpReplicaOf(node, owner.NodeID)
//	AttachedToOwner = node.Role == RoleReplica && node.MasterNodeID == owner.NodeID
//
// SyncedWithOwner implies AttachedToOwner, but nothing here relies on the implication: Synced is
// the authority for the gate, Attached only ever refines the blocked/holding REPORT.
type shardRolloutPod struct {
	// Ordinal is the pod's StatefulSet ordinal. Shard K's master is ordinal 0.
	Ordinal int
	// Revision is the pod's controller-revision-hash label.
	Revision string
	// Ready is the kubelet's verdict on the redis container (redisContainerReady) — the
	// blackhole-proof signal of LR-023, never the operator's own dial (LR-017).
	Ready bool
	// ReadySince is when readiness last transitioned, from the pod's own Ready condition
	// LastTransitionTime. Zero means unknown, which is never treated as evidence.
	ReadySince time.Time
	// AttachedToOwner: this pod is a replica of the shard's slot owner, link state aside.
	AttachedToOwner bool
	// SyncedWithOwner: clause (c) — a link-up replica of the shard's slot owner (LR-025).
	SyncedWithOwner bool
	// IsOwner: this pod IS the shard's slot owner. Reporting only.
	IsOwner bool
}

// shardRolloutInput is everything planShardRolloutPartition decides from: one shard StatefulSet,
// its pods, and this pass's clock. No client, no context, no reads.
type shardRolloutInput struct {
	ShardIdx int
	// ReplicasPerShard is spec.cluster.replicasPerShard, so the highest ordinal is known
	// (a shard STS is sized 1+replicasPerShard). 0 means no redundancy exists at all.
	ReplicasPerShard int
	// DesiredHash / AppliedHash are AnnotationPodSpecHash on the desired and the live pod
	// template — LR-021's cache-safe change detector, compared as a stored value.
	DesiredHash string
	AppliedHash string
	// Generation / ObservedGeneration close LR-021's cache-lag race: a status that has not
	// observed the current spec describes the previous one.
	Generation         int64
	ObservedGeneration int64
	UpdateRevision     string
	CurrentRevision    string
	// AppliedPartition is the live StatefulSet's spec.updateStrategy.rollingUpdate.partition,
	// and it IS the cursor — nothing else persists this decision. nil is meaningful: it is
	// today's ungated state, and Kubernetes reads an unset partition as 0, so this seam does
	// too.
	AppliedPartition *int32
	Pods             []shardRolloutPod
	Now              time.Time
}

// shardRolloutPlan is the decision. Partition is what the builder renders; everything else is
// what the operator says about it.
type shardRolloutPlan struct {
	Verdict shardRolloutVerdict
	// Partition is the value to stamp on the shard StatefulSet, or nil for "set no partition
	// at all" (the ungated replicasPerShard == 0 case).
	Partition *int32
	// Hold names the first unsatisfied clause, and HoldPod the ordinal it was observed on
	// (-1 when nothing is holding) — the message material for the operational signal.
	Hold    shardRolloutHold
	HoldPod int
	// Blocked / BlockedPods are ADVISORY: they change what the operator reports
	// (ClusterRolloutBlocked and its Warning), never what it does. Partition is identical
	// whether a hold is blocked or not.
	Blocked     bool
	BlockedPods []int
}

func (in shardRolloutInput) highestOrdinal() int { return in.ReplicasPerShard }

func (in shardRolloutInput) currentPartition() int {
	p := 0
	if in.AppliedPartition != nil {
		p = int(*in.AppliedPartition)
	}
	if p < 0 {
		p = 0
	}
	if h := in.highestOrdinal(); p > h {
		p = h
	}
	return p
}

// planShardRolloutPartition computes the `spec.updateStrategy.rollingUpdate.partition` for one
// cluster shard's StatefulSet, so the shard's master is never taken down while no synced copy of
// its slots exists (ADR-017, LR-047). It is pure and I/O-free: everything it needs is passed in.
//
// Why a partition at all. Nothing gates today's intra-shard handover on the replacement actually
// being a copy. `buildClusterShardStatefulSet` sets no `rollingUpdate` block, so the sequence
// belongs entirely to the StatefulSet controller: delete the highest ordinal, wait until it is
// Ready and has been for `minReadySeconds`, delete the next. Both of those gates are blind to
// redundancy — readiness is `[ ! -f /data/bootstrap-in-progress ]` plus a local PING, which
// asserts that a process answers on a socket and says nothing about cluster membership, slot
// ownership or a replication link. But a replaced pod comes back on a wiped EmptyDir (pillar
// 3.1), hence with a NEW node ID, hence needing the old ID FORGOTten, itself MEETed, CLUSTER
// REPLICATEd and full-synced — all of which only the operator does, and only in the
// `allPodsReady` branch. So the invariant actually enforced was "the replacement answers PING
// and has done so for 30s" where the invariant data safety requires is LR-025's: *the
// replacement is a link-`up` replica of this shard's slot owner*. A full-suite run on
// 2026-08-23 lost shard 1's whole dataset that way, in a rolling update that reported complete
// success (96s with zero copies of `5462-10922`).
//
// The decision, per ADR-017 Decision 1:
//
//   - **Ungated** when `replicasPerShard == 0`. With one copy per shard any rollout takes that
//     copy down; no operator-side gate can change it, and refusing would make a documented
//     topology un-upgradable. The caller turns this verdict into the Warning that says so.
//   - **Started** on the first pass that sees a template change (applied hash != desired):
//     gate at the shard's highest ordinal. This is the ONLY direction in which the value ever
//     rises, which is what makes the cursor flap-proof.
//   - **Advanced** — lower by exactly one — once EVERY pod at or above the current partition is
//     simultaneously (a) at `UpdateRevision`, (b) Ready per the kubelet (LR-023's blackhole-proof
//     signal, not the operator's dial — LR-017), and (c) a link-`up` replica of the shard's slot
//     owner (`redisclient.IsLinkUpReplicaOf`, the LR-025 predicate, one definition shared with
//     the migration planner).
//   - **Holding** otherwise: re-emit the current partition unchanged. The old pods keep serving
//     and the data is intact; a stalled rollout is availability-safe where a time-released one
//     is the lossy path this seam removes. There is deliberately no timer fallback — one would
//     be the current defect with a longer timer.
//   - **Complete** when the shard has settled (`statefulSetRolloutSettled`'s proposition,
//     computed here from the same facts): partition 0, nothing left to gate. Checked BEFORE the
//     clauses, because a settled shard's own master owns the slots and so is nobody's replica —
//     evaluating clause (c) against it would report a healthy shard as stuck.
//
// The cursor is the StatefulSet's own `partition` field; nothing new is persisted and there is
// no status field to reconcile against ADR-006. Pods BELOW the partition are ignored entirely:
// they have not been asked to update yet, so their state says nothing about the handover.
//
// Blocked vs holding — a distinction in what the operator SAYS, never in what it DOES.
// `Blocked` is advisory: it selects nothing, gates nothing, and the emitted `Partition` is
// identical either way. A hold is indistinguishable from ordinary convergence at the instant it
// starts, so something has to decide when one has become a stall worth naming, and it is derived
// from live state only — no marker, nothing persisted. A pod is reported blocked when it is at
// `UpdateRevision`, has been Ready per the kubelet for longer than `clusterRolloutReattachBudget`,
// and is not attached to the shard's slot owner AT ALL. The last clause is what keeps the signal
// honest: a pod that IS attached but whose replication link is still down is mid-full-sync, which
// is dataset-dependent and can legitimately run for minutes (ADR-017 Consequences) — that is
// progress, and it is never reported blocked however long it takes. A pod with no readiness
// timestamp is likewise never reported blocked: unknown is not evidence.
func planShardRolloutPartition(in shardRolloutInput) shardRolloutPlan {
	plan := shardRolloutPlan{HoldPod: -1}

	// replicasPerShard == 0: no redundancy to wait for, so gating is vacuous (ADR-017 Decision 3).
	if in.ReplicasPerShard <= 0 {
		plan.Verdict = rolloutUngated
		return plan
	}

	// First sight of a template change. The one legal raise, and it must be decided before the
	// settled check: the shard is still settled on the OLD template at this instant.
	if in.AppliedHash != in.DesiredHash {
		plan.Verdict = rolloutStart
		plan.Partition = new(int32(in.highestOrdinal()))
		return plan
	}

	partition := int32(in.currentPartition())
	plan.Partition = &partition

	// Settled: the StatefulSet controller has observed this template (closing the cache-lag race
	// LR-021 named) and no roll is in progress. With partition > 0 the controller never advances
	// CurrentRevision, so this cannot be true mid-rollout.
	if in.ObservedGeneration == in.Generation &&
		in.UpdateRevision != "" && in.UpdateRevision == in.CurrentRevision {
		plan.Verdict = rolloutComplete
		plan.Partition = new(int32(0))
		return plan
	}

	byOrdinal := make(map[int]shardRolloutPod, len(in.Pods))
	for _, p := range in.Pods {
		byOrdinal[p.Ordinal] = p
	}

	// Ascending from the partition: the pod AT the partition is the one the StatefulSet
	// controller is working on right now, so it is the informative one to name.
	for ord := in.currentPartition(); ord <= in.highestOrdinal(); ord++ {
		pod, ok := byOrdinal[ord]
		switch {
		case !ok:
			plan.hold(holdPodAbsent, ord)
		case in.UpdateRevision == "" || pod.Revision != in.UpdateRevision:
			plan.hold(holdRevision, ord)
		case !pod.Ready:
			plan.hold(holdReadiness, ord)
		case !pod.SyncedWithOwner:
			plan.hold(holdRedundancy, ord)
			if in.podStalled(pod) {
				plan.Blocked = true
				plan.BlockedPods = append(plan.BlockedPods, ord)
			}
		}
	}

	if plan.Hold != holdNone {
		plan.Verdict = rolloutHold
		return plan
	}

	// Every pod at or above the partition is updated, Ready and a synced copy.
	if partition > 0 {
		plan.Verdict = rolloutAdvance
		plan.Partition = new(partition - 1)
		return plan
	}

	// Already at 0 with every clause satisfied, but the StatefulSet has not settled yet — the
	// last convergence window. Hold at 0; the next pass reports Complete.
	plan.Verdict = rolloutHold
	plan.Hold = holdSettling
	return plan
}

// hold records the FIRST failing clause (lowest ordinal wins) without stopping the scan, so the
// blocked survey still covers every pod at or above the partition.
func (p *shardRolloutPlan) hold(reason shardRolloutHold, ordinal int) {
	if p.Hold == holdNone {
		p.Hold, p.HoldPod = reason, ordinal
	}
}

// podStalled reports whether a pod has been Ready long enough, with no attachment to the shard's
// slot owner at all, that the hold is worth naming as a stall. Reporting only — see
// planShardRolloutPartition's doc comment.
func (in shardRolloutInput) podStalled(pod shardRolloutPod) bool {
	// A pod that IS the shard's slot owner is a replica of nobody by construction — it is the
	// thing everything else attaches TO — so its failing clause (c) is structural, not a stall.
	// This is reachable on the ordinary path, not only in the exotic case ADR-017 anticipated:
	// once the partition reaches 0 the StatefulSet deletes the master, its preStop hands
	// mastership to the replica, and that promoted replica sits inside this survey with a
	// ReadySince from when IT was replaced — deliberately long ago, because waiting for it to
	// sync is what the gate does. Observed firing falsely on t3e (2026-08-25) 8s before the
	// rollout completed normally.
	if pod.IsOwner || pod.AttachedToOwner || pod.ReadySince.IsZero() {
		return false
	}
	return !in.Now.Before(pod.ReadySince.Add(clusterRolloutReattachBudget))
}

// ---- turning live objects into the seam's input (ADR-017 wiring, LR-047) ----

// shardSlotOwner resolves "the shard's slot owner": the node that currently serves shard
// K's slot range, as the cluster itself reports it. It is the master side of clause (c) —
// the thing a replaced pod has to be a link-`up` replica OF before the StatefulSet may take
// the next pod down.
//
// It keys on the START of the shard's expected aligned range (redisclient.GenerateSlotRanges,
// the same pure function that assigns them) and accepts any owner whose ranges CONTAIN that
// slot, rather than requiring an exact range match. Exact matching would silently return
// "no owner" on a fragmented or mid-reshard range (LR-018) — and no owner means clause (c)
// can never be satisfied, i.e. a permanent stall on a cluster that is merely resharding.
// Containment degrades to the exact case and covers the rest.
//
// Iteration is over ClusterPodRefs rather than the gt.Nodes map so the answer is
// deterministic when two nodes transiently claim the same slot (a mid-failover view): map
// order would make the gate's verdict depend on hash seeding.
func shardSlotOwner(gt *redisclient.ClusterGroundTruth, name string, shards, replicasPerShard, shardIdx int) *redisclient.ClusterNodeState {
	if gt == nil {
		return nil
	}
	ranges := redisclient.GenerateSlotRanges(shards)
	if shardIdx < 0 || shardIdx >= len(ranges) {
		return nil
	}
	want := ranges[shardIdx].Start
	for _, ref := range ClusterPodRefs(name, shards, replicasPerShard) {
		n := gt.Nodes[ref.Name]
		if n == nil || n.Role != RoleMaster {
			continue
		}
		for _, s := range n.Slots {
			st, en, err := redisclient.ParseSlotRange(s)
			if err == nil && want >= st && want <= en {
				return n
			}
		}
	}
	return nil
}

// buildShardRolloutInput assembles planShardRolloutPartition's input from live objects. It
// is pure — the caller does the reads — so the whole translation (ordinal parsing, which
// readiness signal counts, and the two redundancy booleans) is unit-testable without a
// cluster.
//
// pods is keyed by pod name and may be missing entries: an ordinal the StatefulSet has
// deleted and not recreated is simply absent, which the seam reads as holdPodAbsent.
//
// gt may be nil. That is the PRE-GATHER call: with no pods and no ground truth the seam
// takes its structural branches only (template change ⇒ gate at the highest ordinal;
// settled ⇒ 0; otherwise re-emit the cursor unchanged), which is exactly what the
// build-time apply needs and nothing more. The clause reporting from such a call is
// meaningless and the caller ignores it.
func buildShardRolloutInput(
	lr *littleredv1alpha1.LittleRed,
	shardIdx, replicasPerShard int,
	desiredHash string,
	sts *appsv1.StatefulSet,
	pods map[string]*corev1.Pod,
	gt *redisclient.ClusterGroundTruth,
	now time.Time,
) shardRolloutInput {
	in := shardRolloutInput{
		ShardIdx:         shardIdx,
		ReplicasPerShard: replicasPerShard,
		DesiredHash:      desiredHash,
		Now:              now,
	}
	if sts != nil {
		in.AppliedHash = sts.Spec.Template.Annotations[AnnotationPodSpecHash]
		in.Generation = sts.Generation
		in.ObservedGeneration = sts.Status.ObservedGeneration
		in.UpdateRevision = sts.Status.UpdateRevision
		in.CurrentRevision = sts.Status.CurrentRevision
		if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil {
			p := *ru.Partition
			in.AppliedPartition = &p
		}
	}

	shards := clusterShardCount(lr)
	owner := shardSlotOwner(gt, lr.Name, shards, replicasPerShard, shardIdx)
	ownerID := ""
	if owner != nil {
		ownerID = owner.NodeID
	}

	for ord := 0; ord <= replicasPerShard; ord++ {
		podName := clusterShardPodName(lr.Name, shardIdx, ord)
		pod := pods[podName]
		if pod == nil {
			continue
		}
		p := shardRolloutPod{
			Ordinal:  ord,
			Revision: pod.Labels[labelControllerRevisionHash],
			// The kubelet's verdict on the redis container specifically — LR-023's
			// blackhole-proof signal, never the operator's own dial (LR-017).
			Ready: redisContainerReady(pod),
		}
		// ReadySince comes from the POD-level Ready condition's LastTransitionTime, because
		// container statuses carry no timestamp at all. It is only ever used to decide
		// whether a hold has lasted long enough to be REPORTED as a stall, and a zero value
		// is never treated as evidence, so the small imprecision between "pod Ready" and
		// "redis container Ready" cannot change a partition.
		for i := range pod.Status.Conditions {
			if c := &pod.Status.Conditions[i]; c.Type == corev1.PodReady {
				p.ReadySince = c.LastTransitionTime.Time
				break
			}
		}
		// The two redundancy questions, per the shardRolloutPod contract. Synced is the
		// gate (LR-025's one shared definition of "synced"); Attached only refines the
		// blocked/holding report, and is deliberately false when the owner is unknown.
		if node := gtNode(gt, podName); node != nil && ownerID != "" {
			p.SyncedWithOwner = redisclient.IsLinkUpReplicaOf(node, ownerID)
			p.AttachedToOwner = node.Role == RoleReplica && node.MasterNodeID == ownerID
			p.IsOwner = node.NodeID == ownerID
		}
		in.Pods = append(in.Pods, p)
	}
	return in
}
