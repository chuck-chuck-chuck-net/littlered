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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// The wiring of ADR-020's declared-operation branch into sentinel mode.
//
// Three files, three jobs, and the split is the design: operation_registry.go says WHICH
// spec fields are heavy, operation_plan.go decides WHAT happens, and this file is the
// only place that touches the cluster on their behalf. Nothing here decides anything.
//
// The three traps ADR-020 names for this milestone, and where each is discharged:
//
//  1. LR-050's `rolling` gate stays exactly as it is. It is not read here, not replaced
//     by "an operation is in progress", and not deleted as redundant. It names a FACT —
//     our own StatefulSet is unsettled — so it covers the image bump, the drain and the
//     eviction that nobody declares, none of which this mechanism can ever see. That the
//     two fire together during a rename is what makes unifying them tempting and wrong.
//  2. No planner gains a "skip during an operation" clause. The suppression is one
//     `return` in reconcileSentinelCluster, placed where Rule A returns; planForsaken,
//     planQuarantine, planLeaderlessRecovery, planGhostMasterRecovery and
//     planStaleMasterNames are untouched and every one of their tables passes unedited.
//  3. Exactly one call site reads status.acknowledgedOperations: buildOperationInput,
//     below. The record answers "was this asked for?" and nothing else reads it.

// operationCandidates is the registry, filtered to this instance and fingerprinted —
// the Candidates half of ADR-020's decision input.
//
// Applies is the only filter, and the registry entry owns what it means (mode, feature
// flags). An operation this instance cannot have is not a candidate at all, which is
// what makes a Requires edge on it vacuously satisfied rather than a deadlock.
func operationCandidates(lr *littleredv1alpha1.LittleRed, ops []heavyOperation) []operationCandidate {
	in := fingerprintInput{LR: lr, UID: lr.UID}
	var out []operationCandidate
	for _, op := range ops {
		if op.Applies == nil || !op.Applies(lr) {
			continue
		}
		out = append(out, operationCandidate{
			Name:        op.Name,
			Fingerprint: op.Fingerprint(in),
			Requires:    op.Requires,
			StallAfter:  op.StallAfter,
		})
	}
	return out
}

// buildOperationInput assembles everything planOperation reads. Pure: the caller
// supplies the settledness it measured and the clock.
//
// THIS IS THE ONE CALL SITE THAT READS status.acknowledgedOperations (ADR-020 trap 3).
// The record identifies which VALUE completed, never which field, and it is a keyed
// digest precisely so that no later rule can read it as "the previous master name" and
// quietly walk ADR-018's refusal back.
func buildOperationInput(
	lr *littleredv1alpha1.LittleRed, settled bool, now time.Time,
) operationInput {
	acks := make(map[string]string, len(lr.Status.AcknowledgedOperations))
	for _, a := range lr.Status.AcknowledgedOperations {
		acks[a.Name] = a.Fingerprint
	}
	return operationInput{
		Candidates: operationCandidates(lr, heavyOperations),
		Acks:       acks,
		// A quarantined instance has no pods, and a replicas:0 StatefulSet reads
		// SETTLED — so an operation allowed to proceed here would acknowledge work no
		// pod ever executed. Held, and REPORTED as held (row 1).
		Quarantined: lr.Status.QuarantinedSince != nil,
		// A brand new instance is by construction already in the state its spec asks
		// for. Unreachable from reconcileSentinelCluster, which returns above while
		// BootstrapRequired holds — row 3 catches the same instance on its first
		// post-bootstrap pass — but passed honestly rather than hardcoded false.
		Bootstrapping: lr.Status.Phase == "" || lr.Status.BootstrapRequired,
		Settled:       settled,
		Active:        lr.Status.Operation,
		Now:           now,
	}
}

// instanceStatefulSetsSettled reports whether EVERY StatefulSet this sentinel-mode
// instance owns has finished rolling — the Redis one and the Sentinel one.
//
// Both, and that is not thoroughness for its own sake: the Sentinel StatefulSet is what
// carries the master name the rename is about, so acknowledging while it is still being
// replaced would call the operation complete on pods that never ran under the new value.
//
// Read UNCACHED, for the same reason LR-047 reads the rollout cursor uncached and LR-050
// reads its attribution gate uncached: the dangerous direction is a stale read that says
// "settled" while a roll is in flight, and the informer necessarily lags BEHIND. Here a
// stale-settled read acknowledges an operation early and hands the exit edge into the
// churn LR-050 is about (row 7). A StatefulSet we cannot read at all counts as NOT
// settled, which withholds the acknowledgment — the conservative direction.
//
// A NotFound is a definitive answer rather than an unknown, and it must read as settled:
// otherwise an instance whose StatefulSets have not been created yet could never
// acknowledge anything. (Unreachable on the normal path — reconcileSentinel applies both
// well before this runs.)
func (r *LittleRedReconciler) instanceStatefulSetsSettled(
	ctx context.Context, lr *littleredv1alpha1.LittleRed,
) bool {
	for _, name := range []string{statefulSetName(lr), sentinelStatefulSetName(lr)} {
		sts := &appsv1.StatefulSet{}
		err := r.apiReader().Get(ctx, types.NamespacedName{Name: name, Namespace: lr.Namespace}, sts)
		switch {
		case err == nil:
			if !statefulSetRolloutSettled(sts) {
				return false
			}
		case apierrors.IsNotFound(err):
			continue
		default:
			return false
		}
	}
	return true
}

// reconcileOperation runs planOperation and persists everything it decided: the
// acknowledgments, status.operation, the OperationInProgress condition and at most one
// event per transition. It never runs a driver — the driver for registry v1 is Rule 0
// plus Rule N, which the caller has already run, because a driver's completion is an
// INPUT to this decision (rows 7 to 10) rather than an output of it.
//
// The returned plan's Run is what the caller suppresses healing on.
func (r *LittleRedReconciler) reconcileOperation(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, in operationInput,
) (operationPlan, error) {
	plan := planOperation(in)
	return plan, r.applyOperationStatus(ctx, lr, plan, metav1.NewTime(in.Now))
}

// operationConditionFor renders the plan as the condition the operator publishes.
//
// True means an operation is declared and unfinished — running, blocked, stalled, or
// held by a quarantine. That is a normal, expected state and never affects Ready: an
// instance mid-rename is not an unhealthy instance, and conflating the two trains an
// operator to ignore the signal that matters. It is nonetheless reported loudly, because
// the instance is deliberately not being healed while it holds and there is no auto-exit
// timer (ADR-017: a timer is the defect with a delay).
func operationConditionFor(plan operationPlan) metav1.Condition {
	status := metav1.ConditionFalse
	if plan.Report != "" {
		status = metav1.ConditionTrue
	}
	msg := plan.Detail
	if msg == "" {
		msg = "No declared heavy operation is in progress."
	}
	return metav1.Condition{
		Type:    littleredv1alpha1.ConditionOperationInProgress,
		Status:  status,
		Reason:  plan.Reason,
		Message: msg,
	}
}

// upsertOperationAcks writes one completion row per acknowledged candidate, in place.
// The list is bounded by the registry — one row per operation NAME — so it needs no
// expiry, and an age-based one would be a defect rather than hygiene: a missing row is
// re-seeded (row 3), never re-run.
func upsertOperationAcks(
	existing []littleredv1alpha1.OperationAck, ack []operationCandidate, at metav1.Time,
) []littleredv1alpha1.OperationAck {
	out := append([]littleredv1alpha1.OperationAck(nil), existing...)
	for _, c := range ack {
		replaced := false
		for i := range out {
			if out[i].Name == c.Name {
				out[i].Fingerprint = c.Fingerprint
				out[i].AcknowledgedAt = at
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, littleredv1alpha1.OperationAck{
				Name: c.Name, Fingerprint: c.Fingerprint, AcknowledgedAt: at,
			})
		}
	}
	return out
}

// applyOperationStatus persists the plan. Idempotent and quiet: this runs on every
// sentinel pass, and a status write every 2s for an unchanged verdict is churn of
// exactly the kind LR-042 removed.
//
// The write goes through retry.RetryOnConflict on a re-fetched object (rule §7.1),
// matching setForsaken and setStaleMasterNameCondition, and mirrors what it persisted
// back onto the in-memory object — updateSentinelStatus runs later in the SAME pass and
// writes the whole status back from it, so a missing mirror silently reverts what was
// just written (LR-044's defect, which must not be reintroduced).
func (r *LittleRedReconciler) applyOperationStatus(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, plan operationPlan, at metav1.Time,
) error {
	cond := operationConditionFor(plan)
	prev := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionOperationInProgress)

	desiredOp := operationStatusFor(plan, lr.Status.Operation, at)
	unchanged := prev != nil && prev.Status == cond.Status && prev.Reason == cond.Reason &&
		prev.Message == cond.Message && len(plan.Ack) == 0 &&
		operationStatusEqual(lr.Status.Operation, desiredOp)
	if unchanged {
		return nil
	}
	// One event per TRANSITION, never per reconcile (LR-042). A transition is a change
	// of (operation, reason): the same operation moving Running -> Stalled is news, and
	// the same operation reporting Running for the twentieth pass is not.
	changed := prev == nil || prev.Reason != cond.Reason ||
		operationName(lr.Status.Operation) != operationName(desiredOp)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		if len(plan.Ack) > 0 {
			latest.Status.AcknowledgedOperations =
				upsertOperationAcks(latest.Status.AcknowledgedOperations, plan.Ack, at)
		}
		// Re-derive against the object we are actually writing, so the StartedAt clock
		// is carried from what is persisted rather than from a stale in-memory copy.
		latest.Status.Operation = operationStatusFor(plan, latest.Status.Operation, at)
		meta.SetStatusCondition(&latest.Status.Conditions, cond)
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		lr.Status.Conditions = latest.Status.Conditions
		lr.Status.Operation = latest.Status.Operation
		lr.Status.AcknowledgedOperations = latest.Status.AcknowledgedOperations
		return nil
	}); err != nil {
		return err
	}

	if !changed {
		return nil
	}
	switch plan.Reason {
	case operationReasonBlocked, operationReasonStalled:
		// Both are permanent until a human acts, by design (ADR-017 applied twice), so
		// both are Warnings.
		r.event(lr, corev1.EventTypeWarning, plan.Reason, cond.Message)
	case operationReasonRunning, operationReasonQuarantined:
		r.event(lr, corev1.EventTypeNormal, plan.Reason, cond.Message)
	}
	return nil
}

// operationStatusFor renders status.operation, preserving the StartedAt of an operation
// that is already running. That clock anchors StallAfter, so restarting it every pass
// would make a stall unreportable; attributing ANOTHER operation's clock to this one
// would manufacture a stall out of a stale monitoring field, which is why it is only
// carried forward when the name matches.
func operationStatusFor(
	plan operationPlan, current *littleredv1alpha1.OperationStatus, at metav1.Time,
) *littleredv1alpha1.OperationStatus {
	if plan.Report == "" {
		return nil
	}
	started := at
	if current != nil && current.Name == plan.Report && !current.StartedAt.IsZero() {
		started = current.StartedAt
	}
	return &littleredv1alpha1.OperationStatus{
		Name:      plan.Report,
		StartedAt: started,
		Reason:    plan.Reason,
		Pending:   plan.Pending,
	}
}

func operationName(s *littleredv1alpha1.OperationStatus) string {
	if s == nil {
		return ""
	}
	return s.Name
}

func operationStatusEqual(a, b *littleredv1alpha1.OperationStatus) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Name != b.Name || a.Reason != b.Reason || len(a.Pending) != len(b.Pending) {
		return false
	}
	for i := range a.Pending {
		if a.Pending[i] != b.Pending[i] {
			return false
		}
	}
	return true
}

// operationDriverReport maps a driver's verdict onto the two flags planOperation reads.
//
// For registry v1 the driver is the code that already ships — Rule 0 (bare-Sentinel
// re-registration) then Rule N (the stale-master-name prune) — and its verdict is Rule
// N's own plan. No new healing logic exists anywhere in this mechanism, and nothing here
// changes what Rule N decides: this reads a value the planner already computes.
//
//	Complete ⟺ Converged: every Sentinel monitors exactly the desired name and nothing
//	           else, which IS the rename's desired state.
//	Blocked  ⟸ Foreign — a capture is in evidence and ADR-016 owns the instance — and
//	           every Deferred gate EXCEPT G6. Never auto-skipped: head-of-line blocking
//	           here is correct.
//	neither  ⟸ Pruning (work in flight) and Deferred by G6.
//
// G6 is the carve-out and it is not a convenience. "No Sentinel carries the desired name
// yet; Rule 0 registers it next pass" is discharged by the operator's own next pass, by
// design — so it is PROGRESS, and it fires on the first pass of every ordinary rename
// (measured on t3e: one Warning per rename, on the happy path, before this). A check that
// cries wolf on the happy path is a check nobody reads, and ADR-020's whole loud-condition
// story rests on Blocked meaning something. This is ADR-017's rule verbatim: a cluster pod
// that is attached but link-down is mid-full-sync, hence genuine progress, and
// ClusterRolloutBlocked is deliberately never raised for it.
//
// G1-G5 stay Blocked. Each names a state that persists until something OUTSIDE this rule
// changes — an empty desired name, no living master of ours, a failover in flight, a lost
// quorum, an address we cannot attribute — which is exactly what Blocked is for.
func operationDriverReport(plan StaleMasterNamePlan) (done, blocked bool) {
	switch plan.Reason {
	case staleNamesConverged:
		return true, false
	case staleNamesForeign:
		return false, true
	case staleNamesDeferred:
		return false, plan.Gate != staleGateRule0Pending
	default:
		// Pruning, and anything a future contributor adds: work in flight, or a verdict
		// nobody has taught this function about. Neither may read as Complete —
		// acknowledging on a value nobody defined is the acknowledge-on-sight failure by
		// another route.
		return false, false
	}
}
