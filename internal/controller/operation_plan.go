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
	"sort"
	"time"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// The reasons planOperation reports. They are the vocabulary of status.operation.reason,
// the OperationInProgress condition and the operator's events, so they are named once
// here rather than spelled at each call site.
//
// Converged and Seeded mean nothing is pending, so status.operation is absent for them;
// the other four each name a state in which a declared change exists and is either
// being carried out or deliberately held.
const (
	// The controller spells the vocabulary through the exported API constants rather
	// than keeping a second copy: they are status.operation.reason's documented values,
	// and cmd/lrctl needs them too without importing this package. Keeping the short
	// unexported names is deliberate — every call site and decision-table row reads the
	// same as before, so this is a plumbing change with no behavioural surface.
	operationReasonConverged   = littleredv1alpha1.OperationReasonConverged
	operationReasonRunning     = littleredv1alpha1.OperationReasonRunning
	operationReasonBlocked     = littleredv1alpha1.OperationReasonBlocked
	operationReasonStalled     = littleredv1alpha1.OperationReasonStalled
	operationReasonQuarantined = littleredv1alpha1.OperationReasonQuarantined
	operationReasonSeeded      = littleredv1alpha1.OperationReasonSeeded
)

// operationCandidate is one registered heavy operation, already filtered to this
// instance's mode and fingerprinted by the caller. The seam takes candidates as an
// INPUT and never consults the registry, which is what keeps the decision independent
// of which operations happen to be registered.
type operationCandidate struct {
	// Name is the registry key; it appears in status, conditions, events and lrctl.
	Name string

	// Fingerprint is the keyed digest of the value this operation's spec field
	// currently declares. Compared against the ack row of the SAME name, so the
	// comparison identifies which VALUE completed rather than which field changed.
	Fingerprint string

	// Requires names operations that must be NOT PENDING before this one may run.
	//
	// "Not pending" and NOT "has run", and the difference is the whole of it. The
	// event reading deadlocks the common case: an instance created with auth already
	// on never performs an enablement, so a rotation would wait forever for something
	// that will never happen. Under the state reading a seeded requirement satisfies
	// the edge, an absent one (its Applies is false — auth disabled) satisfies it
	// vacuously, and only a genuinely half-applied one holds its dependant, which is
	// ordinary head-of-line blocking and correct.
	//
	// Edges are assumed ACYCLIC. A cycle is a static property of the registry, so it
	// is detected and tested there, not here — this function degrades to "nothing is
	// runnable, report Blocked" rather than looping or panicking.
	Requires []string

	// StallAfter is how long this operation may run before it is reported Stalled.
	// Zero disables the report. It never ends the operation.
	StallAfter time.Duration
}

// operationInput is everything the decision reads. Pure: no client, no context, no
// clock — Now is an input.
type operationInput struct {
	// Candidates is the registry filtered by Applies to this instance and
	// fingerprinted. The seam never sees the registry itself.
	Candidates []operationCandidate

	// Acks is status.acknowledgedOperations projected to name -> fingerprint. The
	// distinction between "no row" and "a row that differs" is load-bearing: the
	// first is seeded (row 3), the second is pending work.
	Acks map[string]string

	// Quarantined is status.quarantinedSince != nil. A quarantined instance has no
	// pods, so nothing can be carried out on it — and a replicas:0 StatefulSet reads
	// SETTLED, so an unheld operation would "complete" work no pod ever executed.
	Quarantined bool

	// Bootstrapping is status.phase == "" || status.bootstrapRequired. A brand new
	// instance is already in the state its spec asks for.
	Bootstrapping bool

	// NOTE: there is deliberately NO whole-list "first observation" flag. Seeding is
	// decided PER CANDIDATE (row 3). Keying it on len(Acks) == 0 is a whole-list
	// heuristic doing a per-row job — correct for a one-entry registry, and it
	// silently re-runs a completed operation the moment there are two.

	// Settled: ALL of this instance's own StatefulSets are settled. This is the other
	// half of completion (row 7) and it is why acknowledgment is not the driver's
	// call alone.
	Settled bool

	// DriverDone / DriverBlocked are what the running driver reported THIS pass.
	DriverDone    bool
	DriverBlocked bool

	// Active is status.operation, consulted for StartedAt only — the stall clock —
	// and for the name that clock belongs to, so another operation's clock is never
	// attributed to this one. No ordering decision reads it: the pick is deterministic
	// from the candidates alone, so it is stable across passes without needing the
	// monitoring surface to be durable.
	Active *littleredv1alpha1.OperationStatus

	Now time.Time
}

// operationPlan is the decision. The caller runs a driver iff Run != "", writes an ack
// row for every entry in Ack, and renders status.operation from Report/Pending/Reason.
type operationPlan struct {
	// Run names the operation whose driver executes this pass. "" means none.
	Run string

	// Report names the operation status.operation describes. It equals Run whenever
	// something runs, and is set WITHOUT Run in exactly one case: a pending change
	// held by the quarantine (row 1) or by an unsatisfiable dependency, which must be
	// visible rather than silently withheld.
	//
	// It is a monitoring name, never an instruction. Nothing may execute a driver
	// because Report is set; the action is Run and only Run.
	Report string

	// Pending lists what is queued behind Report, in the order it will be considered.
	Pending []string

	// Reason is one of the operationReason* constants.
	Reason string

	// Ack lists the candidates whose acknowledgment must be written THIS pass — a
	// completion (row 8) or a seed (rows 2 and 3), never an observation.
	Ack []operationCandidate

	// Detail is free text for the condition message and the event. Nothing parses it.
	Detail string
}

// planOperation decides which declared heavy operation runs this pass, which is
// acknowledged, and what is reported (ADR-020). It is the decision seam: I/O-free,
// registry-free, and the only place that reads status.acknowledgedOperations.
//
// The shape of the thing: intent is diff( heavy(spec_now), heavy(spec_last_completed) )
// — a diff over the heavy projection of the DECLARATION, never over the world. The
// world's disagreement with the spec is drift, drift has many causes, and deriving
// intent from it is the exact conflation that produced LR-050.
//
// Most of the value is in the rows that DECLINE. The two seeding rows are what stop an
// operator upgrade declaring an operation for every instance in a fleet; row 1 holds a
// change an instance has no pods to perform; row 7 refuses to acknowledge work whose
// StatefulSet is still rolling. Rows 9 and 10 are ADR-017's lesson applied twice: a
// blocked queue and a stalled operation are both loud and both permanent until a human
// acts, because a timer would be the defect with a delay.
func planOperation(in operationInput) operationPlan {
	cands := append([]operationCandidate(nil), in.Candidates...)
	sort.Slice(cands, func(i, j int) bool { return cands[i].Name < cands[j].Name })

	// Three-way classification, per candidate. "No ack row" is not "pending": an
	// instance that has never been asked about an operation is not mid-change.
	var unseeded, pending []operationCandidate
	for _, c := range cands {
		ack, recorded := in.Acks[c.Name]
		switch {
		case !recorded:
			unseeded = append(unseeded, c)
		case ack != c.Fingerprint:
			pending = append(pending, c)
		}
	}

	// Row 1 — quarantined. Nothing runs and nothing is acknowledged, but a pending
	// change is still REPORTED. This row outranks every other, including the seeding
	// rows: a quarantined instance's StatefulSets are at replicas:0, which reads
	// SETTLED, so an operation allowed to proceed here would acknowledge work no pod
	// ever executed. Reporting it is ADR-020's standing requirement from LR-054 —
	// anything this mechanism withholds must say that it withheld.
	if in.Quarantined {
		if len(pending) == 0 {
			return operationPlan{Reason: operationReasonConverged}
		}
		return operationPlan{
			Report:  pending[0].Name,
			Pending: operationNames(pending[1:]),
			Reason:  operationReasonQuarantined,
			Detail: "held: the instance is quarantined and has no pods to carry " +
				pending[0].Name + " out",
		}
	}

	// Row 2 — bootstrapping. A brand new instance is, by construction, already in the
	// state its spec asks for; without this it would declare a rename it never asked
	// for, because the spec value differs from a nonexistent ack.
	if in.Bootstrapping && len(cands) > 0 {
		return operationPlan{
			Reason: operationReasonSeeded,
			Ack:    cands,
			Detail: "seeded at bootstrap: the instance already declares these values",
		}
	}

	// Row 3 — seeding, PER CANDIDATE. An already-initialized instance with no ack row
	// for a candidate is seeded, never run. The fleet-upgrade case then falls out as
	// the special case where every candidate is missing, rather than being a second
	// rule — and a missing row is harmless, re-seeded and never re-run, which is why
	// the ack list needs no expiry.
	//
	// Seeding lands BEFORE any pending operation is picked up, deliberately: one
	// action per pass, and a seeded row is what satisfies a Requires edge, so it must
	// exist before a dependant is considered.
	if len(unseeded) > 0 {
		return operationPlan{
			Reason: operationReasonSeeded,
			Ack:    unseeded,
			Detail: "seeded: no completion record yet, and the instance is already in " +
				"the state its spec asks for",
		}
	}

	// Row 4 — converged.
	if len(pending) == 0 {
		return operationPlan{Reason: operationReasonConverged}
	}

	// Rows 5 and 6 — pick what runs. Runnable means pending with every Requires edge
	// satisfied, i.e. every required operation NOT pending.
	runnable := operationRunnable(pending)
	if len(runnable) == 0 {
		// Pending, but every candidate is held by a dependency — which is also how a
		// registry cycle would present. Report it; never auto-skip an edge, because
		// running a dependant through a half-applied requirement is precisely what the
		// edge exists to prevent.
		return operationPlan{
			Report:  pending[0].Name,
			Pending: operationNames(pending[1:]),
			Reason:  operationReasonBlocked,
			Detail:  "blocked: every pending operation is waiting on a required operation",
		}
	}
	// The tiebreak is name order, and that is sufficient rather than arbitrary-and-
	// regrettable: operations with no edge between them commute — either order is
	// safe, which is what having no edge MEANS — and serialization already prevents
	// concurrency. It is also stable across passes (the pending set only shrinks), so
	// the pick does not need status.operation to remember what was running, which is
	// what keeps that field genuinely monitoring-only.
	pick := runnable[0]

	// Row 8 — the driver reported Complete AND the instance's own StatefulSets have
	// settled. Only now is the work finished, and only now is it acknowledged.
	if in.DriverDone && in.Settled {
		remaining := operationExcept(pending, pick.Name)
		plan := operationPlan{
			Reason: operationReasonConverged,
			Ack:    []operationCandidate{pick},
			Detail: "completed: " + pick.Name + " converged and the instance settled",
		}
		if len(remaining) == 0 {
			return plan
		}
		// The completion is what releases the queue — and the released operation is
		// typically the one whose edge this completion just satisfied.
		next := operationRunnable(remaining)
		if len(next) == 0 {
			plan.Reason = operationReasonBlocked
			plan.Report = remaining[0].Name
			plan.Pending = operationNames(remaining[1:])
			plan.Detail += "; the queue is blocked on a required operation"
			return plan
		}
		plan.Reason = operationReasonRunning
		plan.Run = next[0].Name
		plan.Report = next[0].Name
		plan.Pending = operationNames(operationExcept(remaining, next[0].Name))
		return plan
	}

	// Rows 7, 9 and 10 all keep the SAME operation running and acknowledge nothing.
	// They differ only in what is reported, so the order below is an escalation of
	// loudness, not of action.
	plan := operationPlan{
		Run:     pick.Name,
		Report:  pick.Name,
		Pending: operationNames(operationExcept(pending, pick.Name)),
		Reason:  operationReasonRunning,
	}
	switch {
	// Row 10 — past StallAfter. Checked before the other two, which is a deliberate
	// ruling on an overlap the plan's table does not order: a driver that converged
	// and then waits forever on a wedged rollout would otherwise report Running for
	// ever, which is exactly the silence this mechanism exists to remove. The stall
	// takes the REASON; row 7 keeps the action, so there is still no acknowledgment.
	case operationStalled(in, pick):
		plan.Reason = operationReasonStalled
		plan.Detail = pick.Name + " has run past its StallAfter budget; it is not " +
			"auto-exited (a timer would be the defect with a delay)"
	// Row 9 — the driver reported Blocked. Never auto-skipped to the next in the
	// queue: head-of-line blocking here is correct.
	case in.DriverBlocked:
		plan.Reason = operationReasonBlocked
		plan.Detail = pick.Name + " reported blocked; the queue is held rather than skipped"
	// Row 7 — the transition guard. "The driver is done" is not "the operation is
	// over": Rule N converges the moment the Sentinels agree, which can be well before
	// the Redis StatefulSet finishes rolling. Acknowledging here hands the exit edge
	// straight into the churn LR-050 is about.
	case in.DriverDone:
		plan.Detail = pick.Name + " converged, but the instance is still rolling; " +
			"acknowledgment waits for it to settle"
	default:
		plan.Detail = pick.Name + " is in progress"
	}
	return plan
}

// operationRunnable filters pending candidates to those whose every Requires edge is
// satisfied — the required operation is NOT PENDING. A requirement absent from the
// pending set is satisfied whether it was seeded, completed long ago, or is not a
// candidate on this instance at all (its Applies is false, e.g. rotation with auth
// disabled). That is the state reading, and it is the whole point: the event reading
// would deadlock an instance whose auth was on from creation.
func operationRunnable(pending []operationCandidate) []operationCandidate {
	isPending := make(map[string]bool, len(pending))
	for _, c := range pending {
		isPending[c.Name] = true
	}
	var out []operationCandidate
	for _, c := range pending {
		blocked := false
		for _, req := range c.Requires {
			if isPending[req] {
				blocked = true
				break
			}
		}
		if !blocked {
			out = append(out, c)
		}
	}
	return out
}

// operationStalled reports whether the running operation has outlived its budget. The
// clock belongs to the operation status.operation names: attributing another
// operation's StartedAt to this one would manufacture a stall out of a stale
// monitoring field. No status.operation at all means this is the first pass, which
// cannot be stalled.
func operationStalled(in operationInput, c operationCandidate) bool {
	if c.StallAfter <= 0 || in.Active == nil || in.Active.Name != c.Name {
		return false
	}
	if in.Active.StartedAt.IsZero() {
		return false
	}
	return in.Now.Sub(in.Active.StartedAt.Time) > c.StallAfter
}

func operationNames(cands []operationCandidate) []string {
	if len(cands) == 0 {
		return nil
	}
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.Name)
	}
	return out
}

func operationExcept(cands []operationCandidate, name string) []operationCandidate {
	var out []operationCandidate
	for _, c := range cands {
		if c.Name != name {
			out = append(out, c)
		}
	}
	return out
}
