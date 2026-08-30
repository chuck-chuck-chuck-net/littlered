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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// Names used throughout, mirroring the registry ADR-020 describes: one CR-resident
// operation that exists today, one CR-resident and one Secret-resident operation the
// auth design will add, with the one declared edge between them.
const (
	opRename = "SentinelMasterNameRename" // CR-resident, no edges
	opAuth   = "AuthEnablement"           // CR-resident
	opRotate = "PasswordRotation"         // Secret-resident, Requires AuthEnablement
)

const (
	fpOld = "1111111111111111"
	fpNew = "2222222222222222"
	fpB   = "3333333333333333"
)

func cand(name, fp string, requires ...string) operationCandidate {
	return operationCandidate{
		Name: name, Fingerprint: fp, Requires: requires,
		StallAfter: 15 * time.Minute,
	}
}

func ackNames(acks []operationCandidate) []string {
	out := make([]string, 0, len(acks))
	for _, a := range acks {
		out = append(out, a.Name)
	}
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// planOperation is the decision seam of ADR-020: which declared heavy operation runs
// this pass, which is acknowledged, and what is reported. Ten rows, and the value is
// concentrated in the ones that DECLINE to run — the quarantine hold, the two seeding
// rows that stop an operator upgrade declaring an operation for a whole fleet, and the
// transition guard that refuses to acknowledge work whose StatefulSet is still rolling.
func TestPlanOperation(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	active := func(name string, ago time.Duration) *littleredv1alpha1.OperationStatus {
		return &littleredv1alpha1.OperationStatus{
			Name: name, StartedAt: metav1.NewTime(now.Add(-ago)),
		}
	}

	cases := []struct {
		name    string
		in      operationInput
		run     string
		report  string
		pending []string
		reason  string
		ack     []string
	}{
		// ── Row 1 ────────────────────────────────────────────────────────────────
		{
			// Nothing runs on a quarantined instance — it has no pods to carry the
			// work out — but the held change is still REPORTED. ADR-020's standing
			// requirement, taken from LR-054: anything this mechanism withholds must
			// say that it withheld.
			name: "row 1: quarantined — the pending operation is held, and reported",
			in: operationInput{
				Candidates:  []operationCandidate{cand(opRename, fpNew)},
				Acks:        map[string]string{opRename: fpOld},
				Quarantined: true,
				Settled:     true,
				Now:         now,
			},
			run: "", report: opRename, reason: operationReasonQuarantined,
		},
		{
			// Row 1 wins over every seeding and running row, so nothing is written
			// either: a replicas:0 StatefulSet reads SETTLED, and an acknowledgment
			// there would record work no pod ever executed (plan §7.4's trap).
			name: "row 1: quarantined outranks seeding — no ack is written",
			in: operationInput{
				Candidates:  []operationCandidate{cand(opRename, fpNew)},
				Acks:        map[string]string{},
				Quarantined: true,
				Settled:     true,
				Now:         now,
			},
			run: "", reason: operationReasonConverged,
		},
		// ── Row 2 ────────────────────────────────────────────────────────────────
		{
			// A freshly created CR is already in the state its spec asks for. Without
			// this row it would declare a rename it never asked for, because the spec
			// value differs from a nonexistent ack.
			name: "row 2: bootstrapping — seed every candidate, run nothing",
			in: operationInput{
				Candidates:    []operationCandidate{cand(opRename, fpNew), cand(opAuth, fpB)},
				Acks:          map[string]string{},
				Bootstrapping: true,
				Now:           now,
			},
			run: "", reason: operationReasonSeeded, ack: []string{opAuth, opRename},
		},
		// ── Row 3 ────────────────────────────────────────────────────────────────
		{
			// The fleet-upgrade case, and it is NOT its own rule: it is the special
			// case of per-candidate seeding where every candidate is missing.
			name: "row 3: initialized instance, no ack rows at all — seed, never run",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew), cand(opAuth, fpB)},
				Acks:       map[string]string{},
				Settled:    true,
				Now:        now,
			},
			run: "", reason: operationReasonSeeded, ack: []string{opAuth, opRename},
		},
		{
			// The row that fails a whole-list len(Acks)==0 heuristic: one operation has
			// already completed, so the list is non-empty, and a second is newly
			// registered. The new one must be SEEDED, not run.
			name: "row 3: seeding is PER CANDIDATE — a non-empty ack list does not suppress it",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew), cand(opAuth, fpB)},
				Acks:       map[string]string{opRename: fpNew},
				Settled:    true,
				Now:        now,
			},
			run: "", reason: operationReasonSeeded, ack: []string{opAuth},
		},
		{
			// The mirror image, and the one a whole-list heuristic gets catastrophically
			// wrong: a genuinely pending change alongside a missing row. Seeding lands
			// first; the pending operation runs on the next pass. Nothing is lost —
			// intent is a record, not an edge — and seeding first is what satisfies a
			// Requires edge before its dependant is considered.
			name: "row 3: a missing row is seeded before a pending change is run",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew), cand(opAuth, fpB)},
				Acks:       map[string]string{opRename: fpOld},
				Settled:    true,
				Now:        now,
			},
			run: "", reason: operationReasonSeeded, ack: []string{opAuth},
		},
		// ── Row 4 ────────────────────────────────────────────────────────────────
		{
			name: "row 4: every fingerprint matches its ack — converged, nothing to do",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew), cand(opAuth, fpB)},
				Acks:       map[string]string{opRename: fpNew, opAuth: fpB},
				Settled:    true,
				Now:        now,
			},
			run: "", reason: operationReasonConverged,
		},
		{
			name: "row 4: no candidates at all — converged",
			in:   operationInput{Acks: map[string]string{}, Settled: true, Now: now},
			run:  "", reason: operationReasonConverged,
		},
		// ── Row 5 ────────────────────────────────────────────────────────────────
		{
			name: "row 5: exactly one fingerprint differs — run it",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew), cand(opAuth, fpB)},
				Acks:       map[string]string{opRename: fpOld, opAuth: fpB},
				Settled:    true,
				Now:        now,
			},
			run: opRename, report: opRename, reason: operationReasonRunning,
		},
		// ── Row 6 ────────────────────────────────────────────────────────────────
		{
			// The CR+Secret pair with its declared edge: rotation presupposes auth is
			// on, so enablement runs and rotation queues behind it. The order is a fact
			// about one of the operations, never a ranking (ADR-020 Alternative E).
			name: "row 6: two differ with an edge — the required one runs, the other pends",
			in: operationInput{
				Candidates: []operationCandidate{
					cand(opRotate, fpNew, opAuth), cand(opAuth, fpNew),
				},
				Acks:    map[string]string{opRotate: fpOld, opAuth: fpOld},
				Settled: true,
				Now:     now,
			},
			run: opAuth, report: opAuth, pending: []string{opRotate},
			reason: operationReasonRunning,
		},
		{
			// No edge: rename and rotation commute, so an arbitrary-but-deterministic
			// tiebreak suffices — "arbitrary is fine" is what commuting means. Name
			// order is that tiebreak; serialization already prevents concurrency.
			name: "row 6: two differ with no edge — a deterministic tiebreak, the rest pend",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew), cand(opRotate, fpNew)},
				Acks:       map[string]string{opRename: fpOld, opRotate: fpOld},
				Settled:    true,
				Now:        now,
			},
			run: opRotate, report: opRotate, pending: []string{opRename},
			reason: operationReasonRunning,
		},
		// ── Row 7 ────────────────────────────────────────────────────────────────
		{
			// The transition guard, and ADR-020's central claim. "The driver is done"
			// is not "the operation is over": Rule N converges the moment the Sentinels
			// agree, which is well before the Redis StatefulSet finishes rolling.
			// Acknowledging here hands the exit edge straight into the churn LR-050 is
			// about.
			name: "row 7: driver done but the StatefulSet is unsettled — keep running, do NOT ack",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew)},
				Acks:       map[string]string{opRename: fpOld},
				DriverDone: true,
				Settled:    false,
				Active:     active(opRename, time.Minute),
				Now:        now,
			},
			run: opRename, report: opRename, reason: operationReasonRunning,
		},
		// ── Row 8 ────────────────────────────────────────────────────────────────
		{
			name: "row 8: driver done AND settled — acknowledge, nothing left to run",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew)},
				Acks:       map[string]string{opRename: fpOld},
				DriverDone: true,
				Settled:    true,
				Active:     active(opRename, time.Minute),
				Now:        now,
			},
			run: "", reason: operationReasonConverged, ack: []string{opRename},
		},
		{
			// Completion is what releases the queue — and the released operation is the
			// one whose edge the completion just satisfied.
			name: "row 8: driver done AND settled with a queue — ack, and the next one runs",
			in: operationInput{
				Candidates: []operationCandidate{
					cand(opRotate, fpNew, opAuth), cand(opAuth, fpNew),
				},
				Acks:       map[string]string{opRotate: fpOld, opAuth: fpOld},
				DriverDone: true,
				Settled:    true,
				Active:     active(opAuth, time.Minute),
				Now:        now,
			},
			run: opRotate, report: opRotate, reason: operationReasonRunning,
			ack: []string{opAuth},
		},
		// ── Row 9 ────────────────────────────────────────────────────────────────
		{
			// Never auto-skip. A blocked operation holds its queue indefinitely and
			// loudly; skipping it would run a dependant through a half-applied change.
			name: "row 9: driver blocked — keep it, report Blocked, never skip to the next",
			in: operationInput{
				Candidates: []operationCandidate{
					cand(opRotate, fpNew, opAuth), cand(opAuth, fpNew),
				},
				Acks:          map[string]string{opRotate: fpOld, opAuth: fpOld},
				DriverBlocked: true,
				Settled:       true,
				Active:        active(opAuth, time.Minute),
				Now:           now,
			},
			run: opAuth, report: opAuth, pending: []string{opRotate},
			reason: operationReasonBlocked,
		},
		// ── Row 10 ───────────────────────────────────────────────────────────────
		{
			// ADR-017's lesson: there is no auto-exit timer, because a timer would be
			// the defect with a delay. StallAfter changes what the operator SAYS, never
			// what it does.
			name: "row 10: past StallAfter — still running, loudly, and never auto-exited",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew)},
				Acks:       map[string]string{opRename: fpOld},
				Settled:    true,
				Active:     active(opRename, 16*time.Minute),
				Now:        now,
			},
			run: opRename, report: opRename, reason: operationReasonStalled,
		},
		{
			// The stall clock belongs to the operation status.operation names. Reading
			// another operation's StartedAt would manufacture a stall out of a stale
			// monitoring field — the only thing Active is consulted for.
			name: "row 10: the stall clock is not transferable between operations",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew)},
				Acks:       map[string]string{opRename: fpOld},
				Settled:    true,
				Active:     active(opAuth, 16*time.Minute),
				Now:        now,
			},
			run: opRename, report: opRename, reason: operationReasonRunning,
		},
		{
			name: "row 10: no status.operation yet — the first pass cannot be stalled",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew)},
				Acks:       map[string]string{opRename: fpOld},
				Settled:    true,
				Now:        now,
			},
			run: opRename, report: opRename, reason: operationReasonRunning,
		},
		// ── Overlaps, ruled deliberately ─────────────────────────────────────────
		{
			// A driver that converges and then waits forever on a wedged rollout is
			// exactly the state a human must see. Reporting Running for it would be the
			// silence ADR-020 exists to remove, so the stall wins the REASON — while
			// row 7 keeps its grip on the ACTION: still no acknowledgment.
			name: "overlap: stalled while the transition guard holds — Stalled wins, still no ack",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew)},
				Acks:       map[string]string{opRename: fpOld},
				DriverDone: true,
				Settled:    false,
				Active:     active(opRename, 16*time.Minute),
				Now:        now,
			},
			run: opRename, report: opRename, reason: operationReasonStalled,
		},
		{
			// Completion outranks the stall: an operation that finished on its last
			// pass is finished, however long it took.
			name: "overlap: complete and settled past StallAfter — acknowledge anyway",
			in: operationInput{
				Candidates: []operationCandidate{cand(opRename, fpNew)},
				Acks:       map[string]string{opRename: fpOld},
				DriverDone: true,
				Settled:    true,
				Active:     active(opRename, 16*time.Minute),
				Now:        now,
			},
			run: "", reason: operationReasonConverged, ack: []string{opRename},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planOperation(tc.in)
			if got.Run != tc.run {
				t.Errorf("Run = %q, want %q", got.Run, tc.run)
			}
			if got.Report != tc.report {
				t.Errorf("Report = %q, want %q", got.Report, tc.report)
			}
			if !sameStrings(got.Pending, tc.pending) {
				t.Errorf("Pending = %v, want %v", got.Pending, tc.pending)
			}
			if got.Reason != tc.reason {
				t.Errorf("Reason = %q, want %q", got.Reason, tc.reason)
			}
			if !sameStrings(ackNames(got.Ack), tc.ack) {
				t.Errorf("Ack = %v, want %v", ackNames(got.Ack), tc.ack)
			}
		})
	}
}

// "Requires X" means "X is NOT PENDING", never "X has run" — the single most
// load-bearing ruling in the seam (ADR-020, Serialization).
//
// The event reading deadlocks the common case: an instance created with auth already
// on never performs an enablement, so a rotation would wait forever for something that
// will never happen. Under the state reading every case is right, and the seeding rows
// are what make it work — which is their second job, beyond upgrade-safety.
func TestPlanOperationRequiresMeansNotPending(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	t.Run("a requirement that never ran, only seeded, satisfies the edge", func(t *testing.T) {
		// AuthEnablement was seeded on a previous pass — auth has been on since
		// creation and no enablement will ever be performed. Rotation must run.
		in := operationInput{
			Candidates: []operationCandidate{
				cand(opRotate, fpNew, opAuth), cand(opAuth, fpB),
			},
			Acks:    map[string]string{opRotate: fpOld, opAuth: fpB},
			Settled: true,
			Now:     now,
		}
		if got := planOperation(in); got.Run != opRotate {
			t.Fatalf("Run = %q, want %q (a seeded requirement is NOT pending)", got.Run, opRotate)
		}
	})

	t.Run("a requirement absent from the candidate set satisfies the edge", func(t *testing.T) {
		// Auth is disabled, so AuthEnablement is not a candidate at all. An absent
		// operation cannot be pending, so the edge is inert rather than blocking.
		in := operationInput{
			Candidates: []operationCandidate{cand(opRotate, fpNew, opAuth)},
			Acks:       map[string]string{opRotate: fpOld},
			Settled:    true,
			Now:        now,
		}
		if got := planOperation(in); got.Run != opRotate {
			t.Fatalf("Run = %q, want %q (an absent requirement is not pending)", got.Run, opRotate)
		}
	})

	t.Run("a requirement that IS pending holds its dependant, and says so", func(t *testing.T) {
		// Head-of-line blocking, and correct: rotating through a half-applied
		// enablement is precisely what the edge prevents.
		in := operationInput{
			Candidates: []operationCandidate{
				cand(opRotate, fpNew, opAuth), cand(opAuth, fpNew),
			},
			Acks:    map[string]string{opRotate: fpOld, opAuth: fpOld},
			Settled: true,
			Now:     now,
		}
		got := planOperation(in)
		if got.Run != opAuth {
			t.Fatalf("Run = %q, want %q", got.Run, opAuth)
		}
		if !sameStrings(got.Pending, []string{opRotate}) {
			t.Fatalf("Pending = %v, want [%s]", got.Pending, opRotate)
		}
	})

	t.Run("pending but nothing runnable is reported, never auto-skipped", func(t *testing.T) {
		// The dependant is pending and its requirement is pending but NOT a runnable
		// candidate this pass — the state a cycle would also present as. It is
		// reported as Blocked; it is never resolved by skipping an edge.
		in := operationInput{
			Candidates: []operationCandidate{
				cand(opRotate, fpNew, opRename), cand(opRename, fpNew, opRotate),
			},
			Acks:    map[string]string{opRotate: fpOld, opRename: fpOld},
			Settled: true,
			Now:     now,
		}
		got := planOperation(in)
		if got.Run != "" {
			t.Fatalf("Run = %q, want \"\" (nothing is runnable)", got.Run)
		}
		if got.Reason != operationReasonBlocked {
			t.Fatalf("Reason = %q, want %q", got.Reason, operationReasonBlocked)
		}
		if got.Report == "" {
			t.Fatalf("Report = %q, want the held operation named", got.Report)
		}
	})
}

// D1/D2's central claim, as its own named test: acknowledge on COMPLETION, never on
// observation. The operator acknowledges when the work is finished — driver done AND
// the instance's own StatefulSets settled — and at no earlier moment.
//
// Acknowledging on sight fails ADR-020's 100% bar in the forbidden direction: the
// operator dies between the write and the action and the intent is lost SILENTLY.
func TestPlanOperationAcknowledgesOnCompletionNotObservation(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	base := func() operationInput {
		return operationInput{
			Candidates: []operationCandidate{cand(opRename, fpNew)},
			Acks:       map[string]string{opRename: fpOld},
			Active: &littleredv1alpha1.OperationStatus{
				Name: opRename, StartedAt: metav1.NewTime(now.Add(-time.Minute)),
			},
			Now: now,
		}
	}

	t.Run("observed only — the operation is picked up, not acknowledged", func(t *testing.T) {
		in := base()
		in.Settled = true
		got := planOperation(in)
		if got.Run != opRename {
			t.Fatalf("Run = %q, want %q", got.Run, opRename)
		}
		if len(got.Ack) != 0 {
			t.Fatalf("Ack = %v, want none: seeing the intent is not carrying it out", ackNames(got.Ack))
		}
	})

	t.Run("driver converged but unsettled — still not acknowledged", func(t *testing.T) {
		in := base()
		in.DriverDone, in.Settled = true, false
		got := planOperation(in)
		if len(got.Ack) != 0 {
			t.Fatalf("Ack = %v, want none: Rule N converges before the StatefulSet finishes rolling, "+
				"and acknowledging there hands the exit edge into the churn LR-050 is about",
				ackNames(got.Ack))
		}
		if got.Run != opRename {
			t.Fatalf("Run = %q, want %q — the operation is still in force", got.Run, opRename)
		}
	})

	t.Run("driver converged and settled — only now acknowledged", func(t *testing.T) {
		in := base()
		in.DriverDone, in.Settled = true, true
		got := planOperation(in)
		if !sameStrings(ackNames(got.Ack), []string{opRename}) {
			t.Fatalf("Ack = %v, want [%s]", ackNames(got.Ack), opRename)
		}
		if got.Run != "" {
			t.Fatalf("Run = %q, want \"\"", got.Run)
		}
	})
}
