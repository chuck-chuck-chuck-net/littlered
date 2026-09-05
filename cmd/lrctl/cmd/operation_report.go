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

package cmd

import (
	"fmt"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// The `lrctl` surfaces for ADR-020's declared heavy operations.
//
// `lrctl` is the tool an owner reaches for at 03:00 (rule §7.8), so the mechanism that
// decides whether the operator is healing or standing down must be visible in it. Every
// function here is a PURE formatter over the CR's status: read-only by construction, and
// testable without a cluster.
//
// The vocabulary is the controller's — status.operation.reason — mirrored here rather
// than imported, because those constants are unexported in a package the CLI does not
// depend on. TestOperationReasonsMatchController is the drift guard: it reads the
// controller's source and asserts the two agree, so the shipped binary stays decoupled
// while a rename cannot pass silently.
const (
	// Imported from the API package rather than mirrored (ADR-020's vocabulary is an API
	// surface). The previous copy here was kept honest by a test that parsed the
	// controller's source — a workaround, not a design, and importing deletes it.
	opReasonRunning     = littleredv1alpha1.OperationReasonRunning
	opReasonBlocked     = littleredv1alpha1.OperationReasonBlocked
	opReasonStalled     = littleredv1alpha1.OperationReasonStalled
	opReasonQuarantined = littleredv1alpha1.OperationReasonQuarantined
	opReasonConverged   = littleredv1alpha1.OperationReasonConverged
	opReasonSeeded      = littleredv1alpha1.OperationReasonSeeded
)

// operationView is everything `lrctl` reports about a declared heavy operation: the
// monitoring surface itself, plus the OperationInProgress condition's message, which is
// where the planner says WHAT the operation is waiting on. That message is the single
// most useful string an owner gets, so it is carried into both verbs rather than left
// for a separate `kubectl get -o yaml`.
//
// AcknowledgedOperations is deliberately NOT here. It is a list of keyed digests, which
// tells a human nothing — and ADR-020 D3 is that the record is never an operational
// input, so no verdict in this file may consult it. It is mirrored into `status --json`
// (a faithful projection of the CR) and nowhere else.
type operationView struct {
	Op      *littleredv1alpha1.OperationStatus
	Message string
}

// operationViewOf reads the two surfaces off the CR. Nil-safe: an instance with no
// operation yields the zero view, which every renderer treats as "print nothing".
func operationViewOf(lr *littleredv1alpha1.LittleRed) operationView {
	if lr == nil || lr.Status.Operation == nil {
		return operationView{}
	}
	v := operationView{Op: lr.Status.Operation}
	if c := apimeta.FindStatusCondition(
		lr.Status.Conditions, littleredv1alpha1.ConditionOperationInProgress,
	); c != nil {
		v.Message = c.Message
	}
	return v
}

// operationFails is `verify`'s verdict on an operation, and it is the whole judgement of
// this milestone.
//
// It fails for exactly two states, and both for the same documented reason: ADR-020
// guarantees that Blocked and Stalled NEVER auto-resolve. There is no auto-exit timer and
// no auto-skip, on ADR-017's lesson that a timer is the defect with a delay — so each one
// is a standing request for a human, and a `verify` that stayed silent on it would defeat
// the loud condition the mechanism is built around.
//
// Everything else is deliberately not a failure:
//
//   - Running is a supported thing an owner asked for. Going red because someone renamed
//     an instance trains people to ignore `verify`, which costs more than the check buys.
//     It is reported, and reported as explicitly benign. This is the distinction ADR-017
//     draws for a held cluster rollout and the one Rule N's G6 deferral draws for
//     progress-versus-blockage.
//   - Quarantined is a HOLD with its own owner. The operation is correctly waiting for
//     ADR-016 to finish, and an instance held at zero pods already fails verification on
//     its own topology (no authority master, every Sentinel unreachable). Failing twice
//     for one state sends the reader after the wrong thing; it is reported as [WARN] so
//     the held change is not invisible, which is ADR-020's standing requirement from
//     LR-054.
//   - An unknown reason — a future registry entry's — is not evidence of failure. The
//     loud set is enumerated by the ADR and is exactly {Blocked, Stalled}.
func operationFails(op *littleredv1alpha1.OperationStatus) bool {
	if op == nil {
		return false
	}
	return op.Reason == opReasonBlocked || op.Reason == opReasonStalled
}

// operationAge is how long the operation has been running, rounded to the second. The
// clock is an input so the rendering is deterministic under test.
func operationAge(op *littleredv1alpha1.OperationStatus, now time.Time) string {
	if op == nil || op.StartedAt.IsZero() {
		return "an unknown duration"
	}
	return max(now.Sub(op.StartedAt.Time).Round(time.Second), 0).String()
}

// renderOperationVerify is `verify`'s block: a heading, one fact line, one severity line
// with what to do about it, and the queue behind it. Empty — not a placeholder line —
// when no operation is in flight, so `verify` output for every instance that is not
// mid-operation is byte-for-byte what it was.
//
// The returned bool is the same value the exit code is derived from. Keeping the printed
// verdict and the process's status driven by one expression is the property
// sentinelVerifyFailure exists to guarantee: a [FAIL] line beside an exit 0 would be
// worse than no check at all.
func renderOperationVerify(v operationView, now time.Time) (lines []string, fail bool) {
	if v.Op == nil {
		return nil, false
	}
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }

	add("\nDeclared Operation:")
	add("  %s — %s for %s (started %s)",
		v.Op.Name, v.Op.Reason, operationAge(v.Op, now), v.Op.StartedAt.Format(timeFormat))
	if len(v.Op.Pending) > 0 {
		add("  Pending: %s", strings.Join(v.Op.Pending, ", "))
	}
	if v.Message != "" {
		add("  Operator: %s", v.Message)
	}

	switch v.Op.Reason {
	case opReasonBlocked:
		fail = true
		add("  [FAIL] The operation cannot proceed and is not auto-skipped — head-of-line")
		add("         blocking here is deliberate (ADR-020). Nothing will resolve it on its")
		add("         own: read the OperationInProgress condition above for what it is")
		add("         waiting on, and fix that.")
	case opReasonStalled:
		fail = true
		add("  [FAIL] The operation has outlived its StallAfter budget and is not auto-exited")
		add("         (ADR-017: a timer would be the defect with a delay). It will stay in")
		add("         this state until a human acts. Regular healing that ASSIGNS AUTHORITY")
		add("         is stood down for as long as it holds.")
	case opReasonQuarantined:
		add("  [WARN] A heavy change is pending but held: the instance is quarantined and has")
		add("         no pods to carry it out (ADR-016). It resumes when the quarantine")
		add("         releases. The quarantine, not the operation, is what to investigate.")
	case opReasonRunning:
		add("  [OK] A declared heavy operation is in progress. This is a normal, expected")
		add("       state and not a fault — regular healing that assigns authority is")
		add("       deliberately stood down until it completes.")
	default:
		// A reason this build does not know about. Say so rather than classifying it,
		// and do not fail: an unrecognised state is not evidence of a defect.
		add("  [WARN] Unrecognised operation state %q — this lrctl is older than the operator.", v.Op.Reason)
	}
	return lines, fail
}

// renderOperationStatus is `status`'s view: name, reason, age, and the queue. One line,
// plus at most two, and only while something is in flight — a permanent line here would
// be noise on every instance in the fleet.
//
// The [!] marker is printStatus's existing idiom for "action may be required"
// (FailoverRecovery, LeaderlessRecovery), reused rather than reinvented, and applied to
// exactly the two states operationFails names.
func renderOperationStatus(v operationView, now time.Time) []string {
	if v.Op == nil {
		return nil
	}
	marker := ""
	if operationFails(v.Op) {
		marker = "[!] ACTION MAY BE REQUIRED — "
	}
	lines := []string{fmt.Sprintf("Operation: %s%s — %s for %s",
		marker, v.Op.Name, v.Op.Reason, operationAge(v.Op, now))}
	if len(v.Op.Pending) > 0 {
		lines = append(lines, fmt.Sprintf("  Pending: %s", strings.Join(v.Op.Pending, ", ")))
	}
	if v.Message != "" {
		lines = append(lines, "  "+v.Message)
	}
	return lines
}

// operationJSON is status.operation as `lrctl --json` emits it, plus the two things a
// machine consumer would otherwise have to re-derive: the condition message and the
// verdict lrctl itself reached. The field names match the CRD's (ADR-020 §2) so the JSON
// and `kubectl get -o json` are the same shape.
type operationJSON struct {
	Name      string      `json:"name"`
	StartedAt metav1.Time `json:"startedAt"`
	Reason    string      `json:"reason"`
	Pending   []string    `json:"pending,omitempty"`
	// Message is the OperationInProgress condition's message — what the operation is
	// waiting on, in the operator's own words.
	Message string `json:"message,omitempty"`
	// NeedsAction is operationFails: true for Blocked and Stalled, which never
	// auto-resolve. It is exported so a script and the exit code cannot disagree.
	NeedsAction bool `json:"needsAction"`
}

// operationJSONOf renders the view, or nil when nothing is in flight (the field is
// omitempty, so a non-operating instance's JSON is unchanged).
func operationJSONOf(v operationView) *operationJSON {
	if v.Op == nil {
		return nil
	}
	return &operationJSON{
		Name:        v.Op.Name,
		StartedAt:   v.Op.StartedAt,
		Reason:      v.Op.Reason,
		Pending:     v.Op.Pending,
		Message:     v.Message,
		NeedsAction: operationFails(v.Op),
	}
}
