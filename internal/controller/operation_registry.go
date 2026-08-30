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
	"regexp"
	"sort"
	"strings"
	"time"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

// The heavy-operation registry (ADR-020, implementation plan §3).
//
// Membership is an API concept, not an implementation detail: adding a field here
// changes behaviour, because a heavy operation suppresses regular healing while it
// runs. Admission therefore requires ADR-020's two-clause test —
//
//	A — Human-initiated. The operation exists only because a human edited a declared
//	    field on the CR, or on an object the CR names. Nothing the WORLD does is ever
//	    a heavy operation; undeclared churn is the invariants' job.
//	B — Demonstrated interference, with a citation. At least one named, documented
//	    case where regular healing and this operation contradict each other.
//
// — and clause B is enforced mechanically by TestEveryEntryHasCitation. "It feels
// risky, give it a window" cannot be admitted, because there is nothing to cite.
//
// Nothing in this file consumes the registry. The wiring is M3.1.

// fieldPathSentinelMasterName is the CR field whose edit declares the rename. It is
// named because the CEL admission rule, the registry row and the agreement test must
// all mean the same path.
const fieldPathSentinelMasterName = "spec.sentinel.masterName"

// fingerprintInput is everything a Fingerprint may read. EXTEND IT — never widen a
// Fingerprint's reach — when a new operation needs a new source.
type fingerprintInput struct {
	LR  *littleredv1alpha1.LittleRed
	UID types.UID

	// Referenced holds the content of objects the CR NAMES but does not own, keyed by
	// a stable identifier (e.g. "auth-secret"). Absent when unreadable — and an
	// unreadable reference must HOLD, never advance.
	//
	// It is here before any operation uses it because it is the shape rotation
	// requires, not speculative generality: a password is a secretKeyRef whose value
	// changes no pod template, so an operation over it has no observable signature at
	// all. If this took only *LittleRed, admitting rotation would mean reshaping the
	// registry rather than adding a row.
	Referenced map[string][]byte
}

// heavyOperation is one registry row.
//
// There is deliberately NO precedence, priority or rank field, and none may be added
// (ADR-020, Alternatives E and F; TestNoPrecedenceField holds the refusal). Ordering
// comes from three places instead: a running operation is itself the record of order
// for anything sequential; simultaneous CR-resident heavy changes are refused at
// ADMISSION so no order has to be invented; and the one pair admission cannot see —
// a CR intent and a Secret intent — is ordered by a declared Requires edge.
type heavyOperation struct {
	// Name is the stable identity. It appears in status.operation,
	// status.acknowledgedOperations, conditions, events and lrctl, and it is the HMAC
	// domain separator, so renaming one is an API change.
	Name string

	// FieldPath is the CR field a human edits to declare this operation, in CRD
	// notation ("spec.sentinel.masterName"), or EMPTY when the declared value lives
	// outside the CR (rotation's value is a Secret's content).
	//
	// The distinction is load-bearing rather than documentary. Only CR-resident
	// operations can appear in the CEL admission rule, because CEL evaluates the CR
	// and nothing else; an externally-resident one is structurally unrefusable at
	// apply time and is ordered by a Requires edge instead. Empty here is therefore a
	// statement about what admission can see, not a missing value.
	FieldPath string

	// Modes lists the spec.mode values in which this operation can exist at all.
	Modes []string

	// Citation is ADR-020 admission clause B: the named, documented case where regular
	// healing and this operation contradict each other. Non-empty, asserted by test.
	Citation string

	// Requires names operations that must be NOT PENDING before this one may run — a
	// fact about this operation's own preconditions (rotation requires auth to be on),
	// never a ranking against other entries. Edges form a DAG; a cycle is an error, not
	// an ordering. Empty for operations that commute.
	//
	// "Not pending" — its fingerprint matches its ack — and NOT "has run". The event
	// reading deadlocks the common case: an instance created with auth already on
	// never performs an enablement, so a rotation would wait forever for something
	// that will never happen. Per-candidate seeding is what satisfies the edge there,
	// which is its second job beyond upgrade-safety.
	//
	// The edge is an explicit field rather than a precondition folded into Applies.
	// The two are equivalent in effect and not in maintainability: an ordering
	// constraint hidden inside a predicate that reads like a mode filter is how it
	// gets deleted in a refactor by someone who never knew what it was holding.
	Requires []string

	// StallAfter is how long the operation may run before it is reported STALLED.
	// There is no auto-exit: a timer would be the defect with a delay (ADR-017).
	StallAfter time.Duration

	// Fingerprint is PURE. It never dials anything and never reads the cluster: the
	// caller gathers, it decides. It identifies WHICH VALUE was asked for, not which
	// field — the same field changes repeatedly, so a field-keyed record would
	// acknowledge an edit that never ran.
	Fingerprint func(in fingerprintInput) string

	// Applies reports whether this operation is meaningful for this instance at all
	// (mode, feature flags). PURE. It is NOT the place to hide an ordering
	// constraint — see Requires.
	Applies func(lr *littleredv1alpha1.LittleRed) bool
}

// heavyOperations is the registry. v1 has exactly one member.
//
// THE CEL ADMISSION RULE AND THIS TABLE MUST AGREE, and their drift is the new
// maintenance cost ADR-020 records. The rule is the spec-level XValidation transition
// rule on LittleRedSpec (api/v1alpha1/littlered_types.go); it carries one term per
// CR-RESIDENT entry here, and TestAdmissionRuleAgreesWithRegistry fails the moment the
// two sets differ in either direction.
//
// The rule is VACUOUSLY TRUE today, deliberately. With one CR-resident member the
// count of changed heavy fields can never exceed one. It deliberately carries no term
// for spec.auth.enabled — the form ADR-020 verified live does, but auth is not a
// registered operation, and refusing at admission a change that no driver converges
// would make the API stricter than the operator. The live deliverable of this pairing
// is therefore the agreement test, not the constraint; the constraint is the shape the
// second term slots into, already proven to compile against a real API server.
var heavyOperations = []heavyOperation{
	{
		Name:      "SentinelMasterNameRename",
		FieldPath: fieldPathSentinelMasterName,
		Modes:     []string{ModeSentinel},
		Citation: "LR-050: measured 42.5s in which planForsaken read our own rename churn " +
			"as a capture",
		Requires: nil,
		// 15m against a measured settle of 176.8s (ADR-018's verification) plus ~30s at
		// the master edge — roughly 4x, deliberately generous, because the failure
		// direction of a too-tight stall signal is a false alarm on a slow cluster.
		StallAfter: 15 * time.Minute,
		Fingerprint: func(in fingerprintInput) string {
			// The EFFECTIVE name, which is what Rule N reconciles the Sentinels to. An
			// instance that omits spec.sentinel is genuinely running under the legacy
			// name, so fingerprinting the raw field would read its first explicit
			// "mymaster" as a rename that changes nothing.
			return littleredv1alpha1.OperationFingerprint(
				in.UID, "SentinelMasterNameRename", in.LR.SentinelMasterName())
		},
		Applies: func(lr *littleredv1alpha1.LittleRed) bool {
			// Not gated on spec.sentinel being present: an instance without it is
			// running under the legacy name, and setting the field IS the rename.
			return lr.Spec.Mode == ModeSentinel
		},
	},
}

// operationsMissingCitation returns the names of entries with no citation, sorted.
// ADR-020 admission clause B, made mechanical.
func operationsMissingCitation(ops []heavyOperation) []string {
	var missing []string
	for _, op := range ops {
		if strings.TrimSpace(op.Citation) == "" {
			missing = append(missing, op.Name)
		}
	}
	sort.Strings(missing)
	return missing
}

// operationsWithDanglingRequires returns "<from> -> <target>" for every Requires edge
// naming an operation that is not in the registry — a typo, or an entry someone
// de-registered without following the edges, either of which leaves a dependency that
// can never be satisfied.
func operationsWithDanglingRequires(ops []heavyOperation) []string {
	known := make(map[string]bool, len(ops))
	for _, op := range ops {
		known[op.Name] = true
	}
	var dangling []string
	for _, op := range ops {
		for _, req := range op.Requires {
			if !known[req] {
				dangling = append(dangling, op.Name+" -> "+req)
			}
		}
	}
	sort.Strings(dangling)
	return dangling
}

// operationRequiresCycle returns one cycle in the Requires edges, as the sequence of
// names traversed (first name repeated at the end), or nil when the graph is acyclic.
//
// "A genuine cycle — A requires B requires C requires A — is DETECTABLE, where
// precedence integers accepted it silently by being unable to express it. That is the
// honest place for refusal: not 'two things changed at once', but 'you have declared
// something unorderable'" (ADR-020, Alternative E).
//
// Acyclicity is also what the operation planner assumes when it orders pending
// operations, so this is where that assumption is discharged. Edges naming an unknown
// operation are ignored here — that is operationsWithDanglingRequires' finding, and
// reporting it twice would misattribute a typo as an unorderable declaration.
func operationRequiresCycle(ops []heavyOperation) []string {
	requires := make(map[string][]string, len(ops))
	for _, op := range ops {
		requires[op.Name] = op.Requires
	}

	const (
		unvisited = 0
		onStack   = 1
		done      = 2
	)
	state := make(map[string]int, len(ops))
	var stack []string
	var cycle []string

	var visit func(name string) bool
	visit = func(name string) bool {
		state[name] = onStack
		stack = append(stack, name)
		for _, req := range requires[name] {
			if _, known := requires[req]; !known {
				continue // dangling; not this function's finding
			}
			switch state[req] {
			case onStack:
				// Cut the stack at the repeated name so the report is the cycle
				// itself, not the path that led into it.
				for i, n := range stack {
					if n == req {
						cycle = append(append([]string{}, stack[i:]...), req)
						break
					}
				}
				return true
			case unvisited:
				if visit(req) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[name] = done
		return false
	}

	// Iterate the slice, not the map, so the reported cycle is deterministic.
	for _, op := range ops {
		if state[op.Name] == unvisited && visit(op.Name) {
			return cycle
		}
	}
	return nil
}

// celFieldTermRe matches a CEL reference to a spec field, on either side of a
// transition rule.
var celFieldTermRe = regexp.MustCompile(`\b(?:old)?[Ss]elf((?:\.[A-Za-z_][A-Za-z0-9_]*)+)`)

// celHasGuardRe matches a has() guard. Guards are stripped before terms are
// extracted: has(self.sentinel) names a parent whose presence must be checked before
// its child is read, and is not itself a constrained field.
var celHasGuardRe = regexp.MustCompile(`has\([^()]*\)`)

// heavyOperationCELTerms returns the spec field paths a CEL admission rule constrains,
// in CRD notation ("spec.sentinel.masterName"), sorted and deduplicated.
func heavyOperationCELTerms(rule string) []string {
	stripped := celHasGuardRe.ReplaceAllString(rule, "")
	seen := map[string]bool{}
	var terms []string
	for _, m := range celFieldTermRe.FindAllStringSubmatch(stripped, -1) {
		path := "spec" + m[1]
		if !seen[path] {
			seen[path] = true
			terms = append(terms, path)
		}
	}
	sort.Strings(terms)
	return terms
}

// crResidentFieldPaths returns the field paths of the operations whose declared value
// lives in the CR, sorted — exactly the set the CEL admission rule must constrain.
// Externally-resident operations are absent by construction: CEL cannot see a Secret,
// so their ordering is a Requires edge instead.
func crResidentFieldPaths(ops []heavyOperation) []string {
	var paths []string
	for _, op := range ops {
		if op.FieldPath != "" {
			paths = append(paths, op.FieldPath)
		}
	}
	sort.Strings(paths)
	return paths
}
