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

package worklist

import (
	"slices"
	"strings"
	"testing"

	"github.com/littlered-operator/littlered-operator/internal/watchscope"
)

// The launch gate against a CONFLICTING OPERATOR.
//
// An operator whose watch scope overlaps the array's namespaces double-reconciles
// the items' CRs alongside their own per-item operators. That is exactly the
// corruption design §3 exists to prevent, and its victim is the parked forensic
// state — so it does not fail cleanly, it fails by quietly ruining the evidence.
// Hence a launch-time refusal, in the same shape as the image and parked gates.

const guardList = `
array: guard
items:
  - {name: alpha, labelFilter: pdb}
  - {name: beta, labelFilter: pdb}
`

// Literals the table reuses.
const (
	guardOpNS0    = "lr-harness-op-guard-0-alpha"
	operatorName  = "littlered"
	operatorNS    = "littlered-system"
	foreignNS     = "other"
	reasonWatches = "watches"
)

func guardArray(t *testing.T) List {
	t.Helper()
	l, err := Parse([]byte(guardList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return l
}

func TestNamespacesCoversBothFamilies(t *testing.T) {
	l := guardArray(t)
	got := l.Namespaces()
	want := []string{
		"lr-harness-guard-0-alpha", "lr-harness-op-guard-0-alpha",
		"lr-harness-guard-1-beta", "lr-harness-op-guard-1-beta",
	}
	for _, w := range want {
		if !slices.Contains(got, w) {
			t.Errorf("Namespaces() = %v, missing %q — a scope check that misses the "+
				"operator namespaces would let an operator scoped to one through", got, w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("Namespaces() = %v, want exactly %d entries", got, len(want))
	}
}

func TestConflictingOperators(t *testing.T) {
	cases := []struct {
		name       string
		op         ObservedOperator
		wantConfl  bool
		wantReason string
	}{
		{
			name:       "cluster-scoped operator watches everything",
			op:         ObservedOperator{Namespace: operatorNS, Name: operatorName, Replicas: 1},
			wantConfl:  true,
			wantReason: "cluster-scoped",
		},
		{
			name: "scaled to zero is not running, so not a conflict",
			// This IS the documented remedy, so the guard must recognise it or it
			// would refuse forever after the user has already fixed the problem.
			op:        ObservedOperator{Namespace: operatorNS, Name: operatorName, Replicas: 0},
			wantConfl: false,
		},
		{
			name: "allow-list overlapping an item's TEST namespace",
			op: ObservedOperator{
				Namespace: foreignNS, Name: "op", Replicas: 1,
				WatchNamespace: "something-else,lr-harness-guard-1-beta",
			},
			wantConfl:  true,
			wantReason: reasonWatches,
		},
		{
			name: "allow-list overlapping an item's OPERATOR namespace",
			op: ObservedOperator{
				Namespace: foreignNS, Name: "op", Replicas: 1,
				WatchNamespace: guardOpNS0,
			},
			wantConfl:  true,
			wantReason: reasonWatches,
		},
		{
			name: "allow-list disjoint from the array",
			op: ObservedOperator{
				Namespace: foreignNS, Name: "op", Replicas: 1,
				WatchNamespace: "some-team-a,some-team-b",
			},
			wantConfl: false,
		},
		{
			name: "deny-list that does not exclude the array still watches it",
			op: ObservedOperator{
				Namespace: foreignNS, Name: "op", Replicas: 1,
				IgnoreNamespace: "kube-system",
			},
			wantConfl:  true,
			wantReason: reasonWatches,
		},
		{
			name: "deny-list excluding every array namespace is safe",
			op: ObservedOperator{
				Namespace: foreignNS, Name: "op", Replicas: 1,
				IgnoreNamespace: "lr-harness-guard-0-alpha," + guardOpNS0 + "," +
					"lr-harness-guard-1-beta,lr-harness-op-guard-1-beta",
			},
			wantConfl: false,
		},
		{
			name: "an operator living in one of THIS array's own operator namespaces",
			// A parked operator from an earlier run of the same array. The driver's
			// clean-slate step deletes that namespace before the item runs, so
			// refusing on it would block the documented clobber path.
			op: ObservedOperator{
				Namespace: guardOpNS0, Name: operatorName, Replicas: 1,
				WatchNamespace: "lr-harness-guard-0-alpha",
			},
			wantConfl: false,
		},
		{
			name: "both scoping modes set — refuse rather than reason about it",
			// watchscope.Parse rejects this pair and the operator exits, so it
			// reconciles nothing. But "it is crash-looping" is a guess about
			// another binary's startup, and a guard that guesses wrong here
			// silently permits the corruption it exists to prevent. Fail closed.
			op: ObservedOperator{
				Namespace: foreignNS, Name: "op", Replicas: 1,
				WatchNamespace: "a", IgnoreNamespace: "b",
			},
			wantConfl:  true,
			wantReason: "mutually exclusive",
		},
	}

	l := guardArray(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := l.ConflictingOperators([]ObservedOperator{tc.op})
			if !tc.wantConfl {
				if len(got) != 0 {
					t.Fatalf("ConflictingOperators() = %+v, want none", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("ConflictingOperators() = %+v, want exactly one conflict", got)
			}
			if !strings.Contains(got[0].Reason, tc.wantReason) {
				t.Errorf("reason = %q, want it to mention %q", got[0].Reason, tc.wantReason)
			}
		})
	}
}

func TestConflictingOperatorsIsOrderedAndComplete(t *testing.T) {
	l := guardArray(t)
	got := l.ConflictingOperators([]ObservedOperator{
		{Namespace: "zeta", Name: "op", Replicas: 1},
		{Namespace: "alpha", Name: "op", Replicas: 1},
		{Namespace: "safe", Name: "op", Replicas: 1, WatchNamespace: "elsewhere"},
	})
	if len(got) != 2 {
		t.Fatalf("got %d conflicts, want 2", len(got))
	}
	if got[0].Operator.Namespace != "alpha" || got[1].Operator.Namespace != "zeta" {
		t.Errorf("conflicts are not sorted: %v, %v", got[0].Operator, got[1].Operator)
	}
}

func TestOperatorRefusalMessageNamesTheProblemAndTheWayOut(t *testing.T) {
	l := guardArray(t)
	confl := l.ConflictingOperators([]ObservedOperator{
		{Namespace: operatorNS, Name: operatorName, Replicas: 1},
	})
	msg := OperatorRefusalMessage(l.Array, confl)

	for _, want := range []string{
		"REFUSING TO LAUNCH",
		operatorNS,                            // where it is
		operatorName,                          // what it is called
		"scale",                               // the fix
		"--replicas=0",                        // the exact command shape
		"E2E_HARNESS_ALLOW_OPERATOR_CONFLICT", // the established override style
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message does not mention %q:\n%s", want, msg)
		}
	}
}

// DRIFT GUARD. The watch-scope semantics are the OPERATOR's, defined in
// internal/watchscope (ADR-014). This package deliberately re-implements the
// tiny classification rather than importing that package, because importing it
// would pull controller-runtime into the lrharness binary and into the driver
// image build. The test imports it, so the shipped binary stays decoupled while
// a semantic change upstream still breaks the build here.
func TestScopeClassificationMatchesTheOperators(t *testing.T) {
	cases := []struct{ watch, ignore string }{
		{"", ""},
		{"a", ""},
		{"a,b", ""},
		{" a , b ,", ""},
		{"", "a"},
		{"", "a,b"},
	}
	for _, tc := range cases {
		cfg, err := watchscope.Parse(tc.watch, tc.ignore)
		if err != nil {
			t.Fatalf("watchscope.Parse(%q,%q): %v", tc.watch, tc.ignore, err)
		}
		mode, namespaces := classifyScope(tc.watch, tc.ignore)
		if string(mode) != string(cfg.Mode) {
			t.Errorf("classifyScope(%q,%q) mode = %q, watchscope says %q",
				tc.watch, tc.ignore, mode, cfg.Mode)
		}
		if !slices.Equal(namespaces, cfg.Namespaces) {
			t.Errorf("classifyScope(%q,%q) namespaces = %v, watchscope says %v",
				tc.watch, tc.ignore, namespaces, cfg.Namespaces)
		}
	}

	// And the one input watchscope rejects outright.
	if _, err := watchscope.Parse("a", "b"); err == nil {
		t.Error("watchscope no longer rejects both modes being set — the " +
			"\"mutually exclusive\" conflict branch is now reasoning about a state " +
			"that can actually run, and must be revisited")
	}
}
