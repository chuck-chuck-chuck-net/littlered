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

// The launch gate against a CONFLICTING OPERATOR — the third of the three, next
// to the image preflight and the parked-evidence guard.
//
// Why it earns a gate of its own: an operator whose watch scope overlaps the
// array reconciles the items' CRs alongside their own per-item operators. That
// is precisely the double-reconciliation design §3 exists to prevent — and it
// does not fail cleanly. Its victim is the PARKED FORENSIC STATE: a foreign
// operator keeps acting on a parked namespace long after the item stopped, so
// what is eventually inspected is not what failed. Until now the only defence
// was a sentence in the README, which is exactly the "the harness knows
// something and does not act on it" shape this gate removes.

import (
	"fmt"
	"sort"
	"strings"
)

// The scoping modes an operator can run in (ADR-014). Re-implemented here rather
// than imported from internal/watchscope: that package pulls controller-runtime
// (for cache.Options), which would land in the lrharness binary and in the
// driver image build for the sake of one string split. TestScopeClassification-
// MatchesTheOperators imports the real package and pins the two together, so the
// shipped binary stays decoupled while a semantic change upstream still breaks
// the build in the tests.
type scopeMode string

const (
	scopeNone  scopeMode = "none"  // no scoping: watches every namespace
	scopeAllow scopeMode = "allow" // WATCH_NAMESPACE: watches only these
	scopeDeny  scopeMode = "deny"  // IGNORE_NAMESPACE: watches all but these
)

// classifyScope mirrors watchscope.Parse's splitting: comma-separated, trimmed,
// empties dropped, deduped, sorted.
func classifyScope(watch, ignore string) (scopeMode, []string) {
	w, i := splitScope(watch), splitScope(ignore)
	switch {
	case len(w) > 0:
		return scopeAllow, w
	case len(i) > 0:
		return scopeDeny, i
	default:
		return scopeNone, nil
	}
}

func splitScope(raw string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, strings.Count(raw, ",")+1)
	for part := range strings.SplitSeq(raw, ",") {
		ns := strings.TrimSpace(part)
		if ns == "" {
			continue
		}
		if _, dup := seen[ns]; dup {
			continue
		}
		seen[ns] = struct{}{}
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// ObservedOperator is one operator Deployment found on the target cluster,
// reduced to what the scope decision needs.
type ObservedOperator struct {
	Namespace string
	Name      string
	// WatchNamespace / IgnoreNamespace are the raw env values from the manager
	// container, exactly as the operator process would read them.
	WatchNamespace  string
	IgnoreNamespace string
	// Replicas is the DESIRED replica count. Zero means the operator is not
	// running — which is also the documented remedy, so it must not be a
	// conflict, or the gate would keep refusing after the user has fixed it.
	Replicas int32
}

// OperatorConflict is one operator whose scope overlaps the array.
type OperatorConflict struct {
	Operator ObservedOperator
	// Reason is the human sentence for the refusal.
	Reason string
	// Overlap lists the array namespaces this operator would also reconcile
	// (empty for a cluster-scoped one, which would reconcile all of them).
	Overlap []string
}

// Namespaces returns every namespace the array will create — BOTH families.
// The operator namespaces belong in the set as much as the test ones: an
// operator scoped to one of them would reconcile whatever a later milestone
// puts there, and it would certainly outlive a parked item.
func (l List) Namespaces() []string {
	out := make([]string, 0, 2*len(l.Items))
	for i := range l.Items {
		r, err := l.Resolve(i)
		if err != nil {
			continue // Parse's business; a caller here has already validated.
		}
		out = append(out, r.Namespace, r.OperatorNamespace)
	}
	return out
}

// ConflictingOperators returns the operators that would fight this array,
// sorted by namespace/name so a refusal reads the same way twice.
func (l List) ConflictingOperators(observed []ObservedOperator) []OperatorConflict {
	arrayNS := l.Namespaces()
	own := map[string]bool{}
	for i := range l.Items {
		if r, err := l.Resolve(i); err == nil {
			own[r.OperatorNamespace] = true
		}
	}

	var conflicts []OperatorConflict
	for _, op := range observed {
		if op.Replicas == 0 {
			// Not running. This is the documented fix, so recognising it is what
			// lets the gate be satisfiable rather than merely loud.
			continue
		}
		if own[op.Namespace] {
			// This array's OWN operator, from a previous run of the same array
			// name — a parked one. The driver's clean-slate step deletes that
			// namespace before the item runs, so refusing here would block the
			// documented parked-clobber path for no safety gain.
			continue
		}

		watch, ignore := strings.TrimSpace(op.WatchNamespace), strings.TrimSpace(op.IgnoreNamespace)
		if watch != "" && ignore != "" {
			// watchscope.Parse rejects this pair and the operator exits, so in
			// principle it reconciles nothing. But "it is crash-looping" is a
			// guess about another binary's startup behaviour, and a guard that
			// guesses wrong here silently permits the corruption it exists to
			// prevent. Fail closed and let a human look.
			conflicts = append(conflicts, OperatorConflict{
				Operator: op,
				Reason: "WATCH_NAMESPACE and IGNORE_NAMESPACE are both set, which is " +
					"mutually exclusive — its effective scope cannot be determined",
			})
			continue
		}

		mode, scoped := classifyScope(watch, ignore)
		switch mode {
		case scopeNone:
			conflicts = append(conflicts, OperatorConflict{
				Operator: op,
				Reason: "cluster-scoped (no WATCH_NAMESPACE / IGNORE_NAMESPACE), so it " +
					"watches every namespace this array creates",
			})
		case scopeAllow:
			if overlap := intersect(scoped, arrayNS); len(overlap) > 0 {
				conflicts = append(conflicts, OperatorConflict{
					Operator: op,
					Reason:   "watches namespace(s) this array will use",
					Overlap:  overlap,
				})
			}
		case scopeDeny:
			if overlap := notIn(arrayNS, scoped); len(overlap) > 0 {
				conflicts = append(conflicts, OperatorConflict{
					Operator: op,
					Reason: "watches everything except its IGNORE_NAMESPACE list, which " +
						"does not exclude namespace(s) this array will use",
					Overlap: overlap,
				})
			}
		}
	}

	sort.Slice(conflicts, func(a, b int) bool {
		if conflicts[a].Operator.Namespace != conflicts[b].Operator.Namespace {
			return conflicts[a].Operator.Namespace < conflicts[b].Operator.Namespace
		}
		return conflicts[a].Operator.Name < conflicts[b].Operator.Name
	})
	return conflicts
}

// intersect returns the members of `set` that appear in `other`, sorted.
func intersect(set, other []string) []string {
	in := map[string]bool{}
	for _, s := range other {
		in[s] = true
	}
	var out []string
	for _, s := range set {
		if in[s] {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// notIn returns the members of `set` absent from `excluded`, sorted.
func notIn(set, excluded []string) []string {
	skip := map[string]bool{}
	for _, s := range excluded {
		skip[s] = true
	}
	var out []string
	for _, s := range set {
		if !skip[s] {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// OperatorRefusalMessage renders the launch refusal: what is in the way, why it
// matters, and both ways forward — the same contract as RefusalMessage.
func OperatorRefusalMessage(array string, conflicts []OperatorConflict) string {
	var b strings.Builder
	fmt.Fprintf(&b, "REFUSING TO LAUNCH: array %q would run beside %d operator(s) that reconcile "+
		"its namespaces:\n\n", array, len(conflicts))
	for _, c := range conflicts {
		fmt.Fprintf(&b, "    %s/%s\n        %s\n", c.Operator.Namespace, c.Operator.Name, c.Reason)
		if len(c.Overlap) > 0 {
			fmt.Fprintf(&b, "        overlaps: %s\n", strings.Join(c.Overlap, ", "))
		}
	}
	b.WriteString(`
Every work item deploys its OWN namespace-scoped operator. A second operator
watching the same namespaces double-reconciles the items' CRs — the failure mode
the per-item operator exists to prevent (design §3) — and it keeps acting on a
PARKED namespace long after the item stopped, so what you eventually inspect is
not what failed. It corrupts the evidence rather than failing cleanly, which is
why this is a refusal and not a warning.

Two ways forward:

  1. Scale it to zero for the run, and back afterwards:

`)
	for _, c := range conflicts {
		fmt.Fprintf(&b, "         kubectl -n %s scale deploy %s --replicas=0\n",
			c.Operator.Namespace, c.Operator.Name)
	}
	b.WriteString(`
  2. Run the array on a cluster that hosts no operator of its own.

Or, if you are certain the overlap is harmless:

         make e2e-harness-launch E2E_HARNESS_ALLOW_OPERATOR_CONFLICT=true
`)
	return b.String()
}
