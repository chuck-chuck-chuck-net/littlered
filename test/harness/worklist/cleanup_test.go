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
)

// `e2e-harness-clean` is the EXPLICIT instruction to discard evidence, so it may
// delete — but a multi-site array leaves state on the sites too, and until now
// clean only reached the hub. The ordering is the whole subject: get it wrong
// and a namespace hangs in Terminating until someone edits a finalizer by hand.

var labSites = []string{"site-a", "site-b"}

func TestCleanupPlanOrderingIsTheOperatorLifetime(t *testing.T) {
	multi, err := Parse([]byte(multiSiteList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	plan, err := multi.CleanupPlan(labSites)
	if err != nil {
		t.Fatalf("CleanupPlan: %v", err)
	}

	// hub instance namespaces, then every site, then the hub OPERATOR namespaces.
	//
	// The rule underneath: the per-item operator must outlive everything it owns.
	// It holds the multisite-teardown finalizer that sweeps the spoke workloads,
	// so killing its namespace first strands both the hub namespace AND the site
	// namespaces in Terminating.
	if len(plan) != 2+len(labSites) {
		t.Fatalf("plan has %d steps, want %d (hub instance, one per site, hub operator):\n%+v",
			len(plan), 2+len(labSites), plan)
	}
	if plan[0].Site != "" || !strings.Contains(plan[0].Selector, "!"+LabelRole) {
		t.Errorf("step 0 = %+v, want the HUB instance namespaces (role-less) first — "+
			"their CRs carry the finalizer that sweeps the spokes", plan[0])
	}
	for i, site := range labSites {
		s := plan[1+i]
		if s.Site != site {
			t.Errorf("step %d targets site %q, want %q (inventory order)", 1+i, s.Site, site)
		}
	}
	last := plan[len(plan)-1]
	if last.Site != "" || !strings.Contains(last.Selector, LabelRole+"=operator") {
		t.Errorf("last step = %+v, want the HUB OPERATOR namespaces — the operator must "+
			"outlive both the hub CRs and the site workloads it sweeps", last)
	}
}

func TestCleanupPlanWaitsWhereAFinalizerCanStrandIt(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	plan, err := multi.CleanupPlan(labSites)
	if err != nil {
		t.Fatalf("CleanupPlan: %v", err)
	}

	if !plan[0].Wait {
		t.Error("the hub instance step must WAIT: a LittleRed CR carries the operator's " +
			"finalizer, so the namespace only finishes terminating while the operator runs")
	}
	for _, s := range plan[1 : len(plan)-1] {
		if !s.Wait {
			t.Errorf("site step %q must WAIT: removing the operator while a site namespace "+
				"is still terminating strands the spoke sweep", s.Site)
		}
	}
	if plan[len(plan)-1].Wait {
		t.Error("the hub operator step must NOT wait: nothing holds a finalizer on it, " +
			"and blocking there only slows the teardown down")
	}
}

// The site step deletes NAMES, not a label glob. The harness knows exactly which
// namespaces it would have created, and a prefix sweep on a shared lab site would
// happily delete another array's parked evidence.
func TestCleanupPlanTargetsSiteNamespacesByExactName(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	plan, _ := multi.CleanupPlan(labSites)

	want := make([]string, 0, len(multi.Items))
	for i := range multi.Items {
		r, _ := multi.Resolve(i)
		want = append(want, r.Namespace)
	}

	for _, s := range plan {
		if s.Site == "" {
			continue
		}
		if !slices.Equal(s.Names, want) {
			t.Errorf("site %q targets %v, want the item TEST namespaces %v", s.Site, s.Names, want)
		}
		if s.Selector != "" {
			t.Errorf("site %q uses selector %q — a label glob on a shared site could match "+
				"another array's parked evidence; delete by exact name", s.Site, s.Selector)
		}
		// The item's OPERATOR namespace exists only on the hub (the operator is
		// deployed there); naming it on a site would be a confusing no-op.
		for i := range multi.Items {
			r, _ := multi.Resolve(i)
			if slices.Contains(s.Names, r.OperatorNamespace) {
				t.Errorf("site %q names the operator namespace %q, which never exists on a site",
					s.Site, r.OperatorNamespace)
			}
		}
	}
}

func TestCleanupPlanForASingleSiteArrayNeverTouchesSites(t *testing.T) {
	single, err := Parse([]byte(singleSiteList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// Even when a lab happens to be configured, a classic array put nothing there.
	plan, err := single.CleanupPlan(labSites)
	if err != nil {
		t.Fatalf("CleanupPlan: %v", err)
	}
	if len(plan) != 2 {
		t.Fatalf("plan has %d steps, want 2 (hub instance, hub operator):\n%+v", len(plan), plan)
	}
	for _, s := range plan {
		if s.Site != "" {
			t.Errorf("a single-site array must not reach site %q", s.Site)
		}
	}
}

// A multi-site array cleaned without a lab inventory would SILENTLY leave the
// site namespaces behind — the exact gap this change closes. Refuse instead.
func TestCleanupPlanRefusesAMultiSiteArrayWithNoSites(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	_, err := multi.CleanupPlan(nil)
	if err == nil {
		t.Fatal("cleaning a multi-site array with no site list must fail: it would " +
			"silently leave every site namespace standing")
	}
	if !strings.Contains(err.Error(), "E2E_HARNESS_LAB") {
		t.Errorf("error %q does not say how to fix it (E2E_HARNESS_LAB)", err)
	}
}

// Every step must explain itself: the plan is rendered to the operator running
// `e2e-harness-clean`, and an unexplained destructive step is how a habit of
// not reading them starts.
func TestCleanupPlanStepsExplainThemselves(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	plan, _ := multi.CleanupPlan(labSites)
	for i, s := range plan {
		if strings.TrimSpace(s.Why) == "" {
			t.Errorf("step %d (%+v) has no explanation", i, s)
		}
	}
}

// The plan crosses into a shell recipe as one line per step, and several fields
// are legitimately EMPTY (a hub step has no site; a site step has no selector).
// TAB looked like the obvious separator and is wrong: tab is an IFS *whitespace*
// character, so `read` collapses runs of them and silently shifts every field
// after the first empty one. Found by testing the split rather than eyeballing
// the printed output, which looks perfectly fine either way.
func TestCleanupPlanLineSurvivesEmptyFields(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	plan, err := multi.CleanupPlan(labSites)
	if err != nil {
		t.Fatalf("CleanupPlan: %v", err)
	}

	if strings.ContainsAny(CleanupFieldSep, " \t\n") {
		t.Fatalf("the field separator %q is IFS whitespace: a shell `read` collapses "+
			"runs of it, so an empty field shifts every field after it", CleanupFieldSep)
	}

	for i, s := range plan {
		line := s.Line("some-context")
		if strings.Contains(line, "\n") {
			t.Errorf("step %d renders a multi-line record", i)
		}
		fields := strings.Split(line, CleanupFieldSep)
		if len(fields) != CleanupFieldCount {
			t.Errorf("step %d splits into %d fields, want %d: %q", i, len(fields), CleanupFieldCount, line)
		}
		// The separator must not occur inside any field, or the split is a lie.
		for _, f := range fields {
			if strings.Contains(f, CleanupFieldSep) {
				t.Errorf("step %d: field %q contains the separator", i, f)
			}
		}
	}

	// Specifically: the hub steps DO have empty leading fields, which is the case
	// that broke.
	hub := plan[0]
	if hub.Site != "" {
		t.Fatal("expected step 0 to be a hub step")
	}
	f := strings.Split(hub.Line(""), CleanupFieldSep)
	if f[0] != "" || f[1] != "" {
		t.Errorf("hub step's empty site/context fields did not survive the round trip: %q", f)
	}
	if f[2] != LabelValueTrue {
		t.Errorf("hub step's wait field landed in the wrong column: %q", f)
	}
}

// The operator step must be CONDITIONAL, not merely last.
//
// Found live: cleaning a parked multi-site array, the hub instance namespace did
// not finish terminating within the wait, the recipe continued as designed, and
// the final step then deleted the operator namespace anyway — killing the only
// process that could release the CR's finalizers. The namespace is then stranded
// in Terminating forever, needing a manual finalizer edit. Ordering alone is not
// enough: the last step needs a precondition.
//
// (The same hazard predates multi-site — the hub-only clean deleted the operator
// namespace unconditionally after its own timeout. The per-item driver always had
// the rule; the array-level clean did not.)
func TestCleanupPlanGuardsTheOperatorStepOnTheInstancesBeingGone(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	plan, err := multi.CleanupPlan(labSites)
	if err != nil {
		t.Fatalf("CleanupPlan: %v", err)
	}

	last := plan[len(plan)-1]
	if last.RequireGone == "" {
		t.Fatal("the operator step has no precondition: if the instance namespaces are " +
			"still terminating, deleting the operator strands them forever")
	}
	// It must be the SAME set the first step deletes — the instance namespaces.
	if last.RequireGone != plan[0].Selector {
		t.Errorf("operator step requires %q gone, but step 0 deletes %q — the precondition "+
			"must name exactly the namespaces whose finalizer the operator releases",
			last.RequireGone, plan[0].Selector)
	}

	// No other step carries one: they are ordered, not conditional.
	for i, s := range plan[:len(plan)-1] {
		if s.RequireGone != "" {
			t.Errorf("step %d (%+v) has a precondition it does not need", i, s)
		}
	}

	// And it has to survive the trip through the shell.
	for _, s := range plan {
		f := strings.Split(s.Line("ctx"), CleanupFieldSep)
		if len(f) != CleanupFieldCount {
			t.Fatalf("Line() emits %d fields, want %d", len(f), CleanupFieldCount)
		}
	}
	lf := strings.Split(last.Line(""), CleanupFieldSep)
	if !strings.Contains(strings.Join(lf, "|"), last.RequireGone) {
		t.Errorf("the precondition is not in the rendered record: %v", lf)
	}
}
