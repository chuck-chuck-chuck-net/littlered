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

// The ordered cleanup plan behind `make e2e-harness-clean`.
//
// Why this is a decision and not a shell loop: a multi-site array leaves state
// on the SITES as well as the hub, and the order in which the three families go
// away is load-bearing. The per-item operator holds the multisite-teardown
// finalizer that sweeps the spoke workloads, so it has to outlive everything it
// owns — on both sides. Deleting its namespace first strands the hub namespace
// AND the site namespaces in Terminating until someone edits a finalizer by
// hand. That lesson was already learned once on the hub (M2); the site half is
// the same lesson one cluster further out.
//
// The standing rule still holds: the harness never destroys forensic evidence
// unless explicitly told to. `e2e-harness-clean` IS that instruction — it is the
// one verb whose entire purpose is "discard this array" — so it may delete, and
// it must be equally deliberate on the site side: exact namespace NAMES, never a
// prefix sweep, so it cannot reach another array's parked evidence on a shared
// lab site. Nothing else triggers it: the run-scoped teardown phase stays
// report-only, precisely because a Job cannot schedule a successor and a
// deleting phase would therefore sometimes silently not run.

import (
	"fmt"
	"strings"
)

// The wire format of a plan step, as the shell recipe reads it.
//
// The separator is deliberately NOT a tab. Several fields are legitimately empty
// (a hub step has no site; a site step has no selector), and tab is an IFS
// *whitespace* character — so `read` collapses runs of it and silently shifts
// every field after the first empty one. ASCII unit separator cannot appear in a
// namespace name, a label selector, or the prose, and is not IFS whitespace.
const (
	CleanupFieldSep   = "\x1f"
	CleanupFieldCount = 7
)

// CleanupTarget is one ordered step of the cleanup.
type CleanupTarget struct {
	// Site is the site whose cluster this step runs against; "" means the hub.
	Site string
	// Selector selects hub namespaces by label. Hub steps use it because it also
	// catches leftovers from a driver pod that died mid-item.
	Selector string
	// Names are exact namespace names, used for the sites. Deliberately not a
	// selector: the site-side namespaces are created by the e2e suite and carry
	// no harness labels, and a name prefix glob on a shared lab site would match
	// another array's parked evidence.
	Names []string
	// Wait means: block until the namespaces have finished terminating before
	// moving on. Set wherever a finalizer can strand the next step.
	Wait bool
	// Why is printed before the step runs. An unexplained destructive step is
	// how a habit of not reading them starts.
	Why string
	// RequireGone is a hub label selector that must match NO namespaces before
	// this step may run; otherwise the step is skipped, loudly.
	//
	// Ordering alone is not enough for the operator step. If the instance
	// namespaces did not finish terminating within their wait, deleting the
	// operator anyway kills the only process that can release their finalizers,
	// and they are stranded in Terminating forever. Observed live, on exactly
	// the parked multi-site array this plan was written for.
	RequireGone string
}

// CleanupPlan returns the ordered cleanup steps for this array across the hub
// and the given sites, in inventory order.
//
// A multi-site array cleaned without a site list would silently leave every site
// namespace behind — the exact gap this closes — so that is an error, not a
// best-effort partial clean.
func (l List) CleanupPlan(sites []string) ([]CleanupTarget, error) {
	needsLab := l.NeedsLab()
	if needsLab && len(sites) == 0 {
		return nil, fmt.Errorf(
			"array %q has multi-site items, so cleaning it must also reach the sites, "+
				"but no site inventory was given — re-run with E2E_HARNESS_LAB=<your lab inventory>",
			l.Array)
	}

	// 1. The hub INSTANCE namespaces. First, because their LittleRed CRs carry
	//    the operator's finalizer and the operator is still running: deleting
	//    the CR is what triggers the spoke sweep in the first place.
	plan := []CleanupTarget{{
		Selector: LabelArray + "=" + l.Array + ",!" + LabelRole,
		Wait:     true,
		Why: "the hub instance namespaces: their LittleRed CRs hold the operator's " +
			"finalizer, so they can only finish terminating while their own operator runs",
	}}

	// 2. Every site. After the hub CRs, so the operator's own sweep has had its
	//    chance; and this is also what UNBLOCKS a hub namespace stuck because a
	//    site was unreachable — once the site namespace is gone, the sweep
	//    succeeds trivially and the finalizer is released.
	//
	//    Only the item TEST namespaces: the item's operator lives on the hub, so
	//    naming its namespace here would be a confusing no-op.
	if needsLab {
		var names []string
		for i := range l.Items {
			r, err := l.Resolve(i)
			if err != nil {
				continue
			}
			names = append(names, r.Namespace)
		}
		for _, site := range sites {
			plan = append(plan, CleanupTarget{
				Site:  site,
				Names: names,
				Wait:  true,
				Why: "the instance namespaces this array created on site " + site +
					" — deleted by exact name, never by prefix, so a shared lab site's " +
					"other arrays are untouched",
			})
		}
	}

	// 3. The hub OPERATOR namespaces. Last: the operator must outlive both the
	//    hub CRs and the site workloads it sweeps. Nothing holds a finalizer on
	//    this namespace, so there is nothing to wait for.
	plan = append(plan, CleanupTarget{
		Selector: LabelArray + "=" + l.Array + "," + LabelRole + "=operator",
		Why: "the per-item operator namespaces, last of all: the operator owns the " +
			"finalizer that sweeps the spokes, so it has to outlive everything above",
		// ...and it must still be there if anything above did NOT finish. Skipping
		// this step leaves a live operator that can still release a finalizer; not
		// skipping it strands the namespace permanently.
		RequireGone: LabelArray + "=" + l.Array + ",!" + LabelRole,
	})
	return plan, nil
}

// Line renders one step as the single record the shell recipe consumes:
//
//	site SEP kubecontext SEP wait SEP selector SEP names SEP require-gone SEP why
//
// The kubecontext is passed in rather than stored: the seam names the SITE (its
// logical name, which is what the operator and the CR use), while how to REACH
// it from a workstation is inventory configuration.
func (c CleanupTarget) Line(kubecontext string) string {
	if c.Site == "" {
		kubecontext = ""
	}
	return strings.Join([]string{
		c.Site,
		kubecontext,
		fmt.Sprintf("%t", c.Wait),
		c.Selector,
		strings.Join(c.Names, " "),
		c.RequireGone,
		strings.ReplaceAll(c.Why, "\n", " "),
	}, CleanupFieldSep)
}

// String renders one step for the operator running the clean.
func (c CleanupTarget) String() string {
	where := "hub"
	if c.Site != "" {
		where = "site " + c.Site
	}
	what := c.Selector
	if what == "" {
		what = strings.Join(c.Names, " ")
	}
	wait := ""
	if c.Wait {
		wait = " (waiting)"
	}
	return fmt.Sprintf("%s: %s%s\n    %s", where, what, wait, c.Why)
}
