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
	"strings"
	"testing"

	"github.com/littlered-operator/littlered-operator/test/harness/fixture"
)

// M3 additions to the work-list seam: an item may name a multi-site topology
// FIXTURE (matrix §3), which makes the array need the lab substrate — the
// four-context kubeconfig credentials and the lab inventory — and introduces
// the run-scoped setup phase around the array (design §9.2.4).

const multiSiteList = `
array: msbring
items:
  - name: f2k4-a
    fixture: F2-K4
    labelFilter: cluster-behaviour
    focus: Bring-up and basic operations
  - name: f2k4-b
    fixture: F2-K4
    labelFilter: cluster-behaviour
    focus: Bring-up and basic operations
`

// gateArray is the array name used by the parking-marker regression test.
const gateArray = "msgate"

// testNS is a placeholder harness namespace for the render tests (testImage is
// declared beside the M1 render tests, in worklist_test.go).
const testNS = "ns"

const singleSiteList = `
array: classic
items:
  - name: pdb
    labelFilter: pdb
`

func TestParseValidatesTheFixtureName(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "a known fixture",
			yaml: multiSiteList,
		},
		{
			name: "an unknown fixture is refused at launch",
			yaml: "array: a\nitems:\n  - {name: x, fixture: F9-K99, labelFilter: l}\n",
			// The refusal must name the item AND list the alternatives: a typo
			// found at item 37 of an overnight array costs the whole night.
			wantErr: "F9-K99",
		},
		{
			name:    "an unknown fixture lists the known ones",
			yaml:    "array: a\nitems:\n  - {name: x, fixture: typo, labelFilter: l}\n",
			wantErr: fixture.F2K4,
		},
		{
			name: "no fixture is fine — that is a single-site item",
			yaml: singleSiteList,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Parse() = %v, want success", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Parse() = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Parse() = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestResolveCarriesTheFixtureToTheShell(t *testing.T) {
	l, err := Parse([]byte(multiSiteList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	r, err := l.Resolve(1)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if r.Fixture != fixture.F2K4 {
		t.Errorf("Fixture = %q, want %q", r.Fixture, fixture.F2K4)
	}
	if got := r.ShellExport(); !strings.Contains(got, "export LRH_FIXTURE='"+fixture.F2K4+"'") {
		t.Errorf("ShellExport() does not carry LRH_FIXTURE:\n%s", got)
	}

	// A single-site item must export an EMPTY fixture rather than omitting the
	// variable: the driver branches on it, and an unset variable under `set -u`
	// is a different failure from an empty one.
	s, err := Parse([]byte(singleSiteList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	rs, _ := s.Resolve(0)
	if !strings.Contains(rs.ShellExport(), "export LRH_FIXTURE=''") {
		t.Errorf("a single-site item must still export an empty LRH_FIXTURE:\n%s", rs.ShellExport())
	}
}

func TestNeedsLabIsDrivenByTheItems(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	single, _ := Parse([]byte(singleSiteList))

	if !multi.NeedsLab() {
		t.Error("a list with a fixture needs the lab substrate")
	}
	if single.NeedsLab() {
		t.Error("a single-site list must NOT need the lab substrate — mounting a lab " +
			"inventory that was never bootstrapped would fail every item at pod start")
	}
}

// The lab volumes are mounted only when the array needs them. A classic soak
// array must stay runnable on any plain workload cluster, where neither the lab
// ConfigMap nor the site-credential Secret exists — and a Job referencing a
// missing ConfigMap never starts a single pod.
func TestRenderJobMountsTheLabOnlyWhenNeeded(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	single, _ := Parse([]byte(singleSiteList))

	base := func(l List) JobParams {
		return JobParams{List: l, Namespace: testNS, Image: testImage, Parallelism: 1}
	}

	withLab, err := RenderJob(base(multi))
	if err != nil {
		t.Fatalf("RenderJob(multi): %v", err)
	}
	for _, want := range []string{LabConfigMapName, SiteCredentialSecretName, "/lab", "/site-creds"} {
		if !strings.Contains(withLab, want) {
			t.Errorf("multi-site Job does not reference %q:\n%s", want, withLab)
		}
	}

	withoutLab, err := RenderJob(base(single))
	if err != nil {
		t.Fatalf("RenderJob(single): %v", err)
	}
	for _, unwanted := range []string{LabConfigMapName, SiteCredentialSecretName, "/site-creds"} {
		if strings.Contains(withoutLab, unwanted) {
			t.Errorf("single-site Job references %q, which does not exist on a plain "+
				"workload cluster:\n%s", unwanted, withoutLab)
		}
	}
}

// The run-scoped setup phase (design §9.2.4). Nothing needs it until M5's
// resident monitors, but it is introduced now so they slot in without
// restructuring — and M3 already gets value from it: one site-reachability
// check for the whole array instead of N items each burning their own timeout
// discovering the same broken site.
func TestRenderBundleAddsTheRunScopedSetupPhase(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	out, err := RenderBundle(BundleParams{
		JobParams:    JobParams{List: multi, Namespace: testNS, Image: testImage, Parallelism: 1},
		WorkListYAML: multiSiteList,
	})
	if err != nil {
		t.Fatalf("RenderBundle: %v", err)
	}

	setupJob := SetupJobName(multi.Array)
	if !strings.Contains(out, "name: "+setupJob) {
		t.Errorf("bundle has no setup Job %q:\n%s", setupJob, out)
	}
	if !strings.Contains(out, "value: setup") {
		t.Error("the setup Job must run the driver in the `setup` phase")
	}
	// The ordering has to be enforced IN-CLUSTER: a Job cannot depend on
	// another, and a Makefile-side wait would put the ordering back on the
	// workstation the whole design exists to get off (§9.3).
	if !strings.Contains(out, "LRH_SETUP_JOB") {
		t.Error("the array Job must be told to wait for the setup Job; without it the " +
			"phase is decorative and the items race the setup")
	}

	// A single-site array declares no setup phase, so it renders none.
	single, _ := Parse([]byte(singleSiteList))
	plain, err := RenderBundle(BundleParams{
		JobParams:    JobParams{List: single, Namespace: testNS, Image: testImage, Parallelism: 1},
		WorkListYAML: singleSiteList,
	})
	if err != nil {
		t.Fatalf("RenderBundle(single): %v", err)
	}
	if strings.Contains(plain, SetupJobName(single.Array)) {
		t.Errorf("a single-site array must not render a setup Job:\n%s", plain)
	}
	if strings.Contains(plain, "LRH_SETUP_JOB") {
		t.Error("a single-site array's items must not wait for a setup Job that does not exist")
	}
}

func TestRenderTeardownJob(t *testing.T) {
	multi, _ := Parse([]byte(multiSiteList))
	out, err := RenderTeardownJob(JobParams{List: multi, Namespace: testNS, Image: testImage, Parallelism: 1})
	if err != nil {
		t.Fatalf("RenderTeardownJob: %v", err)
	}
	if !strings.Contains(out, "name: "+TeardownJobName(multi.Array)) {
		t.Errorf("teardown Job is misnamed:\n%s", out)
	}
	if !strings.Contains(out, "value: teardown") {
		t.Error("the teardown Job must run the driver in the `teardown` phase")
	}
	// It runs after the array, so it must reach the sites the same way the items
	// did — the lab mounts are not optional for it.
	if !strings.Contains(out, SiteCredentialSecretName) {
		t.Error("the teardown Job needs the site credentials to see what the run left behind")
	}

	// A teardown for a single-site array has nothing to inspect.
	single, _ := Parse([]byte(singleSiteList))
	sp := JobParams{List: single, Namespace: testNS, Image: testImage, Parallelism: 1}
	if _, err := RenderTeardownJob(sp); err == nil {
		t.Error("rendering a teardown Job for an array with no lab substrate must fail " +
			"rather than produce a Job that mounts nothing and reports nothing")
	}
}

// The three Job names of one array must be distinct and derived, or `kubectl
// delete job <array>` would reap the wrong one.
func TestPhaseJobNamesAreDistinct(t *testing.T) {
	const array = "msbring"
	names := map[string]string{
		"array":    array,
		"setup":    SetupJobName(array),
		"teardown": TeardownJobName(array),
	}
	seen := map[string]string{}
	for role, n := range names {
		if prev, dup := seen[n]; dup {
			t.Errorf("phase %q and %q both render Job name %q", role, prev, n)
		}
		seen[n] = role
		if !strings.HasPrefix(n, array) {
			t.Errorf("phase %q renders Job %q, which does not carry the array name", role, n)
		}
	}
}

// A blocked item never created a namespace, so there is nothing parked — the
// run-scoped-setup gate and the unresolved-index path both record a FAIL with
// an empty namespace. Rendering "[PARKED]" there says evidence was kept when
// none was, which is the one direction the parking story must never lie in.
// (Observed on the first M3 gate run: "0  f2k4-a  FAIL  103s    [PARKED]".)
func TestRollupDoesNotClaimParkedWithNothingToPark(t *testing.T) {
	s := Rollup(gateArray, 1, []Result{{
		Array: gateArray, Index: 0, Name: "f2k4-a",
		Verdict: VerdictFail, Namespace: "", Parked: true,
	}})
	if strings.Contains(s.String(), "[PARKED]") {
		t.Errorf("rollup claims a parked namespace for an item that has none:\n%s", s)
	}

	// The real parking case must still be marked.
	withNS := Rollup(gateArray, 1, []Result{{
		Array: gateArray, Index: 0, Name: "blocked-none",
		Verdict: VerdictFail, Namespace: "lr-harness-" + gateArray + "-0-doomed", Parked: true,
	}})
	if !strings.Contains(withNS.String(), "[PARKED]") {
		t.Errorf("rollup does not mark a genuinely parked namespace:\n%s", withNS)
	}
}
