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
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"
)

const (
	regOpRename = "SentinelMasterNameRename"
	// The path the registry declares; named here too so the fixtures read as data.
	regOpRenamePath = fieldPathSentinelMasterName
	// Not registered — the fixtures use it as the "second CR-resident operation"
	// that must make the agreement test fail until its term is added.
	fixtureAuthPath = "spec.auth.enabled"
)

// ---------------------------------------------------------------------------
// The v1 registry itself
// ---------------------------------------------------------------------------

// renameEntry returns the registry's rename entry, failing loudly if it is absent.
func renameEntry(t *testing.T) heavyOperation {
	t.Helper()
	for _, op := range heavyOperations {
		if op.Name == regOpRename {
			return op
		}
	}
	t.Fatalf("registry has no %q entry", regOpRename)
	return heavyOperation{}
}

// TestRegistryV1Contents pins the one entry the plan specifies (§3). The registry is
// an API surface (ADR-020 Consequences), so its contents are asserted, not assumed.
func TestRegistryV1Contents(t *testing.T) {
	if len(heavyOperations) != 1 {
		t.Fatalf("registry has %d entries, want exactly 1 (v1 = %s)", len(heavyOperations), regOpRename)
	}
	op := heavyOperations[0]
	if op.Name != regOpRename {
		t.Errorf("Name = %q, want %q", op.Name, regOpRename)
	}
	if op.FieldPath != regOpRenamePath {
		t.Errorf("FieldPath = %q, want %q", op.FieldPath, regOpRenamePath)
	}
	if !reflect.DeepEqual(op.Modes, []string{ModeSentinel}) {
		t.Errorf("Modes = %v, want [%s]", op.Modes, ModeSentinel)
	}
	if op.StallAfter != 15*time.Minute {
		t.Errorf("StallAfter = %v, want 15m", op.StallAfter)
	}
	if len(op.Requires) != 0 {
		t.Errorf("Requires = %v, want none — the rename commutes with everything registered", op.Requires)
	}
	if !strings.Contains(op.Citation, "LR-050") {
		t.Errorf("Citation = %q, want the LR-050 measurement", op.Citation)
	}
}

// TestRenameApplies — Applies is the mode/feature filter, and the rename is meaningful
// in sentinel mode only.
func TestRenameApplies(t *testing.T) {
	op := renameEntry(t)
	cases := []struct {
		mode string
		want bool
	}{
		{ModeSentinel, true},
		{ModeStandalone, false},
		{ModeCluster, false},
		{ModeFailover, false},
	}
	for _, c := range cases {
		lr := &littleredv1alpha1.LittleRed{Spec: littleredv1alpha1.LittleRedSpec{Mode: c.mode}}
		if got := op.Applies(lr); got != c.want {
			t.Errorf("Applies(mode=%s) = %v, want %v", c.mode, got, c.want)
		}
	}
	// A sentinel instance that omits spec.sentinel entirely still APPLIES: it is
	// running under the legacy name, and setting the field is a rename.
	lr := &littleredv1alpha1.LittleRed{Spec: littleredv1alpha1.LittleRedSpec{Mode: ModeSentinel}}
	if !op.Applies(lr) {
		t.Errorf("Applies(sentinel, spec.sentinel absent) = false, want true — it runs under %q",
			littleredv1alpha1.LegacySentinelMasterName)
	}
}

// TestRenameFingerprint — the fingerprint identifies the EFFECTIVE master name (the
// value Rule N reconciles the Sentinels to), is UID-keyed, and never contains the
// value in plaintext.
func TestRenameFingerprint(t *testing.T) {
	op := renameEntry(t)
	const uidA = types.UID("11111111-1111-1111-1111-111111111111")
	const uidB = types.UID("22222222-2222-2222-2222-222222222222")

	fp := func(uid types.UID, name string) string {
		lr := &littleredv1alpha1.LittleRed{
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:     ModeSentinel,
				Sentinel: &littleredv1alpha1.SentinelSpec{MasterName: name},
			},
		}
		lr.UID = uid
		return op.Fingerprint(fingerprintInput{LR: lr, UID: uid})
	}

	first := fp(uidA, "prod.cache")
	for range 3 {
		if got := fp(uidA, "prod.cache"); got != first {
			t.Errorf("fingerprint is not deterministic: %q then %q", first, got)
		}
	}
	if fp(uidA, "prod.cache") == fp(uidA, "prod.cache2") {
		t.Errorf("two different master names share a fingerprint — a completed rename would match the next edit")
	}
	if fp(uidA, "prod.cache") == fp(uidB, "prod.cache") {
		t.Errorf("fingerprint is not UID-keyed")
	}
	if got := fp(uidA, "prod.cache"); strings.Contains(got, "prod") {
		t.Errorf("fingerprint %q leaks the value", got)
	}
	// It must equal the API helper's digest for the effective name — the registry
	// does not get its own hashing scheme.
	want := littleredv1alpha1.OperationFingerprint(uidA, regOpRename, "prod.cache")
	if got := fp(uidA, "prod.cache"); got != want {
		t.Errorf("fingerprint = %q, want OperationFingerprint(...) = %q", got, want)
	}
	// An instance with spec.sentinel absent fingerprints the LEGACY name, so an
	// operator upgrade over such a fleet sees no change (and seeding, not a run).
	bare := &littleredv1alpha1.LittleRed{Spec: littleredv1alpha1.LittleRedSpec{Mode: ModeSentinel}}
	bare.UID = uidA
	wantLegacy := littleredv1alpha1.OperationFingerprint(uidA, regOpRename, littleredv1alpha1.LegacySentinelMasterName)
	if got := op.Fingerprint(fingerprintInput{LR: bare, UID: uidA}); got != wantLegacy {
		t.Errorf("fingerprint(spec.sentinel absent) = %q, want the legacy-name digest %q", got, wantLegacy)
	}
}

// ---------------------------------------------------------------------------
// Structural invariants of ANY registry — the admission test, made mechanical
// ---------------------------------------------------------------------------

// TestEveryEntryHasCitation is ADR-020's admission clause B enforced by machine:
// "it feels risky, give it a window" cannot be admitted, because there is nothing to
// cite. Asserted over a fixture (so the check itself has teeth) and over the real
// registry (so no future row slips in without one).
func TestEveryEntryHasCitation(t *testing.T) {
	fixture := []heavyOperation{
		{Name: "Cited", Citation: "LR-050: measured window"},
		{Name: "Uncited"},
		{Name: "Blank", Citation: "   "},
	}
	got := operationsMissingCitation(fixture)
	want := []string{"Blank", "Uncited"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("operationsMissingCitation(fixture) = %v, want %v", got, want)
	}
	if got := operationsMissingCitation(heavyOperations); len(got) != 0 {
		t.Errorf("registry entries without a citation: %v — ADR-020 admission clause B", got)
	}
}

// TestRequiresTargetsExist — a Requires edge naming an operation that is not in the
// registry is a typo or a de-registration, and it would silently never be satisfiable.
func TestRequiresTargetsExist(t *testing.T) {
	fixture := []heavyOperation{
		{Name: "A", Requires: []string{"B", "Ghost"}},
		{Name: "B"},
		{Name: "C", Requires: []string{"Nope"}},
	}
	got := operationsWithDanglingRequires(fixture)
	want := []string{"A -> Ghost", "C -> Nope"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("operationsWithDanglingRequires(fixture) = %v, want %v", got, want)
	}
	if got := operationsWithDanglingRequires(heavyOperations); len(got) != 0 {
		t.Errorf("registry has dangling Requires edges: %v", got)
	}
}

// TestRequiresEdgesAreAcyclic — "a genuine cycle is DETECTABLE, where precedence
// integers accepted it silently by being unable to express it" (ADR-020, Alternative
// E). It is also the assumption M2.2's planner makes when it orders pending
// operations, so this is where that assumption is discharged.
//
// The check is a real traversal over fixture graphs; asserting it over a v1 registry
// with no edges at all would prove nothing.
func TestRequiresEdgesAreAcyclic(t *testing.T) {
	cases := []struct {
		name      string
		ops       []heavyOperation
		wantCycle bool
	}{
		{
			name: "no edges",
			ops:  []heavyOperation{{Name: "A"}, {Name: "B"}},
		},
		{
			name: "chain",
			ops:  []heavyOperation{{Name: "A", Requires: []string{"B"}}, {Name: "B", Requires: []string{"C"}}, {Name: "C"}},
		},
		{
			name: "diamond — a shared dependency is not a cycle",
			ops: []heavyOperation{
				{Name: "A", Requires: []string{"B", "C"}},
				{Name: "B", Requires: []string{"D"}},
				{Name: "C", Requires: []string{"D"}},
				{Name: "D"},
			},
		},
		{
			name:      "self edge",
			ops:       []heavyOperation{{Name: "A", Requires: []string{"A"}}},
			wantCycle: true,
		},
		{
			name: "two-cycle",
			ops: []heavyOperation{
				{Name: "A", Requires: []string{"B"}},
				{Name: "B", Requires: []string{"A"}},
			},
			wantCycle: true,
		},
		{
			name: "three-cycle behind an acyclic prefix",
			ops: []heavyOperation{
				{Name: "Root", Requires: []string{"A"}},
				{Name: "A", Requires: []string{"B"}},
				{Name: "B", Requires: []string{"C"}},
				{Name: "C", Requires: []string{"A"}},
			},
			wantCycle: true,
		},
		{
			name: "dangling edges must not be reported as a cycle",
			ops:  []heavyOperation{{Name: "A", Requires: []string{"Ghost"}}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cycle := operationRequiresCycle(c.ops)
			if got := len(cycle) > 0; got != c.wantCycle {
				t.Errorf("operationRequiresCycle = %v, want cycle=%v", cycle, c.wantCycle)
			}
		})
	}
	if cycle := operationRequiresCycle(heavyOperations); len(cycle) > 0 {
		t.Errorf("registry Requires edges contain a cycle: %v", cycle)
	}
}

// TestNoPrecedenceField — ADR-020 Alternatives E and F rejected precedence outright,
// and a rejected design is best held out by a test rather than by memory. Same idea
// as M2.1's negative CRD assertion.
func TestNoPrecedenceField(t *testing.T) {
	forbidden := map[string]bool{
		"precedence": true, "priority": true, "rank": true, "order": true,
		"ordering": true, "weight": true, "sequence": true, "seq": true,
		"before": true, "after": true, "level": true, "tier": true,
	}
	for f := range reflect.TypeFor[heavyOperation]().Fields() {
		if forbidden[strings.ToLower(f.Name)] {
			t.Errorf("heavyOperation declares %q — ordering is Requires edges plus the running "+
				"operation (ADR-020 Alternatives E/F), never a precedence value", f.Name)
		}
	}
}

// ---------------------------------------------------------------------------
// The registry <-> CEL admission rule agreement
// ---------------------------------------------------------------------------

// TestCELTermExtraction pins the parser the agreement test rests on, including
// against ADR-020's verified two-term form. Parent has() guards are not terms.
func TestCELTermExtraction(t *testing.T) {
	cases := []struct {
		name string
		rule string
		want []string
	}{
		{name: "empty", rule: "", want: nil},
		{
			name: "one term, with a has() guard on the leaf",
			rule: "(has(self.sentinel) && has(oldSelf.sentinel) && " +
				"(has(self.sentinel.masterName) ? self.sentinel.masterName : 'mymaster') != " +
				"(has(oldSelf.sentinel.masterName) ? oldSelf.sentinel.masterName : 'mymaster') ? 1 : 0) <= 1",
			want: []string{regOpRenamePath},
		},
		{
			// ADR-020's Verification section, verbatim: the form proved live on t3e.
			name: "ADR-020's two-term form",
			rule: "( (has(self.sentinel) && has(oldSelf.sentinel) && " +
				"self.sentinel.masterName != oldSelf.sentinel.masterName ? 1 : 0) " +
				"+ (has(self.auth) && has(oldSelf.auth) && " +
				"self.auth.enabled != oldSelf.auth.enabled ? 1 : 0) ) <= 1",
			want: []string{fixtureAuthPath, regOpRenamePath},
		},
		{
			name: "guards alone contribute nothing",
			rule: "has(self.sentinel) && has(oldSelf.sentinel)",
			want: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := heavyOperationCELTerms(c.rule); !reflect.DeepEqual(got, c.want) {
				t.Errorf("heavyOperationCELTerms = %v, want %v", got, c.want)
			}
		})
	}
}

// admissionRuleFromCRD returns the generated CRD's heavy-operation transition rule.
//
// It is read from the GENERATED CRD rather than from the marker comment, and that is
// the load-bearing choice: the CRD is what the API server enforces, the marker is only
// its input. Reading the marker would assert intent; reading the CRD asserts effect,
// and additionally fails when someone edits the marker and forgets `make manifests`.
func admissionRuleFromCRD(t *testing.T) string {
	t.Helper()
	const crdPath = "../../config/crd/bases/redis.chuck-chuck-chuck.net_littlereds.yaml"
	raw, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("reading generated CRD: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parsing generated CRD: %v", err)
	}
	cur := any(doc)
	for _, step := range []any{"spec", "versions", 0, "schema", "openAPIV3Schema", "properties", "spec", "x-kubernetes-validations"} {
		switch k := step.(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok || m[k] == nil {
				t.Fatalf("generated CRD has no spec-level %q; run `make manifests`", k)
			}
			cur = m[k]
		case int:
			a, ok := cur.([]any)
			if !ok || len(a) <= k {
				t.Fatalf("generated CRD: expected a list at versions[%d]", k)
			}
			cur = a[k]
		}
	}
	list, ok := cur.([]any)
	if !ok {
		t.Fatalf("spec x-kubernetes-validations is not a list")
	}
	var found []string
	for _, e := range list {
		m, ok := e.(map[string]any)
		if !ok {
			continue
		}
		msg, _ := m["message"].(string)
		if !strings.Contains(strings.ToLower(msg), "heavy") {
			continue
		}
		rule, _ := m["rule"].(string)
		found = append(found, rule)
	}
	if len(found) != 1 {
		t.Fatalf("found %d spec-level validation rules mentioning \"heavy\", want exactly 1: %v", len(found), found)
	}
	return found[0]
}

// TestAdmissionRuleAgreesWithRegistry is the milestone's actual deliverable: the
// registry lives in Go and the CEL rule in the CRD, and ADR-020 records their drift as
// the new maintenance cost to watch. This test is the mitigation.
//
// It must fail LOUDLY the moment a second CR-resident heavy field is registered
// without a term being added to the rule — admission and reconcile would otherwise
// silently disagree, and the disagreement is the one D4 exists to prevent.
func TestAdmissionRuleAgreesWithRegistry(t *testing.T) {
	rule := admissionRuleFromCRD(t)
	inRule := heavyOperationCELTerms(rule)
	registered := crResidentFieldPaths(heavyOperations)

	if !reflect.DeepEqual(inRule, registered) {
		t.Errorf("the admission rule and the registry disagree.\n"+
			"  rule constrains: %v\n"+
			"  registry (CR-resident): %v\n"+
			"Add the missing term to the XValidation marker on LittleRedSpec (or the missing "+
			"registry row), then `make manifests generate`.", inRule, registered)
	}
}

// TestCRResidentFieldPaths — externally-resident operations (rotation, whose value
// lives in a Secret) must NOT appear in the rule, because CEL cannot see outside the
// CR. Their ordering is a Requires edge instead (ADR-020, Serialization clause 3).
func TestCRResidentFieldPaths(t *testing.T) {
	fixture := []heavyOperation{
		{Name: "Rename", FieldPath: regOpRenamePath},
		{Name: "PasswordRotation"}, // externally resident: the value is a Secret's content
		{Name: "AuthEnablement", FieldPath: fixtureAuthPath},
	}
	got := crResidentFieldPaths(fixture)
	want := []string{fixtureAuthPath, regOpRenamePath}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("crResidentFieldPaths(fixture) = %v, want %v", got, want)
	}
}
