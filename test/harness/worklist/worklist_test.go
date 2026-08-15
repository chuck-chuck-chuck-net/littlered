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
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// Parse — the work list is DATA (design §9.1). A malformed list must be
// rejected at launch time, not discovered at item 37 of an overnight array.
// ---------------------------------------------------------------------------

func TestParse(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr string // substring; "" means success
	}{
		{
			name: "minimal valid list",
			yaml: `
array: smoke
items:
  - name: a
    labelFilter: pdb
`,
		},
		{
			name: "focus alone is a valid selection",
			yaml: `
array: smoke
items:
  - name: a
    focus: "should never create a PDB"
`,
		},
		{
			name:    "empty item list is rejected",
			yaml:    "array: smoke\nitems: []\n",
			wantErr: "at least one item",
		},
		{
			name: "item without a name is rejected",
			yaml: `
array: smoke
items:
  - labelFilter: pdb
`,
			wantErr: "name",
		},
		{
			name: "duplicate item names are rejected",
			yaml: `
array: smoke
items:
  - name: a
    labelFilter: pdb
  - name: a
    labelFilter: pdb
`,
			wantErr: "duplicate",
		},
		{
			name: "item with neither labelFilter nor focus is rejected",
			yaml: `
array: smoke
items:
  - name: a
`,
			wantErr: "labelFilter",
		},
		{
			name: "unparseable timeout is rejected",
			yaml: `
array: smoke
items:
  - name: a
    labelFilter: pdb
    timeout: "5 fortnights"
`,
			wantErr: "timeout",
		},
		{
			name: "missing array name is rejected",
			yaml: `
items:
  - name: a
    labelFilter: pdb
`,
			wantErr: "array",
		},
		{
			name: "item name that is not a DNS label is rejected (it becomes a namespace)",
			yaml: `
array: smoke
items:
  - name: "Not A Label"
    labelFilter: pdb
`,
			wantErr: "DNS label",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Parse() unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Parse() = nil error, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Parse() error = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Resolve — selecting item N and layering the array defaults under it.
// This is the "work LIST, not repeat counter" seam: index -> concrete work.
// ---------------------------------------------------------------------------

// testImage is a placeholder driver image ref for the render tests.
const testImage = "img"

const defaultsList = `
array: smoke
defaults:
  timeout: 10m
  operatorImage: reg/op:pinned
  chaosClientImage: reg/chaos:pinned
  env:
    E2E_TOPOLOGY: single
    SHARED: from-defaults
items:
  - name: plain
    labelFilter: pdb
  - name: overridden
    labelFilter: pdb
    focus: "a focused spec"
    timeout: 45s
    operatorImage: reg/op:older
    env:
      SHARED: from-item
      EXTRA: "yes"
`

func TestResolveLayersDefaults(t *testing.T) {
	l, err := Parse([]byte(defaultsList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	plain, err := l.Resolve(0)
	if err != nil {
		t.Fatalf("Resolve(0): %v", err)
	}
	if plain.Name != "plain" {
		t.Errorf("Name = %q, want %q", plain.Name, "plain")
	}
	if plain.Index != 0 {
		t.Errorf("Index = %d, want 0", plain.Index)
	}
	if plain.Timeout != 10*time.Minute {
		t.Errorf("Timeout = %v, want 10m (from defaults)", plain.Timeout)
	}
	if plain.OperatorImage != "reg/op:pinned" {
		t.Errorf("OperatorImage = %q, want the array default", plain.OperatorImage)
	}
	if plain.ChaosClientImage != "reg/chaos:pinned" {
		t.Errorf("ChaosClientImage = %q, want the array default", plain.ChaosClientImage)
	}
	if got := plain.Env["SHARED"]; got != "from-defaults" {
		t.Errorf("Env[SHARED] = %q, want %q", got, "from-defaults")
	}

	over, err := l.Resolve(1)
	if err != nil {
		t.Fatalf("Resolve(1): %v", err)
	}
	if over.Timeout != 45*time.Second {
		t.Errorf("Timeout = %v, want 45s (item overrides default)", over.Timeout)
	}
	if over.OperatorImage != "reg/op:older" {
		t.Errorf("OperatorImage = %q, want the item override (design §9.2.6)", over.OperatorImage)
	}
	if over.ChaosClientImage != "reg/chaos:pinned" {
		t.Errorf("ChaosClientImage = %q, want the array default to survive a partial override",
			over.ChaosClientImage)
	}
	if over.Focus != "a focused spec" {
		t.Errorf("Focus = %q, want the item's focus", over.Focus)
	}
	if got := over.Env["SHARED"]; got != "from-item" {
		t.Errorf("Env[SHARED] = %q, want the item to win over defaults", got)
	}
	if got := over.Env["EXTRA"]; got != "yes" {
		t.Errorf("Env[EXTRA] = %q, want %q", got, "yes")
	}
	if got := over.Env["E2E_TOPOLOGY"]; got != "single" {
		t.Errorf("Env[E2E_TOPOLOGY] = %q, want the default to survive an env override of a sibling key", got)
	}
}

// A resolved item must not be able to mutate the parsed list's default env map —
// otherwise item 1's override leaks into every later item of the same array.
func TestResolveDoesNotAliasDefaultEnv(t *testing.T) {
	l, err := Parse([]byte(defaultsList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := l.Resolve(1); err != nil { // the item that overrides SHARED
		t.Fatalf("Resolve(1): %v", err)
	}
	plain, err := l.Resolve(0)
	if err != nil {
		t.Fatalf("Resolve(0): %v", err)
	}
	if got := plain.Env["SHARED"]; got != "from-defaults" {
		t.Errorf("Env[SHARED] = %q after resolving a sibling; want %q — defaults were mutated", got, "from-defaults")
	}
}

func TestResolveIndexOutOfRange(t *testing.T) {
	l, err := Parse([]byte(defaultsList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	for _, idx := range []int{-1, 2, 99} {
		if _, err := l.Resolve(idx); err == nil {
			t.Errorf("Resolve(%d) = nil error, want out-of-range error", idx)
		}
	}
}

// The per-item namespace must be derived deterministically and must be a legal
// DNS-1123 label (<=63 chars), because a long array/item name pair otherwise
// produces a namespace the API server rejects at item N.
func TestResolveNamespaceIsDeterministicAndLegal(t *testing.T) {
	var long strings.Builder
	long.WriteString("array: " + strings.Repeat("a", 40) + "\nitems:\n")
	for i := range 3 {
		long.WriteString("  - name: " + strings.Repeat("b", 40) + string(rune('0'+i)) + "\n    labelFilter: pdb\n")
	}
	l, err := Parse([]byte(long.String()))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	seen := map[string]bool{}
	for i := range 3 {
		r, err := l.Resolve(i)
		if err != nil {
			t.Fatalf("Resolve(%d): %v", i, err)
		}
		if len(r.Namespace) > 63 {
			t.Errorf("Namespace %q is %d chars, want <=63 (DNS-1123 label)", r.Namespace, len(r.Namespace))
		}
		if !strings.HasPrefix(r.Namespace, NamespacePrefix) {
			t.Errorf("Namespace %q must start with %q so parked namespaces are identifiable",
				r.Namespace, NamespacePrefix)
		}
		if seen[r.Namespace] {
			t.Errorf("Namespace %q collides with an earlier item", r.Namespace)
		}
		seen[r.Namespace] = true

		again, err := l.Resolve(i)
		if err != nil {
			t.Fatalf("Resolve(%d) again: %v", i, err)
		}
		if again.Namespace != r.Namespace {
			t.Errorf("Namespace not deterministic: %q then %q", r.Namespace, again.Namespace)
		}
	}
}

// ---------------------------------------------------------------------------
// M2: the per-item OPERATOR namespace (design §3, §11 M2).
//
// N parked items can only coexist if each carries its own operator, blind to
// the others' namespaces. So every item needs a SECOND namespace, derived by
// the same rules as the first: deterministic, legal, collision-free — and
// never equal to any item's test namespace, since a shared name would make the
// operator's own workload live inside the namespace it reconciles.
// ---------------------------------------------------------------------------

func TestResolveOperatorNamespaceIsDistinctDeterministicAndLegal(t *testing.T) {
	var long strings.Builder
	long.WriteString("array: " + strings.Repeat("a", 40) + "\nitems:\n")
	for i := range 3 {
		long.WriteString("  - name: " + strings.Repeat("b", 40) + string(rune('0'+i)) + "\n    labelFilter: pdb\n")
	}
	l, err := Parse([]byte(long.String()))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	seen := map[string]int{}
	for i := range 3 {
		r, err := l.Resolve(i)
		if err != nil {
			t.Fatalf("Resolve(%d): %v", i, err)
		}
		if r.OperatorNamespace == "" {
			t.Fatalf("Resolve(%d): OperatorNamespace is empty — every item needs its own operator (design §3)", i)
		}
		if len(r.OperatorNamespace) > 63 {
			t.Errorf("OperatorNamespace %q is %d chars, want <=63 (DNS-1123 label)",
				r.OperatorNamespace, len(r.OperatorNamespace))
		}
		if !dnsLabel.MatchString(r.OperatorNamespace) {
			t.Errorf("OperatorNamespace %q is not a legal DNS-1123 label", r.OperatorNamespace)
		}
		if !strings.HasPrefix(r.OperatorNamespace, NamespacePrefix) {
			t.Errorf("OperatorNamespace %q must start with %q so a parked operator is identifiable and reapable",
				r.OperatorNamespace, NamespacePrefix)
		}
		if r.OperatorNamespace == r.Namespace {
			t.Errorf("item %d: operator namespace equals the test namespace (%q) — the operator would "+
				"live inside the namespace it reconciles", i, r.Namespace)
		}
		if prev, dup := seen[r.OperatorNamespace]; dup {
			t.Errorf("OperatorNamespace %q collides with item %d", r.OperatorNamespace, prev)
		}
		seen[r.OperatorNamespace] = i
		if prev, dup := seen[r.Namespace]; dup {
			t.Errorf("test namespace %q collides with the operator namespace of item %d", r.Namespace, prev)
		}
		seen[r.Namespace] = i

		again, err := l.Resolve(i)
		if err != nil {
			t.Fatalf("Resolve(%d) again: %v", i, err)
		}
		if again.OperatorNamespace != r.OperatorNamespace {
			t.Errorf("OperatorNamespace not deterministic: %q then %q", r.OperatorNamespace, again.OperatorNamespace)
		}
	}
}

// The redis image is a work-list knob so a whole array can be pointed at a
// mirrored copy (design §11 M2: the preflight's public pull is the one
// guaranteed-uncacheable pull per item) and so a single item can pin an older
// version. It layers like every other field: item over defaults.
func TestResolveRedisImageLayersLikeTheOtherImages(t *testing.T) {
	l, err := Parse([]byte(`
array: images
defaults:
  redisImage: registry.example.lan/library/redis:8.4.2
items:
  - name: inherits
    labelFilter: pdb
  - name: overrides
    labelFilter: pdb
    redisImage: redis:7.4.0
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	inherited, err := l.Resolve(0)
	if err != nil {
		t.Fatalf("Resolve(0): %v", err)
	}
	if got, want := inherited.RedisImage, "registry.example.lan/library/redis:8.4.2"; got != want {
		t.Errorf("item 0 RedisImage = %q, want the array default %q", got, want)
	}

	overridden, err := l.Resolve(1)
	if err != nil {
		t.Fatalf("Resolve(1): %v", err)
	}
	if got, want := overridden.RedisImage, "redis:7.4.0"; got != want {
		t.Errorf("item 1 RedisImage = %q, want its own override %q", got, want)
	}
}

// The driver is shell: whatever the seam decides has to cross that boundary, or
// it is not actually wired. Both M2 decisions do.
func TestShellExportCarriesTheM2Decisions(t *testing.T) {
	l, err := Parse([]byte(`
array: smoke
defaults:
  redisImage: registry.example.lan/library/redis:8.4.2
items:
  - name: item-a
    labelFilter: pdb
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	r, err := l.Resolve(0)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	out := r.ShellExport()

	for _, want := range []string{
		"export LRH_OPERATOR_NAMESPACE='" + r.OperatorNamespace + "'",
		"export E2E_REDIS_IMAGE='registry.example.lan/library/redis:8.4.2'",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("ShellExport() is missing %q:\n%s", want, out)
		}
	}
}

// ---------------------------------------------------------------------------
// ShellExport — the driver is shell; the resolved item crosses that boundary.
// A focus string is arbitrary spec text and WILL contain spaces and quotes.
// ---------------------------------------------------------------------------

func TestShellExportQuotesHostileValues(t *testing.T) {
	l, err := Parse([]byte(`
array: smoke
items:
  - name: quoting
    labelFilter: pdb
    focus: "it's a spec; rm -rf /"
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	r, err := l.Resolve(0)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	out := r.ShellExport()

	if strings.Contains(out, "; rm -rf /'") && !strings.Contains(out, `'\''`) {
		t.Fatalf("ShellExport did not escape the embedded single quote:\n%s", out)
	}
	// The canonical POSIX single-quote escape is '\'' — assert it is present,
	// which is the only way the apostrophe in "it's" can survive.
	if !strings.Contains(out, `'\''`) {
		t.Errorf("ShellExport() = %q, want the embedded single quote escaped as '\\''", out)
	}
	if !strings.Contains(out, "LRH_ITEM_NAME=") {
		t.Errorf("ShellExport() = %q, want it to export LRH_ITEM_NAME", out)
	}
}

// ---------------------------------------------------------------------------
// Classify — turning a process outcome into a verdict. This is the decision
// that makes "always exit 0" (design §4.2) safe: the exit code is recorded,
// not propagated.
// ---------------------------------------------------------------------------

func TestClassify(t *testing.T) {
	cases := []struct {
		name     string
		exitCode int
		timedOut bool
		want     Verdict
	}{
		{"zero exit is a pass", 0, false, VerdictPass},
		{"non-zero exit is a fail", 1, false, VerdictFail},
		{"ginkgo suite failure is a fail", 197, false, VerdictFail},
		{"timeout flag wins over exit code", 124, true, VerdictTimeout},
		{"GNU timeout's 124 is a timeout even unflagged", 124, false, VerdictTimeout},
		{"SIGKILL after timeout grace is a timeout", 137, true, VerdictTimeout},
		{"a zero exit that also timed out is still a timeout", 0, true, VerdictTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.exitCode, tc.timedOut); got != tc.want {
				t.Errorf("Classify(%d, %v) = %q, want %q", tc.exitCode, tc.timedOut, got, tc.want)
			}
		})
	}
}

func TestVerdictGreen(t *testing.T) {
	if !VerdictPass.Green() {
		t.Error("VerdictPass.Green() = false, want true")
	}
	for _, v := range []Verdict{VerdictFail, VerdictTimeout, VerdictMissing} {
		if v.Green() {
			t.Errorf("%q.Green() = true, want false", v)
		}
	}
}

// ---------------------------------------------------------------------------
// Rollup — "5 items, 4 green" at a glance (design §9.2.7, M1 done-when).
// The load-bearing decision is the MISSING row: a pod that died before writing
// its result must not silently shrink the denominator.
// ---------------------------------------------------------------------------

func TestRollupCountsAndMissing(t *testing.T) {
	results := []Result{
		{Index: 0, Name: "a", Verdict: VerdictPass, DurationSeconds: 61},
		{Index: 1, Name: "b", Verdict: VerdictPass, DurationSeconds: 62},
		// index 2 deliberately absent — its pod never wrote a result
		{Index: 3, Name: "d", Verdict: VerdictFail, DurationSeconds: 45},
		{Index: 4, Name: "e", Verdict: VerdictPass, DurationSeconds: 60},
	}

	r := Rollup("smoke", 5, results)

	if r.Total != 5 {
		t.Errorf("Total = %d, want 5 (the ARRAY length, not the result count)", r.Total)
	}
	if r.Green != 3 {
		t.Errorf("Green = %d, want 3", r.Green)
	}
	if r.AllGreen {
		t.Error("AllGreen = true, want false")
	}
	if len(r.Rows) != 5 {
		t.Fatalf("len(Rows) = %d, want 5 (one row per index, including the gap)", len(r.Rows))
	}
	if r.Rows[2].Verdict != VerdictMissing {
		t.Errorf("Rows[2].Verdict = %q, want %q — a pod that wrote no result must be reported, not dropped",
			r.Rows[2].Verdict, VerdictMissing)
	}

	text := r.String()
	if !strings.Contains(text, "5 items, 3 green") {
		t.Errorf("Rollup text must lead with the headline count; got:\n%s", text)
	}
	for _, want := range []string{"a", "b", "d", "e", "MISSING"} {
		if !strings.Contains(text, want) {
			t.Errorf("Rollup text missing %q; got:\n%s", want, text)
		}
	}
}

func TestRollupAllGreen(t *testing.T) {
	results := []Result{
		{Index: 0, Name: "a", Verdict: VerdictPass},
		{Index: 1, Name: "b", Verdict: VerdictPass},
	}
	r := Rollup("smoke", 2, results)
	if !r.AllGreen {
		t.Error("AllGreen = false, want true")
	}
	if !strings.Contains(r.String(), "2 items, 2 green") {
		t.Errorf("got:\n%s", r.String())
	}
}

// Results arrive from `kubectl get configmap` in label order, which is NOT index
// order. The rollup must sort, or "item 10" reads before "item 2".
func TestRollupSortsByIndex(t *testing.T) {
	r := Rollup("smoke", 3, []Result{
		{Index: 2, Name: "c", Verdict: VerdictPass},
		{Index: 0, Name: "a", Verdict: VerdictPass},
		{Index: 1, Name: "b", Verdict: VerdictPass},
	})
	for i, row := range r.Rows {
		if row.Index != i {
			t.Fatalf("Rows[%d].Index = %d, want %d — rollup did not sort by index", i, row.Index, i)
		}
	}
}

// ---------------------------------------------------------------------------
// RenderJob — the Indexed Job's completions MUST equal the work list length.
// Getting this wrong either drops work items or hangs the array forever.
// ---------------------------------------------------------------------------

func TestRenderJobCompletionsMatchWorkListLength(t *testing.T) {
	l, err := Parse([]byte(defaultsList)) // 2 items
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out, err := RenderJob(JobParams{
		List:        l,
		Namespace:   "lr-harness-system",
		Image:       "reg/driver:pinned",
		Parallelism: 1,
	})
	if err != nil {
		t.Fatalf("RenderJob: %v", err)
	}
	if !strings.Contains(out, "completions: 2") {
		t.Errorf("rendered Job must set completions to the work list length (2); got:\n%s", out)
	}
	if !strings.Contains(out, "completionMode: Indexed") {
		t.Errorf("rendered Job must be an Indexed Job (design §4.1); got:\n%s", out)
	}
	if !strings.Contains(out, "parallelism: 1") {
		t.Errorf("rendered Job must set parallelism; got:\n%s", out)
	}
	if !strings.Contains(out, "backoffLimit: 0") {
		t.Errorf("driver always exits 0, so a retry means the DRIVER crashed; want backoffLimit: 0, got:\n%s", out)
	}
}

func TestRenderJobParallelismIsParametrized(t *testing.T) {
	l, err := Parse([]byte(defaultsList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out, err := RenderJob(JobParams{
		List: l, Namespace: "ns", Image: testImage, Parallelism: 3,
	})
	if err != nil {
		t.Fatalf("RenderJob: %v", err)
	}
	if !strings.Contains(out, "parallelism: 3") {
		t.Errorf("want parallelism: 3 (design §9.2.2 needs 1 and 3); got:\n%s", out)
	}
}

func TestRenderJobRejectsBadParallelism(t *testing.T) {
	l, err := Parse([]byte(defaultsList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := RenderJob(JobParams{List: l, Namespace: "ns", Image: testImage, Parallelism: 0}); err == nil {
		t.Error("RenderJob with parallelism 0 = nil error, want rejection (the array would never start)")
	}
}

// The driver image ref and its pull policy are Job PARAMETERS, never baked-in
// literals: an array pins one pre-built image (design §1), while a dev loop
// re-pushes the same tag and needs Always to defeat the node's warm cache.
func TestRenderJobImageAndPullPolicyAreParameters(t *testing.T) {
	l, err := Parse([]byte(defaultsList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out, err := RenderJob(JobParams{
		List: l, Namespace: "ns", Image: "reg/driver:dev-7", Parallelism: 1,
		ImagePullPolicy: "Always",
	})
	if err != nil {
		t.Fatalf("RenderJob: %v", err)
	}
	if !strings.Contains(out, "image: reg/driver:dev-7") {
		t.Errorf("driver image must come from the parameter; got:\n%s", out)
	}
	if !strings.Contains(out, "imagePullPolicy: Always") {
		t.Errorf("pull policy must come from the parameter; got:\n%s", out)
	}

	def, err := RenderJob(JobParams{List: l, Namespace: "ns", Image: testImage, Parallelism: 1})
	if err != nil {
		t.Fatalf("RenderJob: %v", err)
	}
	if !strings.Contains(def, "imagePullPolicy: IfNotPresent") {
		t.Errorf("shipped default must be IfNotPresent (pinned ref, warm nodes); got:\n%s", def)
	}
}

// The image tag and the registry the suite pulls the chaos-client/operator from
// are Job parameters too. There is no .git inside the driver image, so the
// Makefile cannot derive GIT_TAG itself; and the registry must be swappable
// (a LAN registry for a lab run) without editing the committed default.
func TestRenderJobPinsRegistryAndTag(t *testing.T) {
	l, err := Parse([]byte(defaultsList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out, err := RenderJob(JobParams{
		List: l, Namespace: "ns", Image: testImage, Parallelism: 1,
		GitTag: "abc1234", Registry: "registry.example.test",
	})
	if err != nil {
		t.Fatalf("RenderJob: %v", err)
	}
	if !strings.Contains(out, "abc1234") {
		t.Errorf("rendered Job must carry the pinned GIT_TAG; got:\n%s", out)
	}
	if !strings.Contains(out, "registry.example.test") {
		t.Errorf("rendered Job must carry the pinned registry; got:\n%s", out)
	}

	// Omitting them must not emit empty env values that would override the
	// Makefile's own defaults with "".
	bare, err := RenderJob(JobParams{List: l, Namespace: "ns", Image: testImage, Parallelism: 1})
	if err != nil {
		t.Fatalf("RenderJob: %v", err)
	}
	if strings.Contains(bare, "LRH_GIT_TAG") {
		t.Errorf("an unset GitTag must not emit an empty LRH_GIT_TAG env var; got:\n%s", bare)
	}
	if strings.Contains(bare, "LITTLERED_REGISTRY") {
		t.Errorf("an unset Registry must not emit an empty LITTLERED_REGISTRY env var; got:\n%s", bare)
	}
}

// ---------------------------------------------------------------------------
// RenderBundle — everything needed to launch one array, as a single applyable
// document: the namespace, the driver's identity and RBAC, the work list as a
// ConfigMap, and the Job. Keeping it one command keeps the ConfigMap the Job
// mounts and the work list the rollup counts from provably the same list.
// ---------------------------------------------------------------------------

func TestRenderBundleIsSelfConsistent(t *testing.T) {
	raw := []byte(defaultsList) // 2 items
	l, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out, err := RenderBundle(BundleParams{
		JobParams: JobParams{
			List: l, Namespace: "lr-harness-system", Image: "reg/driver:pinned", Parallelism: 1,
		},
		WorkListYAML: string(raw),
	})
	if err != nil {
		t.Fatalf("RenderBundle: %v", err)
	}

	for _, kind := range []string{
		"kind: Namespace", "kind: ServiceAccount", "kind: ClusterRole",
		"kind: ClusterRoleBinding", "kind: ConfigMap", "kind: Job",
	} {
		if !strings.Contains(out, kind) {
			t.Errorf("bundle is missing %q:\n%s", kind, out)
		}
	}

	// The Job mounts the ConfigMap by name; they must agree.
	if !strings.Contains(out, "name: smoke-worklist") {
		t.Errorf("bundle must name the work-list ConfigMap deterministically; got:\n%s", out)
	}

	// The work list must be embedded verbatim (indented), so what the driver
	// resolves is exactly what was validated at launch.
	if !strings.Contains(out, "    array: smoke") {
		t.Errorf("bundle must embed the work list indented under the ConfigMap data key; got:\n%s", out)
	}

	// hostPath artifacts need a namespace that is not PSS-baseline.
	if !strings.Contains(out, "pod-security.kubernetes.io/enforce: privileged") {
		t.Errorf("harness namespace must be PSS-privileged (hostPath artifact volume); got:\n%s", out)
	}
}

// The driver helm-installs a per-item operator (M2), which creates that
// operator's leader-election Role. Kubernetes forbids granting permissions the
// creator does not itself hold, so a rule missing here does not degrade the
// driver — it makes every single work item fail at `helm upgrade --install`
// with "attempt to grant extra privileges".
func TestRenderBundleGrantsWhatThePerItemOperatorNeeds(t *testing.T) {
	l, err := Parse([]byte("array: a\nitems:\n  - name: i\n    labelFilter: pdb\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out, err := RenderBundle(BundleParams{
		JobParams:    JobParams{List: l, Namespace: "ns", Image: "img", Parallelism: 1},
		WorkListYAML: "array: a\n",
	})
	if err != nil {
		t.Fatalf("RenderBundle: %v", err)
	}
	// The operator's own leader-election Role holds leases; the driver must be
	// able to grant that.
	if !strings.Contains(out, "coordination.k8s.io") {
		t.Errorf("driver ClusterRole does not cover coordination.k8s.io/leases, so creating the "+
			"operator's leader-election Role would be an RBAC escalation:\n%s", out)
	}
}

func TestRenderBundleRejectsEmptyWorkListYAML(t *testing.T) {
	l, err := Parse([]byte(defaultsList))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	_, err = RenderBundle(BundleParams{
		JobParams: JobParams{List: l, Namespace: "ns", Image: testImage, Parallelism: 1},
	})
	if err == nil {
		t.Error("RenderBundle with no work-list YAML = nil error, want rejection (the Job would mount an empty list)")
	}
}

// ---------------------------------------------------------------------------
// ImageRefs — what an array must be able to pull before it is worth launching.
//
// The design note's own warning is that image problems "fail in the least
// helpful way — mid-run, as an ImagePullBackOff on item 47". The harness had
// exactly that property against itself: a missing OPERATOR image turned a
// 5-item array into five identical 2-minute readiness timeouts. Naming the set
// is the decision; the shell only has to look each one up.
// ---------------------------------------------------------------------------

// The array-wide refs the Makefile derives, as the tables below spell them.
const (
	testDriverImage = "reg.lan/littlered-e2e-driver:t"
	testOperImage   = "reg.lan/littlered:t"
	testChaosImage  = "reg.lan/littlered-chaos-client:t"
)

func TestImageRefsNamesEveryPinnedImage(t *testing.T) {
	l, err := Parse([]byte(`
array: a
items:
  - name: plain
    labelFilter: pdb
  - name: same-again
    labelFilter: pdb
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := l.ImageRefs(ImageParams{
		DriverImage:      testDriverImage,
		OperatorImage:    testOperImage,
		ChaosClientImage: testChaosImage,
	})
	want := []string{
		testChaosImage,
		testDriverImage,
		testOperImage,
	}
	if !slices.Equal(got, want) {
		t.Errorf("ImageRefs() = %v, want %v (deduplicated across identical items, sorted)", got, want)
	}
}

// A per-item override (design §9.2.6, matrix cell G9) is a ref the array must
// also be able to pull — and it does NOT replace the array-wide one, which the
// other items still use.
func TestImageRefsIncludesPerItemOverrides(t *testing.T) {
	l, err := Parse([]byte(`
array: a
items:
  - name: current
    labelFilter: pdb
  - name: older-operator
    labelFilter: pdb
    operatorImage: reg.lan/littlered:old
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := l.ImageRefs(ImageParams{
		DriverImage:      testDriverImage,
		OperatorImage:    testOperImage,
		ChaosClientImage: testChaosImage,
	})
	for _, want := range []string{"reg.lan/littlered:old", testOperImage} {
		if !slices.Contains(got, want) {
			t.Errorf("ImageRefs() = %v, missing %q", got, want)
		}
	}
}

// The one that repaid itself immediately: ExporterSpec.Registry INHERITS the
// main image's registry, so pinning redisImage at a mirror silently requires
// the exporter in that same mirror. The first M2 array died on exactly this
// (ImagePullBackOff, "manifest unknown"), so the preflight must derive it.
func TestImageRefsDerivesTheExporterFromThePinnedRedisRegistry(t *testing.T) {
	l, err := Parse([]byte(`
array: a
defaults:
  redisImage: reg.lan/library/redis:8.4.2
items:
  - name: mirrored
    labelFilter: pdb
  - name: official
    labelFilter: pdb
    redisImage: redis:7.4.0
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := l.ImageRefs(ImageParams{DriverImage: testDriverImage})

	exporter := "reg.lan/" + v1alpha1.DefaultExporterPath + ":" + v1alpha1.DefaultExporterTag
	official := "docker.io/" + v1alpha1.DefaultExporterPath + ":" + v1alpha1.DefaultExporterTag
	for _, want := range []string{"reg.lan/library/redis:8.4.2", exporter, "redis:7.4.0", official} {
		if !slices.Contains(got, want) {
			t.Errorf("ImageRefs() = %v, missing %q", got, want)
		}
	}
}

// An unset ref must not become an empty entry the shell then tries to inspect.
func TestImageRefsSkipsUnsetRefs(t *testing.T) {
	l, err := Parse([]byte("array: a\nitems:\n  - name: i\n    labelFilter: pdb\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := l.ImageRefs(ImageParams{DriverImage: testDriverImage})
	if !slices.Equal(got, []string{testDriverImage}) {
		t.Errorf("ImageRefs() = %v, want only the driver image", got)
	}
}

// ---------------------------------------------------------------------------
// LaunchBlockers — the harness must never destroy forensic evidence unless it
// is explicitly told to.
//
// Parking (Classify -> Result.Parked) already fails SAFE: a namespace is kept
// unless the record explicitly says the item passed. Launch then threw the same
// evidence away, silently, as a "leftover from an earlier run" — five parked
// failures with their artifacts and live operators, gone, with the only warning
// buried in a per-item driver log. Same principle, opposite behaviour; this
// closes it at the same place the image check lives.
// ---------------------------------------------------------------------------

func TestLaunchBlockersOnlyBlocksOnParkedEvidence(t *testing.T) {
	cases := []struct {
		name     string
		observed []ObservedNamespace
		want     []string
	}{
		{
			name:     "nothing observed",
			observed: nil,
		},
		{
			name: "an ordinary leftover is not evidence",
			observed: []ObservedNamespace{
				{Name: "lr-harness-a-0-x", Array: "a"},
			},
		},
		{
			name: "a parked pair blocks, ordered by name within an index",
			observed: []ObservedNamespace{
				{Name: parkedOpNS, Array: "a", Parked: true, Index: "2", Item: "x", Verdict: string(VerdictFail)},
				{Name: parkedTestNS, Array: "a", Parked: true, Index: "2", Item: "x", Verdict: string(VerdictFail)},
			},
			want: []string{parkedTestNS, parkedOpNS},
		},
		{
			name: "another array's parked evidence is none of this array's business",
			observed: []ObservedNamespace{
				{Name: "lr-harness-b-0-y", Array: "b", Parked: true, Verdict: string(VerdictFail)},
			},
		},
		{
			name: "ordered by index, so a 10-item array reads in run order",
			observed: []ObservedNamespace{
				{Name: "lr-harness-a-10-j", Array: "a", Parked: true, Index: "10", Verdict: string(VerdictFail)},
				{Name: "lr-harness-a-2-b", Array: "a", Parked: true, Index: "2", Verdict: string(VerdictTimeout)},
				{Name: "lr-harness-a-1-c", Array: "a", Parked: true, Index: "1", Verdict: string(VerdictFail)},
			},
			want: []string{"lr-harness-a-1-c", "lr-harness-a-2-b", "lr-harness-a-10-j"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, b := range LaunchBlockers("a", tc.observed) {
				got = append(got, b.Name)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("LaunchBlockers() = %v, want %v", got, tc.want)
			}
		})
	}
}

// The refusal has to be actionable on its own: which evidence is in the way,
// what it was, and the two ways forward. A bare "refusing" would just move the
// dead end.
const (
	doomedItem   = "pdb-standalone-doomed"
	parkedOpNS   = "lr-harness-op-a-2-x"
	parkedTestNS = "lr-harness-a-2-x"
)

func TestRefusalMessageNamesTheEvidenceAndBothWaysOut(t *testing.T) {
	msg := RefusalMessage("m1smoke", "test/harness/manifests/worklist-m1-smoke.yaml", []ObservedNamespace{
		{Name: "lr-harness-m1smoke-2-doomed", Array: "m1smoke", Parked: true, Index: "2",
			Item: doomedItem, Verdict: string(VerdictFail)},
		{Name: "lr-harness-op-m1smoke-2-doomed", Array: "m1smoke", Parked: true, Index: "2",
			Item: doomedItem, Verdict: string(VerdictFail), Role: RoleOperator},
	})

	for _, want := range []string{
		"lr-harness-m1smoke-2-doomed",
		"lr-harness-op-m1smoke-2-doomed",
		doomedItem,
		string(VerdictFail),
		RoleOperator,
		"make e2e-harness-clean E2E_HARNESS_WORKLIST=test/harness/manifests/worklist-m1-smoke.yaml",
		"array:",
		"E2E_HARNESS_ALLOW_PARKED_CLOBBER=true",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("RefusalMessage() is missing %q:\n%s", want, msg)
		}
	}
}

// The override has to reach the DRIVER, not just the launch gate: with the
// per-item clean-slate step refusing parked namespaces too, a launch that
// bypassed only the gate would make every affected item fail instead.
func TestRenderJobCarriesTheParkedClobberOverride(t *testing.T) {
	l, err := Parse([]byte("array: a\nitems:\n  - name: i\n    labelFilter: pdb\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	base := JobParams{List: l, Namespace: "ns", Image: "img", Parallelism: 1}

	off, err := RenderJob(base)
	if err != nil {
		t.Fatalf("RenderJob: %v", err)
	}
	if strings.Contains(off, "LRH_ALLOW_PARKED_CLOBBER") {
		t.Errorf("RenderJob() smuggles the clobber override into a default array:\n%s", off)
	}

	base.AllowParkedClobber = true
	on, err := RenderJob(base)
	if err != nil {
		t.Fatalf("RenderJob: %v", err)
	}
	if !strings.Contains(on, "LRH_ALLOW_PARKED_CLOBBER") || !strings.Contains(on, `value: "true"`) {
		t.Errorf("RenderJob() did not pass the clobber override to the driver:\n%s", on)
	}
}

// The launch gate reads stamps that the DRIVER writes, in shell. Nothing in the
// compiler connects the two, so a renamed label would silently turn the guard
// into a no-op — it would observe nothing and cheerfully permit the clobber.
// Read the script and assert every key it must write is there.
func TestNamespaceStampsMatchTheDriver(t *testing.T) {
	script, err := os.ReadFile("../driver/harness-entrypoint.sh")
	if err != nil {
		t.Fatalf("reading the driver script: %v", err)
	}
	for _, key := range []string{
		LabelArray, LabelParked, LabelIndex, LabelRole, AnnotationItem, AnnotationVerdict,
	} {
		if !strings.Contains(string(script), key) {
			t.Errorf("the driver never writes %q, so the launch gate cannot read it", key)
		}
	}
}

func TestObserveNamespaceReadsTheStamps(t *testing.T) {
	got := ObserveNamespace(parkedOpNS,
		map[string]string{LabelArray: "a", LabelParked: "true", LabelIndex: "2", LabelRole: RoleOperator},
		map[string]string{AnnotationItem: "doomed", AnnotationVerdict: string(VerdictFail)})
	want := ObservedNamespace{
		Name: parkedOpNS, Array: "a", Parked: true,
		Index: "2", Role: RoleOperator, Item: "doomed", Verdict: string(VerdictFail),
	}
	if got != want {
		t.Errorf("ObserveNamespace() = %+v, want %+v", got, want)
	}
	// An unstamped namespace must read as "not parked", never as unknown-so-park:
	// this side of the fence, a false positive would block every launch.
	if ObserveNamespace("plain", nil, nil).Parked {
		t.Error("an unstamped namespace must not read as parked")
	}
}
