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

// Package worklist holds the pure decision seam of the in-cluster e2e work-list
// driver (docs/E2E_SOAK_HARNESS_DESIGN.md §9.1, §11 M1).
//
// The driver itself is shell plumbing around `make run-test-e2e`; everything in
// it that is a DECISION lives here, I/O-free and unit-testable:
//
//   - Parse     — the work list is data (a ConfigMap), validated at launch time.
//   - Resolve   — index N of the array becomes one concrete unit of work,
//     with the array defaults layered underneath the item.
//   - Classify  — a process outcome becomes a verdict, so the driver can always
//     `exit 0` (§4.2) while still recording pass/fail truthfully.
//   - Rollup    — N per-item results become "5 items, 4 green" at a glance.
//   - RenderJob — the work list becomes an Indexed Job whose `completions`
//     equals the list length.
//
// The soak workload is the degenerate case of a work list: the same item N
// times. Building the list first — rather than a repeat counter — is the one
// piece of rework the design explicitly calls out as avoidable (§11).
package worklist

import (
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

const (
	// NamespacePrefix prefixes every namespace the harness creates, so a parked
	// (deliberately not cleaned up) namespace is trivially identifiable.
	NamespacePrefix = "lr-harness-"

	// OperatorNamespacePrefix prefixes the item's OWN operator namespace (M2,
	// design §3). It extends NamespacePrefix rather than replacing it, so one
	// label-free `lr-harness-*` glob still finds everything the harness made.
	OperatorNamespacePrefix = NamespacePrefix + "op-"

	// DefaultItemTimeout bounds a work item that names no timeout and belongs to
	// an array that declares no default. It sits just under the e2e suite's own
	// 120m `go test` timeout so the driver, not `go test`, owns the deadline.
	DefaultItemTimeout = 110 * time.Minute

	// maxDNSLabel is the API server's limit for a namespace name.
	maxDNSLabel = 63
)

// dnsLabel matches a legal DNS-1123 label, which every item name must be: the
// name is carried verbatim into the item's namespace.
var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// Item is one unit of work in the array. The soak workload repeats one item N
// times; the multi-site matrix gives every cell its own item.
type Item struct {
	// Name identifies the item in the rollup and seeds its namespace.
	Name string `json:"name"`
	// LabelFilter is the Ginkgo label selector (make LABEL_FILTER=...).
	LabelFilter string `json:"labelFilter,omitempty"`
	// Focus is the Ginkgo focus regex (make FOCUS=...).
	Focus string `json:"focus,omitempty"`
	// Timeout is a Go duration string bounding this item.
	Timeout string `json:"timeout,omitempty"`
	// OperatorImage overrides the array's operator image for this item
	// (design §9.2.6 — matrix cell G9 starts on an older operator).
	OperatorImage string `json:"operatorImage,omitempty"`
	// ChaosClientImage overrides the array's chaos-client image for this item.
	ChaosClientImage string `json:"chaosClientImage,omitempty"`
	// RedisImage overrides the redis image the suite pins into its CRs and its
	// preflight probe (E2E_REDIS_IMAGE). Two uses: pointing a whole array at a
	// mirrored copy so the run never depends on Docker Hub, and per-item version
	// coverage (the migration tier pins 7.4.0 for regression fidelity).
	RedisImage string `json:"redisImage,omitempty"`
	// Env is passed through to the suite, layered over the array defaults.
	Env map[string]string `json:"env,omitempty"`
}

// Defaults are layered underneath every item of the array.
type Defaults struct {
	Timeout          string            `json:"timeout,omitempty"`
	OperatorImage    string            `json:"operatorImage,omitempty"`
	ChaosClientImage string            `json:"chaosClientImage,omitempty"`
	RedisImage       string            `json:"redisImage,omitempty"`
	Env              map[string]string `json:"env,omitempty"`
}

// List is the whole work list, as held in a ConfigMap.
type List struct {
	Array    string   `json:"array"`
	Defaults Defaults `json:"defaults,omitempty"`
	Items    []Item   `json:"items"`
}

// ResolvedItem is item N with the array defaults applied — one concrete unit of
// work, ready to hand to the shell.
type ResolvedItem struct {
	Index int
	Name  string
	Array string
	// Namespace is the item's test namespace (where the CRs and Redis pods live).
	Namespace string
	// OperatorNamespace is where the item's OWN operator is deployed (M2, design
	// §3): N parked items can only coexist if each has its own operator, and each
	// operator is blind to the others' namespaces.
	OperatorNamespace string
	LabelFilter       string
	Focus             string
	Timeout           time.Duration
	OperatorImage     string
	ChaosClientImage  string
	RedisImage        string
	Env               map[string]string
}

// Verdict is the outcome of one work item.
type Verdict string

// The verdicts a work item can end in.
const (
	VerdictPass    Verdict = "PASS"
	VerdictFail    Verdict = "FAIL"
	VerdictTimeout Verdict = "TIMEOUT"
	// VerdictMissing is synthesised by Rollup for an index that wrote no result
	// at all — a driver pod that died. It must never shrink the denominator.
	VerdictMissing Verdict = "MISSING"
)

// Green reports whether the verdict counts towards the "N green" headline.
func (v Verdict) Green() bool { return v == VerdictPass }

// Result is the per-item record the driver writes (design §4.2: the driver
// exits 0 and records pass/fail itself).
type Result struct {
	Array           string  `json:"array"`
	Index           int     `json:"index"`
	Name            string  `json:"name"`
	Verdict         Verdict `json:"verdict"`
	ExitCode        int     `json:"exitCode"`
	DurationSeconds float64 `json:"durationSeconds"`
	StartedAt       string  `json:"startedAt"`
	FinishedAt      string  `json:"finishedAt"`
	Namespace       string  `json:"namespace"`
	Parked          bool    `json:"parked"`
	ArtifactDir     string  `json:"artifactDir,omitempty"`
}

// Row is one line of the rollup.
type Row struct {
	Index           int
	Name            string
	Verdict         Verdict
	DurationSeconds float64
	Namespace       string
	Parked          bool
}

// Summary is the folded array result.
type Summary struct {
	Array    string
	Total    int
	Green    int
	AllGreen bool
	Rows     []Row
}

// Parse decodes and validates a work list. Validation happens once, at launch,
// so a typo cannot surface as a mystery failure at item 37 of an overnight run.
func Parse(data []byte) (List, error) {
	var l List
	if err := yaml.UnmarshalStrict(data, &l); err != nil {
		return List{}, fmt.Errorf("work list is not valid YAML: %w", err)
	}

	if strings.TrimSpace(l.Array) == "" {
		return List{}, fmt.Errorf("work list needs an array name (field %q)", "array")
	}
	if !dnsLabel.MatchString(l.Array) {
		return List{}, fmt.Errorf("array name %q must be a DNS label (it names the Job and its namespaces)", l.Array)
	}
	if len(l.Items) == 0 {
		return List{}, fmt.Errorf("work list must hold at least one item")
	}
	if l.Defaults.Timeout != "" {
		if _, err := time.ParseDuration(l.Defaults.Timeout); err != nil {
			return List{}, fmt.Errorf("defaults: timeout %q is not a duration: %w", l.Defaults.Timeout, err)
		}
	}

	seen := make(map[string]int, len(l.Items))
	for i, it := range l.Items {
		if strings.TrimSpace(it.Name) == "" {
			return List{}, fmt.Errorf("item %d has no name", i)
		}
		if !dnsLabel.MatchString(it.Name) {
			return List{}, fmt.Errorf("item %d: name %q must be a DNS label (it seeds the item's namespace)", i, it.Name)
		}
		if prev, dup := seen[it.Name]; dup {
			return List{}, fmt.Errorf("item %d: duplicate name %q (already used by item %d)", i, it.Name, prev)
		}
		seen[it.Name] = i

		if it.LabelFilter == "" && it.Focus == "" {
			return List{}, fmt.Errorf(
				"item %d (%q): needs a labelFilter or a focus, else it selects the whole suite", i, it.Name)
		}
		if it.Timeout != "" {
			if _, err := time.ParseDuration(it.Timeout); err != nil {
				return List{}, fmt.Errorf("item %d (%q): timeout %q is not a duration: %w", i, it.Name, it.Timeout, err)
			}
		}
	}
	return l, nil
}

// Resolve returns item index with the array defaults layered underneath it.
func (l List) Resolve(index int) (ResolvedItem, error) {
	if index < 0 || index >= len(l.Items) {
		return ResolvedItem{}, fmt.Errorf("index %d out of range: the work list holds %d item(s)", index, len(l.Items))
	}
	it := l.Items[index]

	timeout := DefaultItemTimeout
	if s := firstNonEmpty(it.Timeout, l.Defaults.Timeout); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return ResolvedItem{}, fmt.Errorf("item %d (%q): timeout %q is not a duration: %w", index, it.Name, s, err)
		}
		timeout = d
	}

	// Copy, never alias: a resolved item must not be able to mutate the list's
	// default env map, or item 1's override leaks into every later item.
	env := make(map[string]string, len(l.Defaults.Env)+len(it.Env))
	maps.Copy(env, l.Defaults.Env)
	maps.Copy(env, it.Env)

	return ResolvedItem{
		Index:             index,
		Name:              it.Name,
		Array:             l.Array,
		Namespace:         itemNamespace(NamespacePrefix, l.Array, index, it.Name),
		OperatorNamespace: itemNamespace(OperatorNamespacePrefix, l.Array, index, it.Name),
		LabelFilter:       it.LabelFilter,
		Focus:             it.Focus,
		Timeout:           timeout,
		OperatorImage:     firstNonEmpty(it.OperatorImage, l.Defaults.OperatorImage),
		ChaosClientImage:  firstNonEmpty(it.ChaosClientImage, l.Defaults.ChaosClientImage),
		RedisImage:        firstNonEmpty(it.RedisImage, l.Defaults.RedisImage),
		Env:               env,
	}, nil
}

// itemNamespace derives one of the item's namespaces deterministically. The
// index sits BEFORE the item name so that truncating an over-long name to fit
// the 63-char DNS label limit can never make two items collide.
//
// The prefix is what separates the two families: an item's test namespace and
// its operator namespace differ in a segment that truncation never touches, so
// no amount of name truncation can make one item's operator namespace equal
// another item's test namespace.
func itemNamespace(prefix, array string, index int, name string) string {
	head := prefix + array + "-" + strconv.Itoa(index) + "-"
	budget := max(maxDNSLabel-len(head), 0)
	if len(name) > budget {
		name = name[:budget]
	}
	return strings.TrimRight(head+name, "-")
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ShellExport renders the resolved item as POSIX `export` assignments for
// `eval "$(lrharness plan ...)"`. A focus is arbitrary spec text and will
// contain spaces, apostrophes and semicolons, so every value is single-quoted
// with the canonical '\” escape.
func (r ResolvedItem) ShellExport() string {
	var b strings.Builder
	put := func(k, v string) {
		fmt.Fprintf(&b, "export %s=%s\n", k, shellQuote(v))
	}
	put("LRH_ARRAY", r.Array)
	put("LRH_INDEX", strconv.Itoa(r.Index))
	put("LRH_ITEM_NAME", r.Name)
	put("LRH_NAMESPACE", r.Namespace)
	put("LRH_OPERATOR_NAMESPACE", r.OperatorNamespace)
	put("LRH_LABEL_FILTER", r.LabelFilter)
	put("LRH_FOCUS", r.Focus)
	put("LRH_TIMEOUT_SECONDS", strconv.FormatInt(int64(r.Timeout.Seconds()), 10))
	put("LRH_OPERATOR_IMAGE", r.OperatorImage)
	put("LRH_CHAOS_CLIENT_IMAGE", r.ChaosClientImage)
	// The redis image crosses as the suite's OWN env var, not an LRH_ one: it is
	// consumed by the e2e suite (preflight probe + every CR's spec.image), which
	// inherits the driver's environment. Emitting it before the item's `env` map
	// keeps that map the last word, so an item can still override it by hand.
	if r.RedisImage != "" {
		put("E2E_REDIS_IMAGE", r.RedisImage)
	}

	keys := make([]string, 0, len(r.Env))
	for k := range r.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		put(k, r.Env[k])
	}
	return b.String()
}

// shellQuote wraps s in single quotes, escaping any embedded single quote the
// only way POSIX sh allows: close the quote, emit an escaped quote, reopen.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// Classify turns a process outcome into a verdict. This is what makes the
// always-`exit 0` trick (design §4.2) honest: the outcome is recorded here
// rather than propagated to the Job's backoffLimit.
func Classify(exitCode int, timedOut bool) Verdict {
	// GNU timeout reports 124 when it killed the child, and the driver also
	// flags the deadline itself. Either signal alone means TIMEOUT — a timeout
	// is a distinct diagnosis from an assertion failure, so it is not folded
	// into FAIL.
	if timedOut || exitCode == 124 {
		return VerdictTimeout
	}
	if exitCode == 0 {
		return VerdictPass
	}
	return VerdictFail
}

// Rollup folds per-item results into the array summary. `total` is the ARRAY
// length, not len(results): an index that wrote no result is reported MISSING
// rather than quietly shrinking the denominator.
func Rollup(array string, total int, results []Result) Summary {
	byIndex := make(map[int]Result, len(results))
	for _, r := range results {
		byIndex[r.Index] = r
	}

	rows := make([]Row, 0, total)
	green := 0
	for i := range total {
		r, ok := byIndex[i]
		if !ok {
			rows = append(rows, Row{Index: i, Name: "-", Verdict: VerdictMissing})
			continue
		}
		if r.Verdict.Green() {
			green++
		}
		rows = append(rows, Row{
			Index:           i,
			Name:            r.Name,
			Verdict:         r.Verdict,
			DurationSeconds: r.DurationSeconds,
			Namespace:       r.Namespace,
			Parked:          r.Parked,
		})
	}

	// Results arrive in whatever order the API server listed the ConfigMaps.
	sort.Slice(rows, func(a, b int) bool { return rows[a].Index < rows[b].Index })

	return Summary{
		Array:    array,
		Total:    total,
		Green:    green,
		AllGreen: green == total,
		Rows:     rows,
	}
}

// String renders the one-glance rollup: the headline count first, then a row
// per index (design §9.2.7 — for a 20h array this is the primary output).
func (s Summary) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "e2e harness array %q: %d items, %d green\n\n", s.Array, s.Total, s.Green)
	fmt.Fprintf(&b, "  %-3s  %-28s  %-8s  %9s  %s\n", "IDX", "ITEM", "VERDICT", "DURATION", "NAMESPACE")
	for _, r := range s.Rows {
		dur := "-"
		if r.DurationSeconds > 0 {
			dur = strconv.FormatInt(int64(r.DurationSeconds), 10) + "s"
		}
		ns := r.Namespace
		if r.Parked {
			ns += "  [PARKED]"
		}
		fmt.Fprintf(&b, "  %-3d  %-28s  %-8s  %9s  %s\n", r.Index, r.Name, r.Verdict, dur, ns)
	}
	if !s.AllGreen {
		fmt.Fprintf(&b, "\n%d item(s) not green. Parked namespaces are left standing for forensics.\n", s.Total-s.Green)
	}
	return b.String()
}

// JobParams drives RenderJob.
type JobParams struct {
	// List is the work list; its length fixes the Job's completions.
	List List
	// Namespace is where the Job, its results and its RBAC live.
	Namespace string
	// Image is the driver image ref. It is a PARAMETER, never a baked-in
	// literal: an array pins one pre-built ref (design §1), while a dev loop
	// re-pushes one tag and pairs it with ImagePullPolicy: Always.
	Image string
	// ImagePullPolicy defaults to IfNotPresent — the shipped array pins a ref
	// and the nodes are warm, so re-pulling per item is pure waste.
	ImagePullPolicy string
	// Parallelism is the concurrency cap (design §9.2.2 wants 1 and, later, 3).
	Parallelism int
	// ServiceAccount the driver pods run as.
	ServiceAccount string
	// WorkListName is the ConfigMap holding the work list.
	WorkListName string
	// ArtifactsPVC, when set, backs the artifact volume; otherwise ArtifactsHostPath is used.
	ArtifactsPVC string
	// ArtifactsHostPath backs the artifact volume when no PVC is given (the
	// single-node test-cluster case: no StorageClass to bind against).
	ArtifactsHostPath string
	// GitTag pins the image tag the suite resolves its operator/chaos-client
	// refs from. The driver image holds no .git, so the Makefile cannot derive
	// it in-pod; the array pins one tag for every item (design §6).
	GitTag string
	// Registry overrides LITTLERED_REGISTRY for the run — e.g. a LAN registry
	// for a lab array — without touching the repo's committed public default.
	Registry string
}

// RenderJob renders the Indexed Job for a work list. The invariant that matters:
// completions == len(items). Too few silently drops work items; too many hangs
// the array on an index that resolves to nothing.
func RenderJob(p JobParams) (string, error) {
	if len(p.List.Items) == 0 {
		return "", fmt.Errorf("cannot render a Job for an empty work list")
	}
	if p.Parallelism < 1 {
		return "", fmt.Errorf("parallelism must be >= 1, got %d (the array would never start)", p.Parallelism)
	}
	if p.Namespace == "" {
		return "", fmt.Errorf("namespace is required")
	}
	if p.Image == "" {
		return "", fmt.Errorf("driver image is required")
	}

	d := jobTemplateData{
		Array:             p.List.Array,
		Namespace:         p.Namespace,
		Image:             p.Image,
		ImagePullPolicy:   firstNonEmpty(p.ImagePullPolicy, "IfNotPresent"),
		Completions:       len(p.List.Items),
		Parallelism:       p.Parallelism,
		ServiceAccount:    firstNonEmpty(p.ServiceAccount, "lr-e2e-harness"),
		WorkListName:      firstNonEmpty(p.WorkListName, p.List.Array+"-worklist"),
		ArtifactsPVC:      p.ArtifactsPVC,
		ArtifactsHostPath: firstNonEmpty(p.ArtifactsHostPath, "/var/lib/lr-e2e-harness"),
		GitTag:            p.GitTag,
		Registry:          p.Registry,
	}

	var b strings.Builder
	if err := jobTemplate.Execute(&b, d); err != nil {
		return "", fmt.Errorf("rendering Job manifest: %w", err)
	}
	return b.String(), nil
}

type jobTemplateData struct {
	Array             string
	Namespace         string
	Image             string
	ImagePullPolicy   string
	Completions       int
	Parallelism       int
	ServiceAccount    string
	WorkListName      string
	ArtifactsPVC      string
	ArtifactsHostPath string
	GitTag            string
	Registry          string
}

var jobTemplate = template.Must(template.New("job").Parse(`apiVersion: batch/v1
kind: Job
metadata:
  name: {{ .Array }}
  namespace: {{ .Namespace }}
  labels:
    app.kubernetes.io/name: lr-e2e-harness
    lr-e2e-harness/array: {{ .Array }}
spec:
  # An Indexed Job IS the batch array (design §4.1): completions == work list
  # length, and each pod gets its own JOB_COMPLETION_INDEX.
  completionMode: Indexed
  completions: {{ .Completions }}
  parallelism: {{ .Parallelism }}
  # The driver always exits 0 and records the verdict itself, so a retry here
  # would mean the DRIVER crashed, not that a work item failed.
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/name: lr-e2e-harness
        lr-e2e-harness/array: {{ .Array }}
    spec:
      restartPolicy: Never
      serviceAccountName: {{ .ServiceAccount }}
      containers:
        - name: driver
          image: {{ .Image }}
          imagePullPolicy: {{ .ImagePullPolicy }}
          command: ["/usr/local/bin/harness-entrypoint.sh"]
          env:
            - name: LRH_ARRAY
              value: {{ .Array }}
            - name: LRH_WORKLIST
              value: /worklist/worklist.yaml
            - name: LRH_ARTIFACTS_ROOT
              value: /artifacts
{{- if .GitTag }}
            - name: LRH_GIT_TAG
              value: {{ .GitTag }}
{{- end }}
{{- if .Registry }}
            - name: LITTLERED_REGISTRY
              value: {{ .Registry }}
{{- end }}
            - name: LRH_HARNESS_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: LRH_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          volumeMounts:
            - name: worklist
              mountPath: /worklist
              readOnly: true
            - name: artifacts
              mountPath: /artifacts
      volumes:
        - name: worklist
          configMap:
            name: {{ .WorkListName }}
        - name: artifacts
{{- if .ArtifactsPVC }}
          persistentVolumeClaim:
            claimName: {{ .ArtifactsPVC }}
{{- else }}
          hostPath:
            path: {{ .ArtifactsHostPath }}
            type: DirectoryOrCreate
{{- end }}
`))

// ImageParams carries the array-wide image refs the Makefile derives (from
// LITTLERED_REGISTRY + GIT_TAG). A work item may override the operator and
// chaos-client refs; the driver ref belongs to the Job.
type ImageParams struct {
	DriverImage      string
	OperatorImage    string
	ChaosClientImage string
}

// ImageRefs returns every image reference the array pins, deduplicated and
// sorted — the set a launch must be able to pull.
//
// Checking this BEFORE applying the Job is the difference between one sentence
// naming a missing ref and N identical mid-array readiness timeouts: a missing
// operator image is not visible until each item has helm-installed it and
// waited out its own 2-minute readiness poll (observed: a 5-item array spending
// 10 minutes failing the same way five times).
func (l List) ImageRefs(p ImageParams) []string {
	seen := map[string]bool{}
	add := func(ref string) {
		if ref = strings.TrimSpace(ref); ref != "" {
			seen[ref] = true
		}
	}

	add(p.DriverImage)
	for i := range l.Items {
		r, err := l.Resolve(i)
		if err != nil {
			// Unresolvable items are Parse's business, not this function's; a
			// caller that got here has already validated the list.
			continue
		}
		add(firstNonEmpty(r.OperatorImage, p.OperatorImage))
		add(firstNonEmpty(r.ChaosClientImage, p.ChaosClientImage))
		if r.RedisImage != "" {
			add(r.RedisImage)
			// The metrics exporter INHERITS the main image's registry
			// (api/v1alpha1 ExporterSpec.FullImage), so pinning redis at a
			// mirror silently requires the exporter in that same mirror. The
			// first M2 array died on exactly this, with "manifest unknown".
			add(registryOf(r.RedisImage) + "/" + v1alpha1.DefaultExporterPath + ":" + v1alpha1.DefaultExporterTag)
		}
	}

	refs := make([]string, 0, len(seen))
	for ref := range seen {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return refs
}

// registryOf returns the registry host of an image reference, defaulting to
// docker.io for the short forms ("redis:8.4.2", "library/redis:8.4.2"). It
// mirrors the registry half of the e2e suite's parseImageRef, which cannot be
// imported (it lives behind the suite's build tag); TestRegistryOfMatchesTheAPIDefault
// takes its default straight from the API package, so it cannot drift.
func registryOf(ref string) string {
	name := ref
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		name = ref[:i]
	}
	if first, _, ok := strings.Cut(name, "/"); ok {
		if strings.ContainsAny(first, ".:") || first == "localhost" {
			return first
		}
	}
	return v1alpha1.DefaultRegistry
}
