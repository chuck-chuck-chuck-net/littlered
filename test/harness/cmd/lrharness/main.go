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

// Command lrharness is the thin CLI over the pure work-list seam
// (test/harness/worklist). The driver is shell; every decision it makes is
// delegated here so it can be unit-tested.
//
//	lrharness validate   --worklist F
//	lrharness plan       --worklist F --index N     # POSIX `export` lines
//	lrharness record     --array A --index N ...    # one Result as JSON
//	lrharness rollup     --array A --total N        # NDJSON on stdin -> summary
//	lrharness render-job --worklist F --namespace NS --image IMG [--parallelism P]
//	lrharness images     --worklist F --driver-image I ...   # one ref per line
//	lrharness check-parked --array A --worklist F            # `kubectl get ns -o json` on stdin
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/littlered-operator/littlered-operator/test/harness/worklist"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "validate":
		err = cmdValidate(os.Args[2:])
	case "plan":
		err = cmdPlan(os.Args[2:])
	case "record":
		err = cmdRecord(os.Args[2:])
	case "rollup":
		err = cmdRollup(os.Args[2:])
	case "check-parked":
		err = cmdCheckParked(os.Args[2:])
	case "check-operators":
		err = cmdCheckOperators(os.Args[2:])
	case "cleanup-plan":
		err = cmdCleanupPlan(os.Args[2:])
	case "images":
		err = cmdImages(os.Args[2:])
	case "render-job":
		err = cmdRender(os.Args[2:], false)
	case "render":
		err = cmdRender(os.Args[2:], true)
	case "render-teardown":
		err = cmdRenderTeardown(os.Args[2:])
	case "site-credential":
		err = cmdSiteCredential(os.Args[2:])
	case "lab-validate":
		err = cmdLabValidate(os.Args[2:])
	case "kubeconfig":
		err = cmdKubeconfig(os.Args[2:])
	case "fixture":
		err = cmdFixture(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "lrharness: unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "lrharness: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `lrharness — in-cluster e2e work-list driver helper

  validate   --worklist F
  plan       --worklist F --index N
  record     --array A --index N --name X --exit C [...]
  rollup     --array A --total N            (reads NDJSON results on stdin)
  render     --worklist F --namespace NS --image IMG [--parallelism P]
             (namespace + RBAC + work-list ConfigMap + Job, as one manifest)
  render-job --worklist F --namespace NS --image IMG   (the Job alone)
  check-parked --array A --worklist F
             (reads "kubectl get ns -o json" on stdin; exits 1, with the
              refusal, if the array has PARKED namespaces from an earlier run)
  check-operators --worklist F
             (reads "kubectl get deploy -A -l control-plane=controller-manager
              -o json" on stdin; exits 1, with the refusal, if an operator on
              the cluster would also reconcile this array's namespaces)
  cleanup-plan --worklist F [--lab L]
             (the ORDERED cleanup steps across hub and sites, TAB-separated:
              site, wait, selector, names, why)
  images     --worklist F [--driver-image I] [--operator-image O]
             [--chaos-client-image C]
             (every image ref the array pins, one per line — what a launch
              must be able to pull before it is worth starting)

multi-site (design §11 M3):

  site-credential
             (the harness's OWN per-site credential manifest: namespace, SA,
              ClusterRole, binding and a long-lived token. A ClusterRole because
              namespaces is a cluster-scoped resource. Applied once per site by the
              bootstrap; deliberately SEPARATE from the operator's Role.)
  lab-validate   --lab F
  kubeconfig     --lab F --creds DIR --out FILE
             (the driver pod's four-context kubeconfig: the hub from its own
              in-cluster ServiceAccount, one context per site from the mounted
              credential Secret)
  fixture        --name N --lab F --operator-namespace NS --msconfig-out FILE
             (resolve a matrix §3 topology: writes the item's MS_CONFIG, prints
              the suite environment as POSIX exports)
  render-teardown --worklist F --namespace NS --image IMG
             (the run-scoped teardown Job; NOT part of the launch bundle)
`)
}

func loadList(path string) (worklist.List, error) {
	data, err := os.ReadFile(path) //nolint:gosec // operator-supplied work list path
	if err != nil {
		return worklist.List{}, fmt.Errorf("reading work list: %w", err)
	}
	return worklist.Parse(data)
}

func cmdValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	path := fs.String("worklist", "", "path to the work list YAML")
	if err := fs.Parse(args); err != nil {
		return err
	}
	l, err := loadList(*path)
	if err != nil {
		return err
	}
	fmt.Printf("work list %q is valid: %d item(s)\n", l.Array, len(l.Items))
	for i, it := range l.Items {
		r, err := l.Resolve(i)
		if err != nil {
			return err
		}
		fmt.Printf("  %d  %-28s  timeout=%-6s  ns=%s  operator-ns=%s\n",
			i, it.Name, r.Timeout, r.Namespace, r.OperatorNamespace)
	}
	return nil
}

func cmdPlan(args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	path := fs.String("worklist", "", "path to the work list YAML")
	index := fs.Int("index", -1, "work item index (JOB_COMPLETION_INDEX)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	l, err := loadList(*path)
	if err != nil {
		return err
	}
	r, err := l.Resolve(*index)
	if err != nil {
		return err
	}
	fmt.Print(r.ShellExport())
	return nil
}

func cmdRecord(args []string) error {
	fs := flag.NewFlagSet("record", flag.ExitOnError)
	array := fs.String("array", "", "array name")
	index := fs.Int("index", -1, "work item index")
	name := fs.String("name", "", "work item name")
	namespace := fs.String("namespace", "", "the item's test namespace")
	exitCode := fs.Int("exit", 0, "exit code of the work item")
	timedOut := fs.Bool("timed-out", false, "the item hit its deadline")
	startedEpoch := fs.Int64("started-epoch", 0, "start time, unix seconds")
	finishedEpoch := fs.Int64("finished-epoch", 0, "end time, unix seconds")
	artifactDir := fs.String("artifact-dir", "", "where the item's artifacts landed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *index < 0 {
		return fmt.Errorf("--index is required")
	}

	started := time.Unix(*startedEpoch, 0).UTC()
	finished := time.Unix(*finishedEpoch, 0).UTC()
	verdict := worklist.Classify(*exitCode, *timedOut)

	res := worklist.Result{
		Array:           *array,
		Index:           *index,
		Name:            *name,
		Verdict:         verdict,
		ExitCode:        *exitCode,
		DurationSeconds: finished.Sub(started).Seconds(),
		StartedAt:       started.Format(time.RFC3339),
		FinishedAt:      finished.Format(time.RFC3339),
		Namespace:       *namespace,
		// A non-green item leaves its namespace standing for forensics
		// (design §4.2). "Parked" is derived from the verdict, never asserted
		// by the shell, so the record cannot disagree with the cleanup.
		Parked:      !verdict.Green(),
		ArtifactDir: *artifactDir,
	}
	enc := json.NewEncoder(os.Stdout)
	return enc.Encode(res)
}

func cmdRollup(args []string) error {
	fs := flag.NewFlagSet("rollup", flag.ExitOnError)
	array := fs.String("array", "", "array name")
	total := fs.Int("total", 0, "work list length (the array's completions)")
	strict := fs.Bool("strict", false, "exit non-zero unless every item is green")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var results []worklist.Result
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r worklist.Result
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return fmt.Errorf("parsing result record %q: %w", line, err)
		}
		results = append(results, r)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("reading results: %w", err)
	}

	t := *total
	if t == 0 {
		t = len(results)
	}
	s := worklist.Rollup(*array, t, results)
	fmt.Print(s.String())
	if *strict && !s.AllGreen {
		os.Exit(1)
	}
	return nil
}

// cmdCheckParked refuses a launch that would clobber parked evidence. The
// cluster lookup stays in the recipe; everything decided here comes from the
// pure seam.
func cmdCheckParked(args []string) error {
	fs := flag.NewFlagSet("check-parked", flag.ExitOnError)
	array := fs.String("array", "", "array name")
	path := fs.String("worklist", "", "work list path, for the remediation command")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *array == "" {
		return fmt.Errorf("--array is required")
	}

	// The shape of `kubectl get namespaces -o json`, reduced to the stamps.
	var list struct {
		Items []struct {
			Metadata struct {
				Name        string            `json:"name"`
				Labels      map[string]string `json:"labels"`
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		} `json:"items"`
	}
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("reading the namespace list: %w", err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		// No namespaces at all is the normal case for a fresh array.
		fmt.Printf("no parked namespaces for array %q\n", *array)
		return nil
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return fmt.Errorf("parsing the namespace list: %w", err)
	}

	observed := make([]worklist.ObservedNamespace, 0, len(list.Items))
	for _, it := range list.Items {
		observed = append(observed, worklist.ObserveNamespace(
			it.Metadata.Name, it.Metadata.Labels, it.Metadata.Annotations))
	}

	blockers := worklist.LaunchBlockers(*array, observed)
	if len(blockers) == 0 {
		fmt.Printf("no parked namespaces for array %q\n", *array)
		return nil
	}
	fmt.Fprint(os.Stderr, worklist.RefusalMessage(*array, *path, blockers))
	os.Exit(1)
	return nil
}

// cmdImages lists every image reference the array pins. The Makefile feeds the
// list to a registry lookup before applying the Job, so a missing ref is one
// sentence at launch instead of N identical ImagePullBackOff timeouts mid-array.
func cmdImages(args []string) error {
	fs := flag.NewFlagSet("images", flag.ExitOnError)
	path := fs.String("worklist", "", "path to the work list YAML")
	driver := fs.String("driver-image", "", "the driver image the Job runs")
	operator := fs.String("operator-image", "", "array-wide operator image")
	chaos := fs.String("chaos-client-image", "", "array-wide chaos-client image")
	if err := fs.Parse(args); err != nil {
		return err
	}
	l, err := loadList(*path)
	if err != nil {
		return err
	}
	for _, ref := range l.ImageRefs(worklist.ImageParams{
		DriverImage:      *driver,
		OperatorImage:    *operator,
		ChaosClientImage: *chaos,
	}) {
		fmt.Println(ref)
	}
	return nil
}

// cmdRender emits either the Job alone or the whole launch bundle. They share
// every flag, so they share one implementation.
func cmdRender(args []string, bundle bool) error {
	fs := flag.NewFlagSet("render", flag.ExitOnError)
	path := fs.String("worklist", "", "path to the work list YAML")
	namespace := fs.String("namespace", "lr-harness-system", "namespace for the Job")
	image := fs.String("image", "", "driver image ref")
	pullPolicy := fs.String("image-pull-policy", "", "driver image pull policy (default IfNotPresent)")
	parallelism := fs.Int("parallelism", 1, "concurrency cap")
	sa := fs.String("service-account", "", "driver ServiceAccount")
	cmName := fs.String("worklist-configmap", "", "ConfigMap holding the work list")
	pvc := fs.String("artifacts-pvc", "", "PVC backing the artifact volume (else hostPath)")
	hostPath := fs.String("artifacts-hostpath", "", "node path backing the artifact volume")
	gitTag := fs.String("git-tag", "", "image tag the suite resolves its images from")
	registry := fs.String("registry", "", "LITTLERED_REGISTRY override for the run")
	clobber := fs.Bool("allow-parked-clobber", false,
		"let the driver delete a PARKED namespace of this array (evidence loss; off by default)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	raw, err := os.ReadFile(*path) //nolint:gosec // operator-supplied work list path
	if err != nil {
		return fmt.Errorf("reading work list: %w", err)
	}
	l, err := worklist.Parse(raw)
	if err != nil {
		return err
	}

	jp := worklist.JobParams{
		List:               l,
		Namespace:          *namespace,
		Image:              *image,
		ImagePullPolicy:    *pullPolicy,
		Parallelism:        *parallelism,
		ServiceAccount:     *sa,
		WorkListName:       *cmName,
		ArtifactsPVC:       *pvc,
		ArtifactsHostPath:  *hostPath,
		GitTag:             *gitTag,
		Registry:           *registry,
		AllowParkedClobber: *clobber,
	}

	var out string
	if bundle {
		out, err = worklist.RenderBundle(worklist.BundleParams{JobParams: jp, WorkListYAML: string(raw)})
	} else {
		out, err = worklist.RenderJob(jp)
	}
	if err != nil {
		return err
	}
	fmt.Print(out)
	return nil
}
