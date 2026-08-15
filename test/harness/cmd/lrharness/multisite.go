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

package main

// The multi-site verbs (design §11 M3). As everywhere else in this CLI, the
// decisions live in the pure seams (test/harness/lab, test/harness/fixture) and
// this file is only the I/O around them: read files, write files, print shell.

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/littlered-operator/littlered-operator/test/harness/fixture"
	"github.com/littlered-operator/littlered-operator/test/harness/lab"
	"github.com/littlered-operator/littlered-operator/test/harness/worklist"
)

// Where the in-cluster ServiceAccount credentials of the pod's OWN cluster (the
// hub) are mounted by the kubelet.
const inClusterSADir = "/var/run/secrets/kubernetes.io/serviceaccount"

// cmdSiteCredential prints the harness's per-site credential manifest. The
// bootstrap applies it to each site with the workstation's admin context; it is
// long-lived lab infrastructure, not per-array state.
func cmdSiteCredential(args []string) error {
	fs := flag.NewFlagSet("site-credential", flag.ExitOnError)
	names := fs.Bool("names", false,
		"print the object names as shell assignments instead of the manifest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *names {
		// So the bootstrap recipe can `eval` them rather than restating the
		// names in Make, where they would drift from the seam that renders them.
		fmt.Printf("LRH_SITE_NAMESPACE=%s\n", lab.SiteNamespace)
		fmt.Printf("LRH_SITE_SERVICEACCOUNT=%s\n", lab.SiteServiceAccount)
		fmt.Printf("LRH_SITE_CLUSTERROLE=%s\n", lab.SiteClusterRole)
		fmt.Printf("LRH_SITE_TOKEN_SECRET=%s\n", lab.SiteTokenSecret)
		fmt.Printf("LRH_SITE_CRED_SECRET=%s\n", worklist.SiteCredentialSecretName)
		fmt.Printf("LRH_LAB_CONFIGMAP=%s\n", worklist.LabConfigMapName)
		return nil
	}
	fmt.Print(lab.RenderSiteCredential())
	return nil
}

// cmdLabValidate checks a lab inventory without touching a cluster.
func cmdLabValidate(args []string) error {
	fs := flag.NewFlagSet("lab-validate", flag.ExitOnError)
	path := fs.String("lab", "", "path to the lab inventory YAML")
	print := fs.String("print", "",
		`machine-readable output for the bootstrap recipe: "hub" (the hub admin context) `+
			`or "sites" (one TAB-separated name/context/endpoint row per site)`)
	if err := fs.Parse(args); err != nil {
		return err
	}
	l, err := loadLab(*path)
	if err != nil {
		return err
	}
	switch *print {
	case "hub":
		fmt.Println(l.HubContext)
		return nil
	case "sites":
		for _, s := range l.Sites {
			fmt.Printf("%s\t%s\t%s\n", s.Name, s.Context, s.Endpoint)
		}
		return nil
	case "":
	default:
		return fmt.Errorf("--print %q is not a form; use \"hub\" or \"sites\"", *print)
	}
	fmt.Printf("lab inventory is valid: hub context %q, %d site(s)\n", l.HubContext, len(l.Sites))
	for i, s := range l.Sites {
		fmt.Printf("  %d  %-12s  admin-context=%-12s  endpoint=%s\n", i, s.Name, s.Context, s.Endpoint)
	}
	return nil
}

func loadLab(path string) (lab.Lab, error) {
	if strings.TrimSpace(path) == "" {
		return lab.Lab{}, fmt.Errorf("--lab is required (the lab inventory YAML)")
	}
	data, err := os.ReadFile(path) //nolint:gosec // operator-supplied inventory path
	if err != nil {
		return lab.Lab{}, fmt.Errorf("reading the lab inventory: %w", err)
	}
	return lab.ParseLab(data)
}

// cmdKubeconfig assembles the driver pod's four-context kubeconfig: the hub
// from the pod's own in-cluster ServiceAccount, and one context per site from
// the mounted credential Secret plus the inventory's endpoints.
//
// This is M0's proven mechanism, and it is not replaceable by in-cluster config:
// the multi-site e2e helpers drive sites BY KUBECONTEXT (`kctl` prepends
// --context), so a real file with real context names has to exist.
func cmdKubeconfig(args []string) error {
	fs := flag.NewFlagSet("kubeconfig", flag.ExitOnError)
	labPath := fs.String("lab", "", "path to the mounted lab inventory")
	credDir := fs.String("creds", "", "directory of the mounted per-site credential Secret")
	saDir := fs.String("sa-dir", inClusterSADir, "the pod's own in-cluster ServiceAccount directory")
	out := fs.String("out", "", "where to write the kubeconfig")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*out) == "" {
		return fmt.Errorf("--out is required")
	}
	l, err := loadLab(*labPath)
	if err != nil {
		return err
	}

	hub, err := hubFromServiceAccount(*saDir)
	if err != nil {
		return err
	}

	// The ENDPOINT comes from the inventory and the credentials from the Secret,
	// so a site's hub-routable URL is written down exactly once.
	sites := make([]lab.Cluster, 0, len(l.Sites))
	for _, s := range l.Sites {
		ca, err := readCred(*credDir, s.Name, "ca")
		if err != nil {
			return err
		}
		token, err := readCred(*credDir, s.Name, "token")
		if err != nil {
			return err
		}
		sites = append(sites, lab.Cluster{Name: s.Name, Server: s.Endpoint, CAPEM: ca, Token: token})
	}

	kc, err := lab.RenderKubeconfig(hub, sites)
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out, []byte(kc), 0o600); err != nil {
		return fmt.Errorf("writing the kubeconfig: %w", err)
	}
	// STDOUT is the machine-readable half: one SITE context name per line, so
	// the driver can iterate the sites without re-parsing the inventory in
	// shell. The human summary goes to stderr, and the hub is deliberately not
	// on the list — it is `current-context`, not a site.
	names := make([]string, 0, len(sites))
	for _, s := range sites {
		names = append(names, s.Name)
		fmt.Println(s.Name)
	}
	fmt.Fprintf(os.Stderr, "wrote %s: contexts %s (current: %s)\n",
		*out, strings.Join(append([]string{lab.HubContextName}, names...), ", "), lab.HubContextName)
	return nil
}

// hubFromServiceAccount reads the pod's own cluster credentials. The API server
// address comes from the env the kubelet injects, exactly as an in-cluster
// client would resolve it.
func hubFromServiceAccount(dir string) (lab.Cluster, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return lab.Cluster{}, fmt.Errorf("KUBERNETES_SERVICE_HOST/PORT are unset: this verb " +
			"assembles the hub context from the pod's OWN in-cluster credentials and only " +
			"runs inside a pod")
	}
	token, err := os.ReadFile(filepath.Join(dir, "token")) //nolint:gosec // fixed in-cluster path
	if err != nil {
		return lab.Cluster{}, fmt.Errorf("reading the pod's ServiceAccount token: %w", err)
	}
	ca, err := os.ReadFile(filepath.Join(dir, "ca.crt")) //nolint:gosec // fixed in-cluster path
	if err != nil {
		return lab.Cluster{}, fmt.Errorf("reading the pod's cluster CA: %w", err)
	}
	return lab.Cluster{
		Name:   lab.HubContextName,
		Server: "https://" + net(host, port),
		CAPEM:  string(ca),
		Token:  strings.TrimSpace(string(token)),
	}, nil
}

// net joins host and port, bracketing an IPv6 literal.
func net(host, port string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]:" + port
	}
	return host + ":" + port
}

func readCred(dir, site, key string) (string, error) {
	if strings.TrimSpace(dir) == "" {
		return "", fmt.Errorf("--creds is required (the mounted per-site credential Secret)")
	}
	name := site + "." + key
	data, err := os.ReadFile(filepath.Join(dir, name)) //nolint:gosec // mounted Secret path
	if err != nil {
		return "", fmt.Errorf("site %q: reading %s from the credential Secret: %w — has the "+
			"lab been bootstrapped? (make e2e-harness-sites-setup)", site, name, err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return "", fmt.Errorf("site %q: %s in the credential Secret is empty", site, name)
	}
	return string(data), nil
}

// cmdFixture resolves a work item's topology fixture: it writes the item's own
// MS_CONFIG and prints the environment the e2e suite needs, as POSIX exports.
//
// MS_CONFIG is per ITEM rather than a single mounted file, and that is forced,
// not chosen: the operator reads its site kubeconfig Secrets from its OWN
// namespace, and every item deploys its own operator into its own namespace.
func cmdFixture(args []string) error {
	fs := flag.NewFlagSet("fixture", flag.ExitOnError)
	name := fs.String("name", "", "fixture name (matrix §3), e.g. F2-K4")
	labPath := fs.String("lab", "", "path to the mounted lab inventory")
	opNS := fs.String("operator-namespace", "", "the ITEM's own operator namespace")
	msOut := fs.String("msconfig-out", "", "where to write the item's MS_CONFIG")
	if err := fs.Parse(args); err != nil {
		return err
	}
	f, err := fixture.Lookup(*name)
	if err != nil {
		return err
	}
	if err := f.Validate(); err != nil {
		return err
	}
	l, err := loadLab(*labPath)
	if err != nil {
		return err
	}
	sites, err := l.Select(f.Sites)
	if err != nil {
		return fmt.Errorf("fixture %s: %w", f.Name, err)
	}
	if strings.TrimSpace(*msOut) == "" {
		return fmt.Errorf("--msconfig-out is required")
	}

	cfg, err := lab.RenderMSConfig(sites, *opNS)
	if err != nil {
		return err
	}
	if err := os.WriteFile(*msOut, []byte(cfg), 0o600); err != nil {
		return fmt.Errorf("writing MS_CONFIG: %w", err)
	}

	env := f.Env()
	env["MS_CONFIG"] = *msOut
	// Pin the suite to the hub context. The suite's own kubeconfig pinning
	// deliberately does NOT --minify when a multi-site config is present, so the
	// sibling site contexts survive.
	env["KUBECONTEXT"] = lab.HubContextName

	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("export %s=%s\n", k, shellQuote(env[k]))
	}

	names := make([]string, 0, len(sites))
	for _, s := range sites {
		names = append(names, s.Name)
	}
	fmt.Fprintf(os.Stderr,
		"fixture %s: %d site(s) %s, K=%d shards, sitesPerShard=%d, replicasPerShard=%d "+
			"=> %d pods, initial master split %v, quorum %d (takeover floor %d, native target %d)\n",
		f.Name, f.Sites, strings.Join(names, "+"), f.Shards, f.SitesPerShard, f.ReplicasPerShard,
		f.Pods(), f.MasterSplit(), f.Quorum(), f.TakeoverSurvivalFloor(), f.NativeSurvivalTarget())
	return nil
}

// shellQuote mirrors worklist.ShellExport's quoting: single quotes with the
// canonical '\” escape, so a value can hold anything.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// cmdRenderTeardown emits the run-scoped teardown Job. It is NOT part of the
// launch bundle: applying it at launch would run it immediately, before the
// array it is meant to follow.
func cmdRenderTeardown(args []string) error {
	fs := flag.NewFlagSet("render-teardown", flag.ExitOnError)
	path := fs.String("worklist", "", "path to the work list YAML")
	namespace := fs.String("namespace", "lr-harness-system", "namespace for the Job")
	image := fs.String("image", "", "driver image ref")
	pullPolicy := fs.String("image-pull-policy", "", "driver image pull policy (default IfNotPresent)")
	sa := fs.String("service-account", "", "driver ServiceAccount")
	cmName := fs.String("worklist-configmap", "", "ConfigMap holding the work list")
	pvc := fs.String("artifacts-pvc", "", "PVC backing the artifact volume (else hostPath)")
	hostPath := fs.String("artifacts-hostpath", "", "node path backing the artifact volume")
	if err := fs.Parse(args); err != nil {
		return err
	}
	l, err := loadList(*path)
	if err != nil {
		return err
	}
	out, err := worklist.RenderTeardownJob(worklist.JobParams{
		List:              l,
		Namespace:         *namespace,
		Image:             *image,
		ImagePullPolicy:   *pullPolicy,
		Parallelism:       1,
		ServiceAccount:    *sa,
		WorkListName:      *cmName,
		ArtifactsPVC:      *pvc,
		ArtifactsHostPath: *hostPath,
	})
	if err != nil {
		return err
	}
	fmt.Print(out)
	return nil
}

// cmdCheckOperators refuses a launch that would run beside an operator whose
// watch scope overlaps the array. The cluster lookup stays in the recipe; the
// decision comes from the pure seam, exactly like check-parked.
func cmdCheckOperators(args []string) error {
	fs := flag.NewFlagSet("check-operators", flag.ExitOnError)
	path := fs.String("worklist", "", "path to the work list YAML")
	if err := fs.Parse(args); err != nil {
		return err
	}
	l, err := loadList(*path)
	if err != nil {
		return err
	}

	// The shape of `kubectl get deployments -A -o json`, reduced to what the
	// scope decision needs.
	var list struct {
		Items []struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Spec struct {
				Replicas *int32 `json:"replicas"`
				Template struct {
					Spec struct {
						Containers []struct {
							Env []struct {
								Name  string `json:"name"`
								Value string `json:"value"`
							} `json:"env"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		} `json:"items"`
	}
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("reading the deployment list: %w", err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		fmt.Printf("no operator on this cluster conflicts with array %q\n", l.Array)
		return nil
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return fmt.Errorf("parsing the deployment list: %w", err)
	}

	observed := make([]worklist.ObservedOperator, 0, len(list.Items))
	for _, it := range list.Items {
		op := worklist.ObservedOperator{
			Namespace: it.Metadata.Namespace,
			Name:      it.Metadata.Name,
			Replicas:  1, // a Deployment with no explicit replicas defaults to 1
		}
		if it.Spec.Replicas != nil {
			op.Replicas = *it.Spec.Replicas
		}
		// Scan every container: the scope env lives on the manager, but the
		// container name is not something this check should depend on.
		for _, c := range it.Spec.Template.Spec.Containers {
			for _, e := range c.Env {
				switch e.Name {
				case "WATCH_NAMESPACE":
					op.WatchNamespace = e.Value
				case "IGNORE_NAMESPACE":
					op.IgnoreNamespace = e.Value
				}
			}
		}
		observed = append(observed, op)
	}

	conflicts := l.ConflictingOperators(observed)
	if len(conflicts) == 0 {
		fmt.Printf("no operator on this cluster conflicts with array %q (%d checked)\n",
			l.Array, len(observed))
		return nil
	}
	fmt.Fprint(os.Stderr, worklist.OperatorRefusalMessage(l.Array, conflicts))
	os.Exit(1)
	return nil
}

// cmdCleanupPlan prints the ordered cleanup steps as shell-consumable lines, so
// the ORDER and the WAITING — the load-bearing parts — are decided here and not
// re-derived in a recipe.
//
// The record format is worklist.CleanupTarget.Line — six fields separated by
// ASCII unit separator, NOT tab: several fields are legitimately empty, and tab
// is IFS whitespace, so a shell `read` would collapse runs of it and shift every
// field after the first empty one.
//
// The seam names the SITE (its logical name, which is what the operator and the
// CR use); reaching it from a workstation needs the lab inventory's admin
// kubecontext, which is I/O configuration and so is resolved here.
func cmdCleanupPlan(args []string) error {
	fs := flag.NewFlagSet("cleanup-plan", flag.ExitOnError)
	path := fs.String("worklist", "", "path to the work list YAML")
	labPath := fs.String("lab", "", "lab inventory (required when the array has multi-site items)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	l, err := loadList(*path)
	if err != nil {
		return err
	}

	var sites []string
	adminContext := map[string]string{}
	if strings.TrimSpace(*labPath) != "" {
		lb, lerr := loadLab(*labPath)
		if lerr != nil {
			return lerr
		}
		for _, s := range lb.Sites {
			sites = append(sites, s.Name)
			adminContext[s.Name] = s.Context
		}
	}

	plan, err := l.CleanupPlan(sites)
	if err != nil {
		return err
	}
	for _, s := range plan {
		fmt.Println(s.Line(adminContext[s.Site]))
	}
	return nil
}
