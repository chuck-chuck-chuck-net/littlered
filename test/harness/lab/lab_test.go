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

package lab

import (
	"encoding/base64"
	"os"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const sampleLab = `
hubContext: my-hub
sites:
  - {name: site-a, context: admin-a, endpoint: "https://10.1.1.10:6443"}
  - {name: site-b, context: admin-b, endpoint: "https://10.1.2.10:6443"}
  - {name: site-c, context: admin-c, endpoint: "https://10.1.3.10:6443"}
`

// ---------------------------------------------------------------------------
// ParseLab — the inventory is the ONE place a lab address is written down, and
// it is never committed. A malformed one must be rejected at the bootstrap /
// launch, not at item 1 of an overnight array.
// ---------------------------------------------------------------------------

func TestParseLab(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr string // substring; "" = success
	}{
		{name: "the sample", yaml: sampleLab},
		{
			name:    "no hub",
			yaml:    "sites:\n  - {name: a, context: c, endpoint: \"https://h:6443\"}\n",
			wantErr: "hubContext",
		},
		{
			name:    "no sites",
			yaml:    "hubContext: h\n",
			wantErr: "site",
		},
		{
			name: "duplicate site names",
			yaml: "hubContext: h\nsites:\n" +
				"  - {name: a, context: c1, endpoint: \"https://h1:6443\"}\n" +
				"  - {name: a, context: c2, endpoint: \"https://h2:6443\"}\n",
			wantErr: "duplicate",
		},
		{
			name:    "site without a context",
			yaml:    "hubContext: h\nsites:\n  - {name: a, endpoint: \"https://h1:6443\"}\n",
			wantErr: "context",
		},
		{
			name:    "site without an endpoint",
			yaml:    "hubContext: h\nsites:\n  - {name: a, context: c}\n",
			wantErr: "endpoint",
		},
		{
			// The endpoint is the site API URL as routable FROM HUB PODS. A
			// laptop-vantage http:// or bare host is the classic wrong-vantage
			// mistake (MS_CONFIG sample warns about exactly this).
			name:    "endpoint that is not an https URL",
			yaml:    "hubContext: h\nsites:\n  - {name: a, context: c, endpoint: \"10.1.1.10:6443\"}\n",
			wantErr: "https://",
		},
		{
			// The site name becomes a kubeconfig context AND a CR site name, so
			// it must be a DNS label.
			name:    "site name that is not a DNS label",
			yaml:    "hubContext: h\nsites:\n  - {name: Site_A, context: c, endpoint: \"https://h:6443\"}\n",
			wantErr: "DNS label",
		},
		{
			// The in-pod kubeconfig gives the hub a fixed context name. A site
			// called the same thing would shadow it and every hub call would
			// silently go to a site.
			name: "a site named like the hub context",
			yaml: "hubContext: h\nsites:\n  - {name: " + HubContextName +
				", context: c, endpoint: \"https://h:6443\"}\n",
			wantErr: HubContextName,
		},
		{
			name:    "not YAML at all",
			yaml:    "\tthis: [is: broken",
			wantErr: "YAML",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseLab([]byte(tc.yaml))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ParseLab() = %v, want success", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ParseLab() = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ParseLab() = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestSelectIsOrderedAndBounded(t *testing.T) {
	l, err := ParseLab([]byte(sampleLab))
	if err != nil {
		t.Fatalf("ParseLab: %v", err)
	}

	got, err := l.Select(2)
	if err != nil {
		t.Fatalf("Select(2): %v", err)
	}
	if len(got) != 2 || got[0].Name != "site-a" || got[1].Name != "site-b" {
		t.Errorf("Select(2) = %v, want the first two sites in inventory order — "+
			"placement is index-based (LR-032), so the ORDER is the topology", got)
	}

	if _, err := l.Select(4); err == nil {
		t.Error("Select(4) on a 3-site inventory must fail rather than quietly " +
			"running a smaller topology than the fixture asked for")
	}
	if _, err := l.Select(0); err == nil {
		t.Error("Select(0) must fail")
	}
}

// ---------------------------------------------------------------------------
// The per-site harness credential. THE decision of M3 (design §11 M0 Finding 2):
// `namespaces` is a cluster-scoped resource, so a namespaced Role cannot grant
// `create` on it at any rule content — and the harness creates a fresh
// namespace per work item, so pre-provisioning cannot serve it either.
//
// The property the tests defend is not "it works" but "it stays SEPARATE from
// the operator's credential": ADR-012 makes "multi-site keeps the namespaced
// per-site Role — no cluster-scoped RBAC" a property of the OPERATOR, and a
// later reader must not be able to "simplify" the two together.
// ---------------------------------------------------------------------------

// operatorSiteObjectName is what msSetupSites names the OPERATOR's own per-site
// SA / Role / RoleBinding (test/e2e/multisite_utils_test.go, msSiteRBAC).
const operatorSiteObjectName = "littlered-site"

// A stand-in server URL for the negative kubeconfig cases.
const someServer = "https://x:6443"

func TestSiteCredentialIsClusterScopedBecauseNamespacesAre(t *testing.T) {
	m := RenderSiteCredential()

	if !strings.Contains(m, "kind: ClusterRole") {
		t.Error("the harness site credential must be a ClusterRole: `namespaces` is a " +
			"cluster-scoped resource, so no Role can grant create on it (design §11 M0 Finding 2)")
	}
	if !strings.Contains(m, "kind: ClusterRoleBinding") {
		t.Error("a ClusterRole without a ClusterRoleBinding grants nothing")
	}
	if !strings.Contains(m, "namespaces") {
		t.Error("the credential must grant `namespaces` — creating the per-item namespace " +
			"is the whole reason it cannot be a Role")
	}
	// The harness execs into redis pods; the operator deliberately does not
	// (it reaches Redis over TCP), which is why this credential exists at all.
	if !strings.Contains(m, "pods/exec") {
		t.Error("the harness credential must grant pods/exec — the operator's does not, " +
			"and that asymmetry is the reason for a separate credential (design §11 M0 Finding 1)")
	}
}

func TestSiteCredentialNeverCollidesWithTheOperators(t *testing.T) {
	m := RenderSiteCredential()

	// Check the OBJECT names, not the text: the manifest's comments legitimately
	// mention the operator's credential in order to say "not this one".
	for i, doc := range strings.Split(m, "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var obj struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			RoleRef struct {
				Name string `json:"name"`
			} `json:"roleRef"`
		}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("document %d is not valid YAML: %v", i, err)
		}
		for _, n := range []string{obj.Metadata.Name, obj.RoleRef.Name} {
			if n == operatorSiteObjectName {
				t.Errorf("document %d (%s) is named %q — the OPERATOR's per-site object. "+
					"They must be separate objects with separate names, so nobody later "+
					"'simplifies' the operator's namespaced Role by merging them (ADR-012 D1).",
					i, obj.Kind, operatorSiteObjectName)
			}
		}
	}
	for _, want := range []string{SiteNamespace, SiteServiceAccount, SiteClusterRole, SiteTokenSecret} {
		if want == "" {
			t.Fatal("the credential object names must not be empty")
		}
		if !strings.Contains(m, want) {
			t.Errorf("rendered credential does not mention %q", want)
		}
	}
	// A dedicated namespace, not the operator's: the harness credential must not
	// be reachable by anything that only has the instance namespace.
	if SiteNamespace == operatorSiteObjectName {
		t.Error("the harness site namespace must differ from the operator's object name")
	}
}

func TestSiteCredentialIsValidMultiDocumentYAML(t *testing.T) {
	docs := strings.Split(RenderSiteCredential(), "\n---\n")
	if len(docs) < 5 {
		t.Fatalf("expected at least 5 documents (Namespace, SA, ClusterRole, "+
			"ClusterRoleBinding, token Secret), got %d", len(docs))
	}
	for i, d := range docs {
		if strings.TrimSpace(d) == "" {
			continue
		}
		var obj map[string]any
		if err := yaml.Unmarshal([]byte(d), &obj); err != nil {
			t.Errorf("document %d is not valid YAML: %v", i, err)
			continue
		}
		if obj["kind"] == nil || obj["apiVersion"] == nil {
			t.Errorf("document %d has no kind/apiVersion: %v", i, obj)
		}
	}
}

// ---------------------------------------------------------------------------
// The in-pod kubeconfig (design §11 M0: SA token + CA + hub-routable endpoint
// -> one context per site, plus the hub). The suite drives sites BY CONTEXT
// NAME (`kctl` prepends --context), so a real file with real context names is
// not optional.
// ---------------------------------------------------------------------------

func hubCluster() Cluster {
	return Cluster{Name: HubContextName, Server: "https://10.96.0.1:443", CAPEM: "HUB-CA-PEM", Token: "hub-token"}
}

func siteClusters() []Cluster {
	return []Cluster{
		{Name: "site-a", Server: "https://10.1.1.10:6443", CAPEM: "A-CA-PEM", Token: "a-token"},
		{Name: "site-b", Server: "https://10.1.2.10:6443", CAPEM: "B-CA-PEM", Token: "b-token"},
	}
}

func TestRenderKubeconfigProducesOneContextPerCluster(t *testing.T) {
	out, err := RenderKubeconfig(hubCluster(), siteClusters())
	if err != nil {
		t.Fatalf("RenderKubeconfig: %v", err)
	}

	var kc struct {
		CurrentContext string `json:"current-context"`
		Clusters       []struct {
			Name    string `json:"name"`
			Cluster struct {
				Server string `json:"server"`
				CAData string `json:"certificate-authority-data"`
			} `json:"cluster"`
		} `json:"clusters"`
		Contexts []struct {
			Name    string `json:"name"`
			Context struct {
				Cluster string `json:"cluster"`
				User    string `json:"user"`
			} `json:"context"`
		} `json:"contexts"`
		Users []struct {
			Name string `json:"name"`
			User struct {
				Token string `json:"token"`
			} `json:"user"`
		} `json:"users"`
	}
	if err := yaml.Unmarshal([]byte(out), &kc); err != nil {
		t.Fatalf("rendered kubeconfig is not valid YAML: %v\n%s", err, out)
	}

	if len(kc.Contexts) != 3 {
		t.Errorf("got %d contexts, want 3 (hub + 2 sites)", len(kc.Contexts))
	}
	if kc.CurrentContext != HubContextName {
		t.Errorf("current-context = %q, want %q — ambient kubectl in the driver pod "+
			"must resolve to the HUB, never to a site", kc.CurrentContext, HubContextName)
	}

	byName := map[string]string{}
	for _, c := range kc.Clusters {
		byName[c.Name] = c.Cluster.CAData
	}
	// The CA travels as base64 in a kubeconfig but arrives as PEM from a
	// mounted Secret. Getting that wrong yields "x509: certificate signed by
	// unknown authority" against a site, minutes into an item.
	if got, want := byName["site-a"], base64.StdEncoding.EncodeToString([]byte("A-CA-PEM")); got != want {
		t.Errorf("site-a certificate-authority-data = %q, want the base64 of the PEM (%q)", got, want)
	}
	if got, want := byName[HubContextName], base64.StdEncoding.EncodeToString([]byte("HUB-CA-PEM")); got != want {
		t.Errorf("hub certificate-authority-data = %q, want %q", got, want)
	}

	for _, c := range kc.Contexts {
		if c.Context.Cluster != c.Name || c.Context.User != c.Name {
			t.Errorf("context %q points at cluster %q / user %q — a cross-wired context "+
				"sends a site's commands to another cluster with another token",
				c.Name, c.Context.Cluster, c.Context.User)
		}
	}
	if len(kc.Users) != 3 {
		t.Errorf("got %d users, want 3", len(kc.Users))
	}
}

func TestRenderKubeconfigRejectsUnusableInput(t *testing.T) {
	cases := []struct {
		name    string
		hub     Cluster
		sites   []Cluster
		wantErr string
	}{
		{
			name:    "no sites",
			hub:     hubCluster(),
			wantErr: "site",
		},
		{
			name: "a site named like the hub shadows it",
			hub:  hubCluster(),
			sites: []Cluster{
				{Name: HubContextName, Server: someServer, CAPEM: "P", Token: "t"},
			},
			wantErr: "hub",
		},
		{
			name: "duplicate site names",
			hub:  hubCluster(),
			sites: []Cluster{
				{Name: "a", Server: someServer, CAPEM: "P", Token: "t"},
				{Name: "a", Server: "https://y:6443", CAPEM: "P", Token: "t"},
			},
			wantErr: "duplicate",
		},
		{
			name:    "site with no token",
			hub:     hubCluster(),
			sites:   []Cluster{{Name: "a", Server: someServer, CAPEM: "P"}},
			wantErr: "token",
		},
		{
			name:    "site with no CA",
			hub:     hubCluster(),
			sites:   []Cluster{{Name: "a", Server: someServer, Token: "t"}},
			wantErr: "certificate authority",
		},
		{
			name:    "site with a non-https server",
			hub:     hubCluster(),
			sites:   []Cluster{{Name: "a", Server: "10.1.1.10:6443", CAPEM: "P", Token: "t"}},
			wantErr: "https://",
		},
		{
			name:    "hub with no token",
			hub:     Cluster{Name: HubContextName, Server: "https://x:443", CAPEM: "P"},
			sites:   siteClusters(),
			wantErr: "token",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RenderKubeconfig(tc.hub, tc.sites)
			if err == nil {
				t.Fatalf("RenderKubeconfig() = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("RenderKubeconfig() = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MS_CONFIG. The suite re-reads MS_CONFIG on every call, so a rendered file
// works with no code change — but the file is per ITEM, not per array: the
// operator reads its site kubeconfig Secrets from its OWN namespace
// (site_registry.go), and every work item deploys its own operator into its own
// namespace (M2). So operatorNamespace cannot be a launch-time constant.
// ---------------------------------------------------------------------------

func TestRenderMSConfigTargetsTheItemsOwnOperator(t *testing.T) {
	l, err := ParseLab([]byte(sampleLab))
	if err != nil {
		t.Fatalf("ParseLab: %v", err)
	}
	sites, err := l.Select(2)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	out, err := RenderMSConfig(sites, "lr-harness-op-msbring-0-f2k4")
	if err != nil {
		t.Fatalf("RenderMSConfig: %v", err)
	}

	var cfg struct {
		HubContext        string `json:"hubContext"`
		OperatorNamespace string `json:"operatorNamespace"`
		HubPodCIDR        string `json:"hubPodCIDR"`
		Sites             []struct {
			Name     string `json:"name"`
			Context  string `json:"context"`
			Endpoint string `json:"endpoint"`
			VMs      []any  `json:"vms"`
		} `json:"sites"`
	}
	if err := yaml.Unmarshal([]byte(out), &cfg); err != nil {
		t.Fatalf("rendered MS_CONFIG is not valid YAML: %v\n%s", err, out)
	}

	if cfg.OperatorNamespace != "lr-harness-op-msbring-0-f2k4" {
		t.Errorf("operatorNamespace = %q, want the ITEM's operator namespace — the "+
			"operator reads its site Secrets from its own namespace", cfg.OperatorNamespace)
	}
	if cfg.HubContext != HubContextName {
		t.Errorf("hubContext = %q, want the in-pod hub context %q", cfg.HubContext, HubContextName)
	}
	if len(cfg.Sites) != 2 {
		t.Fatalf("got %d sites, want 2", len(cfg.Sites))
	}
	for i, s := range cfg.Sites {
		// The lab inventory's `context` is the WORKSTATION's admin context; in
		// the pod the context is named after the site. Emitting the admin
		// context here is the bug this asserts against — it fails as
		// "context does not exist", but only once an item is already running.
		if s.Context != s.Name {
			t.Errorf("site %d: context = %q, want the in-pod context name %q",
				i, s.Context, s.Name)
		}
		if s.Endpoint != sites[i].Endpoint {
			t.Errorf("site %d: endpoint = %q, want %q", i, s.Endpoint, sites[i].Endpoint)
		}
		// M3 is explicitly NO FAULTS. A `vms` block would arm the Scenario A
		// whole-site kill for anything that selected it.
		if len(s.VMs) != 0 {
			t.Errorf("site %d carries a `vms` inventory — M3 injects no faults, and a "+
				"VM inventory is what arms the hypervisor power-off", i)
		}
	}
	// Likewise: hubPodCIDR is what arms the Scenario B data-plane cut.
	if cfg.HubPodCIDR != "" {
		t.Errorf("hubPodCIDR = %q, want empty — it arms the Scenario B cut", cfg.HubPodCIDR)
	}
}

func TestRenderMSConfigRejectsAnEmptyOperatorNamespace(t *testing.T) {
	l, _ := ParseLab([]byte(sampleLab))
	sites, _ := l.Select(2)
	if _, err := RenderMSConfig(sites, ""); err == nil {
		t.Error("an empty operatorNamespace must be refused: MS_CONFIG would silently " +
			"default to littlered-system and the operator would never find its site Secrets")
	}
	if _, err := RenderMSConfig(nil, "ns"); err == nil {
		t.Error("rendering MS_CONFIG with no sites must be refused")
	}
}

// Drift guard: the MS_CONFIG schema is owned by the e2e suite (msConfig in
// multisite_utils_test.go), which lives behind the `e2e` build tag and cannot
// be imported here. docs/multisite-e2e-config.sample.yaml is the maintained
// documentation of that schema, so every key we emit must appear in it — a
// renamed field shows up here instead of as a silently-ignored setting.
func TestRenderedMSConfigKeysExistInTheDocumentedSchema(t *testing.T) {
	raw, err := os.ReadFile("../../../docs/multisite-e2e-config.sample.yaml")
	if err != nil {
		t.Fatalf("reading the documented MS_CONFIG sample: %v", err)
	}
	var sample map[string]any
	if err := yaml.Unmarshal(raw, &sample); err != nil {
		t.Fatalf("the documented sample is not valid YAML: %v", err)
	}

	l, _ := ParseLab([]byte(sampleLab))
	sites, _ := l.Select(2)
	out, err := RenderMSConfig(sites, "some-operator-ns")
	if err != nil {
		t.Fatalf("RenderMSConfig: %v", err)
	}
	var rendered map[string]any
	if err := yaml.Unmarshal([]byte(out), &rendered); err != nil {
		t.Fatalf("rendered MS_CONFIG is not valid YAML: %v", err)
	}

	for k := range rendered {
		if _, ok := sample[k]; !ok {
			t.Errorf("rendered MS_CONFIG has top-level key %q, which the documented "+
				"schema (docs/multisite-e2e-config.sample.yaml) does not define", k)
		}
	}

	sampleSiteKeys := map[string]bool{}
	for _, s := range sample["sites"].([]any) {
		for k := range s.(map[string]any) {
			sampleSiteKeys[k] = true
		}
	}
	for _, s := range rendered["sites"].([]any) {
		for k := range s.(map[string]any) {
			if !sampleSiteKeys[k] {
				t.Errorf("rendered MS_CONFIG site has key %q, which the documented schema "+
					"does not define", k)
			}
		}
	}
}
