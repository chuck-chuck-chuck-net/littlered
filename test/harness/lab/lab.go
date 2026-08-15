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

// Package lab is the multi-site SUBSTRATE seam of the in-cluster e2e harness
// (docs/E2E_SOAK_HARNESS_DESIGN.md §11 M0/M3). Four pure decisions live here:
//
//   - Lab inventory   — WHERE the lab is: the hub context, and each site's
//     name / workstation admin context / hub-pod-routable API endpoint. Never
//     committed; a lab address is configuration, not a default.
//   - Site credential — the harness's OWN per-site credential. A ClusterRole,
//     because `namespaces` is a cluster-scoped resource (M0 Finding 2).
//   - Kubeconfig      — the in-pod four-context kubeconfig assembled from SA
//     tokens + CAs + endpoints (M0's proven mechanism).
//   - MS_CONFIG       — the per-ITEM multi-site config the e2e suite reads.
//
// # Why the harness credential is a ClusterRole, and why it is separate
//
// M0 established both halves. First: a namespaced Role cannot grant `create` on
// `namespaces` at any rule content, because `namespaces` is cluster-scoped —
// and the harness creates a fresh namespace per work item, on every site. The
// only alternative, pre-provisioning a fixed set of namespaces, cannot serve
// per-item namespaces at all: the names are derived from array+index+item, and
// the e2e suite refuses to start if its test namespace already exists. So:
// ClusterRole + ClusterRoleBinding, one per site.
//
// Second, and load-bearing: this does NOT weaken ADR-012's "multi-site keeps
// the namespaced per-site Role — no cluster-scoped RBAC". That property is
// about the OPERATOR's site credential, which is untouched and stays a
// namespaced Role (test/e2e/multisite_utils_test.go, msSiteRBAC). The harness
// is a TEST DRIVER with a deliberately broader credential — it execs into redis
// pods, which the operator never does (it reaches Redis over TCP), and it
// creates namespaces, which the operator never does either. The two are
// separate objects with separate names precisely so that nobody later
// "simplifies" the operator's Role by merging them.
package lab

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	"sigs.k8s.io/yaml"
)

// The harness's own per-site credential objects. Every name is distinct from
// the operator's `littlered-site` (msSiteRBAC) — see the package comment.
const (
	// SiteNamespace holds the harness's site ServiceAccount and its token. It is
	// long-lived lab infrastructure, created once by the bootstrap, NOT per item.
	SiteNamespace = "lr-e2e-harness"
	// SiteServiceAccount is the identity the driver pod presents to each site.
	SiteServiceAccount = "lr-e2e-harness"
	// SiteClusterRole is the cluster-scoped grant. Named "-driver" so that a
	// reader scanning RBAC on a site can tell at a glance that this is the test
	// driver, not the operator.
	SiteClusterRole = "lr-e2e-harness-driver"
	// SiteTokenSecret is the long-lived ServiceAccount token the bootstrap reads
	// to assemble the in-pod kubeconfig.
	SiteTokenSecret = "lr-e2e-harness-token"

	// HubContextName is what the hub is called in the ASSEMBLED in-pod
	// kubeconfig. Fixed, because the driver and the rendered MS_CONFIG must
	// agree on it without another parameter to get wrong. It is deliberately
	// NOT the workstation's hub context name: the two files live in different
	// places and mean different things.
	HubContextName = "hub"
)

// dnsLabel matches a legal DNS-1123 label. A site name becomes both a
// kubeconfig context and a `spec.multiSite.sites[].name`, so it must be one.
var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// Site is one workload site in the lab inventory.
type Site struct {
	// Name is the logical site name. It is the operator's ground-truth key
	// (siteQualifiedKey), the CR's site name, AND the in-pod kubeconfig context.
	Name string `json:"name"`
	// Context is the WORKSTATION's admin kubecontext for this site, used only by
	// the host-side bootstrap. It is never what the driver pod uses.
	Context string `json:"context"`
	// Endpoint is the site's API server URL as routable FROM HUB PODS (the
	// repl-net VIP), not from a workstation. A wrong-vantage URL here is the
	// classic multi-site setup mistake.
	Endpoint string `json:"endpoint"`
}

// Lab is the inventory of the physical lab: everything the harness needs to
// REACH the hub and its sites. It intentionally mirrors the shape of the
// documented MS_CONFIG schema plus the per-site admin context.
type Lab struct {
	// HubContext is the workstation's admin kubecontext for the hub.
	HubContext string `json:"hubContext"`
	// Sites is an ORDERED sequence: placement is index-based (LR-032/033), so
	// the order in this file is part of the topology, not a presentation detail.
	Sites []Site `json:"sites"`
}

// ParseLab decodes and validates a lab inventory. Everything it rejects is
// something that would otherwise surface minutes into a work item as an
// unhelpful "context does not exist" or an x509 error.
func ParseLab(data []byte) (Lab, error) {
	var l Lab
	if err := yaml.UnmarshalStrict(data, &l); err != nil {
		return Lab{}, fmt.Errorf("lab inventory is not valid YAML: %w", err)
	}
	if strings.TrimSpace(l.HubContext) == "" {
		return Lab{}, fmt.Errorf("lab inventory needs a hubContext (the workstation kubecontext for the hub)")
	}
	if len(l.Sites) == 0 {
		return Lab{}, fmt.Errorf("lab inventory needs at least one site")
	}
	seen := map[string]int{}
	for i, s := range l.Sites {
		switch {
		case strings.TrimSpace(s.Name) == "":
			return Lab{}, fmt.Errorf("lab inventory site %d has no name", i)
		case !dnsLabel.MatchString(s.Name):
			return Lab{}, fmt.Errorf("lab inventory site %d: name %q must be a DNS label "+
				"(it becomes a kubeconfig context and a spec.multiSite site name)", i, s.Name)
		case s.Name == HubContextName:
			return Lab{}, fmt.Errorf("lab inventory site %d is named %q, which is the in-pod "+
				"hub context name — it would shadow the hub and send every hub call to a site",
				i, HubContextName)
		case strings.TrimSpace(s.Context) == "":
			return Lab{}, fmt.Errorf("lab inventory site %q has no context (the workstation "+
				"admin kubecontext the bootstrap uses)", s.Name)
		case strings.TrimSpace(s.Endpoint) == "":
			return Lab{}, fmt.Errorf("lab inventory site %q has no endpoint", s.Name)
		case !strings.HasPrefix(s.Endpoint, "https://"):
			return Lab{}, fmt.Errorf("lab inventory site %q: endpoint %q must be an https:// URL "+
				"routable FROM HUB PODS", s.Name, s.Endpoint)
		}
		if prev, dup := seen[s.Name]; dup {
			return Lab{}, fmt.Errorf("lab inventory site %d: duplicate site name %q (already used by site %d)",
				i, s.Name, prev)
		}
		seen[s.Name] = i
	}
	return l, nil
}

// Select returns the first n sites of the inventory, in inventory order.
//
// Order matters and is not cosmetic: multi-site placement is index-based (shard
// K -> sites (K+j) mod S), so the sequence IS the topology (LR-033). Taking a
// prefix keeps a fixture's placement reproducible across runs.
func (l Lab) Select(n int) ([]Site, error) {
	if n < 1 {
		return nil, fmt.Errorf("a multi-site fixture needs at least one site, asked for %d", n)
	}
	if n > len(l.Sites) {
		return nil, fmt.Errorf("the fixture needs %d site(s) but the lab inventory lists only %d",
			n, len(l.Sites))
	}
	out := make([]Site, n)
	copy(out, l.Sites[:n])
	return out, nil
}

// RenderSiteCredential renders the harness's per-site credential as one
// applyable multi-document manifest. It is applied ONCE per site by the
// bootstrap, using the workstation's admin context, and then lives on.
//
// The rule set is a superset of the operator's per-site Role, and it has to be:
// RBAC forbids granting rights the grantor lacks, and the harness CREATES that
// operator Role on every site (msSetupSites). Without the superset every item
// would fail at "apply site RBAC".
func RenderSiteCredential() string {
	return `apiVersion: v1
kind: Namespace
metadata:
  name: ` + SiteNamespace + `
  labels:
    app.kubernetes.io/name: lr-e2e-harness
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ` + SiteServiceAccount + `
  namespace: ` + SiteNamespace + `
  labels:
    app.kubernetes.io/name: lr-e2e-harness
---
# CLUSTER-scoped, and it has to be: ` + "`namespaces`" + ` is a cluster-scoped
# resource, so no Role can grant create on it at any rule content, and the
# harness creates one namespace per work item on every site.
#
# THIS IS THE TEST DRIVER'S CREDENTIAL, NOT THE OPERATOR'S. ADR-012's
# "multi-site keeps the namespaced per-site Role" is a property of the operator,
# whose credential (littlered-site) is untouched and stays namespaced. Keep the
# two separate.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ` + SiteClusterRole + `
  labels:
    app.kubernetes.io/name: lr-e2e-harness
rules:
  # The reason this cannot be a Role: one fresh namespace per work item.
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["*"]
  # pods/exec is the OTHER reason the harness needs its own credential: the
  # operator reaches Redis over TCP and never execs, so its Role deliberately
  # does not grant it (design §11 M0 Finding 1).
  - apiGroups: [""]
    resources: ["pods", "pods/exec", "pods/log", "pods/status", "services",
                "configmaps", "secrets", "serviceaccounts", "events",
                "persistentvolumeclaims"]
    verbs: ["*"]
  - apiGroups: ["apps"]
    resources: ["statefulsets", "deployments", "replicasets"]
    verbs: ["*"]
  - apiGroups: ["policy"]
    resources: ["poddisruptionbudgets"]
    verbs: ["*"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["*"]
  - apiGroups: ["events.k8s.io"]
    resources: ["events"]
    verbs: ["*"]
  # Not for the harness's own use: RBAC forbids granting rights the grantor
  # lacks, and the harness creates the OPERATOR's per-site Role/RoleBinding
  # (msSetupSites) on every site.
  - apiGroups: ["rbac.authorization.k8s.io"]
    resources: ["roles", "rolebindings"]
    verbs: ["*"]
  # Read-only, and only because the shipped harness waits for a site's nodes to
  # come back Ready after a whole-site kill (msWaitSiteReady / ms-heal). The
  # OPERATOR has no node RBAC and must not gain any — site-loss safety is
  # Domain-M quorum arithmetic, not a Kubernetes node read (ADR-012 D1).
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ` + SiteClusterRole + `
  labels:
    app.kubernetes.io/name: lr-e2e-harness
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ` + SiteClusterRole + `
subjects:
  - kind: ServiceAccount
    name: ` + SiteServiceAccount + `
    namespace: ` + SiteNamespace + `
---
# A long-lived (non-expiring) token: the driver pod reads it from a hub Secret
# assembled at bootstrap, so a projected, auto-rotating token is not available
# to it. Scoped to a dedicated test cluster by construction.
apiVersion: v1
kind: Secret
metadata:
  name: ` + SiteTokenSecret + `
  namespace: ` + SiteNamespace + `
  annotations:
    kubernetes.io/service-account.name: ` + SiteServiceAccount + `
type: kubernetes.io/service-account-token
`
}

// Cluster is one entry of the in-pod kubeconfig: a cluster, a user and the
// context binding them, all named alike.
type Cluster struct {
	// Name is the context name. For a site it is the logical site name; for the
	// hub it is HubContextName.
	Name string
	// Server is the API server URL.
	Server string
	// CAPEM is the certificate authority in PEM form, exactly as it comes off a
	// mounted Secret / the in-cluster serviceaccount directory. Render base64s
	// it, because that is what a kubeconfig wants.
	CAPEM string
	// Token is the bearer token.
	Token string
}

// kubeconfig mirrors the subset of the kubeconfig schema the harness needs.
type kubeconfig struct {
	APIVersion     string              `json:"apiVersion"`
	Kind           string              `json:"kind"`
	CurrentContext string              `json:"current-context"`
	Clusters       []kubeconfigCluster `json:"clusters"`
	Users          []kubeconfigUser    `json:"users"`
	Contexts       []kubeconfigContext `json:"contexts"`
}

type kubeconfigCluster struct {
	Name    string `json:"name"`
	Cluster struct {
		Server string `json:"server"`
		CAData string `json:"certificate-authority-data"`
	} `json:"cluster"`
}

type kubeconfigUser struct {
	Name string `json:"name"`
	User struct {
		Token string `json:"token"`
	} `json:"user"`
}

type kubeconfigContext struct {
	Name    string `json:"name"`
	Context struct {
		Cluster string `json:"cluster"`
		User    string `json:"user"`
	} `json:"context"`
}

// RenderKubeconfig assembles the multi-context kubeconfig the driver pod drives
// the lab through: one context per site plus the hub, current-context on the
// hub. This is M0's proven mechanism (SA token + CA + hub-routable endpoint),
// with its validation written down.
//
// A real FILE with real CONTEXT NAMES is not optional: the multi-site e2e
// helpers drive sites by kubecontext (`kctl` prepends --context), so there is
// no in-cluster-config shortcut for anything but the hub.
func RenderKubeconfig(hub Cluster, sites []Cluster) (string, error) {
	if len(sites) == 0 {
		return "", fmt.Errorf("a multi-site kubeconfig needs at least one site")
	}
	if err := validateCluster("hub", hub); err != nil {
		return "", err
	}

	// Validate the sites BEFORE emitting anything: a site whose name equals the
	// hub's would otherwise be appended as a second cluster under one context
	// name, and every ambient hub call would resolve to whichever won.
	seen := map[string]bool{hub.Name: true}
	for _, c := range sites {
		if c.Name == hub.Name || c.Name == HubContextName {
			return "", fmt.Errorf("site %q is named like the hub context — it would shadow "+
				"the hub, and every ambient call would silently go to a site", c.Name)
		}
		if seen[c.Name] {
			return "", fmt.Errorf("duplicate context name %q: two sites cannot share one "+
				"kubeconfig context", c.Name)
		}
		seen[c.Name] = true
		if err := validateCluster("site "+c.Name, c); err != nil {
			return "", err
		}
	}

	kc := kubeconfig{APIVersion: "v1", Kind: "Config", CurrentContext: hub.Name}
	for _, c := range append([]Cluster{hub}, sites...) {
		cl := kubeconfigCluster{Name: c.Name}
		cl.Cluster.Server = c.Server
		cl.Cluster.CAData = base64.StdEncoding.EncodeToString([]byte(c.CAPEM))
		kc.Clusters = append(kc.Clusters, cl)

		u := kubeconfigUser{Name: c.Name}
		u.User.Token = c.Token
		kc.Users = append(kc.Users, u)

		cx := kubeconfigContext{Name: c.Name}
		cx.Context.Cluster = c.Name
		cx.Context.User = c.Name
		kc.Contexts = append(kc.Contexts, cx)
	}

	out, err := yaml.Marshal(kc)
	if err != nil {
		return "", fmt.Errorf("rendering the kubeconfig: %w", err)
	}
	return string(out), nil
}

func validateCluster(what string, c Cluster) error {
	switch {
	case strings.TrimSpace(c.Name) == "":
		return fmt.Errorf("%s: no context name", what)
	case strings.TrimSpace(c.Server) == "":
		return fmt.Errorf("%s: no server URL", what)
	case !strings.HasPrefix(c.Server, "https://"):
		return fmt.Errorf("%s: server %q must be an https:// URL", what, c.Server)
	case strings.TrimSpace(c.CAPEM) == "":
		return fmt.Errorf("%s: no certificate authority — an unverified API server is never "+
			"the right answer to an x509 error", what)
	case strings.TrimSpace(c.Token) == "":
		return fmt.Errorf("%s: no bearer token", what)
	}
	return nil
}

// msConfig mirrors the documented MS_CONFIG schema
// (docs/multisite-e2e-config.sample.yaml; the suite's own type lives behind the
// `e2e` build tag and cannot be imported). The omitempty on the fault-arming
// fields is deliberate — see RenderMSConfig.
type msConfig struct {
	HubContext        string         `json:"hubContext"`
	OperatorNamespace string         `json:"operatorNamespace"`
	Sites             []msConfigSite `json:"sites"`
}

type msConfigSite struct {
	Name     string `json:"name"`
	Context  string `json:"context"`
	Endpoint string `json:"endpoint"`
}

// RenderMSConfig renders the multi-site config for ONE work item.
//
// Per item, not per array, and that is forced rather than chosen: the operator
// reads its site kubeconfig Secrets from its OWN namespace (site_registry.go),
// and every work item helm-installs its own operator into its own namespace
// (M2, design §3). So `operatorNamespace` cannot be a launch-time constant —
// a single mounted MS_CONFIG would point every item at one operator's namespace
// and the site Secrets would land where no operator looks.
//
// What is NOT emitted matters as much as what is. `vms` arms the Scenario A
// whole-site hypervisor kill and `hubPodCIDR` arms the Scenario B data-plane
// cut; M3 injects no faults, so neither is ever rendered.
func RenderMSConfig(sites []Site, operatorNamespace string) (string, error) {
	if strings.TrimSpace(operatorNamespace) == "" {
		return "", fmt.Errorf("MS_CONFIG needs the item's operatorNamespace: left empty it " +
			"defaults to littlered-system, and the operator would never find its site Secrets")
	}
	if len(sites) == 0 {
		return "", fmt.Errorf("MS_CONFIG needs at least one site")
	}

	cfg := msConfig{HubContext: HubContextName, OperatorNamespace: operatorNamespace}
	for _, s := range sites {
		// context = the SITE NAME, never the lab inventory's admin context: in
		// the pod the contexts are the ones RenderKubeconfig created.
		cfg.Sites = append(cfg.Sites, msConfigSite{Name: s.Name, Context: s.Name, Endpoint: s.Endpoint})
	}

	out, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("rendering MS_CONFIG: %w", err)
	}
	return string(out), nil
}
