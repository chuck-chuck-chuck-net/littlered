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

// Package mesh reads the lab/mesh inventory: the one file describing which
// Kubernetes clusters make up a multi-site deployment.
//
// It lives outside both lrctl and the e2e suite because BOTH read it, and a
// second copy of this parser is exactly the shape LR-045 is about. The format
// itself is shared with the sibling project slaptain — see Inventory.
package mesh

import (
	"fmt"
	"strings"

	"sigs.k8s.io/yaml"
)

// Mesh inventory schema — SHARED with the sibling project slaptain (../slaptain,
// docs/MULTI-SITE.md), deliberately, so one file can describe a lab for both and nobody has
// to hold two vocabularies for one set of clusters. Three rules make it shared rather than
// merely similar:
//
//  1. the site inventory is the COMMON part, at the top level under `sites:`;
//
//  2. each project's own knobs live under its own key (`littlered:` / `slaptain:`);
//
//  3. unknown keys are IGNORED, so one file carries both sections and either project's
//     extra per-site fields (slaptain's `serverIDIndex`, `nodeAccessIP`, …).
//
//     sites:
//     - name: scm-cp
//     context: scm-cp                      # kubecontext; OPTIONAL, defaults to name
//     roles: [hub]                         # littlered: the operator and the CR live here
//     podCIDR: "10.233.224.0/19"           # this site's pod network; Scenario B denies the hub's
//     - name: scm-s1
//     context: scm-s1
//     endpoint: https://10.2.1.10:6443     # API URL routable FROM THE OTHER SITES' PODS
//     vms:                                 # optional; the whole-site kill inventory
//     - {name: scm-s1-node1, uri: qemu+ssh://user@host/system}
//     littlered:
//     operatorNamespace: littlered-system    # optional (default littlered-system)
//
// ROLES. `roles` is a LIST and defaults to [workload] when absent — which is what makes an
// existing slaptain inventory valid here unchanged, since slaptain is peer-to-peer and every
// one of its sites is a workload site. littlered adds exactly one role, `hub`: the cluster
// running the operator and holding the CR. It is a list rather than a scalar because a site
// may be BOTH — littlered's own model treats a site with no kubeconfigSecretRef as hub-local
// (hubSiteClient), so "the hub also carries shards" is a supported topology and a single
// `role:` could not express it. `hub` is the project's established word (hubClient,
// IsHubLocal, "hub-and-spokes" throughout MULTI_SITE_DESIGN.md); "control-plane" was rejected
// because it already means something else in Kubernetes.
//
// The file is named by E2E_CONFIG (slaptain's variable) or MS_CONFIG; conventionally
// mesh.yaml at the repo root. See mesh.yaml.sample and docs/E2E_TESTING.md ("Multi-site").
//
// Env-var fallback, unchanged and untouched by the schema change: MS_HUB_CONTEXT,
// MS_SITE_CONTEXTS ("name=ctx,..."), MS_SITE_ENDPOINTS ("name=url,..."), MS_OPERATOR_NS,
// MS_HUB_POD_CIDR, and the legacy MS_VM_INVENTORY file ("<site> <vm> <uri>" lines).
type Inventory struct {
	Sites     []Site  `json:"sites"`
	LittleRed Project `json:"littlered,omitempty"`

	// HubContextLegacy catches the superseded top-level field so an old inventory is
	// NAMED rather than parsing to something that merely looks unconfigured. An
	// unrecognised schema that yields an empty site list reads exactly like "no config
	// present", which Skips — the silent outcome this field exists to prevent.
	HubContextLegacy string `json:"hubContext,omitempty"`
}

// Project is one project's own section. Only littlered's is read here; slaptain's
// rides along in the same file and is ignored.
type Project struct {
	OperatorNamespace string `json:"operatorNamespace,omitempty"`
}

type Site struct {
	Name     string   `json:"name"`
	Context  string   `json:"context,omitempty"`
	Roles    []string `json:"roles,omitempty"`
	Endpoint string   `json:"endpoint,omitempty"`
	// KubeconfigSecret is the name of the Secret in the OPERATOR's namespace on the hub
	// holding this site's kubeconfig — what `lrctl multisite setup-site` creates, and what
	// spec.multiSite.sites[].kubeconfigSecretRef.name must say. Defaults to "site-<name>",
	// which is setup-site's own default, so an inventory only states it when something else
	// owns the naming.
	KubeconfigSecret string `json:"kubeconfigSecret,omitempty"`
	PodCIDR          string `json:"podCIDR,omitempty"`
	VMs              []VM   `json:"vms,omitempty"`
}

type VM struct {
	Name string `json:"name"`
	URI  string `json:"uri"`
}

// The two roles the shared format defines. Absent ⇒ RoleWorkload.
const (
	RoleHub      = "hub"
	RoleWorkload = "workload"
)

// KubeContext is the site's kubecontext, defaulting to its name — slaptain's rule, and the
// reason its lab.yaml marks `context` optional.
func (s Site) KubeContext() string {
	if c := strings.TrimSpace(s.Context); c != "" {
		return c
	}
	return strings.TrimSpace(s.Name)
}

// KubeconfigSecretName is the hub Secret holding this site's kubeconfig, defaulting to
// setup-site's own "site-<name>" convention so the two cannot drift by omission.
func (s Site) KubeconfigSecretName() string {
	if n := strings.TrimSpace(s.KubeconfigSecret); n != "" {
		return n
	}
	return "site-" + strings.TrimSpace(s.Name)
}

func (s Site) HasRole(want string) bool {
	if len(s.Roles) == 0 {
		return want == RoleWorkload
	}
	for _, r := range s.Roles {
		if strings.EqualFold(strings.TrimSpace(r), want) {
			return true
		}
	}
	return false
}

// HubContext is the kubecontext of the site carrying the hub role.
func (c *Inventory) HubContext() string {
	for _, s := range c.Sites {
		if s.HasRole(RoleHub) {
			return s.KubeContext()
		}
	}
	return ""
}

// HubPodCIDR is the hub site's pod network, which Scenario B denies to cut the hub off from
// a site without cutting the sites off from each other. It is a property of that SITE rather
// than a top-level fact, which is also what lets a future scenario read any site's.
func (c *Inventory) HubPodCIDR() string {
	for _, s := range c.Sites {
		if s.HasRole(RoleHub) {
			return strings.TrimSpace(s.PodCIDR)
		}
	}
	return ""
}

func (c *Inventory) OperatorNamespace() string {
	return strings.TrimSpace(c.LittleRed.OperatorNamespace)
}

// WorkloadSites are the sites that carry pods, in spec order — which is load-bearing:
// spec.multiSite.sites is listType=atomic and a site's POSITION is its placement index
// (LR-032/LR-033), so this order must be the order the CR is written with.
func (c *Inventory) WorkloadSites() []Site {
	var out []Site
	for _, s := range c.Sites {
		if s.HasRole(RoleWorkload) {
			out = append(out, s)
		}
	}
	return out
}

// Parse parses the shared inventory. It validates only what the FORMAT requires;
// what littlered additionally needs is ValidateForLittleRed, so a slaptain inventory parses
// here cleanly and is refused only when this project tries to use it.
func Parse(data []byte) (*Inventory, error) {
	var c Inventory
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parsing the mesh inventory as YAML: %w", err)
	}
	if strings.TrimSpace(c.HubContextLegacy) != "" {
		return nil, fmt.Errorf("this inventory uses the superseded top-level %q field. "+
			"The hub is now a SITE carrying `roles: [hub]`, so the `sites:` list is shared "+
			"verbatim with the sibling project slaptain. Move it: add an entry "+
			"{name: %s, context: %s, roles: [hub]} to `sites:`, move `hubPodCIDR` onto that "+
			"entry as `podCIDR`, and move `operatorNamespace` under a `littlered:` key. "+
			"See mesh.yaml.sample",
			"hubContext", c.HubContextLegacy, c.HubContextLegacy)
	}
	seen := map[string]bool{}
	for _, s := range c.Sites {
		name := strings.TrimSpace(s.Name)
		if name == "" {
			return nil, fmt.Errorf("every site needs a name")
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate site name %q: the name is the ground-truth key "+
				"(it is what a site-qualified pod key is built from), so it must be unique", name)
		}
		seen[name] = true
		for _, r := range s.Roles {
			r = strings.TrimSpace(r)
			if !strings.EqualFold(r, RoleHub) && !strings.EqualFold(r, RoleWorkload) {
				return nil, fmt.Errorf("site %q declares unknown role %q; known roles are %q and %q",
					name, r, RoleHub, RoleWorkload)
			}
		}
	}
	return &c, nil
}

// ValidateForLittleRed refuses an inventory this project cannot run on. It is separate from
// parsing on purpose: the format is shared, and a perfectly valid slaptain inventory declares
// no hub — that is not a malformed file, it is a file for the other project.
func (c *Inventory) ValidateForLittleRed() error {
	hubs := 0
	for _, s := range c.Sites {
		if s.HasRole(RoleHub) {
			hubs++
		}
	}
	if hubs != 1 {
		return fmt.Errorf("littlered needs exactly one site with `roles: [hub]` (the cluster "+
			"running the operator and holding the CR); this inventory declares %d", hubs)
	}
	ws := c.WorkloadSites()
	if len(ws) == 0 {
		return fmt.Errorf("no workload sites: at least one site must carry pods " +
			"(a site with no `roles` is a workload site)")
	}
	hubCtx := c.HubContext()
	for _, s := range ws {
		// A hub-local workload site is reached through the manager's own client, so it
		// needs no kubeconfig and therefore no endpoint — littlered's own
		// "kubeconfigSecretRef == nil ⇒ hub-local" rule, falling out of the inventory.
		if s.HasRole(RoleHub) && s.KubeContext() == hubCtx {
			continue
		}
		if strings.TrimSpace(s.Endpoint) == "" {
			return fmt.Errorf("site %q carries pods and is not the hub, so the hub needs a "+
				"kubeconfig for it: set `endpoint` to its API URL as routable FROM HUB PODS "+
				"(on dual-homed nodes this is NOT the address in your kubeconfig)", s.Name)
		}
	}
	return nil
}
