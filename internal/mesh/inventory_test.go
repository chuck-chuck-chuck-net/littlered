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

package mesh

import (
	"strings"
	"testing"
)

const (
	siteHub = "scm-cp"
	siteOne = "scm-s1"
)

// The mesh inventory is a SHARED format with the sibling project slaptain
// (../slaptain, docs/MULTI-SITE.md): one `sites:` list at the top level, one
// per-project section for that project's own knobs, and unknown keys ignored so
// one file can carry both projects. These tests pin the three properties that
// make it shared rather than merely similar:
//
//  1. a slaptain inventory parses here unchanged (no `roles`, extra keys);
//  2. littlered's own fact — which site is the hub — is a ROLE on a site rather
//     than a top-level field, so the list stays the common part;
//  3. the mistakes that are expensive and silent are refused at parse time.

func TestParseMeshConfigSharedFormat(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string // substring; empty means it must parse
		check   func(t *testing.T, c *Inventory)
	}{
		{
			name: "the hub is a site with a role, not a top-level field",
			yaml: `
sites:
  - name: scm-cp
    context: scm-cp
    roles: [hub]
    podCIDR: "10.233.224.0/19"
  - name: scm-s1
    context: scm-s1
    endpoint: https://10.2.1.10:6443
littlered:
  operatorNamespace: littlered-system
`,
			check: func(t *testing.T, c *Inventory) {
				if got := c.HubContext(); got != siteHub {
					t.Errorf("HubContext() = %q, want %q", got, siteHub)
				}
				if got := c.HubPodCIDR(); got != "10.233.224.0/19" {
					t.Errorf("HubPodCIDR() = %q, want the hub site's podCIDR", got)
				}
				if got := c.OperatorNamespace(); got != "littlered-system" {
					t.Errorf("OperatorNamespace() = %q", got)
				}
				ws := c.WorkloadSites()
				if len(ws) != 1 || ws[0].Name != siteOne {
					t.Errorf("WorkloadSites() = %+v, want just scm-s1 (the hub-only site is not a workload site)", ws)
				}
			},
		},
		{
			// The compatibility property that matters: slaptain declares no roles
			// and carries keys littlered knows nothing about. It must parse, and
			// every site must default to being a workload site.
			name: "a slaptain inventory parses unchanged — no roles, unknown keys ignored",
			yaml: `
slaptain:
  registry: registry.k8s.example
  operatorNamespace: slaptain-system
sites:
  - name: site-a
    serverIDIndex: 0
    context: site-a
    endpoint: https://192.0.2.10:6443
    nodeAccessIP: 198.51.100.10
  - name: site-b
    serverIDIndex: 1
    context: site-b
    endpoint: https://192.0.2.20:6443
`,
			check: func(t *testing.T, c *Inventory) {
				if len(c.WorkloadSites()) != 2 {
					t.Errorf("WorkloadSites() = %d, want 2: a site with no roles is a workload site", len(c.WorkloadSites()))
				}
				// No hub declared: littlered cannot run on this file, but PARSING
				// must succeed — the refusal belongs to littlered's own validation,
				// not to the shared format.
				if err := c.ValidateForLittleRed(); err == nil {
					t.Error("ValidateForLittleRed() = nil, want a refusal naming the missing hub role")
				}
			},
		},
		{
			// littlered's own model allows a hub-local workload site (a site with no
			// kubeconfigSecretRef is hub-local — hubSiteClient). A single-valued
			// `role:` could not express it, which is why roles is a list.
			name: "a site may be both hub and workload",
			yaml: `
sites:
  - name: scm-cp
    context: scm-cp
    roles: [hub, workload]
  - name: scm-s1
    context: scm-s1
    endpoint: https://10.2.1.10:6443
`,
			check: func(t *testing.T, c *Inventory) {
				if c.HubContext() != siteHub {
					t.Errorf("HubContext() = %q", c.HubContext())
				}
				names := make([]string, 0, len(c.WorkloadSites()))
				for _, s := range c.WorkloadSites() {
					names = append(names, s.Name)
				}
				if strings.Join(names, ",") != "scm-cp,scm-s1" {
					t.Errorf("WorkloadSites() = %v, want both in spec order", names)
				}
			},
		},
		{
			// A workload site that is NOT the hub is reached over its API server, so
			// the hub needs a kubeconfig for it — which needs the endpoint. A
			// hub-local one needs none, which is exactly littlered's own
			// kubeconfigSecretRef == nil rule falling out of the format.
			name: "a remote workload site without an endpoint is refused",
			yaml: `
sites:
  - name: scm-cp
    context: scm-cp
    roles: [hub]
  - name: scm-s1
    context: scm-s1
`,
			wantErr: "endpoint",
		},
		{
			name: "two hubs are refused — one operator, one hub",
			yaml: `
sites:
  - {name: a, context: a, roles: [hub]}
  - {name: b, context: b, roles: [hub]}
  - {name: c, context: c, endpoint: https://10.0.0.1:6443}
`,
			wantErr: "exactly one",
		},
		{
			name: "duplicate site names are refused — the name is the ground-truth key",
			yaml: `
sites:
  - {name: scm-cp, context: scm-cp, roles: [hub]}
  - {name: dup, context: c1, endpoint: https://10.0.0.1:6443}
  - {name: dup, context: c2, endpoint: https://10.0.0.2:6443}
`,
			wantErr: "duplicate",
		},
		{
			name: "an unknown role is refused rather than silently ignored",
			yaml: `
sites:
  - {name: scm-cp, context: scm-cp, roles: [hub]}
  - {name: scm-s1, context: scm-s1, roles: [spoke], endpoint: https://10.0.0.1:6443}
`,
			wantErr: "spoke",
		},
		{
			// The format changed; an old file must say so rather than parse to an
			// empty site list and Skip, which reads exactly like "no config".
			name: "the superseded top-level hubContext is named, not ignored",
			yaml: `
hubContext: scm-cp
operatorNamespace: littlered-system
sites:
  - {name: scm-s1, context: scm-s1, endpoint: https://10.0.0.1:6443}
`,
			wantErr: "roles: [hub]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Parse([]byte(tc.yaml))
			if err == nil && c != nil && tc.wantErr != "" {
				err = c.ValidateForLittleRed()
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parse/validate = nil error, want one containing %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %q, want it to contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse = %v, want success", err)
			}
			tc.check(t, c)
		})
	}
}
