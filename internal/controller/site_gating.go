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

// site_gating.go holds the PURE, I/O-free per-site reconcile-gating decision for
// multi-site cluster mode (docs/MULTI_SITE_DESIGN.md §8: "allPodsReady-style gates and
// wipe recovery are evaluated per site, so one unreachable site does not blind repair of
// the rest").
//
// The single-site allPodsReady gate compares one ready count to one expected count. The
// multi-site analogue must NOT treat an unreachable site as "not ready" — that would
// wedge the whole instance whenever any site is briefly unreachable, exactly the
// degradation the design forbids (a site the hub cannot reach is unmanaged, not broken).
// So readiness is computed over the REACHABLE sites only; an unreachable site is skipped
// (it contributes no pods to the gather and is surfaced unreachable in status).

// siteReadiness is one site's contribution to the multi-site readiness gate.
type siteReadiness struct {
	// Name is the site name.
	Name string
	// Reachable reports whether the site's Kubernetes API answered this pass.
	Reachable bool
	// Ready is the count of this instance's ready cluster pods observed in the site.
	Ready int32
	// Total is the count of cluster pods this instance expects to host in the site
	// (from the placement SSOT).
	Total int32
}

// multiSiteReady reports whether the operator may proceed past the readiness gate to
// gather/repair the stretched cluster, and whether ANY site was reachable at all.
//
// Semantics (design §8): proceed when there is at least one reachable site AND every
// reachable site has all of its expected pods ready. An unreachable site is excluded
// from the decision — repair of the reachable rest is never blocked by it. If no site is
// reachable, the operator cannot act (anyReachable=false): there is nothing to gather or
// repair, so the caller waits. A reachable site expecting zero pods (should not happen
// under a valid placement) does not by itself block readiness.
func multiSiteReady(sites []siteReadiness) (ready, anyReachable bool) {
	for _, s := range sites {
		if !s.Reachable {
			continue
		}
		anyReachable = true
		if s.Ready < s.Total {
			return false, true
		}
	}
	if !anyReachable {
		return false, false
	}
	return true, true
}
