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

// repair_guards.go holds the PURE, I/O-free guards that make the generic cluster repair loop
// (repairCluster) INCAPABLE of the two unsafe acts across a site that is merely unreachable
// (and therefore possibly alive) in multi-site mode:
//
//   - an unguarded quorum-loss CLUSTER FAILOVER TAKEOVER (repair Step-0, and the Step-1
//     stuck-orphan force-promote) — both cooldown-free and reachability-blind; and
//   - a CLUSTER FORGET of a node whose SITE the hub cannot reach.
//
// In multi-site the ONLY authority for a quorum-loss takeover is the fence-gated
// recoverSiteLoss (ADR-012): it runs ahead of repair, waits out the cooldown, and refuses on a
// drift split where the lost side could still hold a serving majority. Repair must never race
// ahead of that gate. Ghost FORGET is likewise restricted to the case where every hosting site
// is observable, so a gossip-known node on an unreachable site — "unknown, not gone"
// (pillar 3.4) — is never forgotten.

import (
	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// repairMayForceTakeover reports whether the generic repair loop is allowed to issue a
// CLUSTER FAILOVER TAKEOVER for this instance. It is TRUE only in single-site mode: there the
// repair loop's Step-0 quorum recovery and Step-1 stuck-orphan promotion are the correct
// (and only) owner of quorum-loss recovery. In MULTI-SITE it is FALSE unconditionally — the
// fence-gated recoverSiteLoss (ADR-012) is the sole takeover authority, so repair can never
// fire a cooldown-free, reachability-blind takeover across a site that may be alive-but-
// unreachable (a live split-brain). recoverSiteLoss runs before repair and, whenever a site
// loss is in play, either acts (blocking repair) or defers to native failover.
func repairMayForceTakeover(lr *littleredv1alpha1.LittleRed) bool {
	return !isMultiSite(lr)
}

// ghostForgetPlan returns the subset of gossip-known ghost NodeIDs that are SAFE to
// CLUSTER FORGET. A ghost is a NodeID seen in the mesh with no backing pod in the gathered
// view. In single-site (and multi-site with every hosting site observable) that unambiguously
// means the pod is gone, so all candidates are forgettable (routine ghost cleanup for pod
// churn). When a hosting site is unobservable (forgetSafe=false) we cannot attribute a
// no-backing-pod ghost to a confirmed-gone pod versus a node on the unreachable site — the two
// are indistinguishable by NodeID alone — so NONE are forgotten this pass: an unreachable
// site's nodes are "unknown, not gone" (pillar 3.4 / ADR-012 "forgotten only when its own
// site's reachability supports it, never merely because the hub cannot reach it"). FORGET
// trails idempotently until the site is observable again.
func ghostForgetPlan(candidates []string, forgetSafe bool) []string {
	if !forgetSafe {
		return nil
	}
	return candidates
}
