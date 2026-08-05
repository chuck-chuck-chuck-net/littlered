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

// site_witness.go holds the PURE, I/O-free per-site Kubernetes WITNESS classifier for
// the multi-site site-loss takeover (ADR-012 condition 3). It answers one question about
// each affected site from the hub's Kubernetes view alone (never a Redis dial): is the
// site Confirmed dead, Unobservable, or contradicting the loss (alive)?
//
// The load-bearing distinction (ADR-008 / LR-017, in Kubernetes clothing):
//   - kubelet readiness is authoritative and blackhole-proof — a redis pod not-Ready
//     while its NODE is Ready is dead per the kubelet that actually runs it.
//   - the NODE's Ready condition being NotReady/Unknown is set by the node-lifecycle
//     controller inferring from MISSED HEARTBEATS — a remote inference, not a local
//     verdict. A pod not-Ready on such a node may still be running and serving; its
//     not-Ready flag is untrustworthy. That case DOWNGRADES the witness to Unobservable
//     grade, which forces the fence (ADR-012 condition 4). It is a distinct branch, not
//     folded into ConfirmedDown.

// siteWitnessGrade is the hub's Kubernetes verdict on one site (ADR-012 condition 3).
type siteWitnessGrade int

const (
	// witnessConfirmedDown: the site's Kubernetes API answered, and every redis pod is
	// either GONE or not-Ready while its Node is Ready (kubelet-attested death). No fence
	// needed — this alone authorizes a takeover.
	witnessConfirmedDown siteWitnessGrade = iota
	// witnessUnobservable: the site's Kubernetes API did not answer (true site death and a
	// mere hub-management-path cut look identical here), OR at least one redis pod is
	// not-Ready on a Node that is itself NotReady/Unknown (node-controller inference — the
	// LR-017 trap). Ambiguous ⇒ a takeover here requires the fence (condition 4).
	witnessUnobservable
	// witnessContradiction: the site's Kubernetes API answered and at least one redis pod
	// is Ready. The site is alive; the hub's data path to it is partitioned. NEVER take
	// over — this is the descoped link-cut (ADR-012 condition 3, "Never take over here").
	witnessContradiction
)

func (g siteWitnessGrade) String() string {
	switch g {
	case witnessConfirmedDown:
		return "ConfirmedDown"
	case witnessUnobservable:
		return "Unobservable"
	case witnessContradiction:
		return "Contradiction"
	default:
		return "Unknown"
	}
}

// witnessPod is one EXPECTED redis pod of a site, resolved to the Kubernetes facts the
// classifier needs — all from the site's Kubernetes API, never a Redis dial.
type witnessPod struct {
	// present is whether the pod object exists in the site's API this pass. A gone pod is
	// confirmed-down evidence (nothing is running).
	present bool
	// ready is the redis container's kubelet readiness (meaningful only when present).
	ready bool
	// nodeReady is the pod's Node's Ready condition == True (meaningful only when present
	// and not ready — it decides kubelet-attested vs node-controller-inferred death).
	nodeReady bool
	// nodeKnown is whether the pod's Node Ready condition could be determined at all. When
	// false (node missing, or the node read failed), we cannot attest kubelet-freshness and
	// must treat the not-Ready as node-controller-grade (downgrade → Unobservable). This is
	// the safe default: an unknown node can only make the witness MORE cautious, never
	// authorize an unfenced takeover.
	nodeKnown bool
}

// classifySiteWitness computes a site's witness grade (ADR-012 condition 3) from whether
// its Kubernetes API answered and the per-pod facts. It is total and pure.
//
// Aggregation precedence within a site:
//  1. API unreachable ⇒ Unobservable (nothing else can be trusted).
//  2. Any redis pod Ready ⇒ Contradiction (the site is provably alive — this dominates
//     everything else, because one serving pod refutes a whole-site loss).
//  3. Else any not-Ready pod whose node is NOT Ready-attested (NotReady/Unknown/unknown)
//     ⇒ Unobservable (node-controller inference — the downgrade branch).
//  4. Else (every pod gone, or not-Ready with its Node Ready) ⇒ ConfirmedDown.
func classifySiteWitness(apiReachable bool, pods []witnessPod) siteWitnessGrade {
	if !apiReachable {
		return witnessUnobservable
	}
	anyReady := false
	anyNodeControllerInferred := false
	for _, p := range pods {
		if !p.present {
			continue // gone: confirmed-down evidence, nothing running
		}
		if p.ready {
			anyReady = true
			continue
		}
		// present and not-Ready: whose verdict is it?
		if p.nodeKnown && p.nodeReady {
			continue // kubelet-attested death (node alive, pod not ready) — ConfirmedDown grade
		}
		anyNodeControllerInferred = true // node NotReady/Unknown ⇒ inference we cannot trust
	}
	switch {
	case anyReady:
		return witnessContradiction
	case anyNodeControllerInferred:
		return witnessUnobservable
	default:
		return witnessConfirmedDown
	}
}
