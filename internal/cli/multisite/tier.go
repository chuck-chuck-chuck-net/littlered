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

package multisite

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Tier is the transport lrctl uses to obtain multi-site ground truth. The three
// tiers correspond to the three privilege levels a caller may have (see
// docs/LRCTL.md "Multi-Site Clusters"):
//
//	TierStatus (tier 1) — read-only hub access. Render the cross-site view the
//	  operator already published to status.multiSite. Needs only `get littlered`.
//	TierLive   (tier 2) — exec/run on the hub. Run the operator's own `--gather`
//	  as a throwaway Job; the operator (which already has site access) gathers and
//	  returns JSON over the control path. Needs create-jobs + read pod logs.
//	TierDirect (tier 3) — direct site access. Gather each site through the user's
//	  own per-site kube-contexts, bypassing the operator entirely.
type Tier int

const (
	// TierLive is tier 2: live gather via a throwaway hub Job. The default for
	// the live read verbs (verify/inspect/debug-dump).
	TierLive Tier = iota
	// TierStatus is tier 1: render from the operator's published status.
	TierStatus
	// TierDirect is tier 3: direct per-site kube-context gather (opt-in fallback).
	TierDirect
)

// Label is the human-readable "tier in use" string surfaced in command output so
// the reader always knows how the data was obtained.
func (t Tier) Label() string {
	switch t {
	case TierLive:
		return "gathered live via hub Job"
	case TierStatus:
		return "from operator status"
	case TierDirect:
		return "gathered via direct site contexts"
	default:
		return "unknown"
	}
}

// TierFlags carries the caller's tier-selecting flags.
type TierFlags struct {
	// FromStatus is --from-status: render tier 1 from published operator status.
	FromStatus bool
	// Direct is --direct: force tier 3 direct per-site kube-context gather.
	Direct bool
	// SiteContextSet is true when --site-context was given, which also implies
	// tier 3 (the user is telling us the per-site contexts to use).
	SiteContextSet bool
}

// SelectTier resolves the transport for a read verb from its flags.
//
// status is always tier 1 — it has no live-gather form, it renders what the
// operator published. For the live verbs the default is tier 2 (live via hub
// Job); --from-status selects tier 1; --direct or --site-context selects tier 3.
// --from-status combined with a tier-3 flag is a contradiction and errors.
func SelectTier(verb string, f TierFlags) (Tier, error) {
	if f.FromStatus && (f.Direct || f.SiteContextSet) {
		return 0, fmt.Errorf("--from-status (tier 1) cannot be combined with --direct/--site-context (tier 3)")
	}
	if verb == "status" {
		return TierStatus, nil
	}
	if f.FromStatus {
		return TierStatus, nil
	}
	if f.Direct || f.SiteContextSet {
		return TierDirect, nil
	}
	return TierLive, nil
}

// GatherJobParams are the inputs to BuildGatherJob (tier 2).
type GatherJobParams struct {
	// OperatorNamespace is where the Job runs (the operator's own namespace) —
	// it must be able to read the site kubeconfig Secrets and dial the sites.
	OperatorNamespace string
	// ServiceAccountName is the operator's SA, so the Job inherits the exact
	// RBAC + Secret access the operator uses to gather.
	ServiceAccountName string
	// Image is the operator's image (already cached on hub nodes).
	Image string
	// InstanceNamespace / InstanceName identify the CR to gather.
	InstanceNamespace string
	InstanceName      string
	// TTLSeconds sets ttlSecondsAfterFinished so the API server GCs the Job even
	// if lrctl dies before deleting it (no reliance on client-side cleanup).
	TTLSeconds int32
	// NameSuffix makes the Job name unique per invocation (caller supplies a
	// timestamp/random token); keeping it a parameter keeps the builder pure.
	NameSuffix string
}

// BuildGatherJob builds the throwaway tier-2 gather Job. It runs the operator's
// own binary in `--gather` mode under the operator's SA and image, prints the
// site-attributed ground truth as JSON to its logs, and is server-side
// GC'd via ttlSecondsAfterFinished.
func BuildGatherJob(p GatherJobParams) *batchv1.Job {
	backoff := int32(0)
	ttl := p.TTLSeconds
	labels := map[string]string{
		"app.kubernetes.io/name":       "lrctl-gather",
		"app.kubernetes.io/managed-by": "lrctl",
		"app.kubernetes.io/instance":   p.InstanceName,
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lrctl-gather-" + p.NameSuffix,
			Namespace: p.OperatorNamespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &ttl,
			BackoffLimit:            &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: p.ServiceAccountName,
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "gather",
						Image: p.Image,
						Command: []string{
							"/manager", "--gather",
							p.InstanceNamespace + "/" + p.InstanceName,
							"-o", "json",
						},
					}},
				},
			},
		},
	}
}

// OperatorInfo is the subset of the operator Deployment the tier-2 Job needs.
type OperatorInfo struct {
	ServiceAccountName string
	Image              string
}

// ExtractOperatorInfo reads the SA and manager-container image off the operator
// Deployment. It prefers the container named "manager" (the operator binary),
// falling back to the first container. Errors when the SA is unset (the Job
// would run as default and lack Secret/site RBAC) or there are no containers.
func ExtractOperatorInfo(d *appsv1.Deployment) (OperatorInfo, error) {
	sa := d.Spec.Template.Spec.ServiceAccountName
	if sa == "" {
		return OperatorInfo{}, fmt.Errorf("operator Deployment %s/%s has no serviceAccountName; cannot run a gather Job with its RBAC",
			d.Namespace, d.Name)
	}
	containers := d.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		return OperatorInfo{}, fmt.Errorf("operator Deployment %s/%s has no containers", d.Namespace, d.Name)
	}
	image := containers[0].Image
	for _, c := range containers {
		if c.Name == "manager" {
			image = c.Image
			break
		}
	}
	return OperatorInfo{ServiceAccountName: sa, Image: image}, nil
}

// ShardView is the tier-1 per-shard cross-site placement summary (the render-side
// projection of the operator's api ShardSiteView).
type ShardView struct {
	Shard        int
	MasterSite   string
	ReplicaSites []string
	CrossSite    bool
	LinkDown     bool
}

// Verdict is the tier-1 health assessment derived purely from the published
// cross-site view.
type Verdict struct {
	OK       bool
	Degraded bool
	Reasons  []string
}

// StatusVerdict computes the health verdict from the operator's published
// cross-site view (tier 1). A shard that does not span >= 2 sites, an
// unreachable site, a non-ok cluster_state, or fewer than all slots assigned is
// a FAIL; a replica link:down is a non-fatal DEGRADED. Mirrors the checks the
// direct/live verify paths run over live ground truth, so tier 1 gives the same
// verdict shape from published state.
func StatusVerdict(clusterState string, slots int, shards []ShardView, unreachableSites []string) Verdict {
	v := Verdict{OK: true}
	if clusterState != "" && clusterState != "ok" {
		v.OK = false
		v.Reasons = append(v.Reasons, fmt.Sprintf("cluster_state is %q (not ok)", clusterState))
	}
	if slots != 16384 {
		v.OK = false
		v.Reasons = append(v.Reasons, fmt.Sprintf("only %d/16384 slots assigned", slots))
	}
	for _, s := range shards {
		if !s.CrossSite {
			v.OK = false
			v.Reasons = append(v.Reasons, fmt.Sprintf(
				"shard %d does not span >= 2 sites (master site %s, replica sites %v) — a site loss takes the whole shard",
				s.Shard, s.MasterSite, s.ReplicaSites))
		}
		if s.LinkDown {
			v.Degraded = true
			v.Reasons = append(v.Reasons, fmt.Sprintf("shard %d has a replica with link:down — reduced redundancy", s.Shard))
		}
	}
	for _, site := range unreachableSites {
		v.OK = false
		v.Reasons = append(v.Reasons, fmt.Sprintf("site %s unreachable at last reconcile", site))
	}
	return v
}
