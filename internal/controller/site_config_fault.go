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

import (
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// Multi-site site resolution can fail in two fundamentally different ways, and
// the operator used to report both as "site unreachable":
//
//   - REACHABILITY — the site's Kubernetes API did not answer. Normal, often
//     transient, self-healing; the design treats it as "unmanaged, not broken"
//     (pillar 3.2) and deliberately does nothing about it.
//   - CONFIGURATION — the operator could not read or parse its OWN hub-local
//     inputs: its namespace was unresolvable, or the site's kubeconfig Secret is
//     missing, unreadable, or malformed. This never heals without a human.
//
// The difference is exactly "must someone act?", so conflating them makes the
// alert an operator actually wants impossible to write. The tell is hub-local vs
// remote: everything up to and including parsing the kubeconfig is the operator
// reading its own configuration; only what follows touches the site.
const (
	// siteFaultOperatorNamespace: the operator cannot determine its own namespace
	// (no OPERATOR_NAMESPACE, no POD_NAMESPACE, no serviceaccount namespace file),
	// so it cannot locate any site kubeconfig Secret. Breaks every REMOTE site at
	// once; hub-local sites are unaffected (they never look one up).
	siteFaultOperatorNamespace = "OperatorNamespaceUnresolvable"
	// siteFaultSecretUnreadable: the kubeconfig Secret could not be read — absent,
	// or RBAC-forbidden (allow-list scoping renders no ClusterRole, so the operator
	// needs an explicit `get secrets` Role in its own namespace).
	siteFaultSecretUnreadable = "KubeconfigSecretUnreadable"
	// siteFaultSecretMalformed: the Secret exists but does not carry a usable
	// kubeconfig — the "kubeconfig" key is missing/empty, or its bytes do not parse.
	siteFaultSecretMalformed = "KubeconfigSecretMalformed"

	// siteReasonUnreachable is the non-actionable counterpart published in
	// SiteStatus.Reason: the site's Kubernetes API simply did not answer.
	siteReasonUnreachable = "Unreachable"

	// reasonMultiSiteConfigInvalid / reasonMultiSiteConfigValid are the condition
	// (and event) reasons for the surfaced configuration state.
	reasonMultiSiteConfigInvalid = "MultiSiteConfigInvalid"
	reasonMultiSiteConfigValid   = "MultiSiteConfigValid"
)

// siteConfigError marks a site-resolution failure as hub-local CONFIGURATION
// rather than site reachability. Callers distinguish it with errors.As
// (siteConfigFaultOf); it wraps the cause so the underlying message still
// reaches operators.
type siteConfigError struct {
	site   string
	reason string
	err    error
}

func (e *siteConfigError) Error() string {
	return fmt.Sprintf("site %q: %s: %v", e.site, e.reason, e.err)
}

func (e *siteConfigError) Unwrap() error { return e.err }

// siteConfigFault wraps err as a configuration fault for the named site.
func siteConfigFault(site, reason string, err error) error {
	return &siteConfigError{site: site, reason: reason, err: err}
}

// siteConfigFaultOf reports whether err (or anything it wraps) is a hub-local
// configuration fault, returning it for its reason/site.
func siteConfigFaultOf(err error) (*siteConfigError, bool) {
	var sce *siteConfigError
	if errors.As(err, &sce) {
		return sce, true
	}
	return nil, false
}

// siteConfigObservation is one site's configuration state this pass — the pure
// seam's input, projected from the spec (remote vs hub-local) and the resolve
// error (fault classification).
type siteConfigObservation struct {
	name string
	// remote is whether the site has a kubeconfigSecretRef. A hub-local site is
	// served by the operator's own client and can never config-fault.
	remote  bool
	fault   bool
	reason  string
	message string
}

// siteConfigFaultSummary is the aggregate a reader needs to tell a FULLY
// unmanaged instance from a partially unmanaged one.
type siteConfigFaultSummary struct {
	// Faulted names the config-faulted sites, in spec order.
	Faulted []string
	// Details is one "site: Reason — cause" line per faulted site, in spec order.
	Details []string
	// RemoteTotal / HubLocalTotal split the spec's sites; only remote ones can fault.
	RemoteTotal   int
	HubLocalTotal int
	// AllRemoteFaulted is true when every REMOTE site is faulted — the instance is
	// fully unmanaged except for any hub-local site, which keeps working.
	AllRemoteFaulted bool
}

// Any reports whether at least one site is config-faulted.
func (s siteConfigFaultSummary) Any() bool { return len(s.Faulted) > 0 }

// summarizeSiteConfigFaults aggregates the per-site configuration state. Pure.
func summarizeSiteConfigFaults(in []siteConfigObservation) siteConfigFaultSummary {
	out := siteConfigFaultSummary{}
	for _, o := range in {
		if !o.remote {
			out.HubLocalTotal++
			continue
		}
		out.RemoteTotal++
		if !o.fault {
			continue
		}
		out.Faulted = append(out.Faulted, o.name)
		out.Details = append(out.Details, fmt.Sprintf("%s: %s — %s", o.name, o.reason, o.message))
	}
	out.AllRemoteFaulted = out.RemoteTotal > 0 && len(out.Faulted) == out.RemoteTotal
	return out
}

// describeSiteConfigFaults renders the human-facing condition/event message. It
// states the coverage honestly: a hub-local site is never counted as unmanaged
// (it has no kubeconfig Secret to fail on), so "all remote sites" never becomes
// "all sites" while one still works.
func describeSiteConfigFaults(s siteConfigFaultSummary) string {
	if !s.Any() {
		return ""
	}
	var b strings.Builder
	if s.AllRemoteFaulted {
		fmt.Fprintf(&b, "Cannot read the configuration for ALL %d remote site(s) (%s), so the instance is "+
			"unmanaged", s.RemoteTotal, strings.Join(s.Faulted, ", "))
		if s.HubLocalTotal > 0 {
			fmt.Fprintf(&b, " apart from its %d hub-local site(s), which need no kubeconfig and keep being "+
				"reconciled", s.HubLocalTotal)
		}
		b.WriteString(".")
	} else {
		fmt.Fprintf(&b, "Cannot read the configuration for %d of %d remote site(s) (%s), so those sites are "+
			"unmanaged; the remaining site(s) are reconciled normally.",
			len(s.Faulted), s.RemoteTotal, strings.Join(s.Faulted, ", "))
	}
	b.WriteString(" This is the operator failing to read its OWN hub-local inputs, not a site failing to " +
		"answer: unlike an unreachable site it will NOT heal on its own. Check the site kubeconfig Secret " +
		"in the operator's namespace (it must exist, be readable — allow-list scoping needs an explicit " +
		"`get secrets` Role there — and hold a parseable \"kubeconfig\" key), and that the operator can " +
		"resolve its own namespace (OPERATOR_NAMESPACE / POD_NAMESPACE). Details: " +
		strings.Join(s.Details, "; "))
	return b.String()
}

// siteConfigObservationsFrom projects the resolved sites onto the pure seam's
// input, pairing each with its spec entry by index (resolveSites preserves spec
// order) to learn whether it is remote, and classifying its error with errors.As.
func siteConfigObservationsFrom(lr *littleredv1alpha1.LittleRed, sites []resolvedSite) []siteConfigObservation {
	if lr.Spec.MultiSite == nil {
		return nil
	}
	specSites := lr.Spec.MultiSite.Sites
	out := make([]siteConfigObservation, 0, len(sites))
	for _, rs := range sites {
		o := siteConfigObservation{name: rs.name}
		if rs.idx >= 0 && rs.idx < len(specSites) {
			o.remote = specSites[rs.idx].KubeconfigSecretRef != nil
		}
		if sce, ok := rs.configFault(); ok {
			o.fault = true
			o.reason = sce.reason
			o.message = sce.Error()
		}
		out = append(out, o)
	}
	return out
}

// resolvedFromObs recovers the resolved sites carried by the observations, so the
// status path can classify configuration faults without re-resolving.
func resolvedFromObs(obs []siteObservation) []resolvedSite {
	out := make([]resolvedSite, 0, len(obs))
	for _, o := range obs {
		out = append(out, o.site)
	}
	return out
}

// siteStatusReason classifies one resolved+observed site for SiteStatus. A
// reachable site has no reason; an unreachable one is either a configuration
// fault (actionable, never self-healing) or a plain non-answer (not actionable).
func siteStatusReason(o siteObservation) (reason, message string) {
	if o.reachable {
		return "", ""
	}
	if sce, ok := o.site.configFault(); ok {
		return sce.reason, sce.Error()
	}
	if o.site.err != nil {
		return siteReasonUnreachable, o.site.err.Error()
	}
	return siteReasonUnreachable, "the site's Kubernetes API did not answer at the last probe"
}

// multiSiteNothingBuilt reports whether the operator has never got as far as
// publishing per-site status for this instance — the proxy for "nothing has been
// built yet", used to decide whether a configuration fault is refusable.
//
// Deliberately NOT keyed on the phase: an instance that dips to Initializing (a
// pod restart, a brief unreadiness) is still a live instance, and refusing there
// would halt management of its healthy sites. status.multiSite.sites is written
// only by updateMultiSiteClusterStatus, which runs after resources have been
// ensured, and every later pass republishes it — so it is monotone: once true it
// never reverts, and a refusal can never trap a running instance.
func multiSiteNothingBuilt(lr *littleredv1alpha1.LittleRed) bool {
	return lr.Status.MultiSite == nil || len(lr.Status.MultiSite.Sites) == 0
}

// reconcileSiteConfigCondition sets or clears the MultiSiteConfigInvalid warning
// condition from the per-site configuration state, mirroring the
// ReducedSiteLossResilience precedent: an idempotent condition plus a ONE-SHOT
// Warning event on the transition into the state (no event spam), and never a
// phase change — the data plane is healthy, phase is single-valued and cannot
// carry two orthogonal facts, and any non-Running value breaks every consumer
// that tests phase == "Running". The bootstrap case, where nothing is running to
// protect, is handled separately by enforceSiteConfigGuard.
func (r *LittleRedReconciler) reconcileSiteConfigCondition(lr *littleredv1alpha1.LittleRed, cfgObs []siteConfigObservation) {
	summary := summarizeSiteConfigFaults(cfgObs)

	if !summary.Any() {
		meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
			Type:               littleredv1alpha1.ConditionMultiSiteConfigInvalid,
			Status:             metav1.ConditionFalse,
			Reason:             reasonMultiSiteConfigValid,
			Message:            "Every site's configuration is readable.",
			LastTransitionTime: metav1.Now(),
		})
		return
	}

	msg := describeSiteConfigFaults(summary)
	if existing := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionMultiSiteConfigInvalid); existing == nil || existing.Status != metav1.ConditionTrue {
		r.event(lr, corev1.EventTypeWarning, reasonMultiSiteConfigInvalid, msg)
	}
	meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionMultiSiteConfigInvalid,
		Status:             metav1.ConditionTrue,
		Reason:             reasonMultiSiteConfigInvalid,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
}

// enforceSiteConfigGuard refuses to bring up a multi-site instance whose
// configuration the operator cannot read. It fires ONLY before anything has been
// built (multiSiteNothingBuilt): there is no data plane to protect, and
// proceeding would half-build an undesigned topology across whichever sites did
// resolve. Unlike the LR-032 topology case this needs no dangerous inference —
// "I cannot read my own Secret" is unambiguous and hub-local.
//
// On an existing instance it never fires: one site's deleted Secret must not stop
// management of the healthy sites, which would turn a typo into an instance-wide
// management outage (worse than the disease, and contrary to pillar 3.2). There
// the warning condition alone carries the state.
//
// The refusal is a STATE, not a latch: it is recomputed from the live resolve
// every pass, so fixing the Secret lets the next reconcile proceed even though
// the phase is still Failed.
func (r *LittleRedReconciler) enforceSiteConfigGuard(lr *littleredv1alpha1.LittleRed, sites []resolvedSite) (bool, ctrl.Result) {
	cfgObs := siteConfigObservationsFrom(lr, sites)
	summary := summarizeSiteConfigFaults(cfgObs)
	if !summary.Any() || !multiSiteNothingBuilt(lr) {
		return false, ctrl.Result{}
	}

	msg := "Refusing to bring up the multi-site instance: " + describeSiteConfigFaults(summary) +
		" Nothing has been built yet, so the operator refuses rather than half-building a topology " +
		"across only the sites it could resolve. Reconciliation resumes automatically once the " +
		"configuration is readable."

	r.reconcileSiteConfigCondition(lr, cfgObs)
	lr.Status.Phase = littleredv1alpha1.PhaseFailed
	lr.Status.Status = reasonMultiSiteConfigInvalid
	meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reasonMultiSiteConfigInvalid,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})

	// The site kubeconfig Secrets are read UNCACHED and are not watched (they live
	// outside the operator's declared watch scope), so nothing wakes the reconcile
	// when one is fixed: the requeue interval IS the recovery latency. Steady
	// rather than fast — a misconfiguration is not worth a 2s poll.
	_, steady := lr.GetRequeueIntervals()
	return true, ctrl.Result{RequeueAfter: steady}
}
