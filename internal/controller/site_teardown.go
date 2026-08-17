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
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// isMultiSite reports whether lr is a multi-site instance.
func isMultiSite(lr *littleredv1alpha1.LittleRed) bool {
	return lr.Spec.MultiSite != nil
}

// Condition reasons for a blocked spoke sweep. The split exists for one reason:
// only ONE of these three states clears without a human, and a reader deciding
// whether to wait or to act needs to know which one they are in.
const (
	// reasonTeardownConfigFault: the operator could not read its OWN hub-local
	// inputs for a site (Secret missing/unreadable/malformed, own namespace
	// unresolvable). Never self-heals. Deleting the site kubeconfig Secrets while
	// tearing an instance down is a natural thing to do, and it is exactly what
	// wedges deletion here — so this must not point at the remote site.
	reasonTeardownConfigFault = "SiteConfigurationUnreadable"
	// reasonTeardownSweepFailed: the site answered, but deleting its workloads
	// failed (RBAC, admission webhook, a stuck object). Does not self-heal either,
	// but the cause is remote and distinct from the above.
	reasonTeardownSweepFailed = "SiteSweepFailed"
	// reasonTeardownSiteUnreachable: the site's Kubernetes API did not answer. The
	// only one of the three that may clear on its own, and the only one the
	// original single label described correctly.
	reasonTeardownSiteUnreachable = "SiteUnreachable"
)

// siteTeardownBlock is one site's reason for blocking the spoke sweep this pass.
type siteTeardownBlock struct {
	site   string
	reason string // one of the reasonTeardown* constants
	cause  string
}

// classifySiteClientBlock classifies a failure to obtain a site's client. The
// whole of site-client construction is the operator reading and using its OWN
// hub-local inputs — nothing dials until the cache starts — and siteClient
// already marks those failures with siteConfigFault, so this only has to stop
// throwing that classification away.
func classifySiteClientBlock(site string, err error) siteTeardownBlock {
	if _, ok := siteConfigFaultOf(err); ok {
		return siteTeardownBlock{site: site, reason: reasonTeardownConfigFault, cause: err.Error()}
	}
	return siteTeardownBlock{site: site, reason: reasonTeardownSiteUnreachable, cause: err.Error()}
}

// classifySweepBlock classifies a sweep failure.
//
// "The sweep failed" is NOT the same as "the site answered and refused": the
// client is built from a cached kubeconfig, so the very first List can still die
// in the transport. Saying "the site answered but the delete failed" about a
// dial timeout would be the same species of false statement this change exists
// to remove, so the two are split on whether the API server was actually
// reached.
func classifySweepBlock(site string, err error) siteTeardownBlock {
	return siteTeardownBlock{site: site, reason: sweepBlockReason(err), cause: err.Error()}
}

// sweepBlockReason reports whether a sweep error means the site never answered.
func sweepBlockReason(err error) string {
	// Transport-level: the request never reached an API server.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return reasonTeardownSiteUnreachable
	}
	// Answered, but with "ask again later" — indistinguishable in kind from not
	// answering, for the only question this label decides (does waiting help?).
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsServiceUnavailable(err) {
		return reasonTeardownSiteUnreachable
	}
	return reasonTeardownSweepFailed
}

// siteTeardownSummary aggregates one pass's blocks, split by whether waiting
// helps. Pure.
type siteTeardownSummary struct {
	// Blocked names every blocked site, in spec order — what the sweep still owes.
	Blocked []string
	// The three classes, each in spec order.
	ConfigFaulted []string
	SweepFailed   []string
	Unreachable   []string
	// Details is one "site: Reason — cause" line per block, in spec order.
	Details []string
}

// Any reports whether the sweep is blocked at all.
func (s siteTeardownSummary) Any() bool { return len(s.Blocked) > 0 }

// Reason picks the condition reason by ACTIONABILITY, most-actionable first. A
// mixed pass must not advertise the one class that might resolve itself while a
// permanent fault is also present: that combination is exactly the wait-forever
// trap, and the permanent fault is the one a human has to act on.
func (s siteTeardownSummary) Reason() string {
	switch {
	case len(s.ConfigFaulted) > 0:
		return reasonTeardownConfigFault
	case len(s.SweepFailed) > 0:
		return reasonTeardownSweepFailed
	default:
		return reasonTeardownSiteUnreachable
	}
}

// summarizeSiteTeardownBlocks aggregates per-site blocks. Pure.
func summarizeSiteTeardownBlocks(in []siteTeardownBlock) siteTeardownSummary {
	out := siteTeardownSummary{}
	for _, b := range in {
		out.Blocked = append(out.Blocked, b.site)
		out.Details = append(out.Details, fmt.Sprintf("%s: %s — %s", b.site, b.reason, b.cause))
		switch b.reason {
		case reasonTeardownConfigFault:
			out.ConfigFaulted = append(out.ConfigFaulted, b.site)
		case reasonTeardownSweepFailed:
			out.SweepFailed = append(out.SweepFailed, b.site)
		default:
			out.Unreachable = append(out.Unreachable, b.site)
		}
	}
	return out
}

// describeSiteTeardownBlocks renders the human-facing condition message. Pure.
//
// The policy it encodes: RETAIN the finalizer in every case — releasing it would
// complete the deletion while stranding Redis pods on the site forever and
// invisibly, since owner references do not cross Kubernetes clusters and this
// sweep is the only garbage collection there is. But say the true thing, because
// a message that blames the remote site for a Secret the reader deleted locally
// a minute ago actively prevents them from fixing it.
func describeSiteTeardownBlocks(s siteTeardownSummary) string {
	if !s.Any() {
		return ""
	}
	const finalizer = multiSiteFinalizerName
	var b strings.Builder
	fmt.Fprintf(&b, "Deletion is blocked: the spoke sweep could not complete for site(s) %s.",
		strings.Join(s.Blocked, ", "))

	if len(s.ConfigFaulted) > 0 {
		fmt.Fprintf(&b, " For %s the operator cannot read its OWN hub-local configuration — the site "+
			"kubeconfig Secret in the operator's namespace (missing, unreadable, or malformed), or the "+
			"operator's own namespace. This will NOT clear on its own, however long it retries. Restore "+
			"the kubeconfig Secret, or remove the %q finalizer and delete this instance's workloads on "+
			"those sites by hand.", strings.Join(s.ConfigFaulted, ", "), finalizer)
	}
	if len(s.SweepFailed) > 0 {
		fmt.Fprintf(&b, " For %s the site answered but deleting the workloads failed (RBAC, an admission "+
			"webhook, or a stuck object). This will NOT clear on its own either: fix the cause, or remove "+
			"the %q finalizer and clean up by hand.", strings.Join(s.SweepFailed, ", "), finalizer)
	}
	if len(s.Unreachable) > 0 {
		fmt.Fprintf(&b, " Site(s) %s did not answer; this is the one case that may clear on its own, and "+
			"it is being retried. If such a site is permanently gone, remove the %q finalizer manually and "+
			"clean it up by hand.", strings.Join(s.Unreachable, ", "), finalizer)
	}
	fmt.Fprintf(&b, " Details: %s", strings.Join(s.Details, "; "))
	return b.String()
}

// ensureMultiSiteFinalizer adds the spoke-teardown finalizer to a multi-site CR
// if absent. Spoke-side children carry no owner references (owner references do
// not work across Kubernetes clusters), so a finalizer-driven sweep is their
// only garbage collection. Returns true if it updated the object (caller should
// requeue).
func (r *LittleRedReconciler) ensureMultiSiteFinalizer(ctx context.Context, lr *littleredv1alpha1.LittleRed) (bool, error) {
	if !isMultiSite(lr) || controllerutil.ContainsFinalizer(lr, multiSiteFinalizerName) {
		return false, nil
	}
	controllerutil.AddFinalizer(lr, multiSiteFinalizerName)
	if err := r.Update(ctx, lr); err != nil {
		return false, err
	}
	return true, nil
}

// multiSiteTeardownKinds are the instance-labeled workload objects the spoke
// sweep deletes, one empty-list-typed object per kind.
func multiSiteTeardownKinds() []client.ObjectList {
	return []client.ObjectList{
		&appsv1.StatefulSetList{},
		&corev1.ServiceList{},
		&corev1.ConfigMapList{},
		&policyv1.PodDisruptionBudgetList{},
	}
}

// handleMultiSiteDeletion sweeps the instance-labeled workload objects through
// every site's SiteClient, then removes the spoke-teardown finalizer. Any site
// the sweep cannot finish keeps the finalizer (bounded implicitly by reconcile
// requeue) and surfaces the MultiSiteTeardown condition; the documented escape
// hatch is manual finalizer removal. Returns (done, error): done=false means the
// caller must requeue and NOT remove the standard finalizer yet.
//
// Blocks are classified rather than lumped together, because the three ways this
// can block differ in the only respect that matters to whoever is waiting for
// `kubectl delete` to return: a hub-local configuration fault and a failed delete
// never clear by themselves, an unreachable site may. Reporting all three as
// "site unreachable" (as this path did before) is worse here than on the
// reconcile path LR-034 fixed — there the instance kept serving, whereas here the
// CR can never be deleted, and the natural trigger is a human tidying up the site
// kubeconfig Secrets just before deleting the instance.
func (r *LittleRedReconciler) handleMultiSiteDeletion(ctx context.Context, lr *littleredv1alpha1.LittleRed) (bool, error) {
	if !controllerutil.ContainsFinalizer(lr, multiSiteFinalizerName) {
		return true, nil
	}
	log := r.getLogger(ctx, lr, LogCategoryRecon)

	// The registry may be nil in unit/envtest (reconciler built without a
	// manager). Without site clients there is nothing to sweep; release the
	// finalizer so deletion is not wedged.
	if r.sites == nil {
		return r.removeMultiSiteFinalizer(ctx, lr)
	}

	// Defensive: a malformed CR can carry the teardown finalizer while its
	// MultiSite spec block is gone (finalizer present without the spec). There
	// is no per-site spec to sweep, and dereferencing lr.Spec.MultiSite.Sites
	// would nil-panic every reconcile and wedge deletion forever. Drop the
	// finalizer so deletion can complete.
	if lr.Spec.MultiSite == nil {
		log.Info("Multi-site teardown: finalizer present but Spec.MultiSite is nil; nothing to sweep, releasing finalizer")
		return r.removeMultiSiteFinalizer(ctx, lr)
	}

	owner := types.NamespacedName{Namespace: lr.Namespace, Name: lr.Name}
	var blocks []siteTeardownBlock
	for _, site := range lr.Spec.MultiSite.Sites {
		sc, err := r.sites.siteClient(ctx, lr, site)
		if err != nil {
			blk := classifySiteClientBlock(site.Name, err)
			log.Info("Multi-site teardown: site client unavailable, retaining finalizer",
				"site", site.Name, "reason", blk.reason, "err", err.Error())
			blocks = append(blocks, blk)
			continue
		}
		if err := sweepSite(ctx, sc, lr); err != nil {
			blk := classifySweepBlock(site.Name, err)
			log.Info("Multi-site teardown: sweep failed, retaining finalizer",
				"site", site.Name, "reason", blk.reason, "err", err.Error())
			blocks = append(blocks, blk)
		}
	}

	if summary := summarizeSiteTeardownBlocks(blocks); summary.Any() {
		r.setMultiSiteTeardownCondition(ctx, lr, summary)
		// Not done: keep the finalizer, requeue. Retaining is the deliberate
		// policy even for the two classes that will never clear by themselves —
		// releasing would complete the deletion while stranding this instance's
		// pods on the site, unowned and unfindable. The condition now says which
		// class this is, so "wait" versus "act" is the reader's call to make.
		return false, nil
	}

	// Sweep complete across all sites: drop the site clients and release the
	// finalizer.
	r.sites.drop(owner)
	return r.removeMultiSiteFinalizer(ctx, lr)
}

// sweepSite deletes every instance-labeled workload object of the CR in one
// site. Idempotent: not-found deletions are ignored, so repeated passes and the
// hub-local site (whose owner references already GC the same objects) are safe.
func sweepSite(ctx context.Context, sc SiteClient, lr *littleredv1alpha1.LittleRed) error {
	sel := client.MatchingLabels(selectorLabels(lr))
	inNS := client.InNamespace(lr.Namespace)
	for _, list := range multiSiteTeardownKinds() {
		if err := sc.List(ctx, list, inNS, sel); err != nil {
			return fmt.Errorf("site %q: listing %T: %w", sc.Name(), list, err)
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			return fmt.Errorf("site %q: extracting %T: %w", sc.Name(), list, err)
		}
		for _, item := range items {
			obj, ok := item.(client.Object)
			if !ok {
				continue
			}
			if err := sc.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("site %q: deleting %s/%s: %w", sc.Name(), obj.GetNamespace(), obj.GetName(), err)
			}
		}
	}
	return nil
}

// removeMultiSiteFinalizer clears the spoke-teardown finalizer.
func (r *LittleRedReconciler) removeMultiSiteFinalizer(ctx context.Context, lr *littleredv1alpha1.LittleRed) (bool, error) {
	controllerutil.RemoveFinalizer(lr, multiSiteFinalizerName)
	if err := r.Update(ctx, lr); err != nil {
		return false, err
	}
	return true, nil
}

// setMultiSiteTeardownCondition surfaces a blocked teardown as a status
// condition, naming the blocked sites AND which of the three classes each is in
// — because only one of them clears by waiting. Best-effort (status update errors
// are logged, not fatal — the teardown retries regardless).
func (r *LittleRedReconciler) setMultiSiteTeardownCondition(ctx context.Context, lr *littleredv1alpha1.LittleRed, summary siteTeardownSummary) {
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionMultiSiteTeardown,
		Status:             metav1.ConditionTrue,
		Reason:             summary.Reason(),
		Message:            describeSiteTeardownBlocks(summary),
		LastTransitionTime: metav1.Now(),
	})
	if err := r.Status().Update(ctx, lr); err != nil && !apierrors.IsConflict(err) {
		log.Error(err, "Failed to set MultiSiteTeardown condition")
	}
}
