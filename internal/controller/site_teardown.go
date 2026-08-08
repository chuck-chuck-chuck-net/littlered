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
	"fmt"

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
// every site's SiteClient, then removes the spoke-teardown finalizer. A site
// unreachable at deletion time keeps the finalizer (bounded implicitly by
// reconcile requeue) and surfaces the MultiSiteTeardown condition; the
// documented escape hatch is manual finalizer removal. Returns (done, error):
// done=false means the caller must requeue and NOT remove the standard
// finalizer yet.
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
	var unreachable []string
	for _, site := range lr.Spec.MultiSite.Sites {
		sc, err := r.sites.siteClient(ctx, lr, site)
		if err != nil {
			log.Info("Multi-site teardown: site client unavailable, retaining finalizer", "site", site.Name, "err", err.Error())
			unreachable = append(unreachable, site.Name)
			continue
		}
		if err := sweepSite(ctx, sc, lr); err != nil {
			log.Info("Multi-site teardown: sweep failed, retaining finalizer", "site", site.Name, "err", err.Error())
			unreachable = append(unreachable, site.Name)
		}
	}

	if len(unreachable) > 0 {
		r.setMultiSiteTeardownCondition(ctx, lr, unreachable)
		// Not done: keep the finalizer, requeue. Bounded by the reconcile loop;
		// the escape hatch for a permanently dead site is manual finalizer
		// removal (documented).
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
// condition naming the unreachable sites. Best-effort (status update errors are
// logged, not fatal — the teardown retries regardless).
func (r *LittleRedReconciler) setMultiSiteTeardownCondition(ctx context.Context, lr *littleredv1alpha1.LittleRed, unreachable []string) {
	log := r.getLogger(ctx, lr, LogCategoryRecon)
	meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionMultiSiteTeardown,
		Status:             metav1.ConditionTrue,
		Reason:             "SiteUnreachable",
		Message:            fmt.Sprintf("Deletion blocked: sweep pending for unreachable site(s) %v; retrying. Escape hatch: remove the %q finalizer manually.", unreachable, multiSiteFinalizerName),
		LastTransitionTime: metav1.Now(),
	})
	if err := r.Status().Update(ctx, lr); err != nil && !apierrors.IsConflict(err) {
		log.Error(err, "Failed to set MultiSiteTeardown condition")
	}
}
