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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// Multi-site finalizer wiring (Milestone 1): a multi-site CR gains the
// spoke-teardown finalizer, and on deletion the teardown sweep runs and the
// finalizers are released. The registry is nil here (reconciler built without a
// manager), so the sweep degenerates to "nothing to sweep" and releases the
// finalizer — the wiring, not the remote sweep, is under test.
var _ = Describe("MultiSite finalizer teardown", func() {
	const resourceName = "ms-teardown"
	ctx := context.Background()
	nn := types.NamespacedName{Name: resourceName, Namespace: testNamespaceDefault}

	var reconciler *LittleRedReconciler

	reconcileOnce := func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		reconciler = &LittleRedReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	})

	AfterEach(func() {
		lr := &littleredv1alpha1.LittleRed{}
		if err := k8sClient.Get(ctx, nn, lr); err == nil {
			// Force-clear any lingering finalizers so cleanup never wedges.
			lr.Finalizers = nil
			_ = k8sClient.Update(ctx, lr)
			_ = k8sClient.Delete(ctx, lr)
		}
	})

	It("does not panic and drops the finalizer when the finalizer is present but Spec.MultiSite is nil", func() {
		// Malformed CR: the multi-site teardown finalizer is present but the
		// MultiSite spec block is gone (e.g. edited away). Without a nil-guard,
		// `range lr.Spec.MultiSite.Sites` dereferences a nil pointer and the
		// operator panics on every reconcile, wedging deletion forever.
		lr := &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{
				Name:       resourceName,
				Namespace:  testNamespaceDefault,
				Finalizers: []string{finalizerName, multiSiteFinalizerName},
			},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:    ModeCluster,
				Cluster: &littleredv1alpha1.ClusterSpec{Shards: 3},
				// MultiSite intentionally nil.
			},
		}
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())
		Expect(k8sClient.Get(ctx, nn, lr)).To(Succeed())

		// Non-nil registry so the sweep is not short-circuited by the
		// `r.sites == nil` guard — this drives execution into the loop that
		// dereferences the nil MultiSite spec.
		reconciler.sites = newSiteClientRegistry(k8sClient, k8sClient, k8sClient.Scheme(), nil)

		var done bool
		var err error
		Expect(func() {
			done, err = reconciler.handleMultiSiteDeletion(ctx, lr)
		}).NotTo(Panic(), "must not dereference a nil Spec.MultiSite")
		Expect(err).NotTo(HaveOccurred())
		Expect(done).To(BeTrue(), "with no per-site spec to sweep, teardown is done")
		Expect(controllerutil.ContainsFinalizer(lr, multiSiteFinalizerName)).To(BeFalse(),
			"the spoke-teardown finalizer must be released so deletion can complete")
	})

	It("adds the spoke-teardown finalizer and releases both on deletion", func() {
		lr := &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: testNamespaceDefault},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:    ModeCluster,
				Cluster: &littleredv1alpha1.ClusterSpec{Shards: 3},
				MultiSite: &littleredv1alpha1.MultiSiteSpec{
					Sites: []littleredv1alpha1.SiteSpec{
						{Name: siteAlpha},
						{Name: siteBeta, KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: "site-beta-kubeconfig"}},
					},
					SitesPerShard: 2,
				},
			},
		}
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())

		By("reconciling: standard finalizer, then multi-site finalizer")
		reconcileOnce() // adds standard finalizer, requeues
		reconcileOnce() // adds multi-site finalizer, requeues

		Expect(k8sClient.Get(ctx, nn, lr)).To(Succeed())
		Expect(controllerutil.ContainsFinalizer(lr, finalizerName)).To(BeTrue())
		Expect(controllerutil.ContainsFinalizer(lr, multiSiteFinalizerName)).To(BeTrue(),
			"multi-site CR must carry the spoke-teardown finalizer")

		By("deleting the CR")
		Expect(k8sClient.Delete(ctx, lr)).To(Succeed())

		// Deletion is blocked by the finalizers until reconcileDelete runs.
		reconcileOnce() // sweeps spokes (nil registry: nothing to sweep), releases both finalizers

		By("asserting the CR is fully gone (both finalizers released)")
		err := k8sClient.Get(ctx, nn, lr)
		Expect(err).To(HaveOccurred(), "CR should be garbage-collected once both finalizers are removed")
	})
})
