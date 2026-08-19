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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

const testNamespaceDefault = "default"

var _ = Describe("LittleRed Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: testNamespaceDefault, // TODO(user):Modify as needed
		}
		littlered := &littleredv1alpha1.LittleRed{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind LittleRed")
			err := k8sClient.Get(ctx, typeNamespacedName, littlered)
			if err != nil && errors.IsNotFound(err) {
				resource := &littleredv1alpha1.LittleRed{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: testNamespaceDefault,
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &littleredv1alpha1.LittleRed{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance LittleRed")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &LittleRedReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})

	// Mode-mismatch validation is enforced by CEL x-kubernetes-validations rules
	// on the CRD, so it is rejected by the apiserver at admission time.
	Context("When validating mode-specific spec blocks (issue #61)", func() {
		ctx := context.Background()

		newLR := func(name string) *littleredv1alpha1.LittleRed {
			return &littleredv1alpha1.LittleRed{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespaceDefault},
			}
		}

		It("rejects spec.cluster when mode is not cluster", func() {
			lr := newLR("mismatch-cluster")
			lr.Spec.Mode = ModeStandalone
			lr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{Shards: 3}

			err := k8sClient.Create(ctx, lr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.cluster may only be set when spec.mode is 'cluster'"))
		})

		It("rejects spec.sentinel when mode is not sentinel", func() {
			lr := newLR("mismatch-sentinel")
			lr.Spec.Mode = ModeStandalone
			// masterName must be set even though this spec is expected to be rejected:
			// omitting it makes the API server reject the object for the *required
			// field* instead of the mode mismatch under test, which would be a false
			// pass. (Same trap as LR-033's negative specs and sitesPerShard.)
			lr.Spec.Sentinel = &littleredv1alpha1.SentinelSpec{MasterName: "probe.mismatch"}

			err := k8sClient.Create(ctx, lr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.sentinel may only be set when spec.mode is 'sentinel'"))
		})

		It("allows the matching mode-specific block", func() {
			lr := newLR("match-cluster")
			lr.Spec.Mode = ModeCluster
			lr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{Shards: 3}

			Expect(k8sClient.Create(ctx, lr)).To(Succeed())
			Expect(k8sClient.Delete(ctx, lr)).To(Succeed())
		})

		It("allows a CR with no mode-specific block", func() {
			lr := newLR("no-block")
			lr.Spec.Mode = ModeStandalone

			Expect(k8sClient.Create(ctx, lr)).To(Succeed())
			Expect(k8sClient.Delete(ctx, lr)).To(Succeed())
		})
	})
})
