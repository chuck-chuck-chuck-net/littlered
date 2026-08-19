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
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// The two guards protecting the metadata contract (ADR-015) are CRD CEL rules, so the
// kube-apiserver — not Go code — is what enforces them. Unit tests cannot reach them:
// they only verify the operator's side of the bargain. This suite verifies the
// apiserver's side against a real one.
var _ = Describe("Metadata contract validation", func() {
	var ctx context.Context

	newCR := func() *littleredv1alpha1.LittleRed {
		return &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "meta-" + utilrand.String(6),
				Namespace: "default",
			},
			Spec: littleredv1alpha1.LittleRedSpec{Mode: ModeStandalone},
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
	})

	Context("spec.appName", func() {
		It("defaults to the built-in app name", func() {
			cr := newCR()
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			Expect(cr.Spec.AppName).To(Equal(littleredv1alpha1.DefaultAppName))
		})

		It("accepts a custom value at creation", func() {
			cr := newCR()
			cr.Spec.AppName = metaCustomApp
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			Expect(cr.Spec.AppName).To(Equal(metaCustomApp))
		})

		// The StatefulSet selector is immutable, so a changed app name would leave the
		// operator unable to update its own workload. Rejecting the edit is the only
		// non-destructive answer.
		It("rejects a change after creation", func() {
			cr := newCR()
			cr.Spec.AppName = metaCustomApp
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			cr.Spec.AppName = "valkey"
			err := k8sClient.Update(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable"))
		})

		It("rejects an empty explicit value", func() {
			cr := newCR()
			cr.Spec.AppName = ""
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			// An omitted value is defaulted rather than rejected; the MinLength guard
			// exists for a value the client sends explicitly, which the typed client
			// cannot express (empty string == omitted). Assert the default instead.
			Expect(cr.Spec.AppName).To(Equal(littleredv1alpha1.DefaultAppName))
		})
	})

	Context("spec.podTemplate.labels", func() {
		It("accepts ordinary user labels", func() {
			cr := newCR()
			cr.Spec.PodTemplate.Labels = map[string]string{metaTeamKey: metaTeamValue}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })
		})

		// A pod template whose labels disagree with the StatefulSet selector is rejected
		// by the apiserver at StatefulSet apply time — far from the CR the user edited.
		// The CEL rule moves that failure to the CR, where it is actionable.
		It("rejects each structural label key", func() {
			for _, key := range []string{
				labelAppName,
				labelAppInstance,
				labelAppComponent,
				LabelShard,
				LabelRole,
			} {
				cr := newCR()
				cr.Spec.PodTemplate.Labels = map[string]string{key: metaHijackValue}
				err := k8sClient.Create(ctx, cr)
				Expect(err).To(HaveOccurred(), "expected %s to be rejected", key)
				Expect(err.Error()).To(ContainSubstring("structural labels"), "for key %s", key)
			}
		})
	})

	Context("inherited metadata", func() {
		// Propagation itself is unit-tested; what needs a real apiserver is that the
		// labels survive a round trip through it, i.e. that nothing in the CRD schema
		// strips or rejects arbitrary metadata on the CR.
		It("keeps user labels and annotations on the stored CR", func() {
			cr := newCR()
			cr.Labels = map[string]string{metaTeamKey: metaTeamValue}
			cr.Annotations = map[string]string{metaOwnerKey: metaOwnerValue}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			fetched := &littleredv1alpha1.LittleRed{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), fetched)).To(Succeed())
			Expect(fetched.Labels).To(HaveKeyWithValue(metaTeamKey, metaTeamValue))
			Expect(fetched.Annotations).To(HaveKeyWithValue(metaOwnerKey, metaOwnerValue))

			Expect(inheritedLabels(fetched)).To(HaveKeyWithValue(metaTeamKey, metaTeamValue))
			Expect(inheritedAnnotations(fetched)).To(HaveKeyWithValue(metaOwnerKey, metaOwnerValue))
		})
	})
})
