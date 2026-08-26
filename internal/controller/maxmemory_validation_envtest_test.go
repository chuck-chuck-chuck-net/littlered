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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilrand "k8s.io/apimachinery/pkg/util/rand"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
)

// The CEL rule on spec.config.maxmemory is the apply-time half of the guard: it must
// reject the milli-suffix mistake at the apiserver, before the CR is ever stored, so
// the user sees the error at `kubectl apply` rather than in a reconcile event.
// ValidateMaxmemory's unit tests cover the rule's logic; only a real apiserver can
// show that the CEL expression compiles and is enforced.
var _ = Describe("spec.config.maxmemory CEL validation (envtest)", func() {
	var ns string

	BeforeEach(func() {
		ns = "maxmem-" + utilrand.String(6)
		createNamespace(ns)
	})

	newCR := func(name, maxmemory string) *littleredv1alpha1.LittleRed {
		cr := &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		}
		cr.Spec.Config.Maxmemory = maxmemory
		return cr
	}

	It("rejects the milli suffix at admission", func() {
		err := k8sClient.Create(ctx, newCR("milli", "375m"))
		Expect(err).To(HaveOccurred(), "375m is 0.375 bytes and renders as maxmemory 1")
		Expect(err.Error()).To(ContainSubstring("375Mi"),
			"the rejection must name the value the user probably meant")
	})

	It("rejects the micro and nano suffixes at admission", func() {
		Expect(k8sClient.Create(ctx, newCR("micro", "375u"))).NotTo(Succeed())
		Expect(k8sClient.Create(ctx, newCR("nano", "375n"))).NotTo(Succeed())
	})

	It("accepts the quantities a user actually means", func() {
		Expect(k8sClient.Create(ctx, newCR("mebi", "375Mi"))).To(Succeed())
		Expect(k8sClient.Create(ctx, newCR("mega", "375M"))).To(Succeed())
		Expect(k8sClient.Create(ctx, newCR("gibi", "2Gi"))).To(Succeed())
		Expect(k8sClient.Create(ctx, newCR("unset", ""))).To(Succeed())
	})
})
