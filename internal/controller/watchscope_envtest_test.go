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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	"github.com/chuck-chuck-chuck-net/littlered/internal/watchscope"
)

// This suite proves ADR-014 Milestone N4: that watchscope.Config.CacheOptions()
// actually restricts what a controller-runtime cache surfaces, verified against a
// real kube-apiserver (envtest). N1's unit tests cover the pure Parse/CacheOptions
// mapping and N2 covers helm renders; neither exercises the runtime list/watch
// filtering the apiserver performs. Cache scoping is enforced at the apiserver
// list/watch layer (namespace path for allow-list, metadata.namespace field
// selector for deny-list), so envtest (real apiserver + etcd, no kubelet) is the
// correct and sufficient substrate.
var _ = Describe("watchscope cache scoping (envtest)", func() {
	const crName = "scoped-cr"

	var (
		nsAllowA string // watched (allow-list) / non-ignored (deny-list)
		nsAllowB string // NOT watched (allow-list) / non-ignored (deny-list)
		nsDenyC  string // non-ignored (allow-list N/A) / IGNORED (deny-list)
	)

	BeforeEach(func() {
		// Unique suffix keeps namespaces collision-free even if the suite is
		// run with parallel Ginkgo processes.
		sfx := utilrand.String(6)
		nsAllowA = "ws-allow-a-" + sfx
		nsAllowB = "ws-allow-b-" + sfx
		nsDenyC = "ws-deny-c-" + sfx

		for _, ns := range []string{nsAllowA, nsAllowB, nsDenyC} {
			createNamespace(ns)
			createLittleRed(crName, ns)
		}
	})

	AfterEach(func() {
		for _, ns := range []string{nsAllowA, nsAllowB, nsDenyC} {
			cr := &littleredv1alpha1.LittleRed{
				ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: ns},
			}
			Expect(k8sClient.Delete(ctx, cr)).To(Succeed())
			// Namespaces are not deleted: envtest has no namespace controller to
			// finalize them, so a Delete would leave them stuck Terminating. The
			// unique suffix keeps them out of each other's way.
		}
	})

	It("allow-list surfaces CRs only from watched namespaces", func() {
		cfgScope, err := watchscope.Parse(nsAllowA, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfgScope.Mode).To(Equal(watchscope.ModeAllow))

		c := startScopedCache(cfgScope.CacheOptions())

		// A cluster-wide List through the allow-scoped cache must see only the
		// watched namespace's CR.
		Eventually(func(g Gomega) {
			got := scopedNamespaces(g, c, crName)
			g.Expect(got).To(HaveKey(nsAllowA))
			g.Expect(got).NotTo(HaveKey(nsAllowB), "unwatched namespace leaked into allow-scoped cache")
			g.Expect(got).NotTo(HaveKey(nsDenyC), "unwatched namespace leaked into allow-scoped cache")
		}, 30*time.Second, time.Second).Should(Succeed())
	})

	It("deny-list surfaces CRs from all namespaces except the ignored one", func() {
		cfgScope, err := watchscope.Parse("", nsDenyC)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfgScope.Mode).To(Equal(watchscope.ModeDeny))

		c := startScopedCache(cfgScope.CacheOptions())

		Eventually(func(g Gomega) {
			got := scopedNamespaces(g, c, crName)
			g.Expect(got).To(HaveKey(nsAllowA), "non-ignored namespace missing from deny-scoped cache")
			g.Expect(got).To(HaveKey(nsAllowB), "non-ignored namespace missing from deny-scoped cache")
			g.Expect(got).NotTo(HaveKey(nsDenyC), "ignored namespace leaked into deny-scoped cache")
		}, 30*time.Second, time.Second).Should(Succeed())
	})

	It("unscoped (none) surfaces CRs from all namespaces", func() {
		cfgScope, err := watchscope.Parse("", "")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfgScope.Mode).To(Equal(watchscope.ModeNone))

		c := startScopedCache(cfgScope.CacheOptions())

		Eventually(func(g Gomega) {
			got := scopedNamespaces(g, c, crName)
			g.Expect(got).To(HaveKey(nsAllowA))
			g.Expect(got).To(HaveKey(nsAllowB))
			g.Expect(got).To(HaveKey(nsDenyC))
		}, 30*time.Second, time.Second).Should(Succeed())
	})
})

// startScopedCache builds a controller-runtime cache from the given options
// against the envtest apiserver, starts it in a goroutine (cancelled at spec
// teardown), and waits for the LittleRed informer to sync.
func startScopedCache(opts cache.Options) cache.Cache {
	GinkgoHelper()
	if opts.Scheme == nil {
		opts.Scheme = scheme.Scheme
	}

	c, err := cache.New(cfg, opts)
	Expect(err).NotTo(HaveOccurred())

	cctx, ccancel := context.WithCancel(ctx)
	DeferCleanup(ccancel)

	go func() {
		defer GinkgoRecover()
		Expect(c.Start(cctx)).To(Succeed())
	}()

	// Registering the informer + WaitForCacheSync ensures the initial LIST from
	// the apiserver (already namespace/field-selector-scoped by opts) has been
	// stored before we read.
	_, err = c.GetInformer(cctx, &littleredv1alpha1.LittleRed{})
	Expect(err).NotTo(HaveOccurred())
	Expect(c.WaitForCacheSync(cctx)).To(BeTrue())
	return c
}

// scopedNamespaces lists LittleRed CRs through the scoped cache and returns the
// set of namespaces holding a CR of the given name. Filtering by name isolates
// this assertion from CRs created by other specs in the shared suite.
func scopedNamespaces(g Gomega, c cache.Cache, name string) map[string]bool {
	list := &littleredv1alpha1.LittleRedList{}
	g.Expect(c.List(ctx, list)).To(Succeed())
	out := map[string]bool{}
	for i := range list.Items {
		if list.Items[i].Name == name {
			out[list.Items[i].Namespace] = true
		}
	}
	return out
}

func createNamespace(name string) {
	GinkgoHelper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	Expect(k8sClient.Create(ctx, ns)).To(Succeed())
}

func createLittleRed(name, namespace string) {
	GinkgoHelper()
	cr := &littleredv1alpha1.LittleRed{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	Expect(k8sClient.Create(ctx, cr)).To(Succeed())
}
