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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/watchscope"
)

// The site kubeconfig Secrets live in the OPERATOR's own namespace, which is not
// (and need not be) part of the operator's declared watch scope (ADR-014). The
// manager's client is cache-backed for every type including Secrets
// (client.CacheOptions.DisableFor is empty in cmd/littlered), so reading them
// through it is a read outside the cache's scope: the allow-list cache rejects
// the namespace outright, the deny-list cache silently returns NotFound. Both
// surface as "site unreachable" (resolveSites folds the error into the site
// observation), so a namespace-scoped operator never resolves any site and never
// says why.
//
// envtest is the honest substrate here: the failure is produced by
// controller-runtime's real cache against a real apiserver's list/watch scoping,
// not by anything a fake client would model. The cheap sibling guard (a fake
// scoped client) lives in site_registry_test.go.
var _ = Describe("site kubeconfig Secret read under a scoped cache (envtest)", func() {
	const secretName = "site-alpha-kubeconfig"

	var (
		opNS     string // the operator's own namespace — holds the site Secrets, NOT watched
		tenantNS string // the watched namespace — holds the LittleRed CR
		lr       *littleredv1alpha1.LittleRed
		site     littleredv1alpha1.SiteSpec
	)

	BeforeEach(func() {
		sfx := utilrand.String(6)
		opNS = "sr-operator-" + sfx
		tenantNS = "sr-tenant-" + sfx
		createNamespace(opNS)
		createNamespace(tenantNS)

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: opNS},
			Data:       map[string][]byte{siteKubeconfigSecretKey: []byte("apiVersion: v1\nkind: Config\n")},
		})).To(Succeed())

		lr = &littleredv1alpha1.LittleRed{}
		lr.Name = "ms"
		lr.Namespace = tenantNS
		site = littleredv1alpha1.SiteSpec{
			Name:                siteAlpha,
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: secretName},
		}
	})

	// registryOverScope wires the registry the way the manager does: hubClient is
	// a cache-backed client over a cache built from the given watchscope options.
	registryOverScope := func(opts cache.Options) *siteClientRegistry {
		GinkgoHelper()
		c := startScopedCache(opts)

		hub, err := client.New(cfg, client.Options{
			Scheme: scheme.Scheme,
			Cache:  &client.CacheOptions{Reader: c},
		})
		Expect(err).NotTo(HaveOccurred())

		// Production passes mgr.GetAPIReader(); an uncached client over the same
		// rest.Config is exactly what the manager hands out for it.
		apiReader, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
		Expect(err).NotTo(HaveOccurred())

		reg := newSiteClientRegistry(hub, apiReader, scheme.Scheme, make(chan event.GenericEvent, 1))
		reg.operatorNamespace = func() (string, error) { return opNS, nil }
		reg.newRemote = func(name string, _ []byte, _ string, _ types.NamespacedName, _ chan<- event.GenericEvent) (SiteClient, error) {
			return &fakeSiteClient{name: name}, nil
		}
		return reg
	}

	It("resolves the site with an allow-list scope that excludes the operator namespace", func() {
		cfgScope, err := watchscope.Parse(tenantNS, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfgScope.Mode).To(Equal(watchscope.ModeAllow))

		sc, err := registryOverScope(cfgScope.CacheOptions()).siteClient(ctx, lr, site)
		Expect(err).NotTo(HaveOccurred(),
			"the operator must read its own namespace's site kubeconfig Secret regardless of the watch scope")
		Expect(sc).NotTo(BeNil())
	})

	It("resolves the site with a deny-list scope that ignores the operator namespace", func() {
		cfgScope, err := watchscope.Parse("", opNS)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfgScope.Mode).To(Equal(watchscope.ModeDeny))

		sc, err := registryOverScope(cfgScope.CacheOptions()).siteClient(ctx, lr, site)
		Expect(err).NotTo(HaveOccurred(),
			"the operator must read its own namespace's site kubeconfig Secret regardless of the watch scope")
		Expect(sc).NotTo(BeNil())
	})

	// The specs above build the manager's two clients by hand. This one takes the
	// production wiring itself: a real manager with a scoped cache, run through
	// the real SetupWithManager, so the reader under test is literally the one
	// mgr.GetAPIReader() hands the registry. The manager is never started — the
	// APIReader is a direct client and needs no cache sync (mgr.GetClient() would
	// block here, which is precisely why it is the wrong reader for this Get).
	It("SetupWithManager wires the manager's APIReader as the site Secret reader", func() {
		cfgScope, err := watchscope.Parse(tenantNS, "")
		Expect(err).NotTo(HaveOccurred())

		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 scheme.Scheme,
			Cache:                  cfgScope.CacheOptions(),
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			LeaderElection:         false,
		})
		Expect(err).NotTo(HaveOccurred())

		r := &LittleRedReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
		Expect(r.SetupWithManager(mgr)).To(Succeed())
		Expect(r.sites).NotTo(BeNil())
		Expect(r.sites.secretReader).NotTo(BeNil(), "SetupWithManager must supply an uncached secret reader")

		// The out-of-scope read succeeds — which a cache-backed client cannot do.
		got := &corev1.Secret{}
		Expect(r.sites.secretReader.Get(ctx, types.NamespacedName{Namespace: opNS, Name: secretName}, got)).
			To(Succeed(), "the wired reader must reach the operator's own namespace, outside the watch scope")
		Expect(got.Data).To(HaveKey(siteKubeconfigSecretKey))
	})

	// Control: the unscoped (default) operator was never broken. Green from birth;
	// it exists so a fix that broke the cluster-scoped path could not pass.
	It("resolves the site unscoped (control)", func() {
		cfgScope, err := watchscope.Parse("", "")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfgScope.Mode).To(Equal(watchscope.ModeNone))

		sc, err := registryOverScope(cfgScope.CacheOptions()).siteClient(ctx, lr, site)
		Expect(err).NotTo(HaveOccurred())
		Expect(sc).NotTo(BeNil())
	})
})
