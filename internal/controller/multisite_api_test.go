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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// These specs assert the CRD-schema layer of the multi-site validation
// (structural markers + CEL XValidation rules) against a real API server
// (envtest). The controller-side mirror lives in LittleRed.Validate()
// (api/v1alpha1/littlered_multisite_test.go).
var _ = Describe("MultiSite CRD validation", func() {
	var counter int

	newMultiSiteCR := func(mode string, ms *littleredv1alpha1.MultiSiteSpec) *littleredv1alpha1.LittleRed {
		counter++
		lr := &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("ms-crd-%d", counter),
				Namespace: testNamespaceDefault,
			},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:      mode,
				MultiSite: ms,
			},
		}
		if mode == ModeCluster {
			lr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{Shards: 3}
		}
		return lr
	}

	remote := func(name, secret string) littleredv1alpha1.SiteSpec {
		return littleredv1alpha1.SiteSpec{
			Name:                name,
			KubeconfigSecretRef: &littleredv1alpha1.KubeconfigSecretRef{Name: secret},
		}
	}
	hubLocal := func(name string) littleredv1alpha1.SiteSpec {
		return littleredv1alpha1.SiteSpec{Name: name}
	}

	It("accepts a valid dual-site cluster CR and defaults network.mode", func() {
		lr := newMultiSiteCR(ModeCluster, &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{hubLocal("alpha"), remote("beta", "site-beta-kubeconfig")},
			SitesPerShard: 2,
			Network:       littleredv1alpha1.MultiSiteNetworkSpec{Mode: "pod-routed"},
		})
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, lr) })
	})

	It("rejects multiSite outside cluster mode", func() {
		lr := newMultiSiteCR(ModeStandalone, &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 2,
		})
		err := k8sClient.Create(ctx, lr)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("multiSite"))
	})

	It("rejects fewer than 2 sites", func() {
		lr := newMultiSiteCR(ModeCluster, &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a")},
			SitesPerShard: 2,
		})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("rejects duplicate site names", func() {
		lr := newMultiSiteCR(ModeCluster, &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("alpha", "b")},
			SitesPerShard: 2,
		})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("rejects sitesPerShard below 2", func() {
		lr := newMultiSiteCR(ModeCluster, &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 1,
		})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("rejects sitesPerShard above the number of sites", func() {
		lr := newMultiSiteCR(ModeCluster, &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 3,
		})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("rejects a second hub-local site", func() {
		lr := newMultiSiteCR(ModeCluster, &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{hubLocal("alpha"), hubLocal("beta")},
			SitesPerShard: 2,
		})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("rejects the future network mode nodeport", func() {
		lr := newMultiSiteCR(ModeCluster, &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 2,
			Network:       littleredv1alpha1.MultiSiteNetworkSpec{Mode: "nodeport"},
		})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("rejects an empty kubeconfigSecretRef name", func() {
		lr := newMultiSiteCR(ModeCluster, &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", ""), remote("beta", "b")},
			SitesPerShard: 2,
		})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	// --- Topology immutability (LR-032) ---------------------------------------
	//
	// The multi-site topology is FIXED for an instance's lifetime. The round-robin
	// placement (shard K -> sites (K+j) mod S) is INDEX-based, so adding, removing
	// or reordering a site remaps shards onto different sites — a RELOCATION of
	// live members, not an addition. Cluster storage is EmptyDir and there is no
	// member-relocation mechanism (LR-025's replicate-then-failover primitive is
	// wired for the legacy migration only), so a relocation can only orphan data.
	// Topology growth was never a stated requirement (MULTI_SITE_DESIGN.md §4
	// non-goal); these transition rules make that non-goal enforceable at
	// ADMISSION, before the operator can half-build the new topology. The runtime
	// LR-028 scale-down guard stays as defence in depth for drift it can observe.
	create := func(ms *littleredv1alpha1.MultiSiteSpec) *littleredv1alpha1.LittleRed {
		lr := newMultiSiteCR(ModeCluster, ms)
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, lr) })
		return lr
	}

	It("rejects adding a site to an existing instance", func() {
		lr := create(&littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 2,
		})
		lr.Spec.MultiSite.Sites = append(lr.Spec.MultiSite.Sites, remote("gamma", "c"))
		err := k8sClient.Update(ctx, lr)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("immutable"))
	})

	It("rejects removing a site from an existing instance", func() {
		lr := create(&littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b"), remote("gamma", "c")},
			SitesPerShard: 2,
		})
		lr.Spec.MultiSite.Sites = lr.Spec.MultiSite.Sites[:2]
		Expect(k8sClient.Update(ctx, lr)).NotTo(Succeed())
	})

	It("rejects reordering the site list (placement is index-based)", func() {
		lr := create(&littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 2,
		})
		lr.Spec.MultiSite.Sites = []littleredv1alpha1.SiteSpec{remote("beta", "b"), remote("alpha", "a")}
		Expect(k8sClient.Update(ctx, lr)).NotTo(Succeed())
	})

	It("rejects changing sitesPerShard", func() {
		lr := create(&littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b"), remote("gamma", "c")},
			SitesPerShard: 2,
		})
		lr.Spec.MultiSite.SitesPerShard = 3
		Expect(k8sClient.Update(ctx, lr)).NotTo(Succeed())
	})

	It("rejects changing cluster.shards while multiSite is set", func() {
		lr := create(&littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 2,
		})
		lr.Spec.Cluster.Shards = 6
		Expect(k8sClient.Update(ctx, lr)).NotTo(Succeed())
	})

	It("rejects adding multiSite to an existing single-site instance", func() {
		lr := create(nil)
		lr.Spec.MultiSite = &littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 2,
		}
		Expect(k8sClient.Update(ctx, lr)).NotTo(Succeed())
	})

	It("rejects removing multiSite from an existing stretched instance", func() {
		lr := create(&littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 2,
		})
		lr.Spec.MultiSite = nil
		Expect(k8sClient.Update(ctx, lr)).NotTo(Succeed())
	})

	// LR-033: sitesPerShard is REQUIRED, so there is no dynamic default for the
	// operator to persist behind the user's back — the stored spec always equals the
	// effective value, and the immutability rule above is plain equality.
	It("rejects a CR without sitesPerShard", func() {
		lr := newMultiSiteCR(ModeCluster, &littleredv1alpha1.MultiSiteSpec{
			Sites: []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
		})
		err := k8sClient.Create(ctx, lr)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("sitesPerShard"))
	})

	// Immutability pins the site NAMES and their ORDER — the placement inputs.
	// Rotating a site's kubeconfig Secret is an operational necessity and stays open.
	It("allows changing a site's kubeconfigSecretRef", func() {
		lr := create(&littleredv1alpha1.MultiSiteSpec{
			Sites:         []littleredv1alpha1.SiteSpec{remote("alpha", "a"), remote("beta", "b")},
			SitesPerShard: 2,
		})
		lr.Spec.MultiSite.Sites[1].KubeconfigSecretRef = &littleredv1alpha1.KubeconfigSecretRef{Name: "site-beta-rotated"}
		Expect(k8sClient.Update(ctx, lr)).To(Succeed())
	})
})
