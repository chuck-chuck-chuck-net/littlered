//go:build e2e
// +build e2e

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

package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	clik8s "github.com/chuck-chuck-chuck-net/littlered/internal/cli/k8s"
)

// Metadata inheritance (ADR-021) is unit-tested at the builder level; what only a live
// cluster shows is that the labels survive the full round trip — operator applies, API
// server stores, kubelet stamps them on running pods — so a scrape config selecting on
// them actually matches. That, and that a custom spec.appName produces a workload which
// reconciles to Running rather than one whose StatefulSet the API server rejects for a
// selector/template mismatch.
//
// The round trip runs in ALL FOUR modes, following security_test.go's pattern. That is
// not redundancy with the builder-level table: every mode has its own StatefulSet builder
// and its own pod template, and failover mode's went unwired for exactly as long as the
// coverage was "one mode stands in for the rest" (see the ADR-021 addendum). The appName
// tiers below stay single-mode — they exercise API-server behaviour, which is mode-neutral.

// metadataModeCR returns a CR in the given mode carrying the inherited metadata, plus
// the name of a StatefulSet that mode is expected to produce.
func metadataModeCR(mode, name string, labels, annotations map[string]string) (*littleredv1alpha1.LittleRed, string) {
	cr := &littleredv1alpha1.LittleRed{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testNamespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: littleredv1alpha1.LittleRedSpec{Mode: mode},
	}
	// The Redis StatefulSet is "<name>-redis" in every mode but cluster, which has one
	// per shard.
	sts := name + "-redis"
	switch mode {
	case "cluster":
		replicas := clusterReplicasPerShard
		cr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{ReplicasPerShard: &replicas}
		sts = name + "-shard-0"
	case "sentinel":
		// masterName is required and must be unique per pod network (pillar 3.7,
		// LR-039); without it the instance never leaves Initializing.
		cr.Spec.Sentinel = &littleredv1alpha1.SentinelSpec{
			MasterName:            e2eMasterName(testNamespace, name),
			Quorum:                2,
			DownAfterMilliseconds: 5000,
			FailoverTimeout:       10000,
		}
	}
	return cr, sts
}

var _ = Describe("LittleRed metadata inheritance", Label("metadata"), func() {
	var k8sClient client.Client
	ctx := context.Background()

	BeforeEach(func() {
		var err error
		k8sClient, _, _, _, err = clik8s.NewClient("")
		Expect(err).NotTo(HaveOccurred())
	})

	waitForRunning := func(name string, timeout time.Duration) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			lr := &littleredv1alpha1.LittleRed{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, lr)).To(Succeed())
			g.Expect(lr.Status.Phase).To(Equal(littleredv1alpha1.PhaseRunning))
		}, timeout, 5*time.Second).Should(Succeed())
	}

	for _, mode := range []string{"standalone", "sentinel", "failover", "cluster"} {
		mode := mode // capture range variable

		Context("in "+mode+" mode", modeLabel(mode), func() {
			It("propagates CR labels and annotations to the pods and the StatefulSet", func() {
				labels := map[string]string{"team": "payments", "environment": "e2e"}
				annotations := map[string]string{"owner": "team-payments@example.com"}
				cr, stsName := metadataModeCR(mode, "meta-inherit-"+mode, labels, annotations)

				Expect(k8sClient.Create(ctx, cr)).To(Succeed())
				DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

				waitForRunning(cr.Name, 5*time.Minute)

				By("finding the pods by an inherited label alone")
				pods := &corev1.PodList{}
				Eventually(func(g Gomega) {
					g.Expect(k8sClient.List(ctx, pods,
						client.InNamespace(testNamespace),
						client.MatchingLabels{"team": "payments", "app.kubernetes.io/instance": cr.Name},
					)).To(Succeed())
					g.Expect(pods.Items).NotTo(BeEmpty())
				}, 2*time.Minute, 5*time.Second).Should(Succeed())

				// Sentinel mode must return the sentinel pods here too, not just the
				// Redis ones: buildSentinelStatefulSet was historically the builder that
				// applied no pod-template metadata at all.
				if mode == "sentinel" {
					var sentinels int
					for _, pod := range pods.Items {
						if pod.Labels["app.kubernetes.io/component"] == "sentinel" {
							sentinels++
						}
					}
					Expect(sentinels).To(BeNumerically(">", 0),
						"no sentinel pod carried the inherited label")
				}

				for _, pod := range pods.Items {
					Expect(pod.Labels).To(HaveKeyWithValue("environment", "e2e"))
					Expect(pod.Annotations).To(HaveKeyWithValue("owner", "team-payments@example.com"))
					// The operator's own labels must have survived the merge intact, or
					// the StatefulSet could not have adopted this pod at all.
					Expect(pod.Labels).To(HaveKeyWithValue("app.kubernetes.io/name", littleredv1alpha1.DefaultAppName))
				}

				By("checking the StatefulSet's own metadata")
				sts := &appsv1.StatefulSet{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: stsName, Namespace: testNamespace}, sts)).To(Succeed())
				Expect(sts.Labels).To(HaveKeyWithValue("team", "payments"))
				Expect(sts.Annotations).To(HaveKeyWithValue("owner", "team-payments@example.com"))
			})
		})
	}

	// The appName tiers exercise API-server behaviour (selector agreement, the CEL
	// immutability rule), which is mode-neutral; standalone is the cheapest carrier.
	Context("spec.appName", modeLabel("standalone"), func() {
		It("brings up an instance with a custom spec.appName", func() {
			const customAppName = "valkey-store"
			cr := &littleredv1alpha1.LittleRed{
				ObjectMeta: metav1.ObjectMeta{Name: "meta-appname", Namespace: testNamespace},
				Spec: littleredv1alpha1.LittleRedSpec{
					Mode:    "standalone",
					AppName: customAppName,
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			// Reaching Running is the assertion that matters: a StatefulSet whose pod
			// template disagreed with its selector would have been rejected outright.
			waitForRunning(cr.Name, 3*time.Minute)

			// The Redis StatefulSet is "<name>-redis"; the Service is "<name>".
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cr.Name + "-redis", Namespace: testNamespace}, sts)).To(Succeed())
			Expect(sts.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/name", customAppName))
			Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("app.kubernetes.io/name", customAppName))

			By("checking the Service selector moved with it, so clients still resolve")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: testNamespace}, svc)).To(Succeed())
			Expect(svc.Spec.Selector).To(HaveKeyWithValue("app.kubernetes.io/name", customAppName))

			Eventually(func(g Gomega) {
				endpoints := &corev1.Endpoints{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: testNamespace}, endpoints)).To(Succeed())
				g.Expect(endpoints.Subsets).NotTo(BeEmpty())
				g.Expect(endpoints.Subsets[0].Addresses).NotTo(BeEmpty())
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("rejects a change to spec.appName", func() {
			cr := &littleredv1alpha1.LittleRed{
				ObjectMeta: metav1.ObjectMeta{Name: "meta-appname-immutable", Namespace: testNamespace},
				Spec: littleredv1alpha1.LittleRedSpec{
					Mode:    "standalone",
					AppName: "before",
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			cr.Spec.AppName = "after"
			err := k8sClient.Update(ctx, cr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable"))
		})
	})
})
