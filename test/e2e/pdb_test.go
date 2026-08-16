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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	clik8s "github.com/littlered-operator/littlered-operator/internal/cli/k8s"
)

// pdbCreate returns a pointer to b for the *bool spec.podDisruptionBudget.create field.
func pdbCreate(b bool) *bool { return &b }

var _ = Describe("LittleRed PodDisruptionBudget", Label("pdb"), func() {
	var k8sClient client.Client
	ctx := context.Background()

	BeforeEach(func() {
		var err error
		k8sClient, _, _, _, err = clik8s.NewClient("")
		Expect(err).NotTo(HaveOccurred())
	})

	// getPDB fetches a PDB by name, returning (pdb, found, err).
	// A not-found response is returned as (nil, false, nil).
	getPDB := func(name string) (*policyv1.PodDisruptionBudget, bool, error) {
		pdb := &policyv1.PodDisruptionBudget{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, pdb)
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		return pdb, true, nil
	}

	// waitForRunning waits for a LittleRed CR to reach PhaseRunning.
	waitForRunning := func(name string, timeout time.Duration) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			lr := &littleredv1alpha1.LittleRed{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, lr)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(lr.Status.Phase).To(Equal(littleredv1alpha1.PhaseRunning))
		}, timeout, 5*time.Second).Should(Succeed())
	}

	// newStandaloneCR returns a minimal standalone LittleRed CR.
	newStandaloneCR := func(name string) *littleredv1alpha1.LittleRed {
		return &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec:       littleredv1alpha1.LittleRedSpec{Mode: "standalone"},
		}
	}

	// newSentinelCR returns a minimal sentinel LittleRed CR.
	newSentinelCR := func(name string) *littleredv1alpha1.LittleRed {
		return &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode: "sentinel",
				Sentinel: &littleredv1alpha1.SentinelSpec{
					Quorum:                2,
					DownAfterMilliseconds: 5000,
					FailoverTimeout:       10000,
				},
			},
		}
	}

	// newFailoverCR returns a minimal failover-mode LittleRed CR (ADR-011).
	//
	// spec.failover is deliberately left unset. The CRD only forbids the block in
	// OTHER modes (rule: self.mode == 'failover' || !has(self.failover)), it never
	// requires it here, and failoverSpecOrDefault supplies the defaults — so this
	// also exercises the nil-spec.failover path. The default of 2 replicas means
	// 3 data pods, so a failover instance is redundant by construction and always
	// qualifies for a PDB (unlike standalone or a 0-replica cluster above).
	newFailoverCR := func(name string) *littleredv1alpha1.LittleRed {
		return &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec:       littleredv1alpha1.LittleRedSpec{Mode: "failover"},
		}
	}

	// newClusterCR returns a minimal cluster LittleRed CR.
	newClusterCR := func(name string) *littleredv1alpha1.LittleRed {
		replicas := clusterReplicasPerShard
		return &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode: "cluster",
				Cluster: &littleredv1alpha1.ClusterSpec{
					Shards:           clusterShards,
					ReplicasPerShard: &replicas,
				},
			},
		}
	}

	// clusterPDBNames returns the per-shard PDB names for a cluster CR. Since 0.3.0 the
	// operator creates one PDB per shard ({name}-shard-K-pdb), not a single cluster PDB.
	clusterPDBNames := func(name string) []string {
		names := make([]string, 0, clusterShards)
		for k := 0; k < clusterShards; k++ {
			names = append(names, fmt.Sprintf("%s-shard-%d-pdb", name, k))
		}
		return names
	}

	// ============================================================================
	// Single-pod workloads never get a PDB (PR #92)
	// ============================================================================
	//
	// A PodDisruptionBudget over a single-pod workload is counter-productive: it can
	// only ever block node drains, never protect availability. The operator therefore
	// never creates one for standalone mode or for a cluster with no replicas per
	// shard, regardless of spec.podDisruptionBudget.create.

	Context("Single-pod workloads", func() {
		It("should never create a PDB in standalone mode, even when create=true", func() {
			crName := "pdb-standalone-none"
			pdbName := crName + "-redis-pdb"

			cr := newStandaloneCR(crName)
			cr.Spec.PodDisruptionBudget = littleredv1alpha1.PodDisruptionBudgetSpec{Create: pdbCreate(true)}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			By("waiting for CR to be Running to ensure at least one full reconciliation")
			waitForRunning(crName, 2*time.Minute)

			By("verifying no PDB is created for the single standalone pod")
			Consistently(func(g Gomega) {
				_, found, err := getPDB(pdbName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(found).To(BeFalse(), "standalone is a single pod and must not get a PDB")
			}, 15*time.Second, 5*time.Second).Should(Succeed())
		})

		It("should never create a PDB in cluster mode with replicasPerShard=0", func() {
			crName := "pdb-cluster-zero"

			zero := 0
			cr := &littleredv1alpha1.LittleRed{
				ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: testNamespace},
				Spec: littleredv1alpha1.LittleRedSpec{
					Mode: "cluster",
					Cluster: &littleredv1alpha1.ClusterSpec{
						Shards:           clusterShards,
						ReplicasPerShard: &zero,
					},
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			By("waiting for CR to be Running")
			waitForRunning(crName, 5*time.Minute)

			By("verifying no per-shard PDB is created when shards have no replicas")
			Consistently(func(g Gomega) {
				for _, pdbName := range clusterPDBNames(crName) {
					_, found, err := getPDB(pdbName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(found).To(BeFalse(), "a cluster with no replicas has no redundancy and must not get a PDB")
				}
			}, 15*time.Second, 5*time.Second).Should(Succeed())
		})
	})

	// ============================================================================
	// PDB opt-out (create=false)
	// ============================================================================

	Context("PDB opt-out (create=false)", func() {
		type modeCase struct {
			mode     string
			pdbNames []string
			timeout  time.Duration
		}
		cases := []modeCase{
			{
				mode:     "sentinel",
				pdbNames: []string{"pdb-off-sentinel-redis-pdb", "pdb-off-sentinel-sentinel-pdb"},
				timeout:  3 * time.Minute,
			},
			{
				// Failover mode has data pods only, so exactly one PDB name is in play.
				mode:     "failover",
				pdbNames: []string{"pdb-off-failover-redis-pdb"},
				timeout:  3 * time.Minute,
			},
			{
				mode:     "cluster",
				pdbNames: clusterPDBNames("pdb-off-cluster"),
				timeout:  5 * time.Minute,
			},
		}

		for _, tc := range cases {
			tc := tc
			crName := fmt.Sprintf("pdb-off-%s", tc.mode)

			It(fmt.Sprintf("should not create any PDB in %s mode when create is false", tc.mode), func() {
				AddReportEntry("cr:" + crName)

				var cr *littleredv1alpha1.LittleRed
				switch tc.mode {
				case "sentinel":
					cr = newSentinelCR(crName)
				case "failover":
					cr = newFailoverCR(crName)
				case "cluster":
					cr = newClusterCR(crName)
				}
				cr.Spec.PodDisruptionBudget = littleredv1alpha1.PodDisruptionBudgetSpec{
					Create: pdbCreate(false),
				}
				Expect(k8sClient.Create(ctx, cr)).To(Succeed())
				DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

				By("waiting for CR to be Running to ensure at least one full reconciliation")
				waitForRunning(crName, tc.timeout)

				By("verifying no PDB exists for any expected name")
				for _, pdbName := range tc.pdbNames {
					_, found, err := getPDB(pdbName)
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeFalse(), "PDB %q should not exist when create=false", pdbName)
				}
			})
		}
	})

	// ============================================================================
	// PDB created by default (create unset) for redundant modes
	// ============================================================================

	Context("PDB created by default", func() {
		type modeCase struct {
			mode     string
			pdbNames []string
			// absentPDBNames must NOT exist. Used by failover mode to pin the
			// ADR-011 invariant at the PDB layer: the mode reuses the sentinel
			// data-pod PDB builder, so a regression that also emitted the
			// sentinel-process PDB would otherwise pass unnoticed.
			absentPDBNames []string
			timeout        time.Duration
		}
		cases := []modeCase{
			{
				mode:     "sentinel",
				pdbNames: []string{"pdb-on-sentinel-redis-pdb", "pdb-on-sentinel-sentinel-pdb"},
				timeout:  3 * time.Minute,
			},
			{
				mode:           "failover",
				pdbNames:       []string{"pdb-on-failover-redis-pdb"},
				absentPDBNames: []string{"pdb-on-failover-sentinel-pdb"},
				timeout:        3 * time.Minute,
			},
			{
				mode:     "cluster",
				pdbNames: clusterPDBNames("pdb-on-cluster"),
				timeout:  5 * time.Minute,
			},
		}

		for _, tc := range cases {
			tc := tc
			crName := fmt.Sprintf("pdb-on-%s", tc.mode)

			It(fmt.Sprintf("should create PDB(s) with maxUnavailable=1 in %s mode when create is not set", tc.mode), func() {
				AddReportEntry("cr:" + crName)

				var cr *littleredv1alpha1.LittleRed
				switch tc.mode {
				case "sentinel":
					cr = newSentinelCR(crName)
				case "failover":
					cr = newFailoverCR(crName)
				case "cluster":
					cr = newClusterCR(crName)
				}
				// Do not set PodDisruptionBudget — create defaults to true (CRD default).
				Expect(k8sClient.Create(ctx, cr)).To(Succeed())
				DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

				By("waiting for CR to be Running")
				waitForRunning(crName, tc.timeout)

				By("verifying each PDB exists with the correct default spec")
				for _, pdbName := range tc.pdbNames {
					Eventually(func(g Gomega) {
						pdb, found, err := getPDB(pdbName)
						g.Expect(err).NotTo(HaveOccurred())
						g.Expect(found).To(BeTrue(), "PDB %q should exist", pdbName)

						g.Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil(),
							"PDB %q should have MaxUnavailable set by default", pdbName)
						g.Expect(*pdb.Spec.MaxUnavailable).To(Equal(intstr.FromInt32(1)),
							"PDB %q MaxUnavailable should default to 1", pdbName)
						g.Expect(pdb.Spec.MinAvailable).To(BeNil(),
							"PDB %q MinAvailable should not be set by default", pdbName)
					}, 30*time.Second, 5*time.Second).Should(Succeed())
				}

				By("verifying no PDB exists for any name this mode must not emit")
				for _, pdbName := range tc.absentPDBNames {
					_, found, err := getPDB(pdbName)
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeFalse(), "PDB %q must not exist in %s mode", pdbName, tc.mode)
				}
			})
		}
	})

	// ============================================================================
	// Custom PDB values
	// ============================================================================

	Context("Custom PDB values", func() {
		It("should apply custom maxUnavailable", func() {
			crName := "pdb-custom-max"
			pdbName := crName + "-redis-pdb"
			maxUnavailable := intstr.FromInt32(2)

			cr := newSentinelCR(crName)
			cr.Spec.PodDisruptionBudget = littleredv1alpha1.PodDisruptionBudgetSpec{
				Create:         pdbCreate(true),
				MaxUnavailable: &maxUnavailable,
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			By("waiting for CR to be Running")
			waitForRunning(crName, 3*time.Minute)

			By("verifying PDB has maxUnavailable=2")
			Eventually(func(g Gomega) {
				pdb, found, err := getPDB(pdbName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(found).To(BeTrue())
				g.Expect(*pdb.Spec.MaxUnavailable).To(Equal(intstr.FromInt32(2)))
				g.Expect(pdb.Spec.MinAvailable).To(BeNil())
			}, 30*time.Second, 5*time.Second).Should(Succeed())
		})

		It("should apply custom minAvailable", func() {
			crName := "pdb-custom-min"
			pdbName := crName + "-redis-pdb"
			minAvailable := intstr.FromInt32(1)

			cr := newSentinelCR(crName)
			cr.Spec.PodDisruptionBudget = littleredv1alpha1.PodDisruptionBudgetSpec{
				Create:       pdbCreate(true),
				MinAvailable: &minAvailable,
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			By("waiting for CR to be Running")
			waitForRunning(crName, 3*time.Minute)

			By("verifying PDB has minAvailable=1 and no maxUnavailable")
			Eventually(func(g Gomega) {
				pdb, found, err := getPDB(pdbName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(found).To(BeTrue())
				g.Expect(*pdb.Spec.MinAvailable).To(Equal(intstr.FromInt32(1)))
				g.Expect(pdb.Spec.MaxUnavailable).To(BeNil())
			}, 30*time.Second, 5*time.Second).Should(Succeed())
		})

		It("should prefer minAvailable over maxUnavailable when both are set", func() {
			crName := "pdb-custom-both"
			pdbName := crName + "-redis-pdb"
			minAvailable := intstr.FromInt32(2)
			maxUnavailable := intstr.FromInt32(1)

			cr := newSentinelCR(crName)
			cr.Spec.PodDisruptionBudget = littleredv1alpha1.PodDisruptionBudgetSpec{
				Create:         pdbCreate(true),
				MinAvailable:   &minAvailable,
				MaxUnavailable: &maxUnavailable,
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			By("waiting for CR to be Running")
			waitForRunning(crName, 3*time.Minute)

			By("verifying PDB uses minAvailable=2 and ignores maxUnavailable")
			Eventually(func(g Gomega) {
				pdb, found, err := getPDB(pdbName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(found).To(BeTrue())
				g.Expect(*pdb.Spec.MinAvailable).To(Equal(intstr.FromInt32(2)))
				g.Expect(pdb.Spec.MaxUnavailable).To(BeNil())
			}, 30*time.Second, 5*time.Second).Should(Succeed())
		})

		It("should support percentage value for maxUnavailable", func() {
			crName := "pdb-custom-pct"
			pdbName := crName + "-redis-pdb"
			maxUnavailable := intstr.FromString("50%")

			cr := newSentinelCR(crName)
			cr.Spec.PodDisruptionBudget = littleredv1alpha1.PodDisruptionBudgetSpec{
				Create:         pdbCreate(true),
				MaxUnavailable: &maxUnavailable,
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			By("waiting for CR to be Running")
			waitForRunning(crName, 3*time.Minute)

			By("verifying PDB has maxUnavailable=50%")
			Eventually(func(g Gomega) {
				pdb, found, err := getPDB(pdbName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(found).To(BeTrue())
				g.Expect(*pdb.Spec.MaxUnavailable).To(Equal(intstr.FromString("50%")))
			}, 30*time.Second, 5*time.Second).Should(Succeed())
		})
	})

	// ============================================================================
	// PDB lifecycle
	// ============================================================================

	Context("PDB lifecycle", func() {
		It("should delete PDB when create is toggled from true to false", func() {
			crName := "pdb-toggle-off"
			pdbName := crName + "-redis-pdb"

			cr := newSentinelCR(crName)
			cr.Spec.PodDisruptionBudget = littleredv1alpha1.PodDisruptionBudgetSpec{Create: pdbCreate(true)}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			By("waiting for CR to be Running")
			waitForRunning(crName, 3*time.Minute)

			By("waiting for PDB to exist")
			Eventually(func(g Gomega) {
				_, found, err := getPDB(pdbName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(found).To(BeTrue())
			}, 30*time.Second, 5*time.Second).Should(Succeed())

			By("patching CR to disable PDB creation")
			// Retry on conflict — the CR may have been updated by the controller.
			Eventually(func(g Gomega) {
				current := &littleredv1alpha1.LittleRed{}
				err := k8sClient.Get(ctx, types.NamespacedName{Name: crName, Namespace: testNamespace}, current)
				g.Expect(err).NotTo(HaveOccurred())
				current.Spec.PodDisruptionBudget.Create = pdbCreate(false)
				g.Expect(k8sClient.Update(ctx, current)).To(Succeed())
			}, 30*time.Second, 5*time.Second).Should(Succeed())

			By("verifying PDB is deleted by the operator")
			Eventually(func(g Gomega) {
				_, found, err := getPDB(pdbName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(found).To(BeFalse())
			}, 30*time.Second, 5*time.Second).Should(Succeed())
		})

		It("should recreate PDB after manual deletion", func() {
			crName := "pdb-self-heal"
			pdbName := crName + "-redis-pdb"

			cr := newSentinelCR(crName)
			cr.Spec.PodDisruptionBudget = littleredv1alpha1.PodDisruptionBudgetSpec{Create: pdbCreate(true)}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			By("waiting for CR to be Running")
			waitForRunning(crName, 3*time.Minute)

			By("waiting for PDB to exist")
			Eventually(func(g Gomega) {
				_, found, err := getPDB(pdbName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(found).To(BeTrue())
			}, 30*time.Second, 5*time.Second).Should(Succeed())

			By("deleting the PDB manually")
			pdb, found, err := getPDB(pdbName)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(k8sClient.Delete(ctx, pdb)).To(Succeed())

			By("verifying the operator recreates the PDB")
			Eventually(func(g Gomega) {
				_, found, err := getPDB(pdbName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(found).To(BeTrue())
			}, 30*time.Second, 5*time.Second).Should(Succeed())
		})

		It("should garbage-collect all PDBs when the LittleRed CR is deleted in sentinel mode", func() {
			crName := "pdb-gc-sentinel"
			pdbNames := []string{
				crName + "-redis-pdb",
				crName + "-sentinel-pdb",
			}

			cr := newSentinelCR(crName)
			cr.Spec.PodDisruptionBudget = littleredv1alpha1.PodDisruptionBudgetSpec{Create: pdbCreate(true)}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			// DeferCleanup as a safety net in case the test fails before the explicit delete.
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, cr) })

			By("waiting for CR to be Running")
			waitForRunning(crName, 3*time.Minute)

			By("verifying both PDBs exist")
			for _, pdbName := range pdbNames {
				Eventually(func(g Gomega) {
					_, found, err := getPDB(pdbName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(found).To(BeTrue(), "PDB %q should exist", pdbName)
				}, 30*time.Second, 5*time.Second).Should(Succeed())
			}

			By("deleting the LittleRed CR")
			Expect(k8sClient.Delete(ctx, cr)).To(Succeed())

			By("verifying both PDBs are garbage-collected")
			for _, pdbName := range pdbNames {
				Eventually(func(g Gomega) {
					_, found, err := getPDB(pdbName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(found).To(BeFalse(), "PDB %q should be garbage-collected", pdbName)
				}, 1*time.Minute, 5*time.Second).Should(Succeed())
			}
		})
	})
})
