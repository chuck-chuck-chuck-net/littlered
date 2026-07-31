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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// Failover-mode envtest (ADR-011, M4): the reconcile flow creates the
// Sentinel-free resource set, and bootstrap stamps the initial assignment
// annotations (redis-0 master, the rest replicas, all at epoch 1) — the
// operator-assignment startup protocol's first hand-off.
var _ = Describe("Failover mode reconciliation", func() {
	const resourceName = "failover-m4"
	const revision = "failover-m4-redis-abc123"

	ctx := context.Background()
	nn := types.NamespacedName{Name: resourceName, Namespace: testNamespaceDefault}

	var recorder *record.FakeRecorder
	var reconciler *LittleRedReconciler

	podName := func(i int) string { return fmt.Sprintf("%s-redis-%d", resourceName, i) }
	podIP := func(i int) string { return fmt.Sprintf("192.0.2.%d", 10+i) } // TEST-NET-1: never routable

	reconcileOnce := func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())
	}

	// drainEvents returns all events currently buffered in the fake recorder.
	drainEvents := func() []string {
		var evs []string
		for {
			select {
			case e := <-recorder.Events:
				evs = append(evs, e)
			default:
				return evs
			}
		}
	}

	BeforeEach(func() {
		recorder = record.NewFakeRecorder(64)
		reconciler = &LittleRedReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}
	})

	AfterEach(func() {
		// Delete the CR and reconcile once so the finalizer is removed.
		lr := &littleredv1alpha1.LittleRed{}
		if err := k8sClient.Get(ctx, nn, lr); err == nil {
			Expect(k8sClient.Delete(ctx, lr)).To(Succeed())
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		}
		// Delete the manually created data pods.
		for i := range 3 {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName(i), Namespace: testNamespaceDefault}}
			_ = k8sClient.Delete(ctx, pod)
		}
	})

	It("creates the Sentinel-free resource set and bootstraps via assignment annotations", func() {
		By("creating a failover-mode CR")
		lr := &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: testNamespaceDefault},
			Spec:       littleredv1alpha1.LittleRedSpec{Mode: ModeFailover},
		}
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())

		By("reconciling: finalizer, bootstrap flag, resources")
		reconcileOnce() // adds finalizer, requeues
		reconcileOnce() // arms bootstrapRequired, creates resources

		By("asserting bootstrapRequired is armed")
		Expect(k8sClient.Get(ctx, nn, lr)).To(Succeed())
		Expect(lr.Status.BootstrapRequired).To(BeTrue())

		By("asserting the redis StatefulSet: 1 + replicas(2) pods and the downward-API volume")
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-redis", Namespace: testNamespaceDefault}, sts)).To(Succeed())
		Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
		volNames := []string{}
		for _, v := range sts.Spec.Template.Spec.Volumes {
			volNames = append(volNames, v.Name)
		}
		Expect(volNames).To(ContainElement(volNamePodInfo))

		By("asserting master + replicas Services, ConfigMap, PDB exist")
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: testNamespaceDefault}, svc)).To(Succeed())
		Expect(svc.Spec.Selector).To(HaveKeyWithValue(LabelRole, RoleMaster))
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-replicas", Namespace: testNamespaceDefault}, &corev1.Service{})).To(Succeed())
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: testNamespaceDefault}, cm)).To(Succeed())
		Expect(cm.Data).To(HaveKey(fileRedisConf))
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-redis-pdb", Namespace: testNamespaceDefault}, &policyv1.PodDisruptionBudget{})).To(Succeed())

		By("asserting NO Sentinel resources are created")
		err := k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-sentinel", Namespace: testNamespaceDefault}, &appsv1.StatefulSet{})
		Expect(errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-sentinel-config", Namespace: testNamespaceDefault}, &corev1.ConfigMap{})
		Expect(errors.IsNotFound(err)).To(BeTrue())

		By("asserting the experimental-mode warning event was emitted")
		experimental := false
		for _, e := range drainEvents() {
			if strings.Contains(e, reasonExperimentalMode) && strings.Contains(e, "experimental") {
				experimental = true
			}
		}
		Expect(experimental).To(BeTrue(), "expected an ExperimentalMode warning event on first reconcile")

		By("simulating the StatefulSet controller: pods with IPs + matching revision")
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-redis", Namespace: testNamespaceDefault}, sts)).To(Succeed())
		sts.Status.CurrentRevision = revision
		sts.Status.UpdateRevision = revision
		sts.Status.Replicas = 3
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())

		Expect(k8sClient.Get(ctx, nn, lr)).To(Succeed())
		labels := redisSelectorLabels(lr)
		labels["controller-revision-hash"] = revision
		for i := range 3 {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName(i),
					Namespace: testNamespaceDefault,
					Labels:    labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: ComponentRedis, Image: "redis:8.4.2"}},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: podIP(i),
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  ComponentRedis,
					Ready: true,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
				}},
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}

		By("reconciling: bootstrap stamps the initial assignment set")
		reconcileOnce()

		By("asserting redis-0 is stamped master at epoch 1")
		pod0 := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podName(0), Namespace: testNamespaceDefault}, pod0)).To(Succeed())
		Expect(pod0.Annotations).To(HaveKeyWithValue(AnnotationAssignedRole, RoleMaster))
		Expect(pod0.Annotations).To(HaveKeyWithValue(AnnotationAssignmentEpoch, "1"))
		Expect(pod0.Annotations).To(HaveKeyWithValue(AnnotationAssignedMasterIP, ""))

		By("asserting the replicas are stamped replica-of-redis-0 at the same epoch")
		for i := 1; i < 3; i++ {
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podName(i), Namespace: testNamespaceDefault}, pod)).To(Succeed())
			Expect(pod.Annotations).To(HaveKeyWithValue(AnnotationAssignedRole, RoleReplica))
			Expect(pod.Annotations).To(HaveKeyWithValue(AnnotationAssignedMasterIP, podIP(0)))
			Expect(pod.Annotations).To(HaveKeyWithValue(AnnotationAssignmentEpoch, "1"))
		}

		By("asserting bootstrapRequired is cleared and the epoch is mirrored to status")
		Expect(k8sClient.Get(ctx, nn, lr)).To(Succeed())
		Expect(lr.Status.BootstrapRequired).To(BeFalse())
		Expect(lr.Status.Failover).NotTo(BeNil())
		Expect(lr.Status.Failover.AssignmentEpoch).To(Equal(int64(1)))

		By("asserting a second reconcile is idempotent: no epoch churn while the transition is unsettled")
		reconcileOnce()
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podName(0), Namespace: testNamespaceDefault}, pod0)).To(Succeed())
		Expect(pod0.Annotations).To(HaveKeyWithValue(AnnotationAssignmentEpoch, "1"))
		Expect(pod0.Annotations).To(HaveKeyWithValue(AnnotationAssignedRole, RoleMaster))
	})

	// CEL admission for spec.failover (owed from M2, which could only assert the
	// rule's presence in the CRD YAML — this exercises the apiserver's actual
	// admission behavior, mirroring the issue-#61 tests for cluster/sentinel).
	Context("When validating the failover spec block (CEL admission)", func() {
		It("rejects spec.failover when mode is not failover", func() {
			lr := &littleredv1alpha1.LittleRed{
				ObjectMeta: metav1.ObjectMeta{Name: "mismatch-failover", Namespace: testNamespaceDefault},
				Spec: littleredv1alpha1.LittleRedSpec{
					Mode:     ModeStandalone,
					Failover: &littleredv1alpha1.FailoverSpec{},
				},
			}
			err := k8sClient.Create(ctx, lr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.failover may only be set when spec.mode is 'failover'"))
		})

		It("allows spec.failover with mode failover", func() {
			replicas := int32(2)
			lr := &littleredv1alpha1.LittleRed{
				ObjectMeta: metav1.ObjectMeta{Name: "match-failover", Namespace: testNamespaceDefault},
				Spec: littleredv1alpha1.LittleRedSpec{
					Mode:     ModeFailover,
					Failover: &littleredv1alpha1.FailoverSpec{Replicas: &replicas},
				},
			}
			Expect(k8sClient.Create(ctx, lr)).To(Succeed())
			Expect(k8sClient.Delete(ctx, lr)).To(Succeed())
		})
	})
})
