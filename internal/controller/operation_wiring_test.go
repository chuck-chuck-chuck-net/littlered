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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// The M3.1 wiring guard for ADR-020: the declared-operation branch, driven through the
// real reconcileSentinelCluster rather than through planOperation.
//
// It reuses the Rule N harness (scriptedSentinel / fakeRedisMaster,
// stale_master_name_wiring_test.go) because the rename IS registry v1's only member and
// Rule 0 + Rule N ARE its driver. The three tiers are the three rows whose behaviour
// nothing else can pin end to end:
//
//   - row 3, per-candidate seeding — an already-initialized instance with no ack row is
//     SEEDED, never run. Without it every instance in a fleet declares an operation the
//     moment the operator is upgraded.
//   - row 7, the transition guard — the driver converging is NOT the operation being
//     over. Rule N reports Converged the moment the Sentinels agree, which is well
//     before the Redis StatefulSet finishes rolling, and acknowledging there hands the
//     exit edge straight into the churn LR-050 is about.
//   - row 8, completion — acknowledged only once the driver is done AND the instance's
//     own StatefulSets have settled.
//
// Shared literals for this file, named so the package's goconst budget is not spent
// on fixture noise.
const (
	opTestImage    = "redis:8"
	opTestRevision = "rev-1"
)

var _ = Describe("ADR-020 declared operations (sentinel mode)", func() {
	const (
		desired  = "ops-a.cache"
		masterIP = "127.0.0.20"
	)
	var (
		reconciler *LittleRedReconciler
		recorder   *events.FakeRecorder
		lr         *littleredv1alpha1.LittleRed
		sentinelIP = []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}
	)

	// opStatefulSets gives the instance both of the StatefulSets it owns, in a chosen
	// settledness. Both matter: ADR-020's Settled input is "ALL of this instance's own
	// StatefulSets", and in sentinel mode that is the Redis one AND the Sentinel one.
	opStatefulSets := func(redisSettled, sentinelSettled bool) {
		mk := func(name string, settled bool, labels map[string]string) {
			replicas := int32(3)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: lr.Namespace},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{MatchLabels: labels},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: labels},
						Spec: corev1.PodSpec{Containers: []corev1.Container{
							{Name: ComponentRedis, Image: opTestImage},
						}},
					},
					ServiceName: name,
				},
			}
			Expect(k8sClient.Create(ctx, sts)).To(Succeed())
			sts.Status = appsv1.StatefulSetStatus{
				ObservedGeneration: sts.Generation,
				Replicas:           3,
				ReadyReplicas:      3,
				UpdatedReplicas:    3,
				CurrentRevision:    opTestRevision,
				UpdateRevision:     opTestRevision,
			}
			if !settled {
				sts.Status.ReadyReplicas = 2
				sts.Status.UpdatedReplicas = 1
				sts.Status.UpdateRevision = opTestRevision + "-next"
			}
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())
		}
		mk(statefulSetName(lr), redisSettled, redisSelectorLabels(lr))
		mk(sentinelStatefulSetName(lr), sentinelSettled, sentinelSelectorLabels(lr))
	}

	// acks reads the completion record back off the API server.
	acks := func() map[string]string {
		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		out := map[string]string{}
		for _, a := range latest.Status.AcknowledgedOperations {
			out[a.Name] = a.Fingerprint
		}
		return out
	}

	operationStatus := func() *littleredv1alpha1.OperationStatus {
		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		return latest.Status.Operation
	}

	operationCondition := func() *metav1.Condition {
		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		return meta.FindStatusCondition(latest.Status.Conditions, littleredv1alpha1.ConditionOperationInProgress)
	}

	BeforeEach(func() {
		recorder = events.NewFakeRecorder(64)
		reconciler = &LittleRedReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}

		lr = &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "ops-", Namespace: "default"},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:     ModeSentinel,
				Sentinel: &littleredv1alpha1.SentinelSpec{Quorum: 2, MasterName: desired},
			},
		}
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())
		lr.Status.Phase = littleredv1alpha1.PhaseRunning
		lr.Status.BootstrapRequired = false
		Expect(k8sClient.Status().Update(ctx, lr)).To(Succeed())

		makePod := func(name, ip string, labels map[string]string) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: lr.Name + "-" + name, Namespace: lr.Namespace, Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: ComponentRedis, Image: opTestImage},
				}},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			pod.Status.PodIP = ip
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: ComponentRedis, Ready: true}}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}
		makePod("redis-0", masterIP, redisSelectorLabels(lr))
		for i, ip := range sentinelIP {
			makePod(fmt.Sprintf("sentinel-%d", i), ip, sentinelSelectorLabels(lr))
		}
		fakeRedisMaster(GinkgoT(), masterIP)
	})

	// startSentinels binds the three Sentinels for THIS spec. Deliberately not in
	// BeforeEach — only one listener can hold 127.0.0.x:26379 at a time.
	startSentinels := func() {
		for _, ip := range sentinelIP {
			newScriptedSentinelNamed(GinkgoT(), ip, [][2]string{{desired, masterIP}})
		}
	}

	// stampAck writes a completion record for the rename at a given effective name, i.e.
	// "the operator finished carrying out a rename to THIS value". Stamping the value the
	// spec no longer asks for is exactly what an unfinished rename looks like.
	stampAck := func(value string) {
		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		latest.Status.AcknowledgedOperations = []littleredv1alpha1.OperationAck{{
			Name:           opRename,
			Fingerprint:    littleredv1alpha1.OperationFingerprint(latest.UID, opRename, value),
			AcknowledgedAt: metav1.Now(),
		}}
		Expect(k8sClient.Status().Update(ctx, latest)).To(Succeed())
		lr.Status.AcknowledgedOperations = latest.Status.AcknowledgedOperations
	}

	It("seeds an already-initialized instance instead of declaring an operation it never asked for (row 3)", func() {
		// The fleet-upgrade case: the operator gains the registry, the instance has no
		// ack row, and its spec value is the one it has been running under all along.
		// Declaring a rename here would suppress healing on every instance in a fleet at
		// the moment of an operator upgrade.
		opStatefulSets(true, true)
		startSentinels()

		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())

		Expect(acks()).To(HaveKeyWithValue(opRename,
			littleredv1alpha1.OperationFingerprint(lr.UID, opRename, desired)),
			"a candidate with no ack row on an initialized instance is SEEDED, never run")
		Expect(operationStatus()).To(BeNil(), "seeding declares no operation")
		c := operationCondition()
		Expect(c).NotTo(BeNil(), "the mechanism must be observable even when it declares nothing")
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
	})

	It("holds the acknowledgment while the instance is still rolling, even though the driver has converged (row 7)", func() {
		// The transition guard. Rule N converges the moment the Sentinels agree — here
		// they already monitor exactly the desired name, so it reports Converged on the
		// first pass — but the Redis StatefulSet is mid-roll. Acknowledging here hands
		// the exit edge straight into the churn LR-050 is about.
		opStatefulSets(false, true)
		startSentinels()
		stampAck("the-previous-name")

		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())

		Expect(acks()).To(HaveKeyWithValue(opRename,
			littleredv1alpha1.OperationFingerprint(lr.UID, opRename, "the-previous-name")),
			"the acknowledgment must NOT advance while our own StatefulSets are unsettled")

		op := operationStatus()
		Expect(op).NotTo(BeNil(), "a declared, unfinished operation must be reported")
		Expect(op.Name).To(Equal(opRename))
		Expect(op.Reason).To(Equal(operationReasonRunning))

		c := operationCondition()
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(operationReasonRunning))
	})

	It("acknowledges only once the driver is done AND both StatefulSets have settled (row 8)", func() {
		opStatefulSets(true, true)
		startSentinels()
		stampAck("the-previous-name")

		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())

		Expect(acks()).To(HaveKeyWithValue(opRename,
			littleredv1alpha1.OperationFingerprint(lr.UID, opRename, desired)),
			"completion is the driver converging AND the instance settling")
		Expect(operationStatus()).To(BeNil(), "a completed operation is no longer reported as in progress")
		c := operationCondition()
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(operationReasonConverged))
	})

	It("does not acknowledge while only the SENTINEL StatefulSet is still rolling (row 7, the sibling nobody reads)", func() {
		// Settled means ALL of this instance's own StatefulSets. Reading only the Redis
		// one — the object LR-050's attribution gate happens to read — would acknowledge
		// a rename while the Sentinels that carry the name are still being replaced.
		opStatefulSets(true, false)
		startSentinels()
		stampAck("the-previous-name")

		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())

		Expect(acks()).To(HaveKeyWithValue(opRename,
			littleredv1alpha1.OperationFingerprint(lr.UID, opRename, "the-previous-name")),
			"the Sentinel StatefulSet is one of ours too")
	})
})
