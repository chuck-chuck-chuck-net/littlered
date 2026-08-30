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
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"

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
	opFieldFlags   = "flags"
	// The three loopback addresses the scripted Sentinels bind. Named because only one
	// listener can hold 127.0.0.x:26379 at a time.
	opSentinelIP0 = "127.0.0.1"
	opSentinelIP1 = "127.0.0.2"
	opSentinelIP2 = "127.0.0.3"
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
		sentinelIP = []string{opSentinelIP0, opSentinelIP1, opSentinelIP2}
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
			ObjectMeta: metav1.ObjectMeta{GenerateName: "ops-", Namespace: testNamespaceDefault},
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

// ---- ADR-020: convergence must survive an operation, rescue must not ----

// opRecordingSentinel is a Sentinel that monitors exactly one name, reports one healthy
// replica plus one GHOST replica, and records every command it is asked to run — which
// is what makes "no SENTINEL RESET was issued" assertable rather than merely believed.
//
// It is a separate fake from scriptedSentinel (stale_master_name_wiring_test.go) because
// that one answers SENTINEL REPLICAS with an empty array, and a ghost replica is the
// whole precondition of Rule D.
type opRecordingSentinel struct {
	mu       sync.Mutex
	name     string
	masterIP string
	replicas [][2]string // ip, flags
	commands [][]string
}

func newOpRecordingSentinel(
	t GinkgoTInterface, host, name, masterIP string, replicas [][2]string,
) *opRecordingSentinel {
	f := &opRecordingSentinel{name: name, masterIP: masterIP, replicas: replicas}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, littleredv1alpha1.SentinelPort))
	if err != nil {
		Skip(fmt.Sprintf("cannot bind %s:%d (in use?): %v", host, littleredv1alpha1.SentinelPort, err))
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				for {
					args, err := readRESPCommand(r)
					if err != nil {
						return
					}
					if _, err := c.Write([]byte(f.reply(args))); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return f
}

func (f *opRecordingSentinel) reply(args []string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(args) > 0 && strings.EqualFold(args[0], "hello") {
		return respNoHelloResp
	}
	if len(args) < 2 || !strings.EqualFold(args[0], "sentinel") {
		return respOK
	}
	f.commands = append(f.commands, append([]string(nil), args...))
	verb := strings.ToLower(args[1])
	name := ""
	if len(args) >= 3 {
		name = args[2]
	}
	known := name == f.name
	switch verb {
	case "masters":
		return "*1\r\n" + sentinelMasterRecord(f.name, f.masterIP)
	case RoleMaster:
		if !known {
			return respNoSuchName
		}
		return sentinelMasterRecord(f.name, f.masterIP)
	case "get-master-addr-by-name":
		if !known {
			return "*-1\r\n"
		}
		return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$4\r\n6379\r\n", len(f.masterIP), f.masterIP)
	case "replicas", "slaves":
		if !known {
			return respNoSuchName
		}
		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(f.replicas))
		for _, rep := range f.replicas {
			fields := []string{"ip", rep[0], fieldPort, "6379", opFieldFlags, rep[1]}
			fmt.Fprintf(&b, "*%d\r\n", len(fields))
			for _, fl := range fields {
				fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(fl), fl)
			}
		}
		return b.String()
	}
	return respOK
}

// sawReset reports whether Rule D's destructive primitive reached this Sentinel.
func (f *opRecordingSentinel) sawReset() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.commands {
		if len(c) >= 2 && strings.EqualFold(c[1], "reset") {
			return true
		}
	}
	return false
}

// opRecordingRedis answers INFO with a chosen replication view and records REPLICAOF,
// which is how Rule R's one action is observed.
type opRecordingRedis struct {
	mu        sync.Mutex
	info      string
	replicaOf [][]string
}

func newOpRecordingRedis(t GinkgoTInterface, host, info string) *opRecordingRedis {
	f := &opRecordingRedis{info: info}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, littleredv1alpha1.RedisPort))
	if err != nil {
		Skip(fmt.Sprintf("cannot bind %s:%d (in use?): %v", host, littleredv1alpha1.RedisPort, err))
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				for {
					args, err := readRESPCommand(r)
					if err != nil {
						return
					}
					reply := respOK
					switch {
					case len(args) > 0 && strings.EqualFold(args[0], "hello"):
						reply = respNoHelloResp
					case len(args) > 0 && strings.EqualFold(args[0], "info"):
						f.mu.Lock()
						reply = fmt.Sprintf("$%d\r\n%s\r\n", len(f.info), f.info)
						f.mu.Unlock()
					case len(args) > 0 &&
						(strings.EqualFold(args[0], "replicaof") || strings.EqualFold(args[0], "slaveof")):
						f.mu.Lock()
						f.replicaOf = append(f.replicaOf, append([]string(nil), args[1:]...))
						f.mu.Unlock()
					}
					if _, err := c.Write([]byte(reply)); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return f
}

func (f *opRecordingRedis) sawReplicaOf() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.replicaOf) > 0
}

// This is the M3.1 regression guard, and it is an A/B with exactly ONE variable: the
// acknowledgment. Everything else — pods, Sentinels, the straggler, the ghost replica,
// the unsettled StatefulSet — is identical between the two specs.
//
// The regression it pins was measured on t3e, three runs, and cost the first-replaced
// pod ~180s: the branch returned before Rule A, which suppressed RULE R, which is the
// one rule that repoints a replaced pod still following the old master. That pod then
// never became Ready, so the StatefulSet never settled, so the operation never
// completed, so Rule R stayed suppressed. The operation suppressed the healing its own
// completion condition depends on.
//
// So the fork is CONVERGENCE versus RESCUE, not operation versus healing:
//
//	Rule R  — convergence. One idempotent SLAVEOF at the master the operation is itself
//	          converging on. Must run. (The name lies: "Replica Rescue" is not rescue.)
//	Rule D  — rescue. SENTINEL RESET wipes the whole replica list and is LR-024's
//	          self-inflicted deadlock trigger. Must not run.
var _ = Describe("ADR-020 an operation suppresses rescue, never convergence", func() {
	const (
		opDesired  = "ops-c.cache"
		opMasterIP = "127.0.0.30"
		opHealthy  = "127.0.0.31"
		opStragglr = "127.0.0.32"
		opGhostIP  = "10.99.99.99"
	)
	var (
		reconciler *LittleRedReconciler
		lr         *littleredv1alpha1.LittleRed
		sentinels  []*opRecordingSentinel
		straggler  *opRecordingRedis
		sentinelIP = []string{opSentinelIP0, opSentinelIP1, opSentinelIP2}
	)

	BeforeEach(func() {
		reconciler = &LittleRedReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(64),
		}
		lr = &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "opconv-", Namespace: testNamespaceDefault},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:     ModeSentinel,
				Sentinel: &littleredv1alpha1.SentinelSpec{Quorum: 2, MasterName: opDesired},
			},
		}
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())
		lr.Status.Phase = littleredv1alpha1.PhaseRunning
		Expect(k8sClient.Status().Update(ctx, lr)).To(Succeed())

		makePod := func(name, ip string, labels map[string]string) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: lr.Name + "-" + name, Namespace: lr.Namespace, Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: ComponentRedis, Image: opTestImage}}},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			pod.Status.PodIP = ip
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: ComponentRedis, Ready: true}}
			// PodReady is load-bearing for this fixture, not decoration:
			// getSentinelAddresses skips any Sentinel pod without it, so without this the
			// SENTINEL RESET goes only to the Service FQDN, reaches no fake, and the
			// "no RESET was issued" assertion below passes having tested nothing. The
			// control spec is what caught exactly that.
			pod.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}
		makePod("redis-0", opMasterIP, redisSelectorLabels(lr))
		makePod("redis-1", opHealthy, redisSelectorLabels(lr))
		makePod("redis-2", opStragglr, redisSelectorLabels(lr))
		for i, ip := range sentinelIP {
			makePod(fmt.Sprintf("sentinel-%d", i), ip, sentinelSelectorLabels(lr))
		}

		// The Redis side: a master, a healthy replica, and a straggler still following
		// an address that is nobody's master any more — precisely the state a replaced
		// pod returns in, and the only thing Rule R acts on.
		newOpRecordingRedis(GinkgoT(), opMasterIP,
			"# Replication\r\nrole:master\r\nconnected_slaves:2\r\nmaster_replid:abc\r\n"+
				"master_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:100\r\n")
		newOpRecordingRedis(GinkgoT(), opHealthy,
			"# Replication\r\nrole:slave\r\nmaster_host:"+opMasterIP+"\r\nmaster_link_status:up\r\n"+
				"master_replid:abc\r\nmaster_repl_offset:100\r\n")
		straggler = newOpRecordingRedis(GinkgoT(), opStragglr,
			"# Replication\r\nrole:slave\r\nmaster_host:10.99.99.98\r\nmaster_link_status:down\r\n"+
				"master_replid:abc\r\nmaster_repl_offset:90\r\n")

		// The Sentinel side: a ghost replica (dead IP, s_down) plus a healthy one, which
		// is the exact shape Rule D fires on — LR-011 requires >=1 healthy known replica
		// and LR-013 requires the instance to be whole, and both hold here.
		sentinels = make([]*opRecordingSentinel, 0, len(sentinelIP))
		for _, ip := range sentinelIP {
			sentinels = append(sentinels, newOpRecordingSentinel(GinkgoT(), ip, opDesired, opMasterIP,
				[][2]string{{opHealthy, roleSlave}, {opGhostIP, "s_down," + roleSlave}}))
		}

		// Both StatefulSets exist and are UNSETTLED in both specs, so the only thing
		// that differs between them is the acknowledgment.
		for _, n := range []struct {
			name   string
			labels map[string]string
		}{{statefulSetName(lr), redisSelectorLabels(lr)}, {sentinelStatefulSetName(lr), sentinelSelectorLabels(lr)}} {
			replicas := int32(3)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: n.name, Namespace: lr.Namespace},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{MatchLabels: n.labels},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: n.labels},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: ComponentRedis, Image: opTestImage}}},
					},
					ServiceName: n.name,
				},
			}
			Expect(k8sClient.Create(ctx, sts)).To(Succeed())
			sts.Status = appsv1.StatefulSetStatus{
				ObservedGeneration: sts.Generation,
				Replicas:           3, ReadyReplicas: 2, UpdatedReplicas: 1,
				CurrentRevision: opTestRevision, UpdateRevision: opTestRevision + "-next",
			}
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())
		}
	})

	setAck := func(value string) {
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

	sawAnyReset := func() bool {
		for _, s := range sentinels {
			if s.sawReset() {
				return true
			}
		}
		return false
	}

	It("runs Rule R and withholds Rule D while an operation is in progress", func() {
		setAck("the-previous-name") // fingerprint differs => the rename is pending

		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())

		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		Expect(latest.Status.Operation).NotTo(BeNil(), "precondition: an operation must be in progress")
		Expect(latest.Status.Operation.Reason).To(Equal(operationReasonRunning))

		Expect(straggler.sawReplicaOf()).To(BeTrue(),
			"Rule R is CONVERGENCE and must still run: this is the M3.1 regression, where the "+
				"replaced pod was never repointed, so it never became Ready, so the StatefulSet "+
				"never settled, so the operation never completed")
		Expect(sawAnyReset()).To(BeFalse(),
			"Rule D is RESCUE and must stand down: SENTINEL RESET wipes the replica list and is "+
				"LR-024's self-inflicted deadlock trigger")
	})

	It("runs both once no operation is in progress (the control)", func() {
		setAck(opDesired) // fingerprint matches => converged, no operation

		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())

		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		Expect(latest.Status.Operation).To(BeNil(), "precondition: no operation may be in progress")

		Expect(straggler.sawReplicaOf()).To(BeTrue(), "Rule R runs here too")
		Expect(sawAnyReset()).To(BeTrue(),
			"the control that stops the spec above passing vacuously: with no operation, Rule D "+
				"MUST fire on this fixture, so 'no RESET' there is attributable to the operation "+
				"and not to a fixture that never reaches Rule D at all")
	})
})
