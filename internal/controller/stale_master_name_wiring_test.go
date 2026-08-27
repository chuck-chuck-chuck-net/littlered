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

// This is the WP4 wiring guard: the rename's ONE observable promise, driven through
// the real reconcile path rather than through the pure planner.
//
// It runs the actual reconcileSentinelCluster against three scripted Sentinels and one
// scripted Redis master bound on distinct loopback addresses, so the whole chain is
// exercised end to end: gather → planForsaken → Rule 0 → Rule N → the bounded
// IsMonitoring re-confirm → SENTINEL REMOVE → the condition. Every command each fake
// Sentinel receives is recorded, which is what makes "the desired name was never
// removed" assertable rather than merely believed.

const (
	respOK          = "+OK\r\n"
	respNoSuchName  = "-ERR No such master with that name\r\n"
	respNoHelloResp = "-ERR unknown command 'HELLO'\r\n"
)

// scriptedSentinel is a scripted Sentinel that monitors a desired name and (optionally) a
// stale one — the state a half-finished rename leaves behind. It records every command
// it is asked to run and honours SENTINEL REMOVE, so the effect of Rule N is observable.
//
// Like twoNameSentinel (gatherer_masters_test.go) it speaks RESP2 by answering HELLO
// with an error, and it must bind the real Sentinel port because GetSentinelState builds
// the address from littleredv1alpha1.SentinelPort. Distinct 127.0.0.x addresses are what
// let three of them coexist — a quorum is a gate Rule N actually has (G4).
type scriptedSentinel struct {
	mu       sync.Mutex
	masters  map[string]string // name -> master IP
	order    []string          // stable order for SENTINEL masters
	commands [][]string
}

// newScriptedSentinelNamed starts a scripted Sentinel with a per-name master address,
// which is what distinguishes a leftover entry still pointing at OUR master (ordinary rename
// debris) from one pointing somewhere else entirely (§7.3's trap).
func newScriptedSentinelNamed(t GinkgoTInterface, host string, pairs [][2]string) *scriptedSentinel {
	f := &scriptedSentinel{masters: map[string]string{}}
	for _, p := range pairs {
		f.masters[p[0]] = p[1]
		f.order = append(f.order, p[0])
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, littleredv1alpha1.SentinelPort))
	if err != nil {
		Skip(fmt.Sprintf("cannot bind %s:%d (in use?): %v", host, littleredv1alpha1.SentinelPort, err))
	}
	t.Cleanup(func() { _ = ln.Close() })
	go f.serve(ln)
	return f
}

func (f *scriptedSentinel) serve(ln net.Listener) {
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
}

func (f *scriptedSentinel) reply(args []string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(args) > 0 && strings.EqualFold(args[0], "hello") {
		return respNoHelloResp
	}
	if len(args) == 0 || !strings.EqualFold(args[0], "sentinel") || len(args) < 2 {
		return respOK
	}
	f.commands = append(f.commands, append([]string(nil), args...))
	verb := strings.ToLower(args[1])
	name := ""
	if len(args) >= 3 {
		name = args[2]
	}
	ip, known := f.masters[name]
	switch verb {
	case "masters":
		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(f.order))
		for _, n := range f.order {
			b.WriteString(sentinelMasterRecord(n, f.masters[n]))
		}
		return b.String()
	case "master":
		if !known {
			return respNoSuchName
		}
		return sentinelMasterRecord(name, ip)
	case "get-master-addr-by-name":
		if !known {
			return "*-1\r\n"
		}
		return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$4\r\n6379\r\n", len(ip), ip)
	case "replicas", "slaves":
		return "*0\r\n"
	case "remove":
		if !known {
			return respNoSuchName
		}
		delete(f.masters, name)
		for i, n := range f.order {
			if n == name {
				f.order = append(f.order[:i], f.order[i+1:]...)
				break
			}
		}
		return respOK
	case "monitor":
		if len(args) >= 4 {
			if _, dup := f.masters[name]; !dup {
				f.order = append(f.order, name)
			}
			f.masters[name] = args[3]
		}
		return respOK
	}
	return respOK
}

// monitored reports the names this Sentinel still carries.
func (f *scriptedSentinel) monitored() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

// removals reports the master names this Sentinel was asked to REMOVE.
func (f *scriptedSentinel) removals() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.commands {
		if len(c) >= 3 && strings.EqualFold(c[1], "remove") {
			out = append(out, c[2])
		}
	}
	return out
}

func sentinelMasterRecord(name, ip string) string {
	fields := []string{
		"name", name, "ip", ip, "port", "6379",
		"flags", "master", "num-slaves", "2", "num-other-sentinels", "2",
	}
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(fields))
	for _, f := range fields {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(f), f)
	}
	return b.String()
}

// fakeRedisMaster answers INFO like a healthy, empty master. G2 needs the consensus
// master to say so itself — "RealMasterIP != ”" is the easy mis-read of that gate.
func fakeRedisMaster(t GinkgoTInterface, host string) {
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, littleredv1alpha1.RedisPort))
	if err != nil {
		Skip(fmt.Sprintf("cannot bind %s:%d (in use?): %v", host, littleredv1alpha1.RedisPort, err))
	}
	t.Cleanup(func() { _ = ln.Close() })

	info := "# Replication\r\nrole:master\r\nconnected_slaves:2\r\nmaster_replid:abc\r\n" +
		"master_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:100\r\n"
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
						reply = fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)
					}
					if _, err := c.Write([]byte(reply)); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
}

var _ = Describe("Rule N: stale Sentinel master names", func() {
	const (
		desired  = "team-a.cache"
		stale    = "mymaster"
		masterIP = "127.0.0.10"
	)
	var (
		reconciler *LittleRedReconciler
		recorder   *events.FakeRecorder
		lr         *littleredv1alpha1.LittleRed
		sentinels  []*scriptedSentinel
		sentinelIP = []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}
	)

	BeforeEach(func() {
		recorder = events.NewFakeRecorder(64)
		reconciler = &LittleRedReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: recorder,
		}

		lr = &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "rulen-",
				Namespace:    "default",
			},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:     ModeSentinel,
				Sentinel: &littleredv1alpha1.SentinelSpec{Quorum: 2, MasterName: desired},
			},
		}
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())
		lr.Status.Phase = littleredv1alpha1.PhaseRunning
		lr.Status.BootstrapRequired = false
		Expect(k8sClient.Status().Update(ctx, lr)).To(Succeed())

		// One Redis master and three Sentinels, each on its own loopback address.
		makePod := func(name, ip string, labels map[string]string) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: lr.Name + "-" + name, Namespace: lr.Namespace, Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: ComponentRedis, Image: "redis:8"},
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
		sentinels = nil
	})

	// startSentinels binds the three Sentinels for THIS spec. It is deliberately not in
	// BeforeEach: only one listener can hold 127.0.0.x:26379 at a time, so a second bind
	// in a spec body Skips it — silently, which is how the suspicion tier below first
	// went "green" having never run.
	startSentinels := func(staleAt string) {
		sentinels = make([]*scriptedSentinel, 0, len(sentinelIP))
		for _, ip := range sentinelIP {
			sentinels = append(sentinels, newScriptedSentinelNamed(GinkgoT(), ip, [][2]string{
				{desired, masterIP}, {stale, staleAt},
			}))
		}
	}

	It("removes the stale name from every Sentinel, keeps the desired one, and says so", func() {
		// The stale entry still points at OUR master: §4's measured two-name state,
		// which is what a rename leaves behind and what nothing removed before Rule N.
		startSentinels(masterIP)
		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())

		for i, s := range sentinels {
			Expect(s.removals()).To(ConsistOf(stale),
				"sentinel-%d: the stale name must be REMOVEd exactly once and nothing else may be", i)
			Expect(s.monitored()).To(ConsistOf(desired),
				"sentinel-%d: exactly one monitored name must remain, and it must be the desired one (R3)", i)
		}

		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		c := meta.FindStatusCondition(latest.Status.Conditions, littleredv1alpha1.ConditionStaleMasterName)
		Expect(c).NotTo(BeNil(), "the rename must be observable (R5)")
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(staleNamesPruning))
		Expect(c.Message).To(ContainSubstring(stale))

		// The condition must also be mirrored onto the in-memory object: the rest of
		// the pass (updateSentinelStatus) writes the whole status back from it, so a
		// missing mirror silently reverts what was just persisted (LR-044).
		inMemory := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionStaleMasterName)
		Expect(inMemory).NotTo(BeNil(), "the persisted condition must be mirrored in memory (LR-044)")
		Expect(inMemory.Reason).To(Equal(staleNamesPruning))
	})

	// setRedisStatefulSet gives the instance a Redis StatefulSet in a chosen state.
	// LR-050's gate reads exactly this object: while it is not settled the operator
	// withholds ATTRIBUTION, because a pod it has just replaced is indistinguishable
	// from a captor's master from Sentinel's vantage.
	setRedisStatefulSet := func(settled bool) {
		replicas := int32(3)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: lr.Name + "-redis", Namespace: lr.Namespace},
			Spec: appsv1.StatefulSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: redisSelectorLabels(lr)},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: redisSelectorLabels(lr)},
					Spec: corev1.PodSpec{Containers: []corev1.Container{
						{Name: ComponentRedis, Image: "redis:8"},
					}},
				},
				ServiceName: lr.Name + "-redis",
			},
		}
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())
		sts.Status = appsv1.StatefulSetStatus{
			ObservedGeneration: sts.Generation,
			Replicas:           3,
			ReadyReplicas:      3,
			UpdatedReplicas:    3,
			CurrentRevision:    "rev-1",
			UpdateRevision:     "rev-1",
		}
		if !settled {
			// Mid-roll: the highest ordinal has been taken down and its replacement is
			// not Ready yet. This is the state a rename patch produces at t0+0.6s.
			sts.Status.ReadyReplicas = 2
			sts.Status.UpdatedReplicas = 1
			sts.Status.UpdateRevision = "rev-2"
		}
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())
	}

	It("reports a stale entry pointing elsewhere as Foreign on a SETTLED instance, and prunes nothing", func() {
		// §7.3: the stale name points at an address that is not one of our pods and
		// that Sentinel has not flagged down. On a settled instance something else is
		// alive there, so this is the trap the warning exists for.
		setRedisStatefulSet(true)
		startSentinels("10.9.9.9")

		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())

		for i, s := range sentinels {
			Expect(s.removals()).To(BeEmpty(), "sentinel-%d: nothing may be pruned in this state", i)
			Expect(s.monitored()).To(ConsistOf(desired, stale), "sentinel-%d", i)
		}

		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		c := meta.FindStatusCondition(latest.Status.Conditions, littleredv1alpha1.ConditionStaleMasterName)
		Expect(c).NotTo(BeNil())
		Expect(c.Reason).To(Equal(staleNamesForeign))
		Expect(c.Message).To(ContainSubstring("may be captured"))
		Expect(drainMasterNameEvents(recorder)).NotTo(BeEmpty(),
			"a settled Foreign reading must be reported once, loudly")
	})

	It("holds the SAME reading as unattributable while our own StatefulSet is rolling, and does not accuse", func() {
		// LR-050 / §9.2, the measured shape: mid-rename the just-replaced pod's address
		// has already left the pod list while Sentinel has not yet flagged it down. The
		// operator must not tell the owner their supported field edit looks like a
		// capture — and it must not raise its voice at all.
		setRedisStatefulSet(false)
		startSentinels("10.9.9.9")

		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())

		for i, s := range sentinels {
			Expect(s.removals()).To(BeEmpty(), "sentinel-%d: an unattributable entry is never pruned", i)
			Expect(s.monitored()).To(ConsistOf(desired, stale), "sentinel-%d", i)
		}

		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		c := meta.FindStatusCondition(latest.Status.Conditions, littleredv1alpha1.ConditionStaleMasterName)
		Expect(c).NotTo(BeNil())
		Expect(c.Reason).To(Equal(staleNamesDeferred),
			"mid-rollout an address of ours in the air is unattributable, not foreign")
		Expect(c.Message).To(ContainSubstring("mid-rollout"))
		Expect(c.Message).NotTo(ContainSubstring("may be captured"))

		// No event at all, let alone a Warning — on either pass.
		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())
		Expect(drainMasterNameEvents(recorder)).To(BeEmpty(),
			"a rename in progress must not raise the operator's voice")
	})

	It("reports Converged, and prunes nothing, once only the desired name is left", func() {
		startSentinels(masterIP)
		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())
		for _, s := range sentinels {
			Expect(s.monitored()).To(ConsistOf(desired))
		}

		Expect(reconciler.reconcileSentinelCluster(ctx, lr)).To(Succeed())
		for i, s := range sentinels {
			Expect(s.removals()).To(ConsistOf(stale),
				"sentinel-%d: a converged instance must not be touched again", i)
		}

		latest := &littleredv1alpha1.LittleRed{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest)).To(Succeed())
		c := meta.FindStatusCondition(latest.Status.Conditions, littleredv1alpha1.ConditionStaleMasterName)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(staleNamesConverged))
	})
})
