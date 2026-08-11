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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// --- advanceFailoverProbeStreak: the fire/re-arm decision (ADR-011 §4) ------
//
// The watcher's only decision — WHEN to push the accelerating GenericEvent —
// is this pure seam: fire exactly once per failure streak once it crosses
// downAfterMilliseconds; re-arm on a successful probe or a master-IP change.
// Topology semantics stay with reconcile/planMasterDeath (LR-016).

func TestAdvanceFailoverProbeStreak(t *testing.T) {
	const downAfter = 5 * time.Second
	const ipA, ipB = ipMaster, ipReplica
	t0 := time.Unix(3_000_000, 0)

	tests := []struct {
		name       string
		prev       failoverProbeStreak
		masterIP   string
		probeOK    bool
		now        time.Time
		downAfter  time.Duration
		wantStreak failoverProbeStreak
		wantFire   bool
	}{
		{
			name:       "no master to probe -> full reset, no fire (idle tick during transitions)",
			prev:       failoverProbeStreak{masterIP: ipA, failingSince: t0, fired: true},
			masterIP:   "",
			probeOK:    false,
			now:        t0.Add(10 * time.Second),
			downAfter:  downAfter,
			wantStreak: failoverProbeStreak{},
			wantFire:   false,
		},
		{
			name:       "probe OK -> healthy, no fire",
			prev:       failoverProbeStreak{},
			masterIP:   ipA,
			probeOK:    true,
			now:        t0,
			downAfter:  downAfter,
			wantStreak: failoverProbeStreak{masterIP: ipA},
			wantFire:   false,
		},
		{
			name:       "probe OK after a fired streak -> re-arm (next streak may fire again)",
			prev:       failoverProbeStreak{masterIP: ipA, failingSince: t0.Add(-10 * time.Second), fired: true},
			masterIP:   ipA,
			probeOK:    true,
			now:        t0,
			downAfter:  downAfter,
			wantStreak: failoverProbeStreak{masterIP: ipA},
			wantFire:   false,
		},
		{
			name:       "first failure ever -> streak starts now, no fire",
			prev:       failoverProbeStreak{},
			masterIP:   ipA,
			probeOK:    false,
			now:        t0,
			downAfter:  downAfter,
			wantStreak: failoverProbeStreak{masterIP: ipA, failingSince: t0},
			wantFire:   false,
		},
		{
			name:       "first failure after healthy ticks -> streak starts now, no fire",
			prev:       failoverProbeStreak{masterIP: ipA},
			masterIP:   ipA,
			probeOK:    false,
			now:        t0,
			downAfter:  downAfter,
			wantStreak: failoverProbeStreak{masterIP: ipA, failingSince: t0},
			wantFire:   false,
		},
		{
			name:       "failure within the window -> wait, streak start preserved",
			prev:       failoverProbeStreak{masterIP: ipA, failingSince: t0},
			masterIP:   ipA,
			probeOK:    false,
			now:        t0.Add(2 * time.Second),
			downAfter:  downAfter,
			wantStreak: failoverProbeStreak{masterIP: ipA, failingSince: t0},
			wantFire:   false,
		},
		{
			name:       "failure crossing the window (>=) -> fire exactly now",
			prev:       failoverProbeStreak{masterIP: ipA, failingSince: t0},
			masterIP:   ipA,
			probeOK:    false,
			now:        t0.Add(downAfter),
			downAfter:  downAfter,
			wantStreak: failoverProbeStreak{masterIP: ipA, failingSince: t0, fired: true},
			wantFire:   true,
		},
		{
			name:       "failure long past the window after firing -> hysteresis, no event spam",
			prev:       failoverProbeStreak{masterIP: ipA, failingSince: t0, fired: true},
			masterIP:   ipA,
			probeOK:    false,
			now:        t0.Add(30 * time.Second),
			downAfter:  downAfter,
			wantStreak: failoverProbeStreak{masterIP: ipA, failingSince: t0, fired: true},
			wantFire:   false,
		},
		{
			name:       "master IP changed mid-streak -> streak restarts against the new IP (no stale-probe leak)",
			prev:       failoverProbeStreak{masterIP: ipA, failingSince: t0, fired: true},
			masterIP:   ipB,
			probeOK:    false,
			now:        t0.Add(20 * time.Second),
			downAfter:  downAfter,
			wantStreak: failoverProbeStreak{masterIP: ipB, failingSince: t0.Add(20 * time.Second)},
			wantFire:   false,
		},
		{
			name:       "zero downAfter -> a fresh failure fires on the same tick",
			prev:       failoverProbeStreak{masterIP: ipA},
			masterIP:   ipA,
			probeOK:    false,
			now:        t0,
			downAfter:  0,
			wantStreak: failoverProbeStreak{masterIP: ipA, failingSince: t0, fired: true},
			wantFire:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStreak, gotFire := advanceFailoverProbeStreak(tt.prev, tt.masterIP, tt.probeOK, tt.now, tt.downAfter)
			if gotStreak != tt.wantStreak {
				t.Errorf("streak = %+v, want %+v", gotStreak, tt.wantStreak)
			}
			if gotFire != tt.wantFire {
				t.Errorf("fire = %v, want %v", gotFire, tt.wantFire)
			}
		})
	}
}

// --- ensure/stop scaffolding (mirrors sentinel_monitor.go, which has no tests;
// these are the cheap bookkeeping checks: registration, dedupe, the
// disable-annotation kill switch, stop idempotence, and the no-channel guard) --

var _ = Describe("Failover master watcher scaffolding", func() {
	const name = "failover-watcher-scaffold"
	nn := types.NamespacedName{Name: name, Namespace: testNamespaceDefault}
	ctx := context.Background()

	newLR := func() *littleredv1alpha1.LittleRed {
		return &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespaceDefault},
			Spec:       littleredv1alpha1.LittleRedSpec{Mode: ModeFailover},
		}
	}
	newReconciler := func(events chan event.GenericEvent) *LittleRedReconciler {
		return &LittleRedReconciler{
			Client:           k8sClient,
			Scheme:           k8sClient.Scheme(),
			monitorEvents:    events,
			failoverMonitors: make(map[types.NamespacedName]func()),
		}
	}

	It("registers exactly one watcher per instance (ensure is idempotent)", func() {
		r := newReconciler(make(chan event.GenericEvent, 8))
		defer r.stopFailoverMonitor(nn)

		r.ensureFailoverMonitor(ctx, newLR())
		Expect(r.failoverMonitors).To(HaveKey(nn))

		// Wrap the stored cancel in a detectable marker: if the second ensure
		// replaced the registration (goroutine churn), stop would invoke the
		// replacement instead of the marker and kept stays false.
		orig := r.failoverMonitors[nn]
		kept := false
		r.failoverMonitors[nn] = func() { kept = true; orig() }

		r.ensureFailoverMonitor(ctx, newLR())
		Expect(r.failoverMonitors).To(HaveLen(1))
		r.stopFailoverMonitor(nn)
		Expect(kept).To(BeTrue(), "second ensure must keep the existing registration, not replace it")
	})

	It("honors the disable-event-monitoring annotation and stops a running watcher", func() {
		r := newReconciler(make(chan event.GenericEvent, 8))
		defer r.stopFailoverMonitor(nn)

		disabled := newLR()
		disabled.Annotations = map[string]string{AnnotationDisableEventMonitoring: annotationValueTrue}
		r.ensureFailoverMonitor(ctx, disabled)
		Expect(r.failoverMonitors).To(BeEmpty())

		// A running watcher is stopped when the annotation appears.
		r.ensureFailoverMonitor(ctx, newLR())
		Expect(r.failoverMonitors).To(HaveKey(nn))
		r.ensureFailoverMonitor(ctx, disabled)
		Expect(r.failoverMonitors).To(BeEmpty())
	})

	It("stop removes the watcher and is idempotent", func() {
		r := newReconciler(make(chan event.GenericEvent, 8))

		r.ensureFailoverMonitor(ctx, newLR())
		Expect(r.failoverMonitors).To(HaveKey(nn))

		r.stopFailoverMonitor(nn)
		Expect(r.failoverMonitors).To(BeEmpty())
		Expect(func() { r.stopFailoverMonitor(nn) }).NotTo(Panic())
	})

	It("does not start a watcher without the event channel (direct-construction reconcilers)", func() {
		r := newReconciler(nil) // unit/envtest construction: no SetupWithManager wiring
		r.ensureFailoverMonitor(ctx, newLR())
		Expect(r.failoverMonitors).To(BeEmpty())
	})
})
