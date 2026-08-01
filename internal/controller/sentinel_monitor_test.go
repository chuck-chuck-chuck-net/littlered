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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// --- ensure/stop scaffolding (the sentinel twin of the failover watcher's
// scaffolding specs — §7 cross-mode parity): registration, dedupe, the
// disable-annotation kill switch, stop idempotence, and the no-channel guard. ---

var _ = Describe("Sentinel monitor scaffolding", func() {
	const name = "sentinel-monitor-scaffold"
	nn := types.NamespacedName{Name: name, Namespace: testNamespaceDefault}
	ctx := context.Background()

	newLR := func() *littleredv1alpha1.LittleRed {
		return &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespaceDefault},
			Spec:       littleredv1alpha1.LittleRedSpec{Mode: ModeSentinel},
		}
	}
	newReconciler := func(events chan event.GenericEvent) *LittleRedReconciler {
		return &LittleRedReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			monitorEvents: events,
			monitors:      make(map[types.NamespacedName]func()),
		}
	}

	It("registers exactly one monitor per instance (ensure is idempotent)", func() {
		r := newReconciler(make(chan event.GenericEvent, 8))
		defer r.stopSentinelMonitor(nn)

		r.ensureSentinelMonitor(ctx, newLR())
		Expect(r.monitors).To(HaveKey(nn))

		// Wrap the stored cancel in a detectable marker: if the second ensure
		// replaced the registration (goroutine churn), stop would invoke the
		// replacement instead of the marker and kept stays false.
		orig := r.monitors[nn]
		kept := false
		r.monitors[nn] = func() { kept = true; orig() }

		r.ensureSentinelMonitor(ctx, newLR())
		Expect(r.monitors).To(HaveLen(1))
		r.stopSentinelMonitor(nn)
		Expect(kept).To(BeTrue(), "second ensure must keep the existing registration, not replace it")
	})

	It("honors the disable-event-monitoring annotation and stops a running monitor", func() {
		r := newReconciler(make(chan event.GenericEvent, 8))
		defer r.stopSentinelMonitor(nn)

		disabled := newLR()
		disabled.Annotations = map[string]string{AnnotationDisableEventMonitoring: annotationValueTrue}
		r.ensureSentinelMonitor(ctx, disabled)
		Expect(r.monitors).To(BeEmpty())

		// A running monitor is stopped when the annotation appears.
		r.ensureSentinelMonitor(ctx, newLR())
		Expect(r.monitors).To(HaveKey(nn))
		r.ensureSentinelMonitor(ctx, disabled)
		Expect(r.monitors).To(BeEmpty())
	})

	It("stop removes the monitor and is idempotent", func() {
		r := newReconciler(make(chan event.GenericEvent, 8))

		r.ensureSentinelMonitor(ctx, newLR())
		Expect(r.monitors).To(HaveKey(nn))

		r.stopSentinelMonitor(nn)
		Expect(r.monitors).To(BeEmpty())
		Expect(func() { r.stopSentinelMonitor(nn) }).NotTo(Panic())
	})

	It("does not start a monitor without the event channel (direct-construction reconcilers)", func() {
		r := newReconciler(nil) // unit/envtest construction: no SetupWithManager wiring
		r.ensureSentinelMonitor(ctx, newLR())
		Expect(r.monitors).To(BeEmpty())
	})
})
