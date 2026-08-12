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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// drainRecorderEvents returns all events currently buffered in the fake recorder.
func drainRecorderEvents(rec *events.FakeRecorder) []string {
	var evs []string
	for {
		select {
		case e := <-rec.Events:
			evs = append(evs, e)
		default:
			return evs
		}
	}
}

func hasExperimentalEvent(evs []string) bool {
	for _, e := range evs {
		if strings.Contains(e, reasonExperimentalMode) && strings.Contains(e, "experimental") {
			return true
		}
	}
	return false
}

// TestMultiSiteExperimentalModeEvent asserts parity with failover mode's ADR-011
// §8 notice: a multi-site stretched cluster emits a one-time ExperimentalMode
// warning on its first reconcile, and a single-site cluster CR does NOT.
func TestMultiSiteExperimentalModeEvent(t *testing.T) {
	ctx := context.Background()

	t.Run("multi-site cluster CR emits the ExperimentalMode warning on first reconcile", func(t *testing.T) {
		lr := &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: "ms-exp", Namespace: testNamespaceDefault},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:    ModeCluster,
				Cluster: &littleredv1alpha1.ClusterSpec{Shards: 3},
				MultiSite: &littleredv1alpha1.MultiSiteSpec{
					Sites: []littleredv1alpha1.SiteSpec{{Name: siteAlpha}, {Name: siteBeta}},
				},
			},
		}
		rec := events.NewFakeRecorder(16)
		fc := newStatusFakeClient(t, lr)
		r := &LittleRedReconciler{Client: fc, Scheme: fc.Scheme(), Recorder: rec}

		// reconcileCluster emits the warning in the Phase=="" block, then routes
		// to reconcileMultiSiteCluster which requeues (r.sites is nil here — no
		// site registry without a manager). We only assert the event.
		if _, err := r.reconcileCluster(ctx, lr); err != nil {
			t.Fatalf("reconcileCluster returned error: %v", err)
		}
		if !hasExperimentalEvent(drainRecorderEvents(rec)) {
			t.Fatal("expected an ExperimentalMode warning event for a multi-site cluster CR on first reconcile")
		}
	})

	t.Run("single-site cluster CR does NOT emit the ExperimentalMode warning", func(t *testing.T) {
		lr := &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{Name: "ss-exp", Namespace: testNamespaceDefault},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:    ModeCluster,
				Cluster: &littleredv1alpha1.ClusterSpec{Shards: 3},
				// No MultiSite: single-site cluster.
			},
		}
		rec := events.NewFakeRecorder(16)
		fc := newStatusFakeClient(t, lr)
		r := &LittleRedReconciler{Client: fc, Scheme: fc.Scheme(), Recorder: rec}

		// The single-site path proceeds past the Phase=="" block; it may requeue
		// on not-yet-ready StatefulSets, but it must never emit ExperimentalMode.
		if _, err := r.reconcileCluster(ctx, lr); err != nil {
			t.Fatalf("reconcileCluster returned error: %v", err)
		}
		if hasExperimentalEvent(drainRecorderEvents(rec)) {
			t.Fatal("single-site cluster CR must NOT receive an ExperimentalMode warning")
		}
	})
}
