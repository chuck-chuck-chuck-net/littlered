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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// TestReducedResilienceCondition covers the ADR-012 D2 reduced-site-loss-resilience warning:
// a topology whose takeover FLOOR is structurally unreachable (S*floor(K/2) < K) sets the
// ReducedSiteLossResilience condition True with a one-shot Warning event; a resilient topology
// clears it to False with no warning event; and re-running while already warned does NOT
// re-emit the event (idempotent, one-shot).
//
// LR-031: the fixtures moved off K=4-over-3-sites, which the native-bound derivation wrongly
// warned on. Given Redis Cluster's three-master minimum the warned regime is exactly "two
// sites with an odd K".
func TestReducedResilienceCondition(t *testing.T) {
	countReducedEvents := func(evs []string) int {
		n := 0
		for _, e := range evs {
			if strings.Contains(e, reasonReducedSiteLossResilience) {
				n++
			}
		}
		return n
	}

	t.Run("non-resilient topology warns once", func(t *testing.T) {
		// K=3 over 2 sites → floor 1, 2*1 < 3 → not resilient. Odd K on two sites is always
		// lopsided: the 2-master side's loss leaves it holding a majority, so no fence.
		lr := multiSiteClusterCR(2, 2, 3, 1)
		rec := events.NewFakeRecorder(16)
		r := &LittleRedReconciler{Recorder: rec}

		r.reconcileReducedResilienceCondition(lr)
		c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionReducedSiteLossResilience)
		if c == nil || c.Status != metav1.ConditionTrue || c.Reason != reasonReducedSiteLossResilience {
			t.Fatalf("condition = %+v, want True/%s", c, reasonReducedSiteLossResilience)
		}
		if got := countReducedEvents(drainRecorderEvents(rec)); got != 1 {
			t.Fatalf("warning events = %d, want 1", got)
		}

		// Second pass while already warned: condition stays True, NO new event (one-shot).
		r.reconcileReducedResilienceCondition(lr)
		if got := countReducedEvents(drainRecorderEvents(rec)); got != 0 {
			t.Fatalf("re-emitted warning events = %d, want 0 (one-shot)", got)
		}
	})

	t.Run("resilient topology clears to False without warning", func(t *testing.T) {
		// K=4 over 2 sites → floor 2, 2*2 >= 4 → resilient (the D3-recommended dual-site
		// topology, which the pre-LR-031 native bound wrongly warned on).
		lr := multiSiteClusterCR(2, 2, 4, 1)
		rec := events.NewFakeRecorder(16)
		r := &LittleRedReconciler{Recorder: rec}

		r.reconcileReducedResilienceCondition(lr)
		c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionReducedSiteLossResilience)
		if c == nil || c.Status != metav1.ConditionFalse || c.Reason != reasonSiteLossResilient {
			t.Fatalf("condition = %+v, want False/%s", c, reasonSiteLossResilient)
		}
		if got := countReducedEvents(drainRecorderEvents(rec)); got != 0 {
			t.Fatalf("warning events = %d, want 0 for a resilient topology", got)
		}
	})
}
