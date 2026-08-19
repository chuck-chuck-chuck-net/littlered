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

// TestSentinelMasterNameUnscopedCondition covers the runtime warning for an instance that
// has NOT decided its Sentinel master name and is therefore falling back to the shared
// legacy value — the state in which any other unscoped Sentinel deployment reachable on
// the pod network can absorb its topology and destroy its data.
//
// The CRD requires the field, so this can only be an instance created before the field
// existed. Validation cannot reach those (CRD validation ratcheting excuses an unchanged
// spec.sentinel), so this condition is the ONLY signal such an instance ever gets.
//
// Setting the field to "mymaster" EXPLICITLY is deliberately not warned about: that is a
// decision (a legacy client may hardcode the value with no way to parameterise it), and
// the operator does not second-guess decisions. Only the absence of one is reported.
// drainMasterNameEvents empties a fake recorder's channel. Declared locally: this
// branch has no shared drain helper.
func drainMasterNameEvents(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestSentinelMasterNameUnscopedCondition(t *testing.T) {
	countEvents := func(evs []string) int {
		n := 0
		for _, e := range evs {
			if strings.Contains(e, reasonSentinelMasterNameUnscoped) {
				n++
			}
		}
		return n
	}

	sentinelCR := func(s *littleredv1alpha1.SentinelSpec) *littleredv1alpha1.LittleRed {
		return &littleredv1alpha1.LittleRed{
			Spec: littleredv1alpha1.LittleRedSpec{Mode: ModeSentinel, Sentinel: s},
		}
	}

	t.Run("unset masterName warns once", func(t *testing.T) {
		lr := sentinelCR(&littleredv1alpha1.SentinelSpec{Quorum: 2})
		rec := events.NewFakeRecorder(16)
		r := &LittleRedReconciler{Recorder: rec}

		r.reconcileSentinelMasterNameCondition(lr)
		c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionSentinelMasterNameUnscoped)
		if c == nil || c.Status != metav1.ConditionTrue || c.Reason != reasonSentinelMasterNameUnscoped {
			t.Fatalf("condition = %+v, want True/%s", c, reasonSentinelMasterNameUnscoped)
		}
		if !strings.Contains(c.Message, littleredv1alpha1.LegacySentinelMasterName) {
			t.Errorf("message must name the value in use, got %q", c.Message)
		}
		if got := countEvents(drainMasterNameEvents(rec)); got != 1 {
			t.Fatalf("warning events = %d, want 1", got)
		}

		// One-shot: still True on the next pass, no repeat event.
		r.reconcileSentinelMasterNameCondition(lr)
		if got := countEvents(drainMasterNameEvents(rec)); got != 0 {
			t.Fatalf("re-emitted warning events = %d, want 0 (one-shot)", got)
		}
	})

	t.Run("absent sentinel block also warns", func(t *testing.T) {
		lr := sentinelCR(nil)
		rec := events.NewFakeRecorder(16)
		r := &LittleRedReconciler{Recorder: rec}

		r.reconcileSentinelMasterNameCondition(lr)
		c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionSentinelMasterNameUnscoped)
		if c == nil || c.Status != metav1.ConditionTrue {
			t.Fatalf("condition = %+v, want True", c)
		}
	})

	t.Run("explicit mymaster is a decision and is not warned about", func(t *testing.T) {
		lr := sentinelCR(&littleredv1alpha1.SentinelSpec{
			Quorum:     2,
			MasterName: littleredv1alpha1.LegacySentinelMasterName,
		})
		rec := events.NewFakeRecorder(16)
		r := &LittleRedReconciler{Recorder: rec}

		r.reconcileSentinelMasterNameCondition(lr)
		c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionSentinelMasterNameUnscoped)
		if c == nil || c.Status != metav1.ConditionFalse {
			t.Fatalf("condition = %+v, want False (explicit choice is honoured)", c)
		}
		if got := countEvents(drainMasterNameEvents(rec)); got != 0 {
			t.Fatalf("warning events = %d, want 0", got)
		}
	})

	t.Run("scoped masterName clears the condition", func(t *testing.T) {
		lr := sentinelCR(&littleredv1alpha1.SentinelSpec{Quorum: 2})
		rec := events.NewFakeRecorder(16)
		r := &LittleRedReconciler{Recorder: rec}

		r.reconcileSentinelMasterNameCondition(lr) // warned
		lr.Spec.Sentinel.MasterName = "team-a.cache"
		r.reconcileSentinelMasterNameCondition(lr)

		c := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionSentinelMasterNameUnscoped)
		if c == nil || c.Status != metav1.ConditionFalse {
			t.Fatalf("condition = %+v, want False after the name is set", c)
		}
	})
}
