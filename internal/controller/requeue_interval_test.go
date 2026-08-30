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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// requeueAfterNotRunning is the one seam updateStatus and updateSentinelStatus must
// both go through when the instance is not yet Running (LR-045). It exists because
// LR-042 threaded the Forsaken-aware slow-cadence choice through updateStatus but not
// through updateSentinelStatus (the function every sentinel-mode instance actually
// returns through), so a quarantined/forsaken instance was polled at the fast cadence
// forever. A shared, directly-testable predicate is how this class of miss (rule 11,
// cross-mode parity) gets closed for good instead of re-diverging next time.
func TestRequeueAfterNotRunning(t *testing.T) {
	const fast, steady = 2 * time.Second, 30 * time.Second

	forsakenTrue := []metav1.Condition{{
		Type:   littleredv1alpha1.ConditionForsaken,
		Status: metav1.ConditionTrue,
	}}
	forsakenFalse := []metav1.Condition{{
		Type:   littleredv1alpha1.ConditionForsaken,
		Status: metav1.ConditionFalse,
	}}

	cases := []struct {
		name       string
		phase      littleredv1alpha1.LittleRedPhase
		conditions []metav1.Condition
		want       time.Duration
	}{
		{
			name:       "not running, not forsaken -> fast",
			phase:      littleredv1alpha1.PhaseInitializing,
			conditions: nil,
			want:       fast,
		},
		{
			name:       "not running, forsaken condition explicitly false -> fast",
			phase:      littleredv1alpha1.PhaseInitializing,
			conditions: forsakenFalse,
			want:       fast,
		},
		{
			// The defect: a quarantined/forsaken sentinel instance must be re-examined
			// at the steady cadence, not polled fast forever (measured on t3e,
			// LR-044 milestone M4a: 31 reconciles in 114s while Forsaken=True).
			name:       "not running, forsaken -> steady",
			phase:      littleredv1alpha1.PhaseInitializing,
			conditions: forsakenTrue,
			want:       steady,
		},
		{
			name:       "running -> steady, regardless of forsaken",
			phase:      littleredv1alpha1.PhaseRunning,
			conditions: forsakenTrue,
			want:       steady,
		},
		{
			// ADR-020, and LR-045's lesson applied to the new mechanism rather than
			// inherited: an instance under a declared heavy operation is frequently
			// RUNNING (its pods are healthy between rollout waves), so the phase check
			// alone would poll it at the steady cadence for the whole window. Its
			// healing is suppressed until the operation is acknowledged, and that
			// acknowledgment can only be written on a pass — so every steady interval
			// waited is an extra interval of suppression.
			name:  "running, but a declared operation is running -> fast",
			phase: littleredv1alpha1.PhaseRunning,
			conditions: []metav1.Condition{{
				Type:   littleredv1alpha1.ConditionOperationInProgress,
				Status: metav1.ConditionTrue,
				Reason: operationReasonRunning,
			}},
			want: fast,
		},
		{
			// Deliberately narrow. A stalled operation is permanent until a human acts
			// (there is no auto-exit timer, ADR-017), so polling it fast forever buys
			// nothing but the churn LR-042 removed.
			name:  "a stalled operation -> steady, not fast",
			phase: littleredv1alpha1.PhaseRunning,
			conditions: []metav1.Condition{{
				Type:   littleredv1alpha1.ConditionOperationInProgress,
				Status: metav1.ConditionTrue,
				Reason: operationReasonStalled,
			}},
			want: steady,
		},
		{
			// A blocked operation is head-of-line blocking, held loudly and
			// indefinitely — same reasoning as Stalled. Here the fast cadence comes from
			// the phase, not from the operation.
			name:  "a blocked operation, not running -> fast for the ordinary reason",
			phase: littleredv1alpha1.PhaseInitializing,
			conditions: []metav1.Condition{{
				Type:   littleredv1alpha1.ConditionOperationInProgress,
				Status: metav1.ConditionTrue,
				Reason: operationReasonBlocked,
			}},
			want: fast,
		},
		{
			// Converged is the steady state of the mechanism: the condition sits on
			// every sentinel instance, so it must not perturb the ordinary cadence.
			name:  "a converged operation condition changes nothing",
			phase: littleredv1alpha1.PhaseRunning,
			conditions: []metav1.Condition{{
				Type:   littleredv1alpha1.ConditionOperationInProgress,
				Status: metav1.ConditionFalse,
				Reason: operationReasonConverged,
			}},
			want: steady,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := requeueAfterNotRunning(tc.phase, tc.conditions, fast, steady)
			if got != tc.want {
				t.Errorf("requeueAfterNotRunning(%v, forsaken-conditions) = %v, want %v", tc.phase, got, tc.want)
			}
		})
	}
}
