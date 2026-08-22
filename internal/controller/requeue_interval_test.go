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
