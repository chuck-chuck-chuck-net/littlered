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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// The §9.2 guard: a NORMAL rename must not be reported as a possible capture.
//
// G5's discriminator ("not one of our pods, and not flagged down") is byte-identical to
// planForsaken clause 3, and WP0 measured it going true during an ordinary rename — at
// t0+89.1s the just-replaced redis-0's address had already left the pod list while
// Sentinel had not yet flagged it down (down-after-milliseconds, 30s).
//
// M3 answered that with a caller-side settling period (`ForeignSuspected`,
// `status.staleMasterNameForeignSince`). **LR-050 deleted all three surfaces** and moved
// the answer into the planner, gated on our own Redis StatefulSet being settled: the
// settle was a margin against a user-settable timer, while the gate closes the window at
// its source and needs no state at all. So this file no longer tests a clock — the
// rename-window rows live in `rollout_attribution_gate_test.go`, and what is left here
// is the rendering: which plan reason becomes which condition, and which one is allowed
// to raise its voice.

// renameWindow is the measured shape: the stale entry points at the address of the pod
// the rename has just replaced — gone from ValidIPs (the pod object is gone) and NOT yet
// flagged down (Sentinel is still inside down-after-milliseconds).
func renameWindow() *redisclient.ReplicationState {
	s := staleBase()
	for i, ip := range []string{senIP1, senIP2, senIP3} {
		setMasters(s, ip, []string{senPod1, senPod2, senPod3}[i],
			mon(desiredName, ipMaster, "master", ""),
			mon(staleName, justReplacedIP, "master", ""),
		)
	}
	return s
}

func TestPlanStaleMasterNameReport(t *testing.T) {
	cases := []struct {
		name         string
		state        *redisclient.ReplicationState
		rolling      bool
		wantStatus   metav1.ConditionStatus
		wantReason   string
		wantWarn     bool
		wantNotInMsg []string
	}{
		{
			// The happy path of the whole feature: an ordinary rename, mid-roll. The
			// operator must not tell the owner their supported field edit looks like a
			// capture — no accusation, and no event.
			name:  "rename window, mid-roll: deferred, and it accuses nobody",
			state: renameWindow(), rolling: true,
			wantStatus: metav1.ConditionTrue, wantReason: staleNamesDeferred, wantWarn: false,
			wantNotInMsg: []string{"may be captured", "escape a capture"},
		},
		{
			// The positive control: the identical gather on a SETTLED instance is the
			// §7.3 trap and must still be reported, loudly.
			name:  "the same reading on a settled instance is reported, loudly",
			state: renameWindow(), rolling: false,
			wantStatus: metav1.ConditionTrue, wantReason: staleNamesForeign, wantWarn: true,
		},
		{
			name:  "converged is the quiet steady state",
			state: staleBase(), rolling: false,
			wantStatus: metav1.ConditionFalse, wantReason: staleNamesConverged, wantWarn: false,
		},
		{
			name:  "pruning is progress, not an alarm",
			state: midRename(), rolling: false,
			wantStatus: metav1.ConditionTrue, wantReason: staleNamesPruning, wantWarn: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := planStaleMasterNames(tc.state, desiredName, 2, false, tc.rolling)
			got := planStaleMasterNameReport(plan)

			if got.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", got.Status, tc.wantStatus)
			}
			if got.Reason != tc.wantReason {
				t.Fatalf("Reason = %q, want %q (message: %s)", got.Reason, tc.wantReason, got.Message)
			}
			if got.Warn != tc.wantWarn {
				t.Fatalf("Warn = %v, want %v: a Warning during an ordinary rename tells the owner "+
					"their supported field edit looks like a capture (message: %s)", got.Warn, tc.wantWarn, got.Message)
			}
			for _, s := range tc.wantNotInMsg {
				if strings.Contains(got.Message, s) {
					t.Fatalf("message must not accuse while rolling, got %q (contains %q)", got.Message, s)
				}
			}
			if got.Warn && !strings.Contains(got.Message, staleName) {
				t.Fatalf("a Foreign report must name the stale entry, got %q", got.Message)
			}
		})
	}
}
