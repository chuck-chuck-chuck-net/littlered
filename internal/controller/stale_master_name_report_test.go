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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// The §9.2 guard: a NORMAL rename must not be reported as a possible capture.
//
// G5's discriminator ("not one of our pods, and not flagged down") is byte-identical to
// planForsaken clause 3, and WP0 measured it going true during an ordinary rename — at
// t0+89.1s the just-replaced redis-0's address had already left the pod list while
// Sentinel had not yet flagged it down (down-after-milliseconds, 30s). The planner is
// correct as a pure function; what must not happen is the CALLER turning that reading
// into a Warning saying "do not rename to escape a capture" at the exact moment the
// owner is performing the rename the runbook asked for.
//
// The fixture below is that measured shape, not a synthetic one.

// renameWindow is midRename with the stale entry pointing at the address of the pod the
// rename has just replaced: gone from ValidIPs (the pod object is gone) and NOT yet
// flagged down (Sentinel is still inside down-after-milliseconds).
func renameWindow() *redisclient.ReplicationState {
	s := staleBase()
	const justReplaced = "10.0.0.42" // redis-0's previous address; deliberately not in ValidIPs
	for i, ip := range []string{senIP1, senIP2, senIP3} {
		setMasters(s, ip, []string{senPod1, senPod2, senPod3}[i],
			mon(desiredName, ipMaster, "master", ""),
			mon(staleName, justReplaced, "master", ""),
		)
	}
	return s
}

func TestPlanStaleMasterNameReport(t *testing.T) {
	t0 := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	armed := metav1.NewTime(t0)

	cases := []struct {
		name         string
		state        *redisclient.ReplicationState
		foreignSince *metav1.Time
		now          time.Time
		wantReason   string
		wantWarn     bool
		wantSince    *metav1.Time
		wantNotInMsg []string
	}{
		{
			name:  "rename window, first observation: a suspicion, never an accusation",
			state: renameWindow(),
			now:   t0,

			wantReason: staleNamesForeignSuspected,
			wantWarn:   false,
			wantSince:  &armed,
			// The accusing sentence the settled verdict carries must not appear yet.
			wantNotInMsg: []string{"may be captured", "escape a capture"},
		},
		{
			name:         "rename window, still inside the settle: still a suspicion, still no event",
			state:        renameWindow(),
			foreignSince: &armed,
			now:          t0.Add(staleMasterNameForeignCooldown - time.Second),

			wantReason:   staleNamesForeignSuspected,
			wantWarn:     false,
			wantSince:    &armed,
			wantNotInMsg: []string{"may be captured"},
		},
		{
			name:         "persisted past the settle: now it is reported, loudly",
			state:        renameWindow(),
			foreignSince: &armed,
			now:          t0.Add(staleMasterNameForeignCooldown),

			wantReason: staleNamesForeign,
			wantWarn:   true,
			wantSince:  &armed,
		},
		{
			name:  "converged: the suspicion timer is cleared, so a later one starts fresh",
			state: staleBase(),
			// A timer left over from an earlier window must not make the NEXT Foreign
			// reading instantly settled.
			foreignSince: &armed,
			now:          t0.Add(time.Hour),

			wantReason: staleNamesConverged,
			wantWarn:   false,
			wantSince:  nil,
		},
		{
			name:       "pruning is progress, not an alarm",
			state:      midRename(),
			now:        t0,
			wantReason: staleNamesPruning,
			wantWarn:   false,
			wantSince:  nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := planStaleMasterNames(tc.state, desiredName, 2, false)
			got := planStaleMasterNameReport(plan, tc.foreignSince, tc.now)

			if got.Reason != tc.wantReason {
				t.Fatalf("Reason = %q, want %q (message: %s)", got.Reason, tc.wantReason, got.Message)
			}
			if got.Warn != tc.wantWarn {
				t.Fatalf("Warn = %v, want %v: a Warning during an ordinary rename tells the owner "+
					"their supported field edit looks like a capture (message: %s)", got.Warn, tc.wantWarn, got.Message)
			}
			if !timesEqual(got.ForeignSince, tc.wantSince) {
				t.Fatalf("ForeignSince = %v, want %v", got.ForeignSince, tc.wantSince)
			}
			for _, s := range tc.wantNotInMsg {
				if strings.Contains(got.Message, s) {
					t.Fatalf("message must not accuse below the settle, got %q (contains %q)", got.Message, s)
				}
			}
			if got.Warn && !strings.Contains(got.Message, staleName) {
				t.Fatalf("a settled Foreign must name the stale entry, got %q", got.Message)
			}
		})
	}
}

// TestStaleMasterNameForeignSettleCoversTheMeasuredWindow pins the ONE number: WP0
// measured the false reading persisting for up to a full down-after-milliseconds (30s),
// so a shorter settle would not close it.
func TestStaleMasterNameForeignSettleCoversTheMeasuredWindow(t *testing.T) {
	if staleMasterNameForeignCooldown < 30*time.Second {
		t.Fatalf("settle = %v, want >= 30s (the measured down-after-milliseconds window)",
			staleMasterNameForeignCooldown)
	}
	if staleMasterNameForeignCooldown != forsakenCooldown {
		t.Logf("settle (%v) deliberately differs from forsakenCooldown (%v)",
			staleMasterNameForeignCooldown, forsakenCooldown)
	}
}
