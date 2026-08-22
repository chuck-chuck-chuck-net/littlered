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
)

// A non-Running instance is polled at the FAST interval (2s) so recovery rules get
// the loop iterations they need. That is right while converging and wrong forever:
// an instance that cannot converge — a Sentinel deployment captured by another that
// shares its master name is the worked example, where recovery is declined by design
// (ADR-015 §9.2) — sits at 30 reconciles/minute and ~120 log lines/minute
// indefinitely, with no progress possible.
//
// So after a grace period the cadence falls back to the steady interval. The grace
// MUST outlast every recovery cooldown, or backing off would starve the very rules
// the fast cadence exists for: Rule L 30s, ghost-master 30s, cluster wipe 120s.
func TestRequeueForNotRunning(t *testing.T) {
	const (
		fast   = 2 * time.Second
		steady = 30 * time.Second
	)
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *metav1.Time {
		t := metav1.NewTime(now.Add(-d))
		return &t
	}

	cases := []struct {
		name  string
		since *metav1.Time
		want  time.Duration
	}{
		{"no marker yet — assume converging", nil, fast},
		{"just went not-Ready", at(0), fast},
		{"still inside every recovery cooldown", at(90 * time.Second), fast},
		{"past the cluster-wipe cooldown but inside the grace", at(3 * time.Minute), fast},
		{"one second before the grace expires", at(stallGracePeriod - time.Second), fast},
		{"grace expired — stalled, back off", at(stallGracePeriod), steady},
		{"long stalled (the captured-instance case)", at(3 * time.Hour), steady},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := requeueForNotRunning(tc.since, now, fast, steady)
			if got != tc.want {
				t.Fatalf("requeueForNotRunning(%v) = %v, want %v", tc.since, got, tc.want)
			}
		})
	}
}

// The grace has to outlast the longest recovery cooldown in the tree, or the backoff
// would suppress a rule that was about to fire. Asserted rather than trusted to a
// comment, so raising a cooldown past it fails here instead of in the field.
func TestStallGraceOutlastsRecoveryCooldowns(t *testing.T) {
	longest := 120 * time.Second // LR-023 cluster wipe; Rule L and ghost-master are 30s
	if stallGracePeriod <= longest {
		t.Fatalf("stallGracePeriod %v must exceed the longest recovery cooldown %v",
			stallGracePeriod, longest)
	}
}
