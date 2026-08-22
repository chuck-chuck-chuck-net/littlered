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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// stallGracePeriod is how long an instance may sit not-Ready at the fast requeue
// cadence before the operator decides it is stalled rather than converging, and falls
// back to the steady interval.
//
// It MUST outlast every recovery cooldown in the tree, or backing off would starve the
// rules the fast cadence exists to feed: Rule L's leaderless cooldown (30s, LR-015),
// the ghost-master cooldown (30s, LR-024), and the cluster wipe-deadlock cooldown
// (120s, LR-023). Five minutes clears the longest of those by 2.5x.
// TestStallGraceOutlastsRecoveryCooldowns enforces the relationship.
const stallGracePeriod = 5 * time.Minute

// requeueForNotRunning returns the polling interval for an instance that is not
// Running.
//
// The fast interval is correct while an instance is converging: the healing rules are
// driven by the loop, and LR-012/LR-014/LR-017 are all about giving them enough
// iterations. It is wrong once an instance cannot converge at all. The worked example
// is a sentinel instance captured by another Sentinel deployment sharing its master
// name: recovery is declined by design (ADR-015 §9.2 — nothing survives to salvage,
// and the operator structurally cannot win the reclaim), so the instance sits at
// Ready=False forever. Measured on a live cluster in that state: 30 reconciles and
// ~120 log lines per minute, indefinitely, achieving nothing.
//
// notReadySince is the Ready condition's LastTransitionTime, which is already
// persisted and is exactly the marker needed — no new status field. It only moves when
// Ready actually flips, so a flap back to True resets the clock and a recovering
// instance returns to the fast cadence.
//
// Backing off slows the TIMER only. Owned-object events (StatefulSet status changes as
// pods become ready) and the Sentinel event subscriber still wake the reconcile
// immediately, so a stalled instance that starts making progress is not held at the
// slow cadence waiting for a tick.
func requeueForNotRunning(
	notReadySince *metav1.Time, now time.Time, fast, steady time.Duration,
) time.Duration {
	// No marker means the Ready condition has not been written yet: a brand-new
	// instance, which is converging by definition.
	if notReadySince == nil || notReadySince.IsZero() {
		return fast
	}
	if now.Sub(notReadySince.Time) >= stallGracePeriod {
		return steady
	}
	return fast
}
