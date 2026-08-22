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

// quarantinedCR is a sentinel-mode instance whose quarantine is ARMED in status —
// exactly what the operator sees on the pass AFTER setForsaken persisted the marker.
func quarantinedCR(since time.Time, attempts int32) *littleredv1alpha1.LittleRed {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	t := metav1.NewTime(since)
	lr.Status.QuarantinedSince = &t
	lr.Status.QuarantineAttempts = attempts
	return lr
}

// TestQuarantinedInstanceNeverGetsItsPodsPutBackByTheBuilders is the flap guard.
//
// The failure mode this protects against is a SEQUENCE, not a state, so no single-pass
// assertion catches it. Both sentinel StatefulSets are applied by r.apply — server-side
// apply with client.ForceOwnership — from reconcileSentinel steps that run BEFORE
// reconcileSentinelCluster, where the capture verdict and the quarantine decision live.
// So if the desired replica count is not a function of the quarantine at BUILD time, then
// on every pass of an active quarantine the builders force 3 back, the healing step takes
// it to 0 again, and the instance flaps 0→3→0 forever — actually scheduling pods, which
// rejoin the captor's quorum and re-pollute it. That is strictly worse than the log churn
// LR-042 removed.
//
// The assertion is therefore: on a pass where NO gather has happened and hence no verdict
// exists — only status.quarantinedSince — both builders must still stamp 0.
func TestQuarantinedInstanceNeverGetsItsPodsPutBackByTheBuilders(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

	// The interleaving that would flap, walked pass by pass. No gather is performed in
	// any of them: the only input is the CR.
	steps := []struct {
		name         string
		lr           *littleredv1alpha1.LittleRed
		redis, sntnl int32
	}{
		{
			name: "before any capture — the CR's own shape",
			lr: func() *littleredv1alpha1.LittleRed {
				l := newTestLittleRed(testLRName, testNamespace)
				l.Spec.Mode = ModeSentinel
				return l
			}(),
			redis: littleredv1alpha1.SentinelRedisReplicas, sntnl: sentinelProcessReplicas,
		},
		{
			name: "the pass right after arming — pods away, and NO gather has run yet",
			lr:   quarantinedCR(now, 1),
			// zero
		},
		{
			name: "still settling, still no verdict this pass — must NOT put 3 back",
			lr:   quarantinedCR(now.Add(-60*time.Second), 1),
			// zero
		},
		{
			name: "latched at the attempt limit — stays at zero indefinitely",
			lr:   quarantinedCR(now.Add(-10*time.Minute), quarantineAttemptLimit),
			// zero
		},
		{
			name: "released (the planner cleared the marker) — pods come back",
			lr: func() *littleredv1alpha1.LittleRed {
				l := quarantinedCR(now, 1)
				l.Status.QuarantinedSince = nil
				return l
			}(),
			redis: littleredv1alpha1.SentinelRedisReplicas, sntnl: sentinelProcessReplicas,
		},
	}

	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			redis, sntnl := sentinelDesiredReplicas(st.lr, now)

			// What the operator actually applies is what the BUILDERS stamp; assert on
			// those, not only on the decision function.
			redisSTS := buildRedisStatefulSetSentinel(st.lr, redis)
			sentinelSTS := buildSentinelStatefulSet(st.lr, sntnl)
			if got := *redisSTS.Spec.Replicas; got != st.redis {
				t.Errorf("redis StatefulSet .spec.replicas = %d, want %d", got, st.redis)
			}
			if got := *sentinelSTS.Spec.Replicas; got != st.sntnl {
				t.Errorf("sentinel StatefulSet .spec.replicas = %d, want %d", got, st.sntnl)
			}
		})
	}
}

// A fresh instance has an empty status. Nothing there may ever read as "quarantine
// armed" — the marker is a *metav1.Time, so the only two shapes to exclude are nil and
// the zero value, and this pins both.
func TestFreshInstanceIsNeverReadAsQuarantined(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name  string
		since *metav1.Time
	}{
		{"status never written", nil},
		{"marker present but zero-valued", &metav1.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lr := newTestLittleRed(testLRName, testNamespace)
			lr.Spec.Mode = ModeSentinel
			lr.Status.QuarantinedSince = tc.since
			redis, sntnl := sentinelDesiredReplicas(lr, now)
			if redis == 0 || sntnl == 0 {
				t.Errorf("a fresh instance was scaled to zero: redis=%d sentinel=%d", redis, sntnl)
			}
		})
	}
}

// The quarantine is a sentinel-mode verdict (planForsaken reads Sentinels; cluster and
// failover modes cannot reach it). A quarantine marker left in status by a mode change,
// or hand-edited in, must not scale anything down: this must be impossible, not merely
// unlikely.
func TestQuarantineNeverScalesDownANonSentinelInstance(t *testing.T) {
	now := time.Now()
	for _, mode := range []string{ModeStandalone, ModeCluster, ModeFailover} {
		t.Run(mode, func(t *testing.T) {
			lr := quarantinedCR(now, 1)
			lr.Spec.Mode = mode
			redis, sntnl := sentinelDesiredReplicas(lr, now)
			if redis == 0 || sntnl == 0 {
				t.Errorf("mode %q was scaled to zero by a sentinel quarantine marker: redis=%d sentinel=%d",
					mode, redis, sntnl)
			}
		})
	}
}
