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
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// The quarantine lifecycle of a forsaken instance (LR-044).
//
// A captured instance is unrecoverable by design (ADR-015 §9.2) and, while its pods
// keep replicating from the captor's master, it actively poisons that captor's
// Sentinel failover-candidate set. Quarantine takes the victim's pods away so the
// captor heals through Rule D, waits out a settling period, and then lets the pods
// back so Rule L's no-data reseed re-bootstraps them. It is bounded: after N attempts
// the instance stays down, because every recapture re-pollutes a healthy neighbour.
//
// The direction of conservatism is the same as planForsaken's, and stricter: this
// planner authorizes DELETING pods. A false negative leaves today's behaviour; a false
// positive destroys a workload. So every data clause refuses.
func TestPlanQuarantine(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) *metav1.Time { t := metav1.NewTime(now.Add(-d)); return &t }

	cases := []struct {
		name  string
		in    quarantineInput
		want  quarantinePhase
		zero  bool // ScaleToZero
		arm   bool
		clear bool
		next  int32 // NextAttempts
		limit int32
	}{
		{
			name:  "not captured — nothing to do",
			in:    quarantineInput{Now: now},
			want:  quarantineNone,
			limit: quarantineAttemptLimit,
		},
		{
			name:  "captured but inside the forsakenCooldown — suspicion is not a verdict",
			in:    quarantineInput{Captured: true, Now: now},
			want:  quarantineHoldSuspected,
			limit: quarantineAttemptLimit,
		},
		{
			name:  "forsaken — quarantine: arm the marker and take the pods away",
			in:    quarantineInput{Captured: true, Forsaken: true, Now: now},
			want:  quarantineStart,
			zero:  true,
			arm:   true,
			next:  1,
			limit: quarantineAttemptLimit,
		},
		{
			name: "quarantined and still settling — stay at zero",
			in: quarantineInput{
				QuarantinedSince: ago(60 * time.Second), Attempts: 1, Now: now,
			},
			want:  quarantineSettling,
			zero:  true,
			next:  1,
			limit: quarantineAttemptLimit,
		},
		{
			name: "settled and attempts remain — release, let Rule L reseed",
			in: quarantineInput{
				QuarantinedSince: ago(quarantineSettlePeriod), Attempts: 1, Now: now,
			},
			want:  quarantineRelease,
			clear: true,
			next:  1,
			limit: quarantineAttemptLimit,
		},
		{
			name: "recaptured with attempts below the limit — quarantine again",
			in: quarantineInput{
				Captured: true, Forsaken: true, Attempts: 1, Now: now,
			},
			want:  quarantineStart,
			zero:  true,
			arm:   true,
			next:  2,
			limit: quarantineAttemptLimit,
		},
		{
			name: "attempts have reached the limit — latch, never release",
			in: quarantineInput{
				QuarantinedSince: ago(10 * time.Minute), Attempts: 2, Now: now,
			},
			want:  quarantineLatched,
			zero:  true,
			next:  2,
			limit: quarantineAttemptLimit,
		},
		{
			name: "known-dangerous config (auth off + legacy master name) latches after one attempt",
			in: quarantineInput{
				QuarantinedSince: ago(10 * time.Minute), Attempts: 1, Dangerous: true, Now: now,
			},
			want:  quarantineLatched,
			zero:  true,
			next:  1,
			limit: quarantineAttemptLimitDangerous,
		},
		{
			name: "a reachable pod holds data that is not the captor's copy — NEVER quarantine",
			in: quarantineInput{
				Captured: true, Forsaken: true, DataAtRisk: true, Now: now,
			},
			want:  quarantineHoldDataPresent,
			limit: quarantineAttemptLimit,
		},
		{
			name: "a pod of ours cannot be proven empty — NEVER quarantine",
			in: quarantineInput{
				Captured: true, Forsaken: true, DataUnverified: true, Now: now,
			},
			want:  quarantineHoldDataUnknown,
			limit: quarantineAttemptLimit,
		},
		{
			name: "no verdict this pass (pre-gather) — an active quarantine still holds",
			in: quarantineInput{
				QuarantinedSince: ago(30 * time.Second), Attempts: 1, Now: now,
			},
			want:  quarantineSettling,
			zero:  true,
			next:  1,
			limit: quarantineAttemptLimit,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planQuarantine(tc.in)
			if got.Phase != tc.want {
				t.Errorf("Phase = %q, want %q", got.Phase, tc.want)
			}
			if got.ScaleToZero != tc.zero {
				t.Errorf("ScaleToZero = %v, want %v", got.ScaleToZero, tc.zero)
			}
			if got.Arm != tc.arm {
				t.Errorf("Arm = %v, want %v", got.Arm, tc.arm)
			}
			if got.Clear != tc.clear {
				t.Errorf("Clear = %v, want %v", got.Clear, tc.clear)
			}
			if got.NextAttempts != tc.next {
				t.Errorf("NextAttempts = %d, want %d", got.NextAttempts, tc.next)
			}
			if got.AttemptLimit != tc.limit {
				t.Errorf("AttemptLimit = %d, want %d", got.AttemptLimit, tc.limit)
			}
		})
	}
}

// quarantineDataRisk is the clause that makes the quarantine provably lossless rather
// than lossless-by-argument. Keys on a pod that is a link-up replica of the CAPTOR's
// master are the captor's own dataset, replicated in — destroying that copy loses
// nothing. Keys anywhere else may be the only copy in existence, which is exactly the
// "replication was blocked before the sync started" path ADR-015 §9.2 could not rule
// out on timing alone.
func TestQuarantineDataRisk(t *testing.T) {
	const foreign = "10.9.9.9"
	const ourM, ourR1 = "10.0.0.1", "10.0.0.2"

	node := func(ip string, reachable bool, keys int64, masterHost, link string) *redisclient.RedisNodeState {
		return &redisclient.RedisNodeState{
			PodName: "redis-" + ip, IP: ip, Reachable: reachable, Keys: keys,
			Role: "slave", MasterHost: masterHost, LinkStatus: link,
		}
	}
	// authFailed is a pod that ANSWERED us — in the protocol, to refuse our
	// credential (LR-051). It is unreachable to the gather and very much alive on
	// the wire.
	authFailed := func(ip string) *redisclient.RedisNodeState {
		return &redisclient.RedisNodeState{
			PodName: "redis-" + ip, IP: ip, Reachable: false,
			ProbeFailure: redisclient.ProbeAuthFailed,
			ProbeError:   wrongPassReply,
		}
	}
	build := func(nodes ...*redisclient.RedisNodeState) *redisclient.ReplicationState {
		s := redisclient.NewReplicationState()
		for _, n := range nodes {
			s.AddLiveTopologyIP(n.IP)
			s.RedisNodes[n.IP] = n
		}
		return s
	}

	// Kubelet readiness, keyed by pod name exactly as the reconciler builds it.
	ready := func(names ...string) map[string]bool {
		m := map[string]bool{"redis-" + ourM: false, "redis-" + ourR1: false}
		for _, n := range names {
			m[n] = true
		}
		return m
	}
	allReady := ready("redis-"+ourM, "redis-"+ourR1)

	cases := []struct {
		name              string
		state             *redisclient.ReplicationState
		ready             map[string]bool
		atRisk, unverifid bool
	}{
		{
			name:  "all reachable and empty",
			state: build(node(ourM, true, 0, foreign, "up"), node(ourR1, true, 0, foreign, "up")),
			ready: allReady,
		},
		{
			name:  "keys held as a link-up replica of the captor are the captor's copy",
			state: build(node(ourM, true, 500, foreign, "up"), node(ourR1, true, 500, foreign, "up")),
			ready: allReady,
		},
		{
			name:   "keys on a pod that is NOT following the captor may be the only copy",
			state:  build(node(ourM, true, 500, "", ""), node(ourR1, true, 0, foreign, "up")),
			ready:  allReady,
			atRisk: true,
		},
		{
			name:   "keys on a pod whose link to the captor is down are unexplained",
			state:  build(node(ourM, true, 500, foreign, "down")),
			ready:  allReady,
			atRisk: true,
		},
		{
			name:      "a pod we cannot dial but the kubelet reports Ready cannot be proven empty",
			state:     build(node(ourM, true, 0, foreign, "up"), node(ourR1, false, 0, "", "")),
			ready:     allReady,
			unverifid: true,
		},
		{
			name: "a pod we cannot dial and whose redis is NOT Ready is provably empty",
			// LR-023's signal: the kubelet's local probe is authoritative and
			// blackhole-proof, and in a pure in-memory instance a not-Ready redis holds
			// no data. Blocking on it would hold the quarantine open forever on a
			// crash-looping pod, keeping the captor dirty for exactly as long.
			state: build(node(ourM, true, 0, foreign, "up"), node(ourR1, false, 0, "", "")),
			ready: ready("redis-" + ourM),
		},
		{
			name: "a pod the kubelet has no view of at all is NOT assumed empty",
			// Absent from the map means we could not establish readiness either; the
			// conservative direction is the same as an unreachable Ready pod.
			state:     build(node(ourM, true, 0, foreign, "up"), node(ourR1, false, 0, "", "")),
			ready:     map[string]bool{"redis-" + ourM: true},
			unverifid: true,
		},
		{
			name: "a pod that REFUSED our credential is never provably empty, even not-Ready",
			// LR-051. The readiness clause above rests on LR-023: a not-Ready redis is
			// DOWN, so it holds nothing. An AuthFailed pod falsifies that premise — it
			// answered, which means a live server is running there — so the kubelet's
			// negative cannot overrule our own positive observation.
			//
			// And the combination is not exotic, it is the CHARACTERISTIC shape of a
			// credential mismatch: sentinel-mode readiness requires role:master or
			// master_link_status:up, and a mismatch is exactly what breaks replication,
			// so a live pod full of data reads not-Ready while refusing our probes.
			state:     build(node(ourM, true, 0, foreign, "up"), authFailed(ourR1)),
			ready:     ready("redis-" + ourM),
			unverifid: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			atRisk, unverified := quarantineDataRisk(tc.state, foreign, tc.ready)
			if atRisk != tc.atRisk {
				t.Errorf("atRisk = %v, want %v", atRisk, tc.atRisk)
			}
			if unverified != tc.unverifid {
				t.Errorf("unverified = %v, want %v", unverified, tc.unverifid)
			}
		})
	}
}

// quarantineConfigDangerous picks the N=1 policy for an instance whose own
// configuration is what makes a capture reachable. Both halves must be true: the
// shared legacy master name is what lets a foreign quorum absorb us, and auth is the
// only thing closing the narrower address-adoption path.
//
// Green from birth (it pins a two-clause predicate rather than driving new code); the
// mutation check is that flipping either clause to an OR fails the two mixed rows.
func TestQuarantineConfigDangerous(t *testing.T) {
	lr := func(auth bool, name string) *littleredv1alpha1.LittleRed {
		l := &littleredv1alpha1.LittleRed{}
		l.Spec.Auth.Enabled = auth
		if name != "" {
			l.Spec.Sentinel = &littleredv1alpha1.SentinelSpec{MasterName: name}
		}
		return l
	}
	cases := []struct {
		name string
		lr   *littleredv1alpha1.LittleRed
		want bool
	}{
		{"auth off, master name unset (legacy fallback)", lr(false, ""), true},
		{"auth off, master name set to mymaster explicitly — just as capturable", lr(false, littleredv1alpha1.LegacySentinelMasterName), true},
		{"auth off, scoped master name", lr(false, "team-a.cache"), false},
		{"auth on, legacy master name", lr(true, littleredv1alpha1.LegacySentinelMasterName), false},
		{"auth on, scoped master name", lr(true, "team-a.cache"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := quarantineConfigDangerous(tc.lr); got != tc.want {
				t.Errorf("quarantineConfigDangerous = %v, want %v", got, tc.want)
			}
		})
	}
}
