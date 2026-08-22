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

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// state builds a ReplicationState from ours=our pod IPs.
func forsakenState(ourRedis, ourSentinels []string) *redisclient.ReplicationState {
	s := redisclient.NewReplicationState()
	for _, ip := range append(append([]string{}, ourRedis...), ourSentinels...) {
		s.ValidIPs[ip] = true
	}
	return s
}

func withSentinel(s *redisclient.ReplicationState, ip string, reachable, monitoring bool, masterIP, flags string) {
	s.SentinelNodes[ip] = &redisclient.SentinelNodeState{
		PodName: "sentinel-" + ip, IP: ip,
		Reachable: reachable, Monitoring: monitoring,
		MasterIP: masterIP, MasterFlags: flags,
	}
}

func withRedis(s *redisclient.ReplicationState, ip string, reachable bool, role string) {
	s.RedisNodes[ip] = &redisclient.RedisNodeState{
		PodName: "redis-" + ip, IP: ip, Reachable: reachable, Role: role,
	}
}

// A forsaken instance is one CAPTURED by another Sentinel deployment sharing its
// master name: every reachable Sentinel serves a live master that is not ours, and no
// pod of ours is a master any more. Recovery is declined (ADR-015 §9.2), so the
// verdict exists to let the operator stop managing it rather than to fix it.
//
// The predicate has to be conservative in a specific direction: a false positive
// parks a healthy instance, which is far worse than a false negative (which merely
// leaves today's behaviour). So every clause below must hold, and the state must
// persist past a cooldown.
func TestPlanForsaken(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) *metav1.Time { t := metav1.NewTime(now.Add(-d)); return &t }

	const (
		ourM, ourR1, ourR2  = "10.0.0.1", "10.0.0.2", "10.0.0.3"
		ourS1, ourS2, ourS3 = "10.0.1.1", "10.0.1.2", "10.0.1.3"
		foreign             = "10.9.9.9"
	)

	// the captured shape: all sentinels on a live foreign master, no master of ours
	captured := func() *redisclient.ReplicationState {
		s := forsakenState([]string{ourM, ourR1, ourR2}, []string{ourS1, ourS2, ourS3})
		for _, sip := range []string{ourS1, ourS2, ourS3} {
			withSentinel(s, sip, true, true, foreign, "master")
		}
		for _, rip := range []string{ourM, ourR1, ourR2} {
			withRedis(s, rip, true, "slave")
		}
		return s
	}

	cases := []struct {
		name         string
		state        *redisclient.ReplicationState
		since        *metav1.Time
		wantCaptured bool
		wantForsaken bool
	}{
		{"captured, past cooldown", captured(), ago(forsakenCooldown), true, true},
		{"captured, still inside cooldown", captured(), ago(5 * time.Second), true, false},
		{"captured, timer not armed yet", captured(), nil, true, false},

		{"healthy: sentinels on our own master", func() *redisclient.ReplicationState {
			s := forsakenState([]string{ourM, ourR1, ourR2}, []string{ourS1, ourS2, ourS3})
			for _, sip := range []string{ourS1, ourS2, ourS3} {
				withSentinel(s, sip, true, true, ourM, "master")
			}
			withRedis(s, ourM, true, RoleMaster)
			withRedis(s, ourR1, true, "slave")
			return s
		}(), ago(time.Hour), false, false},

		{"ordinary dead ghost master is NOT capture (flagged down)", func() *redisclient.ReplicationState {
			s := captured()
			for _, sip := range []string{ourS1, ourS2, ourS3} {
				withSentinel(s, sip, true, true, foreign, "s_down,o_down,master")
			}
			return s
		}(), ago(time.Hour), false, false},

		{"one of our pods is still master — not forsaken, still ours to heal", func() *redisclient.ReplicationState {
			s := captured()
			withRedis(s, ourM, true, RoleMaster)
			return s
		}(), ago(time.Hour), false, false},

		{"sentinels disagree — a transition, not a settled capture", func() *redisclient.ReplicationState {
			s := captured()
			withSentinel(s, ourS3, true, true, ourM, "master")
			return s
		}(), ago(time.Hour), false, false},

		{"no reachable monitoring sentinel — we know nothing", func() *redisclient.ReplicationState {
			s := forsakenState([]string{ourM}, []string{ourS1})
			withSentinel(s, ourS1, false, false, "", "")
			withRedis(s, ourM, true, "slave")
			return s
		}(), ago(time.Hour), false, false},

		{"our master merely unreachable does not save us", func() *redisclient.ReplicationState {
			// Clause 4 asks whether a REACHABLE pod of ours is a master. An
			// unreachable one does not count, and that is deliberate: combined with
			// every Sentinel unanimously serving a live foreign address, and held for
			// the cooldown, "our master stopped answering" is what a capture looks
			// like from here — the pod was repointed at the captor and its old
			// mastership is gone. A network blip alone cannot produce the other three
			// clauses.
			s := captured()
			withRedis(s, ourM, false, RoleMaster)
			return s
		}(), ago(time.Hour), true, true},

		{"bare sentinels are Rule L's job, not ours", func() *redisclient.ReplicationState {
			s := forsakenState([]string{ourM}, []string{ourS1, ourS2, ourS3})
			for _, sip := range []string{ourS1, ourS2, ourS3} {
				withSentinel(s, sip, true, false, "", "")
			}
			withRedis(s, ourM, true, "slave")
			return s
		}(), ago(time.Hour), false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planForsaken(tc.state, tc.since, now)
			if got.Captured != tc.wantCaptured {
				t.Errorf("Captured = %v, want %v", got.Captured, tc.wantCaptured)
			}
			if got.Forsaken != tc.wantForsaken {
				t.Errorf("Forsaken = %v, want %v", got.Forsaken, tc.wantForsaken)
			}
			if tc.wantCaptured && got.ForeignMaster != foreign {
				t.Errorf("ForeignMaster = %q, want %q", got.ForeignMaster, foreign)
			}
		})
	}
}
