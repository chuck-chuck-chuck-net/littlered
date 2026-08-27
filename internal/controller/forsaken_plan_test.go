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
			got := planForsaken(tc.state, tc.since, now, false)
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

// withMonitoredSentinel builds a Sentinel that answers the DESIRED-name probe with
// (masterIP, flags) — exactly what withSentinel does — and additionally reports the
// full `SENTINEL MASTERS` list it carries. Separate helper so the pre-existing table
// above stays byte-for-byte untouched.
func withMonitoredSentinel(
	s *redisclient.ReplicationState, ip string, monitoring bool, masterIP, flags string,
	masters ...redisclient.MonitoredMaster,
) {
	s.SentinelNodes[ip] = &redisclient.SentinelNodeState{
		PodName: "sentinel-" + ip, IP: ip,
		Reachable: true, Monitoring: monitoring,
		MasterIP: masterIP, MasterFlags: flags,
		MonitoredMasters: masters,
	}
}

// A capture under a STALE master name is still a capture.
//
// planForsaken used to read only sn.MasterIP / sn.MasterFlags — the single-name
// probe's answer about the name we currently WANT. So renaming a captured instance
// (which is exactly what the LR-039/LR-042 runbook tempts an owner into) made the
// verdict evaporate: the new name reads bare, clause 1 fails, and with the verdict
// goes ADR-016's quarantine, which is the thing that heals BOTH sides. Design
// §7.3.
//
// The change is a widening of what the four clauses range over — from "the desired
// name's master" to "every master this Sentinel monitors" — and nothing else. Their
// intent is unchanged, which is why the whole pre-existing table above still passes
// with no row edited.
//
// Conservatism still runs one way (a false positive parks a live instance), and the
// widening is where that could go wrong: an ordinary rename transiently presents the
// capture signature (WP0 measured `Forsaken=False/CaptureSuspected` at t0+89.1s of a
// healthy rename, with all four clauses genuinely holding). The rename rows below
// are built from that measured shape rather than a synthetic one.
func TestPlanForsakenIsNameAgnostic(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) *metav1.Time { t := metav1.NewTime(now.Add(-d)); return &t }

	const (
		ourM, ourR1, ourR2  = "10.0.0.1", "10.0.0.2", "10.0.0.3"
		ourS1, ourS2, ourS3 = "10.0.1.1", "10.0.1.2", "10.0.1.3"
		captor              = "10.9.9.9"
		desired             = "team-a.cache"
		stale               = "mymaster"
		// The address WP0 measured: the just-replaced redis-0, no longer in
		// ValidIPs and not yet s_down (down-after-milliseconds still running).
		replacedRedis0 = "10.233.192.110"
	)
	ourSentinels := []string{ourS1, ourS2, ourS3}

	// ours is the pod set; the replaced redis-0 is deliberately NOT in it.
	ours := func() *redisclient.ReplicationState {
		return forsakenState([]string{ourM, ourR1, ourR2}, ourSentinels)
	}
	master := redisclient.MonitoredMaster{Name: desired}
	old := redisclient.MonitoredMaster{Name: stale}
	at := func(m redisclient.MonitoredMaster, ip, flags string) redisclient.MonitoredMaster {
		m.IP, m.Flags = ip, flags
		return m
	}

	cases := []struct {
		name         string
		state        *redisclient.ReplicationState
		since        *metav1.Time
		wantCaptured bool
		wantForsaken bool
		wantForeign  string
	}{
		{
			// THE load-bearing row. Renaming to escape a capture: the Sentinels
			// still carry the old name pointing at the captor's live master, the
			// new name reads bare, and no pod of ours is a master because they are
			// all replicas of the foreign master. Before this change the verdict
			// vanished here and the quarantine with it.
			name: "capture under a stale name while the desired name reads bare",
			state: func() *redisclient.ReplicationState {
				s := ours()
				for _, sip := range ourSentinels {
					withMonitoredSentinel(s, sip, false, "", "", at(old, captor, "master"))
				}
				for _, rip := range []string{ourM, ourR1, ourR2} {
					withRedis(s, rip, true, "slave")
				}
				return s
			}(), since: ago(time.Hour), wantCaptured: true, wantForsaken: true, wantForeign: captor,
		},
		{
			// Both names present, both on the captor. Same verdict — the desired
			// name being registered by Rule 0 on top of the stale one must not
			// change the answer.
			name: "capture visible under both names",
			state: func() *redisclient.ReplicationState {
				s := ours()
				for _, sip := range ourSentinels {
					withMonitoredSentinel(s, sip, true, captor, "master",
						at(master, captor, "master"), at(old, captor, "master"))
				}
				for _, rip := range []string{ourM, ourR1, ourR2} {
					withRedis(s, rip, true, "slave")
				}
				return s
			}(), since: ago(time.Hour), wantCaptured: true, wantForsaken: true, wantForeign: captor,
		},
		{
			// WP0's measured rename window, t0+89.1s: the master pod has just been
			// replaced, BOTH names still name its address, that address is a ghost
			// (not in ValidIPs) and not yet flagged down, and no reachable pod of
			// ours is a master. All four clauses hold — they held before this
			// change too, on the desired name alone.
			//
			// This row exists to prove the widening does not make it WORSE. The
			// verdict is a suspicion; what stops it parking a healthy instance is
			// forsakenCooldown, which is asserted here as the backstop it is.
			name: "rename window: both names on the just-replaced master, inside the cooldown",
			state: func() *redisclient.ReplicationState {
				s := ours()
				for _, sip := range ourSentinels {
					withMonitoredSentinel(s, sip, true, replacedRedis0, "master",
						at(master, replacedRedis0, "master"), at(old, replacedRedis0, "master"))
				}
				withRedis(s, ourR1, true, "slave")
				withRedis(s, ourR2, true, "slave")
				return s
			}(), since: ago(5 * time.Second), wantCaptured: true, wantForsaken: false, wantForeign: replacedRedis0,
		},
		{
			// The other half of the same measured window: for 88.5s the two names
			// named DIFFERENT addresses (56.6s of it two different live pods),
			// because redis-0's baked stale-name preStop forced a +switch-master
			// under the old name while the operator's correction had not yet
			// repointed the new one.
			//
			// Two names disagreeing is a transition, and clause 2 says transitions
			// are not verdicts. So the widening REMOVES a suspicion the desired-name
			// view would have raised here — the safe direction, and the reason a
			// mutant that ignores stale names fails this row.
			name: "rename window: the two names disagree — a transition, not a verdict",
			state: func() *redisclient.ReplicationState {
				s := ours()
				for _, sip := range ourSentinels {
					withMonitoredSentinel(s, sip, true, replacedRedis0, "master",
						at(master, replacedRedis0, "master"), at(old, ourR1, "master"))
				}
				withRedis(s, ourR1, true, "slave")
				withRedis(s, ourR2, true, "slave")
				return s
			}(), since: ago(time.Hour), wantCaptured: false, wantForsaken: false,
		},
		{
			// Pass 1 of an ordinary rename on a HEALTHY instance: the desired name
			// is not registered yet and the stale name names our own live master.
			// Not ours ⇒ ghost is the whole of clause 3, and this address IS ours.
			name: "rename pass 1: the stale name still names our own live master",
			state: func() *redisclient.ReplicationState {
				s := ours()
				for _, sip := range ourSentinels {
					withMonitoredSentinel(s, sip, false, "", "", at(old, ourM, "master"))
				}
				withRedis(s, ourM, true, RoleMaster)
				withRedis(s, ourR1, true, "slave")
				return s
			}(), since: ago(time.Hour), wantCaptured: false, wantForsaken: false,
		},
		{
			// Clause 3 is unchanged and still refuses a flagged-down address,
			// whichever name carries it. An address that is not ours and is not
			// answering is indistinguishable from our own dead ex-master (LR-024's
			// subject), and calling it a capture would park live instances.
			//
			// Consequence, recorded rather than fixed: a captor whose master is
			// transiently s_down is NOT caught here. See the report.
			name: "a flagged-down address under a stale name is still not a capture",
			state: func() *redisclient.ReplicationState {
				s := ours()
				for _, sip := range ourSentinels {
					withMonitoredSentinel(s, sip, false, "", "", at(old, captor, "s_down,master"))
				}
				for _, rip := range []string{ourM, ourR1, ourR2} {
					withRedis(s, rip, true, "slave")
				}
				return s
			}(), since: ago(time.Hour), wantCaptured: false, wantForsaken: false,
		},
		{
			// A partial capture: two Sentinels carry only the captured stale name,
			// the third still serves the desired name from our own master. Clause 2
			// disagreement plus clause 3 — either way, no verdict. LR-013's lesson
			// is that a partial capture is where a wrong action does the damage.
			name: "partial capture: one Sentinel still on a master of ours",
			state: func() *redisclient.ReplicationState {
				s := ours()
				withMonitoredSentinel(s, ourS1, false, "", "", at(old, captor, "master"))
				withMonitoredSentinel(s, ourS2, false, "", "", at(old, captor, "master"))
				withMonitoredSentinel(s, ourS3, true, ourM, "master", at(master, ourM, "master"))
				for _, rip := range []string{ourM, ourR1, ourR2} {
					withRedis(s, rip, true, "slave")
				}
				return s
			}(), since: ago(time.Hour), wantCaptured: false, wantForsaken: false,
		},
		{
			// The extra SENTINEL MASTERS round trip degrades to an EMPTY list, not
			// to Reachable:false (LR-041). Emptiness is "we could not read it", so
			// the desired-name view must still carry the verdict on its own —
			// i.e. exactly the pre-change behaviour.
			name: "unreadable master list falls back to the desired-name view",
			state: func() *redisclient.ReplicationState {
				s := ours()
				for _, sip := range ourSentinels {
					withMonitoredSentinel(s, sip, true, captor, "master")
				}
				for _, rip := range []string{ourM, ourR1, ourR2} {
					withRedis(s, rip, true, "slave")
				}
				return s
			}(), since: ago(time.Hour), wantCaptured: true, wantForsaken: true, wantForeign: captor,
		},
		{
			// Mirrors the pre-existing "a monitoring Sentinel with no master
			// address" abort, one level down: an entry Sentinel reported without an
			// address cannot be attributed to anything, so it aborts the verdict
			// rather than being skipped.
			name: "a stale entry with no address aborts the verdict",
			state: func() *redisclient.ReplicationState {
				s := ours()
				for _, sip := range ourSentinels {
					withMonitoredSentinel(s, sip, true, captor, "master",
						at(master, captor, "master"), at(old, "", "master"))
				}
				for _, rip := range []string{ourM, ourR1, ourR2} {
					withRedis(s, rip, true, "slave")
				}
				return s
			}(), since: ago(time.Hour), wantCaptured: false, wantForsaken: false,
		},
		{
			// Clause 4 is untouched by the widening: while a reachable pod of ours
			// is still a master there is something to heal back toward, whatever
			// the Sentinels carry.
			name: "a master of ours still standing beats a stale-name capture signature",
			state: func() *redisclient.ReplicationState {
				s := ours()
				for _, sip := range ourSentinels {
					withMonitoredSentinel(s, sip, false, "", "", at(old, captor, "master"))
				}
				withRedis(s, ourM, true, RoleMaster)
				withRedis(s, ourR1, true, "slave")
				return s
			}(), since: ago(time.Hour), wantCaptured: false, wantForsaken: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planForsaken(tc.state, tc.since, now, false)
			if got.Captured != tc.wantCaptured {
				t.Errorf("Captured = %v, want %v", got.Captured, tc.wantCaptured)
			}
			if got.Forsaken != tc.wantForsaken {
				t.Errorf("Forsaken = %v, want %v", got.Forsaken, tc.wantForsaken)
			}
			if got.ForeignMaster != tc.wantForeign {
				t.Errorf("ForeignMaster = %q, want %q", got.ForeignMaster, tc.wantForeign)
			}
		})
	}
}
