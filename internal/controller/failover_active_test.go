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
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// Field names of the `SENTINEL master` reply, and the two flag strings the rows
// discriminate on. Named because the whole subject of LR-052 is that one of these
// keys was wrong, so they are worth having in exactly one place.
const (
	fieldName      = "name"
	fieldPort      = "port"
	fieldFlags     = "flags"
	fieldNumSlaves = "num-slaves"

	flagsFailingOver = "master,failover_in_progress"
	respEmptyArray   = "*0\r\n"
)

// failoverSentinel is a scripted fake Sentinel that answers
// `SENTINEL master <name>` with a caller-supplied field set, so a test can feed
// DetermineRealMaster the EXACT reply a real Sentinel emits mid-failover.
//
// The reply shape is the point of this harness. `addReplySentinelRedisInstance`
// emits `failover-state` only while the instance carries SRI_FAILOVER_IN_PROGRESS
// (redis/redis 8.0 src/sentinel.c:3435, valkey-io/valkey 8.1 src/sentinel.c:3317),
// so the steady-state reply omits the key entirely rather than sending "none" —
// which is why the fields are passed as an ordered slice and an absent key is
// genuinely absent rather than empty.
//
// It speaks RESP2 by answering HELLO with an error, exactly as twoNameSentinel
// does; the parser has unit tables for both wire shapes.
//
// Binds host:SentinelPort because GetSentinelState builds the address from
// littleredv1alpha1.SentinelPort. Distinct 127.0.0.0/8 hosts let several fakes
// coexist in one test.
func failoverSentinel(t *testing.T, host, name string, fields []string) {
	t.Helper()
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, littleredv1alpha1.SentinelPort))
	if err != nil {
		t.Skipf("cannot bind %s:%d (in use?): %v", host, littleredv1alpha1.SentinelPort, err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	record := func(fields []string) string {
		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(fields))
		for _, f := range fields {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(f), f)
		}
		return b.String()
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				for {
					args, err := readRESPCommand(r)
					if err != nil {
						return
					}
					reply := "+OK\r\n"
					switch {
					case len(args) > 0 && strings.EqualFold(args[0], "hello"):
						reply = "-ERR unknown command 'HELLO'\r\n"
					case len(args) >= 3 && strings.EqualFold(args[0], "sentinel") &&
						strings.EqualFold(args[1], "master"):
						if args[2] == name {
							reply = record(fields)
						} else {
							reply = "-ERR No such master with that name\r\n"
						}
					case len(args) >= 3 && strings.EqualFold(args[0], "sentinel") &&
						strings.EqualFold(args[1], "replicas"):
						reply = respEmptyArray
					case len(args) >= 2 && strings.EqualFold(args[0], "sentinel") &&
						strings.EqualFold(args[1], "masters"):
						reply = "*1\r\n" + record(fields)
					}
					if _, err := c.Write([]byte(reply)); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
}

// masterReply builds the `SENTINEL master` field list. `failoverState == ""` omits
// the key altogether, which is what a Sentinel with no failover running sends.
func masterReply(masterIP, flags, failoverState string) []string {
	f := []string{
		fieldName, "ns.inst",
		"ip", masterIP,
		fieldPort, "6379",
		fieldFlags, flags,
		"num-other-sentinels", "2",
		fieldNumSlaves, "2",
	}
	if failoverState != "" {
		f = append(f, "failover-state", failoverState)
	}
	return f
}

// gatherOne runs the REAL gather against one scripted Sentinel and returns the
// SentinelNodeState it produces. Going through the wire is the whole point: the
// defect this test guards was never in the guard, it was in the reply parsing —
// the operator asked for `failover-status`, a key neither Redis nor Valkey has
// ever emitted, so the state was populated from a miss and read empty forever
// (LR-052). A hand-built SentinelNodeState cannot see that.
func gatherOne(t *testing.T, host, podName string) *redisclient.SentinelNodeState {
	t.Helper()
	g := &operatorGatherer{}
	st, err := g.GetSentinelState(context.Background(), podName, host, "ns.inst")
	if err != nil {
		t.Fatalf("GetSentinelState(%s): %v", host, err)
	}
	if !st.Reachable || !st.Monitoring {
		t.Fatalf("GetSentinelState(%s): want reachable+monitoring, got %+v", host, st)
	}
	return st
}

// TestDetermineRealMasterFailoverActive is the LR-052 guard.
//
// `ReplicationState.FailoverActive` is a sound invariant — Rule A's "Sentinel is
// already working, stay out of its way" (pillar 3.5) and DetermineRealMaster's
// LR-004 fallback suppression both rest on it. Its evidence pipeline was broken:
// the field was populated from `result["failover-status"]`, and the key Sentinel
// actually emits is `failover-state`. So it was permanently false, and Rule A's
// second half had never fired in the product's history.
//
// The rows go through the real gather, so the assertion is about the wire.
func TestDetermineRealMasterFailoverActive(t *testing.T) {
	const host = "127.0.0.31"

	cases := []struct {
		name          string
		flags         string
		failoverState string
		want          bool
	}{
		{
			// The headline row. This is the reply a Sentinel that has elected itself
			// leader and is picking a replica actually sends.
			name:          "failover-state select_slave means a failover is running",
			flags:         flagsFailingOver,
			failoverState: "select_slave",
			want:          true,
		},
		{
			// The mirror: the flag alone must be enough. FailoverInProgress reads two
			// independent signals from the same reply so that a version emitting one
			// without the other is still read correctly.
			name:          "the failover_in_progress flag alone, no failover-state field at all",
			flags:         flagsFailingOver,
			failoverState: "",
			want:          true,
		},
		{
			// The idle/absent distinction, and it is the row that keeps this fix from
			// being strictly worse than the bug. Sentinel omits `failover-state` in
			// steady state, so if absence read as "in progress" then Rule A would skip
			// ALL healing on every pass, forever.
			name:          "steady state: no failover-state key in the reply",
			flags:         RoleMaster,
			failoverState: "",
			want:          false,
		},
		{
			name:          "explicit none is idle",
			flags:         RoleMaster,
			failoverState: "none",
			want:          false,
		},
		{
			// An unrecognised value must fail SAFE, i.e. read as in-flight. The idle
			// set is a whitelist for exactly this reason.
			name:          "a value neither project emits today fails safe",
			flags:         RoleMaster,
			failoverState: "brand_new_state",
			want:          true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failoverSentinel(t, host, "ns.inst", masterReply("10.0.0.1", tc.flags, tc.failoverState))

			sn := gatherOne(t, host, "sentinel-0")

			state := redisclient.NewReplicationState()
			state.SentinelNodes[host] = sn
			state.AddLiveTopologyIP("10.0.0.1")
			state.DetermineRealMaster()

			if state.FailoverReported != tc.want {
				t.Fatalf("FailoverReported = %v, want %v\n"+
					"  gathered sentinel state: %+v\n"+
					"  the operator must read Sentinel's own `failover-state` key (and the "+
					"`failover_in_progress` flag), not the never-emitted `failover-status` (LR-052)",
					state.FailoverReported, tc.want, sn)
			}
		})
	}
}

// TestFailoverActiveSuppressesTheRedisOnlyFallback pins the consequence that
// actually changes live behaviour, and it is the reason this milestone is not a
// two-line key rename.
//
// DetermineRealMaster's step 4 falls back to "whichever reachable Redis pod says
// role:master" when the Sentinels are split. LR-004 hardened that fallback so it
// is suppressed while Sentinel is mid-decision — falling back there would name a
// stale or restarting pod as master and let the operator issue RESETs that wipe
// Sentinel's failover state. `!s.FailoverActive` is that suppression, and it has
// been permanently open.
//
// The topology is a genuine mid-failover split: two reachable Sentinels naming two
// different valid pods, so neither reaches a majority and neither address is a
// ghost — the one shape where step 4 is actually reached.
func TestFailoverActiveSuppressesTheRedisOnlyFallback(t *testing.T) {
	const (
		hostA = "127.0.0.32"
		hostB = "127.0.0.33"
		podA  = "10.0.0.1"
		podB  = "10.0.0.2"
	)

	build := func(t *testing.T, failingOver bool) *redisclient.ReplicationState {
		t.Helper()
		flagsA, stateA := RoleMaster, ""
		if failingOver {
			flagsA, stateA = flagsFailingOver, "reconf_slaves"
		}
		failoverSentinel(t, hostA, "ns.inst", masterReply(podA, flagsA, stateA))
		failoverSentinel(t, hostB, "ns.inst", masterReply(podB, RoleMaster, ""))

		state := redisclient.NewReplicationState()
		state.SentinelNodes[hostA] = gatherOne(t, hostA, "sentinel-0")
		state.SentinelNodes[hostB] = gatherOne(t, hostB, "sentinel-1")
		state.AddLiveTopologyIP(podA)
		state.AddLiveTopologyIP(podB)
		state.RedisNodes[podA] = &redisclient.RedisNodeState{IP: podA, Role: RoleMaster, Reachable: true}
		state.RedisNodes[podB] = &redisclient.RedisNodeState{IP: podB, Role: roleSlave, Reachable: true}
		state.DetermineRealMaster()
		return state
	}

	t.Run("split with a failover in flight: no fallback, no master named", func(t *testing.T) {
		state := build(t, true)
		if !state.FailoverReported {
			t.Fatalf("precondition: FailoverReported = false, want true (the split must be mid-failover)")
		}
		if state.RealMasterIP != "" {
			t.Fatalf("RealMasterIP = %q, want \"\": while Sentinel is mid-decision the operator "+
				"must not fall back to the Redis-only view (LR-004)", state.RealMasterIP)
		}
	})

	t.Run("positive control: the same split with no failover still falls back", func(t *testing.T) {
		// Without this row the test above would pass against a mutant that always
		// suppresses the fallback — which would strand every legitimately split
		// instance at RealMasterIP == "".
		state := build(t, false)
		if state.FailoverReported {
			t.Fatalf("precondition: FailoverReported = true, want false")
		}
		if state.RealMasterIP != podA {
			t.Fatalf("RealMasterIP = %q, want %q: an ordinary Sentinel split must still "+
				"resolve through the LR-004 fallback", state.RealMasterIP, podA)
		}
	})
}
