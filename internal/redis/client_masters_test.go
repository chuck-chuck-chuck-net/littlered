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

package redis

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// stateSelectSlave is one of sentinelFailoverStateStr's values; see MonitoredMaster.
const (
	stateSelectSlave = "select_slave"
	// nameA is one master name reused across the fixtures below.
	nameA = "ns-a.cache"
)

// resp2Entry renders one `SENTINEL MASTERS` record the way go-redis hands it back
// over RESP2: a flat array of alternating key/value bulk strings. Redis and Valkey
// both build the record with addReplyDeferredLen + setDeferredMapLen, which
// degrades to a 2N array on a RESP2 connection.
func resp2Entry(kv ...string) []any {
	out := make([]any, 0, len(kv))
	for _, s := range kv {
		out = append(out, s)
	}
	return out
}

// resp3Entry renders the same record as go-redis hands it back over RESP3 — a map,
// because setDeferredMapLen emits a true map type there. go-redis negotiates RESP3
// by default and HELLO carries the SENTINEL command flag, so this is the shape the
// operator actually sees in production; the RESP2 shape is kept because the parser
// must not depend on which protocol was negotiated.
func resp3Entry(kv ...string) map[any]any {
	m := make(map[any]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// TestParseMonitoredMasters is the WP1 parse table.
//
// The field set is taken from addReplySentinelRedisInstance (redis/redis 8.0
// src/sentinel.c:3380, valkey-io/valkey 8.1 src/sentinel.c:3230) rather than
// invented: `name`, `ip`, `port`, `runid`, `flags`, … and — only while
// SRI_FAILOVER_IN_PROGRESS is set — `failover-state`. There is no
// `failover-status` field in either project at any version examined; see the
// GetMonitoredMasters doc comment.
func TestParseMonitoredMasters(t *testing.T) {
	cases := []struct {
		name  string
		reply []any
		want  []MonitoredMaster
	}{
		{
			name: "two masters, RESP3 maps (the production shape)",
			reply: []any{
				resp3Entry(
					"name", nameA, "ip", masterIP, "port", "6379",
					"runid", "abc", "flags", roleMaster, "num-slaves", "2",
				),
				resp3Entry(
					"name", "mymaster", "ip", ipOurDeadPod, "port", "6379",
					"flags", "s_down,master",
				),
			},
			want: []MonitoredMaster{
				{Name: nameA, IP: masterIP, Flags: roleMaster},
				{Name: "mymaster", IP: ipOurDeadPod, Flags: "s_down,master"},
			},
		},
		{
			name: "two masters, RESP2 flat arrays",
			reply: []any{
				resp2Entry("name", nameA, "ip", masterIP, "flags", roleMaster),
				resp2Entry("name", "mymaster", "ip", ipOurDeadPod, "flags", roleMaster),
			},
			want: []MonitoredMaster{
				{Name: nameA, IP: masterIP, Flags: roleMaster},
				{Name: "mymaster", IP: ipOurDeadPod, Flags: roleMaster},
			},
		},
		{
			name: "a failover in flight carries failover-state",
			reply: []any{
				resp3Entry(
					"name", nameA, "ip", masterIP,
					"flags", "master,failover_in_progress",
					"failover-state", stateSelectSlave,
				),
			},
			want: []MonitoredMaster{
				{
					Name: nameA, IP: masterIP,
					Flags: "master,failover_in_progress", FailoverState: stateSelectSlave,
				},
			},
		},
		{
			name: "missing fields degrade to empty strings, entry is kept",
			reply: []any{
				resp3Entry("name", nameA),
			},
			want: []MonitoredMaster{{Name: nameA}},
		},
		{
			name:  "empty reply (a bare Sentinel monitors nothing)",
			reply: []any{},
			want:  nil,
		},
		{
			name:  "nil reply",
			reply: nil,
			want:  nil,
		},
		{
			name: "a malformed entry is skipped, its neighbours survive",
			reply: []any{
				resp3Entry("name", nameA, "ip", masterIP),
				"not-a-record",
				42,
				nil,
				resp2Entry("name", "odd-number-of-fields", "ip"),
				resp3Entry("ip", "10.0.0.7"), // no name at all
				resp3Entry("name", "ns-b.cache", "ip", "10.0.0.2"),
			},
			want: []MonitoredMaster{
				{Name: nameA, IP: masterIP},
				{Name: "odd-number-of-fields"},
				{Name: "ns-b.cache", IP: "10.0.0.2"},
			},
		},
		{
			name: "non-string keys and values are ignored, not fatal",
			reply: []any{
				map[any]any{
					"name": nameA,
					7:      "seven",
					"ip":   int64(5),
				},
			},
			want: []MonitoredMaster{{Name: nameA}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseMonitoredMasters(tc.reply)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseMonitoredMasters() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// TestMonitoredMasterFailoverInProgress pins the safe reading of a field that is
// ABSENT unless a failover is running. Only an explicitly-idle value counts as
// idle; anything unrecognised is treated as in-flight (design §9 G3).
func TestMonitoredMasterFailoverInProgress(t *testing.T) {
	cases := []struct {
		name string
		m    MonitoredMaster
		want bool
	}{
		{"steady state: no failover-state field at all", MonitoredMaster{Flags: roleMaster}, false},
		{"explicit none", MonitoredMaster{Flags: roleMaster, FailoverState: failoverStateNone}, false},
		{"wait_start", MonitoredMaster{FailoverState: "wait_start"}, true},
		{"select_slave", MonitoredMaster{FailoverState: stateSelectSlave}, true},
		{"send_slaveof_noone", MonitoredMaster{FailoverState: "send_slaveof_noone"}, true},
		{"wait_promotion", MonitoredMaster{FailoverState: "wait_promotion"}, true},
		{"reconf_slaves", MonitoredMaster{FailoverState: "reconf_slaves"}, true},
		{"update_config", MonitoredMaster{FailoverState: "update_config"}, true},
		{"a value neither project emits today", MonitoredMaster{FailoverState: "brand-new"}, true},
		{"the flag alone, with no state field", MonitoredMaster{Flags: "master,failover_in_progress"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.m.FailoverInProgress(); got != tc.want {
				t.Fatalf("FailoverInProgress() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestGetMonitoredMastersIsProbeTimeoutBounded is the LR-040/LR-046 guard for the
// new read. A later milestone runs this call during churn — deliberately, before
// Rule A — so a blackholing stale Sentinel IP must cost one ProbeTimeout, not one
// DefaultTimeout times go-redis's retries.
//
// The budget deliberately discriminates 3s from DefaultTimeout's 5s: LR-040 showed
// that a context deadline ALONE is inert here (go-redis reports the deadline and
// then unwinds for roughly another DefaultTimeout), so a budget that cannot tell
// the two apart proves nothing.
func TestGetMonitoredMastersIsProbeTimeoutBounded(t *testing.T) {
	addr := blackholeListener(t)
	const budget = ProbeTimeout + time.Second

	c := NewSentinelClient([]string{addr}, "", false)

	start := time.Now()
	_, _ = c.GetMonitoredMasters(context.Background(), addr)
	elapsed := time.Since(start)

	if elapsed > budget {
		t.Fatalf("SENTINEL MASTERS against a blackholing address took %v, want <= %v: "+
			"the call is not bounded by ProbeTimeout (LR-040/LR-046)", elapsed, budget)
	}
}

// TestIsMonitoringIsProbeTimeoutBounded is the guard for a LATENT half-bound.
//
// IsMonitoring builds its client with newBoundedClient, so each socket operation
// is capped at ProbeTimeout — but it carried no per-call context deadline, unlike
// Remove, GetMonitoredMasters and every other LR-040/LR-046 site. Both halves are
// required and neither is sufficient: the client timeouts bound each individual
// attempt, the context bounds go-redis's dial-retry loop around them. That is the
// inertness LR-040 measured at 5.02s -> 5.00s.
//
// There is no unprotected caller today — Rule N wraps the call in its own
// ProbeTimeout context — so this is a trap for the next caller rather than a live
// stall: the method name and the newBoundedClient line both read as "bounded".
// Bounding it inside the method puts the guarantee on the primitive instead of on
// each caller remembering.
//
// GREEN FROM BIRTH, and disclosed as such rather than dressed up. This harness
// blackholes the READ, and on that path the client-timeout half ALONE already
// delivers the bound, so it cannot isolate the missing ctx. Measured on this
// listener, all three shapes of the same command:
//
//	unbounded client, no ctx    5.019s   (LR-040's original red)
//	unbounded client, ctx only  5.001s   (LR-040's "a ctx alone is inert")
//	bounded client, no ctx      3.018s   (IsMonitoring before the fix -> green)
//
// The ctx's job is the DIAL-retry loop — five dials against an address that
// swallows SYNs — which needs a packet filter this suite does not have; LR-040 and
// LR-046 both recorded the same limitation for the same reason. So what this test
// guards is that the bound EXISTS at all, not which half provides it. Its teeth were
// shown by a mutation: replacing the bounded client with a DefaultTimeout one fails
// it at 5.026619249s against the 4s budget, and the budget discriminates 3s from
// DefaultTimeout's 5s on purpose.
func TestIsMonitoringIsProbeTimeoutBounded(t *testing.T) {
	addr := blackholeListener(t)
	const budget = ProbeTimeout + time.Second

	c := NewSentinelClient([]string{addr}, "", false)

	start := time.Now()
	_, _ = c.IsMonitoring(context.Background(), addr, "ns.inst")
	elapsed := time.Since(start)

	if elapsed > budget {
		t.Fatalf("SENTINEL GET-MASTER-ADDR-BY-NAME against a blackholing address took %v, "+
			"want <= %v: IsMonitoring is not bounded by ProbeTimeout (LR-040/LR-046)", elapsed, budget)
	}
}
