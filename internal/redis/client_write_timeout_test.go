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
	"net"
	"testing"
	"time"
)

// blackholeListener accepts TCP connections and then never replies, holding the
// connection open. This is the deterministic local stand-in for the failure mode
// LR-040 was found on: a deleted pod IP that swallows packets instead of sending
// an RST. A dial-refusing address cannot reproduce it — the whole point is that
// the peer looks reachable and then goes silent, which is what burns
// DefaultTimeout once per go-redis retry.
func blackholeListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Hold it open and never write a reply.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	return ln.Addr().String()
}

// TestSentinelWritePathsAreProbeTimeoutBounded is the repeatable (tier-2) guard for
// LR-040. LR-017 bounded every sentinel READ path with ProbeTimeout but deliberately
// left the write paths unbounded, reasoning that they "are gated by Rule A during
// churn and are not on the stall path". Rule 0 (bare-sentinel re-registration) runs
// BEFORE Rule A's guardrail, so that reasoning does not hold for it: a blackholing
// stale sentinel IP stalled one reconcile ~117s inside Monitor, starving the
// re-registration of the freshly-rolled sentinels.
//
// Each write must therefore return within roughly one ProbeTimeout per address,
// not DefaultTimeout x go-redis retries. Against the unbounded implementation each
// subtest takes ~20s (5s ReadTimeout x 4 attempts) and this fails.
func TestSentinelWritePathsAreProbeTimeoutBounded(t *testing.T) {
	addr := blackholeListener(t)

	// One address, so the bound is a single ProbeTimeout (3s). Unbounded, the same
	// call costs one full DefaultTimeout (5s), so the budget has to discriminate
	// between the two: 4s leaves a second of slack either side.
	//
	// The listener blackholes the READ rather than the dial, which is the variant
	// that reproduces locally and deterministically. Production's stall was a dial
	// blackhole costing DialTimeout x retries (~117s observed); the same
	// context deadline bounds both, since go-redis honours ctx during dial too, so
	// this asserts the property that matters — the deadline is actually applied.
	const budget = ProbeTimeout + time.Second

	cases := []struct {
		name string
		call func(context.Context, *SentinelClient) error
	}{
		{"Monitor", func(ctx context.Context, c *SentinelClient) error {
			return c.Monitor(ctx, "ns.inst", "10.0.0.1", 6379, 2)
		}},
		{"Set", func(ctx context.Context, c *SentinelClient) error {
			return c.Set(ctx, "ns.inst", "down-after-milliseconds", "5000")
		}},
		{"Reset", func(ctx context.Context, c *SentinelClient) error {
			return c.Reset(ctx, "ns.inst")
		}},
		{"Remove", func(ctx context.Context, c *SentinelClient) error {
			return c.Remove(ctx, "ns.inst")
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewSentinelClient([]string{addr}, "", false)

			start := time.Now()
			_ = tc.call(context.Background(), c)
			elapsed := time.Since(start)

			if elapsed > budget {
				t.Fatalf("SENTINEL %s against a blackholing address took %v, want <= %v: "+
					"the write path is not bounded by ProbeTimeout (LR-040)",
					tc.name, elapsed, budget)
			}
		})
	}
}
