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
	"testing"
	"time"
)

// TestClusterControlPathsAreProbeTimeoutBounded is the repeatable (tier-2) guard for
// LR-046, and the cluster-mode twin of TestSentinelWritePathsAreProbeTimeoutBounded.
//
// LR-012 bounded the cluster *gather* probes with a context deadline only, and LR-040
// then established that a context deadline alone is inert against go-redis (it reports
// the deadline and keeps unwinding for roughly another DefaultTimeout). LR-040 also
// recorded an explicit exemption for `(*ClusterClient).getClient` on the grounds that
// slot migration issues MIGRATE with its own multi-second budget. That premise is true
// for MIGRATE and false for every single-round-trip control command on the same client,
// which is what let one blackholing dead pod IP burn ~25s per call (5 dial attempts x
// DefaultTimeout) inside CLUSTER FORGET and starve the reconcile loop for ~100s during
// a rolling update.
//
// Each bounded method must therefore return within roughly one ProbeTimeout, not
// DefaultTimeout (nor DefaultTimeout x go-redis retries). Against the unbounded
// implementation each subtest takes ~5s and this fails.
func TestClusterControlPathsAreProbeTimeoutBounded(t *testing.T) {
	addr := blackholeListener(t)

	// One address, so the bound is a single ProbeTimeout (3s). Unbounded, the same
	// call costs one full DefaultTimeout (5s) — the budget has to discriminate
	// between the two, so 4s leaves a second of slack either side.
	//
	// The listener blackholes the READ rather than the dial, which is the variant
	// that reproduces locally and deterministically. Production's stall was a dial
	// blackhole costing DialTimeout x 5 retries; the same two halves (ctx deadline +
	// client timeouts) bound both, so this asserts the property that matters — that
	// the bound is actually applied.
	const budget = ProbeTimeout + time.Second

	cases := []struct {
		name string
		call func(context.Context, *ClusterClient) error
	}{
		{"CLUSTER MYID", func(ctx context.Context, c *ClusterClient) error {
			_, err := c.GetMyID(ctx, addr)
			return err
		}},
		{"CLUSTER NODES", func(ctx context.Context, c *ClusterClient) error {
			_, err := c.GetClusterNodes(ctx, addr)
			return err
		}},
		{"CLUSTER INFO", func(ctx context.Context, c *ClusterClient) error {
			_, err := c.GetClusterInfo(ctx, addr)
			return err
		}},
		{"CLUSTER MEET", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterMeet(ctx, addr, "10.0.0.1", 6379)
		}},
		{"CLUSTER FORGET", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterForget(ctx, addr, "deadbeef")
		}},
		{"CLUSTER ADDSLOTS", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterAddSlots(ctx, addr, 0, 1, 2)
		}},
		{"CLUSTER REPLICATE", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterReplicate(ctx, addr, "deadbeef")
		}},
		{"CLUSTER RESET SOFT", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterResetSoft(ctx, addr)
		}},
		{"CLUSTER FAILOVER", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterFailover(ctx, addr)
		}},
		{"CLUSTER FAILOVER TAKEOVER", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterFailoverTakeover(ctx, addr)
		}},
		{"CLUSTER SETSLOT IMPORTING", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterSetSlotImporting(ctx, addr, 42, "deadbeef")
		}},
		{"CLUSTER SETSLOT MIGRATING", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterSetSlotMigrating(ctx, addr, 42, "deadbeef")
		}},
		{"CLUSTER SETSLOT NODE", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterSetSlotNode(ctx, addr, 42, "deadbeef")
		}},
		{"CLUSTER SETSLOT STABLE", func(ctx context.Context, c *ClusterClient) error {
			return c.ClusterSetSlotStable(ctx, addr, 42)
		}},
		{"CLUSTER COUNTKEYSINSLOT", func(ctx context.Context, c *ClusterClient) error {
			_, err := c.ClusterCountKeysInSlot(ctx, addr, 42)
			return err
		}},
		{"CLUSTER GETKEYSINSLOT", func(ctx context.Context, c *ClusterClient) error {
			_, err := c.ClusterGetKeysInSlot(ctx, addr, 42, 10)
			return err
		}},
		{"CLUSTER MIGRATION IMPORT", func(ctx context.Context, c *ClusterClient) error {
			_, err := c.ClusterMigrationImport(ctx, addr, [][2]int{{0, 10}})
			return err
		}},
		{"CLUSTER MIGRATION STATUS ALL", func(ctx context.Context, c *ClusterClient) error {
			_, err := c.ClusterMigrationStatusAll(ctx, addr)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewClusterClient("", false)

			start := time.Now()
			_ = tc.call(context.Background(), c)
			elapsed := time.Since(start)

			if elapsed > budget {
				t.Fatalf("%s against a blackholing address took %v, want <= %v: "+
					"the cluster control path is not bounded by ProbeTimeout (LR-046)",
					tc.name, elapsed, budget)
			}
		})
	}
}

// TestClusterBulkPathsKeepTheLongBudget pins the deliberate exemption from BOTH sides,
// so neither half of it can drift.
//
// MIGRATE carries its own multi-second transfer budget (spec.cluster.reshardMigrateTimeoutMillis,
// default 5000ms) and the pipelined SETSLOT / COUNTKEYSINSLOT calls issue up to one command
// per slot of a shard range (5461 at shards=3) in a single round trip. Their *per-attempt*
// budget therefore stays DefaultTimeout: bounding it at ProbeTimeout would abort an
// in-flight reshard, which is exactly the hazard LR-040's exemption was written to avoid.
//
// But the retry loop around those attempts must still be bounded, which is the second half
// and the finding this test produced: with no context deadline at all, a pipelined SETSLOT
// against a blackholing address took **20.15s** (four attempts x DefaultTimeout), because
// go-redis breaks out of its retry loop only on ctx.Done(). So each row asserts a floor
// (not squeezed into the probe budget) *and* a ceiling (one attempt, not retries of it).
func TestClusterBulkPathsKeepTheLongBudget(t *testing.T) {
	addr := blackholeListener(t)

	// MIGRATE's ceiling is one attempt plus the transfer budget the caller asked for;
	// the pipelines' is one attempt. A second of slack either side.
	const migrateTimeoutMS = 5000

	cases := []struct {
		name string
		max  time.Duration
		call func(context.Context, *ClusterClient) error
	}{
		{"MIGRATE", DefaultTimeout + migrateTimeoutMS*time.Millisecond + time.Second,
			func(ctx context.Context, c *ClusterClient) error {
				return c.MigrateKeys(ctx, addr, "10.0.0.1", 6379, migrateTimeoutMS, "k1")
			}},
		{"SETSLOT NODE (pipelined)", DefaultTimeout + time.Second,
			func(ctx context.Context, c *ClusterClient) error {
				return c.ClusterSetSlotsNode(ctx, addr, []int{0, 1, 2}, "deadbeef")
			}},
		{"COUNTKEYSINSLOT (pipelined)", DefaultTimeout + time.Second,
			func(ctx context.Context, c *ClusterClient) error {
				_, err := c.ClusterCountKeysInSlots(ctx, addr, []int{0, 1, 2})
				return err
			}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewClusterClient("", false)

			start := time.Now()
			_ = tc.call(context.Background(), c)
			elapsed := time.Since(start)

			if elapsed < ProbeTimeout+time.Second {
				t.Fatalf("%s against a blackholing address returned after %v, i.e. within "+
					"the ProbeTimeout budget: it has been bounded, which cuts off a "+
					"legitimate long command (LR-046 exemption)", tc.name, elapsed)
			}
			if elapsed > tc.max {
				t.Fatalf("%s against a blackholing address took %v, want <= %v: the long "+
					"budget must be one attempt, not go-redis retries of it (LR-046)",
					tc.name, elapsed, tc.max)
			}
		})
	}
}
