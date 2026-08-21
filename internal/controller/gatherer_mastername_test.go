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
)

// fakeSentinel accepts one or more connections and answers EVERY command with
// `-ERR No such master with that name`, which is exactly what a real Sentinel
// replies to `SENTINEL master <unknown>` — including the empty name that an
// unwired gatherer sends. The point is to make the dial SUCCEED, so the code path
// under test is the reply handling rather than a connection error.
// It must bind the real Sentinel port: GetSentinelState builds the address from
// littleredv1alpha1.SentinelPort, so a random port would never be reached.
func fakeSentinel(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", littleredv1alpha1.SentinelPort))
	if err != nil {
		t.Skipf("cannot bind 127.0.0.1:%d (in use?): %v", littleredv1alpha1.SentinelPort, err)
	}
	t.Cleanup(func() { _ = ln.Close() })

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
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					// Consume RESP array/bulk framing; reply once per command line.
					if strings.HasPrefix(line, "*") || strings.HasPrefix(line, "$") {
						continue
					}
					if _, err := c.Write([]byte("-ERR No such master with that name\r\n")); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// TestSentinelGatherRequiresMasterName is the guard for LR-041.
//
// LR-039 made the Sentinel master name per-instance and added operatorGatherer.masterName
// for it, with the comment "Sentinel-mode paths must set it". The sentinel-mode
// reconcile did not, so every probe issued `SENTINEL master ""`, Sentinel answered
// `ERR No such master with that name`, and GetSentinelState took its
// not-monitoring branch: Monitoring=false, Reachable=true, for ALL sentinels, forever.
//
// That is silently catastrophic rather than loud. Everything gated on sn.Monitoring
// goes dead — ghost-master correction (LR-005/LR-008), ghost-replica pruning (Rule D),
// HasHealthyKnownReplica (LR-024's discriminator) — while Rule 0 sees a permanently
// bare quorum and re-registers all three sentinels every 2s forever.
//
// An empty master name is a programming error, not a cluster state, so the gather must
// refuse it instead of reporting a plausible-looking bare sentinel. Against the
// pre-fix code this returns (state{Monitoring:false}, nil) and the test fails.
func TestSentinelGatherRequiresMasterName(t *testing.T) {
	fakeSentinel(t)
	const host = "127.0.0.1"

	t.Run("empty master name is refused", func(t *testing.T) {
		g := &operatorGatherer{} // masterName deliberately unset, as the bug had it
		st, err := g.GetSentinelState(context.Background(), "sentinel-0", host)
		if err == nil {
			t.Fatalf("GetSentinelState with an empty masterName returned no error "+
				"(state=%+v); an unwired gatherer must fail loudly, not report a bare "+
				"sentinel and silently disable every Monitoring-gated rule (LR-041)", st)
		}
		if !strings.Contains(err.Error(), "master name") {
			t.Fatalf("error should name the missing master name, got: %v", err)
		}
	})

	t.Run("a real not-monitoring sentinel is still reported, not an error", func(t *testing.T) {
		// The refusal must be specific to the empty name. A sentinel that genuinely
		// does not know THIS instance's master is ordinary runtime state (a freshly
		// restarted pod) and Rule 0 depends on seeing it as reachable-but-bare.
		g := &operatorGatherer{masterName: "ns.inst"}
		st, err := g.GetSentinelState(context.Background(), "sentinel-0", host)
		if err != nil {
			t.Fatalf("unexpected error for a known-name miss: %v", err)
		}
		if st == nil || st.Monitoring || !st.Reachable {
			t.Fatalf("want reachable-but-not-monitoring, got %+v", st)
		}
	})
}
