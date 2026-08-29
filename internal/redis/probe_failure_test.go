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
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// serverError implements go-redis's Error interface, so errors.As finds it exactly
// as it finds a real proto.RedisError off the wire. proto.RedisError itself lives in
// an internal package and cannot be constructed here; the end-to-end test below
// pins that a REAL reply is classified the same way, so this stand-in is a
// convenience for the table, not the only evidence.
type serverError string

func (e serverError) Error() string { return string(e) }
func (e serverError) RedisError()   {}

// TestClassifyProbeError is the LR-051 classifier table.
//
// The load-bearing rows are the four AuthFailed ones: a credential mismatch is the
// single classification that changes a DECISION (it vetoes every action that
// discards data). Everything else is diagnostic, and is here so the enum stays
// closed and so an auth failure can never be silently absorbed into a neighbouring
// bucket.
//
// The enablement asymmetry is deliberately NOT tested as a failure: go-redis sends
// the three-argument `HELLO … AUTH default <pw>`, which hits ACL's nopass
// short-circuit and SUCCEEDS against a password-less server, so enabling auth
// produces no mismatch at all. Only rotation (WRONGPASS) and disable (NOAUTH) do.
func TestClassifyProbeError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ProbeFailure
	}{
		{"no error is not a failure", nil, ProbeOK},

		// --- AuthFailed: the four real server replies -----------------------
		{
			name: "NOAUTH — the disable direction: we hold no password, the pod enforces one",
			err:  fmt.Errorf("failed to get info: %w", serverError("NOAUTH Authentication required.")),
			want: ProbeAuthFailed,
		},
		{
			name: "WRONGPASS — the rotation direction: we hold the new password, the pod has the old",
			err: fmt.Errorf("failed to get info: %w",
				serverError("WRONGPASS invalid username-password pair or user is disabled.")),
			want: ProbeAuthFailed,
		},
		{
			name: "two-argument AUTH against a nopass server",
			err:  serverError("ERR Client sent AUTH, but no password is set"),
			want: ProbeAuthFailed,
		},
		{
			name: "ERR invalid password",
			err:  serverError("ERR invalid password"),
			want: ProbeAuthFailed,
		},
		{
			name: "an auth failure surfaced as a plain (unwrapped, non-RedisError) string still classifies",
			// lrctl's exec gatherer returns redis-cli's stdout as a plain error, and
			// go-redis reports a failed HELLO handshake through the dial path rather
			// than as a command reply. Neither is a proto.RedisError, and both must
			// still read as AuthFailed or the veto is bypassed on those paths.
			err:  errors.New("failed to get info: NOAUTH Authentication required."),
			want: ProbeAuthFailed,
		},

		// --- Timeout: the LR-017 blackhole shape ---------------------------
		{"context deadline exceeded", context.DeadlineExceeded, ProbeTimedOut},
		{"wrapped context deadline", fmt.Errorf("probe: %w", context.DeadlineExceeded), ProbeTimedOut},
		{"os deadline exceeded", os.ErrDeadlineExceeded, ProbeTimedOut},
		{
			name: "go-redis dial-retry exhaustion on a blackholing IP",
			err: errors.New("redis: connection pool: failed to dial after 5 attempts: " +
				"dial tcp 10.233.192.209:26379: i/o timeout"),
			want: ProbeTimedOut,
		},
		{
			name: "a net.Error reporting a timeout",
			err:  &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded},
			want: ProbeTimedOut,
		},

		// --- Unroutable: the local-kubeadm fast-RST shape -------------------
		{
			name: "connection refused (a dead pod whose IP still routes)",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
			want: ProbeUnroutable,
		},
		{
			name: "no route to host",
			err:  fmt.Errorf("dial tcp 10.0.0.1:6379: %w", syscall.EHOSTUNREACH),
			want: ProbeUnroutable,
		},
		{
			name: "network unreachable",
			err:  fmt.Errorf("dial tcp: %w", syscall.ENETUNREACH),
			want: ProbeUnroutable,
		},
		{
			name: "connection refused as a plain string",
			err:  errors.New("dial tcp 10.0.0.1:6379: connect: connection refused"),
			want: ProbeUnroutable,
		},

		// --- ProtocolError: we reached it and it answered unusably ----------
		{
			name: "a non-auth server error reply",
			err:  serverError("LOADING Redis is loading the dataset in memory"),
			want: ProbeProtocolError,
		},
		{
			name: "a non-auth server error reply is NOT mistaken for an auth failure",
			err:  serverError("ERR unknown command 'INFO'"),
			want: ProbeProtocolError,
		},

		// --- Unknown: everything else, named rather than absorbed -----------
		{"an unrecognised error", errors.New("something went sideways"), ProbeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyProbeError(tt.err); got != tt.want {
				t.Errorf("ClassifyProbeError(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// errorServer accepts connections on a random port and answers EVERY command line
// with the given RESP error, so a real go-redis client produces a real error and
// the classification is exercised against the actual error surface rather than
// against a hand-built stand-in.
func errorServer(t *testing.T, reply string) string {
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
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if strings.HasPrefix(line, "*") || strings.HasPrefix(line, "$") {
						continue
					}
					if _, err := c.Write([]byte("-" + reply + "\r\n")); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// TestClassifyProbeErrorAgainstRealGoRedis pins the four AuthFailed strings against
// the errors a REAL go-redis client actually returns, so the table above cannot
// drift from the wire. The exact wrapping go-redis applies to a rejected handshake
// is a library detail; what must hold is that whatever it returns still classifies
// as AuthFailed.
func TestClassifyProbeErrorAgainstRealGoRedis(t *testing.T) {
	replies := []string{
		"NOAUTH Authentication required.",
		"WRONGPASS invalid username-password pair or user is disabled.",
		"ERR Client sent AUTH, but no password is set",
		"ERR invalid password",
	}
	for _, reply := range replies {
		t.Run(reply, func(t *testing.T) {
			addr := errorServer(t, reply)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := GetReplicationInfo(ctx, addr, "some-password", false)
			if err == nil {
				t.Fatalf("expected an error from a server that rejects every command")
			}
			if got := ClassifyProbeError(err); got != ProbeAuthFailed {
				t.Errorf("ClassifyProbeError(%v) = %q, want %q", err, got, ProbeAuthFailed)
			}
		})
	}
}
