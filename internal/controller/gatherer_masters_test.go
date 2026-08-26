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
	"io"
	"net"
	"strconv"
	"strings"
	"testing"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// twoNameSentinel is a scripted fake Sentinel that monitors TWO master names —
// the state a half-finished master-name change leaves behind, and the whole reason
// the gather needs the full list.
//
// `SENTINEL master <known>` answers normally, `SENTINEL master <unknown>` answers
// `-ERR No such master with that name`, and `SENTINEL masters` always lists both
// names regardless of which one was asked about. That asymmetry is the point: the
// single-name probe cannot see the second name in either direction.
//
// It speaks RESP2 by replying to HELLO with a Redis error, which go-redis treats as
// "server does not support HELLO" and falls back (redis.go:793-820). Real Sentinels
// do support HELLO and answer RESP3 maps; the parser handles both shapes and has a
// unit table for each, so the protocol chosen here is a test-rig convenience.
//
// It must bind the real Sentinel port: GetSentinelState builds the address from
// littleredv1alpha1.SentinelPort, so a random port would never be reached.
func twoNameSentinel(t *testing.T, knownName, otherName string) {
	t.Helper()
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", littleredv1alpha1.SentinelPort))
	if err != nil {
		t.Skipf("cannot bind 127.0.0.1:%d (in use?): %v", littleredv1alpha1.SentinelPort, err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	masterRecord := func(name, ip, flags string) string {
		fields := []string{"name", name, "ip", ip, "port", "6379", "flags", flags, "num-slaves", "2"}
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
						if args[2] == knownName {
							reply = masterRecord(knownName, "10.0.0.1", "master")
						} else {
							reply = "-ERR No such master with that name\r\n"
						}
					case len(args) >= 3 && strings.EqualFold(args[0], "sentinel") &&
						strings.EqualFold(args[1], "replicas"):
						reply = "*0\r\n"
					case len(args) >= 2 && strings.EqualFold(args[0], "sentinel") &&
						strings.EqualFold(args[1], "masters"):
						reply = "*2\r\n" +
							masterRecord(knownName, "10.0.0.1", "master") +
							masterRecord(otherName, "10.0.0.9", "s_down,master")
					}
					if _, err := c.Write([]byte(reply)); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
}

// readRESPCommand reads one inline-free RESP array of bulk strings.
func readRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, io.ErrUnexpectedEOF
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 {
		return nil, io.ErrUnexpectedEOF
	}
	args := make([]string, 0, n)
	for range n {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if !strings.HasPrefix(hdr, "$") {
			return nil, io.ErrUnexpectedEOF
		}
		l, err := strconv.Atoi(hdr[1:])
		if err != nil || l < 0 {
			return nil, io.ErrUnexpectedEOF
		}
		buf := make([]byte, l+2) // payload + CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:l]))
	}
	return args, nil
}

// TestSentinelGatherCarriesEveryMonitoredName is the WP2 guard.
//
// A Sentinel carrying BOTH the old and the new master name reports
// `Monitoring: true` for whichever one it is asked about, so the single-name probe
// this gather has always used cannot see the other one — in either direction. That
// is the field state a half-finished rename leaves behind, and it is what
// MonitoredMasters exists to expose.
func TestSentinelGatherCarriesEveryMonitoredName(t *testing.T) {
	const (
		host    = "127.0.0.1"
		desired = "ns.inst"
		stale   = "mymaster"
	)
	twoNameSentinel(t, desired, stale)

	names := func(t *testing.T, masterName string) []string {
		t.Helper()
		g := &operatorGatherer{}
		st, err := g.GetSentinelState(context.Background(), "sentinel-0", host, masterName)
		if err != nil {
			t.Fatalf("GetSentinelState(%q): %v", masterName, err)
		}
		if !st.Reachable {
			t.Fatalf("GetSentinelState(%q): sentinel reported unreachable: %+v", masterName, st)
		}
		out := make([]string, 0, len(st.MonitoredMasters))
		for _, m := range st.MonitoredMasters {
			out = append(out, m.Name)
		}
		return out
	}

	t.Run("asked about the name it monitors, it still reports the other one", func(t *testing.T) {
		got := names(t, desired)
		if len(got) != 2 || got[0] != desired || got[1] != stale {
			t.Fatalf("MonitoredMasters = %v, want [%s %s]: a Sentinel carrying two names must be "+
				"gathered with both, or a leftover name is invisible to every rule", got, desired, stale)
		}
	})

	t.Run("asked about a name it does NOT monitor, it still reports what it has", func(t *testing.T) {
		// This is the rename's first pass: the desired name is unknown, so the
		// single-name probe reads bare — and the stale name it is still carrying is
		// exactly what must not be lost with it.
		g := &operatorGatherer{}
		st, err := g.GetSentinelState(context.Background(), "sentinel-0", host, "nobody.knows.me")
		if err != nil {
			t.Fatalf("GetSentinelState: %v", err)
		}
		if st.Monitoring {
			t.Fatalf("want the unknown name to read as not-monitoring, got %+v", st)
		}
		if len(st.MonitoredMasters) != 2 {
			t.Fatalf("MonitoredMasters = %+v, want both names: the not-monitoring branch must "+
				"carry the list too, or the rename's first pass is blind", st.MonitoredMasters)
		}
	})

	t.Run("the record's fields survive the round trip", func(t *testing.T) {
		g := &operatorGatherer{}
		st, err := g.GetSentinelState(context.Background(), "sentinel-0", host, desired)
		if err != nil {
			t.Fatalf("GetSentinelState: %v", err)
		}
		if len(st.MonitoredMasters) != 2 {
			t.Fatalf("MonitoredMasters = %+v, want 2 entries", st.MonitoredMasters)
		}
		if got := st.MonitoredMasters[1]; got.IP != "10.0.0.9" || got.Flags != "s_down,master" {
			t.Fatalf("stale entry = %+v, want ip 10.0.0.9 flags s_down,master: the address and "+
				"flags are what distinguish our own debris from someone else's live master", got)
		}
	})
}
