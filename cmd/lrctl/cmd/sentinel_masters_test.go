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

package cmd

import (
	"reflect"
	"testing"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// ipMaster is the master address reused across the fixtures below.
const (
	ipMaster = "10.0.0.1"
	// nameDesired is the master name the instance currently wants.
	nameDesired = "ns.inst"
)

// TestParseSentinelMasters is the CLI half of the operator/CLI parity rule
// (LR-041): `lrctl` and the operator must not be able to disagree about which
// names a Sentinel monitors, so this parses the same reply the operator reads over
// the wire, out of redis-cli's flattened line output.
//
// Records are delimited by `name`, which addReplySentinelRedisInstance emits first
// for every entry (redis/redis 8.0 src/sentinel.c:3387, valkey-io/valkey 8.1
// src/sentinel.c:3237) — the same property parseSentinelReplicas already relies on.
func TestParseSentinelMasters(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want []redisclient.MonitoredMaster
	}{
		{
			name: "two masters, the leftover one flagged down",
			out: "name\nns.inst\nip\n10.0.0.1\nport\n6379\nrunid\nabc\nflags\nmaster\n" +
				"num-slaves\n2\n" +
				"name\nmymaster\nip\n10.0.0.9\nport\n6379\nflags\ns_down,master\n",
			want: []redisclient.MonitoredMaster{
				{Name: nameDesired, IP: ipMaster, Flags: "master"},
				{Name: "mymaster", IP: "10.0.0.9", Flags: "s_down,master"},
			},
		},
		{
			name: "a failover in flight carries failover-state",
			out: "name\nns.inst\nip\n10.0.0.1\nflags\nmaster,failover_in_progress\n" +
				"failover-state\nselect_slave\n",
			want: []redisclient.MonitoredMaster{{
				Name: nameDesired, IP: ipMaster,
				Flags: "master,failover_in_progress", FailoverState: "select_slave",
			}},
		},
		{
			name: "CRLF line endings",
			out:  "name\r\nns.inst\r\nip\r\n10.0.0.1\r\n",
			want: []redisclient.MonitoredMaster{{Name: nameDesired, IP: ipMaster}},
		},
		{
			name: "a bare Sentinel monitors nothing",
			out:  "\n",
			want: nil,
		},
		{
			name: "an entry with no name is not a record",
			out:  "ip\n10.0.0.1\nflags\nmaster\n",
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseSentinelMasters(tc.out, nil)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseSentinelMasters() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// TestParseSentinelMastersResolvesIdentities pins that a reported hostname is put
// through the same resolver the rest of the CLI gather uses, so an operator-side IP
// and a CLI-side hostname do not read as two different addresses.
func TestParseSentinelMastersResolvesIdentities(t *testing.T) {
	resolve := func(s string) string {
		if s == "inst-redis-0" {
			return ipMaster
		}
		return s
	}
	got := parseSentinelMasters("name\nns.inst\nip\ninst-redis-0\n", resolve)
	want := []redisclient.MonitoredMaster{{Name: nameDesired, IP: ipMaster}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseSentinelMasters() = %#v, want %#v", got, want)
	}
}
