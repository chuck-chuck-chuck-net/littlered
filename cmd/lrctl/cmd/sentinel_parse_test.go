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

import "testing"

// The parser this replaces FABRICATED flags — "found" for addresses that matched one of
// our pods, "s_down,ghost" for everything else. That made the cross-instance diagnostic
// impossible by construction: a live foreign replica arrived pre-labelled as dead
// debris, which is exactly the class the diagnostic filters out. So the property under
// test is not "it parses" but "it reports the flags Sentinel actually sent".
func TestParseSentinelReplicas(t *testing.T) {
	// Two records: one of ours, one belonging to another instance. Note the foreign one
	// is reported healthy — that is the captured state, and the reason it must not be
	// labelled s_down by the parser.
	out := "name\n10.0.0.2:6379\nip\n10.0.0.2\nport\n6379\nrunid\nabc\nflags\nslave\n" +
		"name\n10.9.9.8:6379\nip\n10.9.9.8\nport\n6379\nrunid\ndef\nflags\nslave\n"

	reps := parseSentinelReplicas(out, nil)
	if len(reps) != 2 {
		t.Fatalf("got %d replicas, want 2: %+v", len(reps), reps)
	}
	for _, r := range reps {
		if r.Flags != roleSlave {
			t.Errorf("replica %s has flags %q, want the reported %q — fabricated flags "+
				"would hide a live foreign replica", r.IP, r.Flags, roleSlave)
		}
		if r.Port != "6379" {
			t.Errorf("replica %s has port %q, want 6379", r.IP, r.Port)
		}
	}
	if reps[0].IP != "10.0.0.2" || reps[1].IP != "10.9.9.8" {
		t.Errorf("IPs = %s, %s; want 10.0.0.2, 10.9.9.8", reps[0].IP, reps[1].IP)
	}
}

func TestParseSentinelReplicasPreservesDownFlags(t *testing.T) {
	out := "name\n10.0.0.9:6379\nip\n10.0.0.9\nport\n6379\nflags\ns_down,slave\n"
	reps := parseSentinelReplicas(out, nil)
	if len(reps) != 1 || reps[0].Flags != "s_down,slave" {
		t.Fatalf("got %+v, want one replica flagged s_down,slave — the diagnostic relies "+
			"on this to tell debris from a foreign deployment", reps)
	}
}

func TestParseSentinelReplicasResolvesIdentities(t *testing.T) {
	out := "name\nhost-a:6379\nip\nhost-a\nport\n6379\nflags\nslave\n"
	reps := parseSentinelReplicas(out, func(string) string { return "10.0.0.5" })
	if len(reps) != 1 || reps[0].IP != "10.0.0.5" {
		t.Fatalf("got %+v, want the resolver applied to the reported identity", reps)
	}
}

func TestParseSentinelReplicasHandlesEmpty(t *testing.T) {
	if reps := parseSentinelReplicas("", nil); len(reps) != 0 {
		t.Fatalf("got %+v, want none", reps)
	}
}
