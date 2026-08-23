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

package chaos

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestClassifyRead pins the distinction that DataCorruptions alone could never
// catch: a read that finds an already-ACKed key *absent* is a lost write (a
// durability failure), not a read failure (an availability one). Writes carry no
// TTL, so absence has no benign explanation.
func TestClassifyRead(t *testing.T) {
	const expected = "cafebabe"

	tests := []struct {
		name   string
		result string
		err    error
		want   readOutcome
	}{
		{
			name:   "key present with expected value",
			result: expected,
			want:   readOK,
		},
		{
			name: "key absent — an acknowledged write vanished",
			err:  redis.Nil,
			want: readLost,
		},
		{
			name: "key absent, error wrapped by a caller",
			err:  fmt.Errorf("get 42: %w", redis.Nil),
			want: readLost,
		},
		{
			name: "transport error",
			err:  errors.New("dial tcp 10.0.0.1:6379: i/o timeout"),
			want: readFailed,
		},
		{
			name: "server refuses to serve the slot",
			err:  errors.New("CLUSTERDOWN The cluster is down"),
			want: readFailed,
		},
		{
			name:   "key present with the wrong value",
			result: "deadbeef",
			want:   readCorrupt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyRead(tt.result, tt.err, expected); got != tt.want {
				t.Errorf("classifyRead() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpectedValue(t *testing.T) {
	// Test deterministic value generation
	v1 := expectedValue(42)
	v2 := expectedValue(42)
	v3 := expectedValue(43)

	if v1 != v2 {
		t.Errorf("expectedValue should be deterministic: got %s and %s for same input", v1, v2)
	}

	if v1 == v3 {
		t.Error("expectedValue should produce different values for different inputs")
	}

	// Check it's a valid hex string (64 chars for sha256)
	if len(v1) != 64 {
		t.Errorf("expectedValue should produce 64-char hex string, got %d chars", len(v1))
	}
}

func TestMetricsSnapshot(t *testing.T) {
	m := MetricsSnapshot{
		WriteAttempts:   100,
		WriteSuccesses:  90,
		WriteFailures:   10,
		ReadAttempts:    100,
		ReadSuccesses:   95,
		ReadFailures:    5,
		DataCorruptions: 0,
	}

	if m.WriteAvailability() != 0.9 {
		t.Errorf("WriteAvailability: expected 0.9, got %f", m.WriteAvailability())
	}

	if m.ReadAvailability() != 0.95 {
		t.Errorf("ReadAvailability: expected 0.95, got %f", m.ReadAvailability())
	}

	// Test zero case
	m2 := MetricsSnapshot{}
	if m2.WriteAvailability() != 1.0 {
		t.Errorf("WriteAvailability with zero attempts should be 1.0, got %f", m2.WriteAvailability())
	}
}

func TestKeyName(t *testing.T) {
	tc := &TestClient{keyPrefix: ""}
	if tc.keyName(42) != "42" {
		t.Errorf("keyName without prefix: expected '42', got '%s'", tc.keyName(42))
	}

	tc.keyPrefix = "test"
	if tc.keyName(42) != "test:42" {
		t.Errorf("keyName with prefix: expected 'test:42', got '%s'", tc.keyName(42))
	}
}

// clusterInfoOK is a minimal CLUSTER INFO reply for a node with a complete view.
const nodeA, nodeB, nodeC = "10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379"

const clusterInfoOK = "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:16384\r\n" +
	"cluster_slots_ok:16384\r\ncluster_known_nodes:3\r\ncluster_size:3\r\n"

// clusterInfoLagging is what a node that has not yet learned the other shards'
// slot assignments answers: it owns its own range and nothing else. This is the
// shape observed in the field (chaos-cluster-stable, 2026-08-23), where two of
// three masters logged "Cluster state changed: ok" and the third never did.
const clusterInfoLagging = "cluster_enabled:1\r\ncluster_state:fail\r\ncluster_slots_assigned:5461\r\n" +
	"cluster_slots_ok:5461\r\ncluster_known_nodes:3\r\ncluster_size:1\r\n"

// TestClusterReadinessIsAnAndOverNodes pins the gate's decisive property: one
// node still reporting an incomplete view holds the gate closed, however many
// other nodes say ok. The operator's own status is an OR over nodes
// (ClusterGroundTruth.ClusterState — "ok if ANY node says ok") and a MAX over
// cluster_slots_assigned, so a client that copies that shape can be told the
// cluster is whole while the node it is about to be routed to answers
// -CLUSTERDOWN. Teeth shown by mutating clusterReadiness to return nil as soon
// as one node reports ok: the "one lagging master among two healthy ones" row
// then fails.
func TestClusterReadinessIsAnAndOverNodes(t *testing.T) {
	tests := []struct {
		name    string
		infos   map[string]string
		wantErr string
	}{
		{
			name:  "every master whole",
			infos: map[string]string{nodeA: clusterInfoOK, nodeB: clusterInfoOK},
		},
		{
			name: "one lagging master among two healthy ones",
			infos: map[string]string{
				nodeA: clusterInfoOK,
				nodeB: clusterInfoOK,
				nodeC: clusterInfoLagging,
			},
			wantErr: nodeC + " reports cluster_state:fail",
		},
		{
			name: "state ok but the slot view is still incomplete",
			infos: map[string]string{
				nodeA: "cluster_state:ok\r\ncluster_slots_assigned:10923\r\ncluster_slots_ok:10923\r\n",
			},
			wantErr: nodeA + " reports cluster_slots_ok:10923",
		},
		{
			name:    "no master answered",
			infos:   map[string]string{},
			wantErr: "no reachable master answered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := clusterReadiness(tt.infos)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("clusterReadiness() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("clusterReadiness() = nil, want an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("clusterReadiness() = %q, want it to contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestClusterInfoFieldNamesTheOffender guards the diagnosability half: the old
// gate could only say "cluster not ready (state not ok)", which is what 25
// consecutive identical log lines said while nothing identified the node.
func TestClusterInfoFieldNamesTheOffender(t *testing.T) {
	if got := clusterInfoField(clusterInfoLagging, "cluster_state"); got != "cluster_state:fail" {
		t.Errorf("clusterInfoField(cluster_state) = %q", got)
	}
	if got := clusterInfoField(clusterInfoLagging, "cluster_slots_ok"); got != "cluster_slots_ok:5461" {
		t.Errorf("clusterInfoField(cluster_slots_ok) = %q", got)
	}
	if got := clusterInfoField("cluster_state:ok\r\n", "cluster_size"); got != "cluster_size:<absent>" {
		t.Errorf("clusterInfoField(missing) = %q", got)
	}
}
