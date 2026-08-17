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
