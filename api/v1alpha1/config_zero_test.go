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

package v1alpha1

import (
	"encoding/json"
	"testing"
)

// `tcp-keepalive 0` is a real Redis semantic — it DISABLES keepalive — and a user who
// asks for it cannot have it.
//
// The shape is the one LR-033 already hit once and LR-038 avoided for
// MinReplicasToWrite by making it a *int: a NON-POINTER field with a NON-ZERO CRD
// default and `omitempty`. An explicitly-set zero is indistinguishable from unset at
// every layer — SetDefaults overwrites it here, and the reconciler's finalizer Update
// writes the whole object back, so the API server re-applies the default too. The value
// is rendered straight into redis.conf in all four modes.
//
// The fix is the MinReplicasToWrite precedent: a pointer, where nil is unambiguously
// "unset" and 0 is a value the user chose.
func TestExplicitZeroTCPKeepaliveSurvivesDefaulting(t *testing.T) {
	// Through JSON rather than a struct literal on purpose: an explicit zero in the
	// user's YAML is exactly what has to survive, and a literal cannot express the
	// difference between "set to 0" and "not set" while the field is a plain int.
	var cfg ConfigSpec
	if err := json.Unmarshal([]byte(`{"tcpKeepalive":0}`), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	cfg.SetDefaults()

	if got := cfg.TCPKeepaliveOrDefault(); got != 0 {
		t.Errorf("tcpKeepalive = %d, want 0: an explicitly-set zero is a user's choice "+
			"(it disables keepalive in Redis), and defaulting must not overwrite it — "+
			"the LR-033 shape, fixed for MinReplicasToWrite by using a pointer", got)
	}
}

// The other half, and the reason the fix is a pointer rather than "stop defaulting":
// an OMITTED field must still get the documented default.
func TestOmittedTCPKeepaliveStillDefaults(t *testing.T) {
	var cfg ConfigSpec
	if err := json.Unmarshal([]byte(`{}`), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	cfg.SetDefaults()

	if got := cfg.TCPKeepaliveOrDefault(); got != DefaultTCPKeepalive {
		t.Errorf("tcpKeepalive = %d, want the default %d", got, DefaultTCPKeepalive)
	}
}
