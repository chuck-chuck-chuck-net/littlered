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

import "testing"

const testModeSentinel = "sentinel"

// SentinelMasterName resolves the effective Sentinel master name. It must NEVER
// invent a derived value: a derived default cannot be a CRD constant, so it would
// live in Go, and the reconciler's finalizer Update (littlered_controller.go) would
// persist it back into the user's spec — the LR-033 defect. The fallback is the
// fixed legacy constant, so "field absent" has exactly one documented meaning.
func TestSentinelMasterName(t *testing.T) {
	tests := []struct {
		name string
		lr   *LittleRed
		want string
	}{
		{
			name: "explicit instance-scoped name is used verbatim",
			lr:   &LittleRed{Spec: LittleRedSpec{Mode: testModeSentinel, Sentinel: &SentinelSpec{MasterName: "team-a.cache"}}},
			want: "team-a.cache",
		},
		{
			name: "explicit mymaster is honoured, not rewritten",
			lr:   &LittleRed{Spec: LittleRedSpec{Mode: testModeSentinel, Sentinel: &SentinelSpec{MasterName: "mymaster"}}},
			want: LegacySentinelMasterName,
		},
		{
			// A pre-existing instance created before the field was required.
			name: "absent field falls back to the legacy constant",
			lr:   &LittleRed{Spec: LittleRedSpec{Mode: testModeSentinel, Sentinel: &SentinelSpec{Quorum: 2}}},
			want: LegacySentinelMasterName,
		},
		{
			// spec.sentinel is optional even in sentinel mode, so the nested
			// Required marker is never evaluated for such an object.
			name: "absent sentinel block falls back to the legacy constant",
			lr:   &LittleRed{Spec: LittleRedSpec{Mode: testModeSentinel}},
			want: LegacySentinelMasterName,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.lr.SentinelMasterName(); got != tc.want {
				t.Errorf("SentinelMasterName() = %q, want %q", got, tc.want)
			}
		})
	}
}

// The accessor must not mutate the object it reads — anything it wrote into the spec
// would be persisted by the reconciler's finalizer Update (see LR-033).
func TestSentinelMasterNameDoesNotMutateSpec(t *testing.T) {
	lr := &LittleRed{Spec: LittleRedSpec{Mode: testModeSentinel, Sentinel: &SentinelSpec{Quorum: 2}}}
	_ = lr.SentinelMasterName()
	if lr.Spec.Sentinel.MasterName != "" {
		t.Errorf("accessor wrote %q back into spec.sentinel.masterName; it must stay empty",
			lr.Spec.Sentinel.MasterName)
	}
}

// LegacySentinelMasterName is the value every instance created before the field
// existed is running with. It is load-bearing for the fallback and must not drift.
func TestLegacySentinelMasterNameIsStable(t *testing.T) {
	if LegacySentinelMasterName != "mymaster" { //nolint:goconst // the literal is the assertion
		t.Errorf("LegacySentinelMasterName = %q; changing it silently renames the master of "+
			"every instance that has not set the field, breaking every Sentinel-aware client",
			LegacySentinelMasterName)
	}
}
