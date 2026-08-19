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

// LegacySentinelMasterName is the Sentinel master name every instance created before
// spec.sentinel.masterName existed is running with, and therefore the fallback when
// the field is absent.
//
// It is a fixed constant on purpose. A *derived* fallback (e.g. "<namespace>.<name>")
// could not be expressed as a CRD default, so it would live in Go — and the
// reconciler adds its finalizer with a plain Update on the whole object, which would
// persist the computed value into the user's spec. That is the LR-033 defect, and here
// it would be worse: the effective value is a client-visible contract string, so a
// later change to the derivation would silently rename the master of every instance
// that never set the field, breaking every Sentinel-aware client on an operator
// upgrade with no user action to correlate the outage to.
const LegacySentinelMasterName = "mymaster"

// SentinelMasterName returns the effective Sentinel master name for this instance.
//
// The master name is the only isolation boundary Sentinel's gossip protocol has, so
// this value decides which other Sentinel deployments this instance can be absorbed
// by. See the SentinelMasterName field documentation and
// docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md.
//
// The field is Required, so a newly created instance always carries one. Objects that
// predate the field, and objects that omit spec.sentinel entirely (legal even in
// sentinel mode, in which case the nested Required marker is never evaluated), fall
// back to the legacy constant — which is what they are already running with.
//
// This accessor is pure: it never writes the resolved value back into the spec.
func (r *LittleRed) SentinelMasterName() string {
	if r.Spec.Sentinel != nil && r.Spec.Sentinel.MasterName != "" {
		return r.Spec.Sentinel.MasterName
	}
	return LegacySentinelMasterName
}

// SentinelMasterNameUnscoped reports whether this instance is falling back to the
// shared legacy master name because the field is unset — the state in which it can be
// captured by, or capture, any other Sentinel deployment on the same pod network that
// is also unscoped.
//
// Setting the field to "mymaster" explicitly is deliberately NOT flagged: that is a
// choice (a legacy client may hardcode the value), and the operator does not
// second-guess it. Only the absence of a decision is reported.
func (r *LittleRed) SentinelMasterNameUnscoped() bool {
	return r.Spec.Sentinel == nil || r.Spec.Sentinel.MasterName == ""
}
