//go:build e2e

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

package e2e

import (
	. "github.com/onsi/ginkgo/v2"
)

// Every spec in this suite carries exactly one deployment-mode label, so a run can
// be cut down to one mode: `make test-e2e MODE=sentinel` (and `make list-e2e
// MODE=sentinel` to preview the selection without touching a cluster). A full run
// is too long to be the only option when the work at hand is mode-specific.
//
// The label goes on the OUTERMOST mode-pure container. Most files are mode-pure at
// the Describe; the mixed ones (littlered_test.go, kill9_chaos_test.go,
// sentinel_standalone_chaos_test.go, pdb_test.go, security_test.go) are labelled at
// the inner Context or, where a table drives one spec per mode, on the It itself via
// modeLabel.
//
// The invariant that makes the cut trustworthy: the four mode label sets must
// PARTITION the suite — every spec labelled, none labelled twice. An unlabelled
// spec is invisible to every MODE run, which is a silently smaller test run and
// therefore worse than no knob at all. `hack/verify-e2e-mode-labels.sh` checks the
// partition holds, by comparing the per-mode selections against the full one.
//
// modeLabel maps a spec.mode value to its label. Only `failover` is not its own
// name: the failover-mode tier already shipped with `failover-mode` and that label
// is referenced in the Makefile docs and in muscle memory, so it is reused rather
// than renamed. The Makefile hides the asymmetry behind MODE=failover.
func modeLabel(mode string) Labels {
	if mode == "failover" {
		return Label("failover-mode")
	}
	return Label(mode)
}
