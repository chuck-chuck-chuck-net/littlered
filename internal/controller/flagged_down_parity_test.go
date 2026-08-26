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
	"testing"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// TestFlaggedDownParity pins the operator's down-discriminator against the CLI's.
//
// The same one-line predicate exists twice — flaggedDown here (planForsaken clause 3
// and Rule N's gate G5) and its twin inside internal/redis (DetectCrossInstance and
// ClassifyMonitoredName, which is what `lrctl verify` renders). The duplication is
// structural: the redis-side copy is unexported, so this package cannot import it, and
// internal/redis cannot import the controller. Both answer the SAME question — is this
// stale entry ordinary debris of ours, or somebody else's live master? — so a drift
// would make `lrctl` disagree with the operator about what is prunable, in the tool
// CLAUDE.md §7 rule 8 designates as the ground-truth authority. That is LR-041's parity
// failure mode.
//
// This test is the only place the two can be compared: a _test.go in package controller
// can call the unexported predicate AND import internal/redis. It is TEST-ONLY on
// purpose — the shipped binaries stay decoupled.
//
// It is compared through the exported surface rather than against a hardcoded expected
// value, so it fails if EITHER side moves: for an address that is not one of our pods
// and a name that is not the desired one, ClassifyMonitoredName returns "stale"
// (ordinary debris) exactly when it considers the entry flagged down, and "foreign"
// otherwise.
//
// Green from birth by construction — a parity assertion cannot go red against correct
// code. Its teeth were shown by mutation: dropping the o_down clause from the
// internal/redis copy fails the "o_down,master" row, naming the flag and both verdicts
// ("controller flaggedDown = true, but ... returned \"foreign\""). The
// "s_down,o_down,master" row survives that particular mutation and correctly so — it
// still carries s_down — which is why o_down is also covered on its own.
func TestFlaggedDownParity(t *testing.T) {
	const (
		notOurName = "some-other-name"
		desired    = "ns.inst"
		notOurIP   = "10.9.9.9"
	)
	// Deliberately empty: the address must NOT be attributable to us, so the class
	// turns purely on the down-flags, which is the predicate under comparison.
	var noneOfOurIPs map[string]bool

	flagStrings := []string{
		"",
		RoleMaster,
		"slave",
		"s_down," + RoleMaster,
		"o_down," + RoleMaster,
		"s_down,o_down," + RoleMaster,
		// Must NOT read as down. The flags string carries other tokens, and a
		// sloppy substring test over the whole reply is exactly how this drifts.
		"failover_in_progress," + RoleMaster,
		"disconnected," + RoleMaster,
	}

	for _, flags := range flagStrings {
		t.Run("flags="+flags, func(t *testing.T) {
			class := redisclient.ClassifyMonitoredName(notOurName, notOurIP, flags, desired, noneOfOurIPs)
			cliSaysDown := class == redisclient.MasterNameStale
			if got := flaggedDown(flags); got != cliSaysDown {
				t.Fatalf(
					"down-discriminator drift on flags %q: controller flaggedDown = %v, "+
						"but redisclient.ClassifyMonitoredName returned %q (down = %v). "+
						"The operator and lrctl now disagree about whether this entry is "+
						"ordinary debris of ours or somebody else's live master.",
					flags, got, class, cliSaysDown)
			}
			// Sanity on the other half of the classification, so the comparison
			// cannot be satisfied by a class this test does not expect at all.
			if !cliSaysDown && class != redisclient.MasterNameForeign {
				t.Fatalf("flags %q classified %q, want %q or %q",
					flags, class, redisclient.MasterNameStale, redisclient.MasterNameForeign)
			}
		})
	}
}
