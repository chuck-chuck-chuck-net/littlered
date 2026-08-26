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
	"fmt"
	"strings"

	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// renderMasterNameScope turns the survey into the lines `verify` prints, and says
// whether the instance fails verification because of them.
//
// Pure, so the wording and the verdict are testable without a cluster.
func renderMasterNameScope(scope redisclient.MasterNameScope, desired string) (lines []string, fail bool) {
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }

	if len(scope.Findings) > 0 {
		add("  Monitored master names (every name each Sentinel carries):")
		for _, f := range scope.Findings {
			add("    - %s: %q at %s, flags:%s  (%s)",
				f.SentinelPod, f.Name, addrOrUnknown(f.IP), flagsOrNone(f.Flags), classLabel(f.Class))
		}
	}

	// Foreign first: it is the more serious of the two findings and the one that
	// changes what an owner should DO next (do not rename, let the quarantine run).
	if len(scope.Foreign) > 0 {
		fail = true
		add("  [FAIL] Master name(s) %s point at an address that is not one of this instance's",
			quoteList(scope.Foreign))
		add("         pods and is not flagged down — someone else's live master. This instance")
		add("         may be captured, and a rename does not escape a capture: it converts a")
		add("         diagnosed, self-healing capture into an undiagnosed leaderless refusal.")
	}
	if len(scope.Stale) > 0 {
		fail = true
		add("  [FAIL] Stale master name(s) %s are still monitored alongside %q.",
			quoteList(scope.Stale), desired)
		add("         One instance under two names runs two independent failover state machines")
		add("         over the same pods, which can promote different replicas (LR-039, LR-048).")
		add("         The operator prunes them once its gates pass — read the StaleMasterName")
		add("         condition on the CR, whose message names the gate that refused.")
	}
	if len(scope.Unreported) > 0 {
		// Not a failure: an unread list is no evidence either way (LR-041), and
		// reporting it as convergence is exactly the plausible-looking lie this
		// whole check exists to remove.
		add("  [WARN] Could not read the monitored master list from: %s — a leftover name",
			strings.Join(scope.Unreported, ", "))
		add("         there would not be visible to this check.")
	}
	if !fail && len(scope.Findings) > 0 {
		add("  [OK] Every reachable Sentinel monitors only %q.", desired)
	}
	return lines, fail
}

// classLabel is the human wording for a class. The two failing classes are
// deliberately named rather than given separate severity tokens: `verify` has one
// severity that fails ([FAIL]) and one that does not ([WARN]/[DEGRADED]), and both a
// stale and a foreign name must fail, so inventing a third token would be inventing
// an output idiom rather than reusing one.
func classLabel(class string) string {
	switch class {
	case redisclient.MasterNameDesired:
		return "desired"
	case redisclient.MasterNameStale:
		return "stale — ours"
	case redisclient.MasterNameForeign:
		return "FOREIGN — not one of our pods, and alive"
	default:
		return class
	}
}

func flagsOrNone(flags string) string {
	if flags == "" {
		return valueNone
	}
	return flags
}

func addrOrUnknown(ip string) string {
	if ip == "" {
		return "an address Sentinel did not report"
	}
	return ip
}

func quoteList(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, n := range names {
		quoted = append(quoted, fmt.Sprintf("%q", n))
	}
	return strings.Join(quoted, ", ")
}
