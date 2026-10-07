//go:build e2e
// +build e2e

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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/gomega"
)

// Dataset read-back for the cluster tiers (migration, image upgrade): the seed helper
// writes keys behind one hash tag per shard, so a key's tag IS its shard, and a report
// grouped by tag is a report grouped by shard.
//
// Why this file exists. On 2026-10-05 a cluster image upgrade destroyed shard 2's entire
// 20-key dataset, and the tier reported it as `value mismatch for key {t4}mig:14` — one
// arbitrary key, because the old verifyDataset iterated a map and failed on the first
// mismatch. "One key is missing" reads as a replication tail; "a shard died" is a
// redundancy-gate failure. The two lead to different investigations, and whoever picks a
// failure up from the summary line alone drills in the wrong direction. So the read-back
// collects EVERY mismatch, the report states the blast radius first, grouped by shard,
// and an ABSENT key is distinguished from one holding a WRONG value (LR-038: lost is not
// corrupt). The rendering is pure and pinned in dataset_report_unit_test.go.

// datasetMismatch is one seeded key that did not read back as written.
type datasetMismatch struct {
	Key  string
	Want string
	// Got is the value read back; meaningless when Absent.
	Got string
	// Absent: the key does not exist at all, as opposed to existing with a wrong value.
	Absent bool
}

// datasetMismatchListLimit bounds how many keys the report lists per tag. A lost shard
// has dozens; the count is the finding and the first few keys are the evidence.
const datasetMismatchListLimit = 5

// datasetTag returns the leading `{tag}` of a key, or "" when it has none. The seed
// helpers put exactly one at the front of every key.
func datasetTag(key string) string {
	if !strings.HasPrefix(key, "{") {
		return ""
	}
	end := strings.Index(key, "}")
	if end <= 1 {
		return ""
	}
	return key[:end+1]
}

// describeDatasetMismatches renders the mismatches for a failure message: the blast
// radius first, then one block per hash tag (sorted), each with its own count, an
// absent/wrong-value split, and a bounded, sorted key list. Empty when nothing
// mismatched, so a clean read-back never reads as a finding. Deterministic in the input
// order, because the same incident must render the same way on every call.
func describeDatasetMismatches(data map[string]string, mismatches []datasetMismatch) string {
	if len(mismatches) == 0 {
		return ""
	}
	seededByTag := make(map[string]int)
	for k := range data {
		seededByTag[datasetTag(k)]++
	}
	byTag := make(map[string][]datasetMismatch)
	for _, m := range mismatches {
		tag := datasetTag(m.Key)
		byTag[tag] = append(byTag[tag], m)
	}
	tags := make([]string, 0, len(byTag))
	for tag := range byTag {
		tags = append(tags, tag)
	}
	sort.Strings(tags)

	var b strings.Builder
	fmt.Fprintf(&b, "%d of %d seeded keys did not read back. Grouped by hash tag, i.e. by shard:\n",
		len(mismatches), len(data))
	for _, tag := range tags {
		ms := byTag[tag]
		sort.Slice(ms, func(i, j int) bool { return ms[i].Key < ms[j].Key })
		absent, wrong := 0, 0
		for _, m := range ms {
			if m.Absent {
				absent++
			} else {
				wrong++
			}
		}
		var parts []string
		if absent > 0 {
			parts = append(parts, fmt.Sprintf("%d absent", absent))
		}
		if wrong > 0 {
			parts = append(parts, fmt.Sprintf("%d wrong value", wrong))
		}
		label := tag
		if label == "" {
			label = "(untagged)"
		}
		fmt.Fprintf(&b, "  %s: %d of %d seeded — %s\n", label, len(ms), seededByTag[tag], strings.Join(parts, ", "))
		for i, m := range ms {
			if i == datasetMismatchListLimit {
				fmt.Fprintf(&b, "      ... (and %d more)\n", len(ms)-datasetMismatchListLimit)
				break
			}
			if m.Absent {
				fmt.Fprintf(&b, "      %s absent\n", m.Key)
			} else {
				fmt.Fprintf(&b, "      %s = %q, want %q\n", m.Key, m.Got, m.Want)
			}
		}
	}
	return b.String()
}

// readDatasetMismatches reads every seeded key back through readPod (a -c client, so
// redirection resolves whichever node owns the slot) and returns the ones that did not
// come back as written.
//
// Cost is per TAG, not per key: keys sharing a hash tag share a slot, so one EXISTS and
// one MGET per tag are legal in cluster mode — three execs for a 150-key dataset, the
// same shape verifySentinelDataset already uses. The per-key classification runs only
// for a tag whose EXISTS count fell short, i.e. on the failure path, where the extra
// execs buy the absent-vs-wrong split the report is for.
//
// An exec error is returned, never recorded as an absence (LR-051: unreachability is
// not emptiness); the caller retries the whole attempt.
func readDatasetMismatches(readPod string, data map[string]string) ([]datasetMismatch, error) {
	byTag := make(map[string][]string)
	for k := range data {
		tag := datasetTag(k)
		byTag[tag] = append(byTag[tag], k)
	}
	tags := make([]string, 0, len(byTag))
	for tag := range byTag {
		tags = append(tags, tag)
	}
	sort.Strings(tags)

	var mismatches []datasetMismatch
	for _, tag := range tags {
		keys := byTag[tag]
		sort.Strings(keys)
		out, err := redisExec(testNamespace, readPod, append([]string{"-c", "EXISTS"}, keys...)...)
		if err != nil {
			return nil, fmt.Errorf("EXISTS over tag %q: %w", tag, err)
		}
		present, err := strconv.Atoi(strings.TrimSpace(out))
		if err != nil {
			return nil, fmt.Errorf("EXISTS over tag %q did not return a count: %q", tag, out)
		}
		if present == len(keys) {
			// Every key exists, so MGET's output has no nil lines and splits cleanly.
			out, err = redisExec(testNamespace, readPod, append([]string{"-c", "MGET"}, keys...)...)
			if err != nil {
				return nil, fmt.Errorf("MGET over tag %q: %w", tag, err)
			}
			got := strings.Split(strings.TrimSpace(out), "\n")
			if len(got) != len(keys) {
				return nil, fmt.Errorf("MGET over tag %q returned %d values for %d keys: %q", tag, len(got), len(keys), out)
			}
			for i, k := range keys {
				if v := strings.TrimSpace(got[i]); v != data[k] {
					mismatches = append(mismatches, datasetMismatch{Key: k, Want: data[k], Got: v})
				}
			}
			continue
		}
		// Shortfall: classify each key, so the report can say which are gone.
		for _, k := range keys {
			out, err := redisExec(testNamespace, readPod, "-c", "EXISTS", k)
			if err != nil {
				return nil, fmt.Errorf("EXISTS %s: %w", k, err)
			}
			if strings.TrimSpace(out) == "0" {
				mismatches = append(mismatches, datasetMismatch{Key: k, Want: data[k], Absent: true})
				continue
			}
			out, err = redisExec(testNamespace, readPod, "-c", "GET", k)
			if err != nil {
				return nil, fmt.Errorf("GET %s: %w", k, err)
			}
			if v := strings.TrimSpace(out); v != data[k] {
				mismatches = append(mismatches, datasetMismatch{Key: k, Want: data[k], Got: v})
			}
		}
	}
	return mismatches, nil
}

// verifyDatasetWithin asserts, within timeout, that every seeded key reads back with its
// exact value through readPod. On failure the message is the whole blast radius grouped
// by shard (describeDatasetMismatches), never a single arbitrary key. `what` names the
// operation the dataset was supposed to survive.
func verifyDatasetWithin(readPod string, data map[string]string, timeout time.Duration, what string) {
	EventuallyWithOffset(1, func(g Gomega) {
		mismatches, err := readDatasetMismatches(readPod, data)
		g.Expect(err).NotTo(HaveOccurred(), "reading the dataset back through %s", readPod)
		g.Expect(mismatches).To(BeEmpty(), "%s\n%s", what, describeDatasetMismatches(data, mismatches))
	}, timeout, 10*time.Second).Should(Succeed())
}
