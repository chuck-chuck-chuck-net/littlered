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
	"math/rand"
	"strings"
	"testing"
)

// Plain table tests, not Ginkgo specs, needing no cluster (the auth_utils_unit_test.go
// precedent). They pin the dataset-mismatch RENDERING, which is pure and therefore
// decidable here rather than behind a 12-minute rollout.
//
//	go test -tags e2e ./test/e2e/ -run 'TestDescribeDatasetMismatches|TestDatasetTag' -count=1
//
// Why this exists: the 2026-10-05 cluster image-upgrade failure destroyed one shard's
// entire 20-key dataset, and the tier reported it as
//
//	value mismatch for key {t4}mig:14
//
// — one arbitrary key, because the old verifyDataset iterated a map and failed on the
// first mismatch. "One key is missing" and "a shard died" lead to different
// investigations. The report must state the blast radius first, grouped by hash tag
// (which is by shard), and must distinguish a key that is ABSENT from one holding a
// WRONG value (LR-038: lost is not corrupt).

// incidentDataset rebuilds the failing tier's shape: 20 keys per shard behind one hash
// tag each, {t2}/{t1}/{t4} for shards 0/1/2, exactly as writeDatasetSpanningShards seeds.
func incidentDataset() map[string]string {
	data := make(map[string]string, 60)
	for _, tag := range []string{"{t2}", "{t1}", "{t4}"} {
		for j := 1; j <= 20; j++ {
			data[fmt.Sprintf("%smig:%d", tag, j)] = fmt.Sprintf("v-%s-%d", tag, j)
		}
	}
	return data
}

// shardLost returns the mismatches a whole lost shard produces: every key under tag absent.
func shardLost(data map[string]string, tag string) []datasetMismatch {
	var ms []datasetMismatch
	for k, v := range data {
		if strings.HasPrefix(k, tag) {
			ms = append(ms, datasetMismatch{Key: k, Want: v, Absent: true})
		}
	}
	return ms
}

func TestDescribeDatasetMismatchesStatesTheBlastRadiusFirst(t *testing.T) {
	data := incidentDataset()
	got := describeDatasetMismatches(data, shardLost(data, "{t4}"))

	first := strings.SplitN(got, "\n", 2)[0]
	if !strings.HasPrefix(first, "20 of 60 seeded keys did not read back") {
		t.Fatalf("report does not state the blast radius (want \"20 of 60\"): %s", got)
	}
	if !strings.Contains(got, "{t4}: 20 of 20 seeded") {
		t.Errorf("report does not attribute the loss to the shard's hash tag: %s", got)
	}
	if !strings.Contains(got, "20 absent") {
		t.Errorf("report does not say the keys are ABSENT (as opposed to wrong): %s", got)
	}
	for _, other := range []string{"{t1}", "{t2}"} {
		if strings.Contains(got, other) {
			t.Errorf("report names %s, which lost nothing: %s", other, got)
		}
	}
	// The list is bounded: a lost shard of 20 must not print 20 lines, but must say how
	// many it left out, so the count stays verifiable.
	if n := strings.Count(got, " absent\n"); n != datasetMismatchListLimit+1 {
		// +1 for the per-tag summary line, which also ends in "absent".
		t.Errorf("report lists %d keys, want %d plus a summary: %s", n-1, datasetMismatchListLimit, got)
	}
	if !strings.Contains(got, fmt.Sprintf("(and %d more)", 20-datasetMismatchListLimit)) {
		t.Errorf("report does not say how many keys it left out: %s", got)
	}
}

func TestDescribeDatasetMismatchesDistinguishesAbsentFromWrongValue(t *testing.T) {
	data := incidentDataset()
	ms := []datasetMismatch{
		{Key: "{t1}mig:3", Want: "v-{t1}-3", Absent: true},
		{Key: "{t2}mig:7", Want: "v-{t2}-7", Got: "stale"},
	}
	got := describeDatasetMismatches(data, ms)

	if !strings.HasPrefix(got, "2 of 60 seeded keys did not read back") {
		t.Fatalf("blast radius: %s", got)
	}
	if !strings.Contains(got, "{t1}: 1 of 20 seeded — 1 absent") {
		t.Errorf("absent key not reported as absent under its tag: %s", got)
	}
	if !strings.Contains(got, "{t2}: 1 of 20 seeded — 1 wrong value") {
		t.Errorf("wrong value not reported as such under its tag: %s", got)
	}
	if !strings.Contains(got, `{t2}mig:7 = "stale", want "v-{t2}-7"`) {
		t.Errorf("wrong value does not show got and want: %s", got)
	}
	if !strings.Contains(got, "{t1}mig:3 absent") {
		t.Errorf("absent key line missing: %s", got)
	}
}

func TestDescribeDatasetMismatchesIsDeterministic(t *testing.T) {
	data := incidentDataset()
	ms := shardLost(data, "{t4}")
	ms = append(ms, datasetMismatch{Key: "{t1}mig:9", Want: "v-{t1}-9", Got: "x"})
	want := describeDatasetMismatches(data, ms)
	for range 20 {
		rand.Shuffle(len(ms), func(a, b int) { ms[a], ms[b] = ms[b], ms[a] })
		if got := describeDatasetMismatches(data, ms); got != want {
			t.Fatalf("report depends on input order:\n--- first ---\n%s\n--- shuffled ---\n%s", want, got)
		}
	}
}

// Positive control: a clean read-back renders nothing, so the report cannot be
// mistaken for a finding when there is none. Green from birth by construction; its
// teeth are shown by removing the empty-mismatch early return.
func TestDescribeDatasetMismatchesIsSilentOnSuccess(t *testing.T) {
	if got := describeDatasetMismatches(incidentDataset(), nil); got != "" {
		t.Errorf("describeDatasetMismatches(data, nil) = %q, want empty", got)
	}
}

func TestDatasetTag(t *testing.T) {
	for in, want := range map[string]string{
		"{t4}mig:14":  "{t4}",
		"{t12}mig:1":  "{t12}",
		"mig:1":       "",
		"{}mig:1":     "",
		"{unclosed":   "",
		"img:7":       "",
		"{a}{b}mig:1": "{a}",
	} {
		if got := datasetTag(in); got != want {
			t.Errorf("datasetTag(%q) = %q, want %q", in, got, want)
		}
	}
}
