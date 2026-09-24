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

package multisite

import (
	"encoding/json"
	"testing"
)

func TestExtractResultJSON(t *testing.T) {
	result := `{
  "instance": "ms-smoke",
  "namespace": "ms-smoke",
  "groundTruth": {"ClusterState": "ok", "TotalSlots": 16384}
}`

	t.Run("clean output", func(t *testing.T) {
		got, err := ExtractResultJSON(result)
		if err != nil {
			t.Fatal(err)
		}
		assertDecodes(t, got)
	})

	t.Run("interleaved zap log lines before and after", func(t *testing.T) {
		// A hub Job's pod log merges the operator's stderr (zap/site-client logs,
		// themselves single-line JSON objects) with our stdout JSON. Extraction
		// must pick our object out of the noise.
		noisy := `{"level":"info","ts":"2026-08-07T10:00:00Z","logger":"setup","msg":"starting"}` + "\n" +
			`{"level":"info","ts":"2026-08-07T10:00:01Z","logger":"site-client","msg":"site cache did not sync"}` + "\n" +
			result + "\n" +
			`{"level":"info","ts":"2026-08-07T10:00:05Z","msg":"done"}` + "\n"
		got, err := ExtractResultJSON(noisy)
		if err != nil {
			t.Fatal(err)
		}
		assertDecodes(t, got)
	})

	t.Run("brace inside a string value is not a false boundary", func(t *testing.T) {
		tricky := `{"level":"info","msg":"weird } brace { in message"}` + "\n" +
			`{"instance":"x","groundTruth":{"note":"a } inside"}}` + "\n"
		got, err := ExtractResultJSON(tricky)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(got), &m); err != nil {
			t.Fatalf("extracted substring is not valid JSON: %v\n%s", err, got)
		}
		if m["instance"] != "x" {
			t.Errorf("extracted the wrong object: %v", m)
		}
	})

	t.Run("no result object present → error", func(t *testing.T) {
		if _, err := ExtractResultJSON(`{"level":"info","msg":"no gather here"}`); err == nil {
			t.Error("expected error when no groundTruth object is present")
		}
	})
}

func assertDecodes(t *testing.T, s string) {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("extracted substring is not valid JSON: %v\n%s", err, s)
	}
	if _, ok := m["groundTruth"]; !ok {
		t.Errorf("extracted object lacks groundTruth: %s", s)
	}
}
