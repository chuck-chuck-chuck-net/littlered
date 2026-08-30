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
	"os"
	"reflect"
	"testing"

	"sigs.k8s.io/yaml"
)

const (
	fieldName  = "name"
	typeString = "string"
)

// statusSchema returns the generated CRD's schema for status.<field>, so an
// assertion can be structural rather than a substring match on the whole file
// (`x-kubernetes-list-type: map` already appears for status.conditions, so a
// substring check would pass without the new field existing at all).
func statusSchema(t *testing.T, field string) map[string]any {
	t.Helper()
	const crdPath = "../../config/crd/bases/redis.chuck-chuck-chuck.net_littlereds.yaml"
	raw, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("reading generated CRD: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parsing generated CRD: %v", err)
	}
	cur := any(doc)
	path := []any{"spec", "versions", 0, "schema", "openAPIV3Schema", "properties", "status", "properties", field}
	for i, step := range path {
		switch k := step.(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok || m[k] == nil {
				t.Fatalf("generated CRD has no %v (missing at %q); run `make manifests`", path[:i+1], k)
			}
			cur = m[k]
		case int:
			a, ok := cur.([]any)
			if !ok || len(a) <= k {
				t.Fatalf("generated CRD has no %v; run `make manifests`", path[:i+1])
			}
			cur = a[k]
		}
	}
	out, ok := cur.(map[string]any)
	if !ok {
		t.Fatalf("status.%s is %T, want an object schema", field, cur)
	}
	return out
}

// TestCRDAcknowledgedOperationsIsAListMap guards the marker set on
// status.acknowledgedOperations. The list-map semantics are not cosmetic: the ack
// list is one row per operation NAME, updated in place, and a server-side apply of
// a plain atomic list would replace the whole record rather than merge one row —
// so a second operation's acknowledgment would silently erase the first, which
// reads as unfinished work and re-runs completed work.
func TestCRDAcknowledgedOperationsIsAListMap(t *testing.T) {
	s := statusSchema(t, "acknowledgedOperations")

	if got := s["type"]; got != "array" {
		t.Errorf("type = %v, want array", got)
	}
	if got := s["x-kubernetes-list-type"]; got != "map" {
		t.Errorf("x-kubernetes-list-type = %v, want map (the +listType=map marker)", got)
	}
	if got := s["x-kubernetes-list-map-keys"]; !reflect.DeepEqual(got, []any{fieldName}) {
		t.Errorf("x-kubernetes-list-map-keys = %v, want [name] (the +listMapKey=name marker)", got)
	}

	items, ok := s["items"].(map[string]any)
	if !ok {
		t.Fatalf("items = %T, want an object schema", s["items"])
	}
	props, ok := items["properties"].(map[string]any)
	if !ok {
		t.Fatalf("items.properties = %T, want an object", items["properties"])
	}
	for field, wantType := range map[string]string{
		fieldName:        typeString,
		"fingerprint":    typeString,
		"acknowledgedAt": typeString, // metav1.Time renders as string/date-time
	} {
		p, ok := props[field].(map[string]any)
		if !ok {
			t.Errorf("OperationAck is missing %q", field)
			continue
		}
		if p["type"] != wantType {
			t.Errorf("OperationAck.%s type = %v, want %s", field, p["type"], wantType)
		}
	}
	if got, want := items["required"], []any{"acknowledgedAt", "fingerprint", fieldName}; !reflect.DeepEqual(got, want) {
		t.Errorf("OperationAck required = %v, want %v", got, want)
	}
}

// TestCRDOperationIsAnObject guards status.operation, the monitoring surface. It is
// a single object (not a list), and Pending is the queue behind the running
// operation. Deliberately asserted here too: there is NO precedence field, and
// ADR-020 rejected one outright (Alternatives E and F) — ordering comes from the
// operation already running, from admission refusal, and from declared Requires
// dependencies on the registry entry, never from a number in the API.
func TestCRDOperationIsAnObject(t *testing.T) {
	s := statusSchema(t, "operation")

	if got := s["type"]; got != "object" {
		t.Errorf("type = %v, want object", got)
	}
	props, ok := s["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties = %T, want an object", s["properties"])
	}
	for _, field := range []string{fieldName, "startedAt", "reason", "pending"} {
		if _, ok := props[field]; !ok {
			t.Errorf("OperationStatus is missing %q", field)
		}
	}
	if p, ok := props["pending"].(map[string]any); ok {
		if p["type"] != "array" {
			t.Errorf("OperationStatus.pending type = %v, want array", p["type"])
		}
	}
	for _, forbidden := range []string{"precedence", "priority", "previousValue", "phase", "cursor"} {
		if _, ok := props[forbidden]; ok {
			t.Errorf("OperationStatus carries %q; ADR-020 rejected it (D1/D3, Alternatives E and F)", forbidden)
		}
	}
	if got, want := s["required"], []any{fieldName, "reason", "startedAt"}; !reflect.DeepEqual(got, want) {
		t.Errorf("OperationStatus required = %v, want %v", got, want)
	}
}
