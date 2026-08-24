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
	"strings"
	"testing"
)

// Plain table tests, not Ginkgo specs, needing no cluster. They live here only
// because the code they guard carries the e2e build tag. Run them on their own —
// an unfiltered `go test -tags e2e ./test/e2e/...` starts the whole suite:
//
//	go test -tags e2e ./test/e2e/ -run 'TestE2EAuth'

// The credential registry is the single point where a missed auth plumbing turns
// into an opaque NOAUTH, and its one non-obvious rule is LONGEST-prefix matching:
// map iteration order is random, so a "first match wins" lookup would attribute
// `iso-a-17-redis-0` to whichever of `iso-a-17` / `iso-a-17-extra` Go happened to
// walk first, non-deterministically, on some runs only.
func TestE2EPasswordForResourceLongestPrefixWins(t *testing.T) {
	e2eAuthMu.Lock()
	saved := e2eAuthByCRName
	e2eAuthByCRName = map[string]string{}
	e2eAuthMu.Unlock()
	t.Cleanup(func() {
		e2eAuthMu.Lock()
		e2eAuthByCRName = saved
		e2eAuthMu.Unlock()
	})

	registerE2EAuth("fo")
	registerE2EAuth("fo-min")

	cases := []struct {
		name     string
		resource string
		want     string
	}{
		{"pod of the short instance", "fo-redis-0", e2eAuthPassword("fo")},
		{"pod of the long instance", "fo-min-redis-0", e2eAuthPassword("fo-min")},
		{"sentinel pod of the long instance", "fo-min-sentinel-2", e2eAuthPassword("fo-min")},
		{"the CR/Service name itself", "fo-min", e2eAuthPassword("fo-min")},
		{"unregistered instance is auth-free", "cluster-basic-shard-0-0", ""},
		{"a prefix that is not a name boundary", "fool-redis-0", ""},
	}
	for _, tc := range cases {
		if got := e2ePasswordForResource(tc.resource); got != tc.want {
			t.Errorf("%s: e2ePasswordForResource(%q) = %q, want %q", tc.name, tc.resource, got, tc.want)
		}
	}

	// And the arg shape the exec helpers splice in.
	if got := redisCliAuthArgs("cluster-basic-shard-0-0"); got != nil {
		t.Errorf("auth-free instance must get NO redis-cli args, got %v", got)
	}
	if got := redisCliAuthArgs("fo-min-redis-0"); len(got) != 3 ||
		got[0] != "-a" || got[1] != e2eAuthPassword("fo-min") || got[2] != "--no-auth-warning" {
		t.Errorf("redisCliAuthArgs = %v, want [-a <pw> --no-auth-warning]", got)
	}
}

// The Secret rides in front of the CR in one apply stream, so the document
// separator and the base64 encoding are load-bearing: get either wrong and the
// CR either never applies or references a Secret whose password nothing matches.
func TestE2EAuthSecretDocRendersOneApplyableStream(t *testing.T) {
	doc := e2eAuthSecretDoc("fo-x")
	for _, want := range []string{
		"kind: Secret",
		"name: fo-x-auth",
		"namespace: " + testNamespace,
		// base64("e2e-pw-fo-x")
		"password: ZTJlLXB3LWZvLXg=",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("secret doc missing %q:\n%s", want, doc)
		}
	}
	if !strings.HasSuffix(strings.TrimRight(doc, "\n"), "---") {
		t.Errorf("secret doc must end with a YAML document separator so the CR follows it:\n%s", doc)
	}
	if !strings.Contains(e2eAuthSpecYAML("fo-x"), "existingSecret: fo-x-auth") {
		t.Errorf("spec.auth block must reference the instance's own Secret")
	}
}
