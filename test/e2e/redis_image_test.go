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
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// mirroredRedis stands in for "the same redis image, kept in our own registry".
const mirroredRedis = "registry.example.lan/library/redis:8.4.2"

// The preflight probe is the one guaranteed-uncacheable public pull per work
// item (it uses imagePullPolicy: Always on purpose). If it does not resolve
// through the same knob as the CRs, a run "pinned to a mirror" still pulls the
// public ref every item and the pinning is a fiction.
func TestRedisImageRefResolvesThroughTheSameKnobAsTheCRs(t *testing.T) {
	if got, want := redisImageRefFrom(env(nil)), defaultRedis; got != want {
		t.Errorf("redisImageRef() with no override = %q, want the operator default %q", got, want)
	}
	if got := redisImageRefFrom(env(map[string]string{envRedisImage: mirroredRedis})); got != mirroredRedis {
		t.Errorf("redisImageRef() = %q, want the pinned mirror %q", got, mirroredRedis)
	}
}

// An UNSET E2E_REDIS_IMAGE must still render no image block at all. Emitting
// the default block instead would look equivalent and quietly stop exercising
// the operator's own defaulting — and would change every existing CR in the
// suite.
func TestRedisImageSpecFieldsIsEmptyWithoutAnOverride(t *testing.T) {
	if got := redisImageSpecFieldsFrom(env(nil)); got != "" {
		t.Errorf("redisImageSpecFields() with no override = %q, want \"\" (the operator defaults the image)", got)
	}
}

// The rendered block is spec-level YAML pasted into a CR literal, so its
// indentation is load-bearing: two spaces for the key, four for its fields.
func TestRedisImageSpecFieldsRendersASpecLevelBlock(t *testing.T) {
	got := redisImageSpecFieldsFrom(env(map[string]string{
		envRedisImage: mirroredRedis,
	}))
	want := "  image:\n" +
		"    registry: registry.example.lan\n" +
		"    path: library/redis\n" +
		"    tag: \"8.4.2\"\n" +
		"    pullPolicy: IfNotPresent\n"
	if got != want {
		t.Errorf("redisImageSpecFields() =\n%q\nwant\n%q", got, want)
	}
}

// Two renderings of one decision (a YAML block for the string builders, a typed
// ImageSpec for the ones that construct the object) must describe the SAME
// image, or "this run uses image X" quietly becomes "…except in pdb_test".
func TestRedisImageTypedAndYAMLRenderingsAgree(t *testing.T) {
	getenv := env(map[string]string{"E2E_REDIS_IMAGE": "host.example:5000/team/redis:1.2.3"})

	spec := redisImageSpecFrom(getenv)
	block := redisImageSpecFieldsFrom(getenv)
	for _, want := range []string{
		"registry: " + spec.Registry,
		"path: " + spec.Path,
		"tag: \"" + spec.Tag + "\"",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("YAML block %q disagrees with the typed spec %+v (missing %q)", block, spec, want)
		}
	}
	if spec.PullPolicy != corev1.PullIfNotPresent {
		t.Errorf("typed spec PullPolicy = %q, want %q (matching the YAML block)",
			spec.PullPolicy, corev1.PullIfNotPresent)
	}

	// Unset must stay unset in BOTH renderings: an empty ImageSpec lets the CRD
	// defaults apply, exactly as an empty YAML block does.
	if got := redisImageSpecFrom(env(nil)); !reflect.DeepEqual(got, littleredv1alpha1.ImageSpec{}) {
		t.Errorf("redisImageSpec() with no override = %+v, want the zero value", got)
	}
}

// withRedisImage is what actually reaches the ~20 YAML CR builders, so its
// three properties are load-bearing: no override changes nothing, an override
// appends a spec-level block, and a CR that already pins an image is left alone
// (the migration tier renders one itself — a second block would be a duplicate
// YAML key).
func TestWithRedisImage(t *testing.T) {
	const cr = "apiVersion: v1\nkind: LittleRed\nspec:\n  mode: standalone\n" +
		"  resources:\n    requests:\n      cpu: \"100m\"\n"

	t.Run("no override leaves the CR byte-identical", func(t *testing.T) {
		t.Setenv(envRedisImage, "")
		if got := withRedisImage(cr); got != cr {
			t.Errorf("withRedisImage() changed the CR with no override:\n%q", got)
		}
	})

	t.Run("an override appends a spec-level block", func(t *testing.T) {
		t.Setenv(envRedisImage, mirroredRedis)
		got := withRedisImage(cr)
		if !strings.HasPrefix(got, cr) {
			t.Errorf("withRedisImage() rewrote the original CR:\n%q", got)
		}
		if !strings.Contains(got, "\n  image:\n    registry: registry.example.lan\n") {
			t.Errorf("withRedisImage() did not append the image block at spec level:\n%q", got)
		}
	})

	t.Run("a CR that already pins an image is untouched", func(t *testing.T) {
		t.Setenv(envRedisImage, mirroredRedis)
		pinned := cr + "  image:\n    registry: other.example\n    path: library/redis\n    tag: \"7.4.0\"\n"
		if got := withRedisImage(pinned); got != pinned {
			t.Errorf("withRedisImage() added a second image block:\n%q", got)
		}
	})
}

// The suite calls the os.Getenv-backed entry points, not the *From variants
// the tables above drive. Cover them too, or the wiring could be wrong in a way
// every other test here would miss.
func TestRedisImageEntryPointsReadTheEnvironment(t *testing.T) {
	const ref = mirroredRedis
	t.Setenv(envRedisImage, ref)

	if got := redisImageRef(); got != ref {
		t.Errorf("redisImageRef() = %q, want %q", got, ref)
	}
	if got := redisImageSpec().Registry; got != "registry.example.lan" {
		t.Errorf("redisImageSpec().Registry = %q, want the override's registry", got)
	}
	if got := redisImageSpecFields(); !strings.Contains(got, "registry.example.lan") {
		t.Errorf("redisImageSpecFields() = %q, want the override's registry", got)
	}
	if got := redisImageDescription(); !strings.Contains(got, ref) {
		t.Errorf("redisImageDescription() = %q, want it to name the pinned image", got)
	}

	t.Setenv(envRedisImage, "")
	if got := redisImageRef(); got != defaultRedis {
		t.Errorf("redisImageRef() = %q, want the operator default %q", got, defaultRedis)
	}
	if got := redisImageDescription(); !strings.Contains(got, "operator-default") {
		t.Errorf("redisImageDescription() = %q, want it to say the operator default applies", got)
	}
}

// parseImageRef is moved code, so this is a characterization table: it pins the
// behaviour the migration tier already depends on (E2E_REDIS_IMAGE=redis:7.4.0
// must keep meaning docker.io/library/redis:7.4.0) plus the mirrored-ref and
// registry-port forms the harness introduces.
func TestParseImageRef(t *testing.T) {
	cases := []struct {
		ref                 string
		registry, path, tag string
	}{
		{"redis:7.4.0", defaultRedisRegistry, defaultRedisPath, "7.4.0"},
		{"redis", defaultRedisRegistry, defaultRedisPath, defaultRedisTag},
		{defaultRedisPath + ":" + defaultRedisTag, defaultRedisRegistry, defaultRedisPath, defaultRedisTag},
		{mirroredRedis, "registry.example.lan", defaultRedisPath, defaultRedisTag},
		{"host.example:5000/team/redis:1.2.3", "host.example:5000", "team/redis", "1.2.3"},
		{"localhost:5000/redis:1.0", "localhost:5000", "redis", "1.0"},
	}
	for _, tc := range cases {
		t.Run(tc.ref, func(t *testing.T) {
			reg, path, tag := parseImageRef(tc.ref)
			if reg != tc.registry || path != tc.path || tag != tc.tag {
				t.Errorf("parseImageRef(%q) = (%q, %q, %q), want (%q, %q, %q)",
					tc.ref, reg, path, tag, tc.registry, tc.path, tc.tag)
			}
		})
	}
}
