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

// Untagged on purpose (see operator_deploy.go): resolving which redis image a
// run uses is a pure decision and belongs under plain `go test`.
//
// These helpers were previously private to cluster_migration_test.go, which
// used them for VERSION-REGRESSION fidelity (E2E_REDIS_IMAGE=redis:7.4.0
// reproduces the environment that drove the LR-025 redesign). They are promoted
// here unchanged in behaviour and given a second job: pointing a whole run at a
// MIRRORED copy of the redis image, so an unattended array does not hard-depend
// on Docker Hub (design §9.3, §11 M2). One ref, two uses — deliberately the
// same knob, because "which redis does this run use" is one question.

package e2e

import (
	"fmt"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// envRedisImage names the image every part of a run resolves through.
const envRedisImage = "E2E_REDIS_IMAGE"

// The operator's own image defaults (api/v1alpha1 littlered_defaults.go),
// mirrored here because the e2e suite cannot import the defaulting webhook's
// logic. parseImageRef fills them in for a partial ref, and defaultRedis is
// composed from them so the two can never drift apart.
const (
	defaultRedisRegistry = "docker.io"
	defaultRedisPath     = "library/redis"
	defaultRedisTag      = "8.4.2"
)

// defaultRedis is the image LittleRed uses when a CR names none. The e2e
// preflight probes exactly this ref, so it must track the operator's own default.
const defaultRedis = defaultRedisRegistry + "/" + defaultRedisPath + ":" + defaultRedisTag

// redisImageRef is the redis image this run actually uses: E2E_REDIS_IMAGE when
// set, the operator default otherwise. The preflight probe resolves through it
// so that a run pinned to a mirror does not still pull the public ref — the
// preflight is the ONE guaranteed-uncacheable pull per work item, because it
// deliberately uses imagePullPolicy: Always.
func redisImageRef() string { return redisImageRefFrom(os.Getenv) }

func redisImageRefFrom(getenv func(string) string) string {
	if ref := strings.TrimSpace(getenv(envRedisImage)); ref != "" {
		return ref
	}
	return defaultRedis
}

// redisImageSpecFields returns a `spec.image` YAML block for E2E_REDIS_IMAGE, or
// "" when the env var is unset (in which case the operator default — Redis
// 8.4.2 — applies). Returning "" rather than the default block is deliberate:
// an unset run must produce exactly the CR it always produced, so the operator's
// own defaulting stays under test.
//
// For the migration tier this flows to BOTH the pre-split bootstrap pods AND the
// migration per-shard pods (the migration operator redeploys over the SAME CR
// with no spec change), so source and destination nodes run the intended
// version.
//
//	Regression run: E2E_REDIS_IMAGE=redis:7.4.0
//	Mirrored run:   E2E_REDIS_IMAGE=registry.example.lan/library/redis:8.4.2
//	Default run:    E2E_REDIS_IMAGE unset (redis:8.4.2 from the operator)
func redisImageSpecFields() string { return redisImageSpecFieldsFrom(os.Getenv) }

func redisImageSpecFieldsFrom(getenv func(string) string) string {
	ref := strings.TrimSpace(getenv(envRedisImage))
	if ref == "" {
		return ""
	}
	reg, path, tag := parseImageRef(ref)
	return fmt.Sprintf("  image:\n    registry: %s\n    path: %s\n    tag: %q\n    pullPolicy: IfNotPresent\n",
		reg, path, tag)
}

// withRedisImage pins the run's redis image into a rendered LittleRed CR, so a
// run with E2E_REDIS_IMAGE set never falls back to Docker Hub for the pods the
// operator creates. It is a no-op when no override is set (the operator's own
// defaulting stays under test) and when the CR already carries an image block.
//
// The block is APPENDED rather than spliced after `spec:`: every CR in this
// suite is a spec-level mapping, and a key at indentation 2 closes whatever
// deeper mapping preceded it, so appending is position-independent and cannot
// be broken by a caller's extra fields. Splicing would have to know where each
// builder's extras belong.
func withRedisImage(cr string) string {
	if strings.Contains(cr, "\n  image:") {
		return cr
	}
	if !strings.HasSuffix(cr, "\n") {
		cr += "\n"
	}
	return cr + redisImageSpecFields()
}

// redisImageSpecFrom is the TYPED sibling of redisImageSpecFieldsFrom, for the
// CR builders that construct the object instead of rendering YAML. A zero value
// means "no override": the CRD defaults then apply, exactly as an empty YAML
// block leaves them to.
func redisImageSpecFrom(getenv func(string) string) littleredv1alpha1.ImageSpec {
	ref := strings.TrimSpace(getenv(envRedisImage))
	if ref == "" {
		return littleredv1alpha1.ImageSpec{}
	}
	reg, path, tag := parseImageRef(ref)
	return littleredv1alpha1.ImageSpec{
		Registry:   reg,
		Path:       path,
		Tag:        tag,
		PullPolicy: corev1.PullIfNotPresent,
	}
}

// redisImageSpec pins the run's redis image in a typed CR.
func redisImageSpec() littleredv1alpha1.ImageSpec { return redisImageSpecFrom(os.Getenv) }

// redisImageDescription is a short suffix for the CR-creation By() line noting the redis image.
func redisImageDescription() string {
	if ref := strings.TrimSpace(os.Getenv(envRedisImage)); ref != "" {
		return " with redis image " + ref
	}
	return " with the operator-default redis image"
}

// parseImageRef splits a Docker-style image reference into (registry, path, tag) using the same
// defaults as api/v1alpha1.ImageSpec (docker.io / library/redis / 8.4.2). It handles the two
// forms this suite uses — a bare "redis:TAG" (→ docker.io/library/redis:TAG) and an explicit
// "host[:port]/path:TAG" — so E2E_REDIS_IMAGE can name either an official image tag or a full ref.
func parseImageRef(ref string) (registry, path, tag string) {
	// path is assigned on every branch below; only the registry and the tag have
	// a genuine fallback.
	registry, tag = defaultRedisRegistry, defaultRedisTag

	name := ref
	// A ':' that comes AFTER the last '/' delimits the tag (a ':' before it is a registry port).
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		name = ref[:i]
		tag = ref[i+1:]
	}

	if first, rest, ok := strings.Cut(name, "/"); ok {
		// A leading segment that looks like a hostname (contains '.' or ':') or is "localhost"
		// is the registry; otherwise the whole thing is the image path (e.g. "library/redis").
		if strings.ContainsAny(first, ".:") || first == "localhost" {
			registry = first
			path = rest
		} else {
			path = name
		}
	} else {
		// Bare official image name (e.g. "redis") → docker.io library namespace.
		path = "library/" + name
	}
	return registry, path, tag
}
