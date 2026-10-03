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

// Package toolchain holds the agreement test between the Go version that
// go.mod declares and the Go version the container builders compile with.
//
// Every CI job, including the govulncheck scan, selects its toolchain from
// the `go` directive in go.mod. Dependabot patches the `golang:` builder
// tags in the Dockerfiles but never the directive, so the two drift until
// the vulnerability database catches up with the older one: that is how the
// Security scan on main went red while the shipped images were already on a
// patched toolchain. The parsers here are the test's only inputs.
package toolchain

import (
	"errors"
	"regexp"
)

var (
	// goDirectiveRe matches the `go` directive of a go.mod file. The version
	// is captured whole so the test can insist on a full patch version.
	goDirectiveRe = regexp.MustCompile(`(?m)^go\s+(\S+)\s*$`)

	// builderFromRe matches the builder stage of a Dockerfile:
	// `FROM golang:<version>[-<variant>] AS builder`. The variant suffix
	// (`-alpine`) is deliberately not part of the version.
	builderFromRe = regexp.MustCompile(`(?m)^FROM\s+golang:(\d+\.\d+(?:\.\d+)?)(?:-[A-Za-z0-9.]+)?\s+AS\s+builder\s*$`)

	// fullVersionRe is the shape go.mod must declare: `major.minor.patch`.
	// A two-component directive such as `1.26` reads as a floor and lets
	// setup-go pick whatever patch it likes, which is the drift this test
	// exists to prevent.
	fullVersionRe = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

	errNoGoDirective = errors.New("no `go` directive found")
	errNoBuilderFrom = errors.New("no `FROM golang:<version> AS builder` line found")
)

// GoDirective returns the version declared by the `go` directive in the
// given go.mod contents.
func GoDirective(gomod string) (string, error) {
	m := goDirectiveRe.FindStringSubmatch(gomod)
	if m == nil {
		return "", errNoGoDirective
	}
	return m[1], nil
}

// BuilderGoVersion returns the Go version of the builder stage in the given
// Dockerfile contents, without any image-variant suffix.
func BuilderGoVersion(dockerfile string) (string, error) {
	m := builderFromRe.FindStringSubmatch(dockerfile)
	if m == nil {
		return "", errNoBuilderFrom
	}
	return m[1], nil
}

// IsFullVersion reports whether v names an exact patch release.
func IsFullVersion(v string) bool {
	return fullVersionRe.MatchString(v)
}
