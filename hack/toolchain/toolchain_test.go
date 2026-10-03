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

package toolchain

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	goModPath = "../../go.mod"

	// sample is the version the parser tables use; it carries no meaning
	// beyond being a well-formed patch release.
	sample = "1.26.8"
)

// builderDockerfiles are every Dockerfile whose builder stage compiles Go
// from this module. Add a new image here when it gains a Go builder stage;
// the test is only as complete as this list.
var builderDockerfiles = []string{
	"../../cmd/littlered/Dockerfile",
	"../../cmd/littlered-chaos-client/Dockerfile",
}

func TestGoDirective(t *testing.T) {
	tests := []struct {
		name    string
		gomod   string
		want    string
		wantErr bool
	}{
		{name: "full patch version", gomod: "module m\n\ngo 1.26.8\n\nrequire x v1\n", want: sample},
		{name: "two components", gomod: "module m\n\ngo 1.26\n", want: "1.26"},
		{
			name:  "toolchain line is not the directive",
			gomod: "module m\n\ngo 1.26.8\ntoolchain go1.27.1\n",
			want:  sample,
		},
		{name: "missing", gomod: "module m\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GoDirective(tt.gomod)
			if (err != nil) != tt.wantErr {
				t.Fatalf("GoDirective() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("GoDirective() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuilderGoVersion(t *testing.T) {
	tests := []struct {
		name       string
		dockerfile string
		want       string
		wantErr    bool
	}{
		{
			name:       "plain tag",
			dockerfile: "# Build the manager binary\nFROM golang:1.26.8 AS builder\nARG TARGETOS\n",
			want:       sample,
		},
		{name: "variant suffix is dropped", dockerfile: "FROM golang:1.26.8-alpine AS builder\n", want: sample},
		{
			name:       "second stage is ignored",
			dockerfile: "FROM golang:1.26.8 AS builder\nFROM gcr.io/distroless/static:nonroot\n",
			want:       sample,
		},
		{name: "release candidate tag", dockerfile: "FROM golang:1.27rc2 AS builder\n", wantErr: true},
		{name: "no builder stage", dockerfile: "FROM alpine:3.24.1\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuilderGoVersion(tt.dockerfile)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuilderGoVersion() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("BuilderGoVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestBuildersMatchGoDirective is the agreement test: the Go version the
// container builders compile with must be the one go.mod declares, because
// go.mod is what every CI job, govulncheck included, actually runs. A
// Dependabot patch bump to a `golang:` builder tag fails here until the
// `go` directive is moved with it; a `go` directive moved on its own fails
// here until the builders follow.
func TestBuildersMatchGoDirective(t *testing.T) {
	gomod, err := os.ReadFile(goModPath)
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	want, err := GoDirective(string(gomod))
	if err != nil {
		t.Fatalf("go.mod: %v", err)
	}
	if !IsFullVersion(want) {
		t.Fatalf("go.mod declares `go %s`; declare a full patch version (major.minor.patch) "+
			"so CI resolves an exact toolchain", want)
	}

	for _, path := range builderDockerfiles {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			got, err := BuilderGoVersion(string(raw))
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			if got != want {
				t.Errorf("%s builds with golang:%s but go.mod declares `go %s`; move both together "+
					"(patch bumps to the builder image arrive from Dependabot, the `go` directive does not)",
					path, got, want)
			}
		})
	}
}
