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

// Package tooling holds guard tests for the build toolchain — the parts of the repo that
// are configuration rather than code, and so are invisible to the compiler and the linter.
package tooling

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const (
	goModPath        = "../../go.mod"
	lintWorkflowPath = "../../.github/workflows/lint.yml"

	golangciModule = "github.com/golangci/golangci-lint/v2"
	golangciAction = "golangci/golangci-lint-action@"
)

// semverPattern is deliberately strict. A drift guard that accepts anything would pass
// on a parse failure, which is the one way it could report green while drifting.
var semverPattern = regexp.MustCompile(`^v\d+\.\d+\.\d+`)

// TestGolangciLintVersionMatchesCI is the guard for issue #98: `make lint` resolves the
// linter version from the go.mod tool pin (Makefile: GOLANGCI_LINT_VERSION via gomodver),
// while the CI workflow states it a second time in `with.version`. Nothing in the build
// keeps the two in step, so a Dependabot bump of the go.mod tool directive moves local
// lint while CI stays pinned — reintroducing the local/CI skew that #90 and #93 closed,
// only inverted. Skew is expensive: findings appear in CI that never reproduce locally
// (or worse, the reverse).
//
// Until the workflow derives the version from go.mod, this test is what holds them equal.
func TestGolangciLintVersionMatchesCI(t *testing.T) {
	goMod, err := os.ReadFile(goModPath)
	if err != nil {
		t.Fatalf("reading %s: %v", goModPath, err)
	}
	workflow, err := os.ReadFile(lintWorkflowPath)
	if err != nil {
		t.Fatalf("reading %s: %v", lintWorkflowPath, err)
	}

	pinned, err := golangciVersionFromGoMod(string(goMod))
	if err != nil {
		t.Fatalf("%s: %v", goModPath, err)
	}
	ci, err := golangciVersionFromWorkflow(workflow)
	if err != nil {
		t.Fatalf("%s: %v", lintWorkflowPath, err)
	}

	if pinned != ci {
		t.Errorf("golangci-lint version skew: %s pins %s (what `make lint` runs), "+
			"%s pins %s (what CI runs).\n"+
			"Local and CI must run the same linter or findings differ between them. "+
			"Update the `version:` in the workflow to %s, or bump the go.mod tool pin to %s.",
			goModPath, pinned, lintWorkflowPath, ci, pinned, ci)
	}
}

// golangciVersionFromGoMod extracts the golangci-lint version from a go.mod's require
// block. The Makefile derives GOLANGCI_LINT_VERSION from the same line, so this is the
// version a local `make lint` installs and runs.
func golangciVersionFromGoMod(src string) (string, error) {
	scanner := bufio.NewScanner(strings.NewReader(src))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		// Inside a require block a line is "<module> <version>"; the single-line form is
		// "require <module> <version>", so drop a leading require keyword.
		if len(fields) > 0 && fields[0] == "require" {
			fields = fields[1:]
		}
		// The tool directive naming the same module's command package is a single field,
		// and is skipped by the length check.
		if len(fields) < 2 || fields[0] != golangciModule {
			continue
		}
		version := fields[1]
		if !semverPattern.MatchString(version) {
			return "", fmt.Errorf("%s has version %q, which is not a semver string", golangciModule, version)
		}
		return version, nil
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scanning go.mod: %w", err)
	}
	return "", fmt.Errorf("no require line found for %s", golangciModule)
}

// golangciVersionFromWorkflow extracts the version passed to the golangci-lint action in
// a GitHub Actions workflow. It walks the parsed structure rather than pattern-matching
// the text, so an unrelated `version:` key elsewhere in the file cannot be mistaken for
// this one.
func golangciVersionFromWorkflow(src []byte) (string, error) {
	var wf struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string         `json:"uses"`
				With map[string]any `json:"with"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	if err := yaml.Unmarshal(src, &wf); err != nil {
		return "", fmt.Errorf("parsing workflow: %w", err)
	}

	for _, job := range wf.Jobs {
		for _, step := range job.Steps {
			if !strings.HasPrefix(step.Uses, golangciAction) {
				continue
			}
			raw, ok := step.With["version"]
			if !ok {
				return "", fmt.Errorf("step %q sets no with.version", step.Uses)
			}
			version := fmt.Sprint(raw)
			if !semverPattern.MatchString(version) {
				return "", fmt.Errorf("step %q has version %q, which is not a semver string", step.Uses, version)
			}
			return version, nil
		}
	}
	return "", fmt.Errorf("no step using %s* found", golangciAction)
}

// TestGolangciVersionFromGoMod pins the go.mod parser. Its failure modes matter as much as
// its success: a parser that quietly returns "" would make the drift guard pass on a
// malformed file, which is exactly the false green a guard must not have.
func TestGolangciVersionFromGoMod(t *testing.T) {
	tests := []struct {
		name    string
		src     string
		want    string
		wantErr bool
	}{
		{
			name: "indirect require line",
			src:  "require (\n\tgithub.com/golangci/golangci-lint/v2 v2.12.2 // indirect\n)\n",
			want: "v2.12.2",
		},
		{
			name: "direct require line",
			src:  "require (\n\tgithub.com/golangci/golangci-lint/v2 v2.8.1\n)\n",
			want: "v2.8.1",
		},
		{
			name: "single-line require",
			src:  "require github.com/golangci/golangci-lint/v2 v2.7.2\n",
			want: "v2.7.2",
		},
		{
			name: "tool directive alone is not a version source",
			src:  "tool (\n\tgithub.com/golangci/golangci-lint/v2/cmd/golangci-lint\n)\n",
			// The tool directive names the command package, carries no version, and must
			// not be mistaken for the require line.
			wantErr: true,
		},
		{
			name:    "module absent",
			src:     "require (\n\tgithub.com/onsi/gomega v1.42.1\n)\n",
			wantErr: true,
		},
		{
			name:    "non-semver version",
			src:     "require github.com/golangci/golangci-lint/v2 latest\n",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := golangciVersionFromGoMod(tt.src)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("golangciVersionFromGoMod() = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("golangciVersionFromGoMod() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("golangciVersionFromGoMod() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestGolangciVersionFromWorkflow pins the workflow parser, including the false-green
// cases: a missing step, a missing version key, and a stray `version:` on another action.
func TestGolangciVersionFromWorkflow(t *testing.T) {
	tests := []struct {
		name    string
		src     string
		want    string
		wantErr bool
	}{
		{
			name: "version on the golangci step",
			src: `jobs:
  lint:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version-file: go.mod
      - uses: golangci/golangci-lint-action@v9
        with:
          version: v2.11.5
`,
			want: "v2.11.5",
		},
		{
			name: "other actions' version keys are ignored",
			src: `jobs:
  other:
    steps:
      - uses: some/other-action@v1
        with:
          version: v9.9.9
  lint:
    steps:
      - uses: golangci/golangci-lint-action@v9
        with:
          version: v2.9.3
`,
			want: "v2.9.3",
		},
		{
			name: "extra non-string with keys do not break parsing",
			src: `jobs:
  lint:
    steps:
      - uses: golangci/golangci-lint-action@v9
        with:
          only-new-issues: true
          version: v2.10.4
`,
			want: "v2.10.4",
		},
		{
			name: "step present but version unset",
			src: `jobs:
  lint:
    steps:
      - uses: golangci/golangci-lint-action@v9
`,
			wantErr: true,
		},
		{
			name: "no golangci step at all",
			src: `jobs:
  lint:
    steps:
      - uses: actions/checkout@v7
`,
			wantErr: true,
		},
		{
			name:    "malformed yaml",
			src:     "jobs: [unclosed\n",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := golangciVersionFromWorkflow([]byte(tt.src))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("golangciVersionFromWorkflow() = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("golangciVersionFromWorkflow() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("golangciVersionFromWorkflow() = %q, want %q", got, tt.want)
			}
		})
	}
}
