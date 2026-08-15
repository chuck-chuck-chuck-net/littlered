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

// Untagged on purpose (see operator_deploy.go): these are plain unit tests of
// the suite's deployment decisions and need no cluster. `make test` runs them.

package e2e

import (
	"slices"
	"strings"
	"testing"
)

// Shared fixtures for the tables below.
const (
	pinnedImage = "reg.lan/littlered:abc123"
	itemOpNS    = "lr-harness-op-a-0-x"
	itemTestNS  = "lr-harness-a-0-x"
)

func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

// The three modes. deployModePinned is the one M2 adds, and the reason it must exist is
// in the last case: before it, an in-cluster array's only alternative to
// rebuilding the operator per work item was to share one cluster-scoped
// operator — which is exactly what parking a failed item forbids (design §3).
func TestResolveOperatorDeployModes(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want operatorDeployPlan
	}{
		{
			name: "default is the historical build-and-kind-load loop",
			env:  nil,
			want: operatorDeployPlan{
				Build: true, KindLoad: true,
				Namespace: defaultOperatorNamespace, PullPolicy: "Never",
			},
		},
		{
			name: "a non-Kind cluster pushes instead of side-loading",
			env:  map[string]string{envSkipKindSetup: envTrue},
			want: operatorDeployPlan{
				Build: true, Push: true,
				Namespace: defaultOperatorNamespace,
			},
		},
		{
			name: "SKIP_OPERATOR_DEPLOY still means deploy nothing at all",
			env:  map[string]string{envSkipOperatorDeploy: envTrue},
			want: operatorDeployPlan{Skip: true, Namespace: defaultOperatorNamespace},
		},
		{
			name: "an explicit skip beats a mode",
			env: map[string]string{
				envSkipOperatorDeploy:       envTrue,
				envDeployMode:               deployModePinned,
				envOperatorImage:            pinnedImage,
				envOperatorNamespace:        itemOpNS,
				envOperatorScoped:           envTrue,
				"E2E_OPERATOR_UNRELATED_NO": "1",
			},
			want: operatorDeployPlan{
				Skip: true, Namespace: itemOpNS,
				WatchNamespaces: []string{itemTestNS},
			},
		},
		{
			name: "pinned builds nothing and deploys the given ref",
			env: map[string]string{
				envDeployMode:        deployModePinned,
				envOperatorImage:     pinnedImage,
				envSkipKindSetup:     envTrue,
				envOperatorNamespace: itemOpNS,
				envOperatorScoped:    envTrue,
			},
			want: operatorDeployPlan{
				Namespace:       itemOpNS,
				WatchNamespaces: []string{itemTestNS},
				Image:           pinnedImage,
				SkipCodegen:     true,
			},
		},
		{
			name: "an explicit watch list wins over the scoped shorthand",
			env: map[string]string{
				envDeployMode:      deployModePinned,
				envOperatorImage:   pinnedImage,
				envOperatorScoped:  envTrue,
				envWatchNamespaces: "ns-a, ns-b",
			},
			want: operatorDeployPlan{
				Namespace:       "littlered-system",
				WatchNamespaces: []string{"ns-a", "ns-b"},
				Image:           pinnedImage,
				SkipCodegen:     true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveOperatorDeploy(env(tc.env), itemTestNS)
			if err != nil {
				t.Fatalf("resolveOperatorDeploy: unexpected error %v", err)
			}
			if got.Skip != tc.want.Skip || got.Build != tc.want.Build ||
				got.KindLoad != tc.want.KindLoad || got.Push != tc.want.Push ||
				got.Namespace != tc.want.Namespace || got.Image != tc.want.Image ||
				got.PullPolicy != tc.want.PullPolicy || got.SkipCodegen != tc.want.SkipCodegen ||
				!slices.Equal(got.WatchNamespaces, tc.want.WatchNamespaces) {
				t.Errorf("resolveOperatorDeploy()\n got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

// A pinned deploy with nothing to deploy is a configuration error that must
// surface at BeforeSuite, not as an operator Deployment pulling ":".
func TestResolveOperatorDeployRejectsIncoherentPinning(t *testing.T) {
	if _, err := resolveOperatorDeploy(env(map[string]string{
		envDeployMode: deployModePinned,
	}), "ns"); err == nil {
		t.Error("pinned mode without OPERATOR_IMAGE must fail: nothing is built, so the ref must be given")
	}
	if _, err := resolveOperatorDeploy(env(map[string]string{
		envDeployMode: "rebuild-everything",
	}), "ns"); err == nil {
		t.Error("an unknown mode must fail loudly rather than silently falling back to build")
	}
}

// The plan crosses into `make`. A namespace that is decided but not passed is
// the M1 gap this milestone closes, so assert the argument list itself.
func TestDeployArgsCarryNamespaceScopeAndImage(t *testing.T) {
	p, err := resolveOperatorDeploy(env(map[string]string{
		envDeployMode:        deployModePinned,
		envOperatorImage:     pinnedImage,
		envOperatorNamespace: itemOpNS,
		envOperatorScoped:    envTrue,
	}), itemTestNS)
	if err != nil {
		t.Fatalf("resolveOperatorDeploy: %v", err)
	}

	args := p.deployArgs()
	if args[0] != "deploy" {
		t.Errorf("deployArgs()[0] = %q, want the make target %q", args[0], "deploy")
	}
	for _, want := range []string{
		"DEPLOY_NAMESPACE=" + itemOpNS,
		"WATCH_NAMESPACES=" + itemTestNS,
		"SKIP_CODEGEN=true",
		"IMG=" + pinnedImage,
	} {
		if !slices.Contains(args, want) {
			t.Errorf("deployArgs() = %v, missing %q", args, want)
		}
	}
	if slices.ContainsFunc(args, func(a string) bool { return strings.HasPrefix(a, "PULL_POLICY=") }) {
		t.Errorf("deployArgs() = %v, must not pin a pull policy outside Kind", args)
	}

	// Teardown must name the same namespace, or it uninstalls nothing at all.
	wantUndeploy := []string{"undeploy", "DEPLOY_NAMESPACE=" + itemOpNS}
	if got := p.undeployArgs(); !slices.Equal(got, wantUndeploy) {
		t.Errorf("undeployArgs() = %v, want %v", got, wantUndeploy)
	}
}

// A cluster-scoped run must NOT pass an empty watch list: `WATCH_NAMESPACES=`
// renders `--set scope.watchNamespaces={}`, which the chart reads as allow-list
// mode with no namespaces — an operator that watches nothing.
func TestDeployArgsOmitEmptyWatchScope(t *testing.T) {
	p, err := resolveOperatorDeploy(env(nil), itemTestNS)
	if err != nil {
		t.Fatalf("resolveOperatorDeploy: %v", err)
	}
	for _, a := range p.deployArgs() {
		if strings.HasPrefix(a, "WATCH_NAMESPACES=") {
			t.Errorf("deployArgs() = %v, must omit the watch scope when the operator is cluster-scoped", p.deployArgs())
		}
	}
}
