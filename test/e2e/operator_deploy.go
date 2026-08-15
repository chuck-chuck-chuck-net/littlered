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

// This file deliberately carries NO `e2e` build tag. It holds the pure
// decisions of the suite's operator-deployment step — which is exactly the part
// that must be unit-testable without a cluster. The Ginkgo specs that consume
// it (e2e_suite_test.go) stay behind the tag.

package e2e

import (
	"fmt"
	"strings"
)

// defaultOperatorNamespace is where the operator is deployed unless a run says
// otherwise. It is the historical hardcoded value.
const defaultOperatorNamespace = "littlered-system"

// The environment this decision reads. Named as constants because the driver,
// the Makefile and these tests all have to spell them identically.
const (
	envSkipOperatorDeploy = "SKIP_OPERATOR_DEPLOY"
	envSkipKindSetup      = "SKIP_KIND_SETUP"
	envDeployMode         = "E2E_OPERATOR_DEPLOY_MODE"
	envOperatorNamespace  = "E2E_OPERATOR_NAMESPACE"
	envOperatorScoped     = "E2E_OPERATOR_SCOPED"
	envWatchNamespaces    = "E2E_WATCH_NAMESPACES"
	envOperatorImage      = "OPERATOR_IMAGE"

	envTrue = "true"
)

// Deployment modes (E2E_OPERATOR_DEPLOY_MODE).
const (
	// deployModeBuild builds the operator images from the working tree, loads
	// or pushes them, and helm-installs the result. The historical behaviour and
	// still the default.
	deployModeBuild = "build"
	// deployModePinned helm-installs an ALREADY-BUILT, ALREADY-PUSHED image ref
	// and builds nothing. This is the mode the in-cluster harness needs: an array
	// pins one image for every work item (design §1), so rebuilding per item is
	// both waste and a contradiction of the pinning.
	deployModePinned = "pinned"
)

// operatorDeployPlan is what BeforeSuite decided to do about the operator.
// Everything the shell needs is derived here rather than in the Ginkgo body, so
// the decisions are testable without a cluster.
type operatorDeployPlan struct {
	// Skip means: assume an operator is already running, deploy nothing.
	Skip bool
	// Build/KindLoad/Push describe the image work. All false in pinned mode.
	Build    bool
	KindLoad bool
	Push     bool
	// Namespace is where the operator is deployed (helm release namespace).
	Namespace string
	// WatchNamespaces scopes the operator (ADR-014). Empty means cluster-scoped.
	WatchNamespaces []string
	// Image, when set, pins the operator image ref explicitly. Required in
	// pinned mode: the Makefile would otherwise derive the tag from git, which
	// does not exist inside the driver image.
	Image string
	// PullPolicy overrides the chart's image pull policy ("Never" for Kind,
	// where the image was side-loaded and cannot be pulled at all).
	PullPolicy string
	// SkipCodegen drops the manifests/generate prerequisite of `make deploy`,
	// leaving only the plain `kubectl apply` of the committed CRDs. A pinned run
	// regenerates nothing that it then uses, and running codegen would need a
	// controller-gen download inside the driver pod.
	SkipCodegen bool
}

// resolveOperatorDeploy turns the run's environment into the deployment plan.
//
// The three modes are deliberately distinct: "skip" (an operator already
// exists), "build" (the historical develop-and-test loop) and "pinned" (deploy
// a pre-built ref). Before M2 there were only the first two, so an in-cluster
// array had to choose between rebuilding the operator on every work item and
// sharing one cluster-scoped operator across all of them (design §11 M2.2).
func resolveOperatorDeploy(getenv func(string) string, testNamespace string) (operatorDeployPlan, error) {
	p := operatorDeployPlan{
		Namespace: defaultOperatorNamespace,
	}
	if ns := strings.TrimSpace(getenv(envOperatorNamespace)); ns != "" {
		p.Namespace = ns
	}

	// Watch scope. The explicit list wins; the shorthand means "watch exactly my
	// own test namespace", which is what a per-item operator wants.
	if raw := strings.TrimSpace(getenv(envWatchNamespaces)); raw != "" {
		for ns := range strings.SplitSeq(raw, ",") {
			if ns = strings.TrimSpace(ns); ns != "" {
				p.WatchNamespaces = append(p.WatchNamespaces, ns)
			}
		}
	} else if getenv(envOperatorScoped) == envTrue {
		p.WatchNamespaces = []string{testNamespace}
	}

	// An explicit skip wins over any mode: it is the caller asserting that an
	// operator is already there, which no deployment plan can improve on.
	if getenv(envSkipOperatorDeploy) == envTrue {
		p.Skip = true
		return p, nil
	}

	switch mode := strings.TrimSpace(getenv(envDeployMode)); mode {
	case "", deployModeBuild:
		p.Build = true
		if getenv(envSkipKindSetup) == envTrue {
			p.Push = true
		} else {
			p.KindLoad = true
			// Kind side-loads the image; it exists nowhere to pull from.
			p.PullPolicy = "Never"
		}
	case deployModePinned:
		p.SkipCodegen = true
		p.Image = strings.TrimSpace(getenv(envOperatorImage))
		if p.Image == "" {
			return operatorDeployPlan{}, fmt.Errorf(
				envDeployMode+"=%s needs "+envOperatorImage+" to name the pre-built, pre-pushed "+
					"operator image (nothing is built in this mode)", deployModePinned)
		}
	default:
		return operatorDeployPlan{}, fmt.Errorf(
			"E2E_OPERATOR_DEPLOY_MODE=%q is not a mode; use %q or %q (or SKIP_OPERATOR_DEPLOY=true)",
			mode, deployModeBuild, deployModePinned)
	}
	return p, nil
}

// deployArgs renders the plan as the argument list for `make`, starting with the
// target. Namespace and watch scope are always passed explicitly, so the
// Makefile's defaults are never silently in play during a test run.
func (p operatorDeployPlan) deployArgs() []string {
	args := []string{"deploy", "DEPLOY_NAMESPACE=" + p.Namespace}
	if len(p.WatchNamespaces) > 0 {
		args = append(args, "WATCH_NAMESPACES="+strings.Join(p.WatchNamespaces, ","))
	}
	if p.SkipCodegen {
		args = append(args, "SKIP_CODEGEN=true")
	}
	if p.Image != "" {
		args = append(args, "IMG="+p.Image)
	}
	if p.PullPolicy != "" {
		args = append(args, "PULL_POLICY="+p.PullPolicy)
	}
	return args
}

// undeployArgs renders the teardown call. It must name the same namespace the
// release was installed into, or it silently uninstalls nothing.
func (p operatorDeployPlan) undeployArgs() []string {
	return []string{"undeploy", "DEPLOY_NAMESPACE=" + p.Namespace}
}
