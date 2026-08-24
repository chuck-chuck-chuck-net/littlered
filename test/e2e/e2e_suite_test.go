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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/littlered-operator/littlered-operator/test/utils"
)

var (
	// operatorNamespace is where the operator is deployed.
	operatorNamespace = "littlered-system"

	// testNamespace is the namespace used for all e2e test resources.
	// Override with TEST_NAMESPACE env var. Must not be "default".
	testNamespace = "littlered-e2e"

	// skipOperatorDeploy skips operator deployment (use existing deployment).
	skipOperatorDeploy = false

	// skipKindSetup skips kind setup and assumes non-kind environment for images.
	skipKindSetup = false

	// debugOnFailure skips cleanup on failure.
	debugOnFailure = false

	// suiteFailed tracks if any test in the suite failed.
	suiteFailed = false

	// clusterShards is the number of shards to use in cluster mode e2e tests.
	// Override with CLUSTER_SHARDS env var. Must be >= 3.
	clusterShards = 3

	// clusterReplicasPerShard is the number of replicas per shard for cluster tests that use replicas.
	// Override with CLUSTER_REPLICAS_PER_SHARD env var.
	clusterReplicasPerShard = 1
)

func suiteOrSpecFailed() bool {
	failed := suiteFailed || CurrentSpecReport().Failed() || GinkgoT().Failed()
	if failed {
		// Using fmt.Fprintf instead of By because we might be in an AfterSuite where By is not allowed
		_, _ = fmt.Fprintf(GinkgoWriter, "Failure detected (suiteFailed: %v, SpecReport.Failed: %v, GinkgoT.Failed: %v)\n",
			suiteFailed, CurrentSpecReport().Failed(), GinkgoT().Failed())
	}
	return failed
}

// TestE2E runs the e2e test suite.
//
// Environment variables:
//   - SKIP_OPERATOR_DEPLOY: Set to "true" to skip operator deployment
//   - SKIP_KIND_SETUP: Set to "true" to skip cluster creation and assume non-Kind (registry-push) deployment
//   - DEBUG_ON_FAILURE: Set to "true" to skip cleanup on failure
//   - TEST_NAMESPACE: Namespace for test resources (default: "littlered-e2e"); must not be "default"
//   - KUBECONTEXT_PINNING: Set to "true" to snapshot the current kubeconfig (prevents accidental context switches)
//   - KUBECONTEXT: Pin to a specific named context (implies pinning)
//   - KIND_CLUSTER: Kind cluster name (default: kind)
//   - CLUSTER_SHARDS: Number of shards for cluster mode tests (default: 3, minimum: 3)
//   - CLUSTER_REPLICAS_PER_SHARD: Replicas per shard for cluster tests that use replicas (default: 1)
func TestE2E(t *testing.T) {
	RegisterFailHandler(func(message string, callerSkip ...int) {
		_, _ = fmt.Fprintf(GinkgoWriter, "Fail handler called: %s\n", message)
		suiteFailed = true
		Fail(message, callerSkip...)
	})
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting LittleRed operator e2e test suite\n")
	RunSpecs(t, "LittleRed E2E Suite")
}

var _ = BeforeSuite(func() {
	// Initialize streaming log tmp directory (suite-wide)
	initE2ETmpDir()

	// Assert bin/lrctl exists, ONCE, up front, rather than at each use. lrctl
	// is the project's designated ground-truth CLI (CLAUDE.md §7 rule 8) and
	// this suite must always invoke its own freshly-built ./bin/lrctl,
	// rooted at the repo, never a PATH-installed copy (e.g. a stale
	// `make install-lrctl` at /usr/local/bin/lrctl). Freshness is `make`'s
	// job — `run-test-e2e` depends on the real `bin/lrctl` file target — so
	// by the time we get here the binary should already be built and
	// current. Failing loudly here, instead of opportunistically at each
	// call site, is what lets every downstream use (specs and the failure-
	// path debug collector) skip existence/fallback handling entirely.
	By("verifying bin/lrctl was built by make")
	{
		lrctlBin, err := lrctlBinPath()
		Expect(err).NotTo(HaveOccurred(), "failed to determine bin/lrctl path")
		_, statErr := os.Stat(lrctlBin)
		Expect(statErr).NotTo(HaveOccurred(),
			"bin/lrctl not found at %s — build it before running e2e:\n\n    make lrctl\n\n"+
				"(the test-e2e/run-test-e2e make targets do this automatically)", lrctlBin)
	}

	// Pin kubeconfig so that switching contexts in another terminal during a
	// long-running test suite doesn't break anything.
	//   KUBECONTEXT_PINNING=true             — pin the current active context
	//   KUBECONTEXT=<name>                   — pin a specific context
	//   Both set                             — KUBECONTEXT wins
	kubeCtx := os.Getenv("KUBECONTEXT")
	if kubeCtx != "" || os.Getenv("KUBECONTEXT_PINNING") == "true" {
		pinKubeContext(kubeCtx)
	}

	// Load configuration from environment
	if ns := os.Getenv("TEST_NAMESPACE"); ns != "" {
		testNamespace = ns
	}
	if os.Getenv("SKIP_OPERATOR_DEPLOY") == "true" {
		skipOperatorDeploy = true
	}
	if os.Getenv("SKIP_KIND_SETUP") == "true" {
		skipKindSetup = true
	}
	if os.Getenv("DEBUG_ON_FAILURE") == "true" {
		debugOnFailure = true
	}
	if v := os.Getenv("CLUSTER_SHARDS"); v != "" {
		n, err := strconv.Atoi(v)
		Expect(err).NotTo(HaveOccurred(), "CLUSTER_SHARDS must be an integer")
		Expect(n).To(BeNumerically(">=", 3), "CLUSTER_SHARDS must be >= 3")
		clusterShards = n
	}
	if v := os.Getenv("CLUSTER_REPLICAS_PER_SHARD"); v != "" {
		n, err := strconv.Atoi(v)
		Expect(err).NotTo(HaveOccurred(), "CLUSTER_REPLICAS_PER_SHARD must be an integer")
		Expect(n).To(BeNumerically(">=", 0), "CLUSTER_REPLICAS_PER_SHARD must be >= 0")
		clusterReplicasPerShard = n
	}

	_, _ = fmt.Fprintf(GinkgoWriter, "Test namespace: %s\n", testNamespace)
	_, _ = fmt.Fprintf(GinkgoWriter, "Skip operator deploy: %v\n", skipOperatorDeploy)
	_, _ = fmt.Fprintf(GinkgoWriter, "Skip Kind setup: %v\n", skipKindSetup)
	_, _ = fmt.Fprintf(GinkgoWriter, "Debug on failure: %v\n", debugOnFailure)
	_, _ = fmt.Fprintf(GinkgoWriter, "Cluster shards: %d\n", clusterShards)
	_, _ = fmt.Fprintf(GinkgoWriter, "Cluster replicas per shard: %d\n", clusterReplicasPerShard)

	// Require a dedicated namespace — "default" is not safe for e2e tests.
	Expect(testNamespace).NotTo(Equal("default"),
		"TEST_NAMESPACE must not be 'default'; use a dedicated namespace such as 'littlered-e2e'")

	// Require the namespace to not exist yet. Stale resources from a previous run
	// (Pods, LittleRed CRs, etc.) would corrupt the tests, so we refuse to start
	// rather than silently inheriting leftover state.
	By("verifying test namespace " + testNamespace + " does not exist")
	cmd := exec.Command("kubectl", "get", "ns", testNamespace)
	_, err := utils.Run(cmd)
	Expect(err).To(HaveOccurred(),
		"Test namespace %q already exists — delete it first to ensure a clean slate:\n  kubectl delete ns %s",
		testNamespace, testNamespace)

	By("creating test namespace " + testNamespace)
	cmd = exec.Command("kubectl", "create", "ns", testNamespace)
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to create test namespace %q", testNamespace)

	// The kill-9 chaos tests launch a helper pod with hostPID:true and a
	// privileged container to SIGKILL the in-container PID 1 from the host PID
	// namespace. That is forbidden under the "baseline" and "restricted"
	// Pod Security Standards. Kind (the default e2e target) enforces nothing,
	// but hardened/real clusters often set a cluster-wide default of
	// enforce=baseline that every unlabeled namespace inherits — which rejects
	// the helper pod. Pin this test-only namespace to the "privileged" level so
	// the helper is admitted regardless of the cluster default.
	By("labeling test namespace " + testNamespace + " as pod-security privileged")
	cmd = exec.Command("kubectl", "label", "ns", testNamespace,
		"pod-security.kubernetes.io/enforce=privileged",
		"pod-security.kubernetes.io/warn=privileged",
		"pod-security.kubernetes.io/audit=privileged",
		"--overwrite")
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to set pod-security level on namespace %q", testNamespace)

	if skipOperatorDeploy {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping operator deployment (SKIP_OPERATOR_DEPLOY=true)\n")
	} else {
		deployOperator()
	}

	// Verify all required images are pullable before running any tests.
	// This catches "forgot to push the chaos-client image" early instead of
	// 40 minutes into the suite.
	preflightImageChecks()
})

var _ = BeforeEach(func() {
	// Track when each test starts for log filtering
	testStartTime = time.Now()
	// Clear any pre-delete logs captured by a previous test
	resetPreDeleteLogs()
	// Stop any streaming log processes from the previous test
	resetStreamingLogs()
})

var _ = AfterEach(func() {
	if CurrentSpecReport().Failed() {
		suiteFailed = true

		// Collect debug artifacts if enabled
		if debugOnFailure {
			// Try to extract namespace and CR name from the spec labels
			// This is best-effort - if we can't find them, we'll just collect what we can
			namespace, crName, chaosPod := extractTestContext()
			CollectDebugArtifacts(namespace, crName, chaosPod)
		}
	}
})

// extractTestContext extracts CR name and chaos pod from spec labels.
// Namespace is always the global testNamespace.
func extractTestContext() (namespace, crName, chaosPod string) {
	namespace = testNamespace

	labels := CurrentSpecReport().Labels()
	for _, label := range labels {
		if strings.HasPrefix(label, "cr:") {
			crName = strings.TrimPrefix(label, "cr:")
		} else if strings.HasPrefix(label, "chaos:") {
			chaosPod = strings.TrimPrefix(label, "chaos:")
		}
	}

	// Also check report entries for dynamic names
	for _, entry := range CurrentSpecReport().ReportEntries {
		name := entry.Name
		if strings.HasPrefix(name, "cr:") {
			crName = strings.TrimPrefix(name, "cr:")
		} else if strings.HasPrefix(name, "chaos:") {
			chaosPod = strings.TrimPrefix(name, "chaos:")
		}
	}

	return namespace, crName, chaosPod
}

var _ = AfterSuite(func() {
	// Always clean up the streaming log tmp dir (even on failure — artifacts were
	// already copied to the debug-artifacts dir by CollectDebugArtifacts).
	cleanupE2ETmpDir()

	if debugOnFailure && suiteOrSpecFailed() {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping cleanup due to failure and DEBUG_ON_FAILURE=true\n")
		return
	}

	By("cleaning up test namespace " + testNamespace)
	cmd := exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found", "--timeout=5m")
	_, _ = utils.Run(cmd)

	if skipOperatorDeploy {
		return
	}

	undeployOperator()
})

func deployOperator() {
	By("building the operator images")
	cmd := exec.Command("make", "build-images")
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the operator images")

	if !skipKindSetup {
		By("loading images into Kind")
		cmd = exec.Command("make", "kind-load")
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load images into Kind")

		By("deploying the operator via Helm (Kind mode)")
		// We use PULL_POLICY=Never to ensure Kind uses the local image we just loaded
		cmd = exec.Command("make", "deploy", "PULL_POLICY=Never")
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy the operator via Helm")
	} else {
		By("pushing images to registry")
		cmd = exec.Command("make", "push-images")
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to push images to registry")

		By("deploying the operator via Helm (Registry mode)")
		// Use default PULL_POLICY (Always) from Makefile for generic clusters
		cmd = exec.Command("make", "deploy")
		_, err = utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy the operator via Helm")
	}

	By("waiting for operator to be ready")
	waitForOperator()
}

func undeployOperator() {
	By("undeploying the operator")
	cmd := exec.Command("make", "undeploy")
	_, _ = utils.Run(cmd)
}

func waitForOperator() {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "deployment", "-n", operatorNamespace,
			"-l", "control-plane=controller-manager",
			"-o", "jsonpath={.items[0].status.availableReplicas}")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(Equal("1"))
	}, "2m", "5s").Should(Succeed())
}

// pinKubeContext snapshots the effective kubeconfig into a temp file and points
// KUBECONFIG at it. This isolates the test process from context switches in
// other terminals.
//
//   - contextName == "": pin the current active context
//   - contextName != "": pin a specific named context
//
// Uses "kubectl config view --raw --flatten --minify" which produces a
// self-contained kubeconfig with only the selected context/cluster/user.
func pinKubeContext(contextName string) {
	args := []string{"config", "view", "--raw", "--flatten", "--minify"}
	label := "current"
	if contextName != "" {
		args = append(args, "--context", contextName)
		label = contextName
	}

	By(fmt.Sprintf("pinning kubeconfig (context: %s)", label))

	cmd := exec.Command("kubectl", args...)
	raw, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(),
		"Failed to snapshot kubeconfig for context %q — does the context exist?", label)

	tmpDir, err := os.MkdirTemp("", "littlered-e2e-kubeconfig-*")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	tmpFile := filepath.Join(tmpDir, "config")
	ExpectWithOffset(1, os.WriteFile(tmpFile, []byte(raw), 0o600)).To(Succeed())

	os.Setenv("KUBECONFIG", tmpFile) //nolint:errcheck
	_, _ = fmt.Fprintf(GinkgoWriter, "Pinned KUBECONFIG to %s (context: %s)\n", tmpFile, label)
}
