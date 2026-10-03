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
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/chuck-chuck-chuck-net/littlered/internal/watchscope"
	"github.com/chuck-chuck-chuck-net/littlered/test/utils"
)

// Operator redundancy (#114).
//
// The operator's HA story rests on three claims that nothing exercised until now: with
// several replicas exactly one holds the lease and the others do nothing (no reconcile,
// no per-instance watcher); when the leader dies the lease moves to a standby inside the
// lease window and the new leader picks up from live state; and the pod that replaces the
// dead leader is itself an idle standby. This tier runs the operator at two replicas
// against a live sentinel instance and asserts each of the three, recording the measured
// handover time in the report.
//
// Leader election is always on (cmd/littlered/main.go), with controller-runtime's default
// lease timings: LeaseDuration 15s, RenewDeadline 10s, RetryPeriod 2s, and release-on-cancel
// OFF — so a deleted leader's lease must expire before a standby can take it, and the
// expected handover is on the order of 15-30s. The bound below is deliberately generous;
// the measured value is what the report entry is for.
const (
	// operatorLeaseName is the unscoped operator's lease. The e2e suite deploys the
	// operator unscoped, so this is the lease it holds (ADR-014 derives scoped IDs).
	operatorLeaseName = watchscope.BaseLeaderElectionID

	operatorPodSelector = "control-plane=controller-manager"

	// leaseHandoverBound: LeaseDuration + RenewDeadline + scheduling slack.
	leaseHandoverBound = 90 * time.Second

	// The probe edits a Service-only field, so observing it costs no pod roll.
	redundancyProbePath = `{"spec":{"service":{"annotations":{"e2e.redis.chuck-chuck-chuck.net/redundancy-probe":"%d"}}}}`
)

var _ = Describe("Operator Redundancy", Label("sentinel", "operator-ha"), Ordered, func() {
	var (
		crName    string
		startedAt time.Time
	)

	deploy := func(name string) {
		AddReportEntry("cr:" + name)
		registerE2EAuth(name)
		cr := e2eAuthSecretDoc(name) + fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
%s  sentinel:
    masterName: %s
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, name, testNamespace, e2eAuthSpecYAML(name), e2eMasterName(testNamespace, name))
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(cr)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the instance to be Running")
		Eventually(func(g Gomega) {
			g.Expect(getPhase(name)).To(Equal("Running"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
		verifySentinelTopologySync(testNamespace, name, 3, 2)
	}

	BeforeAll(func() {
		startedAt = time.Now()
		crName = fmt.Sprintf("operator-ha-%d", time.Now().Unix())
		deploy(crName)

		By("scaling the operator to two replicas")
		scaleOperator(2)
	})

	AfterAll(func() {
		// Restore the suite's single-replica baseline FIRST, and wait until the surviving
		// pod actually holds the lease: scaling down may remove the leader, and a later tier
		// starting inside the resulting handover window would see an operator that is not
		// reconciling for reasons that have nothing to do with that tier.
		By("restoring a single operator replica that holds the lease")
		scaleOperator(1)
		Eventually(func(g Gomega) {
			pods := operatorPods()
			g.Expect(pods).To(HaveLen(1))
			g.Expect(leaseHolderPod()).To(Equal(pods[0]))
		}, leaseHandoverBound, 2*time.Second).Should(Succeed(), "surviving operator pod never took the lease")

		if debugOnFailure && suiteOrSpecFailed() {
			By("skipping CR cleanup to allow debugging")
			return
		}
		_, _ = utils.Run(exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found"))
	})

	It("runs two replicas with exactly one lease holder, and the standby does nothing", func() {
		pods := operatorPods()
		Expect(pods).To(HaveLen(2), "expected two operator pods after scaling")

		leader := leaseHolderPod()
		Expect(pods).To(ContainElement(leader), "lease holder %q is not one of the operator pods", leader)
		standby := otherOperatorPod(pods, leader)
		AddReportEntry("operator-ha:leader", leader)
		AddReportEntry("operator-ha:standby", standby)

		By("the lease holder stays put while both replicas are healthy")
		Consistently(leaseHolderPod, 20*time.Second, 2*time.Second).Should(Equal(leader))

		By("the leader still reconciles: a spec change is observed")
		expectOperatorObserves(crName)

		By("the standby joined the election but neither reconciles nor watches the instance")
		standbyLogs := operatorPodLogs(standby, startedAt)
		// controller-runtime's own log line (logger "leaderelection"), via the manager's zap
		// routing; matched case-insensitively because client-go's klog variant differs in case.
		Expect(standbyLogs).To(MatchRegexp(`(?i)attempting to acquire leader lease`),
			"standby never attempted the lease — is leader election on?")
		Expect(standbyLogs).NotTo(ContainSubstring("Reconciling sentinel mode"), "standby reconciled")
		Expect(standbyLogs).NotTo(ContainSubstring("Starting Sentinel monitor"), "standby started a per-instance watcher")

		By("the leader does watch the instance")
		Expect(operatorPodLogs(leader, startedAt)).To(ContainSubstring("Starting Sentinel monitor"))
	})

	It("hands the lease to the standby when the leader pod is deleted, and the new leader heals the instance", func() {
		pods := operatorPods()
		Expect(pods).To(HaveLen(2))
		leader := leaseHolderPod()
		standby := otherOperatorPod(pods, leader)

		By("deleting the leader pod " + leader)
		t0 := time.Now()
		_, err := utils.Run(exec.Command("kubectl", "delete", "pod", leader, "-n", operatorNamespace, "--wait=false"))
		Expect(err).NotTo(HaveOccurred())

		// Any surviving replica may win the lease once it expires: the standby that was
		// already waiting, or the pod the Deployment creates to replace the dead leader,
		// which is usually up well inside the 15s lease window and races it. Both are
		// correct; what must hold is that a RUNNING pod other than the dead leader wins.
		By("the lease moves off the deleted leader to a running replica within the lease window")
		var newLeader string
		Eventually(func(g Gomega) {
			holder := leaseHolderPod()
			g.Expect(holder).NotTo(BeEmpty(), "lease has no holder")
			g.Expect(holder).NotTo(Equal(leader), "lease still names the deleted leader")
			g.Expect(operatorPods()).To(ContainElement(holder), "lease holder %q is not a running operator pod", holder)
			newLeader = holder
		}, leaseHandoverBound, time.Second).Should(Succeed())
		handover := time.Since(t0).Round(100 * time.Millisecond)
		wonBy := "replacement"
		if newLeader == standby {
			wonBy = "standby"
		}
		AddReportEntry("operator-ha:handover", handover.String())
		AddReportEntry("operator-ha:new-leader", fmt.Sprintf("%s (%s)", newLeader, wonBy))
		_, _ = fmt.Fprintf(GinkgoWriter, "operator-ha: lease handover %s -> %s (%s) took %s\n", leader, newLeader, wonBy, handover)

		By("the deployment is back at two available replicas")
		Eventually(operatorAvailableReplicas, 2*time.Minute, 2*time.Second).Should(Equal(2))

		By("the new leader reconciles")
		expectOperatorObserves(crName)

		By("the new leader starts the instance watcher")
		Eventually(func() string { return operatorPodLogs(newLeader, t0) }, 60*time.Second, 2*time.Second).
			Should(ContainSubstring("Starting Sentinel monitor"))

		By("the new leader heals the instance: a replica pod is recycled and rejoins")
		victim := aSentinelModeReplicaPod(crName)
		_, err = deletePod(testNamespace, victim)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			g.Expect(getPhase(crName)).To(Equal("Running"))
		}, 3*time.Minute, 5*time.Second).Should(Succeed(), "instance did not return to Running under the new leader")
		verifySentinelTopologySync(testNamespace, crName, 3, 2)

		By("the pod that did not win stays idle")
		idle := otherOperatorPod(operatorPods(), newLeader)
		Consistently(func() string { return operatorPodLogs(idle, t0) }, 15*time.Second, 5*time.Second).
			ShouldNot(Or(ContainSubstring("Reconciling sentinel mode"), ContainSubstring("Starting Sentinel monitor")))
		Expect(leaseHolderPod()).To(Equal(newLeader), "lease moved again without a reason")
	})
})

// --- helpers -----------------------------------------------------------------

// operatorPods returns the names of the operator pods that are Running (a terminating
// pod keeps its name in the list until it is gone, so Running is what callers want).
func operatorPods() []string {
	out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", operatorNamespace,
		"-l", operatorPodSelector, "--field-selector=status.phase=Running",
		"-o", "jsonpath={.items[*].metadata.name}"))
	Expect(err).NotTo(HaveOccurred())
	return strings.Fields(out)
}

// leaseHolderPod returns the pod named by the operator lease's holderIdentity, which
// controller-runtime writes as "<pod>_<uuid>". Empty when the lease has no holder.
func leaseHolderPod() string {
	out, err := utils.Run(exec.Command("kubectl", "get", "lease", operatorLeaseName,
		"-n", operatorNamespace, "-o", "jsonpath={.spec.holderIdentity}"))
	if err != nil {
		return ""
	}
	holder := strings.TrimSpace(out)
	if i := strings.Index(holder, "_"); i > 0 {
		return holder[:i]
	}
	return holder
}

func otherOperatorPod(pods []string, not string) string {
	for _, p := range pods {
		if p != not {
			return p
		}
	}
	Fail(fmt.Sprintf("no operator pod other than %q among %v", not, pods))
	return ""
}

// operatorPodLogs returns one operator pod's manager-container log since the given time.
func operatorPodLogs(pod string, since time.Time) string {
	out, _ := utils.Run(exec.Command("kubectl", "logs", pod, "-n", operatorNamespace,
		"-c", "manager", "--tail=-1", "--since-time="+since.UTC().Format(time.RFC3339Nano)))
	return out
}

// expectOperatorObserves proves the operator is reconciling the CR: it bumps the CR's
// generation through a Service-only field and waits for status.observedGeneration to
// catch up. Annotations alone would not do — under the status subresource only spec
// changes bump metadata.generation.
func expectOperatorObserves(name string) {
	patch := fmt.Sprintf(redundancyProbePath, time.Now().UnixNano())
	_, err := utils.Run(exec.Command("kubectl", "patch", "littlered", name, "-n", testNamespace,
		"--type=merge", "-p", patch))
	Expect(err).NotTo(HaveOccurred())
	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("kubectl", "get", "littlered", name, "-n", testNamespace,
			"-o", "jsonpath={.metadata.generation} {.status.observedGeneration}"))
		g.Expect(err).NotTo(HaveOccurred())
		parts := strings.Fields(out)
		g.Expect(parts).To(HaveLen(2), "generation/observedGeneration not both present: %q", out)
		gen, _ := strconv.ParseInt(parts[0], 10, 64)
		obs, _ := strconv.ParseInt(parts[1], 10, 64)
		g.Expect(obs).To(BeNumerically(">=", gen), "observedGeneration %d has not caught up with generation %d", obs, gen)
	}, 90*time.Second, 2*time.Second).Should(Succeed(), "the operator did not observe a spec change")
}

// aSentinelModeReplicaPod returns a Redis data pod of the instance that is NOT the
// current master, so deleting it exercises healing without a failover.
func aSentinelModeReplicaPod(name string) string {
	master := getMasterPod(name)
	Expect(master).NotTo(BeEmpty(), "instance has no master to exclude")
	out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", testNamespace,
		"-l", "app.kubernetes.io/instance="+name+",app.kubernetes.io/component=redis",
		"-o", "jsonpath={.items[*].metadata.name}"))
	Expect(err).NotTo(HaveOccurred())
	for _, p := range strings.Fields(out) {
		if p != master {
			return p
		}
	}
	Fail(fmt.Sprintf("no replica pod found for %s (master %s, pods %q)", name, master, out))
	return ""
}
