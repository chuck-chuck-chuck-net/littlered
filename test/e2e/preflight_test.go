//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive
	. "github.com/onsi/gomega"    //nolint:revive

	"github.com/chuck-chuck-chuck-net/littlered/test/utils"
)

// defaultRedis is the image used by LittleRed when no image is specified in the CR.
const defaultRedis = "docker.io/library/redis:8.4.2"

// preflightImageChecks verifies that all required container images are pullable
// before running any tests. Called from BeforeSuite so a missing image fails the
// entire suite immediately with a clear message instead of producing confusing
// failures 40 minutes later.
func preflightImageChecks() {
	// Each probe uses the pull policy the real consumer of that image uses. The chaos
	// client follows e2eImagePullPolicy: Always on a generic cluster, so a stale local
	// cache cannot mask an image that is not actually pullable from the registry; Never
	// in Kind mode, where it is kind-loaded and no registry is involved. The redis image
	// is a registry pull by the kubelet in every mode, so it is always probed with Always.
	verifyImagePullable("chaos-client", getChaosClientImage(), e2eImagePullPolicy())
	verifyImagePullable("redis", defaultRedis, "Always")
}

func verifyImagePullable(label, img, pullPolicy string) {
	podName := fmt.Sprintf("preflight-%s", label)

	pod := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Never
  containers:
  - name: check
    image: %s
    imagePullPolicy: %s
    command: ["true"]
`, podName, testNamespace, img, pullPolicy)

	By(fmt.Sprintf("preflight: verifying image %s (%s) is pullable", label, img))
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(pod)
	_, err := utils.Run(cmd)
	ExpectWithOffset(2, err).NotTo(HaveOccurred(), "Failed to create preflight pod for %s", label)

	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "pod", podName,
			"-n", testNamespace,
			"-o", "jsonpath={.status.phase}")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(SatisfyAny(Equal("Succeeded"), Equal("Running")),
			"PREFLIGHT FAILURE: image %q (%s) is not pullable.\n"+
				"  Chaos client: make build-images && make push-images (or make kind-load)\n"+
				"  Redis:        verify %s is accessible from the cluster",
			img, label, defaultRedis)
	}, 2*time.Minute, 5*time.Second).Should(Succeed(),
		"PREFLIGHT FAILURE: image %q (%s) could not be pulled within 2 minutes. "+
			"Build and push it before running e2e tests.", img, label)

	cmd = exec.Command("kubectl", "delete", "pod", podName,
		"-n", testNamespace, "--ignore-not-found", "--wait=false")
	_, _ = utils.Run(cmd)
}
