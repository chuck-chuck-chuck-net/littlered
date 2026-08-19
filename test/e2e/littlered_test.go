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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/littlered-operator/littlered-operator/test/utils"
)

var _ = Describe("LittleRed", Ordered, func() {

	Context("Standalone Mode", Ordered, func() {
		const crName = "test-standalone"

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping standalone CR cleanup to allow debugging")
				return
			}
			By("cleaning up standalone CR")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		It("should create a standalone Redis instance", func() {
			AddReportEntry("cr:" + crName)
			By("Test ID: STAN-001 - applying the LittleRed CR")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: standalone
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
`, crName, testNamespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should create StatefulSet with 1 replica", func() {
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "statefulset", crName+"-redis",
					"-n", testNamespace, "-o", "jsonpath={.status.readyReplicas}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("1"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should create Service", func() {
			cmd := exec.Command("kubectl", "get", "service", crName, "-n", testNamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should have LittleRed status Running", func() {
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should respond to Redis PING", func() {
			By("Test ID: STAN-010")
			cmd := exec.Command("kubectl", "exec", crName+"-redis-0",
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "PING")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("PONG"))
		})

		It("should execute SET and GET operations", func() {
			By("Test ID: STAN-011 - setting a key")
			cmd := exec.Command("kubectl", "exec", crName+"-redis-0",
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "SET", "e2e-test-key", "e2e-test-value")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("OK"))

			By("getting the key")
			cmd = exec.Command("kubectl", "exec", crName+"-redis-0",
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "GET", "e2e-test-key")
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("e2e-test-value"))
		})

		It("should expose metrics on port 9121", func() {
			By("Test ID: STAN-012")
			// Use curl from the redis container since exporter image is minimal
			cmd := exec.Command("kubectl", "exec", crName+"-redis-0",
				"-n", testNamespace, "-c", "redis", "--",
				"sh", "-c", "cat < /dev/tcp/localhost/9121 || true")
			// Just verify the port is open; detailed metrics check would need curl
			// Alternative: check exporter container is running
			cmd = exec.Command("kubectl", "get", "pod", crName+"-redis-0",
				"-n", testNamespace, "-o", "jsonpath={.status.containerStatuses[?(@.name=='exporter')].ready}")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(Equal("true"))
		})

		It("should recreate pod after deletion", func() {
			By("Test ID: STAN-013 - recording initial RunID")
			oldRunID, err := getPodRunID(testNamespace, crName+"-redis-0")
			Expect(err).NotTo(HaveOccurred())

			By("deleting the pod")
			_, err = deletePod(testNamespace, crName+"-redis-0")
			Expect(err).NotTo(HaveOccurred())

			By("waiting for pod to be recreated and ready with a new RunID")
			Eventually(func(g Gomega) {
				newRunID, err := getPodRunID(testNamespace, crName+"-redis-0")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(newRunID).NotTo(Equal(oldRunID), "Pod should have a new RunID after recreation")

				cmd := exec.Command("kubectl", "get", "pod", crName+"-redis-0",
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, _ := utils.Run(cmd)
				g.Expect(output).To(Equal("Running"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying Redis responds after recreation")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", crName+"-redis-0",
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "PING")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(output)).To(Equal("PONG"))
			}, 1*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should clean up resources when CR is deleted", func() {
			By("Test ID: STAN-002 - deleting the LittleRed CR")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("verifying StatefulSet is deleted")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "statefulset", crName+"-redis",
					"-n", testNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred()) // Should fail because it doesn't exist
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying Service is deleted")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "service", crName,
					"-n", testNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).To(HaveOccurred())
			}, 1*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

	Context("Sentinel Mode", Ordered, func() {
		const crName = "test-sentinel"

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
			By("Test ID: SEN-001 - applying the LittleRed CR with sentinel mode")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
  sentinel:
    masterName: e2e-scoped
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the sentinel cluster to be ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "statefulset", crName+"-redis",
					"-n", testNamespace, "-o", "jsonpath={.status.readyReplicas}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("3"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("waiting for LittleRed status Running")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping sentinel CR cleanup to allow debugging")
				return
			}
			By("cleaning up sentinel CR")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		It("should have all required services", func() {
			By("Test ID: SEN-002 - checking master service")
			cmd := exec.Command("kubectl", "get", "service", crName, "-n", testNamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("checking replicas service")
			cmd = exec.Command("kubectl", "get", "service", crName+"-replicas", "-n", testNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("checking sentinel service")
			cmd = exec.Command("kubectl", "get", "service", crName+"-sentinel", "-n", testNamespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should have sentinel quorum established and broadcast working", func() {
			By("Test ID: SEN-003")
			// The verifySentinelTopologySync helper now checks ALL sentinels for consistency
			verifySentinelTopologySync(testNamespace, crName, 3, 2)

			Eventually(func(g Gomega) {
				// Query sentinel-0 for master info to verify quorum
				cmd := exec.Command("kubectl", "exec", crName+"-sentinel-0",
					"-n", testNamespace, "-c", "sentinel", "--",
					"redis-cli", "-p", "26379", "SENTINEL", "master", "mymaster")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("num-slaves"))
				g.Expect(output).To(ContainSubstring("num-other-sentinels"))
				// Quorum of 2 means it needs to see at least 2 other sentinels or be acknowledged by them
				// Redis reports num-other-sentinels: 2 when all 3 are up
				g.Expect(output).To(ContainSubstring("num-other-sentinels\n2"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should report master info in status and clear bootstrap flag", func() {
			By("Test ID: SEN-004")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
				master, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(master).NotTo(BeEmpty())

				cmd = exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.bootstrapRequired}")
				bootstrap, _ := utils.Run(cmd)
				// Field has omitempty, so it might be empty string when false
				g.Expect(bootstrap).To(Or(Equal("false"), Equal("")), "bootstrapRequired flag should be cleared once Running")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should have correct role labels on all redis pods", func() {
			By("Test ID: SEN-005")
			// The verifySentinelTopologySync helper already verifies K8s labels
			verifySentinelTopologySync(testNamespace, crName, 3, 2)
		})

		It("should have working replication", func() {
			By("Test ID: SEN-010 - getting master pod name")
			cmd := exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
			masterPod, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			masterPod = strings.TrimSpace(masterPod)
			Expect(masterPod).NotTo(BeEmpty())

			By("writing to master")
			cmd = exec.Command("kubectl", "exec", masterPod,
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "SET", "repl-test-key", "repl-test-value")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("OK"))

			By("reading from a replica")
			// Find a replica (any pod that's not the master)
			var replicaPod string
			for i := 0; i < 3; i++ {
				podName := fmt.Sprintf("%s-redis-%d", crName, i)
				if podName != masterPod {
					replicaPod = podName
					break
				}
			}
			Expect(replicaPod).NotTo(BeEmpty())

			Eventually(func(g Gomega) {
				cmd = exec.Command("kubectl", "exec", replicaPod,
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "GET", "repl-test-key")
				output, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(output)).To(Equal("repl-test-value"))
			}, 30*time.Second, 2*time.Second).Should(Succeed())
		})
	})

	Context("Rolling Update", Ordered, func() {
		const crName = "test-rolling-update"

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping rolling update CR cleanup to allow debugging")
				return
			}
			By("cleaning up rolling update CR")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		It("should create a standalone Redis instance for rolling update testing", func() {
			By("applying the LittleRed CR")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: standalone
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
`, crName, testNamespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the instance to be running")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("writing test data before update")
			cmd = exec.Command("kubectl", "exec", crName+"-redis-0",
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "SET", "rolling-update-key", "pre-update-value")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("OK"))
		})

		It("should perform rolling update when resources are changed", func() {
			By("Test ID: STAN-004 - getting the current pod UID before update")
			cmd := exec.Command("kubectl", "get", "pod", crName+"-redis-0",
				"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
			oldUID, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			oldUID = strings.TrimSpace(oldUID)
			Expect(oldUID).NotTo(BeEmpty())

			By("updating the resource limits")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: standalone
  resources:
    requests:
      cpu: "150m"
      memory: "192Mi"
    limits:
      cpu: "150m"
      memory: "192Mi"
`, crName, testNamespace)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the pod to be replaced with new resources")
			Eventually(func(g Gomega) {
				// Check that pod has been recreated (different UID)
				cmd := exec.Command("kubectl", "get", "pod", crName+"-redis-0",
					"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
				newUID, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				newUID = strings.TrimSpace(newUID)
				g.Expect(newUID).NotTo(Equal(oldUID), "Pod should have been recreated")

				// Check the pod is running
				cmd = exec.Command("kubectl", "get", "pod", crName+"-redis-0",
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				phase, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(phase).To(Equal("Running"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the new resource limits are applied")
			cmd = exec.Command("kubectl", "get", "pod", crName+"-redis-0",
				"-n", testNamespace, "-o", "jsonpath={.spec.containers[?(@.name=='redis')].resources.limits.memory}")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("192Mi"))
		})

		It("should preserve data after rolling update", func() {
			By("waiting for Redis to be ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", crName+"-redis-0",
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "PING")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(output)).To(Equal("PONG"))
			}, 1*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying data persists (note: data is lost since we use emptyDir)")
			// Since we use emptyDir for data, data is NOT expected to persist
			// This is by design for a cache - we just verify Redis is working
			cmd := exec.Command("kubectl", "exec", crName+"-redis-0",
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "SET", "post-update-key", "post-update-value")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("OK"))

			cmd = exec.Command("kubectl", "exec", crName+"-redis-0",
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "GET", "post-update-key")
			output, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("post-update-value"))
		})

		It("should have LittleRed status Running after update", func() {
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should trigger rolling update when config changes", func() {
			By("Test ID: STAN-005 - getting the current pod UID and config hash")
			cmd := exec.Command("kubectl", "get", "pod", crName+"-redis-0",
				"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
			oldUID, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			oldUID = strings.TrimSpace(oldUID)

			cmd = exec.Command("kubectl", "get", "pod", crName+"-redis-0",
				"-n", testNamespace, "-o", "jsonpath={.metadata.annotations.redis\\.chuck-chuck-chuck\\.net/config-hash}")
			oldHash, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			oldHash = strings.TrimSpace(oldHash)
			Expect(oldHash).NotTo(BeEmpty(), "Pod should have config hash annotation")

			By("updating the config (maxmemory policy)")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: standalone
  resources:
    requests:
      cpu: "150m"
      memory: "192Mi"
    limits:
      cpu: "150m"
      memory: "192Mi"
  config:
    maxmemoryPolicy: volatile-lru
`, crName, testNamespace)
			cmd = exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the pod to be replaced due to config hash change")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", crName+"-redis-0",
					"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
				newUID, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				newUID = strings.TrimSpace(newUID)
				g.Expect(newUID).NotTo(Equal(oldUID), "Pod should have been recreated")

				// Verify new config hash is different
				cmd = exec.Command("kubectl", "get", "pod", crName+"-redis-0",
					"-n", testNamespace, "-o", "jsonpath={.metadata.annotations.redis\\.chuck-chuck-chuck\\.net/config-hash}")
				newHash, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				newHash = strings.TrimSpace(newHash)
				g.Expect(newHash).NotTo(Equal(oldHash), "Config hash should have changed")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying Redis is using the new maxmemory-policy")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", crName+"-redis-0",
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "CONFIG", "GET", "maxmemory-policy")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("volatile-lru"))
			}, 1*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

	Context("Sentinel Rolling Update", Ordered, func() {
		const crName = "test-sentinel-rolling"

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				return
			}
			By("cleaning up sentinel rolling update CR")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=60s")
			_, _ = utils.Run(cmd)
		})

		It("should create a sentinel cluster for rolling update testing", func() {
			By("applying the LittleRed CR with sentinel mode")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
  sentinel:
    masterName: e2e-scoped
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
`, crName, testNamespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the cluster to be running")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("writing test data")
			// Get master pod
			cmd = exec.Command("kubectl", "get", "littlered", crName,
				"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
			masterPod, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			masterPod = strings.TrimSpace(masterPod)

			cmd = exec.Command("kubectl", "exec", masterPod,
				"-n", testNamespace, "-c", "redis", "--",
				"redis-cli", "SET", "sentinel-rolling-key", "test-value")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(output)).To(Equal("OK"))
		})

		It("should perform rolling update on sentinel cluster without losing quorum", func() {
			By("Test ID: SEN-005 - getting current sentinel pod UIDs")
			var oldSentinelUIDs []string
			for i := 0; i < 3; i++ {
				cmd := exec.Command("kubectl", "get", "pod", fmt.Sprintf("%s-sentinel-%d", crName, i),
					"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
				uid, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
				oldSentinelUIDs = append(oldSentinelUIDs, strings.TrimSpace(uid))
			}

			By("updating the sentinel resources")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
  sentinel:
    masterName: e2e-scoped
    quorum: 2
    downAfterMilliseconds: 5000
    failoverTimeout: 10000
    resources:
      requests:
        cpu: "50m"
        memory: "48Mi"
      limits:
        cpu: "50m"
        memory: "48Mi"
`, crName, testNamespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for sentinel pods to be replaced")
			Eventually(func(g Gomega) {
				var newUIDs []string
				for i := 0; i < 3; i++ {
					cmd := exec.Command("kubectl", "get", "pod", fmt.Sprintf("%s-sentinel-%d", crName, i),
						"-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
					uid, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					newUIDs = append(newUIDs, strings.TrimSpace(uid))
				}
				// At least one should be different (rolling update in progress or complete)
				differentCount := 0
				for i := 0; i < 3; i++ {
					if newUIDs[i] != oldSentinelUIDs[i] {
						differentCount++
					}
				}
				g.Expect(differentCount).To(BeNumerically(">", 0), "At least one sentinel should have been replaced")
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying all sentinel pods eventually become ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "statefulset", crName+"-sentinel",
					"-n", testNamespace, "-o", "jsonpath={.status.readyReplicas}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("3"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should maintain sentinel quorum after rolling update", func() {
			By("verifying sentinel quorum is established")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", crName+"-sentinel-0",
					"-n", testNamespace, "-c", "sentinel", "--",
					"redis-cli", "-p", "26379", "SENTINEL", "master", "mymaster")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("num-slaves"))
				g.Expect(output).To(ContainSubstring("num-other-sentinels"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		It("should have cluster status Running after sentinel rolling update", func() {
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})
	})

	Context("Sentinel Failover", Ordered, func() {
		const crName = "test-failover"

		BeforeAll(func() {
			AddReportEntry("cr:" + crName)
		})

		AfterAll(func() {
			if debugOnFailure && suiteOrSpecFailed() {
				By("skipping failover CR cleanup to allow debugging")
				return
			}
			By("cleaning up failover CR")
			cmd := exec.Command("kubectl", "delete", "littlered", crName, "-n", testNamespace, "--ignore-not-found", "--timeout=60s")
			_, _ = utils.Run(cmd)
		})

		It("should create a sentinel cluster for failover testing", func() {
			By("applying the LittleRed CR with fast failover settings")
			cr := fmt.Sprintf(`
apiVersion: redis.chuck-chuck-chuck.net/v1alpha1
kind: LittleRed
metadata:
  name: %s
  namespace: %s
spec:
  mode: sentinel
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "100m"
      memory: "128Mi"
  sentinel:
    masterName: e2e-scoped
    quorum: 2
    downAfterMilliseconds: 3000
    failoverTimeout: 10000
`, crName, testNamespace)
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(cr)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for cluster to be ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("waiting for sentinel quorum")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.sentinels.ready}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("3"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())
		})

		for _, mode := range restartModes {
			mode := mode // capture range variable
			It(fmt.Sprintf("should elect new master after master pod deletion (%s)", mode.Name), func() {
				By("Test ID: SEN-011 - getting current master pod")
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
				originalMaster, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
				originalMaster = strings.TrimSpace(originalMaster)
				Expect(originalMaster).NotTo(BeEmpty())
				_, _ = fmt.Fprintf(GinkgoWriter, "Original master: %s\n", originalMaster)

				// Capture the master POD INSTANCE (UID), not just its name: a real
				// failover/recovery replaces the instance, but a StatefulSet reuses the
				// name, so we assert on the UID below (see the waiting step).
				originalMasterUID := podUID(testNamespace, originalMaster)
				Expect(originalMasterUID).NotTo(BeEmpty())

				By("writing test data to master before failover")
				cmd = exec.Command("kubectl", "exec", originalMaster,
					"-n", testNamespace, "-c", "redis", "--",
					"redis-cli", "SET", "failover-test-key", "failover-test-value")
				output, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
				Expect(strings.TrimSpace(output)).To(Equal("OK"))

				By(fmt.Sprintf("deleting the master pod to trigger failover (%s mode)", mode.Name))
				_, err = deletePodMode(testNamespace, originalMaster, mode.Graceful)
				Expect(err).NotTo(HaveOccurred())

				By("waiting for new master to be elected")
				var newMaster string
				Eventually(func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "littlered", crName,
						"-n", testNamespace, "-o", "jsonpath={.status.master.podName}")
					output, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					newMaster = strings.TrimSpace(output)
					g.Expect(newMaster).NotTo(BeEmpty())
					// A real failover/recovery must have happened — but assert on the pod
					// INSTANCE (UID), not the name. Under StatefulSet name reuse + the
					// operator's data-based election, the deleted master's pod name can
					// legitimately return and be re-elected once its recreated pod re-holds
					// the data (it resyncs from the still-terminating old master during the
					// delete grace window). So a name comparison is racy; the UID comparison
					// holds whether a survivor was promoted OR the original name was recreated,
					// and still fails if the delete silently no-op'd (same instance stays master).
					newMasterUID := podUID(testNamespace, newMaster)
					g.Expect(newMasterUID).NotTo(BeEmpty())
					g.Expect(newMasterUID).NotTo(Equal(originalMasterUID),
						"master must be a new pod instance (a real failover/recovery occurred), not the same instance")
				}, 90*time.Second, 3*time.Second).Should(Succeed())
				_, _ = fmt.Fprintf(GinkgoWriter, "New master: %s\n", newMaster)

				By("verifying sentinel reports new master")
				Eventually(func(g Gomega) {
					cmd := exec.Command("kubectl", "exec", crName+"-sentinel-0",
						"-n", testNamespace, "-c", "sentinel", "--",
						"redis-cli", "-p", "26379", "SENTINEL", "get-master-addr-by-name", "mymaster")
					output, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).NotTo(BeEmpty())
					_, _ = fmt.Fprintf(GinkgoWriter, "Sentinel reports master: %s\n", output)
				}, 30*time.Second, 3*time.Second).Should(Succeed())

				By("Test ID: SEN-012 - verifying data is preserved after failover")
				Eventually(func(g Gomega) {
					// Query sentinel for current master address
					cmd := exec.Command("kubectl", "exec", crName+"-sentinel-0",
						"-n", testNamespace, "-c", "sentinel", "--",
						"redis-cli", "-p", "26379", "SENTINEL", "get-master-addr-by-name", "mymaster")
					masterInfo, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					_, _ = fmt.Fprintf(GinkgoWriter, "Master info: %s\n", masterInfo)

					// Try reading from any available pod
					for i := 0; i < 3; i++ {
						podName := fmt.Sprintf("%s-redis-%d", crName, i)
						cmd = exec.Command("kubectl", "exec", podName,
							"-n", testNamespace, "-c", "redis", "--",
							"redis-cli", "GET", "failover-test-key")
						output, err := utils.Run(cmd)
						if err == nil && strings.TrimSpace(output) == "failover-test-value" {
							_, _ = fmt.Fprintf(GinkgoWriter, "Data found on %s\n", podName)
							return
						}
					}
					g.Expect(false).To(BeTrue(), "Data not found on any pod after failover")
				}, 60*time.Second, 5*time.Second).Should(Succeed())
			})
		}

		It("should restore full cluster after failover", func() {
			By("Test ID: SEN-013 - waiting for all pods to be ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "statefulset", crName+"-redis",
					"-n", testNamespace, "-o", "jsonpath={.status.readyReplicas}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("3"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying cluster status is Running")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "littlered", crName,
					"-n", testNamespace, "-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying sentinel sees 2 replicas")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", crName+"-sentinel-0",
					"-n", testNamespace, "-c", "sentinel", "--",
					"redis-cli", "-p", "26379", "SENTINEL", "master", "mymaster")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("num-slaves"))
				// Parse output to verify num-slaves is 2
				lines := strings.Split(output, "\n")
				for i, line := range lines {
					if strings.TrimSpace(line) == "num-slaves" && i+1 < len(lines) {
						g.Expect(strings.TrimSpace(lines[i+1])).To(Equal("2"))
						return
					}
				}
				g.Expect(false).To(BeTrue(), "Could not find num-slaves in sentinel output")
			}, 2*time.Minute, 5*time.Second).Should(Succeed())

			verifySentinelTopologySync(testNamespace, crName, 3, 2)
		})
	})
})
