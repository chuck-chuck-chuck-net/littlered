//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	clik8s "github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/littlered-operator/littlered-operator/test/utils"
)

var _ = Describe("LittleRed Security Features", Label("security"), func() {
	var k8sClient client.Client

	BeforeEach(func() {
		var err error
		k8sClient, _, _, _, err = clik8s.NewClient("")
		Expect(err).NotTo(HaveOccurred())
	})

	// Failover mode (ADR-011) needs no mode-specific arm below: its data pods are
	// {crName}-redis-N like standalone/sentinel, so the shared `-redis-0` branch
	// addresses the right pod, and spec.failover may be omitted entirely (the CRD
	// only forbids the block in other modes; failoverSpecOrDefault defaults it).
	// The sentinel-only arm is skipped by its own mode guard — correctly, as this
	// mode has no sentinel processes to authenticate against.
	modes := []string{"standalone", "sentinel", "failover", "cluster"}

	for _, mode := range modes {
		mode := mode // capture range variable

		Context(fmt.Sprintf("Password Authentication in %s mode", mode), func() {
			It("should enforce password authentication when enabled", func() {
				crName := fmt.Sprintf("auth-%s", mode)
				password := "e2e-secret-password"
				secretName := fmt.Sprintf("redis-auth-secret-%s", mode)

				By("creating the password secret")
				secret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      secretName,
						Namespace: testNamespace,
					},
					Data: map[string][]byte{
						"password": []byte(password),
					},
				}
				Expect(k8sClient.Create(context.Background(), secret)).To(Succeed())

				By(fmt.Sprintf("deploying LittleRed in %s mode with auth enabled", mode))
				cr := &littleredv1alpha1.LittleRed{
					ObjectMeta: metav1.ObjectMeta{
						Name:      crName,
						Namespace: testNamespace,
					},
					Spec: littleredv1alpha1.LittleRedSpec{
						Mode: mode,
						Auth: littleredv1alpha1.AuthSpec{
							Enabled:        true,
							ExistingSecret: secretName,
						},
					},
				}
				if mode == "cluster" {
					replicas := clusterReplicasPerShard
					cr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{
						Shards:           clusterShards,
						ReplicasPerShard: &replicas,
					}
				}
				if mode == "sentinel" {
					// masterName is required and has no default (ADR-015): derive it
					// per instance via the shared helper, never a suite-wide literal.
					cr.Spec.Sentinel = &littleredv1alpha1.SentinelSpec{
						MasterName: e2eMasterName(testNamespace, crName),
					}
				}
				Expect(k8sClient.Create(context.Background(), cr)).To(Succeed())

				By("waiting for the instance to be ready")
				Eventually(func(g Gomega) {
					curr := &littleredv1alpha1.LittleRed{}
					err := k8sClient.Get(context.Background(), client.ObjectKey{Name: crName, Namespace: testNamespace}, curr)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(curr.Status.Phase).To(Equal(littleredv1alpha1.PhaseRunning))
				}, 5*time.Minute, 5*time.Second).Should(Succeed())

				var podName string
				if mode == "cluster" {
					// Cluster mode (0.3.0+): shard 0's master is {crName}-shard-0-0.
					podName = clusterMasterPod(crName, 0)
				} else {
					podName = fmt.Sprintf("%s-redis-0", crName)
				}

				By("verifying access is DENIED without password")
				cmd := exec.Command("kubectl", "exec", podName, "-n", testNamespace, "-c", "redis", "--", "redis-cli", "PING")
				output, err := utils.Run(cmd)
				if err == nil {
					Expect(output).To(ContainSubstring("NOAUTH"), "PING succeeded without password")
				} else {
					Expect(output).To(ContainSubstring("NOAUTH"), "PING failed but not with NOAUTH")
				}

				By("verifying access is GRANTED with correct password")
				cmd = exec.Command("kubectl", "exec", podName, "-n", testNamespace, "-c", "redis", "--", "redis-cli", "-a", password, "--no-auth-warning", "PING")
				output, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("PING with password failed: %v, output: %s", err, output))
				Expect(output).To(ContainSubstring("PONG"))

				if mode == "sentinel" {
					By("verifying Sentinel also requires password")
					sentinelPod := fmt.Sprintf("%s-sentinel-0", crName)
					cmd = exec.Command("kubectl", "exec", sentinelPod, "-n", testNamespace, "--", "redis-cli", "-p", "26379", "SENTINEL", "masters")
					output, err = utils.Run(cmd)
					// Sentinel returns NOAUTH if not authenticated
					Expect(output).To(ContainSubstring("NOAUTH"))

					cmd = exec.Command("kubectl", "exec", sentinelPod, "-n", testNamespace, "--", "redis-cli", "-p", "26379", "-a", password, "--no-auth-warning", "SENTINEL", "masters")
					output, err = utils.Run(cmd)
					Expect(err).NotTo(HaveOccurred())
					Expect(output).To(ContainSubstring(e2eMasterName(testNamespace, crName)))
				}

				By("cleaning up")
				Expect(k8sClient.Delete(context.Background(), cr)).To(Succeed())
			})
		})

		Context(fmt.Sprintf("TLS Encryption in %s mode", mode), func() {
			It("should enforce TLS encryption when enabled", func() {
				crName := fmt.Sprintf("tls-%s", mode)
				secretName := fmt.Sprintf("redis-tls-secret-%s", mode)

				By("generating and creating TLS secret")
				err := utils.CreateTLSSecret(context.Background(), k8sClient, secretName, testNamespace, crName)
				Expect(err).NotTo(HaveOccurred())

				By(fmt.Sprintf("deploying LittleRed in %s mode with TLS enabled", mode))
				cr := &littleredv1alpha1.LittleRed{
					ObjectMeta: metav1.ObjectMeta{
						Name:      crName,
						Namespace: testNamespace,
					},
					Spec: littleredv1alpha1.LittleRedSpec{
						Mode: mode,
						TLS: littleredv1alpha1.TLSSpec{
							Enabled:        true,
							ExistingSecret: secretName,
							CACertSecret:   secretName,
						},
					},
				}
				if mode == "cluster" {
					replicas := clusterReplicasPerShard
					cr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{
						Shards:           clusterShards,
						ReplicasPerShard: &replicas,
					}
				}
				if mode == "sentinel" {
					// masterName is required and has no default (ADR-015): derive it
					// per instance via the shared helper, never a suite-wide literal.
					cr.Spec.Sentinel = &littleredv1alpha1.SentinelSpec{
						MasterName: e2eMasterName(testNamespace, crName),
					}
				}
				Expect(k8sClient.Create(context.Background(), cr)).To(Succeed())

				By("waiting for the instance to be ready")
				Eventually(func(g Gomega) {
					curr := &littleredv1alpha1.LittleRed{}
					err := k8sClient.Get(context.Background(), client.ObjectKey{Name: crName, Namespace: testNamespace}, curr)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(curr.Status.Phase).To(Equal(littleredv1alpha1.PhaseRunning))
				}, 5*time.Minute, 5*time.Second).Should(Succeed())

				var podName string
				if mode == "cluster" {
					// Cluster mode (0.3.0+): shard 0's master is {crName}-shard-0-0.
					podName = clusterMasterPod(crName, 0)
				} else {
					podName = fmt.Sprintf("%s-redis-0", crName)
				}

				By("verifying plain PING fails")
				cmd := exec.Command("kubectl", "exec", podName, "-n", testNamespace, "-c", "redis", "--", "redis-cli", "PING")
				_, err = utils.Run(cmd)
				Expect(err).To(HaveOccurred(), "Plain text PING should fail on TLS-enabled instance")

				By("verifying TLS PING succeeds")
				cmd = exec.Command("kubectl", "exec", podName, "-n", testNamespace, "-c", "redis", "--",
					"redis-cli",
					"--tls",
					"--cacert", "/tls/ca.crt",
					"--cert", "/tls/tls.crt",
					"--key", "/tls/tls.key",
					"PING")
				output, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("TLS PING failed: %v, output: %s", err, output))
				Expect(output).To(ContainSubstring("PONG"))

				By("cleaning up")
				Expect(k8sClient.Delete(context.Background(), cr)).To(Succeed())
			})
		})
	}
})
