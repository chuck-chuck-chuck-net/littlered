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

package controller

import (
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

const (
	testLRName          = "my-cache"
	testSentinelName    = testLRName + "-sentinel"
	testReplicasName    = testLRName + "-replicas"
	testStatefulSetName = testLRName + "-redis"
	testPDBName         = testLRName + "-redis-pdb"
	testNamespace       = "test-ns"
	testTLSSecret       = "tls-secret"
	testMaxmemPolicy    = "volatile-lru"
)

// Helper to create a minimal LittleRed for testing
func newTestLittleRed(name, namespace string) *littleredv1alpha1.LittleRed {
	lr := &littleredv1alpha1.LittleRed{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: littleredv1alpha1.LittleRedSpec{},
	}
	// Apply defaults
	lr.SetDefaults()
	return lr
}

// ============================================================================
// Name Helper Tests
// ============================================================================

func TestConfigMapName(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	expected := "my-cache-config"
	if got := configMapName(lr); got != expected {
		t.Errorf("configMapName() = %q, want %q", got, expected)
	}
}

func TestSentinelConfigMapName(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	expected := "my-cache-sentinel-config"
	if got := sentinelConfigMapName(lr); got != expected {
		t.Errorf("sentinelConfigMapName() = %q, want %q", got, expected)
	}
}

func TestStatefulSetName(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	expected := testStatefulSetName
	if got := statefulSetName(lr); got != expected {
		t.Errorf("statefulSetName() = %q, want %q", got, expected)
	}
}

func TestSentinelStatefulSetName(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	expected := testSentinelName
	if got := sentinelStatefulSetName(lr); got != expected {
		t.Errorf("sentinelStatefulSetName() = %q, want %q", got, expected)
	}
}

func TestServiceName(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	expected := testLRName
	if got := serviceName(lr); got != expected {
		t.Errorf("serviceName() = %q, want %q", got, expected)
	}
}

func TestReplicasServiceName(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	expected := testReplicasName
	if got := replicasServiceName(lr); got != expected {
		t.Errorf("replicasServiceName() = %q, want %q", got, expected)
	}
}

func TestSentinelServiceName(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	expected := testSentinelName
	if got := sentinelServiceName(lr); got != expected {
		t.Errorf("sentinelServiceName() = %q, want %q", got, expected)
	}
}

// ============================================================================
// Label Tests
// ============================================================================

func TestCommonLabels(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	labels := commonLabels(lr)

	tests := []struct {
		key      string
		expected string
	}{
		{"app.kubernetes.io/name", "littlered"},
		{"app.kubernetes.io/instance", testLRName},
		{"app.kubernetes.io/managed-by", "littlered-operator"},
		{"app.kubernetes.io/version", littleredv1alpha1.DefaultImageTag},
		{"redis.chuck-chuck-chuck.net/mode", ModeStandalone},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := labels[tt.key]; got != tt.expected {
				t.Errorf("commonLabels()[%q] = %q, want %q", tt.key, got, tt.expected)
			}
		})
	}
}

func TestSelectorLabels(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	labels := selectorLabels(lr)

	if labels["app.kubernetes.io/name"] != "littlered" {
		t.Errorf("selectorLabels() missing app.kubernetes.io/name")
	}
	if labels["app.kubernetes.io/instance"] != testLRName {
		t.Errorf("selectorLabels() missing app.kubernetes.io/instance")
	}
	// Should not have other labels
	if len(labels) != 2 {
		t.Errorf("selectorLabels() has %d labels, want 2", len(labels))
	}
}

func TestRedisSelectorLabels(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	labels := redisSelectorLabels(lr)

	if labels["app.kubernetes.io/component"] != ComponentRedis {
		t.Errorf("redisSelectorLabels() missing component=redis")
	}
}

func TestSentinelSelectorLabels(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	labels := sentinelSelectorLabels(lr)

	if labels["app.kubernetes.io/component"] != ComponentSentinel {
		t.Errorf("sentinelSelectorLabels() missing component=sentinel")
	}
}

func TestMasterSelectorLabels(t *testing.T) {
	lr := newTestLittleRed(testLRName, "default")
	labels := masterSelectorLabels(lr)

	if labels[LabelRole] != RoleMaster {
		t.Errorf("masterSelectorLabels() missing role=master")
	}
	if labels["app.kubernetes.io/component"] != ComponentRedis {
		t.Errorf("masterSelectorLabels() missing component=redis")
	}
}

// ============================================================================
// ConfigMap Tests
// ============================================================================

func TestBuildConfigMap(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	cm := buildConfigMap(lr)

	// Check metadata
	if cm.Name != "my-cache-config" {
		t.Errorf("ConfigMap name = %q, want %q", cm.Name, "my-cache-config")
	}
	if cm.Namespace != testNamespace {
		t.Errorf("ConfigMap namespace = %q, want %q", cm.Namespace, testNamespace)
	}

	// Check data has redis.conf
	if _, ok := cm.Data["redis.conf"]; !ok {
		t.Error("ConfigMap missing redis.conf key")
	}
}

func TestBuildRedisConfig(t *testing.T) {
	tests := []struct {
		name        string
		setupLR     func(*littleredv1alpha1.LittleRed)
		mustHave    []string
		mustNotHave []string
	}{
		{
			name:    "basic config",
			setupLR: func(lr *littleredv1alpha1.LittleRed) {},
			mustHave: []string{
				"bind 0.0.0.0",
				"port 6379",
				"save \"\"",
				"appendonly no",
				"maxmemory-policy noeviction",
			},
		},
		{
			name: "with TLS enabled",
			setupLR: func(lr *littleredv1alpha1.LittleRed) {
				lr.Spec.TLS.Enabled = true
				lr.Spec.TLS.ExistingSecret = testTLSSecret
			},
			mustHave: []string{
				"tls-port 6379",
				"port 0",
				"tls-cert-file /tls/tls.crt",
				"tls-key-file /tls/tls.key",
			},
		},
		{
			name: "with TLS client auth",
			setupLR: func(lr *littleredv1alpha1.LittleRed) {
				lr.Spec.TLS.Enabled = true
				lr.Spec.TLS.ExistingSecret = testTLSSecret
				lr.Spec.TLS.CACertSecret = testTLSSecret // CA is in the same secret → mounted at /tls
				lr.Spec.TLS.ClientAuth = true
			},
			mustHave: []string{
				"tls-ca-cert-file /tls/ca.crt",
				"tls-auth-clients yes",
			},
		},
		{
			name: "with raw config",
			setupLR: func(lr *littleredv1alpha1.LittleRed) {
				lr.Spec.Config.Raw = "custom-setting value"
			},
			mustHave: []string{
				"custom-setting value",
			},
		},
		{
			name: "with custom maxmemory policy",
			setupLR: func(lr *littleredv1alpha1.LittleRed) {
				lr.Spec.Config.MaxmemoryPolicy = testMaxmemPolicy
			},
			mustHave: []string{
				"maxmemory-policy volatile-lru",
			},
			mustNotHave: []string{
				"maxmemory-policy noeviction",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := newTestLittleRed("test", "default")
			tt.setupLR(lr)
			config := buildRedisConfig(lr)

			for _, s := range tt.mustHave {
				if !strings.Contains(config, s) {
					t.Errorf("redis.conf missing %q\nGot:\n%s", s, config)
				}
			}
			for _, s := range tt.mustNotHave {
				if strings.Contains(config, s) {
					t.Errorf("redis.conf should not contain %q\nGot:\n%s", s, config)
				}
			}
		})
	}
}

// ============================================================================
// Config Hash Tests
// ============================================================================

func TestComputeConfigHash(t *testing.T) {
	const (
		k1, k2 = "key1", "key2"
		v1, v2 = "value1", "value2"
	)
	// Same data should produce same hash
	data1 := map[string]string{k1: v1, k2: v2}
	data2 := map[string]string{k2: v2, k1: v1} // Different order
	hash1 := computeConfigHash(data1)
	hash2 := computeConfigHash(data2)

	if hash1 != hash2 {
		t.Errorf("Same data different order should produce same hash: %q != %q", hash1, hash2)
	}

	// Different data should produce different hash
	data3 := map[string]string{k1: v1, k2: "different"}
	hash3 := computeConfigHash(data3)

	if hash1 == hash3 {
		t.Error("Different data should produce different hash")
	}

	// Hash should be 16 characters
	if len(hash1) != 16 {
		t.Errorf("Hash length = %d, want 16", len(hash1))
	}
}

func TestConfigHashChangesWithConfig(t *testing.T) {
	lr1 := newTestLittleRed("test", "default")
	lr2 := newTestLittleRed("test", "default")
	lr2.Spec.Config.MaxmemoryPolicy = testMaxmemPolicy

	config1 := buildRedisConfig(lr1)
	config2 := buildRedisConfig(lr2)

	hash1 := computeConfigHash(map[string]string{"redis.conf": config1})
	hash2 := computeConfigHash(map[string]string{"redis.conf": config2})

	if hash1 == hash2 {
		t.Error("Config change should produce different hash")
	}
}

// ============================================================================
// StatefulSet Tests
// ============================================================================

func TestBuildStatefulSet(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	sts := buildStatefulSet(lr)

	// Check metadata
	if sts.Name != testStatefulSetName {
		t.Errorf("StatefulSet name = %q, want %q", sts.Name, testStatefulSetName)
	}
	if sts.Namespace != testNamespace {
		t.Errorf("StatefulSet namespace = %q, want %q", sts.Namespace, testNamespace)
	}

	// Check replicas
	if *sts.Spec.Replicas != 1 {
		t.Errorf("StatefulSet replicas = %d, want 1", *sts.Spec.Replicas)
	}

	// Check serviceName
	if sts.Spec.ServiceName != testLRName {
		t.Errorf("StatefulSet serviceName = %q, want %q", sts.Spec.ServiceName, testLRName)
	}

	// Check containers (should have redis + exporter by default)
	containers := sts.Spec.Template.Spec.Containers
	if len(containers) != 2 {
		t.Errorf("StatefulSet has %d containers, want 2 (redis + exporter)", len(containers))
	}

	// Verify redis container exists
	var hasRedis bool
	for _, c := range containers {
		if c.Name == ComponentRedis {
			hasRedis = true
			// Check image
			wantImage := littleredv1alpha1.DefaultRegistry + "/" + littleredv1alpha1.DefaultImagePath + ":" + littleredv1alpha1.DefaultImageTag
			if c.Image != wantImage {
				t.Errorf("Redis container image = %q, want default", c.Image)
			}
			// Check port
			if len(c.Ports) == 0 || c.Ports[0].ContainerPort != 6379 {
				t.Error("Redis container missing port 6379")
			}
			// Check security context
			if c.SecurityContext == nil {
				t.Error("Redis container missing security context")
			} else {
				if c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation {
					t.Error("Redis container should not allow privilege escalation")
				}
				if c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem {
					t.Error("Redis container should have read-only root filesystem")
				}
			}
		}
	}
	if !hasRedis {
		t.Error("StatefulSet missing redis container")
	}

	// Check config hash annotation is present
	annotations := sts.Spec.Template.Annotations
	if annotations == nil {
		t.Fatal("StatefulSet pod template missing annotations")
	}
	if _, ok := annotations[AnnotationConfigHash]; !ok {
		t.Error("StatefulSet pod template missing config hash annotation")
	}
}

func TestBuildStatefulSetConfigHashChangesOnConfigChange(t *testing.T) {
	lr1 := newTestLittleRed(testLRName, testNamespace)
	lr2 := newTestLittleRed(testLRName, testNamespace)
	lr2.Spec.Config.MaxmemoryPolicy = testMaxmemPolicy

	sts1 := buildStatefulSet(lr1)
	sts2 := buildStatefulSet(lr2)

	hash1 := sts1.Spec.Template.Annotations[AnnotationConfigHash]
	hash2 := sts2.Spec.Template.Annotations[AnnotationConfigHash]

	if hash1 == hash2 {
		t.Error("Config hash should change when config changes")
	}
}

func TestBuildStatefulSetWithoutMetrics(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	enabled := false
	lr.Spec.Metrics.Enabled = &enabled
	sts := buildStatefulSet(lr)

	// Should only have redis container
	containers := sts.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Errorf("StatefulSet has %d containers, want 1 (redis only)", len(containers))
	}
	if containers[0].Name != ComponentRedis {
		t.Errorf("Container name = %q, want redis", containers[0].Name)
	}
}

func TestBuildStatefulSetWithAuth(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Auth.Enabled = true
	lr.Spec.Auth.ExistingSecret = "redis-password"
	sts := buildStatefulSet(lr)

	// Find redis container
	var redisContainer *corev1.Container
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].Name == ComponentRedis {
			redisContainer = &sts.Spec.Template.Spec.Containers[i]
			break
		}
	}

	if redisContainer == nil {
		t.Fatal("redis container not found")
	}

	// Check for REDIS_PASSWORD env var
	var hasPasswordEnv bool
	for _, env := range redisContainer.Env {
		if env.Name == "REDIS_PASSWORD" {
			hasPasswordEnv = true
			if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
				t.Error("REDIS_PASSWORD should reference a secret")
			} else if env.ValueFrom.SecretKeyRef.Name != "redis-password" {
				t.Errorf("REDIS_PASSWORD secret = %q, want %q",
					env.ValueFrom.SecretKeyRef.Name, "redis-password")
			}
		}
	}
	if !hasPasswordEnv {
		t.Error("redis container missing REDIS_PASSWORD env var")
	}
}

func TestBuildStatefulSetWithCustomResources(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}
	sts := buildStatefulSet(lr)

	// Find redis container
	var redisContainer *corev1.Container
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].Name == ComponentRedis {
			redisContainer = &sts.Spec.Template.Spec.Containers[i]
			break
		}
	}

	if redisContainer == nil {
		t.Fatal("redis container not found")
	}

	// Verify resources
	if redisContainer.Resources.Requests.Cpu().String() != "500m" {
		t.Errorf("CPU request = %s, want 500m", redisContainer.Resources.Requests.Cpu().String())
	}
	if redisContainer.Resources.Requests.Memory().String() != "1Gi" {
		t.Errorf("Memory request = %s, want 1Gi", redisContainer.Resources.Requests.Memory().String())
	}
}

// ============================================================================
// Service Tests
// ============================================================================

func TestBuildService(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	svc := buildService(lr)

	// Check metadata
	if svc.Name != testLRName {
		t.Errorf("Service name = %q, want %q", svc.Name, testLRName)
	}
	if svc.Namespace != testNamespace {
		t.Errorf("Service namespace = %q, want %q", svc.Namespace, testNamespace)
	}

	// Check type
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Service type = %q, want ClusterIP", svc.Spec.Type)
	}

	// Check ports (should have redis + metrics by default)
	if len(svc.Spec.Ports) != 2 {
		t.Errorf("Service has %d ports, want 2", len(svc.Spec.Ports))
	}

	var hasRedisPort, hasMetricsPort bool
	for _, p := range svc.Spec.Ports {
		if p.Name == ComponentRedis && p.Port == 6379 {
			hasRedisPort = true
		}
		if p.Name == portNameMetrics && p.Port == 9121 {
			hasMetricsPort = true
		}
	}
	if !hasRedisPort {
		t.Error("Service missing redis port 6379")
	}
	if !hasMetricsPort {
		t.Error("Service missing metrics port 9121")
	}
}

func TestBuildServiceWithoutMetrics(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	enabled := false
	lr.Spec.Metrics.Enabled = &enabled
	svc := buildService(lr)

	// Should only have redis port
	if len(svc.Spec.Ports) != 1 {
		t.Errorf("Service has %d ports, want 1", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Name != ComponentRedis {
		t.Errorf("Port name = %q, want redis", svc.Spec.Ports[0].Name)
	}
}

func TestBuildServiceWithCustomLabels(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Service.Labels = map[string]string{
		"custom-label": "custom-value",
	}
	svc := buildService(lr)

	if svc.Labels["custom-label"] != "custom-value" {
		t.Error("Service missing custom label")
	}
}

// ============================================================================
// Volumes Tests
// ============================================================================

func TestBuildVolumes(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	volumes := buildVolumes(lr)

	// Should have config and data volumes
	if len(volumes) != 2 {
		t.Errorf("got %d volumes, want 2", len(volumes))
	}

	var hasConfig, hasData bool
	for _, v := range volumes {
		if v.Name == volNameConfig {
			hasConfig = true
			if v.ConfigMap == nil {
				t.Error("config volume should be a ConfigMap")
			}
		}
		if v.Name == volNameData {
			hasData = true
			if v.EmptyDir == nil {
				t.Error("data volume should be EmptyDir")
			}
		}
	}
	if !hasConfig {
		t.Error("missing config volume")
	}
	if !hasData {
		t.Error("missing data volume")
	}
}

func TestBuildVolumesWithTLS(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.TLS.Enabled = true
	lr.Spec.TLS.ExistingSecret = testTLSSecret
	volumes := buildVolumes(lr)

	// Should have config, data, and tls volumes
	if len(volumes) != 3 {
		t.Errorf("got %d volumes, want 3", len(volumes))
	}

	var hasTLS bool
	for _, v := range volumes {
		if v.Name == volNameTLS {
			hasTLS = true
			if v.Secret == nil || v.Secret.SecretName != testTLSSecret {
				t.Error("tls volume should reference tls-secret")
			}
		}
	}
	if !hasTLS {
		t.Error("missing tls volume")
	}
}

// ============================================================================
// Probe Tests
// ============================================================================

func TestBuildLivenessProbe(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	probe := buildLivenessProbe(lr)

	if probe.Exec == nil {
		t.Fatal("liveness probe should use exec")
	}

	cmd := strings.Join(probe.Exec.Command, " ")
	if !strings.Contains(cmd, "redis-cli") || !strings.Contains(cmd, "ping") {
		t.Errorf("liveness probe command = %q, should contain redis-cli ping", cmd)
	}

	if probe.InitialDelaySeconds != 5 {
		t.Errorf("InitialDelaySeconds = %d, want 5", probe.InitialDelaySeconds)
	}
}

func TestBuildLivenessProbeWithAuth(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Auth.Enabled = true
	probe := buildLivenessProbe(lr)

	cmd := strings.Join(probe.Exec.Command, " ")
	if !strings.Contains(cmd, "-a") || !strings.Contains(cmd, "$(REDIS_PASSWORD)") {
		t.Errorf("liveness probe should include auth flag, got: %s", cmd)
	}
}

func TestBuildLivenessProbeWithTLS(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.TLS.Enabled = true
	probe := buildLivenessProbe(lr)

	cmd := strings.Join(probe.Exec.Command, " ")
	if !strings.Contains(cmd, "--tls") {
		t.Errorf("liveness probe should include --tls flag, got: %s", cmd)
	}
}

// ============================================================================
// Sentinel Mode Tests
// ============================================================================

func TestBuildSentinelLivenessProbe(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Sentinel = &littleredv1alpha1.SentinelSpec{
		Quorum:                2,
		DownAfterMilliseconds: 5000,
		FailoverTimeout:       10000,
	}
	probe := buildSentinelLivenessProbe(lr)

	if probe.Exec == nil {
		t.Fatal("sentinel liveness probe should use exec")
	}

	cmd := strings.Join(probe.Exec.Command, " ")
	// LR-016: the sentinel liveness probe is a plain local health check. It must NOT make
	// topology decisions — restarting a masterless replica during a leaderless deadlock wipes
	// the survivor data Rule L preserves. Zombie redirect is Rule R's job; leaderless is Rule L's.
	if !strings.Contains(cmd, "bootstrap-in-progress") {
		t.Errorf("probe command should skip check while bootstrap is in progress, got: %s", cmd)
	}
	if !strings.Contains(cmd, "ping") {
		t.Errorf("probe command should PING locally, got: %s", cmd)
	}
	if strings.Contains(cmd, "info replication") {
		t.Errorf("probe must NOT inspect replication topology (LR-016), got: %s", cmd)
	}
	if strings.Contains(cmd, "master_link_status") || strings.Contains(cmd, "master_host") {
		t.Errorf("probe must NOT decide on master link/host (LR-016), got: %s", cmd)
	}

	if probe.FailureThreshold != 3 {
		t.Errorf("FailureThreshold = %d, want 3", probe.FailureThreshold)
	}
	if probe.InitialDelaySeconds != 15 {
		t.Errorf("InitialDelaySeconds = %d, want 15", probe.InitialDelaySeconds)
	}
}

func TestBuildSentinelLivenessProbeThresholdIndependentOfTimings(t *testing.T) {
	// LR-016: the failure threshold no longer derives from downAfter+failoverTimeout (the probe
	// no longer waits out a failover window). It is a fixed local-health threshold regardless of
	// Sentinel timings — including the nil-spec default path.
	nilSpec := newTestLittleRed(testLRName, testNamespace)
	if got := buildSentinelLivenessProbe(nilSpec).FailureThreshold; got != 3 {
		t.Errorf("nil sentinel spec: FailureThreshold = %d, want 3", got)
	}
	bigSpec := newTestLittleRed(testLRName, testNamespace)
	bigSpec.Spec.Sentinel = &littleredv1alpha1.SentinelSpec{DownAfterMilliseconds: 30000, FailoverTimeout: 180000}
	if got := buildSentinelLivenessProbe(bigSpec).FailureThreshold; got != 3 {
		t.Errorf("large sentinel timings: FailureThreshold = %d, want 3 (must not scale with timings)", got)
	}
}

func TestBuildSentinelLivenessProbeWithTLS(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.TLS.Enabled = true
	probe := buildSentinelLivenessProbe(lr)

	cmd := strings.Join(probe.Exec.Command, " ")
	if !strings.Contains(cmd, "--tls") {
		t.Errorf("sentinel liveness probe should include --tls flag, got: %s", cmd)
	}
}

func TestBuildSentinelReadinessProbe(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	probe := buildSentinelReadinessProbe(lr)

	if probe.Exec == nil {
		t.Fatal("sentinel readiness probe should use exec")
	}

	cmd := strings.Join(probe.Exec.Command, " ")
	if !strings.Contains(cmd, "info replication") {
		t.Errorf("probe command should query 'info replication', got: %s", cmd)
	}
	if !strings.Contains(cmd, "role:master") {
		t.Errorf("probe command should check role:master, got: %s", cmd)
	}
	if !strings.Contains(cmd, "master_link_status:up") {
		t.Errorf("probe command should check master_link_status:up, got: %s", cmd)
	}
	// Readiness probe exits 1 during bootstrap (opposite of liveness probe)
	if !strings.Contains(cmd, "exit 1") {
		t.Errorf("readiness probe should fail (exit 1) while bootstrap is in progress, got: %s", cmd)
	}
	// Readiness probe should NOT perform master-reachability check — its only job is
	// to remove traffic from a zombie replica quickly, not to keep the pod alive.
	if strings.Contains(cmd, "redis-cli -h \"$master_host\"") {
		t.Errorf("readiness probe should not check master reachability (that is the liveness probe's job), got: %s", cmd)
	}

	if probe.InitialDelaySeconds != 5 {
		t.Errorf("InitialDelaySeconds = %d, want 5", probe.InitialDelaySeconds)
	}
}

func TestBuildSentinelReadinessProbeWithTLS(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.TLS.Enabled = true
	probe := buildSentinelReadinessProbe(lr)

	cmd := strings.Join(probe.Exec.Command, " ")
	if !strings.Contains(cmd, "--tls") {
		t.Errorf("sentinel readiness probe should include --tls flag, got: %s", cmd)
	}
}

func TestBuildSentinelConfig(t *testing.T) {
	// The static sentinel.conf is intentionally minimal: sentinels start with no master
	// configured. The operator issues SENTINEL MONITOR at runtime (bootstrapSentinel /
	// Rule 0), so timing parameters (quorum, downAfterMs, failoverTimeout) are not baked
	// into the config file. IP-only mode (ADR-001) means resolve/announce-hostnames are
	// both set to "no".
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	lr.Spec.Sentinel = &littleredv1alpha1.SentinelSpec{
		Quorum:                2,
		DownAfterMilliseconds: 5000,
		FailoverTimeout:       60000,
	}
	config := buildSentinelConfig(lr)

	mustHave := []string{
		"port 26379",
		"sentinel resolve-hostnames no",
		"sentinel announce-hostnames no",
	}
	for _, s := range mustHave {
		if !strings.Contains(config, s) {
			t.Errorf("sentinel.conf missing %q\nGot:\n%s", s, config)
		}
	}

	// Static config must NOT contain a monitor stanza — that is issued at runtime.
	mustNotHave := []string{
		"sentinel monitor mymaster",
		"sentinel down-after-milliseconds",
		"sentinel failover-timeout",
		"resolve-hostnames yes",
		"announce-hostnames yes",
	}
	for _, s := range mustNotHave {
		if strings.Contains(config, s) {
			t.Errorf("sentinel.conf should not contain %q (must be configured at runtime)\nGot:\n%s", s, config)
		}
	}
}

func TestBuildSentinelConfigMap(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	cm := buildSentinelConfigMap(lr)

	if cm.Name != "my-cache-sentinel-config" {
		t.Errorf("ConfigMap name = %q, want %q", cm.Name, "my-cache-sentinel-config")
	}

	if _, ok := cm.Data["sentinel.conf"]; !ok {
		t.Error("ConfigMap missing sentinel.conf key")
	}

	if cm.Labels["app.kubernetes.io/component"] != ComponentSentinel {
		t.Error("ConfigMap missing component=sentinel label")
	}
}

func TestBuildRedisStatefulSetSentinel(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	sts := buildRedisStatefulSetSentinel(lr, littleredv1alpha1.SentinelRedisReplicas)

	// Check replicas (should be 3 for sentinel mode)
	if *sts.Spec.Replicas != 3 {
		t.Errorf("StatefulSet replicas = %d, want 3", *sts.Spec.Replicas)
	}

	// Check serviceName (should be headless replicas service)
	if sts.Spec.ServiceName != testReplicasName {
		t.Errorf("StatefulSet serviceName = %q, want %q", sts.Spec.ServiceName, testReplicasName)
	}

	// Check PodManagementPolicy
	if sts.Spec.PodManagementPolicy != "Parallel" {
		t.Errorf("PodManagementPolicy = %q, want Parallel", sts.Spec.PodManagementPolicy)
	}

	// Verify selector uses redisSelectorLabels
	if sts.Spec.Selector.MatchLabels["app.kubernetes.io/component"] != ComponentRedis {
		t.Error("StatefulSet selector should have component=redis")
	}

	// Check config hash annotation is present
	annotations := sts.Spec.Template.Annotations
	if annotations == nil {
		t.Fatal("StatefulSet pod template missing annotations")
	}
	if _, ok := annotations[AnnotationConfigHash]; !ok {
		t.Error("StatefulSet pod template missing config hash annotation")
	}
}

func TestBuildSentinelStatefulSet(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	sts := buildSentinelStatefulSet(lr, sentinelProcessReplicas)

	// Check name
	if sts.Name != testSentinelName {
		t.Errorf("StatefulSet name = %q, want %q", sts.Name, testSentinelName)
	}

	// Check replicas
	if *sts.Spec.Replicas != 3 {
		t.Errorf("StatefulSet replicas = %d, want 3", *sts.Spec.Replicas)
	}

	// Check serviceName
	if sts.Spec.ServiceName != testSentinelName {
		t.Errorf("StatefulSet serviceName = %q, want %q", sts.Spec.ServiceName, testSentinelName)
	}

	// Check containers: sentinel + exporter sidecar (metrics default-on)
	containers := sts.Spec.Template.Spec.Containers
	if len(containers) != 2 {
		t.Errorf("StatefulSet has %d containers, want 2 (sentinel + exporter)", len(containers))
	}
	if containers[0].Name != ComponentSentinel {
		t.Errorf("Container name = %q, want sentinel", containers[0].Name)
	}

	// Check sentinel port
	var hasSentinelPort bool
	for _, p := range containers[0].Ports {
		if p.Name == ComponentSentinel && p.ContainerPort == 26379 {
			hasSentinelPort = true
		}
	}
	if !hasSentinelPort {
		t.Error("Sentinel container missing port 26379")
	}

	// Check exporter sidecar scrapes the Sentinel port, not the Redis port
	exporter := containers[1]
	if exporter.Name != containerNameExporter {
		t.Fatalf("second container name = %q, want exporter", exporter.Name)
	}
	var exporterAddr string
	for _, env := range exporter.Env {
		if env.Name == envRedisAddr {
			exporterAddr = env.Value
		}
	}
	if exporterAddr != "redis://localhost:26379" {
		t.Errorf("exporter REDIS_ADDR = %q, want redis://localhost:26379", exporterAddr)
	}

	// Check config hash annotation is present
	annotations := sts.Spec.Template.Annotations
	if annotations == nil {
		t.Fatal("StatefulSet pod template missing annotations")
	}
	if _, ok := annotations[AnnotationConfigHash]; !ok {
		t.Error("StatefulSet pod template missing config hash annotation")
	}
}

func TestBuildSentinelStatefulSetWithoutMetrics(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	enabled := false
	lr.Spec.Metrics.Enabled = &enabled
	sts := buildSentinelStatefulSet(lr, sentinelProcessReplicas)

	// Should only have the sentinel container, no exporter sidecar
	containers := sts.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Errorf("StatefulSet has %d containers, want 1 (sentinel only)", len(containers))
	}
	if containers[0].Name != ComponentSentinel {
		t.Errorf("Container name = %q, want sentinel", containers[0].Name)
	}

	// Sentinel headless service should not expose a metrics port
	svc := buildSentinelHeadlessService(lr)
	for _, p := range svc.Spec.Ports {
		if p.Name == portNameMetrics {
			t.Error("Sentinel service should not have metrics port when metrics disabled")
		}
	}
	if svc.Annotations["prometheus.io/scrape"] != "" {
		t.Error("Sentinel service should not have scrape annotation when metrics disabled")
	}
}

// ============================================================================
// Cross-mode parity: pod scheduling passthrough (CLAUDE.md §7)
// ============================================================================

// TestStatefulSetBuildersPropagatePodTemplateScheduling asserts that every
// StatefulSet builder copies ALL of spec.podTemplate's scheduling fields onto
// the pod spec. The sentinel monitor STS once silently dropped
// TopologySpreadConstraints and PriorityClassName while the other three
// builders set them (a cross-mode-parity defect); this guards every builder
// against regressing that passthrough.
func TestStatefulSetBuildersPropagatePodTemplateScheduling(t *testing.T) {
	nodeSelector := map[string]string{"disktype": diskTypeSSD}
	tolerations := []corev1.Toleration{{
		Key:      "dedicated",
		Operator: corev1.TolerationOpEqual,
		Value:    "redis",
		Effect:   corev1.TaintEffectNoSchedule,
	}}
	affinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/os",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"linux"},
					}},
				}},
			},
		},
	}
	priorityClass := "high-priority"
	tsc := []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       corev1.LabelHostname,
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{labelAppInstance: testLRName},
		},
	}}

	applyPodTemplate := func(lr *littleredv1alpha1.LittleRed) {
		lr.Spec.PodTemplate.NodeSelector = nodeSelector
		lr.Spec.PodTemplate.Tolerations = tolerations
		lr.Spec.PodTemplate.Affinity = affinity
		lr.Spec.PodTemplate.PriorityClassName = priorityClass
		lr.Spec.PodTemplate.TopologySpreadConstraints = tsc
	}

	builders := []struct {
		name  string
		mode  string
		build func(*littleredv1alpha1.LittleRed) *appsv1.StatefulSet
	}{
		{"standalone", ModeStandalone, buildStatefulSet},
		{"sentinel-redis", ModeSentinel, func(lr *littleredv1alpha1.LittleRed) *appsv1.StatefulSet {
			return buildRedisStatefulSetSentinel(lr, littleredv1alpha1.SentinelRedisReplicas)
		}},
		{"sentinel-monitor", ModeSentinel, func(lr *littleredv1alpha1.LittleRed) *appsv1.StatefulSet {
			return buildSentinelStatefulSet(lr, sentinelProcessReplicas)
		}},
		{"cluster", ModeCluster, func(lr *littleredv1alpha1.LittleRed) *appsv1.StatefulSet {
			return buildClusterShardStatefulSet(lr, 0, nil)
		}},
	}

	for _, b := range builders {
		t.Run(b.name, func(t *testing.T) {
			lr := newTestLittleRed(testLRName, testNamespace)
			lr.Spec.Mode = b.mode
			applyPodTemplate(lr)
			spec := b.build(lr).Spec.Template.Spec

			if !reflect.DeepEqual(spec.NodeSelector, nodeSelector) {
				t.Errorf("NodeSelector = %v, want %v", spec.NodeSelector, nodeSelector)
			}
			if !reflect.DeepEqual(spec.Tolerations, tolerations) {
				t.Errorf("Tolerations = %v, want %v", spec.Tolerations, tolerations)
			}
			if !reflect.DeepEqual(spec.Affinity, affinity) {
				t.Errorf("Affinity = %v, want %v", spec.Affinity, affinity)
			}
			if spec.PriorityClassName != priorityClass {
				t.Errorf("PriorityClassName = %q, want %q", spec.PriorityClassName, priorityClass)
			}
			if !reflect.DeepEqual(spec.TopologySpreadConstraints, tsc) {
				t.Errorf("TopologySpreadConstraints = %v, want %v", spec.TopologySpreadConstraints, tsc)
			}
		})
	}
}

func TestBuildMasterService(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	svc := buildMasterService(lr)

	// Check name (same as standalone)
	if svc.Name != testLRName {
		t.Errorf("Service name = %q, want %q", svc.Name, testLRName)
	}

	// Check selector includes role=master
	if svc.Spec.Selector[LabelRole] != RoleMaster {
		t.Error("Master service selector should have role=master")
	}

	// Master service is role-scoped and must NOT expose metrics or scrape
	// annotations — the all-pods replicas headless service is the scrape target,
	// so the master is not scraped twice.
	for _, p := range svc.Spec.Ports {
		if p.Name == portNameMetrics {
			t.Error("Master service should not expose metrics port (avoid double-scrape of master)")
		}
	}
	if svc.Annotations["prometheus.io/scrape"] != "" {
		t.Error("Master service should not carry prometheus scrape annotation")
	}
}

func TestBuildReplicasHeadlessService(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	svc := buildReplicasHeadlessService(lr)

	// Check name
	if svc.Name != testReplicasName {
		t.Errorf("Service name = %q, want %q", svc.Name, testReplicasName)
	}

	// Check ClusterIP is None (headless)
	if svc.Spec.ClusterIP != serviceClusterNone {
		t.Errorf("Service ClusterIP = %q, want None", svc.Spec.ClusterIP)
	}

	// Check publishNotReadyAddresses
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("Headless service should publishNotReadyAddresses")
	}

	// Selects all redis data pods, so it carries the metrics port + scrape
	// annotations (metrics default-on) — this is the data-plane scrape target.
	var hasMetricsPort bool
	for _, p := range svc.Spec.Ports {
		if p.Name == portNameMetrics && p.Port == 9121 {
			hasMetricsPort = true
		}
	}
	if !hasMetricsPort {
		t.Error("Replicas headless service should expose metrics port 9121 when metrics enabled")
	}
	if svc.Annotations["prometheus.io/scrape"] != annotationValueTrue {
		t.Error("Replicas headless service missing prometheus.io/scrape annotation")
	}
}

func TestBuildReplicasHeadlessServiceWithoutMetrics(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	enabled := false
	lr.Spec.Metrics.Enabled = &enabled
	svc := buildReplicasHeadlessService(lr)

	for _, p := range svc.Spec.Ports {
		if p.Name == portNameMetrics {
			t.Error("Replicas headless service should not expose metrics port when metrics disabled")
		}
	}
	if svc.Annotations["prometheus.io/scrape"] != "" {
		t.Error("Replicas headless service should not carry scrape annotation when metrics disabled")
	}
}

func TestBuildSentinelHeadlessService(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	svc := buildSentinelHeadlessService(lr)

	// Check name
	if svc.Name != testSentinelName {
		t.Errorf("Service name = %q, want %q", svc.Name, testSentinelName)
	}

	// Check ClusterIP is None (headless)
	if svc.Spec.ClusterIP != serviceClusterNone {
		t.Errorf("Service ClusterIP = %q, want None", svc.Spec.ClusterIP)
	}

	// Check ports: sentinel + metrics (metrics default-on)
	var hasSentinelPort, hasMetricsPort bool
	for _, p := range svc.Spec.Ports {
		switch {
		case p.Name == ComponentSentinel && p.Port == 26379:
			hasSentinelPort = true
		case p.Name == portNameMetrics && p.Port == 9121:
			hasMetricsPort = true
		}
	}
	if !hasSentinelPort {
		t.Error("Sentinel service should have port 26379")
	}
	if !hasMetricsPort {
		t.Error("Sentinel service should expose metrics port 9121 when metrics enabled")
	}

	// Check Prometheus scrape annotations are present
	if svc.Annotations["prometheus.io/scrape"] != annotationValueTrue {
		t.Error("Sentinel service missing prometheus.io/scrape annotation")
	}
}

// ============================================================================
// ServiceMonitor Tests
// ============================================================================

func TestBuildServiceMonitor(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Metrics.ServiceMonitor.Enabled = true
	sm := buildServiceMonitor(lr)

	// Check name
	if sm.Name != testLRName {
		t.Errorf("ServiceMonitor name = %q, want %q", sm.Name, testLRName)
	}

	// Check namespace (defaults to LittleRed namespace)
	if sm.Namespace != testNamespace {
		t.Errorf("ServiceMonitor namespace = %q, want %q", sm.Namespace, testNamespace)
	}

	// Check endpoints
	if len(sm.Spec.Endpoints) != 1 {
		t.Errorf("ServiceMonitor has %d endpoints, want 1", len(sm.Spec.Endpoints))
	}
	if sm.Spec.Endpoints[0].Port != portNameMetrics {
		t.Errorf("ServiceMonitor endpoint port = %q, want %q", sm.Spec.Endpoints[0].Port, portNameMetrics)
	}
}

func TestBuildServiceMonitorWithCustomNamespace(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Metrics.ServiceMonitor.Enabled = true
	lr.Spec.Metrics.ServiceMonitor.Namespace = "monitoring"
	sm := buildServiceMonitor(lr)

	if sm.Namespace != "monitoring" {
		t.Errorf("ServiceMonitor namespace = %q, want monitoring", sm.Namespace)
	}
}

func TestBuildServiceMonitorWithCustomLabels(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Metrics.ServiceMonitor.Enabled = true
	lr.Spec.Metrics.ServiceMonitor.Labels = map[string]string{
		"release": "prometheus",
	}
	sm := buildServiceMonitor(lr)

	if sm.Labels["release"] != "prometheus" {
		t.Error("ServiceMonitor missing custom label 'release'")
	}
}

// ============================================================================
// Exporter Container Tests
// ============================================================================

func TestBuildExporterContainer(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	container := buildExporterContainer(lr, int32(littleredv1alpha1.RedisPort))

	// Check name
	if container.Name != containerNameExporter {
		t.Errorf("Container name = %q, want exporter", container.Name)
	}

	// Check image
	expectedImage := littleredv1alpha1.DefaultRegistry + "/" +
		littleredv1alpha1.DefaultExporterPath + ":" + littleredv1alpha1.DefaultExporterTag
	if container.Image != expectedImage {
		t.Errorf("Container image = %q, want %q", container.Image, expectedImage)
	}

	// Check REDIS_ADDR env var
	var hasRedisAddr bool
	for _, env := range container.Env {
		if env.Name == envRedisAddr {
			hasRedisAddr = true
			if env.Value != "redis://localhost:6379" {
				t.Errorf("REDIS_ADDR = %q, want redis://localhost:6379", env.Value)
			}
		}
	}
	if !hasRedisAddr {
		t.Error("Exporter container missing REDIS_ADDR env var")
	}

	// Check metrics port
	var hasMetricsPort bool
	for _, p := range container.Ports {
		if p.Name == portNameMetrics && p.ContainerPort == 9121 {
			hasMetricsPort = true
		}
	}
	if !hasMetricsPort {
		t.Error("Exporter container missing metrics port 9121")
	}
}

func TestBuildExporterContainerWithTLS(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.TLS.Enabled = true
	container := buildExporterContainer(lr, int32(littleredv1alpha1.RedisPort))

	// Check REDIS_ADDR uses rediss://
	var hasRedisAddr bool
	for _, env := range container.Env {
		if env.Name == envRedisAddr {
			hasRedisAddr = true
			if !strings.HasPrefix(env.Value, "rediss://") {
				t.Errorf("REDIS_ADDR = %q, should use rediss:// for TLS", env.Value)
			}
		}
	}
	if !hasRedisAddr {
		t.Error("Exporter container missing REDIS_ADDR env var")
	}
}

// ============================================================================
// PodDisruptionBudget Tests
// ============================================================================

func TestPDBNameHelpers(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)

	if got := podDisruptionBudgetName(lr); got != testPDBName {
		t.Errorf("podDisruptionBudgetName() = %q, want %q", got, testPDBName)
	}
	if got := sentinelPodDisruptionBudgetName(lr); got != "my-cache-sentinel-pdb" {
		t.Errorf("sentinelPodDisruptionBudgetName() = %q, want %q", got, "my-cache-sentinel-pdb")
	}
	if got := clusterPodDisruptionBudgetName(lr); got != "my-cache-cluster-pdb" {
		t.Errorf("clusterPodDisruptionBudgetName() = %q, want %q", got, "my-cache-cluster-pdb")
	}
}

func TestPDBSpec(t *testing.T) {
	tests := []struct {
		name               string
		setupPDB           func(*littleredv1alpha1.LittleRed)
		wantMaxUnavailable *intstr.IntOrString
		wantMinAvailable   *intstr.IntOrString
	}{
		{
			name:               "defaults to maxUnavailable=1 when nothing set",
			setupPDB:           func(lr *littleredv1alpha1.LittleRed) {},
			wantMaxUnavailable: new(intstr.FromInt32(1)),
			wantMinAvailable:   nil,
		},
		{
			name: "uses custom maxUnavailable",
			setupPDB: func(lr *littleredv1alpha1.LittleRed) {
				v := intstr.FromInt32(2)
				lr.Spec.PodDisruptionBudget.MaxUnavailable = &v
			},
			wantMaxUnavailable: new(intstr.FromInt32(2)),
			wantMinAvailable:   nil,
		},
		{
			name: "uses minAvailable when set",
			setupPDB: func(lr *littleredv1alpha1.LittleRed) {
				v := intstr.FromInt32(2)
				lr.Spec.PodDisruptionBudget.MinAvailable = &v
			},
			wantMaxUnavailable: nil,
			wantMinAvailable:   new(intstr.FromInt32(2)),
		},
		{
			name: "minAvailable takes precedence over maxUnavailable",
			setupPDB: func(lr *littleredv1alpha1.LittleRed) {
				min := intstr.FromInt32(2)
				max := intstr.FromInt32(1)
				lr.Spec.PodDisruptionBudget.MinAvailable = &min
				lr.Spec.PodDisruptionBudget.MaxUnavailable = &max
			},
			wantMaxUnavailable: nil,
			wantMinAvailable:   new(intstr.FromInt32(2)),
		},
		{
			name: "supports percentage for maxUnavailable",
			setupPDB: func(lr *littleredv1alpha1.LittleRed) {
				v := intstr.FromString("25%")
				lr.Spec.PodDisruptionBudget.MaxUnavailable = &v
			},
			wantMaxUnavailable: new(intstr.FromString("25%")),
			wantMinAvailable:   nil,
		},
		{
			name: "supports percentage for minAvailable",
			setupPDB: func(lr *littleredv1alpha1.LittleRed) {
				v := intstr.FromString("50%")
				lr.Spec.PodDisruptionBudget.MinAvailable = &v
			},
			wantMaxUnavailable: nil,
			wantMinAvailable:   new(intstr.FromString("50%")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := newTestLittleRed(testLRName, testNamespace)
			tt.setupPDB(lr)
			gotMax, gotMin := pdbSpec(lr)

			if tt.wantMaxUnavailable == nil && gotMax != nil {
				t.Errorf("MaxUnavailable = %v, want nil", gotMax)
			} else if tt.wantMaxUnavailable != nil {
				if gotMax == nil {
					t.Fatal("MaxUnavailable = nil, want non-nil")
				}
				if *gotMax != *tt.wantMaxUnavailable {
					t.Errorf("MaxUnavailable = %v, want %v", *gotMax, *tt.wantMaxUnavailable)
				}
			}

			if tt.wantMinAvailable == nil && gotMin != nil {
				t.Errorf("MinAvailable = %v, want nil", gotMin)
			} else if tt.wantMinAvailable != nil {
				if gotMin == nil {
					t.Fatal("MinAvailable = nil, want non-nil")
				}
				if *gotMin != *tt.wantMinAvailable {
					t.Errorf("MinAvailable = %v, want %v", *gotMin, *tt.wantMinAvailable)
				}
			}
		})
	}
}

func TestPdbEnabled(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }
	tests := []struct {
		name   string
		create *bool
		want   bool
	}{
		{"nil defaults to enabled", nil, true},
		{"explicit true", boolPtr(true), true},
		{"explicit false opts out", boolPtr(false), false},
	}
	r := &LittleRedReconciler{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := newTestLittleRed(testLRName, testNamespace)
			lr.Spec.PodDisruptionBudget.Create = tt.create
			if got := r.pdbEnabled(lr); got != tt.want {
				t.Errorf("pdbEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClusterHasReplicas(t *testing.T) {
	intPtr := func(i int) *int { return &i }
	tests := []struct {
		name    string
		cluster *littleredv1alpha1.ClusterSpec
		want    bool
	}{
		{"nil cluster defaults to redundant", nil, true},
		{"nil replicasPerShard defaults to redundant", &littleredv1alpha1.ClusterSpec{Shards: 3}, true},
		{"replicasPerShard 0 has no redundancy", &littleredv1alpha1.ClusterSpec{Shards: 3, ReplicasPerShard: intPtr(0)}, false},
		{"replicasPerShard 1 is redundant", &littleredv1alpha1.ClusterSpec{Shards: 3, ReplicasPerShard: intPtr(1)}, true},
		{"replicasPerShard 2 is redundant", &littleredv1alpha1.ClusterSpec{Shards: 3, ReplicasPerShard: intPtr(2)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := newTestLittleRed(testLRName, testNamespace)
			lr.Spec.Cluster = tt.cluster
			if got := clusterHasReplicas(lr); got != tt.want {
				t.Errorf("clusterHasReplicas() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildSentinelRedisPDB(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	pdb := buildSentinelRedisPDB(lr)

	if pdb.Name != testPDBName {
		t.Errorf("PDB name = %q, want %q", pdb.Name, testPDBName)
	}

	// Default: maxUnavailable=1
	if pdb.Spec.MaxUnavailable == nil {
		t.Fatal("PDB MaxUnavailable should be set by default")
	}
	if *pdb.Spec.MaxUnavailable != intstr.FromInt32(1) {
		t.Errorf("PDB MaxUnavailable = %v, want 1", *pdb.Spec.MaxUnavailable)
	}
	if pdb.Spec.MinAvailable != nil {
		t.Error("PDB MinAvailable should not be set by default")
	}

	if pdb.Spec.Selector.MatchLabels["app.kubernetes.io/component"] != ComponentRedis {
		t.Error("PDB selector should have component=redis")
	}
}

func TestBuildSentinelPDB(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeSentinel
	pdb := buildSentinelPDB(lr)

	if pdb.Name != "my-cache-sentinel-pdb" {
		t.Errorf("PDB name = %q, want %q", pdb.Name, "my-cache-sentinel-pdb")
	}

	// Default: maxUnavailable=1
	if pdb.Spec.MaxUnavailable == nil {
		t.Fatal("PDB MaxUnavailable should be set by default")
	}
	if *pdb.Spec.MaxUnavailable != intstr.FromInt32(1) {
		t.Errorf("PDB MaxUnavailable = %v, want 1", *pdb.Spec.MaxUnavailable)
	}
	if pdb.Spec.MinAvailable != nil {
		t.Error("PDB MinAvailable should not be set by default")
	}

	if pdb.Spec.Selector.MatchLabels["app.kubernetes.io/component"] != ComponentSentinel {
		t.Error("PDB selector should have component=sentinel")
	}
	if pdb.Spec.Selector.MatchLabels["app.kubernetes.io/instance"] != testLRName {
		t.Errorf("PDB selector instance = %q, want %q", pdb.Spec.Selector.MatchLabels["app.kubernetes.io/instance"], testLRName)
	}
}

func TestBuildClusterRedisConfig(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeCluster
	cfg := buildClusterRedisConfig(lr)

	// The operator is the sole topology authority (ADR-007): Redis must not autonomously
	// migrate replicas across shards, or it re-pairs a shard's master/replica across shard
	// StatefulSets and defeats per-shard failure-domain placement.
	if !strings.Contains(cfg, "cluster-allow-replica-migration no") {
		t.Errorf("cluster config must disable replica migration; got:\n%s", cfg)
	}
	if !strings.Contains(cfg, "cluster-enabled yes") {
		t.Error("cluster config must enable cluster mode")
	}
	// Persistence stays disabled (pillar 3.1).
	if !strings.Contains(cfg, "save \"\"") || !strings.Contains(cfg, "appendonly no") {
		t.Error("cluster config must keep persistence disabled")
	}
}

func TestBuildShardSpreadConstraint(t *testing.T) {
	// Unset knob → no operator constraint.
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeCluster
	if c := buildShardSpreadConstraint(lr, 1); c != nil {
		t.Errorf("no placement set: expected nil constraint, got %+v", c)
	}

	// Knob set → per-shard constraint scoped to that shard's pods.
	lr.Spec.Placement = &littleredv1alpha1.PlacementSpec{
		ShardAntiAffinity: &littleredv1alpha1.ShardAntiAffinitySpec{
			TopologyKey:       "topology.kubernetes.io/zone",
			WhenUnsatisfiable: corev1.DoNotSchedule,
		},
	}
	c := buildShardSpreadConstraint(lr, 2)
	if c == nil {
		t.Fatal("placement set: expected a constraint, got nil")
	}
	if c.MaxSkew != 1 {
		t.Errorf("MaxSkew = %d, want 1", c.MaxSkew)
	}
	if c.TopologyKey != "topology.kubernetes.io/zone" {
		t.Errorf("TopologyKey = %q, want zone", c.TopologyKey)
	}
	if c.WhenUnsatisfiable != corev1.DoNotSchedule {
		t.Errorf("WhenUnsatisfiable = %q, want DoNotSchedule", c.WhenUnsatisfiable)
	}
	if c.LabelSelector == nil || c.LabelSelector.MatchLabels[LabelShard] != "2" {
		t.Errorf("selector must scope to shard 2, got %+v", c.LabelSelector)
	}
	if c.LabelSelector.MatchLabels["app.kubernetes.io/component"] != ComponentCluster {
		t.Error("selector must carry component=cluster")
	}

	// Self-defaulting: empty fields fall back to the documented defaults.
	lr.Spec.Placement.ShardAntiAffinity = &littleredv1alpha1.ShardAntiAffinitySpec{}
	d := buildShardSpreadConstraint(lr, 0)
	if d == nil || d.TopologyKey != littleredv1alpha1.DefaultShardTopologyKey || d.WhenUnsatisfiable != corev1.ScheduleAnyway {
		t.Errorf("self-defaulting failed: got %+v", d)
	}
}

func TestValidatePlacementSpec(t *testing.T) {
	r := &LittleRedReconciler{}
	saa := func(when corev1.UnsatisfiableConstraintAction) *littleredv1alpha1.PlacementSpec {
		return &littleredv1alpha1.PlacementSpec{ShardAntiAffinity: &littleredv1alpha1.ShardAntiAffinitySpec{WhenUnsatisfiable: when}}
	}
	tests := []struct {
		name      string
		mode      string
		placement *littleredv1alpha1.PlacementSpec
		wantErr   bool
	}{
		{"nil placement", ModeCluster, nil, false},
		{"cluster + soft", ModeCluster, saa(corev1.ScheduleAnyway), false},
		{"cluster + hard", ModeCluster, saa(corev1.DoNotSchedule), false},
		{"cluster + empty when (defaulted later)", ModeCluster, saa(""), false},
		{"cluster + bogus when", ModeCluster, saa("Sometimes"), true},
		{"non-cluster mode rejected", ModeStandalone, saa(corev1.ScheduleAnyway), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := newTestLittleRed(testLRName, testNamespace)
			lr.Spec.Mode = tt.mode
			lr.Spec.Placement = tt.placement
			err := r.validatePlacementSpec(lr)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePlacementSpec() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClusterShardStatefulSetMergesShardSpread(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeCluster
	replicas := 1
	lr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{Shards: 3, ReplicasPerShard: &replicas}
	// A user-supplied constraint plus the operator knob.
	userTSC := corev1.TopologySpreadConstraint{
		MaxSkew:           2,
		TopologyKey:       "custom/key",
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	}
	lr.Spec.PodTemplate.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{userTSC}
	lr.Spec.Placement = &littleredv1alpha1.PlacementSpec{
		ShardAntiAffinity: &littleredv1alpha1.ShardAntiAffinitySpec{
			TopologyKey:       corev1.LabelHostname,
			WhenUnsatisfiable: corev1.DoNotSchedule,
		},
	}

	tsc := buildClusterShardStatefulSet(lr, 1, nil).Spec.Template.Spec.TopologySpreadConstraints
	if len(tsc) != 2 {
		t.Fatalf("expected 2 constraints (user + operator), got %d: %+v", len(tsc), tsc)
	}
	// Order: user first, operator's per-shard constraint appended.
	if !reflect.DeepEqual(tsc[0], userTSC) {
		t.Errorf("first constraint should be the user's, got %+v", tsc[0])
	}
	if tsc[1].TopologyKey != corev1.LabelHostname || tsc[1].LabelSelector.MatchLabels[LabelShard] != "1" {
		t.Errorf("second constraint should be the operator's shard-1 spread, got %+v", tsc[1])
	}
	// The user's spec slice must not be mutated by the merge.
	if len(lr.Spec.PodTemplate.TopologySpreadConstraints) != 1 {
		t.Error("merge must not mutate the user's spec.podTemplate.topologySpreadConstraints slice")
	}

	// Knob unset → only the user's constraints pass through (no operator injection).
	lr.Spec.Placement = nil
	plain := buildClusterShardStatefulSet(lr, 1, nil).Spec.Template.Spec.TopologySpreadConstraints
	if len(plain) != 1 {
		t.Errorf("without placement, expected only the user constraint, got %d", len(plain))
	}
}

func TestBuildClusterShardStatefulSet(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeCluster
	replicas := 1
	lr.Spec.Cluster = &littleredv1alpha1.ClusterSpec{Shards: 3, ReplicasPerShard: &replicas}

	sts := buildClusterShardStatefulSet(lr, 2, nil)

	// One StatefulSet per shard, named {name}-shard-K.
	if sts.Name != "my-cache-shard-2" {
		t.Errorf("STS name = %q, want %q", sts.Name, "my-cache-shard-2")
	}
	// Sized 1 + replicasPerShard (not the whole cluster).
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 2 {
		t.Errorf("STS replicas = %v, want 2", sts.Spec.Replicas)
	}
	// All shard StatefulSets share the one headless Service so peer discovery/DNS resolve.
	if sts.Spec.ServiceName != "my-cache-cluster" {
		t.Errorf("STS serviceName = %q, want %q (shared headless service)", sts.Spec.ServiceName, "my-cache-cluster")
	}
	// Stable per-shard identity label on selector, pod template, and STS metadata.
	if sts.Spec.Selector.MatchLabels[LabelShard] != "2" {
		t.Errorf("STS selector shard = %q, want %q", sts.Spec.Selector.MatchLabels[LabelShard], "2")
	}
	if sts.Spec.Template.Labels[LabelShard] != "2" {
		t.Errorf("pod template shard label = %q, want %q", sts.Spec.Template.Labels[LabelShard], "2")
	}
	if sts.Labels[LabelShard] != "2" {
		t.Errorf("STS metadata shard label = %q, want %q", sts.Labels[LabelShard], "2")
	}
	if sts.Spec.Template.Labels["app.kubernetes.io/component"] != ComponentCluster {
		t.Error("pod template should keep component=cluster so the shared Services select it")
	}
}

func TestBuildClusterShardPDB(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeCluster
	pdb := buildClusterShardPDB(lr, 1)

	if pdb.Name != "my-cache-shard-1-pdb" {
		t.Errorf("PDB name = %q, want %q", pdb.Name, "my-cache-shard-1-pdb")
	}
	if pdb.Namespace != testNamespace {
		t.Errorf("PDB namespace = %q, want %q", pdb.Namespace, testNamespace)
	}

	// Default: maxUnavailable=1
	if pdb.Spec.MaxUnavailable == nil {
		t.Fatal("PDB MaxUnavailable should be set by default")
	}
	if *pdb.Spec.MaxUnavailable != intstr.FromInt32(1) {
		t.Errorf("PDB MaxUnavailable = %v, want 1", *pdb.Spec.MaxUnavailable)
	}
	if pdb.Spec.MinAvailable != nil {
		t.Error("PDB MinAvailable should not be set by default")
	}

	if pdb.Spec.Selector.MatchLabels["app.kubernetes.io/component"] != ComponentCluster {
		t.Error("PDB selector should have component=cluster")
	}
	if pdb.Spec.Selector.MatchLabels["app.kubernetes.io/instance"] != testLRName {
		t.Errorf("PDB selector instance = %q, want %q", pdb.Spec.Selector.MatchLabels["app.kubernetes.io/instance"], testLRName)
	}
	// Per-shard PDB must be scoped to its shard so a drain can't take out a whole shard.
	if pdb.Spec.Selector.MatchLabels[LabelShard] != "1" {
		t.Errorf("PDB selector shard = %q, want %q", pdb.Spec.Selector.MatchLabels[LabelShard], "1")
	}
}

// --- cluster preStop last-copy self-fence (ADR-017 Decision 4, LR-047) -------
//
// HONEST TIER NOTE. This is a shell script embedded in a Go string: asserting the
// built script contains the fence is GREEN FROM BIRTH and can only ever be a
// structural guard against a future edit quietly deleting it. The behaviour — a
// write against the departing master failing -NOREPLICAS instead of being
// acknowledged and lost — is only observable live, and was validated on t3e with
// a `replicasPerShard: 0` cluster, where every master is by construction the last
// copy of its range. Teeth here were shown by mutation: restoring the old
// two-line branch body (the bare log + `exit 0`) fails the fence assertion, and
// hoisting the fence above the `if [ "$IS_MASTER" != "yes" ]` early exit fails
// the placement assertion.
func TestBuildClusterPreStopFencesLastCopy(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeCluster
	container := buildClusterRedisContainer(lr)

	if container.Lifecycle == nil || container.Lifecycle.PreStop == nil ||
		container.Lifecycle.PreStop.Exec == nil {
		t.Fatal("cluster container missing preStop exec hook")
	}
	script := strings.Join(container.Lifecycle.PreStop.Exec.Command, " ")

	const fence = "CONFIG SET min-replicas-to-write 99"
	if !strings.Contains(script, fence) {
		t.Fatalf("cluster preStop must self-fence on the last-copy branch; missing %q\n%s", fence, script)
	}

	// The fence is target-free (LR-038 Addendum 2): it must not need to know a
	// successor, because needing one reintroduces the race the fence removes.
	if strings.Contains(script, "min-replicas-to-write $") {
		t.Error("the fence must be target-free: no computed successor may appear in it")
	}

	// PLACEMENT. The fence belongs to the last-copy branch only. It must sit after
	// the master check (a replica has no slots to lose and must leave at once) and
	// after the replica lookup, inside the `-z "$REPLICA_IP"` branch — fencing a
	// master that DOES have a healthy replica would refuse writes for the whole
	// hand-over window that the CLUSTER FAILOVER below is there to make seamless.
	masterCheckAt := strings.Index(script, `if [ "$IS_MASTER" != "yes" ]`)
	lookupAt := strings.Index(script, "REPLICA_IP=$(redis-cli")
	branchAt := strings.Index(script, `if [ -z "$REPLICA_IP" ]`)
	failoverAt := strings.Index(script, "CLUSTER FAILOVER")
	fenceAt := strings.Index(script, fence)
	if masterCheckAt < 0 || lookupAt < 0 || branchAt < 0 || failoverAt < 0 {
		t.Fatalf("cluster preStop no longer has the expected shape:\n%s", script)
	}
	if !(masterCheckAt < lookupAt && lookupAt < branchAt && branchAt < fenceAt && fenceAt < failoverAt) {
		t.Errorf("fence must live inside the last-copy branch only "+
			"(masterCheck=%d lookup=%d branch=%d fence=%d failover=%d)",
			masterCheckAt, lookupAt, branchAt, fenceAt, failoverAt)
	}

	// A mitigation, not a refusal: the kubelet SIGKILLs at the grace period, so
	// blocking would only delay the loss and make every rollout pay the window.
	tail := script[fenceAt:failoverAt]
	if !strings.Contains(tail, "exit 0") {
		t.Error("the last-copy branch must still exit 0 after fencing — it is a mitigation, not a refusal")
	}
	if strings.Contains(tail, "sleep") || strings.Contains(tail, "while ") {
		t.Errorf("the last-copy branch must not wait after fencing:\n%s", tail)
	}

	// Loud: the operator has to be able to find this in the pod's logs afterwards.
	if !strings.Contains(script, "last copy of my slots") {
		t.Error("the last-copy branch must log loudly that this pod is the last copy")
	}
}

func TestBuildClusterPreStopFenceTLSFlags(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.Mode = ModeCluster
	plain := strings.Join(buildClusterRedisContainer(lr).Lifecycle.PreStop.Exec.Command, " ")
	if strings.Contains(plain, "--tls") {
		t.Error("cluster preStop must not pass --tls when TLS is disabled")
	}

	lr.Spec.TLS.Enabled = true
	secure := strings.Join(buildClusterRedisContainer(lr).Lifecycle.PreStop.Exec.Command, " ")
	fenceAt := strings.Index(secure, "CONFIG SET min-replicas-to-write 99")
	if fenceAt < 0 {
		t.Fatal("fence missing from the TLS-enabled cluster preStop")
	}
	// The fence command itself must carry the TLS flags, or it silently fails to
	// connect on a TLS instance and the branch degrades to today's silent loss.
	line := secure[strings.LastIndex(secure[:fenceAt], "\n")+1 : fenceAt]
	if !strings.Contains(line, "--tls") {
		t.Errorf("the fence command must pass TLS flags to redis-cli, got %q", line)
	}
}
