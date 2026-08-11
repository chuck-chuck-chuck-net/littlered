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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// testFailoverCMName is the expected ConfigMap name for the test instance.
const testFailoverCMName = testLRName + "-config"

// newFailoverTestLittleRed builds a defaulted failover-mode LittleRed. Mode is
// set before SetDefaults so the FailoverSpec defaulting path runs.
func newFailoverTestLittleRed() *littleredv1alpha1.LittleRed {
	lr := &littleredv1alpha1.LittleRed{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testLRName,
			Namespace: testNamespace,
		},
		Spec: littleredv1alpha1.LittleRedSpec{Mode: ModeFailover},
	}
	lr.SetDefaults()
	return lr
}

// failoverStartupScript extracts the startup script from the failover Redis
// container (Command = ["sh", "-c", script]).
func failoverStartupScript(t *testing.T, lr *littleredv1alpha1.LittleRed) string {
	t.Helper()
	container := buildRedisContainerFailover(lr)
	if len(container.Command) != 3 || container.Command[0] != "sh" || container.Command[1] != "-c" {
		t.Fatalf("failover container Command = %v, want [sh -c <script>]", container.Command)
	}
	return container.Command[2]
}

// ============================================================================
// Consts (data invariants — the ADR-011 wire contract with the operator)
// ============================================================================

func TestFailoverModeAndAnnotationConsts(t *testing.T) {
	if ModeFailover != "failover" {
		t.Errorf("ModeFailover = %q, want failover", ModeFailover)
	}
	tests := []struct{ got, want string }{
		{AnnotationAssignedRole, "redis.chuck-chuck-chuck.net/assigned-role"},
		{AnnotationAssignedMasterIP, "redis.chuck-chuck-chuck.net/assigned-master-ip"},
		{AnnotationAssignmentEpoch, "redis.chuck-chuck-chuck.net/assignment-epoch"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("annotation const = %q, want %q", tt.got, tt.want)
		}
	}
}

// ============================================================================
// StatefulSet
// ============================================================================

func TestBuildRedisStatefulSetFailover(t *testing.T) {
	lr := newFailoverTestLittleRed()
	sts := buildRedisStatefulSetFailover(lr)

	if sts.Name != testStatefulSetName {
		t.Errorf("StatefulSet name = %q, want %q", sts.Name, testStatefulSetName)
	}

	// Default failover replicas is 2 -> 1 master + 2 replicas = 3 pods.
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 3 {
		t.Errorf("StatefulSet replicas = %v, want 3 (1 + default 2)", sts.Spec.Replicas)
	}

	if sts.Spec.ServiceName != testReplicasName {
		t.Errorf("StatefulSet serviceName = %q, want %q", sts.Spec.ServiceName, testReplicasName)
	}

	if sts.Spec.PodManagementPolicy != "Parallel" {
		t.Errorf("PodManagementPolicy = %q, want Parallel", sts.Spec.PodManagementPolicy)
	}

	if sts.Spec.Selector == nil || sts.Spec.Selector.MatchLabels["app.kubernetes.io/component"] != ComponentRedis {
		t.Error("StatefulSet selector should have component=redis")
	}

	annotations := sts.Spec.Template.Annotations
	if annotations == nil {
		t.Fatal("StatefulSet pod template missing annotations")
	}
	if _, ok := annotations[AnnotationConfigHash]; !ok {
		t.Error("StatefulSet pod template missing config hash annotation")
	}

	// Metrics default on: redis + exporter sidecar.
	containers := sts.Spec.Template.Spec.Containers
	if len(containers) != 2 {
		t.Fatalf("StatefulSet has %d containers, want 2 (redis + exporter)", len(containers))
	}
	if containers[0].Name != ComponentRedis {
		t.Errorf("first container name = %q, want redis", containers[0].Name)
	}
}

func TestBuildRedisStatefulSetFailoverReplicaCount(t *testing.T) {
	lr := newFailoverTestLittleRed()
	lr.Spec.Failover.Replicas = new(int32(4))
	sts := buildRedisStatefulSetFailover(lr)
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 5 {
		t.Errorf("StatefulSet replicas = %v, want 5 (1 + 4)", sts.Spec.Replicas)
	}
}

func TestBuildRedisStatefulSetFailoverPropagatesScheduling(t *testing.T) {
	lr := newFailoverTestLittleRed()
	lr.Spec.PodTemplate.NodeSelector = map[string]string{"disktype": diskTypeSSD}
	sts := buildRedisStatefulSetFailover(lr)
	if sts.Spec.Template.Spec.NodeSelector["disktype"] != diskTypeSSD {
		t.Error("StatefulSet should propagate spec.podTemplate.nodeSelector")
	}
}

// ============================================================================
// Downward-API assignment volume (ADR-011 §3)
// ============================================================================

func TestFailoverDownwardAPIVolume(t *testing.T) {
	lr := newFailoverTestLittleRed()
	volumes := buildFailoverVolumes(lr)

	var podinfo *corev1.Volume
	for i := range volumes {
		if volumes[i].DownwardAPI != nil {
			podinfo = &volumes[i]
			break
		}
	}
	if podinfo == nil {
		t.Fatal("failover volumes missing a downward-API volume")
	}

	var annotationsFile *corev1.DownwardAPIVolumeFile
	for i := range podinfo.DownwardAPI.Items {
		item := &podinfo.DownwardAPI.Items[i]
		if item.Path == "annotations" {
			annotationsFile = item
		}
	}
	if annotationsFile == nil {
		t.Fatal("downward-API volume missing the 'annotations' item")
	}
	if annotationsFile.FieldRef == nil || annotationsFile.FieldRef.FieldPath != "metadata.annotations" {
		t.Errorf("annotations item fieldRef = %v, want metadata.annotations", annotationsFile.FieldRef)
	}

	// The redis container must mount it at the stable path the script polls.
	container := buildRedisContainerFailover(lr)
	var mounted bool
	for _, m := range container.VolumeMounts {
		if m.Name == podinfo.Name && m.MountPath == "/podinfo" {
			mounted = true
		}
	}
	if !mounted {
		t.Errorf("redis container does not mount the downward-API volume at /podinfo (mounts: %v)", container.VolumeMounts)
	}

	// Config and data volumes must still be present (shared plumbing).
	var hasConfig, hasData bool
	for _, v := range volumes {
		switch v.Name {
		case volNameConfig:
			hasConfig = true
		case volNameData:
			hasData = true
		}
	}
	if !hasConfig || !hasData {
		t.Errorf("failover volumes missing config/data (config=%v data=%v)", hasConfig, hasData)
	}
}

// ============================================================================
// Startup script (ADR-011 §3 protocol invariants)
// ============================================================================

func TestFailoverStartupScriptContent(t *testing.T) {
	lr := newFailoverTestLittleRed()
	script := failoverStartupScript(t, lr)

	mustContain := []string{
		// the assignment channel: all three annotation keys, read from the
		// downward-API file at its stable path
		AnnotationAssignedRole,
		AnnotationAssignedMasterIP,
		AnnotationAssignmentEpoch,
		"/podinfo/annotations",
		// the epoch gate: run-marker on the EmptyDir + numeric greater-than
		"/data/littlered-run-epoch",
		"-gt",
		// replica start path + IP identity
		"--replicaof",
		"--replica-announce-ip",
		// the distinctive kill-9 parked-state log line
		"already consumed",
		// probes' bootstrap guard marker
		"/data/bootstrap-in-progress",
	}
	for _, want := range mustContain {
		if !strings.Contains(script, want) {
			t.Errorf("startup script missing %q", want)
		}
	}

	mustNotContain := []string{
		// no Sentinel anywhere in this mode
		"sentinel get-master-addr-by-name",
		"26379",
		// no reachability PING / no Redis queries at all before starting
		// (ADR-002): the script only reads the annotations file and execs.
		"redis-cli",
	}
	for _, banned := range mustNotContain {
		if strings.Contains(script, banned) {
			t.Errorf("startup script must not contain %q", banned)
		}
	}
}

func TestFailoverPreStopHook(t *testing.T) {
	lr := newFailoverTestLittleRed()
	container := buildRedisContainerFailover(lr)

	if container.Lifecycle == nil || container.Lifecycle.PreStop == nil || container.Lifecycle.PreStop.Exec == nil {
		t.Fatal("failover container missing preStop exec hook")
	}
	hook := strings.Join(container.Lifecycle.PreStop.Exec.Command, " ")
	if !strings.Contains(hook, "sleep") {
		t.Errorf("preStop hook should just sleep (operator-led handover, ADR-011 §7), got %q", hook)
	}
	// Do NOT port the sentinel-mode `SENTINEL failover` preStop.
	if strings.Contains(hook, "SENTINEL") || strings.Contains(hook, "sentinel") {
		t.Errorf("preStop hook must not talk to Sentinel, got %q", hook)
	}
}

// ============================================================================
// redis.conf / ConfigMap
// ============================================================================

func TestBuildRedisConfigFailover(t *testing.T) {
	lr := newFailoverTestLittleRed()
	config := buildRedisConfigFailover(lr)

	mustContain := []string{
		"save \"\"",
		"appendonly no",
		"replica-serve-stale-data yes",
		"replica-read-only yes",
		"repl-diskless-sync yes",
	}
	for _, want := range mustContain {
		if !strings.Contains(config, want) {
			t.Errorf("failover redis.conf missing %q", want)
		}
	}

	if strings.Contains(strings.ToLower(config), "sentinel") {
		t.Error("failover redis.conf must not contain anything sentinel-specific")
	}

	// minReplicasToWrite defaults to 0 -> directive omitted entirely.
	if strings.Contains(config, "min-replicas-to-write") {
		t.Error("min-replicas-to-write must be omitted when spec.failover.minReplicasToWrite is 0")
	}
}

func TestBuildRedisConfigFailoverMinReplicasToWrite(t *testing.T) {
	lr := newFailoverTestLittleRed()
	lr.Spec.Failover.MinReplicasToWrite = 1
	config := buildRedisConfigFailover(lr)
	if !strings.Contains(config, "min-replicas-to-write 1") {
		t.Error("failover redis.conf missing 'min-replicas-to-write 1'")
	}
}

func TestBuildConfigMapFailoverMode(t *testing.T) {
	lr := newFailoverTestLittleRed()
	cm := buildConfigMapFailoverMode(lr)

	if cm.Name != testFailoverCMName {
		t.Errorf("ConfigMap name = %q, want %q", cm.Name, testFailoverCMName)
	}
	if _, ok := cm.Data[fileRedisConf]; !ok {
		t.Error("ConfigMap missing redis.conf key")
	}
	if cm.Labels["app.kubernetes.io/component"] != ComponentRedis {
		t.Error("ConfigMap missing component=redis label")
	}
}

// ============================================================================
// Probes (LR-016: liveness local-only; readiness link-gated)
// ============================================================================

func TestBuildFailoverLivenessProbe(t *testing.T) {
	lr := newFailoverTestLittleRed()
	probe := buildFailoverLivenessProbe(lr)
	if probe == nil || probe.Exec == nil {
		t.Fatal("failover liveness probe missing exec handler")
	}
	cmd := strings.Join(probe.Exec.Command, " ")

	if !strings.Contains(cmd, "ping") {
		t.Error("liveness probe should be a plain local PING")
	}
	if !strings.Contains(cmd, "/data/bootstrap-in-progress") {
		t.Error("liveness probe missing the bootstrap guard")
	}
	// LR-016: NO topology logic — never restart a replica for its master state.
	for _, banned := range []string{"info replication", "master_link_status", "role:"} {
		if strings.Contains(cmd, banned) {
			t.Errorf("liveness probe must not contain topology logic (%q found)", banned)
		}
	}
}

func TestBuildFailoverReadinessProbe(t *testing.T) {
	lr := newFailoverTestLittleRed()
	probe := buildFailoverReadinessProbe(lr)
	if probe == nil || probe.Exec == nil {
		t.Fatal("failover readiness probe missing exec handler")
	}
	cmd := strings.Join(probe.Exec.Command, " ")

	// Master passes on role:master; a replica requires link:up so a
	// masterless/mis-pointed replica is pulled from traffic without being killed.
	if !strings.Contains(cmd, "role:master") {
		t.Error("readiness probe should pass a master on role:master")
	}
	if !strings.Contains(cmd, "master_link_status:up") {
		t.Error("readiness probe should gate replicas on master_link_status:up")
	}
	if !strings.Contains(cmd, "/data/bootstrap-in-progress") {
		t.Error("readiness probe missing the bootstrap guard (not ready while parked)")
	}
}

// ============================================================================
// PDB + Services
// ============================================================================

func TestBuildFailoverRedisPDB(t *testing.T) {
	lr := newFailoverTestLittleRed()
	pdb := buildFailoverRedisPDB(lr)

	if pdb.Name != testPDBName {
		t.Errorf("PDB name = %q, want %q", pdb.Name, testPDBName)
	}
	if pdb.Spec.Selector == nil || pdb.Spec.Selector.MatchLabels["app.kubernetes.io/component"] != ComponentRedis {
		t.Error("PDB selector should target the redis data pods")
	}
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 {
		t.Errorf("PDB maxUnavailable = %v, want 1", pdb.Spec.MaxUnavailable)
	}
}

// The master + replicas Services are reused verbatim from sentinel mode (they
// are mode-agnostic: label-selector routing only). These assertions document
// that the failover data pods are selectable by them — the pod labels the
// failover StatefulSet stamps must match what the Services select.
func TestFailoverServicesSelectPods(t *testing.T) {
	lr := newFailoverTestLittleRed()

	sts := buildRedisStatefulSetFailover(lr)
	podLabels := sts.Spec.Template.Labels

	// Master service: selects component=redis + role=master. The role label is
	// stamped by the operator at runtime; everything else must match already.
	master := buildMasterService(lr)
	for k, v := range master.Spec.Selector {
		if k == LabelRole {
			if v != RoleMaster {
				t.Errorf("master service role selector = %q, want master", v)
			}
			continue
		}
		if podLabels[k] != v {
			t.Errorf("failover pod labels do not satisfy master service selector %s=%s (pod has %q)", k, v, podLabels[k])
		}
	}

	// Replicas headless service: selects all data pods.
	replicas := buildReplicasHeadlessService(lr)
	if replicas.Spec.ClusterIP != serviceClusterNone {
		t.Error("replicas service should be headless")
	}
	for k, v := range replicas.Spec.Selector {
		if podLabels[k] != v {
			t.Errorf("failover pod labels do not satisfy replicas service selector %s=%s (pod has %q)", k, v, podLabels[k])
		}
	}
}
