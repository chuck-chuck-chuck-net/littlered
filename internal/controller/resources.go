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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"text/template"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// Annotation keys for config hash
const (
	AnnotationConfigHash              = "redis.chuck-chuck-chuck.net/config-hash"
	AnnotationPodSpecHash             = "redis.chuck-chuck-chuck.net/pod-spec-hash"
	AnnotationDisablePolling          = "redis.chuck-chuck-chuck.net/disable-polling"
	AnnotationDisableEventMonitoring  = "redis.chuck-chuck-chuck.net/disable-event-monitoring"
	AnnotationDebugSkipSlotAssignment = "redis.chuck-chuck-chuck.net/debug-skip-slot-assignment"

	// Failover-mode assignment channel (ADR-011 §3): the operator stamps these
	// annotations on each data pod; the pod reads them back through a
	// downward-API volume and starts redis-server only on a fresh assignment.
	AnnotationAssignedRole     = "redis.chuck-chuck-chuck.net/assigned-role"
	AnnotationAssignedMasterIP = "redis.chuck-chuck-chuck.net/assigned-master-ip"
	AnnotationAssignmentEpoch  = "redis.chuck-chuck-chuck.net/assignment-epoch"
	// AnnotationMasterStartAuthorizedEpoch is the operator's explicit permission
	// for a pod to *start* a fresh Redis process as master (LR-038). It is
	// deliberately separate from AnnotationAssignedRole, which conflates two jobs:
	// a boot instruction ("start as master") and a standing intent record ("you
	// are the master"). For a pod that has died those must not be the same thing —
	// an in-place promotion advances the assignment epoch without changing the
	// process incarnation, so a restarted pod cannot tell a pre-death instruction
	// from a fresh one by ordering alone. The operator stamps this ONLY after it
	// has observed the restart and decided no data is at risk, so it cannot exist
	// before the death it refers to.
	AnnotationMasterStartAuthorizedEpoch = "redis.chuck-chuck-chuck.net/master-start-authorized-epoch"
)

// Resource name helpers
func configMapName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-config", lr.Name)
}

func sentinelConfigMapName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-sentinel-config", lr.Name)
}

func statefulSetName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-redis", lr.Name)
}

func sentinelStatefulSetName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-sentinel", lr.Name)
}

func serviceName(lr *littleredv1alpha1.LittleRed) string {
	return lr.Name
}

func replicasServiceName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-replicas", lr.Name)
}

func sentinelServiceName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-sentinel", lr.Name)
}

func clusterStatefulSetName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-cluster", lr.Name)
}

func clusterHeadlessServiceName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-cluster", lr.Name)
}

func serviceMonitorName(lr *littleredv1alpha1.LittleRed) string {
	return lr.Name
}

func podDisruptionBudgetName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-redis-pdb", lr.Name)
}

func sentinelPodDisruptionBudgetName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-sentinel-pdb", lr.Name)
}

func clusterPodDisruptionBudgetName(lr *littleredv1alpha1.LittleRed) string {
	return fmt.Sprintf("%s-cluster-pdb", lr.Name)
}

// Label keys
const (
	ComponentRedis        = "redis"
	ComponentSentinel     = "sentinel"
	ComponentCluster      = "cluster"
	LabelRole             = "redis.chuck-chuck-chuck.net/role"
	LabelShard            = "redis.chuck-chuck-chuck.net/shard"
	ModeStandalone        = "standalone"
	ModeSentinel          = "sentinel"
	ModeCluster           = "cluster"
	ModeFailover          = "failover"
	RoleMaster            = "master"
	RoleReplica           = "replica"
	RoleOrphan            = "orphan"
	RoleUndefined         = "undefined"
	annotationValueTrue   = "true"
	tlsInsecureFlags      = "--tls --insecure"
	portNameMetrics       = "metrics"
	containerNameExporter = "exporter"
	pathMetrics           = "/metrics"

	labelAppName      = "app.kubernetes.io/name"
	labelAppInstance  = "app.kubernetes.io/instance"
	fileRedisConf     = "redis.conf"
	volNameConfig     = "config"
	volNameData       = "data"
	volNameTLS        = "tls"
	volNameCACert     = "ca-cert"
	mountPathTLS      = "/tls"
	mountPathTLSCA    = "/tls-ca"
	capAll            = "ALL"
	envRedisPassword  = "REDIS_PASSWORD"
	envRedisAddr      = "REDIS_ADDR"
	envPodIP          = "POD_IP"
	fieldPathPodIP    = "status.podIP"
	secretKeyPassword = "password"
	appName           = "littlered"

	labelAppComponent  = "app.kubernetes.io/component"
	mountPathEtcRedis  = "/etc/redis"
	mountPathDataDir   = "/data"
	serviceClusterNone = "None"
	binRedisCli        = "redis-cli"
	// bootstrapGuardScript short-circuits a probe with success while the
	// bootstrap-in-progress marker exists, so K8s does not kill a pod before
	// operator authorization completes.
	bootstrapGuardScript = "if [ -f /data/bootstrap-in-progress ]; then exit 0; fi;"
)

// computeConfigHash computes a SHA256 hash of the ConfigMap data
// This is used to trigger pod restarts when config changes
func computeConfigHash(data map[string]string) string {
	h := sha256.New()
	// Sort keys for deterministic output
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte(data[k]))
	}
	return hex.EncodeToString(h.Sum(nil))[:16] // Use first 16 chars for brevity
}

// computePodTemplateHash returns a stable 16-char hash of the operator-authored pod
// template. Stamped as AnnotationPodSpecHash, it lets the serialized cluster rollout
// (LR-021, reconcileClusterStatefulSet) detect — cache-safely and without diffing
// server-defaulted fields — whether a shard's desired template differs from what is already
// applied. It is computed before the hash annotation itself is added, so it never depends
// on its own value.
func computePodTemplateHash(tmpl *corev1.PodTemplateSpec) string {
	raw, err := json.Marshal(tmpl)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:16]
}

// commonLabels returns the standard labels applied to all resources
func commonLabels(lr *littleredv1alpha1.LittleRed) map[string]string {
	return map[string]string{
		labelAppName:                       appName,
		labelAppInstance:                   lr.Name,
		"app.kubernetes.io/managed-by":     "littlered-operator",
		"app.kubernetes.io/version":        lr.Spec.Image.Tag,
		"redis.chuck-chuck-chuck.net/mode": lr.Spec.Mode,
	}
}

// selectorLabels returns labels used for selecting pods
func selectorLabels(lr *littleredv1alpha1.LittleRed) map[string]string {
	return map[string]string{
		labelAppName:     appName,
		labelAppInstance: lr.Name,
	}
}

// redisSelectorLabels returns labels for selecting Redis pods
func redisSelectorLabels(lr *littleredv1alpha1.LittleRed) map[string]string {
	return map[string]string{
		labelAppName:      appName,
		labelAppInstance:  lr.Name,
		labelAppComponent: ComponentRedis,
	}
}

// sentinelSelectorLabels returns labels for selecting Sentinel pods
func sentinelSelectorLabels(lr *littleredv1alpha1.LittleRed) map[string]string {
	return map[string]string{
		labelAppName:      appName,
		labelAppInstance:  lr.Name,
		labelAppComponent: ComponentSentinel,
	}
}

// masterSelectorLabels returns labels for selecting the master pod
func masterSelectorLabels(lr *littleredv1alpha1.LittleRed) map[string]string {
	return map[string]string{
		labelAppName:      appName,
		labelAppInstance:  lr.Name,
		labelAppComponent: ComponentRedis,
		LabelRole:         RoleMaster,
	}
}

// clusterSelectorLabels returns labels for selecting cluster pods. It is shard-agnostic
// and is used by the shared headless/client Services and cross-shard concerns, which must
// front every shard's pods.
func clusterSelectorLabels(lr *littleredv1alpha1.LittleRed) map[string]string {
	return map[string]string{
		labelAppName:      appName,
		labelAppInstance:  lr.Name,
		labelAppComponent: ComponentCluster,
	}
}

// clusterShardSelectorLabels returns labels for selecting the pods of a single shard:
// clusterSelectorLabels plus the stable per-shard identity label. Each shard StatefulSet
// selects on exactly these, so it owns only its own pods and a per-shard
// topologySpreadConstraint can be scoped to same-shard pods. See ADR per-shard StatefulSets.
func clusterShardSelectorLabels(lr *littleredv1alpha1.LittleRed, shardIdx int) map[string]string {
	labels := clusterSelectorLabels(lr)
	labels[LabelShard] = clusterShardLabelValue(shardIdx)
	return labels
}

// buildConfigMap creates the ConfigMap for redis.conf
func buildConfigMap(lr *littleredv1alpha1.LittleRed) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(lr),
			Namespace: lr.Namespace,
			Labels:    commonLabels(lr),
		},
		Data: map[string]string{
			fileRedisConf: buildRedisConfig(lr),
		},
	}
}

// buildRedisConfig generates the redis.conf content
func buildRedisConfig(lr *littleredv1alpha1.LittleRed) string {
	var sb strings.Builder

	// Basic settings
	sb.WriteString("# LittleRed generated configuration\n")
	sb.WriteString("bind 0.0.0.0\n")
	fmt.Fprintf(&sb, "port %d\n", littleredv1alpha1.RedisPort)
	sb.WriteString("dir /data\n")

	// Disable persistence (in-memory only)
	sb.WriteString("\n# Persistence disabled (in-memory mode)\n")
	sb.WriteString("save \"\"\n")
	sb.WriteString("appendonly no\n")

	// Memory settings
	sb.WriteString("\n# Memory configuration\n")
	maxmemory := lr.CalculateMaxmemory()
	fmt.Fprintf(&sb, "maxmemory %s\n", maxmemory)
	fmt.Fprintf(&sb, "maxmemory-policy %s\n", lr.GetEffectiveMaxmemoryPolicy())

	// Timeout settings
	sb.WriteString("\n# Connection settings\n")
	fmt.Fprintf(&sb, "timeout %d\n", lr.Spec.Config.Timeout)
	fmt.Fprintf(&sb, "tcp-keepalive %d\n", lr.Spec.Config.TCPKeepalive)

	// TLS settings
	if lr.Spec.TLS.Enabled {
		sb.WriteString("\n# TLS configuration\n")
		fmt.Fprintf(&sb, "tls-port %d\n", littleredv1alpha1.RedisPort)
		sb.WriteString("port 0\n") // Disable non-TLS port
		sb.WriteString("tls-cert-file /tls/tls.crt\n")
		sb.WriteString("tls-key-file /tls/tls.key\n")
		if lr.Spec.TLS.CACertSecret != "" {
			if lr.Spec.TLS.CACertSecret == lr.Spec.TLS.ExistingSecret {
				sb.WriteString("tls-ca-cert-file /tls/ca.crt\n")
			} else {
				sb.WriteString("tls-ca-cert-file /tls-ca/ca.crt\n")
			}
		}
		if lr.Spec.TLS.ClientAuth {
			sb.WriteString("tls-auth-clients yes\n")
		} else {
			sb.WriteString("tls-auth-clients no\n")
		}
	}

	// Auth settings (requirepass is set via command line args to reference secret)
	// The actual password is mounted as env var and used with --requirepass

	// Raw config (expert mode)
	if lr.Spec.Config.Raw != "" {
		sb.WriteString("\n# Custom configuration\n")
		sb.WriteString(lr.Spec.Config.Raw)
		if !strings.HasSuffix(lr.Spec.Config.Raw, "\n") {
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

// buildStatefulSet creates the StatefulSet for Redis
func buildStatefulSet(lr *littleredv1alpha1.LittleRed) *appsv1.StatefulSet {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentRedis

	podLabels := make(map[string]string)
	maps.Copy(podLabels, selectorLabels(lr))
	podLabels[labelAppComponent] = ComponentRedis
	// Add user-defined pod labels
	maps.Copy(podLabels, lr.Spec.PodTemplate.Labels)

	// Compute config hash for pod annotations to trigger rolling update on config change
	configData := map[string]string{fileRedisConf: buildRedisConfig(lr)}
	configHash := computeConfigHash(configData)

	podAnnotations := make(map[string]string)
	maps.Copy(podAnnotations, lr.Spec.PodTemplate.Annotations)
	podAnnotations[AnnotationConfigHash] = configHash

	replicas := int32(1)

	containers := []corev1.Container{buildRedisContainer(lr)}

	// Add exporter sidecar if metrics enabled
	if lr.Spec.Metrics.IsEnabled() {
		containers = append(containers, buildExporterContainer(lr, int32(littleredv1alpha1.RedisPort)))
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName(lr),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: serviceName(lr),
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels(lr),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					SecurityContext:           lr.Spec.PodTemplate.SecurityContext,
					Containers:                containers,
					Volumes:                   buildVolumes(lr),
					NodeSelector:              lr.Spec.PodTemplate.NodeSelector,
					Tolerations:               lr.Spec.PodTemplate.Tolerations,
					Affinity:                  lr.Spec.PodTemplate.Affinity,
					PriorityClassName:         lr.Spec.PodTemplate.PriorityClassName,
					TopologySpreadConstraints: lr.Spec.PodTemplate.TopologySpreadConstraints,
					ImagePullSecrets:          lr.Spec.Image.PullSecrets,
				},
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.StatefulSetUpdateStrategyType(lr.Spec.UpdateStrategy.Type),
			},
		},
	}

	return sts
}

// buildRedisContainer creates the main Redis container
func buildRedisContainer(lr *littleredv1alpha1.LittleRed) corev1.Container {
	args := []string{
		"/etc/redis/redis.conf",
	}

	container := corev1.Container{
		Name:            ComponentRedis,
		Image:           lr.Spec.Image.FullImage(),
		ImagePullPolicy: lr.Spec.Image.PullPolicy,
		Args:            args,
		Ports: []corev1.ContainerPort{
			{
				Name:          ComponentRedis,
				ContainerPort: int32(littleredv1alpha1.RedisPort),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Resources: lr.Spec.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      volNameConfig,
				MountPath: mountPathEtcRedis,
				ReadOnly:  true,
			},
			{
				Name:      volNameData,
				MountPath: mountPathDataDir,
			},
		},
		LivenessProbe:  buildLivenessProbe(lr),
		ReadinessProbe: buildReadinessProbe(lr),
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(true),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{capAll},
			},
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
	}

	// Add TLS volume mounts
	if lr.Spec.TLS.Enabled {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volNameTLS,
			MountPath: mountPathTLS,
			ReadOnly:  true,
		})
		if lr.Spec.TLS.CACertSecret != "" && lr.Spec.TLS.CACertSecret != lr.Spec.TLS.ExistingSecret {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      volNameCACert,
				MountPath: mountPathTLSCA,
				ReadOnly:  true,
			})
		}
	}

	// Add auth env var
	if lr.Spec.Auth.Enabled {
		container.Env = append(container.Env, corev1.EnvVar{
			Name: envRedisPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: lr.Spec.Auth.ExistingSecret,
					},
					Key: secretKeyPassword,
				},
			},
		})
		// Add requirepass to args
		container.Args = append(container.Args, "--requirepass", "$(REDIS_PASSWORD)")
	}

	return container
}

// buildExporterContainer creates a redis_exporter sidecar that scrapes the
// node listening on targetPort. Redis data nodes use RedisPort; Sentinel nodes
// use SentinelPort — oliver006/redis_exporter detects a Sentinel endpoint and
// exposes redis_sentinel_* series. The exporter itself always listens on
// RedisExporterPort regardless of the target.
func buildExporterContainer(lr *littleredv1alpha1.LittleRed, targetPort int32) corev1.Container {
	env := []corev1.EnvVar{
		{
			Name:  envRedisAddr,
			Value: fmt.Sprintf("redis://localhost:%d", targetPort),
		},
	}

	// Add password env if auth enabled
	if lr.Spec.Auth.Enabled {
		env = append(env, corev1.EnvVar{
			Name: envRedisPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: lr.Spec.Auth.ExistingSecret,
					},
					Key: secretKeyPassword,
				},
			},
		})
	}

	// Handle TLS
	if lr.Spec.TLS.Enabled {
		env[0].Value = fmt.Sprintf("rediss://localhost:%d", littleredv1alpha1.RedisPort)
		env = append(env,
			corev1.EnvVar{Name: "REDIS_EXPORTER_SKIP_TLS_VERIFICATION", Value: annotationValueTrue},
		)
	}

	container := corev1.Container{
		Name:            containerNameExporter,
		Image:           lr.Spec.Metrics.Exporter.FullImage(lr.Spec.Image.Registry),
		ImagePullPolicy: lr.Spec.Image.PullPolicy,
		Env:             env,
		Ports: []corev1.ContainerPort{
			{
				Name:          portNameMetrics,
				ContainerPort: int32(littleredv1alpha1.RedisExporterPort),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Resources: lr.Spec.Metrics.Exporter.Resources,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(true),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{capAll},
			},
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
	}

	if lr.Spec.TLS.Enabled {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volNameTLS,
			MountPath: mountPathTLS,
			ReadOnly:  true,
		})
		if lr.Spec.TLS.CACertSecret != "" && lr.Spec.TLS.CACertSecret != lr.Spec.TLS.ExistingSecret {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      volNameCACert,
				MountPath: mountPathTLSCA,
				ReadOnly:  true,
			})
		}
	}

	return container
}

// buildVolumes creates the volumes for the pod
func buildVolumes(lr *littleredv1alpha1.LittleRed) []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: volNameConfig,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configMapName(lr),
					},
				},
			},
		},
		{
			Name: volNameData,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	// Add TLS volumes
	if lr.Spec.TLS.Enabled {
		volumes = append(volumes, corev1.Volume{
			Name: volNameTLS,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: lr.Spec.TLS.ExistingSecret,
				},
			},
		})
		if lr.Spec.TLS.CACertSecret != "" {
			volumes = append(volumes, corev1.Volume{
				Name: volNameCACert,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: lr.Spec.TLS.CACertSecret,
					},
				},
			})
		}
	}

	return volumes
}

// buildLivenessProbe creates the liveness probe for Redis
func buildLivenessProbe(lr *littleredv1alpha1.LittleRed) *corev1.Probe {
	// While the bootstrap-in-progress file exists, we report success to avoid being killed
	// by K8s before authorization is complete.
	cmd := []string{
		bootstrapGuardScript,
		binRedisCli,
	}
	if lr.Spec.Auth.Enabled {
		cmd = append(cmd, "-a", "$(REDIS_PASSWORD)", "--no-auth-warning")
	}
	if lr.Spec.TLS.Enabled {
		cmd = append(cmd, "--tls", "--insecure")
	}
	cmd = append(cmd, "-t", "2", "ping")

	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"sh", "-c",
					strings.Join(cmd, " "),
				},
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    3,
	}
}

// buildReadinessProbe creates the readiness probe for Redis
func buildReadinessProbe(lr *littleredv1alpha1.LittleRed) *corev1.Probe {
	// While bootstrapping, we are NOT ready.
	cmd := []string{
		"if [ -f /data/bootstrap-in-progress ]; then exit 1; fi;",
		binRedisCli,
	}
	if lr.Spec.Auth.Enabled {
		cmd = append(cmd, "-a", "$(REDIS_PASSWORD)", "--no-auth-warning")
	}
	if lr.Spec.TLS.Enabled {
		cmd = append(cmd, "--tls", "--insecure")
	}
	cmd = append(cmd, "-t", "2", "ping")

	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"sh", "-c",
					strings.Join(cmd, " "),
				},
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      3,
		FailureThreshold:    3,
	}
}

// buildSentinelLivenessProbe creates the liveness probe for the Redis container in sentinel mode.
//
// It is a plain local health check — the bootstrap guard plus a local PING — identical in spirit
// to standalone (buildLivenessProbe) and cluster mode. The probe deliberately makes NO topology
// decisions: a replica whose master is unreachable is healthy-and-waiting, not a candidate for
// restart.
//
// This is deliberate (LR-016, superseding the ADR-003 2026-02-19 "zombie replica" amendment). A
// pod's local INFO cannot distinguish a zombie following a ghost master (a real master exists
// elsewhere — the operator's Rule R redirects it via SLAVEOF) from a leaderless survivor (no
// master exists — Rule L preserves/promotes it): both read role:slave + link:down + master
// unreachable. Restarting on that ambiguous signal wipes the survivor's data (storage is
// EmptyDir), destroying exactly what Rule L exists to preserve. Topology repair is therefore
// operator-owned; the readiness probe (buildSentinelReadinessProbe) still removes a masterless
// replica from client traffic without killing it.
func buildSentinelLivenessProbe(lr *littleredv1alpha1.LittleRed) *corev1.Probe {
	// While the bootstrap-in-progress file exists, redis-server is not running yet (the pod is
	// parked in the startup wait-loop); report success so k8s does not kill it before Sentinel
	// authorizes a master.
	cmd := []string{
		bootstrapGuardScript,
		binRedisCli,
	}
	if lr.Spec.Auth.Enabled {
		cmd = append(cmd, "-a", "$(REDIS_PASSWORD)", "--no-auth-warning")
	}
	if lr.Spec.TLS.Enabled {
		cmd = append(cmd, "--tls", "--insecure")
	}
	cmd = append(cmd, "-t", "2", "ping")

	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", "-c", strings.Join(cmd, " ")},
			},
		},
		InitialDelaySeconds: 15,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    3,
	}
}

// buildSentinelReadinessProbe creates the readiness probe for the Redis container in sentinel mode.
//
// A replica is ready only when its replication link to the master is up.
// This ensures that a zombie replica (link:down following a ghost master) stops receiving
// traffic immediately, before the liveness probe eventually kills and replaces the pod.
func buildSentinelReadinessProbe(lr *littleredv1alpha1.LittleRed) *corev1.Probe {
	tlsFlags := ""
	if lr.Spec.TLS.Enabled {
		tlsFlags = " --tls --insecure"
	}

	tmpl := template.Must(template.New("sentinel-readiness").Delims("[[", "]]").Parse(
		`if [ -f /data/bootstrap-in-progress ]; then exit 1; fi
AUTH=""; [ -n "$REDIS_PASSWORD" ] && AUTH="-a $REDIS_PASSWORD --no-auth-warning"
info=$(redis-cli -h 127.0.0.1 $AUTH[[.TLSFlags]] -t 2 info replication 2>/dev/null) || exit 1
echo "$info" | grep -q "^role:master" && exit 0
echo "$info" | grep -q "^master_link_status:up" && exit 0
exit 1`))
	var buf bytes.Buffer
	err := tmpl.Execute(&buf, struct {
		TLSFlags string
	}{
		TLSFlags: tlsFlags,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to execute sentinel readiness template: %v", err))
	}
	script := buf.String()

	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", "-c", script},
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      3,
		FailureThreshold:    3,
	}
}

// serviceAnnotations returns merged user + Prometheus annotations for a Redis service.
func serviceAnnotations(lr *littleredv1alpha1.LittleRed) map[string]string {
	annotations := map[string]string{}
	maps.Copy(annotations, lr.Spec.Service.Annotations)
	if lr.Spec.Metrics.IsEnabled() {
		annotations["prometheus.io/scrape"] = annotationValueTrue
		annotations["prometheus.io/port"] = fmt.Sprintf("%d", littleredv1alpha1.RedisExporterPort)
		annotations["prometheus.io/path"] = pathMetrics
	}
	return annotations
}

// buildService creates the Service for Redis
func buildService(lr *littleredv1alpha1.LittleRed) *corev1.Service {
	labels := commonLabels(lr)
	// Add user-defined labels
	maps.Copy(labels, lr.Spec.Service.Labels)

	ports := []corev1.ServicePort{
		{
			Name:       ComponentRedis,
			Port:       int32(littleredv1alpha1.RedisPort),
			TargetPort: intstr.FromString(ComponentRedis),
			Protocol:   corev1.ProtocolTCP,
		},
	}

	// Add metrics port if enabled
	if lr.Spec.Metrics.IsEnabled() {
		ports = append(ports, corev1.ServicePort{
			Name:       portNameMetrics,
			Port:       int32(littleredv1alpha1.RedisExporterPort),
			TargetPort: intstr.FromString(portNameMetrics),
			Protocol:   corev1.ProtocolTCP,
		})
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        serviceName(lr),
			Namespace:   lr.Namespace,
			Labels:      labels,
			Annotations: serviceAnnotations(lr),
		},
		Spec: corev1.ServiceSpec{
			Type:     lr.Spec.Service.Type,
			Selector: selectorLabels(lr),
			Ports:    ports,
		},
	}

	return svc
}

// buildServiceMonitor creates the ServiceMonitor for Prometheus
func buildServiceMonitor(lr *littleredv1alpha1.LittleRed) *monitoringv1.ServiceMonitor {
	labels := commonLabels(lr)
	// Add user-defined labels
	maps.Copy(labels, lr.Spec.Metrics.ServiceMonitor.Labels)

	namespace := lr.Namespace
	if lr.Spec.Metrics.ServiceMonitor.Namespace != "" {
		namespace = lr.Spec.Metrics.ServiceMonitor.Namespace
	}

	interval := lr.Spec.Metrics.ServiceMonitor.Interval
	if interval == "" {
		interval = "30s"
	}

	scrapeTimeout := lr.Spec.Metrics.ServiceMonitor.ScrapeTimeout
	if scrapeTimeout == "" {
		scrapeTimeout = "10s"
	}

	sm := &monitoringv1.ServiceMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceMonitorName(lr),
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: monitoringv1.ServiceMonitorSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: selectorLabels(lr),
			},
			NamespaceSelector: monitoringv1.NamespaceSelector{
				MatchNames: []string{lr.Namespace},
			},
			Endpoints: []monitoringv1.Endpoint{
				{
					Port:          "metrics",
					Interval:      monitoringv1.Duration(interval),
					ScrapeTimeout: monitoringv1.Duration(scrapeTimeout),
				},
			},
		},
	}

	return sm
}

// ============================================================================
// Sentinel Mode Resources
// ============================================================================

// buildSentinelConfigMap creates the ConfigMap for sentinel.conf
func buildSentinelConfigMap(lr *littleredv1alpha1.LittleRed) *corev1.ConfigMap {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentSentinel

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentinelConfigMapName(lr),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"sentinel.conf": buildSentinelConfig(lr),
		},
	}
}

// buildSentinelConfig generates the sentinel.conf content
func buildSentinelConfig(lr *littleredv1alpha1.LittleRed) string {
	var sb strings.Builder

	sb.WriteString("# LittleRed Sentinel configuration\n")
	fmt.Fprintf(&sb, "port %d\n", littleredv1alpha1.SentinelPort)
	sb.WriteString("dir /data\n")

	// Auth configuration
	if lr.Spec.Auth.Enabled {
		// Sentinels use sentinel-pass to authenticate with each other.
		// Since we use the same password for everything, we use REDIS_PASSWORD.
		// The actual value is provided via environment to the shell, and we can't
		// easily inject it into the static config file without a rewrite.
		// However, for startup, we can just use the environment variable in the command line
		// or use 'sentinel sentinel-pass' in the config if we were to rewrite it.
		//
		// For LittleRed, we'll use the CLI args for requirepass, and for sentinel-pass
		// we'll rely on the operator setting it or adding it to the startup script.
		sb.WriteString("\n# Auth configured via startup arguments\n")
	}

	// TLS settings
	if lr.Spec.TLS.Enabled {
		sb.WriteString("\n# TLS configuration\n")
		fmt.Fprintf(&sb, "tls-port %d\n", littleredv1alpha1.SentinelPort)
		sb.WriteString("port 0\n")
		sb.WriteString("tls-cert-file /tls/tls.crt\n")
		sb.WriteString("tls-key-file /tls/tls.key\n")
		if lr.Spec.TLS.CACertSecret != "" {
			if lr.Spec.TLS.CACertSecret == lr.Spec.TLS.ExistingSecret {
				sb.WriteString("tls-ca-cert-file /tls/ca.crt\n")
			} else {
				sb.WriteString("tls-ca-cert-file /tls-ca/ca.crt\n")
			}
		}
		if lr.Spec.TLS.ClientAuth {
			sb.WriteString("tls-auth-clients yes\n")
		} else {
			sb.WriteString("tls-auth-clients no\n")
		}
		// Sentinels use replication protocol to talk to each other and master.
		sb.WriteString("tls-replication yes\n")
	}

	// Announce settings for proper discovery
	// For pure in-memory mode, we strictly use IPs. This ensures that when a pod
	// restarts and gets a new IP, it is treated as a new (empty) node, preventing
	// it from being incorrectly trusted as a former master with data.
	sb.WriteString("\n# Announce settings\n")
	sb.WriteString("sentinel resolve-hostnames no\n")
	sb.WriteString("sentinel announce-hostnames no\n")

	return sb.String()
}

// buildRedisConfigSentinel generates redis.conf for sentinel mode (includes replication)
func buildRedisConfigSentinel(lr *littleredv1alpha1.LittleRed) string {
	var sb strings.Builder

	// Start with base config
	sb.WriteString("# LittleRed generated configuration (sentinel mode)\n")
	sb.WriteString("bind 0.0.0.0\n")
	fmt.Fprintf(&sb, "port %d\n", littleredv1alpha1.RedisPort)
	sb.WriteString("dir /data\n")

	// Disable persistence (in-memory only)
	sb.WriteString("\n# Persistence disabled (in-memory mode)\n")
	sb.WriteString("save \"\"\n")
	sb.WriteString("appendonly no\n")

	// Memory settings
	sb.WriteString("\n# Memory configuration\n")
	maxmemory := lr.CalculateMaxmemory()
	fmt.Fprintf(&sb, "maxmemory %s\n", maxmemory)
	fmt.Fprintf(&sb, "maxmemory-policy %s\n", lr.GetEffectiveMaxmemoryPolicy())

	// Timeout settings
	sb.WriteString("\n# Connection settings\n")
	fmt.Fprintf(&sb, "timeout %d\n", lr.Spec.Config.Timeout)
	fmt.Fprintf(&sb, "tcp-keepalive %d\n", lr.Spec.Config.TCPKeepalive)

	// Replication settings - allow replicas to serve stale data during sync
	sb.WriteString("\n# Replication settings\n")
	sb.WriteString("replica-serve-stale-data yes\n")
	sb.WriteString("replica-read-only yes\n")
	sb.WriteString("repl-diskless-sync yes\n")
	sb.WriteString("repl-diskless-sync-delay 5\n")
	sb.WriteString("repl-diskless-load on-empty-db\n")

	// TLS settings
	if lr.Spec.TLS.Enabled {
		sb.WriteString("\n# TLS configuration\n")
		fmt.Fprintf(&sb, "tls-port %d\n", littleredv1alpha1.RedisPort)
		sb.WriteString("port 0\n")
		sb.WriteString("tls-cert-file /tls/tls.crt\n")
		sb.WriteString("tls-key-file /tls/tls.key\n")
		sb.WriteString("tls-replication yes\n")
		if lr.Spec.TLS.CACertSecret != "" {
			if lr.Spec.TLS.CACertSecret == lr.Spec.TLS.ExistingSecret {
				sb.WriteString("tls-ca-cert-file /tls/ca.crt\n")
			} else {
				sb.WriteString("tls-ca-cert-file /tls-ca/ca.crt\n")
			}
		}
		if lr.Spec.TLS.ClientAuth {
			sb.WriteString("tls-auth-clients yes\n")
		} else {
			sb.WriteString("tls-auth-clients no\n")
		}
	}

	// Raw config (expert mode)
	if lr.Spec.Config.Raw != "" {
		sb.WriteString("\n# Custom configuration\n")
		sb.WriteString(lr.Spec.Config.Raw)
		if !strings.HasSuffix(lr.Spec.Config.Raw, "\n") {
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

// buildConfigMapSentinelMode creates the ConfigMap for redis.conf in sentinel mode
func buildConfigMapSentinelMode(lr *littleredv1alpha1.LittleRed) *corev1.ConfigMap {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentRedis

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(lr),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			fileRedisConf: buildRedisConfigSentinel(lr),
		},
	}
}

// buildRedisStatefulSetSentinel creates the Redis StatefulSet for sentinel mode.
//
// replicas is NOT derived here. Sentinel mode is a fixed 1 master + 2 replicas
// (littleredv1alpha1.SentinelRedisReplicas), but a quarantined instance's desired count
// is 0 (LR-044), and that has to be the value this builder stamps: both sentinel
// StatefulSets are applied with server-side apply and client.ForceOwnership, so whatever
// .Spec.Replicas is built here wins unconditionally on every pass. Scaling the live
// object out of band would be force-overwritten right back. The single source of truth is
// sentinelDesiredReplicas; it is passed in for the same reason the cluster builder takes
// its shard index — the builder renders a decision, it does not make one.
func buildRedisStatefulSetSentinel(lr *littleredv1alpha1.LittleRed, replicas int32) *appsv1.StatefulSet {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentRedis

	podLabels := make(map[string]string)
	maps.Copy(podLabels, redisSelectorLabels(lr))
	maps.Copy(podLabels, lr.Spec.PodTemplate.Labels)

	// Compute config hash for pod annotations to trigger rolling update on config change
	configData := map[string]string{fileRedisConf: buildRedisConfigSentinel(lr)}
	configHash := computeConfigHash(configData)

	podAnnotations := make(map[string]string)
	maps.Copy(podAnnotations, lr.Spec.PodTemplate.Annotations)
	podAnnotations[AnnotationConfigHash] = configHash

	containers := []corev1.Container{buildRedisContainerSentinel(lr)}

	// Add exporter sidecar if metrics enabled
	if lr.Spec.Metrics.IsEnabled() {
		containers = append(containers, buildExporterContainer(lr, int32(littleredv1alpha1.RedisPort)))
	}

	// MinReadySeconds for Sentinel mode: allow time for Sentinel-managed failover
	// Sentinels need to detect master is down, reach quorum, and promote a replica
	// Default down-after-milliseconds is 30000ms, plus promotion time
	minReadySeconds := int32(35)
	if lr.Spec.UpdateStrategy.MinReadySeconds != nil {
		minReadySeconds = *lr.Spec.UpdateStrategy.MinReadySeconds
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName(lr),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:        &replicas,
			ServiceName:     replicasServiceName(lr),
			MinReadySeconds: minReadySeconds,
			Selector: &metav1.LabelSelector{
				MatchLabels: redisSelectorLabels(lr),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					SecurityContext:           lr.Spec.PodTemplate.SecurityContext,
					Containers:                containers,
					Volumes:                   buildVolumes(lr),
					NodeSelector:              lr.Spec.PodTemplate.NodeSelector,
					Tolerations:               lr.Spec.PodTemplate.Tolerations,
					Affinity:                  lr.Spec.PodTemplate.Affinity,
					PriorityClassName:         lr.Spec.PodTemplate.PriorityClassName,
					TopologySpreadConstraints: lr.Spec.PodTemplate.TopologySpreadConstraints,
					ImagePullSecrets:          lr.Spec.Image.PullSecrets,
				},
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
		},
	}

	return sts
}

// buildRedisContainerSentinel creates the Redis container for sentinel mode
func buildRedisContainerSentinel(lr *littleredv1alpha1.LittleRed) corev1.Container {
	// Script to configure replication strictly based on Sentinel state.
	// This is "Operator-Authorized" startup: no pod assumes mastership
	// unless Sentinel (configured by the Operator) says so.
	script := `#!/bin/sh
set -e

# Helper to log with timestamp
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') [Startup] $1"
}

# Create marker file to tell liveness probe we are starting up
touch /data/bootstrap-in-progress

cp /etc/redis/redis.conf /data/redis.conf

HOSTNAME=$(hostname)
SENTINEL_SVC="[[.Name]]-sentinel.[[.Namespace]].svc"

log "Starting Redis node $HOSTNAME. Waiting for Sentinel authorization..."
log "Auth enabled: $([ -n "$REDIS_PASSWORD" ] && echo yes || echo no)"

	AUTH_ARGS=""
	SENTINEL_AUTH_ARGS=""
	if [ -n "$REDIS_PASSWORD" ]; then
	  AUTH_ARGS="--requirepass $REDIS_PASSWORD --masterauth $REDIS_PASSWORD"
	  SENTINEL_AUTH_ARGS="-a $REDIS_PASSWORD --no-auth-warning"
	fi

	# Kill-9 / in-pod crash protection (see ADR-001 amendment):
	# If Sentinel already has a non-empty run-id stored for a master at MY IP, a previous
	# Redis process ran here and Sentinel observed it. The container must have restarted
	# (kill-9, OOM, etc.) while the pod IP stayed the same.
	# Starting as master in that state means an empty node becomes master and replicas
	# perform a FULLRESYNC from empty data — all data for this shard is lost.
	#
	# Defence: set YIELD_MASTER=true. The main loop sleeps (Redis NOT started) until
	# Sentinel's down-after-milliseconds fires, failover completes, and a different
	# master is elected. We then join as a replica.
	STORED_RUNID=$(redis-cli -h $SENTINEL_SVC -p 26379 $SENTINEL_AUTH_ARGS [[.TLSFlags]] -t 2 --raw sentinel master [[.MasterName]] 2>/dev/null \
	  | awk 'prev=="runid"{print; exit} {prev=$0}' || true)
	SENTINEL_MASTER_IP=$(redis-cli -h $SENTINEL_SVC -p 26379 $SENTINEL_AUTH_ARGS [[.TLSFlags]] -t 2 --raw sentinel get-master-addr-by-name [[.MasterName]] 2>/dev/null \
	  | head -n 1 || true)

	YIELD_MASTER=false
	YIELD_COUNT=0
	# 60 iterations x 2 s = 120 s: enough for 30 s SDOWN + failover + margin.
	# If failover never completes (no eligible replica), we give up and start as master.
	YIELD_LIMIT=60

	if [ "$SENTINEL_MASTER_IP" = "$POD_IP" ] && [ -n "$STORED_RUNID" ]; then
	  log "Kill-9 protection: Sentinel knows $POD_IP as master (run-id=$STORED_RUNID)."
	  log "Suppressing Redis start. Waiting for Sentinel SDOWN detection and failover..."
	  YIELD_MASTER=true
	fi

	# Loop until Sentinel has a master for us
	while true; do
	  # Use --raw to get just the values (IP/Host on line 1, Port on line 2)
	  # Use -t to avoid hanging on DNS or network issues
	  SENTINEL_REPLY=$(redis-cli -h $SENTINEL_SVC -p 26379 $SENTINEL_AUTH_ARGS [[.TLSFlags]] -t 2 --raw sentinel get-master-addr-by-name [[.MasterName]] || true)
	  CURRENT_MASTER_HOST=$(echo "$SENTINEL_REPLY" | head -n 1)
	  CURRENT_MASTER_PORT=$(echo "$SENTINEL_REPLY" | sed -n '2p')

	  if [ -n "$CURRENT_MASTER_HOST" ]; then
	    log "Sentinel reported master at $CURRENT_MASTER_HOST:$CURRENT_MASTER_PORT"

	    # Check if reported master is ME (compare IP)
	    if [ "$CURRENT_MASTER_HOST" = "$POD_IP" ]; then
	      if [ "$YIELD_MASTER" = "true" ]; then
	        YIELD_COUNT=$((YIELD_COUNT + 1))
	        if [ "$YIELD_COUNT" -ge "$YIELD_LIMIT" ]; then
	          log "Yield timeout (${YIELD_LIMIT} attempts). No failover completed — no eligible replica? Starting as master."
	          YIELD_MASTER=false
	        else
	          log "Yielding ($YIELD_COUNT/$YIELD_LIMIT): still master per Sentinel. Waiting for SDOWN + failover..."
	          sleep 2
	          continue
	        fi
	      fi
	      log "I am the authorized master. Starting redis-server..."
	      rm -f /data/bootstrap-in-progress
	      exec redis-server /data/redis.conf --replica-announce-ip ${POD_IP} $AUTH_ARGS
	    fi

	    # I am a replica. Check if master is reachable before committing.
	    # Retry several times: during initial cluster boot all pods start in parallel,
	    # so the master's redis-server may not be listening yet even though the IP is valid.
	    PING_ATTEMPTS=6
	    PING_DELAY=3
	    MASTER_ALIVE=false
	    for attempt in $(seq 1 $PING_ATTEMPTS); do
	      if redis-cli -h $CURRENT_MASTER_HOST -p $CURRENT_MASTER_PORT $SENTINEL_AUTH_ARGS [[.TLSFlags]] -t 2 ping > /dev/null 2>&1; then
	        MASTER_ALIVE=true
	        break
	      fi
	      log "Master $CURRENT_MASTER_HOST not yet reachable (attempt $attempt/$PING_ATTEMPTS). Waiting ${PING_DELAY}s..."
	      sleep $PING_DELAY
	    done

	    if [ "$MASTER_ALIVE" = "true" ]; then
	       log "Master is alive. Joining $CURRENT_MASTER_HOST as replica..."
	       rm -f /data/bootstrap-in-progress
	       exec redis-server /data/redis.conf --replicaof $CURRENT_MASTER_HOST $CURRENT_MASTER_PORT --replica-announce-ip ${POD_IP} $AUTH_ARGS
	    fi

	    # Master is unreachable after retries (likely a ghost IP). Start as bare server
	    # so Sentinel can discover us and perform a failover. This avoids both the
	    # deadlock (ADR-002) and the zombie-replica problem.
	    log "Master $CURRENT_MASTER_HOST unreachable after $PING_ATTEMPTS attempts. Starting bare for Sentinel discovery..."
	    rm -f /data/bootstrap-in-progress
	    exec redis-server /data/redis.conf --replica-announce-ip ${POD_IP} $AUTH_ARGS
	  fi

	  log "Sentinel has no master info. Waiting..."
	  sleep 2
	done`
	tmpl := template.Must(template.New("sentinel-startup").Delims("[[", "]]").Parse(script))
	var buf bytes.Buffer
	tlsFlags := ""
	if lr.Spec.TLS.Enabled {
		tlsFlags = tlsInsecureFlags
	}
	err := tmpl.Execute(&buf, struct {
		Name       string
		Namespace  string
		TLSFlags   string
		MasterName string
	}{
		Name:       lr.Name,
		Namespace:  lr.Namespace,
		TLSFlags:   tlsFlags,
		MasterName: lr.SentinelMasterName(),
	})

	if err != nil {
		panic(fmt.Sprintf("failed to execute sentinel startup template: %v", err))
	}
	finalScript := buf.String()

	preStopTmpl := template.Must(template.New("sentinel-prestop").Delims("[[", "]]").Parse(`
# Redirect all output from this point forward
exec >/proc/1/fd/1 2>&1

echo "preStop hook starting at $(date), PID $$"

AUTH_ARGS=""
if [ -n "$REDIS_PASSWORD" ]; then
  AUTH_ARGS="-a $REDIS_PASSWORD --no-auth-warning"
fi

ROLE=$(redis-cli $AUTH_ARGS [[.TLSFlags]] info replication | grep role | cut -d: -f2 | tr -d '\r')
echo "preStop: my role is $ROLE"
if [ "$ROLE" = "master" ]; then
  echo "preStop: I am the master. Proactively failing over..."
  SENTINEL_SVC="[[.Name]]-sentinel.[[.Namespace]].svc"
  # Wait until Sentinel has discovered all expected replicas before forcing a
  # failover. If the master was just restarted, Sentinel may not yet know about
  # replicas that connected within the last polling cycle (~1 s). Triggering
  # SENTINEL FAILOVER too early means those replicas miss the REPLICAOF
  # reconfiguration step and get stuck pointing at the dead master IP.
  EXPECTED_SLAVES=2
  for i in $(seq 1 10); do
    SLAVE_COUNT=$(redis-cli --raw -h $SENTINEL_SVC -p 26379 $AUTH_ARGS [[.TLSFlags]] SENTINEL SLAVES [[.MasterName]] 2>/dev/null | grep -c "^name$" || true)
    SLAVE_COUNT=${SLAVE_COUNT:-0}
    if [ "$SLAVE_COUNT" -ge "$EXPECTED_SLAVES" ]; then
      echo "Sentinel knows $SLAVE_COUNT/$EXPECTED_SLAVES replicas. Proceeding with failover."
      break
    fi
    echo "Waiting for Sentinel to discover replicas ($SLAVE_COUNT/$EXPECTED_SLAVES)..."
    sleep 1
  done
  # Pause writes for 30s to ensure a clean handover
  redis-cli $AUTH_ARGS [[.TLSFlags]] CLIENT PAUSE 30000 WRITE || true
  # Trigger failover
  redis-cli -h $SENTINEL_SVC -p 26379 $AUTH_ARGS [[.TLSFlags]] SENTINEL failover [[.MasterName]] || true
  # Wait for Sentinel to acknowledge the new master
  for i in $(seq 1 10); do
    MASTER_IP=$(redis-cli -h $SENTINEL_SVC -p 26379 $AUTH_ARGS [[.TLSFlags]] -t 2 --raw sentinel get-master-addr-by-name [[.MasterName]] | head -n 1 || true)
    if [ -n "$MASTER_IP" ] && [ "$MASTER_IP" != "$POD_IP" ]; then
       echo "Failover confirmed. New master: $MASTER_IP"
       exit 0
    fi
    sleep 1
  done
fi`))
	var preStopBuf bytes.Buffer
	sentinelPreStopTLSFlags := ""
	if lr.Spec.TLS.Enabled {
		sentinelPreStopTLSFlags = tlsInsecureFlags
	}
	err = preStopTmpl.Execute(&preStopBuf, struct {
		Name       string
		Namespace  string
		TLSFlags   string
		MasterName string
	}{
		Name:       lr.Name,
		Namespace:  lr.Namespace,
		TLSFlags:   sentinelPreStopTLSFlags,
		MasterName: lr.SentinelMasterName(),
	})
	if err != nil {
		panic(fmt.Sprintf("failed to execute sentinel prestop template: %v", err))
	}
	preStopScript := preStopBuf.String()

	container := corev1.Container{
		Name:            ComponentRedis,
		Image:           lr.Spec.Image.FullImage(),
		ImagePullPolicy: lr.Spec.Image.PullPolicy,
		Command:         []string{"sh", "-c", finalScript},
		Ports: []corev1.ContainerPort{
			{
				Name:          ComponentRedis,
				ContainerPort: int32(littleredv1alpha1.RedisPort),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Resources: lr.Spec.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      volNameConfig,
				MountPath: mountPathEtcRedis,
				ReadOnly:  true,
			},
			{
				Name:      volNameData,
				MountPath: mountPathDataDir,
			},
		},
		Env: []corev1.EnvVar{
			{
				Name: "LITTLERED_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.labels['app.kubernetes.io/instance']",
					},
				},
			},
			{
				Name: "LITTLERED_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
			{
				Name: envPodIP,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: fieldPathPodIP,
					},
				},
			},
		},
		LivenessProbe:  buildSentinelLivenessProbe(lr),
		ReadinessProbe: buildSentinelReadinessProbe(lr),
		Lifecycle: &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"sh", "-c", preStopScript},
				},
			},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(true),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{capAll},
			},
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
	}

	// Add TLS volume mounts
	if lr.Spec.TLS.Enabled {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volNameTLS,
			MountPath: mountPathTLS,
			ReadOnly:  true,
		})
		if lr.Spec.TLS.CACertSecret != "" && lr.Spec.TLS.CACertSecret != lr.Spec.TLS.ExistingSecret {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      volNameCACert,
				MountPath: mountPathTLSCA,
				ReadOnly:  true,
			})
		}
	}

	// Add auth env var
	if lr.Spec.Auth.Enabled {
		container.Env = append(container.Env, corev1.EnvVar{
			Name: envRedisPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: lr.Spec.Auth.ExistingSecret,
					},
					Key: secretKeyPassword,
				},
			},
		})
	}

	return container
}

// buildSentinelVolumes returns the volumes for the Sentinel StatefulSet pod spec.
func buildSentinelVolumes(lr *littleredv1alpha1.LittleRed) []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: volNameConfig,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: sentinelConfigMapName(lr),
					},
				},
			},
		},
		{
			Name: volNameData,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
	if lr.Spec.TLS.Enabled {
		volumes = append(volumes, corev1.Volume{
			Name: volNameTLS,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: lr.Spec.TLS.ExistingSecret,
				},
			},
		})
		if lr.Spec.TLS.CACertSecret != "" {
			volumes = append(volumes, corev1.Volume{
				Name: volNameCACert,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: lr.Spec.TLS.CACertSecret,
					},
				},
			})
		}
	}
	return volumes
}

// buildSentinelStatefulSet creates the Sentinel StatefulSet.
//
// replicas is passed in for the same reason as buildRedisStatefulSetSentinel's: a
// quarantine takes the SENTINEL pods away too, and that is not optional — the victim's
// sentinels publish hellos on the captor's master's channel under the shared master name,
// so the captor learns them as peers (the num-other-sentinels inflation that distorts its
// quorum math). See sentinelDesiredReplicas and LR-044.
func buildSentinelStatefulSet(lr *littleredv1alpha1.LittleRed, replicas int32) *appsv1.StatefulSet {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentSentinel

	podLabels := make(map[string]string)
	maps.Copy(podLabels, sentinelSelectorLabels(lr))

	// Compute config hash for pod annotations to trigger rolling update on config change
	configData := map[string]string{"sentinel.conf": buildSentinelConfig(lr)}
	configHash := computeConfigHash(configData)

	podAnnotations := map[string]string{
		AnnotationConfigHash: configHash,
	}

	containers := []corev1.Container{buildSentinelContainer(lr)}
	// Add exporter sidecar if metrics enabled — points at the Sentinel port so
	// oliver006/redis_exporter emits redis_sentinel_* series for these pods.
	if lr.Spec.Metrics.IsEnabled() {
		containers = append(containers, buildExporterContainer(lr, int32(littleredv1alpha1.SentinelPort)))
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentinelStatefulSetName(lr),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: sentinelServiceName(lr),
			Selector: &metav1.LabelSelector{
				MatchLabels: sentinelSelectorLabels(lr),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					SecurityContext:           lr.Spec.PodTemplate.SecurityContext,
					Containers:                containers,
					Volumes:                   buildSentinelVolumes(lr),
					NodeSelector:              lr.Spec.PodTemplate.NodeSelector,
					Tolerations:               lr.Spec.PodTemplate.Tolerations,
					Affinity:                  lr.Spec.PodTemplate.Affinity,
					PriorityClassName:         lr.Spec.PodTemplate.PriorityClassName,
					TopologySpreadConstraints: lr.Spec.PodTemplate.TopologySpreadConstraints,
					ImagePullSecrets:          lr.Spec.Image.PullSecrets,
				},
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
		},
	}

	return sts
}

// buildSentinelContainer creates the Sentinel container
func buildSentinelContainer(lr *littleredv1alpha1.LittleRed) corev1.Container {
	sentinel := lr.Spec.Sentinel
	if sentinel == nil {
		sentinel = &littleredv1alpha1.SentinelSpec{}
	}

	// Use text/template for consistency across all startup scripts
	scriptTmpl := template.Must(template.New("sentinel-startup").Delims("[[", "]]").Parse(`#!/bin/sh
set -e

# Helper to log with timestamp
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') [Startup] $1"
}

cp /etc/sentinel/sentinel.conf /data/sentinel.conf

AUTH_ARGS=""
if [ -n "$REDIS_PASSWORD" ]; then
  AUTH_ARGS="--requirepass $REDIS_PASSWORD --sentinel sentinel-pass $REDIS_PASSWORD"
fi

log "Starting Sentinel node with IP ${POD_IP} (auth: $([ -n "$REDIS_PASSWORD" ] && echo yes || echo no))..."
exec redis-sentinel /data/sentinel.conf --sentinel announce-ip ${POD_IP} $AUTH_ARGS
`))
	var buf bytes.Buffer
	err := scriptTmpl.Execute(&buf, struct{}{})
	if err != nil {
		panic(fmt.Sprintf("failed to execute sentinel container template: %v", err))
	}
	script := buf.String()

	container := corev1.Container{
		Name:            ComponentSentinel,
		Image:           lr.Spec.Image.FullImage(),
		ImagePullPolicy: lr.Spec.Image.PullPolicy,
		Command:         []string{"sh", "-c", script},
		Ports: []corev1.ContainerPort{
			{
				Name:          ComponentSentinel,
				ContainerPort: int32(littleredv1alpha1.SentinelPort),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Resources: sentinel.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      volNameConfig,
				MountPath: "/etc/sentinel",
				ReadOnly:  true,
			},
			{
				Name:      volNameData,
				MountPath: mountPathDataDir,
			},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"sh", "-c",
						fmt.Sprintf("AUTH=\"\"; [ -n \"$REDIS_PASSWORD\" ] && AUTH=\"-a $REDIS_PASSWORD --no-auth-warning\"; TLS=\"\"; [ \"%t\" = \"true\" ] && TLS=\"--tls --insecure\"; redis-cli -p %d $AUTH $TLS ping", lr.Spec.TLS.Enabled, littleredv1alpha1.SentinelPort),
					},
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			TimeoutSeconds:      5,
			FailureThreshold:    3,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"sh", "-c",
						fmt.Sprintf("AUTH=\"\"; [ -n \"$REDIS_PASSWORD\" ] && AUTH=\"-a $REDIS_PASSWORD --no-auth-warning\"; TLS=\"\"; [ \"%t\" = \"true\" ] && TLS=\"--tls --insecure\"; redis-cli -p %d $AUTH $TLS ping", lr.Spec.TLS.Enabled, littleredv1alpha1.SentinelPort),
					},
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      3,
			FailureThreshold:    3,
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(true),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{capAll},
			},
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
	}

	// Add POD_IP env var
	container.Env = append(container.Env, corev1.EnvVar{
		Name: envPodIP,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: fieldPathPodIP,
			},
		},
	})

	// Add auth env var
	if lr.Spec.Auth.Enabled {
		container.Env = append(container.Env, corev1.EnvVar{
			Name: envRedisPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: lr.Spec.Auth.ExistingSecret,
					},
					Key: secretKeyPassword,
				},
			},
		})
	}

	// Add TLS volume mounts
	if lr.Spec.TLS.Enabled {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volNameTLS,
			MountPath: mountPathTLS,
			ReadOnly:  true,
		})
		if lr.Spec.TLS.CACertSecret != "" && lr.Spec.TLS.CACertSecret != lr.Spec.TLS.ExistingSecret {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      volNameCACert,
				MountPath: mountPathTLSCA,
				ReadOnly:  true,
			})
		}
	}

	return container
}

// buildMasterService creates the Service that points to the current master.
// It is a role-scoped client entrypoint (selector role=master) and deliberately
// does not expose metrics or Prometheus scrape annotations: the all-pods
// replicas headless service is the scrape target, so the master is scraped once.
func buildMasterService(lr *littleredv1alpha1.LittleRed) *corev1.Service {
	labels := commonLabels(lr)
	maps.Copy(labels, lr.Spec.Service.Labels)

	annotations := map[string]string{}
	maps.Copy(annotations, lr.Spec.Service.Annotations)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        serviceName(lr),
			Namespace:   lr.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     lr.Spec.Service.Type,
			Selector: masterSelectorLabels(lr),
			Ports: []corev1.ServicePort{
				{
					Name:       ComponentRedis,
					Port:       int32(littleredv1alpha1.RedisPort),
					TargetPort: intstr.FromString(ComponentRedis),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}

// buildReplicasHeadlessService creates the headless Service for all Redis pods
func buildReplicasHeadlessService(lr *littleredv1alpha1.LittleRed) *corev1.Service {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentRedis

	ports := []corev1.ServicePort{
		{
			Name:       ComponentRedis,
			Port:       int32(littleredv1alpha1.RedisPort),
			TargetPort: intstr.FromString(ComponentRedis),
			Protocol:   corev1.ProtocolTCP,
		},
	}

	var annotations map[string]string
	// This headless service selects every Redis data pod (master + replicas),
	// so it is the scrape target for the whole sentinel data plane — the
	// role-scoped master service deliberately does not expose metrics to avoid
	// scraping the master twice.
	if lr.Spec.Metrics.IsEnabled() {
		ports = append(ports, corev1.ServicePort{
			Name:       portNameMetrics,
			Port:       int32(littleredv1alpha1.RedisExporterPort),
			TargetPort: intstr.FromString(portNameMetrics),
			Protocol:   corev1.ProtocolTCP,
		})
		annotations = map[string]string{
			"prometheus.io/scrape": annotationValueTrue,
			"prometheus.io/port":   fmt.Sprintf("%d", littleredv1alpha1.RedisExporterPort),
			"prometheus.io/path":   pathMetrics,
		}
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        replicasServiceName(lr),
			Namespace:   lr.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			ClusterIP:                serviceClusterNone,
			Selector:                 redisSelectorLabels(lr),
			PublishNotReadyAddresses: true,
			Ports:                    ports,
		},
	}
}

// buildSentinelHeadlessService creates the headless Service for Sentinel pods
func buildSentinelHeadlessService(lr *littleredv1alpha1.LittleRed) *corev1.Service {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentSentinel

	ports := []corev1.ServicePort{
		{
			Name:       ComponentSentinel,
			Port:       int32(littleredv1alpha1.SentinelPort),
			TargetPort: intstr.FromString(ComponentSentinel),
			Protocol:   corev1.ProtocolTCP,
		},
	}

	var annotations map[string]string
	// Expose the exporter port so the ServiceMonitor (selects on name+instance)
	// and prometheus.io annotations pick up the Sentinel pods' metrics.
	if lr.Spec.Metrics.IsEnabled() {
		ports = append(ports, corev1.ServicePort{
			Name:       portNameMetrics,
			Port:       int32(littleredv1alpha1.RedisExporterPort),
			TargetPort: intstr.FromString(portNameMetrics),
			Protocol:   corev1.ProtocolTCP,
		})
		annotations = map[string]string{
			"prometheus.io/scrape": annotationValueTrue,
			"prometheus.io/port":   fmt.Sprintf("%d", littleredv1alpha1.RedisExporterPort),
			"prometheus.io/path":   pathMetrics,
		}
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        sentinelServiceName(lr),
			Namespace:   lr.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			ClusterIP:                serviceClusterNone,
			Selector:                 sentinelSelectorLabels(lr),
			PublishNotReadyAddresses: true,
			Ports:                    ports,
		},
	}
}

// ============================================================================
// Cluster Mode Resources
// ============================================================================

// buildClusterConfigMap creates the ConfigMap for cluster mode redis.conf
func buildClusterConfigMap(lr *littleredv1alpha1.LittleRed) *corev1.ConfigMap {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentCluster

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(lr),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			fileRedisConf: buildClusterRedisConfig(lr),
		},
	}
}

// buildClusterRedisConfig generates redis.conf for cluster mode
func buildClusterRedisConfig(lr *littleredv1alpha1.LittleRed) string {
	var sb strings.Builder

	cluster := lr.Spec.Cluster
	if cluster == nil {
		cluster = &littleredv1alpha1.ClusterSpec{}
		cluster.SetDefaults()
	}

	sb.WriteString("# LittleRed generated configuration (cluster mode)\n")
	sb.WriteString("bind 0.0.0.0\n")
	fmt.Fprintf(&sb, "port %d\n", littleredv1alpha1.RedisPort)
	sb.WriteString("dir /data\n")

	// Cluster configuration
	sb.WriteString("\n# Cluster configuration\n")
	sb.WriteString("cluster-enabled yes\n")
	sb.WriteString("cluster-config-file /data/nodes.conf\n")
	fmt.Fprintf(&sb, "cluster-node-timeout %d\n", cluster.ClusterNodeTimeout)
	// The operator is the sole topology authority (ADR-007): it pins each Redis shard's
	// master+replicas inside one shard StatefulSet so per-shard placement holds. Redis's
	// autonomous replica migration would re-pair replicas across shards (topology-blind)
	// and silently break that invariant, so we disable it — "a more stable cluster
	// topology when it is managed externally" (Redis cluster spec).
	sb.WriteString("cluster-allow-replica-migration no\n")

	// Replication settings
	sb.WriteString("\n# Replication settings\n")
	sb.WriteString("repl-diskless-sync yes\n")
	sb.WriteString("repl-diskless-sync-delay 5\n")
	sb.WriteString("repl-diskless-load on-empty-db\n")

	// Disable persistence (in-memory only)
	sb.WriteString("\n# Persistence disabled (in-memory mode)\n")
	sb.WriteString("save \"\"\n")
	sb.WriteString("appendonly no\n")

	// Memory settings
	sb.WriteString("\n# Memory configuration\n")
	maxmemory := lr.CalculateMaxmemory()
	fmt.Fprintf(&sb, "maxmemory %s\n", maxmemory)
	fmt.Fprintf(&sb, "maxmemory-policy %s\n", lr.GetEffectiveMaxmemoryPolicy())

	// Timeout settings
	sb.WriteString("\n# Connection settings\n")
	fmt.Fprintf(&sb, "timeout %d\n", lr.Spec.Config.Timeout)
	fmt.Fprintf(&sb, "tcp-keepalive %d\n", lr.Spec.Config.TCPKeepalive)

	// TLS settings
	if lr.Spec.TLS.Enabled {
		sb.WriteString("\n# TLS configuration\n")
		fmt.Fprintf(&sb, "tls-port %d\n", littleredv1alpha1.RedisPort)
		sb.WriteString("port 0\n")
		sb.WriteString("tls-cert-file /tls/tls.crt\n")
		sb.WriteString("tls-key-file /tls/tls.key\n")
		sb.WriteString("tls-cluster yes\n")
		sb.WriteString("tls-replication yes\n")
		if lr.Spec.TLS.CACertSecret != "" {
			if lr.Spec.TLS.CACertSecret == lr.Spec.TLS.ExistingSecret {
				sb.WriteString("tls-ca-cert-file /tls/ca.crt\n")
			} else {
				sb.WriteString("tls-ca-cert-file /tls-ca/ca.crt\n")
			}
		}
		if lr.Spec.TLS.ClientAuth {
			sb.WriteString("tls-auth-clients yes\n")
		} else {
			sb.WriteString("tls-auth-clients no\n")
		}
	}

	// Raw config (expert mode)
	if lr.Spec.Config.Raw != "" {
		sb.WriteString("\n# Custom configuration\n")
		sb.WriteString(lr.Spec.Config.Raw)
		if !strings.HasSuffix(lr.Spec.Config.Raw, "\n") {
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

// buildShardSpreadConstraint builds the per-shard topologySpreadConstraint for shard shardIdx
// from spec.placement.shardAntiAffinity, or nil when the knob is unset. It spreads exactly that
// shard's pods (selected by the shard identity label via clusterShardSelectorLabels) across the
// configured failure domain with maxSkew 1, so a single node/zone loss cannot take a whole
// shard. Self-defaulting (topologyKey/whenUnsatisfiable) so it never emits an invalid
// constraint even if SetDefaults was not applied. See ADR-007.
func buildShardSpreadConstraint(lr *littleredv1alpha1.LittleRed, shardIdx int) *corev1.TopologySpreadConstraint {
	if lr.Spec.Placement == nil || lr.Spec.Placement.ShardAntiAffinity == nil {
		return nil
	}
	saa := lr.Spec.Placement.ShardAntiAffinity

	topologyKey := saa.TopologyKey
	if topologyKey == "" {
		topologyKey = littleredv1alpha1.DefaultShardTopologyKey
	}
	whenUnsatisfiable := saa.WhenUnsatisfiable
	if whenUnsatisfiable == "" {
		whenUnsatisfiable = littleredv1alpha1.DefaultShardWhenUnsatisfiable
	}

	return &corev1.TopologySpreadConstraint{
		MaxSkew:           1,
		TopologyKey:       topologyKey,
		WhenUnsatisfiable: whenUnsatisfiable,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: clusterShardSelectorLabels(lr, shardIdx)},
	}
}

// buildClusterShardStatefulSet creates the StatefulSet for a single cluster shard.
// In the per-shard model (0.3.0) cluster mode is N StatefulSets — one per shard,
// {name}-shard-K — each sized 1+replicasPerShard, carrying the stable per-shard identity
// label. Pod {name}-shard-K-0 is the shard's intended master; -1..R are its replicas.
// All shard StatefulSets share the one headless Service ({name}-cluster) as their
// governing service, so peer discovery and pod DNS keep resolving across every shard.
//
// partition is the state-gated intra-shard rollout cursor (ADR-017, LR-047) and is
// RENDERED here, never computed here — exactly as replicas are for the sentinel builders
// (LR-044: a builder renders a decision, it does not make one). It has to be a build-time
// parameter because r.apply is server-side apply with client.ForceOwnership, so whatever
// this function emits wins unconditionally on every pass; a partition patched out of band
// would be force-overwritten back to whatever was built here on the next reconcile, which
// for this field means releasing the shard's master while its replacement replica is still
// unsynced — today's defect, on a 2s cycle.
//
// nil means "emit no rollingUpdate block at all", which is byte-identical to the shape
// this builder produced before ADR-017. That is the replicasPerShard == 0 case (with one
// copy per shard, gating is vacuous — the caller warns and proceeds) and the pre-gate
// baseline. Note that the partition lives in spec.updateStrategy, OUTSIDE the pod template
// hashed into AnnotationPodSpecHash, so neither introducing it nor stepping it down is
// itself a template change — pinned by
// TestClusterShardPartitionIsOutsideThePodTemplateHash.
func buildClusterShardStatefulSet(lr *littleredv1alpha1.LittleRed, shardIdx int, partition *int32) *appsv1.StatefulSet {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentCluster
	labels[LabelShard] = clusterShardLabelValue(shardIdx)

	podLabels := make(map[string]string)
	maps.Copy(podLabels, clusterShardSelectorLabels(lr, shardIdx))
	maps.Copy(podLabels, lr.Spec.PodTemplate.Labels)

	// Compute config hash for pod annotations to trigger rolling update on config change
	configData := map[string]string{fileRedisConf: buildClusterRedisConfig(lr)}
	configHash := computeConfigHash(configData)

	podAnnotations := make(map[string]string)
	maps.Copy(podAnnotations, lr.Spec.PodTemplate.Annotations)
	podAnnotations[AnnotationConfigHash] = configHash

	cluster := lr.Spec.Cluster
	if cluster == nil {
		cluster = &littleredv1alpha1.ClusterSpec{}
		cluster.SetDefaults()
	}

	// One StatefulSet per shard: master (ordinal 0) plus replicasPerShard replicas.
	replicasPerShard := 0
	if cluster.ReplicasPerShard != nil {
		replicasPerShard = *cluster.ReplicasPerShard
	}
	replicas := int32(1 + replicasPerShard)

	containers := []corev1.Container{buildClusterRedisContainer(lr)}

	// Add exporter sidecar if metrics enabled
	if lr.Spec.Metrics.IsEnabled() {
		containers = append(containers, buildExporterContainer(lr, int32(littleredv1alpha1.RedisPort)))
	}

	// MinReadySeconds ensures pods are stable before next pod is restarted during rolling updates.
	// For cluster mode with replicas, this allows time for automatic failover to complete
	// before the next master is taken down.
	// - cluster-node-timeout (default 15000ms) for replica to detect master is down
	// - A few seconds for election and promotion
	// - Buffer for operator reconciliation
	// Total: 30 seconds is a safe default for replica mode
	minReadySeconds := int32(0)
	if lr.Spec.UpdateStrategy.MinReadySeconds != nil {
		// User-specified value takes precedence
		minReadySeconds = *lr.Spec.UpdateStrategy.MinReadySeconds
	} else if cluster.ReplicasPerShard != nil && *cluster.ReplicasPerShard > 0 {
		// Default for replica mode: 30 seconds
		minReadySeconds = 30
	}

	// Merge the user's topologySpreadConstraints with the operator's per-shard constraint
	// (spec.placement.shardAntiAffinity). Append so the operator's shard-scoped spread always
	// applies and users can layer additional constraints; copy to avoid mutating the spec slice.
	topologySpread := lr.Spec.PodTemplate.TopologySpreadConstraints
	if shardConstraint := buildShardSpreadConstraint(lr, shardIdx); shardConstraint != nil {
		topologySpread = append(append([]corev1.TopologySpreadConstraint{}, topologySpread...), *shardConstraint)
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterShardStatefulSetName(lr, shardIdx),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:        &replicas,
			ServiceName:     clusterHeadlessServiceName(lr),
			MinReadySeconds: minReadySeconds,
			Selector: &metav1.LabelSelector{
				MatchLabels: clusterShardSelectorLabels(lr, shardIdx),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					SecurityContext:           lr.Spec.PodTemplate.SecurityContext,
					Containers:                containers,
					Volumes:                   buildClusterVolumes(lr),
					NodeSelector:              lr.Spec.PodTemplate.NodeSelector,
					Tolerations:               lr.Spec.PodTemplate.Tolerations,
					Affinity:                  lr.Spec.PodTemplate.Affinity,
					PriorityClassName:         lr.Spec.PodTemplate.PriorityClassName,
					TopologySpreadConstraints: topologySpread,
					ImagePullSecrets:          lr.Spec.Image.PullSecrets,
				},
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
		},
	}

	if partition != nil {
		p := *partition
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &p}
	}

	// Stamp a hash of the operator-authored pod template so the serialized rollout can
	// detect a template change cache-safely (LR-021). Computed last, over the template as
	// built above (which already carries the config hash), so it reflects any change.
	sts.Spec.Template.Annotations[AnnotationPodSpecHash] = computePodTemplateHash(&sts.Spec.Template)

	return sts
}

// buildClusterRedisContainer creates the Redis container for cluster mode
func buildClusterRedisContainer(lr *littleredv1alpha1.LittleRed) corev1.Container {
	// Startup script that implements "Kill-9 / Crash Protection" (see ADR-001 amendment):
	//
	// 1. Detects if this is a container restart (memory lost, IP same) by checking
	//    for a surviving nodes.conf.
	// 2. If it is a restart, it YIELDS startup by polling peers until they confirm
	//    that a failover has occurred (i.e. this IP is no longer seen as master with SLOTS).
	//    This prevents the "empty master" from wiping its promoted replicas.
	// 3. If failover is not confirmed within 30s, it actively attempts to find its
	//    own replica and issues an aggressive CLUSTER FAILOVER TAKEOVER to break the deadlock.
	// 4. If failover still fails after 60s, it yields to the K8s liveness probe.
	// 5. Copies the read-only config to /data to allow for CONFIG REWRITE.
	// 6. Always deletes nodes.conf before starting to ensure a fresh identity.
	startupScript := `#!/bin/sh
set -e

# Helper to log with timestamp
log() {
  echo "$(date '+%Y-%m-%dT%H:%M:%SZ') [Startup] $1"
}

log "Starting Redis cluster node ${POD_NAME} with IP: ${POD_IP}"

# Create marker file to tell liveness probe we are starting up
touch /data/bootstrap-in-progress

# STEP 1: Detect if this is a container restart (Dangerous) or a fresh Pod (Safe)
RESTART_DETECTED=false
if [ -f /data/nodes.conf ]; then
  log "Marker nodes.conf found. This is a container restart (memory lost, IP same). Data loss risk is HIGH."
  RESTART_DETECTED=true
else
  log "No nodes.conf found. This is a fresh Pod start or bootstrap. Safe to proceed."
fi

# STEP 2: Copy config to writable /data to support CONFIG REWRITE
cp /etc/redis/redis.conf /data/redis.conf

# STEP 3: Yield Loop (Only for restarts)
if [ "$RESTART_DETECTED" = "true" ]; then
  log "Safety Check: Verifying failover state on peers..."

  # Configuration from environment
  AUTH_ARGS=""
  [ -n "$REDIS_PASSWORD" ] && AUTH_ARGS="-a $REDIS_PASSWORD --no-auth-warning"
  PEER_SERVICE="[[.Name]]-cluster.[[.Namespace]].svc"

  # Wait up to 60s for peers to confirm failover
  for i in $(seq 1 12); do
    PEER_IPS=$(getent hosts "$PEER_SERVICE" | awk '{print $1}')
    STILL_OWN_SLOTS=false
    PEERS_CONTACTED=0
    MY_NODE_ID=""
    MY_REPLICA_IP=""

    for peer in $PEER_IPS; do
      [ "$peer" = "$POD_IP" ] && continue
      NODES_INFO=$(redis-cli -h "$peer" -p [[.RedisPort]] $AUTH_ARGS [[.TLSFlags]] -t 2 CLUSTER NODES 2>/dev/null || true)
      if [ -n "$NODES_INFO" ]; then
        PEERS_CONTACTED=$((PEERS_CONTACTED + 1))

        # Identify myself in the peer's view
        MY_LINE=$(echo "$NODES_INFO" | grep "$POD_IP:[[.RedisPort]]")
        if [ -n "$MY_LINE" ]; then
           MY_NODE_ID=$(echo "$MY_LINE" | awk '{print $1}')
           IS_MASTER=$(echo "$MY_LINE" | awk '{print $3}' | grep -q "master" && echo "true" || echo "false")
           HAS_SLOTS=$(echo "$MY_LINE" | awk '{print $9}')

           if [ "$IS_MASTER" = "true" ] && [ -n "$HAS_SLOTS" ]; then
             STILL_OWN_SLOTS=true
             log "Attempt $i/12: Peer $peer still sees me as master with slots assigned ($HAS_SLOTS). Waiting..."

             # If we've waited > 30s (attempt 6), try to find a replica to force failover
             if [ "$i" -ge 6 ]; then
                MY_REPLICA_IP=$(echo "$NODES_INFO" | grep "slave $MY_NODE_ID" | grep -v "fail" | head -n 1 | awk '{print $2}' | cut -d: -f1 | cut -d@ -f1)
                if [ -n "$MY_REPLICA_IP" ]; then
                   log "Deadlock detected! Forcing replica $MY_REPLICA_IP to TAKEOVER slots for dead node $MY_NODE_ID..."
                   redis-cli -h "$MY_REPLICA_IP" -p [[.RedisPort]] $AUTH_ARGS [[.TLSFlags]] -t 2 CLUSTER FAILOVER TAKEOVER || true
                fi
             fi
             break
           fi
        fi
      fi
    done

    if [ "$STILL_OWN_SLOTS" = "false" ] && [ "$PEERS_CONTACTED" -gt 0 ]; then
      log "Verified: Failover confirmed (I no longer own slots). Safe to start."
      break
    fi
    [ "$i" -lt 12 ] && sleep 5
  done

  if [ "$STILL_OWN_SLOTS" = "true" ]; then
    log "FATAL: Failover timeout. I still own slots in the cluster map. Yielding to Kubernetes liveness probe..."
    rm -f /data/bootstrap-in-progress
    while true; do sleep 3600; done
  fi
fi

# STEP 4: Guarantee a fresh node ID on every process start
if [ "$RESTART_DETECTED" = "true" ]; then
  log "Removing stale nodes.conf"
  rm -f /data/nodes.conf
fi

# STEP 5: Start Redis
log "Final launch..."
rm -f /data/bootstrap-in-progress
exec redis-server /data/redis.conf \
  --cluster-enabled yes \
  --cluster-config-file /data/nodes.conf \
  --cluster-announce-ip ${POD_IP} \
  --cluster-announce-port [[.RedisPort]] \
  --cluster-announce-bus-port [[.ClusterBusPort]] \
  --requirepass "${REDIS_PASSWORD}" \
  --masterauth "${REDIS_PASSWORD}"
`
	tmpl := template.Must(template.New("startup").Delims("[[", "]]").Parse(startupScript))
	var buf bytes.Buffer
	tlsFlags := ""
	if lr.Spec.TLS.Enabled {
		tlsFlags = tlsInsecureFlags
	}
	err := tmpl.Execute(&buf, struct {
		Name           string
		Namespace      string
		RedisPort      int
		ClusterBusPort int
		TLSFlags       string
	}{
		Name:           lr.Name,
		Namespace:      lr.Namespace,
		RedisPort:      littleredv1alpha1.RedisPort,
		ClusterBusPort: littleredv1alpha1.ClusterBusPort,
		TLSFlags:       tlsFlags,
	})
	if err != nil {
		// This should never happen with Must() and valid data
		panic(fmt.Sprintf("failed to execute cluster startup template: %v", err))
	}
	script := buf.String()

	preStopTmpl := template.Must(template.New("cluster-prestop").Delims("[[", "]]").Parse(`
# Redirect all output from this point forward
exec >/proc/1/fd/1 2>&1

echo "preStop hook starting at $(date), PID $$"

AUTH_ARGS=""
if [ -n "$REDIS_PASSWORD" ]; then
  AUTH_ARGS="-a $REDIS_PASSWORD --no-auth-warning"
fi

# Check if I am a master
MY_ID=$(redis-cli $AUTH_ARGS [[.TLSFlags]] cluster nodes | grep myself | awk '{print $1}')
IS_MASTER=$(redis-cli $AUTH_ARGS [[.TLSFlags]] cluster nodes | grep myself | grep -q master && echo "yes" || echo "no")
echo "preStop: my node ID is $MY_ID, master=$IS_MASTER"

if [ "$IS_MASTER" != "yes" ]; then
  echo "preStop: I am not a master. Safe to stop."
  exit 0
fi

# Find a healthy replica for my shard
REPLICA_IP=$(redis-cli $AUTH_ARGS [[.TLSFlags]] cluster nodes | grep "$MY_ID" | grep "slave" | grep -v "fail" | head -n 1 | awk '{print $2}' | cut -d: -f1 | cut -d@ -f1)

if [ -z "$REPLICA_IP" ]; then
  echo "preStop: No healthy replica found to take over. Proceeding with restart."
  exit 0
fi

echo "preStop: Requesting replica $REPLICA_IP to take over shard..."
redis-cli -h $REPLICA_IP $AUTH_ARGS [[.TLSFlags]] CLUSTER FAILOVER

# Wait for role swap (I become slave)
for i in $(seq 1 10); do
  NEW_ROLE=$(redis-cli $AUTH_ARGS [[.TLSFlags]] cluster nodes | grep myself | awk '{print $3}')
  if echo "$NEW_ROLE" | grep -q "slave"; then
    echo "preStop: Failover successful. I am now a replica."
    exit 0
  fi
  echo "preStop: Waiting for role swap ($i/10)..."
  sleep 1
done
echo "preStop: Failover did not complete within 10s. Proceeding with shutdown."`))
	var preStopBuf bytes.Buffer
	clusterPreStopTLSFlags := ""
	if lr.Spec.TLS.Enabled {
		clusterPreStopTLSFlags = tlsInsecureFlags
	}
	err = preStopTmpl.Execute(&preStopBuf, struct {
		TLSFlags string
	}{
		TLSFlags: clusterPreStopTLSFlags,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to execute cluster prestop template: %v", err))
	}
	preStopScript := preStopBuf.String()

	container := corev1.Container{
		Name:            ComponentRedis,
		Image:           lr.Spec.Image.FullImage(),
		ImagePullPolicy: lr.Spec.Image.PullPolicy,
		Command:         []string{"sh", "-c", script},
		Ports: []corev1.ContainerPort{
			{
				Name:          ComponentRedis,
				ContainerPort: int32(littleredv1alpha1.RedisPort),
				Protocol:      corev1.ProtocolTCP,
			},
			{
				Name:          "cluster-bus",
				ContainerPort: int32(littleredv1alpha1.ClusterBusPort),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Resources: lr.Spec.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      volNameConfig,
				MountPath: mountPathEtcRedis,
				ReadOnly:  true,
			},
			{
				Name:      volNameData,
				MountPath: mountPathDataDir,
			},
		},
		LivenessProbe:  buildClusterLivenessProbe(lr),
		ReadinessProbe: buildClusterReadinessProbe(lr),
		Lifecycle: &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"sh", "-c", preStopScript},
				},
			},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(true),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{capAll},
			},
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
	}

	// Add TLS volume mounts
	if lr.Spec.TLS.Enabled {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volNameTLS,
			MountPath: mountPathTLS,
			ReadOnly:  true,
		})
		if lr.Spec.TLS.CACertSecret != "" && lr.Spec.TLS.CACertSecret != lr.Spec.TLS.ExistingSecret {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      volNameCACert,
				MountPath: mountPathTLSCA,
				ReadOnly:  true,
			})
		}
	}

	// Add POD_IP env var (required for cluster-announce-ip)
	container.Env = append(container.Env, corev1.EnvVar{
		Name: envPodIP,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: fieldPathPodIP,
			},
		},
	})

	// Add POD_NAME env var (useful for logging)
	container.Env = append(container.Env, corev1.EnvVar{
		Name: "POD_NAME",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.name",
			},
		},
	})

	// Add NAMESPACE env var
	container.Env = append(container.Env, corev1.EnvVar{
		Name: "NAMESPACE",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.namespace",
			},
		},
	})

	// Add CLUSTER_NAME env var
	container.Env = append(container.Env, corev1.EnvVar{
		Name:  "CLUSTER_NAME",
		Value: lr.Name,
	})

	// Add auth env var
	if lr.Spec.Auth.Enabled {
		container.Env = append(container.Env, corev1.EnvVar{
			Name: envRedisPassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: lr.Spec.Auth.ExistingSecret,
					},
					Key: secretKeyPassword,
				},
			},
		})
	}

	return container
}

// buildClusterVolumes creates volumes for cluster mode pods
func buildClusterVolumes(lr *littleredv1alpha1.LittleRed) []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: volNameConfig,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configMapName(lr),
					},
				},
			},
		},
		{
			Name: volNameData,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	// Add TLS volumes
	if lr.Spec.TLS.Enabled {
		volumes = append(volumes, corev1.Volume{
			Name: volNameTLS,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: lr.Spec.TLS.ExistingSecret,
				},
			},
		})
		if lr.Spec.TLS.CACertSecret != "" {
			volumes = append(volumes, corev1.Volume{
				Name: volNameCACert,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: lr.Spec.TLS.CACertSecret,
					},
				},
			})
		}
	}

	return volumes
}

// buildClusterLivenessProbe creates the liveness probe for cluster mode
func buildClusterLivenessProbe(lr *littleredv1alpha1.LittleRed) *corev1.Probe {
	cmd := []string{
		bootstrapGuardScript,
		binRedisCli,
	}
	if lr.Spec.Auth.Enabled {
		cmd = append(cmd, "-a", "$(REDIS_PASSWORD)", "--no-auth-warning")
	}
	if lr.Spec.TLS.Enabled {
		cmd = append(cmd, "--tls", "--insecure")
	}
	cmd = append(cmd, "-t", "2", "ping")

	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"sh", "-c",
					strings.Join(cmd, " "),
				},
			},
		},
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    3,
	}
}

// buildClusterReadinessProbe creates the readiness probe for cluster mode
func buildClusterReadinessProbe(lr *littleredv1alpha1.LittleRed) *corev1.Probe {
	cmd := []string{
		"if [ -f /data/bootstrap-in-progress ]; then exit 1; fi;",
		binRedisCli,
	}
	if lr.Spec.Auth.Enabled {
		cmd = append(cmd, "-a", "$(REDIS_PASSWORD)", "--no-auth-warning")
	}
	if lr.Spec.TLS.Enabled {
		cmd = append(cmd, "--tls", "--insecure")
	}
	cmd = append(cmd, "-t", "2", "ping")

	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"sh", "-c",
					strings.Join(cmd, " "),
				},
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      3,
		FailureThreshold:    3,
	}
}

// buildClusterHeadlessService creates the headless Service for cluster pods
func buildClusterHeadlessService(lr *littleredv1alpha1.LittleRed) *corev1.Service {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentCluster

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterHeadlessServiceName(lr),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			ClusterIP:                serviceClusterNone,
			Selector:                 clusterSelectorLabels(lr),
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{
					Name:       ComponentRedis,
					Port:       int32(littleredv1alpha1.RedisPort),
					TargetPort: intstr.FromString(ComponentRedis),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "cluster-bus",
					Port:       int32(littleredv1alpha1.ClusterBusPort),
					TargetPort: intstr.FromString("cluster-bus"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}

// buildClusterClientService creates the client Service for cluster mode
func buildClusterClientService(lr *littleredv1alpha1.LittleRed) *corev1.Service {
	labels := commonLabels(lr)
	maps.Copy(labels, lr.Spec.Service.Labels)

	ports := []corev1.ServicePort{
		{
			Name:       ComponentRedis,
			Port:       int32(littleredv1alpha1.RedisPort),
			TargetPort: intstr.FromString(ComponentRedis),
			Protocol:   corev1.ProtocolTCP,
		},
	}

	if lr.Spec.Metrics.IsEnabled() {
		ports = append(ports, corev1.ServicePort{
			Name:       portNameMetrics,
			Port:       int32(littleredv1alpha1.RedisExporterPort),
			TargetPort: intstr.FromString(portNameMetrics),
			Protocol:   corev1.ProtocolTCP,
		})
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        serviceName(lr),
			Namespace:   lr.Namespace,
			Labels:      labels,
			Annotations: serviceAnnotations(lr),
		},
		Spec: corev1.ServiceSpec{
			Type:     lr.Spec.Service.Type,
			Selector: clusterSelectorLabels(lr),
			Ports:    ports,
		},
	}
}

// ============================================================================
// PodDisruptionBudget builders
// ============================================================================

// pdbSpec returns the PodDisruptionBudgetSpec fields resolved from the LittleRed spec,
// defaulting to maxUnavailable=1 when neither field is set.
func pdbSpec(lr *littleredv1alpha1.LittleRed) (maxUnavailable, minAvailable *intstr.IntOrString) {
	if lr.Spec.PodDisruptionBudget.MinAvailable != nil {
		return nil, lr.Spec.PodDisruptionBudget.MinAvailable
	}
	if lr.Spec.PodDisruptionBudget.MaxUnavailable != nil {
		return lr.Spec.PodDisruptionBudget.MaxUnavailable, nil
	}
	defaultMax := intstr.FromInt32(1)
	return &defaultMax, nil
}

func buildSentinelRedisPDB(lr *littleredv1alpha1.LittleRed) *policyv1.PodDisruptionBudget {
	maxUnavailable, minAvailable := pdbSpec(lr)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podDisruptionBudgetName(lr),
			Namespace: lr.Namespace,
			Labels:    commonLabels(lr),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: maxUnavailable,
			MinAvailable:   minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: redisSelectorLabels(lr),
			},
		},
	}
}

func buildSentinelPDB(lr *littleredv1alpha1.LittleRed) *policyv1.PodDisruptionBudget {
	maxUnavailable, minAvailable := pdbSpec(lr)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentinelPodDisruptionBudgetName(lr),
			Namespace: lr.Namespace,
			Labels:    commonLabels(lr),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: maxUnavailable,
			MinAvailable:   minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: sentinelSelectorLabels(lr),
			},
		},
	}
}

// buildClusterShardPDB creates the PodDisruptionBudget for a single cluster shard.
// One PDB per shard scopes the disruption budget to the shard's failure domain — a
// voluntary drain can take at most maxUnavailable (default 1) pod of any one shard, so
// it can never evict a whole shard at once. Only created when the shard is redundant
// (replicasPerShard > 0); see reconcileClusterPDB.
func buildClusterShardPDB(lr *littleredv1alpha1.LittleRed, shardIdx int) *policyv1.PodDisruptionBudget {
	maxUnavailable, minAvailable := pdbSpec(lr)
	labels := commonLabels(lr)
	labels[LabelShard] = clusterShardLabelValue(shardIdx)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterShardPDBName(lr, shardIdx),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: maxUnavailable,
			MinAvailable:   minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: clusterShardSelectorLabels(lr, shardIdx),
			},
		},
	}
}
