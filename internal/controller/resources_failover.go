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
	"fmt"
	"maps"
	"strings"
	"text/template"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// ============================================================================
// Failover Mode Resources (ADR-011)
//
// Operator-managed HA without Sentinel: one Redis StatefulSet (1 + replicas
// pods), the label-routed master Service and the replicas headless Service
// reused verbatim from sentinel mode (they are mode-agnostic label routing —
// the master label being the sole writer-selector authority is the point of
// the mode), and no Sentinel resources of any kind.
//
// The Sentinel query loop of pillar 3.6 is replaced by an operator-stamped
// assignment channel: the operator patches assigned-role / assigned-master-ip /
// assignment-epoch annotations onto each data pod, and the pod reads them back
// through a downward-API volume (see buildFailoverVolumes and the startup
// script in buildRedisContainerFailover).
// ============================================================================

const (
	// volNamePodInfo / mountPathPodInfo: the downward-API volume exposing the
	// pod's own annotations to the startup script (ADR-011 §3). The kubelet
	// rewrites the projected file whenever the pod's annotations change, so
	// polling it observes operator (re-)assignments without any API access.
	volNamePodInfo   = "podinfo"
	mountPathPodInfo = "/podinfo"
	// fileAnnotations is the projected file name; the script polls
	// mountPathPodInfo/fileAnnotations.
	fileAnnotations = "annotations"
	// runMarkerPath is the epoch run-marker on the EmptyDir: it survives a
	// container restart (same pod, same IP, wiped dataset) but not a pod
	// replacement — the ADR-001 same-IP kill-9 hazard, re-owned (ADR-011 §3).
	// startMarkerPath records the assignment epoch THIS PROCESS STARTED UNDER.
	// Named for what it is: not "the epoch we are at" but "what my current
	// incarnation booted with". The distinction is load-bearing (LR-038).
	startMarkerPath = "/data/littlered-started-under-epoch"
)

// failoverSpecOrDefault returns the (defaulted) FailoverSpec, tolerating a nil
// spec.failover the same way the sentinel/cluster builders do.
func failoverSpecOrDefault(lr *littleredv1alpha1.LittleRed) *littleredv1alpha1.FailoverSpec {
	f := lr.Spec.Failover
	if f == nil {
		f = &littleredv1alpha1.FailoverSpec{}
		f.SetDefaults()
	}
	return f
}

// buildRedisConfigFailover generates redis.conf for failover mode. It is the
// sentinel-mode data-pod config (replication settings, persistence disabled,
// strict in-memory posture) without anything Sentinel-specific — there is no
// sentinel.conf sibling in this mode — plus the optional min-replicas-to-write
// knob: rendered only when spec.failover.minReplicasToWrite > 0, omitted at 0
// so the Redis default (off) applies.
func buildRedisConfigFailover(lr *littleredv1alpha1.LittleRed) string {
	var sb strings.Builder

	failover := failoverSpecOrDefault(lr)

	sb.WriteString("# LittleRed generated configuration (failover mode)\n")
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

	// min-replicas-to-write (ADR-011 §1): off by default (parity with sentinel
	// mode keeps the graduation-gate comparison honest); bounded write loss is
	// an explicit user choice.
	if failover.MinReplicasToWrite > 0 {
		sb.WriteString("\n# Write-safety bound (spec.failover.minReplicasToWrite)\n")
		fmt.Fprintf(&sb, "min-replicas-to-write %d\n", failover.MinReplicasToWrite)
	}

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

// buildConfigMapFailoverMode creates the ConfigMap for redis.conf in failover mode
func buildConfigMapFailoverMode(lr *littleredv1alpha1.LittleRed) *corev1.ConfigMap {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentRedis

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(lr),
			Namespace: lr.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			fileRedisConf: buildRedisConfigFailover(lr),
		},
	}
}

// buildFailoverVolumes returns the volumes for the failover-mode data pods:
// the shared config/data(/TLS) plumbing plus the downward-API volume that
// projects the pod's own annotations — the operator-assignment channel the
// startup script polls (ADR-011 §3).
func buildFailoverVolumes(lr *littleredv1alpha1.LittleRed) []corev1.Volume {
	volumes := buildVolumes(lr)
	volumes = append(volumes, corev1.Volume{
		Name: volNamePodInfo,
		VolumeSource: corev1.VolumeSource{
			DownwardAPI: &corev1.DownwardAPIVolumeSource{
				Items: []corev1.DownwardAPIVolumeFile{
					{
						Path: fileAnnotations,
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.annotations",
						},
					},
				},
			},
		},
	})
	return volumes
}

// buildRedisStatefulSetFailover creates the Redis StatefulSet for failover
// mode: 1 master + spec.failover.replicas replicas, parallel pod management.
// Same shape as the sentinel-mode data StatefulSet (governing service is the
// replicas headless service, selector component=redis so the label-routed
// master Service works unchanged), with the downward-API assignment volume
// added.
func buildRedisStatefulSetFailover(lr *littleredv1alpha1.LittleRed) *appsv1.StatefulSet {
	labels := commonLabels(lr)
	labels[labelAppComponent] = ComponentRedis

	podLabels := make(map[string]string)
	maps.Copy(podLabels, redisSelectorLabels(lr))
	maps.Copy(podLabels, lr.Spec.PodTemplate.Labels)

	// Compute config hash for pod annotations to trigger rolling update on config change
	configData := map[string]string{fileRedisConf: buildRedisConfigFailover(lr)}
	configHash := computeConfigHash(configData)

	podAnnotations := make(map[string]string)
	maps.Copy(podAnnotations, lr.Spec.PodTemplate.Annotations)
	podAnnotations[AnnotationConfigHash] = configHash

	failover := failoverSpecOrDefault(lr)
	replicas := 1 + *failover.Replicas

	containers := []corev1.Container{buildRedisContainerFailover(lr)}

	// Add exporter sidecar if metrics enabled
	if lr.Spec.Metrics.IsEnabled() {
		containers = append(containers, buildExporterContainer(lr, int32(littleredv1alpha1.RedisPort)))
	}

	// MinReadySeconds for failover mode: during a rolling update the operator
	// performs the graceful handover itself (ADR-011 §7) — detection is the
	// downAfterMilliseconds window (default 5s) plus promote/repoint, far below
	// sentinel mode's 30s SDOWN. 15s gives the transition room to settle before
	// the next pod rolls; the user's UpdateStrategy.MinReadySeconds wins.
	minReadySeconds := int32(15)
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
					Volumes:                   buildFailoverVolumes(lr),
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

// buildRedisContainerFailover creates the Redis container for failover mode.
//
// The startup protocol (ADR-011 §3) keeps pillar 3.6 — a Redis pod does not
// start redis-server until the operator has assigned it a role — but replaces
// the Sentinel query loop with the operator-stamped assignment annotations,
// read back through the downward-API volume. The kill-9 same-IP hazard
// (ADR-001) is owned by the epoch gate: a container restart keeps the pod,
// its annotations, and the EmptyDir run-marker, so the stale assignment is
// "already consumed" and the script PARKS in the wait-loop — that parking IS
// the yield; no Sentinel run-id query is needed. Only an operator epoch bump
// (its global view deciding failover or re-authorization) releases it.
func buildRedisContainerFailover(lr *littleredv1alpha1.LittleRed) corev1.Container {
	script := `#!/bin/sh
set -e

# Helper to log with timestamp
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') [Startup] $1"
}

# Create marker file to tell the probes we are starting up (liveness passes,
# readiness fails while we are parked here waiting for an assignment).
touch /data/bootstrap-in-progress

cp /etc/redis/redis.conf /data/redis.conf

ANNOTATIONS_FILE=[[.AnnotationsFile]]
START_MARKER=[[.StartMarker]]

# Read one annotation value from the downward-API projection. The kubelet
# renders one key="value" per line (value quoted/escaped) and rewrites the file
# whenever the pod's annotations change, so polling it observes operator
# (re-)assignments without any API-server access. cut keeps values containing
# '='; sed strips the surrounding quotes (our values embed none).
annotation() {
  grep "^$1=" "$ANNOTATIONS_FILE" 2>/dev/null | head -n 1 | cut -d= -f2- | sed -e 's/^"//' -e 's/"$//'
}

AUTH_ARGS=""
if [ -n "$REDIS_PASSWORD" ]; then
  AUTH_ARGS="--requirepass $REDIS_PASSWORD --masterauth $REDIS_PASSWORD"
fi

# Start gate (ADR-011 §3; the ADR-001 same-IP kill-9 hazard, re-owned).
#
# The marker survives a container restart (same pod, same IP, dataset wiped in
# RAM) but not a pod replacement, so its presence means exactly: "I am a
# restarted process and I hold no data."
#
# TWO DIFFERENT RULES, because the two roles carry different risk (LR-038):
#
#   replica  ordering suffices. Honor an assignment whose epoch is GREATER than
#            the one we started under. Starting as a replica of the live master
#            loses nothing (we resync), so a stale replica assignment is
#            harmless and the operator's re-authorization releases us.
#
#   master   ordering does NOT suffice, and this is the subtle one. An in-place
#            promotion (the operator sending REPLICAOF NO ONE to a RUNNING
#            replica) advances the assignment epoch WITHOUT restarting the
#            process, so the marker is never rewritten and stays behind. A
#            later kill-9 then reads a strictly greater epoch and treats a
#            pre-death instruction as fresh — coming back as an EMPTY master
#            that the operator believes is healthy, whereupon the replicas
#            holding the only surviving copy are repointed onto it and resync
#            from nothing. Measured: 352 of 1145 acknowledged writes destroyed,
#            with corruptions 0 and write availability 95.50%.
#
#            So a restarted process may only start as master with the operator's
#            EXPLICIT permission, stamped after it observed the restart and
#            established that no data is at risk. Permission cannot predate the
#            death it refers to, which is what closes the race that ordering
#            alone cannot.
#
# Parking is the yield either way: not Ready, serving nothing, so the operator
# sees a dead master and fails over to the pod that actually holds the data.
STARTED_UNDER_EPOCH=""
[ -f "$START_MARKER" ] && STARTED_UNDER_EPOCH=$(cat "$START_MARKER")

log "Starting Redis node $(hostname) with IP ${POD_IP}. Waiting for operator assignment..."
log "Auth enabled: $([ -n "$REDIS_PASSWORD" ] && echo yes || echo no)"
[ -n "$STARTED_UNDER_EPOCH" ] && log "Start marker found (started under epoch $STARTED_UNDER_EPOCH): this is a RESTARTED process holding no data; a fresher assignment is required, and starting as master additionally requires explicit operator authorization."

while true; do
  ASSIGNED_ROLE=$(annotation "[[.AnnRole]]")
  ASSIGNED_MASTER_IP=$(annotation "[[.AnnMasterIP]]")
  ASSIGNMENT_EPOCH=$(annotation "[[.AnnEpoch]]")

  if [ -z "$ASSIGNED_ROLE" ] || [ -z "$ASSIGNMENT_EPOCH" ]; then
    log "No operator assignment yet. Waiting..."
    sleep 2
    continue
  fi

  if [ "$ASSIGNED_ROLE" = "replica" ] && [ -z "$ASSIGNED_MASTER_IP" ]; then
    log "Replica assignment (epoch $ASSIGNMENT_EPOCH) carries no master IP yet. Waiting..."
    sleep 2
    continue
  fi

  if [ -n "$STARTED_UNDER_EPOCH" ] && ! [ "$ASSIGNMENT_EPOCH" -gt "$STARTED_UNDER_EPOCH" ] 2>/dev/null; then
    log "Assignment epoch $ASSIGNMENT_EPOCH is not newer than the epoch this process started under ($STARTED_UNDER_EPOCH); waiting for operator re-authorization..."
    sleep 2
    continue
  fi

  # A restarted process starting as MASTER needs explicit authorization, not
  # just a newer epoch (see the start-gate note above).
  if [ "$ASSIGNED_ROLE" = "master" ] && [ -n "$STARTED_UNDER_EPOCH" ]; then
    MASTER_START_AUTH=$(annotation "[[.AnnMasterStartAuth]]")
    if [ -z "$MASTER_START_AUTH" ] || ! [ "$MASTER_START_AUTH" -gt "$STARTED_UNDER_EPOCH" ] 2>/dev/null; then
      log "Master assignment (epoch $ASSIGNMENT_EPOCH) carries no operator authorization to START as master newer than epoch $STARTED_UNDER_EPOCH (found '''$MASTER_START_AUTH'''). This process restarted and holds no data; parking so the operator can fail over to a pod that does..."
      sleep 2
      continue
    fi
    log "Operator authorized this restarted process to start as master (authorization epoch $MASTER_START_AUTH > $STARTED_UNDER_EPOCH)."
  fi

  # Record what this process is starting under BEFORE exec, so a container
  # restart replaying the same annotations parks above instead of re-honoring
  # them. Note this records the START only: an in-place role change made later
  # by the operator is invisible here, which is exactly why the master path
  # needs its own authorization rather than trusting epoch ordering.
  echo "$ASSIGNMENT_EPOCH" > "$START_MARKER"
  rm -f /data/bootstrap-in-progress

  if [ "$ASSIGNED_ROLE" = "master" ]; then
    log "Assignment epoch $ASSIGNMENT_EPOCH: I am the authorized master. Starting redis-server..."
    exec redis-server /data/redis.conf --replica-announce-ip ${POD_IP} $AUTH_ARGS
  fi

  # Replica: deliberately NO reachability check before starting (ADR-002's
  # deadlock constraint) — redis-server retries an unreachable master itself,
  # and the operator repoints us if the target is truly dead.
  log "Assignment epoch $ASSIGNMENT_EPOCH: joining $ASSIGNED_MASTER_IP:[[.RedisPort]] as replica..."
  exec redis-server /data/redis.conf --replicaof $ASSIGNED_MASTER_IP [[.RedisPort]] --replica-announce-ip ${POD_IP} $AUTH_ARGS
done`
	tmpl := template.Must(template.New("failover-startup").Delims("[[", "]]").Parse(script))
	var buf bytes.Buffer
	err := tmpl.Execute(&buf, struct {
		AnnotationsFile    string
		StartMarker        string
		AnnRole            string
		AnnMasterIP        string
		AnnEpoch           string
		AnnMasterStartAuth string
		RedisPort          int
	}{
		AnnotationsFile:    mountPathPodInfo + "/" + fileAnnotations,
		StartMarker:        startMarkerPath,
		AnnRole:            AnnotationAssignedRole,
		AnnMasterIP:        AnnotationAssignedMasterIP,
		AnnEpoch:           AnnotationAssignmentEpoch,
		AnnMasterStartAuth: AnnotationMasterStartAuthorizedEpoch,
		RedisPort:          littleredv1alpha1.RedisPort,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to execute failover startup template: %v", err))
	}
	finalScript := buf.String()

	preStopScript := buildFailoverPreStop(lr)

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
			{
				Name:      volNamePodInfo,
				MountPath: mountPathPodInfo,
				ReadOnly:  true,
			},
		},
		Env: []corev1.EnvVar{
			{
				Name: envPodIP,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: fieldPathPodIP,
					},
				},
			},
		},
		LivenessProbe:  buildFailoverLivenessProbe(lr),
		ReadinessProbe: buildFailoverReadinessProbe(lr),
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

// failoverPreStopWaitIterations x failoverPreStopWaitSeconds bounds how long the
// hook waits for the operator to move mastership away. The product matches the
// old fixed `sleep 10`, so the worst case is unchanged and stays well inside the
// default 30s termination grace period; the difference is that the hook now
// leaves as soon as the handover is done instead of always sleeping it out.
const (
	failoverPreStopWaitIterations = 10
	failoverPreStopWaitSeconds    = 1
)

// buildFailoverPreStop renders the failover-mode preStop hook: SELF-FENCE, then
// wait for the operator to move mastership away, then exit promptly.
//
// WHY THE POD AND NOT THE OPERATOR (LR-038). ADR-011 §7 made the handover
// operator-led and left the hook as a bare `sleep 10`, on the reading that a pod
// must make no topology decisions (LR-016). That reading was too broad: LR-016
// forbids inferring the state of OTHER nodes — the mistake was a probe restarting
// a replica because its master looked unreachable. "I am being terminated" is
// local knowledge that cannot be wrong, and the pod is the only party that has it
// instantly. Acting on it is not a topology decision.
//
// This matters because operator-side fencing can only ever win a race, while the
// pod cannot lose one: the write path closes before the operator has even noticed
// the deletionTimestamp. Measured, the operator-side fence alone left 202 of 1171
// acknowledged writes lost per two graceful failovers before it worked at all.
//
// The fence is deliberately TARGET-FREE. `min-replicas-to-write 99` cannot be
// satisfied, so writes fail -NOREPLICAS immediately, and we never need to know
// who the new master is — needing that would reintroduce exactly the race this
// removes. Reads keep serving and replication of already-ACKed writes continues,
// so the promoted replica still receives them. Nothing is persisted (no CONFIG
// REWRITE), so a container restart clears it.
//
// Then it waits for mastership to move and exits at once when it has. The old
// fixed sleep held the pod — and with it the client's pinned TCP connection,
// which a label flip does not re-route — for the full grace window even though
// the handover completed in about a second, turning ~9s per failover into refused
// writes. A replica exits immediately, since it has nothing to hand over (which
// also speeds up rolling updates).
func buildFailoverPreStop(lr *littleredv1alpha1.LittleRed) string {
	tmpl := template.Must(template.New("failover-prestop").Delims("[[", "]]").Parse(`
exec >/proc/1/fd/1 2>&1
echo "preStop: starting at $(date)"

AUTH_ARGS=""
if [ -n "$REDIS_PASSWORD" ]; then
  AUTH_ARGS="-a $REDIS_PASSWORD --no-auth-warning"
fi

myrole() {
  redis-cli $AUTH_ARGS [[.TLSFlags]] -t 2 info replication 2>/dev/null \
    | grep -F 'role:' | head -n 1 | cut -d: -f2 | tr -d '\r'
}

ROLE=$(myrole)
echo "preStop: my role is '$ROLE'"

if [ "$ROLE" != "master" ]; then
  echo "preStop: not the master — nothing to hand over, exiting immediately"
  exit 0
fi

# SELF-FENCE. Target-free on purpose (see buildFailoverPreStop).
if redis-cli $AUTH_ARGS [[.TLSFlags]] -t 2 CONFIG SET min-replicas-to-write 99 >/dev/null 2>&1; then
  echo "preStop: writes fenced (-NOREPLICAS); reads and replication continue"
else
  echo "preStop: WARNING could not fence writes — the operator-side fence is the backstop"
fi

# Wait for the operator to move mastership away, then leave at once.
i=0
while [ $i -lt [[.WaitIterations]] ]; do
  R=$(myrole)
  if [ "$R" != "master" ]; then
    echo "preStop: mastership handed over (role now '$R'), exiting"
    exit 0
  fi
  i=$((i + 1))
  sleep [[.WaitSeconds]]
done
echo "preStop: still master after the wait budget — exiting anyway, writes stay fenced"
exit 0
`))

	tlsFlags := ""
	if lr.Spec.TLS.Enabled {
		tlsFlags = tlsInsecureFlags
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, struct {
		TLSFlags       string
		WaitIterations int
		WaitSeconds    int
	}{
		TLSFlags:       tlsFlags,
		WaitIterations: failoverPreStopWaitIterations,
		WaitSeconds:    failoverPreStopWaitSeconds,
	}); err != nil {
		panic(fmt.Sprintf("failed to execute failover preStop template: %v", err))
	}
	return buf.String()
}

// buildFailoverLivenessProbe creates the liveness probe for the failover-mode
// Redis container: a plain local health check (bootstrap guard + local PING),
// identical to sentinel mode's. Probes make NO topology decisions (LR-016) —
// a replica whose master is unreachable is healthy-and-waiting; the operator
// owns every repoint/promote decision. Delegates to the sentinel builder so
// the two modes cannot drift apart (§7 cross-mode parity).
func buildFailoverLivenessProbe(lr *littleredv1alpha1.LittleRed) *corev1.Probe {
	return buildSentinelLivenessProbe(lr)
}

// buildFailoverReadinessProbe creates the readiness probe for the failover-mode
// Redis container: a master passes on role:master, a replica requires
// master_link_status:up — so a masterless or mis-pointed replica is pulled
// from traffic without being killed. Same contract as sentinel mode
// (local-INFO-based); delegates for the same anti-drift reason.
func buildFailoverReadinessProbe(lr *littleredv1alpha1.LittleRed) *corev1.Probe {
	return buildSentinelReadinessProbe(lr)
}

// buildFailoverRedisPDB creates the PodDisruptionBudget over the failover-mode
// data pods. Failover mode always runs >= 2 data pods (1 + replicas, replicas
// >= 1), so the PDB redundancy rule (never PDB a single-pod deployment) is
// satisfied by construction. Identical shape to sentinel mode's data-pod PDB
// (same name helper, same redis selector); delegates to avoid drift.
func buildFailoverRedisPDB(lr *littleredv1alpha1.LittleRed) *policyv1.PodDisruptionBudget {
	return buildSentinelRedisPDB(lr)
}
