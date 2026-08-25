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

package v1alpha1

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// LittleRedSpec defines the desired state of LittleRed
// +kubebuilder:validation:XValidation:rule="self.mode == 'cluster' || !has(self.cluster)",message="spec.cluster may only be set when spec.mode is 'cluster'"
// +kubebuilder:validation:XValidation:rule="self.mode == 'sentinel' || !has(self.sentinel)",message="spec.sentinel may only be set when spec.mode is 'sentinel'"
// +kubebuilder:validation:XValidation:rule="self.mode == 'failover' || !has(self.failover)",message="spec.failover may only be set when spec.mode is 'failover'"
type LittleRedSpec struct {
	// Mode is the deployment mode: standalone, sentinel, cluster, or failover (experimental)
	// +kubebuilder:validation:Enum=standalone;sentinel;cluster;failover
	// +kubebuilder:default=standalone
	// +optional
	Mode string `json:"mode,omitempty"`

	// Image defines the container image to use
	// +optional
	Image ImageSpec `json:"image,omitempty"`

	// Resources defines CPU/memory for Redis container
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Config defines Redis configuration
	// +optional
	Config ConfigSpec `json:"config,omitempty"`

	// Auth defines authentication settings
	// +optional
	Auth AuthSpec `json:"auth,omitempty"`

	// TLS defines TLS/SSL settings
	// +optional
	TLS TLSSpec `json:"tls,omitempty"`

	// Metrics defines Prometheus metrics settings
	// +optional
	Metrics MetricsSpec `json:"metrics,omitempty"`

	// UpdateStrategy defines how updates are rolled out
	// +optional
	UpdateStrategy UpdateStrategySpec `json:"updateStrategy,omitempty"`

	// Service defines Service configuration
	// +optional
	Service ServiceSpec `json:"service,omitempty"`

	// PodTemplate defines pod customizations
	// +optional
	PodTemplate PodTemplateSpec `json:"podTemplate,omitempty"`

	// RequeueIntervals defines how often the operator checks the state.
	// This is useful for tuning large-scale installations to avoid API server pressure.
	// +optional
	RequeueIntervals *RequeueIntervals `json:"requeueIntervals,omitempty"`

	// Sentinel defines sentinel-specific settings (sentinel mode only)
	// +optional
	Sentinel *SentinelSpec `json:"sentinel,omitempty"`

	// Cluster defines cluster-specific settings (cluster mode only)
	// +optional
	Cluster *ClusterSpec `json:"cluster,omitempty"`

	// Failover defines failover-specific settings (failover mode only).
	// Mode failover is experimental: operator-managed HA without Sentinel,
	// under active validation — see docs for current status and trade-offs
	// vs sentinel mode.
	// +optional
	Failover *FailoverSpec `json:"failover,omitempty"`

	// PodDisruptionBudget defines PodDisruptionBudget settings
	// +optional
	PodDisruptionBudget PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`

	// Placement defines topology/failure-domain placement rules (cluster mode only)
	// +optional
	Placement *PlacementSpec `json:"placement,omitempty"`
}

// PodDisruptionBudgetSpec defines whether a PodDisruptionBudget should be created
type PodDisruptionBudgetSpec struct {
	// Create controls whether a PodDisruptionBudget is created for the StatefulSet(s).
	// Defaults to true; set to false to opt out.
	// +kubebuilder:default=true
	// +optional
	Create *bool `json:"create,omitempty"`

	// MaxUnavailable is the maximum number of pods that can be unavailable during a disruption.
	// Mutually exclusive with MinAvailable.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// MinAvailable is the minimum number of pods that must be available during a disruption.
	// Mutually exclusive with MaxUnavailable.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
}

// RequeueIntervals defines the timings for reconciliation loops
type RequeueIntervals struct {
	// Fast is the interval used when the system is initializing or recovering.
	// +kubebuilder:default="2s"
	// +optional
	Fast *metav1.Duration `json:"fast,omitempty"`

	// SteadyState is the interval used for periodic health checks when Running.
	// +kubebuilder:default="30s"
	// +optional
	SteadyState *metav1.Duration `json:"steadyState,omitempty"`
}

// ImageSpec defines container image configuration
type ImageSpec struct {
	// Registry is the container registry hostname
	// +kubebuilder:default="docker.io"
	// +optional
	Registry string `json:"registry,omitempty"`

	// Path is the image path (without registry or tag)
	// +kubebuilder:default="library/redis"
	// +optional
	Path string `json:"path,omitempty"`

	// Tag is the image version tag
	// +kubebuilder:default="8.4.2"
	// +optional
	Tag string `json:"tag,omitempty"`

	// PullPolicy is the image pull policy
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default=IfNotPresent
	// +optional
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`

	// PullSecrets are references to secrets for pulling the image
	// +optional
	PullSecrets []corev1.LocalObjectReference `json:"pullSecrets,omitempty"`
}

// FullImage returns the complete image reference: {registry}/{path}:{tag}
func (i *ImageSpec) FullImage() string {
	registry := i.Registry
	if registry == "" {
		registry = DefaultRegistry
	}
	path := i.Path
	if path == "" {
		path = DefaultImagePath
	}
	tag := i.Tag
	if tag == "" {
		tag = DefaultImageTag
	}
	return fmt.Sprintf("%s/%s:%s", registry, path, tag)
}

// ConfigSpec defines Redis configuration
type ConfigSpec struct {
	// Maxmemory sets Redis maxmemory (e.g., "1Gi")
	// +optional
	Maxmemory string `json:"maxmemory,omitempty"`

	// MaxmemoryPolicy sets the eviction policy
	// +kubebuilder:validation:Enum=noeviction;allkeys-lru;allkeys-lfu;allkeys-random;volatile-lru;volatile-lfu;volatile-random;volatile-ttl
	// +kubebuilder:default="noeviction"
	// +optional
	MaxmemoryPolicy string `json:"maxmemoryPolicy,omitempty"`

	// Timeout is client idle timeout in seconds (0 = disabled)
	// +kubebuilder:default=0
	// +optional
	Timeout int `json:"timeout,omitempty"`

	// TCPKeepalive interval in seconds
	// +kubebuilder:default=300
	// +optional
	TCPKeepalive int `json:"tcpKeepalive,omitempty"`

	// Raw is raw redis.conf content (expert mode)
	// +optional
	Raw string `json:"raw,omitempty"`
}

// AuthSpec defines authentication settings
type AuthSpec struct {
	// Enabled enables password authentication
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ExistingSecret is the name of a Secret containing the password key
	// +optional
	ExistingSecret string `json:"existingSecret,omitempty"`
}

// TLSSpec defines TLS/SSL settings
type TLSSpec struct {
	// Enabled enables TLS encryption
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ExistingSecret is the name of a Secret with tls.crt and tls.key
	// +optional
	ExistingSecret string `json:"existingSecret,omitempty"`

	// CACertSecret is the name of a Secret with ca.crt for client verification
	// +optional
	CACertSecret string `json:"caCertSecret,omitempty"`

	// ClientAuth requires client certificate authentication
	// +kubebuilder:default=false
	// +optional
	ClientAuth bool `json:"clientAuth,omitempty"`
}

// MetricsSpec defines Prometheus metrics settings
type MetricsSpec struct {
	// Enabled enables the redis_exporter sidecar
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Exporter defines the exporter container settings
	// +optional
	Exporter ExporterSpec `json:"exporter,omitempty"`

	// ServiceMonitor defines ServiceMonitor settings
	// +optional
	ServiceMonitor ServiceMonitorSpec `json:"serviceMonitor,omitempty"`
}

// IsEnabled returns whether metrics are enabled (defaults to true)
func (m *MetricsSpec) IsEnabled() bool {
	if m.Enabled == nil {
		return true
	}
	return *m.Enabled
}

// ExporterSpec defines the redis_exporter container settings
type ExporterSpec struct {
	// Registry is the container registry hostname (empty = inherit from spec.image.registry)
	// +optional
	Registry string `json:"registry,omitempty"`

	// Path is the image path
	// +kubebuilder:default="oliver006/redis_exporter"
	// +optional
	Path string `json:"path,omitempty"`

	// Tag is the image version tag.
	// Keep in sync with redis-exporter.Dockerfile (the source Dependabot bumps);
	// kubebuilder markers must be string literals so this cannot reference the const.
	// +kubebuilder:default="v1.89.0"
	// +optional
	Tag string `json:"tag,omitempty"`

	// Resources defines CPU/memory for exporter container
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// FullImage returns the complete exporter image reference
func (e *ExporterSpec) FullImage(mainRegistry string) string {
	registry := e.Registry
	if registry == "" {
		registry = mainRegistry
		if registry == "" {
			registry = DefaultRegistry
		}
	}
	path := e.Path
	if path == "" {
		path = DefaultExporterPath
	}
	tag := e.Tag
	if tag == "" {
		tag = DefaultExporterTag
	}
	return fmt.Sprintf("%s/%s:%s", registry, path, tag)
}

// ServiceMonitorSpec defines ServiceMonitor settings
type ServiceMonitorSpec struct {
	// Enabled creates a ServiceMonitor CR
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Namespace overrides the ServiceMonitor namespace
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Labels are additional labels for the ServiceMonitor
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Interval is the scrape interval
	// +kubebuilder:default="30s"
	// +optional
	Interval string `json:"interval,omitempty"`

	// ScrapeTimeout is the scrape timeout
	// +kubebuilder:default="10s"
	// +optional
	ScrapeTimeout string `json:"scrapeTimeout,omitempty"`
}

// UpdateStrategySpec defines how updates are rolled out
type UpdateStrategySpec struct {
	// Type is the update strategy type: RollingUpdate or Recreate
	// +kubebuilder:validation:Enum=RollingUpdate;Recreate
	// +kubebuilder:default=RollingUpdate
	// +optional
	Type string `json:"type,omitempty"`

	// MinReadySeconds is the minimum number of seconds for which a newly created pod should be ready
	// without any of its containers crashing before it is considered available.
	// For cluster mode with replicas, this should be at least 30 seconds to allow automatic failover to complete.
	// For sentinel mode, this should be at least 35 seconds to allow sentinel-managed failover.
	// For standalone mode, this can be 0.
	// If not specified, defaults are applied based on mode and replica count.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=300
	// +optional
	MinReadySeconds *int32 `json:"minReadySeconds,omitempty"`
}

// ServiceSpec defines Service configuration
type ServiceSpec struct {
	// Type is the Service type
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	// +optional
	Type corev1.ServiceType `json:"type,omitempty"`

	// Annotations are Service annotations
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// Labels are additional Service labels
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// PodTemplateSpec defines pod customizations
type PodTemplateSpec struct {
	// Annotations are pod annotations
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// Labels are additional pod labels
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// NodeSelector is the node selector
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations are pod tolerations
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Affinity is the pod affinity/anti-affinity rules
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// PriorityClassName is the priority class name
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// SecurityContext is the pod security context
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`

	// TopologySpreadConstraints for pod distribution
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
}

// SentinelSpec defines sentinel-specific settings
type SentinelSpec struct {
	// MasterName is the Sentinel master name for THIS instance, and it must be unique
	// among every Sentinel deployment reachable on the same pod network.
	//
	// It is not a cosmetic label. The master name is the ONLY isolation boundary
	// Sentinel's gossip protocol has: a Sentinel receiving a hello message looks the
	// name up and, if it does not know it, discards the message — and performs no
	// other check. There is no instance identifier, no namespace, and no
	// authentication between Sentinels beyond the optional password. Two instances
	// that share a master name and can reach each other are, protocol-wise, ONE
	// deployment: the one with the higher config epoch can reassign the other's
	// master to a foreign Redis pod, whose replicas then FLUSH their datasets to
	// resynchronise from it. Recommended value: "<namespace>.<name>".
	//
	// The historic value "mymaster" is accepted — a legacy client may hardcode it
	// with no way to parameterise it, and it is the current value of every instance
	// created before this field existed. Setting it deliberately is a choice the
	// operator does not second-guess; only LEAVING IT UNSET raises the
	// SentinelMasterNameUnscoped warning condition.
	//
	// Required. Instances created before this field existed keep running with
	// "mymaster" and must set it explicitly on their next change to spec.sentinel;
	// this is a client-visible change requiring Sentinel-aware clients to be
	// reconfigured (clients using the label-routed master Service are unaffected).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$`
	MasterName string `json:"masterName,omitempty"`

	// Quorum is the number of Sentinels needed to agree on failure
	// +kubebuilder:default=2
	// +optional
	Quorum int `json:"quorum,omitempty"`

	// DownAfterMilliseconds is the time to mark master as down
	// +kubebuilder:default=30000
	// +optional
	DownAfterMilliseconds int `json:"downAfterMilliseconds,omitempty"`

	// FailoverTimeout is the failover timeout
	// +kubebuilder:default=180000
	// +optional
	FailoverTimeout int `json:"failoverTimeout,omitempty"`

	// ParallelSyncs is the number of parallel replica syncs
	// +kubebuilder:default=1
	// +optional
	ParallelSyncs int `json:"parallelSyncs,omitempty"`

	// AllowUnsafeRebootstrapOnDeadlock permits the operator to break a leaderless
	// Sentinel bootstrap deadlock when TWO OR MORE surviving Redis pods hold data.
	// (A deadlock with no data, or with a single data-holding pod, is always broken
	// automatically and safely — the sole holder is promoted, discarding nothing —
	// regardless of this flag.) When two or more pods hold data, electing one as
	// master necessarily DISCARDS the data on the others, which full-resync from the
	// elected pod. With this flag set the operator force-elects the best-effort
	// most-complete pod (highest replication offset); with it unset it refuses and
	// waits for manual intervention. Only enable for instances where data loss is
	// acceptable (e.g. caches).
	// +kubebuilder:default=false
	// +optional
	AllowUnsafeRebootstrapOnDeadlock bool `json:"allowUnsafeRebootstrapOnDeadlock,omitempty"`

	// Resources defines CPU/memory for Sentinel container
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// FailoverSpec defines failover-specific settings. Mode failover is
// experimental: operator-managed HA without Sentinel, under active
// validation — see docs for current status and trade-offs vs sentinel mode.
type FailoverSpec struct {
	// Replicas is the number of Redis replicas; total data pods = 1 + replicas.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// DownAfterMilliseconds is the sustained-failure window before the operator
	// declares the master down on probe evidence and initiates a failover.
	// +kubebuilder:default=5000
	// +optional
	DownAfterMilliseconds int `json:"downAfterMilliseconds,omitempty"`

	// MinReplicasToWrite is rendered into redis.conf as min-replicas-to-write:
	// the master stops accepting writes when fewer than this many replicas are
	// connected (within min-replicas-max-lag, Redis default 10s). 0 disables the
	// check.
	//
	// Defaults to 1 (LR-038). With the default replicas: 2 this is the "master
	// plus one replica" durable pair — writes stop only when BOTH replicas are
	// gone, and it is what makes a master isolated from its replicas fence
	// ITSELF, locally, during a partition, with no operator involvement. That is
	// the one case operator-side fencing cannot reach.
	//
	// Cost at replicas >= 2, from 10 passes of the rapid-double-failover tier:
	// free at the median (18.5 refused writes against 16-19 with the check off),
	// with a ~20% tail where it costs ~45 more (~4.5s). Bimodal rather than noisy,
	// so it is an ordering condition, not a smear. No data loss either way in 60
	// measured runs.
	//
	// SET IT TO 0 IF YOU RUN replicas: 1. There the promoted master has ZERO
	// replicas until the old pod returns and resyncs, so the check blocks writes
	// for the whole recovery rather than just the handover, and any single replica
	// blip stops writes in steady state. That is a deliberate choice between
	// availability (0) and a bound on loss (1), and it is yours to make — which is
	// why it is not defaulted per-topology: a derived default would put the
	// effective value out of the reader's sight and out of the CRD (LR-033).
	// A POINTER on purpose. The default is non-zero, and the operator Updates the
	// whole object to add its finalizer — so with a bare int + omitempty an
	// explicit 0 would be dropped on serialization and the API server would
	// re-apply the default, silently turning a user's deliberate "off" back on.
	// That is LR-033's hazard (a reconciler Update persisting a value back into the
	// user's spec), and it would hit exactly the replicas: 1 users told above to
	// set 0. nil means unset; a pointer to 0 survives the round-trip.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	// +optional
	MinReplicasToWrite *int `json:"minReplicasToWrite,omitempty"`

	// AllowUnsafeRebootstrapOnDeadlock permits the operator to break a
	// no-master deadlock when the surviving data-holding Redis pods have
	// DIVERGED replication lineages. (A deadlock with no data, or with all
	// survivors on a single lineage — including a normal post-failover
	// promotion chain — is always broken automatically and safely: the
	// most-complete holder is promoted, discarding nothing, regardless of
	// this flag.) With diverged lineages, electing one master necessarily
	// DISCARDS the data on the other lineages, which full-resync from the
	// elected pod. With this flag set the operator force-elects the
	// best-effort most-complete pod (highest replication offset); with it
	// unset it refuses and waits for manual intervention. Only enable for
	// instances where data loss is acceptable (e.g. caches).
	// +kubebuilder:default=false
	// +optional
	AllowUnsafeRebootstrapOnDeadlock bool `json:"allowUnsafeRebootstrapOnDeadlock,omitempty"`
}

// ClusterSpec defines Redis Cluster settings
type ClusterSpec struct {
	// Shards is the number of master shards (minimum 3)
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:default=3
	// +optional
	Shards int `json:"shards,omitempty"`

	// ReplicasPerShard is the number of replicas per master (0 = no replicas)
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	// +optional
	ReplicasPerShard *int `json:"replicasPerShard,omitempty"`

	// ClusterNodeTimeout in milliseconds
	// +kubebuilder:default=15000
	// +optional
	ClusterNodeTimeout int `json:"clusterNodeTimeout,omitempty"`

	// FailoverGracePeriod is additional time (in seconds) beyond cluster-node-timeout
	// to wait for natural gossip-based failover before the operator force-promotes
	// a stuck orphaned replica. Default: 15 seconds.
	// Total timeout = clusterNodeTimeout + failoverGracePeriod.
	// +kubebuilder:default=15
	// +optional
	FailoverGracePeriod int `json:"failoverGracePeriod,omitempty"`

	// ReshardKeyBatchSize is the number of keys moved per MIGRATE call during a
	// key-preserving reshard on engines WITHOUT native atomic slot migration
	// (pre-Redis-8.4). Larger amortizes round-trips but blocks the source longer
	// per call. Advanced; the default suits most workloads. Ignored on Redis 8.4+
	// (native atomic slot migration is used instead). See LR-018.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=128
	// +optional
	ReshardKeyBatchSize int `json:"reshardKeyBatchSize,omitempty"`

	// ReshardMaxKeysPerReconcile bounds how many keys one reconcile migrates during
	// a pre-8.4 key-preserving reshard, so a large migration is spread across
	// reconciles rather than blocking the single reconcile worker. Advanced.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2000
	// +optional
	ReshardMaxKeysPerReconcile int `json:"reshardMaxKeysPerReconcile,omitempty"`

	// ReshardMigrateTimeoutMillis bounds a single MIGRATE call during a pre-8.4
	// key-preserving reshard (anti-hang, so a batch can never wedge the reconcile).
	// Advanced. See LR-018.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5000
	// +optional
	ReshardMigrateTimeoutMillis int `json:"reshardMigrateTimeoutMillis,omitempty"`
}

// PlacementSpec defines topology/failure-domain placement rules. Cluster mode only.
type PlacementSpec struct {
	// ShardAntiAffinity spreads each shard's pods (its master and replicas) across a
	// failure domain, so a single node/zone loss cannot take a whole shard. The operator
	// translates it into a per-shard topologySpreadConstraint scoped (by the shard identity
	// label) to that shard's pods, merged with any spec.podTemplate.topologySpreadConstraints.
	// See ADR-007.
	// +optional
	ShardAntiAffinity *ShardAntiAffinitySpec `json:"shardAntiAffinity,omitempty"`
}

// ShardAntiAffinitySpec configures per-shard failure-domain isolation for cluster mode.
type ShardAntiAffinitySpec struct {
	// TopologyKey is the node label defining the failure domain to spread a shard's pods
	// across (e.g. kubernetes.io/hostname for node-level, topology.kubernetes.io/zone for
	// zone-level). Defaults to kubernetes.io/hostname.
	// +optional
	TopologyKey string `json:"topologyKey,omitempty"`

	// WhenUnsatisfiable controls whether isolation is best-effort or enforced. ScheduleAnyway
	// (the default) spreads a shard's pods when possible but never blocks scheduling, so small
	// or single-node clusters still come up. DoNotSchedule enforces the spread, at the risk of
	// pods staying Pending when there are fewer domains than a shard has pods.
	// +kubebuilder:validation:Enum=DoNotSchedule;ScheduleAnyway
	// +optional
	WhenUnsatisfiable corev1.UnsatisfiableConstraintAction `json:"whenUnsatisfiable,omitempty"`
}

// LittleRedPhase represents the current phase of the LittleRed resource
type LittleRedPhase string

const (
	// PhasePending means the resource is waiting for resources
	PhasePending LittleRedPhase = "Pending"
	// PhaseInitializing means pods are starting
	PhaseInitializing LittleRedPhase = "Initializing"
	// PhaseRunning means all components are ready
	PhaseRunning LittleRedPhase = "Running"
	// PhaseFailed means validation error, pod crash, etc.
	PhaseFailed LittleRedPhase = "Failed"
	// PhaseTerminating means the resource is being deleted
	PhaseTerminating LittleRedPhase = "Terminating"
)

// Condition types
const (
	// ConditionReady indicates all components are ready
	ConditionReady = "Ready"
	// ConditionInitialized indicates initial setup is complete
	ConditionInitialized = "Initialized"
	// ConditionConfigValid indicates configuration is valid
	ConditionConfigValid = "ConfigValid"
	// ConditionTLSReady indicates TLS secrets are valid
	ConditionTLSReady = "TLSReady"
	// ConditionAuthReady indicates auth secrets are valid
	ConditionAuthReady = "AuthReady"
	// ConditionSentinelReady indicates sentinel quorum is established
	ConditionSentinelReady = "SentinelReady"
	// ConditionClusterReady indicates cluster is formed and healthy
	ConditionClusterReady = "ClusterReady"
	// ConditionLeaderlessRecovery reflects a leaderless Sentinel bootstrap deadlock
	// and the operator's response to it (sentinel mode). True means the instance is
	// deadlocked and needs attention (in cooldown, or refusing because data is
	// present); False records a completed recovery.
	ConditionLeaderlessRecovery = "LeaderlessRecovery"
	// ConditionGhostMasterRecovery reflects a ghost-master Sentinel failover deadlock —
	// a majority of Sentinels pinned to a dead (ghost) master IP with no promotable
	// replica, so failover aborts no-good-slave while living survivors hold the data —
	// and the operator's recovery of it (sentinel mode). True means deadlocked/needs
	// attention (in cooldown, or refusing because divergent data is present); False
	// records a completed recovery.
	ConditionGhostMasterRecovery = "GhostMasterRecovery"
	// ConditionForsaken marks a sentinel instance that has been CAPTURED by another
	// Sentinel deployment sharing its master name, and is therefore beyond the
	// operator's help: every reachable Sentinel monitors a live master that is not
	// one of this instance's pods, and no pod of ours is a master any more.
	//
	// Forsaken is terminal by design, not by omission. Recovery is declined (ADR-015
	// §9.2): the captured instance's data was flushed about a second after the
	// SLAVEOF, so there is nothing to salvage, and the operator structurally cannot
	// win the reclaim — `SENTINEL MONITOR` creates the entry at config_epoch 0 and
	// loses to the captor's epoch on the next hello. A human must run the runbook.
	//
	// This is NOT the controller-side collision check that ADR-015 rejected. That
	// one would have had to claim ISOLATION, and its silence would have been read as
	// an all-clear it could not give. This condition only ever reports a positive,
	// locally-observed fact — "our Sentinels serve someone else's master" — and says
	// nothing whatsoever when absent.
	ConditionForsaken = "Forsaken"

	// ConditionSentinelMasterNameUnscoped is a surfaced WARNING (never a refusal): the
	// instance has not set spec.sentinel.masterName and is therefore falling back to the
	// shared legacy Sentinel master name. The master name is the only isolation boundary
	// Sentinel's gossip protocol has, so any other unscoped Sentinel deployment reachable
	// on the same pod network can absorb this instance's topology — reassigning its master
	// to a foreign Redis pod, whose replicas then flush their datasets to resync from it.
	// The CRD requires the field, so this can only be an instance created before the field
	// existed; validation cannot reach it, which is why this condition exists.
	ConditionSentinelMasterNameUnscoped = "SentinelMasterNameUnscoped"

	// ConditionFailoverRecovery reflects a failover-mode no-master state and the
	// operator's response to it (failover mode only). True means the instance
	// needs attention — most importantly the refuse-and-wait state, where the
	// surviving data holders span divergent replication lineages and electing
	// any one would discard independent writes (set
	// failover.allowUnsafeRebootstrapOnDeadlock to authorize); False records a
	// completed recovery. A dedicated type (rather than reusing
	// LeaderlessRecovery, whose documented semantics are the sentinel-mode
	// bare-Sentinel deadlock) keeps the sentinel-vs-failover graduation-gate
	// comparison honest.
	ConditionFailoverRecovery = "FailoverRecovery"

	// ConditionClusterRolloutBlocked reports that a cluster shard's rolling update is
	// HELD because the shard is not redundant: a replaced pod is at the StatefulSet's
	// UpdateRevision and Ready per the kubelet, but has no attachment at all to its
	// shard's slot owner, and has been in that state past the operator's reattach
	// budget. While it holds, the shard's remaining pods — including its master — are
	// not taken down, so the instance keeps serving and its data is intact; the update
	// simply does not finish. Manual release is raising the StatefulSet's
	// spec.updateStrategy.rollingUpdate.partition by hand.
	//
	// A dedicated condition rather than Ready=False, deliberately (ADR-017): a stalled
	// rollout is a rollout that has not finished, not an unhealthy instance, and
	// conflating them trains an operator to ignore the one signal that matters. Ready
	// will nonetheless read false for the ordinary LR-014 reason (a not-yet-reattached
	// replacement is an empty master); this condition is what distinguishes
	// "converging" from "stuck".
	//
	// It is never set for a pod that is attached to the owner but whose replication
	// link is still down. That is a full sync in flight — dataset-dependent, genuinely
	// unbounded, and real progress — and reporting it would make the one alarm that
	// must not cry wolf cry wolf on exactly the topology large deployments have.
	ConditionClusterRolloutBlocked = "ClusterRolloutBlocked"
)

// LittleRedStatus defines the observed state of LittleRed
type LittleRedStatus struct {
	// Phase is the overall phase
	// +optional
	Phase LittleRedPhase `json:"phase,omitempty"`

	// Status is a human-readable summary of the current state
	// +optional
	Status string `json:"status,omitempty"`

	// BootstrapRequired indicates that the cluster needs initial master election.
	// This is set to true on creation and cleared after successful bootstrap.
	// +optional
	BootstrapRequired bool `json:"bootstrapRequired,omitempty"`

	// LeaderlessSince records when the operator first observed the instance in a
	// leaderless, all-sentinels-bare state (a bootstrap deadlock). It is cleared
	// as soon as a master is known again. The operator only attempts leaderless
	// recovery once this state has persisted past a cooldown, so a brief startup
	// blip does not trigger a rebootstrap.
	// +optional
	LeaderlessSince *metav1.Time `json:"leaderlessSince,omitempty"`

	// GhostMasterStuckSince records when the operator first observed the instance stuck
	// in a ghost-master failover deadlock: a majority of Sentinels pinned to a dead
	// (ghost) master IP with no promotable replica, so Sentinel aborts every failover
	// no-good-slave while living survivors still hold the data. Cleared once a master is
	// known again. Recovery only fires after this persists past a cooldown, so a recent
	// master death gets its full Sentinel election window first.
	// +optional
	GhostMasterStuckSince *metav1.Time `json:"ghostMasterStuckSince,omitempty"`

	// ForsakenSince records when the operator first observed this instance captured
	// by another Sentinel deployment (see ConditionForsaken). It exists only to hold
	// the verdict below a cooldown, so a transient mid-failover read cannot declare
	// an instance forsaken; it is a timer, like LeaderlessSince.
	// +optional
	ForsakenSince *metav1.Time `json:"forsakenSince,omitempty"`

	// QuarantinedSince records when the operator took this instance's pods away
	// because it is forsaken (see ConditionForsaken): a captured instance keeps
	// replicating from the captor's master, which poisons the CAPTOR's Sentinel
	// failover-candidate set with foreign pods. While this is set the desired Redis
	// and Sentinel replica count is 0, so the captor can heal through its own
	// existing ghost-replica pruning.
	//
	// It is a timer, like LeaderlessSince — but it is also the only thing that holds
	// the quarantine: with no pods there is no reachable monitoring Sentinel, so the
	// capture signature itself provably disappears. Cleared when the settling period
	// elapses and the pods are allowed back (they return empty, which the existing
	// no-data leaderless reseed handles).
	// +optional
	QuarantinedSince *metav1.Time `json:"quarantinedSince,omitempty"`

	// QuarantineAttempts counts how many times this instance has been quarantined as
	// forsaken. Attempt 2 therefore means it was captured again after its first
	// quarantine and reseed. The count is bounded: once it reaches the operator's
	// limit the instance stays at zero replicas instead of being released again,
	// because every recapture re-pollutes a healthy neighbour.
	//
	// It is a monitoring surface, and the clearest operational signal available —
	// "quarantined twice" says the instance's configuration is the problem (an
	// unscoped Sentinel master name, no authentication) far better than any condition
	// message. It is reset when the instance reaches a healthy Running state, never
	// merely because the capture signature stopped being observable.
	// +optional
	QuarantineAttempts int32 `json:"quarantineAttempts,omitempty"`

	// ObservedGeneration is the last observed generation
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the current state of the resource
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Redis contains Redis pod status
	// +optional
	Redis RedisStatus `json:"redis,omitempty"`

	// Master contains current master info (sentinel mode only)
	// +optional
	Master *MasterStatus `json:"master,omitempty"`

	// Replicas contains replica status (sentinel mode only)
	// +optional
	Replicas *ReplicaStatus `json:"replicas,omitempty"`

	// Sentinels contains sentinel status (sentinel mode only)
	// +optional
	Sentinels *SentinelStatus `json:"sentinels,omitempty"`

	// Cluster contains cluster state (cluster mode only)
	// +optional
	Cluster *ClusterStatusInfo `json:"cluster,omitempty"`

	// Failover contains failover-mode state (failover mode only)
	// +optional
	Failover *FailoverStatus `json:"failover,omitempty"`
}

// FailoverStatus contains failover-mode state. Both fields are monitoring
// surfaces only: every value is re-derived from live state on each reconcile,
// and nothing load-bearing is persisted here.
type FailoverStatus struct {
	// MasterDownSince records when the operator first observed the current
	// master as unreachable (the detection window / recovery cooldown marker).
	// Cleared as soon as the master is reachable again or a failover completes.
	// Monitoring surface only — re-derivable from live state.
	// +optional
	MasterDownSince *metav1.Time `json:"masterDownSince,omitempty"`

	// AssignmentEpoch mirrors the monotonic assignment epoch stamped on the
	// data pods' annotations. Monitoring surface only — the authoritative
	// epoch lives on the pods and is re-derived from live state, never read
	// back from status.
	// +optional
	AssignmentEpoch int64 `json:"assignmentEpoch,omitempty"`

	// TransitionSince records when the operator last stamped a new master
	// intent (an assignment-epoch bump: bootstrap seed, failover promotion, or
	// unsafe elect). It anchors the short post-transition cooldown that
	// serializes cascading failovers (ADR-011 §6). Monitoring surface only —
	// if lost, at worst one cooldown window is skipped; nothing load-bearing
	// is persisted.
	// +optional
	TransitionSince *metav1.Time `json:"transitionSince,omitempty"`
}

// RedisStatus contains Redis pod status
type RedisStatus struct {
	// Ready is the number of ready Redis pods
	Ready int32 `json:"ready"`
	// Total is the total number of Redis pods
	Total int32 `json:"total"`
}

// MasterStatus contains current master info
type MasterStatus struct {
	// PodName is the current master pod name
	PodName string `json:"podName,omitempty"`
	// IP is the current master pod IP
	IP string `json:"ip,omitempty"`
}

// ReplicaStatus contains replica status
type ReplicaStatus struct {
	// Ready is the number of ready replicas
	Ready int32 `json:"ready"`
	// Total is the total number of replicas
	Total int32 `json:"total"`
}

// SentinelStatus contains sentinel status
type SentinelStatus struct {
	// Ready is the number of ready sentinels
	Ready int32 `json:"ready"`
	// Total is the total number of sentinels
	Total int32 `json:"total"`
}

// ClusterStatusInfo contains cluster state (persisted instead of nodes.conf)
type ClusterStatusInfo struct {
	// State is the cluster state: ok, fail, initializing
	State string `json:"state,omitempty"`

	// Nodes contains per-node state for recovery
	Nodes []ClusterNodeState `json:"nodes,omitempty"`

	// LastBootstrap timestamp
	LastBootstrap *metav1.Time `json:"lastBootstrap,omitempty"`

	// OrphanedReplicas tracks replicas whose master is gone, for timeout-based force-promotion
	// +optional
	OrphanedReplicas []OrphanedReplicaInfo `json:"orphanedReplicas,omitempty"`

	// WipeDeadlockSince records when the operator first observed the total-/partial-wipe
	// deadlock signature: cluster pods stuck not-Ready and crash-looping (redis down, so —
	// pure in-memory — holding no data) while the instance cannot reach a healthy topology.
	// It arms the cooldown before the operator recycles the stuck pods (the cluster analog
	// of the sentinel leaderless recovery; see the reconciliation changelog). Cleared as
	// soon as the signature no longer holds.
	// +optional
	WipeDeadlockSince *metav1.Time `json:"wipeDeadlockSince,omitempty"`

	// Migration reports an in-progress in-place legacy→per-shard cluster migration
	// (ADR-013). Monitoring surface only; set while a legacy {name}-cluster StatefulSet
	// is being drained into the per-shard layout, and cleared once migration completes.
	// +optional
	Migration *ClusterMigrationStatus `json:"migration,omitempty"`
}

// ClusterMigrationStatus is a monitoring-only view of an in-progress in-place
// legacy→per-shard migration (ADR-013). Re-derived from live cluster state every
// reconcile; nothing here is load-bearing (ADR-006).
type ClusterMigrationStatus struct {
	// Phase is the current migration phase: Standup, Meet, Replicate, Failover,
	// Decommission, or Complete.
	// +optional
	Phase string `json:"phase,omitempty"`

	// ShardsMoved is the number of shards whose new master {name}-shard-K-0 already owns
	// range K (i.e. has failed over).
	// +optional
	ShardsMoved int `json:"shardsMoved,omitempty"`

	// TotalShards is the total number of shard slot ranges to migrate.
	// +optional
	TotalShards int `json:"totalShards,omitempty"`

	// StartedAt records when the operator first entered migration mode.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
}

// OrphanedReplicaInfo tracks an orphaned replica for timeout-based recovery
type OrphanedReplicaInfo struct {
	// PodName of the orphaned replica
	PodName string `json:"podName"`
	// NodeID of the orphaned replica
	NodeID string `json:"nodeId"`
	// MasterNodeID that this replica is orphaned from
	MasterNodeID string `json:"masterNodeId"`
	// DetectedAt is when this orphan was first detected
	DetectedAt metav1.Time `json:"detectedAt"`
}

// ClusterNodeState tracks a cluster node's identity (replaces nodes.conf)
type ClusterNodeState struct {
	// PodName is the stable pod name (e.g., my-cache-shard-0-0)
	PodName string `json:"podName"`

	// NodeID is the Redis cluster node ID (40-char hex)
	NodeID string `json:"nodeId"`

	// Role is master or replica
	Role string `json:"role"`

	// MasterNodeID for replicas - which master this replicates
	MasterNodeID string `json:"masterNodeId,omitempty"`

	// SlotRanges for masters (e.g., "0-5460")
	SlotRanges string `json:"slotRanges,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=lr
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`,description="Deployment mode"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`,description="Current phase"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.redis.ready`,description="Ready pods"
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.status`,description="High-level status summary"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LittleRed is the Schema for the littlereds API
type LittleRed struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LittleRedSpec   `json:"spec,omitempty"`
	Status LittleRedStatus `json:"status,omitempty"`
}

// Validate ensures the configuration is within currently supported bounds.
// Note: While the API is architected for flexibility, the initial release
// specifically supports and validates the configurations covered by our E2E suite.
func (r *LittleRed) Validate() error {
	// Sentinel mode: validated for 3 sentinels and 3 redis (1+2).
	// Since we don't expose sentinel count yet, we just check replicas.

	if r.Spec.Mode == ModeCluster {
		if r.Spec.Cluster != nil {
			if r.Spec.Cluster.Shards < 3 {
				return fmt.Errorf("cluster mode requires at least 3 shards (found %d)", r.Spec.Cluster.Shards)
			}
		}
	}

	// Failover mode: defense-in-depth mirror of the CRD minimums (like the
	// cluster shards check above). Placement (spec.placement.shardAntiAffinity)
	// stays cluster-only; the controller rejects it for every other mode,
	// failover included.
	if r.Spec.Mode == ModeFailover {
		if r.Spec.Failover != nil {
			if r.Spec.Failover.Replicas != nil && *r.Spec.Failover.Replicas < 1 {
				return fmt.Errorf("failover mode requires at least 1 replica (found %d)", *r.Spec.Failover.Replicas)
			}
			if r.Spec.Failover.MinReplicasToWrite != nil && *r.Spec.Failover.MinReplicasToWrite < 0 {
				return fmt.Errorf("failover minReplicasToWrite must be >= 0 (found %d)", *r.Spec.Failover.MinReplicasToWrite)
			}
		}
	}

	return nil
}

// +kubebuilder:object:root=true

// LittleRedList contains a list of LittleRed
type LittleRedList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LittleRed `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &LittleRed{}, &LittleRedList{})
		metav1.AddToGroupVersion(s, GroupVersion)
		return nil
	})
}

// Default resource values
var (
	DefaultCPURequest = resource.MustParse("128m")
	DefaultMemory     = resource.MustParse("512Mi")

	DefaultExporterCPURequest    = resource.MustParse("50m")
	DefaultExporterCPULimit      = resource.MustParse("100m")
	DefaultExporterMemoryRequest = resource.MustParse("32Mi")
	DefaultExporterMemoryLimit   = resource.MustParse("64Mi")

	DefaultSentinelCPU    = resource.MustParse("100m")
	DefaultSentinelMemory = resource.MustParse("64Mi")
)
