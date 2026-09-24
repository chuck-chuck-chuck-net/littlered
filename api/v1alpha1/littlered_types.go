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
// At most one registered CR-resident heavy field may change per update, so a spec
// delta the operator could not order is refused at apply time (ADR-020). It is a
// TRANSITION rule and does not fire on create. The rationale, and the reason it is
// vacuously true while the registry has one CR-resident member, are on
// heavyOperations in internal/controller/operation_registry.go.
// +kubebuilder:validation:XValidation:rule="(has(self.sentinel) && has(oldSelf.sentinel) && (has(self.sentinel.masterName) ? self.sentinel.masterName : 'mymaster') != (has(oldSelf.sentinel.masterName) ? oldSelf.sentinel.masterName : 'mymaster') ? 1 : 0) <= 1",message="at most one heavy field may change per update (ADR-020): apply them one at a time"
// +kubebuilder:validation:XValidation:rule="self.mode == 'cluster' || !has(self.multiSite)",message="spec.multiSite may only be set when spec.mode is 'cluster'"
//
// Multi-site topology immutability (LR-032), the two clauses that need a
// spec-level view; the site list and sitesPerShard are pinned on MultiSiteSpec.
// Converting a running instance between single-site and stretched, or resharding
// a stretched instance, are both undesigned topology changes over EmptyDir data.
// +kubebuilder:validation:XValidation:rule="has(self.multiSite) == has(oldSelf.multiSite)",message="spec.multiSite cannot be added to or removed from an existing instance: converting between single-site and stretched topology is not supported; create a new instance instead"
// +kubebuilder:validation:XValidation:rule="!has(self.multiSite) || !has(self.cluster) || !has(oldSelf.cluster) || self.cluster.shards == oldSelf.cluster.shards",message="spec.cluster.shards is immutable while spec.multiSite is set: the multi-site topology is fixed for an instance's lifetime"
type LittleRedSpec struct {
	// Mode is the deployment mode: standalone, sentinel, cluster, or failover (experimental)
	// +kubebuilder:validation:Enum=standalone;sentinel;cluster;failover
	// +kubebuilder:default=standalone
	// +optional
	Mode string `json:"mode,omitempty"`

	// AppName is the value used for the app.kubernetes.io/name label on every resource
	// this instance owns. Set it to group the instance under your own application name
	// in monitoring and scrape configs.
	//
	// It is immutable: the label is part of the StatefulSet's spec.selector, which
	// Kubernetes does not allow to change after creation. Changing the name therefore
	// requires recreating the instance — and because storage is EmptyDir, that discards
	// the data.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?$`
	// +kubebuilder:default="littlered"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.appName is immutable: it is part of the StatefulSet selector, which Kubernetes does not allow to change"
	// +optional
	AppName string `json:"appName,omitempty"`

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

	// MultiSite stretches a Redis Cluster across multiple workload Kubernetes
	// clusters ("sites") managed by this (hub) operator. Cluster mode only.
	// Absent means single-site behavior, unchanged. See docs/MULTI_SITE_DESIGN.md.
	// +optional
	MultiSite *MultiSiteSpec `json:"multiSite,omitempty"`
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

	// TCPKeepalive interval in seconds. Zero DISABLES keepalive, which is a real Redis
	// semantic and a legitimate choice — hence a pointer: nil means "unset, take the
	// default", 0 means the user asked for 0. As a plain int with a non-zero default it
	// was indistinguishable from unset at every layer, so an explicit 0 was silently
	// overwritten with 300 (the LR-033 shape; MinReplicasToWrite is the precedent).
	// +kubebuilder:default=300
	// +optional
	TCPKeepalive *int `json:"tcpKeepalive,omitempty"`

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

	// Labels are additional pod labels, merged over any labels inherited from the
	// LittleRed resource's own metadata.
	//
	// The operator's structural labels cannot be set here: they make up the
	// StatefulSet and Service selectors, and a pod template whose labels disagree with
	// its StatefulSet's selector is rejected by the API server. Use spec.appName to
	// choose the app.kubernetes.io/name value.
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['app.kubernetes.io/name', 'app.kubernetes.io/instance', 'app.kubernetes.io/component', 'redis.chuck-chuck-chuck.net/shard', 'redis.chuck-chuck-chuck.net/role'])",message="spec.podTemplate.labels may not set the operator's structural labels (app.kubernetes.io/name, app.kubernetes.io/instance, app.kubernetes.io/component, redis.chuck-chuck-chuck.net/shard, redis.chuck-chuck-chuck.net/role); use spec.appName for the app name"
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

// MultiSiteSpec configures a stretched Redis Cluster across multiple sites
// (workload Kubernetes clusters), managed hub-and-spokes by one operator.
// Cluster mode only. See docs/MULTI_SITE_DESIGN.md and ADR-012.
// +kubebuilder:validation:XValidation:rule="self.sitesPerShard <= self.sites.size()",message="spec.multiSite.sitesPerShard cannot exceed the number of sites"
// +kubebuilder:validation:XValidation:rule="self.sites.filter(s, !has(s.kubeconfigSecretRef)).size() <= 1",message="at most one site may be hub-local (kubeconfigSecretRef omitted)"
//
// Site names must be unique. This was previously enforced for free by the sites
// list being a listType=map keyed on name; LR-033 made the list ATOMIC (its ORDER
// is load-bearing — it is the round-robin placement input — which is exactly what
// a map-typed list declares to be insignificant), so uniqueness is now an explicit
// rule. It is still load-bearing beyond tidiness: the site name is the operator's
// ground-truth key (siteQualifiedKey), so duplicates would collide observations.
// Quadratic, but MaxItems=16 keeps it far inside the CEL cost budget.
// +kubebuilder:validation:XValidation:rule="self.sites.all(s, self.sites.filter(x, x.name == s.name).size() == 1)",message="spec.multiSite.sites names must be unique"
//
// The multi-site TOPOLOGY is immutable for an instance's lifetime (LR-032): the
// round-robin placement (shard K -> sites (K+j) mod S) is index-based, so adding,
// removing or reordering a site remaps shards onto different sites — a relocation
// of live members, for which no mechanism exists (EmptyDir storage, pillar 3.1).
// Pinned here at admission rather than refused at runtime, because a runtime
// detector would have to distinguish "site added" from "site temporarily
// unobservable" and would fire during the very outage recoverSiteLoss must handle.
// Only the site NAMES and their ORDER are pinned; a site's kubeconfigSecretRef
// stays mutable so Secret rotation remains possible. sitesPerShard is a plain
// equality comparison because it is required and never defaulted (LR-033) — the
// stored spec always equals the effective value.
// +kubebuilder:validation:XValidation:rule="self.sites.map(s, s.name) == oldSelf.sites.map(s, s.name)",message="spec.multiSite.sites is immutable: the site list (names and order) is fixed for an instance's lifetime — adding, removing or reordering sites remaps shard placement onto different sites and is not supported; create a new instance instead"
// +kubebuilder:validation:XValidation:rule="self.sitesPerShard == oldSelf.sitesPerShard",message="spec.multiSite.sitesPerShard is immutable: changing it re-places every shard across sites and would orphan the per-site StatefulSets it moves away from"
type MultiSiteSpec struct {
	// Sites lists the workload Kubernetes clusters hosting Redis pods of this
	// instance. At least 2 sites are required (sitesPerShard >= 2 is what lets
	// the design descope single-site-loss data repair); site names must be
	// unique. At most one site may be hub-local (kubeconfigSecretRef omitted).
	//
	// ORDER IS SIGNIFICANT: a site's position in this list is its placement
	// index — shard K is hosted by sites (K+j) mod S for j < sitesPerShard — so
	// the list is an ordered sequence of objects, keyed by position, and is
	// declared listType=atomic accordingly (LR-033). It was previously
	// listType=map keyed on name, which tells the API machinery the opposite:
	// that entries are identified by name and order carries no meaning (CEL
	// equality on such a list is order-blind, and a server-side-apply merge is
	// free to reorder it — which would silently re-place every shard).
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	Sites []SiteSpec `json:"sites"`

	// SitesPerShard is how many sites host each shard. REQUIRED and immutable —
	// deliberately not defaulted (LR-033). This is the copy-factor knob of a
	// RAM-resident store: total members per shard = sitesPerShard x
	// (replicasPerShard + 1), so it sets the memory bill, the cross-site
	// replication traffic, and how many simultaneous site losses the data
	// survives. Every candidate default is wrong for somebody — the number of
	// sites multiplies copies as sites are added, a constant is arbitrary — and a
	// default would also hide the knob from the reader of a CR. Choose it once, at
	// creation. 2 keeps the design's contract (survive one site loss) at the
	// lowest cost and lets further sites scale capacity rather than copies; a
	// higher value buys more simultaneous site losses. Must be between 2 and the
	// number of sites.
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:validation:Required
	SitesPerShard int `json:"sitesPerShard"`

	// Network selects the cross-site replication transport.
	// +optional
	Network MultiSiteNetworkSpec `json:"network,omitempty"`

	// AllowUnsafeSiteTakeover permits the operator to take over a lost site's
	// shards (CLUSTER FAILOVER TAKEOVER on surviving replicas) even when the
	// fence argument is unavailable — the unreachable side could hold a serving
	// master majority, so a wrong takeover creates a live dual writer. With it
	// unset the operator refuses (drift-refusal), surfaces a condition and
	// waits for Kubernetes-confirmed site death or a human. See ADR-012.
	// +optional
	AllowUnsafeSiteTakeover bool `json:"allowUnsafeSiteTakeover,omitempty"`
}

// SiteSpec identifies one site (workload Kubernetes cluster) of a multi-site
// instance.
type SiteSpec struct {
	// Name is the site's unique name within this instance. It is the operator's
	// ground-truth key for everything observed in the site (siteQualifiedKey) and
	// is surfaced in status, so keep it a short identifier. The upper bound is not
	// cosmetic: CEL cost estimation assumes the declared maximum for every string
	// comparison, and the uniqueness rule compares names pairwise — without a
	// MaxLength the CRD is rejected for exceeding the rule-cost budget.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// KubeconfigSecretRef names a Secret in the operator namespace holding a
	// kubeconfig whose credentials are bound to a namespaced Role in the target
	// site (workload CRUD + pod read in the instance namespace — not
	// cluster-admin). Omitted means the site is hub-local: its workloads live
	// in the same Kubernetes cluster as the operator.
	// +optional
	KubeconfigSecretRef *KubeconfigSecretRef `json:"kubeconfigSecretRef,omitempty"`
}

// KubeconfigSecretRef names a Secret in the operator namespace that holds a
// site kubeconfig under the "kubeconfig" data key.
type KubeconfigSecretRef struct {
	// Name of the Secret.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// MultiSiteNetworkSpec selects the cross-site replication transport.
type MultiSiteNetworkSpec struct {
	// Mode is the transport for cross-site pod reachability. Only "pod-routed"
	// is supported: pod CIDRs are routed between all sites (and the hub), so
	// Redis nodes announce and reach plain pod IPs across sites. Future
	// transports (nodeport, multus) are not yet accepted.
	// +kubebuilder:validation:Enum=pod-routed
	// +kubebuilder:default=pod-routed
	// +optional
	Mode string `json:"mode,omitempty"`
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

	// ConditionStaleMasterName reports whether this instance's Sentinels monitor any
	// master name OTHER than the one spec.sentinel.masterName asks for (sentinel mode).
	//
	// A Sentinel persists every `sentinel monitor <name> …` it is given, and nothing in
	// the operator used to take one away: editing spec.sentinel.masterName registered the
	// new name and left the old one in place FOREVER, so one master was monitored under
	// two names by two independent failover state machines — the hazard LR-039 named,
	// reachable from a supported field edit on a healthy instance.
	//
	// Polarity: True is bad, matching Forsaken and LegacyClusterTopology. A converged
	// instance carries False/Converged, so the condition is quiet until something is
	// actually wrong. The reason says what the operator is doing about it: `Pruning`
	// (stale names observed and being removed, message names them and any Sentinel
	// skipped because it does not carry the desired name yet), `Deferred` (observed, but
	// a gate refuses — the message names WHICH gate, including the LR-050 rollout gate
	// that withholds attribution while our own Redis StatefulSet is not settled), and
	// `Foreign` (a stale entry points at an address that is neither one of our pods nor
	// flagged down, on a SETTLED instance — so something else is alive there and this
	// instance may be captured; do NOT rename to escape a capture, let the quarantine
	// complete first).
	ConditionStaleMasterName = "StaleMasterName"

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
	// ConditionMultiSiteTeardown reflects the multi-site deletion sweep (cluster
	// mode with spec.multiSite). Spoke-side children carry no owner references
	// (owner references do not work across Kubernetes clusters), so on CR
	// deletion the operator sweeps the instance-labeled workload objects
	// through every site before releasing the CR. True means the sweep is
	// blocked — a site was unreachable or its sweep failed — and the finalizer
	// is retained while the operator retries with bounded per-site attempts;
	// the documented escape hatch is manually removing the
	// redis.chuck-chuck-chuck.net/multisite-teardown finalizer.
	ConditionMultiSiteTeardown = "MultiSiteTeardown"
	// ConditionMultiSiteTakeover reflects the multi-site site-loss takeover (cluster mode
	// with spec.multiSite; ADR-012). It is the operator-owned recovery of a whole site's
	// loss that native Redis failover cannot resolve (survivors below the master
	// majority). True means the instance needs attention — the cooldown is armed, or the
	// operator is refusing because the fence is unavailable (the unreachable side could
	// hold a serving master majority, so an automatic takeover risks a live split-brain;
	// set multiSite.allowUnsafeSiteTakeover to authorise, or act manually). False records
	// a completed takeover or that no site-loss recovery is needed.
	ConditionMultiSiteTakeover = "MultiSiteTakeover"
	// ConditionReducedSiteLossResilience is a surfaced WARNING (not an admission refusal)
	// for a multi-site topology that cannot structurally auto-survive an arbitrary
	// single-site loss (cluster mode with spec.multiSite; ADR-012). It is True when the D2
	// balance invariant is structurally unsatisfiable — S sites x (ceil(K/2)-1) masters/site
	// < K shards, so pigeonhole forces some site over the balance target and losing that
	// heavier-master site strands the survivors below quorum (the headline case is K=4 over
	// 3 sites). The topology is still valid and functional — recovery of the heavier site's
	// loss simply requires manual action or multiSite.allowUnsafeSiteTakeover — so LittleRed
	// surfaces this rather than blocking it (the opt-in-not-block norm). Structural
	// (spec-level) only; transient master drift within a resilient topology is owned by the
	// rebalancer, not this condition. False records a resilient topology.
	ConditionReducedSiteLossResilience = "ReducedSiteLossResilience"
	// ConditionMultiSiteConfigInvalid is a surfaced WARNING for a site the operator cannot
	// CONFIGURE (cluster mode with spec.multiSite): its own namespace was unresolvable, or
	// the site's kubeconfig Secret is missing, unreadable or malformed. This is a different
	// state from an unreachable site, and the difference is whether a human must act: an
	// unreachable site may heal itself and is treated as normal (an unreachable site is
	// unmanaged, not broken), whereas the operator failing to read its own hub-local inputs
	// never heals on its own. Both previously surfaced only as "site unreachable", which
	// made the actionable alert impossible to write. True means at least one site is
	// unmanaged until the configuration is fixed — before anything is built the operator
	// refuses outright (phase Failed); on an existing instance the phase is left alone and
	// the healthy sites keep being reconciled. False records readable configuration.
	ConditionMultiSiteConfigInvalid = "MultiSiteConfigInvalid"

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

	// ConditionOperatorCannotAuthenticate reports that at least one of this
	// instance's own pods REFUSED the operator's credential — NOAUTH, WRONGPASS,
	// "Client sent AUTH, but no password is set", or "invalid password".
	//
	// This is the loudest thing the operator can say about a state that was
	// previously invisible (LR-051). Such a pod is not unreachable in any useful
	// sense: something answered on that address and spoke the protocol well enough
	// to turn us away. But every field the gather would have filled stays at its
	// zero value, so before this condition existed a credential mismatch was
	// byte-identical to a dial timeout — and because DataHolders() filters on
	// Reachable, a fleet holding its entire dataset read as holding nothing.
	//
	// Polarity: True is bad, matching Forsaken, StaleMasterName and
	// LegacyClusterTopology. The condition is absent while every pod answers, so it
	// is quiet until something is actually wrong. The message names the pods and
	// what each server said, because the reply IS the diagnosis: WRONGPASS means the
	// Secret was rotated under a running fleet (a Secret edit rolls nothing, so the
	// pods keep the old password indefinitely), while NOAUTH means the operator lost
	// a password the pods still enforce. See the auth design §3.5a.
	//
	// While it is True the operator REFUSES every action that would discard data —
	// Rule L's reseed, the ghost-master election, and the ADR-016 quarantine — since
	// a pod whose keyspace cannot be read cannot be shown to be empty. That refusal
	// is deliberately NOT overridable by
	// sentinel.allowUnsafeRebootstrapOnDeadlock: the remedy is to fix the
	// credential, which is trivial and destroys nothing.
	ConditionOperatorCannotAuthenticate = "OperatorCannotAuthenticate"

	// ConditionOperationInProgress reports that a declared HEAVY operation is being
	// carried out on this instance: a human edited one of the small, registered set of
	// spec fields whose change cannot safely proceed alongside regular healing, and the
	// operator has taken the early branch that suppresses that healing until the work
	// completes (ADR-020).
	//
	// Polarity: True means an operation is running — which is a normal, expected state,
	// not a fault. It is nonetheless reported loudly, because the instance is
	// deliberately not being healed while it holds, and an operation that never
	// completes is the failure mode this mechanism has to make visible. There is no
	// auto-exit timer (ADR-017's lesson: a timer is the defect with a delay), so a
	// stalled or blocked operation stays True until a human acts.
	//
	// It never affects Ready. An instance mid-rename is not an unhealthy instance, and
	// conflating the two trains an operator to ignore the signal that matters.
	ConditionOperationInProgress = "OperationInProgress"
)

// The declared-operation reason vocabulary (ADR-020). These are an API SURFACE: they are
// the values of status.operation.reason and of the OperationInProgress condition, they
// are enumerated in that field's own documentation, and TWO packages depend on the exact
// strings — the controller and cmd/lrctl, which does not import internal/controller.
//
// They live here so both can import them. Before, each kept its own copy and agreement
// rested on a test that parsed the controller's SOURCE — a workaround rather than a
// design, and one that a rename could only be caught by rather than prevented from.
//
// Converged and Seeded mean nothing is pending, so status.operation is absent for them;
// the other four each name a state in which a declared change exists and is either being
// carried out or deliberately held.
const (
	// OperationReasonConverged: every candidate's fingerprint equals its ack. There is
	// no unfinished work from any spec change.
	OperationReasonConverged = "Converged"
	// OperationReasonRunning: a heavy operation is being carried out this pass.
	OperationReasonRunning = "Running"
	// OperationReasonBlocked: work is pending but cannot proceed — the driver said so,
	// or a Requires edge holds it. Never auto-skipped (ADR-017).
	OperationReasonBlocked = "Blocked"
	// OperationReasonStalled: the running operation has outlived its StallAfter budget.
	// This changes what the operator SAYS, never what it does — there is no auto-exit.
	OperationReasonStalled = "Stalled"
	// OperationReasonQuarantined: a change is pending but the instance has no pods to
	// carry it out. Reported precisely so the hold is not silent (LR-054).
	OperationReasonQuarantined = "Quarantined"
	// OperationReasonSeeded: an ack was written without anything being run, because the
	// instance is already in the state its spec asks for.
	OperationReasonSeeded = "Seeded"
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

	// AcknowledgedOperations records, per registered heavy operation (ADR-020), the
	// value fingerprint the operator has FINISHED carrying out. A candidate whose
	// freshly computed fingerprint differs from its row here is unfinished work from a
	// spec change; equal means converged. A candidate with no row at all on an
	// already-initialized instance is SEEDED — the ack is written without anything
	// being run — which is what stops an operator upgrade declaring an operation for
	// every instance in a fleet.
	//
	// This is persisted state, and it is consistent with ADR-006 rather than an
	// exception to it. ADR-006's rule is "do not persist what is RECOMPUTABLE": the
	// cases it and ADR-011/013/017/018 declined — a capability probe, a phase, a
	// cursor, a remembered name — could all be re-derived from live state, so
	// persisting them created a second source of truth able to disagree with the
	// first. "This work finished at T" is an observation of an event at a point in
	// time and is by construction not recomputable from live state; the world cannot
	// be asked whether a change was ever asked for. That is the same ground on which
	// LeaderlessSince, GhostMasterStuckSince, ForsakenSince, QuarantinedSince and
	// QuarantineAttempts already sit. See ADR-020 Alternative A.
	//
	// The record is acknowledged on COMPLETION, never on observation: acknowledging
	// on sight loses the intent silently if the operator dies between the write and
	// the action, which is a false negative of exactly the kind the mechanism must
	// not have. It is never an operational input — it answers "was this asked for?"
	// and nothing else reads it — and the fingerprint is a keyed digest precisely so
	// that no later rule can extract the previous value from it (ADR-018).
	//
	// The list is bounded by the registry, not by time: one row per operation NAME,
	// updated in place. It needs no expiry, and an age-based one would be a defect
	// rather than hygiene — a missing row is re-seeded, never re-run.
	// +listType=map
	// +listMapKey=name
	// +optional
	AcknowledgedOperations []OperationAck `json:"acknowledgedOperations,omitempty"`

	// Operation is the heavy operation currently in progress and the queue behind it
	// (ADR-020). MONITORING SURFACE ONLY: every field is re-derived on every pass from
	// (spec, AcknowledgedOperations, live StatefulSets) and is never read back by any
	// decision. Losing it costs nothing — at worst a stall clock restarts.
	//
	// Heavy operations serialize, so at most one is ever running; Pending lists what
	// is queued behind it. There is no precedence field and none is coming: ordering
	// comes from the operation already running, simultaneous heavy spec edits are
	// refused at admission, and the one pair admission cannot see carries a declared
	// dependency on the registry entry (ADR-020 Alternatives E and F).
	// +optional
	Operation *OperationStatus `json:"operation,omitempty"`

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

	// MultiSite contains multi-site observed state (cluster mode with
	// spec.multiSite only)
	// +optional
	MultiSite *MultiSiteStatus `json:"multiSite,omitempty"`
}

// MultiSiteStatus contains multi-site observed state (cluster mode with
// spec.multiSite only). Monitoring surface: per-site observed state is
// re-derived from live probes; the takeover marker mirrors the
// leaderlessSince/wipeDeadlockSince cooldown-anchor pattern.
//
// The cross-site view fields (ClusterState/SlotsAssigned/CrossSiteOK/Shards)
// are the operator's published tier-1 surface: the operator gathers every site
// each reconcile and stamps a compact, site-attributed topology summary here, so
// a read-only-hub client (lrctl status / verify --from-status) can render the
// cross-site health with only `get littlered` — no site access. Kept small (a
// handful of shards) to stay within status size limits; freshness is the
// reconcile interval, anchored by LastReconcileTime.
type MultiSiteStatus struct {
	// Sites is the per-site observed state.
	// +listType=map
	// +listMapKey=name
	// +optional
	Sites []SiteStatus `json:"sites,omitempty"`

	// TakeoverPendingSince records when the operator first observed a site-loss
	// state that native Redis failover cannot resolve, arming the cooldown
	// before an operator-led CLUSTER FAILOVER TAKEOVER (ADR-012). Cleared when
	// the state clears.
	// +optional
	TakeoverPendingSince *metav1.Time `json:"takeoverPendingSince,omitempty"`

	// RebalancePendingSince records when the operator last issued a graceful
	// CLUSTER FAILOVER to correct master-distribution drift (ADR-012 D2). It is a
	// post-move settle/anti-thrash marker: while it is set and within the
	// rebalance cooldown, the drift-triggered rebalancer neither re-evaluates nor
	// re-issues a move for the same shard, so a graceful failover in flight is
	// never restarted. Cleared once the balance invariant holds again. Monitoring
	// surface only; nothing load-bearing is persisted (the decision is re-derived
	// from the live gather each pass).
	// +optional
	RebalancePendingSince *metav1.Time `json:"rebalancePendingSince,omitempty"`

	// ClusterState is the stretched cluster's overall cluster_state from the last
	// gather ("ok"/"fail"/"unknown") — the tier-1 published equivalent of the
	// live gather's ClusterState.
	// +optional
	ClusterState string `json:"clusterState,omitempty"`

	// SlotsAssigned is how many of the 16384 hash slots are assigned across the
	// stretched cluster (16384 when whole).
	// +optional
	SlotsAssigned int32 `json:"slotsAssigned,omitempty"`

	// CrossSiteOK is true when every shard's members span at least two sites (a
	// master and a replica in different sites) — the multi-site failure-domain
	// guarantee. False means at least one shard would be lost with a single site.
	// +optional
	CrossSiteOK bool `json:"crossSiteOK,omitempty"`

	// Shards is the per-shard site-attributed topology summary (which site holds
	// the master, which sites hold replicas, and whether the shard is cross-site).
	// +listType=map
	// +listMapKey=shard
	// +optional
	Shards []ShardSiteView `json:"shards,omitempty"`

	// MasterCounts is the current number of slot-serving masters hosted per site
	// (ADR-012 D2), from the last gather. Published so a tier-1 (read-only-hub)
	// reader can render the master-per-site distribution and the balance invariant's
	// live health (per-site count vs the ceil(K/2)-1 balance target) without any
	// site access. Monitoring surface only — the balancing decision is re-derived
	// from the live gather each pass.
	// +listType=map
	// +listMapKey=site
	// +optional
	MasterCounts []SiteMasterCount `json:"masterCounts,omitempty"`

	// LastReconcileTime is when the operator last published this cross-site view.
	// A tier-1 reader reports it as the data's freshness ("as of last reconcile
	// N ago").
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`
}

// ShardSiteView is one shard's site-attributed placement in the published
// cross-site view (tier 1). Derived from the union ground truth each reconcile.
type ShardSiteView struct {
	// Shard is the shard index.
	Shard int `json:"shard"`

	// MasterSite is the site hosting this shard's current master (empty if no
	// reachable master was observed).
	// +optional
	MasterSite string `json:"masterSite,omitempty"`

	// ReplicaSites are the sites hosting this shard's replicas, sorted.
	// +optional
	ReplicaSites []string `json:"replicaSites,omitempty"`

	// CrossSite is true when the shard's master and at least one replica live in
	// different sites.
	// +optional
	CrossSite bool `json:"crossSite,omitempty"`

	// LinkDown is true when any of this shard's replicas reported its replication
	// link down at the last gather (reduced redundancy).
	// +optional
	LinkDown bool `json:"linkDown,omitempty"`
}

// SiteMasterCount is the number of slot-serving masters currently hosted in one
// site (ADR-012 D2 master-per-site distribution). A site holding more than
// ceil(K/2)-1 (the balance target, K = shards) violates the balance invariant —
// surfaced as informational drift, corrected by the rebalancer.
type SiteMasterCount struct {
	// Site is the site name from spec.multiSite.sites.
	Site string `json:"site"`

	// Masters is the count of slot-serving masters observed in the site at the last
	// gather.
	Masters int32 `json:"masters"`
}

// SiteStatus is the observed state of one site.
type SiteStatus struct {
	// Name is the site name from spec.multiSite.sites.
	Name string `json:"name"`

	// Reachable reports whether the operator obtained this site's observed state at
	// the last probe. False means "did not answer, OR we could not ask": both a site
	// whose Kubernetes API did not respond and a site the operator could not
	// configure (unresolvable operator namespace, missing/unreadable/malformed
	// kubeconfig Secret) are false here. The two are NOT the same state — only the
	// first may heal itself — so read Reason to tell them apart. The boolean keeps
	// this deliberately broad meaning because it feeds the multi-site observability
	// arithmetic (allSitesObservable), where "we could not ask" must count exactly
	// like "did not answer".
	// +optional
	Reachable bool `json:"reachable,omitempty"`

	// Reason is a short, machine-readable classification of why this site is not
	// reachable; empty when it is. "Unreachable" means the site's Kubernetes API did
	// not answer — normal, possibly transient, and no action is implied (an
	// unreachable site is unmanaged, not broken). Any other value is a CONFIGURATION
	// fault that will never heal on its own and needs a human:
	// "OperatorNamespaceUnresolvable", "KubeconfigSecretUnreadable" or
	// "KubeconfigSecretMalformed". This is the field to alert on.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is the human-readable detail behind Reason (the underlying error),
	// empty when the site is reachable.
	// +optional
	Message string `json:"message,omitempty"`

	// Pods counts this instance's Redis pods observed in the site.
	// +optional
	Pods *RedisStatus `json:"pods,omitempty"`

	// LastProbeTime is when the operator last probed the site.
	// +optional
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`
}

// OperationAck is one row of the completion record: the value fingerprint of a
// registered heavy operation that the operator has finished carrying out, and when
// (ADR-020). One row per operation name, updated in place.
type OperationAck struct {
	// Name is the registry key of the heavy operation, e.g. "SentinelMasterNameRename".
	Name string `json:"name"`

	// Fingerprint identifies WHICH VALUE completed — an HMAC-SHA256 keyed on the
	// instance UID, hex-truncated (see OperationFingerprint). It is deliberately not
	// the value: the record must never leak it (the value is a password once auth
	// joins the registry) and must never be readable as "the previous value" by a
	// later rule, which is how ADR-018's refusal to remember a name is structurally
	// enforced. Naming an intent by its FIELD instead would match the wrong edit,
	// since the same field changes repeatedly.
	Fingerprint string `json:"fingerprint"`

	// AcknowledgedAt is when the operator observed this operation complete — or, for
	// a seeded row, when it first recorded that the instance was already in the state
	// its spec asks for.
	AcknowledgedAt metav1.Time `json:"acknowledgedAt"`
}

// OperationStatus is the monitoring view of the heavy operation in progress and the
// queue behind it (ADR-020). Nothing here is load-bearing; see
// LittleRedStatus.Operation.
type OperationStatus struct {
	// Name is the registry key of the running operation.
	Name string `json:"name"`

	// StartedAt is when the operator first ran this operation. It anchors the
	// operation's StallAfter budget, which raises a condition and an event and then
	// waits — there is deliberately no auto-exit timer (ADR-017).
	StartedAt metav1.Time `json:"startedAt"`

	// Reason is what the operation is doing: Running; Blocked (the driver reported it
	// cannot proceed — never auto-skipped); Stalled (past StallAfter); or Quarantined
	// (a heavy change is pending but held, because a quarantined instance has no pods
	// to carry it out).
	//
	// Quarantined is deliberately reported rather than left absent. A pending change
	// that is silently held would otherwise show up nowhere at all, and ADR-020's
	// standing requirement — taken from LR-054, where a withheld invariant was silent
	// by construction — is that anything this mechanism withholds must SAY that it
	// withheld. The planner's other reasons (Converged, Seeded) mean no operation is
	// pending, so this whole field is absent for them.
	Reason string `json:"reason"`

	// Pending lists the operations queued behind this one, in the order they will
	// run. Heavy operations serialize, so a wedged operation holds the queue
	// indefinitely and loudly rather than being skipped.
	// +optional
	Pending []string `json:"pending,omitempty"`
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

	return r.validateMultiSite()
}

// validateMultiSite validates spec.multiSite. Defense-in-depth mirror of the
// CRD structural rules (CEL, MinItems, listType=map, enum) plus the relational
// rules the schema cannot express against the dynamically defaulted
// sitesPerShard. Expects SetDefaults to have run.
func (r *LittleRed) validateMultiSite() error {
	ms := r.Spec.MultiSite
	if ms == nil {
		return nil
	}

	if r.Spec.Mode != ModeCluster {
		return fmt.Errorf("spec.multiSite is only supported in cluster mode (mode is %q)", r.Spec.Mode)
	}

	if len(ms.Sites) < 2 {
		return fmt.Errorf("spec.multiSite requires at least 2 sites (found %d); a single-site instance simply omits multiSite", len(ms.Sites))
	}

	seen := make(map[string]bool, len(ms.Sites))
	hubLocal := 0
	for _, s := range ms.Sites {
		if s.Name == "" {
			return fmt.Errorf("spec.multiSite.sites[].name must not be empty")
		}
		if seen[s.Name] {
			return fmt.Errorf("duplicate site name %q in spec.multiSite.sites", s.Name)
		}
		seen[s.Name] = true
		if s.KubeconfigSecretRef == nil {
			hubLocal++
		} else if s.KubeconfigSecretRef.Name == "" {
			return fmt.Errorf("site %q: kubeconfigSecretRef.name must not be empty (omit kubeconfigSecretRef entirely for a hub-local site)", s.Name)
		}
	}
	if hubLocal > 1 {
		return fmt.Errorf("at most one site may be hub-local (kubeconfigSecretRef omitted); found %d", hubLocal)
	}

	// Required, never defaulted (LR-033): 0 means "the user did not choose", which is
	// an error here, not a cue to pick for them. The CRD schema rejects it at
	// admission; this mirror covers objects that predate the requirement.
	if ms.SitesPerShard == 0 {
		return fmt.Errorf("spec.multiSite.sitesPerShard is required (between 2 and the number of sites, %d): it sets how many copies of each shard exist, so it is not defaulted", len(ms.Sites))
	}
	if ms.SitesPerShard < 2 || ms.SitesPerShard > len(ms.Sites) {
		return fmt.Errorf("spec.multiSite.sitesPerShard must be between 2 and the number of sites (%d), got %d", len(ms.Sites), ms.SitesPerShard)
	}

	switch ms.Network.Mode {
	case "", MultiSiteNetworkModePodRouted:
		return nil
	default:
		return fmt.Errorf("spec.multiSite.network.mode %q is not supported (only %q; nodeport and multus are future transports)", ms.Network.Mode, MultiSiteNetworkModePodRouted)
	}
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
