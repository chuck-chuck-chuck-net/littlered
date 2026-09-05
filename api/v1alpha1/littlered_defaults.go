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
	_ "embed"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Mode values for LittleRedSpec.Mode.
const (
	ModeCluster  = "cluster"
	ModeFailover = "failover"
)

// Default values
const (
	DefaultMode            = "standalone"
	DefaultRegistry        = "docker.io"
	DefaultImagePath       = "library/redis"
	DefaultImageTag        = "8.4.2"
	DefaultPullPolicy      = corev1.PullIfNotPresent
	DefaultMaxmemoryPolicy = "noeviction"
	DefaultTimeout         = 0
	DefaultTCPKeepalive    = 300
	DefaultServiceType     = corev1.ServiceTypeClusterIP
	DefaultUpdateStrategy  = "RollingUpdate"
	DefaultScrapeInterval  = "30s"
	DefaultScrapeTimeout   = "10s"
	DefaultSentinelQuorum  = 2
	DefaultDownAfterMs     = 30000
	DefaultFailoverTimeout = 180000
	DefaultParallelSyncs   = 1
	DefaultSecurityUserID  = int64(999)
	DefaultSecurityGroupID = int64(999)
	RedisPort              = 6379
	RedisExporterPort      = 9121
	SentinelPort           = 26379

	// SentinelRedisReplicas is the fixed number of Redis pods in sentinel mode
	// (1 master + 2 replicas). Sentinel HA is not horizontally scalable.
	SentinelRedisReplicas int32 = 3

	// Requeue defaults
	DefaultFastRequeueInterval        = 2 * time.Second
	DefaultSteadyStateRequeueInterval = 30 * time.Second

	// Cluster defaults
	DefaultClusterShards       = 3
	DefaultReplicasPerShard    = 1
	DefaultClusterNodeTimeout  = 15000
	DefaultFailoverGracePeriod = 15
	ClusterBusPortOffset       = 10000
	ClusterBusPort             = RedisPort + ClusterBusPortOffset // 16379

	// Failover-mode defaults (experimental; see ADR-011)
	DefaultFailoverDownAfterMs       = 5000
	DefaultFailoverReplicas    int32 = 2
	// DefaultFailoverMinReplicasToWrite is 1 (LR-038): with the default 2 replicas
	// this is the "master plus one replica" durable pair, and it is what lets an
	// isolated master fence ITSELF during a partition — the one case operator-side
	// fencing cannot reach. Cost at replicas >= 2 over 10 passes: free at the
	// median, ~45 extra refused writes in a ~20% tail. Set 0 explicitly at
	// replicas: 1.
	DefaultFailoverMinReplicasToWrite = 1

	// Placement defaults (cluster-mode shard anti-affinity)
	DefaultShardTopologyKey       = "kubernetes.io/hostname"
	DefaultShardWhenUnsatisfiable = corev1.ScheduleAnyway
)

// redis-exporter.Dockerfile is the single source of truth for the default
// redis_exporter sidecar image. Dependabot's docker ecosystem bumps the FROM
// line there; the values below are parsed from it at init time so a Dependabot
// PR is all that's needed to update the default version.
//
//go:embed redis-exporter.Dockerfile
var redisExporterDockerfile string

// DefaultExporterPath and DefaultExporterTag are parsed from
// redis-exporter.Dockerfile. Keep the kubebuilder default marker on
// ExporterSpec.Tag in sync (it must be a string literal); TestExporterDefaultsMatchDockerfile guards against drift.
var DefaultExporterPath, DefaultExporterTag = parseExporterImage(redisExporterDockerfile)

// parseExporterImage extracts the image path (without registry host) and tag
// from the first FROM line of the embedded Dockerfile. It panics on a malformed
// reference, surfacing the problem at startup/test time rather than shipping a
// broken default.
func parseExporterImage(dockerfile string) (path, tag string) {
	var ref string
	for line := range strings.SplitSeq(dockerfile, "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, "FROM "); ok {
			ref = strings.TrimSpace(rest)
			break
		}
	}

	// Split off the tag: the last ':' that comes after the last '/'.
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		tag = ref[i+1:]
		ref = ref[:i]
	}

	// Strip the registry host. The first path segment is a registry if it
	// contains a '.' or ':' (or is "localhost"); otherwise the registry is
	// implicit and handled separately via ExporterSpec.Registry inheritance.
	if i := strings.Index(ref, "/"); i >= 0 {
		if first := ref[:i]; strings.ContainsAny(first, ".:") || first == "localhost" {
			ref = ref[i+1:]
		}
	}
	path = ref

	if path == "" || tag == "" {
		panic(fmt.Sprintf("redis-exporter.Dockerfile: could not parse image path/tag from FROM line %q", ref))
	}
	return path, tag
}

// SetDefaults applies default values to the LittleRed spec
func (r *LittleRed) SetDefaults() {
	spec := &r.Spec

	// Mode
	if spec.Mode == "" {
		spec.Mode = DefaultMode
	}

	// Image
	spec.Image.SetDefaults()

	// Resources
	setDefaultResources(&spec.Resources)

	// Config
	spec.Config.SetDefaults()

	// Metrics — exporter follows the main container's QoS pattern for CPU limits.
	_, mainHasCPULimit := spec.Resources.Limits[corev1.ResourceCPU]
	spec.Metrics.SetDefaults(spec.Image.Registry, mainHasCPULimit)

	// Service
	if spec.Service.Type == "" {
		spec.Service.Type = DefaultServiceType
	}

	// UpdateStrategy
	if spec.UpdateStrategy.Type == "" {
		spec.UpdateStrategy.Type = DefaultUpdateStrategy
	}

	// PodTemplate security context
	if spec.PodTemplate.SecurityContext == nil {
		spec.PodTemplate.SecurityContext = defaultPodSecurityContext()
	}

	// Sentinel defaults (only if sentinel mode)
	if spec.Mode == "sentinel" && spec.Sentinel == nil {
		spec.Sentinel = &SentinelSpec{}
	}
	if spec.Sentinel != nil {
		spec.Sentinel.SetDefaults(mainHasCPULimit)
	}

	// Cluster defaults (only if cluster mode)
	if spec.Mode == ModeCluster && spec.Cluster == nil {
		spec.Cluster = &ClusterSpec{}
	}
	if spec.Cluster != nil {
		spec.Cluster.SetDefaults()
	}

	// Failover defaults (only if failover mode)
	if spec.Mode == ModeFailover && spec.Failover == nil {
		spec.Failover = &FailoverSpec{}
	}
	if spec.Failover != nil {
		spec.Failover.SetDefaults()
	}

	// Placement defaults (only when the block is present; not mode-gated / never auto-created)
	if spec.Placement != nil {
		spec.Placement.SetDefaults()
	}
}

// SetDefaults applies default values to PlacementSpec.
func (p *PlacementSpec) SetDefaults() {
	if p.ShardAntiAffinity == nil {
		return
	}
	if p.ShardAntiAffinity.TopologyKey == "" {
		p.ShardAntiAffinity.TopologyKey = DefaultShardTopologyKey
	}
	if p.ShardAntiAffinity.WhenUnsatisfiable == "" {
		p.ShardAntiAffinity.WhenUnsatisfiable = DefaultShardWhenUnsatisfiable
	}
}

// SetDefaults applies default values to ImageSpec
func (i *ImageSpec) SetDefaults() {
	if i.Registry == "" {
		i.Registry = DefaultRegistry
	}
	if i.Path == "" {
		i.Path = DefaultImagePath
	}
	if i.Tag == "" {
		i.Tag = DefaultImageTag
	}
	if i.PullPolicy == "" {
		i.PullPolicy = DefaultPullPolicy
	}
}

// SetDefaults applies default values to ConfigSpec
func (c *ConfigSpec) SetDefaults() {
	if c.MaxmemoryPolicy == "" {
		c.MaxmemoryPolicy = DefaultMaxmemoryPolicy
	}
	if c.TCPKeepalive == nil {
		v := DefaultTCPKeepalive
		c.TCPKeepalive = &v
	}
}

// TCPKeepaliveOrDefault is the effective tcp-keepalive, and it is the ONLY thing that
// should be rendered into redis.conf. It exists so the nil-vs-zero distinction lives in
// one place rather than at each of the four render sites (LR-041's shape: put the
// guarantee in the primitive, not in every caller).
func (c *ConfigSpec) TCPKeepaliveOrDefault() int {
	if c.TCPKeepalive == nil {
		return DefaultTCPKeepalive
	}
	return *c.TCPKeepalive
}

// SetDefaults applies default values to MetricsSpec
func (m *MetricsSpec) SetDefaults(mainRegistry string, mainHasCPULimit bool) {
	m.Exporter.SetDefaults(mainRegistry, mainHasCPULimit)

	if m.ServiceMonitor.Interval == "" {
		m.ServiceMonitor.Interval = DefaultScrapeInterval
	}
	if m.ServiceMonitor.ScrapeTimeout == "" {
		m.ServiceMonitor.ScrapeTimeout = DefaultScrapeTimeout
	}
}

// SetDefaults applies default values to ExporterSpec
func (e *ExporterSpec) SetDefaults(mainRegistry string, mainHasCPULimit bool) {
	if e.Registry == "" {
		e.Registry = mainRegistry
		if e.Registry == "" {
			e.Registry = DefaultRegistry
		}
	}
	if e.Path == "" {
		e.Path = DefaultExporterPath
	}
	if e.Tag == "" {
		e.Tag = DefaultExporterTag
	}
	setDefaultExporterResources(&e.Resources, mainHasCPULimit)
}

// SetDefaults applies default values to SentinelSpec
func (s *SentinelSpec) SetDefaults(mainHasCPULimit bool) {
	if s.Quorum == 0 {
		s.Quorum = DefaultSentinelQuorum
	}
	if s.DownAfterMilliseconds == 0 {
		s.DownAfterMilliseconds = DefaultDownAfterMs
	}
	if s.FailoverTimeout == 0 {
		s.FailoverTimeout = DefaultFailoverTimeout
	}
	if s.ParallelSyncs == 0 {
		s.ParallelSyncs = DefaultParallelSyncs
	}
	setDefaultSentinelResources(&s.Resources, mainHasCPULimit)
}

// SetDefaults applies default values to ClusterSpec
func (c *ClusterSpec) SetDefaults() {
	if c.Shards == 0 {
		c.Shards = DefaultClusterShards
	}
	if c.ReplicasPerShard == nil {
		c.ReplicasPerShard = new(DefaultReplicasPerShard)
	}
	if c.ClusterNodeTimeout == 0 {
		c.ClusterNodeTimeout = DefaultClusterNodeTimeout
	}
	if c.FailoverGracePeriod == 0 {
		c.FailoverGracePeriod = DefaultFailoverGracePeriod
	}
}

// SetDefaults applies default values to FailoverSpec
func (f *FailoverSpec) SetDefaults() {
	if f.Replicas == nil {
		f.Replicas = new(DefaultFailoverReplicas)
	}
	if f.DownAfterMilliseconds == 0 {
		f.DownAfterMilliseconds = DefaultFailoverDownAfterMs
	}
	// MinReplicasToWrite defaults to 1 (LR-038). Settable here only because the
	// field is a POINTER: with a bare int, "unset" and "explicitly 0" are the same
	// value, so defaulting would override a user's deliberate "off". nil is
	// unambiguous, so the Go-side and CRD-side defaults finally agree instead of
	// depending on which path created the object.
	if f.MinReplicasToWrite == nil {
		f.MinReplicasToWrite = new(DefaultFailoverMinReplicasToWrite)
	}
}

// GetTotalNodes returns the total number of cluster nodes (shards * (1 + replicas))
func (c *ClusterSpec) GetTotalNodes() int {
	replicas := 0
	if c.ReplicasPerShard != nil {
		replicas = *c.ReplicasPerShard
	}
	return c.Shards * (1 + replicas)
}

func setDefaultResources(r *corev1.ResourceRequirements) {
	if r.Requests == nil {
		r.Requests = corev1.ResourceList{}
	}
	if r.Limits == nil {
		r.Limits = corev1.ResourceList{}
	}

	if _, ok := r.Requests[corev1.ResourceCPU]; !ok {
		r.Requests[corev1.ResourceCPU] = DefaultCPURequest
	}
	if _, ok := r.Requests[corev1.ResourceMemory]; !ok {
		r.Requests[corev1.ResourceMemory] = DefaultMemory
	}
	// No default CPU limit — allow bursting.
	if _, ok := r.Limits[corev1.ResourceMemory]; !ok {
		r.Limits[corev1.ResourceMemory] = DefaultMemory
	}
}

func setDefaultExporterResources(r *corev1.ResourceRequirements, mainHasCPULimit bool) {
	if r.Requests == nil {
		r.Requests = corev1.ResourceList{}
	}
	if r.Limits == nil {
		r.Limits = corev1.ResourceList{}
	}

	if _, ok := r.Requests[corev1.ResourceCPU]; !ok {
		r.Requests[corev1.ResourceCPU] = DefaultExporterCPURequest
	}
	if _, ok := r.Requests[corev1.ResourceMemory]; !ok {
		r.Requests[corev1.ResourceMemory] = DefaultExporterMemoryRequest
	}
	// Only set a default CPU limit on the exporter if the main Redis container
	// has one. If the user chose Burstable QoS (no CPU limit), the sidecar should
	// follow the same pattern — otherwise tools like k9s report misleading CPU
	// utilization percentages for the pod.
	if _, ok := r.Limits[corev1.ResourceCPU]; !ok && mainHasCPULimit {
		r.Limits[corev1.ResourceCPU] = DefaultExporterCPULimit
	}
	if _, ok := r.Limits[corev1.ResourceMemory]; !ok {
		r.Limits[corev1.ResourceMemory] = DefaultExporterMemoryLimit
	}
}

func setDefaultSentinelResources(r *corev1.ResourceRequirements, mainHasCPULimit bool) {
	if r.Requests == nil {
		r.Requests = corev1.ResourceList{}
	}
	if r.Limits == nil {
		r.Limits = corev1.ResourceList{}
	}

	if _, ok := r.Requests[corev1.ResourceCPU]; !ok {
		r.Requests[corev1.ResourceCPU] = DefaultSentinelCPU
	}
	if _, ok := r.Requests[corev1.ResourceMemory]; !ok {
		r.Requests[corev1.ResourceMemory] = DefaultSentinelMemory
	}
	if _, ok := r.Limits[corev1.ResourceCPU]; !ok && mainHasCPULimit {
		r.Limits[corev1.ResourceCPU] = DefaultSentinelCPU
	}
	if _, ok := r.Limits[corev1.ResourceMemory]; !ok {
		r.Limits[corev1.ResourceMemory] = DefaultSentinelMemory
	}
}

func defaultPodSecurityContext() *corev1.PodSecurityContext {
	runAsNonRoot := true
	return &corev1.PodSecurityContext{
		RunAsNonRoot: &runAsNonRoot,
		RunAsUser:    new(DefaultSecurityUserID),
		RunAsGroup:   new(DefaultSecurityGroupID),
		FSGroup:      new(DefaultSecurityGroupID),
	}
}

// CalculateMaxmemory calculates maxmemory based on memory limit (90% of limit)
func (r *LittleRed) CalculateMaxmemory() string {
	if r.Spec.Config.Maxmemory != "" {
		// Try to parse as Kubernetes quantity (e.g., "200Mi", "1Gi")
		if qty, err := resource.ParseQuantity(r.Spec.Config.Maxmemory); err == nil {
			return fmt.Sprintf("%d", qty.Value())
		}
		// If not a valid quantity, return as-is (might be raw bytes)
		return r.Spec.Config.Maxmemory
	}

	memLimit := r.Spec.Resources.Limits[corev1.ResourceMemory]
	if memLimit.IsZero() {
		memLimit = DefaultMemory
	}

	// Calculate 90% of memory limit
	bytes := memLimit.Value()
	maxmemoryBytes := int64(float64(bytes) * 0.9)

	return fmt.Sprintf("%d", maxmemoryBytes)
}

// GetEffectiveMaxmemoryPolicy returns the maxmemory policy, defaulting to noeviction
func (r *LittleRed) GetEffectiveMaxmemoryPolicy() string {
	if r.Spec.Config.MaxmemoryPolicy != "" {
		return r.Spec.Config.MaxmemoryPolicy
	}
	return DefaultMaxmemoryPolicy
}

// GetPort returns the Redis port (with TLS awareness for future use)
func (r *LittleRed) GetPort() int32 {
	return RedisPort
}

// GetExporterPort returns the metrics exporter port
func (r *LittleRed) GetExporterPort() int32 {
	return RedisExporterPort
}

// GetRequeueIntervals returns the effective requeue intervals
func (r *LittleRed) GetRequeueIntervals() (fast, steady time.Duration) {
	fast = DefaultFastRequeueInterval
	steady = DefaultSteadyStateRequeueInterval

	if r.Spec.RequeueIntervals != nil {
		if r.Spec.RequeueIntervals.Fast != nil {
			fast = r.Spec.RequeueIntervals.Fast.Duration
		}
		if r.Spec.RequeueIntervals.SteadyState != nil {
			steady = r.Spec.RequeueIntervals.SteadyState.Duration
		}
	}
	return
}

// ParseMaxmemory parses the maxmemory string into bytes
func ParseMaxmemory(maxmemory string) (int64, error) {
	q, err := resource.ParseQuantity(maxmemory)
	if err != nil {
		return 0, err
	}
	return q.Value(), nil
}
