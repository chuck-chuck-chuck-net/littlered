package types

import (
	corev1 "k8s.io/api/core/v1"
)

// ClusterContext provides all metadata needed to interact with a Redis/Sentinel cluster,
// regardless of whether it is managed by the LittleRed operator.
type ClusterContext struct {
	Name      string
	Namespace string
	Mode      string // sentinel, cluster

	// SentinelMasterName is the instance's Sentinel master name. It is per-instance
	// (it is the only isolation boundary Sentinel's gossip has), so every SENTINEL
	// command lrctl issues must use this value and never a constant. Empty when the
	// CR could not be read (unmanaged discovery), in which case callers fall back to
	// v1alpha1.LegacySentinelMasterName.
	SentinelMasterName string

	// Pods grouped by their role/component
	RedisPods    []corev1.Pod
	SentinelPods []corev1.Pod // In sidecar mode, this might be the same as RedisPods

	// Container names to use for 'exec'
	RedisContainer    string
	SentinelContainer string
}

// GetRedisIPs returns a slice of all Redis pod IPs
func (c *ClusterContext) GetRedisIPs() []string {
	ips := make([]string, 0, len(c.RedisPods))
	for _, p := range c.RedisPods {
		if p.Status.PodIP != "" {
			ips = append(ips, p.Status.PodIP)
		}
	}
	return ips
}
