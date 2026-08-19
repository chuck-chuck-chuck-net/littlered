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

package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// makeTLSConfig returns a TLS config for operator-to-pod connections.
//
// We use InsecureSkipVerify deliberately. TLS here provides encryption in
// transit (confidentiality); it does not need to provide authentication.
//
// Authentication is already established at a higher level: the operator
// resolves pod IPs from the Kubernetes API, and only the actual pod can
// receive traffic on its assigned IP within the cluster network. Intercepting
// traffic at the pod-IP level requires compromising node networking or the CNI
// — at that point, TLS cert verification is the least of your concerns.
//
// Certificate verification would also fail in practice: TLS secrets are scoped
// to service hostnames, not pod IPs, so the cert's SANs never match the
// address we dial.
//
// For full PKI-based mutual authentication, use a service mesh (Istio,
// Linkerd). See docs/adr/004-tls-insecure-skip-verify.md.
func makeTLSConfig(enabled bool) *tls.Config {
	if !enabled {
		return nil
	}
	return &tls.Config{InsecureSkipVerify: true} //nolint:gosec // intentional, see ADR 004
}

const (
	// NOTE: there is deliberately NO package-level Sentinel master-name constant.
	// The master name is the only isolation boundary Sentinel's gossip protocol has,
	// so it must be per-instance (LittleRed.SentinelMasterName()) and is passed
	// explicitly to every method here. A shared constant is what allowed two
	// unrelated instances to merge into one quorum — see
	// docs/SENTINEL_CROSS_INSTANCE_CAPTURE_ANALYSIS.md.
	// DefaultTimeout for redis operations
	DefaultTimeout = 5 * time.Second
	// ProbeTimeout bounds a single ground-truth probe against one pod address — a
	// cluster probe (CLUSTER MYID/INFO/NODES), a sentinel probe (SENTINEL master/
	// replicas, GET-MASTER-ADDR), or a Redis INFO. Ground-truth gathering and status
	// resolution dial every expected pod IP, and during pod churn the K8s pod cache
	// can hand us a stale IP belonging to a deleted pod. Without a hard per-probe
	// deadline, go-redis spends ~25s (5 dial attempts × DefaultTimeout) on each dead
	// IP, serializing the whole reconcile loop behind it — and on a managed cloud a
	// killed pod's IP blackholes (i/o timeout) rather than RST-ing fast, so the stall
	// is real, not theoretical. A short deadline lets a dead IP fail fast while staying
	// far above the sub-second response time of a live in-cluster node. Originally
	// added for cluster mode (LR-012); sentinel mode lacked it and hit the identical
	// stall — see the cross-mode-parity rule in CLAUDE.md.
	ProbeTimeout = 3 * time.Second
)

// MasterInfo contains information about the current master
type MasterInfo struct {
	IP             string
	Port           string
	Name           string
	Flags          string
	FailoverStatus string
}

// SentinelClient wraps sentinel operations
type SentinelClient struct {
	addresses  []string
	password   string
	tlsEnabled bool
}

// NewSentinelClient creates a new sentinel client
func NewSentinelClient(addresses []string, password string, tlsEnabled bool) *SentinelClient {
	return &SentinelClient{
		addresses:  addresses,
		password:   password,
		tlsEnabled: tlsEnabled,
	}
}

// GetMaster queries sentinels to find the current master IP and Port
func (c *SentinelClient) GetMaster(ctx context.Context, masterName string) (*MasterInfo, error) {
	var lastErr error

	for _, addr := range c.addresses {
		actx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		master, err := c.getMasterFromSentinel(actx, addr, masterName)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		return master, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to get master from any sentinel: %w", lastErr)
	}
	return nil, fmt.Errorf("no sentinels available")
}

// GetMasterState returns the full state of the master as seen by the first reachable sentinel
func (c *SentinelClient) GetMasterState(ctx context.Context, name string) (*MasterInfo, error) {
	var lastErr error

	for _, addr := range c.addresses {
		actx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		client := redis.NewSentinelClient(&redis.Options{
			Addr:        addr,
			Password:    c.password,
			DialTimeout: DefaultTimeout,
			ReadTimeout: DefaultTimeout,
			TLSConfig:   makeTLSConfig(c.tlsEnabled),
		})
		result, err := client.Master(actx, name).Result()
		_ = client.Close()
		cancel()

		if err != nil {
			lastErr = err
			continue
		}

		return &MasterInfo{
			Name:           result["name"],
			IP:             result["ip"],
			Port:           result["port"],
			Flags:          result["flags"],
			FailoverStatus: result["failover-status"],
		}, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to get master state: %w", lastErr)
	}
	return nil, fmt.Errorf("no sentinels available")
}

// IsFailoverInProgress checks if any reachable sentinel reports an active failover for the master
func (c *SentinelClient) IsFailoverInProgress(ctx context.Context, name string) (bool, error) {
	reachable := false
	for _, addr := range c.addresses {
		actx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		client := redis.NewSentinelClient(&redis.Options{
			Addr:        addr,
			Password:    c.password,
			DialTimeout: DefaultTimeout,
			ReadTimeout: DefaultTimeout,
			TLSConfig:   makeTLSConfig(c.tlsEnabled),
		})
		result, err := client.Master(actx, name).Result()
		_ = client.Close()
		cancel()

		if err != nil {
			continue
		}
		reachable = true
		status := result["failover-status"]
		// Valid statuses when NO failover is happening are typically empty or "none" (depending on version)
		if status != "" && status != "none" && status != "no-failover" {
			return true, nil
		}
	}

	if !reachable {
		return false, fmt.Errorf("no sentinels reachable to check failover status")
	}
	return false, nil
}

// GetMasterAcrossAll queries all sentinels and returns a map of master IP -> count.
// This is used to detect split-brain or lack of consensus.
func (c *SentinelClient) GetMasterAcrossAll(ctx context.Context, masterName string) (map[string]int, error) {
	counts := make(map[string]int)
	reachable := 0

	for _, addr := range c.addresses {
		actx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		master, err := c.getMasterFromSentinel(actx, addr, masterName)
		cancel()
		if err != nil {
			continue
		}
		reachable++
		counts[master.IP]++
	}

	if reachable == 0 {
		return nil, fmt.Errorf("no sentinels reachable")
	}
	return counts, nil
}

// Subscribe connects to a sentinel and subscribes to the given channels.
// It returns a channel that receives messages and a close function.
// Note: This connects to the first available sentinel address.
func (c *SentinelClient) Subscribe(ctx context.Context, channels ...string) (<-chan *redis.Message, func(), error) {
	var client *redis.Client
	var lastErr error

	// Try to connect to any available sentinel
	for _, addr := range c.addresses {
		// Use a standard client for Pub/Sub connections to Sentinel
		rdb := redis.NewClient(&redis.Options{
			Addr:        addr,
			Password:    c.password,
			DialTimeout: DefaultTimeout,
			// No read timeout for Pub/Sub
			ReadTimeout: -1,
			TLSConfig:   makeTLSConfig(c.tlsEnabled),
		})

		if err := rdb.Ping(ctx).Err(); err == nil {
			client = rdb
			break
		} else {
			lastErr = err
			_ = rdb.Close()
		}
	}

	if client == nil {
		if lastErr != nil {
			return nil, nil, fmt.Errorf("failed to connect to any sentinel: %w", lastErr)
		}
		return nil, nil, fmt.Errorf("no sentinels available")
	}

	pubsub := client.Subscribe(ctx, channels...)

	// Verify subscription
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		_ = client.Close()
		return nil, nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	// Return the channel and a cleanup function
	return pubsub.Channel(), func() {
		_ = pubsub.Close()
		_ = client.Close()
	}, nil
}

// Monitor tells the sentinels to start monitoring a new master
func (c *SentinelClient) Monitor(ctx context.Context, name, ip string, port int, quorum int) error {
	var errors []string
	for _, addr := range c.addresses {
		client := redis.NewSentinelClient(&redis.Options{
			Addr:        addr,
			Password:    c.password,
			DialTimeout: DefaultTimeout,
			ReadTimeout: DefaultTimeout,
			TLSConfig:   makeTLSConfig(c.tlsEnabled),
		})
		err := client.Process(ctx, redis.NewStatusCmd(ctx, "SENTINEL", "MONITOR", name, ip, port, quorum))
		_ = client.Close()
		if err != nil {
			// If it's already monitored, that's fine
			if strings.Contains(err.Error(), "ERR Duplicate master name") {
				continue
			}
			errors = append(errors, fmt.Sprintf("%s: %v", addr, err))
		}
	}
	if len(errors) == len(c.addresses) && len(c.addresses) > 0 {
		return fmt.Errorf("failed to issue MONITOR command to all sentinels: %s", strings.Join(errors, "; "))
	}
	return nil
}

// Set updates sentinel configuration for a master
func (c *SentinelClient) Set(ctx context.Context, name, option, value string) error {
	var errors []string
	for _, addr := range c.addresses {
		client := redis.NewSentinelClient(&redis.Options{
			Addr:        addr,
			Password:    c.password,
			DialTimeout: DefaultTimeout,
			ReadTimeout: DefaultTimeout,
			TLSConfig:   makeTLSConfig(c.tlsEnabled),
		})
		err := client.Process(ctx, redis.NewStatusCmd(ctx, "SENTINEL", "SET", name, option, value))
		_ = client.Close()
		if err != nil {
			// If master not found on this node, we'll try again later
			if strings.Contains(err.Error(), "ERR No such master") {
				continue
			}
			errors = append(errors, fmt.Sprintf("%s: %v", addr, err))
		}
	}
	// We consider it a success if at least one sentinel was updated
	if len(errors) == len(c.addresses) && len(c.addresses) > 0 {
		return fmt.Errorf("failed to issue SET command to any sentinel: %s", strings.Join(errors, "; "))
	}
	return nil
}

func (c *SentinelClient) getMasterFromSentinel(ctx context.Context, addr, masterName string) (*MasterInfo, error) {
	client := redis.NewSentinelClient(&redis.Options{
		Addr:        addr,
		Password:    c.password,
		DialTimeout: DefaultTimeout,
		ReadTimeout: DefaultTimeout,
		TLSConfig:   makeTLSConfig(c.tlsEnabled),
	})
	defer func() { _ = client.Close() }()

	// SENTINEL GET-MASTER-ADDR-BY-NAME <masterName>
	result, err := client.GetMasterAddrByName(ctx, masterName).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get master addr: %w", err)
	}

	if len(result) != 2 {
		return nil, fmt.Errorf("unexpected result length: %d", len(result))
	}

	return &MasterInfo{
		IP:   result[0],
		Port: result[1],
	}, nil
}

// ReplicaInfo contains information about a sentinel-monitored replica
type ReplicaInfo struct {
	IP    string
	Port  string
	Flags string
}

// GetReplicas returns the list of replicas for a master as seen by any reachable sentinel
func (c *SentinelClient) GetReplicas(ctx context.Context, masterName string) ([]ReplicaInfo, error) {
	var lastErr error

	for _, addr := range c.addresses {
		actx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		replicas, err := c.getReplicasFromSentinel(actx, addr, masterName)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		return replicas, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to get replicas from any sentinel: %w", lastErr)
	}
	return nil, fmt.Errorf("no sentinels available")
}

func (c *SentinelClient) getReplicasFromSentinel(ctx context.Context, sentinelAddr, masterName string) ([]ReplicaInfo, error) {
	client := redis.NewSentinelClient(&redis.Options{
		Addr:        sentinelAddr,
		Password:    c.password,
		DialTimeout: DefaultTimeout,
		ReadTimeout: DefaultTimeout,
		TLSConfig:   makeTLSConfig(c.tlsEnabled),
	})
	defer func() { _ = client.Close() }()

	// SENTINEL REPLICAS mymaster
	result, err := client.Replicas(ctx, masterName).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get replicas: %w", err)
	}

	replicas := make([]ReplicaInfo, 0, len(result))
	for _, raw := range result {
		// go-redis returns []map[string]string for SENTINEL REPLICAS
		replica := ReplicaInfo{
			IP:    raw["ip"],
			Port:  raw["port"],
			Flags: raw["flags"],
		}
		replicas = append(replicas, replica)
	}

	return replicas, nil
}

// Reset clears state for a master in ALL sentinels (forcing re-discovery of replicas/sentinels)
func (c *SentinelClient) Reset(ctx context.Context, masterName string) error {
	var errors []string
	for _, addr := range c.addresses {
		client := redis.NewSentinelClient(&redis.Options{
			Addr:        addr,
			Password:    c.password,
			DialTimeout: DefaultTimeout,
			ReadTimeout: DefaultTimeout,
			TLSConfig:   makeTLSConfig(c.tlsEnabled),
		})
		// SENTINEL RESET masterName
		err := client.Process(ctx, redis.NewIntCmd(ctx, "SENTINEL", "RESET", masterName))
		_ = client.Close()
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", addr, err))
		}
	}
	if len(errors) == len(c.addresses) && len(c.addresses) > 0 {
		return fmt.Errorf("failed to issue RESET command to all sentinels: %s", strings.Join(errors, "; "))
	}
	return nil
}

// Remove tells the sentinels to stop monitoring a master
func (c *SentinelClient) Remove(ctx context.Context, masterName string) error {
	var errors []string
	for _, addr := range c.addresses {
		client := redis.NewSentinelClient(&redis.Options{
			Addr:        addr,
			Password:    c.password,
			DialTimeout: DefaultTimeout,
			ReadTimeout: DefaultTimeout,
			TLSConfig:   makeTLSConfig(c.tlsEnabled),
		})
		// SENTINEL REMOVE masterName
		err := client.Process(ctx, redis.NewStatusCmd(ctx, "SENTINEL", "REMOVE", masterName))
		_ = client.Close()
		if err != nil {
			// If it's already removed, that's fine
			if strings.Contains(err.Error(), "ERR No such master") {
				continue
			}
			errors = append(errors, fmt.Sprintf("%s: %v", addr, err))
		}
	}
	if len(errors) == len(c.addresses) && len(c.addresses) > 0 {
		return fmt.Errorf("failed to issue REMOVE command to all sentinels: %s", strings.Join(errors, "; "))
	}
	return nil
}

// IsMonitoring checks if a specific sentinel address is monitoring the given master
func (c *SentinelClient) IsMonitoring(ctx context.Context, sentinelAddr, masterName string) (bool, error) {
	client := redis.NewSentinelClient(&redis.Options{
		Addr:        sentinelAddr,
		Password:    c.password,
		DialTimeout: DefaultTimeout,
		ReadTimeout: DefaultTimeout,
		TLSConfig:   makeTLSConfig(c.tlsEnabled),
	})
	defer func() { _ = client.Close() }()

	_, err := client.GetMasterAddrByName(ctx, masterName).Result()
	if err != nil {
		if strings.Contains(err.Error(), "ERR No such master") || strings.Contains(err.Error(), "redis: nil") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Ping checks if a redis instance is reachable
func Ping(ctx context.Context, addr, password string, tlsEnabled bool) error {
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    password,
		DialTimeout: DefaultTimeout,
		ReadTimeout: DefaultTimeout,
		TLSConfig:   makeTLSConfig(tlsEnabled),
	})
	defer func() { _ = client.Close() }()

	return client.Ping(ctx).Err()
}

// SlaveOf reconfigures a redis instance to follow a new master
func SlaveOf(ctx context.Context, addr, password, masterIP, masterPort string, tlsEnabled bool) error {
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    password,
		DialTimeout: DefaultTimeout,
		ReadTimeout: DefaultTimeout,
		TLSConfig:   makeTLSConfig(tlsEnabled),
	})
	defer func() { _ = client.Close() }()

	if masterIP == "" {
		return client.Do(ctx, "REPLICAOF", "NO", "ONE").Err()
	}
	return client.Do(ctx, "REPLICAOF", masterIP, masterPort).Err()
}

// ReplicationSnapshot is the subset of a Redis node's INFO that the reconciler
// needs to reason about replication topology and data safety.
type ReplicationSnapshot struct {
	Role             string
	MasterHost       string
	MasterLinkStatus string
	Offset           int64
	// Keys is the total number of keys across all databases (INFO keyspace).
	// Zero means the node currently holds no data. This is the signal used to
	// decide whether a leaderless-recovery rebootstrap is data-safe — role does
	// not answer that question (a freshly restarted empty pod reports role:master).
	Keys int64
	// Replid is the master_replid — it identifies the current replication lineage.
	Replid string
	// Replid2 is the master_replid2 — the lineage this node descended from before its
	// last promotion/resync rotated the replid. Needed to recognise a promotion chain as
	// a single lineage; comparing Replid alone flags a normal post-failover survivor as
	// divergent. See SentinelClusterState.holdersDiverged.
	Replid2 string
}

// GetReplicationInfo gets replication + keyspace state from a redis instance.
// It fetches the default INFO (a superset of the replication section) so a single
// round trip yields role/offset, the replication id, and the total key count.
func GetReplicationInfo(ctx context.Context, addr, password string, tlsEnabled bool) (*ReplicationSnapshot, error) {
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    password,
		DialTimeout: DefaultTimeout,
		ReadTimeout: DefaultTimeout,
		TLSConfig:   makeTLSConfig(tlsEnabled),
	})
	defer func() { _ = client.Close() }()

	info, err := client.Info(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get info: %w", err)
	}

	snap := &ReplicationSnapshot{
		Role:             ParseInfoField(info, "role"),
		MasterHost:       ParseInfoField(info, "master_host"),
		MasterLinkStatus: ParseInfoField(info, "master_link_status"),
		Replid:           ParseInfoField(info, "master_replid"),
		Replid2:          ParseInfoField(info, "master_replid2"),
		Keys:             ParseKeyspaceKeys(info),
	}

	offsetField := "slave_repl_offset"
	if snap.Role == roleMaster {
		offsetField = "master_repl_offset"
	}
	if offsetStr := ParseInfoField(info, offsetField); offsetStr != "" {
		snap.Offset, _ = strconv.ParseInt(offsetStr, 10, 64)
	}

	return snap, nil
}

// ParseKeyspaceKeys sums the keys across all databases reported in the INFO
// keyspace section. Each database line has the form:
//
//	db0:keys=42,expires=0,avg_ttl=0
//
// It returns 0 when no keyspace lines are present (an empty node).
func ParseKeyspaceKeys(info string) int64 {
	var total int64
	for line := range strings.SplitSeq(info, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "db") {
			continue
		}
		_, after, found := strings.Cut(line, "keys=")
		if !found {
			continue
		}
		if before, _, ok := strings.Cut(after, ","); ok {
			after = before
		}
		if n, err := strconv.ParseInt(strings.TrimSpace(after), 10, 64); err == nil {
			total += n
		}
	}
	return total
}

// ParseInfoField extracts a field value from redis INFO output
func ParseInfoField(info, field string) string {
	// INFO output format: "field:value\r\n"
	prefix := field + ":"
	start := 0
	for i := 0; i < len(info); i++ {
		if i == 0 || info[i-1] == '\n' {
			if len(info[i:]) > len(prefix) && info[i:i+len(prefix)] == prefix {
				start = i + len(prefix)
				for j := start; j < len(info); j++ {
					if info[j] == '\r' || info[j] == '\n' {
						return info[start:j]
					}
				}
				return info[start:]
			}
		}
	}
	return ""
}
