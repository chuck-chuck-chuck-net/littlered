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
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ClusterNodeInfo contains parsed information about a cluster node
type ClusterNodeInfo struct {
	NodeID      string
	Addr        string
	Hostname    string
	Flags       []string
	MasterID    string // "-" if this is a master
	PingSent    int64
	PongRecv    int64
	ConfigEpoch int64
	LinkState   string // "connected" or "disconnected"
	Slots       []string
}

// IsMaster returns true if this node is a master
func (n *ClusterNodeInfo) IsMaster() bool {
	return slices.Contains(n.Flags, "master")
}

// IsReplica returns true if this node is a replica
func (n *ClusterNodeInfo) IsReplica() bool {
	return slices.Contains(n.Flags, "slave")
}

// ClusterInfo contains parsed CLUSTER INFO output
type ClusterInfo struct {
	State                    string
	SlotsAssigned            int
	SlotsOk                  int
	SlotsPfail               int
	SlotsFail                int
	KnownNodes               int
	Size                     int
	CurrentEpoch             int64
	MyEpoch                  int64
	StatsMessagesSent        int64
	StatsMessagesReceived    int64
	TotalLinksBufferLimitExc int64
	// AtomicSlotMigration is true when this node's CLUSTER INFO exposes the
	// cluster_slot_migration_* machinery — i.e. it supports Redis 8.4+ native
	// atomic slot migration (the CLUSTER MIGRATION command). Used as a free,
	// gather-time capability probe for LR-018; see reshard executor.
	AtomicSlotMigration bool
}

// ClusterClient wraps Redis cluster operations
type ClusterClient struct {
	password   string
	tlsEnabled bool
}

// NewClusterClient creates a new cluster client
func NewClusterClient(password string, tlsEnabled bool) *ClusterClient {
	return &ClusterClient{
		password:   password,
		tlsEnabled: tlsEnabled,
	}
}

// getClient creates a redis client with the LONG (DefaultTimeout) budget for the given
// address. Reserved for the slot-migration primitives that legitimately need it:
// MIGRATE carries its own per-call transfer budget (spec.cluster.reshardMigrateTimeoutMillis),
// and the pipelined SETSLOT / COUNTKEYSINSLOT calls issue up to one command per slot of a
// whole shard range (5461 at shards=3) in a single round trip. Bounding those at
// ProbeTimeout would abort a legitimate in-flight reshard — the hazard LR-040's exemption
// was written to avoid — and they are reachable only from the LR-018 reshard executor and
// the LR-025 migration driver, both re-entrant and resumable from the cluster's own markers.
//
// Everything else on this client is a single-round-trip control command and must use
// getBoundedClient. See LR-046.
func (c *ClusterClient) getClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    c.password,
		DialTimeout: DefaultTimeout,
		ReadTimeout: DefaultTimeout,
		TLSConfig:   makeTLSConfig(c.tlsEnabled),
	})
}

// getBoundedClient is the cluster-mode twin of (*SentinelClient).newBoundedClient: a
// client for ONE single-shot control command against ONE address, with all three of
// DialTimeout/ReadTimeout/WriteTimeout set to ProbeTimeout.
//
// Both halves of the bound are load-bearing and neither is sufficient alone (LR-040's
// measured finding, re-confirmed here for cluster mode). A context deadline alone is
// inert: go-redis reports `context deadline exceeded` at the deadline but still spends
// roughly another DefaultTimeout unwinding, so a 3s ctx over a 5s ReadTimeout still costs
// ~5s of wall clock. The client timeouts alone leave the pool's dial-retry loop unbounded
// — it breaks early only on ctx.Done() — which is the ~25s-per-call (5 attempts x
// DialTimeout) shape that starved a cluster reconcile for ~100s on a blackholing dead pod
// IP during a rolling update. So the ctx bounds the retry loop, the timeouts bound each
// individual attempt. See boundedCtx and LR-046.
func (c *ClusterClient) getBoundedClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     c.password,
		DialTimeout:  ProbeTimeout,
		ReadTimeout:  ProbeTimeout,
		WriteTimeout: ProbeTimeout,
		TLSConfig:    makeTLSConfig(c.tlsEnabled),
	})
}

// boundedCtx caps one single-shot control command at ProbeTimeout. It is applied inside
// the client rather than left to callers so the bound cannot be forgotten at a call site
// (LR-041's lesson): the cluster gather already wrapped its three probes (LR-012), but the
// repair-loop commands — CLUSTER MEET/FORGET/REPLICATE/ADDSLOTS/FAILOVER — had no deadline
// at all. Nesting inside a caller's own deadline is harmless: the shorter one wins.
func boundedCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, ProbeTimeout)
}

// longBudgetCtx caps one deliberately-long slot-migration call. The *per-attempt* budget
// stays DefaultTimeout (getClient) because the command legitimately needs it, but the
// pool's retry loop still has to be bounded: measured against a blackholing address a
// pipelined SETSLOT took 20.15s — four attempts x DefaultTimeout — because go-redis breaks
// out of its retry loop only on ctx.Done(). `budget` must therefore cover one attempt plus
// whatever transfer budget the caller asked for, and nothing more. See LR-046.
func longBudgetCtx(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, budget)
}

// GetMyID returns the cluster node ID for the node at the given address
func (c *ClusterClient) GetMyID(ctx context.Context, addr string) (string, error) {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(addr)
	defer func() { _ = client.Close() }()

	result, err := client.Do(ctx, "CLUSTER", "MYID").Result()
	if err != nil {
		return "", fmt.Errorf("failed to get node ID: %w", err)
	}

	nodeID, ok := result.(string)
	if !ok {
		return "", fmt.Errorf("unexpected result type for CLUSTER MYID")
	}

	return nodeID, nil
}

// GetClusterNodes returns parsed CLUSTER NODES output
func (c *ClusterClient) GetClusterNodes(ctx context.Context, addr string) ([]ClusterNodeInfo, error) {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(addr)
	defer func() { _ = client.Close() }()

	result, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster nodes: %w", err)
	}

	return ParseClusterNodes(result), nil
}

// GetClusterInfo returns parsed CLUSTER INFO output
func (c *ClusterClient) GetClusterInfo(ctx context.Context, addr string) (*ClusterInfo, error) {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(addr)
	defer func() { _ = client.Close() }()

	result, err := client.ClusterInfo(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info: %w", err)
	}

	return ParseClusterInfo(result), nil
}

// ClusterMeet introduces a new node to the cluster
func (c *ClusterClient) ClusterMeet(ctx context.Context, addr, newHost string, newPort int) error {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(addr)
	defer func() { _ = client.Close() }()

	err := client.ClusterMeet(ctx, newHost, strconv.Itoa(newPort)).Err()
	if err != nil {
		return fmt.Errorf("failed to meet node %s:%d: %w", newHost, newPort, err)
	}

	return nil
}

// ClusterForget removes a node from the cluster's known nodes
func (c *ClusterClient) ClusterForget(ctx context.Context, addr, nodeIDToForget string) error {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(addr)
	defer func() { _ = client.Close() }()

	err := client.ClusterForget(ctx, nodeIDToForget).Err()
	if err != nil {
		return fmt.Errorf("failed to forget node %s: %w", nodeIDToForget, err)
	}

	return nil
}

// ClusterAddSlots assigns slots to the node at the given address
func (c *ClusterClient) ClusterAddSlots(ctx context.Context, addr string, slots ...int) error {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(addr)
	defer func() { _ = client.Close() }()

	err := client.ClusterAddSlots(ctx, slots...).Err()
	if err != nil {
		return fmt.Errorf("failed to add slots: %w", err)
	}

	return nil
}

// ClusterReplicate makes the node at replicaAddr a replica of the given master
func (c *ClusterClient) ClusterReplicate(ctx context.Context, replicaAddr, masterNodeID string) error {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(replicaAddr)
	defer func() { _ = client.Close() }()

	err := client.ClusterReplicate(ctx, masterNodeID).Err()
	if err != nil {
		return fmt.Errorf("failed to replicate master %s: %w", masterNodeID, err)
	}

	return nil
}

// ClusterResetSoft performs a soft reset of the cluster node
func (c *ClusterClient) ClusterResetSoft(ctx context.Context, addr string) error {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(addr)
	defer func() { _ = client.Close() }()

	err := client.Do(ctx, "CLUSTER", "RESET", "SOFT").Err()
	if err != nil {
		return fmt.Errorf("failed to reset node: %w", err)
	}

	return nil
}

// ClusterFailover initiates a manual failover
func (c *ClusterClient) ClusterFailover(ctx context.Context, addr string) error {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(addr)
	defer func() { _ = client.Close() }()

	err := client.ClusterFailover(ctx).Err()
	if err != nil {
		return fmt.Errorf("failed to initiate failover: %w", err)
	}

	return nil
}

// ClusterFailoverTakeover initiates a manual failover with TAKEOVER option
// This is used when the master is not available or quorum is lost
func (c *ClusterClient) ClusterFailoverTakeover(ctx context.Context, addr string) error {
	ctx, cancel := boundedCtx(ctx)
	defer cancel()
	client := c.getBoundedClient(addr)
	defer func() { _ = client.Close() }()

	// go-redis ClusterFailover doesn't easily support arguments in all versions/wrappers,
	// so we use Do command directly to be safe and explicit.
	err := client.Do(ctx, "CLUSTER", "FAILOVER", "TAKEOVER").Err()
	if err != nil {
		return fmt.Errorf("failed to initiate failover takeover: %w", err)
	}

	return nil
}

// ParseClusterNodes parses the output of CLUSTER NODES command
// Format: <id> <ip:port@cport,hostname> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> ...
func ParseClusterNodes(output string) []ClusterNodeInfo {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	nodes := make([]ClusterNodeInfo, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 8 {
			continue
		}

		node := ClusterNodeInfo{
			NodeID:   parts[0],
			MasterID: parts[3],
		}

		// Parse address (ip:port@cport,hostname or ip:port@cport)
		addrParts := strings.Split(parts[1], ",")
		if len(addrParts) >= 2 {
			node.Hostname = addrParts[1]
		}
		// Get ip:port (strip @cport if present)
		ipPort := addrParts[0]
		if atIdx := strings.Index(ipPort, "@"); atIdx != -1 {
			ipPort = ipPort[:atIdx]
		}
		node.Addr = ipPort

		// Parse flags
		node.Flags = strings.Split(parts[2], ",")

		// Parse ping/pong times
		node.PingSent, _ = strconv.ParseInt(parts[4], 10, 64)
		node.PongRecv, _ = strconv.ParseInt(parts[5], 10, 64)

		// Parse config epoch
		node.ConfigEpoch, _ = strconv.ParseInt(parts[6], 10, 64)

		// Parse link state
		node.LinkState = parts[7]

		// Parse slots (remaining parts). Skip per-slot migrating "[slot->-id]" and
		// importing "[slot-<-id]" notations: those are NOT owned slots, and including
		// them breaks ParseSlotRange and makes an importing-but-slotless node look like
		// a slot-owning master (LR-018 — the reshard dance is the first path that puts
		// slots into migrating/importing state, exposing this).
		for _, s := range parts[8:] {
			if strings.HasPrefix(s, "[") {
				continue
			}
			node.Slots = append(node.Slots, s)
		}

		nodes = append(nodes, node)
	}

	return nodes
}

// ParseClusterInfo parses the output of CLUSTER INFO command
func ParseClusterInfo(output string) *ClusterInfo {
	info := &ClusterInfo{}
	lines := strings.SplitSeq(strings.TrimSpace(output), "\n")

	for line := range lines {
		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		value := strings.TrimSpace(parts[1])

		// Presence of any cluster_slot_migration_* field signals Redis 8.4+ native
		// atomic slot migration support (LR-018 §7.3, gather-time capability probe).
		if strings.HasPrefix(key, "cluster_slot_migration") {
			info.AtomicSlotMigration = true
		}

		switch key {
		case "cluster_state":
			info.State = value
		case "cluster_slots_assigned":
			info.SlotsAssigned, _ = strconv.Atoi(value)
		case "cluster_slots_ok":
			info.SlotsOk, _ = strconv.Atoi(value)
		case "cluster_slots_pfail":
			info.SlotsPfail, _ = strconv.Atoi(value)
		case "cluster_slots_fail":
			info.SlotsFail, _ = strconv.Atoi(value)
		case "cluster_known_nodes":
			info.KnownNodes, _ = strconv.Atoi(value)
		case "cluster_size":
			info.Size, _ = strconv.Atoi(value)
		case "cluster_current_epoch":
			info.CurrentEpoch, _ = strconv.ParseInt(value, 10, 64)
		case "cluster_my_epoch":
			info.MyEpoch, _ = strconv.ParseInt(value, 10, 64)
		case "cluster_stats_messages_sent":
			info.StatsMessagesSent, _ = strconv.ParseInt(value, 10, 64)
		case "cluster_stats_messages_received":
			info.StatsMessagesReceived, _ = strconv.ParseInt(value, 10, 64)
		case "total_cluster_links_buffer_limit_exceeded":
			info.TotalLinksBufferLimitExc, _ = strconv.ParseInt(value, 10, 64)
		}
	}

	return info
}

// GenerateSlotRanges generates slot ranges for the given number of shards
// Redis Cluster has 16384 slots (0-16383)
func GenerateSlotRanges(shards int) []struct {
	Start int
	End   int
} {
	if shards <= 0 {
		return nil
	}

	const totalSlots = 16384
	slotsPerShard := totalSlots / shards
	remainder := totalSlots % shards

	ranges := make([]struct {
		Start int
		End   int
	}, shards)

	start := 0
	for i := range shards {
		count := slotsPerShard
		if i < remainder {
			count++ // Distribute remainder slots
		}
		ranges[i].Start = start
		ranges[i].End = start + count - 1
		start += count
	}

	return ranges
}

// FormatSlotRange formats a slot range as a string (e.g., "0-5460")
func FormatSlotRange(start, end int) string {
	if start == end {
		return strconv.Itoa(start)
	}
	return fmt.Sprintf("%d-%d", start, end)
}

// ParseSlotRange parses a slot range string (e.g., "0-5460") into start and end
func ParseSlotRange(s string) (start, end int, err error) {
	parts := strings.Split(s, "-")
	if len(parts) == 1 {
		start, err = strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, err
		}
		return start, start, nil
	}
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid slot range: %s", s)
	}

	start, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	end, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}

	return start, end, nil
}

// ExpandSlotRange expands a slot range string into individual slot numbers
func ExpandSlotRange(s string) ([]int, error) {
	start, end, err := ParseSlotRange(s)
	if err != nil {
		return nil, err
	}

	slots := make([]int, end-start+1)
	for i := start; i <= end; i++ {
		slots[i-start] = i
	}
	return slots, nil
}
