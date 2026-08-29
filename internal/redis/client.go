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

	// failoverStateNone is Sentinel's own name for "no failover in progress"
	// (sentinelFailoverStateStr, SENTINEL_FAILOVER_STATE_NONE).
	failoverStateNone = "none"
)

// newBoundedClient builds a go-redis Sentinel client for ONE single-shot command
// against ONE address — a control command (MONITOR/SET/RESET/REMOVE) or a probe
// (SENTINEL master / is-master-down-by-addr).
//
// Its timeouts are ProbeTimeout, not DefaultTimeout, and that is load-bearing
// rather than tidiness (LR-040). A context deadline alone does NOT bound these
// calls: go-redis reports `context deadline exceeded` at the deadline but still
// spends roughly another DefaultTimeout unwinding, so a 3s ctx over a 5s
// ReadTimeout still costs ~5s of wall clock. The dial and read budgets have to be
// short too. Both matter — the ctx bounds the pool's dial-retry loop (the
// blackholing-IP case that stalled a reconcile ~117s), the timeouts bound each
// individual attempt.
//
// A control command is one round-trip to one pod, so it has the same latency
// profile as a probe: a live in-cluster sentinel answers in well under a second.
func (c *SentinelClient) newBoundedClient(addr string) *redis.SentinelClient {
	return redis.NewSentinelClient(&redis.Options{
		Addr:         addr,
		Password:     c.password,
		DialTimeout:  ProbeTimeout,
		ReadTimeout:  ProbeTimeout,
		WriteTimeout: ProbeTimeout,
		TLSConfig:    makeTLSConfig(c.tlsEnabled),
	})
}

// newBoundedRedisClient is newBoundedClient's twin for the package-level
// single-shot helpers (Ping / SlaveOf / GetReplicationInfo), which talk to one
// Redis pod for one command. Same ProbeTimeout rationale (LR-040).
//
// Deliberately NOT used for cluster mode's per-node client: slot migration issues
// MIGRATE with its own multi-second budget (spec.cluster.reshardMigrateTimeoutMillis),
// so a blanket ProbeTimeout there would cut off legitimate long commands.
func newBoundedRedisClient(addr, password string, tlsEnabled bool) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DialTimeout:  ProbeTimeout,
		ReadTimeout:  ProbeTimeout,
		WriteTimeout: ProbeTimeout,
		TLSConfig:    makeTLSConfig(tlsEnabled),
	})
}

// MasterInfo contains information about the current master
type MasterInfo struct {
	IP    string
	Port  string
	Name  string
	Flags string
	// FailoverState is the `failover-state` field of the `SENTINEL master` reply.
	//
	// The name is source-confirmed and it is NOT "failover-status" — see
	// MonitoredMaster.FailoverState for the citations. Reading the wrong key here
	// is LR-052: the field parsed as a miss, so it was permanently "", so
	// ReplicationState.FailoverActive was permanently false and Rule A's second
	// half never fired.
	//
	// Emitted ONLY while the instance carries SRI_FAILOVER_IN_PROGRESS, so its
	// ordinary steady-state value is *absent*, not "none". Read it through
	// FailoverInProgress rather than comparing it here.
	FailoverState string
	// NumOtherSentinels / NumSlaves are the peer and replica counts this Sentinel
	// reports for the master. Free on the wire (the reply is already a map) and the
	// loudest available sign that another Sentinel deployment shares our master name.
	NumOtherSentinels int
	NumSlaves         int
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
		client := c.newBoundedClient(addr)
		result, err := client.Master(actx, name).Result()
		_ = client.Close()
		cancel()

		if err != nil {
			lastErr = err
			continue
		}

		return &MasterInfo{
			Name:              result["name"],
			IP:                result["ip"],
			Port:              result["port"],
			Flags:             result["flags"],
			FailoverState:     result["failover-state"],
			NumOtherSentinels: atoiOrZero(result["num-other-sentinels"]),
			NumSlaves:         atoiOrZero(result["num-slaves"]),
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
		client := c.newBoundedClient(addr)
		result, err := client.Master(actx, name).Result()
		_ = client.Close()
		cancel()

		if err != nil {
			continue
		}
		reachable = true
		// Both signals, one predicate (LR-052). The previous body compared
		// result["failover-status"] — a key neither project emits — against a
		// hand-written idle set, so it could only ever answer false.
		if failoverInProgress(result["flags"], result["failover-state"]) {
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
		client := c.newBoundedClient(addr)
		actx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		err := client.Process(actx, redis.NewStatusCmd(actx, "SENTINEL", "MONITOR", name, ip, port, quorum))
		cancel()
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
		client := c.newBoundedClient(addr)
		actx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		err := client.Process(actx, redis.NewStatusCmd(actx, "SENTINEL", "SET", name, option, value))
		cancel()
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
	client := c.newBoundedClient(addr)
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
	client := c.newBoundedClient(sentinelAddr)
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
		client := c.newBoundedClient(addr)
		// SENTINEL RESET masterName
		actx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		err := client.Process(actx, redis.NewIntCmd(actx, "SENTINEL", "RESET", masterName))
		cancel()
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
		client := c.newBoundedClient(addr)
		// SENTINEL REMOVE masterName
		actx, cancel := context.WithTimeout(ctx, ProbeTimeout)
		err := client.Process(actx, redis.NewStatusCmd(actx, "SENTINEL", "REMOVE", masterName))
		cancel()
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

// IsMonitoring checks if a specific sentinel address is monitoring the given master.
//
// Bounded twice, and the second half is why this deadline lives HERE rather than at
// the call site: newBoundedClient caps each individual socket operation at
// ProbeTimeout, but only a context deadline caps go-redis's dial-retry loop around
// them — against an address that swallows SYNs the pool dials five times before
// giving up (LR-040's ~117s field stall). Neither half is sufficient alone; LR-040
// measured the converse case, where a ctx over a DefaultTimeout client is inert
// (5.02s -> 5.00s).
//
// Putting the bound in the primitive rather than in each caller is LR-041's lesson
// applied to a duration instead of to a string: a guarantee that every caller must
// remember has no enforcement, and this method both reads as bounded (its client is)
// and is one line from being so. Today's only caller wraps it as well, which is
// belt and braces, not redundancy to remove.
func (c *SentinelClient) IsMonitoring(ctx context.Context, sentinelAddr, masterName string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()

	client := c.newBoundedClient(sentinelAddr)
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
	client := newBoundedRedisClient(addr, password, tlsEnabled)
	defer func() { _ = client.Close() }()

	return client.Ping(ctx).Err()
}

// SlaveOf reconfigures a redis instance to follow a new master.
//
// Bounded twice, in the primitive. newBoundedRedisClient caps each socket
// operation at ProbeTimeout (LR-040); the context deadline caps go-redis's
// dial-retry loop around them, which the client timeouts cannot reach — against an
// address that swallows SYNs the pool dials five times before giving up, and that
// is the shape behind this project's measured 146s (LR-017) and 117s (LR-040)
// reconcile stalls.
//
// The ctx half was missing here and it had a live caller. LR-040 bounded this
// function's client and recorded that doing so "fixed the same latent defect in
// failover mode's slaveOfBounded" — but only the client half travelled. Failover
// mode kept its own ProbeTimeout wrapper, while sentinel mode's Rule R passed the
// raw reconcile context, so a straggler repoint against a dial-blackholing stale
// pod IP cost ~5 x ProbeTimeout per pod, per pass. Rule 11 (cross-mode parity)
// applied to LR-040's own fix, one level down; see LR-049.
//
// The deadline lives here rather than at each call site for LR-041's reason,
// applied to a duration instead of to a string: a guarantee every caller must
// remember has no enforcement. failover_reconcile.go's slaveOfBounded wrapper is
// now belt and braces, not redundancy to remove.
func SlaveOf(ctx context.Context, addr, password, masterIP, masterPort string, tlsEnabled bool) error {
	ctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()

	client := newBoundedRedisClient(addr, password, tlsEnabled)
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
	// divergent. See ReplicationState.holdersDiverged.
	Replid2 string
}

// GetReplicationInfo gets replication + keyspace state from a redis instance.
// It fetches the default INFO (a superset of the replication section) so a single
// round trip yields role/offset, the replication id, and the total key count.
func GetReplicationInfo(ctx context.Context, addr, password string, tlsEnabled bool) (*ReplicationSnapshot, error) {
	client := newBoundedRedisClient(addr, password, tlsEnabled)
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

// atoiOrZero parses a Sentinel reply field, yielding 0 rather than an error: these are
// reporting values, where a malformed entry should degrade to "unknown" rather than
// fail the gather.
func atoiOrZero(s string) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0
	}
	return n
}

// MonitoredMaster is one entry of a Sentinel's `SENTINEL MASTERS` reply — i.e. one
// master name this particular Sentinel currently monitors.
//
// It exists because every other read path in this file asks a Sentinel about ONE
// name we already know. That can only ever confirm or deny the name we asked for;
// it can never reveal a name we did not ask about, which is precisely the state a
// half-finished master-name change leaves behind (a Sentinel carrying both the old
// and the new name, answering `Monitoring: true` for either).
type MonitoredMaster struct {
	// Name is the `name` field — the monitored master's name, i.e. the only
	// isolation boundary Sentinel's gossip protocol has (LR-039).
	Name string
	// IP is the `ip` field, the address this Sentinel believes that master is at.
	IP string
	// Flags is the `flags` field, e.g. "master", "s_down,master",
	// "master,failover_in_progress".
	Flags string
	// FailoverState is the `failover-state` field.
	//
	// The field name is source-confirmed, not guessed, and it is NOT
	// "failover-status": `addReplySentinelRedisInstance` emits the key
	// "failover-state" in redis/redis 8.0 (src/sentinel.c:3435) and in
	// valkey-io/valkey 8.1 (src/sentinel.c:3317) alike, with the values produced
	// by sentinelFailoverStateStr (sentinel.c:3366 / :3249 respectively):
	// "none", "wait_start", "select_slave", "send_slaveof_noone",
	// "wait_promotion", "reconf_slaves", "update_config", "unknown". The two
	// projects agree exactly, including the underscores.
	//
	// It is emitted ONLY while the instance carries SRI_FAILOVER_IN_PROGRESS, so
	// its ordinary steady-state value is *absent*, not "none". Read it through
	// FailoverInProgress rather than comparing it here.
	FailoverState string
}

// idleFailoverStates are the only values that count as "no failover in progress".
//
// The test is deliberately inverted — recognise idle, treat everything else as
// in-flight — so an unrecognised or future value fails safe (design §9 G3). The
// empty string is idle because Sentinel omits the field entirely unless a failover
// is running; "none" is listed for completeness (sentinelFailoverStateStr can
// return it, even though the emitting branch is unreachable while the state is
// NONE) and so that a caller synthesising a MonitoredMaster is not surprised.
var idleFailoverStates = map[string]bool{"": true, failoverStateNone: true}

// FailoverInProgress reports whether this Sentinel says a failover is running for
// this master.
//
// Two independent signals from the same reply, at no extra cost: the presence of a
// non-idle `failover-state`, and the `failover_in_progress` flag that gates that
// field's emission in the first place. Either alone is sufficient — a reply that
// somehow carried the flag without the field, or a version that emits the field
// unconditionally, is still read correctly.
func (m MonitoredMaster) FailoverInProgress() bool {
	return failoverInProgress(m.Flags, m.FailoverState)
}

// failoverInProgress is the ONE definition of "this Sentinel says a failover is
// running", shared by MonitoredMaster (SENTINEL MASTERS) and MasterInfo
// (SENTINEL master) — the same reply fields reached by two different commands.
//
// One definition on purpose, the IsLinkUpReplicaOf precedent: a second copy is
// literally how LR-045 happened, and LR-052 is the same class one level down —
// the `SENTINEL master` path re-derived the predicate against a key that does not
// exist while this one, written for Rule N, was correct all along.
func failoverInProgress(flags, state string) bool {
	if strings.Contains(flags, "failover_in_progress") {
		return true
	}
	return !idleFailoverStates[strings.TrimSpace(state)]
}

// FailoverInProgress reports whether the Sentinel that produced this MasterInfo
// says a failover is running for the master. Same two signals, same one predicate
// as MonitoredMaster's — see failoverInProgress.
func (m *MasterInfo) FailoverInProgress() bool {
	return m != nil && failoverInProgress(m.Flags, m.FailoverState)
}

// GetMonitoredMasters asks ONE Sentinel which masters it monitors.
//
// Single address on purpose — it is not the loop-over-c.addresses shape the other
// read paths use, because the whole value of this call is that different Sentinels
// can disagree about which names they carry, and a first-reachable-wins loop would
// destroy exactly that information.
//
// Bounded twice, and both halves are required (LR-040, re-confirmed by LR-046):
// newBoundedClient caps each individual attempt via Dial/Read/WriteTimeout, and the
// per-call context deadline caps go-redis's dial-retry loop. A context deadline
// ALONE is inert here — go-redis reports `context deadline exceeded` at the
// deadline and then spends roughly another DefaultTimeout unwinding. This call is
// issued during pod churn by design (before Rule A), which is the exact situation
// that stalled a reconcile ~117s in LR-040.
//
// A failure is returned as an error; the gatherers degrade it to an empty list
// rather than to Reachable:false — a Sentinel that cannot answer this one extra
// question is not a dead Sentinel (LR-041's class of mistake).
func (c *SentinelClient) GetMonitoredMasters(ctx context.Context, sentinelAddr string) ([]MonitoredMaster, error) {
	ctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()

	client := c.newBoundedClient(sentinelAddr)
	defer func() { _ = client.Close() }()

	raw, err := client.Masters(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list monitored masters: %w", err)
	}
	return parseMonitoredMasters(raw), nil
}

// parseMonitoredMasters turns go-redis's generic `SENTINEL MASTERS` reply into
// records, defensively: unknown and extra fields are ignored, an entry that is not
// a readable record is skipped rather than fatal, and nothing here can panic on a
// short or odd reply.
//
// Two wire shapes have to be handled and neither is hypothetical. Sentinel builds
// each record with addReplyDeferredLen + setDeferredMapLen, which is a true map on
// a RESP3 connection and a flat 2N array on RESP2. go-redis negotiates RESP3 by
// default and HELLO carries the SENTINEL command flag, so the map shape is what the
// operator sees today — but the parser must not silently depend on a protocol
// choice made in Options.
func parseMonitoredMasters(reply []any) []MonitoredMaster {
	var out []MonitoredMaster
	for _, entry := range reply {
		fields := recordFields(entry)
		if fields == nil {
			continue
		}
		// A record with no name is unusable: the name is the whole point of the
		// call, and Sentinel always emits it first.
		name, ok := fields["name"]
		if !ok || name == "" {
			continue
		}
		out = append(out, MonitoredMaster{
			Name:          name,
			IP:            fields["ip"],
			Flags:         fields["flags"],
			FailoverState: fields["failover-state"],
		})
	}
	return out
}

// recordFields flattens one reply element into string fields, or nil if it is not a
// record at all. Non-string keys and values are dropped rather than coerced —
// every field this parser reads is a bulk string in both projects, so a non-string
// there means the reply is not what we think it is, and guessing would be worse
// than ignoring.
func recordFields(entry any) map[string]string {
	switch e := entry.(type) {
	case map[any]any:
		fields := make(map[string]string, len(e))
		for k, v := range e {
			ks, kok := k.(string)
			vs, vok := v.(string)
			if kok && vok {
				fields[ks] = vs
			}
		}
		return fields
	case []any:
		fields := make(map[string]string, len(e)/2)
		for i := 0; i+1 < len(e); i += 2 {
			ks, kok := e[i].(string)
			vs, vok := e[i+1].(string)
			if kok && vok {
				fields[ks] = vs
			}
		}
		// A trailing key with no value (an odd-length array) is simply dropped by
		// the loop bound above, which is why it does not need its own branch.
		return fields
	default:
		return nil
	}
}
