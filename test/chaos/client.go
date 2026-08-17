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

// Package chaos provides a test client for Redis resilience testing.
// It continuously writes and reads data while tracking success/failure rates
// and data integrity.
package chaos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Metrics tracks test client statistics using atomic counters
type Metrics struct {
	WriteAttempts    atomic.Int64
	WriteSuccesses   atomic.Int64
	WriteFailures    atomic.Int64
	ReadAttempts     atomic.Int64
	ReadSuccesses    atomic.Int64
	ReadFailures     atomic.Int64
	LostKeys         atomic.Int64
	DataCorruptions  atomic.Int64
	HighestConfirmed atomic.Int64
}

// MetricsSnapshot is a point-in-time snapshot of metrics
type MetricsSnapshot struct {
	WriteAttempts    int64 `json:"writeAttempts"`
	WriteSuccesses   int64 `json:"writeSuccesses"`
	WriteFailures    int64 `json:"writeFailures"`
	ReadAttempts     int64 `json:"readAttempts"`
	ReadSuccesses    int64 `json:"readSuccesses"`
	ReadFailures     int64 `json:"readFailures"`
	LostKeys         int64 `json:"lostKeys"`
	DataCorruptions  int64 `json:"dataCorruptions"`
	HighestConfirmed int64 `json:"highestConfirmed"`
}

// WriteAvailability returns the ratio of successful writes to attempted writes
func (m MetricsSnapshot) WriteAvailability() float64 {
	if m.WriteAttempts == 0 {
		return 1.0
	}
	return float64(m.WriteSuccesses) / float64(m.WriteAttempts)
}

// ReadAvailability returns the ratio of successful reads to attempted reads.
//
// A lost key counts as a non-success here, deliberately: the client asked for
// data it had been promised and did not get it. So this ratio alone cannot tell
// "the store was unreachable" from "the store answered, and the data was gone" —
// read it together with LostKeys, which separates the two.
func (m MetricsSnapshot) ReadAvailability() float64 {
	if m.ReadAttempts == 0 {
		return 1.0
	}
	return float64(m.ReadSuccesses) / float64(m.ReadAttempts)
}

// KeyLossRate returns the ratio of reads that found an already-ACKed key absent.
// This is a durability measure, deliberately kept out of ReadAvailability: an
// acknowledged-then-lost write and a read the server could not serve are
// different failures, and conflating them hides the worse one.
func (m MetricsSnapshot) KeyLossRate() float64 {
	if m.ReadAttempts == 0 {
		return 0.0
	}
	return float64(m.LostKeys) / float64(m.ReadAttempts)
}

// String returns a human-readable summary of the metrics
func (m MetricsSnapshot) String() string {
	return fmt.Sprintf(
		"Writes: %d attempted, %d succeeded, %d failed (%.2f%% availability)\n"+
			"Reads: %d attempted, %d succeeded, %d failed (%.2f%% availability)\n"+
			"Lost keys (ACKed write, key absent): %d (%.2f%% of reads)\n"+
			"Data corruptions: %d\n"+
			"Highest confirmed key: %d",
		m.WriteAttempts, m.WriteSuccesses, m.WriteFailures, m.WriteAvailability()*100,
		m.ReadAttempts, m.ReadSuccesses, m.ReadFailures, m.ReadAvailability()*100,
		m.LostKeys, m.KeyLossRate()*100,
		m.DataCorruptions,
		m.HighestConfirmed,
	)
}

// Config holds test client configuration
type Config struct {
	// Addrs is the list of Redis addresses (single for standalone, multiple for cluster/sentinel)
	Addrs []string

	// Password for Redis authentication (optional)
	Password string

	// ClusterMode enables Redis Cluster client
	ClusterMode bool

	// SentinelMaster name (if provided, enables Sentinel mode)
	SentinelMaster string

	// WriteRate is the interval between write attempts (default 100ms = 10/sec)
	WriteRate time.Duration

	// OperationTimeout is the timeout for each Redis operation (default 200ms)
	OperationTimeout time.Duration

	// KeyPrefix is an optional prefix for all keys (useful for namespacing)
	KeyPrefix string
}

// TestClient performs continuous read/write operations for resilience testing
type TestClient struct {
	client           redis.UniversalClient
	metrics          *Metrics
	confirmedKeys    sync.Map // map[int64]struct{} - keys we confirmed were written
	confirmedSlice   []int64  // slice of keys for efficient random selection
	sliceMu          sync.Mutex
	writeRate        time.Duration
	operationTimeout time.Duration
	keyPrefix        string
	stopCh           chan struct{}
	stopOnce         sync.Once
	wg               sync.WaitGroup
	counter          atomic.Int64
}

// expectedValue computes the expected value for a given key (sha256 hash)
func expectedValue(key int64) string {
	hash := sha256.Sum256(fmt.Appendf(nil, "%d", key))
	return hex.EncodeToString(hash[:])
}

// containsClusterStateOk checks if the CLUSTER INFO output contains "cluster_state:ok"
func containsClusterStateOk(info string) bool {
	return strings.Contains(info, "cluster_state:ok")
}

// containsClusterSlotsOk checks if all 16384 hash slots are mapped to healthy nodes.
// cluster_state:ok is insufficient: the cluster can report "ok" while slot assignments
// are still propagating after topology changes, causing MOVED/CLUSTERDOWN errors on writes.
func containsClusterSlotsOk(info string) bool {
	return strings.Contains(info, "cluster_slots_ok:16384")
}

// NewTestClient creates a new test client
func NewTestClient(cfg Config) (*TestClient, error) {
	var client redis.UniversalClient

	if cfg.ClusterMode {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        cfg.Addrs,
			Password:     cfg.Password,
			ReadTimeout:  cfg.OperationTimeout,
			WriteTimeout: cfg.OperationTimeout,
			DialTimeout:  cfg.OperationTimeout,
		})
	} else if cfg.SentinelMaster != "" {
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.SentinelMaster,
			SentinelAddrs: cfg.Addrs,
			Password:      cfg.Password,
			ReadTimeout:   cfg.OperationTimeout,
			WriteTimeout:  cfg.OperationTimeout,
			DialTimeout:   cfg.OperationTimeout,
		})
	} else {
		addr := "localhost:6379"
		if len(cfg.Addrs) > 0 {
			addr = cfg.Addrs[0]
		}
		client = redis.NewClient(&redis.Options{
			Addr:         addr,
			Password:     cfg.Password,
			ReadTimeout:  cfg.OperationTimeout,
			WriteTimeout: cfg.OperationTimeout,
			DialTimeout:  cfg.OperationTimeout,
		})
	}

	// Test connection with retries
	// Early in the cluster lifecycle, DNS or network paths might not be ready,
	// leading to "operation not permitted" or "no such host" errors.
	var lastErr error
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	fmt.Printf("Waiting for Redis to be reachable at %v...\n", cfg.Addrs)

ConnectLoop:
	for {
		select {
		case <-timeout:
			return nil, fmt.Errorf("timeout waiting for Redis connectivity: %w", lastErr)
		case <-ticker.C:
			// Create a context for the duration of this check (Ping + Cluster Info)
			checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			lastErr = client.Ping(checkCtx).Err()

			if lastErr == nil {
				// For cluster mode, PING is not enough. We must ensure the cluster is healthy.
				if cfg.ClusterMode {
					var info string
					info, lastErr = client.Do(checkCtx, "CLUSTER", "INFO").Text()
					if lastErr == nil {
						if !containsClusterStateOk(info) {
							lastErr = fmt.Errorf("cluster not ready (state not ok)")
						} else if !containsClusterSlotsOk(info) {
							lastErr = fmt.Errorf("cluster not ready (slots not fully ok)")
						}
					}
				}

				if lastErr == nil {
					cancel()
					fmt.Println("Successfully connected to Redis")
					break ConnectLoop
				}
			}
			cancel()
			fmt.Printf("Connection attempt failed: %v (retrying...)\n", lastErr)
		}
	}

	writeRate := cfg.WriteRate
	if writeRate == 0 {
		writeRate = 100 * time.Millisecond
	}

	opTimeout := cfg.OperationTimeout
	if opTimeout == 0 {
		opTimeout = 200 * time.Millisecond
	}

	return &TestClient{
		client:           client,
		metrics:          &Metrics{},
		writeRate:        writeRate,
		operationTimeout: opTimeout,
		keyPrefix:        cfg.KeyPrefix,
		stopCh:           make(chan struct{}),
	}, nil
}

// keyName returns the Redis key name for a given counter value
func (tc *TestClient) keyName(n int64) string {
	if tc.keyPrefix != "" {
		return fmt.Sprintf("%s:%d", tc.keyPrefix, n)
	}
	return fmt.Sprintf("%d", n)
}

// doWrite attempts to write a key-value pair to Redis
func (tc *TestClient) doWrite(n int64) {
	tc.metrics.WriteAttempts.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), tc.operationTimeout)
	defer cancel()

	key := tc.keyName(n)
	value := expectedValue(n)

	err := tc.client.Set(ctx, key, value, 0).Err()
	if err != nil {
		tc.metrics.WriteFailures.Add(1)
		return
	}

	tc.metrics.WriteSuccesses.Add(1)
	tc.confirmedKeys.Store(n, struct{}{})

	// Add to slice for efficient random selection in reads
	tc.sliceMu.Lock()
	tc.confirmedSlice = append(tc.confirmedSlice, n)
	tc.sliceMu.Unlock()

	// Update highest confirmed using CAS loop
	for {
		current := tc.metrics.HighestConfirmed.Load()
		if n <= current {
			break
		}
		if tc.metrics.HighestConfirmed.CompareAndSwap(current, n) {
			break
		}
	}
}

// readOutcome is the classification of a single read of an already-confirmed key.
type readOutcome int

const (
	// readOK — the key was present and held the expected value.
	readOK readOutcome = iota
	// readFailed — the read did not complete (transport error, timeout, server
	// refusing to serve). An availability event.
	readFailed
	// readLost — the read completed and the key was absent. Because every write
	// is issued without a TTL and the key was previously ACKed, absence means an
	// acknowledged write vanished. A durability event, not an availability one.
	readLost
	// readCorrupt — the key was present but held a value other than the expected
	// one. The strongest failure signal there is.
	readCorrupt
)

// classifyRead is the pure seam: it decides what a read result means, so the
// distinction between "could not read" and "the data is gone" is testable
// without a live Redis.
func classifyRead(result string, err error, expected string) readOutcome {
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return readLost
		}
		return readFailed
	}
	if result != expected {
		return readCorrupt
	}
	return readOK
}

// doRead attempts to read and verify a random confirmed key
func (tc *TestClient) doRead() {
	// Pick a random key from the confirmed slice
	tc.sliceMu.Lock()
	count := len(tc.confirmedSlice)
	if count == 0 {
		tc.sliceMu.Unlock()
		return // No confirmed writes yet
	}
	n := tc.confirmedSlice[rand.Intn(count)]
	tc.sliceMu.Unlock()

	tc.metrics.ReadAttempts.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), tc.operationTimeout)
	defer cancel()

	key := tc.keyName(n)
	result, err := tc.client.Get(ctx, key).Result()

	switch classifyRead(result, err, expectedValue(n)) {
	case readFailed:
		tc.metrics.ReadFailures.Add(1)
	case readLost:
		tc.metrics.LostKeys.Add(1)
	case readCorrupt:
		tc.metrics.DataCorruptions.Add(1)
	case readOK:
		tc.metrics.ReadSuccesses.Add(1)
	}
}

// Start begins the test client operations
func (tc *TestClient) Start() {
	tc.wg.Go(func() {

		ticker := time.NewTicker(tc.writeRate)
		defer ticker.Stop()

		for {
			select {
			case <-tc.stopCh:
				return
			case <-ticker.C:
				n := tc.counter.Add(1)

				// Track each operation in the WaitGroup
				tc.wg.Add(2)
				go func(val int64) {
					defer tc.wg.Done()
					tc.doWrite(val)
				}(n)

				go func() {
					defer tc.wg.Done()
					tc.doRead()
				}()
			}
		}
	})
}

// Stop stops the test client and waits for pending operations
func (tc *TestClient) Stop() {
	tc.stopOnce.Do(func() {
		close(tc.stopCh)
	})
	// Wait for the main loop AND all spawned operations to finish
	tc.wg.Wait()
}

// Close stops the client and closes the Redis connection
func (tc *TestClient) Close() error {
	tc.Stop()
	return tc.client.Close()
}

// GetMetrics returns a snapshot of current metrics
func (tc *TestClient) GetMetrics() MetricsSnapshot {
	return MetricsSnapshot{
		WriteAttempts:    tc.metrics.WriteAttempts.Load(),
		WriteSuccesses:   tc.metrics.WriteSuccesses.Load(),
		WriteFailures:    tc.metrics.WriteFailures.Load(),
		ReadAttempts:     tc.metrics.ReadAttempts.Load(),
		ReadSuccesses:    tc.metrics.ReadSuccesses.Load(),
		ReadFailures:     tc.metrics.ReadFailures.Load(),
		LostKeys:         tc.metrics.LostKeys.Load(),
		DataCorruptions:  tc.metrics.DataCorruptions.Load(),
		HighestConfirmed: tc.metrics.HighestConfirmed.Load(),
	}
}

// ConfirmedKeyCount returns the number of keys that were confirmed written
func (tc *TestClient) ConfirmedKeyCount() int64 {
	var count int64
	tc.confirmedKeys.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
