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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/littlered-operator/littlered-operator/test/chaos"
)

func main() {
	var (
		addrs            string
		password         string
		clusterMode      bool
		sentinelMaster   string
		writeRate        time.Duration
		operationTimeout time.Duration
		keyPrefix        string
		duration         time.Duration
		statusInterval   time.Duration
	)

	flag.StringVar(&addrs, "addrs", "localhost:6379", "Comma-separated Redis addresses")
	flag.StringVar(&password, "password", "", "Redis password")
	flag.BoolVar(&clusterMode, "cluster", false, "Enable Redis Cluster mode")
	flag.StringVar(&sentinelMaster, "sentinel-master", "", "Sentinel master name (enables Sentinel mode)")
	flag.DurationVar(&writeRate, "write-rate", 100*time.Millisecond, "Interval between writes")
	flag.DurationVar(&operationTimeout, "timeout", 500*time.Millisecond, "Timeout per operation")
	flag.StringVar(&keyPrefix, "prefix", "chaos", "Key prefix")
	flag.DurationVar(&duration, "duration", 0, "Test duration (0 = run until interrupted)")
	flag.DurationVar(&statusInterval, "status-interval", 5*time.Second, "Interval between status outputs")
	flag.Parse()

	addrList := strings.Split(addrs, ",")
	for i := range addrList {
		addrList[i] = strings.TrimSpace(addrList[i])
	}

	fmt.Printf("Chaos Test Client Starting\n")
	fmt.Printf("  Addresses: %v\n", addrList)
	fmt.Printf("  Cluster mode: %v\n", clusterMode)
	if sentinelMaster != "" {
		fmt.Printf("  Sentinel master: %s\n", sentinelMaster)
	}
	fmt.Printf("  Write rate: %v\n", writeRate)
	fmt.Printf("  Operation timeout: %v\n", operationTimeout)
	fmt.Printf("  Key prefix: %s\n", keyPrefix)
	if duration > 0 {
		fmt.Printf("  Duration: %v\n", duration)
	} else {
		fmt.Printf("  Duration: until interrupted\n")
	}
	fmt.Println()

	client, err := chaos.NewTestClient(chaos.Config{
		Addrs:            addrList,
		Password:         password,
		ClusterMode:      clusterMode,
		SentinelMaster:   sentinelMaster,
		WriteRate:        writeRate,
		OperationTimeout: operationTimeout,
		KeyPrefix:        keyPrefix,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}

	// Handle signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start the client
	client.Start()
	fmt.Printf("Client started at %s, running...\n", time.Now().Format(time.RFC3339Nano))

	// Status ticker
	statusTicker := time.NewTicker(statusInterval)
	defer statusTicker.Stop()

	// Duration timer (if specified)
	var durationCh <-chan time.Time
	if duration > 0 {
		durationCh = time.After(duration)
	}

	startTime := time.Now()
	var lastMetrics chaos.MetricsSnapshot

loop:
	for {
		select {
		case <-sigCh:
			fmt.Println("\nReceived shutdown signal")
			break loop
		case <-durationCh:
			fmt.Println("\nDuration reached")
			break loop
		case <-statusTicker.C:
			m := client.GetMetrics()
			elapsed := time.Since(startTime).Round(time.Second)

			// Calculate deltas
			dWrite := m.WriteSuccesses - lastMetrics.WriteSuccesses
			dRead := m.ReadSuccesses - lastMetrics.ReadSuccesses
			lastMetrics = m

			fmt.Printf("[%v] Writes: %d/%d (%.1f%%, +%d), Reads: %d/%d (%.1f%%, +%d), Lost: %d, Corruptions: %d\n",
				elapsed,
				m.WriteSuccesses, m.WriteAttempts, m.WriteAvailability()*100, dWrite,
				m.ReadSuccesses, m.ReadAttempts, m.ReadAvailability()*100, dRead,
				m.LostKeys, m.DataCorruptions)
		}
	}

	// Stop client and collect final metrics
	client.Stop()

	// Exact durability verdict: re-read every acknowledged write once. The
	// in-flight counters sample randomly, so they can only approximate how many
	// writes were actually lost; this counts them.
	fmt.Println("\nVerifying every acknowledged write...")
	verification := client.VerifyAll(context.Background())
	if verification.Missing > 0 {
		fmt.Printf("LOST %d of %d acknowledged writes (sample: %v)\n",
			verification.Missing, verification.Checked, verification.MissingSample)
	}
	if verification.Unreadable > 0 {
		fmt.Printf("WARNING: %d keys were unreadable — not counted as lost\n", verification.Unreadable)
	}

	finalMetrics := client.GetMetrics()

	fmt.Println("\n========== FINAL RESULTS ==========")
	fmt.Println(finalMetrics.String())
	fmt.Println()

	// Output JSON for machine parsing (on a single line with marker)
	jsonBytes, _ := json.Marshal(finalMetrics)
	fmt.Printf("METRICS_JSON:%s\n", string(jsonBytes))

	// Exit with error if data corruption detected
	if finalMetrics.DataCorruptions > 0 {
		fmt.Println("\nCRITICAL: Data corruption detected!")
		os.Exit(2)
	}

	if err := client.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error closing client: %v\n", err)
	}
}
