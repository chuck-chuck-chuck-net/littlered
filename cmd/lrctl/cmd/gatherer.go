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

package cmd

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/littlered-operator/littlered-operator/internal/cli/k8s"
	"github.com/littlered-operator/littlered-operator/internal/cli/types"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

type cliGatherer struct {
	coreClient *kubernetes.Clientset
	config     *rest.Config
	cCtx       *types.ClusterContext
}

func (g *cliGatherer) GetRedisState(ctx context.Context, podName, ip string) (*redisclient.RedisNodeState, error) {
	cmd := []string{redisCliBin, infoSubcommand}
	stdout, _, err := k8s.Exec(ctx, g.coreClient, g.config, g.cCtx.Namespace, podName, g.cCtx.RedisContainer, cmd)
	if err != nil {
		return nil, err
	}

	role := redisclient.ParseInfoField(stdout, "role")
	mHost := redisclient.ParseInfoField(stdout, "master_host")
	link := redisclient.ParseInfoField(stdout, "master_link_status")

	// If mHost is a hostname, resolve it to an IP using K8s API info
	if mHost != "" {
		mHost = g.resolveIdentityToIP(mHost)
	}

	offsetStr := ""
	if role == roleMaster {
		offsetStr = redisclient.ParseInfoField(stdout, "master_repl_offset")
	} else {
		offsetStr = redisclient.ParseInfoField(stdout, "slave_repl_offset")
	}
	var offset int64
	if offsetStr != "" {
		_, _ = fmt.Sscanf(offsetStr, "%d", &offset)
	}

	return &redisclient.RedisNodeState{
		PodName:    podName,
		IP:         ip,
		Role:       role,
		MasterHost: mHost,
		LinkStatus: link,
		Offset:     offset,
		Keys:       redisclient.ParseKeyspaceKeys(stdout),
		Replid:     redisclient.ParseInfoField(stdout, "master_replid"),
		Replid2:    redisclient.ParseInfoField(stdout, "master_replid2"),
		Reachable:  true,
	}, nil
}

func (g *cliGatherer) GetSentinelState(
	ctx context.Context, podName, ip, masterName string,
) (*redisclient.SentinelNodeState, error) {
	// Get Master. The name comes from the caller rather than being re-derived from
	// g.cCtx, so the operator and the CLI resolve it identically (LR-041).
	masterCmd := []string{redisCliBin, "-p", "26379", modeSentinel, roleMaster, masterName}
	stdout, _, err := k8s.Exec(ctx, g.coreClient, g.config, g.cCtx.Namespace, podName, g.cCtx.SentinelContainer, masterCmd)
	if err != nil {
		if strings.Contains(err.Error(), "ERR No such master") {
			return &redisclient.SentinelNodeState{
				PodName:    podName,
				IP:         ip,
				Monitoring: false,
				Reachable:  true,
			}, nil
		}
		return nil, err
	}

	state := &redisclient.SentinelNodeState{
		PodName:    podName,
		IP:         ip,
		Monitoring: true,
		Reachable:  true,
	}

	// Parse SENTINEL MASTER output
	lines := strings.Split(strings.ReplaceAll(stdout, "\r", ""), "\n")
	for i := 0; i < len(lines)-1; i++ {
		line := strings.TrimSpace(lines[i])
		if line == "ip" {
			mIP := strings.TrimSpace(lines[i+1])
			// Resolve hostname to IP
			state.MasterIP = g.resolveIdentityToIP(mIP)
		}
		if line == "failover-status" {
			state.FailoverStatus = strings.TrimSpace(lines[i+1])
		}
		// Retained for the cross-instance diagnostic: `flags` distinguishes a master
		// that is dead from one that is alive but not ours, and the counts are the
		// loudest sign that another deployment has joined this quorum.
		if line == "flags" {
			state.MasterFlags = strings.TrimSpace(lines[i+1])
		}
		if line == "num-other-sentinels" {
			state.NumOtherSentinels = atoiSafe(strings.TrimSpace(lines[i+1]))
		}
		if line == "num-slaves" {
			state.NumSlaves = atoiSafe(strings.TrimSpace(lines[i+1]))
		}
	}

	// Get Replicas. Every replica Sentinel knows is recorded with its REAL flags,
	// including ones that are not this instance's pods — those are the whole point of
	// the cross-instance diagnostic. An earlier version fabricated flags ("found" for
	// our pods, "s_down,ghost" for everything else), which would have made every
	// foreign replica look like dead debris and hidden exactly what we are looking for.
	replicasCmd := []string{redisCliBin, "-p", "26379", modeSentinel, "replicas", masterName}
	stdout, _, err = k8s.Exec(
		ctx, g.coreClient, g.config, g.cCtx.Namespace, podName, g.cCtx.SentinelContainer, replicasCmd)
	if err == nil {
		state.Replicas = parseSentinelReplicas(stdout, g.resolveIdentityToIP)
	}

	return state, nil
}

// resolveIdentityToIP takes an identity (IP, pod name, or FQDN) and tries to resolve it to a Pod IP
// using the pods already discovered in ClusterContext.
func (g *cliGatherer) resolveIdentityToIP(identity string) string {
	if identity == "" {
		return ""
	}

	// If it's already an IP, return it
	if isIP(identity) {
		return identity
	}

	// For hostnames (pod-0.headless-svc.namespace.svc.cluster.local),
	// the first segment is usually the pod name.
	podNameCandidate := identity
	if before, _, ok := strings.Cut(identity, "."); ok {
		podNameCandidate = before
	}

	// Look up in Redis pods
	for _, p := range g.cCtx.RedisPods {
		if p.Name == podNameCandidate && p.Status.PodIP != "" {
			return p.Status.PodIP
		}
	}

	// Look up in Sentinel pods
	for _, p := range g.cCtx.SentinelPods {
		if p.Name == podNameCandidate && p.Status.PodIP != "" {
			return p.Status.PodIP
		}
	}

	return identity // Fallback to original if not found
}

func isIP(s string) bool {
	// Simple IP check: contains 3 dots and only digits/dots
	dots := 0
	for _, c := range s {
		if c == '.' {
			dots++
		} else if c < '0' || c > '9' {
			return false
		}
	}
	return dots == 3
}

func (g *cliGatherer) GetClusterID(ctx context.Context, podName, ip string) (string, error) {
	cmd := []string{redisCliBin, modeCluster, "myid"}
	stdout, _, err := k8s.Exec(ctx, g.coreClient, g.config, g.cCtx.Namespace, podName, g.cCtx.RedisContainer, cmd)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(stdout), nil
}

func (g *cliGatherer) GetClusterInfo(ctx context.Context, podName, ip string) (*redisclient.ClusterInfo, error) {
	cmd := []string{redisCliBin, clusterSubcommand, infoSubcommand}
	stdout, _, err := k8s.Exec(ctx, g.coreClient, g.config, g.cCtx.Namespace, podName, g.cCtx.RedisContainer, cmd)
	if err != nil {
		return nil, err
	}
	return redisclient.ParseClusterInfo(stdout), nil
}

func (g *cliGatherer) GetClusterNodes(ctx context.Context, podName, ip string) ([]redisclient.ClusterNodeInfo, error) {
	cmd := []string{redisCliBin, clusterSubcommand, "nodes"}
	stdout, _, err := k8s.Exec(ctx, g.coreClient, g.config, g.cCtx.Namespace, podName, g.cCtx.RedisContainer, cmd)
	if err != nil {
		return nil, err
	}
	return redisclient.ParseClusterNodes(stdout), nil
}
