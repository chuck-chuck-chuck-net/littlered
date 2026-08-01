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
	"sort"
	"strconv"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	clifailover "github.com/littlered-operator/littlered-operator/internal/cli/failover"
	"github.com/littlered-operator/littlered-operator/internal/cli/types"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// failoverPodViews resolves the K8s-side per-pod inputs (assignment
// annotations, role label, readiness/restarts) for the pure analysis.
func failoverPodViews(cCtx *types.ClusterContext) []clifailover.PodView {
	views := make([]clifailover.PodView, 0, len(cCtx.RedisPods))
	for _, pod := range cCtx.RedisPods {
		v := clifailover.PodView{
			Name:        pod.Name,
			IP:          pod.Status.PodIP,
			Phase:       string(pod.Status.Phase),
			Terminating: pod.DeletionTimestamp != nil,
			RoleLabel:   pod.Labels[clifailover.LabelRole],
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name == cCtx.RedisContainer {
				v.Ready = cs.Ready
				v.Restarted = cs.RestartCount > 0
			}
		}
		if role, ok := pod.Annotations[clifailover.AnnotationAssignedRole]; ok {
			v.HasAssignment = true
			v.AssignedRole = role
			v.AssignedMasterIP = pod.Annotations[clifailover.AnnotationAssignedMasterIP]
			if e, err := strconv.ParseInt(pod.Annotations[clifailover.AnnotationAssignmentEpoch], 10, 64); err == nil {
				v.Epoch = e
			}
		}
		views = append(views, v)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	return views
}

// gatherFailover collects the K8s pod views and the live per-pod replication
// state (exec-based INFO on every data pod; there are no Sentinels to query).
func gatherFailover(
	ctx context.Context, coreClient *kubernetes.Clientset, config *rest.Config,
	cCtx *types.ClusterContext,
) ([]clifailover.PodView, *redisclient.ReplicationState) {
	redisMap := make(map[string]string)
	for _, p := range cCtx.RedisPods {
		if p.Status.PodIP != "" {
			redisMap[p.Status.PodIP] = p.Name
		}
	}
	g := &cliGatherer{coreClient: coreClient, config: config, cCtx: cCtx}
	state := redisclient.GatherReplicationState(ctx, g, redisMap, nil)
	return failoverPodViews(cCtx), state
}

// describeFailoverPod renders one per-pod status line: observed role vs
// assigned role, epoch, link, offset, keys, and the K8s role label.
func describeFailoverPod(v clifailover.PodView, state *redisclient.ReplicationState, parked map[string]bool) string {
	status := statusUnreachable
	if rn := state.RedisNodes[v.IP]; rn != nil && rn.Reachable {
		status = fmt.Sprintf("role:%s", rn.Role)
		if rn.Role != roleMaster {
			status += fmt.Sprintf(", following:%s, link:%s", rn.MasterHost, rn.LinkStatus)
		}
		status += fmt.Sprintf(", offset:%d, keys:%d", rn.Offset, rn.Keys)
	}
	if v.HasAssignment {
		status += fmt.Sprintf(", assigned:%s@%d", v.AssignedRole, v.Epoch)
	} else {
		status += ", assigned:none"
	}
	label := v.RoleLabel
	if label == "" {
		label = "<none>"
	}
	status += ", label:" + label
	if parked[v.Name] {
		status += " [PARKED]"
	}
	if v.Terminating {
		status += " [terminating]"
	}
	if !v.Ready {
		status += " [not-ready]"
	}
	return status
}

// verifyFailover gathers ground truth for a failover-mode instance, computes
// the intent and the authority master (intent ∩ observation), prints per-pod
// lines and findings, and returns an error when verification fails.
func verifyFailover(
	ctx context.Context, coreClient *kubernetes.Clientset, config *rest.Config,
	cCtx *types.ClusterContext,
) error {
	fmt.Println("Gathering Replication Ground Truth...")
	views, state := gatherFailover(ctx, coreClient, config, cCtx)
	analysis := clifailover.Analyze(views, state)

	fmt.Println("\nAssignment Intent:")
	if analysis.Intent.MasterName != "" {
		fmt.Printf("  Intended Master: %s (%s, epoch %d)\n",
			analysis.Intent.MasterName, analysis.Intent.MasterIP, analysis.Intent.MasterEpoch)
	} else {
		fmt.Printf("  Intended Master: NONE (no master assignment stamped)\n")
	}
	fmt.Printf("  Max Assignment Epoch: %d\n", analysis.Intent.MaxEpoch)

	parked := make(map[string]bool, len(analysis.Parked))
	for _, name := range analysis.Parked {
		parked[name] = true
	}

	fmt.Println("\nRedis Status:")
	for _, v := range views {
		fmt.Printf("  - Redis %s: %s\n", v.Name, describeFailoverPod(v, state, parked))
	}

	fmt.Printf("\nGround Truth Summary:\n")
	if analysis.AuthorityIP != "" {
		fmt.Printf("  [OK] Authority Master: %s (%s)\n", analysis.AuthorityPod, analysis.AuthorityIP)
	} else {
		fmt.Printf("  [FAIL] Authority Master: NONE (intent not observed live)\n")
	}

	if len(analysis.Findings) > 0 {
		fmt.Println("\nFindings:")
		for _, f := range analysis.Findings {
			fmt.Printf("  - [%s] %s\n", f.Severity, f.Message)
		}
	}

	switch {
	case analysis.Failed():
		fmt.Println("\n[FAIL] Instance has consistency issues!")
		return fmt.Errorf("instance %s/%s is not healthy or consistent", cCtx.Namespace, cCtx.Name)
	case analysis.Degraded():
		fmt.Printf("\n[DEGRADED] Instance is functional but has %d warning(s); not fully healthy.\n",
			len(analysis.Findings))
		return nil
	default:
		fmt.Println("\n[OK] Instance configuration is consistent.")
		return nil
	}
}

// verifyFailoverJSON gathers failover-mode ground truth and returns it as a
// JSON-serialisable struct without printing anything.
func verifyFailoverJSON(
	ctx context.Context, coreClient *kubernetes.Clientset, config *rest.Config,
	cCtx *types.ClusterContext, name, namespace string,
) failoverVerifyJSON {
	views, state := gatherFailover(ctx, coreClient, config, cCtx)
	analysis := clifailover.Analyze(views, state)
	return buildFailoverVerifyJSON(name, namespace, views, state, analysis)
}
