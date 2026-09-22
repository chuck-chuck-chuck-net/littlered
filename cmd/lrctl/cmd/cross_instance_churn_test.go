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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/chuck-chuck-chuck-net/littlered/internal/cli/types"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// churnInstance is the instance name this file's fixtures use, taken from the live
// t3e run the entry records.
const churnInstance = "store-sentinel"

// churnReplica is the pod each churn row puts into the state under test.
const churnReplica = "inst-redis-1"

// churnPod builds one Redis pod of ours with the given redis-container readiness.
func churnPod(name, ip string, ready bool) corev1.Pod {
	p := corev1.Pod{}
	p.Name = name
	p.Status.PodIP = ip
	p.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: containerNameRedis, Ready: ready},
	}
	return p
}

// TestCrossInstanceChurnCaveat pins the discriminator the live t3e run was missing.
//
// During an operator-driven rollout (measured 2026-09-22, an ordinary
// spec.sentinel.masterName rename) `lrctl verify` reported our own just-replaced
// redis-2 as "live replicas that are not this instance's pods" and pointed the reader
// at the capture runbook — five seconds before the operator itself logged the same
// address as an ordinary ghost. A replaced pod's address leaves the pod list the
// instant its object goes, while Sentinel does not flag it down for a whole
// down-after-milliseconds (LR-050), so no address set can attribute it: OwnedIPs
// covers the pod that is still listed (LR-053) and nothing covers the one that is not.
//
// The evidence is NEVER suppressed — it is the floorless diagnostic LR-039 built to
// fire on a PARTIAL capture and LR-056 deliberately left without a floor, and it is
// LR-054's stated mitigation for a data-holding victim that can no longer be
// diagnosed by the operator. Only the reading is qualified.
func TestCrossInstanceChurnCaveat(t *testing.T) {
	cases := []struct {
		name     string
		pods     []corev1.Pod
		expected int
		wantLine bool
		contains []string
	}{
		{
			// The live shape: redis-2 was replaced ~1s earlier and is still
			// syncing, so its redis container is not Ready.
			name: "a pod of ours is not Ready: the addresses above are not attributable",
			pods: []corev1.Pod{
				churnPod(churnInstance+"-redis-0", "10.233.192.112", true),
				churnPod(churnInstance+"-redis-1", "10.233.192.132", true),
				churnPod(churnInstance+"-redis-2", "10.233.192.177", false),
			},
			expected: 3,
			wantLine: true,
			contains: []string{churnInstance + "-redis-2", "re-run"},
		},
		{
			// POSITIVE CONTROL. Without this the caveat could be unconditional,
			// which would dilute every genuine capture report into a maybe.
			name: "every pod Ready: a genuine capture is reported without a caveat",
			pods: []corev1.Pod{
				churnPod("inst-redis-0", "10.0.0.1", true),
				churnPod(churnReplica, "10.0.0.2", true),
			},
			expected: 2,
			wantLine: false,
		},
		{
			// MEASURED on t3e 2026-09-22: for 18 of the 29 samples in which the
			// false evidence was printed during an ordinary metadata rollout,
			// every pod still LISTED was Ready — the departed one was simply gone
			// from the list, and with it the expected-replica count the surplus
			// clauses compare against, so the evidence was a count surplus rather
			// than an address. The readiness clause alone cannot see that.
			name: "a pod of ours is missing from the list entirely",
			pods: []corev1.Pod{
				churnPod("inst-redis-0", "10.0.0.1", true),
				churnPod(churnReplica, "10.0.0.2", true),
			},
			expected: 3,
			wantLine: true,
			contains: []string{"2 of 3 redis pods"},
		},
		{
			// lrctl's pod list carries terminating pods (it applies no
			// deletionTimestamp filter — LR-053), and a terminating pod is the
			// half-second before the address in question stops being listed at all.
			name: "a terminating pod of ours is churn too",
			pods: func() []corev1.Pod {
				p := churnPod(churnReplica, "10.0.0.2", true)
				now := metav1.Now()
				p.DeletionTimestamp = &now
				return []corev1.Pod{churnPod("inst-redis-0", "10.0.0.1", true), p}
			}(),
			expected: 2,
			wantLine: true,
			contains: []string{churnReplica},
		},
		{
			// A pod whose redis container has not reported at all is being
			// created, which is the same window seen one moment earlier.
			name: "a pod with no container status yet is churn",
			pods: func() []corev1.Pod {
				p := corev1.Pod{}
				p.Name = churnReplica
				return []corev1.Pod{churnPod("inst-redis-0", "10.0.0.1", true), p}
			}(),
			expected: 2,
			wantLine: true,
			contains: []string{churnReplica},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lines := crossInstanceChurnCaveat(churnGroup{
				kind: containerNameRedis, pods: tc.pods,
				container: containerNameRedis, expected: tc.expected,
			})
			if got := len(lines) > 0; got != tc.wantLine {
				t.Fatalf("crossInstanceChurnCaveat produced %d lines, want any = %v\n%s",
					len(lines), tc.wantLine, strings.Join(lines, "\n"))
			}
			joined := strings.Join(lines, "\n")
			for _, want := range tc.contains {
				if !strings.Contains(joined, want) {
					t.Errorf("caveat does not mention %q:\n%s", want, joined)
				}
			}
		})
	}
}

// TestCrossInstanceEvidenceCarriesTheChurnCaveat is the wiring guard: the caveat is
// worth nothing unless it reaches the reader BEFORE the runbook pointer, which is the
// sentence that sends someone to a procedure that deletes pods.
func TestCrossInstanceEvidenceCarriesTheChurnCaveat(t *testing.T) {
	const foreignIP = "10.233.192.146"

	state := redisclient.NewReplicationState()
	state.AddLiveTopologyIP("10.233.192.132")
	state.SentinelNodes["10.0.1.1"] = &redisclient.SentinelNodeState{
		PodName: churnInstance + "-sentinel-0", IP: "10.0.1.1",
		Reachable: true, Monitoring: true,
		MasterIP: "10.233.192.132", MasterFlags: roleMaster,
		Replicas: []redisclient.ReplicaInfo{{IP: foreignIP, Flags: "slave"}},
	}

	cCtx := &types.ClusterContext{
		Name: churnInstance, Namespace: "lr062",
		SentinelMasterName: "renamed.sentinel",
		RedisContainer:     containerNameRedis,
		RedisPods: []corev1.Pod{
			churnPod(churnInstance+"-redis-0", "10.233.192.112", true),
			churnPod(churnInstance+"-redis-1", "10.233.192.132", true),
			churnPod(churnInstance+"-redis-2", "10.233.192.177", false),
		},
		SentinelPods: podsWithIPs(churnInstance+"-sentinel-0", "10.0.1.1"),
	}

	out := captureStdout(t, func() { reportCrossInstance(state, cCtx) })

	// The finding itself must survive: this is a diagnostic that must keep firing
	// on a partial capture (LR-039/LR-056) and is LR-054's remaining signal.
	if !strings.Contains(out, foreignIP) {
		t.Fatalf("the cross-instance evidence was suppressed; it must only be qualified:\n%s", out)
	}
	lower := strings.ToLower(out)
	caveat := strings.Index(lower, "own pods are in churn")
	runbook := strings.Index(lower, "runbook in docs/usage.md")
	switch {
	case caveat < 0:
		t.Fatalf("no churn caveat while one of our own pods is not Ready:\n%s", out)
	case runbook < 0:
		t.Fatalf("the runbook pointer disappeared:\n%s", out)
	case caveat > runbook:
		t.Fatalf("the caveat is printed AFTER the runbook pointer it qualifies:\n%s", out)
	}
}

// TestCrossInstanceEvidenceCaveatsSentinelChurn is the half the first live A/B missed.
//
// Measured on t3e (2026-09-22, three consecutive rollouts triggered by a CR metadata
// edit): of the samples in which the false evidence was printed, the majority carried
// no foreign ADDRESS at all — they were `<pod> reports 3 other sentinels; 2 were
// deployed`, i.e. our own Sentinels still counting a peer that the SENTINEL
// StatefulSet had just replaced. A stale known-sentinel entry never ages out on its
// own (LR-039), and every Redis pod is Ready by then, so a Redis-keyed caveat is
// structurally blind to it.
func TestCrossInstanceEvidenceCaveatsSentinelChurn(t *testing.T) {
	state := redisclient.NewReplicationState()
	state.AddLiveTopologyIP("10.0.0.1")
	state.SentinelNodes["10.0.1.1"] = &redisclient.SentinelNodeState{
		PodName: churnInstance + "-sentinel-2", IP: "10.0.1.1",
		Reachable: true, Monitoring: true,
		MasterIP: "10.0.0.1", MasterFlags: roleMaster,
		NumOtherSentinels: 3, // 2 were deployed: the departed peer is still counted
	}

	cCtx := &types.ClusterContext{
		Name: churnInstance, Namespace: "lr062",
		SentinelMasterName: "renamed.sentinel",
		RedisContainer:     containerNameRedis,
		SentinelContainer:  "sentinel",
		RedisPods: []corev1.Pod{
			churnPod(churnInstance+"-redis-0", "10.0.0.1", true),
			churnPod(churnInstance+"-redis-1", "10.0.0.2", true),
			churnPod(churnInstance+"-redis-2", "10.0.0.3", true),
		},
		// The Sentinel pod that was just replaced is not Ready yet; its predecessor
		// is the peer still being counted.
		SentinelPods: []corev1.Pod{
			sentinelChurnPod(churnInstance+"-sentinel-0", "10.0.1.1", true),
			sentinelChurnPod(churnInstance+"-sentinel-1", "10.0.1.2", true),
			sentinelChurnPod(churnInstance+"-sentinel-2", "10.0.1.3", false),
		},
	}

	out := captureStdout(t, func() { reportCrossInstance(state, cCtx) })
	if !strings.Contains(out, "reports 3 other sentinels") {
		t.Fatalf("the peer-surplus evidence was suppressed; it must only be qualified:\n%s", out)
	}
	if !strings.Contains(strings.ToLower(out), "own pods are in churn") {
		t.Fatalf("no churn caveat while one of our own SENTINEL pods is not Ready:\n%s", out)
	}
	if !strings.Contains(out, churnInstance+"-sentinel-2") {
		t.Errorf("the caveat does not name the sentinel pod that is in churn:\n%s", out)
	}
}

// sentinelChurnPod is churnPod for a Sentinel pod, whose container is named differently.
func sentinelChurnPod(name, ip string, ready bool) corev1.Pod {
	p := churnPod(name, ip, ready)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: containerNameSentinel, Ready: ready}}
	return p
}
