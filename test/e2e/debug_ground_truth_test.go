//go:build e2e
// +build e2e

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

package e2e

import "testing"

// This is a plain table test, not a Ginkgo spec, and it needs no cluster. It
// lives here only because the code it guards carries the e2e build tag. Run it
// on its own — an unfiltered `go test -tags e2e ./test/e2e/...` starts the whole
// suite:
//
//	go test -tags e2e ./test/e2e/ -run TestClusterEnabled

// clusterInfo842 is a real Redis 8.4.2 CLUSTER INFO reply, captured verbatim from
// the shipped default image (chaos-cluster-stable-shard-2-0, 2026-08-23).
//
// The load-bearing detail is what is NOT here: there is no cluster_enabled field.
// The reply opens with cluster_state. Older Redis did carry cluster_enabled:1 as
// its first line, which is why gating on it looks obviously right and is silently
// wrong — see the doc comment on clusterEnabled.
const clusterInfo842 = `cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:3
cluster_size:3
cluster_current_epoch:2
cluster_my_epoch:0
cluster_stats_messages_ping_sent:1370
cluster_stats_messages_sent:2792
cluster_slot_migration_active_tasks:0`

// clusterInfoLagging is the shape that mattered on 2026-08-23: a cluster node
// answering perfectly well while holding only its own slot range. It must still
// count as cluster-enabled — the point of the probe is to capture exactly this.
const clusterInfoLagging = `cluster_state:fail
cluster_slots_assigned:5461
cluster_slots_ok:5461
cluster_known_nodes:3
cluster_size:1`

// clusterInfoDisabled is what a non-cluster node (standalone, sentinel, failover)
// answers to every CLUSTER subcommand, as execInPod records it.
const clusterInfoDisabled = "ERR This instance has cluster support disabled\n(probe exited with exit status 1)"

// TestClusterEnabled pins the discriminator behind the conditional CLUSTER NODES
// probe. It exists because that gate was keyed on cluster_enabled — a field Redis
// 8.4.2 does not emit — and was therefore permanently inert until a live run
// caught it. That is the LR-041 shape: a probe returning a plausible-looking
// negative instead of an error, invisible to every future reader, so the property
// belongs in a test rather than in a doc comment.
//
// Teeth shown by reverting clusterEnabled to `strings.Contains(clusterInfo,
// "cluster_enabled")`: the two live-reply rows below go red, which is precisely
// the regression being prevented.
func TestClusterEnabled(t *testing.T) {
	tests := []struct {
		name  string
		reply string
		want  bool
	}{
		{
			name:  "Redis 8.4.2 cluster reply — no cluster_enabled field at all",
			reply: clusterInfo842,
			want:  true,
		},
		{
			name:  "cluster node with an incomplete slot view is still cluster-enabled",
			reply: clusterInfoLagging,
			want:  true,
		},
		{
			name:  "non-cluster node: CLUSTER support disabled",
			reply: clusterInfoDisabled,
			want:  false,
		},
		{
			name:  "empty reply (pod unreachable or parked in the startup wait-loop)",
			reply: "",
			want:  false,
		},
		{
			name:  "probe failure rendering carries no cluster_state",
			reply: "(probe failed: exit status 1)",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clusterEnabled(tt.reply); got != tt.want {
				t.Fatalf("clusterEnabled() = %v, want %v\nreply:\n%s", got, tt.want, tt.reply)
			}
		})
	}
}
