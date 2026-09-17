package cmd

import (
	"testing"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

func TestMigrationBanner(t *testing.T) {
	tests := []struct {
		name string
		in   *littleredv1alpha1.ClusterMigrationStatus
		want string
	}{
		{
			name: "in-progress Replicate renders the banner",
			in: &littleredv1alpha1.ClusterMigrationStatus{
				Phase:       string(redisclient.MigrationReplicate),
				ShardsMoved: 2,
				TotalShards: 4,
			},
			want: "Migration: Replicate (2/4 shards moved) — legacy→per-shard in progress",
		},
		{
			name: "in-progress Standup at zero renders the banner",
			in: &littleredv1alpha1.ClusterMigrationStatus{
				Phase:       string(redisclient.MigrationStandup),
				ShardsMoved: 0,
				TotalShards: 4,
			},
			want: "Migration: Standup (0/4 shards moved) — legacy→per-shard in progress",
		},
		{
			name: "nil migration renders nothing",
			in:   nil,
			want: "",
		},
		{
			name: "empty phase renders nothing",
			in:   &littleredv1alpha1.ClusterMigrationStatus{Phase: "", ShardsMoved: 0, TotalShards: 4},
			want: "",
		},
		{
			name: "Complete renders nothing",
			in: &littleredv1alpha1.ClusterMigrationStatus{
				Phase:       string(redisclient.MigrationComplete),
				ShardsMoved: 4,
				TotalShards: 4,
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := migrationBanner(tt.in)
			if got != tt.want {
				t.Errorf("migrationBanner()\n  got:  %q\n  want: %q", got, tt.want)
			}
		})
	}
}

// TestClusterMigration_NilSafe pins the nil-safe accessor: no CR, no cluster
// status, and a populated migration each resolve correctly.
func TestClusterMigration_NilSafe(t *testing.T) {
	if got := clusterMigration(nil); got != nil {
		t.Errorf("clusterMigration(nil) = %v, want nil", got)
	}

	lrNoCluster := &littleredv1alpha1.LittleRed{}
	if got := clusterMigration(lrNoCluster); got != nil {
		t.Errorf("clusterMigration(no cluster status) = %v, want nil", got)
	}

	mig := &littleredv1alpha1.ClusterMigrationStatus{Phase: "Draining"}
	lr := &littleredv1alpha1.LittleRed{}
	lr.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{Migration: mig}
	if got := clusterMigration(lr); got != mig {
		t.Errorf("clusterMigration(populated) = %v, want %v", got, mig)
	}
}
