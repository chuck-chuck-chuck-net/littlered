package cmd

import (
	"fmt"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// migrationBanner returns a one-line, human-readable banner describing an
// in-progress in-place legacy→per-shard cluster migration (ADR-013), or the
// empty string when there is nothing to surface: no migration (nil), an unset
// phase, or a migration that has already Completed. Pure formatter, read-only.
func migrationBanner(m *littleredv1alpha1.ClusterMigrationStatus) string {
	if m == nil || m.Phase == "" || m.Phase == string(redisclient.MigrationComplete) {
		return ""
	}
	return fmt.Sprintf("Migration: %s (%d/%d shards moved) — legacy→per-shard in progress",
		m.Phase, m.ShardsMoved, m.TotalShards)
}

// clusterMigration nil-safely extracts the migration status from a LittleRed CR.
func clusterMigration(lr *littleredv1alpha1.LittleRed) *littleredv1alpha1.ClusterMigrationStatus {
	if lr == nil || lr.Status.Cluster == nil {
		return nil
	}
	return lr.Status.Cluster.Migration
}
