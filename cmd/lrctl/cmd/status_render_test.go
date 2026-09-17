package cmd

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
	redisclient "github.com/chuck-chuck-chuck-net/littlered/internal/redis"
)

// captureStdout runs f and returns everything it wrote to os.Stdout.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	f()
	_ = w.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("copy: %v", err)
	}
	return buf.String()
}

func clusterLR() *littleredv1alpha1.LittleRed {
	lr := &littleredv1alpha1.LittleRed{}
	lr.Name = "demo"
	lr.Namespace = "default"
	lr.Spec.Mode = modeCluster
	lr.Status.Phase = "Initializing"
	lr.Status.Redis = littleredv1alpha1.RedisStatus{Ready: 6, Total: 6}
	return lr
}

// TestPrintStatus_MigrationBanner exercises the real status renderer end-to-end:
// an in-progress migration shows the banner line, and a non-migrating instance
// must NOT (documented status output for non-migrating instances must not drift).
func TestPrintStatus_MigrationBanner(t *testing.T) {
	const wantLine = "Migration: Replicate (2/4 shards moved) — legacy→per-shard in progress"

	t.Run("in-progress shows banner", func(t *testing.T) {
		lr := clusterLR()
		lr.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{
			Migration: &littleredv1alpha1.ClusterMigrationStatus{
				Phase:       string(redisclient.MigrationReplicate),
				ShardsMoved: 2,
				TotalShards: 4,
			},
		}
		out := captureStdout(t, func() { printStatus(lr) })
		if !strings.Contains(out, wantLine) {
			t.Errorf("expected banner line %q in output, got:\n%s", wantLine, out)
		}
	})

	t.Run("no migration renders no banner line", func(t *testing.T) {
		out := captureStdout(t, func() { printStatus(clusterLR()) })
		if strings.Contains(out, "Migration:") {
			t.Errorf("non-migrating status must not contain a Migration banner, got:\n%s", out)
		}
	})

	t.Run("Complete renders no banner line", func(t *testing.T) {
		lr := clusterLR()
		lr.Status.Cluster = &littleredv1alpha1.ClusterStatusInfo{
			Migration: &littleredv1alpha1.ClusterMigrationStatus{
				Phase:       string(redisclient.MigrationComplete),
				ShardsMoved: 4,
				TotalShards: 4,
			},
		}
		out := captureStdout(t, func() { printStatus(lr) })
		if strings.Contains(out, "Migration:") {
			t.Errorf("completed migration must not contain a Migration banner, got:\n%s", out)
		}
	})
}
