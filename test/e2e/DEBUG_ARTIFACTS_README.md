# E2E Test Debug Artifacts

## Overview

When `DEBUG_ON_FAILURE=true` is set, failed e2e tests automatically collect comprehensive debug artifacts to help with post-mortem analysis.

## What Gets Collected

When a test fails, a timestamped directory is created in the current directory with the format:

```
./debug-artifacts-YYYYMMDD-HHMMSS-<sanitized-test-name>/
```

This directory contains:

### 1. Operator Logs
- **File**: `operator-logs.txt`
- **Content**: Operator logs since the test started (filtered by `--since` duration)
- **Fallback**: If time-filtered logs fail, collects last 1000 lines
- **Use**: Trace operator's decisions and actions during the test

### 2. CR Status
- **File**: `cr-<name>.yaml`
- **Content**: Full YAML representation of the LittleRed custom resource
- **Use**: See final state of spec and status, including node topology

### 3. Pod Logs
- **Files**: `pod-<podname>-<container>.log`
- **Content**: Last 1000 lines of logs from each container
- **Includes**: Redis/Valkey pods, Sentinel pods, previous logs if pod restarted
- **Use**: Debug application-level issues, see Redis cluster state changes

### 4. Chaos Client Logs (if applicable)
- **Files**: `chaos-client-<name>.log`, `chaos-metrics-<name>.json`
- **Content**: Chaos client output and metrics
- **Use**: Analyze availability, data corruption, performance during chaos tests

### 5. Cluster State
- **Files**: `pods.txt`, `events.txt`, `littlered-crs.yaml`, `statefulsets.yaml`, `services.yaml`
- **Content**: Kubernetes resource states at time of failure
- **Use**: See pod status, events, resource configuration

### 6. Test Metadata
- **File**: `test-metadata.txt`
- **Content**: Test name, namespace, CR name, timing, failure location and message
- **Use**: Quick reference for what failed and when

## Usage

### Running Tests with Debug Collection

```bash
# Enable debug artifact collection
DEBUG_ON_FAILURE=true make test-e2e

# Or with individual test
DEBUG_ON_FAILURE=true go test -tags e2e -v ./test/e2e -ginkgo.focus="should heal cluster"
```

### What Happens on Failure

1. Test fails
2. AfterEach hook detects failure
3. Artifacts are collected automatically
4. Directory path is printed to console
5. Test continues (namespace cleanup is skipped)

### Analyzing Artifacts

After a test fails:

```bash
# Find the debug directory
ls -lt debug-artifacts-* | head -1

# Look at test metadata first
cat debug-artifacts-*/test-metadata.txt

# Check operator decision-making. The per-reconcile "Reconciling <mode>" lines are
# debug-level; they are present here because the e2e suite deploys the operator with
# LOG_LEVEL=debug (see e2e_suite_test.go). A production operator at the chart's default
# `logLevel: info` does not emit them.
grep "Reconciling cluster mode" debug-artifacts-*/operator-logs.txt

# See final CR status
cat debug-artifacts-*/cr-*.yaml

# Check Redis cluster state
grep "cluster_state" debug-artifacts-*/pod-*-redis.log
```

## Integration Details

### Automatic Collection

The collection happens automatically in `e2e_suite_test.go`:

```go
var _ = AfterEach(func() {
    if CurrentSpecReport().Failed() && debugOnFailure {
        namespace, crName, chaosPod := extractTestContext()
        CollectDebugArtifacts(namespace, crName, chaosPod)
    }
})
```

### Context Detection

All tests share a single namespace (default: `default`, override with `TEST_NAMESPACE`).
CR name and chaos pod name are extracted from spec labels if set.

## Troubleshooting

### No artifacts collected

- Verify `DEBUG_ON_FAILURE=true` is set
- Check that the test actually failed (not skipped)
- Ensure you have write permissions in the current directory

### Missing logs

**Empty operator logs**:
- Check GinkgoWriter output for error messages
- Operator pod might have restarted (check previous logs manually)
- Time filter might have issues (fallback to `--tail 1000` should work)
- Verify operator namespace is correct: `kubectl get pods -n littlered-system`

**Missing pod logs**:
- Pods may have been deleted before collection (timing issue)
- Check if namespace still exists: `kubectl get ns <namespace>`

**Missing chaos logs**:
- Chaos pod may have completed and been removed
- Check pod status: `kubectl get pods -n <namespace> | grep chaos`

### Permissions

The debug artifact collection uses `kubectl` commands, so ensure your kubeconfig has appropriate permissions to:

- Get pods and logs
- Read custom resources
- List events and services

## Cleaning Up

Debug artifact directories are NOT automatically deleted. Clean them up manually:

```bash
# Remove old artifacts
rm -rf debug-artifacts-*

# Keep only recent artifacts
ls -t debug-artifacts-* | tail -n +6 | xargs rm -rf  # Keep 5 most recent
```

## Future Enhancements

Possible improvements:

- Automatic retention policy (keep last N)
- Tarball creation for easier sharing
- Upload to S3/GCS for CI integration
- Comparison tool for before/after states
- Integration with test reporting frameworks
