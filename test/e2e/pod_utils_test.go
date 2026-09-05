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

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive // dot-import is the Ginkgo/Gomega convention in tests
	. "github.com/onsi/gomega"    //nolint:revive // dot-import is the Ginkgo/Gomega convention in tests

	"github.com/littlered-operator/littlered-operator/test/utils"
)

// =============================================================================
// Pre-delete log snapshots (instant capture, no dying logs)
// =============================================================================

// preDeleteLogs stores logs captured from pods just before they are deleted.
// Keyed by "podName/containerName". Cleared before each test.
var (
	preDeleteLogsMu sync.Mutex
	preDeleteLogs   = map[string][]byte{}
)

// resetPreDeleteLogs clears the pre-deletion log buffer. Called in BeforeEach.
func resetPreDeleteLogs() {
	preDeleteLogsMu.Lock()
	defer preDeleteLogsMu.Unlock()
	preDeleteLogs = map[string][]byte{}
}

// capturePreDeleteLogs saves the current logs of all containers in a pod so they
// can be written to debug artifacts even after the pod has been replaced.
func capturePreDeleteLogs(namespace, podName string) {
	cmd := exec.Command("kubectl", "get", "pod", podName,
		"-n", namespace,
		"-o", "jsonpath={.spec.containers[*].name}")
	out, err := utils.Run(cmd)
	if err != nil || out == "" {
		return
	}

	preDeleteLogsMu.Lock()
	defer preDeleteLogsMu.Unlock()

	for _, container := range strings.Fields(out) {
		logCmd := exec.Command("kubectl", "logs", podName,
			"-n", namespace,
			"-c", container,
			"--timestamps",
			"--tail", "2000")
		logs, err := logCmd.CombinedOutput()
		if err != nil {
			continue
		}
		preDeleteLogs[podName+"/"+container] = logs
		fmt.Printf("[Utility] Captured %d bytes pre-delete snapshot for %s/%s\n", len(logs), podName, container)
	}
}

// =============================================================================
// Streaming logs — follow a pod through its death (catches preStop output)
// =============================================================================

// streamEntry holds a running "kubectl logs -f" process and its output file.
type streamEntry struct {
	cmd      *exec.Cmd
	filePath string
}

var (
	streamLogsMu sync.Mutex
	streamLogs   = map[string]*streamEntry{} // key: "podName/containerName"

	// e2eTmpDir is created once for the whole suite and cleaned up in AfterSuite.
	e2eTmpDir string
)

// initE2ETmpDir creates the suite-wide temporary directory for streaming logs.
// Called from BeforeSuite.
func initE2ETmpDir() {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("tmp-e2e-logs-%d", time.Now().Unix()))
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Printf("[Utility] Failed to create tmp log dir %s: %v\n", dir, err)
		return
	}
	e2eTmpDir = dir
	fmt.Printf("[Utility] Streaming log tmp dir: %s\n", e2eTmpDir)
}

// cleanupE2ETmpDir kills any still-running log streamers and removes the tmp dir.
// Called from AfterSuite.
func cleanupE2ETmpDir() {
	stopAllStreamers()
	if e2eTmpDir != "" {
		_ = os.RemoveAll(e2eTmpDir)
		fmt.Printf("[Utility] Removed tmp log dir %s\n", e2eTmpDir)
	}
}

// stopAllStreamers kills every active kubectl logs -f process and clears the map.
func stopAllStreamers() {
	streamLogsMu.Lock()
	defer streamLogsMu.Unlock()
	for key, entry := range streamLogs {
		if entry.cmd != nil && entry.cmd.Process != nil {
			_ = entry.cmd.Process.Kill()
		}
		fmt.Printf("[Utility] Stopped log streamer for %s\n", key)
	}
	streamLogs = map[string]*streamEntry{}
}

// resetStreamingLogs stops streamers from the previous test and clears the map.
// Called from BeforeEach so each test starts with a clean slate.
func resetStreamingLogs() {
	stopAllStreamers()
}

// startStreamingLogs spawns one "kubectl logs -f" child process per container.
// Each process writes to a file in e2eTmpDir and runs until the container exits.
// Must be called BEFORE kubectl delete so we don't miss the last log lines.
func startStreamingLogs(namespace, podName string) {
	if e2eTmpDir == "" {
		return
	}

	cmd := exec.Command("kubectl", "get", "pod", podName,
		"-n", namespace,
		"-o", "jsonpath={.spec.containers[*].name}")
	out, err := utils.Run(cmd)
	if err != nil || out == "" {
		fmt.Printf("[Utility] Could not list containers for streaming %s/%s: %v\n", namespace, podName, err)
		return
	}

	streamLogsMu.Lock()
	defer streamLogsMu.Unlock()

	for _, container := range strings.Fields(out) {
		key := podName + "/" + container
		// Don't start a second streamer for the same pod/container.
		if _, exists := streamLogs[key]; exists {
			continue
		}

		filePath := filepath.Join(e2eTmpDir, fmt.Sprintf("pod-%s-%s-streaming.log", podName, container))
		f, err := os.Create(filePath)
		if err != nil {
			fmt.Printf("[Utility] Failed to create streaming log file %s: %v\n", filePath, err)
			continue
		}

		logCmd := exec.Command("kubectl", "logs", "-f", podName,
			"-n", namespace,
			"-c", container,
			"--timestamps")
		logCmd.Stdout = f
		logCmd.Stderr = f

		if err := logCmd.Start(); err != nil {
			fmt.Printf("[Utility] Failed to start log streamer for %s/%s: %v\n", podName, container, err)
			_ = f.Close()
			continue
		}

		entry := &streamEntry{cmd: logCmd, filePath: filePath}
		streamLogs[key] = entry
		fmt.Printf("[Utility] Started log streamer for %s/%s → %s\n", podName, container, filePath)

		// Wait for the process to finish in a goroutine so the file gets flushed and closed.
		go func(e *streamEntry, k string) {
			_ = e.cmd.Wait()
			_ = f.Sync()
			_ = f.Close()
			fmt.Printf("[Utility] Log streamer finished for %s\n", k)
		}(entry, key)
	}
}

// copyStreamingLogsToDir copies all streaming log files currently tracked to dst.
// It gives streamers a short grace period to flush their last bytes.
func copyStreamingLogsToDir(dst string) {
	// Give streaming processes a moment to flush after the pod terminated.
	time.Sleep(2 * time.Second)

	streamLogsMu.Lock()
	// Snapshot the file paths; don't hold the lock while doing file I/O.
	paths := make([]string, 0, len(streamLogs))
	for _, entry := range streamLogs {
		paths = append(paths, entry.filePath)
	}
	streamLogsMu.Unlock()

	for _, src := range paths {
		dstFile := filepath.Join(dst, filepath.Base(src))
		if err := copyFile(src, dstFile); err != nil {
			fmt.Printf("[Utility] Failed to copy streaming log %s → %s: %v\n", src, dstFile, err)
		} else {
			info, _ := os.Stat(dstFile)
			if info != nil {
				fmt.Printf("[Utility] Copied streaming log → %s (%d bytes)\n", dstFile, info.Size())
			}
		}
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

// =============================================================================
// Pod deletion helpers
// =============================================================================

// restartMode defines how a pod deletion is performed.
type restartMode struct {
	Name     string
	Graceful bool
}

// restartModes contains the two deletion modes used in dual-mode tests.
//
// NAMING CAVEAT (LR-038): "crash" here is a `--grace-period=0 --force` delete,
// which is NOT a crash — the pod OBJECT is removed from the API while the
// container still terminates through the kubelet's normal path, hooks included.
// The genuine crash is kill-9 of the container's PID 1, where no hook can run.
// Conflating the two hid a data-loss bug for a whole release: the only kill-9
// coverage killed the BOOTSTRAP master, the one master the failover-mode start
// gate happened to protect, so a kill-9 of a PROMOTED master silently destroyed
// 352 of 1145 acknowledged writes.
//
// The chaos tiers use chaosDisruptions below, which names all three shapes
// correctly. restartModes is kept for the four non-chaos dual-mode specs: adding
// a third shape there would grow the suite everywhere, and renaming its variants
// would rename six specs plus every FOCUS string that selects them. Tracked as a
// separate sweep.
var restartModes = []restartMode{
	{"graceful", true},
	{"crash", false},
}

// chaosDisruption is one way a master can be lost, named for what it actually
// does to the PROCESS — which is the distinction that decides whether
// acknowledged writes survive (LR-038).
type chaosDisruption struct {
	Name string
	// Planned marks a disruption the operator/Sentinel is told about in advance
	// (a deletionTimestamp), so a clean handover is possible and its cost is
	// assertable. An abrupt loss has no handover whose cost could be measured.
	Planned bool
	Apply   func(namespace, podName string)
}

// chaosDisruptions is the shared three-shape set for the mode-comparison chaos
// tiers. Shared deliberately: the failover tier exists to be measured against the
// sentinel tier on ONE yardstick, and a per-tier copy is exactly how yardsticks
// drift.
//
//	graceful      pod deleted normally. SIGTERM, preStop runs, handover happens.
//	force-delete  `--grace-period=0 --force`. The pod OBJECT vanishes from the API
//	              but the container still terminates through the kubelet's path.
//	kill-9        the container's PID 1 is killed from outside its PID namespace.
//	              NO hook can run, the process dies instantly, and the pod and its
//	              IP survive — the case both modes need a restart guard for, and
//	              where they solve it differently: sentinel mode yields on
//	              Sentinel's stored run-id (identity, kept by a continuous external
//	              observer), failover mode on an operator-stamped start
//	              authorization.
var chaosDisruptions = []chaosDisruption{
	{
		Name:    "graceful",
		Planned: true,
		Apply: func(namespace, podName string) {
			_, err := deletePodMode(namespace, podName, true)
			Expect(err).NotTo(HaveOccurred())
		},
	},
	{
		Name: "force-delete",
		Apply: func(namespace, podName string) {
			_, err := deletePodMode(namespace, podName, false)
			Expect(err).NotTo(HaveOccurred())
		},
	},
	{
		Name:  "kill-9",
		Apply: killPodProcess,
	},
}

// chaosWriteAvailabilityFloor is the per-cell regression bar for the mode-comparison
// chaos tiers, and it is DERIVED from a measured distribution rather than fitted to a
// run: LR-038's addendum 5 ran both modes x three shapes ten consecutive times (60 chaos
// runs, t3e, operator 803eb26), and each bar sits a few points below that cell's measured
// MINIMUM, rounded to a round number.
//
//	cell                     min / median / max        bar
//	failover graceful        95.83 / 96.42 / 96.91     0.90
//	failover force-delete    94.66 / 98.46 / 98.92     0.90
//	failover kill-9          85.13 / 92.19 / 95.73     0.80
//	sentinel graceful        94.75 / 95.29 / 96.50     0.90
//	sentinel force-delete    96.83 / 97.58 / 98.33     0.90
//	sentinel kill-9          43.32 / 54.92 / 73.89     0.40
//
// WHY A SINGLE BAR WAS WRONG IN BOTH DIRECTIONS. Every cell used to assert `> 0.40`.
// For the five cells measuring 85-99% that is uselessly loose — a mode could lose a
// third of its write availability and still pass. For sentinel kill-9 it is nearly
// TIGHT: 0.40 sits ~3pp under a measured minimum of 43.32, so a real regression there
// would read as a flaky test rather than as what it is.
//
// SENTINEL KILL-9 KEEPS 0.40 DELIBERATELY, and its number is the one to read carefully.
// It is low because the mode's restart guard suppresses Redis on the killed master and
// waits for Sentinel to reach SDOWN, elect and be observed — a design cost, not a defect,
// and it buys zero data loss (that column measured 0 MISSING in all ten passes). The cell
// is also BIMODAL rather than noisy (~314 vs ~600+ failed writes), so a single sample of
// it means nothing. If this bar ever trips, read it as "the yield's cost grew", not as
// flakiness — and check the durability assertion first, because that is the property that
// matters.
//
// These are regression detectors, not SLOs. A cell that drifts below its bar has changed
// behaviour; what "acceptable availability" is for a deployment is the deployment's call.
func chaosWriteAvailabilityFloor(mode, shape string) float64 {
	if mode == "sentinel" && shape == "kill-9" {
		return 0.40
	}
	if shape == "kill-9" {
		return 0.80
	}
	return 0.90
}

// deletePod deletes a pod in the given namespace.
// If NON_GRACEFUL_RESTART environment variable is set to "true", it performs a
// non-graceful deletion (--grace-period=0 --force). Otherwise graceful.
func deletePod(namespace, podName string) (string, error) {
	graceful := os.Getenv("NON_GRACEFUL_RESTART") != "true"
	return deletePodMode(namespace, podName, graceful)
}

// deletePodMode deletes a pod with explicit control over graceful vs crash mode.
// It captures a log snapshot AND starts a streaming follower before deletion so
// the full dying sequence (including preStop output) is available in artifacts.
func deletePodMode(namespace, podName string, graceful bool) (string, error) {
	// 1. Instant snapshot — catches all lines up to this moment.
	capturePreDeleteLogs(namespace, podName)
	// 2. Start streaming — catches lines produced during Terminating (preStop, SIGTERM).
	startStreamingLogs(namespace, podName)

	args := []string{"delete", "pod", podName, "-n", namespace}
	if !graceful {
		fmt.Printf("[Utility] Performing NON-GRACEFUL deletion of pod %s/%s\n", namespace, podName)
		args = append(args, "--grace-period=0", "--force")
	} else {
		fmt.Printf("[Utility] Performing GRACEFUL deletion of pod %s/%s\n", namespace, podName)
	}

	cmd := exec.Command("kubectl", args...)
	return utils.Run(cmd)
}

// killPodProcess kills the redis container's init process, triggering a
// container restart without deleting the pod (pod IP is preserved).
//
// Why not "kubectl exec -- kill -9 1"?
//
//  1. "kill" is absent from minimal Redis/Valkey images.
//  2. Even when present: Linux unconditionally blocks SIGKILL sent to PID 1
//     from within the same PID namespace. An exec'd redis-server IS PID 1,
//     so the kernel silently drops the signal.
//
// Solution — escape the container's PID namespace:
//
//  1. Spin up a one-shot busybox pod on the same node with hostPID:true.
//     hostPID gives it a view of every process on the node via /proc.
//
//  2. Scan /proc/*/cgroup for entries containing the container ID. The
//     container runtime embeds the container ID in every cgroup path.
//
//  3. Among matching host PIDs, find the one whose /proc/<pid>/status
//     "NSpid" field ends in "1" — that is PID 1 inside the container's
//     PID namespace, expressed as a host-namespace PID.
//
//  4. kill -9 that host PID. The signal comes from outside the container's
//     PID namespace, so the kernel's PID-1 immunity rule does not apply.
func killPodProcess(namespace, podName string) {
	capturePreDeleteLogs(namespace, podName)
	startStreamingLogs(namespace, podName)

	// ── resolve node and container ID ──────────────────────────────────────

	cmd := exec.Command("kubectl", "get", "pod", podName,
		"-n", namespace, "-o", "jsonpath={.spec.nodeName}")
	nodeNameRaw, err := utils.Run(cmd)
	if err != nil || strings.TrimSpace(nodeNameRaw) == "" {
		fmt.Printf("[Utility] killPodProcess: cannot get node for %s/%s: %v\n", namespace, podName, err)
		return
	}
	nodeName := strings.TrimSpace(nodeNameRaw)

	// Target the redis container by name — pods may have sidecars (e.g. metrics exporter).
	cmd = exec.Command("kubectl", "get", "pod", podName,
		"-n", namespace, "-o", `jsonpath={.status.containerStatuses[?(@.name=="redis")].containerID}`)
	containerIDRaw, err := utils.Run(cmd)
	if err != nil || strings.TrimSpace(containerIDRaw) == "" {
		fmt.Printf("[Utility] killPodProcess: cannot get containerID for %s/%s: %v\n", namespace, podName, err)
		return
	}
	// Strip runtime prefix, e.g. "containerd://" → bare 64-char hash.
	containerID := strings.TrimSpace(containerIDRaw)
	if idx := strings.Index(containerID, "://"); idx >= 0 {
		containerID = containerID[idx+3:]
	}
	// 12 hex chars (48 bits) is more than enough for a unique grep substring match.
	shortID := containerID
	if len(shortID) > 12 {
		shortID = shortID[:12]
	}
	fmt.Printf("[Utility] killPodProcess: hunting host PID for container %s on node %s\n", shortID, nodeName)

	// ── build the kill script ───────────────────────────────────────────────

	script := "ID=" + shortID + "\n" +
		"for f in $(grep -rl $ID /proc/[0-9]*/cgroup 2>/dev/null); do\n" +
		"  pid=$(echo $f | cut -d/ -f3)\n" +
		"  nspid=$(grep NSpid /proc/$pid/status 2>/dev/null | awk '{print $NF}')\n" +
		"  [ \"$nspid\" = \"1\" ] || continue\n" +
		"  echo \"Killing host PID $pid (container $ID)\"\n" +
		"  kill -9 $pid\n" +
		"  exit 0\n" +
		"done\n" +
		"echo \"Container init not found for $ID\" >&2; exit 1\n"

	// ── launch a privileged hostPID pod on the target node ─────────────────
	//
	// spec.nodeName bypasses the scheduler for guaranteed placement.
	// json.Marshal encodes the script with proper escaping.

	helperPodName := "kill-proc-" + shortID
	scriptJSON, err := json.Marshal(script)
	if err != nil {
		fmt.Printf("[Utility] killPodProcess: script marshal failed: %v\n", err)
		return
	}
	podManifest := fmt.Sprintf(`{
		"apiVersion":"v1","kind":"Pod",
		"metadata":{"name":%q,"namespace":%q},
		"spec":{
			"hostPID":true,
			"nodeName":%q,
			"tolerations":[{"operator":"Exists"}],
			"restartPolicy":"Never",
			"containers":[{
				"name":"kill-proc",
				"image":"busybox",
				"command":["sh","-c",%s],
				"securityContext":{"privileged":true}
			}]
		}
	}`, helperPodName, namespace, nodeName, string(scriptJSON))

	cmd = exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(podManifest)
	if _, err := utils.Run(cmd); err != nil {
		// Fail loudly here: a swallowed error surfaces later as a confusing
		// "restart count 0 to be > 0" timeout that hides the real cause.
		// The most common cause is Pod Security Standards forbidding the
		// hostPID/privileged helper — the test namespace must be labeled
		// pod-security.kubernetes.io/enforce=privileged (see BeforeSuite).
		Fail(fmt.Sprintf("killPodProcess: failed to create privileged hostPID helper pod %q "+
			"(the SIGKILL never happened). If this is a PodSecurity 'Forbidden' error, the "+
			"namespace %q must allow hostPID+privileged. Underlying error: %v",
			helperPodName, namespace, err))
	}
	defer func() {
		cmd := exec.Command("kubectl", "delete", "pod", helperPodName,
			"-n", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)
	}()

	// ── wait for the helper pod to finish (up to 60 s) ─────────────────────

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		cmd = exec.Command("kubectl", "get", "pod", helperPodName,
			"-n", namespace, "-o", "jsonpath={.status.phase}")
		phase, _ := utils.Run(cmd)
		if p := strings.TrimSpace(phase); p == "Succeeded" || p == "Failed" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	// Surface the helper pod output so it appears in test logs and artifacts.
	cmd = exec.Command("kubectl", "logs", helperPodName, "-n", namespace)
	output, _ := utils.Run(cmd)
	fmt.Printf("[Utility] killPodProcess: %s\n", strings.TrimSpace(output))
}

// deletePodsWithLabel deletes all pods matching the label selector in the given
// namespace. If NON_GRACEFUL_RESTART=true it force-deletes; otherwise graceful.
func deletePodsWithLabel(namespace, labelSelector string) (string, error) {
	graceful := os.Getenv("NON_GRACEFUL_RESTART") != "true"
	return deletePodsWithLabelMode(namespace, labelSelector, graceful)
}

// deletePodsWithLabelMode deletes all pods matching the label selector with explicit
// control over graceful vs force (--grace-period=0 --force). Force deletion drops the
// pod objects from the API immediately, so a caller that needs a killed pod's IP to
// leave the pod list at once (rather than lingering ~30s in Terminating) can request it.
func deletePodsWithLabelMode(namespace, labelSelector string, graceful bool) (string, error) {
	args := []string{"delete", "pods", "-n", namespace, "-l", labelSelector}

	if !graceful {
		fmt.Printf("[Utility] Performing NON-GRACEFUL deletion of pods with label %s in %s\n", labelSelector, namespace)
		args = append(args, "--grace-period=0", "--force")
	} else {
		fmt.Printf("[Utility] Performing GRACEFUL deletion of pods with label %s in %s\n", labelSelector, namespace)
	}

	cmd := exec.Command("kubectl", args...)
	return utils.Run(cmd)
}

// podUID returns a pod's metadata.uid (empty string on error). Used to assert that a
// failover produced a NEW pod instance without depending on the pod name — a StatefulSet
// reuses the name of a deleted pod, and the operator's data-based election can legitimately
// re-elect that recreated pod, so a name comparison is racy while a UID comparison is not.
func podUID(namespace, pod string) string {
	out, err := utils.Run(exec.Command("kubectl", "get", "pod", pod,
		"-n", namespace, "-o", "jsonpath={.metadata.uid}"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}
