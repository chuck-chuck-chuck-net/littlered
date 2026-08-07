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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	"github.com/littlered-operator/littlered-operator/internal/cli/multisite"
	"github.com/littlered-operator/littlered-operator/internal/multisitegather"
)

const (
	// gatherJobTTLSeconds is the server-side GC window (ttlSecondsAfterFinished)
	// for the throwaway tier-2 gather Job: even if lrctl dies before deleting it,
	// the API server reaps it. lrctl also deletes it proactively on the happy path.
	gatherJobTTLSeconds = 120
	// gatherJobWaitTimeout bounds how long lrctl waits for the Job to finish. It
	// exceeds the operator's in-Job gather budget (gatherOneShotBudget, 25s) plus
	// image pull / scheduling slack.
	gatherJobWaitTimeout = 90 * time.Second
	// gatherJobPollInterval is the Job-status poll cadence.
	gatherJobPollInterval = 2 * time.Second
	// jobNameLabel is the label the Job controller stamps on its pods.
	jobNameLabel = "batch.kubernetes.io/job-name"
	// legacyJobNameLabel is the pre-1.27 equivalent, still set for compatibility.
	legacyJobNameLabel = "job-name"
)

// currentTierFlags snapshots the tier-selecting persistent flags into the pure
// TierFlags. --site-context implies tier 3 (direct).
func currentTierFlags() multisite.TierFlags {
	return multisite.TierFlags{
		FromStatus:     fromStatus,
		Direct:         direct,
		SiteContextSet: siteContext != "",
	}
}

// discoverOperatorInfo reads the operator Deployment on the hub (well-known
// littlered-system/littlered, overridable) to learn the SA and image the tier-2
// gather Job must run as. Both come straight off the running operator, so the
// Job inherits its exact RBAC + Secret access and reuses the image already
// cached on hub nodes.
func discoverOperatorInfo(
	ctx context.Context, k8sClient client.Client, opNS, opDeploy string,
) (multisite.OperatorInfo, error) {
	deploy := &appsv1.Deployment{}
	key := client.ObjectKey{Namespace: opNS, Name: opDeploy}
	if err := k8sClient.Get(ctx, key, deploy); err != nil {
		return multisite.OperatorInfo{}, fmt.Errorf(
			"discovering operator Deployment %s/%s (override with --operator-namespace/--operator-deployment): %w",
			opNS, opDeploy, err)
	}
	return multisite.ExtractOperatorInfo(deploy)
}

// randSuffix returns a short DNS-safe token making each gather Job name unique.
func randSuffix() string {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// gatherViaHubJob is the tier-2 transport: it runs the operator's own `--gather`
// mode as a throwaway Kubernetes Job on the hub and returns the site-attributed
// ground truth the Job printed. The Job — not a kubectl-exec into the operator —
// is resource-isolated and works even when the operator pod is unhealthy, which
// is a prime debug case. lrctl needs only create-jobs + read-pod-logs in the
// operator namespace; the JOB runs as the operator SA and does the site access.
func gatherViaHubJob(
	ctx context.Context, k8sClient client.Client, coreClient *kubernetes.Clientset, lr *littleredv1alpha1.LittleRed,
	opNS, opDeploy string,
) (*multisitegather.Result, error) {
	info, err := discoverOperatorInfo(ctx, k8sClient, opNS, opDeploy)
	if err != nil {
		return nil, err
	}

	job := multisite.BuildGatherJob(multisite.GatherJobParams{
		OperatorNamespace:  opNS,
		ServiceAccountName: info.ServiceAccountName,
		Image:              info.Image,
		InstanceNamespace:  lr.Namespace,
		InstanceName:       lr.Name,
		TTLSeconds:         gatherJobTTLSeconds,
		NameSuffix:         randSuffix(),
	})

	if err := k8sClient.Create(ctx, job); err != nil {
		return nil, fmt.Errorf("creating gather Job %s/%s (needs create-jobs RBAC in the operator namespace): %w",
			job.Namespace, job.Name, err)
	}
	// Proactive cleanup on the happy path; ttlSecondsAfterFinished is the backstop
	// if lrctl dies before this runs.
	defer func() {
		bg := metav1.DeletePropagationBackground
		_ = k8sClient.Delete(context.Background(), job, &client.DeleteOptions{PropagationPolicy: &bg})
	}()

	fmt.Printf("Running gather Job %s/%s (image %s, as SA %s)...\n",
		job.Namespace, job.Name, info.Image, info.ServiceAccountName)

	finished, err := waitForJob(ctx, k8sClient, job.Namespace, job.Name)
	if err != nil {
		return nil, err
	}

	logText, podName, err := readJobPodLogs(ctx, k8sClient, coreClient, job.Namespace, job.Name)
	if err != nil {
		return nil, fmt.Errorf("reading gather Job pod logs (needs pods/log RBAC): %w", err)
	}
	if !finished {
		return nil, fmt.Errorf("gather Job %s did not finish within %s; last pod (%s) log:\n%s",
			job.Name, gatherJobWaitTimeout, podName, logText)
	}

	resultJSON, err := multisite.ExtractResultJSON(logText)
	if err != nil {
		return nil, fmt.Errorf("%w; Job pod %s log was:\n%s", err, podName, logText)
	}
	var result multisitegather.Result
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		return nil, fmt.Errorf("parsing gather Result JSON: %w", err)
	}
	return &result, nil
}

// waitForJob polls until the Job succeeds or fails, or the wait budget expires.
// finished is true iff the Job reached a terminal Succeeded state.
func waitForJob(ctx context.Context, k8sClient client.Client, namespace, name string) (bool, error) {
	deadline := time.Now().Add(gatherJobWaitTimeout)
	for {
		job := &batchv1.Job{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, job); err != nil {
			return false, fmt.Errorf("polling gather Job: %w", err)
		}
		if job.Status.Succeeded > 0 {
			return true, nil
		}
		if job.Status.Failed > 0 {
			return false, nil
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(gatherJobPollInterval)
	}
}

// readJobPodLogs finds the Job's pod and returns its (stdout+stderr) log.
func readJobPodLogs(
	ctx context.Context, k8sClient client.Client, coreClient *kubernetes.Clientset, namespace, jobName string,
) (string, string, error) {
	pod, err := findJobPod(ctx, k8sClient, namespace, jobName)
	if err != nil {
		return "", "", err
	}
	req := coreClient.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{Container: "gather"})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", pod, err
	}
	defer func() { _ = stream.Close() }()
	data, err := io.ReadAll(stream)
	if err != nil {
		return "", pod, err
	}
	return string(data), pod, nil
}

// findJobPod locates the (single) pod the Job created, trying the current and
// legacy job-name labels.
func findJobPod(ctx context.Context, k8sClient client.Client, namespace, jobName string) (string, error) {
	for _, lbl := range []string{jobNameLabel, legacyJobNameLabel} {
		pods := &corev1.PodList{}
		if err := k8sClient.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{lbl: jobName}); err != nil {
			continue
		}
		if len(pods.Items) > 0 {
			return pods.Items[0].Name, nil
		}
	}
	return "", fmt.Errorf("no pod found for gather Job %s/%s", namespace, jobName)
}
