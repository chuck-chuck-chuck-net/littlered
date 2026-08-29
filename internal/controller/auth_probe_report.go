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

package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
	redisclient "github.com/littlered-operator/littlered-operator/internal/redis"
)

// Reasons for the OperatorCannotAuthenticate condition (LR-051).
const (
	// reasonCredentialRejected: at least one pod refused our credential.
	reasonCredentialRejected = "CredentialRejected"
	// reasonCredentialAccepted: no pod is refusing it. The condition is set False
	// rather than removed, so the transition is visible in the object's history.
	reasonCredentialAccepted = "CredentialAccepted"

	// reasonRefusedDataUnverified is the LeaderlessRecovery / GhostMasterRecovery
	// reason when a recovery is refused because a pod could not be proven empty.
	// Deliberately NOT reasonRefusedDataPresent: that one means "we can see the
	// holders and there are too many", this one means "we cannot see them at all",
	// and they want opposite remedies.
	reasonRefusedDataUnverified = "RefusedDataUnverified"
)

// unverifiedPodSummary renders the vetoing pods for a condition message: the pod
// names plus the distinct server replies, which is what tells an owner whether the
// Secret moved under the fleet (WRONGPASS) or the operator lost a password the pods
// still enforce (NOAUTH).
func unverifiedPodSummary(nodes []*redisclient.RedisNodeState) string {
	if len(nodes) == 0 {
		return "a pod could not be proven empty"
	}
	names := make([]string, 0, len(nodes))
	replies := map[string]bool{}
	for _, n := range nodes {
		names = append(names, n.PodName)
		if n.ProbeError != "" {
			replies[n.ProbeError] = true
		}
	}
	distinct := make([]string, 0, len(replies))
	for reply := range replies {
		distinct = append(distinct, reply)
	}
	sort.Strings(distinct)

	s := fmt.Sprintf("%d Redis pod(s) refused the operator's credential (%s)",
		len(names), strings.Join(names, ", "))
	if len(distinct) > 0 {
		s += ", server said: " + strings.Join(distinct, "; ")
	}
	return s
}

// authReport is the pure rendering of "which of our pods refuse our credential".
//
// It is a rendering and not a decision: the DECISION (refuse to discard data) lives
// in unprovablyEmptyVeto, on the state itself, so no planner depends on this having
// been computed. This exists because the state was previously undiagnosable — a
// rotated Secret silently unmanages an instance forever, with nothing anywhere
// connecting the edit to the symptom (auth design §3.5a Path B).
type authReport struct {
	Status  metav1.ConditionStatus
	Reason  string
	Message string
	// Pods is every pod that refused us, sorted, for the log line.
	Pods []string
}

// authFailure is one pod that refused us, reduced to what the report needs. The
// report is deliberately built from THIS shape rather than from a mode's state
// type, so every mode renders the same sentence from one implementation — LR-045's
// lesson, where a required behaviour was threaded through the generic path and
// never carried to the one that actually executes.
type authFailure struct {
	PodName string
	Reply   string
}

// replicationAuthFailures collects the refusals from a sentinel-/failover-mode
// gather (Redis pods and, in sentinel mode, Sentinel pods).
func replicationAuthFailures(state *redisclient.ReplicationState) []authFailure {
	if state == nil {
		return nil
	}
	var out []authFailure
	for _, rn := range state.AuthFailedRedisNodes() {
		out = append(out, authFailure{PodName: rn.PodName, Reply: rn.ProbeError})
	}
	for _, sn := range state.AuthFailedSentinelNodes() {
		out = append(out, authFailure{PodName: sn.PodName, Reply: sn.ProbeError})
	}
	return out
}

// clusterAuthFailures is the cluster-mode collector. No cluster decision keys on
// it; it exists so an owner who rotates a Secret on a cluster instance gets the
// same named diagnosis as on a sentinel one instead of the same silence.
func clusterAuthFailures(gt *redisclient.ClusterGroundTruth) []authFailure {
	if gt == nil {
		return nil
	}
	var out []authFailure
	for _, n := range gt.AuthFailedNodes() {
		out = append(out, authFailure{PodName: n.PodName, Reply: n.ProbeError})
	}
	return out
}

// planAuthReport renders the report. Pure: it reads only what it is given.
//
// The message names the pods AND what each distinct server reply was, because the
// reply is the diagnosis and the two point at opposite remedies: WRONGPASS means
// the Secret moved and the pods did not (a Secret edit rolls nothing), NOAUTH means
// the operator lost a password the pods still enforce.
func planAuthReport(failures []authFailure) authReport {
	pods := make([]string, 0, len(failures))
	replies := map[string]bool{}
	for _, f := range failures {
		pods = append(pods, f.PodName)
		if f.Reply != "" {
			replies[f.Reply] = true
		}
	}
	if len(pods) == 0 {
		return authReport{Status: metav1.ConditionFalse, Reason: reasonCredentialAccepted,
			Message: "No pod is refusing the operator's credential."}
	}

	distinct := make([]string, 0, len(replies))
	for reply := range replies {
		distinct = append(distinct, reply)
	}
	sort.Strings(distinct)

	msg := fmt.Sprintf("The operator cannot authenticate to %d of this instance's own pods (%s). "+
		"The server said: %s. These pods ANSWERED — a live server is running there — so their "+
		"keyspace is unknown, not empty, and the operator is refusing every action that would "+
		"discard data (leaderless reseed, ghost-master election, capture quarantine) until the "+
		"credential is fixed. Most likely the password in the Secret was changed under a running "+
		"instance: editing a Secret rolls no pods, so they keep enforcing the previous password "+
		"indefinitely.",
		len(pods), strings.Join(pods, ", "), strings.Join(distinct, "; "))

	return authReport{
		Status: metav1.ConditionTrue, Reason: reasonCredentialRejected, Message: msg, Pods: pods,
	}
}

// reportOperatorAuth persists the report and emits ONE Warning per transition, never
// per reconcile — the LR-042 lesson: this runs on every sentinel pass, and a status
// write plus an event every 2s for an unchanged verdict is exactly the churn that
// entry removed.
func (r *LittleRedReconciler) reportOperatorAuth(
	ctx context.Context, lr *littleredv1alpha1.LittleRed, failures []authFailure,
) error {
	rep := planAuthReport(failures)

	prev := meta.FindStatusCondition(lr.Status.Conditions, littleredv1alpha1.ConditionOperatorCannotAuthenticate)
	// Never introduce the condition just to say "all fine" — an instance that has
	// never had a credential problem should not carry the condition at all.
	if prev == nil && rep.Status == metav1.ConditionFalse {
		return nil
	}
	if prev != nil && prev.Status == rep.Status && prev.Reason == rep.Reason && prev.Message == rep.Message {
		return nil
	}
	transitioned := prev == nil || prev.Status != rep.Status

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &littleredv1alpha1.LittleRed{}
		if err := r.Get(ctx, types.NamespacedName{Name: lr.Name, Namespace: lr.Namespace}, latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type: littleredv1alpha1.ConditionOperatorCannotAuthenticate, Status: rep.Status,
			Reason: rep.Reason, Message: rep.Message,
		})
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		// Mirror the persisted conditions back onto the in-memory object.
		// updateSentinelStatus runs later in the SAME pass and writes the whole status
		// from this object, so without the mirror it silently reverts what was just
		// written — the LR-044 defect, which must not be reintroduced.
		lr.Status.Conditions = latest.Status.Conditions
		return nil
	}); err != nil {
		return err
	}

	if transitioned {
		if rep.Status == metav1.ConditionTrue {
			r.getLogger(ctx, lr, LogCategoryAudit).Info(
				"The operator cannot authenticate to its own pods; refusing every action that discards data",
				"pods", strings.Join(rep.Pods, ","))
			r.event(lr, corev1.EventTypeWarning, rep.Reason, rep.Message)
		} else {
			r.getLogger(ctx, lr, LogCategoryAudit).Info(
				"The operator can authenticate to its pods again")
		}
	}
	return nil
}
