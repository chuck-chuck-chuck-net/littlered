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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// reconcileSentinelMasterNameCondition sets or clears the SentinelMasterNameUnscoped
// warning condition.
//
// spec.sentinel.masterName is Required by the CRD, so a newly created instance always
// carries one. This condition exists for the instances validation cannot reach: those
// created before the field existed. CRD validation ratcheting excuses a required-field
// violation at any schema location whose value is unchanged, so such an instance keeps
// reconciling — and keeps being editable — while silently falling back to the shared
// legacy master name. This condition is the only signal it will ever get.
//
// Warning, never a refusal (the opt-in-not-block norm, as ConditionReducedSiteLossResilience):
// the instance is serving, and refusing would turn a latent exposure into a certain outage.
// A one-shot Warning event fires on the transition into the warning state; the condition
// itself is idempotent.
//
// Setting the field to the legacy value EXPLICITLY is not warned about. That is a decision
// — a legacy client may hardcode the name with no way to parameterise it — and the operator
// does not second-guess decisions. Only the absence of one is reported.
func (r *LittleRedReconciler) reconcileSentinelMasterNameCondition(lr *littleredv1alpha1.LittleRed) {
	if !lr.SentinelMasterNameUnscoped() {
		meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
			Type:   littleredv1alpha1.ConditionSentinelMasterNameUnscoped,
			Status: metav1.ConditionFalse,
			Reason: reasonSentinelMasterNameScoped,
			Message: fmt.Sprintf("Sentinel master name is set explicitly (%q), so this instance has "+
				"its own gossip identity.", lr.SentinelMasterName()),
			LastTransitionTime: metav1.Now(),
		})
		return
	}

	msg := fmt.Sprintf("spec.sentinel.masterName is not set, so this instance uses the shared legacy "+
		"Sentinel master name %q. The master name is the only isolation boundary Sentinel's gossip "+
		"protocol has: any other Sentinel deployment reachable on this pod network that also uses %q "+
		"can merge with this instance's Sentinels and reassign its master to a foreign Redis pod, whose "+
		"replicas then FLUSH their datasets to resynchronise from it. Set spec.sentinel.masterName "+
		"(suggested: %q) — this is a client-visible change, so Sentinel-aware clients must be "+
		"reconfigured to the new name (clients using the label-routed master Service are unaffected). "+
		"Setting it to %q explicitly is accepted and silences this warning.",
		littleredv1alpha1.LegacySentinelMasterName, littleredv1alpha1.LegacySentinelMasterName,
		fmt.Sprintf("%s.%s", lr.Namespace, lr.Name), littleredv1alpha1.LegacySentinelMasterName)

	// One-shot Warning event: only on the transition INTO the warning state.
	if existing := meta.FindStatusCondition(lr.Status.Conditions,
		littleredv1alpha1.ConditionSentinelMasterNameUnscoped); existing == nil ||
		existing.Status != metav1.ConditionTrue {
		r.event(lr, corev1.EventTypeWarning, reasonSentinelMasterNameUnscoped, msg)
	}
	meta.SetStatusCondition(&lr.Status.Conditions, metav1.Condition{
		Type:               littleredv1alpha1.ConditionSentinelMasterNameUnscoped,
		Status:             metav1.ConditionTrue,
		Reason:             reasonSentinelMasterNameUnscoped,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
}
