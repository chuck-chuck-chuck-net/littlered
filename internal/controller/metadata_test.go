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
	"testing"

	appsv1 "k8s.io/api/apps/v1"

	littleredv1alpha1 "github.com/chuck-chuck-chuck-net/littlered/api/v1alpha1"
)

// withCRMetadata returns a test CR carrying the given metadata labels/annotations.
func withCRMetadata(labels, annotations map[string]string) *littleredv1alpha1.LittleRed {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Labels = labels
	lr.Annotations = annotations
	return lr
}

// TestInheritedLabels covers the propagation filter: user labels on the CR flow to
// child resources, operator-owned and tool-injected keys do not (ADR-021).
func TestInheritedLabels(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want map[string]string
	}{
		{
			name: "plain user labels propagate",
			in:   map[string]string{metaTeamKey: metaTeamValue, "env": "prod"},
			want: map[string]string{metaTeamKey: metaTeamValue, "env": "prod"},
		},
		{
			name: "non-owned app.kubernetes.io keys propagate",
			in:   map[string]string{"app.kubernetes.io/part-of": "checkout"},
			want: map[string]string{"app.kubernetes.io/part-of": "checkout"},
		},
		{
			name: "structural selector keys are dropped",
			in: map[string]string{
				labelAppName:      metaHijackValue,
				labelAppInstance:  metaHijackValue,
				labelAppComponent: metaHijackValue,
				LabelShard:        "9",
				LabelRole:         RoleMaster,
				metaKeepKey:       metaKeepValue,
			},
			want: map[string]string{metaKeepKey: metaKeepValue},
		},
		{
			name: "operator-managed informational keys are dropped",
			in: map[string]string{
				"app.kubernetes.io/managed-by":     "someone-else",
				labelAppVersion:                    metaStaleTag,
				"redis.chuck-chuck-chuck.net/mode": "bogus",
				metaKeepKey:                        metaKeepValue,
			},
			want: map[string]string{metaKeepKey: metaKeepValue},
		},
		{
			name: "GitOps and packaging metadata is not propagated",
			in: map[string]string{
				"argocd.argoproj.io/instance":            "app",
				"helm.sh/chart":                          "littlered-0.3.0",
				"kustomize.toolkit.fluxcd.io/name":       "flux",
				"helm.toolkit.fluxcd.io/name":            "hr",
				"redis.chuck-chuck-chuck.net/some-inner": "x",
				metaKeepKey:                              metaKeepValue,
			},
			want: map[string]string{metaKeepKey: metaKeepValue},
		},
		{
			name: "no labels yields empty, never nil",
			in:   nil,
			want: map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inheritedLabels(withCRMetadata(tt.in, nil))
			assertMapEqual(t, "inheritedLabels", got, tt.want)
		})
	}
}

// TestInheritedAnnotations mirrors TestInheritedLabels for annotations. The
// last-applied-configuration annotation matters most: propagating it would embed a
// full copy of the CR into every child object.
func TestInheritedAnnotations(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want map[string]string
	}{
		{
			name: "plain user annotations propagate",
			in:   map[string]string{metaOwnerKey: metaOwnerValue},
			want: map[string]string{metaOwnerKey: metaOwnerValue},
		},
		{
			name: "kubectl last-applied-configuration is dropped",
			in: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": "{...}",
				metaOwnerKey: metaOwnerValue,
			},
			want: map[string]string{metaOwnerKey: metaOwnerValue},
		},
		{
			name: "operator annotations are dropped",
			in: map[string]string{
				AnnotationConfigHash:     "deadbeef",
				AnnotationDisablePolling: "true",
				metaOwnerKey:             metaOwnerValue,
			},
			want: map[string]string{metaOwnerKey: metaOwnerValue},
		},
		{
			name: "GitOps tracking annotations are dropped",
			in: map[string]string{
				"argocd.argoproj.io/tracking-id": "app:v1/LittleRed:ns/name",
				"meta.helm.sh/release-name":      "rel",
				metaOwnerKey:                     metaOwnerValue,
			},
			want: map[string]string{metaOwnerKey: metaOwnerValue},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inheritedAnnotations(withCRMetadata(nil, tt.in))
			assertMapEqual(t, "inheritedAnnotations", got, tt.want)
		})
	}
}

// TestObjectLabelsOperatorWins pins the precedence: inherited labels are a base layer
// that the operator's own labels always overlay.
func TestObjectLabelsOperatorWins(t *testing.T) {
	lr := withCRMetadata(map[string]string{metaTeamKey: metaTeamValue, labelAppName: metaHijackValue}, nil)

	got := objectLabels(lr, map[string]string{
		labelAppName:      appName,
		labelAppComponent: ComponentRedis,
	})

	if got[metaTeamKey] != metaTeamValue {
		t.Errorf("inherited label lost: %v", got)
	}
	if got[labelAppName] != appName {
		t.Errorf("%s = %q, want operator value %q", labelAppName, got[labelAppName], appName)
	}
	if got[labelAppComponent] != ComponentRedis {
		t.Errorf("owned label not applied: %v", got)
	}

	// The path every builder actually takes: commonLabels must not surface the hijack
	// attempt either, whether or not the caller happens to overlay that key.
	if common := commonLabels(lr); common[labelAppName] != appName {
		t.Errorf("commonLabels[%s] = %q, want %q", labelAppName, common[labelAppName], appName)
	}
}

// TestPodLabelsPrecedence pins the three-layer order for pod templates: inherited CR
// labels, then operator-owned selector labels, then spec.podTemplate.labels — with the
// structural keys never overridable, because a pod template that drifts from the
// StatefulSet selector is rejected by the API server.
func TestPodLabelsPrecedence(t *testing.T) {
	lr := withCRMetadata(map[string]string{metaTeamKey: metaTeamValue, metaTierKey: "from-cr"}, nil)
	lr.Spec.PodTemplate.Labels = map[string]string{
		metaTierKey:      "from-podtemplate",
		labelAppName:     metaHijackValue,
		labelAppInstance: metaHijackValue,
		LabelShard:       "9",
		labelAppVersion:  metaStaleTag,
	}

	owned := redisSelectorLabels(lr)
	got := podTemplateLabels(lr, owned)

	for k, want := range owned {
		if got[k] != want {
			t.Errorf("structural label %s = %q, want %q (selector must be a subset of pod labels)", k, got[k], want)
		}
	}
	if got[LabelShard] != "" {
		t.Errorf("%s must not be settable by the user, got %q", LabelShard, got[LabelShard])
	}
	if got[metaTierKey] != "from-podtemplate" {
		t.Errorf("podTemplate.labels should beat inherited CR labels, got %q", got[metaTierKey])
	}
	if got[metaTeamKey] != metaTeamValue {
		t.Errorf("inherited label lost: %v", got)
	}
	if got[labelAppVersion] != metaStaleTag {
		t.Errorf("non-structural key should stay overridable by podTemplate.labels, got %q", got[labelAppVersion])
	}
}

// TestAppNameConfigurable checks that spec.appName replaces the app.kubernetes.io/name
// value everywhere it appears — object labels, pod labels and every selector — since a
// selector that disagrees with the pod labels breaks the workload.
func TestAppNameConfigurable(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.AppName = metaCustomApp

	selectors := map[string]map[string]string{
		"selectorLabels":            selectorLabels(lr),
		"redisSelectorLabels":       redisSelectorLabels(lr),
		"sentinelSelectorLabels":    sentinelSelectorLabels(lr),
		"masterSelectorLabels":      masterSelectorLabels(lr),
		"clusterSelectorLabels":     clusterSelectorLabels(lr),
		"clusterShardSelectorLabel": clusterShardSelectorLabels(lr, 1),
		"commonLabels":              commonLabels(lr),
	}
	for name, got := range selectors {
		if got[labelAppName] != metaCustomApp {
			t.Errorf("%s[%s] = %q, want %q", name, labelAppName, got[labelAppName], metaCustomApp)
		}
	}

	if got := podTemplateLabels(lr, redisSelectorLabels(lr)); got[labelAppName] != metaCustomApp {
		t.Errorf("podLabels[%s] = %q, want %q", labelAppName, got[labelAppName], metaCustomApp)
	}
}

// TestAppNameDefaultsWhenUnset guards the zero value: an object that never went through
// defaulting must still get the built-in app name, never an empty label value (an empty
// selector value would match nothing).
func TestAppNameDefaultsWhenUnset(t *testing.T) {
	lr := newTestLittleRed(testLRName, testNamespace)
	lr.Spec.AppName = ""

	if got := selectorLabels(lr)[labelAppName]; got != appName {
		t.Errorf("selectorLabels[%s] = %q, want fallback %q", labelAppName, got, appName)
	}
}

// TestBuildersCarryInheritedMetadata is the integration-level check that the
// propagation reaches the resources a scrape config actually selects on, not just the
// pure helpers.
//
// It enumerates EVERY object-producing builder in EVERY mode rather than "one builder
// per kind", because the per-kind sampling is what let failover mode ship uncovered:
// resources_failover.go was written after the inheritance branch was cut, its builders
// were never wired, and a test that checked buildStatefulSet as the representative
// StatefulSet saw nothing. A builder added to a mode from now on fails this test until
// it is wired, which is the property worth having.
func TestBuildersCarryInheritedMetadata(t *testing.T) {
	type object struct {
		name        string
		labels      map[string]string
		annotations map[string]string
	}

	// withMode returns a defaulted CR in the given mode, carrying one inherited label
	// and one inherited annotation.
	withMode := func(mode string) *littleredv1alpha1.LittleRed {
		lr := withCRMetadata(
			map[string]string{metaTeamKey: metaTeamValue},
			map[string]string{metaOwnerKey: metaOwnerValue},
		)
		lr.Spec.Mode = mode
		enabled := true
		lr.Spec.Metrics.Enabled = &enabled
		lr.SetDefaults()
		return lr
	}

	// podTemplate flattens a StatefulSet into the object pair plus its pod template,
	// which is the half that actually reaches a running pod.
	stsObjects := func(what string, sts *appsv1.StatefulSet) []object {
		return []object{
			{what, sts.Labels, sts.Annotations},
			{what + " pod template", sts.Spec.Template.Labels, sts.Spec.Template.Annotations},
		}
	}

	modes := []struct {
		mode    string
		objects func(lr *littleredv1alpha1.LittleRed) []object
	}{
		{
			mode: ModeStandalone,
			objects: func(lr *littleredv1alpha1.LittleRed) []object {
				svc, cm, sm := buildService(lr), buildConfigMap(lr), buildServiceMonitor(lr)
				objs := stsObjects("StatefulSet", buildStatefulSet(lr))
				return append(objs,
					object{"Service", svc.Labels, svc.Annotations},
					object{"ConfigMap", cm.Labels, cm.Annotations},
					object{"ServiceMonitor", sm.Labels, sm.Annotations},
				)
			},
		},
		{
			mode: ModeSentinel,
			objects: func(lr *littleredv1alpha1.LittleRed) []object {
				objs := stsObjects("Redis StatefulSet", buildRedisStatefulSetSentinel(lr, 3))
				objs = append(objs, stsObjects("Sentinel StatefulSet", buildSentinelStatefulSet(lr, 3))...)
				redisCM, sentinelCM := buildConfigMapSentinelMode(lr), buildSentinelConfigMap(lr)
				master, replicas := buildMasterService(lr), buildReplicasHeadlessService(lr)
				sentinelSvc := buildSentinelHeadlessService(lr)
				redisPDB, sentinelPDB := buildSentinelRedisPDB(lr), buildSentinelPDB(lr)
				return append(objs,
					object{"Redis ConfigMap", redisCM.Labels, redisCM.Annotations},
					object{"Sentinel ConfigMap", sentinelCM.Labels, sentinelCM.Annotations},
					object{"master Service", master.Labels, master.Annotations},
					object{"replicas headless Service", replicas.Labels, replicas.Annotations},
					object{"sentinel headless Service", sentinelSvc.Labels, sentinelSvc.Annotations},
					object{"Redis PDB", redisPDB.Labels, redisPDB.Annotations},
					object{"Sentinel PDB", sentinelPDB.Labels, sentinelPDB.Annotations},
				)
			},
		},
		{
			mode: ModeCluster,
			objects: func(lr *littleredv1alpha1.LittleRed) []object {
				objs := stsObjects("shard StatefulSet", buildClusterShardStatefulSet(lr, 0, nil))
				cm := buildClusterConfigMap(lr)
				headless, client := buildClusterHeadlessService(lr), buildClusterClientService(lr)
				pdb := buildClusterShardPDB(lr, 0)
				return append(objs,
					object{"cluster ConfigMap", cm.Labels, cm.Annotations},
					object{"cluster headless Service", headless.Labels, headless.Annotations},
					object{"cluster client Service", client.Labels, client.Annotations},
					object{"shard PDB", pdb.Labels, pdb.Annotations},
				)
			},
		},
		{
			// Failover mode reuses sentinel mode's master/replicas Services (the label
			// routing is identical, pillar 3.14) and adds three builders of its own.
			mode: ModeFailover,
			objects: func(lr *littleredv1alpha1.LittleRed) []object {
				objs := stsObjects("failover StatefulSet", buildRedisStatefulSetFailover(lr))
				cm, pdb := buildConfigMapFailoverMode(lr), buildFailoverRedisPDB(lr)
				master, replicas := buildMasterService(lr), buildReplicasHeadlessService(lr)
				return append(objs,
					object{"failover ConfigMap", cm.Labels, cm.Annotations},
					object{"failover PDB", pdb.Labels, pdb.Annotations},
					object{"master Service", master.Labels, master.Annotations},
					object{"replicas headless Service", replicas.Labels, replicas.Annotations},
				)
			},
		},
	}

	for _, m := range modes {
		t.Run(m.mode, func(t *testing.T) {
			lr := withMode(m.mode)
			for _, o := range m.objects(lr) {
				if o.labels[metaTeamKey] != metaTeamValue {
					t.Errorf("%s labels missing inherited team label: %v", o.name, o.labels)
				}
				if o.annotations[metaOwnerKey] != metaOwnerValue {
					t.Errorf("%s annotations missing inherited owner annotation: %v", o.name, o.annotations)
				}
			}
		})
	}
}

// assertMapEqual compares two string maps and reports the whole pair on mismatch.
func assertMapEqual(t *testing.T, what string, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s = %v, want %v", what, got, want)
		return
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", what, got, want)
			return
		}
	}
}
