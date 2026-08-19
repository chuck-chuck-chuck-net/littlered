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
	"maps"
	"slices"
	"strings"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// Metadata propagation (ADR-015). Labels and annotations on the LittleRed resource are
// inherited by every resource the operator owns, so an instance can be grouped under a
// team, environment or application name for monitoring and scraping without the operator
// needing a knob per use case. Two rules bound it:
//
//  1. The operator's own keys always win. Its structural labels are Service and
//     StatefulSet selectors, and a StatefulSet whose pod template disagrees with its
//     selector is rejected by the API server — so those keys can never come from user
//     input. spec.appName is the supported way to choose the app.kubernetes.io/name
//     value.
//  2. Tool-injected metadata does not propagate. Helm, Flux, Argo CD and kubectl stamp
//     their own bookkeeping onto the resources they apply; copying it onto children
//     misattributes ownership (Argo CD's tracking labels in particular confuse its
//     pruning) and last-applied-configuration would embed a whole copy of the CR into
//     every child object.

// nonInheritedPrefixes are key prefixes that never propagate from the LittleRed resource
// to the resources it owns: the operator's own namespace, plus the bookkeeping that
// packaging and GitOps tooling stamps onto whatever it applies.
var nonInheritedPrefixes = []string{
	operatorKeyPrefix,
	"kubectl.kubernetes.io/",
	"argocd.argoproj.io/",
	"meta.helm.sh/",
	"helm.sh/",
	"kustomize.toolkit.fluxcd.io/",
	"helm.toolkit.fluxcd.io/",
}

// structuralLabelKeys are the label keys that make up the Service and StatefulSet
// selectors. They identify which pods belong to which workload, so they are operator
// property: user input never sets or overrides them.
var structuralLabelKeys = []string{
	labelAppName,
	labelAppInstance,
	labelAppComponent,
	LabelShard,
	LabelRole,
}

// operatorOwnedKeys are the keys the operator sets itself and therefore refuses to
// inherit: the structural selector labels plus the descriptive ones it keeps current.
var operatorOwnedKeys = append([]string{
	labelAppManagedBy,
	labelAppVersion,
}, structuralLabelKeys...)

// appNameFor returns the configured app.kubernetes.io/name value, falling back to the
// built-in name. The fallback matters because this value lands in selectors: an object
// that never passed through defaulting (a hand-built test fixture, or a CR stored before
// the field existed) must not produce an empty selector value, which matches nothing.
func appNameFor(lr *littleredv1alpha1.LittleRed) string {
	if lr.Spec.AppName != "" {
		return lr.Spec.AppName
	}
	return appName
}

// isStructuralLabelKey reports whether key is one of the selector labels the operator
// owns outright.
func isStructuralLabelKey(key string) bool {
	return slices.Contains(structuralLabelKeys, key)
}

// isInheritableKey reports whether a label or annotation key on the LittleRed resource
// should propagate to the resources it owns.
func isInheritableKey(key string) bool {
	for _, prefix := range nonInheritedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}
	return !slices.Contains(operatorOwnedKeys, key)
}

// inheritable copies the entries of src whose keys propagate. It always returns a
// non-nil map so callers can merge into it unconditionally.
func inheritable(src map[string]string) map[string]string {
	out := make(map[string]string, len(src))
	for k, v := range src {
		if isInheritableKey(k) {
			out[k] = v
		}
	}
	return out
}

// inheritedLabels returns the labels the LittleRed resource passes on to the resources
// it owns.
func inheritedLabels(lr *littleredv1alpha1.LittleRed) map[string]string {
	return inheritable(lr.Labels)
}

// inheritedAnnotations returns the annotations the LittleRed resource passes on to the
// resources it owns.
func inheritedAnnotations(lr *littleredv1alpha1.LittleRed) map[string]string {
	return inheritable(lr.Annotations)
}

// objectLabels layers the operator's own labels over the inherited ones, for the metadata
// of an owned resource.
func objectLabels(lr *littleredv1alpha1.LittleRed, owned map[string]string) map[string]string {
	out := inheritedLabels(lr)
	maps.Copy(out, owned)
	return out
}

// podTemplateLabels builds the labels for a pod template: labels inherited from the LittleRed
// resource, then the operator's own (which include the selector this pod must match),
// then spec.podTemplate.labels as the most specific layer.
//
// The structural keys are dropped from the podTemplate layer rather than merged: they are
// what the StatefulSet selects on, and the API server rejects a StatefulSet whose pod
// template does not match its own selector. CRD validation rejects them up front, so this
// only has to hold the line for objects that predate the rule.
func podTemplateLabels(lr *littleredv1alpha1.LittleRed, owned map[string]string) map[string]string {
	out := inheritedLabels(lr)
	maps.Copy(out, owned)
	for k, v := range lr.Spec.PodTemplate.Labels {
		if isStructuralLabelKey(k) {
			continue
		}
		out[k] = v
	}
	return out
}

// podTemplateAnnotations builds the annotations for a pod template: inherited, then
// spec.podTemplate.annotations, then the operator's own (the config hash, which must
// reflect the real config for the rollout to be correct).
func podTemplateAnnotations(lr *littleredv1alpha1.LittleRed, owned map[string]string) map[string]string {
	out := inheritedAnnotations(lr)
	maps.Copy(out, lr.Spec.PodTemplate.Annotations)
	maps.Copy(out, owned)
	return out
}
