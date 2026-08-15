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

package worklist

import (
	"fmt"
	"strings"
	"text/template"
)

// BundleParams drives RenderBundle.
type BundleParams struct {
	JobParams
	// WorkListYAML is the verbatim work list, embedded in the ConfigMap the Job
	// mounts. Emitting the Job and the ConfigMap from one call is what
	// guarantees the list the driver resolves is the list that was validated.
	WorkListYAML string
}

// RenderBundle renders everything needed to launch one array as a single
// applyable multi-document manifest: the harness namespace, the driver's
// identity and RBAC, the work-list ConfigMap and the Indexed Job.
func RenderBundle(p BundleParams) (string, error) {
	if strings.TrimSpace(p.WorkListYAML) == "" {
		return "", fmt.Errorf("work list YAML is required (the Job would mount an empty list)")
	}
	job, err := RenderJob(p.JobParams)
	if err != nil {
		return "", err
	}

	d := bundleTemplateData{
		Array:          p.List.Array,
		Namespace:      p.Namespace,
		ServiceAccount: firstNonEmpty(p.ServiceAccount, "lr-e2e-harness"),
		WorkListName:   firstNonEmpty(p.WorkListName, p.List.Array+"-worklist"),
		WorkListYAML:   indent(p.WorkListYAML, "    "),
	}

	var b strings.Builder
	if err := bundleTemplate.Execute(&b, d); err != nil {
		return "", fmt.Errorf("rendering bundle: %w", err)
	}
	b.WriteString("---\n")
	b.WriteString(job)
	return b.String(), nil
}

func indent(s, pad string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		if l == "" {
			continue
		}
		lines[i] = pad + l
	}
	return strings.Join(lines, "\n")
}

type bundleTemplateData struct {
	Array          string
	Namespace      string
	ServiceAccount string
	WorkListName   string
	WorkListYAML   string
}

// The driver's RBAC is deliberately broad (design §4.4): it creates namespaces,
// deploys operators, creates CRs and execs into pods. That is acceptable on a
// dedicated test cluster, and it is enumerated rather than cluster-admin so the
// blast radius is reviewable. The OPERATOR's own RBAC is untouched by this.
var bundleTemplate = template.Must(template.New("bundle").Parse(`apiVersion: v1
kind: Namespace
metadata:
  name: {{ .Namespace }}
  labels:
    app.kubernetes.io/name: lr-e2e-harness
    # The artifact volume is a hostPath, which PSS "baseline" forbids.
    pod-security.kubernetes.io/enforce: privileged
    pod-security.kubernetes.io/warn: privileged
    pod-security.kubernetes.io/audit: privileged
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ .ServiceAccount }}
  namespace: {{ .Namespace }}
  labels:
    app.kubernetes.io/name: lr-e2e-harness
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ .ServiceAccount }}
  labels:
    app.kubernetes.io/name: lr-e2e-harness
rules:
  # Namespaces: one per work item, created and (on success) deleted.
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["*"]
  # Everything the suite and the operator touch inside a test namespace.
  - apiGroups: [""]
    resources: ["pods", "pods/exec", "pods/log", "pods/status", "services",
                "configmaps", "secrets", "serviceaccounts", "events",
                "persistentvolumeclaims"]
    verbs: ["*"]
  - apiGroups: ["apps"]
    resources: ["statefulsets", "deployments", "replicasets"]
    verbs: ["*"]
  - apiGroups: ["policy"]
    resources: ["poddisruptionbudgets"]
    verbs: ["*"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["*"]
  - apiGroups: ["redis.chuck-chuck-chuck.net"]
    resources: ["*"]
    verbs: ["*"]
  - apiGroups: ["events.k8s.io"]
    resources: ["events"]
    verbs: ["*"]
  - apiGroups: ["monitoring.coreos.com"]
    resources: ["servicemonitors"]
    verbs: ["*"]
  # M2 helm-installs a per-item operator, which needs its RBAC and CRDs. The
  # leases rule is not for the driver's own use: RBAC forbids granting rights
  # the grantor lacks, and the operator's leader-election Role holds leases, so
  # without this every item would fail at "helm upgrade --install".
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["*"]
  - apiGroups: ["rbac.authorization.k8s.io"]
    resources: ["roles", "rolebindings", "clusterroles", "clusterrolebindings"]
    verbs: ["*"]
  - apiGroups: ["apiextensions.k8s.io"]
    resources: ["customresourcedefinitions"]
    verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ .ServiceAccount }}
  labels:
    app.kubernetes.io/name: lr-e2e-harness
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ .ServiceAccount }}
subjects:
  - kind: ServiceAccount
    name: {{ .ServiceAccount }}
    namespace: {{ .Namespace }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .WorkListName }}
  namespace: {{ .Namespace }}
  labels:
    app.kubernetes.io/name: lr-e2e-harness
    lr-e2e-harness/array: {{ .Array }}
data:
  worklist.yaml: |
{{ .WorkListYAML }}
`))
