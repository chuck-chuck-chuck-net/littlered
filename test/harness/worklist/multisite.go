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

// The multi-site half of the work-list seam (design §11 M3).
//
// Two things arrive together here, and the pairing is the point:
//
//  1. An item may name a topology FIXTURE (matrix §3). That makes it a
//     multi-site item, and makes the ARRAY need the lab substrate — the
//     four-context kubeconfig credentials and the lab inventory.
//  2. The array gains a run-scoped SETUP phase (design §9.2.4): a Job that runs
//     ONCE before any item, which every item then waits for. Nothing needs it
//     until M5's resident split-brain monitor, but introducing it now is what
//     lets that monitor slot in without restructuring — and M3 already earns it
//     back, because one site-reachability check for the whole array beats N
//     items each burning their own timeout rediscovering the same broken site.

// The names of the two objects the lab bootstrap leaves in the harness
// namespace. They are ARRAY-INDEPENDENT: they describe the lab, not a run, and
// they outlive any array (rebuilding them per array would mean re-reading every
// site's admin credentials at every launch).
const (
	// LabConfigMapName holds the lab inventory (hub context + per-site name,
	// admin context and hub-pod-routable endpoint).
	LabConfigMapName = "lr-e2e-harness-lab"
	// SiteCredentialSecretName holds one endpoint/CA/token triple per site,
	// harvested from the per-site harness ServiceAccounts.
	SiteCredentialSecretName = "lr-e2e-harness-sites"

	// Where the driver pod sees them.
	labMountPath      = "/lab"
	siteCredMountPath = "/site-creds"
)

// NeedsLab reports whether any item of the array names a topology fixture, and
// therefore whether the Job must mount the lab substrate.
//
// It gates the MOUNTS, which is why it must be exact rather than generous: a
// classic soak array has to stay runnable on any plain workload cluster, where
// neither the lab ConfigMap nor the credential Secret exists — and a Job that
// references a missing ConfigMap never starts a single pod.
func (l List) NeedsLab() bool {
	for _, it := range l.Items {
		if strings.TrimSpace(it.Fixture) != "" {
			return true
		}
	}
	return false
}

// SetupJobName / TeardownJobName name the run-scoped phase Jobs of an array.
// Derived from the array name and distinct from it, so `kubectl delete job
// <array>` can never reap the wrong one.
func SetupJobName(array string) string    { return array + "-setup" }
func TeardownJobName(array string) string { return array + "-teardown" }

// phaseJobData drives the setup/teardown Job template.
type phaseJobData struct {
	jobTemplateData
	JobName string
	Phase   string
}

// RenderSetupJob renders the run-scoped SETUP Job: one pod, run to completion
// before any work item proceeds.
func RenderSetupJob(p JobParams) (string, error) { return renderPhaseJob(p, "setup") }

// RenderTeardownJob renders the run-scoped TEARDOWN Job.
//
// It is applied AFTER the array ends, and deliberately not by the array itself:
// a Kubernetes Job cannot schedule a successor. `make e2e-harness-watch` fires
// it when the array finishes, and `make e2e-harness-teardown` runs it by hand.
// Because that trigger is not guaranteed, the teardown phase is REPORT-ONLY —
// it never deletes anything, so a run that is abandoned before it fires loses
// no evidence and needs no repair.
func RenderTeardownJob(p JobParams) (string, error) { return renderPhaseJob(p, "teardown") }

func renderPhaseJob(p JobParams, phase string) (string, error) {
	if !p.List.NeedsLab() {
		return "", fmt.Errorf("array %q declares no multi-site fixture, so it has no %s phase: "+
			"the run-scoped phases exist to reach the SITES, and this array has none",
			p.List.Array, phase)
	}
	base, err := jobData(p)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	d := phaseJobData{jobTemplateData: base, Phase: phase}
	switch phase {
	case "setup":
		d.JobName = SetupJobName(p.List.Array)
	default:
		d.JobName = TeardownJobName(p.List.Array)
	}
	if err := phaseJobTemplate.Execute(&b, d); err != nil {
		return "", fmt.Errorf("rendering the %s Job: %w", phase, err)
	}
	return b.String(), nil
}

// The phase Job is the SAME driver image and the same mounts as a work item —
// only LRH_PHASE differs, which is what keeps the phases from drifting away
// from the item path they bracket. It is not Indexed: there is exactly one.
var phaseJobTemplate = template.Must(template.New("phasejob").Parse(`apiVersion: batch/v1
kind: Job
metadata:
  name: {{ .JobName }}
  namespace: {{ .Namespace }}
  labels:
    app.kubernetes.io/name: lr-e2e-harness
    lr-e2e-harness/array: {{ .Array }}
    lr-e2e-harness/phase: {{ .Phase }}
spec:
  completions: 1
  parallelism: 1
  # A failed run-scoped phase is a real failure — unlike a work item, there is
  # no verdict to record and nothing downstream that can carry on regardless.
  backoffLimit: 0
  template:
    metadata:
      labels:
        app.kubernetes.io/name: lr-e2e-harness
        lr-e2e-harness/array: {{ .Array }}
        lr-e2e-harness/phase: {{ .Phase }}
    spec:
      restartPolicy: Never
      serviceAccountName: {{ .ServiceAccount }}
      containers:
        - name: driver
          image: {{ .Image }}
          imagePullPolicy: {{ .ImagePullPolicy }}
          command: ["/usr/local/bin/harness-entrypoint.sh"]
          env:
            - name: LRH_PHASE
              value: {{ .Phase }}
            - name: LRH_ARRAY
              value: {{ .Array }}
            - name: LRH_WORKLIST
              value: /worklist/worklist.yaml
            - name: LRH_ARTIFACTS_ROOT
              value: /artifacts
            - name: LRH_HARNESS_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          volumeMounts:
            - name: worklist
              mountPath: /worklist
              readOnly: true
            - name: artifacts
              mountPath: /artifacts
{{- if .NeedsLab }}
            - name: lab
              mountPath: ` + labMountPath + `
              readOnly: true
            - name: site-creds
              mountPath: ` + siteCredMountPath + `
              readOnly: true
{{- end }}
      volumes:
        - name: worklist
          configMap:
            name: {{ .WorkListName }}
        - name: artifacts
{{- if .ArtifactsPVC }}
          persistentVolumeClaim:
            claimName: {{ .ArtifactsPVC }}
{{- else }}
          hostPath:
            path: {{ .ArtifactsHostPath }}
            type: DirectoryOrCreate
{{- end }}
{{- if .NeedsLab }}
        - name: lab
          configMap:
            name: {{ .LabConfigMap }}
        - name: site-creds
          secret:
            secretName: {{ .SiteCredSecret }}
{{- end }}
`))
