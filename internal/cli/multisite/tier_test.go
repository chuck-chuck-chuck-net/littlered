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

package multisite

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSelectTier(t *testing.T) {
	cases := []struct {
		name    string
		verb    string
		flags   TierFlags
		want    Tier
		wantErr bool
	}{
		{"verify default → live", verbVerify, TierFlags{}, TierLive, false},
		{"inspect default → live", "inspect", TierFlags{}, TierLive, false},
		{"debug-dump default → live", "debug-dump", TierFlags{}, TierLive, false},
		{"status default → status", "status", TierFlags{}, TierStatus, false},
		{"verify --from-status → status", verbVerify, TierFlags{FromStatus: true}, TierStatus, false},
		{"verify --direct → direct", verbVerify, TierFlags{Direct: true}, TierDirect, false},
		{"verify --site-context → direct", verbVerify, TierFlags{SiteContextSet: true}, TierDirect, false},
		{"status ignores flags → status", "status", TierFlags{Direct: true}, TierStatus, false},
		{"conflict from-status+direct", verbVerify, TierFlags{FromStatus: true, Direct: true}, 0, true},
		{"conflict from-status+site-context", verbVerify, TierFlags{FromStatus: true, SiteContextSet: true}, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SelectTier(tc.verb, tc.flags)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got tier %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("SelectTier(%q,%+v)=%v, want %v", tc.verb, tc.flags, got, tc.want)
			}
		})
	}
}

func TestTierLabel(t *testing.T) {
	for tier, want := range map[Tier]string{
		TierLive:   "live via hub Job",
		TierStatus: "operator status",
		TierDirect: "direct site contexts",
	} {
		if got := tier.Label(); !strings.Contains(got, want) {
			t.Errorf("Tier(%d).Label()=%q, want substring %q", tier, got, want)
		}
	}
}

func TestBuildGatherJob(t *testing.T) {
	job := BuildGatherJob(GatherJobParams{
		OperatorNamespace:  testOperatorNS,
		ServiceAccountName: "littlered-controller-manager",
		Image:              "ghcr.io/littlered-operator/littlered:abc123",
		InstanceNamespace:  testInstanceName,
		InstanceName:       testInstanceName,
		TTLSeconds:         120,
		NameSuffix:         "fixed99",
	})

	if job.Namespace != testOperatorNS {
		t.Errorf("namespace=%q, want littlered-system", job.Namespace)
	}
	if !strings.HasSuffix(job.Name, "fixed99") {
		t.Errorf("job name %q must end with the supplied suffix", job.Name)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 120 {
		t.Errorf("TTLSecondsAfterFinished must be set to 120, got %v", job.Spec.TTLSecondsAfterFinished)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("BackoffLimit must be 0 (no retries), got %v", job.Spec.BackoffLimit)
	}
	pod := job.Spec.Template.Spec
	if pod.ServiceAccountName != "littlered-controller-manager" {
		t.Errorf("SA=%q, want the operator SA", pod.ServiceAccountName)
	}
	if pod.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("RestartPolicy=%q, want Never", pod.RestartPolicy)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("want exactly one container, got %d", len(pod.Containers))
	}
	c := pod.Containers[0]
	if c.Image != "ghcr.io/littlered-operator/littlered:abc123" {
		t.Errorf("image=%q, want the operator image", c.Image)
	}
	wantCmd := []string{"/manager", "--gather", "ms-smoke/ms-smoke", "-o", "json"}
	if strings.Join(c.Command, " ") != strings.Join(wantCmd, " ") {
		t.Errorf("command=%v, want %v", c.Command, wantCmd)
	}
}

func TestExtractOperatorInfo(t *testing.T) {
	mk := func(sa string, containers ...corev1.Container) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "littlered", Namespace: testOperatorNS},
			Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				ServiceAccountName: sa, Containers: containers,
			}}},
		}
	}

	t.Run("picks manager container", func(t *testing.T) {
		d := mk("op-sa",
			corev1.Container{Name: "kube-rbac-proxy", Image: "proxy:1"},
			corev1.Container{Name: "manager", Image: testOpKey2},
		)
		info, err := ExtractOperatorInfo(d)
		if err != nil {
			t.Fatal(err)
		}
		if info.ServiceAccountName != "op-sa" || info.Image != testOpKey2 {
			t.Errorf("got %+v, want {op-sa op:2}", info)
		}
	})

	t.Run("falls back to first container", func(t *testing.T) {
		d := mk("op-sa", corev1.Container{Name: "only", Image: "op:9"})
		info, err := ExtractOperatorInfo(d)
		if err != nil || info.Image != "op:9" {
			t.Errorf("got %+v err=%v, want image op:9", info, err)
		}
	})

	t.Run("errors on missing SA", func(t *testing.T) {
		if _, err := ExtractOperatorInfo(mk("", corev1.Container{Name: "manager", Image: testOpKey2})); err == nil {
			t.Error("expected error for missing serviceAccountName")
		}
	})

	t.Run("errors on no containers", func(t *testing.T) {
		if _, err := ExtractOperatorInfo(mk("op-sa")); err == nil {
			t.Error("expected error for no containers")
		}
	})
}

func TestStatusVerdict(t *testing.T) {
	healthy := []ShardView{
		{Shard: 0, MasterSite: ctxS1, ReplicaSites: []string{ctxS2}, CrossSite: true},
		{Shard: 1, MasterSite: ctxS2, ReplicaSites: []string{"scm-s3"}, CrossSite: true},
	}

	t.Run("all healthy", func(t *testing.T) {
		v := StatusVerdict("ok", 16384, healthy, nil)
		if !v.OK || v.Degraded {
			t.Errorf("want OK & not degraded, got %+v", v)
		}
	})

	t.Run("cluster_state fail", func(t *testing.T) {
		v := StatusVerdict("fail", 16384, healthy, nil)
		if v.OK {
			t.Errorf("want not OK for cluster_state fail, got %+v", v)
		}
	})

	t.Run("missing slots", func(t *testing.T) {
		v := StatusVerdict("ok", 8192, healthy, nil)
		if v.OK {
			t.Errorf("want not OK for partial slots, got %+v", v)
		}
	})

	t.Run("shard not cross-site → FAIL", func(t *testing.T) {
		bad := []ShardView{{Shard: 0, MasterSite: ctxS1, ReplicaSites: []string{ctxS1}, CrossSite: false}}
		v := StatusVerdict("ok", 16384, bad, nil)
		if v.OK {
			t.Errorf("want not OK for single-site shard, got %+v", v)
		}
	})

	t.Run("unreachable site → FAIL", func(t *testing.T) {
		v := StatusVerdict("ok", 16384, healthy, []string{"scm-s3"})
		if v.OK {
			t.Errorf("want not OK when a site is unreachable, got %+v", v)
		}
	})

	t.Run("link down → degraded not failed", func(t *testing.T) {
		deg := []ShardView{{Shard: 0, MasterSite: ctxS1, ReplicaSites: []string{ctxS2}, CrossSite: true, LinkDown: true}}
		v := StatusVerdict("ok", 16384, deg, nil)
		if !v.OK || !v.Degraded {
			t.Errorf("want OK & degraded for link down, got %+v", v)
		}
	})
}
