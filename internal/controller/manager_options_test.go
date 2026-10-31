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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// The manager's client must read Secrets live from the API server, never through the
// informer cache. A cached read starts a cluster-wide Secret informer — every Secret in
// the cluster, Helm release Secrets included — so the operator's memory scales with the
// cluster rather than with the instances it manages (slaptain#28 is the same defect in a
// sibling operator).
//
// The test runs a manager as a user that may only GET Secrets, which is exactly what the
// operator's ClusterRole grants. A cached read needs LIST+WATCH to sync its informer, so
// it cannot succeed here; only a live read can.
var _ = Describe("Manager client options", func() {
	const (
		userName   = "littlered-secret-get-only"
		roleName   = "littlered-secret-get-only"
		secretName = "manager-options-auth"
	)

	It("reads Secrets live, needing only the get verb", func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "mgr-opts-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(context.Background(), ns) })

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: ns.Name},
			Data:       map[string][]byte{"password": []byte("s3cret")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())

		role := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: roleName},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get"},
			}},
		}
		Expect(k8sClient.Create(ctx, role)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(context.Background(), role) })

		binding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: roleName},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: userName}},
		}
		Expect(k8sClient.Create(ctx, binding)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(context.Background(), binding) })

		user, err := testEnv.AddUser(envtest.User{Name: userName}, cfg)
		Expect(err).NotTo(HaveOccurred())

		mgr, err := ctrl.NewManager(user.Config(), ctrl.Options{
			Scheme:                 scheme.Scheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			Client:                 ManagerClientOptions(),
		})
		Expect(err).NotTo(HaveOccurred())

		mgrCtx, stop := context.WithCancel(ctx)
		DeferCleanup(stop)
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
		Expect(mgr.GetCache().WaitForCacheSync(mgrCtx)).To(BeTrue())

		// Bounded: a cached read whose informer cannot list blocks until the deadline
		// instead of failing, so the deadline is what turns the defect into a failure.
		readCtx, cancelRead := context.WithTimeout(mgrCtx, 5*time.Second)
		defer cancelRead()

		got := &corev1.Secret{}
		Expect(mgr.GetClient().Get(readCtx, types.NamespacedName{Name: secretName, Namespace: ns.Name}, got)).
			To(Succeed(), "a Secret read through the manager's client must not depend on list/watch")
		Expect(got.Data).To(HaveKeyWithValue("password", []byte("s3cret")))
	})
})
