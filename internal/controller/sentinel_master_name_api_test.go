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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	littleredv1alpha1 "github.com/littlered-operator/littlered-operator/api/v1alpha1"
)

// The Sentinel master name is the ONLY isolation boundary Sentinel's gossip protocol
// has: sentinelProcessHelloMessage() looks the name up and discards the message if it
// is unknown, and performs no other check. Two LittleRed instances that share a master
// name and can reach each other are, protocol-wise, one deployment — one can reassign
// the other's master and destroy its data.
//
// These specs assert the CRD-schema layer that forces every new instance to state a
// name. The value is deliberately NOT constrained to be unique or non-"mymaster":
// uniqueness is a property of the pod network and is not checkable at admission, and
// "mymaster" must stay expressible or the pre-migration state of every existing
// instance becomes inexpressible.
var _ = Describe("Sentinel masterName CRD validation", func() {
	var counter int

	newSentinelCR := func(s *littleredv1alpha1.SentinelSpec) *littleredv1alpha1.LittleRed {
		counter++
		return &littleredv1alpha1.LittleRed{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("smn-crd-%d", counter),
				Namespace: testNamespaceDefault,
			},
			Spec: littleredv1alpha1.LittleRedSpec{
				Mode:     ModeSentinel,
				Sentinel: s,
			},
		}
	}

	It("rejects a sentinel instance whose sentinel block omits masterName", func() {
		lr := newSentinelCR(&littleredv1alpha1.SentinelSpec{Quorum: 2})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("accepts an instance-scoped masterName", func() {
		lr := newSentinelCR(&littleredv1alpha1.SentinelSpec{Quorum: 2, MasterName: "team-a.cache"})
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())
	})

	// A legacy application may hardcode "mymaster" with no way to parameterise it, and
	// it is every existing instance's current value — so it must stay legal. The
	// operator warns at runtime instead.
	It("accepts mymaster when set explicitly", func() {
		lr := newSentinelCR(&littleredv1alpha1.SentinelSpec{Quorum: 2, MasterName: "mymaster"})
		Expect(k8sClient.Create(ctx, lr)).To(Succeed())
	})

	// The hello payload is comma-split (8 tokens) and sentinel.conf is space-split, so
	// either character in the name corrupts the wire format or the config file.
	It("rejects a masterName containing a comma", func() {
		lr := newSentinelCR(&littleredv1alpha1.SentinelSpec{Quorum: 2, MasterName: "team,a"})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("rejects a masterName containing whitespace", func() {
		lr := newSentinelCR(&littleredv1alpha1.SentinelSpec{Quorum: 2, MasterName: "team a"})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("rejects an empty masterName", func() {
		lr := newSentinelCR(&littleredv1alpha1.SentinelSpec{Quorum: 2, MasterName: ""})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})

	It("rejects an over-long masterName", func() {
		lr := newSentinelCR(&littleredv1alpha1.SentinelSpec{Quorum: 2, MasterName: strings.Repeat("a", 200)})
		Expect(k8sClient.Create(ctx, lr)).NotTo(Succeed())
	})
})
