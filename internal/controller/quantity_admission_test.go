/*
Copyright 2026.

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

// These specs run against the envtest API server with the CRDs generated
// from the kubebuilder markers in api/v1alpha1. They prove that the
// quantity patterns on ClusterTemplate.Memory, ClusterTemplate.RootVolumeSize,
// and CRCBundleSpec.GoldenVolumeSize reject bad values at admission time,
// before the operator ever reads them. The reconciler-level fallback for
// objects that predate this validation is covered separately in
// clusterinstance_quantity_validation_test.go.

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
)

var _ = Describe("CRD quantity validation", func() {
	quantityAdmissionInstance := func(name, memory, rootVolumeSize string) *brokerv1alpha1.ClusterInstance {
		return &brokerv1alpha1.ClusterInstance{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: brokerv1alpha1.ClusterInstanceSpec{
				Type: brokerv1alpha1.TopologyCRC,
				Template: brokerv1alpha1.ClusterTemplate{
					Memory:         memory,
					Cores:          2,
					RootVolumeSize: rootVolumeSize,
				},
			},
		}
	}

	It("rejects a ClusterInstance whose template memory is not a quantity", func() {
		instance := quantityAdmissionInstance("quantity-admission-bad-memory", "16 GB", "")
		err := k8sClient.Create(ctx, instance)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "error should be an Invalid rejection: %v", err)
	})

	It("rejects a ClusterInstance whose root volume size is not a quantity", func() {
		instance := quantityAdmissionInstance("quantity-admission-bad-root", testMemory, "35 GB")
		err := k8sClient.Create(ctx, instance)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "error should be an Invalid rejection: %v", err)
	})

	It("rejects a ClusterPool whose template root volume size is not a quantity", func() {
		pool := &brokerv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{Name: "quantity-admission-bad-pool", Namespace: testNamespace},
			Spec: brokerv1alpha1.ClusterPoolSpec{
				Type:    brokerv1alpha1.TopologyCRC,
				MaxSize: 1,
				Template: brokerv1alpha1.ClusterTemplate{
					Memory:         testMemory,
					Cores:          2,
					RootVolumeSize: "35GB",
				},
			},
		}
		err := k8sClient.Create(ctx, pool)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "error should be an Invalid rejection: %v", err)
	})

	It("rejects a CRCBundle whose golden volume size is not a quantity", func() {
		bundle := &brokerv1alpha1.CRCBundle{
			ObjectMeta: metav1.ObjectMeta{Name: "quantity-admission-bad-bundle"},
			Spec: brokerv1alpha1.CRCBundleSpec{
				Version:          "4.16.0",
				GoldenVolumeSize: "nope",
			},
		}
		err := k8sClient.Create(ctx, bundle)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "error should be an Invalid rejection: %v", err)
	})

	It("admits valid quantities and an unset root volume size", func() {
		valid := quantityAdmissionInstance("quantity-admission-valid", testMemory, "35Gi")
		Expect(k8sClient.Create(ctx, valid)).To(Succeed())

		unsetRoot := quantityAdmissionInstance("quantity-admission-unset-root", testMemory, "")
		Expect(k8sClient.Create(ctx, unsetRoot)).To(Succeed())
	})
})
