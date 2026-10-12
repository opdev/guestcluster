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

package resources

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
)

// quantityTestInstance returns a ClusterInstance whose template carries the
// given memory and root volume size.
func quantityTestInstance(memory, rootVolumeSize string) *brokerv1alpha1.ClusterInstance {
	return &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "quantity-instance", Namespace: "tenant"},
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

// quantityTestBundle returns a CRCBundle with the given golden volume size
// and populated Ready disk references, as BuildCRCDataVolumeFromBundle
// requires.
func quantityTestBundle(size string) *brokerv1alpha1.CRCBundle {
	return &brokerv1alpha1.CRCBundle{
		ObjectMeta: metav1.ObjectMeta{Name: "crc-4-16-0-amd64"},
		Spec: brokerv1alpha1.CRCBundleSpec{
			Version:          "4.16.0",
			GoldenVolumeSize: size,
		},
		Status: brokerv1alpha1.CRCBundleStatus{
			DiskImagePVCRef:       &corev1.LocalObjectReference{Name: "golden"},
			DiskImagePVCNamespace: "guestcluster-operator-system",
		},
	}
}

// TestBuildersRejectInvalidQuantities covers the run-time safety net behind
// the CRD pattern validation. Every builder that parses a CRD quantity field
// must return an error, never panic, when the value cannot parse. Before
// this safety net existed, each call below panicked through
// resource.MustParse and crashed the reconciler.
func TestBuildersRejectInvalidQuantities(t *testing.T) {
	invalidMemory := []string{"16 GB", "16GB", "garbage", "16Gi ", " ", "1.2.3"}
	for _, memory := range invalidMemory {
		instance := quantityTestInstance(memory, "")
		if _, err := BuildCRCVirtualMachine(instance, "rootdisk"); err == nil || !strings.Contains(err.Error(), "template.memory") {
			t.Errorf("BuildCRCVirtualMachine(memory=%q) error = %v, want a template.memory failure", memory, err)
		}
		if _, err := BuildNodePool(instance, "hosted-cluster", "hcp-ns", 1); err == nil || !strings.Contains(err.Error(), "template.memory") {
			t.Errorf("BuildNodePool(memory=%q) error = %v, want a template.memory failure", memory, err)
		}
	}

	invalidSizes := []string{"35 GB", "35GB", "garbage", "1.2.3", " "}
	for _, size := range invalidSizes {
		instance := quantityTestInstance("16Gi", size)
		if _, err := BuildCRCDataVolume(instance); err == nil || !strings.Contains(err.Error(), "template.rootVolumeSize") {
			t.Errorf("BuildCRCDataVolume(rootVolumeSize=%q) error = %v, want a template.rootVolumeSize failure", size, err)
		}
		if _, err := BuildCRCDataVolumeFromBundle(instance, quantityTestBundle("")); err == nil || !strings.Contains(err.Error(), "template.rootVolumeSize") {
			t.Errorf("BuildCRCDataVolumeFromBundle(rootVolumeSize=%q) error = %v, want a template.rootVolumeSize failure", size, err)
		}
		if _, err := BuildNodePool(instance, "hosted-cluster", "hcp-ns", 1); err == nil || !strings.Contains(err.Error(), "template.rootVolumeSize") {
			t.Errorf("BuildNodePool(rootVolumeSize=%q) error = %v, want a template.rootVolumeSize failure", size, err)
		}
		if _, err := BuildGoldenPVC(quantityTestBundle(size)); err == nil || !strings.Contains(err.Error(), "spec.goldenVolumeSize") {
			t.Errorf("BuildGoldenPVC(goldenVolumeSize=%q) error = %v, want a spec.goldenVolumeSize failure", size, err)
		}
	}
}

// TestBuildersAcceptValidQuantities keeps the accepted values aligned with
// resource.ParseQuantity. The documented examples, and an empty optional
// size (which selects the topology default), must keep working.
func TestBuildersAcceptValidQuantities(t *testing.T) {
	validMemory := []string{"16Gi", "6Gi", "2048Mi", "1.5Gi", "1e3"}
	for _, memory := range validMemory {
		instance := quantityTestInstance(memory, "")
		if _, err := BuildCRCVirtualMachine(instance, "rootdisk"); err != nil {
			t.Errorf("BuildCRCVirtualMachine(memory=%q): %v", memory, err)
		}
		if _, err := BuildNodePool(instance, "hosted-cluster", "hcp-ns", 1); err != nil {
			t.Errorf("BuildNodePool(memory=%q): %v", memory, err)
		}
	}

	validSizes := []string{"", "35Gi", "80Gi", "512Mi", "1.5Ti"}
	for _, size := range validSizes {
		instance := quantityTestInstance("16Gi", size)
		if _, err := BuildCRCDataVolume(instance); err != nil {
			t.Errorf("BuildCRCDataVolume(rootVolumeSize=%q): %v", size, err)
		}
		if _, err := BuildCRCDataVolumeFromBundle(instance, quantityTestBundle("")); err != nil {
			t.Errorf("BuildCRCDataVolumeFromBundle(rootVolumeSize=%q): %v", size, err)
		}
		if _, err := BuildNodePool(instance, "hosted-cluster", "hcp-ns", 1); err != nil {
			t.Errorf("BuildNodePool(rootVolumeSize=%q): %v", size, err)
		}
		if _, err := BuildGoldenPVC(quantityTestBundle(size)); err != nil {
			t.Errorf("BuildGoldenPVC(goldenVolumeSize=%q): %v", size, err)
		}
	}
}
