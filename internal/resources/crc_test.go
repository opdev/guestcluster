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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

func TestBuildCRCDataVolumeFromBundleUsesCrossNamespaceGoldenPVC(t *testing.T) {
	const (
		instanceNamespace = "crc-pool-ns"
		bundleNamespace   = "guestcluster-operator-system"
		goldenPVCName     = "crc-4-16-0-amd64-golden"
	)

	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "crc-instance", Namespace: instanceNamespace},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Template: brokerv1alpha1.ClusterTemplate{RootVolumeSize: "80Gi"},
		},
	}
	bundle := &brokerv1alpha1.CRCBundle{
		ObjectMeta: metav1.ObjectMeta{Name: "crc-4-16-0-amd64"},
		Status: brokerv1alpha1.CRCBundleStatus{
			Phase:             brokerv1alpha1.CRCBundlePhaseReady,
			QCOW2PVCRef:       &corev1.LocalObjectReference{Name: goldenPVCName},
			QCOW2PVCNamespace: bundleNamespace,
		},
	}

	dv := BuildCRCDataVolumeFromBundle(instance, bundle)
	if dv.Namespace != instanceNamespace {
		t.Fatalf("DataVolume namespace = %q, want instance namespace %q", dv.Namespace, instanceNamespace)
	}
	if dv.Spec.Source == nil || dv.Spec.Source.PVC == nil {
		t.Fatal("DataVolume source is not a PVC clone")
	}
	if got := dv.Spec.Source.PVC.Namespace; got != bundleNamespace {
		t.Errorf("source PVC namespace = %q, want CRCBundle namespace %q", got, bundleNamespace)
	}
	if got := dv.Spec.Source.PVC.Name; got != goldenPVCName {
		t.Errorf("source PVC name = %q, want CRCBundle golden PVC %q", got, goldenPVCName)
	}
}
