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

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
)

func TestReconcileReadyRestoresDiskImagePVCReferences(t *testing.T) {
	t.Setenv(resources.OperatorNamespaceEnvVar, "bundle-operator")
	ctx := context.Background()
	version, arch := testOCPVersion, "amd64"
	bundle := &brokerv1alpha1.CRCBundle{
		ObjectMeta: metav1.ObjectMeta{Name: resources.CRCBundleName(version, arch)},
		Spec:       brokerv1alpha1.CRCBundleSpec{Version: version, Arch: arch},
		Status: brokerv1alpha1.CRCBundleStatus{
			Phase: brokerv1alpha1.CRCBundlePhaseReady,
		},
	}
	goldenPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: resources.GoldenPVCName(version, arch), Namespace: resources.OperatorNamespace(),
	}}
	sshSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: resources.BundleSSHKeySecretName(version, arch), Namespace: resources.OperatorNamespace(),
	}}
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("adding core scheme: %v", err)
	}
	if err := brokerv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("adding CRCBundle scheme: %v", err)
	}
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&brokerv1alpha1.CRCBundle{}).
		WithObjects(bundle, goldenPVC, sshSecret).
		Build()
	r := &CRCBundleReconciler{Client: c}

	if _, err := r.reconcileReady(ctx, bundle); err != nil {
		t.Fatalf("reconcileReady: %v", err)
	}
	got := &brokerv1alpha1.CRCBundle{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(bundle), got); err != nil {
		t.Fatalf("getting CRCBundle: %v", err)
	}
	if got.Status.DiskImagePVCRef == nil || got.Status.DiskImagePVCRef.Name != goldenPVC.Name {
		t.Fatalf("disk image PVC ref = %#v, want %q", got.Status.DiskImagePVCRef, goldenPVC.Name)
	}
	if got.Status.DiskImagePVCNamespace != goldenPVC.Namespace {
		t.Fatalf("disk image PVC namespace = %q, want %q", got.Status.DiskImagePVCNamespace, goldenPVC.Namespace)
	}
}
