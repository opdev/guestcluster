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
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
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
	version, arch := testOCPVersion, resources.DefaultCRCArch
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

func TestReconcilePreparingFailsWhenPrepJobIsTerminal(t *testing.T) {
	t.Setenv(resources.OperatorNamespaceEnvVar, "bundle-operator")

	// BuildBundlePrepJob sets BackoffLimit=2 and a two-hour
	// ActiveDeadlineSeconds. The table below covers every terminal shape
	// the Job status can take, plus the in-progress shape the next test
	// guards separately.
	backoffLimit := int32(2)
	tests := []struct {
		name       string
		failed     int32
		conditions []batchv1.JobCondition
		wantReason string
	}{
		{
			// The bug this test guards against: a Job that fails by
			// deadline reports JobFailed=True with Reason=DeadlineExceeded
			// while its Failed pod count stays at or below BackoffLimit,
			// so the previous count-only check never fired and the
			// CRCBundle requeued in Preparing forever.
			name:   "deadline exceeded with failed count below backoff limit",
			failed: 1,
			conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobFailed,
				Status: corev1.ConditionTrue,
				Reason: batchv1.JobReasonDeadlineExceeded,
			}},
			wantReason: batchv1.JobReasonDeadlineExceeded,
		},
		{
			name:   "backoff limit exceeded condition with failed count at limit",
			failed: 2,
			conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobFailed,
				Status: corev1.ConditionTrue,
				Reason: batchv1.JobReasonBackoffLimitExceeded,
			}},
			wantReason: batchv1.JobReasonBackoffLimitExceeded,
		},
		{
			// Fallback for a status whose JobFailed condition lags behind
			// its Failed pod count.
			name:       "failed count beyond backoff limit without condition",
			failed:     3,
			conditions: nil,
			wantReason: batchv1.JobReasonBackoffLimitExceeded,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			version, arch := testOCPVersion, resources.DefaultCRCArch
			bundle := &brokerv1alpha1.CRCBundle{
				ObjectMeta: metav1.ObjectMeta{Name: resources.CRCBundleName(version, arch)},
				Spec:       brokerv1alpha1.CRCBundleSpec{Version: version, Arch: arch},
				Status:     brokerv1alpha1.CRCBundleStatus{Phase: brokerv1alpha1.CRCBundlePhasePreparing},
			}
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resources.BundlePrepJobName(version, arch),
					Namespace: resources.OperatorNamespace(),
				},
				Spec: batchv1.JobSpec{BackoffLimit: &backoffLimit},
				Status: batchv1.JobStatus{
					Failed:     tc.failed,
					Conditions: tc.conditions,
				},
			}
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
				WithObjects(bundle, job).
				Build()
			r := &CRCBundleReconciler{Client: c}

			if _, err := r.reconcilePreparing(ctx, bundle); err != nil {
				t.Fatalf("reconcilePreparing: %v", err)
			}
			got := &brokerv1alpha1.CRCBundle{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(bundle), got); err != nil {
				t.Fatalf("getting CRCBundle: %v", err)
			}
			if got.Status.Phase != brokerv1alpha1.CRCBundlePhaseFailed {
				t.Fatalf("phase = %s, want Failed", got.Status.Phase)
			}
			condition := apimeta.FindStatusCondition(got.Status.Conditions, conditionTypeBundleReady)
			if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "PrepJobFailed" {
				t.Fatalf("expected Ready=False with PrepJobFailed, got %+v", condition)
			}
			if !strings.Contains(condition.Message, tc.wantReason) || !strings.Contains(condition.Message, job.Name) {
				t.Fatalf("condition message = %q, want reason %q and job name %q", condition.Message, tc.wantReason, job.Name)
			}
		})
	}
}

func TestReconcilePreparingRequeuesWhilePrepJobRuns(t *testing.T) {
	t.Setenv(resources.OperatorNamespaceEnvVar, "bundle-operator")
	ctx := context.Background()
	version, arch := testOCPVersion, resources.DefaultCRCArch
	bundle := &brokerv1alpha1.CRCBundle{
		ObjectMeta: metav1.ObjectMeta{Name: resources.CRCBundleName(version, arch)},
		Spec:       brokerv1alpha1.CRCBundleSpec{Version: version, Arch: arch},
		Status:     brokerv1alpha1.CRCBundleStatus{Phase: brokerv1alpha1.CRCBundlePhasePreparing},
	}
	// One failed pod so far: still within BackoffLimit, so the Job is
	// retrying, not terminal. The reconciler must keep waiting, not mark
	// the bundle Failed.
	backoffLimit := int32(2)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resources.BundlePrepJobName(version, arch),
			Namespace: resources.OperatorNamespace(),
		},
		Spec:   batchv1.JobSpec{BackoffLimit: &backoffLimit},
		Status: batchv1.JobStatus{Failed: 1},
	}
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
		WithObjects(bundle, job).
		Build()
	r := &CRCBundleReconciler{Client: c}

	result, err := r.reconcilePreparing(ctx, bundle)
	if err != nil {
		t.Fatalf("reconcilePreparing: %v", err)
	}
	if result.RequeueAfter != crcBundleRequeueInterval {
		t.Fatalf("requeueAfter = %v, want %v", result.RequeueAfter, crcBundleRequeueInterval)
	}
	got := &brokerv1alpha1.CRCBundle{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(bundle), got); err != nil {
		t.Fatalf("getting CRCBundle: %v", err)
	}
	if got.Status.Phase != brokerv1alpha1.CRCBundlePhasePreparing {
		t.Fatalf("phase = %s, want Preparing", got.Status.Phase)
	}
}
