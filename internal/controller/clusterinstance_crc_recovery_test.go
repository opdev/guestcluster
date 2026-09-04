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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	routev1 "github.com/openshift/api/route/v1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

const (
	recoveryInstanceName = "crc-recovery"
	oldAPIEndpoint       = "https://old.example.test"
	recoveryVMIUID       = "vmi-uid"
)

func TestCRCVMIDChanged(t *testing.T) {
	const oldUID = "old"
	tests := []struct {
		name     string
		previous string
		current  string
		changed  bool
	}{
		{name: "same UID", previous: oldUID, current: oldUID, changed: false},
		{name: "replacement", previous: oldUID, current: "new", changed: true},
		{name: "unrecorded UID", current: "new", changed: true},
		{name: "missing current UID", previous: oldUID, changed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := crcVMIChanged(tt.previous, tt.current); got != tt.changed {
				t.Fatalf("crcVMIChanged(%q, %q) = %t, want %t", tt.previous, tt.current, got, tt.changed)
			}
		})
	}
}

func TestReconcileReadyCRC_VMIReplacementRemovesPreviousHandoff(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: recoveryInstanceName, Namespace: testNamespace},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			Phase:               brokerv1alpha1.PhaseReady,
			APIEndpoint:         oldAPIEndpoint,
			KubeconfigSecretRef: corev1.LocalObjectReference{Name: resources.KubeconfigSecretName(recoveryInstanceName)},
			CRC:                 &brokerv1alpha1.CRCBackingStatus{VMName: recoveryInstanceName, DataVolumeName: recoveryInstanceName + "-rootdisk", VMIUID: "old-vmi"},
		},
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentJobName(instance.Name, "old-vmi"), Namespace: instance.Namespace}}
	raw := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.RawKubeconfigSecretNameForVMI(instance.Name, "old-vmi"), Namespace: instance.Namespace}}
	published := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeconfigSecretName(instance.Name), Namespace: instance.Namespace}}
	identity := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCIdentitySecretName(instance.Name), Namespace: instance.Namespace}}
	vmi := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{Name: instance.Name, Namespace: instance.Namespace, UID: types.UID("new-vmi")}}
	c := newCRCRecoveryFakeClient(t, instance, job, raw, published, identity, vmi)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	if _, err := r.reconcileReadyCRC(ctx, instance); err != nil {
		t.Fatalf("reconcileReadyCRC: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(identity), identity); err != nil {
		t.Fatalf("expected CRC identity to survive VMI replacement: %v", err)
	}
	for _, obj := range []client.Object{job, published} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err == nil {
			t.Fatalf("expected %T %s to be deleted", obj, client.ObjectKeyFromObject(obj))
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(raw), raw); err != nil {
		t.Fatalf("expected retained raw handoff: %v", err)
	}

	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatalf("getting instance: %v", err)
	}
	if got.Status.Phase != brokerv1alpha1.PhaseProvisioning || got.Status.CRC.VMIUID != "new-vmi" {
		t.Fatalf("expected Provisioning with new VMI UID, got %+v", got.Status)
	}
	if got.Status.APIEndpoint != "" || got.Status.KubeconfigSecretRef.Name != "" {
		t.Fatalf("expected cleared published access details, got %+v", got.Status)
	}
}

func TestReconcileReadyCRCRequeuesHealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/readyz" {
			t.Errorf("request path = %q, want /readyz", request.URL.Path)
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "crc-ready", Namespace: testNamespace},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			Phase: brokerv1alpha1.PhaseReady,
			CRC:   &brokerv1alpha1.CRCBackingStatus{VMIUID: recoveryVMIUID},
		},
	}
	vmi := &kubevirtv1.VirtualMachineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: instance.Name, Namespace: instance.Namespace, UID: types.UID(recoveryVMIUID)},
		Status:     kubevirtv1.VirtualMachineInstanceStatus{Phase: kubevirtv1.Running},
	}
	published := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: resources.KubeconfigSecretName(instance.Name), Namespace: instance.Namespace},
		Data: map[string][]byte{resources.KubeconfigSecretKey: []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- cluster:
    server: %s
    insecure-skip-tls-verify: true
  name: guest
contexts:
- context:
    cluster: guest
  name: guest
current-context: guest
`, server.URL))},
	}
	c := newCRCRecoveryFakeClient(t, instance, vmi, published)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	result, err := r.reconcileReadyCRC(context.Background(), instance)
	if err != nil {
		t.Fatalf("reconcileReadyCRC: %v", err)
	}
	if result.RequeueAfter != crcReadyRequeueInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, crcReadyRequeueInterval)
	}
}

func TestReconcileReadyCRCRemovesLeaseEligibilityWhenHealthCheckFails(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: recoveryInstanceName, Namespace: testNamespace},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			Phase:               brokerv1alpha1.PhaseReady,
			APIEndpoint:         oldAPIEndpoint,
			KubeconfigSecretRef: corev1.LocalObjectReference{Name: resources.KubeconfigSecretName(recoveryInstanceName)},
			CRC:                 &brokerv1alpha1.CRCBackingStatus{VMIUID: recoveryVMIUID},
		},
	}
	vmi := &kubevirtv1.VirtualMachineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: instance.Name, Namespace: instance.Namespace, UID: types.UID(recoveryVMIUID)},
		Status:     kubevirtv1.VirtualMachineInstanceStatus{Phase: kubevirtv1.Running},
	}
	published := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: resources.KubeconfigSecretName(instance.Name), Namespace: instance.Namespace},
		Data:       map[string][]byte{resources.KubeconfigSecretKey: []byte("invalid")},
	}
	raw := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: resources.RawKubeconfigSecretNameForVMI(instance.Name, recoveryVMIUID), Namespace: instance.Namespace},
		Data:       map[string][]byte{resources.KubeconfigSecretKey: []byte("retained")},
	}
	c := newCRCRecoveryFakeClient(t, instance, vmi, published, raw)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	result, err := r.reconcileReadyCRC(ctx, instance)
	if err != nil {
		t.Fatalf("reconcileReadyCRC: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	for _, obj := range []client.Object{published, raw} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Fatalf("expected %T to be retained: %v", obj, err)
		}
	}
	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatalf("getting instance: %v", err)
	}
	if got.Status.Phase != brokerv1alpha1.PhaseProvisioning {
		t.Fatalf("phase = %s, want Provisioning", got.Status.Phase)
	}
	if got.Status.KubeconfigSecretRef.Name != "" {
		t.Fatalf("kubeconfig reference = %q, want empty", got.Status.KubeconfigSecretRef.Name)
	}
	readyCondition := apimeta.FindStatusCondition(got.Status.Conditions, conditionTypeReady)
	if readyCondition == nil || readyCondition.Status != metav1.ConditionFalse {
		t.Fatalf("expected Ready=False, got %+v", readyCondition)
	}
	condition := apimeta.FindStatusCondition(got.Status.Conditions, conditionTypeGuestAPIReachable)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("expected GuestAPIReachable=False, got %+v", condition)
	}
}

func TestReconcileReadyCRCDoesNotRestoreKubeconfigBeforeHealthCheck(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: recoveryInstanceName, Namespace: testNamespace},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			Phase:               brokerv1alpha1.PhaseReady,
			APIEndpoint:         oldAPIEndpoint,
			KubeconfigSecretRef: corev1.LocalObjectReference{Name: resources.KubeconfigSecretName(recoveryInstanceName)},
			CRC:                 &brokerv1alpha1.CRCBackingStatus{VMIUID: recoveryVMIUID},
		},
	}
	vmi := &kubevirtv1.VirtualMachineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: instance.Name, Namespace: instance.Namespace, UID: types.UID(recoveryVMIUID)},
		Status:     kubevirtv1.VirtualMachineInstanceStatus{Phase: kubevirtv1.Running},
	}
	raw := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: resources.RawKubeconfigSecretNameForVMI(instance.Name, recoveryVMIUID), Namespace: instance.Namespace},
		Data: map[string][]byte{
			resources.KubeconfigSecretKey: []byte("invalid"),
			resources.VMIUIDSecretKey:     []byte(recoveryVMIUID),
		},
	}
	c := newCRCRecoveryFakeClient(t, instance, vmi, raw)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	result, err := r.reconcileReadyCRC(ctx, instance)
	if err != nil {
		t.Fatalf("reconcileReadyCRC: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(raw), raw); err != nil {
		t.Fatalf("expected raw handoff to be retained: %v", err)
	}
	published := &corev1.Secret{}
	publishedKey := types.NamespacedName{Name: resources.KubeconfigSecretName(instance.Name), Namespace: instance.Namespace}
	if err := c.Get(ctx, publishedKey, published); err == nil {
		t.Fatalf("expected published kubeconfig to remain absent")
	}
	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatalf("getting instance: %v", err)
	}
	if got.Status.Phase != brokerv1alpha1.PhaseProvisioning {
		t.Fatalf("phase = %s, want Provisioning", got.Status.Phase)
	}
	if got.Status.KubeconfigSecretRef.Name != "" {
		t.Fatalf("kubeconfig reference = %q, want empty", got.Status.KubeconfigSecretRef.Name)
	}
	for _, conditionType := range []string{conditionTypeReady, conditionTypeGuestAPIReachable} {
		condition := apimeta.FindStatusCondition(got.Status.Conditions, conditionType)
		if condition == nil || condition.Status != metav1.ConditionFalse {
			t.Fatalf("expected %s=False, got %+v", conditionType, condition)
		}
	}
}

func TestTeardownCRCBackingDeletesAllVMIHandoffs(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: recoveryInstanceName, Namespace: testNamespace},
	}
	oldJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.CRCAgentJobName(instance.Name, "old-vmi"),
		Namespace: instance.Namespace,
		Labels:    resources.CommonLabels(instance),
	}}
	currentJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.CRCAgentJobName(instance.Name, "current-vmi"),
		Namespace: instance.Namespace,
		Labels:    resources.CommonLabels(instance),
	}}
	rawLabels := map[string]string{resources.LabelManagedBy: "crc-agent", resources.LabelInstance: instance.Name}
	oldRaw := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.RawKubeconfigSecretNameForVMI(instance.Name, "old-vmi"),
		Namespace: instance.Namespace,
		Labels:    rawLabels,
	}}
	currentRaw := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.RawKubeconfigSecretNameForVMI(instance.Name, "current-vmi"),
		Namespace: instance.Namespace,
		Labels:    rawLabels,
	}}
	c := newCRCRecoveryFakeClient(t, instance, oldJob, currentJob, oldRaw, currentRaw)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	if err := r.teardownCRCBacking(ctx, instance); err != nil {
		t.Fatalf("teardownCRCBacking: %v", err)
	}
	for _, obj := range []client.Object{oldJob, currentJob, oldRaw, currentRaw} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err == nil {
			t.Fatalf("expected %T %s to be deleted", obj, client.ObjectKeyFromObject(obj))
		}
	}
}

func TestReconcileProvisioningCRCVMI_VMIReplacementRemovesPreviousHandoff(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: recoveryInstanceName, Namespace: testNamespace},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			Phase:               brokerv1alpha1.PhaseProvisioning,
			APIEndpoint:         oldAPIEndpoint,
			KubeconfigSecretRef: corev1.LocalObjectReference{Name: resources.KubeconfigSecretName(recoveryInstanceName)},
			CRC:                 &brokerv1alpha1.CRCBackingStatus{VMName: recoveryInstanceName, DataVolumeName: recoveryInstanceName + "-rootdisk", VMIUID: "old-vmi"},
		},
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentJobName(instance.Name, "old-vmi"), Namespace: instance.Namespace}}
	raw := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.RawKubeconfigSecretNameForVMI(instance.Name, "old-vmi"), Namespace: instance.Namespace}}
	published := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeconfigSecretName(instance.Name), Namespace: instance.Namespace}}
	identity := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCIdentitySecretName(instance.Name), Namespace: instance.Namespace}}
	vmi := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{Name: instance.Name, Namespace: instance.Namespace, UID: types.UID("new-vmi")}}
	c := newCRCRecoveryFakeClient(t, instance, job, raw, published, identity, vmi)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	result, err := r.reconcileProvisioningCRCVMI(ctx, instance)
	if err != nil {
		t.Fatalf("reconcileProvisioningCRCVMI: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(identity), identity); err != nil {
		t.Fatalf("expected CRC identity to survive VMI replacement: %v", err)
	}
	if result == nil || result.RequeueAfter != requeueInterval {
		t.Fatalf("expected a recovery requeue, got %+v", result)
	}
	for _, obj := range []client.Object{job, published} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err == nil {
			t.Fatalf("expected %T %s to be deleted", obj, client.ObjectKeyFromObject(obj))
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(raw), raw); err != nil {
		t.Fatalf("expected retained raw handoff: %v", err)
	}

	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatalf("getting instance: %v", err)
	}
	if got.Status.Phase != brokerv1alpha1.PhaseProvisioning || got.Status.CRC.VMIUID != "new-vmi" {
		t.Fatalf("expected Provisioning with new VMI UID, got %+v", got.Status)
	}
}

func TestReconcileProvisioningCRCVMI_UnrecordedVMIUIDDoesNotInvalidateHandoff(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: recoveryInstanceName, Namespace: testNamespace},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			Phase: brokerv1alpha1.PhaseProvisioning,
			CRC:   &brokerv1alpha1.CRCBackingStatus{VMName: recoveryInstanceName, DataVolumeName: recoveryInstanceName + "-rootdisk"},
		},
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentJobName(instance.Name, "vmi"), Namespace: instance.Namespace}}
	vmi := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{Name: instance.Name, Namespace: instance.Namespace, UID: types.UID("vmi")}}
	c := newCRCRecoveryFakeClient(t, instance, job, vmi)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	result, err := r.reconcileProvisioningCRCVMI(ctx, instance)
	if err != nil {
		t.Fatalf("reconcileProvisioningCRCVMI: %v", err)
	}
	if result != nil {
		t.Fatalf("expected normal provisioning to continue, got %+v", result)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
		t.Fatalf("expected crc-agent Job to be preserved: %v", err)
	}
}

func TestCheckCRCKubeconfigHandoffRejectsDifferentVMI(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: recoveryInstanceName, Namespace: testNamespace},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			CRC: &brokerv1alpha1.CRCBackingStatus{VMIUID: "current-vmi"},
		},
	}
	raw := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: resources.RawKubeconfigSecretNameForVMI(instance.Name, "current-vmi"), Namespace: instance.Namespace},
		Data: map[string][]byte{
			resources.KubeconfigSecretKey: []byte("stale-kubeconfig"),
			resources.VMIUIDSecretKey:     []byte("old-vmi"),
		},
	}
	c := newCRCRecoveryFakeClient(t, instance, raw)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	kubeconfig, _, err := r.checkCRCKubeconfigHandoff(ctx, instance)
	if err != nil {
		t.Fatalf("checkCRCKubeconfigHandoff: %v", err)
	}
	if kubeconfig != nil {
		t.Fatalf("kubeconfig = %q, want stale handoff rejected", kubeconfig)
	}
}

func newCRCRecoveryFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("adding core scheme: %v", err)
	}
	if err := brokerv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("adding broker scheme: %v", err)
	}
	if err := kubevirtv1.AddToScheme(s); err != nil {
		t.Fatalf("adding KubeVirt scheme: %v", err)
	}
	if err := cdiv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("adding CDI scheme: %v", err)
	}
	if err := routev1.AddToScheme(s); err != nil {
		t.Fatalf("adding OpenShift Route scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&brokerv1alpha1.ClusterInstance{}).
		WithIndex(&brokerv1alpha1.ClusterLease{}, leaseInstanceRefIndexField, func(obj client.Object) []string {
			lease, ok := obj.(*brokerv1alpha1.ClusterLease)
			if !ok || lease.Status.InstanceRef == nil {
				return nil
			}
			return []string{lease.Status.InstanceRef.Name}
		}).
		WithObjects(objects...).
		Build()
}
