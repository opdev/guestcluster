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
	"time"

	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

const (
	operandNotFoundReason = "OperandNotFound"
	apiVersionField       = "apiVersion"
	kindField             = "kind"
	metadataField         = "metadata"
	nameField             = "name"
	statusField           = "status"
	conditionTypeField    = "type"
)

func TestCheckHyperConverged(t *testing.T) {
	tests := []struct {
		name       string
		objects    []client.Object
		wantReady  bool
		wantReason string
	}{
		{name: "missing operand", wantReason: operandNotFoundReason},
		{name: "multiple operands", objects: []client.Object{healthyHCO("one"), healthyHCO("two")}, wantReason: "MultipleOperandsFound"},
		{name: "stale status", objects: []client.Object{hco("hco", 2, 1, "True", "False", "False")}, wantReason: "StatusStale"},
		{name: "degraded", objects: []client.Object{hco("hco", 1, 1, "True", "False", "True")}, wantReason: "OperandNotReady"},
		{name: "healthy", objects: []client.Object{healthyHCO("hco")}, wantReady: true, wantReason: "OperandReady"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPlatformFakeClient(t, tt.objects...)
			r := &ClusterInstanceReconciler{Client: c, APIReader: c}
			got := r.checkHyperConverged(context.Background())
			if got.ready != tt.wantReady || got.condition.Reason != tt.wantReason {
				t.Fatalf("checkHyperConverged() = ready %t, reason %q; want ready %t, reason %q", got.ready, got.condition.Reason, tt.wantReady, tt.wantReason)
			}
		})
	}
}

func TestCheckMultiClusterEngine(t *testing.T) {
	tests := []struct {
		name       string
		objects    []client.Object
		wantReady  bool
		wantReason string
	}{
		{name: "missing operand", wantReason: "OperandNotFound"},
		{name: "component disabled", objects: []client.Object{mce(false)}, wantReason: "ComponentDisabled"},
		{name: "missing API", objects: []client.Object{mce(true)}, wantReason: "APIUnavailable"},
		{name: "operator unavailable", objects: append(hyperShiftAPIs(), mce(true)), wantReason: "APIUnavailable"},
		{name: "aggregate progressing does not block", objects: append(append(hyperShiftAPIs(), mce(true)), availableHyperShiftOperator()), wantReady: true, wantReason: "OperandReady"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPlatformFakeClient(t, tt.objects...)
			r := &ClusterInstanceReconciler{Client: c, APIReader: c}
			got := r.checkMultiClusterEngine(context.Background())
			if got.ready != tt.wantReady || got.condition.Reason != tt.wantReason {
				t.Fatalf("checkMultiClusterEngine() = ready %t, reason %q; want ready %t, reason %q", got.ready, got.condition.Reason, tt.wantReady, tt.wantReason)
			}
		})
	}
}

func TestReconcileBlocksHCPBeforeProviderSideEffects(t *testing.T) {
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "blocked-hcp", Namespace: testNamespace},
		Spec:       brokerv1alpha1.ClusterInstanceSpec{Type: brokerv1alpha1.TopologyHCP},
	}
	c := newPlatformFakeClient(t, instance, healthyHCO("hco"))
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(instance)}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("adding finalizer: %v", err)
	}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("blocking reconcile: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}

	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatalf("getting ClusterInstance: %v", err)
	}
	if got.Status.Phase != brokerv1alpha1.PhaseProvisioning {
		t.Fatalf("Phase = %q, want %q", got.Status.Phase, brokerv1alpha1.PhaseProvisioning)
	}
	if condition := apimeta.FindStatusCondition(got.Status.Conditions, conditionTypeMultiClusterEngineReady); condition == nil || condition.Reason != operandNotFoundReason {
		t.Fatalf("MultiClusterEngineReady = %+v, want OperandNotFound", condition)
	}
	namespace := &corev1.Namespace{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "clusters"}, namespace); err == nil {
		t.Fatal("HCP reconcile created the clusters namespace while MCE was blocked")
	}
}

func TestGatePlatformOperationsDoesNotRequireMCEForCRC(t *testing.T) {
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "crc", Namespace: testNamespace},
		Spec:       brokerv1alpha1.ClusterInstanceSpec{Type: brokerv1alpha1.TopologyCRC},
	}
	c := newPlatformFakeClient(t, instance, healthyHCO("hco"))
	r := &ClusterInstanceReconciler{Client: c, APIReader: c}
	result, err := r.gatePlatformOperations(context.Background(), instance)
	if err != nil || result == nil || result.RequeueAfter != 0 {
		t.Fatalf("gatePlatformOperations() = (%+v, %v), want an immediate successful requeue", result, err)
	}
	if condition := apimeta.FindStatusCondition(instance.Status.Conditions, conditionTypeHyperConvergedReady); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("HyperConvergedReady = %+v, want True", condition)
	}
}

func TestReconcileReadyHCPProjectsLeaseWhenDependenciesFail(t *testing.T) {
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "ready-hcp", Namespace: testNamespace, Finalizers: []string{instanceFinalizer}},
		Spec:       brokerv1alpha1.ClusterInstanceSpec{Type: brokerv1alpha1.TopologyHCP},
		Status:     brokerv1alpha1.ClusterInstanceStatus{Phase: brokerv1alpha1.PhaseReady},
	}
	lease := &brokerv1alpha1.ClusterLease{
		ObjectMeta: metav1.ObjectMeta{Name: "lease", Namespace: testNamespace},
		Status:     brokerv1alpha1.ClusterLeaseStatus{InstanceRef: &corev1.LocalObjectReference{Name: instance.Name}},
	}
	c := newPlatformFakeClient(t, instance, lease)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(instance)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatalf("getting ClusterInstance: %v", err)
	}
	if got.Status.LeaseRef == nil || got.Status.LeaseRef.Name != lease.Name {
		t.Fatalf("LeaseRef = %+v, want %q", got.Status.LeaseRef, lease.Name)
	}
}

func TestReconcileDeletionBypassesPlatformGate(t *testing.T) {
	deletionTime := metav1.NewTime(time.Now())
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "deleting-hcp", Namespace: testNamespace, Finalizers: []string{instanceFinalizer}, DeletionTimestamp: &deletionTime},
		Spec:       brokerv1alpha1.ClusterInstanceSpec{Type: brokerv1alpha1.TopologyHCP},
	}
	c := newPlatformFakeClient(t, instance)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(instance)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(instance), got); err == nil {
		t.Fatal("ClusterInstance still exists after deletion")
	}
}

func TestReconcileDoesNotResetFailedInstanceWhenPlatformIsUnavailable(t *testing.T) {
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "failed", Namespace: testNamespace, Finalizers: []string{instanceFinalizer}},
		Spec:       brokerv1alpha1.ClusterInstanceSpec{Type: brokerv1alpha1.TopologyCRC},
		Status:     brokerv1alpha1.ClusterInstanceStatus{Phase: brokerv1alpha1.PhaseFailed},
	}
	c := newPlatformFakeClient(t, instance)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(instance)}); err == nil {
		t.Fatal("Reconcile succeeded without the required CRC pull secret")
	}
	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatalf("getting ClusterInstance: %v", err)
	}
	if got.Status.Phase != brokerv1alpha1.PhaseFailed {
		t.Fatalf("Phase = %q, want %q", got.Status.Phase, brokerv1alpha1.PhaseFailed)
	}
}

func newPlatformFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("adding core scheme: %v", err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("adding apps scheme: %v", err)
	}
	if err := brokerv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("adding GuestCluster scheme: %v", err)
	}
	if err := hyperv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("adding HyperShift scheme: %v", err)
	}
	for _, gvk := range []schema.GroupVersionKind{hyperConvergedGVK, multiClusterEngineGVK, customResourceDefinitionGVK} {
		s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &unstructured.UnstructuredList{})
	}
	return fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&brokerv1alpha1.ClusterInstance{}, &brokerv1alpha1.ClusterLease{}).
		WithIndex(&brokerv1alpha1.ClusterLease{}, leaseInstanceRefIndexField, func(obj client.Object) []string {
			lease, ok := obj.(*brokerv1alpha1.ClusterLease)
			if !ok || lease.Status.InstanceRef == nil {
				return nil
			}
			return []string{lease.Status.InstanceRef.Name}
		}).WithObjects(objects...).Build()
}

func healthyHCO(name string) *unstructured.Unstructured {
	return hco(name, 1, 1, "True", "False", "False")
}

func hco(name string, generation, observedGeneration int64, available, progressing, degraded string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		apiVersionField: hyperConvergedGVK.GroupVersion().String(), kindField: hyperConvergedGVK.Kind,
		metadataField: map[string]interface{}{nameField: name, "generation": generation},
		statusField: map[string]interface{}{"observedGeneration": observedGeneration, "conditions": []interface{}{
			map[string]interface{}{conditionTypeField: "Available", statusField: available},
			map[string]interface{}{conditionTypeField: "Progressing", statusField: progressing},
			map[string]interface{}{conditionTypeField: "Degraded", statusField: degraded},
		}},
	}}
}

func mce(enabled bool) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		apiVersionField: multiClusterEngineGVK.GroupVersion().String(), kindField: multiClusterEngineGVK.Kind,
		metadataField: map[string]interface{}{nameField: "engine"},
		"spec":        map[string]interface{}{"overrides": map[string]interface{}{"components": []interface{}{map[string]interface{}{nameField: "hypershift", "enabled": enabled}}}},
		statusField:   map[string]interface{}{"phase": "Progressing"},
	}}
}

func hyperShiftAPIs() []client.Object {
	return []client.Object{establishedCRD("hostedclusters." + hyperShiftGroup), establishedCRD("nodepools." + hyperShiftGroup)}
}

func establishedCRD(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		apiVersionField: customResourceDefinitionGVK.GroupVersion().String(), kindField: customResourceDefinitionGVK.Kind,
		metadataField: map[string]interface{}{nameField: name},
		statusField:   map[string]interface{}{"conditions": []interface{}{map[string]interface{}{conditionTypeField: "Established", statusField: "True"}}},
	}}
}

func availableHyperShiftOperator() *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "operator", Namespace: "hypershift"}, Status: appsv1.DeploymentStatus{AvailableReplicas: 1}}
}
