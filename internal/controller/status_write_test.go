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
	"sync/atomic"
	"testing"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

const (
	statusCRCReleaseImage = "quay.io/example/release:latest"
	statusCRCPullSecret   = "pull-secret"
	statusCRCSSHKey       = "bundle-ssh-key"
	statusHCPReleaseImage = "quay.io/openshift-release-dev/ocp-release:4.16.0-x86_64"
	statusHCPPullSecret   = "hcp-pull-secret"
	statusHCPMemory       = "8Gi"
	statusIngressName     = "cluster"
	statusIngressDomain   = "apps.example.test"
)

type countingStatusClient struct {
	client.Client
	statusUpdates atomic.Int32
}

func (c *countingStatusClient) Status() client.SubResourceWriter {
	return &countingStatusWriter{
		SubResourceWriter: c.Client.Status(),
		statusUpdates:     &c.statusUpdates,
	}
}

func (c *countingStatusClient) resetStatusUpdates() {
	c.statusUpdates.Store(0)
}

func (c *countingStatusClient) statusUpdateCount() int32 {
	return c.statusUpdates.Load()
}

type countingStatusWriter struct {
	client.SubResourceWriter
	statusUpdates *atomic.Int32
}

func (w *countingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	w.statusUpdates.Add(1)
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestClusterPoolSkipsUnchangedStatusUpdate(t *testing.T) {
	ctx := context.Background()
	pool := &brokerv1alpha1.ClusterPool{
		ObjectMeta: metav1.ObjectMeta{Name: "status-pool", Namespace: testNamespace},
		Spec: brokerv1alpha1.ClusterPoolSpec{
			Type:    brokerv1alpha1.TopologyCRC,
			MaxSize: 1,
			Template: brokerv1alpha1.ClusterTemplate{
				OCPVersion: testOCPVersion,
				Memory:     testMemory,
				Cores:      4,
			},
		},
	}
	base := newStatusWriteFakeClient(t, pool)
	c := &countingStatusClient{Client: base}
	r := &ClusterPoolReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pool)}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if got := c.statusUpdateCount(); got != 1 {
		t.Fatalf("first status update count = %d, want 1", got)
	}

	c.resetStatusUpdates()
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if got := c.statusUpdateCount(); got != 0 {
		t.Fatalf("second status update count = %d, want 0", got)
	}
}

func TestClusterPoolScaleDownWaitSkipsUnchangedStatusUpdate(t *testing.T) {
	ctx := context.Background()
	pool := &brokerv1alpha1.ClusterPool{
		ObjectMeta: metav1.ObjectMeta{Name: "status-scale-down-pool", Namespace: testNamespace},
		Spec: brokerv1alpha1.ClusterPoolSpec{
			Type:    brokerv1alpha1.TopologyCRC,
			MaxSize: 1,
			Template: brokerv1alpha1.ClusterTemplate{
				OCPVersion: testOCPVersion,
				Memory:     testMemory,
				Cores:      4,
			},
		},
	}
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "status-scale-down-instance",
			Namespace: testNamespace,
			Labels:    resources.PoolLabels(pool.Name),
		},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type:    brokerv1alpha1.TopologyCRC,
			PoolRef: corev1.LocalObjectReference{Name: pool.Name},
			Template: brokerv1alpha1.ClusterTemplate{
				OCPVersion: testOCPVersion,
				Memory:     testMemory,
				Cores:      4,
			},
		},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			Phase: brokerv1alpha1.PhaseReady,
			Conditions: []metav1.Condition{{
				Type:               conditionTypeReady,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(time.Now()),
			}},
		},
	}
	base := newStatusWriteFakeClient(t, pool, instance)
	c := &countingStatusClient{Client: base}
	r := &ClusterPoolReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pool)}

	result, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("first RequeueAfter = %s, want a positive stability-window requeue", result.RequeueAfter)
	}
	if got := c.statusUpdateCount(); got != 1 {
		t.Fatalf("first status update count = %d, want 1", got)
	}

	c.resetStatusUpdates()
	result, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("second RequeueAfter = %s, want a positive stability-window requeue", result.RequeueAfter)
	}
	if got := c.statusUpdateCount(); got != 0 {
		t.Fatalf("second status update count = %d, want 0", got)
	}
}

func TestClusterLeaseSkipsUnchangedPendingStatusUpdate(t *testing.T) {
	ctx := context.Background()
	lease := &brokerv1alpha1.ClusterLease{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "status-lease",
			Namespace:  testNamespace,
			Finalizers: []string{leaseFinalizer},
		},
		Spec: brokerv1alpha1.ClusterLeaseSpec{
			PoolRef: corev1.LocalObjectReference{Name: "status-pool"},
		},
	}
	base := newStatusWriteFakeClient(t, lease)
	c := &countingStatusClient{Client: base}
	r := &ClusterLeaseReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(lease)}

	result, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if result.RequeueAfter != leasePendingRequeue {
		t.Fatalf("first RequeueAfter = %s, want %s", result.RequeueAfter, leasePendingRequeue)
	}
	if got := c.statusUpdateCount(); got != 1 {
		t.Fatalf("first status update count = %d, want 1", got)
	}

	c.resetStatusUpdates()
	result, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if result.RequeueAfter != leasePendingRequeue {
		t.Fatalf("second RequeueAfter = %s, want %s", result.RequeueAfter, leasePendingRequeue)
	}
	if got := c.statusUpdateCount(); got != 0 {
		t.Fatalf("second status update count = %d, want 0", got)
	}
}

func TestClusterInstanceCRCProvisioningSkipsUnchangedStatusUpdate(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "status-crc", Namespace: testNamespace},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type: brokerv1alpha1.TopologyCRC,
			Template: brokerv1alpha1.ClusterTemplate{
				PullSecretRef:   corev1.LocalObjectReference{Name: statusCRCPullSecret},
				BundleSSHKeyRef: &corev1.LocalObjectReference{Name: statusCRCSSHKey},
				ReleaseImage:    statusCRCReleaseImage,
				Memory:          testMemory,
				Cores:           4,
			},
		},
	}
	pullSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: statusCRCPullSecret, Namespace: testNamespace},
		Data:       map[string][]byte{resources.PullSecretDataKey: []byte("pull")},
	}
	sshSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: statusCRCSSHKey, Namespace: testNamespace},
		Data:       map[string][]byte{"id_rsa": []byte("key")},
	}
	base := newCRCRecoveryFakeClient(t, instance, pullSecret, sshSecret)
	c := &countingStatusClient{Client: base}
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	result, err := r.reconcileCRC(ctx, instance)
	if err != nil {
		t.Fatalf("first reconcileCRC: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("first RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	if got := c.statusUpdateCount(); got != 1 {
		t.Fatalf("first status update count = %d, want 1", got)
	}

	current := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), current); err != nil {
		t.Fatalf("getting instance after first reconcile: %v", err)
	}
	c.resetStatusUpdates()
	result, err = r.reconcileCRC(ctx, current)
	if err != nil {
		t.Fatalf("second reconcileCRC: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("second RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	if got := c.statusUpdateCount(); got != 0 {
		t.Fatalf("second status update count = %d, want 0", got)
	}
}

func TestClusterInstanceHyperShiftProvisioningSkipsUnchangedStatusUpdate(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "status-hcp", Namespace: testNamespace},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type: brokerv1alpha1.TopologyHCP,
			Template: brokerv1alpha1.ClusterTemplate{
				OCPVersion:    testOCPVersion,
				ReleaseImage:  statusHCPReleaseImage,
				PullSecretRef: corev1.LocalObjectReference{Name: statusHCPPullSecret},
				Memory:        statusHCPMemory,
				Cores:         2,
			},
		},
	}
	pullSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: statusHCPPullSecret, Namespace: testNamespace},
		Data:       map[string][]byte{resources.PullSecretDataKey: []byte("pull")},
	}
	ingress := &configv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: statusIngressName},
		Spec:       configv1.IngressSpec{Domain: statusIngressDomain},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0"},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.10"}},
		},
	}
	base := newHyperShiftFakeClient(t, instance, pullSecret, ingress, node)
	c := &countingStatusClient{Client: base}
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	result, err := r.reconcileHyperShift(ctx, instance)
	if err != nil {
		t.Fatalf("first reconcileHyperShift: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("first RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	if got := c.statusUpdateCount(); got != 1 {
		t.Fatalf("first status update count = %d, want 1", got)
	}

	current := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), current); err != nil {
		t.Fatalf("getting instance after first reconcile: %v", err)
	}
	c.resetStatusUpdates()
	result, err = r.reconcileHyperShift(ctx, current)
	if err != nil {
		t.Fatalf("second reconcileHyperShift: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("second RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	if got := c.statusUpdateCount(); got != 0 {
		t.Fatalf("second status update count = %d, want 0", got)
	}
}

func TestClusterInstanceRepeatedFailureSkipsUnchangedStatusUpdate(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "status-failed", Namespace: testNamespace},
	}
	base := newStatusWriteFakeClient(t, instance)
	c := &countingStatusClient{Client: base}
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	cause := context.Canceled

	if _, err := r.markFailedWithReason(ctx, instance, "TestFailure", cause); err != cause {
		t.Fatalf("first failure error = %v, want %v", err, cause)
	}
	if got := c.statusUpdateCount(); got != 1 {
		t.Fatalf("first status update count = %d, want 1", got)
	}

	c.resetStatusUpdates()
	if _, err := r.markFailedWithReason(ctx, instance, "TestFailure", cause); err != cause {
		t.Fatalf("second failure error = %v, want %v", err, cause)
	}
	if got := c.statusUpdateCount(); got != 0 {
		t.Fatalf("second status update count = %d, want 0", got)
	}
}

func newStatusWriteFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("adding core scheme: %v", err)
	}
	if err := brokerv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("adding GuestCluster scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(
			&brokerv1alpha1.ClusterPool{},
			&brokerv1alpha1.ClusterLease{},
			&brokerv1alpha1.ClusterInstance{},
		).
		WithIndex(&brokerv1alpha1.ClusterLease{}, leaseInstanceRefIndexField, func(obj client.Object) []string {
			lease, ok := obj.(*brokerv1alpha1.ClusterLease)
			if !ok || lease.Status.InstanceRef == nil {
				return nil
			}
			return []string{lease.Status.InstanceRef.Name}
		}).WithObjects(objects...).Build()
}
