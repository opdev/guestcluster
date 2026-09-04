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

// Distills the ClusterInstance side of the binding-model verification
// matrix: Status.LeaseRef is a read-only, DERIVED projection of whichever
// ClusterLease's Status.InstanceRef (the single source of truth) currently
// names the instance -- see clusterinstance_controller.go's
// reconcileLeaseRefProjection and clusterlease_types.go's InstanceRef doc.
//
// This uses the controller-runtime fake client (not envtest) because
// reconcileLeaseRefProjection depends on the leaseInstanceRefIndexField
// field index, which is normally registered against the manager's cache by
// ClusterLeaseReconciler.SetupWithManager. envtest's plain client.New()
// client (used by the other verification specs in this package) has no
// cache and does not support indexed MatchingFields lookups against custom
// CRD status fields; the fake client's WithIndex lets us register the same
// index hermetically, in-process, with no API server at all -- these tests
// run in milliseconds.

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

func newLeaseRefProjectionFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("adding core scheme: %v", err)
	}
	if err := brokerv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("adding brokerv1alpha1 scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&brokerv1alpha1.ClusterInstance{}, &brokerv1alpha1.ClusterLease{}).
		WithIndex(&brokerv1alpha1.ClusterLease{}, leaseInstanceRefIndexField, func(obj client.Object) []string {
			lease, ok := obj.(*brokerv1alpha1.ClusterLease)
			if !ok || lease.Status.InstanceRef == nil {
				return nil
			}
			return []string{lease.Status.InstanceRef.Name}
		}).
		WithObjects(objs...).
		Build()
}

func newProjectionTestInstance(name string, leaseRef *corev1.LocalObjectReference) *brokerv1alpha1.ClusterInstance {
	return &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  testNamespace,
			Finalizers: []string{instanceFinalizer},
		},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type: brokerv1alpha1.TopologyHCP,
			Template: brokerv1alpha1.ClusterTemplate{
				OCPVersion: testOCPVersion,
				Memory:     testMemory,
				Cores:      4,
			},
		},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			Phase:    brokerv1alpha1.PhaseReady,
			LeaseRef: leaseRef,
		},
	}
}

func TestClusterInstance_LeaseRefProjection_SetWhenClaimed(t *testing.T) {
	ctx := context.Background()

	inst := newProjectionTestInstance("proj-inst-claimed", nil)
	lease := &brokerv1alpha1.ClusterLease{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-lease", Namespace: testNamespace},
		Spec: brokerv1alpha1.ClusterLeaseSpec{
			PoolRef: corev1.LocalObjectReference{Name: "proj-pool"},
		},
		Status: brokerv1alpha1.ClusterLeaseStatus{
			Phase:       brokerv1alpha1.PhaseLeaseBound,
			InstanceRef: &corev1.LocalObjectReference{Name: inst.Name},
		},
	}

	c := newLeaseRefProjectionFakeClient(t, inst, lease)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(inst)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(inst), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.LeaseRef == nil || got.Status.LeaseRef.Name != lease.Name {
		t.Fatalf("expected derived LeaseRef to be %q, got %+v", lease.Name, got.Status.LeaseRef)
	}
}

func TestClusterInstance_LeaseRefProjection_ClearedWhenUnclaimed(t *testing.T) {
	ctx := context.Background()

	// Seed the instance with a stale LeaseRef (as if it were released and
	// this projection hasn't caught up yet) and no claiming lease at all.
	inst := newProjectionTestInstance("proj-inst-unclaimed", &corev1.LocalObjectReference{Name: "stale-lease"})

	c := newLeaseRefProjectionFakeClient(t, inst)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(inst)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(inst), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.LeaseRef != nil {
		t.Fatalf("expected LeaseRef to be cleared, got %+v", got.Status.LeaseRef)
	}
}
