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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

func TestClusterPoolWaitsForTerminatingInstanceAfterLeaseTTLRelease(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)
	pool := &brokerv1alpha1.ClusterPool{
		ObjectMeta: metav1.ObjectMeta{Name: testTTLPoolName, Namespace: testNamespace},
		Spec: brokerv1alpha1.ClusterPoolSpec{
			Type:    brokerv1alpha1.TopologyCRC,
			MaxSize: 1,
			MinSize: 1,
			Template: brokerv1alpha1.ClusterTemplate{
				OCPVersion: testOCPVersion,
				Memory:     testMemory,
				Cores:      4,
			},
		},
	}
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "ttl-pool-old",
			Namespace:  testNamespace,
			Labels:     resources.PoolLabels(pool.Name),
			Finalizers: []string{instanceFinalizer},
		},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type:    brokerv1alpha1.TopologyCRC,
			PoolRef: corev1.LocalObjectReference{Name: pool.Name},
		},
		Status: brokerv1alpha1.ClusterInstanceStatus{Phase: brokerv1alpha1.PhaseReady},
	}
	boundTime := metav1.NewTime(now.Add(-2 * time.Minute))
	lease := &brokerv1alpha1.ClusterLease{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "ttl-lease",
			Namespace:  testNamespace,
			Finalizers: []string{leaseFinalizer},
		},
		Spec: brokerv1alpha1.ClusterLeaseSpec{
			PoolRef: corev1.LocalObjectReference{Name: pool.Name},
			TTL:     &metav1.Duration{Duration: time.Minute},
		},
		Status: brokerv1alpha1.ClusterLeaseStatus{
			Phase:       brokerv1alpha1.PhaseLeaseBound,
			InstanceRef: &corev1.LocalObjectReference{Name: instance.Name},
			BoundTime:   &boundTime,
		},
	}
	c := newStatusWriteFakeClient(t, pool, instance, lease)
	leaseReconciler := &ClusterLeaseReconciler{
		Client: c,
		Scheme: c.Scheme(),
		Now:    func() time.Time { return now },
	}
	leaseRequest := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(lease)}

	if _, err := leaseReconciler.Reconcile(ctx, leaseRequest); err != nil {
		t.Fatalf("reconciling expired lease TTL: %v", err)
	}
	if _, err := leaseReconciler.Reconcile(ctx, leaseRequest); err != nil {
		t.Fatalf("reconciling TTL lease deletion: %v", err)
	}

	oldInstance := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), oldInstance); err != nil {
		t.Fatalf("getting released ClusterInstance: %v", err)
	}
	if oldInstance.DeletionTimestamp.IsZero() {
		t.Fatal("lease release did not mark ClusterInstance for deletion")
	}
	if oldInstance.Status.Phase != brokerv1alpha1.PhaseReady {
		t.Fatalf("phase after delete request = %q, want Ready to prove accounting uses deletion state", oldInstance.Status.Phase)
	}

	poolRequest := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pool)}
	for pass := range 2 {
		// A new reconciler on each pass models a controller restart: all
		// teardown accounting must come from the API object, not memory.
		poolReconciler := &ClusterPoolReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
		if _, err := poolReconciler.Reconcile(ctx, poolRequest); err != nil {
			t.Fatalf("pool reconcile %d during teardown: %v", pass+1, err)
		}
		instances := &brokerv1alpha1.ClusterInstanceList{}
		if err := c.List(ctx, instances, client.InNamespace(testNamespace), client.MatchingLabels(resources.PoolLabels(pool.Name))); err != nil {
			t.Fatalf("listing instances during teardown: %v", err)
		}
		if len(instances.Items) != 1 {
			t.Fatalf("instance count during teardown = %d, want only the deleting instance", len(instances.Items))
		}
		currentPool := &brokerv1alpha1.ClusterPool{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(pool), currentPool); err != nil {
			t.Fatalf("getting pool status: %v", err)
		}
		if currentPool.Status.TotalInstances != 1 || currentPool.Status.TerminatingInstances != 1 {
			t.Fatalf("pool counts during teardown = total %d, terminating %d; want 1, 1",
				currentPool.Status.TotalInstances, currentPool.Status.TerminatingInstances)
		}
	}

	// The ClusterInstance finalizer is removed only after backing-resource
	// cleanup completes. Removing it here models the final successful teardown
	// reconcile after a delayed VMI/launcher-pod deletion.
	oldInstance.Finalizers = nil
	if err := c.Update(ctx, oldInstance); err != nil {
		t.Fatalf("finishing old instance cleanup: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), &brokerv1alpha1.ClusterInstance{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old ClusterInstance get error = %v, want NotFound after cleanup", err)
	}

	poolReconciler := &ClusterPoolReconciler{Client: c, Scheme: c.Scheme(), APIReader: c}
	if _, err := poolReconciler.Reconcile(ctx, poolRequest); err != nil {
		t.Fatalf("pool reconcile after teardown: %v", err)
	}
	instances := &brokerv1alpha1.ClusterInstanceList{}
	if err := c.List(ctx, instances, client.InNamespace(testNamespace), client.MatchingLabels(resources.PoolLabels(pool.Name))); err != nil {
		t.Fatalf("listing replacement instance: %v", err)
	}
	if len(instances.Items) != 1 || instances.Items[0].Name == instance.Name {
		t.Fatalf("instances after teardown = %v, want one replacement", instanceNames(instances.Items))
	}

	if _, err := poolReconciler.Reconcile(ctx, poolRequest); err != nil {
		t.Fatalf("repeated pool reconcile after replacement: %v", err)
	}
	instances = &brokerv1alpha1.ClusterInstanceList{}
	if err := c.List(ctx, instances, client.InNamespace(testNamespace), client.MatchingLabels(resources.PoolLabels(pool.Name))); err != nil {
		t.Fatalf("listing instances after repeated reconcile: %v", err)
	}
	if len(instances.Items) != 1 {
		t.Fatalf("instance count after repeated reconcile = %d, want 1", len(instances.Items))
	}
	currentPool := &brokerv1alpha1.ClusterPool{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pool), currentPool); err != nil {
		t.Fatalf("getting final pool status: %v", err)
	}
	if currentPool.Status.TotalInstances != 1 || currentPool.Status.TerminatingInstances != 0 {
		t.Fatalf("pool counts after replacement = total %d, terminating %d; want 1, 0",
			currentPool.Status.TotalInstances, currentPool.Status.TerminatingInstances)
	}
}

func instanceNames(instances []brokerv1alpha1.ClusterInstance) []string {
	names := make([]string, 0, len(instances))
	for i := range instances {
		names = append(names, instances[i].Name)
	}
	return names
}
