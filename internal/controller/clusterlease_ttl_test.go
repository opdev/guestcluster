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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

func TestClusterLeaseReconcileBoundSchedulesAtTTLDeadline(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	boundTime := metav1.NewTime(now.Add(-time.Minute))

	tests := []struct {
		name             string
		ttl              *metav1.Duration
		wantRequeueAfter time.Duration
		wantDelete       bool
	}{
		{
			name:             "future deadline",
			ttl:              &metav1.Duration{Duration: 5 * time.Minute},
			wantRequeueAfter: 4 * time.Minute,
		},
		{
			name:       "expired deadline",
			ttl:        &metav1.Duration{Duration: 30 * time.Second},
			wantDelete: true,
		},
		{
			name: "zero disables expiry",
			ttl:  &metav1.Duration{Duration: 0},
		},
		{
			name: "omitted disables expiry",
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lease := &brokerv1alpha1.ClusterLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:       fmt.Sprintf("ttl-%d", i),
					Namespace:  testNamespace,
					Finalizers: []string{leaseFinalizer},
				},
				Spec: brokerv1alpha1.ClusterLeaseSpec{
					PoolRef: corev1.LocalObjectReference{Name: testTTLPoolName},
					TTL:     tt.ttl,
				},
				Status: brokerv1alpha1.ClusterLeaseStatus{
					Phase:     brokerv1alpha1.PhaseLeaseBound,
					BoundTime: &boundTime,
				},
			}
			c := newStatusWriteFakeClient(t, lease)
			r := &ClusterLeaseReconciler{Client: c, Scheme: c.Scheme(), Now: func() time.Time { return now }}

			result, err := r.reconcileBound(ctx, lease)
			if err != nil {
				t.Fatalf("reconcileBound: %v", err)
			}
			if result.RequeueAfter != tt.wantRequeueAfter {
				t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, tt.wantRequeueAfter)
			}

			stored := &brokerv1alpha1.ClusterLease{}
			err = c.Get(ctx, client.ObjectKeyFromObject(lease), stored)
			if tt.wantDelete {
				if err != nil {
					t.Fatalf("getting expired lease: %v", err)
				}
				if stored.DeletionTimestamp.IsZero() {
					t.Fatal("expired lease was not marked for deletion")
				}
			} else if err != nil {
				t.Fatalf("getting non-expired lease: %v", err)
			}
		})
	}
}

func TestClusterLeaseBindUsesInjectedClock(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.February, 3, 4, 5, 6, 0, time.UTC)
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "clock-instance", Namespace: testNamespace},
		Status: brokerv1alpha1.ClusterInstanceStatus{
			KubeconfigSecretRef: corev1.LocalObjectReference{Name: "clock-instance-kubeconfig"},
			OCPVersion:          testOCPVersion,
			Topology:            brokerv1alpha1.TopologyCRC,
			APIEndpoint:         "https://api.example.test",
		},
	}
	lease := &brokerv1alpha1.ClusterLease{
		ObjectMeta: metav1.ObjectMeta{Name: "clock-lease", Namespace: testNamespace},
		Spec: brokerv1alpha1.ClusterLeaseSpec{
			PoolRef: corev1.LocalObjectReference{Name: "clock-pool"},
		},
	}
	instanceSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "clock-instance-kubeconfig", Namespace: testNamespace},
		Data:       map[string][]byte{resources.KubeconfigSecretKey: []byte("kubeconfig")},
	}
	c := newStatusWriteFakeClient(t, instance, lease, instanceSecret)
	r := &ClusterLeaseReconciler{Client: c, Scheme: c.Scheme(), Now: func() time.Time { return now }}

	if _, err := r.bind(ctx, lease, instance); err != nil {
		t.Fatalf("bind: %v", err)
	}

	stored := &brokerv1alpha1.ClusterLease{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(lease), stored); err != nil {
		t.Fatalf("getting bound lease: %v", err)
	}
	if stored.Status.BoundTime == nil || !stored.Status.BoundTime.Time.Equal(now) {
		t.Fatalf("BoundTime = %v, want %v", stored.Status.BoundTime, now)
	}
}

func TestClusterLeaseReconcileBoundUsesUpdatedTTLDeadline(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.March, 4, 5, 6, 7, 0, time.UTC)
	boundTime := metav1.NewTime(now.Add(-time.Minute))
	lease := &brokerv1alpha1.ClusterLease{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "ttl-updated",
			Namespace:  testNamespace,
			Finalizers: []string{leaseFinalizer},
		},
		Spec: brokerv1alpha1.ClusterLeaseSpec{
			PoolRef: corev1.LocalObjectReference{Name: testTTLPoolName},
			TTL:     &metav1.Duration{Duration: 5 * time.Minute},
		},
		Status: brokerv1alpha1.ClusterLeaseStatus{
			Phase:     brokerv1alpha1.PhaseLeaseBound,
			BoundTime: &boundTime,
		},
	}
	c := newStatusWriteFakeClient(t, lease)
	r := &ClusterLeaseReconciler{Client: c, Scheme: c.Scheme(), Now: func() time.Time { return now }}

	result, err := r.reconcileBound(ctx, lease)
	if err != nil {
		t.Fatalf("reconcileBound with extended TTL: %v", err)
	}
	if result.RequeueAfter != 4*time.Minute {
		t.Fatalf("extended TTL RequeueAfter = %s, want 4m", result.RequeueAfter)
	}

	lease.Spec.TTL = &metav1.Duration{Duration: 30 * time.Second}
	if err := c.Update(ctx, lease); err != nil {
		t.Fatalf("updating shortened TTL: %v", err)
	}
	current := &brokerv1alpha1.ClusterLease{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(lease), current); err != nil {
		t.Fatalf("getting lease after TTL update: %v", err)
	}
	result, err = r.reconcileBound(ctx, current)
	if err != nil {
		t.Fatalf("reconcileBound with shortened TTL: %v", err)
	}
	if result != (ctrl.Result{}) {
		t.Fatalf("shortened TTL result = %+v, want empty result", result)
	}
	current = &brokerv1alpha1.ClusterLease{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(lease), current); err != nil {
		t.Fatalf("getting expired lease after shortened TTL: %v", err)
	}
	if current.DeletionTimestamp.IsZero() {
		t.Fatal("shortened TTL did not mark lease for deletion")
	}
}
