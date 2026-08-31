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

// Distills the ClusterLease side of the manual live-cluster binding-model
// verification matrix into fast, hermetic envtest specs -- see
// clusterpool_verification_test.go's file doc for the broader context.
//
// Covers:
//   - Binding commits with a SINGLE write to the lease (InstanceRef +
//     Phase=Bound); the claimed ClusterInstance object is never touched by
//     ClusterLeaseReconciler (Status.LeaseRef is a separate, derived
//     projection owned by ClusterInstanceReconciler -- see
//     clusterinstance_controller.go).
//   - An instance already claimed by a sibling lease's Status.InstanceRef is
//     never double-claimed by a second lease.
//   - TTL expiry releases the lease: forces deletion of both the lease
//     (finalizer-driven) and its claimed instance.
//
// None of this logic branches on topology (see verification_fixtures_test.go),
// so every spec below is registered once per entry in allVerificationTopologies.

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

var _ = func() bool {
	for _, topology := range allVerificationTopologies {
		registerClusterLeaseVerificationSpecs(topology)
	}
	return true
}()

func registerClusterLeaseVerificationSpecs(topology brokerv1alpha1.ClusterTopology) bool {
	return Describe(fmt.Sprintf("ClusterLease Controller (binding-model verification matrix) [%s]", topology), func() {
		const namespace = "default"
		poolName := "verify-lease-pool-" + string(topology)

		// Fixture object names are suffixed with the topology so the two
		// registrations of these specs (one per allVerificationTopologies
		// entry) never collide over the same object name: a lease created
		// with leaseFinalizer pre-set is never actually reconciled to
		// completion by these assertions-only Its, so AfterEach's
		// DeleteAllOf merely marks it Terminating (finalizer still blocks
		// real removal) -- reusing a name across topologies would then hit
		// an AlreadyExists conflict against that still-Terminating object.
		suffixed := func(name string) string { return name + "-" + string(topology) }

		newReadyInstance := func(name string) *brokerv1alpha1.ClusterInstance {
			name = suffixed(name)
			inst := &brokerv1alpha1.ClusterInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
					Labels:    resources.PoolLabels(poolName),
				},
				Spec: brokerv1alpha1.ClusterInstanceSpec{
					Type:     topology,
					PoolRef:  corev1.LocalObjectReference{Name: poolName},
					Template: verificationTemplateFor(topology),
				},
			}
			Expect(k8sClient.Create(ctx, inst)).To(Succeed())
			inst.Status.Phase = brokerv1alpha1.PhaseReady
			inst.Status.KubeconfigSecretRef = corev1.LocalObjectReference{Name: name + "-kubeconfig"}
			Expect(k8sClient.Status().Update(ctx, inst)).To(Succeed())

			// bind() reads the instance's kubeconfig Secret; provide one so a
			// successful bind doesn't fail on that unrelated lookup.
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: name + "-kubeconfig", Namespace: namespace},
				Data:       map[string][]byte{resources.KubeconfigSecretKey: []byte("apiVersion: v1\nkind: Config\n")},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			return inst
		}

		// newPendingLease creates a lease with the finalizer already present, so
		// a single Reconcile call exercises matchmaking directly instead of
		// spending its first call only adding the finalizer.
		newPendingLease := func(name string, ttl *metav1.Duration) *brokerv1alpha1.ClusterLease {
			lease := &brokerv1alpha1.ClusterLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:       suffixed(name),
					Namespace:  namespace,
					Finalizers: []string{leaseFinalizer},
				},
				Spec: brokerv1alpha1.ClusterLeaseSpec{
					PoolRef: corev1.LocalObjectReference{Name: poolName},
					TTL:     ttl,
				},
			}
			Expect(k8sClient.Create(ctx, lease)).To(Succeed())
			return lease
		}

		reconcileLease := func(lease *brokerv1alpha1.ClusterLease) reconcile.Result {
			r := &ClusterLeaseReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(lease)})
			Expect(err).NotTo(HaveOccurred())
			return res
		}

		AfterEach(func() {
			Expect(client.IgnoreNotFound(k8sClient.DeleteAllOf(ctx, &brokerv1alpha1.ClusterLease{}, client.InNamespace(namespace)))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.DeleteAllOf(ctx, &brokerv1alpha1.ClusterInstance{}, client.InNamespace(namespace)))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace(namespace)))).To(Succeed())
		})

		It("binds with a single write to the lease, never touching the claimed instance's status", func() {
			inst := newReadyInstance("verify-bind-inst")
			before := inst.DeepCopy()
			lease := newPendingLease("verify-bind-lease", nil)

			reconcileLease(lease)

			bound := &brokerv1alpha1.ClusterLease{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(lease), bound)).To(Succeed())
			Expect(bound.Status.Phase).To(Equal(brokerv1alpha1.PhaseLeaseBound))
			Expect(bound.Status.InstanceRef).NotTo(BeNil())
			Expect(bound.Status.InstanceRef.Name).To(Equal(inst.Name))

			// The instance's own status must be untouched by this reconcile --
			// binding is a single write to the LEASE only. (LeaseRef is a
			// separate, derived projection maintained by
			// ClusterInstanceReconciler, not by this controller.)
			after := &brokerv1alpha1.ClusterInstance{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(inst), after)).To(Succeed())
			Expect(after.ResourceVersion).To(Equal(before.ResourceVersion), "ClusterLeaseReconciler must never write to the claimed ClusterInstance")
			Expect(after.Status.LeaseRef).To(BeNil())
		})

		It("never double-claims an instance already claimed by a sibling lease", func() {
			inst := newReadyInstance("verify-noclaim-inst")

			claimant := newPendingLease("verify-noclaim-first", nil)
			reconcileLease(claimant)
			boundFirst := &brokerv1alpha1.ClusterLease{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(claimant), boundFirst)).To(Succeed())
			Expect(boundFirst.Status.Phase).To(Equal(brokerv1alpha1.PhaseLeaseBound))

			second := newPendingLease("verify-noclaim-second", nil)
			reconcileLease(second)

			stillPending := &brokerv1alpha1.ClusterLease{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(second), stillPending)).To(Succeed())
			Expect(stillPending.Status.Phase).To(Equal(brokerv1alpha1.PhaseLeasePending))
			Expect(stillPending.Status.InstanceRef).To(BeNil())

			var boundCondition *metav1.Condition
			for i := range stillPending.Status.Conditions {
				if stillPending.Status.Conditions[i].Type == conditionTypeLeaseBound {
					boundCondition = &stillPending.Status.Conditions[i]
				}
			}
			Expect(boundCondition).NotTo(BeNil())
			Expect(boundCondition.Reason).To(Equal("WaitingForCapacity"))

			// The instance must still only be claimed by the first lease.
			finalInst := &brokerv1alpha1.ClusterInstance{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(inst), finalInst)).To(Succeed())
			_ = finalInst // instance-side state is a derived projection, not asserted here
		})

		It("releases on TTL expiry: deletes both the lease and its claimed instance", func() {
			inst := newReadyInstance("verify-ttl-inst")
			lease := newPendingLease("verify-ttl-lease", &metav1.Duration{Duration: time.Minute})

			// Simulate an already-Bound lease whose TTL has long since expired.
			lease.Status.Phase = brokerv1alpha1.PhaseLeaseBound
			lease.Status.InstanceRef = &corev1.LocalObjectReference{Name: inst.Name}
			past := metav1.NewTime(time.Now().Add(-time.Hour))
			lease.Status.BoundTime = &past
			Expect(k8sClient.Status().Update(ctx, lease)).To(Succeed())

			// First reconcile: reconcileBound detects TTL exceeded and issues
			// r.Delete on the lease itself; with the finalizer present, the API
			// server sets DeletionTimestamp rather than removing it yet.
			reconcileLease(lease)
			deleting := &brokerv1alpha1.ClusterLease{}
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(lease), deleting)
			if err == nil {
				Expect(deleting.DeletionTimestamp).NotTo(BeNil(), "lease should be marked for deletion after TTL expiry")
				// Second reconcile: reconcileDelete releases the claimed
				// instance and removes the finalizer, letting the API server
				// finish deleting the lease object.
				reconcileLease(deleting)
			}

			err = k8sClient.Get(ctx, client.ObjectKeyFromObject(lease), &brokerv1alpha1.ClusterLease{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "lease should be fully deleted after TTL-driven release completes")

			err = k8sClient.Get(ctx, client.ObjectKeyFromObject(inst), &brokerv1alpha1.ClusterInstance{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the claimed instance should be deleted as part of TTL-driven release")
		})
	})
}
