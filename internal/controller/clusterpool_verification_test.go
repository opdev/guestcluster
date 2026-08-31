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

// This file distills the manual live-cluster verification matrix performed
// while validating the single-source-of-truth binding model (see
// clusterlease_types.go's ClusterLease.Status.InstanceRef doc, and the
// "Binding model" section of README.md) into fast, deterministic, hermetic
// tests. Unlike a real cluster (where a CRC VM takes ~15 minutes to boot),
// these call ClusterPoolReconciler.Reconcile directly against the envtest
// API server with hand-crafted ClusterInstance/ClusterLease fixtures, so
// each scenario runs in well under a second and requires no VM, no
// crc-agent, and no CRCBundle.
//
// Covers:
//   - On-demand provisioning creates exactly one instance for pending
//     demand, and does not create a second (phantom) instance on a
//     subsequent reconcile while that instance is still in flight (the
//     bug that originally motivated the single-write binding model).
//   - warmSpares and minSize floors independently trigger top-up.
//   - The CapacityAvailable condition transitions between
//     CapacitySufficient and AtCapacity.
//   - Scale-down never targets an instance claimed by a ClusterLease
//     (Status.InstanceRef is the single source of truth consulted).
//   - Scale-down respects the stability window: a freshly-Ready excess
//     instance is not deleted immediately, but is deleted once it has
//     been idle long enough.
//
// None of this logic branches on topology (see verification_fixtures_test.go),
// so every spec below is registered once per entry in allVerificationTopologies.

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

var _ = func() bool {
	for _, topology := range allVerificationTopologies {
		registerClusterPoolVerificationSpecs(topology)
	}
	return true
}()

func registerClusterPoolVerificationSpecs(topology brokerv1alpha1.ClusterTopology) bool {
	return Describe(fmt.Sprintf("ClusterPool Controller (binding-model verification matrix) [%s]", topology), func() {
		const namespace = "default"

		// Fixture object names are suffixed with the topology so the two
		// registrations of these specs (one per allVerificationTopologies
		// entry) never collide over the same object name -- see the
		// analogous comment in clusterlease_verification_test.go.
		suffixed := func(name string) string { return name + "-" + string(topology) }

		// newVerifyPool builds a minimal, schema-valid ClusterPool. Template
		// deliberately omits CRCVersion so ensureCRCBundle is a no-op (no
		// CRCBundle/bundle-prep dependency in these tests).
		newVerifyPool := func(name string, maxSize, minSize, warmSpares int32) *brokerv1alpha1.ClusterPool {
			return &brokerv1alpha1.ClusterPool{
				ObjectMeta: metav1.ObjectMeta{Name: suffixed(name), Namespace: namespace},
				Spec: brokerv1alpha1.ClusterPoolSpec{
					Type:       topology,
					MaxSize:    maxSize,
					MinSize:    minSize,
					WarmSpares: warmSpares,
					Template:   verificationTemplateFor(topology),
				},
			}
		}

		// newVerifyInstance builds a ClusterInstance labeled/owned as if
		// ClusterPoolReconciler had created it for poolName, in the given phase.
		newVerifyInstance := func(name, poolName string, phase brokerv1alpha1.ClusterInstancePhase) *brokerv1alpha1.ClusterInstance {
			inst := &brokerv1alpha1.ClusterInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      suffixed(name),
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
			inst.Status.Phase = phase
			if phase == brokerv1alpha1.PhaseReady {
				inst.Status.Conditions = []metav1.Condition{{
					Type:               conditionTypeReady,
					Status:             metav1.ConditionTrue,
					Reason:             "BackingResourcesAvailable",
					Message:            "test fixture",
					LastTransitionTime: metav1.Now(),
					ObservedGeneration: inst.Generation,
				}}
			}
			Expect(k8sClient.Status().Update(ctx, inst)).To(Succeed())
			return inst
		}

		// newVerifyLease builds a ClusterLease targeting poolName, optionally
		// pre-claiming an instance via Status.InstanceRef (the single source of
		// truth for the binding).
		newVerifyLease := func(name, poolName string, claimedInstance string) *brokerv1alpha1.ClusterLease {
			lease := &brokerv1alpha1.ClusterLease{
				ObjectMeta: metav1.ObjectMeta{Name: suffixed(name), Namespace: namespace},
				Spec: brokerv1alpha1.ClusterLeaseSpec{
					PoolRef: corev1.LocalObjectReference{Name: poolName},
				},
			}
			Expect(k8sClient.Create(ctx, lease)).To(Succeed())
			if claimedInstance != "" {
				lease.Status.Phase = brokerv1alpha1.PhaseLeaseBound
				lease.Status.InstanceRef = &corev1.LocalObjectReference{Name: claimedInstance}
				Expect(k8sClient.Status().Update(ctx, lease)).To(Succeed())
			}
			return lease
		}

		listPoolInstances := func(poolName string) []brokerv1alpha1.ClusterInstance {
			list := &brokerv1alpha1.ClusterInstanceList{}
			Expect(k8sClient.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels(resources.PoolLabels(poolName)))).To(Succeed())
			return list.Items
		}

		reconcilePool := func(pool *brokerv1alpha1.ClusterPool) reconcile.Result {
			// k8sClient (see suite_test.go) is a direct client.New(cfg, ...)
			// client with no cache/informer in front of it, so it doubles as
			// both the regular (would-be-cached) client and the APIReader here
			// -- there is no staleness to guard against in this test harness.
			r := &ClusterPoolReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), APIReader: k8sClient}
			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
			Expect(err).NotTo(HaveOccurred())
			return res
		}

		getPool := func(name string) *brokerv1alpha1.ClusterPool {
			p := &brokerv1alpha1.ClusterPool{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, p)).To(Succeed())
			return p
		}

		capacityCondition := func(pool *brokerv1alpha1.ClusterPool) *metav1.Condition {
			for i := range pool.Status.Conditions {
				if pool.Status.Conditions[i].Type == conditionTypeCapacityAvailable {
					return &pool.Status.Conditions[i]
				}
			}
			return nil
		}

		AfterEach(func() {
			Expect(k8sClient.DeleteAllOf(ctx, &brokerv1alpha1.ClusterLease{}, client.InNamespace(namespace))).To(Succeed())
			Expect(k8sClient.DeleteAllOf(ctx, &brokerv1alpha1.ClusterInstance{}, client.InNamespace(namespace))).To(Succeed())
			Expect(k8sClient.DeleteAllOf(ctx, &brokerv1alpha1.ClusterPool{}, client.InNamespace(namespace))).To(Succeed())
		})

		It("provisions exactly one instance on demand, and does not create a phantom second instance on the next reconcile", func() {
			pool := newVerifyPool("pool-ondemand", 4, 0, 0)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			newVerifyLease("lease-ondemand", pool.Name, "") // Pending, no claim yet

			reconcilePool(pool)
			Expect(listPoolInstances(pool.Name)).To(HaveLen(1), "first reconcile should create exactly one instance for the pending lease")

			// The created instance is still Provisioning (never reconciled by
			// ClusterInstanceReconciler in this test), so it counts as "pending"
			// supply. A second pool reconcile, with demand unchanged, must NOT
			// provision another instance -- this is exactly the phantom-instance
			// race the single-write binding model eliminates.
			reconcilePool(pool)
			Expect(listPoolInstances(pool.Name)).To(HaveLen(1), "second reconcile must not create a phantom second instance while demand is already covered by in-flight supply")
		})

		It("tops up to satisfy warmSpares even with no pending demand", func() {
			pool := newVerifyPool("pool-warmspares", 4, 0, 1)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			reconcilePool(pool)
			Expect(listPoolInstances(pool.Name)).To(HaveLen(1))
		})

		It("tops up to satisfy minSize even with no pending demand and warmSpares=0", func() {
			pool := newVerifyPool("pool-minsize", 4, 1, 0)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			reconcilePool(pool)
			Expect(listPoolInstances(pool.Name)).To(HaveLen(1))
		})

		It("reports AtCapacity when saturated with unmet demand, and CapacitySufficient once demand is relieved", func() {
			pool := newVerifyPool("pool-atcapacity", 1, 0, 0)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			newVerifyInstance("inst-atcapacity", pool.Name, brokerv1alpha1.PhaseReady)
			leaseA := newVerifyLease("lease-atcapacity-a", pool.Name, "")
			newVerifyLease("lease-atcapacity-b", pool.Name, "")

			reconcilePool(pool)
			cond := capacityCondition(getPool(pool.Name))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("AtCapacity"))

			Expect(k8sClient.Delete(ctx, leaseA)).To(Succeed())
			reconcilePool(pool)
			cond = capacityCondition(getPool(pool.Name))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal("CapacitySufficient"))
		})

		It("never scales down an instance claimed by a ClusterLease, regardless of floors", func() {
			pool := newVerifyPool("pool-neverdelete-claimed", 4, 0, 0)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			inst := newVerifyInstance("inst-claimed", pool.Name, brokerv1alpha1.PhaseReady)
			newVerifyLease("lease-claims-it", pool.Name, inst.Name)

			reconcilePool(pool)

			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(inst), &brokerv1alpha1.ClusterInstance{})).To(Succeed(),
				"a claimed instance must never be scaled down even though minSize=warmSpares=0")
		})

		It("respects the scale-down stability window: does not delete a freshly-Ready excess instance, but does once it has been idle long enough", func() {
			pool := newVerifyPool("pool-stability", 4, 0, 0)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			inst := newVerifyInstance("inst-stability", pool.Name, brokerv1alpha1.PhaseReady)
			// newVerifyInstance stamps the Ready condition with metav1.Now(), so
			// this instance is well within the stability window right now.

			res := reconcilePool(pool)
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(inst), &brokerv1alpha1.ClusterInstance{})).To(Succeed(),
				"a freshly-Ready excess instance must not be deleted before the stability window elapses")
			Expect(res.RequeueAfter).To(BeNumerically(">", 0), "should schedule a requeue for when the instance becomes eligible")

			// Age the instance out of the stability window by backdating its
			// Ready condition's LastTransitionTime.
			fresh := &brokerv1alpha1.ClusterInstance{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(inst), fresh)).To(Succeed())
			for i := range fresh.Status.Conditions {
				if fresh.Status.Conditions[i].Type == conditionTypeReady {
					fresh.Status.Conditions[i].LastTransitionTime = metav1.NewTime(time.Now().Add(-2 * scaleDownStabilityPeriod))
				}
			}
			Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())

			reconcilePool(pool)
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(inst), &brokerv1alpha1.ClusterInstance{})
			Expect(client.IgnoreNotFound(err)).To(Succeed())
			Expect(err).To(HaveOccurred(), "the instance should have been deleted once it cleared the stability window")
		})
	})
}
