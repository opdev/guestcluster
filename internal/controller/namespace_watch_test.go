package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var _ = Describe("Namespace policy watch", func() {
	It("wakes a pool created before opt-in through the manager watch", func() {
		managerCtx, stop := context.WithCancel(ctx)
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: k8sClient.Scheme(), Metrics: metricsserver.Options{BindAddress: "0"}})
		Expect(err).NotTo(HaveOccurred())
		r := &ClusterPoolReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme()}
		Expect(r.SetupWithManager(mgr)).To(Succeed())
		done := make(chan error, 1)
		go func() { done <- mgr.Start(managerCtx) }()
		DeferCleanup(func() { stop(); Expect(<-done).To(Succeed()) })
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "namespace-watch"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		pool := &brokerv1alpha1.ClusterPool{ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: ns.Name}, Spec: brokerv1alpha1.ClusterPoolSpec{Type: brokerv1alpha1.TopologyHCP, MinSize: 1, MaxSize: 1, Template: verificationTemplateFor(brokerv1alpha1.TopologyHCP)}}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pool); _ = k8sClient.Delete(ctx, ns) })
		Eventually(func() string {
			current := &brokerv1alpha1.ClusterPool{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
				return ""
			}
			for _, condition := range current.Status.Conditions {
				if condition.Type == conditionTypeProvisioningAllowed {
					return condition.Reason
				}
			}
			return ""
		}, 5*time.Second, 50*time.Millisecond).Should(Equal("NamespaceDisabled"))
		ns.Labels = map[string]string{namespaceEnabledLabel: namespaceEnabledValue}
		Expect(k8sClient.Update(ctx, ns)).To(Succeed())
		// Less than the 20-second polling backstop: this requires a watch event.
		Eventually(func() int {
			instances := &brokerv1alpha1.ClusterInstanceList{}
			if err := k8sClient.List(ctx, instances, client.InNamespace(ns.Name)); err != nil {
				return -1
			}
			return len(instances.Items)
		}, 5*time.Second, 50*time.Millisecond).Should(Equal(1))
	})

	It("wakes pools when quota or ClusterInstance capacity changes", func() {
		managerCtx, stop := context.WithCancel(ctx)
		skipNameValidation := true
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:     k8sClient.Scheme(),
			Metrics:    metricsserver.Options{BindAddress: "0"},
			Controller: config.Controller{SkipNameValidation: &skipNameValidation},
		})
		Expect(err).NotTo(HaveOccurred())
		r := &ClusterPoolReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme()}
		Expect(r.SetupWithManager(mgr)).To(Succeed())
		done := make(chan error, 1)
		go func() { done <- mgr.Start(managerCtx) }()
		DeferCleanup(func() { stop(); Expect(<-done).To(Succeed()) })

		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:   "namespace-capacity-watch",
			Labels: map[string]string{namespaceEnabledLabel: namespaceEnabledValue},
		}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Name: "clusterinstance-limit", Namespace: ns.Name},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceName(clusterInstanceCountQuota): resource.MustParse("1"),
			}},
		}
		Expect(k8sClient.Create(ctx, quota)).To(Succeed())
		first := &brokerv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: ns.Name},
			Spec:       brokerv1alpha1.ClusterPoolSpec{Type: brokerv1alpha1.TopologyHCP, MinSize: 1, MaxSize: 2, Template: verificationTemplateFor(brokerv1alpha1.TopologyHCP)},
		}
		second := &brokerv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: ns.Name},
			Spec:       brokerv1alpha1.ClusterPoolSpec{Type: brokerv1alpha1.TopologyCRC, MinSize: 1, MaxSize: 2, Template: verificationTemplateFor(brokerv1alpha1.TopologyCRC)},
		}
		third := &brokerv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{Name: "third", Namespace: ns.Name},
			Spec:       brokerv1alpha1.ClusterPoolSpec{Type: brokerv1alpha1.TopologyHCP, MinSize: 1, MaxSize: 2, Template: verificationTemplateFor(brokerv1alpha1.TopologyHCP)},
		}
		Expect(k8sClient.Create(ctx, first)).To(Succeed())

		var firstInstance *brokerv1alpha1.ClusterInstance
		Eventually(func() string {
			instances := &brokerv1alpha1.ClusterInstanceList{}
			if err := k8sClient.List(ctx, instances, client.InNamespace(ns.Name)); err != nil {
				return ""
			}
			for i := range instances.Items {
				if instances.Items[i].Spec.PoolRef.Name == first.Name {
					firstInstance = instances.Items[i].DeepCopy()
					return firstInstance.Name
				}
			}
			return ""
		}, 5*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())
		currentFirst := &brokerv1alpha1.ClusterPool{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(first), currentFirst)).To(Succeed())
		currentFirst.Spec.MinSize = 0
		Expect(k8sClient.Update(ctx, currentFirst)).To(Succeed())
		Expect(k8sClient.Create(ctx, second)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, first)
			_ = k8sClient.Delete(ctx, second)
			_ = k8sClient.Delete(ctx, third)
			_ = k8sClient.Delete(ctx, ns)
		})
		Eventually(func() string {
			current := &brokerv1alpha1.ClusterPool{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(second), current); err != nil {
				return ""
			}
			for _, condition := range current.Status.Conditions {
				if condition.Type == conditionTypeCapacityAvailable {
					return condition.Reason
				}
			}
			return ""
		}, 5*time.Second, 50*time.Millisecond).Should(Equal("NamespaceAtCapacity"))

		// Increasing the quota must wake the blocked pool before the retry interval.
		currentQuota := &corev1.ResourceQuota{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(quota), currentQuota)).To(Succeed())
		currentQuota.Spec.Hard[corev1.ResourceName(clusterInstanceCountQuota)] = resource.MustParse("2")
		Expect(k8sClient.Update(ctx, currentQuota)).To(Succeed())
		Eventually(func() int {
			instances := &brokerv1alpha1.ClusterInstanceList{}
			if err := k8sClient.List(ctx, instances, client.InNamespace(ns.Name)); err != nil {
				return -1
			}
			count := 0
			for i := range instances.Items {
				if instances.Items[i].Spec.PoolRef.Name == second.Name {
					count++
				}
			}
			return count
		}, 5*time.Second, 50*time.Millisecond).Should(Equal(1))

		Expect(k8sClient.Create(ctx, third)).To(Succeed())
		Eventually(func() string {
			current := &brokerv1alpha1.ClusterPool{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(third), current); err != nil {
				return ""
			}
			for _, condition := range current.Status.Conditions {
				if condition.Type == conditionTypeCapacityAvailable {
					return condition.Reason
				}
			}
			return ""
		}, 5*time.Second, 50*time.Millisecond).Should(Equal("NamespaceAtCapacity"))

		currentSecond := &brokerv1alpha1.ClusterPool{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(second), currentSecond)).To(Succeed())
		currentSecond.Spec.MinSize = 0
		Expect(k8sClient.Update(ctx, currentSecond)).To(Succeed())
		secondInstances := &brokerv1alpha1.ClusterInstanceList{}
		Expect(k8sClient.List(ctx, secondInstances, client.InNamespace(ns.Name))).To(Succeed())
		var secondInstance *brokerv1alpha1.ClusterInstance
		for i := range secondInstances.Items {
			if secondInstances.Items[i].Spec.PoolRef.Name == second.Name {
				secondInstance = secondInstances.Items[i].DeepCopy()
				break
			}
		}
		Expect(secondInstance).NotTo(BeNil())
		// Deleting this instance must wake the third pool before the retry interval.
		Expect(k8sClient.Delete(ctx, secondInstance)).To(Succeed())
		Eventually(func() int {
			instances := &brokerv1alpha1.ClusterInstanceList{}
			if err := k8sClient.List(ctx, instances, client.InNamespace(ns.Name)); err != nil {
				return -1
			}
			count := 0
			for i := range instances.Items {
				if instances.Items[i].Spec.PoolRef.Name == third.Name {
					count++
				}
			}
			return count
		}, 5*time.Second, 50*time.Millisecond).Should(Equal(1))
		Expect(firstInstance).NotTo(BeNil())
	})

	It("wakes a standalone instance when its namespace resource quota increases", func() {
		managerCtx, stop := context.WithCancel(ctx)
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: k8sClient.Scheme(), Metrics: metricsserver.Options{BindAddress: "0"}})
		Expect(err).NotTo(HaveOccurred())
		r := &ClusterInstanceReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme()}
		err = ctrl.NewControllerManagedBy(mgr).
			For(&brokerv1alpha1.ClusterInstance{}).
			Watches(&corev1.ResourceQuota{}, handler.EnqueueRequestsFromMapFunc(r.resourceQuotaRequests), builder.WithPredicates(predicate.GenerationChangedPredicate{})).
			Named("clusterinstance-resource-quota-watch-test").
			Complete(r)
		Expect(err).NotTo(HaveOccurred())
		done := make(chan error, 1)
		go func() { done <- mgr.Start(managerCtx) }()
		DeferCleanup(func() { stop(); Expect(<-done).To(Succeed()) })

		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:   "namespace-direct-resource-watch",
			Labels: map[string]string{namespaceEnabledLabel: namespaceEnabledValue},
		}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Name: "guestcluster-resource-budget", Namespace: ns.Name},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceName(clusterInstanceCPUQuota): resource.MustParse("1"),
			}},
		}
		Expect(k8sClient.Create(ctx, quota)).To(Succeed())
		instance := &brokerv1alpha1.ClusterInstance{
			ObjectMeta: metav1.ObjectMeta{Name: "direct-instance", Namespace: ns.Name},
			Spec: brokerv1alpha1.ClusterInstanceSpec{
				Type:     brokerv1alpha1.TopologyHCP,
				Template: verificationTemplateFor(brokerv1alpha1.TopologyHCP),
			},
		}
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, instance)
			_ = k8sClient.Delete(ctx, quota)
			_ = k8sClient.Delete(ctx, ns)
		})

		Eventually(func() string {
			current := &brokerv1alpha1.ClusterInstance{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(instance), current); err != nil {
				return ""
			}
			condition := apimeta.FindStatusCondition(current.Status.Conditions, conditionTypeReady)
			if condition == nil {
				return ""
			}
			return condition.Reason
		}, 5*time.Second, 50*time.Millisecond).Should(Equal(namespaceResourceQuotaReason))

		currentQuota := &corev1.ResourceQuota{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(quota), currentQuota)).To(Succeed())
		currentQuota.Spec.Hard[corev1.ResourceName(clusterInstanceCPUQuota)] = resource.MustParse("2")
		Expect(k8sClient.Update(ctx, currentQuota)).To(Succeed())
		// The ResourceQuota watch must recover this instance before the 20-second polling backstop.
		Eventually(func() bool {
			current := &brokerv1alpha1.ClusterInstance{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(instance), current); err != nil {
				return false
			}
			return current.Status.Provisioning != nil
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
	})
})
