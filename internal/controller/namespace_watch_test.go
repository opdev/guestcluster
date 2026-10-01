package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
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
})
