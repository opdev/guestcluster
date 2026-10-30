package controller

import (
	"context"
	"testing"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestNamespaceResourceQuotaBlocksDirectInstanceUntilRaised(t *testing.T) {
	ctx := context.Background()
	ns := enabledTestNamespace(pullSecretTestNamespace)
	candidate := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "direct-candidate", Namespace: ns.Name},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type:     brokerv1alpha1.TopologyHCP,
			Template: brokerv1alpha1.ClusterTemplate{Cores: 2, Memory: "8Gi"},
		},
	}
	poolInstance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-instance", Namespace: ns.Name},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type:     brokerv1alpha1.TopologyCRC,
			PoolRef:  corev1.LocalObjectReference{Name: "pool"},
			Template: brokerv1alpha1.ClusterTemplate{Cores: 4, Memory: "16Gi"},
		},
	}
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "guestcluster-budget", Namespace: ns.Name},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(clusterInstanceCPUQuota):     resource.MustParse("5"),
			corev1.ResourceName(clusterInstanceMemoryQuota):  resource.MustParse("24Gi"),
			corev1.ResourceName(clusterInstanceStorageQuota): resource.MustParse("112Gi"),
		}},
	}
	c := policyClient(t, ns, candidate, poolInstance, quota)
	r := &ClusterInstanceReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}

	result, err := r.gateNamespaceProvisioning(ctx, candidate)
	if err != nil || result == nil {
		t.Fatalf("resource quota did not block direct provisioning: result=%v error=%v", result, err)
	}
	if candidate.Status.Provisioning != nil {
		t.Fatal("blocked direct instance received durable provisioning authorization")
	}
	if condition := meta.FindStatusCondition(candidate.Status.Conditions, conditionTypeReady); condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != namespaceResourceQuotaReason {
		t.Fatalf("Ready condition = %+v, want False/%s", condition, namespaceResourceQuotaReason)
	}
	if condition := meta.FindStatusCondition(candidate.Status.Conditions, conditionTypeProvisioningAllowed); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("ProvisioningAllowed condition = %+v, want True while resource quota is the blocker", condition)
	}

	currentQuota := &corev1.ResourceQuota{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(quota), currentQuota); err != nil {
		t.Fatal(err)
	}
	currentQuota.Spec.Hard[corev1.ResourceName(clusterInstanceCPUQuota)] = resource.MustParse("6")
	if err := c.Update(ctx, currentQuota); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(candidate), candidate); err != nil {
		t.Fatal(err)
	}
	if _, err := r.gateNamespaceProvisioning(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	if candidate.Status.Provisioning == nil {
		t.Fatal("direct instance was not authorized after the quota had enough capacity")
	}

	currentQuota.Spec.Hard[corev1.ResourceName(clusterInstanceCPUQuota)] = resource.MustParse("0")
	if err := c.Update(ctx, currentQuota); err != nil {
		t.Fatal(err)
	}
	if _, err := r.gateNamespaceProvisioning(ctx, candidate); err != nil {
		t.Fatalf("lowering a quota revoked an existing authorization: %v", err)
	}
}

func TestNamespaceResourceQuotaAllowsPoolLinkedCandidateAtExactLimits(t *testing.T) {
	ctx := context.Background()
	ns := enabledTestNamespace(pullSecretTestNamespace)
	candidate := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "crc-standalone-sample", Namespace: ns.Name, UID: types.UID("candidate-uid")},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type:    brokerv1alpha1.TopologyCRC,
			PoolRef: corev1.LocalObjectReference{Name: "crc-pool"},
			Template: brokerv1alpha1.ClusterTemplate{
				Cores: 4, Memory: "16Gi", RootVolumeSize: "80Gi",
			},
		},
	}
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "guestcluster-budget", Namespace: ns.Name},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(clusterInstanceCPUQuota):     resource.MustParse("4"),
			corev1.ResourceName(clusterInstanceMemoryQuota):  resource.MustParse("16Gi"),
			corev1.ResourceName(clusterInstanceStorageQuota): resource.MustParse("80Gi"),
		}},
	}
	c := policyClient(t, ns, candidate, quota)
	r := &ClusterInstanceReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}

	result, err := r.gateNamespaceProvisioning(ctx, candidate)
	if err != nil || result == nil {
		t.Fatalf("exact-fit pool-linked candidate was rejected: result=%v error=%v", result, err)
	}
	if candidate.Status.Provisioning == nil {
		t.Fatal("exact-fit pool-linked candidate did not receive provisioning authorization")
	}
	if condition := meta.FindStatusCondition(candidate.Status.Conditions, conditionTypeReady); condition != nil && condition.Reason == namespaceResourceQuotaReason {
		t.Fatalf("exact-fit pool-linked candidate retained quota-blocked Ready condition: %+v", condition)
	}
}
