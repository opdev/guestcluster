package controller

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNamespaceClusterInstanceLimitUsesLowestQuota(t *testing.T) {
	quotas := &corev1.ResourceQuotaList{Items: []corev1.ResourceQuota{
		{Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(clusterInstanceCountQuota): resource.MustParse("8"),
		}}},
		{Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(clusterInstanceCountQuota): resource.MustParse("3"),
		}}},
		{Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceRequestsCPU: resource.MustParse("4"),
		}}},
	}}

	limit, limited := namespaceClusterInstanceLimit(quotas)
	if !limited || limit != 3 {
		t.Fatalf("namespace limit = (%d, %t), want (3, true)", limit, limited)
	}
}

func TestNamespaceClusterInstanceLimitAllowsZero(t *testing.T) {
	quotas := &corev1.ResourceQuotaList{Items: []corev1.ResourceQuota{{
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(clusterInstanceCountQuota): resource.MustParse("0"),
		}},
	}}}

	limit, limited := namespaceClusterInstanceLimit(quotas)
	if !limited || limit != 0 {
		t.Fatalf("namespace limit = (%d, %t), want (0, true)", limit, limited)
	}
	if !(namespaceInstanceCapacity{total: 0, limit: limit, limited: limited}).atLimit() {
		t.Fatal("zero limit must block creation when the namespace has no instances")
	}
}

func TestIsClusterInstanceQuotaExceeded(t *testing.T) {
	quotaError := apierrors.NewForbidden(corev1.Resource("clusterinstances"), "new-instance", errors.New("exceeded quota: clusterinstance-limit, requested: count/clusterinstances.guestcluster.opdev.io=1, used: 2, limited: 2"))
	if !isClusterInstanceQuotaExceeded(quotaError) {
		t.Fatal("expected ClusterInstance quota rejection to be identified")
	}
	otherForbidden := apierrors.NewForbidden(corev1.Resource("clusterinstances"), "new-instance", errors.New("user cannot create resource"))
	if isClusterInstanceQuotaExceeded(otherForbidden) {
		t.Fatal("unrelated forbidden error must not be identified as a quota rejection")
	}
}

func TestNamespaceLimitCondition(t *testing.T) {
	condition := namespaceLimitCondition(4, namespaceInstanceCapacity{total: 5, limit: 5, limited: true})
	if condition.Status != metav1.ConditionFalse || condition.Reason != "NamespaceAtCapacity" {
		t.Fatalf("condition = (%s, %s), want (False, NamespaceAtCapacity)", condition.Status, condition.Reason)
	}
}
