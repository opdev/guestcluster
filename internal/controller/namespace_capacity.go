package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
)

const clusterInstanceCountQuota = "count/clusterinstances.guestcluster.opdev.io"

type namespaceInstanceCapacity struct {
	total   int64
	limit   int64
	limited bool
}

func (c namespaceInstanceCapacity) atLimit() bool {
	return c.limited && c.total >= c.limit
}

func namespaceClusterInstanceLimit(quotas *corev1.ResourceQuotaList) (int64, bool) {
	var limit int64
	limited := false
	resourceName := corev1.ResourceName(clusterInstanceCountQuota)
	for i := range quotas.Items {
		quotaLimit, found := quotas.Items[i].Spec.Hard[resourceName]
		if !found {
			continue
		}
		value := quotaLimit.Value()
		if !limited || value < limit {
			limit = value
			limited = true
		}
	}
	return limit, limited
}

func namespaceLimitCondition(generation int64, capacity namespaceInstanceCapacity) metav1.Condition {
	message := "ResourceQuota blocks creation of another ClusterInstance in this namespace"
	if capacity.limited {
		message = fmt.Sprintf("namespace ClusterInstance limit reached: %d of %d instances exist", capacity.total, capacity.limit)
	}
	return metav1.Condition{
		Type:               conditionTypeCapacityAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             "NamespaceAtCapacity",
		Message:            message,
		ObservedGeneration: generation,
	}
}

func isClusterInstanceQuotaExceeded(err error) bool {
	if !apierrors.IsForbidden(err) {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "exceeded quota:") && strings.Contains(message, clusterInstanceCountQuota)
}

func poolRequestsForNamespace(ctx context.Context, c client.Client, namespace string) []reconcile.Request {
	list := &brokerv1alpha1.ClusterPoolList{}
	if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "listing pools for namespace capacity")
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
	}
	return requests
}
