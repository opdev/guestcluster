package controller

import (
	"context"
	"fmt"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const namespaceEnabledLabel = "guestcluster.opdev.io/enabled"
const namespaceEnabledValue = "true"
const conditionTypeProvisioningAllowed = "ProvisioningAllowed"

type namespacePolicy struct {
	enabled     bool
	terminating bool
	reason      string
	message     string
}

// Use a live read for allocation decisions. An informer can still contain the
// old label after an administrator has disabled a namespace.
func readNamespacePolicy(ctx context.Context, reader client.Reader, namespace string) (namespacePolicy, error) {
	ns := &corev1.Namespace{}
	if err := reader.Get(ctx, client.ObjectKey{Name: namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return namespacePolicy{terminating: true, reason: "NamespaceUnavailable", message: "Source namespace no longer exists"}, nil
		}
		return namespacePolicy{}, fmt.Errorf("reading source namespace policy: %w", err)
	}
	if !ns.DeletionTimestamp.IsZero() || ns.Status.Phase == corev1.NamespaceTerminating {
		return namespacePolicy{terminating: true, reason: "NamespaceTerminating", message: "Source namespace is terminating; new allocations are disabled"}, nil
	}
	if ns.Labels[namespaceEnabledLabel] != namespaceEnabledValue {
		return namespacePolicy{reason: "NamespaceDisabled", message: "New provisioning requires namespace label guestcluster.opdev.io/enabled=\"true\""}, nil
	}
	return namespacePolicy{enabled: true, reason: "NamespaceEnabled", message: "Source namespace permits new provisioning"}, nil
}

func (p namespacePolicy) condition(generation int64) metav1.Condition {
	status := metav1.ConditionFalse
	if p.enabled {
		status = metav1.ConditionTrue
	}
	return metav1.Condition{Type: conditionTypeProvisioningAllowed, Status: status, Reason: p.reason, Message: p.message, ObservedGeneration: generation}
}

// gateNamespaceProvisioning never handles deletion. Reconcile must dispatch
// finalizers first. Namespace termination also stops authorized provisioning:
// Kubernetes cannot create local resources in a terminating namespace.
func (r *ClusterInstanceReconciler) gateNamespaceProvisioning(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (*ctrl.Result, error) {
	policy, err := readNamespacePolicy(ctx, r.platformReader(), instance.Namespace)
	if err != nil {
		return nil, err
	}
	previous := instance.Status.DeepCopy()
	if policy.terminating {
		apimeta.SetStatusCondition(&instance.Status.Conditions, policy.condition(instance.Generation))
		return &ctrl.Result{RequeueAfter: requeueInterval}, r.updateStatusIfChanged(ctx, instance, previous, "recording namespace termination")
	}
	if instance.Status.Provisioning != nil {
		return nil, nil
	}
	if policy.enabled {
		instance.Status.Provisioning = &brokerv1alpha1.ProvisioningAuthorization{StartedAt: metav1.Now()}
		policy = namespacePolicy{enabled: true, reason: "ProvisioningAuthorized", message: "Provisioning authorization is recorded; namespace opt-out does not revoke existing work"}
		apimeta.SetStatusCondition(&instance.Status.Conditions, policy.condition(instance.Generation))
		// End this pass. No backing-resource write can precede the durable decision.
		return &ctrl.Result{RequeueAfter: requeueInterval}, r.updateStatusIfChanged(ctx, instance, previous, "recording provisioning authorization")
	}
	apimeta.SetStatusCondition(&instance.Status.Conditions, policy.condition(instance.Generation))
	instance.Status.Phase = brokerv1alpha1.PhaseProvisioning
	apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: conditionTypeReady, Status: metav1.ConditionFalse, Reason: policy.reason, Message: policy.message, ObservedGeneration: instance.Generation})
	return &ctrl.Result{RequeueAfter: requeueInterval}, r.updateStatusIfChanged(ctx, instance, previous, "recording namespace provisioning block")
}
func namespaceRequests(ctx context.Context, c client.Client, obj client.Object, pools bool) []reconcile.Request {
	var requests []reconcile.Request
	if pools {
		list := &brokerv1alpha1.ClusterPoolList{}
		if err := c.List(ctx, list, client.InNamespace(obj.GetName())); err != nil {
			logf.FromContext(ctx).Error(err, "listing pools for namespace policy")
			return nil
		}
		for i := range list.Items {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
		}
	} else {
		list := &brokerv1alpha1.ClusterInstanceList{}
		if err := c.List(ctx, list, client.InNamespace(obj.GetName())); err != nil {
			logf.FromContext(ctx).Error(err, "listing instances for namespace policy")
			return nil
		}
		for i := range list.Items {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
		}
	}
	return requests
}
