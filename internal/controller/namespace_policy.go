package controller

import (
	"context"
	"fmt"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
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
	legacy, err := r.verifyLegacyProvisioning(ctx, instance)
	if err != nil {
		policy = namespacePolicy{reason: "LegacyIdentityConflict", message: err.Error()}
	} else if policy.enabled || legacy {
		instance.Status.Provisioning = &brokerv1alpha1.ProvisioningAuthorization{StartedAt: metav1.Now(), Legacy: legacy}
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

// A phase, a finalizer, or recorded placement alone cannot establish that
// provisioning started. Verify an actual managed backing object instead.
func (r *ClusterInstanceReconciler) verifyLegacyProvisioning(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (bool, error) {
	if instance.Spec.Type == brokerv1alpha1.TopologyHCP {
		namespace, err := r.recoverHCPNamespace(ctx, instance)
		if err != nil {
			return false, err
		}
		_, name := hcpLocation(instance)
		for _, obj := range []client.Object{
			&hyperv1beta1.HostedCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KASServingCertName(instance.Name), Namespace: namespace}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.DefaultPullSecretName(instance.Name), Namespace: namespace}},
		} {
			if namespace == instance.Namespace && obj.GetName() == resources.DefaultPullSecretName(instance.Name) {
				continue
			}
			if err := r.platformReader().Get(ctx, client.ObjectKeyFromObject(obj), obj); apierrors.IsNotFound(err) {
				continue
			} else if err != nil {
				return false, err
			}
			if err := r.verifyHCPResource(ctx, instance, obj); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, nil
	}
	if instance.Spec.Type != brokerv1alpha1.TopologyCRC {
		return false, nil
	}
	vm := &kubevirtv1.VirtualMachine{}
	key := client.ObjectKey{Namespace: instance.Namespace, Name: resources.VMName(instance.Name)}
	if instance.Status.CRC != nil && instance.Status.CRC.VMName != "" {
		key.Name = instance.Status.CRC.VMName
	}
	if err := r.platformReader().Get(ctx, key, vm); apierrors.IsNotFound(err) {
		// A disk can exist before the VM is created.
		dv := &cdiv1beta1.DataVolume{}
		key.Name = resources.DataVolumeName(instance.Name)
		if instance.Status.CRC != nil && instance.Status.CRC.DataVolumeName != "" {
			key.Name = instance.Status.CRC.DataVolumeName
		}
		if err := r.platformReader().Get(ctx, key, dv); apierrors.IsNotFound(err) {
			copy := &corev1.Secret{}
			key.Name = resources.CRCBootKeySecretName(instance.Name)
			if err := r.platformReader().Get(ctx, key, copy); apierrors.IsNotFound(err) {
				return false, nil
			} else if err != nil {
				return false, err
			}
			binding := crcBootKeyBinding(instance)
			if !metav1.IsControlledBy(copy, instance) || binding == nil || bootKeyHash(copy.Data[crcBundleSSHKeyDataKey]) != binding.KeySHA256 {
				return false, fmt.Errorf("legacy CRC boot-key copy %s has no verified binding", key)
			}
			return true, nil
		} else if err != nil {
			return false, err
		}
		return true, verifyLegacyCRCIdentity(instance, dv)
	} else if err != nil {
		return false, err
	}
	return true, verifyLegacyCRCIdentity(instance, vm)
}

func verifyLegacyCRCIdentity(instance *brokerv1alpha1.ClusterInstance, obj client.Object) error {
	if obj.GetNamespace() != instance.Namespace {
		return fmt.Errorf("CRC resource is outside the source namespace")
	}
	if metav1.IsControlledBy(obj, instance) && instance.UID != "" {
		return nil
	}
	if len(obj.GetOwnerReferences()) != 0 || obj.GetLabels()[resources.LabelManagedBy] != resources.ManagerName || obj.GetLabels()[resources.LabelInstance] != instance.Name {
		return fmt.Errorf("CRC resource %s has no verified instance identity", client.ObjectKeyFromObject(obj))
	}
	created := obj.GetCreationTimestamp()
	if created.Before(&instance.CreationTimestamp) {
		return fmt.Errorf("CRC resource %s predates this instance UID", client.ObjectKeyFromObject(obj))
	}
	if vm, ok := obj.(*kubevirtv1.VirtualMachine); ok {
		if vm.Spec.Template != nil {
			for _, volume := range vm.Spec.Template.Spec.Volumes {
				if volume.DataVolume != nil && volume.DataVolume.Name == resources.CRCDiskName(instance) {
					return nil
				}
			}
		}
		return fmt.Errorf("legacy CRC VM %s does not reference the recorded instance disk", client.ObjectKeyFromObject(vm))
	}
	return nil
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
