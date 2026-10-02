package controller

import (
	"context"
	"fmt"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (r *ClusterInstanceReconciler) verifyCRCResource(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, obj client.Object) error {
	if obj.GetNamespace() != instance.Namespace {
		return apiEndpointConflict("CRC resource %s is outside the instance namespace", client.ObjectKeyFromObject(obj))
	}
	if metav1.IsControlledBy(obj, instance) {
		return nil
	}
	// Platform-created dependents retain their local platform owner. Its UID
	// must match a verified live parent or the recorded parent during teardown.
	owner := metav1.GetControllerOf(obj)
	if owner != nil && owner.UID != "" {
		var parent client.Object
		var recorded string
		status := instance.Status.CRC
		if status == nil {
			status = &brokerv1alpha1.CRCBackingStatus{}
		}
		switch obj.(type) {
		case *kubevirtv1.VirtualMachineInstance:
			if owner.Kind == "VirtualMachine" && owner.Name == resources.CRCVMName(instance) {
				parent = &kubevirtv1.VirtualMachine{}
				recorded = status.VMUID
			}
		case *corev1.PersistentVolumeClaim:
			if owner.Kind == "DataVolume" && owner.Name == resources.CRCDiskName(instance) {
				parent = &cdiv1beta1.DataVolume{}
				recorded = status.DataVolumeUID
			}
		case *corev1.Pod:
			if owner.Kind == "VirtualMachineInstance" && owner.Name == resources.CRCVMName(instance) {
				parent = &kubevirtv1.VirtualMachineInstance{}
				recorded = status.VMIUID
			}
		}
		if parent != nil {
			if recorded != "" && string(owner.UID) == recorded {
				return nil
			}
			if err := r.platformReader().Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: owner.Name}, parent); err == nil {
				if parent.GetUID() == owner.UID {
					return r.verifyCRCResource(ctx, instance, parent)
				}
			} else if !apierrors.IsNotFound(err) {
				return err
			} else if recorded == "" && (instance.Status.Provisioning == nil || instance.Status.Provisioning.Legacy) {
				// Legacy cleanup can find a dependent after its parent is gone.
				// The expected local parent kind/name, managed labels, and creation
				// time must agree. A known but different parent UID never uses this path.
				legacy := obj.DeepCopyObject().(client.Object)
				legacy.SetOwnerReferences(nil)
				if err := verifyLegacyCRCIdentity(instance, legacy); err == nil {
					return nil
				}
			}
		}
		return apiEndpointConflict("CRC resource %s belongs to another owner UID", client.ObjectKeyFromObject(obj))
	}
	if instance.Status.Provisioning == nil || instance.Status.Provisioning.Legacy {
		if err := verifyLegacyCRCObject(instance, obj); err == nil {
			return nil
		}
	}
	return apiEndpointConflict("CRC resource %s is not owned by ClusterInstance %s (UID %s)", client.ObjectKeyFromObject(obj), instance.Name, instance.UID)
}

func verifyLegacyCRCObject(instance *brokerv1alpha1.ClusterInstance, obj client.Object) error {
	// Agent handoffs from old Jobs use a different managed-by label. Check
	// the VMI-specific name, or the exact pre-VMI legacy result name.
	if secret, ok := obj.(*corev1.Secret); ok && secret.Labels[resources.LabelManagedBy] == "crc-agent" {
		vmiUID := string(secret.Data[resources.VMIUIDSecretKey])
		if secret.Name != resources.RawKubeconfigSecretName(instance.Name) &&
			(vmiUID == "" || secret.Name != resources.RawKubeconfigSecretNameForVMI(instance.Name, vmiUID)) {
			return fmt.Errorf("legacy handoff name does not match its VMI")
		}
		copy := secret.DeepCopy()
		copy.Labels[resources.LabelManagedBy] = resources.ManagerName
		return verifyLegacyCRCIdentity(instance, copy)
	}
	return verifyLegacyCRCIdentity(instance, obj)
}

// ensureCRCObject verifies both ordinary reuse and a concurrent create. Only
// verified legacy objects receive a missing controller reference.
func (r *ClusterInstanceReconciler) ensureCRCObject(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, desired client.Object) (client.Object, error) {
	if err := controllerutil.SetControllerReference(instance, desired, r.Scheme); err != nil {
		return nil, err
	}
	existing := desired.DeepCopyObject().(client.Object)
	key := client.ObjectKeyFromObject(desired)
	if err := r.platformReader().Get(ctx, key, existing); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err == nil {
			return desired, nil
		} else if !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		if err := r.platformReader().Get(ctx, key, existing); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if err := r.verifyCRCResource(ctx, instance, existing); err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(existing, instance) {
		if err := controllerutil.SetControllerReference(instance, existing, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Update(ctx, existing); err != nil {
			return nil, err
		}
	}
	return existing, nil
}

func (r *ClusterInstanceReconciler) deleteCRCObject(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, obj client.Object, label string, opts ...client.DeleteOption) (bool, error) {
	if err := r.platformReader().Get(ctx, client.ObjectKeyFromObject(obj), obj); apierrors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if err := r.verifyCRCResource(ctx, instance, obj); err != nil {
		return false, err
	}
	uid := obj.GetUID()
	opts = append(opts, client.Preconditions{UID: &uid})
	return r.deleteIfExists(ctx, obj, label, opts...)
}

// Persist the names before allocation, and parent UIDs before deleting parents.
// These identities let later passes verify dependent VMIs, Pods, and PVCs.
func (r *ClusterInstanceReconciler) recordCRCIdentity(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) error {
	previous := instance.Status.DeepCopy()
	if instance.Status.CRC == nil {
		instance.Status.CRC = &brokerv1alpha1.CRCBackingStatus{}
	}
	status := instance.Status.CRC
	status.VMName, status.DataVolumeName = resources.CRCVMName(instance), resources.CRCDiskName(instance)
	for _, obj := range []client.Object{
		&kubevirtv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: status.VMName, Namespace: instance.Namespace}},
		&cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{Name: status.DataVolumeName, Namespace: instance.Namespace}},
		&kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{Name: status.VMName, Namespace: instance.Namespace}},
	} {
		if err := r.platformReader().Get(ctx, client.ObjectKeyFromObject(obj), obj); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
		if err := r.verifyCRCResource(ctx, instance, obj); err != nil {
			return err
		}
		switch obj.(type) {
		case *kubevirtv1.VirtualMachine, *cdiv1beta1.DataVolume:
			if instance.DeletionTimestamp.IsZero() && !metav1.IsControlledBy(obj, instance) {
				if _, err := r.ensureCRCObject(ctx, instance, obj); err != nil {
					return err
				}
			}
		}
		switch obj.(type) {
		case *kubevirtv1.VirtualMachine:
			status.VMUID = string(obj.GetUID())
		case *cdiv1beta1.DataVolume:
			status.DataVolumeUID = string(obj.GetUID())
		case *kubevirtv1.VirtualMachineInstance:
			// Recovery owns VMI transitions during normal reconciliation.
			if !instance.DeletionTimestamp.IsZero() {
				status.VMIUID = string(obj.GetUID())
			}
		}
	}
	if err := r.updateStatusIfChanged(ctx, instance, previous, "recording CRC backing identity"); err != nil {
		return fmt.Errorf("cannot record CRC backing identity: %w", err)
	}
	return nil
}
