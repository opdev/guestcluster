package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

const crcBundleSSHKeyDataKey = "id_ecdsa"

func crcBootKeyBinding(instance *brokerv1alpha1.ClusterInstance) *brokerv1alpha1.CRCBootKeyStatus {
	if instance.Status.CRC == nil {
		return nil
	}
	return instance.Status.CRC.BootKey
}

func boundCRCSource(instance *brokerv1alpha1.ClusterInstance, binding *brokerv1alpha1.CRCBootKeyStatus) *crcDataVolumeSource {
	bundle := &brokerv1alpha1.CRCBundle{
		Spec: brokerv1alpha1.CRCBundleSpec{StorageClassName: binding.StorageClassName},
		Status: brokerv1alpha1.CRCBundleStatus{
			QCOW2PVCNamespace: binding.PVCNamespace,
			QCOW2PVCRef:       &corev1.LocalObjectReference{Name: binding.PVCName},
		},
	}
	return &crcDataVolumeSource{
		dv:            resources.BuildCRCDataVolumeFromBundle(instance, bundle),
		sshSecretName: resources.CRCBootKeySecretName(instance.Name), sshDataKey: crcBundleSSHKeyDataKey,
	}
}

func bootKeyHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// verifyCRCBootSource checks both halves of the bundle before recording a
// binding or restoring a lost copy. A changed Secret or golden PVC must never
// silently become the boot key or disk of an already-created VM.
func (r *ClusterInstanceReconciler) verifyCRCBootSource(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, binding *brokerv1alpha1.CRCBootKeyStatus) (*brokerv1alpha1.CRCBootKeyStatus, []byte, error) {
	arch := instance.Spec.Template.CRCArch
	if arch == "" {
		arch = resources.DefaultCRCArch
	}
	name := resources.CRCBundleName(instance.Spec.Template.CRCVersion, arch)
	bundle := &brokerv1alpha1.CRCBundle{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, bundle); err != nil {
		return nil, nil, fmt.Errorf("getting CRCBundle %s for boot key: %w", name, err)
	}
	s := bundle.Status
	if s.Phase != brokerv1alpha1.CRCBundlePhaseReady || s.SHA256 == "" || s.QCOW2PVCNamespace == "" || s.QCOW2PVCRef == nil || s.QCOW2PVCRef.Name == "" || s.SSHKeySecretRef == nil || s.SSHKeySecretRef.Name == "" {
		return nil, nil, fmt.Errorf("CRCBundle %s has invalid or incomplete Ready disk/SSH key references", name)
	}
	pvcKey := types.NamespacedName{Namespace: s.QCOW2PVCNamespace, Name: s.QCOW2PVCRef.Name}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, pvcKey, pvc); err != nil {
		return nil, nil, fmt.Errorf("getting CRCBundle golden PVC %s: %w", pvcKey, err)
	}
	secretKey := types.NamespacedName{Namespace: s.QCOW2PVCNamespace, Name: s.SSHKeySecretRef.Name}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, secretKey, secret); err != nil {
		return nil, nil, fmt.Errorf("getting CRCBundle SSH key Secret %s: %w", secretKey, err)
	}
	data := secret.Data[crcBundleSSHKeyDataKey]
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("CRCBundle SSH key Secret %s has empty or missing id_ecdsa data", secretKey)
	}
	current := &brokerv1alpha1.CRCBootKeyStatus{
		BundleUID: string(bundle.UID), BundleSHA256: s.SHA256, PVCNamespace: pvcKey.Namespace, PVCName: pvcKey.Name,
		PVCUID: string(pvc.UID), StorageClassName: bundle.Spec.StorageClassName, SecretNamespace: secretKey.Namespace, SecretName: secretKey.Name,
		SecretUID: string(secret.UID), KeySHA256: bootKeyHash(data),
	}
	if current.BundleUID == "" || current.PVCUID == "" || current.SecretUID == "" {
		return nil, nil, fmt.Errorf("CRCBundle %s disk or SSH key has no UID", name)
	}
	if binding != nil && *binding != *current {
		return nil, nil, fmt.Errorf("CRCBundle %s disk or boot SSH key changed since this instance was created; refusing recovery", name)
	}
	return current, data, nil
}

// ensureCRCBootKey materializes and checks an instance-owned, immutable-in-
// practice boot key. It returns pending after first persisting the binding so
// no DataVolume can be created before its source identity is durable.
func (r *ClusterInstanceReconciler) ensureCRCBootKey(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, dv *cdiv1beta1.DataVolume) (string, bool, error) {
	name := resources.CRCBootKeySecretName(instance.Name)
	key := types.NamespacedName{Namespace: instance.Namespace, Name: name}
	binding := crcBootKeyBinding(instance)
	if binding == nil {
		if dv != nil {
			existingDV := &cdiv1beta1.DataVolume{}
			if err := r.Get(ctx, types.NamespacedName{Namespace: dv.Namespace, Name: dv.Name}, existingDV); err == nil {
				return "", false, fmt.Errorf("CRC DataVolume %s/%s exists without a recorded disk/key binding; cannot verify its boot key", dv.Namespace, dv.Name)
			} else if !apierrors.IsNotFound(err) {
				return "", false, err
			}
		}
		current, _, err := r.verifyCRCBootSource(ctx, instance, nil)
		if err != nil {
			return "", false, err
		}
		if instance.Status.CRC == nil {
			instance.Status.CRC = &brokerv1alpha1.CRCBackingStatus{}
		}
		instance.Status.CRC.BootKey = current
		if err := r.Status().Update(ctx, instance); err != nil {
			return "", false, fmt.Errorf("recording CRC disk/key binding: %w", err)
		}
		return name, true, nil
	}

	copy := &corev1.Secret{}
	err := r.Get(ctx, key, copy)
	if err == nil {
		if !metav1.IsControlledBy(copy, instance) {
			return "", false, fmt.Errorf("CRC boot key Secret %s conflicts with this ClusterInstance (owner UID differs)", key)
		}
		if len(copy.Data[crcBundleSSHKeyDataKey]) == 0 || bootKeyHash(copy.Data[crcBundleSSHKeyDataKey]) != binding.KeySHA256 {
			return "", false, fmt.Errorf("CRC boot key Secret %s does not match the recorded disk/key binding", key)
		}
	} else if !apierrors.IsNotFound(err) {
		return "", false, fmt.Errorf("getting CRC boot key Secret %s: %w", key, err)
	} else {
		_, data, err := r.verifyCRCBootSource(ctx, instance, binding)
		if err != nil {
			return "", false, err
		}
		copy = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace, Labels: resources.CommonLabels(instance)},
			Type:       corev1.SecretTypeOpaque, Data: map[string][]byte{crcBundleSSHKeyDataKey: bytes.Clone(data)},
		}
		if err := controllerutil.SetControllerReference(instance, copy, r.Scheme); err != nil {
			return "", false, fmt.Errorf("setting CRC boot key owner: %w", err)
		}
		if err := r.Create(ctx, copy); err != nil {
			// Retry by reading the new object; never assume an AlreadyExists
			// result means that another writer created the intended copy.
			return "", false, fmt.Errorf("creating CRC boot key Secret %s: %w", key, err)
		}
	}
	if dv != nil {
		existingDV := &cdiv1beta1.DataVolume{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: dv.Namespace, Name: dv.Name}, existingDV); apierrors.IsNotFound(err) {
			if _, _, err := r.verifyCRCBootSource(ctx, instance, binding); err != nil {
				return "", false, fmt.Errorf("cannot create CRC disk from changed source: %w", err)
			}
		} else if err != nil {
			return "", false, err
		} else if existingDV.Spec.Source == nil || existingDV.Spec.Source.PVC == nil ||
			existingDV.Spec.Source.PVC.Namespace != binding.PVCNamespace || existingDV.Spec.Source.PVC.Name != binding.PVCName {
			return "", false, fmt.Errorf("CRC DataVolume %s/%s does not match the recorded golden disk", dv.Namespace, dv.Name)
		} else if existingDV.Status.Phase != cdiv1beta1.Succeeded {
			// A pending clone still reads the source PVC. Its name in the
			// DataVolume spec does not identify the original PVC if the
			// bundle re-creates that PVC under the same name.
			pvcKey := types.NamespacedName{Namespace: binding.PVCNamespace, Name: binding.PVCName}
			pvc := &corev1.PersistentVolumeClaim{}
			if err := r.Get(ctx, pvcKey, pvc); err != nil {
				return "", false, fmt.Errorf("getting recorded CRC golden PVC %s while clone is pending: %w", pvcKey, err)
			}
			if string(pvc.UID) != binding.PVCUID {
				return "", false, fmt.Errorf("CRC golden PVC %s changed UID while DataVolume %s/%s clone is pending", pvcKey, dv.Namespace, dv.Name)
			}
		}
	}
	return name, false, nil
}
