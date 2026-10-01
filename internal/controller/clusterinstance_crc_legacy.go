package controller

import (
	"context"
	"fmt"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// An existing legacy Job can complete using its immutable Pod template. A
// missing disk/key binding must not cancel that Job or select a new bundle key.
// Creating another Job requires a verified binding; an old Secret name alone
// cannot prove which private key belongs to an already-cloned disk.
func (r *ClusterInstanceReconciler) continueLegacyCRCAgent(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, pullSecretName string) (crcResult, error) {
	res := crcResult{vmName: resources.CRCVMName(instance), dvName: resources.CRCDiskName(instance)}
	vmi := &kubevirtv1.VirtualMachineInstance{}
	if err := r.platformReader().Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: res.vmName}, vmi); err != nil {
		return res, crcBootKeyError{fmt.Errorf("legacy CRC disk/key binding is absent; cannot start recovery: %w", err)}
	}
	if err := r.verifyCRCResource(ctx, instance, vmi); err != nil {
		return res, err
	}
	res.vmiUID = string(vmi.UID)
	job := &batchv1.Job{}
	key := client.ObjectKey{Namespace: instance.Namespace, Name: resources.CRCAgentJobName(instance.Name, res.vmiUID)}
	if err := r.platformReader().Get(ctx, key, job); err != nil {
		return res, crcBootKeyError{fmt.Errorf("legacy CRC disk/key binding is absent and no verified Job exists for this VMI: %w", err)}
	}
	if err := r.verifyCRCResource(ctx, instance, job); err != nil {
		return res, err
	}
	identity := &corev1.Secret{}
	if err := r.platformReader().Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: resources.CRCIdentitySecretName(instance.Name)}, identity); err != nil {
		return res, crcBootKeyError{fmt.Errorf("legacy CRC identity is unavailable: %w", err)}
	}
	if err := r.verifyCRCResource(ctx, instance, identity); err != nil {
		return res, err
	}
	if vmi.Status.Phase != kubevirtv1.Running {
		return res, nil
	}
	for _, iface := range vmi.Status.Interfaces {
		if iface.IP != "" {
			res.sshEndpoint = iface.IP
			break
		}
	}
	if res.sshEndpoint == "" {
		return res, nil
	}
	// The backing reconciler preserves existing Job templates. These names are
	// read only for its desired-object builder; they never change the old Job.
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "bundle-ssh-key" && volume.Secret != nil && len(volume.Secret.Items) > 0 {
			return r.ensureCRCAgentBackingForJob(ctx, instance, res, volume.Secret.SecretName, volume.Secret.Items[0].Key, pullSecretName, job)
		}
	}
	return res, crcBootKeyError{fmt.Errorf("legacy Job %s has no verified boot-key volume", key)}
}
