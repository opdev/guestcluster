/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"

	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
)

const crcReadyRequeueInterval = time.Minute

// crcResult carries the outcome of reconciling a topology=crc instance's
// backing objects. The caller (Reconcile) uses this outcome to decide phase
// transitions, so this function does not need to know about phase
// semantics itself.
type crcResult struct {
	ready          bool
	agentCondition *metav1.Condition
	ocpVersion     string
	kubeconfig     []byte
	sshEndpoint    string
	vmiUID         string
	// apiEndpoint is the externally-routable URL of the guest API server
	// (the passthrough Route host, see ensureCRCAPIRoute). markReady copies
	// it into ClusterInstanceStatus.APIEndpoint / ClusterLeaseStatus.APIEndpoint.
	apiEndpoint string
}

// crcAgentImage resolves the container image used by CRC agent and bundle-prep
// Jobs. Operator deployments can override it with the
// CRC_AGENT_IMAGE environment variable on the manager Deployment (an
// OLM RELATED_IMAGE-style override point). If unset, crcAgentImage falls
// back to a default image for local and dev use.
func crcAgentImage() string {
	if img := os.Getenv(resources.CRCAgentImageEnvVar); img != "" {
		return img
	}
	return resources.DefaultCRCAgentImage
}

// crcDataVolumeSource carries the CRC DataVolume spec and the boot SSH key
// location resolved by resolveCRCDataVolumeSource.
type crcDataVolumeSource struct {
	dv            *cdiv1beta1.DataVolume
	sshSecretName string
	sshDataKey    string
}

// resolveCRCDataVolumeSource resolves the DataVolume source and the SSH key
// the crc-agent Job uses. These differ between the turnkey path (CRCVersion
// set, clone from a shared CRCBundle) and the manual/fallback path
// (releaseImage + BundleSSHKeyRef). See api/v1alpha1/clusterpool_types.go's
// ClusterTemplate doc comments for the full contract. A nil source with a
// nil error means the caller should wait and retry on a later reconcile.
func (r *ClusterInstanceReconciler) resolveCRCDataVolumeSource(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, bundleKeyDataKey string) (*crcDataVolumeSource, error) {
	log := logf.FromContext(ctx)
	tmpl := instance.Spec.Template
	if tmpl.CRCVersion == "" {
		return &crcDataVolumeSource{
			dv:            resources.BuildCRCDataVolume(instance),
			sshSecretName: tmpl.BundleSSHKeyRef.Name,
			sshDataKey:    bundleKeyDataKey,
		}, nil
	}
	if binding := crcBootKeyBinding(instance); binding != nil {
		return boundCRCSource(instance, binding), nil
	}

	arch := tmpl.CRCArch
	if arch == "" {
		arch = resources.DefaultCRCArch
	}
	bundleName := resources.CRCBundleName(tmpl.CRCVersion, arch)
	bundle := &brokerv1alpha1.CRCBundle{}
	if err := r.Get(ctx, types.NamespacedName{Name: bundleName}, bundle); err != nil {
		if apierrors.IsNotFound(err) {
			// Normally ClusterPoolReconciler has already created this
			// bundle by the time an instance exists. A standalone
			// ClusterInstance (not created via a pool) could race ahead
			// of it. Reconcile self-corrects once the CRCBundle appears.
			log.Info("CRCBundle not found yet, waiting", "crcBundle", bundleName)
			return nil, nil
		}
		return nil, fmt.Errorf("getting CRCBundle %s: %w", bundleName, err)
	}
	if bundle.Status.Phase != brokerv1alpha1.CRCBundlePhaseReady {
		log.Info(resources.BundleNotReadyMessage(bundle))
		return nil, nil
	}
	if bundle.Status.QCOW2PVCNamespace == "" || bundle.Status.QCOW2PVCRef == nil || bundle.Status.QCOW2PVCRef.Name == "" || bundle.Status.SSHKeySecretRef == nil || bundle.Status.SSHKeySecretRef.Name == "" {
		return nil, crcBootKeyError{fmt.Errorf("CRCBundle %s has invalid or incomplete Ready disk/SSH key references", bundleName)}
	}
	return &crcDataVolumeSource{
		dv:            resources.BuildCRCDataVolumeFromBundle(instance, bundle),
		sshSecretName: resources.CRCBootKeySecretName(instance.Name),
		sshDataKey:    crcBundleSSHKeyDataKey, // fixed data key written by the bundle-prep script
	}, nil
}

// ensureCRCBacking creates the CDI DataVolume and KubeVirt VirtualMachine
// that back a topology=crc ClusterInstance, if they do not already exist.
// It reports readiness by checking two conditions: the VM reports
// Status.Ready, and the crc-agent has published the raw kubeconfig handoff
// Secret (see resources.RawKubeconfigSecretName).
//
// ensureCRCBacking treats the DataVolume and VM specs as immutable once
// created, because CRC bundle imports and running VMs cannot be mutated
// safely in place. Drift correction here only re-creates objects that were
// deleted out-of-band; it does not patch existing ones.
func (r *ClusterInstanceReconciler) ensureCRCBacking(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, bundleKeyDataKey, pullSecretName string) (crcResult, error) {
	log := logf.FromContext(ctx)
	res := crcResult{}

	src, err := r.resolveCRCDataVolumeSource(ctx, instance, bundleKeyDataKey)
	if err != nil {
		return res, err
	}
	if src == nil {
		return res, nil // waiting on a dependency; resolveCRCDataVolumeSource already logged why
	}
	dv, sshSecretName, sshDataKey := src.dv, src.sshSecretName, src.sshDataKey
	if instance.Spec.Template.CRCVersion != "" {
		var pending bool
		sshSecretName, pending, err = r.ensureCRCBootKey(ctx, instance, dv)
		if err != nil {
			return res, crcBootKeyError{err}
		}
		if pending {
			return res, nil
		}
		// The bundle may have been re-prepared since this instance's disk
		// was selected. Always clone from the recorded PVC.
		dv = boundCRCSource(instance, crcBootKeyBinding(instance)).dv
	}

	if _, err := r.ensureCRCObject(ctx, instance, dv); err != nil {
		return res, err
	}
	vm := resources.BuildCRCVirtualMachine(instance, resources.CRCDiskName(instance))
	obj, err := r.ensureCRCObject(ctx, instance, vm)
	if err != nil {
		return res, err
	}
	existingVM := obj.(*kubevirtv1.VirtualMachine)

	if !existingVM.Status.Ready {
		return res, nil
	}

	// The VM reports ready. Discover its IP via the VirtualMachineInstance
	// before doing anything else, because the crc-agent Job needs the IP to
	// SSH in. A VM can be Ready before its VMI reports an interface IP; this
	// is a valid transient state.
	vmi := &kubevirtv1.VirtualMachineInstance{}
	if err := r.Get(ctx, types.NamespacedName{Name: vm.Name, Namespace: vm.Namespace}, vmi); apierrors.IsNotFound(err) {
		log.Info("CRC VM ready but VMI not found yet, waiting", "virtualMachine", vm.Name)
		return res, nil
	} else if err != nil {
		return res, fmt.Errorf("getting CRC VirtualMachineInstance %s/%s: %w", vm.Namespace, vm.Name, err)
	}
	if vmi.Status.Phase != kubevirtv1.Running {
		log.Info("CRC VMI is not running yet, waiting", "virtualMachine", vm.Name, "phase", vmi.Status.Phase)
		return res, nil
	}
	if err := r.verifyCRCResource(ctx, instance, vmi); err != nil {
		return res, err
	}
	res.vmiUID = string(vmi.UID)

	var vmIP string
	for _, iface := range vmi.Status.Interfaces {
		if iface.IP != "" {
			vmIP = iface.IP
			break
		}
	}
	if vmIP == "" {
		log.Info("CRC VMI running but no interface IP reported yet, waiting", "virtualMachine", vm.Name)
		return res, nil
	}
	res.sshEndpoint = vmIP
	return r.ensureCRCAgentBacking(ctx, instance, res, sshSecretName, sshDataKey, pullSecretName)
}

func (r *ClusterInstanceReconciler) ensureCRCAgentBacking(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, res crcResult, sshSecretName, sshDataKey, pullSecretName string) (crcResult, error) {
	log := logf.FromContext(ctx)
	vmIP := res.sshEndpoint
	if err := r.ensureCRCAgentRBAC(ctx, instance); err != nil {
		return res, err
	}
	// Ensure the Service and passthrough Route that expose the guest API
	// server externally exist; this call is idempotent. The VMI's own
	// pod-network IP is not routable outside the management cluster. The
	// management cluster's own ingress and router front this Route, which
	// is what makes the published kubeconfig usable from outside the
	// cluster.
	apiHost, err := r.ensureCRCAPIRoute(ctx, instance)
	if err != nil {
		return res, err
	}
	res.apiEndpoint = "https://" + apiHost
	identitySecretName, err := r.ensureCRCIdentity(ctx, instance, apiHost)
	if err != nil {
		return res, err
	}

	// Ensure the crc-agent Job exists. It SSHes into the VM and runs the
	// post-boot fixups natively (see cmd/crc-agent). This call is
	// idempotent: once created, the Job runs to completion or reaches a terminal
	// failure condition on its own.
	job := resources.BuildCRCAgentJob(instance, vmIP, res.vmiUID, sshSecretName, sshDataKey, identitySecretName, crcAgentImage(), apiHost, pullSecretName)
	if err := controllerutil.SetControllerReference(instance, job, r.Scheme); err != nil {
		return res, fmt.Errorf("setting owner reference on crc-agent Job %s/%s: %w", job.Namespace, job.Name, err)
	}
	obj, err := r.ensureCRCObject(ctx, instance, job)
	if err != nil {
		return res, err
	}
	existingJob := obj.(*batchv1.Job)
	if existingJob.Spec.Template.Spec.ServiceAccountName != resources.CRCAgentAccountName(instance.Name) {
		return res, apiEndpointConflict("crc-agent Job %s/%s does not use its instance ServiceAccount", job.Namespace, job.Name)
	}

	// Once the crc-agent Job completes successfully, it publishes the raw
	// kubeconfig and observed OCP version handoff Secret. checkCRCKubeconfigHandoff
	// reports whether it has done so yet.
	// The status write that records this VMI can lag behind Job creation.
	// Inspect the handoff for the VMI we just observed, not the old status.
	handoffInstance := instance.DeepCopy()
	if handoffInstance.Status.CRC == nil {
		handoffInstance.Status.CRC = &brokerv1alpha1.CRCBackingStatus{}
	}
	handoffInstance.Status.CRC.VMIUID = res.vmiUID
	kubeconfig, ocpVersion, err := r.checkCRCKubeconfigHandoff(ctx, handoffInstance)
	if err != nil {
		return res, err
	}
	// A freshly created Job is not necessarily visible through the cache yet.
	res.agentCondition, err = r.inspectCRCAgent(ctx, instance, existingJob, len(kubeconfig) > 0)
	if err != nil {
		return res, err
	}
	if len(kubeconfig) == 0 {
		return res, nil // not published yet; checkCRCKubeconfigHandoff already logged why
	}
	if err := checkGuestAPIReady(ctx, kubeconfig); err != nil {
		log.Info("CRC guest API is not externally ready yet", "error", err)
		return res, nil
	}

	res.ready = true
	res.kubeconfig = kubeconfig
	res.ocpVersion = ocpVersion
	return res, nil
}

// ensureCRCIdentity creates the credentials that stay stable while this
// ClusterInstance exists. VMI recovery intentionally does not delete it.
func (r *ClusterInstanceReconciler) ensureCRCIdentity(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, apiHostname string) (string, error) {
	name := resources.CRCIdentitySecretName(instance.Name)
	existing := &corev1.Secret{}
	key := types.NamespacedName{Name: name, Namespace: instance.Namespace}
	if err := r.Get(ctx, key, existing); err == nil {
		if _, err := resources.CRCIdentityFromSecretData(existing.Data, apiHostname); err != nil {
			return "", fmt.Errorf("validating CRC identity secret %s/%s: %w", key.Namespace, key.Name, err)
		}
		if !metav1.IsControlledBy(existing, instance) {
			return "", fmt.Errorf("CRC identity secret %s/%s is not controlled by this ClusterInstance", key.Namespace, key.Name)
		}
		return name, nil
	} else if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("getting CRC identity secret %s/%s: %w", key.Namespace, key.Name, err)
	}

	identity, err := resources.NewCRCIdentity(apiHostname)
	if err != nil {
		return "", fmt.Errorf("generating CRC identity: %w", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace, Labels: resources.CommonLabels(instance)},
		Type:       corev1.SecretTypeOpaque,
		Data:       identity.SecretData(),
	}
	if err := controllerutil.SetControllerReference(instance, secret, r.Scheme); err != nil {
		return "", fmt.Errorf("setting owner reference on CRC identity secret %s/%s: %w", key.Namespace, key.Name, err)
	}
	if err := r.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("creating CRC identity secret %s/%s: %w", key.Namespace, key.Name, err)
		}
		if err := r.Get(ctx, key, existing); err != nil {
			return "", fmt.Errorf("getting concurrently created CRC identity secret %s/%s: %w", key.Namespace, key.Name, err)
		}
		if _, err := resources.CRCIdentityFromSecretData(existing.Data, apiHostname); err != nil || !metav1.IsControlledBy(existing, instance) {
			return "", fmt.Errorf("validating concurrently created CRC identity secret %s/%s", key.Namespace, key.Name)
		}
	}
	return name, nil
}

// reconcileReadyCRC verifies that the published kubeconfig still belongs to
// the running VMI and can reach the guest API before preserving Ready.
func (r *ClusterInstanceReconciler) reconcileReadyCRC(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (ctrl.Result, error) {
	if err := r.recordCRCIdentity(ctx, instance); err != nil {
		return ctrl.Result{}, err
	}
	// Keep the permissions available for recovery even when no new Job is needed.
	if err := r.ensureCRCAgentRBAC(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling CRC agent permissions: %w", err)
	}
	if instance.Spec.Template.CRCVersion != "" {
		binding := crcBootKeyBinding(instance)
		if binding == nil {
			return r.markCRCBootKeyUnavailable(ctx, instance, fmt.Errorf("CRC boot disk/key binding is missing; automatic recovery is unsafe"))
		}
		if _, pending, err := r.ensureCRCBootKey(ctx, instance, nil); err != nil {
			return r.markCRCBootKeyUnavailable(ctx, instance, err)
		} else if pending {
			return ctrl.Result{RequeueAfter: requeueInterval}, nil
		}
	}
	vmi := &kubevirtv1.VirtualMachineInstance{}
	key := types.NamespacedName{Name: resources.CRCVMName(instance), Namespace: instance.Namespace}
	if err := r.Get(ctx, key, vmi); err != nil {
		if apierrors.IsNotFound(err) {
			return r.invalidateCRCReadiness(ctx, instance, "", "VMIUnavailable", "CRC VirtualMachineInstance is not running")
		}
		return ctrl.Result{}, fmt.Errorf("getting CRC VirtualMachineInstance %s/%s: %w", key.Namespace, key.Name, err)
	}
	if err := r.verifyCRCResource(ctx, instance, vmi); err != nil {
		return ctrl.Result{}, err
	}
	if vmi.Status.Phase != kubevirtv1.Running {
		return r.invalidateCRCReadiness(ctx, instance, string(vmi.UID), "VMIUnavailable", "CRC VirtualMachineInstance is not running")
	}

	previousUID := ""
	if instance.Status.CRC != nil {
		previousUID = instance.Status.CRC.VMIUID
	}
	currentUID := string(vmi.UID)
	if crcVMIChanged(previousUID, currentUID) {
		reason := "VMIReplaced"
		message := "CRC VirtualMachineInstance was replaced; rerunning post-boot setup"
		if previousUID == "" || currentUID == "" {
			reason = "VMIIdentityUnknown"
			message = "CRC VirtualMachineInstance identity was not recorded; rerunning post-boot setup"
		}
		return r.invalidateCRCReadiness(ctx, instance, currentUID, reason, message)
	}

	published := &corev1.Secret{}
	publishedName := resources.KubeconfigSecretName(instance.Name)
	if err := r.Get(ctx, types.NamespacedName{Name: publishedName, Namespace: instance.Namespace}, published); err != nil {
		if apierrors.IsNotFound(err) {
			kubeconfig, ocpVersion, handoffErr := r.checkCRCKubeconfigHandoff(ctx, instance)
			if handoffErr != nil {
				return ctrl.Result{}, handoffErr
			}
			if len(kubeconfig) == 0 {
				return r.markCRCAPIUnavailable(ctx, instance, "KubeconfigUnavailable", "published CRC kubeconfig is missing")
			}
			if err := checkGuestAPIReady(ctx, kubeconfig); err != nil {
				return r.markCRCAPIUnavailable(ctx, instance, "GuestAPIUnavailable", fmt.Sprintf("guest API readiness check failed: %v", err))
			}
			return r.markReady(ctx, instance, ocpVersion, instance.Status.APIEndpoint, kubeconfig)
		}
		return ctrl.Result{}, fmt.Errorf("getting published kubeconfig secret %s/%s: %w", instance.Namespace, publishedName, err)
	}
	if err := r.verifyCRCResource(ctx, instance, published); err != nil {
		return ctrl.Result{}, err
	}
	if err := checkGuestAPIReady(ctx, published.Data[resources.KubeconfigSecretKey]); err != nil {
		return r.markCRCAPIUnavailable(ctx, instance, "GuestAPIUnavailable", fmt.Sprintf("guest API readiness check failed: %v", err))
	}
	if err := r.recordCRCAPIHealth(ctx, instance, metav1.ConditionTrue, "GuestAPIReady", "guest API readiness check succeeded"); err != nil {
		return ctrl.Result{}, err
	}

	result, err := r.reconcileLeaseRefProjection(ctx, instance)
	if err != nil || result.RequeueAfter > 0 {
		return result, err
	}
	return ctrl.Result{RequeueAfter: crcReadyRequeueInterval}, nil
}

// crcBootKeyError marks dependency failures that can recover when the
// bundle source or a conflicting local Secret is corrected.
type crcBootKeyError struct{ error }

func isCRCBootKeyError(err error) bool {
	var keyErr crcBootKeyError
	return errors.As(err, &keyErr)
}

func (r *ClusterInstanceReconciler) markCRCBootKeyUnavailable(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, cause error) (ctrl.Result, error) {
	previous := instance.Status.DeepCopy()
	instance.Status.Phase = brokerv1alpha1.PhaseProvisioning
	instance.Status.KubeconfigSecretRef = corev1.LocalObjectReference{}
	apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type: conditionTypeReady, Status: metav1.ConditionFalse,
		Reason: "BootKeyUnavailable", Message: cause.Error(), ObservedGeneration: instance.Generation,
	})
	if err := r.updateStatusIfChanged(ctx, instance, previous, "updating CRC boot key condition"); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// reconcileProvisioningCRCVMI records the VMI identity before the crc-agent
// handoff can be reused. A changed identity invalidates that handoff first.
func (r *ClusterInstanceReconciler) reconcileProvisioningCRCVMI(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (*ctrl.Result, error) {
	vmi := &kubevirtv1.VirtualMachineInstance{}
	key := types.NamespacedName{Name: resources.CRCVMName(instance), Namespace: instance.Namespace}
	if err := r.Get(ctx, key, vmi); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting CRC VirtualMachineInstance %s/%s: %w", key.Namespace, key.Name, err)
	}
	if err := r.verifyCRCResource(ctx, instance, vmi); err != nil {
		return nil, err
	}

	previousUID := ""
	if instance.Status.CRC != nil {
		previousUID = instance.Status.CRC.VMIUID
	}
	// An initial provisioning reconcile can observe status before
	// ensureCRCBacking records the VMI UID. That is not a VMI replacement.
	if previousUID == "" {
		return nil, nil
	}
	currentUID := string(vmi.UID)
	if !crcVMIChanged(previousUID, currentUID) {
		return nil, nil
	}

	reason := "VMIReplaced"
	message := "CRC VirtualMachineInstance was replaced; rerunning post-boot setup"
	if previousUID == "" || currentUID == "" {
		reason = "VMIIdentityUnknown"
		message = "CRC VirtualMachineInstance identity was not recorded; rerunning post-boot setup"
	}
	result, err := r.invalidateCRCReadiness(ctx, instance, currentUID, reason, message)
	return &result, err
}

func crcVMIChanged(previousUID, currentUID string) bool {
	return previousUID == "" || currentUID == "" || previousUID != currentUID
}

func checkGuestAPIReady(ctx context.Context, kubeconfig []byte) error {
	if len(kubeconfig) == 0 {
		return fmt.Errorf("kubeconfig is empty")
	}
	config, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("parsing kubeconfig: %w", err)
	}
	transport, err := rest.TransportFor(config)
	if err != nil {
		return fmt.Errorf("creating API transport: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(config.Host, "/")+"/readyz", nil)
	if err != nil {
		return fmt.Errorf("creating readiness request: %w", err)
	}
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return fmt.Errorf("requesting guest API readiness: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("guest API returned %s", response.Status)
	}
	return nil
}

// invalidateCRCReadiness removes the one-shot handoff from a previous VMI so
// the next reconcile creates a fresh crc-agent Job for the current VMI.
func (r *ClusterInstanceReconciler) invalidateCRCReadiness(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, vmiUID, reason, message string) (ctrl.Result, error) {
	previousStatus := instance.Status.DeepCopy()
	if instance.Status.CRC != nil && instance.Status.CRC.VMIUID != "" {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentJobName(instance.Name, instance.Status.CRC.VMIUID), Namespace: instance.Namespace}}
		foreground := metav1.DeletePropagationForeground
		pending, err := r.deleteCRCObject(ctx, instance, job, "stale crc-agent Job", client.PropagationPolicy(foreground))
		if err != nil {
			return ctrl.Result{}, err
		}
		if pending {
			return ctrl.Result{RequeueAfter: requeueInterval}, nil
		}
	}
	published := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeconfigSecretName(instance.Name), Namespace: instance.Namespace}}
	pending, err := r.deleteCRCObject(ctx, instance, published, "published CRC kubeconfig secret")
	if err != nil {
		return ctrl.Result{}, err
	}
	if pending {
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	if instance.Status.CRC == nil {
		instance.Status.CRC = &brokerv1alpha1.CRCBackingStatus{}
	}
	if vmiUID != "" {
		instance.Status.CRC.VMIUID = vmiUID
	}
	instance.Status.Phase = brokerv1alpha1.PhaseProvisioning
	instance.Status.KubeconfigSecretRef = corev1.LocalObjectReference{}
	apimeta.RemoveStatusCondition(&instance.Status.Conditions, conditionTypeCRCAgent)
	apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: instance.Generation,
	})
	if err := r.updateStatusIfChanged(ctx, instance, previousStatus, "updating status while recovering CRC"); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

func (r *ClusterInstanceReconciler) recordCRCAPIHealth(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, status metav1.ConditionStatus, reason, message string) error {
	previousStatus := instance.Status.DeepCopy()
	apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:               conditionTypeGuestAPIReachable,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: instance.Generation,
	})
	return r.updateStatusIfChanged(ctx, instance, previousStatus, "updating CRC guest API health")
}

// markCRCAPIUnavailable prevents leases from using an unreachable guest API
// while retaining the VMI handoff and completed agent Job for a later retry.
func (r *ClusterInstanceReconciler) markCRCAPIUnavailable(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, reason, message string) (ctrl.Result, error) {
	previousStatus := instance.Status.DeepCopy()
	instance.Status.Phase = brokerv1alpha1.PhaseProvisioning
	instance.Status.KubeconfigSecretRef = corev1.LocalObjectReference{}
	apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: instance.Generation,
	})
	apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:               conditionTypeGuestAPIReachable,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: instance.Generation,
	})
	if err := r.updateStatusIfChanged(ctx, instance, previousStatus, "updating status while waiting for CRC guest API"); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// checkCRCKubeconfigHandoff reports whether the crc-agent Job has published
// the VMI-specific raw kubeconfig handoff Secret. The controller retains this
// result so it can restore the canonical Secret without rerunning the agent.
// A nil kubeconfig with a nil error means the caller should wait and retry.
func (r *ClusterInstanceReconciler) checkCRCKubeconfigHandoff(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) ([]byte, string, error) {
	log := logf.FromContext(ctx)
	raw := &corev1.Secret{}
	if instance.Status.CRC == nil || instance.Status.CRC.VMIUID == "" {
		return nil, "", nil
	}
	rawName := resources.RawKubeconfigSecretNameForVMI(instance.Name, instance.Status.CRC.VMIUID)
	if err := r.Get(ctx, types.NamespacedName{Name: rawName, Namespace: instance.Namespace}, raw); apierrors.IsNotFound(err) {
		log.Info("CRC VM ready, awaiting crc-agent kubeconfig handoff", "secret", rawName)
		return nil, "", nil
	} else if err != nil {
		return nil, "", fmt.Errorf("getting raw kubeconfig secret %s/%s: %w", instance.Namespace, rawName, err)
	}

	kubeconfig, ok := raw.Data[resources.KubeconfigSecretKey]
	if !ok || len(kubeconfig) == 0 {
		log.Info("raw kubeconfig secret present but missing kubeconfig key, awaiting crc-agent", "secret", rawName)
		return nil, "", nil
	}
	if instance.Status.CRC == nil || raw.Data[resources.VMIUIDSecretKey] == nil || string(raw.Data[resources.VMIUIDSecretKey]) != instance.Status.CRC.VMIUID {
		log.Info("ignoring raw kubeconfig handoff for a different CRC VMI", "secret", rawName)
		return nil, "", nil
	}
	if err := r.verifyCRCResource(ctx, instance, raw); err != nil {
		return nil, "", err
	}
	return kubeconfig, string(raw.Data[resources.OCPVersionSecretKey]), nil
}

// ensureCRCAPIRoute creates the ClusterIP Service and passthrough Route
// that expose a topology=crc instance's guest API server externally, and
// returns the Route's hostname. This call is idempotent. The Service and
// Route persist for the ClusterInstance's whole lifetime; teardownCRCBacking
// tears them down only on deletion. This keeps the hostname stable for the
// duration of a bound lease. When ClusterPoolReconciler provisions a new
// instance after a released instance is deleted, it creates a fresh
// Service, Route, and hostname for that instance.
func (r *ClusterInstanceReconciler) ensureCRCAPIRoute(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (string, error) {
	log := logf.FromContext(ctx)

	svc := resources.BuildCRCAPIService(instance)
	if _, err := r.ensureCRCObject(ctx, instance, svc); err != nil {
		return "", err
	}

	routeKey := types.NamespacedName{Name: resources.CRCAPIRouteName(instance.Name), Namespace: instance.Namespace}
	existingRoute := &routev1.Route{}
	if err := r.Get(ctx, routeKey, existingRoute); err == nil {
		if _, err := r.ensureCRCObject(ctx, instance, existingRoute.DeepCopy()); err != nil {
			return "", err
		}
		if instance.Status.APIEndpoint == "" {
			return "", apiEndpointConflict("existing CRC API Route has no recorded endpoint")
		}
		if existingRoute.Spec.Host == "" {
			return "", apiEndpointConflict("CRC API Route %s/%s has an empty host", routeKey.Namespace, routeKey.Name)
		}
		hostname, err := apiEndpointHostname(instance.Status.APIEndpoint)
		if err != nil {
			return "", err
		}
		if existingRoute.Spec.Host != hostname {
			return "", apiEndpointConflict("CRC Route hostname %q differs from recorded endpoint %q", existingRoute.Spec.Host, hostname)
		}
		if err := r.ensureAPIHostnameAvailable(ctx, existingRoute.Spec.Host, routeKey); err != nil {
			return "", err
		}
		return hostname, nil
	} else if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("getting CRC API Route %s/%s: %w", routeKey.Namespace, routeKey.Name, err)
	}

	host, err := r.crcAPIHostnameForNewRoute(ctx, instance)
	if err != nil {
		return "", err
	}
	if err := r.ensureAPIHostnameAvailable(ctx, host, routeKey); err != nil {
		return "", err
	}
	if err := r.recordCRCAPIEndpoint(ctx, instance, host); err != nil {
		return "", err
	}
	route := resources.BuildCRCAPIRoute(instance, host, svc.Name)
	obj, err := r.ensureCRCObject(ctx, instance, route)
	if err != nil {
		return "", err
	}
	if obj.(*routev1.Route).Spec.Host != host {
		return "", apiEndpointConflict("CRC Route changed hostname during creation")
	}
	log.Info("created CRC API Route", "route", route.Name, "host", host)
	return route.Spec.Host, nil
}

// crcAPIHostnameForNewRoute uses the recorded endpoint when it exists. A new
// instance uses the current naming rule and management ingress domain.
func (r *ClusterInstanceReconciler) crcAPIHostnameForNewRoute(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (string, error) {
	if instance.Status.APIEndpoint != "" {
		return apiEndpointHostname(instance.Status.APIEndpoint)
	}

	domain, err := r.mgmtIngressDomain(ctx)
	if err != nil {
		return "", fmt.Errorf("resolving management cluster ingress domain: %w", err)
	}
	host, err := resources.CRCAPIServerHostname(instance.Name, instance.Namespace, domain)
	if err != nil {
		return "", fmt.Errorf("building CRC API hostname: %w", err)
	}
	return host, nil
}

func (r *ClusterInstanceReconciler) recordCRCAPIEndpoint(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, hostname string) error {
	if instance.Status.APIEndpoint != "" && instance.Status.APIEndpoint != "https://"+hostname {
		return apiEndpointConflict("CRC Route differs from recorded endpoint")
	}
	previous := instance.Status.DeepCopy()
	instance.Status.APIEndpoint = "https://" + hostname
	return r.updateStatusIfChanged(ctx, instance, previous, "recording CRC API endpoint")
}

// mgmtIngressDomain resolves the management cluster's own ingress domain
// (for example, "apps.example.com") from the cluster-scoped
// ingresses.config.openshift.io singleton. Callers use this domain to
// construct a stable per-instance API Route hostname.
func (r *ClusterInstanceReconciler) mgmtIngressDomain(ctx context.Context) (string, error) {
	ingress := &configv1.Ingress{}
	if err := r.Get(ctx, types.NamespacedName{Name: "cluster"}, ingress); err != nil {
		return "", fmt.Errorf("getting management cluster ingress config: %w", err)
	}
	if ingress.Spec.Domain == "" {
		return "", fmt.Errorf("management cluster ingress config has empty spec.domain")
	}
	return ingress.Spec.Domain, nil
}

// teardownCRCBacking deletes all backing objects for a topology=crc
// instance, as part of ClusterInstance deletion (finalizer processing).
// There is no separate in-place "recycle" path. On lease release,
// ClusterLeaseReconciler deletes the ClusterInstance object outright, and
// ClusterPoolReconciler tops up a brand new replacement. The released
// instance goes through this exact same teardown, and the replacement goes
// through the normal creation path. See clusterinstance_controller.go's
// Reconcile doc comment for the rationale.
func (r *ClusterInstanceReconciler) teardownCRCBacking(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (bool, error) {
	if err := r.recordCRCIdentity(ctx, instance); err != nil {
		return false, err
	}
	pending := false
	deleteObject := func(obj client.Object, label string, opts ...client.DeleteOption) error {
		objectPending, err := r.deleteCRCObject(ctx, instance, obj, label, opts...)
		pending = pending || objectPending
		return err
	}

	// Delete all crc-agent Jobs before the VM. VMI-scoped Job names mean more
	// than one completed Job can exist after VMI replacement. Read directly
	// from the API: a stale empty cache list cannot prove that no Job is
	// still using the instance account and boot key.
	jobs := &batchv1.JobList{}
	if err := r.platformReader().List(ctx, jobs, client.InNamespace(instance.Namespace)); err != nil {
		return false, fmt.Errorf("listing crc-agent Jobs: %w", err)
	}
	// Foreground deletion waits for Job pods to stop using the boot key.
	foreground := metav1.DeletePropagationForeground
	jobCount := 0
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if !metav1.IsControlledBy(job, instance) {
			continue
		}
		jobCount++
		if err := deleteObject(&jobs.Items[i], "crc-agent Job", client.PropagationPolicy(foreground)); err != nil {
			return false, err
		}
	}
	// Do not remove the boot key while a Job pod can still be running.
	if jobCount > 0 {
		return true, nil
	}
	rbacPending, err := r.deleteCRCAgentRBAC(ctx, instance)
	if err != nil {
		return false, err
	}
	pending = pending || rbacPending
	if crcBootKeyBinding(instance) != nil {
		copyKey := types.NamespacedName{Name: resources.CRCBootKeySecretName(instance.Name), Namespace: instance.Namespace}
		copy := &corev1.Secret{}
		if err := r.Get(ctx, copyKey, copy); err == nil {
			// An unrelated Secret with the same name is not ours to remove.
			if metav1.IsControlledBy(copy, instance) {
				if err := deleteObject(copy, "CRC boot key secret"); err != nil {
					return false, err
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("getting CRC boot key secret %s: %w", copyKey, err)
		}
	}

	vmCleanupPending, err := r.teardownCRCVMAndStorage(ctx, instance)
	if err != nil {
		return false, err
	}
	pending = pending || vmCleanupPending

	rawSecrets := &corev1.SecretList{}
	if err := r.platformReader().List(ctx, rawSecrets, client.InNamespace(instance.Namespace), client.MatchingLabels{
		resources.LabelManagedBy: "crc-agent",
		resources.LabelInstance:  instance.Name,
	}); err != nil {
		return false, fmt.Errorf("listing CRC handoff secrets: %w", err)
	}
	for i := range rawSecrets.Items {
		if !metav1.IsControlledBy(&rawSecrets.Items[i], instance) {
			continue
		}
		if err := deleteObject(&rawSecrets.Items[i], "raw kubeconfig secret"); err != nil {
			return false, err
		}
	}
	route := &routev1.Route{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.CRCAPIRouteName(instance.Name),
		Namespace: instance.Namespace,
	}}
	if err := deleteObject(route, "CRC API Route"); err != nil {
		return false, err
	}

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.CRCAPIServiceName(instance.Name),
		Namespace: instance.Namespace,
	}}
	if err := deleteObject(svc, "CRC API Service"); err != nil {
		return false, err
	}
	return pending, nil
}

// teardownCRCVMAndStorage halts the CRC VM, requests VM deletion, then deletes
// its VMI. It waits for the VMI and its launcher pod to disappear before it
// deletes the root DataVolume. Each step is safe to repeat on a later reconcile.
func (r *ClusterInstanceReconciler) teardownCRCVMAndStorage(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (bool, error) {
	vmKey := types.NamespacedName{Name: resources.CRCVMName(instance), Namespace: instance.Namespace}
	vm := &kubevirtv1.VirtualMachine{}
	vmExists := true
	if err := r.platformReader().Get(ctx, vmKey, vm); err != nil {
		if apierrors.IsNotFound(err) {
			vmExists = false
		} else {
			return false, fmt.Errorf("getting CRC VirtualMachine %s/%s for teardown: %w", vmKey.Namespace, vmKey.Name, err)
		}
	}

	if vmExists {
		if err := r.verifyCRCResource(ctx, instance, vm); err != nil {
			return false, err
		}
	}
	if vmExists && vm.DeletionTimestamp.IsZero() &&
		(vm.Spec.RunStrategy == nil || *vm.Spec.RunStrategy != kubevirtv1.RunStrategyHalted || vm.Spec.Running != nil) {
		halted := kubevirtv1.RunStrategyHalted
		vm.Spec.RunStrategy = &halted
		vm.Spec.Running = nil
		if err := r.Update(ctx, vm); err != nil {
			return false, fmt.Errorf("halting CRC VirtualMachine %s/%s for teardown: %w", vmKey.Namespace, vmKey.Name, err)
		}
		logf.FromContext(ctx).Info("halted CRC VirtualMachine before deleting its VMI", "virtualMachine", vmKey.Name)
		// Let the VM controller observe the persisted Halted strategy before
		// asking it to remove the VMI. This prevents RunStrategyAlways from
		// recreating the instance during teardown.
		return true, nil
	}

	if vmExists && vm.DeletionTimestamp.IsZero() {
		currentVMI := &kubevirtv1.VirtualMachineInstance{}
		if err := r.platformReader().Get(ctx, vmKey, currentVMI); err == nil {
			if vm.Status.RunStrategy != kubevirtv1.RunStrategyHalted {
				// KubeVirt records the strategy it has processed in VM status.
				// Wait for that acknowledgement before deleting a live VMI, so a
				// controller reconcile based on the prior Always strategy cannot
				// replace the VMI during teardown.
				return true, nil
			}
		} else if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("getting CRC VirtualMachineInstance %s/%s for teardown: %w", vmKey.Namespace, vmKey.Name, err)
		}
	}

	vmPending := false
	var err error
	if vmExists {
		vmForDelete := &kubevirtv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: vmKey.Name, Namespace: vmKey.Namespace}}
		vmPending, err = r.deleteCRCObject(ctx, instance, vmForDelete, "CRC VirtualMachine")
		if err != nil {
			return false, err
		}
	}

	vmi := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{Name: vmKey.Name, Namespace: vmKey.Namespace}}
	vmiPending, err := r.deleteCRCObject(ctx, instance, vmi, "CRC VirtualMachineInstance")
	if err != nil {
		return false, err
	}
	if vmiPending {
		return true, nil
	}

	launcherPods := &corev1.PodList{}
	// A cached empty list does not prove that launcher cleanup is complete.
	if err := r.platformReader().List(ctx, launcherPods,
		client.InNamespace(instance.Namespace),
		client.MatchingLabels{kubevirtv1.DeprecatedVirtualMachineNameLabel: vmKey.Name},
	); err != nil {
		return false, fmt.Errorf("listing CRC virt-launcher pods: %w", err)
	}
	launcherPodPending := false
	for i := range launcherPods.Items {
		pod := &launcherPods.Items[i]
		podPending, err := r.deleteCRCObject(ctx, instance, pod, "CRC virt-launcher pod")
		if err != nil {
			return false, err
		}
		launcherPodPending = launcherPodPending || podPending
	}
	if launcherPodPending {
		return true, nil
	}
	if vmPending {
		return true, nil
	}

	dv := &cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.CRCDiskName(instance),
		Namespace: instance.Namespace,
	}}
	dvPending, err := r.deleteCRCObject(ctx, instance, dv, "CRC DataVolume")
	if err != nil {
		return false, err
	}
	if dvPending {
		return true, nil
	}

	// CDI normally removes the PVC created for this DataVolume. Wait for that
	// claim explicitly as well: the ClusterInstance finalizer must continue to
	// hold pool capacity while storage cleanup lags behind DataVolume deletion.
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.CRCDiskName(instance),
		Namespace: instance.Namespace,
	}}
	pvcPending, err := r.deleteCRCObject(ctx, instance, pvc, "CRC root disk PVC")
	if err != nil {
		return false, err
	}
	return pvcPending, nil
}
