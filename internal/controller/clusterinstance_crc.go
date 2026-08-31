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
	"fmt"
	"os"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"

	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

// crcResult carries the outcome of reconciling a topology=crc instance's
// backing objects. The caller (Reconcile) uses this outcome to decide phase
// transitions, so this function does not need to know about phase
// semantics itself.
type crcResult struct {
	ready       bool
	ocpVersion  string
	kubeconfig  []byte
	vmName      string
	dvName      string
	sshEndpoint string
	// apiEndpoint is the externally-routable URL of the guest API server
	// (the passthrough Route host, see ensureCRCAPIRoute). markReady copies
	// it into ClusterInstanceStatus.APIEndpoint / ClusterLeaseStatus.APIEndpoint.
	apiEndpoint string
}

// crcAgentImage resolves the container image for the per-instance
// crc-agent Job. Operator deployments can override it with the
// CRC_AGENT_IMAGE environment variable on the manager Deployment (an
// OLM RELATED_IMAGE-style override point). If unset, crcAgentImage falls
// back to a default image for local and dev use.
func crcAgentImage() string {
	if img := os.Getenv(resources.CRCAgentImageEnvVar); img != "" {
		return img
	}
	return resources.DefaultCRCAgentImage
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
	res := crcResult{
		vmName: resources.VMName(instance.Name),
		dvName: resources.DataVolumeName(instance.Name),
	}

	// Resolve the DataVolume source and the SSH key the crc-agent Job uses.
	// These differ between the turnkey path (CRCVersion set, clone from a
	// shared CRCBundle) and the manual/fallback path (releaseImage +
	// BundleSSHKeyRef). See api/v1alpha1/clusterpool_types.go's
	// ClusterTemplate doc comments for the full contract.
	tmpl := instance.Spec.Template
	var dv *cdiv1beta1.DataVolume
	var sshSecretName, sshDataKey string
	if tmpl.CRCVersion != "" {
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
				return res, nil
			}
			return res, fmt.Errorf("getting CRCBundle %s: %w", bundleName, err)
		}
		if bundle.Status.Phase != brokerv1alpha1.CRCBundlePhaseReady {
			log.Info(resources.BundleNotReadyMessage(bundle))
			return res, nil
		}
		dv = resources.BuildCRCDataVolumeFromBundle(instance, bundle)
		sshSecretName = bundle.Status.SSHKeySecretRef.Name
		sshDataKey = "id_ecdsa" // fixed data key written by the bundle-prep script
	} else {
		dv = resources.BuildCRCDataVolume(instance)
		sshSecretName = tmpl.BundleSSHKeyRef.Name
		sshDataKey = bundleKeyDataKey
	}

	if err := r.Get(ctx, types.NamespacedName{Name: dv.Name, Namespace: dv.Namespace}, &cdiv1beta1.DataVolume{}); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, dv); err != nil && !apierrors.IsAlreadyExists(err) {
			return res, fmt.Errorf("creating CRC DataVolume %s/%s: %w", dv.Namespace, dv.Name, err)
		}
		log.Info("created CRC DataVolume", "dataVolume", dv.Name)
	} else if err != nil {
		return res, fmt.Errorf("getting CRC DataVolume %s/%s: %w", dv.Namespace, dv.Name, err)
	}

	vm := resources.BuildCRCVirtualMachine(instance, res.dvName)
	existingVM := &kubevirtv1.VirtualMachine{}
	if err := r.Get(ctx, types.NamespacedName{Name: vm.Name, Namespace: vm.Namespace}, existingVM); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, vm); err != nil && !apierrors.IsAlreadyExists(err) {
			return res, fmt.Errorf("creating CRC VirtualMachine %s/%s: %w", vm.Namespace, vm.Name, err)
		}
		log.Info("created CRC VirtualMachine", "virtualMachine", vm.Name)
		return res, nil // freshly created, definitely not ready yet
	} else if err != nil {
		return res, fmt.Errorf("getting CRC VirtualMachine %s/%s: %w", vm.Namespace, vm.Name, err)
	}

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

	// Ensure the crc-agent Job exists. It SSHes into the VM and runs the
	// post-boot fixups natively (see cmd/crc-agent). This call is
	// idempotent: once created, the Job runs to completion, or exhausts its
	// BackoffLimit, on its own.
	job := resources.BuildCRCAgentJob(instance, vmIP, sshSecretName, sshDataKey, crcAgentImage(), apiHost, pullSecretName)
	if err := r.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &batchv1.Job{}); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			return res, fmt.Errorf("creating crc-agent Job %s/%s: %w", job.Namespace, job.Name, err)
		}
		log.Info("created crc-agent Job", "job", job.Name, "vmIP", vmIP)
	} else if err != nil {
		return res, fmt.Errorf("getting crc-agent Job %s/%s: %w", job.Namespace, job.Name, err)
	}

	// Once the crc-agent Job completes successfully, it publishes the raw
	// kubeconfig and observed OCP version handoff Secret. Check whether it
	// has done so yet. This raw secret is transient: markReady deletes it
	// once markReady has durably copied its contents into the canonical
	// <instance>-kubeconfig secret (see markReady). It is normal for the raw
	// secret to be gone again on later reconciles of an already-Ready
	// instance.
	raw := &corev1.Secret{}
	rawName := resources.RawKubeconfigSecretName(instance.Name)
	if err := r.Get(ctx, types.NamespacedName{Name: rawName, Namespace: instance.Namespace}, raw); apierrors.IsNotFound(err) {
		// Fall back to the canonical published secret if the handoff
		// already happened on a previous reconcile. Without this fallback,
		// a Ready instance reconciling again (for example, on a periodic
		// resync) would regress to Provisioning, because the one-time raw
		// secret it originally consumed no longer exists.
		published := &corev1.Secret{}
		publishedName := resources.KubeconfigSecretName(instance.Name)
		if getErr := r.Get(ctx, types.NamespacedName{Name: publishedName, Namespace: instance.Namespace}, published); getErr == nil {
			if kubeconfig := published.Data[resources.KubeconfigSecretKey]; len(kubeconfig) > 0 {
				res.ready = true
				res.kubeconfig = kubeconfig
				res.ocpVersion = string(published.Data[resources.OCPVersionSecretKey])
				return res, nil
			}
		} else if !apierrors.IsNotFound(getErr) {
			return res, fmt.Errorf("getting published kubeconfig secret %s/%s: %w", instance.Namespace, publishedName, getErr)
		}
		log.Info("CRC VM ready, awaiting crc-agent kubeconfig handoff", "secret", rawName)
		return res, nil
	} else if err != nil {
		return res, fmt.Errorf("getting raw kubeconfig secret %s/%s: %w", instance.Namespace, rawName, err)
	}

	kubeconfig, ok := raw.Data[resources.KubeconfigSecretKey]
	if !ok || len(kubeconfig) == 0 {
		log.Info("raw kubeconfig secret present but missing kubeconfig key, awaiting crc-agent", "secret", rawName)
		return res, nil
	}

	res.ready = true
	res.kubeconfig = kubeconfig
	res.ocpVersion = string(raw.Data[resources.OCPVersionSecretKey])
	return res, nil
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
	if err := r.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &corev1.Service{}); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, svc); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("creating CRC API Service %s/%s: %w", svc.Namespace, svc.Name, err)
		}
		log.Info("created CRC API Service", "service", svc.Name)
	} else if err != nil {
		return "", fmt.Errorf("getting CRC API Service %s/%s: %w", svc.Namespace, svc.Name, err)
	}

	domain, err := r.mgmtIngressDomain(ctx)
	if err != nil {
		return "", fmt.Errorf("resolving management cluster ingress domain: %w", err)
	}
	host := resources.APIServerHostname(instance.Name, domain)

	route := resources.BuildCRCAPIRoute(instance, host, svc.Name)
	existingRoute := &routev1.Route{}
	if err := r.Get(ctx, types.NamespacedName{Name: route.Name, Namespace: route.Namespace}, existingRoute); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, route); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("creating CRC API Route %s/%s: %w", route.Namespace, route.Name, err)
		}
		log.Info("created CRC API Route", "route", route.Name, "host", host)
		return host, nil
	} else if err != nil {
		return "", fmt.Errorf("getting CRC API Route %s/%s: %w", route.Namespace, route.Name, err)
	}
	// The Route already exists. Its host is authoritative because
	// ensureCRCAPIRoute fixed it at creation time and never mutates it.
	// Return the existing host rather than the freshly-computed one, in
	// case the ingress domain has changed since.
	return existingRoute.Spec.Host, nil
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
func (r *ClusterInstanceReconciler) teardownCRCBacking(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) error {
	// Delete the crc-agent Job first. It references the VM's IP, and its
	// Pods should not keep running, or be left orphaned, against a VM that
	// is about to be torn down.
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.CRCAgentJobName(instance.Name),
		Namespace: instance.Namespace,
	}}
	background := metav1.DeletePropagationBackground
	if err := r.deleteIfExists(ctx, job, "crc-agent Job", client.PropagationPolicy(background)); err != nil {
		return err
	}

	vm := &kubevirtv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.VMName(instance.Name),
		Namespace: instance.Namespace,
	}}
	if err := r.deleteIfExists(ctx, vm, "CRC VirtualMachine"); err != nil {
		return err
	}

	dv := &cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.DataVolumeName(instance.Name),
		Namespace: instance.Namespace,
	}}
	if err := r.deleteIfExists(ctx, dv, "CRC DataVolume"); err != nil {
		return err
	}

	raw := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.RawKubeconfigSecretName(instance.Name),
		Namespace: instance.Namespace,
	}}
	if err := r.deleteIfExists(ctx, raw, "stale raw kubeconfig secret"); err != nil {
		return err
	}

	route := &routev1.Route{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.CRCAPIRouteName(instance.Name),
		Namespace: instance.Namespace,
	}}
	if err := r.deleteIfExists(ctx, route, "CRC API Route"); err != nil {
		return err
	}

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.CRCAPIServiceName(instance.Name),
		Namespace: instance.Namespace,
	}}
	return r.deleteIfExists(ctx, svc, "CRC API Service")
}
