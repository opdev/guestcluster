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
	"bytes"
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kubevirtv1 "kubevirt.io/api/core/v1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

const (
	// instanceFinalizer ensures backing VMs/HostedClusters are torn down
	// before the ClusterInstance object itself is removed.
	instanceFinalizer = "guestcluster.opdev.io/cleanup"

	// requeueInterval is used while waiting for slow external operations
	// (VM boot, HostedCluster provisioning) to progress.
	requeueInterval = 20 * time.Second

	conditionTypeReady             = "Ready"
	conditionTypeVersionMismatch   = "VersionMismatch"
	conditionTypeGuestAPIReachable = "GuestAPIReachable"
)

// ClusterInstanceReconciler reconciles a ClusterInstance object
type ClusterInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=clusterleases,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachineinstances,verbs=get;list;watch
// +kubebuilder:rbac:groups=cdi.kubevirt.io,resources=datavolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hypershift.openshift.io,resources=hostedclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hypershift.openshift.io,resources=nodepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=crcbundles,verbs=get;list;watch
// +kubebuilder:rbac:groups=cdi.kubevirt.io,resources=datavolumes/source,verbs=create
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes/custom-host,verbs=create;update
// +kubebuilder:rbac:groups=config.openshift.io,resources=ingresses,verbs=get;list;watch

// Reconcile drives a ClusterInstance through Provisioning -> Ready. It
// delegates the actual creation and inspection of backing objects (KubeVirt
// VM+DataVolume for topology=crc, HostedCluster+NodePool for topology=hcp)
// to the per-topology helpers in clusterinstance_crc.go and
// clusterinstance_hypershift.go.
//
// There is no "Leased" phase and no in-place "recycle" phase. Whether a
// ClusterLease currently claims an instance is not part of the instance's
// own lifecycle; it is a fact about demand, and the single source of truth
// for that fact is ClusterLease.Status.InstanceRef (see
// clusterlease_types.go), never anything stored here. This mirrors how a
// Kubernetes Node has no "occupied" phase: occupancy is derived by listing
// Pods with matching Spec.NodeName, not stored on the Node. When a
// ClusterLease releases its claimed instance, ClusterLeaseReconciler
// deletes the ClusterInstance object outright (see releaseBoundInstance)
// rather than resetting it back to Provisioning. All leasable instances are
// pool-owned and interchangeable and anonymous from the pool's perspective,
// and nothing depends on a stable identity, Service, or Route hostname
// across lease cycles. So a plain delete, followed by
// ClusterPoolReconciler's normal top-up, is simpler than a dedicated
// teardown-and-recreate-in-place phase, and it reuses the exact same
// (already necessary) finalizer-driven teardown path used for any other
// ClusterInstance deletion.
//
// Phase semantics:
//   - Provisioning: backing objects exist but are not yet reporting a
//     usable kubeconfig.
//   - Ready: Reconcile has published the kubeconfig to
//     KubeconfigSecretName. Whether a lease currently claims the instance
//     is orthogonal; see Status.LeaseRef, a read-only derived projection
//     that reconcileLeaseRefProjection below maintains for observability
//     only.
//   - Failed: set when provisioning hits an unrecoverable error, or when
//     Reconcile detects a hard version mismatch.
func (r *ClusterInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	instance := &brokerv1alpha1.ClusterInstance{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("ClusterInstance deleted, nothing to reconcile")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting ClusterInstance: %w", err)
	}

	// Handle deletion: tear down backing objects, then drop our finalizer.
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, instance)
	}

	if !controllerutil.ContainsFinalizer(instance, instanceFinalizer) {
		controllerutil.AddFinalizer(instance, instanceFinalizer)
		if err := r.Update(ctx, instance); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		// The Update call above already triggers a new reconcile via the
		// watch, so no explicit requeue is needed here.
		return ctrl.Result{}, nil
	}

	if instance.Status.Phase == brokerv1alpha1.PhaseReady {
		if instance.Spec.Type == brokerv1alpha1.TopologyCRC {
			return r.reconcileReadyCRC(ctx, instance)
		}
		return r.reconcileLeaseRefProjection(ctx, instance)
	}

	switch instance.Spec.Type {
	case brokerv1alpha1.TopologyCRC:
		return r.reconcileCRC(ctx, instance)
	case brokerv1alpha1.TopologyHCP:
		return r.reconcileHyperShift(ctx, instance)
	default:
		return r.markFailed(ctx, instance, fmt.Errorf("unknown topology %q", instance.Spec.Type))
	}
}

// reconcileLeaseRefProjection maintains Status.LeaseRef as a read-only,
// derived mirror of whichever ClusterLease (if any) currently names this
// instance via its own Status.InstanceRef, the single source of truth for
// the binding (see clusterlease_types.go). This projection exists only for
// observability, for example the "Lease" column on `kubectl get
// clusterinstance`; nothing reads this field to make a scheduling or
// lifecycle decision. reconcileLeaseRefProjection uses the
// leaseInstanceRefIndexField index that ClusterLeaseReconciler registers
// (shared manager cache, so no re-registration is needed here).
func (r *ClusterInstanceReconciler) reconcileLeaseRefProjection(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (ctrl.Result, error) {
	leaseList := &brokerv1alpha1.ClusterLeaseList{}
	if err := r.List(ctx, leaseList,
		client.InNamespace(instance.Namespace),
		client.MatchingFields{leaseInstanceRefIndexField: instance.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing ClusterLeases claiming instance %s: %w", instance.Name, err)
	}

	var claimant *corev1.LocalObjectReference
	for i := range leaseList.Items {
		l := &leaseList.Items[i]
		if l.DeletionTimestamp.IsZero() {
			claimant = &corev1.LocalObjectReference{Name: l.Name}
			break
		}
	}

	if (claimant == nil) == (instance.Status.LeaseRef == nil) &&
		(claimant == nil || claimant.Name == instance.Status.LeaseRef.Name) {
		// Already in sync; avoid a needless write/requeue.
		return ctrl.Result{}, nil
	}

	instance.Status.LeaseRef = claimant
	if err := r.Status().Update(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating derived LeaseRef projection: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *ClusterInstanceReconciler) reconcileCRC(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (ctrl.Result, error) {
	if result, err := r.reconcileProvisioningCRCVMI(ctx, instance); result != nil || err != nil {
		if result == nil {
			return ctrl.Result{}, err
		}
		return *result, err
	}

	pullSecretName, err := r.resolvePullSecret(ctx, instance, instance.Namespace)
	if err != nil {
		return r.markFailedWithReason(ctx, instance, "InvalidPullSecret", err)
	}
	// When Template.CRCVersion is set (turnkey path), reconcileCRC derives
	// the SSH key used to reach the booted VM automatically from the
	// referenced CRCBundle's SSHKeySecretRef (always data key "id_ecdsa"),
	// so it skips the manual BundleSSHKeyRef precheck entirely. That
	// precheck only applies to the fallback path, where an admin supplies
	// releaseImage/bundleSSHKeyRef directly.
	var bundleKeyDataKey string
	if instance.Spec.Template.CRCVersion == "" {
		bundleKeyDataKey, err = r.validateBundleSSHKey(ctx, instance)
		if err != nil {
			return r.markFailedWithReason(ctx, instance, "MissingBundleSSHKey", err)
		}
	}

	res, err := r.ensureCRCBacking(ctx, instance, bundleKeyDataKey, pullSecretName)
	if err != nil {
		return r.markFailed(ctx, instance, err)
	}

	instance.Status.CRC = &brokerv1alpha1.CRCBackingStatus{
		VMName:         res.vmName,
		DataVolumeName: res.dvName,
		SSHEndpoint:    res.sshEndpoint,
		VMIUID:         res.vmiUID,
	}

	if !res.ready {
		instance.Status.Phase = brokerv1alpha1.PhaseProvisioning
		if err := r.Status().Update(ctx, instance); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating status while provisioning CRC: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	return r.markReady(ctx, instance, res.ocpVersion, res.apiEndpoint, res.kubeconfig)
}

func (r *ClusterInstanceReconciler) reconcileHyperShift(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (ctrl.Result, error) {
	// resources.DefaultHostedClusterNamespace, where every hcp instance's
	// HostedCluster, NodePool, and pull-secret copy live, has no guaranteed
	// creator. This differs from instance.Namespace (created by whatever
	// provisioned the ClusterPool) and the operator's own namespace
	// (created by its Deployment manifests). Ensure it exists idempotently,
	// rather than treating it as an undocumented manual prerequisite.
	if err := r.ensureNamespace(ctx, resources.DefaultHostedClusterNamespace); err != nil {
		return r.markFailedWithReason(ctx, instance, "NamespaceEnsureFailed", err)
	}

	pullSecretName, err := r.resolvePullSecret(ctx, instance, resources.DefaultHostedClusterNamespace)
	if err != nil {
		return r.markFailedWithReason(ctx, instance, "InvalidPullSecret", err)
	}

	res, err := r.ensureHyperShiftBacking(ctx, instance, pullSecretName)
	if err != nil {
		return r.markFailed(ctx, instance, err)
	}

	instance.Status.HyperShift = &brokerv1alpha1.HyperShiftBackingStatus{
		HostedClusterName:      res.hostedClusterName,
		HostedClusterNamespace: resources.DefaultHostedClusterNamespace,
		NodePoolNames:          res.nodePoolNames,
	}

	if !res.ready {
		instance.Status.Phase = brokerv1alpha1.PhaseProvisioning
		if err := r.Status().Update(ctx, instance); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating status while provisioning HyperShift: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	return r.markReady(ctx, instance, res.ocpVersion, res.apiEndpoint, res.kubeconfig)
}

// markReady publishes the kubeconfig to the canonical per-instance Secret,
// checks for a version mismatch against the template's expected OCPVersion,
// and transitions the instance to Ready.
func (r *ClusterInstanceReconciler) markReady(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, ocpVersion, apiEndpoint string, kubeconfig []byte) (ctrl.Result, error) {
	secretName := resources.KubeconfigSecretName(instance.Name)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: instance.Namespace,
			Labels:    resources.CommonLabels(instance),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			resources.KubeconfigSecretKey: kubeconfig,
			resources.OCPVersionSecretKey: []byte(ocpVersion),
		},
	}
	// The owner reference lets Kubernetes garbage-collect this secret
	// automatically when it deletes the ClusterInstance, instead of
	// leaving a now-useless kubeconfig behind after cleanup.
	if err := controllerutil.SetControllerReference(instance, secret, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting owner reference on kubeconfig secret %s/%s: %w", instance.Namespace, secretName, err)
	}

	// changed also covers a missing owner reference, so secrets created
	// before owner-reference support was added still get backfilled once.
	if err := r.upsertSecret(ctx, secret, func(existing *corev1.Secret) bool {
		return !bytes.Equal(existing.Data[resources.KubeconfigSecretKey], kubeconfig) ||
			string(existing.Data[resources.OCPVersionSecretKey]) != ocpVersion ||
			len(existing.OwnerReferences) == 0
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("upserting kubeconfig secret %s/%s: %w", instance.Namespace, secretName, err)
	}

	instance.Status.Phase = brokerv1alpha1.PhaseReady
	instance.Status.OCPVersion = ocpVersion
	instance.Status.Topology = instance.Spec.Type
	instance.Status.APIEndpoint = apiEndpoint
	instance.Status.KubeconfigSecretRef = corev1.LocalObjectReference{Name: secretName}
	instance.Status.ObservedGeneration = instance.Generation

	readyCondition := metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "BackingResourcesAvailable",
		Message:            "Guest cluster is provisioned and kubeconfig is available",
		ObservedGeneration: instance.Generation,
	}

	if instance.Spec.Template.OCPVersion != "" && ocpVersion != "" && instance.Spec.Template.OCPVersion != ocpVersion {
		// A version mismatch is a hard-fail signal for CI (per the
		// acceptance criteria), not a controller error: the cluster is
		// usable but does not match what was requested. markReady surfaces
		// this via a distinct condition instead of failing the instance
		// outright, so CI can decide whether to treat it as a hard fail or
		// a documented skip.
		apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
			Type:               conditionTypeVersionMismatch,
			Status:             metav1.ConditionTrue,
			Reason:             "OCPVersionMismatch",
			Message:            fmt.Sprintf("requested OCPVersion %q but observed %q", instance.Spec.Template.OCPVersion, ocpVersion),
			ObservedGeneration: instance.Generation,
		})
	} else {
		apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
			Type:               conditionTypeVersionMismatch,
			Status:             metav1.ConditionFalse,
			Reason:             "OCPVersionMatches",
			Message:            "observed OCPVersion matches template",
			ObservedGeneration: instance.Generation,
		})
	}
	apimeta.SetStatusCondition(&instance.Status.Conditions, readyCondition)

	if err := r.Status().Update(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status to Ready: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *ClusterInstanceReconciler) markFailed(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, cause error) (ctrl.Result, error) {
	return r.markFailedWithReason(ctx, instance, "ReconcileError", cause)
}

// markFailedWithReason is markFailed with a caller-supplied condition Reason,
// used by prechecks (missing/invalid pull secret or bundle SSH key) so
// misconfiguration surfaces as a distinct, actionable reason instead of the
// generic "ReconcileError".
func (r *ClusterInstanceReconciler) markFailedWithReason(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, reason string, cause error) (ctrl.Result, error) {
	instance.Status.Phase = brokerv1alpha1.PhaseFailed
	apimeta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            cause.Error(),
		ObservedGeneration: instance.Generation,
	})
	if err := r.Status().Update(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status to Failed (original error: %v): %w", cause, err)
	}
	// Return the original error so the controller-runtime work queue retries
	// with backoff.
	return ctrl.Result{}, cause
}

// ensureNamespace guarantees that the namespace named name exists, creating
// it if necessary. name is caller-supplied (e.g.
// resources.DefaultHostedClusterNamespace for hcp backing objects) rather
// than hardcoded here, since callers may target different namespaces.
func (r *ClusterInstanceReconciler) ensureNamespace(ctx context.Context, name string) error {
	ns := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: name}, ns); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("getting namespace %s: %w", name, err)
	}

	ns = &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
	if err := r.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating namespace %s: %w", name, err)
	}
	return nil
}

// resolvePullSecret returns the name (in targetNamespace) of the Secret to
// use as instance's pull-secret, and materializes it there if necessary.
//
// If Template.PullSecretRef is set, resolvePullSecret uses that Secret
// (which must exist in instance.Namespace and carry the dockerconfigjson
// data key) as-is. This is the explicit-override path, for example for
// disconnected or mirrored registries that need a narrower or different
// credential than the management cluster's own.
//
// If Template.PullSecretRef is unset, the operator defaults to a copy of
// the management cluster's own global pull secret
// (ClusterPullSecretNamespace/ClusterPullSecretName), which is present on
// every OpenShift cluster and already contains
// quay.io/openshift-release-dev credentials, exactly what provisioning a
// guest OCP/CRC cluster needs. resolvePullSecret materializes the copy
// under resources.DefaultPullSecretName(instance.Name) in targetNamespace
// (kept up to date on drift), so downstream consumers (crc-agent Job
// mount, HostedCluster.Spec.PullSecret) always reference a concrete Secret
// in their own namespace, the same as the explicit-ref path.
//
// reconcileCRC and reconcileHyperShift check this before creating any
// backing objects, so misconfiguration (missing ref, missing cluster
// default) surfaces immediately as Phase=Failed rather than a late, opaque
// provisioning error.
func (r *ClusterInstanceReconciler) resolvePullSecret(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, targetNamespace string) (string, error) {
	ref := instance.Spec.Template.PullSecretRef
	if ref.Name != "" {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: instance.Namespace}, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return "", fmt.Errorf("pull secret %s/%s not found", instance.Namespace, ref.Name)
			}
			return "", fmt.Errorf("getting pull secret %s/%s: %w", instance.Namespace, ref.Name, err)
		}
		if len(secret.Data[resources.PullSecretDataKey]) == 0 {
			return "", fmt.Errorf("pull secret %s/%s is missing data key %q", instance.Namespace, ref.Name, resources.PullSecretDataKey)
		}
		return ref.Name, nil
	}

	clusterSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: resources.ClusterPullSecretName, Namespace: resources.ClusterPullSecretNamespace}, clusterSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("template.pullSecretRef is unset and the cluster's default pull secret %s/%s was not found", resources.ClusterPullSecretNamespace, resources.ClusterPullSecretName)
		}
		return "", fmt.Errorf("getting cluster default pull secret %s/%s: %w", resources.ClusterPullSecretNamespace, resources.ClusterPullSecretName, err)
	}
	data := clusterSecret.Data[resources.PullSecretDataKey]
	if len(data) == 0 {
		return "", fmt.Errorf("cluster default pull secret %s/%s is missing data key %q", resources.ClusterPullSecretNamespace, resources.ClusterPullSecretName, resources.PullSecretDataKey)
	}

	copyName := resources.DefaultPullSecretName(instance.Name)
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      copyName,
			Namespace: targetNamespace,
			Labels:    resources.CommonLabels(instance),
		},
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{resources.PullSecretDataKey: data},
	}
	// Owner references only work within the same namespace as the owner.
	// For topologies where targetNamespace == instance.Namespace (crc),
	// this lets Kubernetes garbage-collect the copy automatically alongside
	// the instance, the same as the canonical kubeconfig secret. For
	// cross-namespace targets (hcp's HostedCluster namespace), teardown
	// cleans it up explicitly instead.
	if targetNamespace == instance.Namespace {
		if err := controllerutil.SetControllerReference(instance, desired, r.Scheme); err != nil {
			return "", fmt.Errorf("setting owner reference on default pull secret %s/%s: %w", targetNamespace, copyName, err)
		}
	}

	if err := r.upsertSecret(ctx, desired, func(existing *corev1.Secret) bool {
		return !bytes.Equal(existing.Data[resources.PullSecretDataKey], data)
	}); err != nil {
		return "", fmt.Errorf("upserting default pull secret %s/%s: %w", targetNamespace, copyName, err)
	}

	return copyName, nil
}

// validateBundleSSHKey fails fast if the instance's template does not
// reference a bundle SSH key Secret (required for topology=crc only), or if
// that Secret does not exist or does not contain any of the recognized
// bundle SSH key data keys. It returns the data key name found, so the
// caller can pass it through to resources.BuildCRCAgentJob for the Secret
// volume item remap.
func (r *ClusterInstanceReconciler) validateBundleSSHKey(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (string, error) {
	ref := instance.Spec.Template.BundleSSHKeyRef
	if ref == nil || ref.Name == "" {
		return "", fmt.Errorf("template.bundleSSHKeyRef is required for topology=crc but not set")
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: instance.Namespace}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("bundle SSH key secret %s/%s not found", instance.Namespace, ref.Name)
		}
		return "", fmt.Errorf("getting bundle SSH key secret %s/%s: %w", instance.Namespace, ref.Name, err)
	}
	for _, key := range resources.BundleSSHKeyDataKeys {
		if len(secret.Data[key]) > 0 {
			return key, nil
		}
	}
	return "", fmt.Errorf("bundle SSH key secret %s/%s does not contain any of the expected data keys %v", instance.Namespace, ref.Name, resources.BundleSSHKeyDataKeys)
}

// reconcileDelete tears down backing objects and removes the finalizer so
// the ClusterInstance object can be garbage collected.
func (r *ClusterInstanceReconciler) reconcileDelete(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(instance, instanceFinalizer) {
		return ctrl.Result{}, nil
	}

	if instance.Status.Phase != brokerv1alpha1.PhaseTerminating {
		instance.Status.Phase = brokerv1alpha1.PhaseTerminating
		if err := r.Status().Update(ctx, instance); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating status to Terminating: %w", err)
		}
	}

	var err error
	switch instance.Spec.Type {
	case brokerv1alpha1.TopologyCRC:
		err = r.teardownCRCBacking(ctx, instance)
	case brokerv1alpha1.TopologyHCP:
		err = r.teardownHyperShiftBacking(ctx, instance)
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("tearing down backing resources: %w", err)
	}

	controllerutil.RemoveFinalizer(instance, instanceFinalizer)
	if err := r.Update(ctx, instance); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// instanceForLease maps a ClusterLease event to a reconcile.Request for the
// instance it currently claims (Status.InstanceRef). This updates that
// instance's derived LeaseRef projection (see reconcileLeaseRefProjection)
// promptly on bind or release, instead of waiting for an unrelated
// trigger.
func (r *ClusterInstanceReconciler) instanceForLease(_ context.Context, obj client.Object) []reconcile.Request {
	lease, ok := obj.(*brokerv1alpha1.ClusterLease)
	if !ok || lease.Status.InstanceRef == nil {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: client.ObjectKey{Namespace: lease.Namespace, Name: lease.Status.InstanceRef.Name},
	}}
}

// instanceForVMI maps the deterministically named CRC VMI to its
// ClusterInstance. This lets the controller recover as soon as KubeVirt
// replaces a VMI, instead of waiting for an unrelated instance update.
func (r *ClusterInstanceReconciler) instanceForVMI(_ context.Context, obj client.Object) []reconcile.Request {
	vmi, ok := obj.(*kubevirtv1.VirtualMachineInstance)
	if !ok {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: client.ObjectKey{Namespace: vmi.Namespace, Name: vmi.Name},
	}}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&brokerv1alpha1.ClusterInstance{}).
		Watches(&brokerv1alpha1.ClusterLease{}, handler.EnqueueRequestsFromMapFunc(r.instanceForLease)).
		Watches(&kubevirtv1.VirtualMachineInstance{}, handler.EnqueueRequestsFromMapFunc(r.instanceForVMI)).
		Named("clusterinstance").
		Complete(r)
}
