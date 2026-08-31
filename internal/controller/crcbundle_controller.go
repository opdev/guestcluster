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
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

// crcBundleRequeueInterval is how often a CRCBundle whose prep Job is still
// running gets re-checked, as a safety net alongside the Owns(&batchv1.Job{})
// watch below (which should fire promptly on completion/failure anyway).
const crcBundleRequeueInterval = 20 * time.Second

// conditionTypeBundleReady is the Condition Type recorded on CRCBundle.status.
const conditionTypeBundleReady = "Ready"

// CRCBundleReconciler turns a version-keyed CRCBundle request into a
// downloaded, verified, extracted crc.qcow2, cached in a golden
// PersistentVolumeClaim, plus a derived SSH-key Secret, by running a
// one-shot bundle-prep Job. It is the turnkey counterpart to hosting a
// crc.qcow2 manually: an admin, or more commonly ClusterPoolReconciler
// acting on their behalf (see clusterpool_controller.go's ensureCRCBundle),
// only needs to specify a version. Every ClusterPool and ClusterInstance
// across every namespace that references the same version shares the SAME
// CRCBundle and its golden PVC, because CRCBundle is cluster-scoped (see
// resources.CRCBundleName).
type CRCBundleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=crcbundles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=crcbundles/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=guestcluster.opdev.io,resources=crcbundles/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

func (r *CRCBundleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	bundle := &brokerv1alpha1.CRCBundle{}
	if err := r.Get(ctx, req.NamespacedName, bundle); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting CRCBundle %s: %w", req.Name, err)
	}

	// CRCBundle is cluster-scoped, but every object it manages, the golden
	// and scratch PVCs it creates directly, plus the Secret and ConfigMap
	// the prep Job's own script creates with an inline ownerReference, is
	// namespaced and owned by it. So plain Kubernetes garbage collection
	// handles cleanup on delete, and no finalizer is needed here.
	if !bundle.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	switch bundle.Status.Phase {
	case brokerv1alpha1.CRCBundlePhaseReady:
		return r.reconcileReady(ctx, bundle)
	case brokerv1alpha1.CRCBundlePhasePreparing:
		return r.reconcilePreparing(ctx, bundle)
	case brokerv1alpha1.CRCBundlePhaseFailed:
		return r.reconcileFailed(ctx, bundle)
	default:
		return r.reconcilePending(ctx, bundle)
	}
}

// reconcilePending creates the golden+scratch PVCs and the bundle-prep Job, then
// transitions to Preparing.
func (r *CRCBundleReconciler) reconcilePending(ctx context.Context, bundle *brokerv1alpha1.CRCBundle) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if err := r.ensurePVC(ctx, bundle, resources.BuildGoldenPVC(bundle)); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensurePVC(ctx, bundle, resources.BuildScratchPVC(bundle)); err != nil {
		return ctrl.Result{}, err
	}

	bundleURL, sha256URL := resources.ResolveBundleURLs(bundle)
	job := resources.BuildBundlePrepJob(bundle, bundleURL, sha256URL, bundlePrepImage())
	if err := controllerutil.SetControllerReference(bundle, job, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting owner reference on bundle-prep Job: %w", err)
	}
	if err := r.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &batchv1.Job{}); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("creating bundle-prep Job %s/%s: %w", job.Namespace, job.Name, err)
		}
		log.Info("created bundle-prep Job", "job", job.Name, "bundleURL", bundleURL)
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting bundle-prep Job %s/%s: %w", job.Namespace, job.Name, err)
	}

	bundle.Status.Phase = brokerv1alpha1.CRCBundlePhasePreparing
	setBundleCondition(bundle, metav1.ConditionFalse, "Preparing", "downloading and extracting the CRC bundle")
	if err := r.Status().Update(ctx, bundle); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating CRCBundle status: %w", err)
	}
	return ctrl.Result{Requeue: true}, nil
}

// ensurePVC creates pvc (setting bundle as its controller owner) if it does not
// already exist, tolerating AlreadyExists races.
func (r *CRCBundleReconciler) ensurePVC(ctx context.Context, bundle *brokerv1alpha1.CRCBundle, pvc *corev1.PersistentVolumeClaim) error {
	if err := controllerutil.SetControllerReference(bundle, pvc, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference on PVC %s: %w", pvc.Name, err)
	}
	err := r.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, &corev1.PersistentVolumeClaim{})
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}
	return nil
}

// reconcilePreparing watches the bundle-prep Job to completion: on success it
// reads back the derived metadata ConfigMap, confirms the SSH-key Secret exists,
// records everything on status, and cleans up the (no longer needed) bundle-prep
// Job and scratch PVC; on permanent failure it transitions to Failed.
func (r *CRCBundleReconciler) reconcilePreparing(ctx context.Context, bundle *brokerv1alpha1.CRCBundle) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	arch := crcBundleArch(bundle)

	job := &batchv1.Job{}
	jobName := resources.BundlePrepJobName(bundle.Spec.Version, arch)
	if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: resources.OperatorNamespace()}, job); err != nil {
		if apierrors.IsNotFound(err) {
			// The Job was deleted unexpectedly. Go back to Pending to
			// recreate it.
			bundle.Status.Phase = brokerv1alpha1.CRCBundlePhasePending
			if err := r.Status().Update(ctx, bundle); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting bundle-prep Job %s: %w", jobName, err)
	}

	if job.Status.Failed > 0 && (job.Spec.BackoffLimit == nil || job.Status.Failed > *job.Spec.BackoffLimit) {
		bundle.Status.Phase = brokerv1alpha1.CRCBundlePhaseFailed
		setBundleCondition(bundle, metav1.ConditionFalse, "PrepJobFailed", fmt.Sprintf("bundle-prep Job %s failed (backoffLimit exhausted)", jobName))
		if err := r.Status().Update(ctx, bundle); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if job.Status.Succeeded == 0 {
		return ctrl.Result{RequeueAfter: crcBundleRequeueInterval}, nil
	}

	cmName := resources.BundleMetadataConfigMapName(bundle.Spec.Version, arch)
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: resources.OperatorNamespace()}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("bundle-prep Job succeeded but metadata configmap not published yet, waiting", "configMap", cmName)
			return ctrl.Result{RequeueAfter: crcBundleRequeueInterval}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting bundle metadata configmap %s: %w", cmName, err)
	}

	sshSecretName := resources.BundleSSHKeySecretName(bundle.Spec.Version, arch)
	if err := r.Get(ctx, types.NamespacedName{Name: sshSecretName, Namespace: resources.OperatorNamespace()}, &corev1.Secret{}); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("bundle-prep Job succeeded but SSH key secret not published yet, waiting", "secret", sshSecretName)
			return ctrl.Result{RequeueAfter: crcBundleRequeueInterval}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting bundle SSH key secret %s: %w", sshSecretName, err)
	}

	bundle.Status.Phase = brokerv1alpha1.CRCBundlePhaseReady
	bundle.Status.QCOW2PVCRef = &corev1.LocalObjectReference{Name: resources.GoldenPVCName(bundle.Spec.Version, arch)}
	bundle.Status.QCOW2PVCNamespace = resources.OperatorNamespace()
	bundle.Status.SSHKeySecretRef = &corev1.LocalObjectReference{Name: sshSecretName}
	bundle.Status.OCPVersion = cm.Data["ocpVersion"]
	bundle.Status.SHA256 = cm.Data["sha256"]
	setBundleCondition(bundle, metav1.ConditionTrue, "BundleReady", "golden PVC and SSH key are ready to be cloned")
	if err := r.Status().Update(ctx, bundle); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating CRCBundle status: %w", err)
	}
	log.Info("CRCBundle ready", "version", bundle.Spec.Version, "arch", arch, "ocpVersion", bundle.Status.OCPVersion)

	// The bundle-prep Job's completed Pod still references the scratch PVC
	// in its spec (both are mounted into the same Pod; see
	// BuildBundlePrepJob). Kubernetes' pvc-protection finalizer on this
	// cluster does not release the PVC merely because the Pod's phase is
	// terminal; it stays blocked until the Pod object itself is gone.
	// Without this delete, the scratch PVC below would sit in Terminating
	// indefinitely, unblocked only incidentally whenever someone deletes
	// the CRCBundle itself, which cascades via the Job's owner reference to
	// delete the Job and Pod too. Deleting the Job here (Background
	// propagation also deletes its Pods) is safe, because its outputs
	// (golden PVC, metadata ConfigMap, SSH key Secret) have already been
	// consumed above. This delete is best-effort: failing to delete it is
	// not worth failing reconciliation over.
	background := metav1.DeletePropagationBackground
	if err := r.Delete(ctx, job, client.PropagationPolicy(background)); err != nil && !apierrors.IsNotFound(err) {
		log.Info("failed to delete bundle-prep Job after bundle became ready, ignoring", "job", jobName, "error", err.Error())
	}

	// The scratch PVC is no longer needed once the golden PVC is populated;
	// deleting it reclaims most of the transient storage footprint. This
	// delete is best-effort: failing to delete it is not worth failing
	// reconciliation over.
	scratch := &corev1.PersistentVolumeClaim{}
	scratchName := resources.ScratchPVCName(bundle.Spec.Version, arch)
	if err := r.Get(ctx, types.NamespacedName{Name: scratchName, Namespace: resources.OperatorNamespace()}, scratch); err == nil {
		if err := r.Delete(ctx, scratch); err != nil && !apierrors.IsNotFound(err) {
			log.Info("failed to delete scratch PVC after bundle became ready, ignoring", "pvc", scratchName, "error", err.Error())
		}
	}

	return ctrl.Result{}, nil
}

// reconcileFailed allows recovery if an operator manually deletes the failed
// bundle-prep Job (e.g. after fixing a transient mirror outage): reconciliation
// then starts over from Pending.
func (r *CRCBundleReconciler) reconcileFailed(ctx context.Context, bundle *brokerv1alpha1.CRCBundle) (ctrl.Result, error) {
	arch := crcBundleArch(bundle)
	jobName := resources.BundlePrepJobName(bundle.Spec.Version, arch)
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: resources.OperatorNamespace()}, &batchv1.Job{})
	if apierrors.IsNotFound(err) {
		bundle.Status.Phase = brokerv1alpha1.CRCBundlePhasePending
		if err := r.Status().Update(ctx, bundle); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// reconcileReady self-heals if the golden PVC or SSH key Secret disappear out
// of band (e.g. accidental manual deletion), re-triggering preparation.
func (r *CRCBundleReconciler) reconcileReady(ctx context.Context, bundle *brokerv1alpha1.CRCBundle) (ctrl.Result, error) {
	arch := crcBundleArch(bundle)
	ns := resources.OperatorNamespace()

	goldenName := resources.GoldenPVCName(bundle.Spec.Version, arch)
	if err := r.Get(ctx, types.NamespacedName{Name: goldenName, Namespace: ns}, &corev1.PersistentVolumeClaim{}); apierrors.IsNotFound(err) {
		return r.resetToPending(ctx, bundle, "golden PVC is missing")
	} else if err != nil {
		return ctrl.Result{}, err
	}

	sshSecretName := resources.BundleSSHKeySecretName(bundle.Spec.Version, arch)
	if err := r.Get(ctx, types.NamespacedName{Name: sshSecretName, Namespace: ns}, &corev1.Secret{}); apierrors.IsNotFound(err) {
		return r.resetToPending(ctx, bundle, "SSH key secret is missing")
	} else if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *CRCBundleReconciler) resetToPending(ctx context.Context, bundle *brokerv1alpha1.CRCBundle, reason string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("CRCBundle no longer ready, re-preparing", "reason", reason)
	bundle.Status.Phase = brokerv1alpha1.CRCBundlePhasePending
	setBundleCondition(bundle, metav1.ConditionFalse, "Reprovisioning", reason)
	if err := r.Status().Update(ctx, bundle); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func setBundleCondition(bundle *brokerv1alpha1.CRCBundle, status metav1.ConditionStatus, reason, message string) {
	apimeta.SetStatusCondition(&bundle.Status.Conditions, metav1.Condition{
		Type:               conditionTypeBundleReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: bundle.Generation,
	})
}

// crcBundleArch returns bundle.Spec.Arch, defaulting to DefaultCRCArch when
// empty.
func crcBundleArch(bundle *brokerv1alpha1.CRCBundle) string {
	if bundle.Spec.Arch != "" {
		return bundle.Spec.Arch
	}
	return resources.DefaultCRCArch
}

// bundlePrepImage resolves the container image used for the bundle-prep
// Job. Per project decision, bundlePrepImage reuses the crc-agent image,
// because that image already needs curl/tar/jq/oc-adjacent tooling for its
// own native post-boot fixups. So bundlePrepImage shares
// CRCAgentImageEnvVar/DefaultCRCAgentImage rather than introducing a
// separate override point.
func bundlePrepImage() string {
	if img := os.Getenv(resources.CRCAgentImageEnvVar); img != "" {
		return img
	}
	return resources.DefaultCRCAgentImage
}

// SetupWithManager sets up the controller with the Manager.
func (r *CRCBundleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&brokerv1alpha1.CRCBundle{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&batchv1.Job{}).
		Named("crcbundle").
		Complete(r)
}
