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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CRCBundlePhase represents where a CRCBundle is in its download/verify/extract
// lifecycle.
type CRCBundlePhase string

const (
	// CRCBundlePhasePending means no preparation work has started yet.
	CRCBundlePhasePending CRCBundlePhase = "Pending"
	// CRCBundlePhasePreparing means the bundle-prep Job is downloading, verifying,
	// and extracting the bundle into the golden PVC.
	CRCBundlePhasePreparing CRCBundlePhase = "Preparing"
	// CRCBundlePhaseReady means the golden PVC and derived SSH-key Secret are
	// populated and ClusterInstances may clone from this bundle.
	CRCBundlePhaseReady CRCBundlePhase = "Ready"
	// CRCBundlePhaseFailed means the bundle-prep Job exhausted its retries.
	CRCBundlePhaseFailed CRCBundlePhase = "Failed"
)

// CRCBundleSpec defines the desired state of a cached, version-keyed CRC bundle.
//
// A CRCBundle is the turnkey alternative to manually extracting a .crcbundle and
// hosting crc.qcow2/id_ecdsa_crc yourself. The admin specifies only a version (and,
// on the ClusterPool/ClusterInstance referencing it, a pull secret for runtime
// injection). CRCBundleReconciler downloads the official bundle from the OpenShift
// mirror, verifies its checksum, extracts it, and caches the resulting crc.qcow2
// disk image in a golden PersistentVolumeClaim (plus the bundle's SSH private key
// in a derived Secret). Any number of ClusterPools/ClusterInstances, in any
// namespace, can clone from this bundle via CDI's cross-namespace PVC clone support.
type CRCBundleSpec struct {
	// Version is the CRC/OpenShift Local bundle version to fetch, e.g. "4.16.0".
	// +kubebuilder:validation:Required
	Version string `json:"version"`

	// Arch is the CPU architecture of the bundle to fetch.
	// +optional
	// +kubebuilder:validation:Enum=amd64;arm64
	// +kubebuilder:default=amd64
	Arch string `json:"arch,omitempty"`

	// BundleURL optionally overrides the deterministic official mirror URL
	// (https://mirror.openshift.com/pub/openshift-v4/clients/crc/bundles/openshift/<version>/crc_libvirt_<version>_<arch>.crcbundle),
	// which is derived from Version/Arch when this is left empty.
	// +optional
	BundleURL string `json:"bundleURL,omitempty"`

	// SHA256URL optionally overrides the deterministic mirror checksum URL (the
	// sha256sum.txt published alongside the bundle) used to verify the download.
	// Derived from Version/Arch when left empty.
	// +optional
	SHA256URL string `json:"sha256URL,omitempty"`

	// StorageClassName is the StorageClass used for the golden PVC that caches the
	// extracted crc.qcow2 disk image for this version. For fast per-instance
	// provisioning it should support CSI clone or snapshot; if it does not,
	// ClusterInstances fall back to a full re-import per instance.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`

	// GoldenVolumeSize is the size of the golden PVC that stores the extracted
	// crc.qcow2. Defaults to 35Gi (CRC bundle disks are typically ~31Gi).
	// +optional
	// +kubebuilder:default="35Gi"
	GoldenVolumeSize string `json:"goldenVolumeSize,omitempty"`
}

// CRCBundleStatus defines the observed state of a CRCBundle.
type CRCBundleStatus struct {
	// Phase summarizes where this bundle is in its download/extract lifecycle.
	// +optional
	Phase CRCBundlePhase `json:"phase,omitempty"`

	// QCOW2PVCRef references the golden PersistentVolumeClaim holding the extracted
	// crc.qcow2 disk image, once Phase=Ready. ClusterInstance DataVolumes clone
	// from it via a cross-namespace CDI PVC source.
	// +optional
	QCOW2PVCRef *corev1.LocalObjectReference `json:"qcow2PVCRef,omitempty"`

	// QCOW2PVCNamespace is the namespace QCOW2PVCRef lives in (the operator's own
	// namespace, where the bundle-prep Job runs).
	// +optional
	QCOW2PVCNamespace string `json:"qcow2PVCNamespace,omitempty"`

	// SSHKeySecretRef references the Secret (in QCOW2PVCNamespace) holding the
	// bundle's id_ecdsa_crc SSH private key under data key "id_ecdsa", derived
	// automatically during extraction.
	// +optional
	SSHKeySecretRef *corev1.LocalObjectReference `json:"sshKeySecretRef,omitempty"`

	// OCPVersion is the OpenShift version reported by the bundle's own
	// crc-bundle-info.json metadata, recorded here once extraction succeeds.
	// +optional
	OCPVersion string `json:"ocpVersion,omitempty"`

	// SHA256 is the verified checksum of the downloaded .crcbundle artifact.
	// +optional
	SHA256 string `json:"sha256,omitempty"`

	// Conditions represent the latest available observations of the bundle's state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=crcb
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="Arch",type=string,JSONPath=`.spec.arch`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="OCPVersion",type=string,JSONPath=`.status.ocpVersion`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CRCBundle is the Schema for the crcbundles API. It is cluster-scoped and
// version-keyed (see internal/resources.CRCBundleName) so that a version is
// downloaded and extracted at most once and shared by every ClusterPool /
// ClusterInstance across all namespaces that reference it.
type CRCBundle struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CRCBundleSpec   `json:"spec,omitempty"`
	Status CRCBundleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CRCBundleList contains a list of CRCBundle.
type CRCBundleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CRCBundle `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CRCBundle{}, &CRCBundleList{})
}
