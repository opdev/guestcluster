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

// ClusterTopology identifies the shape of the provisioned guest cluster.
// +kubebuilder:validation:Enum=crc;hcp
type ClusterTopology string

const (
	// TopologyCRC is a single-node OpenShift cluster (CodeReady Containers / OpenShift Local)
	// running as a nested KubeVirt VirtualMachine on the hypervisor cluster.
	TopologyCRC ClusterTopology = "crc"
	// TopologyHCP is a HyperShift-hosted cluster. Worker NodePool replica count is
	// controlled by ClusterTemplate.NodePoolReplicas (default 1) and control-plane
	// availability by ClusterTemplate.ControllerAvailabilityPolicy (default
	// SingleReplica). Both are independent, non-topology-encoded choices.
	TopologyHCP ClusterTopology = "hcp"
)

// ControllerAvailabilityPolicy selects the HostedCluster control-plane
// availability for topology=hcp. This type is locally defined (rather than
// importing HyperShift's own hyperv1beta1.AvailabilityPolicy into this API
// package), so callers compare against these constants instead of an
// upstream enum's string form. internal/resources maps this type to
// hyperv1beta1.AvailabilityPolicy when building the HostedCluster.
// +kubebuilder:validation:Enum=SingleReplica;HighlyAvailable
type ControllerAvailabilityPolicy string

const (
	// AvailabilityPolicySingleReplica runs a single replica of each
	// control-plane component. The default: see ClusterTemplate's
	// ControllerAvailabilityPolicy doc comment for why.
	AvailabilityPolicySingleReplica ControllerAvailabilityPolicy = "SingleReplica"
	// AvailabilityPolicyHighlyAvailable spreads control-plane components
	// across multiple replicas for resilience, at the cost of requiring more
	// schedulable management-cluster capacity.
	AvailabilityPolicyHighlyAvailable ControllerAvailabilityPolicy = "HighlyAvailable"
)

// ClusterTemplate describes how new ClusterInstances for a pool should be provisioned.
type ClusterTemplate struct {
	// CRCVersion, when set (topology=crc only), is the turnkey path. The admin
	// specifies only a CRC/OpenShift Local version (e.g. "4.16.0"), and the operator
	// downloads, verifies, and extracts the official bundle for that version via a
	// cluster-scoped CRCBundle. The operator auto-creates this CRCBundle and reuses it
	// across pools/instances (see api/v1alpha1/crcbundle_types.go), then clones its
	// cached crc.qcow2 and SSH key for this instance. When set, ReleaseImage and
	// BundleSSHKeyRef below are ignored (they are derived from the CRCBundle instead).
	// Ignored for topology=hcp.
	// +optional
	CRCVersion string `json:"crcVersion,omitempty"`

	// CRCArch is the CPU architecture of the CRC bundle to fetch when CRCVersion is
	// set. Defaults to "amd64" if left empty. Ignored when CRCVersion is unset, and
	// for topology=hcp.
	// +optional
	// +kubebuilder:validation:Enum=amd64;arm64
	CRCArch string `json:"crcArch,omitempty"`

	// ReleaseImage is the OCP release payload image used to provision the guest
	// cluster. For topology=crc this is a FALLBACK to the turnkey CRCVersion path
	// above. When CRCVersion is unset, this MUST be an HTTP(S) URL to the
	// *extracted* crc.qcow2 disk image from an official CRC/OpenShift Local
	// .crcbundle (NOT the .crcbundle tar itself). For example, extract via
	// `curl <bundle-url> | tar --zstd -xf -` and host the resulting crc.qcow2 at a
	// URL reachable by CDI's HTTP DataVolume importer. For topology=hcp this is
	// always the HyperShift/OCP release image pullspec (required, CRCVersion does
	// not apply).
	// +optional
	ReleaseImage string `json:"releaseImage,omitempty"`

	// OCPVersion is the expected/recorded OpenShift version for clusters provisioned from
	// this template (e.g. "4.16.10"). It is echoed back on ClusterInstance.status.ocpVersion
	// once the guest cluster reports its own version, and is used to detect drift.
	// +kubebuilder:validation:Required
	OCPVersion string `json:"ocpVersion"`

	// NodePoolReplicas is the number of worker replicas for topology=hcp. Ignored
	// for topology=crc. Defaults to 1 when unset.
	// +optional
	// +kubebuilder:validation:Minimum=1
	NodePoolReplicas *int32 `json:"nodePoolReplicas,omitempty"`

	// ControllerAvailabilityPolicy sets the HostedCluster control-plane
	// availability for topology=hcp: SingleReplica or HighlyAvailable. Defaults
	// to SingleReplica, since HighlyAvailable's 3-way etcd/kube-apiserver spread
	// requires anti-affinity across that many schedulable, untainted nodes on
	// the management cluster, which small/dev management clusters (e.g.
	// CRC-hosted) often don't have. HighlyAvailable is therefore opt-in.
	// This field maps directly to HostedCluster.spec.controllerAvailabilityPolicy,
	// which is immutable once the HostedCluster is created. Changing this field
	// on an existing ClusterTemplate has no effect on already-provisioned
	// instances. Ignored for topology=crc.
	// +optional
	// +kubebuilder:default=SingleReplica
	ControllerAvailabilityPolicy ControllerAvailabilityPolicy `json:"controllerAvailabilityPolicy,omitempty"`

	// Memory is the amount of memory allocated per VM (CRC VM, or each HyperShift KubeVirt
	// worker VM). E.g. "16Gi" for CRC, "6Gi" for a HyperShift worker.
	// +kubebuilder:validation:Required
	Memory string `json:"memory"`

	// Cores is the number of vCPUs allocated per VM.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Cores int32 `json:"cores"`

	// RootVolumeSize is the size of the root disk (e.g. "35Gi").
	// +optional
	RootVolumeSize string `json:"rootVolumeSize,omitempty"`

	// PullSecretRef references a Secret (in the operator's namespace) containing the
	// pull-secret used to pull the release payload / CRC bundle images. This field is
	// optional. When unset, the operator defaults to the management cluster's own
	// global pull secret (the "pull-secret" Secret in the "openshift-config"
	// namespace), which is present on every OpenShift cluster. Set this field
	// explicitly to use a different/narrower credential (e.g. one scoped to a
	// disconnected mirror).
	// +optional
	PullSecretRef corev1.LocalObjectReference `json:"pullSecretRef,omitempty"`

	// IDMSRef optionally references an ImageDigestMirrorSet-shaped ConfigMap applied at
	// provision time for disconnected/mirrored registries. Interpreted per-topology:
	// for hcp it seeds HostedCluster.spec.imageContentSources; for crc it is applied
	// as an ImageDigestMirrorSet inside the guest cluster post-boot.
	// +optional
	IDMSRef *corev1.LocalObjectReference `json:"idmsRef,omitempty"`

	// BundleSSHKeyRef references a Secret (in the operator's namespace) containing the
	// CRC bundle's SSH private key (the "id_ecdsa_crc" file shipped inside an official
	// .crcbundle, used to reach the booted VM as user "core"), under a data key named
	// "id_ecdsa" or "ssh-privatekey". This field is a FALLBACK. When CRCVersion is set,
	// the operator derives the SSH key automatically from the referenced CRCBundle
	// instead, and this field is ignored. When CRCVersion is unset, this field is
	// required for topology=crc: the crc-agent Job uses this key to SSH into the
	// freshly booted CRC VM and run the post-boot fixups natively (start kubelet,
	// approve kubelet CSRs, inject the real pull secret, set credentials, rewrite the
	// kubeconfig server to the externally-routable API Route hostname the
	// ClusterInstance controller provisions). Ignored for topology=hcp.
	// +optional
	BundleSSHKeyRef *corev1.LocalObjectReference `json:"bundleSSHKeyRef,omitempty"`

	// HCPWorkerSSHKeyRef optionally references a Secret (in the operator's namespace)
	// containing an SSH public key, under a data key named "id_rsa.pub", to inject as
	// an authorized key for the "core" user on every hcp NodePool worker (via
	// HostedCluster.spec.sshKey, see HyperShift's own ignition machine-config
	// generation). This field is only a debugging convenience (e.g. to inspect a
	// worker that is stuck before ever registering as a Node) and has no effect on
	// cluster function. Most deployments should leave it unset. Ignored for
	// topology=crc (see BundleSSHKeyRef for that path's own, unrelated SSH mechanism).
	// +optional
	HCPWorkerSSHKeyRef *corev1.LocalObjectReference `json:"hcpWorkerSSHKeyRef,omitempty"`

	// VMNodeSelector constrains which hypervisor node(s) the guest VM(s) are scheduled to.
	// +optional
	VMNodeSelector map[string]string `json:"vmNodeSelector,omitempty"`

	// StorageClassName is the StorageClass used for VM root/data volumes.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`
}

// ClusterPoolSpec defines the desired state of ClusterPool.
// +kubebuilder:validation:XValidation:rule="self.maxSize >= self.minSize",message="maxSize must be greater than or equal to minSize"
// +kubebuilder:validation:XValidation:rule="self.maxSize >= self.warmSpares",message="maxSize must be greater than or equal to warmSpares"
type ClusterPoolSpec struct {
	// Type is the topology of guest clusters this pool manages.
	// +kubebuilder:validation:Required
	Type ClusterTopology `json:"type"`

	// MaxSize is the hard budget cap: the pool will never have more than this many
	// ClusterInstances (Ready+Leased+Provisioning) at once. Enforced by the
	// ClusterPool controller and used by the ClusterLease controller to decide
	// whether a new instance may be provisioned on-demand.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	MaxSize int32 `json:"maxSize"`

	// MinSize is the minimum number of ClusterInstances (any non-terminal phase,
	// Provisioning, Ready, or Leased) the pool controller keeps in existence at all
	// times, independent of lease demand. Unlike WarmSpares, this is a stable,
	// total-count floor. An instance transitioning Ready->Leased (or back) does not
	// change how many instances count against it, so top-up/scale-down against
	// MinSize cannot race with ClusterLease binding. 0 (the default) means the pool
	// may shrink to zero instances when there is no demand and WarmSpares is also 0
	// (pure on-demand provisioning). Subject to MaxSize.
	// +optional
	// +kubebuilder:default=0
	MinSize int32 `json:"minSize,omitempty"`

	// WarmSpares is the number of Ready, unleased ClusterInstances the pool
	// controller tries to keep provisioned ahead of demand, so that a ClusterLease
	// can bind instantly instead of waiting for a full provision. This floor is
	// measured against spare (available) capacity, so it rises with load. Under N
	// active leases the pool targets roughly N+WarmSpares total instances. Subject
	// to MaxSize. (Formerly named MinAvailable.)
	// +optional
	// +kubebuilder:default=0
	WarmSpares int32 `json:"warmSpares,omitempty"`

	// Template describes how to provision new ClusterInstances for this pool.
	// +kubebuilder:validation:Required
	Template ClusterTemplate `json:"template"`
}

// ClusterPoolStatus defines the observed state of ClusterPool.
type ClusterPoolStatus struct {
	// TotalInstances is the current count of ClusterInstances owned by this pool
	// (any phase except Failed/deleted).
	TotalInstances int32 `json:"totalInstances,omitempty"`

	// AvailableInstances is the count of Ready, unleased ClusterInstances.
	AvailableInstances int32 `json:"availableInstances,omitempty"`

	// LeasedInstances is the count of ClusterInstances currently bound to a ClusterLease.
	LeasedInstances int32 `json:"leasedInstances,omitempty"`

	// Conditions represent the latest available observations of the pool's state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cpool
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Max",type=integer,JSONPath=`.spec.maxSize`
// +kubebuilder:printcolumn:name="Total",type=integer,JSONPath=`.status.totalInstances`
// +kubebuilder:printcolumn:name="Available",type=integer,JSONPath=`.status.availableInstances`
// +kubebuilder:printcolumn:name="Leased",type=integer,JSONPath=`.status.leasedInstances`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.status.conditions[?(@.type=="CapacityAvailable")].reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClusterPool is the Schema for the clusterpools API. It declares a budgeted pool of
// guest OpenShift clusters (CRC or HyperShift) that CI jobs can lease from via
// ClusterLease objects.
type ClusterPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterPoolSpec   `json:"spec,omitempty"`
	Status ClusterPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterPoolList contains a list of ClusterPool.
type ClusterPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterPool{}, &ClusterPoolList{})
}
