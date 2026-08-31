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

// ClusterInstancePhase is the lifecycle phase of a ClusterInstance.
//
// This phase deliberately has no "Leased" value. Whether a ClusterLease
// claims an instance is not part of the instance's own lifecycle. It is a
// fact about demand, derived from ClusterLease.Status.InstanceRef, the
// single source of truth for the binding (see clusterlease_types.go). This
// design mirrors a Kubernetes Node, which has no "occupied" phase; a Node's
// use is derived by listing Pods with a matching Spec.NodeName, never stored
// on the Node itself. An instance's own lifecycle has exactly four states:
// it is being created (Provisioning), it is usable (Ready), it failed
// (Failed), or it is being torn down (Terminating), regardless of whether a
// lease currently claims it. See Status.LeaseRef for the read-only, derived
// projection of which lease currently claims a Ready instance, if any.
// +kubebuilder:validation:Enum=Provisioning;Ready;Failed;Terminating
type ClusterInstancePhase string

const (
	// PhaseProvisioning means the backing VM/HostedCluster is being created and has not
	// yet reported a usable kubeconfig.
	PhaseProvisioning ClusterInstancePhase = "Provisioning"
	// PhaseReady means the instance has a valid kubeconfig and is usable. It may or may
	// not currently be claimed by a ClusterLease. See Status.LeaseRef.
	PhaseReady ClusterInstancePhase = "Ready"
	// PhaseFailed means provisioning failed and the instance requires operator/human
	// intervention or deletion.
	PhaseFailed ClusterInstancePhase = "Failed"
	// PhaseTerminating means the instance and its backing resources are being torn down.
	PhaseTerminating ClusterInstancePhase = "Terminating"
)

// ClusterInstanceSpec defines the desired state of ClusterInstance.
type ClusterInstanceSpec struct {
	// Type is the topology of this guest cluster.
	// +kubebuilder:validation:Required
	Type ClusterTopology `json:"type"`

	// PoolRef names the owning ClusterPool in the same namespace. Empty for
	// standalone instances created outside of pool management.
	// +optional
	PoolRef corev1.LocalObjectReference `json:"poolRef,omitempty"`

	// Template is a (possibly pool-inherited) copy of the provisioning parameters used
	// to create this instance.
	// +kubebuilder:validation:Required
	Template ClusterTemplate `json:"template"`
}

// CRCBackingStatus tracks the KubeVirt VM backing a topology=crc instance.
type CRCBackingStatus struct {
	// VMName is the name of the KubeVirt VirtualMachine running the CRC/SNO bundle.
	VMName string `json:"vmName,omitempty"`
	// DataVolumeName is the CDI DataVolume providing the VM's root disk.
	DataVolumeName string `json:"dataVolumeName,omitempty"`
	// SSHEndpoint is host:port used by the crc-agent to reach the CRC VM for
	// post-boot fixups and kubeconfig extraction.
	SSHEndpoint string `json:"sshEndpoint,omitempty"`
}

// HyperShiftBackingStatus tracks the HostedCluster/NodePool backing a topology=hcp
// instance.
type HyperShiftBackingStatus struct {
	// HostedClusterName is the name of the hypershift.openshift.io/v1beta1 HostedCluster.
	HostedClusterName string `json:"hostedClusterName,omitempty"`
	// HostedClusterNamespace is the namespace holding the HostedCluster (conventionally
	// "clusters").
	HostedClusterNamespace string `json:"hostedClusterNamespace,omitempty"`
	// NodePoolNames lists the NodePool(s) backing this instance's workers.
	NodePoolNames []string `json:"nodePoolNames,omitempty"`
}

// ClusterInstanceStatus defines the observed state of ClusterInstance.
type ClusterInstanceStatus struct {
	// Phase is the current lifecycle phase.
	Phase ClusterInstancePhase `json:"phase,omitempty"`

	// OCPVersion is the OpenShift version reported by the running guest
	// cluster (as opposed to Spec.Template.OCPVersion, which is the requested version).
	// A mismatch between requested and observed is surfaced via the VersionMismatch
	// condition and must be treated by CI as a hard fail unless explicitly waived.
	OCPVersion string `json:"ocpVersion,omitempty"`

	// Topology echoes Spec.Type once the instance is Ready, for convenient consumption
	// as an explicit CI output alongside OCPVersion.
	Topology ClusterTopology `json:"topology,omitempty"`

	// APIEndpoint is the guest cluster's externally reachable API server URL.
	APIEndpoint string `json:"apiEndpoint,omitempty"`

	// KubeconfigSecretRef names the Secret (in this ClusterInstance's namespace)
	// containing the guest cluster's admin kubeconfig under key "kubeconfig".
	KubeconfigSecretRef corev1.LocalObjectReference `json:"kubeconfigSecretRef,omitempty"`

	// LeaseRef names the ClusterLease currently claiming this instance, if any.
	// ClusterInstanceReconciler maintains this READ-ONLY, DERIVED projection for
	// observability only (e.g. the "Lease" column on `kubectl get clusterinstance`).
	// This field is never authoritative, and nothing, including the ClusterPool and
	// ClusterInstance controllers themselves, makes scheduling/lifecycle decisions
	// based on it. The single source of truth for the binding is
	// ClusterLease.Status.InstanceRef. ClusterInstanceReconciler keeps this field in
	// sync with that value by watching ClusterLeases, so it may lag by one reconcile.
	LeaseRef *corev1.LocalObjectReference `json:"leaseRef,omitempty"`

	// CRC holds backing-object references for topology=crc instances.
	// +optional
	CRC *CRCBackingStatus `json:"crc,omitempty"`

	// HyperShift holds backing-object references for topology=hcp instances.
	// +optional
	HyperShift *HyperShiftBackingStatus `json:"hyperShift,omitempty"`

	// ObservedGeneration is the generation last reconciled.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the instance's state,
	// e.g. type=Ready, type=VersionMismatch.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cinst
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.status.ocpVersion`
// +kubebuilder:printcolumn:name="Topology",type=string,JSONPath=`.status.topology`
// +kubebuilder:printcolumn:name="Lease",type=string,JSONPath=`.status.leaseRef.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClusterInstance is the Schema for the clusterinstances API. It represents a single
// concrete guest OpenShift cluster (CRC VM or HyperShift hosted cluster) managed by
// this operator.
type ClusterInstance struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterInstanceSpec   `json:"spec,omitempty"`
	Status ClusterInstanceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterInstanceList contains a list of ClusterInstance.
type ClusterInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterInstance `json:"items"`
}
