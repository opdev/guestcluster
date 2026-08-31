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

// ClusterLeasePhase is the lifecycle phase of a ClusterLease claim.
// +kubebuilder:validation:Enum=Pending;Bound;Releasing;Released;Failed
type ClusterLeasePhase string

const (
	// PhaseLeasePending means no matching Ready ClusterInstance has been bound yet.
	// The ClusterLease controller is either waiting for one to free up or has
	// triggered on-demand provisioning (subject to pool MaxSize).
	PhaseLeasePending ClusterLeasePhase = "Pending"
	// PhaseLeaseBound means a ClusterInstance is exclusively bound to this lease and
	// its kubeconfig Secret is available.
	PhaseLeaseBound ClusterLeasePhase = "Bound"
	// PhaseLeaseReleasing means the lease is being torn down and its instance is being
	// recycled.
	PhaseLeaseReleasing ClusterLeasePhase = "Releasing"
	// PhaseLeaseReleased is a terminal phase set just before the ClusterLease object
	// itself is deleted by the requester; kept for observability in edge cases.
	PhaseLeaseReleased ClusterLeasePhase = "Released"
	// PhaseLeaseFailed means binding or provisioning failed (e.g. pool at MaxSize with
	// no available instance, or provisioning error propagated from ClusterInstance).
	PhaseLeaseFailed ClusterLeasePhase = "Failed"
)

// ClusterLeaseSpec defines the desired state of ClusterLease.
type ClusterLeaseSpec struct {
	// PoolRef names the ClusterPool to lease from, in this namespace. The
	// referenced ClusterPool's Spec.Type determines the requested guest
	// cluster topology (echoed back on Status.Topology once bound). This
	// field does not duplicate the topology, because a ClusterPool name is
	// unique within a namespace and already unambiguously implies its type.
	// +kubebuilder:validation:Required
	PoolRef corev1.LocalObjectReference `json:"poolRef"`

	// TTL bounds how long the lease may remain Bound before the controller forcibly
	// releases and recycles the underlying instance, protecting the pool from CI jobs
	// that fail to release. A value of 0 (or omitted) means no TTL enforcement.
	// +optional
	TTL *metav1.Duration `json:"ttl,omitempty"`

	// RequestedBy is a free-form identifier of the CI job/run requesting this lease,
	// recorded for audit purposes only.
	// +optional
	RequestedBy string `json:"requestedBy,omitempty"`
}

// ClusterLeaseStatus defines the observed state of ClusterLease.
type ClusterLeaseStatus struct {
	// Phase is the current lifecycle phase of the lease.
	Phase ClusterLeasePhase `json:"phase,omitempty"`

	// InstanceRef names the ClusterInstance claimed by this lease, once Phase=Bound.
	// This field is THE SINGLE SOURCE OF TRUTH for the lease<->instance binding
	// (analogous to a Kubernetes Pod's Spec.NodeName). ClusterLeaseReconciler writes
	// it exactly once, atomically with Phase transitioning to Bound, and never
	// duplicates it onto the ClusterInstance side. An instance is considered
	// claimed/in-use the moment a non-terminal (Pending or Bound) ClusterLease's
	// InstanceRef names it. The instance is "assumed" immediately, even before it
	// necessarily exists yet in some accounting paths. This design ensures that
	// ClusterPoolReconciler's supply accounting and ClusterInstance's derived
	// LeaseRef projection can never observe a state where the binding is ambiguous
	// or split across two independently-written records. (Formerly named
	// BoundInstanceRef.)
	InstanceRef *corev1.LocalObjectReference `json:"instanceRef,omitempty"`

	// KubeconfigSecretRef names the Secret (in this ClusterLease's namespace) containing
	// the bound guest cluster's admin kubeconfig under key "kubeconfig".
	// ClusterLeaseReconciler copies/mirrors this Secret from the bound ClusterInstance's
	// kubeconfig Secret, so that CI can consume a single, stable location regardless of
	// which instance backs the lease.
	KubeconfigSecretRef *corev1.LocalObjectReference `json:"kubeconfigSecretRef,omitempty"`

	// OCPVersion is the OpenShift version of the bound guest cluster. This field is an
	// explicit CI output alongside Topology, expected to be recorded next to the
	// artifact OCP line.
	OCPVersion string `json:"ocpVersion,omitempty"`

	// Topology echoes the bound instance's topology (crc | hcp). This field is an
	// explicit CI output. A mismatch between Spec.Type and this value should never
	// occur, and indicates a controller bug if observed.
	Topology ClusterTopology `json:"topology,omitempty"`

	// APIEndpoint is the bound guest cluster's API server URL, mirrored for convenience.
	APIEndpoint string `json:"apiEndpoint,omitempty"`

	// BoundTime is when this lease was bound to an instance.
	BoundTime *metav1.Time `json:"boundTime,omitempty"`

	// Conditions represent the latest available observations of the lease's state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=clease
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=`.status.instanceRef.name`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.status.ocpVersion`
// +kubebuilder:printcolumn:name="Topology",type=string,JSONPath=`.status.topology`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClusterLease is the Schema for the clusterleases API. CI creates a ClusterLease to
// acquire an available guest cluster of a given topology from a ClusterPool. Deleting
// the ClusterLease releases the instance back to the pool for recycling.
type ClusterLease struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterLeaseSpec   `json:"spec,omitempty"`
	Status ClusterLeaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterLeaseList contains a list of ClusterLease.
type ClusterLeaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterLease `json:"items"`
}
