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

// Package v1alpha1 contains API Schema definitions for the guestcluster v1alpha1 API group.
// +kubebuilder:object:generate=true
// +groupName=guestcluster.opdev.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "guestcluster.opdev.io", Version: "v1alpha1"}

	// SchemeBuilder adds Go types to the GroupVersionKind scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes registers every type in this API group-version with the
// given scheme. This replaces the deprecated sigs.k8s.io/controller-runtime
// pkg/scheme.Builder helper (see its Register method's doc comment), which
// recommends registering types directly against the apimachinery
// runtime.SchemeBuilder instead.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&ClusterInstance{}, &ClusterInstanceList{},
		&ClusterLease{}, &ClusterLeaseList{},
		&ClusterPool{}, &ClusterPoolList{},
		&CRCBundle{}, &CRCBundleList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
