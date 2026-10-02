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

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func hcpLocation(instance *brokerv1alpha1.ClusterInstance) (string, string) {
	return instance.Namespace, resources.HostedClusterName(instance.Name, instance.Namespace)
}

func hcpNodePoolName(instance *brokerv1alpha1.ClusterInstance) string {
	return resources.NodePoolName(instance.Name)
}

func (r *ClusterInstanceReconciler) checkHCPPlacement(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) error {
	namespace, name := hcpLocation(instance)
	controlPlaneNamespace := resources.HostedControlPlaneNamespace(namespace, name)
	if problems := validation.IsDNS1123Label(controlPlaneNamespace); len(problems) != 0 {
		return apiEndpointConflict("unsupported HCP namespace %q: %v", controlPlaneNamespace, problems)
	}
	if problems := validation.IsDNS1123Label(hcpNodePoolName(instance)); len(problems) != 0 {
		return apiEndpointConflict("unsupported HCP NodePool name: %v", problems)
	}
	// HyperShift concatenates names and replaces dots. Reject collisions before
	// it can create workloads in another HostedCluster's control-plane namespace.
	clusters := &hyperv1beta1.HostedClusterList{}
	if err := r.platformReader().List(ctx, clusters); err != nil {
		return err
	}
	ownedClusterExists := false
	for _, hc := range clusters.Items {
		if hc.Namespace == namespace && hc.Name == name {
			if err := r.verifyHCPResource(ctx, instance, &hc); err != nil {
				return err
			}
			ownedClusterExists = true
			continue
		}
		if resources.HostedControlPlaneNamespace(hc.Namespace, hc.Name) == controlPlaneNamespace {
			return apiEndpointConflict("control-plane namespace %q is used by HostedCluster %s/%s", controlPlaneNamespace, hc.Namespace, hc.Name)
		}
	}
	if !ownedClusterExists {
		existing := &corev1.Namespace{}
		if err := r.platformReader().Get(ctx, client.ObjectKey{Name: controlPlaneNamespace}, existing); err == nil {
			return apiEndpointConflict("control-plane namespace %q exists without an owned HostedCluster", controlPlaneNamespace)
		} else if !apierrors.IsNotFound(err) {
			return err
		}
	}
	instances := &brokerv1alpha1.ClusterInstanceList{}
	if err := r.platformReader().List(ctx, instances); err != nil {
		return err
	}
	for _, other := range instances.Items {
		if other.Spec.Type != brokerv1alpha1.TopologyHCP || client.ObjectKeyFromObject(&other) == client.ObjectKeyFromObject(instance) {
			continue
		}
		otherNamespace, otherName := hcpLocation(&other)
		if resources.HostedControlPlaneNamespace(otherNamespace, otherName) == controlPlaneNamespace {
			return apiEndpointConflict("control-plane namespace %q is recorded by ClusterInstance %s/%s", controlPlaneNamespace, other.Namespace, other.Name)
		}
	}
	return nil
}

func (r *ClusterInstanceReconciler) setHCPResourceOwner(instance *brokerv1alpha1.ClusterInstance, obj client.Object) error {
	obj.SetLabels(resources.APIEndpointLabels(instance))
	if obj.GetNamespace() != instance.Namespace {
		return apiEndpointConflict("HCP resource %s/%s is outside the instance namespace", obj.GetNamespace(), obj.GetName())
	}
	if _, nodePool := obj.(*hyperv1beta1.NodePool); nodePool {
		// HyperShift owns the NodePool controller reference. Keep it, and the
		// instance reference, non-blocking. The manager can create HostedClusters
		// but does not need update permission on HostedCluster finalizers just
		// to create the dependent NodePool.
		if err := controllerutil.SetOwnerReference(instance, obj, r.Scheme); err != nil {
			return err
		}
		_, hostedClusterName := hcpLocation(instance)
		owners := obj.GetOwnerReferences()
		for i := range owners {
			owner := owners[i]
			if (owner.Kind == clusterInstanceKind && owner.Name == instance.Name && owner.UID == instance.UID) ||
				(owner.Kind == "HostedCluster" && owner.Name == hostedClusterName) {
				block := false
				owners[i].BlockOwnerDeletion = &block
			}
		}
		obj.SetOwnerReferences(owners)
		return nil
	}
	return controllerutil.SetControllerReference(instance, obj, r.Scheme)
}

func (r *ClusterInstanceReconciler) verifyHCPResource(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, obj client.Object) error {
	for _, owner := range obj.GetOwnerReferences() {
		if owner.Kind != clusterInstanceKind {
			continue
		}
		if obj.GetNamespace() == instance.Namespace && owner.Name == instance.Name && owner.UID == instance.UID {
			if np, ok := obj.(*hyperv1beta1.NodePool); ok {
				_, name := hcpLocation(instance)
				var hostedClusterOwner *metav1.OwnerReference
				for i := range np.OwnerReferences {
					ref := &np.OwnerReferences[i]
					if ref.Kind == "HostedCluster" && ref.Name == name && ref.UID != "" {
						hostedClusterOwner = ref
						break
					}
				}
				if np.Spec.ClusterName != name || hostedClusterOwner == nil {
					return apiEndpointConflict("NodePool %s/%s does not belong to HostedCluster %s", np.Namespace, np.Name, name)
				}
				hcpNamespace, _ := hcpLocation(instance)
				hostedCluster := &hyperv1beta1.HostedCluster{}
				if err := r.platformReader().Get(ctx, client.ObjectKey{Namespace: hcpNamespace, Name: name}, hostedCluster); err == nil {
					if hostedClusterOwner.UID != hostedCluster.UID {
						return apiEndpointConflict("NodePool %s/%s has a different HostedCluster owner UID", np.Namespace, np.Name)
					}
				} else if !apierrors.IsNotFound(err) {
					return err
				}
			}
			return nil
		}
		return apiEndpointConflict("resource %s/%s belongs to another ClusterInstance", obj.GetNamespace(), obj.GetName())
	}
	if obj.GetNamespace() == instance.Namespace {
		return apiEndpointConflict("resource %s/%s has no ClusterInstance owner reference", obj.GetNamespace(), obj.GetName())
	}
	labels := obj.GetLabels()
	if labels[resources.LabelManagedBy] != resources.ManagerName || labels[resources.LabelInstance] != instance.Name || labels[resources.LabelInstanceNamespace] != instance.Namespace {
		return apiEndpointConflict("resource %s/%s has no verified ClusterInstance identity", obj.GetNamespace(), obj.GetName())
	}
	namespace, name := hcpLocation(instance)
	expectedNamespace := resources.HostedControlPlaneNamespace(namespace, name)
	if obj.GetNamespace() != expectedNamespace {
		return apiEndpointConflict("resource %s/%s is outside the expected control-plane namespace %s", obj.GetNamespace(), obj.GetName(), expectedNamespace)
	}
	return nil
}
