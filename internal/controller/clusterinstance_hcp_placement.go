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

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func hcpLocation(instance *brokerv1alpha1.ClusterInstance) (string, string) {
	namespace, name := resources.DefaultHostedClusterNamespace, instance.Name
	if status := instance.Status.HyperShift; status != nil {
		if status.HostedClusterNamespace != "" {
			namespace = status.HostedClusterNamespace
		}
		if status.HostedClusterName != "" {
			name = status.HostedClusterName
		}
	}
	return namespace, name
}

func hcpNodePoolName(instance *brokerv1alpha1.ClusterInstance) string {
	if status := instance.Status.HyperShift; status != nil && len(status.NodePoolNames) > 0 {
		return status.NodePoolNames[0]
	}
	return resources.NodePoolName(instance.Name)
}

// recordHCPPlacement runs before any backing-resource write. Legacy resources
// remain in clusters. New resources use the source namespace.
func (r *ClusterInstanceReconciler) recordHCPPlacement(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (bool, error) {
	if status := instance.Status.HyperShift; status != nil && status.HostedClusterNamespace != "" && status.HostedClusterName != "" {
		return false, nil
	}
	namespace, err := r.recoverHCPNamespace(ctx, instance)
	if err != nil {
		return false, err
	}
	_, name := hcpLocation(instance)
	if err := r.checkHCPPlacement(ctx, instance, namespace, name); err != nil {
		return false, err
	}
	if instance.Status.HyperShift == nil {
		instance.Status.HyperShift = &brokerv1alpha1.HyperShiftBackingStatus{}
	}
	instance.Status.HyperShift.HostedClusterNamespace = namespace
	instance.Status.HyperShift.HostedClusterName = name
	if len(instance.Status.HyperShift.NodePoolNames) == 0 {
		instance.Status.HyperShift.NodePoolNames = []string{resources.NodePoolName(instance.Name)}
	}
	if err := r.Status().Update(ctx, instance); err != nil {
		return false, fmt.Errorf("recording HCP placement: %w", err)
	}
	return true, nil
}

func (r *ClusterInstanceReconciler) recoverHCPNamespace(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (string, error) {
	if status := instance.Status.HyperShift; status != nil && status.HostedClusterNamespace != "" {
		return status.HostedClusterNamespace, nil
	}
	_, name := hcpLocation(instance)
	for _, obj := range []client.Object{
		&hyperv1beta1.HostedCluster{ObjectMeta: metav1.ObjectMeta{Name: name}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KASServingCertName(instance.Name)}},
		&hyperv1beta1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: resources.NodePoolName(instance.Name)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.DefaultPullSecretName(instance.Name)}},
	} {
		key := client.ObjectKey{Namespace: resources.DefaultHostedClusterNamespace, Name: obj.GetName()}
		if err := r.platformReader().Get(ctx, key, obj); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return "", err
		}
		source := obj.GetLabels()[resources.LabelInstanceNamespace]
		if source == "" {
			var err error
			source, err = r.legacyHCPSource(ctx, instance.Name, obj.GetNamespace())
			if err != nil {
				return "", err
			}
		}
		if source != instance.Namespace {
			continue
		}
		if err := r.verifyHCPResource(ctx, instance, obj); err != nil {
			return "", err
		}
		return obj.GetNamespace(), nil
	}
	if instance.Status.APIEndpoint != "" || instance.Status.Phase == brokerv1alpha1.PhaseReady {
		return "", apiEndpointConflict("cannot recover HCP placement for %s/%s from existing resources", instance.Namespace, instance.Name)
	}
	// A remaining legacy control-plane namespace can contain worker compute
	// or storage after its HostedCluster is gone. Do not select a new location
	// or release the instance finalizer while that old location is uncertain.
	legacyNamespace := &corev1.Namespace{}
	key := client.ObjectKey{Name: resources.HostedControlPlaneNamespace(resources.DefaultHostedClusterNamespace, instance.Name)}
	if err := r.platformReader().Get(ctx, key, legacyNamespace); err == nil {
		source, sourceErr := r.legacyHCPSource(ctx, instance.Name, resources.DefaultHostedClusterNamespace)
		if sourceErr != nil || source == instance.Namespace {
			return "", apiEndpointConflict("cannot identify the source of remaining HCP namespace %s", key.Name)
		}
	} else if !apierrors.IsNotFound(err) {
		return "", err
	}
	return instance.Namespace, nil
}

func (r *ClusterInstanceReconciler) checkHCPPlacement(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, namespace, name string) error {
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
			// An old HostedCluster can be gone while its namespace and a
			// managed Secret remain. That Secret can identify the old source
			// for finalizer cleanup; an unclaimed namespace alone cannot.
			verifiedLegacy := false
			if namespace == resources.DefaultHostedClusterNamespace {
				cert := &corev1.Secret{}
				key := client.ObjectKey{Namespace: namespace, Name: resources.KASServingCertName(instance.Name)}
				if err := r.platformReader().Get(ctx, key, cert); err == nil {
					if err := r.verifyHCPResource(ctx, instance, cert); err != nil {
						return err
					}
					verifiedLegacy = true
				} else if !apierrors.IsNotFound(err) {
					return err
				}
			}
			if !verifiedLegacy {
				return apiEndpointConflict("control-plane namespace %q exists without a verified HostedCluster", controlPlaneNamespace)
			}
		} else if !apierrors.IsNotFound(err) {
			return err
		}
	}
	instances := &brokerv1alpha1.ClusterInstanceList{}
	if err := r.platformReader().List(ctx, instances); err != nil {
		return err
	}
	for _, other := range instances.Items {
		if other.Spec.Type != brokerv1alpha1.TopologyHCP || client.ObjectKeyFromObject(&other) == client.ObjectKeyFromObject(instance) || other.Status.HyperShift == nil {
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
	if obj.GetNamespace() == instance.Namespace {
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
	return nil
}

func (r *ClusterInstanceReconciler) verifyExistingHCPResource(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, obj client.Object) error {
	existing := obj.DeepCopyObject().(client.Object)
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), existing); apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}
	return r.verifyHCPResource(ctx, instance, existing)
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
	// A local label alone cannot prove ownership. The one exception is a
	// pre-placement legacy object: its source namespace can itself be "clusters",
	// and those objects did not carry a source-namespace label.
	if obj.GetNamespace() == instance.Namespace &&
		(obj.GetNamespace() != resources.DefaultHostedClusterNamespace || obj.GetLabels()[resources.LabelInstanceNamespace] != "") {
		return apiEndpointConflict("resource %s/%s has no ClusterInstance owner reference", obj.GetNamespace(), obj.GetName())
	}
	labels := obj.GetLabels()
	if labels[resources.LabelManagedBy] != resources.ManagerName || labels[resources.LabelInstance] != instance.Name {
		return apiEndpointConflict("resource %s/%s has no verified ClusterInstance identity", obj.GetNamespace(), obj.GetName())
	}
	if source := labels[resources.LabelInstanceNamespace]; source != "" {
		if source != instance.Namespace {
			return apiEndpointConflict("resource %s/%s belongs to namespace %s", obj.GetNamespace(), obj.GetName(), source)
		}
		return nil
	}
	source, err := r.legacyHCPSource(ctx, instance.Name, obj.GetNamespace())
	if err != nil {
		return err
	}
	if source != instance.Namespace {
		return apiEndpointConflict("legacy resource %s/%s belongs to namespace %s", obj.GetNamespace(), obj.GetName(), source)
	}
	return nil
}

// Prefer a recorded claim over an unstarted same-named instance. If there is
// no recorded claim, only a unique source name can identify legacy resources.
func (r *ClusterInstanceReconciler) legacyHCPSource(ctx context.Context, name, namespace string) (string, error) {
	instances := &brokerv1alpha1.ClusterInstanceList{}
	if err := r.platformReader().List(ctx, instances); err != nil {
		return "", err
	}
	var recorded, unrecorded []string
	for _, other := range instances.Items {
		if other.Spec.Type != brokerv1alpha1.TopologyHCP || other.Name != name {
			continue
		}
		if other.Status.HyperShift == nil || other.Status.HyperShift.HostedClusterNamespace == "" {
			unrecorded = append(unrecorded, other.Namespace)
		} else if other.Status.HyperShift.HostedClusterNamespace == namespace {
			recorded = append(recorded, other.Namespace)
		}
	}
	if len(recorded) == 1 {
		return recorded[0], nil
	}
	if len(recorded) == 0 && len(unrecorded) == 1 {
		return unrecorded[0], nil
	}
	return "", apiEndpointConflict("legacy HCP resources for %s/%s have ambiguous source identity", namespace, name)
}
