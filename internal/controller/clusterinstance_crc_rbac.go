package controller

import (
	"context"
	"fmt"
	"reflect"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// ensureCRCAgentRBAC keeps permissions for the whole instance lifetime. A
// previous instance with the same name has a different UID and cannot claim them.
func (r *ClusterInstanceReconciler) ensureCRCAgentRBAC(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) error {
	if err := r.ensureCRCAgentClusterRole(ctx); err != nil {
		return err
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentAccountName(instance.Name), Namespace: instance.Namespace, Labels: resources.CommonLabels(instance)}}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentBindingName(instance.Name), Namespace: instance.Namespace, Labels: resources.CommonLabels(instance)},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: resources.CRCAgentClusterRole()},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: sa.Name, Namespace: instance.Namespace}},
	}
	for _, obj := range []client.Object{sa, binding} {
		if err := controllerutil.SetControllerReference(instance, obj, r.Scheme); err != nil {
			return fmt.Errorf("setting CRC agent RBAC owner: %w", err)
		}
	}
	key := client.ObjectKeyFromObject(sa)
	existingSA := &corev1.ServiceAccount{}
	if err := r.platformReader().Get(ctx, key, existingSA); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating CRC agent ServiceAccount %s: %w", key, err)
		}
		if err := r.platformReader().Get(ctx, key, existingSA); err != nil {
			return fmt.Errorf("getting CRC agent ServiceAccount %s after creation: %w", key, err)
		}
	} else if err != nil {
		return fmt.Errorf("getting CRC agent ServiceAccount %s: %w", key, err)
	}
	if !metav1.IsControlledBy(existingSA, instance) {
		return fmt.Errorf("CRC agent ServiceAccount %s is not controlled by ClusterInstance UID %s", key, instance.UID)
	}

	key = client.ObjectKeyFromObject(binding)
	existingBinding := &rbacv1.RoleBinding{}
	if err := r.platformReader().Get(ctx, key, existingBinding); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, binding); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating CRC agent RoleBinding %s: %w", key, err)
		}
		if err := r.platformReader().Get(ctx, key, existingBinding); err != nil {
			return fmt.Errorf("getting CRC agent RoleBinding %s after creation: %w", key, err)
		}
	} else if err != nil {
		return fmt.Errorf("getting CRC agent RoleBinding %s: %w", key, err)
	}
	if !metav1.IsControlledBy(existingBinding, instance) {
		return fmt.Errorf("CRC agent RoleBinding %s is not controlled by ClusterInstance UID %s", key, instance.UID)
	}
	if existingBinding.RoleRef != binding.RoleRef {
		return fmt.Errorf("CRC agent RoleBinding %s has a different immutable roleRef", key)
	}
	if !reflect.DeepEqual(existingBinding.Subjects, binding.Subjects) {
		existingBinding.Subjects = binding.Subjects
		if err := r.Update(ctx, existingBinding); err != nil {
			return fmt.Errorf("repairing CRC agent RoleBinding %s: %w", key, err)
		}
	}
	return nil
}

// OLM does not install arbitrary ClusterRoles from a bundle. The manager
// creates the same fixed role used by direct installs when it is absent.
func (r *ClusterInstanceReconciler) ensureCRCAgentClusterRole(ctx context.Context) error {
	desired := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentClusterRole()}, Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{"kubevirt.io"}, Resources: []string{"virtualmachineinstances"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "create", "update"}},
	}}
	existing := &rbacv1.ClusterRole{}
	key := client.ObjectKeyFromObject(desired)
	if err := r.platformReader().Get(ctx, key, existing); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating CRC agent ClusterRole %s: %w", key, err)
		}
		if err := r.platformReader().Get(ctx, key, existing); err != nil {
			return fmt.Errorf("getting CRC agent ClusterRole %s after creation: %w", key, err)
		}
	} else if err != nil {
		return fmt.Errorf("getting CRC agent ClusterRole %s: %w", key, err)
	}
	if !reflect.DeepEqual(existing.Rules, desired.Rules) {
		return fmt.Errorf("CRC agent ClusterRole %s has unexpected rules", key)
	}
	return nil
}

// deleteCRCAgentRBAC removes only objects controlled by this instance. The
// caller must first wait for all agent Jobs and their Pods to stop.
func (r *ClusterInstanceReconciler) deleteCRCAgentRBAC(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) (bool, error) {
	objects := []client.Object{
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentBindingName(instance.Name), Namespace: instance.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentAccountName(instance.Name), Namespace: instance.Namespace}},
	}
	pending := false
	for _, obj := range objects {
		key := types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}
		if err := r.platformReader().Get(ctx, key, obj); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return false, fmt.Errorf("getting CRC agent RBAC %s: %w", key, err)
		}
		if !metav1.IsControlledBy(obj, instance) {
			continue
		}
		deleting, err := r.deleteIfExists(ctx, obj, "CRC agent RBAC", instanceUIDPrecondition(obj.GetUID()))
		if err != nil {
			return false, err
		}
		pending = pending || deleting
	}
	return pending, nil
}
