package controller

import (
	"context"
	"strings"
	"testing"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const crcAgentRoleKind = "ClusterRole"

func agentRBACInstance(name, namespace, uid string) *brokerv1alpha1.ClusterInstance {
	return &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(uid)}}
}

func TestCRCAgentRBACReconcileAndCleanup(t *testing.T) {
	ctx := context.Background()
	a := agentRBACInstance("first", "tenant", "first-uid")
	b := agentRBACInstance("second", "tenant", "second-uid")
	otherNamespace := agentRBACInstance("first", "other", "other-uid")
	c := newCRCRecoveryFakeClient(t, a, b, otherNamespace)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	for _, instance := range []*brokerv1alpha1.ClusterInstance{a, b, otherNamespace} {
		if err := r.ensureCRCAgentRBAC(ctx, instance); err != nil {
			t.Fatal(err)
		}
		assertCRCAgentBinding(t, ctx, c, instance)
	}
	// A missing account and a changed subject are repaired on the next reconcile.
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentAccountName(a.Name), Namespace: a.Namespace}}
	if err := c.Delete(ctx, sa); err != nil {
		t.Fatal(err)
	}
	binding := &rbacv1.RoleBinding{}
	key := client.ObjectKey{Namespace: a.Namespace, Name: resources.CRCAgentBindingName(a.Name)}
	if err := c.Get(ctx, key, binding); err != nil {
		t.Fatal(err)
	}
	binding.Subjects[0].Name = "wrong"
	if err := c.Update(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureCRCAgentRBAC(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(sa), sa); err != nil || !metav1.IsControlledBy(sa, a) {
		t.Fatalf("account not repaired: %v", err)
	}
	if err := c.Get(ctx, key, binding); err != nil || binding.Subjects[0].Name != sa.Name {
		t.Fatalf("binding not repaired: %v", err)
	}
	if err := c.Delete(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureCRCAgentRBAC(ctx, a); err != nil {
		t.Fatalf("missing binding not repaired: %v", err)
	}
	if err := c.Get(ctx, key, binding); err != nil {
		t.Fatal(err)
	}
	// Retries and VMI replacement keep the same RBAC even though Job names change.
	for _, vmiUID := range []string{"first-vmi", "replacement-vmi"} {
		job := resources.BuildCRCAgentJob(a, "192.0.2.1", vmiUID, "ssh", "key", "identity", "agent", "api", "pull")
		if job.Spec.Template.Spec.ServiceAccountName != sa.Name {
			t.Fatalf("account changed on %s", vmiUID)
		}
		if err := r.ensureCRCAgentRBAC(ctx, a); err != nil {
			t.Fatalf("retry on %s: %v", vmiUID, err)
		}
	}
	// An active Job holds the permissions until its Pods are gone.
	job := resources.BuildCRCAgentJob(a, "192.0.2.1", "active-vmi", "ssh", "key", "identity", "image", "api", "pull")
	if err := c.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	if pending, err := r.teardownCRCBacking(ctx, a); err != nil || !pending {
		t.Fatalf("expected Job cleanup first: %t %v", pending, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(sa), sa); err != nil {
		t.Fatalf("account removed before Job stopped: %v", err)
	}
	// The next pass deletes the owned RBAC (the fake client deletes immediately).
	if _, err := r.teardownCRCBacking(ctx, a); err != nil {
		t.Fatal(err)
	}
	if pending, err := r.teardownCRCBacking(ctx, a); err != nil || pending {
		t.Fatalf("cleanup: %t %v", pending, err)
	}
	for _, obj := range []client.Object{sa, binding} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err == nil {
			t.Fatalf("still present: %T", obj)
		}
	}
	for _, instance := range []*brokerv1alpha1.ClusterInstance{b, otherNamespace} {
		if err := r.ensureCRCAgentRBAC(ctx, instance); err != nil {
			t.Fatalf("other instance lost RBAC: %v", err)
		}
	}
}

func assertCRCAgentBinding(t *testing.T, ctx context.Context, c client.Client, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	binding := &rbacv1.RoleBinding{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: resources.CRCAgentBindingName(instance.Name)}, binding); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(binding, instance) || binding.Subjects[0].Namespace != instance.Namespace ||
		binding.Subjects[0].Name != resources.CRCAgentAccountName(instance.Name) ||
		binding.RoleRef.Kind != crcAgentRoleKind || binding.RoleRef.Name != resources.CRCAgentClusterRole() {
		t.Fatalf("wrong binding: %+v", binding)
	}
}

// A new Job can be present in the API before it appears in the manager cache.
// An empty cached list must not allow RBAC cleanup ahead of Job deletion.
type emptyCachedAgentJobList struct{ client.Client }

func (c emptyCachedAgentJobList) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*batchv1.JobList); ok {
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

func TestTeardownCRCUsesLiveJobListBeforeRemovingRBAC(t *testing.T) {
	ctx := context.Background()
	instance := agentRBACInstance("active", "tenant", "uid")
	c := newCRCRecoveryFakeClient(t, instance)
	r := &ClusterInstanceReconciler{Client: emptyCachedAgentJobList{c}, APIReader: c, Scheme: c.Scheme()}
	if err := r.ensureCRCAgentRBAC(ctx, instance); err != nil {
		t.Fatal(err)
	}
	job := resources.BuildCRCAgentJob(instance, "192.0.2.1", "vmi", "ssh", "key", "identity", "agent", "api", "pull")
	if err := c.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	if pending, err := r.teardownCRCBacking(ctx, instance); err != nil || !pending {
		t.Fatalf("expected live Job cleanup: pending=%t err=%v", pending, err)
	}
	for _, obj := range []client.Object{
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentAccountName(instance.Name), Namespace: instance.Namespace}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentBindingName(instance.Name), Namespace: instance.Namespace}},
	} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Fatalf("removed %T before Job cleanup: %v", obj, err)
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), &batchv1.Job{}); err == nil {
		t.Fatal("expected Job deletion to be requested")
	}
}

func TestCRCAgentRBACRejectsRuleAndRoleRefDrift(t *testing.T) {
	ctx := context.Background()
	instance := agentRBACInstance("drift", "tenant", "uid")
	c := newCRCRecoveryFakeClient(t, instance)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if err := r.ensureCRCAgentRBAC(ctx, instance); err != nil {
		t.Fatal(err)
	}
	binding := &rbacv1.RoleBinding{}
	key := client.ObjectKey{Namespace: instance.Namespace, Name: resources.CRCAgentBindingName(instance.Name)}
	if err := c.Get(ctx, key, binding); err != nil {
		t.Fatal(err)
	}
	binding.RoleRef.Name = "wrong"
	if err := c.Update(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureCRCAgentRBAC(ctx, instance); err == nil || !strings.Contains(err.Error(), "roleRef") {
		t.Fatalf("expected roleRef conflict: %v", err)
	}
	role := &rbacv1.ClusterRole{}
	if err := c.Get(ctx, client.ObjectKey{Name: resources.CRCAgentClusterRole()}, role); err != nil {
		t.Fatal(err)
	}
	role.Rules[0].Verbs = append(role.Rules[0].Verbs, "delete")
	if err := c.Update(ctx, role); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureCRCAgentRBAC(ctx, instance); err == nil || !strings.Contains(err.Error(), "unexpected rules") {
		t.Fatalf("expected rule conflict: %v", err)
	}
}

func TestCRCAgentRBACRejectsOtherOwners(t *testing.T) {
	ctx := context.Background()
	instance := agentRBACInstance("name", "tenant", "new-uid")
	old := agentRBACInstance("name", "tenant", "old-uid")
	c := newCRCRecoveryFakeClient(t, instance)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if err := r.ensureCRCAgentRBAC(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureCRCAgentRBAC(ctx, instance); err == nil || !strings.Contains(err.Error(), "UID") {
		t.Fatalf("expected owner UID conflict: %v", err)
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentAccountName(instance.Name), Namespace: instance.Namespace}}
	if err := c.Delete(ctx, sa); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureCRCAgentRBAC(ctx, instance); err == nil || !strings.Contains(err.Error(), "UID") {
		t.Fatalf("expected binding UID conflict: %v", err)
	}
	// Cleanup of a new object must not remove the old owner's resources.
	if _, err := r.deleteCRCAgentRBAC(ctx, instance); err != nil {
		t.Fatal(err)
	}
	binding := &rbacv1.RoleBinding{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: resources.CRCAgentBindingName(instance.Name)}, binding); err != nil {
		t.Fatal(err)
	}
}
