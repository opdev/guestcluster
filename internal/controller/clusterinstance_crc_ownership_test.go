package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const replacementResourceUID = "replacement-resource"

func crcOwnedObjects(instance *brokerv1alpha1.ClusterInstance) []client.Object {
	return []client.Object{
		resources.BuildCRCVirtualMachine(instance, resources.CRCDiskName(instance)),
		resources.BuildCRCDataVolume(instance),
		resources.BuildCRCAPIService(instance),
		resources.BuildCRCAPIRoute(instance, "api.example.test", resources.CRCAPIServiceName(instance.Name)),
		resources.BuildCRCAgentJob(instance, "192.0.2.1", "vmi", "key", "id_ecdsa", "identity", "agent", "api.example.test", "pull"),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KubeconfigSecretName(instance.Name), Namespace: instance.Namespace, OwnerReferences: resources.InstanceOwnerReferences(instance)}},
	}
}

func TestCRCOwnershipRejectsForeignReuseAndDeletion(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: "same-name", Namespace: pullSecretTestNamespace, UID: "current"}, Spec: brokerv1alpha1.ClusterInstanceSpec{Template: brokerv1alpha1.ClusterTemplate{Memory: testMemory, Cores: 2}}, Status: brokerv1alpha1.ClusterInstanceStatus{Provisioning: testProvisioningAuthorization()}}
	for _, desired := range crcOwnedObjects(instance) {
		for _, ownerUID := range []string{"", "old-instance"} {
			t.Run(desired.GetName()+"/"+ownerUID, func(t *testing.T) {
				foreign := desired.DeepCopyObject().(client.Object)
				foreign.SetUID("foreign-resource")
				if ownerUID == "" {
					foreign.SetOwnerReferences(nil)
				} else {
					refs := foreign.GetOwnerReferences()
					refs[0].UID = types.UID(ownerUID)
					foreign.SetOwnerReferences(refs)
				}
				c := newCRCRecoveryFakeClient(t, instance, foreign)
				if err := c.Get(ctx, client.ObjectKeyFromObject(foreign), foreign); err != nil {
					t.Fatal(err)
				}
				r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
				if _, err := r.ensureCRCObject(ctx, instance, desired.DeepCopyObject().(client.Object)); err == nil {
					t.Fatal("foreign object adopted")
				}
				if _, err := r.deleteCRCObject(ctx, instance, desired.DeepCopyObject().(client.Object), "test"); err == nil {
					t.Fatal("foreign object deleted")
				}
				current := foreign.DeepCopyObject().(client.Object)
				if err := c.Get(ctx, client.ObjectKeyFromObject(foreign), current); err != nil || current.GetResourceVersion() != foreign.GetResourceVersion() {
					t.Fatalf("foreign object changed: %v", err)
				}
			})
		}
	}
}

func TestCRCRecordedParentsProtectDependentCleanup(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: policyTestInstanceName, Namespace: pullSecretTestNamespace, UID: crcTestInstanceUID, Finalizers: []string{instanceFinalizer}, DeletionTimestamp: timePointer(time.Now())}, Status: brokerv1alpha1.ClusterInstanceStatus{CRC: &brokerv1alpha1.CRCBackingStatus{}}}
	vm := &kubevirtv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: resources.VMName(instance.Name), Namespace: instance.Namespace, UID: "vm", OwnerReferences: resources.InstanceOwnerReferences(instance)}}
	dv := &cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{Name: resources.DataVolumeName(instance.Name), Namespace: instance.Namespace, UID: "disk", OwnerReferences: resources.InstanceOwnerReferences(instance)}}
	vmi := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{Name: vm.Name, Namespace: instance.Namespace, UID: "vmi", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(vm, kubevirtv1.SchemeGroupVersion.WithKind("VirtualMachine"))}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: dv.Name, Namespace: instance.Namespace, UID: "pvc", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(dv, cdiv1beta1.SchemeGroupVersion.WithKind("DataVolume"))}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "launcher", Namespace: instance.Namespace, UID: "pod", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(vmi, kubevirtv1.SchemeGroupVersion.WithKind("VirtualMachineInstance"))}}}
	c := newCRCRecoveryFakeClient(t, instance, vm, dv, vmi, pvc, pod)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if err := r.recordCRCIdentity(ctx, instance); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []client.Object{vm, dv, vmi} {
		if err := c.Delete(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []client.Object{pvc, pod} {
		if err := r.verifyCRCResource(ctx, instance, obj); err != nil {
			t.Fatalf("recorded dependent rejected: %v", err)
		}
		foreign := obj.DeepCopyObject().(client.Object)
		refs := foreign.GetOwnerReferences()
		refs[0].UID = "other-parent"
		foreign.SetOwnerReferences(refs)
		if err := r.verifyCRCResource(ctx, instance, foreign); err == nil {
			t.Fatal("foreign dependent accepted")
		}
		if _, err := r.deleteCRCObject(ctx, instance, obj, "dependent"); err != nil {
			t.Fatal(err)
		}
	}
}

type replaceOnDeleteClient struct {
	client.Client
	replaced bool
}

func (c *replaceOnDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if !c.replaced {
		c.replaced = true
		if err := c.Client.Delete(ctx, obj); err != nil {
			return err
		}
		replacement := obj.DeepCopyObject().(client.Object)
		replacement.SetUID(replacementResourceUID)
		replacement.SetResourceVersion("")
		if err := c.Create(ctx, replacement); err != nil {
			return err
		}
		options := &client.DeleteOptions{}
		for _, opt := range opts {
			opt.ApplyToDelete(options)
		}
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != obj.GetUID() {
			return c.Client.Delete(ctx, replacement)
		}
		return apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, obj.GetName(), fmt.Errorf("UID changed"))
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func TestCRCDeletionUsesUIDPrecondition(t *testing.T) {
	instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: policyTestInstanceName, Namespace: pullSecretTestNamespace, UID: crcTestInstanceUID}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: policyTestConfigName, Namespace: instance.Namespace, UID: "original", OwnerReferences: resources.InstanceOwnerReferences(instance)}}
	c := &replaceOnDeleteClient{Client: newCRCRecoveryFakeClient(t, instance, secret)}
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.deleteCRCObject(context.Background(), instance, secret, policyTestConfigName); !apierrors.IsConflict(err) {
		t.Fatalf("expected UID conflict: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(secret), secret); err != nil || secret.UID != replacementResourceUID {
		t.Fatalf("replacement deleted: %v", err)
	}
}

func TestCRCJobWithoutLabelsStillHoldsRBAC(t *testing.T) {
	instance := agentRBACInstance(policyTestInstanceName, pullSecretTestNamespace, crcTestInstanceUID)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: instance.Namespace, OwnerReferences: resources.InstanceOwnerReferences(instance)}}
	c := newCRCRecoveryFakeClient(t, instance, job)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if err := r.ensureCRCAgentRBAC(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	if pending, err := r.teardownCRCBacking(context.Background(), instance); !pending || err != nil {
		t.Fatalf("did not wait for unlabelled Job: %t %v", pending, err)
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: instance.Namespace, Name: resources.CRCAgentAccountName(instance.Name)}, &corev1.ServiceAccount{}); err != nil {
		t.Fatal(err)
	}
}
