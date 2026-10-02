package controller

import (
	"context"
	"fmt"
	"testing"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const policyTestInstanceName = "policy-guest"
const policyTestPoolName = "policy-pool"
const policyTestConfigName = "policy-config"

func enabledTestNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{namespaceEnabledLabel: namespaceEnabledValue}}}
}

func testProvisioningAuthorization() *brokerv1alpha1.ProvisioningAuthorization {
	return &brokerv1alpha1.ProvisioningAuthorization{StartedAt: metav1.Now()}
}

func policyClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	return newHyperShiftFakeClient(t, objects...)
}

func TestNamespaceProvisioningTransitions(t *testing.T) {
	ctx := context.Background()
	for _, topology := range []brokerv1alpha1.ClusterTopology{brokerv1alpha1.TopologyCRC, brokerv1alpha1.TopologyHCP} {
		t.Run(string(topology), func(t *testing.T) {
			ns := enabledTestNamespace(pullSecretTestNamespace)
			instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: policyTestInstanceName, Namespace: ns.Name, UID: crcTestInstanceUID}, Spec: brokerv1alpha1.ClusterInstanceSpec{Type: topology}}
			c := policyClient(t, ns, instance)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			for _, value := range []string{"", "false", "TRUE", "1"} {
				ns.Labels[namespaceEnabledLabel] = value
				if err := c.Update(ctx, ns); err != nil {
					t.Fatal(err)
				}
				result, err := r.gateNamespaceProvisioning(ctx, instance)
				if err != nil || result == nil || instance.Status.Provisioning != nil {
					t.Fatalf("value %q authorized provisioning: %v %v", value, result, err)
				}
				if condition := apimeta.FindStatusCondition(instance.Status.Conditions, conditionTypeProvisioningAllowed); condition == nil || condition.Reason != "NamespaceDisabled" {
					t.Fatalf("missing policy condition: %+v", condition)
				}
			}
			ns.Labels[namespaceEnabledLabel] = namespaceEnabledValue
			if err := c.Update(ctx, ns); err != nil {
				t.Fatal(err)
			}
			if _, err := r.gateNamespaceProvisioning(ctx, instance); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
				t.Fatal(err)
			}
			if instance.Status.Provisioning == nil {
				t.Fatal("authorization not persisted")
			}
			// Opt-out after authorization but before any backing write is durable.
			delete(ns.Labels, namespaceEnabledLabel)
			if err := c.Update(ctx, ns); err != nil {
				t.Fatal(err)
			}
			r = &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			if result, err := r.gateNamespaceProvisioning(ctx, instance); result != nil || err != nil {
				t.Fatalf("authorized work blocked after restart: %v %v", result, err)
			}
			ns.Status.Phase = corev1.NamespaceTerminating
			if err := c.Status().Update(ctx, ns); err != nil {
				t.Fatal(err)
			}
			if result, err := r.gateNamespaceProvisioning(ctx, instance); result == nil || err != nil {
				t.Fatalf("termination did not block allocation: %v %v", result, err)
			}
		})
	}
}

func TestNamespaceAuthorizationRequiresSuccessfulStatusWrite(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("unstarted", pullSecretTestNamespace)
	c := policyClient(t, instance, enabledTestNamespace(instance.Namespace))
	r := &ClusterInstanceReconciler{Client: &failOnceHCPStatusClient{Client: c, fail: true}, Scheme: c.Scheme()}
	if _, err := r.gateNamespaceProvisioning(ctx, instance); err == nil {
		t.Fatal("expected failed authorization write")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.Provisioning != nil {
		t.Fatal("failed decision persisted")
	}
	r.APIReader = unavailableHCPReader{Reader: c}
	if _, err := r.gateNamespaceProvisioning(ctx, instance); err == nil {
		t.Fatal("failed namespace read authorized provisioning")
	}
}

func TestDisabledPoolDoesNotPrepareExpandOrReplace(t *testing.T) {
	ctx := context.Background()
	for _, topology := range []brokerv1alpha1.ClusterTopology{brokerv1alpha1.TopologyCRC, brokerv1alpha1.TopologyHCP} {
		t.Run(string(topology), func(t *testing.T) {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: pullSecretTestNamespace}}
			pool := &brokerv1alpha1.ClusterPool{ObjectMeta: metav1.ObjectMeta{Name: policyTestPoolName, Namespace: ns.Name}, Spec: brokerv1alpha1.ClusterPoolSpec{Type: topology, MinSize: 1, MaxSize: 3, Template: brokerv1alpha1.ClusterTemplate{CRCVersion: "4.16.0"}}}
			c := newStatusWriteFakeClient(t, ns, pool)
			r := &ClusterPoolReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
			req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pool)}
			checkCount := func(want int) {
				t.Helper()
				if _, err := r.Reconcile(ctx, req); err != nil {
					t.Fatal(err)
				}
				list := &brokerv1alpha1.ClusterInstanceList{}
				if err := c.List(ctx, list); err != nil || len(list.Items) != want {
					t.Fatalf("instances = %d, want %d: %v", len(list.Items), want, err)
				}
			}
			checkCount(0)
			bundles := &brokerv1alpha1.CRCBundleList{}
			if err := c.List(ctx, bundles); err != nil || len(bundles.Items) != 0 {
				t.Fatalf("disabled pool prepared bundle: %v", err)
			}
			ns.Labels = map[string]string{namespaceEnabledLabel: namespaceEnabledValue}
			if err := c.Update(ctx, ns); err != nil {
				t.Fatal(err)
			}
			checkCount(1)
			delete(ns.Labels, namespaceEnabledLabel)
			if err := c.Update(ctx, ns); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(ctx, req.NamespacedName, pool); err != nil {
				t.Fatal(err)
			}
			pool.Spec.MinSize = 2
			if err := c.Update(ctx, pool); err != nil {
				t.Fatal(err)
			}
			checkCount(1)
			if err := c.DeleteAllOf(ctx, &brokerv1alpha1.ClusterInstance{}, client.InNamespace(ns.Name)); err != nil {
				t.Fatal(err)
			}
			checkCount(0)
			ns.Labels = map[string]string{namespaceEnabledLabel: namespaceEnabledValue}
			if err := c.Update(ctx, ns); err != nil {
				t.Fatal(err)
			}
			checkCount(1)
		})
	}
}

func TestNamespaceWatchIncludesPreviouslyDisabledObjects(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: pullSecretTestNamespace}}
	pool := &brokerv1alpha1.ClusterPool{ObjectMeta: metav1.ObjectMeta{Name: policyTestPoolName, Namespace: ns.Name}}
	instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: policyTestInstanceName, Namespace: ns.Name}}
	other := instance.DeepCopy()
	other.Namespace = "other"
	c := newStatusWriteFakeClient(t, ns, pool, instance, other)
	for _, pools := range []bool{true, false} {
		requests := namespaceRequests(context.Background(), c, ns, pools)
		if len(requests) != 1 || requests[0].Namespace != ns.Name {
			t.Fatalf("unexpected requests: %v", requests)
		}
	}
}

func TestNamespaceReadFailureIsNotOptIn(t *testing.T) {
	c := policyClient(t)
	if _, err := readNamespacePolicy(context.Background(), unavailableHCPReader{Reader: c}, pullSecretTestNamespace); err == nil {
		t.Fatal("namespace read failure was ignored")
	}
}

func TestOptOutAllowsLeaseBindingButTerminationStopsIt(t *testing.T) {
	for _, terminating := range []bool{false, true} {
		t.Run(fmt.Sprint(terminating), func(t *testing.T) {
			ctx := context.Background()
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: pullSecretTestNamespace}}
			if terminating {
				ns.Status.Phase = corev1.NamespaceTerminating
			}
			instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: policyTestInstanceName, Namespace: ns.Name, Labels: resources.PoolLabels(policyTestPoolName)}, Status: brokerv1alpha1.ClusterInstanceStatus{Phase: brokerv1alpha1.PhaseReady, KubeconfigSecretRef: corev1.LocalObjectReference{Name: policyTestConfigName}}}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: policyTestConfigName, Namespace: ns.Name}, Data: map[string][]byte{resources.KubeconfigSecretKey: []byte("test-config")}}
			lease := &brokerv1alpha1.ClusterLease{ObjectMeta: metav1.ObjectMeta{Name: "lease", Namespace: ns.Name, Finalizers: []string{leaseFinalizer}}, Spec: brokerv1alpha1.ClusterLeaseSpec{PoolRef: corev1.LocalObjectReference{Name: policyTestPoolName}}}
			c := newStatusWriteFakeClient(t, ns, instance, secret, lease)
			r := &ClusterLeaseReconciler{Client: c, Scheme: c.Scheme()}
			req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(lease)}
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(ctx, req.NamespacedName, lease); err != nil {
				t.Fatal(err)
			}
			if (lease.Status.InstanceRef != nil) == terminating {
				t.Fatalf("unexpected binding during terminating=%t: %+v", terminating, lease.Status)
			}
			if !terminating {
				if err := c.Delete(ctx, lease); err != nil {
					t.Fatal(err)
				}
				if _, err := r.Reconcile(ctx, req); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); !apierrors.IsNotFound(err) {
					t.Fatalf("opt-out prevented lease cleanup: %v", err)
				}
			}
		})
	}
}

func TestLeaseDoesNotOverwriteForeignKubeconfig(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{ObjectMeta: metav1.ObjectMeta{Name: policyTestInstanceName, Namespace: pullSecretTestNamespace}, Status: brokerv1alpha1.ClusterInstanceStatus{KubeconfigSecretRef: corev1.LocalObjectReference{Name: policyTestConfigName}}}
	lease := &brokerv1alpha1.ClusterLease{ObjectMeta: metav1.ObjectMeta{Name: "foreign-result", Namespace: instance.Namespace, UID: "lease-uid"}}
	source := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: policyTestConfigName, Namespace: instance.Namespace}, Data: map[string][]byte{resources.KubeconfigSecretKey: []byte("new")}}
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.LeaseKubeconfigSecretName(lease.Name), Namespace: lease.Namespace}, Data: map[string][]byte{resources.KubeconfigSecretKey: []byte("keep")}}
	c := newStatusWriteFakeClient(t, instance, lease, source, foreign)
	r := &ClusterLeaseReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.bind(ctx, lease, instance); err == nil {
		t.Fatal("foreign lease kubeconfig was overwritten")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(foreign), foreign); err != nil {
		t.Fatal(err)
	}
	if string(foreign.Data[resources.KubeconfigSecretKey]) != "keep" {
		t.Fatal("foreign kubeconfig changed")
	}
}
