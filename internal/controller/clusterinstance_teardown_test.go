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
	"strings"
	"testing"
	"time"

	routev1 "github.com/openshift/api/route/v1"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
)

const teardownTestNamespace = "teardown-test"

func TestReconcileDeleteCRCWaitsForVMIAndLauncherPod(t *testing.T) {
	ctx := context.Background()
	instance := deletingCRCInstance("crc-vmi-teardown", time.Now())
	vm := crcTeardownVM(instance, kubevirtv1.RunStrategyAlways)
	vmi := &kubevirtv1.VirtualMachineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: resources.VMName(instance.Name), Namespace: instance.Namespace},
		Status:     kubevirtv1.VirtualMachineInstanceStatus{Phase: kubevirtv1.Running},
	}
	launcherPod := crcLauncherPod(instance.Name, nil)
	dv := &cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.DataVolumeName(instance.Name),
		Namespace: instance.Namespace,
	}}
	c := newTeardownFakeClient(t, instance, vm, vmi, launcherPod, dv)
	c.holdVMIDeletes = true
	c.holdPodDeletes = true
	r := &ClusterInstanceReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
	req := client.ObjectKeyFromObject(instance)

	result, err := r.Reconcile(ctx, ctrlRequest(req))
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("first RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	gotVM := &kubevirtv1.VirtualMachine{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(vm), gotVM); err != nil {
		t.Fatalf("getting CRC VM: %v", err)
	}
	if gotVM.Spec.RunStrategy == nil || *gotVM.Spec.RunStrategy != kubevirtv1.RunStrategyHalted {
		t.Fatalf("VM runStrategy = %v, want Halted", gotVM.Spec.RunStrategy)
	}
	if c.vmiDeleteRequests != 0 {
		t.Fatal("VMI deletion was requested before KubeVirt acknowledged the Halted strategy")
	}
	assertObjectExists(t, ctx, c, vmi)
	assertObjectExists(t, ctx, c, dv)
	assertInstanceFinalizer(t, ctx, c, instance)

	// KubeVirt reports the run strategy that it has processed in VM status.
	gotVM.Status.RunStrategy = kubevirtv1.RunStrategyHalted
	if err := c.Update(ctx, gotVM); err != nil {
		t.Fatalf("recording KubeVirt halt acknowledgement: %v", err)
	}

	result, err = r.Reconcile(ctx, ctrlRequest(req))
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("second RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	assertObjectMissing(t, ctx, c, vm)
	if c.vmiDeleteRequests == 0 {
		t.Fatal("VMI delete was not requested")
	}
	assertObjectExists(t, ctx, c, vmi)
	assertObjectExists(t, ctx, c, dv)
	assertInstanceFinalizer(t, ctx, c, instance)

	// Keep the VMI present for one reconcile to model KubeVirt cleanup taking
	// more than one pass. Then let the backing API delete it.
	c.holdVMIDeletes = false
	if err := c.Client.Delete(ctx, vmi); err != nil {
		t.Fatalf("removing VMI: %v", err)
	}
	result, err = r.Reconcile(ctx, ctrlRequest(req))
	if err != nil {
		t.Fatalf("third Reconcile: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("third RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	if c.podDeleteRequests == 0 {
		t.Fatal("launcher pod deletion was not requested after the VMI disappeared")
	}
	assertObjectExists(t, ctx, c, launcherPod)
	assertObjectExists(t, ctx, c, dv)
	assertInstanceFinalizer(t, ctx, c, instance)

	c.holdPodDeletes = false
	if err := c.Client.Delete(ctx, launcherPod); err != nil {
		t.Fatalf("removing launcher pod: %v", err)
	}
	if _, err := r.Reconcile(ctx, ctrlRequest(req)); err != nil {
		t.Fatalf("final Reconcile: %v", err)
	}
	assertObjectMissing(t, ctx, c, vm)
	assertObjectMissing(t, ctx, c, dv)
	assertInstanceMissing(t, ctx, c, instance)
}

func TestReconcileDeleteWaitsForAlreadyTerminatingVMI(t *testing.T) {
	ctx := context.Background()
	instance := deletingCRCInstance("crc-terminating-vmi", time.Now())
	vmiDeletionTime := metav1.NewTime(time.Now().Add(-time.Minute))
	vm := crcTeardownVM(instance, kubevirtv1.RunStrategyHalted)
	vmi := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{
		Name:              resources.VMName(instance.Name),
		Namespace:         instance.Namespace,
		DeletionTimestamp: &vmiDeletionTime,
		Finalizers:        []string{"test.example.io/vmi-cleanup"},
	}}
	dv := &cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.DataVolumeName(instance.Name),
		Namespace: instance.Namespace,
	}}
	c := newTeardownFakeClient(t, instance, vm, vmi, dv)
	c.holdVMIDeletes = true
	r := &ClusterInstanceReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}

	result, err := r.Reconcile(ctx, ctrlRequest(client.ObjectKeyFromObject(instance)))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	assertObjectExists(t, ctx, c, vmi)
	assertObjectExists(t, ctx, c, dv)
	assertInstanceFinalizer(t, ctx, c, instance)
}

func TestReconcileDeleteReportsBlockedVMICleanupTimeout(t *testing.T) {
	ctx := context.Background()
	instance := deletingCRCInstance("crc-vmi-timeout", time.Now().Add(-cleanupTimeout-time.Minute))
	vm := crcTeardownVM(instance, kubevirtv1.RunStrategyHalted)
	vmi := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{
		Name:      resources.VMName(instance.Name),
		Namespace: instance.Namespace,
	}}
	c := newTeardownFakeClient(t, instance, vm, vmi)
	c.holdVMIDeletes = true
	r := &ClusterInstanceReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}

	result, err := r.Reconcile(ctx, ctrlRequest(client.ObjectKeyFromObject(instance)))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	gotInstance := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), gotInstance); err != nil {
		t.Fatalf("getting ClusterInstance: %v", err)
	}
	if !controllerutil.ContainsFinalizer(gotInstance, instanceFinalizer) {
		t.Fatal("ClusterInstance finalizer was removed while VMI cleanup was blocked")
	}
	condition := apimeta.FindStatusCondition(gotInstance.Status.Conditions, conditionTypeTerminating)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "CleanupTimedOut" {
		t.Fatalf("Terminating condition = %+v, want CleanupTimedOut", condition)
	}
	if !strings.Contains(condition.Message, "finalizer remains") {
		t.Fatalf("timeout message = %q, want it to explain that the finalizer remains", condition.Message)
	}
	assertObjectExists(t, ctx, c, vmi)
}

func TestReconcileDeleteHCPWaitsForWorkerVMIsAndLauncherPods(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "hcp-worker-cleanup",
			Namespace:         teardownTestNamespace,
			Finalizers:        []string{instanceFinalizer},
			DeletionTimestamp: timePointer(time.Now()),
		},
		Spec: brokerv1alpha1.ClusterInstanceSpec{Type: brokerv1alpha1.TopologyHCP},
	}
	const resourceFinalizer = "test.example.io/cleanup"
	hc := &hyperv1beta1.HostedCluster{ObjectMeta: metav1.ObjectMeta{
		Name:       resources.HostedClusterName(instance.Name),
		Namespace:  resources.DefaultHostedClusterNamespace,
		Finalizers: []string{resourceFinalizer},
	}}
	np := &hyperv1beta1.NodePool{ObjectMeta: metav1.ObjectMeta{
		Name:       resources.NodePoolName(instance.Name),
		Namespace:  resources.DefaultHostedClusterNamespace,
		Finalizers: []string{resourceFinalizer},
	}}
	hcpNamespace := resources.HostedControlPlaneNamespace(resources.DefaultHostedClusterNamespace, instance.Name)
	workerVMIName := "hcp-worker-vmi"
	workerVMI := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{
		Name:      workerVMIName,
		Namespace: hcpNamespace,
		Labels:    map[string]string{hyperv1beta1.NodePoolNameLabel: np.Name},
	}}
	otherPoolVMI := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{
		Name:      "other-worker-0",
		Namespace: hcpNamespace,
		Labels:    map[string]string{hyperv1beta1.NodePoolNameLabel: "another-pool"},
	}}
	workerPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      workerVMIName + "-launcher",
		Namespace: hcpNamespace,
		Labels:    map[string]string{hyperv1beta1.NodePoolNameLabel: np.Name},
	}}
	otherPoolPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "other-worker-launcher",
		Namespace: hcpNamespace,
		Labels:    map[string]string{hyperv1beta1.NodePoolNameLabel: "another-pool"},
	}}
	c := newTeardownFakeClient(t, instance, hc, np, workerVMI, otherPoolVMI, workerPod, otherPoolPod)
	c.holdPodDeletes = true
	r := &ClusterInstanceReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
	req := client.ObjectKeyFromObject(instance)

	result, err := r.Reconcile(ctx, ctrlRequest(req))
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("first RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	assertObjectExists(t, ctx, c, workerVMI)
	assertObjectExists(t, ctx, c, workerPod)
	assertInstanceFinalizer(t, ctx, c, instance)
	gotOtherVMI := &kubevirtv1.VirtualMachineInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(otherPoolVMI), gotOtherVMI); err != nil {
		t.Fatalf("getting other NodePool VMI: %v", err)
	}
	if !gotOtherVMI.DeletionTimestamp.IsZero() {
		t.Fatal("VMI from another NodePool was changed")
	}

	// HyperShift finishes deleting its resources and VMI. The remaining pod
	// must still block ClusterInstance finalizer removal.
	for _, obj := range []client.Object{hc, np} {
		stored := obj.DeepCopyObject().(client.Object)
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), stored); err != nil {
			t.Fatalf("getting %T: %v", obj, err)
		}
		stored.SetFinalizers(nil)
		if err := c.Update(ctx, stored); err != nil {
			t.Fatalf("removing %T finalizer: %v", obj, err)
		}
	}
	if err := c.Delete(ctx, workerVMI); err != nil {
		t.Fatalf("simulating HyperShift VMI deletion: %v", err)
	}

	result, err = r.Reconcile(ctx, ctrlRequest(req))
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("second RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	if c.podDeleteRequests == 0 {
		t.Fatal("orphaned worker launcher pod deletion was not requested")
	}
	assertObjectExists(t, ctx, c, workerPod)
	assertObjectExists(t, ctx, c, otherPoolPod)
	assertInstanceFinalizer(t, ctx, c, instance)

	c.holdPodDeletes = false
	if err := c.Client.Delete(ctx, workerPod); err != nil {
		t.Fatalf("removing worker launcher pod: %v", err)
	}
	if _, err := r.Reconcile(ctx, ctrlRequest(req)); err != nil {
		t.Fatalf("final Reconcile: %v", err)
	}
	assertInstanceMissing(t, ctx, c, instance)
}

func TestReconcileDeleteWaitsForLauncherPodMissingFromCache(t *testing.T) {
	for _, topology := range []brokerv1alpha1.ClusterTopology{brokerv1alpha1.TopologyCRC, brokerv1alpha1.TopologyHCP} {
		t.Run(string(topology), func(t *testing.T) {
			ctx := context.Background()
			instance := deletingCRCInstance("uncached-launcher", time.Now())
			instance.Spec.Type = topology
			pod := crcLauncherPod(instance.Name, nil)
			objects := []client.Object{instance, pod}
			var dv *cdiv1beta1.DataVolume
			if topology == brokerv1alpha1.TopologyHCP {
				pod.Namespace = resources.HostedControlPlaneNamespace(resources.DefaultHostedClusterNamespace, instance.Name)
				pod.Labels = map[string]string{hyperv1beta1.NodePoolNameLabel: resources.NodePoolName(instance.Name)}
			} else {
				dv = &cdiv1beta1.DataVolume{ObjectMeta: metav1.ObjectMeta{
					Name: resources.DataVolumeName(instance.Name), Namespace: instance.Namespace,
				}}
				objects = append(objects, dv)
			}
			c := newTeardownFakeClient(t, objects...)
			c.hidePodsFromList = true
			c.holdPodDeletes = true
			// The API has a surviving launcher pod, but the cache has not seen it.
			// All parent resources and VMIs have already disappeared.
			r := &ClusterInstanceReconciler{Client: c, APIReader: c.Client, Scheme: c.Scheme()}
			req := ctrlRequest(client.ObjectKeyFromObject(instance))
			for range 2 {
				result, err := r.Reconcile(ctx, req)
				if err != nil {
					t.Fatalf("Reconcile: %v", err)
				}
				if result.RequeueAfter != requeueInterval {
					t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
				}
				assertInstanceFinalizer(t, ctx, c, instance)
				assertObjectExists(t, ctx, c, pod)
				if dv != nil {
					assertObjectExists(t, ctx, c, dv)
				}
			}
			if c.podDeleteRequests == 0 {
				t.Fatal("launcher pod deletion was not requested")
			}
			if err := c.Client.Delete(ctx, pod); err != nil {
				t.Fatalf("removing launcher pod: %v", err)
			}
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatalf("final Reconcile: %v", err)
			}
			assertInstanceMissing(t, ctx, c, instance)
			if dv != nil {
				assertObjectMissing(t, ctx, c, dv)
			}
		})
	}
}

func deletingCRCInstance(name string, deletionTime time.Time) *brokerv1alpha1.ClusterInstance {
	return &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         teardownTestNamespace,
			Finalizers:        []string{instanceFinalizer},
			DeletionTimestamp: timePointer(deletionTime),
		},
		Spec: brokerv1alpha1.ClusterInstanceSpec{Type: brokerv1alpha1.TopologyCRC},
	}
}

func crcTeardownVM(instance *brokerv1alpha1.ClusterInstance, runStrategy kubevirtv1.VirtualMachineRunStrategy) *kubevirtv1.VirtualMachine {
	return &kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: resources.VMName(instance.Name), Namespace: instance.Namespace},
		Spec:       kubevirtv1.VirtualMachineSpec{RunStrategy: &runStrategy},
		Status:     kubevirtv1.VirtualMachineStatus{RunStrategy: runStrategy},
	}
}

func crcLauncherPod(instanceName string, finalizers []string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:       instanceName + "-launcher",
		Namespace:  teardownTestNamespace,
		Labels:     map[string]string{kubevirtv1.DeprecatedVirtualMachineNameLabel: resources.VMName(instanceName)},
		Finalizers: finalizers,
	}}
}

func timePointer(value time.Time) *metav1.Time {
	result := metav1.NewTime(value)
	return &result
}

func ctrlRequest(key client.ObjectKey) reconcile.Request {
	return reconcile.Request{NamespacedName: key}
}

type teardownTestClient struct {
	client.Client
	holdVMIDeletes    bool
	holdPodDeletes    bool
	hidePodsFromList  bool
	vmiDeleteRequests int
	podDeleteRequests int
}

func (c *teardownTestClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if pods, ok := list.(*corev1.PodList); ok && c.hidePodsFromList {
		pods.Items = nil
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

func (c *teardownTestClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	switch obj.(type) {
	case *kubevirtv1.VirtualMachineInstance:
		c.vmiDeleteRequests++
		if c.holdVMIDeletes {
			return nil
		}
	case *corev1.Pod:
		c.podDeleteRequests++
		if c.holdPodDeletes {
			return nil
		}
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func newTeardownFakeClient(t *testing.T, objects ...client.Object) *teardownTestClient {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		scheme.AddToScheme,
		brokerv1alpha1.AddToScheme,
		kubevirtv1.AddToScheme,
		cdiv1beta1.AddToScheme,
		routev1.AddToScheme,
		hyperv1beta1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("adding scheme: %v", err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&brokerv1alpha1.ClusterInstance{}).
		WithObjects(objects...).Build()
	return &teardownTestClient{Client: c}
}

func assertInstanceFinalizer(t *testing.T, ctx context.Context, c client.Client, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	got := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), got); err != nil {
		t.Fatalf("getting ClusterInstance: %v", err)
	}
	if !controllerutil.ContainsFinalizer(got, instanceFinalizer) {
		t.Fatal("ClusterInstance finalizer was removed while backing resources remained")
	}
}

func assertInstanceMissing(t *testing.T, ctx context.Context, c client.Client, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	got := &brokerv1alpha1.ClusterInstance{}
	err := c.Get(ctx, client.ObjectKeyFromObject(instance), got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("ClusterInstance get error = %v, want NotFound", err)
	}
}

func assertObjectExists(t *testing.T, ctx context.Context, c client.Client, obj client.Object) {
	t.Helper()
	err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if err != nil {
		t.Fatalf("getting %T %s: %v", obj, client.ObjectKeyFromObject(obj), err)
	}
}

func assertObjectMissing(t *testing.T, ctx context.Context, c client.Client, obj client.Object) {
	t.Helper()
	err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("%T %s get error = %v, want NotFound", obj, client.ObjectKeyFromObject(obj), err)
	}
}
