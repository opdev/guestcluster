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

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
)

const (
	testVMINamespace = "test"
	testVMIName      = "crc"
	testVMIUID       = "vmi-uid"
)

func TestMonitorCRCVMILifecycleCancelsWhenVMIIsDeleted(t *testing.T) {
	client := newVMIDynamicClient(t, testVMIUID)
	cfg := config{Namespace: testVMINamespace, InstanceName: testVMIName, ExpectedVMIUID: testVMIUID}
	ctx, stop, err := monitorCRCVMILifecycle(context.Background(), client, cfg, discardLog{})
	if err != nil {
		t.Fatalf("monitorCRCVMILifecycle: %v", err)
	}
	defer stop()

	vmis := client.Resource(crcVMIGVR).Namespace(cfg.Namespace)
	if err := vmis.Delete(context.Background(), cfg.InstanceName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting VMI: %v", err)
	}

	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), errCRCVMINoLongerCurrent) {
			t.Fatalf("context cause = %v, want VMI lifecycle error", context.Cause(ctx))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("VMI deletion did not cancel the agent context")
	}
}

func TestEnsureExpectedCRCVMIRunningRejectsReplacement(t *testing.T) {
	client := newVMIDynamicClient(t, "new-vmi-uid")
	cfg := config{Namespace: testVMINamespace, InstanceName: testVMIName, ExpectedVMIUID: "old-vmi-uid"}
	err := ensureExpectedCRCVMIRunning(context.Background(), client.Resource(crcVMIGVR).Namespace(cfg.Namespace), cfg)
	if !errors.Is(err, errCRCVMINoLongerCurrent) {
		t.Fatalf("ensureExpectedCRCVMIRunning error = %v, want VMI lifecycle error", err)
	}
}

func TestMonitorCRCVMILifecycleCancelsWhenVMIStopsRunning(t *testing.T) {
	client := newVMIDynamicClient(t, testVMIUID)
	cfg := config{Namespace: testVMINamespace, InstanceName: testVMIName, ExpectedVMIUID: testVMIUID}
	ctx, stop, err := monitorCRCVMILifecycle(context.Background(), client, cfg, discardLog{})
	if err != nil {
		t.Fatalf("monitorCRCVMILifecycle: %v", err)
	}
	defer stop()

	vmis := client.Resource(crcVMIGVR).Namespace(cfg.Namespace)
	vmi, err := vmis.Get(context.Background(), cfg.InstanceName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting VMI: %v", err)
	}
	if err := unstructured.SetNestedField(vmi.Object, "Failed", "status", "phase"); err != nil {
		t.Fatalf("setting VMI phase: %v", err)
	}
	if _, err := vmis.Update(context.Background(), vmi, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating VMI: %v", err)
	}

	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), errCRCVMINoLongerCurrent) {
			t.Fatalf("context cause = %v, want VMI lifecycle error", context.Cause(ctx))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("non-running VMI did not cancel the agent context")
	}
}

func TestEnsureExpectedCRCVMIRunningRejectsNonRunningVMI(t *testing.T) {
	client := newVMIDynamicClient(t, testVMIUID)
	cfg := config{Namespace: testVMINamespace, InstanceName: testVMIName, ExpectedVMIUID: testVMIUID}
	vmis := client.Resource(crcVMIGVR).Namespace(cfg.Namespace)
	vmi, err := vmis.Get(context.Background(), cfg.InstanceName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting VMI: %v", err)
	}
	if err := unstructured.SetNestedField(vmi.Object, "Pending", "status", "phase"); err != nil {
		t.Fatalf("setting VMI phase: %v", err)
	}
	if _, err := vmis.Update(context.Background(), vmi, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating VMI: %v", err)
	}

	err = ensureExpectedCRCVMIRunning(context.Background(), vmis, cfg)
	if !errors.Is(err, errCRCVMINoLongerCurrent) {
		t.Fatalf("ensureExpectedCRCVMIRunning error = %v, want VMI lifecycle error", err)
	}
}

func TestWaitForCRCVMITerminationIgnoresBookmarkWithoutUID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := watch.NewRaceFreeFake()
	cfg := config{Namespace: testVMINamespace, InstanceName: testVMIName, ExpectedVMIUID: testVMIUID}
	result := make(chan error, 1)
	go func() { result <- waitForCRCVMITermination(ctx, w, cfg) }()

	w.Action(watch.Bookmark, &unstructured.Unstructured{Object: map[string]interface{}{
		"metadata": map[string]interface{}{"resourceVersion": "2"},
	}})

	select {
	case err := <-result:
		t.Fatalf("watch ended after bookmark: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("watch error after cancellation: %v", err)
	}
}

func newVMIDynamicClient(t *testing.T, uid string) *fake.FakeDynamicClient {
	t.Helper()
	vmi := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachineInstance",
		"metadata": map[string]interface{}{
			"name":      testVMIName,
			"namespace": testVMINamespace,
			"uid":       uid,
		},
		"status": map[string]interface{}{"phase": "Running"},
	}}
	vmi.SetUID(types.UID(uid))
	return fake.NewSimpleDynamicClient(runtime.NewScheme(), vmi)
}

type discardLog struct{}

func (discardLog) Info(string, ...any) {}
