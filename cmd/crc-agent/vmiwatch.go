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
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

var (
	crcVMIGVR = schema.GroupVersionResource{
		Group: "kubevirt.io", Version: "v1", Resource: "virtualmachineinstances",
	}
	errCRCVMINoLongerCurrent = errors.New("CRC VirtualMachineInstance is no longer current")
)

const vmiWatchRetryInterval = time.Second

// monitorCRCVMILifecycle verifies the VMI before guest changes start, then
// cancels the returned context if that VMI is deleted or replaced. It relists
// before each watch to recover from closed or expired watch streams.
func monitorCRCVMILifecycle(
	ctx context.Context, client dynamic.Interface, cfg config, log logrLike,
) (context.Context, context.CancelFunc, error) {
	vmis := client.Resource(crcVMIGVR).Namespace(cfg.Namespace)
	if err := ensureExpectedCRCVMIRunning(ctx, vmis, cfg); err != nil {
		return nil, nil, err
	}

	watchCtx, cancel := context.WithCancelCause(ctx)
	go func() {
		for {
			vmi, err := getExpectedCRCVMI(watchCtx, vmis, cfg)
			if err != nil {
				if errors.Is(err, errCRCVMINoLongerCurrent) {
					cancel(err)
					return
				}
				log.Info("unable to list CRC VirtualMachineInstance; retrying watch", "error", err.Error())
				if !waitForVMIWatchRetry(watchCtx) {
					return
				}
				continue
			}

			w, err := vmis.Watch(watchCtx, metav1.ListOptions{
				FieldSelector:       fields.OneTermEqualSelector("metadata.name", cfg.InstanceName).String(),
				ResourceVersion:     vmi.GetResourceVersion(),
				AllowWatchBookmarks: true,
			})
			if err != nil {
				log.Info("unable to watch CRC VirtualMachineInstance; retrying", "error", err.Error())
				if !waitForVMIWatchRetry(watchCtx) {
					return
				}
				continue
			}

			if err := waitForCRCVMITermination(watchCtx, w, cfg); err != nil {
				w.Stop()
				cancel(err)
				return
			}
			w.Stop()
			if !waitForVMIWatchRetry(watchCtx) {
				return
			}
		}
	}()

	return watchCtx, func() { cancel(nil) }, nil
}

func ensureExpectedCRCVMIRunning(ctx context.Context, vmis dynamic.ResourceInterface, cfg config) error {
	_, err := getExpectedCRCVMI(ctx, vmis, cfg)
	return err
}

func getExpectedCRCVMI(
	ctx context.Context, vmis dynamic.ResourceInterface, cfg config,
) (*unstructured.Unstructured, error) {
	vmi, err := vmis.Get(ctx, cfg.InstanceName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: %s/%s was deleted", errCRCVMINoLongerCurrent, cfg.Namespace, cfg.InstanceName)
	}
	if err != nil {
		return nil, err
	}
	if string(vmi.GetUID()) != cfg.ExpectedVMIUID {
		return nil, fmt.Errorf("%w: %s/%s UID changed from %s to %s",
			errCRCVMINoLongerCurrent, cfg.Namespace, cfg.InstanceName, cfg.ExpectedVMIUID, vmi.GetUID())
	}
	phase, _, _ := unstructured.NestedString(vmi.Object, "status", "phase")
	if phase != string(kubevirtv1.Running) {
		return nil, fmt.Errorf("%w: %s/%s is not running", errCRCVMINoLongerCurrent, cfg.Namespace, cfg.InstanceName)
	}
	return vmi, nil
}

func waitForCRCVMITermination(ctx context.Context, w watch.Interface, cfg config) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-w.ResultChan():
			if !ok {
				return nil
			}
			vmi, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			switch event.Type {
			case watch.Deleted:
				return fmt.Errorf("%w: %s/%s was deleted or replaced", errCRCVMINoLongerCurrent, cfg.Namespace, cfg.InstanceName)
			case watch.Added, watch.Modified:
				if string(vmi.GetUID()) != cfg.ExpectedVMIUID {
					return fmt.Errorf("%w: %s/%s was deleted or replaced", errCRCVMINoLongerCurrent, cfg.Namespace, cfg.InstanceName)
				}
				phase, _, _ := unstructured.NestedString(vmi.Object, "status", "phase")
				if phase != string(kubevirtv1.Running) {
					return fmt.Errorf("%w: %s/%s is not running", errCRCVMINoLongerCurrent, cfg.Namespace, cfg.InstanceName)
				}
			}
		}
	}
}

func waitForVMIWatchRetry(ctx context.Context) bool {
	timer := time.NewTimer(vmiWatchRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
