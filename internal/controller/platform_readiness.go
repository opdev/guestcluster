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

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	conditionTypeHyperConvergedReady     = "HyperConvergedReady"
	conditionTypeMultiClusterEngineReady = "MultiClusterEngineReady"
	hyperShiftGroup                      = "hypershift.openshift.io"
)

var (
	hyperConvergedGVK           = schema.GroupVersionKind{Group: "hco.kubevirt.io", Version: "v1beta1", Kind: "HyperConverged"}
	multiClusterEngineGVK       = schema.GroupVersionKind{Group: "multicluster.openshift.io", Version: "v1", Kind: "MultiClusterEngine"}
	customResourceDefinitionGVK = schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}
)

type platformCondition struct {
	condition metav1.Condition
	ready     bool
}

func (r *ClusterInstanceReconciler) platformReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *ClusterInstanceReconciler) checkHyperConverged(ctx context.Context) platformCondition {
	operands := &unstructured.UnstructuredList{}
	operands.SetGroupVersionKind(hyperConvergedGVK.GroupVersion().WithKind("HyperConvergedList"))
	if err := r.platformReader().List(ctx, operands); err != nil {
		return unavailablePlatformCondition(conditionTypeHyperConvergedReady, "HyperConverged")
	}
	if len(operands.Items) != 1 {
		return singletonPlatformCondition(conditionTypeHyperConvergedReady, "HyperConverged", len(operands.Items))
	}

	operand := &operands.Items[0]
	if observedGeneration, found, _ := unstructured.NestedInt64(operand.Object, "status", "observedGeneration"); !found || observedGeneration < operand.GetGeneration() {
		return notReadyPlatformCondition(conditionTypeHyperConvergedReady, "StatusStale", "HyperConverged status does not observe its current generation")
	}
	for _, expected := range []struct {
		typeName string
		status   metav1.ConditionStatus
	}{
		{typeName: "Available", status: metav1.ConditionTrue},
		{typeName: "Progressing", status: metav1.ConditionFalse},
		{typeName: "Degraded", status: metav1.ConditionFalse},
	} {
		actual, found := conditionStatus(operand, expected.typeName)
		if !found || actual != expected.status {
			return notReadyPlatformCondition(conditionTypeHyperConvergedReady, "OperandNotReady", fmt.Sprintf("HyperConverged condition %s must be %s", expected.typeName, expected.status))
		}
	}

	return readyPlatformCondition(conditionTypeHyperConvergedReady, "HyperConverged is available")
}

func (r *ClusterInstanceReconciler) checkMultiClusterEngine(ctx context.Context) platformCondition {
	operands := &unstructured.UnstructuredList{}
	operands.SetGroupVersionKind(multiClusterEngineGVK.GroupVersion().WithKind("MultiClusterEngineList"))
	if err := r.platformReader().List(ctx, operands); err != nil {
		return unavailablePlatformCondition(conditionTypeMultiClusterEngineReady, "MultiClusterEngine")
	}
	if len(operands.Items) != 1 {
		return singletonPlatformCondition(conditionTypeMultiClusterEngineReady, "MultiClusterEngine", len(operands.Items))
	}
	if !hyperShiftComponentEnabled(&operands.Items[0]) {
		return notReadyPlatformCondition(conditionTypeMultiClusterEngineReady, "ComponentDisabled", "MultiClusterEngine does not enable the hypershift component")
	}

	for _, name := range []string{"hostedclusters." + hyperShiftGroup, "nodepools." + hyperShiftGroup} {
		crd := &unstructured.Unstructured{}
		crd.SetGroupVersionKind(customResourceDefinitionGVK)
		if err := r.platformReader().Get(ctx, client.ObjectKey{Name: name}, crd); err != nil {
			return unavailablePlatformCondition(conditionTypeMultiClusterEngineReady, name)
		}
		if status, found := conditionStatus(crd, "Established"); !found || status != metav1.ConditionTrue {
			return notReadyPlatformCondition(conditionTypeMultiClusterEngineReady, "OperandNotReady", fmt.Sprintf("CustomResourceDefinition %s is not established", name))
		}
	}

	operator := &appsv1.Deployment{}
	if err := r.platformReader().Get(ctx, client.ObjectKey{Namespace: "hypershift", Name: "operator"}, operator); err != nil {
		return unavailablePlatformCondition(conditionTypeMultiClusterEngineReady, "hypershift/operator Deployment")
	}
	if operator.Status.AvailableReplicas < 1 {
		return notReadyPlatformCondition(conditionTypeMultiClusterEngineReady, "OperandNotReady", "Deployment hypershift/operator has no available replicas")
	}

	return readyPlatformCondition(conditionTypeMultiClusterEngineReady, "HyperShift APIs and operator are available")
}

func conditionStatus(obj *unstructured.Unstructured, typeName string) (metav1.ConditionStatus, bool) {
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return "", false
	}
	for _, item := range conditions {
		condition, ok := item.(map[string]interface{})
		if !ok || condition["type"] != typeName {
			continue
		}
		status, ok := condition["status"].(string)
		return metav1.ConditionStatus(status), ok
	}
	return "", false
}

func hyperShiftComponentEnabled(mce *unstructured.Unstructured) bool {
	components, found, _ := unstructured.NestedSlice(mce.Object, "spec", "overrides", "components")
	if !found {
		return false
	}
	for _, item := range components {
		component, ok := item.(map[string]interface{})
		if !ok || component["name"] != "hypershift" {
			continue
		}
		enabled, found, _ := unstructured.NestedBool(component, "enabled")
		return !found || enabled
	}
	return false
}

func readyPlatformCondition(conditionType, message string) platformCondition {
	return platformCondition{ready: true, condition: metav1.Condition{Type: conditionType, Status: metav1.ConditionTrue, Reason: "OperandReady", Message: message}}
}

func singletonPlatformCondition(conditionType, operand string, count int) platformCondition {
	if count == 0 {
		return notReadyPlatformCondition(conditionType, "OperandNotFound", fmt.Sprintf("no %s operand was found", operand))
	}
	return notReadyPlatformCondition(conditionType, "MultipleOperandsFound", fmt.Sprintf("found %d %s operands; exactly one is required", count, operand))
}

func unavailablePlatformCondition(conditionType, operand string) platformCondition {
	return notReadyPlatformCondition(conditionType, "APIUnavailable", fmt.Sprintf("cannot read %s", operand))
}

func notReadyPlatformCondition(conditionType, reason, message string) platformCondition {
	return platformCondition{condition: metav1.Condition{Type: conditionType, Status: metav1.ConditionFalse, Reason: reason, Message: message}}
}
