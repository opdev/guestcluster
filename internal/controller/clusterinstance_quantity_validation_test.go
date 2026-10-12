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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
)

func TestValidateTemplateQuantities(t *testing.T) {
	tests := []struct {
		name    string
		memory  string
		root    string
		wantErr string // empty means the template must pass
	}{
		{name: "valid quantities", memory: testMemory, root: "35Gi"},
		{name: "empty root volume size selects the default", memory: testMemory, root: ""},
		{name: "invalid memory", memory: "16 GB", root: "", wantErr: "template.memory"},
		{name: "invalid root volume size", memory: testMemory, root: "35 GB", wantErr: "template.rootVolumeSize"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTemplateQuantities(brokerv1alpha1.ClusterTemplate{Memory: tc.memory, RootVolumeSize: tc.root})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateTemplateQuantities(%q, %q) = %v, want success", tc.memory, tc.root, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateTemplateQuantities(%q, %q) = %v, want a %q failure", tc.memory, tc.root, err, tc.wantErr)
			}
		})
	}
}

// TestReconcileFailsInvalidTemplateQuantity drives the CRC and HCP
// provisioning paths with a template quantity that cannot parse. The CRD
// rejects such values at admission time; this covers objects that predate
// that validation. The instance must fail with the distinct InvalidTemplate
// reason, and no backing object may be created.
func TestReconcileFailsInvalidTemplateQuantity(t *testing.T) {
	t.Run("crc rejects an unparsable memory", func(t *testing.T) {
		ctx := context.Background()
		instance := &brokerv1alpha1.ClusterInstance{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-memory-crc", Namespace: pullSecretTestNamespace, UID: "invalid-memory-crc"},
			Spec: brokerv1alpha1.ClusterInstanceSpec{
				Type:     brokerv1alpha1.TopologyCRC,
				Template: brokerv1alpha1.ClusterTemplate{Memory: "16 GB", Cores: 2},
			},
		}
		c := newCRCRecoveryFakeClient(t, instance)
		r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

		_, err := r.reconcileCRC(ctx, instance)
		if err == nil || !strings.Contains(err.Error(), "template.memory") {
			t.Fatalf("reconcileCRC error = %v, want a template.memory failure", err)
		}
		assertInvalidTemplateFailed(t, c, instance)
		dv := &cdiv1beta1.DataVolume{}
		if err := c.Get(ctx, types.NamespacedName{Name: resources.CRCDiskName(instance), Namespace: instance.Namespace}, dv); !apierrors.IsNotFound(err) {
			t.Fatalf("DataVolume created for an invalid template: %v", err)
		}
		vm := &kubevirtv1.VirtualMachine{}
		if err := c.Get(ctx, types.NamespacedName{Name: resources.CRCVMName(instance), Namespace: instance.Namespace}, vm); !apierrors.IsNotFound(err) {
			t.Fatalf("VirtualMachine created for an invalid template: %v", err)
		}
	})

	t.Run("hcp rejects an unparsable root volume size", func(t *testing.T) {
		ctx := context.Background()
		instance := &brokerv1alpha1.ClusterInstance{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-root-hcp", Namespace: pullSecretTestNamespace, UID: "invalid-root-hcp"},
			Spec: brokerv1alpha1.ClusterInstanceSpec{
				Type:     brokerv1alpha1.TopologyHCP,
				Template: brokerv1alpha1.ClusterTemplate{Memory: "6Gi", Cores: 2, RootVolumeSize: "35 GB"},
			},
		}
		c := newHyperShiftFakeClient(t, instance)
		r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

		_, err := r.reconcileHyperShift(ctx, instance)
		if err == nil || !strings.Contains(err.Error(), "template.rootVolumeSize") {
			t.Fatalf("reconcileHyperShift error = %v, want a template.rootVolumeSize failure", err)
		}
		assertInvalidTemplateFailed(t, c, instance)
	})
}

// assertInvalidTemplateFailed checks that instance was persisted with
// Phase=Failed and a Ready condition whose reason is InvalidTemplate.
func assertInvalidTemplateFailed(t *testing.T, c client.Client, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	updated := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(instance), updated); err != nil {
		t.Fatalf("getting updated instance: %v", err)
	}
	if updated.Status.Phase != brokerv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want %q", updated.Status.Phase, brokerv1alpha1.PhaseFailed)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, conditionTypeReady)
	if condition == nil || condition.Reason != "InvalidTemplate" {
		t.Fatalf("Ready condition = %+v, want reason InvalidTemplate", condition)
	}
}
