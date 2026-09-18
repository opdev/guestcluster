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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

const pullSecretTestNamespace = "tenant"

func TestResolvePullSecretUsesNamespaceDefault(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "crc-default-pull-secret", Namespace: pullSecretTestNamespace},
	}
	secretData := []byte(`{"auths":{"quay.io":{"auth":"local"}}}`)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: resources.ClusterPullSecretName, Namespace: instance.Namespace},
		Data:       map[string][]byte{resources.PullSecretDataKey: secretData},
	}
	c := newHyperShiftFakeClient(t, instance, secret)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	name, err := r.resolvePullSecret(ctx, instance, instance.Namespace)
	if err != nil {
		t.Fatalf("resolvePullSecret: %v", err)
	}
	if name != resources.ClusterPullSecretName {
		t.Fatalf("pull secret name = %q, want %q", name, resources.ClusterPullSecretName)
	}

	copy := &corev1.Secret{}
	err = c.Get(ctx, client.ObjectKey{Name: resources.DefaultPullSecretName(instance.Name), Namespace: instance.Namespace}, copy)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("default pull secret copy error = %v, want NotFound", err)
	}
}

func TestResolvePullSecretCopiesNamespaceDefaultForHyperShift(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "hcp-default-pull-secret", Namespace: pullSecretTestNamespace},
	}
	secretData := []byte(`{"auths":{"quay.io":{"auth":"local"}}}`)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: resources.ClusterPullSecretName, Namespace: instance.Namespace},
		Data:       map[string][]byte{resources.PullSecretDataKey: secretData},
	}
	c := newHyperShiftFakeClient(t, instance, secret)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	name, err := r.resolvePullSecret(ctx, instance, resources.DefaultHostedClusterNamespace)
	if err != nil {
		t.Fatalf("resolvePullSecret: %v", err)
	}
	wantName := resources.DefaultPullSecretName(instance.Name)
	if name != wantName {
		t.Fatalf("pull secret name = %q, want %q", name, wantName)
	}

	copy := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: wantName, Namespace: resources.DefaultHostedClusterNamespace}, copy); err != nil {
		t.Fatalf("getting copied pull secret: %v", err)
	}
	if string(copy.Data[resources.PullSecretDataKey]) != string(secretData) {
		t.Fatalf("copied pull secret data = %q, want %q", copy.Data[resources.PullSecretDataKey], secretData)
	}
}

func TestResolvePullSecretReportsMissingNamespaceDefault(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "missing-pull-secret", Namespace: pullSecretTestNamespace},
	}
	c := newHyperShiftFakeClient(t, instance)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	_, err := r.resolvePullSecret(ctx, instance, instance.Namespace)
	if err == nil || !strings.Contains(err.Error(), "tenant/pull-secret") {
		t.Fatalf("resolvePullSecret error = %v, want missing tenant/pull-secret", err)
	}
}
