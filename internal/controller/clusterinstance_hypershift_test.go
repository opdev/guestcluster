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
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

// TestDesiredReplicas is a plain testing.T table test (not ginkgo/envtest --
// desiredReplicas is a pure function of ClusterInstance.Spec, so it needs no
// live cluster) covering the topology=hcp worker replica default/override
// behavior now that hcp-1/hcp-n no longer exist as separate topologies.
func TestDesiredReplicas(t *testing.T) {
	int32Ptr := func(v int32) *int32 { return &v }

	cases := []struct {
		name     string
		replicas *int32
		want     int32
	}{
		{name: "unset defaults to 1", replicas: nil, want: 1},
		{name: "explicit 1", replicas: int32Ptr(1), want: 1},
		{name: "explicit N", replicas: int32Ptr(3), want: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instance := &brokerv1alpha1.ClusterInstance{
				ObjectMeta: metav1.ObjectMeta{Name: "hcp-pool-99h8d"},
				Spec: brokerv1alpha1.ClusterInstanceSpec{
					Type: brokerv1alpha1.TopologyHCP,
					Template: brokerv1alpha1.ClusterTemplate{
						NodePoolReplicas: tc.replicas,
					},
				},
			}
			if got := desiredReplicas(instance); got != tc.want {
				t.Errorf("desiredReplicas() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestReconcileHyperShiftCopiesExplicitPullSecret(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "hcp-pull-secret", Namespace: pullSecretTestNamespace},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type: brokerv1alpha1.TopologyHCP,
			Template: brokerv1alpha1.ClusterTemplate{
				OCPVersion:   "4.16.0",
				ReleaseImage: "quay.io/openshift-release-dev/ocp-release:4.16.0-x86_64",
				Memory:       "8Gi",
				Cores:        2,
				PullSecretRef: corev1.LocalObjectReference{
					Name: "explicit-pull-secret",
				},
			},
		},
	}
	pullSecretData := []byte(`{"auths":{"quay.io":{"auth":"explicit"}}}`)
	pullSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "explicit-pull-secret", Namespace: instance.Namespace},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{resources.PullSecretDataKey: pullSecretData},
	}
	ingress := &configv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Spec:       configv1.IngressSpec{Domain: "apps.example.test"},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0"},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.10"}},
		},
	}
	c := newHyperShiftFakeClient(t, instance, pullSecret, ingress, node)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
		t.Fatalf("reconcileHyperShift: %v", err)
	}

	copyName := resources.DefaultPullSecretName(instance.Name)
	copy := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: copyName, Namespace: resources.DefaultHostedClusterNamespace}, copy); err != nil {
		t.Fatalf("getting copied pull secret: %v", err)
	}
	if got := string(copy.Data[resources.PullSecretDataKey]); got != string(pullSecretData) {
		t.Errorf("copied pull secret data = %q, want %q", got, pullSecretData)
	}

	hostedCluster := &hyperv1beta1.HostedCluster{}
	if err := c.Get(ctx, client.ObjectKey{Name: resources.HostedClusterName(instance.Name), Namespace: resources.DefaultHostedClusterNamespace}, hostedCluster); err != nil {
		t.Fatalf("getting HostedCluster: %v", err)
	}
	if got := hostedCluster.Spec.PullSecret.Name; got != copyName {
		t.Errorf("HostedCluster.Spec.PullSecret.Name = %q, want %q", got, copyName)
	}
}

func newHyperShiftFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("adding core scheme: %v", err)
	}
	if err := brokerv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("adding GuestCluster scheme: %v", err)
	}
	if err := configv1.AddToScheme(s); err != nil {
		t.Fatalf("adding OpenShift Config scheme: %v", err)
	}
	if err := hyperv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("adding HyperShift scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&brokerv1alpha1.ClusterInstance{}).
		WithObjects(objects...).Build()
}
