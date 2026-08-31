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

package resources

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	routev1 "github.com/openshift/api/route/v1"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

func TestHostedControlPlaneNamespace(t *testing.T) {
	cases := []struct {
		name         string
		namespace    string
		instanceName string
		want         string
	}{
		{
			name:         "simple names are joined with a hyphen",
			namespace:    "clusters",
			instanceName: "hcp-pool-99h8d",
			want:         "clusters-hcp-pool-99h8d",
		},
		{
			name:         "dots in the instance name are replaced with hyphens",
			namespace:    "clusters",
			instanceName: "my.instance.name",
			want:         "clusters-my-instance-name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := HostedControlPlaneNamespace(tc.namespace, tc.instanceName)
			if got != tc.want {
				t.Errorf("HostedControlPlaneNamespace(%q, %q) = %q, want %q", tc.namespace, tc.instanceName, got, tc.want)
			}
		})
	}
}

func TestHostedClusterAPIRouteName(t *testing.T) {
	got := HostedClusterAPIRouteName("hcp-pool-99h8d")
	want := "hcp-pool-99h8d-api"
	if got != want {
		t.Errorf("HostedClusterAPIRouteName() = %q, want %q", got, want)
	}
}

func TestBuildHostedClusterAPIRoute(t *testing.T) {
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "hcp-pool-99h8d"},
	}
	host := "api-hcp-pool-99h8d.apps.example.com"
	hcpNamespace := "clusters-hcp-pool-99h8d"

	route := BuildHostedClusterAPIRoute(instance, host, hcpNamespace)

	if route.Name != "hcp-pool-99h8d-api" {
		t.Errorf("route.Name = %q, want %q", route.Name, "hcp-pool-99h8d-api")
	}
	if route.Namespace != hcpNamespace {
		t.Errorf("route.Namespace = %q, want %q", route.Namespace, hcpNamespace)
	}
	if route.Spec.Host != host {
		t.Errorf("route.Spec.Host = %q, want %q", route.Spec.Host, host)
	}
	if route.Spec.To.Kind != "Service" || route.Spec.To.Name != "kube-apiserver" {
		t.Errorf("route.Spec.To = %+v, want Service/kube-apiserver", route.Spec.To)
	}
	wantPort := intstr.FromInt32(hostedClusterKASPort)
	if route.Spec.Port == nil || route.Spec.Port.TargetPort != wantPort {
		t.Errorf("route.Spec.Port = %+v, want TargetPort %+v", route.Spec.Port, wantPort)
	}
	if route.Spec.TLS == nil || route.Spec.TLS.Termination != routev1.TLSTerminationPassthrough {
		t.Errorf("route.Spec.TLS = %+v, want Passthrough termination", route.Spec.TLS)
	}
}

func TestBuildHostedClusterSSHKey(t *testing.T) {
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "hcp-pool-99h8d"},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type: brokerv1alpha1.TopologyHCP,
			Template: brokerv1alpha1.ClusterTemplate{
				ReleaseImage: "quay.io/openshift-release-dev/ocp-release:4.22.10-x86_64",
			},
		},
	}

	baseOpts := HostedClusterOptions{
		Namespace:       "clusters",
		PullSecretName:  "pull-secret",
		NodePortAddress: "api-host.example.com",
	}

	t.Run("empty sshKeySecretName leaves HostedCluster.Spec.SSHKey unset", func(t *testing.T) {
		hc := BuildHostedCluster(instance, baseOpts)
		if hc.Spec.SSHKey.Name != "" {
			t.Errorf("Spec.SSHKey.Name = %q, want empty", hc.Spec.SSHKey.Name)
		}
	})

	t.Run("non-empty sshKeySecretName sets HostedCluster.Spec.SSHKey", func(t *testing.T) {
		opts := baseOpts
		opts.SSHKeySecretName = "hcp-pool-99h8d-hcp-ssh-key"
		hc := BuildHostedCluster(instance, opts)
		if hc.Spec.SSHKey.Name != "hcp-pool-99h8d-hcp-ssh-key" {
			t.Errorf("Spec.SSHKey.Name = %q, want %q", hc.Spec.SSHKey.Name, "hcp-pool-99h8d-hcp-ssh-key")
		}
	})
}

func TestBuildHostedClusterNamedCertificate(t *testing.T) {
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "hcp-pool-99h8d"},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type: brokerv1alpha1.TopologyHCP,
			Template: brokerv1alpha1.ClusterTemplate{
				ReleaseImage: "quay.io/openshift-release-dev/ocp-release:4.22.10-x86_64",
			},
		},
	}

	baseOpts := HostedClusterOptions{
		Namespace:       "clusters",
		PullSecretName:  "pull-secret",
		NodePortAddress: "api-host.example.com",
	}

	t.Run("empty servingCertName/servingCertHostname leaves Configuration.APIServer unset", func(t *testing.T) {
		hc := BuildHostedCluster(instance, baseOpts)
		if hc.Spec.Configuration != nil && hc.Spec.Configuration.APIServer != nil {
			t.Errorf("Configuration.APIServer = %+v, want nil", hc.Spec.Configuration.APIServer)
		}
	})

	t.Run("non-empty servingCertName/servingCertHostname wires a NamedCertificate", func(t *testing.T) {
		opts := baseOpts
		opts.ServingCertName = "hcp-pool-99h8d-kas-serving-cert"
		opts.ServingCertHostname = "api-hcp-pool-99h8d.apps.example.com"
		hc := BuildHostedCluster(instance, opts)
		if hc.Spec.Configuration == nil || hc.Spec.Configuration.APIServer == nil {
			t.Fatal("Configuration.APIServer = nil, want populated")
		}
		certs := hc.Spec.Configuration.APIServer.ServingCerts.NamedCertificates
		if len(certs) != 1 {
			t.Fatalf("len(NamedCertificates) = %d, want 1", len(certs))
		}
		if got, want := certs[0].ServingCertificate.Name, "hcp-pool-99h8d-kas-serving-cert"; got != want {
			t.Errorf("NamedCertificates[0].ServingCertificate.Name = %q, want %q", got, want)
		}
		if got, want := certs[0].Names, []string{"api-hcp-pool-99h8d.apps.example.com"}; len(got) != 1 || got[0] != want[0] {
			t.Errorf("NamedCertificates[0].Names = %v, want %v", got, want)
		}
	})
}

func TestBuildHostedClusterControllerAvailabilityPolicy(t *testing.T) {
	cases := []struct {
		name   string
		policy brokerv1alpha1.ControllerAvailabilityPolicy
		want   hyperv1beta1.AvailabilityPolicy
	}{
		{name: "empty defaults to SingleReplica", policy: "", want: hyperv1beta1.SingleReplica},
		{name: "explicit SingleReplica", policy: brokerv1alpha1.AvailabilityPolicySingleReplica, want: hyperv1beta1.SingleReplica},
		{name: "explicit HighlyAvailable", policy: brokerv1alpha1.AvailabilityPolicyHighlyAvailable, want: hyperv1beta1.HighlyAvailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instance := &brokerv1alpha1.ClusterInstance{
				ObjectMeta: metav1.ObjectMeta{Name: "hcp-pool-99h8d"},
				Spec: brokerv1alpha1.ClusterInstanceSpec{
					Type: brokerv1alpha1.TopologyHCP,
					Template: brokerv1alpha1.ClusterTemplate{
						ReleaseImage:                 "quay.io/openshift-release-dev/ocp-release:4.22.10-x86_64",
						ControllerAvailabilityPolicy: tc.policy,
					},
				},
			}
			hc := BuildHostedCluster(instance, HostedClusterOptions{
				Namespace:       "clusters",
				PullSecretName:  "pull-secret",
				NodePortAddress: "api-host.example.com",
			})
			if hc.Spec.ControllerAvailabilityPolicy != tc.want {
				t.Errorf("ControllerAvailabilityPolicy = %q, want %q", hc.Spec.ControllerAvailabilityPolicy, tc.want)
			}
		})
	}
}
