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
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

const (
	crcHostnameTestInstanceName = "crc-pool-abc123"
	crcHostnameTestNamespace    = "tenant-one"
)

func TestCRCAPIHostnameIsUniqueAndSharedAcrossResources(t *testing.T) {
	ctx := context.Background()
	instances := []*brokerv1alpha1.ClusterInstance{
		{ObjectMeta: metav1.ObjectMeta{Name: crcHostnameTestInstanceName, Namespace: crcHostnameTestNamespace}},
		{ObjectMeta: metav1.ObjectMeta{Name: crcHostnameTestInstanceName, Namespace: "tenant-two"}},
	}
	ingress := &configv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: statusIngressName},
		Spec:       configv1.IngressSpec{Domain: statusIngressDomain},
	}
	c := newCRCRecoveryFakeClient(t, instances[0], instances[1], ingress)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	hostnames := make([]string, 0, len(instances))

	for _, instance := range instances {
		hostname, err := r.ensureCRCAPIRoute(ctx, instance)
		if err != nil {
			t.Fatalf("ensureCRCAPIRoute for %s/%s: %v", instance.Namespace, instance.Name, err)
		}
		hostnames = append(hostnames, hostname)

		route := &routev1.Route{}
		routeKey := client.ObjectKey{Namespace: instance.Namespace, Name: resources.CRCAPIRouteName(instance.Name)}
		if err := c.Get(ctx, routeKey, route); err != nil {
			t.Fatalf("getting CRC API Route %s/%s: %v", routeKey.Namespace, routeKey.Name, err)
		}
		if route.Spec.Host != hostname {
			t.Errorf("Route host = %q, want %q", route.Spec.Host, hostname)
		}

		identityName, err := r.ensureCRCIdentity(ctx, instance, hostname)
		if err != nil {
			t.Fatalf("ensureCRCIdentity for %s/%s: %v", instance.Namespace, instance.Name, err)
		}
		identitySecret := &corev1.Secret{}
		identityKey := client.ObjectKey{Namespace: instance.Namespace, Name: identityName}
		if err := c.Get(ctx, identityKey, identitySecret); err != nil {
			t.Fatalf("getting CRC identity Secret %s/%s: %v", identityKey.Namespace, identityKey.Name, err)
		}
		if _, err := resources.CRCIdentityFromSecretData(identitySecret.Data, route.Spec.Host); err != nil {
			t.Errorf("CRC identity certificate does not match Route host %q: %v", route.Spec.Host, err)
		}

		job := resources.BuildCRCAgentJob(instance, "192.0.2.1", "vmi-"+instance.Namespace, "ssh-key", "id_ecdsa", identityName, "agent:test", hostname, "pull-secret")
		var configuredHostname string
		for _, env := range job.Spec.Template.Spec.Containers[0].Env {
			if env.Name == resources.CRCAPIHostnameEnvVar {
				configuredHostname = env.Value
				break
			}
		}
		if configuredHostname != route.Spec.Host {
			t.Errorf("crc-agent hostname = %q, want Route host %q", configuredHostname, route.Spec.Host)
		}
	}

	if hostnames[0] == hostnames[1] {
		t.Fatalf("same-named instances in different namespaces have the same Route host %q", hostnames[0])
	}
}

func TestEnsureCRCAPIRouteKeepsExistingHostname(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: crcHostnameTestInstanceName, Namespace: crcHostnameTestNamespace},
	}
	const legacyHostname = "api-crc-pool-abc123.apps.example.test"
	legacyRoute := resources.BuildCRCAPIRoute(instance, legacyHostname, resources.CRCAPIServiceName(instance.Name))
	ingress := &configv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: statusIngressName},
		Spec:       configv1.IngressSpec{Domain: statusIngressDomain},
	}
	c := newCRCRecoveryFakeClient(t, instance, legacyRoute, ingress)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	hostname, err := r.ensureCRCAPIRoute(ctx, instance)
	if err != nil {
		t.Fatalf("ensureCRCAPIRoute: %v", err)
	}
	if hostname != legacyHostname {
		t.Fatalf("hostname = %q, want existing hostname %q", hostname, legacyHostname)
	}
	storedRoute := &routev1.Route{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(legacyRoute), storedRoute); err != nil {
		t.Fatalf("getting existing CRC API Route: %v", err)
	}
	if storedRoute.Spec.Host != legacyHostname {
		t.Errorf("existing Route host = %q, want %q", storedRoute.Spec.Host, legacyHostname)
	}
}

func TestEnsureCRCAPIRouteRestoresIdentityHostname(t *testing.T) {
	for _, domain := range []string{"apps.example.test", "apps.changed.test", ""} {
		t.Run("ingress="+domain, func(t *testing.T) {
			ctx := context.Background()
			instance := &brokerv1alpha1.ClusterInstance{
				ObjectMeta: metav1.ObjectMeta{Name: crcHostnameTestInstanceName, Namespace: crcHostnameTestNamespace, UID: crcTestInstanceUID},
			}
			const hostname = "api-crc-pool-abc123.apps.example.test"
			route := resources.BuildCRCAPIRoute(instance, hostname, resources.CRCAPIServiceName(instance.Name))
			c := newCRCRecoveryFakeClient(t, instance, route)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			identityName, err := r.ensureCRCIdentity(ctx, instance, hostname)
			if err != nil {
				t.Fatalf("creating identity: %v", err)
			}
			before := &corev1.Secret{}
			key := client.ObjectKey{Namespace: instance.Namespace, Name: identityName}
			if err := c.Get(ctx, key, before); err != nil {
				t.Fatalf("getting identity: %v", err)
			}
			if domain != "" {
				ingress := &configv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: statusIngressName}, Spec: configv1.IngressSpec{Domain: domain}}
				if err := c.Create(ctx, ingress); err != nil {
					t.Fatalf("creating ingress config: %v", err)
				}
			}
			if err := c.Delete(ctx, route); err != nil {
				t.Fatalf("deleting Route: %v", err)
			}

			// Both Route recovery and a later reconcile must use the same host.
			for range 2 {
				got, err := r.ensureCRCAPIRoute(ctx, instance)
				if err != nil {
					t.Fatalf("restoring Route: %v", err)
				}
				if got != hostname {
					t.Fatalf("restored hostname = %q, want %q", got, hostname)
				}
				if _, err := r.ensureCRCIdentity(ctx, instance, got); err != nil {
					t.Fatalf("reusing identity after Route recovery: %v", err)
				}
			}
			storedRoute := &routev1.Route{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(route), storedRoute); err != nil {
				t.Fatalf("getting restored Route: %v", err)
			}
			if storedRoute.Spec.Host != hostname {
				t.Errorf("stored Route host = %q, want %q", storedRoute.Spec.Host, hostname)
			}
			after := &corev1.Secret{}
			if err := c.Get(ctx, key, after); err != nil {
				t.Fatalf("getting preserved identity: %v", err)
			}
			if !reflect.DeepEqual(before.Data, after.Data) {
				t.Fatal("Route recovery changed the identity credentials")
			}
		})
	}
}

func TestEnsureCRCAPIRouteRejectsInvalidRecoveryIdentity(t *testing.T) {
	for _, invalid := range []string{"certificate", "client key", "owner"} {
		t.Run(invalid, func(t *testing.T) {
			ctx := context.Background()
			instance := &brokerv1alpha1.ClusterInstance{
				ObjectMeta: metav1.ObjectMeta{Name: "crc-instance", Namespace: crcHostnameTestNamespace, UID: crcTestInstanceUID},
			}
			ingress := &configv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: statusIngressName}, Spec: configv1.IngressSpec{Domain: statusIngressDomain}}
			c := newCRCRecoveryFakeClient(t, instance, ingress)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			name, err := r.ensureCRCIdentity(ctx, instance, "api-crc-instance.apps.example.test")
			if err != nil {
				t.Fatalf("creating identity: %v", err)
			}
			secret := &corev1.Secret{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: name}, secret); err != nil {
				t.Fatalf("getting identity: %v", err)
			}
			switch invalid {
			case "certificate":
				secret.Data[resources.CRCIdentityServingCertKey] = []byte("invalid certificate")
			case "client key":
				secret.Data[resources.CRCIdentityClientPrivateKey] = []byte("invalid key")
			case "owner":
				secret.OwnerReferences[0].UID = "another-instance-uid"
			}
			if err := c.Update(ctx, secret); err != nil {
				t.Fatalf("updating identity: %v", err)
			}
			if _, err := r.ensureCRCAPIRoute(ctx, instance); err == nil {
				t.Fatal("Route recovery accepted an invalid identity")
			}
			key := client.ObjectKey{Namespace: instance.Namespace, Name: resources.CRCAPIRouteName(instance.Name)}
			if err := c.Get(ctx, key, &routev1.Route{}); !apierrors.IsNotFound(err) {
				t.Fatalf("expected no Route after failed recovery, got error %v", err)
			}
		})
	}
}
