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
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	routev1 "github.com/openshift/api/route/v1"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
)

type apiEndpointConflictError struct {
	message string
}

func (e apiEndpointConflictError) Error() string { return e.message }

func apiEndpointConflict(format string, args ...any) error {
	return apiEndpointConflictError{message: fmt.Sprintf(format, args...)}
}

func apiEndpointHostname(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Host != parsed.Hostname() ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", apiEndpointConflict("stored API endpoint %q is not a hostname-only HTTPS URL", endpoint)
	}
	return parsed.Hostname(), nil
}

func validateAPIHostname(hostname string) error {
	if problems := validation.IsDNS1123Subdomain(hostname); len(problems) > 0 {
		return apiEndpointConflict("API hostname %q is not a valid DNS name: %s", hostname, strings.Join(problems, ", "))
	}
	return nil
}

// ensureAPIHostnameAvailable reports a conflict if another Route already uses
// hostname. Route hostnames are cluster-wide even when their Route objects
// live in different namespaces.
func (r *ClusterInstanceReconciler) ensureAPIHostnameAvailable(ctx context.Context, hostname string, ownRoute types.NamespacedName) error {
	routes := &routev1.RouteList{}
	if err := r.platformReader().List(ctx, routes); err != nil {
		return fmt.Errorf("listing Routes to check API hostname %q: %w", hostname, err)
	}
	for i := range routes.Items {
		route := &routes.Items[i]
		if route.Namespace == ownRoute.Namespace && route.Name == ownRoute.Name {
			continue
		}
		if route.Spec.Host == hostname {
			return apiEndpointConflict("API hostname %q is already used by Route %s/%s", hostname, route.Namespace, route.Name)
		}
	}
	return nil
}

// instanceForRoute maps managed API Route changes back to their source
// ClusterInstance. New Routes carry the source namespace label. For legacy
// Routes, CRC Routes share the instance namespace and HCP Routes use the
// HostedControlPlane namespace.
func (r *ClusterInstanceReconciler) instanceForRoute(ctx context.Context, obj client.Object) []reconcile.Request {
	route, ok := obj.(*routev1.Route)
	if !ok || route.Labels[resources.LabelManagedBy] != resources.ManagerName {
		return nil
	}
	instanceName := route.Labels[resources.LabelInstance]
	if instanceName == "" {
		return nil
	}
	if instanceNamespace := route.Labels[resources.LabelInstanceNamespace]; instanceNamespace != "" {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: instanceName, Namespace: instanceNamespace}}}
	}

	legacyCRC := &brokerv1alpha1.ClusterInstance{}
	if err := r.Get(ctx, types.NamespacedName{Name: instanceName, Namespace: route.Namespace}, legacyCRC); err == nil && legacyCRC.Spec.Type == brokerv1alpha1.TopologyCRC {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: legacyCRC.Name, Namespace: legacyCRC.Namespace}}}
	}

	instances := &brokerv1alpha1.ClusterInstanceList{}
	if err := r.List(ctx, instances); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, 1)
	for i := range instances.Items {
		instance := &instances.Items[i]
		if instance.Spec.Type != brokerv1alpha1.TopologyHCP || instance.Name != instanceName {
			continue
		}
		namespace, name := hcpLocation(instance)
		if resources.HostedControlPlaneNamespace(namespace, name) != route.Namespace {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace},
		})
	}
	return requests
}

func routeIsAdmitted(route *routev1.Route) bool {
	for _, ingress := range route.Status.Ingress {
		if ingress.Host != "" && ingress.Host != route.Spec.Host {
			continue
		}
		for _, condition := range ingress.Conditions {
			if condition.Type == routev1.RouteAdmitted && condition.Status == corev1.ConditionTrue {
				return true
			}
		}
	}
	return false
}

func (r *ClusterInstanceReconciler) getServingCertHostname(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, namespace, secretName string) (string, bool, error) {
	key := types.NamespacedName{Name: secretName, Namespace: namespace}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, key, secret); apierrors.IsNotFound(err) {
		return "", false, nil
	} else if err != nil {
		return "", false, fmt.Errorf("getting KAS serving certificate Secret %s/%s: %w", namespace, secretName, err)
	}
	if err := r.verifyHCPResource(ctx, instance, secret); err != nil {
		return "", false, err
	}
	block, _ := pem.Decode(secret.Data[corev1.TLSCertKey])
	if block == nil {
		return "", false, apiEndpointConflict("KAS serving certificate Secret %s/%s has no valid certificate PEM", namespace, secretName)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", false, apiEndpointConflict("KAS serving certificate Secret %s/%s has an invalid certificate: %v", namespace, secretName, err)
	}
	if len(cert.DNSNames) != 1 || cert.DNSNames[0] == "" {
		return "", false, apiEndpointConflict("KAS serving certificate Secret %s/%s must have exactly one DNS SAN", namespace, secretName)
	}
	return cert.DNSNames[0], true, nil
}

func routeOwnedByInstance(route *routev1.Route, instance *brokerv1alpha1.ClusterInstance) bool {
	if route.Labels[resources.LabelManagedBy] != resources.ManagerName || route.Labels[resources.LabelInstance] != instance.Name {
		return false
	}
	if sourceNamespace := route.Labels[resources.LabelInstanceNamespace]; sourceNamespace != "" && sourceNamespace != instance.Namespace {
		return false
	}
	return true
}

func ensureHostnameEvidenceMatches(hostname, source string, evidence *string) error {
	if hostname == "" {
		return nil
	}
	if *evidence != "" && *evidence != hostname {
		return apiEndpointConflict("conflicting API hostnames in existing resources: %q and %q from %s", *evidence, hostname, source)
	}
	*evidence = hostname
	return nil
}
