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
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
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

// instanceForRoute maps a managed API Route change back to its source
// ClusterInstance. Every managed Route carries its source namespace label.
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
	return nil
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

func routeOwnedByInstance(route *routev1.Route, instance *brokerv1alpha1.ClusterInstance) bool {
	return route.Labels[resources.LabelManagedBy] == resources.ManagerName &&
		route.Labels[resources.LabelInstance] == instance.Name &&
		route.Labels[resources.LabelInstanceNamespace] == instance.Namespace
}
