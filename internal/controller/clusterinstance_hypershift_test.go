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
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

const hcpEndpointTestAdminName = "admin"

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

func TestReconcileHyperShiftUsesLocalPullSecret(t *testing.T) {
	ctx := context.Background()
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "hcp-pull-secret", Namespace: pullSecretTestNamespace},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type: brokerv1alpha1.TopologyHCP,
			Template: brokerv1alpha1.ClusterTemplate{
				OCPVersion:   testOCPVersion,
				ReleaseImage: statusHCPReleaseImage,
				Memory:       statusHCPMemory,
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
		ObjectMeta: metav1.ObjectMeta{Name: statusIngressName},
		Spec:       configv1.IngressSpec{Domain: statusIngressDomain},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: testManagementNodeName},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: testManagementNodeIP}},
		},
	}
	c := newHyperShiftFakeClient(t, instance, pullSecret, ingress, node)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.recordHCPPlacement(ctx, instance); err != nil {
		t.Fatal(err)
	}

	if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
		t.Fatalf("reconcileHyperShift: %v", err)
	}
	if instance.Status.APIEndpoint == "" {
		t.Fatal("first reconcile did not persist the selected API endpoint")
	}
	hostedClusterKey := client.ObjectKey{Name: resources.HostedClusterName(instance.Name), Namespace: instance.Namespace}
	if err := c.Get(ctx, hostedClusterKey, &hyperv1beta1.HostedCluster{}); err == nil {
		t.Fatal("HostedCluster was created before status recorded the selected API endpoint")
	}
	if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
		t.Fatalf("second reconcileHyperShift: %v", err)
	}

	copyName := instance.Spec.Template.PullSecretRef.Name
	copy := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: copyName, Namespace: instance.Namespace}, copy); err != nil {
		t.Fatalf("getting copied pull secret: %v", err)
	}
	if got := string(copy.Data[resources.PullSecretDataKey]); got != string(pullSecretData) {
		t.Errorf("copied pull secret data = %q, want %q", got, pullSecretData)
	}

	hostedCluster := &hyperv1beta1.HostedCluster{}
	if err := c.Get(ctx, hostedClusterKey, hostedCluster); err != nil {
		t.Fatalf("getting HostedCluster: %v", err)
	}
	if got := hostedCluster.Spec.PullSecret.Name; got != copyName {
		t.Errorf("HostedCluster.Spec.PullSecret.Name = %q, want %q", got, copyName)
	}
}

func TestHCPAPIEndpointIsRecordedAndSharedAcrossResources(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("hcp-endpoint", "tenant-one")
	pullSecret := hcpEndpointTestPullSecret(instance)
	ingress := &configv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: statusIngressName},
		Spec:       configv1.IngressSpec{Domain: "apps.original.test"},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: testManagementNodeName},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: testManagementNodeIP}},
		},
	}
	c := newHyperShiftFakeClient(t, instance, pullSecret, ingress, node)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	hc, hostname, routeKey := createHCPWithSelectedEndpoint(t, c, r, instance, ingress)

	adminConfig, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"guest": {Server: "https://192.0.2.20:31381"}},
		Contexts:       map[string]*clientcmdapi.Context{hcpEndpointTestAdminName: {Cluster: "guest", AuthInfo: hcpEndpointTestAdminName}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{hcpEndpointTestAdminName: {Token: "admin-token"}},
		CurrentContext: hcpEndpointTestAdminName,
	})
	if err != nil {
		t.Fatalf("building admin kubeconfig: %v", err)
	}
	hc.Status.Conditions = []metav1.Condition{{
		Type: string(hyperv1beta1.HostedClusterAvailable), Status: metav1.ConditionTrue, Reason: "Available", Message: "ready",
	}}
	hc.Status.KubeConfig = &corev1.LocalObjectReference{Name: "admin-kubeconfig"}
	if err := c.Update(ctx, hc); err != nil {
		t.Fatalf("updating HostedCluster status: %v", err)
	}
	nodePool := resources.BuildNodePool(instance, hc.Name, hc.Namespace, 1)
	if err := controllerutil.SetControllerReference(hc, nodePool, c.Scheme()); err != nil {
		t.Fatal(err)
	}
	if err := r.setHCPResourceOwner(instance, nodePool); err != nil {
		t.Fatal(err)
	}
	nodePool.Status.Replicas = 1
	if err := c.Create(ctx, nodePool); err != nil {
		t.Fatalf("creating ready NodePool: %v", err)
	}
	adminSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-kubeconfig", Namespace: hc.Namespace},
		Data:       map[string][]byte{resources.KubeconfigSecretKey: adminConfig},
	}
	if err := c.Create(ctx, adminSecret); err != nil {
		t.Fatalf("creating admin kubeconfig Secret: %v", err)
	}

	result, err := r.reconcileHyperShift(ctx, instance)
	if err != nil {
		t.Fatalf("reconcile before Route admission: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("RequeueAfter before Route admission = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	apiRoute := &routev1.Route{}
	if err := c.Get(ctx, routeKey, apiRoute); err != nil {
		t.Fatalf("getting created API Route: %v", err)
	}
	if apiRoute.Spec.Host != hostname {
		t.Fatalf("API Route host = %q, want %q", apiRoute.Spec.Host, hostname)
	}
	if instance.Status.Phase == brokerv1alpha1.PhaseReady {
		t.Fatal("instance became Ready before the API Route was admitted")
	}
	readyCondition := apimeta.FindStatusCondition(instance.Status.Conditions, conditionTypeReady)
	if readyCondition == nil || readyCondition.Reason != "APIEndpointRouteNotAdmitted" || readyCondition.Status != metav1.ConditionFalse {
		t.Fatalf("Ready condition = %+v, want False with reason APIEndpointRouteNotAdmitted", readyCondition)
	}

	apiRoute.Status.Ingress = []routev1.RouteIngress{{
		Host:       hostname,
		Conditions: []routev1.RouteIngressCondition{{Type: routev1.RouteAdmitted, Status: corev1.ConditionTrue}},
	}}
	if err := c.Update(ctx, apiRoute); err != nil {
		t.Fatalf("admitting API Route: %v", err)
	}
	if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
		t.Fatalf("reconcile after Route admission: %v", err)
	}
	if instance.Status.Phase != brokerv1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", instance.Status.Phase)
	}
	if instance.Status.APIEndpoint != "https://"+hostname {
		t.Errorf("status APIEndpoint = %q, want https://%s", instance.Status.APIEndpoint, hostname)
	}
	kubeconfigSecret := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: resources.KubeconfigSecretName(instance.Name)}, kubeconfigSecret); err != nil {
		t.Fatalf("getting published kubeconfig Secret: %v", err)
	}
	published, err := clientcmd.Load(kubeconfigSecret.Data[resources.KubeconfigSecretKey])
	if err != nil {
		t.Fatalf("loading published kubeconfig: %v", err)
	}
	for _, cluster := range published.Clusters {
		if cluster.Server != "https://"+hostname {
			t.Errorf("published kubeconfig server = %q, want https://%s", cluster.Server, hostname)
		}
	}
}

func createHCPWithSelectedEndpoint(t *testing.T, c client.Client, r *ClusterInstanceReconciler, instance *brokerv1alpha1.ClusterInstance, ingress *configv1.Ingress) (*hyperv1beta1.HostedCluster, string, client.ObjectKey) {
	t.Helper()
	ctx := context.Background()
	if _, err := r.recordHCPPlacement(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
		t.Fatalf("first reconcileHyperShift: %v", err)
	}
	if instance.Status.APIEndpoint == "" {
		t.Fatal("first reconcile did not record APIEndpoint")
	}
	hostname, err := apiEndpointHostname(instance.Status.APIEndpoint)
	if err != nil {
		t.Fatalf("parsing selected endpoint: %v", err)
	}
	hcKey := client.ObjectKey{Name: resources.HostedClusterName(instance.Name), Namespace: instance.Namespace}
	if err := c.Get(ctx, hcKey, &hyperv1beta1.HostedCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("HostedCluster exists before endpoint status was written; get error = %v", err)
	}
	certKey := client.ObjectKey{Name: resources.KASServingCertName(instance.Name), Namespace: instance.Namespace}
	if err := c.Get(ctx, certKey, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("KAS serving certificate exists before endpoint status was written; get error = %v", err)
	}
	routeKey := client.ObjectKey{Name: resources.HostedClusterAPIRouteName(instance.Name), Namespace: resources.HostedControlPlaneNamespace(instance.Namespace, instance.Name)}
	if err := c.Get(ctx, routeKey, &routev1.Route{}); !apierrors.IsNotFound(err) {
		t.Fatalf("API Route exists before endpoint status was written; get error = %v", err)
	}

	// A management ingress change after selection must not change the endpoint.
	ingress.Spec.Domain = "apps.changed.test"
	if err := c.Update(ctx, ingress); err != nil {
		t.Fatalf("updating ingress config: %v", err)
	}
	if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
		t.Fatalf("second reconcileHyperShift: %v", err)
	}
	hc := &hyperv1beta1.HostedCluster{}
	if err := c.Get(ctx, hcKey, hc); err != nil {
		t.Fatalf("getting created HostedCluster: %v", err)
	}
	if namedCertHostname(hc) != hostname {
		t.Fatalf("HostedCluster named certificate hostname = %q, want %q", namedCertHostname(hc), hostname)
	}
	certSecret := &corev1.Secret{}
	if err := c.Get(ctx, certKey, certSecret); err != nil {
		t.Fatalf("getting KAS serving certificate: %v", err)
	}
	if got := certDNSHostname(t, certSecret.Data[corev1.TLSCertKey]); got != hostname {
		t.Fatalf("KAS serving certificate SAN = %q, want %q", got, hostname)
	}
	return hc, hostname, routeKey
}

func TestHCPAPIEndpointRecoversFromLegacyResourcesWithoutStatus(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("hcp-legacy", "tenant-one")
	const oldDomain = "apps.legacy.test"
	const currentDomain = "apps.current.test"
	legacyHostname := resources.APIServerHostname(instance.Name, oldDomain)
	certPEM, keyPEM, err := resources.GenerateAPIServerServingCert(legacyHostname)
	if err != nil {
		t.Fatalf("generating legacy serving certificate: %v", err)
	}
	certSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resources.KASServingCertName(instance.Name),
			Namespace: resources.DefaultHostedClusterNamespace,
			Labels:    resources.CommonLabels(instance),
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
	}
	hc := resources.BuildHostedCluster(instance, resources.HostedClusterOptions{
		Namespace: resources.DefaultHostedClusterNamespace, PullSecretName: statusCRCPullSecret,
		NodePortAddress: testManagementNodeIP, ServingCertName: certSecret.Name, ServingCertHostname: legacyHostname,
	})
	ingress := &configv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: statusIngressName},
		Spec:       configv1.IngressSpec{Domain: currentDomain},
	}
	c := newHyperShiftFakeClient(t, instance, certSecret, hc, ingress)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	if _, err := r.ensureHyperShiftBacking(ctx, instance, statusCRCPullSecret); err != nil {
		t.Fatalf("recovering endpoint: %v", err)
	}
	if got, want := instance.Status.APIEndpoint, "https://"+legacyHostname; got != want {
		t.Fatalf("recovered APIEndpoint = %q, want %q", got, want)
	}
	storedCert := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(certSecret), storedCert); err != nil {
		t.Fatalf("getting legacy serving certificate: %v", err)
	}
	if !bytes.Equal(storedCert.Data[corev1.TLSCertKey], certPEM) {
		t.Fatal("endpoint recovery replaced the existing KAS serving certificate")
	}
	if namedCertHostname(hc) != legacyHostname {
		t.Fatalf("legacy HostedCluster hostname = %q, want %q", namedCertHostname(hc), legacyHostname)
	}
}

func TestReadyHCPRecoversEndpointBeforeRestoringMissingRoute(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("hcp-ready-recovery", "tenant-one")
	instance.Finalizers = []string{instanceFinalizer}
	instance.Status.Phase = brokerv1alpha1.PhaseReady
	instance.Status.HyperShift = &brokerv1alpha1.HyperShiftBackingStatus{
		HostedClusterName: resources.HostedClusterName(instance.Name), HostedClusterNamespace: resources.DefaultHostedClusterNamespace,
		NodePoolNames: []string{resources.NodePoolName(instance.Name)},
	}
	legacyHostname := resources.APIServerHostname(instance.Name, "apps.legacy.test")
	certPEM, keyPEM, err := resources.GenerateAPIServerServingCert(legacyHostname)
	if err != nil {
		t.Fatalf("generating legacy serving certificate: %v", err)
	}
	certSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: resources.KASServingCertName(instance.Name), Namespace: resources.DefaultHostedClusterNamespace,
			Labels: resources.CommonLabels(instance),
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
	}
	hc := resources.BuildHostedCluster(instance, resources.HostedClusterOptions{
		Namespace: resources.DefaultHostedClusterNamespace, PullSecretName: statusCRCPullSecret,
		NodePortAddress: testManagementNodeIP, ServingCertName: certSecret.Name, ServingCertHostname: legacyHostname,
	})
	c := newHyperShiftFakeClient(t, instance, certSecret, hc)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	result, err := r.reconcileReadyHyperShift(ctx, instance)
	if err != nil {
		t.Fatalf("recovering Ready HCP endpoint: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("RequeueAfter after persisting endpoint = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	if got, want := instance.Status.APIEndpoint, "https://"+legacyHostname; got != want {
		t.Fatalf("recovered APIEndpoint = %q, want %q", got, want)
	}
	routeKey := client.ObjectKey{
		Name:      resources.HostedClusterAPIRouteName(instance.Name),
		Namespace: resources.HostedControlPlaneNamespace(resources.DefaultHostedClusterNamespace, instance.Name),
	}
	if err := c.Get(ctx, routeKey, &routev1.Route{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Route was created before endpoint status was saved; get error = %v", err)
	}

	if _, err := r.reconcileReadyHyperShift(ctx, instance); err != nil {
		t.Fatalf("restoring missing API Route: %v", err)
	}
	restoredRoute := &routev1.Route{}
	if err := c.Get(ctx, routeKey, restoredRoute); err != nil {
		t.Fatalf("getting restored API Route: %v", err)
	}
	if restoredRoute.Spec.Host != legacyHostname {
		t.Fatalf("restored Route host = %q, want preserved legacy hostname %q", restoredRoute.Spec.Host, legacyHostname)
	}
}

func TestReadyHCPRouteDeletionTriggersRecovery(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("hcp-route-watch", "tenant-one")
	instance.Finalizers = []string{instanceFinalizer}
	instance.Status.Phase = brokerv1alpha1.PhaseReady
	instance.Status.APIEndpoint = "https://" + resources.APIServerHostname(instance.Name, "apps.legacy.test")
	instance.Status.HyperShift = &brokerv1alpha1.HyperShiftBackingStatus{
		HostedClusterName: resources.HostedClusterName(instance.Name), HostedClusterNamespace: resources.DefaultHostedClusterNamespace,
		NodePoolNames: []string{resources.NodePoolName(instance.Name)},
	}
	hostname, err := apiEndpointHostname(instance.Status.APIEndpoint)
	if err != nil {
		t.Fatalf("parsing recorded endpoint: %v", err)
	}
	certPEM, keyPEM, err := resources.GenerateAPIServerServingCert(hostname)
	if err != nil {
		t.Fatalf("generating serving certificate: %v", err)
	}
	certSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: resources.KASServingCertName(instance.Name), Namespace: resources.DefaultHostedClusterNamespace,
			Labels: resources.CommonLabels(instance),
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
	}
	hc := resources.BuildHostedCluster(instance, resources.HostedClusterOptions{
		Namespace: resources.DefaultHostedClusterNamespace, PullSecretName: statusCRCPullSecret,
		NodePortAddress: testManagementNodeIP, ServingCertName: certSecret.Name, ServingCertHostname: hostname,
	})
	route := resources.BuildHostedClusterAPIRoute(instance, hostname, resources.HostedControlPlaneNamespace(resources.DefaultHostedClusterNamespace, instance.Name))
	route.Status.Ingress = []routev1.RouteIngress{{
		Host:       hostname,
		Conditions: []routev1.RouteIngressCondition{{Type: routev1.RouteAdmitted, Status: corev1.ConditionTrue}},
	}}
	c := newHyperShiftFakeClient(t, instance, certSecret, hc, route)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}

	// Legacy Routes do not carry the source namespace label. They must still
	// map to the correct instance during upgrade recovery.
	legacyRouteEvent := route.DeepCopy()
	delete(legacyRouteEvent.Labels, resources.LabelInstanceNamespace)
	legacyRequests := r.instanceForRoute(ctx, legacyRouteEvent)
	if len(legacyRequests) != 1 || legacyRequests[0].NamespacedName != client.ObjectKeyFromObject(instance) {
		t.Fatalf("legacy Route requests = %v, want instance %s/%s", legacyRequests, instance.Namespace, instance.Name)
	}

	if err := c.Delete(ctx, route); err != nil {
		t.Fatalf("deleting admitted Route: %v", err)
	}
	requests := r.instanceForRoute(ctx, route)
	if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(instance) {
		t.Fatalf("deleted Route requests = %v, want instance %s/%s", requests, instance.Namespace, instance.Name)
	}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: requests[0].NamespacedName})
	if err != nil {
		t.Fatalf("reconciling deleted Route: %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, requeueInterval)
	}
	current := &brokerv1alpha1.ClusterInstance{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), current); err != nil {
		t.Fatalf("getting instance after Route recovery: %v", err)
	}
	if current.Status.Phase != brokerv1alpha1.PhaseProvisioning {
		t.Fatalf("phase = %q, want Provisioning while restored Route awaits admission", current.Status.Phase)
	}
	if current.Status.APIEndpoint != "https://"+hostname {
		t.Fatalf("APIEndpoint = %q, want preserved https://%s", current.Status.APIEndpoint, hostname)
	}
	readyCondition := apimeta.FindStatusCondition(current.Status.Conditions, conditionTypeReady)
	if readyCondition == nil || readyCondition.Reason != "APIEndpointRouteNotAdmitted" {
		t.Fatalf("Ready condition = %+v, want APIEndpointRouteNotAdmitted", readyCondition)
	}
	restoredRoute := &routev1.Route{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(route), restoredRoute); err != nil {
		t.Fatalf("getting restored Route: %v", err)
	}
	if restoredRoute.Spec.Host != hostname {
		t.Fatalf("restored Route host = %q, want %q", restoredRoute.Spec.Host, hostname)
	}
}

func TestHCPAPIEndpointReportsExistingRouteHostnameConflict(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("hcp-conflict", "tenant-one")
	const domain = statusIngressDomain
	hostname, err := resources.APIHostname(instance.Name, instance.Namespace, domain)
	if err != nil {
		t.Fatalf("building expected API hostname: %v", err)
	}
	ingress := &configv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: statusIngressName}, Spec: configv1.IngressSpec{Domain: domain}}
	conflictingRoute := &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{Name: "other-api", Namespace: "other-namespace", Labels: map[string]string{resources.LabelManagedBy: resources.ManagerName, resources.LabelInstance: "other"}},
		Spec:       routev1.RouteSpec{Host: hostname},
	}
	c := newHyperShiftFakeClient(t, instance, hcpEndpointTestPullSecret(instance), ingress, conflictingRoute)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.recordHCPPlacement(ctx, instance); err != nil {
		t.Fatal(err)
	}

	_, err = r.reconcileHyperShift(ctx, instance)
	var conflict apiEndpointConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("ensureHyperShiftBacking error = %v, want API endpoint conflict", err)
	}
	if !strings.Contains(err.Error(), "other-namespace/other-api") {
		t.Fatalf("conflict error = %q, want conflicting Route identity", err)
	}
	if instance.Status.APIEndpoint != "" {
		t.Fatalf("conflicting endpoint was recorded as %q", instance.Status.APIEndpoint)
	}
	if instance.Status.Phase != brokerv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed for an endpoint conflict", instance.Status.Phase)
	}
	readyCondition := apimeta.FindStatusCondition(instance.Status.Conditions, conditionTypeReady)
	if readyCondition == nil || readyCondition.Reason != "APIEndpointConflict" {
		t.Fatalf("Ready condition = %+v, want reason APIEndpointConflict", readyCondition)
	}
}

func TestSameNamedHCPInstancesInDifferentNamespacesGetDifferentEndpoints(t *testing.T) {
	ctx := context.Background()
	first := hcpEndpointTestInstance("hcp-shared-name", "tenant-one")
	second := hcpEndpointTestInstance("hcp-shared-name", "tenant-two")
	ingress := &configv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: statusIngressName},
		Spec:       configv1.IngressSpec{Domain: statusIngressDomain},
	}
	c := newHyperShiftFakeClient(t, first, second, ingress)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	firstHostname, persist, err := r.resolveHCPAPIHostname(ctx, first, &hyperv1beta1.HostedCluster{}, false, resources.HostedControlPlaneNamespace(resources.DefaultHostedClusterNamespace, first.Name))
	if err != nil {
		t.Fatalf("resolving first HCP endpoint: %v", err)
	}
	if !persist {
		t.Fatal("first HCP endpoint was not marked for status persistence")
	}
	secondHostname, persist, err := r.resolveHCPAPIHostname(ctx, second, &hyperv1beta1.HostedCluster{}, false, resources.HostedControlPlaneNamespace(resources.DefaultHostedClusterNamespace, second.Name))
	if err != nil {
		t.Fatalf("resolving second HCP endpoint: %v", err)
	}
	if !persist {
		t.Fatal("second HCP endpoint was not marked for status persistence")
	}
	if firstHostname == secondHostname {
		t.Fatalf("same-named HCP instances share endpoint %q", firstHostname)
	}
}

func hcpEndpointTestInstance(name, namespace string) *brokerv1alpha1.ClusterInstance {
	return &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type: brokerv1alpha1.TopologyHCP,
			Template: brokerv1alpha1.ClusterTemplate{
				OCPVersion: testOCPVersion, ReleaseImage: statusHCPReleaseImage,
				Memory: statusHCPMemory, Cores: 2, PullSecretRef: corev1.LocalObjectReference{Name: statusCRCPullSecret},
			},
		},
	}
}

func hcpEndpointTestPullSecret(instance *brokerv1alpha1.ClusterInstance) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: statusCRCPullSecret, Namespace: instance.Namespace},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{resources.PullSecretDataKey: []byte(`{"auths":{"quay.io":{"auth":"test"}}}`)},
	}
}

func namedCertHostname(hc *hyperv1beta1.HostedCluster) string {
	if hc.Spec.Configuration == nil || hc.Spec.Configuration.APIServer == nil {
		return ""
	}
	certs := hc.Spec.Configuration.APIServer.ServingCerts.NamedCertificates
	if len(certs) != 1 || len(certs[0].Names) != 1 {
		return ""
	}
	return certs[0].Names[0]
}

func certDNSHostname(t *testing.T, certPEM []byte) string {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("serving certificate has invalid PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing serving certificate: %v", err)
	}
	if len(cert.DNSNames) != 1 {
		t.Fatalf("serving certificate DNS SANs = %v, want one", cert.DNSNames)
	}
	return cert.DNSNames[0]
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
	if err := routev1.AddToScheme(s); err != nil {
		t.Fatalf("adding OpenShift Route scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&brokerv1alpha1.ClusterInstance{}).
		WithObjects(objects...).Build()
}
