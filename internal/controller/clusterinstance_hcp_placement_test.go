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
	"errors"
	"strings"
	"testing"
	"time"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type unavailableHCPReader struct{ client.Reader }

func (unavailableHCPReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("temporary API read failure")
}

type failOnceHCPStatusClient struct {
	client.Client
	fail bool
}

func (c *failOnceHCPStatusClient) Status() client.SubResourceWriter {
	return &failOnceHCPStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type failOnceHCPStatusWriter struct {
	client.SubResourceWriter
	client *failOnceHCPStatusClient
}

func (w *failOnceHCPStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if w.client.fail {
		w.client.fail = false
		return errors.New("temporary status write failure")
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestSameNamedHCPLifecycleIsIndependent(t *testing.T) {
	ctx := context.Background()
	first := hcpEndpointTestInstance("shared-hcp", "tenant-one")
	second := hcpEndpointTestInstance(first.Name, "tenant-two")
	first.UID, second.UID = "first-instance", "second-instance"
	ingress := &configv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: statusIngressName}, Spec: configv1.IngressSpec{Domain: statusIngressDomain}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testManagementNodeName}, Status: corev1.NodeStatus{
		Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: testManagementNodeIP}},
	}}
	c := newHyperShiftFakeClient(t, first, second, hcpEndpointTestPullSecret(first), hcpEndpointTestPullSecret(second), ingress, node)
	if err := kubevirtv1.AddToScheme(c.Scheme()); err != nil {
		t.Fatal(err)
	}
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	// Finish the first instance before the second starts. This catches adoption
	// of the first instance's already-existing certificate, HostedCluster or Route.
	provisionHCPForPlacementTest(t, r, first)
	provisionHCPForPlacementTest(t, r, second)
	if first.Status.APIEndpoint == second.Status.APIEndpoint {
		t.Fatalf("instances share endpoint %q", first.Status.APIEndpoint)
	}
	crc := hcpEndpointTestInstance(first.Name, "tenant-crc")
	crc.Spec.Type = brokerv1alpha1.TopologyCRC
	if err := c.Create(ctx, crc); err != nil {
		t.Fatal(err)
	}
	crcHostname, err := r.ensureCRCAPIRoute(ctx, crc)
	if err != nil || "https://"+crcHostname == first.Status.APIEndpoint || "https://"+crcHostname == second.Status.APIEndpoint {
		t.Fatalf("mixed-topology endpoint is not independent: %q, %v", crcHostname, err)
	}
	before := hcpPublishedConfig(t, c, second)
	ingress.Spec.Domain = "apps.placement.test"
	if err := c.Update(ctx, ingress); err != nil {
		t.Fatal(err)
	}
	for _, instance := range []*brokerv1alpha1.ClusterInstance{first, second} {
		if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
			t.Fatal(err)
		}
	}
	// Delete the first backing cluster and its generated Secrets. User inputs
	// and every resource for the other source namespace must survive.
	for range 2 {
		if _, err := r.teardownHyperShiftBacking(ctx, first); err != nil {
			t.Fatal(err)
		}
	}
	for _, obj := range []client.Object{
		&hyperv1beta1.HostedCluster{ObjectMeta: metav1.ObjectMeta{Name: second.Name, Namespace: second.Namespace}},
		&hyperv1beta1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: resources.NodePoolName(second.Name), Namespace: second.Namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KASServingCertName(second.Name), Namespace: second.Namespace}},
		hcpEndpointTestPullSecret(first), hcpEndpointTestPullSecret(second),
	} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Fatalf("resource %T %s did not survive: %v", obj, client.ObjectKeyFromObject(obj), err)
		}
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: first.Namespace, Name: first.Name}, &hyperv1beta1.HostedCluster{}); !apierrors.IsNotFound(err) {
		t.Fatalf("first HostedCluster was not deleted: %v", err)
	}
	if _, err := r.reconcileHyperShift(ctx, second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, hcpPublishedConfig(t, c, second)) {
		t.Fatal("reconcile or deletion changed the other instance's credentials")
	}
}

func provisionHCPForPlacementTest(t *testing.T, r *ClusterInstanceReconciler, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	ctx := context.Background()
	if r.GuestAPIReadinessCheck == nil {
		r.GuestAPIReadinessCheck = func(context.Context, []byte) error { return nil }
	}
	for range 4 {
		if err := r.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
			t.Fatal(err)
		}
		if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
			t.Fatalf("provisioning %s: %v", client.ObjectKeyFromObject(instance), err)
		}
		assignFakeHostedClusterUID(t, r, instance)
	}
	if instance.Status.HyperShift.HostedClusterNamespace != instance.Namespace {
		t.Fatalf("unexpected placement: %+v", instance.Status.HyperShift)
	}
	hc := &hyperv1beta1.HostedCluster{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: instance.Name}, hc); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(hc, instance) || hc.Spec.PullSecret.Name != instance.Spec.Template.PullSecretRef.Name {
		t.Fatal("HostedCluster does not use its local owner and input Secret")
	}
	hc.Status.Conditions = []metav1.Condition{{Type: string(hyperv1beta1.HostedClusterAvailable), Status: metav1.ConditionTrue}}
	hc.Status.KubeConfig = &corev1.LocalObjectReference{Name: resources.AdminKubeconfigSecretName(instance.Name)}
	if err := r.Update(ctx, hc); err != nil {
		t.Fatal(err)
	}
	np := &hyperv1beta1.NodePool{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: resources.NodePoolName(instance.Name)}, np); err != nil {
		t.Fatal(err)
	}
	var hostedClusterOwner *metav1.OwnerReference
	for i := range np.OwnerReferences {
		if np.OwnerReferences[i].Kind == "HostedCluster" && np.OwnerReferences[i].Name == hc.Name {
			hostedClusterOwner = &np.OwnerReferences[i]
		}
	}
	if hostedClusterOwner == nil || hostedClusterOwner.UID != hc.UID {
		t.Fatal("NodePool does not retain its HostedCluster owner")
	}
	if hostedClusterOwner.BlockOwnerDeletion == nil || *hostedClusterOwner.BlockOwnerDeletion {
		t.Fatalf("NodePool HostedCluster owner must be non-blocking: %+v", hostedClusterOwner)
	}
	var instanceOwner *metav1.OwnerReference
	for i := range np.OwnerReferences {
		if np.OwnerReferences[i].Kind == clusterInstanceKind {
			instanceOwner = &np.OwnerReferences[i]
		}
	}
	if instanceOwner == nil || instanceOwner.BlockOwnerDeletion == nil || *instanceOwner.BlockOwnerDeletion {
		t.Fatalf("NodePool ClusterInstance owner must not block deletion: %+v", instanceOwner)
	}
	np.Status.Replicas = 1
	if err := r.Update(ctx, np); err != nil {
		t.Fatal(err)
	}
	kubeconfig, err := clientcmd.Write(clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"placement-guest": {Server: "https://192.0.2.10:30443"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: hc.Status.KubeConfig.Name, Namespace: hc.Namespace}, Data: map[string][]byte{resources.KubeconfigSecretKey: kubeconfig}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
		t.Fatal(err)
	}
	route := &routev1.Route{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: resources.HostedControlPlaneNamespace(hc.Namespace, hc.Name), Name: resources.HostedClusterAPIRouteName(instance.Name)}, route); err != nil {
		t.Fatal(err)
	}
	route.Status.Ingress = []routev1.RouteIngress{{Host: route.Spec.Host, Conditions: []routev1.RouteIngressCondition{{Type: routev1.RouteAdmitted, Status: corev1.ConditionTrue}}}}
	if err := r.Update(ctx, route); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileHyperShift(ctx, instance); err != nil {
		t.Fatal(err)
	}
	verifyProvisionedHCP(t, r.Client, instance, hc, route)
}

func assignFakeHostedClusterUID(t *testing.T, r *ClusterInstanceReconciler, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	ctx := context.Background()
	hc := &hyperv1beta1.HostedCluster{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: instance.Name}, hc); err != nil || hc.UID != "" {
		return
	}
	hc.UID = types.UID("hosted-cluster-" + instance.Namespace + "-" + instance.Name)
	if err := r.Update(ctx, hc); err != nil {
		t.Fatal(err)
	}
}

func verifyProvisionedHCP(t *testing.T, c client.Client, instance *brokerv1alpha1.ClusterInstance, hc *hyperv1beta1.HostedCluster, route *routev1.Route) {
	t.Helper()
	if instance.Status.Phase != brokerv1alpha1.PhaseReady || instance.Status.APIEndpoint != "https://"+route.Spec.Host || namedCertHostname(hc) != route.Spec.Host {
		t.Fatal("HCP status, named certificate and Route do not agree")
	}
	cert := &corev1.Secret{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: hc.Namespace, Name: resources.KASServingCertName(instance.Name)}, cert); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(cert, instance) || certDNSHostname(t, cert.Data[corev1.TLSCertKey]) != route.Spec.Host {
		t.Fatal("certificate owner or SAN is incorrect")
	}
	cfg, err := clientcmd.Load(hcpPublishedConfig(t, c, instance))
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range cfg.Clusters {
		if cluster.Server != instance.Status.APIEndpoint || !bytes.Equal(cluster.CertificateAuthorityData, cert.Data[corev1.TLSCertKey]) {
			t.Fatal("published kubeconfig does not match the endpoint certificate")
		}
	}
}

func hcpPublishedConfig(t *testing.T, c client.Client, instance *brokerv1alpha1.ClusterInstance) []byte {
	t.Helper()
	secret := &corev1.Secret{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: instance.Namespace, Name: resources.KubeconfigSecretName(instance.Name)}, secret); err != nil {
		t.Fatal(err)
	}
	return secret.Data[resources.KubeconfigSecretKey]
}

func TestHCPPlacementRejectsInvalidControlPlaneNamespace(t *testing.T) {
	instance := hcpEndpointTestInstance(strings.Repeat("a", 40), strings.Repeat("n", 40))
	c := newHyperShiftFakeClient(t, instance)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.recordHCPPlacement(context.Background(), instance); err == nil {
		t.Fatal("accepted a control-plane namespace longer than 63 characters")
	}
}

func TestHCPPlacementRecoversLegacyLocation(t *testing.T) {
	instance := hcpEndpointTestInstance("legacy-placement", "tenant-one")
	instance.UID = types.UID("legacy-instance")
	hc := resources.BuildHostedCluster(instance, resources.HostedClusterOptions{Namespace: resources.DefaultHostedClusterNamespace})
	c := newHyperShiftFakeClient(t, instance, hc)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.recordHCPPlacement(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.HyperShift.HostedClusterNamespace != resources.DefaultHostedClusterNamespace {
		t.Fatal("legacy HostedCluster was relocated")
	}
}

func TestReadyHCPRetriesTemporaryPlacementFailures(t *testing.T) {
	for _, phase := range []brokerv1alpha1.ClusterInstancePhase{brokerv1alpha1.PhaseReady, brokerv1alpha1.PhaseProvisioning} {
		for _, failure := range []string{"read", "status write"} {
			t.Run(string(phase)+"/"+failure, func(t *testing.T) {
				ctx := context.Background()
				instance := hcpEndpointTestInstance("retry-legacy", "tenant-one")
				instance.Status.Phase = phase
				hc := resources.BuildHostedCluster(instance, resources.HostedClusterOptions{Namespace: resources.DefaultHostedClusterNamespace})
				base := newHyperShiftFakeClient(t, instance, hc)
				writer := &failOnceHCPStatusClient{Client: base, fail: failure == "status write"}
				r := &ClusterInstanceReconciler{Client: writer, Scheme: base.Scheme()}
				if failure == "read" {
					r.APIReader = unavailableHCPReader{Reader: base}
				}
				reconcile := r.reconcileHyperShift
				if phase == brokerv1alpha1.PhaseReady {
					reconcile = r.reconcileReadyHyperShift
				}
				if _, err := reconcile(ctx, instance); err == nil {
					t.Fatal("expected a retryable placement error")
				}
				stored := &brokerv1alpha1.ClusterInstance{}
				if err := base.Get(ctx, client.ObjectKeyFromObject(instance), stored); err != nil {
					t.Fatal(err)
				}
				if stored.Status.Phase != phase || stored.Status.HyperShift != nil {
					t.Fatalf("temporary failure changed instance status: %+v", stored.Status)
				}
				r.APIReader = base
				if result, err := reconcile(ctx, stored); err != nil || result.RequeueAfter != requeueInterval {
					t.Fatalf("placement retry = %+v, %v", result, err)
				}
				if stored.Status.HyperShift == nil || stored.Status.HyperShift.HostedClusterNamespace != hc.Namespace {
					t.Fatalf("legacy placement was not recorded on retry: %+v", stored.Status.HyperShift)
				}
			})
		}
	}
}

func TestHCPPlacementRecoversLegacySourceInClusters(t *testing.T) {
	instance := hcpEndpointTestInstance("legacy-source", resources.DefaultHostedClusterNamespace)
	hc := resources.BuildHostedCluster(instance, resources.HostedClusterOptions{Namespace: instance.Namespace})
	c := newHyperShiftFakeClient(t, instance, hc)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.recordHCPPlacement(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.HyperShift.HostedClusterNamespace != instance.Namespace {
		t.Fatal("legacy HostedCluster in the source namespace was not recovered")
	}
}

func TestHCPPlacementPreservesPartialLegacyStatus(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("partial-legacy", "tenant-one")
	instance.Status.HyperShift = &brokerv1alpha1.HyperShiftBackingStatus{
		HostedClusterName: instance.Name, NodePoolNames: []string{"custom-workers"},
	}
	instance.Status.APIEndpoint = "https://api.partial-legacy.apps.old.test"
	hc := resources.BuildHostedCluster(instance, resources.HostedClusterOptions{Namespace: resources.DefaultHostedClusterNamespace})
	c := newHyperShiftFakeClient(t, instance, hc)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.recordHCPPlacement(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.HyperShift.HostedClusterNamespace != resources.DefaultHostedClusterNamespace ||
		instance.Status.HyperShift.NodePoolNames[0] != "custom-workers" ||
		instance.Status.APIEndpoint != "https://api.partial-legacy.apps.old.test" {
		t.Fatalf("partial legacy status changed: %+v", instance.Status)
	}
}

func TestHCPPlacementRecoversLegacySecretAfterHostedClusterDeletion(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("orphaned-cluster", "tenant-one")
	cert := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: resources.KASServingCertName(instance.Name), Namespace: resources.DefaultHostedClusterNamespace,
		Labels: resources.CommonLabels(instance),
	}}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: resources.HostedControlPlaneNamespace(cert.Namespace, instance.Name),
	}}
	c := newHyperShiftFakeClient(t, instance, cert, ns)
	if err := kubevirtv1.AddToScheme(c.Scheme()); err != nil {
		t.Fatal(err)
	}
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.recordHCPPlacement(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.HyperShift.HostedClusterNamespace != resources.DefaultHostedClusterNamespace {
		t.Fatal("lost the legacy location after HostedCluster deletion")
	}
	if _, err := r.teardownHyperShiftBacking(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(cert), cert); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy certificate was not cleaned up: %v", err)
	}
}

func TestHCPPlacementRejectsUnclaimedControlPlaneNamespace(t *testing.T) {
	instance := hcpEndpointTestInstance("existing-namespace", "tenant-one")
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: resources.HostedControlPlaneNamespace(instance.Namespace, instance.Name)}}
	c := newHyperShiftFakeClient(t, instance, ns)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.recordHCPPlacement(context.Background(), instance); err == nil {
		t.Fatal("claimed an existing control-plane namespace without a HostedCluster")
	}
}

func TestRecordedLegacyHCPAndSameNamedLocalInstance(t *testing.T) {
	ctx := context.Background()
	legacy := hcpEndpointTestInstance("shared-upgrade", "old-tenant")
	local := hcpEndpointTestInstance(legacy.Name, "new-tenant")
	setLegacyHCPTestPlacement(legacy)
	hostname := resources.APIServerHostname(legacy.Name, "apps.old.test")
	legacy.Status.APIEndpoint = "https://" + hostname
	certPEM, keyPEM, err := resources.GenerateAPIServerServingCert(hostname)
	if err != nil {
		t.Fatal(err)
	}
	cert := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: resources.KASServingCertName(legacy.Name), Namespace: resources.DefaultHostedClusterNamespace,
		Labels: resources.CommonLabels(legacy),
	}, Data: map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM}}
	hc := resources.BuildHostedCluster(legacy, resources.HostedClusterOptions{
		Namespace: cert.Namespace, ServingCertName: cert.Name, ServingCertHostname: hostname,
		NodePortAddress: testManagementNodeIP,
	})
	route := resources.BuildHostedClusterAPIRoute(legacy, hostname, resources.HostedControlPlaneNamespace(hc.Namespace, hc.Name))
	delete(route.Labels, resources.LabelInstanceNamespace)
	objs := []client.Object{legacy, local, hc, cert, route, hcpEndpointTestPullSecret(legacy), hcpEndpointTestPullSecret(local),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: route.Namespace}},
		&configv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: statusIngressName}, Spec: configv1.IngressSpec{Domain: statusIngressDomain}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testManagementNodeName}, Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: testManagementNodeIP}},
		}},
	}
	c := newHyperShiftFakeClient(t, objs...)
	if err := kubevirtv1.AddToScheme(c.Scheme()); err != nil {
		t.Fatal(err)
	}
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	provisionHCPForPlacementTest(t, r, local)
	if _, err := r.reconcileHyperShift(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := r.teardownHyperShiftBacking(ctx, local); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(cert), cert); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(certPEM, cert.Data[corev1.TLSCertKey]) || legacy.Status.APIEndpoint != "https://"+hostname || legacy.Status.HyperShift.HostedClusterNamespace != hc.Namespace {
		t.Fatal("new instance changed the legacy location or endpoint identity")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(hc), hc); err != nil {
		t.Fatal(err)
	}
}

func TestHCPPlacementRejectsNamespaceCollisions(t *testing.T) {
	for _, name := range []string{"a-b", "a.b"} {
		t.Run(name, func(t *testing.T) {
			instance := hcpEndpointTestInstance(name, "tenant")
			other := hcpEndpointTestInstance("b", "tenant-a")
			other.Status.HyperShift = &brokerv1alpha1.HyperShiftBackingStatus{HostedClusterNamespace: other.Namespace, HostedClusterName: other.Name}
			c := newHyperShiftFakeClient(t, instance, other)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			if _, err := r.recordHCPPlacement(context.Background(), instance); err == nil {
				t.Fatal("accepted a colliding HyperShift control-plane namespace")
			}
		})
	}
}

func TestHCPPlacementRejectsAmbiguousLegacyOwnership(t *testing.T) {
	first := hcpEndpointTestInstance("ambiguous-legacy", "tenant-one")
	second := hcpEndpointTestInstance(first.Name, "tenant-two")
	hc := resources.BuildHostedCluster(first, resources.HostedClusterOptions{Namespace: resources.DefaultHostedClusterNamespace})
	c := newHyperShiftFakeClient(t, first, second, hc)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	for _, instance := range []*brokerv1alpha1.ClusterInstance{first, second} {
		if _, err := r.recordHCPPlacement(context.Background(), instance); err == nil {
			t.Fatal("adopted legacy resources without a unique source identity")
		}
	}
}

func TestHCPWorkerSSHInputRemainsLocalAndUserOwned(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("local-ssh", "tenant-one")
	instance.Spec.Template.HCPWorkerSSHKeyRef = &corev1.LocalObjectReference{Name: resources.HCPWorkerSSHKeyName(instance.Name)}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: instance.Spec.Template.HCPWorkerSSHKeyRef.Name, Namespace: instance.Namespace},
		Data: map[string][]byte{resources.HCPWorkerSSHKeyDataKey: []byte("ssh-ed25519 test-key")}}
	c := newHyperShiftFakeClient(t, instance, secret)
	if err := kubevirtv1.AddToScheme(c.Scheme()); err != nil {
		t.Fatal(err)
	}
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.recordHCPPlacement(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if name, err := r.resolveHCPWorkerSSHKey(ctx, instance, instance.Namespace); err != nil || name != secret.Name {
		t.Fatalf("local SSH key resolution = %q, %v", name, err)
	}
	if _, err := r.teardownHyperShiftBacking(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(secret), secret); err != nil {
		t.Fatal(err)
	}
	if len(secret.OwnerReferences) != 0 {
		t.Fatal("added an instance owner to a user input")
	}
}

func TestHCPRefusesForeignLocalResources(t *testing.T) {
	for _, kind := range []string{"HostedCluster", "TLS Secret", "labelled HostedCluster", "labelled TLS Secret"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			instance := hcpEndpointTestInstance("foreign-resource", "tenant-one")
			instance.Status.HyperShift = &brokerv1alpha1.HyperShiftBackingStatus{HostedClusterNamespace: instance.Namespace, HostedClusterName: instance.Name}
			var foreign client.Object = &hyperv1beta1.HostedCluster{ObjectMeta: metav1.ObjectMeta{Name: instance.Name, Namespace: instance.Namespace}}
			if strings.Contains(kind, "TLS Secret") {
				foreign = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: resources.KASServingCertName(instance.Name), Namespace: instance.Namespace}}
			}
			if strings.HasPrefix(kind, "labelled") {
				foreign.SetLabels(resources.APIEndpointLabels(instance))
			}
			c := newHyperShiftFakeClient(t, instance, foreign)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			if _, err := r.ensureHyperShiftBacking(ctx, instance, statusCRCPullSecret); err == nil {
				t.Fatal("reused a foreign resource")
			}
			if _, err := r.teardownHyperShiftBacking(ctx, instance); err == nil {
				t.Fatal("deleted a foreign resource")
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(foreign), foreign); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHCPRefusesNodePoolWithWrongHostedClusterOwner(t *testing.T) {
	ctx := context.Background()
	instance := hcpEndpointTestInstance("nodepool-owner", "tenant-one")
	instance.UID = "instance-uid"
	instance.Status.HyperShift = &brokerv1alpha1.HyperShiftBackingStatus{
		HostedClusterNamespace: instance.Namespace, HostedClusterName: instance.Name,
	}
	c := newHyperShiftFakeClient(t, instance)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	np := resources.BuildNodePool(instance, instance.Name, instance.Namespace, 1)
	foreign := &hyperv1beta1.HostedCluster{ObjectMeta: metav1.ObjectMeta{Name: "another-cluster", Namespace: instance.Namespace, UID: "foreign-uid"}}
	if err := controllerutil.SetControllerReference(foreign, np, c.Scheme()); err != nil {
		t.Fatal(err)
	}
	if err := r.setHCPResourceOwner(instance, np); err != nil {
		t.Fatal(err)
	}
	if err := r.verifyHCPResource(ctx, instance, np); err == nil {
		t.Fatal("accepted a NodePool controlled by another HostedCluster")
	}
}

func TestHCPPublishedKubeconfigOwnership(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "conflicting"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			instance := hcpEndpointTestInstance("kubeconfig-owner", "tenant-one")
			instance.UID = "instance-uid"
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: resources.KubeconfigSecretName(instance.Name), Namespace: instance.Namespace,
				Labels: resources.CommonLabels(instance),
			}}
			if legacy {
				instance.Status.Phase = brokerv1alpha1.PhaseReady
				instance.Status.APIEndpoint = "https://api.old.test"
				instance.Status.KubeconfigSecretRef.Name = secret.Name
			}
			c := newHyperShiftFakeClient(t, instance, secret)
			r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
			_, err := r.markReady(ctx, instance, "4.19.0", "https://api.old.test", []byte("kubeconfig"))
			if !legacy && err == nil {
				t.Fatal("overwrote an unclaimed kubeconfig Secret")
			}
			if legacy && err != nil {
				t.Fatal(err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(secret), secret); err != nil {
				t.Fatal(err)
			}
			if legacy && !metav1.IsControlledBy(secret, instance) {
				t.Fatal("did not backfill the legacy kubeconfig owner")
			}
			if !legacy && len(secret.Data) != 0 {
				t.Fatal("changed the unclaimed kubeconfig Secret")
			}
		})
	}
}

func TestHCPUnrecordedCleanupDoesNotIgnoreLegacyNamespace(t *testing.T) {
	instance := hcpEndpointTestInstance("orphan-placement", "tenant-one")
	instance.DeletionTimestamp = timePointer(time.Now())
	instance.Finalizers = []string{instanceFinalizer}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: resources.HostedControlPlaneNamespace(resources.DefaultHostedClusterNamespace, instance.Name)}}
	c := newHyperShiftFakeClient(t, instance, namespace)
	r := &ClusterInstanceReconciler{Client: c, Scheme: c.Scheme()}
	if _, err := r.teardownHyperShiftBacking(context.Background(), instance); err == nil {
		t.Fatal("cleanup ignored an unverified legacy control-plane namespace")
	}
}
