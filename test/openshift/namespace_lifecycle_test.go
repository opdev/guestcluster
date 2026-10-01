//go:build openshift

package openshift

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	brokerv1alpha1 "github.com/opdev/guestcluster/api/v1alpha1"
	"github.com/opdev/guestcluster/internal/resources"
	routev1 "github.com/openshift/api/route/v1"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authorization/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	kubevirtv1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/yaml"
)

const enabledLabel = "guestcluster.opdev.io/enabled"
const poolName = "same-pool"
const instanceName = "same-pool-0"
const defaultOperatorNamespace = "guestcluster-operator-system"
const clusterRoleKind = "ClusterRole"

// This suite uses real guest clusters on an OpenShift management cluster. The
// test installs one isolated direct manager and never supplies synthetic Ready
// status.
func TestOpenShiftNamespaceLifecycle(t *testing.T) {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	config := ctrl.GetConfigOrDie()
	kube, err := kubernetes.NewForConfig(config)
	must(t, err)
	install := installDirectManager(t, kube, config)

	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		scheme.AddToScheme, brokerv1alpha1.AddToScheme, kubevirtv1.AddToScheme,
		routev1.AddToScheme, hyperv1beta1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	c, err := client.New(config, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	if !t.Run("direct-install-permissions", func(t *testing.T) {
		verifyInstallation(t, c, config, install)
	}) {
		t.Fatal("direct manager installation or permissions failed; stopping before lifecycle tests")
	}
	templates := map[brokerv1alpha1.ClusterTopology]brokerv1alpha1.ClusterTemplate{
		brokerv1alpha1.TopologyCRC: loadTemplate(t, "OPENSHIFT_CRC_POOL", "crc-pool.yaml"),
		brokerv1alpha1.TopologyHCP: loadTemplate(t, "OPENSHIFT_HCP_POOL", "hcp-pool.yaml"),
	}
	const crc, hcp = brokerv1alpha1.TopologyCRC, brokerv1alpha1.TopologyHCP
	for _, pair := range [][2]brokerv1alpha1.ClusterTopology{{crc, crc}, {hcp, hcp}, {crc, hcp}} {
		if !t.Run(string(pair[0])+"-"+string(pair[1]), func(t *testing.T) {
			runNamespaceLifecyclePair(t, c, config, pair, templates, install.inputNamespace, install.imageNamespace)
		}) {
			t.Fatalf("topology pair %s-%s failed; stopping before the next lifecycle case", pair[0], pair[1])
		}
	}
}

func runNamespaceLifecyclePair(
	t *testing.T,
	c client.Client,
	config *rest.Config,
	pair [2]brokerv1alpha1.ClusterTopology,
	templates map[brokerv1alpha1.ClusterTopology]brokerv1alpha1.ClusterTemplate,
	inputNamespace string,
	imageNamespace string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	var namespaces [2]*corev1.Namespace
	var instances [2]*brokerv1alpha1.ClusterInstance
	var leases [2]*brokerv1alpha1.ClusterLease
	var credentials [2][]byte
	var shared []client.Object
	for i, topology := range pair {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "gc-openshift-case-"}}
		must(t, c.Create(ctx, ns))
		namespaces[i] = ns
		t.Cleanup(func() { cleanupNamespace(t, c, ns) })
		grantImagePull(t, c, ns.Name, imageNamespace)
		copyInputs(t, ctx, c, ns.Name, templates[topology], inputNamespace)
		pool := &brokerv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: ns.Name},
			Spec:       brokerv1alpha1.ClusterPoolSpec{Type: topology, MinSize: 1, MaxSize: 2, Template: templates[topology]},
		}
		must(t, c.Create(ctx, pool))
		wait(t, ctx, "disabled pool condition", func() bool {
			must(t, c.Get(ctx, client.ObjectKeyFromObject(pool), pool))
			return conditionReason(pool.Status.Conditions, "ProvisioningAllowed") == "NamespaceDisabled"
		})
		assertInstanceCount(t, ctx, c, ns.Name, 0)
		// A standalone CR created before opt-in must also wait.
		unstarted := &brokerv1alpha1.ClusterInstance{
			ObjectMeta: metav1.ObjectMeta{Name: "unstarted", Namespace: ns.Name},
			Spec:       brokerv1alpha1.ClusterInstanceSpec{Type: topology, Template: templates[topology]},
		}
		must(t, c.Create(ctx, unstarted))
		wait(t, ctx, "unstarted instance block", func() bool {
			must(t, c.Get(ctx, client.ObjectKeyFromObject(unstarted), unstarted))
			return conditionReason(unstarted.Status.Conditions, "ProvisioningAllowed") == "NamespaceDisabled"
		})
		if unstarted.Status.Provisioning != nil {
			t.Fatal("disabled instance was authorized")
		}
		must(t, c.Delete(ctx, unstarted))
		waitGone(t, ctx, c, unstarted)
		setEnabled(t, ctx, c, ns, true)
		instance := &brokerv1alpha1.ClusterInstance{}
		key := client.ObjectKey{Namespace: ns.Name, Name: instanceName}
		wait(t, ctx, "durable authorization", func() bool {
			err := c.Get(ctx, key, instance)
			if apierrors.IsNotFound(err) {
				return false
			}
			must(t, err)
			return instance.Status.Provisioning != nil
		})
		setEnabled(t, ctx, c, ns, false)
		instances[i] = instance
	}
	for i, instance := range instances {
		waitReady(t, ctx, c, instance)
		verifyPlacement(t, ctx, c, instance)
		if binding := instance.Status.CRC; binding != nil && binding.BootKey != nil {
			key := binding.BootKey
			for _, obj := range []client.Object{
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: key.PVCNamespace, Name: key.PVCName}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: key.SecretNamespace, Name: key.SecretName}},
				&brokerv1alpha1.CRCBundle{ObjectMeta: metav1.ObjectMeta{
					Name: resources.CRCBundleName(instance.Spec.Template.CRCVersion, crcArch(instance)),
				}},
			} {
				must(t, c.Get(ctx, client.ObjectKeyFromObject(obj), obj))
				shared = append(shared, obj)
			}
		}
		lease := &brokerv1alpha1.ClusterLease{
			ObjectMeta: metav1.ObjectMeta{Name: "same-lease", Namespace: instance.Namespace},
			Spec:       brokerv1alpha1.ClusterLeaseSpec{PoolRef: corev1.LocalObjectReference{Name: poolName}},
		}
		must(t, c.Create(ctx, lease))
		wait(t, ctx, "lease binding after opt-out", func() bool {
			must(t, c.Get(ctx, client.ObjectKeyFromObject(lease), lease))
			return lease.Status.Phase == brokerv1alpha1.PhaseLeaseBound
		})
		leases[i] = lease
		secret := &corev1.Secret{}
		secretKey := client.ObjectKey{Namespace: lease.Namespace, Name: lease.Status.KubeconfigSecretRef.Name}
		must(t, c.Get(ctx, secretKey, secret))
		credentials[i] = secret.Data[resources.KubeconfigSecretKey]
		useKubeconfig(t, ctx, credentials[i], instance.Namespace)
		pool := &brokerv1alpha1.ClusterPool{}
		must(t, c.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: poolName}, pool))
		pool.Spec.MinSize = 2
		must(t, c.Update(ctx, pool))
		pending := &brokerv1alpha1.ClusterLease{
			ObjectMeta: metav1.ObjectMeta{Name: "waiting-demand", Namespace: instance.Namespace}, Spec: lease.Spec,
		}
		must(t, c.Create(ctx, pending))
	}
	if instances[0].Status.APIEndpoint == instances[1].Status.APIEndpoint {
		t.Fatal("clusters share an endpoint")
	}
	consistently(t, ctx, 30*time.Second, func() {
		for _, ns := range namespaces {
			assertInstanceCount(t, ctx, c, ns.Name, 1)
		}
	})
	for i, instance := range instances {
		endpoint := instance.Status.APIEndpoint
		if instance.Spec.Type == brokerv1alpha1.TopologyCRC {
			verifyAgentAuthorization(t, ctx, config, instance, namespaces[1-i].Name)
			oldUID := instance.Status.CRC.VMIUID
			vmi := &kubevirtv1.VirtualMachineInstance{ObjectMeta: metav1.ObjectMeta{
				Name: resources.CRCVMName(instance), Namespace: instance.Namespace,
			}}
			must(t, c.Delete(ctx, vmi))
			wait(t, ctx, "CRC VMI replacement and recovery after opt-out", func() bool {
				must(t, c.Get(ctx, client.ObjectKeyFromObject(instance), instance))
				return instance.Status.Phase == brokerv1alpha1.PhaseReady && instance.Status.CRC.VMIUID != oldUID
			})
		} else {
			hs := instance.Status.HyperShift
			route := &routev1.Route{ObjectMeta: metav1.ObjectMeta{
				Name:      resources.HostedClusterAPIRouteName(instance.Name),
				Namespace: resources.HostedControlPlaneNamespace(hs.HostedClusterNamespace, hs.HostedClusterName),
			}}
			must(t, c.Get(ctx, client.ObjectKeyFromObject(route), route))
			oldUID := route.UID
			must(t, c.Delete(ctx, route))
			wait(t, ctx, "HCP Route repair after opt-out", func() bool {
				err := c.Get(ctx, client.ObjectKeyFromObject(route), route)
				if apierrors.IsNotFound(err) {
					return false
				}
				must(t, err)
				return route.UID != oldUID && routeAdmitted(route)
			})
			waitReady(t, ctx, c, instance)
		}
		if instance.Status.APIEndpoint != endpoint {
			t.Fatal("recovery changed endpoint")
		}
		useKubeconfig(t, ctx, credentials[i], instance.Namespace)
	}
	must(t, c.Delete(ctx, leases[0]))
	waitGone(t, ctx, c, instances[0])
	verifyCleanup(t, ctx, c, instances[0])
	consistently(t, ctx, 30*time.Second, func() { assertInstanceCount(t, ctx, c, namespaces[0].Name, 0) })
	useKubeconfig(t, ctx, credentials[1], instances[1].Namespace)
	for _, obj := range shared {
		current := obj.DeepCopyObject().(client.Object)
		must(t, c.Get(ctx, client.ObjectKeyFromObject(obj), current))
		if current.GetUID() != obj.GetUID() {
			t.Fatal("instance cleanup changed shared CRCBundle artifacts")
		}
	}
	input := &corev1.Secret{}
	inputName := instances[0].Spec.Template.PullSecretRef.Name
	if inputName == "" {
		inputName = resources.ClusterPullSecretName
	}
	must(t, c.Get(ctx, client.ObjectKey{Namespace: namespaces[0].Name, Name: inputName}, input))
	if len(input.OwnerReferences) != 0 {
		t.Fatal("instance adopted user input")
	}
	// Resume waiting demand with one instance, then delete its namespace.
	pool := &brokerv1alpha1.ClusterPool{}
	must(t, c.Get(ctx, client.ObjectKey{Namespace: namespaces[0].Name, Name: poolName}, pool))
	pool.Spec.MinSize = 1
	must(t, c.Update(ctx, pool))
	oldUID := instances[0].UID
	setEnabled(t, ctx, c, namespaces[0], true)
	wait(t, ctx, "replacement after re-enable", func() bool {
		err := c.Get(ctx, client.ObjectKey{Namespace: namespaces[0].Name, Name: instanceName}, instances[0])
		if apierrors.IsNotFound(err) {
			return false
		}
		must(t, err)
		return instances[0].UID != oldUID && instances[0].Status.Phase == brokerv1alpha1.PhaseReady
	})
	waiting := &brokerv1alpha1.ClusterLease{}
	wait(t, ctx, "waiting demand resumes", func() bool {
		must(t, c.Get(ctx, client.ObjectKey{Namespace: namespaces[0].Name, Name: "waiting-demand"}, waiting))
		return waiting.Status.Phase == brokerv1alpha1.PhaseLeaseBound
	})
	must(t, c.Delete(ctx, namespaces[0]))
	waitGone(t, ctx, c, namespaces[0])
	verifyCleanup(t, ctx, c, instances[0])
	useKubeconfig(t, ctx, credentials[1], instances[1].Namespace)
	must(t, c.Delete(ctx, namespaces[1]))
	waitGone(t, ctx, c, namespaces[1])
	verifyCleanup(t, ctx, c, instances[1])
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func loadTemplate(t *testing.T, env, defaultFile string) brokerv1alpha1.ClusterTemplate {
	t.Helper()
	path := os.Getenv(env)
	if path == "" {
		path = defaultFile
	}
	data, err := os.ReadFile(path)
	must(t, err)
	pool := &brokerv1alpha1.ClusterPool{}
	must(t, yaml.Unmarshal(data, pool))
	return pool.Spec.Template
}
func wait(t *testing.T, ctx context.Context, description string, ready func() bool) {
	t.Helper()
	for {
		if ready() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", description, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
func consistently(t *testing.T, ctx context.Context, duration time.Duration, check func()) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		check()
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
func waitGone(t *testing.T, ctx context.Context, c client.Client, obj client.Object) {
	t.Helper()
	wait(t, ctx, "cleanup of "+client.ObjectKeyFromObject(obj).String(), func() bool {
		err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		if apierrors.IsNotFound(err) {
			return true
		}
		must(t, err)
		return false
	})
}
func waitReady(t *testing.T, ctx context.Context, c client.Client, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	wait(t, ctx, "guest Ready", func() bool {
		must(t, c.Get(ctx, client.ObjectKeyFromObject(instance), instance))
		if instance.Status.Phase == brokerv1alpha1.PhaseFailed {
			for _, condition := range instance.Status.Conditions {
				if condition.Type == "Ready" {
					t.Fatalf("guest provisioning failed: %s: %s", condition.Reason, condition.Message)
				}
			}
			t.Fatal("guest provisioning failed without a Ready condition")
		}
		return instance.Status.Phase == brokerv1alpha1.PhaseReady
	})
}
func assertInstanceCount(t *testing.T, ctx context.Context, c client.Client, namespace string, want int) {
	t.Helper()
	list := &brokerv1alpha1.ClusterInstanceList{}
	must(t, c.List(ctx, list, client.InNamespace(namespace)))
	if len(list.Items) != want {
		t.Fatalf("%s has %d instances, want %d", namespace, len(list.Items), want)
	}
}
func conditionReason(conditions []metav1.Condition, kind string) string {
	for _, condition := range conditions {
		if condition.Type == kind {
			return condition.Reason
		}
	}
	return ""
}
func setEnabled(t *testing.T, ctx context.Context, c client.Client, ns *corev1.Namespace, enabled bool) {
	t.Helper()
	must(t, c.Get(ctx, client.ObjectKeyFromObject(ns), ns))
	if ns.Labels == nil {
		ns.Labels = map[string]string{}
	}
	if enabled {
		ns.Labels[enabledLabel] = "true"
	} else {
		delete(ns.Labels, enabledLabel)
	}
	must(t, c.Update(ctx, ns))
}
func copyInputs(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	namespace string,
	template brokerv1alpha1.ClusterTemplate,
	inputNamespace string,
) {
	t.Helper()
	names := map[string]bool{}
	pull := template.PullSecretRef.Name
	if pull == "" {
		pull = resources.ClusterPullSecretName
	}
	names[pull] = true
	if template.BundleSSHKeyRef != nil && template.CRCVersion == "" {
		names[template.BundleSSHKeyRef.Name] = true
	}
	if template.HCPWorkerSSHKeyRef != nil {
		names[template.HCPWorkerSSHKeyRef.Name] = true
	}
	for name := range names {
		secret := &corev1.Secret{}
		must(t, c.Get(ctx, client.ObjectKey{Namespace: inputNamespace, Name: name}, secret))
		copy := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Type: secret.Type, Data: secret.Data,
		}
		must(t, c.Create(ctx, copy))
	}
}
func useKubeconfig(t *testing.T, ctx context.Context, data []byte, identity string) {
	t.Helper()
	config, err := clientcmd.RESTConfigFromKubeConfig(data)
	must(t, err)
	config.Timeout = 30 * time.Second
	c, err := kubernetes.NewForConfig(config)
	must(t, err)
	deadline := time.Now().Add(5 * time.Minute)
	var readinessErr error
	for time.Now().Before(deadline) {
		readinessErr = c.Discovery().RESTClient().Get().AbsPath("/readyz").Do(ctx).Error()
		if readinessErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("guest API for %s did not become ready: %v", identity, readinessErr)
		case <-time.After(5 * time.Second):
		}
	}
	if readinessErr != nil {
		t.Fatalf(
			"guest API for %s did not become ready; TLS details: %s: %v",
			identity,
			diagnoseKubeconfigTLS(data),
			readinessErr,
		)
	}
	nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	must(t, err)
	if len(nodes.Items) == 0 {
		t.Fatal("guest has no nodes")
	}
	marker := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "guestcluster-openshift-e2e-identity", Namespace: "kube-system"},
		Data:       map[string]string{"sourceNamespace": identity},
	}
	created, err := c.CoreV1().ConfigMaps("kube-system").Create(ctx, marker, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = c.CoreV1().ConfigMaps("kube-system").Get(ctx, marker.Name, metav1.GetOptions{})
	}
	must(t, err)
	if created.Data["sourceNamespace"] != identity {
		t.Fatalf("kubeconfig for %s reaches guest marked %s", identity, created.Data["sourceNamespace"])
	}
}

func diagnoseKubeconfigTLS(data []byte) string {
	config, err := clientcmd.Load(data)
	if err != nil {
		return "parse kubeconfig: " + err.Error()
	}
	var output []string
	for name, cluster := range config.Clusters {
		endpoint, err := url.Parse(cluster.Server)
		if err != nil {
			output = append(output, name+": invalid endpoint: "+err.Error())
			continue
		}
		port := endpoint.Port()
		if port == "" {
			port = "443"
		}
		dialer := &net.Dialer{Timeout: 8 * time.Second}
		conn, err := tls.DialWithDialer(dialer, "tcp", net.JoinHostPort(endpoint.Hostname(), port), &tls.Config{
			ServerName: endpoint.Hostname(), InsecureSkipVerify: true,
		})
		if err != nil {
			output = append(output, name+": dial: "+err.Error())
			continue
		}
		state := conn.ConnectionState()
		_ = conn.Close()
		peer := "missing"
		if len(state.PeerCertificates) > 0 {
			cert := state.PeerCertificates[0]
			fingerprint := sha256.Sum256(cert.Raw)
			peer = fmt.Sprintf("sha256=%s sans=%v issuer=%q",
				hex.EncodeToString(fingerprint[:8]), cert.DNSNames, cert.Issuer.CommonName)
		}
		ca := "missing"
		if block, _ := pem.Decode(cluster.CertificateAuthorityData); block != nil {
			if cert, parseErr := x509.ParseCertificate(block.Bytes); parseErr == nil {
				fingerprint := sha256.Sum256(cert.Raw)
				ca = hex.EncodeToString(fingerprint[:8])
			}
		}
		output = append(output, fmt.Sprintf("%s endpoint=%s CA-sha256=%s peer={%s}", name, cluster.Server, ca, peer))
	}
	return strings.Join(output, "; ")
}
func verifyPlacement(t *testing.T, ctx context.Context, c client.Client, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	if instance.Spec.Type == brokerv1alpha1.TopologyHCP {
		hs := instance.Status.HyperShift
		if hs == nil || hs.HostedClusterNamespace != instance.Namespace {
			t.Fatal("HCP backing is not namespace-local")
		}
		hc := &hyperv1beta1.HostedCluster{}
		must(t, c.Get(ctx, client.ObjectKey{Namespace: hs.HostedClusterNamespace, Name: hs.HostedClusterName}, hc))
		if !metav1.IsControlledBy(hc, instance) {
			t.Fatal("HostedCluster owner is incorrect")
		}
	} else {
		vm := &kubevirtv1.VirtualMachine{}
		must(t, c.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: resources.CRCVMName(instance)}, vm))
		if !metav1.IsControlledBy(vm, instance) {
			t.Fatal("CRC VM owner is incorrect")
		}
	}
}
func verifyCleanup(t *testing.T, ctx context.Context, c client.Client, instance *brokerv1alpha1.ClusterInstance) {
	t.Helper()
	for _, list := range []client.ObjectList{
		&batchv1.JobList{}, &corev1.SecretList{}, &corev1.ServiceAccountList{},
		&rbacv1.RoleBindingList{}, &corev1.ServiceList{}, &routev1.RouteList{},
	} {
		wait(t, ctx, "instance-owned object cleanup", func() bool {
			must(t, c.List(ctx, list, client.InNamespace(instance.Namespace)))
			objects, err := apimeta.ExtractList(list)
			must(t, err)
			for _, raw := range objects {
				obj := raw.(client.Object)
				for _, owner := range obj.GetOwnerReferences() {
					if owner.UID == instance.UID {
						return false
					}
				}
			}
			return true
		})
	}
	if instance.Status.HyperShift != nil {
		hs := instance.Status.HyperShift
		name := resources.HostedControlPlaneNamespace(hs.HostedClusterNamespace, hs.HostedClusterName)
		waitGone(t, ctx, c, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	} else {
		for _, obj := range []client.Object{
			&kubevirtv1.VirtualMachine{}, &kubevirtv1.VirtualMachineInstance{}, &corev1.PersistentVolumeClaim{},
		} {
			obj.SetNamespace(instance.Namespace)
			obj.SetName(resources.CRCVMName(instance))
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				obj.SetName(resources.CRCDiskName(instance))
			}
			waitGone(t, ctx, c, obj)
		}
	}
}

func crcArch(instance *brokerv1alpha1.ClusterInstance) string {
	if instance.Spec.Template.CRCArch != "" {
		return instance.Spec.Template.CRCArch
	}
	return resources.DefaultCRCArch
}

func routeAdmitted(route *routev1.Route) bool {
	for _, ingress := range route.Status.Ingress {
		for _, condition := range ingress.Conditions {
			if condition.Type == routev1.RouteAdmitted && condition.Status == corev1.ConditionTrue {
				return true
			}
		}
	}
	return false
}
func cleanupNamespace(t *testing.T, c client.Client, ns *corev1.Namespace) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if err := c.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
		t.Errorf("deleting test namespace %s: %v", ns.Name, err)
		return
	}
	waitGone(t, ctx, c, ns)
}
func verifyAgentAuthorization(
	t *testing.T, ctx context.Context, config *rest.Config, instance *brokerv1alpha1.ClusterInstance, other string,
) {
	t.Helper()
	agent := rest.CopyConfig(config)
	agent.Impersonate.UserName = fmt.Sprintf("system:serviceaccount:%s:%s",
		instance.Namespace, resources.CRCAgentAccountName(instance.Name))
	agent.Impersonate.Groups = []string{
		"system:authenticated", "system:serviceaccounts", "system:serviceaccounts:" + instance.Namespace,
	}
	c, err := kubernetes.NewForConfig(agent)
	must(t, err)
	for _, ns := range []string{instance.Namespace, other} {
		for _, verb := range []string{"get", "create", "update"} {
			request := &authv1.SelfSubjectAccessReview{Spec: authv1.SelfSubjectAccessReviewSpec{
				ResourceAttributes: &authv1.ResourceAttributes{Namespace: ns, Resource: "secrets", Verb: verb},
			}}
			review, err := c.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, request, metav1.CreateOptions{})
			must(t, err)
			if review.Status.Allowed != (ns == instance.Namespace) {
				t.Fatalf("unexpected agent %s authorization in %s", verb, ns)
			}
		}
	}
}
func verifyInstallation(t *testing.T, c client.Client, config *rest.Config, install directManagerInstall) {
	t.Helper()
	ctx := context.Background()
	deployment := &appsv1.Deployment{}
	must(t, c.Get(ctx, client.ObjectKey{Namespace: install.namespace, Name: install.deployment}, deployment))
	if deployment.Status.AvailableReplicas < 1 {
		t.Fatal("direct manager Deployment is not Available")
	}
	if len(deployment.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("direct manager Deployment has no containers")
	}
	if deployment.Spec.Template.Spec.Containers[0].Image != install.managerImage {
		t.Fatal("installed manager is not the image under test")
	}
	agentMatches := false
	for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
		if env.Name == resources.CRCAgentImageEnvVar && env.Value == install.agentImage {
			agentMatches = true
		}
	}
	if !agentMatches {
		t.Fatal("installed agent is not the image under test")
	}
	managerSA := deployment.Spec.Template.Spec.ServiceAccountName
	var agentRole string
	for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "CRC_AGENT_CLUSTER_ROLE" {
			agentRole = env.Value
			break
		}
	}
	if agentRole == "" {
		t.Fatal("direct manager Deployment does not set CRC_AGENT_CLUSTER_ROLE")
	}
	verifyInstalledManagerPermissions(t, ctx, c, config, deployment, managerSA, agentRole)
}

func verifyInstalledManagerPermissions(
	t *testing.T, ctx context.Context, c client.Client, config *rest.Config,
	deployment *appsv1.Deployment, managerSA, agentRole string,
) {
	t.Helper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "gc-openshift-rbac-"}}
	must(t, c.Create(ctx, namespace))
	t.Cleanup(func() { cleanupNamespace(t, c, namespace) })
	installedRole := &rbacv1.ClusterRole{}
	must(t, c.Get(ctx, client.ObjectKey{Name: agentRole}, installedRole))
	if !hasAgentRules(installedRole.Rules) {
		t.Fatalf("installed agent ClusterRole %q has unexpected rules", agentRole)
	}
	managerConfig := rest.CopyConfig(config)
	managerConfig.Impersonate.UserName = fmt.Sprintf("system:serviceaccount:%s:%s", deployment.Namespace, managerSA)
	managerConfig.Impersonate.Groups = []string{
		"system:authenticated", "system:serviceaccounts", "system:serviceaccounts:" + deployment.Namespace,
	}
	manager, err := kubernetes.NewForConfig(managerConfig)
	must(t, err)
	instance := &brokerv1alpha1.ClusterInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-rbac-check", Namespace: namespace.Name},
		Spec: brokerv1alpha1.ClusterInstanceSpec{
			Type:     brokerv1alpha1.TopologyCRC,
			Template: brokerv1alpha1.ClusterTemplate{Cores: 1, Memory: "1Gi"},
		},
	}
	must(t, c.Create(ctx, instance))
	accountName := resources.CRCAgentAccountName(instance.Name)
	account := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: accountName, Namespace: namespace.Name, OwnerReferences: resources.InstanceOwnerReferences(instance),
	}}
	account.OwnerReferences[0].BlockOwnerDeletion = nil
	_, err = manager.CoreV1().ServiceAccounts(namespace.Name).Create(ctx, account, metav1.CreateOptions{})
	must(t, err)
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: resources.CRCAgentBindingName(instance.Name), Namespace: namespace.Name,
			OwnerReferences: resources.InstanceOwnerReferences(instance)},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: clusterRoleKind, Name: agentRole},
		Subjects: []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: accountName, Namespace: namespace.Name}},
	}
	binding.OwnerReferences[0].BlockOwnerDeletion = nil
	if _, err := manager.RbacV1().RoleBindings(namespace.Name).Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		t.Fatalf("installed manager could not create the agent RoleBinding: %v", err)
	}
}

func hasAgentRules(rules []rbacv1.PolicyRule) bool {
	var vmiRead, secretWrite bool
	for _, rule := range rules {
		if contains(rule.APIGroups, "kubevirt.io") && contains(rule.Resources, "virtualmachineinstances") &&
			contains(rule.Verbs, "get") && contains(rule.Verbs, "list") && contains(rule.Verbs, "watch") {
			vmiRead = true
		}
		if contains(rule.APIGroups, "") && contains(rule.Resources, "secrets") &&
			contains(rule.Verbs, "get") && contains(rule.Verbs, "create") && contains(rule.Verbs, "update") {
			secretWrite = true
		}
	}
	return vmiRead && secretWrite
}

func grantImagePull(t *testing.T, c client.Client, sourceNamespace, imageNamespace string) {
	t.Helper()
	name := "guestcluster-pull-" + sourceNamespace
	if len(name) > 63 {
		name = name[:63]
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: imageNamespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: clusterRoleKind, Name: "system:image-puller"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, Name: "system:serviceaccounts:" + sourceNamespace}},
	}
	if err := c.Create(context.Background(), binding); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := c.Delete(cleanupCtx, binding); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("deleting test image-pull RoleBinding %s/%s: %v", imageNamespace, name, err)
			return
		}
		waitGone(t, cleanupCtx, c, binding)
	})
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
