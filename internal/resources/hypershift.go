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
	"strings"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	configv1 "github.com/openshift/api/config/v1"
	routev1 "github.com/openshift/api/route/v1"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/openshift/hypershift/api/util/ipnet"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
)

// defaultNodePoolRootVolumeSize mirrors the upstream `hcp create cluster`
// default when ClusterTemplate.RootVolumeSize is unset.
const defaultNodePoolRootVolumeSize = "32Gi"

// BuildHostedCluster constructs a minimal-but-real HyperShift HostedCluster
// for the KubeVirt platform, mirroring the field set that `hcp create
// cluster kubevirt` produces. Some fields have safe CRD-side defaults (HA
// policies, cluster/service network CIDRs beyond the documented defaults),
// but BuildHostedCluster populates them explicitly anyway. This keeps the
// desired-state object fully deterministic and reconciliation-stable. If
// the spec relied on server-side defaulting instead, a controller-runtime
// CreateOrUpdate would otherwise perpetually see a diff on fields the
// apiserver filled in.
//
// pullSecretName is the name of the Secret, in namespace, that holds the
// pull-secret to use. namespace is the HostedCluster's own namespace,
// because LocalObjectReference resolves relative to the referencing
// object. The caller resolves pullSecretName, either from the template's
// explicit PullSecretRef, or from a materialized copy of the management
// cluster's own default pull secret (see
// ClusterInstanceReconciler.resolvePullSecret).
//
// nodePortAddress must be a reachable address on the raw NodePort number
// assigned to the Services[APIServer] NodePort Service. In practice, this
// is an InternalIP of any current, Ready management-cluster node (see
// ClusterInstanceReconciler.managementNodeAddress). It is not the
// externally-routable admin hostname; see BuildHostedClusterAPIRoute for
// that, a Route this operator manages separately from this
// HostedCluster object. These are two different addresses with different
// reachability requirements. Conflating them is a real, previously-shipped
// bug (see below).
//
// Services[APIServer] is deliberately set to Type=NodePort, not Route,
// even though the actual NodePort Service/port is never used for the
// operator's own admin access path. This is a two-part workaround for how
// HyperShift's KubeVirt platform handles Route-published services:
//
//  1. If APIServer used Type=Route instead, HyperShift requires an
//     explicit Route.Hostname. OAuthServer/Konnectivity/Ignition instead
//     fall back to auto-deriving one from the control-plane-operator's own
//     DefaultIngressDomain. Without an explicit hostname, infrastructure
//     reconciliation hard-fails with "route hostname is required for
//     service APIServer".
//  2. Setting that hostname is itself the trigger (see HyperShift's
//     netutil.UseDedicatedDNSForKAS/LabelHCPRoutes/UseHCPRouter) for
//     provisioning a separate, per-HostedCluster "private-router"
//     Deployment plus its own dedicated LoadBalancer Service, a second,
//     independently-provisioned cloud load balancer, instead of routing
//     through the management cluster's own already-working shared ingress
//     router. On this project's target environments, that extra
//     LoadBalancer is frequently slow to provision or unreachable, for
//     example depending on the management cluster's own
//     AWS security groups. This defeats the point of a lightweight
//     dev/test guest cluster.
//
// NodePort avoids both problems. Per HyperShift's own control-plane-operator
// code (netutil.LabelHCPRoutes's KubevirtPlatform case), APIServer on
// NodePort or LoadBalancer routes ordinary (non-KAS) Route traffic through
// the management cluster's own router, matching what
// BuildHostedClusterAPIRoute already does for KAS itself.
//
// IMPORTANT: nodePortAddress is not merely a cosmetic or cert-SAN choice.
// HyperShift's own generated worker bootstrap ignition embeds
// Services[APIServer].NodePort.Address:<assigned-nodeport> verbatim as the
// kubelet bootstrap kubeconfig's server URL (kas.ReconcileServiceStatus's
// NodePort case). An earlier revision of this function passed the
// externally-routable admin hostname here, reasoning that it would also
// become a valid DNS SAN on the KAS serving certificate. That hostname,
// however, only resolves to the shared ingress router, which proxies ports
// 80/443 only, not arbitrary NodePort numbers. Every worker's kubelet was
// therefore permanently unable to reach the API server at all, confirmed
// via direct in-guest journalctl: "No valid client certificate is found but
// the server is not responsive". No CSR was ever filed, so NodePools never
// reached their desired replica count. Correctness requires a real,
// cluster-reachable node address. As a consequence, nodePortAddress itself
// is not covered by a DNS SAN matching servingCertHostname's own hostname.
// servingCertName/servingCertHostname below close that gap separately,
// without reintroducing this bug.
//
// servingCertName, if non-empty, is the name of a kubernetes.io/tls Secret
// (in namespace) that the caller has already materialized there (see
// ClusterInstanceReconciler.ensureKASServingCert), holding a certificate
// valid for servingCertHostname. This is typically the same
// externally-routable admin hostname that BuildHostedClusterAPIRoute's
// Route uses. BuildHostedCluster wires this Secret in as a
// Configuration.APIServer.ServingCerts.NamedCertificate. HyperShift mounts
// it into the guest kube-apiserver and serves it via SNI whenever a client
// connects using servingCertHostname. This is independent of, and not to
// be confused with, HyperShift's own native KubeAPIServerDNSName/
// status.customKubeconfig feature. BuildHostedCluster deliberately does
// not use that feature here: it is hard-gated to control-plane-operator
// images built from OCP 4.19+ release payloads, and even then it would
// embed nodePortAddress's NodePort number, not 443, into its generated
// kubeconfig's server URL, which does not route through this project's
// shared-ingress-router-based admin access path at all. Both
// servingCertName and servingCertHostname must be non-empty for the
// NamedCertificate to be wired in. The caller
// (ClusterInstanceReconciler.ensureHyperShiftBacking) embeds the same
// certificate as certificate-authority-data in the published admin
// kubeconfig (see RewriteKubeconfigServer), so callers verify the TLS
// connection normally with no --insecure-skip-tls-verify needed.
//
// sshKeySecretName, if non-empty, is the name of a Secret (in namespace,
// with data key HCPWorkerSSHKeyDataKey) that the caller has already
// materialized there (see ClusterInstanceReconciler.resolveHCPWorkerSSHKey)
// from ClusterTemplate.HCPWorkerSSHKeyRef. BuildHostedCluster injects it as
// HostedCluster.spec.sshKey, so every NodePool worker's "core" user accepts
// that key. This is a debugging convenience (see HCPWorkerSSHKeyRef's doc
// comment). When sshKeySecretName is empty, HostedCluster.spec.sshKey stays
// at its zero value, so no key is injected.
// HostedClusterOptions groups BuildHostedCluster's per-instance,
// caller-resolved parameters (see that function's doc comment for what each
// one means and why the function cannot recompute them from instance/tmpl
// alone). Named fields prevent the kind of transposition mistake that a long
// run of same-typed positional string parameters invites.
type HostedClusterOptions struct {
	Namespace           string
	PullSecretName      string
	NodePortAddress     string
	ServingCertName     string
	ServingCertHostname string
	SSHKeySecretName    string
}

func BuildHostedCluster(instance *brokerv1alpha1.ClusterInstance, opts HostedClusterOptions) *hyperv1beta1.HostedCluster {
	tmpl := instance.Spec.Template

	// ControllerAvailabilityPolicy is immutable once set on the
	// HostedCluster, so BuildHostedCluster must decide it correctly at
	// creation time. A later change to
	// ClusterTemplate.ControllerAvailabilityPolicy has no effect on an
	// already-provisioned instance. It defaults to SingleReplica (see the
	// CRD's own +kubebuilder:default), because HighlyAvailable's 3-way
	// etcd/kube-apiserver spread requires anti-affinity across that many
	// schedulable, untainted nodes on the management cluster. Small or dev
	// management clusters (for example CRC-hosted ones) often lack that
	// many nodes. HighlyAvailable is consequently opt-in, not the default.
	availabilityPolicy := hyperv1beta1.SingleReplica
	if tmpl.ControllerAvailabilityPolicy == brokerv1alpha1.AvailabilityPolicyHighlyAvailable {
		availabilityPolicy = hyperv1beta1.HighlyAvailable
	}

	hc := &hyperv1beta1.HostedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HostedClusterName(instance.Name),
			Namespace: opts.Namespace,
			Labels:    CommonLabels(instance),
		},
		Spec: hyperv1beta1.HostedClusterSpec{
			Release: hyperv1beta1.Release{
				Image: tmpl.ReleaseImage,
			},
			ControllerAvailabilityPolicy: availabilityPolicy,
			Platform: hyperv1beta1.PlatformSpec{
				Type:     hyperv1beta1.KubevirtPlatform,
				Kubevirt: &hyperv1beta1.KubevirtPlatformSpec{},
			},
			Networking: hyperv1beta1.ClusterNetworking{
				NetworkType: hyperv1beta1.OVNKubernetes,
				ClusterNetwork: []hyperv1beta1.ClusterNetworkEntry{
					{CIDR: *ipnet.MustParseCIDR("10.132.0.0/14")},
				},
				ServiceNetwork: []hyperv1beta1.ServiceNetworkEntry{
					{CIDR: *ipnet.MustParseCIDR("172.31.0.0/16")},
				},
			},
			Etcd: hyperv1beta1.EtcdSpec{
				ManagementType: hyperv1beta1.Managed,
				Managed: &hyperv1beta1.ManagedEtcdSpec{
					Storage: hyperv1beta1.ManagedEtcdStorageSpec{
						Type: hyperv1beta1.PersistentVolumeEtcdStorage,
						PersistentVolume: &hyperv1beta1.PersistentVolumeEtcdStorageSpec{
							Size: quantityPtr(resource.MustParse("8Gi")),
						},
					},
				},
			},
			Services: []hyperv1beta1.ServicePublishingStrategyMapping{
				{
					Service: hyperv1beta1.APIServer,
					ServicePublishingStrategy: hyperv1beta1.ServicePublishingStrategy{
						Type:     hyperv1beta1.NodePort,
						NodePort: &hyperv1beta1.NodePortPublishingStrategy{Address: opts.NodePortAddress},
					},
				},
				{Service: hyperv1beta1.OAuthServer, ServicePublishingStrategy: hyperv1beta1.ServicePublishingStrategy{Type: hyperv1beta1.Route}},
				{Service: hyperv1beta1.Konnectivity, ServicePublishingStrategy: hyperv1beta1.ServicePublishingStrategy{Type: hyperv1beta1.Route}},
				{Service: hyperv1beta1.Ignition, ServicePublishingStrategy: hyperv1beta1.ServicePublishingStrategy{Type: hyperv1beta1.Route}},
			},
			PullSecret: corev1.LocalObjectReference{Name: opts.PullSecretName},
		},
	}

	if tmpl.IDMSRef != nil {
		// NOTE: real IDMS/ICSP parsing is stubbed. That parsing would read
		// mirror mappings out of the referenced ConfigMap and translate
		// them into []ImageContentSource. Wiring the field here documents
		// the intended integration point for disconnected/mirrored
		// registry support. Populating it from IDMSRef's actual contents
		// is TODO.
		hc.Spec.ImageContentSources = []hyperv1beta1.ImageContentSource{}
	}

	if opts.SSHKeySecretName != "" {
		hc.Spec.SSHKey = corev1.LocalObjectReference{Name: opts.SSHKeySecretName}
	}

	if opts.ServingCertName != "" && opts.ServingCertHostname != "" {
		if hc.Spec.Configuration == nil {
			hc.Spec.Configuration = &hyperv1beta1.ClusterConfiguration{}
		}
		if hc.Spec.Configuration.APIServer == nil {
			hc.Spec.Configuration.APIServer = &configv1.APIServerSpec{}
		}
		hc.Spec.Configuration.APIServer.ServingCerts.NamedCertificates = append(
			hc.Spec.Configuration.APIServer.ServingCerts.NamedCertificates,
			configv1.APIServerNamedServingCert{
				Names:              []string{opts.ServingCertHostname},
				ServingCertificate: configv1.SecretNameReference{Name: opts.ServingCertName},
			},
		)
	}

	return hc
}

// quantityPtr returns a pointer to a copy of q, because resource.Quantity
// fields in the HyperShift API are typically optional pointers.
func quantityPtr(q resource.Quantity) *resource.Quantity {
	return &q
}

// BuildNodePool constructs the default HyperShift NodePool backing a
// ClusterInstance's KubeVirt workers. replicas is the desired worker count
// for topology=hcp; see ClusterInstanceReconciler.desiredReplicas.
func BuildNodePool(instance *brokerv1alpha1.ClusterInstance, hcName, namespace string, replicas int32) *hyperv1beta1.NodePool {
	tmpl := instance.Spec.Template

	size := tmpl.RootVolumeSize
	if size == "" {
		size = defaultNodePoolRootVolumeSize
	}
	rootVolSize := resource.MustParse(size)

	cores := uint32(tmpl.Cores)
	memory := resource.MustParse(tmpl.Memory)

	kubevirtPlatform := &hyperv1beta1.KubevirtNodePoolPlatform{
		RootVolume: &hyperv1beta1.KubevirtRootVolume{
			KubevirtVolume: hyperv1beta1.KubevirtVolume{
				Type: hyperv1beta1.KubevirtVolumeTypePersistent,
				Persistent: &hyperv1beta1.KubevirtPersistentVolume{
					Size: &rootVolSize,
				},
			},
		},
		Compute: &hyperv1beta1.KubevirtCompute{
			Memory: &memory,
			Cores:  &cores,
		},
		NodeSelector: tmpl.VMNodeSelector,
	}
	if tmpl.StorageClassName != "" {
		sc := tmpl.StorageClassName
		kubevirtPlatform.RootVolume.Persistent.StorageClass = &sc
	}

	return &hyperv1beta1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NodePoolName(instance.Name),
			Namespace: namespace,
			Labels:    CommonLabels(instance),
		},
		Spec: hyperv1beta1.NodePoolSpec{
			ClusterName: hcName,
			Release: hyperv1beta1.Release{
				Image: tmpl.ReleaseImage,
			},
			Platform: hyperv1beta1.NodePoolPlatform{
				Type:     hyperv1beta1.KubevirtPlatform,
				Kubevirt: kubevirtPlatform,
			},
			Management: hyperv1beta1.NodePoolManagement{
				UpgradeType: hyperv1beta1.UpgradeTypeReplace,
			},
			Replicas: &replicas,
		},
	}
}

// HostedClusterAvailable reports whether hc's Available condition is True.
func HostedClusterAvailable(hc *hyperv1beta1.HostedCluster) bool {
	return apimeta.IsStatusConditionTrue(hc.Status.Conditions, string(hyperv1beta1.HostedClusterAvailable))
}

// HostedControlPlaneNamespace returns the namespace HyperShift creates to
// hold the actual control-plane workloads (etcd, kube-apiserver, and so
// on) backing a HostedCluster. This namespace is distinct from the
// HostedCluster object's own namespace. It mirrors HyperShift's own naming
// convention exactly (see
// hypershift-operator/controllers/manifests.HostedControlPlaneNamespace):
// "<HostedCluster namespace>-<HostedCluster name>", with any dots in the
// name replaced by hyphens. This replacement is irrelevant here in
// practice, since ClusterInstance names are always valid Kubernetes object
// names and never contain dots.
func HostedControlPlaneNamespace(namespace, instanceName string) string {
	return namespace + "-" + strings.ReplaceAll(instanceName, ".", "-")
}

// hostedClusterKASServiceName is the fixed name of HyperShift's own
// internal ClusterIP Service (in the HostedControlPlane namespace) that
// fronts the guest kube-apiserver, regardless of platform. It mirrors
// HyperShift's own manifests.KubeAPIServerServiceName, which lives in an
// internal/ package and is not importable here.
const hostedClusterKASServiceName = "kube-apiserver"

// hostedClusterKASPort is the fixed port HyperShift's own internal
// "kube-apiserver" ClusterIP Service (in the HostedControlPlane namespace)
// listens on, regardless of platform. See HyperShift's config.KASSVCPort.
const hostedClusterKASPort = 6443

// HostedClusterAPIRouteName is the deterministic name of the Route
// BuildHostedClusterAPIRoute creates.
func HostedClusterAPIRouteName(instanceName string) string {
	return instanceName + "-api"
}

// BuildHostedClusterAPIRoute constructs a passthrough Route, in the
// HostedControlPlane namespace (see HostedControlPlaneNamespace), that
// fronts HyperShift's own internal "kube-apiserver" Service with a stable,
// externally-routable hostname via the management cluster's normal shared
// ingress router. This is the same mechanism CRC's BuildCRCAPIRoute
// already uses successfully. It is deliberately independent of
// HyperShift's own Services[APIServer] publishing strategy (see
// BuildHostedCluster's NodePort choice and its doc comment for why
// Type=Route there would otherwise provision a separate, often
// slow/unreachable dedicated LoadBalancer).
//
// host is the operator's own externally-routable admin hostname
// (api-<instance>.<mgmt-ingress-domain>, computed by the caller; see
// ClusterInstanceReconciler.mgmtIngressDomain/ensureCRCAPIRoute for the
// same convention on the CRC path). Unlike an earlier revision of this
// code, host is deliberately not the same value passed to
// BuildHostedCluster's nodePortAddress parameter (see that function's doc
// comment for why conflating the two broke worker bootstrap), so
// this hostname is not covered by a DNS SAN on the KAS's default serving
// certificate. Because this Route uses passthrough termination, though,
// the client's SNI hostname reaches the guest kube-apiserver untouched.
// BuildHostedCluster's servingCertName/servingCertHostname parameters
// therefore wire in a dedicated NamedCertificate for this exact hostname,
// which the KAS serves via SNI, closing that gap without needing
// --insecure-skip-tls-verify.
//
// Passthrough termination is required, not edge or reencrypt, for the same
// reason as BuildCRCAPIRoute: mTLS client-certificate authentication and
// SPDY/websocket upgrades (exec, logs, port-forward) only work if the
// router forwards the raw TLS stream to the guest API server untouched.
func BuildHostedClusterAPIRoute(instance *brokerv1alpha1.ClusterInstance, host, hcpNamespace string) *routev1.Route {
	weight := int32(100)
	return &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HostedClusterAPIRouteName(instance.Name),
			Namespace: hcpNamespace,
			Labels:    CommonLabels(instance),
		},
		Spec: routev1.RouteSpec{
			Host: host,
			To: routev1.RouteTargetReference{
				Kind:   "Service",
				Name:   hostedClusterKASServiceName,
				Weight: &weight,
			},
			Port: &routev1.RoutePort{
				TargetPort: intstr.FromInt32(hostedClusterKASPort),
			},
			TLS: &routev1.TLSConfig{
				Termination:                   routev1.TLSTerminationPassthrough,
				InsecureEdgeTerminationPolicy: routev1.InsecureEdgeTerminationPolicyNone,
			},
		},
	}
}
