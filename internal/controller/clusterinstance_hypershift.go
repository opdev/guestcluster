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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	routev1 "github.com/openshift/api/route/v1"
	hyperv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"

	brokerv1alpha1 "github.com/caxu-rh/guestcluster-operator/api/v1alpha1"
	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

// hypershiftResult carries the outcome of reconciling a topology=hcp
// instance's backing objects.
type hypershiftResult struct {
	ready             bool
	ocpVersion        string
	apiEndpoint       string
	kubeconfig        []byte
	hostedClusterName string
	nodePoolNames     []string
}

// managementNodeAddress returns the InternalIP of a Ready, schedulable node
// on the management cluster, to use as Services[APIServer].NodePort.Address
// in BuildHostedCluster.
//
// This address must be reachable on the raw NodePort number.
// apiServerHostname (the "api-<instance>.<mgmt-ingress-domain>" name used
// for this operator's own admin Route) resolves to the shared ingress
// router, which only proxies ports 80/443. A NodePort Service differs: it
// is exposed on *every* node's host IP at that port by kube-proxy,
// cluster-wide, regardless of which node backs the pod. Testing
// found this to be a hard requirement, not a nice-to-have. HyperShift's own
// generated worker bootstrap ignition embeds
// Services[APIServer].NodePort.Address:<assigned-nodeport> verbatim as the
// kubelet bootstrap kubeconfig's server URL (see
// kas.ReconcileServiceStatus's NodePort case). Pointing this at the router
// hostname left every worker's kubelet permanently unable to reach the API
// server at all (confirmed via direct in-guest journalctl: "No valid
// client certificate is found but the server is not responsive"). As a
// result, no CSR was ever filed, and the NodePool never reached its
// desired replica count.
//
// Any single node satisfies NodePort semantics equally well, because
// kube-proxy forwards from whichever node receives the connection. This
// function uses the first Ready, schedulable node found; the operator does
// not track node churn for this address after HostedCluster creation.
func (r *ClusterInstanceReconciler) managementNodeAddress(ctx context.Context) (string, error) {
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return "", fmt.Errorf("listing management cluster nodes: %w", err)
	}
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if node.Spec.Unschedulable {
			continue
		}
		ready := false
		for _, c := range node.Status.Conditions {
			if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
				ready = true
				break
			}
		}
		if !ready {
			continue
		}
		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP && addr.Address != "" {
				return addr.Address, nil
			}
		}
	}
	return "", fmt.Errorf("no Ready, schedulable management cluster node with an InternalIP found")
}

// desiredReplicas returns the worker NodePool replica count for instance:
// Template.NodePoolReplicas if set (and >=1, enforced by the CRD), otherwise
// a default of 1.
func desiredReplicas(instance *brokerv1alpha1.ClusterInstance) int32 {
	if r := instance.Spec.Template.NodePoolReplicas; r != nil && *r >= 1 {
		return *r
	}
	return 1
}

// resolveHCPWorkerSSHKey copies ClusterTemplate.HCPWorkerSSHKeyRef (if set)
// into targetNamespace, under resources.HCPWorkerSSHKeyName, and validates
// that it carries the data key HyperShift itself requires. targetNamespace
// must be the HostedCluster's own namespace, because HostedCluster.spec.sshKey
// is a LocalObjectReference resolved relative to the HostedCluster itself;
// a Secret living in the operator's namespace cannot be referenced
// directly.
//
// Unlike resolvePullSecret, there is no cluster-wide default to fall back
// to: this is an opt-in debugging convenience (see HCPWorkerSSHKeyRef's doc
// comment). An unset ref returns "" rather than an error, and
// BuildHostedCluster then leaves HostedCluster.spec.sshKey unset.
func (r *ClusterInstanceReconciler) resolveHCPWorkerSSHKey(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, targetNamespace string) (string, error) {
	ref := instance.Spec.Template.HCPWorkerSSHKeyRef
	if ref == nil || ref.Name == "" {
		return "", nil
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: instance.Namespace}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("HCP worker SSH key secret %s/%s not found", instance.Namespace, ref.Name)
		}
		return "", fmt.Errorf("getting HCP worker SSH key secret %s/%s: %w", instance.Namespace, ref.Name, err)
	}
	data, ok := secret.Data[resources.HCPWorkerSSHKeyDataKey]
	if !ok || len(data) == 0 {
		return "", fmt.Errorf("HCP worker SSH key secret %s/%s is missing data key %q", instance.Namespace, ref.Name, resources.HCPWorkerSSHKeyDataKey)
	}

	copyName := resources.HCPWorkerSSHKeyName(instance.Name)
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      copyName,
			Namespace: targetNamespace,
			Labels:    resources.CommonLabels(instance),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{resources.HCPWorkerSSHKeyDataKey: data},
	}

	changed := func(existing *corev1.Secret) bool {
		return !bytes.Equal(existing.Data[resources.HCPWorkerSSHKeyDataKey], data)
	}
	if err := r.upsertSecret(ctx, desired, changed); err != nil {
		return "", err
	}

	return copyName, nil
}

// ensureKASServingCert gets or creates the kubernetes.io/tls Secret (in
// targetNamespace, the HostedCluster's own namespace) that holds a
// self-signed certificate valid for hostname (this operator's
// externally-routable admin hostname; see BuildHostedClusterAPIRoute). It
// regenerates the certificate via resources.GenerateAPIServerServingCert
// whenever resources.ServingCertNeedsRegen reports the certificate missing,
// no longer valid for hostname, or nearing expiry. ensureKASServingCert
// returns the Secret's name, to wire into resources.BuildHostedCluster's
// servingCertName parameter, and the certificate's PEM bytes, to embed as
// certificate-authority-data in the published admin kubeconfig (see
// resources.RewriteKubeconfigServer).
func (r *ClusterInstanceReconciler) ensureKASServingCert(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, targetNamespace, hostname string) (secretName string, certPEM []byte, err error) {
	log := logf.FromContext(ctx)
	secretName = resources.KASServingCertName(instance.Name)

	existing := &corev1.Secret{}
	getErr := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: targetNamespace}, existing)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return "", nil, fmt.Errorf("getting KAS serving cert secret %s/%s: %w", targetNamespace, secretName, getErr)
	}

	if getErr == nil && !resources.ServingCertNeedsRegen(existing.Data[corev1.TLSCertKey], hostname) {
		return secretName, existing.Data[corev1.TLSCertKey], nil
	}

	certPEM, keyPEM, genErr := resources.GenerateAPIServerServingCert(hostname)
	if genErr != nil {
		return "", nil, fmt.Errorf("generating KAS serving certificate for %s: %w", hostname, genErr)
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: targetNamespace,
			Labels:    resources.CommonLabels(instance),
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}

	// Unlike upsertSecret's callers, ensureKASServingCert already decided
	// above (via ServingCertNeedsRegen, against the Get result already in
	// hand) that a regen is needed. So this applies the create/update
	// directly rather than through upsertSecret, which would re-Get first.
	if apierrors.IsNotFound(getErr) {
		if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", nil, fmt.Errorf("creating KAS serving cert secret %s/%s: %w", targetNamespace, secretName, err)
		}
		log.Info("created KAS serving certificate", "secret", secretName, "hostname", hostname)
	} else {
		existing.Type = corev1.SecretTypeTLS
		existing.Data = desired.Data
		if err := r.Update(ctx, existing); err != nil {
			return "", nil, fmt.Errorf("updating KAS serving cert secret %s/%s: %w", targetNamespace, secretName, err)
		}
		log.Info("regenerated KAS serving certificate", "secret", secretName, "hostname", hostname)
	}

	return secretName, certPEM, nil
}

// ensureHyperShiftBacking creates the HostedCluster and its default
// NodePool that back a topology=hcp ClusterInstance, if they do not already
// exist, and reports readiness once the HostedCluster is Available and the
// NodePool has reached its desired replica count. Unlike the CRC path, and
// aside from immutable-spec fields, this function does *patch* the
// NodePool's replica count. This lets a ClusterPool template change (for
// example, scaling hcp workers) reflect without a full teardown and
// recreate of the instance.
func (r *ClusterInstanceReconciler) ensureHyperShiftBacking(ctx context.Context, instance *brokerv1alpha1.ClusterInstance, pullSecretName string) (hypershiftResult, error) {
	log := logf.FromContext(ctx)
	namespace := resources.DefaultHostedClusterNamespace
	res := hypershiftResult{
		hostedClusterName: resources.HostedClusterName(instance.Name),
		nodePoolNames:     []string{resources.NodePoolName(instance.Name)},
	}

	domain, err := r.mgmtIngressDomain(ctx)
	if err != nil {
		return res, fmt.Errorf("resolving management cluster ingress domain: %w", err)
	}
	apiServerHostname := resources.APIServerHostname(instance.Name, domain)

	// Get the HostedCluster first, before resolving any of the other
	// inputs BuildHostedCluster needs. Those inputs (the management node
	// address for NodePort.Address, and the worker SSH key copy) are
	// consumed only at HostedCluster creation time and never revisited
	// afterward. So once the HostedCluster exists, later reconciles that
	// poll for readiness do not need to pay for a cluster-wide Node List
	// or a Secret Get/copy on every pass.
	hcName := resources.HostedClusterName(instance.Name)
	existingHC := &hyperv1beta1.HostedCluster{}
	getHCErr := r.Get(ctx, types.NamespacedName{Name: hcName, Namespace: namespace}, existingHC)
	if getHCErr != nil && !apierrors.IsNotFound(getHCErr) {
		return res, fmt.Errorf("getting HostedCluster %s/%s: %w", namespace, hcName, getHCErr)
	}

	if apierrors.IsNotFound(getHCErr) {
		nodePortAddress, err := r.managementNodeAddress(ctx)
		if err != nil {
			return res, fmt.Errorf("resolving NodePort.Address for HostedCluster APIServer service: %w", err)
		}
		sshKeySecretName, err := r.resolveHCPWorkerSSHKey(ctx, instance, namespace)
		if err != nil {
			return res, fmt.Errorf("resolving HCP worker SSH key: %w", err)
		}
		servingCertName, _, err := r.ensureKASServingCert(ctx, instance, namespace, apiServerHostname)
		if err != nil {
			return res, fmt.Errorf("ensuring KAS serving certificate: %w", err)
		}

		hc := resources.BuildHostedCluster(instance, resources.HostedClusterOptions{
			Namespace:           namespace,
			PullSecretName:      pullSecretName,
			NodePortAddress:     nodePortAddress,
			ServingCertName:     servingCertName,
			ServingCertHostname: apiServerHostname,
			SSHKeySecretName:    sshKeySecretName,
		})
		if err := r.Create(ctx, hc); err != nil && !apierrors.IsAlreadyExists(err) {
			return res, fmt.Errorf("creating HostedCluster %s/%s: %w", hc.Namespace, hc.Name, err)
		}
		log.Info("created HostedCluster", "hostedCluster", hc.Name)
		return res, nil // freshly created, definitely not ready yet
	}

	replicas := desiredReplicas(instance)
	np := resources.BuildNodePool(instance, existingHC.Name, namespace, replicas)
	existingNP := &hyperv1beta1.NodePool{}
	if err := r.Get(ctx, types.NamespacedName{Name: np.Name, Namespace: np.Namespace}, existingNP); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, np); err != nil && !apierrors.IsAlreadyExists(err) {
			return res, fmt.Errorf("creating NodePool %s/%s: %w", np.Namespace, np.Name, err)
		}
		log.Info("created NodePool", "nodePool", np.Name, "replicas", replicas)
		return res, nil
	} else if err != nil {
		return res, fmt.Errorf("getting NodePool %s/%s: %w", np.Namespace, np.Name, err)
	}

	if existingNP.Spec.Replicas == nil || *existingNP.Spec.Replicas != replicas {
		existingNP.Spec.Replicas = &replicas
		if err := r.Update(ctx, existingNP); err != nil {
			return res, fmt.Errorf("scaling NodePool %s/%s to %d replicas: %w", np.Namespace, np.Name, replicas, err)
		}
		log.Info("updated NodePool replica count", "nodePool", np.Name, "replicas", replicas)
	}

	if !resources.HostedClusterAvailable(existingHC) {
		log.Info("HostedCluster not yet Available", "hostedCluster", hcName)
		return res, nil
	}
	if existingNP.Status.Replicas < replicas {
		log.Info("NodePool has not reached desired replica count", "nodePool", np.Name, "current", existingNP.Status.Replicas, "desired", replicas)
		return res, nil
	}

	if existingHC.Status.KubeConfig == nil {
		log.Info("HostedCluster Available but kubeconfig secret ref not yet published", "hostedCluster", hcName)
		return res, nil
	}

	adminSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: existingHC.Status.KubeConfig.Name, Namespace: namespace}, adminSecret); err != nil {
		return res, fmt.Errorf("getting admin kubeconfig secret %s/%s: %w", namespace, existingHC.Status.KubeConfig.Name, err)
	}
	kubeconfig, ok := adminSecret.Data[resources.KubeconfigSecretKey]
	if !ok || len(kubeconfig) == 0 {
		log.Info("admin kubeconfig secret present but missing kubeconfig key", "secret", adminSecret.Name)
		return res, nil
	}

	// Ensure our own Route fronting the guest API server. See
	// resources.BuildHostedClusterAPIRoute's doc comment for why this
	// operator manages this Route itself, targeting HyperShift's own
	// internal "kube-apiserver" Service in the HostedControlPlane
	// namespace, rather than relying on HyperShift's own
	// Services[APIServer] Route publishing. On KubeVirt, that publishing
	// path would otherwise provision a separate, often slow or unreachable
	// dedicated LoadBalancer. Unlike that LoadBalancer path, this Route's
	// host is fixed at creation time and becomes usable as soon as the
	// management cluster's router admits it, so creating it needs no
	// additional readiness gating.
	//
	// This Route is NOT a DNS SAN on the real kube-apiserver serving
	// certificate (see BuildHostedCluster's doc comment for why), hence
	// the InsecureSkipTLSVerify rewrite below.
	hcpNamespace := resources.HostedControlPlaneNamespace(namespace, instance.Name)
	apiRoute := resources.BuildHostedClusterAPIRoute(instance, apiServerHostname, hcpNamespace)
	if err := r.Get(ctx, types.NamespacedName{Name: apiRoute.Name, Namespace: apiRoute.Namespace}, &routev1.Route{}); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, apiRoute); err != nil && !apierrors.IsAlreadyExists(err) {
			return res, fmt.Errorf("creating HostedCluster API Route %s/%s: %w", apiRoute.Namespace, apiRoute.Name, err)
		}
		log.Info("created HostedCluster API Route", "route", apiRoute.Name, "host", apiServerHostname)
	} else if err != nil {
		return res, fmt.Errorf("getting HostedCluster API Route %s/%s: %w", apiRoute.Namespace, apiRoute.Name, err)
	}

	apiEndpoint := "https://" + apiServerHostname

	// This call is only needed now, to embed the certificate as
	// certificate-authority-data below. The HostedCluster is Available and
	// past all readiness gates by this point, so Reconcile reaches this
	// call at most once per instance, rather than on every reconcile while
	// still provisioning.
	_, servingCertPEM, err := r.ensureKASServingCert(ctx, instance, namespace, apiServerHostname)
	if err != nil {
		return res, fmt.Errorf("ensuring KAS serving certificate: %w", err)
	}

	// HyperShift's own admin kubeconfig embeds the NodePort.Address:port
	// combination as its server URL (see BuildHostedCluster's doc comment
	// for why that address must be a management-cluster-internal node IP,
	// not an externally-reachable one). Publishing that kubeconfig verbatim
	// would hand callers a kubeconfig that only works from inside the
	// management cluster's own network, not the externally-reachable
	// apiEndpoint this operator advertises via
	// ClusterInstanceStatus/ClusterLeaseStatus. Rewrite the kubeconfig to
	// point at our own externally-reachable admin Route instead, embedding
	// servingCertPEM (the same certificate wired into the HostedCluster
	// above) as certificate-authority-data so the rewritten kubeconfig
	// verifies normally. See RewriteKubeconfigServer's doc comment.
	rewrittenKubeconfig, err := resources.RewriteKubeconfigServer(kubeconfig, apiEndpoint, servingCertPEM)
	if err != nil {
		return res, fmt.Errorf("rewriting admin kubeconfig server for %s/%s: %w", namespace, existingHC.Status.KubeConfig.Name, err)
	}

	res.ready = true
	res.kubeconfig = rewrittenKubeconfig
	res.apiEndpoint = apiEndpoint
	if existingHC.Status.Version != nil {
		res.ocpVersion = existingHC.Status.Version.Desired.Version
	}
	return res, nil
}

// teardownHyperShiftBacking deletes the HostedCluster backing a
// topology=hcp instance (NodePools are owned by, and cascade with, the
// HostedCluster in HyperShift's own garbage collection), as part of
// ClusterInstance deletion (finalizer processing). There is no separate
// in-place "recycle" path. On lease release, ClusterLeaseReconciler deletes
// the ClusterInstance object outright, and ClusterPoolReconciler tops up a
// brand new replacement. A full destroy and recreate, going through this
// exact teardown, is the only way to guarantee no leftover cluster-scoped
// operator/CSV state survives between lease holders, matching the
// acceptance requirement.
func (r *ClusterInstanceReconciler) teardownHyperShiftBacking(ctx context.Context, instance *brokerv1alpha1.ClusterInstance) error {
	namespace := resources.DefaultHostedClusterNamespace
	name := resources.HostedClusterName(instance.Name)

	// Deleting the HostedCluster cascades, via HyperShift's own
	// controllers, to deleting its entire HostedControlPlane namespace
	// (resources.HostedControlPlaneNamespace), which takes our own
	// BuildHostedClusterAPIRoute Route down with it. So this teardown needs
	// no separate explicit deletion of that Route.
	hc := &hyperv1beta1.HostedCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	if err := r.deleteIfExists(ctx, hc, "HostedCluster"); err != nil {
		return err
	}

	// NodePool is expected to cascade-delete with its HostedCluster via
	// HyperShift's own controllers. teardownHyperShiftBacking also issues
	// an explicit delete in case reconciliation observes an intermediate
	// state (for example, HostedCluster already gone but NodePool GC still
	// in flight; either state is fine). This call is a best-effort
	// belt-and-suspenders step and tolerates NotFound.
	npName := resources.NodePoolName(instance.Name)
	np := &hyperv1beta1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: npName, Namespace: namespace}}
	if err := r.deleteIfExists(ctx, np, "NodePool"); err != nil {
		return err
	}

	// resolvePullSecret materializes the default pull-secret copy in the
	// HostedCluster's namespace only when Template.PullSecretRef is unset.
	// That copy cannot carry an owner reference across namespaces, so it is
	// not garbage-collected automatically like the same-namespace crc copy
	// is. Delete it explicitly here. This delete is a harmless no-op
	// (NotFound) when an explicit PullSecretRef was used instead, because
	// that name never collides with resources.DefaultPullSecretName.
	pullSecretName := resources.DefaultPullSecretName(instance.Name)
	pullSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: pullSecretName, Namespace: namespace}}
	if err := r.deleteIfExists(ctx, pullSecret, "default pull secret copy"); err != nil {
		return err
	}

	// The same cross-namespace reasoning as the pull secret copy above
	// applies: resolveHCPWorkerSSHKey's copy cannot carry an owner
	// reference across namespaces, so delete it explicitly. This is a
	// harmless no-op (NotFound) when HCPWorkerSSHKeyRef was never set for
	// this instance.
	sshKeyName := resources.HCPWorkerSSHKeyName(instance.Name)
	sshKeySecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: sshKeyName, Namespace: namespace}}
	if err := r.deleteIfExists(ctx, sshKeySecret, "HCP worker SSH key copy"); err != nil {
		return err
	}

	// The same cross-namespace reasoning as the pull secret and SSH key
	// copies above applies: ensureKASServingCert's Secret cannot carry an
	// owner reference across namespaces, so delete it explicitly.
	servingCertName := resources.KASServingCertName(instance.Name)
	servingCertSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: servingCertName, Namespace: namespace}}
	return r.deleteIfExists(ctx, servingCertSecret, "KAS serving certificate")
}
