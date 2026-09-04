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

// Package main - cluster.go
//
// This file holds cluster-level fixups for the CRC guest cluster. It
// performs them through typed Kubernetes/OpenShift clients that connect
// through the SSH tunnel (see clusterclient.go). These fixups run after
// guest.go's bootstrapCA has produced a valid admin cert, so the API
// server is reachable and trusted.
//
// Ported/adapted from:
//   - github.com/crc-org/crc pkg/crc/cluster/cluster.go
//   - github.com/crc-org/crc pkg/crc/cluster/csr.go + cert_renewal.go
//   - github.com/crc-org/crc pkg/crc/cluster/kubeadmin_password.go
//   - github.com/crc-org/crc-cloud pkg/bundle/setup/clustersetup.sh
//     (create_certificate_and_patch_secret, patch_ingress_config,
//     patch_api_server, patch_default_route, wait_cluster_become_healthy)
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/caxu-rh/guestcluster-operator/internal/resources"
)

const (
	// kubelet signer names per k8s.io/api/certificates/v1
	kubeletClientSigner  = "kubernetes.io/kube-apiserver-client-kubelet"
	kubeletServingSigner = "kubernetes.io/kubelet-serving"

	// crc-owned htpasswd users (matches crc's resolveUserPasswords)
	userKubeadmin = "kubeadmin"
	userDeveloper = "developer"

	// defaultDeveloperPassword matches crc's constants.DefaultDeveloperPassword
	defaultDeveloperPassword = "developer"

	// pollInterval / pollTimeout used for cluster health waits.
	pollInterval = 10 * time.Second
	pollTimeout  = 20 * time.Minute

	// configOpenshiftIOGroup is the API group for the cluster-scoped
	// config.openshift.io singleton resources (ingresses, apiservers,
	// clusteroperators, clusterversions) patched/read below.
	configOpenshiftIOGroup = "config.openshift.io"

	// specKey/nameKey are the map keys used repeatedly when building the
	// unstructured JSON merge-patch payloads below.
	specKey = "spec"
	nameKey = "name"

	// externalAPICertSecretName is the TLS Secret holding the self-signed
	// external-API serving certificate created by applyExternalAPIPatches.
	externalAPICertSecretName = "external-api-serving-cert"

	// externalAPICertSecretNamespace is where externalAPICertSecretName
	// must live. OpenShift requires
	// apiserver.spec.servingCerts.namedCertificates' servingCertificate
	// secrets to be present in openshift-config, and nowhere else: the
	// kube-apiserver-operator's config-observer looks only there. This is
	// not the same namespace ingress componentRoutes use, despite the
	// superficially similar shape.
	externalAPICertSecretNamespace = "openshift-config"
)

// clusterFixupConfig holds parameters the cluster fixups need beyond what is
// in the global config struct.
type clusterFixupConfig struct {
	// KubeadminPassword is the password to set for the kubeadmin user.
	// If empty, a random installer-style password is generated.
	KubeadminPassword string
	// DeveloperPassword defaults to defaultDeveloperPassword if empty.
	DeveloperPassword string
	// PullSecretJSON is the raw pull-secret JSON content.
	PullSecretJSON []byte
	// APIHostname is the externally-routable hostname (the management
	// cluster's passthrough Route host, see
	// internal/resources.BuildCRCAPIRoute) used for the external API
	// serving cert + apiserver namedCertificate patch.
	APIHostname string
	// Identity is the management-side certificate material that remains stable
	// when the backing VMI is replaced.
	Identity resources.CRCIdentity
}

// ClusterFixupResult is what a successful RunClusterFixups call returns.
type ClusterFixupResult struct {
	// OCPVersion is the OpenShift version read from ClusterVersion.
	OCPVersion string
	// ExternalAPICACertPEM is the self-signed external-API serving
	// certificate (it is its own CA/trust root), suitable for embedding as
	// certificate-authority-data in a kubeconfig whose server has been
	// rewritten to the externally-routable API hostname.
	ExternalAPICACertPEM []byte
}

// RunClusterFixups performs all cluster-level mutations after the CA bootstrap.
//
// Steps (in order):
//  1. Wait for the API server to respond.
//  2. Approve pending kubelet CSRs.
//  3. Inject the real pull secret.
//  4. Update kubeadmin + developer passwords via htpasswd.
//  5. Create the external API serving cert secret + patch apiserver config
//     (namedCertificates) so the API server presents it for the Route host.
//  6. Wait for key cluster operators to become Available.
//  7. Read OCP version.
func RunClusterFixups(
	ctx context.Context,
	clients *GuestClients,
	cfg clusterFixupConfig,
	log logrLike,
) (*ClusterFixupResult, error) {
	// 1. Wait for API server
	log.Info("cluster: waiting for API server")
	if err := waitForAPIServer(ctx, clients, log); err != nil {
		return nil, fmt.Errorf("wait for API server: %w", err)
	}

	// 2. Approve pending kubelet CSRs
	log.Info("cluster: approving pending kubelet CSRs")
	if err := approvePendingCSRs(ctx, clients); err != nil {
		return nil, fmt.Errorf("approve CSRs: %w", err)
	}

	// 3. Inject pull secret
	log.Info("cluster: injecting pull secret")
	if err := injectPullSecret(ctx, clients, cfg.PullSecretJSON); err != nil {
		return nil, fmt.Errorf("inject pull secret: %w", err)
	}

	// 4. Update passwords
	log.Info("cluster: updating cluster passwords")
	kubeadminPass := cfg.KubeadminPassword
	if kubeadminPass == "" {
		kubeadminPass = GenerateRandomPassword()
	}
	devPass := cfg.DeveloperPassword
	if devPass == "" {
		devPass = defaultDeveloperPassword
	}
	if err := updatePasswords(ctx, clients, kubeadminPass, devPass); err != nil {
		return nil, fmt.Errorf("update passwords: %w", err)
	}

	// 5. External API serving cert + apiserver namedCertificate patch
	log.Info("cluster: applying external API patches", "apiHostname", cfg.APIHostname)
	externalAPICACertPEM, err := applyExternalAPIPatches(
		ctx, clients, cfg.APIHostname, cfg.Identity.ServingCert, cfg.Identity.ServingPrivateKey,
	)
	if err != nil {
		return nil, fmt.Errorf("external API patches: %w", err)
	}

	// 6. Wait for cluster operators
	log.Info("cluster: waiting for cluster operators to become available")
	if err := waitForClusterOperators(ctx, clients, log); err != nil {
		return nil, fmt.Errorf("wait for cluster operators: %w", err)
	}

	// 7. OCP version
	log.Info("cluster: reading OCP version")
	version, err := readOCPVersion(ctx, clients, log)
	if err != nil {
		return nil, fmt.Errorf("read OCP version: %w", err)
	}
	return &ClusterFixupResult{OCPVersion: version, ExternalAPICACertPEM: externalAPICACertPEM}, nil
}

// ---------------------------------------------------------------------------
// API server readiness
// ---------------------------------------------------------------------------

func waitForAPIServer(ctx context.Context, clients *GuestClients, log logrLike) error {
	var lastErr error
	var attempts int
	pollErr := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		_, err := clients.Core.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
		if err != nil {
			lastErr = err
			attempts++
			// Log periodically (not every attempt) so a slow-to-start API
			// server doesn't produce excessive log spam, but the underlying
			// error is still visible if this ends up timing out.
			if attempts == 1 || attempts%6 == 0 {
				log.Info("cluster: still waiting for API server", "attempts", attempts, "lastError", err.Error())
			}
			return false, nil
		}
		return true, nil
	})
	if pollErr != nil {
		if lastErr != nil {
			return fmt.Errorf("%w (last error: %v)", pollErr, lastErr)
		}
		return pollErr
	}
	return nil
}

// ---------------------------------------------------------------------------
// CSR approval
// ---------------------------------------------------------------------------

// approvePendingCSRs lists all CSRs and approves those signed by the kubelet
// client or serving signers that are still pending (no Approved/Denied
// condition).  Ported from crc pkg/crc/cluster/csr.go approvePendingCSRs.
func approvePendingCSRs(ctx context.Context, clients *GuestClients) error {
	csrs, err := clients.Core.CertificatesV1().CertificateSigningRequests().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing CSRs: %w", err)
	}
	for i := range csrs.Items {
		csr := &csrs.Items[i]
		if csr.Spec.SignerName != kubeletClientSigner && csr.Spec.SignerName != kubeletServingSigner {
			continue
		}
		if !isPendingCSR(csr) {
			continue
		}
		csr.Status.Conditions = append(csr.Status.Conditions, certsv1.CertificateSigningRequestCondition{
			Type:           certsv1.CertificateApproved,
			Status:         corev1.ConditionTrue,
			Reason:         "CRCAgentApproval",
			Message:        "Approved by crc-agent during CRC cluster setup",
			LastUpdateTime: metav1.Now(),
		})
		if _, err := clients.Core.CertificatesV1().CertificateSigningRequests().
			UpdateApproval(ctx, csr.Name, csr, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("approving CSR %s: %w", csr.Name, err)
		}
	}
	return nil
}

func isPendingCSR(csr *certsv1.CertificateSigningRequest) bool {
	for _, c := range csr.Status.Conditions {
		if c.Type == certsv1.CertificateApproved || c.Type == certsv1.CertificateDenied {
			return false
		}
	}
	return len(csr.Status.Certificate) == 0
}

// ---------------------------------------------------------------------------
// Pull secret
// ---------------------------------------------------------------------------

// injectPullSecret patches the pull-secret Secret in openshift-config.
// Ported from crc EnsurePullSecretPresentInTheCluster.
func injectPullSecret(ctx context.Context, clients *GuestClients, pullSecretJSON []byte) error {
	b64 := base64.StdEncoding.EncodeToString(pullSecretJSON)
	patch := fmt.Sprintf(`{"data":{".dockerconfigjson":%q}}`, b64)
	_, err := clients.Core.CoreV1().Secrets("openshift-config").
		Patch(ctx, "pull-secret", types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// ---------------------------------------------------------------------------
// Passwords
// ---------------------------------------------------------------------------

// updatePasswords reads the current htpass-secret, preserves external users,
// and replaces kubeadmin + developer with freshly bcrypt-hashed passwords.
// Ported from crc UpdateUserPasswords.
func updatePasswords(ctx context.Context, clients *GuestClients, kubeadminPass, developerPass string) error {
	// Read existing secret (may not exist yet on a freshly prepared bundle).
	existing := ""
	sec, err := clients.Core.CoreV1().Secrets("openshift-config").
		Get(ctx, "htpass-secret", metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get htpass-secret: %w", err)
	}
	if sec != nil {
		existing = string(sec.Data["htpasswd"])
	}

	externalLines := ParseExternalHtpasswdLines(existing, []string{userKubeadmin, userDeveloper})
	htpasswd, err := BuildHtpasswd(map[string]string{
		userKubeadmin: kubeadminPass,
		userDeveloper: developerPass,
	}, externalLines)
	if err != nil {
		return fmt.Errorf("building htpasswd: %w", err)
	}

	patch := fmt.Sprintf(`{"data":{"htpasswd":%q}}`, htpasswd)
	_, err = clients.Core.CoreV1().Secrets("openshift-config").
		Patch(ctx, "htpass-secret", types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		// Secret doesn't exist yet; create it.
		newSec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "htpass-secret",
				Namespace: "openshift-config",
			},
			Data: map[string][]byte{"htpasswd": []byte(htpasswd)},
		}
		_, err = clients.Core.CoreV1().Secrets("openshift-config").Create(ctx, newSec, metav1.CreateOptions{})
	}
	return err
}

// ---------------------------------------------------------------------------
// External API access (cert + apiserver namedCertificate)
// ---------------------------------------------------------------------------

// applyExternalAPIPatches installs the stable serving certificate for the
// externally routable API hostname (the management cluster's
// passthrough Route; see internal/resources.BuildCRCAPIRoute). It publishes
// the certificate as a TLS secret (see externalAPICertSecretName), and it
// patches the apiserver config so the API server presents that
// certificate for the Route's SNI hostname. The function returns the
// certificate's PEM bytes. Because the cert is self-signed, it also
// doubles as its own CA/trust root, so the caller can embed it as
// certificate-authority-data in the externally routable kubeconfig instead
// of disabling TLS verification.
//
// This function does not patch ingress config or the image-registry default
// route on purpose. This operator exposes only the guest API server
// externally (through a Route; see internal/resources.BuildCRCAPIRoute),
// not the guest's web console, apps, or image registry. The guest's own
// VMI network is not routable from outside the management cluster for
// arbitrary wildcard hostnames the way a single API hostname is.
func applyExternalAPIPatches(
	ctx context.Context, clients *GuestClients, apiHostname string, certPEM, keyPEM []byte,
) ([]byte, error) {
	if err := createOrUpdateTLSSecret(
		ctx, clients, externalAPICertSecretNamespace, externalAPICertSecretName, certPEM, keyPEM,
	); err != nil {
		return nil, fmt.Errorf("external API serving cert secret: %w", err)
	}
	if err := patchAPIServerConfig(ctx, clients, apiHostname); err != nil {
		return nil, fmt.Errorf("patch apiserver config: %w", err)
	}
	return certPEM, nil
}

// createOrUpdateTLSSecret creates or replaces a TLS secret.
func createOrUpdateTLSSecret(ctx context.Context, clients *GuestClients, ns, name string, cert, key []byte) error {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       cert,
			corev1.TLSPrivateKeyKey: key,
		},
	}
	_, err := clients.Core.CoreV1().Secrets(ns).Create(ctx, sec, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := clients.Core.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if existing.Type == sec.Type &&
			bytes.Equal(existing.Data[corev1.TLSCertKey], cert) &&
			bytes.Equal(existing.Data[corev1.TLSPrivateKeyKey], key) {
			return nil
		}
		existing.Data, existing.Type = sec.Data, sec.Type
		_, err = clients.Core.CoreV1().Secrets(ns).Update(ctx, existing, metav1.UpdateOptions{})
	}
	return err
}

// patchAPIServerConfig patches apiserver.config.openshift.io/cluster to add
// a namedCertificate for the externally-routable API hostname.
// Ported from crc-cloud patch_api_server.
func patchAPIServerConfig(ctx context.Context, clients *GuestClients, apiHostname string) error {
	gvr := schema.GroupVersionResource{
		Group:    configOpenshiftIOGroup,
		Version:  "v1",
		Resource: "apiservers",
	}
	patch := map[string]interface{}{
		specKey: map[string]interface{}{
			"servingCerts": map[string]interface{}{
				"namedCertificates": []interface{}{
					map[string]interface{}{
						"names": []interface{}{apiHostname},
						"servingCertificate": map[string]interface{}{
							nameKey: externalAPICertSecretName,
						},
					},
				},
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = clients.Dynamic.Resource(gvr).Patch(ctx, "cluster", types.MergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

// ---------------------------------------------------------------------------
// Cluster operator health wait
// ---------------------------------------------------------------------------

// clusterOperatorsToWatch are the ones crc-cloud's wait_cluster_become_healthy
// monitors for the final health check. crc-agent applies a less strict
// check: it needs the cluster up enough for the kubeconfig to be usable.
var clusterOperatorsToWatch = []string{
	"authentication",
	"console",
	"etcd",
	"ingress",
	"openshift-apiserver",
}

// waitForClusterOperators polls until each watched operator has Available=True.
func waitForClusterOperators(ctx context.Context, clients *GuestClients, log logrLike) error {
	gvr := schema.GroupVersionResource{
		Group:    configOpenshiftIOGroup,
		Version:  "v1",
		Resource: "clusteroperators",
	}
	var attempts int
	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		attempts++
		var notReady []string
		for _, name := range clusterOperatorsToWatch {
			obj, err := clients.Dynamic.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				notReady = append(notReady, name+" (get error: "+err.Error()+")")
				continue
			}
			conditions, _ := unstructuredConditions(obj.Object)
			if !isOperatorAvailable(conditions) {
				notReady = append(notReady, name)
			}
		}
		if len(notReady) > 0 {
			// Log periodically (not every attempt) so a slow-to-settle
			// cluster doesn't produce excessive log spam, but which
			// operators are blocking is still visible if this times out.
			if attempts == 1 || attempts%6 == 0 {
				log.Info("cluster: still waiting for cluster operators", "attempts", attempts, "notReady", notReady)
			}
			return false, nil
		}
		return true, nil
	})
}

// ---------------------------------------------------------------------------
// OCP version
// ---------------------------------------------------------------------------

// readOCPVersion retries reading ClusterVersion.status.desired.version.
// Earlier in RunClusterFixups, applyExternalAPIPatches's named-certificate
// patch runs before waitForClusterOperators. That patch can trigger a
// kube-apiserver-operator revision rollout. The rollout can transiently
// drop connectivity again, even after cluster operators already reported
// Available. A single unretried Get here can race that blip. This blip
// shows up as a "connection refused" error over the SSH tunnel. This
// function applies the same retry-until-timeout treatment that
// waitForAPIServer applies to the same class of transient error.
func readOCPVersion(ctx context.Context, clients *GuestClients, log logrLike) (string, error) {
	gvr := schema.GroupVersionResource{
		Group:    configOpenshiftIOGroup,
		Version:  "v1",
		Resource: "clusterversions",
	}
	var version string
	var lastErr error
	var attempts int
	pollErr := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		obj, err := clients.Dynamic.Resource(gvr).Get(ctx, "version", metav1.GetOptions{})
		switch {
		case err != nil:
			lastErr = fmt.Errorf("get clusterversion: %w", err)
		default:
			status, ok := obj.Object["status"].(map[string]interface{})
			if !ok {
				lastErr = fmt.Errorf("clusterversion has no status")
				break
			}
			desired, ok := status["desired"].(map[string]interface{})
			if !ok {
				lastErr = fmt.Errorf("clusterversion status has no desired")
				break
			}
			v, _ := desired["version"].(string)
			if v == "" {
				lastErr = fmt.Errorf("clusterversion status.desired.version is empty")
				break
			}
			version = v
			return true, nil
		}
		attempts++
		if attempts == 1 || attempts%6 == 0 {
			log.Info("cluster: still waiting to read OCP version", "attempts", attempts, "lastError", lastErr.Error())
		}
		return false, nil
	})
	if pollErr != nil {
		if lastErr != nil {
			return "", fmt.Errorf("%w (last error: %v)", pollErr, lastErr)
		}
		return "", pollErr
	}
	return version, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// GenerateRandomPassword generates a random installer-style password of the
// form XXXXX-XXXXX-XXXXX-XXXXX.  Ported from crc GenerateRandomPasswordHash.
func GenerateRandomPassword() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const groupLen = 5
	const groups = 4
	var sb strings.Builder
	b := make([]byte, groupLen*groups)
	if _, err := rand.Read(b); err != nil {
		// Extremely unlikely; fall back to a deterministic default rather than panic.
		return "changeme-changeme-changeme-changeme"
	}
	for i, byt := range b {
		if i > 0 && i%groupLen == 0 {
			sb.WriteByte('-')
		}
		sb.WriteByte(chars[int(byt)%len(chars)])
	}
	return sb.String()
}

// unstructuredConditions extracts a []map[string]interface{} from
// obj["status"]["conditions"]. The bool result reports whether obj["status"]
// was present and shaped as expected.
func unstructuredConditions(obj map[string]interface{}) ([]map[string]interface{}, bool) {
	status, ok := obj["status"].(map[string]interface{})
	if !ok {
		return nil, false
	}
	raw, ok := status["conditions"].([]interface{})
	if !ok {
		return nil, true
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for _, c := range raw {
		if m, ok := c.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out, true
}

// isOperatorAvailable returns true if the conditions contain Available=True.
func isOperatorAvailable(conditions []map[string]interface{}) bool {
	for _, c := range conditions {
		t, _ := c["type"].(string)
		s, _ := c["status"].(string)
		if t == "Available" && s == "True" {
			return true
		}
	}
	return false
}
