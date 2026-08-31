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

// Package main - guest.go
//
// This file holds guest-side SSH fixups for a freshly booted (parked) CRC
// bundle VM. The SSH runner runs these steps as root on the guest. All
// steps must finish before crc-agent uses the typed Kubernetes client
// (clusterclient.go), for these reasons:
//
//  1. dnsmasq must start so api.crc.testing resolves inside the VM
//     (needed for `oc` commands issued over SSH).
//  2. The kubelet must start so the API server comes up.
//  3. bootstrapCA must regenerate the CA and replace the bundle's stale
//     admin client cert with a new one. This is the only step that still
//     runs oc on the guest over SSH. Typed client auth would create a
//     circular dependency: it needs a valid cert to connect, but it needs
//     to connect to replace the cert.
//
// cluster.go performs all other cluster mutations (CSR approval, pull
// secret, passwords, external API serving-cert patches, and more) through
// typed clients over the SSH tunnel.
//
// Ported from:
//   - github.com/crc-org/crc pkg/crc/services/dns/ (dnsmasq)
//   - github.com/crc-org/crc pkg/crc/machine/start.go (updateSSHKeyPair,
//     Start kubelet, EnsureGeneratedClientCAPresentInTheCluster sequencing)
//   - github.com/crc-org/crc pkg/crc/cluster/cluster.go (CA patch)
//   - github.com/crc-org/crc-cloud pkg/bundle/setup/clustersetup.sh (cred setup)
package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
	"time"

	gossh "golang.org/x/crypto/ssh"
	k8syaml "sigs.k8s.io/yaml"
)

// generateEd25519Key generates a fresh ed25519 keypair.
// Ported from crc pkg/crc/ssh/keys.go NewKeyPair.
func generateEd25519Key() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// detectGuestIP determines the IP address the guest OS itself considers its
// primary or default-route address.
//
// This address can differ from cfg.SSHHost, the externally reachable address
// crc-agent uses to SSH into the VM. On masquerade-networked KubeVirt VMs,
// the virt-launcher pod NAT-translates the pod-network IP. The guest never
// sees this address assigned to any of its own interfaces. Instead the guest
// sees an internal NAT address, conventionally 10.0.2.2 for KubeVirt's
// masquerade binding. dnsmasq's listen-address and the guest's own
// /etc/resolv.conf must reference an address the guest OS owns and can bind
// or route to itself. Otherwise the guest can never resolve its own
// *.crc.testing names: queries silently go nowhere, because the configured
// listen-address matches no real local interface.
func detectGuestIP(runner *Runner) (string, error) {
	const cmd = `ip -4 route get 1.1.1.1 2>/dev/null | ` +
		`awk '{for(i=1;i<=NF;i++) if ($i=="src") print $(i+1)}'`
	out, err := runner.Run(cmd)
	if err != nil {
		return "", fmt.Errorf("detect guest IP via ip route: %w", err)
	}
	ip := strings.TrimSpace(out)
	if ip == "" {
		return "", fmt.Errorf("could not determine guest's own IP address")
	}
	return ip, nil
}

// guestResult holds everything guest.go produces that later stages need.
type guestResult struct {
	// SSHSigner is the ed25519 key that generateAndSwapSSHKey generated and
	// swapped onto the guest. If non-nil, crc-agent must reconnect using this
	// key after the swap so subsequent SSH sessions use the new credential.
	SSHSigner gossh.Signer
	// AdminKubeconfigPEM is the patched kubeconfig (api.crc.testing:6443) with
	// the new admin client cert injected. The SSH-tunneled typed client uses
	// this as its TLS credential.
	AdminKubeconfigPEM []byte
	// CACert is the self-signed CA that bootstrapCA generates. The API server
	// trusts this CA for verifying client certificates through the
	// admin-kubeconfig-client-ca configmap (it signs ClientCertPEM below).
	// It does not sign the API server's own TLS serving certificate. See
	// ServerCAPEM for that.
	CACert *x509.Certificate
	// CAKey is the matching private key, kept for later use if needed.
	CAKey *rsa.PrivateKey
	// ClientCertPEM and ClientKeyPEM are the admin client cert and key PEM
	// bytes, signed by CACert.
	ClientCertPEM []byte
	ClientKeyPEM  []byte
	// ServerCAPEM is the CA bundle that verifies the API server's own TLS
	// serving certificate for the internal api.crc.testing SNI. bootstrapCA
	// extracts it from the bundle kubeconfig's
	// clusters[0].cluster.certificate-authority-data field, which it never
	// modifies. This is a different trust root than CACert: it is whatever
	// the CRC bundle was built with (for example kube-apiserver-lb-signer
	// and related certs), not something crc-agent generates. Typed clients
	// connecting to api.crc.testing (clusterclient.go) must use this as
	// their CAData, not CACert. Conflating the two produces "certificate
	// signed by unknown authority", because CACert never signed the
	// server's serving cert.
	ServerCAPEM []byte
}

// dnsmasqConfTemplate is the dnsmasq configuration written to the guest.
// Ported from github.com/crc-org/crc pkg/crc/services/dns/template.go.
const dnsmasqConfTemplate = `listen-address={{ .IP }}
expand-hosts
log-queries
local=/crc.testing/
domain=crc.testing
address=/apps-crc.testing/{{ .IP }}
address=/api.crc.testing/{{ .IP }}
address=/api-int.crc.testing/{{ .IP }}
`

// resolvConfDirectTemplate is written to /etc/resolv.conf as a last-resort
// fallback for bundles that predate ovs-configuration.service (see
// configureGuestDNS). It mirrors upstream crc's own
// pkg/crc/network/template.go resolvFileTemplate.
const resolvConfDirectTemplate = `# Generated by crc-agent
search crc.testing
nameserver {{ .IP }}
`

// RunGuestFixups performs all guest-root steps in order:
//  1. Write the dnsmasq config and start dnsmasq.
//  2. Rewrite /etc/resolv.conf so the VM resolves *.crc.testing.
//  3. Generate a new ed25519 SSH keypair and swap it onto authorized_keys.
//  4. Start the kubelet.
//  5. Regenerate the admin CA and client cert, then patch the cluster
//     (the CA bootstrap step; this uses oc on the guest for this one step).
//  6. Return the new kubeconfig bytes and crypto material for the
//     typed-client stage.
//
// runner must be connected with the original bundle SSH key. After
// RunGuestFixups returns a non-nil guestResult.SSHSigner, the caller must
// reconnect using that signer.
func RunGuestFixups(runner *Runner, cfg config, log logrLike) (*guestResult, error) {
	// 0. Determine the guest's own internal IP (this can differ from
	// cfg.SSHHost; see detectGuestIP's doc comment).
	guestIP, err := detectGuestIP(runner)
	if err != nil {
		return nil, fmt.Errorf("detect guest IP: %w", err)
	}
	log.Info("guest: detected internal IP", "guestIP", guestIP, "externalHost", cfg.SSHHost)

	// 1. dnsmasq
	log.Info("guest: configuring dnsmasq")
	if err := setupDnsmasq(runner, guestIP); err != nil {
		return nil, fmt.Errorf("setup dnsmasq: %w", err)
	}

	// 2. Guest DNS: point the guest at our dnsmasq for *.crc.testing.
	log.Info("guest: configuring guest DNS")
	if err := configureGuestDNS(runner, guestIP); err != nil {
		return nil, fmt.Errorf("configure guest DNS: %w", err)
	}

	// 3. SSH key swap: generate a new ed25519 keypair.
	log.Info("guest: swapping SSH key")
	newSigner, err := generateAndSwapSSHKey(runner)
	if err != nil {
		return nil, fmt.Errorf("swap SSH key: %w", err)
	}

	// 4. Start kubelet
	log.Info("guest: starting kubelet")
	if err := startKubelet(runner); err != nil {
		return nil, fmt.Errorf("start kubelet: %w", err)
	}

	// 5. CA bootstrap: generate new CA+client cert, patch the cluster
	log.Info("guest: regenerating admin CA and client cert (bootstrap)")
	res, err := bootstrapCA(runner)
	if err != nil {
		return nil, fmt.Errorf("bootstrap CA: %w", err)
	}
	res.SSHSigner = newSigner
	return res, nil
}

// setupDnsmasq writes /etc/dnsmasq.d/crc-dnsmasq.conf and enables and starts
// the dnsmasq service. Ported from crc pkg/crc/services/dns/.
func setupDnsmasq(runner *Runner, vmIP string) error {
	tmpl, err := template.New("dnsmasq").Parse(dnsmasqConfTemplate)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, map[string]string{"IP": vmIP}); err != nil {
		return err
	}
	if err := runner.CopyDataPrivileged(buf.Bytes(), "/etc/dnsmasq.d/crc-dnsmasq.conf"); err != nil {
		return fmt.Errorf("writing dnsmasq conf: %w", err)
	}
	for _, cmd := range []string{
		"systemctl enable dnsmasq",
		"systemctl start dnsmasq",
	} {
		if _, err := runner.RunPrivileged(cmd); err != nil {
			return fmt.Errorf("dnsmasq systemctl %q: %w", cmd, err)
		}
	}
	return nil
}

// configureGuestDNS points the guest at guestIP for DNS resolution, where the
// dnsmasq instance set up by setupDnsmasq answers *.crc.testing.
//
// Ported from github.com/crc-org/crc pkg/crc/network/nameservers.go
// UpdateResolvFileOnInstance. A plain one-time overwrite of /etc/resolv.conf
// does not survive on modern (OVN-based) bundles. NetworkManager owns
// /etc/resolv.conf through the ovs-configuration.service / ovs-if-br-ex
// connection it manages, and it periodically resyncs resolv.conf from the
// DHCP-provided upstream DNS server (the management cluster's own DNS,
// which has no knowledge of *.crc.testing). This silently reverts a direct
// overwrite within minutes of boot. Like upstream crc, this function
// configures DNS through NetworkManager instead of fighting it, and it
// falls back to a direct /etc/resolv.conf write only for older bundles
// that predate ovs-configuration.service.
func configureGuestDNS(runner *Runner, guestIP string) error {
	loadState, err := runner.Run("systemctl show ovs-configuration.service -p LoadState --value")
	if err != nil {
		return fmt.Errorf("checking ovs-configuration.service: %w", err)
	}
	if strings.TrimSpace(loadState) == "not-found" {
		// This old bundle lacks OVN's ovs-configuration.service / ovs-if-br-ex
		// connection, so there is nothing to configure through NetworkManager.
		// Write resolv.conf directly, as crc does for this case too.
		return writeResolvConfDirect(runner, guestIP)
	}

	if _, err := runner.RunPrivileged("systemctl start ovs-configuration.service"); err != nil {
		return fmt.Errorf("starting ovs-configuration.service: %w", err)
	}

	nmCmd := fmt.Sprintf(
		"nmcli con modify --temporary ovs-if-br-ex ipv4.dns %s ipv4.dns-search crc.testing",
		guestIP,
	)
	out, err := runner.RunPrivileged(nmCmd)
	if err != nil {
		if strings.Contains(out, "unknown connection") {
			// The ovs-if-br-ex NetworkManager connection exists only when
			// OVN's ovs-configuration builds the br-ex bridge. Bundles that
			// use a different CNI lack it, so fall back to a direct write.
			return writeResolvConfDirect(runner, guestIP)
		}
		return fmt.Errorf("nmcli con modify ovs-if-br-ex: %w", err)
	}
	if _, err := runner.RunPrivileged("systemctl restart NetworkManager.service"); err != nil {
		return fmt.Errorf("restarting NetworkManager: %w", err)
	}
	return nil
}

// writeResolvConfDirect writes /etc/resolv.conf directly. Code uses this
// only as a fallback for bundles without ovs-configuration.service. See
// configureGuestDNS's doc comment for why this is not the primary mechanism.
func writeResolvConfDirect(runner *Runner, guestIP string) error {
	tmpl, err := template.New("resolvconf").Parse(resolvConfDirectTemplate)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, map[string]string{"IP": guestIP}); err != nil {
		return err
	}
	if err := runner.CopyDataPrivileged(buf.Bytes(), "/etc/resolv.conf"); err != nil {
		return fmt.Errorf("rewrite resolv.conf: %w", err)
	}
	return nil
}

// generateAndSwapSSHKey generates a new ed25519 keypair and appends the new
// public key to /home/core/.ssh/authorized_keys. It returns the new signer
// and the authorized_keys bytes. The function appends the new key rather
// than overwriting the bundle's original key, on purpose: if this crc-agent
// Job pod is killed and retried after this step has already run against the
// VM, the retry's first SSH connection (main.go's fetchClusterInfo) still
// authenticates with the original bundle key. Recovery does not depend on
// the ephemeral, in-memory-only key that a previous, failed pod attempt
// generated.
// Ported from crc updateSSHKeyPair and pkg/crc/ssh/keys.go GenerateSSHKey.
func generateAndSwapSSHKey(runner *Runner) (gossh.Signer, error) {
	_, privKey, err := generateEd25519Key()
	if err != nil {
		return nil, fmt.Errorf("generate ed25519 key: %w", err)
	}
	signer, err := gossh.NewSignerFromKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("ssh signer from ed25519 key: %w", err)
	}
	pubBytes := gossh.MarshalAuthorizedKey(signer.PublicKey())
	appendData := append([]byte("\n"), pubBytes...)
	if err := runner.AppendDataPrivileged(appendData, "/home/core/.ssh/authorized_keys", 0o644); err != nil {
		return nil, fmt.Errorf("append authorized_keys: %w", err)
	}
	return signer, nil
}

// startKubelet issues systemctl daemon-reload and start kubelet over SSH.
// Ported from crc pkg/crc/systemd/systemd.go Commander.Start.
func startKubelet(runner *Runner) error {
	for _, cmd := range []string{
		"systemctl daemon-reload",
		"systemctl start kubelet",
	} {
		if _, err := runner.RunPrivileged(cmd); err != nil {
			return fmt.Errorf("%q: %w", cmd, err)
		}
	}
	return nil
}

// bootstrapCA is the CA-regen bootstrap step:
//  1. Generate a self-signed CA (crypto/x509).
//  2. Mint a system:admin / system:masters client cert.
//  3. Read the bundle's /opt/kubeconfig and splice in the new client cert.
//  4. Run `oc patch configmap admin-kubeconfig-client-ca` on the guest over
//     SSH so the API server trusts the new CA. This is the one intentional
//     oc-on-guest exception, matching crc's own approach: the API server
//     must trust the CA before any typed client using it can connect.
//  5. Copy the patched kubeconfig into /opt/kubeconfig on the guest.
//  6. Patch the 99-master-ssh machineconfig with the new public key.
//
// Ported from crc pkg/crc/machine/start.go updateKubeconfig and
// pkg/crc/cluster/cluster.go EnsureGeneratedClientCAPresentInTheCluster.
func bootstrapCA(runner *Runner) (*guestResult, error) {
	// Generate CA + client cert.
	caKey, caCert, err := SelfSignedCA()
	if err != nil {
		return nil, err
	}
	clientCertPEM, clientKeyPEM, err := ClientCertificate(caKey, caCert)
	if err != nil {
		return nil, err
	}
	caPEM := CAPem(caCert)

	// Read the bundle's admin kubeconfig from the guest, with retries. The
	// caller started the kubelet a moment ago, and the kubelet may not have
	// finished (re)writing this file with real cluster data yet. See
	// readGuestKubeconfig.
	bundleKubeconfigYAML, err := readGuestKubeconfig(runner, 10*time.Minute, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("reading bundle kubeconfig: %w", err)
	}

	// Capture the bundle's own server CA bundle. See guestResult.ServerCAPEM's
	// doc comment for why code must not confuse this with the generated CA.
	serverCAPEM, err := extractServerCAPEM([]byte(bundleKubeconfigYAML))
	if err != nil {
		return nil, fmt.Errorf("extracting server CA from bundle kubeconfig: %w", err)
	}

	// Splice in the new client cert+key.
	patchedKubeconfig, err := spliceClientCertIntoKubeconfig(
		[]byte(bundleKubeconfigYAML), clientCertPEM, clientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("splicing client cert into kubeconfig: %w", err)
	}

	// Patch admin-kubeconfig-client-ca configmap via oc on the guest.
	// This is the only oc-on-guest call; it must happen BEFORE typed clients
	// try to authenticate, so the API server trusts our new CA.
	caPEMJSON, _ := json.Marshal(string(caPEM))
	patchCmd := fmt.Sprintf(
		`oc --kubeconfig /opt/kubeconfig patch configmap admin-kubeconfig-client-ca `+
			`-n openshift-config --patch '{"data":{"ca-bundle.crt":%s}}'`,
		string(caPEMJSON),
	)
	// The API server can take well over a minute to come up after the kubelet
	// starts (static pod scheduling, etcd formation, and so on). Retry until
	// it responds instead of failing on the first, expected,
	// connection-refused or TLS error.
	if err := retryRunPrivileged(runner, patchCmd, 10*time.Minute, 10*time.Second); err != nil {
		return nil, fmt.Errorf("patch admin-kubeconfig-client-ca: %w", err)
	}

	// Write the patched kubeconfig back to the guest, so subsequent oc calls
	// on the guest use the new cert.
	if err := runner.CopyDataPrivileged(patchedKubeconfig, "/opt/kubeconfig"); err != nil {
		return nil, fmt.Errorf("writing patched kubeconfig to guest: %w", err)
	}

	// NOTE: this code does not patch the 99-master-ssh machineconfig with the
	// new SSH public key here, unlike upstream crc's
	// EnsureSSHKeyPresentInTheCluster. A JSON merge-patch on
	// sshAuthorizedKeys replaces the whole array. Once the
	// machine-config-operator/daemon reconciles that MachineConfig, it
	// rewrites the guest's actual authorized_keys file to match. That would
	// discard the bundle's original key, which generateAndSwapSSHKey
	// appends rather than overwrites for retry safety (see its doc
	// comment). Patching the machineconfig would silently reintroduce the
	// exact "a retried crc-agent Job pod can never SSH in again" failure
	// mode this file works hard to avoid, but at the machineconfig layer
	// instead of the file layer. Keeping only the guest's own
	// authorized_keys file in sync, which the code already does above, is
	// enough for this operator's purposes: these CRC VMs are ephemeral and
	// recycled per lease, so the cluster's own declarative machineconfig
	// does not need to match the currently active SSH key.

	return &guestResult{
		AdminKubeconfigPEM: patchedKubeconfig,
		CACert:             caCert,
		CAKey:              caKey,
		ServerCAPEM:        serverCAPEM,
		ClientCertPEM:      clientCertPEM,
		ClientKeyPEM:       clientKeyPEM,
	}, nil
}

// retryRunPrivileged retries a guest-side privileged command until it
// succeeds or the timeout elapses. Code uses this for oc-on-guest calls
// issued shortly after starting the kubelet, where the API server is not
// immediately reachable (static pod scheduling and etcd formation can take
// well over a minute).
// readGuestKubeconfig reads /opt/kubeconfig from the guest over SSH. It
// retries until the file parses as YAML with at least one populated
// "clusters" entry, or until timeout.
//
// The bundle's own kubeconfig at this path is not a static bundled
// artifact. The kubelet, part of the single-node control plane, (re)writes
// it during its own bootstrapping, and again whenever the API server's
// serving configuration changes. This notably happens right after
// cluster.go's applyExternalAPIPatches patches in the external-facing
// SNI/named-certificate, which triggers a kube-apiserver-operator revision
// rollout. A read that races either of these windows can observe a
// transient empty or templated file (valid YAML, zero "clusters" entries)
// instead of a hard error. That is why this function uses a content-level
// retry rather than retryRunPrivileged's exit-code-only retry. Without
// this, the very first crc-agent Job pod attempt can fail here even though
// nothing is wrong, it asked too early. Only a backoffLimit-driven retry
// against the same, already-further-along, VM happens to succeed, because
// more wall-clock time passed. Retrying in place avoids depending on that
// coincidence.
func readGuestKubeconfig(runner *Runner, timeout, interval time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		raw, err := runner.RunPrivileged("cat /opt/kubeconfig")
		switch {
		case err != nil:
			lastErr = fmt.Errorf("reading /opt/kubeconfig: %w", err)
		case !kubeconfigHasClusters([]byte(raw)):
			lastErr = fmt.Errorf("kubeconfig has no clusters entries yet")
		default:
			return raw, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("timed out after %s waiting for a populated guest kubeconfig (last error: %w)", timeout, lastErr)
		}
		time.Sleep(interval)
	}
}

// kubeconfigHasClusters reports whether kubeconfigYAML parses as YAML and has
// at least one entry under "clusters". See readGuestKubeconfig.
func kubeconfigHasClusters(kubeconfigYAML []byte) bool {
	var kc map[string]interface{}
	if err := k8syaml.Unmarshal(kubeconfigYAML, &kc); err != nil {
		return false
	}
	clusters, ok := kc["clusters"].([]interface{})
	return ok && len(clusters) > 0
}

func retryRunPrivileged(runner *Runner, cmd string, timeout, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		_, err := runner.RunPrivileged(cmd)
		if err == nil {
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s (last error: %w)", timeout, lastErr)
		}
		time.Sleep(interval)
	}
}

// spliceClientCertIntoKubeconfig reads a YAML kubeconfig, finds the admin
// user's client-certificate-data and client-key-data fields, replaces them
// with the newly generated cert and key, then re-encodes the result as
// YAML.
//
// The bundle kubeconfig has a single user (admin) whose client cert is
// valid for only about 30 days. This function replaces it with a 10-year
// cert signed by the new CA.
func spliceClientCertIntoKubeconfig(kubeconfigYAML, certPEM, keyPEM []byte) ([]byte, error) {
	// Unmarshal into a generic map so we don't depend on a specific kubeconfig
	// schema version.
	var kc map[string]interface{}
	if err := k8syaml.Unmarshal(kubeconfigYAML, &kc); err != nil {
		return nil, fmt.Errorf("unmarshal kubeconfig: %w", err)
	}

	users, ok := kc["users"].([]interface{})
	if !ok || len(users) == 0 {
		return nil, fmt.Errorf("kubeconfig has no users")
	}
	// Patch the first (admin) user.
	userMap, ok := users[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("kubeconfig user[0] is not a map")
	}
	userEntry, ok := userMap["user"].(map[string]interface{})
	if !ok {
		userEntry = make(map[string]interface{})
		userMap["user"] = userEntry
	}
	userEntry["client-certificate-data"] = base64.StdEncoding.EncodeToString(certPEM)
	userEntry["client-key-data"] = base64.StdEncoding.EncodeToString(keyPEM)
	// Remove PEM file references if present.
	delete(userEntry, "client-certificate")
	delete(userEntry, "client-key")

	out, err := k8syaml.Marshal(kc)
	if err != nil {
		return nil, fmt.Errorf("marshal patched kubeconfig: %w", err)
	}
	return out, nil
}

// extractServerCAPEM reads the clusters[0].cluster.certificate-authority-data
// field of a YAML kubeconfig and returns its decoded PEM bytes. This is the
// CA bundle that verifies the API server's own TLS serving certificate for
// the api.crc.testing SNI. See guestResult.ServerCAPEM's doc comment for why
// this must stay distinct from the CA that bootstrapCA generates.
func extractServerCAPEM(kubeconfigYAML []byte) ([]byte, error) {
	var kc map[string]interface{}
	if err := k8syaml.Unmarshal(kubeconfigYAML, &kc); err != nil {
		return nil, fmt.Errorf("unmarshal kubeconfig: %w", err)
	}
	clusters, ok := kc["clusters"].([]interface{})
	if !ok || len(clusters) == 0 {
		return nil, fmt.Errorf("kubeconfig has no clusters entries")
	}
	clusterEntry, ok := clusters[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("kubeconfig cluster[0] is not a map")
	}
	clusterField, ok := clusterEntry["cluster"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("kubeconfig cluster[0].cluster is not a map")
	}
	caDataB64, ok := clusterField["certificate-authority-data"].(string)
	if !ok || caDataB64 == "" {
		return nil, fmt.Errorf("kubeconfig cluster[0] has no certificate-authority-data")
	}
	caPEM, err := base64.StdEncoding.DecodeString(caDataB64)
	if err != nil {
		return nil, fmt.Errorf("decoding certificate-authority-data: %w", err)
	}
	return caPEM, nil
}
