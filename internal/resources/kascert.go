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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

// kasServingCertValidity is how long a self-signed KAS named-certificate
// (see GenerateAPIServerServingCert) stays valid. This operator has no
// rotation UX beyond regenerating the certificate on the next reconcile
// once kasServingCertRenewBefore is reached, and every regeneration
// briefly invalidates any cached copy an external client holds (see
// ServingCertNeedsRegen's doc comment). The validity window is therefore
// long-lived.
const (
	kasServingCertValidity = 10 * 365 * 24 * time.Hour
)

// kasServingCertRenewBefore is how far ahead of actual expiry
// ServingCertNeedsRegen requests regeneration. This gives a reconcile
// comfortable lead time to notice and roll the certificate before it
// expires.
const kasServingCertRenewBefore = 30 * 24 * time.Hour

// KASServingCertName is the deterministic name of the kubernetes.io/tls
// Secret (in the HostedCluster's own namespace; see
// ClusterInstanceReconciler.ensureKASServingCert) that holds the
// self-signed serving certificate BuildHostedCluster wires into
// HostedCluster.Spec.Configuration.APIServer.ServingCerts.NamedCertificates
// for hostname.
func KASServingCertName(instanceName string) string {
	return instanceName + "-kas-serving-cert"
}

// GenerateAPIServerServingCert mints a self-signed ECDSA P-256 TLS
// certificate/key pair valid for hostname (as its sole DNS SAN), for use as
// a HyperShift HostedCluster APIServer named certificate (see
// BuildHostedCluster).
//
// The certificate is self-signed and marked as its own CA (IsCA=true).
// HyperShift's own cluster-internal root CA signs the internal KAS serving
// certificate, but callers outside the cluster have no way to trust that
// CA. No separate, pre-existing CA exists that an externally-reachable
// admin hostname's certificate could chain to. Making the leaf itself the
// trust anchor lets RewriteKubeconfigServer embed certPEM verbatim as
// certificate-authority-data, so oc/kubectl verify the connection normally
// with no --insecure-skip-tls-verify required. HyperShift's own
// combineRootCAWithServingCerts, which builds the trust bundle for its
// native customKubeconfig feature, documents relying on this same
// self-signed-leaf-as-trust-anchor property.
func GenerateAPIServerServingCert(hostname string) (certPEM, keyPEM []byte, err error) {
	if hostname == "" {
		return nil, nil, fmt.Errorf("hostname must not be empty")
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating serving key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generating serial number: %w", err)
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: hostname},
		DNSNames:              []string{hostname},
		NotBefore:             now.Add(-5 * time.Minute), // small clock-skew allowance
		NotAfter:              now.Add(kasServingCertValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, fmt.Errorf("creating certificate: %w", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, nil, fmt.Errorf("marshaling private key: %w", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: pemCertificateType, Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

// ServingCertNeedsRegen reports whether the certificate in certPEM needs
// regeneration (via GenerateAPIServerServingCert) because it is missing,
// unparsable, no longer valid for wantHostname (for example, the
// ClusterInstance's ingress domain changed), or is within
// kasServingCertRenewBefore of expiring.
//
// Regenerating a rotated CA-signed certificate does not disturb existing
// clients, but regenerating a self-signed certificate invalidates the
// certificate-authority-data embedded in any previously published
// kubeconfig. This function is deliberately conservative, using a
// multi-year validity window and renewing only shortly before actual
// expiry, to minimize how often already-distributed kubeconfigs break.
func ServingCertNeedsRegen(certPEM []byte, wantHostname string) bool {
	if len(certPEM) == 0 {
		return true
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return true
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return true
	}
	if len(cert.DNSNames) != 1 || cert.DNSNames[0] != wantHostname {
		return true
	}
	if time.Now().After(cert.NotAfter.Add(-kasServingCertRenewBefore)) {
		return true
	}
	return false
}
