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

// Package main - tlsutil.go
//
// This file provides pure TLS and x509 helpers for crc-agent. The logic is
// ported from github.com/crc-org/crc pkg/crc/tls/tls.go (commit ~2025-Q3),
// with crc-specific coupling removed. No openssl binary is required.
// Everything uses Go's crypto/x509 and crypto/rsa packages.
//
// The functions in this file are stateless pure functions, so they are
// easy to unit test.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

const (
	// validityTenYears mirrors crc's own ValidityTenYears for CA/client certs.
	validityTenYears = 10 * 365 * 24 * time.Hour
	// rsaKeySize matches crc (RSA 2048).
	rsaKeySize = 2048

	// pemBlockCertificate and pemBlockRSAPrivateKey are the PEM block types
	// used throughout this file and guest.go.
	pemBlockCertificate   = "CERTIFICATE"
	pemBlockRSAPrivateKey = "RSA PRIVATE KEY"
)

// SelfSignedCA generates a self-signed CA certificate suitable for use as the
// admin-kubeconfig-signer-custom CA.  Ported from crc GetSelfSignedCA.
func SelfSignedCA() (*rsa.PrivateKey, *x509.Certificate, error) {
	key, err := rsa.GenerateKey(rand.Reader, rsaKeySize)
	if err != nil {
		return nil, nil, fmt.Errorf("generating CA RSA key: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: bigRandSerial(),
		Subject: pkix.Name{
			CommonName:         "admin-kubeconfig-signer-custom",
			OrganizationalUnit: []string{"openshift"},
		},
		NotBefore:             now,
		NotAfter:              now.Add(validityTenYears),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("creating CA certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing CA certificate: %w", err)
	}
	return key, cert, nil
}

// ClientCertificate generates a system:admin / system:masters client
// certificate signed by the given CA.  Ported from crc GenerateClientCertificate.
func ClientCertificate(caKey *rsa.PrivateKey, caCert *x509.Certificate) (certPEM, keyPEM []byte, err error) {
	key, err := rsa.GenerateKey(rand.Reader, rsaKeySize)
	if err != nil {
		return nil, nil, fmt.Errorf("generating client RSA key: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: bigRandSerial(),
		Subject: pkix.Name{
			CommonName:         "system:admin",
			OrganizationalUnit: []string{"system:masters"},
		},
		NotBefore:             now,
		NotAfter:              now.Add(validityTenYears),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("creating client certificate: %w", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: pemBlockCertificate, Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: pemBlockRSAPrivateKey, Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM, nil
}

// CAPem returns the PEM-encoded certificate for cert.
func CAPem(cert *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: pemBlockCertificate, Bytes: cert.Raw})
}

// CAKeyPem returns the PEM-encoded PKCS1 private key.
func CAKeyPem(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: pemBlockRSAPrivateKey, Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

// VerifyCertPEM verifies that certPEM is signed by caCert.
// Returns nil if valid. Ported from crc VerifyCertificateAgainstRootCA.
func VerifyCertPEM(certPEM []byte, caCert *x509.Certificate) error {
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return fmt.Errorf("no PEM block in certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parsing certificate: %w", err)
	}
	_, err = cert.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
	return err
}

// ExternalAPIServingCert generates a self-signed TLS serving certificate for
// the externally routable API hostname (the management cluster's
// passthrough Route host, e.g. api-<instance>.apps.<mgmt-domain>; see
// internal/resources.BuildCRCAPIRoute). patchAPIServerConfig in cluster.go
// installs this certificate as the guest API server's namedCertificate for
// that hostname. Because the certificate is self-signed, it also doubles
// as its own trust root when embedded as certificate-authority-data in the
// published kubeconfig.
//
// The VMI's pod-network IP is not routable outside the management cluster,
// so a hostname resolving directly to it never works for real external
// clients. Only the management cluster's own router can reach the VMI,
// which is what the passthrough Route provides.
func ExternalAPIServingCert(hostname string) (certPEM, keyPEM []byte, err error) {
	key, err := rsa.GenerateKey(rand.Reader, rsaKeySize)
	if err != nil {
		return nil, nil, fmt.Errorf("generating external API serving RSA key: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: bigRandSerial(),
		Subject: pkix.Name{
			CommonName: hostname,
		},
		DNSNames:              []string{hostname},
		NotBefore:             now,
		NotAfter:              now.Add(validityTenYears),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("creating external API serving certificate: %w", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: pemBlockCertificate, Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: pemBlockRSAPrivateKey, Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM, nil
}

// LoadBundleCA reads the CA data from a kubeconfig TLS block. The returned
// *x509.CertPool and raw PEM are used to build the rest.Config that talks
// to the guest API through the SSH tunnel.
func LoadBundleCA(caPEM []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certificates could be parsed from CA PEM")
	}
	return pool, nil
}

// TLSFromPEM parses a tls.Certificate from certPEM + keyPEM.
func TLSFromPEM(certPEM, keyPEM []byte) (tls.Certificate, error) {
	return tls.X509KeyPair(certPEM, keyPEM)
}

func bigRandSerial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		// Extremely unlikely; use a fixed value rather than panic.
		return big.NewInt(1)
	}
	return n
}
