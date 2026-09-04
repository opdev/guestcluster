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
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

const (
	crcIdentityValidity = 10 * 365 * 24 * time.Hour
	crcIdentityKeySize  = 2048
	pemCertificateType  = "CERTIFICATE"

	CRCIdentityClientCAKey       = "client-ca.crt"
	CRCIdentityClientCertKey     = "client.crt"
	CRCIdentityClientPrivateKey  = "client.key"
	CRCIdentityServingCertKey    = "serving.crt"
	CRCIdentityServingPrivateKey = "serving.key"
)

// CRCIdentity holds the long-lived credentials for a CRC ClusterInstance.
// The client CA private key is deliberately discarded after initial creation.
type CRCIdentity struct {
	ClientCA          []byte
	ClientCert        []byte
	ClientPrivateKey  []byte
	ServingCert       []byte
	ServingPrivateKey []byte
}

// CRCIdentitySecretName returns the Secret name for a CRC instance identity.
func CRCIdentitySecretName(instanceName string) string {
	return instanceName + "-crc-identity"
}

// SecretData returns identity material using the stable Secret data keys.
func (i CRCIdentity) SecretData() map[string][]byte {
	return map[string][]byte{
		CRCIdentityClientCAKey:       i.ClientCA,
		CRCIdentityClientCertKey:     i.ClientCert,
		CRCIdentityClientPrivateKey:  i.ClientPrivateKey,
		CRCIdentityServingCertKey:    i.ServingCert,
		CRCIdentityServingPrivateKey: i.ServingPrivateKey,
	}
}

// CRCIdentityFromSecretData validates and returns Secret identity data.
func CRCIdentityFromSecretData(data map[string][]byte, hostname string) (CRCIdentity, error) {
	identity := CRCIdentity{
		ClientCA:          data[CRCIdentityClientCAKey],
		ClientCert:        data[CRCIdentityClientCertKey],
		ClientPrivateKey:  data[CRCIdentityClientPrivateKey],
		ServingCert:       data[CRCIdentityServingCertKey],
		ServingPrivateKey: data[CRCIdentityServingPrivateKey],
	}
	if err := identity.Validate(hostname); err != nil {
		return CRCIdentity{}, err
	}
	return identity, nil
}

// NewCRCIdentity creates the credentials that remain stable for the complete
// ClusterInstance lifetime.
func NewCRCIdentity(hostname string) (CRCIdentity, error) {
	if hostname == "" {
		return CRCIdentity{}, fmt.Errorf("hostname must not be empty")
	}
	now := time.Now()
	caKey, err := rsa.GenerateKey(rand.Reader, crcIdentityKeySize)
	if err != nil {
		return CRCIdentity{}, fmt.Errorf("generating client CA key: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: randomSerial(),
		Subject:      pkix.Name{CommonName: "admin-kubeconfig-signer-custom", OrganizationalUnit: []string{"openshift"}},
		NotBefore:    now, NotAfter: now.Add(crcIdentityValidity), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return CRCIdentity{}, fmt.Errorf("creating client CA certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return CRCIdentity{}, fmt.Errorf("parsing client CA certificate: %w", err)
	}
	clientKey, err := rsa.GenerateKey(rand.Reader, crcIdentityKeySize)
	if err != nil {
		return CRCIdentity{}, fmt.Errorf("generating client key: %w", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: randomSerial(),
		Subject:      pkix.Name{CommonName: "system:admin", OrganizationalUnit: []string{"system:masters"}},
		NotBefore:    now, NotAfter: now.Add(crcIdentityValidity), BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		return CRCIdentity{}, fmt.Errorf("creating client certificate: %w", err)
	}
	servingKey, err := rsa.GenerateKey(rand.Reader, crcIdentityKeySize)
	if err != nil {
		return CRCIdentity{}, fmt.Errorf("generating serving key: %w", err)
	}
	servingTemplate := &x509.Certificate{
		SerialNumber: randomSerial(), Subject: pkix.Name{CommonName: hostname}, DNSNames: []string{hostname},
		NotBefore: now, NotAfter: now.Add(crcIdentityValidity), IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	servingDER, err := x509.CreateCertificate(rand.Reader, servingTemplate, servingTemplate, &servingKey.PublicKey, servingKey)
	if err != nil {
		return CRCIdentity{}, fmt.Errorf("creating serving certificate: %w", err)
	}
	identity := CRCIdentity{
		ClientCA:          pem.EncodeToMemory(&pem.Block{Type: pemCertificateType, Bytes: caDER}),
		ClientCert:        pem.EncodeToMemory(&pem.Block{Type: pemCertificateType, Bytes: clientDER}),
		ClientPrivateKey:  pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)}),
		ServingCert:       pem.EncodeToMemory(&pem.Block{Type: pemCertificateType, Bytes: servingDER}),
		ServingPrivateKey: pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(servingKey)}),
	}
	return identity, identity.Validate(hostname)
}

// Validate rejects incomplete or unexpected identity material. Reconciliation
// must fail rather than silently replacing a corrupted stable identity.
func (i CRCIdentity) Validate(hostname string) error {
	if hostname == "" {
		return fmt.Errorf("hostname must not be empty")
	}
	ca, err := parseCertificate(i.ClientCA)
	if err != nil || !ca.IsCA || !certificateCurrent(ca) {
		return fmt.Errorf("invalid client CA")
	}
	client, err := parseCertificate(i.ClientCert)
	if err != nil || !certificateCurrent(client) || client.Subject.CommonName != "system:admin" || !hasOrganizationalUnit(client, "system:masters") || !hasUsage(client, x509.ExtKeyUsageClientAuth) || client.CheckSignatureFrom(ca) != nil {
		return fmt.Errorf("invalid client certificate")
	}
	if !keyMatchesCertificate(i.ClientPrivateKey, client) {
		return fmt.Errorf("client private key does not match certificate")
	}
	serving, err := parseCertificate(i.ServingCert)
	if err != nil || !certificateCurrent(serving) || serving.Subject.CommonName != hostname || len(serving.DNSNames) != 1 || serving.DNSNames[0] != hostname || serving.VerifyHostname(hostname) != nil || !hasUsage(serving, x509.ExtKeyUsageServerAuth) || serving.CheckSignatureFrom(serving) != nil {
		return fmt.Errorf("invalid serving certificate")
	}
	if !keyMatchesCertificate(i.ServingPrivateKey, serving) {
		return fmt.Errorf("serving private key does not match certificate")
	}
	return nil
}

func certificateCurrent(cert *x509.Certificate) bool {
	now := time.Now()
	return !now.Before(cert.NotBefore) && now.Before(cert.NotAfter)
}

func hasOrganizationalUnit(cert *x509.Certificate, want string) bool {
	for _, unit := range cert.Subject.OrganizationalUnit {
		if unit == want {
			return true
		}
	}
	return false
}

func parseCertificate(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("no certificate PEM block")
	}
	return x509.ParseCertificate(block.Bytes)
}

func keyMatchesCertificate(keyPEM []byte, cert *x509.Certificate) bool {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return false
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return false
	}
	certPublic, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
	if err != nil {
		return false
	}
	keyPublic, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	return err == nil && bytes.Equal(certPublic, keyPublic)
}

func hasUsage(cert *x509.Certificate, want x509.ExtKeyUsage) bool {
	for _, usage := range cert.ExtKeyUsage {
		if usage == want {
			return true
		}
	}
	return false
}

func randomSerial() *big.Int {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return big.NewInt(1)
	}
	return serial
}
