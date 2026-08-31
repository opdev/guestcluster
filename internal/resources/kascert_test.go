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
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"
)

func TestGenerateAPIServerServingCert(t *testing.T) {
	hostname := "api-hcp-pool-99h8d.apps.example.com"

	certPEM, keyPEM, err := GenerateAPIServerServingCert(hostname)
	if err != nil {
		t.Fatalf("GenerateAPIServerServingCert returned error: %v", err)
	}
	if len(certPEM) == 0 || len(keyPEM) == 0 {
		t.Fatal("expected non-empty certPEM and keyPEM")
	}

	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		t.Fatal("failed to PEM-decode certPEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatalf("failed to parse generated certificate: %v", err)
	}

	if len(cert.DNSNames) != 1 || cert.DNSNames[0] != hostname {
		t.Errorf("cert.DNSNames = %v, want [%q]", cert.DNSNames, hostname)
	}
	if !cert.IsCA {
		t.Error("cert.IsCA = false, want true (self-signed leaf must be its own trust anchor)")
	}
	if !cert.BasicConstraintsValid {
		t.Error("cert.BasicConstraintsValid = false, want true")
	}

	foundServerAuth := false
	for _, eku := range cert.ExtKeyUsage {
		if eku == x509.ExtKeyUsageServerAuth {
			foundServerAuth = true
		}
	}
	if !foundServerAuth {
		t.Errorf("cert.ExtKeyUsage = %v, want to include ExtKeyUsageServerAuth", cert.ExtKeyUsage)
	}

	if time.Until(cert.NotAfter) < 5*365*24*time.Hour {
		t.Errorf("cert.NotAfter = %v, want at least 5 years in the future", cert.NotAfter)
	}
	if cert.NotBefore.After(time.Now()) {
		t.Errorf("cert.NotBefore = %v, want in the past", cert.NotBefore)
	}

	// A self-signed cert should verify against itself as the trust anchor.
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	if _, err := cert.Verify(x509.VerifyOptions{DNSName: hostname, Roots: pool}); err != nil {
		t.Errorf("cert failed to verify against itself as trust anchor: %v", err)
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		t.Fatal("failed to PEM-decode keyPEM")
	}
	if _, err := x509.ParseECPrivateKey(keyBlock.Bytes); err != nil {
		t.Errorf("failed to parse generated private key: %v", err)
	}
}

func TestGenerateAPIServerServingCert_EmptyHostname(t *testing.T) {
	if _, _, err := GenerateAPIServerServingCert(""); err == nil {
		t.Error("expected an error for an empty hostname, got nil")
	}
}

func TestServingCertNeedsRegen(t *testing.T) {
	hostname := "api-hcp-pool-99h8d.apps.example.com"
	certPEM, _, err := GenerateAPIServerServingCert(hostname)
	if err != nil {
		t.Fatalf("GenerateAPIServerServingCert returned error: %v", err)
	}

	if ServingCertNeedsRegen(certPEM, hostname) {
		t.Error("ServingCertNeedsRegen = true for a freshly generated, matching-hostname cert, want false")
	}
	if !ServingCertNeedsRegen(certPEM, "different.hostname.example.com") {
		t.Error("ServingCertNeedsRegen = false for a hostname mismatch, want true")
	}
	if !ServingCertNeedsRegen(nil, hostname) {
		t.Error("ServingCertNeedsRegen = false for nil certPEM, want true")
	}
	if !ServingCertNeedsRegen([]byte("not a cert"), hostname) {
		t.Error("ServingCertNeedsRegen = false for unparsable certPEM, want true")
	}
}
