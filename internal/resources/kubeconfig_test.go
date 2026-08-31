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
	"testing"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const testContextName = "context"

func sampleKubeconfig(t *testing.T, server string) []byte {
	t.Helper()

	cfg := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"cluster": {
				Server:                   server,
				CertificateAuthorityData: []byte("original-ca-data"),
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			testContextName: {Cluster: "cluster", AuthInfo: "admin"},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"admin": {Token: "admin-token"},
		},
		CurrentContext: testContextName,
	}

	raw, err := clientcmd.Write(cfg)
	if err != nil {
		t.Fatalf("building sample kubeconfig: %v", err)
	}
	return raw
}

func TestRewriteKubeconfigServer(t *testing.T) {
	raw := sampleKubeconfig(t, "https://10.0.70.156:31381")
	caPEM := []byte("-----BEGIN CERTIFICATE-----\nfakecertdata\n-----END CERTIFICATE-----\n")

	out, err := RewriteKubeconfigServer(raw, "https://api-hcp-pool-1.apps.example.com", caPEM)
	if err != nil {
		t.Fatalf("RewriteKubeconfigServer returned error: %v", err)
	}

	cfg, err := clientcmd.Load(out)
	if err != nil {
		t.Fatalf("loading rewritten kubeconfig: %v", err)
	}

	if len(cfg.Clusters) != 1 {
		t.Fatalf("expected exactly 1 cluster entry, got %d", len(cfg.Clusters))
	}
	for name, cluster := range cfg.Clusters {
		if got, want := cluster.Server, "https://api-hcp-pool-1.apps.example.com"; got != want {
			t.Errorf("cluster %q Server = %q, want %q", name, got, want)
		}
		if string(cluster.CertificateAuthorityData) != string(caPEM) {
			t.Errorf("cluster %q CertificateAuthorityData = %q, want %q", name, cluster.CertificateAuthorityData, caPEM)
		}
		if cluster.CertificateAuthority != "" {
			t.Errorf("cluster %q CertificateAuthority = %q, want empty", name, cluster.CertificateAuthority)
		}
		if cluster.InsecureSkipTLSVerify {
			t.Errorf("cluster %q InsecureSkipTLSVerify = true, want false", name)
		}
	}

	// Contexts/auth-info/current-context should be preserved untouched.
	if cfg.CurrentContext != testContextName {
		t.Errorf("CurrentContext = %q, want %q", cfg.CurrentContext, testContextName)
	}
	if _, ok := cfg.AuthInfos["admin"]; !ok {
		t.Errorf("expected AuthInfo %q to be preserved", "admin")
	}
}

func TestRewriteKubeconfigServer_InvalidInput(t *testing.T) {
	if _, err := RewriteKubeconfigServer([]byte("not a kubeconfig"), "https://example.com", nil); err == nil {
		t.Error("expected an error for invalid kubeconfig input, got nil")
	}
}
