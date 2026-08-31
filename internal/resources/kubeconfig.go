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
	"fmt"

	"k8s.io/client-go/tools/clientcmd"
)

// RewriteKubeconfigServer parses raw (a kubeconfig document, such as the one
// published in a HyperShift HostedCluster's admin-kubeconfig Secret) and
// returns a copy with every cluster entry's server URL replaced by
// externalServer and its certificate-authority-data replaced by caPEM.
//
// This function exists because HyperShift's admin kubeconfig for a
// KubeVirt-platform HostedCluster embeds Services[APIServer]'s
// NodePort.Address:port combination verbatim as the server URL. As
// documented on BuildHostedCluster, that address must be a reachable
// management-cluster node InternalIP, a hard requirement for worker
// bootstrap to work at all. That address is only reachable from inside the
// management cluster's own network. Publishing the admin kubeconfig
// verbatim would therefore hand callers a kubeconfig that only works from
// inside that network, not the externally-reachable one documented in
// ClusterInstanceStatus.APIEndpoint / ClusterLeaseStatus.APIEndpoint.
//
// externalServer should be this operator's own externally-reachable admin
// Route (see BuildHostedClusterAPIRoute), typically the same value as
// ClusterInstanceStatus.APIEndpoint. caPEM should be the certificate
// GenerateAPIServerServingCert generated for that Route's hostname.
// BuildHostedCluster also wires this certificate into the HostedCluster as
// a NamedCertificate, so the guest kube-apiserver presents it via SNI when
// a client connects using that hostname. Because the certificate is
// self-signed, embedding it as certificate-authority-data lets it serve as
// its own trust anchor, so callers verify the connection normally with no
// --insecure-skip-tls-verify required.
func RewriteKubeconfigServer(raw []byte, externalServer string, caPEM []byte) ([]byte, error) {
	cfg, err := clientcmd.Load(raw)
	if err != nil {
		return nil, fmt.Errorf("parsing kubeconfig: %w", err)
	}

	for _, cluster := range cfg.Clusters {
		cluster.Server = externalServer
		cluster.CertificateAuthority = ""
		cluster.CertificateAuthorityData = caPEM
		cluster.InsecureSkipTLSVerify = false
	}

	out, err := clientcmd.Write(*cfg)
	if err != nil {
		return nil, fmt.Errorf("serializing rewritten kubeconfig: %w", err)
	}
	return out, nil
}
