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

// Package main - clusterclient.go
//
// This file provides the typed Kubernetes client factory for the crc-agent.
// All connections to the guest cluster API go through an SSH port-forward.
// The existing Runner's ssh.Client dials api.crc.testing:6443 inside the VM.
// This dial removes the need for an external DNS entry for api.crc.testing
// on the management cluster, and it removes the need for a NodePort or
// LoadBalancer.
//
// Trust model:
//   - The rest.Config's Dial function routes every TCP connection through the
//     SSH tunnel via Runner.DialAPIServer.
//   - TLSClientConfig.ServerName is set to "api.crc.testing" so TLS SNI
//     matches the API server's serving certificate.
//   - The CA is the freshly generated admin-kubeconfig-signer-custom CA
//     (from guest.go bootstrapCA). The client cert and key are the
//     system:admin credentials produced by the same step.
package main

import (
	"context"
	"fmt"
	"net"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// GuestClients bundles the typed client sets the crc-agent needs to perform
// cluster-level mutations on the GUEST OpenShift cluster.
type GuestClients struct {
	// Core is the standard Kubernetes client set (Secrets, ConfigMaps,
	// CertificateSigningRequests, etc.).
	Core *kubernetes.Clientset
	// Dynamic is the dynamic client, used for resources whose Go types are not
	// available (e.g. config.openshift.io/v1 Ingress, APIServer, Routes).
	Dynamic dynamic.Interface
}

// NewGuestClients builds GuestClients whose transport dials through the SSH
// tunnel. runner must already be connected (NewRunner succeeded).
//
//   - caCert: PEM-encoded CA that signed the API server's serving cert;
//     also the CA we installed in admin-kubeconfig-client-ca (bootstrapCA).
//   - clientCertPEM / clientKeyPEM: the system:admin client cert+key produced
//     by bootstrapCA in guest.go.
func NewGuestClients(
	runner *Runner,
	caCertPEM []byte,
	clientCertPEM []byte,
	clientKeyPEM []byte,
) (*GuestClients, error) {
	// TLSFromPEM validates the cert and key, but we don't need to keep the resulting
	// tls.Certificate (rest.Config uses the raw PEM instead).
	_, err := TLSFromPEM(clientCertPEM, clientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("parsing client cert/key: %w", err)
	}

	restCfg := &rest.Config{
		Host: "https://api.crc.testing:6443",
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Ignore the addr from the HTTP stack (it will be
			// api.crc.testing:6443). Always route through the SSH tunnel.
			return runner.DialAPIServer()
		},
		TLSClientConfig: rest.TLSClientConfig{
			ServerName: "api.crc.testing",
			CAData:     caCertPEM,
			CertData:   clientCertPEM,
			KeyData:    clientKeyPEM,
		},
	}

	core, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("building core k8s client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("building dynamic client: %w", err)
	}
	return &GuestClients{Core: core, Dynamic: dyn}, nil
}
