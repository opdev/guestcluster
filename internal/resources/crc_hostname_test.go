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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	hostnameTestIngressDomain = "apps.example.test"
	hostnameTestInstanceName  = "instance"
	hostnameTestTopologyCRC   = "crc"
	hostnameTestTopologyHCP   = "hcp"
	hostnameTestNamespaceOne  = "tenant-one"
	hostnameTestNamespaceTwo  = "tenant-two"
)

func TestCRCAPIServerHostnameIsUniqueByNamespace(t *testing.T) {
	const (
		instanceName  = "crc-pool-abc123"
		ingressDomain = hostnameTestIngressDomain
	)

	first, err := CRCAPIServerHostname(instanceName, hostnameTestNamespaceOne, ingressDomain)
	if err != nil {
		t.Fatalf("CRCAPIServerHostname for tenant-one: %v", err)
	}
	second, err := CRCAPIServerHostname(instanceName, hostnameTestNamespaceTwo, ingressDomain)
	if err != nil {
		t.Fatalf("CRCAPIServerHostname for tenant-two: %v", err)
	}
	if first == second {
		t.Fatalf("same-named instances in different namespaces have the same hostname %q", first)
	}
	if again, err := CRCAPIServerHostname(instanceName, hostnameTestNamespaceOne, ingressDomain); err != nil || again != first {
		t.Fatalf("hostname is not stable: got %q, error %v; want %q", again, err, first)
	}
	if problems := validation.IsDNS1123Subdomain(first); len(problems) > 0 {
		t.Errorf("hostname %q is not DNS-safe: %s", first, strings.Join(problems, ", "))
	}
}

func TestAPIHostnamePreservesCRCOutputAndUsesNamespaceIdentity(t *testing.T) {
	const ingressDomain = hostnameTestIngressDomain

	got, err := APIHostname("crc-pool-abc123", hostnameTestNamespaceOne, ingressDomain)
	if err != nil {
		t.Fatalf("APIHostname: %v", err)
	}
	const want = "api-crc-pool-abc123-ee31c954cb2e84a3.apps.example.test"
	if got != want {
		t.Fatalf("APIHostname() = %q, want stable CRC output %q", got, want)
	}
	crcGot, err := CRCAPIServerHostname("crc-pool-abc123", hostnameTestNamespaceOne, ingressDomain)
	if err != nil {
		t.Fatalf("CRCAPIServerHostname: %v", err)
	}
	if crcGot != got {
		t.Fatalf("CRCAPIServerHostname() = %q, want shared helper output %q", crcGot, got)
	}
}

func TestAPIHostnameUsesNamespaceIdentityAcrossTopologies(t *testing.T) {
	const (
		instanceName  = "same-instance"
		ingressDomain = hostnameTestIngressDomain
	)
	pairs := []struct {
		name       string
		firstType  string
		firstNS    string
		secondType string
		secondNS   string
	}{
		{name: "same topology CRC", firstType: hostnameTestTopologyCRC, firstNS: hostnameTestNamespaceOne, secondType: hostnameTestTopologyCRC, secondNS: hostnameTestNamespaceTwo},
		{name: "same topology HCP", firstType: hostnameTestTopologyHCP, firstNS: hostnameTestNamespaceOne, secondType: hostnameTestTopologyHCP, secondNS: hostnameTestNamespaceTwo},
		{name: "mixed topology", firstType: hostnameTestTopologyCRC, firstNS: hostnameTestNamespaceOne, secondType: hostnameTestTopologyHCP, secondNS: hostnameTestNamespaceTwo},
	}
	for _, pair := range pairs {
		t.Run(pair.name, func(t *testing.T) {
			first, err := APIHostname(instanceName, pair.firstNS, ingressDomain)
			if err != nil {
				t.Fatalf("APIHostname for %s/%s: %v", pair.firstType, pair.firstNS, err)
			}
			second, err := APIHostname(instanceName, pair.secondNS, ingressDomain)
			if err != nil {
				t.Fatalf("APIHostname for %s/%s: %v", pair.secondType, pair.secondNS, err)
			}
			if first == second {
				t.Fatalf("%s and %s instances have the same endpoint %q", pair.firstType, pair.secondType, first)
			}
		})
	}
}

func TestAPIHostnameUsesUnambiguousIdentityEncoding(t *testing.T) {
	const ingressDomain = hostnameTestIngressDomain
	first, err := APIHostname("c", "ab", ingressDomain)
	if err != nil {
		t.Fatalf("APIHostname(ab/c): %v", err)
	}
	second, err := APIHostname("bc", "a", ingressDomain)
	if err != nil {
		t.Fatalf("APIHostname(a/bc): %v", err)
	}
	if first == second {
		t.Fatalf("ambiguous namespace/name pairs share endpoint %q", first)
	}
	if !strings.Contains(first, "-6c032e631d39a14d.") || !strings.Contains(second, "-40bb547d936bbd31.") {
		t.Fatalf("namespace/name identity hashes are not encoded with a separator: %q, %q", first, second)
	}
}

func TestCRCAPIServerHostnameFitsDNSLimits(t *testing.T) {
	longIngressDomain := strings.Join([]string{
		strings.Repeat("d", 63),
		strings.Repeat("e", 63),
		strings.Repeat("f", 63),
		strings.Repeat("g", 30),
	}, ".")
	hostname, err := CRCAPIServerHostname(strings.Repeat("a", 63), hostnameTestNamespaceOne, longIngressDomain)
	if err != nil {
		t.Fatalf("CRCAPIServerHostname: %v", err)
	}
	if len(hostname) != 253 {
		t.Errorf("hostname length = %d, want 253: %q", len(hostname), hostname)
	}
	for _, label := range strings.Split(hostname, ".") {
		if len(label) > 63 {
			t.Errorf("hostname label length = %d, want at most 63: %q", len(label), label)
		}
	}
	if problems := validation.IsDNS1123Subdomain(hostname); len(problems) > 0 {
		t.Errorf("hostname %q is not DNS-safe: %s", hostname, strings.Join(problems, ", "))
	}
}

func TestCRCAPIServerHostnameRejectsIngressDomainWithoutRoom(t *testing.T) {
	tooLongIngressDomain := strings.Join([]string{
		strings.Repeat("d", 63),
		strings.Repeat("e", 63),
		strings.Repeat("f", 63),
		strings.Repeat("g", 43),
	}, ".")
	if _, err := CRCAPIServerHostname("crc-instance", hostnameTestNamespaceOne, tooLongIngressDomain); err == nil {
		t.Fatal("CRCAPIServerHostname accepted a domain that leaves no room for a unique hostname")
	}
}

func TestAPIHostnameRejectsInvalidInputs(t *testing.T) {
	cases := []struct {
		name      string
		instance  string
		namespace string
		domain    string
	}{
		{name: "empty name", instance: "", namespace: hostnameTestNamespaceOne, domain: hostnameTestIngressDomain},
		{name: "empty namespace", instance: hostnameTestInstanceName, namespace: "", domain: hostnameTestIngressDomain},
		{name: "invalid instance name", instance: "bad/name", namespace: hostnameTestNamespaceOne, domain: hostnameTestIngressDomain},
		{name: "invalid namespace", instance: hostnameTestInstanceName, namespace: "tenant one", domain: hostnameTestIngressDomain},
		{name: "empty ingress domain", instance: hostnameTestInstanceName, namespace: hostnameTestNamespaceOne, domain: ""},
		{name: "invalid ingress domain", instance: hostnameTestInstanceName, namespace: hostnameTestNamespaceOne, domain: "apps_example.test"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := APIHostname(tc.instance, tc.namespace, tc.domain); err == nil {
				t.Fatal("APIHostname accepted invalid input")
			}
		})
	}
}

func TestAPIHostnameFitsLongNamespaceNameAndDomain(t *testing.T) {
	longDomain := strings.Join([]string{
		strings.Repeat("d", 63),
		strings.Repeat("e", 63),
		strings.Repeat("f", 63),
		strings.Repeat("g", 30),
	}, ".")
	name := strings.Join([]string{strings.Repeat("a", 63), strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 61)}, ".")
	host, err := APIHostname(name, strings.Repeat("n", 63), longDomain)
	if err != nil {
		t.Fatalf("APIHostname: %v", err)
	}
	if len(host) > dns1123SubdomainMaxLength {
		t.Fatalf("hostname length = %d, want at most %d: %q", len(host), dns1123SubdomainMaxLength, host)
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) > dns1123LabelMaxLength {
			t.Errorf("hostname label length = %d, want at most %d: %q", len(label), dns1123LabelMaxLength, label)
		}
	}
	if problems := validation.IsDNS1123Subdomain(host); len(problems) > 0 {
		t.Errorf("hostname %q is not DNS-safe: %s", host, strings.Join(problems, ", "))
	}
}
