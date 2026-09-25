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

func TestCRCAPIServerHostnameIsUniqueByNamespace(t *testing.T) {
	const (
		instanceName  = "crc-pool-abc123"
		ingressDomain = "apps.example.test"
	)

	first, err := CRCAPIServerHostname(instanceName, "tenant-one", ingressDomain)
	if err != nil {
		t.Fatalf("CRCAPIServerHostname for tenant-one: %v", err)
	}
	second, err := CRCAPIServerHostname(instanceName, "tenant-two", ingressDomain)
	if err != nil {
		t.Fatalf("CRCAPIServerHostname for tenant-two: %v", err)
	}
	if first == second {
		t.Fatalf("same-named instances in different namespaces have the same hostname %q", first)
	}
	if again, err := CRCAPIServerHostname(instanceName, "tenant-one", ingressDomain); err != nil || again != first {
		t.Fatalf("hostname is not stable: got %q, error %v; want %q", again, err, first)
	}
	if problems := validation.IsDNS1123Subdomain(first); len(problems) > 0 {
		t.Errorf("hostname %q is not DNS-safe: %s", first, strings.Join(problems, ", "))
	}
}

func TestCRCAPIServerHostnameFitsDNSLimits(t *testing.T) {
	longIngressDomain := strings.Join([]string{
		strings.Repeat("d", 63),
		strings.Repeat("e", 63),
		strings.Repeat("f", 63),
		strings.Repeat("g", 30),
	}, ".")
	hostname, err := CRCAPIServerHostname(strings.Repeat("a", 63), "tenant-one", longIngressDomain)
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
	if _, err := CRCAPIServerHostname("crc-instance", "tenant-one", tooLongIngressDomain); err == nil {
		t.Fatal("CRCAPIServerHostname accepted a domain that leaves no room for a unique hostname")
	}
}
