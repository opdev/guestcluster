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

import "testing"

func TestCRCIdentity(t *testing.T) {
	hostname := "api.cluster.example.test"
	identity, err := NewCRCIdentity(hostname)
	if err != nil {
		t.Fatalf("NewCRCIdentity: %v", err)
	}
	if err := identity.Validate(hostname); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if _, err := CRCIdentityFromSecretData(identity.SecretData(), hostname); err != nil {
		t.Fatalf("CRCIdentityFromSecretData: %v", err)
	}
	if err := identity.Validate("api.other.example.test"); err == nil {
		t.Fatal("Validate accepted a different hostname")
	}
}

func TestCRCIdentityRejectsCorruptData(t *testing.T) {
	identity, err := NewCRCIdentity("api.cluster.example.test")
	if err != nil {
		t.Fatalf("NewCRCIdentity: %v", err)
	}
	data := identity.SecretData()
	data[CRCIdentityClientPrivateKey] = []byte("not a private key")
	if _, err := CRCIdentityFromSecretData(data, "api.cluster.example.test"); err == nil {
		t.Fatal("CRCIdentityFromSecretData accepted corrupt data")
	}
}
