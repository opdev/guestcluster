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

package main

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestUpdatePasswordsCreatesRawHtpasswdData(t *testing.T) {
	clients := &GuestClients{Core: fake.NewSimpleClientset()}

	if err := updatePasswords(context.Background(), clients, "kubeadmin-pass", "developer-pass"); err != nil {
		t.Fatalf("updatePasswords: %v", err)
	}

	secret, err := clients.Core.CoreV1().Secrets("openshift-config").Get(
		context.Background(), "htpass-secret", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("getting htpass-secret: %v", err)
	}
	assertHtpasswdPassword(t, string(secret.Data["htpasswd"]), userKubeadmin, "kubeadmin-pass")
	assertHtpasswdPassword(t, string(secret.Data["htpasswd"]), userDeveloper, "developer-pass")
}

func TestUpdatePasswordsUpdatesAndPreservesExternalUser(t *testing.T) {
	externalHash, err := BCryptHash("external-pass")
	if err != nil {
		t.Fatalf("hashing external password: %v", err)
	}
	clients := &GuestClients{Core: fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "htpass-secret", Namespace: "openshift-config"},
		Data: map[string][]byte{
			"htpasswd": []byte(strings.Join([]string{
				"external:" + externalHash,
				userKubeadmin + ":old-hash",
			}, "\n")),
		},
	})}

	if err := updatePasswords(context.Background(), clients, "new-kubeadmin-pass", "new-developer-pass"); err != nil {
		t.Fatalf("updatePasswords: %v", err)
	}

	secret, err := clients.Core.CoreV1().Secrets("openshift-config").Get(
		context.Background(), "htpass-secret", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("getting htpass-secret: %v", err)
	}
	htpasswd := string(secret.Data["htpasswd"])
	assertHtpasswdPassword(t, htpasswd, "external", "external-pass")
	assertHtpasswdPassword(t, htpasswd, userKubeadmin, "new-kubeadmin-pass")
	assertHtpasswdPassword(t, htpasswd, userDeveloper, "new-developer-pass")
}

func assertHtpasswdPassword(t *testing.T, htpasswd, username, password string) {
	t.Helper()
	for _, line := range strings.Split(htpasswd, "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 && parts[0] == username {
			if err := BCryptVerify(parts[1], password); err != nil {
				t.Fatalf("password for %q does not verify: %v", username, err)
			}
			return
		}
	}
	t.Fatalf("htpasswd entry for %q not found", username)
}
