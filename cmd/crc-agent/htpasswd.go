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

// Package main - htpasswd.go
//
// This file provides pure htpasswd and bcrypt helpers for the crc-agent.
// The logic is ported from
// github.com/crc-org/crc pkg/crc/cluster/kubeadmin_password.go
// (commit ~2025-Q3). It uses golang.org/x/crypto/bcrypt, so it needs no
// htpasswd binary, no podman, and no external container.
//
// All functions in this file are stateless pure functions.
package main

import (
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// BCryptHash hashes password with bcrypt at DefaultCost.
// Ported from crc hashBcrypt.
func BCryptHash(password string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("bcrypt hash: %w", err)
	}
	return string(b), nil
}

// BCryptVerify returns nil if hash matches password.
func BCryptVerify(hash, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

// BuildHtpasswd constructs the raw htpasswd file content for the
// given credential map (username -> plaintext password), preserving any
// existing lines (externalLines) that belong to unknown users.
// The result must be stored as raw bytes in Secret.Data. Callers that create a
// JSON patch for the Secret must base64-encode it at that boundary.
//
// Ported from crc getHtpasswd + compareHtpasswd.
func BuildHtpasswd(credentials map[string]string, externalLines []string) (string, error) {
	lines := make([]string, 0, len(externalLines)+len(credentials))
	lines = append(lines, externalLines...)
	for user, pass := range credentials {
		hash, err := BCryptHash(pass)
		if err != nil {
			return "", fmt.Errorf("hashing password for %q: %w", user, err)
		}
		lines = append(lines, fmt.Sprintf("%s:%s", user, hash))
	}
	return strings.Join(lines, "\n"), nil
}

// ParseExternalHtpasswdLines parses raw htpasswd file content (as returned by
// the Kubernetes client's Secret.Data) and returns the lines that do not
// belong to any of the named users. This preserves external and unknown
// users across updates, matching crc's compareHtpasswd behavior. No base64
// decode is needed because Secret.Data is already decoded.
func ParseExternalHtpasswdLines(htpasswd string, ownedUsers []string) []string {
	owned := make(map[string]bool, len(ownedUsers))
	for _, u := range ownedUsers {
		owned[u] = true
	}
	var external []string
	for _, line := range strings.Split(htpasswd, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		if !owned[parts[0]] {
			external = append(external, line)
		}
	}
	return external
}
