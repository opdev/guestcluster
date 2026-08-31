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

// Package main - sshrunner.go
//
// This file provides a trimmed SSH runner for the crc-agent. The logic is
// ported from github.com/crc-org/crc pkg/crc/ssh/ssh.go (commit ~2025-Q3),
// with the crc-specific constants and logging coupling stripped out. The
// Runner wraps a single *gossh.Client and provides the helpers crc-agent
// needs:
//
//   - Run and RunPrivileged: execute commands on the guest (sudo for
//     privileged commands).
//   - CopyData and CopyDataPrivileged: write bytes to a remote path
//     atomically, using the same base64+install+tee idiom crc uses, so no
//     sftp dependency is needed.
//   - WaitForConnectivity: poll until the guest's SSH port accepts
//     connections.
//   - DialAPIServer: open a TCP channel through the SSH session to the
//     guest's api.crc.testing:6443 endpoint, for use as the Dial function
//     in a rest.Config (see clusterclient.go).
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"time"

	gossh "golang.org/x/crypto/ssh"
)

// Runner is a thin wrapper around a single *gossh.Client that exposes the
// subset of SSH operations the crc-agent needs.
type Runner struct {
	client *gossh.Client
	// user is the remote user (always "core" for CRC bundles).
	user string
}

// NewRunner dials host:port as user using signer and returns a ready Runner.
// The caller must call Close() when done.
func NewRunner(host string, port int, user string, signer gossh.Signer) (*Runner, error) {
	cfg := &gossh.ClientConfig{
		User:            user,
		Auth:            []gossh.AuthMethod{gossh.PublicKeys(signer)},
		HostKeyCallback: gossh.InsecureIgnoreHostKey(), //nolint:gosec // CRC bundles have no pre-known host key
		Timeout:         30 * time.Second,
	}
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	client, err := gossh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}
	return &Runner{client: client, user: user}, nil
}

// Close releases the underlying SSH connection.
func (r *Runner) Close() error {
	return r.client.Close()
}

// Run executes cmd on the guest and returns combined stdout+stderr.
// A non-zero exit code is returned as an error.
func (r *Runner) Run(cmd string) (string, error) {
	return r.runSession(cmd)
}

// RunPrivileged prepends sudo to cmd.
func (r *Runner) RunPrivileged(cmd string) (string, error) {
	return r.runSession("sudo " + cmd)
}

func (r *Runner) runSession(cmd string) (string, error) {
	sess, err := r.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("new ssh session: %w", err)
	}
	defer func() { _ = sess.Close() }()

	var buf bytes.Buffer
	sess.Stdout = &buf
	sess.Stderr = &buf
	if err := sess.Run(cmd); err != nil {
		return buf.String(), fmt.Errorf("running %q: %w (output: %s)", cmd, err, buf.String())
	}
	return buf.String(), nil
}

// CopyData writes data to remotePath (mode 0o644) on the guest.
// Uses the base64+install+tee idiom so no sftp daemon is needed.
// Ported from crc pkg/crc/ssh/ssh.go copyDataFull.
func (r *Runner) CopyData(data []byte, remotePath string) error {
	return r.copyDataFull(data, remotePath, 0o644, false)
}

// CopyDataPrivileged writes data to remotePath via sudo (mode 0o644).
func (r *Runner) CopyDataPrivileged(data []byte, remotePath string) error {
	return r.copyDataFull(data, remotePath, 0o644, true)
}

// AppendDataPrivileged appends data to remotePath via sudo, creating the file
// (with the given mode) first if it does not already exist. Unlike
// CopyDataPrivileged this never truncates an existing file, which matters for
// files like ~/.ssh/authorized_keys where clobbering existing content would
// lock out other keys (e.g. the bundle's original SSH key, which a retried
// crc-agent Job pod must still be able to authenticate with).
func (r *Runner) AppendDataPrivileged(data []byte, remotePath string, mode uint32) error {
	encoded := base64.StdEncoding.EncodeToString(data)
	cmd := fmt.Sprintf(
		`sudo test -f %q || sudo install -m 0%o -D /dev/null %q; `+
			`echo %s | base64 --decode | sudo tee -a %q > /dev/null`,
		remotePath, mode, remotePath,
		encoded, remotePath,
	)
	_, err := r.runSession(cmd)
	return err
}

// copyDataFull implements the base64+install+tee upload idiom from crc.
// The install step creates the file with the right mode before tee writes it,
// so permissions are set atomically.
func (r *Runner) copyDataFull(data []byte, remotePath string, mode uint32, sudo bool) error {
	encoded := base64.StdEncoding.EncodeToString(data)
	prefix := ""
	if sudo {
		prefix = "sudo "
	}
	// install creates the file (and any parent dirs via -D) with the right mode,
	// then tee overwrites it with the base64-decoded content.
	cmd := fmt.Sprintf(
		`%sinstall -m 0%o -D /dev/null %q && echo %s | base64 --decode | %stee %q > /dev/null`,
		prefix, mode, remotePath,
		encoded,
		prefix, remotePath,
	)
	_, err := r.runSession(cmd)
	return err
}

// WaitForConnectivity polls host:port until it accepts a TCP connection or
// ctx is cancelled. interval sets how often to retry.
// Ported from crc pkg/crc/ssh/ssh.go WaitForConnectivity.
func WaitForConnectivity(ctx context.Context, host string, port int, interval time.Duration) error {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	for {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// DialAPIServer opens a TCP channel through the existing SSH session that
// terminates at the guest's api.crc.testing:6443. The returned net.Conn is
// suitable for use as the Dial function in a rest.Config (see
// clusterclient.go). The channel runs inside the existing authenticated
// SSH session, so it does not need to re-authenticate.
func (r *Runner) DialAPIServer() (net.Conn, error) {
	conn, err := r.client.Dial("tcp", "api.crc.testing:6443")
	if err != nil {
		return nil, fmt.Errorf("ssh channel to api.crc.testing:6443: %w", err)
	}
	return conn, nil
}
