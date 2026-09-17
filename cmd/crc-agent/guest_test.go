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
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReadGuestKubeconfigRetriesUntilPopulated(t *testing.T) {
	runner := &fakePrivilegedRunner{run: func(_ string, call int) (string, error) {
		switch call {
		case 1:
			return "not: [valid", nil
		case 2:
			return "clusters: []\n", nil
		default:
			return "clusters:\n- name: crc\n", nil
		}
	}}

	got, err := readGuestKubeconfig(context.Background(), runner, time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("readGuestKubeconfig: %v", err)
	}
	if got != "clusters:\n- name: crc\n" {
		t.Fatalf("kubeconfig = %q, want populated kubeconfig", got)
	}
	if runner.calls != 3 {
		t.Fatalf("attempts = %d, want 3", runner.calls)
	}
}

func TestReadGuestKubeconfigCancellationStopsFurtherAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &fakePrivilegedRunner{run: func(_ string, call int) (string, error) {
		if call == 1 {
			cancel()
		}
		return "", errors.New("guest is not ready")
	}}

	_, err := readGuestKubeconfig(ctx, runner, time.Minute, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if runner.calls != 1 {
		t.Fatalf("attempts = %d, want 1", runner.calls)
	}
}

func TestReadGuestKubeconfigAlreadyCancelledMakesNoAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runner := &fakePrivilegedRunner{}

	_, err := readGuestKubeconfig(ctx, runner, time.Minute, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if runner.calls != 0 {
		t.Fatalf("attempts = %d, want 0", runner.calls)
	}
}

func TestReadGuestKubeconfigTimeoutIncludesLastError(t *testing.T) {
	runner := &fakePrivilegedRunner{run: func(_ string, _ int) (string, error) {
		return "", errors.New("guest kubeconfig is unavailable")
	}}

	_, err := readGuestKubeconfig(context.Background(), runner, 20*time.Millisecond, time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if !strings.Contains(err.Error(), "guest kubeconfig is unavailable") {
		t.Fatalf("error = %v, want last guest error", err)
	}
	if runner.calls == 0 {
		t.Fatal("expected at least one attempt")
	}
}

func TestRetryRunPrivilegedRetriesAndSucceeds(t *testing.T) {
	runner := &fakePrivilegedRunner{run: func(cmd string, call int) (string, error) {
		if cmd != "oc get clusteroperator" {
			return "", errors.New("unexpected command")
		}
		if call < 3 {
			return "", errors.New("API server is not ready")
		}
		return "ok", nil
	}}

	if err := retryRunPrivileged(
		context.Background(), runner, "oc get clusteroperator", time.Second, time.Millisecond,
	); err != nil {
		t.Fatalf("retryRunPrivileged: %v", err)
	}
	if runner.calls != 3 {
		t.Fatalf("attempts = %d, want 3", runner.calls)
	}
}

func TestRetryRunPrivilegedCancellationStopsFurtherAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &fakePrivilegedRunner{run: func(_ string, call int) (string, error) {
		if call == 1 {
			cancel()
		}
		return "", errors.New("API server is not ready")
	}}

	err := retryRunPrivileged(ctx, runner, "oc patch configmap", time.Minute, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if runner.calls != 1 {
		t.Fatalf("attempts = %d, want 1", runner.calls)
	}
}

func TestRetryRunPrivilegedAlreadyCancelledMakesNoAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runner := &fakePrivilegedRunner{}

	err := retryRunPrivileged(ctx, runner, "oc patch configmap", time.Minute, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if runner.calls != 0 {
		t.Fatalf("attempts = %d, want 0", runner.calls)
	}
}

func TestRetryRunPrivilegedTimeoutIncludesLastError(t *testing.T) {
	runner := &fakePrivilegedRunner{run: func(_ string, _ int) (string, error) {
		return "", errors.New("API server refused connection")
	}}

	err := retryRunPrivileged(context.Background(), runner, "oc patch configmap", 20*time.Millisecond, time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if !strings.Contains(err.Error(), "API server refused connection") {
		t.Fatalf("error = %v, want last guest error", err)
	}
	if runner.calls == 0 {
		t.Fatal("expected at least one attempt")
	}
}

type fakePrivilegedRunner struct {
	calls int
	run   func(cmd string, call int) (string, error)
}

func (f *fakePrivilegedRunner) RunPrivileged(cmd string) (string, error) {
	f.calls++
	if f.run == nil {
		return "", errors.New("fake command failure")
	}
	return f.run(cmd, f.calls)
}
