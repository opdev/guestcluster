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
	"net"
	"sync"
	"testing"
	"time"
)

func TestWaitForConnectivityClosesSuccessfulProbe(t *testing.T) {
	originalDial := dialTCPContext
	defer func() { dialTCPContext = originalDial }()

	client, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	probe := &trackingConn{Conn: client, closed: make(chan struct{})}
	dialTCPContext = func(_ context.Context, _ *net.Dialer, _ string) (net.Conn, error) {
		return probe, nil
	}

	if err := WaitForConnectivity(context.Background(), "guest", 22, time.Millisecond); err != nil {
		t.Fatalf("WaitForConnectivity: %v", err)
	}
	select {
	case <-probe.closed:
	default:
		t.Fatal("successful probe connection was not closed")
	}
}

func TestWaitForConnectivityCancellationStopsFurtherAttempts(t *testing.T) {
	originalDial := dialTCPContext
	defer func() { dialTCPContext = originalDial }()

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	dialTCPContext = func(_ context.Context, _ *net.Dialer, _ string) (net.Conn, error) {
		calls++
		cancel()
		return nil, errors.New("guest is not accepting connections")
	}

	err := WaitForConnectivity(ctx, "guest", 22, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("dial attempts = %d, want 1", calls)
	}
}

func TestWaitForConnectivityAlreadyCancelledMakesNoAttempt(t *testing.T) {
	originalDial := dialTCPContext
	defer func() { dialTCPContext = originalDial }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	dialTCPContext = func(_ context.Context, _ *net.Dialer, _ string) (net.Conn, error) {
		calls++
		return nil, errors.New("unexpected dial")
	}

	err := WaitForConnectivity(ctx, "guest", 22, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("dial attempts = %d, want 0", calls)
	}
}

func TestWaitForConnectivityCancellationInterruptsDial(t *testing.T) {
	originalDial := dialTCPContext
	defer func() { dialTCPContext = originalDial }()

	ctx, cancel := context.WithCancel(context.Background())
	dialStarted := make(chan struct{})
	dialTCPContext = func(ctx context.Context, _ *net.Dialer, _ string) (net.Conn, error) {
		close(dialStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	result := make(chan error, 1)
	go func() { result <- WaitForConnectivity(ctx, "guest", 22, time.Hour) }()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("connectivity check did not start dialing")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt dial")
	}
}

func TestWaitForConnectivityHonorsContextTimeout(t *testing.T) {
	originalDial := dialTCPContext
	defer func() { dialTCPContext = originalDial }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	dialTCPContext = func(ctx context.Context, _ *net.Dialer, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	err := WaitForConnectivity(ctx, "guest", 22, time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
}

type trackingConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *trackingConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}
