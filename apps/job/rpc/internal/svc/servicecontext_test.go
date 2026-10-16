package svc

import (
	"context"
	"testing"
	"time"
)

func TestCloseWaitsForRuntimeCleanupAndIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runtimeDone := make(chan struct{})
	s := &ServiceContext{cancel: cancel, done: runtimeDone}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("close did not cancel runtime")
	}
	select {
	case err := <-closed:
		t.Fatalf("close returned before runtime stopped: %v", err)
	default:
	}
	close(runtimeDone)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not finish")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceClock(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	if got := (&ServiceContext{Clock: func() time.Time { return now }}).Now(); !got.Equal(now) {
		t.Fatalf("clock=%s", got)
	}
	before := time.Now()
	got := (&ServiceContext{}).Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("fallback clock=%s", got)
	}
	if err := (&ServiceContext{}).Close(); err != nil {
		t.Fatal(err)
	}
}
