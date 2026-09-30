package loginprotection

import (
	"context"
	"errors"
	"testing"
)

type scriptClientStub struct {
	result any
	err    error
	keys   []string
	args   []any
}

func (s *scriptClientStub) EvalCtx(_ context.Context, _ string, keys []string, args ...any) (any, error) {
	s.keys = keys
	s.args = args
	return s.result, s.err
}
func TestLockStoreErrorsAndIdentity(t *testing.T) {
	ctx := context.Background()
	dependencyErr := errors.New("redis down")
	client := &scriptClientStub{err: dependencyErr}
	store := NewFailureStore(client)
	if locked, err := store.Locked(ctx, "Admin"); locked || !errors.Is(err, dependencyErr) {
		t.Fatalf("%v %v", locked, err)
	}
	if err := store.Fail(ctx, "Admin", validConfig()); !errors.Is(err, dependencyErr) {
		t.Fatal(err)
	}
	if len(client.args) != 3 {
		t.Fatal("missing script parameters")
	}
	if err := store.Clear(ctx, "ADMIN"); !errors.Is(err, dependencyErr) {
		t.Fatal(err)
	}
	if len(client.keys) != 1 || client.keys[0] != failureKey("admin") {
		t.Fatal("username keys differ")
	}
	client.err = nil
	client.result = "1"
	if locked, err := store.Locked(ctx, "Admin"); !locked || err != nil {
		t.Fatalf("%v %v", locked, err)
	}
	if failureKey("admin") == failureKey("another") {
		t.Fatal("account keys collide")
	}
}
