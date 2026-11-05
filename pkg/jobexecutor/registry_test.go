package jobexecutor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type ordinaryProcessorStub struct{}

func (*ordinaryProcessorStub) Execute(context.Context, Input) (Result, error) {
	return Result{}, nil
}

type shardingProcessorStub struct{}

func (*shardingProcessorStub) PreProcess(context.Context, Input) (PrepareResult, error) {
	return PrepareResult{Items: []json.RawMessage{}}, nil
}

func (*shardingProcessorStub) ShardingProcess(context.Context, ShardInput) (Result, error) {
	return Result{}, nil
}

func (*shardingProcessorStub) PostProcess(context.Context, PostInput) (Result, error) {
	return Result{}, nil
}

func TestRegistrySharesExactNameNamespace(t *testing.T) {
	r := NewRegistry()
	ordinary := &ordinaryProcessorStub{}
	sharding := &shardingProcessorStub{}
	if err := r.Register("System.Cleanup", ordinary); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterSharding("System.Cleanup", sharding); !errors.Is(err, ErrProcessorExists) {
		t.Fatalf("cross-kind duplicate error = %v", err)
	}
	if err := r.RegisterSharding("system.cleanup", sharding); err != nil {
		t.Fatalf("exact names must be case-sensitive: %v", err)
	}
	if err := r.Register("system.cleanup", ordinary); !errors.Is(err, ErrProcessorExists) {
		t.Fatalf("reverse cross-kind duplicate error = %v", err)
	}
	if got, ok := r.Lookup("System.Cleanup"); !ok || got != ordinary {
		t.Fatalf("ordinary lookup = (%v, %v)", got, ok)
	}
	if got, ok := r.Lookup("system.cleanup"); ok || got != nil {
		t.Fatal("sharding registration was exposed as an ordinary processor")
	}
	got, ok := r.LookupSharding("system.cleanup")
	if !ok || got != sharding {
		t.Fatalf("sharding lookup = (%v, %v)", got, ok)
	}
	if _, ok := got.(PostProcessor); !ok {
		t.Fatal("optional Post capability was not preserved")
	}
	if got, ok := r.LookupSharding("System.Cleanup"); ok || got != nil {
		t.Fatal("ordinary registration was exposed as sharding")
	}
	if got, ok := r.Lookup("missing"); ok || got != nil {
		t.Fatal("missing name returned a processor")
	}
}

func TestRegistryRejectsInvalidRegistrations(t *testing.T) {
	r := NewRegistry()
	for _, name := range []string{"", " ", " leading", "trailing ", "nul\x00name", "\xff", strings.Repeat("界", 129)} {
		if err := r.Register(name, &ordinaryProcessorStub{}); !errors.Is(err, ErrInvalidRegistration) {
			t.Errorf("Register invalid name: error = %v", err)
		}
	}
	if err := r.Register(strings.Repeat("界", 128), &ordinaryProcessorStub{}); err != nil {
		t.Fatalf("128 Unicode characters must be accepted: %v", err)
	}
	var nilOrdinary *ordinaryProcessorStub
	var nilSharding *shardingProcessorStub
	for _, test := range []struct {
		name string
		add  func() error
	}{
		{name: "nil interface", add: func() error { return r.Register("nil", nil) }},
		{name: "typed nil ordinary", add: func() error { return r.Register("typed", nilOrdinary) }},
		{name: "typed nil sharding", add: func() error { return r.RegisterSharding("sharding", nilSharding) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.add(); !errors.Is(err, ErrInvalidRegistration) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRegistryConcurrentRegistrationAndFreeze(t *testing.T) {
	var r Registry
	var accepted atomic.Int32
	var failures atomic.Int32
	var group sync.WaitGroup
	for i := range 32 {
		group.Go(func() {
			var err error
			if i%2 == 0 {
				err = r.Register("shared", &ordinaryProcessorStub{})
			} else {
				err = r.RegisterSharding("shared", &shardingProcessorStub{})
			}
			switch {
			case err == nil:
				accepted.Add(1)
			case !errors.Is(err, ErrProcessorExists):
				failures.Add(1)
			}
		})
	}
	group.Wait()
	if accepted.Load() != 1 || failures.Load() != 0 {
		t.Fatalf("accepted = %d, unexpected failures = %d", accepted.Load(), failures.Load())
	}
	for i := range 32 {
		group.Go(func() {
			r.Freeze()
			if err := r.Register(fmt.Sprintf("after-freeze-%d", i), &ordinaryProcessorStub{}); !errors.Is(err, ErrRegistryFrozen) {
				failures.Add(1)
			}
			r.Lookup("shared")
			r.LookupSharding("shared")
		})
	}
	group.Wait()
	if failures.Load() != 0 {
		t.Fatalf("unexpected errors after Freeze: %d", failures.Load())
	}
}
