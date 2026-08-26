package authorization

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestNewReadinessValidatesDependencies(t *testing.T) {
	reloader, err := NewPolicyReloader(&policyLoaderStub{})
	if err != nil {
		t.Fatalf("create policy reloader: %v", err)
	}
	if _, err := NewReadiness(nil, reloader); err == nil {
		t.Fatal("expected nil readiness database to be rejected")
	}
	if _, err := NewReadiness(&sql.DB{}, nil); err == nil {
		t.Fatal("expected nil policy reloader to be rejected")
	}
}

func TestReadinessRejectsInvalidContextAndUnloadedPolicy(t *testing.T) {
	reloader, err := NewPolicyReloader(&policyLoaderStub{})
	if err != nil {
		t.Fatalf("create policy reloader: %v", err)
	}
	checker, err := NewReadiness(&sql.DB{}, reloader)
	if err != nil {
		t.Fatalf("create authorization readiness: %v", err)
	}

	if err := checker.Check(nil); err == nil || !strings.Contains(err.Error(), "context") {
		t.Fatalf("unexpected nil-context result: %v", err)
	}
	if err := checker.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "not loaded") {
		t.Fatalf("unexpected unloaded-policy result: %v", err)
	}
}
