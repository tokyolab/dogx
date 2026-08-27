package authorization

import (
	"testing"

	"gorm.io/gorm"
)

type policyServiceNotifierStub struct{}

func (policyServiceNotifierStub) Update() error { return nil }

func TestNewRolePolicyServiceRejectsMissingDependencies(t *testing.T) {
	if _, err := NewRolePolicyService(nil, policyServiceNotifierStub{}); err == nil {
		t.Fatal("nil database was accepted")
	}
	if _, err := NewRolePolicyService(&gorm.DB{}, nil); err == nil {
		t.Fatal("nil notifier was accepted")
	}
}

func TestPolicyDifferenceReportsChangedRules(t *testing.T) {
	current := [][]string{{"r:1", "/a", "POST"}, {"r:1", "/b", "POST"}}
	target := [][]string{{"r:1", "/b", "POST"}, {"r:1", "/c", "POST"}}

	removed, added := policyDifference(current, target)
	if len(removed) != 1 || len(added) != 1 {
		t.Fatalf("unexpected difference: removed=%v added=%v", removed, added)
	}
	if removed[0][1] != "/a" || added[0][1] != "/c" {
		t.Fatalf("unexpected changed rules: removed=%v added=%v", removed, added)
	}
}
