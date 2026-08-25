package authorization

import (
	"errors"
	"testing"

	"github.com/casbin/casbin/v3"
)

func TestAuthorizationModelUsesExactRolePathAndMethodMatching(t *testing.T) {
	policy, err := NewModel()
	if err != nil {
		t.Fatalf("create policy model: %v", err)
	}
	enforcer, err := casbin.NewEnforcer(policy)
	if err != nil {
		t.Fatalf("create enforcer: %v", err)
	}
	if added, err := enforcer.AddPolicy("r:2", "/user/get", "POST"); err != nil || !added {
		t.Fatalf("add policy: added=%v err=%v", added, err)
	}

	tests := []struct {
		subject string
		path    string
		method  string
		allowed bool
	}{
		{subject: "r:2", path: "/user/get", method: "POST", allowed: true},
		{subject: "r:7", path: "/user/get", method: "POST"},
		{subject: "r:2", path: "/user/list", method: "POST"},
		{subject: "r:2", path: "/user/get", method: "GET"},
	}
	for _, test := range tests {
		allowed, err := enforcer.Enforce(test.subject, test.path, test.method)
		if err != nil {
			t.Fatalf("enforce %+v: %v", test, err)
		}
		if allowed != test.allowed {
			t.Fatalf("unexpected decision for %+v: %v", test, allowed)
		}
	}
}

func TestRoleSubjectAndPolicyRuleValidation(t *testing.T) {
	if subject, err := RoleSubject(42); err != nil || subject != "r:42" {
		t.Fatalf("unexpected role subject: subject=%q err=%v", subject, err)
	}
	if _, err := RoleSubject(0); !errors.Is(err, ErrInvalidRoleID) {
		t.Fatalf("expected invalid role id, got: %v", err)
	}
	rule, err := PolicyRule(42, " /user/get ", "post")
	if err != nil {
		t.Fatalf("build policy rule: %v", err)
	}
	if rule[0] != "r:42" || rule[1] != "/user/get" || rule[2] != "POST" {
		t.Fatalf("unexpected policy rule: %v", rule)
	}
	if _, err := PolicyRule(42, "user/get", "POST"); err == nil {
		t.Fatal("expected invalid path to be rejected")
	}
}

func TestPolicyDifferenceOnlyReturnsChanges(t *testing.T) {
	current := [][]string{
		{"r:1", "/a", "POST"},
		{"r:1", "/b", "POST"},
		{"r:1", "/c", "POST"},
	}
	target := [][]string{
		{"r:1", "/b", "POST"},
		{"r:1", "/c", "POST"},
		{"r:1", "/d", "POST"},
	}
	removed, added := policyDifference(current, target)
	if len(removed) != 1 || removed[0][1] != "/a" {
		t.Fatalf("unexpected removed policies: %v", removed)
	}
	if len(added) != 1 || added[0][1] != "/d" {
		t.Fatalf("unexpected added policies: %v", added)
	}
}

func TestNormalizedIDsSortsAndDeduplicates(t *testing.T) {
	ids, err := normalizedIDs([]int64{7, 2, 7, 1})
	if err != nil {
		t.Fatalf("normalize ids: %v", err)
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 7 {
		t.Fatalf("unexpected normalized ids: %v", ids)
	}
	if _, err := normalizedIDs([]int64{1, 0}); err == nil {
		t.Fatal("expected invalid API id to be rejected")
	}
}
