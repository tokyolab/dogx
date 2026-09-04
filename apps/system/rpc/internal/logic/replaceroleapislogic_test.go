package logic

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	systemsubcode "github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rolePolicyWriterStub struct {
	roleID       int64
	apiIDs       []int64
	result       authorization.ReplaceResult
	err          error
	listed       []int64
	listErr      error
	deleted      bool
	deleteResult authorization.DeleteRoleResult
}

func (s *rolePolicyWriterStub) ListRoleAPIIDs(_ context.Context, roleID int64) ([]int64, error) {
	s.roleID = roleID
	return append([]int64(nil), s.listed...), s.listErr
}

func (s *rolePolicyWriterStub) ReplaceRoleAPIs(
	_ context.Context,
	roleID int64,
	apiIDs []int64,
) (authorization.ReplaceResult, error) {
	s.roleID = roleID
	s.apiIDs = append([]int64(nil), apiIDs...)
	return s.result, s.err
}

func (s *rolePolicyWriterStub) DeleteRole(
	_ context.Context,
	roleID int64,
) (authorization.DeleteRoleResult, error) {
	s.roleID = roleID
	s.deleted = true
	return s.deleteResult, s.err
}

func TestReplaceRoleAPIsDelegatesCompleteTargetSet(t *testing.T) {
	writer := &rolePolicyWriterStub{result: authorization.ReplaceResult{
		Added:             1,
		Removed:           2,
		NotificationError: errors.New("Redis notification failed"),
	}}
	logic := NewReplaceRoleAPIsLogic(context.Background(), &svc.ServiceContext{RolePolicies: writer})
	response, err := logic.ReplaceRoleAPIs(&system.ReplaceRoleAPIsRequest{
		RoleId: 7,
		ApiIds: []int64{2, 3, 5},
	})
	if err != nil {
		t.Fatalf("replace role APIs: %v", err)
	}
	if response == nil || writer.roleID != 7 || len(writer.apiIDs) != 3 || writer.apiIDs[2] != 5 {
		t.Fatalf("unexpected delegated request: response=%+v roleId=%d apiIds=%v", response, writer.roleID, writer.apiIDs)
	}
}

func TestReplaceRoleAPIsValidatesAndMapsKnownErrors(t *testing.T) {
	if _, err := NewReplaceRoleAPIsLogic(context.Background(), &svc.ServiceContext{}).
		ReplaceRoleAPIs(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unexpected nil request error: %v", err)
	}
	if _, err := NewReplaceRoleAPIsLogic(context.Background(), &svc.ServiceContext{}).
		ReplaceRoleAPIs(&system.ReplaceRoleAPIsRequest{RoleId: 1}); err == nil {
		t.Fatal("expected missing role policy service to be rejected")
	}

	tests := []struct {
		name string
		err  error
	}{
		{name: "invalid role id", err: authorization.ErrInvalidRoleID},
		{name: "role unavailable", err: authorization.ErrRoleUnavailable},
		{name: "API unavailable", err: authorization.ErrAPIUnavailable},
		{name: "super administrator protected", err: authorization.ErrSuperAdminPolicyProtected},
		{name: "storage unavailable", err: errors.New("postgres unavailable")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logic := NewReplaceRoleAPIsLogic(context.Background(), &svc.ServiceContext{
				RolePolicies: &rolePolicyWriterStub{err: test.err},
			})
			_, err := logic.ReplaceRoleAPIs(&system.ReplaceRoleAPIsRequest{RoleId: 1})
			if err == nil {
				t.Fatal("expected role policy error")
			}
			if test.err == authorization.ErrSuperAdminPolicyProtected &&
				!hasBusinessSubcode(err, systemsubcode.RoleSuperAdminAPIProtected) {
				t.Fatalf("unexpected super administrator error: %v", err)
			}
		})
	}
}
