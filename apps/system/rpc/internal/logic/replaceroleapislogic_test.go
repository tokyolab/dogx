package logic

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rolePolicyWriterStub struct {
	roleID int64
	apiIDs []int64
	result authorization.ReplaceResult
	err    error
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
		{name: "storage unavailable", err: errors.New("postgres unavailable")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logic := NewReplaceRoleAPIsLogic(context.Background(), &svc.ServiceContext{
				RolePolicies: &rolePolicyWriterStub{err: test.err},
			})
			if _, err := logic.ReplaceRoleAPIs(&system.ReplaceRoleAPIsRequest{RoleId: 1}); err == nil {
				t.Fatal("expected role policy error")
			}
		})
	}
}
