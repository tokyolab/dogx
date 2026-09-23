package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProfileUsesAuthenticatedIdentity(t *testing.T) {
	rpc := &systemRPCStub{profileResponse: &systemclient.ProfileResponse{Username: "alice", Nickname: "Alice", Email: "a@example.com", Phone: "123", DepartmentName: "Sales", Roles: []string{"Reader"}}}
	sc := &svc.ServiceContext{SystemRpc: rpc}
	got, err := NewGetProfileLogic(authenticatedTestContext(), sc).GetProfile()
	if err != nil || rpc.profileRequest.UserId != 42 || got.Username != "alice" || got.Nickname != "Alice" || got.Email != "a@example.com" || got.Phone != "123" || got.DepartmentName != "Sales" || len(got.Roles) != 1 || got.Roles[0] != "Reader" {
		t.Fatalf("profile: %+v, %v", got, err)
	}
	_, err = NewUpdateProfileLogic(authenticatedTestContext(), sc).UpdateProfile(&types.UpdateProfileReq{Nickname: "New", Email: "n@example.com", Phone: "456"})
	if err != nil || rpc.updateProfileRequest.UserId != 42 || rpc.updateProfileRequest.Nickname != "New" || rpc.updateProfileRequest.Email != "n@example.com" || rpc.updateProfileRequest.Phone != "456" {
		t.Fatalf("update: %+v, %v", rpc.updateProfileRequest, err)
	}
}

func TestProfileErrors(t *testing.T) {
	rpc := &systemRPCStub{}
	sc := &svc.ServiceContext{SystemRpc: rpc}
	if _, err := NewGetProfileLogic(context.Background(), sc).GetProfile(); err == nil {
		t.Fatal("missing identity accepted")
	}
	if _, err := NewUpdateProfileLogic(context.Background(), sc).UpdateProfile(&types.UpdateProfileReq{}); err == nil {
		t.Fatal("missing identity accepted")
	}
	if rpc.profileRequest != nil || rpc.updateProfileRequest != nil {
		t.Fatal("unauthenticated RPC call")
	}
	if _, err := NewGetProfileLogic(authenticatedTestContext(), sc).GetProfile(); status.Code(err) != codes.Internal {
		t.Fatalf("nil result: %v", err)
	}
	rpc.err = errors.New("rpc unavailable")
	if _, err := NewGetProfileLogic(authenticatedTestContext(), sc).GetProfile(); !errors.Is(err, rpc.err) {
		t.Fatal(err)
	}
	if _, err := NewUpdateProfileLogic(authenticatedTestContext(), sc).UpdateProfile(&types.UpdateProfileReq{Nickname: "Alice"}); !errors.Is(err, rpc.err) {
		t.Fatal(err)
	}
	rpc.err = nil
	rpc.profileResponse = &systemclient.ProfileResponse{}
	got, err := NewGetProfileLogic(authenticatedTestContext(), sc).GetProfile()
	if err != nil || got.Roles == nil {
		t.Fatalf("empty roles must serialize as array: %+v %v", got, err)
	}
}
