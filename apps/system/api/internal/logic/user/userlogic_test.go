package user

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type userRPCStub struct {
	systemclient.System
	request         proto.Message
	err             error
	nilResult       bool
	listResponse    *systemclient.ListUsersResponse
	optionsResponse *systemclient.ListUserRoleOptionsResponse
}

func (s *userRPCStub) ListUsers(_ context.Context, in *systemclient.ListUsersRequest, _ ...grpc.CallOption) (*systemclient.ListUsersResponse, error) {
	s.request = in
	if s.nilResult {
		return nil, s.err
	}
	if s.listResponse != nil {
		return s.listResponse, s.err
	}
	return &systemclient.ListUsersResponse{Items: []*systemclient.UserInfo{}, Total: 0}, s.err
}

func (s *userRPCStub) GetUser(_ context.Context, in *systemclient.GetUserRequest, _ ...grpc.CallOption) (*systemclient.GetUserResponse, error) {
	s.request = in
	if s.nilResult {
		return nil, s.err
	}
	return &systemclient.GetUserResponse{User: &systemclient.UserInfo{Id: 9, Username: "alice", Nickname: "Alice"}}, s.err
}

func (s *userRPCStub) CreateUser(_ context.Context, in *systemclient.CreateUserRequest, _ ...grpc.CallOption) (*systemclient.CreateUserResponse, error) {
	s.request = in
	if s.nilResult {
		return nil, s.err
	}
	return &systemclient.CreateUserResponse{Id: 9}, s.err
}

func (s *userRPCStub) UpdateUser(_ context.Context, in *systemclient.UpdateUserRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.nilResult {
		return nil, s.err
	}
	return &systemclient.EmptyResponse{}, s.err
}

func (s *userRPCStub) UpdateUserStatus(_ context.Context, in *systemclient.UpdateUserStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.nilResult {
		return nil, s.err
	}
	return &systemclient.EmptyResponse{}, s.err
}

func (s *userRPCStub) DeleteUser(_ context.Context, in *systemclient.DeleteUserRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.nilResult {
		return nil, s.err
	}
	return &systemclient.EmptyResponse{}, s.err
}

func (s *userRPCStub) ReplaceUserRoles(_ context.Context, in *systemclient.ReplaceUserRolesRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.nilResult {
		return nil, s.err
	}
	return &systemclient.EmptyResponse{}, s.err
}

func (s *userRPCStub) ResetUserPassword(_ context.Context, in *systemclient.ResetUserPasswordRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.nilResult {
		return nil, s.err
	}
	return &systemclient.EmptyResponse{}, s.err
}

func (s *userRPCStub) ListUserRoleOptions(_ context.Context, in *systemclient.ListRolesRequest, _ ...grpc.CallOption) (*systemclient.ListUserRoleOptionsResponse, error) {
	s.request = in
	if s.nilResult {
		return nil, s.err
	}
	if s.optionsResponse != nil {
		return s.optionsResponse, s.err
	}
	return &systemclient.ListUserRoleOptionsResponse{Items: []*systemclient.UserRoleInfo{}}, s.err
}

func authenticatedUserContext() context.Context {
	ctx := context.WithValue(context.Background(), "userId", int64(42))
	ctx = context.WithValue(ctx, "sessionId", "test-session")
	ctx = context.WithValue(ctx, "isSuperAdmin", false)
	return ctx
}

func TestUserAPILogicPreservesRequestAndErrorContracts(t *testing.T) {
	tests := []struct {
		name string
		call func(context.Context, *svc.ServiceContext) error
		want proto.Message
	}{
		{"list", func(ctx context.Context, sc *svc.ServiceContext) error {
			_, err := NewListUsersLogic(ctx, sc).ListUsers(&types.UserListReq{Page: 2, PageSize: 20, Keyword: "Alice"})
			return err
		}, &systemclient.ListUsersRequest{Page: 2, PageSize: 20, Keyword: "Alice"}},
		{"get", func(ctx context.Context, sc *svc.ServiceContext) error {
			_, err := NewGetUserLogic(ctx, sc).GetUser(&types.IDReq{Id: 9})
			return err
		}, &systemclient.GetUserRequest{Id: 9}},
		{"create", func(ctx context.Context, sc *svc.ServiceContext) error {
			_, err := NewCreateUserLogic(ctx, sc).CreateUser(&types.CreateUserReq{Username: "alice", Nickname: "Alice", Password: "abcdefghijkl", Status: 1, RoleIds: []int64{8}})
			return err
		}, &systemclient.CreateUserRequest{Username: "alice", Nickname: "Alice", Password: "abcdefghijkl", Status: 1, RoleIds: []int64{8}}},
		{"update", func(ctx context.Context, sc *svc.ServiceContext) error {
			_, err := NewUpdateUserLogic(ctx, sc).UpdateUser(&types.UpdateUserReq{Id: 9, Nickname: "Alice"})
			return err
		}, &systemclient.UpdateUserRequest{Id: 9, Nickname: "Alice", OperatorId: 42}},
		{"status", func(ctx context.Context, sc *svc.ServiceContext) error {
			_, err := NewUpdateUserStatusLogic(ctx, sc).UpdateUserStatus(&types.UpdateUserStatusReq{Id: 9, Status: 0})
			return err
		}, &systemclient.UpdateUserStatusRequest{Id: 9, Status: 0, OperatorId: 42}},
		{"delete", func(ctx context.Context, sc *svc.ServiceContext) error {
			_, err := NewDeleteUserLogic(ctx, sc).DeleteUser(&types.IDReq{Id: 9})
			return err
		}, &systemclient.DeleteUserRequest{Id: 9, OperatorId: 42}},
		{"roles", func(ctx context.Context, sc *svc.ServiceContext) error {
			_, err := NewUpdateUserRolesLogic(ctx, sc).UpdateUserRoles(&types.UpdateUserRolesReq{Id: 9, RoleIds: []int64{8}})
			return err
		}, &systemclient.ReplaceUserRolesRequest{Id: 9, RoleIds: []int64{8}, OperatorId: 42}},
		{"password", func(ctx context.Context, sc *svc.ServiceContext) error {
			_, err := NewResetUserPasswordLogic(ctx, sc).ResetUserPassword(&types.ResetUserPasswordReq{Id: 9, Password: "abcdefghijkl"})
			return err
		}, &systemclient.ResetUserPasswordRequest{Id: 9, Password: "abcdefghijkl", OperatorId: 42}},
		{"options", func(ctx context.Context, sc *svc.ServiceContext) error {
			_, err := NewListUserRoleOptionsLogic(ctx, sc).ListUserRoleOptions(&types.UserRoleOptionsReq{Page: 1, PageSize: 20, Keyword: "read"})
			return err
		}, &systemclient.ListRolesRequest{Page: 1, PageSize: 20, Keyword: "read"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, rpcErr := range []error{nil, bizerror.New("system.user.username_exists", "readable diagnostic"), status.Error(codes.Unavailable, "RPC offline")} {
				rpc := &userRPCStub{err: rpcErr}
				err := test.call(authenticatedUserContext(), &svc.ServiceContext{SystemRpc: rpc})
				if !errors.Is(err, rpcErr) || !proto.Equal(rpc.request, test.want) {
					t.Fatalf("mapping mismatch: got=%v want=%v error=%v", rpc.request, test.want, err)
				}
			}
		})
	}
}

func TestUserAPIMutationsRequireAuthenticatedOperator(t *testing.T) {
	rpc := &userRPCStub{}
	sc := &svc.ServiceContext{SystemRpc: rpc}
	calls := []func() error{
		func() error {
			_, err := NewUpdateUserLogic(context.Background(), sc).UpdateUser(&types.UpdateUserReq{Id: 9, Nickname: "Alice"})
			return err
		},
		func() error {
			_, err := NewUpdateUserStatusLogic(context.Background(), sc).UpdateUserStatus(&types.UpdateUserStatusReq{Id: 9})
			return err
		},
		func() error {
			_, err := NewDeleteUserLogic(context.Background(), sc).DeleteUser(&types.IDReq{Id: 9})
			return err
		},
		func() error {
			_, err := NewUpdateUserRolesLogic(context.Background(), sc).UpdateUserRoles(&types.UpdateUserRolesReq{Id: 9})
			return err
		},
		func() error {
			_, err := NewResetUserPasswordLogic(context.Background(), sc).ResetUserPassword(&types.ResetUserPasswordReq{Id: 9})
			return err
		},
	}
	for _, call := range calls {
		if err := call(); status.Code(err) != codes.Unauthenticated || rpc.request != nil {
			t.Fatalf("unauthenticated mutation reached RPC: %v", err)
		}
	}
}

func TestUserAPIRejectsNilInputAndNilQueryResponses(t *testing.T) {
	sc := &svc.ServiceContext{SystemRpc: &userRPCStub{nilResult: true}}
	ctx := authenticatedUserContext()
	nilCalls := []func() error{
		func() error { _, err := NewListUsersLogic(ctx, sc).ListUsers(nil); return err },
		func() error { _, err := NewGetUserLogic(ctx, sc).GetUser(nil); return err },
		func() error { _, err := NewCreateUserLogic(ctx, sc).CreateUser(nil); return err },
		func() error { _, err := NewUpdateUserLogic(ctx, sc).UpdateUser(nil); return err },
		func() error { _, err := NewUpdateUserStatusLogic(ctx, sc).UpdateUserStatus(nil); return err },
		func() error { _, err := NewDeleteUserLogic(ctx, sc).DeleteUser(nil); return err },
		func() error { _, err := NewUpdateUserRolesLogic(ctx, sc).UpdateUserRoles(nil); return err },
		func() error { _, err := NewResetUserPasswordLogic(ctx, sc).ResetUserPassword(nil); return err },
		func() error { _, err := NewListUserRoleOptionsLogic(ctx, sc).ListUserRoleOptions(nil); return err },
	}
	for _, call := range nilCalls {
		if err := call(); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	queryCalls := []func() error{
		func() error {
			_, err := NewListUsersLogic(ctx, sc).ListUsers(&types.UserListReq{Page: 1, PageSize: 20})
			return err
		},
		func() error { _, err := NewGetUserLogic(ctx, sc).GetUser(&types.IDReq{Id: 9}); return err },
		func() error { _, err := NewCreateUserLogic(ctx, sc).CreateUser(&types.CreateUserReq{}); return err },
		func() error {
			_, err := NewListUserRoleOptionsLogic(ctx, sc).ListUserRoleOptions(&types.UserRoleOptionsReq{Page: 1, PageSize: 20})
			return err
		},
	}
	for _, call := range queryCalls {
		if err := call(); status.Code(err) != codes.Internal {
			t.Fatal(err)
		}
	}
}

func TestUserAPIListPreservesRecordsRolesAndTotal(t *testing.T) {
	rpc := &userRPCStub{listResponse: &systemclient.ListUsersResponse{
		Items: []*systemclient.UserInfo{
			{
				Id: 9, Username: "alice", Nickname: "Alice", Email: "alice@example.com", Phone: "123",
				Remark: "note", Status: 1, CreatedAt: "2026-09-07T08:30:00Z", UpdatedAt: "2026-09-08T08:30:00Z",
				LastLoginAt: "2026-09-08T09:30:00Z",
				Roles: []*systemclient.UserRoleInfo{
					{Id: 8, Code: "reader", Name: "Reader", Status: 0},
					{Id: 10, Code: "editor", Name: "Editor", Status: 1},
				},
			},
			{Id: 7, Username: "bob", Nickname: "Bob", Status: 0},
		},
		Total: 37,
	}}
	response, err := NewListUsersLogic(authenticatedUserContext(), &svc.ServiceContext{SystemRpc: rpc}).
		ListUsers(&types.UserListReq{Page: 2, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := &types.UserListResp{
		Items: []types.UserItem{
			{
				Id: 9, Username: "alice", Nickname: "Alice", Email: "alice@example.com", Phone: "123",
				Remark: "note", Status: 1, CreatedAt: "2026-09-07T08:30:00Z", UpdatedAt: "2026-09-08T08:30:00Z",
				LastLoginAt: "2026-09-08T09:30:00Z",
				Roles: []types.UserRoleItem{
					{Id: 8, Code: "reader", Name: "Reader", Status: 0},
					{Id: 10, Code: "editor", Name: "Editor", Status: 1},
				},
			},
			{Id: 7, Username: "bob", Nickname: "Bob", Status: 0, Roles: []types.UserRoleItem{}},
		},
		Total: 37,
	}
	if !reflect.DeepEqual(response, want) {
		t.Fatalf("user page lost fields, ordering or total: got=%+v want=%+v", response, want)
	}
}

func TestUserAPIRoleOptionsPreserveItemsAndTotal(t *testing.T) {
	rpc := &userRPCStub{optionsResponse: &systemclient.ListUserRoleOptionsResponse{
		Items: []*systemclient.UserRoleInfo{
			{Id: 8, Code: "reader", Name: "Reader", Status: 1},
			{Id: 10, Code: "editor", Name: "Editor", Status: 1},
		},
		Total: 25,
	}}
	response, err := NewListUserRoleOptionsLogic(authenticatedUserContext(), &svc.ServiceContext{SystemRpc: rpc}).
		ListUserRoleOptions(&types.UserRoleOptionsReq{Page: 2, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := &types.UserRoleOptionsResp{
		Items: []types.UserRoleItem{
			{Id: 8, Code: "reader", Name: "Reader", Status: 1},
			{Id: 10, Code: "editor", Name: "Editor", Status: 1},
		},
		Total: 25,
	}
	if !reflect.DeepEqual(response, want) {
		t.Fatalf("role options lost fields, ordering or total: got=%+v want=%+v", response, want)
	}
}

func TestUserAPIEmptyPagesKeepTotalsAndNonNilItems(t *testing.T) {
	rpc := &userRPCStub{
		listResponse:    &systemclient.ListUsersResponse{Total: 7},
		optionsResponse: &systemclient.ListUserRoleOptionsResponse{Total: 3},
	}
	sc := &svc.ServiceContext{SystemRpc: rpc}
	ctx := authenticatedUserContext()
	users, err := NewListUsersLogic(ctx, sc).ListUsers(&types.UserListReq{Page: 10, PageSize: 20})
	if err != nil || users == nil || users.Total != 7 || users.Items == nil || len(users.Items) != 0 {
		t.Fatalf("empty user page must retain total and an empty array: %+v error=%v", users, err)
	}
	roles, err := NewListUserRoleOptionsLogic(ctx, sc).ListUserRoleOptions(&types.UserRoleOptionsReq{Page: 10, PageSize: 20})
	if err != nil || roles == nil || roles.Total != 3 || roles.Items == nil || len(roles.Items) != 0 {
		t.Fatalf("empty role page must retain total and an empty array: %+v error=%v", roles, err)
	}
}
