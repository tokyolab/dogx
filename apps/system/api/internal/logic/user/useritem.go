package user

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/authctx"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func operatorID(ctx context.Context) (int64, error) {
	identity, err := authctx.FromContext(ctx)
	if err != nil {
		return 0, status.Error(codes.Unauthenticated, "authentication required")
	}
	return identity.UserID, nil
}

func toUserRoleItem(role *systemclient.UserRoleInfo) types.UserRoleItem {
	return types.UserRoleItem{Id: role.GetId(), Code: role.GetCode(), Name: role.GetName(), Status: role.GetStatus()}
}

func toUserItem(user *systemclient.UserInfo) types.UserItem {
	roles := make([]types.UserRoleItem, 0, len(user.GetRoles()))
	for _, role := range user.GetRoles() {
		roles = append(roles, toUserRoleItem(role))
	}
	return types.UserItem{Id: user.GetId(), Username: user.GetUsername(), Nickname: user.GetNickname(),
		Email: user.GetEmail(), Phone: user.GetPhone(), Remark: user.GetRemark(), Status: user.GetStatus(), Roles: roles,
		CreatedAt: user.GetCreatedAt(), UpdatedAt: user.GetUpdatedAt(), LastLoginAt: user.GetLastLoginAt()}
}
