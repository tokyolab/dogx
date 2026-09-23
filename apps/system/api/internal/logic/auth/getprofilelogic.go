// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package auth

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetProfileLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetProfileLogic {
	return &GetProfileLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetProfileLogic) GetProfile() (resp *types.ProfileResp, err error) {
	identity, err := authenticatedIdentity(l.ctx)
	if err != nil {
		return nil, err
	}
	result, err := l.svcCtx.SystemRpc.GetProfile(l.ctx, &systemclient.CurrentUserRequest{UserId: identity.UserID})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "profile response is nil")
	}
	roles := append([]string{}, result.Roles...)
	return &types.ProfileResp{Username: result.Username, Nickname: result.Nickname,
		Email: result.Email, Phone: result.Phone, DepartmentName: result.DepartmentName, Roles: roles}, nil
}
