// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package user

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateUserLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateUserLogic {
	return &CreateUserLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateUserLogic) CreateUser(req *types.CreateUserReq) (resp *types.CreateUserResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	result, err := l.svcCtx.SystemRpc.CreateUser(l.ctx, &systemclient.CreateUserRequest{Username: req.Username, Nickname: req.Nickname, Password: req.Password, Email: req.Email, Phone: req.Phone, Remark: req.Remark, Status: req.Status, RoleIds: req.RoleIds})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Id <= 0 {
		return nil, status.Error(codes.Internal, "system RPC returned an invalid user id")
	}
	return &types.CreateUserResp{Id: result.Id}, nil
}
