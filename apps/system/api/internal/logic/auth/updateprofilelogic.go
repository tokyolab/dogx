// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package auth

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateProfileLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateProfileLogic {
	return &UpdateProfileLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateProfileLogic) UpdateProfile(req *types.UpdateProfileReq) (resp *types.EmptyResp, err error) {
	identity, err := authenticatedIdentity(l.ctx)
	if err != nil {
		return nil, err
	}
	// Only the verified identity can select the account; request JSON cannot.
	_, err = l.svcCtx.SystemRpc.UpdateProfile(l.ctx, &systemclient.UpdateProfileRequest{
		UserId: identity.UserID, Nickname: req.Nickname, Email: req.Email, Phone: req.Phone,
	})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
