// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package role

import (
	"context"
	"strings"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CreateRoleLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Create a role
func NewCreateRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateRoleLogic {
	return &CreateRoleLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateRoleLogic) CreateRole(req *types.CreateRoleReq) (resp *types.CreateRoleResp, err error) {
	if req == nil || strings.TrimSpace(req.Code) == "" || strings.TrimSpace(req.Name) == "" ||
		req.Sort < 0 || (req.Status != 0 && req.Status != 1) {
		return nil, status.Error(codes.InvalidArgument, "invalid create role request")
	}
	result, err := l.svcCtx.SystemRpc.CreateRole(l.ctx, &systemclient.CreateRoleRequest{
		Code:        req.Code,
		Name:        req.Name,
		Description: req.Description,
		Sort:        req.Sort,
		Status:      req.Status,
	})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Id <= 0 {
		return nil, status.Error(codes.Internal, "system RPC returned an invalid role id")
	}

	return &types.CreateRoleResp{Id: result.Id}, nil
}
