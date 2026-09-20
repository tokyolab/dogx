// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package department

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetDepartmentLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetDepartmentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDepartmentLogic {
	return &GetDepartmentLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetDepartmentLogic) GetDepartment(req *types.IDReq) (resp *types.DepartmentItem, err error) {
	if req == nil || req.Id <= 0 {
		return nil, invalidDepartmentRequest()
	}
	result, err := l.svcCtx.SystemRpc.GetDepartment(l.ctx, &systemclient.GetDepartmentRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned no department")
	}
	item, err := ToDepartmentItem(result.GetDepartment())
	if err != nil {
		return nil, err
	}
	return &item, nil
}
