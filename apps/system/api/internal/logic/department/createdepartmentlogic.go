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

type CreateDepartmentLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateDepartmentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateDepartmentLogic {
	return &CreateDepartmentLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateDepartmentLogic) CreateDepartment(req *types.CreateDepartmentReq) (resp *types.CreateDepartmentResp, err error) {
	if req == nil {
		return nil, invalidDepartmentRequest()
	}
	result, err := l.svcCtx.SystemRpc.CreateDepartment(l.ctx, &systemclient.CreateDepartmentRequest{ParentId: req.ParentId, Name: req.Name, Sort: req.Sort, Remark: req.Remark, Status: req.Status})
	if err != nil {
		return nil, err
	}
	if result == nil || result.GetId() <= 0 {
		return nil, status.Error(codes.Internal, "system RPC returned an invalid department id")
	}
	return &types.CreateDepartmentResp{Id: result.GetId()}, nil
}
