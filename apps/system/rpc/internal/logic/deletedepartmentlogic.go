package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DeleteDepartmentLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteDepartmentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteDepartmentLogic {
	return &DeleteDepartmentLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *DeleteDepartmentLogic) DeleteDepartment(in *system.DeleteDepartmentRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid delete department request")
	}
	if err := l.svcCtx.DepartmentRepo.Delete(l.ctx, in.Id); err != nil {
		return nil, departmentError(err)
	}
	return &system.EmptyResponse{}, nil
}
