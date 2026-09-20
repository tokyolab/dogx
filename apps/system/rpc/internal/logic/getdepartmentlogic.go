package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetDepartmentLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetDepartmentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDepartmentLogic {
	return &GetDepartmentLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *GetDepartmentLogic) GetDepartment(in *system.GetDepartmentRequest) (*system.GetDepartmentResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidDepartmentRequest()
	}
	item, err := l.svcCtx.DepartmentRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, departmentError(err)
	}
	return &system.GetDepartmentResponse{Department: toDepartmentInfo(*item)}, nil
}
