package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/zeromicro/go-zero/core/logx"
)

type ListDepartmentsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListDepartmentsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListDepartmentsLogic {
	return &ListDepartmentsLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *ListDepartmentsLogic) ListDepartments(_ *system.ListDepartmentsRequest) (*system.ListDepartmentsResponse, error) {
	items, err := l.svcCtx.DepartmentRepo.List(l.ctx)
	if err != nil {
		return nil, departmentError(err)
	}
	result := make([]*system.DepartmentInfo, 0, len(items))
	for _, item := range items {
		result = append(result, toDepartmentInfo(item))
	}
	return &system.ListDepartmentsResponse{Items: result}, nil
}
