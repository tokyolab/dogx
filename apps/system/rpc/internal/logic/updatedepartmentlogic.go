package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UpdateDepartmentLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateDepartmentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateDepartmentLogic {
	return &UpdateDepartmentLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *UpdateDepartmentLogic) UpdateDepartment(in *system.UpdateDepartmentRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || in.ParentId < 0 || in.Sort < 0 || !validDepartmentFields(in.Name, in.Remark) {
		return nil, status.Error(codes.InvalidArgument, "invalid update department request")
	}
	department := departmentModel(in.ParentId, in.Name, in.Remark, in.Sort, model.RecordStatusEnabled)
	if err := l.svcCtx.DepartmentRepo.Update(l.ctx, in.Id, department); err != nil {
		return nil, departmentError(err)
	}
	return &system.EmptyResponse{}, nil
}
