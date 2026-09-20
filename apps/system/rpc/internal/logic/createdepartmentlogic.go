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

type CreateDepartmentLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateDepartmentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateDepartmentLogic {
	return &CreateDepartmentLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *CreateDepartmentLogic) CreateDepartment(in *system.CreateDepartmentRequest) (*system.CreateDepartmentResponse, error) {
	if in == nil || in.ParentId < 0 || in.Sort < 0 || !validRecordStatus(in.Status) || !validDepartmentFields(in.Name, in.Remark) {
		return nil, status.Error(codes.InvalidArgument, "invalid create department request")
	}
	department := departmentModel(in.ParentId, in.Name, in.Remark, in.Sort, model.RecordStatus(in.Status))
	if err := l.svcCtx.DepartmentRepo.Create(l.ctx, department); err != nil {
		return nil, departmentError(err)
	}
	return &system.CreateDepartmentResponse{Id: department.ID}, nil
}
