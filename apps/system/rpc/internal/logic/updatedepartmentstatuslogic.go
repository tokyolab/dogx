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

type UpdateDepartmentStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateDepartmentStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateDepartmentStatusLogic {
	return &UpdateDepartmentStatusLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *UpdateDepartmentStatusLogic) UpdateDepartmentStatus(in *system.UpdateDepartmentStatusRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || !validRecordStatus(in.Status) {
		return nil, status.Error(codes.InvalidArgument, "invalid update department status request")
	}
	if err := l.svcCtx.DepartmentRepo.UpdateStatus(l.ctx, in.Id, model.RecordStatus(in.Status)); err != nil {
		return nil, departmentError(err)
	}
	return &system.EmptyResponse{}, nil
}
