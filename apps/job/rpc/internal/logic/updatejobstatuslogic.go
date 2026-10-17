package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	basemodel "github.com/tokyolab/dogx/pkg/model"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateJobStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateJobStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateJobStatusLogic {
	return &UpdateJobStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateJobStatusLogic) UpdateJobStatus(in *job.UpdateJobStatusRequest) (*job.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || in.Status == nil || !validStatus(*in.Status) {
		return nil, invalidInput()
	}
	if err := l.svcCtx.Jobs.UpdateJobStatus(l.ctx, in.Id, basemodel.RecordStatus(*in.Status)); err != nil {
		return nil, serviceError(err)
	}
	return &job.EmptyResponse{}, nil
}
