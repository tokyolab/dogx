package logic

import (
	"context"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetExecutorGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetExecutorGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetExecutorGroupLogic {
	return &GetExecutorGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetExecutorGroupLogic) GetExecutorGroup(in *job.GetExecutorGroupRequest) (*job.GetExecutorGroupResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidInput()
	}
	group, err := l.svcCtx.GroupRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, executorGroupError("get executor group", err)
	}
	if group == nil {
		return nil, status.Error(codes.Internal, "executor group repository returned a nil record without an error")
	}
	return &job.GetExecutorGroupResponse{Group: &job.ExecutorGroupInfo{
		Id: group.ID, Name: group.Name, DiscoveryKey: group.DiscoveryKey, Token: group.Token,
		CreatedAt: group.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: group.UpdatedAt.Format(time.RFC3339Nano),
	}}, nil
}
