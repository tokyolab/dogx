package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// taskDetailResponseLimit matches this management method's larger client receive
// limit. A bounded document plus an independent summary can still exceed it.
const taskDetailResponseLimit = 9 << 20

type GetTaskLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetTaskLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTaskLogic {
	return &GetTaskLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetTaskLogic) GetTask(in *job.GetTaskRequest) (*job.GetTaskResponse, error) {
	if in == nil || in.Id <= 0 || (in.DataKind != "" && in.DataKind != "input" && in.DataKind != "result") {
		return nil, invalidInput()
	}
	item, err := l.svcCtx.TaskRepo.FindDetail(l.ctx, in.Id, repository.TaskDataSelection(in.DataKind))
	switch {
	case errors.Is(err, repository.ErrTaskNotFound):
		return nil, bizerror.New(subcode.TaskNotFound, "任务执行记录不存在")
	case errors.Is(err, repository.ErrTaskDataTooLarge):
		return nil, bizerror.New(subcode.TaskDataTooLarge, "任务详情超过管理查看大小限制")
	case err != nil:
		return nil, rpcOperationError("get job task", err)
	case item == nil:
		return nil, status.Error(codes.Internal, "task repository returned a nil record without an error")
	}
	response := &job.GetTaskResponse{Task: taskListItem(item.TaskSummary), Data: item.Data}
	if proto.Size(response) > taskDetailResponseLimit {
		return nil, bizerror.New(subcode.TaskDataTooLarge, "任务详情超过管理查看大小限制")
	}
	return response, nil
}
