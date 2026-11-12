// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package jobexecution

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc"
)

type GetTaskLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetTaskLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetTaskLogic {
	return &GetTaskLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetTaskLogic) GetTask(req *types.JobTaskDetailReq) (resp *types.JobTaskDetailResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	// The repository bounds a single JSON text document at 8 MiB. Allow room
	// for metadata without raising the receive limit of other management RPCs.
	result, err := l.svcCtx.JobRpc.GetTask(l.ctx, &jobclient.GetTaskRequest{Id: req.Id, DataKind: req.DataKind},
		grpc.MaxCallRecvMsgSize(9<<20))
	if err != nil {
		return nil, err
	}
	if result == nil || result.Task == nil {
		return nil, invalidResponse()
	}
	return &types.JobTaskDetailResp{Task: taskProjection(result.Task), Data: result.Data}, nil
}
