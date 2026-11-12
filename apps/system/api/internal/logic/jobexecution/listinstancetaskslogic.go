// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package jobexecution

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListInstanceTasksLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListInstanceTasksLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListInstanceTasksLogic {
	return &ListInstanceTasksLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListInstanceTasksLogic) ListInstanceTasks(req *types.JobTaskListReq) (resp *types.JobTaskListResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.ListInstanceTasks(l.ctx, &jobclient.ListInstanceTasksRequest{InstanceId: req.InstanceId, Page: req.Page, PageSize: req.PageSize})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidResponse()
	}
	items := make([]types.JobTaskListItem, 0, len(result.Items))
	for _, item := range result.Items {
		if item == nil {
			return nil, invalidResponse()
		}
		items = append(items, taskProjection(item))
	}
	return &types.JobTaskListResp{Items: items, PageMeta: types.PageMeta{Total: result.Total}}, nil
}
