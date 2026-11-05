// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package jobgroup

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListExecutorGroupsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListExecutorGroupsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListExecutorGroupsLogic {
	return &ListExecutorGroupsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListExecutorGroupsLogic) ListExecutorGroups(req *types.ListExecutorGroupsReq) (resp *types.ListExecutorGroupsResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.ListExecutorGroups(l.ctx, &jobclient.ListExecutorGroupsRequest{
		Page: req.Page, PageSize: req.PageSize, Name: req.Name,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidResponse()
	}
	items := make([]types.ExecutorGroupListItem, 0, len(result.Items))
	for _, item := range result.Items {
		if item == nil {
			return nil, invalidResponse()
		}
		items = append(items, types.ExecutorGroupListItem{
			Id: item.Id, Name: item.Name, DiscoveryKey: item.DiscoveryKey,
			CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
		})
	}
	return &types.ListExecutorGroupsResp{Items: items, PageMeta: types.PageMeta{Total: result.Total}}, nil
}
