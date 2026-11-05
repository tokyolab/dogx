package logic

import (
	"context"
	"math"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListExecutorGroupsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListExecutorGroupsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListExecutorGroupsLogic {
	return &ListExecutorGroupsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListExecutorGroupsLogic) ListExecutorGroups(in *job.ListExecutorGroupsRequest) (*job.ListExecutorGroupsResponse, error) {
	if in == nil || in.Page < 1 || in.PageSize < 1 || in.PageSize > 200 || in.Page-1 > math.MaxInt64/in.PageSize {
		return nil, invalidInput()
	}
	name, err := normalizeText(in.Name, false, 128)
	if err != nil {
		return nil, err
	}
	items, total, err := l.svcCtx.GroupRepo.List(l.ctx, repository.ExecutorGroupFilter{Page: in.Page, PageSize: in.PageSize, Name: name})
	if err != nil {
		return nil, rpcOperationError("list executor groups", err)
	}
	result := make([]*job.ExecutorGroupListItem, 0, len(items))
	for _, item := range items {
		result = append(result, &job.ExecutorGroupListItem{
			Id: item.ID, Name: item.Name, DiscoveryKey: item.DiscoveryKey,
			CreatedAt: item.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: item.UpdatedAt.Format(time.RFC3339Nano),
		})
	}
	return &job.ListExecutorGroupsResponse{Items: result, Total: total}, nil
}
