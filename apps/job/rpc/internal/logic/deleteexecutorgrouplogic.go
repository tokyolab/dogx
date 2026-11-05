package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteExecutorGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteExecutorGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteExecutorGroupLogic {
	return &DeleteExecutorGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteExecutorGroupLogic) DeleteExecutorGroup(in *job.DeleteExecutorGroupRequest) (*job.EmptyResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidInput()
	}
	err := l.svcCtx.Transactions(l.ctx, func(r repository.Repositories) error {
		// Job creation and group changes take this same group lock first. The
		// following separate statements see their commits under READ COMMITTED.
		if err := r.Groups.LockAvailable(l.ctx, in.Id); err != nil {
			return err
		}
		referenced, err := r.Jobs.HasGroupReferences(l.ctx, in.Id)
		if err != nil {
			return err
		}
		if referenced {
			return bizerror.New(subcode.ExecutorGroupInUse, "执行器组仍被引用，不能删除")
		}
		// An old round retains its original group after its Job changes groups.
		// STOPPING remains active until every child has reached a terminal state.
		active, err := r.Instances.HasActiveForGroup(l.ctx, in.Id)
		if err != nil {
			return err
		}
		if active {
			return bizerror.New(subcode.ExecutorGroupInUse, "执行器组仍被引用，不能删除")
		}
		return r.Groups.Delete(l.ctx, in.Id)
	})
	if err != nil {
		return nil, executorGroupError("delete executor group", err)
	}
	return &job.EmptyResponse{}, nil
}
