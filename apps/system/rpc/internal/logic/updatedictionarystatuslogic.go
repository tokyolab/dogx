package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateDictionaryStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateDictionaryStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateDictionaryStatusLogic {
	return &UpdateDictionaryStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateDictionaryStatusLogic) UpdateDictionaryStatus(in *system.UpdateDictionaryStatusRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || !validRecordStatus(in.Status) {
		return nil, invalidDictionaryRequest()
	}
	current, err := l.svcCtx.DictionaryRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, dictionaryError(err)
	}
	if err := l.svcCtx.DictionaryRepo.UpdateStatus(l.ctx, in.Id, model.RecordStatus(in.Status)); err != nil {
		return nil, dictionaryError(err)
	}
	if err := invalidateDictionary(l.ctx, l.svcCtx, current.Code); err != nil {
		return nil, err
	}
	return &system.EmptyResponse{}, nil
}
