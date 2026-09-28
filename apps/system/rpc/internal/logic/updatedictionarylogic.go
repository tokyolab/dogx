package logic

import (
	"context"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateDictionaryLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateDictionaryLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateDictionaryLogic {
	return &UpdateDictionaryLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateDictionaryLogic) UpdateDictionary(in *system.UpdateDictionaryRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || !validDictionaryFields(in.Name, in.Remark) {
		return nil, invalidDictionaryRequest()
	}
	current, err := l.svcCtx.DictionaryRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, dictionaryError(err)
	}
	if err := l.svcCtx.DictionaryRepo.Update(l.ctx, in.Id, &model.Dictionary{Name: strings.TrimSpace(in.Name), Remark: strings.TrimSpace(in.Remark), IsPublic: in.IsPublic}); err != nil {
		return nil, dictionaryError(err)
	}
	if err := invalidateDictionary(l.ctx, l.svcCtx, current.Code); err != nil {
		return nil, err
	}
	return &system.EmptyResponse{}, nil
}
