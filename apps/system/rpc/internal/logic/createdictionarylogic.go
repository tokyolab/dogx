package logic

import (
	"context"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateDictionaryLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateDictionaryLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateDictionaryLogic {
	return &CreateDictionaryLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateDictionaryLogic) CreateDictionary(in *system.CreateDictionaryRequest) (*system.CreateDictionaryResponse, error) {
	if in == nil || !validDictionaryFields(in.Name, in.Remark) || !dictionaryCodePattern.MatchString(in.Code) || !validRecordStatus(in.Status) {
		return nil, invalidDictionaryRequest()
	}
	item := &model.Dictionary{Name: strings.TrimSpace(in.Name), Code: in.Code, Remark: strings.TrimSpace(in.Remark), Status: model.RecordStatus(in.Status), IsPublic: in.IsPublic}
	if err := l.svcCtx.DictionaryRepo.Create(l.ctx, item); err != nil {
		return nil, dictionaryError(err)
	}
	if err := invalidateDictionary(l.ctx, l.svcCtx, item.Code); err != nil {
		return nil, err
	}
	return &system.CreateDictionaryResponse{Id: item.ID}, nil
}
