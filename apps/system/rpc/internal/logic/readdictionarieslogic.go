package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/dictcache"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ReadDictionariesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewReadDictionariesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReadDictionariesLogic {
	return &ReadDictionariesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ReadDictionariesLogic) ReadDictionaries(in *system.ReadDictionariesRequest) (*system.ReadDictionariesResponse, error) {
	if in == nil || len(in.Codes) == 0 || len(in.Codes) > 50 {
		return nil, invalidDictionaryRequest()
	}
	codes := make([]string, 0, len(in.Codes))
	seen := make(map[string]bool, len(in.Codes))
	for _, code := range in.Codes {
		if !dictionaryCodePattern.MatchString(code) {
			return nil, invalidDictionaryRequest()
		}
		if !seen[code] {
			codes = append(codes, code)
			seen[code] = true
		}
	}
	entries, err := l.svcCtx.DictionaryCache.Read(l.ctx, codes)
	if err != nil {
		l.Errorf("read dictionary cache: %v", err)
		entries = nil
	}
	if entries == nil {
		entries = make(map[string]dictcache.Entry)
	}
	missing := make([]string, 0, len(codes))
	for _, code := range codes {
		if _, ok := entries[code]; !ok {
			missing = append(missing, code)
		}
	}
	if len(missing) > 0 {
		dictionaries, err := l.svcCtx.DictionaryRepo.FindByCodes(l.ctx, missing)
		if err != nil {
			return nil, dictionaryError(err)
		}
		ids := make([]int64, 0, len(dictionaries))
		byID := make(map[int64]string, len(dictionaries))
		for _, d := range dictionaries {
			ids = append(ids, d.ID)
			byID[d.ID] = d.Code
			entries[d.Code] = dictcache.Entry{Status: int32(d.Status), IsPublic: d.IsPublic, Items: []dictcache.Item{}}
		}
		items, err := l.svcCtx.DictionaryItemRepo.ListByDictionaryIDs(l.ctx, ids)
		if err != nil {
			return nil, dictionaryError(err)
		}
		for _, item := range items {
			code := byID[item.DictionaryID]
			entry := entries[code]
			entry.Items = append(entry.Items, dictcache.Item{Label: item.Label, Value: item.Value, Status: int32(item.Status), Sort: item.Sort})
			entries[code] = entry
		}
		for _, d := range dictionaries {
			if err := l.svcCtx.DictionaryCache.Put(l.ctx, d.Code, entries[d.Code]); err != nil {
				l.Errorf("fill dictionary cache %s: %v", d.Code, err)
			}
		}
	}
	result := &system.ReadDictionariesResponse{Items: make([]*system.DictionaryOptions, 0, len(codes))}
	for _, code := range codes {
		entry, ok := entries[code]
		// Missing and non-public codes are both omitted; mixed public requests cannot disclose internal data.
		if !ok || (in.PublicOnly && !entry.IsPublic) {
			continue
		}
		options := &system.DictionaryOptions{Code: code, Status: entry.Status, Items: make([]*system.DictionaryOption, 0, len(entry.Items))}
		for _, item := range entry.Items {
			options.Items = append(options.Items, &system.DictionaryOption{Label: item.Label, Value: item.Value, Sort: item.Sort, Status: item.Status})
		}
		result.Items = append(result.Items, options)
	}
	return result, nil
}
