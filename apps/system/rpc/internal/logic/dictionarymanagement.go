package logic

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var dictionaryCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

func validDictionaryText(value string, max int) bool {
	return strings.TrimSpace(value) != "" && utf8.RuneCountInString(value) <= max
}
func validDictionaryFields(name, remark string) bool {
	return validDictionaryText(name, 128) && utf8.RuneCountInString(remark) <= 500
}
func invalidDictionaryRequest() error {
	return status.Error(codes.InvalidArgument, "invalid dictionary request")
}
func dictionaryError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, repository.ErrDictionaryNotFound):
		return bizerror.New(subcode.DictionaryNotFound, "字典不存在")
	case errors.Is(err, repository.ErrDictionaryCodeExists):
		return bizerror.New(subcode.DictionaryCodeExists, "字典编码已存在")
	case errors.Is(err, repository.ErrDictionaryHasItems):
		return bizerror.New(subcode.DictionaryHasItems, "字典存在字典项，不能删除")
	case errors.Is(err, repository.ErrDictionaryItemNotFound):
		return bizerror.New(subcode.DictionaryItemNotFound, "字典项不存在")
	case errors.Is(err, repository.ErrDictionaryValueExists):
		return bizerror.New(subcode.DictionaryValueExists, "该字典值已存在")
	default:
		return fmt.Errorf("manage dictionary: %w", err)
	}
}
func invalidateDictionary(ctx context.Context, s *svc.ServiceContext, code string) error {
	// Database writes are already committed. Never imply rollback or retry the business mutation here.
	if err := s.DictionaryCache.Invalidate(ctx, code); err != nil {
		logx.WithContext(ctx).Errorf("dictionary %s saved but cache invalidation failed: %v", code, err)
		return bizerror.New(subcode.DictionaryCacheInvalidationFailed, "数据已保存，但缓存清理失败，请使用刷新缓存重试")
	}
	return nil
}
func toDictionaryInfo(v model.Dictionary) *system.DictionaryInfo {
	return &system.DictionaryInfo{Id: v.ID, Name: v.Name, Code: v.Code, Remark: v.Remark, Status: int32(v.Status), IsPublic: v.IsPublic, CreatedAt: v.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: v.UpdatedAt.UTC().Format(time.RFC3339Nano)}
}
func toDictionaryItemInfo(v model.DictionaryItem) *system.DictionaryItemInfo {
	return &system.DictionaryItemInfo{Id: v.ID, DictionaryId: v.DictionaryID, Label: v.Label, Value: v.Value, Sort: v.Sort, Remark: v.Remark, Status: int32(v.Status), CreatedAt: v.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: v.UpdatedAt.UTC().Format(time.RFC3339Nano)}
}
