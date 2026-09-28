package dictionary

import (
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
)

func toDictionaryInfo(v *systemclient.DictionaryInfo) *types.DictionaryInfo {
	result := &types.DictionaryInfo{Id: v.Id, Name: v.Name, Code: v.Code, Remark: v.Remark, Status: v.Status, IsPublic: v.IsPublic, CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt}
	return result
}
func toDictionaryItemInfo(v *systemclient.DictionaryItemInfo) *types.DictionaryItemInfo {
	result := &types.DictionaryItemInfo{Id: v.Id, DictionaryId: v.DictionaryId, Label: v.Label, Value: v.Value, Sort: v.Sort, Remark: v.Remark, Status: v.Status, CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt}
	return result
}
func toDictionaryOption(v *systemclient.DictionaryOption) *types.DictionaryOption {
	result := &types.DictionaryOption{Label: v.Label, Value: v.Value, Sort: v.Sort, Status: v.Status}
	return result
}
func toDictionaryOptions(v *systemclient.DictionaryOptions) *types.DictionaryOptions {
	result := &types.DictionaryOptions{Code: v.Code, Status: v.Status}
	result.Items = make([]types.DictionaryOption, 0, len(v.Items))
	for _, item := range v.Items {
		if item != nil {
			result.Items = append(result.Items, *toDictionaryOption(item))
		}
	}
	return result
}
