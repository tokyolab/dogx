package role

import (
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
)

func toRoleItem(role *systemclient.RoleInfo) *types.RoleItem {
	if role == nil {
		return nil
	}
	return &types.RoleItem{
		Id:          role.Id,
		Code:        role.Code,
		Name:        role.Name,
		Description: role.Description,
		Sort:        role.Sort,
		Status:      role.Status,
		IsSystem:    role.IsSystem,
		CreatedAt:   role.CreatedAt,
		UpdatedAt:   role.UpdatedAt,
	}
}
