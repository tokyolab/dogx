package logic

import (
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
)

func toRoleInfo(role model.Role) *system.RoleInfo {
	return &system.RoleInfo{
		Id:          role.ID,
		Code:        role.Code,
		Name:        role.Name,
		Description: role.Description,
		Sort:        role.Sort,
		Status:      int32(role.Status),
		CreatedAt:   role.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:   role.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}
