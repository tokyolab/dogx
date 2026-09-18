package logic

import (
	"errors"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
	systemsubcode "github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/pkg/bizerror"
)

func roleMenuBusinessError(err error) error {
	switch {
	case errors.Is(err, repository.ErrRoleNotFound):
		return bizerror.New(systemsubcode.RoleNotFound, "角色不存在")
	case errors.Is(err, repository.ErrRoleMenuRoleDisabled):
		return bizerror.New(systemsubcode.RoleUnavailable, "角色不存在或已停用")
	case errors.Is(err, repository.ErrRoleMenuUnavailable):
		return bizerror.New(systemsubcode.RoleMenuUnavailable, "菜单或其上级已不存在，请重新加载")
	case errors.Is(err, repository.ErrSuperAdminMenusProtected):
		return bizerror.New(systemsubcode.RoleSuperAdminMenuProtected, "超级管理员拥有全部菜单权限，不能配置菜单权限")
	default:
		return err
	}
}
