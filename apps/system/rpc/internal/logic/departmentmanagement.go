package logic

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validDepartmentFields(name, remark string) bool {
	name = strings.TrimSpace(name)
	return name != "" && utf8.RuneCountInString(name) <= 128 && utf8.RuneCountInString(strings.TrimSpace(remark)) <= 500
}

func invalidDepartmentRequest() error {
	return status.Error(codes.InvalidArgument, "invalid department request")
}

func departmentError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, repository.ErrDepartmentNotFound):
		return bizerror.New(subcode.DepartmentNotFound, "部门不存在")
	case errors.Is(err, repository.ErrDepartmentNameExists):
		return bizerror.New(subcode.DepartmentNameExists, "同级部门名称已存在")
	case errors.Is(err, repository.ErrDepartmentHasChildren):
		return bizerror.New(subcode.DepartmentHasChildren, "部门存在子部门，不能删除")
	case errors.Is(err, repository.ErrDepartmentHasUsers):
		return bizerror.New(subcode.DepartmentHasUsers, "部门存在关联用户，不能删除")
	case errors.Is(err, repository.ErrDepartmentParentInvalid):
		return bizerror.New(subcode.DepartmentParentInvalid, "父部门不存在或无效")
	case errors.Is(err, repository.ErrDepartmentCycle):
		return bizerror.New(subcode.DepartmentCycle, "部门不能移动到自己的下级")
	default:
		return fmt.Errorf("manage department: %w", err)
	}
}
