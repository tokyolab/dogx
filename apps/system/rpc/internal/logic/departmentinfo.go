package logic

import (
	"strings"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
)

func toDepartmentInfo(department model.Department) *system.DepartmentInfo {
	parentID := int64(0)
	if department.ParentID != nil {
		parentID = *department.ParentID
	}
	return &system.DepartmentInfo{
		Id: department.ID, ParentId: parentID, Name: department.Name,
		Sort: department.Sort, Status: int32(department.Status), Remark: department.Remark,
		CreatedAt: department.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: department.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func departmentModel(parentID int64, name, remark string, sort int32, status model.RecordStatus) *model.Department {
	var parent *int64
	if parentID > 0 {
		parent = &parentID
	}
	return &model.Department{ParentID: parent, Name: strings.TrimSpace(name), Sort: sort, Status: status, Remark: strings.TrimSpace(remark)}
}
