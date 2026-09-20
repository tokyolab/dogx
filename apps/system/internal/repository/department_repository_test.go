package repository

import (
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
)

func TestValidateDepartmentTree(t *testing.T) {
	parentID := int64(1)
	childID := int64(2)
	missingID := int64(99)
	departments := []model.Department{
		{Base: model.Base{ID: 1}, Name: "总部"},
		{Base: model.Base{ID: 2}, ParentID: &parentID, Name: "研发部"},
	}

	tests := []struct {
		name      string
		id        int64
		candidate model.Department
		wantErr   error
	}{
		{name: "root department", candidate: model.Department{Name: "独立部门"}},
		{name: "existing parent", candidate: model.Department{ParentID: &parentID, Name: "平台组"}},
		{name: "missing parent", candidate: model.Department{ParentID: &missingID, Name: "孤儿部门"}, wantErr: ErrDepartmentParentInvalid},
		{name: "missing edited department", id: 99, candidate: model.Department{Name: "不存在"}, wantErr: ErrDepartmentNotFound},
		{name: "self cycle", id: 2, candidate: model.Department{ParentID: &childID, Name: "研发部"}, wantErr: ErrDepartmentCycle},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateDepartmentTree(departments, test.id, &test.candidate)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("validate department tree: got %v, want %v", err, test.wantErr)
			}
		})
	}
}
