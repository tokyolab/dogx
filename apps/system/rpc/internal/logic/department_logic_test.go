package logic

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDepartmentMutationLogicNormalizesAndDelegates(t *testing.T) {
	stub := &departmentRepositoryStub{}
	ctx := context.Background()
	svcCtx := &svc.ServiceContext{DepartmentRepo: stub}
	created, err := NewCreateDepartmentLogic(ctx, svcCtx).CreateDepartment(&system.CreateDepartmentRequest{
		ParentId: 7, Name: "  研发部 ", Remark: "  负责研发 ", Sort: 3, Status: 1,
	})
	if err != nil || created.GetId() != 42 || stub.created == nil || stub.created.ParentID == nil ||
		*stub.created.ParentID != 7 || stub.created.Name != "研发部" || stub.created.Remark != "负责研发" ||
		stub.created.Sort != 3 || stub.created.Status != model.RecordStatusEnabled {
		t.Fatalf("create result=%+v error=%v department=%+v", created, err, stub.created)
	}
	if _, err := NewUpdateDepartmentLogic(ctx, svcCtx).UpdateDepartment(&system.UpdateDepartmentRequest{
		Id: 42, ParentId: 0, Name: "  平台组 ", Remark: "  ", Sort: 4,
	}); err != nil || stub.updateID != 42 || stub.updated.ParentID != nil || stub.updated.Name != "平台组" || stub.updated.Remark != "" {
		t.Fatalf("update error=%v id=%d department=%+v", err, stub.updateID, stub.updated)
	}
	if _, err := NewUpdateDepartmentStatusLogic(ctx, svcCtx).UpdateDepartmentStatus(&system.UpdateDepartmentStatusRequest{Id: 42, Status: 0}); err != nil || stub.statusID != 42 || stub.status != model.RecordStatusDisabled {
		t.Fatalf("status error=%v id=%d status=%d", err, stub.statusID, stub.status)
	}
	if _, err := NewDeleteDepartmentLogic(ctx, svcCtx).DeleteDepartment(&system.DeleteDepartmentRequest{Id: 42}); err != nil || stub.deleteID != 42 {
		t.Fatalf("delete error=%v id=%d", err, stub.deleteID)
	}
}

func TestDepartmentMutationLogicRejectsInvalidRequests(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(*svc.ServiceContext) error
	}{
		{"create nil", func(s *svc.ServiceContext) error {
			_, e := NewCreateDepartmentLogic(ctx, s).CreateDepartment(nil)
			return e
		}},
		{"create invalid parent", func(s *svc.ServiceContext) error {
			_, e := NewCreateDepartmentLogic(ctx, s).CreateDepartment(&system.CreateDepartmentRequest{ParentId: -1, Name: "部门", Status: 1})
			return e
		}},
		{"create invalid sort", func(s *svc.ServiceContext) error {
			_, e := NewCreateDepartmentLogic(ctx, s).CreateDepartment(&system.CreateDepartmentRequest{Name: "部门", Sort: -1, Status: 1})
			return e
		}},
		{"create invalid status", func(s *svc.ServiceContext) error {
			_, e := NewCreateDepartmentLogic(ctx, s).CreateDepartment(&system.CreateDepartmentRequest{Name: "部门", Status: 2})
			return e
		}},
		{"create blank name", func(s *svc.ServiceContext) error {
			_, e := NewCreateDepartmentLogic(ctx, s).CreateDepartment(&system.CreateDepartmentRequest{Name: " \t", Status: 1})
			return e
		}},
		{"create long name", func(s *svc.ServiceContext) error {
			_, e := NewCreateDepartmentLogic(ctx, s).CreateDepartment(&system.CreateDepartmentRequest{Name: strings.Repeat("部", 129), Status: 1})
			return e
		}},
		{"create long remark", func(s *svc.ServiceContext) error {
			_, e := NewCreateDepartmentLogic(ctx, s).CreateDepartment(&system.CreateDepartmentRequest{Name: "部门", Remark: strings.Repeat("记", 501), Status: 1})
			return e
		}},
		{"update nil", func(s *svc.ServiceContext) error {
			_, e := NewUpdateDepartmentLogic(ctx, s).UpdateDepartment(nil)
			return e
		}},
		{"update invalid id", func(s *svc.ServiceContext) error {
			_, e := NewUpdateDepartmentLogic(ctx, s).UpdateDepartment(&system.UpdateDepartmentRequest{Name: "部门"})
			return e
		}},
		{"update invalid status field absent", func(s *svc.ServiceContext) error {
			_, e := NewUpdateDepartmentLogic(ctx, s).UpdateDepartment(&system.UpdateDepartmentRequest{Id: 1, Name: "部门", ParentId: -1})
			return e
		}},
		{"status nil", func(s *svc.ServiceContext) error {
			_, e := NewUpdateDepartmentStatusLogic(ctx, s).UpdateDepartmentStatus(nil)
			return e
		}},
		{"status invalid id", func(s *svc.ServiceContext) error {
			_, e := NewUpdateDepartmentStatusLogic(ctx, s).UpdateDepartmentStatus(&system.UpdateDepartmentStatusRequest{Status: 1})
			return e
		}},
		{"status invalid value", func(s *svc.ServiceContext) error {
			_, e := NewUpdateDepartmentStatusLogic(ctx, s).UpdateDepartmentStatus(&system.UpdateDepartmentStatusRequest{Id: 1, Status: math.MaxInt32})
			return e
		}},
		{"delete nil", func(s *svc.ServiceContext) error {
			_, e := NewDeleteDepartmentLogic(ctx, s).DeleteDepartment(nil)
			return e
		}},
		{"delete invalid id", func(s *svc.ServiceContext) error {
			_, e := NewDeleteDepartmentLogic(ctx, s).DeleteDepartment(&system.DeleteDepartmentRequest{})
			return e
		}},
		{"get nil", func(s *svc.ServiceContext) error { _, e := NewGetDepartmentLogic(ctx, s).GetDepartment(nil); return e }},
		{"get invalid id", func(s *svc.ServiceContext) error {
			_, e := NewGetDepartmentLogic(ctx, s).GetDepartment(&system.GetDepartmentRequest{})
			return e
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if code := status.Code(test.call(&svc.ServiceContext{DepartmentRepo: &departmentRepositoryStub{}})); code != codes.InvalidArgument {
				t.Fatalf("error code=%v, want %v", code, codes.InvalidArgument)
			}
		})
	}
}

func TestDepartmentQueryLogicMapsDataAndErrors(t *testing.T) {
	created := time.Date(2026, 9, 20, 1, 2, 3, 0, time.FixedZone("CST", 8*60*60))
	parent := int64(3)
	department := model.Department{Base: model.Base{ID: 8, CreatedAt: created, UpdatedAt: created.Add(time.Hour)}, ParentID: &parent, Name: "研发部", Sort: 2, Status: model.RecordStatusDisabled, Remark: "备注"}
	stub := &departmentRepositoryStub{departments: []model.Department{department}, department: &department}
	svcCtx := &svc.ServiceContext{DepartmentRepo: stub}
	item, err := NewGetDepartmentLogic(context.Background(), svcCtx).GetDepartment(&system.GetDepartmentRequest{Id: 8})
	if err != nil || item.GetDepartment().GetParentId() != 3 || item.GetDepartment().GetStatus() != 0 || item.GetDepartment().GetCreatedAt() != created.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("get item=%+v error=%v", item, err)
	}
	list, err := NewListDepartmentsLogic(context.Background(), svcCtx).ListDepartments(&system.ListDepartmentsRequest{})
	if err != nil || len(list.GetItems()) != 1 || list.GetItems()[0].GetName() != "研发部" {
		t.Fatalf("list=%+v error=%v", list, err)
	}
	for _, dependencyErr := range []error{repository.ErrDepartmentNotFound, repository.ErrDepartmentNameExists, repository.ErrDepartmentHasChildren, repository.ErrDepartmentHasUsers, repository.ErrDepartmentParentInvalid, repository.ErrDepartmentCycle, errors.New("database unavailable")} {
		if _, err := NewGetDepartmentLogic(context.Background(), &svc.ServiceContext{DepartmentRepo: &departmentRepositoryStub{findErr: dependencyErr}}).GetDepartment(&system.GetDepartmentRequest{Id: 1}); err == nil {
			t.Fatalf("dependency error %v was accepted", dependencyErr)
		}
	}
	if got := departmentError(repository.ErrDepartmentCycle); !hasDepartmentSubcode(got, subcode.DepartmentCycle) {
		t.Fatalf("department cycle error=%v", got)
	}
}

func hasDepartmentSubcode(err error, want string) bool {
	businessErr, ok := bizerror.From(err)
	return ok && businessErr.Subcode() == want
}
