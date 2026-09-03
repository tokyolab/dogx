package logic

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	systemsubcode "github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateRoleNormalizesAndPersistsRole(t *testing.T) {
	repositoryStub := &roleRepositoryStub{}
	response, err := NewCreateRoleLogic(context.Background(), &svc.ServiceContext{
		RoleRepo: repositoryStub,
	}).CreateRole(&system.CreateRoleRequest{
		Code: " Content_Editor ", Name: " 内容编辑员 ", Description: " 维护内容 ",
		Sort: 10, Status: int32(model.RecordStatusEnabled),
	})
	if err != nil {
		t.Fatalf("create role: %v", err)
	}
	if response.Id != 41 || repositoryStub.created == nil ||
		repositoryStub.created.Code != "content_editor" || repositoryStub.created.Name != "内容编辑员" ||
		repositoryStub.created.Description != "维护内容" || repositoryStub.created.Sort != 10 ||
		repositoryStub.created.Status != model.RecordStatusEnabled || repositoryStub.created.IsSystem {
		t.Fatalf("unexpected created role: response=%+v role=%+v", response, repositoryStub.created)
	}
}

func TestCreateRoleRejectsInvalidInputAndMapsDuplicateCode(t *testing.T) {
	requests := []*system.CreateRoleRequest{
		nil,
		{Code: "", Name: "Role", Status: 1},
		{Code: "not-valid-code", Name: "Role", Status: 1},
		{Code: "role", Name: "", Status: 1},
		{Code: "role", Name: strings.Repeat("角", maxRoleNameRunes+1), Status: 1},
		{Code: "role", Name: "Role", Description: strings.Repeat("描", maxRoleDescriptionRunes+1), Status: 1},
		{Code: "role", Name: "Role", Sort: -1, Status: 1},
		{Code: "role", Name: "Role", Status: 2},
	}
	for _, request := range requests {
		if _, err := NewCreateRoleLogic(context.Background(), &svc.ServiceContext{}).
			CreateRole(request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("request %+v error = %v, want invalid argument", request, err)
		}
	}
	if _, err := NewCreateRoleLogic(context.Background(), &svc.ServiceContext{}).
		CreateRole(&system.CreateRoleRequest{Code: "role", Name: "Role", Status: 1}); err == nil {
		t.Fatal("missing role repository was accepted")
	}
	if _, err := NewCreateRoleLogic(context.Background(), &svc.ServiceContext{
		RoleRepo: &roleRepositoryStub{createErr: repository.ErrRoleCodeExists},
	}).CreateRole(&system.CreateRoleRequest{Code: "role", Name: "Role", Status: 1}); err == nil {
		t.Fatal("duplicate role code was accepted")
	}
	if _, err := NewCreateRoleLogic(context.Background(), &svc.ServiceContext{
		RoleRepo: &roleRepositoryStub{createErr: repository.ErrReservedRoleCode},
	}).CreateRole(&system.CreateRoleRequest{Code: "super_admin", Name: "Super Admin", Status: 1}); !hasBusinessSubcode(err, systemsubcode.RoleCodeReserved) {
		t.Fatalf("reserved role code error = %v", err)
	}
}

func hasBusinessSubcode(err error, want string) bool {
	businessErr, ok := bizerror.From(err)
	return ok && businessErr.Subcode() == want
}

func TestUpdateRoleDelegatesNormalizedMetadataAndMapsErrors(t *testing.T) {
	repositoryStub := &roleRepositoryStub{}
	response, err := NewUpdateRoleLogic(context.Background(), &svc.ServiceContext{
		RoleRepo: repositoryStub,
	}).UpdateRole(&system.UpdateRoleRequest{
		Id: 7, Code: " Auditor ", Name: " 审计员 ", Description: " 查看审计记录 ", Sort: 5,
	})
	if err != nil {
		t.Fatalf("update role: %v", err)
	}
	if response == nil || repositoryStub.updateID != 7 || repositoryStub.update.Code != "auditor" ||
		repositoryStub.update.Name != "审计员" || repositoryStub.update.Description != "查看审计记录" ||
		repositoryStub.update.Sort != 5 {
		t.Fatalf("unexpected role update: response=%+v id=%d update=%+v", response, repositoryStub.updateID, repositoryStub.update)
	}
	if _, err := NewUpdateRoleLogic(context.Background(), &svc.ServiceContext{}).
		UpdateRole(&system.UpdateRoleRequest{Id: 7, Code: "role", Name: "Role"}); err == nil {
		t.Fatal("missing role repository was accepted")
	}

	for _, dependencyErr := range []error{
		repository.ErrRoleNotFound,
		repository.ErrRoleCodeExists,
		repository.ErrReservedRoleCode,
		repository.ErrSystemRoleProtected,
		errors.New("postgres unavailable"),
	} {
		if _, err := NewUpdateRoleLogic(context.Background(), &svc.ServiceContext{
			RoleRepo: &roleRepositoryStub{updateErr: dependencyErr},
		}).UpdateRole(&system.UpdateRoleRequest{Id: 7, Code: "role", Name: "Role"}); err == nil {
			t.Fatalf("dependency error %v was accepted", dependencyErr)
		}
	}
}

func TestRoleStatusAndDeleteDelegateLifecycleOperations(t *testing.T) {
	policies := &rolePolicyWriterStub{deleteResult: authorization.DeleteRoleResult{
		RemovedPolicies:   2,
		NotificationError: errors.New("watcher unavailable"),
	}}
	sessions := &sessionStoreLogicStub{}
	serviceContext := &svc.ServiceContext{RolePolicies: policies, Sessions: sessions}

	if _, err := NewUpdateRoleStatusLogic(context.Background(), serviceContext).
		UpdateRoleStatus(&system.UpdateRoleStatusRequest{Id: 8, Status: 0}); err != nil {
		t.Fatalf("disable role: %v", err)
	}
	if policies.roleID != 8 || policies.status != 0 {
		t.Fatalf("unexpected status delegation: roleId=%d status=%d", policies.roleID, policies.status)
	}
	if _, err := NewDeleteRoleLogic(context.Background(), serviceContext).
		DeleteRole(&system.DeleteRoleRequest{Id: 9}); err != nil {
		t.Fatalf("delete role: %v", err)
	}
	if !policies.deleted || policies.roleID != 9 {
		t.Fatalf("unexpected delete delegation: deleted=%v roleId=%d", policies.deleted, policies.roleID)
	}
}

func TestRoleLifecycleLogicRejectsInvalidRequestsAndMapsKnownErrors(t *testing.T) {
	if _, err := NewUpdateRoleStatusLogic(context.Background(), &svc.ServiceContext{}).
		UpdateRoleStatus(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil role status request error = %v, want invalid argument", err)
	}
	if _, err := NewUpdateRoleStatusLogic(context.Background(), &svc.ServiceContext{}).
		UpdateRoleStatus(&system.UpdateRoleStatusRequest{Id: 1, Status: 2}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid role status error = %v, want invalid argument", err)
	}
	if _, err := NewDeleteRoleLogic(context.Background(), &svc.ServiceContext{}).
		DeleteRole(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil role deletion error = %v, want invalid argument", err)
	}

	sessions := &sessionStoreLogicStub{}
	for _, dependencyErr := range []error{
		repository.ErrRoleNotFound,
		repository.ErrRoleInUse,
		repository.ErrSystemRoleProtected,
		authorization.ErrInvalidRoleID,
		errors.New("postgres unavailable"),
	} {
		policies := &rolePolicyWriterStub{err: dependencyErr}
		serviceContext := &svc.ServiceContext{RolePolicies: policies, Sessions: sessions}
		if _, err := NewUpdateRoleStatusLogic(context.Background(), serviceContext).
			UpdateRoleStatus(&system.UpdateRoleStatusRequest{Id: 1, Status: 0}); err == nil {
			t.Fatalf("status dependency error %v was accepted", dependencyErr)
		}
		if _, err := NewDeleteRoleLogic(context.Background(), serviceContext).
			DeleteRole(&system.DeleteRoleRequest{Id: 1}); err == nil {
			t.Fatalf("delete dependency error %v was accepted", dependencyErr)
		}
	}
}
