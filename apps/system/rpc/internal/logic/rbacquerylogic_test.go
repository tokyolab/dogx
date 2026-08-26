package logic

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type roleQueryRepositoryStub struct {
	listQuery repository.RoleListQuery
	roles     []model.Role
	total     int64
	listErr   error
	role      *model.Role
	findID    int64
	findErr   error
}

func (s *roleQueryRepositoryStub) ListEnabledRoleIDs(context.Context, int64) ([]int64, error) {
	return nil, nil
}

func (s *roleQueryRepositoryStub) List(
	_ context.Context,
	query repository.RoleListQuery,
) ([]model.Role, int64, error) {
	s.listQuery = query
	return s.roles, s.total, s.listErr
}

func (s *roleQueryRepositoryStub) FindByID(_ context.Context, id int64) (*model.Role, error) {
	s.findID = id
	return s.role, s.findErr
}

type apiQueryRepositoryStub struct {
	query     repository.APIListQuery
	resources []model.API
	err       error
}

func (s *apiQueryRepositoryStub) List(
	_ context.Context,
	query repository.APIListQuery,
) ([]model.API, error) {
	s.query = query
	return s.resources, s.err
}

func TestListRolesReturnsPageAndStableContract(t *testing.T) {
	createdAt := time.Date(2026, time.August, 26, 1, 2, 3, 4, time.UTC)
	repositoryStub := &roleQueryRepositoryStub{
		roles: []model.Role{{
			Base: model.Base{
				ID:        7,
				CreatedAt: createdAt,
				UpdatedAt: createdAt.Add(time.Minute),
			},
			Code:        "operator",
			Name:        "Operator",
			Description: "Operations role",
			Sort:        12,
			Status:      model.RecordStatusDisabled,
			IsSystem:    true,
		}},
		total: 3,
	}
	response, err := NewListRolesLogic(context.Background(), &svc.ServiceContext{
		RoleRepo: repositoryStub,
	}).ListRoles(&system.ListRolesRequest{
		Page:     2,
		PageSize: 20,
		Keyword:  " operator ",
	})
	if err != nil {
		t.Fatalf("list roles: %v", err)
	}
	if repositoryStub.listQuery.Keyword != "operator" ||
		repositoryStub.listQuery.Offset != 20 ||
		repositoryStub.listQuery.Limit != 20 {
		t.Fatalf("unexpected role list query: %+v", repositoryStub.listQuery)
	}
	if response.Total != 3 || len(response.Items) != 1 ||
		response.Items[0].Id != 7 ||
		response.Items[0].Status != int32(model.RecordStatusDisabled) ||
		!response.Items[0].IsSystem ||
		response.Items[0].CreatedAt != createdAt.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected role list response: %+v", response)
	}
}

func TestListRolesRejectsInvalidRequestsAndDependencyFailures(t *testing.T) {
	for _, request := range []*system.ListRolesRequest{
		nil,
		{},
		{Page: 1, PageSize: 101},
		{Page: 1, PageSize: 1, Keyword: strings.Repeat("x", maxRoleKeywordBytes+1)},
		{Page: int64(^uint64(0) >> 1), PageSize: 100},
	} {
		if _, err := NewListRolesLogic(context.Background(), &svc.ServiceContext{}).
			ListRoles(request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("request %+v error = %v, want invalid argument", request, err)
		}
	}
	if _, err := NewListRolesLogic(context.Background(), &svc.ServiceContext{}).
		ListRoles(&system.ListRolesRequest{Page: 1, PageSize: 20}); err == nil {
		t.Fatal("missing role repository was accepted")
	}
	databaseErr := errors.New("postgres unavailable")
	if _, err := NewListRolesLogic(context.Background(), &svc.ServiceContext{
		RoleRepo: &roleQueryRepositoryStub{listErr: databaseErr},
	}).ListRoles(&system.ListRolesRequest{Page: 1, PageSize: 20}); !errors.Is(err, databaseErr) {
		t.Fatalf("role list dependency error = %v, want wrapped %v", err, databaseErr)
	}
}

func TestGetRoleReturnsRoleAndMapsMissingRole(t *testing.T) {
	repositoryStub := &roleQueryRepositoryStub{role: &model.Role{
		Base:   model.Base{ID: 9},
		Code:   "auditor",
		Name:   "Auditor",
		Status: model.RecordStatusEnabled,
	}}
	response, err := NewGetRoleLogic(context.Background(), &svc.ServiceContext{
		RoleRepo: repositoryStub,
	}).GetRole(&system.GetRoleRequest{Id: 9})
	if err != nil {
		t.Fatalf("get role: %v", err)
	}
	if repositoryStub.findID != 9 || response.Role == nil || response.Role.Code != "auditor" {
		t.Fatalf("unexpected role response: findId=%d response=%+v", repositoryStub.findID, response)
	}

	if _, err := NewGetRoleLogic(context.Background(), &svc.ServiceContext{
		RoleRepo: &roleQueryRepositoryStub{findErr: repository.ErrRoleNotFound},
	}).GetRole(&system.GetRoleRequest{Id: 404}); err == nil {
		t.Fatal("missing role was accepted")
	}
	if _, err := NewGetRoleLogic(context.Background(), &svc.ServiceContext{}).
		GetRole(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil role request error = %v, want invalid argument", err)
	}
}

func TestListAPIsReturnsFilteredResources(t *testing.T) {
	repositoryStub := &apiQueryRepositoryStub{resources: []model.API{{
		Base:        model.Base{ID: 11},
		ServiceName: "system-api",
		Group:       "角色管理",
		Name:        "查询角色",
		Path:        "/role/get",
		Method:      "POST",
		IsRequired:  true,
		Status:      model.RecordStatusEnabled,
		Remark:      "built in",
	}}}
	response, err := NewListAPIsLogic(context.Background(), &svc.ServiceContext{
		APIRepo: repositoryStub,
	}).ListAPIs(&system.ListAPIsRequest{
		Keyword:     " role ",
		ServiceName: " system-api ",
		ApiGroup:    " 角色管理 ",
	})
	if err != nil {
		t.Fatalf("list APIs: %v", err)
	}
	if repositoryStub.query.Keyword != "role" ||
		repositoryStub.query.ServiceName != "system-api" ||
		repositoryStub.query.Group != "角色管理" {
		t.Fatalf("unexpected API list query: %+v", repositoryStub.query)
	}
	if len(response.Items) != 1 || response.Items[0].Id != 11 ||
		response.Items[0].ApiGroup != "角色管理" || !response.Items[0].IsRequired {
		t.Fatalf("unexpected API list response: %+v", response)
	}
}

func TestListAPIsRejectsInvalidRequestsAndDependencyFailures(t *testing.T) {
	if _, err := NewListAPIsLogic(context.Background(), &svc.ServiceContext{}).
		ListAPIs(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil API list request error = %v, want invalid argument", err)
	}
	if _, err := NewListAPIsLogic(context.Background(), &svc.ServiceContext{}).
		ListAPIs(&system.ListAPIsRequest{}); err == nil {
		t.Fatal("missing API repository was accepted")
	}
	databaseErr := errors.New("postgres unavailable")
	if _, err := NewListAPIsLogic(context.Background(), &svc.ServiceContext{
		APIRepo: &apiQueryRepositoryStub{err: databaseErr},
	}).ListAPIs(&system.ListAPIsRequest{}); !errors.Is(err, databaseErr) {
		t.Fatalf("API list dependency error = %v, want wrapped %v", err, databaseErr)
	}
}

func TestGetRoleAPIsValidatesRoleAndReturnsPolicyResourceIDs(t *testing.T) {
	roleRepository := &roleQueryRepositoryStub{role: &model.Role{Base: model.Base{ID: 7}}}
	policies := &rolePolicyWriterStub{listed: []int64{11, 12}}
	response, err := NewGetRoleAPIsLogic(context.Background(), &svc.ServiceContext{
		RoleRepo:     roleRepository,
		RolePolicies: policies,
	}).GetRoleAPIs(&system.GetRoleAPIsRequest{RoleId: 7})
	if err != nil {
		t.Fatalf("get role APIs: %v", err)
	}
	if roleRepository.findID != 7 || policies.roleID != 7 ||
		len(response.ApiIds) != 2 || response.ApiIds[1] != 12 {
		t.Fatalf(
			"unexpected role API response: findId=%d policyRoleId=%d response=%+v",
			roleRepository.findID,
			policies.roleID,
			response,
		)
	}

	if _, err := NewGetRoleAPIsLogic(context.Background(), &svc.ServiceContext{
		RoleRepo:     &roleQueryRepositoryStub{findErr: repository.ErrRoleNotFound},
		RolePolicies: &rolePolicyWriterStub{},
	}).GetRoleAPIs(&system.GetRoleAPIsRequest{RoleId: 404}); err == nil {
		t.Fatal("missing role API query was accepted")
	}
	if _, err := NewGetRoleAPIsLogic(context.Background(), &svc.ServiceContext{}).
		GetRoleAPIs(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil role API request error = %v, want invalid argument", err)
	}
}

var _ repository.RoleRepository = (*roleQueryRepositoryStub)(nil)
var _ repository.APIRepository = (*apiQueryRepositoryStub)(nil)
