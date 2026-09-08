package logic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateUserHashesPasswordAndMapsProfile(t *testing.T) {
	repo := &userRepositoryStub{}
	passwords := &passwordVerifierStub{nextHash: "password-hash"}
	logic := NewCreateUserLogic(context.Background(), &svc.ServiceContext{UserRepo: repo, Passwords: passwords})
	result, err := logic.CreateUser(&system.CreateUserRequest{Username: " Alice ", Nickname: " 昵称 ", Password: "abcdefghijkl", Status: 1, RoleIds: []int64{8}, Remark: " note "})
	if err != nil {
		t.Fatal(err)
	}
	if result.Id != 51 || repo.created.Username != "Alice" || repo.created.Nickname != "昵称" || repo.created.Remark != "note" ||
		repo.created.PasswordHash != "password-hash" || repo.created.Email != nil || repo.created.Phone != nil || len(repo.roleIDs) != 1 {
		t.Fatalf("unexpected user creation: %+v", repo.created)
	}
}

func TestCreateUserRejectsInvalidInputWithoutWrites(t *testing.T) {
	for _, change := range []func(*system.CreateUserRequest){
		func(in *system.CreateUserRequest) { in.Username = " " },
		func(in *system.CreateUserRequest) { in.Username = strings.Repeat("用", 65) },
		func(in *system.CreateUserRequest) { in.Nickname = " " },
		func(in *system.CreateUserRequest) { in.Nickname = strings.Repeat("用", 65) },
		func(in *system.CreateUserRequest) { in.Password = "short" },
		func(in *system.CreateUserRequest) { in.Password = strings.Repeat("a", 73) },
		func(in *system.CreateUserRequest) { in.Password = strings.Repeat("密", 25) },
		func(in *system.CreateUserRequest) { in.Password = strings.Repeat("😀", 19) },
		func(in *system.CreateUserRequest) { in.Email = "invalid" },
		func(in *system.CreateUserRequest) { in.Email = "Name <test@example.com>" },
		func(in *system.CreateUserRequest) { in.Phone = strings.Repeat("1", 33) },
		func(in *system.CreateUserRequest) { in.Remark = strings.Repeat("用", 501) },
		func(in *system.CreateUserRequest) { in.Status = 65537 },
		func(in *system.CreateUserRequest) { in.RoleIds = []int64{0} },
		func(in *system.CreateUserRequest) { in.RoleIds = make([]int64, 101) },
	} {
		in := &system.CreateUserRequest{Username: "alice", Nickname: "Alice", Password: "abcdefghijkl", Status: 1}
		change(in)
		_, err := NewCreateUserLogic(context.Background(), &svc.ServiceContext{}).CreateUser(in)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid user input: %v", err)
		}
	}
}

func TestManagedUserPasswordByteBoundaries(t *testing.T) {
	for _, password := range []string{strings.Repeat("a", 72), strings.Repeat("密", 24), strings.Repeat("😀", 18)} {
		repo := &userRepositoryStub{user: &model.User{Base: model.Base{ID: 9}}}
		hasher := &passwordVerifierStub{nextHash: "new-hash"}
		sc := &svc.ServiceContext{UserRepo: repo, Passwords: hasher, Sessions: &sessionStoreLogicStub{}}
		if _, err := NewCreateUserLogic(context.Background(), sc).CreateUser(&system.CreateUserRequest{
			Username: "alice", Nickname: "Alice", Password: password, Status: 1,
		}); err != nil {
			t.Fatalf("create user with boundary password: %v", err)
		}
		if hasher.password != password {
			t.Fatal("create user changed the password before hashing")
		}
		if _, err := NewResetUserPasswordLogic(context.Background(), sc).ResetUserPassword(&system.ResetUserPasswordRequest{
			Id: 9, OperatorId: 1, Password: password,
		}); err != nil {
			t.Fatalf("reset user with boundary password: %v", err)
		}
		if hasher.password != password || repo.passwordHash != "new-hash" {
			t.Fatal("reset user did not persist the new password hash")
		}
	}
	for _, password := range []string{strings.Repeat("a", 73), strings.Repeat("密", 25), strings.Repeat("😀", 19)} {
		if _, err := NewResetUserPasswordLogic(context.Background(), &svc.ServiceContext{}).ResetUserPassword(
			&system.ResetUserPasswordRequest{Id: 9, OperatorId: 1, Password: password},
		); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected overlong reset to fail before accessing dependencies: %v", err)
		}
	}
}

func TestUserManagementMapsBusinessAndTechnicalErrors(t *testing.T) {
	for source, code := range map[error]string{
		repository.ErrUserNotFound: subcode.UserNotFound, repository.ErrUsernameExists: subcode.UserUsernameExists,
		repository.ErrUserEmailExists: subcode.UserEmailExists, repository.ErrUserPhoneExists: subcode.UserPhoneExists,
		repository.ErrUserRoleUnavailable: subcode.UserRoleUnavailable, repository.ErrSuperAdminNotAssignable: subcode.UserSuperAdminNotAssignable,
		repository.ErrSuperAdminProtected: subcode.UserSuperAdminProtected,
	} {
		if !hasBusinessSubcode(userManagementError(source), code) {
			t.Errorf("incorrect mapping for %v", source)
		}
	}
	if userManagementError(nil) != nil {
		t.Fatal("nil error was changed")
	}
	dependencyErr := errors.New("dependency unavailable")
	if !errors.Is(userManagementError(dependencyErr), dependencyErr) {
		t.Fatal("dependency error lost")
	}
	for _, hashFailure := range []bool{true, false} {
		repo := &userRepositoryStub{updateErr: dependencyErr}
		hasher := &passwordVerifierStub{nextHash: "hash"}
		if hashFailure {
			hasher.hashErr = dependencyErr
		}
		_, err := NewCreateUserLogic(context.Background(), &svc.ServiceContext{UserRepo: repo, Passwords: hasher}).CreateUser(
			&system.CreateUserRequest{Username: "alice", Nickname: "Alice", Password: "abcdefghijkl", Status: 1})
		if !errors.Is(err, dependencyErr) || (hashFailure && repo.writeCalls != 0) {
			t.Fatalf("create dependency failure: %v", err)
		}
	}
}

func TestUserQueriesAndTimeMapping(t *testing.T) {
	localTime := time.Date(2026, 9, 7, 16, 30, 0, 0, time.FixedZone("test", 8*3600))
	email, phone := "alice@example.com", "123"
	record := repository.UserRecord{User: model.User{Base: model.Base{ID: 9, CreatedAt: localTime, UpdatedAt: localTime}, Username: "alice", Nickname: "Alice", Email: &email, Phone: &phone, LastLoginAt: &localTime, PasswordHash: "not-returned"}, Roles: []model.Role{{Base: model.Base{ID: 8}, Code: "reader", Name: "Reader", Status: 1}}}
	repo := &userRepositoryStub{record: &record, records: []repository.UserRecord{record}, total: 7}
	sc := &svc.ServiceContext{UserRepo: repo}
	disabled := int32(0)
	list, err := NewListUsersLogic(context.Background(), sc).ListUsers(&system.ListUsersRequest{Page: 3, PageSize: 20, Status: &disabled})
	if err != nil || len(list.Items) != 1 || list.Total != 7 || repo.query.Offset != 40 || repo.query.Status == nil || *repo.query.Status != 0 {
		t.Fatalf("user list: %+v %v", list, err)
	}
	detail, err := NewGetUserLogic(context.Background(), sc).GetUser(&system.GetUserRequest{Id: 9})
	if err != nil || detail.User.Email != email || detail.User.Phone != phone || detail.User.CreatedAt != "2026-09-07T08:30:00Z" || detail.User.LastLoginAt != detail.User.CreatedAt || len(detail.User.Roles) != 1 {
		t.Fatalf("user detail: %+v %v", detail, err)
	}
	for _, in := range []*system.ListUsersRequest{nil, {}, {Page: 1, PageSize: 201}, {Page: math.MaxInt64, PageSize: 200}, {Page: 1, PageSize: 1, Keyword: strings.Repeat("x", 129)}} {
		if _, err := NewListUsersLogic(context.Background(), sc).ListUsers(in); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("bad page accepted: %v", err)
		}
	}
	for _, raw := range []int32{-1, 2, 65536, 65537, math.MaxInt32} {
		if _, err := NewListUsersLogic(context.Background(), sc).ListUsers(&system.ListUsersRequest{Page: 1, PageSize: 1, Status: &raw}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("bad status accepted: %d", raw)
		}
	}
	repo.findErr = repository.ErrUserNotFound
	if _, err := NewGetUserLogic(context.Background(), sc).GetUser(&system.GetUserRequest{Id: 9}); !hasBusinessSubcode(err, subcode.UserNotFound) {
		t.Fatal(err)
	}
	if _, err := NewListUsersLogic(context.Background(), sc).ListUsers(&system.ListUsersRequest{Page: 1, PageSize: 1}); err == nil {
		t.Fatal("list failure ignored")
	}
}

func TestUserRoleOptionsExcludeSuperAdminAtRepositoryBoundary(t *testing.T) {
	repo := &roleRepositoryStub{roles: []model.Role{{Base: model.Base{ID: 8}, Code: "reader", Name: "Reader", Status: 1}}, total: 1}
	result, err := NewListUserRoleOptionsLogic(context.Background(), &svc.ServiceContext{RoleRepo: repo}).ListUserRoleOptions(&system.ListRolesRequest{Page: 2, PageSize: 20, Keyword: "read"})
	if err != nil || len(result.Items) != 1 || !repo.listQuery.AssignableOnly || repo.listQuery.Offset != 20 {
		t.Fatalf("options: %+v %v", result, err)
	}
	repo.listErr = errors.New("db offline")
	if _, err := NewListUserRoleOptionsLogic(context.Background(), &svc.ServiceContext{RoleRepo: repo}).ListUserRoleOptions(&system.ListRolesRequest{Page: 1, PageSize: 20}); !errors.Is(err, repo.listErr) {
		t.Fatal(err)
	}
}

func TestUserProfileUpdateDoesNotChangeCredentialsOrRoles(t *testing.T) {
	repo := &userRepositoryStub{user: &model.User{Base: model.Base{ID: 9}}}
	sessions := &sessionStoreLogicStub{}
	sc := &svc.ServiceContext{UserRepo: repo, Sessions: sessions}
	_, err := NewUpdateUserLogic(context.Background(), sc).UpdateUser(&system.UpdateUserRequest{Id: 9, OperatorId: 1, Nickname: " 新昵称 "})
	if err != nil || repo.profile.Nickname != "新昵称" || repo.profile.Email != nil || repo.profile.Phone != nil || repo.profile.Remark != "" || sessions.revokedUserID != 0 || repo.passwordHash != "" || len(repo.roleIDs) != 0 {
		t.Fatalf("profile update: %+v %v", repo, err)
	}
}

func TestUserMutationsRevokeSessionsBeforeWriting(t *testing.T) {
	operations := map[string]func(*svc.ServiceContext) error{
		"disable": func(sc *svc.ServiceContext) error {
			_, err := NewUpdateUserStatusLogic(context.Background(), sc).UpdateUserStatus(&system.UpdateUserStatusRequest{Id: 9, OperatorId: 1, Status: 0})
			return err
		},
		"delete": func(sc *svc.ServiceContext) error {
			_, err := NewDeleteUserLogic(context.Background(), sc).DeleteUser(&system.DeleteUserRequest{Id: 9, OperatorId: 1})
			return err
		},
		"roles": func(sc *svc.ServiceContext) error {
			_, err := NewReplaceUserRolesLogic(context.Background(), sc).ReplaceUserRoles(&system.ReplaceUserRolesRequest{Id: 9, OperatorId: 1, RoleIds: []int64{8}})
			return err
		},
		"password": func(sc *svc.ServiceContext) error {
			_, err := NewResetUserPasswordLogic(context.Background(), sc).ResetUserPassword(&system.ResetUserPasswordRequest{Id: 9, OperatorId: 1, Password: "abcdefghijkl"})
			return err
		},
	}
	for name, call := range operations {
		t.Run(name, func(t *testing.T) {
			for _, fail := range []bool{false, true} {
				repo := &userRepositoryStub{user: &model.User{Base: model.Base{ID: 9}, Status: 1}}
				sessions := &sessionStoreLogicStub{}
				if fail {
					sessions.revokeAllErr = errors.New("redis offline")
				}
				err := call(&svc.ServiceContext{UserRepo: repo, Sessions: sessions, Passwords: &passwordVerifierStub{nextHash: "new-hash"}})
				if sessions.revokedUserID != 9 {
					t.Fatal("sessions not revoked")
				}
				if fail {
					if !errors.Is(err, sessions.revokeAllErr) || repo.writeCalls != 0 || repo.passwordHash != "" {
						t.Fatalf("failed revocation allowed mutation: %v", err)
					}
				} else if err != nil || (repo.writeCalls != 1 && repo.passwordHash != "new-hash") {
					t.Fatalf("mutation failed: %v", err)
				}
			}
		})
	}
}

func TestUserProtectionRejectsWithoutSessionSideEffects(t *testing.T) {
	record := &repository.UserRecord{User: model.User{Base: model.Base{ID: 9}, Status: 1}, Roles: []model.Role{{Code: model.SuperAdminRoleCode, Status: 1}}}
	for _, test := range []struct {
		name     string
		operator int64
		want     string
	}{
		{"self", 9, subcode.UserSelfProtected},
		{"ordinary operator", 1, subcode.UserSuperAdminProtected},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &userRepositoryStub{record: record}
			sessions := &sessionStoreLogicStub{}
			sc := &svc.ServiceContext{UserRepo: repo, Sessions: sessions}
			_, err := NewDeleteUserLogic(context.Background(), sc).DeleteUser(&system.DeleteUserRequest{Id: 9, OperatorId: test.operator})
			if !hasBusinessSubcode(err, test.want) || sessions.revokedUserID != 0 || repo.writeCalls != 0 {
				t.Fatalf("protection: %v", err)
			}
		})
	}
	repo := &userRepositoryStub{user: &model.User{Base: model.Base{ID: 9}}, rolesErr: repository.ErrSuperAdminNotAssignable}
	sessions := &sessionStoreLogicStub{}
	_, err := NewReplaceUserRolesLogic(context.Background(), &svc.ServiceContext{UserRepo: repo, Sessions: sessions}).ReplaceUserRoles(&system.ReplaceUserRolesRequest{Id: 9, OperatorId: 1, RoleIds: []int64{1}})
	if !hasBusinessSubcode(err, subcode.UserSuperAdminNotAssignable) || sessions.revokedUserID != 0 || repo.writeCalls != 0 {
		t.Fatalf("invalid grant changed session: %v", err)
	}
}

func TestReplaceUserRolesRejectsSuperAdminWithoutSideEffects(t *testing.T) {
	for _, state := range []model.RecordStatus{model.RecordStatusEnabled, model.RecordStatusDisabled} {
		for _, operator := range []int64{1, 42} {
			for _, test := range []struct {
				name   string
				target []int64
			}{
				{"add", []int64{2, 3}},
				{"replace", []int64{3}},
				{"unchanged", []int64{2}},
				{"clear", nil},
				{"explicit super role", []int64{77, 2}},
			} {
				t.Run(fmt.Sprintf("%s/status=%d/operator=%d", test.name, state, operator), func(t *testing.T) {
					repo := &userRepositoryStub{record: &repository.UserRecord{
						User: model.User{Base: model.Base{ID: 42}, Status: 1},
						Roles: []model.Role{
							{Base: model.Base{ID: 77}, Code: model.SuperAdminRoleCode, Status: state},
							{Base: model.Base{ID: 2}, Code: "reader", Status: 1},
						},
					}}
					sessions := &sessionStoreLogicStub{}
					result, err := NewReplaceUserRolesLogic(context.Background(), &svc.ServiceContext{UserRepo: repo, Sessions: sessions}).
						ReplaceUserRoles(&system.ReplaceUserRolesRequest{Id: 42, OperatorId: operator, RoleIds: test.target})
					if result != nil || !hasBusinessSubcode(err, subcode.UserSuperAdminProtected) {
						t.Fatalf("super administrator role change was not rejected: %v", err)
					}
					if repo.validateRolesCalls != 0 || repo.writeCalls != 0 || sessions.revokedUserID != 0 {
						t.Fatalf("rejected role change had side effects: validation=%d writes=%d revoked=%d", repo.validateRolesCalls, repo.writeCalls, sessions.revokedUserID)
					}
				})
			}
		}
	}
}

func TestReplaceUserRolesNoopDoesNotRevokeSessionsOrWrite(t *testing.T) {
	reader := model.Role{Base: model.Base{ID: 2}, Code: "reader", Status: 1}
	reviewer := model.Role{Base: model.Base{ID: 3}, Code: "reviewer", Status: 1}
	disabled := model.Role{Base: model.Base{ID: 4}, Code: "disabled", Status: 0}
	for _, test := range []struct {
		name    string
		current []model.Role
		target  []int64
	}{
		{"same roles", []model.Role{reader, reviewer}, []int64{2, 3}},
		{"order and duplicates", []model.Role{reviewer, reader}, []int64{3, 2, 2}},
		{"empty roles", nil, nil},
		{"explicit empty roles", nil, []int64{}},
		{"retained disabled role", []model.Role{reader, disabled}, []int64{4, 2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &userRepositoryStub{
				record:    &repository.UserRecord{User: model.User{Base: model.Base{ID: 42}, Status: 1}, Roles: test.current},
				updateErr: errors.New("unexpected role write"),
			}
			sessions := &sessionStoreLogicStub{revokeAllErr: errors.New("unexpected session revocation")}
			request := &system.ReplaceUserRolesRequest{Id: 42, OperatorId: 42, RoleIds: slices.Clone(test.target)}
			result, err := NewReplaceUserRolesLogic(context.Background(), &svc.ServiceContext{UserRepo: repo, Sessions: sessions}).
				ReplaceUserRoles(request)
			if err != nil || result == nil {
				t.Fatalf("unchanged roles failed: %v", err)
			}
			if repo.validateRolesCalls != 1 || repo.writeCalls != 0 || sessions.revokedUserID != 0 {
				t.Fatalf("noop side effects: validation=%d writes=%d revoked=%d", repo.validateRolesCalls, repo.writeCalls, sessions.revokedUserID)
			}
			if !slices.Equal(request.RoleIds, test.target) {
				t.Fatalf("comparison mutated request: %v", request.RoleIds)
			}
		})
	}
}

func TestReplaceUserRolesStillValidatesBeforeNoop(t *testing.T) {
	reader := model.Role{Base: model.Base{ID: 2}, Code: "reader", Status: 1}
	for _, test := range []struct {
		name          string
		current       []model.Role
		target        []int64
		validationErr error
		want          string
	}{
		{"unchanged roles became unavailable", []model.Role{reader}, []int64{2}, repository.ErrUserRoleUnavailable, subcode.UserRoleUnavailable},
		{"explicit super grant", []model.Role{reader}, []int64{2, 77}, repository.ErrSuperAdminNotAssignable, subcode.UserSuperAdminNotAssignable},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &userRepositoryStub{
				record:   &repository.UserRecord{User: model.User{Base: model.Base{ID: 42}, Status: 1}, Roles: test.current},
				rolesErr: test.validationErr,
			}
			sessions := &sessionStoreLogicStub{}
			_, err := NewReplaceUserRolesLogic(context.Background(), &svc.ServiceContext{UserRepo: repo, Sessions: sessions}).
				ReplaceUserRoles(&system.ReplaceUserRolesRequest{Id: 42, OperatorId: 42, RoleIds: test.target})
			if !hasBusinessSubcode(err, test.want) || repo.validateRolesCalls != 1 ||
				repo.writeCalls != 0 || sessions.revokedUserID != 0 {
				t.Fatalf("validation bypassed or caused side effects: %v", err)
			}
		})
	}
}

func TestReplaceUserRolesChangedSetsStillRevokeAndPersist(t *testing.T) {
	reader := model.Role{Base: model.Base{ID: 2}, Code: "reader", Status: 1}
	reviewer := model.Role{Base: model.Base{ID: 3}, Code: "reviewer", Status: 1}
	for _, test := range []struct {
		name    string
		current []model.Role
		target  []int64
	}{
		{"add", []model.Role{reader}, []int64{2, 3}},
		{"remove", []model.Role{reader, reviewer}, []int64{2}},
		{"replace", []model.Role{reader, reviewer}, []int64{2, 4}},
		{"clear", []model.Role{reader}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &userRepositoryStub{record: &repository.UserRecord{
				User: model.User{Base: model.Base{ID: 42}, Status: 1}, Roles: test.current,
			}}
			sessions := &sessionStoreLogicStub{}
			_, err := NewReplaceUserRolesLogic(context.Background(), &svc.ServiceContext{UserRepo: repo, Sessions: sessions}).
				ReplaceUserRoles(&system.ReplaceUserRolesRequest{Id: 42, OperatorId: 42, RoleIds: test.target})
			if err != nil || repo.validateRolesCalls != 1 || repo.writeCalls != 1 ||
				sessions.revokedUserID != 42 || !slices.Equal(repo.roleIDs, test.target) {
				t.Fatalf("changed roles were not persisted with session revocation: %v", err)
			}
		})
	}
}

func TestUserEnableDoesNotRevokeSessionsAndStatusCannotTruncate(t *testing.T) {
	repo := &userRepositoryStub{user: &model.User{Base: model.Base{ID: 9}}}
	sessions := &sessionStoreLogicStub{}
	sc := &svc.ServiceContext{UserRepo: repo, Sessions: sessions}
	if _, err := NewUpdateUserStatusLogic(context.Background(), sc).UpdateUserStatus(&system.UpdateUserStatusRequest{Id: 9, OperatorId: 1, Status: 1}); err != nil {
		t.Fatal(err)
	}
	if sessions.revokedUserID != 0 || repo.writeCalls != 1 {
		t.Fatal("enable revoked sessions")
	}
	for _, value := range []int32{-1, 2, 65536, 65537} {
		if _, err := NewUpdateUserStatusLogic(context.Background(), sc).UpdateUserStatus(&system.UpdateUserStatusRequest{Id: 9, OperatorId: 1, Status: value}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("status %d accepted", value)
		}
	}
}

func TestUserMutationDependencyFailuresAreNotReportedAsSuccess(t *testing.T) {
	dependencyErr := errors.New("database write failed")
	operations := []func(*svc.ServiceContext) error{
		func(sc *svc.ServiceContext) error {
			_, err := NewUpdateUserLogic(context.Background(), sc).UpdateUser(&system.UpdateUserRequest{Id: 9, OperatorId: 1, Nickname: "Alice"})
			return err
		},
		func(sc *svc.ServiceContext) error {
			_, err := NewUpdateUserStatusLogic(context.Background(), sc).UpdateUserStatus(&system.UpdateUserStatusRequest{Id: 9, OperatorId: 1, Status: 0})
			return err
		},
		func(sc *svc.ServiceContext) error {
			_, err := NewDeleteUserLogic(context.Background(), sc).DeleteUser(&system.DeleteUserRequest{Id: 9, OperatorId: 1})
			return err
		},
		func(sc *svc.ServiceContext) error {
			_, err := NewReplaceUserRolesLogic(context.Background(), sc).ReplaceUserRoles(&system.ReplaceUserRolesRequest{Id: 9, OperatorId: 1, RoleIds: []int64{8}})
			return err
		},
		func(sc *svc.ServiceContext) error {
			_, err := NewResetUserPasswordLogic(context.Background(), sc).ResetUserPassword(&system.ResetUserPasswordRequest{Id: 9, OperatorId: 1, Password: "abcdefghijkl"})
			return err
		},
	}
	for _, call := range operations {
		for _, readFailure := range []bool{true, false} {
			repo := &userRepositoryStub{user: &model.User{Base: model.Base{ID: 9}, Status: 1}, updateErr: dependencyErr}
			if readFailure {
				repo.findErr = dependencyErr
			}
			sessions := &sessionStoreLogicStub{}
			err := call(&svc.ServiceContext{UserRepo: repo, Sessions: sessions, Passwords: &passwordVerifierStub{nextHash: "hash"}})
			if !errors.Is(err, dependencyErr) || (readFailure && (sessions.revokedUserID != 0 || repo.writeCalls != 0)) {
				t.Fatalf("dependency failure: %v", err)
			}
		}
	}
	repo := &userRepositoryStub{user: &model.User{Base: model.Base{ID: 9}}}
	sessions := &sessionStoreLogicStub{}
	_, err := NewResetUserPasswordLogic(context.Background(), &svc.ServiceContext{UserRepo: repo, Sessions: sessions, Passwords: &passwordVerifierStub{hashErr: dependencyErr}}).
		ResetUserPassword(&system.ResetUserPasswordRequest{Id: 9, OperatorId: 1, Password: "abcdefghijkl"})
	if !errors.Is(err, dependencyErr) || sessions.revokedUserID != 0 || repo.passwordHash != "" {
		t.Fatalf("hash failure changed user: %v", err)
	}
}

func TestInitializedSuperAdminManagementBoundary(t *testing.T) {
	operations := []struct {
		name         string
		deactivating bool
		allowSelf    bool
		call         func(*svc.ServiceContext, int64) error
	}{
		{"profile", false, true, func(sc *svc.ServiceContext, operator int64) error {
			_, err := NewUpdateUserLogic(context.Background(), sc).UpdateUser(&system.UpdateUserRequest{Id: 42, OperatorId: operator, Nickname: "Administrator"})
			return err
		}},
		{"roles", false, false, func(sc *svc.ServiceContext, operator int64) error {
			_, err := NewReplaceUserRolesLogic(context.Background(), sc).ReplaceUserRoles(&system.ReplaceUserRolesRequest{Id: 42, OperatorId: operator, RoleIds: []int64{8}})
			return err
		}},
		{"password", false, true, func(sc *svc.ServiceContext, operator int64) error {
			_, err := NewResetUserPasswordLogic(context.Background(), sc).ResetUserPassword(&system.ResetUserPasswordRequest{Id: 42, OperatorId: operator, Password: "abcdefghijkl"})
			return err
		}},
		{"disable", true, false, func(sc *svc.ServiceContext, operator int64) error {
			_, err := NewUpdateUserStatusLogic(context.Background(), sc).UpdateUserStatus(&system.UpdateUserStatusRequest{Id: 42, OperatorId: operator, Status: 0})
			return err
		}},
		{"delete", true, false, func(sc *svc.ServiceContext, operator int64) error {
			_, err := NewDeleteUserLogic(context.Background(), sc).DeleteUser(&system.DeleteUserRequest{Id: 42, OperatorId: operator})
			return err
		}},
	}
	for _, operation := range operations {
		for _, operator := range []int64{1, 42} {
			t.Run(fmt.Sprintf("%s/operator=%d", operation.name, operator), func(t *testing.T) {
				repo := &userRepositoryStub{record: &repository.UserRecord{
					User:  model.User{Base: model.Base{ID: 42}, Status: 1},
					Roles: []model.Role{{Code: model.SuperAdminRoleCode, Status: 1}},
				}}
				sessions := &sessionStoreLogicStub{}
				sc := &svc.ServiceContext{UserRepo: repo, Sessions: sessions, Passwords: &passwordVerifierStub{nextHash: "hash"}}
				err := operation.call(sc, operator)
				if operator == 42 && operation.allowSelf {
					if err != nil || (repo.writeCalls != 1 && repo.passwordHash != "hash") {
						t.Fatalf("super administrator could not manage own account: %v", err)
					}
					return
				}
				want := subcode.UserSuperAdminProtected
				if operator == 42 && operation.deactivating {
					want = subcode.UserSelfProtected
				}
				if !hasBusinessSubcode(err, want) || sessions.revokedUserID != 0 || repo.writeCalls != 0 || repo.passwordHash != "" {
					t.Fatalf("protected operation had side effects: %v", err)
				}
			})
		}
	}
}

func TestUserManagementInvalidRequestsStopBeforeDependencies(t *testing.T) {
	sc := &svc.ServiceContext{}
	ctx := context.Background()
	calls := []func() error{
		func() error { _, err := NewCreateUserLogic(ctx, sc).CreateUser(nil); return err },
		func() error { _, err := NewGetUserLogic(ctx, sc).GetUser(nil); return err },
		func() error { _, err := NewUpdateUserLogic(ctx, sc).UpdateUser(nil); return err },
		func() error {
			_, err := NewUpdateUserLogic(ctx, sc).UpdateUser(&system.UpdateUserRequest{Id: 9, OperatorId: 1, Nickname: " "})
			return err
		},
		func() error { _, err := NewDeleteUserLogic(ctx, sc).DeleteUser(nil); return err },
		func() error {
			_, err := NewDeleteUserLogic(ctx, sc).DeleteUser(&system.DeleteUserRequest{Id: 9})
			return err
		},
		func() error { _, err := NewReplaceUserRolesLogic(ctx, sc).ReplaceUserRoles(nil); return err },
		func() error { _, err := NewResetUserPasswordLogic(ctx, sc).ResetUserPassword(nil); return err },
		func() error { _, err := NewListUserRoleOptionsLogic(ctx, sc).ListUserRoleOptions(nil); return err },
		func() error {
			_, err := NewListUserRoleOptionsLogic(ctx, sc).ListUserRoleOptions(&system.ListRolesRequest{})
			return err
		},
	}
	for _, call := range calls {
		if err := call(); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid request reached dependencies: %v", err)
		}
	}
	profile, err := normalizeUserProfile("昵称", "test@example.com", "123", "")
	if err != nil || profile.Email == nil || *profile.Email != "test@example.com" || profile.Phone == nil {
		t.Fatalf("valid contacts: %+v %v", profile, err)
	}
}
