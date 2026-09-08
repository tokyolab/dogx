package logic

import (
	"context"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
)

type userRepositoryStub struct {
	record             *repository.UserRecord
	records            []repository.UserRecord
	query              repository.UserListQuery
	total              int64
	created            *model.User
	profile            repository.UserProfileUpdate
	roleIDs            []int64
	rolesErr           error
	validateRolesCalls int
	writeCalls         int
	user               *model.User
	findErr            error
	findByIDErr        error
	username           string
	lastLoginAt        time.Time
	passwordHash       string
	updateErr          error
}

func (s *userRepositoryStub) Create(context.Context, *model.User) error {
	return nil
}

func (s *userRepositoryStub) FindByID(context.Context, int64) (*model.User, error) {
	if s.findByIDErr != nil {
		return nil, s.findByIDErr
	}
	if s.user == nil {
		return nil, repository.ErrUserNotFound
	}
	return s.user, nil
}

func (s *userRepositoryStub) FindByUsername(_ context.Context, username string) (*model.User, error) {
	s.username = username
	return s.user, s.findErr
}

func (s *userRepositoryStub) UpdateLastLoginAt(_ context.Context, _ int64, lastLoginAt time.Time) error {
	s.lastLoginAt = lastLoginAt
	return s.updateErr
}

func (s *userRepositoryStub) UpdatePasswordHash(_ context.Context, _ int64, passwordHash string) error {
	s.passwordHash = passwordHash
	return s.updateErr
}

func (s *userRepositoryStub) List(_ context.Context, query repository.UserListQuery) ([]repository.UserRecord, int64, error) {
	s.query = query
	return s.records, s.total, s.findErr
}

func (s *userRepositoryStub) FindWithRoles(context.Context, int64) (*repository.UserRecord, error) {
	if s.findErr != nil {
		return nil, s.findErr
	}
	if s.record != nil {
		return s.record, nil
	}
	if s.user != nil {
		return &repository.UserRecord{User: *s.user}, nil
	}
	return nil, repository.ErrUserNotFound
}

func (s *userRepositoryStub) CreateWithRoles(_ context.Context, user *model.User, ids []int64) error {
	s.writeCalls++
	s.created, s.roleIDs = user, ids
	if s.updateErr == nil {
		user.ID = 51
	}
	return s.updateErr
}

func (s *userRepositoryStub) ValidateRoles(context.Context, []int64, []model.Role) error {
	s.validateRolesCalls++
	return s.rolesErr
}

func (s *userRepositoryStub) UpdateProfile(_ context.Context, _ int64, profile repository.UserProfileUpdate) error {
	s.writeCalls++
	s.profile = profile
	return s.updateErr
}

func (s *userRepositoryStub) ReplaceRoles(_ context.Context, _ int64, ids []int64) error {
	s.writeCalls++
	s.roleIDs = ids
	return s.updateErr
}

func (s *userRepositoryStub) UpdateStatus(context.Context, int64, model.RecordStatus) error {
	s.writeCalls++
	return s.updateErr
}

func (s *userRepositoryStub) Delete(context.Context, int64) error {
	s.writeCalls++
	return s.updateErr
}

var _ repository.UserRepository = (*userRepositoryStub)(nil)
