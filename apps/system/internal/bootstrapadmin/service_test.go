package bootstrapadmin

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
)

type userRepositoryStub struct {
	created *model.User
	err     error
}

func (s *userRepositoryStub) Create(_ context.Context, user *model.User) error {
	s.created = user
	user.ID = 42
	return s.err
}

func (s *userRepositoryStub) FindByID(context.Context, int64) (*model.User, error) {
	return nil, errors.New("not implemented")
}

func (s *userRepositoryStub) FindByUsername(context.Context, string) (*model.User, error) {
	return nil, errors.New("not implemented")
}

type passwordHasherStub struct {
	password string
	hash     string
	err      error
}

func (s *passwordHasherStub) Hash(password string) (string, error) {
	s.password = password
	return s.hash, s.err
}

func (s *passwordHasherStub) Verify(string, string) error {
	return errors.New("not implemented")
}

func TestCreate(t *testing.T) {
	repo := &userRepositoryStub{}
	hasher := &passwordHasherStub{hash: "encoded-hash"}

	user, err := Create(context.Background(), repo, hasher, Input{
		Username: "  admin  ",
		Password: "secure-password",
	})
	if err != nil {
		t.Fatalf("create administrator: %v", err)
	}
	if user != repo.created || user.ID != 42 || user.Username != "admin" || user.Nickname != "admin" {
		t.Fatalf("unexpected administrator: %+v", user)
	}
	if user.PasswordHash != "encoded-hash" || hasher.password != "secure-password" {
		t.Fatal("administrator password was not hashed")
	}
	if user.Status != model.RecordStatusEnabled {
		t.Fatalf("administrator is not enabled: %d", user.Status)
	}
}

func TestCreateValidatesInputAndDependencies(t *testing.T) {
	valid := Input{Username: "admin", Password: "secure-password", Nickname: "Administrator"}
	tests := []struct {
		name   string
		repo   userCreator
		hasher authn.PasswordHasher
		input  Input
	}{
		{name: "nil repository", hasher: &passwordHasherStub{}, input: valid},
		{name: "nil hasher", repo: &userRepositoryStub{}, input: valid},
		{name: "empty username", repo: &userRepositoryStub{}, hasher: &passwordHasherStub{}, input: Input{Password: valid.Password}},
		{name: "long username", repo: &userRepositoryStub{}, hasher: &passwordHasherStub{}, input: Input{Username: strings.Repeat("a", 65), Password: valid.Password}},
		{name: "long nickname", repo: &userRepositoryStub{}, hasher: &passwordHasherStub{}, input: Input{Username: "admin", Nickname: strings.Repeat("a", 65), Password: valid.Password}},
		{name: "short password", repo: &userRepositoryStub{}, hasher: &passwordHasherStub{}, input: Input{Username: "admin", Password: "short"}},
		{name: "long password", repo: &userRepositoryStub{}, hasher: &passwordHasherStub{}, input: Input{Username: "admin", Password: strings.Repeat("a", 129)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Create(context.Background(), test.repo, test.hasher, test.input); err == nil {
				t.Fatal("expected invalid input to be rejected")
			}
		})
	}
}

func TestCreatePreservesHashAndRepositoryErrors(t *testing.T) {
	hashErr := errors.New("hash failed")
	if _, err := Create(
		context.Background(),
		&userRepositoryStub{},
		&passwordHasherStub{err: hashErr},
		Input{Username: "admin", Password: "secure-password"},
	); !errors.Is(err, hashErr) {
		t.Fatalf("expected hash error, got: %v", err)
	}

	repositoryErr := errors.New("create failed")
	if _, err := Create(
		context.Background(),
		&userRepositoryStub{err: repositoryErr},
		&passwordHasherStub{hash: "encoded-hash"},
		Input{Username: "admin", Password: "secure-password"},
	); !errors.Is(err, repositoryErr) {
		t.Fatalf("expected repository error, got: %v", err)
	}
}
