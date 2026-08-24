package logic

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type userRepositoryStub struct {
	user        *model.User
	findErr     error
	findByIDErr error
	username    string
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

type passwordVerifierStub struct {
	hash     string
	password string
	err      error
}

func (s *passwordVerifierStub) Verify(hash, password string) error {
	s.hash = hash
	s.password = password
	return s.err
}

type credentialIssuerStub struct {
	userID      int64
	credentials *authn.Credentials
	err         error
}

func (s *credentialIssuerStub) Issue(_ context.Context, userID int64) (*authn.Credentials, error) {
	s.userID = userID
	return s.credentials, s.err
}

func TestLogin(t *testing.T) {
	repo := &userRepositoryStub{user: enabledUser()}
	passwords := &passwordVerifierStub{}
	tokens := &credentialIssuerStub{credentials: &authn.Credentials{
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		ExpiresIn:    900,
	}}
	logic := newLoginLogicForTest(repo, passwords, tokens)

	response, err := logic.Login(&system.LoginRequest{
		Username: "  Admin  ",
		Password: "correct-password",
	})
	if err != nil {
		t.Fatalf("login returned error: %v", err)
	}
	if repo.username != "Admin" || passwords.hash != "encoded-hash" || passwords.password != "correct-password" {
		t.Fatalf("unexpected credential verification: username=%q hash=%q password=%q", repo.username, passwords.hash, passwords.password)
	}
	if tokens.userID != 42 {
		t.Fatalf("unexpected token user id: %d", tokens.userID)
	}
	if response.AccessToken != "access-token" || response.RefreshToken != "refresh-token" || response.ExpiresIn != 900 {
		t.Fatalf("unexpected login response: %+v", response)
	}
}

func TestLoginRejectsInvalidRequest(t *testing.T) {
	logic := newLoginLogicForTest(&userRepositoryStub{}, &passwordVerifierStub{}, &credentialIssuerStub{})
	requests := []*system.LoginRequest{
		nil,
		{},
		{Username: strings.Repeat("a", maxUsernameBytes+1), Password: "password"},
		{Username: "admin", Password: strings.Repeat("p", maxPasswordBytes+1)},
	}
	for _, request := range requests {
		if _, err := logic.Login(request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected invalid argument, got: %v", err)
		}
	}
}

func TestLoginUsesSameErrorForUnknownUserAndWrongPassword(t *testing.T) {
	unknownPasswords := &passwordVerifierStub{}
	unknown := newLoginLogicForTest(
		&userRepositoryStub{findErr: repository.ErrUserNotFound},
		unknownPasswords,
		&credentialIssuerStub{},
	)
	_, unknownErr := unknown.Login(&system.LoginRequest{Username: "missing", Password: "password"})

	wrong := newLoginLogicForTest(
		&userRepositoryStub{user: enabledUser()},
		&passwordVerifierStub{err: authn.ErrPasswordMismatch},
		&credentialIssuerStub{},
	)
	_, wrongErr := wrong.Login(&system.LoginRequest{Username: "admin", Password: "wrong"})

	unknownBizErr, unknownOK := bizerror.From(unknownErr)
	wrongBizErr, wrongOK := bizerror.From(wrongErr)
	if !unknownOK || !wrongOK || unknownBizErr.Code() != wrongBizErr.Code() || unknownBizErr.Error() != wrongBizErr.Error() {
		t.Fatalf("credential failures differ: unknown=%v wrong=%v", unknownErr, wrongErr)
	}
	if unknownPasswords.hash != authn.DummyPasswordHash() || unknownPasswords.password != "password" {
		t.Fatal("unknown user did not execute dummy password verification")
	}
}

func TestLoginRejectsDisabledUserAfterPasswordVerification(t *testing.T) {
	user := enabledUser()
	user.Status = model.RecordStatusDisabled
	passwords := &passwordVerifierStub{}
	logic := newLoginLogicForTest(&userRepositoryStub{user: user}, passwords, &credentialIssuerStub{})

	_, err := logic.Login(&system.LoginRequest{Username: "admin", Password: "password"})
	bizErr, ok := bizerror.From(err)
	if !ok || bizErr.Error() != "账号已停用" {
		t.Fatalf("unexpected disabled account error: %v", err)
	}
	if passwords.password != "password" {
		t.Fatal("disabled account was rejected before password verification")
	}
}

func TestLoginPreservesTechnicalFailures(t *testing.T) {
	repositoryErr := errors.New("database unavailable")
	logic := newLoginLogicForTest(
		&userRepositoryStub{findErr: repositoryErr},
		&passwordVerifierStub{},
		&credentialIssuerStub{},
	)
	if _, err := logic.Login(&system.LoginRequest{Username: "admin", Password: "password"}); !errors.Is(err, repositoryErr) {
		t.Fatalf("expected repository error, got: %v", err)
	}

	passwordErr := errors.New("malformed password hash")
	logic = newLoginLogicForTest(
		&userRepositoryStub{user: enabledUser()},
		&passwordVerifierStub{err: passwordErr},
		&credentialIssuerStub{},
	)
	if _, err := logic.Login(&system.LoginRequest{Username: "admin", Password: "password"}); !errors.Is(err, passwordErr) {
		t.Fatalf("expected password error, got: %v", err)
	}

	tokenErr := errors.New("redis unavailable")
	logic = newLoginLogicForTest(
		&userRepositoryStub{user: enabledUser()},
		&passwordVerifierStub{},
		&credentialIssuerStub{err: tokenErr},
	)
	if _, err := logic.Login(&system.LoginRequest{Username: "admin", Password: "password"}); !errors.Is(err, tokenErr) {
		t.Fatalf("expected token error, got: %v", err)
	}
}

func newLoginLogicForTest(
	repo repository.UserRepository,
	passwords authn.PasswordVerifier,
	tokens authn.CredentialIssuer,
) *LoginLogic {
	return NewLoginLogic(context.Background(), &svc.ServiceContext{
		UserRepo:  repo,
		Passwords: passwords,
		Tokens:    tokens,
	})
}

func enabledUser() *model.User {
	return &model.User{
		Base:         model.Base{ID: 42},
		Username:     "admin",
		PasswordHash: "encoded-hash",
		Status:       model.RecordStatusEnabled,
	}
}
