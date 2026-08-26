package logic

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type credentialRefresherStub struct {
	token       string
	credentials *authn.Credentials
	err         error
}

func (s *credentialRefresherStub) Refresh(_ context.Context, token string) (*authn.Credentials, error) {
	s.token = token
	return s.credentials, s.err
}

type sessionStoreLogicStub struct {
	session       *authn.Session
	getErr        error
	revokeErr     error
	revokeAllErr  error
	revokedUserID int64
	revokedID     string
}

func (s *sessionStoreLogicStub) Create(context.Context, authn.Session, time.Duration) error {
	return nil
}

func (s *sessionStoreLogicStub) Get(context.Context, string) (*authn.Session, error) {
	return s.session, s.getErr
}

func (s *sessionStoreLogicStub) RotateRefreshToken(
	context.Context,
	string,
	string,
	string,
	time.Time,
	time.Duration,
) (*authn.Session, error) {
	return nil, nil
}

func (s *sessionStoreLogicStub) Revoke(_ context.Context, userID int64, sessionID string) error {
	s.revokedUserID = userID
	s.revokedID = sessionID
	return s.revokeErr
}

func (s *sessionStoreLogicStub) RevokeAll(_ context.Context, userID int64) error {
	s.revokedUserID = userID
	return s.revokeAllErr
}

func TestRefreshCredentials(t *testing.T) {
	refresher := &credentialRefresherStub{credentials: &authn.Credentials{
		AccessToken:  "new-access",
		RefreshToken: "new-refresh",
		ExpiresIn:    900,
	}}
	logic := NewRefreshCredentialsLogic(context.Background(), &svc.ServiceContext{RefreshTokens: refresher})

	response, err := logic.RefreshCredentials(&system.RefreshCredentialsRequest{RefreshToken: "old-refresh"})
	if err != nil {
		t.Fatalf("refresh credentials: %v", err)
	}
	if refresher.token != "old-refresh" || response.AccessToken != "new-access" || response.RefreshToken != "new-refresh" {
		t.Fatalf("unexpected refresh: token=%q response=%+v", refresher.token, response)
	}
}

func TestRefreshCredentialsMapsInvalidTokenToUnauthenticated(t *testing.T) {
	logic := NewRefreshCredentialsLogic(context.Background(), &svc.ServiceContext{
		RefreshTokens: &credentialRefresherStub{err: authn.ErrInvalidRefreshToken},
	})
	if _, err := logic.RefreshCredentials(&system.RefreshCredentialsRequest{RefreshToken: "invalid"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got: %v", err)
	}
	if _, err := logic.RefreshCredentials(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got: %v", err)
	}
}

func TestGetCurrentUserAndRevokeDisabledUser(t *testing.T) {
	user := enabledUser()
	user.Nickname = "Administrator"
	sessions := &sessionStoreLogicStub{}
	logic := NewGetCurrentUserLogic(context.Background(), &svc.ServiceContext{
		UserRepo: &userRepositoryStub{user: user},
		Sessions: sessions,
	})
	response, err := logic.GetCurrentUser(&system.CurrentUserRequest{UserId: 42})
	if err != nil {
		t.Fatalf("get current user: %v", err)
	}
	if response.Id != 42 || response.Username != "admin" || response.Nickname != "Administrator" {
		t.Fatalf("unexpected current user: %+v", response)
	}

	user.Status = model.RecordStatusDisabled
	if _, err := logic.GetCurrentUser(&system.CurrentUserRequest{UserId: 42}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected disabled user to be rejected, got: %v", err)
	}
	if sessions.revokedUserID != 42 {
		t.Fatal("disabled user sessions were not revoked")
	}
}

func TestSessionRevocationLogics(t *testing.T) {
	sessions := &sessionStoreLogicStub{}
	svcCtx := &svc.ServiceContext{Sessions: sessions}
	if _, err := NewRevokeSessionLogic(context.Background(), svcCtx).RevokeSession(
		&system.RevokeSessionRequest{UserId: 42, SessionId: "session-id"},
	); err != nil {
		t.Fatalf("revoke session: %v", err)
	}
	if sessions.revokedUserID != 42 || sessions.revokedID != "session-id" {
		t.Fatalf("unexpected session revocation: %+v", sessions)
	}
	if _, err := NewRevokeUserSessionsLogic(context.Background(), svcCtx).RevokeUserSessions(
		&system.RevokeUserSessionsRequest{UserId: 42},
	); err != nil {
		t.Fatalf("revoke user sessions: %v", err)
	}

	sessions.revokeErr = authn.ErrSessionUserMismatch
	if _, err := NewRevokeSessionLogic(context.Background(), svcCtx).RevokeSession(
		&system.RevokeSessionRequest{UserId: 42, SessionId: "other"},
	); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected ownership mismatch to be forbidden, got: %v", err)
	}
}

func TestAuthenticationLogicsRejectInvalidInputAndPropagateDependencyFailures(t *testing.T) {
	dependencyErr := errors.New("dependency unavailable")

	t.Run("refresh dependency failure", func(t *testing.T) {
		logic := NewRefreshCredentialsLogic(context.Background(), &svc.ServiceContext{
			RefreshTokens: &credentialRefresherStub{err: dependencyErr},
		})
		_, err := logic.RefreshCredentials(&system.RefreshCredentialsRequest{RefreshToken: "refresh-token"})
		if !errors.Is(err, dependencyErr) {
			t.Fatalf("refresh error = %v, want dependency failure", err)
		}
	})

	t.Run("current user invalid request", func(t *testing.T) {
		_, err := NewGetCurrentUserLogic(context.Background(), &svc.ServiceContext{}).GetCurrentUser(nil)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("current-user error = %v, want invalid argument", err)
		}
	})

	t.Run("current user missing", func(t *testing.T) {
		logic := NewGetCurrentUserLogic(context.Background(), &svc.ServiceContext{
			UserRepo: &userRepositoryStub{findByIDErr: repository.ErrUserNotFound},
		})
		_, err := logic.GetCurrentUser(&system.CurrentUserRequest{UserId: 42})
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("missing-user error = %v, want unauthenticated", err)
		}
	})

	t.Run("current user repository failure", func(t *testing.T) {
		logic := NewGetCurrentUserLogic(context.Background(), &svc.ServiceContext{
			UserRepo: &userRepositoryStub{findByIDErr: dependencyErr},
		})
		_, err := logic.GetCurrentUser(&system.CurrentUserRequest{UserId: 42})
		if !errors.Is(err, dependencyErr) {
			t.Fatalf("current-user error = %v, want dependency failure", err)
		}
	})

	t.Run("disabled user revocation failure", func(t *testing.T) {
		user := enabledUser()
		user.Status = model.RecordStatusDisabled
		logic := NewGetCurrentUserLogic(context.Background(), &svc.ServiceContext{
			UserRepo: &userRepositoryStub{user: user},
			Sessions: &sessionStoreLogicStub{revokeAllErr: dependencyErr},
		})
		_, err := logic.GetCurrentUser(&system.CurrentUserRequest{UserId: 42})
		if !errors.Is(err, dependencyErr) {
			t.Fatalf("disabled-user error = %v, want revocation failure", err)
		}
	})

	t.Run("single-session invalid request and dependency failure", func(t *testing.T) {
		logic := NewRevokeSessionLogic(context.Background(), &svc.ServiceContext{})
		if _, err := logic.RevokeSession(nil); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid revoke error = %v, want invalid argument", err)
		}
		logic = NewRevokeSessionLogic(context.Background(), &svc.ServiceContext{
			Sessions: &sessionStoreLogicStub{revokeErr: dependencyErr},
		})
		_, err := logic.RevokeSession(&system.RevokeSessionRequest{UserId: 42, SessionId: "session-id"})
		if !errors.Is(err, dependencyErr) {
			t.Fatalf("revoke error = %v, want dependency failure", err)
		}
	})

	t.Run("all-sessions invalid request and dependency failure", func(t *testing.T) {
		logic := NewRevokeUserSessionsLogic(context.Background(), &svc.ServiceContext{})
		if _, err := logic.RevokeUserSessions(nil); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid revoke-all error = %v, want invalid argument", err)
		}
		logic = NewRevokeUserSessionsLogic(context.Background(), &svc.ServiceContext{
			Sessions: &sessionStoreLogicStub{revokeAllErr: dependencyErr},
		})
		_, err := logic.RevokeUserSessions(&system.RevokeUserSessionsRequest{UserId: 42})
		if !errors.Is(err, dependencyErr) {
			t.Fatalf("revoke-all error = %v, want dependency failure", err)
		}
	})
}
