package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/authn"

	"github.com/golang-jwt/jwt/v4"
	"github.com/zeromicro/go-zero/rest/handler"
)

const middlewareTestSecret = "0123456789abcdef0123456789abcdef"

type sessionReaderStub struct {
	calls     int
	sessionID string
	session   *authn.Session
	err       error
}

func (s *sessionReaderStub) Get(_ context.Context, sessionID string) (*authn.Session, error) {
	s.calls++
	s.sessionID = sessionID
	return s.session, s.err
}

func TestJWTRejectsRandomTokenBeforeRedisSessionLookup(t *testing.T) {
	sessions := &sessionReaderStub{}
	nextCalled := false
	protected := handler.Authorize(middlewareTestSecret)(
		NewSessionAuthMiddleware(sessions).Handle(func(http.ResponseWriter, *http.Request) {
			nextCalled = true
		}),
	)

	request := httptest.NewRequest(http.MethodPost, "/auth/me", nil)
	request.Header.Set("Authorization", "Bearer random-invalid-token")
	recorder := httptest.NewRecorder()
	protected.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	if sessions.calls != 0 || nextCalled {
		t.Fatalf("invalid JWT reached Redis session lookup: calls=%d next=%v", sessions.calls, nextCalled)
	}
}

func TestValidJWTChecksRedisSessionBeforeHandler(t *testing.T) {
	sessions := &sessionReaderStub{session: &authn.Session{
		ID:        "session-id",
		UserID:    42,
		ExpiresAt: time.Now().Add(time.Hour),
	}}
	nextCalled := false
	protected := handler.Authorize(middlewareTestSecret)(
		NewSessionAuthMiddleware(sessions).Handle(func(http.ResponseWriter, *http.Request) {
			nextCalled = true
		}),
	)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"userId":    42,
		"sessionId": "session-id",
		"exp":       time.Now().Add(time.Minute).Unix(),
	})
	signed, err := token.SignedString([]byte(middlewareTestSecret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/auth/me", nil)
	request.Header.Set("Authorization", "Bearer "+signed)
	recorder := httptest.NewRecorder()
	protected.ServeHTTP(recorder, request)

	if sessions.calls != 1 || sessions.sessionID != "session-id" {
		t.Fatalf("unexpected Redis session lookup: calls=%d sessionId=%q", sessions.calls, sessions.sessionID)
	}
	if !nextCalled {
		t.Fatal("valid session did not reach handler")
	}
}

func TestInvalidOrUnavailableSessionDoesNotReachHandler(t *testing.T) {
	tests := []struct {
		name     string
		sessions authn.SessionReader
	}{
		{name: "missing", sessions: &sessionReaderStub{err: authn.ErrSessionNotFound}},
		{name: "wrong user", sessions: &sessionReaderStub{session: &authn.Session{UserID: 7, ExpiresAt: time.Now().Add(time.Hour)}}},
		{name: "expired", sessions: &sessionReaderStub{session: &authn.Session{UserID: 42, ExpiresAt: time.Now().Add(-time.Minute)}}},
		{name: "redis unavailable", sessions: &sessionReaderStub{err: errors.New("redis unavailable")}},
		{name: "missing reader", sessions: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nextCalled := false
			middleware := NewSessionAuthMiddleware(test.sessions).Handle(func(http.ResponseWriter, *http.Request) {
				nextCalled = true
			})
			ctx := context.WithValue(context.Background(), "userId", int64(42))
			ctx = context.WithValue(ctx, "sessionId", "session-id")
			request := httptest.NewRequest(http.MethodPost, "/auth/me", nil).WithContext(ctx)
			recorder := httptest.NewRecorder()

			middleware.ServeHTTP(recorder, request)

			if nextCalled {
				t.Fatal("invalid session reached handler")
			}
		})
	}
}
