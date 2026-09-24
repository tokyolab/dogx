package authn

import (
	"context"
	"time"
)

type sessionStoreStub struct {
	session Session
	ttl     time.Duration
	err     error
	revoked bool
	now     time.Time
}

func (s *sessionStoreStub) Create(_ context.Context, session Session, ttl time.Duration) error {
	s.session = session
	s.ttl = ttl
	return s.err
}

func (s *sessionStoreStub) Get(_ context.Context, sessionID string) (*Session, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.revoked || s.session.ID == "" || s.session.ID != sessionID {
		return nil, ErrSessionNotFound
	}
	copy := s.session
	return &copy, nil
}

func (s *sessionStoreStub) RotateRefreshToken(
	_ context.Context,
	sessionID string,
	currentHash string,
	nextHash string,
	expiresAt time.Time,
	ttl time.Duration,
	encryptedResult string,
) (*Session, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.revoked || s.session.ID != sessionID {
		return nil, ErrSessionNotFound
	}
	if s.now.UnixMilli() < s.session.RefreshReplayUntil && s.session.EncryptedRefreshResult != "" &&
		(currentHash == s.session.RefreshTokenHash || currentHash == s.session.PreviousRefreshTokenHash) {
		copy := s.session
		return &copy, nil
	}
	if s.session.RefreshTokenHash != currentHash {
		s.revoked = true
		return nil, ErrRefreshTokenMismatch
	}
	s.session.PreviousRefreshTokenHash = s.session.RefreshTokenHash
	s.session.RefreshTokenHash = nextHash
	s.session.RefreshReplayUntil = s.now.Add(5 * time.Second).UnixMilli()
	s.session.ExpiresAt = expiresAt
	s.session.EncryptedRefreshResult = encryptedResult
	s.ttl = ttl
	copy := s.session
	return &copy, nil
}

func (s *sessionStoreStub) Revoke(_ context.Context, userID int64, sessionID string) error {
	if s.session.UserID != userID || s.session.ID != sessionID {
		return ErrSessionUserMismatch
	}
	s.revoked = true
	return nil
}

func (s *sessionStoreStub) RevokeAll(_ context.Context, userID int64) error {
	if s.session.UserID == userID {
		s.revoked = true
	}
	return nil
}
