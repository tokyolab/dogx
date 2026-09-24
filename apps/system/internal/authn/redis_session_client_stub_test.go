package authn

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

type redisSessionClientStub struct {
	values map[string]string
	sets   map[string]map[string]struct{}
	ttls   map[string]int

	setexErr   error
	getErr     error
	delErr     error
	saddErr    error
	sremErr    error
	scanErr    error
	expireErr  error
	evalErr    error
	evalResult *int64
	evalValue  *string
	now        time.Time
}

func newRedisSessionClientStub() *redisSessionClientStub {
	return &redisSessionClientStub{
		values: make(map[string]string),
		sets:   make(map[string]map[string]struct{}),
		ttls:   make(map[string]int),
	}
}

func (s *redisSessionClientStub) SetexCtx(_ context.Context, key, value string, seconds int) error {
	if s.setexErr != nil {
		return s.setexErr
	}
	s.values[key] = value
	s.ttls[key] = seconds
	return nil
}

func (s *redisSessionClientStub) GetCtx(_ context.Context, key string) (string, error) {
	if s.getErr != nil {
		return "", s.getErr
	}
	return s.values[key], nil
}

func (s *redisSessionClientStub) DelCtx(_ context.Context, keys ...string) (int, error) {
	if s.delErr != nil {
		return 0, s.delErr
	}
	deleted := 0
	for _, key := range keys {
		if _, ok := s.values[key]; ok {
			delete(s.values, key)
			deleted++
		}
		if _, ok := s.sets[key]; ok {
			delete(s.sets, key)
			deleted++
		}
		delete(s.ttls, key)
	}
	return deleted, nil
}

func (s *redisSessionClientStub) SaddCtx(_ context.Context, key string, values ...any) (int, error) {
	if s.saddErr != nil {
		return 0, s.saddErr
	}
	if s.sets[key] == nil {
		s.sets[key] = make(map[string]struct{})
	}
	added := 0
	for _, value := range values {
		member := value.(string)
		if _, ok := s.sets[key][member]; !ok {
			s.sets[key][member] = struct{}{}
			added++
		}
	}
	return added, nil
}

func (s *redisSessionClientStub) SremCtx(_ context.Context, key string, values ...any) (int, error) {
	if s.sremErr != nil {
		return 0, s.sremErr
	}
	removed := 0
	for _, value := range values {
		member := value.(string)
		if _, ok := s.sets[key][member]; ok {
			delete(s.sets[key], member)
			removed++
		}
	}
	return removed, nil
}

func (s *redisSessionClientStub) SscanCtx(
	_ context.Context,
	key string,
	cursor uint64,
	_ string,
	count int64,
) ([]string, uint64, error) {
	if s.scanErr != nil {
		return nil, 0, s.scanErr
	}
	members := make([]string, 0, len(s.sets[key]))
	for member := range s.sets[key] {
		members = append(members, member)
	}
	sort.Strings(members)
	start := int(cursor)
	if start >= len(members) {
		return nil, 0, nil
	}
	end := start + int(count)
	if end >= len(members) {
		return members[start:], 0, nil
	}
	return members[start:end], uint64(end), nil
}

func (s *redisSessionClientStub) ExpireCtx(_ context.Context, key string, seconds int) error {
	if s.expireErr != nil {
		return s.expireErr
	}
	s.ttls[key] = seconds
	return nil
}

func (s *redisSessionClientStub) EvalCtx(
	_ context.Context,
	_ string,
	keys []string,
	args ...any,
) (any, error) {
	if s.evalErr != nil {
		return nil, s.evalErr
	}
	if s.evalValue != nil {
		return *s.evalValue, nil
	}
	if s.evalResult != nil {
		result := *s.evalResult
		if result <= 0 {
			delete(s.values, keys[0])
		}
		return result, nil
	}
	value := s.values[keys[0]]
	if value == "" {
		return int64(0), nil
	}
	var session Session
	if err := json.Unmarshal([]byte(value), &session); err != nil {
		return nil, err
	}
	now := s.now.UnixMilli()
	if now < session.RefreshReplayUntil && session.EncryptedRefreshResult != "" &&
		(session.RefreshTokenHash == args[0].(string) || session.PreviousRefreshTokenHash == args[0].(string)) {
		return value, nil
	}
	if session.RefreshTokenHash != args[0].(string) {
		delete(s.values, keys[0])
		return int64(-1), nil
	}
	session.PreviousRefreshTokenHash = session.RefreshTokenHash
	session.RefreshTokenHash = args[1].(string)
	session.RefreshReplayUntil = now + 5000
	session.EncryptedRefreshResult = args[4].(string)
	expiresAt, err := time.Parse(time.RFC3339Nano, args[2].(string))
	if err != nil {
		return nil, err
	}
	session.ExpiresAt = expiresAt
	encoded, _ := json.Marshal(session)
	s.values[keys[0]] = string(encoded)
	s.ttls[keys[0]] = args[3].(int)
	return string(encoded), nil
}
