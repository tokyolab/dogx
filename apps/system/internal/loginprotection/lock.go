package loginprotection

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
)

type FailureStore interface {
	Locked(context.Context, string) (bool, error)
	Fail(context.Context, string, *system.LoginSecurityConfig) error
	Clear(context.Context, string) error
}

type scriptClient interface {
	EvalCtx(context.Context, string, []string, ...any) (any, error)
}
type RedisFailureStore struct{ client scriptClient }

func NewFailureStore(client scriptClient) *RedisFailureStore {
	return &RedisFailureStore{client: client}
}

func failureKey(username string) string {
	// Case-insensitive identity matches username lookup; a digest also bounds key size.
	return fmt.Sprintf("dogx:auth:failure:%x", sha256.Sum256([]byte(strings.ToLower(username))))
}

func (s *RedisFailureStore) Locked(ctx context.Context, username string) (bool, error) {
	value, err := s.client.EvalCtx(ctx, "return redis.call('HGET', KEYS[1], 'locked') or 0", []string{failureKey(username)})
	return value == "1", err
}

func (s *RedisFailureStore) Fail(ctx context.Context, username string, cfg *system.LoginSecurityConfig) error {
	_, err := s.client.EvalCtx(ctx, failScript, []string{failureKey(username)}, cfg.FailureWindowSeconds, cfg.FailureThreshold, cfg.LockDurationSeconds)
	return err
}

func (s *RedisFailureStore) Clear(ctx context.Context, username string) error {
	// A successful in-flight attempt must not remove a lock installed after its
	// initial check by concurrent failed requests. Existing sessions remain valid.
	_, err := s.client.EvalCtx(ctx, clearScript, []string{failureKey(username)})
	return err
}

// One hash transitions from counting to locked atomically. Its TTL changes only
// on the first failure and on that transition; retries cannot extend a lock.
const failScript = `
if redis.call('HGET', KEYS[1], 'locked') == '1' then return 0 end
local count = redis.call('HINCRBY', KEYS[1], 'count', 1)
if count == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
if count >= tonumber(ARGV[2]) then
    redis.call('HSET', KEYS[1], 'locked', '1')
    redis.call('EXPIRE', KEYS[1], ARGV[3])
end
return count
`

const clearScript = `
if redis.call('HGET', KEYS[1], 'locked') ~= '1' then
    return redis.call('DEL', KEYS[1])
end
return 0
`
