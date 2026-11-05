package execution

import (
	"context"
	"crypto/subtle"
	"errors"

	"github.com/tokyolab/dogx/apps/job/internal/groupcache"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
)

// CachedGroupAuthenticator checks result credentials against the process-local
// group cache. It performs no Task queries and never returns or logs supplied
// credentials. A successful match establishes only the group identity; the
// result consumer must still verify persisted ownership and execution facts.
type CachedGroupAuthenticator struct {
	cache *groupcache.Cache
}

// NewCachedGroupAuthenticator requires a cache whose initial full load has
// succeeded. It does not start its refresh loop or establish queue readiness.
func NewCachedGroupAuthenticator(cache *groupcache.Cache) (*CachedGroupAuthenticator, error) {
	if cache == nil {
		return nil, errors.New("executor group credential cache is nil")
	}
	return &CachedGroupAuthenticator{cache: cache}, nil
}

// Authenticate distinguishes definite absence or a token mismatch from a
// failed source lookup. It compares immutable tokens in constant time for
// equal-length inputs; token length and format are public. No normalization is
// applied to the case-sensitive group name or token.
func (a *CachedGroupAuthenticator) Authenticate(ctx context.Context, groupName, token string) (int64, error) {
	if err := jobexecutor.ValidateCredentials(groupName, token); err != nil {
		return 0, ErrInvalidCredentials
	}
	config, err := a.cache.ByName(ctx, groupName)
	if errors.Is(err, groupcache.ErrNotFound) {
		return 0, ErrInvalidCredentials
	}
	if err != nil {
		return 0, err
	}
	if subtle.ConstantTimeCompare([]byte(config.Token), []byte(token)) != 1 {
		return 0, ErrInvalidCredentials
	}
	return config.ID, nil
}
