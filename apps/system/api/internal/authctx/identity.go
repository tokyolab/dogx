package authctx

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
)

const (
	userIDClaimKey    = "userId"
	sessionIDClaimKey = "sessionId"
)

var ErrInvalidIdentity = errors.New("invalid authenticated identity")

type Identity struct {
	UserID    int64
	SessionID string
}

func FromContext(ctx context.Context) (Identity, error) {
	if ctx == nil {
		return Identity{}, ErrInvalidIdentity
	}

	userID, ok := claimInt64(ctx.Value(userIDClaimKey))
	if !ok || userID <= 0 {
		return Identity{}, ErrInvalidIdentity
	}
	sessionID, ok := ctx.Value(sessionIDClaimKey).(string)
	if !ok || sessionID == "" {
		return Identity{}, ErrInvalidIdentity
	}

	return Identity{UserID: userID, SessionID: sessionID}, nil
}

func claimInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Int64()
		return parsed, err == nil
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case float64:
		if math.Trunc(typed) != typed || typed > math.MaxInt64 || typed < math.MinInt64 {
			return 0, false
		}
		return int64(typed), true
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
