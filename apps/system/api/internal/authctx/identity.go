package authctx

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
)

const (
	userIDClaimKey     = "userId"
	sessionIDClaimKey  = "sessionId"
	roleIDsClaimKey    = "roleIds"
	superAdminClaimKey = "isSuperAdmin"
)

var ErrInvalidIdentity = errors.New("invalid authenticated identity")

type Identity struct {
	UserID       int64
	SessionID    string
	RoleIDs      []int64
	IsSuperAdmin bool
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
	roleIDs, ok := claimInt64Slice(ctx.Value(roleIDsClaimKey))
	if !ok {
		return Identity{}, ErrInvalidIdentity
	}
	isSuperAdmin, ok := ctx.Value(superAdminClaimKey).(bool)
	if !ok {
		return Identity{}, ErrInvalidIdentity
	}

	return Identity{
		UserID:       userID,
		SessionID:    sessionID,
		RoleIDs:      roleIDs,
		IsSuperAdmin: isSuperAdmin,
	}, nil
}

func claimInt64Slice(value any) ([]int64, bool) {
	if value == nil {
		return []int64{}, true
	}

	var values []any
	switch typed := value.(type) {
	case []any:
		values = typed
	case []int64:
		values = make([]any, len(typed))
		for index, item := range typed {
			values[index] = item
		}
	case []int:
		values = make([]any, len(typed))
		for index, item := range typed {
			values[index] = item
		}
	default:
		return nil, false
	}

	unique := make(map[int64]struct{}, len(values))
	roleIDs := make([]int64, 0, len(values))
	for _, value := range values {
		roleID, ok := claimInt64(value)
		if !ok || roleID <= 0 {
			return nil, false
		}
		if _, exists := unique[roleID]; exists {
			continue
		}
		unique[roleID] = struct{}{}
		roleIDs = append(roleIDs, roleID)
	}
	return roleIDs, true
}

// go-zero JWT decoding and direct test contexts can expose numeric claims as
// different concrete Go types; normalize them without accepting fractions or
// overflowing int64.
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
