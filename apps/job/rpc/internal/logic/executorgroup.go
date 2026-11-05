package logic

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/pkg/bizerror"
)

// validGroupIdentity preserves the exact immutable name or discovery key.
// Normalizing either value could silently create a different executor identity.
func validGroupIdentity(value string, capacity int) bool {
	return value != "" && value == strings.TrimSpace(value) && utf8.ValidString(value) &&
		!strings.ContainsRune(value, 0) && utf8.RuneCountInString(value) <= capacity
}

// executorGroupError maps group-management failures without changing the
// existing Job-reference error contract. Unknown causes remain technical errors.
func executorGroupError(operation string, err error) error {
	switch {
	case errors.Is(err, repository.ErrExecutorGroupUnavailable):
		return bizerror.New(subcode.ExecutorGroupNotFound, "执行器组不存在")
	case errors.Is(err, repository.ErrExecutorGroupNameExists):
		return bizerror.New(subcode.ExecutorGroupNameExists, "执行器组名称已存在")
	case errors.Is(err, repository.ErrExecutorGroupDiscoveryKeyExists):
		return bizerror.New(subcode.ExecutorGroupDiscoveryKeyExists, "执行器组发现 Key 已存在")
	default:
		return rpcOperationError(operation, err)
	}
}
