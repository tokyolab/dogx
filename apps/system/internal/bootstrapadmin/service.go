package bootstrapadmin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
)

const (
	minPasswordBytes = 12
	maxPasswordBytes = 128
	maxUsernameBytes = 64
	maxNicknameBytes = 64
)

type Input struct {
	Username string
	Password string
	Nickname string
}

func Create(
	ctx context.Context,
	repo repository.UserRepository,
	hasher authn.PasswordHasher,
	input Input,
) (*model.User, error) {
	if repo == nil {
		return nil, errors.New("user repository is nil")
	}
	if hasher == nil {
		return nil, errors.New("password hasher is nil")
	}

	username := strings.TrimSpace(input.Username)
	nickname := strings.TrimSpace(input.Nickname)
	if nickname == "" {
		nickname = username
	}
	if username == "" || len(username) > maxUsernameBytes {
		return nil, fmt.Errorf("username must contain 1 to %d bytes", maxUsernameBytes)
	}
	if nickname == "" || len(nickname) > maxNicknameBytes {
		return nil, fmt.Errorf("nickname must contain 1 to %d bytes", maxNicknameBytes)
	}
	if len(input.Password) < minPasswordBytes || len(input.Password) > maxPasswordBytes {
		return nil, fmt.Errorf(
			"password must contain %d to %d bytes",
			minPasswordBytes,
			maxPasswordBytes,
		)
	}

	passwordHash, err := hasher.Hash(input.Password)
	if err != nil {
		return nil, fmt.Errorf("hash administrator password: %w", err)
	}
	user := &model.User{
		Username:     username,
		PasswordHash: passwordHash,
		Nickname:     nickname,
		Status:       model.RecordStatusEnabled,
		Remark:       "initial administrator",
	}
	if err := repo.Create(ctx, user); err != nil {
		return nil, err
	}

	return user, nil
}
