package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/model"

	"gorm.io/gorm"
)

type LoginLogRepository interface {
	Create(ctx context.Context, loginLog *model.LoginLog) error
}

type loginLogRepository struct {
	db *gorm.DB
}

func NewLoginLogRepository(db *gorm.DB) (LoginLogRepository, error) {
	if db == nil {
		return nil, errors.New("login log repository database is nil")
	}
	return &loginLogRepository{db: db}, nil
}

func (r *loginLogRepository) Create(ctx context.Context, loginLog *model.LoginLog) error {
	if loginLog == nil {
		return errors.New("login log is nil")
	}
	if err := r.db.WithContext(ctx).Create(loginLog).Error; err != nil {
		return fmt.Errorf("create login log: %w", err)
	}
	return nil
}
