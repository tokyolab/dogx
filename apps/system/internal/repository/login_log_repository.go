package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/model"

	"gorm.io/gorm"
)

type LoginLogRepository interface {
	Create(ctx context.Context, loginLog *model.LoginLog) error
	List(ctx context.Context, query LoginLogListQuery) ([]model.LoginLog, int64, error)
}

type LoginLogListQuery struct {
	Username string
	Success  *bool
	Offset   int
	Limit    int
}

func (r *loginLogRepository) List(ctx context.Context, query LoginLogListQuery) ([]model.LoginLog, int64, error) {
	if ctx == nil || query.Offset < 0 || query.Limit <= 0 {
		return nil, 0, errors.New("invalid login log list query")
	}
	database := r.db.WithContext(ctx).Model(&model.LoginLog{})
	if username := strings.TrimSpace(query.Username); username != "" {
		database = database.Where("username ILIKE ? ESCAPE '!'", containsLikePattern(username))
	}
	// A nil filter means all results; false must still filter failed attempts.
	if query.Success != nil {
		database = database.Where("success = ?", *query.Success)
	}
	var total int64
	if err := database.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count login logs: %w", err)
	}
	logs := make([]model.LoginLog, 0)
	if err := database.Order("id DESC").Offset(query.Offset).Limit(query.Limit).Find(&logs).Error; err != nil {
		return nil, 0, fmt.Errorf("list login logs: %w", err)
	}
	return logs, total, nil
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
