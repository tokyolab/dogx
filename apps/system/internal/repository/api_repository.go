package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

type APIListQuery struct {
	Keyword     string
	ServiceName string
	Group       string
}

type APIRepository interface {
	List(ctx context.Context, query APIListQuery) ([]model.API, error)
}

type apiRepository struct {
	db *gorm.DB
}

func NewAPIRepository(db *gorm.DB) (APIRepository, error) {
	if db == nil {
		return nil, errors.New("API repository database is nil")
	}
	return &apiRepository{db: db}, nil
}

func (r *apiRepository) List(ctx context.Context, query APIListQuery) ([]model.API, error) {
	if ctx == nil {
		return nil, errors.New("list APIs context is nil")
	}

	database := r.db.WithContext(ctx).Model(&model.API{})
	if serviceName := strings.TrimSpace(query.ServiceName); serviceName != "" {
		database = database.Where("service_name = ?", serviceName)
	}
	if group := strings.TrimSpace(query.Group); group != "" {
		database = database.Where("api_group = ?", group)
	}
	if keyword := strings.TrimSpace(query.Keyword); keyword != "" {
		pattern := containsLikePattern(keyword)
		database = database.Where(
			"(name ILIKE ? ESCAPE '!' OR path ILIKE ? ESCAPE '!' OR method ILIKE ? ESCAPE '!')",
			pattern,
			pattern,
			pattern,
		)
	}

	resources := make([]model.API, 0)
	if err := database.
		Order("service_name ASC, api_group ASC, id ASC").
		Find(&resources).Error; err != nil {
		return nil, fmt.Errorf("list APIs: %w", err)
	}
	return resources, nil
}
