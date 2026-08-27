//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/model"
)

func TestAPIRepositoryListsFilteredResourcesInStableOrder(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	resources := []model.API{
		{
			ServiceName: "inventory-api",
			Group:       "stock",
			Name:        "Zeta",
			Path:        "/inventory/zeta",
			Method:      "POST",
			Status:      model.RecordStatusEnabled,
		},
		{
			ServiceName: "inventory-api",
			Group:       "stock",
			Name:        "Alpha % Resource",
			Path:        "/inventory/alpha",
			Method:      "POST",
			Status:      model.RecordStatusDisabled,
		},
		{
			ServiceName: "inventory-api",
			Group:       "deleted",
			Name:        "Deleted",
			Path:        "/inventory/deleted",
			Method:      "POST",
			Status:      model.RecordStatusEnabled,
		},
	}
	if err := db.Create(&resources).Error; err != nil {
		t.Fatalf("create API query fixtures: %v", err)
	}
	if err := db.Delete(&resources[2]).Error; err != nil {
		t.Fatalf("soft delete API query fixture: %v", err)
	}

	repository, err := NewAPIRepository(db)
	if err != nil {
		t.Fatalf("create API repository: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	items, err := repository.List(ctx, APIListQuery{
		ServiceName: "inventory-api",
		Group:       "stock",
	})
	if err != nil {
		t.Fatalf("list filtered APIs: %v", err)
	}
	if len(items) != 2 || items[0].ID != resources[0].ID || items[1].ID != resources[1].ID {
		t.Fatalf("unexpected API ordering or filtering: %+v", items)
	}

	items, err = repository.List(ctx, APIListQuery{
		ServiceName: "inventory-api",
		Keyword:     "%",
	})
	if err != nil {
		t.Fatalf("list API by literal wildcard: %v", err)
	}
	if len(items) != 1 || items[0].ID != resources[1].ID {
		t.Fatalf("LIKE wildcard was not escaped: %+v", items)
	}
}
