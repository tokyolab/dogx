package repository

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
)

func TestInstanceHistoryRejectsInvalidFiltersBeforeDatabaseAccess(t *testing.T) {
	repo, err := NewJobInstanceRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	zeroID, negativeID := int64(0), int64(-1)
	zeroStatus, invalidStatus := model.InstanceStatus(0), model.InstanceStatus(7)
	zeroTrigger, invalidTrigger := model.TriggerType(0), model.TriggerType(4)
	now, zeroTime := time.Now(), time.Time{}
	for _, f := range []InstanceFilter{
		{}, {Page: 1, PageSize: 201}, {Page: math.MaxInt64, PageSize: 2},
		{Page: 1, PageSize: 20, ID: &zeroID}, {Page: 1, PageSize: 20, JobID: &negativeID},
		{Page: 1, PageSize: 20, ExecutorGroupID: &zeroID},
		{Page: 1, PageSize: 20, Status: &zeroStatus}, {Page: 1, PageSize: 20, Status: &invalidStatus},
		{Page: 1, PageSize: 20, TriggerType: &zeroTrigger}, {Page: 1, PageSize: 20, TriggerType: &invalidTrigger},
		{Page: 1, PageSize: 20, CreatedFrom: &zeroTime}, {Page: 1, PageSize: 20, CreatedTo: &zeroTime},
		{Page: 1, PageSize: 20, CreatedFrom: &now, CreatedTo: &now},
	} {
		if items, total, err := repo.List(context.Background(), f); err == nil || items != nil || total != 0 {
			t.Fatalf("invalid history filter reached the database: filter=%+v err=%v", f, err)
		}
	}
}

func TestTaskDetailRejectsInvalidSelectionBeforeDatabaseAccess(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []struct {
		id   int64
		kind TaskDataSelection
	}{{0, TaskMetadataOnly}, {-1, TaskInputData}, {1, "both"}, {1, "INPUT"}, {1, "input_data"}} {
		if item, err := repo.FindDetail(context.Background(), query.id, query.kind); err == nil || item != nil {
			t.Fatalf("invalid task detail reached the database: query=%+v err=%v", query, err)
		}
	}
}
