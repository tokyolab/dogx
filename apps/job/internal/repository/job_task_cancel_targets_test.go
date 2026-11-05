package repository

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func TestCancelTargetQueryRejectsInvalidInputBeforeDatabaseUse(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct {
		id    int64
		after string
		limit int
	}{
		{0, "", 1}, {-1, "", 1}, {1, "", 0}, {1, "", 33},
		{1, strings.Repeat("a", 256), 1}, {1, string([]byte{0xff}), 1},
	} {
		if addresses, err := repo.ListCancelTargets(context.Background(), input.id, input.after, input.limit); err == nil || addresses != nil {
			t.Fatalf("invalid cancellation address query reached database: %+v", input)
		}
	}
}

func TestCancelTargetQueryRetainsDatabaseFailureCause(t *testing.T) {
	cause := errors.New("synthetic cancellation address query failure")
	conn := &resultDriverConn{queryErr: cause}
	repo, err := NewJobTaskRepository(resultDriverDatabase(t, conn))
	if err != nil {
		t.Fatal(err)
	}
	addresses, err := repo.ListCancelTargets(context.Background(), 1, "", 32)
	if addresses != nil || !errors.Is(err, cause) || conn.queries != 1 {
		t.Fatalf("query failure lost its cause or became an empty success: addresses=%v queries=%d err=%v", addresses, conn.queries, err)
	}
}
