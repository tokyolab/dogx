//go:build integration

package svc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/execution"
)

func TestServiceGroupCacheLoadsBeforeReturnAndFindsNewGroups(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	initial := model.ExecutorGroup{
		Name: "initial", DiscoveryKey: "/test/executors/initial",
		Token: strings.ReplaceAll(uuid.NewString(), "-", ""),
	}
	if err := db.Create(&initial).Error; err != nil {
		t.Fatal(err)
	}
	c, applicationName := jobServiceConfig(t, sqlDB)
	s, err := NewServiceContext(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close Job service: %v", err)
		}
	})
	auth, err := execution.NewCachedGroupAuthenticator(s.GroupCache)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if id, err := auth.Authenticate(ctx, initial.Name, initial.Token); err != nil || id != initial.ID {
		t.Fatalf("authenticate initial snapshot: id=%d error=%v", id, err)
	}

	// This node has not refreshed since startup. A newly committed group must
	// authenticate immediately through a cache miss, even with etcd offline.
	added := model.ExecutorGroup{
		Name: "added", DiscoveryKey: "/test/executors/added",
		Token: strings.ReplaceAll(uuid.NewString(), "-", ""),
	}
	if err := db.Create(&added).Error; err != nil {
		t.Fatal(err)
	}
	if id, err := auth.Authenticate(ctx, added.Name, added.Token); err != nil || id != added.ID {
		t.Fatalf("authenticate newly committed group: id=%d error=%v", id, err)
	}
	if err := db.Delete(&added).Error; err != nil {
		t.Fatal(err)
	}
	if id, err := auth.Authenticate(ctx, added.Name, added.Token); err != nil || id != added.ID {
		t.Fatalf("last snapshot should remain valid until refresh: id=%d error=%v", id, err)
	}
	if err := s.GroupCache.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if id, err := auth.Authenticate(ctx, added.Name, added.Token); id != 0 || !errors.Is(err, execution.ErrInvalidCredentials) {
		t.Fatalf("authenticate deleted group after refresh: id=%d error=%v", id, err)
	}
	if id, err := auth.Authenticate(ctx, initial.Name, initial.Token); err != nil || id != initial.ID {
		t.Fatalf("refresh discarded active group: id=%d error=%v", id, err)
	}
	s.Stop()
	if err := s.Wait(ctx); err != nil {
		t.Fatalf("wait for scheduler and group refresh: %v", err)
	}
	// Wait joins background users but leaves dependency release to the host.
	if err := s.sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("Wait closed database before host cleanup: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertNoServiceConnections(t, sqlDB, applicationName)
}

func TestServiceRejectsInvalidGroupSnapshotAndClosesConnections(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	invalidToken := "invalid-stored-credential"
	group := model.ExecutorGroup{Name: "invalid", DiscoveryKey: "/test/invalid", Token: invalidToken}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	c, applicationName := jobServiceConfig(t, sqlDB)
	s, err := NewServiceContext(c)
	if s != nil {
		_ = s.Close()
		t.Fatal("invalid initial snapshot returned a usable service")
	}
	if err == nil || !strings.Contains(err.Error(), "initialize executor group cache") {
		t.Fatalf("initial snapshot error=%v", err)
	}
	if strings.Contains(err.Error(), invalidToken) {
		t.Fatal("initialization error exposed the stored credential")
	}
	assertNoServiceConnections(t, sqlDB, applicationName)
}
