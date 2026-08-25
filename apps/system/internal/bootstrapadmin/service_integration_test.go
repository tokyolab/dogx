//go:build integration

package bootstrapadmin

import (
	"context"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
)

func TestCreateInitialAdministratorAssignsSeededSuperAdminRoleAtomically(t *testing.T) {
	db, sqlDB := testutil.OpenPostgres(t)
	provider, err := migration.NewProvider(sqlDB)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	user, err := CreateInitialAdministrator(ctx, db, &passwordHasherStub{hash: "encoded-hash"}, Input{
		Username: "admin",
		Password: "secure-password",
		Nickname: "Administrator",
	})
	if err != nil {
		t.Fatalf("create initial administrator: %v", err)
	}
	var assignmentCount int64
	if err := db.Model(&model.UserRole{}).
		Joins("JOIN sys_role ON sys_role.id = sys_user_role.role_id").
		Where("sys_user_role.user_id = ? AND sys_role.code = ?", user.ID, InitialRoleCode).
		Count(&assignmentCount).Error; err != nil {
		t.Fatalf("query initial role assignment: %v", err)
	}
	if assignmentCount != 1 {
		t.Fatalf("unexpected initial role assignment count: %d", assignmentCount)
	}
}
