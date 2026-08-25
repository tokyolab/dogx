package authorization

import (
	"context"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestNewTransactionQueryDBClearsAdapterTableScope(t *testing.T) {
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  "host=localhost user=test dbname=test sslmode=disable",
		PreferSimpleProtocol: true,
	}), &gorm.Config{
		DisableAutomaticPing: true,
		DryRun:               true,
	})
	if err != nil {
		t.Fatalf("create dry-run database: %v", err)
	}

	adapterDB := db.Table("casbin_rule")
	queryDB := newTransactionQueryDB(adapterDB, context.Background())
	if queryDB.Statement.ConnPool != adapterDB.Statement.ConnPool {
		t.Fatal("query database did not retain Adapter transaction connection")
	}
	var role model.Role
	statement := queryDB.
		Where("id = ? AND status = ?", int64(1), model.RecordStatusEnabled).
		First(&role).
		Statement

	if statement.Table != role.TableName() {
		t.Fatalf("query table = %q, want %q", statement.Table, role.TableName())
	}
	query := statement.SQL.String()
	if !strings.Contains(query, `FROM "sys_role"`) || strings.Contains(query, `FROM "casbin_rule"`) {
		t.Fatalf("query retained Casbin Adapter table scope: %s", query)
	}
}
