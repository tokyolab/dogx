package migration

import (
	"database/sql"
	"embed"
	"io/fs"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func NewProvider(db *sql.DB) (*goose.Provider, error) {
	migrations, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}
	// Both domains can share a database, but must never share a migration ledger.
	return goose.NewProvider(goose.DialectPostgres, db, migrations, goose.WithTableName("job_goose_db_version"))
}
