package authorization

import (
	"errors"
	"fmt"

	gormadapter "github.com/casbin/gorm-adapter/v3"
	"gorm.io/gorm"
)

// Seven bound values per rule keep a 5,000-row insert below PostgreSQL's
// 65,535-parameter limit, including policies added as required APIs.
const policyInsertBatchSize = 5000

func NewGormAdapter(db *gorm.DB) (*gormadapter.Adapter, error) {
	if db == nil {
		return nil, errors.New("authorization database is nil")
	}

	// TurnOffAutoMigrate mutates the supplied *gorm.DB in place. Clone the
	// handle to isolate that adapter-specific context while retaining the
	// current connection pool, including an active transaction when present.
	// Scope batching to Casbin. GORM retains the supplied transaction across
	// every batch, so a later insert failure cannot commit earlier batches.
	adapterDB := db.Session(&gorm.Session{CreateBatchSize: policyInsertBatchSize})
	// Goose exclusively owns the casbin_rule schema and its migrations.
	gormadapter.TurnOffAutoMigrate(adapterDB)
	adapter, err := gormadapter.NewAdapterByDB(adapterDB)
	if err != nil {
		return nil, fmt.Errorf("initialize Casbin GORM adapter: %w", err)
	}
	return adapter, nil
}
