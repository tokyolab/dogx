package authorization

import (
	"errors"
	"fmt"

	gormadapter "github.com/casbin/gorm-adapter/v3"
	"gorm.io/gorm"
)

func NewGormAdapter(db *gorm.DB) (*gormadapter.Adapter, error) {
	if db == nil {
		return nil, errors.New("authorization database is nil")
	}

	adapterDB := db.Session(&gorm.Session{})
	gormadapter.TurnOffAutoMigrate(adapterDB)
	adapter, err := gormadapter.NewAdapterByDB(adapterDB)
	if err != nil {
		return nil, fmt.Errorf("initialize Casbin GORM adapter: %w", err)
	}
	return adapter, nil
}
