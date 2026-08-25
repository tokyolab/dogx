package authorization

import (
	"context"
	"database/sql"
	"errors"
)

type Readiness struct {
	database *sql.DB
	reloader *PolicyReloader
}

func NewReadiness(database *sql.DB, reloader *PolicyReloader) (*Readiness, error) {
	if database == nil {
		return nil, errors.New("authorization readiness database is nil")
	}
	if reloader == nil {
		return nil, errors.New("authorization policy reloader is nil")
	}
	return &Readiness{database: database, reloader: reloader}, nil
}

func (r *Readiness) Check(ctx context.Context) error {
	if ctx == nil {
		return errors.New("authorization readiness context is nil")
	}
	if !r.reloader.Ready() {
		return errors.New("initial authorization policy is not loaded")
	}
	return r.database.PingContext(ctx)
}
