package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// Repositories is scoped to a single transaction. Callers must not retain these
// handles or start asynchronous work with them after the callback returns.
type Repositories struct {
	Jobs      JobDefinitionRepository
	Groups    ExecutorGroupRepository
	Instances JobInstanceRepository
}

// TransactionRunner returns nil only after commit succeeds. A successful
// callback alone does not authorize publishing results or notifications.
type TransactionRunner func(context.Context, func(Repositories) error) error

func NewTransactionRunner(db *gorm.DB) (TransactionRunner, error) {
	if db == nil {
		return nil, errors.New("job transaction database is nil")
	}
	return func(ctx context.Context, fn func(Repositories) error) error {
		if fn == nil {
			return errors.New("job transaction callback is nil")
		}
		// Retain PostgreSQL's default READ COMMITTED and bind every repository
		// to the same transaction; business decisions stay with the caller.
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			jobs, err := NewJobDefinitionRepository(tx)
			if err != nil {
				return err
			}
			groups, err := NewExecutorGroupRepository(tx)
			if err != nil {
				return err
			}
			instances, err := NewJobInstanceRepository(tx)
			if err != nil {
				return err
			}
			return fn(Repositories{Jobs: jobs, Groups: groups, Instances: instances})
		})
		if err != nil {
			return fmt.Errorf("job transaction: %w", err)
		}
		return nil
	}, nil
}
