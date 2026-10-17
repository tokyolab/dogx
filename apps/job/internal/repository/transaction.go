package repository

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// Repositories shares one transaction, so each repository can read writes made
// by the others before commit. The handles are valid only during the
// TransactionRunner callback. Callers must not retain them or start asynchronous
// database operations that can outlive the callback.
type Repositories struct {
	Jobs      JobDefinitionRepository
	Groups    ExecutorGroupRepository
	Instances JobInstanceRepository
}

// TransactionRunner runs a callback in one transaction. It attempts rollback if
// the callback returns an error, or commit if the callback returns nil. It does
// not retry the callback. Only a nil result from the runner confirms commit;
// a successful callback or repository call does not authorize publishing results
// or sending post-commit notifications.
//
// The callback must be non-nil and short-lived, without RPC, Redis, or other
// external I/O. A panic triggers an attempted rollback and propagates unchanged.
// The context governs database operations; canceling it does not cancel business
// execution. A commit error, such as a lost connection, does not always establish
// that the transaction rolled back. Changes to Go objects are not undone by
// rollback and must not be treated as evidence of success after an error.
type TransactionRunner func(context.Context, func(Repositories) error) error

// NewTransactionRunner creates a runner without taking ownership of or closing db.
// The handle must be non-nil and must not already be in a transaction: GORM nested
// transactions cannot provide TransactionRunner's commit-confirmation contract.
// Construction does not check database connectivity.
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
