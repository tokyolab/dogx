package repository

import (
	"context"
	"testing"

	"gorm.io/gorm"
)

func TestTransactionRunnerRejectsMissingDependencies(t *testing.T) {
	if run, err := NewTransactionRunner(nil); err == nil || run != nil {
		t.Fatal("accepted nil database")
	}
	run, err := NewTransactionRunner(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), nil); err == nil {
		t.Fatal("accepted nil callback")
	}
}
