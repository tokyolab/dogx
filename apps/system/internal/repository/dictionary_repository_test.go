package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestDictionaryRepositoryConstructionAndErrors(t *testing.T) {
	if _, err := NewDictionaryRepository(nil); err == nil {
		t.Fatal("nil database")
	}
	if _, err := NewDictionaryItemRepository(nil); err == nil {
		t.Fatal("nil database")
	}
	d := &dictionaryRepository{}
	i := &dictionaryItemRepository{}
	ctx := context.Background()
	if d.Create(ctx, nil) == nil || d.Update(ctx, 1, nil) == nil || i.Create(ctx, nil) == nil || i.Update(ctx, 1, nil) == nil {
		t.Fatal("nil model accepted")
	}
	sentinel := errors.New("database down")
	for _, mapper := range []func(error) error{mapDictionaryWriteError, mapDictionaryItemWriteError} {
		if mapper(nil) != nil || !errors.Is(mapper(sentinel), sentinel) {
			t.Fatal("error chain lost")
		}
	}
	if !errors.Is(mapDictionaryWriteError(&pgconn.PgError{Code: "23505", ConstraintName: "uk_sys_dictionary_code_active"}), ErrDictionaryCodeExists) {
		t.Fatal("code collision mapping")
	}
	if !errors.Is(mapDictionaryItemWriteError(&pgconn.PgError{Code: "23505", ConstraintName: "uk_sys_dictionary_item_value_active"}), ErrDictionaryValueExists) {
		t.Fatal("value collision mapping")
	}
}
