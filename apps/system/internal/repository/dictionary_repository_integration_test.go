//go:build integration

package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
)

func TestDictionaryRepositoryLifecycleAndConstraints(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	d, err := NewDictionaryRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	i, err := NewDictionaryItemRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dictionary := &model.Dictionary{Name: "来源", Code: "source", Status: 1}
	if err := d.Create(ctx, dictionary); err != nil {
		t.Fatal(err)
	}
	if err := d.Create(ctx, &model.Dictionary{Name: "重复", Code: "source"}); !errors.Is(err, ErrDictionaryCodeExists) {
		t.Fatal(err)
	}
	if err := d.Create(ctx, &model.Dictionary{Name: "非法", Code: "INVALID"}); err == nil {
		t.Fatal("database code constraint missing")
	}
	item := &model.DictionaryItem{DictionaryID: dictionary.ID, Label: "官网", Value: "web", Status: 1}
	if err := i.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := i.Create(ctx, &model.DictionaryItem{DictionaryID: dictionary.ID, Label: "重复", Value: "web"}); !errors.Is(err, ErrDictionaryValueExists) {
		t.Fatal(err)
	}
	other := &model.Dictionary{Name: "渠道", Code: "channel", Status: 1}
	if err := d.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	otherItem := &model.DictionaryItem{DictionaryID: other.ID, Label: "其他官网", Value: "web", Status: 1}
	if err := i.Create(ctx, otherItem); err != nil {
		t.Fatal("same value in another dictionary", err)
	}
	if err := d.Delete(ctx, dictionary.ID); !errors.Is(err, ErrDictionaryHasItems) {
		t.Fatal(err)
	}
	if err := d.Update(ctx, dictionary.ID, &model.Dictionary{Name: "新名称", Code: "changed", IsPublic: true}); err != nil {
		t.Fatal(err)
	}
	if err := i.Update(ctx, item.ID, &model.DictionaryItem{DictionaryID: other.ID, Label: "新标签", Value: "changed", Sort: 3, Remark: ""}); err != nil {
		t.Fatal(err)
	}
	current, err := d.FindByID(ctx, dictionary.ID)
	if err != nil || current.Code != "source" || current.Name != "新名称" || !current.IsPublic {
		t.Fatal(current, err)
	}
	currentItem, err := i.FindByID(ctx, item.ID)
	if err != nil || currentItem.DictionaryID != dictionary.ID || currentItem.Value != "web" || currentItem.Label != "新标签" || currentItem.Sort != 3 {
		t.Fatal(currentItem, err)
	}
	if err := d.UpdateStatus(ctx, dictionary.ID, 0); err != nil {
		t.Fatal(err)
	}
	if err := i.UpdateStatus(ctx, item.ID, 0); err != nil {
		t.Fatal(err)
	}
	dictionaries, err := d.FindByCodes(ctx, []string{"source", "channel", "missing"})
	if err != nil || len(dictionaries) != 2 || dictionaries[0].Status != 0 {
		t.Fatal(dictionaries, err)
	}
	items, err := i.ListByDictionaryIDs(ctx, []int64{dictionary.ID, other.ID})
	if err != nil || len(items) != 2 || items[1].Status != 0 {
		t.Fatal(items, err)
	}
	if result, err := d.List(ctx); err != nil || len(result) != 2 {
		t.Fatal(result, err)
	}
	if result, err := d.FindByCodes(ctx, nil); err != nil || len(result) != 0 {
		t.Fatal(result, err)
	}
	if result, err := i.ListByDictionaryIDs(ctx, nil); err != nil || len(result) != 0 {
		t.Fatal(result, err)
	}
	if err := i.Delete(ctx, item.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := i.FindByID(ctx, item.ID); !errors.Is(err, ErrDictionaryItemNotFound) {
		t.Fatal(err)
	}
	if err := d.Delete(ctx, dictionary.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.FindByID(ctx, dictionary.ID); !errors.Is(err, ErrDictionaryNotFound) {
		t.Fatal(err)
	}
	if err := d.Create(ctx, &model.Dictionary{Name: "重建", Code: "source", Status: 1}); err != nil {
		t.Fatal(err)
	}
	for _, call := range []func() error{
		func() error { return d.Update(ctx, -1, &model.Dictionary{Name: "x"}) },
		func() error { return d.UpdateStatus(ctx, -1, 1) },
		func() error { return d.Delete(ctx, -1) },
	} {
		if err := call(); !errors.Is(err, ErrDictionaryNotFound) {
			t.Fatal(err)
		}
	}
	for _, call := range []func() error{
		func() error { return i.Update(ctx, -1, &model.DictionaryItem{Label: "x"}) },
		func() error { return i.UpdateStatus(ctx, -1, 1) },
		func() error { return i.Delete(ctx, -1) },
	} {
		if err := call(); !errors.Is(err, ErrDictionaryItemNotFound) {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := d.FindByID(canceled, other.ID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := i.FindByID(canceled, otherItem.ID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := d.Delete(canceled, other.ID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := d.Update(canceled, other.ID, &model.Dictionary{Name: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := i.Update(canceled, otherItem.ID, &model.DictionaryItem{Label: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
