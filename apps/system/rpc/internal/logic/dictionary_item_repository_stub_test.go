package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
)

type dictionaryItemRepositoryStub struct {
	repository.DictionaryItemRepository
	item     *model.DictionaryItem
	items    []model.DictionaryItem
	err      error
	writeErr error
	writes   int
	reads    int
	ids      []int64
}

func (s *dictionaryItemRepositoryStub) FindByID(context.Context, int64) (*model.DictionaryItem, error) {
	s.reads++
	return s.item, s.err
}
func (s *dictionaryItemRepositoryStub) ListByDictionaryIDs(_ context.Context, ids []int64) ([]model.DictionaryItem, error) {
	s.ids = ids
	s.reads++
	return s.items, s.err
}
func (s *dictionaryItemRepositoryStub) Create(_ context.Context, v *model.DictionaryItem) error {
	s.writes++
	v.ID = 7
	s.item = v
	return s.writeErr
}
func (s *dictionaryItemRepositoryStub) Update(_ context.Context, _ int64, v *model.DictionaryItem) error {
	s.writes++
	s.item = v
	return s.writeErr
}
func (s *dictionaryItemRepositoryStub) UpdateStatus(context.Context, int64, model.RecordStatus) error {
	s.writes++
	return s.writeErr
}
func (s *dictionaryItemRepositoryStub) Delete(context.Context, int64) error {
	s.writes++
	return s.writeErr
}
