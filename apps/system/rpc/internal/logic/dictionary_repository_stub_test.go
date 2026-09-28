package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
)

type dictionaryRepositoryStub struct {
	repository.DictionaryRepository
	item     *model.Dictionary
	items    []model.Dictionary
	err      error
	writeErr error
	writes   int
	reads    int
	codes    []string
}

func (s *dictionaryRepositoryStub) FindByID(context.Context, int64) (*model.Dictionary, error) {
	s.reads++
	return s.item, s.err
}
func (s *dictionaryRepositoryStub) FindByCodes(_ context.Context, codes []string) ([]model.Dictionary, error) {
	s.codes = codes
	s.reads++
	return s.items, s.err
}
func (s *dictionaryRepositoryStub) List(context.Context) ([]model.Dictionary, error) {
	s.reads++
	return s.items, s.err
}
func (s *dictionaryRepositoryStub) Create(_ context.Context, v *model.Dictionary) error {
	s.writes++
	v.ID = 7
	s.item = v
	return s.writeErr
}
func (s *dictionaryRepositoryStub) Update(_ context.Context, _ int64, v *model.Dictionary) error {
	s.writes++
	s.item = v
	return s.writeErr
}
func (s *dictionaryRepositoryStub) UpdateStatus(context.Context, int64, model.RecordStatus) error {
	s.writes++
	return s.writeErr
}
func (s *dictionaryRepositoryStub) Delete(context.Context, int64) error {
	s.writes++
	return s.writeErr
}
