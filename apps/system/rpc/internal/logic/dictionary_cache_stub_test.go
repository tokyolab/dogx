package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/dictcache"
)

type dictionaryCacheStub struct {
	entries                                  map[string]dictcache.Entry
	readErr, putErr, invalidateErr, clearErr error
	invalidated                              []string
	puts                                     int
	cleared                                  bool
}

func (s *dictionaryCacheStub) Read(context.Context, []string) (map[string]dictcache.Entry, error) {
	copy := make(map[string]dictcache.Entry)
	for k, v := range s.entries {
		copy[k] = v
	}
	return copy, s.readErr
}
func (s *dictionaryCacheStub) Put(_ context.Context, _ string, _ dictcache.Entry) error {
	s.puts++
	return s.putErr
}
func (s *dictionaryCacheStub) Invalidate(_ context.Context, code string) error {
	s.invalidated = append(s.invalidated, code)
	return s.invalidateErr
}
func (s *dictionaryCacheStub) Clear(context.Context) error { s.cleared = true; return s.clearErr }
