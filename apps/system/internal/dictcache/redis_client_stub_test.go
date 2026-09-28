package dictcache

import "context"

type redisClientStub struct {
	values                    []string
	readErr, evalErr, scanErr error
	evals                     int
	scanned                   int
	keys                      []string
	script                    string
	args                      []any
	pages                     [][]string
}

func (s *redisClientStub) MgetCtx(context.Context, ...string) ([]string, error) {
	return s.values, s.readErr
}
func (s *redisClientStub) EvalCtx(_ context.Context, script string, keys []string, args ...any) (any, error) {
	s.evals++
	s.script = script
	s.keys = keys
	s.args = args
	return int64(1), s.evalErr
}
func (s *redisClientStub) SscanCtx(_ context.Context, _ string, cursor uint64, _ string, _ int64) ([]string, uint64, error) {
	s.scanned++
	if s.scanErr != nil {
		return nil, 0, s.scanErr
	}
	if int(cursor) >= len(s.pages) {
		return nil, 0, nil
	}
	next := cursor + 1
	if int(next) >= len(s.pages) {
		next = 0
	}
	return s.pages[cursor], next, nil
}
