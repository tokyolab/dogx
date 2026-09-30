package logic

import (
	"context"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
)

type securityRuntimeStub struct {
	cfg                       *system.LoginSecurityConfig
	err, reloadErr, notifyErr error
	reloads, notifications    int
}

func (s *securityRuntimeStub) Current() (*system.LoginSecurityConfig, error) {
	if s.cfg == nil {
		return &system.LoginSecurityConfig{}, s.err
	}
	return s.cfg, s.err
}
func (s *securityRuntimeStub) Reload(context.Context) error { s.reloads++; return s.reloadErr }
func (s *securityRuntimeStub) Notify(context.Context) error { s.notifications++; return s.notifyErr }

type loginFailuresStub struct {
	locked                      bool
	checkErr, failErr, clearErr error
	checks, failures, clears    int
	username                    string
}

func (s *loginFailuresStub) Locked(_ context.Context, username string) (bool, error) {
	s.checks++
	s.username = username
	return s.locked, s.checkErr
}
func (s *loginFailuresStub) Fail(_ context.Context, username string, _ *system.LoginSecurityConfig) error {
	s.failures++
	s.username = username
	return s.failErr
}
func (s *loginFailuresStub) Clear(_ context.Context, username string) error {
	s.clears++
	s.username = username
	return s.clearErr
}
