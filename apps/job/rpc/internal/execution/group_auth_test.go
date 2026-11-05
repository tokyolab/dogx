package execution

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/groupcache"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type credentialSourceStub struct {
	groups  []repository.ExecutorGroupConfig
	err     error
	lookups int
}

func (s *credentialSourceStub) LoadExecutionGroups(context.Context) ([]repository.ExecutorGroupConfig, error) {
	return s.groups, s.err
}

func (s *credentialSourceStub) FindExecutionGroupByID(context.Context, int64) (*repository.ExecutorGroupConfig, error) {
	return nil, errors.New("unexpected group ID lookup")
}

func (s *credentialSourceStub) FindExecutionGroupByName(_ context.Context, name string) (*repository.ExecutorGroupConfig, error) {
	s.lookups++
	if s.err != nil {
		return nil, s.err
	}
	for _, group := range s.groups {
		if group.Name == name {
			return &group, nil
		}
	}
	return nil, repository.ErrExecutorGroupUnavailable
}

func TestCachedGroupAuthenticationAndReceiverClassification(t *testing.T) {
	if auth, err := NewCachedGroupAuthenticator(nil); err == nil || auth != nil {
		t.Fatal("nil cache was accepted")
	}
	source := &credentialSourceStub{}
	cache, err := groupcache.New(context.Background(), source, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewCachedGroupAuthenticator(cache)
	if err != nil {
		t.Fatal(err)
	}
	group := repository.ExecutorGroupConfig{ID: 9, Name: "test-workers", DiscoveryKey: "test/workers", Token: receiverTestToken}
	source.groups = []repository.ExecutorGroupConfig{group}
	for range 2 {
		id, err := auth.Authenticate(context.Background(), group.Name, group.Token)
		if err != nil || id != group.ID {
			t.Fatal("new group was not authenticated through a cache miss")
		}
	}
	if source.lookups != 1 {
		t.Fatal("cache hit queried the source")
	}
	for _, credentials := range [][2]string{
		{"", group.Token}, {group.Name, ""}, {group.Name, "33333333333343338333333333333333"},
		{strings.ToUpper(group.Name), group.Token}, {"other", group.Token},
	} {
		if id, err := auth.Authenticate(context.Background(), credentials[0], credentials[1]); id != 0 || !errors.Is(err, ErrInvalidCredentials) {
			t.Fatal("definitely invalid credentials were not rejected")
		}
	}
	acceptor := &receiverAcceptorStub{accepted: true}
	receiver := testReceiver(t, auth, acceptor)
	request := validReceiverRequest()
	request.Auth.GroupName = "new-workers"
	cause := errors.New("private driver failure " + group.Token)
	source.err = cause
	_, err = receiver.ReportTaskResult(context.Background(), request)
	if status.Code(err) != codes.Unavailable || strings.Contains(err.Error(), group.Token) || len(acceptor.reports) != 0 {
		t.Fatal("source failure was accepted or exposed credentials")
	}
	source.err = nil
	_, err = receiver.ReportTaskResult(context.Background(), request)
	if status.Code(err) != codes.Unauthenticated || len(acceptor.reports) != 0 {
		t.Fatal("confirmed missing group was not rejected")
	}
	request.Auth.GroupName = group.Name
	if _, err = receiver.ReportTaskResult(context.Background(), request); err != nil || len(acceptor.reports) != 1 || acceptor.reports[0].GroupID != group.ID {
		t.Fatal("trusted group identity did not reach result admission")
	}
}
