package instancecheck

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestPostCreationUsesStableCountsAndConfirmedCommit(t *testing.T) {
	for _, counts := range []repository.ShardCounts{{}, {Total: 3, Succeeded: 2, Failed: 1}, {Total: 500, Failed: 500}} {
		for _, uncertain := range []bool{false, true} {
			parent, tasks := postCreationStubs()
			cause := errors.New("synthetic lost commit confirmation")
			committed, hints := false, 0
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			creator, err := NewPostCreator(func(got context.Context, fn func(repository.Repositories) error) error {
				if got != ctx {
					t.Fatal("transaction replaced check Context")
				}
				if err := fn(repository.Repositories{Instances: parent, Tasks: tasks}); err != nil {
					return err
				}
				if uncertain {
					return cause
				}
				committed = true
				return nil
			}, func(id int64) {
				if !committed || id != 1 {
					t.Fatal("hint preceded commit")
				}
				hints++
			})
			if err != nil {
				t.Fatal(err)
			}
			created, err := creator.Create(ctx, 1, counts)
			cancel()
			if uncertain {
				if created || !errors.Is(err, cause) || hints != 0 {
					t.Fatalf("uncertain creation announced: %v %v", created, err)
				}
			} else if err != nil || !created || hints != 1 {
				t.Fatalf("confirmed creation: %v %v", created, err)
			}
			if tasks.inserts != 1 || tasks.counts != counts || !tasks.at.Equal(parent.now) || !reflect.DeepEqual(*parent.trace, []string{"lock", "clock", "post", "insert"}) {
				t.Fatalf("creation reinterpreted stable facts: trace=%v counts=%+v", *parent.trace, tasks.counts)
			}
		}
	}
}

func TestPostCreationRechecksAuthorizationAndSingleton(t *testing.T) {
	cause := errors.New("synthetic database failure")
	for _, scenario := range []struct {
		name   string
		change func(*postInstanceStub, *postTaskStub)
		want   error
	}{
		{"missing parent", func(p *postInstanceStub, _ *postTaskStub) { p.parent = nil }, nil},
		{"stopping", func(p *postInstanceStub, _ *postTaskStub) {
			p.parent.Status = model.InstanceStopping
			p.parent.ExecutionConfig = []byte(`invalid`)
		}, nil},
		{"terminal", func(p *postInstanceStub, _ *postTaskStub) { p.parent.Status = model.InstanceFailed }, nil},
		{"unknown parent", func(p *postInstanceStub, _ *postTaskStub) { p.parent.Status = 99 }, ErrInvalidFacts},
		{"wrong identity", func(p *postInstanceStub, _ *postTaskStub) { p.parent.ID++ }, ErrInvalidFacts},
		{"deadline", func(p *postInstanceStub, _ *postTaskStub) { p.now = *p.parent.DeadlineAt }, nil},
		{"invalid time", func(p *postInstanceStub, _ *postTaskStub) { p.parent.StartedAt = nil }, ErrInvalidFacts},
		{"clock failed", func(p *postInstanceStub, _ *postTaskStub) { p.clockErr = cause }, cause},
		{"conflict", func(p *postInstanceStub, _ *postTaskStub) { p.lockErr = repository.ErrInstanceLocked }, repository.ErrInstanceLocked},
		{"existing Post", func(_ *postInstanceStub, s *postTaskStub) {
			s.post = &repository.StageTask{ID: 4, TaskType: model.TaskTypePost, Status: model.TaskRetryWaiting}
		}, nil},
		{"invalid existing Post", func(_ *postInstanceStub, s *postTaskStub) {
			s.post = &repository.StageTask{ID: 4, TaskType: model.TaskTypePost, Status: 99}
		}, ErrInvalidFacts},
		{"Post read failed", func(_ *postInstanceStub, s *postTaskStub) { s.readErr = cause }, cause},
		{"normal mode", func(p *postInstanceStub, _ *postTaskStub) {
			p.parent.ExecutionConfig = []byte(`{"schema_version":1,"execution_mode":1,"post_enabled":true}`)
		}, ErrInvalidConfig},
		{"Post disabled", func(p *postInstanceStub, _ *postTaskStub) {
			p.parent.ExecutionConfig = []byte(`{"schema_version":1,"execution_mode":2,"post_enabled":false}`)
		}, ErrInvalidConfig},
		{"Post missing", func(p *postInstanceStub, _ *postTaskStub) {
			p.parent.ExecutionConfig = []byte(`{"schema_version":1,"execution_mode":2}`)
		}, ErrInvalidConfig},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			parent, tasks := postCreationStubs()
			scenario.change(parent, tasks)
			hints := 0
			creator, _ := NewPostCreator(func(ctx context.Context, fn func(repository.Repositories) error) error {
				return fn(repository.Repositories{Instances: parent, Tasks: tasks})
			}, func(int64) { hints++ })
			if created, err := creator.Create(context.Background(), 1, repository.ShardCounts{Total: 1, Succeeded: 1}); created || !errors.Is(err, scenario.want) || tasks.inserts != 0 || hints != 0 {
				t.Fatalf("unauthorized creation: %v %v", created, err)
			}
		})
	}
}

func TestPostCreationValidatesCountsAndPreservesFailedInsert(t *testing.T) {
	if _, err := NewPostCreator(nil, nil); err == nil {
		t.Fatal("nil runner accepted")
	}
	calls := 0
	creator, _ := NewPostCreator(func(context.Context, func(repository.Repositories) error) error { calls++; return nil }, nil)
	for _, counts := range []repository.ShardCounts{{Total: -1}, {Total: 501, Succeeded: 501}, {Total: 1}, {Total: 1, Succeeded: 2}, {Total: 1, Succeeded: 1, Failed: 1}, {Total: 1, Canceled: 1}, {Total: 1, Failed: -1}} {
		if created, err := creator.Create(context.Background(), 1, counts); created || !errors.Is(err, ErrInvalidFacts) || calls != 0 {
			t.Fatalf("invalid counts reached I/O: %+v %v", counts, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := creator.Create(ctx, 1, repository.ShardCounts{}); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatal("canceled creation reached I/O")
	}
	parent, tasks := postCreationStubs()
	cause := errors.New("synthetic insert failure")
	tasks.writeErr = cause
	creator, _ = NewPostCreator(func(ctx context.Context, fn func(repository.Repositories) error) error {
		return fn(repository.Repositories{Instances: parent, Tasks: tasks})
	}, func(int64) { t.Fatal("failed insert emitted hint") })
	if created, err := creator.Create(context.Background(), 1, repository.ShardCounts{}); created || !errors.Is(err, cause) || tasks.inserts != 1 {
		t.Fatalf("failed insert released creation: %v %v", created, err)
	}
	parent, tasks = postCreationStubs()
	creator, _ = NewPostCreator(func(ctx context.Context, fn func(repository.Repositories) error) error {
		return fn(repository.Repositories{Instances: parent, Tasks: tasks})
	}, func(int64) { panic("private callback failure") })
	if created, err := creator.Create(context.Background(), 1, repository.ShardCounts{}); err != nil || !created {
		t.Fatal("hint panic changed committed creation")
	}
}

type postInstanceStub struct {
	repository.JobInstanceRepository
	parent            *repository.ExecutionInstance
	now               time.Time
	lockErr, clockErr error
	trace             *[]string
}

func (s *postInstanceStub) LockForExecution(context.Context, int64) (*repository.ExecutionInstance, error) {
	*s.trace = append(*s.trace, "lock")
	return s.parent, s.lockErr
}
func (s *postInstanceStub) DatabaseTime(context.Context) (time.Time, error) {
	*s.trace = append(*s.trace, "clock")
	return s.now, s.clockErr
}

type postTaskStub struct {
	repository.JobTaskRepository
	post              *repository.StageTask
	readErr, writeErr error
	counts            repository.ShardCounts
	at                time.Time
	inserts           int
	trace             *[]string
}

func (s *postTaskStub) FindPost(context.Context, int64) (*repository.StageTask, error) {
	*s.trace = append(*s.trace, "post")
	return s.post, s.readErr
}
func (s *postTaskStub) InsertPost(_ context.Context, _ int64, counts repository.ShardCounts, at time.Time) (int64, error) {
	*s.trace = append(*s.trace, "insert")
	s.inserts++
	s.counts, s.at = counts, at
	return 4, s.writeErr
}
func postCreationStubs() (*postInstanceStub, *postTaskStub) {
	now := time.Now().UTC()
	start, deadline := now.Add(-time.Minute), now.Add(time.Minute)
	trace := []string{}
	return &postInstanceStub{now: now, trace: &trace, parent: &repository.ExecutionInstance{ID: 1, Status: model.InstanceRunning, StartedAt: &start, DeadlineAt: &deadline, ExecutionConfig: []byte(`{"schema_version":1,"execution_mode":2,"post_enabled":true}`)}}, &postTaskStub{trace: &trace}
}
