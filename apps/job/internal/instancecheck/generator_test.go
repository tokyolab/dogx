package instancecheck

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestGeneratePreservesCompleteInputsAndCommitBoundary(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		parent, tasks := generationStubs()
		cause := errors.New("synthetic lost commit confirmation")
		committed, hints := false, 0
		generator, err := NewGenerator(func(ctx context.Context, fn func(repository.Repositories) error) error {
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
				t.Fatal("dispatch hint preceded confirmed commit")
			}
			hints++
		})
		if err != nil {
			t.Fatal(err)
		}
		prepared := repository.PreparationResult{ID: 2, Status: model.TaskSucceeded,
			Data: model.TaskData(`[null,9007199254740993,{"ids":[1,2]},"opaque",[true,false]]`)}
		created, err := generator.Generate(context.Background(), 1, prepared)
		if uncertain {
			if created || !errors.Is(err, cause) || hints != 0 {
				t.Fatalf("uncertain commit authorized work: %v %v hints=%d", created, err, hints)
			}
		} else if err != nil || !created || hints != 1 {
			t.Fatalf("confirmed generation: %v %v hints=%d", created, err, hints)
		}
		var got []string
		for _, item := range tasks.items {
			got = append(got, string(item))
		}
		want := []string{`null`, `9007199254740993`, `{"ids":[1,2]}`, `"opaque"`, `[true,false]`}
		if !reflect.DeepEqual(got, want) || !tasks.createdAt.Equal(parent.now) || tasks.inserts != 1 || parent.clocks != 1 {
			t.Fatalf("generation changed input or registration: items=%v tasks=%+v", got, tasks)
		}
	}
}

func TestGenerateRechecksParentAndPreparationBeforeWriting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*generationInstanceStub, *generationTaskStub)
		want   error
	}{
		{"stopping", func(p *generationInstanceStub, _ *generationTaskStub) {
			p.parent.Status = model.InstanceStopping
			p.parent.ExecutionConfig = []byte(`invalid`)
		}, nil},
		{"finished", func(p *generationInstanceStub, _ *generationTaskStub) { p.parent.Status = model.InstanceSucceeded }, nil},
		{"invalid_parent", func(p *generationInstanceStub, _ *generationTaskStub) { p.parent.Status = 99 }, ErrInvalidFacts},
		{"missing", func(p *generationInstanceStub, _ *generationTaskStub) { p.parent = nil }, nil},
		{"deadline", func(p *generationInstanceStub, _ *generationTaskStub) { p.now = *p.parent.DeadlineAt }, nil},
		{"already_generated", func(_ *generationInstanceStub, s *generationTaskStub) { s.exists = true }, nil},
		{"lock_conflict", func(p *generationInstanceStub, _ *generationTaskStub) { p.lockErr = repository.ErrInstanceLocked }, repository.ErrInstanceLocked},
		{"missing_pre", func(_ *generationInstanceStub, s *generationTaskStub) { s.prepared = nil }, ErrInvalidFacts},
		{"different_pre", func(_ *generationInstanceStub, s *generationTaskStub) { s.prepared.ID++ }, ErrInvalidFacts},
		{"uncommitted_pre", func(_ *generationInstanceStub, s *generationTaskStub) { s.prepared.Status = model.TaskRunning }, ErrInvalidFacts},
		{"ordinary", func(p *generationInstanceStub, _ *generationTaskStub) {
			p.parent.ExecutionConfig = []byte(`{"schema_version":1,"execution_mode":1,"post_enabled":false}`)
		}, ErrInvalidConfig},
		{"missing_post", func(p *generationInstanceStub, _ *generationTaskStub) {
			p.parent.ExecutionConfig = []byte(`{"schema_version":1,"execution_mode":2}`)
		}, ErrInvalidConfig},
		{"invalid_times", func(p *generationInstanceStub, _ *generationTaskStub) { p.parent.StartedAt = nil }, ErrInvalidFacts},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, tasks := generationStubs()
			tc.change(parent, tasks)
			calls, hints := 0, 0
			generator, _ := NewGenerator(func(ctx context.Context, fn func(repository.Repositories) error) error {
				calls++
				return fn(repository.Repositories{Instances: parent, Tasks: tasks})
			}, func(int64) { hints++ })
			created, err := generator.Generate(context.Background(), 1, repository.PreparationResult{ID: 2, Status: model.TaskSucceeded, Data: model.TaskData(`[1]`)})
			if created || !errors.Is(err, tc.want) || tasks.inserts != 0 || hints != 0 || calls != 1 {
				t.Fatalf("unexpected generation: %v %v writes=%d hints=%d calls=%d", created, err, tasks.inserts, hints, calls)
			}
		})
	}
}

func TestGenerateRejectsInvalidSavedListsAndLeavesEmptyForCompletion(t *testing.T) {
	calls := 0
	generator, _ := NewGenerator(func(context.Context, func(repository.Repositories) error) error { calls++; return nil }, nil)
	for _, input := range []string{"", `null`, `{}`, `[`, `"value"`, "[" + strings.Repeat("1,", 500) + "1]"} {
		if created, err := generator.Generate(context.Background(), 1, repository.PreparationResult{ID: 2, Status: model.TaskSucceeded, Data: model.TaskData(input)}); created || !errors.Is(err, ErrInvalidFacts) {
			t.Fatalf("invalid list was accepted: %v %v", created, err)
		}
	}
	if created, err := generator.Generate(context.Background(), 1, repository.PreparationResult{ID: 2, Status: model.TaskSucceeded, Data: model.TaskData(`[]`)}); created || err != nil {
		t.Fatalf("empty list should require no generation: %v %v", created, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := generator.Generate(ctx, 1, repository.PreparationResult{}); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("canceled or invalid work reached transaction: %v calls=%d", err, calls)
	}
	if _, err := NewGenerator(nil, nil); err == nil {
		t.Fatal("nil transaction runner accepted")
	}
}

// generationInstanceStub exposes only the parent lock and database clock used
// by generation. Clock access without the preceding lock fails the test contract.
type generationInstanceStub struct {
	repository.JobInstanceRepository
	parent  *repository.ExecutionInstance
	now     time.Time
	lockErr error
	locked  bool
	clocks  int
}

func (s *generationInstanceStub) LockForExecution(context.Context, int64) (*repository.ExecutionInstance, error) {
	s.locked = s.lockErr == nil
	return s.parent, s.lockErr
}

func (s *generationInstanceStub) DatabaseTime(context.Context) (time.Time, error) {
	if !s.locked {
		return time.Time{}, errors.New("clock read before parent lock")
	}
	s.clocks++
	return s.now, nil
}

type generationTaskStub struct {
	repository.JobTaskRepository
	prepared  *repository.PreparationTask
	exists    bool
	items     []model.TaskData
	createdAt time.Time
	inserts   int
}

func (s *generationTaskStub) FindPreparation(context.Context, int64) (*repository.PreparationTask, error) {
	return s.prepared, nil
}
func (s *generationTaskStub) HasShards(context.Context, int64) (bool, error) { return s.exists, nil }
func (s *generationTaskStub) InsertShards(_ context.Context, _ int64, items []model.TaskData, at time.Time) error {
	s.items, s.createdAt = items, at
	s.inserts++
	return nil
}

func generationStubs() (*generationInstanceStub, *generationTaskStub) {
	now := time.Now().UTC()
	start, deadline := now.Add(-time.Minute), now.Add(time.Minute)
	return &generationInstanceStub{now: now, parent: &repository.ExecutionInstance{ID: 1, Status: model.InstanceRunning,
			StartedAt: &start, DeadlineAt: &deadline, ExecutionConfig: []byte(`{"schema_version":1,"execution_mode":2,"post_enabled":false}`)}},
		&generationTaskStub{prepared: &repository.PreparationTask{ID: 2, Status: model.TaskSucceeded}}
}
