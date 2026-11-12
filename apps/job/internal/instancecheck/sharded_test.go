package instancecheck

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func TestCheckShardedQueriesOnlyCurrentStageFacts(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		change func(*shardedFixture)
		want   Outcome
		calls  []string
		writes int
	}{
		{"pre running", func(f *shardedFixture) { f.tasks.stages.Preparation.Status = model.TaskRunning }, OutcomeNotReady, []string{"stages"}, 0},
		{"pre retry", func(f *shardedFixture) { f.tasks.stages.Preparation.Status = model.TaskRetryWaiting }, OutcomeNotReady, []string{"stages"}, 0},
		{"pre failed", func(f *shardedFixture) { f.tasks.stages.Preparation.Status = model.TaskFailed }, OutcomeTerminal, []string{"stages"}, 1},
		{"post waiting", func(f *shardedFixture) { f.enablePost(model.TaskWaiting) }, OutcomeNotReady, []string{"stages"}, 0},
		{"post retry", func(f *shardedFixture) { f.enablePost(model.TaskRetryWaiting) }, OutcomeNotReady, []string{"stages"}, 0},
		{"unfinished shard", func(f *shardedFixture) { f.tasks.unfinished = true }, OutcomeNotReady, []string{"stages", "exists", "unfinished"}, 0},
		{"finished shards", func(*shardedFixture) {}, OutcomeTerminal, []string{"stages", "exists", "unfinished", "counts"}, 1},
		{"post finished", func(f *shardedFixture) { f.enablePost(model.TaskSucceeded) }, OutcomeTerminal, []string{"stages", "exists", "unfinished", "counts"}, 1},
		{"generate shards", func(f *shardedFixture) { f.tasks.exists = false }, OutcomeAdvanced, []string{"stages", "exists", "preparation"}, 0},
		{"empty preparation", func(f *shardedFixture) { f.tasks.exists = false; f.tasks.prepared.Data = model.TaskData(`[]`) }, OutcomeTerminal, []string{"stages", "exists", "preparation"}, 1},
		{"create Post", func(f *shardedFixture) { f.enablePost(0) }, OutcomeAdvanced, []string{"stages", "exists", "unfinished", "counts"}, 0},
		{"empty preparation with Post", func(f *shardedFixture) {
			f.enablePost(0)
			f.tasks.exists = false
			f.tasks.prepared.Data = model.TaskData(`[]`)
		}, OutcomeAdvanced, []string{"stages", "exists", "preparation"}, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newShardedFixture(t)
			scenario.change(f)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := f.checker.Check(ctx, 1)
			if err != nil || got != scenario.want || f.parents.writes != scenario.writes || !reflect.DeepEqual(f.tasks.calls, scenario.calls) {
				t.Fatalf("outcome=%d err=%v writes=%d queries=%v", got, err, f.parents.writes, f.tasks.calls)
			}
			if f.parents.writes+f.generator.calls+f.post.calls > 1 {
				t.Fatal("one check performed more than one stage action")
			}
			for _, seen := range f.tasks.contexts {
				if seen != ctx {
					t.Fatal("stage query replaced the shared Context")
				}
			}
			if f.generator.calls > 0 && f.generator.ctx != ctx || f.post.calls > 0 && f.post.ctx != ctx {
				t.Fatal("stage operation replaced the shared Context")
			}
		})
	}
}

func TestCheckShardedFinalOutcomesAndSummaries(t *testing.T) {
	for _, failed := range []int64{0, 1, 3} {
		for _, postStatus := range []model.TaskStatus{0, model.TaskSucceeded, model.TaskFailed} {
			f := newShardedFixture(t)
			f.tasks.counts = repository.ShardCounts{Total: 3, Succeeded: 3 - failed, Failed: failed}
			if postStatus != 0 {
				f.enablePost(postStatus)
			}
			past := f.parents.now.Add(-time.Hour)
			f.parents.facts.DeadlineAt = &past
			want := model.InstanceSucceeded
			if failed > 0 || postStatus == model.TaskFailed {
				want = model.InstanceFailed
			}
			got, err := f.checker.Check(context.Background(), 1)
			if err != nil || got != OutcomeTerminal || f.parents.target != want || f.parents.clocks != 0 ||
				f.parents.summary == nil || !strings.Contains(*f.parents.summary, "total=3") || f.generator.calls+f.post.calls != 0 {
				t.Fatalf("complete shards failed=%d post=%d: outcome=%d err=%v target=%d", failed, postStatus, got, err, f.parents.target)
			}
		}
	}
	for _, summary := range []*string{nil, checkPointer(""), checkPointer("preparation failed")} {
		f := newShardedFixture(t)
		f.tasks.stages.Preparation.Status, f.tasks.stages.Preparation.ResultSummary = model.TaskFailed, summary
		f.enablePost(0)
		if got, err := f.checker.Check(context.Background(), 1); err != nil || got != OutcomeTerminal || f.parents.target != model.InstanceFailed || f.parents.summary != summary || f.post.calls != 0 {
			t.Fatalf("failed preparation summary changed: outcome=%d err=%v", got, err)
		}
	}
	f := newShardedFixture(t)
	f.enablePost(0)
	f.tasks.counts = repository.ShardCounts{Total: 3, Failed: 3}
	if got, err := f.checker.Check(context.Background(), 1); err != nil || got != OutcomeAdvanced || f.post.counts != f.tasks.counts || f.parents.writes != 0 {
		t.Fatalf("all failed shards skipped required Post: outcome=%d err=%v", got, err)
	}
}

func TestCheckShardedRejectsInvalidFactsWithoutTimeout(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		change func(*shardedFixture)
	}{
		{"nil stages", func(f *shardedFixture) { f.tasks.stages = nil }},
		{"unknown pre", func(f *shardedFixture) { f.tasks.stages.Preparation.Status = 99 }},
		{"wrong pre type", func(f *shardedFixture) { f.tasks.stages.Preparation.TaskType = model.TaskTypeOrdinary }},
		{"unknown Post", func(f *shardedFixture) { f.enablePost(99) }},
		{"unconfigured Post", func(f *shardedFixture) {
			f.tasks.stages.Post = &repository.StageTask{ID: 4, TaskType: model.TaskTypePost, Status: model.TaskSucceeded}
		}},
		{"missing preparation data", func(f *shardedFixture) { f.tasks.exists = false; f.tasks.prepared = nil }},
		{"different preparation", func(f *shardedFixture) { f.tasks.exists = false; f.tasks.prepared.ID++ }},
		{"null preparation", func(f *shardedFixture) { f.tasks.exists = false; f.tasks.prepared.Data = model.TaskData(`null`) }},
		{"unknown shard", func(f *shardedFixture) { f.tasks.counts = repository.ShardCounts{Total: 3, Succeeded: 1, Failed: 1} }},
		{"vanished shard", func(f *shardedFixture) { f.tasks.counts = repository.ShardCounts{} }},
		{"oversized count", func(f *shardedFixture) { f.tasks.counts = repository.ShardCounts{Total: 501, Succeeded: 501} }},
		{"Post without nonempty shards", func(f *shardedFixture) { f.enablePost(model.TaskSucceeded); f.tasks.exists = false }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newShardedFixture(t)
			scenario.change(f)
			f.parents.facts.DeadlineAt = &f.parents.now
			got, err := f.checker.Check(context.Background(), 1)
			if !errors.Is(err, ErrInvalidFacts) || got == OutcomeNotReady || f.timeout.calls != 0 || f.parents.writes+f.generator.calls+f.post.calls != 0 {
				t.Fatalf("invalid facts became timeout: outcome=%d err=%v", got, err)
			}
		})
	}
}

func TestCheckShardedCanceledAndMissingStagesMayTimeOut(t *testing.T) {
	for _, change := range []func(*shardedFixture){
		func(f *shardedFixture) { f.tasks.stages.Preparation = nil },
		func(f *shardedFixture) { f.tasks.stages.Preparation.Status = model.TaskCanceled },
		func(f *shardedFixture) { f.enablePost(model.TaskCanceled) },
		func(f *shardedFixture) { f.tasks.counts = repository.ShardCounts{Total: 3, Succeeded: 1, Canceled: 2} },
	} {
		f := newShardedFixture(t)
		change(f)
		f.parents.facts.DeadlineAt = &f.parents.now
		if got, err := f.checker.Check(context.Background(), 1); err != nil || got != OutcomeStopping || f.timeout.calls != 1 || f.parents.writes+f.generator.calls+f.post.calls != 0 {
			t.Fatalf("anomalous incomplete facts did not use timeout: outcome=%d err=%v", got, err)
		}
	}
}

func TestCheckShardedStageActionsNeverFallThroughToTimeout(t *testing.T) {
	cause := errors.New("synthetic uncertain stage outcome")
	for _, post := range []bool{false, true} {
		for _, failure := range []bool{false, true} {
			f := newShardedFixture(t)
			if post {
				f.enablePost(0)
				f.post.created = false
				if failure {
					f.post.err = cause
				}
			} else {
				f.tasks.exists = false
				f.generator.created = false
				if failure {
					f.generator.err = cause
				}
			}
			got, err := f.checker.Check(context.Background(), 1)
			if failure && !errors.Is(err, cause) || !failure && (err != nil || got != OutcomeNotApplied) || f.timeout.calls != 0 || f.parents.writes != 0 || f.parents.clocks != 1 {
				t.Fatalf("stage action fell through: outcome=%d err=%v", got, err)
			}
		}
	}
	for _, post := range []bool{false, true} {
		f := newShardedFixture(t)
		if post {
			f.enablePost(0)
		} else {
			f.tasks.exists = false
		}
		f.parents.facts.DeadlineAt = &f.parents.now
		if got, err := f.checker.Check(context.Background(), 1); err != nil || got != OutcomeStopping || f.timeout.calls != 1 || f.generator.calls+f.post.calls != 0 {
			t.Fatalf("expired check created work: outcome=%d err=%v", got, err)
		}
	}
	for _, stage := range []string{"stages", "exists", "unfinished", "counts", "preparation"} {
		f := newShardedFixture(t)
		f.tasks.failureAt = stage
		f.tasks.err = cause
		if stage == "preparation" {
			f.tasks.exists = false
		}
		f.parents.facts.DeadlineAt = &f.parents.now
		if _, err := f.checker.Check(context.Background(), 1); !errors.Is(err, cause) || f.timeout.calls != 0 || f.parents.writes+f.generator.calls+f.post.calls != 0 {
			t.Fatalf("failed %s read became timeout: %v", stage, err)
		}
	}
	for _, failure := range []bool{false, true} {
		f := newShardedFixture(t)
		f.parents.changed = false
		f.parents.facts.DeadlineAt = &f.parents.now
		if failure {
			f.parents.writeErr = cause
		}
		got, err := f.checker.Check(context.Background(), 1)
		if failure && !errors.Is(err, cause) || !failure && (err != nil || got != OutcomeNotApplied) ||
			f.parents.writes != 1 || f.parents.clocks != 0 || f.timeout.calls != 0 || f.generator.calls+f.post.calls != 0 {
			t.Fatalf("unapplied completion fell through to timeout: outcome=%d err=%v", got, err)
		}
	}
}

func TestWithShardingRejectsMissingDependencies(t *testing.T) {
	for _, option := range []Option{WithSharding(nil, &shardGeneratorStub{}, &postCreatorStub{}), WithSharding((*shardedStoreStub)(nil), &shardGeneratorStub{}, &postCreatorStub{}), WithSharding(&shardedStoreStub{}, nil, &postCreatorStub{}), WithSharding(&shardedStoreStub{}, &shardGeneratorStub{}, nil)} {
		if _, err := NewChecker(&completionStoreStub{}, option); err == nil {
			t.Fatal("missing stage dependency accepted")
		}
	}
}

type shardedFixture struct {
	checker   *Checker
	parents   *completionStoreStub
	tasks     *shardedStoreStub
	generator *shardGeneratorStub
	post      *postCreatorStub
	timeout   *checkTimeoutStub
}

func newShardedFixture(t *testing.T) *shardedFixture {
	t.Helper()
	now := time.Now().UTC()
	deadline := now.Add(time.Minute)
	f := &shardedFixture{parents: &completionStoreStub{facts: &repository.InstanceCompletion{ID: 1, Status: model.InstanceRunning, DeadlineAt: &deadline, ExecutionConfig: []byte(`{"schema_version":1,"execution_mode":2,"post_enabled":false}`)}, now: now, changed: true},
		tasks: &shardedStoreStub{stages: &repository.StageFacts{Preparation: &repository.StageTask{ID: 2, TaskType: model.TaskTypePrepare, Status: model.TaskSucceeded}}, exists: true, counts: repository.ShardCounts{Total: 3, Succeeded: 3}, prepared: &repository.PreparationResult{ID: 2, Status: model.TaskSucceeded, Data: model.TaskData(`[null,9007199254740993]`)}}, generator: &shardGeneratorStub{created: true}, post: &postCreatorStub{created: true}, timeout: &checkTimeoutStub{decision: newTimeoutDecision()}}
	var err error
	f.checker, err = NewChecker(f.parents, WithSharding(f.tasks, f.generator, f.post), WithStopping(f.timeout, func(instancestop.Decision) {}, func(int64) {}))
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *shardedFixture) enablePost(status model.TaskStatus) {
	f.parents.facts.ExecutionConfig = []byte(`{"schema_version":1,"execution_mode":2,"post_enabled":true}`)
	if status != 0 {
		f.tasks.stages.Post = &repository.StageTask{ID: 4, TaskType: model.TaskTypePost, Status: status}
	}
}

type shardedStoreStub struct {
	stages             *repository.StageFacts
	prepared           *repository.PreparationResult
	exists, unfinished bool
	counts             repository.ShardCounts
	calls              []string
	contexts           []context.Context
	failureAt          string
	err                error
}

func (s *shardedStoreStub) visit(ctx context.Context, name string) error {
	s.calls = append(s.calls, name)
	s.contexts = append(s.contexts, ctx)
	if s.failureAt == name {
		return s.err
	}
	return nil
}
func (s *shardedStoreStub) FindStages(ctx context.Context, _ int64) (*repository.StageFacts, error) {
	return s.stages, s.visit(ctx, "stages")
}
func (s *shardedStoreStub) HasShards(ctx context.Context, _ int64) (bool, error) {
	return s.exists, s.visit(ctx, "exists")
}
func (s *shardedStoreStub) FindPreparationResult(ctx context.Context, _ int64) (*repository.PreparationResult, error) {
	return s.prepared, s.visit(ctx, "preparation")
}
func (s *shardedStoreStub) HasUnfinishedShards(ctx context.Context, _ int64) (bool, error) {
	return s.unfinished, s.visit(ctx, "unfinished")
}
func (s *shardedStoreStub) CountShards(ctx context.Context, _ int64) (repository.ShardCounts, error) {
	return s.counts, s.visit(ctx, "counts")
}

type shardGeneratorStub struct {
	created bool
	calls   int
	err     error
	ctx     context.Context
}

func (s *shardGeneratorStub) Generate(ctx context.Context, _ int64, _ repository.PreparationResult) (bool, error) {
	s.calls++
	s.ctx = ctx
	return s.created, s.err
}

type postCreatorStub struct {
	created bool
	calls   int
	err     error
	ctx     context.Context
	counts  repository.ShardCounts
}

func (s *postCreatorStub) Create(ctx context.Context, _ int64, counts repository.ShardCounts) (bool, error) {
	s.calls++
	s.ctx = ctx
	s.counts = counts
	return s.created, s.err
}
