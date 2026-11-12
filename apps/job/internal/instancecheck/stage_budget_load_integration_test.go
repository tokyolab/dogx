//go:build integration && load

package instancecheck

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const stageLoadCheckBudget = 2 * time.Second

func TestShardedGenerationFitsSharedCheckBudget(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	nearLimit := stageLoadItems(500, true)
	stageLoadReportSize(t, nearLimit)
	observer := newStageLoadObserver(t, db)
	for _, scenario := range []struct {
		name  string
		items []json.RawMessage
	}{
		{"one_compact", stageLoadItems(1, false)},
		{"500_compact", stageLoadItems(500, false)},
		{"500_near_receive_limit_1", nearLimit},
		{"500_near_receive_limit_2", nearLimit},
		{"500_near_receive_limit_3", nearLimit},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			parent, _ := generationFixture(t, db, scenario.items)
			ctx, cancel := context.WithTimeout(context.Background(), stageLoadCheckBudget)
			defer cancel()
			before := sqlDB.Stats()
			if got, err := observer.Check(ctx, parent.ID); err != nil || got != OutcomeAdvanced {
				t.Fatalf("full generation check: outcome=%d err=%v", got, err)
			}
			after := sqlDB.Stats()
			samples := observer.snapshot()
			sample := samples[len(samples)-1]
			stageLoadAssertSample(t, sample, 10)
			if sample.sql.inserts != 1 || sample.sql.insertRows != int64(len(scenario.items)) || len(sample.transactions) != 1 {
				t.Fatalf("generation was not one complete insert and transaction: %+v", sample)
			}
			if sample.sql.payloadReads != 1 || sample.sql.aggregates != 0 {
				t.Fatalf("generation repeated preparation payload or aggregated shards: %+v", sample.sql)
			}
			stageLoadAssertShards(t, db, parent.ID, scenario.items)
			t.Logf("scenario=%s shards=%d check=%s transaction_including_acquire_and_commit=%s statements=%d sql_trace_total=%s longest_sql=%s",
				scenario.name, len(scenario.items), sample.elapsed, sample.transactions[0], sample.sql.statements, sample.sql.elapsed, sample.sql.longest)
			stageLoadLogPool(t, before, after)
		})
	}
}

func TestMixedInstanceCheckQueuePreservesProgressUnderShardLoad(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	observer := newStageLoadObserver(t, db)
	queue, err := NewQueue(observer, Config{Capacity: 512, Concurrency: 8, Timeout: stageLoadCheckBudget})
	if err != nil {
		t.Fatal(err)
	}

	const hotRounds, ordinaryPerRound = 8, 4
	var admissionFailures atomic.Int32
	reports := make([]taskresult.Report, hotRounds*ordinaryPerRound)
	for i := range reports {
		reports[i] = stageLoadOrdinaryReport(t, db)
		observer.kinds[reports[i].InstanceID] = "ordinary_completion"
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	saver, err := taskresult.NewSaver(tasks, taskresult.WithInstanceHint(func(id int64) {
		if !observer.hint(queue, id) {
			admissionFailures.Add(1)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	resultObserver := &stageLoadResultObserver{saver: saver}
	results, err := taskresult.NewQueue(resultObserver, taskresult.QueueConfig{})
	if err != nil {
		t.Fatal(err)
	}

	var initial, completedPostIDs []int64
	generated := make(map[int64][]json.RawMessage)
	nearLimit := stageLoadItems(500, true)
	stageLoadReportSize(t, nearLimit)
	for i := range 8 {
		items := stageLoadItems(500, false)
		if i < 4 {
			items = nearLimit
		}
		parent, _ := generationFixture(t, db, items)
		generated[parent.ID] = items
		observer.kinds[parent.ID] = "shard_generation"
		initial = append(initial, parent.ID)

		waiting := newStageIntegrationFixture(t, db, false, 0)
		waiting.savePreparation(t, stageLoadItems(10, false), taskresult.OutcomeSuccess)
		if got, err := waiting.checker.Check(context.Background(), waiting.parent.ID); err != nil || got != OutcomeAdvanced {
			t.Fatalf("prepare waiting shard fixture: %d %v", got, err)
		}
		observer.kinds[waiting.parent.ID] = "waiting_shards"
		initial = append(initial, waiting.parent.ID)
	}
	for range 4 {
		f := newStageIntegrationFixture(t, db, true, 0)
		f.savePreparation(t, stageLoadItems(1, false), taskresult.OutcomeSuccess)
		if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeAdvanced {
			t.Fatalf("prepare Post fixture shards: %d %v", got, err)
		}
		f.finishTask(t, f.tasksOfType(t, model.TaskTypeShard)[0], taskresult.OutcomeSuccess)
		if got, err := f.checker.Check(context.Background(), f.parent.ID); err != nil || got != OutcomeAdvanced {
			t.Fatalf("prepare Post fixture: %d %v", got, err)
		}
		f.finishTask(t, f.tasksOfType(t, model.TaskTypePost)[0], taskresult.OutcomeSuccess)
		observer.kinds[f.parent.ID] = "post_completion"
		initial = append(initial, f.parent.ID)
		completedPostIDs = append(completedPostIDs, f.parent.ID)
	}
	hot := newStageIntegrationFixture(t, db, false, 0)
	hot.savePreparation(t, stageLoadItems(1, false), taskresult.OutcomeSuccess)
	if got, err := hot.checker.Check(context.Background(), hot.parent.ID); err != nil || got != OutcomeAdvanced {
		t.Fatalf("prepare repeatedly hinted fixture: %d %v", got, err)
	}
	observer.kinds[hot.parent.ID] = "repeated_waiting_shards"
	observer.onStart = func(id int64, attempt int) {
		if id != hot.parent.ID {
			return
		}
		if attempt > hotRounds {
			admissionFailures.Add(1)
			return
		}
		for _, report := range reports[(attempt-1)*ordinaryPerRound : attempt*ordinaryPerRound] {
			if accepted, err := results.Accept(context.Background(), report); err != nil || !accepted {
				admissionFailures.Add(1)
			}
		}
		if attempt < hotRounds {
			// All duplicates arrive while this ID is processing. They must retain
			// one follow-up at the tail, not one queue entry per event.
			for range 64 {
				if !observer.hint(queue, id) {
					admissionFailures.Add(1)
				}
			}
		}
	}

	// Failure cleanup also joins both queues before the schema and pool close.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := results.Drain(ctx); err != nil {
			t.Errorf("drain load result queue: %v", err)
		}
		if err := queue.Drain(ctx); err != nil {
			t.Errorf("drain load check queue: %v", err)
		}
		if err := results.Wait(context.Background()); err != nil {
			t.Errorf("wait load result queue: %v", err)
		}
		if err := queue.Wait(context.Background()); err != nil {
			t.Errorf("wait load check queue: %v", err)
		}
	})
	if err := results.Start(); err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(); err != nil {
		t.Fatal(err)
	}
	before := sqlDB.Stats()
	if !observer.hint(queue, hot.parent.ID) {
		t.Fatal("hot Instance hint was rejected")
	}
	for _, id := range initial {
		if !observer.hint(queue, id) {
			t.Fatal("initial mixed hint was rejected")
		}
	}
	// This only bounds a broken test. It is not a latency acceptance threshold.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wantCalls := len(initial) + len(reports) + hotRounds
	for range wantCalls {
		select {
		case <-observer.finished:
		case <-ctx.Done():
			t.Fatalf("mixed admitted work did not progress: completed=%d expected=%d", len(observer.snapshot()), wantCalls)
		}
	}
	if err := results.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := queue.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	after := sqlDB.Stats()
	if admissionFailures.Load() != 0 || observer.overlap || observer.peak > 8 {
		t.Fatalf("queue admission/concurrency violated: rejected=%d overlap=%v peak=%d", admissionFailures.Load(), observer.overlap, observer.peak)
	}
	samples := observer.snapshot()
	if len(samples) != wantCalls {
		t.Fatalf("duplicate hints created extra work: got=%d want=%d", len(samples), wantCalls)
	}
	if len(observer.calls) != len(observer.kinds) {
		t.Fatalf("mixed checks lost or introduced Instance identities: actual=%d expected=%d", len(observer.calls), len(observer.kinds))
	}
	for id, kind := range observer.kinds {
		want := 1
		if id == hot.parent.ID {
			want = hotRounds
		}
		if observer.calls[id] != want {
			t.Fatalf("unexpected per-Instance check count: id=%d kind=%s actual=%d expected=%d", id, kind, observer.calls[id], want)
		}
	}
	var lastHotStart time.Time
	for _, sample := range samples {
		if sample.id == hot.parent.ID && sample.started.After(lastHotStart) {
			lastHotStart = sample.started
		}
	}
	ordinaryProgressDuringHot := 0
	for _, sample := range samples {
		wantSQL, wantOutcome := 5, OutcomeNotReady
		switch sample.kind {
		case "ordinary_completion":
			wantSQL, wantOutcome = 2, OutcomeTerminal
			if sample.started.Add(sample.elapsed).Before(lastHotStart) {
				ordinaryProgressDuringHot++
			}
		case "shard_generation":
			wantSQL, wantOutcome = 10, OutcomeAdvanced
		case "post_completion":
			wantSQL, wantOutcome = 6, OutcomeTerminal
		}
		stageLoadAssertSample(t, sample, wantSQL)
		if sample.outcome != wantOutcome || (sample.kind != "shard_generation" && sample.sql.payloadReads != 0) {
			t.Fatalf("unexpected stage work: kind=%s outcome=%d payload_reads=%d", sample.kind, sample.outcome, sample.sql.payloadReads)
		}
	}
	if ordinaryProgressDuringHot == 0 {
		t.Fatal("ordinary completion made no progress until repeated hot checks ended")
	}
	if resultObserver.calls != len(reports) || resultObserver.failures != 0 || resultObserver.notApplied != 0 {
		t.Fatalf("result persistence retried or failed: calls=%d failures=%d not_applied=%d", resultObserver.calls, resultObserver.failures, resultObserver.notApplied)
	}
	for id, items := range generated {
		stageLoadAssertShards(t, db, id, items)
	}
	var completed int64
	ids := make([]int64, 0, len(reports)+len(completedPostIDs))
	for _, report := range reports {
		ids = append(ids, report.InstanceID)
	}
	ids = append(ids, completedPostIDs...)
	if err := db.Model(&model.JobInstance{}).Where("id IN ? AND status = ?", ids, model.InstanceSucceeded).Count(&completed).Error; err != nil || completed != int64(len(ids)) {
		t.Fatalf("ordinary/Post work did not complete: count=%d expected=%d err=%v", completed, len(ids), err)
	}
	stageLoadLogSamples(t, samples)
	stageLoadLogPool(t, before, after)
	t.Logf("check_peak=%d result_saves=%d result_save_total=%s result_save_max=%s duplicate_hot_hints=%d hot_checks=%d ordinary_completed_before_last_hot_check=%d",
		observer.peak, resultObserver.calls, resultObserver.total, resultObserver.longest, (hotRounds-1)*64, hotRounds, ordinaryProgressDuringHot)
}

// stageLoadSQLTrace counts actual GORM statements without rendering bound input
// payloads. Timings are client-observed statement time, not server-only execution;
// database/sql BEGIN and COMMIT are measured by the transaction wrapper instead.
type stageLoadSQLTrace struct{ logger.Interface }

func (s *stageLoadSQLTrace) ParamsFilter(_ context.Context, statement string, _ ...any) (string, []any) {
	return statement, nil
}

func (s *stageLoadSQLTrace) Trace(ctx context.Context, begin time.Time, statement func() (string, int64), err error) {
	sample, _ := ctx.Value(stageLoadContextKey{}).(*stageLoadSample)
	if sample == nil {
		return
	}
	elapsed := time.Since(begin)
	query, rows := statement()
	metrics := &sample.sql
	metrics.statements++
	metrics.elapsed += elapsed
	metrics.longest = max(metrics.longest, elapsed)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		metrics.failures++
	}
	if strings.HasPrefix(query, "INSERT") {
		metrics.inserts++
		metrics.insertRows += rows
	}
	if strings.HasPrefix(query, "SELECT") && strings.Contains(query, "result_data") {
		metrics.payloadReads++
	}
	if strings.Contains(query, "COUNT(*)") {
		metrics.aggregates++
	}
}

type stageLoadContextKey struct{}

type stageLoadSQLMetrics struct {
	statements, failures, inserts, payloadReads, aggregates int
	insertRows                                              int64
	elapsed, longest                                        time.Duration
}

type stageLoadSample struct {
	id             int64
	kind           string
	started        time.Time
	wait, elapsed  time.Duration
	transactions   []time.Duration
	sql            stageLoadSQLMetrics
	outcome        Outcome
	err, budgetErr error
}

// stageLoadObserver leaves real Check and Queue decisions unchanged. Per-call
// metrics travel through Context; completed samples are published under the mutex.
// kinds and onStart are configured before queue startup and remain immutable.
type stageLoadObserver struct {
	checker  *Checker
	kinds    map[int64]string
	onStart  func(int64, int)
	finished chan struct{}
	mu       sync.Mutex
	hinted   map[int64]time.Time
	active   map[int64]bool
	calls    map[int64]int
	samples  []stageLoadSample
	running  int
	peak     int
	overlap  bool
}

func newStageLoadObserver(t *testing.T, db *gorm.DB) *stageLoadObserver {
	t.Helper()
	measured := db.Session(&gorm.Session{Logger: &stageLoadSQLTrace{Interface: logger.Default.LogMode(logger.Silent)}})
	instances, err := repository.NewJobInstanceRepository(measured)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := repository.NewJobTaskRepository(measured)
	if err != nil {
		t.Fatal(err)
	}
	run, err := repository.NewTransactionRunner(measured)
	if err != nil {
		t.Fatal(err)
	}
	transact := func(ctx context.Context, fn func(repository.Repositories) error) error {
		started := time.Now()
		err := run(ctx, fn)
		if sample, ok := ctx.Value(stageLoadContextKey{}).(*stageLoadSample); ok {
			sample.transactions = append(sample.transactions, time.Since(started))
		}
		return err
	}
	generator, err := NewGenerator(transact, nil)
	if err != nil {
		t.Fatal(err)
	}
	post, err := NewPostCreator(transact, nil)
	if err != nil {
		t.Fatal(err)
	}
	stop, err := instancestop.NewController(transact)
	if err != nil {
		t.Fatal(err)
	}
	checker, err := NewChecker(instances, WithSharding(tasks, generator, post), WithStopping(stop, func(instancestop.Decision) {}, func(int64) {}))
	if err != nil {
		t.Fatal(err)
	}
	return &stageLoadObserver{checker: checker, kinds: make(map[int64]string), finished: make(chan struct{}, 128),
		hinted: make(map[int64]time.Time), active: make(map[int64]bool), calls: make(map[int64]int)}
}

func (o *stageLoadObserver) hint(queue *Queue, id int64) bool {
	o.mu.Lock()
	if _, exists := o.hinted[id]; !exists {
		o.hinted[id] = time.Now()
	}
	o.mu.Unlock()
	return queue.Hint(id)
}

func (o *stageLoadObserver) Check(ctx context.Context, id int64) (Outcome, error) {
	started := time.Now()
	sample := stageLoadSample{id: id, kind: o.kinds[id], started: started}
	o.mu.Lock()
	if hinted, exists := o.hinted[id]; exists {
		sample.wait = started.Sub(hinted)
		delete(o.hinted, id)
	}
	o.overlap = o.overlap || o.active[id]
	o.active[id] = true
	o.running++
	o.peak = max(o.peak, o.running)
	o.calls[id]++
	call := o.calls[id]
	o.mu.Unlock()
	if o.onStart != nil {
		o.onStart(id, call)
	}
	sample.outcome, sample.err = o.checker.Check(context.WithValue(ctx, stageLoadContextKey{}, &sample), id)
	sample.elapsed, sample.budgetErr = time.Since(started), ctx.Err()
	o.mu.Lock()
	o.running--
	delete(o.active, id)
	o.samples = append(o.samples, sample)
	o.mu.Unlock()
	o.finished <- struct{}{}
	return sample.outcome, sample.err
}

func (o *stageLoadObserver) snapshot() []stageLoadSample {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.samples)
}

// stageLoadResultObserver measures the independent, default result Queue. A
// retry would increase calls and failures, so eventual success cannot hide it.
type stageLoadResultObserver struct {
	saver                       *taskresult.Saver
	mu                          sync.Mutex
	calls, failures, notApplied int
	total, longest              time.Duration
}

func (o *stageLoadResultObserver) Save(ctx context.Context, report taskresult.Report) (taskresult.SaveResult, error) {
	started := time.Now()
	result, err := o.saver.Save(ctx, report)
	elapsed := time.Since(started)
	o.mu.Lock()
	o.calls++
	o.total += elapsed
	o.longest = max(o.longest, elapsed)
	if err != nil {
		o.failures++
	}
	if !result.Applied {
		o.notApplied++
	}
	o.mu.Unlock()
	return result, err
}

func stageLoadItems(count int, large bool) []json.RawMessage {
	items := make([]json.RawMessage, count)
	random := rand.New(rand.NewPCG(17, 29))
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	for i := range items {
		if !large {
			items[i] = json.RawMessage(fmt.Sprintf("%d", i+1))
			continue
		}
		// Vary contents so PostgreSQL TOAST cannot compress a repeated padding
		// string into an unrealistically cheap large-payload fixture.
		body := make([]byte, (4<<20)/500-96)
		for j := range body {
			body[j] = alphabet[random.IntN(len(alphabet))]
		}
		items[i] = json.RawMessage(fmt.Sprintf("\"%03d:%s\"", i, body))
	}
	return items
}

func stageLoadReportSize(t *testing.T, items []json.RawMessage) {
	t.Helper()
	encodedItems := make([][]byte, len(items))
	for i := range items {
		encodedItems[i] = items[i]
	}
	request := &protocol.ReportTaskResultRequest{
		Auth:  &protocol.GroupAuth{GroupName: "stage-load-fixture", Token: strings.ReplaceAll(uuid.NewString(), "-", "")},
		JobId: 11, InstanceId: 12, TaskId: 13, AttemptNo: 1, ExecutorInstanceId: uuid.NewString(), Outcome: protocol.TaskOutcome_SUCCESS,
		Output: &protocol.ReportTaskResultRequest_PreparedItems{PreparedItems: &protocol.PreparedItems{Items: encodedItems}},
	}
	// The current result server retains gRPC's 4 MiB receive default. Measuring
	// protobuf size here selects the DB workload; it does not test RPC admission.
	const receiveLimit = 4 << 20
	size := proto.Size(request)
	if size < receiveLimit*98/100 || size >= receiveLimit {
		t.Fatalf("large preparation fixture misses receive boundary: bytes=%d limit=%d", size, receiveLimit)
	}
	t.Logf("preparation_report_protobuf_bytes=%d receive_default_bytes=%d shards=%d", size, receiveLimit, len(items))
}

func stageLoadOrdinaryReport(t *testing.T, db *gorm.DB) taskresult.Report {
	t.Helper()
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	run, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	registrar, err := dispatch.NewRegistrar(run)
	if err != nil {
		t.Fatal(err)
	}
	request, operator := uuid.New(), int64(1)
	parent := model.JobInstance{JobID: 11, JobName: "mixed ordinary check", ExecutorGroupID: 17,
		TriggerType: model.TriggerManual, RequestID: &request, TriggeredBy: &operator,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "test.ordinary", ExecutionMode: model.ExecutionModeNormal, RetryIntervalSeconds: 1, TimeoutSeconds: 900}}
	if _, created, err := instances.Insert(context.Background(), &parent); err != nil || !created {
		t.Fatalf("create ordinary parent: %v %v", created, err)
	}
	attempt, err := registrar.StartInstance(context.Background(), parent.ID)
	if err != nil || attempt == nil {
		t.Fatalf("start ordinary Task: %+v %v", attempt, err)
	}
	return taskresult.Report{GroupID: parent.ExecutorGroupID, JobID: parent.JobID, InstanceID: parent.ID,
		TaskID: attempt.TaskID, AttemptNo: attempt.AttemptNo, ExecutorInstanceID: uuid.NewString(), Outcome: taskresult.OutcomeSuccess}
}

func stageLoadAssertSample(t *testing.T, sample stageLoadSample, statements int) {
	t.Helper()
	if sample.err != nil || sample.budgetErr != nil || sample.elapsed >= stageLoadCheckBudget || sample.sql.failures != 0 {
		t.Fatalf("check failed its unchanged shared budget: id=%d kind=%s elapsed=%s err=%v budget_err=%v sql_errors=%d",
			sample.id, sample.kind, sample.elapsed, sample.err, sample.budgetErr, sample.sql.failures)
	}
	if sample.sql.statements != statements {
		t.Fatalf("unexpected SQL count: kind=%s actual=%d expected=%d", sample.kind, sample.sql.statements, statements)
	}
}

func stageLoadAssertShards(t *testing.T, db *gorm.DB, id int64, items []json.RawMessage) {
	t.Helper()
	var tasks []model.JobTask
	if err := db.Where("instance_id = ? AND task_type = ?", id, model.TaskTypeShard).Order("id").Find(&tasks).Error; err != nil || len(tasks) != len(items) {
		t.Fatalf("generated shard set: count=%d expected=%d err=%v", len(tasks), len(items), err)
	}
	for i, task := range tasks {
		var compact bytes.Buffer
		if err := json.Compact(&compact, task.InputData); err != nil || !bytes.Equal(compact.Bytes(), items[i]) {
			t.Fatalf("fixed shard input changed: instance=%d position=%d err=%v", id, i, err)
		}
		if task.Status != model.TaskWaiting || task.AttemptNo != 0 || task.UsedAttempts != 0 || task.StartedAt != nil || task.AttemptRegisteredAt != nil {
			t.Fatalf("generation unexpectedly authorized execution: instance=%d task=%d", id, task.ID)
		}
	}
}

func stageLoadLogPool(t *testing.T, before, after sql.DBStats) {
	t.Helper()
	t.Logf("database_pool_max=%d wait_count=%d wait_duration=%s in_use_after=%d",
		after.MaxOpenConnections, after.WaitCount-before.WaitCount, after.WaitDuration-before.WaitDuration, after.InUse)
}

func stageLoadLogSamples(t *testing.T, samples []stageLoadSample) {
	t.Helper()
	for _, kind := range []string{"ordinary_completion", "shard_generation", "waiting_shards", "post_completion", "repeated_waiting_shards"} {
		var waits, durations, transactions []time.Duration
		var sqlTotal, longestSQL time.Duration
		statements := 0
		for _, sample := range samples {
			if sample.kind == kind {
				waits = append(waits, sample.wait)
				durations = append(durations, sample.elapsed)
				transactions = append(transactions, sample.transactions...)
				statements += sample.sql.statements
				sqlTotal += sample.sql.elapsed
				longestSQL = max(longestSQL, sample.sql.longest)
			}
		}
		slices.Sort(waits)
		slices.Sort(durations)
		slices.Sort(transactions)
		t.Logf("kind=%s checks=%d statements=%d hint_to_start_including_merge_p95=%s hint_to_start_max=%s check_p95=%s check_p99=%s check_max=%s transaction_p95=%s transaction_max=%s sql_trace_total=%s longest_sql=%s",
			kind, len(durations), statements, stageLoadPercentile(waits, 95), stageLoadPercentile(waits, 100),
			stageLoadPercentile(durations, 95), stageLoadPercentile(durations, 99), stageLoadPercentile(durations, 100),
			stageLoadPercentile(transactions, 95), stageLoadPercentile(transactions, 100), sqlTotal, longestSQL)
	}
}

func stageLoadPercentile(sorted []time.Duration, percentile int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[(len(sorted)*percentile+99)/100-1]
}
