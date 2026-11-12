package taskresult

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/zeromicro/go-zero/core/logx"
)

const saverExecutorID = "11111111-1111-4111-8111-111111111111"

type saverFixture struct {
	saver  *Saver
	repo   *saverTaskRepositoryStub
	report Report
	hints  []int64
}

func newSaverFixture(t *testing.T, options ...Option) *saverFixture {
	t.Helper()
	f := &saverFixture{
		report: Report{GroupID: 41, JobID: 21, InstanceID: 11, TaskID: 31, AttemptNo: 2,
			ExecutorInstanceID: saverExecutorID, Outcome: OutcomeSuccess},
		repo: &saverTaskRepositoryStub{applied: true, dataValid: true,
			target: &repository.TaskResultTarget{TaskID: 31, InstanceID: 11, JobID: 21, ExecutorGroupID: 41,
				AttemptNo: 2, TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning, UsedAttempts: 2,
				ExecutionConfig: json.RawMessage(`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":3,"retry_interval_seconds":10}`)}},
	}
	opts := append([]Option{WithInstanceHint(func(id int64) { f.hints = append(f.hints, id) })}, options...)
	var err error
	f.saver, err = NewSaver(f.repo, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestNewSaverRejectsInvalidDependenciesAndBudgets(t *testing.T) {
	var typedNil *saverTaskRepositoryStub
	for _, dependency := range []repository.JobTaskRepository{nil, typedNil} {
		if saver, err := NewSaver(dependency); saver != nil || err == nil {
			t.Fatalf("accepted nil repository: saver = %v, error = %v", saver, err)
		}
	}
	for _, options := range [][]Option{{nil}, {WithSaveTimeout(0)}, {WithSaveTimeout(-time.Second)}} {
		if saver, err := NewSaver(&saverTaskRepositoryStub{}, options...); saver != nil || err == nil {
			t.Fatalf("accepted invalid options: saver = %v, error = %v", saver, err)
		}
	}
	f := newSaverFixture(t, WithInstanceHint(nil))
	started := time.Now()
	f.repo.onFind = func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		if !ok || deadline.Before(started.Add(DefaultSaveTimeout)) || deadline.After(time.Now().Add(DefaultSaveTimeout)) {
			t.Fatalf("default save budget is missing or incorrect: %v", deadline)
		}
	}
	if result, err := f.saver.Save(context.Background(), f.report); err != nil || !result.Applied || len(f.hints) != 0 {
		t.Fatalf("optional hint: result = %+v, error = %v, hints = %v", result, err, f.hints)
	}
}

func TestSaverPreservesSuccessIdentityAndOptionalOutput(t *testing.T) {
	for _, data := range []json.RawMessage{nil, json.RawMessage("null"), json.RawMessage(`{"value":9223372036854775807}`)} {
		f := newSaverFixture(t)
		f.report.Data = data
		f.repo.onSave = func(context.Context) {
			if len(f.hints) != 0 {
				t.Fatal("hint preceded confirmed persistence")
			}
		}
		result, err := f.saver.Save(context.Background(), f.report)
		if err != nil || !result.Applied || result.Reason != "saved" || !reflect.DeepEqual(f.hints, []int64{11}) {
			t.Fatalf("success: result = %+v, error = %v, hints = %v", result, err, f.hints)
		}
		if !reflect.DeepEqual(f.repo.readIDs, []int64{31}) || len(f.repo.writes) != 1 || len(f.repo.probes) != 0 {
			t.Fatalf("normal result added queries or probes: reads = %v, writes = %d, probes = %d", f.repo.readIDs, len(f.repo.writes), len(f.repo.probes))
		}
		write := f.repo.writes[0]
		if write.TaskID != f.report.TaskID || write.InstanceID != f.report.InstanceID || write.AttemptNo != f.report.AttemptNo ||
			write.ExecutorInstanceID.String() != saverExecutorID || write.Status != model.TaskSucceeded || write.Summary != "" || write.RetryIntervalSeconds != 0 ||
			!bytes.Equal(write.Data, data) || (write.Data == nil) != (data == nil) {
			t.Fatalf("success changed original identity or output: %+v", write)
		}
	}
}

func TestSaverReleasesNoMatchButRetainsReadWriteAndCommitErrors(t *testing.T) {
	failure := errors.New("synthetic database failure")
	for _, tc := range []struct {
		name      string
		configure func(*saverFixture)
		wantError bool
		writes    int
		hints     int
		reason    string
	}{
		{"read error", func(f *saverFixture) { f.repo.findErr = failure }, true, 0, 0, ""},
		{"missing target", func(f *saverFixture) { f.repo.target = nil }, false, 0, 0, "missing_target"},
		{"write error", func(f *saverFixture) { f.repo.applied, f.repo.saveErr = false, failure }, true, 1, 0, ""},
		{"unconfirmed commit", func(f *saverFixture) { f.repo.applied, f.repo.saveErr = true, failure }, true, 1, 0, ""},
		{"zero rows", func(f *saverFixture) { f.repo.applied = false }, false, 1, 1, "no_match"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSaverFixture(t)
			f.report.Data = json.RawMessage(`{"keep":true}`)
			tc.configure(f)
			result, err := f.saver.Save(context.Background(), f.report)
			if tc.wantError {
				if !errors.Is(err, failure) || result != (SaveResult{}) {
					t.Fatalf("error allowed release or lost its cause: result = %+v, error = %v", result, err)
				}
			} else if err != nil || result.Applied || result.Reason != tc.reason {
				t.Fatalf("skip result = %+v, error = %v", result, err)
			}
			if len(f.repo.readIDs) != 1 || len(f.repo.writes) != tc.writes || len(f.repo.probes) != 0 || len(f.hints) != tc.hints {
				t.Fatalf("unexpected retries or hints: reads = %d, writes = %d, probes = %d, hints = %v", len(f.repo.readIDs), len(f.repo.writes), len(f.repo.probes), f.hints)
			}
		})
	}
}

func TestSaverDisqualifiesKnownInvalidTargetsBeforeBadSnapshot(t *testing.T) {
	otherExecutor := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	for _, tc := range []struct {
		name   string
		change func(*repository.TaskResultTarget)
		reason string
		hint   bool
	}{
		{"wrong task", func(v *repository.TaskResultTarget) { v.TaskID++ }, "ownership_mismatch", false},
		{"wrong instance", func(v *repository.TaskResultTarget) { v.InstanceID++ }, "ownership_mismatch", false},
		{"wrong job", func(v *repository.TaskResultTarget) { v.JobID++ }, "ownership_mismatch", false},
		{"wrong group", func(v *repository.TaskResultTarget) { v.ExecutorGroupID++ }, "ownership_mismatch", false},
		{"old attempt", func(v *repository.TaskResultTarget) { v.AttemptNo++ }, "stale_attempt", true},
		{"waiting", func(v *repository.TaskResultTarget) { v.Status = model.TaskWaiting }, "task_closed", true},
		{"retry waiting", func(v *repository.TaskResultTarget) { v.Status = model.TaskRetryWaiting }, "task_closed", true},
		{"succeeded", func(v *repository.TaskResultTarget) { v.Status = model.TaskSucceeded }, "task_closed", true},
		{"failed", func(v *repository.TaskResultTarget) { v.Status = model.TaskFailed }, "task_closed", true},
		{"canceled", func(v *repository.TaskResultTarget) { v.Status = model.TaskCanceled }, "task_closed", true},
		{"different executor", func(v *repository.TaskResultTarget) { v.ExecutorInstanceID = &otherExecutor }, "executor_mismatch", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSaverFixture(t)
			f.repo.target.ExecutionConfig = json.RawMessage("{")
			f.report.Data = json.RawMessage("also invalid but already obsolete")
			tc.change(f.repo.target)
			result, err := f.saver.Save(context.Background(), f.report)
			if err != nil || result.Applied || result.Reason != tc.reason || (len(f.hints) == 1) != tc.hint || len(f.hints) > 1 {
				t.Fatalf("obsolete result inspected unnecessary data: result = %+v, error = %v, hints = %v", result, err, f.hints)
			}
			if len(f.repo.readIDs) != 1 || len(f.repo.writes) != 0 || len(f.repo.probes) != 0 {
				t.Fatal("obsolete result performed a write or another query")
			}
		})
	}
}

func TestSaverRetainsUnknownStateAndTaskTypes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*repository.TaskResultTarget)
		want   error
	}{
		{"zero state", func(v *repository.TaskResultTarget) { v.Status = 0 }, ErrInvalidStoredState},
		{"unknown state", func(v *repository.TaskResultTarget) { v.Status = 99 }, ErrInvalidStoredState},
		{"unknown type", func(v *repository.TaskResultTarget) { v.TaskType = 0 }, ErrUnsupportedTaskType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSaverFixture(t)
			tc.change(f.repo.target)
			result, err := f.saver.Save(context.Background(), f.report)
			if !errors.Is(err, tc.want) || result != (SaveResult{}) || len(f.hints) != 0 || len(f.repo.writes) != 0 || len(f.repo.probes) != 0 {
				t.Fatalf("platform failure was released: result = %+v, error = %v", result, err)
			}
		})
	}
}

func TestSaverParsesOnlyNecessaryConfiguration(t *testing.T) {
	for _, config := range []string{
		`{"schema_version":1,"execution_mode":1,"post_enabled":false}`,
		`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":"bad","retry_interval_seconds":null,"params":{},"processor_name":42}`,
	} {
		f := newSaverFixture(t)
		f.repo.target.ExecutionConfig = json.RawMessage(config)
		if result, err := f.saver.Save(context.Background(), f.report); err != nil || !result.Applied {
			t.Fatalf("success depended on unrelated configuration: result = %+v, error = %v", result, err)
		}
		f = newSaverFixture(t)
		f.repo.target.ExecutionConfig, f.report.Outcome = json.RawMessage(config), OutcomeFailure
		if result, err := f.saver.Save(context.Background(), f.report); !errors.Is(err, ErrInvalidExecutionConfig) || result != (SaveResult{}) || len(f.repo.writes) != 0 || len(f.hints) != 0 {
			t.Fatalf("failure accepted broken retry policy: result = %+v, error = %v", result, err)
		}
	}
	for _, config := range []string{"{", "null", "[]", `{}`, `{"schema_version":2,"execution_mode":1,"post_enabled":false}`, `{"schema_version":1,"execution_mode":2}`} {
		f := newSaverFixture(t)
		f.repo.target.ExecutionConfig = json.RawMessage(config)
		if result, err := f.saver.Save(context.Background(), f.report); !errors.Is(err, ErrInvalidExecutionConfig) || result != (SaveResult{}) || len(f.repo.writes) != 0 || len(f.hints) != 0 {
			t.Fatalf("required broken snapshot: result = %+v, error = %v", result, err)
		}
	}
}

func TestSaverValidatesFrozenStageContract(t *testing.T) {
	for _, stage := range []model.TaskType{model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
		for _, scenario := range []struct {
			name string
			mode int
			post string
		}{
			{"normal", 1, "false"}, {"normal with post", 1, "true"},
			{"sharded", 2, "false"}, {"sharded with post", 2, "true"},
			{"unknown mode", 3, "false"}, {"missing post", 2, ""},
			{"null post", 2, "null"}, {"invalid post", 2, `"false"`},
		} {
			t.Run(fmt.Sprintf("stage_%d/%s", stage, scenario.name), func(t *testing.T) {
				f := newSaverFixture(t)
				f.repo.target.TaskType = stage
				config := fmt.Sprintf(`{"schema_version":1,"execution_mode":%d`, scenario.mode)
				if scenario.post != "" {
					config += `,"post_enabled":` + scenario.post
				}
				f.repo.target.ExecutionConfig = json.RawMessage(config + `}`)
				if stage == model.TaskTypePrepare {
					f.report.PreparedItems = []json.RawMessage{}
				}
				valid := (stage == model.TaskTypeOrdinary && scenario.mode == 1 && scenario.post == "false") ||
					(stage != model.TaskTypeOrdinary && scenario.mode == 2 &&
						(scenario.post == "true" || (stage != model.TaskTypePost && scenario.post == "false")))
				result, err := f.saver.Save(context.Background(), f.report)
				if valid {
					if err != nil || !result.Applied || len(f.repo.writes) != 1 {
						t.Fatalf("valid stage rejected: result=%+v err=%v", result, err)
					}
				} else if !errors.Is(err, ErrInvalidExecutionConfig) || result != (SaveResult{}) || len(f.repo.writes) != 0 || len(f.hints) != 0 {
					t.Fatalf("invalid frozen stage released: result=%+v err=%v", result, err)
				}
			})
		}
	}
}

func TestSaverPreparationOutputIsOneCompleteArray(t *testing.T) {
	for _, items := range [][]json.RawMessage{
		{}, {json.RawMessage(`null`), json.RawMessage(`{"id":9007199254740993}`), json.RawMessage(`[1,"two"]`)},
		preparedItems(jobexecutor.MaxPreparedItems),
	} {
		f := newSaverFixture(t)
		setSaverStage(f, model.TaskTypePrepare)
		f.report.PreparedItems = items
		want, err := json.Marshal(items)
		if err != nil {
			t.Fatal(err)
		}
		result, err := f.saver.Save(context.Background(), f.report)
		if err != nil || !result.Applied || len(f.repo.writes) != 1 || len(f.repo.probes) != 0 {
			t.Fatalf("preparation output: result=%+v err=%v", result, err)
		}
		if write := f.repo.writes[0]; write.Status != model.TaskSucceeded || !bytes.Equal(write.Data, want) {
			t.Fatalf("success and the complete array were not saved together: %+v", write)
		}
	}
}

func TestSaverRejectsInvalidPreparationWithoutCreatingFailure(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		items []json.RawMessage
		data  json.RawMessage
	}{
		{"missing list", nil, nil}, {"too many items", preparedItems(jobexecutor.MaxPreparedItems + 1), nil},
		{"missing element", []json.RawMessage{nil}, nil}, {"invalid JSON", []json.RawMessage{json.RawMessage(`{`)}, nil},
		{"invalid Unicode", []json.RawMessage{{'"', 0xff, '"'}}, nil},
		{"both output fields", []json.RawMessage{}, json.RawMessage(`null`)},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newSaverFixture(t)
			setSaverStage(f, model.TaskTypePrepare)
			f.report.PreparedItems, f.report.Data = scenario.items, scenario.data
			result, err := f.saver.Save(context.Background(), f.report)
			if err != nil || result.Applied || result.Reason != "invalid_output" || len(f.repo.writes) != 0 || len(f.repo.probes) != 0 || len(f.hints) != 0 {
				t.Fatalf("invalid preparation became a task outcome: result=%+v err=%v", result, err)
			}
		})
	}
	for _, stage := range []model.TaskType{model.TaskTypeShard, model.TaskTypePost} {
		f := newSaverFixture(t)
		setSaverStage(f, stage)
		f.report.PreparedItems = []json.RawMessage{}
		if result, err := f.saver.Save(context.Background(), f.report); err != nil || result.Reason != "invalid_output" || len(f.repo.writes) != 0 {
			t.Fatalf("stage %d accepted preparation output: result=%+v err=%v", stage, result, err)
		}
	}
}

func TestSaverAllStageFailuresIgnoreOversizedAndInvalidPartialOutput(t *testing.T) {
	for _, stage := range []model.TaskType{model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
		f := newSaverFixture(t)
		setSaverStage(f, stage)
		f.report.Outcome = OutcomeFailure
		f.report.Data = json.RawMessage{0xff}
		f.report.PreparedItems = make([]json.RawMessage, jobexecutor.MaxPreparedItems+1)
		if result, err := f.saver.Save(context.Background(), f.report); err != nil || !result.Applied || len(f.repo.writes) != 1 ||
			f.repo.writes[0].Status != model.TaskRetryWaiting || f.repo.writes[0].Data != nil || len(f.repo.probes) != 0 {
			t.Fatalf("stage %d failure depended on partial output: result=%+v err=%v", stage, result, err)
		}
	}
}

func TestSaverPreparationExceptionProbesTheWholeSerializedArray(t *testing.T) {
	f := newSaverFixture(t)
	setSaverStage(f, model.TaskTypePrepare)
	f.report.PreparedItems = []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`"\u0000"`)}
	f.repo.applied, f.repo.dataValid, f.repo.saveErr = false, false, &pgconn.PgError{Code: "22P05"}
	result, err := f.saver.Save(context.Background(), f.report)
	if err != nil || result.Reason != "invalid_output" || len(f.repo.probes) != 1 || len(f.repo.writes) != 1 || len(f.hints) != 0 {
		t.Fatalf("preparation validation: result=%+v err=%v", result, err)
	}
	if !bytes.Equal(f.repo.probes[0], f.repo.writes[0].Data) || string(f.repo.probes[0]) != `[1,"\u0000"]` {
		t.Fatalf("probe did not validate the saved array: %q", f.repo.probes[0])
	}
}

func setSaverStage(f *saverFixture, stage model.TaskType) {
	f.repo.target.TaskType = stage
	mode := model.ExecutionModeNormal
	if stage != model.TaskTypeOrdinary {
		mode = model.ExecutionModeSharded
	}
	f.repo.target.ExecutionConfig = json.RawMessage(fmt.Sprintf(`{"schema_version":1,"execution_mode":%d,"post_enabled":%t,"max_retries":3,"retry_interval_seconds":10}`, mode, stage == model.TaskTypePost))
}

func preparedItems(count int) []json.RawMessage {
	items := make([]json.RawMessage, count)
	for index := range items {
		items[index] = json.RawMessage(fmt.Sprintf(`{"id":%d}`, index))
	}
	return items
}

func TestSaverFailureIgnoresOutputsAndUsesRemainingOpportunities(t *testing.T) {
	for _, tc := range []struct {
		used, retries int32
		want          model.TaskStatus
	}{
		{1, 0, model.TaskFailed}, {2, 3, model.TaskRetryWaiting}, {4, 3, model.TaskFailed},
		{math.MaxInt32, math.MaxInt32, model.TaskRetryWaiting},
	} {
		f := newSaverFixture(t)
		f.report.Outcome, f.report.Summary = OutcomeFailure, "business failed"
		f.report.Data, f.report.PreparedItems = json.RawMessage("invalid unused JSON"), []json.RawMessage{json.RawMessage("{")}
		f.repo.target.UsedAttempts = tc.used
		f.repo.target.ExecutionConfig = json.RawMessage(fmt.Sprintf(`{"schema_version":1,"execution_mode":1,"post_enabled":false,"max_retries":%d,"retry_interval_seconds":10}`, tc.retries))
		before := *f.repo.target
		result, err := f.saver.Save(context.Background(), f.report)
		if err != nil || !result.Applied || len(f.repo.writes) != 1 || len(f.repo.probes) != 0 || len(f.hints) != 1 {
			t.Fatalf("failure output prevented persistence: result = %+v, error = %v", result, err)
		}
		write := f.repo.writes[0]
		if write.Status != tc.want || write.Data != nil || write.Summary != f.report.Summary ||
			(write.Status == model.TaskRetryWaiting && write.RetryIntervalSeconds != 10) ||
			(write.Status == model.TaskFailed && write.RetryIntervalSeconds != 0) || !reflect.DeepEqual(before, *f.repo.target) {
			t.Fatalf("failure changed opportunity facts or retained output: %+v", write)
		}
	}
	for _, policy := range []string{
		`"max_retries":null,"retry_interval_seconds":10`, `"max_retries":-1,"retry_interval_seconds":10`,
		`"max_retries":0.5,"retry_interval_seconds":10`, `"max_retries":2147483648,"retry_interval_seconds":10`,
		`"max_retries":0,"retry_interval_seconds":0`, `"max_retries":0,"retry_interval_seconds":-1`,
		`"max_retries":0,"retry_interval_seconds":null`, `"max_retries":0,"retry_interval_seconds":"10"`,
	} {
		f := newSaverFixture(t)
		f.report.Outcome = OutcomeFailure
		f.repo.target.ExecutionConfig = json.RawMessage(`{"schema_version":1,"execution_mode":1,"post_enabled":false,` + policy + `}`)
		if result, err := f.saver.Save(context.Background(), f.report); !errors.Is(err, ErrInvalidExecutionConfig) || result != (SaveResult{}) || len(f.repo.writes) != 0 || len(f.hints) != 0 {
			t.Fatalf("invalid retry policy: result = %+v, error = %v", result, err)
		}
	}
	for _, used := range []int32{0, -1} {
		f := newSaverFixture(t)
		f.report.Outcome, f.repo.target.UsedAttempts = OutcomeFailure, used
		if result, err := f.saver.Save(context.Background(), f.report); !errors.Is(err, ErrInvalidStoredState) || result != (SaveResult{}) || len(f.repo.writes) != 0 || len(f.hints) != 0 {
			t.Fatalf("invalid used opportunities: result = %+v, error = %v", result, err)
		}
	}
}

func TestSaverDistinguishesInvalidReportsAndActuallyUsedOutput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Report)
		reads  int
		reason string
	}{
		{"untrusted group", func(r *Report) { r.GroupID = 0 }, 0, "invalid_report"},
		{"invalid job", func(r *Report) { r.JobID = 0 }, 0, "invalid_report"},
		{"invalid instance", func(r *Report) { r.InstanceID = 0 }, 0, "invalid_report"},
		{"invalid task", func(r *Report) { r.TaskID = 0 }, 0, "invalid_report"},
		{"invalid attempt", func(r *Report) { r.AttemptNo = 0 }, 0, "invalid_report"},
		{"invalid UUID", func(r *Report) { r.ExecutorInstanceID = "invalid" }, 0, "invalid_report"},
		{"noncanonical UUID", func(r *Report) { r.ExecutorInstanceID = strings.ReplaceAll(saverExecutorID, "-", "") }, 0, "invalid_report"},
		{"unknown outcome", func(r *Report) { r.Outcome = 0 }, 0, "invalid_report"},
		{"NUL summary", func(r *Report) { r.Summary = "bad\x00summary" }, 1, "invalid_output"},
		{"invalid UTF8 summary", func(r *Report) { r.Summary = "bad\xffsummary" }, 1, "invalid_output"},
		{"failure NUL summary", func(r *Report) { r.Outcome, r.Summary = OutcomeFailure, "bad\x00summary" }, 1, "invalid_output"},
		{"invalid JSON", func(r *Report) { r.Data = json.RawMessage("{") }, 1, "invalid_output"},
		{"invalid UTF8 JSON", func(r *Report) { r.Data = json.RawMessage{'"', 0xff, '"'} }, 1, "invalid_output"},
		{"ordinary prepared output", func(r *Report) { r.PreparedItems = []json.RawMessage{} }, 1, "invalid_output"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSaverFixture(t)
			tc.change(&f.report)
			result, err := f.saver.Save(context.Background(), f.report)
			if err != nil || result.Applied || result.Reason != tc.reason || len(f.repo.readIDs) != tc.reads || len(f.repo.writes) != 0 || len(f.repo.probes) != 0 || len(f.hints) != 0 {
				t.Fatalf("invalid input crossed persistence boundary: result = %+v, error = %v", result, err)
			}
		})
	}
}

func TestSaverRequiresIndependentProofForDataExceptions(t *testing.T) {
	writeErr := &pgconn.PgError{Code: "22P05", Message: "synthetic data exception"}
	probeErr := errors.New("synthetic probe unavailable")
	for _, tc := range []struct {
		name       string
		valid      bool
		probeErr   error
		wantRetain bool
	}{
		{"independently invalid", false, nil, false},
		{"same SQLSTATE with valid content", true, nil, true},
		{"unconfirmed validation", false, probeErr, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSaverFixture(t)
			f.report.Data = json.RawMessage(`"\u0000"`)
			f.repo.applied, f.repo.saveErr, f.repo.dataValid, f.repo.probeErr = false, fmt.Errorf("write: %w", writeErr), tc.valid, tc.probeErr
			result, err := f.saver.Save(context.Background(), f.report)
			if tc.wantRetain {
				if result != (SaveResult{}) || !errors.Is(err, writeErr) || (tc.probeErr != nil && !errors.Is(err, tc.probeErr)) {
					t.Fatalf("ambiguous content was discarded: result = %+v, error = %v", result, err)
				}
			} else if err != nil || result.Applied || result.Reason != "invalid_output" {
				t.Fatalf("confirmed invalid content was retained: result = %+v, error = %v", result, err)
			}
			if len(f.repo.readIDs) != 1 || len(f.repo.writes) != 1 || len(f.repo.probes) != 1 || len(f.hints) != 0 || !bytes.Equal(f.repo.probes[0], f.report.Data) {
				t.Fatal("exceptional validation changed the original data or repeated persistence")
			}
		})
	}
}

func TestSaverNeverProbesUnusedDataOrUncertainTechnicalFailures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*saverFixture)
	}{
		{"connection failure", func(f *saverFixture) { f.repo.saveErr = errors.New("lost connection") }},
		{"constraint failure", func(f *saverFixture) { f.repo.saveErr = &pgconn.PgError{Code: "23514"} }},
		{"commit unknown", func(f *saverFixture) { f.repo.applied, f.repo.saveErr = true, errors.New("commit confirmation lost") }},
		{"commit unknown with data SQLSTATE", func(f *saverFixture) { f.repo.applied, f.repo.saveErr = true, &pgconn.PgError{Code: "22P05"} }},
		{"failure output", func(f *saverFixture) {
			f.report.Outcome, f.report.Data = OutcomeFailure, json.RawMessage("invalid unused JSON")
			f.repo.saveErr = &pgconn.PgError{Code: "22P05"}
		}},
		{"absent optional data", func(f *saverFixture) { f.report.Data, f.repo.saveErr = nil, &pgconn.PgError{Code: "22P05"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSaverFixture(t)
			f.report.Data, f.repo.applied, f.repo.dataValid = json.RawMessage(`"\u0000"`), false, false
			tc.configure(f)
			result, err := f.saver.Save(context.Background(), f.report)
			if !errors.Is(err, f.repo.saveErr) || result != (SaveResult{}) || len(f.repo.probes) != 0 || len(f.hints) != 0 || len(f.repo.writes) != 1 {
				t.Fatalf("technical failure became content rejection: result = %+v, error = %v, probes = %d", result, err, len(f.repo.probes))
			}
		})
	}
}

func TestSaverUsesOneContextAndDeadlineForReadWriteAndProbe(t *testing.T) {
	f := newSaverFixture(t, WithSaveTimeout(time.Minute))
	f.report.Data, f.repo.applied, f.repo.saveErr = json.RawMessage(`"\u0000"`), false, &pgconn.PgError{Code: "22P05"}
	deadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	var processing context.Context
	checkContext := func(step context.Context) {
		if processing == nil {
			processing = step
		}
		got, ok := step.Deadline()
		if !ok || !got.Equal(deadline) || step != processing {
			t.Fatalf("step received a renewed budget: deadline = %v, want = %v", got, deadline)
		}
	}
	f.repo.onFind, f.repo.onSave, f.repo.onProbe = checkContext, checkContext, checkContext
	if result, err := f.saver.Save(ctx, f.report); !errors.Is(err, f.repo.saveErr) || result != (SaveResult{}) || len(f.repo.probes) != 1 || len(f.hints) != 0 {
		t.Fatalf("single-budget error result = %+v, error = %v", result, err)
	}
	if processing.Err() != context.Canceled {
		t.Fatalf("processing context was retained after Save returned: %v", processing.Err())
	}
}

func TestSaverDeadlineStopsTheCurrentStageWithoutRenewal(t *testing.T) {
	for _, stage := range []string{"read", "write", "probe"} {
		t.Run(stage, func(t *testing.T) {
			f := newSaverFixture(t, WithSaveTimeout(20*time.Millisecond))
			f.report.Data, f.repo.applied = json.RawMessage(`"\u0000"`), false
			waitForDeadline := func(ctx context.Context) { <-ctx.Done() }
			switch stage {
			case "read":
				f.repo.onFind, f.repo.findErr = waitForDeadline, context.DeadlineExceeded
			case "write":
				f.repo.onSave, f.repo.saveErr = waitForDeadline, context.DeadlineExceeded
			case "probe":
				f.repo.saveErr, f.repo.onProbe, f.repo.probeErr = &pgconn.PgError{Code: "22P05"}, waitForDeadline, context.DeadlineExceeded
			}
			result, err := f.saver.Save(context.Background(), f.report)
			if !errors.Is(err, context.DeadlineExceeded) || result != (SaveResult{}) || len(f.hints) != 0 {
				t.Fatalf("timeout released the original report: result = %+v, error = %v", result, err)
			}
			if stage == "read" && len(f.repo.writes) != 0 || stage != "probe" && len(f.repo.probes) != 0 {
				t.Fatal("expired operation continued into another stage")
			}
		})
	}
	f := newSaverFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := f.saver.Save(ctx, f.report); !errors.Is(err, context.Canceled) || result != (SaveResult{}) || len(f.repo.readIDs) != 0 || len(f.hints) != 0 {
		t.Fatalf("already-canceled work reached persistence: result = %+v, error = %v", result, err)
	}
}

func TestSaverDiagnosticsNeverPrintBusinessContent(t *testing.T) {
	var logs bytes.Buffer
	previous := logx.Reset()
	logx.SetWriter(logx.NewWriter(&logs))
	t.Cleanup(func() { logx.SetWriter(previous) })
	f := newSaverFixture(t)
	f.report.Summary = "private-summary-marker"
	f.report.Data = json.RawMessage(`{"business":"private-data-marker","invalid":"\u0000"}`)
	f.repo.applied, f.repo.dataValid = false, false
	f.repo.saveErr = &pgconn.PgError{Code: "22P05", Message: "private-driver-marker"}
	if result, err := f.saver.Save(context.Background(), f.report); err != nil || result.Reason != "invalid_output" {
		t.Fatalf("invalid result: %+v, error = %v", result, err)
	}
	if !strings.Contains(logs.String(), "invalid_output") || !strings.Contains(logs.String(), "task_id") {
		t.Fatal("discard diagnostic omitted its safe reason or identity")
	}
	f.repo.dataValid = true
	if result, err := f.saver.Save(context.Background(), f.report); !errors.Is(err, f.repo.saveErr) || result != (SaveResult{}) {
		t.Fatalf("technical result: %+v, error = %v", result, err)
	}
	for _, forbidden := range []string{"private-summary-marker", "private-data-marker", "private-driver-marker"} {
		if strings.Contains(logs.String(), forbidden) {
			t.Fatalf("diagnostics exposed %s", forbidden)
		}
	}
}

// saverTaskRepositoryStub keeps all methods together and records observable
// storage boundaries. No method implements SQL or simulates transaction success.
type saverTaskRepositoryStub struct {
	target                     *repository.TaskResultTarget
	findErr, saveErr, probeErr error
	applied, dataValid         bool
	readIDs                    []int64
	writes                     []repository.TaskResultWrite
	probes                     []model.TaskData
	onFind, onSave, onProbe    func(context.Context)
}

func (*saverTaskRepositoryStub) FindDetail(context.Context, int64, repository.TaskDataSelection) (*repository.TaskDetail, error) {
	panic("unexpected management query")
}

func (*saverTaskRepositoryStub) ListByInstance(context.Context, int64, int64, int64) ([]repository.TaskSummary, int64, error) {
	panic("unexpected ListByInstance")
}

func (*saverTaskRepositoryStub) InsertInitial(context.Context, int64, model.TaskType, time.Time) (int64, error) {
	panic("result saving must not register another attempt")
}

func (*saverTaskRepositoryStub) FindPreparation(context.Context, int64) (*repository.PreparationTask, error) {
	panic("result saving must not read a preparation stage")
}

func (*saverTaskRepositoryStub) FindPreparationResult(context.Context, int64) (*repository.PreparationResult, error) {
	panic("result saving must not read another stage's output")
}

func (*saverTaskRepositoryStub) HasShards(context.Context, int64) (bool, error) {
	panic("result saving must not inspect generated shards")
}

func (*saverTaskRepositoryStub) InsertShards(context.Context, int64, []model.TaskData, time.Time) error {
	panic("result saving must not generate shards")
}

func (*saverTaskRepositoryStub) FindStages(context.Context, int64) (*repository.StageFacts, error) {
	panic("result saving must not check stage completion")
}

func (*saverTaskRepositoryStub) HasUnfinishedShards(context.Context, int64) (bool, error) {
	panic("result saving must not inspect unfinished shards")
}

func (*saverTaskRepositoryStub) CountShards(context.Context, int64) (repository.ShardCounts, error) {
	panic("result saving must not aggregate shards")
}

func (*saverTaskRepositoryStub) FindPost(context.Context, int64) (*repository.StageTask, error) {
	panic("result saving must not look up Post")
}

func (*saverTaskRepositoryStub) InsertPost(context.Context, int64, repository.ShardCounts, time.Time) (int64, error) {
	panic("result saving must not create Post")
}

func (*saverTaskRepositoryStub) FindUnboundTargets(context.Context, []int64) ([]repository.UnboundTarget, error) {
	panic("unexpected FindUnboundTargets")
}

func (*saverTaskRepositoryStub) ListUnfinishedIDs(context.Context, int64, int) ([]int64, error) {
	panic("unexpected ListUnfinishedIDs")
}
func (*saverTaskRepositoryStub) CancelUnfinished(context.Context, repository.TaskCancellation) (int64, error) {
	panic("unexpected CancelUnfinished")
}

func (*saverTaskRepositoryStub) ListCancelTargets(context.Context, int64, string, int) ([]string, error) {
	panic("unexpected ListCancelTargets")
}

func (*saverTaskRepositoryStub) ListUnboundCandidates(context.Context, time.Time, time.Time, *repository.UnboundCursor, int) ([]repository.UnboundCandidate, error) {
	panic("result saving must not scan unbound candidates")
}

func (*saverTaskRepositoryStub) SettleUnknown(context.Context, repository.UnknownSettlement) (*repository.SettledTask, error) {
	panic("unexpected SettleUnknown")
}

func (*saverTaskRepositoryStub) ListDispatchCandidates(context.Context, time.Time, *repository.DispatchCursor, int) ([]repository.DispatchCandidate, error) {
	panic("result saving must not scan dispatch candidates")
}

func (*saverTaskRepositoryStub) ListInstanceDispatchCandidates(context.Context, int64, time.Time, int) ([]repository.DispatchCandidate, error) {
	panic("result saving must not scan dispatch candidates")
}

func (*saverTaskRepositoryStub) RegisterAttempt(context.Context, repository.AttemptRegistration) (*repository.RegisteredTask, error) {
	panic("result saving must not register another attempt")
}

func (*saverTaskRepositoryStub) BindExecutor(context.Context, repository.ExecutorBinding) (bool, error) {
	panic("result saving must not bind an executor")
}

func (*saverTaskRepositoryStub) SaveDispatchBusy(context.Context, repository.DispatchBusyWrite) (bool, error) {
	panic("result saving must not refund an execution opportunity")
}

func (*saverTaskRepositoryStub) SaveDispatchRejection(context.Context, repository.DispatchRejectionWrite) (bool, error) {
	panic("result saving must not settle an admission rejection")
}

func (s *saverTaskRepositoryStub) FindResultTarget(ctx context.Context, id int64) (*repository.TaskResultTarget, error) {
	s.readIDs = append(s.readIDs, id)
	if s.onFind != nil {
		s.onFind(ctx)
	}
	return s.target, s.findErr
}

func (s *saverTaskRepositoryStub) SaveResult(ctx context.Context, write repository.TaskResultWrite) (bool, error) {
	s.writes = append(s.writes, write)
	if s.onSave != nil {
		s.onSave(ctx)
	}
	return s.applied, s.saveErr
}

func (s *saverTaskRepositoryStub) CheckResultData(ctx context.Context, data model.TaskData) (bool, error) {
	s.probes = append(s.probes, append(model.TaskData(nil), data...))
	if s.onProbe != nil {
		s.onProbe(ctx)
	}
	return s.dataValid, s.probeErr
}
