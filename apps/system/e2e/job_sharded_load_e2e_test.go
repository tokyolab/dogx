//go:build integration && e2e && load

package e2e

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
)

// TestJobShardedLoadEndToEnd is the approved two-scheduler, two-executor local
// acceptance workload. Every process retains its production worker and queue
// defaults. Cron creates each measured wave; manual calls are used only for the
// separate near-message-limit and stop/result-competition checks.
func TestJobShardedLoadEndToEnd(t *testing.T) {
	f := newJobLifecycleFixture(t)
	f.jobConfigTransform = func(config string) string {
		const override = "\nDispatch:\n  ScanInterval: 100ms\n"
		if strings.Count(config, override) != 1 {
			t.Fatal("cannot restore production dispatch scan defaults")
		}
		return strings.Replace(config, override, "\n", 1)
	}
	// Distinct sequence values expose duplicate calls across executor processes.
	// This changes only the isolated business fixture, not any scheduler table.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := f.sqlDB.ExecContext(ctx, `CREATE SEQUENCE system_job_load_invocations;
		ALTER TABLE system_job_e2e_records ALTER COLUMN invocation_no SET DEFAULT nextval('system_job_load_invocations')`)
	cancel()
	if err != nil {
		t.Fatalf("initialize load invocation audit: %v", err)
	}
	a := f.startJob(t, "job-load-a")
	f.createGroup(t, a)
	b := f.startJob(t, "job-load-b")
	x := f.startSystemHost(t, "system-load-a", "TestJobShardedLoadHostProcess")
	y := f.startSystemHost(t, "system-load-b", "TestJobShardedLoadHostProcess")
	names := map[string]string{a.appName: "job-load-a", b.appName: "job-load-b", x.appName: "system-load-a", y.appName: "system-load-b"}
	report := jobShardedLoadReport{
		Common: jobLoadReport{StartedAt: time.Now().UTC(), WaveCount: 6, WaveIntervalSeconds: 60,
			BusinessDelayMilliseconds: 100, SampleIntervalMilliseconds: 500, JobPoolLimit: 10, SystemPoolLimit: 5},
		Thresholds: jobShardedLoadThresholds{ShardStartP95MS: 15000, ShardStartP99MS: 20000, OrdinaryStartP99MS: 10000, AllSuccessMS: 30000},
		Schedulers: 2, Executors: 2, PerExecutorConcurrency: 32, PerExecutorWaiting: 128,
		ExecutorNames: map[string]string{x.appName: "system-load-a", y.appName: "system-load-b"},
	}
	t.Cleanup(func() {
		report.Common.Resources = parseJobLoadResources(f.processes)
		report.Common.WorkloadPassed = report.Common.WorkloadPassed && !t.Failed()
		writeJobLoadJSON(t, &report)
	})
	for _, scenario := range []jobShardedLoadScenario{
		{Name: "one-instance-500-shards", ShardedInstances: 1, Shards: 500, PayloadBytes: 64},
		{Name: "100-instances-10-shards-mixed-100-ordinary", ShardedInstances: 100, Shards: 10, OrdinaryInstances: 100, PayloadBytes: 64},
	} {
		firstDue := time.Now().Truncate(time.Second).Add(30 * time.Second)
		cron := fmt.Sprintf("%d * * * * *", firstDue.Second())
		ids := createJobShardedLoadRules(t, f, a, scenario, cron)
		setJobLoadStatus(t, a, ids, 1)
		if time.Until(firstDue) < 5*time.Second {
			t.Fatal("load setup left less than five seconds before the first Cron wave")
		}
		if report.Common.FirstDueAt.IsZero() {
			report.Common.FirstDueAt = firstDue.UTC()
		}
		for wave := range 3 {
			due := firstDue.Add(time.Duration(wave) * time.Minute)
			jobLoadWait(t, f, &report.Common, names, due)
			instanceIDs, observed := waitJobShardedLoadWave(t, f, &report.Common, names, ids, due, scenario.ShardedInstances+scenario.OrdinaryInstances)
			measured := readJobShardedLoadWave(t, f.sqlDB, scenario, wave+1, instanceIDs, observed, false, report.ExecutorNames)
			report.Waves = append(report.Waves, measured)
			t.Logf("%s wave %d: shard start P95=%.1fms P99=%.1fms, ordinary P99=%.1fms, completion=%.1fms", scenario.Name, wave+1,
				measured.Stages["shard"].BusinessStart.P95, measured.Stages["shard"].BusinessStart.P99,
				measured.Stages["ordinary"].BusinessStart.P99, measured.AllSuccessVisibleMS)
			if measured.Stages["shard"].BusinessStart.P95 > report.Thresholds.ShardStartP95MS ||
				measured.Stages["shard"].BusinessStart.P99 > report.Thresholds.ShardStartP99MS ||
				measured.Stages["ordinary"].BusinessStart.P99 > report.Thresholds.OrdinaryStartP99MS ||
				measured.AllSuccessVisibleMS > report.Thresholds.AllSuccessMS {
				t.Fatal("sharded Cron wave exceeded an approved acceptance threshold")
			}
		}
		setJobLoadStatus(t, a, ids, 0)
		if !time.Now().Before(firstDue.Add(3 * time.Minute)) {
			t.Fatal("load deactivation crossed the unmeasured fourth Cron wave")
		}
		if count := jobLoadCount(t, f.sqlDB, "SELECT count(*) FROM job_instance WHERE job_id = ANY($1)", ids); count != 3*len(ids) {
			t.Fatalf("Cron produced %d Instances, want %d", count, 3*len(ids))
		}
	}
	runJobShardedLoadExtras(t, f, a, &report, names)
	jobLoadWait(t, f, &report.Common, names, time.Now().Add(5*time.Second))
	report.Common.CompletedAt = time.Now().UTC()
	report.Common.Resources = parseJobLoadResources(f.processes)
	assertJobLoadResourceWindow(t, &report.Common, "job-load-a", "job-load-b", "system-load-a", "system-load-b")
	report.Common.WorkloadPassed = true
}

type jobShardedLoadThresholds struct {
	ShardStartP95MS, ShardStartP99MS, OrdinaryStartP99MS, AllSuccessMS float64
}

type jobShardedLoadReport struct {
	Common                                                            jobLoadReport
	Thresholds                                                        jobShardedLoadThresholds
	Schedulers, Executors, PerExecutorConcurrency, PerExecutorWaiting int
	ExecutorNames                                                     map[string]string
	Waves                                                             []jobShardedLoadWave
}

type jobShardedLoadScenario struct {
	Name                                                      string
	ShardedInstances, Shards, OrdinaryInstances, PayloadBytes int
}

type jobShardedLoadStage struct {
	Count, ResultSavedSamples                                                                        int
	BusinessStart, TaskCreatedToBusiness, AttemptRegisteredToBusiness, BusinessDuration, ResultSaved jobLoadDistribution
}

type jobShardedLoadWave struct {
	Scenario                                                                         jobShardedLoadScenario
	Number, Instances, Tasks, BusinessCalls                                          int
	Stopped                                                                          bool
	AllSuccessVisibleMS                                                              float64
	Stages                                                                           map[string]jobShardedLoadStage
	PreResultToShardCreation, ShardResultsToPostCreation, LastResultToParentFinished jobLoadDistribution
	ExecutorCalls                                                                    map[string]int
	TaskStatuses                                                                     map[int]int
	PreparedReportBytes                                                              int
	DispatchAttempts, RepeatedDispatchTasks                                          int64
	PreparationBuild                                                                 jobLoadDistribution
}

func createJobShardedLoadRules(t testing.TB, f *jobLifecycleFixture, node *jobLifecycleNode, scenario jobShardedLoadScenario, cron string) []int64 {
	t.Helper()
	ids := make([]int64, 0, scenario.ShardedInstances+scenario.OrdinaryInstances)
	for index := range scenario.ShardedInstances + scenario.OrdinaryInstances {
		mode, count, processor, post := int32(2), scenario.Shards, "system.e2e.load.sharded", true
		if index >= scenario.ShardedInstances {
			mode, count, processor, post = 1, 0, "system.e2e.load.ordinary", false
		}
		params := fmt.Sprintf(`{"count":%d,"payload_bytes":%d,"delay_ms":100}`, count, scenario.PayloadBytes)
		disabled, retries, interval, skip, parallel := int32(0), int32(0), int32(1), int32(0), int32(1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		created, err := node.client.CreateJob(ctx, &job.CreateJobRequest{Status: &disabled, Config: &job.JobInput{
			Name: fmt.Sprintf("%s-%03d", scenario.Name, index), ExecutorGroupId: f.group.Id, ProcessorName: processor,
			ExecutionMode: &mode, PostEnabled: &post, Params: params, CronExpr: cron,
			MisfirePolicy: &skip, BlockPolicy: &parallel, MaxRetries: &retries, RetryIntervalSeconds: &interval, TimeoutSeconds: 120,
		}})
		cancel()
		if err != nil {
			t.Fatalf("create sharded load rule: %v", err)
		}
		ids = append(ids, created.Id)
	}
	return ids
}

func waitJobShardedLoadWave(t testing.TB, f *jobLifecycleFixture, report *jobLoadReport, names map[string]string, jobs []int64, due time.Time, expected int) ([]int64, time.Time) {
	t.Helper()
	deadline := due.Add(30 * time.Second)
	for {
		sampleJobLoad(t, f, report, names)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		rows, err := f.sqlDB.QueryContext(ctx, "SELECT id, status FROM job_instance WHERE job_id = ANY($1) AND scheduled_at = $2", jobs, due)
		if err != nil {
			cancel()
			t.Fatalf("observe sharded Cron wave: %v", err)
		}
		var ids []int64
		successes := 0
		for rows.Next() {
			var id int64
			var status int
			if err := rows.Scan(&id, &status); err != nil {
				t.Fatal(err)
			}
			if status == 4 || status == 5 {
				t.Fatalf("sharded load Instance %d ended with status %d", id, status)
			}
			if status == 3 {
				successes++
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		_ = rows.Close()
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		observed := time.Now()
		if len(ids) == expected && successes == expected {
			return ids, observed
		}
		if len(ids) > expected || !observed.Before(deadline) {
			t.Fatalf("Cron wave has %d Instances and %d/%d successes at its approved deadline", len(ids), successes, expected)
		}
		time.Sleep(min(500*time.Millisecond, time.Until(deadline)))
	}
}

func runJobShardedLoadExtras(t testing.TB, f *jobLifecycleFixture, node *jobLifecycleNode, report *jobShardedLoadReport, names map[string]string) {
	t.Helper()
	for _, stopped := range []bool{false, true} {
		scenario := jobShardedLoadScenario{Name: "near-report-limit", ShardedInstances: 1, Shards: 500, PayloadBytes: 8000}
		if stopped {
			scenario.Name, scenario.PayloadBytes = "stop-result-competition", 64
		}
		ids := createJobShardedLoadRules(t, f, node, scenario, "0 0 0 1 1 *")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		overridden := false
		triggered, err := node.client.TriggerJob(ctx, &job.TriggerJobRequest{Id: ids[0], RequestId: uuid.NewString(), ParamsOverridden: &overridden, OperatorId: 1})
		cancel()
		if err != nil {
			t.Fatalf("trigger extra sharded workload: %v", err)
		}
		deadline, stopIssued := time.Now().Add(30*time.Second), false
		for {
			sampleJobLoad(t, f, &report.Common, names)
			if stopped && !stopIssued {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				var completed, running int
				err := f.sqlDB.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE state = 'succeeded'), count(*) FILTER (WHERE state = 'running')
					FROM system_job_e2e_records WHERE instance_id = $1 AND value::jsonb->>'Stage' = 'shard'`, triggered.InstanceId).Scan(&completed, &running)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				if completed > 0 && running > 0 {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					response, err := node.client.StopInstance(ctx, &job.StopInstanceRequest{Id: triggered.InstanceId})
					cancel()
					if err != nil || response.GetStatus() != 6 {
						t.Fatalf("record concurrent load stop: %v", err)
					}
					stopIssued = true
				}
			}
			instance := readJobShardedInstance(t, node, triggered.InstanceId)
			if instance.Status >= 3 && instance.Status <= 5 {
				if stopped && (!stopIssued || instance.Status != 5) || !stopped && instance.Status != 3 {
					t.Fatalf("extra workload ended unexpectedly: stopped=%t status=%d", stopped, instance.Status)
				}
				// A parent terminal state does not prove business has exited. Observe
				// cooperative cancellation separately before reading final audit facts.
				if jobLoadCount(t, f.sqlDB, "SELECT count(*) FROM system_job_e2e_records WHERE instance_id = $1 AND state = 'running'", triggered.InstanceId) == 0 {
					break
				}
			}
			if !time.Now().Before(deadline) {
				t.Fatal("extra sharded workload exceeded its bounded observation window")
			}
			time.Sleep(100 * time.Millisecond)
		}
		measured := readJobShardedLoadWave(t, f.sqlDB, scenario, 1, []int64{triggered.InstanceId}, time.Now(), stopped, report.ExecutorNames)
		if !stopped && (measured.PreparedReportBytes < 15*(1<<18) || measured.PreparedReportBytes >= 4*(1<<20)) {
			t.Fatalf("near-limit encoded report size = %d, want [3.75 MiB, 4 MiB)", measured.PreparedReportBytes)
		}
		report.Waves = append(report.Waves, measured)
	}
}

type jobShardedLoadFact struct {
	Stage, Worker, ParamsHash, ItemHash    string
	Index, PayloadBytes, PreparedWireBytes int
	Total, Succeeded, Failed               int64
	PrepareBuildMilliseconds               float64
}

type jobShardedLoadInstanceTimes struct {
	preResult, firstShard, lastShard, postCreated, lastResult, finished time.Time
	shards                                                              map[int]bool
	counts                                                              map[int]int
}

// readJobShardedLoadWave performs one bounded observation query for the completed
// wave. Event timestamps measure stages and shared-check handoffs, not exact commit
// times. Only fingerprints are stored by business, including the near-4MiB case.
func readJobShardedLoadWave(t testing.TB, db *sql.DB, scenario jobShardedLoadScenario, number int, ids []int64, observed time.Time, stopped bool, workers map[string]string) jobShardedLoadWave {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT i.id, i.job_id, i.status, i.end_reason,
		coalesce(i.scheduled_at, i.created_at), i.finished_at, i.execution_config->>'params',
		t.id, t.task_type, t.status, t.attempt_no, t.used_attempts, t.created_at, t.attempt_registered_at, t.finished_at,
		t.input_data::text, CASE WHEN t.task_type = 2 AND t.status = 4 THEN jsonb_array_length(t.result_data) ELSE 0 END,
		(t.executor_instance_id IS NULL) = (t.executor_address IS NULL),
		b.task_id, b.job_id, b.instance_id, b.value, b.state, b.started_at, b.finished_at
		FROM job_instance i JOIN job_task t ON t.instance_id = i.id
		LEFT JOIN system_job_e2e_records b ON b.task_id = t.id
		WHERE i.id = ANY($1) ORDER BY i.id, t.id, b.invocation_no`, ids)
	if err != nil {
		t.Fatalf("read sharded load facts: %v", err)
	}
	defer func() { _ = rows.Close() }()
	result := jobShardedLoadWave{Scenario: scenario, Number: number, Instances: len(ids), Stopped: stopped,
		Stages: make(map[string]jobShardedLoadStage), ExecutorCalls: make(map[string]int), TaskStatuses: make(map[int]int)}
	instances := make(map[int64]*jobShardedLoadInstanceTimes)
	seenTasks := make(map[int64]bool)
	stageValues := make(map[string][5][]float64)
	var preparationBuild []float64
	for rows.Next() {
		var instanceID, jobID, taskID, attempt int64
		var parentStatus, kind, taskStatus, used, preparedCount int
		var parentReason, params string
		var due, parentFinished, created, finished time.Time
		var registered sql.NullTime
		var input sql.NullString
		var bindingValid bool
		var businessTask, businessJob, businessInstance sql.NullInt64
		var value, state sql.NullString
		var businessStarted, businessFinished sql.NullTime
		if err := rows.Scan(&instanceID, &jobID, &parentStatus, &parentReason, &due, &parentFinished, &params,
			&taskID, &kind, &taskStatus, &attempt, &used, &created, &registered, &finished, &input, &preparedCount, &bindingValid,
			&businessTask, &businessJob, &businessInstance, &value, &state, &businessStarted, &businessFinished); err != nil {
			t.Fatal(err)
		}
		if seenTasks[taskID] {
			t.Fatal("a Task produced duplicate actual business calls across executor processes")
		}
		seenTasks[taskID] = true
		result.TaskStatuses[taskStatus]++
		result.DispatchAttempts += attempt
		if attempt > 1 {
			result.RepeatedDispatchTasks++
		}
		result.AllSuccessVisibleMS = max(result.AllSuccessVisibleMS, observed.Sub(due).Seconds()*1000)
		// Explicit BUSY may return the execution opportunity while preserving an
		// increasing dispatch identity. Only actual business calls must be unique.
		if !bindingValid || !stopped && (parentStatus != 3 || parentReason != "success" || taskStatus != 4 || attempt < 1 || used != 1) ||
			stopped && (parentStatus != 5 || parentReason != "manual_stop" || taskStatus < 4 || taskStatus > 6 || attempt < 0 || used < 0 || used > 1 || int64(used) > attempt) {
			t.Fatalf("invalid load terminal facts: instance=%d parent=%d task=%d status=%d attempt=%d", instanceID, parentStatus, taskID, taskStatus, attempt)
		}
		item := instances[instanceID]
		if item == nil {
			item = &jobShardedLoadInstanceTimes{finished: parentFinished, shards: make(map[int]bool), counts: make(map[int]int)}
			instances[instanceID] = item
		}
		item.counts[kind]++
		if finished.After(item.lastResult) {
			item.lastResult = finished
		}
		switch kind {
		case 2:
			item.preResult = finished
		case 3:
			if item.firstShard.IsZero() || created.Before(item.firstShard) {
				item.firstShard = created
			}
			if finished.After(item.lastShard) {
				item.lastShard = finished
			}
		case 4:
			item.postCreated = created
		}
		if !businessTask.Valid {
			if !stopped || taskStatus != 6 {
				t.Fatal("a successful Task has no actual business invocation")
			}
			continue
		}
		result.BusinessCalls++
		var fact jobShardedLoadFact
		if err := json.Unmarshal([]byte(value.String), &fact); err != nil {
			t.Fatal(err)
		}
		paramsHash := sha256.Sum256([]byte(params))
		if businessTask.Int64 != taskID || businessJob.Int64 != jobID || businessInstance.Int64 != instanceID ||
			fact.ParamsHash != hex.EncodeToString(paramsHash[:]) || !businessStarted.Valid || !businessFinished.Valid || !registered.Valid ||
			!stopped && state.String != "succeeded" || stopped && state.String != "succeeded" && state.String != "canceled" {
			t.Fatal("load business identity, frozen parameters or completion differs from scheduler facts")
		}
		worker, known := workers[fact.Worker]
		if !known {
			t.Fatal("load business came from an unrecognized executor process")
		}
		result.ExecutorCalls[worker]++
		expectedStage := map[int]string{1: "ordinary", 2: "pre", 3: "shard", 4: "post"}[kind]
		if fact.Stage != expectedStage {
			t.Fatal("load Task invoked the wrong processor stage")
		}
		if kind == 2 {
			if preparedCount != scenario.Shards {
				t.Fatal("preparation did not persist the full approved shard count")
			}
			result.PreparedReportBytes = max(result.PreparedReportBytes, fact.PreparedWireBytes)
			preparationBuild = append(preparationBuild, fact.PrepareBuildMilliseconds)
		}
		if kind == 3 {
			if fact.Index < 0 || fact.Index >= scenario.Shards || item.shards[fact.Index] || fact.PayloadBytes != scenario.PayloadBytes || !input.Valid {
				t.Fatal("business shard identity was repeated or changed")
			}
			item.shards[fact.Index] = true
			expected := fmt.Sprintf(`{"index":%d,"payload":%q}`, fact.Index, strings.Repeat("x", scenario.PayloadBytes))
			if fact.ItemHash != jobShardedLoadHash(t, []byte(input.String)) || fact.ItemHash != jobShardedLoadHash(t, []byte(expected)) {
				t.Fatal("persisted shard input and actual business input differ from preparation")
			}
		}
		if kind == 4 {
			var fixed struct{ Total, Succeeded, Failed int64 }
			if !input.Valid || json.Unmarshal([]byte(input.String), &fixed) != nil || fact.Total != int64(scenario.Shards) ||
				fact.Succeeded != fact.Total || fact.Failed != 0 || fixed.Total != fact.Total || fixed.Succeeded != fact.Succeeded || fixed.Failed != fact.Failed {
				t.Fatal("Post fixed statistics differ from the actual shard outcomes")
			}
		}
		values := stageValues[fact.Stage]
		for index, elapsed := range []time.Duration{businessStarted.Time.Sub(due), businessStarted.Time.Sub(created), businessStarted.Time.Sub(registered.Time), businessFinished.Time.Sub(businessStarted.Time)} {
			values[index] = append(values[index], elapsed.Seconds()*1000)
		}
		// A canceled Task's finished_at records database cancellation, not a
		// saved processor result; business may legitimately exit afterward.
		if taskStatus != 6 {
			values[4] = append(values[4], finished.Sub(businessFinished.Time).Seconds()*1000)
		}
		stageValues[fact.Stage] = values
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	result.Tasks = len(seenTasks)
	wantTasks := scenario.ShardedInstances*(scenario.Shards+2) + scenario.OrdinaryInstances
	if stopped {
		wantTasks = scenario.Shards + 1
	}
	if result.Tasks != wantTasks || !stopped && result.BusinessCalls != wantTasks {
		t.Fatalf("load cardinality tasks=%d business=%d expected=%d", result.Tasks, result.BusinessCalls, wantTasks)
	}
	var preToShard, shardToPost, finalization []float64
	ordinary, sharded := 0, 0
	for _, item := range instances {
		if item.counts[1] == 1 {
			ordinary++
			if len(item.counts) != 1 {
				t.Fatal("ordinary load Instance has sharded stages")
			}
		} else {
			sharded++
			wantPost := 1
			if stopped {
				wantPost = 0
			}
			if item.counts[2] != 1 || item.counts[3] != scenario.Shards || item.counts[4] != wantPost || !stopped && len(item.shards) != scenario.Shards {
				t.Fatal("sharded load Instance has an incomplete or duplicated stage set")
			}
			preToShard = append(preToShard, item.firstShard.Sub(item.preResult).Seconds()*1000)
			if !stopped {
				shardToPost = append(shardToPost, item.postCreated.Sub(item.lastShard).Seconds()*1000)
			}
		}
		finalization = append(finalization, item.finished.Sub(item.lastResult).Seconds()*1000)
	}
	if sharded != scenario.ShardedInstances || ordinary != scenario.OrdinaryInstances {
		t.Fatal("load Instance mode counts differ from configured workload")
	}
	for _, name := range workers {
		if result.ExecutorCalls[name] == 0 {
			t.Fatalf("executor %s did not participate in this load wave", name)
		}
	}
	for stage, values := range stageValues {
		result.Stages[stage] = jobShardedLoadStage{Count: len(values[0]), ResultSavedSamples: len(values[4]), BusinessStart: jobLoadPercentiles(values[0]),
			TaskCreatedToBusiness: jobLoadPercentiles(values[1]), AttemptRegisteredToBusiness: jobLoadPercentiles(values[2]),
			BusinessDuration: jobLoadPercentiles(values[3]), ResultSaved: jobShardedLoadDistribution(values[4])}
	}
	result.PreparationBuild = jobShardedLoadDistribution(preparationBuild)
	result.PreResultToShardCreation, result.ShardResultsToPostCreation, result.LastResultToParentFinished =
		jobShardedLoadDistribution(preToShard), jobShardedLoadDistribution(shardToPost), jobShardedLoadDistribution(finalization)
	return result
}

func jobShardedLoadDistribution(values []float64) jobLoadDistribution {
	if len(values) == 0 {
		return jobLoadDistribution{}
	}
	return jobLoadPercentiles(values)
}

func jobShardedLoadHash(t testing.TB, value []byte) string {
	t.Helper()
	canonical := canonicalJobShardedJSON(t, value)
	hash := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(hash[:])
}
