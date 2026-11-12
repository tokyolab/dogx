//go:build integration && e2e && load

package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
)

// TestJobOrdinaryLoadEndToEnd is an opt-in, machine-dependent acceptance run.
// It keeps production queue and worker defaults, uses real Cron callbacks, and
// modifies Job data only through public management RPCs. Database reads below
// observe durable outcomes; recorded row timestamps are not commit timestamps.
func TestJobOrdinaryLoadEndToEnd(t *testing.T) {
	f := newJobLifecycleFixture(t)
	f.jobConfigTransform = func(config string) string {
		const fastScan = "\nDispatch:\n  ScanInterval: 100ms\n"
		if strings.Count(config, fastScan) != 1 {
			t.Fatal("load fixture cannot identify the accelerated dispatch scan override")
		}
		return strings.Replace(config, fastScan, "\n", 1)
	}
	a := f.startJob(t, "job-load-a")
	f.createGroup(t, a)
	b := f.startJob(t, "job-load-b")
	system := f.startSystem(t, "system-load", true)
	processNames := map[string]string{a.appName: "job-load-a", b.appName: "job-load-b", system.appName: "system-load"}
	report := jobLoadReport{StartedAt: time.Now().UTC(), Rules: 500, DuePerWave: 100,
		WaveCount: 5, WaveIntervalSeconds: 30, BusinessDelayMilliseconds: 100,
		SampleIntervalMilliseconds: 500, JobPoolLimit: 10, SystemPoolLimit: 5}
	t.Cleanup(func() { writeJobLoadReport(t, &report, f.processes) })
	ids := createJobLoadRules(t, f, a)

	// Enable within one 30-second window. No load-producing rule is active during setup, and
	// the assertion prevents setup timing from silently splitting the first wave.
	enableAt := time.Now().Truncate(30 * time.Second).Add(31 * time.Second)
	jobLoadWait(t, f, &report, processNames, enableAt)
	firstDue := enableAt.Truncate(30 * time.Second).Add(30 * time.Second)
	setJobLoadStatus(t, a, ids, 1)
	if time.Until(firstDue) < 5*time.Second {
		t.Fatal("rule activation left less than five seconds before the first wave")
	}
	enabled := jobLoadCount(t, f.sqlDB, "SELECT count(*) FROM job_definition WHERE status = 1 AND deleted_at IS NULL")
	if enabled != 500 {
		t.Fatalf("enabled rules = %d, want 500", enabled)
	}
	report.FirstDueAt = firstDue.UTC()
	t.Logf("load ready: 500 enabled rules, first wave %s, five waves of 100, business delay 100ms", firstDue.Format(time.RFC3339))
	for wave := range 5 {
		due := firstDue.Add(time.Duration(wave) * 30 * time.Second)
		jobLoadWait(t, f, &report, processNames, due)
		deadline := due.Add(10 * time.Second)
		for {
			sampleJobLoad(t, f, &report, processNames)
			succeeded := jobLoadCount(t, f.sqlDB, "SELECT count(*) FROM job_instance WHERE scheduled_at = $1 AND status = 3", due)
			observedAt := time.Now()
			if succeeded == 100 {
				metrics := readJobLoadWave(t, f.sqlDB, wave+1, due, observedAt)
				report.Waves = append(report.Waves, metrics)
				t.Logf("wave %d: start P95=%.1fms P99=%.1fms, all parent successes visible after %.1fms", wave+1,
					metrics.BusinessStart.P95, metrics.BusinessStart.P99, metrics.AllSuccessVisibleMilliseconds)
				if metrics.BusinessStart.P95 > 3000 || metrics.BusinessStart.P99 > 5000 || observedAt.After(deadline) {
					t.Fatalf("wave %d exceeded its predeclared latency threshold", wave+1)
				}
				break
			}
			if succeeded > 100 || !observedAt.Before(deadline) {
				t.Fatalf("wave %d has %d/100 successful parents at its ten-second deadline", wave+1, succeeded)
			}
			time.Sleep(min(500*time.Millisecond, time.Until(deadline)))
		}
	}
	setJobLoadStatus(t, a, ids, 0)
	if time.Now().After(firstDue.Add(150 * time.Second)) {
		t.Fatal("rule deactivation exceeded the sixth-wave boundary")
	}
	// Observe an idle tail after deactivation, retaining the real runtime's GC
	// and housekeeping behavior rather than forcing collection or process exit.
	jobLoadWait(t, f, &report, processNames, time.Now().Add(10*time.Second))
	assertJobLoadTotals(t, f.sqlDB)
	report.CompletedAt = time.Now().UTC()
	report.Resources = parseJobLoadResources(f.processes)
	assertJobLoadResourceWindow(t, &report)
	report.WorkloadPassed = true
}

type jobLoadReport struct {
	StartedAt, CompletedAt, FirstDueAt time.Time
	// WorkloadPassed covers the measured workload and observed assertions.
	// The final test exit status also covers subsequent process cleanup checks.
	WorkloadPassed                bool
	Rules, DuePerWave, WaveCount  int
	WaveIntervalSeconds           int
	BusinessDelayMilliseconds     int
	SampleIntervalMilliseconds    int
	JobPoolLimit, SystemPoolLimit int
	Waves                         []jobLoadWave
	Samples                       []jobLoadSample
	Resources                     map[string][]jobLoadResource
}

type jobLoadDistribution struct {
	P50, P95, P99, Max float64
}

type jobLoadWave struct {
	Number, Count                    int
	BoundTasks, UnboundTerminalTasks int
	ScheduledAt                      time.Time
	AllSuccessVisibleMilliseconds    float64
	// These distributions use database-recorded event times, not visibility or
	// physical commit times. BusinessStart is the processor's first business insert.
	InstanceCreated, AttemptRegistered, BusinessStart jobLoadDistribution
	BusinessDuration, ResultSaved, ParentFinished     jobLoadDistribution
}

type jobLoadSample struct {
	At                                               time.Time
	WaitingInstances, RunningInstances               int
	UnfinishedTasks, RunningBusiness, UnsavedResults int
	Connections                                      map[string]jobLoadConnections
}

type jobLoadConnections struct {
	Total, Active, LockWaiting int
	OldestActiveMilliseconds   float64
}

type jobLoadResource struct {
	Sample         bool      `json:"load_resource_sample"`
	At             time.Time `json:"at"`
	Goroutines     int       `json:"goroutines"`
	HeapAllocBytes uint64    `json:"heap_alloc_bytes"`
	HeapSysBytes   uint64    `json:"heap_sys_bytes"`
}

func createJobLoadRules(t testing.TB, f *jobLifecycleFixture, node *jobLifecycleNode) []int64 {
	t.Helper()
	future := time.Now().AddDate(0, 0, 7)
	futureCron := fmt.Sprintf("0 0 0 %d %d *", future.Day(), future.Month())
	var ids []int64
	for index := range 500 {
		status, cron := int32(1), futureCron
		if index >= 400 {
			status, cron = 0, "0,30 * * * * *"
		}
		normal, retries, interval, skip, parallel := int32(1), int32(0), int32(1), int32(0), int32(1)
		post := false
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		created, err := node.client.CreateJob(ctx, &job.CreateJobRequest{Status: &status, Config: &job.JobInput{
			Name: fmt.Sprintf("load-%03d", index), ExecutorGroupId: f.group.Id, ProcessorName: "system.e2e.persist",
			ExecutionMode: &normal, PostEnabled: &post, Params: `{"mode":"load","value":"load","delay_ms":100}`,
			CronExpr: cron, MisfirePolicy: &skip, BlockPolicy: &parallel,
			MaxRetries: &retries, RetryIntervalSeconds: &interval, TimeoutSeconds: 120,
		}})
		cancel()
		if err != nil {
			t.Fatalf("create load rule %d: %v", index, err)
		}
		if index >= 400 {
			ids = append(ids, created.Id)
		}
	}
	return ids
}

func setJobLoadStatus(t testing.TB, node *jobLifecycleNode, ids []int64, status int32) {
	t.Helper()
	for _, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := node.client.UpdateJobStatus(ctx, &job.UpdateJobStatusRequest{Id: id, Status: &status})
		cancel()
		if err != nil {
			t.Fatalf("set load rule %d status: %v", id, err)
		}
	}
}

func jobLoadWait(t testing.TB, f *jobLifecycleFixture, report *jobLoadReport, names map[string]string, until time.Time) {
	t.Helper()
	for time.Now().Before(until) {
		sampleJobLoad(t, f, report, names)
		time.Sleep(min(500*time.Millisecond, time.Until(until)))
	}
}

func sampleJobLoad(t testing.TB, f *jobLifecycleFixture, report *jobLoadReport, names map[string]string) {
	t.Helper()
	for _, process := range f.processes {
		select {
		case <-process.done:
			t.Fatalf("load process %s exited unexpectedly", process.name)
		default:
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sample := jobLoadSample{At: time.Now().UTC(), Connections: make(map[string]jobLoadConnections)}
	err := f.sqlDB.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM job_instance WHERE status = 1),
		(SELECT count(*) FROM job_instance WHERE status = 2),
		(SELECT count(*) FROM job_task WHERE status IN (1, 2, 3)),
		(SELECT count(*) FROM system_job_e2e_records WHERE state = 'running'),
		(SELECT count(*) FROM system_job_e2e_records b JOIN job_task t ON t.id = b.task_id
		 WHERE b.state = 'succeeded' AND t.status <> 4)`).Scan(&sample.WaitingInstances, &sample.RunningInstances,
		&sample.UnfinishedTasks, &sample.RunningBusiness, &sample.UnsavedResults)
	if err != nil {
		t.Fatalf("sample load backlog: %v", err)
	}
	rows, err := f.sqlDB.QueryContext(ctx, `SELECT application_name, count(*),
		count(*) FILTER (WHERE state = 'active'), count(*) FILTER (WHERE wait_event_type = 'Lock'),
		coalesce(max(extract(epoch FROM clock_timestamp() - query_start) * 1000) FILTER (WHERE state = 'active'), 0)
		FROM pg_stat_activity WHERE application_name = ANY($1) GROUP BY application_name`, mapJobLoadNames(names))
	if err != nil {
		t.Fatalf("sample load database connections: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		var counts jobLoadConnections
		if err := rows.Scan(&name, &counts.Total, &counts.Active, &counts.LockWaiting, &counts.OldestActiveMilliseconds); err != nil {
			t.Fatal(err)
		}
		sample.Connections[names[name]] = counts
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	report.Samples = append(report.Samples, sample)
}

func mapJobLoadNames(names map[string]string) []string {
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	return result
}

func readJobLoadWave(t testing.TB, db *sql.DB, number int, due, observed time.Time) jobLoadWave {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT
		extract(epoch FROM i.created_at - i.scheduled_at) * 1000,
		extract(epoch FROM t.attempt_registered_at - i.scheduled_at) * 1000,
		extract(epoch FROM b.started_at - i.scheduled_at) * 1000,
		extract(epoch FROM b.finished_at - b.started_at) * 1000,
		extract(epoch FROM t.finished_at - b.finished_at) * 1000,
		extract(epoch FROM i.finished_at - t.finished_at) * 1000,
		t.id,
		i.status = 3 AND i.trigger_type = 1 AND i.end_reason = 'success',
		t.task_type = 1 AND t.status = 4 AND t.attempt_no = 1 AND t.used_attempts = 1 AND t.end_reason = 'success',
		(t.executor_instance_id IS NULL) = (t.executor_address IS NULL),
		b.invocation_no = 1 AND b.state = 'succeeded' AND b.instance_id = i.id AND b.job_id = i.job_id AND b.value = 'load',
		t.executor_instance_id IS NOT NULL
		FROM job_instance i JOIN job_task t ON t.instance_id = i.id
		JOIN system_job_e2e_records b ON b.task_id = t.id WHERE i.scheduled_at = $1`, due)
	if err != nil {
		t.Fatalf("read load wave timing: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var values [6][]float64
	count, bound := 0, 0
	for rows.Next() {
		var row [6]float64
		var taskID int64
		var instanceValid, taskValid, bindingValid, businessValid, hasBinding bool
		if err := rows.Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &taskID,
			&instanceValid, &taskValid, &bindingValid, &businessValid, &hasBinding); err != nil {
			t.Fatal(err)
		}
		if !instanceValid || !taskValid || !bindingValid || !businessValid {
			t.Fatalf("load wave %d task %d failed invariants: instance=%t task=%t binding=%t business=%t",
				number, taskID, instanceValid, taskValid, bindingValid, businessValid)
		}
		// A result can commit before its ACK binding. The late ACK then leaves
		// both binding fields NULL because a terminal Task must not be rewritten.
		if hasBinding {
			bound++
		}
		for index, value := range row {
			values[index] = append(values[index], value)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("load wave %d joined records = %d, want exactly 100", number, count)
	}
	return jobLoadWave{Number: number, Count: count, BoundTasks: bound, UnboundTerminalTasks: count - bound,
		ScheduledAt: due.UTC(), AllSuccessVisibleMilliseconds: observed.Sub(due).Seconds() * 1000,
		InstanceCreated: jobLoadPercentiles(values[0]), AttemptRegistered: jobLoadPercentiles(values[1]),
		BusinessStart: jobLoadPercentiles(values[2]), BusinessDuration: jobLoadPercentiles(values[3]),
		ResultSaved: jobLoadPercentiles(values[4]), ParentFinished: jobLoadPercentiles(values[5])}
}

func jobLoadPercentiles(values []float64) jobLoadDistribution {
	sort.Float64s(values)
	quantile := func(fraction float64) float64 { return values[int(math.Ceil(float64(len(values))*fraction))-1] }
	return jobLoadDistribution{P50: quantile(.50), P95: quantile(.95), P99: quantile(.99), Max: values[len(values)-1]}
}

func assertJobLoadTotals(t testing.TB, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var instances, tasks, business, future, distinctJobs, uniqueInstances, uniqueTasks int
	var invalidInstances, invalidTasks, invalidBusiness, duplicateSchedules, incorrectWaveCounts int
	err := db.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM job_instance), (SELECT count(*) FROM job_task),
		(SELECT count(*) FROM system_job_e2e_records),
		(SELECT count(*) FROM job_instance i JOIN job_definition j ON j.id = i.job_id WHERE j.name < 'load-400'),
		(SELECT count(DISTINCT job_id) FROM job_instance),
		(SELECT count(DISTINCT instance_id) FROM job_task),
		(SELECT count(DISTINCT task_id) FROM system_job_e2e_records),
		(SELECT count(*) FROM job_instance WHERE status <> 3 OR end_reason IS DISTINCT FROM 'success'
		 OR scheduled_at IS NULL OR finished_at IS NULL OR trigger_type <> 1),
		(SELECT count(*) FROM job_task WHERE status <> 4 OR end_reason IS DISTINCT FROM 'success'
		 OR finished_at IS NULL OR task_type <> 1 OR attempt_no <> 1 OR used_attempts <> 1
		 OR (executor_instance_id IS NULL) <> (executor_address IS NULL)),
		(SELECT count(*) FROM system_job_e2e_records WHERE state <> 'succeeded'
		 OR invocation_no <> 1 OR finished_at IS NULL),
		(SELECT count(*) FROM (SELECT job_id, scheduled_at FROM job_instance
		 GROUP BY job_id, scheduled_at HAVING count(*) <> 1) duplicates),
		(SELECT count(*) FROM (SELECT job_id FROM job_instance GROUP BY job_id HAVING count(*) <> 5) incomplete)
		`).Scan(&instances, &tasks, &business, &future, &distinctJobs, &uniqueInstances, &uniqueTasks,
		&invalidInstances, &invalidTasks, &invalidBusiness, &duplicateSchedules, &incorrectWaveCounts)
	if err != nil {
		t.Fatalf("read final load counts: %v", err)
	}
	if instances != 500 || tasks != 500 || business != 500 || future != 0 || distinctJobs != 100 || uniqueInstances != 500 || uniqueTasks != 500 {
		t.Fatalf("unexpected load totals: instances=%d tasks=%d business=%d future=%d jobs=%d unique_instances=%d unique_tasks=%d",
			instances, tasks, business, future, distinctJobs, uniqueInstances, uniqueTasks)
	}
	if invalidInstances != 0 || invalidTasks != 0 || invalidBusiness != 0 || duplicateSchedules != 0 || incorrectWaveCounts != 0 {
		t.Fatalf("invalid final load outcomes: instances=%d tasks=%d business=%d duplicate_schedules=%d incorrect_wave_counts=%d",
			invalidInstances, invalidTasks, invalidBusiness, duplicateSchedules, incorrectWaveCounts)
	}
}

func jobLoadCount(t testing.TB, db *sql.DB, query string, args ...any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var count int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		t.Fatalf("read load count: %v", err)
	}
	return count
}

func writeJobLoadReport(t testing.TB, report *jobLoadReport, processes []*runningProcess) {
	t.Helper()
	report.Resources = parseJobLoadResources(processes)
	report.WorkloadPassed = report.WorkloadPassed && !t.Failed()
	writeJobLoadJSON(t, report)
}

func writeJobLoadJSON(t testing.TB, report any) {
	t.Helper()
	file := strings.TrimSpace(os.Getenv("DOGX_LOAD_REPORT"))
	if file == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		t.Errorf("create load report directory: %v", err)
		return
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Errorf("encode load report: %v", err)
		return
	}
	if err := os.WriteFile(file, append(encoded, '\n'), 0o600); err != nil {
		t.Errorf("write load report: %v", err)
	}
}

func parseJobLoadResources(processes []*runningProcess) map[string][]jobLoadResource {
	resources := make(map[string][]jobLoadResource)
	for _, process := range processes {
		for _, line := range strings.Split(process.logs.String(), "\n") {
			var sample jobLoadResource
			if json.Unmarshal([]byte(line), &sample) == nil && sample.Sample {
				resources[process.name] = append(resources[process.name], sample)
			}
		}
	}
	return resources
}

func assertJobLoadResourceWindow(t testing.TB, report *jobLoadReport, processNames ...string) {
	t.Helper()
	// Two sampling intervals allow normal ticker jitter while still requiring
	// measurements from the idle tail, not just old samples from process startup.
	tailStart := report.CompletedAt.Add(-2 * time.Duration(report.SampleIntervalMilliseconds) * time.Millisecond)
	if len(processNames) == 0 {
		processNames = []string{"job-load-a", "job-load-b", "system-load"}
	}
	for _, name := range processNames {
		var beforeFirstWave, atTail bool
		for _, sample := range report.Resources[name] {
			if sample.At.IsZero() || sample.Goroutines <= 0 || sample.HeapAllocBytes == 0 || sample.HeapSysBytes == 0 {
				t.Fatalf("process %s returned an incomplete load resource sample", name)
			}
			if !sample.At.Before(report.StartedAt) && sample.At.Before(report.FirstDueAt) {
				beforeFirstWave = true
			}
			if !sample.At.Before(tailStart) && !sample.At.After(report.CompletedAt) {
				atTail = true
			}
		}
		if !beforeFirstWave || !atTail {
			t.Fatalf("process %s lacks load resource samples: before_first_wave=%t idle_tail=%t", name, beforeFirstWave, atTail)
		}
	}
}
