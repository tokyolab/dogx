//go:build integration && e2e && load

package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
)

// TestJobHistoryLoadEndToEnd runs the approved historical-volume comparison.
// The explicit case selector permits sequential, separately retained reports;
// it never reduces the approved data volume, duration, or acceptance thresholds.
func TestJobHistoryLoadEndToEnd(t *testing.T) {
	name := os.Getenv("DOGX_HISTORY_LOAD_CASE")
	if name != "A" && name != "B" && name != "C" {
		t.Skip("set DOGX_HISTORY_LOAD_CASE to A, B, or C for the approved local acceptance")
	}
	if os.Getenv("DOGX_HISTORY_PG_LOG_DIR") == "" || os.Getenv("DOGX_LOAD_REPORT") == "" || os.Getenv("DOGX_LOAD_RESOURCES") != "1" {
		t.Fatal("history acceptance requires PostgreSQL CSV logs and an explicit report path")
	}
	f := newJobLifecycleFixture(t)
	f.jobConfigTransform = historyJobConfiguration(t, 365)
	bootstrap := f.startJob(t, "history-bootstrap")
	f.createGroup(t, bootstrap)
	report := jobHistoryLoadReport{Case: name, Schema: currentSchema(t, f.sqlDB), PreparedAt: time.Now().UTC()}
	t.Cleanup(func() {
		report.Resources = parseJobLoadResources(f.processes)
		report.Passed = report.Passed && !t.Failed()
		writeJobLoadJSON(t, &report)
		for _, p := range f.processes {
			if !strings.Contains(p.logs.String(), f.group.Token) {
				_ = os.WriteFile(filepath.Join(filepath.Dir(os.Getenv("DOGX_LOAD_REPORT")), name+"-"+p.name+".log"), []byte(p.logs.String()), 0o600)
			}
		}
	})
	report.Seed = seedJobHistoryLoad(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := f.sqlDB.ExecContext(ctx, `CREATE SEQUENCE system_job_load_invocations;
		ALTER TABLE system_job_e2e_records ALTER COLUMN invocation_no SET DEFAULT nextval('system_job_load_invocations')`)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	// A future bounded minute set prevents both setup executions and a 31st
	// unmeasured round. All online writes still go through management and Cron.
	report.FirstDueAt = time.Now().UTC().Truncate(time.Minute).Add(5 * time.Minute)
	var rules []jobHistoryRule
	if name != "A" {
		rules = createHistoryRules(t, f, bootstrap, report.FirstDueAt)
	}
	signalE2EProcess(t, bootstrap.process)
	if err := waitE2EProcessExit(t, bootstrap.process, 15*time.Second); err != nil {
		t.Fatal("bootstrap did not stop before the measured topology started")
	}
	f.waitNoDatabaseConnections(t, bootstrap.appName)
	if strings.Contains(bootstrap.process.logs.String(), f.group.Token) {
		t.Fatal("bootstrap exposed its group credential")
	}
	f.processes = nil
	if name == "A" {
		report.FirstDueAt = time.Now().UTC().Add(5 * time.Minute)
	}
	alignJobHistoryAges(t, f.sqlDB, report.FirstDueAt)
	// Remove tuples made dead by fixture timestamp alignment before every
	// comparison. No VACUUM is forced inside a measured online window.
	for _, table := range []string{"job_definition", "job_instance", "job_task", "system_job_history_manifest"} {
		ctx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		_, vacuumErr := f.sqlDB.ExecContext(ctx, "VACUUM (ANALYZE) "+table)
		stop()
		if vacuumErr != nil {
			t.Fatalf("prepare history statistics: %v", vacuumErr)
		}
	}
	report.InitialAudit = assertJobHistoryLoad(t, f.sqlDB, false)
	if t.Failed() {
		t.FailNow()
	}
	retention := 365
	if name == "C" {
		retention = 7
	}
	report.RetentionDays = retention
	f.jobConfigTransform = historyJobConfiguration(t, retention)
	// Only isolated Job sessions log successful SQL. The CSV trace gives the
	// actual cleanup cutoff without instrumenting or changing production code.
	for i, value := range f.environment {
		if strings.HasPrefix(value, "PGOPTIONS=") {
			f.environment[i] = value + " -c log_min_duration_statement=0"
		}
	}
	a := f.startJob(t, "history-job-a")
	b := f.startJob(t, "history-job-b")
	// Executor SQL is not needed to identify cleanup; avoid tracing business data.
	for i, value := range f.environment {
		f.environment[i] = strings.TrimSuffix(value, " -c log_min_duration_statement=0")
	}
	x := f.startSystemHost(t, "history-system-a", "TestJobShardedLoadHostProcess")
	y := f.startSystemHost(t, "history-system-b", "TestJobShardedLoadHostProcess")
	names := map[string]string{a.appName: a.process.name, b.appName: b.process.name, x.appName: x.process.name, y.appName: y.process.name}
	report.DatabaseApplications = names
	report.ExecutorNames = map[string]string{x.appName: x.process.name, y.appName: y.process.name}
	report.ProcessIDs = make(map[string]int)
	for _, p := range f.processes {
		report.ProcessIDs[p.name] = p.cmd.Process.Pid
	}
	writeJobLoadJSON(t, &report)
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	report.Environment, err = readJobHistoryDatabaseEnvironment(ctx, f.sqlDB, report.Schema)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	historyPlans(t, f, a, "before", report.FirstDueAt.Add(20*time.Minute-7*24*time.Hour))
	if time.Until(report.FirstDueAt) < 10*time.Second {
		t.Fatal("preparation missed the agreed activation margin; no measured run started")
	}
	if name != "A" {
		ids := historyRuleIDs(rules)
		setJobLoadStatus(t, a, ids, 1)
		if time.Until(report.FirstDueAt) < 5*time.Second {
			t.Fatal("rule activation missed the preparation margin")
		}
	}
	historyWait(t, f, report.FirstDueAt)
	// The first cleanup must have found no eligible history. Recheck this fact
	// before recording any acceptance window, rather than assuming startup speed.
	if jobLoadCount(t, f.sqlDB, "SELECT count(*) FROM job_instance WHERE id BETWEEN 1 AND 100000") != 100000 {
		t.Fatal("history changed before the measured window")
	}
	report.StartedAt = time.Now().UTC()
	duration := 30 * time.Minute
	if name == "A" {
		duration = 5 * time.Minute
	}
	report.WindowEnd = report.FirstDueAt.Add(duration)
	expected := historyExpected(rules, report.FirstDueAt)
	visible := make(map[string]jobHistoryVisibility)
	// Preserve observations even when a deadline assertion stops the case before
	// the final business audit. This cleanup runs before the report is written.
	t.Cleanup(func() { report.Visible = historyVisibilityValues(visible) })
	nextDatabase, nextProgress := time.Time{}, time.Time{}
	for time.Now().Before(report.WindowEnd) {
		historyAssertAlive(t, f)
		if name != "A" {
			observeHistoryOnline(t, f.sqlDB, rules, expected, visible, report.FirstDueAt)
		}
		if !time.Now().Before(nextDatabase) {
			report.DatabaseSamples = append(report.DatabaseSamples, historyDatabaseSample(t, f, report.Schema, names))
			nextDatabase = time.Now().Add(30 * time.Second)
		}
		if !time.Now().Before(nextProgress) {
			t.Logf("history %s: elapsed=%s successful_online_instances=%d/%d", name, time.Since(report.FirstDueAt).Round(time.Second), len(visible), len(expected))
			nextProgress = time.Now().Add(time.Minute)
		}
		time.Sleep(500 * time.Millisecond)
	}
	report.DatabaseSamples = append(report.DatabaseSamples, historyDatabaseSample(t, f, report.Schema, names))
	// Snapshot eligibility and deletion at the declared end, not after draining.
	remaining := jobLoadCount(t, f.sqlDB, "SELECT count(*) FROM job_instance WHERE id BETWEEN 1 AND 100000")
	wantRemaining := 100000
	if name == "C" {
		wantRemaining = 80000
	}
	if remaining != wantRemaining {
		t.Fatalf("history window ended with %d historical parents, want %d", remaining, wantRemaining)
	}
	if name != "A" {
		setJobLoadStatus(t, a, historyRuleIDs(rules), 0)
		deadline := report.WindowEnd.Add(time.Minute)
		for len(visible) != len(expected) && time.Now().Before(deadline) {
			historyAssertAlive(t, f)
			observeHistoryOnline(t, f.sqlDB, rules, expected, visible, report.FirstDueAt)
			time.Sleep(500 * time.Millisecond)
		}
		if len(visible) != 6000 {
			t.Fatalf("history %s completed %d/6000 online Instances", name, len(visible))
		}
		readHistoryOnline(t, f, &report, rules, visible)
	}
	report.FinalAudit = assertJobHistoryLoad(t, f.sqlDB, name == "C")
	report.DatabaseDelta, err = jobHistoryDatabaseDifference(report.DatabaseSamples[0], report.DatabaseSamples[len(report.DatabaseSamples)-1])
	if err != nil {
		t.Fatal(err)
	}
	if report.DatabaseDelta.Transactions.Deadlocks != 0 {
		t.Fatal("database deadlocks occurred in the measured window")
	}
	historyPlans(t, f, a, "after", report.FirstDueAt.Add(20*time.Minute-7*24*time.Hour))
	report.Cleanup = readHistoryCleanupPasses(t, f.processes, names, retention)
	if name == "C" {
		measureHistoryOverlap(t, &report)
	}
	report.CompletedAt = time.Now().UTC()
	report.Resources = parseJobLoadResources(f.processes)
	resourceWindow := jobLoadReport{StartedAt: report.PreparedAt, FirstDueAt: report.FirstDueAt, CompletedAt: report.WindowEnd, SampleIntervalMilliseconds: 500, Resources: report.Resources}
	assertJobLoadResourceWindow(t, &resourceWindow, "history-job-a", "history-job-b", "history-system-a", "history-system-b")
	report.Passed = true
	t.Logf("history %s passed its approved window and correctness checks", name)
}

type jobHistoryLoadReport struct {
	Case, Schema                                              string
	Passed                                                    bool
	RetentionDays                                             int
	PreparedAt, FirstDueAt, StartedAt, WindowEnd, CompletedAt time.Time
	Seed                                                      jobHistorySeed
	InitialAudit, FinalAudit                                  jobHistoryAudit
	Environment                                               jobHistoryDatabaseEnvironment
	DatabaseSamples                                           []jobHistoryDatabaseSample
	DatabaseDelta                                             jobHistoryDatabaseDelta
	ProcessIDs                                                map[string]int
	DatabaseApplications                                      map[string]string
	ExecutorNames                                             map[string]string
	Resources                                                 map[string][]jobLoadResource
	Minutes                                                   []jobShardedLoadWave
	Cleanup                                                   []jobHistoryCleanupPass
	Overlaps                                                  []jobHistoryOverlap
	Visible                                                   []jobHistoryVisibility
	Events                                                    []jobHistoryEvent
	OrdinaryStart, ShardStart                                 jobLoadDistribution
}

type jobHistoryRule struct {
	ID      int64
	Second  int
	Sharded bool
}
type jobHistoryVisibility struct {
	InstanceID    int64
	Due, Observed time.Time
	Milliseconds  float64
}
type jobHistoryEvent struct {
	Kind                                  int
	Due, Started, BusinessFinished, Saved time.Time
}

func historyJobConfiguration(t testing.TB, retention int) func(string) string {
	return func(config string) string {
		const fast = "\nDispatch:\n  ScanInterval: 100ms\n"
		if strings.Count(config, fast) != 1 {
			t.Fatal("cannot identify the accelerated fixture scan")
		}
		return strings.Replace(config, fast, "\n", 1) + fmt.Sprintf("\nHistoryCleanup:\n  RetentionDays: %d\n  Interval: 10m\n", retention)
	}
}

func createHistoryRules(t testing.TB, f *jobLifecycleFixture, node *jobLifecycleNode, first time.Time) []jobHistoryRule {
	minutes := make([]int, 30)
	for i := range minutes {
		minutes[i] = first.Add(time.Duration(i) * time.Minute).Minute()
	}
	sort.Ints(minutes)
	parts := make([]string, len(minutes))
	for i, minute := range minutes {
		parts[i] = strconv.Itoa(minute)
	}
	var rules []jobHistoryRule
	for i := range 200 {
		mode, count, processor, post := int32(1), 0, "system.e2e.load.ordinary", false
		if i >= 100 {
			mode, count, processor, post = 2, 10, "system.e2e.load.sharded", true
		}
		second := (i % 100) * 60 / 100
		disabled, retry, interval, skip, parallel := int32(0), int32(0), int32(1), int32(0), int32(1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		response, err := node.client.CreateJob(ctx, &job.CreateJobRequest{Status: &disabled, Config: &job.JobInput{
			Name: fmt.Sprintf("history-online-%03d", i), ExecutorGroupId: f.group.Id, ProcessorName: processor,
			ExecutionMode: &mode, PostEnabled: &post, Params: fmt.Sprintf(`{"count":%d,"payload_bytes":64,"delay_ms":100}`, count),
			CronExpr: fmt.Sprintf("%d %s * * * *", second, strings.Join(parts, ",")), MisfirePolicy: &skip, BlockPolicy: &parallel,
			MaxRetries: &retry, RetryIntervalSeconds: &interval, TimeoutSeconds: 120,
		}})
		cancel()
		if err != nil {
			t.Fatalf("create historical load rule: %v", err)
		}
		rules = append(rules, jobHistoryRule{response.Id, second, post})
	}
	return rules
}

func historyRuleIDs(rules []jobHistoryRule) []int64 {
	ids := make([]int64, len(rules))
	for i, r := range rules {
		ids[i] = r.ID
	}
	return ids
}
func historyKey(id int64, due time.Time) string { return fmt.Sprintf("%d/%d", id, due.Unix()) }
func historyExpected(rules []jobHistoryRule, first time.Time) map[string]time.Time {
	result := make(map[string]time.Time, len(rules)*30)
	for _, r := range rules {
		for m := range 30 {
			due := first.Add(time.Duration(m)*time.Minute + time.Duration(r.Second)*time.Second)
			result[historyKey(r.ID, due)] = due
		}
	}
	return result
}

func historyVisibilityValues(visible map[string]jobHistoryVisibility) []jobHistoryVisibility {
	values := make([]jobHistoryVisibility, 0, len(visible))
	for _, value := range visible {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].InstanceID < values[j].InstanceID })
	return values
}

func historyAssertAlive(t testing.TB, f *jobLifecycleFixture) {
	for _, p := range f.processes {
		select {
		case <-p.done:
			t.Fatalf("history load process %s exited unexpectedly", p.name)
		default:
		}
	}
}
func historyWait(t testing.TB, f *jobLifecycleFixture, until time.Time) {
	for time.Now().Before(until) {
		historyAssertAlive(t, f)
		time.Sleep(min(500*time.Millisecond, time.Until(until)))
	}
}
func historyDatabaseSample(t testing.TB, f *jobLifecycleFixture, schema string, names map[string]string) jobHistoryDatabaseSample {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sample, err := readJobHistoryDatabaseSample(ctx, f.sqlDB, schema, names)
	if err != nil {
		t.Fatal(err)
	}
	return sample
}

// observeHistoryOnline observes a rolling bounded time range. The independent
// visibility timestamp is recorded after consuming the statement, not copied
// from a row's pre-commit finished_at. Expected identities detect missing work.
func observeHistoryOnline(t testing.TB, db *sql.DB, rules []jobHistoryRule, expected map[string]time.Time, visible map[string]jobHistoryVisibility, first time.Time) {
	now := time.Now().UTC()
	lower := now.Add(-40 * time.Second)
	if lower.Before(first) {
		lower = first
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT id,job_id,scheduled_at,status FROM job_instance WHERE job_id=ANY($1) AND scheduled_at >= $2 AND scheduled_at <= $3`, historyRuleIDs(rules), lower, now)
	if err != nil {
		t.Fatal(err)
	}
	var successes []struct {
		key string
		id  int64
		due time.Time
	}
	states := make(map[string]struct {
		id     int64
		status int
	})
	for rows.Next() {
		var id, jobID int64
		var due time.Time
		var status int
		if err := rows.Scan(&id, &jobID, &due, &status); err != nil {
			t.Fatal(err)
		}
		key := historyKey(jobID, due)
		states[key] = struct {
			id     int64
			status int
		}{id, status}
		if _, ok := expected[key]; !ok {
			t.Fatal("Cron created an unplanned historical-load Instance")
		}
		if status == 4 || status == 5 || status == 6 {
			t.Fatalf("online Instance %d ended unsuccessfully: %d", id, status)
		}
		if status == 3 {
			successes = append(successes, struct {
				key string
				id  int64
				due time.Time
			}{key, id, due})
		}
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	observed := time.Now().UTC()
	for _, s := range successes {
		if old, ok := visible[s.key]; ok {
			if old.InstanceID != s.id {
				t.Fatal("duplicate Instance for one rule and time")
			}
			continue
		}
		v := jobHistoryVisibility{s.id, s.due, observed, observed.Sub(s.due).Seconds() * 1000}
		visible[s.key] = v
		if v.Milliseconds > 30000 {
			t.Fatalf("Instance %d was not independently visible within 30 seconds: %.1fms", s.id, v.Milliseconds)
		}
	}
	for key, due := range expected {
		if observed.After(due.Add(30 * time.Second)) {
			if _, ok := visible[key]; !ok {
				state, found := states[key]
				t.Fatalf("an expected Instance missed its 30 second visibility limit: %s observed_at=%s found=%t instance_id=%d status=%d", key, observed.Format(time.RFC3339Nano), found, state.id, state.status)
			}
		}
	}
}

func readHistoryOnline(t testing.TB, f *jobLifecycleFixture, report *jobHistoryLoadReport, rules []jobHistoryRule, visible map[string]jobHistoryVisibility) {
	if count := jobLoadCount(t, f.sqlDB, "SELECT count(*) FROM job_instance WHERE job_id=ANY($1)", historyRuleIDs(rules)); count != 6000 {
		t.Fatalf("online parent count=%d, want 6000", count)
	}
	report.Visible = historyVisibilityValues(visible)
	for minute := range 30 {
		var ids []int64
		var maxVisible float64
		for _, v := range report.Visible {
			if int(v.Due.Sub(report.FirstDueAt)/time.Minute) == minute {
				ids = append(ids, v.InstanceID)
				maxVisible = max(maxVisible, v.Milliseconds)
			}
		}
		wave := readJobShardedLoadFacts(t, f.sqlDB, jobShardedLoadScenario{Name: "history-continuous", ShardedInstances: 100, Shards: 10, OrdinaryInstances: 100, PayloadBytes: 64}, minute+1, ids, time.Time{}, false, report.ExecutorNames, false)
		wave.AllSuccessVisibleMS = maxVisible
		report.Minutes = append(report.Minutes, wave)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := f.sqlDB.QueryContext(ctx, `SELECT t.task_type,i.scheduled_at,b.started_at,b.finished_at,t.finished_at FROM job_instance i JOIN job_task t ON t.instance_id=i.id JOIN system_job_e2e_records b ON b.task_id=t.id WHERE i.job_id=ANY($1) ORDER BY i.id,t.id`, historyRuleIDs(rules))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var e jobHistoryEvent
		if err := rows.Scan(&e.Kind, &e.Due, &e.Started, &e.BusinessFinished, &e.Saved); err != nil {
			t.Fatal(err)
		}
		if e.Started.Before(e.Due) {
			t.Fatal("business began before its planned time")
		}
		report.Events = append(report.Events, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(report.Events) != 39000 {
		t.Fatal("online execution facts did not contain exactly 39000 calls")
	}
	var ordinary, shards []float64
	for _, event := range report.Events {
		if event.Kind == 1 {
			ordinary = append(ordinary, event.Started.Sub(event.Due).Seconds()*1000)
		}
		if event.Kind == 3 {
			shards = append(shards, event.Started.Sub(event.Due).Seconds()*1000)
		}
	}
	report.OrdinaryStart, report.ShardStart = jobLoadPercentiles(ordinary), jobLoadPercentiles(shards)
	if report.OrdinaryStart.P99 > 10000 || report.ShardStart.P95 > 15000 || report.ShardStart.P99 > 20000 {
		t.Fatal("the measured window exceeded approved business-start percentiles")
	}
	for _, name := range report.ExecutorNames {
		calls := 0
		for _, minute := range report.Minutes {
			calls += minute.ExecutorCalls[name]
		}
		if calls == 0 {
			t.Fatalf("executor %s did not participate in the continuous workload", name)
		}
	}
}

func historyPlans(t testing.TB, f *jobLifecycleFixture, node *jobLifecycleNode, phase string, cutoff time.Time) {
	output := strings.TrimSuffix(os.Getenv("DOGX_LOAD_REPORT"), ".json") + "-" + phase + "-plans.json"
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, requiredBinary(t, "DOGX_E2E_JOB_RPC_BINARY"), "-test.run=^TestJobHistoryLoadQueryPlans$", "-f", filepath.Join(f.dir, node.process.name+".yaml"))
	command.Env = append(append([]string(nil), f.environment...), "DOGX_HISTORY_PLAN_OUTPUT="+output, "DOGX_HISTORY_PLAN_CUTOFF="+cutoff.Format(time.RFC3339Nano))
	if _, err := command.CombinedOutput(); err != nil {
		t.Fatalf("read actual repository query plans: %v", err)
	}
	data, err := os.ReadFile(output)
	if err != nil || !json.Valid(data) {
		t.Fatal("query plan helper did not produce its JSON evidence")
	}
}
