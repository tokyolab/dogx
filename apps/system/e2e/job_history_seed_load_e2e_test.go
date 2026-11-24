//go:build integration && e2e && load

package e2e

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	jobHistoryInstanceCount = 100000
	jobHistoryTaskCount     = 1150000
	jobHistoryLastTaskID    = 100000502
)

// jobHistorySeed describes the fixed, reproducible history fixture. The two
// expired cohorts each contain 10,000 parents and 115,000 children. They become
// eligible only after alignJobHistoryAges establishes their measurement origin.
type jobHistorySeed struct {
	Instances, Tasks, ExpiredInstances, ExpiredTasks, LargeTasks int64
	LargeRawJSONBytes, MaximumPreparedReportBytes                int
}

// jobHistoryAudit compares the original identity set with surviving history.
// Online identities start above the reserved history ranges and are checked by
// the workload assertions, while Orphans covers every Task in the schema.
type jobHistoryAudit struct {
	Parents, Tasks, LargeTasks                                            int64
	MissingParents, UnexpectedParents, ChangedParents                     int64
	MissingTasks, UnexpectedTasks, ChangedTasks, ChangedPayloads, Orphans int64
}

type jobHistoryPayload struct {
	ID          int             `json:"id"`
	Input       json.RawMessage `json:"input_data"`
	Result      json.RawMessage `json:"result_data"`
	Prepared100 json.RawMessage `json:"prepared_100"`
	Prepared500 json.RawMessage `json:"prepared_500"`
}

// seedJobHistoryLoad writes only a newly migrated, isolated fixture schema. It
// must run before online rules are created. All history initially finishes one
// day ago, so a running Leader cannot clean it during preparation. No product
// repository or execution path is replaced by these fixture writes.
func seedJobHistoryLoad(t testing.TB, f *jobLifecycleFixture) jobHistorySeed {
	t.Helper()
	if f.group == nil || f.group.Id <= 0 {
		t.Fatal("history seeding requires an existing executor group")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var count int64
	err := f.sqlDB.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM job_definition) +
		(SELECT count(*) FROM job_instance) +
		(SELECT count(*) FROM job_task)`).Scan(&count)
	cancel()
	if err != nil || count != 0 {
		t.Fatalf("history seeding requires empty Job tables: count=%d error=%v", count, err)
	}
	seed := jobHistorySeed{Instances: jobHistoryInstanceCount, Tasks: jobHistoryTaskCount,
		ExpiredInstances: 20000, ExpiredTasks: 230000, LargeTasks: 10000}
	payloads := make([]jobHistoryPayload, 100)
	for index := range payloads {
		payload := jobHistoryPayload{ID: index,
			Input: historyJSONPayload(t, index, "input"), Result: historyJSONPayload(t, index, "result")}
		seed.LargeRawJSONBytes = len(payload.Input) + len(payload.Result)
		for _, size := range []int{100, 500} {
			items := make([]json.RawMessage, size)
			wire := make([][]byte, size)
			for item := range items {
				items[item] = json.RawMessage(fmt.Sprintf(`{"item":%d}`, item+1))
				if item == 0 {
					items[item] = payload.Input
				}
				wire[item] = items[item]
			}
			encoded, err := json.Marshal(items)
			if err != nil {
				t.Fatal(err)
			}
			if size == 100 {
				payload.Prepared100 = encoded
			} else {
				payload.Prepared500 = encoded
			}
			request := &protocol.ReportTaskResultRequest{
				Auth:  &protocol.GroupAuth{GroupName: f.group.Name, Token: f.group.Token},
				JobId: 3, InstanceId: jobHistoryInstanceCount, TaskId: jobHistoryLastTaskID,
				AttemptNo: 1, ExecutorInstanceId: "00000000-0000-4000-8000-000000000001",
				Outcome: protocol.TaskOutcome_SUCCESS, Summary: "history preparation",
				Output: &protocol.ReportTaskResultRequest_PreparedItems{PreparedItems: &protocol.PreparedItems{Items: wire}},
			}
			seed.MaximumPreparedReportBytes = max(seed.MaximumPreparedReportBytes, proto.Size(request))
		}
		payloads[index] = payload
	}
	if seed.MaximumPreparedReportBytes >= 4*1024*1024 {
		t.Fatal("history preparation exceeded the production RPC message limit")
	}
	encoded, err := json.Marshal(payloads)
	if err != nil {
		t.Fatal(err)
	}
	historySeedExec(t, f.sqlDB, "create history manifest", `CREATE TABLE system_job_history_manifest (
		instance_id BIGINT PRIMARY KEY, job_id BIGINT NOT NULL,
		executor_group_id BIGINT NOT NULL, shard_count INTEGER NOT NULL,
		task_count INTEGER NOT NULL, cleanup_batch SMALLINT NOT NULL,
		status SMALLINT NOT NULL, payload_id INTEGER NOT NULL,
		finished_at TIMESTAMPTZ NOT NULL);
		CREATE TABLE system_job_history_payload (
		id INTEGER PRIMARY KEY, input_data JSONB NOT NULL, result_data JSONB NOT NULL,
		prepared_100 JSONB NOT NULL, prepared_500 JSONB NOT NULL)`)
	historySeedExec(t, f.sqlDB, "insert deterministic history payloads", `INSERT INTO system_job_history_payload
		SELECT * FROM jsonb_to_recordset($1::jsonb) AS p(id INTEGER, input_data JSONB,
		result_data JSONB, prepared_100 JSONB, prepared_500 JSONB)`, string(encoded))
	historySeedExec(t, f.sqlDB, "insert disabled history definitions", `INSERT INTO job_definition
		(id, name, remark, executor_group_id, processor_name, execution_mode, post_enabled,
		params, cron_expr, status, misfire_policy, block_policy, max_retries, retry_interval_seconds,
		timeout_seconds, created_at, updated_at)
		SELECT n, 'history-seed-' || n, '', $1, 'system.e2e.history',
		CASE WHEN n = 1 THEN 1 ELSE 2 END, n <> 1, '', '0 0 0 1 1 *',
		0, 0, 1, 0, 1, 900, clock_timestamp() - interval '8 days', clock_timestamp() - interval '8 days'
		FROM generate_series(1, 3) n`, f.group.Id)
	historySeedExec(t, f.sqlDB, "insert history manifest", `INSERT INTO system_job_history_manifest
		SELECT n,
		CASE WHEN n <= 90000 THEN 1 WHEN n <= 99900 THEN 2 ELSE 3 END,
		$1, CASE WHEN n <= 90000 THEN 0 WHEN n <= 99900 THEN 100 ELSE 500 END,
		CASE WHEN n <= 90000 THEN 1 WHEN n <= 99900 THEN 102 ELSE 502 END,
		CASE WHEN n % 10 = 0 THEN 1 WHEN n % 10 = 1 THEN 2 ELSE 0 END,
		CASE WHEN (n / 10) % 10 = 8 THEN 4 WHEN (n / 10) % 10 = 9 THEN 5 ELSE 3 END,
		(n - 1) % 100, clock_timestamp() - interval '1 day'
		FROM generate_series(1, 100000) n`, f.group.Id)
	historySeedExec(t, f.sqlDB, "create expected history Task projection", historyExpectedTasksSQL)
	// Fixed parent batches bound fixture preparation; no per-record read is
	// performed, and the real cleanup still uses its unchanged short transactions.
	for start := 1; start <= jobHistoryInstanceCount; start += 1000 {
		end := min(start+999, jobHistoryInstanceCount)
		historySeedExec(t, f.sqlDB, "insert history parents", `INSERT INTO job_instance
			(id, job_id, job_name, executor_group_id, trigger_type, scheduled_at, request_id, triggered_by, params_overridden,
			execution_config, status, started_at, deadline_at, finished_at, end_reason,
			result_summary, created_at, updated_at)
			SELECT instance_id, job_id, 'history-seed-' || job_id, executor_group_id,
			3, NULL, ('00000000-0000-4000-8000-' || lpad(to_hex(instance_id), 12, '0'))::uuid, 1, false,
			jsonb_build_object('schema_version', 1, 'processor_name', 'system.e2e.history',
			'execution_mode', CASE WHEN shard_count = 0 THEN 1 ELSE 2 END,
			'post_enabled', shard_count <> 0, 'params', '', 'max_retries', 0,
			'retry_interval_seconds', 1, 'timeout_seconds', 900),
			status, finished_at - interval '59 seconds', finished_at + interval '841 seconds',
			finished_at, CASE status WHEN 3 THEN 'success' WHEN 4 THEN 'execution_failed' ELSE 'manual_stop' END,
			'history fixture', finished_at - interval '60 seconds', finished_at
			FROM system_job_history_manifest WHERE instance_id BETWEEN $1 AND $2`, start, end)
		historySeedExec(t, f.sqlDB, "insert history children", `INSERT INTO job_task
			(id, instance_id, task_type, status, attempt_no, used_attempts, attempt_registered_at,
			executor_instance_id, executor_address, input_data, result_data, result_summary,
			end_reason, started_at, finished_at, created_at, updated_at)
			SELECT id, instance_id, task_type, status, 1, 1, started_at,
			'00000000-0000-4000-8000-000000000001'::uuid, '127.0.0.1:45500',
			input_data, result_data, 'history fixture',
			CASE status WHEN 4 THEN 'success' WHEN 5 THEN 'execution_failed' ELSE 'manual_stop' END,
			started_at, finished_at, created_at, finished_at
			FROM system_job_history_expected_tasks WHERE instance_id BETWEEN $1 AND $2`, start, end)
		if end%10000 == 0 {
			t.Logf("history fixture prepared %d/%d parents", end, jobHistoryInstanceCount)
		}
	}
	historySeedExec(t, f.sqlDB, "reserve online identity sequences", `SELECT
		setval(pg_get_serial_sequence('job_definition', 'id'), 3),
		setval(pg_get_serial_sequence('job_instance', 'id'), 100000),
		setval(pg_get_serial_sequence('job_task', 'id'), 100000502)`)
	return seed
}

// historyJSONPayload uses deterministic SHA-256 blocks rather than repeated
// characters, so TOAST compression cannot turn the large-record scenario into
// a tiny repeated string. The complete JSON value is approximately 16 KiB.
func historyJSONPayload(t testing.TB, index int, kind string) json.RawMessage {
	t.Helper()
	data := make([]byte, 0, 12288)
	for block := 0; len(data) < cap(data); block++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("dogx-history-load-v1/%d/%s/%d", index, kind, block)))
		data = append(data, digest[:]...)
	}
	encoded, err := json.Marshal(map[string]string{"payload": base64.StdEncoding.EncodeToString(data)})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// alignJobHistoryAges establishes the two expiry cohorts before measurement.
// A caller must keep this preparation outside the measured window and verify
// no cleanup ran before alignment and process readiness completed. It changes
// the complete historical timeline, never only the parent's retention field.
func alignJobHistoryAges(t testing.TB, db *sql.DB, reference time.Time) {
	t.Helper()
	if reference.IsZero() {
		t.Fatal("history alignment requires a nonzero scheduled reference")
	}
	historySeedExec(t, db, "align history manifest clock", `UPDATE system_job_history_manifest SET finished_at =
		CASE cleanup_batch WHEN 1 THEN $1::timestamptz - interval '7 days' + interval '5 minutes'
		WHEN 2 THEN $1::timestamptz - interval '7 days' + interval '15 minutes'
		ELSE $1::timestamptz - interval '1 day' END`, reference)
	for start := 1; start <= jobHistoryInstanceCount; start += 1000 {
		end := min(start+999, jobHistoryInstanceCount)
		historySeedExec(t, db, "align history parent times", `UPDATE job_instance i SET
			created_at = m.finished_at - interval '60 seconds', started_at = m.finished_at - interval '59 seconds',
			deadline_at = m.finished_at + interval '841 seconds', finished_at = m.finished_at, updated_at = m.finished_at
			FROM system_job_history_manifest m WHERE i.id = m.instance_id AND m.instance_id BETWEEN $1 AND $2`, start, end)
		historySeedExec(t, db, "align history child times", `UPDATE job_task t SET
			created_at = e.created_at, started_at = e.started_at, attempt_registered_at = e.started_at,
			finished_at = e.finished_at, updated_at = e.finished_at
			FROM system_job_history_expected_tasks e WHERE t.id = e.id AND e.instance_id BETWEEN $1 AND $2`, start, end)
	}
	historySeedExec(t, db, "analyze prepared history", `ANALYZE job_definition; ANALYZE job_instance; ANALYZE job_task;
		ANALYZE system_job_history_manifest; ANALYZE system_job_history_payload`)
}

// assertJobHistoryLoad verifies identities, immutable facts, and payloads after
// preparation or a completed comparison. It is intentionally outside timed
// sampling: a complete audit must not add a million-row scan to every sample.
func assertJobHistoryLoad(t testing.TB, db *sql.DB, cleaned bool) jobHistoryAudit {
	t.Helper()
	var audit jobHistoryAudit
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	err := db.QueryRowContext(ctx, `SELECT
		count(*) FILTER (WHERE i.id IS NOT NULL),
		count(*) FILTER (WHERE NOT ($1 AND m.cleanup_batch <> 0) AND i.id IS NULL),
		count(*) FILTER (WHERE $1 AND m.cleanup_batch <> 0 AND i.id IS NOT NULL),
		count(*) FILTER (WHERE i.id IS NOT NULL AND (
			i.job_id <> m.job_id OR i.executor_group_id <> m.executor_group_id OR i.status <> m.status OR
			i.job_name <> 'history-seed-' || m.job_id OR i.trigger_type <> 3 OR i.scheduled_at IS NOT NULL OR
			i.request_id IS DISTINCT FROM ('00000000-0000-4000-8000-' || lpad(to_hex(m.instance_id), 12, '0'))::uuid OR
			i.triggered_by IS DISTINCT FROM 1 OR i.params_overridden OR
			i.execution_config IS DISTINCT FROM jsonb_build_object('schema_version', 1, 'processor_name', 'system.e2e.history',
			'execution_mode', CASE WHEN m.shard_count = 0 THEN 1 ELSE 2 END, 'post_enabled', m.shard_count <> 0,
			'params', '', 'max_retries', 0, 'retry_interval_seconds', 1, 'timeout_seconds', 900) OR
			i.end_reason IS DISTINCT FROM CASE m.status WHEN 3 THEN 'success' WHEN 4 THEN 'execution_failed' ELSE 'manual_stop' END OR
			i.updated_at IS DISTINCT FROM m.finished_at OR i.result_summary IS DISTINCT FROM 'history fixture' OR
			i.finished_at IS DISTINCT FROM m.finished_at OR i.started_at IS DISTINCT FROM m.finished_at - interval '59 seconds' OR
			i.created_at IS DISTINCT FROM m.finished_at - interval '60 seconds' OR
			i.deadline_at IS DISTINCT FROM m.finished_at + interval '841 seconds'))
		FROM system_job_history_manifest m LEFT JOIN job_instance i ON i.id = m.instance_id`, cleaned).
		Scan(&audit.Parents, &audit.MissingParents, &audit.UnexpectedParents, &audit.ChangedParents)
	if err != nil {
		t.Fatalf("audit historical parent identity set: %v", err)
	}
	err = db.QueryRowContext(ctx, `SELECT
		count(*) FILTER (WHERE t.id IS NOT NULL),
		count(*) FILTER (WHERE NOT ($1 AND e.cleanup_batch <> 0) AND t.id IS NULL),
		count(*) FILTER (WHERE $1 AND e.cleanup_batch <> 0 AND t.id IS NOT NULL),
		count(*) FILTER (WHERE t.id IS NOT NULL AND (
			t.instance_id <> e.instance_id OR t.task_type <> e.task_type OR t.status <> e.status OR
			t.attempt_no <> 1 OR t.used_attempts <> 1 OR
			t.next_dispatch_at IS NOT NULL OR t.attempt_registered_at IS DISTINCT FROM e.started_at OR
			t.executor_instance_id IS DISTINCT FROM '00000000-0000-4000-8000-000000000001'::uuid OR
			t.executor_address IS DISTINCT FROM '127.0.0.1:45500' OR t.result_summary IS DISTINCT FROM 'history fixture' OR
			t.end_reason IS DISTINCT FROM CASE e.status WHEN 4 THEN 'success' WHEN 5 THEN 'execution_failed' ELSE 'manual_stop' END OR
			t.updated_at IS DISTINCT FROM e.finished_at OR
			t.created_at IS DISTINCT FROM e.created_at OR t.started_at IS DISTINCT FROM e.started_at OR
			t.finished_at IS DISTINCT FROM e.finished_at)),
		count(*) FILTER (WHERE t.id IS NOT NULL AND (t.input_data IS DISTINCT FROM e.input_data OR t.result_data IS DISTINCT FROM e.result_data)),
		count(*) FILTER (WHERE t.id IS NOT NULL AND e.task_type = 3 AND e.slot = 2)
		FROM system_job_history_expected_tasks e LEFT JOIN job_task t ON t.id = e.id`, cleaned).
		Scan(&audit.Tasks, &audit.MissingTasks, &audit.UnexpectedTasks, &audit.ChangedTasks, &audit.ChangedPayloads, &audit.LargeTasks)
	if err != nil {
		t.Fatalf("audit historical child identity set: %v", err)
	}
	var extraParents, extraTasks int64
	// Check historical ownership as well as reserved IDs. A newly inserted
	// parent or child can use the online sequence range while still changing
	// protected history, so an ID-range-only audit would miss that mutation.
	err = db.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM job_instance i LEFT JOIN system_job_history_manifest m ON m.instance_id = i.id
		 WHERE (i.id <= 100000 OR i.job_id IN (1, 2, 3)) AND m.instance_id IS NULL),
		(SELECT count(*) FROM job_task t LEFT JOIN system_job_history_expected_tasks e ON e.id = t.id
		 LEFT JOIN system_job_history_manifest m ON m.instance_id = t.instance_id
		 WHERE (t.id <= 100000502 OR m.instance_id IS NOT NULL) AND e.id IS NULL),
		(SELECT count(*) FROM job_task t LEFT JOIN job_instance i ON i.id = t.instance_id WHERE i.id IS NULL)`).
		Scan(&extraParents, &extraTasks, &audit.Orphans)
	if err != nil {
		t.Fatalf("audit unexpected history identities and orphans: %v", err)
	}
	audit.UnexpectedParents += extraParents
	audit.UnexpectedTasks += extraTasks
	wantParents, wantTasks, wantLarge := int64(jobHistoryInstanceCount), int64(jobHistoryTaskCount), int64(10000)
	if cleaned {
		wantParents, wantTasks, wantLarge = 80000, 920000, 8000
	}
	if audit.Parents != wantParents || audit.Tasks != wantTasks || audit.LargeTasks != wantLarge ||
		audit.MissingParents+audit.UnexpectedParents+audit.ChangedParents+audit.MissingTasks+
			audit.UnexpectedTasks+audit.ChangedTasks+audit.ChangedPayloads+audit.Orphans != 0 {
		t.Errorf("historical identity audit failed: %+v; want parents=%d tasks=%d large=%d", audit, wantParents, wantTasks, wantLarge)
	}
	return audit
}

// historySeedExec bounds fixture-only preparation calls independently of the
// production cleanup timeout. The fixture never changes a product budget to
// make the measured workload pass.
func historySeedExec(t testing.TB, db *sql.DB, operation, statement string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, statement, args...); err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
}

// The expected Task view derives every identity and stage from the small parent
// manifest. Successful preparation and finished shards precede Post creation.
// Failed rounds contain one failed shard and a successful Post; canceled rounds
// were stopped during Post, after their shards had already succeeded.
const historyExpectedTasksSQL = `CREATE VIEW system_job_history_expected_tasks AS
	WITH stages AS (
		SELECT m.*, s.slot, m.instance_id * 1000 + s.slot AS id,
		CASE WHEN m.shard_count = 0 THEN 1 WHEN s.slot = 1 THEN 2
		WHEN s.slot = m.task_count THEN 4 ELSE 3 END AS task_type,
		CASE WHEN m.shard_count = 0 THEN m.status + 1
		WHEN s.slot = m.task_count AND m.status = 5 THEN 6
		WHEN s.slot = 3 AND m.status = 4 THEN 5 ELSE 4 END AS task_status
		FROM system_job_history_manifest m CROSS JOIN LATERAL generate_series(1, m.task_count) s(slot))
	SELECT e.id, e.instance_id, e.slot, e.cleanup_batch, e.task_type, e.task_status AS status,
	CASE e.task_type WHEN 1 THEN e.finished_at - interval '59 seconds'
	WHEN 2 THEN e.finished_at - interval '59 seconds' WHEN 3 THEN e.finished_at - interval '56 seconds'
	ELSE e.finished_at - interval '2 seconds' END AS created_at,
	CASE e.task_type WHEN 1 THEN e.finished_at - interval '58 seconds'
	WHEN 2 THEN e.finished_at - interval '58 seconds' WHEN 3 THEN e.finished_at - interval '55 seconds'
	ELSE e.finished_at - interval '1 second' END AS started_at,
	CASE e.task_type WHEN 2 THEN e.finished_at - interval '57 seconds'
	WHEN 3 THEN e.finished_at - interval '3 seconds' ELSE e.finished_at - interval '500 milliseconds' END AS finished_at,
	CASE e.task_type WHEN 3 THEN CASE WHEN e.slot = 2 THEN p.input_data ELSE jsonb_build_object('item', e.slot - 1) END
	WHEN 4 THEN jsonb_build_object('total', e.shard_count,
	'succeeded', e.shard_count - CASE WHEN e.status = 4 THEN 1 ELSE 0 END,
	'failed', CASE WHEN e.status = 4 THEN 1 ELSE 0 END) ELSE NULL END AS input_data,
	CASE WHEN e.task_status <> 4 THEN NULL
	WHEN e.task_type = 2 THEN CASE WHEN e.shard_count = 100 THEN p.prepared_100 ELSE p.prepared_500 END
	WHEN e.task_type = 3 AND e.slot = 2 THEN p.result_data
	ELSE jsonb_build_object('history', CASE e.task_type WHEN 1 THEN 'ordinary' WHEN 3 THEN 'shard' ELSE 'post' END) END AS result_data
	FROM stages e JOIN system_job_history_payload p ON p.id = e.payload_id`
