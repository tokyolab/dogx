//go:build integration && e2e

package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestJobShardedExecutionEndToEnd uses the real Job process and the System
// production run function with a test-only business processor. All Job state
// is observed through management RPC; only System business records are read.
func TestJobShardedExecutionEndToEnd(t *testing.T) {
	fixture := newJobLifecycleFixture(t)
	node := fixture.startJob(t, "sharded-job")
	fixture.createGroup(t, node)
	executor := fixture.startSystem(t, "sharded-system", true)
	processes := []*runningProcess{node.process, executor.process}
	for _, scenario := range []struct {
		mode    string
		retries int32
	}{
		{mode: "success"},
		{mode: "shard_retry", retries: 1},
		{mode: "post_retry", retries: 1},
	} {
		t.Run(scenario.mode, func(t *testing.T) {
			jobID, instanceID, params := triggerJobSharded(t, fixture, node, scenario.mode, scenario.retries)
			waitJobShardedTerminal(t, node, instanceID, 3, processes)
			assertJobShardedExecution(t, fixture, node, jobID, instanceID, params, scenario.mode)
		})
	}
}

// TestJobShardedSchedulerCrashEndToEnd kills A only after it dispatched and
// bound every business shard. B is already serving the same discovery keys.
// Business then finishes in the unchanged System process, and B must receive
// the results, create Post and finish the Instance without repeating a stage.
func TestJobShardedSchedulerCrashEndToEnd(t *testing.T) {
	fixture := newJobLifecycleFixture(t)
	nodeA := fixture.startJob(t, "sharded-crash-job-a")
	fixture.createGroup(t, nodeA)
	executor := fixture.startSystem(t, "sharded-crash-system", true)
	jobID, instanceID, params := triggerJobSharded(t, fixture, nodeA, "gated", 0)
	accepted := waitJobShardedRunning(t, fixture, nodeA, instanceID, []*runningProcess{nodeA.process, executor.process})
	before := readJobShardedInstance(t, nodeA, instanceID)
	nodeB := fixture.startJob(t, "sharded-crash-job-b")
	jobE2EEventually(t, []*runningProcess{nodeA.process, nodeB.process, executor.process}, "both scheduler result listeners registered", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := fixture.discovery.Get(ctx, fixture.prefix+"/results/", clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("read scheduler result discovery: %v", err)
		}
		addresses := make(map[string]bool)
		for _, entry := range response.Kvs {
			if entry.Lease != 0 {
				addresses[string(entry.Value)] = true
			}
		}
		return addresses[fmt.Sprintf("127.0.0.1:%d", nodeA.resultPort)] && addresses[fmt.Sprintf("127.0.0.1:%d", nodeB.resultPort)]
	})
	if err := nodeA.process.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill sharded scheduler A: %v", err)
	}
	if err := waitE2EProcessExit(t, nodeA.process, 5*time.Second); err == nil {
		t.Fatal("the killed scheduler unexpectedly reported a clean exit")
	}
	fixture.waitNoDatabaseConnections(t, nodeA.appName)

	// A is gone before the only business release. The production Runtime owns
	// reporting and discovery; the test never sends a synthetic Task result.
	releaseJobShardedBusiness(t, fixture, instanceID)
	waitJobShardedTerminal(t, nodeB, instanceID, 3, []*runningProcess{nodeB.process, executor.process})
	after := readJobShardedInstance(t, nodeB, instanceID)
	if after.GetStartedAt() != before.GetStartedAt() || after.GetDeadlineAt() != before.GetDeadlineAt() {
		t.Fatal("scheduler replacement changed the original Instance execution window")
	}
	finalTasks := readJobShardedTasks(t, nodeB, instanceID)
	for _, task := range finalTasks {
		if original, ok := accepted[task.Id]; ok {
			if task.AttemptNo != original.AttemptNo || task.GetExecutorInstanceId() != original.GetExecutorInstanceId() ||
				task.GetExecutorAddress() != original.GetExecutorAddress() {
				t.Fatal("scheduler replacement changed an accepted shard attempt or executor binding")
			}
			delete(accepted, task.Id)
		}
	}
	if len(accepted) != 0 {
		t.Fatal("scheduler replacement lost original shard Tasks")
	}
	assertJobShardedExecution(t, fixture, nodeB, jobID, instanceID, params, "gated")
	t.Log("after A exited, B received real shard results and completed Post; all business stages ran once")
}

func TestJobShardedManualStopEndToEnd(t *testing.T) {
	fixture := newJobLifecycleFixture(t)
	node := fixture.startJob(t, "sharded-stop-job")
	fixture.createGroup(t, node)
	executor := fixture.startSystem(t, "sharded-stop-system", true)
	processes := []*runningProcess{node.process, executor.process}
	jobID, instanceID, params := triggerJobSharded(t, fixture, node, "wait", 0)
	waitJobShardedRunning(t, fixture, node, instanceID, processes)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopped, err := node.client.StopInstance(ctx, &job.StopInstanceRequest{Id: instanceID})
	if err != nil {
		t.Fatalf("stop sharded Instance: %v", err)
	}
	if stopped.InstanceId != instanceID || stopped.Status != 6 || stopped.GetEndReason() != "manual_stop" {
		t.Fatal("manual stop did not acknowledge the persisted STOPPING decision")
	}
	waitJobShardedTerminal(t, node, instanceID, 5, processes)
	// Database convergence does not guarantee RPC cancellation. This healthy
	// network scenario separately verifies that all three running processors
	// observe the one-Instance cancellation and that no Post business starts.
	jobE2EEventually(t, processes, "all sharded business calls observed cancellation", func() bool {
		records := readJobShardedBusiness(t, fixture, instanceID)
		canceled := 0
		for _, record := range records {
			if record.input.Stage == "shard" && record.state == "canceled" {
				canceled++
			}
		}
		return len(records) == 4 && canceled == 3
	})
	assertJobShardedExecution(t, fixture, node, jobID, instanceID, params, "wait")
}

func jobShardedItems() []json.RawMessage {
	return []json.RawMessage{
		json.RawMessage(`"first"`),
		json.RawMessage(`{"order_ids":[9007199254740993,42],"batch":"business-defined"}`),
		json.RawMessage(`[false,null,{"customer":"third"}]`),
	}
}

func triggerJobSharded(t testing.TB, fixture *jobLifecycleFixture, node *jobLifecycleNode, mode string, retries int32) (int64, int64, string) {
	t.Helper()
	params, err := json.Marshal(struct {
		Mode  string            `json:"mode"`
		Value string            `json:"value"`
		Items []json.RawMessage `json:"items"`
	}{Mode: mode, Value: "sharded-" + mode, Items: jobShardedItems()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	disabled, sharded, interval, skip, parallel := int32(0), int32(2), int32(1), int32(0), int32(1)
	post, overridden := true, false
	created, err := node.client.CreateJob(ctx, &job.CreateJobRequest{Status: &disabled, Config: &job.JobInput{
		Name: "sharded " + mode, ExecutorGroupId: fixture.group.Id, ProcessorName: "system.e2e.sharded", ExecutionMode: &sharded,
		PostEnabled: &post, Params: string(params), CronExpr: "0 0 0 1 1 *", MisfirePolicy: &skip, BlockPolicy: &parallel,
		MaxRetries: &retries, RetryIntervalSeconds: &interval, TimeoutSeconds: 120,
	}})
	if err != nil {
		t.Fatalf("create sharded Job: %v", err)
	}
	triggered, err := node.client.TriggerJob(ctx, &job.TriggerJobRequest{Id: created.Id, RequestId: uuid.NewString(), ParamsOverridden: &overridden, OperatorId: 1})
	if err != nil {
		t.Fatalf("trigger sharded Job: %v", err)
	}
	return created.Id, triggered.InstanceId, string(params)
}

func readJobShardedInstance(t testing.TB, node *jobLifecycleNode, instanceID int64) *job.InstanceInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := node.client.GetInstance(ctx, &job.GetInstanceRequest{Id: instanceID})
	if err != nil || response.GetInstance() == nil {
		t.Fatalf("read sharded Instance: %v", err)
	}
	return response.Instance
}

func readJobShardedTasks(t testing.TB, node *jobLifecycleNode, instanceID int64) []*job.TaskListItem {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := node.client.ListInstanceTasks(ctx, &job.ListInstanceTasksRequest{InstanceId: instanceID, Page: 1, PageSize: 20})
	if err != nil {
		t.Fatalf("read sharded Tasks: %v", err)
	}
	if response.Total != int64(len(response.Items)) {
		t.Fatal("sharded test unexpectedly exceeded one bounded Task page")
	}
	return response.Items
}

func waitJobShardedTerminal(t testing.TB, node *jobLifecycleNode, instanceID int64, wantStatus int32, processes []*runningProcess) {
	t.Helper()
	jobE2EEventually(t, processes, "sharded Instance final state", func() bool {
		instance := readJobShardedInstance(t, node, instanceID)
		if instance.Status >= 3 && instance.Status <= 5 && instance.Status != wantStatus {
			t.Fatalf("unexpected sharded Instance terminal status %d: %s", instance.Status, instance.GetEndReason())
		}
		return instance.Status == wantStatus && instance.FinishedAt != nil
	})
}

func waitJobShardedRunning(t testing.TB, fixture *jobLifecycleFixture, node *jobLifecycleNode, instanceID int64, processes []*runningProcess) map[int64]*job.TaskListItem {
	t.Helper()
	var accepted map[int64]*job.TaskListItem
	jobE2EEventually(t, processes, "all three business shards started with saved bindings", func() bool {
		tasks := readJobShardedTasks(t, node, instanceID)
		if len(tasks) != 4 {
			return false
		}
		running := make(map[int64]bool)
		for _, record := range readJobShardedBusiness(t, fixture, instanceID) {
			if record.input.Stage == "shard" && record.invocation == 1 && record.state == "running" {
				running[record.taskID] = true
			}
		}
		accepted = make(map[int64]*job.TaskListItem)
		for _, task := range tasks {
			if task.TaskType == 3 && task.Status == 2 && task.AttemptNo == 1 && task.UsedAttempts == 1 &&
				task.GetExecutorInstanceId() != "" && task.GetExecutorAddress() != "" && running[task.Id] {
				accepted[task.Id] = task
			}
		}
		return len(accepted) == 3
	})
	return accepted
}

func releaseJobShardedBusiness(t testing.TB, fixture *jobLifecycleFixture, instanceID int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := fixture.sqlDB.ExecContext(ctx, `UPDATE system_job_e2e_records SET state = 'release'
		WHERE instance_id = $1 AND state = 'running' AND value::jsonb->>'stage' = 'shard'`, instanceID)
	if err != nil {
		t.Fatalf("release System sharded business: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 3 {
		t.Fatal("all three System shards must still be waiting for release")
	}
}

type jobShardedBusinessInput struct {
	Stage  string          `json:"stage"`
	Params string          `json:"params"`
	Item   json.RawMessage `json:"item"`
	Post   *struct {
		Total     int64 `json:"total"`
		Succeeded int64 `json:"succeeded"`
		Failed    int64 `json:"failed"`
	} `json:"post"`
}

type jobShardedBusinessRecord struct {
	taskID, invocation, instanceID, jobID int64
	state                                 string
	started                               time.Time
	finished                              sql.NullTime
	input                                 jobShardedBusinessInput
}

func readJobShardedBusiness(t testing.TB, fixture *jobLifecycleFixture, instanceID int64) []jobShardedBusinessRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows, err := fixture.sqlDB.QueryContext(ctx, `SELECT task_id, invocation_no, instance_id, job_id, value, state, started_at, finished_at
		FROM system_job_e2e_records WHERE instance_id = $1 ORDER BY task_id, invocation_no`, instanceID)
	if err != nil {
		t.Fatalf("read System sharded business facts: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close System sharded business rows: %v", err)
		}
	}()
	var records []jobShardedBusinessRecord
	for rows.Next() {
		var record jobShardedBusinessRecord
		var value string
		if err := rows.Scan(&record.taskID, &record.invocation, &record.instanceID, &record.jobID, &value, &record.state, &record.started, &record.finished); err != nil {
			t.Fatalf("scan System sharded business fact: %v", err)
		}
		if err := json.Unmarshal([]byte(value), &record.input); err != nil {
			t.Fatalf("decode System sharded business input: %v", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read System sharded business rows: %v", err)
	}
	return records
}

func assertJobShardedExecution(t testing.TB, fixture *jobLifecycleFixture, node *jobLifecycleNode, jobID, instanceID int64, params, mode string) {
	t.Helper()
	instance := readJobShardedInstance(t, node, instanceID)
	wantStatus, wantReason, wantTasks := int32(3), "success", 5
	if mode == "wait" {
		wantStatus, wantReason, wantTasks = 5, "manual_stop", 4
	}
	if instance.JobId != jobID || instance.Status != wantStatus || instance.GetEndReason() != wantReason ||
		instance.StartedAt == nil || instance.DeadlineAt == nil || instance.FinishedAt == nil {
		t.Fatalf("incorrect sharded Instance outcome: status=%d reason=%s", instance.Status, instance.GetEndReason())
	}
	tasks := readJobShardedTasks(t, node, instanceID)
	if len(tasks) != wantTasks {
		t.Fatalf("sharded Task count = %d, want %d", len(tasks), wantTasks)
	}
	records := readJobShardedBusiness(t, fixture, instanceID)
	byTask := make(map[int64][]jobShardedBusinessRecord)
	for _, record := range records {
		if record.instanceID != instanceID || record.jobID != jobID || record.input.Params != params || !record.finished.Valid || record.finished.Time.Before(record.started) {
			t.Fatal("business input identity, frozen parameters or completion timestamp changed")
		}
		byTask[record.taskID] = append(byTask[record.taskID], record)
	}
	if len(byTask) != len(tasks) {
		t.Fatal("public Task identities differ from actual System business calls")
	}
	wantItems := make(map[string]bool)
	for _, item := range jobShardedItems() {
		wantItems[canonicalJobShardedJSON(t, item)] = true
	}
	stageCounts := make(map[int32]int)
	var preFinished, shardsFinished time.Time
	var shardCalls, postCalls []jobShardedBusinessRecord
	for _, task := range tasks {
		stageCounts[task.TaskType]++
		calls := byTask[task.Id]
		if len(calls) == 0 {
			t.Fatal("a Task has no actual business invocation")
		}
		wantAttempts := int64(1)
		if mode == "post_retry" && task.TaskType == 4 || mode == "shard_retry" && task.TaskType == 3 && canonicalJobShardedJSON(t, calls[0].input.Item) == `"first"` {
			wantAttempts = 2
		}
		if int64(len(calls)) != wantAttempts || task.AttemptNo != wantAttempts || int64(task.UsedAttempts) != wantAttempts ||
			task.StartedAt == nil || task.FinishedAt == nil || task.NextDispatchAt != nil {
			t.Fatalf("stage %d repeated, lost a retry or remained unfinished: attempts=%d calls=%d", task.TaskType, task.AttemptNo, len(calls))
		}
		wantTaskStatus, wantTaskReason := int32(4), "success"
		if mode == "wait" && task.TaskType == 3 {
			wantTaskStatus, wantTaskReason = 6, "manual_stop"
			// A cancellation-aware processor returns a failure result. If it
			// commits before the independent database cancellation, that final
			// failure is retained; either ordering must leave the child terminal.
			if task.Status == 5 {
				wantTaskStatus, wantTaskReason = 5, "execution_failed"
			}
		}
		if task.Status != wantTaskStatus || task.GetEndReason() != wantTaskReason {
			t.Fatalf("incorrect stage %d terminal outcome: status=%d reason=%s", task.TaskType, task.Status, task.GetEndReason())
		}
		for index, call := range calls {
			wantState := "succeeded"
			if wantAttempts == 2 && index == 0 {
				wantState = "failed"
			} else if mode == "wait" && task.TaskType == 3 {
				wantState = "canceled"
			}
			if call.invocation != int64(index+1) || call.state != wantState {
				t.Fatal("business invocation count or outcome does not match the durable Task attempt")
			}
			switch task.TaskType {
			case 2:
				if call.input.Stage != "pre" || call.input.Item != nil || call.input.Post != nil {
					t.Fatal("Pre received another stage's input")
				}
				preFinished = call.finished.Time
			case 3:
				item := canonicalJobShardedJSON(t, call.input.Item)
				if call.input.Stage != "shard" || call.input.Post != nil || item != canonicalJobShardedJSON(t, calls[0].input.Item) {
					t.Fatal("a shard retry changed its fixed input")
				}
				if index == 0 {
					if !wantItems[item] {
						t.Fatal("the scheduler changed, combined or repeated a prepared shard item")
					}
					delete(wantItems, item)
				}
				shardCalls = append(shardCalls, call)
				if call.finished.Time.After(shardsFinished) {
					shardsFinished = call.finished.Time
				}
			case 4:
				counts := call.input.Post
				if call.input.Stage != "post" || call.input.Item != nil || counts == nil || counts.Total != 3 || counts.Succeeded != 3 || counts.Failed != 0 {
					t.Fatal("Post received incorrect or changing final shard statistics")
				}
				postCalls = append(postCalls, call)
			default:
				t.Fatalf("unexpected Task type %d in sharded execution", task.TaskType)
			}
		}
	}
	wantPost := 1
	if mode == "wait" {
		wantPost = 0
	}
	if stageCounts[2] != 1 || stageCounts[3] != 3 || stageCounts[4] != wantPost || len(wantItems) != 0 {
		t.Fatalf("incorrect stage cardinality: %v", stageCounts)
	}
	for _, call := range shardCalls {
		if call.started.Before(preFinished) {
			t.Fatal("a business shard started before preparation completed")
		}
	}
	for _, call := range postCalls {
		if call.started.Before(shardsFinished) {
			t.Fatal("Post started before the final shard business call completed")
		}
	}
}

func canonicalJobShardedJSON(t testing.TB, value json.RawMessage) string {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode exact sharded JSON value: %v", err)
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("canonicalize sharded JSON value: %v", err)
	}
	return string(encoded)
}
