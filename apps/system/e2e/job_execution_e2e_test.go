//go:build integration && e2e

package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	commonsubcode "github.com/tokyolab/dogx/pkg/subcode"
	testutil "github.com/tokyolab/dogx/pkg/testutil/postgres"
	"github.com/zeromicro/go-zero/core/stores/redis"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestJobOrdinaryExecutionEndToEnd uses the production API and Job binaries.
// The System executor test binary calls the production run function with a
// test-only business processor; it never reads Job tables. Process cleanup is
// not a graceful-shutdown test.
func TestJobOrdinaryExecutionEndToEnd(t *testing.T) {
	rpcBinary := requiredBinary(t, "DOGX_E2E_RPC_BINARY")
	apiBinary := requiredBinary(t, "DOGX_E2E_API_BINARY")
	jobBinary := requiredBinary(t, "DOGX_E2E_JOB_RPC_BINARY")
	migrateBinary := requiredBinary(t, "DOGX_E2E_JOB_MIGRATE_BINARY")
	executorBinary := requiredBinary(t, "DOGX_E2E_SYSTEM_EXECUTOR_BINARY")
	db, sqlDB := testutil.OpenPostgres(t)
	applyMigrations(t, sqlDB)
	schema := currentSchema(t, sqlDB)
	admin, _ := seedAdministrator(t, db)
	createSystemJobBusinessTable(t, sqlDB)
	postgresConfig, err := pgx.ParseConfig(testutil.PostgresDSN(t))
	if err != nil {
		t.Fatalf("parse end-to-end PostgreSQL DSN: %v", err)
	}
	redisHost := strings.TrimSpace(os.Getenv("DOGX_TEST_REDIS_HOST"))
	if redisHost == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required for end-to-end tests")
	}
	sessionPrefix := "dogx:test:job-e2e:" + schema + ":session"
	userSessionsPrefix := "dogx:test:job-e2e:" + schema + ":user_sessions"
	policyChannel := "dogx:test:job-e2e:" + schema + ":policy"
	redisClient, err := redis.NewRedis(redis.RedisConf{
		Host: redisHost, Type: redis.NodeType, PingTimeout: time.Second,
		DisableIdentity: true, MaintNotifications: "disabled",
	})
	if err != nil {
		t.Fatalf("create end-to-end Redis client: %v", err)
	}
	cleanupKeys := []string{userSessionsPrefix + ":" + strconv.FormatInt(admin.ID, 10)}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := redisClient.DelCtx(ctx, cleanupKeys...); err != nil {
			t.Errorf("delete Job end-to-end Redis keys: %v", err)
		}
	})
	endpoint := systemE2EEtcd(t)
	prefix := "/dogx/test/job-e2e/" + schema
	// Keep all listeners distinct even if the OS immediately reuses a free port.
	ports := make([]int, 7)
	usedPorts := make(map[int]bool)
	for i := range ports {
		for {
			port := freeTCPPort(t)
			if !usedPorts[port] {
				ports[i], usedPorts[port] = port, true
				break
			}
		}
	}
	apiPort, rpcPort, bootstrapExecutorPort := ports[0], ports[1], ports[2]
	jobPort, resultsPort, executorRPCPort, executorPort := ports[3], ports[4], ports[5], ports[6]
	dir := t.TempDir()
	rpcFile, apiFile := filepath.Join(dir, "system-rpc.yaml"), filepath.Join(dir, "system-api.yaml")
	jobFile, executorFile := filepath.Join(dir, "job-rpc.yaml"), filepath.Join(dir, "system-executor.yaml")
	writeFile(t, rpcFile, rpcConfigYAML(rpcPort, bootstrapExecutorPort, endpoint, prefix+"/bootstrap", sessionPrefix, userSessionsPrefix, policyChannel))
	writeFile(t, apiFile, apiConfigYAML(apiPort, rpcPort, sessionPrefix, policyChannel)+fmt.Sprintf(`
JobRpc:
  Etcd:
    Hosts:
      - %q
    Key: %s/management
  NonBlock: true
  Timeout: 2000
`, endpoint, prefix))
	writeFile(t, jobFile, jobExecutionConfigYAML(jobPort, resultsPort, endpoint, prefix))
	processEnv := append(os.Environ(),
		"DOGX_E2E_ACCESS_SECRET="+e2eAccessSecret,
		"DOGX_E2E_POSTGRES_HOST="+postgresConfig.Host,
		"DOGX_E2E_POSTGRES_PORT="+strconv.FormatUint(uint64(postgresConfig.Port), 10),
		"DOGX_E2E_POSTGRES_USER="+postgresConfig.User,
		"DOGX_E2E_POSTGRES_PASSWORD="+postgresConfig.Password,
		"DOGX_E2E_POSTGRES_DATABASE="+postgresConfig.Database,
		"DOGX_E2E_REDIS_HOST="+redisHost,
		"PGOPTIONS=-c search_path="+schema,
	)
	// Use the domain's real migration command instead of importing Job internals.
	migrateCtx, stopMigration := context.WithTimeout(context.Background(), 30*time.Second)
	migrate := exec.CommandContext(migrateCtx, migrateBinary, "-f", jobFile, "up")
	migrate.Env = processEnv
	output, err := migrate.CombinedOutput()
	stopMigration()
	if err != nil {
		t.Fatalf("run Job migration CLI: %v\n%s", err, output)
	}
	jobProcess := startProcess(t, "job-rpc", jobBinary, processEnv, "-f", jobFile)
	rpcProcess := startProcess(t, "system-rpc", rpcBinary, processEnv, "-f", rpcFile)
	apiProcess := startProcess(t, "system-api", apiBinary, processEnv, "-f", apiFile)
	waitForRPCReady(t, rpcPort, rpcProcess)
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	baseURL := "http://127.0.0.1:" + strconv.Itoa(apiPort)
	waitForAPIProbe(t, client, baseURL+"/ready", apiProcess, rpcProcess)
	discovery, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("create end-to-end discovery client: %v", err)
	}
	t.Cleanup(func() { _ = discovery.Close() })
	jobE2EWaitDiscovery(t, discovery, prefix+"/management", jobProcess)
	jobE2EWaitDiscovery(t, discovery, prefix+"/results", jobProcess)
	login := func(username string) string {
		t.Helper()
		status, response := postJSON(t, client, baseURL+"/auth/login", "", map[string]any{"username": username, "password": e2ePassword})
		assertEnvelope(t, status, response, http.StatusOK, 0, "")
		var credentials loginData
		decodeData(t, response, &credentials)
		sessionID, _, ok := strings.Cut(credentials.RefreshToken, ".")
		if credentials.AccessToken == "" || sessionID == "" || !ok {
			t.Fatal("login returned incomplete credentials")
		}
		cleanupKeys = append(cleanupKeys, sessionPrefix+":"+sessionID)
		return credentials.AccessToken
	}
	adminToken := login(admin.Username)
	status, response := postJSON(t, client, baseURL+"/role/create", adminToken, map[string]any{
		"code": "job_e2e_operator", "name": "Job E2E operator", "sort": 0, "status": 1,
	})
	assertEnvelope(t, status, response, http.StatusOK, 0, "")
	var role createdRoleData
	decodeData(t, response, &role)
	operator := seedUserWithRole(t, db, role.ID, "job-e2e-operator")
	cleanupKeys = append(cleanupKeys, userSessionsPrefix+":"+strconv.FormatInt(operator.ID, 10))
	operatorToken := login(operator.Username)
	routes := []struct {
		path string
		body map[string]any
	}{
		{"/job/group/create", map[string]any{"name": "forbidden-group", "discoveryKey": prefix + "/forbidden"}},
		{"/job/group/get", map[string]any{"id": 1}},
		{"/job/group/list", map[string]any{"page": 1, "pageSize": 20}},
		{"/job/group/delete", map[string]any{"id": 1}},
		{"/job/instance/list", map[string]any{"page": 1, "pageSize": 20}},
		{"/job/instance/get", map[string]any{"id": 1}},
		{"/job/instance/stop", map[string]any{"id": 1}},
		{"/job/task/get", map[string]any{"id": 1}},
		{"/job/task/list", map[string]any{"instanceId": 1, "page": 1, "pageSize": 20}},
	}
	var grantedPaths []string
	for _, route := range routes {
		jobE2EAssertAnonymous(t, client, baseURL+route.path, route.body)
		status, response := postJSON(t, client, baseURL+route.path, operatorToken, route.body)
		assertEnvelope(t, status, response, http.StatusForbidden, http.StatusForbidden, commonsubcode.PermissionDenied)
		grantedPaths = append(grantedPaths, route.path)
	}
	grantedPaths = append(grantedPaths, "/job/create", "/job/trigger", "/job/delete")
	var resources []model.API
	if err := db.Where("path IN ? AND method = ?", grantedPaths, http.MethodPost).Find(&resources).Error; err != nil {
		t.Fatalf("load Job API resources: %v", err)
	}
	if len(resources) != len(grantedPaths) {
		t.Fatalf("Job API resources = %d, want %d", len(resources), len(grantedPaths))
	}
	apiIDs := make([]int64, 0, len(resources))
	for _, resource := range resources {
		apiIDs = append(apiIDs, resource.ID)
	}
	status, response = postJSON(t, client, baseURL+"/role/api/update", adminToken, map[string]any{"roleId": role.ID, "apiIds": apiIDs})
	assertEnvelope(t, status, response, http.StatusOK, 0, "")
	waitForPermissionGrant(t, client, baseURL+"/job/group/list", operatorToken, map[string]any{"page": 1, "pageSize": 20}, apiProcess)
	call := func(t testing.TB, path string, body any) responseEnvelope {
		t.Helper()
		status, response := postJSON(t, client, baseURL+path, operatorToken, body)
		assertEnvelope(t, status, response, http.StatusOK, 0, "")
		return response
	}
	groupName, groupKey := "system-e2e", prefix+"/executor"
	var groupID createdRoleData
	decodeData(t, call(t, "/job/group/create", map[string]any{"name": groupName, "discoveryKey": groupKey}), &groupID)
	var group struct {
		ID           int64  `json:"id"`
		Name         string `json:"name"`
		DiscoveryKey string `json:"discoveryKey"`
		Token        string `json:"token"`
	}
	groupResponse := call(t, "/job/group/get", map[string]any{"id": groupID.ID})
	if err := json.Unmarshal(groupResponse.Data, &group); err != nil {
		t.Fatalf("decode group detail without exposing its credential: %v", err)
	}
	if group.ID != groupID.ID || group.ID <= 0 || group.Name != groupName || group.DiscoveryKey != groupKey || len(group.Token) != 32 {
		t.Fatal("group detail returned incorrect identity or credential format")
	}
	if _, err := hex.DecodeString(group.Token); err != nil {
		t.Fatal("group credential is not a 32-character hexadecimal UUID")
	}
	list := call(t, "/job/group/list", map[string]any{"page": 1, "pageSize": 20})
	if bytes.Contains(list.Data, []byte(group.Token)) || bytes.Contains(list.Data, []byte(`"token"`)) {
		t.Fatal("group list exposed a credential")
	}
	var listed struct {
		Total int64 `json:"total"`
		Items []struct {
			ID           int64  `json:"id"`
			Name         string `json:"name"`
			DiscoveryKey string `json:"discoveryKey"`
		} `json:"items"`
	}
	decodeData(t, list, &listed)
	if listed.Total != 1 || len(listed.Items) != 1 || listed.Items[0].ID != group.ID ||
		listed.Items[0].Name != group.Name || listed.Items[0].DiscoveryKey != group.DiscoveryKey {
		t.Fatalf("group list did not return the created group: %+v", listed)
	}
	for _, conflict := range []struct{ name, key, subcode string }{
		{groupName, prefix + "/other", "job.executor_group.name_exists"},
		{"other-workers", groupKey, "job.executor_group.discovery_key_exists"},
	} {
		status, response := postJSON(t, client, baseURL+"/job/group/create", operatorToken, map[string]any{"name": conflict.name, "discoveryKey": conflict.key})
		assertEnvelope(t, status, response, http.StatusOK, 100001, conflict.subcode)
	}
	// The bootstrap System process serves authentication only. This second host
	// registers the test business processor under the actual group's discovery key.
	executorConfig := rpcConfigYAML(executorRPCPort, executorPort, endpoint, prefix, sessionPrefix, userSessionsPrefix, policyChannel)
	executorConfig = strings.Replace(executorConfig, `Token: "00000000000040008000000000000001"`, `Token: "`+group.Token+`"`, 1)
	writeFile(t, executorFile, executorConfig)
	executorEnv := append(append([]string(nil), processEnv...), "DOGX_E2E_SYSTEM_CONFIG="+executorFile)
	executorProcess := startProcess(t, "system-executor-fixture", executorBinary, executorEnv, "-test.run=^TestJobExecutorHostProcess$")
	waitForRPCReady(t, executorRPCPort, executorProcess)
	jobE2EWaitDiscovery(t, discovery, groupKey, executorProcess)
	processes := []*runningProcess{jobProcess, rpcProcess, apiProcess, executorProcess}
	t.Cleanup(func() {
		// Finish the existing forced process cleanup before inspecting logs, so
		// the credential check also covers output emitted during process exit.
		for _, process := range processes {
			process.cancel()
		}
		for _, process := range processes {
			select {
			case <-process.done:
			case <-time.After(5 * time.Second):
				_ = process.cmd.Process.Kill()
				<-process.done
			}
			if strings.Contains(process.logs.String(), group.Token) {
				t.Errorf("%s logs exposed the generated group credential", process.name)
			}
		}
	})
	readHistory := func(t testing.TB, filter map[string]any) jobE2EInstanceList {
		t.Helper()
		response := call(t, "/job/instance/list", filter)
		if bytes.Contains(response.Data, []byte(`"executionConfig"`)) || bytes.Contains(response.Data, []byte(`"resultSummary"`)) {
			t.Fatal("Instance history exposed detail-only fields")
		}
		var list jobE2EInstanceList
		decodeData(t, response, &list)
		return list
	}
	assertHistory := func(t testing.TB, instance jobE2EInstance) {
		t.Helper()
		created, err := time.Parse(time.RFC3339Nano, instance.CreatedAt)
		if err != nil {
			t.Fatalf("parse Instance creation time: %v", err)
		}
		list := readHistory(t, map[string]any{
			"page": 1, "pageSize": 1, "id": instance.ID, "jobId": instance.JobID,
			"jobName": instance.JobName, "executorGroupId": group.ID,
			"status": instance.Status, "triggerType": 3,
			"createdFrom": instance.CreatedAt, "createdTo": created.Add(time.Second).Format(time.RFC3339Nano),
		})
		if list.Total != 1 || len(list.Items) != 1 {
			t.Fatalf("filtered Instance history = %+v, want one matching record", list)
		}
		item := list.Items[0]
		if item.ID != instance.ID || item.JobID != instance.JobID || item.JobName != instance.JobName ||
			item.ExecutorGroupID != group.ID || item.ExecutorGroupName == nil || *item.ExecutorGroupName != group.Name ||
			item.Status != instance.Status || item.TriggerType != 3 || item.CreatedAt != instance.CreatedAt {
			t.Fatalf("unexpected Instance history: %+v", item)
		}
	}
	assertTaskDetail := func(t testing.TB, task jobE2ETask, mode string) {
		t.Helper()
		for _, kind := range []string{"", "input", "result"} {
			body := map[string]any{"id": task.ID}
			if kind != "" {
				body["dataKind"] = kind
			}
			var detail struct {
				Task jobE2ETask `json:"task"`
				Data *string    `json:"data"`
			}
			decodeData(t, call(t, "/job/task/get", body), &detail)
			if detail.Task.ID != task.ID || detail.Task.InstanceID != task.InstanceID || detail.Task.TaskType != 1 ||
				detail.Task.Status != task.Status || detail.Task.AttemptNo != task.AttemptNo || detail.Task.UsedAttempts != task.UsedAttempts ||
				detail.Task.EndReason == nil || task.EndReason == nil || *detail.Task.EndReason != *task.EndReason {
				t.Fatalf("unexpected %q Task detail: %+v", kind, detail.Task)
			}
			if kind != "result" || mode == "wait" {
				if detail.Data != nil {
					t.Fatalf("unexpected data in %q Task detail for %s", kind, mode)
				}
				continue
			}
			if detail.Data == nil {
				t.Fatal("Task result detail omitted persisted business data")
			}
			var result struct {
				Mode         string `json:"mode"`
				Value        string `json:"value"`
				InvocationNo int64  `json:"invocation_no"`
			}
			if err := json.Unmarshal([]byte(*detail.Data), &result); err != nil {
				t.Fatalf("decode Task business result: %v", err)
			}
			if result.Mode != mode || result.Value != "persisted-"+mode || result.InvocationNo != task.AttemptNo {
				t.Fatalf("unexpected persisted Task business result: %+v", result)
			}
		}
	}
	var history []struct {
		instance jobE2EInstance
		task     jobE2ETask
		mode     string
	}
	var jobIDs []int64
	for _, scenario := range []struct {
		mode       string
		maxRetries int
	}{
		{"success", 0}, {"retry", 1}, {"wait", 0},
	} {
		t.Run(scenario.mode, func(t *testing.T) {
			params, err := json.Marshal(map[string]string{"mode": scenario.mode, "value": "persisted-" + scenario.mode})
			if err != nil {
				t.Fatal(err)
			}
			var definition createdRoleData
			decodeData(t, call(t, "/job/create", map[string]any{
				"name": "e2e-" + scenario.mode, "executorGroupId": group.ID,
				"processorName": "system.e2e.persist", "executionMode": 1, "postEnabled": false,
				"params": string(params), "cronExpr": "0 0 0 1 1 *", "status": 0,
				"maxRetries": scenario.maxRetries, "retryIntervalSeconds": 1, "timeoutSeconds": 120,
			}), &definition)
			jobIDs = append(jobIDs, definition.ID)
			status, response := postJSON(t, client, baseURL+"/job/group/delete", operatorToken, map[string]any{"id": group.ID})
			assertEnvelope(t, status, response, http.StatusOK, 100001, "job.executor_group.in_use")
			var trigger struct {
				InstanceID int64 `json:"instanceId"`
			}
			decodeData(t, call(t, "/job/trigger", map[string]any{"id": definition.ID, "requestId": uuid.NewString(), "paramsOverridden": false}), &trigger)
			if trigger.InstanceID <= 0 {
				t.Fatal("manual trigger returned no Instance ID")
			}
			var initialTaskID int64
			if scenario.mode == "wait" {
				jobE2EEventually(t, processes, "business processor started with a saved cancellation target", func() bool {
					tasks := jobE2ETasks(t, call(t, "/job/task/list", map[string]any{"instanceId": trigger.InstanceID, "page": 1, "pageSize": 20}))
					if len(tasks.Items) != 1 || tasks.Items[0].Status != 2 || tasks.Items[0].ExecutorAddress == nil {
						return false
					}
					initialTaskID = tasks.Items[0].ID
					return jobE2EBusinessState(t, sqlDB, initialTaskID, 1) == "running"
				})
				var stop jobE2EStop
				decodeData(t, call(t, "/job/instance/stop", map[string]any{"id": trigger.InstanceID}), &stop)
				if stop.InstanceID != trigger.InstanceID || stop.Status != 6 || stop.EndReason == nil || *stop.EndReason != "manual_stop" {
					t.Fatalf("manual stop did not acknowledge STOPPING: %+v", stop)
				}
			}
			wantInstance, wantTask, wantAttempt, wantReason := int32(3), int32(4), int64(1), "success"
			if scenario.mode == "retry" {
				wantAttempt = 2
			}
			if scenario.mode == "wait" {
				wantInstance, wantTask, wantReason = 5, 6, "manual_stop"
			}
			taskFinished := func(status int32) bool {
				// The canceled processor can report failure before database child
				// cancellation. Both terminal outcomes are valid; stopping does not
				// overwrite an already committed child result.
				return status == wantTask || scenario.mode == "wait" && status == 5
			}
			var instance jobE2EInstance
			var tasks jobE2ETaskList
			jobE2EEventually(t, processes, "ordinary Instance reached its expected terminal state", func() bool {
				decodeData(t, call(t, "/job/instance/get", map[string]any{"id": trigger.InstanceID}), &instance)
				tasks = jobE2ETasks(t, call(t, "/job/task/list", map[string]any{"instanceId": trigger.InstanceID, "page": 1, "pageSize": 20}))
				if instance.Status == 4 {
					t.Fatalf("ordinary Instance failed unexpectedly: %+v, tasks=%+v", instance, tasks)
				}
				return instance.Status == wantInstance && len(tasks.Items) == 1 && taskFinished(tasks.Items[0].Status)
			})
			task := tasks.Items[0]
			wantTaskReason := wantReason
			if scenario.mode == "wait" && task.Status == 5 {
				wantTaskReason = "execution_failed"
			}
			if instance.ID != trigger.InstanceID || instance.JobID != definition.ID || instance.JobName != "e2e-"+scenario.mode ||
				instance.TriggeredBy == nil || *instance.TriggeredBy != operator.ID || string(instance.ShardStats) != "null" ||
				instance.EndReason == nil || *instance.EndReason != wantReason || instance.FinishedAt == nil ||
				tasks.Total != 1 || task.InstanceID != trigger.InstanceID || task.AttemptNo != wantAttempt || task.UsedAttempts != int32(wantAttempt) ||
				task.EndReason == nil || *task.EndReason != wantTaskReason || task.FinishedAt == nil {
				t.Fatalf("unexpected execution facts: instance=%+v tasks=%+v", instance, tasks)
			}
			if initialTaskID != 0 && task.ID != initialTaskID {
				t.Fatal("stopping replaced the accepted Task identity")
			}
			assertHistory(t, instance)
			for _, filter := range []map[string]any{
				{"page": 1, "pageSize": 1, "jobId": instance.JobID, "createdTo": instance.CreatedAt},
				{"page": 1, "pageSize": 1, "jobId": instance.JobID, "triggerType": 1},
				{"page": 1, "pageSize": 1, "jobId": instance.JobID, "jobName": "missing-" + instance.JobName},
			} {
				list := readHistory(t, filter)
				if list.Total != 0 || len(list.Items) != 0 {
					t.Fatalf("nonmatching history filter %v returned %+v", filter, list)
				}
			}
			page := readHistory(t, map[string]any{"page": 2, "pageSize": 1, "jobId": instance.JobID})
			if page.Total != 1 || len(page.Items) != 0 {
				t.Fatalf("history pagination did not preserve the total: %+v", page)
			}
			assertTaskDetail(t, task, scenario.mode)
			wantBusiness := "succeeded"
			if scenario.mode == "wait" {
				wantBusiness = "canceled"
			}
			jobE2EEventually(t, processes, "System business state persisted", func() bool {
				return jobE2EBusinessState(t, sqlDB, task.ID, wantAttempt) == wantBusiness
			})
			jobE2EAssertBusiness(t, sqlDB, task.ID, trigger.InstanceID, definition.ID, "persisted-"+scenario.mode, wantAttempt)
			if scenario.mode == "retry" && jobE2EBusinessState(t, sqlDB, task.ID, 1) != "failed" {
				t.Fatal("retry did not preserve the first business invocation under the same Task")
			}
			var terminalStop jobE2EStop
			decodeData(t, call(t, "/job/instance/stop", map[string]any{"id": trigger.InstanceID}), &terminalStop)
			if terminalStop.Status != wantInstance || terminalStop.EndReason == nil || *terminalStop.EndReason != wantReason {
				t.Fatalf("terminal stop changed the existing result: %+v", terminalStop)
			}
			history = append(history, struct {
				instance jobE2EInstance
				task     jobE2ETask
				mode     string
			}{instance: instance, task: task, mode: scenario.mode})
		})
	}
	if t.Failed() {
		return
	}
	for _, id := range jobIDs {
		call(t, "/job/delete", map[string]any{"id": id})
	}
	call(t, "/job/group/delete", map[string]any{"id": group.ID})
	status, response = postJSON(t, client, baseURL+"/job/group/get", operatorToken, map[string]any{"id": group.ID})
	assertEnvelope(t, status, response, http.StatusOK, 100001, "job.executor_group.not_found")
	for _, record := range history {
		assertHistory(t, record.instance)
		var instance jobE2EInstance
		decodeData(t, call(t, "/job/instance/get", map[string]any{"id": record.instance.ID}), &instance)
		if instance.ID != record.instance.ID || instance.JobID != record.instance.JobID || instance.JobName != record.instance.JobName ||
			instance.Status != record.instance.Status || string(instance.ShardStats) != "null" {
			t.Fatalf("deleted Job lost its historical Instance detail: %+v", instance)
		}
		tasks := jobE2ETasks(t, call(t, "/job/task/list", map[string]any{"instanceId": instance.ID, "page": 1, "pageSize": 20}))
		if tasks.Total != 1 || len(tasks.Items) != 1 || tasks.Items[0].ID != record.task.ID {
			t.Fatalf("deleted Job lost its historical Task list: %+v", tasks)
		}
		assertTaskDetail(t, record.task, record.mode)
	}
	status, response = postJSON(t, client, baseURL+"/role/api/update", adminToken, map[string]any{"roleId": role.ID, "apiIds": []int64{}})
	assertEnvelope(t, status, response, http.StatusOK, 0, "")
	waitForPermissionRevocation(t, client, baseURL+"/job/group/list", operatorToken, map[string]any{"page": 1, "pageSize": 20}, apiProcess)
}

type jobE2EInstance struct {
	ID          int64           `json:"id"`
	JobID       int64           `json:"jobId"`
	JobName     string          `json:"jobName"`
	Status      int32           `json:"status"`
	TriggeredBy *int64          `json:"triggeredBy"`
	EndReason   *string         `json:"endReason"`
	FinishedAt  *string         `json:"finishedAt"`
	CreatedAt   string          `json:"createdAt"`
	ShardStats  json.RawMessage `json:"shardStats"`
}

type jobE2EInstanceList struct {
	Total int64 `json:"total"`
	Items []struct {
		ID                int64   `json:"id"`
		JobID             int64   `json:"jobId"`
		JobName           string  `json:"jobName"`
		ExecutorGroupID   int64   `json:"executorGroupId"`
		ExecutorGroupName *string `json:"executorGroupName"`
		Status            int32   `json:"status"`
		TriggerType       int32   `json:"triggerType"`
		CreatedAt         string  `json:"createdAt"`
	} `json:"items"`
}

type jobE2EStop struct {
	InstanceID int64   `json:"instanceId"`
	Status     int32   `json:"status"`
	EndReason  *string `json:"endReason"`
}

type jobE2ETaskList struct {
	Total int64        `json:"total"`
	Items []jobE2ETask `json:"items"`
}

type jobE2ETask struct {
	ID              int64   `json:"id"`
	InstanceID      int64   `json:"instanceId"`
	TaskType        int32   `json:"taskType"`
	Status          int32   `json:"status"`
	AttemptNo       int64   `json:"attemptNo"`
	UsedAttempts    int32   `json:"usedAttempts"`
	ExecutorAddress *string `json:"executorAddress"`
	EndReason       *string `json:"endReason"`
	FinishedAt      *string `json:"finishedAt"`
}

func jobE2ETasks(t testing.TB, response responseEnvelope) jobE2ETaskList {
	t.Helper()
	if bytes.Contains(response.Data, []byte(`"resultData"`)) {
		t.Fatal("Task management query exposed business result data")
	}
	var tasks jobE2ETaskList
	decodeData(t, response, &tasks)
	return tasks
}

func jobE2EAssertAnonymous(t testing.TB, client *http.Client, url string, body any) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("call anonymous Job route: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous %s returned HTTP %d", url, response.StatusCode)
	}
}

func jobE2EEventually(t testing.TB, processes []*runningProcess, description string, check func() bool) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, process := range processes {
			select {
			case <-process.done:
				t.Fatal(process.failure())
			default:
			}
		}
		if check() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func jobE2EWaitDiscovery(t testing.TB, client *clientv3.Client, key string, process *runningProcess) {
	t.Helper()
	jobE2EEventually(t, []*runningProcess{process}, "RPC discovery registration", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := client.Get(ctx, key+"/", clientv3.WithPrefix())
		return err == nil && len(response.Kvs) > 0
	})
}

func jobE2EBusinessState(t testing.TB, db *sql.DB, taskID, invocation int64) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var state string
	err := db.QueryRowContext(ctx, "SELECT state FROM system_job_e2e_records WHERE task_id = $1 AND invocation_no = $2", taskID, invocation).Scan(&state)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("read System business fixture: %v", err)
	}
	return state
}

func jobE2EAssertBusiness(t testing.TB, db *sql.DB, taskID, instanceID, jobID int64, value string, count int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var total, matching int64
	err := db.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE task_id = $1 AND job_id = $2 AND value = $3)
		FROM system_job_e2e_records WHERE instance_id = $4`, taskID, jobID, value, instanceID).Scan(&total, &matching)
	if err != nil {
		t.Fatalf("read persisted System execution facts: %v", err)
	}
	if total != count || matching != count {
		t.Fatalf("System business records: total=%d matching=%d, want %d", total, matching, count)
	}
}

func jobExecutionConfigYAML(port, resultPort int, endpoint, prefix string) string {
	return fmt.Sprintf(`Name: job-rpc-e2e
ListenOn: 127.0.0.1:%d
Mode: test
Timeout: 5000
DevServer:
  Enabled: false
Etcd:
  Hosts:
    - %q
  Key: %s/management
ExecutionRPC:
  Name: job-result-e2e
  ListenOn: 127.0.0.1:%d
  Auth: false
  Health: true
  Etcd:
    Hosts:
      - %q
    Key: %s/results
Scheduler:
  Prefix: %s
Dispatch:
  ScanInterval: 100ms
Postgres:
  Host: ${DOGX_E2E_POSTGRES_HOST}
  Port: ${DOGX_E2E_POSTGRES_PORT}
  User: ${DOGX_E2E_POSTGRES_USER}
  Password: ${DOGX_E2E_POSTGRES_PASSWORD}
  Database: ${DOGX_E2E_POSTGRES_DATABASE}
  SSLMode: disable
  TimeZone: UTC
  MaxIdleConns: 2
  MaxOpenConns: 10
  ConnMaxLifetime: 5m
`, port, endpoint, prefix, resultPort, endpoint, prefix, prefix)
}
