//go:build integration && e2e

package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gormadapter "github.com/casbin/gorm-adapter/v3"
	"github.com/jackc/pgx/v5"
	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/gorm"
)

const (
	e2ePassword     = "dogx-e2e-password"
	e2eAccessSecret = "dogx-e2e-access-secret-0123456789abcdef"
)

type responseEnvelope struct {
	Code    uint32          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type loginData struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresIn    int64  `json:"expiresIn"`
}

type currentUserData struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Nickname string `json:"nickname"`
}

type createdRoleData struct {
	ID int64 `json:"id"`
}

type roleData struct {
	ID          int64  `json:"id"`
	Code        string `json:"code"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Sort        int64  `json:"sort"`
	Status      int64  `json:"status"`
	IsSystem    bool   `json:"isSystem"`
}

func TestSystemAuthenticationAndRBACEndToEnd(t *testing.T) {
	rpcBinary := requiredBinary(t, "DOGX_E2E_RPC_BINARY")
	apiBinary := requiredBinary(t, "DOGX_E2E_API_BINARY")

	gormDB, sqlDB := testutil.OpenPostgres(t)
	applyMigrations(t, sqlDB)
	schema := currentSchema(t, sqlDB)
	user, role := seedAdministrator(t, gormDB)

	redisHost := strings.TrimSpace(os.Getenv("DOGX_TEST_REDIS_HOST"))
	if redisHost == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required for end-to-end tests")
	}
	sessionPrefix := "dogx:test:e2e:" + schema + ":session"
	userSessionsPrefix := "dogx:test:e2e:" + schema + ":user_sessions"
	policyChannel := "dogx:test:e2e:" + schema + ":authorization:policy"
	redisClient, err := redis.NewRedis(redis.RedisConf{
		Host:               redisHost,
		Type:               redis.NodeType,
		PingTimeout:        time.Second,
		DisableIdentity:    true,
		MaintNotifications: "disabled",
	})
	if err != nil {
		t.Fatalf("create end-to-end Redis client: %v", err)
	}
	cleanupUserIDs := []int64{user.ID}
	cleanupSessionIDs := make([]string, 0, 3)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		keys := make([]string, 0, len(cleanupUserIDs)+len(cleanupSessionIDs))
		for _, userID := range cleanupUserIDs {
			keys = append(keys, userSessionsPrefix+":"+strconv.FormatInt(userID, 10))
		}
		for _, sessionID := range cleanupSessionIDs {
			keys = append(keys, sessionPrefix+":"+sessionID)
		}
		if _, err := redisClient.DelCtx(cleanupCtx, keys...); err != nil {
			t.Errorf("delete end-to-end Redis keys: %v", err)
		}
	})

	postgresConfig, err := pgx.ParseConfig(testutil.PostgresDSN(t))
	if err != nil {
		t.Fatalf("parse end-to-end PostgreSQL DSN: %v", err)
	}
	rpcPort := freeTCPPort(t)
	apiPort := freeTCPPort(t)
	configDir := t.TempDir()
	rpcConfig := filepath.Join(configDir, "system-rpc.yaml")
	apiConfig := filepath.Join(configDir, "system-api.yaml")
	writeFile(t, rpcConfig, rpcConfigYAML(rpcPort, sessionPrefix, userSessionsPrefix, policyChannel))
	writeFile(t, apiConfig, apiConfigYAML(apiPort, rpcPort, sessionPrefix, policyChannel))

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
	rpcProcess := startProcess(t, "system-rpc", rpcBinary, processEnv, "-f", rpcConfig)
	apiProcess := startProcess(t, "system-api", apiBinary, processEnv, "-f", apiConfig)

	baseURL := "http://127.0.0.1:" + strconv.Itoa(apiPort)
	client := &http.Client{Timeout: 2 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	waitForAPIReady(t, client, baseURL+"/ready", rpcProcess, apiProcess)

	statusCode, envelope := postJSON(t, client, baseURL+"/auth/login", "", map[string]any{
		"username": user.Username,
		"password": e2ePassword,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	var credentials loginData
	decodeData(t, envelope, &credentials)
	if credentials.AccessToken == "" || credentials.RefreshToken == "" || credentials.ExpiresIn <= 0 {
		t.Fatalf("login returned incomplete credentials: %+v", credentials)
	}
	sessionID, _, _ := strings.Cut(credentials.RefreshToken, ".")
	if sessionID == "" || sessionID == credentials.RefreshToken {
		t.Fatal("login returned an invalid refresh token")
	}
	cleanupSessionIDs = append(cleanupSessionIDs, sessionID)

	statusCode, envelope = postJSON(t, client, baseURL+"/auth/me", credentials.AccessToken, nil)
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	var currentUser currentUserData
	decodeData(t, envelope, &currentUser)
	if currentUser.ID != user.ID || currentUser.Username != user.Username || currentUser.Nickname != user.Nickname {
		t.Fatalf(
			"unexpected current user: got %+v, want id=%d username=%q nickname=%q",
			currentUser,
			user.ID,
			user.Username,
			user.Nickname,
		)
	}

	statusCode, envelope = postJSON(t, client, baseURL+"/role/create", credentials.AccessToken, map[string]any{
		"code": "e2e_operator", "name": "E2E Operator", "description": "end-to-end role", "sort": 20, "status": 1,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	var createdRole createdRoleData
	decodeData(t, envelope, &createdRole)
	if createdRole.ID <= 0 {
		t.Fatalf("create role returned invalid id: %+v", createdRole)
	}

	statusCode, envelope = postJSON(t, client, baseURL+"/role/update", credentials.AccessToken, map[string]any{
		"id": createdRole.ID, "code": "e2e_auditor", "name": "E2E Auditor", "description": "updated role", "sort": 10,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	statusCode, envelope = postJSON(t, client, baseURL+"/role/get", credentials.AccessToken, map[string]any{
		"id": createdRole.ID,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	var loadedRole roleData
	decodeData(t, envelope, &loadedRole)
	if loadedRole.ID != createdRole.ID || loadedRole.Code != "e2e_auditor" ||
		loadedRole.Name != "E2E Auditor" || loadedRole.Description != "updated role" ||
		loadedRole.Sort != 10 || loadedRole.Status != 1 || loadedRole.IsSystem {
		t.Fatalf("unexpected updated role: %+v", loadedRole)
	}

	roleUser := seedUserWithRole(t, gormDB, createdRole.ID, "dogx-e2e-role-user")
	cleanupUserIDs = append(cleanupUserIDs, roleUser.ID)
	statusCode, envelope = postJSON(t, client, baseURL+"/auth/login", "", map[string]any{
		"username": roleUser.Username,
		"password": e2ePassword,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	var roleCredentials loginData
	decodeData(t, envelope, &roleCredentials)
	roleSessionID, _, _ := strings.Cut(roleCredentials.RefreshToken, ".")
	cleanupSessionIDs = append(cleanupSessionIDs, roleSessionID)

	statusCode, envelope = postJSON(t, client, baseURL+"/role/status/update", credentials.AccessToken, map[string]any{
		"id": createdRole.ID, "status": 0,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	statusCode, envelope = postJSON(t, client, baseURL+"/auth/me", roleCredentials.AccessToken, nil)
	assertEnvelope(t, statusCode, envelope, http.StatusUnauthorized, http.StatusUnauthorized, "authentication required")

	statusCode, envelope = postJSON(t, client, baseURL+"/role/status/update", credentials.AccessToken, map[string]any{
		"id": createdRole.ID, "status": 1,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	statusCode, envelope = postJSON(t, client, baseURL+"/auth/login", "", map[string]any{
		"username": roleUser.Username,
		"password": e2ePassword,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	decodeData(t, envelope, &roleCredentials)
	roleSessionID, _, _ = strings.Cut(roleCredentials.RefreshToken, ".")
	cleanupSessionIDs = append(cleanupSessionIDs, roleSessionID)

	var roleGetAPI model.API
	if err := gormDB.Where("path = ? AND method = ?", "/role/get", http.MethodPost).
		First(&roleGetAPI).Error; err != nil {
		t.Fatalf("load role get API resource: %v", err)
	}
	statusCode, envelope = postJSON(t, client, baseURL+"/role/api/update", credentials.AccessToken, map[string]any{
		"roleId": createdRole.ID, "apiIds": []int64{roleGetAPI.ID},
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	statusCode, envelope = postJSON(t, client, baseURL+"/role/delete", credentials.AccessToken, map[string]any{
		"id": createdRole.ID,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, bizerror.DefaultCode, "角色已被用户使用，不能删除")
	if err := gormDB.Delete(&roleUser).Error; err != nil {
		t.Fatalf("soft delete assigned role user: %v", err)
	}
	statusCode, envelope = postJSON(t, client, baseURL+"/role/delete", credentials.AccessToken, map[string]any{
		"id": createdRole.ID,
	})
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")
	statusCode, envelope = postJSON(t, client, baseURL+"/auth/me", roleCredentials.AccessToken, nil)
	assertEnvelope(t, statusCode, envelope, http.StatusUnauthorized, http.StatusUnauthorized, "authentication required")
	assertRoleDeletionPersisted(t, gormDB, createdRole.ID)

	updateRequest := map[string]any{"roleId": role.ID, "apiIds": []int64{}}
	statusCode, envelope = postJSON(
		t,
		client,
		baseURL+"/role/api/update",
		credentials.AccessToken,
		updateRequest,
	)
	assertEnvelope(t, statusCode, envelope, http.StatusOK, 0, "success")

	waitForPermissionRevocation(
		t,
		client,
		baseURL+"/role/api/update",
		credentials.AccessToken,
		updateRequest,
		apiProcess,
	)
}

func seedUserWithRole(t testing.TB, db *gorm.DB, roleID int64, username string) model.User {
	t.Helper()
	passwordHash, err := authn.NewArgon2id().Hash(e2ePassword)
	if err != nil {
		t.Fatalf("hash role user password: %v", err)
	}
	user := model.User{
		Username: username, PasswordHash: passwordHash, Nickname: "DogX E2E Role User",
		Status: model.RecordStatusEnabled,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create role user: %v", err)
	}
	if err := db.Create(&model.UserRole{UserID: user.ID, RoleID: roleID}).Error; err != nil {
		t.Fatalf("bind role user: %v", err)
	}
	return user
}

func assertRoleDeletionPersisted(t testing.TB, db *gorm.DB, roleID int64) {
	t.Helper()
	var count int64
	checks := []struct {
		name  string
		query *gorm.DB
	}{
		{name: "active role", query: db.Model(&model.Role{}).Where("id = ?", roleID)},
		{name: "user role", query: db.Model(&model.UserRole{}).Where("role_id = ?", roleID)},
		{name: "role menu", query: db.Model(&model.RoleMenu{}).Where("role_id = ?", roleID)},
		{name: "Casbin policy", query: db.Model(&gormadapter.CasbinRule{}).Where("ptype = ? AND v0 = ?", "p", "r:"+strconv.FormatInt(roleID, 10))},
	}
	for _, check := range checks {
		if err := check.query.Count(&count).Error; err != nil {
			t.Fatalf("count deleted role %s records: %v", check.name, err)
		}
		if count != 0 {
			t.Fatalf("deleted role %s records remain: %d", check.name, count)
		}
	}
}

func applyMigrations(t testing.TB, db *sql.DB) {
	t.Helper()
	provider, err := migration.NewProvider(db)
	if err != nil {
		t.Fatalf("create end-to-end migration provider: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply end-to-end migrations: %v", err)
	}
}

func currentSchema(t testing.TB, db *sql.DB) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var schema string
	if err := db.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatalf("read end-to-end PostgreSQL schema: %v", err)
	}
	if !strings.HasPrefix(schema, "dogx_it_") {
		t.Fatalf("refuse to start end-to-end services in unexpected schema %q", schema)
	}
	return schema
}

func seedAdministrator(t testing.TB, db *gorm.DB) (model.User, model.Role) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	passwordHash, err := authn.NewArgon2id().Hash(e2ePassword)
	if err != nil {
		t.Fatalf("hash end-to-end password: %v", err)
	}
	user := model.User{
		Username:     "dogx-e2e-admin",
		PasswordHash: passwordHash,
		Nickname:     "DogX E2E Admin",
		Status:       model.RecordStatusEnabled,
		Remark:       "end-to-end test administrator",
	}
	if err := db.WithContext(ctx).Create(&user).Error; err != nil {
		t.Fatalf("create end-to-end user: %v", err)
	}

	var role model.Role
	if err := db.WithContext(ctx).Where("code = ?", "super_admin").First(&role).Error; err != nil {
		t.Fatalf("load seeded super administrator role: %v", err)
	}
	if err := db.WithContext(ctx).Create(&model.UserRole{UserID: user.ID, RoleID: role.ID}).Error; err != nil {
		t.Fatalf("bind end-to-end user to super administrator role: %v", err)
	}
	return user, role
}

func requiredBinary(t testing.TB, environmentName string) string {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(environmentName))
	if path == "" {
		t.Fatalf("%s is required for end-to-end tests", environmentName)
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("resolve %s: %v", environmentName, err)
	}
	if info, err := os.Stat(absolutePath); err != nil {
		t.Fatalf("inspect %s binary %q: %v", environmentName, absolutePath, err)
	} else if info.IsDir() {
		t.Fatalf("%s points to a directory: %q", environmentName, absolutePath)
	}
	return absolutePath
}

func freeTCPPort(t testing.TB) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve end-to-end TCP port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release end-to-end TCP port: %v", err)
	}
	return port
}

func writeFile(t testing.TB, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write end-to-end configuration %q: %v", path, err)
	}
}

func rpcConfigYAML(port int, sessionPrefix, userSessionsPrefix, policyChannel string) string {
	return fmt.Sprintf(`Name: system-rpc-e2e
ListenOn: 127.0.0.1:%d
Mode: test
Timeout: 5000

App:
  ReadinessTimeout: 2s

Authentication:
  AccessSecret: ${DOGX_E2E_ACCESS_SECRET}
  AccessExpire: 5m
  RefreshExpire: 1h
  Issuer: dogx-e2e
  SessionKeyPrefix: %s
  UserSessionsKeyPrefix: %s

Authorization:
  PolicyChannel: %s

Postgres:
  Host: ${DOGX_E2E_POSTGRES_HOST}
  Port: ${DOGX_E2E_POSTGRES_PORT}
  User: ${DOGX_E2E_POSTGRES_USER}
  Password: ${DOGX_E2E_POSTGRES_PASSWORD}
  Database: ${DOGX_E2E_POSTGRES_DATABASE}
  SSLMode: disable
  TimeZone: UTC
  MaxIdleConns: 2
  MaxOpenConns: 5
  ConnMaxLifetime: 5m

RedisConf:
  Host: ${DOGX_E2E_REDIS_HOST}
  Type: node
  NonBlock: true
  PingTimeout: 1s
  DisableIdentity: true
  MaintNotifications: disabled
`, port, sessionPrefix, userSessionsPrefix, policyChannel)
}

func apiConfigYAML(port, rpcPort int, sessionPrefix, policyChannel string) string {
	return fmt.Sprintf(`Name: system-api-e2e
Host: 127.0.0.1
Port: %d
Mode: test
Timeout: 5000

App:
  Version: e2e
  ReadinessTimeout: 2s

Auth:
  AccessSecret: ${DOGX_E2E_ACCESS_SECRET}
  SessionKeyPrefix: %s

Authorization:
  PolicyChannel: %s
  ReloadInterval: 1h

Postgres:
  Host: ${DOGX_E2E_POSTGRES_HOST}
  Port: ${DOGX_E2E_POSTGRES_PORT}
  User: ${DOGX_E2E_POSTGRES_USER}
  Password: ${DOGX_E2E_POSTGRES_PASSWORD}
  Database: ${DOGX_E2E_POSTGRES_DATABASE}
  SSLMode: disable
  TimeZone: UTC
  MaxIdleConns: 2
  MaxOpenConns: 5
  ConnMaxLifetime: 5m

RedisConf:
  Host: ${DOGX_E2E_REDIS_HOST}
  Type: node
  NonBlock: true
  PingTimeout: 1s
  DisableIdentity: true
  MaintNotifications: disabled

SystemRpc:
  Endpoints:
    - 127.0.0.1:%d
  NonBlock: true
  Timeout: 2000
`, port, sessionPrefix, policyChannel, rpcPort)
}

type synchronizedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(data)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

type runningProcess struct {
	name   string
	cmd    *exec.Cmd
	cancel context.CancelFunc
	done   chan struct{}
	logs   synchronizedBuffer

	mu  sync.Mutex
	err error
}

func startProcess(
	t testing.TB,
	name string,
	binary string,
	environment []string,
	arguments ...string,
) *runningProcess {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	process := &runningProcess{name: name, cancel: cancel, done: make(chan struct{})}
	process.cmd = exec.CommandContext(ctx, binary, arguments...)
	process.cmd.Env = environment
	process.cmd.Stdout = &process.logs
	process.cmd.Stderr = &process.logs
	if err := process.cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start %s: %v", name, err)
	}
	go func() {
		err := process.cmd.Wait()
		process.mu.Lock()
		process.err = err
		process.mu.Unlock()
		close(process.done)
	}()
	t.Cleanup(func() {
		process.cancel()
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
			_ = process.cmd.Process.Kill()
			<-process.done
		}
	})
	return process
}

func (p *runningProcess) failure() string {
	p.mu.Lock()
	err := p.err
	p.mu.Unlock()
	return fmt.Sprintf("%s exited: %v\n%s", p.name, err, p.logs.String())
}

func waitForAPIReady(
	t testing.TB,
	client *http.Client,
	readyURL string,
	processes ...*runningProcess,
) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	lastResult := "no readiness response"

	for {
		for _, process := range processes {
			select {
			case <-process.done:
				t.Fatal(process.failure())
			default:
			}
		}

		response, err := client.Get(readyURL)
		if err == nil {
			var envelope responseEnvelope
			decodeErr := json.NewDecoder(response.Body).Decode(&envelope)
			_ = response.Body.Close()
			lastResult = fmt.Sprintf(
				"status=%d response=%+v decodeError=%v",
				response.StatusCode,
				envelope,
				decodeErr,
			)
			if response.StatusCode == http.StatusOK && decodeErr == nil && envelope.Code == 0 {
				return
			}
		} else {
			lastResult = err.Error()
		}

		select {
		case <-deadline.C:
			logs := make([]string, 0, len(processes))
			for _, process := range processes {
				logs = append(logs, process.name+" logs:\n"+process.logs.String())
			}
			t.Fatalf("API did not become ready: %s\n%s", lastResult, strings.Join(logs, "\n"))
		case <-ticker.C:
		}
	}
}

func postJSON(
	t testing.TB,
	client *http.Client,
	url string,
	accessToken string,
	requestBody any,
) (int, responseEnvelope) {
	t.Helper()
	var body bytes.Buffer
	if requestBody != nil {
		if err := json.NewEncoder(&body).Encode(requestBody); err != nil {
			t.Fatalf("encode HTTP request for %s: %v", url, err)
		}
	}
	request, err := http.NewRequest(http.MethodPost, url, &body)
	if err != nil {
		t.Fatalf("create HTTP request for %s: %v", url, err)
	}
	request.Header.Set("Content-Type", "application/json")
	if accessToken != "" {
		request.Header.Set("Authorization", "Bearer "+accessToken)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("call %s: %v", url, err)
	}
	defer response.Body.Close()
	var envelope responseEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode HTTP response from %s: status=%d error=%v", url, response.StatusCode, err)
	}
	return response.StatusCode, envelope
}

func assertEnvelope(
	t testing.TB,
	statusCode int,
	envelope responseEnvelope,
	wantStatus int,
	wantCode uint32,
	wantMessage string,
) {
	t.Helper()
	if statusCode != wantStatus || envelope.Code != wantCode || envelope.Message != wantMessage {
		t.Fatalf(
			"unexpected HTTP response: status=%d envelope=%+v, want status=%d code=%d message=%q",
			statusCode,
			envelope,
			wantStatus,
			wantCode,
			wantMessage,
		)
	}
}

func decodeData(t testing.TB, envelope responseEnvelope, target any) {
	t.Helper()
	if err := json.Unmarshal(envelope.Data, target); err != nil {
		t.Fatalf("decode HTTP response data: %v data=%s", err, envelope.Data)
	}
}

func waitForPermissionRevocation(
	t testing.TB,
	client *http.Client,
	url string,
	accessToken string,
	requestBody any,
	apiProcess *runningProcess,
) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-apiProcess.done:
			t.Fatal(apiProcess.failure())
		default:
		}

		statusCode, envelope := postJSON(t, client, url, accessToken, requestBody)
		if statusCode == http.StatusForbidden &&
			envelope.Code == http.StatusForbidden &&
			envelope.Message == "permission denied" {
			return
		}
		if statusCode != http.StatusOK || envelope.Code != 0 {
			t.Fatalf(
				"unexpected response while waiting for permission revocation: status=%d envelope=%+v",
				statusCode,
				envelope,
			)
		}

		select {
		case <-deadline.C:
			t.Fatalf(
				"permission policy was not reloaded after Redis notification\n%s",
				apiProcess.logs.String(),
			)
		case <-ticker.C:
		}
	}
}
