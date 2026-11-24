# DogX 测试方案

> 状态：方案草案。测试基础设施按业务开发进度分阶段落地，不为尚未出现的依赖提前建设测试框架。

## 目标

DogX 的测试用于验证业务行为、对外契约和基础设施兼容性，并保证测试结果可重复、可并行演进。测试不以追求单一覆盖率数字为目标，也不测试自动生成代码的内部实现。

## 测试分层

DogX 采用四层测试：

1. Logic 单元测试：验证业务规则、成功分支和失败分支，是数量最多、执行最频繁的测试。
2. HTTP / gRPC 协议测试：验证参数解析、响应结构、错误转换、中间件和传输行为。
3. PostgreSQL / Redis 集成测试：验证真实数据库方言、迁移、约束、查询和缓存行为。
4. 端到端测试：覆盖登录、刷新令牌、权限判定，以及普通 Job 触发、执行、重试和停止等关键业务闭环。

普通测试不得访问公网、共享开发数据库或其他不受测试进程控制的服务。

## Windows 本地测试 {#windows-local-tests}

在 PowerShell 7 中从仓库根目录运行：

```powershell
$testRunner = 'pwsh -NoProfile -File "' + (Resolve-Path 'scripts/test-exec-windows.ps1').Path + '"'
go test -p 2 -exec $testRunner ./...
```

该入口仍由 Go 负责构建、测试选择及覆盖率参数，仅将每个包的临时测试程序复制到忽略目录 `bin/windows-tests` 的固定路径后执行。包完整相对路径区分同名 `config`／`logic`／`svc`，退出码和测试参数原样交给 Go；同一 checkout 不同时运行多个测试命令，避免覆盖正在执行的同包二进制。`-tags integration` 等标记照常使用；隔离依赖的要求不变。Linux／CI 继续使用原生 Go 命令。

包装进程在启动测试前通过 Windows Job Object 管理测试及其子进程；包装进程退出或被强制终止时，一并结束这些进程，避免取消测试后留下监听服务或占用固定程序文件。该管理只作用于本次测试进程树，不终止外部已运行的开发服务。

有网络的测试仅监听 `127.0.0.1` 随机端口；使用 `conf.FillDefault` 的 zrpc 测试须在构造 Server 前明确设置 `DevServer.Enabled = false`，避免 go-zero 默认额外监听全部网卡的 6060 开发端口。测试不修改生产配置，也不关闭 Windows 防火墙或全局通知。

Windows 程序规则按 `.exe` 完整路径匹配，放行 `go.exe` 或旧的临时 `*.test.exe` 无法覆盖之后生成的新路径。确需本机白名单并取得机器使用者授权后，可以在**管理员 PowerShell 7** 中显式执行 `scripts/allow-local-test-firewall.ps1`。脚本仅为已列明的六个固定测试程序添加入站 TCP 规则，本地和远端地址均限 `127.0.0.1`，不允许局域网／公网客户端；报告写入忽略目录，不改其他程序规则。普通测试本身不要求提权，也不会自动运行该脚本。新增监听程序应先审查是否必要，再按实际需要单独补充规则，不能扩大成整个临时目录或全部程序放行。

## Logic 单元测试

RPC Logic 是核心业务测试重点。单元测试直接构造只包含当前用例所需依赖的 `ServiceContext`，不得调用会建立真实数据库、Redis 或 RPC 连接的 `NewServiceContext`。

```go
svcCtx := &svc.ServiceContext{
	UserRepo: userRepoStub{/* test data */},
}

logic := NewLoginLogic(context.Background(), svcCtx)
resp, err := logic.Login(req)
```

API Logic 使用 RPC 客户端接口的测试替身，重点验证：

- 请求是否正确传递给 RPC。
- RPC 业务错误码是否保持不变。
- 标准 gRPC 错误是否映射成约定的 HTTP 状态。
- 内部错误信息是否不会泄漏给客户端。

存在多个输入和结果分支时使用表驱动测试和 `t.Run`。每个业务用例至少覆盖成功结果、预期业务失败和关键依赖失败。

## 测试替身

- Logic 单元测试默认使用手写 Stub，预设返回值并记录调用参数和次数；不因接口方法较多或跨测试文件复用就引入 Mock 生成工具。
- 不建立全局的巨型 Mock 包；测试替身优先放在消费它的包中。
- 同一个 Stub 的结构体与全部方法必须集中在一个 `_test.go` 文件，不得按功能拆散；新增方法必须补在该文件中。
- 仅当前测试文件使用时可就地定义；跨测试文件复用时，必须整体移入消费包内独立的 `<dependency>_stub_test.go`（例如 `user_repository_stub_test.go`），不得把原定义留在业务测试文件、只迁移或新增部分方法。
- 不 Mock 整个 `ServiceContext`，只替换其中真正参与当前用例的接口字段。
- 不共享带可变状态的全局 Mock，避免测试顺序互相影响。

测试断言使用 Go 标准测试能力；当断言数量和测试表规模增大时，可统一使用 `require` 表示不可继续的前置条件，使用 `assert` 检查多个结果。

## HTTP 协议测试

Handler 使用 `httptest` 验证：

- 请求参数解析和校验失败。
- HTTP 状态码。
- `code`、`subcode`、`message`、`data` 的完整 JSON 契约。
- 业务错误与技术错误的响应差异。
- 响应中不包含数据库地址、底层错误或其他内部信息。

普通业务和协议测试断言稳定的 HTTP 状态、code 与 subcode，不锁定允许调整或翻译的 message。Validator 单元测试直接断言失败字段和官方校验规则，不锁定完整原始诊断；只有国际化专项测试验证业务与通用错误的具体翻译行为、语言回退和缺失键降级。

直接调用 Handler 不能覆盖路由注册和中间件，因此每个 API 进程还应保留少量完整路由测试，注册真实 Routes 后验证：

- 路径及 HTTP Method。
- 鉴权、权限和其他中间件。
- 404、参数错误和统一响应处理器。

每个 API 进程维护一张表驱动的路由安全矩阵，把公开、仅登录和接口授权三类路由显式列出。新增路由必须在矩阵中登记安全级别，并验证公开路由不会被误拦截、登录路由无 JWT 返回 401、授权路由无策略返回 403。JWT 解析、Session 过期、Redis 异常和 Casbin 失败关闭等中间件内部细节只在中间件测试中完整覆盖一次，不为每个业务接口复制。

修改全局 HTTP 响应处理器的测试不得调用 `t.Parallel`，并必须使用 `t.Cleanup` 恢复全局状态。

## gRPC 协议测试

纯 Logic 测试不经过网络。需要验证以下行为时，使用内存连接完成一次真实 gRPC 调用：

- 服务端拦截器是否生效。
- DogX 业务错误是否转换为扩展 gRPC Code。
- 业务错误的原始可读 Message 和 `google.rpc.ErrorInfo.Reason` 是否无损传输。
- 标准 gRPC Code 是否保持不变。
- API 层是否能够恢复业务 Code 和 subcode，并按请求语言生成 Message。
- 超时、取消等传输语义。

测试必须显式停止 Server、关闭 Listener 和 Client Connection，避免 goroutine 泄漏。

## PostgreSQL 与 Repository 集成测试

GORM Repository 使用真实 PostgreSQL 测试，不使用 SQLite 代替 PostgreSQL，也不把匹配 GORM 生成的 SQL 字符串作为主要测试方式。

真实 PostgreSQL 集成测试用于发现：

- PostgreSQL 方言和类型问题。
- 唯一索引、检查约束、关联字段索引及应用层引用完整性问题。
- 大小写查询、排序和分页差异。
- 时间、事务和软删除行为。
- GORM Model 与迁移结构不一致。

Casbin 角色策略替换必须在真实 PostgreSQL 上验证：业务表和 `casbin_rule` 的全部插入批次共享同一事务；角色策略变化时只产生一条过滤 `DELETE`，按每批最多 5,000 条执行批量 `INSERT`；覆盖 10,000 个目标 API 加必需 API 的完整保存、后续批次失败及全部批次成功后业务事务回滚，确保原策略恢复且失败时不发布通知；目标集合未变化时不写数据库、不发布 Watcher 通知。测试不得重新使用官方 Adapter 的 `RemovePoliciesCtx` 来构造或验证该流程。

集成测试文件使用构建标签：

```go
//go:build integration
```

推荐放置位置：

```text
apps/system/internal/repository/user_repository_integration_test.go
apps/system/internal/migration/migration_integration_test.go
pkg/testutil/postgres/postgres.go
```

测试数据库必须使用独立名称，例如 `dogx_test`。测试辅助代码在清理数据或重建结构前必须校验数据库名，拒绝对不符合测试命名规则的数据库执行破坏性操作。

PostgreSQL 集成测试通过 `DOGX_TEST_POSTGRES_DSN` 获取连接信息，数据库名必须以 `_test` 结尾。每个测试创建独立的 `dogx_it_` Schema，并在测试结束后通过 `t.Cleanup` 删除，禁止连接 `dogx_dev` 等开发或生产数据库。

```shell
DOGX_TEST_POSTGRES_DSN=postgres://dogx:password@127.0.0.1:5432/dogx_test?sslmode=disable
```

Repository 测试优先为每个用例开启事务，并在 `t.Cleanup` 中回滚。不能安全使用同一事务隔离的用例，使用独立 Schema 或独立数据库，且不得并行操作共享数据。

## Goose 迁移测试

迁移测试至少验证：

- 空数据库可以迁移到最新版本。
- 再次执行迁移不会重复建表或报错。
- 必需的表、字段、唯一索引、检查约束和注释存在，且迁移没有创建物理外键。
- 迁移后的结构能够被当前 GORM Model 正常读写。

迁移文件是否成功嵌入只属于基础检查，不能替代在真实 PostgreSQL 上执行迁移。

## Job 与 etcd 集成测试

Job 测试使用同一个 `DOGX_TEST_POSTGRES_DSN` 测试数据库，但每个测试建立独立 Schema、执行本域迁移，不导入 System 的内部数据层。迁移测试另验证同库的 `goose_db_version` 与 `job_goose_db_version` 互不混淆。不得为测试连接共享开发数据库。

etcd 集成测试通过 `DOGX_TEST_ETCD_BINARY` 指定固定版本 3.5.21 的可执行文件，测试自行启动回环随机端口、临时数据目录的独立进程；用有期限的健康检查确认就绪，结束时关闭。测试不能连接生产或共享 etcd，也不依赖本机预启动服务。CI 从官方 release 下载并核对 SHA256，再运行包含 Job 路径的集成测试。

```powershell
$env:DOGX_TEST_POSTGRES_DSN = 'postgres://dogx:password@127.0.0.1:5432/dogx_test?sslmode=disable'
$env:DOGX_TEST_ETCD_BINARY = '<absolute-path-to-etcd>'
go test -tags=integration -shuffle=on -count=1 -timeout=5m ./apps/job/internal/... ./apps/job/rpc/internal/...
```

必须覆盖真实事务竞争、触发身份唯一性、多副本切主、通知缺失后的分钟核对、取消清理和持久化交接。分钟兜底不是测试中的即时通知；需要等待实际周期的用例单独控制总超时，普通单元测试继续使用可控依赖。

## Redis 测试

- 只验证简单键值、过期时间和会话行为时，可以使用进程内 Redis Fake。
- 使用 Lua、Stream、分布式锁或依赖 Redis 精确语义时，必须增加真实 Redis 集成测试。
- 当前使用真实 Redis 验证 RPC `ServiceContext` 的依赖装配、就绪检查、刷新令牌原子轮换和旧令牌复用撤销。
- Redis 集成测试使用唯一前缀隔离数据，并记录测试创建的精确键名进行清理；禁止使用 Redis `KEYS` 命令清理测试数据。

## 端到端测试

端到端测试数量保持少而稳定，只覆盖高价值闭环，例如：

- 登录、登录审计、获取当前用户、修改密码、刷新令牌和退出。
- 用户绑定角色后获得对应应用的页面菜单与页面元素权限。
- 角色菜单权限与接口权限互不自动授予。
- 权限策略变更后旧权限失效。
- 角色新增、修改、停用和删除形成闭环；停用后已有 Access Token 保持有效，登录或刷新得到的新 Token 排除禁用角色，重新启用后新 Token 再次包含该角色；未软删除用户占用时拒绝删除且保持原样，只有软删除用户遗留引用时允许删除并清理关联数据和 Casbin 策略。

端到端测试启动真实 API、RPC、PostgreSQL 和 Redis，通过公开 HTTP 接口观察结果，不直接读取业务内部变量。服务就绪通过健康检查判断，不使用固定 `time.Sleep` 猜测启动时间。

跨进程端到端测试放在 `apps/system/e2e`，使用 `integration && e2e` 构建标签。CI 先从当前源码构建 API 和 RPC 二进制，再通过 `DOGX_E2E_API_BINARY`、`DOGX_E2E_RPC_BINARY` 指定测试进程；测试生成临时配置并为子进程设置隔离 Schema，不复用开发配置文件。当前基线验证登录、当前用户、角色 CRUD、角色启停前后的旧 Token 与刷新 Token 角色快照、角色占用时拒绝删除、仅剩软删除用户引用时完成数据清理，以及角色接口权限变更经 Redis Watcher 实时传播后旧 JWT 被拒绝。

## 测试目录

测试尽量与被测代码放在同一个包，保持业务代码与测试高内聚：

```text
apps/system/api/internal/logic/<resource>/*_test.go
apps/system/api/internal/handler/<resource>/*_test.go
apps/system/rpc/internal/logic/<resource>/*_test.go
apps/system/rpc/internal/svc/*_integration_test.go
apps/system/internal/repository/*_integration_test.go
apps/system/internal/migration/*_integration_test.go
apps/system/cmd/migrate/*_test.go
apps/job/cmd/migrate/*_test.go
pkg/migratecmd/*_test.go
apps/system/e2e/*_test.go
pkg/<component>/*_test.go
```

不建立统一的顶级 `tests` 目录承载所有测试，避免测试远离业务代码，也避免破坏 Go `internal` 包边界。只有真正跨进程的端到端测试可以集中放置。

## CI 分层

每次 Pull Request 和主分支推送执行快速检查：

```shell
go mod verify
go vet ./...
goctl api validate --api apps/system/api/system.api
go test -race -shuffle=on -count=1 ./...
```

集成测试使用独立 CI Job，启动 PostgreSQL 18、Redis 7.4 并准备 etcd 测试二进制后执行（含抽取后的公共组件）：

```shell
go test -tags=integration -shuffle=on -count=1 -timeout=5m ./apps/system/api/internal/... ./apps/system/internal/... ./apps/system/rpc/internal/... ./apps/job/internal/... ./apps/job/rpc/internal/... ./pkg/...
```

同一集成测试 Job 在依赖就绪后另外执行真实进程端到端测试：

```shell
go build -o /tmp/dogx-bin/system-rpc ./apps/system/rpc
go build -o /tmp/dogx-bin/system-api ./apps/system/api
DOGX_E2E_RPC_BINARY=/tmp/dogx-bin/system-rpc \
DOGX_E2E_API_BINARY=/tmp/dogx-bin/system-api \
go test -tags=integration,e2e -run '^TestSystemAuthenticationAndRBACEndToEnd$' -count=1 -timeout=3m ./apps/system/e2e
```

普通 Job 的 HTTP 执行验收复用上述 System API／RPC 生产二进制，并额外准备 Job RPC、迁移命令和 System 执行测试进程：

```shell
go build -o /tmp/dogx-bin/job-rpc ./apps/job/rpc
go build -o /tmp/dogx-bin/job-migrate ./apps/job/cmd/migrate
go test -c -tags=integration,e2e -o /tmp/dogx-bin/system-executor.test ./apps/system/rpc
DOGX_E2E_RPC_BINARY=/tmp/dogx-bin/system-rpc \
DOGX_E2E_API_BINARY=/tmp/dogx-bin/system-api \
DOGX_E2E_JOB_RPC_BINARY=/tmp/dogx-bin/job-rpc \
DOGX_E2E_JOB_MIGRATE_BINARY=/tmp/dogx-bin/job-migrate \
DOGX_E2E_SYSTEM_EXECUTOR_BINARY=/tmp/dogx-bin/system-executor.test \
go test -tags=integration,e2e -run '^TestJobOrdinaryExecutionEndToEnd$' -count=1 -timeout=5m ./apps/system/e2e
```

两项测试都需要显式配置 `DOGX_TEST_POSTGRES_DSN`、`DOGX_TEST_REDIS_HOST` 和 `DOGX_TEST_ETCD_BINARY`，使用隔离 Schema、Redis 前缀及测试持有的 etcd。Windows 使用上述固定路径包装器，并将这些二进制构建到 `bin/windows-tests` 的固定路径。

System 尚未定义生产 Job 处理器，执行测试进程只在 `integration,e2e` 测试文件中注册测试处理器，通过生产 `run` 函数完成相同的依赖初始化和双 Server 装配；生产 `main` 仍不注册虚构业务。测试处理器只写隔离的 System 测试业务表，不访问 Job 表。该用例验证真实 API／Job／System 生产进程与复用生产装配的 System 测试进程之间的执行链路，不把测试处理器当成已交付业务。此 HTTP 用例的收尾仍使用强制退出，不能用它证明优雅停机；以下独立用例才主动发送操作系统停止信号。

复用上述二进制和环境变量，单独运行进程生命周期验收：

```sh
DOGX_E2E_RPC_BINARY=/tmp/dogx-bin/system-rpc \
DOGX_E2E_JOB_RPC_BINARY=/tmp/dogx-bin/job-rpc \
DOGX_E2E_JOB_MIGRATE_BINARY=/tmp/dogx-bin/job-migrate \
DOGX_E2E_SYSTEM_EXECUTOR_BINARY=/tmp/dogx-bin/system-executor.test \
go test -tags=integration,e2e -run '^Test(ProductionRPCProcessSignalShutdown|SystemExecutorProcessSignalDrain|SystemExecutorProcessShutdownDeadline|JobSchedulerProcessCrashRecovery)EndToEnd$' -count=1 -timeout=4m ./apps/system/e2e
```

此命令运行四个场景：真实 Job／System 生产 Main 空载信号退出；System 测试处理器在双入口停止接收后继续完成已接纳业务并回报；不配合取消的测试业务令停机预算耗尽，宿主异常退出且平台不伪造成功；真实 Job 进程被 kill 后，仍存活的执行器完成业务，新 Job 进程通过相同发现 Key 接收原 Task／尝试结果。Job 记录只通过公开 RPC 读取，测试只操纵 System 业务表来控制业务完成时点，不伪造执行结果。进程退出、端口关闭及数据库连接归零均实际检查，不能单独据此推断内部 Close 调用顺序。

Unix 使用 SIGTERM；Windows 每个测试子进程使用隐藏私有控制台，独立发送进程附着并确认该控制台仅含目标及自身后发送 CTRL_BREAK，不影响用户终端。共享的 `pkg/testutil/process` 只负责信号，每个消费包提供固定的测试发送入口；Windows 子发送器还须输出实际接收事件的确认行，缺少入口或跳过测试不能假成功。正常排空场景要求目标实际正常退出；预算耗尽场景要求实际异常退出，不能用测试收尾的强杀冒充。

Job 域另有已 ACK 结果保存与丢失的真实进程验收，只需上述 Job 二进制、隔离 PostgreSQL 与 etcd：

```sh
DOGX_E2E_JOB_RPC_BINARY=/tmp/dogx-bin/job-rpc \
go test -tags=integration,e2e -run '^TestJobResultAcknowledgementProcessRecoveryEndToEnd$' -count=1 -timeout=3m ./apps/job/rpc/internal/execution
```

测试以两个独立 Instance、单个保存者构造明确的内存排队：精确行锁卡住第一条结果的保存，目标结果获得 ACK 后尚未开始写库。正常信号退出须排空目标结果；kill 则使目标内存结果丢失，替换节点通过原有选主与复核收敛，已有结果正常成功、丢失结果按超时结束父子状态。第一条在途 SQL 的提交结果不从断连推定。该 Job 域测试使用预置的旧开始时间、已过截止时间及协议结果样本，未运行业务处理器或真实等待四分钟；不改写时间或直接调用推进组件代替恢复。

复用生命周期命令中的四个二进制环境变量和隔离依赖，分别运行执行器骤停至最终超时、etcd 停止后保留数据重启两个专项：

```sh
export DOGX_E2E_RPC_BINARY=/tmp/dogx-bin/system-rpc
export DOGX_E2E_JOB_RPC_BINARY=/tmp/dogx-bin/job-rpc
export DOGX_E2E_JOB_MIGRATE_BINARY=/tmp/dogx-bin/job-migrate
export DOGX_E2E_SYSTEM_EXECUTOR_BINARY=/tmp/dogx-bin/system-executor.test
go test -tags=integration,e2e -run '^TestExecutorCrashEventuallyTimesOutEndToEnd$' -count=1 -timeout=6m ./apps/system/e2e
go test -tags=integration,e2e -run '^TestJobProcessesRecoverAfterEtcdRestartEndToEnd$' -count=1 -timeout=3m ./apps/system/e2e
```

执行器骤停专项真实等待生产 4 分钟开始年龄门槛，仅把可部署扫描间隔设为 1 秒，不改写执行时间或直接触发检查；最终核对父子超时终态及原尝试／绑定。etcd 专项保持原 Job／System 进程存活，使用原数据和原地址重启 etcd，核对三类服务注册及实际选主使用新租约，再执行一条新 Task。Unix 实际运行与负载仍须分别取证；本场景不提供非对称分区或 etcd 数据丢失的证据。本机专项结果及最终检查状态统一见[本机收尾记录](../design/scheduler/ordinary-readiness-review.md#ordinary-local-checkpoint)，不以专项通过代替普通模块整体验收。

分片跨进程验收复用上述 Job、迁移 CLI、System 测试执行器及隔离 PostgreSQL／Redis／etcd；System 测试执行器必须从当前源码重新构建，以注册额外的分片处理器：

```sh
go test -tags=integration,e2e -run '^TestJobSharded(Execution|SchedulerCrash|ManualStop)EndToEnd$' -count=1 -timeout=4m ./apps/system/e2e
```

该命令包含正常 Pre → 分片 → Post、单片重试、Post 重试、A 已派发并绑定后宕机由 B 接收真实业务结果，以及人工停止五个场景。测试只通过公开 RPC 观察 Job 记录，业务输入、实际调用次数及阶段先后由隔离 System 业务表核实；不修改 Job 状态来代替正常执行。停止场景分别验证数据库终态与健康网络下的业务取消，不把远端取消成功写成无条件保证。

Job 域另验证结果已保存但本地推进提示丢失的进程接续：

```sh
DOGX_E2E_JOB_RPC_BINARY=/tmp/dogx-bin/job-rpc \
go test -tags=integration,e2e -run '^TestJobShardedStageProcessRecoveryEndToEnd$' -count=1 -timeout=3m ./apps/job/rpc/internal/execution
```

该测试通过单个父行锁阻挡 A 的后续阶段，确认有效结果已提交后 kill A，再由 B 当选后的原恢复路径接续分片生成、Post 创建或正常结束。夹具预置超过四分钟的旧 Instance，结果由协议样本提交；它不运行业务处理器，也不证明真实等待四分钟或非对称网络分区。父锁跨 RPC／进程退出只用于故障注入，生产事务不采用此模式。相同逻辑的精确四分钟边界、停止竞争和迟到结果另在真实 PostgreSQL integration 测试中验证，实际结论见[分片阶段记录](../design/scheduler/sharded-readiness-review.md)。

分片查询与负载验收另见[已确认条件和实际记录](../design/scheduler/sharded-load-validation.md)。查询计划用例随 integration 执行；机器相关的检查预算和跨进程负载使用额外的 `load` 标签，不加入日常 CI 的性能门槛。

```sh
go test -tags=integration -run '^TestDispatchCandidatePlansAcrossHistoryAndParentFiltering$' -count=1 -v ./apps/job/internal/repository
go test -tags=integration,load -run '^Test(ShardedGenerationFitsSharedCheckBudget|MixedInstanceCheckQueuePreservesProgressUnderShardLoad)$' -count=1 -v ./apps/job/internal/instancecheck
```

完整分片负载采用两个调度与两个执行器，需重新构建带资源采样的测试宿主。准备上述隔离依赖及环境变量后，在 Windows 仓库根目录串行执行：

```powershell
$env:GOMAXPROCS = '2'
$env:DOGX_E2E_JOB_RPC_BINARY = Join-Path (Get-Location) 'bin/windows-tests/e2e-job-rpc.exe'
$env:DOGX_E2E_JOB_MIGRATE_BINARY = Join-Path (Get-Location) 'bin/windows-tests/e2e-job-migrate.exe'
$env:DOGX_E2E_SYSTEM_EXECUTOR_BINARY = Join-Path (Get-Location) 'bin/windows-tests/e2e-system-executor.test.exe'
go build -p 2 -o $env:DOGX_E2E_JOB_MIGRATE_BINARY ./apps/job/cmd/migrate
go test -p 2 -c '-tags=integration,e2e,load' -o $env:DOGX_E2E_JOB_RPC_BINARY ./apps/job/rpc
go test -p 2 -c '-tags=integration,e2e,load' -o $env:DOGX_E2E_SYSTEM_EXECUTOR_BINARY ./apps/system/rpc
$env:DOGX_LOAD_RESOURCES = '1'
$env:DOGX_LOAD_REPORT = Join-Path (Get-Location) 'bin/test-tools/sharded-load-report.json'
$testRunner = 'pwsh -NoProfile -File "' + (Resolve-Path 'scripts/test-exec-windows.ps1').Path + '"'
go test -p 2 -exec $testRunner '-tags=integration,e2e,load' -run '^TestJobShardedLoadEndToEnd$' -count=1 -timeout=12m -v ./apps/system/e2e
```

每条构建成功后再继续，完整结果须结合负载断言、报告和最终退出码判断。Windows 的两项组件命令也使用固定包装器；恢复其他 E2E 前重新按原命令构建生产 Job 程序及不带 `load` 的 System 测试宿主，并清除 `DOGX_LOAD_RESOURCES`。负载采样只有测试宿主启用，生产入口和配置不变。

### 历史存量与并行清理持续负载 {#history-load-validation}

`TestJobHistoryLoadEndToEnd` 是额外启用的本机验收入口，固定使用两个调度进程、两个执行器及真实依赖。每组从 100,000 个历史 Instance、1,150,000 个 Task 开始，包含 10,000 条约 32 KiB 的业务输入／结果；各组独立建立相同数据，不复用上一组已经清理过的表。已确认规模、延迟门槛和指标口径见[历史持续负载方案](../design/scheduler/verification.md#history-load-proposal)。本节记录复现步骤，命令存在不表示验收已经通过。

2026-11-23 原始机械盘结果为 A／B 通过、C 在首趟清理附近超过 30 秒成功可见门槛失败。后续机械盘诊断再次复现，NVMe 对照则完整完成原规模、原门槛的 30 分钟及两批历史清理；具体设置、诊断采样与环境差异见[历史负载验收记录](../design/scheduler/history-load-validation.md)。C 的未完成窗口与最终审计不能用部分统计代替；诊断增强会保留失败前已观察的成功明细，并输出当次已读取的目标父 ID／状态，不放宽门槛。

按 A → B → C 顺序执行，禁止在同一 checkout 同时运行多个 Go 测试命令：

| `DOGX_HISTORY_LOAD_CASE` | 正式观察窗口与工作 |
| --- | --- |
| `A` | 历史静置 5 分钟，无在线任务；保留 365 天 |
| `B` | 持续执行 30 分钟，每分钟 100 个普通 Instance＋100 个各含 10 片及 Pre／Post 的 Instance；保留 365 天，不发生实际历史删除 |
| `C` | 与 B 相同的在线负载及窗口，保留 7 天，由真实 Leader 清理预计到期的 20,000 个父与 230,000 个子 |

三组清理间隔均配置为 10 分钟，以在本轮观察窗口内包含多趟清理；产品默认间隔仍为 4 小时，逐父事务与查询预算保持不变。B、C 各预期 6,000 个在线 Instance、39,000 个 Task／业务调用，按各自计划时刻测量，不将分散到一分钟内的任务当作同秒波次。正式窗口合计 65 分钟，数据准备、约 5 分钟的启动余量、B／C 各最多 1 分钟排空及最终核对另计。下面的 `-timeout=65m` 是**每组整个测试进程**的保护上限，不是扩大正式测量窗口。

PostgreSQL 必须是本轮独占、可读取本地日志的隔离进程，数据库名称以 `_test` 结尾，Schema 由现有夹具创建；不要指向共享或开发业务库。除前文 `DOGX_TEST_POSTGRES_DSN`、`DOGX_TEST_REDIS_HOST`、`DOGX_TEST_ETCD_BINARY` 外，还必须准备以下 PostgreSQL 配置并在启动该隔离进程前生效：

```conf
logging_collector = on
log_destination = 'csvlog'
log_directory = 'history-csv'
log_min_duration_statement = -1
log_parameter_max_length = 128
log_autovacuum_min_duration = 0
```

`history-csv` 是隔离 PostgreSQL 数据目录下的日志目录；将 `DOGX_HISTORY_PG_LOG_DIR` 设置为它的实际绝对路径。`log_parameter_max_length` 至少须保留完整的时间戳参数，示例 128 字节足够恢复真实清理 cutoff，不能设为 0。测试仅在两个 Job 进程的 `PGOPTIONS` 中启用 `log_min_duration_statement=0`；System 业务执行器不启用 SQL 跟踪。CSV 中的候选查询参数与应用清理完成日志共同确定每趟实际时间、cutoff 及删除数，不依靠估算的第 10／20 分钟代替证据。日志目录与结果目录均保留在忽略的测试资料目录内。

准备阶段先构造近期历史，再统一对齐父子时间；在正式窗口外执行 `VACUUM (ANALYZE)` 和完整初始数据核对，清除时间对齐 UPDATE 产生的死元组。三组使用相同准备及预热步骤，不称冷缓存测试。起点留约 5 分钟用于对齐、核对、进程就绪和计划检查；余量不足或首趟提前删掉目标数据时，记录准备失败，不缩小数据或跳过检查进入正式窗口。正式观察期间不强制 VACUUM。专项诊断为增加准备余量，使用忽略目录中的 Go overlay 将预定起点由 5 分钟改为 8 分钟；两种磁盘对照使用相同 overlay，正式源码仍为 5 分钟，负载规模、正式窗口和通过门槛未变。复现诊断结果时须同时记录这一差异，不将准备阶段失败当成在线业务失败。

测试会自动调用 Job 测试宿主中的 `TestJobHistoryLoadQueryPlans`，在前后两个检查点使用真实仓储生成的只读查询执行 `EXPLAIN (ANALYZE, BUFFERS)`。该 helper 不启动调度、不写业务状态；`DOGX_HISTORY_PLAN_OUTPUT` 与 `DOGX_HISTORY_PLAN_CUTOFF` 由主测试设置，计划文件和报告保存在同一结果目录，不需要单独运行 helper。

在仓库根目录配置好独立依赖及 `DOGX_HISTORY_PG_LOG_DIR` 后执行；每条构建成功才进入下一步，固定程序路径仍受 Windows 测试包装器管理：

```powershell
$env:GOMAXPROCS = '2'
$env:DOGX_E2E_JOB_RPC_BINARY = Join-Path (Get-Location) 'bin/windows-tests/e2e-job-rpc.exe'
$env:DOGX_E2E_JOB_MIGRATE_BINARY = Join-Path (Get-Location) 'bin/windows-tests/e2e-job-migrate.exe'
$env:DOGX_E2E_SYSTEM_EXECUTOR_BINARY = Join-Path (Get-Location) 'bin/windows-tests/e2e-system-executor.test.exe'
go build -p 2 -o $env:DOGX_E2E_JOB_MIGRATE_BINARY ./apps/job/cmd/migrate
if ($LASTEXITCODE -ne 0) { throw 'Job migration build failed' }
go test -p 2 -c '-tags=integration,e2e,load' -o $env:DOGX_E2E_JOB_RPC_BINARY ./apps/job/rpc
if ($LASTEXITCODE -ne 0) { throw 'Job load host build failed' }
go test -p 2 -c '-tags=integration,e2e,load' -o $env:DOGX_E2E_SYSTEM_EXECUTOR_BINARY ./apps/system/rpc
if ($LASTEXITCODE -ne 0) { throw 'System load host build failed' }
if ([string]::IsNullOrWhiteSpace($env:DOGX_HISTORY_PG_LOG_DIR) -or
    -not (Test-Path -LiteralPath $env:DOGX_HISTORY_PG_LOG_DIR -PathType Container)) {
    throw 'Set DOGX_HISTORY_PG_LOG_DIR to the isolated PostgreSQL CSV directory'
}
$historyEvidence = Join-Path (Get-Location) ('bin/test-tools/history-load-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $historyEvidence | Out-Null
$env:DOGX_LOAD_RESOURCES = '1'
$testRunner = 'pwsh -NoProfile -File "' + (Resolve-Path 'scripts/test-exec-windows.ps1').Path + '"'
foreach ($historyCase in @('A', 'B', 'C')) {
    $env:DOGX_HISTORY_LOAD_CASE = $historyCase
    $env:DOGX_LOAD_REPORT = Join-Path $historyEvidence ($historyCase + '-report.json')
    go test -p 2 -exec $testRunner '-tags=integration,e2e,load' -run '^TestJobHistoryLoadEndToEnd$' -count=1 -timeout=65m -v ./apps/system/e2e
    if ($LASTEXITCODE -ne 0) { throw ('History load case ' + $historyCase + ' failed; retain its report and logs') }
}
```

报告和退出码共同判断执行结果；保留准备失败、正式失败及复跑记录。每组结束核对预期身份集合与无孤儿，再将实际清理期间的延迟单列与 B 对照。Go 堆不称进程工作集，行时间差不称 COMMIT 延迟；自然 autovacuum 没有出现时明确缺少该成本样本。此测试不补 `demo.echo` 单元测试，也不采集子进程覆盖率或加入日常 CI 的性能门槛。运行结束后清除本节设置的 `DOGX_HISTORY_LOAD_CASE`、`DOGX_HISTORY_PG_LOG_DIR`、`DOGX_LOAD_REPORT`、`DOGX_LOAD_RESOURCES`；恢复其他 E2E 前按其命令重新构建对应宿主。

上述 Go 入口自动记录 Go 堆／goroutine 和 PostgreSQL 统计；本轮 Windows CPU／工作集由证据目录中的 `sample-processes.ps1` 每秒采样，日志与匹配窗口分析由 `analyze-results.py` 完成，不能把只执行 Go 命令视为已采到这些额外指标。识别独立 TOAST VACUUM 时还需保留该隔离 Schema 的父表与 TOAST 表名映射；本轮每个 B／C 各用一次只读系统目录查询保存 `relation-identities.json`，不根据其他 Schema 的日志猜测归属。验收结果和这些本机辅助证据保留在忽略目录，不加入常规 CI。

端到端命令没有为子进程采集覆盖率；不能把这些通过结论计入 `coverage/integration.txt` 或推算 patch 覆盖率。

Linux 环境执行 Race、覆盖率和集成测试；Windows 环境执行普通单元测试，用于发现开发环境和生产环境之间的跨平台问题。

### go-zero 退出竞态的临时测试范围 {#gozero-shutdown-race-workaround}

已确认 go-zero v1.10.3 在显式设置 `Shutdown`、关闭 info 日志后，真实 SIGTERM 退出存在配置写入与信号协程读取的竞态，见 [go-zero #5876](https://github.com/zeromicro/go-zero/issues/5876)。2026-11-06 用户同意在上游修复前暂缓这组日志配置下的竞态覆盖：`pkg/rpchost` 测试配置暂用 `info`，保留原有 11 个生命周期场景、真实 SIGTERM／CTRL_BREAK、Context 取消、排空与资源释放断言及 `-race`。不跳过用例或抑制 race 输出，也不修改生产日志配置、停机期限或 go-zero 依赖。

这是明确缩小配置覆盖范围的临时取舍。info 日志引入的同步可避开已知上游问题，测试通过不证明关闭 info 日志时仍然并发安全，也不代表上游缺陷已修复。实施与验证记录见[退出竞态临时处理](../design/scheduler/ordinary-readiness-review.md#ordinary-shutdown-race-workaround)。

采用包含上游修复的官方版本后，应恢复原 `error` 测试配置，复验显式 `Shutdown` 下真实信号退出的 race 用例，并重新确认 info 对照配置；两种配置均通过后再撤销本节临时例外和对应代码注释。

## 覆盖率策略

- Linux 单元测试和依赖集成测试分别生成覆盖率文件，并使用 `unit`、`integration` 标记上传到 Codecov，由 Codecov 在同一提交下合并展示。
- Codecov 通过 GitHub OIDC 验证上传身份，仓库不保存 `CODECOV_TOKEN`。
- `codecov.yml` 排除 `*.pb.go`、生成的 Routes、Types、Server 转发代码和 RPC Client 等自动生成代码。
- API/RPC/命令行的 `main` 只保留进程启动、信号处理和依赖装配，不纳入业务覆盖率；可测试的命令逻辑下沉到域内 `internal` 或已被多域复用的 `pkg/migratecmd`，正常统计覆盖率。
- `pkg/testutil` 与域内 `internal/testutil` 仅用于构造隔离测试环境，不属于产品代码，也不纳入产品覆盖率。公共 PostgreSQL 连接、隔离 Schema 与清理由 `pkg/testutil/postgres` 提供，本域迁移包装和业务 fixture 留在域内。
- 新功能必须在同一提交中包含对应测试：RPC/API Logic 至少覆盖成功、参数校验和关键依赖错误；涉及数据库约束、事务或查询的 Repository 必须增加真实 PostgreSQL 集成测试。不能先合并功能、再把测试作为后续补丁。
- 新增业务代码不能以 0% 覆盖率进入主分支；生成的转发代码、启动入口和协议代码按 `codecov.yml` 的排除规则处理。测试重点是业务分支，不为自动生成代码机械补测试。
- Codecov 的 patch 覆盖率门槛设为 80% 且作为非 informational 状态；新增功能的改动行未达到门槛时，提交必须先补测试。项目总覆盖率暂不设固定百分比，避免新增高价值功能因代码量变化被全局数字误判。
- 每次提交前必须运行受影响包的测试；后端功能提交前还要运行完整单元测试，集成测试交由 CI 的 PostgreSQL/Redis 环境执行，并在提交说明中报告 unit、integration 和合并覆盖率。
- 登录、权限、金额、状态流转和数据隔离等高风险逻辑必须覆盖明确的成功与失败场景。
- 项目总覆盖率出现下降时，必须解释下降来自新增代码、测试分层变化还是遗漏测试；如果是新增业务分支未覆盖，应在同一变更中修复，不能以“当前总数仍然通过”为理由忽略。

## 测试编写规则

- 测试必须有可验证的断言，不能只调用函数或打印日志。
- 测试数据、时间、ID 和随机输入应保持确定性。
- 优先使用 `t.Cleanup`、`t.TempDir` 和 `t.Setenv` 管理测试资源。
- 只有确认不存在全局状态和共享资源时才能使用 `t.Parallel`。
- 不使用固定休眠等待异步结果，应使用有超时的轮询、Channel 或就绪信号。
- 不依赖测试执行顺序；`-shuffle=on` 下仍应稳定通过。
- 一个单元测试只验证一个清晰行为；端到端测试可以验证完整业务闭环。
- 测试业务结果和公开契约，不锁定无业务意义的内部实现细节。

## 落地顺序

1. 固定本测试规范，并建立单元测试、Race 和覆盖率 CI。
2. 建立 PostgreSQL 18、Redis 7.4、Goose、Repository 和依赖装配集成测试基础设施。
3. 以登录用例建立第一套完整的 RPC Logic、API Logic、Handler 和数据库测试样板。
4. RBAC 开发时增加权限矩阵、策略同步和关键端到端测试。
5. Redis 缓存业务、消息队列和 Job 等能力出现后，再分别增加对应的行为测试。
