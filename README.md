# DogX

[![codecov](https://codecov.io/gh/tokyolab/dogx/branch/main/graph/badge.svg)](https://codecov.io/gh/tokyolab/dogx)

DogX 是一套面向开源项目的通用 Go 后台管理骨架。后端采用 go-zero 的 API/BFF → RPC 分层方式；PC 管理端和移动端分别维护在独立仓库中。

## 技术栈

- Go 1.27
- go-zero 1.10.3
- goctl 1.10.2
- Protobuf / gRPC
- GORM
- Goose（SQL 数据库迁移）
- PostgreSQL 18（唯一支持的关系型数据库，迁移脚本尽量兼容 PostgreSQL 17）
- Redis 7.4（按需使用，不为普通 CRUD 自动加缓存）
- etcd 3.5 + robfig/cron/v3（Job 域选主、发现、配置通知与长期规则触发）
- Casbin v3 + 官方 GORM Adapter + Redis Watcher（接口权限判定与多实例策略失效通知）

## 仓库结构

```text
apps/
  system/
    api/
      system.api      HTTP API 唯一生成入口
      desc/           按业务资源拆分的 HTTP 契约
      internal/       API/BFF 进程私有实现
    rpc/              原业务 RPC 与内嵌执行器双入口、内部契约和进程私有实现
    cmd/migrate/      独立数据库迁移命令
    internal/
      database/       System 域共享的 PostgreSQL 配置与连接工厂
      authorization/  Casbin 模型、Adapter、角色策略替换事务与重载机制
      model/          GORM 持久化模型及字段类型和常量
      repository/     数据访问接口与实现
      migration/      Goose 迁移及 SQL 文件
  job/
    rpc/              独立 Job 管理与调度进程，含 etcd 选主和 Cron 运行组件
    cmd/migrate/      Job 独立迁移命令及迁移账本
    internal/         Job 域模型、数据访问、事务服务与迁移
pkg/                跨进程、跨业务域复用的稳定技术组件
docs/               架构决策和开发文档
```

当前调用链如下：

```text
browser → system-api (HTTP JSON) → system-rpc (gRPC) → PostgreSQL / Redis
                   └─ Job RPC (gRPC) → PostgreSQL；etcd 选主／通知／发现
                   └─ 只读认证 Session → Redis
                   └─ 只读 Casbin 策略 → PostgreSQL
```

`system-api` 和 `system-rpc` 是两个独立进程。API 负责 HTTP 契约、参数处理、响应转换、只读校验认证 Session，以及从 PostgreSQL 只读加载本地 Casbin 策略；RPC 负责业务逻辑、Session 写操作和权限策略写操作。API 不查询普通业务表。两层都使用 goctl 生成的官方目录结构。

## 本地启动

仓库只提交示例配置。先复制 API 和 RPC 配置：

```powershell
Copy-Item 'apps/system/api/etc/system-api.example.yaml' 'apps/system/api/etc/system-api.yaml'
Copy-Item 'apps/system/rpc/etc/system-rpc.example.yaml' 'apps/system/rpc/etc/system-rpc.yaml'
```

真实配置已被 Git 忽略。密码可以直接保存在本地真实配置中，也可以使用环境变量注入。System RPC 现在同时装配内嵌执行器，启动前还需准备可用的 etcd、执行器组及其 Token，按下方 [System 内嵌执行器接入](#system-内嵌执行器接入)配置；不提供默认组或跳过认证的启动模式。

首次启动前先执行数据库迁移：

```powershell
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' up
```

然后启动 RPC：

```powershell
$env:DOGX_POSTGRES_PASSWORD = '<postgres-password>'
$env:DOGX_REDIS_PASSWORD = '<redis-password>'
$env:DOGX_ACCESS_SECRET = '<at-least-32-byte-random-secret>'
$env:DOGX_JOB_EXECUTOR_TOKEN = '<token-from-executor-group-detail>'
go run ./apps/system/rpc -f 'apps/system/rpc/etc/system-rpc.yaml'
```

再打开另一个终端启动 API：

```powershell
$env:DOGX_ACCESS_SECRET = '<same-secret-as-system-rpc>'
$env:DOGX_REDIS_PASSWORD = '<redis-password>'
$env:DOGX_POSTGRES_PASSWORD = '<postgres-password>'
go run ./apps/system/api -f 'apps/system/api/etc/system-api.yaml'
```

服务启动后：

- `GET /health`：仅检查 `system-api` 进程是否存活，不访问 RPC 或外部依赖。
- `GET /ready`：`system-api` 检查 Casbin 初始快照、权限 PostgreSQL 和认证 Redis，再调用 `system-rpc` 检查 PostgreSQL 和 Redis；任一依赖异常时返回 HTTP 503。
- `POST /auth/login`：使用账号密码登录，成功后返回访问令牌、刷新令牌和访问令牌有效秒数。
- `POST /auth/refresh`：轮换刷新令牌并签发新的访问令牌。
- `POST /auth/me`：返回当前用户信息；需要 `Authorization: Bearer <access-token>`。
- `POST /auth/logout`：撤销当前设备 Session；需要访问令牌。
- `POST /auth/logout-all`：撤销当前用户的全部 Session；需要访问令牌。
- `POST /auth/change-password`：验证当前密码并修改密码，成功后撤销该用户的全部 Session；需要访问令牌。

下述“Casbin 接口权限”要求适用于普通用户；有效超级管理员在 JWT 与 Redis Session 校验后直接放行，不依赖逐条 Casbin Policy。

- `POST /role/create`：新增角色并返回角色 ID；需要有效 Session 和 Casbin 接口权限。
- `POST /role/list`：分页查询角色；需要有效 Session 和 Casbin 接口权限。
- `POST /role/get`：查询角色详情；需要有效 Session 和 Casbin 接口权限。
- `POST /role/update`：修改角色编码、名称、描述和排序；系统内置角色的编码不可修改；需要有效 Session 和 Casbin 接口权限。
- `POST /role/status/update`：启用或停用角色；系统内置角色不可停用。角色状态在用户下一次登录或刷新令牌时进入新的 JWT 角色快照，已有 Access Token 在自身和 Session 均有效期间不受角色启停影响；需要有效 Session 和 Casbin 接口权限。
- `POST /role/delete`：存在未软删除用户引用时拒绝删除且保持原样；否则软删除角色并清理历史用户关联、菜单关联和 Casbin 策略；系统内置角色不可删除；需要有效 Session 和 Casbin 接口权限。
- `POST /api/list`：按服务、分组和关键字查询接口授权资源；需要有效 Session 和 Casbin 接口权限。
- `POST /role/api/get`：查询角色当前获授的 API ID；需要有效 Session 和 Casbin 接口权限。
- `POST /role/api/update`：提交角色完整 API ID 集合；需要有效 Session 和 Casbin 接口权限。

API 与 RPC 必须使用完全相同的 `DOGX_ACCESS_SECRET`。受保护请求先由 API 本地验证携带 `userId`、`sessionId`、`roleIds` 和 `isSuperAdmin` 的 JWT，再通过精确 Redis `GET` 检查 Session；有效超级管理员直接放行，普通用户最后由本地 Casbin 快照判定接口权限。普通业务请求随后仍只调用一次业务 RPC。Session 的创建、轮换和撤销仍只由 RPC 负责。

## Job 调度域（执行模块开发中）

管理入口为 `system-api → job-rpc → PostgreSQL`。`job-rpc` 是独立进程，副本通过 etcd 选主；只有 Leader 维护 Cron 规则和触发恢复，各副本均可处理管理请求及结果回报。Job 已接通独立的管理／回报两个 Server，回报使用真实组认证、结果队列与单条保存。System 已内嵌公共执行器，以独立 Server 接收派发和取消；当前没有注册生产业务处理器，未知名称明确拒绝。**实际 Task 派发编排、父 Instance 完成和执行查询仍在开发，不代表普通任务已能完整执行。** 组管理接口已实现，尚无组管理页面或隐含默认组。

```powershell
Copy-Item 'apps/job/rpc/etc/job-rpc.example.yaml' 'apps/job/rpc/etc/job-rpc.yaml'
go run ./apps/job/cmd/migrate -f 'apps/job/rpc/etc/job-rpc.yaml' up
go run ./apps/job/rpc -f 'apps/job/rpc/etc/job-rpc.yaml'
```

先配置可用的 PostgreSQL 与 etcd，所有调度副本使用相同的进程时区。`Scheduler.Prefix` 必须隔离环境和逻辑集群；Job RPC 的 `Etcd.Key` 与 API 的 `JobRpc.Etcd.Key` 必须相同。示例的 `Postgres.TimeZone` 是数据库会话时区，不替代操作系统／容器进程时区。数据库默认同库同 `public` schema；Job 有独立配置、连接池、`job_*` 表及 `job_goose_db_version` 迁移账本，初次生产部署可配置到独立数据库。

Job 示例新增以下配置。管理入口仍使用顶层 `ListenOn`（示例 9002）和 `Etcd`；回报监听与发现 Key 分开，执行器的 ReportClient 应指向 ExecutionRPC，不能指向管理 Key。

```yaml
ExecutionRPC:
  Name: job-rpc
  ListenOn: 0.0.0.0:9003
  Etcd:
    Hosts:
      - 127.0.0.1:2379
    Key: /dogx/development/default/rpc/job-execution
  Auth: false
  Health: true

ResultPersistence:
  Capacity: 512
  Concurrency: 8
  SaveTimeout: 2s
```

`ExecutionRPC.Name` 是原生解码的必填项；实际日志、监控、模式与退出等 ServiceConf 配置统一取自顶层，不能配置成另一套进程设置。ResultPersistence 三项均须为正值；保留数含等待、退避与在途，单条预算含读取和提交，不是结果 TTL。新建本地配置时复制示例；已有私有配置须自行按示例补齐，本次不自动改写。缺失或非法配置明确启动失败，不回退为旧的单入口模式。

初始化成功、组快照与结果消费者准备好后才启动两个 Server，不等待 Leader；Health 仅表示标准服务健康，不代表数据库结果已保存。停机先拒绝新结果，已接纳结果在整个进程共享的 Shutdown.WaitTime（默认 30 秒）内善后，实际工作与入站请求结束后才释放依赖。初始化 I/O 尚未全部接入宿主 Context，不能保证收到停止信号就立即中断连接或初始加载。当前实现和本轮验证见[Job 生产接线记录](docs/design/scheduler/ordinary-readiness-review.md#ordinary-result-wiring-progress)。

每个 Job 节点还独立运行无绑定观察扫描，不依赖 Leader。`UnboundRecovery` 的部署参数见 `job-rpc.example.yaml`：实际运行结束后默认等待 10 秒并独立浮动 20%，查询页 128 条，每次运行预算 5 秒、每次数据库操作 1 秒，观察期 30 秒；省略时使用上述默认值，显式非正值拒绝启动。观察期须在副本间保持一致，并与派发及响应保存预算一起检查。观察到期只允许条件结算，不证明原业务已停止；已绑定尝试、整轮超时和人工停止不由此扫描处理，范围见[观察扫描实施记录](docs/design/scheduler/ordinary-readiness-review.md#ordinary-unbound-scan-progress)。

人工停止入口为 `POST /job/instance/stop`，沿用 System API 登录与权限校验。接口先保存停止决定，再独立交接数据库取消和远端通知；响应不代表执行器业务已退出。`Stopping` 控制分批收敛及 STOPPING 补扫的独立预算，`CancelNotification` 控制按 Instance／去重地址的尽力通知，两者都使用宿主共享退出预算。重新发现已存在的 STOPPING 只收敛数据库，不周期重发远端通知；RUNNING 复核新提交超时决定时，仍交接一次尽力通知。

Leader 的 RUNNING／STOPPING 发现共用 `InstanceRecovery.Interval`，默认 2 分钟，新 Leader 就绪后各执行一次有界扫描；各分支独立分页、不重叠、不积累补跑。RUNNING 每趟固定数据库时间减 4 分钟的开始门槛，默认每页 256、每次扫描交接 5 秒、单次查询 1 秒，并在本地检查集合半容量水位内补充。普通 Task 完整最终结果优先正常结束，明确未完成且到期才锁父重新取数据库时间决定超时；分片模式暂跳过。当前普通阶段 1～5 核心已接通，阶段 6 进行中；最新执行器骤停、etcd 恢复及本机收尾结果见[本机收尾记录](docs/design/scheduler/ordinary-readiness-review.md#ordinary-local-checkpoint)。GitHub Linux 实际运行、race、覆盖率和代表性负载尚待验证。

System 域另有增量迁移登记八条 Job 接口权限，按原 System 迁移命令执行，不由 Job 迁移跨域写入。`/job/list`、`/job/get`、`/job/create`、`/job/update`、`/job/status/update`、`/job/delete`、`/job/cron/preview`、`/job/trigger` 均为受 Session 和 Casbin 保护的 POST 路由。

执行基础迁移已增加 `job_task`、Instance 停止中状态相关索引，以及组的完整发现 Key／Token 和全表唯一约束。Job 启动时必须能够读取完整的未删除组配置；各节点随后每 60 秒刷新一次凭据缓存，查询预算复用 `Scheduler.QueryTimeout`。迁移不回填开发中间态的旧组，不在启动时迁移或修复数据；执行器组创建／详情／列表／删除入口已实现，完整 Task 执行链路仍在开发，详见[当前实施进度](docs/design/scheduler/ordinary-readiness-review.md#ordinary-stage2-progress)。

API 的 Job 客户端按首次请求初始化，不在 `system-api` 初始化或 `/ready` 中增加主动 Job 探测。初始化请求仍有超时，并只允许一个在途初始化。go-zero 首次 etcd 发现使用其共享客户端持续重试，无法由单次请求取消；关闭后晚到的连接会被释放，持续断连时最多保留该进程级初始化操作直到进程退出，不随请求数增加。System RPC 的内嵌执行器另有 etcd 注册和首次发现依赖，不能将 API 的懒初始化边界解释成 System RPC 可在 etcd 离线时完成冷启动。

### System 内嵌执行器接入

先启动已完成迁移的 Job RPC，通过组管理入口创建执行器组，并从详情取得生成的 Token。组名和发现 Key 由部署方填写且创建后不可修改，示例使用 `system.executor` 与 `/dogx/development/default/executor/system.executor`；不要把组名当作仅供显示的名称。首次空库部署尚未启动 System API／RPC 时，通过 Job 管理 RPC 的 `CreateExecutorGroup`／`GetExecutorGroup` 完成创建和详情读取，沿用部署方的原生 RPC 鉴权配置；已有后台服务时可使用组管理 HTTP 接口，当前尚无组管理页面。不直接写组表，不增加临时 Token 或额外 CLI。System 初始化只校验配置中的凭据格式，不远程查询组记录，也不创建默认组或跳过认证。

随后复制并补齐 [System RPC 示例配置](apps/system/rpc/etc/system-rpc.example.yaml)，将组详情 Token 注入 `DOGX_JOB_EXECUTOR_TOKEN`。部署值的对应关系如下：

| 配置 | 示例及来源 |
| --- | --- |
| `JobExecutor.GroupName` | 创建组时填写的 `system.executor`，必须精确相同 |
| `JobExecutor.Token` | 组详情返回的 32 字符 Token；示例为 `"${DOGX_JOB_EXECUTOR_TOKEN}"`，保留 YAML 引号，合法 Token 也可能全部由数字组成 |
| `JobExecutor.Server.ListenOn` | `0.0.0.0:9004`，与原业务入口 9001 分开 |
| `JobExecutor.Server.Etcd.Key` | 与组保存的完整发现 Key 精确相同；不是程序根据组名自动生成 |
| `JobExecutor.ReportClient.Etcd.Key` | `/dogx/development/default/rpc/job-execution`，与 Job `ExecutionRPC.Etcd.Key` 相同，不能使用管理 Key |
| `JobExecutor.Runtime` | 业务并发／等待／结果保留／回报并发默认 `32 / 128 / 256 / 16`；配置中的显式值均须为正数 |

执行器 Server 与 ReportClient 使用原生 zrpc＋etcd 配置，禁止将组 Token 填入原生 `App/Token`。执行器 Server 固定 `Auth: false`、保留 Health；原业务 RPC 的鉴权配置保持独立。`Server.Name` 是原生解码必填项，其实际 ServiceConf 仍由宿主顶层统一。回报连接采用非阻塞拨号，调度服务暂不可达不等于原业务接口必须启动失败；但首次发现和注册仍需可用 etcd，非阻塞拨号不消除这一依赖。

业务接入通过 `NewServiceContext(c, register RegisterProcessors)`，在 System 依赖初始化后、接单开始前注册处理器并注入依赖。当前 Main 使用空注册回调；`system.cleanup` 只是设计示例，未实现任何清理任务。具体业务确定后，处理器放入 `apps/system/internal/job` 并显式注册；当前不创建空包，也不增加独立 Worker 入口。启动和停机实现、当前验证状态见 [System 内嵌接线记录](docs/design/scheduler/ordinary-readiness-review.md#ordinary-system-executor-wiring-progress)。

完整范围和验证要求见[触发模块总审](docs/design/scheduler/trigger-readiness-review.md)。数据库迁移须明确执行，不会在服务启动时自动运行；本段命令不代表已有测试完成或已迁移开发数据库。

## 初始化管理员

数据库迁移完成后，可以创建第一个管理员账号。密码至少 12 字节，不通过命令行参数传递；未设置环境变量时，交互式终端会安全提示输入密码：

```powershell
$env:DOGX_ADMIN_PASSWORD = '<administrator-password>'
go run ./apps/system/cmd/bootstrapadmin `
  -f 'apps/system/rpc/etc/system-rpc.yaml' `
  -username 'admin' `
  -nickname 'Administrator'
Remove-Item Env:DOGX_ADMIN_PASSWORD
```

密码使用 Argon2id 哈希后写入 `sys_user.password_hash`，并在同一 PostgreSQL 事务中把账号绑定到迁移创建的 `super_admin` 角色。该角色由 `sys_role.is_system` 标记为系统内置角色，不能停用、删除、修改编码或配置接口与菜单权限；其用户通过服务端签名的 `isSuperAdmin` 旁路 Casbin，并由后端取得全部有效菜单和页面元素，不依赖逐条接口策略或 `sys_role_menu`。同名活动用户已存在时命令会失败，不会覆盖现有账号，也不会留下不完整的角色绑定。

## 数据库迁移

迁移文件以 SQL 形式嵌入迁移命令，位于 `apps/system/internal/migration/migrations`。GORM 负责运行时数据访问，不在服务启动时自动修改数据库结构。

```powershell
# 创建开发迁移，默认使用时间戳版本
go run ./apps/system/cmd/migrate create add_department

# 准备正式发布时转换成连续版本
go run ./apps/system/cmd/migrate fix

# 查看迁移状态
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' status

# 执行所有待处理迁移
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' up

# 查看当前数据库版本
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' version

# 回滚最近一次迁移，会删除或改变数据库结构
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' down
```

开发迁移使用时间戳，准备正式发布时通过 `fix` 转换成连续版本。已经在共享环境执行过的迁移文件不得修改；数据库结构变化必须增加一个新迁移。完整规则见 [数据库迁移规范](docs/development/database-migrations.md)。

## 契约生成

修改 API 契约后，在 API 目录通过唯一入口重新生成；不要单独对 `desc/*.api` 执行 goctl：

```powershell
Set-Location 'apps/system/api'
goctl api go --api system.api --dir . --style gozero
```

修改应用 Protobuf 契约后，在 RPC 目录重新生成：

```powershell
Set-Location 'apps/system/rpc'
goctl rpc protoc system.proto --go_out=./types --go-grpc_out=./types --zrpc_out=. --style gozero
```

Job RPC 使用同一版本和风格，在其独立目录生成：

```powershell
Set-Location 'apps/job/rpc'
goctl rpc protoc job.proto --go_out=./types --go-grpc_out=./types --zrpc_out=. --style gozero
```

上述应用生成命令会更新契约派生文件，但不会覆盖已有的 Logic。修改已有接口的输入或输出类型后，仍需手动同步 Logic 方法签名并运行测试。

公共执行协议位于 `pkg/jobexecutor/protocol/execution.proto`，协议源文件及生成绑定已实现；运行装配进度见[阶段 1 记录](docs/design/scheduler/ordinary-readiness-review.md#ordinary-stage1-progress)。修改该文件时，从**仓库根目录**执行以下固定命令，使用官方 `protoc`、`protoc-gen-go` 和 `protoc-gen-go-grpc`，只生成协议类型与 gRPC 客户端／服务端接口：

```powershell
protoc --proto_path=. --go_out=. --go_opt=module=github.com/tokyolab/dogx --go-grpc_out=. --go-grpc_opt=module=github.com/tokyolab/dogx pkg/jobexecutor/protocol/execution.proto
```

该协议的 `go_package` 必须为 `github.com/tokyolab/dogx/pkg/jobexecutor/protocol`，输出为同目录的 `execution.pb.go`、`execution_grpc.pb.go`；不生成应用 Main、配置、Svc、Logic 或 Server 骨架，不改变应用 goctl 命令和 zrpc＋etcd 通信方式。设计阶段不执行该命令。包含 `DO NOT EDIT` 标记的文件禁止直接修改，完整规则见[契约代码生成规范](docs/development/code-generation.md#execution-protocol-generation)。

## 测试

```powershell
go test ./...
```

测试默认不连接真实 PostgreSQL 或 Redis；外部依赖通过窄接口替换为测试桩。

Windows 本地使用[固定测试程序路径](docs/development/testing.md#windows-local-tests)运行，避免临时程序路径变化导致防火墙反复询问；防火墙规则安装是独立、需要用户授权的管理员操作，测试不会自动修改规则。

更多约定见 [测试方案](docs/development/testing.md)、[PostgreSQL 索引设计与审计规范](docs/development/database-indexes.md)、[架构决策](docs/adr/0001-dogx-v0.1-architecture.md)、[统一响应与 RPC 错误决策](docs/adr/0002-http-response-and-rpc-errors.md)、[认证令牌与会话决策](docs/adr/0003-authentication-and-session.md)、[单租户 RBAC 与接口权限决策](docs/adr/0004-single-tenant-rbac.md)、[Casbin 运行时与多实例同步决策](docs/adr/0005-casbin-runtime-and-policy-sync.md)、[刷新幂等窗口与浏览器同步决策](docs/adr/0006-refresh-rotation-and-browser-sync.md) 和 [v0.1 功能边界](docs/roadmap/v0.1.md)。
