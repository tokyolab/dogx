# DogX 测试方案

> 状态：方案草案。测试基础设施按业务开发进度分阶段落地，不为尚未出现的依赖提前建设测试框架。

## 目标

DogX 的测试用于验证业务行为、对外契约和基础设施兼容性，并保证测试结果可重复、可并行演进。测试不以追求单一覆盖率数字为目标，也不测试自动生成代码的内部实现。

## 测试分层

DogX 采用四层测试：

1. Logic 单元测试：验证业务规则、成功分支和失败分支，是数量最多、执行最频繁的测试。
2. HTTP / gRPC 协议测试：验证参数解析、响应结构、错误转换、中间件和传输行为。
3. PostgreSQL / Redis 集成测试：验证真实数据库方言、迁移、约束、查询和缓存行为。
4. 端到端测试：只覆盖登录、刷新令牌、权限判定等少量关键业务闭环。

普通测试不得访问公网、共享开发数据库或其他不受测试进程控制的服务。

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

- 只有一两个方法、只在当前测试文件使用的接口，采用手写 Stub 或 Fake。
- 方法较多或被多个测试重复使用的接口，再使用类型安全的 Mock 生成工具。
- 不建立全局的巨型 Mock 包；测试替身优先放在消费它的包中。
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

Casbin 角色策略替换必须在真实 PostgreSQL 上验证：业务表和 `casbin_rule` 共享同一事务；角色策略变化时只产生一条过滤 `DELETE` 和一次批量 `INSERT`；任一策略或业务写入失败时全部回滚；目标集合未变化时不写数据库、不发布 Watcher 通知。测试不得重新使用官方 Adapter 的 `RemovePoliciesCtx` 来构造或验证该流程。

集成测试文件使用构建标签：

```go
//go:build integration
```

推荐放置位置：

```text
apps/system/internal/repository/user_repository_integration_test.go
apps/system/internal/migration/migration_integration_test.go
apps/system/internal/testutil/postgres.go
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
apps/system/internal/migratecmd/*_test.go
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

集成测试使用独立 CI Job，启动 PostgreSQL 18 和 Redis 7.4 后执行：

```shell
go test -tags=integration -shuffle=on -count=1 -timeout=2m ./apps/system/internal/... ./apps/system/rpc/internal/...
```

同一集成测试 Job 在依赖就绪后另外执行真实进程端到端测试：

```shell
go build -o /tmp/dogx-bin/system-rpc ./apps/system/rpc
go build -o /tmp/dogx-bin/system-api ./apps/system/api
DOGX_E2E_RPC_BINARY=/tmp/dogx-bin/system-rpc \
DOGX_E2E_API_BINARY=/tmp/dogx-bin/system-api \
go test -tags=integration,e2e -shuffle=on -count=1 -timeout=3m ./apps/system/e2e
```

Linux 环境执行 Race、覆盖率和集成测试；Windows 环境执行普通单元测试，用于发现开发环境和生产环境之间的跨平台问题。

## 覆盖率策略

- Linux 单元测试和依赖集成测试分别生成覆盖率文件，并使用 `unit`、`integration` 标记上传到 Codecov，由 Codecov 在同一提交下合并展示。
- Codecov 通过 GitHub OIDC 验证上传身份，仓库不保存 `CODECOV_TOKEN`。
- `codecov.yml` 排除 `*.pb.go`、生成的 Routes、Types、Server 转发代码和 RPC Client 等自动生成代码。
- API/RPC/命令行的 `main` 只保留进程启动、信号处理和依赖装配，不纳入业务覆盖率；可测试的命令逻辑必须下沉到 `internal` 包并正常统计。
- `internal/testutil` 仅用于构造隔离测试环境，不属于产品代码，也不纳入产品覆盖率。
- 初期只生成覆盖率报告，不立即用全局阈值阻断提交。
- 新增核心 Logic 以 80% 以上为目标，但关键业务分支是否被验证优先于数字。
- 登录、权限、金额、状态流转和数据隔离等高风险逻辑必须覆盖明确的成功与失败场景。
- 认证和 RBAC 形成稳定基线后，再确定 CI 的覆盖率下限，并禁止覆盖率无理由下降。

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
