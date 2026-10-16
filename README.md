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
    rpc/              RPC 进程、内部 gRPC 契约和进程私有实现
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

真实配置已被 Git 忽略。密码可以直接保存在本地真实配置中，也可以使用环境变量注入。

首次启动前先执行数据库迁移：

```powershell
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' up
```

然后启动 RPC：

```powershell
$env:DOGX_POSTGRES_PASSWORD = '<postgres-password>'
$env:DOGX_REDIS_PASSWORD = '<redis-password>'
$env:DOGX_ACCESS_SECRET = '<at-least-32-byte-random-secret>'
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

## Job 调度域（首批触发模块）

管理入口为 `system-api → job-rpc → PostgreSQL`。`job-rpc` 是独立进程，副本通过 etcd 选主；只有 Leader 维护 Cron 规则和触发恢复，各副本均可处理管理请求。本批止于已持久化的 WAITING Instance 和执行配置快照，**尚不执行 Task，没有组管理页面或隐含默认组**。

```powershell
Copy-Item 'apps/job/rpc/etc/job-rpc.example.yaml' 'apps/job/rpc/etc/job-rpc.yaml'
go run ./apps/job/cmd/migrate -f 'apps/job/rpc/etc/job-rpc.yaml' up
go run ./apps/job/rpc -f 'apps/job/rpc/etc/job-rpc.yaml'
```

先配置可用的 PostgreSQL 与 etcd，所有调度副本使用相同的进程时区。`Scheduler.Prefix` 必须隔离环境和逻辑集群；Job RPC 的 `Etcd.Key` 与 API 的 `JobRpc.Etcd.Key` 必须相同。示例的 `Postgres.TimeZone` 是数据库会话时区，不替代操作系统／容器进程时区。数据库默认同库同 `public` schema；Job 有独立配置、连接池、`job_*` 表及 `job_goose_db_version` 迁移账本，初次生产部署可配置到独立数据库。

System 域另有增量迁移登记八条 Job 接口权限，按原 System 迁移命令执行，不由 Job 迁移跨域写入。`/job/list`、`/job/get`、`/job/create`、`/job/update`、`/job/status/update`、`/job/delete`、`/job/cron/preview`、`/job/trigger` 均为受 Session 和 Casbin 保护的 POST 路由。

API 的 Job 客户端按首次请求初始化；Job 或其 etcd 离线不增加原 System 启动及 `/ready` 依赖。初始化请求仍有超时，并只允许一个在途初始化。go-zero 首次 etcd 发现使用其共享客户端持续重试，无法由单次请求取消；关闭后晚到的连接会被释放，持续断连时最多保留该进程级初始化操作直到进程退出，不随请求数增加。

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

修改 Protobuf 契约后，在 RPC 目录重新生成：

```powershell
Set-Location 'apps/system/rpc'
goctl rpc protoc system.proto --go_out=./types --go-grpc_out=./types --zrpc_out=. --style gozero
```

Job RPC 使用同一版本和风格，在其独立目录生成：

```powershell
Set-Location 'apps/job/rpc'
goctl rpc protoc job.proto --go_out=./types --go-grpc_out=./types --zrpc_out=. --style gozero
```

生成命令会更新契约派生文件，但不会覆盖已有的 Logic。修改已有接口的输入或输出类型后，仍需手动同步 Logic 方法签名并运行测试。包含 `DO NOT EDIT` 标记的文件禁止直接修改，完整规则见 [goctl 代码生成规范](docs/development/code-generation.md)。

## 测试

```powershell
go test ./...
```

测试默认不连接真实 PostgreSQL 或 Redis；外部依赖通过窄接口替换为测试桩。

更多约定见 [测试方案](docs/development/testing.md)、[PostgreSQL 索引设计与审计规范](docs/development/database-indexes.md)、[架构决策](docs/adr/0001-dogx-v0.1-architecture.md)、[统一响应与 RPC 错误决策](docs/adr/0002-http-response-and-rpc-errors.md)、[认证令牌与会话决策](docs/adr/0003-authentication-and-session.md)、[单租户 RBAC 与接口权限决策](docs/adr/0004-single-tenant-rbac.md)、[Casbin 运行时与多实例同步决策](docs/adr/0005-casbin-runtime-and-policy-sync.md)、[刷新幂等窗口与浏览器同步决策](docs/adr/0006-refresh-rotation-and-browser-sync.md) 和 [v0.1 功能边界](docs/roadmap/v0.1.md)。
