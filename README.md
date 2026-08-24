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
- PostgreSQL 18（迁移脚本尽量兼容 PostgreSQL 17）
- Redis 7.4（按需使用，不为普通 CRUD 自动加缓存）
- Casbin（后续用于接口权限判定）

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
      model/          GORM 持久化模型及字段类型和常量
      repository/     数据访问接口与实现
      migration/      Goose 迁移及 SQL 文件
pkg/                跨进程、跨业务域复用的稳定技术组件
docs/               架构决策和开发文档
```

当前调用链如下：

```text
browser → system-api (HTTP JSON) → system-rpc (gRPC) → PostgreSQL / Redis
```

`system-api` 和 `system-rpc` 是两个独立进程。API 负责 HTTP 契约、参数处理和响应转换；RPC 负责业务逻辑以及 GORM、PostgreSQL、Redis 等后端依赖。两层都使用 goctl 生成的官方目录结构。

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
go run ./apps/system/rpc -f 'apps/system/rpc/etc/system-rpc.yaml'
```

再打开另一个终端启动 API：

```powershell
go run ./apps/system/api -f 'apps/system/api/etc/system-api.yaml'
```

服务启动后：

- `GET /health`：仅检查 `system-api` 进程是否存活，不访问 RPC 或外部依赖。
- `GET /ready`：由 `system-api` 调用 `system-rpc`；RPC 检查 PostgreSQL 和 Redis，任一依赖异常时 API 返回 HTTP 503。

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

生成命令会更新契约派生文件，但不会覆盖已有的 Logic。修改已有接口的输入或输出类型后，仍需手动同步 Logic 方法签名并运行测试。包含 `DO NOT EDIT` 标记的文件禁止直接修改，完整规则见 [goctl 代码生成规范](docs/development/code-generation.md)。

## 测试

```powershell
go test ./...
```

测试默认不连接真实 PostgreSQL 或 Redis；外部依赖通过窄接口替换为测试桩。

更多约定见 [测试方案](docs/development/testing.md)、[架构决策](docs/adr/0001-dogx-v0.1-architecture.md)、[统一响应与 RPC 错误决策](docs/adr/0002-http-response-and-rpc-errors.md) 和 [v0.1 功能边界](docs/roadmap/v0.1.md)。
