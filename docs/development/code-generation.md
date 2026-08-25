# goctl 代码生成规范

DogX 将 `.api` 和 `.proto` 作为契约源文件，生成文件提交到 Git，但禁止直接编辑。

## API 契约

生成入口：`apps/system/api/system.api`

- `system.api` 只维护 `desc/*.api` 导入。
- `desc/base.api` 只保存跨多个业务资源复用的基础类型。
- 每个业务资源的类型、路由和 Handler 声明放在对应的 `desc/<resource>.api`。
- 只对 `system.api` 运行生成命令，不单独生成任何契约片段。
- 业务路由采用 `/resource/action` 风格并默认统一使用 `POST`，查询和列表也不例外；只有协议或工具生态明确要求时才使用 `GET` 等其他 Method。

在仓库根目录执行：

```powershell
Set-Location 'apps/system/api'
goctl api go --api system.api --dir . --style gozero
```

生成结果分为两类：

- `routes.go`、`types.go` 标记为 `DO NOT EDIT`，每次生成都可能覆盖，只能通过修改 `.api` 改变。
- Handler 和 Logic 是 `Safe to edit` 的脚手架，已有文件不会被 goctl 覆盖；Handler 保持 HTTP 参数解析和调用 Logic 的职责，业务实现写入 Logic。

DogX 使用 goctl 默认 Handler 的 `httpx.ErrorCtx` 和 `httpx.OkJsonCtx`，不维护自定义 Handler 模板。所有对外 API 必须声明响应类型，否则成功响应不会经过全局 JSON 成功处理器。

## RPC 契约

源文件：`apps/system/rpc/system.proto`

在仓库根目录执行：

```powershell
Set-Location 'apps/system/rpc'
goctl rpc protoc system.proto --go_out=./types --go-grpc_out=./types --zrpc_out=. --style gozero
```

以下文件包含 `DO NOT EDIT` 标记，禁止直接编辑：

- `types/**/*.pb.go`
- `types/**/*_grpc.pb.go`
- `internal/server/*.go`
- `systemclient/*.go`

RPC 业务实现写在 `internal/logic`，依赖装配写在 `internal/svc`。

## 变更检查

1. 修改契约源文件。
2. 使用项目约定的 goctl 版本运行生成命令。
3. 检查 Git diff，确认 `DO NOT EDIT` 文件的变化全部能由契约变化解释。
4. 同步已有 Logic 的方法签名并补充测试。
5. 运行 `goctl api validate`、`go test ./...` 和 `go vet ./...`。

模块路径迁移也必须重新运行生成命令，不能对生成文件执行全局文本替换。
