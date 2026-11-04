# 契约代码生成规范

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

API 字段校验标签必须写入 `.api` 契约源文件，由 goctl 生成到 Types。进程启动时通过 `httpx.SetValidator` 注册全局 Validator，生成 Handler 中已有的 `httpx.Parse` 会自动执行校验；不得为接入校验手工修改生成 Handler 或 Types。校验标签只允许使用 `go-playground/validator` 官方内置 Tag，不注册项目自定义 Tag。

## RPC 契约

应用源文件按域维护：`apps/system/rpc/system.proto`、`apps/job/rpc/job.proto`，继续使用 goctl 生成应用代码。

在仓库根目录执行：

```powershell
Set-Location 'apps/system/rpc'
goctl rpc protoc system.proto --go_out=./types --go-grpc_out=./types --zrpc_out=. --style gozero
```

Job RPC 在仓库根目录执行以下固定命令，沿用 goctl 1.10.2：

```powershell
Set-Location 'apps/job/rpc'
goctl rpc protoc job.proto --go_out=./types --go-grpc_out=./types --zrpc_out=. --style gozero
```

Job HTTP 契约放在 `apps/system/api/desc/job.api`，仍由唯一入口 `system.api` 导入并生成，不新增 API 生成入口。各域生成的 RPC Client、Server 和协议文件均遵守下述不可手改规则。

以下文件包含 `DO NOT EDIT` 标记，禁止直接编辑：

- `types/**/*.pb.go`
- `types/**/*_grpc.pb.go`
- `internal/server/*.go`
- `systemclient/*.go`、`jobclient/*.go`

应用 RPC 业务实现写在 `internal/logic`，依赖装配写在 `internal/svc`。

## 公共执行协议 {#execution-protocol-generation}

[执行通信协议](../design/scheduler/data-contracts.md#execution-rpc-contract)已批准、尚未实现：共享契约源文件为 `pkg/jobexecutor/protocol/execution.proto`，同一文件定义 `Executor`（`DispatchTask`、`CancelInstance`）和 `ResultReceiver`（`ReportTaskResult`）两个服务。`go_package` 固定为 `github.com/tokyolab/dogx/pkg/jobexecutor/protocol`。

该子包只保存协议源文件与标准生成代码，不导入父级执行组件或业务域内部包。Job 调度端可单独导入协议子包，无须依赖执行器运行组件。具体 Server、业务适配与依赖由各自宿主装配，运行时仍统一使用 zrpc＋etcd。

这是公共协议的窄例外：goctl 1.10.2 的 RPC 生成要求 `--zrpc_out`，会生成应用骨架，不能用于这里的纯协议生成。以后创建或修改该 `.proto` 时，在**仓库根目录**执行 README 中同一固定命令：

```powershell
protoc --proto_path=. --go_out=. --go_opt=module=github.com/tokyolab/dogx --go-grpc_out=. --go-grpc_opt=module=github.com/tokyolab/dogx pkg/jobexecutor/protocol/execution.proto
```

命令使用官方 `protoc`、`protoc-gen-go` 和 `protoc-gen-go-grpc`，只生成同目录的 `execution.pb.go`、`execution_grpc.pb.go`，不生成或删减 goctl 应用骨架。这些生成文件同样禁止手改，契约变化必须重新生成并提交结果。不得据此替换现有 System／Job 应用 RPC 的 goctl 生成流程；设计记录阶段不创建 `.proto`、生成文件或运行生成命令。

## 变更检查

1. 修改契约源文件。
2. 应用契约使用项目约定的 goctl 版本与固定命令；公共执行协议使用上文限定的 protoc 命令，不混用两种生成流程。
3. 检查 Git diff，确认 `DO NOT EDIT` 文件的变化全部能由契约变化解释。
4. 同步受影响的 Logic 或执行通信适配的方法签名，并补充对应测试。
5. 运行 `goctl api validate`、`go test ./...` 和 `go vet ./...`。

模块路径迁移也必须重新运行生成命令，不能对生成文件执行全局文本替换。
