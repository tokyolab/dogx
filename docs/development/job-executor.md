# 定时任务接入与操作

本文用仓库自带的 `demo.echo` 和 `demo.sharding` 说明业务代码接入、后台配置与执行结果查看。两个示例都不读写业务表，也不解析“执行参数”；它们用于验证从页面发起执行到保存结果的完整链路。

## 业务代码放在哪里

| 位置 | 职责 |
| --- | --- |
| `apps/system/internal/job/echo.go` | System 域的普通示例处理器 `EchoProcessor` |
| `apps/system/internal/job/sharding.go` | System 域的分片示例处理器 `ShardingProcessor`，包含准备、分片处理和收尾 |
| `apps/system/rpc/system.go` | 在 `registerProcessors` 中注册名称与具体处理器 |
| `apps/system/rpc/internal/svc/servicecontext.go` | 初始化 System 依赖，再调用注册函数并启动公共执行组件 |
| `pkg/jobexecutor` | 公共处理器契约、接单、执行、回报和取消机制 |

System 业务处理器属于 System 域，和该域的 Repository 一样放在 `apps/system/internal` 下。RPC 入口负责装配；具体业务处理不写进 RPC Server。处理器可以通过构造函数接收本域 Repository 等依赖，注册回调收到的 `ServiceContext` 用于提供这些依赖，不访问 Job 域内部仓储或 `job_*` 表。

当前注册方式如下，名称区分大小写，必须与页面的“处理器名称”完全一致：

```go
func registerProcessors(_ *svc.ServiceContext, registry *jobexecutor.Registry) error {
    if err := registry.Register("demo.echo", job.EchoProcessor{}); err != nil {
        return err
    }
    return registry.RegisterSharding("demo.sharding", job.ShardingProcessor{})
}
```

注册在依赖初始化之后、开始接单之前执行。处理器表随后冻结；新增业务处理器需要修改代码、注册并重新部署，后台新增 Job 不会动态生成业务代码。同一执行器组的各个进程都应部署该组要执行的处理器，避免负载均衡命中未注册名称的节点。

### 普通与分片接口

完整接口与约束见 [`pkg/jobexecutor/processor.go`](../../pkg/jobexecutor/processor.go)。

| 接口方法 | 输入 | 成功输出 |
| --- | --- | --- |
| `Processor.Execute` | `Input`：Job、Instance、Task ID 与本轮参数 | `Result`：可选 JSON `Data` 和可读 `Summary` |
| `ShardingProcessor.PreProcess` | 同上 | `PrepareResult.Items`：每个 JSON 元素对应一个最终分片 Task |
| `ShardingProcessor.ShardingProcess` | `ShardInput`：公共输入和本片 `Item` | 本片的 `Result` |
| `PostProcessor.PostProcess` | `PostInput`：公共输入及业务分片的 `Total`、`Succeeded`、`Failed` | 收尾的 `Result` |

分片由业务的 `PreProcess` 明确划分。一个元素可以是对象、数组、字符串等合法 JSON，调度器不会再拆分或合并；单轮最多 500 个业务分片。成功准备必须返回非 nil 的 `Items`，空集合表示零片。Post 由同一个分片处理器实现，不单独注册；还须在 Job 配置中打开“Post 收尾处理”。普通任务不运行 Post。

处理器可能被并发调用，准备、各片、重试和 Post 也可能落在不同进程。不要用处理器字段保存一轮执行的中间状态；业务需要共享的数据应保存在自己的业务存储中。Post 只收到最终片数，不收到各片结果；需要明细时查询自己的业务记录。

业务代码应传递并检查 `ctx`，及时结束自身启动的工作，再返回结果。返回 `nil` 错误表示这次业务调用成功，非 nil 错误表示失败；调度端根据 Job 的重试配置决定后续尝试。结果回报或网络故障可能造成再次执行，业务写操作须自行保证幂等。不要用 `panic` 表达可预期的业务失败。

## 启动与执行器组

先按 [README 的本地启动](../../README.md#本地启动)完成 System 和 Job 数据库迁移、配置及服务启动。依赖沿用现有 PostgreSQL、Redis 和 etcd；这两个示例不需要新增服务或数据库。

当前 System 执行器内嵌在 `system-rpc` 进程中，但使用独立的执行器 RPC 入口。Job 进程也分别提供管理与结果回报入口。示例端口如下，实际部署以自己的配置为准：

| 入口 | 示例端口 | 用途 |
| --- | --- | --- |
| System 业务 RPC | 9001 | 原有 System 业务 |
| Job 管理 RPC | 9002 | API 调用 Job 管理接口 |
| Job 结果回报 RPC | 9003 | 执行器回报结果 |
| System 内嵌执行器 RPC | 9004 | 接收调度端的派发和取消 |

后台菜单为：

```text
定时任务
├─ 任务管理
├─ 执行记录
└─ 执行器组
```

在“执行器组”中使用已配置好的组，或点击“新增执行器组”。采用仓库示例配置时填写：

| 页面字段 | 示例值 | 必须匹配的执行器配置 |
| --- | --- | --- |
| 执行器组名称 | `system.executor` | `JobExecutor.GroupName` |
| 服务发现 Key | `/dogx/development/default/executor/system.executor` | `JobExecutor.Server.Etcd.Key` |
| 组 Token | 创建后从“详情”读取，不能自行编造 | `JobExecutor.Token` |

组名与服务发现 Key 创建后不可修改。Key 由部署方明确填写，不是根据组名自动生成；组 Token 由服务端生成。配置文件中的 Token 保留引号，例如 `Token: "${DOGX_JOB_EXECUTOR_TOKEN}"`，环境变量使用该组详情的真实值，禁止将真实 Token 提交到仓库或写入日志。

同时核对以下发现关系：

- `JobExecutor.Server.Etcd.Hosts` 使用调度端能够访问的同一服务发现环境，注册的地址必须能被调度端连接。
- `JobExecutor.ReportClient.Etcd.Key` 对应 Job 的 `ExecutionRPC.Etcd.Key`，示例为 `/dogx/development/default/rpc/job-execution`。
- API 的 `JobRpc.Etcd.Key` 对应 Job 的管理 `Etcd.Key`，示例为 `/dogx/development/default/rpc/job`。这不是结果回报 Key。

执行器组 Token 不填写到 zrpc 原生的 `App/Token` 中。完整配置见 [System RPC 示例](../../apps/system/rpc/etc/system-rpc.example.yaml)与 [Job RPC 示例](../../apps/job/rpc/etc/job-rpc.example.yaml)。组名、Key 或凭据未对应时，应修正配置并重启执行器，不能通过关闭组认证来绕过。

首次空库尚无可用后台时，先按 [System 内嵌执行器接入](../../README.md#system-内嵌执行器接入)通过 Job 管理 RPC 创建组并取得 Token，然后启动 System。已有本地测试环境可直接使用现有组，无须再创建同名组。

演示沿用现有运行参数：执行器业务并发 32、等待队列 128、结果保留 256、回报并发 16；调度端派发并发 64。这些是示例配置／默认值，不要求为三片演示修改并发或队列大小。

## 从页面执行普通任务

打开“定时任务 → 任务管理 → 新增任务”，填写：

| 字段 | 值 |
| --- | --- |
| 任务名称 | `Echo 演示` |
| 执行器组 | 选择已与 System 执行器配置匹配的组，例如 `system.executor` |
| 处理器名称 | `demo.echo` |
| 执行模式 | 普通任务 |
| Cron 表达式 | `0 */5 * * * *` |
| 执行参数 | 留空 |
| 错过调度策略 | 跳过错过的执行 |
| 阻塞策略 | 存在未结束实例时跳过 |
| 最大重试次数 | `0` |
| 重试间隔（秒） | `10` |
| 实例超时（秒） | `60` |

上述数值是方便观察一次执行的演示设置，不修改程序默认值。Cron 为包含秒的六段表达式，示例表示每逢五分钟整点执行；可以点击“预览执行时间”确认，时区取调度进程的时区。

1. 保存后任务默认处于停用状态，先保持停用，避免演示期间自动重复运行。停用不影响手动执行。
2. 点击该行“手动执行”，保持“覆盖本次执行参数”关闭，再点击弹窗中的“手动执行”。
3. 请求成功后页面进入对应的“执行记录”。“已提交执行”表示创建请求已完成，仍需查看执行结果。
4. 打开本轮“详情”，等待 Instance 状态为“成功”。在“本轮 Task”中应有一个类型为“普通”、状态为“成功”的 Task。
5. 打开该 Task 的“详情”，其“结果摘要”为 `hello world`。

`demo.echo` 只返回摘要，没有返回 JSON 结果。“加载结果”显示未保存数据是正常行为；`hello world` 不会自动出现在浏览器控制台。日志位置见下文。

## 从页面执行分片任务

再新增一个任务，沿用上一节的组、Cron、策略和数值，仅调整：

| 字段 | 值 |
| --- | --- |
| 任务名称 | `Sharding 演示` |
| 处理器名称 | `demo.sharding` |
| 执行模式 | 分片任务 |
| Post 收尾处理 | 开启 |
| 执行参数 | 留空 |

保存后同样保持停用并手动执行。正常执行的顺序如下：

```text
准备 Task：返回 Alice、Bob、Carol 三个 JSON 对象
  → 三个分片 Task：分别返回 hello Alice、hello Bob、hello Carol
  → 收尾 Task：记录业务分片最终统计
  → Instance 成功
```

三个分片允许并行执行，显示顺序或日志先后不代表业务顺序。在执行实例详情中检查：

- “本轮配置”的处理器为 `demo.sharding`、执行模式为“分片”、“启用收尾”为“是”。
- “分片统计”为总数 3、成功 3、失败 0、已取消 0、未结束 0；这里不包含准备和收尾 Task。
- “本轮 Task”共 5 条：1 个“准备”、3 个“分片”、1 个“收尾”，全部成功；Instance 也为成功。

各 Task 的“详情 → 业务数据”通过“加载输入”或“加载结果”单独读取。示例成功后的预期内容如下，JSON 对象字段顺序不影响含义：

| Task 类型 | 结果摘要 | 加载输入 | 加载结果 |
| --- | --- | --- | --- |
| 准备 | `prepared 3 shards` | 无独立输入；公共参数见“本轮配置” | `[{"name":"Alice"},{"name":"Bob"},{"name":"Carol"}]` |
| 分片（Alice） | `hello Alice` | `{"name":"Alice"}` | `{"name":"Alice","message":"hello Alice"}` |
| 分片（Bob） | `hello Bob` | `{"name":"Bob"}` | `{"name":"Bob","message":"hello Bob"}` |
| 分片（Carol） | `hello Carol` | `{"name":"Carol"}` | `{"name":"Carol","message":"hello Carol"}` |
| 收尾 | `completed 3 shards: 3 succeeded, 0 failed` | 最终业务分片统计 | `{"total":3,"succeeded":3,"failed":0}` |

未结束的实例和 Task 详情会每 5 秒刷新，隐藏页面时暂停。输入与结果数据不随自动刷新重新加载，需要再次点击按钮获取新值。Task 正在重试时，旧摘要或旧结果不能证明当前尝试已经完成，应同时检查状态和尝试编号。

如需观察不带 Post 的执行，可在任务停用时编辑，关闭“Post 收尾处理”并再次手动执行。新一轮应有 1 个准备和 3 个分片，共 4 个 Task；之前执行记录中的配置快照不会变化。

Post 是分片正常阶段推进的一部分，不是任何退出路径都保证调用的 `finally`。准备最终失败、人工停止或整轮超时不保证执行 Post；片失败后的收尾成功也不会把失败的整轮改成成功。Post 失败不会让已经成功的分片全部重跑，其重试属于 Post Task 自身。

## 日志、定时启停与排查

### 日志在哪里

两个示例的业务日志写到执行它们的 `system-rpc` 进程，使用 info 级别。前台运行且使用 console 日志配置时，在启动该进程的终端查看；后台启动、容器或文件日志则查看相应进程的 stdout／日志配置指定位置。管理页面展示持久化摘要和 JSON 数据，没有在线查看完整进程日志的功能。

普通示例日志消息为 `hello world`；分片为 `hello Alice`、`hello Bob`、`hello Carol`，收尾为 `demo sharding completed`。日志含 `job_id`、`instance_id`、`task_id`，可用页面 ID 关联；收尾另含 `total`、`succeeded`、`failed`。准备阶段不额外写演示日志。若关闭 info 日志，页面中的持久化结果仍是判断本轮状态的入口。

### 启用、停用与停止本轮

| 操作 | 效果 |
| --- | --- |
| 任务管理的“状态”开关开启 | 按 Cron 产生后续执行实例，列表显示“下次计划时间” |
| 任务管理的“状态”开关关闭 | 停止后续定时触发，已有实例继续执行；仍可手动执行 |
| 执行实例详情中的“停止本轮” | 停止这一轮后续调度，收敛未结束 Task 的数据库状态，并尽力通知执行器取消 |

“停止本轮”针对 Instance，覆盖这一轮的相关 Task；不是停用 Job，也不是逐个在页面停止 Task。已经结束的实例没有该按钮。停止过程中可能显示“停止中”，应继续刷新确认最终状态；远端通知成功与数据库状态结束都不保证业务代码已立即退出，取消依赖业务代码响应 Context。已经提交的业务副作用也不会自动撤销。

两个演示都很快结束，点击前可能已经成功，不能把它们当作长任务强制中断的验证工具。演示完成后保持 Job 停用即可；删除任务须先停用，且不能仍有未结束实例。历史记录仍按 Job 历史保留配置清理，不与定义删除同步清空。

### 常见现象

| 现象 | 检查方向 |
| --- | --- |
| 派发被拒绝 | 查看 Task 结束原因和摘要；核对名称、执行模式、Post 实现及组凭据，各执行器副本应注册一致 |
| 长时间待执行或执行结果不确定 | 检查 Job 与执行器进程、etcd 服务发现和目标地址连通性，再查看对应进程日志 |
| 业务日志已有输出，页面尚未成功 | 业务完成后仍须回报并保存结果；检查回报入口配置与 Job 日志，不仅凭一条日志判断成功 |
| 执行失败后再次运行 | 检查“最大重试次数”；首次之外的尝试由此控制，业务必须容忍重复调用 |
| 停用后仍看到实例执行 | 停用只控制后续定时触发；需要取消当前一轮时使用“停止本轮” |
| 修改任务后旧记录仍是原参数 | Instance 保存创建时的本轮配置快照，修改影响之后的新实例 |

## 示例验证记录

2026-11-24 使用隔离 PostgreSQL 测试库、Redis、etcd 和真实 System／Job 进程，从现有前端创建并手动执行两个停用状态的示例：

| 检查项 | 实测结果 |
| --- | --- |
| `demo.echo` | Instance 和唯一普通 Task 成功；页面摘要及 System info 日志均为 `hello world`，未保存 JSON 结果 |
| `demo.sharding`，开启 Post | Instance 与准备、三个分片、收尾共 5 个 Task 全部成功；业务分片统计为 3／3／0 |
| 分片数据 | 页面读取 Alice 分片输入和问候 JSON 结果成功；System 日志包含 Alice、Bob、Carol 各片的问候及对应 Task ID |
| Post 数据 | 页面读取结果为 `{"total":3,"succeeded":3,"failed":0}`，日志统计一致 |

新增分片处理器测试覆盖阶段输出、非法输入和 Context 取消；普通示例单元测试覆盖固定摘要、无 JSON 结果、日志内容与 Task 身份、忽略参数，以及 Context 取消／过期时不执行业务输出。相关 System RPC／ServiceContext 检查及 lint 通过。此次页面验证不包含自动 Cron 触发、失败重试、停止竞争或关闭 Post 的场景；这些示例操作结果不代替调度模块的专项验收。未修改前端代码。
