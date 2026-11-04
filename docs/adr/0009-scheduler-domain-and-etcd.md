# ADR-0009 调度域与 etcd 协调边界

- 状态：已通过，总审已收口；2026-10-16 已应用规范与首批实现，本地验证及后续边界见[进度页](../design/scheduler/progress.md#trigger-local-verification)。
- 日期：2026-10-16
- 关联：[ADR-0001](0001-dogx-v0.1-architecture.md)、[总体架构](../design/scheduler/architecture.md)、[开工总审](../design/scheduler/trigger-readiness-review.md)

## 背景

调度框架已有独立的数据所有权、多副本运行和故障隔离需要，符合 ADR-0001 新增业务域的条件。现有仓库只有 System 域，代码生成、迁移和部分 CI 命令也只列出 System 路径；调度设计中的 etcd 选主还需要明确区别于现行 Redis 分布式锁规则。

本 ADR 不重新选择已通过的架构，不增加 Task 执行协议，也不把 PostgreSQL 状态转换换成分布式锁。

## 已通过的架构依据

- 产品定位为 DogX 一体化后台内置的分布式调度能力；后端保持单 Git 仓库、单 Go Module，当前不拆分独立调度产品或发布对外执行器 SDK。
- 调度数据属于 `job` 域。管理 API/BFF 不直接访问组、Job、Instance 或 Task 表；执行器只通过 RPC 回报，不持有调度数据库写入逻辑。
- 调度服务独立多副本部署，同一组副本承担选主、触发及后续对等推进职责，不拆出单独的 Leader 服务。2026-10-22／23 补充确认：现有 Leader 还负责 RUNNING Instance 的低频统一复核和 STOPPING Instance 的停止恢复候选发现；两类查询分开、各有预算，正常结果与停止决定提交后的处理仍由本节点即时推进，不建立整轮负责人。
- 使用 `robfig/cron/v3` 维护长期规则，etcd 负责选主、服务发现和 Job 配置通知，PostgreSQL 保存权威事实。
- Job 和 Instance 的短事务、条件更新及唯一约束保证持久化规则；不增加全局数据库任期锁，不承诺旧 Leader 的合法在途事务绝不提交。
- 本批交付止于 WAITING Instance 与本轮配置，Task 执行、组凭据和客户端生命周期仍在后续模块落实。

## 本次总审的接入方案

### 进程与代码归属 {#project-layout}

管理调用链为 `system-api → job-rpc → PostgreSQL`。复用现有 BFF 的登录、Session、Casbin 和响应包装，不新增 `job-api`，不把 Job 逻辑塞进 `system-rpc`。

已确认的后端工程目录如下，省略与本批无关的文件：

```text
dogx/                                后端 Git 仓库
├── go.mod                           保持一个 Go Module
├── go.sum
├── apps/
│   ├── system/
│   │   ├── api/                     system-api 进程，统一管理入口
│   │   │   ├── system.api          HTTP 唯一生成入口
│   │   │   ├── desc/
│   │   │   │   └── job.api         Job 管理 HTTP 契约
│   │   │   └── internal/
│   │   │       └── logic/job/      调用 Job RPC，不直接访问调度表
│   │   ├── rpc/                    原 system-rpc 进程
│   │   ├── internal/               System 数据层
│   │   └── cmd/migrate/            System 迁移命令
│   └── job/                        新增调度业务域
│       ├── rpc/                    独立 job-rpc 进程
│       │   ├── job.go              启动入口
│       │   ├── job.proto           RPC 契约
│       │   ├── jobclient/          生成的 RPC 客户端
│       │   ├── types/job/          生成的协议类型
│       │   ├── etc/                独立部署配置
│       │   └── internal/
│       │       ├── config/
│       │       ├── svc/
│       │       ├── server/         生成的 RPC 服务入口
│       │       ├── logic/          Job 管理用例
│       │       └── scheduler/      选主、Cron、同步、恢复等运行组件
│       ├── internal/
│       │   ├── model/              组、Job、Instance，后续增加 Task
│       │   ├── repository/         调度数据访问
│       │   ├── trigger/            自动／手动创建 Instance 的共享运行组件
│       │   ├── subcode/            Job 域稳定业务错误标识
│       │   └── migration/          Job 域迁移与 SQL
│       └── cmd/migrate/            独立 Job 迁移命令
├── pkg/                            真正公共的技术组件
│   ├── model/                      Base 与 RecordStatus 等基础类型，无业务模型
│   ├── postgres/                   配置类型、DSN 与连接工厂
│   ├── migratecmd/                 迁移 CLI 执行器，各域注入 Provider
│   └── testutil/postgres/          测试连接、隔离 Schema 与清理
└── docs/                           设计、规范及 ADR
```

上述为已确认的实现路径。Job HTTP 契约仍由 `system.api` 统一导入和生成；管理 Logic 负责各自用例编排，不直接执行 SQL。自动／手动创建 Instance 提取为 `trigger.Creator`，不把普通 CRUD 一并塞入大 Service。域内 `repository.TransactionRunner` 仅提供同一事务连接上的仓储，不包含业务规则；Logic 与 Creator 保留各自的判断顺序和事务边界，不各自开启独立事务。调度运行组件的资源由进程装配并关闭；继续执行一个 Model 对应一个 Repository 的规范，不建立通用 BaseRepository，也不允许跨域导入 `internal` 数据层。

`job-rpc` 既提供管理 RPC，也承担调度运行职责，不是只有 CRUD 的服务。业务处理器仍运行于所属业务进程或同域 Worker，不放进 `scheduler/`。执行模块已确认公共组件使用 `pkg/jobexecutor`，System 处理器放在 `apps/system/internal/job`，首个接入由 `system-rpc` 装配；具体边界见[执行器接入与处理器注册](../design/scheduler/architecture.md#processor-layout)。不为本次接入增加独立 SDK Module、发布流程或调度端注册／心跳中心。管理入口使用生成的 `jobclient`，不因此获得访问 Job 内部数据层的权限。

2026-10-19 确认执行接入采用同进程独立 RPC Server：`job-rpc` 分开管理与执行器通信入口，System 首版在 `system-rpc` 内嵌由 `pkg/jobexecutor` 创建的执行器 Server，与原业务 Server 分开，不新增 Worker 进程。公共组件和域内处理器不依赖宿主 RPC 的 `internal`，保留以后拆分同域 Worker 的能力。两套监听复用进程内业务组件与数据连接，不增加管理 RPC 到回报 RPC 的内部转发；组 Token 只校验执行协议。管理 RPC 保留 go-zero 原生鉴权配置能力，由部署方决定启用和配置，不另建管理权限机制或强制网络访问规则。具体边界见[RPC 入口与鉴权装配](../design/scheduler/architecture.md#rpc-boundaries)；本项为设计决定，执行组件及第二套入口尚未实现。

2026-10-30 进一步通过[执行 RPC 协议与装配](../design/scheduler/data-contracts.md#execution-rpc-contract)：调度端与执行端的 Server／Client 统一使用 zrpc，注册发现保留 etcd。公共契约置于 `pkg/jobexecutor/protocol`，仅此纯协议包以官方 protoc 生成 protobuf／gRPC 绑定，应用契约仍按 goctl 固定命令生成。三方法为 Executor.DispatchTask、Executor.CancelInstance 与 ResultReceiver.ReportTaskResult；请求体 GroupAuth 由全部执行业务入口共用认证逻辑校验，新增执行入口不叠加原生 App／Token，保留标准 Health 但不将其等同业务就绪，不注册反射。宿主统一进程级配置及包含框架阶段的共享 30 秒停机预算，RpcServer.Stop 不作为完成屏障，退出回调不得等待 Start 返回，全局日志最后关闭。生成、适配和跨平台验证尚未实施，非 BUSY 明确拒绝的当前尝试结算仍须闭合；本决定不授权执行模块编码，也不建立新注册／心跳中心。

新增 Job RPC 客户端使用非阻塞连接；调度服务暂不可达不能使现有 System API 整体无法启动。相关 Job 请求按既有技术错误规则失败，不修改 System 当前 `/health`、`/ready` 的检查范围。Job 运行能力的监控不以“是不是 Leader”代替健康判断。

### 迁移与权限目录

Job 的迁移由 `apps/job/internal/migration` 与独立迁移命令管理，使用 Goose 实例级 `WithTableName("job_goose_db_version")`，不修改全局表名或借用 System 迁移账本。即使两个域使用同一个物理数据库，也不会把不同迁移流的版本混在一起。

命令入口复用 `pkg/migratecmd`，数据库配置类型及连接工厂复用 `pkg/postgres`；公共技术实现不改变各域独立配置、连接池、迁移 SQL、Provider 与账本。符合软删除语义的模型嵌入 `pkg/model.Base`；审计时间不承担配置同步或调度进度语义，规则见[基础模型规范](../development/database-migrations.md#base-model)。

System 的 `sys_api` 登记仍由 System 域增量迁移维护；本批 Job 管理路由须全部登记，并进入现有路由安全矩阵。Job 迁移不写 System 权限表，不因暂未开发菜单页面而绕过接口鉴权。

生产迁移仍由单独的部署步骤顺序执行，不由各 RPC 副本启动时自动迁移。Goose 时间戳开发版本、发布前整理、Up／Down 和已发布迁移不可变规则保持不变；不使用 `AutoMigrate`。

### etcd 是限定例外

etcd 只用于已经批准的调度控制与发现职责：

| 用途 | 协议与边界 |
| --- | --- |
| 调度 Leader 选举 | 官方 `concurrency.Session + Election`；独立任期上下文，部署租约初值 15 秒；同一 Leader 承担 Job 触发、RUNNING Instance 统一复核及 STOPPING 停止恢复候选发现，不另建一套选举 |
| Job 配置通知 | 独立的持久 `reload` 标记；提交后通知，每分钟数据库核对兜底，不是可靠消息 |
| RPC 服务发现 | go-zero 对接 etcd；不由应用另建数据库心跳注册中心 |

键空间、访问权限和 I/O 预算沿用[etcd 接入协议](../design/scheduler/execution.md#etcd-integration)。选主和通知范围集中由调度组件维护；组 Token 不是 etcd 凭据。配置通知与 PostgreSQL 不构成原子提交，失主取消也不是数据库提交屏障。

本 ADR 不允许把所有业务锁迁往 etcd，不为每个 Job／Instance／Task 创建 etcd 锁；现有业务使用 Redis 的机制保持不变。不引入 PostgreSQL advisory lock，也不放开事务内外部 I/O。

## 实施时的规范同步

本节清单已随总审批准，并在获得编码授权后随首批实现应用到生效规范及 CI；交付与验证结果单独记录。

1. 将 `AGENTS.md` 中跨进程协调统一使用 Redis 的条目细化为：

   > 不使用 PostgreSQL advisory lock。跨进程业务互斥默认使用 Redis 分布式锁，并通过公共组件集中管理。ADR-0009 限定允许调度域使用 etcd 官方 Session／Election 选主，并以 etcd 承担已定服务发现和配置通知；不得扩展为逐 Job／Instance／Task 分布式锁。数据库短事务、条件更新和唯一约束的并发协议不因此改变。

2. 将 RPC 契约源文件说明扩展为按域列出 `apps/system/rpc/system.proto` 和 `apps/job/rpc/job.proto`；HTTP 仍只有 `apps/system/api/system.api` 一个生成入口。
3. 在代码生成规范补 Job RPC 的固定命令，版本沿用仓库 goctl，不手改生成文件。
4. 在迁移规范补 Job 域目录、独立账本和命令；System 原命令保持可用。
5. CI 加入 Job 域的 PostgreSQL、etcd 及多副本触发验证，保留现有 System 检查。

## 取舍

新增一个有独立数据职责的 RPC 进程及其部署、迁移、测试配置；换取调度伸缩和故障不挤入 System 业务进程。etcd 是明确的运行依赖，但不代替业务事实存储。独立迁移账本避免跨域版本冲突，不意味着允许各副本并发执行迁移。

接受非 Leader 正常服务、通知最终同步、旧合法事务可能提交以及有界队满可能错失触发等已定边界；不以本 ADR 承诺零故障、恰好一次业务执行或具体吞吐。

RUNNING Instance 复核由 Leader 每 2 分钟执行或继续有界扫描，以本趟数据库时间减 4 分钟为开始时间上界，每页 256 条，交给本节点已有 Instance 检查组件。失主停止新候选发现，已接纳检查仍按数据库协议完成；新 Leader 就绪后从头开始一次有界扫描，仍遵守 4 分钟门槛，不转移游标。没有有效 Leader 时周期复核及其超时发现暂停，正常结果接收与本地推进不因此暂停。这是已批准、待实现的发现职责调整，不把全部执行恢复集中到 Leader；完整范围与取舍见[统一 Instance 复核](../design/scheduler/execution.md#instance-recheck-direction)。

2026-10-29 后续已批准 [RUNNING 扫描预算与恢复补充](../design/scheduler/execution.md#instance-recheck-defaults)：在共享检查集合占用低于总量一半时接纳新的恢复 ID，已有 ID 可合并，不按来源分池或预留并发；达到门槛或集合满时，在本周期剩余扫描预算内等待空间，替代 RUNNING 满即退出规则。默认查询 1 秒、每周期扫描／交接 5 秒，参数与处理预算分别管理。该取舍减少恢复突发挤占正常提示，但不保证持续或覆盖整个扫描窗口的拥塞下恢复仍能准入，也不承诺恢复 SLA；STOPPING 的独立队满退出规则保持。

2026-10-23 已批准停止协议改为持久化 `STOPPING = 6` 后按 Instance 有界取消未结束 Task，确认子记录全部终态后再结束父 Instance；不等待远端停止成功。STOPPING 候选复用同一 Leader 每 2 分钟的低频驱动，但独立查询与分页预算、不套用 RUNNING 的 4 分钟开始时间门槛，新 Leader 就绪后进行一次有界检查。处理使用既有后台恢复容量，不占派发的 64 个工作协程；没有有效 Leader 时周期发现暂停，本地已接纳处理仍可继续。本项不新增选举、整轮 owner、可靠通知表或全局 Task 扫描，具体协议见[停止与超时](../design/scheduler/data-contracts.md#instance-termination-sql)。这是待实现设计，不修改本批已交付的状态枚举及迁移。

## 验证要求

- Job RPC 不可达时，原有 System 启动与非 Job 路由不被新增依赖拖垮；Job 路由不能假成功。
- System 与 Job 迁移在同库使用不同账本，分别执行和回滚不误读对方的版本记录。
- API 不持有 Job 数据库连接，不跨域导入 Model／Repository；八条管理路由有权限登记与安全测试。
- 多副本选主、失主清理、通知丢失和分钟核对通过真实 etcd／PostgreSQL 测试；其他业务未被改成 etcd 锁。
- 生成规范和 CI 覆盖新目录，文档通过不能替代实现后的功能验证。
