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
- 调度服务独立多副本部署，同一组副本承担选主、触发及后续对等推进职责，不拆出单独的 Leader 服务。
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
│       │   ├── service/            跨模型事务等业务编排
│       │   └── migration/          Job 域迁移与 SQL
│       └── cmd/migrate/            独立 Job 迁移命令
├── pkg/                            真正公共的技术组件
│   ├── model/                      Base 主键与审计字段，无业务模型
│   ├── postgres/                   配置类型、DSN 与连接工厂
│   ├── migratecmd/                 迁移 CLI 执行器，各域注入 Provider
│   └── testutil/postgres/          测试连接、隔离 Schema 与清理
└── docs/                           设计、规范及 ADR
```

上述为已确认的实现路径。Job HTTP 契约仍由 `system.api` 统一导入和生成；Logic 负责用例编排，不直接执行 SQL。跨模型短事务由显式业务服务协调，同一事务中的仓储操作共享事务连接，不各自开启独立事务。调度运行组件的资源由进程装配并关闭；继续执行一个 Model 对应一个 Repository 的规范，不建立通用 BaseRepository，也不允许跨域导入 `internal` 数据层。

`job-rpc` 既提供管理 RPC，也承担调度运行职责，不是只有 CRUD 的服务。业务处理器仍运行于所属业务进程或同域 Worker，不放进 `scheduler/`。公共执行组件在仓库内复用，具体包路径在执行模块设计时确定；不为本次接入增加独立 SDK Module、发布流程或调度端注册／心跳中心。管理入口使用生成的 `jobclient`，不因此获得访问 Job 内部数据层的权限。

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
| 触发 Leader 选举 | 官方 `concurrency.Session + Election`；独立任期上下文，部署租约初值 15 秒 |
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

## 验证要求

- Job RPC 不可达时，原有 System 启动与非 Job 路由不被新增依赖拖垮；Job 路由不能假成功。
- System 与 Job 迁移在同库使用不同账本，分别执行和回滚不误读对方的版本记录。
- API 不持有 Job 数据库连接，不跨域导入 Model／Repository；八条管理路由有权限登记与安全测试。
- 多副本选主、失主清理、通知丢失和分钟核对通过真实 etcd／PostgreSQL 测试；其他业务未被改成 etcd 锁。
- 生成规范和 CI 覆盖新目录，文档通过不能替代实现后的功能验证。
