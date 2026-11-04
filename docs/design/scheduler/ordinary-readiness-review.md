# 普通执行模块开工检查与开发计划

状态：2026-11-02 静态审计完成，2026-11-04 按产品版本兼容规则修订，同日确认下述六阶段开发安排。六个阶段均未开始，下一步为阶段 1。本文记录开工检查与开发计划，计划确认不代表功能已实现或测试已通过。

审计结论：已批准的普通执行主路径、错误及恢复规则、最小管理契约足以支持实施；未发现需要重新选择产品行为的开工阻塞。当前缺少的 Task 表、公共执行组件及双 Server 属于已定待实现事项。

## 本次范围与交付边界 {#ordinary-scope}

| 本次实现并验收 | 本次不扩展 |
| --- | --- |
| WAITING Instance 首次开始、唯一普通 Task、真实派发与处理器执行、结果回报保存、重试及父终态 | 分片生成权、分批生成、封口、分片阶段推进和 Post 创建 |
| BUSY、明确拒绝、无绑定观察、完成检查、整轮超时、STOPPING 收敛及尽力取消 | owner／租约、Attempt 历史表、可靠消息／回报文件、心跳或失联提前重派 |
| 正式组字段、创建／详情／列表／删除；Instance 详情、所属 Task 分页及停止 | 组编辑、Token 重置、全局执行历史列表、Task 业务 Data 详情和完整页面 |
| 共享执行协议、Job 双 Server、System 内嵌独立执行器 Server、真实依赖及进程退出验证 | 独立 Worker 部署、替换 zrpc／etcd、改造原 System 鉴权 |

普通执行是独立开发验收里程碑，在隔离环境中完成真实 PostgreSQL、etcd、RPC 和故障验证；完整执行能力待分片完成后正式发布，其他第一版交付条件仍适用。不为中间版本增加模式开关、候选过滤索引或过渡状态，不改变已支持的两种模式管理契约，见[已批准发布边界](progress.md#ordinary-integration-contract)。

公共协议按已定 oneof 表达普通、准备、分片和收尾，不因先做普通而另造只支持普通的协议；表达能力不等于本次实现分片持久化和调度流程。Go 内部队列、同步原语和文件拆分结合完整流程实施，不提前规定一套 goroutine／channel 骨架。

## 已批准设计与现有代码的差距 {#ordinary-code-gap}

| 检查项 | 当前代码事实 | 本次必须落实 |
| --- | --- | --- |
| Task 持久化 | 尚无 job_task 表、Model 和 Repository | 按[公共字段](data-contracts.md#task-field-contract)新增，不带软删除、owner 或独立 Attempt 表 |
| STOPPING | Instance Model 只有 1～5；HasActive 与现有活跃索引只含 1／2 | Model、活跃查询及部分索引同步纳入 6，阻塞／删 Job／删组测试同时覆盖；不能仅加枚举 |
| 创建后本地提示 | Creator 已提供 WithInstanceHint，现有 ServiceContext 未安装执行消费者 | 确认提交后非阻塞提示，连接本节点派发；不能在创建回调内同步执行 RPC，提示丢失仍由数据库补查 |
| WAITING 补查 | 现有 Repository 只有 cursor／limit 的触发交接方法 | 落实已批 upper_id、临时分页及派发供给预算；不把旧交接方法当成完整执行扫描 |
| 组接入 | 现有表只有 Base 和 Name，缺少发现 Key／Token 与唯一约束 | 落实完整字段、数据库约束及迁移，增加四个管理入口 |
| 执行通信 | 两端 Main 仍是单 Server；公共协议及执行器包未创建 | 已定共享协议、独立监听入口、认证与日志边界、组缓存及连接生命周期 |
| 宿主退出 | Job 构造 ServiceContext 即启动选主，Close 无总预算等待；现有 defer 不代表双 Server 已有退出协调 | 明确实际停服和后台完成信号，共享停机宽限，依赖与全局日志按真实完成顺序关闭 |

依据：[Instance Repository](../../../apps/job/internal/repository/job_instance_repository.go)、[原迁移](../../../apps/job/internal/migration/migrations/20261016090000_init_job_trigger.sql)、[Creator](../../../apps/job/internal/trigger/creator.go)、[Job 装配](../../../apps/job/rpc/internal/svc/servicecontext.go)。这些是触发阶段和目标执行能力之间的差距，不将尚未引入状态 6 的旧代码误报为已经发生的运行故障。

## 持久化与迁移实施清单 {#ordinary-persistence-plan}

1. 新增 `apps/job/internal/model/job_task.go` 及单文件 `job_task_repository.go`；扩展已有 Instance／Group Repository 和事务适配。普通 CRUD 沿 System 的 GORM 风格，具名运行组件编排，RPC Logic 不直接访问 GORM。
2. 建立已批准 Task 索引：普通／准备／收尾单例唯一、到期重试、无绑定观察、按 Instance 未结束子记录、按 Instance 地址。Instance 增加 RUNNING 复核和 STOPPING 候选索引；活跃 Job 索引谓词改为 `IN (1, 2, 6)`。遵守既有迁移不可变规则。
3. 最小 Task 管理分页覆盖全部状态，现有单例及未结束部分索引不能代替该用途；实施时核对 `(instance_id, id)` 索引及按组删除依赖查询的执行计划。这是查询实现核对，不增加运行扫描或业务规则；普通完成 JOIN 继续复用单例索引，不给该查询新增索引。
4. 按已批协议实现首次启动、ACK 绑定、单条结果保存、重试登记、BUSY 返还、明确拒绝、未知结算、正常完成和停止收敛，保留原尝试／状态条件、零行语义、短事务与提交确认边界。父锁 NOWAIT 仅用于已定入口，不机械扩展所有更新。

### 组表结构与迁移

按[正式组字段](data-contracts.md#executor-group-management-contract)落实 name、discovery_key、token 的非空约束，以及 name／discovery_key 包含软删除记录的全表唯一约束。软删除保留原字段，不保留历史 NULL 例外。

复用现有迁移命令和域内迁移账本，验证空库完整建表、迁移重跑、隔离回滚、约束及两域账本独立性。迁移与 Model、Repository 和测试数据同步实现，服务启动不自动迁移或修数据。

按[产品版本兼容边界](../../development/database-migrations.md#release-compatibility)，本轮不新增旧组回填工具、专用 up-to 命令扩展或分阶段升级流程。触发阶段已有的表结构不构成历史 Release。

## 接口与代码归属 {#ordinary-interface-plan}

### 管理接口落位草案

以下把[已批准的七个管理操作](data-contracts.md#execution-management-contract)落实到既有 POST 路由和 Job 管理 RPC，未新增操作语义：

| HTTP 路由 | Job RPC 方法 | 关键约束 |
| --- | --- | --- |
| `/job/group/create` | CreateExecutorGroup | name／discoveryKey；Token 后端生成，返回 ID |
| `/job/group/get` | GetExecutorGroup | 按 ID 返回详情及完整 Token，屏蔽正文日志 |
| `/job/group/list` | ListExecutorGroups | 分页、名称筛选；不返回 Token，直接供选组 |
| `/job/group/delete` | DeleteExecutorGroup | 既定组锁和引用保护后软删除 |
| `/job/instance/get` | GetInstance | 本轮冻结事实；不依赖当前 Job 未删除 |
| `/job/task/list` | ListInstanceTasks | instanceId／page／pageSize，最多 200 条、按 Task ID 稳定排序；不返回业务 Data |
| `/job/instance/stop` | StopInstance | 只接受 Instance ID，返回已确认的 ID／状态／原因；不新增 accepted 或 stopped 布尔值 |

HTTP 继续使用 System API 的 JWT、SessionAuth、Authorization；按资源拆分 desc 契约并仅由 system.api 生成，复用现有 ID 和分页基础类型，不把业务 DTO 放入 base.api。管理 Proto 追加上述方法，保留已有消息字段号；每项操作一个 Logic，普通查询直接 Repository，停止复用既定运行组件。

管理新业务错误只覆盖已批准五类；实施映射草案为 `job.executor_group.not_found`、`job.executor_group.name_exists`、`job.executor_group.discovery_key_exists`、`job.executor_group.in_use`、`job.instance.not_found`，复用默认业务码与 ErrorInfo.Reason，补充中英文业务翻译。既有 Job 配置引用组失败的 `job.definition.executor_group_unavailable` 保持，不把所有入口强行换为同一 subcode。参数及技术错误沿现有机制。

七条 API 权限资源以新的 System 域迁移登记，沿用现有路由授权矩阵，不修改旧迁移或自动授权普通角色。Job RPC 服务端和 System API 的 Job 客户端都要屏蔽组详情正文；含冻结参数或摘要的查询沿现有敏感业务内容规则核对，测试覆盖正常、慢调用及错误路径，不能仅检查手写日志。业务翻译按现有 i18n 补齐，源码注释及自有诊断／日志保持英文。

公共执行消息的字段编号、可空字段表达和生成后签名在阶段 1 核对；管理接口的源契约、生成物及手写适配随对应功能阶段同步完成。它们是已定语义的实现映射，不逐字段另开产品提案。若实现暴露改变行为的新问题，则明确提出，不能借编号或 DTO 选择修改已批准规则。

### 公共执行协议与宿主分工

| 位置 | 本次职责 |
| --- | --- |
| `pkg/jobexecutor/protocol` | execution.proto、两个标准生成文件和共享明确拒绝标识；不依赖应用 internal 或执行组件父包 |
| `pkg/jobexecutor` | 已定 Processor 输入输出和注册、独立 zrpc Server、组认证、业务执行／取消、结果保留和回报、自身生命周期 |
| `apps/system/internal/job` | 具名业务处理器，使用 System 域依赖，不实现通信或调度状态机 |
| System config／svc／main | 注入业务依赖和处理器，内嵌装配独立执行器入口；暂不拆 Worker |
| Job config／svc／main 及具名运行组件 | 回报入口、组缓存和客户端、派发、结果保存、检查和停止；只初始化一套数据库、选主及既有 Cron |
| Job 既有 scheduler 任期 | 接入 Leader 负责的 RUNNING／STOPPING 候选发现；其他已定对等职责不随失主一起停止 |

运行组件按职责组织，不建立包办 CRUD 和调度的大 Service。共享 Processor／协议可表达后续阶段，分片存储生成及完整阶段链路仍由后续模块实现。

## 必须提前验证的运行兼容点 {#ordinary-runtime-validation}

已静态核对 go-zero v1.10.3：ServiceGroup 并行启停，不能依靠添加顺序释放依赖；RpcServer.Stop 不构成 Server 完成屏障；Unix Start 返回和全局 shutdown 回调存在等待关系，宿主退出回调不能反向等待 Start；Windows 不提供相同的信号钩子实现。每次 NewServer 执行进程级 SetUp，etcd 发布也不证明监听和业务能力已就绪。

这些事实与已批[双入口和停机约定](data-contracts.md#execution-rpc-contract)一致，不需要更换 zrpc 或追加新的产品机制，但必须在获授权后的前段用真实双 Server／etcd／进程试验验证：

- 两套入口的方法注册与认证隔离，Health 成功不代表业务就绪，三个执行业务方法及组详情的正文日志保护。
- Dispatch 的 Peer 是本次实际执行器连接地址；接单响应与异步结果可交错，不能依赖响应先到。
- 暂无执行器、回报依赖故障和 etcd 初始故障时，按已定就绪及故障边界处理，不误改原 System 业务鉴权。
- Unix 和 Windows 分别验证真实停服、后台善后、依赖释放及日志最后关闭；整个进程共用原 30 秒宽限，不把每段重新计时或 Context 取消当实际完成。
- 业务不响应取消、依赖操作滞留等情况必须观察实际宿主退出和资源结果，不能靠 Stop 返回或测试 Stub 推定成功。

试验失败先修实现；若证据表明已批保证在当前依赖下无法达成，再提出具体差异和取舍。当前未运行上述试验，不宣称框架天然满足目标。

## 分阶段开发安排与验收矩阵 {#ordinary-implementation-order}

**以下六阶段开发安排于 2026-11-04 确认，替代此前五步实施顺序。** 按阶段推进，每阶段形成可验证成果；阶段内可以分多次开发，不要求一次完成全部代码。当前所有阶段均未开始。

| 顺序与阶段 | 主要开发任务 | 阶段完成标准 | 当前状态 |
| --- | --- | --- | --- |
| 1．通信与运行基础 | 公共执行协议、处理器接口与注册；Job 双 RPC Server、System 内嵌执行器；启动与退出装配 | 真实 RPC 可以互通，入口和认证隔离正确，启动、依赖故障及退出行为经过验证 | 未开始，下一步 |
| 2．数据基础与执行器组管理 | Goose 迁移、Task 模型与仓储、Instance 状态及索引；组创建／详情／列表／删除、凭据缓存 | 表结构和约束完整；组可以正常配置和管理；真实 PostgreSQL 测试通过 | 未开始 |
| 3．普通任务成功执行链路 | 首次创建 Task、派发、接单、执行、结果回报与保存、Instance 完成；补充 Instance 详情和 Task 查询 | 一个普通任务能从触发走到成功结束，并通过管理接口查看执行事实 | 未开始 |
| 4．失败处理与重试 | 业务失败、BUSY、明确拒绝、结果不确定、重试登记；迟到 ACK／结果及重复请求处理 | 各种失败按已定规则推进，不错误覆盖新尝试，不因技术故障虚报成功 | 未开始 |
| 5．停止、超时与恢复 | 停止接口、STOPPING 收敛、远端取消；补齐各类候选查询、遗漏推进和 Leader 切换处理 | 人工停止、超时及异常中断后，数据库状态能够按设计收敛；并发竞争和资源限制经过验证 | 未开始 |
| 6．普通模块整体验收 | 完整进程 E2E、跨节点与故障测试、CI 接入、日志检查、代表性负载验证及接入文档 | 普通执行模块独立验收完成，已知限制、测试结果和后续分片工作明确 | 未开始 |

推进约定：

- 每次集中完成一个阶段，必要的依赖接线可以穿插；阶段划分用于组织交付，不要求先写临时实现再改成最终实现。
- 测试随开发同步补齐，最后集中做整体验收，不把前五阶段的验证全部推迟到阶段 6；试验桩不冒充真实执行闭环。
- 管理入口的权限资源、错误翻译和日志保护随所属功能阶段同步完成：四个组入口归阶段 2，两个执行查询入口归阶段 3，停止入口归阶段 5。
- 每阶段结束更新本表状态，并汇报已完成内容、实际验证结果、剩余问题和下一阶段；未运行或失败的验证如实记录。
- 各阶段从一开始遵循已定目标契约，不为演示增加临时协议、模式开关或开发中间态兼容分支；影响业务语义的新问题单独提出，普通实现细节按项目规范处理。
- 阶段 6 完成表示普通执行模块开发验收完成；完整执行能力正式发布仍等待分片执行完成，并满足其他已定交付条件。

以下验收矩阵按各阶段涉及的行为逐步落实，保留跨阶段的最终复核。

| 编号 | 必须验证的行为 | 验证层 |
| --- | --- | --- |
| O01 | 空库完整建表、迁移重跑、隔离回滚、组字段非空、全表唯一冲突、软删除保留字段及两域账本独立 | 真实 PostgreSQL＋迁移 CLI |
| O02 | 双节点首次启动 NOWAIT、唯一普通 Task 原子创建、重复候选、提交不明不派发 | 多连接 PostgreSQL |
| O03 | ACK／结果先后、旧 attempt、绑定冲突、BUSY 只返还一次、拒绝耗尽与重试、未知结算竞争 | 单元＋PostgreSQL＋RPC |
| O04 | 完成 LEFT JOIN 的空子事实和异常分类、条件更新零行、正常完成与停止／超时竞争 | PostgreSQL |
| O05 | STOPPING 纳入阻塞与删除保护；子取消分批、最后父终态、按 Instance／去重地址通知 | PostgreSQL＋执行器 RPC |
| O06 | 各类候选临时分页、较小 ID 晚提交、队满和恢复预算、Leader 切换不终止对等运行职责 | 单元 race＋PostgreSQL／etcd |
| O07 | 执行等待、业务处理、结果队列及保存满载；退避不占发送／保存处理能力，内存结果丢失按已定协议接续 | 单元 race＋故障集成 |
| O08 | JSON 大整数、SQL NULL 与 JSON null、非法报文、明确永久无效与技术故障区别、panic 与 Context 边界 | 协议＋PostgreSQL／执行器 |
| O09 | 七接口登录／权限、五类业务错误、只读投影无 N+1、无凭据日志、已终态停止如实反馈 | API／RPC＋集成 |
| O10 | 真实双 Server、回报到另一节点、定向 Peer／取消、Unix／Windows 共享退出宽限与依赖顺序 | 子进程＋etcd／PostgreSQL |

详细用例以[既有验收清单](verification.md)为准，本表只做实施映射。代表性积压与历史量需用于查询计划和资源稳定性检查；未测量时不宣称吞吐已达标，量化性能目标仍须在正式压测前结合资源确认。

## 生成与 CI 覆盖核对 {#ordinary-ci-plan}

应用 HTTP／管理 RPC 继续使用固定 goctl 1.10.2 命令，公共执行 Proto 使用已批准的 protoc 命令，详见[生成规范](../../development/code-generation.md)。源契约、生成物、手写适配与测试同批核对，禁止手改生成文件；现有 CI 的 API validate 不能代替重新生成后检查差异。

| 现有 CI | 对新增代码的覆盖 | 本次必须补上的部分 |
| --- | --- | --- |
| Linux 全仓普通测试、race 和覆盖率 | 会包含新增 pkg/jobexecutor 等普通测试 | 编写真正覆盖并发交接与资源生命周期的测试，不把空目录纳入命令当成已有覆盖 |
| Windows 全仓普通测试 | 会包含新增普通测试，但没有 integration 标签 | 显式加入 Windows 宿主退出用例，不从现有通过结果推定双 Server 已验证 |
| integration 命令含 Job internal、RPC internal 和 pkg | 可运行放在这些路径的 PostgreSQL／etcd 集成测试 | 新增 Task 执行与并发场景；若新建独立 E2E 目录，必须同步加入执行命令 |
| 现有 System E2E | 验证已有认证／RBAC，当前只构建 System 两进程 | 新增 Job 执行进程闭环：job-rpc、System 内嵌执行器、隔离配置与真实依赖，覆盖跨节点回报、停止及退出 |

以现有工作流提供的真实 PostgreSQL／etcd 环境执行集成验收，测试连接和 Schema 隔离继续复用公共测试工具；不假定用户本地已运行这些服务，也不连接共享业务库。处理器验收使用可核验的真实 Go 调用及测试业务数据，不把讨论中的示例名称当作授权新增数据清理业务。

测试结果按实际运行记录：未运行、未被命令匹配或缺少环境都不能写成通过。integration 标签任务同时包含普通测试，不称为纯集成测试独立贡献；覆盖率使用本次实际数据，不用历史本地数字替代。

## 开工结论与下一步 {#ordinary-review-conclusion}

本次静态审计未发现需要另起架构提案的普通执行开工阻塞。未实现不等于未设计；Proto 字段号、SQL 适配、表结构迁移和 Go 内部组织在上述范围内落实。分片生成及完整管理功能仍归后续模块，不重新变成普通编码的前置条件。

六阶段开发安排已经确认并记录，下一步从阶段 1“通信与运行基础”开始，后续按阶段交付和验收。接口与持久化实现映射随对应阶段核对，不增加逐字段审批。本次仅记录计划，尚未生成协议、修改业务代码、运行功能测试、执行业务库迁移、提交或推送。
