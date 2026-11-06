# 普通执行模块开工检查与开发计划

状态：**普通阶段 1～5 核心开发及阶段 6 适用范围验收已完成，当前达到可提交节点。** 生产实现 `7f9d32e` 的 GitHub CI 全部成功，实际覆盖率见[最新 CI 记录](#ordinary-ci-success)；本机代表性负载五轮通过，原普通执行 E2E 回归及本轮 lint 通过，详见[收尾记录](#ordinary-load-closeout)与[负载结果](ordinary-load-validation.md)。新增负载测试和文档尚未提交，没有该增量的新 CI，提交后仍须远端验证。已批准的 [info 日志临时覆盖限制及上游跟踪](#ordinary-shutdown-race-workaround)保持；本结论不是完整第一版发布，分片与 System 生产处理器未实施。

审计结论：已批准的普通执行主路径、错误及恢复规则、最小管理契约足以支持实施；未发现需要重新选择产品行为的开工阻塞。Task 持久化、公共执行运行组件和生产进程双 Server 装配仍按下述阶段实施；协议及通信边界已开始落地。

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
| Task 持久化 | job_task 公共表、Model、约束、普通首次插入、ACK 绑定、单条结果保存、重试、自动未知结算、停止子收敛及执行查询已接入宿主 | 分片生成与阶段推进仍待实施；不带软删除、owner 或独立 Attempt 表 |
| STOPPING | Model、活跃保护、人工／普通超时决定、子收敛、通知及 Leader 恢复已接通，已定进程及故障场景通过本机与 CI 验证 | 不承诺远端业务必然停止；普通成功链路的[代表性负载](ordinary-load-validation.md)不代替停止洪峰验证 |
| 创建后本地提示 | Creator 的 WithInstanceHint 已连接 ServiceContext 中的真实派发 Runtime，确认提交后非阻塞提示本节点 | 提示不授予执行权，不在创建回调内同步执行 RPC；队满或提示丢失仍由 WAITING 补查发现 |
| 可派发补查 | WAITING 与到期重试共用有界供给；无绑定观察独立运行，Leader RUNNING／STOPPING 发现已接通 | 各类候选保持各自游标、预算和处理职责，不把可派发补查等同于全部恢复能力 |
| 组接入 | 完整字段、唯一约束、凭据缓存、四个管理入口及按组复用的派发客户端已实现，已删组连接随成功刷新清理 | 完整进程及已定跨节点故障已有[最新 CI 证据](#ordinary-ci-success)，普通整体[代表性负载](ordinary-load-validation.md)已通过 |
| 执行通信 | Job Main 已装配管理与回报双 Server，真实 Queue／Saver 已接入；System 已内嵌独立执行器 Server，完整进程场景已通过本机与 CI 验证 | 保持共享协议、独立监听入口、认证与日志边界、组缓存及连接生命周期；不由功能验证推导负载成绩 |
| 执行器运行 | 公共 Runtime 已执行四阶段处理器，具备活动防重、取消、有界业务处理与独立有限回报；System 配置和依赖注入已接线，生产注册表当前为空 | 业务处理器按明确需求注册；不将四阶段调用能力当作分片生成、存储及推进已实现 |
| 宿主退出 | 两域 Main 已使用统一宿主；Job 的 Stop／Drain／Wait／Close 已覆盖结果队列和双 Server，共享截止且实际工作结束后才清理依赖 | 明确实际停服和后台完成信号，共享停机宽限，依赖与全局日志按真实完成顺序关闭 |

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

试验失败先修实现；若证据表明已批保证在当前依赖下无法达成，再提出具体差异和取舍。已完成的通信与生命周期验证见[阶段 1 实施记录](#ordinary-stage1-progress)；生产进程与真实依赖的完整退出验收仍待完成，不宣称框架天然满足目标。

## 分阶段开发安排与验收矩阵 {#ordinary-implementation-order}

**以下六阶段开发安排于 2026-11-04 确认，替代此前五步实施顺序。** 按阶段推进，每阶段形成可验证成果，可以分多次开发。当前普通阶段 1～5 核心实现已完成，各次专项验证见实施记录。阶段 5 已接通无绑定观察、人工停止、数据库取消与通知、普通自动超时及 Leader RUNNING／STOPPING 共用周期发现；阶段 6 适用范围验收已完成，HTTP 执行与跨宿主回报见[首个阶段 6 记录](#ordinary-process-e2e-progress)，Windows 信号退出与调度端骤停见[进程生命周期记录](#ordinary-process-lifecycle-progress)，退出预算耗尽及已 ACK 排队结果见[结果丢失记录](#ordinary-result-loss-progress)，执行器骤停与 etcd 恢复及本机检查见[收尾记录](#ordinary-local-checkpoint)，首次远端失败及测试修正见[历史记录](#ordinary-ci-correction)，`7f9d32e` 的全部 CI 与有效覆盖率见[最新记录](#ordinary-ci-success)。本机[代表性负载](ordinary-load-validation.md)和受影响回归通过，当前可提交测试／文档增量；该增量尚未提交、没有新 CI，见[收尾记录](#ordinary-load-closeout)。

| 顺序与阶段 | 主要开发任务 | 阶段完成标准 | 当前状态 |
| --- | --- | --- | --- |
| 1．通信与运行基础 | 公共执行协议、处理器接口与注册；Job 双 RPC Server、System 内嵌执行器；启动与退出装配 | 真实 RPC 可以互通，入口和认证隔离正确，启动、依赖故障及退出行为经过验证 | 已完成基础实现与专项验证；完整进程及跨节点故障验收归阶段 6 |
| 2．数据基础与执行器组管理 | Goose 迁移、Task 模型与仓储、Instance 状态及索引；组创建／详情／列表／删除、凭据缓存 | 表结构和约束完整；组可以正常配置和管理；真实 PostgreSQL 测试通过 | 已完成基础实现与真实 PostgreSQL 专项验证；后续业务增量随所属阶段扩展 |
| 3．普通任务成功执行链路 | 首次创建 Task、派发、接单、执行、结果回报与保存、Instance 完成；补充 Instance 详情和 Task 查询 | 一个普通任务能从触发走到成功结束，并通过管理接口查看执行事实 | 已完成；真实生产组件链路验证成功／最终失败、父终态与管理查询，范围见最新记录 |
| 4．失败处理与重试 | 业务失败、BUSY、明确拒绝、结果不确定、重试登记；迟到 ACK／结果及重复请求处理 | 各种失败按已定规则推进，不错误覆盖新尝试，不因技术故障虚报成功 | 已完成核心逻辑及专项验证；未知结算的观察扫描／生产调用已在阶段 5 接入 |
| 5．停止、超时与恢复 | 停止接口、STOPPING 收敛、远端取消；补齐各类候选查询、遗漏推进和 Leader 切换处理 | 人工停止、超时及异常中断后，数据库状态能够按设计收敛；并发竞争和资源限制经过验证 | 普通模式核心已完成；自动观察、人工／超时停止、后台收敛与通知、Leader 统一发现已接通，实际专项验证见最新记录 |
| 6．普通模块整体验收 | 完整进程 E2E、跨节点与故障测试、CI 接入、日志检查、代表性负载验证及接入文档 | 普通执行模块独立验收完成，已知限制、测试结果和后续分片工作明确 | 适用范围验收完成；生产实现 `7f9d32e` 的[全部 CI 与覆盖率](#ordinary-ci-success)已有证据，本机[负载与相关回归](#ordinary-load-closeout)通过。新测试／文档增量可提交，尚无该增量的新 CI；上游临时覆盖限制保持，不代表完整第一版发布 |

推进约定：

- 每次集中完成一个阶段，必要的依赖接线可以穿插；阶段划分用于组织交付，不要求先写临时实现再改成最终实现。
- 测试随开发同步补齐，最后集中做整体验收，不把前五阶段的验证全部推迟到阶段 6；试验桩不冒充真实执行闭环。
- 管理入口的权限资源、错误翻译和日志保护随所属功能阶段同步完成：四个组入口归阶段 2，两个执行查询入口归阶段 3，停止入口归阶段 5。
- 每阶段结束更新本表状态，并汇报已完成内容、实际验证结果、剩余问题和下一阶段；未运行或失败的验证如实记录。
- 各阶段从一开始遵循已定目标契约，不为演示增加临时协议、模式开关或开发中间态兼容分支；影响业务语义的新问题单独提出，普通实现细节按项目规范处理。
- 阶段 6 完成表示普通执行模块开发验收完成；完整执行能力正式发布仍等待分片执行完成，并满足其他已定交付条件。

以下实施记录保留各次增量当时的成果、验证和剩余项；当前状态以本页顶部、阶段表、[最新 CI 记录](#ordinary-ci-success)及[负载收尾记录](#ordinary-load-closeout)为准。此前“尚未实现”“未提交或推送”等描述保留其历史时点，不覆盖后续成果。

### 本机收尾与提交节点（2026-11-05） {#ordinary-submission-checkpoint}

本节保留 2026-11-05 的本机提交安排；此后提交与远端结果见[最新 CI 记录](#ordinary-ci-success)，当前工作已推进至[负载验收后的可提交节点](#ordinary-load-closeout)。提交代码与阶段 6 验收完成是两个不同节点。不能把只有推送后才能获得的 GitHub 结果作为首次提交的前置条件；本机交叉编译也不能代替 Linux 实际运行。当前本机故障专项、最终回归及提交前检查已完成，达到可提交节点，证据见[最新记录](#ordinary-local-checkpoint)。本轮未提交或推送，实际 commit／push 前仍按规范检查暂存区和待推送提交及信息。

| 顺序 | 执行位置 | 剩余工作与完成标准 |
| --- | --- | --- |
| 1．本机故障补测 | Windows 本机隔离环境 | 执行器骤停至最终超时与补强后的 etcd 实际 Leader 恢复场景均已通过最终验证。新增用例已接入 CI 的精确运行列表；具体结果见[本机收尾记录](#ordinary-local-checkpoint)，不扩展故障组合 |
| 2．本机提交检查 | Windows 本机 | 已完成最终普通／进程回归、全仓双标签 vet／lint、生成一致性、工作流、文档及差异检查，待提交内容未发现真实凭据，详见[最新记录](#ordinary-local-checkpoint)。本机已达到可提交节点；真正 commit／push 前仍分别检查完整暂存区、待推送提交与拟用信息 |
| 3．提交与推送 | 本机 Git → GitHub | 按仓库既定授权规则执行；commit 前检查完整暂存区、新增文件和提交信息，push 前独立检查待推送全部提交及信息。当前前端干净，不制造空提交。提交不是普通模块已经验收通过 |
| 4．远端验证 | GitHub Actions | 跟踪实际推送的提交，获取 Linux 全仓普通测试的 race／覆盖率、Windows 普通测试、真实依赖集成及 Linux 进程 E2E 结果；发现失败针对性修复并重新验证，不用历史 CI 代替本轮结果 |
| 5．负载与最终收口 | 负载可在本机隔离环境运行；汇总两端证据 | 按 500 条启用规则、100 条同秒到期验证普通执行；事前写清机器资源、业务耗时和目标，记录延迟、积压、内存／goroutine、数据库连接及竞争。可与远端 CI 并行，不阻塞首次推送；若产生修复或验收文档增量，再按相同提交检查流程提交。负载和 CI 结果齐备、实际问题处理完后才宣布阶段 6 完成 |

CI 当前只对全仓普通测试启用 race；integration 与 E2E 未启用 race，E2E 子进程不计入现有集成覆盖率。integration 覆盖率同时包含普通测试；Codecov 上传不是强制成功步骤，CI 绿灯不等于覆盖率已经取得。汇报时分别列出单元、integration 标签任务、合并及 patch 的实际可用数据，缺失项如实说明。

后续每次进度汇报固定说明：当前处于本机补测、提交检查、可提交、远端验证或最终验收中的哪一步；本轮完成了什么；距离可提交还差什么；下一步在哪里执行。未发现新的具体缺陷时，不再用“其余故障待完成”扩大本次收尾范围。分片／Pre／Post 完整推进、完整前端页面及具体 System 生产处理器不计入这次普通模块收尾。

### 阶段 1 实施记录（2026-11-04） {#ordinary-stage1-progress}

阶段状态：**进行中，尚未整阶段验收**。本轮先交付可以独立验证的通信基础，不用临时 ACK、空消费器或硬编码组配置接通生产入口。

| 已实现 | 代码位置与边界 |
| --- | --- |
| 公共执行协议 | `pkg/jobexecutor/protocol`：两个服务、三个 RPC、四阶段 oneof、原始 JSON 字节、标准生成绑定及共享拒绝标识 |
| 处理器契约与固定注册表 | `pkg/jobexecutor`：普通／分片／可选 Post 接口；精确名称、防重、无效处理器检查及启动后冻结 |
| 执行通信适配 | 执行器派发／取消与 Job 回报入口：先认证、再校验、再交给原生 Admission／ResultAcceptor；ACK 只在依赖确认接纳后返回；不查询 Task、不运行处理器、不伪装已落库 |
| 独立 zrpc Server 与 Client 工厂 | 执行器和回报 Server 各自注册；保留 Health、禁用原生 App／Token 鉴权、不注册反射；三个方法的双向正文日志保护，落实 3／1／2 秒方法预算 |
| 真实入站停服屏障 | `pkg/rpchost`：跟踪底层 gRPC 与实际 unary handler 完成；覆盖 zrpc 超时包装先返回、内部 handler 尚未退出的情况；调用方共享截止，到期异步强停并结束等待 |

`rpchost.Server` 只提供入站完成屏障；`rpchost.Run` 已负责进程信号、共享退出预算及最后关闭日志，最终退出仍由 Main 执行。应用队列、组缓存和数据库通过具名运行组件及宿主 Hooks 管理。zrpc 的 etcd Publisher 仍由原生进程钩子管理，停止入站 Server 不等于立即撤销注册地址；Windows 不能据此承诺立即注销。业务自行启动的后台工作仍须由运行组件提供独立完成信号。

当前真实 TCP 测试使用隔离测试进程及受控 Admission／认证／结果缓冲替身，验证协议、入口隔离、Peer 及日志边界，不冒称生产任务执行。临时双 zrpc 进程试验验证了 Windows 和 Linux／WSL 的正常排空、共享截止，以及 Linux 实际 SIGTERM；另证实 shutdown 回调反向等待 Start 会死锁。Windows 控制台信号现已通过下述真实子进程验证；正式 System／Job 配合真实数据库的进程 E2E 仍未完成验收。

本轮实际验证：

| 验证 | 结果与范围 |
| --- | --- |
| Windows 全仓 `go test -p 2 ./...` | 通过，普通测试；不包含 integration／E2E 标签 |
| 新增包 `go vet` 与 execution 的 integration 标签 vet | 通过 |
| `golangci-lint v2.13.2`（新增包，含 integration 标签） | 通过，0 issues |
| Linux 四个新增包 | Windows Go 1.27.0 以 `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -p 2` 生成测试二进制，在 WSL Debian 实际运行，`shuffle=on`、每包两轮通过 |
| 真实 etcd 发现集成 | `TestExecutionServerEtcdDiscovery` 通过；官方 etcd 3.5.21 下载校验后，由测试使用随机回环端口、临时目录自启停；验证两个发现 Key 的租约注册、派发／回报、Peer 定向取消及缺失 Key 的调用期限 |
| 生成一致性 | 固定 protoc 命令重生成两个绑定文件，哈希一致 |
| 尚未运行 | race（本机缺少 C 工具链）、真实 PostgreSQL 执行闭环、正式 System／Job 生产进程 E2E；本轮不以此前 CI 代替验证 |

当前 CI 的普通测试及包含 Job RPC internal 的 integration 命令会覆盖这些新增测试；`DOGX_TEST_ETCD_BINARY` 沿用现有规范，不新增共享 etcd 端点配置。Windows 全仓首次默认并发运行因链接资源竞争而主动中止，降为 `-p 2` 后完整通过。

**接续工作：**结合阶段 2 的真实组缓存及阶段 3 的执行／结果运行组件完成第二入口接线，继续真实依赖和完整进程验收。宿主配置、默认共享 30 秒预算及两域 Main 的统一退出已在下述续做记录中落实。在该宿主基础增量结束时，两域生产 Main 已接入统一初始化与退出协调，但仍只启动原业务／管理 Server；第二入口须接入真实队列、消费者及组缓存后启用。必要的阶段 2／3 依赖实现可以穿插，不能用临时 ACK 或始终拒收的占位实现伪装完成。

### 阶段 1 续做：生产宿主初始化与退出（2026-11-04）

两域 Main 现已使用 `rpchost.Run`，移除了无条件 `defer ServiceContext.Close()` 和 `zrpc.Stop()`。每次停止只建立一个截止时间；初始化、wrap-up、入站实际 handler、业务排空、资源释放及最后关闭日志共用剩余预算，等待超时后由 Main 退出。业务或清理函数忽略 Context 时，不将取消信号当成完成，也不继续开启依赖释放阶段。

配置沿用已有 `Shutdown.WrapUpTime`／`Shutdown.WaitTime`，默认 **1 秒／30 秒**，无需额外配置键。因为 go-zero 的原生退出类型在 Windows 上为空、Unix 默认总等待仅 5.5 秒，宿主从同一文件快照分别解析原应用配置和退出策略，再把策略同步到各 Server 的同源 ServiceConf。文件只读一次、环境变量只展开一次，支持原配置格式；解析错误不回显字段值。两份受版本控制的 RPC 示例配置已同步，本地私密配置不修改。

Job 的 `Stop` 只发出调度取消，`Wait(ctx)` 等待真实任期／后台结束，二者均不关闭 PostgreSQL／etcd；实际入站和后台完成后才调用幂等 `Close`。System 的幂等 `Close` 继续先退出私有登录配置刷新循环，再关闭数据库，宿主约束整个清理等待时间。

初始化也在信号协调内：依赖准备成功前不启动监听；初始化期间停止会向初始化 Context 发出取消信号并等待实际返回，成功返回才进入停止／清理，失败由初始化函数回滚已取得资源。现有 ServiceContext 构造函数没有 Context 参数，宿主因此限制等待时间，不宣称已经中断其数据库或 Redis 初始化；到期由进程退出。初始化未退出时不并发读取、关闭尚在装配的依赖。Run 前已经收到停止信号时不再启动服务，也不注册一个可能永远不会触发的新原生回调。Unix 原生回调注册和完成等待均不阻塞宿主控制路径；晚注册无法完成时受同一预算限制，不宣称已排空。

新增 11 个独立子进程场景，验证双 Server 与后台排空顺序、实际 handler 超时后仍存活、Drain／Close／日志关闭阻塞、启动失败与 panic、Run 前停止、初始化中停止及初始化不退出。Windows 使用隐藏私有 console 的真实 `CTRL_BREAK_EVENT`，不影响用户终端；Linux 使用真实 `SIGTERM`。Windows host 测试随机顺序两轮通过，最终版本的 Linux 11 场景实际运行通过。本轮最终 Windows 全仓 `go test -p 2 ./...`、受影响包的 `go vet` 和含 integration 标签的 `golangci-lint v2.13.2` 均通过。该验证使用受控业务依赖，不能代替正式数据库／Redis／etcd 的生产进程 E2E；race 仍因本机缺少 C 工具链而未执行。

### 阶段 2 实施：数据基础与组凭据缓存（2026-11-04） {#ordinary-stage2-progress}

阶段状态：**进行中，尚未整阶段验收**。本轮为后续真实执行链路补齐持久化和认证依赖，没有用临时接单或空消费器接通第二 RPC 入口。

| 已实现 | 边界 |
| --- | --- |
| 增量迁移 `20261104120000` | 正式组 Key／Token 非空字段、名称与 Key 全表唯一；Task 公共表、单例唯一及已批准的恢复／取消索引；Instance 活跃索引包含 STOPPING，并增加运行复核／停止候选索引。无回填、默认组或开发中间态兼容分支 |
| Task 模型与 STOPPING | Task 使用独立类型／状态，不嵌入软删除 Base；JSONB 保留大整数并区分 SQL NULL／JSON null。Instance 的 `HasActive` 纳入 STOPPING，阻塞策略和 Job 删除保护继续复用该查询 |
| 组仓储 | 普通 GORM 创建、详情、分页列表和缓存投影；唯一冲突在 Repository 转换；列表 SQL 不读取 Token；数据库故障与不存在分开表达 |
| 组凭据缓存 | 各节点启动先完整加载，随后每 60 秒串行完整替换；失败保留旧快照；同键未命中合并回源，新组无需等待周期刷新。较早回源不能覆盖较新的完整替换，Token 不写日志 |
| 认证与生命周期 | 回报认证适配使用真实缓存和恒定时间 Token 比较，技术故障保留为暂时不可用；Job ServiceContext 同时管理调度与组刷新循环，等待两者实际结束后再关闭依赖。缓存就绪不等于结果队列已就绪 |

组缓存每次数据库查询复用现有 `Scheduler.QueryTimeout`，不新增刷新配置；60 秒不是 TTL 或强制撤销期限。ID 与名称在尚未取得组配置前是不同的回源键，同键请求合并不等于全局查询限流。组管理入口的后续实现见下节；该数据基础增量未为了测试提前提供删组入口或执行器连接缓存。

真实 PostgreSQL 18.6 使用 EDB 官方二进制在独立临时目录启动，仅监听随机回环端口；数据库名以 `_test` 结尾，每项测试继续使用独立 Schema。已验证迁移 Up／Down／重跑、两域迁移账本隔离、组非空／唯一及软删除身份保留、Task 单例终态防重、JSONB 空值与精度、STOPPING 活跃判断，以及真实缓存初始化／未命中／删除刷新／初始化失败释放连接。未读取本地私密配置，未连接或迁移共享业务数据库。

本轮 Windows 全仓 `go test -p 2 ./...` 通过；Job internal／RPC internal 及 pkg 的 `-tags integration` 回归通过，包含隔离 PostgreSQL 18.6 及自启停 etcd 3.5.21 的真实集成验证，也包含普通测试。受影响 Job 包的 integration 标签 `go vet -p 2` 和 `golangci-lint v2.13.2` 通过，lint 为 0 issues。缓存并发测试覆盖完整替换、并发回源、调用者取消、失败保留和实际退出等待；race 尚未运行，本机没有 C 工具链。现有索引结构测试不等于代表性负载和实际候选查询执行计划已经验收。

### 阶段 2 续做：组管理入口与删除保护（2026-11-04）

四个 POST 入口 `/job/group/create`、`get`、`list`、`delete` 及对应管理 RPC 已实现。API 契约独立保存在 `desc/jobgroup.api`，继续从 `system.api` 生成；应用 Proto 沿用固定 goctl 命令。创建由后端安全生成 32 字符 UUIDv4 Token，组名和发现 Key 精确保留并拒绝首尾空白；无编辑、重置、启停或兼容入口。详情返回完整 Token，列表的数据库／RPC／HTTP 投影均不包含 Token。

删除在单一短事务中按组主键锁定，依次通过独立语句检查未删除 Job、原组的 WAITING／RUNNING／STOPPING Instance，然后 GORM 软删除；普通回报和触发不新增组锁，也不扫描 Task 表。Job 域迁移 `20261104140000` 增加活跃 Instance 的组索引。隔离 PostgreSQL 并发验证覆盖新增／改组与删除的两种提交顺序和回滚、停用 Job、改组后的旧轮次及 STOPPING。2 万条 Instance／2 万条 Job 合成数据的存在性查询自然使用对应索引，不通过关闭顺序扫描强制计划；该证据不代表生产吞吐验收。

System 域迁移 `20261104143000` 只登记本轮四条 API 资源，不提前登记尚未实现的三个执行管理入口，也不自动授权普通角色。生成路由沿用 JWT、SessionAuth、Authorization；权限矩阵、四类业务 subcode 和中英文翻译已补齐，原 Job 引用组失败的 subcode 保持不变。

组详情在 Job 服务端和 API 的 Job RPC 客户端均屏蔽正文。真实 TCP 测试验证普通、慢调用和失败路径，保留方法、耗时及错误诊断，完整 Token 和请求正文不进入日志；不靠关闭统计实现保护。Windows 默认开发 HTTP 监听已在测试中关闭，固定路径执行及本机防火墙规则见[测试方案](../../development/testing.md#windows-local-tests)，不改变生产监听配置。

本轮全仓普通测试（Windows 固定路径入口）通过；真实 PostgreSQL 18.6 的组 Logic／仓储／迁移测试、HTTP 权限与协议测试和生成重跑一致性通过。Job internal／RPC internal、pkg、System 迁移及 API Job RPC 客户端的 integration 标签回归已完成，包含普通测试及隔离 PostgreSQL 18.6、自启停 etcd 3.5.21 的验证。首轮发现反射禁用测试未在流发送返回 EOF 后读取最终 RPC 状态；按 gRPC 契约修正后，执行通信包的 integration 回归及该传输测试连续 10 次复跑通过，仍严格要求 Unimplemented。受影响包的 vet／lint 通过，lint 为 0 issues。Windows 包装进程的退出码传递和强制退出后的子孙进程清理已单独验证；System E2E 配置关闭默认开发监听后仅验证带 integration,e2e 标签的编译，未运行该 E2E。未运行 race 或正式生产进程 E2E，不将这些组件验证当作普通任务执行闭环。

**接续工作：**Task 仓储随实际执行事务和查询落实，避免提前生成机械 CRUD。首次启动和 ACK 绑定仓储的进展见下节；后续继续接单、执行、结果消费者及第二 RPC 入口。组客户端的创建及删除后清理随真实派发客户端同时接入；当前尚无派发连接缓存，无已有客户端需要回收，不能据此在后续接入时遗漏删除清理。

### 阶段 3 实施记录：首次启动与 ACK 绑定仓储（2026-11-04） {#ordinary-stage3-progress}

阶段状态：**进行中，尚未形成普通任务执行闭环**。本轮落实首次启动事务和单次 ACK 条件绑定，不启用生产派发或第二 RPC 入口。

| 已实现 | 职责与边界 |
| --- | --- |
| `dispatch.Registrar` | 编排普通 WAITING Instance 首次启动；先判断锁内状态再解析冻结配置，不回读当前 Job。事务整体最多 2 秒，并受调用方更早的期限限制；不持有队列、不创建协程、不发送 RPC |
| Instance 执行仓储 | 按主键单行 `FOR UPDATE NOWAIT`，锁冲突交由事务回滚后退出；校验后单独读取数据库当前时间，GORM 条件更新 Running、首次开始及整轮截止 |
| Task 首次插入与事务适配 | 父更新和唯一普通 Task 插入绑定同一事务；直接保存 Running、执行编号 1、已用次数 1，登记／首次开始／审计共用数据库时间。单例冲突不忽略，父子一起回滚 |
| ACK 绑定仓储 | 以原 Task／Instance／尝试编号、Running 及双绑定为空为条件，单次 GORM UPDATE 原子保存 UUIDv4 与直连地址；零行结束且不补查，不查询父状态，不覆盖部分或既有绑定 |

只有本次确实新建 Task 且外层事务明确提交成功，Registrar 才返回可用于本次派发的记录。事务回调成功、ORM 回填 ID 或 Context 取消均不等于提交结论；提交确认丢失返回错误和空记录，不允许查询旧 Task 补发。ACK 仓储只校验地址结构，实际地址来自同次派发 Peer 的责任仍属于调用方；尚未实现 ACK 保存阶段的有限重试与派发总预算装配，不能将一个仓储方法视为完整接单处理。

本轮全仓普通测试通过，均使用 Windows 固定路径入口。隔离 PostgreSQL 18.6 的首次启动、Task／Instance 仓储及 Job Logic 回归通过；补齐并发测试后，dispatch／repository 的 integration 标签回归随机顺序连续两轮通过，该命令也包含普通测试。两个 Registrar 使用真实独立事务竞争，验证仅提交方返回新尝试；ACK 竞争通过不同连接及 `pg_blocking_pids` 确认实际写锁等待，验证状态、尝试号、双绑定变更在前者提交后阻止迟到写入，前者回滚后允许绑定。其他验证覆盖重复候选、冻结配置、父子回滚、单例冲突、终态／部分绑定和 STOPPING 父状态；提交确认失败在 runner 边界注入，并分别核对实际提交与回滚，不声称已经模拟真实网络断连。相关 integration 标签 vet／lint 通过，最终 lint 为 0 issues；未运行 race、完整进程 E2E 或正式负载测试。数据库使用隔离测试环境，未读取私密配置或连接共享业务库，不以此前 CI 代替本轮验证。

本次首次启动和绑定增量的后续结果保存工作见下节；真实派发及 ACK 保存编排、处理器执行、运行队列、Instance 完成和管理查询仍待实现。

### 阶段 3 续做：普通单条结果保存（2026-11-04） {#ordinary-result-save-progress}

阶段状态：**继续进行中，尚未形成普通任务执行闭环**。本轮在[阶段 3 首次启动与 ACK 绑定](#ordinary-stage3-progress)基础上实现结果保存组件，不启用生产结果消费者或第二 RPC 入口。

| 已实现 | 职责与边界 |
| --- | --- |
| `internal/taskresult.Report` 与 `Outcome` | 将已认证、无凭据的原生结果类型从 RPC 适配层移到 Job 域内部；通信适配复制输入缓冲，保存组件不依赖 RPC DTO 或 gRPC Code |
| `taskresult.Saver` | 每次处理一个普通 Task 结果，共用默认 2 秒预算；核对持久化归属、原尝试、当前状态和绑定身份，再解码仍需使用的冻结配置；不读取当前 Job、不创建队列或后台协程 |
| 结果读取与条件写入仓储 | JOIN 在写事务外执行，只读取一次 Task／Instance 必要事实；随后一条 GORM 条件 UPDATE 独立提交，拒绝外层事务句柄。不显式锁父，正常可写结果为两条数据语句及写入提交；UPDATE 内一次数据库取时供结果时间与审计时间共用 |
| 保存分支与确认边界 | 成功、失败等待重试、失败耗尽各用一次 UPDATE 保存；不修改绑定、尝试编号、已用机会或父状态。只有确认提交才返回 `Applied=true`；无错误零行及明确过时结果可释放，但不证明此前已保存成功 |
| 异常内容验证 | 技术错误、提交不明和仍需使用的坏快照返回错误，要求上层保留原结果；明确无效内容才可释放。仅成功 Data 的写入遇到数据异常 SQLSTATE 且仍有预算时，独立调用 PostgreSQL `pg_input_is_valid`；只有明确 false 才证明内容不可保存，不解析数据库错误文案 |

业务失败忽略未采用的 Data；成功的 SQL NULL、JSON null 与大整数保持各自语义。父为 STOPPING 或已到期不单独否决有效子结果，结果不撤销父停止决定，也不补写 ACK 绑定。可选本地提示只携带已核实的父 ID，供后续读取数据库事实；错误或内容无效不发送提示。原生类型移位直接更新调用方，不保留开发中间态的兼容别名。

本轮包含失败保存为等待重试的状态分支，**不代表新尝试登记、重试派发或阶段 4 已完成**。有界结果队列、错误退避消费者、实际派发／处理器执行、第二入口接线、父 Instance 完成及管理查询仍未接通；阶段 1、2、3 继续进行中，阶段 4～6 未开始。

**本轮验证：**全仓普通 `go test ./...` 通过；repository／dispatch 的真实 PostgreSQL 集成回归、taskresult 全部真实 PostgreSQL 集成测试及 execution 包真实 etcd 集成均通过。结果保存验证包含并发竞争、JSONB 不可存内容，以及有效内容遇到相同 SQLSTATE 技术错误时继续保留的边界；integration 标签命令同时包含对应包的普通测试。受影响 Job internal／RPC internal 包的 integration 标签 `go vet` 通过，`golangci-lint --concurrency 2 --build-tags integration` 通过、0 issues。本轮未运行 race、完整进程 E2E 或覆盖率统计，不沿用前一增量的随机顺序测试次数或此前 CI 结论。

本次单条保存增量之后，结果队列与消费者继续落实如下；派发及 ACK 保存编排、处理器执行和父完成仍待实施。

### 阶段 3 续做：调度端结果队列与消费者（2026-11-04） {#ordinary-result-queue-progress}

阶段状态：**继续进行中，尚未形成普通任务执行闭环**。本轮在[单条 Saver](#ordinary-result-save-progress)之上实现 `taskresult.Queue` 和结果 RPC 的原生接纳适配。组件没有数据库或 RPC 连接的所有权，不自动启用第二入口，不宣称生产 Main 已装配。

| 已实现 | 职责与边界 |
| --- | --- |
| `Start`／`Accept` | 构造不接纳、不启动后台工作；Start 一次性启动固定消费者并开放接纳。Accept 不等待空位或持久化；true 才表示报告已进入本地保留，满载 false／nil 由 RPC 映射 ResourceExhausted，未启动／退出使用原生 ErrNotReady 并映射 Unavailable |
| 保留与保存并发 | `QueueConfig` 默认保留 512 条、保存并发 8；512 同时包含待首次处理、等待退避和在途，不能另加 8。每次实际 Save 返回后结束本次处理，nil error 释放记录，任何 error 保留原记录，不凭 Applied 单独推断提交 |
| 到期顺序与退避 | 新结果以接纳时刻可处理，失败以上次处理完成后加正退避；基准 1／2／4／8／16／30 秒、之后封顶 30 秒，各上下浮动 20%。已到期按最早时间、相同时稳定接纳顺序选择；退避不占保存协程，不设重试次数或 TTL，不为失败重新申请队列位置 |
| `Stop` | 与 Accept 在同一同步边界关闭接纳；先接纳者归善后，后到者拒收。立即返回，不取消已接收的保存，也不关闭仍供并发交接使用的通道；停止后不能重启 |
| `Drain`／`Wait` | 首次 Drain 使用宿主已确定的共享 Context，宽限内继续既有处理；到期取消在途并停止后续尝试，后续 Drain 不替换或延长该预算。Wait 是真实工作完成屏障；Drain 超时不代表忽略取消的 Save 已退出，不能据此提前释放依赖 |

保存工作使用独立于入站 RPC 的生命周期 Context。单条处理预算仍由 Saver 的默认 2 秒及其配置控制，同时服从剩余停机宽限；Queue 的保留条数和并发已可通过组件配置设置，**尚未接入生产 YAML／ServiceContext／Main**。重复回报不在队列层去重，仍由原尝试条件写入裁决；平台保存 panic 按失败保留，不把 panic 值或原始数据库错误写入日志。日志在队列接纳锁外记录，慢日志仍可能占用当前消费者，不将其描述为完全不阻塞。

这里的退避重试只重做原报告的结果保存，不重跑业务，不递增 `attempt_no` 或 `used_attempts`，也不替换执行器身份；失败结果是否形成后续执行候选仍由单条 Saver 按冻结配置保存。它不代表阶段 4 的新尝试登记或业务重试派发已经完成。

结果 RPC 已适配真实 Queue 接纳语义，相关测试连接真实 RPC、Queue、Saver 与 PostgreSQL；这不等于生产第二 Server 已启动。组认证与原生结果保存职责不变；派发／执行器队列、实际 RPC 派发、业务处理器执行、父 Instance 完成和管理查询仍待实现。阶段 1、2、3 继续进行中，阶段 4～6 未开始。

**本轮验证：**全仓普通 `go test ./...` 通过，最终新增的到期公平性用例定向补跑通过；`go test -tags integration -count=1 ./apps/job/internal/taskresult ./apps/job/rpc/internal/execution` 通过，包含两包普通测试、真实 PostgreSQL 及真实 etcd／RPC 联通。联测验证接收 ACK 后写库失败保留原报告，数据库恢复后自动保存，执行器身份、`attempt_no` 和 `used_attempts` 不变；队列单测覆盖满载、退避顺序、RPC Context 独立、接纳与 Stop 竞争、共享 Drain 期限及 Wait 实际退出边界。上述两包的 integration 标签 `go vet` 通过，`golangci-lint --concurrency 2 --build-tags integration` 通过、0 issues。本轮未重跑 repository／dispatch 集成，未运行 race、完整生产进程 E2E、覆盖率统计或压测；此前单条 Saver 的验证保留在其独立记录中，不替代本轮结论。

结果组件增量之后，Job 的生产配置和宿主生命周期接线继续落实如下；System 执行器接入、实际派发／ACK 保存、业务执行和父完成仍待实施。

### 阶段 1／3 续做：Job 双 Server 与结果生产接线（2026-11-04） {#ordinary-result-wiring-progress}

阶段状态：**阶段 1、2、3 继续进行中，尚未形成普通任务执行闭环**。本轮将已有组缓存、Receiver、Queue 和 Saver 接入 Job 的真实配置、ServiceContext 及 Main。Job 进程分别启动管理与结果回报两个 zrpc Server；System 执行器第二入口、实际派发和业务执行、父 Instance 完成仍未实现。

| 已实现 | 职责与边界 |
| --- | --- |
| `ExecutionRPC` | 复用原生 RpcServerConf，独立监听和 etcd 发现 Key，只注册 ResultReceiver；Auth 为 false，保留 Health、不注册反射，结果方法关闭正文日志。Job 示例管理端口为 9002、回报为 9003，两种用途不混入同一发现 Key |
| `ResultPersistence` | Capacity／Concurrency／SaveTimeout 默认 512／8／2s，必须为正值；传入实际 Queue／Saver，不新增结果 TTL、次数耗尽丢弃或兼容回退 |
| 配置归一化 | ExecutionRPC.Name 因 go-zero 原生解码要求须填写；实际 ServiceConf 整体来自顶层宿主，统一日志、监控、模式及退出配置。两入口监听与发现配置仍分别保留；碰撞校验识别字面地址及共同 etcd 端点，不解析 DNS 别名或探测不同端点是否属于同一集群 |
| 初始化与注册 | 复用一套数据库、仓储、组缓存、选主与 Cron；初始组快照成功加载后创建真实 Task 仓储／Saver／Queue／Receiver，Queue.Start 后构造成功。Main 先构造 Server，Initialize 成功后才启动，注册回调此时取得真实 Receiver，不使用空消费者或占位 ACK |
| 停止与善后 | ServiceContext.Stop 先关闭结果接纳，再取消调度／组刷新生产；已接收保存使用独立生命周期。Drain 同步把宿主共享 Context 传给 Queue，并与原运行组件并发等待，不在前者退出后才开始结果善后 |
| 实际完成与释放 | Wait 只观察运行组件及 Queue 的实际退出；Close 先 Stop，再无期限等待实际完成后释放 etcd／数据库。宿主在入站与应用 Drain 都完成、且共享预算未到期时才进入 Close；Drain 超时不授权提前关闭仍被使用的依赖 |

初始化 I/O 当前并非全部接受宿主 Context；宿主可以发出取消并限制等待时间，不能据此声称数据库连接或初始加载会立即中断。未完成初始化时不启动两个监听，也不并发释放尚在装配的依赖；超出共享退出预算后由 Main 结束进程。

本轮只装配结果接纳与保存，不安装父完成提示消费者，不启动实际派发或业务处理器。保存退避仍只重做原结果，不改变执行尝试及绑定；任务成功落库不等于父 Instance 已结束。受版本控制的示例配置已更新，已有私有配置未替用户修改；缺失或非法的新配置明确报错，不退回单入口或旧配置兼容模式。

**本轮验证：**全仓普通 `go test ./...` 通过；Job RPC 子树的普通测试与 `go test -tags integration -count=1 ./apps/job/rpc/internal/svc ./apps/job/rpc/internal/execution` 均通过，后者包含对应包普通测试、真实 PostgreSQL 的 ServiceContext／Receiver／Queue／Saver 接线，以及 execution 包真实 etcd／RPC 组件联通。新增用例验证配置容量、ACK 不等待落库、请求 Context 取消不撤回结果、Stop 拒收与善后保存、Drain 在运行组件卡住时仍及时传递共享截止，以及 Wait／Close 不把取消当作实际完成；保存未改变尝试编号、次数和绑定，父 Instance 未被推进。`apps/job/rpc/...` 的 integration 标签 `go vet` 通过，同范围 `golangci-lint --concurrency 2 --build-tags integration` 通过、0 issues。本轮验证覆盖新 Main 路径的编译、配置与组合组件，**未运行完整 Main 生产进程 E2E**，也未运行 race、覆盖率统计或压测；不将代码审计或此前宿主基础测试写成实际 Main 进程验收。临时 PostgreSQL 已正常停止，未执行业务库迁移、提交或推送。

**接续工作：**继续派发／ACK 保存编排、System 执行器接入与业务处理、父 Instance 完成及管理查询；阶段 4、5 的业务重试、停止与恢复仍按原安排实施。

### 阶段 3 续做：公共执行器 Runtime 与结果回报（2026-11-04） {#ordinary-executor-runtime-progress}

阶段状态：**阶段 1、2、3 继续进行中，尚未形成普通任务完整执行闭环；阶段 4～6 未开始**。本轮在已有通信适配和 Job 结果保存接线之上实现 `pkg/jobexecutor.Runtime` 与内部 `reporter`，由真实处理器调用生成结果并异步回报。System 内嵌生产接线、调度端实际派发／ACK 保存编排、父 Instance 完成和管理查询仍未完成。

| 已实现 | 职责与边界 |
| --- | --- |
| 业务执行与共用限制 | `RuntimeConfig` 默认业务处理并发 32、等待队列 128，普通／准备／分片／Post 共用；处理位置覆盖业务调用和完成后的结果交接等待。四阶段均调用注册的真实处理器，但不实现分片生成、持久化、阶段推进或 Post 创建 |
| 接纳、活动防重与取消 | 同步协调接纳与活动登记，活动中的同一 Task／attempt 重复请求优先返回原接纳结果；业务实际退出时删除活动，再交接结果。按 Instance 标记排队项取消并通知运行 Context，不等待业务退出、不保留长期取消标记或已完成去重缓存 |
| Context 与结果语义 | 业务独立于派发 RPC，沿用原 Instance 绝对截止；结果交接和回报独立于已取消或到期的业务 Context。业务 error／当前调用协程 panic 形成失败结果并忽略未采用的输出；成功结果保持缺省 Data、JSON null、大整数及显式空准备 Items 的区别 |
| 有界回报与有限重传 | 默认保留 256 条（待首发、退避及在途）、发送并发 16；单次 RPC 2 秒，最多 7 次应用层调用，退避 1／2／4／8／16／16 秒各上下浮动 20%。退避保留原结果但释放发送能力；重报不重执行业务、不改变执行尝试 |
| 确定无效与诊断 | 明确认证／权限失败、稳定 `common.invalid_request`、确定非法的成功输出及调用次数耗尽按契约释放并记录安全诊断／丢弃指标；不解析错误文案，不凭通用 ResourceExhausted 判断报文超大，不将非法成功输出伪装成业务失败以触发重跑 |
| 停止与实际完成 | Stop 先关新接单，已接纳业务继续交接结果；所有业务处理者实际退出后才关闭 reporter 接纳。Drain 使用宿主首个共享期限，到期取消运行及回报，Wait 等待业务与回报真正返回；取消或 Drain 超时不授权提前关闭依赖，也不另给回报宽限 |
| Post 能力校验 | Post 请求必须显式 `RequiredPost=true` 并具备 PostProcessor 能力；已注册可选 Post 不等于本轮启用。RPC 回归覆盖未启用 Post 的拒绝，普通任务仍不支持 Post |

**本轮已验证：**`pkg/jobexecutor` 普通测试通过；execution 包的 integration 全包测试通过，包含该包普通测试、真实 PostgreSQL 以及既有真实 etcd／RPC 用例。新增 `Dispatch → Runtime → reporter → Receiver → Queue → Saver → PostgreSQL` 联测使用测试处理器、真实运行／通信／保存组件及隔离数据库，注入首次回报 Unavailable，验证自动重传后结果保存且业务只执行一次。最后补充的组合背压回归随 `pkg/jobexecutor` 全包重跑通过：回报满载时已完成业务释放活动身份，但仍占处理位置等待交接；新接单最终 BUSY，取消排队重复项不撤回原已完成结果。现有普通 Saver 仍只保存普通 Task，四阶段运行能力不扩展其持久化范围。

本轮全仓普通 `go test ./...` 通过；`go vet -p 2 -tags integration ./pkg/jobexecutor/... ./apps/job/rpc/internal/execution` 通过，同范围的 integration 标签 `golangci-lint` 通过、0 issues。未运行完整 Main 生产进程 E2E、race、覆盖率统计或压测，不将组件真实链路联测描述为触发至父终态的整体验收。临时 PostgreSQL 已正常停止，检查未发现遗留 etcd／PostgreSQL 或相关测试进程；前端工作区干净，未提交或推送。

**接续工作：**System 配置／ServiceContext／Main 内嵌装配公共执行组件，完成调度端实际派发及 ACK 保存编排、父完成检查和最小执行查询；阶段 4～6 继续按原计划推进。

### 阶段 1／3 续做：System 内嵌执行器配置与宿主接线（2026-11-05） {#ordinary-system-executor-wiring-progress}

阶段状态：**阶段 1、2、3 继续进行中，阶段 4～6 未开始**。System 已在原业务 RPC 之外装配公共执行器专用 Server，复用已有 Runtime／reporter；本轮验证结果及明确未覆盖范围见下表。当前 Main 未注册生产业务处理器，未知处理器名称明确拒绝；`system.cleanup` 仅为设计示例，本轮未增加清理业务。

| 已实现 | 职责与边界 |
| --- | --- |
| 公共配置与独立入口 | `jobexecutor.Config` 复用原生 Server／ReportClient；System 增加 `JobExecutor`，示例业务端口 9001、执行器端口 9004。组名 `system.executor` 与完整发现 Key 由部署方在组管理中填写，执行器配置须精确对应，不自动生成或创建默认组 |
| 凭据与部署参数 | Token 通过组详情取得，示例采用带 YAML 引号的 `"${DOGX_JOB_EXECUTOR_TOKEN}"`，避免合法全数字 Token 被解释为 number。Runtime 配置默认业务并发／等待／结果保留／发送并发为 32／128／256／16，显式配置值须为正数；直接调用 NewRuntime 的零值默认契约不变 |
| 回报目标与冷启动边界 | ReportClient 指向 Job 执行通信 Key，禁止原生 App／Token 及 Endpoints／Target；拨号固定非阻塞，回报目标暂不可达不要求业务入口随之失败。原生首次发现与 Server 注册仍依赖 etcd，不能据此承诺 etcd 冷启动故障时可正常服务 |
| 宿主配置与注册时点 | Main 先归一化共享 Shutdown／ServiceConf，再构造两个 Server；Initialize 初始化依赖后，通过 `NewServiceContext(c, register RegisterProcessors)` 完成业务依赖注入及注册并启动 Runtime。执行器 Server 的 provider 在实际注册时才取得真实 Adapter，不能使用空接纳器或占位 ACK |
| 业务边界 | 注册回调在接单前同步执行并冻结注册表，当前 Main 传 nil。具体 System 业务确定后处理器放入 `apps/system/internal/job`，由宿主注入同域依赖；本轮不创建空包、不另建 Worker，也不跨域读取 Job 表 |
| 停止与释放 | Stop 先关新接单；Drain 立即把宿主共享期限交给 Runtime，等待中业务与结果继续善后。Wait 观察实际业务／回报退出；Close 在实际退出后依次关闭回报连接、安全刷新、策略发布和数据库。入站由宿主另行等待，Drain 到期不授权提前释放仍被使用的依赖 |
| 初始化失败 | 配置先于数据库等依赖校验；注册返回错误或 panic 也进入已拥有资源的清理。panic 保持失败语义，不以部分初始化对象开放入口；原生依赖初始化仍可能阻塞，不承诺立即响应停止信号 |
| CI 环境衔接 | System 认证／RBAC E2E 的临时配置已增加执行器组凭据、独立端口与发现配置；工作流为该步骤传入 `DOGX_TEST_ETCD_BINARY`，测试使用隔离 etcd。脚本／配置就位不等于本轮 E2E 已通过 |

**本轮实际验证结果如下。** 此前 Runtime 组件联测和历史 CI 结论不替代本轮验证。

| 验证 | 已取得结果与范围 |
| --- | --- |
| 定向普通测试 | `pkg/jobexecutor`、`pkg/rpchost`、System RPC 子树及 Job config 通过；修复全数字 Token 的 YAML 引号后，System config／svc 再次通过 |
| System 执行器真实依赖用例 | `TestSystemExecutorInitializationWithRealEtcdAndPostgres` 通过，使用隔离 PostgreSQL／etcd、注入数据库测试业务并直接调用真实 Adapter，覆盖无在线调度服务时初始化、进程 UUID／重复接单、Stop 和 Close；不是完整 `NewServiceContext` 全依赖初始化或两个 Main 进程 E2E |
| System svc 集成包 | integration 全包通过，使用隔离 PostgreSQL 18.6、Redis 7.4.2 和 etcd 3.5.21，覆盖完整 `NewServiceContext` 依赖初始化及上述 Adapter 组件用例。Redis 使用用户提供的二进制另启临时实例，不改已有部署配置 |
| Job execution 集成包 | `apps/job/rpc/internal/execution` 的 integration 全包通过；包含普通测试和该包真实依赖用例，不称为纯集成测试独立贡献 |
| 全仓普通测试 | `go test ./...` 通过 |
| 认证／RBAC 真实 Main 进程 E2E | `go test -tags integration,e2e ./apps/system/e2e` 通过，实际构建并启动 System RPC／API 二进制，使用 PostgreSQL 18.6、Redis 7.4.2 和 etcd 3.5.21。覆盖既有认证／RBAC 生产入口回归；Main 仍为空处理器注册，不能据此声明执行器业务或 Task 触发至父终态已验收 |
| 静态检查 | `go vet -p 2 -tags integration,e2e` 对 `pkg/jobexecutor/...`、`pkg/rpchost`、`apps/system/rpc/...`、`apps/system/e2e`、Job config 和 execution 包通过；同范围 `golangci-lint --build-tags integration,e2e` 通过、0 issues。最终测试断言调整后，System svc 普通测试补跑通过 |
| 尚未完成 | 完整触发至父终态进程验收、race、覆盖率和压测均未完成；CI 已同步临时执行器配置及 etcd 参数，不用工作流适配代替实际运行结论 |

本轮独立临时 Redis 和 PostgreSQL 已正常停止，相关 etcd、System E2E／API／RPC 子进程及临时监听端口已确认无残留，未修改原 Redis 配置或连接共享业务数据库。前端工作区保持干净，未提交或推送；上述验证不替代下一次 CI 运行结果。

**接续工作：**完成调度端实际派发及 ACK 保存编排、父 Instance 完成和最小执行查询；明确具体业务处理器后再注册。阶段 4～6 继续按原安排推进。

### 阶段 3 续做：一次普通派发与响应保存（2026-11-05） {#ordinary-dispatch-operation-progress}

阶段状态：**阶段 1、2、3 继续进行中，阶段 4～6 未开始**。本轮接通一次普通首次派发操作，尚不启动生产候选扫描、派发队列或工作协程；BUSY／拒绝保存重试事实不代表已经实现下一次执行登记。

| 已实现 | 职责与边界 |
| --- | --- |
| 登记与发送编排 | `internal/dispatch.Dispatcher` 只采用 Registrar 本次新建并明确提交的尝试，发送一次 RPC。登记跳过、提交不明或发送前预算耗尽均不发送，不查回旧 Task 补发；调用期间不新增后台协程或长期响应队列 |
| RPC 适配 | `rpc/internal/execution.Sender` 借用宿主提供的组客户端，使用冻结的处理器、参数及整轮截止。组查询／客户端准备服从总预算，实际接单调用才另计最多 3 秒；不创建每请求连接、不自行重试业务 |
| 响应判定 | ACCEPTED 同时要求标准 UUIDv4 与同次调用取得的合法 TCP Peer；专用 BUSY 才退款，标准认证拒绝或默认业务码加精确 Reason 才明确拒绝。未知错误、非法 ACK／Peer、组取得失败均不推断未接单，不依赖 Message 判定，不保存远端原始诊断 |
| 条件保存 | ACK 使用既有双字段绑定；BUSY 原子转等待重试、返还一次机会并按数据库时间延期约 4～6 秒；明确拒绝以当前行 `used_attempts` 和冻结策略选择等待重试或最终失败 `dispatch_rejected`，不退款、不改编号。三者都不以父 STOPPING／到期拒绝当前合法子写入 |
| 有限保存重试 | 总预算默认 10 秒，登记最多 2 秒；响应阶段最多 4 秒，最多 3 次含首次、每次 1 秒、间隔 200 毫秒。保存使用原总 Context，不继承已结束的 RPC 子 Context；每次保留同一身份、摘要和 BUSY 抖动。零行立即结束，不补查或宣称此前提交成功 |
| 确定写入错误 | 参数／事务句柄合同错误和 PostgreSQL 确定的数据、约束、SQL 定义拒绝可识别并立即结束；未知技术故障及提交不明仍有限重试，保留原因链，不解析错误文案。此处只决定是否重试短期派发响应，不改变已接纳业务结果的保留规则 |
| 提交后提示 | 只有 BUSY／拒绝本次实际更新且确认提交，才可提示父检查；零行和错误无提示。提示不是重新派发授权；已经开始的依赖必须实际返回后才释放调用方处理能力 |

**本轮实际验证如下，不沿用此前结果。**

| 验证 | 结果与范围 |
| --- | --- |
| 全仓普通测试 | `go test -p 2 ./...` 通过，Windows 继续使用固定测试二进制包装器 |
| 仓储与编排集成 | repository／dispatch／taskresult 的 integration 标签全包通过，使用隔离 PostgreSQL 18.6；覆盖重复退款保护、旧尝试、完整／部分绑定、父 STOPPING／到期、数据库求值时间、并发更新后的重试决策、行锁等待与回滚、提交不明及确定错误停止重试 |
| 通信与真实组件链路 | execution 的 integration 标签全包通过，包含 PostgreSQL／etcd 及回环 RPC 用例。首次登记到真实执行器业务、首回报临时失败后重传及结果落库已接通；分别验证 ACK 先保存和结果先保存。重复候选仅一次发送、业务仅一次执行；迟到 ACK 不覆盖已保存结果 |
| 预算与故障 | 单元测试验证总预算／保存阶段／单次写入裁剪、客户端准备不误占 RPC 的三秒预算、RPC 子 Context 与保存分离、原响应有限重试、确定拒绝停止重试、零行不补查及 Context 取消后仍等依赖实际返回 |
| 静态检查 | model／repository／dispatch／taskresult／execution 的 integration 标签 `go vet -p 2` 和 `golangci-lint --concurrency 2` 通过，lint 为 0 issues |
| 未执行项目 | 本轮未重跑 System Main 认证／RBAC E2E，未运行 race、覆盖率或压测，未完成触发至父 Instance 终态的完整进程验收。integration 标签运行同时包含普通测试，不称为纯集成测试独立贡献 |

**仍未接入：**生产候选供给、同 Instance 登记合并、默认 64 个派发协程、临时分页、组客户端缓存及成功刷新后的删除清理；本轮 provider 只是依赖边界，实际连接生命周期仍待宿主实现。主要部署参数尚未接线，目前组件采用批准默认值。父 Instance 完成检查、最小执行查询、重试登记、未知结算及停止／恢复继续按后续阶段实施。未新增生产业务处理器，不声称完整触发至父终态已经验收。

### 阶段 3 续做：首次派发候选、组客户端与生产接线（2026-11-05） {#ordinary-dispatch-runtime-progress}

阶段状态：**阶段 1、2、3 继续进行中，阶段 4～6 未开始**。本轮把普通首次派发接入 Job ServiceContext；各节点独立运行，不受 Leader 任期启停。仅消费 WAITING 候选，没有临时模式过滤，也没有提前开启尚未实现登记逻辑的 Task 重试扫描。

| 已实现 | 职责与边界 |
| --- | --- |
| WAITING 有界查询 | 每趟读取当前最大 WAITING ID，固定上界后按 ID 续扫，单页最多 128。只读取 ID，不加载快照、不取得锁、不改状态；较小 ID 迟提交由下一趟从头覆盖。复用已有 WAITING 部分索引 |
| 进程派发 Runtime | 默认 64 个长期工作协程，等待上限 256 包括查询预约、返回页面和即时提示；查询前预留位置，提示不能侵占在途页面的预约。已领取工作另外受并发约束，不新增后台发送或无界等待协程 |
| 扫描与竞争 | 默认空闲间隔 1 秒并独立抖动 ±20%；临时游标只越过已接纳或合并的原排序连续前缀，入队页面采用不同起点。连续 4 页没有本趟成功新登记则退避，确认登记提交可立即恢复读取预算，普通提示的成功不冒充扫描进展；登记返回即释放局部协调，RPC 不占登记门控 |
| 即时提示 | 复用 Creator 已有提交后 Hint；手动、自动创建确认提交后均可立即交接首次派发。提示丢失或队满时仍由持久化 WAITING 候选补查，提示本身不登记执行机会 |
| 组客户端 | 以部署 etcd 配置及数据库组 Key 构造并按组复用原生客户端，同组首次构造合并。成功全量刷新后移除并关闭已删组连接，失败刷新保留；不另起清理轮询，不等待在途 RPC 排空。旧回源或迟到构造不能恢复已删配置，未采用连接明确关闭 |
| 连接构造与预算 | 首位调用者同步等待原生构造实际返回；Context 超时不伪装为构造已结束，不把连接创建转移到无界后台协程。其他同组等待者可独立取消。原生 etcd 冷构造仍可能超出调用 Context，实际退出前不归还派发处理能力 |
| 部署配置 | Job 增加 Dispatch：Concurrency 64、WaitingCapacity 256、ScanInterval 1s、TotalTimeout 10s、RPCTimeout 3s、BusyDelay 5s。查询超时沿用现有 Scheduler.QueryTimeout；内部页大小和保存细分预算不新增配置。执行器 Dispatch 方法使用原生 Server.MethodTimeouts，缺省 3s、显式正值保留，与调度端预算配套配置；取消仍为 1s |
| 宿主退出 | Stop 关闭新提示和扫描，丢弃尚未登记的候选；已开始登记／派发保留原处理 Context。Drain 同时把共享期限交给派发及结果保存，Wait 等实际返回，随后才关闭组连接、etcd 和数据库。构造失败或 panic 清理已经取得的资源 |

本轮 WAITING 查询的真实 PostgreSQL 测试覆盖固定上界、页间状态变化和低 ID 迟提交；对 3 万条代表性记录的实际 GORM 查询执行 `EXPLAIN ANALYZE BUFFERS`，确认使用已有 WAITING 部分索引，不调整 planner 开关。这是该测试数据分布下的执行计划证据，不是吞吐或最大负载结论。

**本轮实际验证如下。**

| 验证 | 结果与范围 |
| --- | --- |
| 全仓普通测试 | `go test -p 2 ./...` 通过，采用 Windows 固定二进制包装器；Runtime、组缓存／客户端、配置和执行器方法超时用例均通过 |
| PostgreSQL 集成 | repository／dispatch／svc 的 integration 全包通过，覆盖固定上界查询、迟提交、执行计划、首次登记和宿主退出；Job Logic 的 integration 全包回归也通过 |
| 真实非 Leader 宿主 | execution 的 integration 全包通过。新增用例持有外部选主资格，让生产 Job ServiceContext 保持非 Leader；分别验证创建后 Hint 和无 Hint 的 WAITING 扫描，通过真实 etcd 发现与执行器 Adapter 接单，保存正确 UUID／Peer，只派发一次，并验证 Drain 未提前关依赖、Close 后释放连接 |
| 验证边界 | 新宿主用例使用测试接纳器，并在发现注册后预热借用连接，以隔离供给路径；不是冷启动可达性保证，也不是完整 Main 进程至业务结果和父终态 E2E。已有真实 Runtime 业务／结果落库组件联测随 execution 全包再次通过 |
| 静态检查 | `go vet -p 2 -tags integration` 对 Job 全域、`pkg/jobexecutor/...` 和 System RPC config 通过；同范围 `golangci-lint --concurrency 2 --build-tags integration` 通过、0 issues |
| 未执行项目 | 本轮未重跑 System Main 认证／RBAC E2E，未运行 race、覆盖率或压测。integration 标签测试同时包含普通用例，不表述为纯集成测试独立贡献 |

**后续工作：**父 Instance 正常完成检查与最小执行查询；阶段 4 的重试登记／供给、阶段 5 的无绑定结算和停止／超时／恢复继续按原计划。System 生产处理器尚未定义，不添加示例清理业务；完整触发至父终态、race、覆盖率及负载验收仍未完成。前端未改动，未提交或推送。

### 阶段 3 续做：父完成、提示合并与执行查询（2026-11-05） {#ordinary-completion-progress}

本轮接通普通 Task 最终结果到父 Instance 终态，并补齐两项最小执行查询。阶段 3 的功能实现已齐；本轮组件集成与阶段 6 的完整 Main 进程、跨节点故障及负载验收分别记录，不把测试处理器当作 System 已有生产业务。

| 已实现 | 职责与边界 |
| --- | --- |
| 完成事实与正常结束 | 一次 GORM LEFT JOIN 读取父与唯一普通 Task，保留关联字段空值，不加载 result_data。先解释父状态，再解读仍需使用的本轮模式；成功／最终失败才条件更新 RUNNING 父。截止已过仍可按完整结果结束，不先显式锁父；完成时间由数据库表达式取得，独立事务确认提交后才确认本次完成 |
| 判定与错误 | 已有终态、尚未完成、已有 STOPPING、本次 UPDATE 零行分别表达。缺失／取消的普通 Task 诊断异常但不补建；未知状态、必要配置不可解读及数据库错误保留技术失败。零行不补查、不推断终态或改判超时，父正常失败原因统一 execution_failed，最终摘要保留 NULL 与合法空串 |
| 本地检查队列 | 默认保留 512 个唯一 ID（含处理中）、8 个检查并发、实际开始后 2 秒预算。首提示固定合并 100ms，等待期间不续期；处理中首个新提示记录下一次最早时间，结束且未确认终态时原条目排到队尾一次。错误本身不循环重试，满载拒绝新 ID 但仍合并已有 ID，限频日志记录满载丢弃 |
| 生产接线与退出 | 结果保存及 BUSY／明确拒绝保存确认后，异步提示同一本地检查组件，各节点都可执行。Job 配置增加 InstanceCheck.Capacity／Concurrency／Timeout；Stop 关闭提示接纳，Drain 传入共享期限，Wait 等依赖实际返回，之后才能关闭数据库。停机后才提交的结果可能没有本地提示，不能把尚未实现的阶段 5 恢复称为当前保障 |
| 执行详情 | 新增 GetInstance 管理 RPC 与 POST /job/instance/get，返回身份、触发、本轮冻结配置、状态及可空执行事实。不依赖仍存在的 Job，未知配置版本／模式或零值无效快照返回技术错误，不读取当前配置回填 |
| Task 分页 | 新增 ListInstanceTasks 与 POST /job/task/list，复用 page／pageSize，最多 200 条、按 ID 升序；先只查询父存在性，不加载或解析配置，再固定执行 count 和页面查询。无父返回 job.instance_not_found，无 Task 返回空数组；列表明确排除 result_data，不增加 N+1 查询 |
| 查询索引与权限 | Job 迁移 20261105120000 增加 (instance_id, id) 全状态分页索引；完成读取继续复用父主键和普通单例索引。System 迁移 20261105180000 登记两条查询 API 资源，不自动授予普通角色。沿用 JWT／SessionAuth／Authorization，新增错误中英文映射，查询 RPC 两端屏蔽正文并保留统计 |

本轮真实 PostgreSQL 验证完成读取同语句快照、缺子／可空字段、冻结详情、截止后正常完成、空摘要、条件零行、提交错误、并发正常完成／停止状态竞争及锁等待取消。查询存在性不读取配置，即使保存的是无法解读的 JSONB 也不妨碍 Task 查询。2 万条父记录及包含已结束分片的样本上，对实际 GORM SQL 执行 EXPLAIN ANALYZE BUFFERS，验证完成和分页索引；不调整 planner 开关，不据此宣称吞吐达标。

新增真实宿主组件联测保持 Job ServiceContext 为非 Leader，启动实际执行器 Runtime、派发 Server 和结果接收 Server，经 etcd 发现与复用连接。由管理触发 Logic 创建 Instance，随即取消原调用 Context，之后经生产提示完成派发、业务、回报、保存及父终态；成功和最终失败均覆盖非空／空摘要，查询可见最终事实，手动请求重放不重复执行业务。测试使用测试处理器及已预热连接，不是 System Main／HTTP 登录权限／冷连接可达性的完整 E2E。

**本轮最终验证如下。**

| 验证 | 结果与范围 |
| --- | --- |
| 全仓普通测试 | 最终 `go test -p 2 ./...` 通过，继续使用 Windows 固定测试二进制包装器；涵盖新检查队列、宿主实际退出、查询映射／参数、可空字段、路由权限与错误翻译 |
| 真实集成 | `-tags integration` 的 Job 全域、System 迁移及 API Job RPC 客户端通过，使用隔离 PostgreSQL 18.6、自启停 etcd 3.5.21 和回环 RPC；包含上文真实普通完成链路、并发提交／锁等待、查询计划、迁移 Up／Down／重跑及日志保护 |
| 生成与静态检查 | Job Proto 与 System API 按固定 goctl 命令重跑，全部 Go 文件哈希无变化；Job 全域、System API、System 迁移及 i18n 的 integration 标签 `go vet -p 2` 和 `golangci-lint --concurrency 2` 通过，lint 为 0 issues |
| 验证限制 | integration 标签运行包含普通测试，不称为纯集成测试独立贡献。本轮未运行 race、覆盖率、正式压测、System Main 登录／RBAC E2E 或跨节点完整进程验收；不报告未取得的覆盖率或吞吐数据 |


**下一步：**阶段 4 的重试登记与供给、结果不确定处理按计划接续；停止接口、STOPPING 收敛、超时和低频复核仍归阶段 5。分片生成推进及正式负载验收尚未实现。System 生产处理器仍待业务定义，前端未改，未提交或推送。

### 阶段 4：普通重试登记与共享派发供给（2026-11-05） {#ordinary-retry-progress}

本轮把普通 Task 的等待重试状态接回真实执行，阶段 4 仍在进行中。结果不确定结算尚未完成；无绑定观察扫描、停止、超时及遗漏复核仍按阶段 5 安排实施，不把这些后续能力当作当前保障。

| 已实现 | 职责与边界 |
| --- | --- |
| 重试候选 | 一次 GORM JOIN 按固定数据库时间读取到期 Task 和仍允许执行的父 Instance；保留候选原 attempt_no，按 (next_dispatch_at, id) 临时分页，每页最多 128。只读取身份和分页字段，不读取业务内容、不提前领取、不按 Task 类型过滤 |
| 登记事务 | 父主键 NOWAIT，锁后读取数据库时间，检查 RUNNING、原起止时间和冻结普通配置；单条条件 UPDATE 同时增加 attempt_no 与 used_attempts，返回新登记事实。保留原 Task、首次开始、父截止和上次摘要，清除旧绑定、输出及终态字段；只有确认提交才获得发送许可 |
| 机会与不确定性 | used_attempts 控制机会预算，attempt_no 仅标识新尝试；明确 BUSY 返还后，零重试配置仍可登记新的尝试编号。旧候选不能跳到更新的尝试，零行不补查，回滚／提交不明／整体预算已耗尽均不发送、不退款、不补发 |
| 统一供给 | 首次 WAITING 与已有 Task 重试共用一个扫描器、默认 256 等待及 64 派发工作协程；两类独立临时游标、轮流补充，不切固定份额。每次查询前预留等待位置，连续四页无有效登记进展才退避；切换来源、普通 Hint 或旧遍历成功都不能随意恢复预算 |
| 本地竞争 | 原身份去重保持到该次派发实际返回，同父登记互斥在事务实际返回后释放，不持有到 RPC 结束。候选按父分组轮转，父锁冲突跳过未登记同父候选，并用有界短期记录防止下一页立即重抢；确认登记的其他发送不受影响 |
| 生产接线 | Job ServiceContext 装配真实重试供给，所有节点独立参与，不依赖 Leader；父已 STOPPING 或在登记时已到期只在事务结束后提示原检查组件，不在登记流程直接停止或决定超时 |

真实宿主组件测试保持 Job 节点为非 Leader，经 etcd 发现、真实执行器 Runtime、结果 RPC 和 PostgreSQL，覆盖业务失败后成功、失败耗尽、BUSY 返还后成功及明确拒绝后成功。首个派发时故意修改当前 Job，后续仍保持原参数、处理器、Task 身份和父截止；四种场景均经生产供给推进至父终态。连接在测试开始前预热，使用测试业务处理器，不代表完整 Main 进程或冷启动可达性验收。

仓储已通过真实 PostgreSQL 候选分页、过滤、计数溢出、回滚和并发谓词重检；三万条父记录样本对实际查询执行 EXPLAIN ANALYZE BUFFERS，验证重试部分索引与父主键索引，无新增索引或 planner 开关。两个独立事务运行器的重试争锁验证只有提交确认者获得新尝试，未提交结果不可见。

| 验证 | 实际结果与范围 |
| --- | --- |
| 普通回归 | 全仓 `go test -p 2 ./...` 通过，使用 Windows 固定测试二进制包装器；新增 Runtime 专项随后补验，并因随机抖动和查询失败的时序回归重复运行 10 次通过 |
| 真实集成 | repository、dispatch、execution、svc 的 integration 标签全包通过；包含 PostgreSQL 18.6 条件更新／锁等待、原候选防重、真实 BUSY 返还、两个独立登记事务竞争，以及 etcd 3.5.21 发现的非 Leader 四种重试链路 |
| 供给边界 | 验证两源共享四页预算、固定时间／复合游标、保留原尝试、登记与 RPC 分别释放、同父候选公平轮转、跨页冲突抑制、有界冲突记录及实际停机；慢第四页成功／失败都从实际返回后退避，期间确认登记的进展仍有效 |
| 静态检查 | Job 全域 integration 标签 `go vet -p 2` 通过；同范围 `golangci-lint --concurrency 2` 通过，0 issues |
| 验证限制 | integration 标签同时包含普通测试，不称为纯集成测试独立贡献。本轮未运行 race、覆盖率、正式压测、完整 Main 进程 E2E 或跨节点全链路故障验收；数据库双事务竞争不等于多宿主完整验收 |

**后续：**继续阶段 4 的结果不确定结算；阶段 5 的扫描和停止／超时恢复未实现，阶段 6 的完整进程、跨节点故障、race、覆盖率及正式压测尚待开展。前端未改，未提交或推送。

### 阶段 4 续做：未知结果结算（2026-11-05） {#ordinary-unknown-progress}

本轮完成普通无绑定尝试的未知结果结算核心，补齐阶段 4 的失败与重试逻辑。`internal/taskrecovery.Settler` 只处理调用方交入的有界候选，不启动扫描、队列或后台协程；当前 ServiceContext 尚未创建或调用它。观察到期候选发现、跨运行游标及宿主启停由阶段 5 接入，因此现阶段不能宣称丢失 ACK／结果的尝试已经自动恢复。

| 已实现 | 职责与边界 |
| --- | --- |
| 有界复核 | 每批最多 16 条，重新取得一次数据库时间，再一次 JOIN 加载 Task／父状态和必要冻结配置，不逐条补查父，不读取当前 Job、业务 Data 或旧摘要。按输入原顺序处理，保留原尝试编号和用于分页的原登记时间 |
| 前置判断 | 先排除旧身份、已结束尝试、已有任意绑定及观察未到期项，再检查父 RUNNING、有效起止时间和严格早于截止；STOPPING／已到期只提示原检查组件，待开始／时间异常安全诊断后跳过。已知其他 Task 类型暂跳过，未知类型／状态及仍需使用的坏快照返回错误，不伪装成正常无事发生 |
| 条件结算 | GORM 单条 UPDATE RETURNING 保留原身份、普通执行中、双绑定为空和观察截止谓词；CASE 使用数据库当前 used_attempts 决定等待重试或最终失败。次数及原时间不变，清输出、写新的固定英文平台诊断；耗尽时 Task 原因为 result_unknown，父正常失败仍为 execution_failed |
| 独立提交 | 每条有自己的显式短事务，拒绝外层事务，即使关闭 GORM 默认写事务也须确认提交；无父或子显式锁、无 RPC。零行不补查或发成功提示，RETURNING 不是提交证据；确认提交后才计入 Applied 并发出本地检查提示 |
| 预算与部分进展 | 默认每次数据库操作 1 秒，继承调用方剩余运行预算；每短批独立取时，默认观察期 30 秒。返回原序连续处理前缀及已确认写入数；取时／批读／必要配置失败不越过未处理项，单条已尝试写失败可计入前缀但立即停止，不重复写入，不撤销之前已确认提交的条目。取消必须等待依赖实际返回，迟到但已确认的提交仍保留其事实 |
| 与既有流程组合 | 真实 PostgreSQL 测试以持久化恢复场景提供候选，经 Settler、已有共享派发 Runtime、真实 Saver 与父检查队列推进；有预算自动登记新尝试并完成，耗尽则父失败。测试替换通信／业务边界，不冒充真实 RPC 或观察扫描验证 |

真实数据库竞争覆盖 ACK、业务结果、BUSY、其他结算、子取消、新尝试、登记时间及已用次数变化的提交／回滚；使用 `pg_blocking_pids` 确认发生行等待，验证条件和 CASE 针对等待后的当前版本重新判断。未知结算先提交后，即使尝试编号尚未增加，旧 ACK／结果也不能恢复原尝试。父预读后 STOPPING 的交错不阻断子条件结算，也不会撤销父停止决定；将重试时间设为已到期后验证下一次登记仍被 STOPPING 拦截。

| 验证 | 实际结果与范围 |
| --- | --- |
| 全仓普通测试 | `go test -p 2 ./...` 通过，使用 Windows 固定测试二进制包装器；覆盖新 Settler、仓储独立提交及扩展后的现有依赖替身 |
| 真实 PostgreSQL | repository、taskrecovery、taskresult、dispatch 的 integration 标签全包均通过，使用隔离 PostgreSQL 18.6；覆盖单条提交确认、真实并发谓词重检、迟到结果保护及上述结算至重试／父完成组合链路 |
| 静态检查 | Job 全域 integration 标签 `go vet -p 2` 和 `golangci-lint --concurrency 2` 通过，lint 为 0 issues |
| 验证限制 | integration 标签运行包含普通测试，不作为纯集成独立覆盖率。本轮未重跑 etcd／真实 RPC、Main 进程 E2E、race、覆盖率或压测；没有自动观察扫描或停止／超时恢复运行结论 |

未新增迁移、协议或生成代码；默认观察值是组件默认值，尚不是已启用的生产扫描配置。前端未改，未提交或推送。

**下一步：**进入阶段 5，先接入无绑定观察的有界扫描与宿主生命周期，再继续停止、超时和遗漏复核。分片推进、完整 Main 进程／跨节点故障及负载验收仍未完成。

### 阶段 5：无绑定观察扫描与宿主接线（2026-11-05） {#ordinary-unbound-scan-progress}

本轮将已完成的普通未知结果结算接入自动发现和 Job ServiceContext。阶段 5 仍在进行中；停止入口、STOPPING 子取消收敛、远端取消、整轮超时及 Leader 低频 Instance 复核尚未实现，不将本轮自动观察等同于全部异常恢复。已有绑定但未回报的尝试不属于此扫描，仍等待结果或后续整轮终止机制。

| 已实现 | 职责与边界 |
| --- | --- |
| 候选查询 | 一次 GORM JOIN 返回原 Task／Instance／attempt／登记时间；筛选执行中、双绑定 NULL、观察已到期且父 RUNNING／期限有效／未到期，按 `(attempt_registered_at, id)` 临时键集分页。无领取写入、显式锁、OFFSET、当前 Job 查询或类型分流 |
| 独立串行运行 | 每节点一条扫描／结算流程，不依赖 Leader；每次实际结束后默认等 10 秒、上下浮动 20%。一趟固定数据库扫描时间和观察上界，每页默认 128，短批最多 16；每短批重新取数据库时间及事实，每条独立提交 |
| 必要配置投影 | 复核查询只返回冻结版本、执行模式、重试次数和间隔四个 JSON 字段，不将业务 Params 或其他配置传回应用；保留成员原始 JSON 类型供严格校验，非对象快照按 JSON null 判为无效。大参数及错误类型均有真实数据库验证 |
| 续扫与故障 | 一次运行默认 5 秒、单次数据库操作默认 1 秒；预算包含连接等待及适用的提交。跨运行只保留上界和已处理连续前缀，丢弃页及事实缓存；读取失败不越过候选，已经尝试的写入失败可越过该条但立即结束运行，到尾等待后重新从头发现。超时不证明已回滚，也不重叠运行或补跑积累的周期 |
| 配置与接线 | `UnboundRecovery.ScanInterval`、`PageSize`、`RunTimeout`、`OperationTimeout`、`ObservationPeriod` 可部署调整；扫描器与 Settler 使用相同观察期及单操作预算。默认观察期 30 秒，省略配置取默认值，显式非正值拒绝；观察期须在节点间一致，并与派发预算协调 |
| 退出与资源 | Stop 立即取消扫描及在途调用，Wait 等调用真实退出；宿主 Drain 同时把已有共享期限交给其他消费者，不被扫描清理阻挡。Close 通过真实完成屏障后才关闭数据库，初始化失败也能清理尚未 Start 的扫描组件 |
| 已有流程接续 | 自动扫描生成重试候选后，由既有共享派发重新授权执行；耗尽时保存 Task `result_unknown`，再由原父检查器正常完成。前置发现 STOPPING／已到期仍只提示原检查器，不能把该提示描述为停止／超时已完成 |

3 万条 Instance／3 万条 Task 合成数据下，初页与续页自然使用 `idx_job_task_unbound_observation` 及父主键，不关闭顺序扫描来强制计划。这只验证当前数据分布下的索引访问与排序，不等于吞吐、WAL／清理成本或正常业务延迟已经通过正式压测。

真实 PostgreSQL 联测使用两个独立扫描组件及既有共享派发、Saver、父检查队列，验证有预算则重试完成、耗尽则父失败；不保证每次运行都形成同一行的锁竞争，确定性交错仍由仓储并发用例覆盖。另以离线 etcd 的真实 ServiceContext 启动既有记录，验证无需 Leader 的自动多页发现、过新／已绑定／父已到期排除、冻结重试间隔及连接释放。这里不替换观察候选或直接调用 Settler，不冒充完整 Main 进程验收。

| 验证 | 实际结果与范围 |
| --- | --- |
| 全仓普通测试 | Windows 固定路径包装器下 `go test -p 2 ./...` 通过；包含新扫描分页、失败前缀、预算、实际返回后等待和宿主关闭测试 |
| 真实 PostgreSQL | repository、taskrecovery、svc 的 integration 标签全包通过；最后缩减配置投影后再次验证通过。使用隔离 PostgreSQL 18.6，覆盖候选过滤、游标、索引计划、自动结算接续及约 1.6 MB Params 不随复核返回、非法 JSON 类型保留 |
| 既有执行链路回归 | dispatch、taskresult、execution 的 integration 标签全包通过，包含真实 PostgreSQL 及测试自行启停的 etcd 3.5.21／gRPC 通信和宿主派发／重试链路 |
| 静态检查 | 最终 Job 全域 integration 标签 `go vet -p 2`、`golangci-lint --concurrency 2` 通过，lint 为 0 issues；`git diff --check` 通过 |
| 验证限制 | integration 标签运行同时包含普通测试，不单列为纯集成覆盖率。本轮未运行 Main 进程 E2E、race、覆盖率或正式压测；不能据此承诺恢复时延、吞吐或已实现停止／超时处理 |

无迁移、协议或生成代码改动，前端未改，未提交或推送。**下一步：**继续阶段 5 的停止决定、STOPPING 收敛和远端取消，再接续超时与 Leader Instance 复核；完整进程、跨节点故障及正式负载验收仍归阶段 6。

### 阶段 5 续做：停止决定与有限收敛核心（2026-11-05） {#ordinary-stop-core-progress}

本轮完成 `internal/instancestop` 的人工停止决定、单批子取消与最终父状态编排，以及 `execution.Canceller` 单目标发送器。这些组件尚未接入 StopInstance API、取消工作队列、地址枚举或通知队列；STOPPING Leader 发现、自动超时决定及 Leader Instance 复核仍待实施。当前不能宣称完整停止功能已可运行，也不能把对已有超时原因的收敛支持写成已启用自动超时。

| 已实现 | 职责与边界 |
| --- | --- |
| 人工停止决定 | `RequestManual` 在短事务内按主键锁单个父记录，锁后另取数据库时间；WAITING／RUNNING 保存 STOPPING 与 `manual_stop`，已过期的 RUNNING 人工停止仍保留人工原因。已有 STOPPING／终态只返回原状态与原因，不重复写入或刷新结果；WAITING 保留空开始与截止时间，异常活动时间明确拒绝 |
| 单批数据库收敛 | `ProcessBatch` 每次重新取得父主键 NOWAIT，确认 STOPPING 后按 ID 查询当前一批未结束 Task，默认最多 256 条；普通、准备、分片和收尾统一处理。只取消状态 1／2／3，保留次数、身份、开始／登记时间及地址，清空 Data 和最早派发时间；子结果先提交形成终态时保留其结果 |
| 最终父状态 | 只有本批子查询为空才执行带 `NOT EXISTS` 的父条件更新；原因匹配才结束，`manual_stop` 转父取消、`instance_timeout` 转父失败。保留冻结原因和摘要，最终数据库时钟写结束时间。非空子批提交后只返回可继续处理，不把查询到一页或影响行数当作整轮结束 |
| 提交、预算与返回 | Controller 不持有队列、协程、连接或 RPC 客户端；调用方控制整次事务期限，每条 SQL 默认 1 秒。决定、单批及最终父状态只有确认提交后才对外返回新事实；提交不明或其他错误返回零结果并保留原因。NOWAIT 竞争结束本次处理，不读取子记录或立即重试；子 UPDATE 仍可能等待结果事务。TransactionRunner 明确拒绝外层事务句柄，避免把释放保存点当成根事务提交 |
| 单目标远端通知 | Canceller 接收已确认停止决定及一个持久化地址，以组凭据发送按 Instance 的取消。每个目标独立临时直连、一次通知内复用并在实际退出后关闭；单次 RPC 1 秒、最多 3 次含首次、间隔 1 秒、总目标预算 5 秒，继续受调用方剩余期限约束。确定永久拒绝停止，其余技术或未知失败按有限预算处理；不读写父子状态，不保证业务代码退出 |
| 数据访问 | 停止父投影不读取冻结参数或摘要，子批只取 ID；子定位与最终 `NOT EXISTS` 复用既有 `idx_job_task_unfinished_instance`。3 万条 Task 测试数据下检查实际 SQL 的索引计划，不新增迁移，也不把索引计划当作正式吞吐验收 |

`Decision.New` 只表示本次新停止决定已提交；已有停止／终态返回原事实且为 false。`BatchResult.More` 只允许调用方在独立预算下继续一批，尚未实现工作队列的四批／两秒轮转；`Completed` 只表示本次最终父转换已提交。新停止决定后的本地处理和远端通知交接仍由后续接线实现，组件不会自行启动后台工作。

| 验证 | 实际结果与范围 |
| --- | --- |
| 全仓普通测试 | Windows 固定路径包装器下 `go test -p 2 ./...` 已通过，包含停止输入／事务边界、返回事实、修正后的 Canceller 分类及真实 TCP 传输测试；不沿用上一增量结果 |
| 停止核心真实 PostgreSQL | repository 与 instancestop 的 integration 标签全包通过，分别为 29.576 秒与 1.777 秒。覆盖人工／已有超时原因收敛、NOWAIT、子结果持锁后提交／回滚竞争、字段保留、迟到响应、外层回滚和 3 万条 Task 索引计划 |
| 既有执行链路回归 | dispatch、instancecheck、taskresult、taskrecovery 的真实 PostgreSQL integration 标签回归均通过，覆盖停止核心增量对已有授权、结果保存、父完成和观察结算的影响 |
| 静态检查 | `go vet -p 2 -tags integration ./apps/job/... ./pkg/jobexecutor/...` 已通过；同范围 `golangci-lint run --concurrency 2 --build-tags integration` 已通过，0 issues；文档 `git diff --check` 通过 |
| 验证边界 | integration 标签运行同时包含普通测试，不单列纯集成覆盖率；本轮运行普通测试、真实 PostgreSQL 及 TCP 验证，未重新运行 etcd、System Main 进程 E2E、race、覆盖率或正式压测。组件与仓储验证不代表停止 API、队列、地址枚举、Leader 恢复或自动超时已经完成 |

无迁移、协议或生成文件变化，前端未改，未提交或推送。**下一步：**接入停止管理入口及有界数据库处理队列，再接入地址枚举与独立远端通知队列，并继续 STOPPING Leader 发现、自动超时决定及 Instance 复核；这些接线仍须分别验证提交后交接与宿主资源生命周期。

### 阶段 5 续做：人工停止入口、后台收敛与通知接线（2026-11-05） {#ordinary-stop-runtime-progress}

本轮把已完成的停止核心接入人工管理入口、独立后台处理和宿主生命周期，并接通 STOPPING Leader 发现。阶段 5 仍在进行中：自动超时决定及 RUNNING Instance 低频复核尚未实现。支持按已有 `instance_timeout` 原因继续收敛，不等于已经有自动产生该停止决定的入口。此前[停止核心记录](#ordinary-stop-core-progress)保留当时范围，不用本轮成果改写历史测试结论。

| 已实现 | 职责与边界 |
| --- | --- |
| 人工停止管理入口 | Job `StopInstance` RPC 和 System API 的 `POST /job/instance/stop` 已接通，沿用后台登录与权限流程。新决定只有确认提交后才分别提示数据库取消与远端通知；已有 STOPPING／终态如实返回原状态与原因，不重新通知。响应表达已保存的决定，不等待全部子状态结束或业务实际退出；提交失败不发布新停止事实 |
| 权限资源与生成契约 | 新增 System Goose 权限资源迁移 `20261105200000_add_instance_stop_api.sql`，撤销迁移同时清理对应策略；HTTP／管理 Proto 契约、生成物、手写适配及错误转换同步。Job 表与公共执行协议未因本轮新增迁移或消息；不增加版本兼容逻辑 |
| 有界数据库取消 | `instancestop.Queue` 按 Instance 合并，本地容量默认 256，包含排队与处理中；默认 2 个处理协程，每批子记录最多 256。每轮最多 4 批或经过 2 秒后不启动下一批，有进展且尚未完成才保留位置并轮到队尾。2 秒是批次间让出预算，不强行中断已开始事务；每条 SQL 仍受 1 秒预算约束。NOWAIT 冲突和技术失败结束本轮，不立即重排；后续提示或持久化 STOPPING 恢复继续处理 |
| 地址枚举与远端通知 | `ListCancelTargets` 使用一条递归 CTE，每步经既有地址索引定位下一个不同地址，最多 32 个地址；锚点、递归项、比较及排序沿用同一列类型和数据库排序规则，不加载全轮 Task，不按子状态过滤。`execution.Notifier` 独立于数据库取消：默认保留 256 个 Instance、256 个目标，1 个地址读取与 16 个发送协程；容量包含在途，读取端只额外保留当前一页，目标接纳后才推进游标，每页交接后轮转。按 Instance／去重地址通知，绝非逐 Task RPC |
| 通知预算与失败 | Instance 从本地接纳起共享默认 60 秒期限，重复提示、翻页及重试不延长；查询最多 1 秒，单目标仍由 Canceller 按 5 秒总预算、单次 RPC 1 秒及最多 3 次调用执行。查询失败只结束剩余枚举，已接纳目标继续原预算；父已终态不撤销通知。满载可以拒收，过期不再启动新目标，正在运行的调用实际返回后才归还额度。自然期限由原 Context deadline 触发，避免清理逻辑将 `DeadlineExceeded` 抢先改成 `Canceled`；日志不输出地址、原始依赖错误、请求或凭据 |
| STOPPING Leader 发现 | 当前选主任期就绪后立即进行一次有界检查，随后默认每 2 分钟串行检查；每页 256、每次扫描／投递预算 5 秒、单 SQL 1 秒。固定本趟可见最大 ID，上界与最后接纳位置跨轮继续；队满停在未接纳项之前，失败不越过未接纳项，完整遍历后从头开始新一趟。失主丢弃游标，新 Leader 从头有界检查；已接纳工作属于进程级队列，不因失主取消 |
| 恢复与通知分工 | Leader 只发现 STOPPING 父 ID 并提示数据库队列，不发远端通知；子批次、恢复扫描及只读到已有停止决定的重复请求均不重发整轮取消。不扫描全局 Task、历史终态或新建可靠通知记录。父子数据库收敛不依赖 RPC 成功，远端取消仍是尽力协作，不能保证业务代码必然退出 |
| 宿主启动与退出 | `ServiceContext` 创建并接入 `Stopping`／`CancelNotification` 部署配置，数据库处理和通知相互独立。Stop 关闭接纳并取消发现；Drain 使用宿主已有共享期限，不新开通知宽限；Wait 等查询、事务和 RPC 实际返回，之后才关闭数据库及其他共享依赖。初始化失败和正常退出沿用同一资源清理边界 |

| 验证 | 当前已取得的实际结果 |
| --- | --- |
| 真实 PostgreSQL | repository、instancestop、System migration 的完整 integration 标签包已通过，分别为 50.490 秒、5.516 秒和 8.423 秒。包含地址分页、全状态覆盖、迟到绑定、首屏／续屏递归 SQL 与实际索引计划，以及取消核心和权限迁移回归；索引计划不是正式吞吐或负载结论 |
| 真实 etcd | 新增 Leader 恢复 hook 的双节点测试通过，验证任期启动、失主退出及接任边界；不等于完整 Main 进程故障验收 |
| 执行链路与宿主集成 | 修正通知自然期限竞态后，execution 的完整 integration 标签包通过（26.957 秒），包括 Notifier、三类人工停止真实 PostgreSQL／TCP 宿主场景及既有执行／重试／结果链路；svc 的 integration 标签包通过（3.938 秒） |
| 全仓普通测试 | Windows 固定路径包装器下 `go test -p 2 -exec $runner ./...` 已通过，包含本轮队列、通知、管理入口、配置及生命周期回归，不沿用上一增量结果 |
| 生产宿主接任 | `TestServiceContextRecoversStoppingOnlyAfterLeadership` 通过（3.977 秒），使用真实 etcd／PostgreSQL 和子进程；备用不处理，取得领导权后无需 API 请求或手动 Hint 即可自动收敛预置 STOPPING，取消 RPC 次数为 0，验证恢复只负责数据库收敛 |
| 静态检查 | `go vet -p 2 -tags integration ./apps/job/... ./apps/system/api/... ./apps/system/internal/migration/... ./pkg/jobexecutor/...` 通过；同范围 `golangci-lint run --concurrency 2 --build-tags integration` 通过，0 issues；`git diff --check` 通过 |
| 验证边界 | integration 标签同时包含普通测试，不称纯集成测试独立贡献；本轮未取得 race、覆盖率或正式压测数据。完整 Main 进程 E2E、系统性跨节点故障与负载验收仍归阶段 6，不能从组件级双节点测试推定全部完成 |

本轮未迁移共享开发或生产业务库；临时 PostgreSQL 测试服务已停止，无残留 etcd 测试进程。前端未改且工作区干净，后端本轮改动未提交或推送。**下一步：**继续阶段 5 的自动超时决定与 Leader RUNNING Instance 复核，并验证正常完成／超时／人工停止竞争；阶段 6 的完整进程、故障及正式负载验收随后开展。

### 阶段 5 续做：普通自动超时与 Leader 统一复核（2026-11-05） {#ordinary-timeout-recovery-progress}

本轮完成普通模式自动超时及 RUNNING 遗漏复核，并将 RUNNING／STOPPING 发现接入同一 Leader 周期。普通阶段 1～5 核心开发完成；阶段 6 尚未开始。以下只记录本轮实现与实际验证，保留前面各次增量在当时尚未实现的边界，不改写历史结论。

| 本轮实现 | 实际行为与边界 |
| --- | --- |
| 普通完成与超时衔接 | Checker 先读取普通最终事实；完整成功／最终失败仍先尝试正常结束，即使已过截止。仅明确未完成且数据库时间到期才请求超时；读取错误、坏快照、未知状态、正常完成零行及提交不明不转超时。缺少普通 Task 或异常取消可作为明确未完成；分片模式跳过，不因缺少普通子记录裁定超时 |
| 超时决定 | `RequestTimeout` 在单父行短事务中取得锁后另取当前数据库时间；仅 RUNNING 且有效截止已到写入 STOPPING／instance_timeout，结束时间仍为空。WAITING、未到期、已有 STOPPING／终态保持，人工原因不改写。父锁内不重读 Task，接受子结果随后齐备而超时先提交，不承诺全局完成优先 |
| 提交后交接 | 只在确认新决定提交后分别提示数据库取消与独立远端通知；调用方 Context 此时取消仍执行交接。已存在 STOPPING 仅继续数据库收敛，不重发通知；失败或提交不明不发布新决定，交接失败不能撤销已保存停止事实 |
| RUNNING 候选与游标 | 只读 `job_instance` 的 ID／开始时间，条件为 RUNNING 且开始早于本趟数据库时间减 4 分钟；按 `(started_at,id)` 分页，不读 Task 或当前 Job、不按截止过滤、不加锁。每页默认 256，接纳／合并后才前移临时游标；未完沿原边界续扫，到尾后下周期新一趟，新任期从头 |
| 有界恢复交接 | 复用已有检查队列及其 512／8／2s、100ms 合并窗口。新恢复 ID 只在共享占用低于半容量时接纳，已有 ID 可合并；本周期预算内等待空间，不保留数据库连接、不派生逐项等待协程，不保证持续拥塞下恢复一定有进展 |
| 共用周期与配置 | `InstanceRecovery.Interval` 默认 2 分钟，同时驱动两条独立发现分支；新任期各执行即时首轮，忙分支丢弃本次 tick，不影响另一分支、不补跑或重叠。RUNNING 的 PageSize／RunTimeout／SQLTimeout 默认 256／5s／1s，4 分钟门槛为内部规则；STOPPING 继续使用自身预算、游标和队满结束规则。原 `Stopping.RecoveryInterval` 移至共用配置，不保留兼容字段 |
| 任期与退出 | 失主取消候选发现并丢弃任期游标，已经交接给进程队列的工作保持独立上下文。驱动等待两条分支的真实返回，不能把取消或超时当成数据库已释放；不新增 owner、分桶、时间轮或高频超时扫描 |

| 已运行验证 | 本轮证据与范围 |
| --- | --- |
| Windows 全仓普通测试 | 使用固定 `scripts/test-exec-windows.ps1` 执行入口、`go test -p 2` 串行组织测试命令，完整通过。覆盖超时条件、错误／零行不改判、恢复半容量／游标／预算及共用周期生命周期；随后补充的提交成功后 Context 取消窗口由下述最终 integration 标签命令覆盖 |
| 真实 PostgreSQL 集成 | repository 全包 50.428s、最终 instancestop 8.083s、instancecheck 0.959s 通过。验证截止后完整结果正常结束、一次超时决定和子父收敛、坏快照不改判、Task 迟到与父终态竞争；真实锁等待证明锁后数据库时钟越过截止才裁定，人工／超时与正常完成竞争保留已提交决定。最终命令也执行普通测试，包含确认提交后 Context 取消仍交接的新增回归 |
| 生产宿主与既有执行回归 | 最终 svc 5.823s、execution 29.555s 全包 integration 标签测试通过。隔离 PostgreSQL／etcd 宿主测试验证外部 Leader 占位时不发现，接任后自动处理既有 RUNNING／STOPPING；旧完整结果正常成功、近期／未到期及分片不误判，超时按组认证发送 CancelInstance，数据库各阶段分别提交 |
| 查询计划 | 3 万行历史数据下的 RUNNING 首屏与续屏使用既有开始时间部分索引；不将索引计划验证等同于正式压测或吞吐结论 |
| 静态检查 | `go vet -p 2 -tags integration ./apps/job/... ./pkg/jobexecutor/...` 通过；相同范围 golangci-lint、integration 标签及并发 2 检查通过，0 issues。本轮新增核心文件 gofmt 检查无差异、注释和自有日志英文扫描及 `git diff --check` 通过 |

integration 标签任务同时包含普通测试，不称纯集成测试独立贡献。上述宿主用例是生产组件与真实依赖联测，不是完整 Main 进程 E2E；本轮未运行 CI、race、覆盖率或正式负载验收。无新迁移、协议或生成文件改动，未迁移共享开发／生产业务库；前端未改，未提交或推送。下一步按阶段 6 进行整体验收，分片生成、阶段推进和 Post 创建不属于本轮实现。

### 阶段 6 开始：普通进程 E2E 与跨节点回报（2026-11-05） {#ordinary-process-e2e-progress}

阶段状态：**进行中，尚未完成普通模块整体验收。** 本轮把已有普通执行链路放到真实 HTTP、RPC、数据库和独立进程中验证，并补充派发宿主退出后的跨节点回报。测试使用隔离 PostgreSQL Schema、Redis 命名空间与独立 etcd；不连接或迁移共享开发／生产业务库。

| 本轮实现 | 代码位置与边界 |
| --- | --- |
| HTTP 与进程执行链路 | `apps/system/e2e/job_execution_e2e_test.go` 启动预先构建的真实 System API、System RPC、Job RPC 二进制，构建由 CI 或本地命令完成；Job 建表使用 `apps/job/cmd/migrate` 的真实 CLI。请求经过登录、API 权限、Job 管理 RPC、派发、System 执行器、回报保存与父完成，不直接调用 Job Repository 替代链路 |
| System 测试业务接入 | `apps/system/rpc/system_execution_e2e_test.go` 只在 integration／e2e 测试二进制中注入测试处理器，复用 `system.go` 的生产 `run` 与双 Server 装配，业务写入隔离 System 测试表。生产 Main 仍传 nil，没有新增配置或环境变量开关来启用测试业务，也不据此宣称已有生产处理器 |
| 七个管理入口的权限 | 四个组入口、Instance 详情／停止、Task 列表均验证匿名 401、已登录未授权 403；通过真实角色授权接口授予权限后操作，最终撤权验证拒绝。授权沿用 System 原有 JWT、会话和 RBAC |
| 组与执行管理 | 验证创建／详情／列表、组名与 Key 冲突、已关联 Job 的删除保护、清理引用后删除及已删除查询；列表不返回 Token，Task 管理列表不返回业务 Data。成功、失败后重试成功、手工停止均读取平台记录和 System 业务持久化事实，已终态停止保持既有原因 |
| 停止竞争边界 | 测试处理器配合 Context 取消并保存业务取消事实；平台父保持 `manual_stop`，子 Task 可以由数据库取消先保存为已取消，也可以由处理器失败回报先保存为最终失败。本用例不要求所有远端业务一定退出，也不覆盖不配合取消的处理器 |
| 跨节点回报 | `apps/job/rpc/internal/execution/cross_node_report_integration_test.go` 中 A、B 使用独立 ServiceContext、连接池、缓存与队列，共享隔离数据库。A 经 etcd／真实 TCP 派发并保存绑定，随后 Drain／Wait／Close，确认 A 数据库连接释放后才结束业务；执行器真实 Runtime 向 B 回报，B 验证组 Token、保存原 Task 并自动完成父，两者全程均为非 Leader |
| 回报故障测试边界 | 错误组 Token 在 B 被明确拒绝；成功结果保留原尝试、绑定和 JSON 大整数。A 的退出是同一隔离测试进程内的宿主关闭，不是单独 OS 进程被 kill；不借此声称已覆盖骤停、网络分区或全部跨节点故障 |
| 凭据日志与 CI | E2E 收集各进程日志并检查本轮后端生成的组 Token 未出现，覆盖测试收尾输出。GitHub Actions 已增加独立 `Test ordinary Job execution end to end` 步骤，构建 Job RPC／迁移 CLI／System 执行器测试二进制并运行真实 E2E；配置接入不等于远端工作流已经通过 |

| 已运行验证 | 本轮证据与范围 |
| --- | --- |
| 跨宿主回报最终复验 | 冻结代码后的 `TestCrossNodeReportCompletesAfterDispatchHostCloses` 通过，3.445s；真实 PostgreSQL、etcd、执行器与 B 回报 TCP，验证 A 宿主关闭后的独立接续。已包含失败清理的实际退出屏障及认证请求独立预算修正 |
| HTTP 进程 E2E 最终复验 | 冻结代码后的 `TestJobOrdinaryExecutionEndToEnd` 通过，8.197s；覆盖七入口权限、组管理、成功／重试／停止和业务表事实，包含最终清理与凭据日志检查 |
| 既有认证与新执行联合回归 | 此前 `apps/system/e2e` 的 integration／e2e 标签全包运行通过，17.413s，包含原认证／RBAC 与新 Job E2E；最终冻结版本的新 Job 用例另按上一行复跑，不能将此前全包运行描述为最终冻结版本的全包结果 |
| 静态检查 | 本轮指定的 Job、System RPC／E2E 和 `pkg/jobexecutor` 范围，integration／e2e 标签 `go vet` 通过；同范围 golangci-lint v2.13.2、并发 2、integration／e2e 标签检查通过，0 issues |
| 工作流与文件检查 | actionlint v1.7.7 对 `.github/workflows/ci.yml` 的语法／结构校验通过，本次禁用 shellcheck／pyflakes，不称这两项已运行。本轮四个 Go 文件 gofmt 无差异、源码中文扫描无匹配；全工作区 `git diff --check` 通过 |
| GitHub Actions | 已新增独立执行步骤，尚未推送或运行本轮 GitHub 工作流，不引用此前 CI 结果证明当前改动 |

本轮未取得 race、覆盖率或正式负载数据；真实生产 Main 的优雅信号停止、共享退出预算和依赖释放的完整进程验收仍待完成。本 E2E 使用强制进程清理，不能将清理成功写成优雅停机验证。系统性跨节点骤停、网络及依赖故障也仍需补齐；阶段 6 保持进行中，普通模块尚未完成独立验收。分片生成、阶段推进、Post 创建和生产业务处理器不在本轮实现范围。

### 阶段 6 续做：进程信号退出与调度端骤停（2026-11-05） {#ordinary-process-lifecycle-progress}

阶段状态：**继续进行中，尚未完成普通模块整体验收。** 本轮仅修改测试、CI 和文档，未改变生产行为。新增三个真实进程场景，在 Windows 使用隔离私有控制台向目标发送 `CTRL_BREAK`；正常信号退出与 `Process.Kill` 明确分开。测试继续使用隔离 PostgreSQL、Redis、etcd、真实 Job 迁移 CLI 和公开 Job 管理 RPC，不直接改写 Job 执行状态。

| 本轮实现与验证 | 实际保证及证据边界 |
| --- | --- |
| 生产 Main 空载信号退出 | `TestProductionRPCProcessSignalShutdownEndToEnd` 启动真实 Job RPC 和 System RPC 生产二进制，各自初始化依赖后发送 OS 信号；要求进程退出码为 0、原双监听端口关闭、启动时确实存在的各自 PGAPPNAME 连接归零。System Main 保持空处理器注册。本用例证明可观察的退出结果，不单凭连接消失推定 Close 调用顺序或依赖一定由 Close 主动释放 |
| 已接纳业务在信号后继续执行 | `TestSystemExecutorProcessSignalDrainEndToEnd` 使用 System 测试二进制复用生产 `run`，注入等待放行的测试处理器。确认真实业务 running 后发送 OS 信号；双入口均返回 Unavailable 且业务仍未结束后，才放行业务。要求业务成功和进程退出 0，再通过管理 RPC 确认 Task／Instance 成功，并检查双端口及连接释放。回报 ACK 只确认接纳，本用例不证明数据库提交一定先于执行器退出；也不是先完成业务再停机或生产 Main 已注册业务 |
| 调度端进程骤停与替换 | `TestJobSchedulerProcessCrashRecoveryEndToEnd` 在 Job A 已派发且 Task 绑定后 kill 真实生产进程，等待实际异常退出及原数据库连接释放；保持执行器存活，在没有 Job 进程时放行业务并确认业务完成。Job B 使用同一 etcd 发现 Key、新端口启动，真实执行器向 B 交付结果，原 Task／attempt／次数保持，父成功且业务只执行一次 |
| 结果保留的验证范围 | 骤停用例的结果在 A 退出后才产生，不覆盖“调度端已 ACK、尚未落库便被杀”造成的结果丢失，也不覆盖执行器自身死机。测试没有观察首次发送失败或统计重传次数，不能推断实际经历了几次失败／重试，更不能据此承诺内存结果可靠持久化 |
| 信号与平台隔离 | Windows 只向目标私有控制台发送 `CTRL_BREAK`，不影响测试运行器或用户控制台；Unix 实现发送 SIGTERM，但本轮尚未在 Unix 实际运行。等待超时后的强制清理不能算正常退出通过，不混用 Windows 与 Unix 的验证结论 |
| CI 接入 | GitHub Actions 新增 `Test RPC process shutdown and scheduler crash recovery` 独立步骤，运行上述三场景；已有普通 Job E2E 与认证／RBAC 步骤保留。本轮尚未推送或运行 GitHub，接入不代表远端已通过 |

| 已运行验证 | 本轮证据与范围 |
| --- | --- |
| Windows 三场景首轮 | 三个新增场景合计通过，包运行 19.837s；调度端骤停 8.90s、生产 Main 空载信号退出 4.91s、System 业务信号排空 4.88s。使用真实依赖和进程，本轮实际发送 CTRL_BREAK |
| Windows 最终全包 E2E | 冻结新增测试代码后，`apps/system/e2e` 以 integration／e2e 标签及 `-shuffle=on` 随机顺序运行通过，37.469s；覆盖原认证／RBAC、完整 Job HTTP 执行和本轮三个进程场景，共五个业务 E2E。Windows 信号 helper 在非调用模式正常跳过 |
| 静态检查 | 受影响的 System RPC／E2E 范围，`go vet -p 2 -tags integration,e2e` 通过；同范围 golangci-lint v2.13.2、并发 2、integration／e2e 标签检查通过，0 issues。actionlint v1.7.7 校验新版 `.github/workflows/ci.yml` 通过，本次禁用 shellcheck／pyflakes，不宣称这两项已运行 |
| Linux 编译与 Unix 验证限制 | `GOOS=linux GOARCH=amd64 CGO_ENABLED=0` 下，`go test -p 2 -c -tags integration,e2e ./apps/system/e2e` 交叉编译通过，未实际运行 Linux。WSL2 对 Windows 隔离回环 PostgreSQL 的连接探测被拒绝，本轮未扩大监听范围或连接共享业务库；编译及 CI 接入不能代替真实 Unix 运行证据 |

本轮验证均在退出宽限内完成，没有验证超时预算耗尽或不配合取消的业务如何退出。进程退出 0 不能替代业务／平台持久化断言；数据库连接归零也不证明应用 Close 的调用顺序。Unix 真实运行、已 ACK 未落库的调度端结果丢失、执行器骤停、网络及依赖故障、race、覆盖率和正式负载仍是阶段 6 余项，不把三个场景扩写为全量故障或完整生命周期验收。生产业务处理器和分片生命周期仍未实施，未提交或推送。

### 阶段 6 续做：退出预算耗尽与已确认结果丢失（2026-11-05） {#ordinary-result-loss-progress}

阶段状态：**继续进行中，尚未完成普通模块整体验收。** 本轮仅修改测试、公共测试工具、CI 和文档，未改变生产行为。新增一个 System 真实进程场景及两个 Job 真实进程结果场景；后者使用 Job 域自有协议测试数据，不冒充实际业务执行。

| 本轮实现与验证 | 实际保证及证据边界 |
| --- | --- |
| System 退出预算耗尽 | `TestSystemExecutorProcessShutdownDeadlineEndToEnd` 用测试二进制复用生产 `run`，注入忽略 Context 的 `stubborn` 处理器；真实派发且保存绑定后发送 OS 信号。双入口停止接单后，由业务成功执行一次数据库探测，确认依赖仍可用；4 秒共享宽限耗尽后观察实际异常退出、双端口关闭及原 PGAPPNAME 连接消失。计时包含 Windows 信号发送器启动，断言容许 3～15 秒，不承诺精确 4 秒硬时限或正常排空成功 |
| 执行器退出后的事实 | 业务记录仍为 running、调用一次；退出后仅连续观察 1 秒，确认父／子仍 RUNNING，唯一 Task、attempt_no／used_attempts、执行器身份与地址不变，没有制造结果或立刻重派。未覆盖该真实退出一直等待至最终超时的完整链路 |
| 确认结果确实仍在内存 | `TestJobResultAcknowledgementProcessRecoveryEndToEnd` 配置一个保存处理者，先通过真实 Task 主键行锁阻塞另一轮的结果 UPDATE，并用 `pg_blocking_pids` 核对准确的锁持有进程。第二条目标结果收到认证 RPC 的 ACK 后，重新确认唯一保存处理者仍被该锁阻塞且目标数据库未变；因此目标只获内存接纳，尚未开始保存。保存预算设为 1 分钟、宿主宽限为 10 秒，信号后释放锁前再次核对阻塞，避免把首条超时退避误认成持续占用 |
| 正常信号排空 | `signal_drains_accepted_result` 在两个 RPC 入口均停止接单后释放阻塞锁；Job A 正常退出后确认目标结果已保存，JSON 大整数保持。B 启动并释放外部选主占位后观察父为 SUCCESS；即使预设截止已过，已保存成功也不改判超时。本用例不限定父最终由 A 的保存后提示还是 B 的恢复推进 |
| 骤停丢失与恢复 | `crash_loses_accepted_result` 在目标仍排队时 kill Job A，观察实际异常退出及原数据库会话全部消失，确认目标仍未落库。替换 B 经自然选主和既有扫描，以 `instance_timeout` 将父置 FAILED、子置 CANCELED；Task 数量、ID、原尝试／次数、绑定身份／地址和冻结时间保持，不创建业务重试。不推断第一条在途结果是否提交 |
| 恢复测试数据与选主边界 | 测试预先创建开始时间为数据库当前时间前 10 分钟、截止为前 5 分钟的 RUNNING 父子；没有实际等待 4 分钟，也不在恢复期间改时间、直接提示 Checker 或删除 etcd 选主键。外部选主占位只控制恢复开始；A 骤停后允许其默认 15 秒租约自然过期，B 的收敛观察上限为 30 秒。取消地址是协议测试监听器，不提供业务派发 |
| 公共信号工具 | `pkg/testutil/process` 由 System E2E 和 Job execution E2E 两处实际复用，消费端只保留薄适配。Windows 校验目标私有控制台仅含目标与发送器，再发送 CTRL_BREAK，并要求发送器明确确认已收到自身信号，避免未匹配辅助测试时误报发送成功；Unix 使用 SIGTERM，实际运行证据仍限本轮 Windows |
| CI 接入 | 既有生命周期步骤增加 System 预算耗尽场景；新增 `Test acknowledged results across shutdown and crash`，独立运行 Job 两场景。既有普通 Job HTTP 和认证／RBAC E2E 保留；尚未推送或实际运行 GitHub |

| 已运行验证 | 本轮证据与范围 |
| --- | --- |
| Windows System 专项及最终回归 | 预算耗尽首轮通过，包运行 13.028s，记录的退出耗时 4.1014548s；采用公共信号工具后，最终 `apps/system/e2e` 全包 integration／e2e 标签、`-shuffle=on` 通过，45.968s，覆盖六个业务 E2E，辅助信号测试在非调用模式正常跳过 |
| Windows Job 最终专项 | 加长阻塞保存窗口并补充两处准确阻塞复核后，Job 两个结果场景以 integration／e2e 标签、`-shuffle=on` 通过，24.182s。此前 26.866s 是补强前首轮，不作为冻结版结果 |
| 静态检查 | 受影响的 Job execution、System RPC／E2E、公共 process 测试工具范围，双标签 `go vet -p 2` 及 golangci-lint v2.13.2、并发 2 通过，0 issues |
| 跨平台与工作流检查 | `GOOS=linux GOARCH=amd64 CGO_ENABLED=0` 下 Job execution 与 System E2E 测试二进制交叉编译通过，仍未实际运行 Unix。actionlint v1.7.7 检查新版 CI 通过，禁用 shellcheck／pyflakes；本轮 Go 文件 gofmt 无差异、无中文匹配，工作区 `git diff --check` 通过。工作流静态检查不代替 GitHub 执行 |

本轮没有为结果引入持久化收据或改变内存 ACK 语义。Unix 实际运行、执行器真实退出至最终超时的完整链路、其余网络／依赖故障、race、覆盖率和正式负载仍待完成。测试预设过期数据不证明四分钟门槛的实际等待时序，成功排空不意味着所有故障都不会丢结果。本轮隔离服务已停止，测试进程及监听无残留；前端工作区干净。分片与 System 生产业务处理器尚未实施，未提交或推送。

### 阶段 6 本机收尾：执行器骤停与 etcd 恢复（2026-11-05） {#ordinary-local-checkpoint}

阶段状态：**本机收尾完成，已达到可提交节点；阶段 6 未完成。** 本轮沿既定普通执行范围补齐真实等待与依赖恢复证据，不扩展分片，不改变生产恢复规则。此前增量的测试结果和限制保留原时点，新证据不倒写到历史记录。

| 本轮实现与验证 | 实际保证及证据边界 |
| --- | --- |
| 执行器骤停至最终超时 | `TestExecutorCrashEventuallyTimesOutEndToEnd` 在真实业务进入 running、Task 接单绑定已保存后 kill System 执行测试进程，Job 与 etcd 持续存活。只通过公开管理 RPC 观察，最终父 FAILED、子 CANCELED，原因均为 `instance_timeout`；原 Task ID、attempt、机会次数、执行器身份／地址及本轮时间保持，业务仅调用一次，未被写为完成，没有失联提前重派 |
| 真实时间门槛 | 保留生产 RUNNING 复核的 4 分钟 MinAge，Job 原超时为 120 秒；仅把可部署调整的扫描间隔设为 1 秒。真实等待至四分钟门槛后收敛，不修改时钟、Job 表时间或直接提示检查器。该配置验证年龄门槛及超时链路，不把所测延迟作为默认两分钟扫描间隔下的恢复 SLA |
| etcd 停止与原数据重启 | `TestJobProcessesRecoverAfterEtcdRestartEndToEnd` 先完成一轮真实业务，再停止测试独占 etcd；本次停机 18 秒，超过观测到的租约授予周期。使用原数据目录和原监听地址重启，原 Job／System 进程全程存活，四个 RPC 监听仍可用；不重建业务宿主，不使用空 etcd 替代恢复 |
| 重新注册及恢复执行 | 核对三类服务发现及调度选主均取得不属于停机前集合的新租约；按最小 CreateRevision 确认实际 Leader，旧键恢复或仅新候选参选不足以通过。自然重连的观察窗口为 60 秒，保留默认客户端退避；etcd ClusterID 不变。随后新 Job／Instance／Task 使用原执行器身份／地址成功完成一次。不声称故障期间所有新调度可用，也不把已有 RPC 连接可用等同于发现可用 |
| CI 与范围 | 新增执行器骤停专项和 etcd 恢复专项两个独立步骤，测试超时分别为 6 分钟、3 分钟；GitHub 尚未运行。本轮覆盖单测试 etcd 的停止与保留数据重启，不覆盖非对称网络分区、etcd 数据丢失或所有依赖故障组合 |

| 已运行验证 | 当前结果 |
| --- | --- |
| Windows 执行器骤停专项 | 测试通过 254.65s，包运行 257.270s；实际 kill 至最终收敛记录为 4m1.0141279s |
| Windows etcd 最终验证 | 补强实际 Leader、新租约及恢复观察窗口后，在最终 System E2E 回归中通过，场景耗时 42.59s；etcd 停止 18 秒，默认客户端自然取得新租约，同 ClusterID 恢复后新 Task 成功一次 |
| Windows 最终普通测试与进程回归 | 全仓普通测试以固定 Windows 包装器、`-p 2 -shuffle=on -count=1 ./...` 通过。System E2E 排除已冻结的四分钟专项后，以 integration／e2e 标签随机顺序全包通过，90.490s，包含七个业务场景，信号辅助测试正常跳过；API 二进制已按重新生成的 routes 重建 |
| 最终静态检查 | 全仓 `go vet -p 2 -tags integration,e2e ./...` 通过；golangci-lint v2.13.2 全仓双标签、并发 2 通过，0 issues。actionlint v1.7.7 检查新版 CI 通过，禁用 shellcheck／pyflakes；本轮三个新增／修改 Go 文件 gofmt 无差异、英文扫描通过，`git diff --check` 通过 |
| 生成一致性 | 按 README 固定的 API goctl、Job RPC goctl、公共执行 protoc 三组命令重新生成，八个生成文件中七个哈希一致；routes.go 重新生成后哈希变化，随后再次生成字节一致。当前 77 条路由与源契约吻合，Method／Path／Handler、中间件及 JWT 已核对，最终 API 进程测试使用重建二进制并通过。已核对生成器对同名组排序不保证唯一顺序，保留原生输出；首次只留有哈希，不能逐字断言那次变化仅是块顺序 |
| 待提交内容检查 | 待提交文件内容未发现真实凭据，私有运行配置、测试产物和日志未误纳入；commit 前仍检查完整暂存区、新文件和拟用提交信息，push 前独立检查目标远端待推送全部提交及信息。本次检查不代替真正提交／推送前的检查 |

本轮隔离 Redis 已正常关闭、独占 PostgreSQL 已停止；按精确路径核对测试进程为零，两个隔离服务监听均无残留。前端工作区干净，未提交或推送。当前本机收尾已完成，可以进入提交与推送节点；实际操作继续遵守授权及敏感信息检查规范。GitHub Linux 实际运行、race、覆盖率及普通任务代表性负载仍待取得证据，阶段 6 保持进行中；分片和 System 生产处理器仍属后续范围。

### 阶段 6 首次 CI 结果与测试修正（2026-11-06） {#ordinary-ci-correction}

实现提交 `e5946ab6a3f10bd67d7cbb51305c865095e325ce` 已推送；[GitHub Actions 42006034575](https://github.com/tokyolab/dogx/actions/runs/42006034575) 已结束且失败。本节补充远端证据并修正此前 RUNNING 查询计划的证据边界，不覆盖历史本地测试记录。

| 本次 GitHub 检查 | 实际结果 |
| --- | --- |
| Go lint、Windows tests | 通过 |
| Linux race | 失败。`TestInstanceRecoveryBranchesDoNotShareBackpressureOrCatchUpTicks` 的测试计数器存在无同步读写；另发现 go-zero 1.10.3 的 `proc.Setup` 写入与 `gracefulStop` 读取并发，产生上游退出 race |
| PostgreSQL 集成 | 失败于 RUNNING 候选查询的指定索引断言；成本优化器选择了其他计划，不能把必须选用某个索引作为查询正确性契约 |
| 后续 E2E 与覆盖率 | E2E 步骤及覆盖率上传因前置失败跳过；本次没有可用的全项目、单元、integration 标签或 patch 覆盖率，不引用失败日志中的零散包覆盖率 |

| 本轮项目内修正 | 本地验证与限制 |
| --- | --- |
| 恢复分支测试计数器 | 改为原子读写，保留原虚拟时间、背压与调用次数断言。`apps/job/rpc/internal/svc` 普通测试全包通过，3.621s；本机本轮未运行 race，仍需由新 CI 验证 |
| RUNNING 候选查询测试 | 分别验证索引 DDL 和真实查询契约，不要求成本优化器选择指定索引；保留分页、边界及行数检查。EXPLAIN 改用捕获的 SQL 与绑定 Vars，避免从格式化日志重建语句 |
| 绑定参数后的真实 PostgreSQL 验证 | `TestRunningCandidate` 专项 7 个顶层测试通过，包运行 5.296s。两页 EXPLAIN 根节点均为 `Actual Rows = 256`、`Loops = 1`；本机自然选择 `idx_job_instance_active_group` 加 top-N 排序，单次耗时 0.937ms／1.258ms，仅作本次计划诊断，不代表吞吐结论 |
| 相关 EXPLAIN 测试扩查 | 重试、无绑定等候选的计划测试也从日志回放含时间参数的 SQL，统一改用测试专用 SQL＋Vars 捕获；事务内查询继续使用原事务连接。最终统一 helper 后的 12 个相关顶层测试通过，包运行 11.238s，覆盖 RUNNING、重试、无绑定、停止事务、取消地址、完成与分页；受影响两包 integration 标签 vet／lint 通过，0 issues |
| go-zero 退出 race | 用户决定本轮不修改依赖；`go.mod`／`go.sum` 不变，不跳过测试、不抑制 race，保留为阶段 6 的已知阻塞 |

此前本地 RUNNING EXPLAIN 从 GORM 日志回放 SQL 时丢失时区，实际返回零行，因此旧计划不能作为目标数据量的性能证据。原分页查询及其结果断言使用真实绑定参数，仍然有效；失效的是日志回放的计划证据。上方阶段 5 历史记录中“3 万行 RUNNING 首屏与续屏使用开始时间部分索引”的结论以本节更正为准。

本轮仅修正上述两类测试问题及进度文档，不修改生产逻辑或依赖，尚未提交或推送；相关用例回归和静态检查已通过，完成提交前敏感信息检查后即可提交，由新 CI 验证项目修正、集成和 E2E。上游 race 暂不处理，不承诺新 CI 全绿。代表性负载仍待开展，阶段 6 保持进行中。

### 阶段 6 后续 CI 与退出竞态临时处理（2026-11-06） {#ordinary-shutdown-race-workaround}

上节两类项目测试修正已随提交 `0949b8fa97a52d1465a47f0541ef0131cfe2f4f8` 推送。[GitHub Actions 42145921510](https://github.com/tokyolab/dogx/actions/runs/42145921510) 的 Go lint、Windows 普通测试、PostgreSQL／Redis／etcd 集成及全部 E2E 已通过；Linux race 仍失败于 `pkg/rpchost` 的三个真实信号场景，项目恢复分支测试竞态未再出现。以上是该提交的远端结果，不代表本轮改动已通过新 CI。

独立原生 zrpc 复现已提交 [go-zero #5876](https://github.com/zeromicro/go-zero/issues/5876)。用户确认接受上游修复前暂缓“关闭 info 日志时的原生信号退出竞态”覆盖，不采用 fork、补丁或依赖替换；本轮 `pkg/rpchost` 测试配置改为 `info`，保留原 11 个生命周期场景及其真实信号、Context、排空／释放断言和 `-race`。这项临时限制不等于上游缺陷已解决，生产日志配置不变；恢复条件见[测试方案](../../development/testing.md#gozero-shutdown-race-workaround)。

同时删除 `rpchost.Run` 中重复的 `proc.Setup`：所有现有调用方已在构造各 Server 前按约定 Normalize，同源停机策略已由 `zrpc.NewServer` 应用，无需 Run 再写一次全局变量。默认 1 秒／30 秒策略和宿主共享截止时间不变。这项冗余初始化清理与上游读取缺少同步的问题分开记录，不能把删除重复写入称为修复了上游首次配置写入的竞态。

本轮正式源码验证：`pkg/rpchost`、`pkg/jobexecutor`、Job execution、System RPC config、Job RPC config 五个受影响包的 Linux race 随机顺序测试通过；同五包 Windows 普通测试通过，使用固定 `-exec` 包装器。Linux 使用 WSL Debian 13、Go 1.27.0 与外部链接的 Zig C 工具链；此次是正式源码运行，未使用临时 overlay。此前临时副本三轮生命周期通过仅作为方案实验。golangci-lint v2.13.2 对 `pkg/rpchost`（含测试）的检查通过；本轮未重跑全仓测试，提交后的 GitHub CI 及代表性负载仍待验收，阶段 6 保持进行中。

### 阶段 6 最新 CI 与负载准备（2026-11-06） {#ordinary-ci-success}

提交 [`7f9d32e8ab4597f81a9f5f84dacd7c41270969fc`](https://github.com/tokyolab/dogx/commit/7f9d32e8ab4597f81a9f5f84dacd7c41270969fc) 已推送；[GitHub Actions 42158366847](https://github.com/tokyolab/dogx/actions/runs/42158366847) 于 2026-11-06 11:55（Asia/Shanghai）结束，结论为 **success**。本节记录该提交的实际远端证据，前述失败、修正及本地验证保留各自历史时点。

| 本次 GitHub 检查 | 实际结果 |
| --- | --- |
| Go lint | 通过 |
| Windows tests | 全仓普通测试通过 |
| Linux fast checks | 全仓普通测试 `-race -shuffle=on -count=1` 及工作流检查通过；`pkg/rpchost` 通过，22.306s |
| PostgreSQL 18／Redis 7.4／etcd integration | 普通及 integration 标签测试通过 |
| 独立 E2E | 认证／RBAC、普通 Job 执行、进程退出与调度端骤停恢复、已 ACK 结果排空／骤停丢失、执行器骤停至真实年龄门槛恢复、etcd 原数据重启恢复全部通过；执行器骤停专项约 244.6s |

[同提交 Codecov](https://app.codecov.io/gh/tokyolab/dogx/commit/7f9d32e8ab4597f81a9f5f84dacd7c41270969fc) 的实际行覆盖率如下；分母沿用项目现有排除规则，不是原始 Go 语句覆盖率。

| 口径 | 覆盖率 | 覆盖行／可覆盖行 |
| --- | --- | --- |
| 全项目合并 | **91.55%** | 10,597／11,574 |
| unit | **78.94%** | 9,137／11,574 |
| integration 标记 | **91.54%** | 10,595／11,574 |
| patch | 不适用 | 本次没有新增可覆盖生产代码行；GitHub patch 检查成功，未取得数值覆盖率 |

两份上传均属于本次运行，状态为已合并的实际上传，无历史 carryforward。integration 标记包含普通测试，不是纯集成测试的独立贡献；独立 E2E 未计入这些覆盖率。不能把 patch 检查成功写成 patch 100%，也不用此前本地覆盖率替代本次数据。

本次绿灯建立在已批准的 [info 日志临时处理](#ordinary-shutdown-race-workaround)之上，不代表 go-zero 上游竞态已修复；关闭 info 日志的相关信号退出竞态覆盖继续暂缓，待官方修复后恢复。该次 CI 完成时普通模块只剩代表性负载验证，阶段 6 仍在进行；用户已批准[负载验收方案与性能门槛](ordinary-load-validation.md)，当时处于本机准备，尚无负载通过结论。负载证据及发现的问题处理完后，再更新阶段完成状态；分片、Post 完整推进和 System 生产处理器仍属后续范围。

### 阶段 6 代表性负载与本机收尾（2026-11-06） {#ordinary-load-closeout}

普通模块阶段 6 的适用范围验收完成，当前达到可提交节点。生产代码仍对应已通过全部 GitHub CI 的 `7f9d32e`；本轮仅增加或调整测试及文档，尚未提交，没有该增量的新 CI。负载场景、事前批准门槛、逐轮指标、资源观察及首次测试断言修正均见[完整负载记录](ordinary-load-validation.md)。

| 本轮验证 | 实际结果及边界 |
| --- | --- |
| 代表性负载 | 两个 Job 进程、一个 System 执行器；500 条启用规则、每 30 秒 100 条同秒到期，连续五轮。500 个 Instance、500 个普通 Task 及 500 次业务调用全部成功，400 条未来规则零触发；每轮业务开始 P95／P99 均满足事前门槛，全部父成功的可见时间约 1.004～1.006s。最终测试包 186.916s、退出码 0 |
| 测量及清理范围 | 三个宿主均有负载前、运行期和排空后的资源样本；完整指标及 NOWAIT 采样限制见负载记录。用例结束复用测试进程强制清理，不据测试包退出码 0 宣称三个宿主优雅退出；共享停机保证仍以既有专项证据为准 |
| 原普通执行回归 | 重新构建生产 Job／System RPC／System API 及不带 load 标签的 System 测试宿主，`TestJobOrdinaryExecutionEndToEnd` 的 success／retry／wait 场景通过，包运行 8.123s |
| 静态检查 | 四个受影响包以 integration／e2e／load 标签运行 lint，通过，0 issues |

成功结果可先于 ACK 绑定提交，迟到 ACK 不再改写终态，因此负载用例已修正原先强制双绑定的错误断言，接受双空或双有、拒绝半绑定；生产实现未改。初次失败的原记录已清理，不能断言已查证其具体失败字段，最终运行结果单独记录。已批准的 info 日志临时覆盖限制继续保留，不宣称上游竞态修复、长期无泄漏、竞品性能优势或完整第一版发布。下一步按授权与敏感信息检查规范提交本轮增量，并由新 CI 验证；分片／Post 完整推进及 System 生产处理器仍为后续工作。

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
| System 认证、普通 Job 执行、进程生命周期与 Job 结果故障独立 E2E 步骤 | 认证／RBAC 原步骤保留；Job 步骤构建 RPC、迁移 CLI 和 System 执行器测试二进制；生命周期步骤覆盖空载信号、业务排空、预算耗尽及 Job kill 后替换，结果步骤覆盖已 ACK 排队结果正常排空／骤停丢失及替换恢复 | 本地 Windows 结果见[本机收尾记录](#ordinary-local-checkpoint)；执行器骤停与 etcd 恢复为两个独立步骤。`7f9d32e` 的全部 E2E 已通过，见[最新 CI 记录](#ordinary-ci-success)；代表性负载与受影响回归已通过，见[收尾记录](#ordinary-load-closeout)，本轮测试／文档增量尚无新 CI |

以现有工作流提供的真实 PostgreSQL／etcd 环境执行集成验收，测试连接和 Schema 隔离继续复用公共测试工具；不假定用户本地已运行这些服务，也不连接共享业务库。处理器验收使用可核验的真实 Go 调用及测试业务数据，不把讨论中的示例名称当作授权新增数据清理业务。

测试结果按实际运行记录：未运行、未被命令匹配或缺少环境都不能写成通过。integration 标签任务同时包含普通测试，不称为纯集成测试独立贡献；覆盖率使用本次实际数据，不用历史本地数字替代。

## 开工结论与下一步 {#ordinary-review-conclusion}

本次静态审计未发现需要另起架构提案的普通执行开工阻塞。未实现不等于未设计；Proto 字段号、SQL 适配、表结构迁移和 Go 内部组织在上述范围内落实。分片生成及完整管理功能仍归后续模块，不重新变成普通编码的前置条件。

普通六阶段适用范围已完成开发与验收：生产实现 `7f9d32e` 的[全部 CI 及覆盖率](#ordinary-ci-success)已有证据，本机代表性负载、原普通执行回归及本轮静态检查见[最新收尾记录](#ordinary-load-closeout)。已批准的[退出竞态临时处理](#ordinary-shutdown-race-workaround)继续保留，上游缺陷及暂缓的 error 日志配置覆盖单独跟踪。当前达到可提交节点；新增测试／文档尚未提交，没有该增量的新 CI，提交／推送仍遵守授权和敏感信息检查规范，之后由新 CI 验证。普通里程碑验收不代表完整第一版发布；分片生命周期及 System 生产处理器仍属后续范围，未迁移共享业务库。
