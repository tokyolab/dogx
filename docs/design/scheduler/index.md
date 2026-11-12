# 分布式定时任务调度框架软件设计说明书

DogX 定时任务调度是 DogX 一体化后台管理项目的内置能力，面向需要可视化管理、集群运行和基础分片处理的本项目业务。当前不作为供任意外部项目接入的独立调度产品，也不独立发布执行器 SDK。它把“何时触发”“如何可靠推进一轮执行”和“如何运行具体业务”分开：调度服务维护计划与执行记录，业务执行器运行处理器，持久化状态支撑节点故障后的恢复。

本文说明系统的目标、架构、行为与实现约束，供架构评审、模块开发、业务接入和测试使用。正文按主题组织，章节之间通过契约关联；设计与实现进度另见[设计进度与评审计划](progress.md)。

当前开发安排：[普通六阶段计划](ordinary-readiness-review.md#ordinary-implementation-order)的适用范围验收已完成，提交 `5b07ec0` 的 [CI 42173750198](https://github.com/tokyolab/dogx/actions/runs/42173750198) 已成功。分片按[五阶段安排](sharded-readiness-review.md#sharded-implementation-order)推进：2026-11-11 已完成阶段 1～4 的实现与本机验证，包括输入及迁移、Pre 与统一派发、Post 与整轮完成，以及[阶段 4 的故障与跨节点验收](sharded-readiness-review.md#sharded-stage4-progress)。2026-11-12 已完成阶段 5 的本机查询、代表性负载及必要回归，达到本机可提交节点，实际范围与限制见[查询与负载记录](sharded-load-validation.md)。分片改动尚未提交或推送，没有本次 CI；远端 Linux／race 与覆盖率须在推送后验证，不用普通历史结果代替，也不宣称完整产品已经发布。

> 历史状态汇总（截至 2026-11-05，当前进度以前文及进度页为准）：触发闭环已实现、提交，已记录的 CI 已通过；此前已提交的触发模块交付止于 WAITING Instance 及本轮配置；本轮普通执行开发成果与验证见上方记录，不将历史 CI 结论用于本轮未提交代码。普通 Task 主路径、处理器注册及[同进程双 RPC Server 边界](architecture.md#rpc-boundaries)已通过；[处理器接口分工、数据语义和共同执行约束](data-contracts.md#processor-contract)及 [Post 职责与参数语义](data-contracts.md#post-input)已收口。普通任务不支持 Post；分片 Post 只接收公共输入和统计，不提供执行明细或查询能力，业务自行读取业务记录。[派发请求与接收 ACK](data-contracts.md#dispatch-acceptance)、[结果上报请求与接收 ACK](data-contracts.md#result-report-contract)及[取消处理与响应](data-contracts.md#cancel-response-contract)语义均已通过；[取消通知粒度及定向客户端生命周期](execution.md#cancel-routing)不变，取消请求不校验预期进程 ID。接入配置、组依赖及启动退出规则已收口；[Task 公共字段与单例唯一索引](data-contracts.md#task-field-contract)、[普通首次创建与重复推进 SQL](data-contracts.md#ordinary-start-sql)、[接单 ACK 绑定 SQL](data-contracts.md#executor-binding-sql)、[普通结果保存与状态转换](data-contracts.md#task-result-persistence)、[普通重试登记 SQL](data-contracts.md#ordinary-retry-registration)、[普通未知结算 SQL](data-contracts.md#ordinary-unknown-settlement)、[普通 BUSY 返还 SQL](data-contracts.md#ordinary-busy-settlement)及[停止超时与统一子取消 SQL](data-contracts.md#instance-termination-sql)已通过，派发工作协程默认每调度进程 64 个，[Instance 完成判定、父条件写入、摘要及超时衔接](data-contracts.md#instance-completion-contract)也已通过；[恢复资源分工](execution.md#recovery-resources)、[可派发候选查询及索引](data-contracts.md#dispatch-recovery-query)、[绑定缺失观察到期查询与索引](data-contracts.md#unbound-recovery-query)及[运行中 Instance 统一复核及查询索引](data-contracts.md#instance-recheck-query)已通过：Leader 每 2 分钟启动或续扫，筛选本趟数据库时间之前已开始超过 4 分钟的 RUNNING Instance，每页 256 条；阶段遗漏推进、正常结束及到期处理共用既有检查组件。[停止协议](data-contracts.md#instance-termination-sql)于 2026-10-23 修订通过：先持久化 `STOPPING = 6` 和停止原因，统一分批取消未结束 Task，确认全部子记录终态后再保存父终态；远端取消独立、尽力执行。STOPPING 候选由同一 Leader 每 2 分钟独立发现，不套用 4 分钟开始时间门槛，新 Leader 就绪后有界检查；使用既有后台恢复容量、独立预算和公平遍历。[STOPPING 取消竞争与容量](execution.md#stopping-processing)于 2026-10-26 收口：每批父主键 NOWAIT 后才查子 Task，竞争失败退出且不立即重排；已确定扫描、本地容量、子批及公平份额初值，不扩展到派发竞争。[远端取消通知](execution.md#cancel-notification-capacity)及[地址查询与索引](data-contracts.md#cancel-target-query)同日通过：按 Instance 向去重地址通知，不逐 Task；新停止决定确认提交后独立交接，不随子批或两分钟恢复重发，父终态不撤销已接纳通知。[派发候选合并与跨节点竞争](execution.md#dispatch-contention)同日单独通过：普通首次启动与已有 Task 新尝试登记改为父行 NOWAIT，本地合并重复候选、同 Instance 登记不自相竞争，失败跳过本次同组且不立即重排；补查独立抖动、已接纳候选分组后不同起点处理。[排队、并发与满载处理](execution.md#capacity-model)于 2026-10-28 通过：共享派发不切来源固定份额，按 Instance 小批轮转；同日补充通过六条设计原则和[执行器处理流程](execution.md#executor-processing-flow)，以有界业务等待队列、有限并发业务处理、有界结果队列及独立回报交接工作。结果队列满时处理者等待交接、不领取下一项业务；活动防重在业务实际退出时清理。各段限制共同说明积压边界，不增加接单到回报的统一计数，不提前固定 Go 并发原语。同日[派发候选容量、分页与退避](execution.md#dispatch-supply-defaults)另已通过，确认等待总量 256、查询最多 128、空闲补查 1 秒上下浮动 20%、每 Instance 先补 8 项及连续无有效登记进展最多 4 页的退避，并补齐临时分页和本轮成功登记恢复预算规则。同日另已通过[单次派发时间预算](execution.md#dispatch-time-budgets)：总处理 10 秒、登记事务 2 秒、接单 RPC 3 秒、响应保存阶段 4 秒，保存最多 3 次含首次、单次 1 秒及间隔 200 毫秒；无绑定观察 30 秒、BUSY 独立延期 5 秒上下浮动 20%，各阶段受剩余预算限制。同日通过[执行器回报安排与初值](execution.md#reporting-defaults)：回报组件默认保留 256 条结果，覆盖待首发、待重试和在途；最多 16 次 RPC 并发，单次调用结束即释放发送能力，退避继续保留原结果位置。按已到期的最早可发送时间选择，同时间稳定排序，不切首发／重试固定份额；单次 RPC 2 秒、最多 7 次应用层调用含首次，六次退避为 1／2／4／8／16／16 秒、各上下浮动 20%。基准退避合计 47 秒，不是保留 TTL；结果保留数和发送并发可配置，其余为内部常量。同日通过[执行器业务处理初值](execution.md#executor-business-defaults)：每执行器进程默认业务处理并发 32、业务等待队列 128，均为可调整的部署配置。32 包含业务运行及业务已完成但结果尚未交接完的处理，128 只计尚未领取的等待任务、不含处理中；普通、准备、分片和收尾共用，不按 Job／Instance 叠加。2026-10-29 后续通过[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)：每节点组件默认保留 512 条待首次处理、待重试和在途结果，保存并发 8，单条一次处理超时 2 秒，三项均可部署调整。每条关联查询 Task／Instance 归属、认证组及本轮冻结重试配置，再按原尝试、执行中状态及绑定身份条件 UPDATE、独立提交，不设整批总超时或超时拆批。一次处理结束即释放处理能力，退避保留原条目原位置；按已到期最早可处理时间及稳定同时间顺序选择，退避基准 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%，可恢复故障不按次数或 TTL 丢弃。[回报错误分类](data-contracts.md#result-error-classification)已通过，[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)与[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)均已通过；[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，Proto 已生成，普通派发、响应保存、父完成与最小执行查询已接入，普通重试与共享派发供给见最新记录；未知结算与自动观察扫描已接入生产组件，见[最新记录](ordinary-readiness-review.md#ordinary-unbound-scan-progress)，停止入口、有界数据库处理、地址枚举及尽力通知、STOPPING Leader 发现与宿主启停已接通，见[停止运行链路记录](ordinary-readiness-review.md#ordinary-stop-runtime-progress)；普通自动超时及 Leader RUNNING／STOPPING 统一发现已接通，见[阶段 5 记录](ordinary-readiness-review.md#ordinary-timeout-recovery-progress)；进程 E2E 与跨宿主回报见[阶段 6 首次记录](ordinary-readiness-review.md#ordinary-process-e2e-progress)，Windows 信号退出及调度端 kill 后接续见[生命周期记录](ordinary-readiness-review.md#ordinary-process-lifecycle-progress)，退出预算耗尽及已 ACK 排队结果见[结果丢失记录](ordinary-readiness-review.md#ordinary-result-loss-progress)，最新执行器骤停与 etcd 恢复及本机检查见[收尾记录](ordinary-readiness-review.md#ordinary-local-checkpoint)；阶段 6 仍进行中，分片流程尚未实施，结果无效判定和技术故障保留按[已定分类](data-contracts.md#result-error-classification)执行，不沿用 STOPPING 数值。通信消息与装配、[明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)及[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已定，普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，普通执行开工静态检查已完成，六阶段开发安排已确认，阶段 1～5 普通核心开发及所列专项验证已完成，阶段 6 进行中，完整退出、系统性故障与正式负载等仍待验收；普通先开发验收，完整执行能力待分片完成后正式发布。第一阶段、B1 及 B2 按本轮终态规则修订后继续有效：Task 默认额外执行重试 3 次、间隔 10 秒；整轮超时必须有限、不允许 `0`，新增表单预填 900 秒，API 必填。更新日期：2026-11-05。设计批准、代码交付与测试结果分别记录，正式负载验收尚未完成。

此前单条 Saver 增量验证已确认：全仓普通测试、repository／dispatch 和 taskresult 的真实 PostgreSQL 集成、execution 的真实 etcd 集成均通过；受影响 Job 包的 integration 标签 vet／lint 通过，lint 为 0 issues。并发与 JSONB 异常覆盖、未运行项目及完整范围见[单条保存验证记录](ordinary-readiness-review.md#ordinary-result-save-progress)；结果队列增量的实际验证另见[结果队列记录](ordinary-readiness-review.md#ordinary-result-queue-progress)，此前双 Server 生产装配验证见[Job 生产接线记录](ordinary-readiness-review.md#ordinary-result-wiring-progress)，此前公共执行器与真实回报落库联测见[运行组件记录](ordinary-readiness-review.md#ordinary-executor-runtime-progress)，本轮配置与 System 接线的实际验证见[内嵌接线记录](ordinary-readiness-review.md#ordinary-system-executor-wiring-progress)，不代表普通任务完整进程闭环已经验收。

### 历史批准项

以下批准项保留对应评审时点的范围与待办；“尚未授权编码”“下一项为开工检查”等措辞只描述历史状态，不限制当前已批准的六阶段实施。当前开发进度及实际验证以上文和[分阶段实施记录](ordinary-readiness-review.md#ordinary-implementation-order)为准。

此前批准项：[正式组字段与最小管理入口](data-contracts.md#executor-group-management-contract)、[最小执行查询与停止契约](data-contracts.md#execution-management-contract)及普通开发验收／完整执行发布边界于 2026-11-02 通过。组名和 Key 分别全表唯一，删除后不复用；所有组记录字段完整，软删除保留原字段；旧组兼容安排已按 2026-11-04 的产品版本规则撤销。四个组入口不含编辑；执行管理提供 Instance 详情、所属 Task 分页和按 Instance 停止，停止响应只表达已确认状态及原因。普通闭环先在隔离环境开发验收，完整执行能力待分片完成后正式发布，不增设临时模式开关、候选过滤索引或过渡状态。[专项验收](verification.md#minimal-management-checks)已记录；下一项为普通执行开工检查，尚未授权实现。

此前批准项：[普通 Task 完成读取与异常处理](data-contracts.md#ordinary-completion-query)及[稳定结束原因](data-contracts.md#execution-end-reasons)于 2026-11-02 通过。普通完成使用一次 LEFT JOIN 同快照读取，复用单例索引，不显式加锁、不读取业务 Data；先按父状态和冻结模式解释。RUNNING 普通子缺失或已取消需诊断、不正常完成，但允许原截止后的既有超时处理；数据库失败、坏快照及未知状态必须报错。六个原因值已定，父正常失败统一为 `execution_failed`，等待重试原因为 NULL，STOPPING 冻结停止原因。[专项验收](verification.md#ordinary-completion-query-checks)已记录；[最小接入／管理契约及发布边界](progress.md#ordinary-integration-contract)已通过，下一项为普通执行开工检查，尚未授权编码。

此前批准项：[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)于 2026-10-30 通过。仅新增公共协议 subcode `job.executor.dispatch_rejected`，复用默认业务码 `100001` 与 `ErrorInfo.Reason`；非法报文复用 `common.invalid_request`，认证采用标准 `Unauthenticated`／`PermissionDenied`。明确拒绝只允许接纳前产生，未知码、通用技术错误及无效 ACK 不推断未接单；具体英文 Message 不参与判断。取消完成含无活动正常回复，明确认证／参数错误结束，技术或未知错误按原预算重试同地址；原执行截止不阻止取消，RPC 成败不改数据库停止决定。拒绝耗尽时 Task 原因为 `dispatch_rejected`，等待重试仍为 NULL。[专项验收](verification.md#dispatch-cancel-error-checks)已记录；普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，下一项为普通执行开工检查，未授权生成协议或实现。

此前批准项：[非 BUSY 明确拒绝的当前尝试结算](data-contracts.md#dispatch-rejection-settlement)已于 2026-10-30 通过。普通、准备、分片及 Post 共用接单前明确拒绝规则：按本轮预算立即转等待重试或最终失败，不返还、不重复扣次、不改原尝试编号。使用原身份、执行中及双绑定空的条件更新，不锁父；复用 ACK／BUSY 的有限响应保存预算，未落库由既有无绑定观察兜底。通用通信错误仍不视为明确拒绝，远端截止不代替数据库超时裁决。[专项验收](verification.md#dispatch-rejection-checks)已记录；普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，下一项为普通执行开工检查，尚未授权编码。

此前批准项：[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)已于 2026-10-30 通过。调度端与执行端统一使用 zrpc＋etcd，公共协议子包采用官方 protoc 生成，三方法分别承担派发、Instance 取消和单条结果回报。请求体 GroupAuth、阶段 oneof、JSON bytes 与缺失／空数组区别、接单进程身份均已明确；全部执行业务入口共用组认证，标准 Health 不代表业务就绪。宿主统一进程配置及共享停机预算，实施时验证真实完成信号和退出无循环等待。普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，下一项为普通执行开工检查；[验收清单](verification.md#execution-rpc-checks)已记录，尚未生成协议或授权执行模块编码。

此前批准项：[结果回报错误分类与无效结果处理](data-contracts.md#result-error-classification)已于 2026-10-30 通过。回报队满／暂时不可用及未知通信错误有限重传，明确认证和参数错误沿用 System 及现有 bizerror 机制；永久不可保存内容须有独立证据，不能凭通用资源不足、SQLSTATE 或失败次数丢弃。后台先排除明确失效结果再解析所需快照，技术故障、提交不明及平台配置异常仍保留告警；父 STOPPING／到期不直接使有效子结果失效。既定次数、队列、并发及状态机不变；后续[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)已通过，[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，当前继续普通执行开工检查，尚未授权编码。

本轮[完成与终态规则](behavior.md#terminal-order)调整已通过：完整最终结果允许在父仍 RUNNING 时迟于截止收尾；正常完成父锁外查询后条件更新。自动超时先尝试仅按结果结束，明确未完成且到期才进入 STOPPING；人工停止进入 STOPPING 时保留操作原因，停止决定和已有终态不被后来的完成或超时覆盖；新业务授权仍严格检查原截止，独立有界恢复不依赖下一次 Cron。此前到期一律超时的规则已替换；[完成条件、正常父 UPDATE、摘要与超时衔接](data-contracts.md#instance-completion-contract)也已通过。分片生成封口、完整分片查询 SQL／索引及容量仍须细化，执行模块实现尚未获授权。

## 系统要解决什么问题

单进程 Cron 可以按时调用函数，但不能单独解决多副本重复触发、分片分发、节点故障恢复和执行记录追踪。本框架围绕这些问题形成一条完整链路：保存任务定义，到点创建一轮执行，将业务工作交给多个执行器，接收结果并结束本轮。

业务方提供具名处理器，解释业务参数并保证副作用幂等。平台负责执行尝试、重试预算、结果校验、状态推进及恢复，不把这些规则分散到每个业务函数中。第一版包含基础分片能力，但不扩展为通用工作流或大数据计算引擎。

## 总体方案

采用**单 Leader 触发与运行中／停止中 Instance 低频候选发现、多节点任务驱动推进、业务执行器运行**的架构：

1. etcd 选出有效 Leader，Leader 使用 `robfig/cron/v3` 维护长期规则，到点创建 Instance（一轮执行）；接任时及每分钟按 Job 触发进度查漏，新注册或替换规则后也检查一次到期。各入口共用同 Job 短事务。
2. PostgreSQL 保存 Job（任务定义）、Instance 和 Task（执行单元）。准备、分片和收尾统一建模为 Task，但保留各自阶段语义。
3. 调度节点在取得必要推进权限后保存 Task；按本地派发容量批量条件登记当前尝试，提交后直接派发，不等待下一轮扫描，也不为整轮 Instance 绑定固定负责人。
4. 执行器使用统一 RPC 入口定位具名业务处理器，完成后通过独立有界队列异步上报结果；有限发送并发每次 RPC 发送一个 Task 当前尝试的结果，一次响应确认该结果，不凑批；一次调用结束即释放发送能力，待重试结果按退避保留在回报组件内。任意调度节点成功入队即 ACK，后台校验并保存结果，提交后才推进后续阶段。
5. 有索引、有界的待处理检查承接积压、到期重试和故障恢复。其中 RUNNING Instance 的遗漏推进及到期候选统一由 Leader 每 2 分钟发现，按本趟数据库时间减 4 分钟过滤首次开始时间，有限投递本节点既有 Instance 检查组件；正常结果仍由提交节点立即提示本地检查，按已定 100ms 窗口合并，不等 Leader。停止决定提交后立即交本地后台处理；STOPPING 恢复候选也由 Leader 每 2 分钟独立发现，无 4 分钟门槛，不占派发工作协程或 RUNNING 复核预算。持久化 Task 与状态是推进依据；未落库的回报结果可能随进程退出或执行器回报重试耗尽而丢失，不承诺无损保存。

完整关系图、职责划分和设计取舍见[第 2 章 总体架构](architecture.md)。Job 触发、RUNNING Instance 低频复核与 STOPPING 停止恢复的候选发现集中在 Leader；正常结果处理、状态推进和其他已定候选职责仍按各自机制运行，不给 Instance 绑定负责人，也不经 Leader 转发。当选节点仍可参与普通调度工作；多个节点能够推进不同工作，也不意味着单轮派发和数据库容量没有上限。

## 关键保证与限制

- **平台状态与业务效果分开负责**：平台防止旧尝试覆盖新结果、重复创建收尾及终态恢复；业务方保证准备、分片和收尾可能重复执行时的副作用幂等。
- **本轮执行配置固定**：Instance 创建时保存必要配置，各阶段、重试和恢复沿用；Job 编辑只影响新建轮次。共享配置每轮一份，采用[必要独立列加 JSONB](data-contracts.md#instance-config-storage)，不冻结业务代码或动态资源，详见[本轮执行配置](behavior.md#instance-execution-config)。
- **接收与持久化分开**：回报成功入队即 ACK，不等待后台写库；Task 状态及后续阶段仍以有效结果落库为准。派发接收成功也不代表业务执行成功。
- **结果单条异步上报与有限重传**：每次 RPC 发送一个 Task 当前尝试的结果，一次响应确认已接收、稍后重传或永久拒绝；发送可并发处理多个单条请求，调用结束即释放发送能力，不占住发送者等待退避；不增加凑批窗口或批内部分重传。永久不可交付结果记录错误和指标后释放，不重新执行业务，详见[结果上报契约](data-contracts.md#result-report-contract)。
- **Instance 完成检查独立合并**：结果分别保存成功后，本节点按 Instance 去重合并检查；普通与分片共用机制，阶段条件分别验证。不合并结果报文，也不减少应保存的 Task 结果；其他节点仍可能分别检查，正常结束以稳定完成事实和父终态条件写入协调，后续新工作仍需独立授权。
- **低频复核不保证准时结束**：[统一复核](execution.md#instance-recheck-direction)在无积压时首次检查约为开始后 4～6 分钟，短超时也受 4 分钟门槛约束；正常长任务仍可能被重复检查。分页、队列压力、数据库故障或切主可继续延迟，没有严格恢复时限。截止不因此延长，到期后不再授权新分片、Post 或重试；没有有效 Leader 时该周期查漏暂停，不阻断正常结果路径。
- **派发父锁冲突快速退出**：[派发竞争](execution.md#dispatch-contention)仅将普通首次启动和已有 Task 新尝试登记改为父行 NOWAIT；本地合并重复候选，失败跳过本次同组、不立即重排，后续查询重新发现。RPC 不持父锁或本地登记限制；仍可能重复读取及等待子写入，扫描与候选容量按[已批准初值](execution.md#dispatch-supply-defaults)执行。
- **派发按单次尝试接纳**：一次 RPC 携带一个 Task 的当前尝试及本轮截止时间；接纳 ACK、BUSY、其他明确拒绝与通信结果不明分别处理。本进程只在排队和执行期防止同一尝试重复接纳，业务实际退出即释放活动记录，不等待结果回报 ACK；不做跨进程、跨重启或终态缓存防重。详见[派发契约](data-contracts.md#dispatch-acceptance)。
- **提交报错不等于回滚**：登记不明放弃本次发送，事后见已登记也不补发；BUSY 记账有界有限重试，结果保存沿用有界保留。重做均受原尝试和旧状态约束，不重复返还或推进。详见[提交结果不明恢复](execution.md#commit-unknown)。
- **已绑定结果等待**：已有持久化执行器绑定但无有效结果时保持当前尝试执行中，等待有效结果、人工停止或整轮截止；失联、重启、回报耗尽及 ACK 后结果丢失不单独授权提前重跑。第一版不增加 Task 活动查询或心跳，接受单片丢结果拖至整轮超时且无正常 Post。
- **BUSY 仅用于明确未接单的满载拒绝**：执行器业务等待队列无法接纳且本次未接单、未入队、未执行时，当前有效尝试条件返还已占用机会并延期，不消耗执行重试次数，零重试同样适用。通信结果不确定不能免费返还；延期有退避且不延长整轮截止，具体规则见[BUSY 背压](execution.md#busy-backpressure)及[普通条件更新](data-contracts.md#ordinary-busy-settlement)。
- **按流程说明排队、并发与满载处理**：[设计原则](execution.md#capacity-model)要求正常效率、过载保护与可维护性共同决定方案。[执行器流程](execution.md#executor-processing-flow)采用有界业务等待队列、有限并发业务处理、有界结果队列及独立回报；结果队列满时当前处理者等待交接，回报继续，压力经业务等待队列传回接单入口。共享派发与 Instance 小批轮转保持，各阶段不默认另设配额或总量管理，具体 Go 组织方式结合完整流程选择。
- **重试与资源使用有边界**：业务执行重试遵守统一预算；执行器结果回报独立有限重试，耗尽后释放并记录错误及丢弃指标。调度端已 ACK 结果逐条关联读取及条件 UPDATE、独立提交，不设整批总超时或超时拆批；数据库故障或提交不明保留对应原条目，不重放其他已明确成功的条目。[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)已定为每节点保留 512 条（含待首次处理、待重试和在途）、保存并发 8、单条处理超时 2 秒，三项可部署调整；一次处理结束释放处理能力，退避继续保留原位置，按到期最早顺序处理，不按次数或 TTL 丢弃。满载拒收新结果，即使全部在退避；不与业务流程共用全生命周期额度。整轮超时必须有限，不允许 `0`，新增表单预填 900 秒，API 必填；停止和超时后不继续业务派发或执行重试。
- **新授权与终态协调，已授权执行尽力取消**：新工作授权保留单父行短事务及截止校验；正常完成先在父锁外读结果，再条件 UPDATE，更新仍有行锁，不将所有终态入口强制改成同样的显式锁流程。STOPPING 停止决定或正常终态先提交则不再授权，登记先提交则可能仍有在途 RPC 或业务；结果回报不统一锁父，平台终态不证明实际退出。详见[授权与终止协调](execution.md#authorization-terminal)。
- **停止决定与停止完成分开**：管理停止成功仅表示 STOPPING 及原因已提交；子 Task 按 Instance 有界处理，已有子终态保留，全部子记录终态后才保存父终态与 `finished_at`。远端 RPC 尽力取消，不控制数据库收敛。STOPPING 计入未结束，阻塞丢弃、Job／组删除保护及历史保留持续生效；数据库故障或积压会延长停止中状态，不承诺无条件恢复时限。当前代码仍为五态，新增状态、查询及索引尚未实现。
- **注册与调用分别授权**：单执行器进程单组，组 Token 保护派发、取消和回报；etcd 权限独立保护注册发现。依赖故障按能力处理，两侧内存结果均不保证跨进程退出保留，包括已 ACK 但尚未落库的结果；不依赖执行器本地文件恢复。
- **同进程内分开 RPC 入口**：`job-rpc` 管理与回报 Server 分开；System 首版内嵌执行组件，但使用独立执行器 Server，不新建 Worker，保留以后拆分能力。管理／业务 RPC 原生鉴权由部署方配置，组 Token 只作用于执行协议；不增加内部 RPC 转发，也不把分端口等同于自动完成调用方权限限制。
- **不承诺恰好执行一次或无限扩展**：未知 RPC 结果可能带来重复业务执行；触发、参数体积、写库和派发都需要容量验证。

这些是已确认的设计边界，具体持久化协议与测试证据仍需补齐，不能据此将草案视为已验证产品。

## 说明书大纲

| 章节 | 回答的问题 | 当前设计状态 |
| --- | --- | --- |
| [1 目标与范围](scope.md) | 面向哪些业务，第一版交付什么、不交付什么？ | 功能范围、基准测试规模与质量方向已通过 |
| [2 总体架构](architecture.md) | 系统由谁组成，如何协作，为什么这样划分？ | 核心架构、动态组管理、部署接入及信任与依赖边界已确认，详细协议待闭合 |
| [3 任务模型](task-model.md) | Job、Instance、Task 如何关联，分片代表什么？ | 核心模型、状态语义与两条生命周期已确认，执行协议及参数契约待设计 |
| [4 调度与执行规则](behavior.md) | 触发、错失、重叠、重试和终止分别如何处理？ | 主要规则已确认，少数边界待细化 |
| [5 执行流程与可靠性](execution.md) | 正常流程如何推进，故障和并发在哪里被处理？ | 触发闭环已实现验证；Task 主路径及故障规则已定，执行协议待闭合 |
| [6 数据与接口契约](data-contracts.md) | 什么必须持久化，服务之间交换哪些事实？ | 触发所需三表、管理接口已实现；Task 与完整组接入契约待设计 |
| [7 管理与运维](operations.md) | 如何管理任务、查询运行、接入业务和维护历史？ | 操作范围、凭据管理方向及依赖故障边界已通过，页面与运维实现待设计 |
| [8 验证与交付](verification.md) | 怎样证明正确性、容错及吞吐、延迟和资源占用满足要求？ | 普通适用验收及历史 CI 已通过；分片执行闭环、本机故障与代表性负载已验证，本次远端 CI 待取得，完整性能清单仍保留未覆盖项 |

补充材料：[设计进度与评审计划](progress.md)用于团队推进；[参考资料与决策索引](references.md)记录来源、适用边界及关键决策。尚未完成的章节保留已有约束和明确问题，不以占位代码代替设计。

## 当前推进位置

普通六阶段的适用范围验收及已记录 CI 已完成。分片完整生命周期、故障与跨节点接续，以及阶段 5 的本机查询、代表性负载和必要回归均已完成，达到本机可提交节点；具体范围见[阶段 5 本机记录](sharded-readiness-review.md#sharded-stage5-progress)和[查询与负载记录](sharded-load-validation.md)。下一步为按规范检查并提交、推送，再取得该提交的远端 CI、race 与覆盖率结论；本轮尚未提交或推送，不将本机短时基准扩大为完整性能清单通过或第一版发布完成。

### 历史批准项补充

以下保留早期设计批准和交接说明，含对应时点的编码授权及验证状态；不表示当前仍未开工。当前事实以本节前文和分阶段实施记录为准。

此前批准项：[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)于 2026-10-29 后续通过。每节点组件默认保留 512 条结果，包含待首次处理、待重试和在途；保存并发默认 8，单条一次处理超时默认 2 秒，三项均为部署配置。2 秒从领取条目开始，覆盖连接等待、关联读取、条件更新及提交，不含排队或退避。每条以一次 JOIN 取得 Task／Instance 归属、认证组及本轮冻结的重试上限、间隔，最终仍由原尝试、执行中状态及空或匹配的绑定身份条件 UPDATE 决定是否写入，每条独立提交，不设整批总超时或超时拆批。一次处理结束即释放处理能力，退避保留原条目原位置；新条目最早处理时间为接纳时刻，重试为上次处理结束加正退避，按已到期最早时间及稳定同时间顺序选择，不切首次／重试固定份额。内部退避基准为 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%；可恢复数据库故障、性质不明的技术错误及提交不明保留原结果，不按次数或 TTL 主动丢弃。零行正常结束但不宣称此前保存成功；已明确成功的其他条目不重放，满 512 条即拒收新结果，即使全部在退避。共享 30 秒停机宽限、Instance 原截止及内存结果可能丢失的边界不变；不预定 Go 并发原语。通信消息与装配设计已通过；[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过；[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过；Proto 生成、SQL／实现验证及其他恢复类别仍待完成，尚未授权执行模块编码。

此前已确认的业务处理初值：[执行器业务处理并发与等待队列初值](execution.md#executor-business-defaults)已于 2026-10-28 通过。每执行器进程默认业务处理并发 32、业务等待队列 128，两项均为可调整的部署配置。32 覆盖正在运行业务及业务已完成但结果交接尚未结束的处理；128 仅包含尚未被处理者领取的等待任务，不含处理中。普通、准备、分片和收尾共享这两个进程级限制，不按 Job／Instance 叠加。回报组件满时，已完成业务的处理者继续占用这 32 个处理位置之一，等待交接后再领取下一项；业务实际退出时仍按原规则清理活动防重。已通过的回报 256／16、单条 RPC、内存接纳 ACK、共享 30 秒停机宽限及丢结果边界不变；不增加跨全生命周期额度，不预定 Go 线程或并发原语结构。[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)已另行通过，[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，其余恢复与分片协议继续闭合；已批准的通信协议仍须生成及 SQL／实现验证；本次仅记录设计，尚未授权执行模块编码。

此前已确认的回报安排：[执行器结果保留与发送安排](execution.md#executor-result-queue)及[回报初值](execution.md#reporting-defaults)已于 2026-10-28 通过。回报组件默认保留 256 条，包含待首发、待重试和在途；收到 ACK、永久错误或 7 次应用层调用耗尽后才释放原位置。发送并发默认 16，一次 RPC 结束即释放，不等待退避。新结果接纳时即可发送，重试以上次调用结束加正退避计算；按已到期的最早可发送时间及稳定同时间顺序处理，不切首发／重试固定份额。单次 RPC 2 秒，六次退避 1／2／4／8／16／16 秒且各上下浮动 20%，基准合计 47 秒不是保留 TTL；256／16 为部署配置，其余为集中内部常量。满载仍由有限业务处理者等待交接，沿业务等待队列传递背压，不增加全生命周期共用额度。[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)已另行通过；单条 RPC、内存接纳 ACK、共享 30 秒停机宽限和结果丢失语义保持，尚未编码。

此前已确认的[单次派发时间预算与确认失败处理](execution.md#dispatch-time-budgets)已于 2026-10-28 通过：总处理预算 10 秒从取出候选起算；登记事务 2 秒、接单 RPC 3 秒、ACK 绑定或 BUSY 保存阶段 4 秒，保存最多 3 次含首次、单次 1 秒、失败间隔 200 毫秒，全部受剩余总预算及停机宽限限制。无绑定观察默认 30 秒，从数据库 `attempt_registered_at` 起算，包含提交耗时，节点间保持一致；到期仅获条件结算资格，不证明旧业务停止或承诺恢复时限。BUSY 独立延期 5 秒上下浮动 20%，保存后结束本次派发处理，由既有到期补查发现，不占协程等待。提交不明或预算耗尽且尚未发送时放弃本次发送，不免费重派；元数据保存不重发业务，接单 RPC 与业务 Context 独立。原生客户端与执行器 Server 方法超时须协调，不能被 go-zero 默认两秒提前截断。调度端结果保存初值已另行通过；无绑定扫描参数已按[观察到期扫描与结算安排](execution.md#unbound-recovery-defaults)另行通过，登记批量等未定事项继续闭合，不冻结 Go 编码结构，尚未授权执行模块编码。

**历史评审汇总：触发闭环及代码质量整理已交付，[已记录的 CI](progress.md#trigger-ci-verification)全部通过。普通 Task 主路径、执行器接入与 RPC 入口边界已通过；处理器分工、输入输出及共同执行约束已收口，[Post 输入](data-contracts.md#post-input)不提供分片执行明细或查询。[派发请求与接收 ACK](data-contracts.md#dispatch-acceptance)、[结果上报请求与接收 ACK](data-contracts.md#result-report-contract)及[取消处理与响应](data-contracts.md#cancel-response-contract)语义均已通过。结果首版单条异步上报，保留落库后按 Instance 合并完成检查；取消按 Instance 向已知目标通知，完成本机取消操作即回复，无匹配活动也成功，不等待业务退出。[定向客户端生命周期](execution.md#cancel-routing)及取消不校验预期进程 ID 的决定不变。接入配置、组依赖及启动退出规则已收口；当前推进 Task 持久化与状态。派发默认 64 个工作协程、Task 公共字段与单例唯一索引、[普通首次创建与重复推进 SQL](data-contracts.md#ordinary-start-sql)、[接单 ACK 绑定 SQL](data-contracts.md#executor-binding-sql)、[普通结果保存与状态转换](data-contracts.md#task-result-persistence)、[普通重试登记 SQL](data-contracts.md#ordinary-retry-registration)、[普通未知结算 SQL](data-contracts.md#ordinary-unknown-settlement)、[普通 BUSY 返还 SQL](data-contracts.md#ordinary-busy-settlement)及[停止超时与统一子取消 SQL](data-contracts.md#instance-termination-sql)已通过；本轮停止协议以 STOPPING 替代父先终态再收敛，所有 Task 类型仍共用有界取消，[Instance 完成判定、父条件写入、摘要及超时衔接](data-contracts.md#instance-completion-contract)也已通过；[恢复资源分工](execution.md#recovery-resources)、[可派发候选查询及索引](data-contracts.md#dispatch-recovery-query)、[绑定缺失观察到期查询与索引](data-contracts.md#unbound-recovery-query)及[运行中 Instance 统一复核及查询索引](data-contracts.md#instance-recheck-query)已通过：Leader 每 2 分钟启动或续扫，筛选本趟数据库时间之前已开始超过 4 分钟的 RUNNING Instance，每页 256 条；阶段遗漏推进、正常结束及到期处理共用既有检查组件。[停止协议](data-contracts.md#instance-termination-sql)于 2026-10-23 修订通过：先持久化 `STOPPING = 6` 和停止原因，统一分批取消未结束 Task，确认全部子记录终态后再保存父终态；远端取消独立、尽力执行。STOPPING 候选由同一 Leader 每 2 分钟独立发现，不套用 4 分钟开始时间门槛，新 Leader 就绪后有界检查；使用既有后台恢复容量、独立预算和公平遍历。[STOPPING 取消竞争与容量](execution.md#stopping-processing)于 2026-10-26 收口：每批父主键 NOWAIT 后才查子 Task，竞争失败退出且不立即重排；已确定扫描、本地容量、子批及公平份额初值，不扩展到派发竞争。[远端取消通知](execution.md#cancel-notification-capacity)及[地址查询与索引](data-contracts.md#cancel-target-query)同日通过：按 Instance 向去重地址通知，不逐 Task；新停止决定确认提交后独立交接，不随子批或两分钟恢复重发，父终态不撤销已接纳通知。[派发候选合并与跨节点竞争](execution.md#dispatch-contention)同日单独通过：普通首次启动与已有 Task 新尝试登记改为父行 NOWAIT，本地合并重复候选、同 Instance 登记不自相竞争，失败跳过本次同组且不立即重排；补查独立抖动、已接纳候选分组后不同起点处理。[排队、并发与满载处理](execution.md#capacity-model)于 2026-10-28 通过：共享派发不切来源固定份额，按 Instance 小批轮转；同日补充通过六条设计原则和[执行器处理流程](execution.md#executor-processing-flow)，以有界业务等待队列、有限并发业务处理、有界结果队列及独立回报交接工作。结果队列满时处理者等待交接、不领取下一项业务；活动防重在业务实际退出时清理。各段限制共同说明积压边界，不增加接单到回报的统一计数，不提前固定 Go 并发原语。同日[派发候选容量、分页与退避](execution.md#dispatch-supply-defaults)另已通过，确认等待总量 256、查询最多 128、空闲补查 1 秒上下浮动 20%、每 Instance 先补 8 项及连续无有效登记进展最多 4 页的退避，并补齐临时分页和本轮成功登记恢复预算规则。同日另已通过[单次派发时间预算](execution.md#dispatch-time-budgets)：总处理 10 秒、登记事务 2 秒、接单 RPC 3 秒、响应保存阶段 4 秒，保存最多 3 次含首次、单次 1 秒及间隔 200 毫秒；无绑定观察 30 秒、BUSY 独立延期 5 秒上下浮动 20%，各阶段受剩余预算限制。同日通过[执行器回报安排与初值](execution.md#reporting-defaults)：回报组件默认保留 256 条结果，覆盖待首发、待重试和在途；最多 16 次 RPC 并发，单次调用结束即释放发送能力，退避继续保留原结果位置。按已到期的最早可发送时间选择，同时间稳定排序，不切首发／重试固定份额；单次 RPC 2 秒、最多 7 次应用层调用含首次，六次退避为 1／2／4／8／16／16 秒、各上下浮动 20%。基准退避合计 47 秒，不是保留 TTL；结果保留数和发送并发可配置，其余为内部常量。同日通过[执行器业务处理初值](execution.md#executor-business-defaults)：每执行器进程默认业务处理并发 32、业务等待队列 128，均为可调整的部署配置。32 包含业务运行及业务已完成但结果尚未交接完的处理，128 只计尚未领取的等待任务、不含处理中；普通、准备、分片和收尾共用，不按 Job／Instance 叠加。2026-10-29 后续通过[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)：每节点组件默认保留 512 条待首次处理、待重试和在途结果，保存并发 8，单条一次处理超时 2 秒，三项均可部署调整。每条关联查询 Task／Instance 归属、认证组及本轮冻结重试配置，再按原尝试、执行中状态及绑定身份条件 UPDATE、独立提交，不设整批总超时或超时拆批。一次处理结束即释放处理能力，退避保留原条目原位置；按已到期最早可处理时间及稳定同时间顺序选择，退避基准 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%，可恢复故障不按次数或 TTL 丢弃。[回报错误分类](data-contracts.md#result-error-classification)已通过，[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)与[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)均已通过；[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，Proto 已生成，普通派发、响应保存、父完成与最小执行查询已接入，普通重试与共享派发供给见最新记录；未知结算、停止／超时／恢复及分片流程仍待实施，完整进程验收仍待完成，结果无效判定和技术故障保留按[已定分类](data-contracts.md#result-error-classification)执行，不沿用 STOPPING 数值。第二组通信消息与装配已通过，Proto 生成及存储适配仍待实现验证，[非 BUSY 拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，未定参数与实现验证继续闭合；达到本模块开工条件并获授权后编码。** [触发总审](trigger-readiness-review.md)、[SQL 对照](trigger-sql-review.md)、[HTTP／RPC 对照](trigger-interface-review.md)及 [ADR-0009](../../adr/0009-scheduler-domain-and-etcd.md)的触发接入细则已落实到本批实现，新增执行器及双 Server 决定尚未实现；B2、持久化交接与最小组依赖继续作为后续输入，不重新选择架构。

[执行接入配置布局与来源](architecture.md#execution-configuration)已通过：保留两侧现有顶层 RPC 配置，分别新增 Job 的 `ExecutionRPC` 与 System 的 `JobExecutor`，复用原生 Server／Client 配置类型。执行器配置的组 `name` 是接入身份，Job／Instance 继续引用数据库组 `id`；etcd 发现 Key 单独手工维护，`name` 与 Key 创建后均不可修改。派发客户端依赖部署 etcd 配置和数据库组 Key，不另列静态组映射。[组凭据格式、明文存储与详情查看](operations.md#group-credentials)已通过：第一版不提供 Token 修改或重置，详情按既有权限返回完整 Token，页面遮挡并可显示、复制，列表不返回。

[组配置缓存](operations.md#group-cache)采用各节点启动加载、每 60 秒完整读取未删除组的执行字段，成功原子替换、失败保留旧快照。命中缓存直接使用，未命中则查询单组，同组并发未命中合并查询；存在且未删除时缓存并继续本次认证和处理，查不到或数据库失败不免认证放行。不做组增删差异同步、不新增 etcd 组变更通知，也不因完整刷新重建全部 RPC 客户端。

组删除后的客户端清理已通过：成功替换快照后检查本地已创建的组客户端，组仍存在则原样复用，不存在则先移出客户端缓存，再调用 `client.Conn().Close()`；全量查询失败不替换也不清理。不新增数据库查询、独立清理线程、引用计数或排空等待，也不等待远端业务；回源、创建和清理在本地协调，迟到旧结果不能重新挂回已清理客户端。少量在途 RPC 可能中断，按通信结果不明处理，不能推断执行器未接单或业务已取消；定向取消仍不跨通知缓存。尚未批准实现，设计通过不代表执行组件已实现。

[启动与就绪](operations.md#startup-readiness)要求先初始化所需依赖、处理器和队列，再接收请求；不等待当选 Leader 或所有组在线。退出宽限按整个进程总计默认 **30 秒**、可配置，两套 Server 与全部清理共享剩余预算。调度端停止触发、派发、恢复工作生产和新回报接纳，结束任期并有限善后落库；执行端拒新单且不使用 BUSY，已接纳排队及运行任务可在剩余宽限和原整轮期限内继续，保持结果回报。宽限到期停止启动排队任务、取消运行 Context 并结束等待，由宿主结束进程；组件不调用 `os.Exit`，不强杀 goroutine。退出允许内存结果丢失，不等同业务取消、失败或重派授权。本项规则已收口，下一项为 Task 持久化与状态推进，执行模块尚未完成或获编码授权。

本批已定契约包括：[Job／Instance 字段](data-contracts.md#trigger-field-draft)、[事务与五条触发索引](data-contracts.md#trigger-transaction-proposal)、[简化恢复查询](data-contracts.md#trigger-recovery-scan-proposal)、[自动跳过留痕](data-contracts.md#trigger-skip-record-proposal)、[Job 逻辑删除](data-contracts.md#job-deletion-proposal)、[组引用校验](data-contracts.md#job-group-reference-proposal)、[超时默认值](data-contracts.md#timeout-default-proposal)、[管理入口](data-contracts.md#job-management-proposal)、[组引用并发保护](data-contracts.md#group-reference-concurrency-proposal)及[输入、查询与错误契约](data-contracts.md#job-input-query-proposal)。组第一版无启停，仅逻辑删除；自动触发、派发与回报不增加组行锁，组缓存命中时不另查组表，未命中按上节回源。交接保留 WAITING Instance 及本轮配置，不以通知代替持久化，不提前启动整轮计时。月分区暂不考虑，组删除后的缓存与客户端清理规则已定、尚未实现；历史清理继续在对应模块设计。

本批短字符串按[数据库字段规范](../../development/database-migrations.md#column-types)采用 VARCHAR(n)，名称快照与来源容量一致；业务参数及自由诊断摘要保留 TEXT，配置快照保留 JSONB，见[字段设计](data-contracts.md#string-field-types)。输入／查询／错误契约已实现：普通编辑的说明／参数省略、null、空串均清空；params 去首尾空白，内部内容不变。迁移已通过隔离测试环境验证，不表示已对开发或生产业务库执行迁移。

核心架构、统一 Task、功能范围、基准规模、动态组管理及组级信任边界继续有效。新执行许可集中在[父行登记关口](execution.md#authorization-terminal)，原生路由、绑定缺失结算、已绑定结果等待、异步回报与结果保留共同构成恢复规则。失败或 BUSY 形成的候选不等于执行许可，STOPPING 下未结束子任务由停止恢复继续取消，全部子记录终态后再结束父。提交不明不改变这套授权与终态边界，不增加整轮负责人或可靠消息基础设施。首批止于待开始 Instance 和配置的持久化交接，不宣称业务执行已完成；后续各模块承接这些规则与事实。

第一阶段、B1 与 B2 通过不表示完整执行协议、全部功能或基准验证已完成；触发模块的 DDL、RPC 和规范落地也不等于 Task 模块已实现。具体性能门槛仍在部署资源明确后、正式压测前确认。议题、章节依赖与阶段完成标准见[评审计划](progress.md)。

## 阅读路径

初次了解系统，先读本页和第 1、2 章；理解执行语义，继续读第 3 至 5 章。开发与业务接入人员结合第 6 章确认契约，管理及运维人员阅读第 7 章，测试与交付人员按第 8 章建立验证清单。

每章保留自己的结论与待决边界；正文描述当前设计，ADR 记录关键选择的原因及替代方案，进度页负责评审顺序。三者分工维护，不把对话过程当作设计正文。
