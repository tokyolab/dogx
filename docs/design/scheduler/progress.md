# 设计进度与评审计划

本页用于团队掌握设计与交付进度；系统本身的说明从[总览](index.md)开始。正文按主题维护，评审按依赖推进，避免把文档目录顺序机械地变成工作顺序。

更新日期：2026-11-12。当前实施和验证结论见下方“当前工作”。

> 历史设计与开工阶段汇总：范围与总体架构、B1 状态语义和 B2 执行故障规则均已通过，作为各模块详细设计的共同输入。触发闭环已实现、提交并通过本地功能验证及已记录的 CI；普通 Task 主路径、处理器注册及同进程双 RPC Server 边界已通过。处理器接口分工、输入输出语义及共同执行约束已收口，普通任务仍不支持 Post；分片 Post 只接收公共输入和统计，业务明细由业务自行查询，不增加结果查询 RPC。派发接单、结果上报及取消处理与响应语义均已通过：结果首版采用单条异步上报，保留有限重传及落库后同节点按 Instance 合并完成检查，不做网络合批。取消按 Instance 向已知目标分别通知，不校验预期进程 ID；完成本机取消操作即回复，无匹配活动也成功，不等待业务退出，通信故障只有限重试同一目标。接入配置、组依赖及启动退出规则已收口；当前推进 Task 持久化与状态，Task 公共字段与单例唯一索引、[普通首次创建与重复推进 SQL](data-contracts.md#ordinary-start-sql)、[接单 ACK 绑定 SQL](data-contracts.md#executor-binding-sql)、[普通结果保存与状态转换](data-contracts.md#task-result-persistence)、[普通重试登记 SQL](data-contracts.md#ordinary-retry-registration)、[普通未知结算 SQL](data-contracts.md#ordinary-unknown-settlement)、[普通 BUSY 返还 SQL](data-contracts.md#ordinary-busy-settlement)及[停止超时与统一子取消 SQL](data-contracts.md#instance-termination-sql)已通过，派发工作协程默认每进程 64 个。[Instance 完成判定、父条件写入、摘要及超时衔接](data-contracts.md#instance-completion-contract)也已通过；[恢复资源分工](execution.md#recovery-resources)、[可派发候选查询及索引](data-contracts.md#dispatch-recovery-query)、[绑定缺失观察到期查询与索引](data-contracts.md#unbound-recovery-query)及[运行中 Instance 统一复核及查询索引](data-contracts.md#instance-recheck-query)已通过：Leader 每 2 分钟启动或续扫，筛选本趟数据库时间之前已开始超过 4 分钟的 RUNNING Instance，每页 256 条；阶段遗漏推进、正常结束及到期处理共用既有检查组件。[停止协议](data-contracts.md#instance-termination-sql)于 2026-10-23 修订通过：先持久化 `STOPPING = 6` 和停止原因，统一分批取消未结束 Task，确认全部子记录终态后再保存父终态；远端取消独立、尽力执行。STOPPING 候选由同一 Leader 每 2 分钟独立发现，不套用 4 分钟开始时间门槛，新 Leader 就绪后有界检查；使用既有后台恢复容量、独立预算和公平遍历。[STOPPING 取消竞争与容量](execution.md#stopping-processing)于 2026-10-26 收口：每批父主键 NOWAIT 后才查子 Task，竞争失败退出且不立即重排；已确定扫描、本地容量、子批及公平份额初值，不扩展到派发竞争。[远端取消通知](execution.md#cancel-notification-capacity)及[地址查询与索引](data-contracts.md#cancel-target-query)同日通过：按 Instance 向去重地址通知，不逐 Task；新停止决定确认提交后独立交接，不随子批或两分钟恢复重发，父终态不撤销已接纳通知。[派发候选合并与跨节点竞争](execution.md#dispatch-contention)同日单独通过：普通首次启动与已有 Task 新尝试登记改为父行 NOWAIT，本地合并重复候选、同 Instance 登记不自相竞争，失败跳过本次同组且不立即重排；补查独立抖动、已接纳候选分组后不同起点处理。[排队、并发与满载处理](execution.md#capacity-model)于 2026-10-28 通过：共享派发与 Instance 小批轮转保持；同日明确[执行器处理流程](execution.md#executor-processing-flow)：有界业务等待队列、有限业务并发、有界结果队列及独立有限回报并发，队满由交接等待向接单传递压力；按六条原则优先采用简单实现，不新增跨阶段统一配额，不提前固定 Go 并发原语。同日[派发候选容量、分页与退避](execution.md#dispatch-supply-defaults)另行通过：等待总量 256、查询最多 128、空闲补查 1 秒上下浮动 20%、每 Instance 先补 8 项、连续无有效登记进展最多 4 页，临时分页与本轮成功登记恢复预算规则已确认。同日另已通过[单次派发时间预算](execution.md#dispatch-time-budgets)：总处理 10 秒、登记事务 2 秒、接单 RPC 3 秒、响应保存阶段 4 秒，保存最多 3 次含首次、单次 1 秒及间隔 200 毫秒；无绑定观察 30 秒、BUSY 独立延期 5 秒上下浮动 20%，各阶段受剩余预算限制。同日通过[执行器回报安排与初值](execution.md#reporting-defaults)：回报组件默认保留 256 条结果，覆盖待首发、待重试和在途；最多 16 次 RPC 并发，单次调用结束即释放发送能力，退避继续保留原结果位置。按已到期的最早可发送时间选择，同时间稳定排序，不切首发／重试固定份额；单次 RPC 2 秒、最多 7 次应用层调用含首次，六次退避为 1／2／4／8／16／16 秒、各上下浮动 20%。256／16 为部署配置，其余为内部常量，47 秒是基准退避合计而非 TTL。同日通过[执行器业务处理初值](execution.md#executor-business-defaults)：每执行器进程默认业务处理并发 32、业务等待队列 128，均为可调整的部署配置。32 包含业务运行及业务已完成但结果尚未交接完的处理，128 只计尚未领取的等待任务、不含处理中；普通、准备、分片和收尾共用，不按 Job／Instance 叠加。2026-10-29 后续通过[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)：每节点组件默认保留 512 条待首次处理、待重试和在途结果，保存并发 8，单条一次处理超时 2 秒，三项均可部署调整。每条关联查询 Task／Instance 归属、认证组及本轮冻结重试配置，再按原尝试、执行中状态及绑定身份条件 UPDATE、独立提交，不设整批总超时或超时拆批。一次处理结束即释放处理能力，退避保留原条目原位置；按已到期最早可处理时间及稳定同时间顺序选择，退避基准 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%，可恢复故障不按次数或 TTL 丢弃。[回报错误分类与无效结果处理](data-contracts.md#result-error-classification)已通过，[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)与[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)均已通过；[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，Proto 已生成，SQL 和完整运行装配验证仍待完成；其余恢复参数、状态查询与分片协议继续闭合，结果无效判定和技术故障保留按[已定分类](data-contracts.md#result-error-classification)执行，不沿用 STOPPING 数值，阶段 1 已获授权开始实施。后续仍按“详细设计 → 评审 → 实现和测试”推进，不等待全系统细节全部完成。Task 默认额外执行重试 3 次、间隔 10 秒，整轮超时必须有限、不允许 `0`，新增表单预填 900 秒，API 必填。

## 当前工作

2026-11-12 管理模块：用户已批准管理范围并授权实施，一级菜单采用“定时任务”，与系统管理同级；二级为任务管理、执行记录及执行器组。页面、历史查询、Task 按需数据、菜单及接口登记已实现，本机验证与边界统一记录于[定时任务管理](management.md)。管理模块的远端 CI 结果需另行记录，不能引用下方分片 CI 证明本轮管理代码通过；历史清理、具体生产处理器及完整发布条件仍分别推进。

最近已推送节点：**分片阶段 1～5 的实施与已确认范围验收完成，提交 `14897bd` 的 [GitHub CI 43139291006](https://github.com/tokyolab/dogx/actions/runs/43139291006) 已结束且全部成功。** 普通六阶段的适用范围验收及历史 CI 继续保留；本次分片的远端结果和四种覆盖率口径统一见[CI 收尾](sharded-readiness-review.md#sharded-ci-closeout)，本机范围见[查询与负载记录](sharded-load-validation.md)。已批准的 [info 日志临时覆盖限制](ordinary-readiness-review.md#ordinary-shutdown-race-workaround)继续有效；管理页面、历史清理、具体生产处理器及其他第一版发布条件未因此完成。

2026-11-04 修订：[产品版本兼容边界](../../development/database-migrations.md#release-compatibility)已确定；同一产品版本不为开发中间态编写兼容，仅在已有历史 Release、明确升级到后续版本且实际不兼容时允许必要兼容。此前旧组回填、分阶段升级及历史 NULL 例外撤销，组字段统一完整非空；该次规则修订本身不授予编码权限；后续已批准的开发安排及当前进度见下段。

当前开发安排：[普通六阶段计划](ordinary-readiness-review.md#ordinary-implementation-order)和[分片五阶段安排](sharded-readiness-review.md#sharded-implementation-order)的实施与已确认范围验收均已完成。分片阶段 1～4 已接通输入及迁移、Pre 清单保存和原子生成、统一派发、Post 与整轮完成，并验证故障和跨节点接续；阶段 5 已完成本机查询计划、两调度／两执行器代表性负载及必要回归。全部分片改动已于 2026-11-12 提交并推送，对应 CI 全成功，实际范围及限制见[CI 收尾](sharded-readiness-review.md#sharded-ci-closeout)和[查询与负载记录](sharded-load-validation.md)。后续管理、运维与完整发布余项继续按各自范围评审，不把本轮验收扩展为完整第一版完成。

### 分片提交与 CI 收尾（2026-11-12） {#sharded-ci-verification}

提交 `14897bd43b8dbcda69023a55fef5bcee30fa4165` 已推送至 `main`，[CI 43139291006](https://github.com/tokyolab/dogx/actions/runs/43139291006) 的 Go lint、Windows tests、Linux fast checks、PostgreSQL／Redis／etcd integration 全部成功。对应 Codecov 合并项目为 91.69%、unit 为 79.36%、integration 为 91.68%、patch 为 95.49%，patch 超过 80% 门槛。数据遵循仓库排除规则；integration 包含普通测试和 integration 标签测试，E2E／负载未采集覆盖率。准确命中行、测试范围及限制统一见[分片 CI 收尾](sharded-readiness-review.md#sharded-ci-closeout)。

下方阶段 1～5 记录保留各本机检查时点的事实，其中“未提交”“待 CI”不是当前状态。本次远端成功不改变既有负载边界、go-zero info 日志临时覆盖限制或其他第一版发布余项。

### 分片阶段 5 验收（2026-11-12） {#sharded-stage5-implementation}

本机查询、代表性负载和必要回归已完成，达到本机可提交节点。两个调度进程、两个同组执行器保留每进程默认配置；单轮 500 片及 100 个 Instance 各 10 片混合普通任务各三轮，全部达到事前确认门槛，近 4 MiB 准备输出与停止竞争也通过。真实 GORM 查询计划、共享检查预算与排队、最终普通 HTTP→RPC 成功／重试／停止及分片、退出回归的证据见[查询与负载记录](sharded-load-validation.md)。原失败记录和夹具修正保留，不以最终复跑覆盖此前事实；本机短时基准不代表长期稳定性、极限吞吐或完整性能清单通过。尚未提交或推送，没有本次 Linux／race 或 CI 覆盖率结论。

### 分片阶段 4 验收（2026-11-11） {#sharded-stage4-implementation}

真实 System 业务处理器验证了完整分片、单片重试、Post 重试、调度节点骤停后由另一节点接收结果，以及人工停止。三处阶段提示丢失另通过真实 Job 进程骤停与接任验证，精确四分钟恢复门槛、阶段事务与停止交错、迟到 ACK／结果及未知结算由真实 PostgreSQL 测试覆盖，实际证据与边界见[阶段 4 记录](sharded-readiness-review.md#sharded-stage4-progress)。

五个相关包的 integration 标签完整测试、普通进程宕机与生命周期回归均通过，带标签 lint 为 0 issues。本轮只增加测试、CI 入口和文档，没有修改生产 Go 逻辑。阶段 4 结束时已关闭当轮隔离 PostgreSQL 与 Redis；后续本机查询与代表性负载已按确认范围通过，见[阶段 5 负载记录](sharded-load-validation.md)。本次 Linux／race 和 CI 覆盖率尚未取得，不用本机功能或负载测试替代。

### 分片阶段 3 实施（2026-11-11） {#sharded-stage3-implementation}

Post 固定统计与唯一创建、零分片及父最终判定、Checker 的最小阶段查询和生产双向提示均已接通。阶段动作共用原检查预算，创建成功保留新提示；已齐最终结果允许晚于截止正常完成，缺阶段且到期不补建新工作。实际实现、测试和剩余边界见[阶段 3 记录](sharded-readiness-review.md#sharded-stage3-progress)。

真实 PostgreSQL 和生产 `svc` 宿主专项通过，包含 8 个完整分片场景及原普通链路回归。该宿主测试不等同于跨业务多进程故障 E2E；后续节点退出、提示丢失等系统性验收已完成，见[阶段 4 记录](sharded-readiness-review.md#sharded-stage4-progress)。后续本机代表性负载已按确认范围通过，见[阶段 5 负载记录](sharded-load-validation.md)；本次 GitHub CI 尚未取得。

### 分片阶段 2 实施（2026-11-11） {#sharded-stage2-implementation}

Pre 首次启动、成功清单保存、整组生成和统一候选／尝试登记组件已实现；四阶段发送、结果与未知结算同步扩展。`HintTasks` 复用原共享容量及串行扫描，没有新的阶段队列。实际验证和边界见[阶段 2 记录](sharded-readiness-review.md#sharded-stage2-progress)。

阶段 2 结束时，完整 Checker 尚未接入分片阶段选择，生成组件由联测显式调用；该阶段结果不作为完整分片宿主闭环证据。后续 Post、零分片／父最终判定及生产双向提示接线见上方阶段 3，分片改动尚未提交或推送。

### 分片阶段 1 实施（2026-11-11） {#sharded-stage1-implementation}

输入协议与数据基础已完成，实际范围见[阶段 1 记录](sharded-readiness-review.md#sharded-stage1-progress)：单份 `Item`、准备清单 500 上限、输入输出共用 `TaskData`，以及可空 `input_data` 和替换派发索引的增量迁移。

本机受影响普通测试、覆盖全部正式源码的普通回归、migration／repository／taskresult 的 PostgreSQL 18.6 测试均通过；相关包带 integration／E2E 标签的 lint 为 0 issues，生成一致性检查通过。首次 `./...` 被本地旧审计快照干扰的原因及完整重跑口径已记录。未修改前端，未提交或推送，没有本次 CI 或覆盖率结论。

本阶段结束后的 Pre 成功与清单原子保存、整组生成和统一派发进展见上方阶段 2。Post、完整阶段推进及系统性故障、负载验收继续留在后续阶段，不把输入存储基础当作完整分片能力已交付。

### 最终分片清单修订（2026-11-06） {#explicit-shard-approval}

| 项目 | 本轮已批准 | 实施与剩余边界 |
| --- | --- | --- |
| 业务分片职责 | Pre 外层数组每个元素定义一个最终逻辑分片 Task；元素内部结构由业务定义，平台不再拆分或组合 | 替代旧的“平台通用切分／子数组”语义；不增加单片最小／最大配置、按节点数分组、优先级或递归 Map |
| 输入输出与通信 | 准备清单为 `[]json.RawMessage`，每个 Task 收到一个 JSON 值；Proto `bytes`、单份 `input_data JSONB` 与执行输出 `result_data` 分离 | 现有分片数组接口及 Proto 尚未修订，输入列尚未新增；执行器通过结果回报交给调度端，由调度端写 Job 数据库，无直接数据库连接或兼容模式 |
| 生成及故障规则 | 成功 Pre 先持久化，完整生成才派发；中断恢复生成且不重复创建，重试及节点变化不改变原分片内容；零清单、重复元素和 Post 统计按现行契约 | 同日后续已通过下述整组原子生成；原先暂留的清单存储和生成完整性问题已确认，尚未实施 |

正文与[专项验收](verification.md#explicit-shard-contract-checks)已同步。本轮仅为设计批准，不将既有普通模式 CI 或接口测试视为新分片契约的实现验证。

### Pre 清单保存与整组生成（2026-11-06） {#atomic-shard-generation-approval}

[生成协议](execution.md#atomic-shard-generation)、[存储契约](data-contracts.md#shard-generation-storage-contract)及[验收要求](verification.md#atomic-shard-generation-checks)已同步，尚未实施。

| 项目 | 已批准决定 | 剩余边界 |
| --- | --- | --- |
| 准备输出保存 | Pre 成功状态与完整清单原子保存到该准备 Task，清单使用 `result_data JSONB`，随本轮历史记录保留 | 接受与单份 `input_data` 重复存储；具体历史保留期限和清理实现仍待落实 |
| 生成与防重 | 父主键 NOWAIT 事务中核实运行状态、成功 Pre 和锁后数据库截止，重新查已有分片；不存在时一次批量插入全部待执行分片，整组提交才派发 | 所有生成入口共用父锁、整组提交，活跃 Task 不删除；不新增生成标志、游标、计数或续写分片序号，具体 SQL／索引及运行交接待实现 |
| 空清单与恢复 | 成功保存的 `[]` 就是零分片完成事实；失败整组回滚，提交不明重入父行协议核实，已成功 Pre 不重跑 | Post 创建仍检查父状态和原截止；通知丢失继续由既有派发补查和 Instance 复核接续，不承诺立即恢复 |
| 数量边界 | 每轮最多 500 个业务分片，Pre／Post 不计入；超限不截断、合并或分次生成 | 沿用无效输出错误契约，拒绝不等于已保存 Pre 失败；500 行及既有报文上限不代替实际事务耗时验证 |

本轮仅记录批准决定和后续验收，不修改 Go／Proto、迁移或前端，不提交或推送；此前普通模式测试与 CI 不作为新分片路径的验证结果。


### Pre 首次启动与分片首次派发（2026-11-06） {#sharded-start-dispatch-approval}

[执行规则](execution.md#sharded-start-dispatch)、[持久化契约](data-contracts.md#sharded-start-dispatch-contract)及[专项验收](verification.md#sharded-start-dispatch-checks)已同步，仍为设计批准，尚未实施。

| 项目 | 本轮已批准 | 实施边界 |
| --- | --- | --- |
| Pre 首次启动 | 先取得派发能力，父 NOWAIT 下合并保存 Instance 首次开始／截止／RUNNING 和唯一执行中 Pre，执行编号与已用次数均为 1；确认提交才发送 | 沿用普通首次启动的重复、冲突、提交不明及停止规则，原普通方法中的类型限制和分片配置校验需扩展 |
| 分片初值与首次登记 | 生成时待执行、编号／次数为 0，首次和当前登记时间为空，最早派发时间取生成数据库时间；首次派发父授权后条件改为执行中及 1／1，同时填两项时间并清空最早派发时间 | 首次与重试保留各自旧状态条件，BUSY 的 1／0 不冒充首次；输入不变，不能给整组积压提前授权 |
| 候选供给 | 新建待执行分片加入现有供给，正常提交及时提示，遗漏通过同一补查接续；共享既有容量、周期与 Instance 轮转 | 具体查询、分页及索引须落实验证，不新增分片专属队列、扫描频率或并发初值 |
| 共用执行协议 | ACK、BUSY、拒绝、回报、未知结算及取消规则保持，按 Task 类型选入口，分片携带固定单份输入 | 具体 SQL、运行交接和普通回归与分片故障验证待完成；本轮仅改文档，不提交或推送 |

### Post 创建、固定输入与首次派发（2026-11-09） {#post-creation-approval}

[执行流程](execution.md#post-creation)、[持久化契约](data-contracts.md#post-creation-contract)及[专项验收](verification.md#post-creation-checks)已同步。本轮只记录批准设计，尚未实施。

| 项目 | 本轮已批准 | 实施与剩余边界 |
| --- | --- | --- |
| 完成事实与统计 | 父锁外确认分片完整生成且全部成功或最终失败，取得稳定三个计数；成功 Pre 的明确 `[]` 对应零计数 | 依赖固定集合、不可逆终态及活跃 Task 不清理；父锁内不重复统计，不逐条更新父计数，具体读取 SQL／索引待落实 |
| 固定输入 | 复用 PostTask 的 `input_data JSONB` 保存 `total / succeeded / failed`；首次、重试及换节点复用，公共参数来自 Instance 快照 | 明确扩展此前仅保存分片业务输入的用途；业务输出继续写 `result_data`，不覆盖固定输入，不增加业务明细查询 |
| 唯一创建 | 本轮独立批准父主键 NOWAIT；锁内复核运行状态、冻结 Post 配置和锁后数据库截止，查已有记录后唯一插入 | 竞争退出不忙重抢；既有单例部分唯一索引覆盖收尾类型，事务不重聚合分片、不含外部 I/O |
| 首次派发与恢复 | 创建为待执行 0／0、首次及当前登记时间为空、最早派发时间取创建数据库时间；不等容量，提交后提示共享供给；取得派发能力后父授权并条件登记 1／1 | 与待执行分片共用供给、首次登记及恢复，覆盖丢失通知／创建节点退出；仅新登记确认提交才 RPC，提交不明不补发原尝试，SQL／运行交接及验证待完成 |

部分或全部分片最终失败仍运行已配置 Post；Pre 最终失败、停止或到期不补建，合法零分片同样检查原截止。Post 重试不重跑 Pre／分片，成功不抹除分片失败，已配置 Post 缺失不能直接正常结束。当前无运行代码、迁移或前端修改，未提交或推送；普通历史 CI 不作为本轮验证。

### 分片查询、索引与组件交接（2026-11-09） {#sharded-query-handoff-approval}

[数据契约](data-contracts.md#sharded-query-contract)、[运行职责](execution.md#sharded-component-handoff)及[专项验收](verification.md#sharded-query-handoff-checks)已同步。设计通过与实现、执行计划、负载成绩分别记录，本轮未实施或运行测试。

| 项目 | 本轮已批准 | 实施与验收边界 |
| --- | --- | --- |
| 输入列与派发索引 | 新增可空且无默认值的 `input_data JSONB`；原重试索引替换为 `(next_dispatch_at,id) WHERE status IN (1,3)` 的 `idx_job_task_dispatch_due`，不保留重叠两份 | 普通／Pre 不使用输入列；JSON null 与 SQL NULL 区分，不新增阶段索引、JSONB 索引或生成字段；迁移尚未执行 |
| 两来源供给 | WAITING Instance 保留原来源；待执行分片／Post 与各类型重试合并为到期 Task，共用固定数据库时间、复合临时游标及到期顺序 | 候选保存原状态、类型、编号；首次 0／0 与重试递增分别条件登记。保持容量、周期及 Instance 轮转，不新增第三扫描或重试优先级 |
| 阶段读取 | 先读父与单例最小状态；仅成功 Pre 且无分片时读完整清单。分片先查未结束存在性，再按需聚合并核验总数等于成功数加最终失败数 | 复用单例、未结束及 Instance 索引；已有分片不反复读完整准备输出，不把取消或异常状态当正常完成，不逐 Task 补查父或输入 |
| 组件交接与处理预算 | 结果提交提示 Instance 检查；阶段创建提交提示派发。派发区分待开始父首次启动与运行中父下待执行 Task 补充。一次检查在原默认 2 秒共享预算内最多执行一次阶段数据库动作 | 本轮明确扩大检查范围，允许最多 500 行整组生成、Post 创建或父完成；不新建生成队列／池，不等 RPC，不重开各阶段预算；零分片直接选择 Post／父完成 |
| 性能与异常边界 | 部分索引谓词可匹配，时间及排序可利用索引，中间 OR 不使其失效；父状态／截止仍须关联过滤 | 不承诺一定选用索引或访问量等于 LIMIT；真实 GORM 执行计划须覆盖历史量和大量不合格父。验证生成耗时及混合检查排队，持续预算不足应优化或调整可配置预算，不靠反复重试掩盖 |

普通类型限制须在派发、结果保存、BUSY、明确拒绝与未知结算等路径逐处扩展，保持冻结模式及阶段校验，不以删除一个类型条件代替完整实现。丢失提示分别由原派发供给和 Leader Instance 复核接续；已登记尝试不补发，停止协议不改变。下一项为分片开工检查；本轮仅更新文档，未提交或推送。

### 分片开工检查（2026-11-09） {#sharded-readiness-review}

已完成[代码差距核对](sharded-readiness-review.md#sharded-code-gap)，未发现必须重新评审产品行为才能编码的阻塞。现有四阶段接口、通信入口和运行组件可复用；输入数组改单份 JSON、输入列与索引、Pre／分片／Post 持久化、统一供给、阶段读取及提示接线仍需实施。

[建议开发顺序](sharded-readiness-review.md#sharded-implementation-order)分为输入与数据基础、Pre 与分片链路、Post 与整轮完成、故障与跨节点验收、查询负载与交付五阶段。各阶段同步验证，错误与停止保护不推迟到最后；这是本次实施安排，不另增业务规则。已明确[本机可提交节点和远端验收边界](sharded-readiness-review.md#sharded-validation-plan)，新增 E2E 须同步 CI 显式选择器。

本次只做静态审计及文档记录，未修改运行代码、生成协议、执行迁移或运行测试，未提交或推送。性能门槛在正式压测前确认，不套用普通历史负载结论；下一步先实施输入协议与数据基础。

### 历史批准项

以下段落保留各项评审通过时的范围和待办。其中“尚未授权编码”“下一项为开工检查”等措辞仅表示对应历史时点，不代表当前授权或实施状态；当前开发安排已获批准，进度以本节前文、下方当前实现状态及分阶段实施记录为准。

此前批准项：[正式组字段与最小管理入口](data-contracts.md#executor-group-management-contract)、[最小执行查询与停止契约](data-contracts.md#execution-management-contract)及普通开发验收／完整执行发布边界于 2026-11-02 通过。组名和 Key 分别全表唯一，删除后不复用；所有组记录字段完整，软删除保留原字段；旧组兼容安排已按 2026-11-04 的产品版本规则撤销。四个组入口不含编辑；执行管理提供 Instance 详情、所属 Task 分页和按 Instance 停止，停止响应只表达已确认状态及原因。普通闭环先在隔离环境开发验收，完整执行能力待分片完成后正式发布，不增设临时模式开关、候选过滤索引或过渡状态。[专项验收](verification.md#minimal-management-checks)已记录；下一项为普通执行开工检查，尚未授权实现。

此前批准项：[普通 Task 完成读取与异常处理](data-contracts.md#ordinary-completion-query)及[稳定结束原因](data-contracts.md#execution-end-reasons)于 2026-11-02 通过。普通完成使用一次 LEFT JOIN 同快照读取，复用单例索引，不显式加锁、不读取业务 Data；先按父状态和冻结模式解释。RUNNING 普通子缺失或已取消需诊断、不正常完成，但允许原截止后的既有超时处理；数据库失败、坏快照及未知状态必须报错。六个原因值已定，父正常失败统一为 `execution_failed`，等待重试原因为 NULL，STOPPING 冻结停止原因。[专项验收](verification.md#ordinary-completion-query-checks)已记录；最小组接入、观察／停止入口及发布边界已通过，下一项为普通执行开工检查，尚未授权编码。

此前批准项：[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)于 2026-10-30 通过。仅新增公共协议 subcode `job.executor.dispatch_rejected`，复用默认业务码 `100001` 与 `ErrorInfo.Reason`；非法报文复用 `common.invalid_request`，认证采用标准 `Unauthenticated`／`PermissionDenied`。明确拒绝只允许接纳前产生，未知码、通用技术错误及无效 ACK 不推断未接单；具体英文 Message 不参与判断。取消完成含无活动正常回复，明确认证／参数错误结束，技术或未知错误按原预算重试同地址；原执行截止不阻止取消，RPC 成败不改数据库停止决定。拒绝耗尽时 Task 原因为 `dispatch_rejected`，等待重试仍为 NULL。[专项验收](verification.md#dispatch-cancel-error-checks)已记录；普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，下一项为普通执行开工检查，未授权生成协议或实现。

此前批准项：[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)于 2026-10-30 通过。普通、准备、分片及 Post 共用接单前明确拒绝规则：按本轮预算立即转等待重试或最终失败，不返还、不重复扣次、不改原尝试编号。使用原身份、执行中及双绑定空的条件更新，不锁父；复用 ACK／BUSY 的有限响应保存预算，未落库由既有无绑定观察兜底。通用通信错误仍不视为明确拒绝，远端截止不代替数据库超时裁决。[专项验收](verification.md#dispatch-rejection-checks)已记录；普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，下一项为普通执行开工检查，尚未授权编码。

此前批准项：[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)于 2026-10-30 通过。调度端与执行端统一 zrpc＋etcd；公共协议位于 `pkg/jobexecutor/protocol`，官方 protoc 仅生成 protobuf／gRPC 绑定，应用契约仍用 goctl。确定 Executor 的 DispatchTask／CancelInstance 与 ResultReceiver 的 ReportTaskResult、请求体 GroupAuth、阶段 oneof、JSON bytes 及 Pre 缺失／空数组区别。三个执行业务入口共用组认证，新增入口不叠加原生 App／Token，保留标准 Health 但不视为业务就绪，不注册反射。宿主统一进程配置及包含框架阶段的 30 秒停机预算，等待真实完成信号，避免退出回调等待 Start 返回，全局日志最后关闭。普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，下一项为普通执行开工检查；Proto 生成、装配及 SQL／运行验收仍待完成，未授权执行模块编码。对应[验收清单](verification.md#execution-rpc-checks)已记录，尚未运行。

此前批准项：[结果回报错误分类与无效结果处理](data-contracts.md#result-error-classification)于 2026-10-30 通过。回报队满使用 ResourceExhausted，未就绪、退出及认证依赖失败使用 Unavailable，沿用有限重传；明确认证失败沿用 System 的 Unauthenticated／PermissionDenied，非法报文复用 bizerror 与 common.invalid_request。未知错误或异常响应保守重传，不解析文案，也不凭通用资源不足推断报文超限。后台利用已有 JOIN 事实先排除无合法目标、跨组、旧尝试或 Task 已结束等明确失效结果，再解析仍需使用的配置；永久不可保存内容必须独立核实，不能仅凭 SQLSTATE。技术故障、提交不明、坏快照或未知版本保留并告警；父 STOPPING 或到期不是丢弃有效子结果的依据。业务失败是合法执行结果，仍按原预算保存；没有新增长度限制、队列或 TTL。后续通信消息与装配已按[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)通过；[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，当前继续普通执行开工检查，尚未授权执行模块编码。

此前批准项：[观察到期扫描与结算安排](execution.md#unbound-recovery-defaults)于 2026-10-29 后续通过。各调度节点独立参与，每节点一个串行、不重叠的后台流程；实际结束后等待 10 秒 ±20%，每页最多 128 条，每短批最多 16 条复核父状态、冻结配置并重新取数据库时间。单次数据库操作预算 1 秒、一次运行预算 5 秒；除 16 为内部初值外，均可部署调整。每条 Task 条件更新并独立提交，16 条复核不构成共同事务；预算内可继续多页。每趟固定扫描时间与观察门槛，按 `(attempt_registered_at, id)` 临时分页，跨运行续接时取得新预算并重新复核。游标仅沿原序已处理连续前缀前移；单项写入技术失败或提交不明可以越过该已尝试项，但立即结束本次运行，下一新趟重新覆盖仍符合条件的失败项；未处理候选不能越过。没有新增长期队列、处理池或 NOWAIT，不占派发、结果保存和 Instance 检查的处理并发；行锁等待和提交不明仍按实际结果处理。30 秒观察、父终止及原尝试预算规则不变。其余生成和实现验收继续推进，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已另行通过，尚未授权执行模块编码。

此前批准项：[RUNNING 扫描预算与恢复补充](execution.md#instance-recheck-defaults)于 2026-10-29 后续通过。候选单次扫描查询默认 1 秒、每周期扫描／交接默认 5 秒，两项部署可调；包含取连接、新趟取时、分页查询、交接及拥塞等待，单次查询服从剩余预算。续扫每周期取得新预算但沿用原扫描上界和游标。恢复新增 ID 只在共享检查集合占用低于总量一半时接纳（默认 256），已有 ID 可合并；达到门槛或集合满时在剩余预算内等空间，替代 RUNNING 队满即结束本周期。正常提示可用全部 512，检查仍共享 8 并发及独立 2 秒，不按来源分池或计数。只接纳／合并才推进，不越过首未接纳项；查询失败结束本周期，到尾不在本周期从头重扫。补充门槛不是周期总量，持续或覆盖整个扫描窗口的拥塞仍可能长期阻止恢复准入，不承诺固定份额或恢复 SLA。STOPPING 规则保持独立。绑定缺失观察到期的[扫描／结算安排与参数](execution.md#unbound-recovery-defaults)已另行通过；其余生成及开工检查继续推进，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已另行通过，尚未授权执行模块编码。

此前批准项：[Instance 检查提示生命周期与初值](execution.md#instance-check-defaults)于 2026-10-29 后续通过。每调度进程保留 512 个唯一 Instance ID（含合并等待、排队及处理中），检查并发 8，单次处理预算 2 秒，三项部署可调；100ms 为内部合并初值，从首提示算且不被后续提示延长。检查期间首条新提示决定下一轮最早时间，本次结束后复用原条目转队尾一次；技术错误也不能清掉新提示，明确确认父终态才可直接清理合并提示。无新提示时结束本次本地处理，错误不自行无限重查，后续通知或既定 Leader 复核补漏。满时已有 ID 仍合并，新正常提示非阻塞放弃；2 秒只覆盖必要事实读取与有限推进，不含排队、业务执行、整轮生成或停止处理，也不证明数据库已回滚。Leader RUNNING [扫描预算与恢复补充](execution.md#instance-recheck-defaults)已于同日后续收口；不增加来源份额，未授权执行模块编码。专项[验收要求](verification.md#instance-check-defaults-checks)已同步，尚未实现运行。

此前批准项：[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)于 2026-10-29 后续通过。每节点组件默认保留 512 条结果，包含待首次处理、待重试和在途；保存并发默认 8，单条一次处理超时默认 2 秒，三项均为部署配置。2 秒从领取条目开始，覆盖连接等待、关联读取、条件更新及提交，不含排队或退避。每条以一次 JOIN 取得 Task／Instance 归属、认证组及本轮冻结的重试上限、间隔，最终仍由原尝试、执行中状态及空或匹配的绑定身份条件 UPDATE 决定是否写入，每条独立提交，不设整批总超时或超时拆批。一次处理结束即释放处理能力，退避保留原条目原位置；新条目最早处理时间为接纳时刻，重试为上次处理结束加正退避，按已到期最早时间及稳定同时间顺序选择，不切首次／重试固定份额。内部退避基准为 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%；可恢复数据库故障、性质不明的技术错误及提交不明保留原结果，不按次数或 TTL 主动丢弃。零行正常结束但不宣称此前保存成功；已明确成功的其他条目不重放，满 512 条即拒收新结果，即使全部在退避。共享 30 秒停机宽限、Instance 原截止及内存结果可能丢失的边界不变；不预定 Go 并发原语。通信消息与装配设计已通过；[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过；[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过；Proto 生成、SQL／实现验证及其他恢复类别仍待完成，尚未授权执行模块编码。

此前已确认的业务处理初值：[执行器业务处理并发与等待队列初值](execution.md#executor-business-defaults)已于 2026-10-28 通过。每执行器进程默认业务处理并发 32、业务等待队列 128，两项均为可调整的部署配置。32 覆盖正在运行业务及业务已完成但结果交接尚未结束的处理；128 仅包含尚未被处理者领取的等待任务，不含处理中。普通、准备、分片和收尾共享这两个进程级限制，不按 Job／Instance 叠加。回报组件满时，已完成业务的处理者继续占用这 32 个处理位置之一，等待交接后再领取下一项；业务实际退出时仍按原规则清理活动防重。已通过的回报 256／16、单条 RPC、内存接纳 ACK、共享 30 秒停机宽限及丢结果边界不变；不增加跨全生命周期额度，不预定 Go 线程或并发原语结构。[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)已另行通过，[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，其余恢复与分片协议继续闭合；已批准的通信协议仍须生成及 SQL／实现验证；本次仅记录设计，尚未授权执行模块编码。

此前已确认的回报安排：[执行器结果保留与发送安排](execution.md#executor-result-queue)及[回报初值](execution.md#reporting-defaults)已于 2026-10-28 通过。回报组件默认保留 256 条，包含待首发、待重试和在途；ACK、永久错误或 7 次应用层调用耗尽后才释放原位置。发送并发默认 16，每次 RPC 结束即释放，不等待退避。新结果以接纳时刻为最早可发送时间，重试以上次调用结束加正退避计算；按已到期的最早时间及稳定同时间顺序处理，不切首发／重试固定份额。单次 RPC 2 秒，六次退避为 1／2／4／8／16／16 秒且各上下浮动 20%，基准合计 47 秒不是保留 TTL 或完成时限；256／16 为部署配置，其余为集中内部常量。满载时有限业务处理者持有结果等待交接，不领取下一项，压力经业务等待队列传回接单，不增加全生命周期统一额度。[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)已另行通过，既有内存接纳 ACK、单条 RPC、共享 30 秒停机宽限和丢结果边界保持；本次仅记录设计，尚未授权执行模块编码。

此前已确认的派发时间预算：[单次派发时间预算与确认失败处理](execution.md#dispatch-time-budgets)已于 2026-10-28 通过：总处理预算 10 秒从取出候选起算；登记事务 2 秒、接单 RPC 3 秒、ACK 绑定或 BUSY 保存阶段 4 秒，保存最多 3 次含首次、单次 1 秒、失败间隔 200 毫秒，全部受剩余总预算及停机宽限限制。无绑定观察默认 30 秒，从数据库 `attempt_registered_at` 起算，包含提交耗时，节点间保持一致；到期仅获条件结算资格，不证明旧业务停止或承诺恢复时限。BUSY 独立延期 5 秒上下浮动 20%，保存后结束本次派发处理，由既有到期补查发现，不占协程等待。提交不明或预算耗尽且尚未发送时放弃本次发送，不免费重派；元数据保存不重发业务，接单 RPC 与业务 Context 独立。原生客户端与执行器 Server 方法超时须协调，不能被 go-zero 默认两秒提前截断。调度端结果保存初值已另行通过；无绑定扫描参数已按[观察到期扫描与结算安排](execution.md#unbound-recovery-defaults)另行通过，登记批量等未定事项继续闭合，不冻结 Go 编码结构，尚未授权执行模块编码。

### 当前实现状态

普通阶段的适用范围验收及历史 CI 已完成。分片阶段 1～5 已接通完整生命周期并完成本机已确认范围验收，提交 `14897bd` 的 CI 全成功；最新范围、覆盖率与保留限制见[分片 CI 收尾](sharded-readiness-review.md#sharded-ci-closeout)。管理页面、历史清理、具体 System 生产处理器和其他完整发布余项继续保留。

#### 普通负载收尾时的历史明细

以下保留普通负载验收后、提交 `5b07ec0` 前的实现和验证记录。其中“增量尚未提交”“没有新 CI”描述当时状态，当前提交结论以上文为准。

普通阶段 1～5 核心开发及阶段 6 适用范围验收已完成，当前测试／文档增量可提交；实际验证与边界见各次实施记录。公共执行协议、处理器注册、通信适配、双 Server 宿主初始化／退出、Task 公共表与模型、STOPPING 活跃判断、组仓储／缓存及四个组管理 API／RPC 已实现。普通首次启动、真实派发、ACK／BUSY／明确拒绝的有限保存、业务结果队列与保存、父完成检查及两项最小执行查询已接入生产组件，实际范围见[父完成与查询记录](ordinary-readiness-review.md#ordinary-completion-progress)。普通重试登记保留原 Task 与父截止，沿用冻结配置和原候选编号，只有明确提交的新尝试才可发送；首次 WAITING 与到期重试共用一个扫描器、有界等待及派发工作协程，组客户端按组复用并在删除后清理，见[普通重试记录](ordinary-readiness-review.md#ordinary-retry-progress)。本轮未知结果结算组件最多复核 16 条候选，一次 JOIN 加载必要事实，每条 Task 独立提交，已完成真实 PostgreSQL 竞争及与重试／父完成的组件组合验证，见[未知结果结算记录](ordinary-readiness-review.md#ordinary-unknown-progress)。本轮已新增独立串行的自动观察扫描并接入 ServiceContext 启停，使用固定整趟扫描时间与复合游标，跨运行只保留位置、重新取得处理预算与必要事实，范围与验证见[自动观察扫描记录](ordinary-readiness-review.md#ordinary-unbound-scan-progress)；已绑定 Task 仍等待结果或整轮终止，不提前重派。公共执行器 Runtime 支持四阶段处理器调用、活动期防重、取消和有限回报，System 已内嵌独立执行入口，但生产注册表仍为空；该调用能力不表示分片生成、存储或阶段推进已经实现。本轮已接通 StopInstance RPC／API、权限迁移、后台数据库取消队列、地址枚举及远端通知、STOPPING Leader 发现与宿主启停，见[停止运行链路记录](ordinary-readiness-review.md#ordinary-stop-runtime-progress)；普通自动超时及 Leader RUNNING 复核已接通，见[最新记录](ordinary-readiness-review.md#ordinary-timeout-recovery-progress)，阶段 5 普通核心开发完成；阶段 6 的 HTTP 进程闭环与跨宿主回报见[首个阶段 6 记录](ordinary-readiness-review.md#ordinary-process-e2e-progress)，Windows 信号退出及调度端 kill 后接续见[生命周期记录](ordinary-readiness-review.md#ordinary-process-lifecycle-progress)，预算耗尽及已 ACK 结果见[结果丢失记录](ordinary-readiness-review.md#ordinary-result-loss-progress)，执行器骤停、etcd 恢复及本机检查见[收尾记录](ordinary-readiness-review.md#ordinary-local-checkpoint)，首次 CI 失败及测试修正保留在[历史记录](ordinary-readiness-review.md#ordinary-ci-correction)；提交 `7f9d32e` 的 Unix E2E、Linux race 与全部 CI 已通过，有效覆盖率见[最新 CI 记录](ordinary-readiness-review.md#ordinary-ci-success)。本机[代表性负载](ordinary-load-validation.md)和受影响回归已通过，见[最新收尾记录](ordinary-readiness-review.md#ordinary-load-closeout)；本轮测试／文档增量尚未提交，没有该增量的新 CI，提交后需继续验证。上游退出竞态及批准暂缓的覆盖范围继续跟踪。各增量的实际测试及限制按实施记录分别保存，不以历史 CI 或本地结果替代本次证据。

- 产品与工程边界：DogX 一体化后台内置分布式调度，同仓单 Go Module、独立 `job-rpc` 与 System BFF；公共执行组件在仓库内复用，暂不做对外 SDK，不另建平台注册／心跳中心。确认目录见 [ADR-0009](../../adr/0009-scheduler-domain-and-etcd.md#project-layout)，不改变已定分布式能力或首批范围。
- 已通过的基础：总体架构、B1 和 B2 的规则不重选；首轮初始化、同 Job 记账、低频恢复、切主边界、编辑启用及本轮配置隔离也已确认。规则通过不等于模块字段、SQL、接口和测试已经完成。
- 触发模块已通过的交接决定：[持久化交接](data-contracts.md#trigger-durable-handoff)与[执行器组生命周期及本批依赖](data-contracts.md#executor-group-lifecycle)。WAITING Instance 和本轮配置是交接事实，本地提示不替代持久化；正式组表包含 `deleted_at`，第一版无组启停，仅逻辑删除并保护引用、旧轮次及未收敛 Task。
- 已通过的总审材料：[总审主稿](trigger-readiness-review.md)、[SQL 对照](trigger-sql-review.md)、[HTTP／RPC 对照](trigger-interface-review.md)和[ADR-0009](../../adr/0009-scheduler-domain-and-etcd.md)。接入细则已应用到代码、规范和 CI 配置；完整组凭据／连接缓存失效和 Task 执行仍留在相应模块。
- 字段类型修订：已落实[字符串选型规范](../../development/database-migrations.md#column-types)，Job／Instance 短字符串采用明确容量的 VARCHAR(n)，参数及自由诊断摘要保留 TEXT，结构化快照保留 JSONB，详见[修订结果](data-contracts.md#string-field-types)。输入／查询／错误契约及迁移已实现并验证，未对既有开发业务库执行迁移。
- 已定容量初值：[Job 总容量 1024、创建并发 32、单次处理 3 秒](execution.md#trigger-runtime-defaults)，自动工作协程数派生，自动与手动共用进程额度。恢复候选上限 1024 与队列容量含义不同，查询 5 秒也不替代处理 3 秒或错失容忍 5 秒；实现后压测校准，不据此宣称已达到竞品吞吐。
- 最近通过的推进决定：按模块执行“详细设计 → 评审 → 编码和测试”；将触发与分片生成拆开安排，首批止于可恢复的 Instance 持久化交接，不缩减第一版分片能力。
- 普通主路径已确认先取得本地派发容量，再将首次开始、截止时间、RUNNING 状态、唯一普通 Task 与首次执行信息同事务保存；[执行器接入](architecture.md#processor-layout)与[RPC 入口边界](architecture.md#rpc-boundaries)已通过。System 首版内嵌 `system-rpc`，组件创建独立执行器 Server，不新增 Worker，但保留以后拆分能力；调度端管理与回报 Server 分开。组 Token 保留，管理 RPC 鉴权由部署方使用 go-zero 原生配置，不另建权限机制。处理器职责与输入输出、派发、结果上报和取消响应语义均已通过；取消定向客户端仍不跨通知缓存。接入配置、组依赖及启动退出规则已收口，继续原顺序的 Task 持久化与状态推进；RPC 编码与存储交叉核对，其余参数在后续排队、并发与恢复总审闭合。
- [派发契约](data-contracts.md#dispatch-acceptance)采用一次 RPC 派发一个 Task 的当前尝试；请求携带身份、处理器与能力要求、阶段输入及本轮截止时间。接纳 ACK、明确未接单的 BUSY、其他明确拒绝与通信结果不明分别处理。只在本进程排队及执行期间防止同一尝试重复接纳，业务实际退出即释放活动记录，不等待结果回报 ACK，不增加终态去重缓存。既有执行预算和故障规则不变；字段编码、错误编码、具体 SQL 及并发、队列长度和批量参数仍在后续实现协议中闭合。
- [结果上报契约](data-contracts.md#result-report-contract)首版采用单条异步上报：独立回报处理者在有限并发下选取回报组件中已经到期的结果，每次 RPC 发送一个 Task 当前尝试的结果，一次响应确认已接收、稍后重传或永久拒绝；可并发多个单条请求，不增加凑批窗口、批内部分接收或重组逻辑。接收线程不查 Task 表，成功入队不代表持久化或状态机采纳；未确认结果有限重传，永久不可交付结果记录错误和指标后释放，不重新执行业务。既有结果丢失、绑定和整轮超时规则不变；[普通结果条件更新与状态分支](data-contracts.md#task-result-persistence)已通过；执行器结果保留数、发送并发及重传参数按[回报初值](execution.md#reporting-defaults)执行；调度端结果保留、保存并发、单条处理超时及重试组织已按[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)通过；[回报错误分类与无效结果处理](data-contracts.md#result-error-classification)已通过，[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)已通过，Proto 生成、驱动适配及 SQL／实现验证仍待完成。
- [执行器处理流程](execution.md#executor-processing-flow)已通过：接单后进入有界业务等待队列，由有限并发业务处理者执行，再交有界结果队列，由独立、有限并发的回报处理者发送。结果队列满时，当前处理者持有已完成结果等待交接，暂不领取下一项业务；独立回报继续，交接后再继续消费。业务实际退出即清理活动防重，不等待交接或回报 ACK；取消信号仍不等于退出，停机沿用共享宽限与晚返回保护。执行器结果保留数、发送并发和重传参数已按[回报初值](execution.md#reporting-defaults)确认；每执行器进程的[业务处理初值](execution.md#executor-business-defaults)也已通过为并发 32、等待 128，均可部署调整；32 包含正在执行业务与已完成但交接未结束的处理，128 仅计尚未领取的等待任务。所有处理器类型共享，不按 Job／Instance 叠加；此前 64／100 仍不是业务默认值。
- [排队、并发与满载处理](execution.md#capacity-model)六条原则已通过：正常效率、故障保护与可维护性共同决定方案；重点约束主要入口和慢资源；用各段队列、有限并发及有界批次说明整体积压；允许交接等待向上游传递压力；职责独立不等于每一步独建队列或处理池；结合完整流程选择 Go 实现并验证有效吞吐、结果落库、排队延迟和资源占用。不新增接单到回报的统一总量管理，也不把抽象“容量控制”作为额外组件；具体队列满载、拒收和继续处理行为以正文为准。
- 已保存结果引起的分片完成检查，继续由同节点按 Instance 去重合并，与结果报文是否合批无关；不重新修改既有检查窗口测试起点。只有压测证明 RPC 成为瓶颈后才评估有界顺手合批，第一版不预留批量模式开关。
- [本地派发实现方式](execution.md#bounded-dispatch)已通过：有界队列配固定长期工作协程，每调度进程默认 64 个、部署可调；与 Instance 创建并发 32 独立。取出候选后才保存本次执行信息，入队不占执行机会；队满暂停补充，不创建无界等待协程。派发处理结束即可继续消费，不等远端业务完成，不额外建立“槽位对象管理”。普通首次启动、Task 创建及首次执行信息合并事务已通过；无派发容量时保持 WAITING，明确提交成功后直接发送，提交不明仍不补发。[排队、并发与满载处理](execution.md#capacity-model)另已通过共享派发、小批轮转和适度缓冲，[派发供给初值](execution.md#dispatch-supply-defaults)已进一步确定等待总量 256、每页最多 128、1 秒上下浮动 20% 的空闲补查、每 Instance 先补 8 项及连续无有效进展 4 页退避；登记批次与具体实现继续落实，不据此授权编码。
- [Task 公共字段与单例约束](data-contracts.md#task-field-contract)已通过：统一 `job_task`，`attempt_no` 与 `used_attempts` 分开，BUSY 只返还后者；执行器身份采用每次进程启动生成的 UUIDv4，直连地址 VARCHAR(255)。结果 JSONB 与摘要 TEXT 分开，首次／当前登记／终态时间与审计字段分开；普通、准备、收尾使用部分唯一索引，各类型每轮至多一条，分片不受此单例限制。不复制公共配置，不增加软删除、物理外键、Attempt 历史表、负责人、租约或关闭字段。分片输入和编号、其余状态写入 SQL、查询索引及完整迁移继续设计，新增验收项尚未运行。
- [普通首次创建与重复推进 SQL](data-contracts.md#ordinary-start-sql)已通过：按主键锁单条 Instance，锁后另读一次数据库时间，父行条件更新必须恰好一行，再插入普通执行中 Task；单例冲突整笔回滚，重复入口不发送旧尝试。仅本事务实际新建 Task 且明确提交成功时发送 RPC。Job 触发仍用调度进程 `time.Now()`，Instance／Task 执行计时仍用数据库时间；首次启动事务已实现，实际验证及尚未接通的派发范围见[阶段 3 记录](ordinary-readiness-review.md#ordinary-stage3-progress)，完整[对应验收](verification.md#ordinary-start-sql-checks)随运行链路继续落实。
- [取消响应](data-contracts.md#cancel-response-contract)只确认本次本机取消操作已处理，无匹配活动也成功；排队转运行与取消协调，运行槽位实际退出才释放。永久错误不重试，通信故障有限重试同一目标且不占 Task 执行预算，耗尽或进程停止后释放客户端；不修改父终态、不替代子状态收敛，不增加停止确认查询或长期取消缓存。通信参数按已批准项执行，消息表达按[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)执行；Proto 生成及 SQL 实现验证仍待完成，不改变执行器启动身份的既有范围。

- [执行接入配置与来源](architecture.md#execution-configuration)已通过：两个进程保留现有顶层 RPC 配置，Job 新增 `ExecutionRPC`，System 新增包含接入组名、Token、`Server` 和 `ReportClient` 的 `JobExecutor`；复用原生配置类型，派发客户端从部署 etcd 配置与数据库组 Key 构造，不在配置文件中重复维护组映射。组删除后的客户端清理、启动退出规则均已通过，不将局部设计通过当作第二组完成或编码授权。
- [组身份与字段规则](data-contracts.md#executor-group-lifecycle)已明确：数据库 `id` 是 Job／Instance 引用的稳定主键；组 `name` 是执行器配置的接入身份，不仅用于展示；etcd 发现 Key 单独手工维护。`name` 与发现 Key 创建后均不可修改，不增加更名或发现目标热切换协议。
- [组 Token 格式与存储](operations.md#group-credentials)已确定：后端安全随机生成 UUIDv4，去掉连字符后为 32 个小写十六进制字符，组表明文保存；不增加应用层加密或部署加密密钥。
- [组配置缓存](operations.md#group-cache)已调整确认：各节点启动加载，每 60 秒串行完整读取未删除组的执行字段，成功原子替换、失败保留旧快照；不做组增删差异同步，也不新增 etcd 组变更通知。命中缓存直接使用，未命中时查询单个组，同组并发未命中合并查询；存在且未删除则缓存并继续本次认证和处理，查不到或数据库失败均不免认证放行。接受故障期间配置撤销滞后，不承诺固定内存上限；完整刷新不重建全部 RPC 客户端。
- 组删除后的客户端清理已通过：各节点在 60 秒全量查询成功并替换快照后，检查本地已创建的组客户端；组仍存在则原样复用，不存在则先移出客户端缓存，再调用 `client.Conn().Close()`。全量查询失败既不替换快照，也不清理客户端；不新增数据库查询、独立清理线程、引用计数或排空等待，也不等待远端业务结束。回源、客户端创建与清理在本地协调，迟到旧结果不能重新挂回已清理客户端。接受少量在途 RPC 中断，按通信结果不明处理，不能推断执行器未接单或业务已取消；定向取消仍不跨通知缓存。该清理不改变组配置完整快照替换方式，尚未批准实现。
- [第一版不提供 Token 重置](operations.md#group-credentials)已确定：仅创建组时生成，无组编辑入口，已有 Token 不可修改，不新增重置入口或自动轮换；轮换不再作为第一版待办或开工条件。
- [Token 查看方式](operations.md#group-credentials)已通过：普通列表不返回，组详情按既有 API／RBAC 权限返回完整 Token，页面默认遮挡并可显示、复制；无组编辑入口且不提交已有 Token 的修改，不另建查看密钥 API，不采用仅创建时展示一次。[日志保护方案](operations.md#token-log-protection)已通过：不主动记录凭据，RPC 正文及 HTTP／配置／错误字符串分别控制；不增加公共日志包，具体控制仍须实现验收。
- [启动与就绪](operations.md#startup-readiness)已通过：初始化所需依赖、处理器和队列后才接收请求；就绪不等待当选 Leader 或所有执行器组在线。退出宽限按整个进程总计默认 **30 秒**、可配置，两套 Server 与所有清理共享剩余预算，不为每个组件重新计时。
- 退出行为已通过：调度端停止触发、派发及恢复工作生产，停止新回报接纳，结束任期，并在剩余宽限内有限善后落库。执行端拒绝新单且不伪装成 BUSY，已接纳的排队和运行任务可在剩余宽限及原整轮期限内继续，结果回报保持工作；宽限到期停止启动排队任务、取消运行 Context、结束等待，由宿主结束进程。公共组件不调用 `os.Exit`，不强杀 goroutine；退出允许内存结果丢失，但不等同于业务取消、失败或重派授权。接入配置与组依赖的规则收口，下一项为 Task 持久化与状态推进，执行模块尚未获编码授权。

- [接单 ACK 绑定 SQL](data-contracts.md#executor-binding-sql)已通过：按原请求 Task／Instance／执行编号、执行中状态及两项绑定均空的条件，原子补存 ACK 进程身份与同次派发 Peer 地址。零行停止，不补查、不推断原因、不自动取消；不额外读锁父，父 STOPPING 而子仍执行中可补绑定，不恢复状态或保证补取消。保存失败仅在当前固定工作协程内有限重试元数据，耗尽或退出释放，不重发业务；[派发时间预算](execution.md#dispatch-time-budgets)已确定保存阶段最多 4 秒、最多 3 次含首次、单次 1 秒及间隔 200 毫秒，仍受剩余总处理预算限制。旧尝试通过 `status` 离开执行中关闭，下一次登记才增加 `attempt_no`，即使旧业务后来成功也不改写当前状态。单次条件绑定仓储已实现并有真实 PostgreSQL 回归，有限保存重试及实际 ACK 处理尚未装配，见[阶段 3 记录](ordinary-readiness-review.md#ordinary-stage3-progress)；完整[专项验收](verification.md#executor-binding-sql-checks)继续落实。

- [普通 Task 结果保存与状态转换](data-contracts.md#task-result-persistence)及[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)于 2026-10-29 完成后续评审：每条以一次 JOIN 取得 Task／Instance 归属、认证组及本轮冻结的重试上限、间隔；最终按 Task／Instance／原尝试、执行中状态及空或匹配的绑定身份条件 UPDATE、独立提交。成功、失败待重试、失败耗尽分别原子保存结果、目标状态和该条数据库处理时间，不扣次数、不递增编号、不补绑定；不设整批总超时或超时拆批。零行正常结束、不补查，也不宣称此前已保存成功；数据库故障或提交不明仅保留对应原条目，不重放其他已明确成功条目。默认组件保留 512 条待首次处理、待重试和在途结果，保存并发 8，单条处理超时 2 秒，三项可部署调整；一次处理结束即释放处理能力，退避保留原条目原位置。确认单条处理正常结束并提交后才提示已核实的 Instance 检查，不统一锁父或直接重派。普通单条保存组件及仓储已于 2026-11-04 实现，JOIN 在写事务外，UPDATE 内共用一次数据库取时；队列、退避消费者和 Job 结果生产入口已接线，父完成仍未接线；失败等待重试不表示新尝试调度已完成。本轮最终验证见[Job 生产接线记录](ordinary-readiness-review.md#ordinary-result-wiring-progress)，完整[验收清单](verification.md#task-result-persistence-checks)继续落实；[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)与[派发／取消错误码映射](data-contracts.md#dispatch-cancel-errors)的批准边界不变。

- [普通 Task 等待重试与新尝试登记](data-contracts.md#ordinary-retry-registration)已通过：候选保留原执行编号，固定工作协程取得容量后，在单父行短事务中锁后取数据库时间、检查父状态与截止和本轮配置，再按等待重试、原编号、到期及剩余次数条件更新原 Task。编号和已用次数各加一，清空旧绑定、Data 与终态字段，首次开始和整轮截止不变；最近结果说明保留，不作为当前尝试已有结果的依据。只有本次返回新登记记录且明确提交成功才发送，零行退出，提交不明不补发；明确 BUSY 返还后的机会可重新占用。[专项验收](verification.md#ordinary-retry-registration-checks)尚未运行。本项已收口，未知结算的后续决定见下项。

- [普通 Task 绑定缺失的未知结果结算](data-contracts.md#ordinary-unknown-settlement)已通过：既有有界恢复按[扫描与结算安排](execution.md#unbound-recovery-defaults)每短批最多 16 条读取父关系和本轮配置，并取一次数据库时间；逐条独立提交，不合并多条 Task 事务；前置观察父运行且未到期后，按原候选编号、执行中、双绑定为空及观察到期条件更新，使用当前行已用次数决定等待重试或最终失败。清 Data、写新的结果不确定诊断，不增加或返还机会，不改变编号、登记时间或父状态。前置父校验不锁父，允许终止交错但不恢复父或授权重派；确认更新并提交后才提示检查，零行结束，单项写入技术失败或提交不明允许游标越过该已尝试项但立即结束本次运行，后续新一趟重读仍符合条件的失败项，不越过未处理候选。[专项验收](verification.md#ordinary-unknown-settlement-checks)尚未运行，观察期已按[派发时间预算](execution.md#dispatch-time-budgets)确定默认 30 秒，原因码仍待定。本项已收口，BUSY 返还的后续决定见下项；不重新讨论已定故障政策，不授权编码。

- [普通 Task 明确 BUSY 的条件返还与延期](data-contracts.md#ordinary-busy-settlement)已通过：收到原派发的专用未接纳响应后，由当前固定工作协程按原 Task／Instance／编号、执行中、正已用次数及双绑定为空的条件，一次更新关闭尝试、返还一次机会并设置延期；编号、登记时间、首次开始及父截止不变，摘要更新为容量拒绝诊断。不额外读锁父，零行退出不补查；保存失败或提交不明只有限重试原响应写入，不重发业务，耗尽或退出释放，丢失证据不能猜测补返。[专项验收](verification.md#ordinary-busy-settlement-checks)尚未运行；[派发时间预算](execution.md#dispatch-time-budgets)已确定独立延期 5 秒上下浮动 20%，保存与 ACK 共用有限重试初值，不占派发协程等待延期。本项已收口，停止超时与统一子收敛的后续决定见下项；未授权编码。

- [Instance 停止超时与子 Task 收敛](data-contracts.md#instance-termination-sql)已于 2026-10-23 修订通过：新增非终态 `STOPPING = 6`，进入时在单父行短事务冻结 `end_reason`，人工停止最终 CANCELED、整轮超时最终 FAILED，`finished_at` 仅最终结束时设置；不新增停止标记、命令表或可靠通知表。每一批实际 Task 插入、Post 创建和新尝试授权与停止决定通过同一父行协调，STOPPING 后不再创建新工作。提交后独立开展按 Instance 有界子取消及去重目标尽力通知；保留子有效终态、执行编号、已用次数和绑定，仅确认无未结束 Task 后结束父 Instance。管理成功只代表停止决定已提交，不代表子状态处理或远端业务退出；提交不明不得报成功。STOPPING 仍参与阻塞丢弃、Job／组删除保护与历史保留，活跃谓词及索引需改为 `status IN (1, 2, 6)`。当前代码、枚举和迁移尚未修改，相关验收未运行。

- [Instance 完成与终态规则调整](behavior.md#terminal-order)已通过：普通与分片共用完成机制，具备完整最终结果且父仍 RUNNING 时允许在截止后按结果结束；正常完成父锁外读稳定事实，再条件 UPDATE，不称无锁。自动超时先尝试仅终态判定，不生成分片、Post 或重试，明确不具备完成条件且到期才进入 STOPPING；技术失败不得当作未完成。人工停止提交 STOPPING 及人工停止原因，已有停止决定和终态不覆盖，接受迟到有效结果及不同入口的条件竞争，不承诺全局完成优先。新工作创建／派发／重试仍受原截止与必要短事务保护；独立有界数据库恢复不依赖下一次 Cron 或 Job 启停。不新增时间轮、500ms 宽限或 Task 独立超时。[完成条件、父 UPDATE、摘要及超时衔接](data-contracts.md#instance-completion-contract)已通过，完整分片生成／查询和索引仍待评审，未授权编码。

- [Instance 完成判定与条件写入](data-contracts.md#instance-completion-contract)已收口：按本轮快照和阶段事实判断，正常父 UPDATE 仅以主键及运行中状态为条件，不加截止谓词；影响一行还须确认提交，零行或技术失败不能转为超时。正常结束时间为 UPDATE 的数据库求值时间；普通及 Pre 最终失败复制最终 Task 摘要，分片使用平台统计及 Post 结果，不拼接全部子结果文本。自动超时只有明确结果未齐才进入既有父短事务，锁内复核父状态和时间，不重扫全轮 Task；接受期间结果到齐而超时先保存的竞争。全分片最终失败仍在父状态和期限允许时创建已配置 Post，Post 缺失不能当作未配置；阶段由 Task 等事实推导，不新增权威阶段字段，也不采用状态与阶段拼接展示，具体页面另审。生成封口字段与完整分片查询仍未闭合，新增验收尚未运行，仅记录设计、未授权编码。

- [恢复资源分工](execution.md#recovery-resources)已通过：待开始 Instance 与到期可派发 Task 复用已有 64 个派发工作协程；阶段推进、正常完成及到期处理复用已有 Instance 检查组件；无绑定观察结算由各节点各自一条串行、不重叠的后台流程处理；STOPPING 的子 Task 状态收敛采用其独立的本地处理安排，不共用未知结算流程。扫描不占派发或结果消费者，结果保存仍独立。不按五类候选另建五套池；真实恢复派发共享容量并须有限补充，数据库等资源仍共享。[可派发候选容量、临时分页及无进展退避](execution.md#dispatch-supply-defaults)已单独通过，不表示所有恢复类别的 SQL、索引、扫描周期和处理预算整体批准；RUNNING Instance 统一复核和 STOPPING 停止恢复的周期、分页及索引分别按已通过决定执行，STOPPING 的并发、容量及处理份额另按[已批准初值](execution.md#stopping-processing)执行；RUNNING [检查提示安排与初值](execution.md#instance-check-defaults)已通过；[RUNNING 扫描预算与恢复补充](execution.md#instance-recheck-defaults)及[观察到期扫描与结算安排](execution.md#unbound-recovery-defaults)已收口，不能套用派发的 64 个工作协程或互相借用参数；其余未定事项仍待总审；[专项验收](verification.md#recovery-resource-checks)尚未运行，执行模块仍未获编码授权。

- [可派发候选查询与索引](data-contracts.md#dispatch-recovery-query)已通过：WAITING Instance 按 ID 有界查找，复用 `(id) WHERE status = 1`；等待重试 Task 按最早派发时间及 ID 排序，关联过滤父仍运行且未截止，返回 Task ID、Instance ID 和原尝试编号，新增 `(next_dispatch_at, id) WHERE status = 3`。查询使用数据库时间，不锁、不写、不占机会、不读取当前 Job 配置；有限投递已有派发组件，处理前仍按授权事务核验，不保存永久消费游标。[临时分页](data-contracts.md#dispatch-candidate-pagination)及[派发供给初值](execution.md#dispatch-supply-defaults)已通过，WAITING 固定本趟 ID 上界，重试返回分页时间并固定本趟数据库时间；分片首次待派发的封口条件、其余恢复查询和剩余并发、排队和批量参数仍待设计；[专项验收](verification.md#dispatch-recovery-query-checks)尚未运行，新增索引尚未迁移。绑定缺失及 RUNNING Instance 的后续决定见下项，不编码。

- [绑定缺失观察到期候选及索引](data-contracts.md#unbound-recovery-query)已通过：执行中、双绑定为空、当前尝试登记已超过观察期，关联父仍运行、时间有效且未截止，按登记时间及 ID 限量读取并保留原尝试编号。处理时重新取得数据库时间、批量复核父和本轮配置，交后台恢复结算而非直接派发，不用摘要或 Data 是否为空判断。采用精确部分索引，接受正常任务也会短暂经过索引，以及绑定／状态变更的写入、WAL、HOT 适用限制和旧版本清理成本，读写收益留待压测验证。观察期按[派发时间预算](execution.md#dispatch-time-budgets)默认 30 秒，[扫描与结算安排](execution.md#unbound-recovery-defaults)已补齐各节点串行流程、结束后等待 10 秒 ±20%、每页 128、复核 16、单次操作 1 秒及每次运行 5 秒；临时游标固定本趟边界并跨运行接续，写失败可越过已尝试项但立即结束、未处理项不可越过，[专项验收](verification.md#unbound-recovery-query-checks)尚未运行；RUNNING Instance 的后续决定见下项，不编码。

- 原独立父 Instance 到期候选查询及 `(deadline_at, id) WHERE status = 2` 索引计划已由下项[统一复核查询](data-contracts.md#instance-recheck-query)替代，旧查询及索引均尚未实现；保留 `deadline_at` 字段、有效截止授权及结果优先的终态协议，不另设短周期到期扫描。2026-10-21 当时的批准事实保留在历史记录。

- [RUNNING Instance 统一复核](execution.md#instance-recheck-direction)及[候选查询与索引](data-contracts.md#instance-recheck-query)已通过：现有 Leader 每 2 分钟启动或续扫，每趟固定数据库 `scan_now`，`eligible_before = scan_now - 4 minutes`；筛选 `status = 2 AND started_at < eligible_before`，每页 256 条，按 `(started_at, id)` 键集分页，使用 `(started_at, id) WHERE status = 2` 部分索引，不按截止过滤、不跨轮扫描 Task、不按当前 Job 启停或删除过滤。首次开始时间不因阶段或重试重置。有限投递 Leader 本节点既有检查组件，正常结果仍在提交节点即时推进，不等待 Leader 或内部 RPC 转发。阶段遗漏推进、完整结果结束及到期处理统一检查，其他候选职责不并入。

- 统一复核一趟固定上界、未完成则下周期继续，不重叠扫描；仅接纳或合并成功才推进临时游标，达到恢复补充门槛或队满时停在首未接纳项，在本周期剩余预算内等待空间再继续；查询失败结束本周期并保留位置、下周期重试，已接纳但检查失败时，检查期间的新提示仍按[本地合并规则](execution.md#instance-check-defaults)再安排，否则由后续通知或新一趟扫描再发现。扫到尾后新一趟从头，不能把临时位置作为永久消费高水位；内存只保留有界一页。新 Leader 就绪后立即从头有限扫描，仍遵守 4 分钟门槛；失主停止新发现，已接纳检查按既有数据库并发协议结束，不转移游标或队列、不增加 Instance owner。没有有效 Leader 时本周期查漏暂停，正常结果推进不停止。

- 统一检查保留阶段完整性与已有授权协议：父不再 RUNNING 时退出，STOPPING 交停止处理；完整最终结果先尝试正常结束；仅明确 `NotReady` 且实际处理数据库时间已到截止才进入原超时协议；未到期且满足阶段条件才恢复后续推进，否则退出。技术错误、正常完成 UPDATE 零行及提交不明均不能转超时；到期不新建分片、Post 或重试，绑定后结果丢失不授权提前重派。无积压时首次周期检查约在开始后 4～6 分钟，短超时也受门槛约束；正常长任务仍可重复检查，积压、故障或切主可延长等待，不承诺准时终态或恢复 SLA。不引入延迟队列、时间轮、分桶、`next_check_at` 或 owner。[检查提示安排与初值](execution.md#instance-check-defaults)已通过；[RUNNING 扫描预算与恢复补充](execution.md#instance-recheck-defaults)已通过，不保证持续拥塞下恢复准入，[专项验收](verification.md#instance-recheck-query-checks)尚未运行，查询与索引尚未实现；[停止协议](data-contracts.md#instance-termination-sql)于 2026-10-23 修订通过：先持久化 `STOPPING = 6` 和停止原因，统一分批取消未结束 Task，确认全部子记录终态后再保存父终态；远端取消独立、尽力执行。STOPPING 候选由同一 Leader 每 2 分钟独立发现，不套用 4 分钟开始时间门槛，新 Leader 就绪后有界检查；使用既有后台恢复容量、独立预算和公平遍历。[STOPPING 取消竞争与容量](execution.md#stopping-processing)于 2026-10-26 收口：每批父主键 NOWAIT 后才查子 Task，竞争失败退出且不立即重排；已确定扫描、本地容量、子批及公平份额初值，不扩展到派发竞争。[远端取消通知](execution.md#cancel-notification-capacity)及[地址查询与索引](data-contracts.md#cancel-target-query)同日通过：按 Instance 向去重地址通知，不逐 Task；新停止决定确认提交后独立交接，不随子批或两分钟恢复重发，父终态不撤销已接纳通知。[派发候选合并与跨节点竞争](execution.md#dispatch-contention)同日单独通过：普通首次启动与已有 Task 新尝试登记改为父行 NOWAIT，本地合并重复候选、同 Instance 登记不自相竞争，失败跳过本次同组且不立即重排；补查独立抖动、已接纳候选分组后不同起点处理。[排队、并发与满载处理](execution.md#capacity-model)于 2026-10-28 通过：共享派发与 Instance 小批轮转保持；同日明确[执行器处理流程](execution.md#executor-processing-flow)：有界业务等待队列、有限业务并发、有界结果队列及独立有限回报并发，队满由交接等待向接单传递压力；按六条原则优先采用简单实现，不新增跨阶段统一配额，不提前固定 Go 并发原语。同日[派发候选容量、分页与退避](execution.md#dispatch-supply-defaults)另已通过，保持 RUNNING 检查组件独立范围；同日另已通过[单次派发时间预算](execution.md#dispatch-time-budgets)：总处理 10 秒、登记事务 2 秒、接单 RPC 3 秒、响应保存阶段 4 秒，保存最多 3 次含首次、单次 1 秒及间隔 200 毫秒；无绑定观察 30 秒、BUSY 独立延期 5 秒上下浮动 20%，各阶段受剩余预算限制。同日通过[执行器回报安排与初值](execution.md#reporting-defaults)：回报组件默认保留 256 条结果，覆盖待首发、待重试和在途；最多 16 次 RPC 并发，单次调用结束即释放发送能力，退避继续保留原结果位置。按已到期的最早可发送时间选择，同时间稳定排序，不切首发／重试固定份额；单次 RPC 2 秒、最多 7 次应用层调用含首次，六次退避为 1／2／4／8／16／16 秒、各上下浮动 20%。256／16 为部署配置，其余为内部常量，47 秒是基准退避合计而非 TTL。同日通过[执行器业务处理初值](execution.md#executor-business-defaults)：每执行器进程默认业务处理并发 32、业务等待队列 128，均为可调整的部署配置。32 包含业务运行及业务已完成但结果尚未交接完的处理，128 只计尚未领取的等待任务、不含处理中；普通、准备、分片和收尾共用，不按 Job／Instance 叠加。2026-10-29 后续通过[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)：每节点组件默认保留 512 条待首次处理、待重试和在途结果，保存并发 8，单条一次处理超时 2 秒，三项均可部署调整。每条关联查询 Task／Instance 归属、认证组及本轮冻结重试配置，再按原尝试、执行中状态及绑定身份条件 UPDATE、独立提交，不设整批总超时或超时拆批。一次处理结束即释放处理能力，退避保留原条目原位置；按已到期最早可处理时间及稳定同时间顺序选择，退避基准 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%，可恢复故障不按次数或 TTL 丢弃。[回报错误分类与无效结果处理](data-contracts.md#result-error-classification)已通过，[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)与[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)均已通过；[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，Proto 已生成，SQL 和完整运行装配验证仍待完成；其余恢复参数、状态查询与分片协议继续闭合，结果无效判定和技术故障保留按[已定分类](data-contracts.md#result-error-classification)执行，不沿用 STOPPING 数值，未授权编码。

- [STOPPING 恢复查询与取消索引](data-contracts.md#stopping-recovery-query)已通过：`idx_job_instance_stopping` 采用 `(id) WHERE status = 6`，每趟取得当时 STOPPING 的最大 ID 作为固定 `upper_id`，按 `id > after_id AND id <= upper_id ORDER BY id LIMIT page_size` 有限遍历；仅候选接纳或合并成功后推进游标，不等待实际取消完成，队满不越过拒收项。未扫完下周期续扫，扫到尾后新一趟从头；低 ID 后进入 STOPPING 可在下一趟发现，切主丢弃本地位置并重新开始。该游标不是永久消费高水位。

- 子取消每批另开短事务，先按父主键 `FOR UPDATE NOWAIT` 并复核 STOPPING，再按单个 `instance_id` 查询 `status IN (1, 2, 3)` 的 Task，按 ID 排序、最多 256 条；`idx_job_task_unfinished_instance` 采用 `(instance_id, id) WHERE status IN (1, 2, 3)`，空批时在当前父事务内复用索引完成最终 `NOT EXISTS` 检查。不全局扫描 Task，不读取全轮 ID 或历史结果内容，不维护 Task 游标或使用 OFFSET；下一批重新取得父锁后读取当前最前面的未结束记录。竞争失败或技术错误退出本次处理，不立即重排，处理中合并的旧提示也不能触发紧接着重抢；成功且未完成者在最多 4 批或达到 2 秒后轮转队尾。单条 SQL 超时 1 秒不代表事务、锁或连接已经释放，提交不明不按成功或确定回滚处理。完整初值及公平边界见[取消竞争与容量](execution.md#stopping-processing)。该索引覆盖所有未结束 Task，须计入正常写入、状态变更和清理成本；远端地址查询及通知容量按下项独立决定，新增索引和验收均未实现或运行。

- [远端取消通知](execution.md#cancel-notification-capacity)及[地址查询与索引](data-contracts.md#cancel-target-query)同日通过：按 Instance 向去重地址通知，不逐 Task；新停止决定确认提交后独立交接，不随子批或两分钟恢复重发，父终态不撤销已接纳通知。使用 `idx_job_task_instance_address (instance_id, executor_address) WHERE executor_address IS NOT NULL`，不按子状态过滤，不保存整轮地址集合；地址键集每页最多 32、交接后队尾、接纳后前移、队满暂停。通知有独立的两个 256 总容量及 1 个读取／16 个发送协程，单 SQL／RPC 各 1 秒、最多 3 次总尝试、间隔 1 秒、单目标 5 秒、Instance 从接纳起 60 秒；不与数据库取消的 256／2／256 额度混用。查询失败停后续枚举，已接纳目标继续原预算；期限到停止新工作但不保证硬时刻释放。周期恢复、重复停止读取旧决定不自动重启通知，提交不明确认仍可能重复；本地合并，不设永久通知标记。尚未编码、创建地址索引或运行[专项验收](verification.md#cancel-notification-checks)。

### 首批本地验证记录（2026-10-16） {#trigger-local-verification}

环境为 Windows、Go 1.27.0、PostgreSQL 18.6 和 etcd 3.5.21。使用独立测试数据库及临时 Schema、独立 etcd 子进程；未对既有开发数据库执行迁移。为降低本机资源占用，Go 测试使用 `GOMAXPROCS=2`、`-p 1`。

| 验证 | 实际结果 |
| --- | --- |
| 全仓普通测试 | `go test -p 1 -shuffle=on -count=1 ./...` 通过，另已生成全仓本地单元覆盖率 |
| Job 域真实依赖集成 | Job 数据层、迁移、RPC 装配与调度集成全部通过；System 增量迁移及独立账本检查通过 |
| 故障恢复 | 实际撤销租约、反复切主、在途数据库行锁、Watch 压缩、etcd 进程重启及实际一分钟兜底均通过；两个关键切主／重连用例追加连续三次运行通过 |
| 规则规模功能验证 | 实际 PostgreSQL＋etcd 下完整加载 500 条规则，100 个同计划时间的到期 Job 各创建一条 WAITING Instance，400 个未来 Job 不提前创建；另有超过 1024 条规则不截断检查 |
| 契约与生成 | `system.api` 校验通过；按固定命令重新生成 API／Job RPC，生成物与契约一致 |
| HTTP 与静态检查 | 八条 Handler 包内边界回归通过（该包语句覆盖 100%）；`go vet -p 1 ./...`、全仓普通 lint、Job 集成标签 lint 通过，后两者均为 0 issues |
| 本地语句覆盖率 | 新增 Job 域非生成产品代码合并单元／集成后为 **91.44%（1026／1122）**；调度运行包 93.0%、事务服务 86.7%、Repository 88.3%、RPC Logic 99.4% |

上述覆盖率是本地 Go **语句**口径，排除生成代码、入口和测试辅助代码，不等同于 Codecov 全项目／patch 行覆盖率；本批 CI／Codecov 结果以对应提交的工作流报告为准，不用本地结果替代。Windows 本机未运行 Linux race，也未重跑依赖 Redis 的 System 集成和认证 E2E；既有 CI 检查保留，Job 的 PostgreSQL／etcd 测试已加入工作流。

规模用例是功能 smoke，不是正式吞吐、长时间内存稳定性或竞品对标验收；正式容量指标仍须按已定流程确认资源及门槛后压测。下一模块为普通 Task 执行闭环，先补齐该模块必要详细设计，不直接扩写执行器和派发代码。

### 已提交版本的 CI 验证（2026-10-17） {#trigger-ci-verification}

提交 [`9689b35`](https://github.com/tokyolab/dogx/commit/9689b35ab4140acdb856e680c26dd54948dbdc47) 已推送至 main，[CI 39014812415](https://github.com/tokyolab/dogx/actions/runs/39014812415) 全部成功。该提交仅补充规范及英文核心契约注释，没有改变运行逻辑；测试验证的是包含此前实现与重构的完整提交。

| 检查 | 实际结果 |
| --- | --- |
| Go lint | 通过 |
| Windows tests | 全仓普通测试通过 |
| Linux fast checks | 模块校验、API 契约、race 与覆盖率测试通过 |
| PostgreSQL 18／Redis 7.4／etcd | 普通及 integration 标签测试通过，认证／RBAC E2E 通过 |

[同提交 Codecov](https://app.codecov.io/gh/tokyolab/dogx/commit/9689b35ab4140acdb856e680c26dd54948dbdc47) 行覆盖率：全项目合并 **92.15%（6096／6615）**，unit **74.24%（4911／6615）**，integration 标记 **92.15%（6096／6615）**。integration 任务包含普通和集成标签测试，不是纯集成测试的独立贡献；E2E 没有单独收集覆盖率。纯注释提交没有 patch 可覆盖行，patch 覆盖率不适用。

上述结果关闭了首批 Linux race 及 CI 待复核项，不代表 Task 执行已实现，也不替代正式容量与长期资源稳定性验收。前节保留 2026-10-16 的本地历史记录，不将其覆盖率当成本次 CI 结果。

## 触发模块开工清单 {#trigger-readiness}

**首批交付目标：多调度节点运行时，按已定规则正确创建 Instance 和保存本轮配置，并在重复回调、切主及数据库故障后按规则恢复。** 本模块完成不等于业务处理器已经能够执行，也不等于整个定时任务功能完成。

### 本批范围与交接

- 输入是 Job 的持久化配置、启停状态和触发进度，以及 Cron 回调、Leader 恢复检查或规则注册后提供的检查提示。覆盖必要的启停事务和配置同步；页面布局不属于本批前置设计。
- 触发入口按错失、阻塞规则决定创建或跳过；自动创建时，Instance、本轮执行配置及 Job 进度在同一事务内保存。跳过仅在确认进度提交后写服务日志，不新增数据库记录。
- 对下一模块的交接是**已持久化的待开始 Instance 及其执行配置**，不是只有一条内存通知。后续节点必须能够据此发现并接续；本批不把创建时间改成首次有效开始时间，不启动整轮执行倒计时。
- 本批 Job 输入、执行器组引用校验、手动触发的创建、防重和阻塞契约已确认；八条管理 API 按[本批接口清单](trigger-interface-review.md)实现，通过 System BFF 调用 Job RPC，不直接访问调度数据。

首批尚无业务推进消费者，待开始 Instance 仍参与同 Job 阻塞判断；测试通过受控数据覆盖前轮结束等场景，不新增临时超时或自动结束规则。管理入口的缺省值按已通过接口契约实现，不能用测试配置替代产品默认值。

### 开工前的三组设计

以下跟踪三组设计的已定输入与剩余工作，不把部分规则通过等同于整组完成。每组形成可审查方案，重大取舍仍逐项讨论，常规字段和实现细节整包审查。

| 设计包 | 需要形成的内容 | 完成标志 |
| --- | --- | --- |
| 触发判定，已通过 | 自动身份、5 秒边界、到期区间合并及向未来推进、六字段 Cron 与调度进程时区；手动启停条件、当次参数、成功响应及有限防重规则 | 按已定协议实现，本地功能验证通过 |
| 规则同步与运行，已通过 | 已定串行差异同步、Watch 衔接、任期生命周期与首次批量装载、触发容量与时间规则；etcd 官方选主、租约初值、键空间、发布反馈、I/O 预算及监听恢复已确认。统一 Cron 计算继续按已定解析、时区与时间来源实现，不另起政策议题 | 适配器、单元及真实 etcd 集成测试通过；最新 CI 的 Linux race 通过 |
| 持久化与验收，已通过 | 本批规则、输入查询及交接均已通过；SQL、HTTP／RPC 字段编号、测试映射和 ADR 细则已随总审确认 | 总审及开工授权已完成，使用隔离测试数据库验证，不对现有开发数据库执行迁移 |

正文分别落在[触发规则](behavior.md#trigger-sync)、[触发记账](execution.md#trigger-accounting)、[数据契约](data-contracts.md)和[验证计划](verification.md#trigger-module-delivery)。清单跟踪是否完成，不复制第二套技术规则。

### 第二组收口与第三组推进 {#runtime-group-review}

本次收口核对已通过决定，不是增加一套规则：

| 运行主题 | 权威设计 |
| --- | --- |
| 资格、键空间、通知及依赖 I/O | [etcd 接入约定](execution.md#etcd-integration) |
| 首次装载、反复切主及资源释放 | [任期生命周期](execution.md#leader-runtime-lifecycle) |
| 串行差异加载与遗漏兜底 | [配置重载](execution.md#job-config-reload)及[独立触发恢复](behavior.md#trigger-recovery) |
| 容量、手动共享额度与过载 | [触发容量](execution.md#trigger-capacity)及[部署初值](execution.md#trigger-runtime-defaults) |
| 统一计算、异常隔离及时间运维 | [Cron 契约](behavior.md#cron-contract)、[时间来源](behavior.md#trigger-clock)及[时区运维](operations.md#scheduler-timezone) |

第二组设计收口不等于模块可开工，也不表示功能测试已运行。第三组的字段、事务、索引、管理输入查询、持久化交接与最小组依赖均已通过；落地 SQL、HTTP／RPC 消息、测试映射及 ADR 现已汇入[开工总审](trigger-readiness-review.md)。只在出现影响正确性的新矛盾时增加实质决策，不逐字段拆审批。

Job 触发遗漏恢复查询采用单批限量、每分钟重读，原 Job 分页及跨分钟游标草案不采用；该决定不限制后续已通过的 RUNNING Instance 统一复核分页。Task 表和完整派发／回报 RPC 仍在后续对应模块。Job／Instance 本批字段、触发事务、约束、索引及 Job 逻辑删除已实现；组表包含软删除字段并排除已删组，完整组管理及删除入口尚未开放。总审附件作为设计依据，契约生成及迁移已在本批落实并通过隔离环境验证；历史清理在对应模块细化。

### 两道完成检查

**开工检查**确认设计是否足够支持本批实现，不要求此时已有通过的功能测试：

- 已通过：三组中的规则和契约，包括持久化交接与最小组依赖；Task 执行和完整组接入等后续设计有明确模块归属。
- 已通过：SQL 对照、HTTP／RPC 字段号与错误清单、十二类验收矩阵及实现顺序。
- 已应用：本次实现范围、附件和 ADR 接入细则；规范、代码生成、迁移及 CI 衔接已在开工授权后落实，验证结果单独记录。

触发、普通 Task 和分片生命周期均已完成对应实施及已记录验证；管理页面、历史维护和其他完整发布余项继续按原定模块顺序推进，不新增泛化的架构审批轮次。

**交付检查**确认实现是否达到本模块目标：代码完成，正常和故障测试通过，持久化交接可验证，设计及接入说明与实现一致。文档链接、渲染检查不能替代这些功能测试。

### 后续实施顺序

| 批次 | 可验收的成果 | 开工前设计边界 |
| --- | --- | --- |
| 触发闭环，已提交并通过本地及 CI 验证 | 正确、可恢复地创建待开始 Instance | 上述三组设计均已通过；正式负载与过载验收单独推进 |
| 普通 Task 执行闭环，已完成适用范围验收 | 从待开始 Instance 到普通 Task 的派发、执行、回报、终态及恢复 | 六阶段实施及对应 CI 已完成，见[普通验收记录](ordinary-readiness-review.md#ordinary-load-closeout)；本次分片提交继续覆盖普通回归 |
| 分片执行闭环，已完成已确认范围验收 | Pre、分片生成与执行、Post 及平台结束形成完整链路 | 五阶段实现、本机查询／代表性负载及提交 `14897bd` 的 CI 已完成，见[分片 CI 收尾](sharded-readiness-review.md#sharded-ci-closeout)；完整性能和第一版发布余项不在本轮结论内 |
| 管理运维与整体验收 | 第一版管理、查询、历史维护、接入说明及故障和容量验证齐全 | 对应管理及清理契约、部署资源和量化验收条件 |

这是实施顺序，不是减少第一版范围。管理与组接入中被前一批实际依赖的部分提前设计和交付，完整页面可后续完善；手动执行、停止、分片和历史维护仍按[第一版范围](scope.md)交付。每个模块重复“设计、评审、实现、测试”，已通过开工评审的模块即可编码验证，其他模块继续设计。普通闭环的开发验收与正式发布分开，完整执行能力待分片完成后正式发布，见[发布边界](#ordinary-integration-contract)。

### 普通 Task 执行闭环的设计安排 {#ordinary-task-design}

本模块开发验收成果：持久化的 WAITING Instance 能生成唯一普通 Task，交给真实业务处理器执行，经过结果回报与保存，由调度服务更新 Instance 终态；同时覆盖重试、停止、有限整轮超时和节点重启恢复。普通任务不增加 Pre／Post，不改为整轮固定负责人，也不对外发布 SDK。

| 设计包 | 本包审查重点 | 当前状态 |
| --- | --- | --- |
| 主路径与职责 | 待开始 Instance 的推进入口、首次开始及唯一普通 Task、提交后派发、结果与 Instance 结束的职责 | 已通过；取得派发容量后，首次开始、首个普通 Task 与首次执行信息同事务保存，明确提交后直接派发 |
| 执行器接入与调用契约 | 仓库内公共执行组件与业务处理器、必要组发现及认证、派发／上报／取消 RPC、接收与完成的边界 | [注册](architecture.md#processor-layout)、[双 Server](architecture.md#rpc-boundaries)、[处理器共同契约](data-contracts.md#processor-contract)、[派发接单语义](data-contracts.md#dispatch-acceptance)、[结果上报契约](data-contracts.md#result-report-contract)及[取消处理与响应](data-contracts.md#cancel-response-contract)已通过；Post 只提供公共输入与统计，不增加查询 RPC。接入配置、组依赖及启动退出规则已收口，第二组[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)已通过；[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，普通六阶段适用范围验收已完成，Job 双 Server 与 System 内嵌执行器、完整普通链路均已接通；分片扩展及当前远端结果见[分片 CI 收尾](sharded-readiness-review.md#sharded-ci-closeout) |
| 持久化、故障与开工检查 | Task 字段及索引、事务和条件写入、两端排队与并发、恢复参数及公平性、实现分工与正常／故障测试映射 | 普通 Task 公共字段与单例唯一索引、首次创建与重复推进 SQL、接单 ACK 绑定 SQL、普通结果保存与状态转换、普通重试登记 SQL、普通未知结算 SQL、普通 BUSY 返还 SQL、停止超时与统一子取消 SQL、Instance 完成判定及父条件写入／摘要／超时衔接、恢复资源分工已通过；[普通完成读取](data-contracts.md#ordinary-completion-query)及[结束原因字典](data-contracts.md#execution-end-reasons)已通过；最小接入／管理契约已通过，普通六阶段适用范围验收已完成；完整分片查询及四阶段推进已在后续[分片五阶段](sharded-readiness-review.md#sharded-implementation-order)实现验证 |

以上是三类交付材料，不承诺三次对话即可完成。已通过的 B1／B2 是输入，不重复审批；新提案通过后才更新正文为已定规则。本模块适用契约与测试预期闭合并获开工授权后即可编码，不等待后续分片和完整管理页面设计完成；普通模块完成验收不等于单独正式发布，遵守下述完整执行发布边界。

以下保留普通执行开工期间的历史评审顺序和当时状态，不作为当前未完成项；对应实现已随普通六阶段与分片五阶段完成，当前验证结论见[CI 收尾](sharded-readiness-review.md#sharded-ci-closeout)。

| 顺序 | 提案 | 所属设计组与边界 |
| --- | --- | --- |
| 1，语义已通过 | 业务处理器与执行协议 | 第二组。普通、分片、可选 Post 的处理器契约，以及派发接单、结果单条异步上报、取消处理与响应语义已收口；落库后按 Instance 合并完成检查独立保留。消息表达及装配已按[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)收口，已批准通信参数继续执行；[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，对应状态写入实现仍待落实，不表示已实现 |
| 2，规则已收口 | 接入配置与组依赖 | 第二组。[配置布局与来源](architecture.md#execution-configuration)、[组 Token 格式、明文存储与查看](operations.md#group-credentials)、[日志保护](operations.md#token-log-protection)、[组名及发现 Key 不可变](data-contracts.md#executor-group-lifecycle)、[60 秒完整刷新与未命中回源](operations.md#group-cache)、组删除后的客户端清理及启动退出已通过，Token 重置已排除第一版。[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)已通过，公共协议生成与存储适配须在实现时交叉验证；[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已通过，[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，尚未批准的参数继续闭合。沿用 System 首版内嵌、以后可拆 Worker 的决定，不重新选择发现架构；规则收口不表示执行模块完成或实现已获授权 |
| 3，进行中 | Task 持久化与状态推进 | 第三组。Task 公共字段与单例唯一索引、[普通首次创建与重复推进 SQL](data-contracts.md#ordinary-start-sql)、[接单 ACK 绑定 SQL](data-contracts.md#executor-binding-sql)、[普通结果保存与状态转换](data-contracts.md#task-result-persistence)、[普通重试登记 SQL](data-contracts.md#ordinary-retry-registration)、[普通未知结算 SQL](data-contracts.md#ordinary-unknown-settlement)、[普通 BUSY 返还 SQL](data-contracts.md#ordinary-busy-settlement)及[停止超时与统一子取消 SQL](data-contracts.md#instance-termination-sql)已通过；[Instance 完成判定、父条件写入、摘要及超时衔接](data-contracts.md#instance-completion-contract)也已通过；[恢复资源分工](execution.md#recovery-resources)、[可派发候选查询及索引](data-contracts.md#dispatch-recovery-query)、[绑定缺失观察到期查询与索引](data-contracts.md#unbound-recovery-query)及[运行中 Instance 统一复核及查询索引](data-contracts.md#instance-recheck-query)已通过：Leader 每 2 分钟启动或续扫，筛选本趟数据库时间之前已开始超过 4 分钟的 RUNNING Instance，每页 256 条；阶段遗漏推进、正常结束及到期处理共用既有检查组件。[停止协议](data-contracts.md#instance-termination-sql)于 2026-10-23 修订通过：先持久化 `STOPPING = 6` 和停止原因，统一分批取消未结束 Task，确认全部子记录终态后再保存父终态；远端取消独立、尽力执行。STOPPING 候选由同一 Leader 每 2 分钟独立发现，不套用 4 分钟开始时间门槛，新 Leader 就绪后有界检查；使用既有后台恢复容量、独立预算和公平遍历。[STOPPING 取消竞争与容量](execution.md#stopping-processing)于 2026-10-26 收口：每批父主键 NOWAIT 后才查子 Task，竞争失败退出且不立即重排；已确定扫描、本地容量、子批及公平份额初值，不扩展到派发竞争。[远端取消通知](execution.md#cancel-notification-capacity)及[地址查询与索引](data-contracts.md#cancel-target-query)同日通过：按 Instance 向去重地址通知，不逐 Task；新停止决定确认提交后独立交接，不随子批或两分钟恢复重发，父终态不撤销已接纳通知。[派发候选合并与跨节点竞争](execution.md#dispatch-contention)同日单独通过：普通首次启动与已有 Task 新尝试登记改为父行 NOWAIT，本地合并重复候选、同 Instance 登记不自相竞争，失败跳过本次同组且不立即重排；补查独立抖动、已接纳候选分组后不同起点处理。[排队、并发与满载处理](execution.md#capacity-model)于 2026-10-28 通过：共享派发与 Instance 小批轮转保持；同日明确[执行器处理流程](execution.md#executor-processing-flow)：有界业务等待队列、有限业务并发、有界结果队列及独立有限回报并发，队满由交接等待向接单传递压力；按六条原则优先采用简单实现，不新增跨阶段统一配额，不提前固定 Go 并发原语。同日[派发候选容量、分页与退避](execution.md#dispatch-supply-defaults)另行通过：等待总量 256、查询最多 128、空闲补查 1 秒上下浮动 20%、每 Instance 先补 8 项、连续无有效登记进展最多 4 页，临时分页与本轮成功登记恢复预算规则已确认。同日另已通过[单次派发时间预算](execution.md#dispatch-time-budgets)：总处理 10 秒、登记事务 2 秒、接单 RPC 3 秒、响应保存阶段 4 秒，保存最多 3 次含首次、单次 1 秒及间隔 200 毫秒；无绑定观察 30 秒、BUSY 独立延期 5 秒上下浮动 20%，各阶段受剩余预算限制。同日通过[执行器回报安排与初值](execution.md#reporting-defaults)：回报组件默认保留 256 条结果，覆盖待首发、待重试和在途；最多 16 次 RPC 并发，单次调用结束即释放发送能力，退避继续保留原结果位置。按已到期的最早可发送时间选择，同时间稳定排序，不切首发／重试固定份额；单次 RPC 2 秒、最多 7 次应用层调用含首次，六次退避为 1／2／4／8／16／16 秒、各上下浮动 20%。256／16 为部署配置，其余为内部常量，47 秒是基准退避合计而非 TTL。同日通过[执行器业务处理初值](execution.md#executor-business-defaults)：每执行器进程默认业务处理并发 32、业务等待队列 128，均为可调整的部署配置。32 包含业务运行及业务已完成但结果尚未交接完的处理，128 只计尚未领取的等待任务、不含处理中；普通、准备、分片和收尾共用，不按 Job／Instance 叠加。2026-10-29 后续通过[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)：每节点组件默认保留 512 条待首次处理、待重试和在途结果，保存并发 8，单条一次处理超时 2 秒，三项均可部署调整。每条关联查询 Task／Instance 归属、认证组及本轮冻结重试配置，再按原尝试、执行中状态及绑定身份条件 UPDATE、独立提交，不设整批总超时或超时拆批。一次处理结束即释放处理能力，退避保留原条目原位置；按已到期最早可处理时间及稳定同时间顺序选择，退避基准 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%，可恢复故障不按次数或 TTL 丢弃。[回报错误分类与无效结果处理](data-contracts.md#result-error-classification)已通过，[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)与[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)均已通过；[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，Proto 已生成，SQL 和完整运行装配验证仍待完成；其余恢复参数、状态查询与分片协议继续闭合，结果无效判定和技术故障保留按[已定分类](data-contracts.md#result-error-classification)执行，不沿用 STOPPING 数值，沿用本轮修订后的 B1／B2 规则 |
| 4，排队与并发、恢复初值及回报错误分类已通过，普通执行静态检查及开发安排已完成 | 排队、并发、恢复与开工总审 | 第三组。[排队、并发与满载处理](execution.md#capacity-model)六条原则和[执行器处理流程](execution.md#executor-processing-flow)已通过，结果队满由业务处理者等待交接，独立回报继续，不新增跨阶段统一配额。[派发供给初值](execution.md#dispatch-supply-defaults)、[候选临时分页](data-contracts.md#dispatch-candidate-pagination)和[派发时间预算](execution.md#dispatch-time-budgets)保持；[回报安排与初值](execution.md#reporting-defaults)、[业务处理初值](execution.md#executor-business-defaults)及[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)也已通过。[回报错误分类与无效结果处理](data-contracts.md#result-error-classification)已通过；通信消息与装配已通过，普通完成读取及结束原因字典已通过，最小接入／管理契约已通过，静态开工检查与六阶段开发安排已完成，阶段 1 已开始；协议和通信组件已落地，SQL、宿主装配及完整功能验证随所属阶段落实 |

当前“恢复候选查询与索引”已确认[资源分工](execution.md#recovery-resources)、[可派发候选查询与索引](data-contracts.md#dispatch-recovery-query)、[绑定缺失观察到期候选与索引](data-contracts.md#unbound-recovery-query)及[RUNNING Instance 统一复核查询与索引](data-contracts.md#instance-recheck-query)。第 4 类中的遗漏推进、正常结束和到期候选已统一，Leader 的 2 分钟周期、4 分钟开始时间门槛及每页 256 条已通过；旧的独立到期查询与索引计划被替代，不改其他类别职责。STOPPING 的独立候选与最终结束已于 2026-10-23 另行确认，不能把停止恢复套进 RUNNING 年龄过滤或派发容量。[停止协议](data-contracts.md#instance-termination-sql)于 2026-10-23 修订通过：先持久化 `STOPPING = 6` 和停止原因，统一分批取消未结束 Task，确认全部子记录终态后再保存父终态；远端取消独立、尽力执行。STOPPING 候选由同一 Leader 每 2 分钟独立发现，不套用 4 分钟开始时间门槛，新 Leader 就绪后有界检查；使用既有后台恢复容量、独立预算和公平遍历。[STOPPING 取消竞争与容量](execution.md#stopping-processing)于 2026-10-26 收口：每批父主键 NOWAIT 后才查子 Task，竞争失败退出且不立即重排；已确定扫描、本地容量、子批及公平份额初值，不扩展到派发竞争。[远端取消通知](execution.md#cancel-notification-capacity)及[地址查询与索引](data-contracts.md#cancel-target-query)同日通过：按 Instance 向去重地址通知，不逐 Task；新停止决定确认提交后独立交接，不随子批或两分钟恢复重发，父终态不撤销已接纳通知。[派发候选合并与跨节点竞争](execution.md#dispatch-contention)同日单独通过：普通首次启动与已有 Task 新尝试登记改为父行 NOWAIT，本地合并重复候选、同 Instance 登记不自相竞争，失败跳过本次同组且不立即重排；补查独立抖动、已接纳候选分组后不同起点处理。[排队、并发与满载处理](execution.md#capacity-model)于 2026-10-28 通过：共享派发与 Instance 小批轮转保持；同日明确[执行器处理流程](execution.md#executor-processing-flow)：有界业务等待队列、有限业务并发、有界结果队列及独立有限回报并发，队满由交接等待向接单传递压力；按六条原则优先采用简单实现，不新增跨阶段统一配额，不提前固定 Go 并发原语。同日[派发候选容量、分页与退避](execution.md#dispatch-supply-defaults)另行通过：等待总量 256、查询最多 128、空闲补查 1 秒上下浮动 20%、每 Instance 先补 8 项、连续无有效登记进展最多 4 页，临时分页与本轮成功登记恢复预算规则已确认。同日另已通过[单次派发时间预算](execution.md#dispatch-time-budgets)：总处理 10 秒、登记事务 2 秒、接单 RPC 3 秒、响应保存阶段 4 秒，保存最多 3 次含首次、单次 1 秒及间隔 200 毫秒；无绑定观察 30 秒、BUSY 独立延期 5 秒上下浮动 20%，各阶段受剩余预算限制。同日通过[执行器回报安排与初值](execution.md#reporting-defaults)：回报组件默认保留 256 条结果，覆盖待首发、待重试和在途；最多 16 次 RPC 并发，单次调用结束即释放发送能力，退避继续保留原结果位置。按已到期的最早可发送时间选择，同时间稳定排序，不切首发／重试固定份额；单次 RPC 2 秒、最多 7 次应用层调用含首次，六次退避为 1／2／4／8／16／16 秒、各上下浮动 20%。256／16 为部署配置，其余为内部常量，47 秒是基准退避合计而非 TTL。同日通过[执行器业务处理初值](execution.md#executor-business-defaults)：每执行器进程默认业务处理并发 32、业务等待队列 128，均为可调整的部署配置。32 包含业务运行及业务已完成但结果尚未交接完的处理，128 只计尚未领取的等待任务、不含处理中；普通、准备、分片和收尾共用，不按 Job／Instance 叠加。2026-10-29 后续通过[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)：每节点组件默认保留 512 条待首次处理、待重试和在途结果，保存并发 8，单条一次处理超时 2 秒，三项均可部署调整。每条关联查询 Task／Instance 归属、认证组及本轮冻结重试配置，再按原尝试、执行中状态及绑定身份条件 UPDATE、独立提交，不设整批总超时或超时拆批。一次处理结束即释放处理能力，退避保留原条目原位置；按已到期最早可处理时间及稳定同时间顺序选择，退避基准 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%，可恢复故障不按次数或 TTL 丢弃。[回报错误分类与无效结果处理](data-contracts.md#result-error-classification)已通过，[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)与[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)均已通过；[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过，Proto 已生成，SQL 和完整运行装配验证仍待完成；其余恢复参数、状态查询与分片协议继续闭合，结果无效判定和技术故障保留按[已定分类](data-contracts.md#result-error-classification)执行，不沿用 STOPPING 数值；分片首次待派发及生成封口相关查询仍随生成协议补齐，不凭尚不存在的字段提前宣称完成。[本地检查提示安排及初值](execution.md#instance-check-defaults)已通过；[RUNNING 扫描预算与恢复补充](execution.md#instance-recheck-defaults)也已通过；其余类别周期、批量及处理参数仍留在相应排队、并发与恢复总审，不重新讨论已收口的终态规则。

### 普通执行最小接入、管理契约与发布边界 {#ordinary-integration-contract}

**本项于 2026-11-02 通过。** [正式组字段与最小管理入口](data-contracts.md#executor-group-management-contract)已确定组名／Key 的全表唯一和不复用、完整字段及创建／详情／列表／删除四入口，无组编辑。2026-11-04 按产品版本兼容规则撤销此前旧组回填及历史 NULL 例外；当前持久化按完整非空字段实施。[最小执行查询与停止契约](data-contracts.md#execution-management-contract)已确定 Instance 详情、所属 Task 分页和整轮停止的输入输出与错误边界；详情读冻结事实，停止反馈已确认状态，不承诺业务退出。

普通执行是可以独立开发与验收的里程碑；其适用设计及开工检查闭合并获授权后即可编码，不等待分片生成及完整管理页面全部设计完成。普通阶段在隔离环境完成真实 PostgreSQL、etcd、RPC 及故障验证；**普通验收完成不等于单独正式发布，完整执行能力待分片执行完成后再供业务正式使用。** 其他第一版管理、运维和整体验收范围不因本项缩减。

这是对先前“普通执行可单独交付”的发布安排调整，不改变普通优先的实施顺序。现有管理代码已支持两种执行模式，Instance 冻结配置也可能保存分片模式；如果单独发布只支持普通的版本，必须额外处理新触发、存量启用配置及冻结轮次。现决定不为开发中间阶段引入临时模式开关、候选过滤索引或过渡状态，不改变既有两种模式的管理契约，也不将分片轮次当普通执行或长期跳过后声称已交付。

接受的代价是完整执行能力正式上线晚于普通模块验收；收益是避免为短期能力不足增加长期维护的门禁与数据处理逻辑。全局执行历史列表、Task 业务 Data 详情及完整页面仍由后续管理模块完成；不把它们全部提前作为普通闭环的开发前置条件。

当前已完成[普通执行开工检查](ordinary-readiness-review.md)，按[六阶段安排](ordinary-readiness-review.md#ordinary-implementation-order)实施；阶段 1～5 普通核心已完成，阶段 6 进行中。实现已推送，首次 CI 的 Linux race 与 PostgreSQL 集成失败；本轮测试同步和 EXPLAIN 证据修正已完成本机验证，上游 go-zero 退出 race 按用户决定保留，详见[CI 修正记录](ordinary-readiness-review.md#ordinary-ci-correction)。远端 E2E、有效覆盖率与代表性负载尚未完成。设计确认、实现和整体验收分别记录。

同时只保留一个进行中的提案。每轮先说明当前提案、已通过部分及本轮需要决定的内容；澄清留在原提案，超出范围的问题归入上表后续项，不插入新提案。用户的“通过”只批准当轮明确讨论的部分，不自动代表整组完成。提案收齐后先记录并明确结束，再进入下一项；只有发现具体新证据及影响，才重新评审已通过决定。重要取舍提供核实后的竞品比较，常规字段集中审查，不逐字段增加审批。

主路径结论见[普通任务执行流程](execution.md#ordinary-flow)，对应[首次开始与派发验收要求](verification.md#dispatch-registration-checks)、[Task 字段验收](verification.md#task-field-checks)、[首次创建 SQL 验收](verification.md#ordinary-start-sql-checks)、[绑定 SQL 验收](verification.md#executor-binding-sql-checks)、[结果保存验收](verification.md#task-result-persistence-checks)、[普通重试登记验收](verification.md#ordinary-retry-registration-checks)、[普通未知结算验收](verification.md#ordinary-unknown-settlement-checks)、[普通 BUSY 返还验收](verification.md#ordinary-busy-settlement-checks)、[停止超时与统一收敛验收](verification.md#instance-termination-sql-checks)及[完成判定与条件写入验收](verification.md#instance-completion-checks)仍待实现测试。当前已通过[单次派发时间预算与确认失败处理](execution.md#dispatch-time-budgets)及[专项验收要求](verification.md#dispatch-time-budget-checks)、[派发候选容量、分页与退避](execution.md#dispatch-supply-defaults)及[专项验收要求](verification.md#dispatch-supply-checks)、[排队、并发与满载处理](execution.md#capacity-model)、[派发候选合并与跨节点竞争](execution.md#dispatch-contention)、首次启动合并事务及其 SQL、ACK 绑定 SQL、普通结果保存与状态转换、普通重试登记 SQL、普通未知结算 SQL、普通 BUSY 返还 SQL、停止超时与统一子取消 SQL、Instance 完成条件及父 UPDATE／摘要／超时衔接、派发并发默认 64、普通 Task 公共字段与单例唯一索引；通信消息与装配另按[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)通过；完整迁移、其余状态写入 SQL、查询索引、分片字段及未定参数仍须闭合；[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)已通过；[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)已另行通过，Proto 生成和运行验证尚未完成。

## B2 闭环总审 {#b2-review}

这是已通过决定的一致性总审，不是重新选择十项政策；B2 总审已通过。普通、准备、分片和收尾共用下列规则，不增加整轮负责人、Attempt 历史表、持久化消息队列、本地回报文件或额外生命周期状态。

| 场景 | 已通过规则 | 恢复与接受的代价 |
| --- | --- | --- |
| 正常登记与提交不明 | 先取得本地容量，在单 Instance 父行 NOWAIT 保护下有界登记，锁冲突退出本次同组；收到明确提交成功确认才发 RPC。记账以数据库实际提交为准 | 提交不明撤掉本地发送，事后见已登记也不补发；已登记走观察结算，真未提交且仍合格则普通条件登记。可能未运行业务却占机会 |
| 接单与绑定 | 原生负载均衡派发，ACK 提供启动身份，调度端结合实际地址条件保存绑定；有效结果可先于绑定落库 | 绑定保存失败只处理元数据，不重发业务；通常多一次行更新，缺失绑定可能无法定向取消 |
| 业务失败与执行重试 | 各 Task 独立预算，默认额外 3 次、间隔 10 秒，即原始执行加重试共 4 次执行机会；失败可形成候选，新尝试须重新通过父行授权并提交占机会 | 不额外叠加通信重派额度，明确 BUSY 可条件返还；等待重试不是执行许可，停止或到期后不再授予新尝试 |
| 无绑定且无有效结果 | 从登记的数据库时间有限观察；绑定、结果与未知结算按当前尝试及旧状态竞争 | 结算不重复扣机会，旧尝试不复活；有额度再登记新尝试，否则结果不确定失败。原业务可能已运行，幂等由业务保证 |
| 已绑定但无有效结果 | 保持当前尝试执行中，等待有效结果、人工停止或有限整轮截止 | 不增加活动探测、心跳或失联提前重派；接受等待剩余整轮时间，单片丢结果可能阻止正常 Post |
| 明确 BUSY 与返还不明 | 仅明确未接单、未入队、未执行的容量拒绝，才条件关闭当前尝试、返还一次并延期；记账处理有界有限重试 | 零重试也能延期，但响应释放、丢失或原尝试先结算后不保证补返；不承诺下次换节点，可能占用实际未执行的机会 |
| 执行器结果上报 | 回报组件默认保留 256 条待首发／待重试／在途结果，发送并发默认 16；单条 RPC 结束即释放发送能力，退避仍保留原位置。按已到期的最早可发送时间及稳定同时间顺序选择，不切首发／重试固定份额；单次 2 秒、最多 7 次应用层调用含首次，退避 1／2／4／8／16／16 秒且各上下浮动 20%，不占执行预算 | 已接收、永久拒绝或调用耗尽后释放；256／16 可配置，其余为内部常量，47 秒基准退避合计不是 TTL。满载由有限业务处理者等待交接；不依赖本地文件，共享 30 秒停机宽限，结果可能丢失 |
| 调度端接收与结果保存 | 入队即 ACK；每条 JOIN 核验归属、认证组与冻结重试配置，再按原尝试／执行中／绑定身份条件 UPDATE、独立提交。组件默认保留 512 条含首次、退避及在途，保存并发 8，单条处理超时 2 秒，三项可部署调整 | 一次处理结束释放处理能力，退避保留原位置并按到期最早处理；可恢复或性质不明的技术故障及提交不明不按次数或 TTL 丢弃，满载拒收。零行正常结束但不证明此前成功，不重放其他成功条目；共享停机宽限，进程退出仍可能丢失已 ACK 结果 |
| 结果已保存但推进中断 | 当前数据库事实驱动共同完成检查、唯一 Post 创建和平台终态更新；新阶段仍需单独取得执行授权 | 不重跑已成功 Pre／Post；完整最终结果可在父仍 RUNNING 时迟到收尾，创建新阶段不能越过截止 |
| 停止与超时 | 自动超时先尝试按完整结果结束，明确未完成且到期才保存 STOPPING 与超时原因；人工停止保存 STOPPING 与人工原因。停止决定和新授权按父行协调，全部子记录终态后才保存父 CANCELED／FAILED | 不承诺全局完成优先或业务已退出；独立有界子取消保留子有效终态，STOPPING 由 Leader 低频独立发现并交后台恢复，阻塞、删除与历史保护在父终态前持续有效 |

### 总审的保证与限制

平台保证已保存状态按当前尝试、旧状态及父终态规则推进，不承诺业务恰好执行一次，也不承诺原始执行结果无损恢复。提交报错或普通查询未见改动均不能直接证明回滚，必要核对按批读取主库，正常路径不追加写后查询。

性能取舍不隐藏：尝试登记按轮每批增加父行检查，同轮短事务串行；绑定通常另有一次行更新；结果单条上报保留每条 RPC 成本，落库后的本地合并只减少完成检查，不消除 Task 写入。队列长度、有限并发及处理及重试中的有界记录共同说明积压，结合 RPC 报文上限验证内存，不承诺整个进程的精确 RSS 上限。

**B2 总审已确认上述规则可作为后续模块设计的共同输入，终态裁决采用 2026-10-21 本轮修订后的规则。** 普通与分片共用完成机制已通过，不表示完整 SQL、接口、生成封口协议、ADR／规范衔接、业务实现或故障测试已经完成。

## 阶段与完成标准

| 阶段 | 对应章节及主要产出 | 完成标准 | 当前状态 |
| --- | --- | --- | --- |
| 1 范围与架构收口 | 第 1、2 章为主；第 7、8 章提供维护与质量边界 | 不再存在改变系统组成或第一版承诺的重大未决选择，后续问题已有归属 | 已完成，A1～A3 及整体复核均通过 |
| 2 核心详细设计 | 第 3 至 6 章；第 8 章同步补故障用例 | 各模块依赖的主路径、故障规则及跨模块契约闭合，相关 ADR 完成 | 进行中，B1、B2 规则已通过；其余按模块收口，不要求全系统细节完成才开工 |
| 3 分模块设计与实现 | 第 6、7 章细化，按协议实现模块并验证 | 每批通过开工评审后编码测试，最终第一版模块交付齐全 | 触发、普通及分片执行模块已实现，适用范围本机验收及对应 CI 通过；管理页面、历史清理和具体 System 生产处理器等其他模块仍待交付 |
| 4 整体验收 | 第 8 章及实际验证报告、接入与运维交付物 | 满足已确认功能、故障恢复及负载与过载验收标准，队列、处理并发和满载行为可说明且无阻塞交付项 | 模块功能、故障及代表性负载已有分阶段证据；第一版整体验收尚未完成，完整性能清单及其他发布余项继续保留 |

设计确认、实现完成与测试通过分别记录，不用一个完成百分比混合表示。阶段 2 与阶段 3 按模块衔接，不是全系统严格串行：本模块所需设计及相关架构前提通过后即可实现，其他模块继续设计。B1、B2 的已完成标记表示规则评审通过，不表示全部 SQL、接口和代码已经完成。

## 范围与架构评审

| 顺序 | 议题 | 依赖与落点 | 本议题停止展开的边界 |
| --- | --- | --- | --- |
| A1 已完成 | 功能范围、基准测试规模及质量方向已通过 | [第 1 章](scope.md)，补充第 7、8 章 | 性能门槛在部署资源明确后、正式压测前确认，不把基线当硬上限 |
| A2 已完成 | 动态组管理、部署与业务接入边界已通过 | A1；[第 2 章](architecture.md) | 具体发现标识格式、组生命周期及配置同步在详细设计解决，不自建负载均衡 |
| A3 已完成 | 组级机器凭据、独立 etcd 注册授权及依赖故障边界已通过 | A2；[第 2 章](architecture.md)、[第 7 章](operations.md) | 后续已明确 Token 格式、存储、查看、组缓存及启动退出，第一版不提供重置；RPC 编码与存储交叉核对，参数在后续排队、并发与恢复总审闭合，ADR 及仓库规范在实现前衔接 |
| 整体复核已完成 | 第一版承诺、系统组成、边界与后续设计安排的一致性已通过 | A1～A3；全篇交叉核对 | 没有要求重选架构的阻断，剩余协议问题进入核心详细设计 |

三个议题及整体复核均已完成，B1、B2 闭环规则总审也已通过，首批触发已经实现验证，当前推进普通 Task 模块设计。议题是工作组织，不是固定对话轮数的承诺；发现影响前提的新证据需说明原因后回到对应决策。

## 核心详细设计的后续安排

以下内容在各自模块实现前完成，不作为首批开工的全部前置条件。重大业务语义、可靠性与成本取舍先核实竞品并逐项评审；常规字段、局部实现及测试组织由架构负责人形成模块方案，供整体审查。

| 设计主题 | 正文位置 | 需要闭合的问题 |
| --- | --- | --- |
| B1 状态与生命周期（已完成） | 第 3、4、5 章 | 已确认三层状态、无独立阶段权威字段、普通单 Task／分片必需 Pre 可选 Post、空数组、终态竞争与父子收敛语义 |
| B2 Task 执行权、尝试与回报协议（已完成） | 第 5、6、8 章 | 闭环总审已通过；执行权、预算、回报和恢复规则作为后续模块的共同输入，具体实现细节按下列归属闭合 |
| 触发模块（已提交，本地及 CI 验证通过） | 第 4、5、6、8 章 | [开工清单](#trigger-readiness)中的触发判定、同步运行、持久化和功能验收已落实；[本地验证](#trigger-local-verification)及[最新 CI](#trigger-ci-verification)分别记录，正式负载与过载验收仍未完成 |
| Task 创建与分片生成 | 第 3、5、6 章 | 普通首次创建已实现；最终分片清单、整组原子生成及 Pre 首次启动／分片首次派发已通过，具体查询／事务及运行交接在分片闭环实施前落实；不再设计跨事务续写或独立完成字段 |
| 派发排队、并发与恢复调度 | 第 5、6 章 | 承接 B2，闭合到期工作、两端背压、定向取消、公平批次与恢复覆盖；不重选已定状态 |
| 回报与完成推进 | 第 5、6 章 | 承接单条异步上报、入队 ACK、易失结果边界和已定进程退出宽限，按已定[执行器回报安排与初值](execution.md#reporting-defaults)落实发送及有限重试，落实[回报错误分类与无效结果处理](data-contracts.md#result-error-classification)及善后落库实现；落实已定[检查提示生命周期与初值](execution.md#instance-check-defaults)，落实已定[RUNNING 扫描预算与恢复补充](execution.md#instance-recheck-defaults)，继续闭合阶段授权实现 |
| 跨模块契约及验证 | 第 6、8 章 | 持久化事实、索引与事务条件、RPC 与仓库内公共执行组件契约、数组容量和故障测试依据；组名及发现 Key 创建后不可修改，补齐组删除对存量 Instance／Task 及恢复依赖的影响 |
| 内部认证与部署恢复 | 第 2、6、7、8 章 | 组凭据格式、明文存储与查看、组名及发现 Key 不可变、周期完整刷新、未命中回源、删组客户端清理及启动退出已定，第一版不提供 Token 重置；RPC 编码与存储交叉核对，部署参数在后续排队、并发与恢复总审闭合，并落实 etcd 权限及[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)的生成和装配验证。在实现时验证初始化、共享 30 秒退出预算、缓存与客户端清理的并发边界；执行协议按独立 Server 装配组认证，管理／业务 RPC 保留原生鉴权配置，不另建管理权限或强制网络策略 |

已确认的本地直接派发、父行授权与终态协调、候选重试状态、提交记账、双端异步回报与入队 ACK、有效结果落库后本地合并、`EXISTS`、收尾主键单行事务和唯一约束等作为输入，不再次当成待选方案。[本地检查初值及 100ms 合并窗口](execution.md#instance-check-defaults)已通过；其余字段命名、管理 API、页面、未定参数和性能验证在对应模块或压测阶段细化。

## 评审与记录方式

架构负责人负责整体地图、依赖、讨论深度和风险；评审者审计方案与取舍。每轮说明当前模块、决策目标、已确认输入、本模块剩余开工条件及本轮不展开的事项；每组收口后更新开工清单，不只汇报下一问。技术规则与批准记录保持分层，普通字段命名不机械拆成独立审批。

技术选择采用“调研与建议 → 评审决策 → 反向核查 → 更新正文”的节奏。比较设计时使用“设计点 / XXL-JOB 已核实行为 / SnailJob 已核实行为 / DogX 建议”表格，区分参考事实、建议与已确认结论。未核实的行为不作为事实交付，参考项目未披露的内容不补猜测。

新问题按三类处理：阻塞当前决定的现在讨论；不阻塞但必须解决的记录到章节并注明解决时点；第一版以外的纳入后续范围。详细协议可以延后讨论，但不能省略；新的证据若影响已通过结论，应明确提出重新评审，不能默默改写。

每项通过后更新正文结论、本页状态与下一议题。正文保留当前有效设计，不追加聊天流水账；ADR 保存重要决策的理由和替代方案。设计索引及批准记录见[参考资料与决策索引](references.md)。

## 进度记录

以下保留各次评审当时的决定。2026-10-30 已通过[结果回报错误分类与无效结果处理](data-contracts.md#result-error-classification)，关闭此前回报错误映射及永久无效证据待定的当前口径；回报入口沿用标准 gRPC 与现有 bizerror，后台先排除明确失效结果再解析所需配置，结果内容永久不可保存须独立核实，技术错误及提交不明继续保留。历史行保留当时事实；不将本次批准扩展为派发／取消完整编码、Proto 或执行实现已完成。2026-10-29 最新通过[观察到期扫描与结算安排](execution.md#unbound-recovery-defaults)，补齐各节点独立串行、运行后等待 10 秒 ±20%、分页 128、复核 16、操作 1 秒及运行 5 秒，替代此前无绑定扫描参数待定的当前口径。本次另明确每条结算独立提交，不从结果保存的历史批准自动外推；临时游标可越过发生写入技术失败或提交不明的已尝试项，但立即结束本次运行，后续新一趟重新覆盖，未处理项不可越过。早期历史行保留当时事实，RUNNING 的接纳游标和 STOPPING 的处理规则不因此改变。2026-10-29 后续通过[RUNNING 扫描预算与恢复补充](execution.md#instance-recheck-defaults)：查询 1 秒、每周期扫描／交接 5 秒均部署可调；恢复新 ID 仅在集合占用低于总量一半时接纳，已有 ID 可合并，达到门槛或满时预算内等待。此决定替代 RUNNING 满即退出及严格来源公平的旧口径，不改 STOPPING；持续或整窗口拥塞可阻止恢复准入，早期历史行保留当时事实。2026-10-29 后续通过[Instance 检查提示生命周期与初值](execution.md#instance-check-defaults)，此前本地检查容量、并发、100ms 窗口及检查中事件合并待定的当前口径按此收口；早期记录保留当时事实，Leader 扫描／交接及恢复补充已按本段开头的同日后续决定收口。2026-10-29 同日后续完整提案已通过 [调度端结果保存安排与初值](execution.md#scheduler-result-defaults)：单条 JOIN 读取归属、认证组与本轮冻结重试配置，最终条件 UPDATE、独立提交；每节点组件保留 512 条（含待首次处理、退避和在途）、保存并发 8、单条处理超时 2 秒，三项部署可调。一次处理结束释放处理能力，原条目原位置退避，按已到期最早时间及稳定同时间顺序选择；内部退避基准 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%，可恢复故障不按次数或 TTL 丢弃，满载仍拒新。此决定替代同日较早“可批量预读”及“保存参数／组织待定”的当前口径，既有历史行保留当时事实；具体错误码映射、SQL／实现验证及其他恢复类别仍待完成。2026-10-28 后续通过六条排队与并发原则及[执行器处理流程](execution.md#executor-processing-flow)：以具体队列、处理并发和满载行为说明资源保护；结果队满时当前业务处理者等待交接，独立回报继续，不增加接单至回报的统一总量管理。早期“交接计量”等待定措辞按这一后续决定收口；同日后续又通过[执行器回报安排与初值](execution.md#reporting-defaults)，以 256 条组件内结果保留（含待首发、待重试和在途）、16 次发送并发、到期最早优先且同时间稳定、单次 2 秒和最多 7 次应用层调用替代此前回报参数待定的描述，退避不占发送能力且保留原结果位置。同日再通过[执行器业务处理初值](execution.md#executor-business-defaults)：每执行器进程业务处理并发 32、业务等待队列 128，均可部署调整；32 含运行及完成后等待结果交接的处理，128 仅计尚未领取的等待任务，所有处理器类型共享。此前“32／128 未批准”及业务并发／等待队列待定的记录，按这一后续决定替代，仍保留原评审事实；2026-10-29 又确认结果逐条条件 UPDATE、独立提交，可批量预读但不合并提交，不设整批总超时或超时拆批；失败或提交不明只保留该原条目，不重放已明确成功的其他条目。早期“后台小批提交”“整批保留”及批次共用数据库处理时间的描述按本次决定替代，历史行保留当时评审事实。调度端结果保存安排及初值按本段开头的同日后续决定执行。早期派发来源份额和公平性全部待定的描述，以 2026-10-28 的[排队、并发与满载处理](execution.md#capacity-model)为准：共享不切来源固定份额，按 Instance 小批轮转，派发具体参数以同日后续[派发候选容量、分页与退避](execution.md#dispatch-supply-defaults)为准，执行器参数及编码实现未由示例冻结。远端通知查询、容量和重试的早期待定描述，以 2026-10-26 后续[通知提案](execution.md#cancel-notification-capacity)为准；该提案不改变按 Instance 停止的粒度。STOPPING 的子取消锁规则及容量以 2026-10-26 的[单批协调提案](execution.md#stopping-processing)为准，替代早期不重新锁父及容量尚待确认的描述；不改写早期评审事实。早期 Job 触发采用数据库当前时间的表述，已由 2026-10-15 的时间来源修订替代；当前规则以[Job 触发时间来源](behavior.md#trigger-clock)为准，Instance／Task 的数据库时间决定未变。2026-10-21 通过的独立父 Instance 到期查询与索引计划，已由 2026-10-22 的[运行中 Instance 统一复核](data-contracts.md#instance-recheck-query)替代；此前未到期复核方向的待定项也按同日完整提案收口，不删除早期批准事实。早期对等恢复的笼统表述以本次职责划分为准：RUNNING Instance 周期候选发现归 Leader，2026-10-23 又批准同一低频驱动的独立 STOPPING 候选分支；正常结果及状态推进仍不由 Leader 统管。父先终态再收敛子 Task 的历史决定已由同日 STOPPING 提案替代，以下早期记录不作为当前协议。2026-10-20 早先“缓存缺组直接暂拒、不回源”的规则已由同日后续“组身份与字段规则、缓存调整”决定替代；当前组缓存以[完整刷新与未命中回源](operations.md#group-cache)为准。

| 日期 | 进展 | 未改变的边界 |
| --- | --- | --- |
| 2026-10-09 | 通过无整轮固定归属的任务驱动架构，并确认统一 Task、重试及完成推进等机制 | 具体状态机与完整恢复协议仍需设计 |
| 2026-10-10 | 确认统一 RPC 派发入口与具名处理器接入 | 部署、分组发现与内部鉴权未一并定案 |
| 2026-10-10 | 将设计草案按总览、八章正文及评审计划重组为软件设计说明书 | 未批准新的技术机制，未开始调度功能实现 |
| 2026-10-10 | 通过 A1 第一版功能范围，确定管理、查询、基础清理与接入交付及三项排除能力 | A1 使用规模与质量目标待评审；未确定历史清理机制或详细接口 |
| 2026-10-10 | 通过基准测试规模与质量方向，A1 完成，进入 A2 部署与业务接入 | 测试规模不是产品硬上限或实测成绩；性能门槛在部署资源明确后、正式压测前确认；A2 尚未定案 |
| 2026-10-10 | A2 通过页面动态执行器组管理，补充纳入第一版范围，替代静态组映射建议 | 部署边界尚待确认；组生命周期、配置同步与客户端更新留待详细设计，不增加资源编排能力 |
| 2026-10-10 | 通过 A2 其余部署与接入提案，A2 整体完成，进入 A3 | 不顺带确定组字段、标识格式、同步周期、客户端更新协议或内部认证机制 |
| 2026-10-12 | A3 整体通过：确认单进程单组与组 Token、独立 etcd 注册授权、继续使用 etcd 服务发现及按能力处理依赖故障；进入第一阶段整体复核 | 不提前确定凭据细则、完整执行权和重传协议；不把 go-zero 原生启动视为已满足就绪目标；未开始实现，整体复核与 ADR／规范同步尚未完成 |
| 2026-10-12 | 第一阶段整体复核通过，进入第二阶段 B1 状态与生命周期；补齐组变更及框架能力差距的详细设计归属，澄清资源有界与状态校验措辞 | 不把 B1 提案视为已确认状态机；尚未开始实现，执行权协议、ADR／规范同步及验证仍待完成 |
| 2026-10-12 | B1 通过：确认 Job／Instance／Task 状态、两条执行路径、合法空数组、阶段推导、超时与停止优先顺序及父子终态收敛；进入 B2 | 不新增独立阶段字段、Task 超时或停止确认状态；不承诺父子同事务终止；执行权、预算记账、通信故障及持久化协议未一并定案，未开始实现 |
| 2026-10-12 | B2 首项通过：保留生成节点本地直接派发，按本地容量批量条件登记当前尝试，提交占用机会，恢复复用条件；记录真实写入成本与提交后未发送的零重试代价 | 不改成全节点扫描后的正常派发，不增整轮归属或每 Task Redis／etcd 锁；父 Instance 终止／截止竞争、接收确认、未知结果、失联和 BUSY 仍待设计；B2 未整体完成 |
| 2026-10-12 | B2 回报方案通过：双端固定异步、有界内存队列，调度端成功入队即 ACK、后台小批量落库；替代落库后确认方案，不做本地回报文件或同步／异步开关 | ACK 不是状态成功，提交后才推进；接受进程故障丢失未落库结果，下一项明确其识别与原尝试结算，包含执行器仍在线；不新增 Task TTL 或整轮负责人，B2 未整体完成 |
| 2026-10-12 | B2 路由与绑定方向通过：保留原生 go-zero 发现及负载均衡，接单响应提供进程实例标识，结合实际地址条件持久绑定，后续控制定向调用 | 不采用调度层预先选址或另建注册中心；绑定通常增加一次行更新。用户要求单独明确绑定缺失窗口，观察、结算与迟到消息规则仍待评审，B2 未整体完成 |
| 2026-10-12 | B2 绑定缺失窗口通过：从登记的数据库时间有限观察、不重置；先到有效结果可落库，到期仅获结算资格，绑定／结果／未知结算持久化竞争；预算不重复扣减，旧尝试不复活，终止优先 | 接受原业务仍运行或已成功时仍可能重试／结果不确定失败；不增加免费重发或广播。具体观察秒数、SQL 与父子协调未定；已绑定但结果未落库的恢复仍待评审，B2 未整体完成 |
| 2026-10-12 | B2 执行器回报清理通过：同一 Task／尝试的结果有限重传，不占执行预算；次数、间隔及单次 RPC 超时用独立内部常量；ACK 或耗尽后释放，耗尽记录错误及丢弃指标 | 接受结果丢失换取长期稳定运行；具体数值、入队等待／退出及批次协议待定，不自动授权调度端丢弃写库失败结果；整轮超时默认 0 重新评审，B2 未整体完成 |
| 2026-10-12 | 明确整轮超时不允许 0 或无限时长，替代原无限默认规则；剩余政策问题按已绑定未落库、BUSY、调度端长期写库失败清理依次评审 | 具体默认时长及其他取值边界待定；未批准截止前提前结算或失联自动重派，B2 未整体完成 |
| 2026-10-12 | B2 已绑定结果等待规则通过：保持当前尝试执行中，等待有效结果、人工停止或整轮截止；第一版不增加 Task 活动查询、心跳或失联提前重派，失联／重启／回报耗尽／ACK 后丢失不单独授权重跑 | 明确失败仍沿用 Task 预算；整轮到期 Instance 失败并记录超时原因，未结束 Task 按既有取消规则收敛，不用剩余额度重试；接受宕机后等待余时、单片丢结果拖至整轮超时且无正常 Post。无绑定规则及平台推进恢复不变，不新增状态或内存等待对象；BUSY 与调度端长期写库失败清理尚未通过 |
| 2026-10-13 | B2 BUSY 规则通过：仅明确容量满且未接单、未入队、未执行时，不消耗执行预算；对当前有效尝试原子关闭、一次返还登记占用的机会并设置延期，零重试仍可延期；复用既有到期检查，下一次使用新尝试身份 | 通用通信错误及 BUSY 答复丢失不免费返还；终止优先、旧响应不复活、整轮截止不延长，原生负载均衡不保证换节点；不新增状态、队列、扫描或 Attempt 历史表。具体 SQL 与退避数值未定，最后剩余政策提案为调度端 ACK 后长期写库失败清理，B2 未整体完成 |
| 2026-10-13 | B2 调度端结果保留规则通过：已 ACK 结果遇到可恢复写库故障时有界保留、退避重试，队满拒收新结果，不按固定次数或保留 TTL 主动清掉；按排队、处理中、待重试合计条数、固定消费并发及 RPC 报文上限控制容量。当前政策问题收口，进入 B2 完整协议、持久化事实与终态竞态一致性复核 | 不套用执行器有限重报耗尽释放，不做队列总字节／MB 精确配额，不另加业务结果长度限制或 Job 页面参数；永久无效数据不无限重试堵整批，错误分类与批次隔离细节待定；进程退出仍可能丢失结果。默认数值、SQL、完整协议及验证尚未完成，B2 未整体完成，未开始实现 |
| 2026-10-13 | B2 新授权与父终态协调通过：按容量、按单 Instance 主键行锁有界登记，提交后派发；停止、超时和正常结束共用父行协调。等待重试改为候选语义，结果回报不统一锁父；截止按锁内决策点当前数据库时间判断，父终态子未收敛由共同恢复处理 | 不保证物理 COMMIT 或 RPC 精确截止，不等待全部业务退出；接受同轮短事务串行和逐轮每批父行检查成本。修正旧重试示例；下一组为提交结果不明恢复，具体 SQL、数值和批次契约未定，B2 未整体完成 |
| 2026-10-13 | B2 提交结果不明恢复通过：登记不明撤销本地发送，事后见已登记也不补发；BUSY 仅有界有限重试原条件记账，执行结果沿用有界保留退避重做，重复提示只按数据库当前事实推进。整理闭环总表，进入总审 | 接受未运行业务却占机会及 BUSY 原响应丢失后无法保证返还；不增加新表、状态、队列或每任务等待对象，正常路径不追加写后查询。单项通过不等于 B2 总审通过；未编码，SQL、参数、批次契约及故障测试仍待对应模块完成 |
| 2026-10-13 | B2 闭环总审通过，本议题在已确认规则范围内完成，转入触发与生成；默认额外执行重试由 1 次调整为 3 次，原始执行加重试共 4 次执行机会，间隔仍为 10 秒 | 每个 Task 独立预算，通信与业务执行失败共用；明确 BUSY 仍可条件返还。执行器结果回报与 BUSY 记账的独立重试规则不变、具体数值待定；触发与生成尚待提案，未开始实现，SQL、接口、ADR／规范及故障测试未因此完成 |
| 2026-10-13 | 首轮计划初始化通过：新建即启用时随创建保存第一轮 `next_trigger_at`，新建停用时在首次启用事务中保存，提交后再通知 Leader 注册规则；注册前宕机或首轮记账未提交仍有原计划依据，超过 5 秒按错失策略处理 | 不把首次初始化扩展为重新启用或修改 Cron 时的进度重置规则；正常与恢复入口的并发防重、检查职责与周期、任期／版本隔离及具体 SQL 未一并定案。未开始实现，新增场景是验证计划而非测试结果 |
| 2026-10-14 | 同 Job 触发记账通过：所有创建入口共用 Job 主键单行短事务，锁后用最新进度及当前数据库时间判断，按需 EXISTS 检查阻塞；自动创建 Instance 与推进进度同事务，跳过也消费进度。手动不推进 Cron，提交不明按持久化事实重入 | 接受逐 Job 有界事务成本；不同 Job 可并行，Instance 结束不统一锁 Job。不等于任期或配置隔离完成；触发身份、唯一键、错失区间、SQL／索引与跳过记录仍待设计 |
| 2026-10-14 | 低频触发恢复通过：保留 robfig 正常触发，Leader 接任立即检查、任期内每 1 分钟按索引小批量查 Job 触发进度；失主停止安排新检查。配置同步与查漏职责独立，不引入本地 Job 退避补试 | 纠正完全不扫描 Job 的表述；1 分钟不是恢复 SLA，迟到超过 5 秒仍按错失策略处理。已有 Instance／Task 恢复仍由对等节点承担，不套用一分钟周期，不改变 Task、BUSY 和结果重试；下一项讨论旧 Leader 与旧配置隔离。未开始实现 |
| 2026-10-14 | 配置同步与触发恢复的运行边界通过：同一 Leader 进程内两个独立后台循环，各每 1 分钟、共用任期启停；不要求同相位、同一条 SQL，也不以配置加载成功作为恢复的前置条件 | 独立运行不等于共享数据库故障时仍可推进；单 Job 仍须满足最新规则和授权条件。在途任期隔离仍待设计，下一议题不变，未开始实现 |
| 2026-10-14 | 切主与配置变更边界通过：每任期独立 `leaderCtx`，发现失主后取消该任期触发、配置和恢复工作；残留触发按最新 Job 合法记账，管理变更与触发共用同 Job 行协调 | 接受新 Leader 当选后旧在途事务仍可能合法提交，不增加全局数据库任期锁，不承诺固定时间内全部旧动作消失；对等 Instance／Task 工作不随失主取消。Cron 修改／重新启用进度、触发唯一键及内存同步顺序尚待设计，未开始实现 |
| 2026-10-14 | 停用编辑与启用进度规则通过：所有 Job 配置仅停用后可编辑，后端协调校验与写入；编辑不计算下一次，从停用切换启用时按当前规则原子保存新进度、不补旧轮次；重复启用、重启、切主及重载不重置 | 不取消已有 Instance，也不要求全部执行结束才能编辑；未批准存量执行配置快照、停用时清空进度字段或合并保存启用接口。下一项明确已有 Instance 的配置来源，未开始实现 |
| 2026-10-14 | Instance 执行配置规则通过：创建时保存本轮必要配置，后续各阶段、重试及恢复沿用；Job 编辑只影响新建 Instance。共享配置每轮一份，不增配置历史表，超时仍从首次有效开始计时 | 不冻结业务代码、数据、动态地址或凭据；具体字段、JSON／列及必要冗余留到数据设计，组生命周期另行闭合。下一项讨论触发身份与错失区间计算；未开始实现，新增用例是验证计划 |
| 2026-10-14 | Instance 配置存储通过：组标识和运行数据保留独立列，共享执行配置使用 `execution_config` JSONB，Go 使用明确结构体，由程序校验、序列化与兼容；Task 保留自身工作项及运行事实 | 不宣称 JSONB 更快，不逐 Task 复制完整配置或查询 Instance，不增加配置历史表；不决定 Job 本身的物理存储。完整字段、兼容细节、DDL 与索引仍待设计，下一项仍为触发身份与错失区间计算，未开始实现 |
| 2026-10-14 | 推进节奏调整通过：以触发闭环为首批实现单元，按模块详细设计、评审、编码和测试；发布触发模块开工清单，区分规则通过、具备开工条件和模块交付 | 不等全系统细节完成才写首批代码；不减少第一版分片、管理及维护范围。三组触发设计尚待收口，未批准具体身份算法、同步方案或 DDL，本次未启动业务编码 |
| 2026-10-14 | 触发身份与到期区间处理通过：自动身份为 `(job_id, scheduled_at)`，原计划时间取锁内旧进度，正常与恢复共用；锁后数据库时间判定，迟到不超过 5 秒为正常，超过则按错失策略。每次最多创建一轮，5 秒内多轮也合并，执行／忽略／阻塞丢弃均推进到判定时间之后的首个 Cron 时刻 | 未到期不创建、不推进；旧进度超 5 秒且忽略时整段跳过，不挑近期时间点补跑。补执行保存原计划时间，不承诺补齐全部业务周期或业务恰好一次；手动身份独立。Cron 格式／时区、手动幂等、时钟边界及 DDL 仍待设计；第一组部分通过，下一项为 Cron 格式与时区，未开始实现 |
| 2026-10-14 | Cron 格式与服务器时区通过：固定秒分时日月周六字段，沿用 robfig 语义，不兼容 Quartz 年／L/W/#，不接收描述符或表达式时区前缀；按调度服务器／容器进程默认时区统一校验、预览、启用、注册和进度计算，所有调度节点部署时区一致 | 不新增 Job `timezone` 或页面选择，不写死北京时间；保存／启用需能求出下一次，编辑试算不改持久化进度，Next 零值不能作为有效进度。运行错误、时钟偏差／回拨及部署时区变更处理留待运行设计；第一组仍缺手动规则，未开始业务实现 |
| 2026-10-14 | 手动触发规则通过：启用／停用均可执行，沿用集群阻塞，不改定时进度；仅当次业务参数覆盖，Instance 与配置提交成功返回 ID；同请求号在记录保留期间防重复创建，内容冲突拒绝、原实例终态不重开。第一组“触发判定”收齐，下一组为规则同步与运行 | 拒绝不永久留痕，响应丢失后迟到重试可能创建；历史清理后不承诺永久防重。不增加请求表、Redis 锁或请求队列，不改变 Task 异步回报；请求表达、内容比较、DDL 与接口留到本模块持久化设计，模块未达到开工条件，未开始业务实现 |
| 2026-10-14 | Job 配置同步渠道与周期通过：管理事务提交后通过 etcd 通知，Leader Watch 后核对 PostgreSQL，每 1 分钟兜底；不采用每 10 秒仅轮询配置。第二组“规则同步与运行”进行中 | 不把通知成功视为应用成功，不改变首轮初始化、固定 5 秒错失规则及独立的每分钟触发恢复；通知键、监听重连及加载顺序仍待设计。本次只更新设计，未开始业务实现 |
| 2026-10-14 | 配置加载与重载流程通过：集群级变更标记、每任期串行工作协程和容量 1 信号；主库一次轻量查询、差异应用；Watch revision 衔接、失败保留及任期隔离；新增或替换规则注册后提交一次到期检查 | 检查沿用原进度、同 Job 事务和 5 秒错失规则，不重算首轮；通知不是应用确认，跨系统窗口和容量延迟仍由既有恢复承接。不扩展为组或 Token 同步；下一项为触发容量与过载，之后收口时间边界，第二组未完成，未开始业务实现 |
| 2026-10-15 | 触发容量与过载通过：自动入口共用按 Job 合并的有界队列与固定工作协程，排队和处理中共同计入容量，新提示保留一次并复用槽位回队尾；满时拒绝并记录、不推进进度，既有恢复公平轮转。手动不入自动队列，共享本进程创建额度；数据库操作有超时、失败不自循环 | 保留原任期及提交不明规则，仍按锁后数据库时间应用 5 秒错失策略，不保证过载时每轮执行；不增加分布式全局限流或 Job 页面参数。容量／超时数值与实现契约待落实，下一项为时间边界，第二组未完成，未开始业务实现 |
| 2026-10-15 | Job 触发时间来源修订通过：取得 Job 行锁后读取一次调度进程 `time.Now()`，统一用于到期、5 秒错失与下一次计算，与 robfig 同源；恢复候选、校验、预览及启用使用相应调度后端进程时间，部署负责系统校时 | 替代此前 Job 触发的数据库当前时间口径；不改变持久化进度、身份与短事务，也不改变 Instance／Task 的数据库时间。异常时钟、无下一次时间及部署时区变更处理未一并通过，第二组未完成，未开始业务实现 |
| 2026-10-15 | Cron 简化异常处理通过：保存／启用时拦截；运行解析或求下一次失败时，单 Job 本次不创建 Instance、不推进进度，限频记录错误。加载非法 Cron 仍移除旧内存规则并隔离其他 Job | 不自动停用、新增异常状态、专用扫描、重试队列或零值 Next 自动修复；接受启用状态下自动触发受阻，由管理员修正，必要时重启。已有 Instance 与合法手动执行不受此错误连带停止；下一项为异常时钟及部署时区变更，第二组未完成，未开始业务实现 |
| 2026-10-15 | 时钟与时区运维边界通过：部署负责系统校时；下一次实际检查时应用原错失策略，持续启用的 Job 不因时间回拨倒退进度。时区变更走维护流程，退出旧时区计算及在途操作，统一修改重启，再启用计算未来时间 | 不重复选择已定进程时间；不新增时差探测、自动校时、自动节点隔离、历史迁移或专用扫描。不保证异常校时期间准点或不漏跑；历史身份冲突 SQL 仍待持久化设计。下一项为容量参数与运行实现契约，第二组仍未完成，未开始业务实现 |
| 2026-10-15 | 触发容量初值通过：待处理 Job 总容量 1024、进程创建并发 32、单次处理超时 3 秒，均为部署配置；自动工作协程数派生，手动立即获取共享额度、满则繁忙，不预留专用额度 | 从开始处理计时，覆盖额度、连接、行锁与提交，服从更短的上游期限；不含自动排队，不冻结 5 秒错失或改变提交不明边界。实现后比较并发档位校准；恢复批次不固定为 100×5，随 SQL 和索引设计。下一项为 Leader 任期生命周期与首次规则装载，第二组仍未完成，未开始业务实现 |
| 2026-10-15 | Leader 任期生命周期与首次装载通过：任期独立资源，本机清理完成后才重新参选，进程连接、创建额度及对等职责复用；首次未运行 Cron 批量注册，启动并确认实际初始化后提交到期检查，热更新也整批确认 | 首次查询失败不启动 Cron，后续核对与独立恢复继续；失主关闭入口、取消并丢弃旧提示，等待本地在途退出，不另设强杀或固定退出秒数，不阻塞其他机器接任。仍接受合法旧提交与容量导致的错失。下一项为 etcd 选主与配置通知接入约定，第二组未完成，未开始业务实现 |
| 2026-10-15 | etcd 接入通过：官方 Session＋Election，租约部署初值 15 秒且正值校验；选主与持久 reload 标记分开，执行器无相关权限。提交后应用调用一次通知，预算最多 1 秒；失败日志／指标及分钟核对，不主动改报已提交保存失败。普通短 etcd 请求、配置读取各 5 秒，长操作绑定生命周期，Watch 原生恢复与 1 至 30 秒抖动退避重建 | 租约不是切主 SLA，继续接受超 5 秒错失及合法旧提交；不增加消息队列或叠加会话／监听。第二组规则经一致性核对收口，进入第三组 Job／Instance 存储分工；新模型仍待评审，模块未达到开工条件，未开始业务实现 |
| 2026-10-15 | Job／Instance 存储分工通过：Job 平台配置普通列、业务参数独立载荷；Instance 必要独立列加执行配置 JSONB。触发事务复用已读 Job 构造快照，不新增配置查询，后续执行不混用当前 Job；进入本批完整字段草案评审 | 参数格式、类型、空值、编码、默认值及配置成员未顺带批准；不机械逐列建索引，不把可调业务范围写成 CHECK。第三组进行中，事务／索引／接口及验收未收口，未开始业务实现 |
| 2026-10-15 | 本批 Job／Instance 字段方案通过：表名及类型、原样 TEXT 参数、新建默认停用、停用保留进度、JSONB v1 显式配置、UUID 请求及覆盖标记比较、历史名称和操作者信息、运行时间初始空值已定；进入触发事务、唯一约束与必要索引评审 | 不提前确定超时产品默认值、输入长度、结束原因字典、删除／历史保留、Task 生成和汇总字段；具体 DDL、SQL、接口及验收仍需闭合。第三组进行中，未开始业务实现 |
| 2026-10-15 | 触发事务、身份约束与五索引通过：自动／手动部分唯一索引、身份结构 CHECK、锁后分语句查重和阻塞、精确冲突处理及五条访问路径已定；自动历史冲突保留旧 Instance 并推进当前 Job。月分区暂不考虑 | 补全计划时间示例，保留大历史存量、持续状态更新与并行清理的性能验证要求，不确定保留期或清理算法。下一项为 Leader 恢复分页、预算与公平续扫，第三组仍进行中，未执行迁移或开始业务实现 |
| 2026-10-15 | Leader 恢复查询简化方案通过：每次重新查主库最早到期 Job，单批 LIMIT 初值 1024、查询超时 5 秒；不分页、不保存跨分钟游标，队满或查询失败结束，下分钟重查。保留接任立即及独立分钟循环 | 不采用此前 256 条分页、跨分钟固定巡回和整趟 5 秒预算草案；明确修订旧 Job 恢复公平轮转要求，接受持续失败或反复队满时其他遗漏 Job 可能长期延迟。自动队内轮转与 Instance／Task 恢复不变。下一项为跳过触发留痕，第三组仍进行中，未开始业务实现 |
| 2026-10-15 | 自动跳过留痕通过：错失忽略与阻塞丢弃在进度确认提交后仅写结构化服务日志；复用已读事实，不额外查询、不建表或增字段，每次决策一条，不枚举历史 Cron 时刻 | 接受管理页无跳过明细、提交后宕机或采集故障可能缺日志，不提供完整审计及精确计数。回滚、提交不明及技术失败不得误报跳过成功。下一项为 Job 删除与历史保留，第三组仍进行中，未开始业务实现 |
| 2026-10-15 | Job 删除通过：改为逻辑删除，其他删除提案通过；停用且无待开始／运行中 Instance 才标记删除，保留定义及执行历史。仅 Job 增加 `deleted_at`，常规读写与调度排除已删除，到期索引同步过滤；历史可显式只读关联 | 不采用物理删除建议；不做回收站或恢复入口、不级联，不把父终态等同业务退出。保留删除时最后定义，不替代逐轮快照；旧创建请求不跨删除重放。下一项为 Job 的组引用与离线配置校验，第三组仍进行中，未开始业务实现 |
| 2026-10-15 | Job 组引用与离线配置校验通过：新增、编辑保存、启用及手动新建检查目标组存在且允许引用；允许组内暂无在线执行器，不做处理器远程探测。旧手动请求按既有查重处理，停用／删除不依赖组可用性 | 不增加常态 Cron 或 Task 派发的组表查询，不在事务中调用执行器；组删除与新引用仍须协调，已有 Instance 保留原快照。下一项为整轮超时默认值，第三组仍进行中，未开始业务实现 |
| 2026-10-15 | 整轮超时初值通过：新增表单预填 900 秒，即 15 分钟，用户可修改；API 必填、不设 SQL 默认，不改已有 Job 或 Instance | 有限正数、整轮范围、首次有效开始计时及绑定后失联等待规则不变；不把 900 当上限，不新增其他业务范围或快速重派。下一项为 Job 管理入口与事务边界，第三组仍进行中，未开始业务实现 |
| 2026-10-15 | Job 管理入口与事务边界通过：动作式 POST，完整配置编辑与启停分离，空串和零值明确写入；编辑条件更新、启用短事务，独立预览最多 5 次。普通新增返回 Job ID，手动创建沿用持久化后返回 Instance ID 及请求防重 | 不加编辑版本号，允许并发停用配置覆盖；普通新增响应丢失不安全自动重试。组引用并发保护、最终输入／查询／SQL／Proto 及交接验收仍须闭合；下一项为组引用与删除的并发保护，未开始业务实现 |
| 2026-10-15 | 组引用与删除并发保护通过：新增／完整编辑与删组共用目标组主键单行短锁，组→Job 锁序；锁后独立检查未删除 Job、原组未结束 Instance 和未收敛 Task。停用 Job 也阻挡，无恢复依赖的完成历史不永久阻挡 | 不增加运行期组锁／逐次查询，不选定组删除形式；凭据、缓存和取消恢复生命周期未落实前不开放删组。下一包为 Job 输入、查询与错误契约，其后持久化交接与最小依赖、模块开工总审；未开始业务实现 |
| 2026-10-15 | 按用户要求补录字符串类型规范并重设计字段：名称及快照 VARCHAR(128)、说明 VARCHAR(500)、处理器与 Cron VARCHAR(128)、结束原因码 VARCHAR(64)；参数／自由诊断摘要 TEXT，配置快照 JSONB。同步输入容量和跨层验证，替代短字段统一 TEXT 的草案 | Cron 128 是项目容量而非解析库极限；不新增摘要截断、不改变数值范围 CHECK 边界，不改已有表或执行迁移。输入／查询／错误整包仍待评审，模块尚未开工 |
| 2026-10-16 | Job 输入／查询／错误契约通过：短字段容量统一，普通新增／编辑的说明和参数省略、null、空串均归空覆盖；params 去首尾空白后保存及比较，替代此前原样保留首尾空白的规则。名称字面不区分大小写包含、轻量列表与详情分离、固定两次查询及按组部分索引、统一错误与重试边界已定 | 默认值／筛选／手动覆盖仍保留必要存在性；手动先校验组合再 trim，内部空白和 JSON 键序仍有区别，不重写旧快照。参数复用报文限制，不新增独立上限；未编码或执行迁移。下一包为持久化交接与最小执行器组依赖，随后模块开工总审 |
| 2026-10-16 | 执行器组生命周期通过：第一版无组启停，仅逻辑删除，保留定义和历史；未删除 Job（含停用）、原组未结束 Instance、未收敛 Task 均阻挡，同组短事务锁后检查并标记删除 | 补定此前保留的组删除形式；普通读取和新引用排除已删组，不自动取消任务、关闭进程或级联删历史。凭据与连接缓存失效及取消恢复依赖在完整删组开放前落实，不新增在线节点必须为零等门槛 |
| 2026-10-16 | 持久化交接与最小执行器组依赖整包通过：WAITING Instance 与完整本轮配置作为交接，本地有界 ID 提示仅加速；候选不是授权，不能使用永久 ID 水位；正式 `job_executor_group` 本批包含身份、名称、创建／修改／删除时间 | 首批不创建假 Task 或消费者，不启动整轮倒计时；组表五字段替代无生命周期四字段草案，发现与凭据字段后续补齐。下一步汇齐最终 SQL／API／Proto、测试映射和 ADR／规范，进入触发模块开工总审；尚未编码或执行迁移 |
| 2026-10-16 | 提交触发模块开工总审材料：主稿、三表六索引 SQL、八条 HTTP／RPC 契约、十二类验收矩阵及 ADR-0009 接入草案 | 本次细则包括独立 job-rpc／System BFF、Job 迁移账本、etcd 规范例外及 CI 范围；待用户总审，不标记已通过，不修改业务代码或执行迁移 |
| 2026-10-16 | 用户通过触发模块开工总审：本批范围、SQL 与 HTTP／RPC 附件、验收安排和 ADR-0009 接入细则均获批准 | 用户随后明确要求先记录、不编码，继续讨论问题；等待明确开始实现指示。未修改业务代码、生成契约、执行迁移或应用规范与 CI 变更 |
| 2026-10-16 | 产品定位及工程目录确认，本次总审收口：DogX 一体化后台内置调度能力，同仓单 Go Module，System BFF 与独立 job-rpc；公共执行组件仓库内复用，暂不发布对外 SDK，继续 etcd 注册发现 | 本批没有剩余开工前必决项，仍止于 WAITING Instance 及配置持久化；后续执行、分片和运维模块按原节奏设计。收口不等于授权编码，等待明确开始实现指示，未改业务代码或执行迁移 |
| 2026-10-16 | 用户明确授权开工；复习规范后完成首批触发代码及本地验证，落地独立 Job 域、八条管理接口、etcd 选主／通知、长期 Cron、分钟恢复、三表及独立账本 | 全仓普通测试、真实 PG／etcd 集成、500／100 功能 smoke、契约生成与静态检查通过；开发数据库未迁移，代码未提交推送。Task／页面未提前实现；CI、正式容量验收及下一模块按各自边界推进 |
| 2026-10-17 | 首批触发及后续质量整理已提交推送；最新提交 9689b35 的四项 CI 全部成功。共享基础组件、GORM 风格与业务分层、错误边界、英文核心契约注释及规范已整理 | 交付仍止于 WAITING Instance；完整组接入、Task 执行、页面及正式容量验收未完成，覆盖率口径见本页 CI 记录 |
| 2026-10-17 | 用户同意同步交付进度并开始普通 Task 执行闭环的必要详细设计；本轮提出主路径与职责方案 | 仅更新文档和讨论设计，不编码；本轮新增方案仍待评审，B1／B2 已定规则不重选 |
| 2026-10-19 | 普通 Task 主路径与职责评审通过：WAITING 由本地提示及有界恢复接续；锁后数据库当前时间确定首次开始与截止，连同 RUNNING 和唯一普通 Task 同事务保存；正常本地派发、双端异步回报及平台结束职责继续沿用 | 不重置本轮计时、不重复创建 Task、不增加整轮负责人或普通任务 Pre／Post；是否合并首次尝试登记、具体 SQL、执行器接入与 RPC 仍待设计。本次只记录决定，不编码；下一项为执行器接入与调用契约 |
| 2026-10-19 | 处理器目录与装配边界通过：System 业务任务处理器放在 `apps/system/internal/job`，嵌入时由 `apps/system/rpc` 装配注册，同域 Worker 可复用；普通 RPC 用例仍留在 Logic | 不增加转发 Service、跨域内部导入或额外 RPC 跳转，不提前创建 Worker；公共执行组件包路径、处理器签名、注册细则和完整接入契约尚未定案。本次只记录决定，不编码 |
| 2026-10-19 | 执行器接入与处理器注册首次方案通过：公共组件采用 `pkg/jobexecutor`，首个接入嵌入 `system-rpc`；启动时显式注册自定义名称，运行期注册表固定，空名称、重复名称或无效处理器拒绝初始化，同组副本保持处理器兼容 | 当时的共用服务端与端口方案已由下条独立 Server 决定替代；组件、目录与注册规则继续有效。处理器返回结果，框架回报；普通处理器签名及完整 RPC 契约尚未通过，本次只记录决定，不编码 |
| 2026-10-19 | RPC 入口方案通过：保留组 Token；`job-rpc` 同进程分开管理与执行器通信 Server；System 首版仍内嵌 `system-rpc`，由 `pkg/jobexecutor` 创建独立执行器 Server，不新增 Worker 进程，组件与域内处理器保持可供以后同域 Worker 复用 | 管理 RPC 是否启用及如何配置鉴权交由部署方使用 go-zero 原生能力，不新增管理权限机制或强制网络限制；隔离指监听、服务注册、发现目标和鉴权链，不承诺自动禁止执行器访问管理入口。不增加内部 RPC 转发、第二套调度器或共享入口切换模式；处理器签名、具体配置与完整协议仍待设计，只记录、不编码 |
| 2026-10-19 | 处理器接口分工、输入输出语义及共同执行约束通过：普通 Processor、分片 ShardingProcessor、可选 PostProcessor；当时公共输入与分片子数组分开，准备显式返回工作项，该分组语义已由 [2026-11-06 最终分片清单](task-model.md#explicit-shard-contract)替代；通用 Result 区分 JSON 业务数据与摘要，error 表示失败；能力校验、并发／跨进程、Context、panic 及取消不提前释放槽位的边界已定 | Post 的有界结果访问仍是该历史提案内未决项，未批准分页访问器或新增结果查询 RPC，不将部分批准当作当时处理器契约或第二组整体结束；后续才讨论派发／回报／取消协议。当时只记录设计与验收要求，不编码、不生成协议、不提交推送 |
| 2026-10-19 | Post 业务职责与参数语义通过：分片完成后的可选业务收尾，先 Post 后由平台确定 Instance 终态；普通任务不支持 Post。输入沿用本轮公共信息，TaskID 指当前 PostTask；提供 Total／Succeeded／Failed 和分片最终 TaskID、Status、Data、Summary、失败原因，区分 Task 数与业务行数 | 不提供尚未确定的 Instance 最终状态，不承诺失败分片有可用业务输出，不把 Post 当必达清理钩子。一次传入还是按需读取仍待评审，未批准分页访问器或新增 RPC；处理器契约尚未整体结束。本次只记录设计及验收要求，不编码、不提交推送 |
| 2026-10-19 | 用户明确收窄 Post 输入：只保留公共 Input（JobID、InstanceID、当前 PostTask 的 TaskID、本轮 Params）和 Total／Succeeded／Failed，方法另接收 Context；不提供分片结果数组、执行明细、访问器、分页或查询 RPC。业务需要的明细和产物由业务自己在所属存储读取，框架不直接访问业务表 | 替代此前要求 Post 能读取分片最终结果的范围，撤销待决有界结果访问提案。普通任务不支持 Post，分片业务收尾、重试和终止边界不变；处理器职责、输入输出与共同执行约束提案收口，下一项为派发请求与接收 ACK。第二组及执行模块未整体完成，不编码、不提交推送 |
| 2026-10-19 | 派发请求与接收 ACK 提案通过：一次 RPC 派发一个 Task 的当前尝试，请求携带 Job／Instance／Task 与尝试身份、处理器名称、Task 类型及能力要求、本轮 Params 与阶段输入、整轮 deadline；区分接纳 ACK、BUSY、其他明确拒绝与通信结果不明。本进程以 TaskID 和尝试标识在排队及执行期有限防重，业务实际退出即释放活动记录，不等待结果 ACK，不跨进程或重启防重，不保存终态缓存 | 接纳不代表执行完成，业务 Context 不随短 RPC 结束取消，结果可早于 ACK；既有机会预算、绑定与故障规则不变。下一项为结果回报请求与接收 ACK，随后讨论取消；字段和错误编码、SQL、容量及恢复参数仍待实现协议闭合。第二组及执行模块未整体完成，不编码、不提交推送 |
| 2026-10-19 | 结果上报请求与接收 ACK 提案通过：公共执行组件组装身份、尝试、执行结论及阶段输出；按条数上限或短窗口先到合批，窗口从第一条结果起算且不续期。一次 RPC 请求对应一次响应，响应逐条区分已接收、稍后重传和永久拒绝；只有限重传未确认项，保持同一尝试与结果，重组批次不重置单条预算 | 接收线程不查 Task 表，入队即 ACK，后台校验并持久化，业务失败不等于报文拒绝。单条不可交付或永久拒绝时记录错误和指标后释放；不增加持久化收据、本地文件或长期缓存，既有结果丢失、绑定及超时兜底不变。下一项为取消请求与响应；数值、Proto 和状态写入 SQL 待后续闭合，第二组及执行模块未整体完成，不编码、不提交推送 |
| 2026-10-19 | 结果上报粒度调整通过：第一版改为单条异步上报，每次 RPC 发送一个 Task 当前尝试的结果，一次响应确认该结果；固定数量发送协程可并发多个单条请求，保留有界结果队列、有限重传与释放、入队 ACK 和后台异步写库。同节点按 Instance 合并“分片是否全部结束”的检查独立保留 | 替代上一条的网络数量／时间合批、批内部分接收／部分重传及重组机制，不改变结果丢失、绑定与超时边界，不改变完成检查的窗口测试起点。后续仅在压测证明 RPC 瓶颈后评估有界顺手合批，第一版不预留批量模式开关。下一项仍为取消请求与响应；具体 Proto、容量／重传数值、状态写入 SQL 仍待闭合，不编码、不提交推送 |
| 2026-10-19 | 取消通知粒度通过：管理操作停止整轮 Instance；按 Instance 向各已知执行器进程分别定向通知，目标去重，本机匹配活动统一请求取消，不逐 Task 发 RPC，不广播，不增加单 Task 停止管理功能 | 不改变父终态先保存、子状态分别收敛及尽力取消边界；少发 RPC 不等于业务全部退出。取消请求去掉预期进程 ID 强制校验已确认，接单及回报身份规则不变；完整取消响应与失败处理仍待收口，只记录、不编码 |
| 2026-10-19 | 用户确认第一版不复用定向客户端以降低复杂度：一次目标取消通知按需创建原生单地址客户端，本次有限重试共用，结束后关闭，不跨通知缓存，不建立地址缓存、引用计数或闲置淘汰机制 | 替代此前定向客户端有界缓存复用的规则；正常派发和结果回报仍复用发现客户端。接受独立通知重新建连及集中停止时的握手开销，发送并发有界；具体数值与原生中间件附属状态清理留在实现契约。完整取消应答仍待确认，不编码、不提交推送 |
| 2026-10-19 | 取消身份范围简化通过：取消请求只针对唯一 Instance，保留组认证和取消原因，不携带或强制校验预期进程实例 ID；按本轮执行器组与已知目标地址去重通知 | 替代原取消接口匹配预期启动身份的要求，不删除接单 ACK、执行绑定及结果核验中的进程身份。同地址重启后无匹配活动则不操作，同一已终止 Instance 的迟到活动仍请求取消，不影响其他轮次；完整取消响应、失败处理和参数仍待闭合，只记录、不编码 |
| 2026-10-19 | 取消处理与响应语义通过：组认证及参数校验后，排队活动标记取消、运行活动取消业务 Context；无匹配活动及重复取消正常回复。协调排队转运行与取消，成功只表示本次本机取消操作已处理，不等待业务退出，运行槽位实际退出才释放。永久错误不重试，通信故障有限重试同一目标，不消耗 Task 执行预算；耗尽或调度进程停止后结束通知、释放客户端并记录未确认结果 | 不修改已有父终态，不替代子 Task 条件收敛，接受已授权派发晚于取消到达；不加停止查询、长期取消缓存或可靠通知表。业务处理器与执行消息语义议题结束，进入接入配置与组依赖；通信数值、Proto、错误编码及 SQL 待后续设计，不顺带确定 executorInstanceId 格式或扩展身份校验，不编码、不提交推送 |
| 2026-10-20 | 执行接入配置布局与来源通过：Job 保留顶层管理 RPC，新增 `ExecutionRPC`；System 保留原业务配置，新增包含组身份、组 Token、`Server` 和 `ReportClient` 的 `JobExecutor`。Server／Client 复用原生配置类型；派发由部署 etcd 连接信息与数据库组 Key 构造客户端，不在配置文件重复维护静态组映射 | 组 Token 与 go-zero 原生 `App/Token` 分开；仅确定配置布局与来源，不确定凭据存储、缓存、启停流程、端口和运行参数。继续组凭据与缓存设计，第二组尚未结束；不编码、不生成协议、不提交推送 |
| 2026-10-20 | 用户确定组 Token 采用明文保存，格式为 32 字符无连字符 UUID；沿用后端安全随机生成要求，使用 UUIDv4 的小写十六进制表示。不采用应用层加密、哈希替代或额外部署加密密钥 | 参考 SnailJob 原文存取方式，不照搬其 `SJ_` 随机字符串格式、前端生成或公开默认值；保留日志和普通列表不泄露、空凭据拒绝的既有边界。查看接口、轮换、缓存及启停尚未通过，不编码、不提交推送 |
| 2026-10-20 | 用户将组配置周期刷新间隔由建议的 10 秒调整为 60 秒：配置低频变更，不追求立即生效；执行器 Token 修改还需配套配置与重启 | 与 SnailJob 仅对齐 60 秒数值，不将其按需过期缓存写成周期刷新。本项不扩展为缓存故障、回报重传衔接及轮换协议全部通过，不承诺 60 秒强制撤销或无缝轮换；不编码、不提交推送 |
| 2026-10-20 | 60 秒版组配置缓存提案通过：各节点启动加载、串行完整刷新、成功原子替换、失败保留旧快照；热路径内存读取，未同步到新组时回报暂时拒收并允许有限重传，不按未知请求查库或新增缓存条目 | 冷启动无快照不免认证放行；接受数据库故障时删除或凭据变更滞后，内存随有效组数增长，刷新不重建全部连接。回报重试参数须覆盖正常同步延迟；查看与更换、组变更和旧客户端释放、完整启动退出仍待闭合。下一项为凭据查看与更换，不编码、不提交推送 |
| 2026-10-20 | 用户明确第一版不提供 Token 重置功能：创建组时生成后保持不变，普通编辑不可修改，不增加重置按钮、API／RPC 或自动轮换；轮换不再列为第一版待办或开工前置条件 | 不把本次范围收缩扩大为凭据查看／复制方案通过；组删除后的凭据缓存失效与旧客户端释放仍须闭合，不设计手工改库或删组重建作为替代功能。只更新文档，不编码、不提交推送 |
| 2026-10-20 | Token 查看方案通过：普通组列表不返回 Token，组详情返回完整 Token，页面默认遮挡并支持显示、复制；普通编辑只读且不提交 Token。复用既有 API／RBAC，组详情权限允许读取，不另建查看密钥 API，不采用仅创建时展示一次 | 第一版不修改或重置 Token 的边界不变；请求、响应及错误诊断日志不得泄露 Token，具体日志控制实现尚待审计与验收，不将设计写成已实现。下一项为组字段变更与旧客户端释放，启动退出仍待闭合；只更新文档，不编码、不提交推送 |
| 2026-10-20 | Token 日志保护方案通过：不主动记录 Token；RPC 自动正文分别控制客户端和服务端，HTTP 报文、配置与错误字符串另查实际输出路径 | 不改变详情返回原值；不新增公共日志包或通用脱敏框架，控制实现与日志测试仍待完成。凭据议题收口，下一项为组字段变更与旧客户端释放；只更新文档，不编码、不提交推送 |
| 2026-10-20 | 组身份与字段规则、缓存调整通过：组 `name` 是执行器配置的接入身份，数据库 `id` 继续供 Job／Instance 引用，etcd 发现 Key 单独手工维护；`name` 与发现 Key 创建后不可修改。缓存未命中查询单组，同组并发未命中合并查询；存在且未删除则缓存并继续本次认证和处理，查不到或数据库失败不免认证放行。各节点每 60 秒仍完整读取未删除组的执行字段，成功原子替换，失败保留旧快照 | 替代此前缓存缺组直接暂拒且不回源的规则；不做组增删差异同步、不新增 etcd 组变更通知，不因完整刷新重建全部 RPC 客户端。未批准完整客户端生命周期，组删除后的旧客户端清理及启动退出仍待评审；只更新文档，不编码、不构建、不提交推送 |
| 2026-10-20 | 组删除后的客户端清理设计通过：各节点在 60 秒全量查询成功替换快照后，检查本地已创建的组客户端；组仍存在则原样复用，不存在则先移出缓存，再调用 `client.Conn().Close()`。查询失败不替换也不清理；回源、创建与清理做本地并发协调，迟到旧结果不能重新挂回已清理客户端 | 不新增数据库查询、独立清理线程、引用计数、排空等待或等待远端业务；接受少量在途 RPC 中断并按通信结果不明处理，不能推断未接单或业务已取消。不改成组配置增删差异同步，定向取消仍不跨通知缓存。仅本项设计通过，下一项为启动退出；尚未批准实现，不编码、不构建、不提交推送 |
| 2026-10-20 | 启动退出提案通过：初始化所需依赖、处理器和队列后才接收，就绪不等待 Leader 或所有组在线。调度端退出停止触发、派发、恢复工作生产和新回报接纳，结束任期并有限善后落库；执行端拒绝新单且不使用 BUSY，已接纳排队及运行任务在剩余宽限内可继续，回报保持工作。整个进程退出宽限默认总计 30 秒、可配置，两套 Server 和所有清理共享剩余预算 | 宽限到期停止启动排队任务、取消运行 Context、结束等待，由宿主结束进程；组件不调用 `os.Exit`、不强杀 goroutine，也不为每个组件重置 30 秒。退出允许内存结果丢失，不等同业务取消、失败或重派授权，不延长本轮截止时间。接入配置与组依赖规则收口，下一项沿用原顺序的 Task 持久化与状态推进，先普通 Task 字段、约束和首次推进事务；第二组完整 Proto 与装配仍待实现前核对，执行模块尚未完成或获编码授权。只更新文档，不构建、不提交推送 |
| 2026-10-20 | 本地派发采用有界队列和固定数量工作协程的实现方式通过：队列限制积压、固定消费者限制并发，取出候选后才登记尝试，不在入队时占机会；队满暂停补充，未接纳工作留在数据库，不无限创建阻塞入队或发送协程。派发处理结束后继续消费，不等业务完成，不额外建立槽位对象管理 | 保留按 Instance 有界小批登记，批内发送不得绕过并发限制；队列长度、工作协程数及批次组织未定。普通 Task 首次创建与首次尝试登记合并事务仍为待评审提案，不将本次澄清批准扩展为整项事务设计通过；只更新文档，不编码、不构建、不提交推送 |
| 2026-10-20 | 派发工作协程默认 64 及普通首次启动合并事务通过：每调度进程固定长期消费者、部署可调，与 Instance 创建并发 32 独立。工作协程取出普通 Instance 候选后，同一单父行短事务保存首次开始、截止、RUNNING、唯一执行中 Task、首次尝试标识及登记时间，并占一次执行机会；明确提交成功后由当前工作协程直接派发 | 没有派发容量时保持 WAITING，不计时、不提前建 Task；数据库等待也占派发并发。明确回滚不留部分状态，提交不明及提交后未发送不免费补发，BUSY 条件返还及后续重试规则不变。SnailJob 保存 retry_count 的事实不替代 DogX 尝试身份与额度协议。队列容量、批次、公平性、Task 字段和 SQL 仍待细化；只记录文档及验收要求，不编码、不构建、不提交推送 |
| 2026-10-20 | Task 表字段与单例唯一约束通过：统一 `job_task`，普通所需公共字段及类型／六态编码确定；`attempt_no BIGINT` 递增识别执行，`used_attempts INTEGER` 包含首次并允许明确 BUSY 条件返还。进程身份为每次启动一次的 UUIDv4，地址 VARCHAR(255)，业务 Data JSONB、摘要 TEXT、终态原因 VARCHAR(64)，首次登记、当前登记、最早派发、终态及审计时间各自表达 | `(instance_id, task_type)` 部分唯一索引仅覆盖普通／准备／收尾；重试复用原 Task，分片编号及生成字段后续设计。不增加软删除、物理外键、公共配置副本、Attempt 历史表、负责人、租约或关闭字段；结果可早于绑定，不强制成功有地址。下一项落实普通首次创建与重复推进 SQL，随后闭合状态更新和查询索引；仅记录文档及验收要求，未编码、建表、构建或提交推送 |
| 2026-10-20 | 普通 Task 首次创建与重复推进 SQL 通过：具备派发容量后，默认隔离级别的短事务按主键锁单条 Instance，复核普通 WAITING 及空运行时间，锁后另读一次数据库时间；父行条件更新必须恰好一行，再插入唯一普通执行中 Task。单例冲突整笔回滚，重复入口不创建、不重设计时、不发送旧尝试；本次确实新建 Task 且明确提交成功才派发 | Job 触发沿用调度进程 `time.Now()`，Instance／Task 执行计时沿用数据库时间；提交不明不补发，等待恢复。不扩展为绑定、结果、重试及终态 SQL 已通过；下一项拟审查接单 ACK 的执行器绑定条件更新，包含结果先到和迟到 ACK。仅记录设计与验收要求，未编码、建表、运行测试、构建或提交推送 |
| 2026-10-21 | 接单 ACK 绑定 SQL 及旧尝试关闭规则确认通过：使用原请求 Task／Instance／执行编号、执行中状态及两项绑定均空的条件，原子保存 ACK 进程 UUID 和同次派发 Peer 地址，只维护绑定与审计字段。零行结束、不补查、不推断原因、不自动取消；不额外读锁父，父终态但子仍执行中可补绑定，不恢复状态或承诺补取消 | 保存故障或提交不明只在当前固定派发工作协程内有限重试元数据，不重发业务，耗尽或退出释放；次数及时间预算待容量设计。旧尝试经 status 改为等待重试或失败并提交后关闭，即使编号未变也拒绝迟到成功，下一次登记才递增编号；不证明旧业务停止。绑定议题结束，下一项为结果保存与状态转换 SQL；仅记录设计与验收要求，未编码、建表、运行测试、构建或提交推送 |
| 2026-10-21 | 普通 Task 结果保存与状态转换通过：后台批量核验关系、认证组和本轮配置，按 Task／Instance／原尝试、执行中状态及空或匹配执行器身份条件更新。成功、失败待重试、失败耗尽三个分支一次保存结果、状态与时间；每短批次取一次数据库处理时间，不采用客户端自报结束时间，不扣次数、不递增编号、不补绑定 | 零行只表示未应用，不追加查询或宣称此前成功；不统一锁父，结果不能恢复父终态、启动 Post 或直接重派。可恢复写库故障和提交不明有界保留原结果，正常完成事务后可向已核实归属的 Instance 发检查提示。原因码名称、批次错误隔离及容量参数仍待设计；下一项为等待重试 Task 的新尝试登记 SQL。仅更新设计、参考及验收要求，未编码、运行数据库测试、构建或提交推送 |
| 2026-10-21 | 普通 Task 等待重试与新尝试登记 SQL 通过：复用有界候选与固定派发工作协程，候选保存原执行编号；单父行短事务锁后取数据库时间，检查父仍运行、未到期及本轮配置，再按等待重试、原编号、到期与剩余次数更新原 Task，原子递增编号和已用次数、清空旧绑定及当前执行字段 | 保留最近结果说明，不将其当作本次结果；首次开始与父截止不变，明确 BUSY 返还后的机会可重新占用。本次返回新登记记录且明确提交成功才发送，零行不换最新编号重试，提交不明不补发。下一项为绑定缺失观察到期后的未知结算 SQL，随后 BUSY 返还、终止及查询索引。仅更新设计、参考及验收要求，未编码、运行数据库测试、构建或提交推送 |
| 2026-10-21 | 普通 Task 绑定缺失的未知结算 SQL 通过：有界批读父关系与本轮配置并取得数据库处理时间，前置观察父运行且未到期；按原编号、执行中、双绑定空及观察到期条件更新，当前行已用次数决定等待重试或最终失败。清 Data、覆盖新的结果不确定诊断，不增减次数、不改编号或登记时间，不用摘要判断有无本次结果 | 不锁父，接受读取后父终止与子结算交错，但不能恢复父或授权重派；更新且明确提交后只提示检查，零行结束。数据库失败或提交不明释放临时候选，由已有恢复退避重读，不套用业务结果保留。观察期秒数、原因码及实现验证未完成；下一项为明确 BUSY 的条件返还与延期 SQL。仅更新设计、参考及验收清单，未编码、运行数据库测试、构建或提交推送 |
| 2026-10-21 | 普通 Task 明确 BUSY 的条件返还与延期 SQL 通过：原派发专用未接纳响应由当前固定工作协程保存，按原请求身份、执行中、正已用次数及双绑定为空，一次更新转等待重试、返还一次机会并设置数据库求值时间加正延期；更新本次容量拒绝摘要，不回退编号或重置执行时间 | 零行结束不补查，结果、绑定、未知结算、取消或新尝试先有效时不覆盖；不额外读锁父，候选不授予新执行。失败或提交不明只有限重试原响应保存，不重发业务，释放后不猜测补返。延期及保存重试参数仍待容量评审，下一项为父子终止条件写入与查询索引。仅更新 DogX 设计、进度及验收要求，未编码、运行数据库测试、构建或提交推送 |
| 2026-10-21 | Instance 停止或超时后的父终态保存与子 Task 收敛通过（父终态裁决由下一条修订，父先终态顺序由 2026-10-23 STOPPING 提案替代）：父主键单行短事务锁后取数据库时间，已有终态保留，到有效截止优先超时失败，否则人工停止置取消。父确认后在事务外统一有界分批取消所有类型的未结束 Task，不为普通任务另写单条分支，保留子已有终态及编号、次数和绑定 | 子状态收敛与按 Instance 去重地址通知分开，取消不依赖本批子更新成功；人工停止提交不明不能宣称成功或恢复未持久化意图。接受子已全终态而通知前退出导致不补送的窗口，不增可靠通知机制。正常完成汇总、索引、容量和原因码仍待设计；下一项为普通 Instance 正常完成条件写入与摘要处理。仅更新设计、进度及验收清单，未编码、运行数据库测试、构建或提交推送 |
| 2026-10-21 | Instance 完成与终态调整通过（父先终态顺序后来由 2026-10-23 STOPPING 提案替代）：完整最终结果允许在父未终态时截止后收尾；普通与分片共用检查，正常完成父锁外读稳定事实、条件 UPDATE 竞争。自动超时先做仅终态判定，不生成新工作，条件不具备且到期才超时；技术错误不能当作未完成。人工停止按取消及人工停止原因保存，不因已到期改记超时 | 替代所有终态入口锁后到期一律超时及正常完成持父锁汇总的旧要求。保留新工作／重试／Post 的截止授权、终态不可逆、生成封口和阶段完整性、父先终态再有界子取消及独立数据库恢复；不新增时间轮、500ms 宽限或独立 Task 超时。接受有效结果迟到及终态竞争，不承诺完成全局优先。下一项为共用完成判定、条件写入与摘要契约；完整 SQL、索引和容量继续评审，仅更新文档，未编码、运行数据库测试、构建或提交推送 |
| 2026-10-21 | Instance 完成判定、父条件写入、摘要及超时衔接通过：普通与分片共用机制，阶段完成事实分别核验；正常结束父锁外读取稳定事实，按主键及 status=2 更新为成功或失败，不加截止条件。全分片最终失败仍在条件允许时执行已配置 Post，Post 未创建不视为未配置 | 更新一行须确认提交，零行／技术错误／提交不明不转超时、不重跑业务；正常结束时间取 UPDATE 数据库求值时间，单例复制最终摘要、分片生成统计与 Post 结果。明确结果未齐才进入超时父短事务，不在锁内重扫 Task，接受结果到齐与超时竞争。保留阶段推导、不新增权威状态列，拒绝状态与阶段拼接展示，UI 后续单独评审。下一项为恢复候选查询与索引；分片生成封口及完整查询仍待设计，仅记录文档与验收，未编码、运行功能测试、构建或提交推送 |
| 2026-10-21 | 恢复资源分工通过（终态父子收敛后来改为 2026-10-23 STOPPING 收敛）：待开始 Instance 与到期可派发 Task 复用已有 64 个派发工作协程；阶段推进、正常完成和到期处理复用 Instance 检查组件；未知结算及终态父的子 Task 状态收敛交后台恢复有界处理。候选扫描不占派发或结果落库消费者，结果保存仍独立，不按五类候选另建五套池 | 真实恢复派发共享派发容量并须有限补充，数据库连接池、CPU 与 I/O 仍共享，不承诺硬隔离。完整候选 SQL、索引、周期、处理预算及公平机制仍待评审；下一步先审可派发候选查询，不将本轮批准扩展为五类查询全部定稿或编码授权。仅更新 DogX 设计、进度及验收要求，未编码、运行功能测试、构建或提交推送 |
| 2026-10-21 | 可派发候选查询及索引通过：WAITING Instance 按 ID 有界查找并复用现有部分索引；等待重试 Task 按到期时间和 ID 排序，JOIN 父运行且未截止，返回 Task ID、Instance ID 与原尝试编号，新增 `(next_dispatch_at, id) WHERE status = 3` | 查询采用数据库时间，不加锁、不写状态、不占机会、不查当前 Job 配置；有限投递并由派发事务重新授权，不设置永久消费游标。公共重试查询不按类型拆分，普通首次仍直接创建执行中 Task，分片首次待派发随封口协议补齐。下一项为绑定缺失观察到期候选；周期、批量、轮转及公平份额未定，仅记录设计和验收要求，未创建索引、编码、运行功能测试、构建或提交推送 |
| 2026-10-21 | 绑定缺失观察到期候选及精确部分索引通过：执行中、双绑定空、当前尝试登记超过观察期，JOIN 父运行且时间有效／未截止，按登记时间及 ID 限量读取并保留原尝试编号。交后台恢复结算，处理时重新取数据库时间、批量复核父，不直接派发、不凭摘要或 Data 判定 | 接受正常任务也会短暂经过该索引，以及绑定／状态变更的写入、WAL、HOT 适用限制和旧版本清理成本，用于换取避开已正常绑定执行的扫描收益；最终性能须压测验证。观察期、扫描周期及批量仍未定，下一项为父 Instance 推进与到期候选。仅更新设计和验收要求，未创建索引、编码、运行功能测试、构建或提交推送 |
| 2026-10-21 | 父 Instance 到期候选查询及索引通过（查询与索引计划后由 2026-10-22 统一复核替代）：运行中且整轮截止已到，按截止时间和 ID 限量只取父 ID，新增 `(deadline_at, id) WHERE status = 2`；扫描不锁、不写、不汇总 Task，有限投递已有 Instance 检查组件 | 先按完整结果尝试正常结束，仅明确 `NotReady` 才进入原超时父事务重新核验；错误、正常完成 UPDATE 零行及提交不明不转超时。不重派、不创建分片或 Post，不延长期限。第 4 类未到期漏推进候选仍待下一轮，扫描周期、批量和公平规则未定；仅记录设计与验收，未建索引、编码、运行功能测试、构建或提交推送 |
| 2026-10-22 | 未到期漏推进的恢复方向通过：放弃 Task 待推进标记与从 Task 表跨轮次发现候选，改从 Instance 侧低频、有界复核，确定轮次后定向查询 Task；不让每条分片结果改写父行 | 接受正常实例被复核及与正常路径竞争，不宣称只查异常；正常结果仍立即检查。Cron 单 Leader、不按 Job 分桶及无整轮 owner 保持，其他类别已通过的候选查询不因此撤销。复核节奏、节点分工、字段／索引及预算仍待评审，不将撤回提案的参数视为通过；仅更新执行说明和进度，未编码、运行测试、构建或提交推送 |
| 2026-10-22 | RUNNING Instance 统一复核完整提案通过：Leader 每 2 分钟启动或续扫，固定每趟数据库时间，`eligible_before = scan_now - 4 minutes`；按首次开始时间与 ID 每页 256 条读取并有界投递本节点检查组件，使用 `(started_at, id) WHERE status = 2`，替代原独立到期查询及索引计划。统一补阶段遗漏推进、完整结果结束和超时处理，正常结果仍在提交节点即时推进 | 仅接纳／合并成功推进临时游标，队满停在未接纳处；未完续扫、到尾重新开始，不重叠或永久越过，查询失败留位置下周期重试。新 Leader 就绪立即从头有限扫描并保留 4 分钟门槛，失主停新发现、已接纳检查可结束。保留结果优先与截止授权；短超时也受门槛约束，首查约 4～6 分钟仅为无积压估算，健康长任务仍可复核，不承诺恢复 SLA。其他候选职责不并入，无延迟队列、时间轮、分桶、next_check_at 或 owner；检查容量／并发和公平参数仍待总审。下一项为终态父的子收敛候选，仅更新文档，未编码、建索引、运行测试、构建或提交推送 |
| 2026-10-23 | 停止与超时完整重提案通过：新增非终态 STOPPING=6，单父行短事务提交停止决定并冻结 end_reason；人工停止最终 CANCELED、超时最终 FAILED，finished_at 仅最终结束时写入。阻止后续 Task 插入、Post 和重试授权，已授权在途 RPC 仍可能到达。提交后按 Instance 有界批量取消未结束 Task，全部子记录终态后条件结束父 Instance；远端按 Instance／已知地址去重通知、有限重试，与数据库处理独立 | 替代父先终态再收敛及相关恢复候选；不增加 task_cancel_pending、可靠通知表、owner 或延迟队列。管理成功只确认停止决定提交，STOPPING 仍阻塞丢弃、阻止 Job／组删除并受历史保护，活跃谓词和索引改为 IN(1,2,6)。同一 Leader 每 2 分钟独立按 STOPPING 有界发现候选、新 Leader 就绪补一轮，无 4 分钟门槛；独立分页预算、公平轮转、既有后台恢复容量，不占 64 派发协程、不全局扫 Task。RUNNING 的 2 分钟／4 分钟／每页256规则不变。数据库恢复以依赖恢复和公平容量为前提，不设固定耗尽丢弃；RPC 不保证最终送达或业务退出。仅记录已批准设计，未编码、修改迁移、运行测试、构建、提交或推送 |
| 2026-10-23 | STOPPING 恢复查询与取消索引通过：父候选使用 `idx_job_instance_stopping (id) WHERE status = 6`，每趟固定当前 STOPPING 最大 ID，按 ID 键集分页，仅接纳或合并后前移，队满停、未完续扫、到尾重来、切主重置。子取消使用 `idx_job_task_unfinished_instance (instance_id, id) WHERE status IN (1, 2, 3)`，每次限定一个 Instance 并有界读取，复用该索引检查最终 `NOT EXISTS` | 不全局扫 Task，不加 Task 游标或 OFFSET；成功处理的子记录退出候选，失败批次退出或退避，单轮达到预算让出容量并公平继续。父扫描游标不表示处理完成，低 ID 晚进入 STOPPING 由下一趟补查。索引覆盖所有未结束 Task，须验证正常写入及维护成本。页大小、取消批量、并发和时间预算留下一步容量评审；远端取消目标的有界去重查询仍待闭合，不宣布所有查询完成。RUNNING 已定规则不变；仅更新文档，未编码、创建索引、修改迁移、运行测试、构建、提交或推送 |
| 2026-10-26 | STOPPING 取消竞争与容量通过：每批另开短事务，先父主键 `FOR UPDATE NOWAIT`，成功并复核 STOPPING 后才查当前未结束子批；抢不到则回滚退出、不查子、不等锁、不立即重排，处理其他候选，旧合并提示不立即重抢。每批提交释放，空批在当前父事务内用 `NOT EXISTS` 条件结束父；父→子锁序、结果保存不锁父且提交后通知，RPC 不入事务 | 每页 256、每周期扫描／投递 5 秒且队满停止，无额外 1024 候选上限；本地容量 256 个 Instance（含排队及处理中）、并发 2、子批最多 256 条、每 Instance 每次最多 4 批或达到 2 秒后不再启动下一批、单条 SQL 超时 1 秒。成功且未完成者到队尾，竞争失败或技术错误退出；父扫描游标仅接纳后前移、不因处理失败回退。NOWAIT 不消除子 UPDATE 等待，超时不证明事务或连接已释放，提交不明不能认定成功／回滚。不扩展到派发竞争，不引入分桶、owner、随机退避队列或持久化领取标记；远端地址有界查询及通知容量另行评审。仅记录文档，未编码、修改迁移、运行测试、提交或推送 |
| 2026-10-26 | 远端取消通知查询与容量通过，并再次确认按批次（Instance）停止、不逐 Task 发 RPC：新 STOPPING 决定确认提交后独立交接数据库取消与通知，不按子批、两分钟恢复或只看到旧决定的重复请求重启通知；提交不明确认等边界仍可能重复，本地只合并排队处理中提示。父已终态时，已接纳通知仍继续原预算。地址索引 `(instance_id, executor_address) WHERE executor_address IS NOT NULL`，不按状态过滤；单条有界 SQL 索引跳跃不同地址，每页最多32，地址游标接纳后前移，交接后轮转，队满暂停 | 本地Instance与目标总容量各256，地址读取1协程、RPC发送16协程；地址SQL和单次RPC各1秒，最多3次总尝试含首次、间隔1秒，目标处理5秒含建连等待，Instance从本地接纳起60秒含排队查址发送，不翻页或合并续期。查询失败停止剩余枚举、已接纳目标继续；过期停新工作并取消在途Context，不承诺硬释放或送达。客户端同目标本次重试共用后关闭，不跨通知缓存；不添加全局通知锁、持久通知表、旧地址历史或全轮去重集合。索引写入/空间、实际计划及通信负载仍须验证。只记录文档，不编码、改迁移、运行测试、提交或推送 |
| 2026-10-26 | 派发候选合并与跨节点竞争通过：普通首次启动及已有 Task 新尝试登记明确由等待式父锁修订为父主键 NOWAIT；成功后仍按数据库时间、父状态、原候选尝试及预算登记，明确提交才发送。本地 WAITING 按 Instance 合并，已有 Task 按 Task ID＋原尝试编号去重，同 Instance 同时只进行一个登记事务，限制不跨 RPC。争锁失败回滚、跳过本次同组，不连续争锁或借旧提示立即重排；后续查询重新发现 | 保留正常提交后本地派发及 64 工作协程，不分桶、不加 owner 或持久化领取标记。补查独立小幅随机抖动，已接纳候选分组后不同起点处理；SQL 保持索引有序、不随机排序，不因竞争失败重查第一页或永久跳过。周期、抖动范围、队列容量、登记批量及公平份额下一轮集中确认，不套 STOPPING 数值。不扩展到生成、Post 创建、停止决定或普通 CRUD；仍有重复读取、事务及子 UPDATE 等待成本，不承诺性能必胜。只更新设计与验收，未编码、改迁移、运行功能测试、提交或推送 |
| 2026-10-28 | 通过[两端容量模型](execution.md#capacity-model)：进程级共享并发、适度有界缓冲、关键职责隔离和拥塞背压；派发不切正常／补查固定份额，Instance 小批轮转且空闲可充分利用；执行器允许有限排队后 ACK，满载明确 BUSY | 只记录容量行为，不提前固定 Go 并发原语或内部编码结构；执行器 64／排队 100 仅解释示例，不是默认值。既有创建、派发、停止和通知参数及状态协议保持；具体参数、交接计量、实现及性能验证继续闭合，未授权编码 |
| 2026-10-28 | 通过[派发候选容量、分页与退避](execution.md#dispatch-supply-defaults)：沿用派发并发 64，等待候选总量 256 包含临时页面及提示，查询每页最多 128 且受剩余空间限制；空闲补查 1 秒独立上下浮动 20%，每 Instance 每次先补最多 8 项，其他 Instance 无工作时继续使用余量。WAITING 固定本趟 ID 上界并按 ID 分页，重试固定本趟数据库时间并按 `(next_dispatch_at, id)` 分页；接纳、合并或按既定竞争规则跳过后才前移，满暂停，到尾下次从头 | 连续无有效登记进展最多读 4 页，只有本轮补查候选新尝试明确登记提交成功可恢复到 4 页，不累加；入队、NOWAIT 失败、失效、技术失败、提交不明及无关正常派发成功均不刷新。用尽退避，已有本轮候选后来成功可续扫，满仍暂停、到尾不立即重扫，同 Instance 冲突不立即重抢。512 只约束默认一次连续无进展补查的候选返回量，不是成功派发／秒或数据库访问量上限。并发、容量、补查间隔为主要部署配置，其他采用集中默认值；8 不是登记批量或 Instance 并发上限。不改低频恢复及状态协议，不外推执行器／回报，不冻结 Go 实现；登记批量、通信预算及剩余参数继续闭合，仅记录文档，未编码、建索引、运行功能测试、提交或推送 |
| 2026-10-28 | 通过[单次派发时间预算与确认失败处理](execution.md#dispatch-time-budgets)：总处理 10 秒从工作协程取出候选起算，含登记、客户端准备、RPC、响应保存及等待；登记事务 2 秒含连接等待与提交，接单 RPC 3 秒仅等接纳。ACK 绑定／BUSY 保存阶段最多 4 秒，最多 3 次含首次、单次数据库操作 1 秒、失败间隔 200 毫秒，受剩余总预算及停机宽限限制。观察默认 30 秒从数据库登记时间开始，包含提交耗时、不重置、节点一致；BUSY 独立延期 5 秒上下浮动 20% | 登记未明确提交或已提交但总预算耗尽且尚未发送时放弃发送，不免费重派；RPC 超时不推断未接单。响应保存成功或零行结束，可恢复错误／提交不明才有限重做原条件写入，耗尽释放，不重发业务或新增长期队列。元数据处理独立于已结束 RPC Context，父 STOPPING 仍按原条件；业务 Context 继续受整轮截止及取消控制，未实际退出不提前释放容量。观察到期仅结算资格、非恢复 SLA、不证明业务停止；BUSY 保存后不占协程等待，由既有一秒补查发现。主要部署参数为总预算、RPC、观察及 BUSY 延期，细分集中维护；协调 go-zero 原生客户端及执行器 Server 方法超时，不被默认两秒截断。扫描参数、执行器／回报容量及登记批量继续闭合，不冻结 Go 实现；仅记录文档，未编码、修改迁移、运行功能测试、提交或推送 |
| 2026-10-28 | 通过[排队、并发与满载处理](execution.md#capacity-model)六条原则并明确[执行器处理流程](execution.md#executor-processing-flow)：接单后进入有界业务等待队列，由有限并发业务处理者执行，结果交给有界结果队列，再由独立、有限并发的回报处理者发送。结果队满时当前处理者持有已完成结果等待交接、不领取下一项，独立回报继续；业务实际退出即清理活动防重，不等交接或回报 ACK | 优先简单实现，主要入口与慢资源受控，各段队列、有限并发及有界批次可说明整体积压，允许自然背压，职责独立不机械拆池，按完整流程选择 Go 组织并验证端到端效果。不新增接单、业务和回报共用的总量配额，不把“容量控制”写成额外组件，也不预定 Go 并发原语。ACK／BUSY、有限回报、调度端已确认结果的故障保留、取消和停机语义保持；既有创建、派发 64／候选 256、停止和通知参数不外推或撤销。后续确定业务与回报并发数、队列长度和重传参数；仅打磨设计说明、进度及验收要求，未编码、运行功能测试、提交或推送 |
| 2026-10-28 | 通过[执行器结果保留与发送安排](execution.md#executor-result-queue)及[回报初值](execution.md#reporting-defaults)：回报组件保留 256 条，覆盖待首发、待重试和在途；ACK、永久错误或次数耗尽后才释放原位置。发送并发 16，一次 RPC 结束即释放，不等待退避。新结果在接纳时刻即可发送，重试以上次调用结束加正退避计算；按已到期最早时间选择，同时间稳定，不切首发／重试固定份额。单次 2 秒，最多 7 次应用层调用含首次；六次退避为 1／2／4／8／16／16 秒，各上下浮动 20% | 256／16 为部署配置，其余为内部常量；47 秒只是基准退避合计，不是 TTL 或完成时限。满载仍由有限业务处理者持有结果等待交接，压力传回业务队列和接单，不新增全生命周期共用额度。业务并发／业务等待队列及调度端写库队列／批次并发仍待定，未批准业务 32／128 示例；保持单条 RPC、内存接纳 ACK、共享 30 秒停机及丢结果语义。不新增竞品对照或研究材料，仅同步设计、当前快照与验收要求，未编码、运行功能测试、提交或推送 |
| 2026-10-28 | 通过[执行器业务处理并发与等待队列初值](execution.md#executor-business-defaults)：每执行器进程业务处理并发默认 32、业务等待队列默认 128，均为可调整的部署配置。32 包含业务运行及业务已完成但结果交接尚未结束的处理，128 仅计尚未领取的等待任务、不含处理中；普通、准备、分片和收尾共用，不按 Job／Instance 叠加 | 满载时仍由当前有限业务处理者持有结果等待交接，占用处理位置且不领取下一项；业务实际退出时清理活动防重的规则不变。回报 256／16、单条 RPC、内存接纳 ACK、共享 30 秒停机及丢结果语义保持，不增加跨全生命周期额度，不预定 Go 线程或并发原语结构。下一项确定调度端结果队列、写库并发和批次初值，其余恢复、分片、错误隔离等未闭合事项保持；只同步设计与验收要求，未编码、运行功能测试、提交或推送 |
| 2026-10-29 | 通过[Task 结果逐条保存修订](data-contracts.md#task-result-persistence)：逐条条件 UPDATE、独立提交；允许批量预读上下文，但不合并提交，不设整批总超时或超时拆批。每条使用自己的数据库处理时间；单条失败或提交不明仅保留该原条目，不重放已明确成功的其他条目 | 可恢复数据库故障不按固定次数或 TTL 主动丢结果，进程退出仍可能丢失内存结果；单次数据库操作仍须有超时，具体值待定。共享 30 秒停机宽限及 Instance 原截止不变；执行器业务 32／128、回报 256／16 和既定重传参数保持。调度端队列、保存并发、预读条数及重试组织仍待评审，本次不批准候选数值或 Go 实现结构。仅同步设计、当前快照与验收要求，未编码、运行测试、构建、提交或推送 |
| 2026-10-29 | 后续通过[调度端结果保存安排与初值](execution.md#scheduler-result-defaults)完整提案：每条一次 JOIN 取得 Task／Instance 归属、认证组及本轮冻结重试上限、间隔，最终按原尝试、执行中状态和空或匹配绑定身份条件 UPDATE、独立提交。每节点组件默认保留 512 条待首次处理、待重试及在途结果，保存并发 8，单条处理超时 2 秒，三项均为部署配置；2 秒含连接等待、读取、更新和提交，不含排队退避 | 处理结束即释放并发，退避保留原条目原位置；新条目接纳时可处理，重试以上次结束加正退避计算，按到期最早及稳定同时间顺序选择，不切首次／重试固定份额。内部退避基准 1／2／4／8／16／30 秒、以后封顶 30 秒，各次上下浮动 20%；可恢复或性质不明的技术故障及提交不明不按次数或 TTL 丢弃，满载仍拒新，即使全部在退避。零行正常结束但不证明此前成功，不重放其他成功条目；无整批总超时或超时拆批。共享 30 秒停机宽限、Instance 原截止和内存易失边界不变，不预定 Go 原语。具体错误码映射、SQL／实现验证及其他恢复类别仍待定；仅同步文档，未编码、测试、构建、提交或推送 |
| 2026-10-29 | 后续通过[Instance 检查提示生命周期与初值](execution.md#instance-check-defaults)：本地按唯一 Instance ID 合并，默认保留 512 个（含合并等待、排队及处理中）、并发 8、单次处理预算 2 秒，三项部署可调；100ms 为首提示起算且不延长的内部初值。处理中首条新提示确定下一轮最早时间，本次结束且父未确认终态时，保留原条目转队尾一次，不重新申请位置；技术错误仍保留期间新提示，结束清理与登记协调防漏 | 无新提示时退出本次检查，错误不自行无限重查，后续事件或既定 Leader 复核补漏；零行、技术失败和提交不明不证明父终态或 NotReady。满时已有 ID 仍合并，新正常提示非阻塞放弃；2 秒不含排队及合并等待，不覆盖整轮生成、业务、派发 RPC 或停止收敛，也不承诺硬释放。跨节点仍可重复检查；Leader 扫描／投递预算及正常／恢复交接公平为下一项，不增加来源份额或固定 Go 结构。仅记录设计及验收要求，未编码、运行测试、构建、提交或推送 |
| 2026-10-29 | 后续通过[RUNNING 扫描预算与恢复补充](execution.md#instance-recheck-defaults)：扫描查询默认 1 秒含连接，每周期扫描／交接默认 5 秒含取时、各页查询、交接和等待，两项部署可调；续周期新预算沿用原 scan_now 与游标，单次受剩余预算。恢复新增 ID 仅在共享检查集合占用低于总量一半时接纳（默认 256），已有 ID 可合并，不按来源计数或切分并发；达到门槛或队满时在剩余预算内等空间，替代 RUNNING 满即退出 | 只接纳／合并才推进游标，不越过首未接纳项；等待不查询后续页、不持连接／事务、不忙循环或新增无界协程，只保留有界单页。门槛不是周期总量，到尾不在同周期重扫；查询失败结束本周期，预算到或失主停新发现，已接纳检查继续独立 2 秒。来源公平收窄为减少恢复挤占及已接纳项排队机会，持续或整窗口拥塞仍可能长期无进展，无固定恢复份额或 SLA。STOPPING 原规则保持；下一项为绑定缺失观察到期检查的扫描／结算安排与参数。仅更新设计及验收要求，未编码、运行测试、构建、提交或推送 |
| 2026-10-29 | 后续通过[观察到期扫描与结算安排](execution.md#unbound-recovery-defaults)：各节点独立串行、不重叠，实际结束后等待 10 秒 ±20%；每页 128、复核短批 16、单次数据库操作 1 秒、每次运行 5 秒，除复核短批为内部初值外均部署可调。每短批重新取时并读取父状态／冻结配置，每条 Task 条件 UPDATE、独立提交。固定本趟扫描边界，按登记时间和 ID 临时分页，跨运行继续；到尾结束，等待后新一趟从头 | 游标仅越过原序已处理连续前缀；单项写技术失败或提交不明可越过已尝试项但立即结束本次运行，后续新一趟重读仍合格的失败项；未处理项及取时／查询失败后的未处理项不可越过。不新增 NOWAIT、长期候选队列或处理池，不占派发／结果保存／Instance 检查并发，不将 128 当运行总量。30 秒观察仅为结算资格，不证明旧业务停止；父终止、预算及 RUNNING／STOPPING 规则不变。仅记录设计及验收要求，未编码、运行测试、提交或推送 |
| 2026-10-30 | 通过[结果回报错误分类与无效结果处理](data-contracts.md#result-error-classification)：ACK 释放；ResourceExhausted／Unavailable 有限重传，明确认证失败沿用 Unauthenticated／PermissionDenied、非法报文复用 bizerror 与 common.invalid_request，未知码及异常响应保守重传。缓存未命中不是认证失败，入口不查 Task，不解析错误文案。永久格式／大小／JSONB 不可表示须有独立证据，SQLSTATE 或失败次数不足以丢弃 | 后台先利用已有 JOIN 排除无合法目标、跨组、旧尝试、Task 终态等明确失效结果，再解析所需快照；零行不补查、不宣称此前成功。技术故障、提交不明、坏快照或未知版本保留并告警，父 STOPPING／到期不直接使子结果无效。业务 error／panic 正常保存，失败不采用 Data；512 满继续背压，无新增 TTL、队列或错误框架。下一项完整 Proto 与装配核对；仅记录设计和验收，未编码、运行测试、构建、提交或推送 |
| 2026-10-30 | 通过[执行 RPC 协议与装配](data-contracts.md#execution-rpc-contract)：统一 zrpc＋etcd、同进程独立入口，公共协议子包以 protoc 生成；确定三方法、请求体 GroupAuth、阶段 oneof、JSON 存在性及 UUIDv4 进程身份。全部执行业务入口共用组认证，保留标准 Health 并区分业务就绪；宿主统一进程配置、共享 30 秒预算和日志关闭 | 仅公共执行协议增加生成规范例外，现有应用 goctl 命令不变；尚未生成或实现。非 BUSY 明确拒绝结算、剩余错误映射、必要查询及开工检查继续闭合；不增加竞品目录／心跳、原生 gRPC 运行替换或 Go 并发实现约束，未运行功能测试、提交或推送 |
| 2026-10-30 | 通过[非 BUSY 明确拒绝结算](data-contracts.md#dispatch-rejection-settlement)：四类 Task 对可识别未接纳拒绝立即结算，有机会按冻结间隔等待重试、耗尽才失败，不返还或重复扣次；原身份、执行中及双绑定空条件保护并发，不锁父；复用有限响应保存预算 | 通用通信错误仍走不确定性规则；未落库可丢失拒绝诊断并由无绑定观察接续，不新增队列；远端截止不直接判父超时。剩余错误映射、必要查询及开工检查继续闭合，仅记录文档及验收，未编码、测试、提交或推送 |
| 2026-10-30 | 通过[派发与取消错误码映射](data-contracts.md#dispatch-cancel-errors)：共享协议仅新增 `job.executor.dispatch_rejected`，复用 `100001` 与 `ErrorInfo.Reason`；参数和认证沿用现有机制。明确拒绝必须发生在接纳前，未知错误保持原不确定性处理；拒绝耗尽的 Task 原因为 `dispatch_rejected` | 取消无活动仍成功，明确认证／参数错误结束，技术／未知错误有限同址重试；不因原执行到期拒绝取消，不改变数据库停止原因。公共标识统一声明，不新增错误包；普通 Task 必要查询及开工检查继续推进，仅更新规范与设计，未编码、测试、提交或推送 |
| 2026-11-02 | 通过[普通完成读取与异常处理](data-contracts.md#ordinary-completion-query)及[稳定结束原因](data-contracts.md#execution-end-reasons)：一次 LEFT JOIN 同快照、可空子字段、复用单例索引；普通子缺失／取消诊断异常但允许原截止超时，技术失败／坏配置／未知状态不降级为 NotReady；六个原因值、父正常失败统一值、等待重试 NULL 和停止原因冻结已定 | 仅更新设计正文、摘要、进度和[验收清单](verification.md#ordinary-completion-query-checks)；最小组接入、观察／停止接口、仅普通交付的分片边界及开工检查仍待评审，未编码、运行功能测试、提交或推送 |
| 2026-11-02 | 通过[正式组字段与四个管理入口](data-contracts.md#executor-group-management-contract)及[最小执行查询／停止契约](data-contracts.md#execution-management-contract)：组名和 Key 全表唯一、删除后不复用，已有组保留 ID／引用回填，旧已删且从未配置的 Key／Token 可保留 NULL；组无编辑；Instance 详情、Task 分页和整轮停止反馈明确 | 普通闭环调整为独立开发验收里程碑，在隔离环境验证；完整执行能力待分片完成后正式发布，不新增临时模式开关／候选过滤索引／过渡状态。下一项为普通执行开工检查；仅更新文档及验收，未编码、生成协议、执行迁移、运行功能测试、提交或推送 |
| 2026-11-04 | 明确产品版本兼容规则：同一产品版本不兼容开发中间态；只有存在已发布的历史 Release、已确定升级到后续版本且确有不兼容时，才允许针对来源版本、目标版本及实际差异编写必要兼容 | 覆盖 2026-11-02 记录中的旧组回填和历史 NULL 例外，撤销开工草案的分阶段升级及迁移 CLI 扩展要求；组字段按完整非空契约实施，其他组身份、引用保护和执行协议保持。仅修正规范与设计，未改业务代码或操作数据库 |
| 2026-11-04 | 确认并记录[普通执行模块六阶段开发安排](ordinary-readiness-review.md#ordinary-implementation-order)：通信与运行基础、数据基础与组管理、普通成功链路、失败与重试、停止／超时／恢复、普通模块整体验收 | 每阶段可分多次开发，测试同步补齐，结束时报告成果、验证结果及剩余问题并更新状态；所有阶段未开始，下一步为阶段 1。阶段 6 只代表普通模块独立验收，完整执行发布仍待分片完成；本次仅更新文档，未编码、执行迁移、运行功能测试、提交或推送 |
| 2026-11-04 | 阶段 1 开始实施：公共执行协议、处理器注册、认证与 RPC 适配、独立 Server／Client 工厂和入站停服屏障 | [实施记录](ordinary-readiness-review.md#ordinary-stage1-progress)区分已实现组件、实际验证及剩余生产装配；阶段 1 尚未整阶段完成，不将测试替身视为任务执行闭环。未执行业务库迁移、提交或推送 |
| 2026-11-04 | 阶段 1 续做：两域 Main 接入统一宿主，沿用 Shutdown 配置并落实默认共享 30 秒退出；初始化纳入信号协调，Job 分离 Stop／Wait／Close，System 清理幂等 | Windows 与 Linux 的真实信号及 11 个宿主故障场景通过；第二执行入口的真实队列／组缓存接线仍待完成，未宣称整阶段验收。详见[实施记录](ordinary-readiness-review.md#ordinary-stage1-progress) |
| 2026-11-04 | 阶段 2 开始：Task 公共表／模型、组完整字段与唯一约束、STOPPING 活跃判断、组仓储和凭据缓存；真实组加载及 60 秒刷新接入 Job 宿主 | 隔离 PostgreSQL 18.6 验证迁移、约束、JSONB、缓存和关闭连接；管理四入口、Task 执行仓储及第二入口真实消费者仍待完成，阶段 1／2 均未整阶段验收。详见[阶段 2 实施记录](ordinary-readiness-review.md#ordinary-stage2-progress)；未迁移业务库、提交或推送 |
| 2026-11-04 | 阶段 2 续做：组管理四个 API／RPC、删除引用保护、System 权限资源、中英文错误与详情日志保护 | 真实 PostgreSQL 并发、权限路由、RPC 日志与生成一致性验证通过；普通任务执行链路仍未完成。另按用户授权建立 Windows 固定测试路径及六个仅回环访问的程序规则，关闭测试默认开发监听；详见[实施记录](ordinary-readiness-review.md#ordinary-stage2-progress)。未提交或推送 |
| 2026-11-04 | 阶段 3 开始：普通首次启动 Registrar、Instance NOWAIT／取时／条件更新仓储、唯一普通 Task 插入和 ACK 条件绑定仓储 | 仅返回新建且确认提交的尝试；绑定零行不补查，尚未装配有限保存重试。相关普通及隔离 PostgreSQL 首次启动／仓储／Logic 回归通过，详细范围见[阶段 3 记录](ordinary-readiness-review.md#ordinary-stage3-progress)。结果保存、运行队列、实际 RPC 派发及第二入口仍未完成；未提交或推送 |
| 2026-11-04 | 阶段 3 续做：原生报告类型移到 `internal/taskresult`，普通单条结果 Saver 与 JOIN／独立提交 UPDATE 仓储已实现；同一 UPDATE 共用数据库处理时间，技术错误保留与明确无效释放分开，异常成功 Data 按 PostgreSQL 输入规则独立验证 | 失败转等待重试仅保存事实，不授予下一次执行；队列、消费者、实际派发／业务执行、第二入口和父完成未接线。全仓普通测试、真实 PostgreSQL／etcd 集成及受影响包 integration 标签 vet／lint 已通过，详细范围见[结果保存续做](ordinary-readiness-review.md#ordinary-result-save-progress)；阶段 1、2、3 继续进行中，未提交或推送 |
| 2026-11-04 | 阶段 3 续做：调度端结果 Queue、有限保存消费者、到期正退避和 RPC 接纳适配已实现；Stop 关闭接纳，Drain 使用共享期限，Wait 等待实际工作退出 | 默认 512 条含在途、并发 8；技术错误保留原报告，无 TTL 或次数丢弃，退避不占保存协程。当前验证统一见[结果队列续做](ordinary-readiness-review.md#ordinary-result-queue-progress)；生产 YAML／宿主接线、实际派发与业务执行、第二入口和父完成仍未完成，阶段 1、2、3 继续进行中；未提交或推送 |
| 2026-11-04 | 阶段 1／3 续做：Job Main 装配管理与结果双 Server，ExecutionRPC／ResultPersistence 接入真实组缓存、Queue 和 Saver；共享 ServiceConf、独立监听／发现 Key、先初始化后注册 | Stop 先关结果接纳再取消生产；Drain 立即传共享期限并并发等待运行组件，Wait／Close 以实际退出为准。初始化 I/O 不保证立即响应宿主取消；只更新示例、不改私有配置或增加兼容分支。实际验证见[Job 生产接线记录](ordinary-readiness-review.md#ordinary-result-wiring-progress)；System 执行器、实际派发／业务执行和父完成待实施，阶段 1、2、3 继续进行中；未提交或推送 |
| 2026-11-04 | 阶段 3 续做：公共执行器 Runtime／reporter 实现真实四阶段处理器调用、活动防重、按 Instance 取消、有界交接与有限回报；业务默认 32／等待 128，回报保留 256／并发 16；修正 Post 请求必须 RequiredPost=true 的校验 | 业务实际退出先释放活动，回报退避不占发送者，Stop／Drain／Wait 区分关闭接纳、共享截止与实际完成；确定非法成功输出释放并安全诊断，不伪造业务失败。真实普通 Dispatch 至 PostgreSQL 联测覆盖首回报故障后重传且业务只执行一次，实际验证见[执行器运行记录](ordinary-readiness-review.md#ordinary-executor-runtime-progress)。System 内嵌接线、实际派发编排、父完成与查询待实施；分片生成存储推进未实现，阶段 1、2、3 进行中，4～6 未开始；未提交或推送 |
| 2026-11-05 | 阶段 1／3 续做：System 内嵌独立执行器 Server，公共 JobExecutor 配置和 Runtime 32／128／256／16 接入宿主；统一 ServiceConf，依赖初始化后注册，provider 取得真实 Adapter，Stop／Drain／Wait／Close 区分关闭接单、共享截止与实际释放 | 当前 Main 无生产处理器，未知名称拒绝，未新增 system.cleanup 或空业务包；配置 Token 保留 YAML 引号，组名／发现 Key 手工对应管理记录，回报使用非阻塞 zrpc＋etcd。全仓普通测试、System svc 真实 PostgreSQL 18.6／Redis 7.4.2／etcd 3.5.21 集成全包及 Job execution 集成包已通过；真实构建并运行 System RPC／API 的认证／RBAC Main 进程 E2E 及带 integration＋e2e 标签的受影响范围 vet 均通过，同范围 lint 通过、0 issues，完整范围见[内嵌接线记录](ordinary-readiness-review.md#ordinary-system-executor-wiring-progress)。该 E2E 不包含执行器业务或 Task 触发至父终态。CI 参数已同步。实际派发、父完成与查询仍待实现，阶段 1、2、3 进行中，4～6 未开始；未提交或推送 |
| 2026-11-05 | 阶段 3 续做：一次普通派发 Dispatcher、组客户端借用 Sender、ACK／BUSY／明确拒绝有限保存及单条条件仓储已接通 | 只有新登记明确提交才发送一次；未知结果不退款、不重发，保存零行不补查。真实组件链路覆盖 ACK 与业务结果两种先后顺序，验证范围见[一次普通派发记录](ordinary-readiness-review.md#ordinary-dispatch-operation-progress)。候选供给、64 个协程、组客户端缓存／清理及部署参数接线仍待完成，父终态与重试调度未实现；未提交或推送 |
| 2026-11-05 | 阶段 3 续做：WAITING 固定上界查询、默认 64 并发及 256 等待的首次派发 Runtime、即时提示、四页无进展退避、组客户端复用／删除清理及 Job 生产接线 | 不依赖 Leader，不扫描尚未支持登记的 Task 重试；登记实际返回后释放协调，停机给派发和结果共享善后期限，连接在实际退出后关闭。主要部署参数和执行器原生方法超时已接通，验证与未覆盖范围见[首次派发生产记录](ordinary-readiness-review.md#ordinary-dispatch-runtime-progress)。父完成、管理查询及阶段 4～6 仍待完成；未提交或推送 |
| 2026-11-05 | 阶段 3 完成：普通父完成 Checker、512／8／2s 提示合并队列、生产接线，GetInstance／ListInstanceTasks、分页索引及权限／日志保护 | 真实非 Leader 生产组件从触发、业务执行、回报保存推进至父终态，覆盖成功／最终失败及空摘要；存在性查询不读取冻结配置。实际测试及限制见[最新记录](ordinary-readiness-review.md#ordinary-completion-progress)。阶段 1～3 基础开发与专项验证完成，下一步阶段 4；完整进程／跨节点／负载验收仍待阶段 6，未提交或推送 |
| 2026-11-05 | 阶段 4 开始：普通重试候选、父 NOWAIT 下按原尝试条件登记、首次与重试共享派发 Runtime 及生产接线 | 真实非 Leader 组件链路覆盖失败后成功、失败耗尽、BUSY 返还后成功及明确拒绝后成功，原 Task／冻结配置／截止不变；共享分页预算、候选去重与有界冲突退避已验证。全仓普通测试、受影响四包真实集成、Job vet／lint 通过，实际范围见[普通重试记录](ordinary-readiness-review.md#ordinary-retry-progress)。结果不确定结算仍待后续，阶段 4 未整阶段完成；阶段 5、6 未开始。前端未改，未提交或推送 |
| 2026-11-05 | 阶段 4 续做：普通未知结果结算核心完成；最多 16 条候选一次 JOIN 复核，每条独立提交，保留原尝试与预算，确认提交后才提示父检查 | 全仓普通测试、repository／taskrecovery／taskresult／dispatch 的真实 PostgreSQL integration 标签全包及 Job 全域 integration 标签 vet／lint 通过，lint 为 0 issues，见[未知结果结算记录](ordinary-readiness-review.md#ordinary-unknown-progress)。本轮未重跑 etcd／真实 RPC、Main E2E、race、覆盖率或压测。阶段 4 普通失败与重试核心逻辑及专项实现完成；尚无观察扫描或 ServiceContext 生产调用，自动恢复未启用，已绑定 Task 不提前重派；阶段 5、6 未开始。前端未改，未提交或推送 |
| 2026-11-05 | 阶段 5 开始：普通无绑定候选查询、独立串行观察扫描与 ServiceContext 启停接入；固定整趟边界及复合游标，逐运行重新取得预算，有界分页与每条独立结算 | 自动双扫描器可接续共享重试与父完成；离线 etcd 的生产宿主可自动发现既有候选；3 万行数据的初页／续页计划使用观察索引和父主键。配置 `UnboundRecovery` 向扫描器和 Settler 传入同一观察期与操作预算，当前实际验证及限制统一见[自动观察扫描记录](ordinary-readiness-review.md#ordinary-unbound-scan-progress)。STOPPING 收敛、超时及 Leader Instance 复核仍待阶段 5 后续实施，阶段 6 未开始；前端未改，未提交或推送 |
| 2026-11-05 | 阶段 5 续做：人工停止决定、单批父 NOWAIT／有限子取消／最终父状态及单目标远端取消组件完成 | 全仓普通测试、真实 TCP、repository／instancestop 全包真实 PostgreSQL 集成及既有派发／结果／父完成／观察组件回归、Job／jobexecutor integration 标签 vet 和 lint 均通过，lint 为 0 issues，完整范围见[停止核心记录](ordinary-readiness-review.md#ordinary-stop-core-progress)。StopInstance API、取消工作队列、地址枚举及通知队列、STOPPING Leader 发现和自动超时决定尚未接线，不能称完整停止功能已可运行。无迁移、协议或生成文件变化，前端未改，未提交或推送 |
| 2026-11-05 | 阶段 5 续做：人工 StopInstance RPC／API、权限迁移、有界数据库取消、递归地址枚举及独立通知、宿主启停、新 Leader 即时及每 2 分钟 STOPPING 发现接通 | 全仓普通测试、真实 PostgreSQL／TCP 停止链路、etcd 双节点 hook 与生产宿主接任测试、受影响范围 integration 标签 vet／lint 均通过，lint 为 0 issues；通知期限竞态已修正并复验，完整范围见[停止运行链路记录](ordinary-readiness-review.md#ordinary-stop-runtime-progress)。恢复只补数据库收敛，不周期重发远端通知；自动超时决定和 Leader RUNNING 复核尚未实现，阶段 5 仍进行中。前端未改，未提交或推送 |
| 2026-11-05 | 阶段 5 普通核心完成：自动超时决定、RUNNING 有界发现与半容量交接、Leader RUNNING／STOPPING 共用周期及生产宿主接线 | 普通最终结果优先；明确未完成到期才锁父重新取数据库时间，已有停止／终态不覆盖。全仓普通测试、真实 PostgreSQL／etcd 宿主与竞争、3 万行索引计划、Job／jobexecutor integration 标签 vet／lint 通过，0 issues；实际范围见[超时与统一复核记录](ordinary-readiness-review.md#ordinary-timeout-recovery-progress)。阶段 6 尚未开始，本轮未运行 CI／race／覆盖率／正式负载或完整 Main E2E；无新迁移、协议、生成文件或前端改动，未提交或推送 |
| 2026-11-05 | 阶段 6 开始：真实 HTTP 管理及普通执行进程 E2E、System 测试处理器复用生产 run、A 宿主关闭后 B 独立回报完成、独立 CI 步骤 | HTTP E2E 与跨宿主回报最终复验通过，七入口认证／授权、组管理、成功／重试／停止、业务持久化及生成 Token 日志检查覆盖范围见[最新记录](ordinary-readiness-review.md#ordinary-process-e2e-progress)。不是 OS 骤停或优雅停机验收；GitHub 未运行，race、覆盖率、正式负载与系统性故障等仍待完成，阶段 6 保持进行中；未提交或推送 |
| 2026-11-05 | 阶段 6 续做：Windows 真实 Job／System Main 信号退出、System 已接纳业务在停止接单后完成，以及 Job 进程 kill 后替换接续 | 最终 Windows 全包随机顺序 E2E 通过37.469s，含原认证、完整 Job HTTP 与本轮三个进程场景；实际断言与限制见[进程生命周期记录](ordinary-readiness-review.md#ordinary-process-lifecycle-progress)。连接释放不推定 Close 调用顺序；未观测首次回报失败／重传次数，不覆盖已 ACK 结果丢失或执行器死机。Unix 尚未运行，预算耗尽、其他故障、race、覆盖率与负载待验收；CI 新步骤已接入但未推送运行，阶段 6 仍进行中 |
| 2026-11-05 | 阶段 6 续做：System 不配合取消的业务耗尽退出预算；真实 Job 进程对已 ACK 排队结果正常排空／骤停丢失，以及替换节点自然选主恢复 | Windows 最终 System 全包双标签随机顺序通过 45.968s，Job 补强后两场景专项通过 24.182s；受影响范围 vet／lint、Linux 双消费者交叉编译和 actionlint 通过。具体断言及预设过期协议数据的边界见[最新记录](ordinary-readiness-review.md#ordinary-result-loss-progress)。System 退出后仅观察 1 秒，不代表完整退出至最终超时链路；Unix 实际运行、其余故障、race、覆盖率及正式负载仍待验收，阶段 6 进行中。隔离服务已清理，前端干净，未提交或推送 |
| 2026-11-05 | 阶段 6 本机收尾：新增执行器骤停至最终超时、etcd 保留数据重启后原进程恢复两个故障 E2E，并接入独立 CI 步骤 | 执行器专项通过 257.270s，真实等待生产 4 分钟年龄门槛；补强后的 etcd 场景在最终 System E2E 中通过 42.59s，全包排除已冻结四分钟专项后通过 90.490s，全仓普通测试通过。全仓双标签 vet／lint、生成一致性与工作流检查通过，待提交内容检查未发现真实凭据，达到可提交节点；结果统一见[本机收尾记录](ordinary-readiness-review.md#ordinary-local-checkpoint)，远端及负载验收尚未完成，阶段 6 仍进行中；本轮隔离服务与测试进程已清理，前端干净，未提交或推送 |
| 2026-11-06 | 阶段 6 首次 CI 失败诊断：实现 e5946ab 已推送；修正测试计数器同步，以及查询计划测试的索引断言与日志 SQL 回放 | Go lint、Windows tests 通过，Linux race 与 PostgreSQL 集成失败，后续 E2E 及覆盖率上传跳过。svc 普通全包及 RUNNING 绑定参数专项已通过；统一 SQL＋Vars 捕获后的最终 12 项查询回归通过 11.238s，受影响两包 integration 标签 vet／lint 通过，完整证据与旧零行计划更正见[CI 修正记录](ordinary-readiness-review.md#ordinary-ci-correction)。用户决定不修改 go-zero，上游退出 race 保留，不跳过测试或抑制 race；本轮仅测试及文档修正，尚未提交或推送，阶段 6 与代表性负载验收仍未完成 |
| 2026-11-06 | 阶段 6 后续 CI 与退出竞态临时处理：`0949b8f` 已推送，集成及全部 E2E 通过；用户批准等待官方修复前暂用 info 测试配置，并删除 Run 的重复停机配置写入 | 上游 [#5876](https://github.com/zeromicro/go-zero/issues/5876) 未修复，关闭 info 日志时的竞态覆盖明确暂缓；不采用 fork／补丁，不修改生产日志级别或 1 秒／30 秒停机策略，保留真实信号、全部生命周期断言及 race 检测。本轮五包 Linux race／Windows 回归及 rpchost lint 已通过，结果统一见[临时处理记录](ordinary-readiness-review.md#ordinary-shutdown-race-workaround)；提交后的新 CI 与代表性负载仍待验收，阶段 6 进行中 |
| 2026-11-06 | 普通阶段 6 适用验收完成，达到本轮测试／文档增量可提交节点 | 生产实现 `7f9d32e` 的全部 CI 已通过；[代表性负载](ordinary-load-validation.md)五轮及原普通执行 E2E 回归通过，受影响四包三标签 lint 为 0 issues，详见[收尾记录](ordinary-readiness-review.md#ordinary-load-closeout)。本轮增量尚未提交、无新 CI，提交后继续远端验证；info 日志临时覆盖限制保持，分片／Post 和 System 生产处理器仍属后续范围，不代表完整第一版发布 |
| 2026-11-06 | 普通负载验收提交 `5b07ec0` 的远端 CI 已完成成功 | [运行 42173750198](https://github.com/tokyolab/dogx/actions/runs/42173750198) 对应完整提交 `5b07ec01c28e21d0c9ae4fb7bf4cbd4e53495e55`；本次记录时以只读 GitHub CLI 核实 `completed/success`，不将该 CI 结论扩大到后续分片修订 |
| 2026-11-06 | [最终分片清单](task-model.md#explicit-shard-contract)及[单份 JSON 输入存储](data-contracts.md#shard-input-storage-contract)通过 | Pre 每个元素对应一个逻辑分片 Task，平台不二次分组；单份 RawMessage／Proto bytes／input_data JSONB 与 result_data 输出分离。嵌套数组、重复元素、顺序、零清单、调度端写库及生成恢复边界同步正文和验收。现有 Go／Proto 及迁移未改，生成持久化协议待下一项评审，本次只改文档，未提交或推送 |
| 2026-11-06 | [Pre 清单保存与整组原子生成](execution.md#atomic-shard-generation)通过 | 准备输出先原子保存，父 NOWAIT 下检查已存在分片并一次事务生成全组；显式零清单本身是完成事实，不新增完成标志或续写进度。整轮最多 500 个业务分片，超限沿用无效输出规则；同步现行单轮基准及验收，不改历史实测，代码／迁移尚未实施，本次仅改文档 |
| 2026-11-06 | [Pre 首次启动与分片首次派发](execution.md#sharded-start-dispatch)通过 | 父／Pre 合并首次启动，分片待执行 0／0 初值、父授权后首次登记 1／1，补齐待执行分片共享供给与丢失提示恢复；沿用现有通信及尝试规则，具体 SQL／索引、运行交接和验证待落实，本轮仅同步文档 |
| 2026-11-09 | [Post 创建、固定统计输入与首次派发衔接](execution.md#post-creation)通过 | 父锁外取得稳定三个计数，独立批准父 NOWAIT 唯一创建待执行 Post；input_data 扩展保存固定统计，0／0 初值、1／1 首次登记及待执行供给恢复与分片共用。同步正文、进度及验收；具体 SQL／索引、运行交接及实现验证待完成，本轮仅改文档 |
| 2026-11-09 | [分片查询、索引与组件交接](#sharded-query-handoff-approval)通过 | 新增输入列和替换派发部分索引的 DDL、统一到期 Task 查询及两来源游标、阶段最小读取与索引复用已记录；原 2 秒检查预算扩展至最多一次阶段数据库动作，区分两种派发提示。索引可用性与实际计划／性能分开，生成和父过滤成本待实测；代码及迁移未实施，本轮仅改文档 |
