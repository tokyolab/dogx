# 分片执行模块开工检查与开发安排

当前状态（2026-11-12）：**阶段 1～5 的本机实施与已确认范围验收完成，已达到可提交节点；远端 CI 尚待提交后取得。** 完整分片生命周期、故障接续、跨节点进程、查询计划及两调度／两执行器代表性负载均已有本机证据。实际结果见各阶段记录及[负载验收](sharded-load-validation.md)，最新收尾见[阶段 5](#sharded-stage5-progress)。当前未提交或推送，不将本机结论代替 Linux、race 或 CI 覆盖率。

2026-11-09 的开工静态检查未发现需要重新选择产品行为的阻塞。下方代码差距表保留该检查时点的事实，实施进度以阶段表和后续记录为准；不将设计批准或代码完成等同于测试、性能验收通过。

设计依据：[最终分片清单](task-model.md#explicit-shard-contract)、[输入存储](data-contracts.md#shard-input-storage-contract)、[整组原子生成](execution.md#atomic-shard-generation)、[Pre 首次启动与分片首次派发](execution.md#sharded-start-dispatch)、[Post 创建](execution.md#post-creation)、[查询与索引](data-contracts.md#sharded-query-contract)及[组件交接](execution.md#sharded-component-handoff)。本页不重新定义这些业务规则。

## 范围与现有基础 {#sharded-readiness-scope}

本次开发范围是 Pre → 业务分片 → 可选 Post → Instance 终态，以及各阶段共用的重试、停止、超时和故障接续。Pre 返回的每个外层元素就是一个最终 Task，平台不再切分；每轮最多 500 个业务分片。普通任务仍不支持 Post，已有普通链路须持续回归。

现有公共执行组件已有四阶段处理器、阶段 oneof、派发／取消／回报三项 RPC，以及业务排队、独立回报和生命周期管理。Job 已有派发、单条结果保存、Instance 检查、停止与恢复组件；System 已内嵌执行器。此次扩展这些组件，不新增 Server、分片专用队列或包办业务的大 Service。

管理页面、历史清理和具体 System 生产处理器不随本项扩展。真实闭环测试通过既有宿主注入测试处理器，不为了测试新增虚构生产业务。本项完成也不能代替其他第一版发布条件。

## 代码差距与实施归属 {#sharded-code-gap}

| 改动范围 | 当前代码事实 | 必须落实 |
| --- | --- | --- |
| 公共处理器与协议 | [Processor](../../../pkg/jobexecutor/processor.go) 的单片输入仍为数组；[Proto](../../../pkg/jobexecutor/protocol/execution.proto) 仍为 `repeated bytes`。Pre 已区分缺失与空清单，尚无 500 上限 | 单片改为一份 `json.RawMessage`／`bytes`；同步 Invocation、RPC 转换、输入复制和调用；保留显式空清单的外层消息表达，补齐校验与生成测试 |
| Model 与迁移 | [Task Model](../../../apps/job/internal/model/job_task.go) 已有原始 JSON 读写语义，尚无输入列；现有索引只覆盖等待重试 | 新增可空、无默认值的 `input_data JSONB`；新迁移将 `retry_due` 替换为已定 `dispatch_due`，不保留重叠索引或增加阶段索引 |
| Task 仓储 | [Task Repository](../../../apps/job/internal/repository/job_task_repository.go) 已有普通首次插入、重试及结果写入；取消、绑定等已有通用能力 | 增加 Pre 首次插入、整组分片插入、Post 固定输入及单例创建；统一到期候选与首次／重试登记，返回发送所需阶段和固定输入；扩展普通专用的 BUSY、拒绝与未知结算条件 |
| Instance 读取及推进 | [Instance Repository](../../../apps/job/internal/repository/job_instance_repository.go) 的完成查询只关联普通 Task；[Checker](../../../apps/job/internal/instancecheck/checker.go) 跳过分片模式 | 保留普通最小路径；增加阶段最小事实、定向存在性和必要统计。现有检查在原共享预算内选择一次阶段数据库动作，复用父正常完成条件更新 |
| 派发与发送 | [Registrar](../../../apps/job/internal/dispatch/registrar.go) 的首次／重试跳过分片，登记结果缺阶段输入；[Runtime](../../../apps/job/internal/dispatch/runtime.go) 提示只表达待开始父；[发送器](../../../apps/job/rpc/internal/execution/dispatch.go) 只构造普通请求 | 校验冻结模式与阶段；待开始父选择普通／Pre，统一到期 Task 处理首次分片／Post及所有阶段重试；区分两类提示，提交后发送时不再逐 Task 查输入 |
| 结果及平台结算 | [Saver](../../../apps/job/internal/taskresult/saver.go) 与[未知结算](../../../apps/job/internal/taskrecovery/settler.go) 存在普通类型限制；JSONB 异常诊断当前仅检查普通 Data | 按持久化类型处理输出与冻结配置，Pre 成功和完整清单一起保存；诊断实际待保存的 JSON 内容；拒绝、未知结算等提交后的父检查提示继续保留 |
| 生产装配与验证入口 | [Job 装配](../../../apps/job/rpc/internal/svc/servicecontext.go) 目前先启动检查再建立派发；[System 测试宿主](../../../apps/system/rpc/system_execution_e2e_test.go) 只注册普通处理器；[CI](../../../.github/workflows/ci.yml) 显式选择 E2E 名称 | 双向提示依赖在开始消费前装配完成；扩展真实宿主分片处理器及故障夹具，同步 CI 的 `-run` 选择器 |

实施时特别保留以下边界：

- SQL NULL 与 JSON `null` 不混淆；任意 JSON 和大整数不经过浮点数转换。Post 固定统计与业务分片输入不被结果保存、BUSY、重试或取消清空。
- 无效 Pre 成功输出沿原拒绝／丢弃契约处理，不能伪造成业务 FAILED；真实业务 error 不保存部分成功清单。结果接收入队不代表后台保存成功。
- [事务适配](../../../apps/job/internal/repository/transaction.go)只绑定仓储，不承载阶段规则或自动重试。父主键 NOWAIT、锁后取数据库时间、父→子顺序、事务内无 RPC 和明确提交后通知继续有效。
- 当前取消用的 `ListUnfinishedIDs` 要求事务并返回取消 ID，不能为了复用而把阶段存在性检查放入父锁。阶段存在性与聚合在同一个 Task Repository 内增加专用只读操作。
- 正常完成仍先读稳定结果、再按父仍 RUNNING 条件更新；不能机械增加父锁或截止过滤。生成、Post 创建和新尝试授权继续检查原截止及停止状态。
- 一次检查原默认 2 秒预算包含读取及一次阶段数据库动作。500 行及报文大小限制不保证耗时达标；合理载荷持续超出预算须优化或调整可配置预算，不靠重复失败掩盖无法推进。

## 建议开发顺序 {#sharded-implementation-order}

下表是开工检查形成、随后按用户继续指令推进的实施安排。阶段只划分交付依赖，不增加产品规则，不提前固定 Go 并发原语或临时模式开关。

| 顺序 | 主要工作 | 阶段完成标准 | 状态 |
| --- | --- | --- | --- |
| 1．输入协议与数据基础 | 单份 JSON 公共契约及所有调用者；协议重新生成；Pre 清单边界；输入 Model、迁移和索引替换 | 单份任意 JSON、缺失／空清单、500／501、JSON null 和大整数验证通过；真实 PostgreSQL 迁移与输入读写通过，普通调用者保持可用 | 已完成实现与本机验证；见[阶段 1 记录](#sharded-stage1-progress) |
| 2．Pre 与分片执行链路 | Pre 首次启动、成功清单保存、整组生成；统一候选、首次／重试登记、阶段发送与结果保存；生成后的派发提示 | 真实数据库证明整组提交或回滚、并发不重复；Pre 结果可生成并派发固定分片；相关 BUSY、拒绝、未知结算和旧尝试保护同步生效 | 组件实现与本机验证完成；完整生产检查接线归阶段 3，见[阶段 2 记录](#sharded-stage2-progress) |
| 3．Post 与整轮完成 | 固定统计、唯一 Post 创建、首次派发和重试；最小阶段读取、零分片及最终判定；完成生产提示装配 | 普通与分片完整链路可共存；零分片、部分／全部失败、可选 Post 及 Post 失败的终态正确；阶段推进不循环占用检查预算 | 已完成实现与本机验证；见[阶段 3 记录](#sharded-stage3-progress) |
| 4．故障与跨节点验收 | 提示丢失、节点退出、提交不明、迟到 ACK／结果；阶段创建与停止／截止竞争，恢复和生命周期回归 | 真实依赖及进程场景证明可恢复工作接续、旧尝试不覆盖、STOPPING 最终收敛；普通回归通过 | 本机验收完成，远端结果待提交后取得；见[阶段 4 记录](#sharded-stage4-progress) |
| 5．查询、负载与交付 | 真实 GORM 执行计划、代表性载荷、共享检查排队；补齐 CI 入口及文档，完成本机检查后提交验证 | 本机结果、未完成项和可提交节点明确；推送后取得该提交的 Linux／race／覆盖率结论，正式负载按事前确认门槛判断 | 本机已确认范围完成、达到可提交节点；[查询与负载实测](sharded-load-validation.md)已记录，远端 CI 待提交后验证 |

测试随所属改动同步完成；阶段 4 是系统性故障验收，不表示此前先省略重试、停止或并发保护。必要的仓储、接线和测试随依赖一起修改，不为了阶段边界制造过渡实现。阶段结束记录实际修改、已运行检查、剩余事项和下一步。

## 验证安排与可提交节点 {#sharded-validation-plan}

| 验证范围 | 本机可完成的工作 | GitHub 上完成的工作 |
| --- | --- | --- |
| 契约与静态检查 | 按 [README 固定命令](../../../README.md)重新生成公共 Proto；相关普通测试、全仓普通回归、受影响标签的 vet／lint、生成结果一致性 | Linux 普通测试及 race，现有 Windows 测试与 lint 工作流 |
| 持久化与竞争 | 真实 PostgreSQL 隔离 Schema 测试迁移、整组事务、父 NOWAIT、条件登记、固定输入及停止交错；etcd 和 RPC 集成 | CI 依赖上的 integration 测试及对应回归 |
| 完整进程 | Windows 真实生产宿主、注入测试处理器，验证完整阶段、跨节点回报、停止与恢复；复用现有进程夹具 | Linux 真实进程 E2E 和退出验证；新增用例须同步工作流显式选择器 |
| 查询与负载 | 统一到期查询初页／续页、历史量及大量不合格父过滤；阶段查询次数、生成事务耗时、检查排队、尾延迟和数据库等待 | 不把功能 CI 当作负载成绩；需要远端负载时另按已确认环境和门槛执行 |
| 覆盖率 | 本地数据仅用于定位缺口，不代替该提交 CI | 分别报告 unit、integration（含普通测试）、合并及 patch，注明 Go 原始值与 Codecov 排除口径；缺失则说明无法取得 |

本机测试遵守[Windows 固定包装器与隔离规则](../../development/testing.md#windows-local-tests)，同一 checkout 不并行启动多个测试命令。开工检查本身只做静态核对；实施后工具、依赖及测试结果记录在对应阶段，不能将历史通过当作新改动结果。

当前工作流的 Linux unit 使用 `-race`；integration 不带 race，覆盖普通测试和 integration 标签测试，不能称为纯集成测试的独立贡献。进程 E2E 不采集子进程覆盖率，也不运行 race。CI 服务版本为 PostgreSQL 18.6、Redis 7.4.11、etcd 3.5.21；实施时若工作流变化，应以对应提交为准。

分片负载沿用已定基准规模：单轮 500 个分片，以及 100 个 Instance 各 10 个分片。还应覆盖接近报文限制的载荷、普通与分片检查混合、停止与持续结果保存的竞争。机器资源、数据体积、持续时间及性能通过门槛在正式压测前明确；不套用普通任务历史 P95／P99，也不测试后按成绩倒定门槛。历史清理尚未实现，不宣称已经完成叠加清理负载验收。

**可提交与验收完成分别判断：** 受影响测试、相关真实数据库／进程场景、普通回归、生成一致性和静态检查通过，文档及已知限制同步后，可以形成阶段可提交节点；整体交付还需完整链路与故障验证。实际提交／推送须有用户授权，并按 [AGENTS.md](../../../AGENTS.md) 分别审查暂存区、待推送全部提交及提交信息中的敏感内容。首次提交前不要求取得尚未发生的新 CI，推送成功也不等于远端验收通过。

已批准的 [go-zero 停机测试 info 日志临时覆盖限制](ordinary-readiness-review.md#ordinary-shutdown-race-workaround)继续保留，不 fork、不加补丁、不修改生产日志级别。普通提交的历史 CI 只作为既有基线，不作为新分片结果。

## 开工检查结果（2026-11-09） {#sharded-readiness-result}

- 已完成：按现有源码核对输入、协议、Model、迁移、仓储、派发、结果、检查、恢复及生产装配；核对测试组织、生成命令和 CI 入口，形成上述安排。
- 尚未开始：分片运行代码、迁移、生成物和测试实现；实际查询计划、事务预算、完整进程及负载验证。
- 下一步：先实施阶段 1 的输入协议与数据基础，随后按依赖推进完整阶段链路。正式负载环境与阈值后续在压测前确认，不阻塞前段开发。

## 阶段 1：输入协议与数据基础（2026-11-11） {#sharded-stage1-progress}

实现范围：

- `ShardInput.Item`、`Invocation.Item` 和 Proto `bytes item` 统一为单份 JSON，RPC 校验、字节所有权及 Runtime 调用同步；按 README 固定命令重新生成绑定，无旧数组包装或兼容别名。
- 公共 `MaxPreparedItems = 500` 用于执行器成功准备输出和调度端接收校验；接收前拒绝超限，保留明确空清单及失败忽略输出的规则，不伪造业务 FAILED。
- `TaskResultData` 改为输入输出共用的 `TaskData`，继续保留原始数值和 SQL NULL／JSON null 区别；`JobTask.InputData` 与 `ResultData` 分离。
- 新增迁移 `20261111120000_add_task_input_and_dispatch_index.sql`，添加可空无默认输入列，以 `idx_job_task_dispatch_due` 替换旧 retry 索引；Down 恢复旧结构，未改既有迁移。

| 验证项 | 当前结果 |
| --- | --- |
| 公共输入、协议、接收端、Model、结果与仓储普通测试 | 6 个包通过，命令退出码 0；使用 Windows 固定测试包装器 |
| PostgreSQL 18.6 隔离库迁移和输入读写 | migration、repository、taskresult 三包带 integration 标签测试全部通过；包含原普通测试和集成测试。迁移回滚／再次 Up、任意 JSON 读写、固定输入保护及原仓储并发回归通过 |
| 全仓正式源码普通回归 | Windows 固定包装器下 `go test -p 2 -exec $testRunner -shuffle=on -count=1 ./apps/... ./pkg/...` 通过，退出码 0；75 个有测试的包通过，另外 14 个包无测试文件 |
| 带标签静态检查及生成一致性 | `golangci-lint run --build-tags integration,e2e ./pkg/jobexecutor/... ./apps/job/...` 通过，0 issues；包含 govet。固定 protoc 命令再次生成的两份绑定哈希不变，`git diff --check` 通过 |
| Linux／race／本次 CI 覆盖率 | 未提交或推送，尚无本次 CI |

本轮使用 Go 1.27.0、protoc 35.1 和 PostgreSQL 18.6；数据库使用新的本机隔离数据目录和 `_test` 数据库，按测试再隔离 Schema。验证后确认隔离 Schema 已清理并关闭本轮数据库，没有对开发业务库执行迁移。输入／输出独立读写、JSONB 限制、结果更新保留固定输入及索引结构／回滚均已加入测试，实际结论以上表为准。

首次全仓 `go test ./...` 的 25 个失败包均位于被 Git 忽略的 `bin/test-tools/ordinary-submit-*/index` 旧审计源码快照；该目录被 Go 当成普通包，产生 `internal package` 导入限制等编译错误，正式源码包没有失败。已核对受版本控制的 Go 文件全部位于 `apps` 和 `pkg`，因此用这两个根目录完整重跑；保留审计资料，不为本机快照修改业务代码或测试断言。本轮没有新增完整分片 E2E，现有 CI 入口也未改动。

阶段 1 结束时，Saver 仍拒绝非普通 Task，Pre 成功与完整清单原子保存、后台校验及清单 JSONB 异常诊断尚未实现；后续进展见阶段 2。Post、完整阶段检查、故障接续及统一查询性能仍按后续阶段推进。

## 阶段 2：Pre 与分片执行组件（2026-11-11） {#sharded-stage2-progress}

本轮扩展既有组件，没有增加分片专用队列或大 Service：

- 首次启动按冻结模式原子创建普通 Task 或 Pre，父开始时间、原截止及第一次尝试同事务提交；统一候选保留状态、类型和原尝试，分别登记首次 Shard／Post 或各阶段重试。
- Pre 成功状态与完整清单一起保存；后台仍校验明确清单、500 上限和 JSON，数据库异常诊断检查实际完整数组。失败不保存部分清单，旧结果仍先淘汰，配置错误和技术失败保留原处理规则。
- `instancecheck.Generator` 接收仓储读取的成功 Pre 结果，事务外解析；在父 NOWAIT 下复核状态、冻结配置、成功 Pre、原截止及已有分片，一次插入整组。只有确认提交才提示派发，提交不明不发送、不立即自我重排；明确空数组不进入生成事务。
- 原派发 Runtime 新增 `HintTasks`，与待开始提示和两个持久化供给来源共用等待容量及串行扫描，有限补充该 Instance 的到期 Task；不是新的扫描线程。Sender 按四类 Task 组装协议，分片携带固定单份 JSON，Post 读取固定统计；不会发送时再逐 Task 查输入。
- BUSY、拒绝、结果保存及未知结算扩展到四阶段，保留旧尝试、绑定、预算及输入不变的条件；方法按实际职责更名，不保留普通专用兼容别名。

**阶段 2 结束时的生产接线边界：** 本阶段实现和验证生成操作及派发提示接口，当时生产 Checker 仍跳过分片模式，测试显式调用生成操作，不将该组件联测称为完整宿主闭环。完整 `Checker` 阶段选择和 `svc` 双向提示装配在[阶段 3](#sharded-stage3-progress)接通：消费启动前装配好依赖，并保留检查处理中新增提示；生成成功不是父终态。

交叉审查发现并修正了两处提示供给边界：提示页读取失败后的退避保存在共享扫描器中，数据库时间提前读取失败也不能被新提示绕过；提示占满等待容量时，可释放一个提示位置供已到期的持久化来源轮转，避免 WAITING／全局 Task 补查饥饿。新增虚拟时钟测试覆盖持续提示、查询失败、满容量轮转和四页预算。

| 验证项 | 本轮实际结果 |
| --- | --- |
| 受影响普通测试 | instancecheck 及 dispatch／repository／taskresult／taskrecovery／execution 均通过；使用 Windows 固定测试包装器 |
| PostgreSQL 18.6 | repository、dispatch、taskresult、taskrecovery、instancestop、instancecheck 六包带 integration 标签测试通过（包含普通测试）；收尾新增用例和最后的派发修复已分别复测 |
| 整组事务及错误保护 | 500 项一次插入、外部连接提交前不可见、整组回滚、JSONB 拒绝、父 NOWAIT、防重复、停止与原截止、模拟提交确认丢失后核实、不补发旧尝试均通过；确认丢失是事务边界注入，不称为真实断网 |
| 真实 RPC 与持久化联测 | 新增 `TestShardedRuntimeGeneratesAndDispatchesSavedItems` 通过：执行器 Pre → RPC 回报／真实保存 → 显式调用生成操作 → 到期候选登记／RPC 派发 → 分片回报保存；单份 JSON null、大整数、数组、固定输入及重复保护通过。原普通 Runtime 回报、结果先于 ACK 两项也通过 |
| 全仓正式源码普通回归 | `go test -p 2 -exec $testRunner -shuffle=on -count=1 ./apps/... ./pkg/...` 通过，退出码 0；75 个有测试包、14 个无测试文件包、0 失败。沿用阶段 1 的正式源码范围，排除忽略目录中的旧审计快照 |
| 静态检查 | `golangci-lint run --build-tags integration,e2e ./apps/job/... ./pkg/jobexecutor/...` 通过，0 issues，包含 govet；gofmt 和 `git diff --check` 通过。本阶段没有修改 Proto，阶段 1 的生成物保持不变 |
| CI、覆盖率及性能 | 尚未提交或推送，没有本次 Linux／race／CI 覆盖率。新增 integration 用例已在现有 CI 包选择范围内，不是完整进程 E2E；没有用功能测试耗时充当查询计划或正式负载验收 |

本轮复用已关闭的阶段 1 隔离 PostgreSQL 目录，验证后确认测试 Schema 数量为 0 并再次关闭数据库，没有操作开发业务库。前端、CI 工作流和生产配置未改。阶段 2 结束时尚不是完整分片可交付节点；后续 Post、零分片和父最终判定、生产提示装配见阶段 3，系统性故障、查询和性能验收仍按计划推进。

## 阶段 3：Post 与整轮完成（2026-11-11） {#sharded-stage3-progress}

本轮接通生产检查流程，没有新增阶段队列、扫描器或配置开关：

- `Checker` 按冻结模式选择普通或分片流程；Pre 最终失败直接复制摘要结束父，成功后定向判断分片，显式空清单才代表零分片。分片部分或全部失败仍执行已配置的 Post，Post 成功不能抹除分片失败。
- `PostCreator` 在锁外接收已稳定的分片统计，父主键 NOWAIT 短事务内复核父状态、冻结配置及原截止，只创建唯一 Post；固定输入只有 `total/succeeded/failed`，首次派发和重试复用同一 Task。确认提交才提示派发，事务内无 RPC、不重复聚合。
- 查询先读阶段最小事实；待完成 Post 不再读取分片或统计，未结束分片不加载业务 JSON。只有缺少分片时读取成功 Pre 的完整清单，统计取消数用于诊断，未知状态差额返回错误，不冒充正常完成。
- 完整结果使用原父条件 UPDATE，即使检查晚于截止也允许正常结束；生成或新建 Post 仍必须在原截止之前。阶段操作返回无变化或错误不会继续转超时，一次检查共用原 Context 且最多进行一个阶段写动作。
- `OutcomeAdvanced` 明确阶段创建成功仍非父终态，保留处理中收到的新提示。`svc` 先完成检查与派发的双向装配再启动消费，原普通回归夹具中的“跳过分片”改为健康运行中的 Pre。

| 验证项 | 本轮实际结果 |
| --- | --- |
| 阶段业务及事务边界 | instancecheck 和 svc 带 integration 标签测试通过（包含普通测试）；覆盖零分片、部分／全部失败、Pre 失败、Post 成功／失败及独立重试、截止、取消与未知状态、NOWAIT 竞争、唯一创建和提交确认不明。提交不明通过事务边界注入，不称为真实网络断开 |
| 仓储与阶段查询 | repository 带 integration 标签的功能测试通过；新增执行计划测试最初因 GORM `Scan` 绕过日志参数捕获器失败，修正测试捕获后专项复测通过，未为测试改变生产查询。固定 Post 输入、提交／回滚、唯一约束和结果保存不覆盖输入均通过 |
| 已有索引验证 | 约 9 万条历史 Task、目标轮 500 个分片下，实际 GORM 阶段查询经 `EXPLAIN (ANALYZE, BUFFERS)` 验证：Pre／Post 使用单例索引，未结束存在性使用部分索引，统计使用 Instance 索引，均无顺序全表扫描。此结果不代替统一到期查询及正式负载验收 |
| 生产宿主联测 | execution 整包带 integration 标签测试通过。新增 `TestServiceContextCompletesShardedExecution` 使用真实 `svc`、etcd 发现、执行器与回报 RPC、PostgreSQL；8 个场景只经生产触发及提示推进，不直接调用 Generator／Checker。覆盖非空和零分片有／无 Post、Pre 失败、全分片失败仍执行 Post、Post 失败及同一 Post 重试；原普通 Runtime、停止和运行中恢复回归同时通过 |
| 全仓正式源码普通回归 | 75 个有测试包通过，14 个包无测试文件。全仓 `./apps/... ./pkg/...` 运行时发现 dispatch／taskresult 两个旧 Stub 缺少新增仓储方法，补齐后两包定向复测通过；其余 73 个包在该次全仓运行通过。使用 Windows 固定包装器、`-shuffle=on -count=1`，不把初次退出码 1 写成完整命令成功 |
| 静态检查 | `golangci-lint run --build-tags integration,e2e ./apps/job/... ./pkg/jobexecutor/...` 通过，0 issues，包含 govet；gofmt 和 `git diff --check` 通过。本轮新增核心源码／测试扫描未发现中文注释或日志；未修改 Proto |
| CI、覆盖率及整体交付 | 尚未提交或推送，无本次 Linux／race／CI 覆盖率。新增宿主用例属于已有 integration 包范围，在隔离子进程中装配真实组件；不是跨业务多进程故障 E2E，不作为阶段 4 或正式负载通过证据 |

本轮复用隔离 PostgreSQL 18.6 和测试自建 etcd 3.5.21，结束前确认库中仅余 `public` 非系统 Schema，并关闭本轮 PostgreSQL。未迁移或清理开发业务库。

阶段 3 结束时，下一步是故障与跨节点验收，实际结果见下方阶段 4；阶段 5 继续补齐统一候选查询计划、代表性载荷和交付检查。阶段 3 未改前端、生产配置与 CI 工作流，当时分片阶段 1～3 的代码未提交。

## 阶段 4：故障与跨节点验收（2026-11-11） {#sharded-stage4-progress}

本轮审计后补充测试、CI 选择器及验收记录，没有发现需要修改生产逻辑的缺陷。原阶段 1～3 变更全部保留，未提交或推送。

| 验收范围 | 实际覆盖与结果 |
| --- | --- |
| 丢失推进提示 | 真实 PostgreSQL 的原 `Recovery → Queue → Checker` 接续成功 Pre 尚未生成分片、分片最终完成但缺 Post、Post 已完成但父未结束三处缺口。固定一次数据库时刻，验证严格超过 4 分钟才纳入；边界与年轻父排除，到期不新建分片／Post。完整结果仍可晚于截止正常结束 |
| 阶段创建与停止 | 生成、Post 创建分别覆盖阶段事务先持锁和停止事务先持锁；使用 `pg_blocking_pids` 证明实际锁等待，验证 NOWAIT 竞争失败、提交后提示、已完成 Pre 保留、未执行子记录分批取消以及父最终收敛。另验证 Post 成功与未提交停止竞争时，正常完成条件 UPDATE 返回无变化，不能覆盖停止原因 |
| 迟到 ACK／结果及未知结算 | Pre、Shard、Post 结果先保存再返回 ACK；Saver 读取后出现成功 Pre、新分片尝试或 Post 取消；Post 结果 UPDATE 实际等待取消事务后重检条件；未知结算读到旧 Pre 后成功清单先提交。旧结果和 ACK 均不覆盖新事实，固定输入、成功 Pre 清单和原尝试保持 |
| STOPPING 下的当前结果 | 仍 RUNNING 的子 Task 可以保存有效结果，失败可能形成等待重试；即使该重试已到期，父 STOPPING 仍拒绝新授权。随后子取消及父终态收敛，迟到成功不能重开 Task 或改写人工停止原因 |
| 真实 System 分片进程 | `TestJobShardedExecutionEndToEnd` 的正常执行、单片重试、Post 重试三个场景通过；`TestJobShardedSchedulerCrashEndToEnd` 在 A 已派发并绑定全部分片、B 已就绪后 kill A，由存活 System 业务实际完成并通过 B 保存结果、创建 Post；`TestJobShardedManualStopEndToEnd` 验证人工停止父子终态、健康网络下三片感知取消且不创建 Post。业务 JSON 含大整数，固定输入、统计、调用次数和阶段先后均核实 |
| 保存后宕机的真实进程接续 | `TestJobShardedStageProcessRecoveryEndToEnd` 三个场景通过：A 的结果保存已确认提交，测试父锁阻挡后续阶段；kill A 并等待其连接消失后，B 当选并恢复分片生成、Post 创建或已过期但结果齐备的父正常完成。旧 Pre／Shard／Post 不重跑，原截止和输入保留 |
| 相关包回归 | instancecheck、dispatch、taskresult、taskrecovery、repository 五包带 integration 标签的完整测试通过，包含普通测试。使用 Windows 固定包装器、`-shuffle=on -count=1`；本轮未改生产 Go 代码，不把阶段 3 的全仓 75 包结果重新标为本轮完整命令成绩 |
| 普通进程与生命周期 | 原普通调度进程宕机恢复、Job／System 信号退出、执行器已接纳任务排空及停机预算耗尽四个 E2E 专项通过；`TestJobResultAcknowledgementProcessRecoveryEndToEnd` 的正常退出排空及 kill 丢失已 ACK 内存结果两个场景复测通过 |
| 静态检查及 CI 接入 | Job、System RPC／E2E 和公共执行器的 integration／e2e 标签 lint 通过，0 issues，包含 govet；修正新增测试遗漏的查询结果关闭错误检查。新进程测试已加入 CI 显式选择器并更新[测试命令](../../development/testing.md)，远端尚未运行 |

故障证据的边界：

- System 进程测试只操纵隔离 System 业务记录，通过公开管理 RPC 观察 Job，不伪造业务回报。取消 RPC 成功只在本次健康网络场景实测，数据库终态不保证任意故障下远端业务已经退出。
- Job 域保存后宕机测试确实强制结束生产进程，但使用预置旧 Instance 和协议结果，未执行实际业务或等待四分钟；精确年龄门槛另由真实数据库查询验证。跨 RPC 持父锁只用于故障注入，未加入生产事务。
- 阶段创建提交确认丢失继续由阶段 2／3 的真实事务加边界注入验证，本轮五包复测包含这些用例；不称为真实数据库断网，也不扩展为所有网络分区组合已验证。
- 本轮 E2E 不采集子进程覆盖率，无本次 Linux／race／CI 覆盖率。已批准的 go-zero info 日志临时覆盖限制继续有效。

本轮复用隔离 PostgreSQL 18.6，另起独立回环端口的 Redis 7.4.2；etcd 由各测试自行启动和清理。结束时确认 PostgreSQL 仅余 `public` 非系统 Schema，已关闭本轮 PostgreSQL 与 Redis，保留隔离数据目录及测试资料；未操作共享开发依赖。

阶段 4 结束时，剩余的统一查询、代表性负载与交付检查归第 5 阶段；后续结果见下节。

## 阶段 5：查询、负载与本机交付检查（2026-11-12） {#sharded-stage5-progress}

用户确认负载门槛后，将拓扑明确为两调度、两执行器；最终实测保持每进程默认并发、队列和原性能门槛。本轮新增查询及负载测试、复用进程夹具并同步记录，没有修改生产 Go 逻辑或配置。

| 验证范围 | 实际结果 |
| --- | --- |
| 统一到期查询 | 真实 GORM SQL／参数经 PostgreSQL EXPLAIN 验证，覆盖 9 万历史 Task、混合阶段首次／重试、首屏／续页及大量不合格父；未强制规划器选索引。最重有结果页为返回 128 条扫描 4,034 条，热缓存执行约 3.521ms；具体路径与限制见[查询记录](sharded-load-validation.md) |
| 完整检查预算与混合队列 | 1 片及 500 片整组生成均 10 条 SQL；近 4 MiB 完整检查约 113～128ms。默认检查／结果队列混合工作逐 ID 核验完成，记录排队、SQL、事务和连接等待。保留一次准备夹具 Pre 保存触发 2 秒预算的失败及不改预算的诊断重跑，不能宣称从未发生保存超时 |
| 两调度／两执行器负载 | 三轮 500 片、三轮 100×10 片混 100 普通任务全部达到事前门槛，六轮共 603 个父、5,406 个 Task 和唯一业务调用成功；最慢分片开始 P99 8.120 秒，最慢波次 10.103 秒，普通 P99 最高 0.484 秒。近上限输出与停止竞争也通过，完整命令退出码 0，资源和非均匀派发数据见[负载记录](sharded-load-validation.md) |
| 测试审计修正 | 首次完整运行仅最后停止专项因测试业务终态审计的 Context 取消竞态失败；新增确定性回归验证后，统一使用独立有界审计收尾 Context，完整负载重新通过。生产取消协议及 100ms 业务等待不变 |
| 普通与进程回归 | 全仓正式源码普通测试 75 包通过，另 14 包无测试；恢复普通宿主后，普通 HTTP→RPC 成功／重试／停止、原分片三场景、执行器信号排空及停机预算专项均通过。普通 E2E 第一次因命令漏配 API 二进制路径未启动，补齐后独立命令退出码 0，不视为产品失败 |
| 静态检查与生成 | integration／e2e／load 标签 lint 为 0 issues，包含 govet；测试审计修正后受影响 System 包再次通过。README 固定 protoc 命令重新生成，两份绑定哈希不变；Go 格式、差异及文档链接检查通过 |
| 敏感信息与远端边界 | 工作区完整差异及新增候选预审未发现新增真实敏感内容，测试注释和日志保持英文；正式 commit／push 仍分别检查最终暂存区、提交信息及全部待推送提交。当前未提交、未推送，无本次 CI／Linux race／覆盖率结论 |

本轮独立 PostgreSQL 和 Redis 已关闭，停止前确认只余 `public` 非系统 Schema；测试自建 etcd 已由夹具清理，保留忽略目录中的证据与数据目录。未操作开发业务库、共享 Redis 或私密配置。

分片本机已达到可提交节点；获授权后完成正式提交及推送检查，再观察对应提交的 GitHub Actions。长期稳定性、极限吞吐、并行历史清理及管理页面等其他第一版发布范围仍按各自安排，不将本轮代表性验收扩大为整个产品已经发布。
