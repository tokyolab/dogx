# 6 数据与接口契约

本章把架构职责转为持久化和跨模块契约的边界。触发模块的表结构和管理契约已经实现；Task、完整执行器组接入及执行 RPC 仍需由状态机和故障路径共同细化。本章描述契约，实际迁移和 Proto 源文件保存在代码仓库中。

首批触发模块的 [SQL 对照](trigger-sql-review.md)、[HTTP／RPC 对照](trigger-interface-review.md)、接入细则和开工范围已随[总审](trigger-readiness-review.md)通过并落实为代码；契约生成、隔离环境迁移及最新 CI 已完成，见[交付记录](progress.md#trigger-ci-verification)。这不表示已迁移开发或生产业务库，也不包含后续 Task／完整组接入字段。

## 6.1 数据所有权

调度配置、触发进度和执行记录属于 `job` 业务域，由调度服务统一维护。业务执行器通过 RPC 接收任务和回报结果，不直接更新调度表，也不持有对调度表结构的编排逻辑。

业务处理器访问所属业务域的数据；跨业务域使用 RPC 或版本化契约，不直接访问其他域的 Model、Repository 或数据表。管理 API/BFF 不访问普通业务数据库。同一业务域的后端进程可以按仓库分层规则复用本域数据层。

## 6.2 必须持久化的事实

| 对象 | 当前确定需要表达的事实 | 落实情况与剩余工作 |
| --- | --- | --- |
| 执行器组 | 属于 `job` 域；第一版无组启停，只做逻辑删除并保护引用；本批正式表保存 ID、名称和管理时间。组名作为执行器接入身份，与手动填写的发现 Key 均创建后固定；Token 创建时由后端生成 32 字符无连字符 UUIDv4，明文保存，不修改或重置 | 完整接入字段与约束的 DDL、组客户端生命周期及管理接口实现；字段语义已定不表示现有最小表已经实现完整约束 |
| Job | 平台配置普通列，短字符串使用 VARCHAR(n)，业务参数去首尾空白后使用 TEXT；新建默认停用，首次启用前进度为空，停用保留进度；启用原子保存下一次时间；六字段 Cron，无独立时区；触发事务、五索引及单批恢复查询已定；逻辑删除保留历史；管理入口校验组引用，新建／编辑与删组共用组行锁；超时表单初值 900 秒；输入、轻量查询与错误契约已定 | 本批 API／Proto、管理事务和 Cron 计算已实现验证；管理页面及历史维护随后续模块交付 |
| Instance | 自动及手动身份、UUID 请求号、参数覆盖标记与比较规则、本轮 JSONB v1 配置、名称及操作者信息；待开始／运行中／成功／失败／取消及新增 STOPPING（6）状态与编码；组标识和运行时间独立列，不另存权威阶段；触发身份约束、冲突处理及本批索引已定 | 触发所需字段和迁移已实现；[普通首次启动 SQL](#ordinary-start-sql)已通过、未实现；[完成条件、父 UPDATE、摘要及超时衔接](#instance-completion-contract)已通过，[STOPPING、子取消及最后父终态协议](#instance-termination-sql)已通过、未实现；[普通完成读取](#ordinary-completion-query)及[稳定结束原因](#execution-end-reasons)已通过；分片生成封口及完整分片查询 SQL／索引、分片字段及迁移与验证仍须补齐 |
| Task | 统一 `job_task` 表；普通／准备／分片／收尾类型及六态编码，执行编号 `attempt_no` 与已用次数 `used_attempts` 分开，保存登记时间、最早派发时间、执行器绑定和结果；普通、准备、收尾每轮各至多一条 | [公共字段及单例唯一索引](#task-field-contract)、[普通首次创建与重复推进 SQL](#ordinary-start-sql)、[接单 ACK 绑定 SQL](#executor-binding-sql)、[普通 Task 结果保存与状态转换](#task-result-persistence)、[普通 Task 重试登记 SQL](#ordinary-retry-registration)、[普通未知结算 SQL](#ordinary-unknown-settlement)、[普通 BUSY 返还 SQL](#ordinary-busy-settlement)及[统一子 Task 取消 SQL](#instance-termination-sql)已通过；[派发时间预算](execution.md#dispatch-time-budgets)已确定 BUSY 延期与无绑定观察初值。[普通完成读取](#ordinary-completion-query)及[稳定结束原因](#execution-end-reasons)已通过；其他类型正常推进的状态写入 SQL、其余恢复查询索引及分片工作项／编号／生成字段仍待细化，尚未创建实际表 |
| 本地临时状态 | 派发／回报队列、待检查提示、在途调用和连接缓存，不直接决定持久化状态；执行器回报组件有界保留待首次发送、延迟重试和在途结果；调度端逐条独立保存结果，默认保留 512 条（含首次待处理、退避和在途）、保存并发 8，满时拒收 | 分别说明局部保留范围、队列长度、处理并发、适用的读取批量、RPC 报文上限及合并标记，不建跨业务阶段统一计量或队列总字节配额；保存结果的退避不占处理并发，从接纳到最终释放始终保留原位置。未落库结果可能只有内存副本，已绑定结果等待不新增内存等待对象，不承诺恢复原结果 |

首次启用的提交必须同时形成启用状态与有效首轮计划，提交后再通知内存规则注册；通知丢失或注册前宕机不丢失该持久化依据，见[首轮计划初始化](behavior.md#first-trigger-initialization)。这不表示管理配置提交后所有调度节点已经应用，也不表示恢复时必须补执行；错失策略仍然适用。

已有 Job 的编辑只在停用时校验并保存配置，试算不重设持久化触发进度；重新启用按当前规则重算下一次，不补旧轮次。重复启用、重启、切主或配置重载不重置进度，详见[编辑与重新启用](behavior.md#job-edit-enable)。Instance 创建时保存本轮必要执行配置，之后编辑 Job 只影响新建轮次，范围见[本轮执行配置](behavior.md#instance-execution-config)。

共享配置在 Instance 级保存一份，不另建配置版本历史表，不在每个 Task 复制完整 Job；Task 保留工作项、状态及尝试计数等自身事实。创建时复用同 Job 短事务已经读取的配置，随 Instance 一起保存，不要求新增一次 Job 查询；有存储和序列化成本。手动指定当次业务参数时，固定覆盖后的实际参数，不修改 Job。后续读取按使用场景复用、批量加载或 JOIN 取得本轮配置，避免读完 Task 后再逐条补查；[结果保存](#task-result-persistence)已确认单条 JOIN，其他查询按对应模块设计，存储分工见下节。

Job、Instance、Task 的 SQL 表名分别为 `job_definition`、`job_instance`、`job_task`；Task 公共字段与单例唯一索引见[已确认契约](#task-field-contract)，实际迁移仍待执行模块实现。触发恢复的持久化依据是 `Job.next_trigger_at`，不是额外的计划表或重试队列。仅 Leader 在接任时及每分钟按索引查找候选 Job，正常回调和恢复均按最新数据库事实进入触发记账；不创建本地 Job 退避补试系统。已创建 Instance／Task 的正常执行仍由对等节点推进；[RUNNING Instance 统一复核候选](#instance-recheck-query)仅由 Leader 发现并交本地检查组件，其余待处理与恢复职责不并入该扫描。

Cron 按[调度进程默认时区](behavior.md#cron-contract)解释，不新增 Job 的 `timezone` 字段或表达式内时区前缀。`next_trigger_at`、`scheduled_at` 使用 `TIMESTAMPTZ` 表达确定的时间点，保留数据库默认微秒精度，计划时间由 Cron 产生为整秒；接口格式在接口契约中统一。所有规则计算入口使用相同部署时区，不能由数据库返回值的显示时区决定 Cron 语义。

准备、业务分片和收尾共用 Task 表，不建立独立 Attempt 历史表。当前尝试身份使用 `(TaskID, attempt_no)` 校验；“不保存历史尝试表”不等于不区分尝试。`attempt_no` 与可返还的 `used_attempts` 分开，BUSY 只返还已用次数，后续新派发递增编号，不复用旧身份，以免迟到响应命中新尝试。Instance 不保存整轮固定负责人。

etcd 承担 Leader 选举、服务发现及 Job 配置变更通知，不取代 PostgreSQL 中的配置与执行事实。配置通知在管理事务提交后更新一个集群级持久标记，固定值为 `reload`，不存完整 Job 配置或逐 Job 监听；etcd revision 仅作监听游标，不是业务配置版本。不与 PostgreSQL 构成原子提交，通知失败不主动改报已提交保存失败，仍由每分钟核对兜底；键空间、权限及预算见[etcd 接入约定](execution.md#etcd-integration)，加载顺序见[配置重载](execution.md#job-config-reload)。Task 派发已确定采用持久化条件登记，不增加每 Task 的 Redis 或 etcd 锁；生成及其他推进动作的执行权和恢复方式仍待设计，不能把选主协议直接当作全部 Task 的执行权协议。

组元数据持久化与实例地址发现是不同职责：前者由调度服务维护，后者由执行器注册及 etcd 发现维护。派发复用组配置缓存和 RPC 客户端，命中时不查组表；[组缓存](operations.md#group-cache)采用各节点启动加载、每 60 秒完整快照替换及未命中时合并同组回源。组名和 Key 创建后固定，不设计改名或发现 Key 热切换；同一 Key 下地址仍可变化。[已删组客户端](operations.md#group-client-cleanup)随成功完整刷新移出缓存并关闭，不等待在途调用排空，受影响调用按通信结果不明处理；[启动与停机规则](operations.md#startup-readiness)已确认，具体装配仍须实现验证。

组 Token 用于内部调用，不是 etcd 注册凭据或管理员登录 JWT。格式为后端安全随机生成的 UUIDv4 去掉连字符后的 32 个小写十六进制字符，组表明文保存原值，不增加应用层加密、哈希替代或部署加密密钥；取舍见[组凭据管理](operations.md#group-credentials)。第一版仅在创建组时生成，不提供修改、重置或轮换；组无编辑入口，后端不得更新已有组 Token。普通组列表不得返回凭据，组详情接口返回完整 Token，沿用 API／RBAC 的组详情权限，不另建查看密钥 API；页面默认遮挡并支持显示、复制，不采用仅创建时展示一次。请求、响应及错误诊断日志不得泄露 Token，具体控制实现须验收。[组缓存](operations.md#group-cache)命中不查库，未命中查询单组；完整刷新失败沿用旧快照并接受组删除滞后，不承诺 60 秒强制失效。

### Job 与 Instance 的存储分工 {#job-storage}

**Job 的平台配置使用普通列，业务参数使用独立载荷列；Instance 保留必要独立列及 `execution_config` JSONB。** 两者组织不同，是因为 Job 维护当前规则，Instance 保存某一轮不可变的执行配置及持续变化的运行事实。

| Job 内容 | 存储方向 |
| --- | --- |
| 标识、名称、说明 | 普通列 |
| 执行器组、处理器、普通／分片模式、Post 选项 | 普通列 |
| Cron、启停、`next_trigger_at`、错失策略 | 普通列 |
| 阻塞策略、重试次数、重试间隔、整轮超时 | 普通列 |
| 创建和修改时间等管理信息 | 普通列 |
| 商户范围、报表日期等业务参数 | 独立参数载荷列，不为各业务扩展平台表字段 |

触发事务用已经读取的 Job 构造本轮配置，与 Instance 同时保存，不额外查询一次 Job。后续执行读取该轮配置，不再用当前 Job 覆盖；组身份和运行数据仍保留独立列，具体分工见下节。

组织方式及下方字段契约已确认。普通列不等于逐列建索引；索引随实际查询设计。数据库约束负责数据完整性，不保存拍定的重试、超时等业务参数范围。表和字段均须提供注释，说明单位、枚举及空值含义。

### Instance 执行配置存储 {#instance-config-storage}

**Instance 采用必要独立列加一个 `execution_config` JSONB 字段。** 共享执行配置每轮保存一份；不在每个 Task 复制完整配置，也不新增配置历史表。

| 内容 | 存储位置 | 用途 |
| --- | --- | --- |
| 稳定的执行器组标识 | Instance 独立列 | 关联、组归属校验与查询；属于创建时固定的本轮执行目标 |
| 处理器标识、普通／分片模式、Post 选项、业务参数、重试次数与间隔、整轮超时时长 | Instance 的 `execution_config`，PostgreSQL `JSONB` | 主要供执行时整体读取，后续生成、重试与恢复沿用 |
| Instance 状态、开始时间、截止时间等运行数据 | Instance 独立列 | 支撑条件更新、恢复扫描及索引 |
| 工作项、Task 状态、当前尝试与重试计数 | Task 自身字段 | 表达各执行单元的输入与运行事实，不混入共享配置 |

Go 侧以明确的结构体表达平台执行配置，由程序负责校验、序列化与兼容，不以任意 Map 替代配置契约。业务参数的具体内容仍由处理器解释；已确认的 JSON v1 成员、单位及显式保存规则见[执行配置与手动参数比较](#execution-config-field-draft)。未来结构变更另行定义旧快照兼容行为。

独立列与 JSONB 分工是为了组织清楚、方便扩展，不承诺 JSONB 比普通列更快。JSONB 仍有存储及序列化成本，增加配置成员也仍需处理旧实例兼容；不能把采用 JSONB 等同于以后无须数据演进设计。恢复查询需要的状态和截止时间保留普通列，批量派发复用或批量加载本轮配置，避免每个 Task 单独查询 Instance。

超时时长在配置中固定，首次有效开始与截止时间是独立运行字段，仍按[整轮超时规则](behavior.md#instance-timeout)从首次有效开始计时。Cron、启停、错失和阻塞策略、动态节点地址、组凭据及队列容量不装入本轮执行配置快照；配置固定范围见[行为规则](behavior.md#instance-execution-config)。Job 的存储组织方式和本批字段已确认，触发约束与五个必要索引也已通过；完整迁移、后续执行字段及 RPC 表达仍须补齐。

### Job 与 Instance 字段契约 {#trigger-field-draft}

触发闭环所需字段、空值、编码及表内注明的初值已通过；短字符串类型按[字段类型规范](../../development/database-migrations.md#column-types)修订为明确长度的 VARCHAR，替代先前统一 TEXT 的设计。Job 逻辑删除决定另补 `deleted_at`，其过滤规则见[删除协议](#job-deletion-proposal)。整轮超时的新增表单初值已补定为 900 秒，API 仍必填，不设 SQL 默认。Task、分片生成与汇总字段在对应模块补齐，这不是整个调度系统的最终 DDL；历史清理及下列明确保留的事项仍须闭合。

表名为 `job_definition` 和 `job_instance`，实体仍称 Job、Instance。主键沿用 `BIGINT GENERATED BY DEFAULT AS IDENTITY`；状态／策略使用 `SMALLINT`，跨 API／RPC 按仓库契约使用整数枚举。时间点统一为 `TIMESTAMPTZ`，保留 PostgreSQL 默认微秒精度；Cron 产生的计划时间为整秒，不把其他运行时间强制截为秒。显示时区不改变时间点，也不决定 Cron 的解释时区。

下表“初值”由创建逻辑明确赋值，不全部设为数据库默认值；必填配置遗漏应报错，不能依靠数据库静默补成另一种任务。`created_at` 可使用数据库 `CURRENT_TIMESTAMP`，`updated_at` 的后续变更必须由应用显式维护。

#### Job 字段 {#job-field-draft}

| 字段 | PostgreSQL 类型 | 空值及创建初值 | 含义 |
| --- | --- | --- | --- |
| `id` | `BIGINT` | 非空，Identity 主键 | Job 身份 |
| `name` | `VARCHAR(128)` | 非空，必填 | 展示名称，不增加名称唯一要求 |
| `remark` | `VARCHAR(500)` | 非空，空串 | 说明 |
| `executor_group_id` | `BIGINT` | 非空，必填 | 执行器组身份，由应用验证引用 |
| `processor_name` | `VARCHAR(128)` | 非空，必填 | 组内具名处理器，不是地址或 RPC 路径 |
| `execution_mode` | `SMALLINT` | 非空，默认 1 | 1 普通，2 分片 |
| `post_enabled` | `BOOLEAN` | 非空，默认 false | 分片模式是否调用收尾；普通模式为 false |
| `params` | `TEXT` | 非空，空串 | 业务参数去首尾空白后保存，允许 JSON 文本，不强制 JSON、不重新编码 |
| `cron_expr` | `VARCHAR(128)` | 非空，必填 | 六字段 Cron，无独立时区列；128 为项目容量，不是语法最大长度 |
| `status` | `SMALLINT` | 非空，默认 0 | 0 停用，1 启用；新建可显式启用 |
| `misfire_policy` | `SMALLINT` | 非空，默认 0 | 0 忽略，1 执行一次；容忍仍固定 5 秒 |
| `block_policy` | `SMALLINT` | 非空，默认 0 | 0 丢弃，1 并行 |
| `max_retries` | `INTEGER` | 非空，默认 3 | 额外执行重试次数，不是已重试次数 |
| `retry_interval_seconds` | `INTEGER` | 非空，默认 10 | 执行重试间隔，单位秒 |
| `timeout_seconds` | `INTEGER` | 非空，必填 | 有限正数；新增表单初值 900 秒，不设 SQL 默认 |
| `next_trigger_at` | `TIMESTAMPTZ` | 可空 | 下一次尚未完成触发记账的计划时间 |
| `created_at` | `TIMESTAMPTZ` | 非空，数据库初始化 | 创建时间 |
| `updated_at` | `TIMESTAMPTZ` | 非空，创建时初始化 | 记录最后更新时间，包含配置修改和触发进度推进，不作业务版本 |
| `deleted_at` | `TIMESTAMPTZ` | 可空，初始 NULL | 逻辑删除时间；NULL 表示未删除，非空表示已删除；不提供恢复入口 |

一个 `processor_name` 定位处理器的普通或分片执行能力，分片时按同一处理器调用准备、分片及可选收尾；不新增三套可任意组合的处理器寻址配置。具体 Go 接口在执行器接入模块设计。

名称、说明、处理器标识和 Cron 属于有明确边界的短字符串，使用 `VARCHAR(n)`，API／RPC 校验与数据库容量保持一致；业务参数仅去首尾空白后保存为 `TEXT`，内部内容不变。这种类型划分表达字段用途，不宣称 VARCHAR 比 TEXT 更快，也不将重试或超时等业务数值范围下沉为 CHECK；容量依据见[字符串类型与容量](#string-field-types)。

`next_trigger_at` 按以下方式表达，停用保留原进度：

| 场景 | 字段值 |
| --- | --- |
| 新建且从未启用 | NULL |
| 新建直接启用或首次启用 | 同一事务保存未来首轮时间，不提交启用且无有效进度的记录 |
| 停用及停用期间编辑 | 保留停用前的值，不将它作为自动触发许可，也不在页面显示为仍将执行的计划 |
| 从停用变为启用 | 按当前规则重算未来时间，与启用状态原子保存 |
| 重复启用、重启、切主或重载 | 不重置 |

#### Instance 字段 {#instance-field-draft}

| 字段 | PostgreSQL 类型 | 空值及创建初值 | 含义 |
| --- | --- | --- | --- |
| `id` | `BIGINT` | 非空，Identity 主键 | 一轮执行身份 |
| `job_id` | `BIGINT` | 非空 | 来源 Job ID |
| `job_name` | `VARCHAR(128)` | 非空 | 创建时名称快照，与 Job.name 容量相同，复用锁内 Job |
| `executor_group_id` | `BIGINT` | 非空 | 本轮固定的执行器组身份 |
| `trigger_type` | `SMALLINT` | 非空，显式赋值 | 1 正常定时，2 错失补执行，3 手动 |
| `scheduled_at` | `TIMESTAMPTZ` | 自动非空，手动 NULL | 锁内原 `Job.next_trigger_at`，不是创建时间 |
| `request_id` | `UUID` | 手动非空，自动 NULL | 一次手动操作的请求号，重发沿用 |
| `params_overridden` | `BOOLEAN` | 非空，默认 false | 是否明确覆盖当次参数；自动固定 false |
| `triggered_by` | `BIGINT` | 手动非空，自动 NULL | 登录操作者 ID，不为保存而查询用户名 |
| `execution_config` | `JSONB` | 非空，无空对象默认 | 创建后不可变的本轮执行配置 |
| `status` | `SMALLINT` | 非空，初始 1 | 1 待开始，2 运行中，3 成功，4 失败，5 取消，6 停止中（STOPPING）；6 为非终态 |
| `started_at` | `TIMESTAMPTZ` | 初始 NULL | 首次有效开始时间，后续不重设 |
| `deadline_at` | `TIMESTAMPTZ` | 初始 NULL | 与首次开始原子保存的整轮截止时间 |
| `finished_at` | `TIMESTAMPTZ` | 初始 NULL | 进入终态时保存；STOPPING 期间仍为 NULL，停止处理耗时包含在平台结束时间内，不表示业务退出时间 |
| `end_reason` | `VARCHAR(64)` | 初始 NULL | 正常结束时保存稳定原因；停止路径在进入 STOPPING 时即冻结人工停止或超时原因，最终结束沿用，不被重复请求或迟到结果改写；不是错误全文，取值见[稳定结束原因](#execution-end-reasons) |
| `result_summary` | `TEXT` | 初始 NULL | 自由文本形式的结果／诊断摘要，不是详细过程日志；正常完成按[已定摘要规则](#instance-completion-contract)，停止／超时保存对应平台说明 |
| `created_at` | `TIMESTAMPTZ` | 非空，数据库初始化 | 创建记录的时间，不启动执行倒计时 |
| `updated_at` | `TIMESTAMPTZ` | 非空，创建时初始化 | 记录最后更新时间，运行状态等数据更新时维护，不作执行期限 |

正常定时和错失补执行共用 `(job_id, scheduled_at)` 身份，不能把 `trigger_type` 加入唯一键或分建两套自动唯一索引；手动使用 `(job_id, request_id)`。`trigger_type` 记录本次判定，不记录 Cron／恢复入口：恢复也可能正常，Cron 回调也可能迟到。

开始、截止和结束时间初始都为空。后续模块按已定数据库时间及父行协议写入，不因表中已经有这些字段而在触发创建时提前启动超时。待开始的 Instance 可经 STOPPING 转取消，期间不设置开始及截止，因此结束时间非空时二者仍可能为空，不能增加“结束必须先开始”的约束。STOPPING 不新增停止请求时间、pending 标记或命令表；Task 的 `end_reason` 仍只在子终态时保存。`job_name` 为历史展示保存，不参与执行或请求内容比较。新增状态及字段语义属于已批准的执行模块设计，现有触发代码／迁移尚未因此修改。

#### 字符串类型与容量 {#string-field-types}

本次重设计沿用项目短字符串 `VARCHAR(n)`、自由长文本 `TEXT`、结构化配置 `JSONB` 的分类，不把业务字段统一成一种字符串类型。

- 名称及其历史快照均为 `VARCHAR(128)`，与现有字典名称容量一致；说明为 `VARCHAR(500)`，与现有用户、字典备注一致。来源与快照长度同步，避免创建 Instance 时才因名称超长失败。
- 处理器标识为 `VARCHAR(128)`，用于具名路由；不是 Go 源码、任意脚本或 RPC 地址，不使用长文本字段。
- Cron 为 `VARCHAR(128)`，校验同步为最多 128 字符，替代先前应用层 256 的草案。128 是本项目的输入容量，参考 XXL-JOB 页面同类字段，不是 Cron 标准或 robfig 的最大合法长度；超过容量的长枚举即使语法合法也会拒绝。长度校验不能替代六字段解析和 Next 校验。
- 结束原因使用 `VARCHAR(64)`，保存平台稳定机器码；详细诊断不塞入原因码。已定[原因字典](#execution-end-reasons)及实现写入路径需验证不超过容量，不截断原因码。
- `params` 继续使用 `TEXT`，仅去首尾空白，业务方解释剩余文本；`result_summary` 继续使用 `TEXT`，允许保存可变长度的自由结果／诊断摘要。不在本轮擅加业务结果字段上限或截断规则，也不因此存储详细过程日志或承诺无限报文。
- `execution_config` 保留明确结构的 `JSONB`，其 `processor_name` 成员遵守来源 128 字符的契约；字段类型变化不改变 v1 JSON 结构。ID、策略、计数、布尔、UUID 与时间点保留现有原生类型。

所有长度按 Unicode 字符数计量，API／RPC 先校验，Model 与 DDL 同步容量，数据库类型保证存储容量；不得为适配容量静默截断名称、表达式或参数。按输入契约去首尾空白不属于超长截断。数值范围不新增业务 CHECK。当前仍是设计稿，未创建调度表、修改已有业务表或执行迁移。

#### 执行配置与手动参数比较 {#execution-config-field-draft}

JSONB 使用下列明确成员，创建时全部显式写入，不用省略字段表达零重试、false 或空参数：

| JSON 成员 | 值及含义 |
| --- | --- |
| `schema_version` | 整数 1，标识快照结构，不是 Job 修改版本 |
| `processor_name` | 字符串，与 Job 处理器含义及 128 字符容量相同 |
| `execution_mode` | 整数 1／2，与 Job 模式编码一致 |
| `post_enabled` | 布尔值 |
| `params` | 字符串，保存已去首尾空白的本轮实际参数；即使内容为 JSON，也不转换为对象 |
| `max_retries` | 整数，本轮额外执行重试次数 |
| `retry_interval_seconds` | 整数，单位秒 |
| `timeout_seconds` | 整数，整轮有限超时时长，单位秒 |

组 ID 已在普通列，不重复放入 JSON；Cron、启停、错失、阻塞、组凭据、节点地址和调度并发不进入快照。第一版只实现结构 v1；未来演进需明确旧快照的解析及缺省语义，未知结构不能默默读取当前 Job 或套新默认值继续执行，不建设配置历史表或通用兼容引擎。

手动请求号使用 UUID；一次新操作生成新值，网络重发继续使用原值。幂等判断先查原 Instance，并比较“是否覆盖”及必要的覆盖内容：

| 原请求与重发请求 | 判断 |
| --- | --- |
| 都沿用 Job 参数 | 相同，不比较后来 Job 的参数 |
| 都明确覆盖 | 本次覆盖参数去首尾空白后，与原 `execution_config.params` 精确比较字符串，相同才返回原 ID |
| 一个沿用、一个覆盖，或覆盖内容不同 | 请求内容冲突 |

覆盖空串与沿用参数必须能够区分，不能用字符串为空推断。手动入口先按[输入契约](#job-input-query-proposal)校验覆盖标记及参数是否提供，再对合法覆盖字符串执行 `strings.TrimSpace`，用于创建快照和同号重发比较。全空白覆盖规范化为空串，仍然属于明确覆盖；仅首尾空白不同的覆盖文本视为相同。覆盖内容不做 JSON 语义归一化，内部空白和键顺序不同仍视为不同文本；不再保存第二份参数、内容哈希或独立请求表。沿用请求使用已保存参数，不比较后来 Job 的参数，也不重新改写旧快照。`triggered_by` 用于记录操作者，不改变已定请求身份；接口仍须执行管理权限校验。

#### 约束与后续补齐范围

主键、非空和两种触发身份的唯一性由数据库保障；自动／手动身份字段的适用非空与互斥属于结构完整性，可用 CHECK。重试、间隔、超时的可调整范围，以及模式与 Post、启停与进度的流程规则由业务层校验。不创建物理外键或级联删除，沿用仓库的数据所有权和引用维护规范。

本批唯一索引表达、冲突处理及五个必要索引见[已确认的触发事务](#trigger-transaction-proposal)；[到期 Job 恢复查询](#trigger-recovery-scan-proposal)采用单批限量、每次重新读取，不分页续扫。不为 JSONB 建通用 GIN 索引，也不机械索引每一列。表及所有字段的 COMMENT 随 DDL 交付。

本批字段不增加 `owner_id`、权威阶段、逐条回报更新的汇总计数或 Job 配置版本字段。Job 按已通过决定使用 `deleted_at`；本域 Model 沿用项目现有 `gorm.DeletedAt` 语义，不跨域复用业务 Model，也不能为复用基础 Model 给 Instance／Task 顺带加入软删除。分片生成／封口及最终汇总数据随执行模块设计；分片参数配置形式、摘要上限和历史清理仍须闭合。管理入口、事务边界、组引用校验及并发保护、超时初值，以及本批最终 [HTTP／RPC](trigger-interface-review.md) 和 [SQL 契约](trigger-sql-review.md)均已通过开工总审，待明确开始实现。

### Task 公共字段与单例唯一约束 {#task-field-contract}

**统一 `job_task` 表的普通任务必需公共字段及以下单例唯一索引已通过。** 本节是设计契约，尚未生成或执行迁移；分片工作项、分片编号和生成字段在分片生成设计中补齐，不另拆 Task 表。

Task 只保存自身执行事实。Job 来源、执行器组、处理器、公共参数、重试配置与整轮截止继续从所属 Instance 及本轮快照读取，不在每条 Task 复制。关联读取使用有界批量查询或 JOIN，不在读取 Task 后再逐条补查 Instance，也不读取当前 Job 改写历史执行配置。[结果保存](#task-result-persistence)已明确每条用一次 JOIN 取得必要事实，不要求批量预读。

| 字段 | PostgreSQL 类型 | 已确认含义 |
| --- | --- | --- |
| `id` | `BIGINT` | Identity 主键，Task 唯一 ID |
| `instance_id` | `BIGINT` | 所属 Instance，非空 |
| `task_type` | `SMALLINT` | 非空；1 普通、2 准备、3 分片、4 收尾 |
| `status` | `SMALLINT` | 非空；1 待执行、2 执行中、3 等待重试、4 成功、5 失败、6 取消 |
| `attempt_no` | `BIGINT` | 当前执行编号；普通首次派发为 1，后续新派发递增，不随 BUSY 返还而回退 |
| `used_attempts` | `INTEGER` | 已占用的执行次数，包含首次；普通首次为 1，明确 BUSY 可按既定条件返还一次 |
| `attempt_registered_at` | `TIMESTAMPTZ` | 当前执行编号在数据库登记的时间，用于绑定缺失观察；不是执行器实际开始时间 |
| `next_dispatch_at` | `TIMESTAMPTZ` | 待执行／等待重试时的最早可派发时间；执行中和终态为空 |
| `executor_instance_id` | `UUID` | 当前尝试的执行器进程身份，尚未取得时为空；每个执行器进程启动时生成一次 UUIDv4 |
| `executor_address` | `VARCHAR(255)` | 已确认的执行器直连 `host:port` 地址，支持 IPv6；未知时为空 |
| `result_data` | `JSONB` | 可选业务 Data；允许对象、数组或其他满足 JSONB 存储要求的合法 JSON，不限定为对象；不可保存内容按[结果错误契约](#result-error-classification)核实 |
| `result_summary` | `TEXT` | 最近保存的结果说明或失败诊断；重试登记时保留，不保证属于当前执行编号，不作为当前尝试已有结果的依据，不保存完整过程日志 |
| `end_reason` | `VARCHAR(64)` | Task 最终结束原因码；尚未结束时为空，不保存错误全文。取值见[稳定结束原因](#execution-end-reasons)，等待重试仍为 NULL |
| `started_at` | `TIMESTAMPTZ` | Task 首次登记执行的时间，重试不重置；不是执行器实际开始跑业务的时间 |
| `finished_at` | `TIMESTAMPTZ` | Task 进入成功、失败或取消的时间；尚未结束时为空 |
| `created_at` | `TIMESTAMPTZ` | 记录创建时间，仅用于审计 |
| `updated_at` | `TIMESTAMPTZ` | 记录最后更新时间，仅用于审计，不承担执行期限、重试或观察语义 |

普通首次启动沿用[合并事务](execution.md#ordinary-flow)：创建时直接保存 `task_type=1`、`status=2`、`attempt_no=1`、`used_attempts=1`，并保存首次及当前登记时间，不先提交待执行 Task 再另开首次登记事务。`started_at` 保留第一次时间，`attempt_registered_at` 随后续新执行编号更新，绑定缺失观察不得改用 `started_at`、`created_at` 或 `updated_at`。

Task 无软删除语义，与现有 Instance 一样独立声明适用字段，不嵌入带 `deleted_at` 的公共 Base。不建立物理外键，不增加独立 Attempt 历史表、负责人、执行租约或独立的“尝试已关闭”字段。表及字段注释、非空等结构约束随最终迁移核对，不把本节批准扩大为分片完整 DDL 或恢复索引已完成。

#### 执行编号与已用次数

`attempt_no` 识别本次派发，`used_attempts` 判断是否还有执行预算。两者都不是业务代码实际运行次数，也不能代替对旧状态的校验。默认额外重试 3 次，正常首次执行加重试共允许占用 4 次；明确 BUSY 的条件返还不复用旧编号。

| 事件 | `attempt_no` | `used_attempts` |
| --- | --- | --- |
| 首次准备派发 | 1 | 1 |
| 执行器明确 BUSY，条件返还本次机会 | 1 | 0 |
| 到期后登记下一次派发 | 2 | 1 |

结果更新继续校验 Task ID、`attempt_no`、允许更新的旧状态及已有执行器身份。即使尚未生成下一个编号，已经进入等待重试的旧尝试也不再接受迟到结果；不因没有独立关闭字段而省掉旧状态条件。重复或迟到 BUSY 不得多次返还，通信结果不明不得按 BUSY 免费返还。

`executor_instance_id` 是进程启动身份，不是组 Token。结果可能早于 ACK 绑定落库，不能增加“成功 Task 必须已有执行器地址”的约束；已存在的冲突身份不能被静默覆盖。[接单 ACK 绑定 SQL](#executor-binding-sql)、[普通结果保存规则](#task-result-persistence)及[普通重试登记](#ordinary-retry-registration)已定；结果不补写绑定字段，重试登记清空旧绑定。[普通未知结算](#ordinary-unknown-settlement)也已按原尝试、执行中状态、双绑定为空及观察期条件落实。

#### 每轮单例 Task 防重

```sql
CREATE UNIQUE INDEX uq_job_task_singleton
ON job_task (instance_id, task_type)
WHERE task_type IN (1, 2, 4);
```

该索引保证同一 Instance 的普通、准备、收尾 Task 各至多一条，覆盖全部状态而非仅未结束记录。重试更新原 Task，不创建第二条；原 Task 结束后也不能在同一 Instance 内重新创建同类型单例，手动重新执行另建 Instance。

分片类型 3 不进入该索引，允许同一轮有多个分片；其稳定编号与防重约束随分片生成设计确定，不把整个 `(instance_id, task_type)` 建成无条件唯一索引。Task 类型是否符合 Instance 模式由父行事务逻辑校验，单例索引不能替代该检查或首次启动的并发协议。

本节只确定主键和单例唯一索引。待处理、绑定缺失、取消及历史查询等普通索引，继续结合实际 SQL 和执行计划确定，不机械索引每个字段。普通结果覆盖摘要、重试登记保留最近说明、未知结算及明确 BUSY 更新平台诊断的规则已定；[停止超时与子取消的摘要处理](#instance-termination-sql)也已定；[正常完成条件写入与摘要](#instance-completion-contract)已通过；[稳定结束原因](#execution-end-reasons)及[普通完成读取](#ordinary-completion-query)已通过；分片输入及生成字段、完整分片查询 SQL／索引及实现验证仍须落实，已批准的 RPC 协议编码尚未实施。

## 6.3 并发与事务约束

数据库继续采用 PostgreSQL 默认 `READ COMMITTED`。需要原子的多条写操作在短事务内完成，不包含 RPC、Redis、MQ 等外部 I/O。

定时、恢复、注册后到期检查和手动创建 Instance 共用 Job 主键等值的单表单行锁。自动触发锁内重读配置与进度，用取得锁后读取一次的调度进程时间判断到期、错失及计算下一次，用 `EXISTS` 做必要的阻塞检查；创建 Instance 与推进 `Job.next_trigger_at` 同事务提交，忽略或丢弃也按策略推进。手动触发不推进 Cron 进度，Instance 结束不要求取得 Job 锁。接受每个实际处理 Job 的短事务成本，批次和并发有界，不按每个 Task 查 Job；见[触发记账](execution.md#trigger-accounting)。

触发提交确认不明时，后续按最新进度及触发身份重入，不盲目重放旧创建操作；已创建的 Instance 由原恢复路径接续。管理编辑、启停和删除与触发在同一 Job 行上协调，后取得行锁的触发读取最新状态，不以旧回调快照授权。编辑的停用状态校验与配置保存必须和启用协调；启用状态与重新计算的下一次时间原子提交。普通管理更新不因此统一增加显式锁。

Job 行锁保护记账顺序，不证明提交者仍是 Leader。第一版采用每任期独立本地上下文，接受切主后旧在途事务按最新 Job 合法提交，不增加数据库全局任期锁；详见[切主与配置变更边界](execution.md#leader-config-boundary)。自动触发身份、向未来推进、Cron 输入／时区规则、时间存储及[内存配置重载流程](execution.md#job-config-reload)已确认，本批约束与索引见下方协议；完整迁移、统一时间计算和并发验证仍待实现。

自动创建的 Instance 使用 `(job_id, scheduled_at)` 唯一约束，`scheduled_at` 取创建事务锁内读取的旧 `Job.next_trigger_at`，不是恢复发现时间或创建时间。正常／错失不进入唯一键维度；手动请求身份与自动触发分开。该约束防止重复创建执行记录，不代替业务幂等，也不要求为忽略／丢弃的每个时间点创建 Instance；跳过原因仅写[服务日志](#trigger-skip-record-proposal)。

手动创建使用 `(job_id, request_id)` 唯一约束，UUID 身份随 Instance 保存。相同身份、相同请求内容返回原 Instance ID，原实例终态不重新创建；相同身份改参数必须报冲突。按 `params_overridden` 与必要的冻结参数比较，规则见[已确认契约](#execution-config-field-draft)，不能用后来变化的 Job 反推原请求。自动与手动部分唯一索引及冲突处理 SQL 已定，接口表达与清理衔接仍须闭合，不增加独立请求记录表。

手动入口在确认 Instance 与本轮配置提交成功后才返回创建成功；取得处理额度后，重发先识别原记录，再做新建所需的阻塞判断。手动与自动入口共用本进程创建并发额度，无空位时返回繁忙，不另建手动等待队列；容量拒绝的具体错误码及接口表达在实现契约中确定，流程见[触发容量](execution.md#trigger-capacity)。未成功创建的拒绝不永久保存，不提供永久请求结果重放；清理 Instance 后旧请求号的防重证据也随之消失。该有限保证与自动触发进度恢复分开，规则及顺序见[手动触发](behavior.md#manual-trigger)和[记账协议](execution.md#manual-trigger-accounting)，不改变 Task 回报的异步 ACK。

每次已到期处理消费从旧进度到锁后本次判定时间的全部 Cron 时间点，最多创建一个 Instance，新进度为严格晚于判定时间的首个 Cron 时刻；5 秒内也合并，不逐轮追赶。创建、执行配置与进度原子保存，未到期则不改进度，见[区间规则](behavior.md#trigger-advancement)。时间类型与精度、自动部分唯一索引及历史身份冲突处理已定；异常回拨后重新启用若遇到已有身份，保留旧 Instance 并推进当前 Job。历史清理的具体衔接仍待设计，不假设唯一索引能保留已删除历史的证据。

新尝试登记、后续阶段和候选收尾创建，仍通过 Instance 主键等值锁定单表单行来协调；锁内在受保护决策点按当前数据库时间检查父状态与截止，不用事务开始时间代替，也不保证物理提交或 RPC 到达在截止毫秒前。正常完成改为先在父锁外读取最少的稳定完成事实，再条件更新仍运行中的父记录；UPDATE 自身仍取得行锁，不能宣称无锁。人工停止和超时保存仍保留父短事务的适用部分，但不再将所有结束操作解释为“锁后到期必判超时”；自动超时先复用仅终态判定，只有明确不具备条件才进入父短事务复核状态和截止，具体衔接已通过。数据库另保证每轮最多一条 PostTask，不将全部分片类型限制为每轮一条。见[完成裁决](#instance-completion-contract)及[授权与终止协调](execution.md#authorization-terminal)。

发送前取得本地容量，按 Instance 组织有界批次，每个事务锁一条父行、批量条件登记该轮候选 Task，原子保存当前尝试、机会记账及执行中状态，提交成功后只发送本次登记项。普通首次派发在同一事务中启动 Instance 并插入已保存首次执行信息的 Task，不再单独更新登记。正常派发与恢复共用协议，不直接重发执行中的 Task。接受每轮每批的父行检查和短事务成本；同轮登记串行，不同轮并行，不使用 `IN` 锁多条父行。稀疏单 Task Instance 的成本不能被批量摊薄，见[记账及成本](execution.md#dispatch-accounting)。公共字段、单例约束及[普通首次启动 SQL](#ordinary-start-sql)已定，其余条件更新 SQL 仍待细化。

等待重试仅是候选状态。结果回报、失败／未知结算和 BUSY 记账按子 Task 条件处理，不统一锁父；它们可与父 STOPPING 交错，不能据此授权新执行或覆盖子终态。STOPPING 决定提交后，在停止决定事务之外另开每批父主键 NOWAIT 短事务，按[统一有界批量协议](#instance-termination-sql)取消未终态子 Task，不按任务类型分支；取消 RPC 独立尽力发送。数据库恢复以 STOPPING 父记录为入口，全部已有子 Task 终态后才结束父，不保证补送全部通知。

以下不变量继续有效；已通过的写入协议见本章对应 SQL 小节，其余 DDL 与条件写入仍待细化：

- 一次计划触发不能因为重复通知或 Leader 切换而无约束地重复创建 Instance。
- 分片生成完整之后集合固定；重复或旧准备结果不能再生成另一批分片。
- 旧尝试不能覆盖新尝试，重复回报不能重复推进，终态不能被迟到结果恢复。
- 绑定缺失结算须校验当前尝试及允许的旧状态；即使下一次尝试尚未创建，已结算的旧尝试也不得由迟到绑定或结果恢复。先到有效结果不以已有绑定为前提，已有冲突身份不得被静默替换。
- 已有持久化绑定但无有效结果时保留当前尝试及执行中状态，不以失联、重启、回报耗尽或 ACK 后结果丢失单独授权重派；整轮截止后按既有终止与子 Task 取消规则收敛。
- 明确 BUSY 只针对当前有效尝试条件处理；关闭该尝试、一次返还登记占用的机会及设置 `next_dispatch_at` 必须原子完成，重复／迟到 BUSY 不得多返还、覆盖新尝试或恢复子终态。与父终态交错的延期只产生候选，下一次新授权被父行检查禁止；[普通 BUSY 条件更新](#ordinary-busy-settlement)已定，其他类型及参数继续设计。
- 重试复用原 Task，遵守当前 Task 的独立预算和最早可派发时间。
- 已有效保存的准备或收尾成功结果不因平台推进失败而被重新执行业务替代。

Instance 的未终态为待开始、运行中、停止中，编码为 1、2、6；终态成功、失败、取消仍为 3、4、5。Task 的未终态为待执行、执行中、等待重试，编码为 1、2、3，终态成功、失败、取消编码为 4、5、6。两者分别使用自身业务状态类型，不混用通用启停枚举或彼此的数值。未结束不等于允许执行：首次启动明确要求 WAITING，后续生成／Post／尝试授权明确要求 RUNNING，STOPPING 均拒绝。[停止决定、子取消与父最终结束](#instance-termination-sql)和[正常完成父 UPDATE 与超时衔接](#instance-completion-contract)已通过，完整分片查询及生成 SQL 继续细化；不用一个大事务更新全轮父子记录。

[提交结果不明](execution.md#commit-unknown)不新增状态或历史表。登记不明撤掉本地发送，即使读回已登记也不补发；后续只按已提交事实走观察结算，或在实际未提交且仍合格时重新条件登记。BUSY 只在有界有限处理重试内按原尝试及尚未结算的旧状态，原子关闭、返还和延期；结果落库则沿用已有有界保留并重做同一条件写入。二者均不能因重试重复修改或复活旧尝试。必要核对按批读主库，正常成功路径不增加写后查询；查不到不能被当作原事务已回滚的证明。

索引设计应围绕启用 Job 的到期触发进度、同 Job 未结束 Instance、活跃实例、待处理 Task、到期工作及未结束分片等访问路径验证。使用 `EXISTS` 并不自动保证低成本；必须把已定状态集合落实为具体查询条件并检查执行计划。数据库约束与可调整业务校验的职责沿用仓库规范，不以接口校验工具的能力决定数据库职责。

### 普通 Task 首次创建与重复推进 SQL {#ordinary-start-sql}

**本节首次启动的写入顺序、重复推进及提交后发送条件已通过。** 适用于普通 WAITING Instance，不包含后续重试、执行器绑定、结果保存或终态推进 SQL。字段及单例索引沿用[Task 契约](#task-field-contract)，本节不是已执行的迁移或测试结果。

#### 锁后校验与取时

固定派发工作协程取出候选、具备本地派发容量后，开启短事务。数据库使用默认 `READ COMMITTED`，不显式修改隔离级别；按 Instance 主键等值尝试锁定单表单行，防止多个节点重复首次启动，并与整轮停止协调。2026-10-26 [派发竞争提案](execution.md#dispatch-contention)将本处等待式父锁修订为 NOWAIT：锁冲突即回滚退出本次该 Instance 组，不继续子操作、不占执行机会、不立即重新排队。事务内不调用 RPC 或其他外部服务。

2026-10-28 通过的[派发时间预算](execution.md#dispatch-time-budgets)从工作协程取出候选起计算默认 10 秒总处理预算；登记短事务默认最多 2 秒，包括连接等待、SQL 和提交确认，并受剩余总预算限制。超时或取消不证明事务已回滚，尚未实际退出的操作不能提前归还其占用的处理能力。

以下为参数化 SQL 示意，命名参数由应用绑定；Go 必须逐步判断返回值和错误，不能无条件执行整段。

```sql
BEGIN;

SELECT id, status, executor_group_id, execution_config,
       started_at, deadline_at, finished_at
FROM job_instance
WHERE id = :instance_id
FOR UPDATE NOWAIT;
```

取得锁后，确认 Instance 仍为待开始、快照模式为普通，且开始、截止及结束时间均为空。不存在、已经运行、已经结束或非普通模式，不执行本节首次创建，也不查询已有 Task 来补发。待开始但时间字段异常时，按内部数据异常处理，不覆盖修复。执行配置来自当前锁定的 Instance 快照，不重新读取当前 Job 配置。

校验通过后读取一次数据库当前时间：

```sql
SELECT clock_timestamp() AS start_time;
```

这次取时发生在成功取得行锁之后，不使用事务开始时间，也不把取时表达式放进前一条加锁查询。开始、截止计算及首次尝试登记共用这个值，本地排队和连接获取不提前消耗本轮执行预算；父锁冲突已退出，不通过等待该锁继续启动。下列基准 SQL 用独立查询表达取时，存在一次额外数据库往返；数据库时间是跨调度节点执行判断的共同来源，不保证数据库时钟永不跳变。

这不修改[Job 触发时间来源](behavior.md#trigger-clock)：Cron 到期、错失及下一次计划计算继续使用锁后的调度进程 `time.Now()`；本节的 Instance／Task 执行计时继续使用数据库时间，不将两类判断合并。

#### 父行更新与 Task 插入

`:timeout_seconds` 来自本轮执行快照。先条件更新 Instance，维护 `updated_at`，必须恰好更新一行；零行或任何错误都结束并回滚，不继续插入 Task。

```sql
UPDATE job_instance
SET status = 2,
    started_at = :start_time,
    deadline_at = :start_time
                  + :timeout_seconds * INTERVAL '1 second',
    updated_at = :start_time
WHERE id = :instance_id
  AND status = 1
  AND started_at IS NULL
  AND deadline_at IS NULL
  AND finished_at IS NULL
RETURNING deadline_at;
```

随后插入该轮唯一普通 Task，直接保存执行中状态、首次执行编号及已用次数；不另开事务登记首次尝试。绑定、结果、结束时间及最早派发时间等可空字段此时为空。

```sql
INSERT INTO job_task (
    instance_id, task_type, status,
    attempt_no, used_attempts,
    attempt_registered_at, started_at,
    created_at, updated_at
)
VALUES (
    :instance_id, 1, 2,
    1, 1,
    :start_time, :start_time,
    :start_time, :start_time
)
RETURNING id;

COMMIT;
```

插入必须成功创建一条 Task。此处不使用 `ON CONFLICT DO NOTHING` 或覆盖式 UPSERT：正常并发已由父行锁协调，若待开始的 Instance 下已有普通 Task，单例冲突属于协议或数据异常。整个事务回滚，包括前面的 Instance 更新；记录持久化错误，不删除、覆盖旧 Task 或自动创建另一条。这不同于触发阶段合法的同请求身份重放，不照搬其冲突忽略写法。

实现继续使用现有 GORM 行锁、显式字段更新、`Create` 和事务适配，不以本节 SQL 示意为由将普通仓储整批改成 Raw／Exec。涉及取时等 SQL 表达式按职责放在仓储，不在 RPC Logic 直接访问数据库。

#### 重复推进与提交后派发

两个节点同时推进同一 Instance 时，先取得父行锁的一方完成上述更新和插入并提交；另一方若遇到锁冲突即回滚退出，转做其他 Instance，不在当前组等锁或立即重抢。如果到达时前一方已经提交，则仍可能取得锁后读到已运行状态，结束本次首次启动处理；NOWAIT 不消除这种无效事务。如果前一方明确回滚，后续重新发现的候选仍可依据待开始状态正常启动。不重设已开始轮次的计时，不追加执行机会，也不把读到的已有 Task 当作本次派发许可。

**本事务确实新建了 Task，且整个事务明确提交成功，两者同时满足才允许发送 RPC。** 事务无操作正常返回也可能提交成功，不能仅凭事务返回 nil 派发；ORM 已回填内存的 Task ID 也不证明事务提交。成功创建并确认提交后，由当前工作协程直接发送本次尝试，不重复排队或登记。

| 情况 | 行为 |
| --- | --- |
| 本次新建 Task 且明确提交成功，仍有派发处理预算 | 当前工作协程派发本次尝试 |
| 父行 NOWAIT 锁冲突 | 回滚退出本次该 Instance 候选组，不执行后续子操作、不发送、不立即重排；后续查询重新发现 |
| 重复推进，Instance 已运行或结束 | 退出首次启动，不查询并补发旧尝试 |
| 事务明确回滚 | 不发送；后续可重新处理仍合格的 WAITING Instance |
| 提交结果不明，或提交后发送前退出 | 不补发同一执行编号；沿用绑定缺失观察和条件恢复 |
| 已明确提交，但尚未发送时总处理预算耗尽 | 放弃本次发送，不免费补发或返还机会；后续按持久化状态和无绑定观察规则处理 |
| 停止先取得父行锁并提交 | 首次启动读到 STOPPING 或终态，不创建 Task；停止事务仍持锁时按 NOWAIT 冲突退出 |
| 首次启动先提交，停止随后提交 | 已登记 RPC 可能在途，沿用尽力取消及禁止后续新尝试的规则 |

具体故障与并发用例见[首次创建 SQL 验收](verification.md#ordinary-start-sql-checks)。本项不增加负责人、租约、锁表或可靠本地发送记录，不代表后续状态更新、完整迁移及执行模块已实现。

### 接单 ACK 的执行器绑定 SQL {#executor-binding-sql}

**本节条件更新、零行处理、父终态边界及有限保存重试已通过。** 适用于已经发送的 Task 当前尝试收到有效接纳 ACK 后，补存执行器启动身份和直连地址；不重新登记尝试。结果保存、重试登记及未知结算另见[普通结果协议](#task-result-persistence)、[普通重试登记 SQL](#ordinary-retry-registration)与[普通未知结算 SQL](#ordinary-unknown-settlement)。

#### 输入与写入范围

只有有效的接纳 ACK 才进入绑定保存。进程身份来自该响应中的合法 UUIDv4；地址取调度端本次派发调用的 `grpc.Peer`，必须符合已定的实例直连寻址条件，不能使用回报请求的来源地址、另一条 RPC 的 Peer 或共享代理地址。BUSY、其他明确拒绝及 RPC 错误不作为接单证据；响应或地址无效时不写入不完整绑定，也不据此重发业务。

Task ID、Instance ID 与执行编号使用本次发送请求中保存的值，不能读数据库当前编号来替换原编号，也不依赖响应回显重新选择目标记录。一次成功绑定只更新 `executor_instance_id`、`executor_address` 和审计字段 `updated_at`，两项绑定信息必须同时保存。

```sql
UPDATE job_task
SET executor_instance_id = :executor_instance_id,
    executor_address = :executor_address,
    updated_at = :updated_at
WHERE id = :task_id
  AND instance_id = :instance_id
  AND attempt_no = :sent_attempt_no
  AND status = 2
  AND executor_instance_id IS NULL
  AND executor_address IS NULL;
```

实现沿用 GORM `Where` 与 `Updates(map[string]any)`，显式列出绑定字段并保留全部条件。`updated_at` 按既有审计更新方式维护，不为绑定另查一次数据库时间；它不参与观察期或执行期限判断。单条条件更新不额外使用显式 `FOR UPDATE`，不先查询再无条件覆盖，也不修改状态、执行编号、已用次数、执行时间、结果或父 Instance。

#### 影响行数与竞争结果

| 写入结果 | 处理 |
| --- | --- |
| 恰好更新一行，且确认提交成功 | 本次新增绑定成功，结束绑定处理 |
| 无错误但更新零行 | 当前条件不允许新增绑定，结束处理；不重试零行，不追加查询判断原因，不将其报告为新增绑定成功 |
| 数据库错误或提交确认不明 | 不宣称成功或回滚；仅按下节有限重试同一绑定写入，禁止重新发送任务 |

零行可能是记录不存在、旧尝试已失效、已有绑定或 Task 已结束等情况。零行本身不能证明具体原因，尤其不能据此发送取消通知；按 Instance 取消可能影响本轮其他合法工作。两项绑定字段只要任意一项已有值就不写入，不覆盖相同或冲突绑定，也不顺便修复部分缺失数据。

结果保存、未知结算和绑定更新通过同一 Task 行的条件写入竞争：

- 有效结果先保存并使状态离开执行中：绑定更新为零行，不覆盖结果；成功 Task 可以没有地址。
- 未知结算先把状态改为等待重试或失败：即使执行编号尚未变化，旧绑定也不能再写入。
- 新尝试已开始：旧请求的执行编号不匹配，不覆盖新尝试。
- 绑定先成功：后续未知结算因已有绑定不再满足条件；有效结果仍按结果协议处理。

绑定条件不包含观察期截止时间。观察期到期仅允许恢复检查尝试结算，未成功结算前，有效绑定或结果仍可先写入。收到 ACK 但延迟保存、数据库等待或有限重试都可能产生迟到的绑定写入；这不表示已返回超时的同步 gRPC 调用后来还会再次返回成功 ACK。

#### 父停止决定与取消边界

绑定只是补存已派发尝试的目标信息，不授予新执行许可，因此不额外查询或锁定父 Instance。父为 STOPPING、子 Task 尚未收敛且仍满足上述条件时，允许补存绑定；子已成功、失败、取消或等待重试时不写入。补存不恢复父子状态，不改变本轮截止时间，也不能据此启动下一次尝试。

父停止决定与子 Task 收敛按[停止协议](#instance-termination-sql)处理。接受取消目标读取之后才补齐绑定、从而错过该次取消通知的窗口；不新增可靠取消记录或“绑定成功必补发取消”的保证，也不以绑定更新零行推断需要取消。

#### 保存失败与资源释放

保存失败或提交确认不明时，在当前固定派发工作协程内有限重试同一条件更新，始终使用原请求身份、原 ACK 身份及本次 Peer 地址。重试仅保存元数据，不重新调用 `DispatchTask`，不增加或返还执行机会，不重置观察起点；不新增后台绑定队列、无界协程或等待业务结束的对象。

更新零行或明确成功即结束；可恢复数据库故障或提交确认不明时，按[已通过的派发时间预算](execution.md#dispatch-time-budgets)有限重试。从开始处理该响应起，绑定保存阶段默认最多 4 秒，最多 3 次保存尝试（包含首次），每次数据库操作最多 1 秒、失败后间隔 200 毫秒。连接等待及执行等待计入单次保存，间隔计入阶段及总预算；始终受从取出候选起的 10 秒剩余总预算和停机剩余宽限限制，不保证一定尝试满三次。

保存使用独立于已结束接单 RPC 的处理 Context，不因 RPC 返回而取消保存，也不因父进入 STOPPING 改写既定条件。预算耗尽停止发起新的保存尝试；仍未实际退出的操作不能被当成已经释放并发能力。处理结束释放 ACK 数据，工作协程继续消费下一项，不复用执行结果在可恢复故障下的长期有界保留策略。

先前写入可能已经提交，重做得到零行时同样停止，不额外核对为“绑定成功”。保存尝试耗尽不撤销可能已提交的绑定；后续按数据库实际状态走已绑定等待、有效结果处理或绑定缺失观察，不保证原目标一定能恢复。

具体并发和故障用例见[ACK 绑定 SQL 验收](verification.md#executor-binding-sql-checks)。本节未增加字段、Attempt 历史表或新的恢复扫描器；不代表这些 SQL 已实现或经过真实数据库验证。

### 普通 Task 结果保存与状态转换 {#task-result-persistence}

**后台归属校验、结果条件更新、三种状态分支及提交后处理规则已通过；2026-10-29 已确认单条 JOIN、逐条条件 UPDATE 和独立提交，以及结果保留、保存并发、超时和退避初值。** 本节以普通 Task 为主，沿用单条异步回报、调度端入队 ACK 和后台有限并发保存，不将多个结果绑定在同一事务中；不展开准备工作项存储、分片生成或 Instance 汇总 SQL。[回报错误分类与无效结果处理](#result-error-classification)已于 2026-10-30 通过；[执行 RPC 消息与装配](#execution-rpc-contract)已通过，Proto 源文件生成、驱动适配及装配实现验证仍须完成。

#### 单条关联读取与核验

回报入口完成组认证、报文校验并成功交给有界结果保存组件后确认接收，不查询 Task 表。组件保留已认证的组上下文；后台每次处理一条结果，用一次 JOIN 读取对应 Task 及其所属 Instance 的必要字段，核对 Job／Instance／Task 关系、与认证组是否一致，并取得本轮冻结的重试上限和间隔。只投影必要信息，不读取当前 Job 配置，不先读 Task 再单独补查父记录，也不扫描父 Instance 的整轮 Task。

本轮明确不要求先组织批量预读或等待凑批。N 条可写结果的正常保存成本是 N 次单条 JOIN 加 N 次条件 UPDATE，以及各自独立提交；不将其描述为与一次批量查询或批量写入成本相同。读取目的为归属、身份和重试规则核验，不把父仍为 RUNNING 或尚未截止另加为结果保存条件。

前置读取用于归属和配置核验，不代表写入时尝试仍有效。每条结果的条件更新必须保留以下谓词；下方仅展示并发与身份条件，不省略前述归属校验：

```sql
WHERE id = :task_id
  AND instance_id = :instance_id
  AND attempt_no = :reported_attempt_no
  AND status = 2
  AND (
      executor_instance_id IS NULL
      OR executor_instance_id = :reported_executor_instance_id
  )
```

报文使用原 Task、尝试及进程身份重传，不能将旧结果套到查询到的新编号。已有绑定必须匹配回报进程；未绑定仍允许当前有效尝试的结果保存。实现沿用 GORM 条件更新和显式字段映射，不先查后无条件覆盖，也不为每条结果增加父行显式锁。每条结果独立保存和提交，保留各自条件核验、行写及 WAL 成本；JOIN 后发生的并发变化仍由 UPDATE 的原尝试、执行中状态和绑定身份条件最终裁决。

#### 一次保存结果和目标状态

一条结果的状态、结果及相关时间使用同一条条件 UPDATE 原子保存，不先写最终失败、再另一次改成等待重试；不同结果独立提交。下表中的处理时间使用该条结果本次保存的数据库处理时间，不使用执行器自报时间，也不将其描述为业务实际结束或精确 COMMIT 时刻。具体取时 SQL 随实现核对，不在本节提前固定。

结果保存不设置批级总超时，也不设计整批超时后降级拆批。每条结果的一次处理默认最多 **2 秒**，覆盖获取数据库连接、JOIN 读取、条件更新及提交确认，不包含领取前排队和后续退避；部署可调，不给各步骤重新分配完整两秒，也不增加独立锁等待超时或其他细分预算。停机时还受共享 30 秒剩余宽限限制。超时或 Context 取消不证明数据库已回滚，实际操作退出前不能提前释放处理能力；这些约束不改变 Task 尝试规则或 Instance 的原截止时间。

| 回报及预算 | `status` | 结果字段 | 时间与原因字段 |
| --- | --- | --- | --- |
| 成功 | 4：成功 | `result_data` 保存本次可选 Data，`result_summary` 保存本次摘要 | `finished_at` 为处理时间，`end_reason` 为成功原因，`next_dispatch_at=NULL` |
| 失败，仍有执行机会 | 3：等待重试 | `result_data=NULL`，`result_summary` 保存本次失败说明 | `next_dispatch_at` 为处理时间加本轮重试间隔，`finished_at=NULL`、`end_reason=NULL` |
| 失败，执行机会耗尽 | 5：失败 | `result_data=NULL`，`result_summary` 保存本次失败说明 | `finished_at` 为处理时间，`end_reason` 为最终执行失败原因，`next_dispatch_at=NULL` |

默认额外重试 3 次，首次加重试共允许占用 4 次机会；是否还有机会由 `used_attempts` 与本轮预算判断。失败不采用返回的业务 Data 作为成功输出，成功没有提供 Data 时同样明确写空，不保留上一次内容。所有分支维护 `updated_at`，但不将其用作执行或重试时间。

结果保存不递增 `attempt_no`，不扣减或返还 `used_attempts`，不改变首次及当前登记时间。失败写成等待重试后，本次尝试已关闭，但尚未授权下一次执行；后续登记须重新检查父状态、截止、Task 状态及预算，成功登记才增加执行编号并占用下一次机会。数据库处理时间不延长整轮截止，也不保证剩余时间足以再执行一次。

#### 不从结果补写执行器绑定

结果更新不修改 `executor_instance_id` 或 `executor_address`。绑定仍由[接单 ACK 协议](#executor-binding-sql)结合实际派发 Peer 原子保存，不从回报请求的来源地址推断执行器监听地址，也不单独补 UUID 形成半绑定。

结果先提交并使状态离开执行中后，迟到 ACK 的绑定更新为零行。接受此时 Task 可能没有持久化执行器身份和地址；回报进程身份可用于关联日志，不新增历史尝试表或后补地址流程。已有绑定则原样保留，不能被回报覆盖。

#### 重复结果及父子终止竞争

恰好更新一行且该条保存明确提交成功，才表示本次结果生效。无数据库错误但影响零行，只表示本次未应用；不追加查询猜测重复、旧尝试、身份冲突、取消或不存在等原因，也不宣称此前已保存成功。同一尝试出现相反结论时，先有效持久化的结果生效，后来结果不得覆盖；正常执行组件应始终重传同一结果。

保存 Task 结果不统一锁定父 Instance，也不把结果保存当作正常推进或重新派发许可。子 Task 取消先提交时，结果条件不成立；有效成功或最终失败先提交时，保留该子终态。父为 STOPPING、子尚未收敛时仍可能保存有效结果，或短暂形成等待重试；任何结果都不能撤销停止决定、恢复父终态、授权重试或启动正常 Post。后续推进及恢复按[父子终止规则](behavior.md#task-convergence)处理。

#### 写库故障与提交后提示

某条保存遇到数据库技术错误、原因不明或提交结果不明时，按[调度端结果保留规则](execution.md#scheduler-result-retention)保留该条原结果，退避后重新核验并重做同一条件写入；不回滚或重放其他已经明确提交成功的结果，不重新执行业务，不套用 ACK 绑定的有限重试后释放策略。只有正常完成保存、无错误零行或已经明确的永久无效结果才结束保留；不能把未知技术错误直接当作永久无效。重新处理仍按当前事实核验，不因此前已经返回接收 ACK 而强制采用结果；零行不证明此前保存成功。

每调度进程默认保留 **512 条**结果，包含首次待处理、延迟重试和正在保存的记录；保存并发默认 **8**，不在 512 之外再加 8 条在途额度。两项及上述单次处理 2 秒均为部署参数。一次处理实际结束即释放保存并发；需要重试的结果保留原位置，等待期间不占处理并发，不通过重新接纳结果刷新保留范围。

数据库重试间隔采用 **1／2／4／8／16／30 秒，后续名义间隔封顶 30 秒，各带 ±20% 抖动**的内部初值，不设置结果 TTL 或重试次数上限。首次可处理时间为组件接纳时刻；重试为本次处理完成时间加正退避。在已到期结果中选择可处理时间最早者，同时间按稳定接纳顺序，不切首次和重试固定配额；同一在途记录不被重复领取。满时拒收，全部保留记录都在退避时允许保存者空闲；数据库恢复也不承诺立即排空。具体 Go 原语不在此冻结，完整初值见[调度端结果保存配置](execution.md#scheduler-result-defaults)。

该条保存正常完成并确认提交后，释放已处理结果，不再向执行器反向发送落库 ACK，也不等待其他结果提交。对已核实归属的 Instance，可发本地推进提示，包括重做后没有再次更新的情况；提示只要求读取数据库当前事实，不直接按原结果创建后续任务或恢复旧状态。归属未核实或跨组引用不能据报文 ID 触发推进。等待重试只形成候选，不在结果保存线程中立即派发。

提交后未及通知或本地提示未接纳，继续由既有恢复检查补上；没有明确提交成功时，不将影响行数当作提交证据。各阶段推进、结果保留和执行预算继续分开。对应[验收清单](verification.md#task-result-persistence-checks)尚待实现验证，不表示本节已经编码或执行 SQL 测试。

### 普通 Task 等待重试与新尝试登记 {#ordinary-retry-registration}

**本节候选身份、父行校验、条件更新及提交后派发协议已通过。** 范围是普通 Task 从等待重试进入下一次执行，不新建 Task，不改变结果保存、BUSY 返还或未知结算的职责；这些入口先形成重试候选，真正派发仍须经过本节授权。

#### 候选与父行校验

到期候选复用既有有界检查及派发队列，由每调度进程默认 64 个固定工作协程处理，不新增重试队列、延迟表或专用扫描器。候选携带 `TaskID`、`InstanceID` 和读取时的 `expected_attempt_no`；入队不消耗执行机会。无容量或队满时保留数据库里的等待重试状态和时间，后续再接续，不提前登记。

工作协程取出候选后，开启默认 `READ COMMITTED` 的短事务。按[派发时间预算](execution.md#dispatch-time-budgets)，总处理从取出候选起默认 10 秒，登记事务最多 2 秒（含连接等待、SQL 和提交），同时受剩余总预算限制：

1. 使用 `SELECT … FOR UPDATE NOWAIT` 按主键等值尝试锁定单条 `job_instance`，与停止、超时及正常结束协调；不额外显式锁 Task，不使用多行或跨表锁。此处等待方式已于 2026-10-26 [单独修订](execution.md#dispatch-contention)：父锁冲突即回滚退出本次该 Instance 候选组，不继续子操作、不占执行机会、不立即重排，继续处理其他 Instance。
2. 取得父锁后，单独执行一次 `SELECT clock_timestamp()`，作为本次 `registered_at`。不能使用候选查询、排队、连接获取或事务开始时的旧时间；NOWAIT 不取消锁后取时要求。
3. 校验 Instance 仍为运行中，首次开始和截止时间有效，锁后当前时间严格早于截止，并且本轮快照为普通模式。父为 STOPPING 则不登记、转停止处理，已有终态则退出；已到期则不登记，转入[完成与超时判定](#instance-completion-contract)，不能凭旧重试候选直接判超时或继续授权。
4. 从该 Instance 的不可变 `execution_config` 取得重试上限，计算包含首次的 `max_attempts = 1 + max_retries`，执行下方条件更新。不读取当前 Job 替换本轮配置，也不在事务内查询 etcd 或发送 RPC。

此处是普通单 Task 的授权事务，不要求未来每条分片分别开事务；同 Instance 的有界批量登记仍按[公共协议](execution.md#attempt-registration)设计。

#### 原 Task 的条件更新

```sql
UPDATE job_task
SET status = 2,
    attempt_no = attempt_no + 1,
    used_attempts = used_attempts + 1,
    attempt_registered_at = :registered_at,
    next_dispatch_at = NULL,
    executor_instance_id = NULL,
    executor_address = NULL,
    result_data = NULL,
    finished_at = NULL,
    end_reason = NULL,
    updated_at = :updated_at
WHERE id = :task_id
  AND instance_id = :instance_id
  AND task_type = 1
  AND status = 3
  AND attempt_no = :expected_attempt_no
  AND next_dispatch_at <= :registered_at
  AND used_attempts < :max_attempts
RETURNING id, instance_id, attempt_no,
          used_attempts, attempt_registered_at;
```

`status=3` 为等待重试，`status=2` 为执行中。编号和已用次数在同一次更新中各加一；预算由 `used_attempts` 判断，不能改用 `attempt_no`。预算计算及自增不得整数回绕，失败应回滚，不因此扩大执行机会。`updated_at` 沿用审计字段维护方式，不另作观察或截止依据。

旧执行器身份与地址一起清空，下一次接单后再按[绑定协议](#executor-binding-sql)保存；业务 Data 和终态字段置空，不清除首次开始时间。Instance 的开始及截止时间均不重置，重试不会重新获得一整轮超时时长。

`expected_attempt_no` 必须来自候选原值，不能取得父锁后替换为数据库最新编号。例如 A、B 都持有第 1 次的候选，A 已登记第 2 次且第 2 次又失败回到等待重试，B 的旧候选仍须更新零行，不能借相同等待状态登记第 3 次。后续正常发现的新候选可按新编号重新竞争。

#### 最近结果说明与执行预算

重试登记**保留 `result_summary`**。字段表示最近保存的结果说明或失败诊断，重新派发并未产生新结果，且不另设尝试历史表。页面与接口说明应称“最近结果说明”，不能称“当前尝试结果”；它不保证属于当前 `attempt_no`，非空不能证明当前尝试已得到有效结果，未知结算也不能通过摘要是否为空判断。

后续有效结果按[结果保存协议](#task-result-persistence)覆盖摘要；成功返回空摘要时同样覆盖，不能保留旧失败说明冒充新成功说明。不增加摘要所属尝试号或历史记录字段。

明确 BUSY 已按原协议返还机会时，首次登记 `(attempt_no, used_attempts)=(1,1)` 可先变为 `(1,0)`，再次登记变为 `(2,1)`。即使 `max_retries=0`，也允许重新占用被返还的首次机会；编号递增不等于额外消耗业务重试。[普通 BUSY 条件返还 SQL](#ordinary-busy-settlement)已定，退避参数仍待容量设计，本节不负责返还。

#### 提交、竞争及发送边界

| 结果或竞争 | 处理 |
| --- | --- |
| 本次 UPDATE 返回一条新登记记录，且事务明确提交成功，仍有派发处理预算 | 当前工作协程只发送该返回记录对应的新尝试，随后沿用 ACK 绑定协议 |
| 父行 NOWAIT 锁冲突 | 回滚退出本次该 Instance 候选组，不登记或发送，不逐条使用同组剩余候选连续争锁；后续查询重新发现 |
| UPDATE 零行，或事务正常结束但没有新登记记录 | 不派发，不把候选编号换成当前编号继续更新，不据零行推断具体原因 |
| 事务明确回滚 | 不发送，本次编号和次数更新不生效 |
| 提交结果不明 | 撤销本地发送计划并释放资源；事后读到已登记也不补发，不退款，沿用既有观察与恢复规则 |
| 确实提交但节点在发送前退出 | 不能由其他节点补发同一尝试；可能未运行业务却已占用机会 |
| 已明确提交，但尚未发送时总处理预算耗尽 | 放弃本次发送，不补发或返还执行机会；后续按持久化状态和无绑定观察规则处理 |
| STOPPING 或父终态先提交，或取得父锁后判断已经到期 | 不登记新尝试；STOPPING 接续子取消，终态退出，仍运行但已到期则转入完成／超时判定，不凭旧候选强制判超时 |
| 登记先提交，随后停止或到期 | 可能仍有已授权 RPC 在途；沿用尽力取消，不保证停止后绝无发送或业务副作用 |

行数与 `RETURNING` 只表明事务内更新结果，不能单独证明提交成功。取得父锁后的时间是授权决策点，不承诺物理 COMMIT 或 RPC 到达也在同一截止毫秒之前。旧 ACK、结果及 BUSY 继续携带原编号并受旧状态条件限制，不能覆盖新尝试。

对应[重试登记验收](verification.md#ordinary-retry-registration-checks)是待实现清单。[普通未知结算](#ordinary-unknown-settlement)、[普通 BUSY 返还](#ordinary-busy-settlement)及[停止超时与子取消](#instance-termination-sql)的适用 SQL 已通过；到期分支按新批准的完成／超时规则衔接，完整条件写入、查询索引和容量参数继续分项细化；本节批准不表示完整执行模块已经实现。

### 可派发候选查询与索引 {#dispatch-recovery-query}

**本节两类候选的筛选条件、返回字段及索引已通过，2026-10-28 另通过临时分页及供给参数。** 仅发现持久化的待开始 Instance 和到期等待重试 Task，不领取执行权，也不代替首次开始或新尝试登记。查询与派发遵守[已定资源分工](execution.md#recovery-resources)，索引尚未因此创建，执行模块尚未获编码授权。

#### 待开始 Instance

复用已有 WAITING 部分索引 `(id) WHERE status = 1`，只返回 Instance ID，按 ID 有界读取。每趟开始固定当时可见 WAITING 候选的最大 ID 为 `:upper_id`；无候选则结束本趟。以下查询包含已批准的上界与续页条件，不表示当前实现已经具备完整分页流程：

```sql
SELECT id
FROM job_instance
WHERE status = 1
  AND id > :cursor
  AND id <= :upper_id
ORDER BY id
LIMIT :limit;
```

`:cursor` 仅用于当前扫描的临时分页，首轮从头读取，后续按最后已交接位置续扫；`:upper_id` 不随本趟新增记录上移。下一趟从头覆盖较小 ID 的迟提交记录，不因已查询或投递就永久越过。具体游标推进规则见[分页生命周期](#dispatch-candidate-pagination)，不承诺严格创建时间 FIFO。

#### 到期等待重试 Task

等待重试使用公共 Task 状态，不按普通、准备、分片或收尾分别建立重试查询。每趟开始取得一次数据库当前时间作为固定 `:scan_now`，限制本趟到期候选范围；不能替代派发授权事务锁后重新取得的时间。以下为续页查询，首个查询不添加复合游标比较条件：

```sql
SELECT t.id, t.instance_id, t.attempt_no, t.next_dispatch_at
FROM job_task AS t
JOIN job_instance AS i ON i.id = t.instance_id
WHERE t.status = 3
  AND t.next_dispatch_at <= :scan_now
  AND (t.next_dispatch_at, t.id) > (:cursor_next_dispatch_at, :cursor_id)
  AND i.status = 2
  AND i.deadline_at > :scan_now
ORDER BY t.next_dispatch_at, t.id
LIMIT :limit;
```

候选保留读取时的 `attempt_no`，作为后续条件写入的原尝试编号，不在处理时替换为最新编号。返回的 `next_dispatch_at` 与 ID 构成本趟复合游标，不额外补查时间字段或以查询后的新值改写原分页位置。父关系在查询中关联过滤，不逐条补查父状态；扫描不加载业务参数、结果或完整配置，不读取当前 Job 的启停及配置决定旧轮次是否继续。实际处理仍沿用 Instance 的本轮快照，父为 STOPPING 时接续子取消、终态退出，仍运行但到期则转入完成／超时判定。

新增等待重试部分索引，按最早可派发时间及 ID 支持筛选和排序：

```sql
CREATE INDEX idx_job_task_retry_due
ON job_task (next_dispatch_at, id)
WHERE status = 3;
```

父 Instance 通过主键关联，不为本查询另加通用状态索引或 JSONB 索引。固定状态谓词与部分索引保持一致；实现沿用 GORM 条件查询及 JOIN，并验证驱动实际 SQL 与执行计划。索引排除非等待重试的 Task，但父条件过滤可能使实际访问条数大于返回条数；不能将 LIMIT 当作数据库总扫描量上限，实施时须用代表性历史量及积压比例验证。

#### 候选分页生命周期 {#dispatch-candidate-pagination}

**2026-10-28 已通过。** 两类候选分别保存本趟上界与临时位置：WAITING 使用固定 ID 上界与 ID 游标，重试使用固定数据库时间与 `(next_dispatch_at, id)` 游标。不用 OFFSET、不增加持久化游标或领取字段，不因重试类型增加独立查询协议。

`:limit` 最多 128，还须受剩余等待候选容量限制；本地等待总量 256 包括查出但尚未交给派发处理的临时页及提示。没有空间不发起补充查询，不能先无界读取再等待入队；这不表示实际扫描行数必然不超过 LIMIT。

游标仅越过已经接纳、合并或按既定竞争规则明确跳过的候选。若页面中的候选按 Instance 调整处理顺序，仍按数据库原排序确认能够连续前移的位置，不能跨过尚未接纳的候选。满载暂停并保留未完成位置，恢复容量后接续；查询失败不将未知结果当成空页、不前移位置，按故障规则退避。

每趟到尾后等待下一次补查，再重新固定上界并从头遍历；临时位置不证明执行成功，也不永久排除低 ID 迟提交、后续到期或先前竞争失败的工作。固定上界并不创建数据库长事务快照，分页间的状态或时间变化仍可能使记录移动或消失，由后续遍历与实际登记条件处理。

空闲补查为 1 秒、各节点独立上下浮动 20%；有候选及容量时继续供给，满载暂停。连续读取最多 4 页仍无本轮补查候选明确提交成功的新登记时退避，成功只恢复至 4 页、不累加；入队、失败事务、提交不明及无关的本地正常派发不能刷新预算。已经交接的候选后来登记成功可以续扫，但不能突破候选容量、同组冲突退出或到尾后等待下一趟的规则。完整行为见[派发供给](execution.md#dispatch-supply-defaults)，不把默认最多返回 512 条候选解释为实际扫描成本或每秒成功吞吐上限。

#### 扫描与实际派发的边界

两类查询均不加锁、不更新状态、不登记尝试或占执行机会。恢复只在有容量时有限投递已有派发组件，队满停止补充，未接纳工作继续以数据库事实为依据，不创建无界等待协程。工作协程取出后仍须按既定事务重新核验父状态、截止、原尝试及剩余次数；只由本次登记成功且明确提交的一方发送。

本节不增加普通首次 Task 的待执行中间步骤，仍由首次开始事务直接创建执行中 Task。分片首次待派发的查询还依赖生成完整性封口条件，须随生成协议补齐；本节公共重试查询通过不代表所有类型的登记写入 SQL 已完成。[派发竞争规则](execution.md#dispatch-contention)已另行批准本地候选合并、父行 NOWAIT、失败跳过同组，以及补查抖动和已接纳候选不同起点处理的原则；SQL 仍按索引有序查询，不增加随机排序，临时分页不因争锁失败反复回头或变成永久水位。[派发供给](execution.md#dispatch-supply-defaults)已确认等待量、查询页、周期、抖动、轮转初值与无进展退避，登记事务批量和具体编码结构仍待落实。对应[查询验收](verification.md#dispatch-recovery-query-checks)、[竞争验收](verification.md#dispatch-contention-checks)及[供给验收](verification.md#dispatch-supply-checks)尚未运行。

### 绑定缺失观察到期的候选查询 {#unbound-recovery-query}

**本节候选条件、返回字段、排序及精确部分索引已通过；2026-10-29 另已通过[观察到期扫描与结算安排](execution.md#unbound-recovery-defaults)，补齐临时分页及处理参数。** 它只发现已登记执行、执行器绑定仍缺失且观察期已到的尝试，由各节点后台独立扫描／结算，不直接派发。查询使用公共 Task 字段，不按执行模式另建一套恢复机制；具体类型的结算写入仍须遵守其已定协议，不因本节通过就视为所有阶段写入 SQL 均已完成。

Task 在首次或重试登记提交后，尚未取得并保存接单 ACK 时，会正常短暂处于执行中且双绑定为空。调度节点退出、ACK 丢失或绑定保存失败可能使这个状态持续；因此它不等于业务尚未执行，也不是仅针对异常记录才发生的状态。已有绑定但未回报结果的执行不属于本查询，仍沿用等待结果、人工停止或整轮终止的规则。

#### 查询条件与返回字段

每趟开始取得数据库当前时间作为固定 `:scan_now`，`:observation_cutoff = :scan_now - 观察期`；分页及跨运行续扫不刷新本趟上界。[派发时间预算](execution.md#dispatch-time-budgets)已确定观察期默认 30 秒。观察从当前 `attempt_registered_at` 起算，不使用首次开始、审计时间或上次扫描时间，重复扫描、节点变更和绑定保存重试不重置起点；各调度节点保持观察期配置一致。到期仅获得条件结算资格，不是准时恢复承诺。以下为续页查询，首个查询不添加复合游标比较条件：

```sql
SELECT t.id, t.instance_id, t.attempt_no, t.attempt_registered_at
FROM job_task AS t
JOIN job_instance AS i ON i.id = t.instance_id
WHERE t.status = 2
  AND t.executor_instance_id IS NULL
  AND t.executor_address IS NULL
  AND t.attempt_registered_at <= :observation_cutoff
  AND (t.attempt_registered_at, t.id) > (:cursor_registered_at, :cursor_id)
  AND i.status = 2
  AND i.started_at IS NOT NULL
  AND i.deadline_at > i.started_at
  AND i.deadline_at > :scan_now
ORDER BY t.attempt_registered_at, t.id
LIMIT :limit;
```

两个绑定字段必须同时为空，单边非空不能当作完全未绑定。摘要和 Data 是否为空不参与候选判定：最近摘要可能来自旧尝试，而当前有效结果与状态同次保存后必然离开执行中状态。查询返回 Task ID、Instance ID、原尝试编号及登记时间；登记时间与 ID 只用于本趟分页，不以处理时的新值改写原位置，也不为游标额外查询。不加锁、不写状态、不登记重试，也不读取当前 Job 配置。

候选若经过等待，处理时须按已有短批规则重新取得数据库时间，并通过关联或有界批量读取复核父状态、运行时间和本轮配置，不能用扫描旧时间冒充当前结算时间，不逐条补查父。结算条件更新继续保留原候选编号、执行中、双绑定为空和观察到期条件；ACK 绑定、有效结果或其他结算先保存后，本次可以更新零行。STOPPING 接续子取消、父终态退出，仍运行但已到截止的记录转由完成／超时路径处理，不利用此候选直接重派或覆盖父终态。

#### 临时分页与本地运行边界

各节点独立参与，每节点只有一个不重叠扫描／结算流程，本地串行处理；本次运行实际结束后再等待默认 10 秒、上下浮动 20%，然后新开或续扫。每次运行总预算默认 5 秒，包含取时、候选查询、短批读取及逐条提交；每次数据库操作默认 1 秒，包含连接等待、执行和适用的提交，受剩余总预算限制。等待、每页 128 条及两个时间预算均部署可调；父状态与本轮配置的复核短批最多 16 条，为内部初值。

每小批重新取得处理时间并批量读取必要父状态／配置，逐条 Task 条件更新并独立提交，不把复核批中的 UPDATE 合并进同一事务。预算内可继续多页，128 不是整次运行上限；扫描只保留当前有界页及短批必要信息，不新增长期候选队列或全量缓存。跨运行接续时重新取处理时间和复核，不复用上一次缓存的父状态或 `processed_at`。

临时游标按原查询顺序处理过的连续前缀推进：明确成功、无错误零行或已核验不适用的候选可以越过；单条写入技术失败或提交不明也可以越过该已尝试项，但立即结束本次运行，不处理下一条或立即重做。后续运行继续后方，整趟到尾后，经过运行后等待，以新扫描时间从头覆盖仍符合条件的失败项；失败项可能因此较晚再处理，前移不证明提交成功。候选查询、取时或小批读取失败时，未处理项不越过；预算到期也不能跳过尚未处理的候选。页面读入内存不是已处理证据。

预算用完时保留原趟扫描上界和最后已处理位置，下次运行取得新的 5 秒预算继续；到尾不在同次运行立刻从头重扫。不使用 OFFSET、持久游标或长期数据库快照，节点退出后从头发现。查询、关联过滤及状态变化可能使页间结果变化，后续新一趟负责再次覆盖，实际写入仍以原候选尝试和当前行条件裁决。

已接纳的业务结果保留、RUNNING 的半量补充门槛及 STOPPING 的 NOWAIT 都不适用于本分支。条件 UPDATE 本身仍可能等待 Task 行锁，不能把没有显式锁描述成立即跳过；超时不证明数据库或清理已经退出，实际退出后才结束本次流程并开始间隔等待。完整运行规则与故障取舍见[观察到期扫描与结算安排](execution.md#unbound-recovery-defaults)。

#### 精确部分索引及维护成本

按当前尝试登记时间和 ID 排序，仅索引执行中且完全未绑定的 Task：

```sql
CREATE INDEX idx_job_task_unbound_observation
ON job_task (attempt_registered_at, id)
WHERE status = 2
  AND executor_instance_id IS NULL
  AND executor_address IS NULL;
```

观察时间阈值和父记录条件留在查询中，不写入索引谓词；父 Instance 通过主键关联，不附加摘要、JSONB 或大字段覆盖。父条件过滤可能使实际访问量超过 LIMIT 返回量，仍须验证执行计划，不能把有界返回等同于固定数据库工作量。

接受这个精确索引的写入维护代价：正常 Task 登记后也会短暂进入索引，保存绑定或离开执行中后，新行版本不再满足谓词；再次登记并清空绑定后又会满足。PostgreSQL 的旧行版本及旧索引项由后续清理机制回收，包括 VACUUM，不是每次更新都立即物理删除索引项，也不是重建整个索引。谓词中的绑定字段会限制相关更新采用 HOT 优化，需计入索引写入、WAL 和清理成本。

本轮选择用上述维护成本换取恢复时直接避开已正常绑定的运行中 Task，而不是只建较宽的执行中索引再过滤绑定。索引候选少不代表维护成本低，也不据此承诺整体吞吐更高；实施压测须覆盖正常高频派发、绑定更新、重试及较多长时间运行任务，对比写入代价和扫描收益。索引尚未创建，查询与压力测试尚未实现；30 秒观察期及上述扫描／结算初值已通过，不承诺准时结算或严格跨节点公平。见[专项验收要求](verification.md#unbound-recovery-query-checks)。

### 普通 Task 绑定缺失的未知结果结算 {#ordinary-unknown-settlement}

**本节候选筛选、观察期条件、预算分支、并发裁决及故障释放规则已通过。** 范围是普通 Task 的当前尝试已经登记，但执行器绑定与有效结果均未落库时，结束该次结果不明的尝试。已有绑定的执行仍按[等待整轮终止](execution.md#bound-result-wait)处理，不适用本节；结算不证明旧业务未执行或已经停止，也不直接授权重新派发。

#### 有界候选与数据库时间

复用已有有界恢复检查，通过关联或批量读取取得 Task、所属 Instance 及本轮不可变配置，不逐条补查父实例，不读取当前 Job 改变本轮预算。候选保留 `TaskID`、`InstanceID`、发现时的原执行编号及用于分页的原登记时间，不新增等待对象、长期缓存、专用队列或扫描器。按[已定运行安排](execution.md#unbound-recovery-defaults)，每节点一个后台流程串行结算，复核短批最多 16 条，各 Task 独立提交，不跨整批持有已写行的锁。

每个短批次取得一次数据库当前时间作为 `processed_at`，据此计算 `observation_cutoff = processed_at - 观察期` 和 `retry_at = processed_at + 本轮重试间隔`。观察从当前 `attempt_registered_at` 起算，换节点、重复扫描或绑定保存重试均不重置起点。[派发时间预算](execution.md#dispatch-time-budgets)已确定默认 30 秒，与 10 秒总派发处理预算、3 秒接单等待及 4 秒响应保存阶段配套；登记时间在事务内取得，观察已包含提交耗时，不能仅按 RPC 和保存时间相加解释。各节点保持一致，调整派发或保存预算时联动检查观察期；余量不能证明旧业务已停止，也不是业务执行超时或恢复完成 SLA。

前置读取须观察到 Instance 为运行中、运行时间有效，且 `processed_at` 严格早于整轮截止。STOPPING 则接续子取消、终态则退出，仍运行且已到期则转入[完成与超时判定](#instance-completion-contract)，不凭当前未知候选直接写父超时；父仍待开始或时间异常则跳过并诊断，空截止不能解释成不限时。该前置判断不锁父，读取后的终止竞争按下节边界处理。

#### 单条条件更新与两个预算分支

以下为已确认条件及字段转换的 SQL 表达；实现保持 GORM 条件更新与显式字段映射，表达式仍使用当前数据库行的已用次数，不先查询再无条件覆盖。`max_attempts` 来自本轮快照的 `1 + max_retries`；`unknown_summary` 表达新的“执行结果不确定”平台诊断，不能写成已经确认业务失败。具体原因码和诊断模板随错误契约闭合，不在这里固定字符串。

```sql
UPDATE job_task
SET status = CASE WHEN used_attempts < :max_attempts THEN 3 ELSE 5 END,
    result_data = NULL,
    result_summary = :unknown_summary,
    next_dispatch_at = CASE WHEN used_attempts < :max_attempts
                            THEN CAST(:retry_at AS TIMESTAMPTZ) ELSE NULL END,
    finished_at = CASE WHEN used_attempts < :max_attempts
                       THEN NULL ELSE CAST(:processed_at AS TIMESTAMPTZ) END,
    end_reason = CASE WHEN used_attempts < :max_attempts
                      THEN NULL ELSE :unknown_reason END,
    updated_at = :updated_at
WHERE id = :task_id
  AND instance_id = :instance_id
  AND task_type = 1
  AND attempt_no = :expected_attempt_no
  AND status = 2
  AND executor_instance_id IS NULL
  AND executor_address IS NULL
  AND attempt_registered_at <= :observation_cutoff
RETURNING id, instance_id, attempt_no, status;
```

两个绑定字段必须都为空；单边非空的异常数据不按完全未绑定处理。候选执行编号不能替换成新编号，观察条件必须在 UPDATE 中保留，不能只依赖扫描结果。有效结果和状态原子保存，生效后必然离开 `status=2`，因此不额外以 `result_data` 或 `result_summary` 是否为空判断本次是否已有结果；旧摘要非空不能阻止结算。

| 当前行预算 | 状态和时间 | 原因与结果 |
| --- | --- | --- |
| `used_attempts < max_attempts` | `status=3` 等待重试，`next_dispatch_at=retry_at`，`finished_at=NULL` | Data 清空，摘要更新为结果不确定诊断，`end_reason=NULL` |
| 执行机会耗尽 | `status=5` 失败，`next_dispatch_at=NULL`，`finished_at=processed_at` | Data 清空，摘要更新为结果不确定诊断，`end_reason` 为结果不确定的终态原因 |

两分支都不增加或返还已用次数，不改变执行编号、首次开始及当前登记时间，不修改父 Instance；`updated_at` 继续仅作审计。重试间隔从本批数据库处理时间计算，不从已过期的观察起点倒推，也不延长整轮截止；处理时间不等于精确 COMMIT 时间，不保证提交后再完整等待一个间隔。

重试开始保留最近说明，是因为重新派发没有新结果；本次未知结算产生了新的平台诊断，所以覆盖旧摘要。等待重试仅是候选，实际新执行仍须通过[普通重试登记](#ordinary-retry-registration)的容量、父行授权、时间及预算检查。

#### 与绑定、结果和终止的竞争

未知结算不增加父行或 Task 显式锁，使用短事务及 Task 条件更新裁决，不在事务内发送 RPC。观察期到期只赋予尝试结算的资格，状态更新有效提交后才关闭原尝试。

| 先有效提交的操作 | 后续未知结算或迟到消息 |
| --- | --- |
| 保存 ACK 执行器绑定 | 双绑定为空的条件不成立，未知结算零行；继续走已有绑定的结果等待路径 |
| 保存当前尝试的有效结果 | 状态已离开执行中，未知结算零行 |
| 未知结算 | 原尝试进入等待重试或最终失败；即使编号未递增，旧 ACK 和结果仍因状态不允许而不能写入 |
| 子 Task 取消 | 状态不再为执行中，未知结算不能覆盖取消 |
| 另一恢复节点已经结算 | 重复候选更新零行，不刷新重试时间、不重复结算 |

结果仅在调度端入队或已经回复接收 ACK，不算有效结果落库；未知结算若先提交，后台处理该结果时仍拒绝覆盖。内存到达顺序不取代持久化裁决。

不锁父意味着前置读取后，父可能先停止或超时，子仍可能交错结算成等待重试或最终失败。这不撤销父停止决定，不启动正常 Post，也不构成严格的父子原子终止；未终态子继续取消收敛，已有子终态按既定规则保留。下一次新尝试必须锁父检查，不能利用交错产生的重试候选恢复执行。已经观察到父终止或到期时，则不继续本项结算。

#### 提交确认与临时候选释放

该条 Task 本次实际更新且其独立事务明确提交成功后，才向所属 Instance 提示检查当前数据库状态，不在结算流程直接发送 Task。复核批中的其他 Task 分别提交，一条失败不回滚此前已明确成功的条目；零行结束该候选处理并可继续下一条，不补查猜测原因，也不宣称已结算成功。

单条写入数据库失败或提交结果不明时，结束本次运行，释放临时候选，不立即重做写入。该已尝试项可推进本趟临时游标，下一次运行续接后方，后续新一趟从头覆盖仍合格的失败项；取时、候选或小批关联读取失败则不越过未处理项。不能把释放候选或前移解释为已经回滚或提交成功：实际未提交且仍符合条件的以后再竞争；实际已提交的已改变状态，由待重试或完成推进恢复接续。失败项可能要等整趟结束后才再次处理。确认成功后提示丢失也由共同恢复补上，完整分页规则见[候选运行边界](#unbound-recovery-query)。

此处没有尚未保存的真实业务结果，判断依据原本就在数据库，因此不套用[已 ACK 业务结果的有界保留](execution.md#scheduler-result-retention)，也不新增长期等待缓存。原业务可能仍在运行或已经成功，平台仍可能按预算重试或以结果不确定失败；平台状态不能证明业务效果，重复副作用继续由业务幂等性负责。

对应[未知结算验收](verification.md#ordinary-unknown-settlement-checks)尚待实现验证。[观察到期候选查询及索引](#unbound-recovery-query)、默认 30 秒[派发观察期](execution.md#dispatch-time-budgets)及[扫描／结算安排](execution.md#unbound-recovery-defaults)已通过，原因码、具体实现及其他类型的写入继续闭合；[普通 BUSY 返还 SQL](#ordinary-busy-settlement)、[停止超时协议](#instance-termination-sql)及[正常完成／超时衔接](#instance-completion-contract)已定，不把本项批准扩大为执行模块已经完成。

### 普通 Task 明确 BUSY 的条件返还与延期 {#ordinary-busy-settlement}

**本节响应依据、条件更新、并发裁决及有限保存重试已通过。** 范围是普通 Task 的原派发调用收到专用 BUSY 响应，明确表示执行器容量已满、本次未接单、未入队且未执行业务。RPC 超时、断连、通用不可用或资源耗尽错误、其他明确拒绝及丢失的答复均不构成返还依据；不得把通用错误推断成该协议响应。

#### 处理归属与单条条件更新

由收到响应的当前固定派发工作协程直接保存，使用原发送请求的 `TaskID`、`InstanceID` 和 `attempt_no`；不新建响应队列、待返还缓存或专用扫描器，不重新读取父 Instance 或显式锁父。以下为已确认条件与字段转换的 SQL 表达，实现仍保持 GORM 条件更新与显式字段映射，不先查次数再无条件覆盖。

```sql
UPDATE job_task
SET status = 3,
    used_attempts = used_attempts - 1,
    next_dispatch_at =
        clock_timestamp() + make_interval(secs => :busy_delay_seconds),
    result_data = NULL,
    result_summary = :busy_summary,
    finished_at = NULL,
    end_reason = NULL,
    updated_at = :updated_at
WHERE id = :task_id
  AND instance_id = :instance_id
  AND task_type = 1
  AND attempt_no = :sent_attempt_no
  AND status = 2
  AND used_attempts > 0
  AND executor_instance_id IS NULL
  AND executor_address IS NULL
RETURNING id, instance_id, attempt_no, status;
```

一次更新将当前尝试关闭为等待重试、返还一次已用机会并设置延期；`used_attempts > 0` 防止减成负数。两项绑定都为空才允许返还：已保存接单成功的绑定，或只有一项非空的异常数据，都不能清空后强行按 BUSY 处理。有效结果原子改变执行中状态，因此不以 Data 或摘要是否为空判断是否可以返还。

不回退 `attempt_no`，不改变首次开始、当前尝试登记时间及父 Instance 的状态和截止。`result_summary` 更新为本次“容量已满、未接纳”的平台诊断，具体英文模板随错误契约落实；不是已确认的业务执行失败。后续登记保留这条最近说明，直到有新的结果或诊断覆盖。

延期秒数必须为正，[派发时间预算](execution.md#dispatch-time-budgets)已确定默认 5 秒、上下浮动 20%，即初值下约 4～6 秒；不复用业务失败默认 10 秒的重试配置，不增加 BUSY 次数状态。表达式继续使用 UPDATE 求值时的数据库时间和实际延期参数，不另查一次时间；它不是精确提交时间，也不保证在所有锁等待之后求值或 COMMIT 后再等待完整间隔。延期不延长整轮截止。保存后结束本次派发处理，由已定一秒补查发现到期工作，不占住派发工作协程等待五秒后重发。

#### 重复响应与终止竞争

| 先有效提交的操作 | 后续处理 |
| --- | --- |
| 本次 BUSY 返还 | 重复处理因状态已离开执行中而更新零行，不再减次数或刷新延期；旧 ACK 和结果也不能恢复该尝试 |
| 当前尝试的有效结果、未知结算或子 Task 取消 | BUSY 更新零行，即使执行编号尚未变化也不能补返 |
| 接单成功的绑定 | BUSY 因绑定条件不符而更新零行，不覆盖已接单事实 |
| 后续新尝试登记 | 旧响应编号不符，不影响新尝试的次数、状态或延期 |

观察期到点本身不关闭尝试，不增加“仍在观察期内”的条件；未知结算尚未提交时，真实 BUSY 仍可竞争生效。更新零行结束本次处理，不补查猜原因、不宣称本次已返还，也不换用最新编号继续处理。

返还不统一锁父，父 Instance 为 STOPPING 而子 Task 仍执行中时，允许子交错进入等待重试；这不恢复父状态或授予新执行许可，未终态子仍由停止处理及恢复取消收敛。真正再次派发必须经过[普通重试登记](#ordinary-retry-registration)的容量、父行授权、截止及预算检查。零重试也可重新占用被返还的首次机会，例如 `(attempt_no, used_attempts)` 从 `(1,1)` 经 BUSY 变为 `(1,0)`，再次登记为 `(2,1)`；不复用旧编号。

#### 提交确认与响应释放

只有实际更新且明确提交成功，才可发出已有的 Instance 检查提示；提示不是立即重派指令，所有派发入口仍遵守 `next_dispatch_at`。确认前不能凭 RETURNING 记录宣布返还成功；不在数据库事务内发送 RPC。提交后提示丢失由已有待处理检查补上。

可恢复数据库故障或提交结果不明时，只在当前工作协程内有限重试**同一原响应的条件保存**，不重发 `DispatchTask`，不单独重试减次数。前次尚未生效时，再次执行可按本次数据库时间计算延期；前次已经生效时，状态条件阻止重复返还和刷新延期。[派发时间预算](execution.md#dispatch-time-budgets)规定：从开始处理响应起，保存阶段默认最多 4 秒，最多 3 次含首次，每次数据库操作最多 1 秒、失败后间隔 200 毫秒；连接及执行等待计入单次保存，间隔计入阶段和总预算，并受从取出候选起的 10 秒剩余总预算及共享退出宽限约束，不保证尝试满三次。

保存 Context 独立于已结束的接单 RPC，父进入 STOPPING 时仍按原条件写入，不能恢复父状态或授权新执行。耗尽后不启动新保存操作，尚未实际退出的操作不能被当成已释放其占用的并发能力。

零行、明确成功、保存重试耗尽或退出后释放原响应，工作协程不无限等待。原响应释放或丢失后，恢复检查不能从数据库凭空补出 BUSY 证据；取消或未知结算已先提交时也不能补返。已提交的返还保持有效，未保存的拒绝可能仍消耗一次执行机会，即使业务实际没有运行。此处不套用[已 ACK 业务结果的有界保留](execution.md#scheduler-result-retention)。

对应[普通 BUSY 专项验收](verification.md#ordinary-busy-settlement-checks)及[派发时间预算验收](verification.md#dispatch-time-budget-checks)尚待实现验证。[STOPPING 与子取消 SQL](#instance-termination-sql)已替换原父先终态方案，[正常完成父 UPDATE 与超时衔接](#instance-completion-contract)已通过；BUSY 延期与有限保存重试初值已定，其余查询与容量继续设计，不增加新表、字段、队列或自动换节点规则。

<a id="instance-due-recovery-query"></a>

### RUNNING Instance 统一复核候选查询 {#instance-recheck-query}

**2026-10-22 完整提案已通过，查询及索引尚未实现。** 本节替代原父 Instance 独立到期查询及 `(deadline_at, id) WHERE status = 2` 待建索引方案，将遗漏阶段推进、正常完成及超时发现合为一条候选路径；不是删除 `deadline_at`，也不改变截止授权和终态裁决。旧章节锚点仅保留链接兼容，不代表仍有独立到期扫描。

仅 Leader 执行该候选发现，交给本节点已有 Instance 检查组件，不向其他节点转发或要求正常结果通过 Leader 推进。每 2 分钟开始或续接有界扫描；新 Leader 就绪后立即开始一趟。每趟先单独读取数据库当前时间 `scan_now`，固定 `eligible_before = scan_now - 4 分钟`，后续页及跨周期续扫不刷新本趟上界。第一页：

```sql
SELECT id, started_at
FROM job_instance
WHERE status = 2
  AND started_at < :eligible_before
ORDER BY started_at, id
LIMIT :page_size;
```

后续页增加最后已接纳位置的键集条件，不使用 OFFSET：

```sql
SELECT id, started_at
FROM job_instance
WHERE status = 2
  AND started_at < :eligible_before
  AND (started_at, id) > (:last_started_at, :last_id)
ORDER BY started_at, id
LIMIT :page_size;
```

`:page_size` 部署初值为 **256**。返回 `started_at` 只为分页；查询不读取整份配置或结果，不加锁、不写状态、不关联当前 Job，不在候选 SQL 查询或聚合 Task。`started_at` 是首次有效开始时保存的业务时间，不随重试或阶段变化刷新；不用 `created_at`、`updated_at` 等审计字段替代。适用查询索引为：

```sql
CREATE INDEX idx_job_instance_running_started
ON job_instance (started_at, id)
WHERE status = 2;
```

候选 SQL 只有开始时间范围及同序分页条件，不增加独立的截止时间范围；未到期和已到期的 RUNNING 父记录都适用。短于 4 分钟的整轮期限不例外，刚开始但已经到期的父记录仍等待满足开始时间门槛。`started_at` 为 NULL 的异常记录不能由本查询推断已开始，更不能据此直接授权重派。

索引只覆盖运行中记录，不纳入全部终态历史，但仍有状态变更时的维护、WAL 及旧版本清理成本。实施时验证活跃量、长任务比例、积压量与历史量下的执行计划；LIMIT 只限制返回行数，不能据此承诺固定访问量或数据库耗时。原截止索引尚未迁移，本次只是替换设计，不执行删除或创建索引。

#### 临时游标与并发可见性

候选成功进入本地检查组件或与已有提示合并后才推进游标，不等实际检查结束。达到恢复补充门槛或集合已满时，不越过首个未接纳项，暂停新增查询，在本周期剩余预算内等待集合释放空间再继续；预算结束后保留本趟上界及最后接纳位置，下周期续扫，不重叠开启新一趟。查询失败直接结束本周期，下周期从已接纳位置重新读取；不在内存保留整个候选集合，也不因读入本页就推进游标。

[扫描预算与恢复补充](execution.md#instance-recheck-defaults)已于 2026-10-29 通过：单次查询默认 1 秒（含连接等待），每周期扫描／交接默认 5 秒（含取时、查询、交接及等待），两项部署可调，单次受剩余预算限制。续扫每周期取得新的 5 秒，沿用本趟固定 `scan_now`。恢复新增 ID 仅在检查集合占用量低于总量一半时接纳（默认门槛 256，极小容量至少 1）；已有 ID 可合并，不增加位置。门槛不加到候选 SQL，不按来源计数，也不是周期总量上限；每页最多 256 条仍只约束有界页数据。正常提示可用全部 512，检查仍共用 8 并发及独立的单次 2 秒。

预算内等待替代 RUNNING 队满立即结束本周期的旧规则，等待不占数据库连接或事务、不继续查询后续页、不创建无界旁路。预算到期取消在途查询并停止新交接，已接纳检查不随扫描取消；取消不证明数据库及清理已退出。持续或覆盖完整扫描窗口的拥塞仍可能阻止恢复准入，不能由共享 FIFO、临时游标或可配置预算推导严格公平。STOPPING 的队满退出规则不因此改变。

到末尾后结束本趟，不在本周期剩余时间内从头重扫；后续周期开启新一趟时重新取时间并从头查。键集分页不提供跨页快照，也不构成永久消费水位：较早开始但迟提交到游标之前的记录、已接纳但处理失败的记录、仍健康运行的长任务都要在后续新一趟重新覆盖。失主取消发现并丢弃游标，新 Leader 不继承旧位置，接受重复发现。以上只用于 RUNNING Instance 复核，不改变 [Job 单批触发恢复](#trigger-recovery-scan-proposal)的不分页规则。

#### 候选不是状态结论

实际处理重新读取父状态、本轮配置、必要 Task 事实及当前数据库时间，不使用旧 `scan_now` 作授权或超时裁决。父已终态退出，STOPPING 转停止处理；只有父仍 RUNNING 且完整最终结果具备，才尝试按结果正常结束，允许截止后收尾；仅明确 `NotReady` 且已到期才进入原父主键单行短事务，锁后复核父状态和数据库时间。查询／写入错误、正常完成 UPDATE 零行及提交不明均不能改判超时。

未到期且具备后续阶段条件时，进入已有生成／Post 等授权协议；仍在正常等待 Task 则退出。候选本身不直接创建业务工作，截止后不创建分片、Post 或登记新尝试；已绑定无结果不提前重派。STOPPING 子收敛及尽力取消走独立分支，不并入 RUNNING 候选 SQL。

正常结果仍在提交节点立即提示本地检查；统一复核只补漏，不保证正常路径绝对优先。无积压时首次周期检查约为开始后 4～6 分钟，短超时亦受门槛限制；没有有效 Leader、数据库故障或集合拥塞可进一步延迟平台终态，恢复补充规则也不保证持续拥塞下必有进展，不延长执行器的原期限。完整生命周期与正常长任务复查成本见[执行规则](execution.md#instance-recheck-direction)，预算及交接见[恢复补充规则](execution.md#instance-recheck-defaults)，验收见[统一复核清单](verification.md#instance-recheck-query-checks)。本项仅记录设计，未运行功能、数据库或压力测试。

### Instance 完成与超时裁决契约 {#instance-completion-contract}

**本节完成条件、父条件 UPDATE、结束时间与摘要规则，以及自动超时衔接已通过；尚未实现。** 普通与分片共用最终状态写入机制，按各自阶段判断完成事实，不各建一套并发控制。分片生成封口字段、生成事务和完整分片查询 SQL／索引仍须在相应议题闭合；本节通过不表示这些依赖已经完成，也不授权执行模块编码。

- 正常完成先在父锁外读取最少的稳定完成事实，具备整轮最终结束条件时，条件更新仍运行中的 Instance。即使检查或有效结果保存发生在截止后，只要父仍 RUNNING，仍允许按结果结束；STOPPING 不能走正常完成；不保证严格按截止时刻裁决结果。UPDATE 本身仍有行锁及可能的锁等待，不是无锁方案。
- 普通与分片共用完成判定，不能只凭“当前查到的 Task 全部终态”结束。按适用阶段确认分片生成完整封口、Task 最终态不可逆、等待重试不算终态，以及本轮应执行的 Post 已满足最终结果要求；零分片有明确生成完成事实，空查询不构成完成证据。准备最终失败仍按既有规则直接结束，不要求生成分片或执行 Post；阶段差异不增加另一套普通任务并发协议。
- 自动超时先复用“仅终态判定”：父已终态则退出，STOPPING 接续停止处理；父仍 RUNNING 且已具备完整结果则尝试按结果结束，明确尚不具备且有效截止已到才尝试保存超时停止决定。该判定不创建分片、Post 或新重试；查询／写入技术失败不能解释为“结果未齐”而改判超时。错误本身不自行产生无限重查，检查期间真实的新提示仍须保留；否则由后续通知或既定 Leader 复核接续，按[检查提示生命周期](execution.md#instance-check-defaults)处理。
- 人工停止针对 WAITING／RUNNING Instance 保存 STOPPING 及人工停止原因；超过截止本身不把人工停止转换为超时。已有 STOPPING 决定、父终态及原因均不可覆盖。不同节点的正常完成、停止和超时仍会竞争，以条件更新后实际提交的状态为准，不承诺全局完成优先。
- 创建后续工作、登记新尝试和授权重试／Post，仍保留父短事务内的状态和截止检查。Post 已完成可恢复收尾，Post 未启动则不得在截止后创建或授权；放宽平台收尾时机不延长业务预算。
- RUNNING 有界复核继续覆盖到期未结束、结果齐备但漏推进；新增独立 STOPPING 父候选分支覆盖停止决定提交后的遗漏子取消或父最终结束，不依赖下一次 Cron 或 Job 仍启用。不新增时间轮、500ms 宽限或专用恢复组件。

#### 按本轮阶段确认最终结果

读取 Instance 当前状态及保存的 `execution_config`，先按父状态分流，再解读仍需使用的配置，已有父终态即退出；正常完成仅处理运行中父记录，不使用编辑后的 Job 配置。结果保存与父推进职责分开：前者按 Task 类型、当前尝试与允许旧状态保存有效结果，特殊阶段仍有自己的数据校验；后者在结果提交后按本轮快照及阶段事实决定下一步，不要求每条结果回报都重新查询 Job 或汇总整轮。

| 场景 | 正常结束所需事实 | 父结果 |
| --- | --- | --- |
| 普通执行 | 唯一普通 Task 已成功或最终失败 | 跟随该 Task 最终结果，不组合 Pre／Post |
| Pre 最终失败 | 准备 Task 已最终失败 | 失败，不要求生成分片或执行 Post |
| 分片，未配置 Post | 生成已完整封口，所有分片已成功或最终失败 | 全成功则成功，存在最终失败则失败 |
| 分片，配置了 Post | 上述分片条件满足，唯一 PostTask 也已成功或最终失败 | 分片及 Post 全成功才成功，否则失败 |
| 合法零分片 | 已明确保存零分片且生成完成；配置了 Post 时仍需其最终结果 | 未配置 Post 可成功，配置了 Post 则由其最终结果决定 |

等待重试不是最终失败，取消也不是正常成功证据，不能只检查“当前查到的 Task 都是终态”。是否需要 Post 由本轮 `execution_config.post_enabled` 决定，不能根据 PostTask 是否存在反推配置。配置了 Post 但记录不存在，表示所需阶段尚未创建，不能直接结束父 Instance。

分片全部结束时，包括全部分片最终失败，正常推进仍在父运行、尚未到期的前提下按[收尾创建协议](execution.md#post-creation)创建唯一 PostTask；创建和执行业务 Post 期间父保持运行中。Post 最终结果保存后再检查整轮；Post 成功不能抹除分片最终失败，Post 最终失败也不重跑分片。结果提交及分片生成完成会提示检查，丢失提示或已创建未派发由已有恢复接续，不依赖父先变成终态触发 Post。

查询按当前阶段读取最少事实：普通采用[一次 LEFT JOIN 同快照读取](#ordinary-completion-query)，Pre、Post 读取对应单条 Task；分片先确认生成完整，再用适配索引的存在性查询检查待执行、执行中、等待重试项，只有具备结束条件时才计算用于父终态摘要的最终统计。Post 输入所需的分片统计仍在相应收尾推进阶段读取，不要求先结束父 Instance。不能逐条结果加载整轮 Task 列表，也不为每条回报更新父完成计数；`EXISTS` 不保证必然避免大量扫描，执行计划随查询索引设计验证。

父锁外读取依赖以下稳定性前提（普通模式使用下述同语句快照，不要求分步查询）：分片生成结束后集合不再增长，Task 成功与最终失败不可回退，活跃 Instance 的 Task 不被历史清理删除。已经确认完整的最终结果不会因正常推进重新变成未完成；父状态仍可能被并发停止或超时改变，由后述条件更新保护。继续使用 PostgreSQL 默认 `READ COMMITTED`，不为本项提高隔离级别或增加父版本号、分布式锁。生成封口的具体字段与原子写入协议仍待设计，不能用当前不存在的字段编造可执行查询。

#### 普通 Task 完成读取与异常处理 {#ordinary-completion-query}

**本项于 2026-11-02 通过，尚未实现验证。** 普通模式用一次关联查询读取父状态、本轮冻结配置、原截止及唯一普通 Task 的完成事实；父状态优先指解释顺序，不要求先后执行两条 SELECT。

```sql
SELECT i.id,
       i.status AS instance_status,
       i.execution_config,
       i.deadline_at,
       t.id AS task_id,
       t.status AS task_status,
       t.result_summary
FROM job_instance AS i
LEFT JOIN job_task AS t
       ON t.instance_id = i.id
      AND t.task_type = 1
WHERE i.id = :instance_id;
```

按 Instance 主键及既有 `uq_job_task_singleton (instance_id, task_type)` 部分唯一索引定位，不新增索引、不加载业务 `result_data` 或整轮 Task 列表。`task_type = 1` 必须放在 JOIN 条件中，不能移到 WHERE 排除没有普通 Task 的父行；关联 Task 字段保留可空表达，使用可空 `task_id` 区分子记录缺失与子字段自身为空。SQL 表达查询语义，实现沿用 GORM 的明确列投影和 JOIN，执行计划在实施时验证。

父与子来自 PostgreSQL 默认 READ COMMITTED 下同一语句快照，避免先查父、后查子期间发生状态变化造成的读取交错，省去第二次查询；不增加显式行锁。读完之后仍可能发生并发停止或完成，继续由后述父条件 UPDATE 保护，不把同快照解释为读写整体原子。

| 读取事实 | 本次处理 |
| --- | --- |
| 父不存在或已经终态 | 结束本次检查，不因关联子行缺失或不再需要解析的快照触发修复 |
| 父 WAITING | 不走正常完成，由既有首次启动路径处理；尚未开始不判执行超时 |
| 父 STOPPING | 转既有停止收敛，不用子结果覆盖停止决定 |
| 父 RUNNING，本轮为普通模式，普通 Task 成功或最终失败 | 按既有正常完成条件写父，复制最终 Task 摘要；合法空摘要保持 |
| 普通 Task 待执行、执行中或等待重试 | 明确尚未完成；到期时可进入既有超时裁决，不新建恢复分支 |
| 父 RUNNING 且普通 Task 缺失 | 诊断数据异常，不补建、不宣称正常完成；作为已成功读取但不具备完成条件的事实，允许到原截止后按既有超时路径结束 |
| 同一查询读到父 RUNNING 且普通 Task 为 CANCELED | 诊断异常组合，不当作正常成功或失败；同样允许原截止后的既有超时处理，不额外重读父行猜测读取交错 |
| 数据库查询失败、仍需使用的冻结配置不可解读，或遇到未知父／子状态 | 返回错误，不伪装成 NotReady，不据此推断超时；沿用已有检查提示及复核规则 |

先按父状态分流，再解释仍需使用的冻结配置和子状态；只有确认本轮是普通模式，才应用普通 Task 缺失或取消的异常规则。分片模式仍按其阶段协议判断，不能因 JOIN 没有普通 Task 就认定分片轮次异常。取消从来不是正常完成证据；父 STOPPING 的合法取消属于停止流程。

普通首次开始与唯一 Task 创建原子提交，因此 RUNNING 缺少普通 Task 是数据异常，不是正常的尚未生成窗口。接受的取舍是：若成功 Task 被错误删除，平台无法还原其成功事实，之后可能按当前数据库事实超时结束；不为此重跑业务、补造结果或增加新的扫描器。活跃轮次的 Task 仍不得被历史清理删除。

本项仅闭合普通模式的完成读取与异常分类，不替代分片生成封口及完整分片查询设计，也不改变已定完成判定、超时锁内复核、处理预算或零行语义。对应[专项验收](verification.md#ordinary-completion-query-checks)与[完成判定验收](verification.md#instance-completion-checks)共同实施。

#### 正常完成的父条件更新

在父锁外确认完整最终结果、计算目标状态及必要摘要后，执行下列语义的单条更新；不先显式锁父再查询整轮 Task。实现继续遵守 GORM 数据访问规范，SQL 用于审查写入条件，不要求一律使用 Raw／Exec。

```sql
UPDATE job_instance
SET status = :target_status,
    finished_at = clock_timestamp(),
    end_reason = :end_reason,
    result_summary = :result_summary,
    updated_at = :updated_at
WHERE id = :instance_id
  AND status = 2;
```

`2` 表示运行中，`target_status` 只能是成功 `3` 或失败 `4`。不增加 `deadline_at` 条件，不改本轮开始、截止或配置；此前读取的稳定完成事实是执行此 UPDATE 的必要前提，不能仅凭父仍运行就直接写入成功。UPDATE 自身取得行锁，与新工作授权、正常完成、人工停止及超时写入竞争，不称无锁或保证无等待。

| 数据库结果 | 本次处理 |
| --- | --- |
| 更新一行且明确提交成功 | 本次完成生效，才可执行本次成功完成后的动作 |
| 更新零行 | 没有完成本次状态转换，结束本次尝试；不覆盖其他终态，不重复执行完成后动作，也不转而尝试超时 |
| 查询或写入错误、提交结果不明 | 不宣称完成或确定回滚；保留检查期间的新提示并按本地合并规则再查，没有新提示则由后续通知或既定 Leader 复核重新读取事实，不因错误自行循环，也不重新执行业务 |

完成事实查询失败与明确未具备条件必须分开返回，不能压成同一个布尔结果；更新零行或提交不明也不是“结果未齐”。后续检查或恢复再次保存时仍须遵守条件更新，不能为保证成功而退回无条件写入。待开始的父状态不是运行中，不走正常完成 UPDATE。本地按 Instance 合并、检查期间新提示保留、每节点 512 个唯一 ID／8 并发／单次 2 秒及 100ms 窗口按[检查提示生命周期与初值](execution.md#instance-check-defaults)执行；这不扩大事务、提前确认父终态或改变阶段授权条件。

#### 稳定结束原因 {#execution-end-reasons}

**以下六个持久化原因值于 2026-11-02 通过。** Task 原因解释该工作项如何最终结束，Instance 原因解释整轮如何结束；两者不机械复制，也不等同于 RPC 错误码或业务 subcode。

| 稳定值 | Task 适用情形 | Instance 适用情形 |
| --- | --- | --- |
| `success` | Task 最终成功 | 整轮正常成功 |
| `execution_failed` | 明确业务失败且本轮允许的执行机会已耗尽 | 按完整最终结果正常结束为失败 |
| `result_unknown` | 无绑定观察到期按结果不确定结算，且执行机会已耗尽 | 不直接使用 |
| `dispatch_rejected` | 可识别的非 BUSY 接单拒绝，且执行机会已耗尽 | 不直接使用 |
| `manual_stop` | 因父人工停止决定被取消 | 人工停止；在 STOPPING 时冻结，终态沿用 |
| `instance_timeout` | 因父整轮超时决定被取消 | 整轮超时；在 STOPPING 时冻结，最终失败沿用 |

正常完成的父失败统一保存 `execution_failed`，即使普通 Task 最终原因是 `result_unknown` 或 `dispatch_rejected`；具体原因保留在 Task 及父结果摘要，不因先收到哪条失败而改变父原因。该规则不把接单拒绝或结果不确定描述为业务处理器已经执行失败。

Task 尚未结束时 `end_reason` 为 NULL，包括等待重试；本次业务失败、明确拒绝或无绑定观察结算后仍有执行机会，不能提前写最终原因。停止只取消仍未结束的子记录，已成功或最终失败 Task 保留自身原因；被取消记录按父已冻结的 `manual_stop` 或 `instance_timeout` 保存原因。重复停止、迟到结果和远端取消成败不能改写父停止决定。

业务 Context 到期、接单／回报／取消 RPC 超时，以及执行器接单前判断原期限已过，都不能直接写成 `instance_timeout`；该值必须来自调度端依既有协议保存的父超时决定。机器码符合 `VARCHAR(64)` 容量，诊断使用英文摘要或日志，不解析诊断文案作控制判断。本字典确定现有结束路径的值，不增设状态或改变执行机会、结果保存及停止顺序。

#### 结束时间与摘要

| 字段／场景 | 保存规则 |
| --- | --- |
| `finished_at` | 本条 UPDATE 中 `clock_timestamp()` 的表达式求值时间；不是业务实际退出、精确取得行锁或 COMMIT 时间，也不倒填为原截止 |
| `end_reason` | 正常成功为 `success`，正常失败统一为 `execution_failed`；人工停止、超时沿用已冻结的 `manual_stop`／`instance_timeout`，不复制子失败原因，详见[原因字典](#execution-end-reasons) |
| 普通执行、Pre 最终失败的 `result_summary` | 复制对应最终 Task 的 `result_summary`；合法空摘要同样保存，不用早前失败说明冒充最终摘要 |
| 分片执行的 `result_summary` | 平台生成分片总数、成功数、失败数和 Post 结果的摘要，未配置 Post 时明确不适用；不拼接全部子结果文本，具体错误保留在对应 Task |
| `updated_at` | 沿用现有审计更新方式，不作为完成、配置版本或执行期限的判断依据 |

例如十个分片中九个成功、一个最终失败而 Post 成功，父仍为失败，摘要保留分片统计及 Post 结果。分片和 Post 同时最终失败时，使用同一稳定的本轮执行失败原因，不能因节点先看到哪一条结果而得到不同机器原因。统计项固定不等于给 Task 结果增加长度限制；本项不新增摘要上限、截断规则、结果引用字段或历史表。

#### 自动超时与人工停止的衔接

自动超时先调用仅终态判定与保存，不在该检查中生成分片、Post 或新重试：

1. 父已有终态则退出，STOPPING 转停止处理；仍 RUNNING 且最终结果齐备才尝试正常完成，遵守上面的提交与零行处理。
2. 只有明确尚不具备最终完成条件，才进入既有父行短事务，按主键锁单条 Instance，重新读取父状态，并在取得锁后单独读取数据库当前时间。
3. 父仍运行且有效截止已到，才保存 STOPPING 及超时原因；已有 STOPPING／终态或尚未到期则不改原因、不写超时。待开始、尚未计时不能当作执行超时。

超时父事务内不再重新扫描全部 Task，父字段写入见[下节](#instance-termination-sql)。接受完成前置检查与父超时写入之间最后一条结果刚好提交、而超时路径先保存 STOPPING 的竞争窗口；已经保存的停止决定或终态不覆盖，不承诺全局完成优先。正常完成先提交时，超时事务看到父终态后退出。

人工停止不要求先汇总 Task；父仍 WAITING／RUNNING 时保存 STOPPING 和人工原因，即使已经超过截止也不改为超时。已有 STOPPING 原因不覆盖；停止决定确认后，子取消与执行器通知在父事务之外独立有界处理，最后再保存父终态。新的 Post 尚未创建或获准执行时，不因上述结果收尾规则允许迟到而在截止后启动；所需结果未齐只能按既定停止／超时规则结束。

上述契约及[专项验收](verification.md#instance-completion-checks)已确认、未实现验证。[普通完成读取](#ordinary-completion-query)及[稳定结束原因](#execution-end-reasons)已通过；完整分片生成／查询协议及其余未闭合参数继续设计，迁移与执行计划须在实现时验证；阶段推导不是新的权威状态字段，也不在此规定页面状态和阶段的拼接展示。

### Instance 停止超时与子 Task 收敛 {#instance-termination-sql}

**本节 STOPPING 停止决定、统一分批取消子 Task、最后保存父终态的方案已通过，替换原父先终态再收敛子 Task 的设计；尚未实现。** 新增 Instance 状态 `6`，复用 `end_reason` 冻结停止原因，不新增 pending 标记、停止命令表、可靠通知表、负责人或延迟队列。数据库取消与远端尽力取消相互独立；STOPPING 恢复明确增加父候选查询，不称“没有新增扫描”。自动超时先执行[完成判定与衔接](#instance-completion-contract)，只有明确尚不具备最终结果才进入本节父短事务，不能直接按旧候选执行超时写入。

#### 父 Instance 的短事务

人工停止以及经过仅终态判定后需要保存超时决定的路径，保留 PostgreSQL 默认隔离级别的父短事务：按主键等值锁住单条 Instance，与首次开始、每批实际 Task 插入、Post 创建及新尝试登记协调。取得行锁后，再单独读取一次 `clock_timestamp()` 作为 `decision_time`；不使用事务开始、候选扫描或执行器自报时间，不查询当前 Job 改变本轮截止。自动超时锁内仅复核父状态及时间，不重新扫描整轮 Task，接受期间结果到齐与停止决定写入的竞争。

| 当前状态及已确认条件 | 人工停止 | 自动超时检查 |
| --- | --- | --- |
| 已成功、失败或取消 | 保留原终态及结束字段 | 保留原终态及结束字段 |
| 已停止中 | 保留冻结原因，接续数据库停止处理 | 保留冻结原因，接续数据库停止处理 |
| 运行中，已经具备整轮最终结果 | 仍可竞争保存 STOPPING 与人工原因，不承诺完成优先 | 先尝试按结果结束，即使检查时已经到期 |
| 运行中，尚不具备最终结果，已经达到有效截止时间 | STOPPING，原因为人工停止 | STOPPING，原因为整轮超时 |
| 运行中，尚不具备最终结果，尚未到期 | STOPPING，原因为人工停止 | 不修改 |
| 待开始，尚未计时 | STOPPING，开始与截止仍为空 | 不修改 |

人工停止的原因由操作本身决定，不由是否过期改变；尚未开始计时的等待不能被当作执行超时。运行中的截止时间继续要求有效，不把异常空值解释为不限时。下列 SQL 只表达已经按新规则决定人工取消或超时后的父字段保存，不包含完整完成判定；自动超时不能跳过该前置判定直接执行，人工停止不以结果是否齐备为前提：

```sql
UPDATE job_instance
SET status = 6,
    finished_at = NULL,
    end_reason = :end_reason,
    result_summary = :terminal_summary,
    updated_at = :updated_at
WHERE id = :instance_id
  AND status = :expected_status
  AND status IN (1, 2);
```

`expected_status` 是本事务锁后读取的原状态；人工允许 `1/2`，自动超时只允许 `2` 且已按前置协议确认到期。决定转换后必须恰好更新一行，否则不能宣称停止决定保存成功。已有 STOPPING 或终态则保留原因，不刷新停止决定；自动前置检查可按结果结束，或尚未到期时，不执行此超时写入。正常完成继续仅更新 `status=2`，不能覆盖 STOPPING。结果是否齐备来自父事务外的前置检查，不在父锁内重新汇总 Task。

父摘要更新为本次人工停止或整轮超时的说明；原因使用稳定机器码并在此冻结，具体字符串随字典闭合。`finished_at` 保持 NULL，到最终结束时才设置，不倒填为 `decision_time` 或 `deadline_at`。开始、截止及本轮配置保持不变，审计字段仍按现有方式维护。

事务内只保存停止决定，不更新全轮子 Task，也不发送 RPC。管理端成功表示 STOPPING 决定已确认提交，不表示数据库停止处理完成或业务退出；已有 STOPPING／终态如实返回，不改原因、不声称本次取消成功。确认提交后立即提交本地有界处理，不能因队满或宕机丢失持久化义务。

每个实际写入新 Task 的批次都必须在同一个父主键单行短事务中校验父状态并插入；首次启动要求 WAITING，后续分片生成、Post 和尝试授权要求 RUNNING 及原截止有效。只在生成开始时检查一次、随后无保护插入是不允许的。先获授权且提交的 RPC 仍可能在停止决定之后到达，这属于尽力取消边界，不因此恢复父运行。不为停止补建尚不存在的分片或 Post。

#### 所有 Task 类型统一有界批量取消 {#task-cancel-query}

**2026-10-26 已通过单批竞争与容量修订，尚未实现。** 父 STOPPING 决定确认提交后，每个取消批次另开短事务，先按父主键尝试取得单行锁，再读取本批子 Task。此规则替换此前“子取消不重新锁父”的做法；不改变停止状态机、候选查询及索引，也不推广到其他派发路径。

```sql
SELECT id, status, end_reason
FROM job_instance
WHERE id = :instance_id
FOR UPDATE NOWAIT;
```

- 父行锁不能立即取得时，回滚本批事务，结束本次 Instance 处理，工作协程处理其他候选；不读取子 Task、不等待竞争者完成、不立即重排当前 Instance。处理期间积累的旧重复提示不能绕过这个退出规则。
- 取得锁后重新确认父仍为 STOPPING；已终态或不再适用时退出，不重写终态及结束时间。确认仍需处理后，才在同一事务内读取当前一批未结束 Task 并执行条件取消；不能使用锁外预取的旧子任务列表。
- 每批提交后释放锁；下一批重新开事务、取得父锁并重查。父锁只覆盖单批数据库操作，不覆盖整轮停止过程、整轮统计或 RPC；不同节点可以接续不同批次，不增加负责人、领取字段或租约。
- 锁顺序保持父→子。结果保存继续不取得父锁，必须在结果事务提交后再触发父检查，不能持有子行锁反向等待父锁。父 NOWAIT 竞争不等待行锁，但子条件 UPDATE 仍可能与结果、ACK 或 BUSY 写入竞争；不能宣称整个取消链路完全不等待。
- 父 NOWAIT 处的锁冲突属于正常竞争退出；子查询、更新或提交失败属于技术失败，不能据此推断其他节点正在取消。超时或失败结束本次处理，提交不明不等于已回滚或已成功取消，后续依据持久化状态继续。

普通、准备、分片和收尾共用以下有界子处理，不按 `task_type` 分支，也不为普通任务另写单条取消流程；只有一条 Task 时就是一批处理一条。处理容量及超时边界见[STOPPING 取消竞争与容量](execution.md#stopping-processing)。

**2026-10-23 已通过以下定位查询及索引，尚未实现。** 每次只读取指定 Instance 的一批未结束 Task，不读取结果内容，不先加载整轮所有 ID：

```sql
SELECT id
FROM job_task
WHERE instance_id = :instance_id
  AND status IN (1, 2, 3)
ORDER BY id
LIMIT :batch_size;

CREATE INDEX idx_job_task_unfinished_instance
ON job_task (instance_id, id)
WHERE status IN (1, 2, 3);
```

这里的 `1/2/3` 为 Task 的待执行、执行中、等待重试，不是 Instance 状态。查询不加显式锁、不授权执行，取得有限 ID 后再执行已定条件更新：

```sql
UPDATE job_task
SET status = 6,
    next_dispatch_at = NULL,
    finished_at = :processed_at,
    end_reason = :cancel_reason,
    result_data = NULL,
    result_summary = :cancel_summary,
    updated_at = :updated_at
WHERE instance_id = :instance_id
  AND id IN (:batch_task_ids)
  AND status IN (1, 2, 3);
```

`batch_task_ids` 只包含本批有限候选，不先拉取全轮 ID 再在内存切分。待执行、执行中及等待重试转为取消；成功、最终失败及已取消保持原样。取消针对 STOPPING 父实例下所有尚未结束的工作，不以候选的旧 `attempt_no` 限制：候选读取后即使由执行中转为等待重试，也仍需取消。本批已取得父主键单行锁，不增加 Task 显式多行锁，正常子行更新仍具有数据库自身的锁行为。

子查询不使用 `OFFSET`，也不另存 Task 游标。每批成功取消的行不再满足未结束谓词，下一批仍从该 Instance 当前最小的一批未结束 ID 开始；并发有效结果先使子记录终态时同样自然排除。更新失败或提交不明时不以候选已经读取为由宣称该批完成，应结束本次处理、让出容量，后续按当前持久化状态继续，不能无限紧循环重做首批。成功处理但本次份额耗尽时，尚未结束的 Instance 放回本地队尾；父锁冲突或技术失败不走立即重排，等待后续新提示或恢复。每批独立释放父锁，不要求每批等一个扫描周期。

该部分索引同时服务于本查询和下节最终更新中的 `NOT EXISTS`，不新建第二个同义未结束检查索引；它不替代单例唯一索引，也不用于宣称完整历史统计或取消目标地址去重查询已经覆盖。所有未结束的正常 Task 也要维护此索引，并非只有被停止的 Task 才有成本。进入或离开谓词会增加索引写入、WAL 及后续清理开销；旧版本由 PostgreSQL 清理机制回收，不是每次状态改变重建整棵索引。实施时须验证大量终态历史、活跃 Task 和并发取消下的执行计划及写入成本，不能把 LIMIT 当作数据库总访问量或耗时上限。

被取消行清空业务 Data 和最早派发时间，摘要更新为“因整轮停止或超时而取消”的平台说明，结束原因沿用父停止决定的 `manual_stop` 或 `instance_timeout`；不宣称业务执行失败或实际退出。每个短批次使用一次数据库处理时间作为 `processed_at`，不复制父结束时间。保留执行编号、已用次数、开始及登记时间、执行器身份和地址，不返还执行机会。因此整轮超时时，Instance 最终为失败，尚未结束的子 Task 为取消，由结束原因说明关联。

批次上限限制单次数据库工作量，不减少总写入量。每批 Task 默认最多 256 条；单个 Instance 每次最多处理 4 批，或达到 2 秒处理预算后不再启动下一批。成功且未完成时放回队尾，失败或竞争未取得父锁时结束本次处理；后续按 STOPPING 和当前未结束子记录接续。单条 SQL 超时初值 1 秒，不将其解释为整个事务或连接释放的硬上限。完整容量、扫描及轮转边界见[取消竞争与容量](execution.md#stopping-processing)。分片生成完整 SQL 另行设计，本节不授权编码。

#### 最后保存 Instance 终态

某批已取得父锁、确认仍为 STOPPING，且本批子查询已无未结束 Task 时，在当前父事务内执行以下条件更新，再提交释放锁。继续保留 `NOT EXISTS` 作为最终条件，不扫描或更新全轮历史 Task；已有父终态直接退出，不重复刷新 `finished_at`。此前取消批次已经分别提交，不将整轮子取消合并进一个事务：

```sql
UPDATE job_instance AS i
SET status = :target_status,
    finished_at = clock_timestamp(),
    updated_at = :updated_at
WHERE i.id = :instance_id
  AND i.status = 6
  AND i.end_reason = :expected_end_reason
  AND NOT EXISTS (
      SELECT 1 FROM job_task AS t
      WHERE t.instance_id = i.id AND t.status IN (1, 2, 3)
  );
```

`target_status` 只由冻结原因确定：人工停止转取消 `5`，整轮超时转失败 `4`；不按当前子结果重新裁决，也不覆盖 `end_reason/result_summary`。更新一行且确认提交才表示数据库停止处理完成；零行不宣布完成，错误或提交不明按持久化事实核对／重试。`finished_at` 是表达式求值时间，不是业务退出或精确 COMMIT 时间，包含等待数据库处理的延迟。

该检查成立依赖 STOPPING 已禁止所有新增 Task／尝试，子终态不可回退；每批父行锁使同一 Instance 的取消批次不能并行处理，正常结果写入仍可并发。没有 Task 的 WAITING 停止也通过这一条件结束，不补建空 Task。正常完成仍使用 RUNNING 完整结果协议，不用此 `NOT EXISTS` 替代分片生成封口和 Post 完整性检查。

#### STOPPING 恢复候选查询与索引 {#stopping-recovery-query}

**2026-10-23 已通过本节查询、部分索引及有限遍历规则，尚未实现。** 确认决定后本地立即处理是正常路径；遗漏由现有 Leader 每 2 分钟驱动增加独立的 `job_instance.status=6` 候选查询，新 Leader 就绪时执行一次有界检查。STOPPING 不套 `started_at` 的 4 分钟门槛，也不要求 `deadline_at` 非空，原 WAITING 可以从未开始计时。

每一趟开始时，用同一 STOPPING 部分索引读取当前可见的最大 ID，固定为本趟 `upper_id`；没有记录则本趟结束。`after_id` 从头开始，后续按 ID 升序读取：

```sql
SELECT id
FROM job_instance
WHERE status = 6
ORDER BY id DESC
LIMIT 1;

SELECT id
FROM job_instance
WHERE status = 6
  AND id > :after_id
  AND id <= :upper_id
ORDER BY id
LIMIT :page_size;

CREATE INDEX idx_job_instance_stopping
ON job_instance (id)
WHERE status = 6;
```

只返回 Instance ID，不关联 Task、不读取执行配置或结果，不加行锁或修改状态。查询保留固定状态谓词以适配部分索引；实现沿用 GORM 查询规范，验证实际生成 SQL 和 PostgreSQL 执行计划，不机械改成 Raw。固定上界防止持续出现的大 ID 候选让本趟永远追赶新增记录，不是对 STOPPING 集合加快照锁。

- 候选被后台组件接纳或与已有提示合并后才推进 `after_id`，不等待实际取消或父最终更新成功；已查询但未接纳不能算消费。
- 队满停在未接纳项之前；本周期预算耗尽时保留 `upper_id/after_id`，下周期继续同一趟。查询失败不推进位置，不叠加并行扫描。
- 本趟结束后，后续新一趟重新取得上界并从头检查；不使用永久高水位。游标经过后才进入 STOPPING 的较小 ID、晚提交记录及前趟已接纳但处理失败的记录，仍由新一趟覆盖。
- 失主丢弃本地上界及位置，新 Leader 从头有界检查；已接纳的进程级工作继续遵守原数据库协议，允许重复处理但不改变冻结原因。
- 分页只约束候选遍历，不替代处理公平性。每个 Instance 仍须按[子取消规则](#task-cancel-query)在有界预算后让出；竞争失败或技术错误结束本次处理，等待后续新提示或恢复，不立即重排，不能由一个大轮次或持续失败项占满后续处理机会。

先取得父 ID，再按单个 `instance_id` 的未结束索引处理子记录，不全局扫描 Task，不持续扫描历史终态父。STOPPING 与 RUNNING 各自保留上界、位置及预算，不合并为一个查询；Instance 停止索引只保留满足谓词的记录，Task 未结束索引则覆盖全部正常活跃子任务，需分别核算维护成本。2026-10-26 已确认容量：每页 256 个 Instance，每周期扫描／投递预算 5 秒，队满也停止；不额外设置每周期 1024 个候选上限。子取消每批最多 256 条、每节点并发 2，本地待处理容量 256 个 Instance（含排队与处理中），单 Instance 每次最多 4 批或到 2 秒后不再启动下一批，单条 SQL 超时 1 秒。扫描游标只按接纳前移，不能因后续竞争失败退回页头；完整边界见[取消竞争与容量](execution.md#stopping-processing)。取消目标地址的[有界查询与索引](#cancel-target-query)已另行通过，不属于这两组数据库取消查询，均尚待实现验证。

复用低频驱动不等于没有新增查询或处理成本。持久化 STOPPING 是未完成义务，不设置永久数据库重试耗尽；服务及数据库恢复且获得公平处理容量时可接续，永久数据库故障、损坏或不可恢复缺陷不承诺必然结束。RUNNING 的 2 分钟周期、4 分钟门槛及 256 条分页保持不变，详见[恢复职责](execution.md#stopping-recovery)。

#### 与结果和派发的竞争

| 先有效提交的操作 | 后续处理 |
| --- | --- |
| 父 Instance STOPPING 决定 | 后续插入及登记取得父锁后拒绝新工作；已有授权仍可能有 RPC 在途 |
| 新尝试登记 | 父终止不能撤销已经发出的业务，只能尽力取消；以后不再授予新尝试 |
| 子成功或最终失败结果 | 取消 UPDATE 不覆盖已有终态和结果 |
| 子取消 | 迟到结果、ACK 和 BUSY 不能恢复该 Task |
| 失败、BUSY 或未知结算形成等待重试 | 子仍未结束，本次取消继续生效，不因短暂候选状态重新派发 |

父停止决定、子批次和父终态分别提交；父最终结束前必须确认所有子行已终态，但不证明业务未执行或已经退出。子条件更新零行不需要逐条查询猜测竞争原因，也不影响依据已确认停止决定处理其他未结束子 Task。

#### 取消通知独立于子状态更新

**远端停止的粒度始终是批次（Instance），不是 Task。** 新的 STOPPING 决定明确提交后，分别交接数据库取消与通知，两者不互相等待。一个 Instance 的 1,000 个 Task 若分布在 5 个已知地址，就生成 5 个目标通知，通信重试另计；每个目标取消本机属于该 Instance 的全部排队或运行活动。数据库每批取消 256 条 Task、地址每页最多 32 个，均不改变 RPC 粒度。

通知授权依据是已确认的停止决定，不是某批子 UPDATE 成功；无需等所有子 Task 改完才发。子更新零行、失败或提交不明不妨碍对已取得地址尽力通知；停止决定未确认时不能凭本地意图发送。每个子取消批次、两分钟 STOPPING 恢复扫描及只读到既有停止决定的重复管理请求，都不自动重新开启整轮通知。提交结果不明后的确认及通信重试仍可能重复，本地仅合并排队或处理中提示，不建立全局通知锁、可靠通知表或永久已通知标记。

通知已接纳后，即使父先进入 CANCELED／FAILED，也在原预算内继续地址读取和发送，不因数据库处理较快就丢弃；不延长预算、不扫描历史补送。RPC 失败、耗尽、队满未接纳或进程退出都不阻挡子取消和父终态。RPC 在所有数据库事务之外，采用[按 Instance 定向通知](execution.md#cancel-routing)及[独立有界通知容量](execution.md#cancel-notification-capacity)；同一次目标通知重试共用临时直连客户端，结束关闭，不跨通知缓存。

#### 取消目标地址查询与索引 {#cancel-target-query}

**2026-10-26 已通过查询方向、索引及分页边界，尚未实现或执行迁移。** 地址只从当前 Instance 已保存的非空 `executor_address` 读取，不按 Task 状态过滤，不只取本批成功取消行，不读取业务结果内容；本轮组从 Instance 取得，按组与地址去重，不按进程 UUID 拆成多次通知。既有结果或取消不会清空已保存地址，但当前 Task 不保存旧尝试地址历史，不承诺找回重试时已清空的旧目标。

```sql
CREATE INDEX idx_job_task_instance_address
ON job_task (instance_id, executor_address)
WHERE executor_address IS NOT NULL;
```

每次只定位一个 Instance，按地址有序键集分页，每页最多 **32 个不同地址**。首屏从最小的非空地址开始；续页从最后已接纳地址之后继续，不使用 OFFSET，也不保存全趟已见地址集合。地址读取本身不加父行锁或 Task 多行显式锁，不依赖取得数据库取消处理资格，不能将慢查询或 RPC 放入取消事务。

查询应在**一条有界 SQL 内利用索引反复定位下一个不同地址**，可以采用递归 CTE：每步以地址严格大于前一个地址、按地址排序并限一条，最多取得一页。不是 Go 按地址逐条发 SQL，也不是 Task ID 分页后仅在单页去重；后者会使同一地址跨页反复通知。直接 `DISTINCT ... LIMIT 32` 不保证只处理少量索引项，大量 Task 共用少数地址时仍可能遍历许多重复值，不能用返回行数宣称查询成本已经有界。

实施时须核对锚点与递归项的结果类型，以及比较、排序、索引的排序规则一致；首屏和续页保留可定位的索引范围。Repository 若使用 Raw SQL，理由是这一项有界索引定位的查询结构，并提供实际生成语句、执行计划及回归验证，不扩展为普通 CRUD 全部改写 SQL。32 限制返回地址和逻辑定位步数，不保证最多读取 32 个物理页或索引元组；MVCC 可见性、死版本、回表和规划器选择仍有成本，单条查询超时初值 **1 秒**，同时服从本次通知剩余预算。

地址游标只在目标被接纳后前移；目标队满暂停，不跳过拒收地址或继续查询后页，不为每个 Instance 各保留一页待发数据。每页交接完后轮到待枚举集合队尾，空页结束本次枚举。查询失败终止剩余枚举，已经接纳的目标继续原预算；不无限重读、不将失败扩展为后台可靠补送。

跨页采用默认读已提交，不维持全趟一致快照。迟到绑定落在已越过地址之前，可能错过本次通知；父终态、旧目标未知或已授权派发晚于取消到达的窗口继续按尽力取消处理。索引覆盖所有状态中仍保存地址的 Task，ACK 绑定、重试清空及历史保留会增加写入、WAL、空间和清理成本，不是仅停止时才付成本，也不能拿未结束 Task 部分索引替代它。专项验证见[远端取消通知验收](verification.md#cancel-notification-checks)，具体 SQL 实现与执行计划尚未验证。

#### 写库故障与未送达窗口

| 故障 | 处理及边界 |
| --- | --- |
| 人工停止的父事务提交结果不明 | 不宣称请求已接受、不据内存决定发送取消；可在当前请求有限处理范围内重入父协议核实；仍 WAITING／RUNNING 则按人工停止处理，已有 STOPPING／终态则保留原原因。仍无法确认时返回未确认，由调用方查询或重试 |
| 人工停止实际未落库 | 没有可恢复的持久化停止意图，后台不能凭空恢复本次人工操作；原 Instance 可能继续运行至正常结束或原截止 |
| 自动超时处理失败或提交不明 | 原运行状态、截止时间及已保存结果仍是恢复依据；释放候选，已有检查重新进行完成／超时判定，不凭旧候选直接写超时，不宣称已回滚或已结束 |
| STOPPING 已提交，子更新失败／提交不明或节点宕机 | Leader 的 STOPPING 候选分支重新发现父，再按本轮索引继续子处理；重复条件更新不覆盖已结束行，停止原因不丢失 |
| 子已全部终态，但父最后更新失败或节点宕机 | 父仍为 STOPPING，恢复重新执行最终条件更新，不重跑业务、不必再次取消已结束 Task |
| 子 Task 已全部终态，节点在取消 RPC 发送前退出 | 接受通知可能根本没有发出且后续不再补发；不持续扫描全部历史终态实例，不新增可靠通知记录 |

数据库子状态收敛和取消消息送达是两种保证。STOPPING 恢复能接续未完成的持久化处理，不保证补齐每条取消通知；绑定缺失、迟到绑定和已授权派发晚于取消到达的窗口继续有效。执行器仍受原截止及协作式取消约束，但平台终态不等于业务必然退出。STOPPING 同时属于阻塞丢弃、Job／组删除保护及历史清理的活跃集合，只有数据库处理结束后才解除这些限制。

对应[停止超时与统一收敛验收](verification.md#instance-termination-sql-checks)、[停止恢复查询验收](verification.md#stopping-recovery-checks)及[完成与超时判定验收](verification.md#instance-completion-checks)尚待实现。正常完成父 UPDATE、摘要及超时衔接、STOPPING 父候选和子定位查询及两条部分索引已通过；2026-10-26 另已通过取消单批父 NOWAIT 协调、容量初值与公平轮转。同日另已通过[取消地址定位与索引](#cancel-target-query)及[远端通知来源、容量与预算](execution.md#cancel-notification-capacity)。原因码、完整分片生成／查询、管理接口及具体实现验证继续按评审计划落实，本节不授权执行模块编码。

### 触发事务与必要索引 {#trigger-transaction-proposal}

本节事务、身份约束、冲突处理与五个必要索引已通过，Job 软删除后补齐有效行谓词。范围是正常创建、重复请求、历史身份冲突及本批查询路径，不改变触发、错失或阻塞规则；恢复查询、跳过留痕、Job 删除、组引用校验及并发保护、管理入口按下方已确认规则执行，最终接口和 SQL 契约已由[总审附件](trigger-readiness-review.md)确认。下列 SQL 是已确认的设计契约，尚未生成或执行迁移。

#### 两种身份使用部分唯一索引

```sql
CREATE UNIQUE INDEX uq_job_instance_auto_identity
ON job_instance (job_id, scheduled_at)
WHERE scheduled_at IS NOT NULL;

CREATE UNIQUE INDEX uq_job_instance_manual_identity
ON job_instance (job_id, request_id)
WHERE request_id IS NOT NULL;
```

正常与错失共用第一个索引，不加入 `trigger_type`。自动记录不进入手动索引，手动记录不进入自动索引；两个普通 UNIQUE 在 PostgreSQL 默认 NULL 不相等语义下也能防重，但会保存另一类记录中无查重作用的 NULL 条目，因此选择部分索引。不再额外创建同列的普通索引。

身份字段的适用非空和互斥用结构完整性 CHECK 表达：

```sql
CHECK (
  (trigger_type IN (1, 2)
   AND scheduled_at IS NOT NULL AND request_id IS NULL
   AND params_overridden = FALSE AND triggered_by IS NULL)
  OR
  (trigger_type = 3
   AND scheduled_at IS NULL AND request_id IS NOT NULL
   AND triggered_by IS NOT NULL)
)
```

这不增加重试、间隔或超时范围 CHECK。历史清理删除记录后，相应身份的数据库防重证据也消失，不提供永久去重。

#### 自动创建和进度推进

1. 开启短事务，以 `WHERE id = $1 FOR UPDATE` 单独锁定一条 Job，读取本次创建所需字段及 `deleted_at`；Job 不存在或已删除则结束，不锁整个扫描批次。使用 ORM 默认过滤也须验证最终 SQL 的软删除条件，不能让手写 SQL 绕过检查。
2. 取得锁后读一次调度进程时间，按最新状态及进度判断。停用或未到期直接结束；有效到期时按已定 Cron 规则求出严格晚于判定时间的下一次，无有效 Next 则不写入。
3. 仅在错失策略允许创建且阻塞策略为丢弃时，用下一条 SQL 查询同 Job 是否存在未结束 Instance；并行策略不需要该查询。
4. 允许创建时写入待开始 Instance 及本轮配置。插入只处理指定的自动身份冲突，冲突动作是 `DO NOTHING`，不改旧记录。
5. 新建成功、相同自动身份已经存在、错失忽略或阻塞丢弃，都按对应成功处理分支推进 `Job.next_trigger_at`，同时维护审计字段 `updated_at`，不将其用于判断配置变化。更新影响行数须为 1，否则整笔回滚。
6. Instance 插入与 Job 进度更新同事务提交。仅新插入且明确提交成功的记录可发出本地推进提示；提交不明沿用原恢复规则，不凭报错重新创建。

指定自动冲突的 INSERT 尾部为：

```sql
ON CONFLICT (job_id, scheduled_at)
WHERE scheduled_at IS NOT NULL
DO NOTHING
RETURNING id
```

必须区分“语句成功但没有返回行”和“语句超时／失败”：前者才表示该身份已存在，后者不能伪装成重复成功。其他写入错误正常报错，不使用无冲突目标的 `ON CONFLICT DO NOTHING` 吞掉其他约束故障。

**历史身份冲突沿用旧记录，并推进当前 Job，不重新执行旧 Instance。** 例如校时回拨后重新启用，再次算到已保留的 `(job_id, 2026-10-15 10:00:00+08:00)`，不覆盖旧参数、名称、组、触发类型或终态；不调整毫秒绕过唯一键，也不因唯一冲突永久卡住进度。这里是完整日期时间对应的时间点，不是仅小时和分钟；同 Job 的 `2026-10-16 10:00:00+08:00` 是另一身份。需要再次执行时使用新的手动请求号。正常路径不额外查询“是否已存在自动身份”，由精确冲突处理兜底；若本次已按错失或阻塞跳过，也不为区分跳过原因再查旧记录。

#### 手动创建和原请求重发

鉴权、请求格式校验及进程容量规则不变。进入事务后先单独按主键锁 Job，确认存在且 `deleted_at IS NULL`，再以新 SQL 查同 Job、同 UUID 的原 Instance，读取 ID、覆盖标记和必要的冻结参数。Job 已逻辑删除时直接返回不存在，不因物理行仍保留而重放旧创建成功。

- 已存在：按已批准的参数比较规则返回原 ID 或内容冲突；不再做新建阻塞判断，也不拿当前 Job 参数或当前组可用性重新判定旧请求。
- 不存在：校验当前创建条件，按需检查同 Job 未结束 Instance，写入手动身份及当前配置快照；不推进 Job 时间。
- INSERT 使用与手动唯一索引匹配的 `ON CONFLICT (job_id, request_id) WHERE request_id IS NOT NULL DO NOTHING RETURNING id`。正常遵守 Job 锁时通常不会撞上冲突，但仍以唯一索引兜底。
- 冲突后没有返回行时，用下一条 SELECT 读取原记录并比较；不直接宣称创建成功，不用 `DO UPDATE SET id = id` 获取旧 ID，也不覆盖旧快照。若新查询仍无记录，例如并发历史清理移除了记录，结束本次处理并返回可重试的不确定结果，不在事务里无限循环。

新建须明确提交成功后才返回新 ID。Job 已删除后不从创建入口返回旧成功响应；原 Instance 仍可通过历史查询读取，不把“Job 不存在”解释为旧请求没有执行过。本协议不扩展为跨 Job 删除仍可永久重放。

#### 行锁和后续查询分成两条语句

READ COMMITTED 每条语句取得自己的快照。初始 Job 锁查询可能等待前一个创建事务；把手动查重或未结束 EXISTS 塞进这条 SELECT 的子查询／同一条 CTE，可能仍按等锁前的快照读取 Instance。

因此必须先取得 Job 行锁，再发下一条语句查 Instance。前一个创建已提交后，后一个创建才能看见它，避免丢弃策略下两个入口都判断“没有未结束实例”。`INSERT DO NOTHING` 后取原记录同样使用下一条 SELECT，不依赖同语句的 INSERT CTE 加 UNION 一定返回旧 ID。

#### 本批查询及五个索引

除两表主键外，触发路径采用以下五个索引，包括上面的两个唯一索引；管理查询另增加一条[按组分页与引用检查索引](#job-input-query-proposal)：

| 索引 | 定义 | 对应查询 |
| --- | --- | --- |
| 到期 Job | `(next_trigger_at, id) WHERE deleted_at IS NULL AND status = 1 AND next_trigger_at IS NOT NULL` | Leader 按时间及 ID 排序，限量读取最早到期的未删除候选 |
| 同 Job 活跃 Instance | `(job_id) WHERE status IN (1, 2, 6)`，非唯一 | 丢弃策略的 EXISTS，不加载或统计全批 |
| 自动身份 | 部分唯一 `(job_id, scheduled_at)` | 正常与错失共用身份 |
| 手动身份 | 部分唯一 `(job_id, request_id)` | 请求去重及读取原记录 |
| 待开始 Instance | `(id) WHERE status = 1` | 按 ID 有界查找持久化交接记录，不扫描全部历史 |

未结束索引必须非唯一，否则会破坏允许同 Job 并行的规则。待开始查询只是候选查找，不代表已领取执行权，不增加固定负责人；启动竞争按后续执行模块的 Instance 单行协议处理。

到期 Job 查询只返回 `id, next_trigger_at`，条件为未删除、启用且进度不晚于本次查询采用的进程时间，按 `(next_trigger_at, id)` 排序并 LIMIT 限量。每次从头重新查询，不使用 OFFSET、不对候选列表加锁、不保存恢复游标。取得候选后仍须在上述事务中重读 Job；数量、超时及局限见[已确认的恢复查询](#trigger-recovery-scan-proposal)。

活跃查询使用 `WHERE job_id = $1 AND status IN (1, 2, 6)` 的 EXISTS；待开始交接使用 `WHERE status = 1 AND id > $1 ORDER BY id LIMIT $2`，不承诺严格创建时间 FIFO。

固定状态谓词写在 Repository SQL 中，Job 的到期查询和索引同时包含 `deleted_at IS NULL`；不额外为删除时间建单列索引。Job ID、截止时间、待开始 Instance 的 ID 游标和条数仍绑定参数。不能依赖通用执行计划把 `status = $2` 必然匹配到 `WHERE status = 1` 的部分索引；验收须检查驱动实际 SQL 及预编译计划。部分唯一索引由列和 WHERE 推断，不把索引名当作 `ON CONFLICT ON CONSTRAINT` 的约束名。

不另建通用 JSONB GIN、独立状态索引或包含大参数的覆盖索引。管理列表的按组索引按[已定查询契约](#job-input-query-proposal)落实，超时扫描和历史清理的索引随对应模块补齐。索引仍有写入、状态变更及清理成本；实现后用代表性历史量和活跃比例验证执行计划，小表使用顺序扫描不直接判为失败。

#### 待实现的验证要求

- 两个自动入口或自动与手动并发时，丢弃策略最多留下一个新活跃 Instance；并行策略仍允许不同身份同时存在。
- 同自动身份及同手动请求不重复创建；正常／错失不能各建一份。参数冲突不覆盖旧记录。
- 在 Instance 插入后、进度更新前制造失败，验证事务回滚；提交响应丢失后重入按已保存事实处理。
- 校时后重遇自动历史身份，验证保留旧记录、推进新进度、不重复通知旧实例执行。
- 通过等锁场景验证后续 SELECT 的新快照；不以无竞争单测证明锁与阻塞检查正确。
- 用实际驱动和足够历史数据验证各查询及部分索引；检查两个唯一索引只覆盖各自适用记录。
- 在大历史存量、持续创建及状态更新、历史清理并行的条件下，测量查询和事务延迟、索引体积、WAL、I/O 与 vacuum 负担；不能只测空表或瞬时插入吞吐。部分索引减少无关历史查询，不消除索引维护、MVCC 死元组及清理成本。

上述是实现后的验收要求，尚未执行数据库功能或性能测试。当前暂不考虑月分区，不因此确定历史保留期或清理机制。竞品路径和 PostgreSQL 语义依据见[触发持久化比较](references.md#trigger-persistence-evidence)。

### Leader 到期 Job 恢复查询 {#trigger-recovery-scan-proposal}

**每次从 PostgreSQL 主库重新读取最早到期的一批 Job，不分页、不保存跨分钟游标或扫描上界。** 本节已通过，替代此前 Job 触发恢复的公平轮转要求；自动队列内部轮转及 Instance／Task 的恢复规则不因此改变。

本路径只查询 `job_definition`，不扫描 Instance 历史，也不决定 Instance／Task 恢复的周期。正常触发仍依靠长期 Cron；Leader 接任立即检查一次，之后由独立的分钟循环兜底。同一任期只有一个串行恢复循环，不重叠执行或补跑积累的 tick。

#### 单批查询及初值

| 参数 | 部署初值 | 范围 |
| --- | --- | --- |
| 每次查询候选上限 | 1024 条 | 单次 LIMIT，仅返回 ID 和时间；不表示全部能入队或完成，也不是 Job 数量上限 |
| 单次恢复查询超时 | 5 秒 | 覆盖获取数据库连接及完整读取本批结果；受更短任期期限约束，不是整轮扫描或触发处理预算 |

两项采用正值部署配置，不加入 Job 页面，不提供每页大小或跨分钟巡回参数。当前几百条规则的规模下，一次读取少量标量即可覆盖候选；扩大规模后的积压恢复耗时仍须验证，不能把初值当成吞吐承诺。

每次查询前读取调度进程当前时间作为 `$1`，`$2` 为候选上限，使用已定到期 Job 索引：

```sql
SELECT id, next_trigger_at
FROM job_definition
WHERE deleted_at IS NULL
  AND status = 1
  AND next_trigger_at IS NOT NULL
  AND next_trigger_at <= $1
ORDER BY next_trigger_at, id
LIMIT $2;
```

普通读取不加候选行锁、不查总数、不逐 Job 预查状态。结果完整读取成功后，按顺序向已有有界触发队列非阻塞提交 Job ID；重复项沿用已有合并规则。候选不是执行授权，实际处理仍锁 Job 重读最新配置及进度。

#### 提交与故障边界

- 队满时结束本次检查，不等待清空、不添加缓冲队列；尚未提交的候选随本次列表释放。不因查过、接纳过或拒收而推进数据库时间。
- 查询失败或超时则结束本次，不使用不完整结果，不立即自循环重试；下分钟重新读取当前数据库。
- 已接纳的 Job 处理失败时，持久化进度未成功推进，后续正常来源或分钟检查仍能重新发现；不为它保存单独的补试对象。
- 成功创建、错失忽略或阻塞丢弃后，事务将进度推向未来，使其退出原来最老的积压位置。高频 Job 下次仍可能到期，但使用新进度排序，不保留原扫描位置。
- 失主后按任期规则取消查询及后续提交；新 Leader 重新查库，没有恢复游标需要继承。查询超时上下文不能用于已经交接的队列工作；查询完成释放资源，不取消这些工作。各 Job 仍沿用 3 秒触发处理预算及锁后 5 秒错失判断。

#### 已接受的公平性限制

**不保证持续异常或超载下的严格恢复公平，也不承诺一分钟恢复完毕。** 最前面大量 Job 若持续失败、始终不推进，可能反复占满查询限额；恢复若反复碰到队满，也可能让其他遗漏 Job 长期得不到接纳。正常 Cron 继续运行，不能替代这项恢复保证。

查询反复截满、队满拒收、持续失败及长期逾期应当可观测，供运维处置；不因此新增自动停用、跳过失败行的持久化标记或另一套重试调度。一次查询返回上限条数，只说明可能仍有积压，不代表精确积压总量。

该取舍保留有限查询、有限队列与固定创建并发，减少跨周期扫描状态。按[恢复验收](verification.md#trigger-recovery-checks)验证正常恢复、队满、暂时故障、长期失败及切主边界。竞品扫描承担常态调度，不能直接作为本节参数或保证的依据，见[扫描用途比较](references.md#trigger-recovery-scan-evidence)。

### 自动跳过触发留痕 {#trigger-skip-record-proposal}

错失忽略、阻塞丢弃不创建执行 Instance，但仍在 Job 短事务中推进进度。本节留痕方案已通过，不改变错失或阻塞策略，也不处理已经创建 Instance 后的失败、取消或超时。

**第一版只写结构化服务日志，不另建跳过记录表，不增加 Job 的最后跳过字段，也不为展示原因创建取消 Instance。** 管理端 Instance 历史因此不展示这些跳过；排查原因依赖日志系统，不能仅凭实例列表空白判断是按策略跳过还是调度故障。

| 情形 | 留痕 | 持久化行为 |
| --- | --- | --- |
| 超过容忍且错失策略为忽略 | `MISFIRE_IGNORED`，记录被消费区间的起点、判定时间及新进度 | 沿用已定事务推进 Job，不创建 Instance |
| 按丢弃策略发现同 Job 尚有未结束 Instance | `BLOCK_DISCARDED`，记录相同时间信息 | 同上，不把旧 Instance 改为取消，也不安排本次延期 |
| 队满、查询或触发事务失败 | 沿用容量／技术错误日志和指标，不记成策略跳过成功 | 拒收和明确回滚不推进；提交不明按原协议核对，不据报错认定回滚 |

一次已确认提交的跳过决策最多形成一条服务事件，不枚举被折叠的每个 Cron 时刻，也不额外查询精确的漏跑次数。字段使用本次处理已经取得的 `job_id`、原 `scheduled_at`、`decision_at`、新 `next_trigger_at` 和原因；可附现有触发来源与节点关联信息，不为日志额外查询用户名、阻塞 Instance 或保存业务参数。

成功跳过日志在数据库提交确认后、事务外输出。回滚不能先写成已跳过；提交不明只记录技术不确定信息，不凭本地判定输出成功跳过。日志与数据库提交不是原子的：提交后进程退出或日志采集故障可能缺失记录，不增加 Outbox、跨系统事务或可靠回放来弥补这一窗口。

本方案提供诊断线索，不承诺完整审计或精确跳过次数；日志输出、采集及保留仍有成本。Instance 成功率只覆盖已经创建的执行轮次，不代表所有计划机会的调度成功率。第一版接受管理页没有跳过明细，原因追溯依赖日志系统的采集、检索和保留能力。

实现按[跳过留痕验收](verification.md#trigger-skip-record-checks)覆盖两种原因、回滚不报成功、提交不明、区间折叠仅一条事件、无额外业务查询及实例历史不出现伪造执行轮次。竞品的错失与阻塞留痕不同，见[跳过记录比较](references.md#trigger-skip-record-evidence)。

### Job 逻辑删除与历史保留 {#job-deletion-proposal}

Job 逻辑删除及本节其余删除边界已通过，替代先前物理删除建议。本节不决定执行器组删除、历史保留期限或清理算法。

**只允许删除已停用、且没有待开始、运行中或 STOPPING Instance 的 Job；标记逻辑删除，保留 Job 定义及 Instance／Task。** 使用 `deleted_at` 保存删除时间，不增加回收站、恢复入口、级联删除或“删除并停止”组合操作。

| 条件或动作 | 行为 |
| --- | --- |
| Job 仍启用 | 拒绝，先停用 |
| 已停用，但有待开始、运行中或 STOPPING Instance | 拒绝，先等待该轮数据库处理结束，或独立发起停止并等其终态，再重试删除 |
| 已停用，所有 Instance 都已进入终态，或从未执行 | 通过 GORM `Delete` 将 `job_definition.deleted_at` 从 NULL 设置为删除时间，沿用 System 行为不补写 `updated_at`；保留原定义、停用状态及进度，不删除组、Instance、Task 或服务日志 |
| 父 Instance 为 STOPPING，子取消或最终父更新尚未完成 | 仍属活跃轮次，阻止删除；数据库处理完成不证明业务进程已经退出 |
| 删除后查询历史 | 继续按 Instance ID、Job ID 及本轮名称快照查询；必要时显式只读关联已删除 Job，不要求其仍为有效定义 |
| 删除后收到旧手动创建请求 | 返回 Job 不存在，不从创建入口重放旧成功响应；不代表旧请求从未执行，已有 Instance 仍可查询 |

删除与创建沿用同一 Job 的短事务：先按主键单独取得 Job 行锁，确认未删除且停用，再用下一条 SQL 执行未结束 Instance 的 `EXISTS` 查询；无活跃轮次才更新删除时间并提交。不能把活跃检查塞进取得行锁的同一语句快照。查询复用已有 `(job_id)` 活跃 Instance 部分索引，不统计或扫描全部历史，也不逐条读取 Task。

手动创建先提交，删除就能看到新轮次并拒绝；删除先提交，后来的自动或手动创建即使读取到物理行，也须因删除标记拒绝推进。检查后某轮恰好结束时允许保守拒绝，用户稍后重试，不追加跨对象锁。提交后沿用 etcd 配置通知及分钟核对移除内存规则；旧提示到达时仍须重新查询，通知不承担防止创建的数据库保证。

普通 Job 列表与详情、配置加载、Job 触发恢复候选、编辑、启停及手动／自动创建均排除已删除记录；不能仅凭 `status = 0` 将其重新启用或手动执行。ORM 默认作用域、手写 SQL 和到期部分索引必须一致使用 `deleted_at IS NULL`。已有 Instance／Task 的恢复不按当前 Job 删除状态排除旧轮次。历史关联在必要的只读查询中显式允许已删除 Job，保持权限校验，不全局取消软删除过滤，也不引入逐条查询。

逻辑删除保留的是删除时的最后一版定义，包括尚无执行历史的 Job；它不是每次执行时的完整配置历史。历史执行展示和恢复仍以 Instance 快照为准，不能用已删除或后来修改的 Job 覆盖本轮配置。关联读取原定义用于排查，不是恢复运行的许可。

后续取消恢复、结果处理和历史查询使用 Instance／Task 已保存的组、配置及尝试信息，不依赖已删除 Job，也不删除执行器组。历史清理继续单独设计，不能把“发起停止成功”当作父已终态，STOPPING 及其 Task 不得清理。Job ID 不复用；同名重建是新的 Job。

该方案保留原定义并采用较严格的删除前置条件，但不保证所有业务已经物理停止；删除不承诺回滚业务。Job／Instance／Task 三者中只给 Job 补删除时间，调整到期索引及有效行查询，不新增全表检查或清理已删定义的任务；执行器组另按[组生命周期](#executor-group-lifecycle)维护删除标记。正式验证见[删除验收](verification.md#job-deletion-checks)；竞品差异见[Job 删除比较](references.md#job-deletion-evidence)。

### Job 的组引用与离线配置校验 {#job-group-reference-proposal}

本节规则已通过，只决定 Job 管理入口的引用校验，不扩展执行器目录、节点健康探测或组凭据生命周期。组定义是否存在，与该组现在是否有在线进程，是两个条件。

**新增、编辑保存和启用时校验目标执行器组存在且 `deleted_at IS NULL`；允许先配置 Job、后部署执行器，不要求在线节点，也不远程探测处理器。** 沿用已有稳定组身份，不能接受不存在或已逻辑删除的组 ID。

| 入口或条件 | 已确认规则 |
| --- | --- |
| 新增、编辑保存、启用 Job | 校验目标组定义存在且未删除；第一版不设组启停，具体字段与删除边界见[组生命周期](#executor-group-lifecycle) |
| 手动执行新建一轮 | 校验组引用；若同请求号已创建且 Job 未删除，按既有查重规则返回原 Instance，不因当前组变化重判旧请求 |
| 组已存在但没有在线执行器 | 允许保存、启用和创建手动轮次；成功只代表配置已保存或轮次已创建，不表示可以立即执行 |
| 处理器名称 | 必填并做本地格式校验；不向执行器逐个查询是否注册，不增加处理器登记表或注册同步机制 |
| 停用或删除 Job | 不以组存在或在线为前置条件，避免坏引用阻止管理员停止触发或清理定义；其他删除条件仍成立 |

实际派发时由执行器统一入口查找具名处理器，错误通过执行协议反馈；无可用节点或处理器不存在的状态、原因及重试分类在执行模块细化，不把本项解释为新增无限等待或自动取消策略。单次在线探测也不能保证之后真正执行时仍可用。

该校验属于低频管理操作，不扩展为每次 Cron 创建或每个 Task 派发额外查组表，不在数据库事务内调用执行器 RPC。单次存在性查询不构成完整引用保护，新增／编辑还须遵守已通过的[组引用并发协议](#group-reference-concurrency-proposal)；完整组生命周期未落实前不开放删除。Job 编辑仍只影响新建轮次，已有 Instance 继续使用已保存的组和执行配置。

这是显式引用与离线部署之间的取舍：及早拦截不存在的组，但错误处理器名称可能到执行时才发现。竞品入口存在差异，见[组引用校验比较](references.md#job-group-reference-evidence)；实现覆盖[组引用与离线配置验收](verification.md#job-group-reference-checks)。

### 整轮超时默认值 {#timeout-default-proposal}

**已通过：新增 Job 的整轮超时初值为 900 秒，即 15 分钟，允许用户按业务修改。** 这是新增表单预填值，不是所有任务的推荐运行时长，也不是最大上限。调用创建 API 仍须显式提供 `timeout_seconds`，数据库不设 SQL 默认；不修改已有 Job 或 Instance 快照。

本项只确定此前留空的产品初值。沿用已定规则：必须有限且为正数，不允许 `0`；从首次有效开始准备／执行计时，创建后尚未开始的等待不算。开始后的准备、分片生成与派发、执行、重试等待及业务 Post 共用一个截止时间，不为重试或每个 Task 重新发放 900 秒。平台等待不延长该截止；父仍 RUNNING 且已具备完整最终结果时，允许迟到的平台汇总按结果结束，见[完成与超时裁决](#instance-completion-contract)。

| 设计点 | XXL-JOB | SnailJob | DogX 已定规则 |
| --- | --- | --- | --- |
| 新增任务的初值 | 0 秒，不启用执行超时 | 管理页面预填 60 秒 | 900 秒，可按 Job 修改 |
| 主要计时范围 | 单次 handler 执行，各分片和重试分别计时 | 服务端有整批次超时检查，客户端也有超时停止机制，不只是单个 Task 预算 | 整个 Instance 共用时间，包含准备、重试等待及收尾 |
| 0 的含义 | 不启用该超时限制 | 不能当成永久不超时；底层仍注册超时检查 | 拒绝，沿用已通过规则 |

选择 15 分钟，是在两种代价间取一个产品初值：整轮需要容纳准备、多批分片、重试及收尾，不宜直接照搬 60 秒；已有执行器绑定但无结果时又要等待整轮截止，因此也不默认给出数小时。尚无业务耗时分布支持某个精确最优值，900 秒不是由竞品或压测推导出的性能结论。

例如预计运行 10 分钟的任务，可以从 15 分钟起按实际波动调整；预计运行 1 小时的任务必须主动配置更长预算。若默认配置下开始后第 1 分钟执行器宕机且已有绑定，在没有有效结果或人工停止时仍须等待剩余 14 分钟才到整轮截止，平台保存超时终态还可能叠加周期复核及排队延迟；不因确定默认值而新增失联快速重派。超时只限制平台继续授权执行，不保证不配合取消的业务代码物理退出。

本项不另设业务最小／最大秒数，不改 API 必填契约，也不增加数据库业务范围 CHECK。竞品的默认来源、批次与单次范围区别见[超时比较](references.md#timeout-default-evidence)；实现覆盖[默认值验收](verification.md#timeout-default-checks)。

### Job 管理入口与事务边界 {#job-management-proposal}

本节管理契约已通过，将 Job 行为落实为请求边界、成功含义及写入方式。沿用 DogX 的动作式 POST 接口和统一响应包装，API/BFF 只做鉴权、输入校验与 RPC 编排，`job` 域后端拥有数据和事务。下表路径相对现有 API 前缀；不是已经生成的 `.api` 或 Proto。

#### 接口划分

| 接口 | 输入与成功结果 | 边界 |
| --- | --- | --- |
| `/job/list`、`/job/get` | 分页列表、按 ID 读取配置；沿用 `items/total` 及单对象响应 | 只读未删除 Job，不附带执行历史查询；列表不加载大参数载荷，不逐行查组名 |
| `/job/create` | 新 Job 配置；成功返回 `id` | 默认停用，允许显式启用；提交成功才返回，不等待 Leader 应用 |
| `/job/update` | `id` 与完整可编辑配置；成功返回空业务对象 | 仅停用可编辑，不把遗漏字段理解为局部更新，不包含启停和运行进度 |
| `/job/status/update` | `id`、目标 `status`；成功返回空业务对象 | 独立切换启停；重复启用不重算，停用保留进度且不停止 Instance |
| `/job/delete` | `id`；成功返回空业务对象 | 复用已定逻辑删除及活跃轮次保护，不同时删除历史 |
| `/job/cron/preview` | `cronExpr`；返回未来最多 5 个时间点 `times` | 不要求先保存 Job，不接受客户端时区或起算时间覆盖，不保存任何数据 |
| `/job/trigger` | `id`、UUID `requestId`、`paramsOverridden` 及必要的 `params`；成功返回 `instanceId` | 复用已定手动创建事务、防重及容量规则，不等待业务完成，不改变 Cron 进度 |

列表与详情是管理读取，不复用 Leader 的完整配置加载接口。分页上限沿用现有 API 的 200；筛选项、投影、输入语义和相应查询索引按下方已通过的[输入查询契约](#job-input-query-proposal)落实，短字段长度按[字段容量](#string-field-types)同步，不把本表当作完整查询 SQL。Instance／Task 历史查询仍属于执行查询模块。

#### 配置输入与重复请求

可编辑配置为名称、说明、执行器组、处理器名、执行模式、Post 选项、业务参数、Cron、错失策略、阻塞策略、重试次数、重试间隔和整轮超时。字段含义及默认值沿用[Job 字段](#job-field-draft)，HTTP 使用现有 camelCase 命名。`status` 仅用于新增或独立启停，`next_trigger_at`、删除标记、创建／修改时间不由编辑请求回写。

新增遗漏有明确默认的配置时采用已定初值，超时仍必须显式传入；完整编辑不做 PATCH，也不为遗漏的必需配置补创建默认值。普通新增／编辑的说明和业务参数使用普通字符串，省略、null、空串均按空串处理；编辑时全部写入固定可编辑列，因此省略也会清空，不表示保留旧值。零重试与 false 是合法显式值，确有默认值或筛选语义差异的字段保留存在性；不能使用忽略零值的 ORM 更新方式。普通模式却开启 Post 属于非法组合，不静默改成 false。说明及业务参数均去首尾空白，业务参数不因内容像 JSON 就重新编码；完整规则见[输入契约](#job-input-query-proposal)。

手动请求中，`paramsOverridden=false` 时 `params` 可省略或为 null，但不得提供字符串；为 true 时必须提供字符串，空串或 trim 后为空也有效，省略／null 拒绝。普通新增／编辑的空值简化不改变手动覆盖的组合校验。新增普通 Job 不增加请求号、名字唯一约束或独立幂等表：若创建响应丢失，直接重发可能得到另一个 Job，客户端不得把它当成可安全自动重试的手动触发请求。手动触发继续使用已通过的同号重发协议。

第一版不增加编辑版本号或乐观版本冲突功能。两个管理员同时编辑同一个停用 Job 时，允许后提交的完整配置覆盖前者；条件更新只保证不会覆盖已启用或已删除的对象，不防止覆盖较早编辑的配置。客户端保存前读一次详情，也不能消除这个并发窗口。

#### 写入与事务

| 操作 | 已确认实现边界 |
| --- | --- |
| 新增 | 新对象没有旧 Job 行可锁。校验配置及组引用；停用创建写空进度，显式启用则按调度进程时间计算首轮并与定义同事务保存 |
| 编辑 | 按主键、`deleted_at IS NULL`、`status = 0` 条件更新明确的可编辑列，维护 `updated_at`，不写启停或进度；不因普通编辑额外先加 `FOR UPDATE` |
| 启用 | 按 Job 主键单行 `FOR UPDATE`，读最新配置并校验；已启用不重算。停用转启用时，锁后读取调度进程时间计算 Next，与状态一起保存 |
| 停用 | 单条带有效 Job 条件的 UPDATE 设置停用，保留进度；利用同一行写入的互斥，不额外读旧配置再整行回写 |
| 删除 | 沿用已通过的 Job 行锁、锁后另发活跃 Instance `EXISTS`、同事务写删除标记 |
| 手动创建 | 沿用已通过的 Job 行锁及独立查重，确认未删除后先识别原请求，再做新建所需组引用与阻塞校验，创建并冻结配置 |

编辑先提交时，启用必须读到新配置；启用先提交时，编辑的停用条件不再成立，拒绝保存。删除先提交时，所有后续管理更新及创建不能越过删除标记。条件更新未命中时，不能返回保存成功；按必要的单次读取区分不存在或当前状态不允许，不增加循环重试。数据库继续使用 `READ COMMITTED`，显式锁仅用于已明确的主键单行读改写。

成功修改配置后，事务外使用已定 etcd 通知和分钟核对机制；通知失败不把已经提交的保存改报失败。预览、只读及手动创建不因此发送配置变更通知。预览只调用统一 Cron 计算组件，从一次读取的调度进程当前时间连续求 Next，不查询组、不注册 Cron、不获取 Job 锁；时间返回采用带偏移的 RFC 3339 表达，前端不重新解释 Cron。

所有接口仍执行菜单／API 角色权限，操作者身份来自认证上下文，不接受请求伪造。响应沿用 `code/subcode/message/data`，参数校验使用 `common.invalid_request`；不存在、状态不允许、活跃轮次阻止删除、手动内容冲突、容量忙和技术失败须可区分，不逐字段新增业务码。提交不明不能伪装成已回滚，保存成功也不等于业务执行成功。

#### 本包之外的依赖与验收

管理组存在性检查不能防止并发删组，Job 行锁也不能代替组引用保护；新增／完整编辑遵守下方已通过的[共同组行锁协议](#group-reference-concurrency-proposal)。本包不放开组删除，不扩展为每次自动触发或 Task 派发查组，不在数据库事务内调用 Redis、etcd 或执行器 RPC。

管理入口、事务边界、输入／查询／错误契约、[持久化交接](#trigger-durable-handoff)及最小组依赖已在触发模块实现验证；组引用写入已遵循共同组行锁协议，完整组删除入口仍待后续交付。实现覆盖本批适用的[管理契约验收](verification.md#job-management-checks)及[输入查询验收](verification.md#job-input-query-checks)；与竞品管理入口的比较见[管理接口依据](references.md#job-management-evidence)。

### 执行器组生命周期与本批依赖 {#executor-group-lifecycle}

**第一版执行器组不提供启用／停用，只做逻辑删除。** 不增加组 `status` 字段或组级暂停执行能力；Job 停用仅停止自动触发，不取消已有 Instance，也不禁止手动执行。删除组不是停止组内任务或关闭执行器进程的替代操作。

触发模块使用正式的 `job_executor_group` 表、Model 和 Repository，不另建临时组表，也不接受虚构组 ID。本批字段如下，替代此前不含生命周期字段的最小表草案：

| 字段 | PostgreSQL 类型 | 含义 |
| --- | --- | --- |
| `id` | `BIGINT`，Identity 主键 | 数据库技术主键，供 Job／Instance 关联，ID 不复用 |
| `name` | `VARCHAR(128)` | 执行器配置的组名与接入身份，创建后不可修改；管理列表仍可批量关联展示 |
| `created_at` | `TIMESTAMPTZ` | 创建时间 |
| `updated_at` | `TIMESTAMPTZ` | 记录最后更新时间；软删除时间单独由 `deleted_at` 记录 |
| `deleted_at` | `TIMESTAMPTZ`，可空，初始 NULL | NULL 为未删除，非空为删除时间 |

普通组读取和新引用排除已删除组；历史查询不要求关联当前有效组，必要时显式只读关联保留行。Job 列表仍使用批量 `LEFT JOIN` 读取组名，不能因组过滤把 Job 行意外丢掉，也不为每条 Job 单独查组。

删除沿用下节的[共同组行锁与引用保护](#group-reference-concurrency-proposal)：未删除 Job（含停用）、使用旧组快照的待开始／运行中／STOPPING Instance，以及尚未收敛的 Task 均阻止删除。锁后依次检查，通过后在同一事务使用 GORM `Delete` 设置 `deleted_at`，不额外补写 `updated_at`。已结束且无恢复依赖的历史不永久阻挡，不级联删除 Job、Instance 或 Task，不通过删除自动取消任务。

触发模块已用上述五字段正式组模型验证真实引用、组行锁及列表关联，尚未实现完整组管理或执行器接入。2026-11-02 已通过下述[正式组字段与最小管理契约](#executor-group-management-contract)，补齐发现 Key／Token、唯一性及四个入口；五字段表仍是当前代码事实，新字段与约束随实现落实，遵守迁移不可变规则和[产品版本兼容边界](../../development/database-migrations.md#release-compatibility)。组删除后的凭据与连接缓存失效、取消恢复依赖仍须在开放删除入口前实现验证，不承诺标记删除即所有副本同步失效或所有业务已停止。

### 正式组字段与最小管理契约 {#executor-group-management-contract}

**本项于 2026-11-02 通过，2026-11-04 按产品版本兼容规则修订字段完整性，尚未实现。** 在现有 `job_executor_group`、Base 和主键引用上补齐执行接入字段，不建立另一张组表或临时组模型。

| 业务字段 | PostgreSQL 类型 | 已定规则 |
| --- | --- | --- |
| `name` | `VARCHAR(128) NOT NULL` | 执行器接入组名，创建后不可修改；全表唯一，包含软删除记录，删除后不复用 |
| `discovery_key` | `VARCHAR(255) NOT NULL` | 人工填写的 etcd 服务发现 Key，创建后不可修改；全表唯一，包含软删除记录，删除后不复用 |
| `token` | `VARCHAR(32) NOT NULL` | 后端使用安全随机源生成 UUIDv4，去连字符后为 32 个小写十六进制字符；明文保存，不提供修改或重置 |

所有组记录必须具备完整字段，软删除保留原字段值。组名和 Key 非空，拒绝首尾空白，按大小写精确匹配；不自动改大小写、拼接 Key 或整理斜杠。执行器配置使用相同值；容量按既有 Unicode 字符计量规范校验，不静默截断。组名和 Key 分别由数据库唯一约束兜底，不使用仅覆盖未删除行的部分唯一索引，也不能用应用先查重替代唯一约束。普通读取仍按 GORM 软删除规则过滤。

全表保留身份是外部注册与本地缓存生命周期带来的取舍：删除组不要求旧执行器已经退出；复用 Key 可能将仍在原 Key 下注册的旧节点混入新组，使派发被旧组名／Token 拒绝。组名也是认证缓存的查询身份，立即复用可能碰到尚未刷新的旧配置。因此与普通字典编码的删除复用规则不同；代价是删除后再次接入必须使用新组名、新 Key，不提供原身份恢复或重建。该规则不改变同组扩缩容和进程重启，也不增加在线节点为零的删除门槛。

#### 最小组管理入口

| 操作 | 输入与结果 |
| --- | --- |
| 创建组 | 输入组名和 Key，后端生成 Token，保存成功返回组 ID；不要求当前已有在线执行器 |
| 组详情 | 按 ID 查询组信息及完整 Token，沿用详情权限和已定日志保护 |
| 组列表 | 分页并支持名称筛选；返回 ID、名称、Key 和审计时间，不返回 Token；可直接用于 Job 选组，不另建选项接口 |
| 删除组 | 按下节既定组主键锁和引用保护执行 GORM 软删除，不取消任务、不要求执行器全部离线 |

组名、Key、Token 均不可修改，没有其他可编辑字段，因此不增加组编辑入口。既定无组启停、无 Token 重置、详情可显示／复制和列表不含 Token 的边界保持。四个后端入口不等于完整组页面已经交付。

#### 组字段持久化

按上述完整字段落实非空及唯一约束，不增加历史 NULL 例外。新建组始终保存完整字段，软删除后保留原 Key／Token 及身份占用；数据库结构与测试数据均以该目标契约为准。

组结构实施遵循[产品版本兼容边界](../../development/database-migrations.md#release-compatibility)，不为当前开发中间态增加旧组回填工具、兼容分支或分阶段升级要求。服务启动不隐式迁移或修数据。

### 最小执行查询与停止管理契约 {#execution-management-contract}

**本项于 2026-11-02 通过，尚未实现。** 现有手动触发已经返回 Instance ID；普通执行阶段先提供以下三个管理操作，支持按轮次观察及人工停止，不等待完整历史列表和页面。

| 操作 | 输入与最小返回内容 |
| --- | --- |
| `GetInstance(id)` | 返回身份、触发信息、状态、开始／截止／结束时间、结束原因、摘要及本轮冻结配置 |
| `ListInstanceTasks(instanceId, page, pageSize)` | 按 Task ID 稳定排序分页，每页最多 200 条；返回类型、状态、执行编号、已用次数、运行时间、绑定身份／地址、结束原因及摘要 |
| `StopInstance(id)` | 请求人工停止该轮；返回已确认观察到的 Instance ID、状态和结束原因，不开放任意状态或原因写入 |

Instance 详情使用本轮冻结配置，不读取编辑后的 Job 替代；Job 删除也不影响仍保留的执行记录查询。Task 分页沿现有 page／pageSize 风格，不改成 cursor 接口；列表不加载业务 `result_data`，不新增整轮成功／失败等统计或整轮 Task 列表读取。Instance 不存在返回明确业务错误；存在但尚无 Task 时返回空列表。Task 最近摘要可能来自上一次尝试，不代表当前尝试已经完成；可空字段沿各自事实表达，不能将空值填成已开始、已结束或已有执行器绑定。

全局执行历史列表、Task 业务 Data 详情及完整页面留到后续管理模块，不增加 `GetTask` 或任意 Task 停止入口。所需分页查询与投影沿现有 Repository 方式落实，不引入 N+1 查询或新的查询 Service。

#### 人工停止响应

| 已确认的持久化事实 | 响应与后续行为 |
| --- | --- |
| 本次新保存人工停止决定并确认提交 | 返回 `STOPPING` 及 `manual_stop`；提交后按既定协议独立交接数据库取消和远端通知 |
| 已有 STOPPING | 如实返回已有状态及原因，不改成人工停止，也不因重复请求重新发送取消通知 |
| 已有终态 | 如实返回终态及原因，不宣称本次将其取消，不改写已有结果 |
| 数据库失败或提交不明 | 返回错误，由调用方查询或重试；不返回虚假的接受成功，不声称确定回滚 |

响应表达本次已确认观察的数据库事实，不保证随后状态不变。成功反馈不等待子记录全部终态、RPC 取消成功或业务退出；既定 STOPPING 收敛、原因冻结及原截止裁决保持不变。停止以 Instance 为单位，不将操作扩展为逐 Task 停止。

#### 管理边界与错误

上述组和执行管理操作仍经 `system-api` 的登录及权限检查调用 Job 管理 RPC；每个操作对应一个 Logic，普通查询直接使用本域 Repository。停止用例复用已定运行组件与持久化协议，不新增大 Service、管理鉴权机制或错误体系。

参数错误继续用 `common.invalid_request`。本轮新增稳定业务 subcode 的分类仅覆盖组不存在、组名冲突、Key 冲突、组仍被引用及 Instance 不存在；具体标识字符串和接口编码在开工附件中核对，不能视为已批准新的数字业务码。唯一冲突由 Repository 识别并在接口边界转换，技术错误保留原因链，不把数据库故障伪装成不存在、冲突或停止成功。已有 STOPPING／终态是正常状态反馈，不新增“重复停止错误”。

组字段、迁移及最小管理入口的[专项验收](verification.md#minimal-management-checks)与既有组引用、日志保护及停止验收一起实施。普通先开发和验收、完整执行能力待分片完成后正式发布的安排见[进度约定](progress.md#ordinary-integration-contract)；本项不授权生成协议、执行迁移或编码。

### 组引用与删除的并发保护 {#group-reference-concurrency-proposal}

**新增／完整编辑 Job 与删除执行器组，共用目标组的主键单行锁。只协调低频管理操作，不让 Cron 创建或 Task 派发争抢组锁。** 本节引用协调协议与删除门槛已通过；删除采用[已定逻辑删除](#executor-group-lifecycle)，不设组启停。凭据随完整快照更新，[旧客户端清理规则](operations.md#group-client-cleanup)已定，仍须连同取消恢复边界实现验证，不能据此提前开放完整组删除入口；Token 轮换不属于第一版前置条件。

#### 哪些操作需要组锁

| 操作 | 已确认顺序 |
| --- | --- |
| 新增 Job | 短事务内按主键锁目标组，确认存在且 `deleted_at IS NULL`，再插入 Job，持锁至事务结束 |
| 完整编辑 Job | 先锁请求中的目标组并校验，再按已定未删除且停用条件更新 Job；只锁新组，不再锁旧组，也不新增 Job 的显式读取锁 |
| 删除执行器组 | 先锁该组，再分别检查引用；满足删除门槛后，同事务使用 GORM `Delete` 设置 `deleted_at`，使该组不再可被新引用 |
| 启用、手动新建 Instance | 沿用 Job 行协调和管理组存在性校验，不再加组锁；未删除 Job 本身已阻止删组 |
| 停用或删除 Job | 不加组锁，不受组存在或在线情况阻碍 |
| 自动触发、Task 派发及回报 | 不因本项增加组行锁或逐次组表查询 |

组锁使用显式短事务内的单表主键等值 `FOR UPDATE`，继续采用 `READ COMMITTED`。编辑先锁目标组，再由条件 UPDATE 获得 Job 写锁；其他持有 Job 锁的路径不反向申请组锁。删组只做普通存在性查询，不逐条锁 Job，因此也不需要锁旧组或同时锁多个组。

删除方先取得组锁，再用后续独立 SQL 检查引用，不能把检查合并进取得锁的同一条语句快照。新增／编辑先提交，删除方就能看见引用并拒绝；删除先提交，后来的新增／编辑会发现组不存在或不可引用并拒绝保存。原事务回滚不留下半套引用，等待受请求 Context 约束，不在事务内访问 Redis、etcd 或执行器 RPC。

#### 删除门槛需要覆盖旧轮次

**所有未删除 Job 都阻止删组，包括停用 Job。** 启用和手动执行没有新建组引用，因此无需再锁组；仅判断“没有启用 Job”不足以支持这条协议。

另外，Job 改组后旧 Instance 仍使用原组快照；STOPPING 仍需原组信息完成数据库处理及尽力通知。因此删除还须阻止：

- 以该组为执行快照的待开始、运行中或 STOPPING Instance。
- 尚未结束的 Task 及其他仍有效的恢复依赖；正常 STOPPING 协议下由未结束父记录覆盖，不将历史终态父下未结束子行作为常态恢复模型。

使用按索引的集合存在性查询，不逐轮加载或统计全部历史。已经结束且无恢复依赖的历史记录不永久阻止删组；逻辑删除的 Job 不单独阻止，但它留下的运行／恢复依赖仍需检查。组 ID 不复用，历史查询不能要求组仍是当前有效配置。

不会因此要求自动触发也锁组：旧组 Instance 的创建与 Job 改组在同一 Job 行上协调。若先创建，改组要等其提交，删组随后可看到旧轮次；若先改组，后来的创建只会使用新组。删组持有组锁时，又不会出现新的 Job 改入该组。删除检查应先确认无未删除 Job，再发后续运行依赖查询，以观察上述已提交事实；不使用一条旧快照 JOIN 代替这个顺序。

#### 成本与实施边界

代价是同组的新增／编辑会串行经过一次短事务，低频管理保存多一次组行锁开销；不同组可并行。不增加引用计数器、Redis 锁、外键或周期扫描，也不要求等待历史保留期结束才删组。

引用协调、逻辑删除及本批组依赖已确认，不等于完整组删除功能已经可交付。运行依赖查询的 Task 字段／索引、取消恢复对组凭据的依赖，以及已定的[快照更新与客户端清理](operations.md#group-client-cleanup)仍须在执行／组模块落实；这些条件未落实时不开放组删除入口。Task 全部终态不证明最后一次尽力取消 RPC 已发送，更不证明所有业务线程物理退出，不能用上述存在性查询承诺零在途通信或强制停止。客户端清理不增加排空等待，允许在途调用因关闭受影响，但不能据此判定业务未执行或已取消。

验收须覆盖新增／删组两种提交顺序、并发改组、停用 Job 仍阻挡删除、改组后的旧轮次、STOPPING 阶段及全部子结束但父尚未终态，以及事务回滚；通过运行主路径无新增查组／加组锁的检查，见[引用并发验收](verification.md#group-reference-concurrency-checks)。竞品删除保护及并发边界见[组删除比较](references.md#group-deletion-concurrency-evidence)。

### Job 输入查询与错误契约 {#job-input-query-proposal}

**本包已通过。** 管理接口采用下列输入、读取和失败反馈契约，不改变触发规则或设计管理页面布局。短字符串类型及容量与[字段设计](#string-field-types)一致；名称采用不区分大小写的字面包含，列表只读轻量字段。普通新增／编辑的说明和参数不区分省略、null、空串，参数统一去首尾空白；复用传输层报文限制，不另设独立参数长度配置。竞品依据见[管理查询比较](references.md#job-input-query-evidence)。

#### 输入及字段存在性

| 输入 | 已确认校验 |
| --- | --- |
| 名称 `name` | 去首尾空白后必填，最多 128 个 Unicode 字符，与现有字典名称长度一致 |
| 说明 `remark` | 去首尾空白，允许空串，最多 500 个 Unicode 字符，沿用项目常用说明长度 |
| 处理器 `processorName` | 去首尾空白后必填、区分大小写，最多 128 个 Unicode 字符；不强制只能是 Go 函数名，不新增处理器远程探测 |
| Cron `cronExpr` | 去首尾空白后必填，最多 128 个字符，与 VARCHAR(128) 容量一致；仍经统一六字段解析与 Next 校验，长度不能代替语义校验 |
| 业务参数 `params` | 统一 `strings.TrimSpace` 去首尾空白，允许空串，全空白归为空串；保留内部空格与换行，不强制 JSON、不重新编码；不另定 2KB／64KB 参数上限 |
| ID、数值和策略 | ID 为正数；重试次数非负；重试间隔和整轮超时为正数；枚举必须属于已定集合，普通模式不能开启 Post。不另拍 1 小时、100 次等业务上限；类型范围和时间运算不得溢出 |

上述短字段长度在 API／域后端、GORM 模型和数据库 VARCHAR(n) 同步落实，不增加重试或超时等业务范围 CHECK；VARCHAR 是有上限的变长字符串，不是定长 CHAR。解析后的字段值须能由 PostgreSQL UTF-8 字符串／JSONB 表示，拒绝 NUL 及仍然非法的 UTF-8，不把数据库写入报错当作常规输入校验；不替换现有 JSON 解析器来保证原始 HTTP 字节逐项严格校验。

HTTP 管理请求沿用 go-zero 的整体请求体限制，当前依赖默认 1 MiB；内部 RPC 保留框架报文限制，当前 gRPC 默认接收上限为 4 MiB。两者计量不同，不将“请求体 1 MiB”宣传为“参数恰好可以填 1 MiB”，也不为了大参数关闭保护。未来调大入口上限须一起检查 RPC、单轮快照和加载成本；接近入口上限不保证加入详情／派发封装后仍可传输，后续执行模块须按实际序列化报文设计分批与拒绝反馈。这里只约束传输，不新增队列总字节配额或业务结果长度配置。

普通新增／完整编辑的 `remark`、`params` 沿用用户管理备注的简化方式：API／RPC 使用普通字符串，省略、null、空串均归为空串，编辑通过固定字段 map 明确覆盖，省略不保留旧值。域后端统一对字符串去首尾空白，API 及页面若做预处理必须保持同一规则；参数内部内容不变，不只依赖前端 trim。

存在性仅在业务语义确有差异时保留：例如新增重试次数省略／null 使用默认 3，显式 0 表示不重试；列表状态省略不筛选，0 表示停用。完整编辑的其余必需配置不得遗漏或传 null，不套新增默认值；必填字符串归空后也必须拒绝。需要区分的字段由 API 可表达存在性的类型及 RPC proto3 `optional` 保留，不把所有字符串机械改成指针。

手动覆盖仍独立校验：`paramsOverridden=false` 时 params 可省略或为 null，但不能提供字符串，包括空串及全空白；为 true 时必须提供字符串，省略／null 拒绝。先检查这项组合，再对合法覆盖值 trim；空串与全空白都表示明确覆盖为空，不表示沿用 Job。规范化后的参数用于保存快照及同请求号内容比较，不重写旧快照。真实解析路径须覆盖缺失、null、零值和空串；不修改生成 Handler 或注册自定义 Validator Tag。

#### 筛选与读取

| 项目 | 已确认契约 |
| --- | --- |
| 列表输入 | `page/pageSize` 沿用现有规则，page 至少为 1，页大小 1～200；可选 `id`、`name`、`executorGroupId`、`status`，组合条件为 AND |
| 名称筛选 | 去首尾空白，空串不筛选；最多 128 字符，按字面包含、不区分大小写。SQL 参数化 `ILIKE`，转义 `%`、`_` 和转义符，不把用户输入当通配表达式 |
| 其余筛选 | ID／组／状态等值；省略代表不筛选，状态 0 仍代表停用，不能当成“全部” |
| 排序 | 固定 `id DESC`，不开放任意列排序；正常有界分页，不将历史 Instance 混入列表 |
| 列表投影 | `id/name/executorGroupId/executorGroupName/processorName/executionMode/cronExpr/status/nextTriggerAt/createdAt/updatedAt`；不查 `params/remark`、执行历史、实时节点或运行统计 |
| 详情投影 | 按 ID 返回全部可编辑配置及上述管理信息；不返回组凭据，不以详情读取触发重载或执行 |

列表先查总数，再查本页投影，组名称通过单次 LEFT JOIN 读取 `job_executor_group.name`；组定义缺失或组侧有效行过滤不能使 Job 行从列表消失，不改变两条查询、无 N+1 的约束。具体 SQL 汇入开工总审。两条查询使用正常 `READ COMMITTED`，接受并发变更时总数与本页短暂不同，不加事务锁强求一致快照。

仅启用 Job 的 `nextTriggerAt` 返回计划时间；停用时列表／详情投影为 null，数据库仍保留原进度，不改写该值。其他可空时间也返回 null，不用 0 或空串；时间点采用带偏移的 RFC 3339，保留已有精度。

为未删除 Job 补一条 `(executor_group_id, id DESC) WHERE deleted_at IS NULL` 索引，服务组内分页和已批准的删组引用 EXISTS。既有五条触发索引保持；第一版不为名称包含搜索增加 trigram 扩展／GIN，也不声称普通 B-tree 能加速 `%关键词%`。默认列表按主键倒序；状态和名称过滤可能扫描候选记录，计数也有成本，在 Job 配置表规模下验证，不用执行历史表的数据量替代分析。

#### 错误及重试反馈

沿用统一响应，不逐字段新增错误码。参数错误使用 `common.invalid_request`；下表业务原因使用 `job` 域稳定 subcode，最终名称在接口清单集中列出，仍使用现有业务 Code，不另开数值码体系。

| 情况 | 必须可区分的反馈与客户端含义 |
| --- | --- |
| Job 不存在或已删除 | 不存在，不从创建入口重放已删除 Job 的历史成功 |
| 启用中编辑、启用中删除 | 当前状态不允许，重新读取后由用户处理，不自动停用再提交 |
| 活跃 Instance 阻止删除、阻塞策略拒绝手动执行 | 分别反馈删除被占用、触发被阻塞，不在后台补操作 |
| 管理入口引用的组不存在或不可引用 | 组不可用；不能把组内暂无在线执行器算作此错误 |
| 同手动请求号的覆盖标记或内容不同 | 请求内容冲突，不复用旧请求号表达新操作 |
| 手动创建额度已满 | 明确容量繁忙，不在后台排队；重试只能沿用同一请求号和内容 |
| 数据库、RPC、超时等技术失败 | 沿用现有技术错误映射，不将未知结果包装成业务拒绝或确定回滚 |

普通新增 Job 响应不明不能安全自动重试；手动创建沿用既有有限请求防重。提交不明不新增第二套后台操作重试器，已提交保存也不因 etcd 通知失败改报失败。

输入表、查询投影和错误语义、[持久化交接](#trigger-durable-handoff)及[本批最小组依赖](#executor-group-lifecycle)均已确认并在触发模块落实；SQL／API／Proto、验收映射及 ADR 接入细则已完成本批实现验证。当前开始普通 Task 执行的必要详细设计，不重新逐字段审批已交付的触发契约；完整组管理和页面按各自依赖推进。

### 触发模块的持久化交接 {#trigger-durable-handoff}

**触发模块交付的是已提交的 WAITING Instance 及完整的本轮执行配置，不是内存队列中的一条消息。** 复用 `job_instance`，不增加交接表、Outbox 或持久化消息队列。自动创建仍与 Job 进度同事务保存，手动创建仍与请求身份同事务保存。

交接时 `started_at`、`deadline_at`、`finished_at` 保持 NULL，不启动整轮超时倒计时，不创建假 Task 或自动把 Instance 改成完成。首批没有业务推进消费者，WAITING Instance 仍参与同 Job 的阻塞判断；测试以前轮已终态等受控数据覆盖相应场景，不修改产品规则迁就演示。

确认提交后可以向本节点后续推进组件发送仅含 Instance ID 的有界提示；未确认提交时不凭本地推测发送。提示丢失、满载拒收或进程退出，不改变已持久化的交接事实；后续执行模块通过架构 B 已有的有界恢复发现遗漏，不为触发模块另建一套消费者或扫描循环。

后续候选读取先有界取得 Instance ID，再按批加载必要配置，或在有界批次内一次投影取得；不得逐 Instance／Task 补查配置，也不能从当前 Job 重建旧轮次的配置。候选读取不是领取成功或执行许可，多节点可以读到同一候选。普通路径已确认先取得本地派发容量，再在单 Instance 父行保护的短事务内，将首次开始、截止时间、RUNNING 状态、唯一普通 Task 及首次执行信息一起保存；Task 直接进入执行中并占用一次执行机会，明确提交成功后才发送 RPC，详见[普通任务主路径](execution.md#ordinary-flow)。不建立整轮负责人，也不先提交待执行 Task 再另开首次登记事务。公共字段和单例唯一索引见[Task 契约](#task-field-contract)，[普通首次创建与重复推进 SQL](#ordinary-start-sql)已通过；其余状态写入、恢复索引及分片路径的首次推进继续细化。

不能把 `id > cursor` 作为永久消费水位：自增 ID 分配顺序不等于事务提交顺序，较小 ID 可能晚提交。恢复须重新覆盖尚未推进的持久化状态，临时分页不能永久越过仍未处理的记录。沿用本批已定的待开始 Instance 索引；具体候选读取和扫描预算随执行模块补齐，普通首次推进复用上面的已定 SQL，不照搬 Leader 的 Job 恢复扫描规则。

对应[交接验收](verification.md#trigger-handoff-checks)验证数据库事实、通知故障与配置隔离；触发交接已实现并测试，后续消费者的批次读取、首次开始和业务执行仍由执行模块落实，不能把现有交接测试当作完整执行链路通过。

## 6.4 跨模块接口边界

接口按[独立 RPC Server 边界](architecture.md#rpc-boundaries)注册：Job 管理接口位于调度端管理 Server，结果回报位于同进程的执行器通信 Server，派发与取消位于业务进程中的执行器专用 Server。接单 ACK 是派发响应，不单独增加回报接口；管理 Logic 直接调用进程内组件，不通过 RPC 转发到本进程的回报入口。管理 RPC 的服务鉴权沿用 go-zero 原生配置，不属于组 Token 的校验范围。

| 交互 | 调用与处理职责 | 已确认的应答含义 |
| --- | --- | --- |
| 实例注册与发现 | go-zero 直接对接 etcd，由 etcd 身份及读写范围授权；不新增调度端注册 RPC | 注册成功不等于组 Token 校验通过，也不证明任务执行健康 |
| 执行器组管理 | 页面维护组与服务发现标识，调度服务保存配置并使各副本动态应用 | 不要求新增组时修改或重启调度服务；配置保存与各副本应用的确认边界仍待设计 |
| 任务配置管理 | 已有 Job 仅停用时可编辑，后端校验并与启用协调；编辑不重设持久化进度，启用时原子保存新进度，提交后通过 etcd 通知 Leader 核对数据库 | 编辑成功不表示已经启用；通知发出不等于 Leader 已应用，每分钟核对兜底 |
| Task 派发 | 原生组内负载均衡调用统一入口；接单响应提供启动实例标识，调度端取得实际地址并条件保存当前尝试的绑定 | 接单只确认接收，不等待业务完成，也不表示调度端已保存绑定；BUSY 明确表示容量满且未接单、未入队、未执行；响应缺失不能推断 BUSY，按既有不确定规则处理 |
| 结果上报 | 执行器公共组件通过有界结果队列、独立有限并发的回报处理，每次 RPC 向任意调度节点的执行器通信 Server 提交一个 Task 当前尝试的结果；后台校验并保存 | 一次响应确认本条结果，区分已接收、稍后重传和永久拒绝；已接收只表示入队，不表示已落库或状态已采纳；提交后才推进 |
| 取消请求 | 按 Instance 向各已知目标进程定向通知；每次目标通知临时持有直连客户端，仅在本次有限重试内共用，结束后关闭，不跨通知缓存；请求保留 InstanceID、组认证信息及取消原因，不携带或强制校验预期进程实例 ID | 成功只表示本次取消请求已处理，无匹配活动也成功，不证明业务已退出；永久错误不重试，通信故障有限重试同一目标，见[取消响应契约](#cancel-response-contract) |

第一版不定义业务内部实时进度上报或 Notify 回调契约，也不提供配置任意 HTTP／RPC 目标执行的接口；不新增 Task 活动查询或心跳协议。结果回报、平台分片统计及业务 Post 仍属于已确认范围，不能因 Notify 后置而省略。

服务发现决定向哪个实例发送请求；处理器名称决定那个实例执行哪段代码。统一入口是执行器共同实现的协议，不是新增一个转发单点。处理器名称、参数和结果的校验及兼容规则，仍然需要显式约定。

公共执行组件承接接单、业务排队、有限并发执行、上下文、协作取消与结果回报；业务处理器返回结果，不手写回报 RPC。尝试授权、预算记账和状态推进由调度服务负责，不下放给处理器。

明确 BUSY 不消耗 Task 执行重试次数，零重试仍可延期；调度端按当前有效尝试条件返还发送前已登记的机会，并复用 `next_dispatch_at` 和既有待处理检查退避派发，不新增队列或扫描。延期不延长整轮截止，下一次派发使用新尝试身份且不保证换节点。通用 RPC 超时、断连、Unavailable 或丢失的 BUSY 答复不构成返还依据，详见[BUSY 背压](execution.md#busy-backpressure)及[普通 BUSY 条件更新](#ordinary-busy-settlement)。

接单后保存绑定通常增加一次行更新，首版由固定派发工作协程直接执行[已确认的绑定 SQL](#executor-binding-sql)，不新增绑定合批缓冲。已有结果或后续尝试不得被迟到绑定覆盖。进程实例标识不是组凭据，RPC 对端地址也不总是可定向访问的执行器地址；本版直连前提见[绑定设计](execution.md#executor-binding)。[绑定缺失规则](execution.md#missing-binding)已经确认：从登记的数据库时间有限观察，到期仅获条件结算资格，结果可先于绑定生效；下一次新执行另经父行授权。[普通结果条件更新](#task-result-persistence)与[普通未知结算 SQL](#ordinary-unknown-settlement)已通过；[派发时间预算](execution.md#dispatch-time-budgets)已确认观察期默认 30 秒、绑定保存阶段 4 秒及其有限重试初值，[观察到期扫描与结算安排](execution.md#unbound-recovery-defaults)已另行通过，实现验证仍待完成。

派发和取消校验目标组及组 Token；回报入队前完成组认证及报文校验，后台写入前再绑定持久化 Task 所属组、当前有效尝试及更新前状态条件。所属组以本轮持久化配置／Task 关联为准，不能用编辑后当前 Job 的组替代；组凭据不复制进 Instance 快照，第一版不提供已有组 Token 更换。认证失败不得因请求来自内网而放行；接收 ACK 不能被当作跨组修改或旧尝试推进的许可。认证模型及[60 秒组缓存](operations.md#group-cache)已确定，回报的[错误分类](#result-error-classification)已通过，[协议字段及认证装配](#execution-rpc-contract)已通过，[非 BUSY 明确拒绝结算](#dispatch-rejection-settlement)已通过，[派发／取消错误码映射](#dispatch-cancel-errors)已通过；完整边界见[总体架构](architecture.md#internal-trust)和[依赖故障与恢复边界](operations.md#dependency-boundaries)。

第一版固定双端异步，不增加同步回报开关、本地回报文件或持久化消息队列。[结果上报契约](#result-report-contract)采用单条发送、一次响应确认本条结果，不设置网络合批窗口或批内部分接收协议。重传沿用同一 Task、尝试及原结果，不消耗执行预算；单次 RPC 2 秒、应用层最多 7 次总调用及带抖动的间隔已确定为独立内部常量，见[回报初值](execution.md#reporting-defaults)。收到接收 ACK 后释放对应结果；永久错误或重传耗尽也释放，但须记录错误及丢弃指标，不直接更新 Task 状态或重新执行业务。详见[回报重试与清理](execution.md#report-retry)。[已绑定结果等待](execution.md#bound-result-wait)沿用执行中状态，等待有效结果、人工停止或整轮截止，不新增 Task TTL、等待结果状态或内存等待对象。

调度端 ACK 后的[写库故障保留策略](execution.md#scheduler-result-retention)已通过：每条一次 JOIN 读取必要 Task／Instance 信息，再条件 UPDATE、独立提交；不要求批量预读，不设置批级总超时或超时后拆批降级。默认保留 **512 条**（含首次待处理、退避和在途）、保存并发 **8**、一次处理 **2 秒**，均部署可调；512 不额外叠加 8 条在途额度，不建立跨业务阶段统一总量。数据库技术错误、原因不明或提交不明时，只保留对应原结果并退避，不回滚或重放其他已明确提交的结果；正常完成、无错误零行或明确永久无效才结束保留。内部退避初值为 1／2／4／8／16／30 秒、后续名义间隔封顶 30 秒，各带 ±20% 抖动，不按次数或 TTL 丢弃，退避不占保存并发。按到期最早、同时间稳定接纳顺序处理，不切首次／重试配额，满时拒收。共享 30 秒停机宽限、进程退出可能丢失结果及 RPC 报文边界不变，不新增总字节配额或独立结果长度限制。B2 总审及[本轮保存初值](execution.md#scheduler-result-defaults)已通过，[回报错误分类](#result-error-classification)已通过，具体驱动适配与 Go 实现仍须核对，不混同 BUSY 的有限记账重试。

### 执行 RPC 协议与装配 {#execution-rpc-contract}

**2026-10-30 已通过，尚未实现。** 调度端和执行端的 Server、Client 统一使用 zrpc，注册发现继续使用 etcd；不新增执行器注册、心跳或节点目录中心。本节确定公共协议的消息表达与装配边界，不授权生成协议或执行模块编码，也不改变已定的队列、并发、重试、持久化和终态规则。

#### 服务、包与生成边界

公共契约位于 `pkg/jobexecutor/protocol/execution.proto`，同目录保存官方 protoc 生成的 `execution.pb.go` 与 `execution_grpc.pb.go`。`protocol` 是独立 Go 包，只依赖协议所需类型，不导入父目录执行运行组件或任何应用域的 `internal`；导入协议不会自动引入执行器。此包只生成 protobuf／gRPC 绑定，不生成应用 Main、Config、ServiceContext 或 Logic 脚手架。已批准的生成规范例外仅适用于此共享执行协议；管理 RPC 和 HTTP 契约继续使用原固定 goctl 命令，详见[代码生成规范](../../development/code-generation.md)。

| 服务与方法 | 调用方向 | 响应含义 |
| --- | --- | --- |
| `Executor.DispatchTask` | 调度端 Client → 执行器 Server | 接单 ACK 是本次派发 RPC 的响应，经同一连接返回；不等待业务完成 |
| `Executor.CancelInstance` | 调度端 Client → 已保存地址的执行器 Server | 本机取消操作已处理；无匹配活动也正常回复，不等待业务真正退出 |
| `ResultReceiver.ReportTaskResult` | 执行器 Client → 调度端执行通信 Server | 成功入内存队列才回复接收 ACK；不证明已落库或状态已采纳 |

调度进程内管理与执行通信 Server 独立；System 内原业务与执行器 Server 独立。管理操作直接调用进程内组件，不转发到本进程回报 Server。派发按组复用发现客户端，回报复用执行通信入口客户端；取消只在一次目标通知及其有限重试内复用直连客户端，结束后关闭。监听、发现目标及凭据来源沿用[已定配置布局](architecture.md#execution-configuration)，不固定回报到原派发节点或 Leader，不改为单条双向流模型。

#### 共同认证上下文

三个请求均包含请求体字段 `auth`，其 `GroupAuth` 消息包含 `group_name: string` 和 `token: string`。由通信组件填充，业务处理器不构造认证信息或回报消息；组名保留已有 Unicode 字符串语义，凭据仍使用已定的 32 位无连字符 UUIDv4 格式。

派发、取消对照执行器本机配置校验目标组和 Token；回报入口使用调度端的[组缓存](operations.md#group-cache)认证，未命中按原规则合并回源，查询失败不伪装成凭据错误。回报认证后保留可信组上下文供后台归属核验，入口不查询 Task 表，后台不反复校验 Token。

组认证覆盖三个执行业务入口，调用运行组件前共用认证逻辑；不采用要求任意 Server 请求均有 GroupAuth 的全局拦截方式，也不增加跳过某个执行业务方法的白名单。新增执行通信入口不叠加 go-zero 原生 App／Token 鉴权，保留框架标准 Health，不注册反射；Health 不携带组凭据，也不代表业务能力已经就绪。管理和原业务入口保持各自鉴权配置。三个业务方法关闭自动请求／响应正文日志，手工日志不记录 Token 或完整报文；具体装配见[RPC 边界](architecture.md#rpc-boundaries)。

#### 派发消息

| 字段 | 类型与语义 |
| --- | --- |
| `auth` | 上述组认证上下文 |
| `job_id`、`instance_id`、`task_id`、`attempt_no` | 均为 `int64`，定位已登记的当前执行尝试 |
| `processor_name` | `string`，本轮冻结的具名处理器，沿用来源字段长度约束 |
| `params` | `string`，本轮冻结的业务参数，不读取当前 Job 或在执行侧改写 |
| `deadline_at` | Protobuf `Timestamp`，有效的原整轮绝对截止时间，不改成每次重置的相对时长 |
| `required_post` | `bool`，本轮是否要求处理器具备 Post 能力，用于接单校验；普通任务不得为 true |
| `input` | `oneof`，明确选择普通、准备、分片或收尾的一种阶段输入 |

`input` 的 `ordinary`、`prepare` 分支分别是明确存在的空消息；`shard` 携带当前 Task 分到的 JSON 工作项子数组；`post` 仅携带 `Total / Succeeded / Failed` 分片统计，语义沿用[Post 参数](#post-input)。JSON 工作项使用原始 JSON 字节表达。缺失或无法识别的分支必须拒绝，不能默认普通任务；Post 分支本身必须检查 Post 能力。

阶段由 oneof 唯一表达，不再并列传递同义 `task_type`。数据库 Task 类型字段及既有编码保留，由协议适配层显式映射，不能以 Proto 字段编号推导数据库类型。协议可表达四个阶段，不代表分片工作项存储、生成封口和阶段推进事务已经获准实现。

派发成功响应明确区分 `ACCEPTED` 与 `BUSY`，枚举零值及未知值为无效响应。`ACCEPTED` 必须携带合法的 `executor_instance_id`，采用带连字符的标准 UUIDv4 字符串，每次进程启动生成一次，接单与回报使用同一进程身份；不与组 Token 混用。地址不放入响应，继续由调度端本次派发调用的 `grpc.Peer` 获取并按[绑定协议](#executor-binding-sql)核验、保存。

只有表示容量满且确定未接纳、未排队、未执行的 `BUSY` 才能进入既定条件返还。通用 ResourceExhausted、Unavailable、超时或无效响应不能推断 BUSY；接纳不表示结果已经完成，业务结果仍可能先于 ACK 到达。

#### 结果回报消息

请求包含 `auth`、四个 `int64` 身份字段 `job_id / instance_id / task_id / attempt_no`、`executor_instance_id`、`outcome`、`summary: string`，以及可选 `oneof output`。有效 outcome 为 `SUCCESS / FAILURE`，由执行组件根据处理器返回结果生成；业务 Result 不增加重复的成功标志，也不能用枚举默认值推定执行成功。

| 输出表达 | 适用规则 |
| --- | --- |
| 不设置 `output` | 普通、分片及 Post 成功时可选 Data 未提供，合法；与显式 JSON null 分开 |
| `data_json: bytes` | 原始有效 JSON 字节，不经 Struct／Value 转换数字；保留大整数及合法 JSON 类型。选择分支却提供空字节不是有效 JSON |
| `prepared_items` 消息，内部为 `repeated bytes` | Pre 成功必须存在此消息；消息存在且数组为空表示明确的零分片，消息缺失不能默认为空数组。每个元素是工作项的原始 JSON 字节 |
| 业务失败 | 不采用 Data 或部分工作项，按已定合法业务失败回报处理 |

没有 Data 与 JSON `null` 分别表达 SQL NULL 与 JSONB null；不把合法 JSON 等同于一定可保存为 JSONB，继续执行[永久无效证据与错误分类](#result-error-classification)。Pre 输出与普通 Data 不混用，不从任意 Data 猜测工作项，也不为 Post 增加分片结果明细或查询接口。

`ReportTaskResult` 正常响应为空消息，成功入队后返回即为本条接收 ACK；不新增回报收据、重复 ID 或第二次确认。身份、阶段输出与持久化事实的后台核验仍使用已定查询与条件更新，入口不为此查询 Task。重传、无效结果、提交不明和内存丢失边界按既有回报契约执行。

#### Instance 取消消息与后续边界

`CancelInstance` 请求仅包含 `auth`、`instance_id: int64`、`reason`，有效原因为 `MANUAL_STOP / INSTANCE_TIMEOUT`；不将默认或未知原因当成有效停止决定。不增加 Task 列表、尝试编号或预期进程 ID。成功响应只确认已完成本机取消操作，无匹配活动同样成功，不等待业务退出，不改变父子数据库终态或既定有限重试。

进程装配采用[统一配置来源与共享停机宽限](operations.md#process-shutdown)：框架入站 RPC 停服与已接纳后台工作有限善后分别协调，`RpcServer.Stop()` 不是完成屏障，退出回调不能反向等待 `zrpc.Start()`／`ServiceGroup.Start()` 返回，全局日志最后由宿主关闭。不得以设计通过宣称现有代码已满足这些要求，具体实现须覆盖 Unix 与 Windows。

**[非 BUSY 明确拒绝的当前尝试结算](#dispatch-rejection-settlement)已另行通过。** 可识别的接单前拒绝按原预算立即结算为等待重试或最终失败，不返还机会，也不统一等待未知观察；具体错误表达按[已通过映射](#dispatch-cancel-errors)执行。Proto 字段号、源文件生成、适配与实现验证在获准实现时落实，不以消息表达通过宣称整个执行模块可开工。验收见[执行 RPC 协议与装配检查](verification.md#execution-rpc-checks)。

### 派发请求与接收 ACK {#dispatch-acceptance}

**派发消息、接单语义和活动期有限防重已通过。** 一次 RPC 只派发一个 Task 的当前执行尝试，普通、准备、分片和收尾共用入口。不提供批量接单或部分接收协议；调度端仍可有界并发发送，数据库仍可按既定规则批量登记。

| 请求内容 | 语义 |
| --- | --- |
| JobID、InstanceID、TaskID、当前尝试标识 | 确定本轮工作及执行身份；尝试身份由框架管理，不要求业务参与回报协议 |
| 处理器名称、Task 类型、必要的处理器能力要求 | 定位本地处理器并选择对应方法，校验本轮所需能力，不静默跳过已要求的 Post |
| 本轮 Params、阶段专属输入 | 普通和准备使用公共输入；分片另带当前工作项子数组；Post 另带分片汇总统计，不带分片明细 |
| 本轮 deadline_at | 沿用已持久化的整轮截止时间，不随派发、排队或重试重置预算 |

组身份与 Token 用于调用认证。不发送整条 Job，也不让执行器查询调度数据库补全输入；消息表达按[执行 RPC 契约](#execution-rpc-contract)执行，Proto 字段编号与生成在获准实现时落实，剩余拒绝错误及结算继续闭合，不改变本轮配置来源。

执行器先校验组凭据、报文、处理器及能力要求，已过截止时间的请求不得开始业务；再检查同一尝试是否已在本进程活动，最后尝试交给有界业务等待队列。只有成功接纳后才返回接单成功，不能先确认再尝试入队。业务异步执行，不等待完成才返回 ACK，也不保证业务开始或结束一定晚于 ACK 到达。

[执行器处理流程](execution.md#executor-processing-flow)采用有界业务等待队列、有限业务处理并发、有界结果保留和独立有限回报 RPC 并发。普通、准备、分片和收尾在每执行器进程内共享 **32 个业务处理并发、128 条业务等待队列**，均为可调整的部署初值，见[执行器业务初值](execution.md#executor-business-defaults)。32 包含已经领取并开始处理、直至完成结果交接的工作；128 仅包含尚未领取的 Task，不含这 32 个处理中工作。允许排队后 ACK，不要求接单时立即开始业务；队列用于吸收短时突发，不增加长期吞吐，也不延长整轮截止。

业务队列无法接纳时，仅对本次确实未接单、未入队、未执行的请求返回 BUSY；结果积压通过业务处理者等待交接传回业务队列，不增加跨业务阶段统一总量配额或独立拥塞水位控制器。回报组件保留上限与 RPC 并发沿用[已定初值](execution.md#reporting-defaults)，不要求业务并发 32 与回报 RPC 并发 16 相等。Go 内部组织仍结合完整流程落实，业务并发不等同于操作系统线程数，不改变下面的重复活动尝试与停机拒绝规则。

| 响应或通信结果 | 调度端含义 |
| --- | --- |
| 已接单 | 响应提供本进程 executorInstanceId，结合实际对端地址条件保存绑定；不表示业务成功或绑定已落库 |
| 明确 BUSY | 业务等待队列无法接纳，且本次未接单、未入队、未执行；沿用机会条件返还、关闭尝试和延期规则 |
| 其他明确拒绝 | 可识别且确认未接纳的处理器缺失、能力不匹配、非法输入或认证失败等，按[拒绝结算](#dispatch-rejection-settlement)立即关闭当前尝试，有机会等待重试、耗尽才失败；不返还或重复扣次，不再等待无绑定观察 |
| 停机中拒绝新接纳 | 可明确识别且确认未接纳的拒绝按[拒绝结算](#dispatch-rejection-settlement)处理，不当作容量 BUSY；通用不可用、退出或地址消失本身不能证明未接单或授权立即重派，具体错误表达按[已通过映射](#dispatch-cancel-errors)执行 |
| 超时、断连等无明确答复 | 不推断为拒绝或 BUSY，沿用绑定缺失观察与已有绑定等待规则 |

业务 Context 独立于派发 RPC 请求生命周期，受本轮截止和取消信号控制；接单 RPC 返回或超时不取消已经接纳的业务。接单等待默认 3 秒，受[单次派发 10 秒剩余总预算](execution.md#dispatch-time-budgets)及停机预算限制，不能解释为业务必须在三秒内结束。结果可以先于接单 ACK 或绑定提交到达，不能因绑定尚未保存而拒绝当前有效尝试的结果；保存绑定失败只重试元数据，不重新调用业务。

#### 活动期有限防重

复用执行器已有的排队／执行活动记录，以 `(TaskID, 当前尝试标识)` 识别重复接纳。同一进程内该尝试仍在排队或执行时，重复请求返回已接单及本进程身份，不重复入队或启动另一份业务，也不因队列已满改报 BUSY。并发检查与接纳必须共同保证本机只形成一份活动执行。

业务实际退出后释放活动记录，不等待结果交接完成或回报 ACK。结果缓冲满时，原业务处理者继续持有已完成结果等待交接，仍占业务处理并发，期间不领取新工作；这不代表原业务仍在运行，也不延长活动防重。例如 20 个正在执行业务、12 个业务已结束但等待交接，已占满默认 32 个处理位置。仅发出取消信号不等于业务退出，不能提前按已经退出处理或归还处理位置。第一版不保存已完成尝试的长期去重记录，不使用本地文件或跨重启去重；记录释放、换节点或重启后仍可能重复执行，业务幂等责任不变。

派发 RPC 超时、绑定观察期及 ACK／BUSY 有限保存重试已由[派发时间预算](execution.md#dispatch-time-budgets)统一确认；go-zero 原生客户端／调用超时和执行器 Server 方法超时须协调，不能只设置外层 Context 而被框架默认的 2 秒提前截断。执行器[业务并发与等待队列](execution.md#executor-business-defaults)、[回报保留、并发和重传](execution.md#reporting-defaults)及[调度端结果保存](execution.md#scheduler-result-defaults)采用各自已定初值，不从派发秒数推导；无绑定扫描周期、分页及逐条结算按[已批准安排](execution.md#unbound-recovery-defaults)执行，具体实现仍待验证。结果上报契约见下节；取消协议、Task DDL 和业务编码不因接单语义通过而一并批准。验收见[派发接单验收](verification.md#dispatch-acceptance-checks)及[时间预算验收](verification.md#dispatch-time-budget-checks)，既有对照见[派发与接单依据](references.md#dispatch-acceptance-evidence)。

### 明确非 BUSY 拒绝的当前尝试结算 {#dispatch-rejection-settlement}

**2026-10-30 已通过，尚未实现。** 普通、准备、分片及 Post Task 共用本节接单拒绝规则：收到能确认本次未接纳的非 BUSY 拒绝后，立即尝试结算当前执行尝试，不再等待 30 秒无绑定观察期；有剩余执行机会则等待重试，耗尽才最终失败。这里确定共同拒绝分支，不因此批准分片生成、阶段推进或执行模块编码。

#### 明确拒绝的证据

必须是执行入口在接纳之前按约定返回、可通过错误码及稳定 subcode 识别的拒绝，例如处理器不存在、所需能力不满足、请求不合法，以及能够明确识别的认证失败或停机拒绝。不解析错误文案，不仅凭通用业务错误数字码推断语义。各拒绝原因与具体错误编码按[已通过映射](#dispatch-cancel-errors)处理，未识别的业务错误仍不推断为明确拒绝。

通用 `Unavailable`、`ResourceExhausted`、超时、断连、未知错误或无效 ACK 不能证明本次未接单，也不能推断为 BUSY，继续遵守原有不确定性处理。只有已定的专用 BUSY 响应才可能条件返还机会；本节不扩大返还范围。

#### 处理归属、条件与状态转换

由收到拒绝响应的当前派发处理者直接保存，使用原发送请求的 `TaskID`、`InstanceID`、`attempt_no` 及派发时已有的本轮冻结重试配置，不另查当前 Job 配置。沿用 GORM 单条条件更新和显式字段映射，同时满足：原 Task 与 Instance 身份一致、执行编号仍为原编号、Task 仍为执行中，以及 `executor_instance_id` 和 `executor_address` 均为空。不增加父行显式锁，不先查后无条件更新，不清空已有绑定来强行采用拒绝。

状态、结果及相关时间在同一次条件 UPDATE 中原子保存；是否耗尽以当前行的 `used_attempts` 与本轮冻结的总执行机会比较，不用 `attempt_no` 或旧读取次数代替。

| 当前预算 | 状态 | 结果字段 | 时间与原因字段 |
| --- | --- | --- | --- |
| 仍有执行机会 | `status=3`，等待重试 | `result_data=NULL`，`result_summary` 保存本次接单拒绝的诊断 | `next_dispatch_at` 为本次保存的数据库处理时间加本轮重试间隔，`finished_at=NULL`、`end_reason=NULL` |
| 执行机会耗尽 | `status=5`，失败 | `result_data=NULL`，`result_summary` 保存本次接单拒绝的诊断 | `finished_at` 为本次保存的数据库处理时间，`end_reason='dispatch_rejected'` 表达最终接单拒绝，`next_dispatch_at=NULL` |

本次机会已经在派发登记提交时占用，拒绝结算不返还、不额外扣一次，也不改变 `attempt_no`、首次开始或当前登记时间；维护 `updated_at`，但不将审计时间用于重试判断。不增加按拒绝原因直接判为不可重试的分类，统一服从本轮有限预算；明确拒绝耗尽机会时的结束原因为 `dispatch_rejected`，对应 RPC 错误表达见[已通过映射](#dispatch-cancel-errors)。

默认首次加 3 次重试共 4 次执行机会，重试间隔 10 秒。第一次收到处理器不存在的明确拒绝，形成等待重试候选；连续四次均被拒绝且没有 BUSY 返还时，第四次最终失败。零重试时第一次拒绝即可最终失败。摘要须准确表达“接单被拒绝”及原因，不能写成处理器已经运行后的业务失败；自有技术诊断使用英文，不记录 Token 或完整报文。

#### 并发裁决及父 Instance 边界

已有绑定、有效结果、取消、未知结算或新尝试先改变相应条件时，旧拒绝不得覆盖。无数据库错误但更新零行即结束，不追加查询猜测原因、不换成最新编号重做，也不宣称本次结算成功。状态离开执行中后，重复拒绝不能刷新重试时间或重复结算；有一项绑定非空的异常数据同样不强行处理。

本次实际更新一行并确认提交后，可提示本节点检查对应 Instance；不直接从拒绝保存路径再派发。等待重试仅形成候选，后续仍须取得容量并重新检查父 Instance、数据库时间及剩余预算，登记成功才增加执行编号并占用下一次机会，不保证改派其他节点。

本节不另读锁父，父停止与子结算仍可能交错；拒绝保存不得恢复父状态或绕过停止决定，未终态子继续按既有取消规则收敛。即使执行器明确回复已过截止，也只说明本次拒绝，不能直接据此把父 Instance 判为超时；由调度端既有完成／超时路径以数据库时间裁决，不延长原截止。

#### 保存预算与失败后的接续

本类拒绝复用[派发响应有限保存预算](execution.md#dispatch-time-budgets)：从处理该响应起最多 4 秒，最多 3 次保存尝试（含首次），单次数据库操作最多 1 秒，失败间隔 200 毫秒；同时受整个派发剩余总预算及共享停机剩余宽限限制，不保证尝试满三次。保存使用独立于已结束接单 RPC 的处理上下文；依赖操作实际退出前不能提前释放其占用的处理能力。

仅可恢复数据库故障或提交确认不明时，有限重做原拒绝的同一条件写入；不重新发送 `DispatchTask`，不增加执行机会。明确保存成功或无错误零行即结束。预算耗尽释放原响应并记录保存失败，不能据此宣称事务回滚，也不撤销可能已经提交的状态。

若拒绝未落库且仍符合条件，后续由既有无绑定观察与结算接续；已经提交的状态按等待重试或完成检查接续。接受明确拒绝诊断可能未能持久化、并因此延后结算的代价，不新增可靠响应队列、专用扫描器或长期保留策略，不套用已 ACK 业务结果的保留规则。

本节只记录已批准的结算策略、条件及预算；[错误码映射](#dispatch-cancel-errors)已通过，实现与数据库故障验证仍待完成。对应[拒绝结算验收](verification.md#dispatch-rejection-checks)是后续要求，不代表测试已经运行。

### 派发与取消的错误码映射 {#dispatch-cancel-errors}

**2026-10-30 已通过，尚未实现。** 本节将已批准的[明确拒绝结算](#dispatch-rejection-settlement)及[取消响应](#cancel-response-contract)对应到具体错误表达，不改变执行预算、响应保存、取消通知预算或父子状态机。普通、准备、分片与 Post 共用派发入口规则；结果回报继续采用独立的[已定错误分类](#result-error-classification)。

#### 派发接口

| `DispatchTask` 结果 | 接口表达 | 调度端处理 |
| --- | --- | --- |
| 已成功接纳 | 正常响应 `ACCEPTED`，携带本进程身份 | 按既定条件保存绑定 |
| 业务等待队列满，且确定未接纳 | 正常响应 `BUSY` | 按既定条件返还机会并延期 |
| 凭据缺失或 Token 错误 | 标准 gRPC `Unauthenticated` | 进入已定明确拒绝结算 |
| 目标组不允许访问 | 标准 gRPC `PermissionDenied` | 进入已定明确拒绝结算 |
| 身份字段、阶段输入等违反报文契约 | `bizerror` 默认码 `100001`，subcode 为 `common.invalid_request` | 进入已定明确拒绝结算 |
| 处理器不存在、能力不满足、接单前发现执行期限已过、明确未就绪或退出拒收 | `bizerror` 默认码 `100001`，subcode 为 `job.executor.dispatch_rejected` | 进入已定明确拒绝结算 |
| 超时、断连、通用技术错误、未知 subcode 或无效 ACK | 保留实际错误，不转换为明确拒绝 | 不推断未接单，继续已有不确定性处理 |

仅新增一个执行协议 subcode `job.executor.dispatch_rejected`，复用现有默认业务码和标准 `ErrorInfo.Reason`，不新增数字错误码、自定义错误 Detail 或新的错误框架。这些拒绝对调度端具有相同处理动作，具体原因通过英文 Message 诊断，程序不解析 Message，不仅凭通用业务码 `100001` 推断拒绝。

标准认证错误按其 gRPC Code 识别；扩展业务错误按既有 `bizerror` 及稳定 subcode 识别。通用 `Unavailable`、`ResourceExhausted` 或 `DeadlineExceeded` 均不等于专用 BUSY 或确定未接纳，不把它们附带的任意文本解释为派发拒绝。各方法只采用自身契约中定义的分类，不能把派发拒绝标识用于取消或把回报队满直接套为派发 BUSY。

#### 接纳前保证与截止含义

上述明确拒绝只能由接纳前的判断产生。已经接纳后发生技术故障，或无法确定是否接纳时，不得伪装为 `job.executor.dispatch_rejected`。已接纳且仍在本机活动的重复请求继续遵守原有防重契约，不能因为队满或退出改报拒绝；业务处理器执行失败通过结果回报表达，不成为派发 RPC 的拒绝错误。

执行器明确判断本轮 `deadline_at` 已过时，使用 `job.executor.dispatch_rejected`；调度端等待 RPC 超时得到的 `DeadlineExceeded` 仍表示结果不确定。前者也不能直接决定父 Instance 超时，完成／超时仍由既有路径结合数据库时间裁决。

#### 取消接口

| `CancelInstance` 结果 | 接口表达 | 调度端处理 |
| --- | --- | --- |
| 本机取消操作完成，包括没有匹配活动 | 正常空响应 | 结束该目标通知 |
| 凭据错误或目标组不允许 | 标准 `Unauthenticated`／`PermissionDenied` | 记录错误，结束该目标通知，不重发同一错误请求 |
| Instance ID、取消原因等非法 | `bizerror` 默认码 `100001`，subcode 为 `common.invalid_request` | 记录错误，结束该目标通知，不重发同一错误请求 |
| 临时不可用、超时、断连或未知错误 | 保留实际技术错误；未识别的业务错误同样不视为永久拒绝 | 在原预算内重试同一地址 |

没有活动不返回 `NotFound`，取消接口不使用派发专用的 `job.executor.dispatch_rejected`。取消可能在整轮超时之后发生，不能因原执行期限已过而拒绝；取消请求不新增执行 deadline 字段。正在关闭新接单的节点，如果仍能完成本机取消就正常处理；入口已停止服务时按通信失败处理，不承诺停机期间始终可访问。

单次 RPC 1 秒、最多 3 次总尝试、间隔 1 秒，单目标共 5 秒、整次 Instance 通知共 60 秒，以及停机剩余宽限保持不变；成功或明确永久错误提前结束，不保证用满次数。取消 RPC 的成败不改变数据库停止决定，也不阻止父子状态收敛，不将通信错误写成 Task 或 Instance 的终态原因。

#### 标识归属与持久化原因

执行协议专用的 `job.executor.dispatch_rejected` 在既定公共协议包 `pkg/jobexecutor/protocol` 统一声明，调度端和执行端复用；不新增 errors 包，不让执行器导入 Job 域的私有 `internal/subcode`，也不移动管理业务的既有错误标识。`common.invalid_request` 继续引用现有公共定义。协议标识属于对外契约，运行组件内部错误仍按原职责归属并在接口边界转换；记录阶段不创建或生成协议、常量或运行代码。

当明确拒绝耗尽执行机会时，Task 的 `end_reason` 统一保存 `dispatch_rejected`，具体拒绝原因保存到 `result_summary`；等待重试时 `end_reason` 仍为 NULL。该持久化原因适用于上述明确拒绝结算，包括认证及参数拒绝，不直接复制 RPC Code 或 subcode，不改变其他 Task／Instance 结束原因的定义。

本节仅完成错误表达设计，不宣称现有管理 RPC 已按本节实现，也不顺带改造它们。协议生成、接口适配及故障验证仍待获准实现后落实；[专项验收](verification.md#dispatch-cancel-error-checks)为后续实现要求。

### 结果上报请求与接收 ACK {#result-report-contract}

**结果上报采用单条异步发送，消息、接收确认、有限结果保留与到期重传、参数初值和永久交付失败处理已通过，尚未编码。** 业务处理器返回后，由公共执行组件将执行结果交给调度端，不要求业务编写上报 RPC；这不是派发接单响应、Post 或过程日志上传。

| 单条内容 | 含义 |
| --- | --- |
| Job、Instance、Task、当前尝试及执行器进程实例身份 | 关联本次执行，重传保持同一 Task、尝试和结果，不新增回报 ID |
| 本次成功或失败结论 | 由业务返回值、error 或框架捕获的 panic 转换；业务失败本身是正常的上报结果，不属于非法报文 |
| 阶段输出 | 普通、分片和 Post 使用既定 Result；准备使用 PrepareResult，保留显式空数组与缺失的区别 |
| 简短失败原因或摘要 | 用于诊断，不上传完整过程日志，不改变业务 Data 与摘要的分工 |

一次请求只携带一个 Task 当前尝试的结果；组认证在请求层完成，不替代后台的 Task 归属校验。

#### 单条异步发送与接收确认

业务完成结果交给有界回报组件，由独立、有限并发的发送者逐条发送。组件默认保留 **256 条**，包括待首次发送、延迟重试和在途结果；回报 RPC 并发默认 **16**，二者是独立部署参数，不能另加 16 条在途结果到保留上限之外。每条结果从组件接纳到 ACK、永久错误或重试耗尽始终占原位置，发送者只负责一次 RPC，调用结束即释放发送并发，不在退避期间持有发送者。完整职责见[执行器回报队列](execution.md#executor-result-queue)。

首次可发送时间为组件接纳时刻，重试可发送时间为本次调用完成时刻加正退避；在已到期记录中优先选择可发送时间最早的一条，同时间使用稳定的本地接纳顺序。后来接纳的新结果不能插到已到期旧记录前，失败后等待下一次到期再参与发送，同一条在途记录不能被再次选中。不为凑批额外等待，不为每条结果创建无约束协程，也不预先指定 Go 容器实现。更新既有结果或释放位置不等待新结果准入，失败记录保持原位置，不让发送者阻塞回填已满 channel。

业务处理器不负责网络回报、不等待远端 ACK；回报组件保留位置满时，当前有限业务处理者持有这一份已完成结果等待交接，期间不领取下一项，不另起无界后台任务等待。活动防重仍按实际业务退出清理。全部已保留记录都在延迟重试时，发送者可以空闲，而新结果仍须等待交接；服务恢复后也可能等待剩余退避，不能宣称立即排空。交接完成后业务处理者再继续工作，不建立从业务接单到回报结束的统一额度；停机等待遵守共享 30 秒宽限。

一次上报对应一次请求和一次响应，响应只确认本条结果，不包含批内应答数组，不另发反向确认 RPC。应答关联本次 Task／尝试，只有得到明确接收确认，才能按已接收释放该结果。

| 应答语义 | 执行器处理 |
| --- | --- |
| 已接收 | 该条已进入调度端有界内存队列；释放，不再等待数据库确认 |
| 稍后重传 | 该条未被接纳，例如调度端结果队列已满；沿用该条已有有限上报额度，退避重传 |
| 永久拒绝 | 入口已经确定的身份字段或结果格式等永久报文错误；记录错误及丢弃指标后释放，不反复发送 |

各条结果独立确认、重传和释放，不维护批内部分接收或重组逻辑。RPC 超时、断连、暂时过载或应答丢失时，未获确认的结果保留在组件内，沿用原有有限额度安排下次发送，不把失败结果移入无界缓冲，也不通过重新接纳重置额度。每次调用最多 **2 秒**，应用层最多 **7 次总调用（含首次）**，六次重试间隔依次为 **1／2／4／8／16／16 秒，各带 ±20% 抖动**，均为内部常量。**47 秒仅为名义退避之和**，不含排队或 RPC 耗时，不是结果 TTL 或交付 SLA。go-zero 原生客户端／调用超时及调度端回报方法超时须与单次 deadline 协调，停机时还受共享剩余宽限限制，不保证用完全部调用；规则以[回报初值](execution.md#reporting-defaults)和[重试与清理](execution.md#report-retry)为准。

调用超时不证明远端未接受结果，重传可能使同一结果重复入队，后台仍用既有条件写入防止重复更新；不增加持久化回报收据、二次确认或长期结果缓存。停止等待或耗尽回报预算不等于 Task 业务失败，不消耗或重置 Task 执行重试预算。

这些是交付应答，不是新增 Task 状态。调度端结果队满拒收只导致重传已有结果，不是派发 BUSY，不返还执行机会、不创建新尝试、不调用业务处理器。

单条网络上报与调度端的后台逐条保存及[按 Instance 合并完成检查](execution.md#completion-check)职责独立：后台每条一次 JOIN 核验必要关系，再条件更新、独立提交，不要求批量预读；完成检查合并重复的结束检查提示，不要求多条结果共用事务或执行器合批发送。第一版承担每条一个 RPC 的成本，换取独立的确认与重传路径；后续仅在压测证明上报 RPC 成为瓶颈时另行评估有界顺手合批，不预设批量模式或等待窗口。

#### 校验与不可发送结果

接收入口只进行组认证、报文基础校验和结果入队，不查询 Task 表。旧尝试、跨组 Task 引用、已取消 Task 等依赖持久化事实的判断由后台完成；已经收到“已接收”的结果仍可能随后被判为无效。ACK 不表示状态机已采纳，后台提交前也不能生成分片、创建 Post 或结束 Instance。

结果大小遵守实际 RPC 报文上限，不新增独立业务结果长度配置。单条结果超限，或存在不可修复的格式错误时，记录错误及丢弃指标后释放，不静默截断为成功结果、不无限重传、不直接重新执行业务。必须区分确定的永久报文错误与队满、临时过载、网络故障等暂时失败，不能仅按一个通用资源不足状态就断言报文永久无效。永久交付失败不能伪装成已落库，也不能直接把数据库 Task 改成失败。

原有易失结果取舍不变：执行器未完成上报就退出，或调度端确认接收后、落库前退出，都可能丢失结果；已有绑定时等待有效结果、人工停止或整轮超时，无绑定及结果时仍按观察与条件结算规则处理。单条确认也不能恢复丢失的业务真实结果。调度端对已接收结果的可恢复写库故障仍有界保留并退避，不套用执行器的有限重传释放规则。

[普通 Task 结果条件更新与状态转换](#task-result-persistence)、执行器回报保留及重传初值、[调度端保存与退避初值](execution.md#scheduler-result-defaults)已通过；[回报错误分类与无效结果处理](#result-error-classification)已通过；[执行 RPC 协议与装配](#execution-rpc-contract)已另行通过，Proto 生成、驱动适配及后台 Go 装配仍须实现验证，不扩展为新的可靠消息系统。[取消响应与失败处理](#cancel-response-contract)、接入配置、组依赖与[有限停机规则](operations.md#process-shutdown)均已确认；通信消息表达及装配设计已按[执行 RPC 契约](#execution-rpc-contract)收口，[非 BUSY 拒绝结算](#dispatch-rejection-settlement)已通过，[派发／取消错误码映射](#dispatch-cancel-errors)已通过，实现验证继续落实，不因设计通过而授权编码。对应验收见[结果上报验收](verification.md#result-report-checks)，既有调研见[结果上报依据](references.md#result-report-evidence)。

### 结果回报错误分类与无效结果处理 {#result-error-classification}

**2026-10-30 已通过，尚未实现。** 本节收口普通 Task 执行闭环中回报入口、执行器重传及调度端后台保存的错误判定；不改变已定 ACK、次数、队列、并发、状态机及易失结果边界，也不将本节编码直接推广为派发 BUSY 或取消接口的完整错误契约。同日后续已通过[执行 RPC 协议与装配](#execution-rpc-contract)，生成、适配与运行验证仍待完成。

#### 回报入口与执行器判断

| 回报结果 | 接口表达 | 执行器处理 |
| --- | --- | --- |
| 已成功入队 | 正常接收 ACK | 释放该结果；只确认内存接纳，不证明状态已采纳或已落库 |
| 调度端结果队列已满 | 标准 gRPC `ResourceExhausted` | 沿用既定次数和间隔有限重传同一结果 |
| 节点尚未就绪、正在退出，或认证所需的数据库查询失败 | 标准 gRPC `Unavailable` | 沿用既定规则有限重传，不重新执行业务 |
| 已明确确认凭据错误或无访问权限 | 沿用 System 的标准 gRPC `Unauthenticated`／`PermissionDenied` | 记录错误及丢弃指标，结束该条重传 |
| 必填身份字段、结果格式等明确违反契约 | 复用 `bizerror` 与稳定 subcode `common.invalid_request` | 记录错误及丢弃指标，结束该条重传 |
| 超时、断连、未知错误码或无法确认接收的异常响应 | 按通信失败处理 | 保守地有限重传；仍服从原调用额度及停机剩余宽限 |

分类依据为错误码和稳定 subcode，不解析错误文案，也不能仅凭通用业务错误数字码将未知错误视为永久拒绝。认证失败必须有明确结论：组缓存未命中继续按已定规则回源，未命中本身不是凭据错误；回源失败属于暂时不可用。入口仍只做认证、基础报文校验和入队，不为分类查询 Task 表。后台查询使用入口已认证的组上下文，不反复校验 Token。

#### 永久无效所需的证据

实际要发送的 Data 不是合法 JSON，或本地已经确认报文超过实际发送上限时，可以结束发送并记录明确原因。仅收到通用 `ResourceExhausted` 时无法区分队满、限流和报文超限，仍按暂时失败有限重传；不能解析英文 Message 推断永久超限，也不新增独立业务结果长度或字节额度。

合法 JSON 不必然能保存为 PostgreSQL JSONB，例如 JSON 字符串中的 `\u0000`。必须明确定位到结果内容，并按实际存储限制独立验证，才能判为永久不可保存；仅有数据库错误码、SQLSTATE 大类或多次失败不足以证明结果内容无效。不能独立确认时，仍按技术故障保留并告警，不宣称已经覆盖全部不可保存数据。

可选 Data 未提供仍然合法，不强制为 JSON 对象。业务返回失败时不采用其 Data，不能因为本来就不使用的 Data 非法而丢掉合法失败回报。业务返回 `error` 或框架捕获到处理器 panic，属于合法的执行失败结果，应正常保存并按原执行预算处理；回报通信失败只重传原结果，不重新调用业务。

#### 调度端 ACK 后的保留与释放

| 已取得的后台事实 | 处理 |
| --- | --- |
| 条件更新生效且确认提交 | 释放该条结果，按原规则提示 Instance 检查 |
| 已明确是旧尝试、Task 已结束或绑定身份不匹配 | 结束该条处理，不用旧结果覆盖当前状态 |
| UPDATE 无数据库错误但影响零行 | 结束该条处理；不补查猜测原因，不宣称此前保存成功；已核实归属后的提示仍按原结果保存契约执行 |
| 成功查询后确认无合法目标、跨组引用或所属关系不符 | 不写入，释放该条并记录诊断；不能据未经核实的报文 ID 触发 Instance 推进 |
| 已定位结果内容并独立确认其永久无效 | 释放该条并记录明确原因，不将丢弃视为业务失败已保存 |
| 数据库技术故障、提交不明或尚不能确定性质的错误 | 保留原结果并按既定规则退避，不重放其他已明确提交的条目 |
| 仍需使用的本轮配置损坏或版本无法识别 | 按平台处理故障保留并告警，不能据此认定执行器结果无效 |

先利用已有单条 JOIN 的事实排除明确失效的结果，再解析仍需使用的本轮配置，避免旧结果因坏快照一直占据保留位置。不增加逐条关联补查或诊断查询；前置核验不能替代原条件 UPDATE。这里的终态指 Task 终态，父 Instance 进入 STOPPING 或超过截止时间不单独成为丢弃有效子结果的依据，不新增父必须 RUNNING 或尚未到期的结果写入条件。

每条异常独立处理，技术故障退避期间不占保存并发。日志记录定位信息和原因，不输出 Token 或完整业务结果；正常旧结果与需要修复的平台错误应能区分。平台持续缺陷仍可能让无法处理的结果占满默认 512 个保留位置，此时告警并向入口传递压力，不靠猜测、次数上限或 TTL 丢掉已 ACK 的结果。不新建隔离队列、持久化收据或错误框架，也不在此固定 Go 并发组织。具体故障分类与驱动适配仍须实现验证，验收见[回报错误分类检查](verification.md#result-error-classification-checks)。

### 取消处理与响应语义 {#cancel-response-contract}

**执行器完成本机取消操作后回复，不等待业务代码真正退出。** 请求沿用已定的 InstanceID、组认证信息及取消原因，按本轮执行器组与已知目标地址分别通知，不携带预期进程 ID；目标选择和直连客户端生命周期见[取消路由](execution.md#cancel-routing)。

执行器通过组 Token、目标组及参数校验后，只处理现有有界活动记录中属于该 Instance 的任务：

| 本机情况 | 处理方式 |
| --- | --- |
| Task 仍在排队 | 标记取消，后续不再启动业务处理器 |
| Task 已经运行 | 取消业务 Context，由业务代码配合退出 |
| 没有匹配活动 | 不做操作，同样正常回复，不将未找到视为失败 |
| 已经请求过取消 | 重复处理安全，不新增等待对象 |

排队转运行与取消标记必须协调：取消先发生就不启动业务，启动先发生则按运行中的任务协作取消。发出取消信号不代表业务已经退出，不能据此启动额外工作绕过处理并发。业务实际退出后清理活动记录；若有待回报结果，处理者仍按[执行器流程](execution.md#executor-processing-flow)完成交接后再领取下一项，取消响应不等待这一过程。

成功响应只表示“本次取消请求已处理”：本次找到的活动已标记取消或已发出取消信号，没有匹配活动也属于成功。不以仅收到报文代替完成这些本机操作，也不返回要求调度端持续追踪的“正在停止”状态或新增取消查询接口。

| 通知结果 | 调度端处理 |
| --- | --- |
| 正常响应，包括没有匹配活动 | 结束该目标的本次通知 |
| Token、目标组或参数明确错误 | 按[错误码映射](#dispatch-cancel-errors)识别 `Unauthenticated`／`PermissionDenied` 或 `common.invalid_request`，不重复发送同一错误请求，记录错误 |
| 超时、断连、临时不可用或未识别错误 | 未获成功确认，在有限额度内重试同一目标，不换节点、不广播 |
| 重试耗尽或调度进程停止 | 结束通知、释放客户端，记录未确认结果 |

同一次目标通知的有限重试复用该次直连客户端，结束后关闭，不跨通知缓存。取消重试不消耗 Task 的业务执行重试次数；初值为每次 RPC 1 秒、最多 3 次总尝试（包含首次）、间隔 1 秒；单目标从开始处理起共 5 秒，覆盖建连和重试等待，一次 Instance 通知从本地接纳起共 60 秒，覆盖排队、查址和发送。各次处理取剩余预算与单次上限的较小值，不保证尝试满三次；不套用 Task 执行重试参数。详见[通知容量与预算](execution.md#cancel-notification-capacity)。

取消响应不改写 Instance 停止决定，也不是最终结束的前置条件；它不替代子 Task 的[条件收敛](behavior.md#task-convergence)。成功响应不证明业务已经退出或副作用停止；回复前已授权、回复后才到达的派发仍可能形成新的本机活动。第一版不为此新增长期取消缓存、可靠通知表或保证送达机制，保留尽力取消的故障窗口。

本节处理与响应语义已通过。[STOPPING、统一子取消及最后父终态](#instance-termination-sql)已通过，取代原父先终态设计；正常完成仍仅处理 RUNNING；地址查询、通知来源及通信初值已另行通过；[取消消息表达与装配](#execution-rpc-contract)已通过，[错误码映射](#dispatch-cancel-errors)已通过，Proto 生成及实现验证仍须落实，不增加停止确认宽限期或授权执行模块编码。验收见[取消处理与响应验收](verification.md#cancel-response-checks)。

## 6.5 参数与执行记录

### 处理器接口与数据语义 {#processor-contract}

**接口分工、输入输出语义及共同执行约束已通过，本项处理器契约评审收口。** 普通、准备、分片和可选收尾共用执行组件，不分别实现派发、取消和回报。Post 只接收公共输入与分片汇总统计，不提供分片明细或结果查询能力；业务自行查询所属业务记录。[派发接单](#dispatch-acceptance)、[结果上报](#result-report-contract)及[取消处理与响应](#cancel-response-contract)的语义已通过；接入配置、已定通信参数及[执行 RPC 表达与装配](#execution-rpc-contract)按各自批准项执行；分片持久化等剩余事项仍须闭合，不代表执行模块已完成或获准编码。

```go
// Processor executes an ordinary task.
type Processor interface {
    Execute(ctx context.Context, input Input) (Result, error)
}

// ShardingProcessor prepares work items and executes each shard.
type ShardingProcessor interface {
    PreProcess(ctx context.Context, input Input) (PrepareResult, error)
    ShardingProcess(ctx context.Context, input ShardInput) (Result, error)
}

// PostProcessor performs optional business finalization.
type PostProcessor interface {
    PostProcess(ctx context.Context, input PostInput) (Result, error)
}
```

上面表达已通过的方法分工，不是已经生成或发布的公共包。普通业务只实现 `Execute`，分片业务实现 `PreProcess` 和 `ShardingProcess`，需要收尾时再实现 `PostProcessor`。同一分片业务仍按一个名称注册一个处理器对象，由框架按 Task 类型选择方法，不要求业务判断阶段，不增加三套阶段寻址或空方法实现。

| 输入 | 已确认内容 |
| --- | --- |
| `Input` | `JobID`、`InstanceID`、`TaskID` 使用 `int64`，`Params` 使用 `string`；参数来自 Instance 的本轮配置，不读取当前 Job，不在执行侧再次改写 |
| `ShardInput` | 组合公共 `Input`，另带当前 Task 分到的工作项子数组；Job 参数与工作项分别表达，不固定为一项数据一个 Task |
| `PostInput` | 仅公共 `Input` 和本轮分片汇总统计；不提供分片明细、结果集合或查询入口，详见[参数语义](#post-input) |

尝试身份由框架管理，用于回报校验与关联日志；业务不自行构造回报消息。工作项由业务定义含义，可以表达 ID、范围或数据引用，不传递数据库模型、连接句柄或任意 Go 运行对象。各阶段以及重试可能在不同执行器进程执行，不以同一处理器名称保证同进程或共享内存。

| 输出 | 已确认内容 |
| --- | --- |
| `PrepareResult` | 显式工作项数组 `Items` 和可选摘要；工作项不藏在通用 `Result.Data` 中供框架猜测 |
| `Result` | 可选业务数据 `Data` 和可读摘要 `Summary`；普通、分片执行及收尾共用，不增加与 `error` 重复的成功／失败字段 |
| 工作项与业务数据 | 使用标准库 `json.RawMessage` 表达已编码的有效 JSON；`Items` 是其数组。业务显式编码、解码，不以 `any` 自动恢复跨进程类型 |
| 摘要 | 使用 `string`，用于执行结果说明，不是详细过程日志 |

结果 Data 的 JSON 语法有效性不代替实际 JSONB 可存储性；明确不可保存所需证据及失败结果不采用 Data 的规则见[回报错误分类](#result-error-classification)。不新增独立结果长度配置，也不以未知数据库错误推定业务输出无效。

`error == nil` 表示本次业务处理成功，普通、分片执行和收尾允许返回零值 `Result`；可选 `Data` 未提供不构成失败。`error != nil` 表示本次失败，不采用返回的业务数据作为成功输出，Pre 失败也不采用部分工作项。失败原因仍进入平台执行结果记录，但不作为分片明细传给 Post；不增加部分成功状态或业务自行控制重试的返回字段。

准备必须明确提供工作项数组：显式空数组是零分片成功，缺失（Go 侧为 nil）或非法数组不得默认为零分片成功。有效空数组仍按已定规则生成零分片完成事实，并在配置 Post 时提供零计数。序列化与 RPC 契约必须保留这个区别，不能靠解码默认值把缺失变为空数组。复用已有报文保护，不因采用 JSON 增加独立业务结果长度配置或承诺无限报文；准备数组暂存、完整原数组保留及生成容量仍在分片模块闭合。

#### 注册与共同执行约束

- 普通和分片使用类型明确的注册入口，共用名称防重规则；可选 Post 能力不自行开启收尾，调用仍由本轮 `post_enabled` 决定。配置要求 Post 而处理器没有实现时必须明确拒绝，不能静默跳过；管理保存仍不远程探测处理器。注册函数的具体命名、能力不匹配的协议错误在接入契约中落实。
- 处理器可被并发调用，成员适合保存连接或业务服务等依赖，每次执行的临时数据放局部变量。跨阶段数据通过已定输入输出或业务持久化传递，不能在 Pre 中写对象成员，再期待其他进程的分片或 Post 读取。
- 方法返回表示本次业务工作结束，不能后台继续处理却提前返回成功；内部并行工作须在返回前完成。同一 Task 的重试及不同 Instance 的业务效果仍由业务幂等保证。
- 业务 Context 独立于短派发 RPC 的请求生命周期，沿用本轮截止时间，停止通过取消信号协作退出；不为每次调用重发完整超时。取消不证明业务已退出，执行槽位不能仅因发出取消信号就视为空闲，实际占用及退出资源仍须按容量与生命周期协议管理。
- 正常可预期失败返回 `error`。公共组件在处理器调用边界恢复当前 goroutine 的 panic，记录错误和堆栈并按失败处理，不将异常当成功；该保护不覆盖业务另起 goroutine 的 panic，不承诺强杀或所有进程故障均可恢复。
- 处理器不直接维护调度表、不手写回报、不决定平台重试预算。返回成功不等于结果已落库或 Instance 已成功，最终结果仍受当前尝试、停止及整轮超时规则约束。

对应实现验收见[处理器契约验收](verification.md#processor-contract-checks)，竞品依据见[处理器与结果传递参考](references.md#processor-interface-evidence)。本节不引入 SnailJob 的多层 Map、Reduce／MergeReduce 或业务进度回调；普通与分片仍沿用[任务模型](task-model.md)。

#### Post 参数语义 {#post-input}

**Post 的业务收尾职责和以下参数语义已通过；第一版普通任务不支持 Post。** 分片流程正常推进时，先完成分片执行，再运行已配置的 Post，最后由平台确定 Instance 终态；Post 不是 Instance 结束后的统一通知，也不是停止或超时后必定执行的清理钩子。业务用途见[业务收尾](task-model.md#business-post)。

| 参数 | 含义与用途 |
| --- | --- |
| `JobID`、`InstanceID` | 沿用公共 Input，识别所属任务与本轮执行；可用于关联业务批次及业务幂等 |
| `TaskID` | 当前 PostTask 的 ID，不是某个分片的 ID |
| `Params` | Instance 保存的本轮业务参数，例如业务批次 ID、统计日期；不改读当前 Job |
| `Total` | 本轮业务分片总数 |
| `Succeeded` | 最终成功的业务分片数 |
| `Failed` | 重试结束后仍最终失败的业务分片数 |

计数仅包含业务分片 Task，不包含 PreTask、PostTask，不表示业务数据行数。正常进入 Post 时没有仍待执行或重试的分片；合法零分片场景提供零计数。参数不包含 Instance 的最终状态，Post 自身的执行结果仍会影响平台终态判定。方法单独接收 `context.Context`，沿用整轮剩余时间和取消规则，不增加一份独立的 Post 时间预算。

Post 不接收每个分片的 TaskID、Status、Data、Summary、失败原因或尝试历史，不定义 `Results` 数组、结果访问器、分页读取方法或新增结果查询 RPC。框架也不为 Post 查询业务表、组装文件清单或传输业务明细。

需要报告、错误明细或文件清单时，由分片业务保存必要的业务记录或产物，Post 根据 Params 中的业务批次标识或业务采用的 InstanceID 关联方式，查询自己的业务存储完成收尾。平台不规定业务表结构，不根据 Task 计数推导业务行数，也不要求业务处理器直接读取调度表。

这一边界只限定 Post 输入，不取消其他处理器已定的 Result 返回约定、执行结果回报、平台状态汇总或管理端执行记录查询。

### 执行记录

Instance、Task 保存状态、执行时间、执行预算及简短结果或错误摘要，详细文本过程日志不入库。日志使用 `job_id`、`instance_id`、`task_id`、当前尝试身份关联，必要时携带 traceId；旧尝试详情到日志系统查询。Task 的 `started_at` 为首次登记时间，`attempt_registered_at` 为当前编号登记时间，`finished_at` 为 Task 终态时间，见[字段契约](#task-field-contract)；这些时间不冒充执行器业务耗时。普通有效结果覆盖摘要，重试登记保留最近说明，不将其当作当前尝试结果，见[重试摘要规则](#ordinary-retry-registration)；[未知结算](#ordinary-unknown-settlement)及[明确 BUSY](#ordinary-busy-settlement)产生新平台诊断时覆盖最近说明。[停止超时与子取消](#instance-termination-sql)使用新的平台诊断；[正常完成摘要](#instance-completion-contract)按单例复制或分片统计生成，数据库和日志的保留期继续设计。

处理器业务 `Data` 采用 `result_data JSONB`，记录摘要采用 `result_summary TEXT`，不把两者混成详细过程日志。可选 Data 允许未提供，不强制为 JSON 对象；公共持久化字段已通过，序列化细节与实际迁移仍待实现，不因本节批准提前编码。平台级业务检查点不属于当前第一版交付承诺，若要纳入需另行评审范围。

## 6.6 后续设计与完成条件

本章依赖[任务模型](task-model.md)、[行为规则](behavior.md)与[可靠性协议](execution.md)，不先写表结构再反推语义。

详细设计按模块落实已确认的状态与生命周期：触发模块先将已定触发身份和区间规则落实到 Job／Instance 数据、配置结构和交接事务；执行尝试、生成封口、派发／回报／取消契约在对应模块实现前闭合。每批同时完成自身所需 DDL、索引和接口，不先冻结全系统全部表结构，也不把跨模块依赖留到联调才决定。

每批开工条件是：本模块适用的恢复判断有持久化依据，输入输出及跨模块交接明确，并发约束能落实到事务和验收预期；未达到条件的模块不进入业务编码。首批按[触发模块开工清单](progress.md#trigger-readiness)评审，通过后即可实现，不要求 Task 执行、分片生成及完整管理接口同时设计完成。最终所有模块都须满足本章契约要求。
