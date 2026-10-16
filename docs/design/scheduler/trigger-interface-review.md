# 触发模块接口总审

本附件将已通过的 [Job 管理契约](data-contracts.md#job-management-proposal)落实为本批 HTTP 和 RPC 清单。接口名称、消息字段号、错误原因码及进程接入细则已于 2026-10-16 随触发模块总审通过；随后用户明确授权编码，契约生成及实现验证按本附件推进。

交付止于 Job 管理、Cron 预览和已提交的 WAITING Instance。组管理、凭据、执行器派发、结果回报、停止及执行历史查询不在本批接口中；不通过临时接口假装执行闭环已经完成。

## 接入位置与权限

| 层次 | 本批落点 | 职责 |
| --- | --- | --- |
| HTTP | `apps/system/api/desc/job.api`，由唯一入口 `system.api` 导入 | 复用 system-api 的认证、权限、解析和响应包装；不新增 job-api 进程 |
| RPC | `apps/job/rpc/job.proto`，`package job`，`service Job` | 独立 job-rpc 调度进程；各副本都提供管理 RPC，只有 Leader 承担自动触发 |
| 数据和用例 | `apps/job/internal/model`、`repository` 及 RPC Logic | job 域拥有配置、Instance 和事务；API 不访问这些表，不经 system-rpc 转发 |
| 权限目录 | system 域增量迁移登记 `sys_api` | 8 条 POST 路由归 `service_name=system-api`，不是 job 域写入 System 表 |

HTTP 路由使用 `group: job`、`jwt: Auth`、`middleware: SessionAuth, Authorization`，保持默认生成 Handler。权限目录登记为可授权业务 API，不标为所有登录用户必需；角色具备相应 API 权限即可操作，不限定超管。本批不新增菜单、页面或给所有角色自动授权。

system-api 增加独立 `JobRpc` 配置和客户端，调用可以落到任一调度副本，不寻找 Leader。该客户端使用非阻塞连接配置；job-rpc 暂不可达只影响 Job 请求，不新增系统启动时读取 Job 配置，也不把现有 `/ready` 的 System 依赖检查改成必须等待 job-rpc。`NonBlock` 不等于消除 etcd 首次发现依赖，冷启动断连路径须单独验证，不能声称仅填该开关就覆盖所有故障。

沿用独立业务域和统一 BFF 是本次目录接入方案，不新增调度网关。生成时使用现有 goctl 1.10.2 及同样的 `--style gozero`；API 只对 `system.api` 生成，RPC 在新域目录对 `job.proto` 生成。同步 README 和生成规范的新入口，不直接改生成文件。

## HTTP 接口清单

以下路径相对现有 API 前缀，全部为 POST。响应列仅列 `data`，不在 DTO 内重复声明统一包装。

| 路径 | Handler 与 RPC 方法 | 请求 | 成功 data |
| --- | --- | --- | --- |
| `/job/list` | `listJobs` → `ListJobs` | `JobListReq` | `JobListResp`，`items` 与 `total` |
| `/job/get` | `getJob` → `GetJob` | 现有 `IDReq` | `JobDetail` |
| `/job/create` | `createJob` → `CreateJob` | `CreateJobReq` | `CreateJobResp { id: int64 }` |
| `/job/update` | `updateJob` → `UpdateJob` | `UpdateJobReq` | 现有 `EmptyResp`，JSON `{}` |
| `/job/status/update` | `updateJobStatus` → `UpdateJobStatus` | `UpdateJobStatusReq` | `EmptyResp` |
| `/job/delete` | `deleteJob` → `DeleteJob` | 现有 `IDReq` | `EmptyResp` |
| `/job/cron/preview` | `previewJobCron` → `PreviewJobCron` | `PreviewJobCronReq` | `PreviewJobCronResp { times: []string }` |
| `/job/trigger` | `triggerJob` → `TriggerJob` | `TriggerJobReq` | `TriggerJobResp { instanceId: int64 }` |

Job 创建成功表示定义已提交，不表示 Leader 已应用。手动触发成功表示 Instance 和本轮配置已提交，不表示开始或执行成功。普通 Job 新增不具备请求幂等；手动触发的网络重发才沿用同一 `requestId`。

### 新增与完整编辑

HTTP 字段平铺，不要求前端提交嵌套 `config`。下表 `*T` 是 `.api` 的存在性类型：省略和 JSON null 均为 nil，显式 0／false 不为 nil；新增默认值只由 job RPC 统一补齐，BFF 不先调用 Getter 抹掉存在性。

| HTTP 字段 | 新增 API 类型及缺省 | 完整编辑 API 类型 | 校验和归一化 |
| --- | --- | --- | --- |
| `id` | 不接收 | `int64`，必填 | 正数 |
| `name` | `string`，必填 | `string`，必填 | TrimSpace 后非空，最多 128 个字符 |
| `remark` | `string`，可省略 | `string`，可省略 | TrimSpace，最多 500 个字符；省略、null、空串均清空 |
| `executorGroupId` | `int64`，必填 | `int64`，必填 | 正数；存在且未逻辑删除，不要求执行器在线 |
| `processorName` | `string`，必填 | `string`，必填 | TrimSpace 后非空，区分大小写，最多 128 个字符 |
| `executionMode` | `*int32`，nil 默认 1 | `int32`，必填 | 1 普通，2 分片 |
| `postEnabled` | `*bool`，nil 默认 false | `*bool`，必须非 nil | 普通模式不能为 true；显式 false 合法 |
| `params` | `string`，可省略 | `string`，可省略 | TrimSpace；省略、null、空串均为空；保留内部内容 |
| `cronExpr` | `string`，必填 | `string`，必填 | TrimSpace 后非空，最多 128 字符，六字段解析及 Next 校验 |
| `status` | `*int32`，nil 默认 0 | 不属于编辑字段 | 0 停用，1 启用 |
| `misfirePolicy` | `*int32`，nil 默认 0 | `*int32`，必须非 nil | 0 忽略，1 执行一次 |
| `blockPolicy` | `*int32`，nil 默认 0 | `*int32`，必须非 nil | 0 丢弃，1 并行 |
| `maxRetries` | `*int32`，nil 默认 3 | `*int32`，必须非 nil | 非负，显式 0 不重试 |
| `retryIntervalSeconds` | `*int32`，nil 默认 10 | `int32`，必填 | 正数，单位秒 |
| `timeoutSeconds` | `int32`，必填，无 API 默认 | `int32`，必填 | 正数，单位秒；900 只是新增表单初值 |

完整编辑通过固定列 map 全量写入；不更新 `status`、`nextTriggerAt`、管理时间输入或删除标记。不因 ORM 忽略零值而保留旧重试次数或旧 Post 选项。未声明字段不能参与业务操作，不把宽松解码理解为支持 PATCH 或支持修改运行字段。

API 采用内置校验 Tag：正 ID `gt=0`；正数 `gt=0`；枚举 `oneof=...`；新增可选指针 `omitempty,...`；编辑必需指针 `required,...`。`required` 用于指针时判断是否提供，不拒绝指向合法 0／false 的值。RPC 对直接调用再次校验，不依赖 BFF。

字符串容量按归一化后的 Unicode 字符数检查。生成 Handler 的校验早于 API Logic，故这些要 TrimSpace 的字符串不在原始输入上使用会提前拒绝合法值的 `max` Tag；必填字符串可用 `required`，job RPC 统一 TrimSpace 后检查非空和容量。名称筛选同样处理。拒绝解析后的 NUL 和非法 UTF-8；不重写 JSON 解析器，不静默截断。不加自定义 Validator Tag 或数据库业务数值范围 CHECK。

普通新增／编辑 `remark` 和 `params` 的 null 与省略合并为空。数值超类型范围由解析拒绝；到 `time.Duration`／时间加法的转换仍须检查溢出。业务参数不强制 JSON，不另加 2KB／64KB 限制；沿用 HTTP 整体请求及 gRPC 报文上限。

### 列表和单项动作

| 请求 | 字段和 API 类型 | 语义 |
| --- | --- | --- |
| `JobListReq` | `page int64`、`pageSize int64` | 必填，page ≥ 1，pageSize 1～200；RPC 检查分页偏移运算溢出 |
| 同上 | `id *int64`、`executorGroupId *int64`、`status *int32` | 省略／null 不筛选；ID 正数，状态 0／1；条件 AND |
| 同上 | `name string`，可省略 | TrimSpace 后空串不筛选，最多 128 字符；不区分大小写的字面包含 |
| `IDReq` | `id int64` | 正数；用于详情和逻辑删除 |
| `UpdateJobStatusReq` | `id int64`、`status *int32` | id 正数；status 必须提供 0／1，不能把省略默认成停用 |
| `PreviewJobCronReq` | `cronExpr string` | 同一套六字段解析和长度规则；不接受时区、起算时间或数量配置 |

预览从一次读取的调度进程当前时间连续求 Next，最多返回 5 个带偏移时间点；首个 Next 不存在时返回参数错误，后续求不到时保留已经求得的时间点，不填零时间。不查询 Job 或组、不注册 Cron、不写数据库。保存和启用同样要求可求出下一次时间，完整编辑不重置持久化进度。

列表固定按 `id DESC`，只读未删除 Job；转义 `%`、`_` 和转义符后参数化 ILIKE。固定总数和分页投影两次查询，分页 LEFT JOIN 组名称，不因组缺失丢掉 Job 行。具体谓词及索引见[数据契约](data-contracts.md#job-input-query-proposal)。

### 手动触发和操作者

`TriggerJobReq` 为 `id int64`、`requestId string`、`paramsOverridden *bool`、`params *string`。id 为正数；requestId 使用 UUID 格式校验并转换为 UUID 值，不以大小写文本区别请求身份；paramsOverridden 必须提供，显式 false 合法。params 的省略和 JSON null 均是未提供，字符串空串是已提供。

| paramsOverridden | params | 处理 |
| --- | --- | --- |
| false | 省略／null | 沿用创建时 Job 参数 |
| false | 任意字符串，包括空串及全空白 | 参数错误 |
| true | 省略／null | 参数错误 |
| true | 任意字符串，包括空串及全空白 | 合法；组合检查后 TrimSpace，冻结本轮实际参数 |

API 可用 `required,uuid` 检查请求号、`required` 检查布尔指针；组合和规范化由 RPC 兜底。禁止先将 nil params 取成空串，也禁止先 trim 后把空串解释为未提供。相同请求号按规范化后的覆盖标记和必要参数比较；沿用请求不比较后来 Job 的参数，不改旧快照。

HTTP 不接收 `operatorId`／`triggeredBy`。API Logic 使用现有 `authctx.FromContext` 取得登录用户 ID，放进内部 `TriggerJobRequest.operator_id`；RPC 拒绝非正操作者 ID，创建 Instance 时写入 `triggered_by`，不额外查询用户名。操作者不进入已定 `(job_id, request_id)` 身份，不因重发改写原操作者。内部字段由受信 BFF 传递不等于 RPC 可暴露公网；组 Token 也不用于管理员认证。

其他管理请求没有需要落库的操作者字段，不机械增加全套 `created_by/updated_by`。8 条路由仍逐项执行 Session 与 API 权限，不能因为 RPC 消息没有 operator_id 就跳过 HTTP 鉴权。

## 响应投影

| 类型 | 字段 |
| --- | --- |
| `JobListItem` | `id int64`、`name string`、`executorGroupId int64`、`executorGroupName *string`、`processorName string`、`executionMode int32`、`cronExpr string`、`status int32`、`nextTriggerAt *string`、`createdAt string`、`updatedAt string` |
| `JobDetail` | JobListItem 的全部字段，加 `remark string`、`postEnabled bool`、`params string`、`misfirePolicy int32`、`blockPolicy int32`、`maxRetries int32`、`retryIntervalSeconds int32`、`timeoutSeconds int32` |
| `JobListResp` | `items []JobListItem`、`total int64`；空列表序列化为 `[]` |

HTTP 详情为平铺单对象；不返回删除标记、组凭据、实时节点、历史统计或 execution_config。列表不加载 remark／params。LEFT JOIN 无有效组时保留 Job，executorGroupName 明确返回 null，不隐去字段、不伪造组名或在线状态，也不另查一次组表。

时间为带偏移的 RFC 3339 字符串，Go 格式化使用保留有效精度的方式，例如 `time.RFC3339Nano`；不将微秒截为整秒。仅启用 Job 返回 nextTriggerAt；停用时该字段明确为 null，不能使用 `omitempty` 隐去字段或写成空串，数据库旧进度保留。RPC 对应 optional string 未设置，BFF 保持 nil，不用 Getter 转成空串。

## RPC 字段号清单

以下表完整指定本批消息，采用 proto3；`?` 表示 `optional`，`[]` 表示 `repeated`，其余为普通字段。消息名和字段号一旦发布不得随意复用；API camelCase 到 RPC snake_case 只做传输映射，不引入另一套默认值。

### 输入消息

| 消息 | 字段号与类型 |
| --- | --- |
| `JobInput` | `1 string name`；`2 string remark`；`3 int64 executor_group_id`；`4 string processor_name`；`5 ?int32 execution_mode`；`6 ?bool post_enabled`；`7 string params`；`8 string cron_expr`；`9 ?int32 misfire_policy`；`10 ?int32 block_policy`；`11 ?int32 max_retries`；`12 ?int32 retry_interval_seconds`；`13 int32 timeout_seconds` |
| `ListJobsRequest` | `1 int64 page`；`2 int64 page_size`；`3 ?int64 id`；`4 string name`；`5 ?int64 executor_group_id`；`6 ?int32 status` |
| `GetJobRequest` | `1 int64 id` |
| `CreateJobRequest` | `1 JobInput config`；`2 ?int32 status` |
| `UpdateJobRequest` | `1 int64 id`；`2 JobInput config` |
| `UpdateJobStatusRequest` | `1 int64 id`；`2 ?int32 status` |
| `DeleteJobRequest` | `1 int64 id` |
| `PreviewJobCronRequest` | `1 string cron_expr` |
| `TriggerJobRequest` | `1 int64 id`；`2 string request_id`；`3 ?bool params_overridden`；`4 ?string params`；`5 int64 operator_id` |

`config` 消息必须非 nil。CreateJob 对 JobInput 缺失的可选值采用上述默认；UpdateJob 要求 JobInput 所有 optional 配置均存在，不借共享消息套新增默认。API 编辑中使用值类型的 executionMode、retryIntervalSeconds 在通过非零校验后显式设置 RPC optional；其他指针直接保留。timeoutSeconds 为 0 一律非法，因此不需要用 optional 区分其遗漏和显式 0。

### 输出消息

| 消息 | 字段号与类型 |
| --- | --- |
| `JobListItem` | `1 int64 id`；`2 string name`；`3 int64 executor_group_id`；`4 ?string executor_group_name`；`5 string processor_name`；`6 int32 execution_mode`；`7 string cron_expr`；`8 int32 status`；`9 ?string next_trigger_at`；`10 string created_at`；`11 string updated_at` |
| `JobInfo` | 1～11 与 JobListItem 同名同类型；`12 string remark`；`13 bool post_enabled`；`14 string params`；`15 int32 misfire_policy`；`16 int32 block_policy`；`17 int32 max_retries`；`18 int32 retry_interval_seconds`；`19 int32 timeout_seconds` |
| `ListJobsResponse` | `1 []JobListItem items`；`2 int64 total` |
| `GetJobResponse` | `1 JobInfo job` |
| `CreateJobResponse` | `1 int64 id` |
| `PreviewJobCronResponse` | `1 []string times` |
| `TriggerJobResponse` | `1 int64 instance_id` |
| `EmptyResponse` | 无字段 |

service Job 的 8 个方法按 HTTP 清单同名映射，输入均为对应 `方法名Request`；List／Get／Create／Preview／Trigger 返回同名 Response，UpdateJob／UpdateJobStatus／DeleteJob 返回 EmptyResponse。每个方法独立 Logic；不增加统一 action RPC。此处 JobInput 是 RPC DTO，不跨域复用 GORM Model。

Leader 在同一 job-rpc 进程通过本域业务组件和 Repository 加载规则、触发记账，不绕回自己的管理 RPC；不为内部配置扫描额外发布管理列表接口或对外触发接口。

## 错误与重试契约

成功仍为 HTTP 200、`code=0`、`subcode=""`；message 由现有国际化处理器生成，data 为上文对象。业务错误为 HTTP 200、默认 `code=100001`、下表稳定 subcode、`data=null`；RPC 使用扩展业务 Code 与 `google.rpc.ErrorInfo.Reason`，不自建数字码体系或错误 Detail。

| 稳定 subcode | 对应情形 | 客户端处理 |
| --- | --- | --- |
| `common.invalid_request` | 解析、字段、Cron／Next、覆盖组合或数值溢出等非法输入 | 修正输入；HTTP 400，不逐字段新增码 |
| `job.definition.not_found` | Job 不存在或已逻辑删除 | 刷新列表；旧请求也不越过删除重放 |
| `job.definition.state_not_allowed` | 启用中完整编辑或删除 | 由用户先停用，不自动补操作 |
| `job.definition.active_instances` | 未结束 Instance 阻止 Job 删除 | 等待结束或通过后续停止功能处理 |
| `job.definition.executor_group_unavailable` | 管理入口引用的组不存在或已删除 | 选择可引用组；离线节点不触发该错误 |
| `job.trigger.blocked` | 新手动创建被同 Job 未结束轮次阻塞 | 明确拒绝，不后台等待补跑 |
| `job.trigger.request_conflict` | 相同 Job／requestId 的覆盖标记或规范化参数不同 | 不用旧号表达新操作 |
| `job.trigger.capacity_busy` | 本进程创建额度没有空位 | 可稍后沿用相同请求号和内容重试，不自动换号 |

权限继续使用 `common.authentication_required`／`common.permission_denied`。已分类技术错误沿用 `common.service_unavailable`（503）、`common.request_timeout`（504）、`common.request_canceled`（408）、`common.internal_error`（500）等现有映射，不把数据库故障误报业务阻塞。新增 job 域 subcode 常量及中英文翻译分别放在本域和已有全局翻译目录，API 不导入 job 内部常量来重写 RPC 错误。

提交确认不明仍返回技术失败，不推断“肯定未创建”。手动重发沿用原号，普通新增响应不明不安全自动重试；正常成功不额外写后查询。管理保存已经提交但 etcd 通知失败，仍返回保存成功并由分钟核对兜底。

## 实现验收入口

本附件的类型与错误名称已随总审确认；下面是编码后的通过条件，不是已运行结果。

| 检查 | 必须验证 |
| --- | --- |
| 生成与真实解码 | 8 条路由生成一致；缺失／null／0／false／空串经过真实 httpx.Parse 和 Validator；不只手工构造 Go 请求 |
| 字符与组合 | trim 后容量、Unicode、非法 NUL、普通模式开启 Post、Cron 无下次时间、手动 params 组合先检查后 trim |
| RPC 独立校验 | 绕过 API 调用时仍拒绝缺失完整编辑字段、非法枚举、非正 ID／超时及伪造空操作者 |
| 权限边界 | 未登录、无对应 API 权限、授权非超管三类用户；8 条 sys_api 登记与路由 Method／Path 一致 |
| 响应 | 列表无大载荷，空 items 为 []，详情平铺；停用 nextTriggerAt 为 null，时间精度保留，无组凭据泄漏 |
| 重发与失败 | 参数规范化后同号相同返回原 ID，不同报冲突；提交不明、容量忙、已删除 Job、通知失败分别符合契约 |
| 依赖与分层 | JobRpc 离线不新增 System 启动依赖；管理请求无 DB 直连；配置加载不用 HTTP 列表；独立进程生命周期可关闭 |

数据库竞争、故障恢复、持久化交接和运行容量仍按[触发模块验收](verification.md#trigger-module-delivery)及其专项清单验证；本附件不替代真实 PostgreSQL、etcd 和多副本测试。
