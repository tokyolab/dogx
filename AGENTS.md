# DogX 仓库协作规则

## 生成代码

- `apps/system/api/system.api` 是 HTTP API 唯一生成入口，只维护对 `desc/*.api` 的导入。
- `apps/system/api/desc/base.api` 只保存被多个业务契约复用且语义完全一致的基础类型；每个业务资源的类型、路由和 Handler 声明放在对应的 `desc/<resource>.api` 中。
- 只对入口 `system.api` 运行 goctl，不得单独对 `desc/*.api` 生成代码。新建公共类型前先确认至少三个业务契约存在同语义重复；业务枚举、业务 DTO 和持久化模型不得放入 `base.api`。
- `apps/system/rpc/system.proto` 是 gRPC 契约源文件。
- 任何包含 `Code generated`、`DO NOT EDIT` 的文件都禁止直接编辑，包括 `routes.go`、`types.go`、`*.pb.go`、`*_grpc.pb.go`、RPC `internal/server` 和 `systemclient` 下的生成文件。
- 修改 `.api` 或 `.proto` 后，必须使用 README 中固定的 goctl 命令重新生成并提交生成结果，不能用文本替换修补生成文件。
- 标记 `Code scaffolded by goctl. Safe to edit.` 的 Handler 和 Logic 只在首次生成时创建，后续允许编辑；HTTP Handler 保持参数解析、调用 Logic、调用 `httpx.ErrorCtx/OkJsonCtx` 的默认职责，业务代码写在 Logic。
- 对外 API 必须声明响应类型，确保成功响应经过 `httpx.OkJsonCtx` 和全局成功处理器。

## 错误与响应

- HTTP Handler 使用 goctl 默认结构，不维护自定义 Handler 模板。
- 成功响应由 `httpx.SetOkHandler` 统一包装；错误响应由 `httpx.SetErrorHandlerCtx` 统一处理。
- HTTP 响应统一包含 `code/subcode/message/data`；程序逻辑和业务测试使用稳定的 `subcode`，不得依赖可调整的 `message`。
- 成功码为 `0`；默认业务错误码为 `100001`，只有客户端确实需要分支处理时才增加具体业务码。
- 内部 RPC 使用 `100000～2147483647` 的扩展 gRPC Code 传递业务错误；标准 gRPC Code `0～16` 留给框架和技术错误。
- RPC 业务错误必须保留可读的原始 Message，并通过标准 `google.rpc.ErrorInfo.Reason` 携带 subcode；不设置 ErrorInfo Domain，不自定义错误 Detail protobuf。
- 参数解析、API 字段校验和 RPC 参数兜底统一使用 `common.invalid_request`，不为每条字段校验规则创建 subcode。
- API 字段校验标签写入 `.api` 源文件；全局 Validator 通过 `httpx.SetValidator` 注册，不得为校验修改 goctl 生成 Handler。
- API 字段校验只允许使用 `go-playground/validator` 官方内置 Tag，不注册项目自定义 Tag；无法由内置 Tag 表达的业务格式在前端、RPC 和数据库约束中校验。
- Validator 失败向客户端保留原始详细诊断以便接口调试；JSON 解析失败只返回通用参数错误，详细原因仅写服务端日志且不得记录完整请求体。
- 后端国际化只覆盖业务错误和通用技术错误，不维护 Validator 字段名与规则文案翻译。
- 扩展 gRPC Code 是 DogX 内部协议，不得直接用于对外 gRPC API。
- 业务错误返回 HTTP 200；参数和技术错误使用真实 HTTP 4xx/5xx。

## 业务域与数据所有权

- `apps/<domain>/internal/model` 定义该业务域的 GORM 持久化模型，以及与模型字段直接绑定的类型和常量；不得作为跨业务域 DTO 使用。
- `apps/<domain>/internal/repository` 封装该业务域的普通业务数据访问；API/BFF 不得直接导入普通业务 Model、Repository，不得访问普通业务表或初始化通用业务数据库连接。
- 只有架构 ADR 明确批准的基础设施或安全只读投影可以例外。当前唯一例外是 `system-api` 按 [ADR-0005](docs/adr/0005-casbin-runtime-and-policy-sync.md) 通过官方 Casbin GORM Adapter 从 `casbin_rule` 加载本地鉴权快照；该连接不得访问普通业务表，API 不得调用策略写入方法，策略持久化仍由 `system-rpc` 负责。
- RPC 保持 go-zero 的“一项接口操作对应一个 Logic”结构；Logic 负责业务用例编排和业务错误转换，不直接调用 GORM、手写 SQL 或解析数据库驱动错误。
- Repository 默认按聚合或业务资源提供一个同时包含必要读写能力的内聚接口；不得仅为了缩小测试 Stub 而继续拆分 Reader、Writer、Store 等接口，也不得建立机械转发 ORM 的通用 BaseRepository。
- Repository 负责查询、持久化事务、锁及数据库约束错误转换；跨多个聚合、Casbin、Session 或事件发布的业务事务由显式业务服务编排，不塞入通用 Repository。
- 同一业务域的 RPC、MQ、Job 等后端进程可以复用该业务域 `internal` 下的数据层；其他业务域不得直接访问其模型或数据表。
- 需要跨业务域传递的字段、枚举和消息必须定义在 Protobuf 或版本化事件契约中；其他业务域通过 RPC 客户端或事件契约使用，不复制常量，也不导入持久化模型。
- `pkg` 只放跨进程或跨业务域复用的稳定技术组件，不放用户、角色、菜单等业务模型或业务常量；只被单个进程使用的代码放在对应 `internal` 中。

## 跨层类型与契约

- 主键 ID 在 API、RPC、Model 中统一使用 `int64`，PostgreSQL 使用 `BIGINT`。
- 排序字段在 API、RPC、Model 中统一使用 `int32`，PostgreSQL 使用 `INTEGER`。
- 通用启停状态在 API、RPC 中使用 `int32`，Model 使用底层为 `int16` 的状态类型，PostgreSQL 使用 `SMALLINT`；RPC 转换为 Model 前必须校验取值合法性。
- 避免同一字段在 API 与 RPC 之间使用不同整数宽度，减少无意义的类型转换。

## PostgreSQL 与 GORM

- 禁止 N+1 数据库查询：业务列表及关联数据的查询次数不得随结果集记录数线性增长，禁止在 `for`、`range` 或逐条 DTO 转换过程中按记录调用 Repository、GORM 或手写 SQL 查询数据库。
- 关联数据必须使用 `JOIN`、`IN`、聚合查询、GORM `Preload` 或其他批量查询一次加载，再在内存中组装；分页场景固定执行总数查询和分页数据查询不属于 N+1。
- 确实无法批量处理的数据库访问必须说明原因、设置明确的批次或数量上限，并通过评审确认，不得以实现方便为由引入随数据量增长的逐条查询。

### 数据库事务与锁

- 多条数据库写操作需要原子性时，必须放在同一事务中。
- 普通 CRUD 不使用显式锁；仅在明确的并发读改写场景中使用 `FOR UPDATE`。
- `FOR UPDATE` 仅允许在显式事务中，通过单表主键等值条件锁定一条记录；禁止对非主键条件、范围、`IN`、`JOIN`、子查询或多行结果加锁。
- 每处显式锁必须注释说明它防止哪一种并发错误。
- 不使用 PostgreSQL advisory lock；确需跨进程协调时统一使用 Redis 分布式锁，并通过公共组件集中管理实现和锁键，业务代码不得自行拼接锁键。
- 事务必须保持短小，不得包含 Redis、RPC、MQ 等外部 I/O。
- 数据库操作继续传递调用方 Context；特殊长任务单独调整超时或异步处理。

## Redis

- 禁止在业务代码、测试、脚本和运维命令中使用 Redis `KEYS` 命令，也不得通过客户端封装间接调用；该命令会全量遍历键空间并阻塞 Redis。
- 已知键名时使用 `GET`、`EXISTS` 等精确操作；确需渐进遍历键空间时使用带游标和合理 `COUNT` 的 `SCAN`，不得一次性加载全部结果。
- 业务上需要按用户、会话或其他维度批量查询、撤销或清理数据时，写入数据的同时维护 `Set`、`ZSet` 等显式索引，不得依赖键名通配扫描实现核心业务逻辑。

## 代码注释

- 注释解释代码无法直接表达的原因、约束和取舍，不复述函数名、条件判断或赋值等表面行为。
- 反直觉的并发同步、资源生命周期、事务与缓存一致性顺序必须就地注释，说明省略或调整顺序会造成的具体问题。
- 第三方库的副作用、兼容处理和刻意绕行必须注明所依赖的外部行为，防止后续维护者误删或“简化”关键代码。
- 认证、令牌、密码和审计等安全代码必须解释非显然的安全目的；原子脚本、扩展协议和特殊返回值必须说明语义。
- 注释必须随实现同步更新；已经失效、仅保留历史过程或无法证明仍然正确的注释必须删除或修正。
- 生成文件不得直接补注释；需要调整时修改契约源文件并重新生成。

## 版本控制

- 未经用户明确同意，不执行 `git commit` 或 `git push`。
- 保留工作区中与当前任务无关的已有修改；发现来源不明的差异时先说明再处理。
