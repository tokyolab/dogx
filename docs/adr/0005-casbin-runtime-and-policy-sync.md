# ADR-0005：Casbin 运行时与多实例策略加载

- 状态：已接受
- 日期：2026-08-25

## 背景

ADR-0004 已确定菜单、页面元素和 API 分开授权。运行时还需要固定以下问题：

- 每个 HTTP 请求如何取得用户角色并交给 Casbin，且不增加第二次权限 RPC；
- `system-rpc` 如何通过官方 Adapter 持久化一个角色的 API 策略；
- 多个 `system-api` 实例如何及时、可靠地取得 `casbin_rule` 中的最新策略；
- 用户角色发生变化后，已经签发的 JWT 如何失效。

方案继续满足以下约束：

- `sys_user_role` 是用户与角色关系的唯一事实源；
- `casbin_rule` 是角色 API 策略的唯一事实源，只保存 `p`，不增加 `sys_role_api`；
- Redis Session 不保存角色、超级管理员标识或具体权限；
- 普通受保护请求完成认证和授权后只调用一次业务 RPC；
- 禁止使用 Redis `KEYS`；
- PostgreSQL 是最终事实源，Redis 只传递策略失效通知，不承载权限事实。

## 决策

### Casbin 模型与请求主体

Casbin 直接以角色作为请求主体，不维护用户到角色的 `g` 关系：

```ini
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.sub == p.sub && r.obj == p.obj && r.act == p.act
```

运行时标识约定为：

```text
角色主体：r:<roleId>
接口策略：p, r:<roleId>, <path>, <METHOD>
```

HTTP 路径和大写 Method 使用精确匹配，不默认使用通配符。DogX 的业务参数主要通过 POST Body 传递，权限路径不包含资源 ID 等动态片段。

JWT 自定义 Claims 保存：

```text
userId
sessionId
roleIds
isSuperAdmin
```

登录和刷新 Token 时，由 `system-rpc` 从 `sys_user_role` 与 `sys_role` 读取用户当前拥有的全部启用角色，把角色 ID 写入 JWT，并根据其中是否存在编码为 `super_admin` 的角色重新计算 `isSuperAdmin`。该判断不固定角色数据库 ID，也不在 API 启动时查询或缓存角色 ID。Redis Session 仍然只保存会话数据，不重复保存角色或 `isSuperAdmin`。

API 权限中间件按固定顺序执行：JWT 验签、Redis Session 校验、超级管理员旁路、普通角色 Casbin 判定。只有签名有效、Session 存在且 `isSuperAdmin = true` 时才直接放行；客户端自行修改 Claim 会导致 JWT 验签失败。

普通用户在前两项校验通过后，为每个角色构造一条请求：

```text
BatchEnforce([
  ["r:1", requestPath, requestMethod],
  ["r:2", requestPath, requestMethod]
])
```

任意普通角色允许即放行。没有角色、没有匹配策略或全部角色拒绝时默认拒绝。

### 用户角色变化与令牌生效

JWT 中的 `roleIds` 和 `isSuperAdmin` 是签发时的角色快照。以下变化必须撤销目标用户的全部 Redis Session：

- 修改用户所属角色；
- 停用或删除用户；
- 修改密码；

Session 撤销继续使用用户 Session Set 和 `SSCAN` 精确定位 Session ID，禁止使用 Redis `KEYS`。旧 JWT 即使签名和有效期仍然有效，也会因为 `sessionId` 不存在而在下一次请求时返回 HTTP 401。用户重新登录后，JWT 获得最新 `roleIds` 和 `isSuperAdmin`。刷新 Token 时同样必须重新读取启用角色并计算两者，不能沿用旧 Access Token 的 Claims。

修改角色 API 策略、菜单授权、页面元素授权或角色名称时不撤销 Session，因为 JWT 不保存具体接口和菜单权限。

角色停用和重新启用只更新 PostgreSQL 中的角色状态，不撤销关联用户 Session。已有 Access Token 在自身和 Session 均有效期间继续使用签发时的角色快照；下一次登录或刷新令牌时重新读取全部启用角色，因此禁用角色从新 JWT 中移除，重新启用的角色重新进入新 JWT。用户没有任何启用角色时仍可完成认证，但新 JWT 的 `roleIds` 为空，访问需要接口权限的路由时默认拒绝。

普通角色删除先锁定目标角色，并查询是否存在通过 `sys_user_role` 引用该角色的未软删除用户；存在引用时返回业务错误，角色、关联、策略和 Session 全部保持原样。没有引用时，以普通 GORM 事务作为事务边界，并在该事务上创建临时官方 Adapter；业务表始终使用原始 `tx`，Adapter 只删除该角色全部 `p` 策略。软删除用户遗留的 `sys_user_role`、`sys_role_menu`、角色策略和 `sys_role` 软删除由同一个 PostgreSQL 事务原子提交。提交后发布普通 Watcher 通知；通知失败仍由周期重载恢复。`super_admin` 拒绝停用、删除和修改编码，用户与角色变更不得移除最后一名有效超级管理员。`is_system = true` 的其他系统内置角色不因此获得超级管理员旁路，仍按普通 Casbin 策略鉴权。

### 官方 Adapter 与 PostgreSQL 持久化

`system-api` 和 `system-rpc` 都使用 Casbin 官方 GORM Adapter v3 访问 `casbin_rule`。表结构由 Goose 管理，包含 `id` 主键、`ptype`、`v0` 至 `v5`，以及 `(ptype, v0, v1, v2, v3, v4, v5)` 唯一索引。两个进程都关闭 Adapter AutoMigrate。

`system-api` 的 Adapter 只用于 `LoadPolicy()`，同时关闭 Casbin AutoSave，不调用任何 Add、Remove 或 Update Policy API，也不写权限表。

角色授权界面提交完整的目标 API ID 集合。`system-rpc` 必须先拒绝目标角色编码为 `super_admin` 的授权请求；对普通角色校验启用的 API 资源和必需 API 后，将目标集合转换为 `p` 规则；随后在同一个 PostgreSQL 事务中完成：

1. 在事务中通过 `sys_role` 主键等值条件对目标角色执行单行 `FOR UPDATE`，串行化同一角色的并发授权更新，并与角色删除协调；
2. 业务查询只使用原始 GORM `tx`，不从 Adapter 反向取得或清洗 `*gorm.DB`；
3. 在同一个 `tx` 上创建关闭 AutoMigrate 的临时官方 Adapter，读取该角色当前 `p` 规则；
4. 计算目标集合与当前集合的差异；完全相同时不写数据库、不通知 Watcher；
5. 有变化时，通过 `RemoveFilteredPolicyCtx` 一次删除该角色全部 `p`，再通过 `AddPoliciesCtx` 批量写入完整目标策略；
6. 提交事务。

例如当前规则为 `A、B、C`，目标规则为 `B、C、D`，对外仍报告删除一条、新增一条；持久化使用一条按角色过滤的 `DELETE` 和一次批量 `INSERT`，两者处于同一事务，外部不会观察到空策略窗口。禁止使用官方 Adapter 当前会逐条删除且吞掉单条错误的 `RemovePoliciesCtx`；也不使用新增阶段逐条写入的 `UpdateFilteredPolicies`。

普通 GORM 事务是业务事务的唯一主人。临时 Adapter 只在事务回调内使用，不保存、不返回、不传入 goroutine；业务代码禁止调用 Adapter `GetDb()` 操作业务表。这样既保留官方 Adapter 的 Policy 映射与加载能力，也隔离其 `casbin_rule` Table Scope。

数据库事务提交成功后，`system-rpc` 调用官方 Redis Watcher v2 的普通 `Update()`。通知只表示“PostgreSQL 中的策略已经变化”，不携带 Add、Remove 或 Update 规则明细。通知失败不能回滚已经提交的数据库事务，由周期重载恢复各 API 实例。

普通角色的菜单与页面元素授权只修改 `sys_role_menu`，不进入 Casbin API 策略。`super_admin` 不依赖 `sys_role_menu`：后端菜单查询直接返回全部有效菜单和页面元素，菜单授权写接口必须拒绝该角色，避免清空关联记录后隐藏管理入口。

初始化迁移创建 `super_admin` 角色和权限管理 API 资源，但超级管理员授权不依赖该角色的 Casbin `p` 策略。`bootstrapadmin` 在创建首个管理员时，必须在同一个 PostgreSQL 事务中写入用户和 `sys_user_role` 绑定，避免出现已创建账号却没有任何角色、无法完成首次授权的启动死锁。后续新增受保护接口时，迁移必须登记对应 `sys_api` 资源供普通角色授权；`super_admin` 自动拥有全部后端接口，不需要随新增接口逐条补充策略。

### 多实例策略重载

每个 `system-api` 实例使用：

```text
Casbin v3 SyncedEnforcer
Casbin GORM Adapter v3
Casbin Redis Watcher v2（普通 Update 通知）
DogX PolicyReloader（串行重载与周期调度）
```

启动流程为：

```text
创建关闭 AutoMigrate 的只读 GORM Adapter
→ 创建关闭 AutoSave 的 SyncedEnforcer
→ 通过 PolicyReloader 串行执行第一次 LoadPolicy()
→ 创建 Redis Watcher
→ 显式把 Watcher 回调设置为 PolicyReloader.Reload()
→ 启动 60 秒周期重载
→ 初始策略加载成功后才进入就绪状态
```

Redis Watcher 收到普通 `Update()` 后，不解析增量规则，而是调用同一个 `PolicyReloader.Reload()`，从 PostgreSQL 完整加载当前全部 `p`。重复通知和乱序通知只会重复读取最终事实，不会把过期的增量事件应用到新状态之上。

`PolicyReloader` 使用进程内互斥锁串行化初始加载、Watcher 回调和周期加载，防止两个数据库快照反向覆盖。所有重载错误都必须记录，不能像 `StartAutoLoadPolicy()` 一样被静默忽略。

Casbin 在数据库读取和新 Model 构造期间允许正常 `Enforce` 继续并发执行，只在应用新 Model 时短暂持有写锁。请求只观察到完整的旧快照或完整的新快照。

Redis Pub/Sub 是至多一次投递，实例断线时可能永久遗漏通知。因此每个 API 实例还通过同一个 `PolicyReloader` 每 60 秒完整加载一次。多实例权限语义为：

```text
system-rpc 事务内按角色原子替换 casbin_rule
→ Redis 普通 Update 通知在线 system-api 立即全量重载
→ 通知遗漏时由下一个 60 秒周期重载恢复
```

### 失败规则

- `system-api` 初次 `LoadPolicy()` 失败时不得进入就绪状态或提供受保护接口。
- Redis Watcher 通知发布或订阅失败时记录错误和指标，不回滚已经提交的 PostgreSQL 策略；周期重载负责恢复。
- 周期重载期间 PostgreSQL 暂时不可用时保留上一次成功快照，记录错误并在下一周期重试。
- 就绪检查同时反映权限 PostgreSQL 连接和初始策略快照状态。
- Casbin 判定为 `false` 返回 HTTP 403；Enforcer 或认证依赖发生技术错误返回 HTTP 503，不能降级为允许。
- 修改用户所属角色时，如果 Session 撤销失败，变更操作必须失败关闭并记录审计日志，不能继续让携带旧角色的会话正常使用。
- 针对 `super_admin` 的接口和菜单授权请求必须由 RPC 业务规则拒绝；不能依赖前端隐藏权限树，也不能把清空其 Casbin Policy 或 `sys_role_menu` 当作撤销超级管理员能力的方式。
- 登录、刷新令牌和健康检查等公开路由不进入 Casbin。只要求登录状态的用户自助接口可以只经过 JWT 与 Session 中间件。

## 后果

- 普通请求不查询 `sys_user_role`，也不增加权限 RPC；
- JWT 保存角色 ID 和派生的 `isSuperAdmin`；修改用户所属角色会强制撤销该用户全部 Session，角色启停则在下一次登录或刷新令牌时进入新快照；
- `casbin_rule` 只保存 `p`，不需要 Casbin `g`、组合 Adapter 或用户角色全量加载；
- `super_admin` 在认证通过后旁路 Casbin，不依赖 `casbin_rule`，不会因误清空策略或新增接口尚未登记授权而失去管理入口；
- PostgreSQL 先判断规则差异；有变化时固定使用一次角色过滤删除和一次批量插入，避免逐条删除或逐条新增；
- Redis 消息只负责使本地快照失效，重复、乱序不会改变最终权限语义；
- 每次策略变更和每个周期都会完整读取 `p`，这是用低频数据库查询换取简单、可恢复的一致性；
- 通知遗漏时，旧权限最长可能保留一个配置周期，因此当前语义是有界最终一致而不是分布式强一致。

## 不采用的方案

- 不把 `sys_user_role` 加载为 Casbin `g`：需要组合 Adapter、内存同步和额外对账；
- 不把 `g` 重复保存到 `casbin_rule`：会与 `sys_user_role` 形成双事实源；
- 不把具体 API 或菜单权限写入 JWT：只写角色 ID 和派生的 `isSuperAdmin`；普通角色的具体权限仍由服务端 Casbin 策略决定；
- 不使用 WatcherEx 增量回调：Redis Pub/Sub 可能丢消息，多写实例的数据库提交顺序也不保证等于消息发布顺序，角色策略集合还可能需要拆成 Remove/Add 两条非原子消息；
- 不使用 Dispatcher：官方 Go Raft Dispatcher 仍为 beta，依赖旧版 Casbin，并要求使用自己的 bbolt 存储，不能继续以 PostgreSQL Adapter 为事实源；
- 不直接使用 `StartAutoLoadPolicy()`：它无条件周期加载、不能与 Watcher 回调共享 DogX 的串行入口，并且内部忽略加载错误；
- 不使用已经从 Casbin 删除的 `LoadPolicyFast()`：当前 `LoadPolicy()` 已包含缩小锁范围的实现；
- 第一版不增加 revision、策略哈希、Outbox 或 Redis Streams；当策略规模或变更频率达到现有方案的性能边界后，再通过基准和故障模型单独决策；
- 不为每个请求增加 `Authorize` RPC，也不在请求热路径从数据库加载策略。

## 测试要求

实现时至少覆盖：

- JWT 正确携带单角色、多角色 ID 和服务端计算的 `isSuperAdmin`，不携带菜单或具体 API 权限；登录与刷新 Token 都根据当前启用角色重新计算该字段；
- JWT 或 Session 无效时，即使 Claim 声明 `isSuperAdmin = true` 也不能放行；有效超级管理员在认证通过后不调用 Casbin；
- 无角色默认拒绝，单角色允许和拒绝，多角色任一允许即放行；
- 修改用户所属角色、用户停用和密码变更后，相关 Session 被全部撤销；角色停用后已有 Access Token 继续有效，登录或刷新得到的新 Token 排除禁用角色，重新启用后新 Token 再次包含该角色；
- 移除超级管理员角色后旧 Session 立即失效，重新登录或刷新 Token 不能继续得到 `isSuperAdmin = true`；
- `super_admin` 生命周期和最后一名有效超级管理员受保护，且不能通过角色授权接口写入或清空其接口、菜单策略；普通角色删除会原子清理关联表和 Casbin 策略；
- 超级管理员不依赖 `sys_role_menu` 即可取得全部有效菜单和页面元素，普通角色仍只取得显式授权的菜单集合；
- 不使用 Redis `KEYS` 查找或撤销 Session；
- 角色授权提交完整目标集合；无变化时不写，有变化时使用一条角色过滤 DELETE 和一次批量 INSERT，并验证两者与业务写入共享同一事务；
- 同一角色并发更新被串行化，不产生部分集合；
- `system-api` 初次加载正确，普通 Watcher 通知触发全量重载，遗漏通知后周期重载能够观察到最新策略；
- 初始、Watcher 和周期重载共享串行入口，不能发生旧快照覆盖新快照；
- 并发 `Enforce` 在重载期间只观察到完整旧策略或完整新策略；
- 初次加载失败、周期重载异常和 Enforcer 错误均按约定失败关闭。
