# ADR-0003：认证令牌与会话

- 状态：已接受
- 日期：2026-08-24

## 背景

DogX 的浏览器请求先进入 `system-api`，业务逻辑由 `system-rpc` 执行。认证需要同时满足以下要求：

- 兼容管理后台常见的访问令牌与刷新令牌流程；
- 密码、签名密钥和刷新令牌不能以明文持久化；
- 退出登录、账号停用和管理员强制下线能够撤销会话；
- API/BFF 不直接访问 System 域数据库，认证 Session 的只读校验不额外增加一次 RPC；
- 登录错误不能帮助攻击者判断账号是否存在。

## 决策

### 密码

- 密码使用 Argon2id 哈希，数据库只保存 PHC 格式编码后的结果。
- 默认参数为 64 MiB 内存、3 次迭代、2 路并行、16 字节盐和 32 字节密钥。
- 新密码长度为 12 至 128 字节，初始化管理员和修改密码共用同一校验规则。
- 修改密码必须验证当前密码；成功修改前先撤销该用户全部 Session，再写入新的密码哈希。若数据库更新失败，用户会被登出但仍可使用旧密码重试，不会出现“修改成功但旧 Session 仍有效”。
- 登录失败统一返回“用户名或密码错误”；不存在的用户与密码错误使用同一业务码和消息。
- 用户名在查询前去除首尾空白，匹配规则沿用数据库中不区分大小写的活动用户唯一约束。

### 访问令牌

- 访问令牌使用 HS256 JWT，默认有效期 15 分钟。
- JWT 由 `system-rpc` 签发，`system-api` 使用同一密钥验证。
- 自定义 Claims 保存 `userId`、`sessionId`、`roleIds` 和 `isSuperAdmin`；不保存菜单、页面元素或具体 API 权限。
- `roleIds` 是签发时从 `sys_user_role` 读取的启用角色快照。`isSuperAdmin` 表示该用户是否拥有未停用、编码为 `super_admin` 的角色；登录和刷新 Access Token 时都必须从数据库重新计算，不能信任旧 Token 中的值，也不固定或缓存该角色的数据库 ID。
- `isSuperAdmin` 是服务端签名 JWT 中的派生授权标识。JWT 与 Session 校验通过后，超级管理员可以直接通过后端接口鉴权；普通用户继续按 `roleIds` 进入 Casbin。客户端不能通过修改 Claim 获得该能力。
- 修改用户所属角色时必须撤销该用户的全部 Session，旧 JWT 因 Session 不存在而立即失效。停用或重新启用角色不撤销关联用户 Session；已有 Access Token 在自身和 Session 均有效期间保留签发时的角色快照，下一次登录或刷新令牌时按当前启用角色生成新快照。仍被未软删除用户引用的角色禁止删除。
- JWT 密钥只通过真实配置或环境变量提供，不提交到 Git；密钥至少 32 字节。
- 受保护 HTTP 路由使用 go-zero `jwt: Auth` 先在 API 本地校验 JWT，签名无效或已过期的请求不会调用 RPC 或查询 Redis。
- JWT 验签成功后，`SessionAuth` 中间件通过只读 `SessionReader` 精确读取 Redis Session；JWT 在这里是经过签名的 Session 凭证，而不是完全无状态的授权结果。
- `system-api` 只拥有认证 Session 的读取能力，不通过该依赖创建、轮换或撤销 Session。

### 刷新令牌与会话

- 刷新令牌是密码学安全随机值，不使用 JWT，默认有效期 7 天。
- 客户端持有的格式为 `<sessionId>.<secret>`；Redis 只保存 `secret` 的 SHA-256 摘要。
- 会话由 `system-rpc` 写入 Redis，键格式为 `<配置前缀>:<sessionId>`，值包含用户 ID、刷新令牌摘要和过期时间。Redis Session 不重复保存 `roleIds` 或 `isSuperAdmin`，避免 JWT 与 Session 形成两份授权快照。
- 每个用户同时维护 `<用户会话索引前缀>:<userId>` Set，成员是该用户的 Session ID，用于全部退出、账号停用和管理员强制下线；禁止通过 Redis `KEYS` 命令查找会话。
- 刷新时同时轮换 Access Token 和 Refresh Token，并延长 Session 有效期；旧 Refresh Token 再次使用时撤销整个 Session。
- 当前设备退出时删除一个 Session；全部退出、密码变更、用户角色变更、账号停用和管理员强制下线时通过用户 Session Set 撤销该用户的全部 Session。
- 按用户撤销时使用 `SSCAN` 分批读取显式索引并精确删除 Session，不全量扫描 Redis 键空间。

### 登录审计

- 每次到达 RPC 的登录尝试写入 `sys_login_log`，保存用户 ID（未知账号时为空）、账号快照、成功状态、内部失败原因标识、客户端 IP、User-Agent 和发生时间；绝不保存密码或令牌。
- API 的 RPC 客户端日志和 RPC 服务端日志不得记录含密码、Token 或 Session ID 的请求与响应正文；框架仍可记录不包含正文的方法级慢调用和错误信息，指标统计不受影响。
- 对外仍统一返回“用户名或密码错误”，内部审计原因不会帮助客户端判断账号是否存在。
- 登录成功后更新 `sys_user.last_login_at`；登录时间和审计日志属于附属记录，写入失败会记录服务日志，但不会废弃已经签发的有效凭证。
- 客户端 IP 仅用于审计，不参与鉴权；部署入口必须正确维护 `X-Forwarded-For`，不能把它当作可信权限依据。

### 服务边界

```text
browser
  -> system-api：本地验证 JWT、从 Redis 只读检查 Session、解析 HTTP
                RBAC 阶段从 PostgreSQL casbin_rule 加载只读 Casbin p 快照
  -> system-rpc：执行一次业务调用，查询用户、验证密码、管理会话、签发令牌
  -> PostgreSQL / Redis
```

认证阶段的 `system-api` 直接连接 Redis，只通过窄接口读取认证 Session，不查询 PostgreSQL。RBAC 阶段增加的 PostgreSQL 连接只读授权表，用于加载本地 Casbin 快照，不参与 Session 校验或普通业务查询。Session 的创建、刷新和撤销规则仍由 RPC 层实现。这样普通受保护请求在完成本地 JWT、Session 与 Casbin 校验后，仍只需调用一次业务 RPC。定时任务和其他内部进程直接按内部调用约定访问 RPC，不经过面向浏览器的 Session 中间件，也不伪造用户 JWT。

## 后果

- 每次登录都会创建独立会话，天然支持同一账号多设备登录和按会话下线。
- JWT 本地验签可以在访问 Redis 前过滤攻击者随机构造的无效 Token；已通过验签的请求仍需检查 Session，以保证退出和强制下线立即生效。
- API 的认证 Redis 是显式运行时依赖，必须纳入启动配置和 `/ready` 检查；Redis 技术故障返回 HTTP 503，认证失败返回 HTTP 401。
- 认证请求减少了一次纯 Session 校验 RPC，但 API 与 RPC 都需要访问同一个认证 Redis；写能力仍集中在 RPC。
- HS256 要求 API 与 RPC 安全共享密钥；如果未来存在不受信任的验签方，再迁移到非对称签名。
- Redis 不可用时不能创建、校验、刷新或撤销会话，系统返回技术错误而不是降级为无状态令牌。
- JWT 携带角色 ID 和服务端派生的 `isSuperAdmin`，但不携带具体 RBAC 权限；Redis Session 不保存角色或超级管理员标识。超级管理员在 JWT 与 Session 校验后直接放行，普通角色的 API 权限通过服务端 Casbin `p` 快照和 [ADR-0005](0005-casbin-runtime-and-policy-sync.md) 规定的 Redis 失效通知与串行周期重载判定。
