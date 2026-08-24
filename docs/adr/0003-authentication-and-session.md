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
- 登录失败统一返回“用户名或密码错误”；不存在的用户与密码错误使用同一业务码和消息。
- 用户名在查询前去除首尾空白，匹配规则沿用数据库中不区分大小写的活动用户唯一约束。

### 访问令牌

- 访问令牌使用 HS256 JWT，默认有效期 15 分钟。
- JWT 由 `system-rpc` 签发，`system-api` 使用同一密钥验证。
- 自定义 Claims 只保存 `userId` 和 `sessionId`；不在 JWT 中保存角色、菜单或按钮权限，避免权限变更后旧权限持续有效。
- JWT 密钥只通过真实配置或环境变量提供，不提交到 Git；密钥至少 32 字节。
- 受保护 HTTP 路由使用 go-zero `jwt: Auth` 先在 API 本地校验 JWT，签名无效或已过期的请求不会调用 RPC 或查询 Redis。
- JWT 验签成功后，`SessionAuth` 中间件通过只读 `SessionReader` 精确读取 Redis Session；JWT 在这里是经过签名的 Session 凭证，而不是完全无状态的授权结果。
- `system-api` 只拥有认证 Session 的读取能力，不通过该依赖创建、轮换或撤销 Session。

### 刷新令牌与会话

- 刷新令牌是密码学安全随机值，不使用 JWT，默认有效期 7 天。
- 客户端持有的格式为 `<sessionId>.<secret>`；Redis 只保存 `secret` 的 SHA-256 摘要。
- 会话由 `system-rpc` 写入 Redis，键格式为 `<配置前缀>:<sessionId>`，值包含用户 ID、刷新令牌摘要和过期时间。
- 每个用户同时维护 `<用户会话索引前缀>:<userId>` Set，成员是该用户的 Session ID，用于全部退出、账号停用和管理员强制下线；禁止通过 Redis `KEYS` 命令查找会话。
- 刷新时同时轮换 Access Token 和 Refresh Token，并延长 Session 有效期；旧 Refresh Token 再次使用时撤销整个 Session。
- 当前设备退出时删除一个 Session；全部退出、密码变更、账号停用和管理员强制下线时通过用户 Session Set 撤销该用户的全部 Session。
- 按用户撤销时使用 `SSCAN` 分批读取显式索引并精确删除 Session，不全量扫描 Redis 键空间。

### 服务边界

```text
browser
  -> system-api：本地验证 JWT、从 Redis 只读检查 Session、解析 HTTP
  -> system-rpc：执行一次业务调用，查询用户、验证密码、管理会话、签发令牌
  -> PostgreSQL / Redis
```

`system-api` 不连接 PostgreSQL，但直接连接 Redis，只通过窄接口读取认证 Session。Session 的创建、刷新和撤销规则仍由 RPC 层实现。这样普通受保护请求在完成本地 JWT 与 Session 校验后，只需调用一次业务 RPC。定时任务和其他内部进程直接按内部调用约定访问 RPC，不经过面向浏览器的 Session 中间件，也不伪造用户 JWT。

## 后果

- 每次登录都会创建独立会话，天然支持同一账号多设备登录和按会话下线。
- JWT 本地验签可以在访问 Redis 前过滤攻击者随机构造的无效 Token；已通过验签的请求仍需检查 Session，以保证退出和强制下线立即生效。
- API 的认证 Redis 是显式运行时依赖，必须纳入启动配置和 `/ready` 检查；Redis 技术故障返回 HTTP 503，认证失败返回 HTTP 401。
- 认证请求减少了一次纯 Session 校验 RPC，但 API 与 RPC 都需要访问同一个认证 Redis；写能力仍集中在 RPC。
- HS256 要求 API 与 RPC 安全共享密钥；如果未来存在不受信任的验签方，再迁移到非对称签名。
- Redis 不可用时不能创建、校验、刷新或撤销会话，系统返回技术错误而不是降级为无状态令牌。
- 后续 RBAC 权限不写入 JWT，通过服务端实时数据与明确的同步机制判定。
