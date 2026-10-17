# 数据库迁移规范

DogX 只支持 PostgreSQL，使用 Goose 管理数据库结构，并采用 Goose 官方推荐的混合版本策略：开发阶段使用时间戳，准备发布时转换成连续版本。迁移可以合理使用 PostgreSQL 能力，不编写 MySQL、SQLite 或其他数据库的兼容分支。

## 创建迁移

从仓库根目录执行：

```powershell
go run ./apps/system/cmd/migrate create add_department
```

命令默认创建时间戳迁移：

```text
20260822103000_add_department.sql
```

开发者在同一个迁移中实现 `Up` 和 `Down`，然后在本地执行：

```powershell
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' up
```

时间戳降低了多个功能分支创建相同版本号的概率。不要手工创建下一个连续编号，也不要使用 `-allow-missing` 掩盖迁移顺序问题。

## 准备发布

功能分支和日常开发提交保留时间戳文件。只有准备生成正式发布版本时，才在已经整合所有待发布迁移的分支执行：

```powershell
go run ./apps/system/cmd/migrate fix
```

`fix` 按时间戳顺序把尚未发布的迁移接在现有连续版本之后：

```text
00001_init_system.sql
20260822103000_add_department.sql
20260822104500_add_login_log.sql

                    ↓ fix

00001_init_system.sql
00002_add_department.sql
00003_add_login_log.sql
```

`fix` 只重命名文件，不修改 SQL 内容。执行后必须检查文件顺序，运行迁移测试，并将重命名结果纳入正式发布提交。不要在多个并行功能分支分别执行 `fix`，否则仍会产生相同的连续版本号。

时间戳迁移只能用于可重建的本地开发数据库，不能直接进入共享测试或生产数据库。`fix` 不会修改数据库中的 `goose_db_version`：如果本地数据库已经执行过被重命名的时间戳迁移，应在重命名前将这些未发布迁移全部回滚，或者在重命名后重建本地数据库，再使用连续版本重新执行。发布验证必须从干净数据库或正式基线开始。

## 业务域独立迁移

各域独立维护迁移目录、命令和账本：System 使用 `apps/system/internal/migration/migrations`、`apps/system/cmd/migrate` 及 `goose_db_version`；Job 使用 `apps/job/internal/migration/migrations`、`apps/job/cmd/migrate` 及 `job_goose_db_version`。Job Provider 使用实例级 `WithTableName`，不修改 Goose 全局表名。

两个命令入口复用 `pkg/migratecmd` 的参数解析与执行逻辑，各自注入配置路径、迁移目录和本域 Provider。PostgreSQL 配置类型和连接工厂由 `pkg/postgres` 提供，各进程仍独立装配、持有和关闭自己的连接池；迁移命令使用独立的 SQL 连接，不借用服务进程连接池。

开发默认同库同 `public` schema，分别使用 `sys_*`（保留现有 `casbin_rule`）与 `job_*` 表名；两个域仍有各自的数据库配置、连接池及数据所有权。初次生产部署可通过 Job 配置指向独立数据库，不允许跨域 SQL 或事务。同库迁移依次执行，账本独立不代表允许并发迁移；Job 管理接口的 `sys_api` 登记仍属于 System 增量迁移。

```powershell
go run ./apps/job/cmd/migrate create init_job
go run ./apps/job/cmd/migrate -f 'apps/job/rpc/etc/job-rpc.yaml' up
go run ./apps/job/cmd/migrate -f 'apps/job/rpc/etc/job-rpc.yaml' status
```

Job 同样遵守时间戳开发版本、发布前 `fix`、显式 `Up`／`Down` 和已发布迁移不可变规则。服务进程不自动迁移，不自动创建默认执行器组。

## 基础模型与审计字段 {#base-model}

符合以下四字段语义的实体统一嵌入 `pkg/model.Base`，不在各域复制定义。Base 仅提供持久化技术字段，不携带业务枚举、业务关联或通用 Repository。

| Go 字段 / 数据库列 | 含义与维护规则 |
| --- | --- |
| `ID` / `id` | `int64` / `BIGINT` 技术主键，标识记录，不承载业务编号含义 |
| `CreatedAt` / `created_at` | 创建时间；插入时由 GORM 或迁移规定的数据库默认值初始化，后续不改写 |
| `UpdatedAt` / `updated_at` | 最后更新时间；创建时初始化，普通更新时维护，包括配置修改、状态变化及调度进度推进，不能限定成“管理配置修改时间” |
| `DeletedAt` / `deleted_at` | `gorm.DeletedAt` / 可空 `TIMESTAMPTZ`；NULL 表示未软删除，由 GORM `Delete` 写入删除时间并提供默认查询过滤 |

三个时间字段只用于审计与记录生命周期，不作为 Cron 进度、配置版本、执行开始／截止时间、租约或重试期限。需要这些业务语义时设置独立字段，例如 `next_trigger_at`。`DeletedAt` 用于过滤软删除记录及判断记录是否仍存在，不受此限制。

普通 GORM 创建／更新沿用默认时间维护；Raw／Exec 不执行模型的时间回调，须显式维护相应审计字段。软删除保持现有 System 行为：使用 GORM `Delete`，仅更新 `deleted_at`，不额外补写 `updated_at`，不为此增加 Hook 或事务。业务删除前的引用校验、状态条件及已有事务仍由业务代码负责；Raw 查询仍需显式写出软删除过滤条件。

只有全部字段都适用时才嵌入 Base：System 的普通软删除实体、JobDefinition 和 ExecutorGroup 复用它；JobInstance 没有软删除语义，不为复用而增加 `deleted_at`。日志、关系表等同样按自身生命周期建模。迁移 SQL 是结构权威，抽取 Base 不使用 `AutoMigrate`，不改变既有列或表归属。

通用启停字段统一使用 `pkg/model.RecordStatus`，底层类型为 `int16`，停用为 `RecordStatusDisabled = 0`，启用为 `RecordStatusEnabled = 1`；各域直接引用，不重复定义或保留别名。API／RPC 仍使用 `int32`，数据库仍使用 `SMALLINT`，RPC 必须先校验原始取值，再转换为 `RecordStatus`，避免缩窄整数后接受非法输入。

`RecordStatus` 是跨域通用基础类型，不是业务专属状态机。`Status` 字段由各模型自行声明，不放进 Base；启停业务行为以及 Instance／Task 运行状态、执行模式等专属类型仍由所属域维护，不因枚举复用而共用业务逻辑。

## 数据访问实现约定 {#data-access-style}

新增业务域或 Repository 前，先查看 System 中同类查询和写入。普通 CRUD 默认沿用 GORM 的 `Model/Where/Find/Create/Updates/Delete`；调度业务有并发要求，不代表普通增删改查需要全部改成 Raw／Exec。一个 Model 对应一个 Repository，不为统一调用形式新增基类、通用 Repository 或跨域业务数据层。

- 多字段更新用 `Updates(map[string]any)` 明确列出可写列，保存合法的 `0`、`false`、空字符串和 NULL。不要用 `Save` 或全量结构体更新，意外覆盖状态、进度、主键和创建时间等不属于本次操作的字段。
- 普通创建／更新和软删除沿用 System 的 GORM 默认审计时间行为：创建／更新由模型回调维护；`Delete` 只维护 `deleted_at`，不额外补写 `updated_at`，不增加补写 Hook 或事务。仍使用 Raw／Exec 的例外按前述规则显式维护审计时间。
- GORM 链可以包含 SQL 表达式、JOIN、子查询和 `clause.Locking`；它们不是要求整条查询改成 Raw 的理由。对关联表的软删除过滤须显式核对，不能假设 GORM 自动处理手写 JOIN 中的每张表。
- 复杂语句确需 Raw／Exec 时，在对应实现说明具体理由并提供测试；涉及 PostgreSQL 冲突处理、JSONB、部分索引或并发的行为，须用真实 PostgreSQL 集成测试验证，不能只靠 SQL 字符串测试。不是禁止 SQL，也不为减少 Raw 数量牺牲查询可读性或原有协议。
- 调整写法不改变事务边界、单主键行锁、锁后独立查询、原子更新条件及影响行数判断。必须保留部分索引依赖的固定谓词，不能将条件更新退化为先查询再无条件更新，也不能将需要新语句快照的查询合并进取锁语句。

## 字段类型与长度 {#column-types}

字段类型表达数据的用途和边界，不只比较数据库的存储实现。短字符串优先沿用项目现有 `VARCHAR(n)` 的建模方式，不因 PostgreSQL 的 `TEXT` 与 `VARCHAR` 存储特性接近，或为省去未来改表，就把所有字符串统一设计为 `TEXT`。

| 数据用途 | 类型规则 |
| --- | --- |
| 有合理长度边界的短字符串，如名称、编码、处理器标识、Cron 表达式、备注 | 使用 `VARCHAR(n)`；逐项明确长度，同语义字段复用已有边界，不给所有字段套同一个长度 |
| 业务参数、长内容或长度随业务变化的自由文本 | 按用途使用 `TEXT`，说明保留原文还是摘要；不将 TEXT 理解为接口或消息可无限大 |
| 需要按结构解释的配置快照 | 使用有明确结构契约的 `JSONB`；本来只是原文字符串的参数，不因看起来像 JSON 就改为 JSONB |
| ID、整数、布尔、UUID、时间点 | 使用对应原生类型，不为了统一而转换成字符串；沿用项目跨层类型约定 |

长度是设计契约的一部分，按以下规则落实：

- `VARCHAR(n)` 和字符串校验上限按 Unicode 字符数理解，不与 UTF-8 字节数或传输报文大小混用；使用 Go 校验时不能用 `len(string)` 代替字符计数。
- API、RPC 的接受上限不得超过数据库容量；常规同语义字段保持一致，GORM 模型同步声明长度。历史名称等完整快照字段采用相同容量，不能来源可保存、生成快照却超长失败。
- 超长输入在应用层明确拒绝，不通过类型转换、裁剪或截断掩盖超限。诊断摘要等确需生成截断文本的字段，须在其业务契约中另行规定，不能套用于名称、表达式或原样业务参数。
- `VARCHAR(n)` 是字符串容量约束，不等于将重试次数、超时时长等可调整业务数值范围写成 `CHECK`；现有业务校验与数据库约束边界不变。
- 调整长度时同步规范、接口、模型与迁移。已发布或共享数据库的旧迁移仍保持不可变，存量缩短长度须先验证已有数据，不自动批量改造无关表。

## 数据库约束与引用关系

DogX 的所有业务表统一不创建数据库物理外键。新增迁移的 `Up` 方向不得创建 `FOREIGN KEY`，也不得依赖 `REFERENCES`、`ON DELETE CASCADE`、`ON DELETE SET NULL` 或 `ON DELETE RESTRICT` 表达业务行为；数据之间的引用关系由应用层维护。用于恢复旧结构的 `Down` 可以重建旧版本原有的约束。

- 创建或修改关联关系时，业务代码负责校验被引用记录是否存在以及是否处于有效状态。
- 删除记录时，业务代码必须在事务中显式拒绝删除或清理关联数据，不得依赖数据库级联操作。
- 登录日志、操作日志和其他历史记录保留实体 ID、名称等必要快照，不因原实体被删除而修改历史数据。
- 所有关联字段必须根据查询方式建立必要的普通索引或唯一索引，禁止用取消外键作为省略索引的理由。
- 主键、具有独立业务语义的唯一约束、非空约束和检查约束继续用于保证单表数据质量；只为外键服务且没有独立业务语义的唯一约束应随外键一并删除。
- 已经执行的旧迁移保持不可变；发现已有物理外键时，通过新的 Goose 增量迁移删除。

业务代码必须在没有物理外键和级联行为的前提下保持正确，使数据库拆分、数据迁移或存储实现变化不会改变业务语义。

## 不可变规则

- 已经在共享数据库执行或随正式版本发布的连续迁移不得修改、重命名或删除。
- 修复旧迁移的问题必须新增迁移。
- 每个版本默认在事务中执行；只有 PostgreSQL 明确禁止事务的语句才使用 `-- +goose NO TRANSACTION`。
- 生产部署只运行一个迁移任务，不由每个 RPC 实例启动时自动迁移。
- 破坏性结构变化采用扩展、数据迁移、收缩三个阶段，保证滚动发布期间新旧代码兼容。

## 常用命令

```powershell
# 创建开发迁移（时间戳版本）
go run ./apps/system/cmd/migrate create <name>

# 发布前转换成连续版本
go run ./apps/system/cmd/migrate fix

# 查看状态
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' status

# 执行待处理迁移
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' up

# 回滚最近一个迁移
go run ./apps/system/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' down
```

参考：[Goose Hybrid Versioning](https://github.com/pressly/goose#hybrid-versioning)。
