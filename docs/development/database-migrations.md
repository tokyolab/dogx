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
