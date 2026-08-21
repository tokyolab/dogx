# 数据库迁移规范

DogX 使用 Goose 管理 PostgreSQL 结构，采用 Goose 官方推荐的混合版本策略：开发阶段使用时间戳，准备发布时转换成连续版本。

## 创建迁移

从仓库根目录执行：

```powershell
go run ./apps/system/rpc/cmd/migrate create add_department
```

命令默认创建时间戳迁移：

```text
20260822103000_add_department.sql
```

开发者在同一个迁移中实现 `Up` 和 `Down`，然后在本地执行：

```powershell
go run ./apps/system/rpc/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' up
```

时间戳降低了多个功能分支创建相同版本号的概率。不要手工创建下一个连续编号，也不要使用 `-allow-missing` 掩盖迁移顺序问题。

## 准备发布

功能分支和日常开发提交保留时间戳文件。只有准备生成正式发布版本时，才在已经整合所有待发布迁移的分支执行：

```powershell
go run ./apps/system/rpc/cmd/migrate fix
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

## 不可变规则

- 已经在共享数据库执行或随正式版本发布的连续迁移不得修改、重命名或删除。
- 修复旧迁移的问题必须新增迁移。
- 每个版本默认在事务中执行；只有 PostgreSQL 明确禁止事务的语句才使用 `-- +goose NO TRANSACTION`。
- 生产部署只运行一个迁移任务，不由每个 RPC 实例启动时自动迁移。
- 破坏性结构变化采用扩展、数据迁移、收缩三个阶段，保证滚动发布期间新旧代码兼容。

## 常用命令

```powershell
# 创建开发迁移（时间戳版本）
go run ./apps/system/rpc/cmd/migrate create <name>

# 发布前转换成连续版本
go run ./apps/system/rpc/cmd/migrate fix

# 查看状态
go run ./apps/system/rpc/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' status

# 执行待处理迁移
go run ./apps/system/rpc/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' up

# 回滚最近一个迁移
go run ./apps/system/rpc/cmd/migrate -f 'apps/system/rpc/etc/system-rpc.yaml' down
```

参考：[Goose Hybrid Versioning](https://github.com/pressly/goose#hybrid-versioning)。
