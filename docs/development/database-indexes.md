# PostgreSQL 索引设计与审计规范

DogX 只支持 PostgreSQL。本规范以 PostgreSQL 18 为准，兼顾 PostgreSQL 17，用于约束新索引设计、代码评审、存量索引审计和生产变更。索引不是字段的附属配置，而是为具体查询、约束或运维目标付出的持续成本。

## 核心原则

1. 每个索引必须对应明确的约束或查询模式。不能因为字段出现在 `WHERE`、`JOIN`、`ORDER BY` 中，或者字段名像状态、时间、外部 ID，就自动创建索引。
2. 索引审计必须同时考虑正确性、读收益、写放大、WAL、存储、缓存占用、VACUUM 和维护复杂度，不能只比较单条查询的执行时间。
3. PostgreSQL 是否使用索引由成本优化器决定。顺序扫描不等于缺少索引，小表或返回大量数据时顺序扫描通常更合理。
4. 区分度只是证据之一，不是复合索引列顺序的总规则。必须结合运算符、数据倾斜、排序、分页、返回比例和查询频率判断。
5. 静态代码和 Schema 审计只能发现确定性问题。生产优化必须结合具有代表性的数据、`EXPLAIN` 和足够长时间的真实工作负载统计。
6. 先减少无效扫描和错误估算，再考虑覆盖索引等局部优化。禁止为了追求表面上的 Index Scan 堆叠索引。

## 证据等级

索引结论按证据强度分为四级：

1. **约束证据**：主键、唯一性或排他性等必须由数据库保证的规则。
2. **查询证据**：代码中存在确定的 SQL 形态，能够说明过滤、关联、排序、分页和返回字段。
3. **计划证据**：在具有代表性的数据和统计信息上，`EXPLAIN (ANALYZE, BUFFERS)` 证明候选索引改善了实际计划。
4. **生产证据**：`pg_stat_statements`、`pg_stat_user_indexes`、I/O 和写入统计证明它在完整业务周期内有净收益。

项目初期可以依据前两级证据建立必要索引；覆盖索引、特殊索引、删除存量索引和生产性能结论通常需要第三级或第四级证据。

## 先从查询开始

设计索引前必须写出目标查询的完整形态：

```sql
SELECT <projection>
FROM <table>
WHERE <equality predicates>
  AND <range predicates>
ORDER BY <ordering>
LIMIT <n>;
```

至少回答以下问题：

- 查询按什么条件等值过滤？
- 是否存在范围、前缀匹配、集合或空值判断？
- 是否要求特定排序，是否带有较小的 `LIMIT`？
- 查询返回多少行，占表中有效数据的比例是多少？
- 查询执行频率和延迟贡献是多少？
- 表的行数、增长速度和写入比例是多少？
- ORM 最终生成的 SQL 是否包含软删除、表达式、类型转换或额外排序？

只看到 Model 字段或单段 Repository 代码时，不得臆造未来查询并提前建立覆盖索引。

## B-tree 复合索引列顺序

PostgreSQL 18 对多列 B-tree 的基本规则是：前导列的等值条件，以及第一个没有等值条件列上的不等值条件，可以稳定缩小需要扫描的索引区间；更右侧的条件可以在索引中检查，但不一定减少扫描区间。

因此，常见起点是：

1. 放置目标查询稳定使用的等值条件列。
2. 再决定由哪个范围条件限制扫描区间，或者由哪些列提供所需排序。
3. 仅为返回结果服务的列最后考虑使用 `INCLUDE`，不得混入搜索键冒充过滤能力。

这不是可以机械套用的“等值、范围、排序”拼接公式。如果范围列和排序列不同，一个 B-tree 往往无法同时最优满足两者。例如：

```sql
WHERE account_id = ? AND amount > ?
ORDER BY created_at DESC
LIMIT 20
```

`(account_id, amount, created_at)` 可以利用金额范围缩小扫描，但通常不能直接提供跨金额值的 `created_at` 全局顺序；`(account_id, created_at DESC)` 可以优先满足 Top-N 排序，再过滤金额。必须依据选择率、`LIMIT` 和执行计划选择，必要时保留不同用途的索引。

### 不使用“最高区分度永远在前”规则

当查询对多个列都使用等值条件时，把哪一个等值列放在前面通常不存在统一的“最高区分度优先”答案。需要比较：

- 哪些查询只使用其中一部分前缀列；
- 数据是否严重倾斜，而不只是有多少个不同值；
- 后续范围和排序列如何排列；
- 索引是否还承担唯一性；
- PG18 skip scan 是否在真实计划中有效；
- 更小的单列索引是否对高频查询仍有可测量价值。

禁止仅用“选择性最高”或“字段在 SQL 中出现得更早”解释列顺序。

### 低区分度列

布尔值、状态、类型、软删除时间等低区分度列建立单列 B-tree，通常无法排除足够多的数据，尤其是查询命中常见值时。以下做法默认应被评审阻止：

```sql
CREATE INDEX idx_record_status ON record (status);
```

但低区分度列位于复合索引第一列并不必然错误。例如：

```sql
WHERE status = ? AND created_at >= ?;
```

`(status, created_at)` 使用前导等值条件和后续时间范围，可以形成有效的连续扫描区间。若绝大多数查询固定查询同一个常见状态，下面的部分索引可能更小：

```sql
CREATE INDEX idx_record_active_created_at
    ON record (created_at)
    WHERE status = 'active';
```

二者必须通过数据分布、参数化方式和执行计划比较。不能只看 `status` 的不同值数量下结论。

### PostgreSQL 18 skip scan

PG18 的 B-tree skip scan 可以在缺少前导列条件时，为少量不同的前导值反复生成内部等值搜索。例如 `(category, item_id)` 在 `category` 只有少数值时，可能服务仅按 `item_id` 查询的场景。

skip scan 是优化器按成本选择的执行策略，不是忽略列顺序的理由：

- 前导列不同值过多时，反复搜索可能接近扫描整个索引，优化器通常不会采用。
- 必须通过 PG18 `EXPLAIN ANALYZE` 的 `Index Searches` 验证，不能凭索引定义推测。
- 若缺失前导列的查询本身高频，仍应比较独立索引或调整复合索引。

### 控制复合索引宽度

多列索引应谨慎使用。超过三个搜索键列的索引必须提供明确的稳定查询和执行计划证据。不能把所有过滤列、排序列和返回列全部塞入一个索引。

## 选择率与数据分布

选择率关注查询实际命中的行数比例，而不是字段理论上有多少个不同值。审计时优先查看 `pg_stats`：

- `n_distinct`：估算不同值数量；负数表示不同值数量随表规模增长。
- `most_common_vals`、`most_common_freqs`：识别常见值和数据倾斜。
- `null_frac`：识别空值比例。
- `correlation`：判断列顺序与物理行顺序的相关性，相关性高时范围索引扫描的随机 I/O 成本可能更低。
- `histogram_bounds`：理解非高频值的范围分布。

不能根据测试库中的几行种子数据判断生产选择率，也不能假设均匀分布。发现估算行数与实际行数差异较大时，应先确认 `ANALYZE` 是否及时、统计目标是否足够，以及多列是否相关。

多个条件相关时，盲目增加复合索引未必能解决错误计划。只有真实计划因相关性误估而受损时，才为实际共同使用的列考虑 `CREATE STATISTICS` 的 `dependencies`、`ndistinct` 或 `mcv` 扩展统计。

## 部分索引

部分索引适合：

- 排除查询很少关心的常见值；
- 只索引占比较小但访问频繁的状态集合；
- 对软删除后的有效记录实施唯一性；
- 避免对历史数据承担持续索引维护成本。

软删除场景通常优先比较：

```sql
CREATE INDEX idx_record_owner_active
    ON record (owner_id)
    WHERE deleted_at IS NULL;
```

而不是：

```sql
CREATE INDEX idx_record_deleted_owner
    ON record (deleted_at, owner_id);
```

部分索引只有在优化器能够证明查询条件蕴含索引谓词时才可用。谓词通常需要与查询条件直接匹配；通用参数化条件无法在规划时保证蕴含固定谓词。因此设计前必须核对 ORM 生成的真实 SQL 和 prepared statement 行为。

禁止为每个状态值建立一组互斥的部分索引来模拟分区。若数据规模确实要求分区，应使用 PostgreSQL 分区能力。

## 排序、分页与 LIMIT

只有 B-tree 能直接产生有序输出。索引排序的价值在 `ORDER BY ... LIMIT n` 场景尤其明显，因为匹配索引可以在取得前 N 行后停止，而显式排序可能需要处理完整候选集合。

- 单列 B-tree 可以正向或反向扫描，通常不需要同时维护 ASC 和 DESC 两个索引。
- 多列混合方向如 `ORDER BY a ASC, b DESC` 可能需要显式定义方向。
- Bitmap Scan 会按物理位置访问表并丢失原索引顺序，之后仍可能需要排序。
- 分页必须审计实际顺序和稳定的尾部唯一键；不能只为 `OFFSET` 很大的查询继续堆索引，应考虑游标分页。

## 多个单列索引与复合索引

PostgreSQL 可以通过 BitmapAnd 或 BitmapOr 合并多个索引，因此不能看到多个条件就必然创建复合索引。

- 查询经常分别只使用 `x`、只使用 `y`，偶尔同时使用二者时，两个单列索引可能更灵活。
- 查询高频同时使用 `x`、`y`，且还要求排序或较小 `LIMIT` 时，复合索引通常更有优势。
- Bitmap 合并需要扫描多个索引并构建位图，而且会丢失索引顺序。
- 是否同时保留 `(x, y)` 和 `y`，取决于独立查询频率、skip scan 效果和写入成本。

禁止默认同时创建 `x`、`y`、`(x, y)` 三个索引。

## 覆盖索引与 INCLUDE

`INCLUDE` 列只是叶子节点中的载荷：不能用于索引搜索、排序或唯一性判断，只可能帮助 Index Only Scan。

只有同时满足以下条件时才考虑覆盖索引：

- 查询稳定且高频，返回列集合较小；
- 表的大部分数据较少更新，使 visibility map 中有足够多的 all-visible 页面；
- `EXPLAIN ANALYZE` 显示显著减少 Heap Fetches 和缓冲区访问；
- 增加的索引尺寸和写入成本可接受。

宽字符串、大对象、高频更新列以及 `SELECT *` 不得通过 `INCLUDE` 填满索引。PG18 中带非键列的 B-tree 不使用去重优化，这也是额外成本。即使物理上能够 Index Only Scan，表频繁更新导致的 Heap Fetches 仍可能让覆盖索引没有收益。

## 表达式、文本与特殊索引类型

- B-tree：默认选择，适用于等值、范围和排序。
- Hash：只支持等值，B-tree 已能覆盖大多数等值场景，没有实测优势时不使用。
- GIN：适用于全文、数组、JSONB 等多值包含查询，写入和维护成本较高。
- GiST：适用于范围、几何、邻近搜索及相应操作符类。
- SP-GiST：适用于可以进行空间分区的数据结构和相应操作符类。
- BRIN：适用于非常大的表，且被索引值与物理写入顺序高度相关；索引很小但属于有损过滤。

表达式索引只有查询使用匹配表达式时才生效。例如大小写无关等值查询可以考虑 `lower(username)`，但必须确认 SQL 实际写成相同表达式。普通 B-tree 不应被假定能够优化任意 `ILIKE '%keyword%'`；模糊匹配应根据真实查询评估 `pg_trgm` 的 GIN/GiST 操作符类。

任何非默认索引类型、非默认操作符类、表达式索引和模糊搜索索引都必须写明目标运算符和执行计划证据。

## 唯一、重复与前缀索引

PostgreSQL 会为主键和唯一约束建立唯一索引，不得再创建键列相同的普通索引。判断两个索引是否重复时必须同时比较：

- 索引方法；
- 键列、顺序和 ASC/DESC/NULLS 顺序；
- 表达式、Collation 和操作符类；
- `WHERE` 谓词；
- `INCLUDE` 列；
- 唯一性和约束语义。

`(a, b)` 可以服务只约束 `a` 的查询，但不能仅凭左前缀原则自动删除 `(a)`：较小的 `(a)` 可能对极高频查询更省缓存和 I/O，唯一性语义也可能不同。反过来，也不能在没有执行计划和使用统计的情况下保留所有前缀索引。

关联表的复合主键 `(a_id, b_id)` 通常能够服务从 `a` 查询 `b`，但不能当然地高效服务从 `b` 查询 `a`。由于 DogX 不使用物理外键，关联校验、反向查询和应用层删除保护所需索引必须按真实访问方向显式评审。

## 写入与维护成本

每增加一个索引都会带来至少一部分以下成本：

- INSERT、DELETE 和涉及索引键、索引表达式依赖列或 `INCLUDE` 列的 UPDATE 维护；
- WAL、复制流量和备份体积；
- 页分裂、缓存占用、VACUUM 和重建成本；
- 更新索引相关列时降低 HOT Update 的机会；
- 迁移时间、锁等待和故障恢复复杂度。

读多写少表可以接受更多针对性索引；高写入表需要更高的收益门槛。审计时结合 `pg_stat_user_tables` 的插入、更新、删除、HOT 更新和扫描统计，不只看查询端。

## 审计流程

### 1. 建立索引清单

从真实 Schema 读取定义，不以 GORM 标签为准：

```sql
SELECT
    ns.nspname AS schema_name,
    tab.relname AS table_name,
    idx.relname AS index_name,
    pi.indisprimary,
    pi.indisunique,
    pi.indisvalid,
    pg_size_pretty(pg_relation_size(idx.oid)) AS index_size,
    pg_get_indexdef(idx.oid) AS index_definition,
    pg_get_expr(pi.indpred, pi.indrelid) AS predicate
FROM pg_index AS pi
JOIN pg_class AS idx ON idx.oid = pi.indexrelid
JOIN pg_class AS tab ON tab.oid = pi.indrelid
JOIN pg_namespace AS ns ON ns.oid = tab.relnamespace
WHERE ns.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY ns.nspname, tab.relname, idx.relname;
```

标记主键/唯一约束索引、精确重复索引、疑似前缀重叠索引、无明确目标查询的索引、无效索引和异常宽索引。

### 2. 建立查询清单

静态阶段检索 Repository、手写 SQL、GORM 查询、排序和分页；生产阶段使用 `pg_stat_statements` 按完整业务周期收集：

- 调用次数；
- 总执行时间和平均执行时间；
- 返回或影响行数；
- shared/temp block 读写；
- WAL 量；
- 统计窗口开始时间、调用次数及单位调用成本。

优先优化总资源贡献高、尾延迟高或影响关键路径的查询，不以单次最慢查询作为唯一排序。

### 3. 检查统计与估算

确认自动 ANALYZE 正常，再检查 `pg_stats` 和必要的扩展统计。估算行数严重偏离实际行数时，先修复统计问题；错误估算可能导致一个正确索引仍被错误使用或完全不使用。

### 4. 对目标查询比较执行计划

使用具有代表性的数据比较变更前后：

```sql
EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS)
SELECT ...;
```

重点检查：

- estimated rows 与 actual rows；
- 各节点 `loops`；
- `Index Cond` 与普通 `Filter` 的边界；
- `Rows Removed by Filter` 或 `Rows Removed by Index Recheck`；
- PG18 `Index Searches`；
- Index Only Scan 的 `Heap Fetches`；
- shared/temp buffers 和排序是否落盘；
- Planning Time、Execution Time 及重复测试的稳定性。

`EXPLAIN ANALYZE` 会真实执行 SQL。生产环境仅在确认负载和副作用可接受时分析只读查询；写语句优先在隔离的代表性环境验证，不能因为准备了 `ROLLBACK` 就忽略触发器、序列、外部副作用和锁风险。禁止用 `enable_seqscan = off` 得到的强制计划证明索引有效；它只能用于诊断候选路径。

### 5. 检查真实索引使用

```sql
SELECT
    schemaname,
    relname AS table_name,
    indexrelname AS index_name,
    idx_scan,
    last_idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
ORDER BY idx_scan, pg_relation_size(indexrelid) DESC;
```

解释这些数字时必须注意：

- PG18 的 `idx_scan` 统计索引搜索次数，不等于业务 SQL 执行次数；`IN`、转换后的 OR 和 skip scan 一次执行可能产生多次搜索。
- Bitmap Scan 会增加索引的 `idx_tup_read`，但表行获取计入表级统计，不计入该索引的 `idx_tup_fetch`。
- Index Only Scan 可能导致 `idx_tup_read` 明显高于 `idx_tup_fetch`。
- 统计可能因重启异常、恢复或人工 reset 丢失；必须覆盖周任务、月任务和低频关键任务。
- 主库未使用不代表只读副本未使用；不同节点必须分别观察。
- 主键、唯一约束和保障正确性的索引不能因 `idx_scan = 0` 删除。

### 6. 形成逐索引结论

每个索引审计项至少记录：

```text
表 / 索引：
约束职责：
目标查询：
过滤、排序与 LIMIT：
数据分布与选择率：
调用频率与资源贡献：
执行计划证据：
索引大小与表写入强度：
重叠索引：
结论：保留 / 新增 / 修改 / 删除 / 等待生产证据
上线与回退方式：
```

没有足够证据时，结论必须写“等待生产证据”，不能让 AI 猜测生产负载。

## 生产变更

- 空表、新表或明确维护窗口内可以使用普通 `CREATE INDEX`。
- 对持续写入的大表增加索引，通常使用 `CREATE INDEX CONCURRENTLY`；它需要更多时间和 I/O，但不会在整个构建期间阻塞写入。
- `CREATE INDEX CONCURRENTLY` 不能在事务块中执行。使用 Goose 时必须放在独立迁移，并使用 Goose 的非事务迁移能力。
- 并发构建失败可能留下仍产生写入成本的 INVALID 索引，上线流程必须检查 `pg_index.indisvalid` 并准备删除或 `REINDEX INDEX CONCURRENTLY`。
- 同一张表一次只能执行一个并发索引构建；必须评估长事务、CPU、I/O、WAL 和复制延迟。
- 删除生产索引前保存完整 DDL、确认统计观察窗口、检查所有节点和低频任务，并准备并发重建回退方案。
- 不把定期 `REINDEX` 当作常规优化动作；只有确认膨胀、损坏或明确维护原因时才执行。

## 代码评审拒绝条件

出现以下任一情况，索引变更默认不得通过：

- 没有目标约束或真实查询；
- 因字段出现在 `WHERE` 中就逐列建索引；
- 低区分度单列索引没有数据分布和计划证据；
- 复合列顺序只用“区分度最高优先”或“最左前缀”解释；
- 依赖 PG18 skip scan 却没有执行计划证据；
- 与主键、唯一约束或现有索引精确重复；
- 超过三个搜索键列但没有稳定查询和计划证据；
- 使用 `INCLUDE` 覆盖宽列、高频更新列或 `SELECT *`；
- 用普通 B-tree 优化无法匹配其操作符类的模糊搜索；
- 只凭 `idx_scan = 0` 删除索引；
- 在生产大表上用普通 `CREATE INDEX`，却没有锁影响说明；
- 使用 GORM `AutoMigrate` 或 Model 标签绕过 Goose 迁移。

## DogX 约定

- Goose SQL 是数据库索引结构的唯一事实源，GORM Model 标签不是已部署索引的证据，DogX 不使用 `AutoMigrate` 管理 Schema。
- 普通索引使用 `idx_<table>_<purpose>`，唯一索引使用 `uk_<table>_<purpose>`；名称表达业务目的，不要求机械列出所有列名。
- DogX 不使用物理外键，因此不会为了外键机制机械创建索引；但应用层关联校验、反向查询、删除保护和清理必须有相应访问路径。
- 项目初期没有真实生产负载时，只建立约束索引和能够由现有查询确定的必要索引，不提前建立推测性的覆盖索引与特殊索引。

## 权威资料

PostgreSQL 18 官方文档用于确定数据库的真实行为和版本边界；书籍用于补充设计方法、案例和解释，二者冲突时以对应 PostgreSQL 版本的官方文档和实际执行计划为准。

- [PostgreSQL 18：Chapter 11. Indexes](https://www.postgresql.org/docs/18/indexes.html)
- [PostgreSQL 18：CREATE INDEX](https://www.postgresql.org/docs/18/sql-createindex.html)
- [PostgreSQL 18：Using EXPLAIN](https://www.postgresql.org/docs/18/using-explain.html)
- [PostgreSQL 18：Planner Statistics](https://www.postgresql.org/docs/18/planner-stats.html)
- [PostgreSQL 18：Cumulative Statistics](https://www.postgresql.org/docs/18/monitoring-stats.html)
- [PostgreSQL 18：pg_stat_statements](https://www.postgresql.org/docs/18/pgstatstatements.html)
- [Mastering PostgreSQL 17, Sixth Edition](https://www.packtpub.com/en-us/product/mastering-postgresql-17-9781836205975)，Hans-Jürgen Schönig，2024
- [PostgreSQL Query Optimization, Second Edition](https://link.springer.com/book/10.1007/979-8-8688-0069-6)，Henrietta Dombrovskaya、Boris Novikov、Anna Bailliekova，2024；示例基于 PostgreSQL 15，只吸收跨版本的查询优化方法
- [PostgreSQL 14 Internals](https://postgrespro.com/community/books/internals)，Egor Rogov、Liudmila Mantrova
- [Use The Index, Luke!](https://use-the-index-luke.com/sql/table-of-contents)，Markus Winand
