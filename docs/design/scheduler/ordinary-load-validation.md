# 普通任务代表性负载验收

状态：2026-11-06 本机代表性负载验收通过。以下场景和门槛在运行前经用户确认，最终运行没有调整负载规模、间隔或性能门槛。本轮仅增加测试与验收记录，未修改生产运行逻辑。

## 范围与通过标准

| 项目 | 本次固定条件 |
| --- | --- |
| 拓扑 | 两个 Job 调度进程、一个 System 内嵌执行器；真实 PostgreSQL、Redis、etcd 和 gRPC |
| 规则 | 500 条启用规则，其中 100 条每 30 秒同秒到期，连续五轮；另 400 条规则的下一次执行在未来 |
| 业务 | 测试专用处理器先保存业务开始记录，等待 100ms，再保存完成记录；等待期间不持有数据库连接；返回固定小 JSON 结果 |
| 正确性 | 五轮共 500 个 Instance、500 个普通 Task、500 次业务调用全部成功；每轮每 Job 唯一，尝试编号及使用次数均为 1；400 条未来规则不提前执行，无残留活跃记录 |
| 业务开始延迟 | 每轮从计划时刻到执行器业务开始记录的 P95 ≤ 3 秒、P99 ≤ 5 秒 |
| 整轮排空 | 每个波次的 100 个父 Instance 在计划时刻后 10 秒内全部成功，按独立数据库查询已经可见的状态判断 |
| 资源观察 | 记录各进程 Go 堆内存及 goroutine 的空闲样本、采样峰值及排空后样本；数据库连接数、活动连接、锁等待、积压；不把采样峰值称作精确峰值 |

这是指定机器和载荷下的短时基准，不是生产 SLA、产品规模上限、极限吞吐、长期无泄漏或竞品性能结论。分片、Pre／Post、历史数据清理和故障压测不叠加在本次场景内；已有功能及故障验收另见[普通模块记录](ordinary-readiness-review.md)。

## 环境与执行约束

- Windows 11 Pro（10.0.26200），Intel Core i5-13500H，16 个逻辑处理器，物理内存约 31.8 GiB；运行前可用内存约 6.4 GiB，工作站仍运行其他应用。
- Go 1.27.0 windows/amd64；各 Go 进程 `GOMAXPROCS=2`，构建和测试 `-p 2`；不并行运行其他 Go 测试命令。
- PostgreSQL 18.6 使用本次独立目录、回环端口和以 `_test` 结尾的数据库；测试再创建独立 Schema。Redis 7.4.2 使用独立回环进程和测试前缀；etcd 3.5.21 由测试自行启动并清理。
- 保持产品默认派发补查间隔 1 秒、派发并发 64、业务并发 32、业务等待 128、执行器结果保留 256／回报并发 16、调度端结果保留 512／保存并发 8。Job 每进程数据库池最多 10，System 最多 5；测试连接单独计数。
- 管理 RPC 创建与启停规则，真实 Cron 自动触发；不写 Job 业务表、不手动触发冒充集中定时调度。完成第五轮后通过管理 RPC 停用这 100 条规则，避免产生第六轮。
- 测试宿主调用生产 `run` 装配，仅注入测试业务处理器及进程内资源采样；不开启 DevServer，不更改生产入口或依赖。测试资源记录不包含业务参数或凭据。
- 用例使用 `integration,e2e,load` 标签，独立于日常 CI；性能值依赖机器，不新增到每次提交的固定 CI 门槛。

## 测量口径

业务表的 `started_at`／`finished_at` 使用 PostgreSQL `clock_timestamp()`；前者表示业务处理器已经开始并写入记录，后者表示测试业务完成写入。Task 的 `started_at` 是尝试登记时间，不能拿来代替业务实际开始。

Instance／Task 的持久化时间字段在提交前赋值，时间差只称为记录时间差，不称为精确 COMMIT 延迟。每轮全部完成另由外部有限轮询确认；观察时刻包含采样和查询延迟。

每 500ms 的采样可能漏掉短暂尖峰。`pg_stat_activity` 的锁等待采样不能完整计数 NOWAIT 冲突；NOWAIT 快速失败的正确性由已有双事务集成测试覆盖，本次只在确有额外证据时报告冲突数。零次等待样本不等于没有竞争。

成功结果可以先于接单 ACK 的绑定保存；这种终态 Task 的执行器身份和地址允许同时为空，迟到 ACK 不再修改终态。验收拒绝半绑定，统计双空终态数量，但不强制所有成功 Task 都有绑定。该规则沿用[结果保存与 ACK 契约](data-contracts.md#task-result-persistence)，不改变产品实现。

## 重复运行

准备独立 PostgreSQL 测试库、独立回环 Redis 和 etcd 可执行文件，设置既有 `DOGX_TEST_POSTGRES_DSN`、`DOGX_TEST_REDIS_HOST`、`DOGX_TEST_ETCD_BINARY`。不得使用业务库或私密配置。Windows 仓库根目录执行以下命令；固定路径程序与现有 E2E 共用，因此不要并行运行。

```powershell
$env:GOMAXPROCS = '2'
$env:DOGX_E2E_JOB_RPC_BINARY = Join-Path (Get-Location) 'bin/windows-tests/e2e-job-rpc.exe'
$env:DOGX_E2E_JOB_MIGRATE_BINARY = Join-Path (Get-Location) 'bin/windows-tests/e2e-job-migrate.exe'
$env:DOGX_E2E_SYSTEM_EXECUTOR_BINARY = Join-Path (Get-Location) 'bin/windows-tests/e2e-system-executor.test.exe'
go build -p 2 -o $env:DOGX_E2E_JOB_MIGRATE_BINARY ./apps/job/cmd/migrate
go test -p 2 -c '-tags=integration,e2e,load' -o $env:DOGX_E2E_JOB_RPC_BINARY ./apps/job/rpc
go test -p 2 -c '-tags=integration,e2e,load' -o $env:DOGX_E2E_SYSTEM_EXECUTOR_BINARY ./apps/system/rpc
$env:DOGX_LOAD_RESOURCES = '1'
$env:DOGX_LOAD_REPORT = Join-Path (Get-Location) 'bin/test-tools/ordinary-load-report.json'
$testRunner = 'pwsh -NoProfile -File "' + (Resolve-Path 'scripts/test-exec-windows.ps1').Path + '"'
go test -p 2 -exec $testRunner '-tags=integration,e2e,load' -run '^TestJobOrdinaryLoadEndToEnd$' -count=1 -timeout=6m -v ./apps/system/e2e
```

每条构建命令必须成功后再继续。报告 `WorkloadPassed` 仅表示负载断言通过，还须结合 `go test` 最终退出码判断包括清理在内的整个用例。恢复运行其他 E2E 时重新按原命令构建生产 Job 二进制及普通测试宿主，并清除 `DOGX_LOAD_RESOURCES`；不要沿用本次带采样的测试程序。

## 实际结果

最终运行于 2026-11-06 13:57～14:00（Asia/Shanghai），`TestJobOrdinaryLoadEndToEnd` 通过，用例 185.68 秒、包命令 186.916 秒，退出码 0。每轮 100 条，五轮共 500 个 Instance／Task／唯一业务调用全部成功；每个 Job 恰好五轮，尝试编号与使用次数均为 1，无重复调度身份、半绑定、活跃残留或未来规则提前触发。此次最终运行所有成功 Task 均已有完整绑定。

45 字节业务参数、18 字节业务 JSON 结果；100ms 是模拟等待，不包含两次业务数据库写入及调度开销。实际业务记录耗时 P50 为 101～107ms，各轮 P99 为 142～185ms。

| 波次 | 业务开始 P95 | 业务开始 P99 | 100 个父成功状态全部可见 | 判定 |
| --- | ---: | ---: | ---: | --- |
| 1 | 350.2ms | 442.8ms | 1,006.1ms | 通过 |
| 2 | 311.0ms | 336.6ms | 1,003.6ms | 通过 |
| 3 | 345.4ms | 378.7ms | 1,006.1ms | 通过 |
| 4 | 320.6ms | 342.1ms | 1,004.8ms | 通过 |
| 5 | 334.3ms | 350.1ms | 1,005.5ms | 通过 |

五轮 Instance 创建记录距计划时刻的最大差为 100.8～140.6ms；业务完成记录至 Task 结果记录的每轮 P99 为 45.3～108.8ms；Task 结果记录至父完成记录的每轮 P99 为 149.2～218.8ms。这些时间差均不是物理提交耗时。

资源基线取首次到期前最后一份样本，尾部取第五轮完成、停用规则并观察 10 秒后的最后样本。每个宿主取得 362～363 份有效样本；峰值窗口包含规则准备阶段，只覆盖 500ms 采样点。内存是 Go 堆，不是操作系统工作集或全部进程内存。

| 进程 | goroutine：基线／采样峰值／尾部 | HeapAlloc：基线／采样峰值／尾部 | HeapSys 采样峰值 |
| --- | --- | --- | ---: |
| Job A | 211／224／222 | 6.57／11.85／7.84 MiB | 21.44 MiB |
| Job B | 171／172／171 | 6.26／9.36／8.98 MiB | 18.03 MiB |
| System 执行器 | 102／107／105 | 5.33／10.32／5.93 MiB | 18.41 MiB |

| 数据库采样 | Job A | Job B | System |
| --- | ---: | ---: | ---: |
| 连接数采样峰值 | 4 | 4 | 2 |
| 活动连接采样峰值 | 1 | 4 | 0 |
| 锁等待连接采样峰值 | 0 | 0 | 0 |
| 采到的最老活动语句时长最大值 | 2.05ms | 8.99ms | 未采到活动语句 |

数据库每 500ms 一次查询，可能完全漏掉短语句，因此 System 的零活动样本不表示它没有执行数据库操作。积压采样峰值为 RUNNING Instance 44、未结束 Task 7、运行中业务记录 5、业务已成功但 Task 尚未成功 2；WAITING Instance 未被采到。上述值不能代替队列内部真实峰值，最终所有活跃及未保存结果记录均为零。

最终运行时间窗口的独立 PostgreSQL 日志未出现 `55P03`（NOWAIT）、`40P01`（死锁）或 `53300`（连接不足）错误，锁等待采样也为零；不能据此宣称所有节点竞争路径已被覆盖或证明极限吞吐。首次未完成运行中另捕获过一条父 Instance NOWAIT 错误。确定性交错继续由既有仓储双事务测试验证。

两个调度进程在线不等于已经测得派发的均摊比例。本次没有逐节点派发计数，不据此推导负载均衡比例或跨节点竞争的性能收益。

首次运行在第四轮的复合断言处失败；当时断言错误地要求成功 Task 绑定非空，与允许结果先于 ACK 保存的既有契约不一致。该次夹具已自动清理，未保留具体失败行，不能将某一字段当作已经取证的失败原因。修正验收断言为双空或双有、加入字段分类诊断及终态绑定数量统计后，完整重跑五轮通过；未因此修改产品代码或放宽性能门槛。

原始报告与日志保存在本机忽略目录 `bin/test-tools/ordinary-load-20261106/`：`report.json`、`run.log` 为最终运行，`initial-report.json`、`initial-run.log` 保留首次运行。辅助部署及执行脚本同目录保存；正式可重复入口是上方命令及仓库测试源码。测试清理独立 Schema、etcd 及业务进程；本次独立 PostgreSQL／Redis 在收尾后停止，不影响用户已有服务。

重新构建生产 Job／System RPC／API 及不带 `load` 的 System 测试宿主后，原 `TestJobOrdinaryExecutionEndToEnd` 的成功、重试和停止分支通过，包耗时 8.123 秒；本轮四个受影响包的 `integration,e2e,load` 标签 lint 通过、0 issues。负载用例的子进程清理仍为取消／必要时强制结束，不将本次退出码 0 当作优雅停机的新证据；既有停机专项和 Linux race 结论见[最新 CI 记录](ordinary-readiness-review.md#ordinary-ci-success)。

普通模块第 6 阶段的适用实测项目已齐备，本轮测试和文档增量达到可提交节点，尚未提交或推送；后续提交仍须做敏感信息检查并观察新 CI。此次没有新增覆盖率采集，不以本机负载代替此前同提交 Codecov 数据，也不表示分片及完整第一版已交付。
