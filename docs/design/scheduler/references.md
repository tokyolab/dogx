# 参考资料与决策索引

本页保留设计依据和关键决策的位置。参考项目用于比较职责、行为和取舍，不将其实现直接当作 DogX 的规范；引用版本也不表示这些项目的当前最新版本。

## 参考资料

| 来源 | 参考范围 | 适用边界 |
| --- | --- | --- |
| [字节跳动《如何做好分布式任务调度——Scheduler 的一些探索》](https://developer.volcengine.com/articles/7599493782568402995) | 飞书 Scheduler 的职责划分、分片处理入口和任务建模 | 架构文章，不是可直接接入的开源组件，也未公开全部表结构与协议 |
| [robfig/cron v3.0.1](https://github.com/robfig/cron/blob/v3.0.1/cron.go) | 长期规则注册、规则排序和 Timer 等待 | 提供解析与内存触发，不负责平台持久化错失恢复和集群协调 |
| [XXL-JOB b34acec](https://github.com/xuxueli/xxl-job/tree/b34acec41d811a91d8efa0b67de20d283100991b) | 错失、阻塞、失败重试和执行超时对照 | 其执行器本地 Job 阻塞判断不等于 DogX 集群 Instance 级阻塞 |
| [XXL-JOB 2.4.1 任务管理](https://github.com/xuxueli/xxl-job/blob/2.4.1/xxl-job-admin/src/main/java/com/xxl/job/admin/controller/JobInfoController.java)、[执行记录管理](https://github.com/xuxueli/xxl-job/blob/2.4.1/xxl-job-admin/src/main/java/com/xxl/job/admin/controller/JobLogController.java) | 第一版范围评审中的任务操作、手动触发、停止与记录清理对照 | 停用 Job 与终止执行不是同一操作；其执行器 Job 线程终止不等于 DogX 整轮分片取消 |
| [SnailJob cacc2b6](https://github.com/aizuda/snail-job/tree/cacc2b698c1880552d4b0c3582b44713dc350566) | 批次／Task、重试、日志及执行协调概念 | 不照搬分桶多活触发、数据库文本日志或其全部状态协议 |
| [XXL-JOB 2.4.1 执行器组持久化](https://github.com/xuxueli/xxl-job/blob/2.4.1/xxl-job-admin/src/main/resources/mybatis-mapper/XxlJobGroupMapper.xml)、[注册维护](https://github.com/xuxueli/xxl-job/blob/2.4.1/xxl-job-admin/src/main/java/com/xxl/job/admin/core/thread/JobRegistryHelper.java) | 后台维护组并存数据库，按 AppName 汇集自动注册地址 | 自动注册实例不等于自动创建后台组；DogX 不照搬其数据库心跳注册机制 |
| [SnailJob 组管理](https://snailjob.opensnail.com/docs/guide/common/group_manage.html)、[cacc2b6 组保存逻辑](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/service/impl/GroupConfigServiceImpl.java) | 页面维护组元数据，客户端配置对应组并动态注册实例 | DogX 已独立确认单进程单组及组凭据，不照搬命名空间、公开默认 Token、生成方式或组启停语义 |

字节文章作者为张宇轩、章逸、曾丹。后续新增比较结论时，应记录相应版本或直接证据；不能将某个版本的行为无条件外推。

## 内部信任与服务发现依据 {#internal-trust-evidence}

| 设计点 | XXL-JOB 2.4.1 | SnailJob cacc2b6 | DogX 已确认选择 |
| --- | --- | --- | --- |
| 机器凭据 | 部署级共享 accessToken，注册／回报及派发／停止接收端校验 | 按命名空间及组取得 Token，注册／回报及派发／停止主路径校验 | 按执行器组共享 Token，校验派发／取消／回报；回报再校验 Task 归属与当前尝试 |
| 空配置与默认值 | 接收端自身配置为空时跳过校验 | 客户端未配置时回退公开默认 Token，不等于关闭认证 | 后端安全随机生成，不使用公开默认值，不以空值或取凭据失败绕过认证 |
| 实例注册 | 向调度服务注册，通过数据库维护地址 | 客户端心跳经调度服务队列、批量落库及缓存同步维护地址 | go-zero 直接向 etcd 注册与发现；etcd 权限独立于组 Token，不另建调度端注册中心 |

上述比较只针对列出的版本和内部通信路径，不推广为所有版本、协议或管理接口的共同行为。SnailJob 标准 Java SDK 的 group、token 为单值，一个应用实例接入一个组；初始化脚本提供默认命名空间，但未初始化默认执行器组。自动注册实例不等于自动创建组。共享口令只能证明持有凭据，不能证明单个进程身份或提供传输加密。

直接证据：

- XXL-JOB：[注册及回报入口校验](https://github.com/xuxueli/xxl-job/blob/2.4.1/xxl-job-admin/src/main/java/com/xxl/job/admin/controller/JobApiController.java)、[执行器接收校验](https://github.com/xuxueli/xxl-job/blob/2.4.1/xxl-job-core/src/main/java/com/xxl/job/core/server/EmbedServer.java)。
- SnailJob：[服务端组 Token 校验](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-common/src/main/java/com/aizuda/snailjob/server/common/rpc/server/grpc/GrpcRequestHandlerActor.java)、[执行器接收校验](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-common/src/main/java/com/aizuda/snailjob/client/common/rpc/supports/handler/SnailDispatcherRequestHandler.java)、[客户端单组配置](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-common/src/main/java/com/aizuda/snailjob/client/common/config/SnailJobProperties.java)、[初始化 SQL](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/doc/sql/snail_job_mysql.sql)。
- SnailJob 自管注册：[注册队列与跨节点刷新](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-common/src/main/java/com/aizuda/snailjob/server/common/register/ClientRegister.java)、[批量持久化](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-common/src/main/java/com/aizuda/snailjob/server/common/register/AbstractRegister.java)、[实例缓存与存活检查](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-common/src/main/java/com/aizuda/snailjob/server/common/handler/InstanceManager.java)。它支持多副本，并非单点内存地址表；DogX 不采用它是为了复用既有基础设施，不是判定该路线天然不可靠。
- go-zero v1.10.3：[注册与续租](https://github.com/zeromicro/go-zero/blob/v1.10.3/core/discov/publisher.go)、[地址加载与监听](https://github.com/zeromicro/go-zero/blob/v1.10.3/core/discov/internal/registry.go)、[先注册后启动 RPC](https://github.com/zeromicro/go-zero/blob/v1.10.3/zrpc/internal/rpcpubserver.go)。运行中订阅失败不立即清空已有地址，不代表冷启动可以不依赖 etcd；不能把库的默认行为当作已满足 DogX 的能力就绪目标。
- etcd：[Lease／Watch API](https://etcd.io/docs/v3.6/learning/api/)、[故障边界](https://etcd.io/docs/v3.6/op-guide/failures/)。租约与目录一致性不能证明 Task 正常执行；高可用取决于部署及多数节点可用，不能推导任务恰好执行一次。

## 状态与生命周期依据 {#lifecycle-evidence}

本次对照固定为 XXL-JOB 2.4.1（提交 `92577cc33e3974a2f46d9223ed9a405a03c8ee1d`）和 SnailJob `cacc2b6`。两者模型不同，不把执行记录结果码、批次状态和业务线程是否退出视为同一事实。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| Job 启停 | 计划触发开关，不直接终止既有执行 | Job 启停与批次停止分开 | Job 启停不代替 Instance 停止 |
| 整轮与子任务 | 广播分片各建执行记录，以调度与执行结果码表达结果，没有对应的独立整轮聚合对象 | 批次状态为等待、运行、成功、失败、停止、取消；子任务另有状态 | Instance 五态、Task 六态；状态、Task 类型、结束原因分开 |
| 自动重试等待 | 重试另建记录，旧记录保留原失败结果 | 自动回调重试分支保留原运行状态，等待后再次执行 | 原 Task 进入等待重试，失败仅表示最终失败，Instance 仍运行中 |
| 手动停止与超时 | 手动终止记录失败码 500；执行超时码 502 | 两者进入停止处理流程，以手动停止或执行超时原因区分 | 手动停止为 Instance 取消；整轮超时为失败加原因，不增加超时状态 |
| 空分片参数 | 没有对应的准备数组模型 | SDK 的 `doMap` 拒绝空列表 | 合法空数组为零分片生成完成，有 Post 则执行，无 Post 则成功结束；受终止及期限约束 |

SnailJob 的空列表校验不能外推为所有“无业务数据”任务都必定失败；处理器是否调用 `doMap` 是另一层业务行为。两家的线程中断或平台停止状态均不能证明业务副作用已经停止。DogX 的父子终态收敛、整轮截止优先顺序和统一 Task，是本项目独立确认的语义，不能反推为竞品具有相同协议。

直接证据：

- XXL-JOB：[Job 启停](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/service/impl/XxlJobServiceImpl.java#L329-L348)、[执行结果码](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-core/src/main/java/com/xxl/job/core/context/XxlJobContext.java#L11-L13)、[重试入口](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/core/thread/JobFailMonitorHelper.java#L53-L58)、[广播及每次新建记录](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/core/trigger/XxlJobTrigger.java#L69-L123)、[手动终止](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/controller/JobLogController.java#L186-L200)。
- SnailJob：[批次状态](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-common/snail-job-common-core/src/main/java/com/aizuda/snailjob/common/core/enums/JobTaskBatchStatusEnum.java#L16-L55)、[子任务状态](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-common/snail-job-common-core/src/main/java/com/aizuda/snailjob/common/core/enums/JobTaskStatusEnum.java#L17-L51)、[自动重试分支](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/callback/AbstractClientCallbackHandler.java#L47-L80)、[停止处理](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/stop/AbstractJobTaskStopHandler.java#L36-L75)、[超时检查](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/timer/JobTimeoutCheckTask.java#L49-L70)、[空列表校验](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-job-core/src/main/java/com/aizuda/snailjob/client/job/core/executor/MapInvokeHandler.java#L46-L54)。

## 本地派发与尝试登记依据 {#dispatch-registration-evidence}

对照固定为 XXL-JOB 2.4.1（提交 `92577cc33e3974a2f46d9223ed9a405a03c8ee1d`）及 SnailJob `cacc2b6`。比较的是正常派发与多节点协调入口，不把两家的恢复机制视为 DogX 的完整协议。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 正常派发 | 触发后交本地线程池，创建执行记录、发送 RPC，再保存派发结果 | 生成 Task 的事务提交后，通过 Actor 推进 RPC 派发 | 完整生成提交后，本地有界直接派发，不等待恢复扫描 |
| 多节点协调基础 | 调度扫描使用 `schedule_lock`，失败处理等特定环节另有条件更新 | 上游分桶决定普通 Job 的处理节点，特定环节另有协调机制 | 无整轮固定归属；正常派发与恢复共用 Task 条件登记 |
| 每次发送前的尝试登记 | 不具备与 DogX 本提案同等的逐 Task 当前尝试授权 | 普通派发路径不具备同等的发送前尝试授权 | 按容量批量登记当前尝试、预算及执行中状态；成功提交后只发送成功登记项 |

DogX 借鉴本地直接推进，但发送前的尝试登记是对等节点恢复模型下独立确认的设计，并非这两家的共同实现。批量减少数据库往返，不消除逐行写入；登记提交后到实际发送之间仍存在不确定性，不承诺恰好执行一次。完整结论见[尝试登记](execution.md#attempt-registration)及[记账边界](execution.md#dispatch-accounting)。

直接证据：

- XXL-JOB：[调度扫描与提交本地工作](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/core/thread/JobScheduleHelper.java#L69-L162)、[本地线程池](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/core/thread/JobTriggerPoolHelper.java#L72-L119)、[记录与派发顺序](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/core/trigger/XxlJobTrigger.java#L118-L196)、[失败处理条件更新](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/core/thread/JobFailMonitorHelper.java#L41-L70)。调度锁不由 RPC 工作线程继承，不能据此称其派发一定发生在扫描事务提交后。
- SnailJob：[生成事务与提交后派发](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/JobExecutorActor.java#L153-L190)、[请求执行器](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/executor/job/RequestClientActor.java#L74-L114)、[重试计数条件更新](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/callback/AbstractClientCallbackHandler.java#L107-L127)。重试计数上限不等于当前尝试唯一授权；上述结论不否认其工作流或收尾等特定环节使用锁。

## 停止与新派发协调依据 {#authorization-terminal-evidence}

本项对照 XXL-JOB `b34acec`（该提交为 3.5.0-SNAPSHOT，与其他小节引用的 2.4.1 分开）及 SnailJob `cacc2b6`。只比较指定停止与派发路径，不据此断言整个项目没有并发保护。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 停止路径 | 根据日志的执行器地址发出 jobId 级 kill，成功后将执行记录按失败完成 | 停止 RPC 与 STOP 结果通过 Actor 推进，不等待全部业务实际退出 | 父 Instance 终态先提交，子取消和 RPC 在父事务外尽力进行 |
| 与新派发协调 | 该路径没有与新触发共用的 Instance 终态授权关口 | 该停止路径与派发前检查不共用 DogX 式父批次授权关口 | 新登记与父终态共用单 Instance 主键行锁；结果回报不统一锁父 |

DogX 的登记关口是适配无整轮固定归属及并发恢复的独立选择，不是这两家的共同实现；仍接受已授权工作与停止交错、业务需要幂等，以及额外父行检查成本。规则见[授权与终止协调](execution.md#authorization-terminal)。

直接证据：XXL-JOB [停止入口](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/controller/JobLogController.java#L180-L218)与[保存记录后派发](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L147-L193)；SnailJob [停止处理](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/stop/AbstractJobTaskStopHandler.java#L38-L74)与[派发前检查](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/executor/job/RequestClientActor.java#L74-L104)。

## 提交结果不明恢复依据 {#commit-unknown-evidence}

本项核实 XXL-JOB `b34acec`（3.5.0-SNAPSHOT）及 SnailJob `cacc2b6` 的派发和结果写入路径。这些路径未将“数据库已提交但确认丢失”单独分类并统一核对恢复；结论不扩展为整个项目没有容错措施。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 派发前写库异常 | 先保存执行日志再 RPC；异常向外传播到普通错误处理 | 生成事务正常提交后由 afterCommit 派发；异常进入批次失败处理 | 登记不明放弃本次发送，事后见已登记也不补发；复用既有条件恢复 |
| 后台结果写库异常 | 该回报工作路径没有保留失败结果重入队 | 该结果 Actor 记录异常后结束，没有保留重试分支 | 已接收执行结果有界保留、退避条件重做；不是按异常推断全部未提交 |
| 重复处理与提交核对 | 普通回报入口有已完成检查，不等于提交报错后的读回协议 | 部分批次推进带未完成条件，不等于提交不明专用协议 | 原尝试与允许旧状态限制重复修改；必要异常核对按批读主库，正常路径不追加查询 |

XXL-JOB 通常入线程池后返回回报 ACK，队满时 caller-runs 会同步执行，不能概括成所有情况都先确认后落库。SnailJob 的 afterCommit 结论仅指该生成路径，不推广到所有重试派发入口。DogX 的[提交结果不明规则](execution.md#commit-unknown)是独立确认的保守取舍；BUSY 只做有界有限记账重试，不新增长期待返还队列。

直接证据：XXL-JOB [保存日志与派发顺序](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L147-L246)、[派发异常处理](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobTriggerPoolHelper.java#L121-L125)、[回报处理](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobCompleteHelper.java#L107-L148)；SnailJob [生成异常处理](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/JobExecutorActor.java#L87-L96)、[提交后派发](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/JobExecutorActor.java#L174-L188)、[结果写入异常](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/JobExecutorResultActor.java#L57-L70)。

## 派发选址与执行器绑定依据 {#executor-binding-evidence}

DogX 第一版保留原生发现与负载均衡，接单后条件保存执行器身份，不采用调度层预先选址。两条路线都需要定向取消能力；前者通常多一次绑定写入，并存在 ACK 丢失或绑定前宕机导致目标未知的窗口，后者需要应用层目录、身份元数据适配及路由管理。该取舍不表示原生负载均衡必然更快。DogX 已单独确认目标未知时的有限观察及条件结算，属于本系统的设计选择，不是下列参考框架的共同机制；本版不增加 Task 活动查询或心跳。

- SnailJob `cacc2b6` 的[实例心跳](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-common/src/main/java/com/aizuda/snailjob/client/common/handler/ClientRegister.java#L53-L59)携带客户端身份和地址，由[服务端注册入口](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-common/src/main/java/com/aizuda/snailjob/server/common/rpc/server/handler/BeatHttpRequestHandler.java#L49-L63)维护实例信息。CLUSTER [生成 Task 时](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/generator/task/ClusterTaskGenerator.java#L47-L71)先选择节点、保存 `clientInfo` 与 RUNNING 状态；事务提交后才派发。首次派发的[成功 ACK 分支](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/executor/job/RequestClientActor.java#L101-L114)只记日志，但[通信重试换节点后的监听器](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/executor/job/RequestClientActor.java#L178-L188)另有直接更新 `clientInfo` 的路径，不能概括成所有重派都先保存新目标。
- go-zero v1.10.3 [发布实例地址](https://github.com/zeromicro/go-zero/blob/v1.10.3/zrpc/internal/rpcpubserver.go#L19-L33)，标准 [resolver](https://github.com/zeromicro/go-zero/blob/v1.10.3/zrpc/resolver/internal/discovbuilder.go#L24-L34)将注册值转换为地址，不提供 DogX 所需的启动实例身份。[P2C](https://github.com/zeromicro/go-zero/blob/v1.10.3/zrpc/internal/balancer/p2c/p2c.go)选择的是可用 RPC 连接，不能据此推断长任务剩余计算量。
- gRPC Go v1.82.1 的 [Peer](https://github.com/grpc/grpc-go/blob/v1.82.1/rpc_util.go#L310-L331)在 RPC 完成后填充对端信息；地址不等于进程身份，也不是派发前的持久化目标。经代理时不能直接把对端地址当成可定向访问的执行器。

DogX 已确认的绑定边界见[执行器身份绑定](execution.md#executor-binding)，故障规则见[绑定缺失处理](execution.md#missing-binding)与[已绑定结果等待](execution.md#bound-result-wait)，验收归属见[绑定验收](verification.md#executor-binding-checks)。观察期秒数及完整事务实现仍按[评审计划](progress.md)推进，不从参考实现推导恢复承诺。

## 异步回报与确认边界依据 {#result-report-evidence}

对照固定为 XXL-JOB 2.4.1（提交 `92577cc33e3974a2f46d9223ed9a405a03c8ee1d`）及 SnailJob `cacc2b6` 的定时 Job 结果路径。接收回报、结果持久化与再次执行业务是不同事件，不能把入队或 RPC 成功当作可靠落库。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 执行器回报 | 独立内存 callback 队列，后台取出并批量发送、等待应答；队列未指定容量 | 完成回调直接发送单条异步 gRPC，不等待 ACK；没有独立的 Job 结果汇聚队列 | 独立有界结果队列，固定工作协程小批量发送并等待接收 ACK |
| 调度端 ACK | 通常提交本地线程池后返回；线程池队满时请求线程直接执行，不能说始终先 ACK 后处理 | 普通结果交给 Actor，ACK 不等待最终结果写库；ACK 前仍查询 Job／Task，失败重试还更新计数 | 基础鉴权及报文校验后成功入队即 ACK，后台校验并幂等小批量落库 |
| 同步／异步选择 | 该完成结果路径无切换参数 | 通用 RPC 构建器可选同步，但内置 Job 回报固定异步，未开放配置开关 | 第一版固定双端异步，容量及并发属于部署参数，不进入 Job 页面 |
| 本地文件补偿 | 所有 Admin 回报失败后写执行器 callbacklog 文件，后续补发结果；不是所有结果先落盘的可靠日志 | 该 Job 完成结果路径无对应的本地持久化补报机制 | 不依赖本地文件、持久卷、持久化消息队列或跨重启结果缓存 |
| ACK 后接收进程退出 | 已确认但未持久化的结果仍可能丢失，失败文件不覆盖这个窗口 | 已确认但未持久化的结果仍可能丢失，不能由此推导一定自动重派 | 明确接受结果丢失；已有持久化绑定时保持当前尝试执行中，等待有效结果、人工停止或整轮截止，不据此提前重跑 |

SnailJob [服务端上报参数文档](https://snailjob.opensnail.com/docs/guide/retry/server_report.html)中的 `@Retryable(async = ...)` 控制的是上传待重试业务及参数：同步立即发送并等待响应，异步放入滑动窗口批量上报。它属于独立 Retry 模块，不是定时 Job 完成结果的同步／异步开关，也不是调度端写库模式开关。

XXL-JOB 的文件仅在回报失败后保存，重传时还存在先删旧文件再发送的窗口；不等于可靠消息日志。它解决部分暂时断连补报，但引入对执行器本地目录的恢复依赖。DogX 选择节点可替换及纯内存有界缓冲，保留平台状态校验，不承诺原业务执行结果跨进程故障无损保存。

直接证据：

- XXL-JOB：[回报队列与批量取出](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-core/src/main/java/com/xxl/job/core/thread/TriggerCallbackThread.java#L34-L76)、[失败落盘及重传](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-core/src/main/java/com/xxl/job/core/thread/TriggerCallbackThread.java#L163-L255)、[Admin 线程池及队满处理](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/core/thread/JobCompleteHelper.java#L37-L56)、[提交线程池后返回](https://github.com/xuxueli/xxl-job/blob/92577cc33e3974a2f46d9223ed9a405a03c8ee1d/xxl-job-admin/src/main/java/com/xxl/job/admin/core/thread/JobCompleteHelper.java#L138-L151)。
- SnailJob：[执行器完成回调](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-job-core/src/main/java/com/aizuda/snailjob/client/job/core/executor/JobExecutorFutureCallback.java#L57-L100)、[异步 RPC 与同步等待分支](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-common/src/main/java/com/aizuda/snailjob/client/common/rpc/client/grpc/GrpcClientInvokeHandler.java#L72-L118)、[回报入口](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/request/ReportDispatchResultPostHttpRequestHandler.java#L41-L55)、[异步结果更新](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/JobExecutorResultActor.java#L37-L72)。
- SnailJob Retry：[待重试请求内容](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-retry-core/src/main/java/com/aizuda/snailjob/client/core/report/AbstractReport.java#L61-L86)、[滑动窗口上报](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-retry-core/src/main/java/com/aizuda/snailjob/client/core/report/AsyncReport.java#L43-L57)、[同步上传](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-retry-core/src/main/java/com/aizuda/snailjob/client/core/report/SyncReport.java#L72-L86)。

DogX 权威协议见[结果回报与确认边界](execution.md#result-report)，待验证的故障场景见[异步回报验收](verification.md#result-report-checks)。不直接继承竞品的结果丢失恢复承诺。

### ACK 后的异步写库故障

此处只比较已经成功 ACK、后台结果写库随后失败的直接消费路径，不包括 ACK 前失败，也不把业务重跑等同于原结果重放。

- XXL-JOB `3.5.0-SNAPSHOT`，固定提交 `b34acec4`：[异步批量回调](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobCompleteHelper.java#L109-L147)中的数据库异常会退出当前 Runnable，未提供原结果的落库重试；队满时请求线程直接处理的分支不属于此 ACK 后窗口。该证据与上方固定旧版本的回报比较分开。
- SnailJob `cacc2b6`：[结果 Actor](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/JobExecutorResultActor.java#L57-L70)捕获写库异常、记日志并停止自身，没有在该路径重放原结果；不能将 Actor 监督或业务重试描述为这里的持久化重试。

DogX 已选择对可恢复写库故障有界保留、退避重试和队满拒收，不按次数或保留 TTL 主动丢弃；这一选择不增加跨进程结果持久化保证，完整边界见[调度端结果保留](execution.md#scheduler-result-retention)。

## 字节处理入口的含义

| 入口 | 文章中的职责 |
| --- | --- |
| PreProcess | 分片执行前的准备，在单个节点执行 |
| ShardingProcess | 多个节点上的分片处理 |
| Notify | 接收任务整体进度变化的处理入口 |
| PostProcess | 分片执行后的收尾，在单个节点执行 |

文章将这些入口分别建模为 PreTask、ProcessTask、NotifyTask、PostTask，但没有证实四类任务一定共用一张数据库表。DogX 的统一 Task 共表是独立评审的设计选择；第一版不提供 Notify 或 NotifyTask，不影响必要的结果回报与业务 Post。

文中的进度链路是执行器上报子任务进度，Scheduler 计算整体进度，在达到通知条件时发送 NotifyTask。Notify 是调度侧向业务处理器传递进度的回调，不是执行器向调度服务回报执行结果，也不必然是短信或消息推送。文章没有说明它最终更新哪张业务或调度表。

文章中“单机调度 PreProcess，PostProcess，Notify，并行调度 PostProcess”的后一个 PostProcess，与接口说明及配图矛盾，应结合上下文理解为分片处理的笔误，不能据此设计并行的多个收尾任务。图中的准备和收尾各展示一个 Task，左右两个框表示执行前后状态。

文章的分片参数包括数据量、单片最大和最小数量、可用执行器数量，按工作项数组下标拆分并为分片创建 Task、保存分片数据；未明确原始完整数组的具体存储及保留方式。DogX 选择 PostgreSQL 持久化、etcd 选主与 RPC 回报，不能反推为文章的实现。优先级二维数组也不构成 DogX 第一版支持优先级或依赖调度的依据。

## 自动触发与进度提交依据 {#trigger-accounting-evidence}

普通定时触发对照固定为 XXL-JOB `b34acec`（3.5.0-SNAPSHOT）及 SnailJob `cacc2b6`，不将该路径的结论推广为整个项目没有其他恢复措施。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 触发协调 | 扫描事务取得全局 `schedule_lock` | 按服务端负责的桶扫描 Job | 单 Leader 正常触发及低频恢复，共用同 Job 单行短事务 |
| 进度与执行记录 | 扫描线程更新触发进度，异步触发线程另建执行日志 | 先更新下次触发时间，再通过 Actor 创建批次 | 创建 Instance 与推进 `Job.next_trigger_at` 同事务提交；跳过也按策略推进 |

源码：[XXL-JOB 调度扫描](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L76-L173)、[异步触发线程池](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobTriggerPoolHelper.java#L100-L123)、[执行日志创建](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L147-L152)；[SnailJob 进度更新与 Actor 推进](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L85-L102)、[批次创建与计时](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/generator/batch/JobTaskBatchGenerator.java#L56-L126)。

DogX 的短事务与低频恢复是独立选择：正常定时由 robfig 承担，数据库以每分钟检查补遗漏，接受发现延迟并继续应用错失策略；不把低频扫描描述成完全不查库，也不增加本地 Job 退避补试系统。完整规则见[触发恢复](behavior.md#trigger-recovery)及[触发记账](execution.md#trigger-accounting)。

## Job 配置同步与加载依据 {#job-config-sync-evidence}

对照固定为 XXL-JOB `b34acec` 和 SnailJob `cacc2b6` 的普通定时调度路径。两家以近期触发扫描推进任务，不是定期重建全部长期 Cron 规则；SnailJob 的常驻任务和工作流不在本项比较范围。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 配置发现与内存计时 | 周期读取近期 Job，时间环保存短期 Job ID | 按桶读取近期 Job，通过 Actor 推进本次时间轮任务 | etcd 单个变更提示加每分钟主库核对，robfig 保存长期规则 |
| 已经过期的计划 | 扫描同时判断正常迟到或错失策略 | 普通扫描路径仍推进一轮；严重过期时调整时间基准，不套用 DogX 的 5 秒忽略规则 | 注册后提交一次有界到期检查，保留数据库原计划及 5 秒规则；独立恢复仍每分钟查漏 |
| 重载的协调方式 | 后续扫描重新读取，不是长期规则差异加载 | 后续扫描重新读取，不是长期规则差异加载 | 每任期串行加载、合并一个待核对信号，只增删替换发生变化的规则 |

robfig v3.0.1 在首次启动时初始化条目的未来触发点，运行中添加条目时按接收处理时刻计算，不负责补建已经过去的数据库计划；`Remove` 和 `Stop` 也不强制终止已经运行的回调。因此 DogX 的整批初始化确认、注册后检查、原进度恢复和旧任期结果隔离由平台负责，不能当作库的自动能力。

etcd Watch 可按 revision 衔接读取与后续变化；历史被压缩时须重建并重新核对。它不提供 PostgreSQL 提交与通知写入的跨系统原子性，也不证明配置已在 Leader 应用。DogX 的 revision 只标识通知流位置，Job 权威仍在主库；一分钟核对保留为通知遗漏兜底，不增加 Job 配置版本表或逐 Job etcd 配置。

直接证据：

- XXL-JOB：[扫描、过期判断与时间环](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L79-L185)、[启用时从当前时间加 5 秒后计算首轮](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L367-L387)。DogX 不采用该首轮偏移。
- SnailJob：[扫描、过期时间调整与近期查询](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L85-L178)、[批次创建与计时](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/generator/batch/JobTaskBatchGenerator.java#L56-L126)。
- robfig：[运行中添加、移除及下一次计算](https://github.com/robfig/cron/blob/v3.0.1/cron.go)、[停止语义](https://github.com/robfig/cron/blob/v3.0.1/cron.go#L323-L336)。
- etcd v3.5：[从指定 revision 监听](https://etcd.io/docs/v3.5/dev-guide/interacting_v3/#watch-historical-changes-of-keys)、[历史压缩](https://etcd.io/docs/v3.5/dev-guide/interacting_v3/#compacted-revisions)。

权威流程见[配置加载与重载](execution.md#job-config-reload)，行为边界见[配置同步](behavior.md#trigger-sync)，验证要求见[配置通知与核对验收](verification.md#job-config-sync-checks)。

## etcd 选主与通知接入依据 {#etcd-integration-evidence}

DogX 当前依赖 etcd client/v3 v3.5.21，采用官方 Session＋Election；15 秒租约、短请求 5 秒、通知 1 秒及重建退避是本系统的实现初值，不是引用项目的默认参数或性能结论。XXL-JOB 普通触发通过数据库调度锁协调，SnailJob 按服务端分桶扫描，不能从两家的扫描参数直接推导 DogX 的租约时长。

| 官方行为 | 接入要求 |
| --- | --- |
| Session 默认 TTL 为 60 秒，WithTTL 只接受正值，否则保留默认值 | DogX 显式设定 15 秒初值并拒绝非法配置，不默默回退到 60 秒 |
| Session 的 Context 同时影响建租约及后续 KeepAlive | 短建租约请求与长会话生命周期分开，可先有限时 Grant，再 WithLease 关联长期会话；不能直接用 5 秒 Context 续租 |
| Done 表示会话租约不再续约、过期或被孤立，Campaign 可以长期等待 | 不将一次短请求失败当成立即失主，不为正常候选等待设置短超时；当选返回后复核生命周期 |
| Campaign 取消路径可能调用基于客户端 Context 的内部 Resign | 不保证取消后固定 5 秒完成；退出仍按本机清理完再参选约束，不能脱离旧协程堆叠新会话 |
| Put 推进 revision 并生成事件，不要求新值与旧值不同 | 固定 reload 标记足够，不引入自增版本，不按值相同过滤提示 |
| Watch 对可恢复错误内部重连；历史压缩等可关闭监听 | 不在库仍恢复时并行建立第二套监听，需要应用重建时再退避并重新核对 |

直接证据：[Session 与 TTL](https://github.com/etcd-io/etcd/blob/v3.5.21/client/v3/concurrency/session.go)、[Campaign 与取消清理](https://github.com/etcd-io/etcd/blob/v3.5.21/client/v3/concurrency/election.go)、[Watch 生命周期与恢复](https://github.com/etcd-io/etcd/blob/v3.5.21/client/v3/watch.go)、[Put 的 revision 与事件语义](https://github.com/etcd-io/etcd/blob/v3.5.21/api/etcdserverpb/rpc.proto)。两家调度路径证据见[触发协调依据](#trigger-accounting-evidence)。

固定标记是核对提示，不是 Job 配置版本或可靠消息。通知失败不主动改报已提交保存失败是 DogX 的接口取舍；PostgreSQL 与 etcd 仍非原子提交。权威规则见[etcd 接入](execution.md#etcd-integration)，验证见[接入验收](verification.md#etcd-integration-checks)。

## Leader 生命周期与首次装载依据 {#leader-runtime-evidence}

robfig v3.0.1 的注册与启动行为决定了适配层的顺序：

| 库行为 | DogX 接入约束 |
| --- | --- |
| 未运行时注册只追加条目；运行时经添加通道交给运行循环，重新进入排序 | 首次完整读取后，在未启动的 Cron 中批量注册，再启动，避免大量首次实时添加产生反复排序 |
| `Start()` 创建运行协程后返回，下一次时间的初始化发生在协程内 | 不能把 Start 返回等同于初始化完成，注册后到期检查须等待整批确认 |
| 热添加的发送返回可能早于接收方完成条目初始化 | 热加载新增／替换也在整批应用后确认一次 |
| 运行中的 `Entries()` 经运行循环取得快照，复制全部条目 | 在生命周期协调下可用于一次批次同步；不逐条调用，不把停止后快照当成就绪，不称为库的独立 readiness 接口 |
| `Stop()` 停止运行循环，返回的 Context 等待已启动的 Job 回调 | 不取消回调，不覆盖平台数据库 workers；DogX 单独管理任期取消和完整退出 |

源码：[Schedule 与 Entries](https://github.com/robfig/cron/blob/v3.0.1/cron.go#L156-L185)、[Start](https://github.com/robfig/cron/blob/v3.0.1/cron.go#L214-L223)、[运行循环](https://github.com/robfig/cron/blob/v3.0.1/cron.go#L239-L304)、[回调与 Stop](https://github.com/robfig/cron/blob/v3.0.1/cron.go#L307-L336)。

本机旧任期清理完才重新参选、进程资源复用，以及异常卡住时不叠加新任期，是 DogX 的生命周期取舍，不由 robfig 自动保证。规则不改变持久化触发进度、5 秒错失判断或合法旧事务提交，也不增加零值 Next 自愈。权威流程见[任期生命周期](execution.md#leader-runtime-lifecycle)，验证见[对应验收](verification.md#leader-runtime-checks)。

## 切主与旧配置边界依据 {#leader-config-evidence}

对照限定为 XXL-JOB `b34acec`（3.5.0-SNAPSHOT）和 SnailJob `cacc2b6` 的普通自动触发链路，不扩展为所有工作流、重试或常驻任务路径的结论。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 协调范围 | `schedule_lock` 保护扫描与进度更新，不延伸到异步日志创建和执行器 RPC | 各节点按分桶扫描；已选中的 Job、提交的 Actor 和普通计时任务没有持久化任期屏障 | etcd 选主控制正常来源，同 Job 短事务校验最新事实；不增加数据库全局任期锁 |
| 旧触发遇到停用 | 异步触发按 ID 重读 Job，缺失则返回；该入口未再次检查 `triggerStatus`，停用也不清掉全部已排队工作 | 普通执行阶段重读 Job；自动任务发现缺失或关闭，会将已有批次取消为 `JOB_CLOSED`，但此读取不是同 Job 行锁下的触发事务 | 停用与创建 Instance 在同一 Job 行上协调；停用先提交则不创建，触发先提交则保留该轮 |
| 配置快照与触发授权 | 时间环保存 Job ID，不携带配置版本或任期；异步阶段重读配置 | 扫描更新下次时间后通过 Actor 建批次；普通下次时间批量更新按 ID 匹配，不附前值、配置版本或任期条件 | 旧回调仅提示 Job ID；按最新配置、状态及 `next_trigger_at` 判断，不沿用旧快照创建 |

XXL-JOB 停用后，先前排队的触发仍可能继续记录和派发，是上述源码路径推导出的故障窗口，不代表固定残留次数或持续时长。SnailJob 的桶集合可随重平衡更新，但这不会自动撤销已经进入后续阶段的全部工作；其常驻任务另有桶检查，不能由普通路径推断所有路径均不检查。

直接证据：XXL-JOB [扫描事务](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L76-L173)、[触发时重读 Job](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L63-L110)、[停用处理](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L397-L421)；SnailJob [扫描进度更新后提交 Actor](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L85-L102)、[进度更新 SQL](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-datasource/snail-job-postgres-datasource/src/main/resources/postgresql/mapper/JobMapper.xml#L5-L17)、[执行阶段启停检查](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/JobExecutorActor.java#L103-L150)。

robfig v3.0.1 的 [Stop](https://github.com/robfig/cron/blob/v3.0.1/cron.go#L323-L336) 停止后续调度并提供等待运行中 Job 结束的 Context，不强制终止已经运行的回调。DogX 每次选主另设 `leaderCtx`，不把停止 Cron 或取消 Context 当作事务回滚证明；接受旧任期合法在途提交是本系统独立确认的取舍。权威规则见[切主与配置变更边界](execution.md#leader-config-boundary)，验证要求见[切主验收](verification.md#leader-config-checks)。

## Job 编辑与启用进度依据 {#job-edit-enable-evidence}

对照固定为 XXL-JOB `b34acec` 和 SnailJob `cacc2b6` 的管理编辑与启停入口；SnailJob 的时间计算结论限定普通、非驻留 Cron Job。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 启用中编辑 | 允许；定时规则变化才主动重算下一次 | 允许；普通保存即重算下一次，包括只改名称等字段 | 不支持；名称、描述等配置也必须停用后编辑，由后端保障 |
| 停用中编辑 | 不重算下一次，由后续启用计算 | 普通保存仍计算，之后启用还会重算 | 编辑校验并保存配置，试算不更新进度；启用时统一计算持久化进度 |
| 重新启用 | 从应用当前时间加 5 秒后的下一个 Cron 时刻开始 | 从当前时间后的下一个 Cron 时刻开始 | 按启用时当前规则计算下一次，与启用状态原子保存，不补旧轮次 |
| 重复启用 | start 入口仍重新计算 | 状态相同则直接返回，不重算 | 已启用则不重算，重启、切主或配置重载也不重置 |

XXL-JOB 的额外 5 秒用于避开预读周期，不是 DogX 的迟到容忍规则。“普通字段不主动重算”也不代表其整行管理更新已与调度进度更新形成并发无损协议。上述启停结论不意味着竞品已排队的旧触发全部撤销，残留动作仍适用前节证据范围。

直接证据：XXL-JOB [编辑规则比较](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L272-L315)、[启用与停用](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L348-L421)；SnailJob [编辑重算](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L158-L194)、[启停与重复请求](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L234-L248)、[普通 Cron 计算基点](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/kit/JobKit.java#L53-L88)。

DogX 选择以停用编辑减少管理规则分支，不照搬启用中直接修改。代价是普通配置修改也需要一次停用与启用；已有 Instance 不因此取消，并沿用创建时保存的执行配置。权威规则见[编辑与重新启用](behavior.md#job-edit-enable)，验证要求见[专项验收](verification.md#job-edit-enable-checks)。

## Job 配置存储依据 {#job-storage-evidence}

对照 XXL-JOB `b34acec`（3.5.0-SNAPSHOT）与 SnailJob `cacc2b6` 的表结构：

| 设计点 | XXL-JOB | SnailJob | DogX |
| --- | --- | --- | --- |
| 平台配置 | `xxl_job_info` 使用处理器、调度规则、策略、超时等普通列 | `sj_job` 使用执行器、调度规则、策略、超时及重试等普通列 | 已确认采用普通列 |
| 业务参数 | 独立 `executor_param TEXT` | 独立 `args_str TEXT`，另有 `args_type` | 使用 TEXT，输入去首尾空白，内部内容不改；不增加格式类型列，业务处理器解释内容 |

直接证据：[XXL-JOB 表结构](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/doc/db/tables_xxl_job.sql#L38-L65)、[SnailJob PostgreSQL 表结构](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/doc/sql/snail_job_postgre.sql#L441-L472)。

以上比较只说明 Job 配置组织，不表示两家采用 DogX 的 Instance 配置冻结，也不据此照搬字段默认值、约束或删除行为。权威决定见[存储分工](data-contracts.md#job-storage)及[字段契约](data-contracts.md#trigger-field-draft)。

## Instance 执行配置依据 {#instance-execution-config-evidence}

对照固定为 XXL-JOB `b34acec` 与 SnailJob `cacc2b6`。两者均不能概括为完整配置随一轮执行冻结，已派发请求保存的值也不等于后续重试沿用全部旧配置。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 参数与配置保存 | 执行日志保存参数等字段，重试带回原参数及剩余次数 | Task 保存 `args_str/args_type`、组及运行计数等分散字段，不是完整配置快照 | 创建 Instance 时保存本轮必要执行配置，共享配置每轮一份 |
| 后续重试取值 | 重新读取 Job 的处理器、路由、超时等配置 | 重新读取 Job 的重试上限、间隔、处理器及超时等配置；业务参数等取原 Task | 全阶段及重试、恢复沿用本轮配置，不混用当前 Job |

SnailJob 的 `args_str` 是业务执行参数，不是整套 Job 配置；`retry_count` 是运行计数，不是配置的重试上限。Task 的 `ext_attrs` 是扩展字段，该生成与重试路径未将它作为完整配置快照；不能据此声称它提供本轮配置冻结。

直接证据：XXL-JOB [重试参数传递](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobFailAlarmMonitorHelper.java#L47-L55)、[触发时重读 Job](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L63-L86)；SnailJob [Task 字段](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-datasource/snail-job-datasource-template/src/main/java/com/aizuda/snailjob/template/datasource/persistence/po/JobTask.java#L23-L118)、[重试读取配置](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/callback/AbstractClientCallbackHandler.java#L120-L157)、[重试请求构建](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/callback/AbstractClientCallbackHandler.java#L46-L80)、[Task 参数映射](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/JobTaskConverter.java#L102-L122)。

DogX 选择本轮配置隔离，代价是 Instance 增加存储字节和序列化成本；不冻结业务代码、业务数据或动态资源，也不引入配置历史表。权威规则见[本轮执行配置](behavior.md#instance-execution-config)，具体验收见[配置隔离验收](verification.md#instance-execution-config-checks)。

## 触发身份与到期区间依据 {#trigger-identity-evidence}

对照固定为 XXL-JOB `b34acec`（3.5.0-SNAPSHOT）和 SnailJob `cacc2b6` 的普通 Cron 路径，SnailJob 不包含常驻任务。两者的执行记录 ID 不能直接等同于持久化计划时间身份。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 执行记录身份 | 每次触发新建执行日志，以日志 ID 标识；没有 Job 与计划时间的唯一约束 | 批次 ID 标识本轮，批次表未保存对应计划时间，也没有该组合唯一约束 | `Instance.id` 标识本轮；自动触发另以 `(job_id, scheduled_at)` 唯一，保存旧进度 |
| 迟到判断 | 旧进度迟到超过 5 秒才进入错失策略；等于 5 秒为正常到期 | 旧进度迟到超过 10 秒时，将本次准备时间改为当前时间；没有相同的忽略／补一次两选项 | 固定 5 秒，正常与错失共用自动身份 |
| 到期后的进度 | 已到期的直接触发或错失处理后，从当前时间求下次 Cron | 超过 10 秒从当前时间求下次，否则从旧进度求下次；扫描也预读未来时间 | 每次到期处理最多一轮，统一从锁后判定时间求下一次；5 秒内多轮也合并 |
| 多轮遗漏 | 错失补一次不逐轮枚举；日志触发时间取实际触发时间 | 严重迟到时只提交一次准备，不枚举遗漏轮次；后续仍受阻塞等条件限制 | 整段忽略或最多补一个，补执行原计划时间取旧进度；不保证补齐全部业务周期 |

DogX 的原计划时间字段、自动唯一约束以及创建与推进的同事务保障，是本项目独立选择，不是照搬两家的持久化协议。竞品的预读与时间轮也不作为本项目正常触发方案。

直接证据：XXL-JOB [到期分支](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L92-L138)、[下一次计算](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L265-L289)、[日志时间与创建](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L90-L168)、[日志表结构](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/doc/db/tables_xxl_job.sql#L83-L105)；SnailJob [过期修正与推进](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L105-L158)、[10 秒常量](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-common/snail-job-common-core/src/main/java/com/aizuda/snailjob/common/core/constant/SystemConstants.java#L227-L230)、[批次字段](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-datasource/snail-job-datasource-template/src/main/java/com/aizuda/snailjob/template/datasource/persistence/po/JobTaskBatch.java#L19-L82)、[批次表结构](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/doc/sql/snail_job_mysql.sql#L382-L405)。

## Cron 格式与时区依据 {#cron-contract-evidence}

对照固定为 XXL-JOB `b34acec`、SnailJob `cacc2b6` 与 robfig v3.0.1。两家 Java 项目采用 Quartz 系 Cron 实现，不能因为均支持秒字段就推导与 robfig 兼容。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 表达式格式 | 六个必填字段，可带第七个年份；支持 `? L W #` | 格式同为六个必填加可选年，支持 `? L W #` | 固定六字段，不接收年份、`L/W/#` 或描述符；`?` 沿用 robfig 通配语义 |
| 星期编号 | `1 = SUN` 至 `7 = SAT` | 相同 | `0 = SUN` 至 `6 = SAT`，示例优先用英文名称 |
| 运行时区 | Cron 未显式设置时区，取 JVM 默认值；无 Job 时区字段 | Cron 取 JVM 默认值，标准启动入口明确设为 `Asia/Shanghai`；无 Job 时区字段 | 使用调度服务器／容器进程默认时区，各节点一致，不设 Job 时区或表达式时区覆盖，不写死上海时区 |
| 计算一致性 | 调度通过其 CronExpression 求下一次 | 标准启动入口设置 JVM 时区，预览工具另有 UTC+8 转换 | 所有校验、预览、注册与进度计算复用同一语义和调度时区 |

SnailJob 的默认时区不仅取决于部署环境，其[标准启动入口](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-starter/src/main/java/com/aizuda/snailjob/server/SnailJobServerApplication.java#L35-L36)会设置 JVM 时区。DogX 不照搬这一硬编码；系统校时交由部署保障，时区变更按[维护流程](operations.md#scheduler-timezone)进行，不增加应用层校时、自动节点隔离或历史迁移功能。

robfig 默认解析器为五字段，需通过官方解析器选项选定六字段；`WithSeconds()` 同时启用描述符，因此不能把直接调用该选项等同于满足 DogX 的输入契约。库本身支持时区前缀和 `@every`，不表示本平台开放这些输入。`Next` 有有限搜索范围，找不到返回零值；DogX 不自行重写日期搜索算法。

直接证据：robfig [格式与时区文档](https://github.com/robfig/cron/blob/v3.0.1/doc.go)、[解析器](https://github.com/robfig/cron/blob/v3.0.1/parser.go)、[选项](https://github.com/robfig/cron/blob/v3.0.1/option.go)、[星期范围、Next 与日周匹配](https://github.com/robfig/cron/blob/v3.0.1/spec.go)；XXL-JOB [Cron 定义](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/cron/CronExpression.java#L41-L103)、[默认时区](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/cron/CronExpression.java#L390-L395)；SnailJob [Cron 定义](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-common/snail-job-common-core/src/main/java/com/aizuda/snailjob/common/core/util/CronExpression.java#L30-L153)、[默认时区](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-common/snail-job-common-core/src/main/java/com/aizuda/snailjob/common/core/util/CronExpression.java#L373-L378)、[预览转换](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-common/src/main/java/com/aizuda/snailjob/server/common/util/CronUtils.java#L24-L39)。

## Cron 异常处理依据 {#cron-error-evidence}

以下对照 XXL-JOB `b34acec`、SnailJob `cacc2b6` 的普通非驻留扫描路径，以及 robfig v3.0.1；不将 SnailJob 结论推广到常驻任务。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 求下一次时间失败 | 自动停用 Job，清空上次及下次触发时间，记录错误 | 求值抛出异常，由扫描外层记录错误；该路径不自动停用 | 当前 Job 自动触发失败，不创建 Instance、不推进进度、不自动停用 |
| 对其他 Job 的影响 | 单 Job 内捕获，继续处理其他 Job | 异常可中断当次扫描后续处理 | 按 Job 隔离，其他 Job 继续 |

robfig 的 `Next` 有有限搜索范围，找不到时返回零值。运行器把零值存入内存条目的 `Next` 后，不再回调该条目，也不自行恢复。DogX 不为此增加零值巡检及自动重注册；保存、启用校验是主要防线，配置加载继续隔离非法规则，运行错误由限频日志供管理员处理。不保证异常自动自愈，必要时在修正原因后重启相关调度进程。

直接证据：

- XXL-JOB：[下一次时间失败时停用及清空进度](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L265-L293)。
- SnailJob：[Cron 求值及异常](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-common/src/main/java/com/aizuda/snailjob/server/common/strategy/WaitStrategies.java#L188-L201)、[扫描外层捕获与批次处理](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L61-L100)。
- robfig：[有限搜索及零值](https://github.com/robfig/cron/blob/v3.0.1/spec.go#L56-L103)、[运行器处理零值](https://github.com/robfig/cron/blob/v3.0.1/cron.go#L239-L280)。

权威规则见[Cron 异常处理](behavior.md#cron-errors)，验收见[异常场景](verification.md#cron-error-checks)。

## 手动触发依据 {#manual-trigger-evidence}

对照固定为 XXL-JOB `b34acec` 与 SnailJob `cacc2b6` 的手动 Job 入口，不包含工作流。接口接受、执行记录持久化和业务执行成功是三个不同事件。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认选择 |
| --- | --- | --- | --- |
| 停用与定时进度 | 停用 Job 可手动触发，不修改主表下一次触发时间 | 停用 Job 可手动触发，但组须启用；仅当次 DTO 将触发时间设为当前时间 | 停用 Job 可手动执行，不自动启用，也不修改 `next_trigger_at` |
| 阻塞策略 | 派发后由执行器按本进程 Job 线程判断 | 手动入口直接调用 Terminal 处理器，绕过自动准备阶段的阻塞判断 | 沿用同 Job 集群级阻塞策略，不提供强制绕过 |
| 当次覆盖 | 支持业务参数及执行器地址覆盖 | 非空白 `tmpArgsStr` 覆盖当次业务参数 | 只允许覆盖当次业务参数，固定进本轮配置，不修改 Job 或临时覆盖其他执行配置 |
| 成功响应 | 提交本地异步调度后返回，不保证执行记录已落库 | 同步插入批次后返回 `true`，不等待业务执行 | Instance 与本轮配置提交成功后返回 Instance ID，不表示业务执行成功 |
| 请求重复 | 手动入口无请求身份去重 | 手动入口无请求身份去重；工作流节点防重不是手动请求防重 | `request_id` 对成功创建去重，保证范围为对应 Instance 记录保留期间 |

SnailJob 的手动批次之后仍可能被自动扫描处理，入口绕过阻塞不等于其整个生命周期不受自动逻辑影响。无在线执行器时，它可以先保存 `CANCEL / NOT_CLIENT` 批次并返回 `true`；注册时间轮异常也可能被记录后吞掉。因此不能把 `true` 解释成已成功派发或执行。

DogX 以 Instance 保存请求身份并建立唯一约束，不增加 Redis 锁、请求队列或独立请求记录表。同一请求号重发返回原 Instance，改参数则冲突；原 Instance 已失败或取消也不重开。阻塞拒绝不永久保存，拒绝响应丢失后再次请求可按当时条件创建；收到明确拒绝则结束本次操作，不在后台补跑。该低频创建接口的同步提交不改变 Task 结果异步回报方案。

直接证据：

- XXL-JOB：[手动入口](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L425-L449)、[异步调度提交](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobTriggerPoolHelper.java#L100-L153)、[当次参数与地址](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L63-L86)、[执行器阻塞判断](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-core/src/main/java/com/xxl/job/core/openapi/executor/impl/ExecutorBizImpl.java#L131-L150)。
- SnailJob：[手动入口与参数](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L72-L98)、[直接生成批次](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/prepare/job/TerminalJobPrepareHandler.java#L37-L43)、[自动准备处理未完成批次](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/JobTaskPrepareActor.java#L52-L102)、[批次插入与无节点分支](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/generator/batch/JobTaskBatchGenerator.java#L56-L128)、[时间轮异常处理](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/timer/JobTimerWheel.java#L57-L80)。

权威规则见[手动触发](behavior.md#manual-trigger)，提交与去重边界见[手动触发记账](execution.md#manual-trigger-accounting)，验收见[手动触发场景](verification.md#manual-trigger-checks)。

## 触发容量与过载依据 {#trigger-capacity-evidence}

XXL-JOB `b34acec` 的快慢触发池分别使用容量为 2,000、5,000 的队列，拒绝处理仅记录错误日志；扫描仍推进触发进度，而执行日志在后台触发任务中创建，因此队满拒绝可能丢失该次触发。池内工作包含数据库操作和执行器 RPC，快慢分类按触发耗时统计，不能直接作为 DogX 创建 Instance 短事务的并发配置。

SnailJob `cacc2b6` 按桶分页读取 Job，但分桶、分页不等于整条触发链路都有积压上限：`JobTimerWheel` 使用固定 32 线程和未指定容量的 `LinkedBlockingQueue`。普通 `JobTimerTask` 回调把工作交给下游 Actor，这个 32 不能等同于 Instance 创建事务并发。DogX 不照搬两家的线程数或队列容量。

DogX 的 Cron 回调、低频恢复及注册后到期检查共用按 Job ID 去重的有界队列和固定工作协程；队满只拒绝本次检查提示、不推进进度，后续由既有触发来源与恢复检查重新发现，处理时仍服从 5 秒错失规则。手动触发共享进程级创建并发额度，同步返回结果，不进入自动队列；容量不足明确拒绝，不后台补跑。

DogX 的已定实现初值为总容量 1024、创建并发 32、单次处理 3 秒，按实测校准；自动工作协程数直接取创建并发配置。SnailJob 官方[性能报告](https://snailjob.opensnail.com/docs/introduce/load_report.html)针对 1.7.2：单服务节点 4 核 8 GB、独立数据库 8 核 16 GB、16 个执行器，报告承载 3 万个每 60 秒执行一次的任务，折算平均约 500 次触发／秒。报告配置的准备 Dispatcher 最小线程数为 128，执行、调用及结果 Dispatcher 为 160，数据库连接池上限为 256；不是仅以时间轮 32 线程取得的成绩，也不能据此推导 3 万规则同秒均在 5 秒内完成触发。该报告作为容量校准参考，不将上述数字转为 DogX 已验证成绩或自动替换现有验收基线。

直接证据：

- XXL-JOB：[快慢池容量与拒绝处理](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobTriggerPoolHelper.java#L29-L66)、[耗时分类与异步提交](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobTriggerPoolHelper.java#L100-L151)、[扫描推进与提交](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L102-L167)、[日志创建与执行器调用](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L147-L246)。
- SnailJob：[分桶与分页查询](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L161-L179)、[计时执行池及队列](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/timer/JobTimerWheel.java#L26-L35)、[回调向 Actor 提交](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/timer/JobTimerTask.java#L26-L39)。

权威规则见[触发容量与过载](execution.md#trigger-capacity)，验收见[触发容量场景](verification.md#trigger-capacity-checks)。

## 触发持久化比较 {#trigger-persistence-evidence}

比较 XXL-JOB `b34acec`（3.5.0-SNAPSHOT）和 SnailJob `cacc2b6` 的普通自动任务路径，不把工作流节点防重等同于周期触发身份。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认方案 |
| --- | --- | --- | --- |
| 进度与运行记录 | 扫描事务更新下次时间，实际触发在线程池另行保存日志 | 扫描先更新下次时间，后续 Actor 路径创建批次 | 同 Job 事务创建 Instance 并推进进度；锁后另发语句查重及阻塞 |
| 自动计划身份 | 所核实执行日志表没有 Job 与原计划时间的唯一约束 | 所核实普通批次表没有同类唯一约束 | 正常／错失共用部分唯一索引，不覆盖历史 Instance |
| 活跃批次查询 | 运行模型与阻塞实现不同，不按 Instance 表硬对齐 | 批次表有 `(job_id, task_batch_status)` 普通索引 | 只索引未结束 Instance，按 Job 做 EXISTS |

XXL-JOB 在[扫描事务](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L79-L168)中取得全局调度锁、投入触发池或时间环并推进下次时间；[触发池](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobTriggerPoolHelper.java#L115-L123)另行运行[日志保存](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L147-L152)。[日志 DDL](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/doc/db/tables_xxl_job.sql#L83-L105)没有 DogX 所选的计划身份唯一键。

SnailJob 的[扫描交接](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L85-L102)先更新 Job 进度再发送 Actor 消息，后续由[批次生成器](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/generator/batch/JobTaskBatchGenerator.java#L56-L83)插入批次；不是与扫描进度同一事务。[批次表及索引](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/doc/sql/snail_job_postgre.sql#L596-L619)没有同类自动计划唯一键；生成器中的 DAG 重复处理不能作为普通 Cron 防重的证据。

PostgreSQL 的 [READ COMMITTED 说明](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED)区分语句快照、等锁后更新行版本，以及 `ON CONFLICT DO NOTHING` 可能受当前快照不可见的并发行影响；[部分索引](https://www.postgresql.org/docs/current/indexes-partial.html)要求规划时能够证明查询蕴含谓词；[INSERT 冲突语法](https://www.postgresql.org/docs/current/sql-insert.html)支持列与谓词的唯一索引推断。DogX 因此先锁 Job，再用下一条语句查 Instance，冲突后读取原记录也另发 SELECT。

以上支持[事务与索引协议](data-contracts.md#trigger-transaction-proposal)的正确性和成本分析，不证明其吞吐已超过竞品。五个索引和逐 Job 短事务仍需实现后的执行计划及并发验证。

## 触发恢复扫描比较 {#trigger-recovery-scan-evidence}

XXL-JOB `b34acec` 与 SnailJob `cacc2b6` 的下列扫描负责常态预取与触发；DogX 的分钟扫描是长期 Cron 触发之外的遗漏恢复。读取量可以比较，但不能把两家的扫描周期或批量直接当成 DogX 的补偿配额。

| 设计点 | XXL-JOB | SnailJob | DogX 已确认方案 |
| --- | --- | --- | --- |
| 读取范围 | 启用且未来 5 秒内到期的 Job | 本节点桶内启用且未来 10 秒内到期的 Job | 启用且已到期的 Job，只读 ID 与进度 |
| 单次读取 | 按 ID 升序 LIMIT；上限为快慢池最大线程数之和乘 10，当前配置为 5000 | 按 ID 升序，每页默认 1000 | 按时间及 ID 排序，单次 LIMIT 初值 1024，只读 ID 和时间 |
| 一次扫描的范围 | 每轮一次候选查询，不循环翻完全部页 | 从 ID 0 起，末 ID 加 1 续页，直到空页；此路径未设整轮条数或 deadline | 只查询一批，不分页；单次查询超时初值 5 秒 |
| 下一次位置 | 下一轮重新执行候选查询 | 每次桶扫描重新从 ID 0 开始 | 每分钟按当前数据库重新查最早到期项，不保存恢复游标 |

XXL-JOB 的扫描上限来自[线程数计算](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L66-L85)及[当前配置的 300 和 200](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/application.properties#L65-L69)，[查询 SQL](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/mapper/business/XxlJobInfoMapper.xml#L222-L229)按 ID 排序。`schedule.batchsize=100` 是更新子批大小，不是候选查询页大小。

SnailJob 的[分页初值](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-common/src/main/java/com/aizuda/snailjob/server/common/config/SystemProperties.java#L37-L40)为 1000；[扫描调用](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L79-L80)从 ID 0 起，[分页工具](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-common/src/main/java/com/aizuda/snailjob/server/common/util/PartitionTaskUtils.java#L30-L78)循环到空页，[候选查询](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L166-L179)按桶、启用状态、未来 10 秒及起始 ID 过滤。

DogX 的单批限量限制恢复查询一次占用的资源，不限制正常 Cron 吞吐；队满结束，下分钟重查，接受持续失败或反复队满导致其他遗漏 Job 长期延迟，不保证严格公平或一分钟恢复完成。查询 5 秒不等于整轮扫描预算，也不替代触发处理 3 秒或错失容忍 5 秒。候选不等于执行权，实际处理继续遵守 Job 短事务。完整规则见[到期 Job 恢复查询](data-contracts.md#trigger-recovery-scan-proposal)；这一简化取舍基于 DogX 的兜底职责和当前规模，不由两家常态扫描参数直接推导。

## 自动跳过触发留痕比较 {#trigger-skip-record-evidence}

以下核对 XXL-JOB `b34acec` 与 SnailJob `cacc2b6` 的普通定时任务。两者阻塞检查发生的位置与 DogX 不同；“没有执行业务”不等于“没有数据库记录”，也不能把技术队满等同于按策略丢弃。

| 场景 | XXL-JOB | SnailJob | DogX 已定留痕方案 |
| --- | --- | --- | --- |
| 错失忽略 | `DO_NOTHING` 仅服务警告，不创建该次调度日志 | 没有相同的忽略分支；严重迟到仍推进一次准备，不逐个补历史时刻 | 不创建 Instance，仅服务日志记错失忽略 |
| 同 Job 阻塞丢弃 | 调度日志先建，执行器拒收后保存调度失败及备注 | 新建批次，插入时即为 `CANCEL / JOB_DISCARD` | 不创建 Instance，仅服务日志记阻塞丢弃 |
| 管理端查看丢弃原因 | 阻塞拒收可在调度日志及备注中查询；错失忽略无该条记录 | 取消批次可查询，带操作原因 | 不提供跳过明细，排查依赖日志系统 |

XXL-JOB 的 [MisfireDoNothing](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/misfire/strategy/MisfireDoNothing.java#L10-L13) 仅输出警告，[扫描线程](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/thread/JobScheduleHelper.java#L92-L100) 随后推进时间。阻塞路径不同：[JobTrigger](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L147-L195) 先创建调度日志，再调用执行器；[DISCARD_LATER](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-core/src/main/java/com/xxl/job/core/openapi/executor/impl/ExecutorBizImpl.java#L133-L159) 返回失败，[调度端保存结果](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/scheduler/trigger/JobTrigger.java#L237-L278)，[页面显示调度结果及备注](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/templates/business/log.list.ftl#L306-L327)。这种失败还可能参与配置的失败重试，不是独立的无重试跳过状态。

SnailJob 的[扫描](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/ScanJobTaskActor.java#L105-L158)在严重迟到时调整本次时间基准，仍加入准备列表，没有 DogX 式固定错失忽略分支。[DiscardBlockStrategy](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/block/job/DiscardBlockStrategy.java#L23-L28) 给新批次设置取消及丢弃原因，再由[批次生成器](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/generator/batch/JobTaskBatchGenerator.java#L56-L105)直接插入取消记录，不创建可派发的工作。[管理查询](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/controller/JobBatchController.java#L24-L40)及[查询实现](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/service/impl/JobWebBatchServiceImpl.java#L40-L64)支持这些批次；已关闭 Job 被扫描排除、已有批次之后因关闭取消，是另外两种场景。

DogX 在创建 Instance 前按集群未结束轮次判断阻塞，已选择跳过时不创建执行 Instance。已通过的只留服务日志方案不是照搬两家的共同机制，而是减少额外数据库写入、放弃管理端跳过明细的取舍；日志并非事务证据，不能保证完整审计。完整规则见[跳过留痕](data-contracts.md#trigger-skip-record-proposal)。

## Job 删除比较 {#job-deletion-evidence}

以下核对 XXL-JOB `b34acec41d811a91d8efa0b67de20d283100991b` 与 SnailJob `cacc2b698c1880552d4b0c3582b44713dc350566` 的普通 Job 删除路径。删除定义、停止业务、清理历史是不同操作，不能因页面上点击“删除”就推导全部执行已经停止。

| 边界 | XXL-JOB | SnailJob 仓库默认配置 | DogX 已定方案 |
| --- | --- | --- | --- |
| 必须先停用 | 不要求 | 要求关闭 | 要求停用 |
| 必须没有未结束轮次 | 不检查执行状态 | 不检查等待／运行批次 | 无待开始或运行中 Instance 才允许 |
| 删除数据 | 物理删除 Job、该 Job 的全部调度日志及 GLUE 历史 | 物理删除 Job 及 JobSummary；不连带删除批次、子任务、日志 | Job 逻辑删除，保留删除时定义和 Instance／Task |
| 主动停止执行器业务 | 删除路径不调用停止 | 删除路径不调用停止 | 不将删除与停止合并；停止是独立操作 |
| 删除后历史可见性 | 被本次删除的调度日志不再可查，执行器日志文件不在删除范围内 | 批次列表内连接 Job，删除后不显示；已知 ID 的子任务、日志查询仍可读取保留数据 | 历史依赖自身快照，必要时只读关联已删除 Job，不受普通软删除过滤误伤 |

XXL-JOB 的[删除服务](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L324-L346)检查组权限后顺序删除三类数据，没有停用、活跃执行检查或 kill。对应 [Job SQL](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/mapper/business/XxlJobInfoMapper.xml#L204-L208)、[调度日志 SQL](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/mapper/business/XxlJobLogMapper.xml#L162-L165) 和 [GLUE SQL](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/mapper/business/XxlJobLogGlueMapper.xml#L66-L69)都是实际 DELETE。删除不会清空内存触发队列；后续触发重新读取 Job 时发现不存在便返回，已读取配置的在途工作不因此撤销。不能把这条管理删除链路描述为执行排空或三次删除已获同事务保证。

SnailJob 的[删除服务](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L108-L143)检查全部目标关闭，再删除对应汇总及 Job，没有检查批次或调用执行器停止。[Job.deleted](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-datasource/snail-job-datasource-template/src/main/java/com/aizuda/snailjob/template/datasource/persistence/po/Job.java#L151-L154)的注释虽写逻辑删除，但没有 `@TableLogic`，[默认配置](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-starter/src/main/resources/application.yml#L61-L68)也没有指定逻辑删除字段。所用 MyBatis-Plus [字段识别](https://github.com/baomidou/mybatis-plus/blob/v3.5.17/mybatis-plus-core/src/main/java/com/baomidou/mybatisplus/core/metadata/TableFieldInfo.java#L396-L419)及 [Delete 分支](https://github.com/baomidou/mybatis-plus/blob/v3.5.17/mybatis-plus-core/src/main/java/com/baomidou/mybatisplus/core/injector/methods/Delete.java#L45-L62)在此走物理删除；不能单凭字段名断言软删除，额外修改 ORM 配置的部署须另核。

SnailJob 的[批次列表 SQL](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-datasource/snail-job-datasource-template/src/main/resources/template/mapper/JobTaskBatchMapper.xml#L5-L17)内连接 Job，所以“数据保留”不等于“列表仍可见”。[子任务查询](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/service/impl/JobTaskServiceImpl.java#L33-L47)和[日志查询](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-datasource/snail-job-datasource-template/src/main/java/com/aizuda/snailjob/template/datasource/access/log/JobLogMessageAccess.java#L53-L71)不依赖该内连接。[服务端超时检查](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/timer/JobTimeoutCheckTask.java#L54-L70)遇到 Job 不存在会返回，也不能将其当成删除后的自动停止保证。

DogX 的[已定删除协议](data-contracts.md#job-deletion-proposal)选择逻辑删除，并保留更严格的删除条件。Job 定义量较小，选择依据是保留原定义的排查价值，不是宣称物理或逻辑删除具有明显性能优势。常规查询及调度排除已删除记录，历史必要时显式只读关联；删除时最后一版定义不等于逐轮配置历史，执行快照仍然是本轮依据。无回收站或恢复入口，有未结束 Instance 时须先等结束或独立停止；平台终态不保证业务已物理退出。

## Job 组引用与离线配置比较 {#job-group-reference-evidence}

以下仍核对 XXL-JOB `b34acec` 与 SnailJob `cacc2b6` 的普通任务管理路径。页面下拉选项、管理权限和组引用完整性不是同一层校验；保存不要求节点在线，也不表示真正执行时可以没有执行器。

| 场景 | XXL-JOB | SnailJob | DogX 已定规则 |
| --- | --- | --- | --- |
| 常规新增／编辑的组引用 | 新增检查组存在；编辑只有传入新的正数组 ID 时检查，否则保留原组 | 常规保存服务不检查组存在；编辑不允许改组；导入另有存在性校验 | 新增、编辑保存都校验组定义 |
| 启用的组引用 | 不重新查组表 | 不查组表 | 启用时校验 |
| 手动触发的组引用 | 受理查 Job 及组权限，不显式查组存在 | 按 Job 的实际组校验存在且开启 | 新建轮次校验；已有同请求成功记录不重判 |
| 保存／启用要求在线节点 | 不要求 | 不要求 | 不要求，允许先配置后部署 |
| 保存／启用探测处理器注册 | 不探测，实际派发到执行器时本地查找 | 不探测，实际派发到客户端时查缓存 | 不探测，只做本地输入校验 |

XXL-JOB 的[新增校验](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L79-L114)读取组，并仅检查 BEAN handler 非空；[编辑](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L192-L198)按输入条件检查组，[启用](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L348-L393)不重查组。[手动受理](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L424-L448)异步提交触发，handler 的存在性直到[执行器实际接收](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-core/src/main/java/com/xxl/job/core/openapi/executor/impl/ExecutorBizImpl.java#L67-L85)才检查。

SnailJob 的[保存服务](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L147-L223)没有组查询，编辑明确置空更新对象的组字段以禁止改组；[Web 前置校验](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/service/impl/JobWebServiceImpl.java#L195-L213)和 [OpenAPI 前置校验](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-openapi/src/main/java/com/aizuda/snailjob/server/openapi/service/impl/JobApiServiceImpl.java#L24-L41)均为空。[启停](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L227-L248)不查组，[手动触发](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L71-L97)则要求实际引用组存在且开启。[客户端派发入口](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-job-core/src/main/java/com/aizuda/snailjob/client/job/core/client/JobEndPoint.java#L43-L65)才检查处理器缓存；不能将请求头认证或导入接口的组检查替代常规保存的行为。

两家均保护删除组时的 Job 引用：[XXL-JOB](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/controller/JobGroupController.java#L214-L224)不区分 Job 启停；[SnailJob](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/service/impl/GroupConfigServiceImpl.java#L358-L365)按命名空间及组名查任意 Job 行，不按启停或 `deleted` 过滤。这个检查不自动证明并发引用创建和删除已经原子协调，也不能直接套用于 DogX 的逻辑删除及本轮配置快照。DogX 已通过[管理入口校验](data-contracts.md#job-group-reference-proposal)及[引用并发保护](data-contracts.md#group-reference-concurrency-proposal)，完整组生命周期仍须设计。

## 超时默认值与作用范围比较 {#timeout-default-evidence}

本节核对 XXL-JOB `b34acec` 与 SnailJob `cacc2b6` 的普通任务路径。页面初值、数据库默认和执行时的计时范围分别比较；两家的数字都不能证明 DogX 的整轮默认值应该是多少。

| 项目 | XXL-JOB | SnailJob |
| --- | --- | --- |
| 新增管理表单 | 超时输入为空或非法归为 0 秒 | 预填 60 秒，页面最小输入为 1 |
| 数据库默认 | `executor_timeout DEFAULT 0` | `executor_timeout DEFAULT 0`；普通保存服务不统一补成 60 秒 |
| 运行时 0 的含义 | 不进入限时等待分支 | 仍注册超时检查，不能作为无限时长 |
| 计时范围 | 一次 `handler.execute()`；分片请求、重试各自计时 | 服务端按批次检查；客户端各次派发也注册定时器，停止对象是本客户端的批次线程池 |
| 自动重试 | 新触发重新获得执行超时额度 | 不重置服务端原批次超时检查；重试等待也占用批次预算 |

XXL-JOB 的[数据库列默认](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/doc/db/tables_xxl_job.sql#L52)与[新增表单归零逻辑](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/templates/business/job.list.ftl#L1086-L1091)一致。[执行线程](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-core/src/main/java/com/xxl/job/core/thread/JobThread.java#L91-L161)只有超时大于 0 才使用有期限的等待，不包含此前调度及执行器队列等待；广播分片分别接收带超时配置的请求，失败重试重新触发，不提供 DogX 等价的整轮 Prepare／Post 预算。

SnailJob 的[页面构建产物](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-ui/src/main/resources/admin/assets/index-BczVyegZ.js#L1)包含 `executorTimeout:60`，但[DDL](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/doc/sql/snail_job_mysql.sql#L309)默认是 0；[请求字段](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-common/snail-job-common-model/src/main/java/com/aizuda/snailjob/model/request/base/JobRequest.java#L103-L108)没有初始值，[保存路径](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L198-L222)也不补成 60 秒。不能把页面初值写成后端统一缺省行为。

SnailJob 在[执行准备事务完成后](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/dispatch/JobExecutorActor.java#L201-L206)注册 `executorTimeout * 1000 + 500ms` 的批次检查；[检查任务](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/timer/JobTimeoutCheckTask.java#L39-L70)按批次停止，而非仅停止某个 Task。扫描兜底以生成初始子任务前保存的 `executionAt` 为起点，两条路径的起算点并不完全相同。[自动重试](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-dispatcher/snail-job-server-job-task/src/main/java/com/aizuda/snailjob/server/job/task/support/callback/AbstractClientCallbackHandler.java#L47-L74)不重建批次计时器，Map／Reduce 等同批次阶段也沿用原批次检查，不应概括为每个子任务各享完整时长。

客户端在[每次派发提交前](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-client/snail-job-client-job-core/src/main/java/com/aizuda/snailjob/client/job/core/executor/AbstractJobExecutor.java#L43-L55)注册定时器，但停止对象仍是本机该批次的共享线程池。服务端和客户端均没有以 0 跳过计时的分支；这些超时停止属于尽力中断，不保证业务已经物理退出，也不等价于 DogX 的统一持久化截止与授权协议。

DogX 的整轮范围及[900 秒新增表单初值](data-contracts.md#timeout-default-proposal)已通过。API 仍要求显式传值，数据库不设默认，不以这两家的默认值作为性能或适配性证明。

## Job 管理入口与事务比较 {#job-management-evidence}

本节对应已通过的[管理入口契约](data-contracts.md#job-management-proposal)，仍使用 XXL-JOB `b34acec` 与 SnailJob `cacc2b6`。比较接口语义，不以两家的 HTTP 动词取代 DogX 现有 API 约定。

| 设计点 | XXL-JOB | SnailJob | DogX 已定规则 |
| --- | --- | --- | --- |
| 配置保存 | 完整配置表单式提交，按固定字段覆盖；组输入另有保留旧值特例 | 页面提交整份配置；后端选择性更新及派生重算，不是严格全量替换 | 完整可编辑配置提交；说明／参数省略、null、空串均清空，其他必需项缺失拒绝；零值明确生效 |
| 启停入口 | 独立 start／stop，编辑不从请求复制启停状态 | 独立 status 入口，但编辑也可以携带并更新 jobStatus | 独立 status 操作，编辑不写启停或进度 |
| 时间预览 | 独立预览，最多 5 个时间点，不先保存 Job | 独立 Cron 接口，返回 5 个时间点 | 独立预览，最多 5 个，统一服务端解析和时区，不查库 |
| 手动成功应答 | 提交异步触发后返回成功，不返回执行记录 ID | 调用创建批次后返回 true，不返回批次 ID | 沿用已定 Instance 提交成功后返回 ID及同请求号防重，不等待执行完成 |
| 管理并发 | 先读取再按 ID 更新，无管理编辑版本条件；全字段更新可能带回旧状态／进度 | 先读后按 ID 更新，启停比较不是条件写入 CAS | 编辑用停用条件 UPDATE；启用按 Job 主键短事务重读计算，不统一给普通 CRUD 加显式锁 |

XXL-JOB 的[编辑实现](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L272-L315)覆盖一组明确字段，[启停](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/controller/JobInfoController.java#L133-L158)与[预览](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/controller/JobInfoController.java#L171-L207)是独立动作。[更新 SQL](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/mapper/business/XxlJobInfoMapper.xml#L170-L202)只以 ID 定位，包含状态和进度字段；不能把独立启停入口等同于编辑并发保护。[手动触发](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L424-L448)返回的是提交触发后的成功，不是 DogX 的持久化 Instance 应答。

SnailJob 的[管理 Controller](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/controller/JobController.java#L70-L117)分别提供新增、编辑、状态、Cron 预览及手动触发。[编辑实现](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L147-L194)允许请求携带 jobStatus；选择性更新也不意味着所有遗漏字段都保持原值。[独立状态更新](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L227-L248)读后比较，再按 ID 写入。[预览](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/service/impl/JobWebServiceImpl.java#L89-L92)返回 5 个时间点，[手动触发](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L79-L97)返回 true，不返回 batchId。

这两条常规管理服务链路没有把所有校验、读取及写入放进同一个显式服务事务；单条 SQL 的原子性和调度内部锁不能替代管理并发协议。DogX 的条件更新、必要短事务及手动防重是自身契约。第一版允许停用 Job 的并发完整编辑按提交顺序覆盖，不承诺编辑版本冲突检测。

## 执行器组删除与并发引用比较 {#group-deletion-concurrency-evidence}

以下核对 XXL-JOB `b34acec` 与 SnailJob `cacc2b6` 的组删除管理路径，对应 DogX [已通过的引用协议](data-contracts.md#group-reference-concurrency-proposal)。普通存在性校验、单条 DELETE 的原子性与跨请求引用保护不是同一保证。

| 设计点 | XXL-JOB | SnailJob | DogX 已定方案 |
| --- | --- | --- | --- |
| 当前 Job 引用 | 有 Job 即拒绝，不区分启停 | 有 Job 即拒绝，不按启停或 deleted 过滤 | 所有未删除 Job 阻挡，停用也不例外 |
| 其他删除检查 | 不允许删除最后一组；不检查历史日志或在线执行器 | 还检查工作流、重试场景、组权限和节点记录；最终只删除停用组 | 另保护原组快照的未结束 Instance 和未收敛 Task，不要求保留全部已完成历史 |
| 与新增／改组的共同协调 | 新增／改组先查组、删组先查引用，没有共同管理锁协议 | 常规新增没有共同锁；编辑不允许改组，删组检查后单独删除 | 新增／完整编辑与删组共用目标组主键行锁及短事务 |
| 运行主路径成本 | 上述管理检查不等于运行期引用保证 | 同左，不能据管理删除路径概括全部调度机制 | 不因本项给自动触发、Task 派发或回报加组锁／逐次查询 |

XXL-JOB 的[完整删组路径](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/controller/JobGroupController.java#L203-L237)先检查全部 Job 引用及最后一组限制，再删除组并清除该 appname 的注册记录，不以在线执行器作为阻止条件，也不查询或删除历史日志。[引用数量 SQL](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/mapper/business/XxlJobInfoMapper.xml#L92-L112)与[组删除 SQL](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/mapper/business/XxlJobGroupMapper.xml#L54-L63)是分开的操作，新增／改组的普通组查询也没有与其共用行锁。因此不能把该顺序检查称为新增引用与删组的原子保护。

SnailJob 的[完整删组路径](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/service/impl/GroupConfigServiceImpl.java#L357-L395)检查 Job、Workflow、RetrySceneConfig、SystemUserPermission、ServerNode，最后按 ID 和停用状态删除。节点查询没有过期时间、心跳或节点类型条件，应称为“节点记录阻挡”，而非实时确认客户端在线。该方法不检查 Job 批次、Task 或执行日志，也不主动停止业务。[普通新增](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L198-L223)没有与删组共用锁，[普通编辑](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-service/src/main/java/com/aizuda/snailjob/server/service/service/impl/AbstractJobService.java#L186-L194)明确保留旧组，不支持改组。

这两条检出管理链路没有把引用检查与引用建立统一到一个共享事务锁协议中；这不是对所有版本或所有调度路径的可靠性结论。DogX 的旧 Instance 使用组快照，父终态与子 Task 收敛又分开，因此不能只复制“查当前 Job 后删组”。共同组行锁、锁序及旧轮次保护属于 DogX 已通过的设计选择。

DogX 第一版进一步确定[组不设启停、只逻辑删除](data-contracts.md#executor-group-lifecycle)：未删除组才可被新引用，删除保留定义及执行历史。引用解除与运行／恢复依赖检查不能因采用软删除而省略；删除后的凭据和连接缓存失效须随组模块落实，不照搬竞品的物理删除或节点记录门槛。

## Job 输入与管理查询比较 {#job-input-query-evidence}

本节对应已通过的[输入查询与错误契约](data-contracts.md#job-input-query-proposal)，核对版本仍为 XXL-JOB `b34acec` 与 SnailJob `cacc2b6`。普通分页、下拉选项、页面限制和后端校验分别看待；两家的包含查询不等于统一忽略大小写，实际行为还受数据库及排序规则影响。

| 设计点 | XXL-JOB | SnailJob | DogX 已定规则 |
| --- | --- | --- | --- |
| 名称筛选及排序 | 名称包含匹配，ID 倒序 | 名称包含匹配，ID 倒序 | 不区分大小写的字面包含，转义通配符；ID 倒序 |
| 普通分页参数载荷 | 查询并返回 `executor_param`，SQL 还包含 `glue_source` | 普通分页查询完整 Job，响应包含 `argsStr`；另外的下拉接口有轻量投影 | 列表不读取业务参数，详情才读取 |
| 空串更新参数 | 普通更新直接写参数值 | 实体 `argsStr` 的 NOT_EMPTY 策略忽略空串 | 普通保存的参数省略／null／空串均归空，trim 后覆盖；0／false 不被当遗漏，手动覆盖另行校验 |
| 输入长度来源 | 页面名称 50、Handler 100、Cron 128、参数 2048；不能等同于所有 API 的后端限制 | PostgreSQL 名称 VARCHAR(64)、执行器信息 VARCHAR(255)、参数 TEXT；不能等同于统一 API 长度校验 | 短字符串使用 VARCHAR(n)，API／域校验与容量一致；参数 TEXT 复用报文上限，不增加业务数值范围 CHECK |

XXL-JOB 的[分页投影与筛选 SQL](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/mapper/business/XxlJobInfoMapper.xml#L41-L112)使用完整基础列、名称／Handler／负责人包含匹配及 ID 倒序，另执行总数查询；[返回路径](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/java/com/xxl/job/admin/business/service/impl/XxlJobServiceImpl.java#L54-L65)没有删除业务参数。[新增页面字段](https://github.com/xuxueli/xxl-job/blob/b34acec41d811a91d8efa0b67de20d283100991b/xxl-job-admin/src/main/resources/templates/business/job.list.ftl#L123-L180)给出了上述 maxlength；这是页面约束，不能说后端已经统一执行相同上限。

SnailJob 的[普通分页](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/service/impl/JobWebServiceImpl.java#L60-L88)使用名称包含匹配、状态等值和 ID 倒序，没有 select 轻量投影；[后面的名称下拉](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-server-interface/snail-job-server-web/src/main/java/com/aizuda/snailjob/server/web/service/impl/JobWebServiceImpl.java#L95-L124)是另一条查询。[参数字段更新策略](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/snail-job-datasource/snail-job-datasource-template/src/main/java/com/aizuda/snailjob/template/datasource/persistence/po/Job.java#L46-L53)与 DogX 显式清空的契约不同。[PostgreSQL 表定义](https://github.com/aizuda/snail-job/blob/cacc2b698c1880552d4b0c3582b44713dc350566/doc/sql/snail_job_postgre.sql#L443-L470)规定存储容量，不据此宣称所有 Web 校验分组都会执行。

DogX 按[字符串字段规范](../../development/database-migrations.md#column-types)重设计短字段：名称及快照 VARCHAR(128)、说明 VARCHAR(500)、处理器 VARCHAR(128)、Cron VARCHAR(128)、结束原因码 VARCHAR(64)。名称／说明沿用现有字典和备注容量，Cron 参考 XXL-JOB 页面 128；处理器及原因码是本项目的容量选择，不是两家的统一标准。Cron 128 也不是语法最大长度，超过容量的合法长枚举仍会被拒绝；原统一 TEXT 与 Cron 应用上限 256 的草案由此替代，详见[类型重设计](data-contracts.md#string-field-types)。

业务参数和自由诊断摘要保留 TEXT，配置快照保留 JSONB。当前 go-zero v1.10.3 [RestConf](https://github.com/zeromicro/go-zero/blob/v1.10.3/rest/config.go#L48-L56) 默认 MaxBytes 为 1048576；gRPC v1.82.1 [服务端默认接收限制](https://github.com/grpc/grpc-go/blob/v1.82.1/server.go#L58-L63)为 4 MiB。实际限制还受部署和调用链配置影响，不能由 TEXT 类型推断无限输入。

## 关键决策索引

| 决策 | 当前结论 | 权威正文 |
| --- | --- | --- |
| 调度底座 | 自研核心，robfig 承担 Cron 解析与内存触发，不采用 Asynq | [总体架构](architecture.md) |
| Job 配置同步与重载 | 主库权威，提交后更新集群级 etcd 提示，每分钟核对；接任、通知、周期及 Watch 重建共用任期内串行入口和一个待核对信号。一次读取完整轻量投影，只调整变化的规则，新增／替换注册后有界检查原计划；查询失败保留旧规则、单条坏 Cron 隔离、旧任期加载不跨任期应用，revision 不作为 Job 版本 | [同步规则](behavior.md#trigger-sync)、[加载流程](execution.md#job-config-reload)、[比较依据](references.md#job-config-sync-evidence)、[验收场景](verification.md#job-config-sync-checks) |
| Cron 格式与时区 | 固定六字段与 robfig 语义，不兼容 Quartz 全语法；按调度服务器／容器进程默认时区统一计算，不新增 Job 时区字段或页面选择；保存试算不重置进度 | [规则契约](behavior.md#cron-contract)、[部署约定](operations.md#scheduler-timezone)、[验收场景](verification.md#cron-contract-checks) |
| 自动触发身份与区间处理 | 自动身份为 Job 与原计划时间；一次到期处理最多创建一轮，包括 5 秒内积压，执行／忽略／丢弃均推进到判定时间之后；旧进度超 5 秒且忽略时整段跳过 | [身份规则](behavior.md#trigger-identity)、[区间规则](behavior.md#trigger-advancement)、[验收场景](verification.md#trigger-identity-checks) |
| 手动触发 | 停用可手动，沿用集群阻塞，不修改 Cron 进度；只覆盖当次业务参数并冻结。Instance 与配置提交后返回 ID，成功创建按 `request_id` 在记录保留期内去重，参数冲突及终态不重开；拒绝不永久留痕、不后台补跑 | [手动规则](behavior.md#manual-trigger)、[提交与去重](execution.md#manual-trigger-accounting)、[验收场景](verification.md#manual-trigger-checks) |
| 首轮计划初始化 | 新建即启用时随创建保存首轮 `next_trigger_at`；新建停用时在首次启用事务中保存，提交后再通知注册。不得等待首次回调初始化，遗漏仍按错失策略处理 | [初始化规则](behavior.md#first-trigger-initialization)、[验收场景](verification.md#first-trigger-checks) |
| 编辑与重新启用 | 所有 Job 配置仅停用时可编辑，后端协调校验与保存；编辑试算不重设持久化进度，停用切换启用时按当前规则原子保存新进度、不补旧轮次。重复启用、重启、切主及重载不重置；不取消已有 Instance，也不要求全部执行结束才可编辑 | [编辑启用规则](behavior.md#job-edit-enable)、[验收场景](verification.md#job-edit-enable-checks) |
| 本轮执行配置 | Instance 创建时固定必要执行配置，覆盖待开始、准备、分片、收尾、重试和恢复；Job 编辑只影响新建轮次。共享配置每轮一份，超时仍从首次有效开始计时，不冻结节点、凭据或业务代码 | [执行配置规则](behavior.md#instance-execution-config)、[验收场景](verification.md#instance-execution-config-checks) |
| 本轮配置存储 | Instance 的组标识和运行数据使用独立列，其余必要执行配置使用 `execution_config` JSONB，Go 明确结构体负责校验、序列化与兼容。Task 不复制完整配置，读取复用或批量加载，不宣称 JSONB 性能更快 | [存储分工](data-contracts.md#instance-config-storage) |
| Job 配置存储 | 平台配置普通列、TEXT 业务参数，输入仅去首尾空白，业务字段不扩展平台表；触发创建快照复用事务已读 Job，不增加查询。不机械逐列建索引或添加业务范围 CHECK | [存储分工](data-contracts.md#job-storage)、[比较依据](#job-storage-evidence) |
| 字符串字段类型修订 | 短字段使用 VARCHAR(n)：名称及快照 128、说明 500、处理器 128、Cron 128、原因码 64；参数及自由诊断摘要保留 TEXT，结构化快照 JSONB。跨层容量一致，原生数字／时间类型不变；不变更已有业务表 | [字段规范](../../development/database-migrations.md#column-types)、[重设计](data-contracts.md#string-field-types)、[验收](verification.md#string-field-type-checks) |
| 触发字段契约 | Job 新建默认停用，未启用进度为空、停用保留；Job／Instance 表名、BIGINT 身份、TIMESTAMPTZ 时间与状态编码已定。Instance 保存名称和操作者，手动 UUID 加覆盖标记比较；配置 JSONB v1 显式写入，创建时三个运行时间为空 | [字段契约](data-contracts.md#trigger-field-draft)、[配置及参数比较](data-contracts.md#execution-config-field-draft) |
| 触发事务与必要索引 | 自动／手动部分唯一索引与身份结构 CHECK；Job 锁后另发语句查重及阻塞，精确处理身份冲突。历史自动身份保留旧记录并推进当前 Job；采用五个必要索引，月分区暂不考虑 | [已定协议](data-contracts.md#trigger-transaction-proposal)、[竞品与数据库依据](#trigger-persistence-evidence) |
| Job 触发恢复查询 | 接任立即及每分钟从主库重查最早到期的一批，LIMIT 初值 1024、查询超时 5 秒；不分页或保留跨分钟游标，队满／查询失败结束，下分钟重查。接受持续异常或超载下不保证严格公平，替代原 Job 恢复轮转要求 | [已定查询](data-contracts.md#trigger-recovery-scan-proposal)、[用途比较](#trigger-recovery-scan-evidence)、[恢复验收](verification.md#trigger-recovery-checks) |
| 自动跳过留痕 | 错失忽略及阻塞丢弃在进度确认提交后仅写结构化服务日志，不增加数据库记录或查询。每次决策一条，不逐个补历史时刻；管理页无跳过明细，接受日志缺失，不保证完整审计或精确计数 | [留痕规则](data-contracts.md#trigger-skip-record-proposal)、[竞品比较](#trigger-skip-record-evidence)、[验收](verification.md#trigger-skip-record-checks) |
| Job 逻辑删除 | 停用且无未结束 Instance 才设置 `deleted_at`，保留定义和执行历史；普通读写、加载和触发排除已删除，历史可显式关联。无恢复站、无级联、不代替停止；旧创建请求不跨删除重放成功 | [删除协议](data-contracts.md#job-deletion-proposal)、[竞品比较](#job-deletion-evidence)、[验收](verification.md#job-deletion-checks) |
| Job 组引用与离线配置 | 新增、编辑保存、启用及手动新建校验组引用；不要求节点在线，不远程探测处理器。旧请求按既定查重处理，停用／删除不受组可用性阻碍；不扩展为热路径组表查询 | [组引用规则](data-contracts.md#job-group-reference-proposal)、[竞品比较](#job-group-reference-evidence)、[验收](verification.md#job-group-reference-checks) |
| 组引用与删除并发保护 | 新增／完整编辑与删组共用目标组单行短锁，组→Job 锁序；删组锁后独立检查未删除 Job、旧组未结束 Instance 与未收敛 Task。不增加运行期组锁／查询；凭据和缓存生命周期落实前不开放组删除 | [并发协议](data-contracts.md#group-reference-concurrency-proposal)、[竞品比较](#group-deletion-concurrency-evidence)、[验收](verification.md#group-reference-concurrency-checks) |
| 组生命周期与最小依赖 | 第一版无组启停，仅逻辑删除；正式 `job_executor_group` 本批保存身份、名称和管理时间，包含 `deleted_at`，新引用排除已删组；发现与凭据接入后续补齐 | [组契约](data-contracts.md#executor-group-lifecycle)、[验收](verification.md#trigger-handoff-checks) |
| 触发模块持久化交接 | WAITING Instance 及本轮配置是交接事实，开始／截止／结束时间为空；本地有界 ID 提示只加速，候选不是授权，不使用永久 ID 消费水位，不实现假消费者 | [交接契约](data-contracts.md#trigger-durable-handoff)、[验收](verification.md#trigger-handoff-checks) |
| Job 管理入口与事务 | 动作式 POST；完整配置编辑不夹启停／进度，合法空串及零值生效；独立启停与最多 5 次时间预览。编辑条件更新、启用短事务；允许并发编辑覆盖，普通新增不做请求防重，手动创建沿用已有协议 | [管理契约](data-contracts.md#job-management-proposal)、[竞品比较](#job-management-evidence)、[验收](verification.md#job-management-checks) |
| Job 输入查询与错误 | 短字段容量跨层一致；普通保存的说明／参数省略、null、空串均清空，params 仅去首尾空白；默认值、筛选及手动覆盖保留必要存在性。名称字面不区分大小写包含、固定分页投影与两条查询，补按组索引；统一错误及有限重试边界 | [已定契约](data-contracts.md#job-input-query-proposal)、[比较依据](#job-input-query-evidence)、[验收](verification.md#job-input-query-checks) |
| 同 Job 触发记账 | 所有创建入口共用 Job 主键单行短事务；锁后读最新进度，并读取一次调度进程时间判断到期、错失及计算下一次，按需 EXISTS 判断阻塞。自动创建 Instance 与推进进度原子提交，手动不推进 Cron；提交不明按持久化事实重入，不把行锁等同于任期隔离 | [记账协议](execution.md#trigger-accounting)、[验收场景](verification.md#trigger-recovery-checks) |
| Job 触发时间来源 | 统一使用调度后端进程时间，与 robfig 保持同源；不另取数据库当前时间判断到点。候选扫描、校验、预览和启用同样采用进程时间，部署保持系统校时；不修改 Instance／Task 的数据库时间规则 | [时间来源](behavior.md#trigger-clock)、[验收场景](verification.md#trigger-clock-checks) |
| 时钟与时区运维边界 | 部署负责系统校时，不新增应用层时差探测、校正或自动节点隔离；下一次实际检查时应用既有错失规则，持续启用的 Job 不因回拨倒退进度。时区变更走停用、处理在途工作、统一改时区重启及重新启用的维护流程，不迁移历史或补停用期间轮次 | [运维规则](operations.md#scheduler-timezone)、[验收场景](verification.md#clock-operations-checks) |
| Cron 异常处理 | 保存与启用拦截；运行时单 Job 失败不创建 Instance、不推进进度，限频记录错误；加载非法规则仍移除旧内存条目。不自动停用、新增状态或设计专门修复机制，接受管理员修正与必要时重启 | [错误规则](behavior.md#cron-errors)、[比较依据](#cron-error-evidence)、[验收场景](verification.md#cron-error-checks) |
| 触发容量与过载 | 三种自动来源共用 Job ID 去重有界队列和固定工作协程；队满拒绝提示、不推进进度，既有恢复仍按 5 秒错失规则处理。手动共享进程级创建并发额度，同步返回、不进入自动队列，容量不足不后台补跑 | [容量规则](execution.md#trigger-capacity)、[比较依据](references.md#trigger-capacity-evidence)、[验收场景](verification.md#trigger-capacity-checks) |
| 触发部署初值 | Job 总容量 1024、创建并发 32、单次处理 3 秒；自动工作协程数派生，手动不预留额度。预算从处理开始计算，覆盖额度、连接、锁和提交，服从更短上游期限；后续实测校准，与恢复查询的数量和超时分开 | [参数与边界](execution.md#trigger-runtime-defaults)、[验收场景](verification.md#trigger-capacity-checks) |
| 任期生命周期与首次装载 | 本地清理完成后才重新参选，进程资源复用；首次未运行 Cron 批量注册，启动及热更新均整批确认后再检查到期。失主丢弃旧提示、等待本地在途退出，不强杀、不阻塞他机接任；首次失败不停止配置核对及独立恢复 | [生命周期](execution.md#leader-runtime-lifecycle)、[库行为依据](#leader-runtime-evidence)、[验收场景](verification.md#leader-runtime-checks) |
| etcd 接入 | 官方 Session＋Election，正值租约初值 15 秒；控制键与持久 reload 标记分离。通知最多 1 秒、失败不改报已提交保存失败；短请求及配置读取各 5 秒，长操作随生命周期，Watch 原生恢复及 1 至 30 秒抖动退避重建，不承诺固定接任时限 | [接入规则](execution.md#etcd-integration)、[官方依据](#etcd-integration-evidence)、[验收场景](verification.md#etcd-integration-checks) |
| 低频触发恢复 | Cron 负责正常触发；Leader 接任立即检查、任期内每 1 分钟按索引小批量查 Job 进度。配置同步与触发恢复是同一进程内两个独立循环，共用任期启停、不串行依赖；不另加本地 Job 退避补试。超过 5 秒仍应用错失策略，已有 Instance／Task 由对等节点恢复 | [恢复规则](behavior.md#trigger-recovery)、[验收场景](verification.md#trigger-recovery-checks) |
| 切主与旧配置 | 每任期独立 `leaderCtx`，发现失主后取消该任期工作，不取消对等推进；旧回调仅提示 Job ID，经同 Job 短事务按最新配置与进度记账，管理变更共用同一行协调。接受新 Leader 当选后旧在途事务合法提交，不设数据库全局任期锁或零旧提交保证 | [切主边界](execution.md#leader-config-boundary)、[验收场景](verification.md#leader-config-checks) |
| 集群推进 | 单 Leader 触发，持久化任务驱动多节点推进，不绑定整轮负责人 | [总体架构](architecture.md) |
| 执行单元 | 准备、分片和收尾共用 Task 模型与表，不建立 Attempt 历史表 | [任务模型](task-model.md) |
| 状态与执行路径 | Instance 五态、Task 六态；不另维护 Instance 阶段权威字段；普通单 Task，分片必需 Pre、可选 Post，合法空数组按零分片推进 | [状态与生命周期](task-model.md#lifecycle) |
| 终态竞争 | 已有终态不覆盖；父行保护决策点用当前数据库时间判断，到期按超时失败，未到期有效终态竞争；不承诺物理 COMMIT 的精确截止或父子同事务终止 | [终态顺序](behavior.md#terminal-order)、[子任务收敛](behavior.md#task-convergence) |
| 重试与终止 | Job 共用配置、各 Task 独立预算；默认额外重试 3 次、间隔 10 秒，原始执行加重试共 4 次执行机会；整轮超时与尽力取消 | [调度与执行规则](behavior.md) |
| 整轮超时默认值 | 新增表单预填 900 秒，允许按 Job 修改；API 必填、无 SQL 默认，不修改既有配置。有限正数、整轮范围与计时起点不变，不增加失联提前重派 | [默认值契约](data-contracts.md#timeout-default-proposal)、[比较依据](#timeout-default-evidence)、[验收](verification.md#timeout-default-checks) |
| 本地派发与尝试登记 | 生成节点按本地容量批量条件登记，提交后直接派发；恢复复用条件，不改为全节点扫描后的正常派发，不增加整轮归属 | [尝试登记](execution.md#attempt-registration) |
| 新授权与父终态协调 | 每个有界登记批次锁单条父 Instance，停止、超时和正常结束共用父行协调；等待重试仅是候选，结果回报不统一锁父；父已终态但子未终态由共同恢复取消收敛。接受逐轮逐批协调成本，同轮短登记事务串行，不同轮并行 | [授权与终止协调](execution.md#authorization-terminal)、[验收场景](verification.md#authorization-terminal-checks) |
| 提交结果不明 | 登记不明撤掉发送，事后见已登记也不补发；BUSY 有界有限重做原条件记账，执行结果沿用有界保留退避重做；旧状态与尝试限制重复修改。普通查询未见不证明回滚，正常路径不加写后查询 | [恢复规则](execution.md#commit-unknown)、[验收场景](verification.md#commit-unknown-checks) |
| 机会记账与代价 | 登记提交即占机会；仅已确认的明确 BUSY 可条件返还，提交后未发送或通信结果不确定不能免费重发，零重试仍可能未运行就失败；批量仅减少往返，不消除逐行写入成本 | [记账边界](execution.md#dispatch-accounting)、[BUSY 背压](execution.md#busy-backpressure) |
| BUSY 背压 | 仅执行器明确容量满且未接单、未入队、未执行时，对当前有效尝试原子关闭、一次返还登记占用的机会并延期，不消耗执行预算，零重试仍可延期；复用最早可派发时间和既有检查，不延长整轮截止，不保证换节点。通信错误或 BUSY 答复丢失不免费返还，旧身份不复用、终止优先 | [BUSY 背压](execution.md#busy-backpressure) |
| 派发路由与执行器绑定 | 保留原生发现与负载均衡，接单后结合启动实例标识与实际地址条件绑定；取消定向进行 | [执行器身份绑定](execution.md#executor-binding) |
| 绑定缺失窗口 | 从登记的数据库时间有限观察且不重置，到期仅获条件结算资格；绑定／有效结果／未知结算持久化竞争，沿用预算、旧尝试不复活、终止优先；接受重复业务与结果不确定失败 | [观察与条件结算](execution.md#missing-binding) |
| 已绑定结果等待 | 已有持久化绑定但无有效结果时保留当前尝试执行中，等待有效结果、人工停止或整轮截止；不新增 Task 活动查询、心跳或独立失联提前重派。明确失败仍按原预算重试，整轮到期按超时及既有取消规则收敛；接受宕机后等待余时和单片丢结果拖至整轮超时且无正常 Post | [已绑定但无有效结果](execution.md#bound-result-wait) |
| 异步回报与确认 | 双端固定异步、有界内存队列，调度端入队即 ACK、后台小批量落库；无本地文件及同步／异步开关，允许进程故障丢失未落库结果 | [结果回报](execution.md#result-report) |
| 执行器回报重试与清理 | 回报同一尝试的已有结果，不占执行预算；次数、间隔及单次 RPC 超时采用独立内部常量，具体值待定；ACK 后释放，耗尽也释放并记录错误及丢弃指标，不无限等 ACK | [回报重试与清理](execution.md#report-retry) |
| 调度端结果保留 | 已 ACK 结果遇到可恢复写库故障时有界保留、退避重试，队满拒收新结果；不按固定次数或保留 TTL 主动清掉，不套用执行器耗尽释放。按排队、处理中、待重试合计条数、固定消费并发及 RPC 报文上限控制，不做队列总字节／MB 精确配额，不另加业务结果长度限制或 Job 页面参数；永久无效数据不无限重试堵整批，进程退出仍可能丢失 | [调度端结果保留](execution.md#scheduler-result-retention) |
| 结果与收尾推进 | 有效结果提交后才本地合并检查，收尾短事务与唯一约束防重；入队 ACK 不改变 Task 终态 | [完成检查](execution.md#completion-check)、[收尾创建](execution.md#post-creation) |
| 业务接入 | 统一 RPC 入口和具名处理器，执行器回报、调度服务写库 | [总体架构](architecture.md)、[数据与接口契约](data-contracts.md) |
| 执行器组 | 第一版页面动态管理，数据库保存组与发现标识；实例由 etcd 自动发现，原生 go-zero 负载均衡；不采用部署配置静态组映射 | [总体架构](architecture.md)、[管理与运维](operations.md) |
| 部署接入 | 调度服务独立多副本；执行组件可嵌入业务 RPC 或随同域 Worker 独立部署，使用同一协议；同组可选副本兼容被调度处理器与参数契约 | [总体架构](architecture.md)、[管理与运维](operations.md) |
| 内部信任与凭据 | 单执行器进程单组，同组共享 Token 且可提供多个处理器；后端随机生成、不使用公开默认或空值放行，回报绑定 Task 组与有效尝试 | [内部信任边界](architecture.md#internal-trust)、[组凭据管理](operations.md#group-credentials) |
| 注册发现授权 | 继续使用 etcd＋go-zero，不自建调度端注册中心；etcd 访问权限独立保护注册发现，组 Token 只保护任务调用 | [总体架构](architecture.md#internal-trust) |
| 依赖故障与就绪 | 按能力处理数据库、选主、发现与 RPC 故障；回报有容量可入队确认，可恢复写库故障保留并退避、队满背压，不按次数或 TTL 主动清掉，已 ACK 未落库结果也不保证跨进程保存；冷启动与运行中故障分别验证 | [调度端结果保留](execution.md#scheduler-result-retention)、[管理与运维](operations.md#dependency-boundaries)、[验证与交付](verification.md#trust-dependency-checks) |
| 执行日志 | 调度表保存简短记录，详细过程和旧尝试详情使用日志系统 | [管理与运维](operations.md) |
| 第一版功能范围 | 交付任务管理、手动执行与停止、执行查询与分片统计、基础清理、角色权限及接入示例；不做业务实时进度与 Notify、任意调用目标配置或手动失败分片重跑 | [目标与范围](scope.md)、[管理与运维](operations.md) |
| 基准规模与质量方向 | 分别验证 500 条规则、100 条同秒触发、单轮 1,000 分片、100 轮各 10 分片；不是产品硬上限或实测成绩；具体性能门槛在部署资源明确后、正式压测前确认 | [目标与范围](scope.md)、[验证与交付](verification.md) |

上述是已经确认的设计决定，不是实现验收结果。[ADR-0009](../../adr/0009-scheduler-domain-and-etcd.md)已随触发模块总审通过，明确调度域归属及 etcd 与现行 Redis 协调规范的边界。当前按用户要求暂不编码，生效规范的同步留待实现时完成；后续模块出现新的架构选择时另补对应 ADR。

## 文档维护边界

软件设计说明书描述当前有效设计；ADR 记录关键选择、替代方案和取舍；[评审计划](progress.md)维护进度和待决项。旧 `docs/roadmap/job.md` 仅保留新说明书的入口，不再维护第二份设计正文。历史差异使用版本控制追溯，不在每章重复讨论过程。
