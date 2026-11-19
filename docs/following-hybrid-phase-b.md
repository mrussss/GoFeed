# Following Feed 混合分发 Phase B

## 发布链路

API 的 `VideoService.Publish` 在同一 MySQL 事务中保存视频、策略快照和待发布 Outbox。现有 Worker 进程运行一条视频 Relay 和两条独立的消费循环。Docker Compose 和 `start.sh` 已包含 API 与 Worker。升级时先停用旧 API 的视频轮询器，避免它抢读并删除待发布行。

Relay 按 ID 读取当前可见的 pending 行。AMQP MessageId、作者、模式和发布时间取自该行。建立发布连接时声明持久化 Topic Exchange `video.timeline.events` 和两个持久化队列，再启用 Confirm。发布键为 `video.timeline.publish`，原全局队列 `video.timeline.update.queue` 保留 `video.timeline.*` 绑定，`video.following.fanout.queue` 使用精确绑定。两个消费循环各自建立连接和通道。

发布使用 persistent 消息、mandatory、单条在途 Confirm 和 3 秒超时。收到 ACK 且无 Return 后，按 `id` 和 `status=pending` 条件标记 published。超时、断开、NACK、Return 或 MySQL 标记失败后，pending 行保留并使用相同的 MessageId 和载荷重投。已发布行保留策略快照。Confirm 反映 broker 对当前路由的处理，拓扑声明负责创建两个目标绑定。

旧 Outbox 的三个快照字段均为 NULL。Relay 使用 `legacy-outbox-<id>` 和显式 `legacy` 标记。全局照常处理，Fanout 确认并跳过。部分缺失或非法的新快照记录错误并保留为 `invalid`，便于排查。已在原全局队列中的旧版 Timeline JSON 仍可消费。

## Redis 写入

`feed:inbox:<userID>` 与 `feed:author_outbox:<authorID>` 通过 Redis Client 添加既有前缀，默认为 `v1:`。候选 ZSET 的 Score 为 0，Member 编码为 `%013d:%020d`，分别是发布时间 Unix 毫秒和无符号 VideoID。倒序 Lex 对应发布时间降序和同毫秒 VideoID 降序。裁剪用 `ZREMRANGEBYRANK 0 -(limit+1)` 移除最老候选。

```yaml
following_fanout:
  inbox_max_len: 1000
  author_outbox_max_len: 500
  batch_size: 500
```

省略配置时采用上述默认值，启动时校验三个值为正数。Push 基于 Social ID 游标只读 `id` 和 `follower_id`，每页用 Redis Pipeline 批量写入和裁剪。Pull 直接写入并裁剪作者索引。两路均使用事件快照。Fanout prefetch 为 1，全部命令成功后手动 ACK。暂时性错误或结果不确定时关闭本消费连接，由 RabbitMQ 重排未确认事件，一秒后重连。ACK 前断开也会重投，同一候选的 Member 保持不变。

非法 Fanout 消息直接隔离。Redis 返回 `WRONGTYPE` 时先等 100 毫秒，再重试一次，仍收到 `WRONGTYPE` 则隔离。隔离使用默认交换机直达持久队列 `video.following.fanout.quarantine.queue`，等到 Publisher Confirm 且无 Return 才 ACK 原投递。目的队列无自动过期，载荷保留可逆的 Base64 原始字节、原 MessageId、来源、分类、原因和时间。原 MessageId 为空时，用原始字节的 SHA-256 生成可重现的 AMQP MessageId。原 ACK 丢失可能产生重复隔离副本，按 MessageId 排查并人工重放。

在已有 MySQL 8.0.46 基线库执行下列语句，`idx_social_vlogger` 利用 InnoDB 二级索引的主键后缀，范围扫描直接输出 500 条记录。单次顶层节点耗时 0.164–3.55 ms。相同查询省略索引提示时，一次执行选用了索引合并与排序，耗时 8.11–8.13 ms。Phase B 复用现有索引。

```sql
EXPLAIN ANALYZE SELECT id, follower_id FROM socials FORCE INDEX (idx_social_vlogger)
WHERE vlogger_id = 155 AND id > 1000 ORDER BY id LIMIT 500;
```

## 当前实现边界

亮点①交付包括 Phase A 的发布策略和 Phase B 的异步分发。

- Following API 继续使用原有 MySQL 读路径。Push Fanout 按每次分页查询时 MySQL 中可见的关注关系扫描。新关注关系若在游标经过该记录前可见，用户可能收到此前已发布视频的候选。若在本次扫描完成后才关注，这次 Fanout 不会自动为该用户补写。新关注者应有机会看到作者近期发布的内容，后续读取方案应明确历史内容如何获取，避免默认按关注时间过滤视频。
- 旧 Relay 已删除的历史事件无法通过当前 Outbox 重放。当前实现尚未定义索引重建、已发布 Outbox 保留期或重放进度恢复。失败重放从首批粉丝重新扫描。
- Worker 运行单个视频 Relay 和一个 Fanout 消费循环。持续基础设施故障或隔离目的地不可用时，未确认的队首事件仍可能暂时阻塞 Fanout。全局队列独立处理。

## Push / Pull 分发成本对照（2026-11-19）

比较同一条视频在两种粉丝规模下的 Worker 分发成本，并验证默认 Hybrid 策略及失败重放。Phase A 记录的是旧版 Following 读路径，原项目性能基线主要测匿名最新流 HTTP 吞吐。本轮直接调用 `writeFollowingCandidates`，计时覆盖事件校验、候选编码、查粉丝、写入和裁剪 ZSET，直到函数返回。发布事务中的粉丝计数、MQ 交接和 ACK 由正确性测试覆盖，其耗时在本轮计时窗口之外。

### 环境与对照方法

| 项目 | 实际环境 |
| --- | --- |
| 源码 | `feat/following-hybrid`，`7e04db9b7bd0f6415dc59cb3cf782bf01d014982`，测量前工作区干净 |
| 系统 | Ubuntu 22.04.5 LTS，WSL2 内核 6.18.33.2，x86_64，Go 1.22.12 |
| CPU 与内存 | Intel Core i9-14900HX，WSL 可见 32 个逻辑 CPU 和 15.51 GiB 内存，swap 4 GiB。Docker Desktop Server 29.8.1，可见 32 个逻辑 CPU 和 15.51 GiB 内存。三个临时容器均无单独 CPU/内存配额或绑核 |
| 服务 | MySQL 8.0.46，InnoDB 缓冲池 128 MiB，默认隔离级别 `REPEATABLE-READ`。Redis 7.4.11，DB 15，AOF `everysec`，`maxmemory=0`，`noeviction`。RabbitMQ 3.13.7 用于集成测试 |
| Fanout 配置 | `batch_size=500`，`inbox_max_len=1000`，`author_outbox_max_len=500`。Hybrid 默认阈值为 `9000 / 10000 / 11000` |

复用本地镜像启动独立的临时容器，端口仅绑定回环地址。MySQL 使用独立 Schema 和当前 `AutoMigrate` 生成的 InnoDB 表及索引，粉丝分页沿用 `idx_social_vlogger` 和 Social ID 游标。Redis 键使用实验 Schema 名作为专属前缀。测试期间原项目及基线容器仍运行，共享宿主机资源。

基线生成器会按固定比例同时生成视频和点赞，现有基线数据最大作者为 649 粉丝。此次用最小数据生成逻辑沿用种子 `20261116`，共插入 12,002 个账号和 13,000 条关注。作者 ID 为 1 和 2。使用 Go `rand.New(rand.NewSource(20261116)).Perm(12000)`，元素加 3 得到粉丝 ID，先按排列顺序给作者 2 插入全部 12,000 条，再给作者 1 插入前 1,000 条。同一作者的两条路径共用这些关注关系。计时前校验账号、关系数量及关联，并逐项比对存入的粉丝列表。

临时 Go 探针使用同一进程和连接池，以事件中的 `distribution_mode` 分别强制走 Push 或 Pull。四组串行执行，每组预热一次、正式测量三次。每次从空的实验 Redis 前缀开始，清理、校验和数据准备都在计时窗之外。GORM Trace 记录真实粉丝分页的返回行数。独立 Redis 实例的 `INFO commandstats` 前后差值记录实际执行的 `ZADD` 和 `ZREMRANGEBYRANK`。计时后扫描专属前缀，核对目标键及每个键中的精确 Member 和 Score，并累加各键的 `MEMORY USAGE`。

### 实测结果

[原始 CSV](../test/following-hybrid-cost.csv) 保留 12 次正式测量和 2 次补测，耗时单位为微秒。A2、B2 前三次的极差分别为各自中位数的 107.89% 和 65.28%，因此各补测一次。A1、B1 分别为 5.82% 和 11.16%。下表对每组所有保留样本计算中位数和范围，四个样本取中间两值的平均。A2、B2 仅前三次的中位数分别为 0.321 ms 和 0.430 ms。

| 实验 | 粉丝 | 路径 | Fanout 中位数（最小–最大，ms） | MySQL 行数 / 查询次数 | Inbox / Outbox | Member | Redis 写命令 | 专属键内存 |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 1,000 | Pure Push | 7.244（7.135–7.557） | 1,000 / 3 | 1,000 / 0 | 1,000 | 2,000 | 152,000 B |
| A2 | 1,000 | Pure Pull | 0.299（0.214–0.561） | 0 / 0 | 0 / 1 | 1 | 2 | 168 B |
| B1 | 12,000 | Pure Push | 91.619（88.839–99.063） | 12,000 / 25 | 12,000 / 0 | 12,000 | 24,000 | 1,824,000 B |
| B2 | 12,000 | Pure Pull | 0.405（0.288–0.569） | 0 / 0 | 0 / 1 | 1 | 2 | 168 B |

14 次测量全部通过逐键校验，每个目标键只有一个正确的候选，实验前缀下没有多余键。A1 返回 `500 + 500 + 0` 行。B1 的 24 个满页后有一次空查询，用于确认已读完。每个目标键对应一条 `ZADD` 和一条 `ZREMRANGEBYRANK`。1,000 粉丝的 Push 用 2 个 Pipeline 批次提交 2,000 条 Redis 命令，12,000 粉丝的 Push 用 24 个批次提交 24,000 条，Pull 用 1 个批次提交 2 条。命令计数来自 Redis 服务端，Pipeline 批次数来自分发路径。实验未测网络往返次数。内存列为实验前缀从空到写入完成后的键级字节差值，范围仅包含这些键及值的 Redis 内存估算。

### Hybrid 策略与快照

| 发布时的原状态 | 粉丝数 | 策略结果 | 验证来源 |
| --- | ---: | --- | --- |
| 首次初始化 | 1,000 | Push | 默认配置下的 MySQL 发布 |
| 首次初始化 | 12,000 | Pull | 默认配置下的 MySQL 发布 |
| Push | 10,999 | 保持 Push | `TestDistributionMode` |
| Push | 11,000 | 切换 Pull | `TestDistributionMode` |
| Pull | 10,000 | 保持 Pull | 默认配置下的 MySQL 发布 |
| Pull | 9,001 | 保持 Pull | `TestDistributionMode` |
| Pull | 9,000 | 切换 Push | `TestDistributionMode` 和默认配置下的 MySQL 发布 |

`TestDistributionMode` 也通过了首次初始化 9,999 → Push、10,000 → Pull 的分界测试。两条切换线之间保留作者已有模式，因此粉丝数在区间内波动时不会来回切换。默认阈值是工程配置。

`TestPublishFollowingMySQL` 使用 9 / 10 / 11 的缩比阈值，验证策略持久化、跨阈值后早期 Video 和 Outbox 快照保持不变、事务回滚，以及全局时间线回归。默认配置复核中，12,000 粉丝作者先生成 Pull 事件，再在降到 9,000 时切换到 Push。此后用原事件重跑实际分发函数，仍只有原 Outbox 的一个候选。Relay 重投测试也验证两次消息载荷一致。Worker 直接使用事件快照决定路径，Push 重放按当时可见的关注关系重新分页。

### 可靠性复核

在原工作树的 `backend/` 中执行 `go test -race -count=1 -json ./...` 和 `go vet ./...`，退出码均为 0。全量测试连接真实的临时 MySQL、Redis 和 RabbitMQ。发布、Fanout、点赞 Relay、Batch 和端到端测试使用各自的独立 RabbitMQ vhost，并配置管理 API 执行断开真实连接与 NACK 用例。JSON 结果共记录 91 个顶层测试及子测试通过，测试用例跳过数为 0。另有 14 个包显示 `[no test files]`。

| 已运行用例 | 核对结果 |
| --- | --- |
| `push_follower_pages` | 0、1、201、1,000 粉丝的目标正确。对同一事件重复投递，所有实际目标仍只有一个候选 |
| `partial_pipeline_and_replay` | 一部分 Inbox 已写入后遇到 WRONGTYPE。修复故障键后重放，故障键补齐，已成功目标仍只有一个候选 |
| `persistent_wrongtype_releases_B_C_D` | 应用层只重试一次，持续 WRONGTYPE 的原消息被确认移交至隔离队列，后续事件继续处理 |
| `quarantine_return_preserves_original_delivery`、`quarantine_confirm_then_original_ack_lost` | 隔离发布失败后原消息仍能重投。隔离已确认而原 ACK 丢失时，副本保留同一消息标识 |
| `disconnect_after_redis_before_ack`、`redis_unavailable_then_recovery` | 管理 API 断开 Redis 写成功后的真实消费连接，重投后 ZSET 仍只有一个候选。Redis 暂时不可用时恢复后能完成分发 |
| `bounded_lex_indexes_and_pull_isolation` | Redis ZSET 排序和裁剪正确，Pull 只写作者 Outbox |

### 结论与适用范围

- **单看发布侧分发成本，Pull 在两种粉丝规模下都更低。** 一条视频的逻辑候选写入量为 `W_push = F`、`W_pull = 1`，其中 `F` 是粉丝数。12,000 粉丝时，Pull 将候选从 12,000 个降至 1 个、Redis 写命令从 24,000 条降至 2 条，并省去 25 次 Worker 粉丝分页查询。该组实测中位耗时从 91.619 ms 降至 0.405 ms。亚毫秒 Pull 样本的相对波动较大，候选、命令和分页计数是更稳定的成本依据。
- **Push 将候选准备前置，Pull 将候选获取留给读取阶段。** 在 1,000 粉丝、单事件和热连接条件下，Push 用两个批次提前构建粉丝 Inbox，异步分发中位耗时为 7.244 ms。预写旨在减少后续 Feed 读取时的候选获取工作，总体收益仍需结合完整读取链路验证。Pull 通过写入作者 Outbox 避免发布时向粉丝扩散，读取时需要获取作者候选并归并。这部分成本取决于活跃粉丝比例、Feed 请求频率和读者关注的大 V 数量。
- **Hybrid 根据作者粉丝数与当前状态选路。** 默认配置下，首次发布分别为 1,000 粉丝选 Push、12,000 粉丝选 Pull。状态机和真实发布测试验证了边界、滞回和策略快照。9,000 / 10,000 / 11,000 是工程配置，实验未求取最优阈值。
- **实验范围是异步分发。** 当前 Following API 仍走原 MySQL 读路径及整页缓存，尚未接入完整的 Inbox + Author Outbox 读取。端到端 Feed 延迟、读取收益和总读写成本仍未验证。成本测量也未覆盖常驻热 ZSET、并发发布、生产容量和发布事务本身。
