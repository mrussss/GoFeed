# GoFeed S3 正式落地验收

## 实现

- `backend/internal/middleware/rabbitmq/commandMQ.go` 将 FNV-1a 64 的哈希输入由两个大端 uint64（账号和视频）改为一个大端 uint64（视频），再对 4 取模。同一视频的点赞和取消点赞由同一分区消费。Relay、消费者现有的分区校验和实验发布器继续调用同一个函数。四个持久化队列、交换机和消息格式沿用原配置。
- `backend/internal/video/like_repo.go` 按 `(video_id, account_id)` 合并到最高 Revision，排序后用一次 tuple IN 和 `SELECT … FOR UPDATE` 锁定已有关系。过期与重复 Revision 跳过，未分配的 Revision 使事务失败。一次 `CASE id` 更新推进有效 Revision，只有状态确实发生变化的行才更新时间。按视频汇总真实 Delta，对非零净值执行一次带非负约束的原子计数更新。
- `backend/internal/worker/commandrelay_test.go` 固定四个分区的哈希向量，覆盖同视频不同账号和跨字节的视频 ID。
- `backend/internal/worker/likebatchworker_test.go` 扩展混合 CASE 更新、多视频、净 Delta 为零和真实四分区测试。四分区测试通过独立发布连接同时向热点视频发送命令，检查实际队列分布、重复投递和最终计数。
- `backend/internal/worker/like_pipeline_test.go` 用真实 AMQP 投递、ACK 和关闭 channel 后的排空验证替换滞后的管理端 ACK 计数断言。
- `docs/GoFeed_S3正式落地验收.md` 记录本次验证。

单个 MySQL 事务覆盖关系锁定、Revision 校验、CASE 更新和视频计数。Worker 保留 50 条或 20ms 的批次限制。事务失败时回滚，RabbitMQ channel 关闭后未 ACK 消息可以重投。成功时按 **COMMIT → 现有 Redis 双 Key 失效 → ACK** 执行。缓存失效失败记录日志后继续 ACK。Worker 不写 `created_at` 或 `next_revision`。

候选归档的 79 个文件均通过 `environment.json` 中的 SHA-256 校验，基线为 `484b7a4`。正式版沿用候选的分区算法和批量 SQL，继续使用原 C 阶段的双 Key 缓存路径、TTL 和单 Relay。性能仪表只加在隔离构建上。生产者与原实验发布器的调用签名保持兼容。复测用的仓库、Worker 和发布器与矩阵单热点 S3 归档逐文件比对，只有共享分区函数的注释与未使用账号参数的写法有差异。

## 集成测试与工程检查

隔离 Compose 使用 MySQL 8.0.46、RabbitMQ 3.13.7、Redis 7.4.11 和 Go 1.24.5。常规与 race 测试分别使用独立的 A2、Batch、端到端 vhost 和临时 MySQL schema。

| 验证 | 结果 |
|---|---|
| 固定哈希向量及实际发布 | 常规与 race 的独立测试进程均命中相同向量。热点视频 59 条 LIKE、UNLIKE 和重复命令全部进入分区 2，其余三个分区各处理一个视频。最终计数为 4、1、1、1。 |
| Revision、批处理与计数 | 首次点赞、取消、连续反转、过期和乱序、批内重复、同状态推进、多用户及多视频均通过。混合 CASE 检查了时间戳和 Revision 水位，净 Delta 为零的视频跳过计数更新。 |
| 故障与重投 | 在第二个视频计数更新处注入失败，关系与已更新的第一个视频完整回滚且未 ACK。真实 RabbitMQ 验证回滚时重投、提交后 ACK 失败时重投且不重复计数。 |
| HTTP、A1/A2、缓存 | 真实验证 HTTP 202、Outbox、Relay Confirm / Return / NACK、消费者重连、缓存失效和 Redis 故障回源。 |
| 工程检查 | `gofmt`、`go test -count=1 ./...`、`go test -race -count=1 ./...`、`go vet ./...` 和 `go build ./...` 通过，集成用例均实际执行。 |

最初的 race 测试暴露了旧端到端用例依赖累计、延迟采样的 ACK 统计，出现提前读到旧消息和等待超时。改用真实 broker 投递与排空检查后，最终常规与 race 全套均通过。

## MySQL 执行计划

在 MySQL 8.0.46 的隔离基线数据集上，用实际 tuple 参数执行 `EXPLAIN FORMAT=JSON`。1 个关系键为 `const`，5 个键、50 个单视频键、50 个跨视频键均为 `range`，每组估算扫描行数与键数相同。全部使用 `idx_like_video_account` 的 `video_id` 和 `account_id` 两列。现有表的主键为 `id`，关系锁定使用既有唯一复合索引，没有全表扫描或新增索引。

## 代表性性能回归

三种场景各跑一轮，采用原固定数据集、意图生成器、15 秒预热、60 秒统计窗口及相同的分析器。三个场景的 CSV 校验和、归一化意图 SHA-256 和分区计数均与原矩阵对应场景一致。Go、MySQL、RabbitMQ 与 Redis 使用原版本，服务配额保持 2 CPU / 2 GiB。数据列为本次 / 原 S3，原值为三轮中位数，Ready 为原三轮最大采样值。显式 SQL 不计 BEGIN 和 COMMIT。

| 场景（命令/s） | Worker ACK/s | 显式 SQL/ACK | 事务 P95（ms） | 行锁等待次数 | 分区 Ready 峰值 |
|---|---:|---:|---:|---:|---:|
| uniform / 100 | 100.017 / 100.000 | 2.255 / 2.268 | 37.334 / 35.564 | 0 / 0 | 0 / 0 |
| zipf / 400 | 400.083 / 399.933 | 1.080 / 1.095 | 45.100 / 44.274 | 0 / 0 | 0 / 0 |
| single / 800 | 798.450 / 798.100 | 0.087 / 0.089 | 29.603 / 28.729 | 0 / 0 | 48 / 47 |

三轮分别完整 ACK 7,500、30,000、60,000 条命令。Outbox、Revision、逐视频计数和 broker 排空均通过在线及独立离线对账。单热点 60,000 条都进入分区 2，三轮无观测到的行锁等待、死锁或锁超时。复测与原 S3 表现一致，各场景 SQL/ACK 差异低于 3%，事务 P95 增幅约 1.9% 至 5.0%，单热点 Ready 峰值比原样本最大值多 1 条。

原历史统计和本次采样保存在工作区。 [原始实验报告](GoFeed_点赞链路性能与缓存对照实验.md) · [复测汇总](../data/perf/s3-acceptance/metrics.json) · [执行计划](../data/perf/s3-acceptance/explain.log)

## 切换与适用范围

开发 broker 的默认 vhost 检查时没有 `like.commands.*` 队列或连接。既有实验 broker 的默认 vhost 和 CacheB-S3 vhost 均核对了四个分区，Ready 与 Unacked 全为 0。业务服务的切换留给部署执行，本次在新建的隔离 Compose 和 vhost 启用 S3。

实际升级时先停止旧入口接收新请求和任何独立旧发布器，保留旧 Worker 处理积压，直到 Outbox pending 与四个分区的 Ready、Unacked 均为 0。停掉包含旧 Relay 的 Worker 后复查队列和发布连接，再统一启动新版 Worker 并恢复入口。回退采用同样的排空顺序。现有队列和持久化数据可以沿用。本次创建的 Compose、vhost、数据库、Redis 容器及卷已随隔离环境清理。

同一热门视频集中到一个分区，单个消费者的上限仍需更高档位验证。Worker 隔离负载通过 A1 分配 Revision、直接向 RabbitMQ 发送命令，端到端吞吐仍受现有单 Relay 约束。管理端 Ready 采用周期采样，可能漏掉短时峰值。

## Git diff stat

`6 files changed, 230 insertions(+), 75 deletions(-)`
