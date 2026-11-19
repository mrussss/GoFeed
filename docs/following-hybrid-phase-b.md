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

## 后续阶段依赖

- Following API 继续使用原有 MySQL 读路径。候选按消费时的粉丝关系构建，新关注者可能收到发布在先的候选。Phase C 需使用关注时间过滤。
- 旧 Relay 已删除的历史事件无法通过当前 Outbox 重放。索引重建、已发布 Outbox 保留期及重放进度恢复留给后续阶段。本阶段的失败重放从首批粉丝重新扫描。
- 本阶段运行单个视频 Relay 和一个 Fanout 消费循环。持续基础设施故障或隔离目的地不可用时，未确认的队首事件仍可能暂时阻塞 Fanout。全局队列独立处理。
