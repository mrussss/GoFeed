# Following Feed 混合分发 Phase A

## 原始基线

基线源码为 `9460a766cee3d4767ef861044bb90facfc2b9a2a`。登录用户的 Following Feed 从当前关注关系查询视频。首屏省略时间条件，翻页使用严格的 `create_time < latest_time`。接口以秒为单位返回末条视频时间作为 `next_time`，按 `create_time DESC` 排序，无 ID 平局条件。`has_more` 取决于返回条数是否等于请求上限。默认页大小为 10，允许 1–50。

Redis 在 `v1:feed:listByFollowing:limit=<limit>:accountID=<id>:before=<unix秒或0>` 下缓存整页响应，TTL 为 60 秒。每次命中仍批查 MySQL 中的用户点赞状态。关注及取关成功后，现有逻辑按用户失效其分页缓存。

### MySQL 查询计划

运行 `docker exec -i gofeed-baseline-mysql-1 sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot "$MYSQL_DATABASE"'`，通过标准输入执行 `SELECT VERSION(), @@transaction_isolation`、各表 `COUNT(*)`、`SHOW INDEX FROM socials`、`SHOW INDEX FROM videos` 和下列查询。MySQL 为 8.0.46，默认隔离级别为 `REPEATABLE-READ`，数据为 1,000 个账号、10,000 条视频、20,000 条关注关系和 100,000 条点赞。

```sql
EXPLAIN ANALYZE SELECT * FROM videos
WHERE author_id IN (SELECT vlogger_id FROM socials WHERE follower_id = 117)
ORDER BY create_time DESC LIMIT 10;

EXPLAIN ANALYZE SELECT * FROM videos
WHERE author_id IN (SELECT vlogger_id FROM socials WHERE follower_id = 117)
  AND create_time < '2026-10-01 00:00:00'
ORDER BY create_time DESC LIMIT 10;

EXPLAIN ANALYZE SELECT * FROM videos
WHERE author_id IN (SELECT vlogger_id FROM socials WHERE follower_id = 1)
ORDER BY create_time DESC LIMIT 10;

EXPLAIN ANALYZE SELECT COUNT(*) FROM socials WHERE vlogger_id = 155;
```

| 查询 | 顶层节点首行至末行时间 | 实际计划与扫描 |
| --- | ---: | --- |
| 用户 117，关注 150 人，首屏 | 1.09–3.10 ms | `idx_videos_create_time` 扫 11 条，嵌套循环覆盖查询关注索引 11 次 |
| 用户 117，同样页大小，有时间游标 | 3.87–3.89 ms | `idx_videos_create_time` 范围扫描 13 条，覆盖查询关注索引 13 次 |
| 用户 1，关注 50 人，首屏 | 7.36–7.37 ms | 先覆盖扫描 50 条关注关系，再走 `idx_videos_author_id` 取 3,640 条候选视频并排序 |
| 作者 155 的粉丝计数 | 0.135 ms | `idx_social_vlogger` 覆盖读取 649 条关注关系 |

三个 Feed 查询都返回 10 条。关注子查询使用 `idx_social_follower_vlogger(follower_id, vlogger_id)`。表上也存在 `idx_social_follower(follower_id)`，这三条计划均选择组合索引。`videos` 有独立的作者和创建时间索引。表中的时间是单次 MySQL 执行观测。

### 整页缓存采样

本地已有脚本分别覆盖 `listLatest`、点赞及其缓存，未找到 Following Feed 专用脚本。用 `git archive 9460a76 backend` 在临时目录提取原始源码，运行临时的 `go run ./cmd/followprobe`，直接调用该版本的 `FeedService.ListByFollowing`。探针使用现有 `gofeed-perf-mysql-1`（同为 1,000 个账号、10,000 条视频和 20,000 条关注关系，另有 100,600 条点赞）和临时 Redis。该 MySQL 库具备此版本查询所需的点赞状态列。探针按用户分别串行执行 5 对首屏请求，每对在首个请求前仅删除该页 Redis key，读取回填后的 `PTTL`，再发起同参数的第二个请求。所有页都返回 10 条。

| 用户 | 冷页缓存中位数（范围） | 热页缓存中位数（范围） |
| --- | ---: | ---: |
| 117 | 3.046 ms（2.599–6.038） | 1.278 ms（1.106–1.435） |
| 1 | 9.182 ms（6.158–10.377） | 1.367 ms（1.050–1.501） |

回填后观测到 `PTTL` 为 59,999–60,000 ms。采样范围是 Go Service 调用，MySQL 缓冲池保持运行时状态。本次采样未覆盖 HTTP 端到端延迟、并发吞吐、冷 MySQL 缓冲池和多策略对照。发布侧的 Push / Pull 成本对照见 [Phase B](following-hybrid-phase-b.md)。

## 发布事务和兼容性

配置通过 API 现有 YAML 加载机制读取，旧配置自动使用默认值。可按项覆盖以下默认值，加载时校验 `0 <= low < initial < high`。

```yaml
following_feed:
  low_threshold: 9000
  initial_threshold: 10000
  high_threshold: 11000
```

发布事务使用 `READ COMMITTED`，先通过账号主键锁定已有 `accounts` 行。即使作者状态尚未创建，同一作者的发布也在这个锁上排队。拿锁后读取作者状态，并使用 `socials.vlogger_id` 索引查一次当前已提交的粉丝数量。首次或跨阈值时写入作者状态，然后保存 Video、Outbox 和视频标签。关注与取关可并发提交，COUNT 依据该语句开始时的已提交快照，作者状态和发布数据则随同一个事务提交或回滚。

API 启动时用 GORM `AutoMigrate` 新建 `author_feed_states`，主键为 `author_id`，`distribution_mode` 只接受 `push` 或 `pull`。`videos.distribution_mode` 和 `outbox_msgs.author_id`、`distribution_mode`、`event_id` 为可空新增列。旧行保留 NULL，新作者在首次发布时初始化。Outbox 的 32 字符十六进制 EventID 有唯一索引，历史 NULL 可重复。Video 和 Outbox 的策略字段在应用模型内仅允许创建时写入。现有发布 JSON 结构保持兼容。

Phase B 可从已提交的 `OutboxMsg` 读取 `EventID`、`VideoID`、`AuthorID`、`DistributionMode`、`EventType` 和 `CreateTime`，以该行的策略为准。Relay 改造需用持久化 EventID 做重试身份，接入 Publisher Confirm 和 Fanout，并为包含 NULL 的历史消息保留兼容路径。Phase A 的现有 TimelineMQ 仍按 `PublishVideo(ctx, videoID, createTime)` 投递，当前 poller 在投递返回成功后删除 Outbox 行。
