# GoFeed Baseline Small 数据导入报告

日期：2026-11-16。结论：**小档数据已成功导入独立基线 MySQL，提交前与提交后完整性验证通过。Redis 仍保持未初始化状态。**

## 1. 工具与数据映射

在现有独立测试模块中新增 [import.go](../tools/baseline/import.go)、[import_test.go](../tools/baseline/import_test.go)，扩展命令入口，增加 MySQL 驱动依赖及 go.sum；未修改业务代码、原始 Compose 或基线 Compose。使用方法见 [IMPORT.md](../tools/baseline/IMPORT.md)。

默认 `-import` 是 dry-run，必须显式传入 `-apply` 才写入。CSV 完整性使用原 ReadDataset/Validate 校验，包括表头、Hash、文件大小、行数、关联及唯一性。四张核心表的实际 SHOW CREATE TABLE、information_schema 字段类型、必要索引和约束已在导入前核对。

| CSV | 目标表 | 处理 |
|---|---|---|
| accounts.csv | accounts | 显式保存原始 ID、用户名、密码 Hash；Token 等为空字符串 |
| videos.csv | videos | 显式保存原始 ID、作者、内容、UTC 发布时间、点赞与热度计数 |
| socials.csv | socials | 显式保存原始 ID、关注者与作者 ID |
| likes.csv | likes | 显式保存原始 ID、视频与账号 ID、UTC 点赞时间 |
| authors.csv | 不导入 | 200 位作者的类型/产量元数据，仅用于校验 |

模型对照来源：[Account](../backend/internal/account/entity.go)、[Video](../backend/internal/video/video_entity.go)、[Social](../backend/internal/social/entity.go)、[Like](../backend/internal/video/like_entity.go)。四表字段及模型的核心约束匹配，时间列均为 datetime(3)，InnoDB，联合唯一约束存在，未发现数据库外键或触发器。

## 2. 正式导入前安全检查

下列条件均通过后才执行 INSERT：

- Compose project 为 `gofeed-baseline`，运行的 MySQL 容器为 `gofeed-baseline-mysql-1`。
- 数据目录挂载专属 `gofeed-baseline_mysql_data`，卷标签 project 匹配。
- 连接端点固定 `127.0.0.1:23307`。容器内外查询的 server_uuid 一致，为 `012a68f6-e194-11f1-9ea6-2a0be94ac354`；hostname 与 Docker 记录一致。
- 库名为 `feedsystem`，这是基线独立实例中的库，**未连接原项目实例的同名库**。
- Backend、Worker 均属于基线项目，配置 TZ=UTC、连接内部 mysql 服务。
- 驱动显式使用 loc=UTC、parseTime=true；global/session time_zone 均为 +00:00。
- 四张核心表行数均为 0。
- 所有 CSV 完整性校验通过，实际 DDL 与映射匹配。

Dry-run 实际耗时 **851 ms**，无 INSERT。正式执行时重新检查容器身份，取得连接级导入锁，并在事务内再次检查四表为空。

## 3. 正式导入结果

实际执行：

```bash
cd tools/baseline
go run . -import -data ../../data/baseline/small
go run . -import -apply -data ../../data/baseline/small -batch-size 500
```

导入顺序为 accounts → videos → socials → likes。全部使用参数化批量 INSERT，每批最多 500 行，合计 **262 条 INSERT、一个事务提交**。无逐行提交，无覆盖、忽略冲突或删除操作。

| 表 | 实际行数 | ID 范围 |
|---|---:|---|
| accounts | 1,000 | 1～1,000 |
| videos | 10,000 | 1～10,000 |
| socials | 20,000 | 1～20,000 |
| likes | 100,000 | 1～100,000 |
| 总计 | **131,000** | 全部保留 CSV 原始 ID |

耗时（程序内部计时，不含 go run 编译启动）：

- 批量 INSERT：**6.409 秒**。
- INSERT + 提交前验证 + COMMIT：**7.854 秒**。
- 全流程，含 CSV 校验、前置检查及提交后验证：**9.101 秒**。

这些是本次导入耗时，不是 Feed 性能基线。

## 4. 提交前及提交后验证

两个阶段均通过：

| 检查项 | 实际结果 |
|---|---|
| 四表行数与 ID 范围 | 与 CSV 一致 |
| 视频作者孤儿关联 | 0 |
| 关注关系账号孤儿关联 | 0 |
| 点赞账号/视频孤儿关联 | 0 |
| 重复关注关系 | 0 |
| 重复点赞关系 | 0 |
| 自关注关系 | 0 |
| videos.likes_count 与实际点赞数不一致 | 0 |
| 基础快照 popularity 与点赞数不一致 | 0 |
| 点赞早于视频发布或时间为空 | 0 |
| 视频冗余作者名与账号不一致 | 0 |

UTC 时间抽样读回结果：

| 表 | ID | 数据库按 UTC 读回 | 与 CSV 对比 |
|---|---:|---|---|
| videos | 1 | 2026-11-10T12:47:53.987Z | 一致 |
| videos | 5,000 | 2026-11-03T00:43:26.593Z | 一致 |
| videos | 10,000 | 2026-09-27T01:22:12.579Z | 一致 |
| likes | 1 | 2026-11-10T13:34:31.867Z | 一致 |
| likes | 50,000 | 2026-09-06T10:29:52.211Z | 一致 |
| likes | 100,000 | 2026-11-15T23:54:41.008Z | 一致 |

以上验证的是独立导入工具的 UTC 驱动读回；本次没有调用业务 Feed 接口进行 API 输出验证。

测试结果：`go test -v ./...` 全部通过（2.122 秒），`go vet ./...` 通过。新增目标安全拒绝与 SQL 映射测试均为不连接数据库的单元测试。导入后再次执行 dry-run，工具按预期报告 `ABORT: accounts has 1000 rows; no cleanup or overwrite allowed`，在任何 INSERT 前拒绝继续，没有清理数据。

## 5. Redis 时间线现状及原代码机制

只读检查目标为 `gofeed-baseline-redis-1`，已核验项目和专属 Redis 卷：

```text
DB 0:
TYPE  v1:feed:global_timeline = none
ZCARD v1:feed:global_timeline = 0
DBSIZE = 0
```

因此数据库导入后，Redis 时间线尚不存在；这是离线导入未执行发布副作用的正常结果。

现有源码机制：

1. [feed/service.go 第 149 行](../backend/internal/feed/service.go#L149)：Redis 不可用或读取时间线失败时，最新 Feed 可以回源数据库。
2. [feed/service.go 第 163 行](../backend/internal/feed/service.go#L163)：Redis 正常但 ZSET 为空时，listLatest 通过 singleflight 从 MySQL 查询最新最多 1000 条，并用视频 ID/Unix 毫秒重建时间线，之后重新查询。无需伪造 MQ 事件。
3. [feed/service.go 第 202 行](../backend/internal/feed/service.go#L202)：已有非空时间线不会在这里自动做全量纠错；超过热点范围的冷数据仍可回源数据库。
4. [outboxworker.go 第 17 行](../backend/internal/worker/outboxworker.go#L17)：OutboxPoller 只处理 pending 消息，不扫描历史视频。此次未创建 Outbox，自然不会触发发布链路初始化缓存。

**本次没有调用 Feed、写入 Redis、构造 MQ 事件或额外执行初始化。**

后续建议：冷缓存场景保留当前状态，单独记录首次请求的重建成本；暖缓存场景先安排一次不计入压测的匿名 listLatest，再只读检查 ZCARD 应为 1000、成员应为数据库最新 1000 条、score 应为 Unix 毫秒，然后按固定请求预热实体缓存。当前只报告方案，尚未执行。现有机制未检查重建 ZAdd 错误，后续必须读回核验，不能只依据接口 200 判定预热成功。

## 6. 遗留问题及范围

- 下一步需要安排 Feed 查询功能验证和冷热缓存准备，再开展 k6 压测；当前仍为冷缓存状态。
- Redis 最近一小时互动热榜未初始化；数据库累计 popularity 不等于 Redis 窗口热度。具体热榜场景需单独确定，不在本轮随意灌分数。
- CSV 中媒体 URL 仍是占位路径，未准备真实媒体文件；不影响记录导入，播放/下载测试前需补齐资源。
- Token 为空，登录类测试前需要准备账号池；本次未执行登录或生成 Token。
- 本次只向独立基线库导入四表，未操作原项目数据库，未修改业务代码、原始 Compose 或执行 Git Commit/Push；未执行 TRUNCATE、DROP 或删除数据。
- 导入后五个基线服务仍全部 healthy。

**结论：数据库层的小档数据准备完成，可进入 Feed 功能验证与缓存场景准备；尚未开始压测。**
