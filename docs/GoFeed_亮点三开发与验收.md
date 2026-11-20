# GoFeed 亮点三开发与验收

## 模块 0 · 读取基线

2026-11-20（UTC）从最新的 `origin/main` 创建 `feat/feed-read-reliability`。实际基准为 `c628b0fb472a0b11ea9448f99a3b5a77a40033d0`。API 使用该提交的 Git 归档构建，测量前未修改业务代码。二进制 SHA-256 为 `722d1cc8cfbc8fb41a5f64160fb59ac6422734301909b67bdac06a9f6f629c1b`。

### 环境与数据

| 项目 | 实际配置 |
| --- | --- |
| Go | 宿主机默认 `go1.22.12 linux/amd64`，后端按 `go.mod` 使用 `go1.24.5` 构建 |
| 宿主机 | Intel Core i9-14900HX，32 个可见逻辑 CPU，约 15 GiB 可见内存，k6、API 与依赖共享宿主机资源 |
| Docker | Docker CLI 和服务端均为 29.8.1，Docker Compose v5.5.1 |
| MySQL | 8.0.46，独立实例及 `gofeed_module0` Schema，`performance_schema` 已启用，全局时区 UTC |
| Redis | 7.4.11，独立实例、DB 1，沿用基准代码固定的 `v1:` 前缀，该实例专供本轮 |
| RabbitMQ | 3.13.7，API 连接独立实例，未启动业务 Worker，压测统计窗口内仅有读取流量 |
| API | 单进程，`GIN_MODE=release`，保留 Gin 请求日志，`TZ=UTC`，端口 28080 |
| 压测与 profiling | k6 v2.4.0，`go tool pprof` 可用，API 的 `127.0.0.1:36060/debug/pprof/` 已验证可访问，heap profile 已用 `go tool pprof -top` 解析 |
| 数据 | 1,000 个账号、10,000 个视频、20,000 条关注、100,000 条有效点赞。数据集含 200 个作者，Following 测试账号关注 50 个作者 |

隔离项目为 `gofeed-read-module0`，使用新建网络及专属卷。MySQL、Redis、RabbitMQ 的宿主机端口分别是 33317、36389、35682，仅映射到回环地址。MySQL 和 Redis 命令行客户端在各自的容器内执行。

小档 CSV 来自 `data/baseline/small/`，生成种子为 `20261116`，参考时间为 `2026-11-16T00:00:00Z`，manifest SHA-256 为 `665080d2749bbe2a7be9ad2a4977c561a88fd0ce0c50cd7d3b4ab7d0871759cb`。导入前按 manifest 核对了文件大小、SHA-256 与行数，确认独立 Schema 的四张业务表为空。事务导入保留原始 ID 和 UTC 时间，为点赞行补齐与 `created_at` 相同的 `updated_at`，随后查询核对各表数量和 ID 范围。

开发前在 `main` 发现本地保留的 `tools/k6/`、`tools/bench/`、`tools/baseline/`、独立 Compose 配置、审计文档和压缩包。这些文件均为未跟踪文件，已原样保留。负载脚本沿用 `tools/k6/feed_latest.js` 的模型与汇总，在实验目录中适配其余场景。复用了 `docker-compose.baseline.yml` 的依赖服务配置，启动时指定新项目及端口。已检查 `tools/bench/`，其中既有写入压测运行器绑定旧实验环境，本轮采用独立库执行只读场景。

### 请求与计量

正式测量中各场景顺序运行一轮。每轮固定 10 VU，配置时长 20 秒，使用 k6 `constant-vus`，不加迭代间隔。每次迭代发送一个 POST 请求，每轮先发送一次同参数预检请求并等待一秒，预检与登录均在统计窗口之外。每页 10 条，校验 HTTP 200、JSON、10 个结果和 `has_more=true`，Popularity 额外校验返回的游标字段。

| 场景 | 路径 | 请求参数及缓存状态 |
| --- | --- | --- |
| Latest | `/feed/listLatest` | 匿名首屏，`latest_time=0`，预检触发全局时间线重建并预热视频实体 |
| Following | `/feed/listByFollowing` | 单个已登录账号首屏，`latest_time=0`，预检预热关注页，统计窗口内仍读取当前点赞状态 |
| LikesCount | `/feed/listLikesCount` | 匿名首屏，无点赞数或 ID 游标 |
| Popularity（MySQL 回退） | `/feed/listByPopularity` | 匿名首屏，`as_of=0`、`offset=0`，隔离 Redis 无热榜窗口数据 |
| Popularity（Redis 热榜） | `/feed/listByPopularity` | 匿名首屏，`as_of` 固定在测试分钟、`offset=0`，从测试数据取点赞数最高的 1,000 个视频，按其点赞数写入单个合成分钟 ZSET，由预检生成榜单快照 |

每轮 SQL 数量取专用 MySQL 实例的 `Com_select` 前后差，扣除采样客户端建立第二次连接产生的 1 条查询，再除以 k6 HTTP 请求总数。空载各采样一次和连续发 7 次匿名 LikesCount 的对照中，原始增量分别为 1 和 8，`Com_stmt_execute` 增量分别为 0 和 7，用于交叉核对参数化查询。首轮按 Schema 筛选的 Performance Schema 摘要未捕获实测的参数化查询，修正后换用该实例中空的 Redis DB 1，重跑全部场景。P95/P99 来自 k6 的 `http_req_duration`，吞吐使用 `http_reqs.rate`。请求错误率按每个请求统计，涵盖 HTTP、超时、JSON 和场景校验。

### 实测结果

| 场景 | 请求数 | SQL 总数 | SQL / 请求 | P95 ms | P99 ms | RPS | HTTP 错误率 | 请求错误率 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Latest | 59,962 | 0 | 0.000 | 4.462 | 5.550 | 3,195.64 | 0.000% | 0.000% |
| Following | 22,779 | 22,779 | 1.000 | 13.188 | 15.854 | 1,216.31 | 0.000% | 0.000% |
| LikesCount | 15,900 | 15,900 | 1.000 | 16.940 | 20.709 | 847.76 | 0.000% | 0.000% |
| Popularity（MySQL 回退） | 11,124 | 11,124 | 1.000 | 23.318 | 26.728 | 555.75 | 0.000% | 0.000% |
| Popularity（Redis 热榜） | 18,681 | 18,681 | 1.000 | 18.291 | 28.443 | 996.72 | 0.000% | 0.000% |

正式测量共 128,446 次请求。各轮 k6 实际统计窗口为 18.728 至 20.016 秒，吞吐采用 k6 汇总的原始速率。

本轮覆盖首屏、固定并发和静态数据。Following 仅使用一个测试账号，Popularity 热榜使用合成分钟桶。结果可在相同配置下用于后续对比，深分页、多账号混合、写入并发、冷缓存和 Redis 故障场景均未纳入本轮压测。既有独立评估及历史基线仅用于参考，本表使用本轮采集的数据。

本地保留 [正式测量原始记录](../data/feed-read-reliability/module0-c628b0f/measured/)，包含各场景的 k6 JSON、预检响应、MySQL 计数快照、适配脚本和配置。启动覆盖配置、导入 SQL、首轮数据和计数校准记录也保存在 [本轮实验目录](../data/feed-read-reliability/module0-c628b0f/)。

## 模块 1 · 排序契约与双字段游标

### 接口与排序

| 场景 | 顺序 | 翻页契约 |
| --- | --- | --- |
| Latest、Following | `create_time DESC, id DESC` | 首屏省略 `latest_time` 与 `id_before`。下一页将响应的 `next_time` 和 `next_id_before` 原样传回，两项必成对。游标时间是 Unix 毫秒，视频卡片的 `create_time` 仍为 Unix 秒。 |
| LikesCount | `likes_count DESC, id DESC` | 保留 `likes_count_before` 和 `id_before`，从本页最后一条视频生成下一页边界。 |
| Popularity | MySQL 使用 `popularity DESC, create_time DESC, id DESC`，Redis 沿用快照排名 | MySQL 延续三字段游标，Redis 延续 `as_of` 和原始榜单位置 `offset`。 |

按时间排序的 MySQL 查询使用 `create_time < ? OR (create_time = ? AND id < ?)`。四个场景都读取最多一条额外的有效结果来判断是否还有下一页。Redis 热榜遇到无效或已删除的候选时继续读取后续排名，下一页位置取最后一个已返回视频之后的榜单位置，探测用的下一条有效视频留给下一页。

### 调用链与失败处理

- 前端 `useVideoFeed` 和 `FeedView` 保存成对的翻页字段，经 `api/feed.ts` 发送给 Feed Handler。Handler 校验双字段后，Service 调用缓存或 Repository，再用 `buildFeedVideos` 组装卡片及点赞状态。少传字段、ID 为零或时间越界时直接返回 HTTP 400。模块 0 历史压测脚本的首屏单字段 `latest_time: 0` 需要在后续对照实验中按被测版本使用对应契约。
- Latest 旧索引以毫秒为 Redis score、普通字符串 ID 为 member。当读取窗口遇到同一毫秒的多个候选，或者同分边界不能确认时，从 MySQL 获取整页以保证数字 ID 排序。其余页面可从 Redis 取候选，并按末条的时间与 ID 从 MySQL 补齐冷数据。Redis 查询失败或实体缺失也会触发完整 MySQL 查询。新版本时间线和写入格式安排在模块 5。
- Following 暂时沿用整页缓存，缓存键包含完整的毫秒和 ID，避免命中旧秒级格式。关注关系变化仍通过原有模式失效缓存，命中页面后仍会批查当前点赞状态。Inbox 和 Author Outbox 的混合候选读取安排在模块 2。
- `FollowingMember` 继续使用 13 位十进制毫秒、冒号和 20 位十进制 VideoID，现支持解析。对于 score 为 0 的候选 ZSET，Redis Client 使用 go-redis Pipeline 批量执行逆序 Lex 范围查询。首屏上界为 `+`，后续页以 `(` 加完整游标 Member 作为排他上界。命令错误传回调用方。

### 验证

- `go test -count=1 -v ./internal/feed ./internal/middleware/redis ./internal/worker` 已通过。Feed 使用隔离 MySQL `gofeed_read_test` 和 miniredis，覆盖同毫秒三页、翻页间插入更新视频、缓存热页、冷热拼接、精确末页、无效游标、Redis Lex 排他与 Pipeline 错误，以及热榜删除项跳过。
- 本轮未配置 Worker 集成套件的独立连接变量，`TestLikeCommandRelay`、`TestFollowingPipeline`、`TestLikeCommandPipeline`、`TestLikeCommandBatchWorker` 已跳过。
- `go vet ./internal/feed ./internal/middleware/redis ./internal/worker`、前端 `npm run build` 和 `git diff --check` 已通过。

## 模块 2 · Following 混合候选读取

### 调用链

1. `POST /feed/listByFollowing` 经 JWT 校验后进入 Feed Handler。Handler 解析严格排他的毫秒与 ID 游标，再调用 `FeedService.ListByFollowing`。
2. `SocialRepository.ListFollowedAuthorIDs` 通过 `idx_social_follower_vlogger` 只取当前关注的作者 ID。Pipeline 逆序读取用户 Inbox 和这些作者的全部 Outbox，每个来源最多读取 `limit+1` 条。作者当前为 Push 时，过去写入 Outbox 的 Pull 视频仍可参加归并。
3. 最大堆比较各来源的首条 Member，每次取出全局最新的一条并推进该来源。固定宽度字符串的比较顺序与毫秒、数字 ID 的降序一致。归并时跳过无法解析的 Member，按 VideoID 去重，保留至多 `limit+1` 个候选。堆只跟踪各来源的当前位置。
4. 对这批 ID 查询 MySQL 的 `id` 和 `create_time`，同时核对当前关注关系。已删除、已取关和索引时间与数据库不符的候选在组装前被过滤，命中旧实体缓存也无法绕过这次检查。随后用 `GetVideoByIDs` 获取详情，维持候选顺序。
5. 过滤或组装后不足 `limit+1` 条时，从原请求游标查询 MySQL 前缀，按相同顺序与有效的 Redis 结果归并。这样可补入位于 Redis 候选之间的历史项，同一视频只保留一次，已有的 Redis 候选保留。Redis Pipeline 失败或实例未配置时，直接走完整 MySQL Following 查询。MySQL 失败会向上传递，由当前 Handler 返回 HTTP 500。
6. `buildTimePage` 去掉探测用的额外结果并生成下一页游标。`buildFeedVideos` 调用 `BatchGetLiked` 查询当前用户的点赞状态。旧 Following 整页缓存、缓存命中后的专用点赞刷新函数和关注写入时的整页缓存失效逻辑已移除。

新关注作者的历史内容以现存候选和按需 MySQL 补齐能覆盖的范围为准。当 Redis 已提供足量候选时，排序区间内未被索引的历史项可能跨过分页边界。关注表结构沿用现有字段，关注写入沿用现有分发流程。

### 验证

- `TestFollowingHybridMySQL` 使用隔离 MySQL 和 miniredis，验证 Inbox 与两个作者 Outbox 的归并、Push 作者的历史 Pull 视频、重复 ID、同毫秒连续三页、无效 Member、旧实体缓存里的已删除视频、取关、补齐时保留有效 Redis 候选、Pipeline 失败和 Redis 不可用回源。该事务测试预置实体缓存，真实服务端到端另覆盖实体冷读。
- 使用独立 MySQL、Redis 和 RabbitMQ 运行 `TestFollowingPipeline` 与 `TestLikeCommandPipeline`，两套用例均通过，覆盖分发侧写入和通过 HTTP 读取取消点赞后的状态。
- Feed、Social、HTTP 和 Redis 相关包的 `go test -race` 通过。上述包及 Worker 的 `go vet` 和 Git diff 格式检查通过。

## 模块 3 · 批量视频组装与缓存回填

### 调用链与行为

1. Latest 的 Redis 时间线、Following 的已核验候选、Popularity 的 Redis 快照都使用 `GetVideoByIDs`。已有 MySQL 分页查询返回完整视频行时，直接交给 `buildFeedVideos`，继续由它组装卡片和批查点赞状态。
2. 视频实体先查 L1，仍使用原有 5 秒实体 TTL。把所有未命中的不同 ID 合并为一次 Redis MGET。Redis 保留原 `video:entity:<id>` 键和 60 秒 TTL。无法解析、ID 与 Key 不符或 Redis 不可用时，相关 ID 进入回源批次。
3. 将缺失 ID 排序后构造进程内 Singleflight 键，使不同输入顺序的同一组 ID 可共享查询。批次执行时重查 L1，仍缺失的 ID 通过一条 MySQL `WHERE id IN (...)` 获取。每个批次共享一次回源执行，等待者可随自身 Context 取消，SQL 使用发起该批次的请求 Context。
4. 用 ID 映射按每个请求的原始顺序返回，保留输入中的重复 ID。MySQL 成功但没有对应行时跳过该 ID，查询报错时直接返回错误。查到的行同步回填 L1，并用一次 Pipeline 回填 Redis。Redis 回填错误记录日志，已读取的数据库结果仍可返回。
5. Popularity 快照保留批量查存在 ID 的窄查询，避免已经删除的视频凭旧实体缓存进入榜单。冷读时另有一条实体批量 SQL。快照本身不可读时沿用 MySQL 排名回退，快照组装阶段的数据库错误向上传递。

### 验证

- `TestVideoLookupBatchMySQL` 在独立 MySQL 与 miniredis 上核对了混合命中、缓存内容 ID 校验、输入顺序和重复 ID、缺失视频跳过、Redis 读写失败以及 SQL 错误传播。一轮混合命中观测到 1 次 MGET、1 条带三个缺失 ID 的 SQL、1 次含两条 SET 的回填 Pipeline。完整 L1 命中无远端查询，完整 Redis 命中无 MySQL 实体查询。
- `TestVideoLookupSingleflightMySQL` 验证两个顺序不同的并发请求只执行一次 SQL，各自保留原始顺序，另一个等待者取消后可独立退出。
- Feed、Redis 和 Worker 包的定向 `go test -race` 及 `go vet` 通过。常规竞态测试中的四个外部服务 Worker 集成测试按配置跳过。
- 在真实隔离 MySQL、Redis 和 RabbitMQ 上单独复跑 `TestLikeCommandPipeline` 并通过，覆盖登录后的 Following HTTP 请求和实时点赞状态。分页集成测试另验证了 Popularity 直接使用缓存实体，并跳过已有实体缓存的删除项。
