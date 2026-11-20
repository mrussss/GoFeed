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

## 模块 1–3 读取链路专项审查

### 视频删除与接受的业务边界

受 JWT 保护的 `POST /video/delete` 接入已有的删除 Handler，沿用作者权限检查。MySQL 删除成功后，单次 DEL 清理 `v1:video:detail:id=<id>` 与 `v1:video:entity:<id>`，ZREM 从 `v1:feed:global_timeline` 移除十进制 ID 字符串。两个 Redis 操作各自记录错误，当前进程通过路由装配的回调清理 Feed L1。Redis 热榜快照仍按照原有 2 分钟 TTL 存续。

其他进程的 L1 视频实体保留最多 5 秒。Redis DEL 失败时，旧实体可在 Redis 的剩余 TTL 内被读取，临近 TTL 再命中时可能额外留下一个 L1 窗口。Global Timeline 的成员没有单独 TTL，ZREM 失败或延迟发布后可留到裁剪，实体缓存失效后的 MySQL 读取会跳过已删除行。与删除重叠、已读到旧值的请求仍可能完成旧结果或回填缓存，当前方案提供尽力失效。

Following 在现有 Redis 索引范围内读取，并按需通过 MySQL 补齐。候选足够时，缺失的历史索引可能跨过分页边界。Latest 和 Following 的排序元组由发布时间和 ID 固定。LikesCount 的点赞数会随并发写入变化，视频可在翻页期间移动。Popularity 通过 `as_of` 和榜单位置翻页，快照以原 TTL 存续，跨越快照重建或切换到 MySQL 排名时序列可变。L1 中点赞数接受既有 5 秒 TTL，用户的 `is_liked` 每页使用 `BatchGetLiked` 查询。

### 性能对照方法

本轮复用模块 0 的 k6 固定 VU 脚本，首屏请求按当前契约发送。API、k6、MySQL 8.0.46 和 Redis 7.4.11 共用同一宿主机，测试依赖使用专用 Docker 容器。新增独立 MySQL Schema `gofeed_read_review` 和独立 Redis DB 7，正确性对照使用独立 DB 8、9。合成数据含 30,000 个视频、10,000 个作者 ID、单个登录用户，每作者 Outbox 三条真实视频，Inbox 11 条真实视频。压测前逐步增加同一用户的关注边并核对行数。每轮启动新 API 进程，两次预检核对首屏 ID 并预热缓存，MySQL 与 Redis 在各轮之间持续运行。

Popularity 的 A/B API 从同一份删除修复源码构建。临时 B 仅绕过快照的存在性 SQL，正式分支保留 A。A/B 交替运行，每种配置各 3 轮，10 VU、配置 20 秒。所有轮次使用同一快照、同一 `as_of`、同样的十条结果和暖缓存。Following 从 50、500、5,000、10,000 作者逐级执行，1 VU 为主，并在代表规模复测 10 VU。所有场景页大小为 10。

P50/P95/P99、RPS 取 k6 原始汇总，再对各轮各自的指标取中位数。SQL 数量取专用实例 `Com_select` 前后差，扣除两条采样开销，通过 `Com_stmt_execute` 增量交叉核对。SQL 耗时使用 MySQL Performance Schema 的 prepared execute 计时差除以请求数。MySQL CPU 是每轮 Docker 采样中位数，再取轮间中位数，100% 表示一个逻辑核。Redis 命令数取 `commandstats`，候选条数由固定来源长度和执行的 Lex 命令数核对，Redis 出站字节使用 INFO 差值。Go 分配采用进程 `TotalAlloc` 差值，进程峰值 RSS 取 Linux `VmHWM`。原始 JSON、脚本和剖析样本保留在 [本地实验目录](../data/feed-read-reliability/read-review/)。

### Popularity 存在性 SQL 对照

| 方案 | 轮数 | P50 ms | P95 ms | P99 ms | RPS 中位数（范围） | 请求错误率 | SQL / 请求 | SQL 时间 ms / 请求 | MySQL CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A 保留核验 | 3 | 3.301 | 6.929 | 7.840 | 2,233（2,217–2,632） | 0% | 1.000 | 0.145 | 99.17% |
| B 临时去掉核验 | 3 | 1.736 | 2.497 | 2.941 | 5,201（5,072–5,261） | 0% | 0.000 | 0.000 | 0.60% |

三个 A 轮次均为每请求一条 SQL，三个 B 轮次的 SQL 增量为零。观察区间内两组都没有额外实体回源。单条查询的服务器用时中位数约 0.145 ms，但每次热榜请求都需经历 SQL 往返和客户端处理，在这个同机、单接口、10 VU 的暖缓存场景中形成可测瓶颈。B 的中位吞吐达到 A 的 2.33 倍，P95 降低约 64%。这些数据对应本地暖热榜场景。

### Following 关注规模对照

| 作者数 | VU | 轮数 | P50 ms | P95 ms | P99 ms | RPS | 请求错误率 | Lex 命令 / 请求 | 候选 / 请求 | SQL / 请求 | Redis KiB / 请求 | Go 分配 KiB / 请求 | 峰值 RSS MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 | 1 | 3 | 4.58 | 6.10 | 7.07 | 206.84 | 0.00% | 51 | 161 | 3.00 | 7.1 | 94.7 | 29.8 |
| 500 | 1 | 3 | 5.63 | 7.24 | 8.96 | 169.77 | 0.00% | 501 | 1,511 | 3.00 | 62.9 | 484.6 | 30.2 |
| 5,000 | 1 | 3 | 18.62 | 22.76 | 25.54 | 52.41 | 0.00% | 5,001 | 15,011 | 3.00 | 621.0 | 4,337.1 | 33.8 |
| 10,000 | 1 | 3 | 34.01 | 40.64 | 44.81 | 28.79 | 0.00% | 10,001 | 30,011 | 3.00 | 1,241.1 | 8,854.3 | 38.0 |
| 500 | 10 | 3 | 19.10 | 29.09 | 33.66 | 512.55 | 0.00% | 501 | 1,511 | 3.00 | 62.9 | 489.8 | 36.3 |
| 5,000 | 10 | 3 | 117.23 | 138.13 | 150.27 | 84.06 | 0.00% | 5,001 | 15,011 | 3.00 | 621.0 | 4,296.8 | 74.5 |
| 10,000 | 10 | 3 | 240.92 | 274.37 | 296.96 | 41.22 | 0.00% | 10,001 | 30,011 | 3.00 | 1,241.1 | 8,814.3 | 121.1 |

21 轮均完成，HTTP 和请求校验错误率均为零。表中 Redis KiB 是 Redis 服务端出站数据，候选数量按每个成功 Lex 命令响应的固定集合长度计算。峰值 RSS 是三轮中的最大值。500 到 10,000 作者，1 VU 的 P95 从 7.24 ms 升至 40.64 ms。10 VU 时则从 29.09 ms 升至 274.37 ms。10,000 作者在本轮时长和并发下可完成请求，实测尾延迟已经显著升高。

SQL 数量固定为每请求三条，10,000 作者、10 VU 的 MySQL 服务端 SQL 总时间中位数为 2.43 ms/请求，MySQL CPU 为 13.10%。同一条件下 Redis 每请求执行 10,001 条 Lex 命令，出站约 1.21 MiB，Go 每请求分配约 8.61 MiB。一次 Pipeline 承载这些命令，避免逐命令等待响应。随着关注规模增长，命令和响应条数仍成比例增加。

10,000 作者、1 VU 的独立诊断轮中，Go CPU 采样按累计调用栈归因，Redis Lex Pipeline 路径占 50.57%，堆归并占 0.38%。分配剖析中两者分别约占 78.10% 和 2.70%。Redis 服务端在该轮处理 3,950,395 条 Lex 命令，累计命令执行时间约 4.32 秒，命令失败数为 0。当前瓶颈主要在候选的 Redis 命令构造、读取和解码路径，堆归并所占比例小。分批 Pipeline 会改变单批峰值和往返次数，候选总量仍由关注规模决定。现有测量支持高关注规模存在真实成本，分批是否改善 P95 和峰值内存仍需对照。

### 删除正确性 A/B 与回归测试

两个 API 进程先预热同一视频，再由其中一个进程通过受保护的 HTTP 接口删除。A 和 B 均验证 MySQL 行已删除、Redis Detail 与 Entity 已失效、Latest 在两个进程均排除该视频、全局时间线只移除目标成员，以及热榜快照本身和其成员仍在。

| Popularity 场景 | A 保留核验 | B 临时去掉核验 |
| --- | --- | --- |
| 删除前 | 返回视频 | 返回视频 |
| 正常删除后，本进程 | 排除 | 排除 |
| 正常删除后，另一进程仍有 L1，约 0.3 秒 | 排除 | 短暂返回 |
| 约 5.15 秒后，另一进程 L1 已过期 | 排除 | 回源后排除 |
| 恢复旧 Redis Entity 模拟 DEL 未生效 | 排除 | 再次返回 |

另外，`TestVideoDeleteFeedCachesMySQL` 通过 Redis 写失败注入直接验证两个失效日志、当前进程 L1 清理，以及旧实体在 Redis 和 L1 TTL 内的行为。虽已物理删除，Latest 在 Redis DEL 和 ZREM 同时失败时仍可命中旧实体，待 TTL 到期后恢复。上述双进程检查使用真实 Redis。真实 MySQL、Redis 和 RabbitMQ 的 `TestFollowingPipeline` 通过。测试复现了删除前已写入 Outbox、删除后才送达的发布事件会重加 Global Timeline 成员。该成员回源时取不到实体，测试验证了此结果。现有发布链路继续允许这类索引残留。

`TestVideoLookupLeaderCancellationMySQL` 使用独立 MySQL 会话持有测试锁，通过 channel 确认 B 已加入相同 Singleflight，并通过 MySQL processlist 确认共享 SQL 正在等待。取消 A 后，唯一一条 SQL 和两个请求均返回 `context.Canceled`，B 自身的 Context 仍有效，等待者正常退出。Feed、Redis、Video、HTTP 的定向竞态测试通过。该轮 Video 包的 `TestLikeCommandMySQL` 与 `TestPublishFollowingMySQL` 未配置各自的集成测试连接变量，按现有条件跳过。定向 `go vet` 和 `git diff --check` 通过。

### 待决定事项

- Popularity 窄核验 SQL 的吞吐代价在本轮明确可测。建议目前保留，因为 B 在实体缓存失效成功时仍有跨进程 L1 窗口，DEL 失败时还可从旧 Redis Entity 重新进入榜单。正式实现保留 A，最终取舍由审查决定。
- Following 在 5,000 到 10,000 作者时已出现显著的候选读取和分配开销。在 10,000 作者、10 VU 的 3 轮内无请求错误，P95 中位数为 274.37 ms。先维持现有 Pipeline，是否进一步对照分批取决于目标延迟和内存预算。

## 模块 4 · MySQL 回源保护

### 三档行为

| 行为 | 触发条件 | 请求结果 |
| --- | --- | --- |
| Normal | Redis 正常，候选索引或实体缓存可用 | 优先读候选索引、L1 和 Redis。能仅靠缓存满足的读取跳过 SQL 额度。业务必需的关注、存在性和点赞状态查询仍共享额度。 |
| Degraded | Redis 不可用、超时，或索引与缓存不足 | 继续使用现有 MySQL 回源和混合补齐。每条 SQL 持有一个执行额度，查询结束后立即释放。 |
| Shed | 执行额度占满，等待队列已满或等待超时 | 返回 `ErrFeedReadOverloaded`，Handler 映射为 HTTP 503，响应仅包含错误字段。 |

### 调用链与预算

`Feed Handler → Feed Service → 缓存及候选选择 → readFeedMySQL → 获取共享额度 → 带查询超时的 Repository → 释放额度 → Feed 组装`。

每进程最多 8 条 Feed SQL 同时执行，额外最多 8 条等待，等待上限 20 ms。拿到额度后，由请求 Context 派生 250 ms 的查询超时。请求取消或查询结束后释放相应的执行与等待额度。查询错误继续沿原有错误路径返回。一个页面需要多条 SQL 时逐条取得额度，Redis 操作、缓存回填和响应组装在额度之外。已有的 Singleflight 仍以发起者的 Context 执行共享查询。

预算覆盖实体批量冷读、Latest 重建及冷热回源、Following 的关系查询与候选核验和补齐、LikesCount、Popularity 排名与存在性核验、Tag、已登录用户的点赞状态。JWT 中间件在 Feed Service 之前执行，认证自身的 MySQL 兜底、其他路由和 Worker 使用各自的现有资源边界。额度为进程内预算，多个 API 进程会分别持有自己的预算。

Latest Redis 候选查询和重建回填调用使用 50 ms 子 Context。运行时 Redis Client 启用 go-redis 的 Context 期限，使已有的 Redis 操作期限在套接字读写阶段生效。该设置也覆盖共享客户端上已有的操作期限。继续沿用现有连接池与配置文件。此前专项压测结果对应启用本模块前的行为，故障下的完整性能对照安排在模块 6。

### 验证

- `TestFeedBackpressureMySQL` 在独立 MySQL 和可关闭、可重启的 miniredis 上验证正常、故障与恢复。故障时暖 L1 实体可用，MySQL 返回完整 Feed。持有 8 个执行额度时，覆盖 Latest、Following、LikesCount、Popularity 和 Tag 的 25 个额外请求全部得到 HTTP 503，未进入 SQL。取消在途请求后，新请求获得释放的额度。Redis 恢复后，已预热的 Latest 恢复为零 SQL。
- `TestFeedSQLDeadlineReleasesSlot` 验证查询期限与额度回收，`TestRuntimeClientHonorsRedisDeadline` 验证运行时客户端从不响应的 Redis 命令退出。现有 Feed 分页、删除一致性和 Singleflight Leader 取消用例一并通过。
- 全量 `go test -count=1 ./...`、定向 `go test -race`、`go vet` 和 Git diff 检查通过。并发额度测试在 race 模式另重复五轮，均通过。Worker 的 `TestLikeCommandRelay`、`TestLikeCommandPipeline`、`TestLikeCommandBatchWorker`、`TestFollowingPipeline` 在本轮未设置各自连接变量，按条件跳过。实际 Redis 停止与恢复的压测留在模块 6。

## 模块 5 · Latest 与榜单分页

### 四条读取链路

| 场景 | 候选与排序 | 组装、回源与分页 |
| --- | --- | --- |
| Latest | `feed:global_timeline:v2`，score 为 0，Member 沿用 `FollowingMember` 的 13 位毫秒和 20 位 ID，从双字段游标严格向后逆序 Lex 读取 | Redis 候选通过 `GetVideoByIDs` 组装。候选数量不足时，从原请求游标查询 MySQL，并按相同顺序归并、去重。Redis 读取失败、Member 无效或实体和索引不匹配时，使用完整 MySQL 页面。空时间线首屏可从 MySQL 重建最近 1,000 条。 |
| Following | Inbox 和所有已关注作者的 Outbox，保持既有 Pipeline、K 路归并和双字段游标 | 核验关注关系与候选后调用 `GetVideoByIDs`，按需合并 MySQL 历史。 |
| LikesCount | MySQL 按 `likes_count DESC, id DESC`，查询 `limit+1` 条 | 使用查询已返回的视频行组装，游标取本页末条的点赞数和 ID。恰好填满的末页返回 `has_more=false`。 |
| Popularity | Redis 使用最近 60 个分钟桶的 ZUNIONSTORE 快照。按 Redis 热度分数降序，同分按固定宽度数字 ID 降序。MySQL 保留 `popularity DESC, create_time DESC, id DESC` 及三字段游标。 | 快照候选经现有存在性 SQL 核验，再用 `GetVideoByIDs`。删除项或无效 Member 之后继续寻找有效结果，按最后一条已返回记录的原始排名位置设置 `next_offset`。快照继续支持 `as_of`、`offset` 和 2 分钟 TTL。 |

四路继续使用 `buildFeedVideos` 组装卡片并批查当前点赞状态。MySQL 已返回完整视频行时直接使用该结果。

### Redis 键与切换边界

Latest 的 Worker、API 重建及删除操作共用新版 Key、Member 编码和 1,000 条上限。删除使用 MySQL 读出的发布时间与 ID 生成精确 Member。无发布时间的旧行从关联发布事件取原时间，保持旧数据的删除清理。延迟发布重新加入无实体候选的边界沿用专项审查结论，读取遇到缺失实体时回源。

Popularity 的新分钟键为 `hot:video:1m:v2:<UTC分钟>`，快照键为 `hot:video:merge:1m:v2:<UTC分钟>`。两种排序索引使用独立版本，旧 Redis 键原样保留，新读写走新版。API 和 Worker 需使用一致版本。新版 Latest 可从 MySQL 重建，新版热榜从当前热度写入开始积累，缺少新版快照数据时首屏走原有 MySQL 回退。跨版本的旧热榜游标可通过刷新首屏重新开始。模块 6 预置热榜时使用新版 Key 和 20 位 ID。

### 验证

- `TestReadPaginationMySQL` 验证旧时间线保持原样、从空新版 Key 重建、同毫秒三页连续读取、暖缓存同毫秒翻页无需 SQL、稀疏 Redis 时间线补进更靠前的 MySQL 视频、LikesCount 同分精确末页与 Popularity 三字段回退。快照测试覆盖同分数字 ID 10 和 9、无效及已删除候选、原始位置偏移和 L2 实体组装。
- `TestPopularityCacheWriter` 验证新版分钟桶、固定宽度 Member 和 2 小时桶 TTL。`TestVideoDeleteFeedCachesMySQL` 验证新版时间线精确删除及缓存失效失败边界。
- 在独立 MySQL、Redis、RabbitMQ 及各测试专用 vhost 上运行 `go test -count=1 -p 1 -v ./...`，全部通过，含真实 HTTP 发布到 Worker 写新版时间线、旧数据的删除清理、Following 分发与点赞回归。定向 Feed 与 Redis `go test -race`、全量 `go vet ./...`、前端 `npm run build` 和 Git diff 检查通过。
