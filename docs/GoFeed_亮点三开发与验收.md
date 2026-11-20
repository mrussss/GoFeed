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
