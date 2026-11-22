# GoFeed 原项目基线测试准备报告

检查日期：2026-11-16。

检查基准：提交 `7c1c27d179bf22246931d249267a57df8b5cca4b`，检查时工作区干净。本次仅做静态代码检查，未启动服务、运行测试或压测，未修改业务代码、数据库或执行 Git 写操作。以下启动命令仅供后续使用。

## 一、服务启动方式

[`docker-compose.yml`](../docker-compose.yml) 定义了 6 个服务：

| 服务 | 镜像或构建目标 | 宿主机端口 |
|---|---|---|
| MySQL | `mysql:8.0` | `3307 → 3306` |
| Redis | `redis:7-alpine` | `6379 → 6379` |
| RabbitMQ | `rabbitmq:3-management` | `5672`；管理端 `15672` |
| backend | backend Dockerfile 的 `api` 目标 | `8080` |
| worker | backend Dockerfile 的 `worker` 目标 | 无映射 |
| frontend | 前端构建产物，由 Nginx 托管 | `5173 → 80` |

完整启动方式：

```bash
docker compose up -d --build
```

API、Worker 均等待三项依赖健康后启动；前端仅声明依赖 backend。MySQL、Redis、RabbitMQ 和上传目录均使用命名卷，重启后会保留状态。Compose 未设置 CPU、内存配额。

后端入口及本地运行方式：

| 进程 | 入口 | 在 `backend/` 下执行 |
|---|---|---|
| API | [cmd/main.go](../backend/cmd/main.go) | `go run ./cmd` |
| Worker | [cmd/worker/main.go](../backend/cmd/worker/main.go) | `go run ./cmd/worker` |

两者通过 `CONFIG_PATH` 指定配置，默认读取 `configs/config.yaml`。

[`start.sh`](../start.sh) 默认启动本地 API、Worker、前端、本地 Redis，并通过 Compose 启动 RabbitMQ。需要注意：

- **脚本不启动 MySQL**，但默认选用 MySQL 端口为 `3307` 的 `config.compose-local.yaml`。
- 默认 Redis 启动命令未设置密码，配置文件却要求密码；检测已有 Redis 时也未传认证参数，可能造成检测或连接失败。
- Go 进程在 `backend/` 下调用 `godotenv.Load()`，不会自动读取仓库根目录的 `.env`；Compose 则会读取根目录 `.env`。
- **API 启动会调用 `AutoMigrate`，可能修改表结构**。Worker 入口不执行迁移。
- `/healthz` 只返回进程存活状态，不检查依赖或消费积压。

## 二、关键 API 及源码位置

路由注册统一位于 [router.go](../backend/internal/http/router.go)。下列业务接口均为 **POST**；直接访问 API 时无 `/api` 前缀，通过前端 Nginx 访问时使用 `/api/...`。

| 业务 | API | 鉴权及主要参数 |
|---|---|---|
| 最新 Feed | `/feed/listLatest` | 可匿名；`limit`、`latest_time` |
| 点赞数排序 Feed | `/feed/listLikesCount` | 可匿名；`limit`、`likes_count_before`、`id_before` |
| 热度 Feed | `/feed/listByPopularity` | 可匿名；`limit`、`as_of`、`offset`，另有数据库回退游标 |
| 标签 Feed | `/feed/listByTag` | 可匿名；`tag_name`、`limit` |
| 关注 Feed | `/feed/listByFollowing` | 必须登录；`limit`、`latest_time` |
| 发布视频 | `/video/publish` | 必须登录；`title`、`description`、`play_url`、`cover_url` |
| 上传视频、封面 | `/video/uploadVideo`、`/video/uploadCover` | 必须登录；文件上传 |
| 分片上传 | `/video/chunk/init`、`/video/chunk/upload`、`/video/chunk/status`、`/video/chunk/complete` | 必须登录 |
| 关注、取关 | `/social/follow`、`/social/unfollow` | 必须登录；`vlogger_id` |
| 关注关系查询 | `/social/getAllFollowers`、`/social/getAllVloggers`、`/social/getCounts` | 必须登录 |
| 点赞、取消点赞 | `/like/like`、`/like/unlike` | 必须登录；`video_id` |
| 点赞查询 | `/like/isLiked`、`/like/listMyLikedVideos` | 必须登录 |

鉴权使用 `Authorization: Bearer <token>`。匿名 Feed 使用软鉴权：不携带 Token 可以访问，携带无效 Token 会被拒绝。

对应调用链：

| 业务 | Handler | Service | Repository |
|---|---|---|---|
| Feed | [feed/handler.go](../backend/internal/feed/handler.go) | [feed/service.go](../backend/internal/feed/service.go) | [feed/repo.go](../backend/internal/feed/repo.go) |
| 视频发布 | [video_handler.go](../backend/internal/video/video_handler.go)：`PublishVideo` | [video_service.go](../backend/internal/video/video_service.go)：`Publish` | [video_repo.go](../backend/internal/video/video_repo.go)；发布事务直接使用该 Repository 持有的 GORM DB |
| 关注 | [social/handler.go](../backend/internal/social/handler.go) | [social/service.go](../backend/internal/social/service.go) | [social/repo.go](../backend/internal/social/repo.go) |
| 点赞 | [like_handler.go](../backend/internal/video/like_handler.go) | [like_service.go](../backend/internal/video/like_service.go) | [like_repo.go](../backend/internal/video/like_repo.go)，并关联 VideoRepository |
| 分片上传 | [chunk_handler.go](../backend/internal/video/chunk_handler.go) | 未单独拆分 | 直接操作 Redis 和上传文件 |

需要纳入基线口径的现有行为：

- 最新 Feed 的时间游标使用**毫秒**，关注 Feed 使用**秒**；`limit` 有效范围为 1～50，否则重置为 10。
- 发布事务写入视频、Outbox 消息及标签关联。Outbox 轮询、时间线消费和通知消费实际运行在 **API 进程**中。
- 独立 Worker 运行 Social、Like、Comment、Popularity 消费者，常规消费 Channel 的 QoS 为 50。
- 关注先同步写库，再失效缓存并发送 MQ 通知。
- 点赞正常路径通过 MQ 异步更新；发布消息失败时存在直接写库、更新 Redis 的回退。**HTTP 成功响应与最终落库完成需要分别统计。**
- Redis 正常时，点赞与取消点赞共享每账号 30 次/分钟限制，关注与取关共享每账号 20 次/分钟限制。Redis 不可用或限流操作出错时会放行，可能改变测试结果。

## 三、数据库、缓存和消息队列配置

配置文件：

- [config.yaml](../backend/configs/config.yaml)：本地默认配置。
- [config.compose-local.yaml](../backend/configs/config.compose-local.yaml)：本地进程连接 Compose 依赖。
- [config.docker.yaml](../backend/configs/config.docker.yaml)：容器内部连接。
- [loadconfig.go](../backend/internal/config/loadconfig.go)：加载配置及环境变量覆盖。

| 项目 | 本地默认 | Compose 内部 | 宿主机连接 Compose |
|---|---|---|---|
| MySQL | `localhost:3306` | `mysql:3306` | `localhost:3307` |
| Redis | `localhost:6379`，DB 0 | `redis:6379`，DB 0 | `localhost:6379` |
| RabbitMQ | `localhost:5672` | `rabbitmq:5672` | `localhost:5672` |

- MySQL 默认库名 `feedsystem`、用户 `root`；DSN 使用 `utf8mb4`、`parseTime=True`、`loc=Local`。Compose 设置 `Asia/Shanghai` 和 `utf8mb4_0900_ai_ci`。代码未显式设置数据库连接池上限、空闲连接数或连接生命周期，见 [db.go](../backend/internal/db/db.go)。
- Redis 启用密码认证；Compose 启用 AOF。业务 Key 默认前缀为 `v1:`，限流另用 `feedsystem:ratelimit:`。未发现 Compose 显式设置内存上限或淘汰策略。
- 最新 Feed 使用 Redis ZSET 时间线，保留约 1000 条热点记录，冷数据回查 MySQL；实体本地缓存回填 TTL 为 5 秒，Redis 实体缓存为 1 小时。关注 Feed 缓存 TTL 为 24 小时，视频详情缓存为 5 分钟。
- RabbitMQ 使用默认 `/` vhost、持久化队列及持久化消息，包含点赞、关注、评论、热度、时间线和通知队列，代码提供死信处理。
- 密码、库名、连接地址等支持环境变量覆盖；配置优先级为环境变量覆盖 YAML。本报告不展开凭据值。
- API 在 Redis、MQ 初始化失败时可以降级启动；Worker 对 MySQL、MQ 连接进行重试。因此不能仅凭 API 健康检查成功认定基线依赖完整。

## 四、现有测试情况

静态定位到 **3 个 Go 测试文件，共 10 个测试函数**：

| 文件 | 已覆盖内容 |
|---|---|
| [pprof_test.go](../backend/internal/observability/pprof_test.go) | pprof 路由、关闭状态及关闭操作，共 3 项 |
| [redis_test.go](../backend/internal/middleware/redis/redis_test.go) | 计数器首次设置 TTL、后续递增不延长窗口，共 1 项 |
| [chunk_handler_test.go](../backend/internal/video/chunk_handler_test.go) | 完整上传、断点续传、幂等、Hash 错误、分片不完整、状态查询，共 6 项 |

Redis 相关测试使用 miniredis；分片测试使用临时目录，不依赖真实 MySQL。**本次未执行，不能据此判断测试通过。**

其他现有资源：

- [test/postman.json](../test/postman.json)：包含账号、视频、关注、点赞、Feed 等手工请求及变量保存脚本。整套运行会修改业务数据。
- Postman 集合存在过时内容：包含当前未注册的 `/like/getLikesCount`，并把最新 Feed 游标描述为秒，而代码实际使用毫秒，需要先校准。
- [ci.yml](../.github/workflows/ci.yml)：后端配置 `go vet` 和 `go test -race -count=1 ./...`；前端执行安装和构建。前端未配置测试命令。
- 本地配置已启用 API `localhost:6060`、Worker `localhost:6061` 的 pprof；Docker 配置关闭 pprof，Compose 也未映射这些端口。
- 未发现 Feed、发布、关注、点赞核心调用链的自动化业务测试、数据库集成测试或 `Benchmark` 函数。

## 五、基线测试还缺少的工具与材料

以下为仓库内缺项，不代表宿主机一定未安装相应软件：

| 缺项 | 后续用途 |
|---|---|
| 可重复的压测脚本 | 仓库未发现 k6、JMeter、Locust、wrk、Vegeta 等场景脚本 |
| 批量数据生成与校验脚本 | 生成账号、视频、关注关系、点赞分布，并验证数量及关联关系 |
| 数据快照及恢复流程 | 保证不同轮次从一致的数据、缓存和队列状态开始 |
| Token 和测试账号池管理 | 避免登录、注册限流及单账号写入限流干扰目标场景 |
| 核心业务正确性断言 | 验证分页、重复操作、发布可见性及点赞最终一致性 |
| 指标采集与结果归档 | 汇总吞吐、延迟分位数、错误率、429、资源消耗、缓存状态和 MQ 积压 |
| 异步完成度检查 | 记录 Outbox 排空、消息消费完成及数据库计数收敛时间 |
| 固定环境清单 | 记录镜像版本、资源配置、时区、连接池默认值及依赖健康状态 |

目前已完成代码层面的准备检查。**尚未确定数据规模、读写比例、并发模型、测试时长及冷热缓存场景，也未开始压测。**
