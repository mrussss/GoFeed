# GoFeed 实验复核与复现材料

验收解释、指标和简历取舍见[最终报告](../GoFeed_三亮点最终简历证据验收.md)。

## 本次可复现的实验

后端基准为 `33e3cfa8a40e9415f24efd28f052964b1be7fd93`。本目录的逐轮 CSV 取自同一后端与本次新增 Go 测试。实验使用 Docker Desktop 29.8.1、MySQL 8.0.46、Redis 7.4.11、RabbitMQ 3.13.7 和 Go 1.22.12，运行于 WSL2 Linux，可见 32 个逻辑核（i9-14900HX）及约 15.5 GiB 内存。专用容器以镜像默认设置启动，未设单独 CPU 或内存配额。相比历史 k6，本次 Go 本地调用减少了客户端和网络层变量，时延和速率按各自口径阅读。测试源码、结果和已选历史材料的完整性可从仓库根目录执行 `sha256sum -c docs/perf-evidence/SHA256SUMS` 核对。

1. 准备回环地址上的独立 MySQL、Redis、RabbitMQ 实例。镜像版本与上文一致。MySQL 预建空的 `gofeed_read_test` 库，账户需能在该实例创建和删除临时 Schema。RabbitMQ 为 Hybrid 测试创建独立 `gofeed-following-test-cost` vhost，仅向实验账户授权。已有 Fanout 集成测试另用 `gofeed-following-test-evidence`。Redis 使用空实例的 DB 15。
2. 在本机设置 `FEED_READ_TEST_DSN`（含 `parseTime=true`）、`LIKE_COMMAND_TEST_DSN`（同一隔离 DSN）、`C_TEST_REDIS_ADDR` 和 `FOLLOWING_PHASE_B_AMQP_URL`（对应当前测试的 vhost）。真实凭据只从本机环境提供。
3. 在仓库根目录使用以下命令。各性能套件默认关闭。两个套件串行执行，严格分页断言独立运行。计量轮次和前置失败都应登记，失败后可用 Go 的分段 `-run` 过滤单个子测试。

```bash
cd backend
GOFEED_HYBRID_COST=1 go test -count=1 -v ./internal/worker -run '^TestFollowingCostEvidence$' | tee /tmp/gofeed-hybrid.log
GOFEED_FOLLOWING_SCALE=1 go test -count=1 -v ./internal/feed -run '^TestFollowingScaleEvidence$' | tee /tmp/gofeed-scale.log
GOFEED_CHECK_INDEX_COVERAGE=1 go test -count=1 -v ./internal/feed -run '^TestFollowingFullIndexPageCoverageMySQL$'
go test -count=1 -v ./internal/feed -run '^TestFollowingHybridMySQL$'
# 将 FOLLOWING_PHASE_B_AMQP_URL 指向独立的 gofeed-following-test-evidence 后再跑已有用例。
go test -count=1 -v ./internal/worker -run '^TestFollowingPipeline$'
```

严格断言在当前实现下会失败，同时输出 API 游标链与独立 MySQL 排序结果。Hybrid 用种子 `20261116` 分配粉丝和生成操作表，规模测试用确定性的 ID、时间与关注边。同一轮计量失败后保留记录，续跑时先在专用环境清理该轮数据。规模测试使用固定测试 ID，专用 MySQL 库应为空。运行新的回归前可另建 vhost，避免同一队列里遗留消息。

可用 [CSV 提取器](workloads/extract-rounds.py) 将两个成功的 Go 日志转成逐轮数据。在仓库根目录运行。

```bash
python3 docs/perf-evidence/workloads/extract-rounds.py hybrid /tmp/gofeed-hybrid.log /tmp/hybrid-rounds.csv
python3 docs/perf-evidence/workloads/extract-rounds.py scale /tmp/gofeed-scale.log /tmp/following-scale-rounds.csv
```

本次的[尝试台账](measured/hybrid-attempts.csv)、[Hybrid 逐轮结果](measured/hybrid-rounds.csv)、[关注规模逐轮结果](measured/following-scale-rounds.csv)与[严格分页断言原始输出](measured/full-index-coverage.txt)保留独立记录。

## 历史材料来源

| 证据 | 原始位置与版本 | 这里保留的材料 |
| --- | --- | --- |
| Following Phase B 分发成本 | `7e04db9b7bd0f6415dc59cb3cf782bf01d014982`，公开树中已有 14 行测量 | [公开 CSV](../../test/following-hybrid-cost.csv)、[环境和种子](../following-hybrid-phase-b.md) |
| 点赞 Worker 矩阵 | `data/perf/worker-lock-and-partition-summary.json` 和同目录的 `worker-*/1/` 归档，基线 `484b7a42b25dc27cd1ea038bbb499c28cd9a0bed` | [36 行逐轮数据](historical/like-worker-rounds.csv)，每行的 `source_round` 对应本地原始目录 |
| 正式 S3 / A2-1 | `data/perf/s3-acceptance/`、`data/perf/a2-acceptance/`，正式提交分别为 `c07498f145679798911ed3d32840cf057742ee73` 与 `e90f3c0665f2b9061a14a8029dd02928d14d0d2f` | [S3 汇总](historical/like-s3-acceptance.json)、[单热点输入校验](historical/like-s3-single-input.json)、[离线对账](historical/like-s3-single-reconciliation.json)、[A2 配对数据](historical/like-a2.json)与[点赞 k6 脚本](workloads/like-http.js) |
| 模块 6 历史首屏与故障对照 | `data/feed-read-reliability/module6/`，`c628b0f` 与 `25ea6b5` 的同等仪表临时构建 | [模块 6 k6 脚本](workloads/feed-module6.js)与 [Latest、Following、故障场景的逐轮 k6 JSON](historical/) |
| 模块 4 连接池和预算 | `data/feed-read-reliability/read-budget-lab-20261120T1455Z/`，`9a455729d47b025840f25db21916b4c2fb23daef` 临时变体 | [原 CSV 的 48 行子集](historical/read-budget-selected-rounds.csv)、[完整实验核验记录](historical/read-budget-verification.txt)、[仪表补丁](historical/read-budget-instrumentation.patch)与 [k6 脚本](workloads/read-budget-k6.js) |
| 最终后端大规模历史对照 | `data/feed-read-reliability/following-pipeline-timeout-20261121T055837Z/` 和 `following-direct-mysql-20261121T062914Z/`，基准 `689490bbfbf7360690aefb22c2243c372fbcc3eb` | [50/100 ms 数据](historical/following-50-vs-100ms.csv)、[直查 MySQL 数据](historical/following-direct-mysql.csv)、[Pipeline 构建哈希](historical/following-50-vs-100ms-builds.json)、[直查变体构建哈希](historical/following-direct-mysql-builds.json)与 [k6 请求、断言](workloads/following-scale-k6.js) |

历史 k6 脚本保留原来的回环端口、输入形状和校验规则。复核模块 6 Latest 时，可在对应版本的隔离 API 预热数据后，使用 `FEED_SCENE=Latest VUS=10 DURATION=20s SUMMARY_PATH=/tmp/latest.json k6 run docs/perf-evidence/workloads/feed-module6.js`。历史 Following、点赞等脚本需要用临时测试账号生成自己的 Token 文件、fixture 和 MySQL 预期结果，具体字段见脚本。模块 4 的 48 行包括 18 行规模、12 行预算范围、9 行 Following 10 VU 连接池和 9 行表锁对照。所有行保留原值。历史完整矩阵以原归档核对，本次未重新压测。

[隔离故障注入脚本](workloads/inject-lab-fault.sh)提供停启专用 Redis 和持有专用 MySQL 库视频表写锁的操作。本次测试了脚本语法，历史实际停启和表锁结果仍以对应的逐轮记录为准。归档中其余原始请求日志、Docker 配置、带凭据的 fixture、二进制和完整构建包留在本机。历史 Phase B 一次性成本探针未在已检查的公开树、`tools/` 或基线 ZIP 中找到，现有 Go 夹具可验证当前路径，原始计时用已公开的 CSV 核对。
