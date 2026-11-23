# 小 V Pure Push / Pure Pull 隔离复现

[实验报告](../../GoFeed_小V_PushPull直接对照实验.md)以 `71f9713b394c90ed1d0b0d2334566860cd6207bf` 为基准。运行器从该 SHA 导出 `backend/` 到临时目录，仅在临时副本注入策略选择和整页回源计数。测试使用正式 `Publish`、快照解析、`writeFollowingCandidates` 和 `FeedService.ListByFollowing`。数据库连接池与 Redis 超时使用项目参数。RabbitMQ、Relay、全局时间线消费者与 ACK 不在实验路径中。

在 Linux 上准备回环绑定的独立 MySQL 8.0.46 与 Redis 7.4.11，使用 Go 1.24.5 和支持 `tarfile.extractall(filter=...)` 的 Python。将 `SMALLV_MYSQL_DSN` 设置为实验实例专用 MySQL DSN，数据库名留空，账号须能创建与清理临时库。将 `SMALLV_REDIS_ADDR` 设置为独立 Redis 的回环地址与端口，使用 DB 15。实例只允许本实验产生 MySQL、Redis 命令，测量依赖实例计数差值。密码只在本机提供。

```bash
python3 docs/perf-evidence/small-v-direct/run.py /tmp/gofeed-smallv-new
```

运行器先做编译与服务连通性检查，再执行固定八轮。第 1 对 Push→Pull，第 2 对 Pull→Push。每轮启动前写尝试台账，从夹具准备、分发和预热开始计数。不会自动重试。只验证环境可使用 `--preflight-only`，它不会执行任何配置轮次。不要重跑整套矩阵来替换失败样本。

- [尝试台账](measured/attempts.csv)、[写入逐轮](measured/write-rounds.csv)、[读取逐轮](measured/read-rounds.csv)记录正式测量。
- [测量目录](measured/)保存每轮日志、逐视频 Publish/Fanout 时间、[环境](measured/environment.log)与[构建哈希](measured/build.json)。
- 读取 SQL 计数使用 MySQL `Com_select`，并保留 GORM 实际查询耗时的累计窗口。Redis 通过 `INFO commandstats` 计算命令数和执行微秒增量。测试前后采样各自计数，CSV 保存差值或每请求值。
- 从仓库根目录执行 `sha256sum -c docs/perf-evidence/small-v-direct/SHA256SUMS`，可核对报告、运行器与保留的测量文件。
