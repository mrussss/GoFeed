# Following 空 Outbox 诊断复现

[诊断报告](../../GoFeed_Hybrid_Push读取收益诊断报告.md)基于本地 `main` 的 `e9f1e4f82654951100250f617ac7b290bc92738f`。本目录中的 Go 文件只由运行器复制进临时源码树。运行器从该 Git 提交导出 `backend/`，在副本加入按测试上下文筛选作者和计时的代码。Current 保留全部关注作者。InboxOnly 使用已由夹具证明为空的 Outbox 集合。功能测试传入明确需要读 Outbox 的作者集合。

在回环地址准备独立的 MySQL 8.0.46 和 Redis 7.4.11，MySQL 使用空库 `gofeed_read_test`，Redis 使用 DB 15。Go 使用 1.24.5，Python 需要支持 `tarfile.extractall(filter=...)`。在本机设置 `FEED_READ_TEST_DSN`（包含 `parseTime=true`）和 `C_TEST_REDIS_ADDR`。确保只有本实验使用该 Redis 实例，实例计数差值才有意义。用新的输出目录运行。

```bash
python3 docs/perf-evidence/hybrid-push-diagnostic/run.py /tmp/gofeed-push-diagnostic-new
PYTHONDONTWRITEBYTECODE=1 python3 docs/perf-evidence/hybrid-push-diagnostic/verify_sql.py
```

运行器先执行功能门禁，再串行执行 K=2、50、500 各四次。第 1 对顺序为 Current、InboxOnly，第 2 对反向。每个配置各两次，一次运行中没有自动重试。每轮启动独立测试进程并清理自身数据。使用 `--functional-only` 可仅验证历史读取而不做计时。完整测量时每轮固定约 4 秒、单闭环请求流，先做 30 次预热请求。固定种子 `20261122`。

- [尝试台账](measured/attempts.csv)保留全部 12 次配置执行。原始 [逐轮 CSV](measured/rounds.csv)中的 `sql_per_request` 字段实际是 Query/Row 回调次数。它包含一条 GORM DryRun 子查询回调。另用 [MySQL 会话计数器](measured/sql-verification.log)核对，两种满页暖路径的实际 SELECT 都是 3 条。逐轮 CSV 保留原字段和值。
- [功能日志](measured/history.log)包括对照结果、候选命令数及预期失败的仅按当前模式反例。[构建记录](measured/build.json)保留正式基准、Go 版本、测试二进制与源码哈希。每轮日志在同一目录。
- [pprof](profiles/)保留六格所有轮次的 CPU 和增量分配摘要，以及 K=500 全部四轮的原始 profile。CPU 与 `alloc_space` 是采样，多轮的请求数也不相同。精确的 Go 分配/请求取 `runtime.MemStats.TotalAlloc` 差值。由 `go tool pprof -top profile.pb.gz` 查看 CPU 原样本。分配差值使用 `go tool pprof -top -sample_index=alloc_space -base before.pb.gz after.pb.gz`。
- Redis `INFO stats commandstats` 是测试独占实例的窗口差值，入站/出站字节包括采样命令产生的很小固定开销。服务端 `ZREVRANGEBYLEX` 调用数与客户端命令数逐轮核对。
