# GoFeed A2-1 批量标记验收

基线为 `feat/like-reliable-ingress` 的 `c07498f`。沿用 S3 四分区和业务双 Key 缓存。

## 实现与故障边界

`commandrelay.go` 继续按 ID 查询最早的最多 100 条 pending 命令，单 Relay 按查询顺序发布并逐条等待 Confirm。每攒齐 50 个确认 ID，执行一次 `UPDATE command_outbox SET published_at = NOW(3) WHERE id IN (…) AND published_at IS NULL`，轮次末尾刷新余量。等待 Confirm 时没有持有 MySQL 事务，现有 Persistent、Mandatory、Confirm 和 Return 检查沿用原实现。

发布失败时先写入本轮已经确认的前缀，再返回发布错误。若退出时上下文已经取消，已确认前缀最多用 3 秒尝试落库。SQL 失败时 pending 行可用原 CommandID、MessageId 与消息体重投。检查 `RowsAffected`，发生异常时返回实际标记数量与错误。Worker 继续用 Revision 保证重复投递的计数幂等。

## 验收设置

原 mixed 数据生成器与种子，50 RPS、8 秒预热并排空、30 秒正式测量。使用隔离 Compose、相同服务镜像和 2 CPU / 2 GiB 配额，MySQL 8.0.46、RabbitMQ 3.13.7、Go 1.24.5、k6 2.4.0。业务构建沿用 S3，计时仅加入实验副本；现有脚本副本更换本机隔离端口。

现有历史的 mixed 对照为 CacheB-S3。正式基线的活跃发布、Confirm、标记及峰值接近历史，且 Outbox 同样持续积压。历史本轮原始记录的负载进程和排空耗时约 31.21 + 20.64 秒，基线为 33.09 + 15.28 秒；从测量启动到排空合计约 51.85 和 48.37 秒。核对了 CSV 哈希、到达率、服务镜像、配额和配置，基线测量进程结束较晚且 Relay 活跃处理较快，排空单项的差异无需重复基线。

## 性能对照

| 指标 | 逐条标记 | 批量标记 |
|---|---:|---:|
| HTTP 202/s（按配置的 30 秒） | 50.03 | 50.00 |
| HTTP P95 / P99（ms） | 49.82 / 68.60 | 43.94 / 119.40 |
| Relay 活跃发布（条/s） | 31.08 | 84.21 |
| Publisher Confirm 平均（ms/条） | 10.89 | 11.24 |
| MySQL 标记平均（ms/条） | 21.28 | 0.63 |
| 标记 SQL 次数 | 1,351 | 32 |
| MySQL 标记平均（ms/次 SQL） | 21.28 | 26.68 |
| Outbox Pending 峰值 | 529 | 103 |
| MQ Ready / Unacked 采样峰值 | 0 / 3 | 0 / 4 |
| Worker ACK/s（测量窗口） | 27.11 | 42.46 |
| 停压至确认排空（秒） | 15.28 | 3.23 |

Relay 活跃速率以完整轮次处理时间计算，包含等待 Confirm 和写标记，轮询间的空闲不计入。mixed 每十次请求有一次幂等重试，50 HTTP RPS 对应约 45 条新命令/s。优化轮共有 1,350 条正式测量新命令，全部 ACK。Outbox 在测量期内周期性回落，采样峰值 103，最终归零。两轮的最终 Ready、Unacked、pending 和未应用 Revision 均为 0，在线与独立离线对账均通过。

HTTP P99 在优化轮升高。原始采样显示约第 4 秒有一组请求延迟集中升高，另有一个客户端样本约 1.51 秒，服务端记录的该轮请求最大耗时约 250 ms。两轮所有正式请求都返回 202，无丢弃迭代。单次对照无法确定尾延迟变化是否由批量标记引起。

## 正确性与结论

`commandrelay_test.go` 覆盖 50+5 两次 SQL、空轮次无写入、发布失败时确认前缀落库、批量更新失败整体回滚并保持原投递身份、RowsAffected 异常。真实 RabbitMQ 验证持久消息、Confirm、Return、NACK 和重连，并将标记失败重投的两条实际投递交给 Worker，验证关系、Revision 和视频计数只应用一次。

真实 MySQL、RabbitMQ 和 Redis 集成用例在 `go test -count=1 ./...` 与 `go test -race -count=1 ./...` 中均实际执行并通过。`gofmt`、`go vet ./...`、`go build ./...`、`git diff --check` 通过。

**达到 50 RPS 验收目标，建议冻结 A2。** Relay 活跃速率超过 55 条/s，Outbox 已无持续积压。批量标记之后 Confirm 是 Relay 活跃时间的主要开销，现有目标无需 A2-2。本次共两次正式运行，基线与优化后各一次，未执行诊断复测。

本机实验归档位于 `data/perf/a2-acceptance/`，包含原始样本、复测汇总 `metrics.json` 和验收时导出的补丁。
