# GoFeed Feed API 功能验证准备报告

日期：2026-11-16。

本报告整理自上一轮只读源码和 Postman 文件检查。该轮检查未调用 API、访问 Redis、生成 MQ 消息、修改文件或执行 Git 操作；本次仅将检查结果保存为文档。

以下“已确认”均指源码行为，实际接口结果仍待运行验证。

## 1. 完整调用链

| 层级 | 文件与函数 | 作用 |
|---|---|---|
| Router | [router.go 第 169 行](../backend/internal/http/router.go#L169) `SetRouter` | 创建 Feed 的 Repository、Service、Handler，注册路由 |
| Middleware | [jwt.go 第 44 行](../backend/internal/middleware/jwt/jwt.go#L44) `SoftJWTAuth` | 允许不携带认证头的匿名请求；携带认证头则验证 |
| Handler | [handler.go 第 19 行](../backend/internal/feed/handler.go#L19) `FeedHandler.ListLatest` | 解析 JSON、处理数量和毫秒游标、获取查看者 ID |
| Service | [service.go 第 149 行](../backend/internal/feed/service.go#L149) `FeedService.ListLatest` | 时间线读取、空时间线重建、冷热数据拼接 |
| Repository | [repo.go 第 20 行](../backend/internal/feed/repo.go#L20) `FeedRepository.ListLatest` | 按发布时间倒序查询数据库 |
| 实体读取分支 | [service.go 第 36 行](../backend/internal/feed/service.go#L36) `GetVideoByIDs` → [repo.go 第 93 行](../backend/internal/feed/repo.go#L93) `GetByIDs` | 根据时间线中的 ID 读取视频实体 |
| 响应构造 | [service.go 第 535 行](../backend/internal/feed/service.go#L535) `buildFeedVideos` → [like_repo.go 第 65 行](../backend/internal/video/like_repo.go#L65) `BatchGetLiked` | 组装作者、视频及当前用户点赞状态 |

另有 Gin 默认 Logger、Recovery 中间件；`listLatest` 未配置专属限流器。

## 2. 请求与响应

已确认：

- 方法：**POST**。
- 路径：**`/feed/listLatest`**。
- 基线访问地址：`http://127.0.0.1:28080/feed/listLatest`。
- 直接访问 Backend 不加 `/api` 前缀。
- 请求使用 JSON Body，而不是 URL 查询参数。
- JWT **可选**。

首屏请求体：

```json
{
  "limit": 10,
  "latest_time": 0
}
```

| 参数 | 类型 | 处理规则 |
|---|---|---|
| `limit` | Go `int` | 有效范围 1～50；未提供、≤0 或 >50 均重置为 **10** |
| `latest_time` | Go `int64` | 正数按 Unix **毫秒**解析；≤0 按首屏处理 |

因此，传 `limit=100` 会返回最多 10 条，而不是截断为 50 条。应发送合法 JSON，首屏也可以使用 `{}`；完全没有请求体可能产生解析错误。

响应结构定义见 [entity.go 第 10 行](../backend/internal/feed/entity.go#L10)。以下是结构说明，不是实际响应，也不是可直接提交的 JSON：

```text
{
  "video_list": [
    {
      "id": 数值,
      "author": {
        "id": 数值,
        "username": 字符串
      },
      "title": 字符串,
      "description": 字符串（为空时省略）,
      "play_url": 字符串,
      "cover_url": 字符串,
      "create_time": Unix秒,
      "likes_count": 数值,
      "is_liked": 布尔值
    }
  ],
  "next_time": Unix毫秒,
  "has_more": 布尔值
}
```

注意：

- 下一页应原样使用响应的 **`next_time`**，不能使用视频项中的秒级 `create_time`。
- 空结果由 Handler 规范化为 `video_list: []`，不是 `null`。
- `has_more` 仅通过“本页数量是否等于 limit”判断，没有额外查询下一条，因此不是严格的剩余数据证明。
- 默认最多 10 条，最大有效请求数量 50 条；实际返回可能更少。

## 3. 时间线、Singleflight 与数据库回源

源码位置：[service.go 第 149 行](../backend/internal/feed/service.go#L149)。

| 条件 | 已确认处理 |
|---|---|
| Redis 客户端不可用 | 调用 `listLatestFromDB` |
| 读取 ZSET 尾部失败 | 回退数据库 |
| Redis 正常、ZSET 为空 | 从数据库读取最新最多 1000 条，重建时间线，再递归调用 `ListLatest` |
| 请求游标不晚于时间线最旧记录 | 查询数据库冷数据，不回填时间线 |
| 请求落在热点范围 | 从 ZSET 获取视频 ID，再读取实体 |
| 热数据不足一页 | 尝试从数据库补足 |

Repository 对应的 SQL 语义如下，**不是本次捕获的运行 SQL**：

```sql
-- 首屏 / 空时间线重建，LIMIT 分别为请求数量 / 1000
SELECT * FROM videos
ORDER BY create_time DESC
LIMIT ?;

-- 带游标的数据库分页
SELECT * FROM videos
WHERE create_time < ?
ORDER BY create_time DESC
LIMIT ?;

-- 实体读取
SELECT * FROM videos WHERE id IN (...);
```

查询没有软删除或状态过滤，也没有 `id` 次级排序。同一毫秒的多条视频可能在分页边界遗漏，仍需后续实测。

Singleflight 使用的是 `FeedService.requestGroup`：

| 场景 | Key 格式 |
|---|---|
| 空时间线重建 | `v1:sf:fallback:global_timeline_rebuild` |
| 单视频实体回源 | `v1:sf:entity:<视频ID>` |
| 冷数据查询 | `v1:sf:cold:listLatest:<limit>:<毫秒游标>` |
| 冷热拼接 | `v1:sf:stitch:listLatest:<剩余数量>:<毫秒游标>` |

这些是**进程内 Singleflight 标识，不是写入 Redis 的锁 Key**。同一个 Service 实例中并发、同 Key 的执行会合并；不跨 API 进程协调，也不保存长期查询结果。

## 4. Redis ZSET 格式与失败处理

默认前缀来源：[redis.go 第 21 行](../backend/internal/middleware/redis/redis.go#L21)。

| 项目 | 已确认格式 |
|---|---|
| Key | `v1:feed:global_timeline` |
| Member | 视频 ID 的十进制字符串 |
| Score | `CreateTime.UnixMilli()` 转为 float64 |
| 空时间线重建数量 | 最新最多 1000 条 |
| 重建写入超时 | 独立后台 Context，2 秒 |
| 重建设置 TTL | 没有 |

失败处理存在以下限制：

- 重建时数据库查询失败：返回错误，Handler 返回 500。
- 数据库无视频：返回空列表，`has_more=false`。
- **重建的 `ZAdd` 错误未检查**，随后仍递归重查。如果持续写失败而读取始终为空，存在反复递归重建风险。
- 热点分页的 ZSET 查询失败时直接返回错误，并非所有 Redis 错误都会回源。
- 部分实体查询错误、冷热拼接查询错误未完整向上返回，可能表现为返回数量不足。

**已有非空时间线不会自动完整纠错。** 没有全量对账、剔除全部无效成员或补齐所有缺失视频的逻辑。查询不足一页时回源补数据，也不等于修复 ZSET。

## 5. 首次请求的其他缓存副作用

`listLatest` 不只访问时间线。

| 缓存 | 读取与写入行为 |
|---|---|
| 进程内视频缓存 | `GetVideoByIDs` 优先读取；回填时 TTL 为 5 秒 |
| `v1:video:entity:<ID>` | 本地未命中后 Redis MGET；数据库命中后异步 SET，TTL 为 1 小时 |
| `v1:account:<ID>` | 仅携带认证时检查；数据库认证回退成功后可能回填，TTL 为 24 小时 |

来源：[GetVideoByIDs](../backend/internal/feed/service.go#L36)、[认证 check](../backend/internal/middleware/jwt/jwt.go#L70)。

需要注意：

- Redis 实体回填是异步的，HTTP 响应结束不代表这些 SET 已全部完成。
- 虽然方法名为 `GetByIDs`，当前缓存未命中分支会对每个缺失 ID 分别发起数据库查询，并用 Singleflight 按 ID 合并。
- 最新 Feed 没有使用关注 Feed 的整页缓存，也没有读取视频详情缓存或热门榜窗口。
- 此调用链没有发布 MQ 消息、修改视频/点赞记录或增加热度的逻辑。

因此，**首次匿名请求也可能改变缓存状态**，不能直接视为已经预热的稳态请求。

## 6. 匿名和登录验证方式

匿名方式已确认：**不发送 Authorization 头即可**。

`SoftJWTAuth` 并不忽略错误 Token：

- 无认证头：匿名放行，查看者 ID 为 0。
- 认证头格式错误、Token 无效或过期：401。
- 签名有效但与保存的 Token 不匹配：401。

匿名 `BatchGetLiked` 会提前返回，不执行点赞 SQL；登录请求则批量查询当前用户的点赞记录。即使视频实体缓存已热，登录 `listLatest` 仍会走这条点赞查询路径。

合法登录入口：

```text
POST /account/login
JSON 字段：username、password
```

调用链：

- [AccountHandler.Login 第 116 行](../backend/internal/account/handler.go#L116)
- [AccountService.Login 第 113 行](../backend/internal/account/service.go#L113)
- [AccountRepository.Login 第 74 行](../backend/internal/account/repo.go#L74)

后续可从已导入的测试账号中选择用户名，使用已约定的测试凭据正常登录。登录会校验密码、生成认证信息，并写入账号表及认证缓存，**不属于只读操作，该轮审查没有执行**。不能仅自行签名一个 JWT 就认为可以通过认证。

访问 Token 有效期为 15 分钟，见 [GenerateToken 第 42 行](../backend/internal/auth/jwt.go#L42)。本报告不展示密码、Hash 或 Token。

## 7. Postman 请求复用情况

已有请求：

- 文件：[test/postman.json 第 597 行](../test/postman.json#L597)
- 位置：`Feed → List Latest Feed (save feedLatestTime)`
- 方法：POST
- 地址：`{{host}}/feed/listLatest`
- 参数：`{{feedLimit}}`、`{{feedLatestTime}}`
- 测试脚本：将响应 `next_time` 保存为 `feedLatestTime`。
- 当前未配置 Authorization，可用于匿名场景。

只读检查确认，替换分页变量后的请求体是合法 JSON。后续复用前需要：

1. 将 `host` 指向 `http://127.0.0.1:28080`，现有默认值为 8080。
2. 首屏将 `feedLatestTime` 设为 0，避免沿用旧游标。
3. 按**毫秒**理解分页；集合描述中的“unix seconds”与当前源码不符。
4. 登录场景单独设置认证；现有匿名 Feed 请求不会因为执行过登录就自动带上 Token。
5. 单独发送所需请求，不运行包含注册、发布等写操作的整个集合。

登录请求位于 [postman.json 第 60 行](../test/postman.json#L60)：`Account → Login (save jwt_token)`。

## 8. 尚待审核后实测

尚未验证：真实响应字段与排序、首屏数量、分页重复/遗漏、匿名与登录的 `is_liked`、首次 ZSET 重建结果、实体异步回填结果及请求耗时。

后续应将**匿名首次请求、匿名预热后请求、登录请求**分开记录。检查到此结束，等待审核后再进行实际接口测试。
