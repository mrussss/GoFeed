# GoFeed 数据库导入前核验报告

核验时间：2026-11-16。已对运行中的 MySQL 执行只读查询，未执行数据写入、AutoMigrate、配置修改或服务启停。本文件整理自当次核验结果，不代表重新检查后的实时状态。

**结论：表结构能够承载当前 CSV，但导入前还需确认 API 运行环境按 UTC 解释时间。** 毫秒精度已满足；目前不能把时区一致性判为通过。

## 1. MySQL 与数据库状态

| 项目 | 实际结果 |
|---|---|
| MySQL 版本 | **8.0.46** |
| 查询数据库 | `feedsystem` |
| 四张核心表 | 均存在 |
| 存储引擎 | 均为 InnoDB |
| 排序规则 | 均为 `utf8mb4_0900_ai_ci` |

当次连接的是现有 `feedsystem_video_go-mysql-1` 容器，没有初始化数据库。

## 2. 实际 SHOW CREATE TABLE

以下为数据库返回的实际结构，仅调整了展示换行。

```sql
CREATE TABLE `accounts` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(191) DEFAULT NULL,
  `password` longtext,
  `token` longtext,
  `refresh_token` longtext,
  `avatar_url` varchar(512) DEFAULT NULL,
  `bio` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uni_accounts_username` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=2
  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

```sql
CREATE TABLE `videos` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `author_id` bigint unsigned NOT NULL,
  `username` varchar(255) NOT NULL,
  `title` varchar(255) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `play_url` varchar(255) NOT NULL,
  `cover_url` varchar(255) NOT NULL,
  `create_time` datetime(3) DEFAULT NULL,
  `likes_count` bigint NOT NULL DEFAULT '0',
  `popularity` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `idx_videos_author_id` (`author_id`),
  KEY `idx_videos_create_time` (`create_time` DESC),
  KEY `idx_videos_popularity_time_id`
    (`popularity` DESC,`create_time` DESC),
  KEY `idx_videos_likes_count_id` (`likes_count` DESC)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

```sql
CREATE TABLE `socials` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `follower_id` bigint unsigned NOT NULL,
  `vlogger_id` bigint unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_social_follower_vlogger`
    (`follower_id`,`vlogger_id`),
  KEY `idx_social_follower` (`follower_id`),
  KEY `idx_social_vlogger` (`vlogger_id`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

```sql
CREATE TABLE `likes` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `video_id` bigint unsigned NOT NULL,
  `account_id` bigint unsigned NOT NULL,
  `created_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_like_video_account` (`video_id`,`account_id`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

## 3. 时间字段、索引及约束核验

| 字段 | 实际类型 | DATETIME_PRECISION | 可空 | 默认值 |
|---|---|---:|---|---|
| `videos.create_time` | `datetime(3)` | **3** | YES | NULL |
| `likes.created_at` | `datetime(3)` | **3** | YES | NULL |

两列均支持毫秒，能够保存当前 CSV 的三位小数秒。导入工具仍需显式填写时间，不能依赖默认值。

实际索引已通过 `information_schema.STATISTICS` 复核：

- 四表均以 `id` 为主键。
- 用户名唯一约束存在。
- 关注关系 `(follower_id,vlogger_id)` 联合唯一约束存在。
- 点赞关系 `(video_id,account_id)` 联合唯一约束存在。
- 视频作者、发布时间、热度和点赞数索引均存在，具体列顺序见上述 DDL。
- 两个名称包含 `_id` 的视频索引，**实际显式列均不包含 `id`**。
- `likes.account_id` 没有独立索引。
- 所有查到的索引均可见，没有索引前缀截断。

`TABLE_CONSTRAINTS`、`KEY_COLUMN_USAGE` 和 `REFERENTIAL_CONSTRAINTS` 查询确认：**四张表均没有实际外键约束**。因此孤儿关联、自关注等仍需要导入工具和校验器保证。

## 4. MySQL 时区实测

| 项目 | 实际值 |
|---|---|
| `@@global.time_zone` | `SYSTEM` |
| `@@session.time_zone` | `SYSTEM` |
| `@@system_time_zone` | `CST` |
| MySQL 容器配置的 `TZ` | `Asia/Shanghai` |

同一次查询结果：

```text
NOW(3)            = 2026-11-16 15:43:56.938
UTC_TIMESTAMP(3)  = 2026-11-16 07:43:56.938
```

由实际时间差确认，**当次 MySQL 查询会话使用 UTC+08:00**，不只是根据含义可能有歧义的 `CST` 缩写推断。

这里检查的是当次 MySQL 客户端连接的会话时区，不能代表其他连接曾经或将来设置的会话时区。

## 5. API 容器检查

当次检查时，现有 `feedsystem_video_go-backend-1` 状态为 **exited**，退出码为 127。

- 容器配置中未设置 `TZ`。
- 因容器未运行，未执行容器内 `date`、`date -u`。
- 没有启动 API，也没有通过运行入口获取信息。
- **API 进程实际 Local 时区尚未验证。** 不能仅凭未设置 `TZ` 就认定一定使用 UTC。

宿主机实测为 UTC+08:00，但宿主机时区不能代替 API 容器时区。

## 6. CSV 与 loc=Local 的一致性风险

CSV 的逻辑时间明确为 UTC，实际采用无时区后缀的字符串，例如：

```text
videos.create_time = 2026-11-10 12:47:53.987
likes.created_at   = 2026-11-10 13:34:31.867
```

清单参考时间为 `2026-11-16T00:00:00Z`。输出实现见 [csv.go](../tools/baseline/csv.go)。

后端 DSN 使用：

```text
parseTime=True&loc=Local
```

见 [db.go 第 17 行](../backend/internal/db/db.go#L17)。本机已有 MySQL 驱动文档也明确：`loc` 决定 Go `time.Time` 的时区解释，**不会修改 MySQL 的 `time_zone`**。

具体影响：

- 当前两列是 **DATETIME**，保存的是日期时间数值，本身不携带时区。直接导入字符串时，MySQL 不会因为会话是 UTC+8 就自动把它转换为 UTC。
- 如果 API 的 Local 为 UTC，读取这些 UTC 数值时，语义可以一致。
- 如果 API 的 Local 为 Asia/Shanghai，同一个 `12:47:53.987` 会被解释为北京时间，对应 `04:47:53.987Z`，与 CSV 原意相差 **8 小时**。
- 这种差异会影响 Unix 时间输出、分页游标、Redis 时间线 score，以及与当前时间相关的判断。
- 如果后续使用 Go 导入工具解析 CSV 后再写库，也必须核对解析时区与驱动序列化时区，避免写入时发生偏移。

## 7. 导入前判断

| 项目 | 判断 |
|---|---|
| 核心表结构与字段容量 | 可用于当前小档 CSV |
| 毫秒时间精度 | 通过 |
| 唯一约束及索引 | 已取得实际结果 |
| 数据库外键兜底 | 不存在，继续依赖工具校验 |
| CSV 与 API 时间解释一致性 | **尚未通过，API 运行时区未验证** |
| 当前库是否可直接覆盖或导入 | 当次未作此授权或判断，应使用隔离测试库 |

建议后续导入阶段统一采用 **UTC 的解析、存储约定和 Go 读取语义**，并抽查一条已知时间：比较 CSV 原值、数据库读回值及 API 输出的 Unix 毫秒是否一致。无需重新生成现有 CSV，也不需要因此修改业务架构。
