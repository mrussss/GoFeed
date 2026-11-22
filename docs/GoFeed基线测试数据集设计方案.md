# GoFeed 基线测试数据集设计方案

日期：2026-11-16。范围：源码分析和测试数据方案设计。

本次未生成实际数据，未连接数据库执行读写，未运行测试，未启动或停止 Docker，未执行 Git 操作。按最终交付要求，仅新增本报告；未修改业务代码或其他文件。

本文的“源码事实”来自现有模型、迁移、Repository 和业务逻辑；“建议方案”是供后续确认的测试参数，不代表项目真实用户分布。未检查实际数据库 DDL，因此物理列类型、索引和历史迁移结果仍需后续只读核验。

## 1. 核心模型及约束

项目实际名称是 Account（User）、Social（Follow）、Video、Like。未配置自定义 NamingStrategy，也未发现这些模型的 TableName 方法；按 GORM 默认命名，预期表为 accounts、socials、videos、likes。实际表名以之后读取 DDL 为准。[源码：db.go 第 18 行](../backend/internal/db/db.go#L18)

### 1.1 Account / User

| 字段 | Go 类型 | 显式模型约束 | 数据准备要求 |
|---|---|---|---|
| ID | uint | primaryKey | 正整数、唯一；通常由 GORM/MySQL 自增，也可在独立数据集中明确分配 |
| Username | string | unique | 生成非空、大小写无歧义的唯一用户名 |
| Password | string | 无显式 not null、长度或默认值 | 必须是可供 bcrypt 校验的密码 Hash，不能直接填明文 |
| Token / RefreshToken | string | 无显式约束 | 静态基础快照可为空；鉴权准备阶段另行登录 |
| AvatarURL | string | varchar(512) | 可空字符串；建议固定本地资源路径 |
| Bio | string | varchar(255) | 可空字符串 |

来源：[account/entity.go 第 3 行](../backend/internal/account/entity.go#L3)。注册 Handler 只绑定 username/password，未声明 required 校验；Service 通过 bcrypt.DefaultCost 加密后保存。因此“数据库可写入”与“可正常登录的有效账号”不能混为一谈。用户名非空是本方案主动收紧的约束。[Handler 第 28 行](../backend/internal/account/handler.go#L28)、[Service 第 33 行及第 113 行](../backend/internal/account/service.go#L33)、[Repository 第 17 行](../backend/internal/account/repo.go#L17)

### 1.2 Video

| 字段 | Go 类型 | 显式模型约束 |
|---|---|---|
| ID | uint | primaryKey |
| AuthorID | uint | not null；普通索引 |
| Username | string | varchar(255)，not null；作者名冗余字段 |
| Title | string | varchar(255)，not null |
| Description | string | varchar(255) |
| PlayURL / CoverURL | string | 各 varchar(255)，not null |
| CreateTime | time.Time | autoCreateTime；降序时间索引；参与热度组合索引 |
| LikesCount | int64 | not null，default:0；参与点赞数组合索引 |
| Popularity | int64 | not null，default:0；参与热度组合索引 |

来源：[video/video_entity.go 第 5 行](../backend/internal/video/video_entity.go#L5)。

模型声明的索引：

- AuthorID 的单列普通索引。
- `idx_videos_create_time`：CreateTime DESC。
- `idx_videos_likes_count_id`：**显式字段只有 LikesCount DESC**，ID 未声明加入该索引。
- `idx_videos_popularity_time_id`：**显式字段只有 Popularity DESC、CreateTime DESC**，ID 未声明加入该索引。

不能根据索引名字认定 ID 是显式组合列；InnoDB 二级索引的主键存储细节也不能代替对实际 DDL 和执行计划的检查。[模型第 13 行](../backend/internal/video/video_entity.go#L13)、[排序查询第 33 行及第 72 行](../backend/internal/feed/repo.go#L33)

有效视频至少需要：存在的 AuthorID、与作者匹配的 Username、非空 Title/PlayURL/CoverURL、明确的 CreateTime，以及一致的 LikesCount/Popularity。Service 会 Trim 标题和 URL 并拒绝空值，Description 可选。通过发布接口时，AuthorID、Username 取自 JWT，CreateTime 为服务端当前时间，不能用请求直接指定历史发布时间。[PublishVideo 第 30 行](../backend/internal/video/video_handler.go#L30)、[Publish 第 29 行](../backend/internal/video/video_service.go#L29)

### 1.3 Social / Follow

| 字段 | Go 类型 | 显式模型约束 |
|---|---|---|
| ID | uint | primaryKey |
| FollowerID | uint | not null；`idx_social_follower`；联合唯一索引第一列 |
| VloggerID | uint | not null；`idx_social_vlogger`；联合唯一索引第二列 |

联合唯一索引 `idx_social_follower_vlogger(FollowerID, VloggerID)`。没有时间字段。[social/entity.go 第 5 行](../backend/internal/social/entity.go#L5)

有效关系需要两个已存在的账号，且不能关注自己、不能重复。A 关注 B 不代表 B 关注 A；允许两个方向分别存在。关注目标在业务代码中只要求是有效账号，并不要求其已经发布视频。接口只提交 vlogger_id，关注者来自鉴权上下文。[social/service.go 第 23 行](../backend/internal/social/service.go#L23)、[social/handler.go 第 20 行](../backend/internal/social/handler.go#L20)

### 1.4 Like

| 字段 | Go 类型 | 显式模型约束 |
|---|---|---|
| ID | uint | primaryKey |
| VideoID | uint | not null；联合唯一索引第一列 |
| AccountID | uint | not null；联合唯一索引第二列 |
| CreatedAt | time.Time | 无显式标签；符合 GORM 创建时间惯例 |

联合唯一索引 `idx_like_video_account(VideoID, AccountID)`；没有显式 AccountID 单列索引。[video/like_entity.go 第 5 行](../backend/internal/video/like_entity.go#L5)

有效点赞需要已存在的账号、视频，以及唯一的 (VideoID, AccountID)。接口从 JWT 获取用户，Service 检查视频存在、是否已点赞，并设置 CreatedAt。没有“必须先关注作者”的要求，也没有禁止作者给自己点赞；本方案选择不生成自赞，这是测试选择而非源码约束。[like_handler.go 第 18 行](../backend/internal/video/like_handler.go#L18)、[like_service.go 第 31 行](../backend/internal/video/like_service.go#L31)

## 2. 外键、迁移、默认值与删除行为

1. 四类模型仅存储关联 ID，没有 GORM 关联对象、foreignKey/references 标签或显式外键 DDL。源码未声明数据库外键、级联删除或 CHECK 约束；not null 也不能阻止 ID=0 或孤儿记录。必须在数据生成阶段主动校验关联完整性。此结论不保证已有数据库不存在手工添加的约束。[四类模型见上节；迁移 db.go 第 26 行](../backend/internal/db/db.go#L26)
2. 迁移入口为 GORM AutoMigrate，包含 Account、Video、Like、Comment、Social、OutboxMsg、Tag、VideoTag、Message、Notification；未发现独立 SQL/版本化迁移文件。API 启动即调用迁移，本次未启动。[db.go 第 26 行](../backend/internal/db/db.go#L26)、[cmd/main.go](../backend/cmd/main.go)
3. 四类模型没有 DeletedAt、gorm.Model、审核/发布状态、可见性字段，也没有创建钩子。视频、点赞、关注 Repository 的 Delete 是物理删除路径；删除视频没有在该方法中同步清理点赞等关联。[video_repo.go 第 32 行](../backend/internal/video/video_repo.go#L32)、[like_repo.go 第 23 行](../backend/internal/video/like_repo.go#L23)、[social/repo.go 第 22 行](../backend/internal/social/repo.go#L22)
4. NOT NULL 不等于非空字符串，数值 default:0 不等于计数自动维护。基线数据应显式填写时间和统计字段，不依赖导入时间、自动默认值或数据库会话时区。
5. 发布事务不仅创建视频，还创建 pending Outbox 和标题/描述中提取的 #标签。只导入 videos 不能声称完整复现发布副作用。[video_service.go 第 49 行](../backend/internal/video/video_service.go#L49)、[tag_entity.go 第 5 行](../backend/internal/video/tag_entity.go#L5)
6. 大小写用户名应避免依赖数据库排序规则区分：Compose 的 MySQL 配置为 utf8mb4_0900_ai_ci；实际已有库需后续核验。[docker-compose.yml](../docker-compose.yml)

## 3. 类型、时间单位与读取依赖

所有核心 ID 及关联 ID 在 Go 中为 uint，位宽与编译平台相关；不是字符串、UUID 或时间戳。本方案使用远低于 2^31 的正整数，避免前端 Number 精度与平台差异。GORM/MySQL 的预期类型是无符号整数主键，精确列宽、自增及时间精度需读取实际 DDL 后确认，不在本次静态检查中当作已验证事实。[四类 Model；db.go 第 18 行](../backend/internal/db/db.go#L18)

| 位置 | 时间表达 | 数据设计影响 |
|---|---|---|
| Video.CreateTime / Like.CreatedAt | Go time.Time，预期数据库日期时间列 | 按日期时间保存，不把 Unix 整数误当 SQL 时间字段 |
| 视频详情/发布结果中的 CreateTime | time.Time 的 JSON 日期时间字符串 | 与 Feed 输出不同 |
| Feed 项 create_time | Unix 秒 | 不能直接充当最新 Feed 毫秒游标 |
| listLatest.latest_time / next_time | Unix 毫秒 | 时间线 ZSET score 和 TimelineEvent 同样是毫秒 |
| listByFollowing.latest_time / next_time | Unix 秒 | 秒内多条发布可能在翻页边界遗漏 |
| 热榜 as_of | Unix 秒，按 UTC 分钟截断 | Redis 热榜按分钟键读取最近 60 个桶 |
| 热榜数据库回退 latest_before | time.Time JSON 日期时间 | 必须同时保留热度、时间、ID 游标 |

来源：[feed/handler.go 第 19 行及第 90 行](../backend/internal/feed/handler.go#L19)、[feed/service.go 第 315 行、第 428 行、第 535 行](../backend/internal/feed/service.go#L535)、[timelineMQ.go 第 44 行](../backend/internal/middleware/rabbitmq/timelineMQ.go#L44)、[feed/entity.go](../backend/internal/feed/entity.go)。数据库连接采用 loc=Local，后续固定会话与进程时区为 UTC 或明确记录转换，不能只修改时间字符串尾缀。[db.go 第 18 行](../backend/internal/db/db.go#L18)

| 读取入口 | 实际依赖 |
|---|---|
| 最新 Feed | videos.create_time、完整视频字段；热数据依赖时间线 ZSET 和实体缓存 |
| 点赞数 Feed | videos.likes_count DESC、id DESC；不会现场聚合 likes 替代计数 |
| 关注 Feed | socials.follower_id → vlogger_id → videos.author_id，按 create_time 排序 |
| 热度 Feed | Redis 最近一小时互动分数；回退才使用 videos.popularity/create_time/id |
| 所有 Feed 的 is_liked | 当前查看者与 likes 的批量查询；匿名查看者返回未点赞 |
| Feed 作者信息 | 直接使用 videos.author_id/username，不实时 JOIN accounts 修复冗余作者名 |
| 作者视频列表 | author_id，按时间排序，最多 200 条 |
| 我的点赞列表 | likes JOIN videos，按 likes.created_at 排序，最多 200 条 |
| 粉丝/关注列表 | socials → accounts；关系查询最多取 200 条，计数接口独立计全部 |
| 个人资料统计 | 作者视频数、SUM(videos.likes_count)、关系计数 |
| 标签 Feed | videos JOIN video_tags JOIN tags；关联缺失即无法命中 |

来源：[feed/repo.go 第 20、33、52、72、105 行](../backend/internal/feed/repo.go#L20)、[feed/service.go 第 428、535 行](../backend/internal/feed/service.go#L428)、[video_repo.go 第 39、106 行](../backend/internal/video/video_repo.go#L39)、[like_repo.go 第 65、86 行](../backend/internal/video/like_repo.go#L65)、[social/repo.go 第 28、56、95 行](../backend/internal/social/repo.go#L28)、[http/router.go](../backend/internal/http/router.go)。

## 4. 可复现数据集规格（建议方案）

以下参数为设计建议，非源码现有配置。设计目的对应第 3 节读取路径；不增加业务表、作者等级字段或新的业务架构，角色与采样信息保存在未来生成器的外部清单。

### 4.1 三种规模

| 规模 | 用户 U | 视频 V | 关注 F | 点赞 L | 作者数（20% U） | 用途 |
|---|---:|---:|---:|---:|---:|---|
| 小 | 1,000 | 10,000 | 20,000 | 100,000 | 200 | 完整性、分页和流程校验；覆盖超过 1000 条时间线及 200 条列表截断 |
| 中 | 10,000 | 100,000 | 200,000 | 1,000,000 | 2,000 | 常规基线；观察索引、缓存工作集与长尾查询 |
| 大 | 100,000 | 1,000,000 | 2,000,000 | 10,000,000 | 20,000 | 数据容量敏感性；观察更大的索引、关联表及聚合成本 |

每档等比例放大 10 倍，平均每用户 20 条关注、100 条点赞，平均每视频 10 条点赞；保持分布参数一致，便于比较数据量影响。大档是容量候选，不承诺当前机器可承载，也不代表并发目标；实际执行前再核定磁盘、内存和场景。本次不生成上述数据。

### 4.2 确定性约定

- 方案版本 `gofeed-baseline-dataset-v1`，根随机种子 `20261116`。
- 后续生成器固定 Go 工具链及随机算法，例如 Go 1.24.5 的 math/rand.NewSource；用户、作者、时间、关注、点赞使用由根种子和阶段名经 SHA-256 派生的独立子种子，派生字节序和截取方式写入生成器规范。阶段内按固定 ID 顺序执行；禁止依赖 map 遍历顺序或并行任务完成顺序。
- 固定逻辑参考时间 `T0=2026-11-16T00:00:00Z`；静态数据均不晚于 T0。物理测试开始时间另记为 R，不用 time.Now 替代生成逻辑时间。
- 每档在独立空测试库从用户/视频 ID=1 分配，关系按 `(follower_id,vlogger_id)`、点赞按 `(video_id,account_id)` 排序后分配 ID。各档独立生成，不宣称小档是大档子集。
- 用户名使用固定小写 ASCII：`bench_v1_<scale>_<id>`；字段长度限定在模型声明以内；Title/Description/URL 使用确定性模板。固定一套本地视频/封面资源并记录文件 Hash；URL 不指向外部随机媒体。
- 密码使用预先冻结的合法 bcrypt 测试 Hash，记录校验和；bcrypt 自带随机盐，不能把“种子相同”误认为现场加密结果逐字节一致。账号身份数据可复现；运行期 JWT、刷新 Token 和有效期单独管理，不计入静态快照 Hash。[密码逻辑：account/service.go 第 33、113 行](../backend/internal/account/service.go#L33)
- 清单记录各阶段种子、排序规则、工具链、参数、计数、时间范围、文件/分块 SHA-256；按主键排序的规范化记录流计算整体摘要，不能仅比较数据库物理文件 Hash。

### 4.3 作者类型及产量

80% 用户不发布，20% 用户为作者。作者身份用固定种子的排列分配，与 ID 大小解耦。

| 作者类型 | 占作者比例 | 占全部用户 | 每作者视频数 | 占全部视频 | 小/中/大作者数 |
|---|---:|---:|---:|---:|---|
| 普通作者 | 90% | 18% | 20 | 36% | 180 / 1,800 / 18,000 |
| 中型作者 | 9% | 1.8% | 200 | 36% | 18 / 180 / 1,800 |
| 大 V | 1% | 0.2% | 1,400 | 28% | 2 / 20 / 200 |

合计 V=10U。固定产量让三档数量精确一致，也覆盖作者列表 200 条上限；第一版不额外引入产量随机误差。作者类型仅为测试分组，不映射任何项目状态字段。[读取上限：video_repo.go 第 39 行](../backend/internal/video/video_repo.go#L39)

### 4.4 长尾关注关系

关注者出度配额：10% 用户关注 0 人、50% 关注 10 人、30% 关注 30 人、9% 关注 50 人、1% 关注 150 人，精确得到 F=20U。所有用户均可作为关注者，作者也包含在内。

目标作者按普通/中型/大 V 分配约 70%/25%/5% 的关注边；组内按固定随机排名使用截断 Zipf 权重 `w(r)=r^-0.8`。这些是分配目标，不是现实统计；大 V 人均入度应高于中型，中型高于普通，组内保留长尾。

必须采用无放回目标选择，排除自己；每对最多一条，每作者入度不超过 U-1。先校验容量，再按固定顺序分配配额；过饱和时在同组重分配，仍不可行再按固定组顺序回填，并记录实际比例。设置有限重试和确定性剩余容量分配，不能靠无限拒绝采样或静默少生成。小档只有 2 位大 V，5% 总边数为 1000，未超过 1998 的理论容量，避免不可能的热点配额。

预留并固定以下样本：无关注、少量关注、重度关注、关注普通作者、关注大 V、互相关注。关注仅指向作者是本方案选择，业务允许关注无视频用户。[关联规则：social/service.go 第 23 行](../backend/internal/social/service.go#L23)

### 4.5 发布时间

所有作者的视频混合分配到四个互不重叠时间区间：最近 1 小时 10%；1 小时至 1 天 20%；1 天至 7 天 30%；7 天至 180 天 40%。边界统一采用半开区间并在清单列明，T0 本身不安排视频。各区间按确定性配额分派至作者，再在区间内无放回抽取毫秒时间点，保证主数据集全局 CreateTime 在毫秒级唯一，并打乱时间与视频 ID 的对应。

该分布同时保留近期热点和足够冷数据。大档最近一小时有 10 万条视频，仍可在 360 万个毫秒槽中安排；不要要求秒级唯一，否则无法满足该规模。

数据库时间精度需先核验；若不足毫秒，不能默默截断后宣称时间唯一。即使毫秒唯一，关注 Feed 的秒级游标仍可能漏掉边界秒内记录：作为现有接口局限记录，不通过删除数据掩盖。另定义 `same-timestamp` 边界变体：在相同总量内选择固定 100 条视频置于同一毫秒，单独验收分页行为，不混入主性能结果。[时间游标：feed/handler.go 第 19、90 行](../backend/internal/feed/handler.go#L19)、[严格小于条件：feed/repo.go 第 20、52 行](../backend/internal/feed/repo.go#L20)

### 4.6 点赞分布与计数

- 固定 20% 视频为零赞；其余视频划分为爆款（全部视频前 1%）、热门（随后 9%）、普通有赞（70%），分别分配 L 的 30%、40%、30%。先用种子决定排序，适度提高大 V 视频被选为热点的权重，但保留普通作者爆款。
- 各组内使用截断 Zipf 权重 `r^-0.8` 分配整数点赞配额：采用最大余数法补足总数，平分时按 ID 排序；每视频上限 U-1，超额转给同组剩余容量。分配前要求配额之和不超过容量；最终必须精确达到 L。
- 点赞用户从该视频作者之外的用户中无放回选择，可用固定用户活跃权重（10% 高活跃权重 5，其余权重 1）。不要求先关注；固定留出已赞/未赞请求样本。该第一版不保证零点赞用户比例，不额外声称每用户恰好 100 赞。
- CreatedAt 必须位于 `[视频 CreateTime,T0)`；保留同一时间点赞的记录，用于验证列表排序并列时的现状。零赞视频不得出现在 likes 中。
- `videos.likes_count` 必须逐视频等于 likes 实际条数。基础数据集不生成评论或其他热度事件，约定 `popularity=likes_count`，仅在这个受控前提下成立；真实业务评论发布也会增加热度，不能把该等式推广到任意运行后的库。[like_service.go 第 31 行](../backend/internal/video/like_service.go#L31)、[likeworker.go 第 105 行](../backend/internal/worker/likeworker.go#L105)、[commentworker.go 第 101 行](../backend/internal/worker/commentworker.go#L101)

### 4.7 数据库快照、缓存和发布副作用

基础快照建议采用后续离线导入方案，导入顺序为 accounts → videos → socials/likes → 标签关系 → 派生计数；它用于准备初始状态，不作为发布/点赞接口耗时测量。历史时间不能通过现有发布 API 指定，批量接口造数也会引入限流、运行时间和异步随机性。[video_handler.go 第 30 行](../backend/internal/video/video_handler.go#L30)、[http/router.go](../backend/internal/http/router.go)

分开定义三种运行准备状态，不改变业务实现：

1. **冷缓存**：数据库已校验，测试专属 Redis 中无业务缓存和热榜，MQ 无历史积压；记录首次请求的重建成本。
2. **暖时间线/实体**：通过固定请求清单预热并记录结果。最新时间线应为确定的最近 1000 条视频；分页还要覆盖更旧的数据。[feed/service.go 第 167 行](../backend/internal/feed/service.go#L167)、[outboxworker.go 第 49 行](../backend/internal/worker/outboxworker.go#L49)
3. **暖热榜**：另有固定的“互动时间清单”，按相对 R 的分钟桶投影到实际 UTC 时间，重建最近 60 桶和其 TTL，再由接口产生合并快照。分数表示窗口内互动，不等于累计点赞数，也不直接把全部 likes_count 填成最近一小时分数。窗口事件只能来自视频发布之后的点赞，跨 T0/R 的平移规则写入运行清单。[popularity_cache.go 第 12 行](../backend/internal/video/popularity_cache.go#L12)、[feed/service.go 第 428 行](../backend/internal/feed/service.go#L428)

固定 T0 的历史数据不会自动生成“现在”的 Redis 热榜；仅导入数据库测到的是回退路径，不能标成 Redis 热榜基线。静态数据 Hash 保持不变，运行期桶名和 TTL 按固定相对时间重建，另记运行参数。

第一版主数据不带 #标签，tags/video_tags 可为空；标签场景另定义同规模变体，使用固定标签词表并同步导入关联。基础离线快照中的 Outbox 保持为空，表示历史发布已完成，不伪造 pending 消息；后续真正测试发布接口时再验证视频、Outbox、标签和时间线的完整副作用。通知、私信、评论不计入四类规模指标。[video_service.go 第 49 行](../backend/internal/video/video_service.go#L49)、[feed/repo.go 第 105 行](../backend/internal/feed/repo.go#L105)

## 5. 生成后的完整性验收计划

以下只描述后续验收，不包含本次执行。优先使用只读查询与离线清单对比；登录、Feed 请求会写 Token/缓存，属于后续另行安排的功能验收步骤，不称为纯只读数据库核验。

| 验收项 | 通过标准 | 依据 |
|---|---|---|
| 表结构 | 只读读取实际列/索引/约束；与第 1 节对照，记录精度、时区、排序规则、自增 | Model 和 db.AutoMigrate |
| 总量与主键 | U/V/F/L 精确匹配；ID 非零唯一；序列范围与清单一致 | 四类 Model |
| 用户唯一性及字段 | 按实际排序规则用户名唯一；固定测试密码能校验 Hash；字段不超长 | account/entity.go、service.go:33/113 |
| 视频作者 | author_id 全部可关联 accounts；username 与生成时作者名一致；标题、URL 非空 | video_handler.go:30；feed/service.go:535 |
| 关注关联 | 两端账号存在；无自关注；(follower_id,vlogger_id) 无重复；出度配额精确 | social/entity.go:5、service.go:23 |
| 点赞关联 | 用户和视频存在；(video_id,account_id) 无重复；本方案无自赞 | like_entity.go:5、like_service.go:31 |
| 计数一致性 | 每视频 likes_count=实际 likes 数，含零赞；总 likes_count=L；基础快照 popularity 同计数 | like_service.go:31、likeworker.go:105 |
| 时间 | 视频/点赞不晚于 T0；点赞不早于发布；分桶配额正确；毫秒唯一性符合版本 | video_entity.go、feed/handler.go |
| 分布 | 作者比例/视频产量精确；关注入度和点赞分组实际值、分位数、最大值及回填偏差入清单 | 第 4 节设计参数 |
| 内容摘要 | 规范化排序流 SHA-256 与清单一致，Token 等运行字段单独排除 | 第 4.2 节设计约定 |
| 缓存引用 | 时间线/热榜所有成员均对应有效视频；score 单位、Top1000 和桶分数正确 | outboxworker.go:49、popularity_cache.go:12 |
| 副表及异步状态 | 基础快照无 pending Outbox、无旧消息；标签变体无孤儿或重复关联 | video_service.go:49、outboxworker.go:17 |

功能抽检清单须固定：三类作者、无关注/重度关注用户、零赞/爆款视频、已有点赞/未点赞组合、冷热边界游标，以及至少一个超过 200 条列表上限的作者。Feed 的作者、is_liked、计数与独立数据库结果交叉比对；计数接口总量不要求等于被截断的列表长度。

分页在静态数据下分别校验排序、重复、遗漏和游标单位。对时间游标并列导致的遗漏输出独立缺陷记录，不能把接口现有问题判成数据损坏。点赞数和热度回退分页按完整组合游标验证。[feed/repo.go 第 20、33、52、72 行](../backend/internal/feed/repo.go#L20)

## 6. 安全重置方案（仅设计，不执行）

推荐在独立测试环境使用专属 MySQL 测试库、Redis 实例、RabbitMQ vhost/实例及上传目录；不修改模型、不混入开发库数据。Redis 仅换 DB 不能隔离 MQ，RabbitMQ vhost 也不能通过现有 Config 直接指定（连接 URL 固定为 `/`）；若保持现有配置代码，应优先使用独立 RabbitMQ 实例。隔离是测试环境安排，不是业务架构改造。[rabbitMQ.go](../backend/internal/middleware/rabbitmq/rabbitMQ.go)、[cmd/worker/main.go](../backend/cmd/worker/main.go)

后续执行时的顺序：

1. 校验目标主机、端口、库名、卷和测试清单指纹，拒绝默认落到非测试 feedsystem；保存可恢复快照，明确快照涵盖的副表和文件。
2. 在已批准的维护窗口暂停测试请求和全部相关生产者/消费者，包括 API 内的 Outbox、时间线和通知任务。先排空或隔离旧 MQ 消息，并确认无在途写入；不能只停独立 Worker 就清库。[http/router.go 第 198 行](../backend/internal/http/router.go#L198)
3. 仅恢复/替换专属测试环境快照。若必须逐表清理，按依赖从子表到主表规划：通知/私信/评论、video_tags、Outbox、likes、socials，再 videos、tags、accounts；结合实际外键 DDL调整，禁止只清四张表后留下副表孤儿。重置自增状态或恢复清单中的显式 ID，使再次生成具有相同映射。
4. 同步恢复或清理测试专属 Redis、MQ 队列/死信队列和上传文件。不能把清理范围仅限定为 `v1:*`：限流键是 `feedsystem:ratelimit:*`；共享 Redis 禁止全库清空。若必须保留共享服务，先确认完整 Key 清单和精确命名空间，否则不执行清理。[redis/redis.go](../backend/internal/middleware/redis/redis.go)、[ratelimit/ratelimit.go](../backend/internal/middleware/ratelimit/ratelimit.go)
5. API/Worker 恢复后再次校验表结构、计数、队列积压和缓存状态，再执行选定的预热流程；API 启动 AutoMigrate 可能改变快照后的结构，必须纳入检查。
6. 在计时之前为固定账号池登录并保存最新 Token，避免重复登录覆盖同一账号的 Token。访问 Token 有效期为 15 分钟，运行前检查剩余有效期；长测试单独规划刷新，不能把过期 401 当成容量失败。Token 使用数据库当前值及 Redis 校验，单独生成一个签名串并不足以完成鉴权。[account/service.go 第 113 行](../backend/internal/account/service.go#L113)、[auth/jwt.go 第 42 行](../backend/internal/auth/jwt.go#L42)、[middleware/jwt/jwt.go 第 91 行](../backend/internal/middleware/jwt/jwt.go#L91)

共享环境的定向删除容易遗漏异步消息和缓存，不作为第一版推荐方案。本报告不提供或执行任何清库、停服或数据写入命令。

## 7. 后续待确认事项

建议先选择小档验证生成器与完整性检查，再依次扩展中、大档。仍需确认实际机器资源、采用哪档数据、测试请求比例与并发，以及热榜/标签变体是否进入首轮。数据生成器、校验器、重置脚本和测试 Token 池均尚未实现；本次仅交付设计报告，不开始造数或压测。
