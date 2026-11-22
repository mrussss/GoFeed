# GoFeed 基线测试数据集设计方案 v2

日期：2026-11-16。范围：源码分析和测试数据方案设计。

本次未生成实际数据，未连接数据库执行读写，未运行测试，未启动或停止 Docker，未执行 Git 操作。本次仅新增完整 v2 报告，保留第一版；未修改业务代码或其他文件。

本文的“源码事实”来自现有模型、迁移、Repository 和业务逻辑；“建议方案”是供后续确认的测试参数，不代表项目真实用户分布。未检查实际数据库 DDL，因此物理列类型、索引和历史迁移结果仍需后续只读核验。

证据分类：**[源码事实]** 表示已从代码核实（不等于已运行验证）；**[待 DDL]** 表示必须读取真实数据库后才能确认；**[建议]** 表示测试设计。第 1～3 节中的 Go 字段、标签与调用路径为源码事实，数据库实际落地类型和约束为待 DDL；第 4～6 节为建议。第 7～11 节是本轮补充与精化，其中算法细则替代第一版模糊约定。所有性能或缺陷预期均尚未实测。

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

- 方案版本 `gofeed-baseline-dataset-v2`，根随机种子 `20261116`；规模、作者产量、关注出度比例和点赞分组比例保持第一版。
- v2 固定整数权重、SHA-256 计数器随机流、加权无放回抽样和确定性图分配；具体字节编码、冲突处理与规范化摘要见第 11 节。禁止依赖浮点近似排序、map 遍历顺序或并行任务完成顺序。
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

必须采用无放回目标选择，排除自己；每对最多一条，每作者入度不超过可用非零出度用户数（排除自己）。v2 按第 11 节将组配额设为硬约束、作者 Zipf 入度设为目标，通过确定性残量图处理冲突；约束不可行则明确失败，不静默改变比例或少生成。小档只有 2 位大 V，5% 总边数为 1000，未超过 1998 的理论容量，避免不可能的热点配额。

预留并固定以下样本：无关注、少量关注、重度关注、关注普通作者、关注大 V、互相关注。关注仅指向作者是本方案选择，业务允许关注无视频用户。[关联规则：social/service.go 第 23 行](../backend/internal/social/service.go#L23)

### 4.5 发布时间

所有作者的视频混合分配到四个互不重叠时间区间：最近 1 小时 10%；1 小时至 1 天 20%；1 天至 7 天 30%；7 天至 180 天 40%。边界统一采用半开区间并在清单列明，T0 本身不安排视频。各区间按确定性配额分派至作者，再在区间内无放回抽取毫秒时间点，保证主数据集全局 CreateTime 在毫秒级唯一，并打乱时间与视频 ID 的对应。

该分布同时保留近期热点和足够冷数据。大档最近一小时有 10 万条视频，仍可在 360 万个毫秒槽中安排；不要要求秒级唯一，否则无法满足该规模。

数据库时间精度需先核验；若不足毫秒，不能默默截断后宣称时间唯一。即使毫秒唯一，关注 Feed 的秒级游标仍可能漏掉边界秒内记录：作为现有接口局限记录，不通过删除数据掩盖。另定义第 8 节的 `same-second` 变体，并保留 `same-timestamp` 边界变体：在相同总量内选择固定 100 条视频置于同一毫秒，单独验收分页行为，不混入主性能结果。[时间游标：feed/handler.go 第 19、90 行](../backend/internal/feed/handler.go#L19)、[严格小于条件：feed/repo.go 第 20、52 行](../backend/internal/feed/repo.go#L20)

### 4.6 点赞分布与计数

- 固定 20% 视频为零赞；其余视频划分为爆款（全部视频前 1%）、热门（随后 9%）、普通有赞（70%），分别分配 L 的 30%、40%、30%。用第 11 节固定的作者类型权重决定热点排序，并保留普通作者爆款。
- 各组内使用截断 Zipf 权重 `r^-0.8` 分配整数点赞配额：采用最大余数法补足总数，平分时按 ID 排序；每视频上限 U-1，超额转给同组剩余容量。分配前要求配额之和不超过容量；最终必须精确达到 L。
- 点赞用户从该视频作者之外的用户中无放回选择，使用固定用户活跃权重（10% 高活跃权重 5，其余权重 1）。不要求先关注；固定留出已赞/未赞请求样本。该第一版不保证零点赞用户比例，不额外声称每用户恰好 100 赞。
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

## 7. 实际数据库 DDL 核验准备

**[待 DDL] 当前没有真实 DDL，以下全部是检查清单，不是检查结果。** 不根据 GORM 标签推断“数据库已经有某个索引/外键”。先核对服务地址、库名及表名；只读采集四表的 SHOW CREATE TABLE、SHOW INDEX，以及 information_schema 的 COLUMNS、STATISTICS、TABLE_CONSTRAINTS、KEY_COLUMN_USAGE、REFERENTIAL_CONSTRAINTS。记录数据库版本、表引擎、字符集/排序规则、SQL_MODE、全局/会话时区和采集时间。当前不连接数据库，也不执行这些查询。

| 核心表（预期名称） | 逐字段核验 | 时间精度 | 索引和约束重点 |
|---|---|---|---|
| accounts | id、username、password、token、refresh_token、avatar_url、bio 的实际 COLUMN_TYPE、可空性、默认值、长度、自增、字符集和排序规则 | 模型无时间字段；检查历史库有无额外列 | PRIMARY(id)、username 的实际唯一约束及前缀长度；是否存在历史外键/触发器 |
| videos | id、author_id、username、title、description、play_url、cover_url、create_time、likes_count、popularity 的类型、unsigned、默认值、长度及可空性 | create_time 实际是 DATETIME/TIMESTAMP 或其他类型、DATETIME_PRECISION、自动默认/更新表达式 | author_id 索引；时间/点赞/热度索引的实际列顺序、升降序、唯一性、可见性；两个名称带 id 的索引是否真的显式包含 id；author_id 外键及动作 |
| socials | id、follower_id、vlogger_id 的类型、unsigned、可空性、默认值和自增 | 模型无时间字段；检查是否多出时间列 | 联合唯一索引的两列及顺序、两个单列索引；两端对 accounts 的实际外键及 ON DELETE/UPDATE |
| likes | id、video_id、account_id、created_at 的类型、unsigned、可空性、默认值和自增 | created_at 类型、DATETIME_PRECISION、默认值/自动维护 | 联合唯一索引(video_id,account_id)；有无 account_id 单列索引；对视频/账号的实际外键及动作 |

验收产物为每表“模型声明 / 实际 DDL / 差异 / 对造数与测试的影响”四列表。当前实际 DDL 一律填写“未取得”，不能填写“无外键”“毫秒精度已通过”等结果。

时间精度验收门槛：DATETIME_PRECISION 至少为 3 才能承载毫秒用例；还要检查实际导入序列化和读取是否保留毫秒。若不满足，将毫秒用例标记为环境前置条件未满足，先记录差异并协商处理；本次不修改表结构，也不把截断后的数据冒充原用例。已有列的 NULL/default/unsigned 与模型不一致时同样先记录，不自行修库。[源码基准：四类 Model，第 1 节；迁移 db.go 第 26 行](../backend/internal/db/db.go#L26)

## 8. 两类时间边界数据与分页验收

**[建议]** 保留主数据集，并分别产生独立的 `same-second`、`same-timestamp` 变体，不增加 U/V/F/L，不改变作者视频产量、关注出度或点赞配额。每个变体从同一基础清单派生，独立计算摘要，不把两类结果混在主性能数据里。

### 8.1 固定夹具

在最近一小时分桶中选定 `S=T0-10分钟`（整秒），按 ID 升序选择某位大 V 的 100 条既有视频作为夹具；关注测试者固定选一位已关注该作者的非作者用户（取满足条件的最小 ID）。若基础图不能满足，生成失败，而不是临时补关注边。选择满足最近桶配额的作者与视频，采用同桶时间置换保留总量和时间区间配额；变体中相关点赞时间重新按同一规则分配到发布之后，点赞关系及计数不变。

| 变体 | 100 条视频时间 | 用途 |
|---|---|---|
| same-second | 第 i 条（i=0..99）为 S + (100+i×8) 毫秒，即 .100～.892 | 同一秒、不同毫秒，验证秒级截断 |
| same-timestamp | 全部为 S + 500 毫秒 | 保留同一毫秒测试，验证仅时间游标的并列边界 |

选定的整秒从基础生成时即保留给夹具，不放无关视频；主数据集可跳过该秒，并从同桶其他毫秒槽补齐，规模和分桶比例不变。夹具视频在主数据集中仍有正常的不同时间。夹具变体最近 1000 条边界不能恰好切在夹具中；否则另记边界切分用例。若所选大 V 在最近桶不足 100 条，应在第 11 节作者/时间槽分配时将此作为预先声明的最低配额，而不是生成后碰运气。

测试时初始游标设为 `S+1秒`：listLatest 传该值的 Unix 毫秒，listByFollowing 传 Unix 秒，使夹具位于首批结果。固定 limit=10，沿服务端返回的 next_time 翻页，不用 Feed 项的秒级 create_time 替代毫秒游标。对 limit=1、50 另作边界复核。关注场景的期望集合仅包括测试者关注作者的视频，不能拿全局视频集合比较。

### 8.2 验证矩阵及预期

三类请求分别运行第 9 节场景；listLatest 还分别运行 Redis 热路径、无 Redis/错误回退的数据库路径、跨最近 1000 条冷热边界路径。这里定义场景，不在本次变更服务或 Redis。

| 数据 | listLatest 源码推断 | listByFollowing 源码推断 |
|---|---|---|
| 同一秒不同毫秒 | 毫秒游标可区分这些视频，静态数据下应无重复无遗漏 | next_time 向下截为整秒；下一页条件 create_time < S，可能跳过该秒未返回的夹具视频 |
| 同一毫秒 | 下一页排除整个末尾毫秒，未返回的并列视频可能遗漏 | 秒级截断同样可能遗漏 |

**[源码事实]** 数据库最新/关注查询只按 create_time 排序并严格 `<` 过滤，没有 ID 次级游标；Redis 最新查询下一页 score 上限为 reqTime-1。以上只能推断风险，不能填入“实测遗漏 90 条”等结果。同分 Redis 成员按字符串顺序处理，不能把它当成数值 ID 排序；数据库并列顺序也不保证相同。[feed/repo.go 第 20、52 行](../backend/internal/feed/repo.go#L20)、[feed/service.go 第 224、273、338 行](../backend/internal/feed/service.go#L224)

**[建议]** 每轮在静止快照中记录每页游标、ID、有序原始响应、请求路径和缓存状态。令 E 为初始游标以下的完整期望 ID 集合，R 为所有页返回序列：重复数=`len(R)-len(set(R))`；遗漏=`E-set(R)`；越界/多余=`set(R)-E`。同时核对夹具 100 个 ID 的集合。记录非递减游标、空页仍 has_more、重复页和请求失败；设置固定页数上限 `ceil(V/limit)+2` 防止死循环。终止依据为响应终止条件加审计上限，绝不能把 has_more=false 等同于完整无遗漏。并列时不强求 DB 与 Redis 页内顺序一致，但必须比较 ID 集合和遗漏。冷热拼接还要检查同页和跨页重复，任何预期之外的重复都保留证据，不靠离线去重掩盖。

## 9. 三种 Feed 读取场景独立统计

| 场景 | 请求身份 | 视频筛选 | is_liked 路径 | 验收 |
|---|---|---|---|---|
| A：匿名 listLatest | 不发送 Authorization | 全局最新 | viewerAccountID=0；BatchGetLiked 提前返回空 map，不执行 likes SQL | 所有项 false；不包含鉴权与点赞 SQL 成本 |
| B：登录 listLatest | 固定测试用户的有效 Bearer Token | 全局最新 | 鉴权后以用户 ID 批量查询 `video_id IN ? AND account_id = ?` | 同一静态页的视频集合与 A 一致，is_liked 与该用户实际点赞集合一致 |
| C：登录 listByFollowing | 固定有效 Token | 只筛选当前用户关注的作者 | 未命中关注 Feed 整页缓存时，构建响应会调用相同点赞批查；缓存命中直接返回已有 is_liked | 作者范围、点赞状态、秒级分页分别验收 |

**[源码事实]** A/B 共用 SoftJWTAuth；B 仍有 JWT 校验以及 Redis/数据库 Token 检查。C 还经过 JWTAuth；外层 SoftJWTAuth 也会处理携带的 Token。不能将 B-C 的成本差简单归结为关注关系查询。[router.go 第 169 行](../backend/internal/http/router.go#L169)、[jwt.go 第 42、69 行](../backend/internal/middleware/jwt/jwt.go#L42)

**[源码事实]** 最新 Feed 即使实体缓存已热，buildFeedVideos 仍批量读取登录用户的点赞状态；关注 Feed 的整页缓存则会跳过这一构建过程。关注整页缓存键包含账号、limit 和秒级 before，缓存结果含 is_liked。[like_repo.go 第 65 行](../backend/internal/video/like_repo.go#L65)、[feed/service.go 第 338、535 行](../backend/internal/feed/service.go#L338)

**[建议]** B、C 各至少固定一位已点赞夹具视频的用户和一位未点赞用户。先由最终关系清单选择已具备条件的用户，不额外改变关系数；找不到则验收失败。A/B/C 分开记录吞吐、分位延迟、错误率、鉴权冷热状态及关注整页缓存命中状态。缓存已热后再点赞的可见性属于独立正确性场景；静态基线不夹杂写入，避免把整页缓存旧状态误判为数据生成错误。

## 10. 离线导入后的 Redis 时间线初始化

### 10.1 已核实的现有路径

**[源码事实]** 实际 Key 为默认前缀下的 `v1:feed:global_timeline`。[redis.go 第 21、27 行](../backend/internal/middleware/redis/redis.go#L21)

1. 离线导入 videos 不调用 Publish，也不产生 pending Outbox；API 的 OutboxPoller 只查 pending 消息，因此不会扫描全部历史视频为时间线补数据。[video_service.go 第 49 行](../backend/internal/video/video_service.go#L49)、[outboxworker.go 第 17 行](../backend/internal/worker/outboxworker.go#L17)
2. 首次 listLatest 若 Redis 客户端为空，或读取 ZSET 尾部报错，直接走数据库，不重建。[feed/service.go 第 149 行](../backend/internal/feed/service.go#L149)
3. Redis 可用且 ZSET 为空时，通过进程内 singleflight 合并重建请求，忽略调用方游标，从 MySQL 取最新最多 1000 条，执行 ZAdd，member 为十进制视频 ID，score 为 CreateTime.UnixMilli()，之后递归重查；数据库无视频则返回空结果。这个路径不依赖 MQ 或 Outbox。[feed/service.go 第 163 行](../backend/internal/feed/service.go#L163)、[feed/repo.go 第 20 行](../backend/internal/feed/repo.go#L20)
4. ZSET 非空时不会全量校验或补齐。历史导入发生在已有非空时间线后，仅靠 listLatest 不保证新导入记录可见；冷数据查询不会回填时间线。listByFollowing、listLikesCount、listByPopularity 也不调用这个重建入口。[feed/service.go 第 202、313、338、427 行](../backend/internal/feed/service.go#L202)
5. 正常发布经 Outbox → TimelineMQ → API 内 TimelineConsumer 写入时间线，并裁剪到最近 1000 条。[router.go 第 198 行](../backend/internal/http/router.go#L198)、[timelineMQ.go 第 44 行](../backend/internal/middleware/rabbitmq/timelineMQ.go#L44)、[outboxworker.go 第 91 行](../backend/internal/worker/outboxworker.go#L91)
6. 重建时 ZAdd 返回错误未检查，singleflight 也只在单个 API 进程内生效；若持续写失败且读仍为空，存在递归重试风险。该代码不是具有失败闭环的初始化工具；本次只记录，不修改业务逻辑。[feed/service.go 第 178～199 行](../backend/internal/feed/service.go#L178)

结论：**现有 listLatest 能有条件自动重建空 ZSET，不能承诺自动修复非空残缺时间线。** 历史视频离线导入后，不必为了首次初始化伪造 Outbox 或重新发布全部视频。

### 10.2 建议预热流程及工具边界

**[建议]** 测试专属 Redis 已确认为空且数据静止时，先执行一条不计时的匿名 listLatest，再只读核验 ZCARD、全部 member/score 和 MySQL/清单 Top1000；确认成功后再按固定请求清单预热实体缓存。首次重建耗时单独归档。该流程是后续计划，本次未执行。

非空残缺、重复导入、重建失败或需要精确初始化时，未来测试专用工具应：

- 校验目标仅为测试环境，记录数据库快照摘要、Redis DB、前缀、Key 与版本；确认无并发发布/导入/时间线消费覆盖结果。
- 读取最多 1000 条预期历史视频，主数据集按 create_time DESC（毫秒唯一）；边界变体明确采用 `create_time DESC,id DESC` 作为工具选集规则，并记录其与业务仅时间排序的差异。不要把所有历史视频灌入 ZSET。
- 以十进制 ID 为 member、Unix 毫秒为 score 构建临时测试 Key；检查每次写入错误，再原子替换目标 Key，避免残留旧成员或暴露半成品。相同输入重复运行应得到完全相同的成员和分数，无须生成任何业务消息。
- 读回核验 `min(V,1000)`、成员存在性、分数、排序边界和无多余成员；失败立即停止预热并保留原因。工具不假装一次 listLatest 成功就代表初始化完成。
- 时间线无显式 TTL；工具不要擅自加 TTL。实体缓存、关注整页缓存和热榜需按各自计划准备，不能因时间线已初始化就宣称全缓存已热。

以上工具只操作测试初始化状态，不添加业务 API、不替换 Repository、不改变发布链路。当前未实现工具，也未执行 Redis 写入。若恰好第 1000 条有并列时间，工具选集与业务自动重建可能不同；须单列记录，不据此伪造一致性结论。

## 11. 确定性生成算法细则（新增建议）

本节把原方案的分布要求落实为可编码规则。算法尚未实现、运行或计算期望摘要；“可复现”是下面规范和后续验收的要求，不是本次已经完成造数验证。

### 11.1 整数随机流与排序

定义 `frame(parts)`：每个参数先按规定转成 UTF-8 字符串，前置 8 字节大端无符号长度，再拼接；整数用无前导零十进制，禁止区域格式。阶段种子为 `SHA256(frame(["gofeed-baseline-dataset-v2","20261116",scale,variant,stage,objectID]))`；无对象时 objectID 为 `"0"`。流第 c 个块为 `SHA256(seed || uint64_be(c))`，c 从 0 开始，按无符号 256 位大端数读取。

取 `[0,n)` 均匀整数：令 M=2^256，拒绝 x>=M-(M mod n) 的块，再返回 x mod n。整数运算使用足够宽度/任意精度；n 必须为正且小于 M。所有阶段使用固定名称，例如 roles、outdegree、author-rank、video-rank、follow-order、like-users、video-time、like-time；每用户/每视频使用自身 ID 的独立流，不能共享受线程调度影响的随机状态。

基础规模生成使用 variant=`base`；两个分页变体从基础结果派生，仅时间变换使用自身 variant 子流，保证其他关系不变。随机排列采用 Fisher–Yates：输入 ID 升序，i 从末尾递减到 1，以 uniform(i+1) 交换。任何排名平分都按数值 ID 升序，字符串不作为数值 ID 比较器。

### 11.2 固定整数 Zipf 权重、整数配额

为避免浮点 pow/log 的平台差异，排名 r>=1 的 Zipf 权重固定为：`W(r)=floor((floor(2^120/r^4))^(1/5))`，五次根用整数二分精确求下取整。它是约 `2^24*r^-0.8` 的整数版本；现有规模内为正数。排名本身使用上节固定种子的排列，不把低 ID 天然当头部。

定义有容量上限的整数配额函数 `quota(Q,weights,caps)`：

1. 检查 Q>=0、各权重为正、Q<=sum(caps)。从零配额开始。
2. 对当前剩余额 R 和未饱和集合 A，精确计算 `R*w_i / sum(w_A)`。如果某项比例不小于其剩余容量，将所有这类项同时填满、扣减 R 并移出 A，再重算；饱和判定用整数交叉乘法。
3. 无饱和项时，先分配各比例的 floor；余下不足 |A| 的单位按除法余数降序、ID 升序逐项补 1。结束后验证总和 Q 和所有上限。

三个作者类型配额、五种关注出度人数、四个时间桶视频数、四个点赞组视频数及三个非零点赞组点赞总额都用该规则；本方案规模下比例本身整除。作者/时间桶的二维分配先放置夹具最低配额，再用确定性整数运输分配满足每作者总量和每桶总量，禁止各作者独立四舍五入破坏总数。节点按作者 ID、时间桶从近到远排序，同可行解按此顺序选择；推荐复用下节的确定性增广实现。

### 11.3 加权无放回抽样

统一采用整数逐次轮盘算法，不使用未限定实现的“weighted random”或浮点指数键：候选 ID 升序放入权重数组，计算剩余总权重 S；取 uniform(S)，选首个累计权重大于该值的候选；输出后将其权重置零，重复 k 次。可用 Fenwick 树加速，但必须与该顺序语义完全相同。先检查 k 不超过正权候选数。排除自己、已选、预留禁选项都在计算 S 之前执行。

该过程给出按剩余权重比例逐次选择的无放回抽样。关注图附加出入度约束后会有确定性修复，因此最终图不声称仍等于独立无约束抽样分布；最终以度数、配额和实际长尾统计验收。

### 11.4 关注出度、作者入度及冲突修复

1. 对全部用户做 outdegree 阶段排列，依次分为 10%/50%/30%/9%/1%，分别赋 d(u)=0/10/30/50/150。作者类型由独立 roles 排列选 20% 作者再分 90%/9%/1%；因此作者身份和出度分组不绑定。校验 sum(d)=F。
2. 非零出度用户集合为 P。每作者有效容量 `cap(a)=|P|-1[a∈P]`，而不只是宽松的 U-1。目标类型总入度 G 为 70%F/25%F/5%F；组内按 author-rank 的 W(r) 与 cap(a) 计算目标 q(a)。不足容量直接失败；不能默默把大 V 关注挪给普通作者。
3. 为每个用户用 follow-order 子流、作者 W(r) 创建加权无放回候选全排列，排除自己。它是图分配优先级，惰性生成或分页存储均可，但不能按线程完成顺序拼接。可以先用前若干候选构图，再按固定轮次、用户 ID 和候选顺序扩展；最终可访问全部合法候选，不能因初始稀疏候选不足就认定全图不可行。
4. 构造容量网络：source→用户容量 d(u)，用户→作者容量 1（无自边），作者→类型节点容量 q(a)，类型→sink 容量 G(type)。用固定顺序的 Dinic：节点 source、用户 ID 升序、作者 ID 升序、普通/中型/大 V、sink；邻接边按创建顺序，候选边按上述排列，反向边立即登记；BFS FIFO、DFS 当前弧顺序，直到无增广路。不使用不确定的集合遍历。若流量 F，即精确满足目标入度和出度。
5. 如果全候选图仍无法同时满足 q(a)，保持 d(u)、G 和无自关注/无重复为硬约束，作者目标放宽为 cap(a)，先保留已有流，再添加作者到同类型节点的溢出容量 cap(a)-q(a)，按作者 ID 升序登记并继续同样的残量增广。残量反向边可将先前选择重新分配，避免贪心填满导致死路。记录每作者实际入度、与 q(a) 的差及修复次数；不修改类型份额。若总流仍<F，则生成失败，不导出“完成”数据。
6. 最终边按 `(follower_id,vlogger_id)` 数值升序分配主键。校验每个用户出度、各组入度总额、所有作者容量与唯一性。记录入度分位数及三类人均入度；“目标 Zipf”与“约束修复后的实际分布”必须分开报告。

这是测试生成器的图分配算法，不是新增业务关注架构。大档完整候选边很多，实现时需评估内存并采用确定性的惰性扩展；任何性能优化必须与参考顺序输出一致，否则提升算法版本。当前仅定义算法，不承诺生成速度。

### 11.5 点赞、时间及冲突规则

- 视频按作者类型设置热点选择权重：普通=1、中型=2、大 V=4；按 video-rank 加权无放回全排列，前 1% 为爆款、随后 9% 热门、再 70% 普通有赞、末 20% 零赞。若爆款无普通作者，用排名最前的普通作者视频与爆款末位交换，交换规则入清单，组大小不变。未满足可行候选条件则失败。
- 每个非零组先给每视频 1 赞，再对剩余组点赞总量用 W(r) 和剩余容量 U-2 执行 quota；这样普通有赞组不被整数取整变成零赞。三组总额保持 30%L/40%L/30%L，逐视频总上限 U-1。容量不足失败，不改比例。
- 用独立排列选固定 10% 高活跃用户，权重 5，其余 1。每视频排除作者后，用第 11.3 节抽取所需唯一用户，绝不靠“碰到重复就跳过”减少条数。最终按 `(video_id,account_id)` 排序分配主键。
- 时间桶定义为 `[T0-1h,T0)`、`[T0-1d,T0-1h)`、`[T0-7d,T0-1d)`、`[T0-180d,T0-7d)`。视频按 ID 升序，从所属桶的可用整数毫秒槽中均匀无放回选择（可使用均匀权重的第 11.3 节，或严格等价的稀疏 Fisher–Yates）；排除夹具保留秒。每次只用该桶的独立随机流，不能以数据库当前时间填空。
- Like.CreatedAt 用每个 `(video_id,account_id)` 独立对象流，从 `[video_ms,T0_ms)` 均匀抽取整数毫秒，允许不同点赞时间重合。对象 ID 编码为 `videoID:accountID`。变体修改发布时刻后重算该视频点赞时间，且只影响该变体时间摘要。
- 热榜相对运行时间的清单单独摘要。运行期 Token、真实系统时间、MQ 随机事件 ID 不进入静态数据摘要；不能把这些非确定字段误算后再宣称摘要不稳定不可避免。

### 11.6 规范化 SHA-256 与确定性验收

为消除 JSON map 顺序、SQL 导出格式、换行和时区差异，定义规范化记录流 `canonical-v2`：

1. 输出固定 ASCII 头 `GOFEED-DATASET-V2` 加一个 LF；随后用 frame 编码版本、种子、scale、variant、T0、所有分布/权重参数、算法标识、冻结密码 Hash 和媒体资源 SHA-256。配置字段顺序固定写入 schema，禁止 map 编码；冻结常量未取得前不能发布正式数据摘要。
2. 表顺序固定 accounts、videos、socials、likes、tags、video_tags、outbox_msgs；后三级主快照为空，仍编码表名和行数。每表行数用 8 字节大端编码，每行按数值主键升序输出，各列按以下固定顺序使用 frame，字符串保持 UTF-8 原值，不 Trim、不自动 Unicode 归一化；所有整数无前导零，时间统一 UTC `YYYY-MM-DDTHH:mm:ss.ffffffZ`（生成值毫秒对齐，后三位为零）。
3. accounts 列顺序：id,username,password,token,refresh_token,avatar_url,bio；静态 token/refresh_token 均为空。videos：id,author_id,username,title,description,play_url,cover_url,create_time,likes_count,popularity。socials：id,follower_id,vlogger_id。likes：id,video_id,account_id,created_at。未来非空标签变体固定 tags 为 id,name，video_tags 为 id,video_id,tag_id；Outbox 主快照强制为空。
4. 基础数据中字符串均用空字符串而非 NULL，不靠数据库默认值补齐；出现意外 NULL 判验收失败，不能悄悄转空掩盖差异。实际库回读先恢复上述逻辑类型，再按相同规范编码；DDL 精度不足时失败，不输出“相同摘要”结论。数据内容摘要不混入采集时间、机器路径、批次号或数据库物理布局。
5. 对整个字节流计算 SHA-256；同时保存每表摘要、行数、可选固定 10000 行分块摘要用于定位差异。参数 Header 进入全局摘要；每表摘要只覆盖该表的表名、行数和记录。派生作者角色/权重排名/修复日志单独形成按 ID 排序的审计清单及摘要。
6. 后续实现验收必须运行“同参数独立生成两次”“不同并行度/导入批量”“导入后只读回算”比较计数和摘要；前两者必须一致，第三者在静态字段未变时必须一致。换 scale、variant、算法或冻结资源常量应产生新的全局摘要。当前未生成任何数据或期望 Hash，不能填写一个虚构的 SHA-256 值。

相同规范、参数和冻结常量下，随机流、配额、候选次序、残量增广及序列化均有固定顺序，因此设计上得到相同数量和规范化摘要；是否实现正确，仍须以上重复生成验收证明。

## 12. 后续待确认事项

建议先选择小档验证生成器与完整性检查，再依次扩展中、大档。仍需确认实际机器资源、采用哪档数据、测试请求比例与并发，以及热榜/标签变体是否进入首轮。数据生成器、校验器、重置脚本和测试 Token 池均尚未实现；本次仅交付设计报告，不开始造数或压测。
