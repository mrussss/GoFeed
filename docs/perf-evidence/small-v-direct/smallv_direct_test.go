package worker

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/feed"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"

	driver "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const smallVSeed = 20261123
const smallVViewer = uint(100)
const smallVVideos = 120
const smallVFollowers = 500
const smallVShared = 100

func smallVMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func smallVMS(value time.Duration) float64 { return float64(value) / float64(time.Millisecond) }

func smallVCPU(t *testing.T) float64 {
	t.Helper()
	var usage syscall.Rusage
	smallVMust(t, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func smallVClients(t *testing.T) (*driver.Config, *sql.DB, *redis.Client) {
	t.Helper()
	cfg, err := driver.ParseDSN(os.Getenv("SMALLV_MYSQL_DSN"))
	smallVMust(t, err)
	host, _, err := net.SplitHostPort(cfg.Addr)
	smallVMust(t, err)
	if cfg.DBName != "" || cfg.Net != "tcp" || host != "127.0.0.1" {
		t.Fatal("SMALLV_MYSQL_DSN must target a disposable loopback admin instance without a schema")
	}
	addr := os.Getenv("SMALLV_REDIS_ADDR")
	host, _, err = net.SplitHostPort(addr)
	smallVMust(t, err)
	if host != "127.0.0.1" {
		t.Fatal("SMALLV_REDIS_ADDR must point to a disposable loopback instance")
	}
	cfg.ParseTime, cfg.Loc, cfg.Timeout = true, time.UTC, 3*time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	smallVMust(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	smallVMust(t, admin.Ping())
	raw := redis.NewClient(&redis.Options{Addr: addr, DB: 15, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = raw.Close() })
	smallVMust(t, raw.Ping(context.Background()).Err())
	return cfg, admin, raw
}

func TestSmallVEnvironment(t *testing.T) {
	_, mysql, raw := smallVClients(t)
	ctx := context.Background()
	var version string
	smallVMust(t, mysql.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version))
	info, err := raw.Info(ctx, "server").Result()
	smallVMust(t, err)
	t.Logf("Go=%s runtime CPUs=%d MySQL=%s Redis=%s", runtime.Version(), runtime.NumCPU(), version, smallVInfo(info)["redis_version"])
	appendOnly, err := raw.ConfigGet(ctx, "appendonly").Result()
	smallVMust(t, err)
	t.Logf("Redis appendonly=%s; experimental clients use DB 15", appendOnly["appendonly"])
}

type smallVPipeline struct {
	times    []float64
	commands int64
	failures int
	timeouts int
}

type smallVSQLTimer struct {
	logger.Interface
	nanos atomic.Int64
	calls atomic.Int64
}

func (timer *smallVSQLTimer) Trace(_ context.Context, start time.Time, _ func() (string, int64), _ error) {
	timer.nanos.Add(time.Since(start).Nanoseconds())
	timer.calls.Add(1)
}

func (*smallVPipeline) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (*smallVPipeline) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (p *smallVPipeline) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		if len(commands) == 0 || commands[0].Name() != "zrevrangebylex" {
			return next(ctx, commands)
		}
		start := time.Now()
		err := next(ctx, commands)
		p.times = append(p.times, smallVMS(time.Since(start)))
		p.commands += int64(len(commands))
		if err != nil {
			p.failures++
			var networkError net.Error
			if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkError) && networkError.Timeout()) {
				p.timeouts++
			}
		}
		return err
	}
}

func smallVInfo(info string) map[string]string {
	fields := make(map[string]string)
	for _, line := range strings.Split(info, "\r\n") {
		if key, value, ok := strings.Cut(line, ":"); ok {
			fields[key] = value
		}
	}
	return fields
}

type smallVCommandStats struct{ calls, usec int64 }

func smallVRedisStats(t *testing.T, ctx context.Context, raw *redis.Client) map[string]smallVCommandStats {
	t.Helper()
	res, err := raw.Info(ctx, "commandstats").Result()
	smallVMust(t, err)
	info := smallVInfo(res)
	stats := make(map[string]smallVCommandStats)
	for _, command := range []string{"zadd", "zremrangebyrank", "zrevrangebylex"} {
		values := make(map[string]int64)
		for _, part := range strings.Split(info["cmdstat_"+command], ",") {
			name, value, ok := strings.Cut(part, "=")
			if ok && (name == "calls" || name == "usec") {
				values[name], err = strconv.ParseInt(value, 10, 64)
				smallVMust(t, err)
			}
		}
		stats[command] = smallVCommandStats{values["calls"], values["usec"]}
	}
	return stats
}

func smallVSQLStats(t *testing.T, ctx context.Context, admin *sql.DB) map[string]int64 {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `SHOW GLOBAL STATUS WHERE Variable_name IN
		('Com_select','Com_insert','Com_update','Com_begin','Com_commit','Com_rollback')`)
	smallVMust(t, err)
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var name string
		var count int64
		smallVMust(t, rows.Scan(&name, &count))
		result[strings.ToLower(name)] = count
	}
	smallVMust(t, rows.Err())
	if len(result) != 6 {
		t.Fatal("missing MySQL statement counters")
	}
	return result
}

func smallVSum(values []float64) float64 {
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum
}

func smallVPercentile(values []float64, p int) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	return ordered[(len(ordered)*p+99)/100-1]
}

func smallVResult(t *testing.T, tag string, values map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(values)
	smallVMust(t, err)
	t.Logf("%s %s", tag, encoded)
}

type smallVEvent struct {
	video               video.Video
	publishMS, fanoutMS float64
}

func smallVEventsCSV(t *testing.T, events []smallVEvent) {
	t.Helper()
	out, err := os.Create(os.Getenv("SMALLV_EVENTS"))
	smallVMust(t, err)
	defer out.Close()
	writer := csv.NewWriter(out)
	smallVMust(t, writer.Write([]string{"ordinal", "video_id", "author_id", "create_time_ms", "publish_ms", "fanout_ms"}))
	for i, event := range events {
		smallVMust(t, writer.Write([]string{strconv.Itoa(i + 1), fmt.Sprint(event.video.ID), fmt.Sprint(event.video.AuthorID),
			fmt.Sprint(event.video.CreateTime.UnixMilli()), strconv.FormatFloat(event.publishMS, 'f', 6, 64),
			strconv.FormatFloat(event.fanoutMS, 'f', 6, 64)}))
	}
	writer.Flush()
	smallVMust(t, writer.Error())
}

func TestSmallVDirect(t *testing.T) {
	k, err := strconv.Atoi(os.Getenv("SMALLV_K"))
	smallVMust(t, err)
	repeat, err := strconv.Atoi(os.Getenv("SMALLV_REPEAT"))
	smallVMust(t, err)
	mode := video.DistributionMode(os.Getenv("SMALLV_MODE"))
	if (k != 2 && k != 100) || (repeat != 1 && repeat != 2) || (mode != video.DistributionPush && mode != video.DistributionPull) {
		t.Fatal("invalid fixed matrix cell")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg, admin, raw := smallVClients(t)
	schema := fmt.Sprintf("gofeed_smallv_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+schema)
	smallVMust(t, err)
	t.Cleanup(func() { _, err := admin.Exec("DROP DATABASE " + schema); smallVMust(t, err) })
	cfg.DBName = schema
	db, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	smallVMust(t, err)
	pool, err := db.DB()
	smallVMust(t, err)
	t.Cleanup(func() { _ = pool.Close() })
	pool.SetMaxOpenConns(16)
	pool.SetMaxIdleConns(8)
	smallVMust(t, db.AutoMigrate(&account.Account{}, &social.Social{}, &video.Video{}, &video.AuthorFeedState{},
		&video.OutboxMsg{}, &video.Tag{}, &video.VideoTag{}, &video.Like{}))
	cache := rediscache.NewClient(raw, schema+":")
	t.Cleanup(func() { smallVMust(t, cache.DelByPattern(context.Background(), cache.Key("*"))) })
	authors, followers := smallVGraph(t, db, k)
	rng := rand.New(rand.NewSource(smallVSeed))
	order := rng.Perm(smallVVideos)
	events := make([]smallVEvent, smallVVideos)
	for i, n := range order {
		author := authors[n%k]
		events[i].video = video.Video{ID: uint(600001 + i), AuthorID: author, Username: fmt.Sprintf("account-%d", author), Title: "small V fixture",
			PlayURL: "/play", CoverURL: "/cover", CreateTime: time.UnixMilli(1_794_012_300_123 + int64(n/4)).UTC()}
	}

	publish := video.NewVideoService(video.NewVideoRepository(db), cache)
	publishCtx := video.SmallVPublishContext(ctx, mode)
	prepareSQL := smallVSQLStats(t, ctx, admin)
	writeCPUStart := smallVCPU(t)
	publishTime, fanoutTime := make([]float64, len(events)), make([]float64, len(events))
	for i := range events {
		event := &events[i]
		start := time.Now()
		err := publish.Publish(publishCtx, &event.video)
		event.publishMS = smallVMS(time.Since(start))
		if err != nil {
			t.Fatalf("publish event %d: %v", i+1, err)
		}
		if event.video.DistributionMode == nil || *event.video.DistributionMode != mode {
			t.Fatalf("publish event %d: incorrect strategy snapshot", i+1)
		}
		publishTime[i] = event.publishMS
	}
	publishCPU := smallVCPU(t) - writeCPUStart
	publishSQL := smallVSQLStats(t, ctx, admin)

	var outbox []video.OutboxMsg
	smallVMust(t, db.Where("event_type = ? AND status = ?", "video_published", "pending").Order("id").Find(&outbox).Error)
	if len(outbox) != len(events) {
		t.Fatalf("got %d pending snapshots", len(outbox))
	}
	fanoutRepo := social.NewSocialRepository(db)
	fanoutConfig := config.DefaultFollowingFanoutConfig()
	if fanoutConfig.InboxMaxLen != 1000 || fanoutConfig.AuthorOutboxMaxLen != 500 {
		t.Fatal("unexpected production index limits")
	}
	redisBefore := smallVRedisStats(t, ctx, raw)
	fanoutSQLStart := smallVSQLStats(t, ctx, admin)
	fanoutCPUStart := smallVCPU(t)
	for i, row := range outbox {
		event, err := timelineEvent(row)
		smallVMust(t, err)
		if event.VideoID != events[i].video.ID || event.DistributionMode != string(mode) {
			t.Fatalf("outbox event %d does not match the published snapshot", i+1)
		}
		start := time.Now()
		err = writeFollowingCandidates(ctx, fanoutRepo, cache, fanoutConfig, event)
		events[i].fanoutMS = smallVMS(time.Since(start))
		fanoutTime[i] = events[i].fanoutMS
		if err != nil {
			t.Fatalf("fanout event %d, zero retries: %v", i+1, err)
		}
	}
	fanoutCPU := smallVCPU(t) - fanoutCPUStart
	fanoutSQL := smallVSQLStats(t, ctx, admin)
	redisAfter := smallVRedisStats(t, ctx, raw)
	smallVEventsCSV(t, events)
	keys, members, mem := smallVAuditIndex(t, ctx, raw, cache, mode, followers, events)
	zadd := redisAfter["zadd"].calls - redisBefore["zadd"].calls
	ztrim := redisAfter["zremrangebyrank"].calls - redisBefore["zremrangebyrank"].calls
	if zadd != int64(members) || ztrim != int64(members) {
		t.Fatalf("unexpected Redis command count: ZADD=%d trim=%d expected each=%d", zadd, ztrim, members)
	}
	smallVResult(t, "SMALLV_WRITE", map[string]any{
		"authors": k, "mode": mode, "repeat": repeat, "published": len(events), "fanout_completed": len(events),
		"index_keys": keys, "index_members": members, "index_bytes": mem, "zadd": zadd, "zremrangebyrank": ztrim,
		"redis_write_usec": redisAfter["zadd"].usec - redisBefore["zadd"].usec + redisAfter["zremrangebyrank"].usec - redisBefore["zremrangebyrank"].usec,
		"publish_p50_ms":   smallVPercentile(publishTime, 50), "publish_total_ms": smallVSum(publishTime), "publish_go_cpu_s": publishCPU,
		"fanout_p50_ms": smallVPercentile(fanoutTime, 50), "fanout_p95_ms": smallVPercentile(fanoutTime, 95), "fanout_total_ms": smallVSum(fanoutTime), "fanout_go_cpu_s": fanoutCPU,
		"publish_selects": publishSQL["com_select"] - prepareSQL["com_select"], "publish_inserts": publishSQL["com_insert"] - prepareSQL["com_insert"],
		"publish_updates": publishSQL["com_update"] - prepareSQL["com_update"], "publish_commits": publishSQL["com_commit"] - prepareSQL["com_commit"],
		"outbox_snapshot_fetch_selects": fanoutSQLStart["com_select"] - publishSQL["com_select"],
		"fanout_selects":                fanoutSQL["com_select"] - fanoutSQLStart["com_select"], "fanout_errors": 0, "fanout_application_retries": 0,
	})
	smallVRead(t, ctx, db, admin, raw, cache, k, repeat, mode)
}

func smallVGraph(t *testing.T, db *gorm.DB, k int) ([]uint, map[uint][]uint) {
	t.Helper()
	authors := make([]uint, k)
	followers := make(map[uint][]uint, k)
	var accounts []account.Account
	addAccount := func(id uint) {
		accounts = append(accounts, account.Account{ID: id, Username: fmt.Sprintf("account-%d", id)})
	}
	for j := 0; j < smallVShared; j++ {
		addAccount(smallVViewer + uint(j))
	}
	links := make([]social.Social, 0, k*smallVFollowers)
	for i := range authors {
		id := uint(9001 + i)
		authors[i] = id
		addAccount(id)
		for j := 0; j < smallVFollowers; j++ {
			follower := smallVViewer + uint(j)
			if j >= smallVShared {
				follower = uint(20000 + i*smallVFollowers + j)
				addAccount(follower)
			}
			followers[id] = append(followers[id], follower)
			links = append(links, social.Social{FollowerID: follower, VloggerID: id})
		}
	}
	smallVMust(t, db.CreateInBatches(accounts, 500).Error)
	smallVMust(t, db.CreateInBatches(links, 500).Error)
	var counts []struct {
		VloggerID uint
		Followers int
	}
	smallVMust(t, db.Raw("SELECT vlogger_id, COUNT(*) AS followers FROM socials GROUP BY vlogger_id").Scan(&counts).Error)
	if len(counts) != k {
		t.Fatal("wrong author count")
	}
	for _, row := range counts {
		if row.Followers != smallVFollowers {
			t.Fatalf("author %d has %d followers", row.VloggerID, row.Followers)
		}
	}
	return authors, followers
}

type smallVIndexCheck struct {
	members *redis.ZSliceCmd
	memory  *redis.IntCmd
	key     string
}

func smallVAuditIndex(t *testing.T, ctx context.Context, raw *redis.Client, cache *rediscache.Client, mode video.DistributionMode, followers map[uint][]uint, events []smallVEvent) (int, int, int64) {
	t.Helper()
	expected := make(map[string][]string)
	for _, entry := range events {
		row := entry.video
		member, err := feed.FollowingMember(row.CreateTime.UnixMilli(), row.ID)
		smallVMust(t, err)
		if mode == video.DistributionPull {
			key := cache.Key("feed:author_outbox:%d", row.AuthorID)
			expected[key] = append(expected[key], member)
			continue
		}
		for _, follower := range followers[row.AuthorID] {
			key := cache.Key("feed:inbox:%d", follower)
			expected[key] = append(expected[key], member)
		}
	}
	keyCount := 0
	var cursor uint64
	for {
		keys, next, err := raw.Scan(ctx, cursor, cache.Key("feed:*"), 1000).Result()
		smallVMust(t, err)
		for _, key := range keys {
			if _, ok := expected[key]; !ok {
				t.Fatalf("unwanted candidate key: %s", key)
			}
			keyCount++
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if keyCount != len(expected) {
		t.Fatalf("found %d keys, expected %d", keyCount, len(expected))
	}
	memberCount := 0
	var memory int64
	pipeline := raw.Pipeline()
	checks := make([]smallVIndexCheck, 0, 400)
	checkBatch := func() {
		_, err := pipeline.Exec(ctx)
		smallVMust(t, err)
		for _, check := range checks {
			want := expected[check.key]
			slices.Sort(want)
			list := check.members.Val()
			if len(list) != len(want) {
				t.Fatalf("index %s: got %d entries, want %d", check.key, len(list), len(want))
			}
			for i, z := range list {
				if z.Score != 0 || z.Member != want[i] {
					t.Fatalf("index %s differs at offset %d", check.key, i)
				}
			}
			memberCount += len(list)
			memory += check.memory.Val()
		}
		checks = checks[:0]
	}
	for key := range expected {
		checks = append(checks, smallVIndexCheck{pipeline.ZRangeWithScores(ctx, key, 0, -1), pipeline.MemoryUsage(ctx, key), key})
		if len(checks) == cap(checks) {
			checkBatch()
		}
	}
	if len(checks) > 0 {
		checkBatch()
	}
	t.Logf("index gate: exact keys=%d members=%d; bytes=%d", keyCount, memberCount, memory)
	return keyCount, memberCount, memory
}

type smallVOrdered struct {
	ID         uint
	CreateTime time.Time
	Liked      bool
}

func smallVRead(t *testing.T, ctx context.Context, db *gorm.DB, admin *sql.DB, raw *redis.Client, cache *rediscache.Client, k, repeat int, mode video.DistributionMode) {
	t.Helper()
	const size = 10
	const repeats = 200
	var likes []video.Like
	for i := 0; i < smallVVideos; i += 13 {
		likes = append(likes, video.Like{VideoID: uint(600001 + i), AccountID: smallVViewer, Status: 1})
	}
	smallVMust(t, db.Create(&likes).Error)
	var ordered []smallVOrdered
	smallVMust(t, db.Raw(`SELECT v.id, v.create_time, EXISTS(
		SELECT 1 FROM likes l WHERE l.video_id=v.id AND l.account_id=? AND l.status=1
	) AS liked FROM videos v
	WHERE EXISTS (SELECT 1 FROM socials s WHERE s.follower_id=? AND s.vlogger_id=v.author_id)
	ORDER BY v.create_time DESC, v.id DESC`, smallVViewer, smallVViewer).Scan(&ordered).Error)
	if len(ordered) != smallVVideos {
		t.Fatalf("oracle got %d videos", len(ordered))
	}
	probe := &smallVPipeline{times: make([]float64, 0, repeats)}
	raw.AddHook(probe)
	fallbacks := 0
	readCtx := feed.SmallVReadContext(ctx, string(mode), func() { fallbacks++ })
	queryTimer := &smallVSQLTimer{Interface: logger.Default.LogMode(logger.Silent)}
	observed := db.Session(&gorm.Session{Logger: queryTimer})
	service := feed.NewFeedService(feed.NewFeedRepository(observed), video.NewLikeRepository(observed), social.NewSocialRepository(observed), cache)
	verify := func(page feed.ListByFollowingResponse, offset int) bool {
		end := min(offset+size, len(ordered))
		expected := ordered[offset:end]
		if len(expected) != len(page.VideoList) || page.HasMore != (end < len(ordered)) {
			return false
		}
		for i, v := range expected {
			if page.VideoList[i].ID != v.ID || page.VideoList[i].IsLiked != v.Liked {
				return false
			}
		}
		if len(expected) == 0 {
			return page.NextIDBefore == 0 && page.NextTime == 0
		}
		last := expected[len(expected)-1]
		return page.NextIDBefore == last.ID && page.NextTime == last.CreateTime.UnixMilli()
	}
	var cursor *feed.TimeCursor
	var pageCursors [3]*feed.TimeCursor
	for offset := 0; offset <= len(ordered); offset += size {
		if offset/size < len(pageCursors) {
			pageCursors[offset/size] = cursor
		}
		page, err := service.ListByFollowing(readCtx, size, cursor, smallVViewer)
		smallVMust(t, err)
		if !verify(page, offset) {
			t.Fatalf("pagination gate differs from SQL at offset %d", offset)
		}
		cursor = &feed.TimeCursor{ID: page.NextIDBefore, Time: time.UnixMilli(page.NextTime).UTC()}
	}
	wantCommands := k
	if mode == video.DistributionPush {
		wantCommands = 1
	}
	probe.commands = 0
	probe.failures, probe.timeouts = 0, 0
	probe.times = probe.times[:0]
	for i := 0; i < 30; i++ {
		p := i % 3
		page, err := service.ListByFollowing(readCtx, size, pageCursors[p], smallVViewer)
		smallVMust(t, err)
		if !verify(page, p*size) {
			t.Fatalf("warmup failed at request %d", i+1)
		}
	}
	if probe.commands != int64(30*wantCommands) {
		t.Fatal("warmup command count differs from configured read path")
	}
	t.Logf("read gate: complete 12-page chain plus empty page; 30 warm requests; %d candidate commands/request", wantCommands)

	probe.commands = 0
	probe.failures, probe.timeouts, fallbacks = 0, 0, 0
	probe.times = probe.times[:0]
	sequence := rand.New(rand.NewSource(smallVSeed)).Perm(repeats)
	times := make([]float64, 0, repeats)
	readErrors, contractErrors := 0, 0
	queryTimer.nanos.Store(0)
	queryTimer.calls.Store(0)
	beforeRedis := smallVRedisStats(t, ctx, raw)
	beforeSQL := smallVSQLStats(t, ctx, admin)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	startCPU := smallVCPU(t)
	start := time.Now()
	for _, i := range sequence {
		p := i % 3
		call := time.Now()
		result, err := service.ListByFollowing(readCtx, size, pageCursors[p], smallVViewer)
		times = append(times, smallVMS(time.Since(call)))
		if err != nil {
			readErrors++
			t.Logf("read request %d page %d: %v", len(times), p+1, err)
		} else if !verify(result, p*size) {
			contractErrors++
			t.Logf("read request %d page %d differs from oracle", len(times), p+1)
		}
	}
	elapsed := time.Since(start).Seconds()
	cpu := smallVCPU(t) - startCPU
	runtime.ReadMemStats(&after)
	afterSQL := smallVSQLStats(t, ctx, admin)
	afterRedis := smallVRedisStats(t, ctx, raw)
	readStats := afterRedis["zrevrangebylex"]
	startStats := beforeRedis["zrevrangebylex"]
	pipelineMean := 0.0
	if len(probe.times) > 0 {
		pipelineMean = smallVSum(probe.times) / float64(len(probe.times))
	}
	n := float64(repeats)
	smallVResult(t, "SMALLV_READ", map[string]any{
		"authors": k, "mode": mode, "repeat": repeat, "requests": repeats, "successes": repeats - readErrors - contractErrors, "wall_s": elapsed,
		"service_mean_ms": smallVSum(times) / n, "service_p50_ms": smallVPercentile(times, 50), "service_p95_ms": smallVPercentile(times, 95),
		"candidate_commands": probe.commands, "candidate_commands_per_request": float64(probe.commands) / n,
		"pipeline_calls": len(probe.times), "pipeline_mean_ms": pipelineMean, "pipeline_p95_ms": smallVPercentile(probe.times, 95),
		"pipeline_errors": probe.failures, "pipeline_timeouts": probe.timeouts, "timeout_rate_per_request": float64(probe.timeouts) / n,
		"mysql_selects": afterSQL["com_select"] - beforeSQL["com_select"], "mysql_selects_per_request": float64(afterSQL["com_select"]-beforeSQL["com_select"]) / n,
		"mysql_gorm_trace_ms_per_request": float64(queryTimer.nanos.Load()) / float64(time.Millisecond) / n,
		"mysql_gorm_trace_calls":          queryTimer.calls.Load(),
		"mysql_fallbacks":                 fallbacks, "mysql_fallback_rate": float64(fallbacks) / n,
		"redis_server_read_calls": readStats.calls - startStats.calls, "redis_server_read_usec": readStats.usec - startStats.usec,
		"success_per_s": float64(repeats-readErrors-contractErrors) / elapsed, "go_alloc_kib_per_request": float64(after.TotalAlloc-before.TotalAlloc) / 1024 / n,
		"go_cpu_pct": 100 * cpu / elapsed, "go_cpu_us_per_request": cpu * 1e6 / n, "read_errors": readErrors, "contract_errors": contractErrors,
	})
	if contractErrors != 0 || readErrors != 0 {
		t.Fatalf("read gate: service errors=%d, contract errors=%d", readErrors, contractErrors)
	}
	if probe.failures == 0 && (probe.commands != int64(repeats*wantCommands) || probe.commands != readStats.calls-startStats.calls) {
		t.Fatalf("unexpected candidate commands: client %d, server %d", probe.commands, readStats.calls-startStats.calls)
	}
}
