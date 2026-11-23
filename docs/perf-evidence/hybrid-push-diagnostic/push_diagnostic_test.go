package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const diagnosticSeed = 20261122
const diagnosticStamp = int64(1794012300123)

type diagnosticRow struct {
	ID         uint
	CreateTime time.Time
}

type candidateProbe struct {
	calls, commands, failures, timeouts atomic.Int64
	durations                           []float64
}

func (*candidateProbe) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (*candidateProbe) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (p *candidateProbe) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if len(cmds) == 0 || cmds[0].Name() != "zrevrangebylex" {
			return next(ctx, cmds)
		}
		start := time.Now()
		err := next(ctx, cmds)
		p.durations = append(p.durations, diagMS(time.Since(start)))
		p.calls.Add(1)
		p.commands.Add(int64(len(cmds)))
		if err != nil {
			p.failures.Add(1)
			if ctx.Err() == context.DeadlineExceeded {
				p.timeouts.Add(1)
			} else if e, ok := err.(interface{ Timeout() bool }); ok && e.Timeout() {
				p.timeouts.Add(1)
			}
		}
		return err
	}
}

func diagMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func diagMySQLOrder(t *testing.T, db *gorm.DB, viewer uint) []diagnosticRow {
	t.Helper()
	var ordered []diagnosticRow
	diagMust(t, db.Raw(`SELECT v.id, v.create_time FROM videos v WHERE EXISTS (
		SELECT 1 FROM socials s WHERE s.follower_id = ? AND s.vlogger_id = v.author_id
	) ORDER BY v.create_time DESC, v.id DESC`, viewer).Scan(&ordered).Error)
	return ordered
}

func diagCheck(page ListByFollowingResponse, expected []diagnosticRow, hasMore bool) bool {
	if len(page.VideoList) != len(expected) || page.HasMore != hasMore {
		return false
	}
	for i, row := range expected {
		if row.ID != page.VideoList[i].ID {
			return false
		}
	}
	if len(expected) == 0 {
		return page.NextIDBefore == 0 && page.NextTime == 0
	}
	last := expected[len(expected)-1]
	return page.NextIDBefore == last.ID && page.NextTime == last.CreateTime.UnixMilli()
}

func diagCursor(row diagnosticRow) *TimeCursor {
	return &TimeCursor{ID: row.ID, Time: row.CreateTime}
}

func diagRedis(t *testing.T, ctx context.Context) (*redis.Client, *rediscache.Client) {
	t.Helper()
	addr := os.Getenv("C_TEST_REDIS_ADDR")
	if addr == "" {
		t.Fatal("C_TEST_REDIS_ADDR must point to an isolated Redis")
	}
	raw := redis.NewClient(&redis.Options{Addr: addr, DB: 15, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = raw.Close() })
	diagMust(t, raw.Ping(ctx).Err())
	cache := rediscache.NewClient(raw, fmt.Sprintf("gofeed-push-diagnostic:%d:", time.Now().UnixNano()))
	t.Cleanup(func() { diagMust(t, cache.DelByPattern(context.Background(), cache.Key("*"))) })
	return raw, cache
}

func diagFill(t *testing.T, ctx context.Context, raw *redis.Client, key string, rows []video.Video) {
	t.Helper()
	candidates := make([]redis.Z, 0, len(rows))
	for _, v := range rows {
		candidates = append(candidates, timelineZ(t, v.CreateTime, v.ID))
	}
	if len(candidates) != 0 {
		diagMust(t, raw.ZAdd(ctx, key, candidates...).Err())
	}
}

func TestPushDiagnosticRound(t *testing.T) {
	k, err := strconv.Atoi(os.Getenv("DIAGNOSTIC_K"))
	diagMust(t, err)
	if k != 2 && k != 50 && k != 500 {
		t.Fatal("only K=2,50,500 supported")
	}
	variant := os.Getenv("DIAGNOSTIC_VARIANT")
	repeat, err := strconv.Atoi(os.Getenv("DIAGNOSTIC_REPEAT"))
	diagMust(t, err)
	if variant != "current" && variant != "inbox-only" {
		t.Fatal("unknown diagnostic variant")
	}
	if repeat != 1 && repeat != 2 {
		t.Fatal("two attempts per configuration")
	}
	out := os.Getenv("DIAGNOSTIC_OUTPUT")
	if out == "" {
		t.Fatal("DIAGNOSTIC_OUTPUT is required")
	}
	ctx := context.Background()
	db := readTestMySQL(t)
	pool, err := db.DB()
	diagMust(t, err)
	pool.SetMaxOpenConns(16)
	pool.SetMaxIdleConns(8)
	raw, cache := diagRedis(t, ctx)

	const viewer, firstAuthor, firstVideo = 480001, 481001, 485001
	rng := rand.New(rand.NewSource(diagnosticSeed))
	push := video.DistributionPush
	var authors []social.Social
	var states []video.AuthorFeedState
	var emptyKeys []string
	for _, index := range rng.Perm(k) {
		id := uint(firstAuthor + index)
		authors = append(authors, social.Social{FollowerID: viewer, VloggerID: id})
		states = append(states, video.AuthorFeedState{AuthorID: id, DistributionMode: push})
		emptyKeys = append(emptyKeys, cache.Key("feed:author_outbox:%d", id))
	}
	videos := make([]video.Video, 600)
	order := rng.Perm(len(videos))
	for i := range videos {
		videos[i] = video.Video{ID: uint(firstVideo + i), AuthorID: authors[order[i]%k].VloggerID,
			CreateTime: time.UnixMilli(diagnosticStamp + int64(order[i]/4)).UTC(), Username: "fixture",
			Title: "Following diagnostic", PlayURL: "/play", CoverURL: "/cover", DistributionMode: &push}
	}
	diagMust(t, db.Transaction(func(tx *gorm.DB) error {
		if err := tx.CreateInBatches(authors, 500).Error; err != nil {
			return err
		}
		if err := tx.CreateInBatches(states, 500).Error; err != nil {
			return err
		}
		return tx.CreateInBatches(videos, 500).Error
	}))
	t.Cleanup(func() {
		diagMust(t, db.Where("follower_id = ?", viewer).Delete(&social.Social{}).Error)
		diagMust(t, db.Where("id BETWEEN ? AND ?", firstVideo, firstVideo+len(videos)-1).Delete(&video.Video{}).Error)
		diagMust(t, db.Where("author_id BETWEEN ? AND ?", firstAuthor, firstAuthor+k-1).Delete(&video.AuthorFeedState{}).Error)
	})
	inbox := cache.Key("feed:inbox:%d", viewer)
	diagFill(t, ctx, raw, inbox, videos)
	outboxes, err := raw.Exists(ctx, emptyKeys...).Result()
	diagMust(t, err)
	ordered := diagMySQLOrder(t, db, viewer)
	members, err := raw.ZRevRange(ctx, inbox, 0, -1).Result()
	diagMust(t, err)
	if outboxes != 0 || len(ordered) != len(videos) || len(members) != len(videos) {
		t.Fatal("fixture must contain the complete Inbox and no Author Outbox")
	}
	for i, row := range ordered {
		expected, err := FollowingMember(row.CreateTime.UnixMilli(), row.ID)
		diagMust(t, err)
		if members[i] != expected {
			t.Fatalf("inbox mismatch at rank %d", i)
		}
	}

	probe := new(candidateProbe)
	raw.AddHook(probe)
	var queries atomic.Int64
	diagMust(t, db.Callback().Query().After("gorm:query").Register("diagnostic:query", func(*gorm.DB) { queries.Add(1) }))
	diagMust(t, db.Callback().Row().After("gorm:row").Register("diagnostic:row", func(*gorm.DB) { queries.Add(1) }))
	service := NewFeedService(NewFeedRepository(db), video.NewLikeRepository(db), social.NewSocialRepository(db), cache)
	var keyNS, buildNS time.Duration
	choice := diagnosticSelection{observeKeys: func(d time.Duration) { keyNS += d }}
	if variant == "inbox-only" {
		choice.include = map[uint]bool{}
	}
	ctx = context.WithValue(ctx, diagnosticSelectionKey{}, choice)
	ctx = context.WithValue(ctx, rediscache.DiagnosticBuildProbeKey{}, func(d time.Duration) { buildNS += d })

	const size, pages = 10, 3
	var cursors [pages]*TimeCursor
	for i := 1; i < pages; i++ {
		cursors[i] = diagCursor(ordered[size*i-1])
	}
	request := func(i int) bool {
		page, err := service.ListByFollowing(ctx, size, cursors[i], viewer)
		return err == nil && diagCheck(page, ordered[size*i:size*(i+1)], true)
	}
	for i := 0; i < 30; i++ {
		if !request(i % pages) {
			t.Fatalf("warmup page %d differs from MySQL oracle", i%pages+1)
		}
	}
	wantCommands := k + 1
	if variant == "inbox-only" {
		wantCommands = 1
	}
	if probe.calls.Load() != 30 || probe.commands.Load() != int64(30*wantCommands) || probe.failures.Load() != 0 {
		t.Fatalf("warmup candidate path: calls=%d commands=%d errors=%d, want 30×%d and zero errors",
			probe.calls.Load(), probe.commands.Load(), probe.failures.Load(), wantCommands)
	}
	probe.calls.Store(0)
	probe.commands.Store(0)
	probe.failures.Store(0)
	probe.timeouts.Store(0)
	probe.durations = make([]float64, 0, 12000)
	queries.Store(0)
	keyNS, buildNS = 0, 0

	diagHeap(t, out+".alloc-before.pb.gz")
	cpu, err := os.Create(out + ".cpu.pb.gz")
	diagMust(t, err)
	diagMust(t, pprof.StartCPUProfile(cpu))
	beforeRedis := diagRedisSnapshot(t, ctx, raw)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuStart := scaleCPU(t)
	durations := make([]float64, 0, 12000)
	bad := 0
	start := time.Now()
	for time.Since(start) < 4*time.Second {
		call := time.Now()
		i := len(durations) % pages
		page, err := service.ListByFollowing(ctx, size, cursors[i], viewer)
		durations = append(durations, diagMS(time.Since(call)))
		if err != nil || !diagCheck(page, ordered[size*i:size*(i+1)], true) {
			bad++
		}
	}
	elapsed := time.Since(start)
	cpuSeconds := scaleCPU(t) - cpuStart
	runtime.ReadMemStats(&after)
	n := float64(len(durations))
	selects := queries.Load()
	afterRedis := diagRedisSnapshot(t, ctx, raw)
	pprof.StopCPUProfile()
	diagMust(t, cpu.Close())
	diagHeap(t, out+".alloc-after.pb.gz")
	slices.Sort(durations)
	slices.Sort(probe.durations)
	percentile := func(values []float64, p int) float64 { return values[(len(values)*p+99)/100-1] }
	var pipelineSum float64
	for _, d := range probe.durations {
		pipelineSum += d
	}
	result := map[string]any{
		"authors": k, "variant": variant, "repeat": repeat, "requests": int(n), "wall_s": elapsed.Seconds(),
		"redis_candidate_commands": probe.commands.Load(), "redis_commands_per_request": float64(probe.commands.Load()) / n,
		"pipeline_calls": probe.calls.Load(), "pipeline_mean_ms": pipelineSum / float64(probe.calls.Load()),
		"pipeline_p95_ms": percentile(probe.durations, 95), "pipeline_failures": probe.failures.Load(), "pipeline_timeouts": probe.timeouts.Load(),
		"key_build_us_per_request":                   float64(keyNS.Microseconds()) / n,
		"cmd_build_us_per_request":                   float64(buildNS.Microseconds()) / n,
		"redis_server_zrevrangebylex_calls":          afterRedis.calls - beforeRedis.calls,
		"redis_server_zrevrangebylex_us_per_request": float64(afterRedis.commandUS-beforeRedis.commandUS) / n,
		"redis_input_bytes_per_request":              float64(afterRedis.input-beforeRedis.input) / n,
		"redis_output_bytes_per_request":             float64(afterRedis.output-beforeRedis.output) / n,
		"service_p50_ms":                             percentile(durations, 50), "service_p95_ms": percentile(durations, 95),
		"success_per_s": (n - float64(bad)) / elapsed.Seconds(), "sql_per_request": float64(selects) / n,
		"go_alloc_kib_per_request": float64(after.TotalAlloc-before.TotalAlloc) / 1024 / n,
		"go_cpu_pct":               cpuSeconds / elapsed.Seconds() * 100, "go_cpu_us_per_request": cpuSeconds * 1e6 / n, "response_errors": bad,
	}
	encoded, err := json.Marshal(result)
	diagMust(t, err)
	t.Logf("DIAGNOSTIC_RESULT %s", encoded)
	if bad != 0 || probe.commands.Load() != int64(len(durations)*wantCommands) ||
		afterRedis.calls-beforeRedis.calls != probe.commands.Load() {
		t.Fatalf("correctness/command gate failed: bad=%d, client commands=%d, server commands=%d",
			bad, probe.commands.Load(), afterRedis.calls-beforeRedis.calls)
	}
}

func diagMS(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func diagHeap(t *testing.T, path string) {
	t.Helper()
	runtime.GC()
	out, err := os.Create(path)
	diagMust(t, err)
	diagMust(t, pprof.WriteHeapProfile(out))
	diagMust(t, out.Close())
}

type diagnosticRedisSnapshot struct{ calls, commandUS, input, output int64 }

func diagRedisSnapshot(t *testing.T, ctx context.Context, raw *redis.Client) diagnosticRedisSnapshot {
	t.Helper()
	stats, err := raw.Info(ctx, "stats", "commandstats").Result()
	diagMust(t, err)
	values := make(map[string]string)
	for _, line := range strings.Split(stats, "\r\n") {
		field, value, ok := strings.Cut(line, ":")
		if ok {
			values[field] = value
		}
	}
	parse := func(s string) int64 { val, err := strconv.ParseInt(s, 10, 64); diagMust(t, err); return val }
	snapshot := diagnosticRedisSnapshot{input: parse(values["total_net_input_bytes"]), output: parse(values["total_net_output_bytes"])}
	command := values["cmdstat_zrevrangebylex"]
	if command != "" {
		for _, field := range strings.Split(command, ",") {
			name, count, ok := strings.Cut(field, "=")
			if ok && name == "calls" {
				snapshot.calls = parse(count)
			}
			if ok && name == "usec" {
				snapshot.commandUS = parse(count)
			}
		}
	}
	return snapshot
}

func TestPushDiagnosticHistory(t *testing.T) {
	db := readTestMySQL(t)
	tx := db.Begin()
	diagMust(t, tx.Error)
	defer tx.Rollback()
	ctx := context.Background()
	raw, cache := diagRedis(t, ctx)
	const viewer, a, b, c = 490001, 490002, 490003, 490004
	push, pull := video.DistributionPush, video.DistributionPull
	diagMust(t, tx.Create(&[]video.AuthorFeedState{
		{AuthorID: a, DistributionMode: push}, {AuthorID: b, DistributionMode: push}, {AuthorID: c, DistributionMode: pull},
	}).Error)
	diagMust(t, tx.Create(&[]social.Social{
		{FollowerID: viewer, VloggerID: a}, {FollowerID: viewer, VloggerID: b}, {FollowerID: viewer, VloggerID: c},
	}).Error)
	stamp := time.UnixMilli(diagnosticStamp).UTC()
	item := func(id, author uint, at time.Time, mode *video.DistributionMode) video.Video {
		return video.Video{ID: id, AuthorID: author, CreateTime: at, DistributionMode: mode}
	}
	a1, a2 := item(490020, a, stamp, &push), item(490025, a, stamp.Add(time.Millisecond), &push)
	bOld, bNew := item(490022, b, stamp, &pull), item(490026, b, stamp.Add(time.Millisecond), &push)
	c1, c2 := item(490021, c, stamp, &pull), item(490024, c, stamp.Add(time.Millisecond), &pull)
	rows := []video.Video{a1, a2, bOld, bNew, c1, c2}
	diagMust(t, tx.Create(&rows).Error)
	diagFill(t, ctx, raw, cache.Key("feed:inbox:%d", viewer), []video.Video{a2, bNew, a1, c1})
	diagFill(t, ctx, raw, cache.Key("feed:author_outbox:%d", b), []video.Video{bOld})
	diagFill(t, ctx, raw, cache.Key("feed:author_outbox:%d", c), []video.Video{c1, c2})
	probe := new(candidateProbe)
	raw.AddHook(probe)
	service := NewFeedService(NewFeedRepository(tx), video.NewLikeRepository(tx), social.NewSocialRepository(tx), cache)
	diagnostic := func(set map[uint]bool) context.Context {
		return context.WithValue(ctx, diagnosticSelectionKey{}, diagnosticSelection{include: set})
	}
	current := ctx
	selected := diagnostic(map[uint]bool{b: true, c: true})
	// Walk to the final empty page using only response-generated cursors.
	checkAll := func(stage string, requestCtx context.Context) {
		t.Helper()
		oracle := diagMySQLOrder(t, tx, viewer)
		before := probe.commands.Load()
		var cursor *TimeCursor
		var readIDs []uint
		const size = 3
		for start := 0; start <= len(oracle); start += size {
			page, err := service.ListByFollowing(requestCtx, size, cursor, viewer)
			diagMust(t, err)
			end := min(start+size, len(oracle))
			if !diagCheck(page, oracle[start:end], end < len(oracle)) {
				t.Fatalf("%s: cursor %v, got %v, expected %v", stage, cursor, feedIDs(page.VideoList), oracle[start:end])
			}
			readIDs = append(readIDs, feedIDs(page.VideoList)...)
			if len(page.VideoList) == 0 {
				break
			}
			cursor = &TimeCursor{ID: page.NextIDBefore, Time: time.UnixMilli(page.NextTime).UTC()}
			if end == len(oracle) && end%size != 0 {
				break
			}
		}
		t.Logf("%s: IDs=%v, candidate commands=%d", stage, readIDs, probe.commands.Load()-before)
	}
	checkAll("current", current)
	checkAll("history-aware", selected)
	// Five remaining candidates fill limit+1, so MySQL fill does not mask
	// the missing historical row when current mode is the only signal.
	page, err := service.ListByFollowing(diagnostic(map[uint]bool{c: true}), 4, nil, viewer)
	diagMust(t, err)
	oracle := diagMySQLOrder(t, tx, viewer)
	if oracle[3].ID != bOld.ID || !page.HasMore || len(page.VideoList) != 4 || slices.Contains(feedIDs(page.VideoList), bOld.ID) {
		t.Fatal("negative control did not demonstrate the consequence of omitting B")
	}
	t.Logf("current-mode-only first page: IDs=%v, historical B=%d omitted", feedIDs(page.VideoList), bOld.ID)
	// Leave stale Redis members to exercise ordinary existence/follow checks.
	diagMust(t, tx.Delete(&video.Video{}, bOld.ID).Error)
	diagMust(t, tx.Where("follower_id = ? AND vlogger_id = ?", viewer, c).Delete(&social.Social{}).Error)
	checkAll("current after deletion/unfollow", current)
	checkAll("history-aware after deletion/unfollow", selected)
}
