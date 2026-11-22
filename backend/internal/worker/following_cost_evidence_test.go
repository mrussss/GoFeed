package worker_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/config"
	appdb "feedsystem_video_go/internal/db"
	"feedsystem_video_go/internal/feed"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"

	driver "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type evidenceReadKey struct{}

type evidenceLogger struct {
	logger.Interface
	readSQL atomic.Int64
}

func (l *evidenceLogger) Trace(ctx context.Context, _ time.Time, _ func() (string, int64), _ error) {
	if ctx.Value(evidenceReadKey{}) != nil {
		l.readSQL.Add(1)
	}
}

type evidenceFanoutWrites struct {
	commands atomic.Int64
	expected atomic.Int64
	finished chan time.Time
}

func (*evidenceFanoutWrites) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (*evidenceFanoutWrites) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (p *evidenceFanoutWrites) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		err := next(ctx, commands)
		if err != nil {
			return err
		}
		var writes int64
		for _, cmd := range commands {
			if cmd.Name() == "zadd" || cmd.Name() == "zremrangebyrank" {
				key, _ := cmd.Args()[1].(string)
				if strings.Contains(key, ":feed:inbox:") || strings.Contains(key, ":feed:author_outbox:") {
					writes++
				}
			}
		}
		if writes > 0 && p.commands.Add(writes) == p.expected.Load() {
			p.finished <- time.Now()
		}
		return nil
	}
}

type evidenceOperation struct {
	author uint
	viewer uint
}

func TestFollowingCostEvidence(t *testing.T) {
	if os.Getenv("GOFEED_HYBRID_COST") != "1" {
		t.Skip("set GOFEED_HYBRID_COST=1 for the 12-round isolated experiment")
	}
	cfg, err := driver.ParseDSN(os.Getenv("LIKE_COMMAND_TEST_DSN"))
	mustFollowing(t, err)
	uri, err := amqp.ParseURI(os.Getenv("FOLLOWING_PHASE_B_AMQP_URL"))
	mustFollowing(t, err)
	addr := os.Getenv("C_TEST_REDIS_ADDR")
	if cfg.DBName != "gofeed_read_test" || uri.Vhost != "gofeed-following-test-cost" || addr == "" {
		t.Fatal("dedicated MySQL, Redis and gofeed-following-test-cost vhost required")
	}
	mq := config.RabbitMQConfig{Host: uri.Host, Port: uri.Port, Username: uri.Username, Password: uri.Password, VHost: uri.Vhost}
	cfg.DBName = ""
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	mustFollowing(t, err)
	defer admin.Close()
	schema := fmt.Sprintf("gofeed_hybrid_cost_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE " + schema)
	mustFollowing(t, err)
	defer func() {
		if _, err := admin.Exec("DROP DATABASE " + schema); err != nil {
			t.Error(err)
		}
	}()
	cfg.DBName = schema
	log := &evidenceLogger{Interface: logger.Default.LogMode(logger.Silent)}
	db, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: log, PrepareStmt: true})
	mustFollowing(t, err)
	pool, err := db.DB()
	mustFollowing(t, err)
	defer pool.Close()
	pool.SetMaxOpenConns(16)
	pool.SetMaxIdleConns(8)
	mustFollowing(t, appdb.AutoMigrate(db))

	rng := rand.New(rand.NewSource(20261116))
	permutation := rng.Perm(12000)
	viewers := make([]uint, len(permutation))
	accounts := []account.Account{{ID: 1, Username: "small"}, {ID: 2, Username: "large"}}
	links := make([]social.Social, 0, 13000)
	for i, n := range permutation {
		id := uint(n + 3)
		viewers[i] = id
		accounts = append(accounts, account.Account{ID: id, Username: fmt.Sprint("reader-", id)})
		links = append(links, social.Social{FollowerID: id, VloggerID: 2})
		if i < 1000 {
			links = append(links, social.Social{FollowerID: id, VloggerID: 1})
		}
	}
	mustFollowing(t, db.CreateInBatches(accounts, 500).Error)
	mustFollowing(t, db.CreateInBatches(links, 500).Error)
	for _, workload := range []struct {
		name             string
		publishes, reads int
	}{{"read_heavy", 8, 96}, {"write_heavy", 48, 12}} {
		schedule := make([]evidenceOperation, 0, workload.publishes+workload.reads)
		for i := 0; i < workload.publishes; i++ {
			schedule = append(schedule, evidenceOperation{author: uint(1 + i%2)})
		}
		for i := 0; i < workload.reads; i++ {
			eligible := viewers
			if i%2 == 0 {
				eligible = viewers[:1000]
			}
			schedule = append(schedule, evidenceOperation{viewer: eligible[rng.Intn(len(eligible))]})
		}
		rng.Shuffle(len(schedule), func(i, j int) { schedule[i], schedule[j] = schedule[j], schedule[i] })
		strategies := []string{"push", "pull", "hybrid"}
		for repeat := 1; repeat <= 2; repeat++ {
			for i := range strategies {
				strategy := strategies[i]
				if repeat == 2 {
					strategy = strategies[len(strategies)-1-i]
				}
				if !t.Run(fmt.Sprintf("%s/%s/%d", workload.name, strategy, repeat), func(t *testing.T) {
					runFollowingCostEvidence(t, db, log, addr, schema, mq, strategy, workload.name, repeat, viewers, schedule)
				}) {
					return
				}
			}
		}
	}
}

func runFollowingCostEvidence(t *testing.T, db *gorm.DB, log *evidenceLogger, addr, schema string, mq config.RabbitMQConfig, strategy, workload string, repeat int, viewers []uint, schedule []evidenceOperation) {
	ctx, cancel := context.WithCancel(context.Background())
	raw := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	defer raw.Close()
	mustFollowing(t, raw.Ping(ctx).Err())
	prefix := fmt.Sprintf("%s:%s:%s:%d:", schema, workload, strategy, repeat)
	cache := rediscache.NewClient(raw, prefix)
	defer cache.DelByPattern(context.Background(), prefix+"*")
	writeProbe := &evidenceFanoutWrites{finished: make(chan time.Time, 1)}
	raw.AddHook(writeProbe)
	for _, table := range []string{"outbox_msgs", "videos", "author_feed_states"} {
		mustFollowing(t, db.Exec("DELETE FROM "+table).Error)
	}

	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); worker.RunGlobalTimelineConsumer(ctx, mq, cache) }()
	go func() {
		defer workers.Done()
		worker.RunFollowingFanoutConsumer(ctx, mq, social.NewSocialRepository(db), cache, config.DefaultFollowingFanoutConfig())
	}()
	defer func() { cancel(); workers.Wait() }()
	publisher := rabbitmq.NewTimelineMQ(mq)
	defer publisher.Close()

	thresholds := config.DefaultFollowingFeedConfig()
	switch strategy {
	case "push":
		thresholds = config.FollowingFeedConfig{LowThreshold: 13000, InitialThreshold: 14000, HighThreshold: 15000}
	case "pull":
		thresholds = config.FollowingFeedConfig{LowThreshold: 1, InitialThreshold: 2, HighThreshold: 3}
	}
	publish := video.NewVideoService(video.NewVideoRepository(db), cache, thresholds)
	reader := feed.NewFeedService(feed.NewFeedRepository(db), video.NewLikeRepository(db), social.NewSocialRepository(db), cache)
	timestamp := time.UnixMilli(1_794_012_300_123).UTC()
	var publishTimes, asyncTimes, readTimes, visibleTimes []float64
	var expectedKeys = make(map[string]map[string]bool)
	var postOrdinal uint
	readAndCheck := func(viewer uint, timed bool) {
		var ordered []uint
		mustFollowing(t, db.Raw(`SELECT v.id FROM videos v
			WHERE EXISTS (SELECT 1 FROM socials s WHERE s.follower_id = ? AND s.vlogger_id = v.author_id)
			ORDER BY v.create_time DESC, v.id DESC LIMIT 11`, viewer).Scan(&ordered).Error)
		var requestCtx = ctx
		if timed {
			requestCtx = context.WithValue(ctx, evidenceReadKey{}, true)
		}
		begin := time.Now()
		page, err := reader.ListByFollowing(requestCtx, 10, nil, viewer)
		elapsed := time.Since(begin)
		mustFollowing(t, err)
		n := min(len(ordered), 10)
		actual := make([]uint, 0, len(page.VideoList))
		for _, v := range page.VideoList {
			actual = append(actual, v.ID)
		}
		if !slices.Equal(actual, ordered[:n]) || page.HasMore != (len(ordered) > 10) ||
			(n > 0 && (page.NextIDBefore != ordered[n-1] || page.NextTime != timestamp.UnixMilli())) {
			t.Fatalf("viewer %d: Following=%+v, MySQL=%v", viewer, page, ordered)
		}
		if timed {
			readTimes = append(readTimes, evidenceMS(elapsed))
		}
	}
	post := func(author uint, timed bool) {
		postOrdinal++
		v := video.Video{ID: postOrdinal, AuthorID: author, Title: "experiment post", PlayURL: "/play", CoverURL: "/cover", CreateTime: timestamp}
		isPush := strategy == "push" || (strategy == "hybrid" && author == 1)
		keys := []string{cache.Key("feed:author_outbox:%d", author)}
		wantMode := video.DistributionPull
		if isPush {
			wantMode = video.DistributionPush
			recipients := viewers
			if author == 1 {
				recipients = viewers[:1000]
			}
			keys = make([]string, len(recipients))
			for i, id := range recipients {
				keys[i] = cache.Key("feed:inbox:%d", id)
			}
		}
		writeProbe.expected.Store(writeProbe.commands.Load() + int64(2*len(keys)))
		begin := time.Now()
		mustFollowing(t, publish.Publish(ctx, &v))
		committed := time.Now()
		if v.DistributionMode == nil || *v.DistributionMode != wantMode {
			t.Fatal("unexpected distribution snapshot")
		}
		count, err := worker.RelayVideoOutboxOnce(ctx, db, publisher)
		mustFollowing(t, err)
		if count != 1 {
			t.Fatalf("relayed %d rows, want 1", count)
		}
		var finished time.Time
		select {
		case finished = <-writeProbe.finished:
		case <-time.After(5 * time.Second):
			t.Fatalf("candidate writes did not finish: counted %d, expected %d", writeProbe.commands.Load(), writeProbe.expected.Load())
		}
		member, err := feed.FollowingMember(timestamp.UnixMilli(), v.ID)
		mustFollowing(t, err)
		for _, key := range keys {
			if expectedKeys[key] == nil {
				expectedKeys[key] = make(map[string]bool)
			}
			expectedKeys[key][member] = true
		}
		// Sample visibility for a follower, separately from scheduled reads.
		page, err := reader.ListByFollowing(ctx, 10, nil, viewers[0])
		mustFollowing(t, err)
		if len(page.VideoList) == 0 || page.VideoList[0].ID != v.ID {
			t.Fatalf("published video %d not visible at the head", v.ID)
		}
		if timed {
			publishTimes = append(publishTimes, evidenceMS(committed.Sub(begin)))
			asyncTimes = append(asyncTimes, evidenceMS(finished.Sub(committed)))
			visibleTimes = append(visibleTimes, evidenceMS(time.Since(committed)))
		}
	}

	for i := 0; i < 6; i++ {
		post(uint(1+i%2), false)
	}
	readAndCheck(viewers[0], false)
	readAndCheck(viewers[len(viewers)-1], false)
	initialWrites := writeProbe.commands.Load()
	log.readSQL.Store(0)
	scheduleStart := time.Now()
	for _, op := range schedule {
		if op.author != 0 {
			post(op.author, true)
		} else {
			readAndCheck(op.viewer, true)
		}
	}
	scheduleSeconds := time.Since(scheduleStart).Seconds()
	var indexBytes int64
	var keyBatch []string
	validateBatch := func() {
		batch := raw.Pipeline()
		members := make([]*redis.StringSliceCmd, 0, len(keyBatch))
		memory := make([]*redis.IntCmd, 0, len(keyBatch))
		for _, key := range keyBatch {
			members = append(members, batch.ZRange(ctx, key, 0, -1))
			memory = append(memory, batch.MemoryUsage(ctx, key))
		}
		_, err := batch.Exec(ctx)
		mustFollowing(t, err)
		for i, key := range keyBatch {
			want, got := expectedKeys[key], members[i].Val()
			if len(got) != len(want) {
				t.Fatalf("incorrect candidate count at %s", key)
			}
			for _, member := range got {
				if !want[member] {
					t.Fatalf("unexpected member at %s", key)
				}
			}
			indexBytes += memory[i].Val()
		}
		keyBatch = keyBatch[:0]
	}
	for key := range expectedKeys {
		keyBatch = append(keyBatch, key)
		if len(keyBatch) == 500 {
			validateBatch()
		}
	}
	if len(keyBatch) != 0 {
		validateBatch()
	}
	var actualKeys int
	var scan uint64
	for {
		keys, next, err := raw.Scan(ctx, scan, prefix+"feed:*", 1024).Result()
		mustFollowing(t, err)
		for _, key := range keys {
			if !strings.HasPrefix(key, prefix+"feed:inbox:") && !strings.HasPrefix(key, prefix+"feed:author_outbox:") {
				continue
			}
			actualKeys++
			if _, ok := expectedKeys[key]; !ok {
				t.Fatalf("unexpected index key %s", key)
			}
		}
		if next == 0 {
			break
		}
		scan = next
	}
	if actualKeys != len(expectedKeys) {
		t.Fatalf("index key count %d != %d", actualKeys, len(expectedKeys))
	}
	var readTotal float64
	for _, ms := range readTimes {
		readTotal += ms
	}
	result, err := json.Marshal(map[string]any{
		"workload": workload, "strategy": strategy, "round": repeat, "publications": len(publishTimes), "reads": len(readTimes),
		"redis_candidate_commands": writeProbe.commands.Load() - initialWrites, "index_keys": actualKeys, "index_bytes_including_warmup": indexBytes,
		"publish_service_p50_ms": evidencePercentile(publishTimes, 50), "commit_to_redis_p50_ms": evidencePercentile(asyncTimes, 50),
		"commit_to_observed_head_p50_ms": evidencePercentile(visibleTimes, 50), "feed_service_p95_ms": evidencePercentile(readTimes, 95),
		"feed_success_per_read_service_second": float64(len(readTimes)) * 1000 / readTotal, "feed_selects": log.readSQL.Load(),
		"feed_success_per_schedule_second": float64(len(readTimes)) / scheduleSeconds, "schedule_seconds": scheduleSeconds,
		"correctness": "pass",
	})
	mustFollowing(t, err)
	t.Logf("HYBRID_RESULT %s", result)
}

func evidenceMS(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
func evidencePercentile(values []float64, p int) float64 {
	sort.Float64s(values)
	return values[(len(values)*p+99)/100-1]
}
