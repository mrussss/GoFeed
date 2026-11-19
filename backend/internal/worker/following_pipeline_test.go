package worker_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
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

	drivermysql "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestFollowingPipeline(t *testing.T) {
	dsn, amqpURL, redisAddr := os.Getenv("LIKE_COMMAND_TEST_DSN"), os.Getenv("FOLLOWING_PHASE_B_AMQP_URL"), os.Getenv("C_TEST_REDIS_ADDR")
	if dsn == "" || amqpURL == "" || redisAddr == "" {
		t.Skip("set LIKE_COMMAND_TEST_DSN, FOLLOWING_PHASE_B_AMQP_URL, and C_TEST_REDIS_ADDR for this integration suite")
	}
	uri, err := amqp.ParseURI(amqpURL)
	if err != nil || !strings.HasPrefix(uri.Vhost, "gofeed-following-test-") {
		t.Fatal("isolated gofeed-following-test-* vhost required")
	}
	dbCfg, err := drivermysql.ParseDSN(dsn)
	mustFollowing(t, err)
	dbCfg.DBName, dbCfg.ParseTime, dbCfg.Timeout = "", true, 3*time.Second
	admin, err := sql.Open("mysql", dbCfg.FormatDSN())
	mustFollowing(t, err)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	schema := fmt.Sprintf("gofeed_following_b_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+schema)
	mustFollowing(t, err)
	defer func() {
		_, err := admin.Exec("DROP DATABASE " + schema)
		if err != nil {
			t.Error(err)
		}
	}()
	dbCfg.DBName = schema
	db, err := gorm.Open(gormmysql.Open(dbCfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	mustFollowing(t, err)
	sqlDB, err := db.DB()
	mustFollowing(t, err)
	defer sqlDB.Close()
	mustFollowing(t, appdb.AutoMigrate(db))

	mqCfg := config.RabbitMQConfig{Host: uri.Host, Port: uri.Port, Username: uri.Username, Password: uri.Password, VHost: uri.Vhost}
	publisher := rabbitmq.NewTimelineMQ(mqCfg)
	defer publisher.Close()
	broker, err := amqp.Dial(amqpURL)
	mustFollowing(t, err)
	defer broker.Close()
	ch, err := broker.Channel()
	mustFollowing(t, err)
	defer ch.Close()
	for _, queue := range []string{rabbitmq.GlobalTimelineQueue, rabbitmq.FollowingFanoutQueue} {
		mustFollowing(t, rabbitmq.DeclareTimelineQueue(ch, queue))
		_, err := ch.QueuePurge(queue, false)
		mustFollowing(t, err)
	}
	// Start with the legacy queue only; the first publisher must declare fanout.
	_, err = ch.QueueDelete(rabbitmq.FollowingFanoutQueue, true, true, false)
	mustFollowing(t, err)

	raw := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 15})
	defer raw.Close()
	cache := rediscache.NewClient(raw, schema+":")
	defer cache.DelByPattern(context.Background(), schema+":*")
	mustFollowing(t, raw.Ping(ctx).Err())
	stamp := time.Now().Truncate(time.Millisecond)
	svc := video.NewVideoService(video.NewVideoRepository(db), nil)
	mustFollowing(t, db.Create(&account.Account{ID: 1, Username: "fanout-test"}).Error)
	publishVideo := func(t *testing.T) video.OutboxMsg {
		t.Helper()
		post := video.Video{AuthorID: 1, Title: "publish through outbox", PlayURL: "/play", CoverURL: "/cover"}
		mustFollowing(t, svc.Publish(ctx, &post))
		var row video.OutboxMsg
		mustFollowing(t, db.Where("video_id = ?", post.ID).Take(&row).Error)
		return row
	}
	status := func(t *testing.T, id uint, want string) {
		t.Helper()
		var row video.OutboxMsg
		mustFollowing(t, db.First(&row, id).Error)
		if row.Status != want {
			t.Fatalf("outbox %d status %q, want %q", id, row.Status, want)
		}
	}
	readBoth := func(t *testing.T, eventID string) rabbitmq.TimelineEvent {
		t.Helper()
		var first amqp.Delivery
		for _, queue := range []string{rabbitmq.GlobalTimelineQueue, rabbitmq.FollowingFanoutQueue} {
			delivery, ok, err := ch.Get(queue, true)
			mustFollowing(t, err)
			if !ok || delivery.MessageId != eventID || delivery.DeliveryMode != amqp.Persistent {
				t.Fatalf("queue %s: got message %q with delivery mode %d", queue, delivery.MessageId, delivery.DeliveryMode)
			}
			if first.Body == nil {
				first = delivery
			} else if string(first.Body) != string(delivery.Body) {
				t.Fatal("global and following queues received different payloads")
			}
		}
		var event rabbitmq.TimelineEvent
		mustFollowing(t, json.Unmarshal(first.Body, &event))
		if event.EventID != eventID {
			t.Fatalf("body event ID %q, want %q", event.EventID, eventID)
		}
		return event
	}

	var legacy rabbitmq.TimelineEvent
	if !t.Run("confirm_replay_return_and_legacy", func(t *testing.T) {
		row := publishVideo(t)
		mustFollowing(t, db.Exec(`CREATE TRIGGER reject_video_mark BEFORE UPDATE ON outbox_msgs
			FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'reject_video_mark'`).Error)
		if _, err := worker.RelayVideoOutboxOnce(ctx, db, publisher); err == nil || !strings.Contains(err.Error(), "reject_video_mark") {
			t.Fatalf("expected a failure marking the confirmed outbox: %v", err)
		}
		status(t, row.ID, "pending")
		first := readBoth(t, *row.EventID)
		if first.VideoID != row.VideoID || first.AuthorID != *row.AuthorID ||
			first.DistributionMode != string(*row.DistributionMode) || first.CreateTime != row.CreateTime.UnixMilli() {
			t.Fatalf("wrong persisted snapshot: %+v", first)
		}
		mustFollowing(t, db.Exec("DROP TRIGGER reject_video_mark").Error)
		_, err := worker.RelayVideoOutboxOnce(ctx, db, publisher)
		mustFollowing(t, err)
		status(t, row.ID, "published")
		if replay := readBoth(t, *row.EventID); replay != first {
			t.Fatalf("changed event on retry: %+v vs %+v", replay, first)
		}

		mustFollowing(t, ch.QueueUnbind(rabbitmq.GlobalTimelineQueue, "video.timeline.*", rabbitmq.TimelineExchange, nil))
		mustFollowing(t, ch.QueueUnbind(rabbitmq.FollowingFanoutQueue, rabbitmq.TimelinePublishKey, rabbitmq.TimelineExchange, nil))
		old := video.OutboxMsg{VideoID: 5000, EventType: "video_published", Status: "pending", CreateTime: stamp}
		mustFollowing(t, db.Create(&old).Error)
		_, err = worker.RelayVideoOutboxOnce(ctx, db, publisher)
		var publishErr *rabbitmq.PublishError
		if !errors.As(err, &publishErr) || publishErr.Code != "unroutable" {
			t.Fatalf("expected mandatory Return, got %v", err)
		}
		status(t, old.ID, "pending")
		_, err = worker.RelayVideoOutboxOnce(ctx, db, publisher)
		mustFollowing(t, err)
		status(t, old.ID, "published")
		legacy = readBoth(t, fmt.Sprintf("legacy-outbox-%d", old.ID))
		if !legacy.Legacy || legacy.AuthorID != 0 || legacy.DistributionMode != "" {
			t.Fatalf("legacy delivery gained distribution fields: %+v", legacy)
		}

		eventID := "0123456789abcdef0123456789abcdef"
		incomplete := video.OutboxMsg{VideoID: 5001, EventType: "video_published", Status: "pending", EventID: &eventID, CreateTime: stamp}
		mustFollowing(t, db.Create(&incomplete).Error)
		_, err = worker.RelayVideoOutboxOnce(ctx, db, publisher)
		mustFollowing(t, err)
		status(t, incomplete.ID, "invalid")
		for _, queue := range []string{rabbitmq.GlobalTimelineQueue, rabbitmq.FollowingFanoutQueue} {
			info, err := ch.QueueInspect(queue)
			mustFollowing(t, err)
			if info.Messages != 0 {
				t.Fatalf("invalid row delivered to %s", queue)
			}
		}
	}) {
		return
	}

	fanoutCfg := config.DefaultFollowingFanoutConfig()
	author := func(t *testing.T, id uint, followers int) []string {
		t.Helper()
		rows := make([]social.Social, followers)
		keys := make([]string, followers)
		for i := range rows {
			follower := id*10000 + uint(i+1)
			rows[i] = social.Social{VloggerID: id, FollowerID: follower}
			keys[i] = cache.Key("feed:inbox:%d", follower)
		}
		if len(rows) > 0 {
			mustFollowing(t, db.CreateInBatches(rows, 500).Error)
		}
		return keys
	}
	event := func(id, author uint, mode video.DistributionMode) rabbitmq.TimelineEvent {
		return rabbitmq.TimelineEvent{
			EventID: fmt.Sprintf("%032x", id), VideoID: id, AuthorID: author, DistributionMode: string(mode),
			CreateTime: stamp.UnixMilli(), OccurredAt: stamp.UTC(),
		}
	}
	member := func(t *testing.T, event rabbitmq.TimelineEvent) string {
		t.Helper()
		value, err := feed.FollowingMember(event.CreateTime, event.VideoID)
		mustFollowing(t, err)
		return value
	}
	publish := func(t *testing.T, event rabbitmq.TimelineEvent) {
		t.Helper()
		mustFollowing(t, publisher.Publish(ctx, event))
	}
	waitKey := func(t *testing.T, key, member string, score float64) {
		t.Helper()
		awaitFollowing(t, func() bool {
			n, err := raw.ZScore(ctx, key, member).Result()
			return err == nil && n == score
		}, "member "+member+" in "+key)
	}
	globalKey := cache.Key("feed:global_timeline")
	waitGlobal := func(t *testing.T, e rabbitmq.TimelineEvent) {
		t.Helper()
		waitKey(t, globalKey, strconv.FormatUint(uint64(e.VideoID), 10), float64(e.CreateTime))
	}
	fence := func(t *testing.T, id uint) {
		t.Helper()
		e := event(id, id, video.DistributionPull)
		publish(t, e)
		waitKey(t, cache.Key("feed:author_outbox:%d", id), member(t, e), 0)
	}

	lostAck := event(9000, 99, video.DistributionPull)
	probe := &followingPipelineProbe{member: member(t, lostAck), written: make(chan struct{}), resume: make(chan struct{})}
	// The first fanout Redis connection is deliberately unavailable. Redis and
	// the global consumer remain available to this test through their own client.
	var offline atomic.Bool
	offline.Store(true)
	dialer := &net.Dialer{Timeout: time.Second}
	fanoutRaw := redis.NewClient(&redis.Options{
		Addr: redisAddr, DB: 15, MaxRetries: -1,
		Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
			if offline.Load() {
				return nil, errors.New("fanout Redis transport unavailable")
			}
			return dialer.DialContext(ctx, network, address)
		},
	})
	fanoutRaw.AddHook(probe)
	defer fanoutRaw.Close()
	fanoutCache := rediscache.NewClient(fanoutRaw, schema+":")
	workerCtx, stop := context.WithCancel(ctx)
	globalDone, fanoutDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(globalDone)
		worker.RunGlobalTimelineConsumer(workerCtx, mqCfg, cache)
	}()
	go func() {
		defer close(fanoutDone)
		worker.RunFollowingFanoutConsumer(workerCtx, mqCfg, social.NewSocialRepository(db), fanoutCache, fanoutCfg)
	}()
	defer func() { stop(); <-fanoutDone; <-globalDone }()

	if !t.Run("redis_unavailable_then_recovery", func(t *testing.T) {
		e := event(100, 10, video.DistributionPull)
		key := cache.Key("feed:author_outbox:%d", e.AuthorID)
		publish(t, e)
		waitGlobal(t, e)
		awaitFollowing(t, func() bool { return probe.failures.Load() > 0 }, "failed fanout Redis pipeline")
		if raw.Exists(ctx, key).Val() != 0 {
			t.Fatal("fanout wrote while its Redis connection was unavailable")
		}
		offline.Store(false)
		waitKey(t, key, member(t, e), 0)
	}) {
		return
	}

	t.Run("publish_to_both_consumers", func(t *testing.T) {
		key := author(t, 1, 1)[0]
		row := publishVideo(t)
		_, err := worker.RelayVideoOutboxOnce(ctx, db, publisher)
		mustFollowing(t, err)
		status(t, row.ID, "published")
		e := rabbitmq.TimelineEvent{VideoID: row.VideoID, CreateTime: row.CreateTime.UnixMilli()}
		waitGlobal(t, e)
		waitKey(t, key, member(t, e), 0)
	})

	t.Run("push_follower_pages", func(t *testing.T) {
		for i, n := range []int{0, 1, 201, 1000} {
			id := uint(i + 11)
			keys := author(t, id, n)
			e := event(200+id, id, video.DistributionPush)
			start := time.Now()
			publish(t, e)
			publish(t, e)
			fence(t, 300+id)
			// The fence proves both prior deliveries have finished. Apply the
			// count to every real follower, including the last page.
			results, err := raw.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for _, key := range keys {
					pipe.ZRangeWithScores(ctx, key, 0, -1)
				}
				return nil
			})
			mustFollowing(t, err)
			for i, result := range results {
				got := result.(*redis.ZSliceCmd).Val()
				if len(got) != 1 || got[0].Score != 0 || got[0].Member != member(t, e) {
					t.Fatalf("inbox %s: %v", keys[i], got)
				}
			}
			if raw.Exists(ctx, cache.Key("feed:author_outbox:%d", id)).Val() != 0 {
				t.Fatal("Push wrote an author Outbox")
			}
			t.Logf("followers=%d verified_inboxes=%d elapsed_including_duplicate_and_fence=%s", n, len(results), time.Since(start))
		}
	})

	t.Run("partial_pipeline_and_replay", func(t *testing.T) {
		keys := author(t, 20, 201)
		mustFollowing(t, raw.Set(ctx, keys[100], "wrong type", 0).Err())
		e := event(400, 20, video.DistributionPush)
		publish(t, e)
		waitGlobal(t, e)
		waitKey(t, keys[0], member(t, e), 0)
		awaitFollowing(t, func() bool { return probe.failures.Load() >= 2 }, "Redis wrong-type pipeline result")
		mustFollowing(t, raw.Del(ctx, keys[100]).Err())
		waitKey(t, keys[100], member(t, e), 0)
		fence(t, 401)
		if n := raw.ZCard(ctx, keys[0]).Val(); n != 1 {
			t.Fatalf("partial replay created %d members", n)
		}
	})

	t.Run("bounded_lex_indexes_and_pull_isolation", func(t *testing.T) {
		for _, mode := range []video.DistributionMode{video.DistributionPush, video.DistributionPull} {
			id, limit := uint(30), fanoutCfg.InboxMaxLen
			if mode == video.DistributionPull {
				id, limit = 31, fanoutCfg.AuthorOutboxMaxLen
			}
			followerKey := author(t, id, 1)[0]
			key := followerKey
			if mode == video.DistributionPull {
				key = cache.Key("feed:author_outbox:%d", id)
			}
			e := event(8000+id, id, mode)
			seed := make([]redis.Z, limit)
			for i := range seed {
				older := event(uint(i+1), id, mode)
				if i < 2 {
					older.CreateTime--
				}
				seed[i] = redis.Z{Member: member(t, older)}
			}
			mustFollowing(t, raw.ZAdd(ctx, key, seed...).Err())
			publish(t, e)
			publish(t, e)
			fence(t, 500+id)
			got, err := raw.ZRevRange(ctx, key, 0, -1).Result()
			mustFollowing(t, err)
			want := make([]string, 0, limit)
			want = append(want, member(t, e))
			for i := len(seed) - 1; i > 0; i-- {
				want = append(want, seed[i].Member.(string))
			}
			if !slices.Equal(got, want) {
				t.Fatalf("%s retained wrong candidates: len %d want %d", mode, len(got), limit)
			}
			if mode == video.DistributionPull && raw.Exists(ctx, followerKey).Val() != 0 {
				t.Fatal("Pull wrote a follower Inbox")
			}
		}
	})

	t.Run("legacy_and_invalid_consumption", func(t *testing.T) {
		key := author(t, 40, 1)[0]
		publish(t, legacy)
		bad := event(901, 40, video.DistributionPush)
		bad.DistributionMode = ""
		publish(t, bad)
		fence(t, 902)
		waitGlobal(t, legacy)
		waitGlobal(t, bad)
		if raw.Exists(ctx, key, cache.Key("feed:author_outbox:0"), cache.Key("feed:author_outbox:40")).Val() != 0 {
			t.Fatal("legacy or invalid event wrote following candidates")
		}
	})

	t.Run("disconnect_after_redis_before_ack", func(t *testing.T) {
		management := os.Getenv("RELAY_TEST_MANAGEMENT_URL")
		if management == "" {
			t.Skip("set RELAY_TEST_MANAGEMENT_URL to close the live consumer connection")
		}
		managementRequest := func(method, path string) []byte {
			t.Helper()
			req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(management, "/")+path, nil)
			mustFollowing(t, err)
			req.SetBasicAuth(uri.Username, uri.Password)
			res, err := http.DefaultClient.Do(req)
			mustFollowing(t, err)
			defer res.Body.Close()
			if res.StatusCode >= 300 {
				t.Fatalf("RabbitMQ management %s %s: HTTP %d", method, path, res.StatusCode)
			}
			var body json.RawMessage
			if method == http.MethodGet {
				mustFollowing(t, json.NewDecoder(res.Body).Decode(&body))
			}
			return body
		}
		var connection string
		awaitFollowing(t, func() bool {
			var consumers []struct {
				Queue   struct{ Name string } `json:"queue"`
				Details json.RawMessage       `json:"channel_details"`
			}
			mustFollowing(t, json.Unmarshal(managementRequest("GET", "/api/consumers/"+url.PathEscape(uri.Vhost)), &consumers))
			for _, c := range consumers {
				var details struct {
					Connection string `json:"connection_name"`
				}
				if c.Queue.Name == rabbitmq.FollowingFanoutQueue && json.Unmarshal(c.Details, &details) == nil && details.Connection != "" {
					connection = details.Connection
					return true
				}
			}
			return false
		}, "live fanout connection")
		defer close(probe.resume)
		publish(t, lostAck)
		select {
		case <-probe.written:
		case <-ctx.Done():
			t.Fatal("Redis pipeline did not finish")
		}
		key := cache.Key("feed:author_outbox:%d", lostAck.AuthorID)
		waitKey(t, key, probe.member, 0)
		managementRequest("DELETE", "/api/connections/"+url.PathEscape(connection))
		// The hook remains blocked until the command context times out or the
		// subtest exits. The replay must reach the now-reconnected consumer.
		awaitFollowing(t, func() bool { return probe.successes.Load() == 2 }, "redelivery after Redis success and lost ACK")
		fence(t, 903)
		if n := raw.ZCard(ctx, key).Val(); n != 1 {
			t.Fatalf("lost ACK produced %d members", n)
		}
	})
}

func mustFollowing(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func awaitFollowing(t *testing.T, ready func() bool, what string) {
	t.Helper()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		case <-ticker.C:
		}
	}
}

type followingPipelineProbe struct {
	member              string
	written, resume     chan struct{}
	successes, failures atomic.Int32
}

func (*followingPipelineProbe) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (*followingPipelineProbe) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (p *followingPipelineProbe) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if err != nil {
			p.failures.Add(1)
			return err
		}
		if len(cmds) == 2 && cmds[0].Name() == "zadd" && cmds[0].Args()[3] == p.member && p.successes.Add(1) == 1 {
			close(p.written)
			select {
			case <-p.resume:
			case <-ctx.Done():
			}
		}
		return nil
	}
}

func TestFollowingMember(t *testing.T) {
	maxID := ^uint(0)
	a, err := feed.FollowingMember(9, maxID)
	mustFollowing(t, err)
	b, err := feed.FollowingMember(10, 9)
	mustFollowing(t, err)
	c, err := feed.FollowingMember(10, 10)
	mustFollowing(t, err)
	if len(a) != 34 || len(b) != 34 || !(a < b && b < c) {
		t.Fatalf("lexicographic order mismatch: %q %q %q", a, b, c)
	}
	for _, stamp := range []int64{-1, 10_000_000_000_000} {
		if _, err := feed.FollowingMember(stamp, 1); err == nil {
			t.Fatalf("accepted timestamp %d outside member format", stamp)
		}
	}
}
