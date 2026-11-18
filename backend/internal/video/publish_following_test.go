package video_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/auth"
	"feedsystem_video_go/internal/config"
	appdb "feedsystem_video_go/internal/db"
	apphttp "feedsystem_video_go/internal/http"
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

func TestPublishFollowingMySQL(t *testing.T) {
	dsn := os.Getenv("LIKE_COMMAND_TEST_DSN")
	if dsn == "" {
		t.Skip("set LIKE_COMMAND_TEST_DSN to run isolated MySQL publish tests")
	}
	must := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	cfg.DBName, cfg.ParseTime = "", true
	cfg.Timeout = 5 * time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	must(t, err)
	defer admin.Close()
	schema := fmt.Sprintf("gofeed_following_test_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE " + schema)
	must(t, err)
	defer func() {
		if _, err := admin.Exec("DROP DATABASE " + schema); err != nil {
			t.Error(err)
		}
	}()
	cfg.DBName = schema
	open := func() *gorm.DB {
		db, err := gorm.Open(gormmysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		must(t, err)
		sqlDB, err := db.DB()
		must(t, err)
		t.Cleanup(func() { _ = sqlDB.Close() })
		return db
	}
	db := open()
	exec := func(t *testing.T, query string) { must(t, db.Exec(query).Error) }
	// Seed the pre-Phase A schema before running the real startup migration.
	exec(t, "CREATE TABLE videos (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, author_id BIGINT UNSIGNED NOT NULL, username VARCHAR(255) NOT NULL, title VARCHAR(255) NOT NULL, play_url VARCHAR(255) NOT NULL, cover_url VARCHAR(255) NOT NULL)")
	exec(t, "CREATE TABLE outbox_msgs (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, video_id BIGINT UNSIGNED, event_type VARCHAR(50), status VARCHAR(50), create_time DATETIME(3))")
	exec(t, "INSERT INTO videos (id, author_id, username, title, play_url, cover_url) VALUES (1, 1, 'author', 'legacy', '/play', '/cover')")
	exec(t, "INSERT INTO outbox_msgs (video_id, event_type, status, create_time) VALUES (1, 'video_published', 'pending', '2026-01-01')")
	must(t, appdb.AutoMigrate(db))
	must(t, appdb.AutoMigrate(db))
	var oldVideo video.Video
	var oldOutbox video.OutboxMsg
	must(t, db.First(&oldVideo, 1).Error)
	must(t, db.First(&oldOutbox, 1).Error)
	if oldVideo.DistributionMode != nil || oldOutbox.AuthorID != nil || oldOutbox.DistributionMode != nil || oldOutbox.EventID != nil {
		t.Fatal("migration assigned a strategy or event identity to legacy data")
	}
	must(t, db.Create(&account.Account{ID: 1, Username: "author"}).Error)
	if err := db.Create(&video.AuthorFeedState{AuthorID: 2, DistributionMode: "unknown"}).Error; err == nil {
		t.Fatal("MySQL accepted an invalid distribution mode")
	}
	thresholds := config.FollowingFeedConfig{LowThreshold: 9, InitialThreshold: 10, HighThreshold: 11}
	svc := video.NewVideoService(video.NewVideoRepository(db), nil, thresholds)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	newVideo := func() *video.Video {
		return &video.Video{AuthorID: 1, Title: "publication", Username: "author", PlayURL: "/play", CoverURL: "/cover"}
	}
	state := func(t *testing.T, want video.DistributionMode) {
		t.Helper()
		var rows []video.AuthorFeedState
		must(t, db.Find(&rows).Error)
		if len(rows) != 1 || rows[0].AuthorID != 1 || rows[0].DistributionMode != want {
			t.Fatalf("author state = %+v, want %s", rows, want)
		}
	}
	snapshot := func(id uint, want video.DistributionMode) string {
		t.Helper()
		var stored video.Video
		var event video.OutboxMsg
		must(t, db.First(&stored, id).Error)
		must(t, db.Where("video_id = ?", id).Take(&event).Error)
		if stored.DistributionMode == nil || *stored.DistributionMode != want ||
			event.DistributionMode == nil || *event.DistributionMode != want ||
			event.AuthorID == nil || *event.AuthorID != stored.AuthorID ||
			event.EventID == nil || len(*event.EventID) != 32 || event.EventType != "video_published" ||
			stored.CreateTime.IsZero() || !stored.CreateTime.Equal(event.CreateTime) {
			t.Fatalf("wrong snapshot for video %d: %+v %+v", id, stored, event)
		}
		return *event.EventID
	}
	start := make(chan struct{})
	publications := make([]*video.Video, 8)
	results := make(chan error, len(publications))
	for i := range publications {
		publications[i] = newVideo()
		go func(v *video.Video) {
			<-start
			results <- svc.Publish(ctx, v)
		}(publications[i])
	}
	close(start)
	for range publications {
		must(t, <-results)
	}
	state(t, video.DistributionPush)
	ids := map[string]bool{}
	for _, v := range publications {
		id := snapshot(v.ID, video.DistributionPush)
		if ids[id] {
			t.Fatalf("duplicate event ID %s", id)
		}
		ids[id] = true
	}
	first := publications[0].ID
	originalEventID := snapshot(first, video.DistributionPush)
	for i := uint(2); i <= 12; i++ {
		must(t, db.Create(&social.Social{FollowerID: i, VloggerID: 1}).Error)
	}

	// All failures occur while the existing author would switch to Pull.
	for _, tc := range []struct {
		table, setup, cleanup string
	}{
		{"videos", "CREATE TRIGGER reject_publish BEFORE INSERT ON videos FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'reject_publish'", "DROP TRIGGER reject_publish"},
		{"outbox_msgs", "CREATE TRIGGER reject_publish BEFORE INSERT ON outbox_msgs FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'reject_publish'", "DROP TRIGGER reject_publish"},
		{"socials", "RENAME TABLE socials TO unavailable_socials", "RENAME TABLE unavailable_socials TO socials"},
	} {
		t.Run("rollback_"+tc.table, func(t *testing.T) {
			exec(t, tc.setup)
			attempt := newVideo()
			err := svc.Publish(ctx, attempt)
			exec(t, tc.cleanup)
			if err == nil {
				t.Fatal("expected Publish to fail")
			}
			if !attempt.CreateTime.IsZero() {
				t.Fatal("failed Publish left a generated timestamp on the input")
			}
			state(t, video.DistributionPush)
			for _, model := range []any{&video.Video{}, &video.OutboxMsg{}} {
				var count int64
				must(t, db.Model(model).Count(&count).Error)
				if count != int64(len(publications)+1) {
					t.Fatalf("%T count = %d after failed publish", model, count)
				}
			}
		})
	}

	v := newVideo()
	must(t, svc.Publish(ctx, v))
	state(t, video.DistributionPull)
	snapshot(v.ID, video.DistributionPull)
	if got := snapshot(first, video.DistributionPush); got != originalEventID {
		t.Fatalf("event ID changed from %s to %s", originalEventID, got)
	}
	// Reopen the pool and recreate the service, then publish in the hysteresis band.
	sqlDB, err := db.DB()
	must(t, err)
	must(t, sqlDB.Close())
	db = open()
	svc = video.NewVideoService(video.NewVideoRepository(db), nil, thresholds)
	state(t, video.DistributionPull)
	exec(t, "DELETE FROM socials WHERE follower_id = 12")
	v = newVideo()
	must(t, svc.Publish(ctx, v))
	snapshot(v.ID, video.DistributionPull)
	exec(t, "DELETE FROM socials WHERE follower_id = 11")
	v = newVideo()
	must(t, svc.Publish(ctx, v))
	state(t, video.DistributionPush)
	snapshot(v.ID, video.DistributionPush)
	must(t, appdb.AutoMigrate(db))
	state(t, video.DistributionPush)
	if got := snapshot(first, video.DistributionPush); got != originalEventID {
		t.Fatalf("migration changed event ID from %s to %s", originalEventID, got)
	}

	t.Run("explicit_service_timestamp", func(t *testing.T) {
		v := newVideo()
		supplied := time.Date(2025, 1, 2, 3, 4, 5, 123987654, cfg.Loc)
		v.CreateTime = supplied
		must(t, svc.Publish(ctx, v))
		var stored video.Video
		var event video.OutboxMsg
		must(t, db.First(&stored, v.ID).Error)
		must(t, db.Where("video_id = ?", v.ID).Take(&event).Error)
		want := supplied.Truncate(time.Millisecond)
		if !v.CreateTime.Equal(want) || !stored.CreateTime.Equal(want) || !event.CreateTime.Equal(want) {
			t.Fatalf("explicit time: response=%v video=%v outbox=%v, want %v", v.CreateTime, stored.CreateTime, event.CreateTime, want)
		}
	})

	var published video.Video
	if !t.Run("http_publish_after_author_lock", func(t *testing.T) {
		// A distinct high threshold proves the router passes config to Publish.
		router := apphttp.SetRouter(db, nil, nil, config.FollowingFeedConfig{LowThreshold: 7, InitialThreshold: 8, HighThreshold: 9})
		token, err := auth.GenerateToken(1, "author")
		must(t, err)
		must(t, db.Model(&account.Account{}).Where("id = ?", 1).Update("token", token).Error)
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		lock := db.WithContext(waitCtx).Begin()
		must(t, lock.Error)
		defer lock.Rollback()
		must(t, lock.Exec("SELECT id FROM accounts WHERE id = 1 FOR UPDATE").Error)

		req := httptest.NewRequest("POST", "/video/publish", strings.NewReader(`{"title":"HTTP publication","play_url":"/play","cover_url":"/cover","create_time":"2001-01-01T00:00:00Z"}`)).WithContext(waitCtx)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		res := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			router.ServeHTTP(res, req)
		}()
		defer func() {
			_ = lock.Rollback().Error
			<-done
		}()

		// Wait for MySQL to report the request queued on the account row.
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			var waiting int
			must(t, admin.QueryRowContext(waitCtx, `SELECT EXISTS (
				SELECT 1 FROM performance_schema.data_locks
				WHERE OBJECT_SCHEMA = ? AND OBJECT_NAME = 'accounts'
				  AND INDEX_NAME = 'PRIMARY' AND LOCK_STATUS = 'WAITING'
			)`, schema).Scan(&waiting))
			if waiting == 1 {
				break
			}
			select {
			case <-done:
				t.Fatalf("publish completed before lock release: %d %s", res.Code, res.Body.String())
			case <-waitCtx.Done():
				t.Fatal("publish did not reach the author row lock", waitCtx.Err())
			case <-tick.C:
			}
		}
		// Separate the observed wait from release even at millisecond precision.
		boundary := time.Now().Truncate(time.Millisecond).Add(time.Millisecond)
		timer := time.NewTimer(time.Until(boundary))
		defer timer.Stop()
		<-timer.C
		must(t, lock.Commit().Error)
		<-done
		if res.Code != 200 || strings.Contains(res.Body.String(), "distribution_mode") {
			t.Fatalf("publish returned %d: %s", res.Code, res.Body.String())
		}
		must(t, json.Unmarshal(res.Body.Bytes(), &published))
		var stored video.Video
		var event video.OutboxMsg
		must(t, db.First(&stored, published.ID).Error)
		must(t, db.Where("video_id = ?", published.ID).Take(&event).Error)
		if published.CreateTime.Before(boundary) || published.CreateTime.After(time.Now()) ||
			!published.CreateTime.Equal(published.CreateTime.Truncate(time.Millisecond)) ||
			!published.CreateTime.Equal(stored.CreateTime) || !published.CreateTime.Equal(event.CreateTime) {
			t.Fatalf("publish times: boundary=%v response=%v video=%v outbox=%v", boundary, published.CreateTime, stored.CreateTime, event.CreateTime)
		}
		if stored.DistributionMode == nil || *stored.DistributionMode != video.DistributionPull ||
			event.DistributionMode == nil || *event.DistributionMode != video.DistributionPull {
			t.Fatalf("HTTP publish did not snapshot the mode from router thresholds: %+v %+v", stored, event)
		}
	}) {
		return
	}

	t.Run("global_timeline", func(t *testing.T) {
		amqpURL, redisAddr := os.Getenv("FOLLOWING_TEST_AMQP_URL"), os.Getenv("C_TEST_REDIS_ADDR")
		if amqpURL == "" || redisAddr == "" {
			t.Skip("set isolated FOLLOWING_TEST_AMQP_URL and C_TEST_REDIS_ADDR for timeline regression")
		}
		uri, err := amqp.ParseURI(amqpURL)
		if err != nil || !strings.HasPrefix(uri.Vhost, "gofeed-following-test-") {
			t.Fatal("isolated gofeed-following-test-* vhost required")
		}
		mq, err := rabbitmq.NewRabbitMQ(&config.RabbitMQConfig{Host: uri.Host, Port: uri.Port, Username: uri.Username, Password: uri.Password, VHost: uri.Vhost})
		must(t, err)
		defer mq.Close()
		timeline, err := rabbitmq.NewTimelineMQ(mq)
		must(t, err)
		raw := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 15})
		defer raw.Close()
		cache := rediscache.NewClient(raw, schema+":")
		defer cache.DelByPattern(context.Background(), schema+":*")
		worker.StartConsumer(timeline, "video.timeline.update.queue", cache, mq)
		worker.StartOutboxPoller(db, timeline)

		router := apphttp.SetRouter(db, cache, nil)
		key := cache.Key("feed:global_timeline")
		for _, id := range []uint{oldVideo.ID, published.ID} {
			deadline := time.Now().Add(5 * time.Second)
			for {
				score, err := raw.ZScore(ctx, key, strconv.FormatUint(uint64(id), 10)).Result()
				if err == nil {
					if id == published.ID && score != float64(published.CreateTime.UnixMilli()) {
						t.Fatalf("timeline score = %.0f, want %d", score, published.CreateTime.UnixMilli())
					}
					break
				}
				if err != redis.Nil || time.Now().After(deadline) {
					t.Fatalf("timeline missing video %d: %v", id, err)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		res := httptest.NewRecorder()
		router.ServeHTTP(res, httptest.NewRequest("POST", "/feed/listLatest", strings.NewReader(`{"limit":10}`)))
		if res.Code != 200 || !strings.Contains(res.Body.String(), "HTTP publication") {
			t.Fatalf("global feed returned %d: %s", res.Code, res.Body.String())
		}
	})
}
