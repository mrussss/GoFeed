package worker_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"feedsystem_video_go/internal/config"
	appdb "feedsystem_video_go/internal/db"
	apphttp "feedsystem_video_go/internal/http"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	drivermysql "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	redis "github.com/redis/go-redis/v9"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Existing A1/A2/B suites cover the detailed transaction and delivery failures.
// This suite connects the production router, JWT, Relay, consumers and cache.
func TestLikeCommandPipeline(t *testing.T) {
	dsn, amqpURL, redisAddr := os.Getenv("LIKE_COMMAND_TEST_DSN"), os.Getenv("C_TEST_AMQP_URL"), os.Getenv("C_TEST_REDIS_ADDR")
	if dsn == "" || amqpURL == "" || redisAddr == "" {
		t.Skip("set LIKE_COMMAND_TEST_DSN, C_TEST_AMQP_URL and C_TEST_REDIS_ADDR for isolated end-to-end verification")
	}
	uri, err := amqp.ParseURI(amqpURL)
	if err != nil || !strings.HasPrefix(uri.Vhost, "gofeed-c-test-") {
		t.Fatal("isolated gofeed-c-test-* vhost required")
	}
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	cfg.DBName = ""
	cfg.ParseTime = true
	cfg.Timeout = 3 * time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("gofeed_c_test_%d", time.Now().UnixNano())
	if _, err = admin.Exec("CREATE DATABASE `" + schema + "`"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP DATABASE `" + schema + "`"); err != nil {
			t.Error(err)
		}
	}()
	cfg.DBName = schema
	db, err := gorm.Open(gormmysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err = appdb.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	// C_TEST_REDIS_ADDR must be a disposable instance; DB 15 is reserved for this suite.
	raw := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 15})
	defer raw.Close()
	if err = raw.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	// The existing limiter uses fixed keys rather than Client.Key's namespace.
	limitKeys := []string{"feedsystem:ratelimit:account_register:127.0.0.1", "feedsystem:ratelimit:account_login:127.0.0.1", "feedsystem:ratelimit:like_write:1"}
	if err := raw.Del(ctx, limitKeys...).Err(); err != nil {
		t.Fatal(err)
	}
	defer raw.Del(context.Background(), limitKeys...)
	prefix := schema + ":"
	cache := rediscache.NewClient(raw, prefix)
	defer func() { _ = cache.DelByPattern(context.Background(), prefix+"*") }()
	// No API MQ connection: accepting commands must work without RabbitMQ.
	server := httptest.NewServer(apphttp.SetRouter(db, cache, nil))
	defer server.Close()
	token := ""
	request := func(path string, body any, want int) map[string]json.RawMessage {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(ctx, "POST", server.URL+path, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err = io.ReadAll(res.Body)
		if err != nil || res.StatusCode != want {
			t.Fatalf("%s: status=%d body=%s err=%v", path, res.StatusCode, data, err)
		}
		var result map[string]json.RawMessage
		if len(data) > 0 && data[0] == '{' {
			if err = json.Unmarshal(data, &result); err != nil {
				t.Fatal(err)
			}
		}
		return result
	}
	request("/like/like", map[string]any{"video_id": 10, "request_id": "unauthorized", "desired_liked": true}, 401)
	request("/account/register", map[string]string{"username": "phase_c_user", "password": "phase-c-password"}, 200)
	login := request("/account/login", map[string]string{"username": "phase_c_user", "password": "phase-c-password"}, 200)
	if err = json.Unmarshal(login["token"], &token); err != nil {
		t.Fatal(err)
	}
	var accountID uint
	if err = json.Unmarshal(login["account_id"], &accountID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint{10, 11} {
		if err = db.Create(&video.Video{ID: id, AuthorID: accountID, Username: "phase_c_user", Title: "C demo", PlayURL: "demo", CoverURL: "demo"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	accept := func(id uint, requestID string, liked bool, want int) map[string]json.RawMessage {
		t.Helper()
		path := "/like/unlike"
		if liked {
			path = "/like/like"
		}
		return request(path, map[string]any{"request_id": requestID, "video_id": id, "desired_liked": liked}, want)
	}
	first := accept(10, "like-first", true, 202)
	repeated := accept(10, "like-first", true, 202)
	if string(first["status"]) != `"accepted"` || string(first["revision"]) != `"1"` || string(first["command_id"]) != string(repeated["command_id"]) {
		t.Fatal("unstable command acceptance identity")
	}
	accept(10, "like-first", false, 409)
	accept(999999, "missing", true, 404)
	request("/like/like", map[string]any{"video_id": 10, "request_id": "bad", "desired_liked": true, "account_id": 999}, 400)
	var commands int64
	if err = db.Model(&video.CommandOutbox{}).Count(&commands).Error; err != nil || commands != 1 {
		t.Fatalf("duplicate commands: %d %v", commands, err)
	}
	repo := video.NewLikeRepository(db)
	mqcfg := config.RabbitMQConfig{Host: uri.Host, Port: uri.Port, Username: uri.Username, Password: uri.Password, VHost: uri.Vhost, LikePublishTimeoutMS: 500}
	unavailable := mqcfg
	unavailable.Port = 1
	offline := rabbitmq.NewLikeCommandMQ(unavailable)
	if _, err = worker.NewLikeCommandRelay(db, offline, time.Second).RunOnce(ctx); err == nil {
		t.Fatal("offline broker should fail publishing")
	}
	offline.Close()
	var pending int64
	if err = db.Model(&video.CommandOutbox{}).Where("published_at IS NULL").Count(&pending).Error; err != nil || pending != 1 {
		t.Fatal("offline broker lost durable intent")
	}
	detailKey := cache.Key("video:detail:id=%d", 10)
	request("/video/getDetail", map[string]uint{"id": 10}, 200)
	entityKey := cache.Key("video:entity:%d", 10)
	if err := raw.Set(ctx, entityKey, `{"id":10,"likes_count":0}`, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if ttl := raw.TTL(ctx, detailKey).Val(); ttl <= 0 || ttl > 60*time.Second {
		t.Fatalf("detail TTL: %v", ttl)
	}
	controlConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	defer controlConn.Close()
	control, err := controlConn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	publisher := rabbitmq.NewLikeCommandMQ(mqcfg)
	defer publisher.Close()
	relay := worker.NewLikeCommandRelay(db, publisher, 10*time.Millisecond)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 2)
	go func() { done <- worker.RunLikeCommandConsumers(runCtx, mqcfg, repo, cache) }()
	go func() { done <- relay.Run(runCtx) }()
	running := true
	shutdown := func() {
		if running {
			stop()
			<-done
			<-done
			running = false
		}
	}
	defer shutdown()
	wait := func(description string, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(8 * time.Second)
		for !condition() {
			if time.Now().After(deadline) || ctx.Err() != nil {
				t.Fatal("timeout: " + description)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	state := func() video.Like {
		t.Helper()
		var row video.Like
		if err := db.Where("account_id=? AND video_id=10", accountID).Take(&row).Error; err != nil {
			t.Fatal(err)
		}
		return row
	}
	count := func() int64 {
		t.Helper()
		var v video.Video
		if err := db.First(&v, 10).Error; err != nil {
			t.Fatal(err)
		}
		return v.LikesCount
	}
	wait("first like applied", func() bool { return state().AppliedRevision == 1 && count() == 1 })
	liked := request("/like/isLiked", map[string]uint{"video_id": 10}, 200)
	if string(liked["is_liked"]) != "true" {
		t.Fatal("API does not read applied status")
	}
	wait("detail cache invalidated", func() bool {
		result := request("/video/getDetail", map[string]uint{"id": 10}, 200)
		return string(result["likes_count"]) == "1"
	})
	wait("feed entity cache invalidated", func() bool { return raw.Exists(ctx, entityKey).Val() == 0 })
	t.Log("registered/logged in -> HTTP 202 -> durable Outbox -> Relay -> MQ -> Batch -> isLiked=true, likes_count=1")

	// Kill only this isolated command-consumer connection at the broker. A new
	// connection must recreate all four consumers, without duplicate groups.
	management := os.Getenv("RELAY_TEST_MANAGEMENT_URL")
	if management == "" {
		t.Fatal("set RELAY_TEST_MANAGEMENT_URL for real consumer reconnection verification")
	}
	adminRequest := func(method, path string) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(management, "/")+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth(uri.Username, uri.Password)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		b, _ := io.ReadAll(response.Body)
		if response.StatusCode >= 300 {
			t.Fatalf("broker management %s: %d %s", path, response.StatusCode, b)
		}
		return b
	}
	var consumerConnection string
	wait("four consumers visible to broker management", func() bool {
		var consumers []struct {
			ChannelDetails json.RawMessage `json:"channel_details"`
		}
		if err := json.Unmarshal(adminRequest("GET", "/api/consumers/"+url.PathEscape(uri.Vhost)), &consumers); err != nil {
			t.Fatal(err)
		}
		if len(consumers) != 4 {
			return false
		}
		for _, consumer := range consumers {
			var details struct {
				ConnectionName string `json:"connection_name"`
			}
			// RabbitMQ management briefly reports [] before channel statistics arrive.
			if json.Unmarshal(consumer.ChannelDetails, &details) != nil || details.ConnectionName == "" {
				return false
			}
			consumerConnection = details.ConnectionName
		}
		return true
	})
	// The initial 5s socket deadline is a handshake bound, cleared by amqp091-go.
	// Observe the same live connection after it, before injecting a real close.
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(6 * time.Second):
	}
	var live []struct {
		ChannelDetails struct {
			ConnectionName string `json:"connection_name"`
		} `json:"channel_details"`
	}
	if err := json.Unmarshal(adminRequest("GET", "/api/consumers/"+url.PathEscape(uri.Vhost)), &live); err != nil || len(live) != 4 {
		t.Fatalf("consumers after handshake deadline: %d %v", len(live), err)
	}
	for _, consumer := range live {
		if consumer.ChannelDetails.ConnectionName != consumerConnection {
			t.Fatal("healthy connection was unexpectedly replaced")
		}
	}
	adminRequest("DELETE", "/api/connections/"+url.PathEscape(consumerConnection))
	second := accept(10, "unlike-second", false, 202)
	if string(second["revision"]) != `"2"` {
		t.Fatal("server revision did not advance")
	}
	wait("consumer connection recovery and unlike", func() bool { return state().AppliedRevision == 2 && count() == 0 })
	if row := state(); row.Status != 0 || row.NextRevision != 2 || row.ID == 0 {
		t.Fatalf("unlike deleted relation: %+v", row)
	}
	liked = request("/like/isLiked", map[string]uint{"video_id": 10}, 200)
	batch, err := repo.BatchGetLiked(ctx, []uint{10}, accountID)
	if err != nil {
		t.Fatal(err)
	}
	list, err := repo.ListLikedVideos(ctx, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if string(liked["is_liked"]) != "false" || batch[10] || len(list) != 0 {
		t.Fatal("inactive relation leaked into reads")
	}
	// A cached following page may retain counts until TTL, but not an obsolete
	// per-account is_liked value.
	followingKey := cache.Key("feed:listByFollowing:limit=10:accountID=%d:before=0:id=0", accountID)
	if err := raw.Set(ctx, followingKey, `{"video_list":[{"id":10,"is_liked":true,"likes_count":1}],"has_more":false}`, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	following := request("/feed/listByFollowing", map[string]int{"limit": 10}, 200)
	var items []struct {
		IsLiked bool `json:"is_liked"`
	}
	if err := json.Unmarshal(following["video_list"], &items); err != nil || len(items) != 1 || items[0].IsLiked {
		t.Fatalf("following cache retained inactive like: %+v %v", items, err)
	}
	var firstCommand video.CommandOutbox
	if err = db.Where("account_id=? AND request_id='like-first'", accountID).Take(&firstCommand).Error; err != nil {
		t.Fatal(err)
	}
	old := rabbitmq.LikeCommandEvent{CommandID: firstCommand.ID, RequestID: firstCommand.RequestID, AccountID: accountID, VideoID: 10, DesiredLiked: true, Revision: 1}
	// Read the replayed commands directly from the broker after the consumer group
	// stops. Management ACK counters are sampled and can lag or include prior runs.
	shutdown()
	partition := rabbitmq.LikeCommandPartition(accountID, 10)
	queue := rabbitmq.LikeCommandQueue(partition)
	for i := 0; i < 2; i++ {
		if err = publisher.Publish(ctx, old); err != nil {
			t.Fatal(err)
		}
	}
	replayChannel, err := controlConn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	var replays []amqp.Delivery
	for {
		delivery, ok, err := replayChannel.Get(queue, false)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		replays = append(replays, delivery)
	}
	if len(replays) < 2 {
		t.Fatalf("expected both duplicate deliveries, got %d", len(replays))
	}
	if err = worker.NewLikeCommandBatchWorker(replayChannel, repo, partition, cache).HandleBatch(ctx, replays); err != nil {
		t.Fatal(err)
	}
	if err = replayChannel.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := control.Get(queue, false); err != nil || ok {
		t.Fatalf("old messages remain after replay: %v %v", ok, err)
	}
	if count() != 0 || state().AppliedRevision != 2 {
		t.Fatal("old revision changed the state")
	}
	t.Log("unlike retained status=0 row; server revision=2, isLiked=false, likes_count=0; broker-disconnected consumers recovered; duplicate old messages had no effect")

	// Reuse real MQ deliveries to verify cache failure cannot prevent commit/ACK.
	// Give this test fixture a short TTL instead of waiting 60 seconds.
	if err = raw.Set(ctx, detailKey, `{"id":10,"likes_count":0}`, 250*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	failedRaw := redis.NewClient(&redis.Options{Addr: redisAddr})
	_ = failedRaw.Close()
	failedCache := rediscache.NewClient(failedRaw, prefix)
	accept(10, "like-third", true, 202)
	if _, err = relay.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var delivery amqp.Delivery
	wait("cache-failure delivery", func() bool {
		var ok bool
		delivery, ok, err = control.Get(queue, false)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	})
	if err = worker.NewLikeCommandBatchWorker(control, repo, partition, failedCache).HandleBatch(ctx, []amqp.Delivery{delivery}); err != nil {
		t.Fatal(err)
	}
	for _, readCache := range []*rediscache.Client{nil, failedCache} {
		v, err := video.NewVideoService(video.NewVideoRepository(db), readCache).GetDetail(ctx, 10)
		if err != nil || v.LikesCount != 1 {
			t.Fatalf("detail cache fallback: %+v %v", v, err)
		}
	}
	if count() != 1 || state().AppliedRevision != 3 {
		t.Fatal("Redis failure prevented commit")
	}
	if _, ok, err := control.Get(queue, true); err != nil || ok {
		t.Fatalf("Redis failure prevented ACK: %v %v", ok, err)
	}
	if b, err := raw.Get(ctx, detailKey).Result(); err != nil || !strings.Contains(b, `"likes_count":0`) {
		t.Fatal("fixture did not retain stale cache after failed deletion")
	}
	wait("TTL expiry", func() bool { return raw.Exists(ctx, detailKey).Val() == 0 })
	detail := request("/video/getDetail", map[string]uint{"id": 10}, 200)
	if string(detail["likes_count"]) != "1" {
		t.Fatal("expired cache did not reload MySQL count")
	}
	t.Log("Redis deletion failed without preventing COMMIT or ACK; actual fixture TTL expired and detail reloaded MySQL")

	// Like-list ordering follows state changes, rather than the original row time.
	for i, step := range []struct {
		id    uint
		liked bool
	}{{11, true}, {10, false}, {10, true}} {
		accept(step.id, "order-"+strconv.Itoa(i), step.liked, 202)
		if _, err = relay.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		q := rabbitmq.LikeCommandQueue(rabbitmq.LikeCommandPartition(accountID, step.id))
		var d amqp.Delivery
		wait("ordering command", func() bool {
			var ok bool
			d, ok, err = control.Get(q, false)
			if err != nil {
				t.Fatal(err)
			}
			return ok
		})
		if err = worker.NewLikeCommandBatchWorker(control, repo, rabbitmq.LikeCommandPartition(accountID, step.id), cache).HandleBatch(ctx, []amqp.Delivery{d}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * time.Millisecond)
	}
	list, err = repo.ListLikedVideos(ctx, accountID)
	if err != nil || len(list) != 2 || list[0].ID != 10 || list[1].ID != 11 {
		t.Fatalf("updated_at ordering: %+v %v", list, err)
	}
	// A request ID remains authoritative even if a retry references a missing
	// video, or its originally accepted video has since been deleted.
	accept(999999, "like-first", true, 409)
	if err := db.Delete(&video.Video{}, 11).Error; err != nil {
		t.Fatal(err)
	}
	retryDeleted := accept(11, "order-0", true, 202)
	if string(retryDeleted["revision"]) != `"1"` {
		t.Fatal("retry of deleted video lost original revision")
	}
}
