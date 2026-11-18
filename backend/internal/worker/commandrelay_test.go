package worker_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	drivermysql "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type commandPublisherFunc func(context.Context, rabbitmq.LikeCommandEvent) error

func (f commandPublisherFunc) Publish(ctx context.Context, e rabbitmq.LikeCommandEvent) error {
	return f(ctx, e)
}

func TestLikeCommandPartition(t *testing.T) {
	// Fixed vectors pin the hash across accounts and process restarts.
	for _, v := range []struct {
		account, video uint
		partition      int
	}{{1, 3, 0}, {1, 4, 1}, {1, 1, 2}, {1, 2, 3}, {2, 1, 2}, {999, 1, 2}, {1, 256, 0}} {
		if got := rabbitmq.LikeCommandPartition(v.account, v.video); got != v.partition {
			t.Fatalf("partition(%d,%d)=%d", v.account, v.video, got)
		}
	}
}

func TestLikeCommandRelay(t *testing.T) {
	dsn := os.Getenv("LIKE_COMMAND_TEST_DSN")
	if dsn == "" {
		t.Skip("set LIKE_COMMAND_TEST_DSN for isolated real MySQL relay tests")
	}
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid MySQL test DSN")
	}
	cfg.DBName = ""
	cfg.ParseTime = true
	cfg.Timeout = 3 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("gofeed_a2_test_%d", time.Now().UnixNano())
	if _, err = admin.Exec("CREATE DATABASE `" + schema + "`"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP DATABASE `" + schema + "`"); err != nil {
			t.Errorf("cleanup: %v", err)
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
	if err := db.AutoMigrate(&video.Like{}, &video.CommandOutbox{}, &video.Video{}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	reset := func(t *testing.T) {
		t.Helper()
		if err := db.Exec("DELETE FROM command_outbox").Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("DELETE FROM likes").Error; err != nil {
			t.Fatal(err)
		}
	}
	insert := func(t *testing.T) video.CommandOutbox {
		t.Helper()
		c, err := video.NewLikeRepository(db).AcceptLikeCommand(ctx, 7, video.LikeCommandRequest{RequestID: "relay", VideoID: 9, DesiredLiked: true})
		if err != nil {
			t.Fatal(err)
		}
		return *c
	}
	assertPending := func(t *testing.T, id uint64) {
		t.Helper()
		var row video.CommandOutbox
		if err := db.First(&row, id).Error; err != nil {
			t.Fatal(err)
		}
		if row.PublishedAt != nil {
			t.Fatal("failed/unknown publish was marked successful")
		}
	}
	seedCommands := func(t *testing.T, size int) []video.CommandOutbox {
		t.Helper()
		commands := make([]video.CommandOutbox, size)
		for i := range commands {
			commands[i] = video.CommandOutbox{AccountID: uint(i + 100), RequestID: fmt.Sprint(i), VideoID: 1, Revision: 1}
		}
		if err := db.Create(&commands).Error; err != nil {
			t.Fatal(err)
		}
		return commands
	}
	pendingCount := func(t *testing.T) int64 {
		t.Helper()
		var count int64
		if err := db.Model(&video.CommandOutbox{}).Where("published_at IS NULL").Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		return count
	}
	t.Run("marks_at_fifty_then_flushes_remainder", func(t *testing.T) {
		reset(t)
		commands := seedCommands(t, 55)
		var updates []int64
		const callback = "a2_capture_mark_batches"
		if err := db.Callback().Update().After("gorm:update").Register(callback, func(tx *gorm.DB) {
			if tx.Statement.Table == "command_outbox" {
				updates = append(updates, tx.RowsAffected)
			}
		}); err != nil {
			t.Fatal(err)
		}
		defer db.Callback().Update().Remove(callback)
		sent := 0
		p := commandPublisherFunc(func(_ context.Context, e rabbitmq.LikeCommandEvent) error {
			if e.CommandID != commands[sent].ID {
				t.Fatalf("publish out of order at offset %d: %d", sent, e.CommandID)
			}
			sent++
			if sent == 50 && pendingCount(t) != 55 {
				t.Fatal("marked before the 50th Confirm")
			}
			if sent == 51 && pendingCount(t) != 5 {
				t.Fatal("first 50 confirmed rows were not marked")
			}
			return nil
		})
		relay := worker.NewLikeCommandRelay(db, p, time.Millisecond)
		if n, err := relay.RunOnce(ctx); n != 55 || err != nil || pendingCount(t) != 0 {
			t.Fatalf("result: %d %v", n, err)
		}
		if n, err := relay.RunOnce(ctx); n != 0 || err != nil {
			t.Fatalf("empty result: %d %v", n, err)
		}
		if len(updates) != 2 || updates[0] != 50 || updates[1] != 5 {
			t.Fatalf("rows per update: %v, want [50 5]", updates)
		}
	})
	t.Run("flushes_confirmed_prefix_on_publish_error", func(t *testing.T) {
		reset(t)
		commands := seedCommands(t, 55)
		publishCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		failure := &rabbitmq.PublishError{Code: "connection_lost", Unknown: true}
		sent := 0
		p := commandPublisherFunc(func(context.Context, rabbitmq.LikeCommandEvent) error {
			sent++
			if sent == 30 {
				cancel()
				return failure
			}
			return nil
		})
		if n, err := worker.NewLikeCommandRelay(db, p, time.Millisecond).RunOnce(publishCtx); n != 29 || !errors.Is(err, failure) {
			t.Fatalf("result: %d %v", n, err)
		}
		var pending []uint64
		if err := db.Model(&video.CommandOutbox{}).Where("published_at IS NULL").Order("id").Pluck("id", &pending).Error; err != nil {
			t.Fatal(err)
		}
		if len(pending) != len(commands)-29 {
			t.Fatalf("pending count: %d", len(pending))
		}
		for i, id := range pending {
			if id != commands[i+29].ID {
				t.Fatalf("wrong command remained pending: %d at offset %d", id, i)
			}
		}
	})
	t.Run("unexpected_rows_affected_is_an_error", func(t *testing.T) {
		reset(t)
		commands := seedCommands(t, 2)
		p := commandPublisherFunc(func(_ context.Context, e rabbitmq.LikeCommandEvent) error {
			if e.CommandID == commands[0].ID {
				return db.Model(&commands[0]).UpdateColumn("published_at", gorm.Expr("NOW(3)")).Error
			}
			return nil
		})
		if n, err := worker.NewLikeCommandRelay(db, p, time.Millisecond).RunOnce(ctx); n != 1 || err == nil || !strings.Contains(err.Error(), "updated 1 rows, expected 2") {
			t.Fatalf("result: %d %v", n, err)
		}
	})
	for _, code := range []string{"broker_nack", "unroutable", "confirm_timeout", "connection_lost"} {
		t.Run("failure_retains_pending/"+code, func(t *testing.T) {
			reset(t)
			c := insert(t)
			failure := &rabbitmq.PublishError{Code: code, Unknown: code == "confirm_timeout" || code == "connection_lost"}
			p := commandPublisherFunc(func(context.Context, rabbitmq.LikeCommandEvent) error { return failure })
			n, err := worker.NewLikeCommandRelay(db, p, time.Millisecond).RunOnce(ctx)
			if n != 0 || !errors.Is(err, failure) {
				t.Fatalf("result: %d %v", n, err)
			}
			assertPending(t, c.ID)
		})
	}
	t.Run("broker_unavailable", func(t *testing.T) {
		reset(t)
		c := insert(t)
		p := rabbitmq.NewLikeCommandMQ(config.RabbitMQConfig{Host: "127.0.0.1", Port: 1, LikePublishTimeoutMS: 100})
		defer p.Close()
		if n, err := worker.NewLikeCommandRelay(db, p, time.Millisecond).RunOnce(ctx); n != 0 || err == nil {
			t.Fatalf("result: %d %v", n, err)
		}
		assertPending(t, c.ID)
	})
	t.Run("mark_failure_republishes_same_identity", func(t *testing.T) {
		reset(t)
		commands := seedCommands(t, 2)
		var sent []rabbitmq.LikeCommandEvent
		p := commandPublisherFunc(func(_ context.Context, e rabbitmq.LikeCommandEvent) error {
			sent = append(sent, e)
			return nil
		})
		trigger := fmt.Sprintf(`CREATE TRIGGER a2_fail_mark BEFORE UPDATE ON command_outbox FOR EACH ROW
			BEGIN IF NEW.id = %d THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='a2 mark failure'; END IF; END`, commands[1].ID)
		if err := db.Exec(trigger).Error; err != nil {
			t.Fatal(err)
		}
		if n, err := worker.NewLikeCommandRelay(db, p, time.Millisecond).RunOnce(ctx); n != 0 || err == nil {
			t.Fatalf("mark failure: %d %v", n, err)
		}
		if pendingCount(t) != int64(len(commands)) {
			t.Fatal("failed update partly marked the batch")
		}
		if err := db.Exec("DROP TRIGGER a2_fail_mark").Error; err != nil {
			t.Fatal(err)
		}
		if n, err := worker.NewLikeCommandRelay(db, p, time.Millisecond).RunOnce(ctx); n != len(commands) || err != nil {
			t.Fatalf("retry: %d %v", n, err)
		}
		if len(sent) != 4 || sent[0] != sent[2] || sent[1] != sent[3] ||
			sent[0].CommandID != commands[0].ID || sent[1].CommandID != commands[1].ID {
			t.Fatalf("retry identity changed: %+v", sent)
		}
	})
	t.Run("run_retries_and_cancels", func(t *testing.T) {
		reset(t)
		c := insert(t)
		runCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		attempts := 0
		p := commandPublisherFunc(func(context.Context, rabbitmq.LikeCommandEvent) error {
			attempts++
			if attempts == 2 {
				cancel()
			}
			return &rabbitmq.PublishError{Code: "publisher_unavailable"}
		})
		started := time.Now()
		err := worker.NewLikeCommandRelay(db, p, 20*time.Millisecond).Run(runCtx)
		if !errors.Is(err, context.Canceled) || attempts != 2 || time.Since(started) < 20*time.Millisecond {
			t.Fatalf("retry/cancel: attempts=%d err=%v", attempts, err)
		}
		assertPending(t, c.ID)
	})

	t.Run("run_drains_backlog_without_interval_wait", func(t *testing.T) {
		reset(t)
		commands := make([]video.CommandOutbox, 105)
		for i := range commands {
			commands[i] = video.CommandOutbox{AccountID: uint(i + 100), RequestID: fmt.Sprint(i), VideoID: 1, Revision: 1}
		}
		if err := db.Create(&commands).Error; err != nil {
			t.Fatal(err)
		}
		runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		finished := make(chan struct{})
		var runErr error
		p := commandPublisherFunc(func(context.Context, rabbitmq.LikeCommandEvent) error { return nil })
		go func() { runErr = worker.NewLikeCommandRelay(db, p, time.Hour).Run(runCtx); close(finished) }()
		defer func() { cancel(); <-finished }()
		for {
			var pending int64
			if err := db.Model(&video.CommandOutbox{}).Where("published_at IS NULL").Count(&pending).Error; err != nil {
				t.Fatal(err)
			}
			if pending == 0 {
				break
			}
			select {
			case <-finished:
				t.Fatalf("backlog not drained before cancellation: pending=%d err=%v", pending, runErr)
			case <-time.After(20 * time.Millisecond):
			}
		}
		cancel()
		<-finished
		if !errors.Is(runErr, context.Canceled) {
			t.Fatal(runErr)
		}
	})
	t.Run("run_empty_queue_waits_and_cancels", func(t *testing.T) {
		reset(t)
		var queries atomic.Int32
		firstQuery := make(chan struct{})
		if err := db.Callback().Query().After("gorm:query").Register("a2_count_empty_polls", func(tx *gorm.DB) {
			if tx.Statement.Table == "command_outbox" && queries.Add(1) == 1 {
				close(firstQuery)
			}
		}); err != nil {
			t.Fatal(err)
		}
		defer db.Callback().Query().Remove("a2_count_empty_polls")
		runCtx, cancel := context.WithTimeout(ctx, time.Second)
		finished := make(chan struct{})
		var runErr error
		p := commandPublisherFunc(func(context.Context, rabbitmq.LikeCommandEvent) error {
			t.Error("empty queue published a message")
			return nil
		})
		go func() { runErr = worker.NewLikeCommandRelay(db, p, time.Hour).Run(runCtx); close(finished) }()
		defer func() { cancel(); <-finished }()
		select {
		case <-firstQuery:
		case <-finished:
			t.Fatalf("empty relay stopped unexpectedly: %v", runErr)
		}
		time.Sleep(40 * time.Millisecond)
		cancel()
		<-finished
		if queries.Load() != 1 || !errors.Is(runErr, context.Canceled) {
			t.Fatalf("empty queue busy loop/cancellation: queries=%d err=%v", queries.Load(), runErr)
		}
	})

	t.Run("hundred_row_limit_restart_and_late_commit", func(t *testing.T) {
		reset(t)
		commands := make([]video.CommandOutbox, 105)
		for i := range commands {
			commands[i] = video.CommandOutbox{AccountID: uint(i + 100), RequestID: fmt.Sprint(i), VideoID: 1, Revision: 1}
		}
		if err := db.Create(&commands).Error; err != nil {
			t.Fatal(err)
		}
		p := commandPublisherFunc(func(context.Context, rabbitmq.LikeCommandEvent) error { return nil })
		if n, err := worker.NewLikeCommandRelay(db, p, time.Millisecond).RunOnce(ctx); n != 100 || err != nil {
			t.Fatalf("first round: %d %v", n, err)
		}
		// A new Relay object has no cursor or in-memory state to restore.
		relay := worker.NewLikeCommandRelay(db, p, time.Millisecond)
		if n, err := relay.RunOnce(ctx); n != 5 || err != nil {
			t.Fatalf("restart: %d %v", n, err)
		}
		tx := db.Begin()
		if tx.Error != nil {
			t.Fatal(tx.Error)
		}
		defer tx.Rollback()
		late := video.CommandOutbox{AccountID: 500, RequestID: "late", VideoID: 1, Revision: 1}
		if err := tx.Create(&late).Error; err != nil {
			t.Fatal(err)
		}
		newer := video.CommandOutbox{AccountID: 501, RequestID: "newer", VideoID: 1, Revision: 1}
		if err := db.Create(&newer).Error; err != nil {
			t.Fatal(err)
		}
		if n, err := relay.RunOnce(ctx); n != 1 || err != nil {
			t.Fatalf("newer visible first: %d %v", n, err)
		}
		if err := tx.Commit().Error; err != nil {
			t.Fatal(err)
		}
		if n, err := relay.RunOnce(ctx); n != 1 || err != nil {
			t.Fatalf("late commit skipped: %d %v", n, err)
		}
	})
	t.Run("real_rabbitmq", func(t *testing.T) {
		uriText := os.Getenv("RELAY_TEST_AMQP_URL")
		if uriText == "" {
			t.Skip("set RELAY_TEST_AMQP_URL using an isolated gofeed-a2-test-* vhost")
		}
		uri, err := amqp.ParseURI(uriText)
		if err != nil {
			t.Fatal("invalid RabbitMQ test URI")
		}
		if len(uri.Vhost) < 15 || uri.Vhost[:15] != "gofeed-a2-test-" {
			t.Fatal("test requires a gofeed-a2-test-* vhost")
		}
		conn, err := amqp.Dial(uriText)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		defer ch.Close()
		rabbitCfg := config.RabbitMQConfig{Host: uri.Host, Port: uri.Port, Username: uri.Username, Password: uri.Password, VHost: uri.Vhost, LikePublishTimeoutMS: 1000}
		p := rabbitmq.NewLikeCommandMQ(rabbitCfg)
		defer p.Close()
		defer func() {
			for i := 0; i < rabbitmq.LikeCommandPartitions; i++ {
				_, _ = ch.QueuePurge(rabbitmq.LikeCommandQueue(i), false)
			}
		}()
		relay := worker.NewLikeCommandRelay(db, p, time.Millisecond)
		reset(t)
		c := insert(t)
		if n, err := relay.RunOnce(ctx); n != 1 || err != nil {
			t.Fatalf("real confirm: %d %v", n, err)
		}
		partition := rabbitmq.LikeCommandPartition(c.AccountID, c.VideoID)
		d, ok, err := ch.Get(rabbitmq.LikeCommandQueue(partition), true)
		if err != nil || !ok || d.MessageId != strconv.FormatUint(c.ID, 10) || d.DeliveryMode != amqp.Persistent {
			t.Fatalf("delivery: present=%v err=%v", ok, err)
		}
		var event rabbitmq.LikeCommandEvent
		if err := json.Unmarshal(d.Body, &event); err != nil || event.CommandID != c.ID || event.Revision != c.Revision || event.RequestID != c.RequestID || !event.DesiredLiked {
			t.Fatalf("body changed: %s %v", d.Body, err)
		}
		var stored video.CommandOutbox
		if err := db.First(&stored, c.ID).Error; err != nil || stored.PublishedAt == nil {
			t.Fatalf("real ACK not marked: %v", err)
		}
		for i := 0; i < 4; i++ {
			if _, ok, err := ch.Get(rabbitmq.LikeCommandQueue(i), true); err != nil || ok {
				t.Fatalf("wrong/duplicate partition %d: %v %v", i, ok, err)
			}
		}

		// A MySQL failure AFTER real broker ACK must permit an actual duplicate send.
		if err := db.Model(&stored).UpdateColumn("published_at", nil).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(`CREATE TRIGGER a2_fail_mark BEFORE UPDATE ON command_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='a2 mark failure'`).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := relay.RunOnce(ctx); err == nil {
			t.Fatal("expected mark failure")
		}
		if err := db.Exec("DROP TRIGGER a2_fail_mark").Error; err != nil {
			t.Fatal(err)
		}
		if n, err := worker.NewLikeCommandRelay(db, p, time.Millisecond).RunOnce(ctx); n != 1 || err != nil {
			t.Fatalf("restarted relay retry: %d %v", n, err)
		}
		if err := db.Create(&video.Video{ID: c.VideoID, AuthorID: c.AccountID}).Error; err != nil {
			t.Fatal(err)
		}
		batchWorker := worker.NewLikeCommandBatchWorker(ch, video.NewLikeRepository(db), partition, nil)
		for i := 0; i < 2; i++ {
			duplicate, ok, err := ch.Get(rabbitmq.LikeCommandQueue(partition), false)
			if err != nil || !ok || duplicate.MessageId != d.MessageId || string(duplicate.Body) != string(d.Body) {
				t.Fatalf("duplicate identity changed: %v %v", ok, err)
			}
			if err := batchWorker.HandleBatch(ctx, []amqp.Delivery{duplicate}); err != nil {
				t.Fatal(err)
			}
			var row video.Like
			var v video.Video
			if err := db.Where("account_id=? AND video_id=?", c.AccountID, c.VideoID).Take(&row).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.First(&v, c.VideoID).Error; err != nil {
				t.Fatal(err)
			}
			if row.Status != 1 || row.AppliedRevision != c.Revision || row.NextRevision != c.Revision || v.LikesCount != 1 {
				t.Fatalf("duplicate delivery changed relation or count: %+v count=%d", row, v.LikesCount)
			}
		}

		// The publisher connection remains open. Remove its partition binding so a
		// real Return + Confirm ACK must leave the new command pending.
		reset(t)
		c = insert(t)
		if err := ch.QueueUnbind(rabbitmq.LikeCommandQueue(partition), strconv.Itoa(partition), rabbitmq.LikeCommandsExchange, nil); err != nil {
			t.Fatal(err)
		}
		_, publishErr := relay.RunOnce(ctx)
		var returned *rabbitmq.PublishError
		if !errors.As(publishErr, &returned) || returned.Code != "unroutable" {
			t.Fatalf("real Return result: %v", publishErr)
		}
		assertPending(t, c.ID)
		// The failed publisher closes its connection; retry restores all topology.
		if n, err := relay.RunOnce(ctx); n != 1 || err != nil {
			t.Fatalf("reconnect: %d %v", n, err)
		}
		t.Run("real_nack", func(t *testing.T) {
			management := os.Getenv("RELAY_TEST_MANAGEMENT_URL")
			if management == "" {
				t.Skip("set RELAY_TEST_MANAGEMENT_URL for real reject-publish NACK verification")
			}
			client := &http.Client{Timeout: 2 * time.Second}
			policyURL := management + "/api/policies/" + url.PathEscape(uri.Vhost) + "/a2-reject"
			policy := `{"pattern":"^like[.]commands[.][0-3]$","apply-to":"queues","definition":{"max-length":0,"overflow":"reject-publish"},"priority":1}`
			req, err := http.NewRequest("PUT", policyURL, strings.NewReader(policy))
			if err != nil {
				t.Fatal(err)
			}
			req.SetBasicAuth(uri.Username, uri.Password)
			req.Header.Set("Content-Type", "application/json")
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode >= 300 {
				t.Fatalf("set isolated rejection policy: HTTP %d", response.StatusCode)
			}
			defer func() {
				req, _ := http.NewRequest("DELETE", policyURL, nil)
				req.SetBasicAuth(uri.Username, uri.Password)
				if response, err := client.Do(req); err == nil {
					response.Body.Close()
				}
			}()
			reset(t)
			command := insert(t)
			_, err = relay.RunOnce(ctx)
			var nack *rabbitmq.PublishError
			if !errors.As(err, &nack) || nack.Code != "broker_nack" {
				t.Fatalf("real NACK result: %v", err)
			}
			assertPending(t, command.ID)
		})
		t.Log("real routing, persistent identity, confirm, Return rejection, duplicate retry, and reconnect verified")
	})
}
