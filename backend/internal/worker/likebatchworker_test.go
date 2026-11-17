package worker_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"feedsystem_video_go/internal/config"
	appdb "feedsystem_video_go/internal/db"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	drivermysql "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// This small probe uses the library's native acknowledgement interface. The
// database is real; the probe only makes the commit-before-ACK window observable.
type batchAckProbe struct {
	calls  int
	failAt int
	check  func() error
}

func (p *batchAckProbe) Ack(_ uint64, multiple bool) error {
	p.calls++
	if multiple {
		return errors.New("unexpected multiple ACK")
	}
	if p.check != nil {
		if err := p.check(); err != nil {
			return err
		}
	}
	if p.calls == p.failAt {
		return errors.New("injected ACK disconnect")
	}
	return nil
}
func (*batchAckProbe) Nack(uint64, bool, bool) error { return errors.New("unexpected NACK") }
func (*batchAckProbe) Reject(uint64, bool) error     { return errors.New("unexpected reject") }

func TestLikeCommandBatchWorker(t *testing.T) {
	dsn := os.Getenv("LIKE_COMMAND_TEST_DSN")
	if dsn == "" {
		t.Skip("set LIKE_COMMAND_TEST_DSN for isolated real MySQL batch tests")
	}
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid MySQL test DSN")
	}
	cfg.DBName = ""
	cfg.ParseTime = true
	cfg.Timeout = 3 * time.Second
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("gofeed_b_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE `" + schema + "`"); err != nil {
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
	// Migration must preserve historical video data and counts.
	if err := db.Exec(`CREATE TABLE videos(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,author_id BIGINT UNSIGNED NOT NULL,
 username VARCHAR(255) NOT NULL,title VARCHAR(255) NOT NULL,description VARCHAR(255),play_url VARCHAR(255) NOT NULL,
 cover_url VARCHAR(255) NOT NULL,create_time DATETIME(3),likes_count BIGINT NOT NULL DEFAULT 0,popularity BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO videos(id,author_id,username,title,play_url,cover_url,likes_count,popularity) VALUES(99,1,'test','old','test','test',7,17)").Error; err != nil {
		t.Fatal(err)
	}
	if err := appdb.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	var legacy video.Video
	if err := db.First(&legacy, 99).Error; err != nil || legacy.LikesCount != 7 || legacy.Popularity != 17 {
		t.Fatalf("legacy video migration: %+v %v", legacy, err)
	}
	ctx := context.Background()
	repo := video.NewLikeRepository(db)
	createdAt := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	updatedAt := createdAt.Add(time.Hour)
	reset := func(t *testing.T) {
		t.Helper()
		for _, table := range []string{"likes", "videos"} {
			if err := db.Exec("DELETE FROM " + table).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	seedVideo := func(t *testing.T, id uint, count int64) {
		t.Helper()
		if err := db.Create(&video.Video{ID: id, AuthorID: 1, Username: "test", Title: "test", PlayURL: "test", CoverURL: "test", LikesCount: count}).Error; err != nil {
			t.Fatal(err)
		}
	}
	seedLike := func(t *testing.T, account, id uint, status uint8, next, applied uint64) {
		t.Helper()
		if err := db.Model(&video.Like{}).Create(map[string]any{"account_id": account, "video_id": id, "status": status, "next_revision": next, "applied_revision": applied, "created_at": createdAt, "updated_at": updatedAt}).Error; err != nil {
			t.Fatal(err)
		}
	}
	event := func(account, id uint, revision uint64, liked bool) rabbitmq.LikeCommandEvent {
		return rabbitmq.LikeCommandEvent{CommandID: uint64(id)*100000 + uint64(account)*100 + revision, RequestID: fmt.Sprintf("%d/%d/%d", account, id, revision), AccountID: account, VideoID: id, Revision: revision, DesiredLiked: liked}
	}
	getLike := func(t *testing.T, account, id uint) video.Like {
		t.Helper()
		var row video.Like
		if err := db.Where("account_id=? AND video_id=?", account, id).Take(&row).Error; err != nil {
			t.Fatal(err)
		}
		return row
	}
	getVideo := func(t *testing.T, id uint) video.Video {
		t.Helper()
		var row video.Video
		if err := db.First(&row, id).Error; err != nil {
			t.Fatal(err)
		}
		return row
	}
	deliveries := func(t *testing.T, events []rabbitmq.LikeCommandEvent, acks *batchAckProbe) []amqp.Delivery {
		t.Helper()
		out := make([]amqp.Delivery, len(events))
		for i, e := range events {
			body, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			out[i] = amqp.Delivery{Body: body, DeliveryTag: uint64(i + 1), Acknowledger: acks}
		}
		return out
	}

	for _, tc := range []struct {
		name     string
		initial  uint8
		applied  uint64
		steps    [][]rabbitmq.LikeCommandEvent
		status   uint8
		revision uint64
		count    int64
	}{
		{"first_like", 0, 0, [][]rabbitmq.LikeCommandEvent{{event(1, 1, 1, true)}}, 1, 1, 1},
		{"unlike_keeps_row", 1, 0, [][]rabbitmq.LikeCommandEvent{{event(1, 1, 1, false)}}, 0, 1, 0},
		{"duplicates_and_old", 1, 3, [][]rabbitmq.LikeCommandEvent{{event(1, 1, 3, true), event(1, 1, 2, false)}}, 1, 3, 1},
		{"same_target_advances_only_revision", 1, 1, [][]rabbitmq.LikeCommandEvent{{event(1, 1, 3, true)}}, 1, 3, 1},
		{"highest_revision_in_batch", 0, 0, [][]rabbitmq.LikeCommandEvent{{event(1, 1, 3, true), event(1, 1, 1, true), event(1, 1, 2, false)}}, 1, 3, 1},
		{"coalesced_no_state_change", 0, 0, [][]rabbitmq.LikeCommandEvent{{event(1, 1, 1, true), event(1, 1, 2, false)}}, 0, 2, 0},
		{"old_batch_after_new", 0, 0, [][]rabbitmq.LikeCommandEvent{{event(1, 1, 3, true)}, {event(1, 1, 1, false)}}, 1, 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset(t)
			seedVideo(t, 1, int64(tc.initial))
			seedLike(t, 1, 1, tc.initial, 10, tc.applied)
			before := getLike(t, 1, 1)
			batchWorker := worker.NewLikeCommandBatchWorker(nil, repo, rabbitmq.LikeCommandPartition(1, 1), nil)
			for _, step := range tc.steps {
				acks := &batchAckProbe{}
				if err := batchWorker.HandleBatch(ctx, deliveries(t, step, acks)); err != nil {
					t.Fatal(err)
				}
				if acks.calls != len(step) {
					t.Fatalf("ACKs=%d messages=%d", acks.calls, len(step))
				}
			}
			row, v := getLike(t, 1, 1), getVideo(t, 1)
			if row.ID != before.ID || !row.CreatedAt.Equal(createdAt) || row.NextRevision != 10 || row.AppliedRevision != tc.revision || row.Status != tc.status || v.LikesCount != tc.count {
				t.Fatalf("state=%+v video=%+v", row, v)
			}
			changed := tc.initial != tc.status
			if row.UpdatedAt.Equal(updatedAt) == changed {
				t.Fatalf("updated_at changed=%v want=%v", !row.UpdatedAt.Equal(updatedAt), changed)
			}
		})
	}
	t.Run("aggregate_by_video_and_net_zero", func(t *testing.T) {
		for _, netZero := range []bool{false, true} {
			reset(t)
			seedVideo(t, 1, 1)
			seedLike(t, 1, 1, 0, 1, 0)
			seedLike(t, 2, 1, 1, 1, 0)
			events := []rabbitmq.LikeCommandEvent{event(1, 1, 1, true), event(2, 1, 1, false)}
			if !netZero {
				seedLike(t, 3, 1, 0, 1, 0)
				events = append(events, event(3, 1, 1, true))
			}
			var updates atomic.Int32
			if err := db.Callback().Update().After("gorm:update").Register("b_count_video_updates", func(tx *gorm.DB) {
				if tx.Statement.Table == "videos" {
					updates.Add(1)
				}
			}); err != nil {
				t.Fatal(err)
			}
			_, err := repo.ApplyLikeCommandBatch(ctx, events)
			db.Callback().Update().Remove("b_count_video_updates")
			if err != nil {
				t.Fatal(err)
			}
			wantCount, wantUpdates := int64(2), int32(1)
			if netZero {
				wantCount, wantUpdates = 1, 0
			}
			v := getVideo(t, 1)
			if v.LikesCount != wantCount || updates.Load() != wantUpdates {
				t.Fatalf("aggregate: %+v updates=%d", v, updates.Load())
			}
		}
	})
	accounts := make([]uint, 4)
	for account := uint(1); ; account++ {
		p := rabbitmq.LikeCommandPartition(account, 1)
		if accounts[p] == 0 {
			accounts[p] = account
		}
		ready := true
		for _, a := range accounts {
			ready = ready && a != 0
		}
		if ready {
			break
		}
	}
	t.Run("four_partitions_concurrent_atomic_counts", func(t *testing.T) {
		reset(t)
		seedVideo(t, 1, 0)
		for _, account := range accounts {
			seedLike(t, account, 1, 0, 2, 0)
		}
		for revision := uint64(1); revision <= 2; revision++ {
			done := make(chan error, 4)
			start := make(chan struct{})
			for _, account := range accounts {
				go func(account uint) {
					<-start
					_, err := repo.ApplyLikeCommandBatch(ctx, []rabbitmq.LikeCommandEvent{event(account, 1, revision, revision == 1)})
					done <- err
				}(account)
			}
			close(start)
			for range accounts {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			wantCount := int64(4)
			if revision == 2 {
				wantCount = 0
			}
			v := getVideo(t, 1)
			if v.LikesCount != wantCount {
				t.Fatalf("concurrent: %+v", v)
			}

			var active int64
			if err := db.Model(&video.Like{}).Where("video_id=1 AND status=1").Count(&active).Error; err != nil || active != v.LikesCount {
				t.Fatalf("count differs from active relations: active=%d video=%d err=%v", active, v.LikesCount, err)
			}
			for _, account := range accounts {
				row := getLike(t, account, 1)
				if row.NextRevision != 2 || row.AppliedRevision != revision {
					t.Fatalf("watermarks: %+v", row)
				}
			}
		}
	})
	t.Run("rollback_all_tables_and_no_ack", func(t *testing.T) {
		reset(t)
		seedVideo(t, 1, 0)
		seedVideo(t, 2, 0)
		seedLike(t, 1, 1, 0, 1, 0)
		partition := rabbitmq.LikeCommandPartition(1, 1)
		account := uint(1)
		for rabbitmq.LikeCommandPartition(account, 2) != partition {
			account++
		}
		seedLike(t, account, 2, 0, 1, 0)
		if err := db.Exec(`CREATE TRIGGER b_fail_count BEFORE UPDATE ON videos FOR EACH ROW BEGIN IF NEW.id=2 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='B rollback injection'; END IF; END`).Error; err != nil {
			t.Fatal(err)
		}
		defer db.Exec("DROP TRIGGER b_fail_count")
		acks := &batchAckProbe{}
		err := worker.NewLikeCommandBatchWorker(nil, repo, partition, nil).HandleBatch(ctx, deliveries(t, []rabbitmq.LikeCommandEvent{event(1, 1, 1, true), event(account, 2, 1, true)}, acks))
		if err == nil || acks.calls != 0 {
			t.Fatalf("failed transaction ACKed: err=%v acks=%d", err, acks.calls)
		}
		for _, pair := range [][2]uint{{1, 1}, {account, 2}} {
			row := getLike(t, pair[0], pair[1])
			v := getVideo(t, pair[1])
			if row.Status != 0 || row.AppliedRevision != 0 || row.NextRevision != 1 || !row.UpdatedAt.Equal(updatedAt) || v.LikesCount != 0 {
				t.Fatalf("partial commit: %+v %+v", row, v)
			}
		}
	})
	t.Run("negative_count_is_error_not_clamp", func(t *testing.T) {
		reset(t)
		seedVideo(t, 1, 0)
		seedLike(t, 1, 1, 1, 1, 0)
		acks := &batchAckProbe{}
		err := worker.NewLikeCommandBatchWorker(nil, repo, rabbitmq.LikeCommandPartition(1, 1), nil).HandleBatch(ctx, deliveries(t, []rabbitmq.LikeCommandEvent{event(1, 1, 1, false)}, acks))
		if err == nil || acks.calls != 0 || getLike(t, 1, 1).AppliedRevision != 0 || getVideo(t, 1).LikesCount != 0 {
			t.Fatalf("inconsistent count hidden: %v", err)
		}
	})
	t.Run("commit_before_ack_and_failed_ack_replay", func(t *testing.T) {
		reset(t)
		seedVideo(t, 1, 0)
		seedLike(t, 1, 1, 0, 2, 0)
		acks := &batchAckProbe{failAt: 2, check: func() error {
			var row video.Like
			var v video.Video
			if err := db.Where("video_id=1 AND account_id=1").Take(&row).Error; err != nil {
				return err
			}
			if err := db.First(&v, 1).Error; err != nil {
				return err
			}
			if row.AppliedRevision != 2 || v.LikesCount != 1 {
				return errors.New("ACK preceded commit")
			}
			return nil
		}}
		w := worker.NewLikeCommandBatchWorker(nil, repo, rabbitmq.LikeCommandPartition(1, 1), nil)
		messages := []rabbitmq.LikeCommandEvent{event(1, 1, 1, true), event(1, 1, 2, true)}
		if err := w.HandleBatch(ctx, deliveries(t, messages, acks)); err == nil || acks.calls != 2 {
			t.Fatalf("ACK failure not surfaced: %v", err)
		}
		before := getLike(t, 1, 1)
		replay := &batchAckProbe{}
		if err := w.HandleBatch(ctx, deliveries(t, messages, replay)); err != nil {
			t.Fatal(err)
		}
		after := getLike(t, 1, 1)
		if replay.calls != 2 || getVideo(t, 1).LikesCount != 1 || !after.UpdatedAt.Equal(before.UpdatedAt) {
			t.Fatal("committed replay changed state")
		}
	})
	t.Run("malformed_target_is_not_acked", func(t *testing.T) {
		acks := &batchAckProbe{}
		err := worker.NewLikeCommandBatchWorker(nil, repo, 0, nil).HandleBatch(ctx, []amqp.Delivery{{Body: []byte(`{"command_id":"1","request_id":"bad","account_id":1,"video_id":1,"revision":"1"}`), Acknowledger: acks}})
		if err == nil || acks.calls != 0 {
			t.Fatal("missing target silently accepted")
		}
	})

	t.Run("real_rabbitmq", func(t *testing.T) {
		uriText := os.Getenv("BATCH_TEST_AMQP_URL")
		if uriText == "" {
			t.Skip("set BATCH_TEST_AMQP_URL with isolated gofeed-b-test-* vhost")
		}
		uri, err := amqp.ParseURI(uriText)
		if err != nil || !strings.HasPrefix(uri.Vhost, "gofeed-b-test-") {
			t.Fatal("isolated RabbitMQ vhost required")
		}
		conn, err := amqp.Dial(uriText)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		control, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		defer control.Close()
		if err := rabbitmq.DeclareLikeCommands(control); err != nil {
			t.Fatal(err)
		}
		publisher := rabbitmq.NewLikeCommandMQ(config.RabbitMQConfig{Host: uri.Host, Port: uri.Port, Username: uri.Username, Password: uri.Password, VHost: uri.Vhost, LikePublishTimeoutMS: 1000})
		defer publisher.Close()
		purge := func() {
			for p := 0; p < 4; p++ {
				_, _ = control.QueuePurge(rabbitmq.LikeCommandQueue(p), false)
			}
		}
		defer purge()
		t.Run("four_exclusive_consumers_and_ack", func(t *testing.T) {
			reset(t)
			purge()
			seedVideo(t, 1, 0)
			for partition, account := range accounts {
				next := uint64(1)
				if partition == 0 {
					next = 50
				}
				seedLike(t, account, 1, 0, next, 0)
				for revision := uint64(1); revision <= next; revision++ {
					if err := publisher.Publish(ctx, event(account, 1, revision, true)); err != nil {
						t.Fatal(err)
					}
				}
			}
			runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			finished := make(chan error, 1)
			go func() { finished <- worker.RunLikeCommandBatchWorkers(runCtx, conn, repo, nil) }()
			defer func() { cancel() }()
			for {
				var applied int64
				err := db.Model(&video.Like{}).Where("applied_revision=next_revision AND status=1").Count(&applied).Error
				if err != nil {
					cancel()
					<-finished
					t.Fatal(err)
				}
				if applied == 4 {
					break
				}
				select {
				case err := <-finished:
					t.Fatalf("group stopped early: %v", err)
				case <-time.After(10 * time.Millisecond):
				}
			}
			for p := 0; p < 4; p++ {
				q, err := control.QueueInspect(rabbitmq.LikeCommandQueue(p))
				if err != nil || q.Consumers != 1 {
					cancel()
					<-finished
					t.Fatalf("partition %d consumers: %+v %v", p, q, err)
				}
			}
			second, err := conn.Channel()
			if err != nil {
				cancel()
				<-finished
				t.Fatal(err)
			}
			var exclusiveErr *amqp.Error
			if err := worker.NewLikeCommandBatchWorker(second, repo, 0, nil).Run(runCtx); !errors.As(err, &exclusiveErr) || (exclusiveErr.Code != 403 && exclusiveErr.Code != 405) {
				cancel()
				<-finished
				t.Fatalf("second consumer was not rejected as exclusive: %v", err)
			}
			// Let the already committed batch finish writing its ACK frames before shutdown.
			time.Sleep(30 * time.Millisecond)
			cancel()
			<-finished
			v := getVideo(t, 1)
			if v.LikesCount != 4 {
				t.Fatalf("four queues: %+v", v)
			}

			for p := 0; p < 4; p++ {
				if _, ok, err := control.Get(rabbitmq.LikeCommandQueue(p), true); err != nil || ok {
					t.Fatalf("completed message was not ACKed: %v %v", ok, err)
				}
			}
		})
		t.Run("db_failure_requeues_and_commit_ack_failure_is_idempotent", func(t *testing.T) {
			attemptCtx, stopAttempts := context.WithTimeout(ctx, 5*time.Second)
			defer stopAttempts()
			reset(t)
			purge()
			seedVideo(t, 1, 0)
			seedLike(t, 1, 1, 0, 1, 0)
			e := event(1, 1, 1, true)
			partition := rabbitmq.LikeCommandPartition(1, 1)
			queue := rabbitmq.LikeCommandQueue(partition)
			if err := publisher.Publish(ctx, e); err != nil {
				t.Fatal(err)
			}
			if err := db.Exec(`CREATE TRIGGER b_fail_count BEFORE UPDATE ON videos FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='B rollback injection'`).Error; err != nil {
				t.Fatal(err)
			}
			ch, err := conn.Channel()
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.NewLikeCommandBatchWorker(ch, repo, partition, nil).Run(attemptCtx); err == nil {
				t.Fatal("DB failure was not returned")
			}
			if err := db.Exec("DROP TRIGGER b_fail_count").Error; err != nil {
				t.Fatal(err)
			}
			d, ok, err := control.Get(queue, false)
			if err != nil || !ok || !d.Redelivered {
				t.Fatalf("DB failure lost message: %v %v", ok, err)
			}
			if err := d.Nack(false, true); err != nil {
				t.Fatal(err)
			}
			if getLike(t, 1, 1).AppliedRevision != 0 || getVideo(t, 1).LikesCount != 0 {
				t.Fatal("DB failure partly committed")
			}

			ch, err = conn.Channel()
			if err != nil {
				t.Fatal(err)
			}
			var closed atomic.Bool
			if err := db.Callback().Update().After("gorm:update").Register("b_close_before_commit", func(tx *gorm.DB) {
				if tx.Statement.Table == "videos" && closed.CompareAndSwap(false, true) {
					_ = ch.Close()
				}
			}); err != nil {
				t.Fatal(err)
			}
			runErr := worker.NewLikeCommandBatchWorker(ch, repo, partition, nil).Run(attemptCtx)
			db.Callback().Update().Remove("b_close_before_commit")
			if runErr == nil || !strings.Contains(runErr.Error(), "ACK failed") {
				t.Fatalf("expected committed ACK failure: %v", runErr)
			}
			before := getLike(t, 1, 1)
			if before.AppliedRevision != 1 || getVideo(t, 1).LikesCount != 1 {
				t.Fatal("transaction did not commit after channel close")
			}

			d, ok, err = control.Get(queue, false)
			if err != nil || !ok || !d.Redelivered {
				t.Fatalf("ACK failure lost message: %v %v", ok, err)
			}
			if err := d.Nack(false, true); err != nil {
				t.Fatal(err)
			}
			ch, err = conn.Channel()
			if err != nil {
				t.Fatal(err)
			}
			retryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			if err := worker.NewLikeCommandBatchWorker(ch, repo, partition, nil).Run(retryCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("retry worker: %v", err)
			}
			if _, ok, err := control.Get(queue, true); err != nil || ok {
				t.Fatalf("replayed command not ACKed: %v %v", ok, err)
			}
			if getVideo(t, 1).LikesCount != 1 || !getLike(t, 1, 1).UpdatedAt.Equal(before.UpdatedAt) {
				t.Fatal("ACK failure replay changed committed state")
			}
		})
	})
}
