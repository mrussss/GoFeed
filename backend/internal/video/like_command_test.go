package video_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appdb "feedsystem_video_go/internal/db"
	"feedsystem_video_go/internal/video"
	drivermysql "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// LIKE_COMMAND_TEST_DSN is an administrative MySQL DSN. Every run creates and
// drops its own database; no existing application schema is migrated or cleared.
func TestLikeCommandMySQL(t *testing.T) {
	dsn := os.Getenv("LIKE_COMMAND_TEST_DSN")
	if dsn == "" {
		t.Skip("set LIKE_COMMAND_TEST_DSN to run real MySQL migration/transaction tests")
	}
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	cfg.DBName = ""
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err = admin.Ping(); err != nil {
		t.Fatal(err)
	}
	var version string
	if err = admin.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(version), "mariadb") {
		t.Fatal("this suite must run on MySQL, not a substitute")
	}
	var isolation string
	if err = admin.QueryRow("SELECT @@transaction_isolation").Scan(&isolation); err != nil {
		t.Fatal(err)
	}
	t.Log("real MySQL:", version, "isolation:", isolation)
	schema := fmt.Sprintf("gofeed_a1_test_%d", time.Now().UnixNano())
	if _, err = admin.Exec("CREATE DATABASE `" + schema + "` CHARACTER SET utf8mb4"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP DATABASE `" + schema + "`"); err != nil {
			t.Errorf("cleanup %s: %v", schema, err)
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
	sqlDB.SetMaxOpenConns(16)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db = db.WithContext(ctx)

	// Start with the actual pre-A1 shape and a real, existing primary key.
	if err = db.Exec(`CREATE TABLE likes (
 id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
 video_id BIGINT UNSIGNED NOT NULL, account_id BIGINT UNSIGNED NOT NULL,
 created_at DATETIME(3), UNIQUE KEY idx_like_video_account(video_id,account_id)
 ) ENGINE=InnoDB`).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Exec("INSERT INTO likes(id,video_id,account_id,created_at) VALUES(42,1,1,'2020-02-03 04:05:06.123')").Error; err != nil {
		t.Fatal(err)
	}
	if err = appdb.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	repo := video.NewLikeRepository(db)
	service := video.NewLikeService(repo, nil) // A1 must not need MQ or Redis.
	state := func(t *testing.T, accountID, videoID uint) video.Like {
		t.Helper()
		var row video.Like
		if err := db.Where("account_id=? AND video_id=?", accountID, videoID).Take(&row).Error; err != nil {
			t.Fatal(err)
		}
		return row
	}
	count := func(t *testing.T, model any, accountID uint) int64 {
		t.Helper()
		var n int64
		if err := db.Model(model).Where("account_id=?", accountID).Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("migration_preserves_existing_likes", func(t *testing.T) {
		old := state(t, 1, 1)
		if old.ID != 42 || old.Status != 1 || old.NextRevision != 0 || old.AppliedRevision != 0 || !old.UpdatedAt.Equal(old.CreatedAt) {
			t.Fatalf("legacy record changed: %+v", old)
		}
		// Receiving an unlike command must not apply it or physically delete an
		// active existing relation before the Batch Worker applies it.
		if _, err := service.AcceptLikeCommand(ctx, 1, video.LikeCommandRequest{RequestID: "legacy-unlike", VideoID: 1, DesiredLiked: false}); err != nil {
			t.Fatal(err)
		}
		old = state(t, 1, 1)
		if old.Status != 1 || old.NextRevision != 1 || old.AppliedRevision != 0 || !old.UpdatedAt.Equal(old.CreatedAt) {
			t.Fatalf("accept changed active relation: %+v", old)
		}
		expected := map[string][]string{
			"PRIMARY": {"id"}, "idx_like_video_account": {"video_id", "account_id"},
			"idx_command_account_request": {"account_id", "request_id"},
			"idx_command_published_at":    {"published_at"},
		}
		type indexRow struct {
			TableName  string
			IndexName  string
			ColumnName string
			NonUnique  int
		}
		var indexes []indexRow
		if err := db.Raw(`SELECT TABLE_NAME,INDEX_NAME,COLUMN_NAME,NON_UNIQUE FROM information_schema.STATISTICS
   WHERE TABLE_SCHEMA=? AND TABLE_NAME IN ('likes','command_outbox') ORDER BY TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX`, schema).Scan(&indexes).Error; err != nil {
			t.Fatal(err)
		}
		actual := map[string][]string{}
		for _, idx := range indexes {
			actual[idx.TableName+"."+idx.IndexName] = append(actual[idx.TableName+"."+idx.IndexName], idx.ColumnName)
			if strings.HasPrefix(idx.IndexName, "idx_command_account_") && idx.NonUnique != 0 {
				t.Fatalf("index is not unique: %s", idx.IndexName)
			}
		}
		indexCount := 0
		for key := range actual {
			if strings.HasPrefix(key, "command_outbox.") {
				indexCount++
			}
		}
		if indexCount != 3 {
			t.Fatalf("command_outbox has unexpected indexes: %v", actual)
		}
		for name, columns := range expected {
			table := "command_outbox"
			if name == "PRIMARY" || name == "idx_like_video_account" {
				table = "likes"
			}
			if !reflect.DeepEqual(actual[table+"."+name], columns) {
				t.Fatalf("%s: %v, want %v", name, actual[table+"."+name], columns)
			}
		}
		if db.Migrator().HasColumn(&video.CommandOutbox{}, "payload_hash") || db.Migrator().HasColumn(&video.CommandOutbox{}, "publish_status") {
			t.Fatal("redundant publication fields remain")
		}
		var unsignedColumns int64
		if err := db.Raw(`SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND
   ((TABLE_NAME='likes' AND COLUMN_NAME IN ('next_revision','applied_revision')) OR (TABLE_NAME='command_outbox' AND COLUMN_NAME='revision'))
   AND COLUMN_TYPE='bigint unsigned'`, schema).Scan(&unsignedColumns).Error; err != nil || unsignedColumns != 3 {
			t.Fatalf("unsigned revisions: %d %v", unsignedColumns, err)
		}
	})

	t.Run("first_duplicate_conflict_and_account_scope", func(t *testing.T) {
		req := video.LikeCommandRequest{RequestID: "first", VideoID: 10, DesiredLiked: true}
		first, err := service.AcceptLikeCommand(ctx, 10, req)
		if err != nil {
			t.Fatal(err)
		}
		if first.ID == 0 || first.Revision != 1 || first.PublishedAt != nil || first.VideoID != req.VideoID || first.DesiredLiked != req.DesiredLiked {
			t.Fatalf("command: %+v", first)
		}
		row := state(t, 10, 10)
		if row.Status != 0 || row.NextRevision != 1 || row.AppliedRevision != 0 {
			t.Fatalf("accept applied state: %+v", row)
		}
		same, err := service.AcceptLikeCommand(ctx, 10, req)
		if err != nil || same.ID != first.ID || same.Revision != first.Revision {
			t.Fatalf("idempotent retry: %+v %v", same, err)
		}
		for _, changed := range []video.LikeCommandRequest{
			{RequestID: "first", VideoID: 10, DesiredLiked: false}, {RequestID: "first", VideoID: 11, DesiredLiked: true},
		} {
			if _, err := service.AcceptLikeCommand(ctx, 10, changed); !errors.Is(err, video.ErrLikeCommandConflict) {
				t.Fatalf("expected conflict, got %v", err)
			}
		}
		if count(t, &video.CommandOutbox{}, 10) != 1 || state(t, 10, 10).NextRevision != 1 || count(t, &video.Like{}, 10) != 1 {
			t.Fatal("duplicate/conflict changed database state")
		}
		if other, err := service.AcceptLikeCommand(ctx, 11, req); err != nil || other.Revision != 1 {
			t.Fatalf("account-scoped ID: %+v %v", other, err)
		}
	})

	t.Run("service_existing_video", func(t *testing.T) {
		if err := db.Create(&video.Video{ID: 80, AuthorID: 80, Username: "test", Title: "A1 existing video", PlayURL: "test.mp4", CoverURL: "test.jpg"}).Error; err != nil {
			t.Fatal(err)
		}
		checkedService := video.NewLikeService(repo, video.NewVideoRepository(db))
		command, err := checkedService.AcceptLikeCommand(ctx, 80, video.LikeCommandRequest{RequestID: "video-exists", VideoID: 80, DesiredLiked: true})
		if err != nil {
			t.Fatal(err)
		}
		if command == nil || command.Revision != 1 || count(t, &video.Like{}, 80) != 1 || count(t, &video.CommandOutbox{}, 80) != 1 {
			t.Fatalf("existing video not accepted: %+v", command)
		}
	})
	t.Run("service_missing_video_no_writes", func(t *testing.T) {
		checkedService := video.NewLikeService(repo, video.NewVideoRepository(db))
		command, err := checkedService.AcceptLikeCommand(ctx, 81, video.LikeCommandRequest{RequestID: "video-missing", VideoID: 999999, DesiredLiked: true})
		if !errors.Is(err, gorm.ErrRecordNotFound) || command != nil {
			t.Fatalf("missing video accepted: command=%+v err=%v", command, err)
		}
		if count(t, &video.Like{}, 81) != 0 || count(t, &video.CommandOutbox{}, 81) != 0 {
			t.Fatal("missing video created likes or outbox records")
		}
	})

	type result struct {
		command *video.CommandOutbox
		err     error
	}
	// The query callback holds all initial misses at a barrier. This forces the
	// concurrent-idempotency test through the UNIQUE conflict/recovery path instead
	// of accidentally testing only requests that arrive after the first commit.
	concurrent := func(t *testing.T, accountID uint, requests []video.LikeCommandRequest, forceMiss bool) []result {
		t.Helper()
		ready := make(chan struct{})
		if forceMiss {
			var misses atomic.Int32
			err := db.Callback().Query().After("gorm:query").Register("a1_initial_miss_barrier", func(tx *gorm.DB) {
				if tx.Statement.Table == "command_outbox" && errors.Is(tx.Error, gorm.ErrRecordNotFound) {
					if misses.Add(1) == int32(len(requests)) {
						close(ready)
					}
					select {
					case <-ready:
					case <-ctx.Done():
					}
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Callback().Query().Remove("a1_initial_miss_barrier")
		}
		start := make(chan struct{})
		done := make(chan result, len(requests))
		for _, req := range requests {
			go func(req video.LikeCommandRequest) {
				<-start
				c, err := service.AcceptLikeCommand(ctx, accountID, req)
				done <- result{c, err}
			}(req)
		}
		close(start)
		results := make([]result, 0, len(requests))
		for range requests {
			results = append(results, <-done)
		}
		return results
	}
	t.Run("concurrent_distinct_requests", func(t *testing.T) {
		const n = 8
		requests := make([]video.LikeCommandRequest, n)
		for i := range requests {
			requests[i] = video.LikeCommandRequest{RequestID: fmt.Sprintf("parallel-%d", i), VideoID: 20, DesiredLiked: i%2 == 0}
		}
		revisions := map[uint64]bool{}
		for _, res := range concurrent(t, 20, requests, true) {
			if res.err != nil {
				t.Fatal(res.err)
			}
			if revisions[res.command.Revision] {
				t.Fatal("duplicate revision")
			}
			revisions[res.command.Revision] = true
		}
		for i := uint64(1); i <= n; i++ {
			if !revisions[i] {
				t.Fatalf("missing revision %d", i)
			}
		}
		if state(t, 20, 20).NextRevision != n || count(t, &video.CommandOutbox{}, 20) != n {
			t.Fatal("revision/outbox count mismatch")
		}
	})
	t.Run("concurrent_same_request", func(t *testing.T) {
		requests := make([]video.LikeCommandRequest, 8)
		for i := range requests {
			requests[i] = video.LikeCommandRequest{RequestID: "same", VideoID: 30, DesiredLiked: true}
		}
		var commandID uint64
		for _, res := range concurrent(t, 30, requests, true) {
			if res.err != nil {
				t.Fatal(res.err)
			}
			if commandID == 0 {
				commandID = res.command.ID
			}
			if res.command.ID != commandID || res.command.Revision != 1 {
				t.Fatalf("duplicate command: %+v", res.command)
			}
		}
		if state(t, 30, 30).NextRevision != 1 || count(t, &video.CommandOutbox{}, 30) != 1 {
			t.Fatal("losing transactions advanced state")
		}
	})
	t.Run("concurrent_same_id_conflicting_videos", func(t *testing.T) {
		requests := []video.LikeCommandRequest{{RequestID: "conflict", VideoID: 40, DesiredLiked: true}, {RequestID: "conflict", VideoID: 41, DesiredLiked: true}}
		accepted, conflicts := 0, 0
		for _, res := range concurrent(t, 40, requests, true) {
			if res.err == nil {
				accepted++
			} else if errors.Is(res.err, video.ErrLikeCommandConflict) {
				conflicts++
			} else {
				t.Fatal(res.err)
			}
		}
		if accepted != 1 || conflicts != 1 || count(t, &video.Like{}, 40) != 1 || count(t, &video.CommandOutbox{}, 40) != 1 {
			t.Fatal("conflicting loser did not roll back")
		}
	})
	t.Run("rollback_existing_and_new_state", func(t *testing.T) {
		if _, err := service.AcceptLikeCommand(ctx, 50, video.LikeCommandRequest{RequestID: "before", VideoID: 50}); err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(`CREATE TRIGGER a1_fail_outbox BEFORE INSERT ON command_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='a1 rollback injection'`).Error; err != nil {
			t.Fatal(err)
		}
		defer db.Exec("DROP TRIGGER a1_fail_outbox")
		for _, pair := range []uint{50, 51} {
			_, err := service.AcceptLikeCommand(ctx, pair, video.LikeCommandRequest{RequestID: "fail", VideoID: pair, DesiredLiked: true})
			var sqlErr *drivermysql.MySQLError
			if !errors.As(err, &sqlErr) || sqlErr.Number != 1644 {
				t.Fatalf("expected actual MySQL trigger failure, got %v", err)
			}
		}
		if state(t, 50, 50).NextRevision != 1 || count(t, &video.CommandOutbox{}, 50) != 1 || count(t, &video.Like{}, 51) != 0 || count(t, &video.CommandOutbox{}, 51) != 0 {
			t.Fatal("partial state survived rollback")
		}
	})
	t.Run("nullable_published_at_polling", func(t *testing.T) {
		first, err := service.AcceptLikeCommand(ctx, 70, video.LikeCommandRequest{RequestID: "pending-1", VideoID: 70})
		if err != nil {
			t.Fatal(err)
		}
		second, err := service.AcceptLikeCommand(ctx, 70, video.LikeCommandRequest{RequestID: "pending-2", VideoID: 70, DesiredLiked: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Model(&video.CommandOutbox{}).Where("id=?", first.ID).UpdateColumn("published_at", time.Now().UTC()).Error; err != nil {
			t.Fatal(err)
		}
		var pending []video.CommandOutbox
		if err := db.Where("published_at IS NULL").Order("id ASC").Limit(100).Find(&pending).Error; err != nil {
			t.Fatal(err)
		}
		found := false
		for i, c := range pending {
			if c.ID == first.ID || c.PublishedAt != nil {
				t.Fatal("polling included a published command")
			}
			if i > 0 && pending[i-1].ID >= c.ID {
				t.Fatal("polling order is not increasing")
			}
			if c.ID == second.ID {
				found = true
			}
		}
		if !found {
			t.Fatal("pending command missing from polling")
		}
		var plan []struct {
			Key   sql.NullString
			Extra sql.NullString
		}
		query := "EXPLAIN SELECT * FROM command_outbox WHERE published_at IS NULL ORDER BY id ASC LIMIT 100"
		if err := db.Raw(query).Scan(&plan).Error; err != nil || len(plan) != 1 {
			t.Fatalf("EXPLAIN: %v %v", plan, err)
		}
		t.Logf("Relay EXPLAIN: key=%s extra=%s", plan[0].Key.String, plan[0].Extra.String)
		if plan[0].Key.String != "idx_command_published_at" {
			// Small fixtures can make a primary-key scan cheaper. Verify that the
			// single-column secondary index itself supplies the required ordering.
			if err := db.Raw("EXPLAIN SELECT * FROM command_outbox FORCE INDEX(idx_command_published_at) WHERE published_at IS NULL ORDER BY id ASC LIMIT 100").Scan(&plan).Error; err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("published_at index EXPLAIN: key=%s extra=%s", plan[0].Key.String, plan[0].Extra.String)
		if len(plan) != 1 || plan[0].Key.String != "idx_command_published_at" || strings.Contains(strings.ToLower(plan[0].Extra.String), "filesort") {
			t.Fatalf("published_at index cannot serve ordered polling: %+v", plan)
		}
	})

	t.Run("uint64_json_case_sensitive_id_and_repeat_migration", func(t *testing.T) {
		if err := db.Create(&video.Like{AccountID: 60, VideoID: 60, NextRevision: 9007199254740992}).Error; err != nil {
			t.Fatal(err)
		}
		c, err := service.AcceptLikeCommand(ctx, 60, video.LikeCommandRequest{RequestID: "Case", VideoID: 60})
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(c)
		if err != nil || !strings.Contains(string(body), `"revision":"9007199254740993"`) {
			t.Fatalf("unsafe JSON revision: %s %v", body, err)
		}
		if c, err = service.AcceptLikeCommand(ctx, 60, video.LikeCommandRequest{RequestID: "case", VideoID: 60}); err != nil || c.Revision != 9007199254740994 {
			t.Fatalf("opaque request ID aliased: %+v %v", c, err)
		}
		preservedTime := time.Date(2025, 2, 3, 4, 5, 6, 123000000, time.UTC)
		if err := db.Model(&video.Like{}).Where("account_id=60 AND video_id=60").UpdateColumn("updated_at", preservedTime).Error; err != nil {
			t.Fatal(err)
		}
		// Also verify that an already-created draft A1 schema is simplified in place,
		// since merely deleting model fields does not make GORM drop SQL columns.
		if err := db.Exec(`ALTER TABLE command_outbox ADD COLUMN payload_hash CHAR(64) NOT NULL DEFAULT '',
   ADD COLUMN publish_status VARCHAR(16) NOT NULL DEFAULT 'pending',
   ADD INDEX idx_command_publish_id(publish_status,id),
   ADD INDEX idx_command_published_id(published_at,id),
   ADD UNIQUE INDEX idx_command_account_video_revision(account_id,video_id,revision)`).Error; err != nil {
			t.Fatal(err)
		}
		if err := appdb.AutoMigrate(db); err != nil {
			t.Fatal(err)
		}
		if db.Migrator().HasColumn(&video.CommandOutbox{}, "payload_hash") || db.Migrator().HasColumn(&video.CommandOutbox{}, "publish_status") || db.Migrator().HasIndex(&video.CommandOutbox{}, "idx_command_publish_id") {
			t.Fatal("draft schema was not simplified")
		}
		for _, index := range []string{"idx_command_published_id", "idx_command_account_video_revision"} {
			if db.Migrator().HasIndex(&video.CommandOutbox{}, index) {
				t.Fatalf("obsolete index remains: %s", index)
			}
		}
		if !state(t, 60, 60).UpdatedAt.Equal(preservedTime) {
			t.Fatal("repeat migration overwrote updated_at")
		}
		if state(t, 60, 60).NextRevision != 9007199254740994 || state(t, 10, 10).Status != 0 || count(t, &video.CommandOutbox{}, 60) != 2 {
			t.Fatal("repeat migration reset state")
		}
	})
}
