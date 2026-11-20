package feed

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
	goredis "github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func readTestRedis(t *testing.T) (*rediscache.Client, *goredis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	raw := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = raw.Close() })
	return rediscache.NewClient(raw, "read-test:"), raw
}

func TestFollowingMemberLexPage(t *testing.T) {
	ctx := context.Background()
	cache, raw := readTestRedis(t)
	now := int64(1_794_012_300_123)
	member := func(at int64, id uint) string {
		t.Helper()
		value, err := FollowingMember(at, id)
		if err != nil {
			t.Fatal(err)
		}
		decodedAt, decodedID, err := ParseFollowingMember(value)
		if err != nil || decodedAt != at || decodedID != id {
			t.Fatalf("decode %q = (%d, %d, %v)", value, decodedAt, decodedID, err)
		}
		return value
	}
	newer, ten, nine, older := member(now+1, 1), member(now, 10), member(now, 9), member(now-1, ^uint(0))
	for _, invalid := range []string{"", strings.Replace(ten, ":", "-", 1), member(now, 1)[:14] + strings.Repeat("0", 20), ten[:14] + strings.Repeat("9", 20)} {
		if _, _, err := ParseFollowingMember(invalid); err == nil {
			t.Fatalf("accepted invalid member %q", invalid)
		}
	}
	for key, members := range map[string][]string{"inbox": {nine, newer, older, ten}, "outbox": {older, ten, newer}} {
		for _, value := range members {
			if err := raw.ZAdd(ctx, key, goredis.Z{Score: 0, Member: value}).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	keys := []string{"inbox", "missing", "outbox"}
	for _, tt := range []struct {
		before string
		want   [][]string
	}{
		{"", [][]string{{newer, ten}, {}, {newer, ten}}},
		{ten, [][]string{{nine, older}, {}, {older}}},
	} {
		got, err := cache.ZRevRangeByLexBatch(ctx, keys, tt.before, 2)
		if err != nil || !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("lex before %q: got %v, want %v, error %v", tt.before, got, tt.want, err)
		}
	}
	if err := raw.Set(ctx, "wrong-type", "value", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.ZRevRangeByLexBatch(ctx, []string{"inbox", "wrong-type"}, "", 2); err == nil {
		t.Fatal("pipeline lost WRONGTYPE error")
	}
}

func TestTimeFeedRejectsInvalidCursor(t *testing.T) {
	r := gin.New()
	handler := NewFeedHandler(nil)
	r.POST("/latest", handler.ListLatest)
	r.POST("/following", handler.ListByFollowing)
	for _, path := range []string{"/latest", "/following"} {
		for _, body := range []string{
			`{"latest_time":0}`, `{"id_before":22}`, `{"latest_time":1,"id_before":0}`,
			`{"latest_time":-1,"id_before":1}`, `{"latest_time":10000000000000,"id_before":1}`,
		} {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			record := httptest.NewRecorder()
			r.ServeHTTP(record, req)
			if record.Code != http.StatusBadRequest {
				t.Fatalf("%s with %s returned %d", path, body, record.Code)
			}
		}
	}
}

func readTestMySQL(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("FEED_READ_TEST_DSN")
	if dsn == "" {
		t.Skip("set FEED_READ_TEST_DSN to an isolated MySQL schema named gofeed_read_test")
	}
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil || cfg.DBName != "gofeed_read_test" {
		t.Fatal("FEED_READ_TEST_DSN must target gofeed_read_test")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&video.Video{}, &social.Social{}, &video.Like{}, &video.AuthorFeedState{}); err != nil {
		t.Fatal(err)
	}

	return db
}

func TestReadPaginationMySQL(t *testing.T) {
	db := readTestMySQL(t)
	ctx := context.Background()
	rollback := errors.New("rollback test fixture")
	err := db.Transaction(func(tx *gorm.DB) error {
		cache, raw := readTestRedis(t)
		service := NewFeedService(NewFeedRepository(tx), video.NewLikeRepository(tx), social.NewSocialRepository(tx), cache)
		at := time.UnixMilli(1_794_012_300_123).UTC()
		author, viewer := uint(1), uint(2)
		all := []uint{109, 99, 19, 12, 10, 9}
		for _, id := range all {
			if err := tx.Create(&video.Video{ID: id, AuthorID: author, CreateTime: at, LikesCount: 3, Popularity: 4}).Error; err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Create(&social.Social{FollowerID: viewer, VloggerID: author}).Error; err != nil {
			t.Fatal(err)
		}

		for _, name := range []string{"latest", "following"} {
			t.Run(name, func(t *testing.T) {
				var cursor *TimeCursor
				for start := 0; start < len(all); start += 2 {
					var page ListLatestResponse
					var err error
					if name == "latest" {
						page, err = service.ListLatest(ctx, 2, cursor, 0)
					} else {
						page, err = service.ListByFollowing(ctx, 2, cursor, viewer)
					}
					ids := feedIDs(page.VideoList)
					want := all[start : start+2]
					if err != nil || !reflect.DeepEqual(ids, want) || page.HasMore != (start+2 < len(all)) ||
						page.NextTime != at.UnixMilli() || page.NextIDBefore != want[1] {
						t.Fatalf("page %d: got %+v, error %v, want ids %v", start/2+1, page, err, want)
					}
					if page.VideoList[0].CreateTime != at.Unix() {
						t.Fatal("card timestamp changed units")
					}
					cursor = &TimeCursor{Time: time.UnixMilli(page.NextTime).UTC(), ID: page.NextIDBefore}
					if name == "latest" && start == 0 {
						newer := video.Video{ID: 110, AuthorID: author, CreateTime: at.Add(time.Millisecond)}
						if err := tx.Create(&newer).Error; err != nil {
							t.Fatal(err)
						}
					}
				}
				if name == "latest" {
					if err := tx.Delete(&video.Video{}, 110).Error; err != nil {
						t.Fatal(err)
					}
				}
			})
		}

		warm, err := service.ListLatest(ctx, 2, nil, 0)
		if err != nil || !warm.HasMore || !reflect.DeepEqual(feedIDs(warm.VideoList), all[:2]) {
			t.Fatalf("warm latest page = %+v, %v", warm, err)
		}
		// Force an old timeline with only one hot record. MySQL must add older
		// records without changing the page boundary.
		timeline := cache.Key("feed:global_timeline")
		newer := video.Video{ID: 110, AuthorID: author, CreateTime: at.Add(time.Millisecond)}
		if err := tx.Create(&newer).Error; err != nil {
			t.Fatal(err)
		}
		if err := raw.Del(ctx, timeline).Err(); err != nil {
			t.Fatal(err)
		}
		if err := raw.ZAdd(ctx, timeline, goredis.Z{Score: float64(newer.CreateTime.UnixMilli()), Member: "110"}).Err(); err != nil {
			t.Fatal(err)
		}
		stitched, err := service.ListLatest(ctx, 2, nil, 0)
		if err != nil || !stitched.HasMore || !reflect.DeepEqual(feedIDs(stitched.VideoList), []uint{110, 109}) {
			t.Fatalf("stitched latest page = %+v, %v", stitched, err)
		}
		if err := tx.Delete(&video.Video{}, 110).Error; err != nil {
			t.Fatal(err)
		}

		likes, err := service.ListLikesCount(ctx, 3, nil, 0)
		if err != nil || !likes.HasMore || *likes.NextLikesCountBefore != 3 || *likes.NextIDBefore != all[2] ||
			!reflect.DeepEqual(feedIDs(likes.VideoList), all[:3]) {
			t.Fatalf("first likes page = %+v, %v", likes, err)
		}
		likes, err = service.ListLikesCount(ctx, 3, &LikesCountCursor{LikesCount: *likes.NextLikesCountBefore, ID: *likes.NextIDBefore}, 0)
		if err != nil || likes.HasMore || !reflect.DeepEqual(feedIDs(likes.VideoList), all[3:]) {
			t.Fatalf("last likes page = %+v, %v", likes, err)
		}

		dbRank, err := service.ListByPopularity(ctx, 3, 0, 0, 0, 0, time.Time{}, 0)
		if err != nil || !dbRank.HasMore || dbRank.AsOf != 0 || !reflect.DeepEqual(feedIDs(dbRank.VideoList), all[:3]) {
			t.Fatalf("first DB rank page = %+v, %v", dbRank, err)
		}
		dbRank, err = service.ListByPopularity(ctx, 3, 0, 0, 0, *dbRank.NextLatestPopularity, *dbRank.NextLatestBefore, *dbRank.NextLatestIDBefore)
		if err != nil || dbRank.HasMore || !reflect.DeepEqual(feedIDs(dbRank.VideoList), all[3:]) {
			t.Fatalf("last DB rank page = %+v, %v", dbRank, err)
		}

		minute := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
		key := cache.Key("hot:video:1m:%s", minute.Format("200601021504"))
		if err := raw.ZAdd(ctx, key,
			goredis.Z{Score: 5, Member: "19"}, goredis.Z{Score: 4, Member: "12"},
			goredis.Z{Score: 3, Member: "110"}, goredis.Z{Score: 2, Member: "9"},
		).Err(); err != nil {
			t.Fatal(err)
		}
		rank, err := service.ListByPopularity(ctx, 2, minute.Unix(), 0, 0, 0, time.Time{}, 0)
		if err != nil || !rank.HasMore || rank.NextOffset != 2 || !reflect.DeepEqual(feedIDs(rank.VideoList), []uint{19, 12}) {
			t.Fatalf("first snapshot page = %+v, %v", rank, err)
		}
		rank, err = service.ListByPopularity(ctx, 2, minute.Unix(), rank.NextOffset, 0, 0, time.Time{}, 0)
		if err != nil || rank.HasMore || rank.NextOffset != 4 || !reflect.DeepEqual(feedIDs(rank.VideoList), []uint{9}) {
			t.Fatalf("last snapshot page = %+v, %v", rank, err)
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
}

func feedIDs(items []FeedVideoItem) []uint {
	ids := make([]uint, len(items))
	for i, item := range items {
		ids[i] = item.ID
	}
	return ids
}

func TestFollowingHybridMySQL(t *testing.T) {
	db := readTestMySQL(t)
	ctx := context.Background()
	rollback := errors.New("rollback following fixture")
	err := db.Transaction(func(tx *gorm.DB) error {
		cache, raw := readTestRedis(t)
		socialRepo := social.NewSocialRepository(tx)
		service := NewFeedService(NewFeedRepository(tx), video.NewLikeRepository(tx), socialRepo, cache)
		const viewer, authorPush, authorPull, other = 1, 2, 3, 4
		stamp := time.UnixMilli(1_794_012_300_123).UTC()
		push, pull := video.DistributionPush, video.DistributionPull
		states := []video.AuthorFeedState{{AuthorID: authorPush, DistributionMode: push}, {AuthorID: authorPull, DistributionMode: pull}}
		if err := tx.Create(&states).Error; err != nil {
			t.Fatal(err)
		}
		videos := []video.Video{
			{ID: 110, AuthorID: authorPush, CreateTime: stamp, DistributionMode: &push},
			{ID: 105, AuthorID: authorPull, CreateTime: stamp, DistributionMode: &pull},
			{ID: 100, AuthorID: authorPush, CreateTime: stamp, DistributionMode: &pull},
			{ID: 99, AuthorID: authorPull, CreateTime: stamp},
			{ID: 98, AuthorID: authorPull, CreateTime: stamp},
			{ID: 97, AuthorID: authorPush, CreateTime: stamp.Add(-time.Millisecond)},
			{ID: 199, AuthorID: authorPush, CreateTime: stamp.Add(time.Millisecond)},
			{ID: 200, AuthorID: other, CreateTime: stamp.Add(time.Millisecond)},
		}
		if err := tx.Create(&videos).Error; err != nil {
			t.Fatal(err)
		}
		// Keep entity reads on Redis so the existing per-ID DB goroutines do
		// not concurrently use this fixture's single transaction connection.
		for _, row := range videos {
			if row.ID == 100 {
				row.Title = "indexed entity cache"
			}
			payload, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			if err := cache.SetBytes(ctx, cache.Key("video:entity:%d", row.ID), payload, time.Minute); err != nil {
				t.Fatal(err)
			}
		}
		links := []social.Social{{FollowerID: viewer, VloggerID: authorPush}, {FollowerID: viewer, VloggerID: authorPull}, {FollowerID: viewer, VloggerID: other}}
		if err := tx.Create(&links).Error; err != nil {
			t.Fatal(err)
		}
		if err := tx.Create(&video.Like{VideoID: 105, AccountID: viewer, Status: 1}).Error; err != nil {
			t.Fatal(err)
		}
		inbox := cache.Key("feed:inbox:%d", viewer)
		pushOutbox := cache.Key("feed:author_outbox:%d", authorPush)
		pullOutbox := cache.Key("feed:author_outbox:%d", authorPull)
		add := func(key string, at time.Time, ids ...uint) {
			t.Helper()
			for _, id := range ids {
				member, err := FollowingMember(at.UnixMilli(), id)
				if err != nil {
					t.Fatal(err)
				}
				if err := raw.ZAdd(ctx, key, goredis.Z{Score: 0, Member: member}).Err(); err != nil {
					t.Fatal(err)
				}
			}
		}
		add(inbox, stamp, 110)
		add(pushOutbox, stamp, 110, 100)
		add(pullOutbox, stamp, 105, 98)
		// The current push author contributes a historical Outbox video.
		hot, err := service.ListByFollowing(ctx, 3, nil, viewer)
		if err != nil || !hot.HasMore || !reflect.DeepEqual(feedIDs(hot.VideoList), []uint{110, 105, 100}) || !hot.VideoList[1].IsLiked {
			t.Fatalf("hot page = %+v, %v", hot, err)
		}
		if _, ok := service.localcache.Get(cache.Key("video:entity:%d", 100)); !ok {
			t.Fatal("push author's historical outbox candidate was not hydrated")
		}
		if err := tx.Model(&video.Like{}).Where("account_id = ? AND video_id = ?", viewer, 105).Update("status", 0).Error; err != nil {
			t.Fatal(err)
		}

		add(inbox, stamp.Add(time.Millisecond), 199, 200)
		if _, err := service.GetVideoByIDs(ctx, []uint{199, 200}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Delete(&video.Video{}, 199).Error; err != nil {
			t.Fatal(err)
		}
		if err := socialRepo.Unfollow(ctx, &links[2]); err != nil {
			t.Fatal(err)
		}
		if err := raw.ZAdd(ctx, inbox, goredis.Z{Score: 0, Member: "malformed"}).Err(); err != nil {
			t.Fatal(err)
		}
		add(inbox, stamp, 97) // Corrupt timestamp, so this must not move ID 97 earlier.

		var cursor *TimeCursor
		for i, want := range [][]uint{{110, 105}, {100, 99}, {98, 97}} {
			page, err := service.ListByFollowing(ctx, 2, cursor, viewer)
			if err != nil || !reflect.DeepEqual(feedIDs(page.VideoList), want) || page.HasMore != (i < 2) {
				t.Fatalf("page %d = %+v, %v, want %v", i+1, page, err, want)
			}
			if i == 1 && page.VideoList[0].Title != "indexed entity cache" {
				t.Fatal("history fill replaced the indexed candidate")
			}
			if page.VideoList[1].IsLiked {
				t.Fatal("Following returned stale like state")
			}
			cursor = &TimeCursor{Time: time.UnixMilli(page.NextTime).UTC(), ID: page.NextIDBefore}
		}
		empty, err := service.ListByFollowing(ctx, 2, nil, other)
		if err != nil || empty.HasMore || len(empty.VideoList) != 0 {
			t.Fatalf("viewer without follows = %+v, %v", empty, err)
		}
		// A failed Redis command in any source switches to the complete MySQL query.
		if err := raw.Set(ctx, pushOutbox, "wrongtype", 0).Err(); err != nil {
			t.Fatal(err)
		}
		fallback, err := service.ListByFollowing(ctx, 4, nil, viewer)
		if err != nil || !fallback.HasMore || !reflect.DeepEqual(feedIDs(fallback.VideoList), []uint{110, 105, 100, 99}) {
			t.Fatalf("pipeline error fallback = %+v, %v", fallback, err)
		}
		if err := raw.Close(); err != nil {
			t.Fatal(err)
		}
		fallback, err = service.ListByFollowing(ctx, 4, nil, viewer)
		if err != nil || !fallback.HasMore || !reflect.DeepEqual(feedIDs(fallback.VideoList), []uint{110, 105, 100, 99}) {
			t.Fatalf("unavailable Redis fallback = %+v, %v", fallback, err)
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
}
