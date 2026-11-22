package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedsystem_video_go/internal/feedindex"
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

func timelineZ(t *testing.T, at time.Time, id uint) goredis.Z {
	t.Helper()
	member, err := FollowingMember(at.UnixMilli(), id)
	if err != nil {
		t.Fatal(err)
	}
	return goredis.Z{Score: 0, Member: member}
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

		legacyTimeline := cache.Key("feed:global_timeline")
		if err := raw.ZAdd(ctx, legacyTimeline, goredis.Z{Score: float64(at.UnixMilli()), Member: "9999"}).Err(); err != nil {
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

		timeline := cache.Key(feedindex.GlobalTimelineKey)
		var wantMembers []string
		for _, id := range all {
			wantMembers = append(wantMembers, timelineZ(t, at, id).Member.(string))
		}
		if got := raw.ZRevRange(ctx, timeline, 0, -1).Val(); !reflect.DeepEqual(got, wantMembers) {
			t.Fatalf("rebuilt latest members = %v, want %v", got, wantMembers)
		}
		if got := raw.ZRevRange(ctx, legacyTimeline, 0, -1).Val(); !reflect.DeepEqual(got, []string{"9999"}) {
			t.Fatalf("the old timeline changed: %v", got)
		}

		if _, err := service.GetVideoByIDs(ctx, all); err != nil {
			t.Fatal(err)
		}
		warmOnly := NewFeedService(nil, nil, nil, cache)
		warmOnly.localcache = service.localcache
		warm, err := warmOnly.ListLatest(ctx, 2, &TimeCursor{Time: at, ID: all[1]}, 0)
		if err != nil || !warm.HasMore || !reflect.DeepEqual(feedIDs(warm.VideoList), all[2:4]) {
			t.Fatalf("warm latest page = %+v, %v", warm, err)
		}
		// A delayed event may be the only indexed row after rollout. Cold
		// stitching must insert missing newer rows above it in the page.
		newer := video.Video{ID: 110, AuthorID: author, CreateTime: at.Add(time.Millisecond)}
		if err := tx.Create(&newer).Error; err != nil {
			t.Fatal(err)
		}
		if err := raw.Del(ctx, timeline).Err(); err != nil {
			t.Fatal(err)
		}
		if err := raw.ZAdd(ctx, timeline, timelineZ(t, at, 109)).Err(); err != nil {
			t.Fatal(err)
		}
		stitched, err := service.ListLatest(ctx, 2, nil, 0)
		if err != nil || !stitched.HasMore || !reflect.DeepEqual(feedIDs(stitched.VideoList), []uint{110, 109}) {
			t.Fatalf("stitched latest page = %+v, %v", stitched, err)
		}
		if _, err := service.GetVideoByIDs(ctx, []uint{110}); err != nil {
			t.Fatal(err)
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
		if err != nil || likes.HasMore || !reflect.DeepEqual(feedIDs(likes.VideoList), all[3:]) ||
			*likes.NextLikesCountBefore != 3 || *likes.NextIDBefore != all[5] {
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
		key := cache.Key(feedindex.PopularityBucketKey, minute.Format(feedindex.PopularityMinuteFormat))
		if err := raw.ZAdd(ctx, key,
			goredis.Z{Score: 6, Member: "19"}, // An invalid old-format member.
			goredis.Z{Score: 5, Member: feedindex.PopularityMember(19)},
			goredis.Z{Score: 4, Member: feedindex.PopularityMember(12)},
			goredis.Z{Score: 3, Member: feedindex.PopularityMember(110)},
			goredis.Z{Score: 2, Member: feedindex.PopularityMember(9)},
			goredis.Z{Score: 2, Member: feedindex.PopularityMember(10)},
		).Err(); err != nil {
			t.Fatal(err)
		}
		cachedRank := video.Video{ID: 19, AuthorID: author, CreateTime: at, Title: "cached ranking entity"}
		service.EvictVideo(cachedRank.ID)
		payload, err := json.Marshal(cachedRank)
		if err != nil {
			t.Fatal(err)
		}
		if err := cache.SetBytes(ctx, service.videoEntityKey(cachedRank.ID), payload, videoRedisTTL); err != nil {
			t.Fatal(err)
		}
		rank, err := service.ListByPopularity(ctx, 2, minute.Unix(), 0, 0, 0, time.Time{}, 0)
		if err != nil || !rank.HasMore || rank.NextOffset != 3 || !reflect.DeepEqual(feedIDs(rank.VideoList), []uint{19, 12}) || rank.VideoList[0].Title != cachedRank.Title {
			t.Fatalf("first snapshot page = %+v, %v", rank, err)
		}
		rank, err = service.ListByPopularity(ctx, 2, minute.Unix(), rank.NextOffset, 0, 0, time.Time{}, 0)
		if err != nil || rank.HasMore || rank.NextOffset != 6 || !reflect.DeepEqual(feedIDs(rank.VideoList), []uint{10, 9}) {
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
		// A cached title lets the test distinguish the indexed candidate from
		// the same video returned by the history query during a merge.
		indexed := videos[2]
		indexed.Title = "indexed entity cache"
		payload, err := json.Marshal(indexed)
		if err != nil {
			t.Fatal(err)
		}
		if err := cache.SetBytes(ctx, cache.Key("video:entity:%d", indexed.ID), payload, time.Minute); err != nil {
			t.Fatal(err)
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

func TestVideoLookupBatchMySQL(t *testing.T) {
	ctx := context.Background()
	db := readTestMySQL(t)
	rollback := errors.New("rollback entity fixture")
	err := db.Transaction(func(tx *gorm.DB) error {
		cache, raw := readTestRedis(t)
		service := NewFeedService(NewFeedRepository(tx), nil, nil, cache)
		var fixture []video.Video
		for _, id := range []uint{2, 4, 6, 8, 9, 10} {
			fixture = append(fixture, video.Video{ID: id, AuthorID: 1})
		}
		if err := tx.Create(&fixture).Error; err != nil {
			t.Fatal(err)
		}
		service.localcache.Set(service.videoEntityKey(2), fixture[0], videoLocalTTL)
		for id, value := range map[uint]string{4: `{"id":4}`, 6: `{"id":404}`} {
			if err := cache.SetBytes(ctx, service.videoEntityKey(id), []byte(value), time.Minute); err != nil {
				t.Fatal(err)
			}
		}

		var redisCalls videoRedisCalls
		raw.AddHook(&redisCalls)
		var queryArgs [][]interface{}
		var failQuery atomic.Bool
		dbError := errors.New("entity query failed")
		queries := tx.Callback().Query()
		if err := queries.Before("gorm:query").Register("test:fail_video_read", func(db *gorm.DB) {
			if db.Statement.Table == "videos" && failQuery.Load() {
				_ = db.AddError(dbError)
			}
		}); err != nil {
			t.Fatal(err)
		}
		if err := queries.After("gorm:query").Register("test:record_video_read", func(db *gorm.DB) {
			if db.Statement.Table == "videos" && db.Error == nil {
				queryArgs = append(queryArgs, append([]interface{}(nil), db.Statement.Vars...))
			}
		}); err != nil {
			t.Fatal(err)
		}

		check := func(s *FeedService, input, expected []uint) {
			t.Helper()
			rows, err := s.GetVideoByIDs(ctx, input)
			var ids []uint
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			if err != nil || !reflect.DeepEqual(ids, expected) {
				t.Fatalf("lookup %v = %v, %v; want %v", input, ids, err, expected)
			}
		}

		check(service, []uint{8, 4, 2, 6, 8, 99}, []uint{8, 4, 2, 6, 8})
		wantSQL := [][]interface{}{{uint(6), uint(8), uint(99)}}
		if !reflect.DeepEqual(queryArgs, wantSQL) {
			t.Fatalf("SQL args = %v, want one batch %v", queryArgs, wantSQL)
		}
		if redisCalls.mgets.Load() != 1 || redisCalls.pipelines.Load() != 1 || redisCalls.sets.Load() != 2 {
			t.Fatalf("Redis counts MGET=%d pipelines=%d SET=%d", redisCalls.mgets.Load(), redisCalls.pipelines.Load(), redisCalls.sets.Load())
		}
		for _, id := range []uint{6, 8} {
			if ttl := raw.TTL(ctx, service.videoEntityKey(id)).Val(); ttl != videoRedisTTL {
				t.Fatalf("Redis TTL for %d = %s", id, ttl)
			}
		}
		if raw.Exists(ctx, service.videoEntityKey(99)).Val() != 0 {
			t.Fatal("missing video was cached")
		}

		check(service, []uint{6, 4, 2, 8}, []uint{6, 4, 2, 8})
		if len(queryArgs) != 1 || redisCalls.mgets.Load() != 1 {
			t.Fatal("a complete L1 hit used a remote lookup")
		}
		fresh := NewFeedService(service.repo, nil, nil, cache)
		check(fresh, []uint{8, 4, 6}, []uint{8, 4, 6})
		if len(queryArgs) != 1 || redisCalls.mgets.Load() != 2 {
			t.Fatal("a complete Redis hit queried MySQL or used multiple MGETs")
		}

		if err := raw.Close(); err != nil {
			t.Fatal(err)
		}
		check(fresh, []uint{10, 9}, []uint{10, 9})
		if len(queryArgs) != 2 || !reflect.DeepEqual(queryArgs[1], []interface{}{uint(9), uint(10)}) {
			t.Fatalf("Redis failure did not use one SQL batch: %v", queryArgs)
		}

		failQuery.Store(true)
		rows, err := fresh.GetVideoByIDs(ctx, []uint{10, 2})
		if rows != nil || !errors.Is(err, dbError) {
			t.Fatalf("failed SQL lookup returned %v, %v", rows, err)
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
}

type videoRedisCalls struct {
	mgets, pipelines, sets atomic.Int32
}

func (*videoRedisCalls) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (c *videoRedisCalls) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == "mget" {
			c.mgets.Add(1)
		}
		return next(ctx, cmd)
	}
}

func (c *videoRedisCalls) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() == "set" {
				c.sets.Add(1)
			}
		}
		if len(cmds) > 0 && cmds[0].Name() == "set" {
			c.pipelines.Add(1)
		}
		return next(ctx, cmds)
	}
}

func TestVideoLookupSingleflightMySQL(t *testing.T) {
	db := readTestMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rollback := errors.New("rollback flight fixture")
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&[]video.Video{{ID: 7, AuthorID: 1}, {ID: 9, AuthorID: 1}}).Error; err != nil {
			t.Fatal(err)
		}
		service := NewFeedService(NewFeedRepository(tx), nil, nil, nil)
		queryStarted, release := make(chan struct{}), make(chan struct{})
		var queries atomic.Int32
		if err := tx.Callback().Query().Before("gorm:query").Register("test:hold_entity_query", func(db *gorm.DB) {
			if db.Statement.Table == "videos" {
				if queries.Add(1) == 1 {
					close(queryStarted)
				}
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
		}); err != nil {
			t.Fatal(err)
		}

		type lookupResult struct {
			ids []uint
			err error
		}
		lookup := func(requestCtx context.Context, input []uint, result chan<- lookupResult) {
			rows, err := service.GetVideoByIDs(requestCtx, input)
			var ids []uint
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			result <- lookupResult{ids, err}
		}
		wait := func(ready <-chan struct{}) {
			t.Helper()
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal("lookup did not reach the expected phase")
			}
		}
		first, second, canceled := make(chan lookupResult, 1), make(chan lookupResult, 1), make(chan lookupResult, 1)
		go lookup(ctx, []uint{9, 7, 99}, first)
		wait(queryStarted)
		waiter := &flightWaitContext{Context: ctx, entered: make(chan struct{})}
		go lookup(waiter, []uint{99, 7, 9, 7}, second)
		wait(waiter.entered)

		cancelCtx, cancelWaiter := context.WithCancel(ctx)
		defer cancelWaiter()
		leaving := &flightWaitContext{Context: cancelCtx, entered: make(chan struct{})}
		go lookup(leaving, []uint{7, 99, 9}, canceled)
		wait(leaving.entered)
		cancelWaiter()
		if got := <-canceled; !errors.Is(got.err, context.Canceled) {
			t.Fatalf("canceled waiter returned %+v", got)
		}
		close(release)
		gotFirst, gotSecond := <-first, <-second
		if gotFirst.err != nil || gotSecond.err != nil || !reflect.DeepEqual(gotFirst.ids, []uint{9, 7}) ||
			!reflect.DeepEqual(gotSecond.ids, []uint{7, 9, 7}) || queries.Load() != 1 {
			t.Fatalf("singleflight first=%+v second=%+v queries=%d", gotFirst, gotSecond, queries.Load())
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
}

type flightWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *flightWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

func TestVideoLookupLeaderCancellationMySQL(t *testing.T) {
	db := readTestMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	leaderCtx, cancelLeader := context.WithCancel(ctx)
	defer cancelLeader()
	service := NewFeedService(NewFeedRepository(db), nil, nil, nil)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	held, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	lockName := fmt.Sprintf("gofeed_read_leader_%d", time.Now().UnixNano())
	var acquired int
	if err := held.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", lockName).Scan(&acquired); err != nil || acquired != 1 {
		t.Fatalf("acquire test lock: %d, %v", acquired, err)
	}
	defer func() {
		releaseCtx, release := context.WithTimeout(context.Background(), time.Second)
		defer release()
		_, _ = held.ExecContext(releaseCtx, "DO RELEASE_LOCK(?)", lockName)
	}()

	queryEntered, queryResult := make(chan struct{}), make(chan error, 1)
	var calls atomic.Int32
	statement := fmt.Sprintf("DO GET_LOCK('%s', 30)", lockName)
	if err := db.Callback().Query().Before("gorm:query").Register("test:cancel_leader_sql", func(tx *gorm.DB) {
		if tx.Statement.Table != "videos" {
			return
		}
		if calls.Add(1) == 1 {
			close(queryEntered)
		}
		_, err := tx.Statement.ConnPool.ExecContext(tx.Statement.Context, statement)
		if err == nil {
			err = errors.New("blocking SQL unexpectedly finished")
		}
		_ = tx.AddError(err)
		queryResult <- err
	}); err != nil {
		t.Fatal(err)
	}

	start := func(requestCtx context.Context) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := service.GetVideoByIDs(requestCtx, []uint{1})
			done <- err
		}()
		return done
	}
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal("lookup did not reach the expected phase")
		}
	}
	first := start(leaderCtx)
	wait(queryEntered)
	waiter := &flightWaitContext{Context: ctx, entered: make(chan struct{})}
	second := start(waiter)
	wait(waiter.entered)

	// Confirm the driver has sent the blocking SQL before canceling its owner.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var active int64
		if err := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO = ?", statement).Scan(&active).Error; err != nil {
			t.Fatal(err)
		}
		if active == 1 {
			break
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("the shared SQL never became active")
		}
	}
	cancelLeader()
	for name, ch := range map[string]<-chan error{"SQL": queryResult, "leader": first, "waiter": second} {
		select {
		case err := <-ch:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s returned %v", name, err)
			}
		case <-ctx.Done():
			t.Fatalf("%s did not exit", name)
		}
	}
	if ctx.Err() != nil || calls.Load() != 1 {
		t.Fatalf("waiter context=%v SQL calls=%d", ctx.Err(), calls.Load())
	}
}

func TestVideoDeleteFeedCachesMySQL(t *testing.T) {
	db := readTestMySQL(t)
	ctx := context.Background()
	rollback := errors.New("rollback video deletion fixture")
	for _, fail := range []bool{false, true} {
		name := "eviction"
		if fail {
			name = "redis_write_failure"
		}
		t.Run(name, func(t *testing.T) {
			err := db.Transaction(func(tx *gorm.DB) error {
				server := miniredis.RunT(t)
				raw := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
				defer raw.Close()
				cache := rediscache.NewClient(raw, "read-delete-test:")
				repo := NewFeedRepository(tx)
				feeds := NewFeedService(repo, video.NewLikeRepository(tx), nil, cache)
				other := NewFeedService(repo, video.NewLikeRepository(tx), nil, cache)
				videos := video.NewVideoService(video.NewVideoRepository(tx), cache)
				videos.SetFeedVideoEviction(feeds.EvictVideo)
				now := time.Now().UTC().Truncate(time.Millisecond)
				fixture := []video.Video{
					{ID: 11, AuthorID: 1, CreateTime: now},
					{ID: 12, AuthorID: 1, CreateTime: now.Add(-time.Second)},
				}
				if err := tx.Create(&fixture).Error; err != nil {
					t.Fatal(err)
				}
				timeline := cache.Key(feedindex.GlobalTimelineKey)
				deleteEntry, keepEntry := timelineZ(t, now, 11), timelineZ(t, now.Add(-time.Second), 12)
				if err := raw.ZAdd(ctx, timeline, deleteEntry, keepEntry).Err(); err != nil {
					t.Fatal(err)
				}
				asOf := now.Truncate(time.Minute)
				snapshot := cache.Key(feedindex.PopularitySnapshotKey, asOf.Format(feedindex.PopularityMinuteFormat))
				deletedRank, keptRank := feedindex.PopularityMember(11), feedindex.PopularityMember(12)
				if err := raw.ZAdd(ctx, snapshot, goredis.Z{Score: 2, Member: deletedRank}, goredis.Z{Score: 1, Member: keptRank}).Err(); err != nil {
					t.Fatal(err)
				}
				if err := raw.Expire(ctx, snapshot, 2*time.Minute).Err(); err != nil {
					t.Fatal(err)
				}

				entity, detail := feeds.videoEntityKey(11), cache.Key("video:detail:id=%d", 11)
				if _, err := videos.GetDetail(ctx, 11); err != nil {
					t.Fatal(err)
				}
				checkLatest := func(s *FeedService, want ...uint) {
					t.Helper()
					page, err := s.ListLatest(ctx, 10, nil, 0)
					if err != nil || !reflect.DeepEqual(feedIDs(page.VideoList), want) {
						t.Fatalf("latest = %v, error %v, want %v", feedIDs(page.VideoList), err, want)
					}
				}
				checkLatest(feeds, 11, 12)
				checkLatest(other, 11, 12)
				if got := raw.Exists(ctx, entity, detail).Val(); got != 2 {
					t.Fatalf("warm cache has %d of the two Redis keys", got)
				}
				if fail {
					raw.AddHook(&deleteFailureHook{})
				}
				if err := videos.Delete(ctx, 11, 1); err != nil {
					t.Fatal(err)
				}
				if feeds.localVideo(11) != nil || other.localVideo(11) == nil {
					t.Fatal("cache eviction did not have process-local scope")
				}

				gotKeys := raw.Exists(ctx, entity, detail).Val()
				gotTimeline := raw.ZRevRange(ctx, timeline, 0, -1).Val()
				if fail {
					if gotKeys != 2 || !reflect.DeepEqual(gotTimeline, []string{deleteEntry.Member.(string), keepEntry.Member.(string)}) {
						t.Fatalf("failed eviction keys=%d; timeline=%v", gotKeys, gotTimeline)
					}
					checkLatest(feeds, 11, 12)
					// Advance only the test Redis clock. L1 retains its real five-second TTL.
					server.FastForward(time.Minute)
					if raw.Exists(ctx, entity, detail).Val() != 0 {
						t.Fatal("stale Redis keys exceeded their TTL")
					}
				} else {
					if gotKeys != 0 || !reflect.DeepEqual(gotTimeline, []string{keepEntry.Member.(string)}) {
						t.Fatalf("successful eviction keys=%d; timeline=%v", gotKeys, gotTimeline)
					}
					checkLatest(feeds, 12)
					checkLatest(other, 12)
				}

				rank, err := other.ListByPopularity(ctx, 10, asOf.Unix(), 0, 0, 0, time.Time{}, 0)
				if err != nil || rank.AsOf != asOf.Unix() || !reflect.DeepEqual(feedIDs(rank.VideoList), []uint{12}) ||
					!reflect.DeepEqual(raw.ZRevRange(ctx, snapshot, 0, -1).Val(), []string{deletedRank, keptRank}) {
					t.Fatalf("popularity = %+v, error %v", rank, err)
				}
				if fail {
					deadline := time.Now().Add(videoLocalTTL + time.Second)
					for feeds.localVideo(11) != nil {
						if time.Now().After(deadline) {
							t.Fatal("stale L1 entry exceeded its TTL")
						}
						time.Sleep(10 * time.Millisecond)
					}
					checkLatest(feeds, 12)
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
		})
	}
}

type deleteFailureHook struct {
	videoRedisCalls
}

func (*deleteFailureHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == "del" || cmd.Name() == "zrem" {
			return errors.New("injected Redis eviction failure")
		}
		return next(ctx, cmd)
	}
}

func TestFeedBackpressureMySQL(t *testing.T) {
	db := readTestMySQL(t)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()

	now := time.Now().UTC().Truncate(time.Millisecond)
	fixture := []video.Video{{AuthorID: 1, CreateTime: now}, {AuthorID: 1, CreateTime: now.Add(-time.Millisecond)}}
	if err := db.Create(&fixture).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Delete(&fixture).Error; err != nil {
			t.Error(err)
		}
	})
	const viewer = 2
	link := social.Social{FollowerID: viewer, VloggerID: fixture[0].AuthorID}
	like := video.Like{VideoID: fixture[0].ID, AccountID: viewer, Status: 1}
	if err := db.Create(&link).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Where("follower_id = ? AND vlogger_id = ?", link.FollowerID, link.VloggerID).Delete(&link).Error; err != nil {
			t.Error(err)
		}
	})
	if err := db.Create(&like).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Delete(&like).Error; err != nil {
			t.Error(err)
		}
	})

	server := miniredis.RunT(t)
	raw := goredis.NewClient(&goredis.Options{Addr: server.Addr(), MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = raw.Close() })
	cache := rediscache.NewClient(raw, "test-read-budget:")
	service := NewFeedService(NewFeedRepository(db), video.NewLikeRepository(db), social.NewSocialRepository(db), cache)
	timeline := cache.Key(feedindex.GlobalTimelineKey)
	for _, row := range fixture {
		for _, key := range []string{timeline, cache.Key("feed:author_outbox:%d", row.AuthorID)} {
			if err := raw.ZAdd(ctx, key, timelineZ(t, row.CreateTime, row.ID)).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}

	router := gin.New()
	h := NewFeedHandler(service)
	router.POST("/latest", h.ListLatest)
	router.POST("/following", func(c *gin.Context) { c.Set("accountID", uint(viewer)) }, h.ListByFollowing)
	router.POST("/popular", h.ListByPopularity)
	request := func(requestCtx context.Context, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(requestCtx, http.MethodPost, path, strings.NewReader(`{"limit":1}`))
		req.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		return res
	}
	checkLatest := func(res *httptest.ResponseRecorder) {
		t.Helper()
		var page ListLatestResponse
		if res.Code != http.StatusOK || json.Unmarshal(res.Body.Bytes(), &page) != nil ||
			len(page.VideoList) != 1 || page.VideoList[0].ID != fixture[0].ID {
			t.Fatalf("latest got status %d, body %s", res.Code, res.Body)
		}
	}
	checkLatest(request(ctx, "/latest"))
	t.Run("warm_following_with_full_budget", func(t *testing.T) {
		for range feedReadConcurrency {
			service.reads.active <- struct{}{}
		}
		defer func() {
			for range feedReadConcurrency {
				<-service.reads.active
			}
		}()
		responses := make(chan *httptest.ResponseRecorder, 32)
		for range cap(responses) {
			go func() { responses <- request(ctx, "/following") }()
		}
		for range cap(responses) {
			res := <-responses
			var page ListLatestResponse
			if res.Code != http.StatusOK || json.Unmarshal(res.Body.Bytes(), &page) != nil || !page.HasMore ||
				len(page.VideoList) != 1 || page.VideoList[0].ID != fixture[0].ID || !page.VideoList[0].IsLiked {
				t.Errorf("warm Following got %d %s", res.Code, res.Body)
			}
		}
	})
	server.Close()
	checkLatest(request(ctx, "/latest"))
	if res := request(ctx, "/following"); res.Code != http.StatusOK {
		t.Fatalf("Following Redis outage fallback got %d %s", res.Code, res.Body)
	}

	entered := make(chan struct{}, 64)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var sqlEntries atomic.Int32
	if err := db.Callback().Query().Before("gorm:query").Register("test:hold_feed_reads", func(tx *gorm.DB) {
		if tx.Statement.Table != "videos" {
			return // The Following relation query can still run during a Redis outage.
		}
		sqlEntries.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-tx.Statement.Context.Done():
			_ = tx.AddError(tx.Statement.Context.Err())
		}
	}); err != nil {
		t.Fatal(err)
	}

	start := func(requestCtx context.Context, path string) <-chan *httptest.ResponseRecorder {
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() { done <- request(requestCtx, path) }()
		return done
	}
	waitForQuery := func() {
		t.Helper()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("request did not enter its query")
		}
	}
	heldCtx, cancelHeld := context.WithCancel(ctx)
	defer cancelHeld()
	held := make([]<-chan *httptest.ResponseRecorder, feedReadConcurrency)
	held[0] = start(heldCtx, "/latest")
	for i := 1; i < len(held); i++ {
		held[i] = start(ctx, "/latest")
	}
	for range held {
		waitForQuery()
	}

	// A valid L1 hit does not require a SQL slot, even during a Redis outage.
	rows, err := service.GetVideoByIDs(ctx, []uint{fixture[0].ID})
	if err != nil || len(rows) != 1 {
		t.Fatalf("L1 hit under pressure: %v, %v", rows, err)
	}

	var overflow []<-chan *httptest.ResponseRecorder
	for _, path := range []string{"/latest", "/following", "/popular"} {
		for range 5 {
			overflow = append(overflow, start(ctx, path))
		}
	}
	service.EvictVideo(fixture[1].ID)
	if _, err := service.GetVideoByIDs(ctx, []uint{fixture[0].ID, fixture[1].ID}); !errors.Is(err, ErrFeedReadOverloaded) {
		t.Fatalf("cold entity batch during Redis outage got %v", err)
	}
	for _, done := range overflow {
		select {
		case res := <-done:
			if res.Code != http.StatusServiceUnavailable || res.Body.String() != `{"error":"feed read overloaded"}` {
				t.Fatalf("overload got %d %s", res.Code, res.Body)
			}
		case <-ctx.Done():
			t.Fatal("overloaded request failed to exit")
		}
	}
	if got := sqlEntries.Load(); got != feedReadConcurrency {
		t.Fatalf("overload started %d SQL operations, want %d", got, feedReadConcurrency)
	}

	cancelHeld()
	select {
	case res := <-held[0]:
		if res.Code != http.StatusInternalServerError || !strings.Contains(res.Body.String(), "context canceled") {
			t.Fatalf("canceled query: %d %s", res.Code, res.Body)
		}
	case <-ctx.Done():
		t.Fatal("the canceled query did not finish")
	}
	replacement := start(ctx, "/latest")
	waitForQuery()
	if got := sqlEntries.Load(); got != feedReadConcurrency+1 {
		t.Fatalf("SQL operations after cancellation = %d", got)
	}
	close(release)
	for _, done := range append(held[1:], replacement) {
		select {
		case res := <-done:
			checkLatest(res)
		case <-ctx.Done():
			t.Fatal("held query did not complete")
		}
	}

	if err := server.Restart(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	before := sqlEntries.Load()
	checkLatest(request(ctx, "/latest"))
	if sqlEntries.Load() != before {
		t.Fatal("warm Latest did not return to the Redis path")
	}
}

func TestFeedSQLDeadlineReleasesSlot(t *testing.T) {
	budget := newFeedReadBudget()
	started := time.Now()
	_, err := readFeedFallbackMySQL(context.Background(), budget, func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) || len(budget.active) != 0 {
		t.Fatalf("deadline got %v with %d occupied slots", err, len(budget.active))
	}
	if elapsed := time.Since(started); elapsed < feedQueryTimeout || elapsed > time.Second {
		t.Fatalf("observed SQL timeout %v", elapsed)
	}
	queryErr := errors.New("injected SQL error")
	_, err = readFeedFallbackMySQL(context.Background(), budget, func(context.Context) (int, error) {
		return 0, queryErr
	})
	if !errors.Is(err, queryErr) || len(budget.active) != 0 {
		t.Fatalf("query error got %v with %d occupied slots", err, len(budget.active))
	}
	_, err = readFeedMySQL(context.Background(), func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("normal SQL deadline got %v", err)
	}
}

func TestPopularityCacheWriter(t *testing.T) {
	ctx := context.Background()
	cache, raw := readTestRedis(t)
	if err := video.UpdatePopularityCache(ctx, cache, 10, 2); err != nil {
		t.Fatal(err)
	}
	keys, err := raw.Keys(ctx, cache.Key("hot:video:1m:v2:*")).Result()
	if err != nil || len(keys) != 1 {
		t.Fatalf("minute buckets = %v, %v", keys, err)
	}
	rank, err := raw.ZRevRangeWithScores(ctx, keys[0], 0, -1).Result()
	if err != nil || !reflect.DeepEqual(rank, []goredis.Z{{Score: 2, Member: "00000000000000000010"}}) {
		t.Fatalf("rank member = %v, %v", rank, err)
	}
	if ttl := raw.TTL(ctx, keys[0]).Val(); ttl != 2*time.Hour {
		t.Fatalf("minute bucket TTL = %v", ttl)
	}
}
