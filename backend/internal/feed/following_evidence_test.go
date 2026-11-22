package feed

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"

	"gorm.io/gorm"
)

// This opt-in assertion records the known gap until Following can establish
// that all rows ahead of its projected page boundary are indexed.
func TestFollowingFullIndexPageCoverageMySQL(t *testing.T) {
	if os.Getenv("GOFEED_CHECK_INDEX_COVERAGE") != "1" {
		t.Skip("set GOFEED_CHECK_INDEX_COVERAGE=1 to run the strict coverage assertion")
	}
	db := readTestMySQL(t)
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	cache, raw := readTestRedis(t)
	const viewer, author = 80101, 80102
	stamp := time.UnixMilli(1_794_012_300_123).UTC()
	rows := []video.Video{
		{ID: 80110, AuthorID: author, CreateTime: stamp},
		{ID: 80111, AuthorID: author, CreateTime: stamp},
		{ID: 80112, AuthorID: author, CreateTime: stamp},
		{ID: 80113, AuthorID: author, CreateTime: stamp},
	}
	if err := tx.Create(&social.Social{FollowerID: viewer, VloggerID: author}).Error; err != nil {
		t.Fatal(err)
	}
	if err := tx.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := raw.ZAdd(ctx, cache.Key("feed:inbox:%d", viewer), timelineZ(t, stamp, 80112), timelineZ(t, stamp, 80111)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := raw.ZAdd(ctx, cache.Key("feed:author_outbox:%d", author), timelineZ(t, stamp, 80112), timelineZ(t, stamp, 80110)).Err(); err != nil {
		t.Fatal(err)
	}
	service := NewFeedService(NewFeedRepository(tx), video.NewLikeRepository(tx), social.NewSocialRepository(tx), cache)
	want, err := followingEvidenceOracle(tx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	var got []uint
	var cursor *TimeCursor
	for pageNumber := 1; pageNumber <= len(rows); pageNumber++ {
		page, err := service.ListByFollowing(ctx, 2, cursor, viewer)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, feedIDs(page.VideoList)...)
		t.Logf("page %d: ids=%v has_more=%t next_id=%d", pageNumber, feedIDs(page.VideoList), page.HasMore, page.NextIDBefore)
		if !page.HasMore {
			break
		}
		cursor = &TimeCursor{Time: time.UnixMilli(page.NextTime).UTC(), ID: page.NextIDBefore}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Following cursor chain = %v, independent MySQL order = %v", got, want)
	}
}

func followingEvidenceOracle(db *gorm.DB, viewer uint) ([]uint, error) {
	var ids []uint
	err := db.Raw(`SELECT v.id FROM videos AS v
		WHERE EXISTS (
			SELECT 1 FROM socials AS s WHERE s.follower_id = ? AND s.vlogger_id = v.author_id
		) ORDER BY v.create_time DESC, v.id DESC`, viewer).Scan(&ids).Error
	if err != nil {
		return nil, fmt.Errorf("Following SQL oracle: %w", err)
	}
	return ids, nil
}
