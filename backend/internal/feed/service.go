package feed

import (
	"context"
	"encoding/json"
	"errors"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"fmt"
	"log"
	"slices"
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
	redis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type FeedService struct {
	repo         *FeedRepository
	likeRepo     *video.LikeRepository
	socialRepo   *social.SocialRepository
	rediscache   *rediscache.Client
	localcache   *cache.Cache
	requestGroup singleflight.Group
}

type CachedFeedData struct {
	PublicVideos []video.Video `json:"public_videos"`
}

func NewFeedService(repo *FeedRepository, likeRepo *video.LikeRepository, socialRepo *social.SocialRepository, rediscache *rediscache.Client) *FeedService {
	return &FeedService{repo: repo, likeRepo: likeRepo, socialRepo: socialRepo, rediscache: rediscache, localcache: cache.New(3*time.Second, 5*time.Second)}
}

const (
	videoLocalTTL = 5 * time.Second
	videoRedisTTL = 60 * time.Second
)

func (f *FeedService) GetVideoByIDs(ctx context.Context, videoIDs []uint) ([]*video.Video, error) {
	videos := make(map[uint]*video.Video, len(videoIDs))
	seen := make(map[uint]struct{}, len(videoIDs))
	var missing []uint
	for _, id := range videoIDs {
		if _, ok := seen[id]; id == 0 || ok {
			continue
		}
		seen[id] = struct{}{}
		if v := f.localVideo(id); v != nil {
			videos[id] = v
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return buildOrderedResult(videoIDs, videos), nil
	}

	if f.rediscache != nil {
		missing = f.readRedisVideos(ctx, missing, videos)
	}
	if len(missing) > 0 {
		// A set of IDs identifies the flight independently of request order.
		slices.Sort(missing)
		key := fmt.Sprint("sf:entities:", missing)
		flight := f.requestGroup.DoChan(key, func() (interface{}, error) {
			return f.loadVideoBatch(ctx, missing)
		})
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-flight:
			if result.Err != nil {
				return nil, result.Err
			}
			for _, v := range result.Val.([]*video.Video) {
				copied := *v
				videos[v.ID] = &copied
			}
		}
	}
	return buildOrderedResult(videoIDs, videos), nil
}

func (f *FeedService) localVideo(id uint) *video.Video {
	if entry, ok := f.localcache.Get(f.videoEntityKey(id)); ok {
		if v, ok := entry.(video.Video); ok {
			return &v
		}
	}
	return nil
}

func (f *FeedService) videoEntityKey(id uint) string {
	return f.rediscache.Key("video:entity:%d", id)
}

func (f *FeedService) readRedisVideos(ctx context.Context, ids []uint, videos map[uint]*video.Video) []uint {
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = f.videoEntityKey(id)
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	results, err := f.rediscache.MGet(cacheCtx, keys...)
	cancel()
	if err != nil {
		log.Printf("feed video MGET failed: %v", err)
		return ids
	}

	missing := ids[:0]
	for i, entry := range results {
		id := ids[i]
		var v video.Video
		if payload, ok := entry.(string); ok && json.Unmarshal([]byte(payload), &v) == nil && v.ID == id {
			f.localcache.Set(keys[i], v, videoLocalTTL)
			videos[id] = &v
		} else {
			missing = append(missing, id)
		}
	}
	return missing
}

func (f *FeedService) loadVideoBatch(ctx context.Context, ids []uint) ([]*video.Video, error) {
	videos := make([]*video.Video, 0, len(ids))
	missing := make([]uint, 0, len(ids))
	for _, id := range ids {
		// A prior flight may have completed since this request checked L1.
		if v := f.localVideo(id); v != nil {
			videos = append(videos, v)
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return videos, nil
	}
	rows, err := f.repo.GetByIDs(ctx, missing)
	if err != nil {
		return nil, err
	}

	values := make(map[string][]byte, len(rows))
	for _, v := range rows {
		key := f.videoEntityKey(v.ID)
		f.localcache.Set(key, *v, videoLocalTTL)
		if f.rediscache != nil {
			if payload, err := json.Marshal(v); err == nil {
				values[key] = payload
			}
		}
	}
	if len(values) > 0 {
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if err := f.rediscache.SetBytesBatch(cacheCtx, values, videoRedisTTL); err != nil {
			log.Printf("feed video cache fill failed: %v", err)
		}
	}
	return append(videos, rows...), nil
}

func (f *FeedService) ListLatest(ctx context.Context, limit int, cursor *TimeCursor, viewerAccountID uint) (ListLatestResponse, error) {
	videos, err := f.latestCandidates(ctx, limit+1, cursor)
	if err != nil {
		return ListLatestResponse{}, err
	}
	return f.buildTimePage(ctx, videos, limit, viewerAccountID)
}

func (f *FeedService) latestCandidates(ctx context.Context, count int, cursor *TimeCursor) ([]*video.Video, error) {
	if f.rediscache == nil {
		return f.repo.ListLatest(ctx, count, cursor)
	}

	timelineKey := f.rediscache.Key("feed:global_timeline")
	maxScore, readCount := "+inf", count
	if cursor != nil {
		// Include the cursor's score to detect ties in the legacy timeline.
		maxScore = strconv.FormatInt(cursor.Time.UnixMilli(), 10)
		readCount++
	}
	members, err := f.rediscache.ZRevRangeByScoreWithScores(ctx, timelineKey, maxScore, int64(readCount))
	if err != nil {
		return f.repo.ListLatest(ctx, count, cursor)
	}

	if len(members) == 0 && cursor == nil {
		loaded, err, _ := f.requestGroup.Do(f.rediscache.Key("sf:fallback:global_timeline_rebuild"), func() (interface{}, error) {
			videos, err := f.repo.ListLatest(ctx, 1000, nil)
			if err != nil || len(videos) == 0 {
				return videos, err
			}
			entries := make([]redis.Z, 0, len(videos))
			for _, video := range videos {
				entries = append(entries, redis.Z{Score: float64(video.CreateTime.UnixMilli()), Member: strconv.FormatUint(uint64(video.ID), 10)})
			}
			cacheCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = f.rediscache.ZAdd(cacheCtx, timelineKey, entries...)
			return videos, nil
		})
		if err != nil {
			return nil, err
		}
		videos := loaded.([]*video.Video)
		return videos[:min(len(videos), count)], nil
	}

	ids, ordered := legacyLatestIDs(members, cursor)
	if !ordered || len(ids) == 0 {
		return f.repo.ListLatest(ctx, count, cursor)
	}
	ids = ids[:min(len(ids), count)]
	videos, err := f.GetVideoByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(videos) != len(ids) {
		return f.repo.ListLatest(ctx, count, cursor)
	}
	if len(videos) < count {
		last := videos[len(videos)-1]
		coldCursor := &TimeCursor{Time: last.CreateTime, ID: last.ID}
		cold, err := f.repo.ListLatest(ctx, count-len(videos), coldCursor)
		if err != nil {
			return nil, err
		}
		videos = append(videos, cold...)
	}
	return videos, nil
}

// Redis orders equal scores by string ID on the legacy key. Defer those pages
// to MySQL, which can enforce the numeric tie-break without scanning a tie.
func legacyLatestIDs(members []redis.Z, cursor *TimeCursor) ([]uint, bool) {
	ids := make([]uint, 0, len(members))
	for i, entry := range members {
		member, ok := entry.Member.(string)
		id, err := strconv.ParseUint(member, 10, strconv.IntSize)
		if !ok || err != nil || id == 0 || (i > 0 && members[i-1].Score == entry.Score) {
			return nil, false
		}
		if cursor != nil && entry.Score == float64(cursor.Time.UnixMilli()) {
			if uint(id) != cursor.ID {
				return nil, false
			}
			continue
		}
		ids = append(ids, uint(id))
	}
	return ids, true
}

func (f *FeedService) buildTimePage(ctx context.Context, videos []*video.Video, limit int, viewerAccountID uint) (ListLatestResponse, error) {
	page := ListLatestResponse{HasMore: len(videos) > limit}
	videos = videos[:min(len(videos), limit)]
	items, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListLatestResponse{}, err
	}
	page.VideoList = items
	if len(videos) != 0 {
		last := videos[len(videos)-1]
		page.NextTime = last.CreateTime.UnixMilli()
		page.NextIDBefore = last.ID
	}
	return page, nil
}

// 按照点赞数查询视频
func (f *FeedService) ListLikesCount(ctx context.Context, limit int, cursor *LikesCountCursor, viewerAccountID uint) (ListLikesCountResponse, error) {
	videos, err := f.repo.ListLikesCountWithCursor(ctx, limit+1, cursor)
	if err != nil {
		return ListLikesCountResponse{}, err
	}
	hasMore := len(videos) > limit
	videos = videos[:min(len(videos), limit)]
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListLikesCountResponse{}, err
	}
	resp := ListLikesCountResponse{
		VideoList: feedVideos,
		HasMore:   hasMore,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextLikesCountBefore := last.LikesCount
		nextIDBefore := last.ID
		resp.NextLikesCountBefore = &nextLikesCountBefore
		resp.NextIDBefore = &nextIDBefore
	}
	return resp, nil
}

var errPopularitySnapshotUnavailable = errors.New("popularity snapshot unavailable")

func (f *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerAccountID uint, latestPopularity int64, latestBefore time.Time, latestIDBefore uint) (ListByPopularityResponse, error) {
	// Redis 热榜（稳定分页：as_of + offset）
	// Continue a MySQL page with its cursor even if Redis has recovered.
	if f.rediscache != nil && (reqAsOf > 0 || latestIDBefore == 0) {
		asOf := time.Now().UTC().Truncate(time.Minute)
		if reqAsOf > 0 {
			asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Minute)
		}

		const win = 60
		keys := make([]string, 0, win)
		for i := 0; i < win; i++ {
			keys = append(keys, f.rediscache.Key("hot:video:1m:%s", asOf.Add(-time.Duration(i)*time.Minute).Format("200601021504")))
		}

		dest := f.rediscache.Key("hot:video:merge:1m:%s", asOf.Format("200601021504")) // 快照key：同一个as_of页内复用
		opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()

		exists, _ := f.rediscache.Exists(opCtx, dest)
		if !exists {
			_ = f.rediscache.ZUnionStore(opCtx, dest, keys, "SUM")
			_ = f.rediscache.Expire(opCtx, dest, 2*time.Minute) // 给翻页留时间
		}

		videos, nextOffset, err := f.popularitySnapshotPage(ctx, opCtx, dest, offset, limit)
		if err != nil && !errors.Is(err, errPopularitySnapshotUnavailable) {
			return ListByPopularityResponse{}, err
		}
		if err == nil && (len(videos) != 0 || offset > 0) {
			hasMore := len(videos) > limit
			videos = videos[:min(len(videos), limit)]
			items, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
			if err != nil {
				return ListByPopularityResponse{}, err
			}
			resp := ListByPopularityResponse{
				VideoList:  items,
				AsOf:       asOf.Unix(),
				NextOffset: nextOffset,
				HasMore:    hasMore,
			}
			resp.setDBCursor(videos)
			return resp, nil
		}
	}

	videos, err := f.repo.ListByPopularity(ctx, limit+1, latestPopularity, latestBefore, latestIDBefore)
	if err != nil {
		return ListByPopularityResponse{}, err
	}
	hasMore := len(videos) > limit
	videos = videos[:min(len(videos), limit)]
	items, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListByPopularityResponse{}, err
	}
	resp := ListByPopularityResponse{VideoList: items, HasMore: hasMore}
	resp.setDBCursor(videos)
	return resp, nil
}

func (r *ListByPopularityResponse) setDBCursor(videos []*video.Video) {
	if len(videos) != 0 {
		last := videos[len(videos)-1]
		r.NextLatestPopularity = &last.Popularity
		r.NextLatestBefore = &last.CreateTime
		r.NextLatestIDBefore = &last.ID
	}
}

func (f *FeedService) popularitySnapshotPage(ctx, redisCtx context.Context, key string, offset, limit int) ([]*video.Video, int, error) {
	videos := make([]*video.Video, 0, limit+1)
	nextOffset := offset
	for start := int64(offset); len(videos) <= limit; {
		batchSize := int64(limit + 1)
		stop := start + batchSize - 1
		members, err := f.rediscache.ZRevRange(redisCtx, key, start, stop)
		if err != nil {
			return nil, offset, fmt.Errorf("%w: %w", errPopularitySnapshotUnavailable, err)
		}
		idsByRank := make([]uint, len(members))
		ids := make([]uint, 0, len(members))
		for i, member := range members {
			if parsed, err := strconv.ParseUint(member, 10, strconv.IntSize); err == nil && parsed != 0 {
				id := uint(parsed)
				idsByRank[i] = id
				ids = append(ids, id)
			}
		}
		// The sorted set and entity cache can both outlive a deleted row.
		existing, err := f.repo.existingVideoIDs(ctx, ids)
		if err != nil {
			return nil, offset, err
		}
		batch, err := f.GetVideoByIDs(ctx, existing)
		if err != nil {
			return nil, offset, err
		}
		byID := make(map[uint]*video.Video, len(batch))
		for _, video := range batch {
			byID[video.ID] = video
		}
		for i, id := range idsByRank {
			if video := byID[id]; video != nil {
				videos = append(videos, video)
				if len(videos) <= limit {
					nextOffset = int(start) + i + 1
				} else {
					return videos, nextOffset, nil
				}
			}
		}
		if int64(len(members)) < batchSize {
			break
		}
		start += batchSize
	}
	return videos, nextOffset, nil
}

func (f *FeedService) buildFeedVideos(ctx context.Context, videos []*video.Video, viewerAccountID uint) ([]FeedVideoItem, error) {
	feedVideos := make([]FeedVideoItem, 0, len(videos))
	videoIDs := make([]uint, len(videos))
	for i, v := range videos {
		videoIDs[i] = v.ID
	}
	likedMap, err := f.likeRepo.BatchGetLiked(ctx, videoIDs, viewerAccountID)
	if err != nil {
		return nil, err
	}
	for _, video := range videos {
		feedVideos = append(feedVideos, FeedVideoItem{
			ID:          video.ID,
			Author:      FeedAuthor{ID: video.AuthorID, Username: video.Username},
			Title:       video.Title,
			Description: video.Description,
			PlayURL:     video.PlayURL,
			CoverURL:    video.CoverURL,
			CreateTime:  video.CreateTime.Unix(),
			LikesCount:  video.LikesCount,
			IsLiked:     likedMap[video.ID],
		})
	}
	return feedVideos, nil
}

func buildOrderedResult(orderedIDs []uint, dataMap map[uint]*video.Video) []*video.Video {
	res := make([]*video.Video, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if v, exists := dataMap[id]; exists && v != nil {
			res = append(res, v)
		}
	}
	return res
}

func (f *FeedService) ListByTag(ctx context.Context, tagName string, limit int, viewerAccountID uint) ([]FeedVideoItem, error) {
	videos, err := f.repo.ListByTag(ctx, tagName, limit)
	if err != nil {
		return nil, err
	}
	return f.buildFeedVideos(ctx, videos, viewerAccountID)
}
