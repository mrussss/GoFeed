package feed

import (
	"container/heap"
	"context"
	"time"

	"feedsystem_video_go/internal/video"
)

func (f *FeedService) ListByFollowing(ctx context.Context, limit int, cursor *TimeCursor, viewerID uint) (ListByFollowingResponse, error) {
	videos, err := f.followingVideos(ctx, limit+1, cursor, viewerID)
	if err != nil {
		return ListByFollowingResponse{}, err
	}
	return f.buildTimePage(ctx, videos, limit, viewerID)
}

func (f *FeedService) followingVideos(ctx context.Context, count int, cursor *TimeCursor, viewerID uint) ([]*video.Video, error) {
	if viewerID == 0 {
		return nil, nil
	}
	if f.rediscache == nil {
		return f.followingFromMySQL(ctx, count, viewerID, cursor)
	}
	authors, err := readFeedMySQL(ctx, f.reads, func(queryCtx context.Context) ([]uint, error) {
		return f.socialRepo.ListFollowedAuthorIDs(queryCtx, viewerID)
	})
	if err != nil || len(authors) == 0 {
		return nil, err
	}
	keys := make([]string, 0, len(authors)+1)
	keys = append(keys, f.rediscache.Key("feed:inbox:%d", viewerID))
	for _, authorID := range authors {
		// A push author may still have videos written while in pull mode.
		keys = append(keys, f.rediscache.Key("feed:author_outbox:%d", authorID))
	}

	var before string
	if cursor != nil {
		before, err = FollowingMember(cursor.Time.UnixMilli(), cursor.ID)
		if err != nil {
			return nil, err
		}
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	sources, err := f.rediscache.ZRevRangeByLexBatch(cacheCtx, keys, before, int64(count))
	cancel()
	if err != nil {
		return f.followingFromMySQL(ctx, count, viewerID, cursor)
	}

	candidates := mergeFollowingCandidates(sources, count)
	videos, err := f.loadFollowingCandidates(ctx, viewerID, candidates)
	if err != nil {
		return nil, err
	}
	if len(videos) == count {
		return videos, nil
	}

	// There can be unindexed rows anywhere below the request cursor. Read the
	// whole DB prefix so a missing recent row can also precede a Redis row.
	history, err := f.followingFromMySQL(ctx, count, viewerID, cursor)
	if err != nil {
		return nil, err
	}
	return mergeTimeOrderedVideos(videos, history, count), nil
}

func (f *FeedService) followingFromMySQL(ctx context.Context, count int, viewerID uint, cursor *TimeCursor) ([]*video.Video, error) {
	return readFeedMySQL(ctx, f.reads, func(queryCtx context.Context) ([]*video.Video, error) {
		return f.repo.ListByFollowing(queryCtx, count, viewerID, cursor)
	})
}

func (f *FeedService) loadFollowingCandidates(ctx context.Context, viewerID uint, candidates []TimeCursor) ([]*video.Video, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	ids := make([]uint, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.ID)
	}
	// Cached entities can outlive deletion. Check existence, the current
	// follow relation and the indexed timestamp using just two columns.
	times, err := readFeedMySQL(ctx, f.reads, func(queryCtx context.Context) (map[uint]int64, error) {
		return f.repo.followingCandidateTimes(queryCtx, viewerID, ids)
	})
	if err != nil {
		return nil, err
	}
	ids = ids[:0]
	for _, candidate := range candidates {
		if timestamp, ok := times[candidate.ID]; ok && timestamp == candidate.Time.UnixMilli() {
			ids = append(ids, candidate.ID)
		} else {
			delete(times, candidate.ID)
		}
	}
	videos, err := f.GetVideoByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[uint]*video.Video, len(videos))
	for _, v := range videos {
		if timestamp, ok := times[v.ID]; ok && timestamp == v.CreateTime.UnixMilli() {
			byID[v.ID] = v
		}
	}
	return buildOrderedResult(ids, byID), nil
}

func mergeFollowingCandidates(sources [][]string, count int) []TimeCursor {
	queue := make(followingHeap, 0, len(sources))
	for _, source := range sources {
		if len(source) != 0 {
			queue = append(queue, source)
		}
	}
	heap.Init(&queue)

	out := make([]TimeCursor, 0, count)
	seen := make(map[uint]struct{}, count)
	for queue.Len() > 0 && len(out) < count {
		head := queue[0][0]
		if len(queue[0]) == 1 {
			heap.Pop(&queue)
		} else {
			queue[0] = queue[0][1:]
			heap.Fix(&queue, 0)
		}
		at, id, err := ParseFollowingMember(head)
		if _, duplicate := seen[id]; err == nil && !duplicate {
			seen[id] = struct{}{}
			out = append(out, TimeCursor{Time: time.UnixMilli(at).UTC(), ID: id})
		}
	}
	return out
}

type followingHeap [][]string

func (h followingHeap) Len() int           { return len(h) }
func (h followingHeap) Less(i, j int) bool { return h[i][0] > h[j][0] }
func (h followingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *followingHeap) Push(x any)        { *h = append(*h, x.([]string)) }
func (h *followingHeap) Pop() any {
	last := len(*h) - 1
	source := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return source
}

func mergeTimeOrderedVideos(cached, history []*video.Video, count int) []*video.Video {
	merged := make([]*video.Video, 0, count)
	seen := make(map[uint]struct{}, count)
	for len(merged) < count && (len(cached) > 0 || len(history) > 0) {
		var next *video.Video
		if len(history) == 0 || (len(cached) > 0 && !timeOrderedVideoBefore(history[0], cached[0])) {
			next, cached = cached[0], cached[1:]
		} else {
			next, history = history[0], history[1:]
		}
		if _, duplicate := seen[next.ID]; !duplicate {
			seen[next.ID] = struct{}{}
			merged = append(merged, next)
		}
	}
	return merged
}

func timeOrderedVideoBefore(a, b *video.Video) bool {
	at, bt := a.CreateTime.UnixMilli(), b.CreateTime.UnixMilli()
	return at > bt || (at == bt && a.ID > b.ID)
}
