package video

import (
	"context"
	"errors"
	"strconv"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

// 更新视频流行度缓存
func UpdatePopularityCache(ctx context.Context, cache *rediscache.Client, id uint, change int64) error {
	if cache == nil || id == 0 || change == 0 {
		return nil
	}

	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 50*time.Millisecond)
	defer cancel()
	invalidateErr := cache.Del(opCtx, cache.Key("video:detail:id=%d", id), cache.Key("video:entity:%d", id))

	now := time.Now().UTC().Truncate(time.Minute)
	windowKey := cache.Key("hot:video:1m:%s", now.Format("200601021504"))
	member := strconv.FormatUint(uint64(id), 10)
	incrementErr := cache.ZincrBy(opCtx, windowKey, member, float64(change))
	expireErr := cache.Expire(opCtx, windowKey, 2*time.Hour)
	return errors.Join(invalidateErr, incrementErr, expireErr)
}
