package worker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/feed"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"

	oredis "github.com/redis/go-redis/v9"
)

func RunFollowingFanoutConsumer(ctx context.Context, mq config.RabbitMQConfig, followers *social.SocialRepository, cache *rediscache.Client, cfg config.FollowingFanoutConfig) {
	quarantine := rabbitmq.NewFollowingQuarantineMQ(mq)
	defer quarantine.Close()
	runFollowingFanoutConsumer(ctx, mq, followers, cache, cfg, quarantine.Publish)
}

func runFollowingFanoutConsumer(ctx context.Context, mq config.RabbitMQConfig, followers *social.SocialRepository, cache *rediscache.Client, cfg config.FollowingFanoutConfig, quarantine quarantineVideoDelivery) {
	runVideoConsumer(ctx, mq, rabbitmq.FollowingFanoutQueue, func(ctx context.Context, event rabbitmq.TimelineEvent) error {
		err := writeFollowingCandidates(ctx, followers, cache, cfg, event)
		if !isFollowingWrongType(err) {
			return err
		}
		// Retry the delivery once to allow a concurrent repair of the Redis key.
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		return writeFollowingCandidates(ctx, followers, cache, cfg, event)
	}, quarantine)
}

func isFollowingWrongType(err error) bool {
	// HasErrorPrefix requires a Redis protocol error and unwraps %w.
	return oredis.HasErrorPrefix(err, "WRONGTYPE ")
}

func followingFailureClass(err error) string {
	switch {
	case errors.Is(err, errInvalidVideoEvent):
		return "invalid_event"
	case isFollowingWrongType(err):
		return "redis_wrongtype"
	default:
		return ""
	}
}

func decodeFollowingEvent(body []byte) (rabbitmq.TimelineEvent, error) {
	var wire struct {
		rabbitmq.TimelineEvent
		CreateTime *int64 `json:"create_time"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return wire.TimelineEvent, fmt.Errorf("%w: malformed JSON: %v", errInvalidVideoEvent, err)
	}
	if wire.VideoID == 0 || wire.CreateTime == nil {
		return wire.TimelineEvent, fmt.Errorf("%w: missing video ID or create time", errInvalidVideoEvent)
	}
	wire.TimelineEvent.CreateTime = *wire.CreateTime
	return wire.TimelineEvent, nil
}

func writeFollowingCandidates(ctx context.Context, followers *social.SocialRepository, cache *rediscache.Client, cfg config.FollowingFanoutConfig, event rabbitmq.TimelineEvent) error {
	if event.Legacy {
		id, ok := strings.CutPrefix(event.EventID, "legacy-outbox-")
		rowID, err := strconv.ParseUint(id, 10, 64)
		if ok && err == nil && rowID > 0 && event.AuthorID == 0 && event.DistributionMode == "" {
			return nil
		}
		return fmt.Errorf("%w: invalid legacy snapshot", errInvalidVideoEvent)
	}
	id, err := hex.DecodeString(event.EventID)
	if err != nil || len(id) != 16 || event.AuthorID == 0 {
		return fmt.Errorf("%w: invalid event ID or missing author ID", errInvalidVideoEvent)
	}
	encoded, err := feed.FollowingMember(event.CreateTime, event.VideoID)
	if err != nil {
		return fmt.Errorf("%w: %v", errInvalidVideoEvent, err)
	}
	member := oredis.Z{Member: encoded, Score: 0}
	switch video.DistributionMode(event.DistributionMode) {
	case video.DistributionPush:
		return pushFollowingCandidates(ctx, followers, cache, cfg, event.AuthorID, member)
	case video.DistributionPull:
		key := cache.Key("feed:author_outbox:%d", event.AuthorID)
		writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		return cache.AddBoundedZSets(writeCtx, []string{key}, member, cfg.AuthorOutboxMaxLen)
	default:
		return fmt.Errorf("%w: invalid distribution mode %q", errInvalidVideoEvent, event.DistributionMode)
	}
}

func pushFollowingCandidates(ctx context.Context, followers *social.SocialRepository, cache *rediscache.Client, cfg config.FollowingFanoutConfig, authorID uint, member oredis.Z) error {
	var afterID uint
	for {
		readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		links, err := followers.ListFollowerLinks(readCtx, authorID, afterID, cfg.BatchSize)
		cancel()
		if err != nil || len(links) == 0 {
			return err
		}

		keys := make([]string, len(links))
		for i, link := range links {
			keys[i] = cache.Key("feed:inbox:%d", link.FollowerID)
		}
		writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = cache.AddBoundedZSets(writeCtx, keys, member, cfg.InboxMaxLen)
		cancel()
		if err != nil || len(links) < cfg.BatchSize {
			return err
		}
		afterID = links[len(links)-1].ID
	}
}
