package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// AddBoundedZSets inserts one member per key and retains the highest ranks.
// Every pipeline result must be known before a caller acknowledges delivery.
func (c *Client) AddBoundedZSets(ctx context.Context, keys []string, member redis.Z, maxLen int64) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	if maxLen <= 0 {
		return errors.New("sorted set limit must be positive")
	}
	commands, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			pipe.ZAdd(ctx, key, member)
			pipe.ZRemRangeByRank(ctx, key, 0, -maxLen-1)
		}
		return nil
	})
	for i, cmd := range commands {
		if commandErr := cmd.Err(); commandErr != nil {
			return fmt.Errorf("%s %s: %w", cmd.Name(), keys[i/2], commandErr)
		}
	}
	return err
}

func (c *Client) ZincrBy(ctx context.Context, key string, member string, score float64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZIncrBy(ctx, key, score, member).Err()
}

func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZAdd(ctx, key, members...).Err()
}

func (c *Client) ZRemRangeByRank(ctx context.Context, key string, start int64, stop int64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZRemRangeByRank(ctx, key, start, stop).Err()
}

func (c *Client) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.ZRangeWithScores(ctx, key, start, stop).Result()
}

func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Expire(ctx, key, ttl).Err()
}

func (c *Client) ZUnionStore(ctx context.Context, dst string, keys []string, aggregate string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZUnionStore(ctx, dst, &redis.ZStore{
		Keys:      keys,
		Aggregate: aggregate,
	}).Err()
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	n, err := c.rdb.Exists(ctx, key).Result()
	return n > 0, err
}

func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRange(ctx, key, start, stop).Result()
}

// ZRevRangeByLexBatch reads each score-zero sorted set in descending member
// order. A nonempty before member is excluded from every range.
func (c *Client) ZRevRangeByLexBatch(ctx context.Context, keys []string, before string, count int64) ([][]string, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	max := "+"
	if before != "" {
		max = "(" + before
	}
	ranges := make([]*redis.StringSliceCmd, 0, len(keys))
	_, err := c.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			ranges = append(ranges, pipe.ZRevRangeByLex(ctx, key, &redis.ZRangeBy{Min: "-", Max: max, Count: count}))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	members := make([][]string, len(ranges))
	for i, cmd := range ranges {
		members[i] = cmd.Val()
	}
	return members, nil
}

func (c *Client) ZRevRangeByScoreWithScores(ctx context.Context, key, max string, count int64) ([]redis.Z, error) {
	return c.rdb.ZRevRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{Min: "-inf", Max: max, Count: count}).Result()
}
