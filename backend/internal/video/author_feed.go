package video

import (
	"context"
	"errors"
	"fmt"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/social"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DistributionMode string

const (
	DistributionPush DistributionMode = "push"
	DistributionPull DistributionMode = "pull"
)

type AuthorFeedState struct {
	AuthorID         uint             `gorm:"primaryKey;autoIncrement:false"`
	DistributionMode DistributionMode `gorm:"type:enum('push','pull');not null;check:chk_author_feed_mode,distribution_mode IN ('push','pull')"`
}

// An empty current mode represents an author without a persisted state.
func distributionMode(current DistributionMode, followers int64, cfg config.FollowingFeedConfig) DistributionMode {
	switch current {
	case DistributionPush:
		if followers < cfg.HighThreshold {
			return DistributionPush
		}
	case DistributionPull:
		if followers <= cfg.LowThreshold {
			return DistributionPush
		}
	default:
		if followers < cfg.InitialThreshold {
			return DistributionPush
		}
	}
	return DistributionPull
}

func (vs *VideoService) resolveDistributionMode(ctx context.Context, tx *gorm.DB, authorID uint) (DistributionMode, error) {
	// Serialize publishes on the existing account row, including the first
	// publish when there is no author_feed_states row to lock yet.
	var author account.Account
	if err := tx.Select("id").Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", authorID).Take(&author).Error; err != nil {
		return "", fmt.Errorf("lock publishing author: %w", err)
	}

	var state AuthorFeedState
	err := tx.Where("author_id = ?", authorID).Take(&state).Error
	firstPublish := errors.Is(err, gorm.ErrRecordNotFound)
	if err != nil && !firstPublish {
		return "", fmt.Errorf("read author feed state: %w", err)
	}
	followers, err := social.NewSocialRepository(tx).CountFollowers(ctx, authorID)
	if err != nil {
		return "", fmt.Errorf("count publishing author's followers: %w", err)
	}
	mode := distributionMode(state.DistributionMode, followers, vs.followingFeed)
	if firstPublish {
		state = AuthorFeedState{AuthorID: authorID, DistributionMode: mode}
		err = tx.Create(&state).Error
	} else if mode != state.DistributionMode {
		err = tx.Model(&state).Update("distribution_mode", mode).Error
	}
	if err != nil {
		return "", fmt.Errorf("persist author feed state: %w", err)
	}
	return mode, nil
}
