package worker

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
	"fmt"
	"log"
	"time"

	"gorm.io/gorm"
)

const likeCommandMarkBatchSize = 50

type LikeCommandPublisher interface {
	Publish(context.Context, rabbitmq.LikeCommandEvent) error
}

// LikeCommandRelay is deliberately single-instance, started by the Worker main.
// It can buffer durable commands while the partition consumers are reconnecting.
// The caller owns and closes the publisher after Run returns.
type LikeCommandRelay struct {
	db        *gorm.DB
	publisher LikeCommandPublisher
	interval  time.Duration
}

func NewLikeCommandRelay(db *gorm.DB, publisher LikeCommandPublisher, interval time.Duration) *LikeCommandRelay {
	if interval <= 0 {
		interval = time.Second
	}
	return &LikeCommandRelay{db: db, publisher: publisher, interval: interval}
}

// RunOnce always starts from the oldest currently visible pending rows, never
// from a high-water ID (which would skip transactions that commit late).
func (r *LikeCommandRelay) RunOnce(ctx context.Context) (int, error) {
	var commands []video.CommandOutbox
	if err := r.db.WithContext(ctx).Where("published_at IS NULL").Order("id ASC").Limit(100).Find(&commands).Error; err != nil {
		return 0, err
	}
	published := 0
	confirmed := make([]uint64, 0, likeCommandMarkBatchSize)
	markConfirmed := func(markCtx context.Context) error {
		if len(confirmed) == 0 {
			return nil
		}
		// Failed updates leave the IDs pending for replay with the same MessageId/body.
		result := r.db.WithContext(markCtx).Model(&video.CommandOutbox{}).
			Where("id IN ? AND published_at IS NULL", confirmed).
			UpdateColumn("published_at", gorm.Expr("NOW(3)"))
		if result.Error != nil {
			return fmt.Errorf("mark %d confirmed like commands: %w", len(confirmed), result.Error)
		}
		published += int(result.RowsAffected)
		if result.RowsAffected != int64(len(confirmed)) {
			return fmt.Errorf("mark confirmed like commands: updated %d rows, expected %d", result.RowsAffected, len(confirmed))
		}
		confirmed = confirmed[:0]
		return nil
	}
	for _, command := range commands {
		event := rabbitmq.LikeCommandEvent{CommandID: command.ID, RequestID: command.RequestID, AccountID: command.AccountID, VideoID: command.VideoID, DesiredLiked: command.DesiredLiked, Revision: command.Revision}
		if err := r.publisher.Publish(ctx, event); err != nil {
			publishErr := fmt.Errorf("publish like command %d: %w", command.ID, err)
			markCtx := ctx
			if len(confirmed) != 0 && ctx.Err() != nil {
				// Give confirmed commands a bounded write during Relay shutdown.
				var cancel context.CancelFunc
				markCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				defer cancel()
			}
			markErr := markConfirmed(markCtx)
			return published, errors.Join(publishErr, markErr)
		}
		confirmed = append(confirmed, command.ID)
		if len(confirmed) == likeCommandMarkBatchSize {
			if err := markConfirmed(ctx); err != nil {
				return published, err
			}
		}
	}
	err := markConfirmed(ctx)
	return published, err
}

func (r *LikeCommandRelay) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		published, err := r.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("like command relay: %v", err)
		}
		if err == nil && published == 100 {
			continue
		}
		// Pause when caught up or after any failure; drain full successful rounds immediately.
		timer := time.NewTimer(r.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
