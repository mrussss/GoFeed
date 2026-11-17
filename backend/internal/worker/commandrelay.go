package worker

import (
	"context"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
	"fmt"
	"log"
	"time"

	"gorm.io/gorm"
)

type LikeCommandPublisher interface {
	Publish(context.Context, rabbitmq.LikeCommandEvent) error
}

// LikeCommandRelay is deliberately single-instance. Run it in a goroutine only
// after the B-stage consumers are ready; it is not started by the current main.
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
	for _, command := range commands {
		event := rabbitmq.LikeCommandEvent{CommandID: command.ID, RequestID: command.RequestID, AccountID: command.AccountID, VideoID: command.VideoID, DesiredLiked: command.DesiredLiked, Revision: command.Revision}
		if err := r.publisher.Publish(ctx, event); err != nil {
			return published, fmt.Errorf("publish like command %d: %w", command.ID, err)
		}
		// No transaction is held while waiting for RabbitMQ. A failure here leaves
		// the row pending; the next pass republishes it with the same MessageId/body.
		result := r.db.WithContext(ctx).Model(&video.CommandOutbox{}).Where("id = ? AND published_at IS NULL", command.ID).UpdateColumn("published_at", gorm.Expr("NOW(3)"))
		if result.Error != nil {
			return published, fmt.Errorf("mark like command %d: %w", command.ID, result.Error)
		}
		published += int(result.RowsAffected)
	}
	return published, nil
}

func (r *LikeCommandRelay) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("like command relay: %v", err)
		}
		// A fixed pause on every round also bounds retry rate for persistent failures.
		timer := time.NewTimer(r.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
