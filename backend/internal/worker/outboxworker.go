package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"

	"gorm.io/gorm"
)

const videoOutboxBatchSize = 100

// RelayVideoOutbox polls the oldest visible pending IDs on every pass. A single
// Worker owns the Relay and each successful Confirm gets a conditional mark.
func RelayVideoOutbox(ctx context.Context, db *gorm.DB, mq *rabbitmq.TimelineMQ) {
	for ctx.Err() == nil {
		iteration, cancel := context.WithTimeout(ctx, 5*time.Second)
		count, err := RelayVideoOutboxOnce(iteration, db, mq)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Printf("video outbox relay: %v", err)
		}
		if err == nil && count == videoOutboxBatchSize {
			continue
		}
		if !waitVideoRetry(ctx) {
			return
		}
	}
}

func RelayVideoOutboxOnce(ctx context.Context, db *gorm.DB, mq *rabbitmq.TimelineMQ) (int, error) {
	var rows []video.OutboxMsg
	if err := db.WithContext(ctx).Where("status = ? AND event_type = ?", "pending", "video_published").
		Order("id ASC").Limit(videoOutboxBatchSize).Find(&rows).Error; err != nil {
		return 0, err
	}
	for i, row := range rows {
		event, err := timelineEvent(row)
		status := "published"
		if err != nil {
			// An incomplete snapshot cannot be reconstructed from current state.
			log.Printf("video outbox %d invalid: %v", row.ID, err)
			status = "invalid"
		} else if err = mq.Publish(ctx, event); err != nil {
			return i, fmt.Errorf("publish video outbox %d: %w", row.ID, err)
		}
		result := db.WithContext(ctx).Model(&video.OutboxMsg{}).
			Where("id = ? AND status = ?", row.ID, "pending").UpdateColumn("status", status)
		if result.Error != nil {
			return i, fmt.Errorf("mark video outbox %d %s: %w", row.ID, status, result.Error)
		}
		if result.RowsAffected != 1 {
			return i, fmt.Errorf("mark video outbox %d %s: changed %d rows", row.ID, status, result.RowsAffected)
		}
	}
	return len(rows), nil
}

func timelineEvent(row video.OutboxMsg) (rabbitmq.TimelineEvent, error) {
	event := rabbitmq.TimelineEvent{
		VideoID: row.VideoID, CreateTime: row.CreateTime.UnixMilli(), OccurredAt: row.CreateTime.UTC(),
	}
	if row.VideoID == 0 || row.CreateTime.IsZero() {
		return event, fmt.Errorf("missing video ID or creation time")
	}
	if row.EventID == nil && row.AuthorID == nil && row.DistributionMode == nil {
		event.EventID = fmt.Sprintf("legacy-outbox-%d", row.ID)
		event.Legacy = true
		return event, nil
	}
	if row.EventID == nil || *row.EventID == "" || row.AuthorID == nil || *row.AuthorID == 0 ||
		row.DistributionMode == nil || (*row.DistributionMode != video.DistributionPush && *row.DistributionMode != video.DistributionPull) {
		return event, fmt.Errorf("incomplete or invalid distribution snapshot")
	}
	event.EventID = *row.EventID
	event.AuthorID = *row.AuthorID
	event.DistributionMode = string(*row.DistributionMode)
	return event, nil
}
