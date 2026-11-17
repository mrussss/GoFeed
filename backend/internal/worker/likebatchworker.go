package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	LikeCommandBatchSize = 50
	LikeCommandMaxWait   = 20 * time.Millisecond
)

type LikeCommandBatchWorker struct {
	ch        *amqp.Channel
	likes     *video.LikeRepository
	partition int
}

func NewLikeCommandBatchWorker(ch *amqp.Channel, likes *video.LikeRepository, partition int) *LikeCommandBatchWorker {
	return &LikeCommandBatchWorker{ch: ch, likes: likes, partition: partition}
}

// HandleBatch applies one collected batch and ACKs every delivery only after
// COMMIT. Returning an error leaves unacknowledged messages for Run's close.
func (w *LikeCommandBatchWorker) HandleBatch(ctx context.Context, batch []amqp.Delivery) error {
	events := make([]rabbitmq.LikeCommandEvent, 0, len(batch))
	for _, delivery := range batch {
		var event rabbitmq.LikeCommandEvent
		// A missing/null desired_liked must not silently become an unlike command.
		body := struct {
			*rabbitmq.LikeCommandEvent
			DesiredLiked *bool `json:"desired_liked"`
		}{LikeCommandEvent: &event}
		if err := json.Unmarshal(delivery.Body, &body); err != nil {
			return fmt.Errorf("invalid like command JSON: %w", err)
		}
		if body.DesiredLiked == nil {
			return errors.New("like command requires desired_liked")
		}
		event.DesiredLiked = *body.DesiredLiked
		if err := event.Validate(); err != nil {
			return err
		}
		if rabbitmq.LikeCommandPartition(event.AccountID, event.VideoID) != w.partition {
			return fmt.Errorf("command %d delivered to wrong partition %d", event.CommandID, w.partition)
		}
		events = append(events, event)
	}
	if err := w.likes.ApplyLikeCommandBatch(ctx, events); err != nil {
		return err
	}
	for _, delivery := range batch {
		if err := delivery.Ack(false); err != nil {
			return fmt.Errorf("like batch committed, ACK failed: %w", err)
		}
	}
	return nil
}

// Run owns its dedicated Channel. On cancellation, malformed input, DB failure
// or ACK failure it closes the Channel so every unacknowledged message requeues.
func (w *LikeCommandBatchWorker) Run(ctx context.Context) error {
	if w.ch == nil || w.likes == nil || w.partition < 0 || w.partition >= rabbitmq.LikeCommandPartitions {
		return errors.New("like batch worker is not initialized")
	}
	defer w.ch.Close()
	if err := rabbitmq.DeclareLikeCommands(w.ch); err != nil {
		return err
	}
	if err := w.ch.Qos(LikeCommandBatchSize, 0, false); err != nil {
		return err
	}
	deliveries, err := w.ch.Consume(rabbitmq.LikeCommandQueue(w.partition), "", false, true, false, false, nil)
	if err != nil {
		return err
	}
	batch := make([]amqp.Delivery, 0, LikeCommandBatchSize)
	var timer *time.Timer
	var timeout <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	flush := func() error {
		if timer != nil {
			timer.Stop()
		}
		timeout = nil
		if err := w.HandleBatch(ctx, batch); err != nil {
			log.Printf("like batch partition %d failed; closing consumer for redelivery: %v", w.partition, err)
			return err
		}
		batch = batch[:0]
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("like batch deliveries channel closed")
			}
			batch = append(batch, delivery)
			if len(batch) == 1 {
				timer = time.NewTimer(LikeCommandMaxWait)
				timeout = timer.C
			}
			if len(batch) == LikeCommandBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-timeout:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// RunLikeCommandBatchWorkers is opt-in assembly for an isolated/new command
// pipeline. It does not start Relay or the legacy Worker. On any worker failure
// all four stop; the caller can restart this group after a fixed pause/reconnect.
func RunLikeCommandBatchWorkers(ctx context.Context, conn *amqp.Connection, likes *video.LikeRepository) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	channels := make([]*amqp.Channel, 0, rabbitmq.LikeCommandPartitions)
	defer func() {
		for _, ch := range channels {
			_ = ch.Close()
		}
	}()
	for i := 0; i < rabbitmq.LikeCommandPartitions; i++ {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		channels = append(channels, ch)
	}
	done := make(chan error, rabbitmq.LikeCommandPartitions)
	for partition, ch := range channels {
		go func(partition int, ch *amqp.Channel) {
			done <- NewLikeCommandBatchWorker(ch, likes, partition).Run(ctx)
		}(partition, ch)
	}
	first := <-done
	cancel()
	for i := 1; i < rabbitmq.LikeCommandPartitions; i++ {
		<-done
	}
	return first
}
