package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"feedsystem_video_go/internal/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TimelineMQ struct{ publisher *confirmedPublisher }

const (
	TimelineExchange     = "video.timeline.events"
	GlobalTimelineQueue  = "video.timeline.update.queue"
	FollowingFanoutQueue = "video.following.fanout.queue"
	TimelinePublishKey   = "video.timeline.publish"
)

type TimelineEvent struct {
	EventID          string    `json:"event_id"`
	VideoID          uint      `json:"video_id"`
	AuthorID         uint      `json:"author_id,omitempty"`
	DistributionMode string    `json:"distribution_mode,omitempty"`
	CreateTime       int64     `json:"create_time"`
	OccurredAt       time.Time `json:"occurred_at"`
	Legacy           bool      `json:"legacy,omitempty"`
}

func DeclareTimelineQueue(ch *amqp.Channel, queue string) error {
	switch queue {
	case GlobalTimelineQueue:
		// Retain the existing queue arguments and binding for deployed brokers.
		return DeclareTopic(ch, TimelineExchange, queue, "video.timeline.*")
	case FollowingFanoutQueue:
		if err := ch.ExchangeDeclare(TimelineExchange, "topic", true, false, false, false, nil); err != nil {
			return err
		}
		if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			return err
		}
		return ch.QueueBind(queue, TimelinePublishKey, TimelineExchange, false, nil)
	default:
		return errors.New("unknown timeline queue")
	}
}

func NewTimelineMQ(cfg config.RabbitMQConfig) *TimelineMQ {
	return &TimelineMQ{publisher: newConfirmedPublisher(cfg, func(ch *amqp.Channel) error {
		if err := DeclareTimelineQueue(ch, GlobalTimelineQueue); err != nil {
			return err
		}
		return DeclareTimelineQueue(ch, FollowingFanoutQueue)
	})}
}

func (t *TimelineMQ) Close() { t.publisher.close() }

func (t *TimelineMQ) Publish(ctx context.Context, event TimelineEvent) error {
	if event.EventID == "" || event.VideoID == 0 {
		return errors.New("timeline event requires event ID and video ID")
	}
	p := t.publisher
	select {
	case p.gate <- struct{}{}:
		defer func() { <-p.gate }()
	default:
		return &PublishError{Code: "publisher_busy"}
	}
	if p.closed {
		return &PublishError{Code: "publisher_unavailable"}
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return p.publishConfirmed(ctx, TimelineExchange, TimelinePublishKey, event.EventID, body, event.OccurredAt)
}
