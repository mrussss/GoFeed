package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type CommentMQ struct {
	ch *amqp.Channel
}

const (
	commentExchange          = "comment.events"
	CommentNotificationQueue = "notification.comment"
	commentPublishRK         = "comment.publish"
)

type CommentEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	AuthorID   uint      `json:"author_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewCommentMQ(base *RabbitMQ) (*CommentMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	ch, err := base.NewChannel()
	if err != nil {
		return nil, err
	}
	if err := DeclareTopic(ch, commentExchange, CommentNotificationQueue, commentPublishRK); err != nil {
		ch.Close()
		return nil, err
	}
	return &CommentMQ{ch: ch}, nil
}

func (c *CommentMQ) Publish(ctx context.Context, videoID, authorID uint) error {
	if c == nil || c.ch == nil {
		return errors.New("comment mq is not initialized")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	return PublishJSON(ctx, c.ch, commentExchange, commentPublishRK, CommentEvent{
		EventID: id, VideoID: videoID, AuthorID: authorID, OccurredAt: time.Now().UTC(),
	})
}
