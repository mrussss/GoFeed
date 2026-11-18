package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SocialMQ struct {
	ch *amqp.Channel
}

const (
	socialExchange          = "social.events"
	SocialNotificationQueue = "notification.social"
	socialFollowRK          = "social.follow"
)

type SocialEvent struct {
	EventID    string    `json:"event_id"`
	FollowerID uint      `json:"follower_id"`
	VloggerID  uint      `json:"vlogger_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewSocialMQ(base *RabbitMQ) (*SocialMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	ch, err := base.NewChannel()
	if err != nil {
		return nil, err
	}
	if err := DeclareTopic(ch, socialExchange, SocialNotificationQueue, socialFollowRK); err != nil {
		ch.Close()
		return nil, err
	}
	return &SocialMQ{ch: ch}, nil
}

func (s *SocialMQ) Follow(ctx context.Context, followerID, vloggerID uint) error {
	if s == nil || s.ch == nil {
		return errors.New("social mq is not initialized")
	}
	if followerID == 0 || vloggerID == 0 {
		return errors.New("followerID and vloggerID are required")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	evt := SocialEvent{
		EventID:    id,
		FollowerID: followerID,
		VloggerID:  vloggerID,
		OccurredAt: time.Now().UTC(),
	}
	return PublishJSON(ctx, s.ch, socialExchange, socialFollowRK, evt)
}
