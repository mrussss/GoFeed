package rabbitmq

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"feedsystem_video_go/internal/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

const FollowingQuarantineQueue = "video.following.fanout.quarantine.queue"

type FollowingQuarantineMessage struct {
	// JSON encodes bytes as base64 so even malformed JSON and binary payloads
	// can be recovered without changing any byte.
	OriginalBody      []byte    `json:"original_body"`
	OriginalMessageID string    `json:"original_message_id"`
	SourceQueue       string    `json:"source_queue"`
	ErrorClass        string    `json:"error_class"`
	Reason            string    `json:"reason"`
	QuarantinedAt     time.Time `json:"quarantined_at"`
}

type FollowingQuarantineMQ struct{ publisher *confirmedPublisher }

func NewFollowingQuarantineMQ(cfg config.RabbitMQConfig) *FollowingQuarantineMQ {
	return &FollowingQuarantineMQ{publisher: newConfirmedPublisher(cfg, func(ch *amqp.Channel) error {
		_, err := ch.QueueDeclare(FollowingQuarantineQueue, true, false, false, false, nil)
		return err
	})}
}

func (m *FollowingQuarantineMQ) Close() { m.publisher.close() }

func (m *FollowingQuarantineMQ) Publish(ctx context.Context, delivery amqp.Delivery, class string, cause error) error {
	p := m.publisher
	select {
	case p.gate <- struct{}{}:
		defer func() { <-p.gate }()
	default:
		return &PublishError{Code: "publisher_busy"}
	}
	if p.closed {
		return &PublishError{Code: "publisher_unavailable"}
	}

	id := delivery.MessageId
	if id == "" {
		id = fmt.Sprintf("missing-id-sha256-%x", sha256.Sum256(delivery.Body))
	}
	now := time.Now().UTC()
	body, err := json.Marshal(FollowingQuarantineMessage{
		OriginalBody: delivery.Body, OriginalMessageID: delivery.MessageId,
		SourceQueue: FollowingFanoutQueue, ErrorClass: class, Reason: cause.Error(), QuarantinedAt: now,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return p.publishConfirmed(ctx, "", FollowingQuarantineQueue, id, body, now)
}
