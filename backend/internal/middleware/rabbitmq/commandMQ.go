package rabbitmq

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/config"
	"hash/fnv"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	LikeCommandsExchange  = "like.commands"
	LikeCommandPartitions = 4
)

// LikeCommandEvent is separate from Phase 1 LikeEvent. All retry-identifying
// fields come from the committed outbox; no fresh EventID or revision is generated.
type LikeCommandEvent struct {
	CommandID    uint64 `json:"command_id,string"`
	RequestID    string `json:"request_id"`
	AccountID    uint   `json:"account_id"`
	VideoID      uint   `json:"video_id"`
	DesiredLiked bool   `json:"desired_liked"`
	Revision     uint64 `json:"revision,string"`
}

func (event LikeCommandEvent) Validate() error {
	if event.CommandID == 0 || event.RequestID == "" || event.AccountID == 0 || event.VideoID == 0 || event.Revision == 0 {
		return errors.New("invalid like command event")
	}
	return nil
}

// LikeCommandPartition is the fixed routing contract for the later consumer:
// FNV-1a 64 over two big-endian uint64 values (account, video), modulo four.
func LikeCommandPartition(accountID, videoID uint) int {
	var key [16]byte
	binary.BigEndian.PutUint64(key[:8], uint64(accountID))
	binary.BigEndian.PutUint64(key[8:], uint64(videoID))
	h := fnv.New64a()
	_, _ = h.Write(key[:])
	return int(h.Sum64() % LikeCommandPartitions)
}

func LikeCommandQueue(partition int) string {
	return LikeCommandsExchange + "." + strconv.Itoa(partition)
}

func DeclareLikeCommands(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(LikeCommandsExchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	for i := 0; i < LikeCommandPartitions; i++ {
		q, err := ch.QueueDeclare(LikeCommandQueue(i), true, false, false, false, nil)
		if err != nil {
			return err
		}
		if err := ch.QueueBind(q.Name, strconv.Itoa(i), LikeCommandsExchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// LikeCommandMQ owns one connection/channel and allows one in-flight confirm.
// Unlike Phase 1's API publisher, each call actually publishes: if MySQL marking
// failed, Relay must be allowed to redeliver the same stable command identity.
type LikeCommandMQ struct{ publisher *LikeMQ }

func NewLikeCommandMQ(cfg config.RabbitMQConfig) *LikeCommandMQ {
	publisher := NewLikeMQ(cfg)
	publisher.declareTopology = DeclareLikeCommands
	return &LikeCommandMQ{publisher: publisher}
}

func (m *LikeCommandMQ) Close() { m.publisher.Close() }

func (m *LikeCommandMQ) Publish(ctx context.Context, event LikeCommandEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
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
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(p.cfg.LikePublishTimeoutMS)*time.Millisecond)
	defer cancel()
	return p.publishConfirmed(ctx, LikeCommandsExchange, strconv.Itoa(LikeCommandPartition(event.AccountID, event.VideoID)), strconv.FormatUint(event.CommandID, 10), body, time.Now().UTC())
}
