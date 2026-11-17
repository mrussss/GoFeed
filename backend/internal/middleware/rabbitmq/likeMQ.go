package rabbitmq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/config"
	"net"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	likeExchange   = "like.events"
	likeQueue      = "like.events"
	likeBindingKey = "like.*"
	likeLikeRK     = "like.like"
	likeUnlikeRK   = "like.unlike"
	likeReceiptTTL = 10 * time.Minute
)

// PublishError distinguishes definite rejection from an uncertain delivery.
// Unknown must be retried with the original, immutable event.
type PublishError struct {
	Code    string
	Unknown bool
}

func (e *PublishError) Error() string { return e.Code }

type LikeEvent struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	UserID        uint      `json:"user_id"`
	VideoID       uint      `json:"video_id"`
	Revision      string    `json:"revision"`
	DesiredLiked  bool      `json:"desired_liked"`
	OccurredAt    time.Time `json:"occurred_at"`
}

func (e LikeEvent) Validate() error {
	id, err := hex.DecodeString(e.EventID)
	if err != nil || len(id) != 16 || hex.EncodeToString(id) != e.EventID {
		return errors.New("event_id must be 32 lowercase hex characters")
	}
	rev, err := strconv.ParseUint(e.Revision, 10, 64)
	if err != nil || rev == 0 || strconv.FormatUint(rev, 10) != e.Revision {
		return errors.New("revision must be a positive canonical uint64 string")
	}
	if e.SchemaVersion != 2 || e.UserID == 0 || e.VideoID == 0 || e.OccurredAt.IsZero() {
		return errors.New("invalid like event")
	}
	return nil
}

// LikeMQ owns a dedicated connection. One non-waiting gate bounds in-flight
// publications to one, including reconnect/topology setup and confirm waiting.
// No shared publisher (comments, follows, timeline) is changed by this component.
type LikeMQ struct {
	cfg             config.RabbitMQConfig
	gate            chan struct{}
	conn            *amqp.Connection
	wire            net.Conn
	ch              *amqp.Channel
	confirms        chan amqp.Confirmation
	returns         chan amqp.Return
	channelClose    chan *amqp.Error
	connectionClose chan *amqp.Error
	closed          bool
	// Short-lived, bounded receipts only. Durable cross-process deduplication
	// and conflict detection belong to the Phase 2 Inbox.
	payloads map[string]likePublication
}

type likePublication struct {
	digest    [32]byte
	expires   time.Time
	confirmed bool
}

func NewLikeMQ(cfg config.RabbitMQConfig) *LikeMQ {
	if cfg.LikePublishTimeoutMS <= 0 {
		cfg.LikePublishTimeoutMS = 3000
	}
	if cfg.LikeEventLimit <= 0 {
		cfg.LikeEventLimit = 100000
	}
	return &LikeMQ{cfg: cfg, gate: make(chan struct{}, 1), payloads: make(map[string]likePublication)}
}

func (l *LikeMQ) disconnect() {
	// Close the socket directly: amqp.Close can wait for a protocol handshake.
	if l.wire != nil {
		_ = l.wire.Close()
	}
	l.wire, l.conn, l.ch = nil, nil, nil
}

func (l *LikeMQ) Close() {
	l.gate <- struct{}{}
	defer func() { <-l.gate }()
	l.closed = true
	l.disconnect()
}

func (l *LikeMQ) connect(ctx context.Context) error {
	if l.conn != nil && !l.conn.IsClosed() && l.ch != nil && !l.ch.IsClosed() {
		return nil
	}
	l.disconnect()
	conn, err := amqp.DialConfig(ConnectionURL(l.cfg), amqp.Config{Dial: func(network, address string) (net.Conn, error) {
		wire, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err == nil {
			l.wire = wire
			deadline, _ := ctx.Deadline()
			_ = wire.SetDeadline(deadline)
		}
		return wire, err
	}})
	if err != nil {
		l.disconnect()
		return err
	}
	l.conn = conn
	l.connectionClose = conn.NotifyClose(make(chan *amqp.Error, 1))
	ch, err := conn.Channel()
	if err != nil {
		l.disconnect()
		return err
	}
	l.ch = ch
	// Establish both consumers' durable destinations before accepting any likes.
	if err = DeclareTopic(ch, likeExchange, likeQueue, likeBindingKey); err == nil {
		err = DeclareTopic(ch, likeExchange, "notification.like", likeLikeRK)
	}
	if err == nil {
		err = ch.Confirm(false)
	}
	if err != nil {
		l.disconnect()
		return err
	}
	l.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	l.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	l.channelClose = ch.NotifyClose(make(chan *amqp.Error, 1))
	return nil
}

// Publish reports whether this call newly confirmed the event. A matching
// cached receipt returns false, nil: the previous broker ACK remains valid,
// and callers must not publish the same transitional heat effect again.
func (l *LikeMQ) Publish(ctx context.Context, evt LikeEvent) (bool, error) {
	if err := evt.Validate(); err != nil {
		return false, err
	}
	if l == nil {
		return false, &PublishError{Code: "publisher_unavailable"}
	}
	select {
	case l.gate <- struct{}{}:
		defer func() { <-l.gate }()
	default:
		return false, &PublishError{Code: "publisher_busy"}
	}
	if l.closed {
		return false, &PublishError{Code: "publisher_unavailable"}
	}
	body, err := json.Marshal(evt)
	if err != nil {
		return false, err
	}
	digest := sha256.Sum256(body)
	now := time.Now()
	if prior, ok := l.payloads[evt.EventID]; ok && now.Before(prior.expires) {
		if prior.digest != digest {
			return false, &PublishError{Code: "event_payload_conflict"}
		}
		if prior.confirmed {
			return false, nil
		}
	} else {
		// No LRU or background cache: drop expired receipts when at capacity, then
		// evict the oldest if still full. The limit must never permanently stop likes.
		if len(l.payloads) >= l.cfg.LikeEventLimit {
			var oldestID string
			var oldestExpiry time.Time
			for id, record := range l.payloads {
				if !now.Before(record.expires) {
					delete(l.payloads, id)
					continue
				}
				if oldestID == "" || record.expires.Before(oldestExpiry) {
					oldestID, oldestExpiry = id, record.expires
				}
			}
			if len(l.payloads) >= l.cfg.LikeEventLimit {
				delete(l.payloads, oldestID)
			}
		}
		l.payloads[evt.EventID] = likePublication{digest: digest, expires: now.Add(likeReceiptTTL)}
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(l.cfg.LikePublishTimeoutMS)*time.Millisecond)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return false, &PublishError{Code: "publish_cancelled"}
	}
	if err := l.connect(ctx); err != nil {
		return false, &PublishError{Code: "publisher_unavailable"}
	}
	deadline, _ := ctx.Deadline()
	_ = l.wire.SetDeadline(deadline)
	// PublishWithContext in amqp091-go v1.10.0 ignores its context. Closing the
	// captured socket interrupts blocked writes as well as confirmation waits.
	wire := l.wire
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			_ = wire.Close()
		case <-stop:
		}
	}()
	defer func() { close(stop); <-stopped }()
	sequence := l.ch.GetNextPublishSeqNo()
	routingKey := likeUnlikeRK
	if evt.DesiredLiked {
		routingKey = likeLikeRK
	}
	if err := l.ch.PublishWithContext(ctx, likeExchange, routingKey, true, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: evt.EventID, Timestamp: evt.OccurredAt, Body: body,
	}); err != nil {
		l.disconnect()
		return false, &PublishError{Code: "publish_unknown", Unknown: true}
	}
	err = waitLikeConfirm(ctx, sequence, evt.EventID, l.confirms, l.returns, l.channelClose, l.connectionClose)
	if err != nil {
		l.disconnect()
		return false, err
	}
	// Do not report success if cancellation or connection failure raced the ACK.
	if ctx.Err() != nil || l.conn.IsClosed() || l.ch.IsClosed() {
		l.disconnect()
		return false, &PublishError{Code: "connection_lost", Unknown: true}
	}
	_ = wire.SetDeadline(time.Time{})
	l.payloads[evt.EventID] = likePublication{digest: digest, expires: time.Now().Add(likeReceiptTTL), confirmed: true}
	return true, nil
}

func waitLikeConfirm(ctx context.Context, sequence uint64, id string, confirms <-chan amqp.Confirmation, returns <-chan amqp.Return, channelClose, connectionClose <-chan *amqp.Error) (result error) {
	defer func() {
		if result != nil && ctx.Err() != nil {
			result = &PublishError{Code: "confirm_timeout", Unknown: true}
		}
	}()
	var returned bool
	checkReturn := func(r amqp.Return) error {
		if r.MessageId != id {
			return &PublishError{Code: "publish_protocol_error", Unknown: true}
		}
		returned = true
		return nil
	}
	for {
		select {
		case r, ok := <-returns:
			if !ok {
				return &PublishError{Code: "connection_lost", Unknown: true}
			}
			if err := checkReturn(r); err != nil {
				return err
			}
		case c, ok := <-confirms:
			if !ok {
				return &PublishError{Code: "connection_lost", Unknown: true}
			}
			if c.DeliveryTag != sequence {
				return &PublishError{Code: "publish_protocol_error", Unknown: true}
			}
			// The library dispatches Return before Confirm. Drain its buffered Return
			// even if select chose the ACK first; otherwise unroutable can look accepted.
			select {
			case r, ok := <-returns:
				if !ok {
					return &PublishError{Code: "connection_lost", Unknown: true}
				}
				if err := checkReturn(r); err != nil {
					return err
				}
			default:
			}
			if returned {
				return &PublishError{Code: "unroutable"}
			}
			if !c.Ack {
				return &PublishError{Code: "broker_nack"}
			}
			select {
			case <-channelClose:
				return &PublishError{Code: "connection_lost", Unknown: true}
			case <-connectionClose:
				return &PublishError{Code: "connection_lost", Unknown: true}
			default:
			}
			return nil
		case <-channelClose:
			return &PublishError{Code: "connection_lost", Unknown: true}
		case <-connectionClose:
			return &PublishError{Code: "connection_lost", Unknown: true}
		case <-ctx.Done():
			return &PublishError{Code: "confirm_timeout", Unknown: true}
		}
	}
}
