package rabbitmq

import (
	"context"
	"feedsystem_video_go/internal/config"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishError distinguishes definite rejection from an uncertain delivery.
// Unknown must be retried with the original, immutable command.
type PublishError struct {
	Code    string
	Unknown bool
}

func (e *PublishError) Error() string { return e.Code }

// confirmedPublisher owns the command publisher connection and confirm channel.
// Its single-publication gate also covers reconnect and topology initialization.
type confirmedPublisher struct {
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
	declareTopology func(*amqp.Channel) error
}

func newConfirmedPublisher(cfg config.RabbitMQConfig, declareTopology func(*amqp.Channel) error) *confirmedPublisher {
	if cfg.LikePublishTimeoutMS <= 0 {
		cfg.LikePublishTimeoutMS = 3000
	}
	return &confirmedPublisher{cfg: cfg, gate: make(chan struct{}, 1), declareTopology: declareTopology}
}

func (l *confirmedPublisher) disconnect() {
	// Close the socket directly: amqp.Close can wait for a protocol handshake.
	if l.wire != nil {
		_ = l.wire.Close()
	}
	l.wire, l.conn, l.ch = nil, nil, nil
}

func (l *confirmedPublisher) close() {
	l.gate <- struct{}{}
	defer func() { <-l.gate }()
	l.closed = true
	l.disconnect()
}

func (l *confirmedPublisher) connect(ctx context.Context) error {
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
	err = l.declareTopology(ch)
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

// publishConfirmed runs under the caller's single-publication gate and timeout.
func (l *confirmedPublisher) publishConfirmed(ctx context.Context, exchange, routingKey, messageID string, body []byte, timestamp time.Time) error {
	if err := ctx.Err(); err != nil {
		return &PublishError{Code: "publish_cancelled"}
	}
	if err := l.connect(ctx); err != nil {
		return &PublishError{Code: "publisher_unavailable"}
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
	if err := l.ch.PublishWithContext(ctx, exchange, routingKey, true, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: messageID, Timestamp: timestamp, Body: body,
	}); err != nil {
		l.disconnect()
		return &PublishError{Code: "publish_unknown", Unknown: true}
	}
	err := waitConfirm(ctx, sequence, messageID, l.confirms, l.returns, l.channelClose, l.connectionClose)
	if err != nil {
		l.disconnect()
		return err
	}
	// Do not report success if cancellation or connection failure raced the ACK.
	if ctx.Err() != nil || l.conn.IsClosed() || l.ch.IsClosed() {
		l.disconnect()
		return &PublishError{Code: "connection_lost", Unknown: true}
	}
	_ = wire.SetDeadline(time.Time{})

	return nil
}

func waitConfirm(ctx context.Context, sequence uint64, id string, confirms <-chan amqp.Confirmation, returns <-chan amqp.Return, channelClose, connectionClose <-chan *amqp.Error) (result error) {
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
