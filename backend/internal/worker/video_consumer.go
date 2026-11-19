package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	amqp "github.com/rabbitmq/amqp091-go"
	oredis "github.com/redis/go-redis/v9"
)

var errInvalidVideoEvent = errors.New("invalid video event")

type quarantineVideoDelivery func(context.Context, amqp.Delivery, string, error) error

func RunGlobalTimelineConsumer(ctx context.Context, cfg config.RabbitMQConfig, cache *rediscache.Client) {
	runVideoConsumer(ctx, cfg, rabbitmq.GlobalTimelineQueue, func(ctx context.Context, event rabbitmq.TimelineEvent) error {
		keys := []string{cache.Key("feed:global_timeline")}
		member := oredis.Z{Score: float64(event.CreateTime), Member: strconv.FormatUint(uint64(event.VideoID), 10)}
		return cache.AddBoundedZSets(ctx, keys, member, 1000)
	}, nil)
}

// One independent connection per queue isolates consumer errors and lets each
// loop recreate its channel after a Redis error or broker disconnect.
func runVideoConsumer(ctx context.Context, cfg config.RabbitMQConfig, queue string, handle func(context.Context, rabbitmq.TimelineEvent) error, quarantine quarantineVideoDelivery) {
	for ctx.Err() == nil {
		err := consumeVideoEvents(ctx, cfg, queue, handle, quarantine)
		if ctx.Err() == nil {
			log.Printf("video consumer %s: %v; retry in 1s", queue, err)
		}
		if !waitVideoRetry(ctx) {
			return
		}
	}
}

func consumeVideoEvents(ctx context.Context, cfg config.RabbitMQConfig, queue string, handle func(context.Context, rabbitmq.TimelineEvent) error, quarantine quarantineVideoDelivery) error {
	conn, err := amqp.DialConfig(rabbitmq.ConnectionURL(cfg), amqp.Config{Dial: amqp.DefaultDial(3 * time.Second)})
	if err != nil {
		return err
	}
	defer func() { _ = conn.CloseDeadline(time.Now().Add(time.Second)) }()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := rabbitmq.DeclareTimelineQueue(ch, queue); err != nil {
		return err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			var event rabbitmq.TimelineEvent
			var err error
			if quarantine != nil {
				event, err = decodeFollowingEvent(delivery.Body)
			} else {
				err = json.Unmarshal(delivery.Body, &event)
				if err != nil || event.VideoID == 0 {
					err = fmt.Errorf("%w: invalid JSON or missing video ID", errInvalidVideoEvent)
				}
			}
			if err == nil {
				err = handle(ctx, event)
			}
			if quarantine != nil {
				if class := followingFailureClass(err); class != "" {
					if publishErr := quarantine(ctx, delivery, class, err); publishErr != nil {
						return fmt.Errorf("quarantine message %q (%s): %w", delivery.MessageId, class, publishErr)
					}
					log.Printf("video consumer %s: quarantined message %q (%s): %v", queue, delivery.MessageId, class, err)
					// Only a confirmed transfer may settle the original delivery.
					err = nil
				}
			}
			switch {
			case quarantine == nil && errors.Is(err, errInvalidVideoEvent):
				log.Printf("video consumer %s: reject message %q: %v", queue, delivery.MessageId, err)
				if err := delivery.Reject(false); err != nil {
					return err
				}
			case err != nil:
				// Closing this connection returns the unacknowledged delivery.
				return fmt.Errorf("event %q: %w", event.EventID, err)
			default:
				if err := delivery.Ack(false); err != nil {
					return err
				}
			}
		}
	}
}

func waitVideoRetry(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
