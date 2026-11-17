package video

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"fmt"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ingressEvent() rabbitmq.LikeEvent {
	return rabbitmq.LikeEvent{SchemaVersion: 2, EventID: "00112233445566778899aabbccddeeff", UserID: 7, VideoID: 9, Revision: "12", DesiredLiked: true, OccurredAt: time.Date(2026, 11, 16, 1, 2, 3, 0, time.UTC)}
}

// The former publisher-based HTTP acceptance tests now check the command contract.
// Successful durable acceptance is exercised with real MySQL in the C suite.
func TestLikeIngressAcceptance(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set("accountID", uint(7)) })
	h := NewLikeHandler(NewLikeService(nil, nil))
	r.POST("/like/like", h.Like)
	for _, body := range []string{
		`{"user_id":999}`, `{"account_id":999}`, `{"video_id":1,"request_id":"x","revision":"1","desired_liked":true}`,
		`{"video_id":1,"request_id":"x","desired_liked":false}`, `{"video_id":1,"request_id":"x"}`,
		`{"video_id":1,"request_id":"","desired_liked":true}`, `{"video_id":0,"request_id":"x","desired_liked":true}`,
		`{"video_id":1,"request_id":"x","desired_liked":true} {}`,
	} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("POST", "/like/like", strings.NewReader(body)))
		if rec.Code != 400 {
			t.Fatalf("invalid command returned %d: %s", rec.Code, body)
		}
	}
}

// A cancelled call performs no network I/O. A tiny receipt limit reproduces
// the former lifetime cap without requiring 100000 events or a live broker.
func TestLikeIngressReceiptCapacity(t *testing.T) {
	p := rabbitmq.NewLikeMQ(config.RabbitMQConfig{LikeEventLimit: 2})
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 8; i++ {
		evt := ingressEvent()
		evt.EventID = fmt.Sprintf("%032x", i)
		_, err := p.Publish(ctx, evt)
		var publishErr *rabbitmq.PublishError
		if !errors.As(err, &publishErr) || publishErr.Code != "publish_cancelled" {
			t.Fatalf("event %d blocked by receipt capacity: %v", i, err)
		}
	}
}

// Run only against a disposable vhost. Never publishes to the application's vhost.
func TestLikeIngressRealRabbitMQ(t *testing.T) {
	vhost := os.Getenv("LIKE_TEST_VHOST")
	if !strings.HasPrefix(vhost, "gofeed-phase1-check-") {
		t.Skip("LIKE_TEST_VHOST: isolated RabbitMQ verification not requested")
	}
	port, _ := strconv.Atoi(os.Getenv("LIKE_TEST_PORT"))
	if port == 0 {
		port = 5672
	}
	cfg := config.RabbitMQConfig{Host: "127.0.0.1", Port: port, Username: os.Getenv("LIKE_TEST_USER"), Password: os.Getenv("LIKE_TEST_PASSWORD"), VHost: vhost, LikePublishTimeoutMS: 1500}
	conn, err := amqp.Dial(rabbitmq.ConnectionURL(cfg))
	if err != nil {
		t.Fatal("isolated broker unavailable:", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	publisher := rabbitmq.NewLikeMQ(cfg)
	defer publisher.Close()
	evt := ingressEvent()
	if _, err := publisher.Publish(context.Background(), evt); err != nil {
		t.Fatal(err)
	}
	d, ok, err := ch.Get("like.events", true)
	if err != nil || !ok || d.MessageId != evt.EventID || d.DeliveryMode != amqp.Persistent {
		t.Fatalf("persistent envelope: %+v %v", d, err)
	}
	var got rabbitmq.LikeEvent
	if err = json.Unmarshal(d.Body, &got); err != nil || got != evt {
		t.Fatalf("event changed: %+v %v", got, err)
	}
	for i := 0; i < 2; i++ {
		if newly, err := publisher.Publish(context.Background(), evt); err != nil || newly {
			t.Fatalf("confirmed retry: %v %v", newly, err)
		}
	}
	if _, ok, err := ch.Get("like.events", true); err != nil || ok {
		t.Fatalf("retry duplicated event: %v %v", ok, err)
	}
	// Confirms also ACK unroutable messages. Remove the only unlike binding and
	// verify that a real basic.return + ACK cannot become a confirmed publish.
	if err = ch.QueueUnbind("like.events", "like.*", "like.events", nil); err != nil {
		t.Fatal(err)
	}
	evt.EventID = "11112233445566778899aabbccddeeff"
	evt.DesiredLiked = false
	if _, err := publisher.Publish(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "unroutable") {
		t.Fatalf("real return: %v", err)
	}
	if _, err := publisher.Publish(context.Background(), evt); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	evt.DesiredLiked = true
	if _, err := publisher.Publish(context.Background(), evt); err == nil {
		t.Fatal("expected payload conflict")
	}
	if err = ch.ExchangeDelete("like.events", false, false); err != nil {
		t.Fatal(err)
	}
	evt.EventID = "22222233445566778899aabbccddeeff"
	_, err = publisher.Publish(context.Background(), evt)
	var publishErr *rabbitmq.PublishError
	if !errors.As(err, &publishErr) || !publishErr.Unknown {
		t.Fatalf("channel close: %v", err)
	}
	if _, err := publisher.Publish(context.Background(), evt); err != nil {
		t.Fatalf("channel recovery: %v", err)
	}
	t.Log("legacy transport ACK, persistent envelope, Return, channel recovery and payload conflict remain covered; no legacy HTTP write path")
}
