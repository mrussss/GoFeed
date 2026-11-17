package video

import (
	"context"
	"encoding/json"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ingressPublisher func(context.Context, rabbitmq.LikeEvent) error

func (f ingressPublisher) Publish(ctx context.Context, e rabbitmq.LikeEvent) error { return f(ctx, e) }

func ingressEvent() rabbitmq.LikeEvent {
	return rabbitmq.LikeEvent{SchemaVersion: 2, EventID: "00112233445566778899aabbccddeeff", UserID: 7, VideoID: 9, Revision: "12", DesiredLiked: true, OccurredAt: time.Date(2026, 11, 16, 1, 2, 3, 0, time.UTC)}
}
func ingressRouter(p LikePublisher) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set("accountID", uint(7)) })
	h := NewLikeHandler(NewLikeService(nil, nil, p, nil)) // any fallback/read would fail with these nil repositories
	r.POST("/like/like", h.Like)
	r.POST("/like/unlike", h.Unlike)
	return r
}
func ingressRequest(t *testing.T, r *gin.Engine, e rabbitmq.LikeEvent) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(LikeIntentRequest{SchemaVersion: e.SchemaVersion, EventID: e.EventID, VideoID: e.VideoID, Revision: e.Revision, DesiredLiked: &e.DesiredLiked, OccurredAt: e.OccurredAt})
	path := "/like/like"
	if !e.DesiredLiked {
		path = "/like/unlike"
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", path, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rec, req)
	return rec
}

// Existing tests cover uploads/rate limiting, not the asynchronous acceptance
// contract. Keep API failure gates and the optional real broker check together.
func TestLikeIngressAcceptance(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		code    string
		unknown bool
		http    int
		status  string
	}{
		{"", false, 202, "accepted"}, {"broker_nack", false, 503, "rejected"},
		{"unroutable", false, 503, "rejected"}, {"confirm_timeout", true, 504, "unknown"},
		{"connection_lost", true, 503, "unknown"}, {"publisher_busy", false, 503, "rejected"},
		{"event_payload_conflict", false, 409, "rejected"},
	} {
		t.Run(tc.status+"/"+tc.code, func(t *testing.T) {
			r := ingressRouter(ingressPublisher(func(_ context.Context, e rabbitmq.LikeEvent) error {
				if e != ingressEvent() {
					t.Errorf("HTTP envelope changed: %+v", e)
				}
				if tc.code != "" {
					return &rabbitmq.PublishError{Code: tc.code, Unknown: tc.unknown}
				}
				return nil
			}))
			rec := ingressRequest(t, r, ingressEvent())
			var result map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &result)
			if rec.Code != tc.http || result["status"] != tc.status || result["event_id"] != ingressEvent().EventID {
				t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
			}
		})
	}
	t.Run("wait_for_publisher", func(t *testing.T) {
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan *httptest.ResponseRecorder, 1)
		r := ingressRouter(ingressPublisher(func(context.Context, rabbitmq.LikeEvent) error { close(entered); <-release; return nil }))
		go func() { done <- ingressRequest(t, r, ingressEvent()) }()
		<-entered
		select {
		case <-done:
			t.Fatal("returned before confirm")
		default:
		}
		close(release)
		if rec := <-done; rec.Code != 202 {
			t.Fatal(rec.Body.String())
		}
	})
	t.Run("client_user_id_rejected", func(t *testing.T) {
		r := ingressRouter(ingressPublisher(func(context.Context, rabbitmq.LikeEvent) error { t.Error("must not publish"); return nil }))
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("POST", "/like/like", strings.NewReader(`{"user_id":999}`)))
		if rec.Code != 400 {
			t.Fatal(rec.Code)
		}
	})
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
	r := ingressRouter(publisher)
	evt := ingressEvent()
	if rec := ingressRequest(t, r, evt); rec.Code != 202 {
		t.Fatalf("real confirm: %d %s", rec.Code, rec.Body.String())
	}
	d, ok, err := ch.Get("like.events", true)
	if err != nil || !ok || d.MessageId != evt.EventID || d.DeliveryMode != amqp.Persistent {
		t.Fatalf("persistent envelope: %+v %v", d, err)
	}
	var got rabbitmq.LikeEvent
	if err = json.Unmarshal(d.Body, &got); err != nil || got != evt {
		t.Fatalf("event changed: %+v %v", got, err)
	}
	// Confirms also ACK unroutable messages. Remove the only unlike binding and
	// verify that a real basic.return + ACK cannot become HTTP 202.
	if err = ch.QueueUnbind("like.events", "like.*", "like.events", nil); err != nil {
		t.Fatal(err)
	}
	evt.EventID = "11112233445566778899aabbccddeeff"
	evt.DesiredLiked = false
	if rec := ingressRequest(t, r, evt); rec.Code != 503 || !strings.Contains(rec.Body.String(), "unroutable") {
		t.Fatalf("real return: %d %s", rec.Code, rec.Body.String())
	}
	// Failure closed the old connection; retry restores topology + confirms.
	if rec := ingressRequest(t, r, evt); rec.Code != 202 {
		t.Fatalf("reconnect: %d %s", rec.Code, rec.Body.String())
	}
	evt.DesiredLiked = true // same ID, changed payload
	if rec := ingressRequest(t, r, evt); rec.Code != 409 {
		t.Fatalf("conflict: %d %s", rec.Code, rec.Body.String())
	}
	// A broker channel error is uncertain, never accepted. Retry must re-declare.
	if err = ch.ExchangeDelete("like.events", false, false); err != nil {
		t.Fatal(err)
	}
	evt.EventID = "22222233445566778899aabbccddeeff"
	if rec := ingressRequest(t, r, evt); rec.Code == 202 || !strings.Contains(rec.Body.String(), "unknown") {
		t.Fatalf("channel close: %d %s", rec.Code, rec.Body.String())
	}
	if rec := ingressRequest(t, r, evt); rec.Code != 202 {
		t.Fatalf("channel recovery: %d %s", rec.Code, rec.Body.String())
	}
	t.Log("real ACK, persistent MessageId/body, Return+ACK rejection, channel close, reconnect and payload conflict verified in isolated vhost")
}
