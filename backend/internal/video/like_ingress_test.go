package video

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// Successful durable acceptance is covered by the real MySQL pipeline test.
func TestLikeCommandParameters(t *testing.T) {
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
