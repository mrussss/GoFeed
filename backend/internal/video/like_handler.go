package video

import (
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/apierror"
	"feedsystem_video_go/internal/middleware/jwt"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type LikeHandler struct {
	service *LikeService
}

func NewLikeHandler(service *LikeService) *LikeHandler {
	return &LikeHandler{service: service}
}

// Write requests deliberately differ from the read-only LikeRequest.
type LikeIntentRequest struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	VideoID       uint      `json:"video_id"`
	Revision      string    `json:"revision"`
	DesiredLiked  *bool     `json:"desired_liked"`
	OccurredAt    time.Time `json:"occurred_at"`
}

func (lh *LikeHandler) Like(c *gin.Context)   { lh.accept(c, true) }
func (lh *LikeHandler) Unlike(c *gin.Context) { lh.accept(c, false) }

func (lh *LikeHandler) accept(c *gin.Context, desired bool) {
	var req LikeIntentRequest
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields() // in particular, user_id cannot be supplied
	if err := decoder.Decode(&req); err != nil {
		c.JSON(400, gin.H{"status": "rejected", "error": "invalid like request"})
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		c.JSON(400, gin.H{"status": "rejected", "error": "expected one JSON object"})
		return
	}
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"status": "rejected", "error": err.Error()})
		return
	}
	evt := rabbitmq.LikeEvent{SchemaVersion: req.SchemaVersion, EventID: req.EventID, UserID: accountID, VideoID: req.VideoID,
		Revision: req.Revision, DesiredLiked: desired, OccurredAt: req.OccurredAt}
	if req.DesiredLiked == nil || *req.DesiredLiked != desired {
		c.JSON(400, gin.H{"status": "rejected", "error": "desired_liked must match endpoint"})
		return
	}
	if err := evt.Validate(); err != nil {
		c.JSON(400, gin.H{"status": "rejected", "error": err.Error()})
		return
	}
	if err := lh.service.Accept(c.Request.Context(), evt); err != nil {
		code, status, result := "request_failed", apierror.ClassifyHTTPStatus(err), "rejected"
		var publishErr *rabbitmq.PublishError
		if errors.As(err, &publishErr) {
			code, status = publishErr.Code, 503
			if publishErr.Code == "event_payload_conflict" {
				status = 409
			}
			if publishErr.Unknown {
				result = "unknown"
			}
			if publishErr.Code == "confirm_timeout" {
				status = 504
			}
		}
		c.JSON(status, gin.H{"status": result, "code": code, "error": code, "event_id": evt.EventID, "revision": evt.Revision})
		return
	}
	c.JSON(202, gin.H{"status": "accepted", "event_id": evt.EventID, "revision": evt.Revision})
}

func (lh *LikeHandler) IsLiked(c *gin.Context) {
	var req LikeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if req.VideoID <= 0 {
		c.JSON(400, gin.H{"error": "video_id is required"})
		return
	}

	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	isLiked, err := lh.service.IsLiked(c.Request.Context(), req.VideoID, accountID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"is_liked": isLiked})
}

func (lh *LikeHandler) ListMyLikedVideos(c *gin.Context) {
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}

	videos, err := lh.service.ListLikedVideos(c.Request.Context(), accountID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if videos == nil {
		videos = []Video{}
	}
	c.JSON(200, videos)
}
