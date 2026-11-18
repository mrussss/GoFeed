package video

import (
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/apierror"
	"feedsystem_video_go/internal/middleware/jwt"
	"gorm.io/gorm"
	"io"
	"net/http"
	"strconv"

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
	RequestID    string `json:"request_id"`
	VideoID      uint   `json:"video_id"`
	DesiredLiked *bool  `json:"desired_liked"`
}

func (lh *LikeHandler) Like(c *gin.Context)   { lh.accept(c, true) }
func (lh *LikeHandler) Unlike(c *gin.Context) { lh.accept(c, false) }

func (lh *LikeHandler) accept(c *gin.Context, desired bool) {
	var req LikeIntentRequest
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields() // account_id and client revisions are not accepted.
	if err := decoder.Decode(&req); err != nil {
		c.JSON(400, gin.H{"error": "invalid like request"})
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		c.JSON(400, gin.H{"error": "expected one JSON object"})
		return
	}
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(401, gin.H{"error": "authentication required"})
		return
	}
	if req.DesiredLiked == nil || *req.DesiredLiked != desired {
		c.JSON(400, gin.H{"error": "desired_liked must match endpoint"})
		return
	}
	command, err := lh.service.AcceptLikeCommand(c.Request.Context(), accountID, LikeCommandRequest{
		RequestID: req.RequestID, VideoID: req.VideoID, DesiredLiked: desired,
	})
	if err != nil {
		status, message := 500, "command acceptance failed; retry with the same request_id"
		switch {
		case errors.Is(err, ErrInvalidLikeCommand):
			status, message = 400, err.Error()
		case errors.Is(err, ErrLikeCommandConflict):
			status, message = 409, err.Error()
		case errors.Is(err, gorm.ErrRecordNotFound):
			status, message = 404, "video not found"
		}
		c.JSON(status, gin.H{"error": message, "request_id": req.RequestID})
		return
	}
	// Accepted means durable intent, not an already applied like.
	c.JSON(202, gin.H{"status": "accepted", "request_id": command.RequestID,
		"command_id": strconv.FormatUint(command.ID, 10), "revision": strconv.FormatUint(command.Revision, 10)})
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
