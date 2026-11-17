package video

import (
	"context"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"log"

	"gorm.io/gorm"
)

type LikePublisher interface {
	Publish(context.Context, rabbitmq.LikeEvent) (bool, error)
}

type LikeService struct {
	repo         *LikeRepository
	VideoRepo    *VideoRepository
	likeMQ       LikePublisher
	popularityMQ *rabbitmq.PopularityMQ
}

func NewLikeService(repo *LikeRepository, videoRepo *VideoRepository, likeMQ LikePublisher, popularityMQ *rabbitmq.PopularityMQ) *LikeService {
	return &LikeService{repo: repo, VideoRepo: videoRepo, likeMQ: likeMQ, popularityMQ: popularityMQ}
}

// Accept only publishes intent. It never checks the existing like relationship
// or writes relationships/counts, even when RabbitMQ is unavailable.
func (s *LikeService) Accept(ctx context.Context, evt rabbitmq.LikeEvent) error {
	if err := evt.Validate(); err != nil {
		return err
	}
	if s.VideoRepo != nil {
		ok, err := s.VideoRepo.IsExist(ctx, evt.VideoID)
		if err != nil {
			return err
		}
		if !ok {
			return gorm.ErrRecordNotFound
		}
	}
	if s.likeMQ == nil {
		return &rabbitmq.PublishError{Code: "publisher_unavailable"}
	}
	newlyConfirmed, err := s.likeMQ.Publish(ctx, evt)
	if err != nil {
		return err
	}
	// Reuse the broker receipt without repeating the transitional heat publish.
	if !newlyConfirmed {
		return nil
	}

	// Transitional Phase 1 heat path. The accepted like cannot be revoked if this
	// independent publish fails. No direct Redis/MySQL fallback is permitted.
	if s.popularityMQ != nil {
		change := int64(-1)
		if evt.DesiredLiked {
			change = 1
		}
		if err := s.popularityMQ.Update(ctx, evt.VideoID, change); err != nil {
			log.Printf("like heat publish failed: event_id=%s (Phase 2 will coordinate committed effects)", evt.EventID)
		}
	}
	return nil
}

func (s *LikeService) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	return s.repo.IsLiked(ctx, videoID, accountID)
}
func (s *LikeService) ListLikedVideos(ctx context.Context, accountID uint) ([]Video, error) {
	return s.repo.ListLikedVideos(ctx, accountID)
}

// AcceptLikeCommand is the independent A1 entry point. Accept continues to
// use the Phase 1 publisher until A2 explicitly switches the HTTP contract.
func (s *LikeService) AcceptLikeCommand(ctx context.Context, accountID uint, req LikeCommandRequest) (*CommandOutbox, error) {
	if s.VideoRepo != nil {
		ok, err := s.VideoRepo.IsExist(ctx, req.VideoID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, gorm.ErrRecordNotFound
		}
	}
	return s.repo.AcceptLikeCommand(ctx, accountID, req)
}
