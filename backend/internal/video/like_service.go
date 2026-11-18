package video

import "context"

type LikeService struct {
	repo      *LikeRepository
	VideoRepo *VideoRepository
}

func NewLikeService(repo *LikeRepository, videoRepo *VideoRepository) *LikeService {
	return &LikeService{repo: repo, VideoRepo: videoRepo}
}

func (s *LikeService) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	return s.repo.IsLiked(ctx, videoID, accountID)
}
func (s *LikeService) ListLikedVideos(ctx context.Context, accountID uint) ([]Video, error) {
	return s.repo.ListLikedVideos(ctx, accountID)
}

// AcceptLikeCommand returns only after the command transaction commits.
func (s *LikeService) AcceptLikeCommand(ctx context.Context, accountID uint, req LikeCommandRequest) (*CommandOutbox, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.VideoRepo != nil {
		ok, err := s.VideoRepo.IsExist(ctx, req.VideoID)
		if err != nil {
			return nil, err
		}
		if !ok {
			// A deleted/missing video must not hide an already committed request
			// identity. Let A1 return its original result or payload conflict.
			if _, err := s.repo.FindLikeCommand(ctx, accountID, req.RequestID); err != nil {
				return nil, err
			}
		}
	}
	return s.repo.AcceptLikeCommand(ctx, accountID, req)
}
