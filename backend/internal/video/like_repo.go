package video

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LikeRepository struct {
	db *gorm.DB
}

func NewLikeRepository(db *gorm.DB) *LikeRepository {
	return &LikeRepository{db: db}
}

func (r *LikeRepository) Like(ctx context.Context, like *Like) error {
	return r.db.WithContext(ctx).Create(like).Error
}

func (r *LikeRepository) Unlike(ctx context.Context, like *Like) error {
	return r.db.WithContext(ctx).
		Where("video_id = ? AND account_id = ?", like.VideoID, like.AccountID).
		Delete(&Like{}).Error
}

func (r *LikeRepository) LikeIgnoreDuplicate(ctx context.Context, like *Like) (created bool, err error) {
	if like == nil || like.VideoID == 0 || like.AccountID == 0 {
		return false, nil
	}
	err = r.db.WithContext(ctx).Create(like).Error
	if err == nil {
		return true, nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return false, nil
	}
	return false, err
}

func (r *LikeRepository) DeleteByVideoAndAccount(ctx context.Context, videoID, accountID uint) (deleted bool, err error) {
	if videoID == 0 || accountID == 0 {
		return false, nil
	}
	res := r.db.WithContext(ctx).
		Where("video_id = ? AND account_id = ?", videoID, accountID).
		Delete(&Like{})
	return res.RowsAffected > 0, res.Error
}

func (r *LikeRepository) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&Like{}).
		Where("video_id = ? AND account_id = ? AND status = 1", videoID, accountID).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *LikeRepository) BatchGetLiked(ctx context.Context, videoIDs []uint, accountID uint) (map[uint]bool, error) {
	likeMap := make(map[uint]bool)
	if len(videoIDs) == 0 {
		return likeMap, nil
	}
	if accountID == 0 {
		return likeMap, nil
	}
	var likes []Like
	err := r.db.WithContext(ctx).Model(&Like{}).
		Where("video_id IN ? AND account_id = ? AND status = 1", videoIDs, accountID).
		Find(&likes).Error
	if err != nil {
		return nil, err
	}
	for _, like := range likes {
		likeMap[like.VideoID] = true
	}
	return likeMap, nil
}

func (r *LikeRepository) ListLikedVideos(ctx context.Context, accountID uint) ([]Video, error) {
	var videos []Video
	if accountID == 0 {
		return videos, nil
	}
	err := r.db.WithContext(ctx).
		Model(&Video{}).
		Joins("JOIN likes ON likes.video_id = videos.id").
		Where("likes.account_id = ? AND likes.status = 1", accountID).
		Order("likes.updated_at desc").
		Limit(200).
		Find(&videos).Error
	if err != nil {
		return nil, err
	}
	return videos, nil
}

var (
	ErrLikeCommandConflict   = errors.New("like request_id reused with different payload")
	ErrLikeRevisionExhausted = errors.New("like revision exhausted")
)

// AcceptLikeCommand atomically allocates a server revision and stores intent.
// It does not apply a like, change counts, or publish to RabbitMQ;
// this method must commit before HTTP acceptance is returned.
func (r *LikeRepository) AcceptLikeCommand(ctx context.Context, accountID uint, req LikeCommandRequest) (*CommandOutbox, error) {
	if accountID == 0 || req.VideoID == 0 || strings.TrimSpace(req.RequestID) == "" || len(req.RequestID) > 64 {
		return nil, errors.New("account_id, video_id and request_id (1-64 bytes) are required")
	}
	var command CommandOutbox
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Where("account_id = ? AND request_id = ?", accountID, req.RequestID).Take(&command).Error
		if err == nil {
			if command.VideoID != req.VideoID || command.DesiredLiked != req.DesiredLiked {
				return ErrLikeCommandConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		// A map is intentional: GORM's default:1 would replace a struct's zero
		// Status. Duplicate pairs are left unchanged; the next read takes the lock.
		now := time.Now().UTC()
		if err := tx.Model(&Like{}).Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
			"account_id": accountID, "video_id": req.VideoID, "status": 0,
			"next_revision": 0, "applied_revision": 0, "created_at": now, "updated_at": now,
		}).Error; err != nil {
			return err
		}
		var state Like
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("account_id = ? AND video_id = ?", accountID, req.VideoID).Take(&state).Error; err != nil {
			return err
		}
		if state.NextRevision == ^uint64(0) {
			return ErrLikeRevisionExhausted
		}
		revision := state.NextRevision + 1
		// Version allocation does not apply a state change or reorder liked videos.
		if err := tx.Model(&Like{}).Where("id = ?", state.ID).UpdateColumn("next_revision", revision).Error; err != nil {
			return err
		}
		command = CommandOutbox{AccountID: accountID, RequestID: req.RequestID, VideoID: req.VideoID, DesiredLiked: req.DesiredLiked,
			Revision: revision}
		return tx.Create(&command).Error
	})
	// Transaction has successfully committed before any command is returned.
	if err == nil {
		return &command, nil
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1062 {
		return nil, err
	}

	// Concurrent requests can both miss the initial consistent read. The unique
	// key chooses a winner; Transaction rolled back ALL loser writes before this
	// lookup, which must use a fresh snapshot rather than the old RR transaction.
	var original CommandOutbox
	lookupErr := r.db.WithContext(ctx).Where("account_id = ? AND request_id = ?", accountID, req.RequestID).Take(&original).Error
	if errors.Is(lookupErr, gorm.ErrRecordNotFound) {
		return nil, err // unrelated unique constraint violation
	}
	if lookupErr != nil {
		return nil, lookupErr
	}
	if original.VideoID != req.VideoID || original.DesiredLiked != req.DesiredLiked {
		return nil, ErrLikeCommandConflict
	}
	return &original, nil
}

// ApplyLikeCommandBatch applies latest target states, not individual +/-1 events.
// It never creates/deletes relations or changes the A1 allocation watermark.
func (r *LikeRepository) ApplyLikeCommandBatch(ctx context.Context, events []rabbitmq.LikeCommandEvent) ([]uint, error) {
	type pair struct{ videoID, accountID uint }
	latest := make(map[pair]rabbitmq.LikeCommandEvent, len(events))
	for _, event := range events {
		if err := event.Validate(); err != nil {
			return nil, err
		}
		key := pair{event.VideoID, event.AccountID}
		previous, exists := latest[key]
		if exists && event.Revision == previous.Revision && event.DesiredLiked != previous.DesiredLiked {
			return nil, fmt.Errorf("conflicting targets for video %d account %d revision %d", event.VideoID, event.AccountID, event.Revision)
		}
		if !exists || event.Revision > previous.Revision {
			latest[key] = event
		}
	}
	if len(latest) == 0 {
		return nil, nil
	}
	keys := make([]pair, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].videoID != keys[j].videoID {
			return keys[i].videoID < keys[j].videoID
		}
		return keys[i].accountID < keys[j].accountID
	})
	var changed []uint
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		deltas := make(map[uint]int64)
		now := time.Now().UTC()
		for _, key := range keys {
			var state Like
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("video_id=? AND account_id=?", key.videoID, key.accountID).Take(&state).Error; err != nil {
				return err
			}
			event := latest[key]
			if event.Revision <= state.AppliedRevision {
				continue
			}
			if event.Revision > state.NextRevision || state.Status > 1 {
				return fmt.Errorf("invalid stored state or unallocated revision: video %d account %d", key.videoID, key.accountID)
			}
			status := uint8(0)
			if event.DesiredLiked {
				status = 1
			}
			changes := map[string]any{"status": status, "applied_revision": event.Revision}
			if status != state.Status {
				changes["updated_at"] = now
				deltas[key.videoID] += int64(status) - int64(state.Status)
			}
			// UpdateColumns deliberately bypasses GORM's automatic UpdatedAt change:
			// true->true/false->false must advance revision without changing that time.
			if err := tx.Model(&Like{}).Where("id=?", state.ID).UpdateColumns(changes).Error; err != nil {
				return err
			}
		}
		videos := make([]uint, 0, len(deltas))
		for id, delta := range deltas {
			if delta != 0 {
				videos = append(videos, id)
			}
		}
		sort.Slice(videos, func(i, j int) bool { return videos[i] < videos[j] })
		for _, id := range videos {
			delta := deltas[id]
			result := tx.Model(&Video{}).Where("id=? AND likes_count + ? >= 0", id, delta).UpdateColumns(map[string]any{
				"likes_count": gorm.Expr("likes_count + ?", delta),
			})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("video %d missing or likes_count would become negative", id)
			}
		}
		changed = videos
		return nil
	})
	if err != nil {
		return nil, err
	}
	return changed, nil
}
