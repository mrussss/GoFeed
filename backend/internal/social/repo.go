package social

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/account"

	mysql "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

var ErrAlreadyFollowed = errors.New("already followed")

type SocialRepository struct {
	db *gorm.DB
}

func NewSocialRepository(db *gorm.DB) *SocialRepository {
	return &SocialRepository{db: db}
}

func (r *SocialRepository) Follow(ctx context.Context, social *Social) error {
	err := r.db.WithContext(ctx).Create(social).Error
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return ErrAlreadyFollowed
	}
	return err
}

func (r *SocialRepository) Unfollow(ctx context.Context, social *Social) error {
	result := r.db.WithContext(ctx).
		Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.VloggerID).
		Delete(&Social{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (r *SocialRepository) GetAllFollowers(ctx context.Context, VloggerID uint) ([]*account.Account, error) {
	var relations []Social
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("vlogger_id = ?", VloggerID).
		Limit(200).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	followerIDs := make([]uint, 0, len(relations))
	for _, rel := range relations {
		followerIDs = append(followerIDs, rel.FollowerID)
	}
	if len(followerIDs) == 0 {
		return []*account.Account{}, nil
	}

	var followers []*account.Account
	if err := r.db.WithContext(ctx).
		Model(&account.Account{}).
		Where("id IN ?", followerIDs).
		Find(&followers).Error; err != nil {
		return nil, err
	}
	return followers, nil
}

func (r *SocialRepository) ListFollowedAuthorIDs(ctx context.Context, followerID uint) ([]uint, error) {
	var authorIDs []uint
	err := r.db.WithContext(ctx).Raw(
		"SELECT vlogger_id FROM socials FORCE INDEX (idx_social_follower_vlogger) WHERE follower_id = ?",
		followerID,
	).Scan(&authorIDs).Error
	return authorIDs, err
}

func (r *SocialRepository) GetAllVloggers(ctx context.Context, FollowerID uint) ([]*account.Account, error) {
	var relations []Social
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("follower_id = ?", FollowerID).
		Limit(200).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	vloggerIDs := make([]uint, 0, len(relations))
	for _, rel := range relations {
		vloggerIDs = append(vloggerIDs, rel.VloggerID)
	}
	if len(vloggerIDs) == 0 {
		return []*account.Account{}, nil
	}

	var vloggers []*account.Account
	if err := r.db.WithContext(ctx).
		Model(&account.Account{}).
		Where("id IN ?", vloggerIDs).
		Find(&vloggers).Error; err != nil {
		return nil, err
	}
	return vloggers, nil
}

func (r *SocialRepository) CountFollowers(ctx context.Context, vloggerID uint) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&Social{}).Where("vlogger_id = ?", vloggerID).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

type FollowerLink struct {
	ID         uint
	FollowerID uint
}

func (r *SocialRepository) ListFollowerLinks(ctx context.Context, authorID, afterID uint, limit int) ([]FollowerLink, error) {
	var links []FollowerLink
	// InnoDB appends the primary key to idx_social_vlogger. Use that
	// ordered range directly to avoid an index merge and filesort.
	err := r.db.WithContext(ctx).Raw(`
		SELECT id, follower_id FROM socials FORCE INDEX (idx_social_vlogger)
		WHERE vlogger_id = ? AND id > ? ORDER BY id LIMIT ?
	`, authorID, afterID, limit).Scan(&links).Error
	return links, err
}

func (r *SocialRepository) CountVloggers(ctx context.Context, followerID uint) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&Social{}).Where("follower_id = ?", followerID).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}
