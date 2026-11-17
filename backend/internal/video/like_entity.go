package video

import (
	"errors"
	"strings"
	"time"
)

type Like struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	VideoID   uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"video_id"`
	AccountID uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"account_id"`
	CreatedAt time.Time `json:"created_at"`
	// Default 1 preserves existing rows during migration. Command
	// acceptance explicitly creates inactive (status=0) rows without applying intent.
	Status          uint8     `gorm:"type:tinyint unsigned;not null;default:1" json:"status"`
	NextRevision    uint64    `gorm:"type:bigint unsigned;not null;default:0" json:"next_revision,string"`
	AppliedRevision uint64    `gorm:"type:bigint unsigned;not null;default:0" json:"applied_revision,string"`
	UpdatedAt       time.Time `gorm:"type:datetime(3)" json:"updated_at"`
}

type LikeRequest struct {
	VideoID uint `json:"video_id"`
}

// LikeCommandRequest contains intent; only MySQL may allocate its revision.
type LikeCommandRequest struct {
	RequestID    string `json:"request_id"`
	VideoID      uint   `json:"video_id"`
	DesiredLiked bool   `json:"desired_liked"`
}

var ErrInvalidLikeCommand = errors.New("video_id and request_id (1-64 bytes) are required")

func (req LikeCommandRequest) Validate() error {
	if req.VideoID == 0 || strings.TrimSpace(req.RequestID) == "" || len(req.RequestID) > 64 {
		return ErrInvalidLikeCommand
	}
	return nil
}

// CommandOutbox contains accepted like commands, not video publication events.
type CommandOutbox struct {
	ID        uint64 `gorm:"primaryKey" json:"id"`
	AccountID uint   `gorm:"not null;uniqueIndex:idx_command_account_request,priority:1" json:"account_id"`
	// Binary comparison keeps opaque request IDs case-sensitive, independent of
	// the database's default collation (including trailing spaces).
	RequestID    string     `gorm:"type:varbinary(64);not null;uniqueIndex:idx_command_account_request,priority:2" json:"request_id"`
	VideoID      uint       `gorm:"not null" json:"video_id"`
	DesiredLiked bool       `gorm:"not null" json:"desired_liked"`
	Revision     uint64     `gorm:"type:bigint unsigned;not null" json:"revision,string"`
	PublishedAt  *time.Time `gorm:"type:datetime(3);index:idx_command_published_at" json:"published_at"`
	CreatedAt    time.Time  `gorm:"not null" json:"created_at"`
}

func (CommandOutbox) TableName() string { return "command_outbox" }
