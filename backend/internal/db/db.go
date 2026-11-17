package db

import (
	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/message"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func NewDB(dbcfg config.DatabaseConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		dbcfg.User, dbcfg.Password, dbcfg.Host, dbcfg.Port, dbcfg.DBName)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return db, nil
}

func AutoMigrate(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&account.Account{}, &video.Video{}, &video.Like{}, &video.CommandOutbox{}, &video.Comment{},
		&social.Social{}, &video.OutboxMsg{}, &video.Tag{}, &video.VideoTag{},
		&message.Message{}, &worker.Notification{},
	); err != nil {
		return err
	}
	// The nullable new column is NULL for legacy rows. Copy the historical time,
	// rather than assigning migration time, and leave already populated times intact.
	if err := db.Exec("UPDATE likes SET updated_at = created_at WHERE updated_at IS NULL").Error; err != nil {
		return err
	}
	// AutoMigrate does not remove obsolete prototype fields or replace a named
	// index. Retire only obsolete A1 fields and indexes.
	migrator := db.Migrator()
	for _, index := range []string{"idx_command_publish_id", "idx_command_published_id", "idx_command_account_video_revision"} {
		if migrator.HasIndex(&video.CommandOutbox{}, index) {
			if err := migrator.DropIndex(&video.CommandOutbox{}, index); err != nil {
				return err
			}
		}
	}
	for _, column := range []string{"payload_hash", "publish_status"} {
		if migrator.HasColumn(&video.CommandOutbox{}, column) {
			if err := migrator.DropColumn(&video.CommandOutbox{}, column); err != nil {
				return err
			}
		}
	}
	return nil
}

func CloseDB(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
