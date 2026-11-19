package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server              ServerConfig          `yaml:"server"`
	Database            DatabaseConfig        `yaml:"database"`
	Redis               RedisConfig           `yaml:"redis"`
	RabbitMQ            RabbitMQConfig        `yaml:"rabbitmq"`
	FollowingFeed       FollowingFeedConfig   `yaml:"following_feed"`
	FollowingFanout     FollowingFanoutConfig `yaml:"following_fanout"`
	ObservabilityConfig ObservabilityConfig   `yaml:"observability"`
}

type FollowingFeedConfig struct {
	LowThreshold     int64 `yaml:"low_threshold"`
	InitialThreshold int64 `yaml:"initial_threshold"`
	HighThreshold    int64 `yaml:"high_threshold"`
}

func DefaultFollowingFeedConfig() FollowingFeedConfig {
	return FollowingFeedConfig{LowThreshold: 9000, InitialThreshold: 10000, HighThreshold: 11000}
}

type FollowingFanoutConfig struct {
	InboxMaxLen        int64 `yaml:"inbox_max_len"`
	AuthorOutboxMaxLen int64 `yaml:"author_outbox_max_len"`
	BatchSize          int   `yaml:"batch_size"`
}

func DefaultFollowingFanoutConfig() FollowingFanoutConfig {
	return FollowingFanoutConfig{InboxMaxLen: 1000, AuthorOutboxMaxLen: 500, BatchSize: 500}
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type RabbitMQConfig struct {
	VHost                string `yaml:"vhost"`
	LikePublishTimeoutMS int    `yaml:"like_publish_timeout_ms"`
	Host                 string `yaml:"host"`
	Port                 int    `yaml:"port"`
	Username             string `yaml:"username"`
	Password             string `yaml:"password"`
}

type ObservabilityConfig struct {
	Pprof PprofConfig `yaml:"pprof"`
}
type PprofConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ApiAddr    string `yaml:"api_addr"`
	WorkerAddr string `yaml:"worker_addr"`
}

func Load(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := Config{FollowingFeed: DefaultFollowingFeedConfig(), FollowingFanout: DefaultFollowingFanoutConfig()}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", filename, err)
	}
	thresholds := cfg.FollowingFeed
	if thresholds.LowThreshold < 0 || thresholds.LowThreshold >= thresholds.InitialThreshold || thresholds.InitialThreshold >= thresholds.HighThreshold {
		return Config{}, errors.New("following_feed requires 0 <= low_threshold < initial_threshold < high_threshold")
	}
	if fanout := cfg.FollowingFanout; fanout.InboxMaxLen <= 0 || fanout.AuthorOutboxMaxLen <= 0 || fanout.BatchSize <= 0 {
		return Config{}, errors.New("following_fanout requires positive index limits and batch_size")
	}

	ApplyEnvOverrides(&cfg)
	return cfg, nil
}

func ApplyEnvOverrides(cfg *Config) {
	if cfg == nil {
		return
	}
	if v := os.Getenv("SERVER_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = port
		}
	}
	if v := os.Getenv("MYSQL_HOST"); v != "" {
		cfg.Database.Host = v
	}
	if v := os.Getenv("MYSQL_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Database.Port = port
		}
	}
	if v := os.Getenv("MYSQL_USER"); v != "" {
		cfg.Database.User = v
	}
	if v := os.Getenv("MYSQL_ROOT_PASSWORD"); v != "" {
		cfg.Database.Password = v
	}
	if v := os.Getenv("MYSQL_PASSWORD"); v != "" {
		cfg.Database.Password = v
	}
	if v := os.Getenv("MYSQL_DATABASE"); v != "" {
		cfg.Database.DBName = v
	}
	if v := os.Getenv("REDIS_HOST"); v != "" {
		cfg.Redis.Host = v
	}
	if v := os.Getenv("REDIS_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Redis.Port = port
		}
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		cfg.Redis.Password = v
	}
	if v := os.Getenv("REDIS_DB"); v != "" {
		if db, err := strconv.Atoi(v); err == nil {
			cfg.Redis.DB = db
		}
	}
	if v := os.Getenv("RABBITMQ_HOST"); v != "" {
		cfg.RabbitMQ.Host = v
	}
	if v := os.Getenv("RABBITMQ_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.RabbitMQ.Port = port
		}
	}
	if v := os.Getenv("RABBITMQ_USER"); v != "" {
		cfg.RabbitMQ.Username = v
	}
	if v := os.Getenv("RABBITMQ_PASS"); v != "" {
		cfg.RabbitMQ.Password = v
	}
}

// bool用来表示是否使用了默认配置，true表示使用了默认配置
func LoadLocalDev(filename string) (Config, bool, error) {
	cfg, err := Load(filename)
	if err == nil {
		return cfg, false, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return DefaultLocalConfig(), true, nil
	}
	return Config{}, false, err
}

func DefaultLocalConfig() Config {
	cfg := Config{
		FollowingFeed:   DefaultFollowingFeedConfig(),
		FollowingFanout: DefaultFollowingFanoutConfig(),
		Server: ServerConfig{
			Port: 8080,
		},
		Database: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "123456",
			DBName:   "feedsystem",
		},
		Redis: RedisConfig{
			Host:     "localhost",
			Port:     6379,
			Password: "123456",
			DB:       0,
		},
		RabbitMQ: RabbitMQConfig{
			Host:     "localhost",
			Port:     5672,
			Username: "admin",
			Password: "password123",
		},
		ObservabilityConfig: ObservabilityConfig{
			Pprof: PprofConfig{
				Enabled:    true,
				ApiAddr:    "localhost:6060",
				WorkerAddr: "localhost:6061",
			},
		},
	}
	ApplyEnvOverrides(&cfg)
	return cfg
}
