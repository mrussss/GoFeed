package video

import (
	"os"
	"path/filepath"
	"testing"

	"feedsystem_video_go/internal/config"
)

func TestDistributionMode(t *testing.T) {
	cfg := config.DefaultFollowingFeedConfig()
	for _, tc := range []struct {
		name      string
		current   DistributionMode
		followers int64
		want      DistributionMode
	}{
		{"initial below threshold", "", 9999, DistributionPush},
		{"initial at threshold", "", 10000, DistributionPull},
		{"push stays in band", DistributionPush, 10999, DistributionPush},
		{"push at high threshold", DistributionPush, 11000, DistributionPull},
		{"pull stays in band", DistributionPull, 9001, DistributionPull},
		{"pull at low threshold", DistributionPull, 9000, DistributionPush},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := distributionMode(tc.current, tc.followers, cfg); got != tc.want {
				t.Fatalf("mode = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestFollowingFeedConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		want    config.FollowingFeedConfig
		invalid bool
	}{
		{"defaults", "server: {}", config.DefaultFollowingFeedConfig(), false},
		{"partial override", "following_feed: {initial_threshold: 10200}", config.FollowingFeedConfig{LowThreshold: 9000, InitialThreshold: 10200, HighThreshold: 11000}, false},
		{"low equals initial", "following_feed: {low_threshold: 10000}", config.FollowingFeedConfig{}, true},
		{"high equals initial", "following_feed: {high_threshold: 10000}", config.FollowingFeedConfig{}, true},
		{"negative low", "following_feed: {low_threshold: -1}", config.FollowingFeedConfig{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(path)
			if (err != nil) != tc.invalid || cfg.FollowingFeed != tc.want {
				t.Fatalf("config = %+v, err = %v", cfg.FollowingFeed, err)
			}
		})
	}
}
