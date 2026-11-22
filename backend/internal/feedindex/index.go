package feedindex

import (
	"fmt"
	"strconv"
)

const (
	GlobalTimelineKey       = "feed:global_timeline:v2"
	GlobalTimelineLimit     = 1000
	followingTimestampWidth = 13
	videoIDWidth            = 20
	MaxTimestamp            = 9_999_999_999_999
)

// FollowingMember sorts ascending by (publishedAtMS, videoID). Use a reverse
// lexicographic traversal to read newest candidates first at score zero.
func FollowingMember(publishedAtMS int64, videoID uint) (string, error) {
	if publishedAtMS < 0 || publishedAtMS > MaxTimestamp || videoID == 0 {
		return "", fmt.Errorf("candidate outside following member format: timestamp=%d video=%d", publishedAtMS, videoID)
	}
	return fmt.Sprintf("%0*d:%0*d", followingTimestampWidth, publishedAtMS, videoIDWidth, videoID), nil
}

func ParseFollowingMember(member string) (int64, uint, error) {
	if len(member) != followingTimestampWidth+1+videoIDWidth || member[followingTimestampWidth] != ':' {
		return 0, 0, fmt.Errorf("invalid following member %q", member)
	}
	publishedAtMS, timeErr := strconv.ParseUint(member[:followingTimestampWidth], 10, 64)
	videoID, ok := parseFixedVideoID(member[followingTimestampWidth+1:])
	if timeErr != nil || !ok {
		return 0, 0, fmt.Errorf("invalid following member %q", member)
	}
	return int64(publishedAtMS), videoID, nil
}

func PopularityMember(videoID uint) string {
	return fmt.Sprintf("%0*d", videoIDWidth, videoID)
}

func ParsePopularityMember(member string) (uint, error) {
	if id, ok := parseFixedVideoID(member); ok {
		return id, nil
	}
	return 0, fmt.Errorf("invalid popularity member %q", member)
}

func parseFixedVideoID(member string) (uint, bool) {
	if len(member) != videoIDWidth {
		return 0, false
	}
	id, err := strconv.ParseUint(member, 10, strconv.IntSize)
	return uint(id), err == nil && id != 0
}

const (
	PopularityMinuteFormat = "200601021504"
	PopularityBucketKey    = "hot:video:1m:v2:%s"
	PopularitySnapshotKey  = "hot:video:merge:1m:v2:%s"
)
