package feed

import (
	"fmt"
	"strconv"
)

const (
	followingTimestampWidth = 13
	followingVideoIDWidth   = 20
	maxFollowingTimestamp   = 9_999_999_999_999
)

// FollowingMember sorts ascending by (publishedAtMS, videoID). Use a reverse
// lexicographic traversal to read newest candidates first at score zero.
func FollowingMember(publishedAtMS int64, videoID uint) (string, error) {
	if publishedAtMS < 0 || publishedAtMS > maxFollowingTimestamp || videoID == 0 {
		return "", fmt.Errorf("candidate outside following member format: timestamp=%d video=%d", publishedAtMS, videoID)
	}
	return fmt.Sprintf("%0*d:%0*d", followingTimestampWidth, publishedAtMS, followingVideoIDWidth, videoID), nil
}

func ParseFollowingMember(member string) (int64, uint, error) {
	if len(member) != followingTimestampWidth+1+followingVideoIDWidth || member[followingTimestampWidth] != ':' {
		return 0, 0, fmt.Errorf("invalid following member %q", member)
	}
	publishedAtMS, timeErr := strconv.ParseUint(member[:followingTimestampWidth], 10, 64)
	videoID, idErr := strconv.ParseUint(member[followingTimestampWidth+1:], 10, strconv.IntSize)
	if timeErr != nil || idErr != nil || videoID == 0 {
		return 0, 0, fmt.Errorf("invalid following member %q", member)
	}
	return int64(publishedAtMS), uint(videoID), nil
}
