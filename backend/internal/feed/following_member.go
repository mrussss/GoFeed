package feed

import "feedsystem_video_go/internal/feedindex"

const maxFollowingTimestamp = feedindex.MaxTimestamp

// FollowingMember sorts by (publishedAtMS, videoID) on score-zero sorted sets.
func FollowingMember(publishedAtMS int64, videoID uint) (string, error) {
	return feedindex.FollowingMember(publishedAtMS, videoID)
}

func ParseFollowingMember(member string) (int64, uint, error) {
	return feedindex.ParseFollowingMember(member)
}
