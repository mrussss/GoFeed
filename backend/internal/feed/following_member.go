package feed

import "fmt"

// FollowingMember sorts ascending by (publishedAtMS, videoID). Use a reverse
// lexicographic traversal to read newest candidates first at score zero.
func FollowingMember(publishedAtMS int64, videoID uint) (string, error) {
	if publishedAtMS < 0 || publishedAtMS > 9_999_999_999_999 || videoID == 0 {
		return "", fmt.Errorf("candidate outside following member format: timestamp=%d video=%d", publishedAtMS, videoID)
	}
	return fmt.Sprintf("%013d:%020d", publishedAtMS, videoID), nil
}
