import { ApiError, postJson } from './client'
import type { IsLikedResponse, Video } from './types'

export type LikeIntent = {
  schema_version: 2
  event_id: string
  video_id: number
  revision: string
  desired_liked: boolean
  occurred_at: string
}
export type LikeAccepted = { status: 'accepted'; event_id: string; revision: string }

export async function sendIntent(intent: LikeIntent, accountId: number) {
  const res = await postJson<LikeAccepted>(intent.desired_liked ? '/like/like' : '/like/unlike', intent,
    { authRequired: true, timeoutMs: 8000, expectedAccountId: accountId })
  if (res?.status !== 'accepted' || res.event_id !== intent.event_id || res.revision !== intent.revision) {
    throw new ApiError('点赞接收结果未知，请重试原操作', 0, { status: 'unknown' })
  }
  return res
}

export function isLiked(videoId: number) {
  return postJson<IsLikedResponse>('/like/isLiked', { video_id: videoId }, { authRequired: true })
}

export function listMyLikedVideos() {
  return postJson<Video[]>('/like/listMyLikedVideos', {}, { authRequired: true })
}
