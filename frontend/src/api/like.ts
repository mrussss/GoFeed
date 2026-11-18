import { ApiError, postJson } from './client'
import type { IsLikedResponse, Video } from './types'

export type LikeCommand = {
  request_id: string
  video_id: number
  desired_liked: boolean
}
export type LikeAccepted = { status: 'accepted'; request_id: string; command_id: string; revision: string }

export async function sendCommand(command: LikeCommand, accountId: number) {
  const res = await postJson<LikeAccepted>(command.desired_liked ? '/like/like' : '/like/unlike', command,
    { authRequired: true, timeoutMs: 8000, expectedAccountId: accountId })
  if (res?.status !== 'accepted' || res.request_id !== command.request_id ||
      !/^[1-9][0-9]*$/.test(res.command_id) || !/^[1-9][0-9]*$/.test(res.revision)) {
    throw new ApiError('命令受理结果未知，请重试原请求', 0)
  }
  return res
}

export function isLiked(videoId: number) {
  return postJson<IsLikedResponse>('/like/isLiked', { video_id: videoId }, { authRequired: true })
}

export function listMyLikedVideos() {
  return postJson<Video[]>('/like/listMyLikedVideos', {}, { authRequired: true })
}
