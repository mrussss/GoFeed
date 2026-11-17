import { computed, reactive } from 'vue'
import { ApiError } from '../api/client'
import * as likeApi from '../api/like'
import type { LikeIntent } from '../api/like'
import { useAuthStore } from '../stores/auth'
import { useSocialStore } from '../stores/social'
import { useToastStore } from '../stores/toast'

type LikeItem = { id: number; is_liked: boolean; likes_count: number }
type Pending = { intent: LikeIntent; beforeLiked: boolean; beforeCount: number; uncertain: boolean }
type Saved = { revision: string; pending?: Pending }
const sending = reactive<Record<string, boolean>>({})
const displays = reactive<Record<string, { liked: boolean; count: number; accepted?: boolean }>>({})
const storageKey = (accountId: number, videoId: number) => `gofeed:like:v2:${accountId}:${videoId}`

function readSaved(key: string): Saved {
  const raw = localStorage.getItem(key)
  if (!raw) return { revision: '0' }
  const saved = JSON.parse(raw) as Saved
  if (!/^(0|[1-9][0-9]*)$/.test(saved.revision) || BigInt(saved.revision) > 18446744073709551615n) {
    throw new Error('本地点赞版本损坏，请保留记录并联系支持')
  }
  return saved
}

export function useLikeFollow(needLogin: () => void) {
  const auth = useAuthStore()
  const social = useSocialStore()
  const toast = useToastStore()

  const likeBusy = computed(() => {
    const prefix = `gofeed:like:v2:${auth.claims?.account_id}:`
    return Object.fromEntries(Object.entries(sending).filter(([key]) => key.startsWith(prefix)).map(([key, busy]) => [key.slice(prefix.length), busy]))
  })
  const followBusy = reactive<Record<string, boolean>>({})

  // Call when a view receives a feed/detail response. A delayed read must not
  // replace an unresolved intent or the accepted optimistic display in this session.
  function protectLike(item: LikeItem, fromRead = false) {
    const accountId = auth.claims?.account_id
    if (!accountId) return item
    const key = storageKey(accountId, item.id)
    let display = displays[key]
    if (!display) {
      try {
        const pending = readSaved(key).pending
        if (pending) display = displays[key] = {
          liked: pending.intent.desired_liked,
          count: Math.max(0, pending.beforeCount + (pending.intent.desired_liked ? 1 : -1)),
        }
      } catch { /* new writes fail visibly if local persistence is unavailable */ }
    }
    // Once a later read agrees with a resolved intent, resume displaying server
    // counts. Unresolved operations must always retain their optimistic overlay.
    if (fromRead && display?.accepted && item.is_liked === display.liked) {
      delete displays[key]
      return item
    }
    if (display) { item.is_liked = display.liked; item.likes_count = display.count }
    return item
  }

  async function toggleLike(item: LikeItem) {
    if (!auth.isLoggedIn) return needLogin()
    const accountId = auth.claims?.account_id
    if (!accountId) return
    const key = storageKey(accountId, item.id)
    if (sending[key]) return
    sending[key] = true
    let saved: Saved | undefined
    let pending: Pending | undefined
    let wasUncertain = false
    let sent = false
    try {
      saved = readSaved(key)
      pending = saved.pending
      wasUncertain = !!pending?.uncertain
      if (!pending) {
        const revision = BigInt(saved.revision) + 1n
        if (revision > 18446744073709551615n) throw new Error('点赞版本已达上限')
        const bytes = crypto.getRandomValues(new Uint8Array(16))
        const eventId = Array.from(bytes, n => n.toString(16).padStart(2, '0')).join('')
        pending = { intent: {
          schema_version: 2, event_id: eventId, video_id: item.id, revision: revision.toString(),
          desired_liked: !item.is_liked, occurred_at: new Date().toISOString(),
        }, beforeLiked: item.is_liked, beforeCount: item.likes_count, uncertain: true }
        saved = { revision: revision.toString(), pending }
      }
      // Persist BEFORE showing/sending. A crash leaves the same immutable event
      // for the next click to retry, rather than permitting an opposite intent.
      localStorage.setItem(key, JSON.stringify(saved))
      displays[key] = { liked: pending.intent.desired_liked,
        count: Math.max(0, pending.beforeCount + (pending.intent.desired_liked ? 1 : -1)) }
      protectLike(item)
      sent = true
      await likeApi.sendIntent(pending.intent, accountId)
      localStorage.setItem(key, JSON.stringify({ revision: saved.revision }))
      displays[key]!.accepted = true
      if (auth.claims?.account_id === accountId) toast.info('已接收，点赞数据正在异步保存')
    } catch (e) {
      const payload = e instanceof ApiError ? e.payload as { status?: string } | undefined : undefined
      const definite = e instanceof ApiError && (payload?.status === 'rejected' || (e.status >= 400 && e.status < 500 && ![408, 409].includes(e.status)))
      if (saved && pending) {
        if (definite && !wasUncertain) {
          // Keep a rollback overlay while synchronous view watchers run; deleting
          // it first could rehydrate the still-persisted optimistic operation.
          displays[key] = { liked: pending.beforeLiked, count: pending.beforeCount, accepted: true }
          try { localStorage.setItem(key, JSON.stringify({ revision: saved.revision })) } catch { /* keep unresolved on disk */ }
          if (auth.claims?.account_id === accountId) protectLike(item)
        }
        // Unknown, conflict, network failure, or rejection after an earlier
        // unknown attempt: preserve pending. Next click retries this event only.
      }
      if (auth.claims?.account_id === accountId) {
        toast.error(!sent || (definite && !wasUncertain) ? (e instanceof Error ? e.message : String(e)) : '接收结果未知，再次点击将重试原操作')
      }
    } finally {
      sending[key] = false
    }
  }

  async function toggleFollow(authorId: number) {
    if (!auth.isLoggedIn) return needLogin()
    const key = String(authorId)
    if (followBusy[key]) return
    followBusy[key] = true
    try {
      if (social.isFollowing(authorId)) {
        await social.unfollow(authorId)
        toast.info('已取关')
      } else {
        await social.follow(authorId)
        toast.success('已关注')
      }
    } catch (e) {
      const msg = e instanceof ApiError ? e.message : String(e)
      toast.error(msg)
    } finally {
      followBusy[key] = false
    }
  }

  async function share(item: { id: number }) {
    const url = `${location.origin}/video/${item.id}`
    try {
      await navigator.clipboard.writeText(url)
      toast.success('链接已复制')
    } catch {
      window.prompt('复制链接', url)
    }
  }

  return { likeBusy, followBusy, toggleLike, protectLike, toggleFollow, share }
}
