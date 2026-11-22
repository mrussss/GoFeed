import { computed, reactive, ref, watch } from 'vue'
import { ApiError } from '../api/client'
import * as feedApi from '../api/feed'
import type { FeedTimeCursor, FeedVideoItem } from '../api/types'
import { useAuthStore } from '../stores/auth'
import { useLikeFollow } from './useLikeFollow'

export type TabKey = 'recommend' | 'hot' | 'following'

export function useVideoFeed() {
  const auth = useAuthStore()
  const { protectLike } = useLikeFollow(() => {})
  const tab = ref<TabKey>('recommend')

  const recommend = reactive({
    items: [] as FeedVideoItem[],
    loading: false, error: '',
    hasMore: false, cursor: undefined as FeedTimeCursor | undefined,
  })

  const hot = reactive({
    items: [] as FeedVideoItem[],
    loading: false, error: '',
    hasMore: false,
    nextLikesCountBefore: undefined as number | undefined,
    nextIdBefore: undefined as number | undefined,
  })

  const following = reactive({
    items: [] as FeedVideoItem[],
    loading: false, error: '',
    hasMore: false, cursor: undefined as FeedTimeCursor | undefined,
  })

  const currentState = computed(() => {
    if (tab.value === 'hot') return hot
    if (tab.value === 'following') return following
    return recommend
  })

  async function loadRecommend(reset: boolean) {
    if (recommend.loading) return
    recommend.loading = true
    recommend.error = ''
    const accountId = auth.claims?.account_id
    try {
      const res = await feedApi.listLatest(reset || !recommend.cursor ? { limit: 10 } : { limit: 10, ...recommend.cursor })
      if (auth.claims?.account_id !== accountId) return
      res.video_list.forEach(item => protectLike(item, true))
      recommend.hasMore = res.has_more
      recommend.cursor = res.has_more ? { latest_time: res.next_time, id_before: res.next_id_before } : undefined
      recommend.items = reset ? res.video_list : recommend.items.concat(res.video_list)
    } catch (e) {
      recommend.error = e instanceof ApiError ? e.message : String(e)
    } finally {
      recommend.loading = false
      if (auth.claims?.account_id !== accountId) void ensureTabLoaded()
    }
  }

  async function loadHot(reset: boolean) {
    if (hot.loading) return
    hot.loading = true
    hot.error = ''
    const accountId = auth.claims?.account_id
    try {
      const res = await feedApi.listLikesCount({
        limit: 10,
        likes_count_before: reset ? undefined : hot.nextLikesCountBefore,
        id_before: reset ? undefined : hot.nextIdBefore,
      })
      if (auth.claims?.account_id !== accountId) return
      res.video_list.forEach(item => protectLike(item, true))
      hot.hasMore = res.has_more
      hot.nextLikesCountBefore = res.next_likes_count_before
      hot.nextIdBefore = res.next_id_before
      hot.items = reset ? res.video_list : hot.items.concat(res.video_list)
    } catch (e) {
      hot.error = e instanceof ApiError ? e.message : String(e)
    } finally {
      hot.loading = false
      if (auth.claims?.account_id !== accountId) void ensureTabLoaded()
    }
  }

  async function loadFollowing(reset: boolean) {
    if (!auth.isLoggedIn) {
      following.error = '登录后才能查看关注流'
      return
    }
    if (following.loading) return
    following.loading = true
    following.error = ''
    const accountId = auth.claims?.account_id
    try {
      const res = await feedApi.listByFollowing(reset || !following.cursor ? { limit: 10 } : { limit: 10, ...following.cursor })
      if (auth.claims?.account_id !== accountId) return
      res.video_list.forEach(item => protectLike(item, true))
      following.hasMore = res.has_more
      following.cursor = res.has_more ? { latest_time: res.next_time, id_before: res.next_id_before } : undefined
      following.items = reset ? res.video_list : following.items.concat(res.video_list)
    } catch (e) {
      following.error = e instanceof ApiError ? e.message : String(e)
    } finally {
      following.loading = false
      if (auth.claims?.account_id !== accountId) void ensureTabLoaded()
    }
  }

  async function ensureTabLoaded() {
    if (tab.value === 'recommend' && recommend.items.length === 0) await loadRecommend(true)
    if (tab.value === 'hot' && hot.items.length === 0) await loadHot(true)
    if (tab.value === 'following' && following.items.length === 0) await loadFollowing(true)
  }

  async function loadMoreIfNeeded(activeIndex: number) {
    const items = currentState.value.items
    if (items.length === 0) return
    if (activeIndex < items.length - 3) return
    if (tab.value === 'recommend' && recommend.hasMore) await loadRecommend(false)
    if (tab.value === 'hot' && hot.hasMore) await loadHot(false)
    if (tab.value === 'following' && following.hasMore) await loadFollowing(false)
  }

  watch(() => auth.claims?.account_id, () => {
    recommend.items = []; hot.items = []; following.items = []
    void ensureTabLoaded()
  })

  return { tab, recommend, hot, following, currentState, loadRecommend, loadHot, loadFollowing, ensureTabLoaded, loadMoreIfNeeded }
}
