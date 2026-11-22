<script setup lang="ts">
import { computed, onMounted, reactive, watch } from 'vue'

import AppShell from '../components/AppShell.vue'
import JsonBox from '../components/JsonBox.vue'
import FeedVideoCard from '../components/FeedVideoCard.vue'
import { ApiError } from '../api/client'
import * as feedApi from '../api/feed'
import { useLikeFollow } from '../composables/useLikeFollow'
import type { FeedTimeCursor, FeedVideoItem } from '../api/types'
import { useAuthStore } from '../stores/auth'

const auth = useAuthStore()

type ListState = {
  loading: boolean
  error: string
  items: FeedVideoItem[]
  has_more: boolean
}

const latest = reactive<ListState & { limit: number; cursor?: FeedTimeCursor }>({
  loading: false,
  error: '',
  items: [],
  has_more: false,
  limit: 10,
  cursor: undefined,
})

const likesCount = reactive<ListState & { limit: number; next_likes_count_before?: number; next_id_before?: number }>({
  loading: false,
  error: '',
  items: [],
  has_more: false,
  limit: 10,
  next_likes_count_before: undefined,
  next_id_before: undefined,
})

const following = reactive<ListState & { limit: number; cursor?: FeedTimeCursor }>({
  loading: false,
  error: '',
  items: [],
  has_more: false,
  limit: 10,
  cursor: undefined,
})

const action = reactive<{ loading: boolean; error: string; payload: unknown; name: string }>({
  loading: false,
  error: '',
  payload: null,
  name: '',
})

const canLike = computed(() => auth.isLoggedIn)

async function loadLatest(reset: boolean) {
  latest.loading = true
  latest.error = ''
  const accountId = auth.claims?.account_id
  try {
    const limit = latest.limit
    const res = await feedApi.listLatest(reset || !latest.cursor ? { limit } : { limit, ...latest.cursor })
    if (auth.claims?.account_id !== accountId) return
    res.video_list.forEach(item => protectLike(item, true))
    latest.has_more = res.has_more
    latest.cursor = res.next_id_before > 0 ? { latest_time: res.next_time, id_before: res.next_id_before } : undefined
    latest.items = reset ? res.video_list : latest.items.concat(res.video_list)
  } catch (e) {
    latest.error = e instanceof ApiError ? e.message : String(e)
  } finally {
    latest.loading = false
  }
}

async function loadLikesCount(reset: boolean) {
  likesCount.loading = true
  likesCount.error = ''
  const accountId = auth.claims?.account_id
  try {
    const res = await feedApi.listLikesCount({
      limit: likesCount.limit,
      likes_count_before: reset ? undefined : likesCount.next_likes_count_before,
      id_before: reset ? undefined : likesCount.next_id_before,
    })
    if (auth.claims?.account_id !== accountId) return
    res.video_list.forEach(item => protectLike(item, true))
    likesCount.has_more = res.has_more
    likesCount.next_likes_count_before = res.next_likes_count_before
    likesCount.next_id_before = res.next_id_before
    likesCount.items = reset ? res.video_list : likesCount.items.concat(res.video_list)
  } catch (e) {
    likesCount.error = e instanceof ApiError ? e.message : String(e)
  } finally {
    likesCount.loading = false
  }
}

async function loadFollowing(reset: boolean) {
  following.loading = true
  following.error = ''
  const accountId = auth.claims?.account_id
  try {
    const limit = following.limit
    const res = await feedApi.listByFollowing(reset || !following.cursor ? { limit } : { limit, ...following.cursor })
    if (auth.claims?.account_id !== accountId) return
    res.video_list.forEach(item => protectLike(item, true))
    following.has_more = res.has_more
    following.cursor = res.next_id_before > 0 ? { latest_time: res.next_time, id_before: res.next_id_before } : undefined
    following.items = reset ? res.video_list : following.items.concat(res.video_list)
  } catch (e) {
    following.error = e instanceof ApiError ? e.message : String(e)
  } finally {
    following.loading = false
  }
}

const { likeBusy, toggleLike, protectLike } = useLikeFollow(() => {})
watch(() => [latest.items, likesCount.items, following.items], lists => lists.forEach(items => items.forEach(item => protectLike(item))), { deep: true, flush: 'sync' })

onMounted(async () => {
  await loadLatest(true)
  await loadLikesCount(true)
  if (auth.isLoggedIn) {
    await loadFollowing(true)
  }
})

watch(
  () => auth.claims?.account_id,
  async (v) => {
    latest.items = []; likesCount.items = []; following.items = []
    await loadLatest(true)
    await loadLikesCount(true)
    if (v) await loadFollowing(true)
  },
)
</script>

<template>
  <AppShell>
    <div class="grid two">
      <div class="card">
        <p class="title">Feed</p>
        <p class="subtle">`/feed/listLatest` 与 `/feed/listLikesCount` 支持匿名（可选 JWT）；`/feed/listByFollowing` 需要 JWT。</p>

        <div class="card" style="margin-top: 12px">
          <div class="row" style="justify-content: space-between">
            <div>
              <p class="title">最新流（listLatest）</p>
              <div class="subtle">limit：{{ latest.limit }} · next_time：{{ latest.cursor?.latest_time ?? 0 }} · next_id_before：{{ latest.cursor?.id_before ?? 0 }} · has_more：{{ latest.has_more }}</div>
            </div>
            <div class="row">
              <label class="subtle" style="margin: 0">limit</label>
              <input v-model.number="latest.limit" type="number" min="1" max="50" style="width: 90px" />
              <button class="primary" type="button" :disabled="latest.loading" @click="loadLatest(true)">刷新</button>
              <button type="button" :disabled="latest.loading || !latest.has_more" @click="loadLatest(false)">加载更多</button>
            </div>
          </div>
          <div v-if="latest.error" class="pill bad" style="margin-top: 10px">错误：{{ latest.error }}</div>
          <div class="grid" style="gap: 10px; margin-top: 12px">
            <FeedVideoCard
              v-for="item in latest.items"
              :key="`latest-${item.id}`"
              :item="item"
              :can-like="canLike"
              :busy="!!likeBusy[String(item.id)]"
              @toggle-like="toggleLike"
            />
          </div>
        </div>

        <div class="card" style="margin-top: 12px">
          <div class="row" style="justify-content: space-between">
            <div>
              <p class="title">点赞数流（listLikesCount）</p>
              <div class="subtle">
                limit：{{ likesCount.limit }} · next=(likes={{ likesCount.next_likes_count_before }}, id={{ likesCount.next_id_before }})
                · has_more：{{ likesCount.has_more }}
              </div>
            </div>
            <div class="row">
              <label class="subtle" style="margin: 0">limit</label>
              <input v-model.number="likesCount.limit" type="number" min="1" max="50" style="width: 90px" />
              <button class="primary" type="button" :disabled="likesCount.loading" @click="loadLikesCount(true)">刷新</button>
              <button type="button" :disabled="likesCount.loading || !likesCount.has_more" @click="loadLikesCount(false)">
                加载更多
              </button>
            </div>
          </div>
          <div v-if="likesCount.error" class="pill bad" style="margin-top: 10px">错误：{{ likesCount.error }}</div>
          <div class="grid" style="gap: 10px; margin-top: 12px">
            <FeedVideoCard
              v-for="item in likesCount.items"
              :key="`likes-${item.id}`"
              :item="item"
              :can-like="canLike"
              :busy="!!likeBusy[String(item.id)]"
              @toggle-like="toggleLike"
            />
          </div>
        </div>

        <div class="card" style="margin-top: 12px">
          <div class="row" style="justify-content: space-between">
            <div>
              <p class="title">关注流（listByFollowing，JWT）</p>
              <div class="subtle">
                limit：{{ following.limit }} · next_time：{{ following.cursor?.latest_time ?? 0 }} · next_id_before：{{ following.cursor?.id_before ?? 0 }} · has_more：{{ following.has_more }}
              </div>
            </div>
            <div class="row">
              <label class="subtle" style="margin: 0">limit</label>
              <input v-model.number="following.limit" type="number" min="1" max="50" style="width: 90px" />
              <button class="primary" type="button" :disabled="following.loading" @click="loadFollowing(true)">刷新</button>
              <button type="button" :disabled="following.loading || !following.has_more" @click="loadFollowing(false)">加载更多</button>
            </div>
          </div>
          <div v-if="!auth.isLoggedIn" class="pill bad" style="margin-top: 10px">未登录：无法访问关注流</div>
          <div v-if="following.error" class="pill bad" style="margin-top: 10px">错误：{{ following.error }}</div>
          <div class="grid" style="gap: 10px; margin-top: 12px">
            <FeedVideoCard
              v-for="item in following.items"
              :key="`following-${item.id}`"
              :item="item"
              :can-like="canLike"
              :busy="!!likeBusy[String(item.id)]"
              @toggle-like="toggleLike"
            />
          </div>
        </div>
      </div>

      <div class="card">
        <p class="title">动作输出（点赞等）</p>
        <div class="row" style="margin-bottom: 10px">
          <span class="pill">动作：{{ action.name || '-' }}</span>
          <span v-if="action.loading" class="pill">请求中…</span>
          <span v-if="action.error" class="pill bad">错误：{{ action.error }}</span>
        </div>
        <JsonBox :value="action.payload" />
      </div>
    </div>
  </AppShell>
</template>
