import { useAuthStore } from '../stores/auth'
import { reportError } from '../utils/error-reporter'

export class ApiError extends Error {
  status: number
  payload?: unknown

  constructor(message: string, status: number, payload?: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.payload = payload
  }
}

type ApiErrorBody = { error?: string }

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? '/api'

let isRefreshing = false
let refreshPromise: Promise<string | null> | null = null

async function tryRefresh(): Promise<string | null> {
  const auth = useAuthStore()
  if (!auth.refreshToken) return null
  if (isRefreshing) return refreshPromise
  isRefreshing = true
  const refreshAccountToken = auth.token
  refreshPromise = (async () => {
    try {
      const res = await fetch(`${API_BASE}/account/refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: auth.refreshToken }),
      })
      if (!res.ok) { if (auth.token === refreshAccountToken) auth.clearTokens(); return null }
      const data = await res.json()
      if (auth.token !== refreshAccountToken) return null
      auth.setToken(data.token)
      return data.token as string
    } catch {
      if (auth.token === refreshAccountToken) auth.clearTokens()
      return null
    } finally {
      isRefreshing = false
    }
  })()
  return refreshPromise
}

export async function postJson<T>(path: string, body: unknown, options?: { authRequired?: boolean; timeoutMs?: number; expectedAccountId?: number }): Promise<T> {
  const auth = useAuthStore()
  const token = auth.token

  if (options?.authRequired && !token) {
    throw new ApiError('需要先登录（缺少 token）', 401)
  }

  const ensureAccount = () => {
    if (options?.expectedAccountId && auth.claims?.account_id !== options.expectedAccountId) {
      throw new ApiError('登录账号已切换，请在原账号重试', 401)
    }
  }
  ensureAccount()
  const controller = new AbortController()
  const timer = options?.timeoutMs ? window.setTimeout(() => controller.abort(), options.timeoutMs) : undefined
  try {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' }
    if (token) headers.Authorization = `Bearer ${token}`

    const res = await fetch(`${API_BASE}${path}`, {
      method: 'POST',
      headers,
      signal: controller.signal,
      body: JSON.stringify(body ?? {}),
    })

    if (options?.expectedAccountId && auth.claims?.account_id !== options.expectedAccountId) {
      throw new ApiError('原账号操作接收结果未知', 0, { status: 'unknown' })
    }
    if (res.status === 401 && path !== '/account/refresh') {
      const newToken = await tryRefresh()
      if (newToken) {
        ensureAccount()
        headers.Authorization = `Bearer ${newToken}`
        const retryRes = await fetch(`${API_BASE}${path}`, {
          method: 'POST', headers, signal: controller.signal, body: JSON.stringify(body ?? {}),
        })
        if (options?.expectedAccountId && auth.claims?.account_id !== options.expectedAccountId) {
          throw new ApiError('原账号操作接收结果未知', 0, { status: 'unknown' })
        }
        return await handleResponse<T>(retryRes, path, options?.expectedAccountId)
      }
    }

    return await handleResponse<T>(res, path, options?.expectedAccountId)
  } finally {
    if (timer !== undefined) window.clearTimeout(timer)
  }
}

export async function postForm<T>(path: string, body: FormData, options?: { authRequired?: boolean }): Promise<T> {
  const auth = useAuthStore()
  const token = auth.token

  if (options?.authRequired && !token) {
    throw new ApiError('需要先登录（缺少 token）', 401)
  }

  const headers: Record<string, string> = {}
  if (token) headers.Authorization = `Bearer ${token}`

  const res = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers,
    body,
  })

  if (res.status === 401 && path !== '/account/refresh') {
    const newToken = await tryRefresh()
    if (newToken) {
      headers.Authorization = `Bearer ${newToken}`
      const retryRes = await fetch(`${API_BASE}${path}`, {
        method: 'POST', headers, body,
      })
      return handleResponse<T>(retryRes, path)
    }
  }

  return handleResponse<T>(res, path)
}

async function handleResponse<T>(res: Response, path: string, expectedAccountId?: number): Promise<T> {
  const auth = useAuthStore()
  const text = await res.text()
  if (expectedAccountId && auth.claims?.account_id !== expectedAccountId) {
    throw new ApiError('原账号操作接收结果未知', 0, { status: 'unknown' })
  }
  let data: unknown = null
  if (text) {
    try { data = JSON.parse(text) } catch { data = text }
  }

  if (!res.ok) {
    if (res.status === 401) auth.clearTokens()
    const msg = data && typeof data === 'object' && (data as ApiErrorBody).error
      ? String((data as ApiErrorBody).error)
      : `请求失败 (${res.status})`
    const apiErr = new ApiError(msg, res.status, data)
    reportError(apiErr, { path, status: res.status })
    throw apiErr
  }

  return data as T
}
