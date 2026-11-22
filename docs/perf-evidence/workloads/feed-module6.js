import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';

// Fixed-VU Closed Model: each VU starts its next request after checks complete.
// No sleep, setup request, login, cache reset, or performance thresholds.
const rawVus = __ENV.VUS || '5';
if (!/^[1-9]\d*$/.test(rawVus) || !Number.isSafeInteger(Number(rawVus))) {
  throw new Error('VUS must be a positive integer');
}
const vus = Number(rawVus);
const duration = __ENV.DURATION || '30s';
if (!/^(?:\d+(?:\.\d+)?(?:ms|s|m|h))+$/.test(duration) || !/[1-9]/.test(duration)) {
  throw new Error('DURATION must be a positive k6 duration, for example 30s or 1m');
}
const scene = __ENV.FEED_SCENE || 'Latest';
const payloads = {
  Latest: ['listLatest', { limit: 10 }],
  Following: ['listByFollowing', { limit: 10 }],
  LikesCount: ['listLikesCount', { limit: 10 }],
  PopularityFallback: ['listByPopularity', { limit: 10, as_of: 0, offset: 0 }],
  PopularityHot: ['listByPopularity', { limit: 10, as_of: Number(__ENV.FEED_AS_OF), offset: 0 }],
};
const [endpoint, payload] = payloads[scene];
const url = 'http://127.0.0.1:28083/feed/' + endpoint;
const body = JSON.stringify(payload);
const headers = { 'Content-Type': 'application/json' };
if (scene === 'Following') {
  if (!__ENV.FEED_TOKEN) throw new Error('Following requires FEED_TOKEN');
  headers.Authorization = 'Bearer ' + __ENV.FEED_TOKEN;
}
const summaryPath = __ENV.SUMMARY_PATH ||
  `data/baseline/k6/feed-latest-vus-${vus}-${Date.now()}.json`;
const requestErrors = new Rate('feed_request_errors');
const shed = new Rate('feed_503');
const serverErrors = new Rate('feed_500');
const seenFailures = new Set();

export const options = {
  scenarios: {
    feed_latest: {
      executor: 'constant-vus',
      vus,
      duration,
      gracefulStop: '10s',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
  maxRedirects: 0,
};

// Count every non-200 response as an HTTP failure, including redirects.
http.setResponseCallback(http.expectedStatuses(200));

export default function () {
  const response = http.post(url, body, {
    headers,
    timeout: '10s',
    tags: { name: 'POST /feed/' + endpoint },
  });
  let json;
  let validJSON = false;
  try {
    json = response.json();
    validJSON = true;
  } catch (_) {
    // Invalid or empty response body is counted as a failed check, not logged per request.
  }
  const statusOK = response.status === 200;
  shed.add(response.status === 503);
  serverErrors.add(response.status === 500);
  if (!statusOK && !seenFailures.has(response.status)) {
    seenFailures.add(response.status);
    console.error(JSON.stringify({status: response.status, detail: String(response.body || response.error || '').slice(0, 200)}));
  }
  const listOK = validJSON && json !== null && Array.isArray(json.video_list) &&
    json.video_list.length === 10;
  const hasMoreOK = validJSON && json !== null && json.has_more === true;
  const routeOK = validJSON && json !== null &&
    (scene !== 'PopularityHot' || (json.as_of === payload.as_of && json.next_offset === 10)) &&
    (scene !== 'PopularityFallback' || (json.as_of === 0 && json.next_offset === 0));

  check(response, {
    'HTTP status is 200': () => statusOK,
    'body is valid JSON': () => validJSON,
    'video_list contains 10 items': () => listOK,
    'has_more is true': () => hasMoreOK,
    'expected ranking path': () => routeOK,
  });
  // One sample per request, including HTTP, transport, JSON and structure failures.
  requestErrors.add(!(statusOK && validJSON && listOK && hasMoreOK && routeOK));
}

export function handleSummary(data) {
  const values = (name) => (data.metrics[name] && data.metrics[name].values) || {};
  const requests = values('http_reqs');
  const latency = values('http_req_duration');
  const httpErrors = values('http_req_failed');
  const errors = values('feed_request_errors');
  const readable = (n, digits = 3) => typeof n === 'number' ? n.toFixed(digits) : 'N/A';
  const percent = (n) => typeof n === 'number' ? `${(n * 100).toFixed(3)}%` : 'N/A';
  const report = {
    metadata: {
      model: 'Closed Model / constant-vus',
      method: 'POST',
      url,
      body: payload,
      authentication: scene === 'Following' ? 'bearer' : 'anonymous',
      vus,
      duration,
      graceful_stop: '10s',
      request_timeout: '10s',
      sleep: false,
      performance_thresholds: 'none',
      exported_at_utc: new Date().toISOString(),
    },
    results: {
      requests: requests.count ?? null,
      rps: requests.rate ?? null,
      p50_ms: latency.med ?? null,
      shed_rate: values('feed_503').rate ?? null,
      server_error_rate: values('feed_500').rate ?? null,
      p95_ms: latency['p(95)'] ?? null,
      p99_ms: latency['p(99)'] ?? null,
      http_error_rate: httpErrors.rate ?? null,
      request_error_rate: errors.rate ?? null,
    },
    k6: data,
  };
  return {
    stdout: [
      `Feed ${scene} | Closed Model | VUS=${vus} | duration=${duration}`,
      `Requests=${requests.count ?? 0} RPS=${readable(requests.rate)}`,
      `P95=${readable(latency['p(95)'])}ms P99=${readable(latency['p(99)'])}ms`,
      `HTTP error rate=${percent(httpErrors.rate)} Request error rate=${percent(errors.rate)}`,
      `Summary: ${summaryPath}`,
      '',
    ].join('\n'),
    [summaryPath]: JSON.stringify(report, null, 2) + '\n',
  };
}
