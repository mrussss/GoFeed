import http from 'k6/http';
import exec from 'k6/execution';
import { Rate, Trend, Counter } from 'k6/metrics';

const fixture = JSON.parse(open(__ENV.FIXTURE));
const mode = __ENV.LAB_SCENE;
const vus = Number(__ENV.VUS);
const duration = __ENV.DURATION || '20s';
const root = 'http://127.0.0.1:28085';
const feedSuccess = new Trend('feed_success_ms', true);
const feedLatency = new Trend("feed_all_ms", true);
const feedPass = new Rate('feed_pass');
const feedContractError = new Rate('feed_contract_error');
const feed500 = new Rate('feed_500');
const feed503 = new Rate('feed_503');
const feedN = new Counter('feed_n');
const profileSuccess = new Trend('profile_success_ms', true);
const profileLatency = new Trend('profile_all_ms', true);
const profilePass = new Rate('profile_pass');
const profileN = new Counter('profile_n');
const observed = new Set();

function call(path, body, token) {
  const headers = {'Content-Type':'application/json'};
  if (token) headers.Authorization = 'Bearer ' + token;
  return http.post(root + path, JSON.stringify(body), {headers,timeout:'5s',tags:{name:path}});
}

function feedCase() {
  if ((mode === 'fault' || mode === 'mixed_fault')) {
    const vu = exec.vu.idInTest - 1;
    const it = exec.vu.iterationInScenario;
    const batch = fixture.fault[vu % fixture.fault.length];
    return batch[it % batch.length];
  }
  if (mode === 'scale') return fixture.scale[__ENV.AUTHORS];
  if (mode === 'mixed' || mode === 'lock') return fixture.normal[mode === 'lock' ? 'Latest' : 'Following'];
  return fixture.normal[mode];
}

export default function () {
  const item = feedCase();
  const response = call(item.path, item.body, item.token);
  let good = false;
  let json;
  try {
    json=response.json();
    good=response.status===200 && json.has_more===true && json.video_list.length===10 &&
      json.video_list.every((video,i)=>video.id===item.ids[i]) &&
      (mode !== "scale" || (json.next_time === item.next_time && json.next_id_before === item.next_id_before)) &&
      (item.path!='/feed/listByPopularity' || (json.as_of===item.body.as_of && json.next_offset===10));
  } catch (_) {}
  feedPass.add(good);
  feedContractError.add(response.status === 200 && !good);
  feedLatency.add(response.timings.duration);
  feedN.add(1);
  feed500.add(response.status===500);
  feed503.add(response.status===503);
  if (good) feedSuccess.add(response.timings.duration);
  else if (!observed.has(response.status)) {
    observed.add(response.status);
    console.error(JSON.stringify({status:response.status,ids:json?.video_list?.map(v=>v.id),error:json?.error || response.error}));
  }
}

export function profile() {
  const expected = fixture.profiles[exec.vu.iterationInScenario % fixture.profiles.length];
  const response = call('/account/getProfile',{account_id:expected.account.id});
  let good=false;
  try {
    const json=response.json();
    good=response.status===200 && json.account.id===expected.account.id &&
         ['video_count','total_likes','follower_count','vlogger_count'].every(key=>json[key]===expected[key]);
  } catch (_) {}
  profilePass.add(good);
  profileN.add(1);
  profileLatency.add(response.timings.duration);
  if (good) profileSuccess.add(response.timings.duration);
}

export const options = {
  scenarios: { main: {executor:'constant-vus',vus,duration,gracefulStop:'5s'} },
  summaryTrendStats: ['avg','med','p(95)','p(99)','max'],
  maxRedirects: 0,
};
if (mode === 'mixed' || mode === 'mixed_fault') options.scenarios.profile={executor:'constant-arrival-rate',rate:20,timeUnit:'1s',duration,
  preAllocatedVUs:6,maxVUs:24,exec:'profile',gracefulStop:'5s'};

export function handleSummary(data) {
  return {[__ENV.SUMMARY_PATH]:JSON.stringify(data,null,2)+'\n'};
}
