import http from 'k6/http';
import exec from 'k6/execution';
import { Rate, Counter } from 'k6/metrics';
const accepted = new Rate('accepted');
const retries=new Counter('retry_requests');const conflicts=new Counter('request_conflicts');const timeouts=new Counter('request_timeouts');const api202=new Rate('api_202');
const tokens = JSON.parse(open(__ENV.TOKENS));
const scenario = __ENV.SCENE || 'uniform';
const rate = Number(__ENV.RATE || 50);
export const options = {
 scenarios: { load: {executor:'constant-arrival-rate',rate,timeUnit:'1s',duration:__ENV.DURATION || '60s',preAllocatedVUs:50,maxVUs:200,gracefulStop:'15s'} },
 summaryTrendStats:['avg','med','p(95)','p(99)','max'],
 thresholds: {http_req_failed:['rate<0.01'],accepted:['rate>0.99'],dropped_iterations:['count==0']},
};
function random(n, salt) { // reproducible across iteration scheduling and repeats
 let x=(n+salt+20261117)>>>0; x=Math.imul(x^(x>>>16),0x45d9f3b); x=Math.imul(x^(x>>>16),0x45d9f3b); return (x^(x>>>16))>>>0;
}
const weights=Array.from({length:100},(_,i)=>1/Math.pow(i+1,1.1));
const sum=weights.reduce((a,b)=>a+b,0);let acc=0;const cdf=weights.map(w=>(acc+=w/sum));
export default function () {
 const iteration=exec.scenario.iterationInTest;
 const isRetry=scenario==='mixed' && iteration%10===9;
 const n=isRetry?iteration-1:iteration;
 retries.add(isRetry?1:0);
 const account=random(n,1)%tokens.length;
 let video=random(n,2)%10000+1;
 if ((scenario==='hot' || scenario==='zipf' || scenario==='readwrite') && random(n,3)%10<8) video=random(n,4)%100+1;
 if (scenario==='zipf' && random(n,3)%10<8) {const u=random(n,4)/4294967296;video=cdf.findIndex(x=>u<x)+1;}
 if (scenario==='single') video=1;
 let desired=scenario==='mixed' ? random(n,5)%2===0 : true;
 if(scenario==='readwrite' && n%2===0) {
  const detail=n%4===0;
  const r=http.post('http://127.0.0.1:38080/'+(detail?'video/getDetail':'feed/listLatest'),JSON.stringify(detail?{id:video}:{limit:10,latest_time:0}),{headers:{'Content-Type':'application/json'},tags:{kind:detail?'detail':'feed'}});
  if(r.status!==200)console.error(JSON.stringify({kind:'read',iteration:n,status:r.status,error:r.error,body:String(r.body).slice(0,1024)}));
  timeouts.add(String(r.error||'').includes('timeout')?1:0);accepted.add(r.status===200); return;
 }
 const r=http.post('http://127.0.0.1:38080/like/'+(desired?'like':'unlike'),JSON.stringify({request_id:__ENV.RUN+'-'+n,video_id:video,desired_liked:desired}),{headers:{'Content-Type':'application/json',Authorization:'Bearer '+tokens[account]},timeout:'10s',tags:{kind:'write'}});
 if(r.status!==202)console.error(JSON.stringify({kind:'write',iteration:n,status:r.status,error:r.error,body:String(r.body).slice(0,1024)}));
 api202.add(r.status===202);conflicts.add(r.status===409?1:0);timeouts.add(String(r.error||'').includes('timeout')?1:0);accepted.add(r.status===202);
}
