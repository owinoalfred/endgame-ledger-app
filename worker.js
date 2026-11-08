// @ts-nocheck
/* Endgame Ledger Worker v13 — profitability: trailing stops, partial exits, tighter signals, cluster split, cooldown differentiation, SIP feed, consecutive loss tracking */
const ALPACA_BASE='https://paper-api.alpaca.markets';
const DATA_BASE='https://data.alpaca.markets';
const CRYPTO_DATA_BASE='https://data.alpaca.markets/v1beta3/crypto/us';
const MARKETS={
IAU:{symbol:'IAU',name:'iShares Gold Trust',assetClass:'us_equity',cluster:'metals'},
SLV:{symbol:'SLV',name:'iShares Silver Trust',assetClass:'us_equity',cluster:'metals'},
SPLG:{symbol:'SPLG',name:'SPDR Portfolio S&P 500 ETF',assetClass:'us_equity',cluster:'equity'},
'BTC/USD':{symbol:'BTC/USD',name:'Bitcoin',assetClass:'crypto',cluster:'crypto_large'},
'ETH/USD':{symbol:'ETH/USD',name:'Ethereum',assetClass:'crypto',cluster:'crypto_large'},
'SOL/USD':{symbol:'SOL/USD',name:'Solana',assetClass:'crypto',cluster:'crypto_alt'},
'DOGE/USD':{symbol:'DOGE/USD',name:'Dogecoin',assetClass:'crypto',cluster:'crypto_alt'},
'LINK/USD':{symbol:'LINK/USD',name:'Chainlink',assetClass:'crypto',cluster:'crypto_alt'}
};
const STRATEGY_SYMBOLS=['IAU','SLV','SPLG','BTC/USD','ETH/USD','SOL/USD','LINK/USD','DOGE/USD'];
const C={
entryTF:'15Min',regimeTF:'1Hour',macroTF:'1Day',
bars:{'15Min':260,'1Hour':260,'1Day':260},
emaFast:20,emaSlow:50,emaMacro:200,rsi:14,atr:14,adx:14,
riskPerTrade:.005,maxSingleRisk:.01,maxPortfolioRisk:.02,maxClusterRisk:.015,
maxPositionPct:.15,maxGrossExposure:.95,maxClusterExposurePct:.40,
correlationRiskWeight:.50,dailySoft:.01,dailyStop:.015,dailyEmergency:.02,
stopATR:1.5,targetR:2.0,minRR:1.5,minScore:65,minADX:15,minATRpct:5,maxATRpct:98,
maxSpread:.005,slippage:.0005,cooldown:60,lossCooldown:180,lockSeconds:300,
logLimit:1000,journalLimit:1000,maxTradesPerDay:12,reconciliationTTL:120,
apiTimeoutMs:8000,apiReadRetries:3,orderRecoveryPolls:8,orderRecoveryPollMs:350,
entryFillPolls:10,entryFillPollMs:350,protectionVerifyPolls:6,protectionVerifyPollMs:300,
cryptoStopLimitBuffer:.005,cryptoStopLimitAtrBuffer:1.25,cryptoStopLimitBufferMax:.05,
cryptoProtectionPolls:8,cryptoProtectionPollMs:400,
maxRunWallMs:25000,
trailingStopATR:1.0,breakevenR:1.0,partialExitR:.7,partialExitQty:.5,
winCooldown:60,lossCooldownVal:180,consecutiveLossCooldown:360,consecutiveLossThreshold:3
};
const cors={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,DELETE,OPTIONS','Access-Control-Allow-Headers':'Content-Type,X-App-Token'};
const json=(d,s=200)=>new Response(JSON.stringify(d),{status:s,headers:{'Content-Type':'application/json',...cors}});
const finite=x=>Number.isFinite(Number(x));
const clamp=(x,a,b)=>Math.max(a,Math.min(b,x));
const iso=()=>new Date().toISOString();
const day=()=>{
const parts=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date());
const p=Object.fromEntries(parts.map(x=>[x.type,x.value]));
return `${p.year}-${p.month}-${p.day}`;
};
const canon=s=>{if(!s)return null;if(MARKETS[s])return s;if(/^[A-Z]+USD$/.test(s))return s.slice(0,-3)+'/USD';return null;};
const market=s=>{const k=canon(s);return k?MARKETS[k]:null;};
const marketFor=s=>market(s);
const isCrypto=m=>m?.assetClass==='crypto';
const qtyRound=(q,m)=>isCrypto(m)?Math.floor(q*1e6)/1e6:Math.floor(q);

async function kvGet(env,k,f=null){if(!env.LEDGER_KV)return f;const v=await env.LEDGER_KV.get(k);return v==null?f:v;}
async function kvPut(env,k,v,ttl){if(env.LEDGER_KV){await env.LEDGER_KV.put(k,String(v),ttl?{expirationTtl:ttl}:undefined);}}
async function getJSON(env,k,f){try{const v=await kvGet(env,k,null);return v==null?f:JSON.parse(v);}catch{return f;}}
async function putJSON(env,k,v,ttl){await kvPut(env,k,JSON.stringify(v),ttl);}

async function log(env,symbol,action,note,meta={}){
for(let attempt=0;attempt<2;attempt++){
try{
const a=await getJSON(env,'bot:log',[]);
a.unshift({t:iso(),symbol,action,note,...meta});
a.splice(C.logLimit);
await putJSON(env,'bot:log',a);
return;
}catch(e){if(attempt===0)await sleepMs(1100);else console.error('log write failed',String(e));}
}
}

function headers(env){return{'APCA-API-KEY-ID':env.ALPACA_KEY_ID,'APCA-API-SECRET-KEY':env.ALPACA_SECRET_KEY};}
async function sleepMs(ms){return new Promise(resolve=>setTimeout(resolve,Math.max(0,ms)));}
function transientStatus(status){return status===408||status===425||status===429||status===500||status===502||status===503||status===504;}
function retryDelay(resp,attempt){
const base=Math.min(4000,250*Math.pow(2,attempt));
const reset=Number(resp?.headers?.['x-ratelimit-reset']||0);
const nowSec=Math.floor(Date.now()/1000);
const resetWait=reset>nowSec?Math.min(5000,Math.max(0,(reset-nowSec)*1000)):0;
const jitter=Math.floor(Math.random()*150);
return resetWait>0?Math.max(base,resetWait+jitter):base+jitter;
}
async function alpaca(url,env,init={}){
const method=String(init.method||'GET').toUpperCase();
const safeRetry=['GET','DELETE','PATCH'].includes(method);
const attempts=safeRetry?Math.max(1,+C.apiReadRetries||1):1;
let lastError=null;
for(let attempt=0;attempt<attempts;attempt++){
const controller=new AbortController();
const timeout=setTimeout(()=>controller.abort(),+C.apiTimeoutMs||8000);
try{
const r=await fetch(url,{...init,signal:init.signal||controller.signal,headers:{...headers(env),...(init.headers||{})}});
const responseHeaders=Object.fromEntries(r.headers.entries());
const bodyText=await r.text();
/** @type {any} */
let body=null;try{body=bodyText?JSON.parse(bodyText):{};}catch{body={raw:bodyText};}
const result={status:r.status,body,headers:responseHeaders,attempts:attempt+1};
clearTimeout(timeout);
if(safeRetry&&transientStatus(r.status)&&attempt<attempts-1){await sleepMs(retryDelay(result,attempt));continue;}
return result;
}catch(e){
clearTimeout(timeout);lastError=e;
if(safeRetry&&attempt<attempts-1){await sleepMs(Math.min(4000,250*Math.pow(2,attempt))+Math.floor(Math.random()*150));continue;}
return{status:598,body:{message:`Alpaca network/timeout error: ${e?.name||'Error'} ${e?.message||String(e)}`},headers:{},attempts:attempt+1};
}}
return{status:598,body:{message:`Alpaca request failed: ${lastError?.message||'unknown error'}`},headers:{},attempts:attempts};
}
async function requireOK(promise,label){const r=await promise;if(r.status>=400){throw new Error(r.body.message||r.body.error||`${label} failed (${r.status})`);}return r.body;}
async function account(env){return requireOK(alpaca(`${ALPACA_BASE}/v2/account`,env),'Account');}
async function positions(env){const b=await requireOK(alpaca(`${ALPACA_BASE}/v2/positions`,env),'Positions');return Array.isArray(b)?b:[];}
async function orders(env,status='open',limit=100){const b=await requireOK(alpaca(`${ALPACA_BASE}/v2/orders?status=${status}&limit=${limit}&direction=desc&nested=true`,env),'Orders');return Array.isArray(b)?b:[];}
async function clock(env){return requireOK(alpaca(`${ALPACA_BASE}/v2/clock`,env),'Clock');}
async function getOrderById(env,id){if(!id)return null;const r=await alpaca(`${ALPACA_BASE}/v2/orders/${encodeURIComponent(id)}?nested=true`,env);return r.status<300?r.body:null;}
async function getOrderByClientOrderId(env,clientOrderId){if(!clientOrderId)return null;const u=new URL(`${ALPACA_BASE}/v2/orders:by_client_order_id`);u.searchParams.set('client_order_id',clientOrderId);const r=await alpaca(u.toString(),env);return r.status<300?r.body:null;}
async function replaceOrder(env,id,patch){if(!id)return{status:400,body:{message:'Missing order id'}};return alpaca(`${ALPACA_BASE}/v2/orders/${encodeURIComponent(id)}`,env,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify(patch)});}
const TERMINAL_ORDER_STATUSES=new Set(['filled','canceled','rejected','expired','done_for_day','suspended']);
const ACTIVE_ORDER_STATUSES=new Set(['new','accepted','partially_filled','calculated','pending_new']);
function orderRemainingQty(o){const q=Math.abs(+o?.qty||0);const filled=Math.abs(+o?.filled_qty||0);return Math.max(0,q-filled);}
async function waitForOrder(env,id,opts={}){const polls=Math.max(1,+opts.polls||C.orderRecoveryPolls||8);const ms=Math.max(50,+opts.ms||C.orderRecoveryPollMs||350);let last=null;for(let i=0;i<polls;i++){last=await getOrderById(env,id);if(last){const s=String(last.status||'').toLowerCase();if(TERMINAL_ORDER_STATUSES.has(s))return last;}if(i<polls-1)await sleepMs(ms);}return last;}
async function submitIdempotentOrder(env,m,side,qty,type,limit,extra={}){const clientOrderId=extra.client_order_id;const first=await order(env,m,side,qty,type,limit,extra);if(first.status<400)return first;if(clientOrderId&&[408,425,429,500,502,503,504,598].includes(first.status)){for(let i=0;i<3;i++){const existing=await getOrderByClientOrderId(env,clientOrderId);if(existing)return{status:200,body:existing,recovered:true,original:first};await sleepMs(300*Math.pow(2,i));}}return first;}

function barLookbackDays(tf,limit){
const n=Math.max(1,+limit||1);
const m=/^(\d+)(Min|Hour|Day)$/.exec(tf||'');
if(!m)return Math.max(30,n*2);
const v=+m[1],unit=m[2];
if(unit==='Min')return Math.max(30,Math.ceil((n*v/390)*2.5));
if(unit==='Hour')return Math.max(120,Math.ceil((n*v/6.5)*2.5));
return Math.max(450,Math.ceil(n*v*1.75));
}
async function historicalBars(env,m,tf,limit){
const want=Math.max(1,+limit||1);const end=new Date();const start=new Date(end.getTime()-barLookbackDays(tf,want)*86400000);
const rows=[];let pageToken=null;
for(let page=0;page<5&&rows.length<want;page++){
const q=new URLSearchParams({timeframe:tf,start:start.toISOString(),end:end.toISOString(),limit:String(Math.min(10000,want-rows.length+10)),sort:'desc'});
if(pageToken)q.set('page_token',pageToken);
let b;let chunk;
if(isCrypto(m)){q.set('symbols',m.symbol);b=await requireOK(alpaca(`${CRYPTO_DATA_BASE}/bars?${q.toString()}`,env),`Crypto bars ${m.symbol}`);chunk=(b.bars||{})[m.symbol]||[];}
else{
q.set('adjustment','raw');q.set('feed','sip');
b=await requireOK(alpaca(`${DATA_BASE}/v2/stocks/${encodeURIComponent(m.symbol)}/bars?${q.toString()}`,env),`Stock SIP bars ${m.symbol}`);
chunk=b.bars||[];
}
rows.push(...chunk);pageToken=b.next_page_token||null;if(!pageToken||chunk.length===0)break;
}
const seen=new Set();
return rows.map(x=>({t:x.t,o:+x.o,h:+x.h,l:+x.l,c:+x.c,v:+x.v})).filter(x=>x.t&&[x.o,x.h,x.l,x.c].every(Number.isFinite)).sort((a,b)=>new Date(a.t).getTime()-new Date(b.t).getTime()).filter(x=>{const k=String(x.t);if(seen.has(k))return false;seen.add(k);return true;}).slice(-want);
}
async function bars(env,m,tf,limit){return historicalBars(env,m,tf,limit);}
async function quote(env,m){
if(isCrypto(m)){const b=await requireOK(alpaca(`${CRYPTO_DATA_BASE}/latest/quotes?symbols=${encodeURIComponent(m.symbol)}`,env),'Crypto quote');const q=(b.quotes||{})[m.symbol]||{};return{bid:+q.bp,ask:+q.ap,mid:(+q.bp&&+q.ap)?(+q.bp + +q.ap)/2:(+q.ap||+q.bp),ts:q.t};}
const b=await requireOK(alpaca(`${DATA_BASE}/v2/stocks/${encodeURIComponent(m.symbol)}/quotes/latest?feed=sip`,env),'Stock quote');const q=b.quote||b;return{bid:+q.bp,ask:+q.ap,mid:(+q.bp&&+q.ap)?(+q.bp + +q.ap)/2:(+q.ap||+q.bid),ts:q.t};
}

function sma(v,n){if(v.length<n)return null;return v.slice(-n).reduce((a,b)=>a+b,0)/n;}
function ema(v,n){if(v.length<n)return null;let e=v.slice(0,n).reduce((a,b)=>a+b,0)/n;const k=2/(n+1);for(let i=n;i<v.length;i++){e=v[i]*k+e*(1-k);}return e;}
function emaPrev(v,n){if(v.length<n+1)return null;return ema(v.slice(0,-1),n);}
function rsi(v,n){if(v.length<=n)return null;let g=0;let l=0;for(let i=1;i<=n;i++){const d=v[i]-v[i-1];g+=Math.max(d,0);l+=Math.max(-d,0);}g/=n;l/=n;for(let i=n+1;i<v.length;i++){const d=v[i]-v[i-1];g=(g*(n-1)+Math.max(d,0))/n;l=(l*(n-1)+Math.max(-d,0))/n;}return l===0?100:100-100/(1+g/l);}
function atr(b,n){if(b.length<n+1)return null;let a=0;for(let i=1;i<=n;i++){a+=Math.max(b[i].h-b[i].l,Math.abs(b[i].h-b[i-1].c),Math.abs(b[i].l-b[i-1].c));}a/=n;for(let i=n+1;i<b.length;i++){const tr=Math.max(b[i].h-b[i].l,Math.abs(b[i].h-b[i-1].c),Math.abs(b[i].l-b[i-1].c));a=(a*(n-1)+tr)/n;}return a;}
function adx(b,n){if(b.length<n*2+2)return null;let tr=0;let p=0;let dm=0;for(let i=1;i<=n;i++){const up=b[i].h-b[i-1].h;const dn=b[i-1].l-b[i].l;tr+=Math.max(b[i].h-b[i].l,Math.abs(b[i].h-b[i-1].c),Math.abs(b[i].l-b[i-1].c));p+=up>dn&&up>0?up:0;dm+=dn>up&&dn>0?dn:0;}let dx=[];for(let i=n+1;i<b.length;i++){const up=b[i].h-b[i-1].h;const dn=b[i-1].l-b[i].l;const t=Math.max(b[i].h-b[i].l,Math.abs(b[i].h-b[i-1].c),Math.abs(b[i].l-b[i-1].c));tr=tr-tr/n+t;p=p-p/n+(up>dn&&up>0?up:0);dm=dm-dm/n+(dn>up&&dn>0?dn:0);const plus=tr?p/tr*100:0;const minus=tr?dm/tr*100:0;dx.push(plus+minus?Math.abs(plus-minus)/(plus+minus)*100:0);}return dx.length<n?null:dx.slice(-n).reduce((a,b)=>a+b,0)/n;}
function pctRank(values,x){const a=values.filter(finite);if(!a.length||!finite(x))return null;return(a.filter(v=>v<=x).length/a.length)*100;}
function correlation(a,b){const n=Math.min(a.length,b.length);if(n<20)return null;const x=a.slice(-n);const y=b.slice(-n);const mx=x.reduce((s,v)=>s+v,0)/n;const my=y.reduce((s,v)=>s+v,0)/n;let num=0;let dx=0;let dy=0;for(let i=0;i<n;i++){const u=x[i]-mx;const v=y[i]-my;num+=u*v;dx+=u*u;dy+=v*v;}return dx&&dy?num/Math.sqrt(dx*dy):null;}
function returns(b){const r=[];for(let i=1;i<b.length;i++){r.push((b[i].c-b[i-1].c)/b[i-1].c);}return r;}

function snapshot(b){const c=b.map(x=>x.c);const a=atr(b,C.atr);const atrs=[];for(let i=Math.max(1,b.length-100);i<b.length;i++){atrs.push(Math.max(b[i].h-b[i].l,Math.abs(b[i].h-b[i-1].c),Math.abs(b[i].l-b[i-1].c)));}const vol=sma(b.map(x=>x.v||0),20);const last=b.at(-1);const prev=b.at(-2);return{price:last.c,ema20:ema(c,C.emaFast),ema50:ema(c,C.emaSlow),ema200:ema(c,C.emaMacro),prevEma20:emaPrev(c,C.emaFast),prevEma50:emaPrev(c,C.emaSlow),rsi:rsi(c,C.rsi),atr:a,atrPct:a/last.c,atrPercentile:pctRank(atrs,a),adx:adx(b,C.adx),volumeRatio:vol?(last.v||0)/vol:null,lastTime:last.t,high20:Math.max(...b.slice(-20).map(x=>x.h)),low20:Math.min(...b.slice(-20).map(x=>x.l)),prevClose:prev?.c};}
function bull(x){return(x.ema20>x.ema50&&x.ema50>x.ema200);}

function signal(e,r,m){
let score=0;const why=[];
if(bull(m)){score+=25;why.push('daily trend aligned');}else if(m.ema20>m.ema50){score+=12;why.push('daily trend positive');}
if(r.ema20>r.ema50){score+=20;why.push('hourly trend positive');}
if(e.ema20>e.ema50){score+=20;why.push('entry trend positive');}
if(e.rsi>50&&e.rsi<68){score+=15;why.push('healthy momentum');}
if(e.adx>=C.minADX){score+=10;why.push('trend strength');}
if(e.volumeRatio==null||e.volumeRatio>=1.05){score+=5;why.push('volume acceptable');}
if(e.atrPercentile==null||(e.atrPercentile>=C.minATRpct&&e.atrPercentile<=C.maxATRpct)){score+=5;why.push('volatility acceptable');}
const alignedBull=m.ema20>m.ema50&&r.ema20>r.ema50&&e.ema20>e.ema50;
const alignedBear=m.ema20<m.ema50&&r.ema20<r.ema50&&e.ema20<e.ema50;
const strongTrend=(r.adx||0)>=C.minADX;
const highVol=(e.atrPercentile||0)>=90;
const lowVol=(e.atrPercentile||0)<=10;
const regime=alignedBull?(highVol?'BULL_HIGH_VOL':strongTrend?'BULL_TREND':'BULL'):alignedBear?(highVol?'BEAR_HIGH_VOL':strongTrend?'BEAR_TREND':'BEAR'):(lowVol?'RANGE_LOW_VOL':'MIXED');
return{score,regime,direction:alignedBull?'LONG':alignedBear?'SHORT':'NEUTRAL',trendStrength:strongTrend?'STRONG':'WEAK',volatility:highVol?'HIGH':lowVol?'LOW':'NORMAL',why};
}

function plan(q,e){const entry=q.ask||q.mid||e.price;if(!finite(entry)||!finite(e.atr)||e.atr<=0)return null;const stop=entry-C.stopATR*e.atr;const target=entry+(entry-stop)*C.targetR;const risk=entry-stop;return{entry,stop,target,risk,rr:(target-entry)/risk};}

function trailingStop(plan,current,atr){
if(!plan||!finite(current)||!finite(atr)||atr<=0)return null;
const entry=+plan.entry,stop=+plan.stop,risk=Math.abs(entry-stop);
if(risk<=0)return null;
const unrealizedR=(current-entry)/risk;
if(unrealizedR>=C.breakevenR){
const breakeven=entry;
if(unrealizedR>=2.0){
const trail=current-C.trailingStopATR*atr;
return Math.max(breakeven,trail);
}
return breakeven;
}
return null;
}

async function consecutiveLosses(env,sym){return+(await kvGet(env,'consecloss:'+sym,'0'))||0;}
async function setConsecutiveLosses(env,sym,count){await kvPut(env,'consecloss:'+sym,String(count),86400);}
async function recordTradeOutcome(env,sym,wasWin){
const count=await consecutiveLosses(env,sym);
if(wasWin){await setConsecutiveLosses(env,sym,0);return 0;}
const newCount=count+1;await setConsecutiveLosses(env,sym,newCount);return newCount;
}
function exitCooldown(reason,consecLosses){
if(reason==='take-profit')return C.winCooldown;
if(reason==='stop-threshold-reached'||reason==='stop-loss'){return consecLosses>=C.consecutiveLossThreshold?C.consecutiveLossCooldown:C.lossCooldownVal;}
return C.cooldown;
}

async function analysis(env,m){
const[b15,b1,bD,q]=await Promise.all([bars(env,m,C.entryTF,C.bars[C.entryTF]),bars(env,m,C.regimeTF,C.bars[C.regimeTF]),bars(env,m,C.macroTF,C.bars[C.macroTF]),quote(env,m)]);
if(b15.length<80||b1.length<80||bD.length<C.emaMacro+10){throw new Error(`Insufficient history ${b15.length}/${b1.length}/${bD.length}`);}
const e=snapshot(b15);const r=snapshot(b1);const d=snapshot(bD);const s=signal(e,r,d);
const spread=q.bid&&q.ask?(q.ask-q.bid)/((q.ask+q.bid)/2):null;
return{symbol:m.symbol,name:m.name,assetClass:m.assetClass,cluster:m.cluster,quote:q,spreadPct:spread,entry:e,regime:r,macro:d,signal:s,plan:plan(q,e)};
}

async function cooldown(env,sym){return await getJSON(env,'cool:'+sym,null);}
async function setCooldown(env,sym,min,reason){await putJSON(env,'cool:'+sym,{until:Date.now()+min*60000,reason},min*60+120);}

async function order(env,m,side,qty,type='market',limit=null,/** @type {any} */ extra={}){
/** @type {Record<string, any>} */
const body={symbol:m.symbol,qty,side,type,time_in_force:isCrypto(m)?'gtc':'day',...extra,client_order_id:extra.client_order_id||`eg-${canon(m.symbol)||m.symbol.replace(/\W/g,'')}-${side}-${Date.now()}-${Math.random().toString(36).slice(2,8)}`,...((type==='limit'||type==='stop_limit')?{limit_price:Number(limit)}:{}),...((type==='stop'||type==='stop_limit')?{stop_price:Number(extra.stop_price)}:{})};
return alpaca(`${ALPACA_BASE}/v2/orders`,env,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
}
function cryptoPlanKey(symbol){return`crypto:plan:${canon(symbol)||symbol}`;}
async function assetInfo(env,m){const key=`asset:${canon(m.symbol)||m.symbol}`;const cached=await getJSON(env,key,null);if(cached?.data&&cached?.cachedAt&&Date.now()-cached.cachedAt<6*60*60*1000)return cached.data;const candidates=[m.symbol,m.symbol.replace('/','')];let last=null;for(const s of candidates){const r=await alpaca(`${ALPACA_BASE}/v2/assets/${encodeURIComponent(s)}`,env);if(r.status<300){const data=r.body||{};if(env.LEDGER_KV)await putJSON(env,key,{cachedAt:Date.now(),data},6*60*60);return data;}last=r;}throw new Error(`Asset metadata unavailable for ${m.symbol}: ${last?.body?.message||last?.status||'unknown error'}`);}
function decimalsForIncrement(v,max=9){const s=String(v??'').trim();if(!s)return max;if(s.includes('e-')){const[a,b]=s.toLowerCase().split('e-');return Math.min(max,(a.split('.')[1]?.length||0)+(+b||0));}return Math.min(max,(s.split('.')[1]||'').length);}
function floorToIncrement(value,increment,maxDecimals=9){const n=Number(value),inc=Number(increment);if(!finite(n)||!finite(inc)||inc<=0)return finite(n)?Number(n.toFixed(maxDecimals)):0;const units=Math.floor((n+Math.max(1,Math.abs(n))*1e-12)/inc);const d=Math.min(maxDecimals,Math.max(0,decimalsForIncrement(increment,maxDecimals)));return Number((units*inc).toFixed(d));}
function cryptoPrice(v,meta=null){const n=Number(v);if(!finite(n)||n<=0)return null;const inc=Number(meta?.price_increment||0);return inc>0?floorToIncrement(n,inc,9):Number(n.toFixed(9));}
function qtyForOrder(q,m,meta=null){const n=Number(q);if(!finite(n)||n<=0)return 0;const inc=Number(meta?.min_trade_increment||0);if(isCrypto(m)&&inc>0)return floorToIncrement(n,inc,9);return isCrypto(m)?Number(n.toFixed(9)):Math.floor(n);}
async function cancelOrderById(env,id){if(!id)return false;const r=await alpaca(`${ALPACA_BASE}/v2/orders/${encodeURIComponent(id)}`,env,{method:'DELETE'});return r.status===204||r.status===404;}
function protectionBuffer(current,atr){const base=Math.max(C.cryptoStopLimitBuffer,finite(atr)&&finite(current)&&current>0?(+atr/current)*C.cryptoStopLimitAtrBuffer:0);return clamp(base,C.cryptoStopLimitBuffer,C.cryptoStopLimitBufferMax);}
function activeProtectionOrders(p,open){const sym=positionKey(p);return orderAndLegs(open).filter(o=>positionKey(o)===sym&&String(o.side||'').toLowerCase()==='sell'&&['stop','stop_limit'].includes(String(o.type||'').toLowerCase())&&finite(stopFromOrder(o))&&+stopFromOrder(o)>0&&orderRemainingQty(o)>0&&ACTIVE_ORDER_STATUSES.has(String(o.status||'').toLowerCase()));}
function protectionCoverage(p,open){return activeProtectionOrders(p,open).reduce((s,o)=>s+orderRemainingQty(o),0);}
async function placeCryptoProtection(env,m,qty,stop,ctx={}){
const meta=ctx.meta||await assetInfo(env,m);const nQty=qtyForOrder(qty,m,meta);const current=+ctx.current||0;const atr=+ctx.atr||0;const s=cryptoPrice(stop,meta);
if(!s||nQty<=0)return{status:422,body:{message:'Invalid crypto stop price or quantity'}};
const buffer=protectionBuffer(current,atr);let limit=cryptoPrice(s*(1-buffer),meta);const inc=Number(meta?.price_increment||0);
if(limit==null||limit>=s){limit=inc>0?floorToIncrement(s-inc,inc,9):Number((s*(1-C.cryptoStopLimitBuffer)).toFixed(9));}
if(!limit||limit>=s)return{status:422,body:{message:'Invalid crypto stop-limit prices after asset tick normalization'}};
const clientOrderId=ctx.clientOrderId||`eg-protect-${m.symbol.replace(/\W/g,'')}-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
const r=await submitIdempotentOrder(env,m,'sell',nQty,'stop_limit',limit,{stop_price:s,client_order_id:clientOrderId});
if(r.status>=300)return r;const id=r.body?.id;const checked=id?await waitForOrder(env,id,{polls:C.protectionVerifyPolls,ms:C.protectionVerifyPollMs}):null;
const status=String(checked?.status||r.body?.status||'').toLowerCase();const ok=!!checked&&(ACTIVE_ORDER_STATUSES.has(status)||status==='new'||status==='accepted');
if(!ok){return{status:409,body:{message:`Crypto protection order was not verified as active (${status||'unknown'})`,order:checked||r.body}};}
return{...r,body:{...r.body,verification:checked,normalizedQty:nQty,normalizedStop:s,normalizedLimit:limit}};
}
async function waitForPosition(env,symbol,tries=C.cryptoProtectionPolls){for(let i=0;i<tries;i++){const pos=await positions(env);const p=pos.find(x=>positionKey(x)===symbol);if(p&&Math.abs(+p.qty||0)>0)return p;if(i<tries-1)await sleepMs(C.cryptoProtectionPollMs);}return null;}
async function storeCryptoPlan(env,symbol,plan){await putJSON(env,cryptoPlanKey(symbol),plan,60*60*24*14);}
async function clearCryptoPlan(env,symbol){if(env.LEDGER_KV)await env.LEDGER_KV.delete(cryptoPlanKey(symbol));}
async function cancelProtectionSet(env,p,open){let canceled=0;for(const o of activeProtectionOrders(p,open)){if(await cancelOrderById(env,o.id))canceled++;}return canceled;}
async function reestablishCryptoProtection(env,m,position,plan,current,atr=0){
const fresh=(await positions(env)).find(x=>positionKey(x)===m.symbol)||position;const qty=Math.abs(+fresh?.qty||0);if(!qty)return{status:200,body:{status:'already_flat'}};
const entry=Math.max(0,+fresh?.avg_entry_price||0);const live=+current||+fresh?.current_price||entry;
const fallbackRisk=Math.max(Math.abs(live-(+plan?.stop||0)),live*0.01,Math.abs(atr||0));
let stop=+plan?.stop||0;if(!(stop>0&&stop<live)){stop=live-Math.max(fallbackRisk,live*0.01);}
const asset=await assetInfo(env,m);stop=cryptoPrice(stop,asset);
for(let i=0;i<3;i++){const pr=await placeCryptoProtection(env,m,qty,stop,{meta:asset,current:live,atr,clientOrderId:`eg-reprotect-${m.symbol.replace(/\W/g,'')}-${Date.now()}`}).catch(e=>({status:598,body:{message:String(e)}}));if(pr.status<300)return pr;await sleepMs(300*Math.pow(2,i));}
return{status:409,body:{message:`${m.symbol} could not be re-protected after failed flatten`}};
}
async function flattenCrypto(env,m,qty,open=[],reason='flatten',plan=null,atr=0,current=0){
const currentPos=(await positions(env)).find(x=>positionKey(x)===m.symbol);const nQty=Math.abs(+currentPos?.qty||+qty||0);if(!nQty)return{status:200,body:{status:'already_flat'}};
const hadProtection=await cancelProtectionSet(env,currentPos||{symbol:m.symbol,qty:nQty},open).catch(()=>false);
const clientOrderId=`eg-crypto-flatten-${m.symbol.replace(/\W/g,'')}-${Date.now()}`;
const r=await submitIdempotentOrder(env,m,'sell',nQty,'market',null,{client_order_id:clientOrderId});
if(r.status>=300){if(hadProtection||plan){const fresh=(await positions(env)).find(x=>positionKey(x)===m.symbol);if(fresh){await reestablishCryptoProtection(env,m,fresh,plan||{},currentPos?.current_price||current,atr).catch(()=>{});}}return r;}
if(r.body?.id)await waitForOrder(env,r.body.id,{polls:6,ms:300}).catch(()=>{});
const remaining=(await positions(env)).find(x=>positionKey(x)===m.symbol);
if(remaining&&Math.abs(+remaining.qty||0)>0){const reprotected=await reestablishCryptoProtection(env,m,remaining,plan||{},current||remaining.current_price,atr).catch(e=>({status:598,body:{message:String(e)}}));return{status:409,body:{message:`${reason}: flatten order did not fully close ${m.symbol}`,remainingQty:Math.abs(+remaining.qty||0),order:r.body,reprotected:reprotected?.status<300}};}
await clearCryptoPlan(env,m.symbol);return r;
}
async function partialFlattenCrypto(env,m,qty,open,plan,atr,current,fraction){
const partialQty=qtyRound(qty*fraction,m);if(partialQty<=0)return{status:200,body:{status:'partial_qty_zero'}};
const currentPos=(await positions(env)).find(x=>positionKey(x)===m.symbol);const remaining=Math.abs(+currentPos?.qty||0)-partialQty;
if(remaining<=0)return{status:200,body:{status:'already_small_enough'}};
const clientOrderId=`eg-crypto-partial-${m.symbol.replace(/\W/g,'')}-${Date.now()}`;
const r=await submitIdempotentOrder(env,m,'sell',partialQty,'market',null,{client_order_id:clientOrderId});
if(r.status>=300)return r;
if(r.body?.id)await waitForOrder(env,r.body.id,{polls:6,ms:300}).catch(()=>{});
if(plan){plan.qty=remaining;await storeCryptoPlan(env,m.symbol,plan);}
return r;
}
async function protectedCryptoEntry(env,m,qty,stop,target,meta={}){
const asset=await assetInfo(env,m);const nQty=qtyForOrder(qty,m,asset);if(nQty<=0)return{status:422,body:{message:'Entry quantity is below the crypto trade increment'}};
const clientOrderId=meta.clientOrderId||`eg-crypto-${m.symbol.replace(/\W/g,'')}-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
const entryOrder=await submitIdempotentOrder(env,m,'buy',nQty,'market',null,{client_order_id:clientOrderId});
if(entryOrder.status>=300)return entryOrder;
let observed=entryOrder.body?.id?await waitForOrder(env,entryOrder.body.id,{polls:C.entryFillPolls,ms:C.entryFillPollMs}):null;
const entryStatus=String(observed?.status||entryOrder.body?.status||'').toLowerCase();
if(observed&&!['filled','canceled','rejected','expired','done_for_day'].includes(entryStatus)){await cancelOrderById(env,entryOrder.body?.id).catch(()=>false);if(entryOrder.body?.id){observed=await waitForOrder(env,entryOrder.body.id,{polls:5,ms:250});}}
const p=await waitForPosition(env,m.symbol);
if(!p){if(['canceled','rejected','expired','done_for_day'].includes(entryStatus)){return{status:409,body:{message:`Crypto BUY did not create a position (${entryStatus})`}};}await kvPut(env,'reconcile:halt','1',300);return{status:409,body:{message:'Crypto BUY state could not be reconciled after submission'}};}
const actualEntry=+p.avg_entry_price||+observed?.filled_avg_price||+entryOrder.body?.filled_avg_price||meta.expectedEntry||0;
const current=+p.current_price||actualEntry;const atr=+meta.atr||0;
const originalRisk=Math.abs(+stop-(+meta.expectedEntry||actualEntry));const fallbackRisk=Math.max(originalRisk,Math.abs(+stop-actualEntry),actualEntry*0.01);
let actualStop=+stop,actualTarget=+target;
if(!(actualStop<actualEntry&&actualTarget>actualEntry)){actualStop=actualEntry-fallbackRisk;actualTarget=actualEntry+fallbackRisk*C.targetR;}
actualStop=cryptoPrice(actualStop,asset);actualTarget=cryptoPrice(actualTarget,asset);
let protection=null;for(let i=0;i<3;i++){protection=await placeCryptoProtection(env,m,Math.abs(+p.qty||nQty),actualStop,{meta:asset,current,atr,clientOrderId:`${clientOrderId}-protect`}).catch(e=>({status:598,body:{message:String(e)}}));if(protection.status<300)break;await sleepMs(300*Math.pow(2,i));}
if(!protection||protection.status>=300){const[freshPos,freshOpen]=await Promise.all([positions(env),orders(env,'open')]);const still=freshPos.find(x=>positionKey(x)===m.symbol);if(still){const flattened=await flattenCrypto(env,m,Math.abs(+still.qty||nQty),freshOpen,'entry protection failure',{stop:actualStop,target:actualTarget},atr,current).catch(e=>({status:598,body:{message:String(e)}}));await kvPut(env,'reconcile:halt','1',300);return{status:409,body:{message:'Crypto BUY filled but protection could not be verified; emergency flatten attempted',protection:protection?.body||protection?.status,flatten:flattened?.body||flattened?.status}};}}
const plan={symbol:m.symbol,qty:Math.abs(+p.qty||nQty),entry:actualEntry,stop:actualStop,target:actualTarget,highestPrice:actualStop,stopOrderId:protection.body?.id||null,stopOrderIds:protection.body?.id?[protection.body.id]:[],createdAt:iso(),targetMode:'worker-managed-oco',protectionBuffer:protection.body?.normalizedLimit?(actualStop-protection.body.normalizedLimit)/actualStop:C.cryptoStopLimitBuffer};
await storeCryptoPlan(env,m.symbol,plan);
return{status:entryOrder.status<300?entryOrder.status:200,body:{...entryOrder.body,protection:{stop:actualStop,target:actualTarget,stopOrderId:plan.stopOrderId,mode:'synthetic-oco',verification:protection.body?.verification||null}}};
}
async function manageCryptoPosition(env,m,p,a,open){
const qty=Math.abs(+p.qty||0);if(!qty)return{action:'none'};
const q=await quote(env,m);const current=+q.bid||+q.mid||+p.current_price||0;const asset=await assetInfo(env,m);
let plan=await getJSON(env,cryptoPlanKey(m.symbol),null);
if(!plan){const entry=+p.avg_entry_price||current;const atr=Math.max(+a.entry?.atr||0,entry*0.005);const derivedStop=Math.min(entry-C.stopATR*atr,current-C.stopATR*atr*0.5);const risk=Math.max(entry-derivedStop,entry*0.005);plan={symbol:m.symbol,qty,entry,stop:cryptoPrice(derivedStop,asset),target:cryptoPrice(entry+risk*C.targetR,asset),highestPrice:current,createdAt:iso(),targetMode:'worker-managed-oco'};}
if(current>(+plan.highestPrice||0)){plan.highestPrice=current;}
const stops=activeProtectionOrders(p,open);const covered=stops.reduce((s,o)=>s+orderRemainingQty(o),0);
const risk=Math.abs(+plan.entry-+plan.stop)||Math.abs(+plan.entry)*0.005;
const unrealizedR=risk>0?(current-+plan.entry)/risk:0;
const trailStop=trailingStop(plan,current,a.entry?.atr||0);
if(trailStop!=null&&trailStop>+plan.stop){
const oldStops=activeProtectionOrders(p,open);
for(const o of oldStops){await cancelOrderById(env,o.id).catch(()=>false);}
const newStop=cryptoPrice(trailStop,asset);
let protection=null;for(let i=0;i<2;i++){protection=await placeCryptoProtection(env,m,qty,newStop,{meta:asset,current,atr:a.entry?.atr,clientOrderId:`eg-trail-${m.symbol.replace(/\W/g,'')}-${Date.now()}-${i}`}).catch(e=>({status:598,body:{message:String(e)}}));if(protection.status<300)break;await sleepMs(300*Math.pow(2,i));}
if(protection?.status<300){plan.stop=newStop;plan.stopOrderId=protection.body?.id||plan.stopOrderId;plan.stopOrderIds=protection.body?.id?[protection.body.id]:plan.stopOrderIds;await storeCryptoPlan(env,m.symbol,plan);await log(env,m.symbol,'TRAIL',`Trailing stop tightened to ${newStop} (unrealized: ${unrealizedR.toFixed(2)}R)`,{state:'TRAILING_STOP',unrealizedR:unrealizedR.toFixed(2)});}
}
if(current>0&&current>=+plan.target){
const exit=await flattenCrypto(env,m,qty,open,'take-profit',plan,a.entry?.atr||0,current).catch(e=>({status:598,body:{message:String(e)}}));
if(exit.status<300){await recordTradeOutcome(env,m.symbol,true);const cl=await consecutiveLosses(env,m.symbol);const cd=exitCooldown('take-profit',cl);await setCooldown(env,m.symbol,cd,'take-profit');return{action:'sell',reason:'take-profit',order:exit,cooldown:cd};}
await kvPut(env,'reconcile:halt','1',300);return{action:'error',reason:`Take-profit failed while managing ${m.symbol}: ${exit.body?.message||exit.status}`};
}
if(current>0&&current<=+plan.stop&&!covered){
const exit=await flattenCrypto(env,m,qty,open,'stop-threshold',plan,a.entry?.atr||0,current).catch(e=>({status:598,body:{message:String(e)}}));
if(exit.status<300){await recordTradeOutcome(env,m.symbol,false);const cl=await consecutiveLosses(env,m.symbol);const cd=exitCooldown('stop-threshold-reached',cl);await setCooldown(env,m.symbol,cd,'stop-loss');return{action:'sell',reason:'stop-threshold-reached',order:exit,cooldown:cd};}
await kvPut(env,'reconcile:halt','1',300);return{action:'error',reason:`Stop fallback failed for ${m.symbol}: ${exit.body?.message||exit.status}`};
}
const bearishCount=(a.entry.ema20<a.entry.ema50?1:0)+(a.regime.ema20<a.regime.ema50?1:0)+(a.macro.ema20<a.macro.ema50?1:0);
if(bearishCount>=2&&unrealizedR>=C.partialExitR&&qty>=2){
const partial=await partialFlattenCrypto(env,m,qty,open,plan,a.entry?.atr||0,current,C.partialExitQty).catch(e=>({status:598,body:{message:String(e)}}));
if(partial.status<300){await log(env,m.symbol,'SELL',`Partial exit ${(C.partialExitQty*100).toFixed(0)}% at ${unrealizedR.toFixed(2)}R — ${bearishCount}/3 timeframes bearish`,{state:'PARTIAL_EXIT',unrealizedR:unrealizedR.toFixed(2)});}
}
const missing=Math.max(0,qty-covered);
if(missing>0){let protection=null;for(let i=0;i<3;i++){protection=await placeCryptoProtection(env,m,missing,plan.stop,{meta:asset,current,atr:a.entry?.atr,clientOrderId:`eg-protect-${m.symbol.replace(/\W/g,'')}-${Date.now()}-${i}`}).catch(e=>({status:598,body:{message:String(e)}}));if(protection.status<300)break;await sleepMs(300*Math.pow(2,i));}
if(!protection||protection.status>=300){await kvPut(env,'reconcile:halt','1',300);return{action:'error',reason:`Could not attach enough crypto stop coverage: ${protection?.body?.message||protection?.status}`};}
plan.qty=qty;plan.stopOrderId=protection.body?.id||plan.stopOrderId||null;plan.stopOrderIds=[...(plan.stopOrderIds||[]),...(protection.body?.id?[protection.body.id]:[])];await storeCryptoPlan(env,m.symbol,plan);return{action:'protect',reason:'automatic-stop-attached',order:protection,plan};}
plan.qty=qty;plan.stopOrderIds=stops.map(o=>o.id).filter(Boolean);plan.stopOrderId=plan.stopOrderIds[0]||plan.stopOrderId||null;await storeCryptoPlan(env,m.symbol,plan);
const exit=a.signal.direction==='SHORT'||(a.entry.ema20<a.entry.ema50&&a.regime.ema20<a.regime.ema50);
if(exit){
const flattened=await flattenCrypto(env,m,qty,open,'trend-exit',plan,a.entry?.atr||0,current).catch(e=>({status:598,body:{message:String(e)}}));
if(flattened.status<300){const wasWin=current>+plan.entry;await recordTradeOutcome(env,m.symbol,wasWin);const cl=await consecutiveLosses(env,m.symbol);const cd=exitCooldown('trend-exit',cl);await setCooldown(env,m.symbol,cd,'trend-exit');return{action:'sell',reason:'trend-exit',order:flattened,cooldown:cd};}
await kvPut(env,'reconcile:halt','1',300);return{action:'error',reason:`Trend exit failed for ${m.symbol}: ${flattened.body?.message||flattened.status}`};
}
return{action:'hold',reason:'crypto-position-managed',plan,coverage:covered};
}
async function bracket(env,m,qty,stop,target,clientOrderId=null){
if(isCrypto(m)){return{status:422,body:{message:'Protected crypto entries are disabled: Alpaca cannot provide the atomic protection required by this strategy.'}};}
const tp=Number(target.toFixed(2));const sl=Number(stop.toFixed(2));
const q=await quote(env,m);const base=+q.ask||+q.mid||+q.bid||0;
if(!finite(base)||!finite(tp)||!finite(sl)||!(sl<base&&tp>base)){return{status:422,body:{message:'Bracket protection is not valid at the current market price after tick-size rounding'}};}
if(Math.abs(tp-base)<0.01){return{status:422,body:{message:'Take-profit must be at least $0.01 above the current market price'}};}
if(Math.abs(base-sl)<0.01){return{status:422,body:{message:'Stop-loss must be at least $0.01 below the current market price'}};}
return order(env,m,'buy',qty,'market',null,{order_class:'bracket',take_profit:{limit_price:tp.toFixed(2)},stop_loss:{stop_price:sl.toFixed(2)},client_order_id:clientOrderId||`eg-${m.symbol}-${Date.now()}-${Math.random().toString(36).slice(2,8)}`});
}

async function dayRisk(env,acct){
const k=day();const sk='daystart:'+k;let start=+(await kvGet(env,sk,''));
if(!start){start=+acct.equity;await kvPut(env,sk,start,172800);}
const equity=+acct.equity||0;const peakKey='daypeak:'+k;let peak=+(await kvGet(env,peakKey,''));
if(!peak||equity>peak){peak=equity;await kvPut(env,peakKey,peak,172800);}
const dd=start?(start-equity)/start:0;const peakDD=peak?(peak-equity)/peak:0;
const level=dd>=C.dailyEmergency?3:dd>=C.dailyStop?2:dd>=C.dailySoft?1:0;
return{start,dd,peak,peakDD,level};
}
async function tradeCount(env){return+(await kvGet(env,'trades:'+day(),'0'))||0;}
async function incTradeCount(env){const n=await tradeCount(env)+1;await kvPut(env,'trades:'+day(),n,172800);return n;}

function exposure(pos){return pos.reduce((s,p)=>s+Math.abs(+p.market_value||0),0);}
function positionKey(p){return(canon(p.symbol)||p.symbol);}
function openRiskFromOrders(pos,open){const r={};for(const p of pos){const sym=positionKey(p);const children=open.filter(o=>positionKey(o)===sym&&(o.stop_price||o.stop_loss));const stop=children.find(o=>o.stop_price)?.stop_price||children.find(o=>o.stop_loss)?.stop_loss?.stop_price;r[sym]=stop&&finite(stop)?Math.abs(+p.qty*(+p.current_price||+p.avg_entry_price)-+p.qty*+stop):0;}return r;}

async function correlationMatrix(env,{force=false}={}){
if(!force){const cached=await getJSON(env,'corr:matrix',null);if(cached?.symbols?.length&&cached?.matrix)return cached;}
const syms=STRATEGY_SYMBOLS;const pairs=await Promise.all(syms.map(async s=>{try{return[s,returns(await bars(env,MARKETS[s],'1Hour',120))];}catch{return[s,[]];}}));
const ret=Object.fromEntries(pairs);const result={symbols:syms,matrix:syms.map(a=>syms.map(b=>a===b?1:correlation(ret[a]||[],ret[b]||[])))};
let highVol=false;for(const s of syms){try{const b=await bars(env,MARKETS[s],C.entryTF,50);const snap=snapshot(b);if(snap.atrPercentile!=null&&snap.atrPercentile>80){highVol=true;break;}}catch{}}
const ttl=highVol?120:300;
await putJSON(env,'corr:matrix',result,ttl);return result;
}

function stopFromOrder(o){return o?.stop_price||o?.stop_loss?.stop_price||o?.stop_loss?.stop_price_limit||null;}
function orderRisk(o){const qty=Math.abs(+o?.qty||0);const stop=+stopFromOrder(o);const px=+o?.limit_price||+o?.filled_avg_price||0;if(!qty||!finite(px))return 0;return finite(stop)&&stop>0?qty*Math.abs(px-stop):qty*px;}
function proposedPortfolioRisk(book,symbol,proposedRisk,corr){
let risk=book.correlationAdjustedRisk+proposedRisk;
if(corr?.symbols&&corr?.matrix){const i=corr.symbols.indexOf(symbol);const proposedCluster=marketFor(symbol)?.cluster;if(i>=0&&proposedCluster){for(const[sym,v]of Object.entries(book.bySymbol)){if(v.cluster!==proposedCluster||!v.risk)continue;const j=corr.symbols.indexOf(sym);if(j>=0){const rho=Math.max(0,Math.min(1,Math.abs(+corr.matrix[i][j]||0)));risk+=Math.min(proposedRisk,v.risk)*rho*C.correlationRiskWeight;}}}}
return risk;
}
function proposedClusterRisk(book,symbol,cluster,proposedRisk,corr){
let risk=(book.byCluster[cluster]||0)+proposedRisk;
if(corr?.symbols&&corr?.matrix){const i=corr.symbols.indexOf(symbol);if(i>=0){for(const[sym,v]of Object.entries(book.bySymbol)){if(v.cluster!==cluster||!v.risk)continue;const j=corr.symbols.indexOf(sym);if(j>=0){const rho=Math.max(0,Math.min(1,Math.abs(+corr.matrix[i][j]||0)));risk+=Math.min(proposedRisk,v.risk)*rho*C.correlationRiskWeight;}}}}
return risk;
}
function orderAndLegs(open){const out=[];for(const o of open||[]){out.push(o);if(Array.isArray(o.legs))out.push(...o.legs);}return out;}
function protectiveStopForPosition(p,open){const stops=activeProtectionOrders(p,open);const needed=Math.abs(+p.qty||0);if(!needed||!stops.length)return null;const covered=stops.reduce((s,o)=>s+orderRemainingQty(o),0);if(covered+1e-9<needed)return null;return[...stops].sort((a,b)=>+stopFromOrder(a)-+stopFromOrder(b))[0]||null;}
function pendingBuyProtected(o){const needed=orderRemainingQty(o);if(!needed)return true;const stops=orderAndLegs([o]).filter(x=>positionKey(x)===positionKey(o)&&String(x.side||'').toLowerCase()==='sell'&&finite(stopFromOrder(x))&&+stopFromOrder(x)>0&&orderRemainingQty(x)>0&&ACTIVE_ORDER_STATUSES.has(String(x.status||'').toLowerCase()));return stops.reduce((s,x)=>s+orderRemainingQty(x),0)+1e-9>=needed;}
function riskBook(pos,open,corr=null){
const bySymbol={},byCluster={};let totalRisk=0,gross=0,unprotected=0;
for(const p of pos){const sym=positionKey(p),m=marketFor(sym),mv=Math.abs(+p.market_value||0);
if(!m){bySymbol[sym]={risk:0,notional:mv,protected:false,stop:null,protectedQty:0,cluster:'off-universe'};gross+=mv;continue;}
gross+=mv;
const stopOrder=protectiveStopForPosition(p,open);const stops=activeProtectionOrders(p,open);
const stop=stops.length?Math.min(...stops.map(x=>+stopFromOrder(x)).filter(Number.isFinite)):null;
const qty=Math.abs(+p.qty||0),px=+p.current_price||+p.avg_entry_price||0;let risk=0;
if(qty&&stop&&px)risk=qty*Math.abs(px-stop);else if(mv){risk=mv;unprotected++;}
const cluster=m.cluster;
bySymbol[sym]={risk,notional:mv,protected:!!stopOrder,stop:stop?+stop:null,protectedQty:stops.reduce((s,o)=>s+orderRemainingQty(o),0),cluster};
byCluster[cluster]=(byCluster[cluster]||0)+risk;totalRisk+=risk;
}
for(const o of open){if(String(o.side||'').toLowerCase()!=='buy')continue;const sym=positionKey(o);if(pos.some(p=>positionKey(p)===sym))continue;const m=marketFor(sym),r=orderRisk(o);const pendingPx=+o.limit_price||+o.filled_avg_price||0;const pendingQty=orderRemainingQty(o);const pendingNotional=pendingQty*pendingPx;if(pendingNotional)gross+=pendingNotional;const protectedBuy=pendingBuyProtected(o);if(!protectedBuy)unprotected++;if(!r&&!pendingNotional){bySymbol[sym]={risk:0,notional:0,protected:protectedBuy,cluster:m?.cluster||'unknown'};continue;}totalRisk+=r;const cluster=m?.cluster||'unknown';byCluster[cluster]=(byCluster[cluster]||0)+r;bySymbol[sym]={risk:r,notional:pendingNotional,protected:protectedBuy,cluster};}
let correlationAdj=0;
if(corr?.symbols&&corr?.matrix){for(let i=0;i<corr.symbols.length;i++){for(let j=i+1;j<corr.symbols.length;j++){const a=corr.symbols[i],b=corr.symbols[j];const ra=bySymbol[a]?.risk||0,rb=bySymbol[b]?.risk||0;const ca=bySymbol[a]?.cluster,cb=bySymbol[b]?.cluster;if(ra>0&&rb>0&&ca&&ca===cb){const rho=Math.max(0,Math.min(1,Math.abs(+corr.matrix[i][j]||0)));correlationAdj+=Math.min(ra,rb)*rho*C.correlationRiskWeight;}}}}
return{totalRisk,correlationAdjustedRisk:totalRisk+correlationAdj,correlationAdj,gross,bySymbol,byCluster,unprotected};
}
function reserveRiskBook(book,symbol,market,notional,risk,corr=null){
const cluster=market?.cluster||'unknown';const bySymbol={...book.bySymbol};
bySymbol[symbol]={risk:Math.max(0,+risk||0),notional:Math.max(0,+notional||0),protected:true,stop:null,protectedQty:0,cluster};
const byCluster={...book.byCluster};byCluster[cluster]=(byCluster[cluster]||0)+Math.max(0,+risk||0);
let correlationAdj=0;
if(corr?.symbols&&corr?.matrix){for(let i=0;i<corr.symbols.length;i++){for(let j=i+1;j<corr.symbols.length;j++){const a=corr.symbols[i],b=corr.symbols[j];const ra=bySymbol[a]?.risk||0,rb=bySymbol[b]?.risk||0;const ca=bySymbol[a]?.cluster,cb=bySymbol[b]?.cluster;if(ra>0&&rb>0&&ca&&ca===cb){const rho=Math.max(0,Math.min(1,Math.abs(+corr.matrix[i][j]||0)));correlationAdj+=Math.min(ra,rb)*rho*C.correlationRiskWeight;}}}}
const totalRisk=(book.totalRisk||0)+Math.max(0,+risk||0);
return{...book,totalRisk,correlationAdjustedRisk:totalRisk+correlationAdj,correlationAdj,gross:(book.gross||0)+Math.max(0,+notional||0),bySymbol,byCluster};
}
async function reconcile(env){
const[acct,pos,open]=await Promise.all([account(env),positions(env),orders(env,'open')]);
const issues=[],advisories=[],positionSymbols=new Set(pos.map(positionKey));
for(const p of pos){const sym=positionKey(p),m=marketFor(sym);const hasStop=!!protectiveStopForPosition(p,open);
if(!m){advisories.push(`${sym} is outside the Endgame strategy universe — not managed or protected by the bot`);continue;}
if(!hasStop&&m){if(!isCrypto(m))issues.push(`${sym} has no active protective stop order`);else advisories.push(`${sym} is an unprotected crypto position (no atomic bracket support) — monitor manually`);}}
for(const o of open){if(o.side==='buy'&&!positionSymbols.has(positionKey(o))&&!pendingBuyProtected(o))issues.push(`${positionKey(o)} has an unprotected pending BUY`);}
const snapshot={equity:+acct.equity||0,positions:pos.map(p=>({symbol:positionKey(p),qty:+p.qty||0})).sort((a,b)=>a.symbol.localeCompare(b.symbol)),openOrders:open.map(o=>({id:o.id,symbol:positionKey(o),qty:+o.qty||0,status:o.status})).sort((a,b)=>String(a.id).localeCompare(String(b.id))),at:iso()};
const previous=await getJSON(env,'reconcile:last',null);const stateChanged=previous?JSON.stringify({...previous,at:undefined})!==JSON.stringify({...snapshot,at:undefined}):false;
await putJSON(env,'reconcile:last',snapshot,3600);const healthy=issues.length===0;
await kvPut(env,'reconcile:halt',healthy?'0':'1',300);
return{healthy,issues,advisories,stateChanged,snapshot,account:acct,positions:pos,openOrders:open};
}
async function getRiskState(env,corr=null){const[acct,pos,open]=await Promise.all([account(env),positions(env),orders(env,'open')]);const book=riskBook(pos,open,corr),equity=+acct.equity||0;const clusterLimits=Object.fromEntries(Object.entries(book.byCluster).map(([k,v])=>[k,{risk:v,pct:equity?v/equity:0,limit:C.maxClusterRisk}]));return{account:acct,positions:pos,openOrders:open,book,clusterLimits};}

async function verifyBracketProtection(env,orderId){
if(!orderId)return false;for(let i=0;i<3;i++){try{const r=await alpaca(`${ALPACA_BASE}/v2/orders/${orderId}?nested=true`,env);if(r.status<300){const legs=r.body?.legs||[];const parentQty=Math.abs(+r.body?.qty||0);const validSellLeg=x=>String(x.side||'').toLowerCase()==='sell'&&Math.abs(+x.qty||0)+1e-9>=parentQty;const hasStop=legs.some(x=>validSellLeg(x)&&!!stopFromOrder(x));const hasTarget=legs.some(x=>validSellLeg(x)&&!!x.limit_price&&x.order_type==='limit');if(parentQty>0&&hasStop&&hasTarget)return true;}}catch{}await new Promise(resolve=>setTimeout(resolve,250));}return false;
}

async function flattenOffUniverse(env,pos){
const off=pos.filter(p=>!marketFor(positionKey(p)));
let flattened=0;
for(const p of off){
const sym=p.symbol;const qty=Math.abs(+p.qty||0);if(!qty)continue;
try{
const r=await alpaca(`${ALPACA_BASE}/v2/orders`,env,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:sym,qty,side:'sell',type:'market',time_in_force:'gtc',client_order_id:`eg-cleanup-${sym.replace(/\W/g,'')}-${Date.now()}`})});
if(r.status<300){flattened++;await log(env,sym,'SELL',`Off-universe position flattened by cleanup (${qty} shares)`,{state:'OFF_UNIVERSE_CLEANUP'});}
else{await log(env,sym,'ERROR',`Off-universe cleanup failed: ${r.body?.message||r.status}`,{state:'CLEANUP_FAILED'});}
}catch(e){await log(env,sym,'ERROR',`Off-universe cleanup exception: ${String(e)}`,{state:'CLEANUP_FAILED'});}
}
return flattened;
}

async function runUnlocked(env){
const startedAt=Date.now();await putJSON(env,'bot:lastRun',{state:'running',startedAt:iso(),trigger:'autopilot'},86400).catch(()=>{});
if(!env.LEDGER_KV||!env.ALPACA_KEY_ID||!env.ALPACA_SECRET_KEY)return;
if((await kvGet(env,'kill','0'))==='1'){await putJSON(env,'bot:lastRun',{state:'kill',startedAt:iso(),finishedAt:iso(),trigger:'autopilot'},86400).catch(()=>{});return;}
const errors=[];let recon;
try{recon=await reconcile(env);}catch(e){errors.push(`reconciliation: ${String(e)}`);await kvPut(env,'reconcile:halt','1',300).catch(()=>{});await putJSON(env,'bot:lastRun',{state:'reconciliation-error',startedAt:iso(),finishedAt:iso(),errors},86400).catch(()=>{});return;}

const offUniverse=recon.positions.filter(p=>!marketFor(positionKey(p)));
if(offUniverse.length){await log(env,'SYSTEM','CLEANUP',`${offUniverse.length} off-universe position(s) detected: ${offUniverse.map(p=>p.symbol).join(', ')}`,{state:'OFF_UNIVERSE_DETECTED'});const cleaned=await flattenOffUniverse(env,offUniverse);if(cleaned>0){await log(env,'SYSTEM','CLEANUP',`Flattened ${cleaned} off-universe position(s)`,{state:'OFF_UNIVERSE_CLEANED'});}}

let acct=recon.account;let pos=recon.positions;let open=recon.openOrders;
const clk=await clock(env).catch(()=>({is_open:false}));
const runSymbols=[...new Set([...STRATEGY_SYMBOLS,...pos.map(positionKey).filter(s=>MARKETS[s]&&!STRATEGY_SYMBOLS.includes(s))])];
for(const s of runSymbols){const m=MARKETS[s];const p=pos.find(x=>positionKey(x)===s);if(!p)continue;
try{
if(isCrypto(m)){const a=await analysis(env,m);const managed=await manageCryptoPosition(env,m,p,a,open);if(managed.action==='sell'&&managed.order?.status<300){await log(env,s,'SELL',`Crypto exit: ${managed.reason}`,{state:'EXIT_SUBMITTED',cooldown:managed.cooldown});await incTradeCount(env);}else if(managed.action==='protect'){await log(env,s,'PROTECT',`Automatic crypto stop attached; protected quantity ${managed.plan?.qty||p.qty}`,{state:'PROTECTION_VERIFIED'});}else if(managed.action==='error'){await log(env,s,'HALT',managed.reason,{state:'CRYPTO_PROTECTION_ERROR'});}else if(managed.action==='hold'){await log(env,s,'HOLD',`Score ${a.signal.score}/100 · ${a.signal.regime} · RSI ${a.entry.rsi?.toFixed(1)} · ADX ${a.entry.adx?.toFixed(1)} · crypto stop ${managed.plan?.stop} · target ${managed.plan?.target} · trail: ${managed.plan?.highestPrice||'N/A'}`,{state:'POSITION_ACTIVE'});}}
else{const stop=protectiveStopForPosition(p,open);if(!stop){await log(env,s,'HALT','Existing equity position has no verified protective stop',{state:'PROTECTION_UNVERIFIED'});}if(clk.is_open){const a=await analysis(env,m);
const entry=+p.avg_entry_price||0;const currentPrice=+p.current_price||+a.entry?.price||0;const stopPrice=stop?+stopFromOrder(stop):0;
const risk=Math.abs(entry-stopPrice)||Math.abs(entry)*0.005;const unrealizedR=risk>0?(currentPrice-entry)/risk:0;
if(unrealizedR>=C.breakevenR&&stopPrice<entry){
const trailStop=unrealizedR>=2.0?Math.max(entry,currentPrice-C.trailingStopATR*(a.entry?.atr||0)):entry;
const slPrice=Number(trailStop.toFixed(2));
if(slPrice>stopPrice&&slPrice<currentPrice){
if(stop?.id)await cancelOrderById(env,stop.id).catch(()=>false);
const r=await order(env,m,'sell',Math.abs(+p.qty||0),'stop',null,{stop_price:slPrice,client_order_id:`eg-trail-${s.replace(/\W/g,'')}-${Date.now()}`});
if(r.status<300){await log(env,s,'TRAIL',`Equity trailing stop tightened to ${slPrice} (unrealized: ${unrealizedR.toFixed(2)}R)`,{state:'TRAILING_STOP'});}
}
}
const bearishCount=(a.entry.ema20<a.entry.ema50?1:0)+(a.regime.ema20<a.regime.ema50?1:0)+(a.macro.ema20<a.macro.ema50?1:0);
if(bearishCount>=2&&unrealizedR>=C.partialExitR&&Math.abs(+p.qty||0)>=2){
const partialQty=Math.floor(Math.abs(+p.qty||0)*C.partialExitQty);
if(partialQty>0){const r=await order(env,m,'sell',partialQty,'market',null,{client_order_id:`eg-partial-${s.replace(/\W/g,'')}-${Date.now()}`});if(r.status<300){await log(env,s,'SELL',`Partial exit ${partialQty} shares at ${unrealizedR.toFixed(2)}R — ${bearishCount}/3 TF bearish`,{state:'PARTIAL_EXIT'});}}
}
const exit=a.signal.direction==='SHORT'||(a.entry.ema20<a.entry.ema50&&a.regime.ema20<a.regime.ema50);
if(exit){const r=await order(env,m,'sell',Math.abs(+p.qty||0),'market',null,{client_order_id:`eg-exit-${s.replace(/\W/g,'')}-${Date.now()}`});if(r.status<300){const wasWin=currentPrice>entry;await recordTradeOutcome(env,s,wasWin);const cl=await consecutiveLosses(env,s);const cd=exitCooldown('trend-exit',cl);await setCooldown(env,s,cd,'trend-exit');await log(env,s,'SELL','Exit: multi-timeframe trend deterioration',{state:'EXIT_SUBMITTED',cooldown:cd});await incTradeCount(env);}else{await log(env,s,'ERROR',r.body?.message||`exit ${r.status}`);}}else{await log(env,s,'HOLD',`Score ${a.signal.score}/100 · ${a.signal.regime} · RSI ${a.entry.rsi?.toFixed(1)} · ADX ${a.entry.adx?.toFixed(1)}`,{state:'POSITION_ACTIVE'});}}}
}catch(e){errors.push(`${s}: ${String(e)}`);await log(env,s,'ERROR',String(e),{state:'ENGINE_ERROR'});}
}
try{[acct,pos,open]=await Promise.all([account(env),positions(env),orders(env,'open')]);}catch(e){errors.push(`refresh: ${String(e)}`);await kvPut(env,'reconcile:halt','1',300).catch(()=>{});await putJSON(env,'bot:lastRun',{state:'refresh-error',startedAt:iso(),finishedAt:iso(),errors},86400).catch(()=>{});return;}
const corr=await correlationMatrix(env).catch(()=>null);let rb=riskBook(pos,open,corr);const reservedSymbols=new Set();const dr=await dayRisk(env,acct);const manualHalt=(await kvGet(env,'manualhalt','0'))==='1';
const entriesBlocked=rb.unprotected>0;await kvPut(env,'reconcile:halt',entriesBlocked?'1':'0',300).catch(()=>{});
if(manualHalt){await log(env,'SYSTEM','SKIP','Autopilot paused; existing positions were still managed',{state:'MANUAL_HALT'});await putJSON(env,'bot:lastRun',{state:'manual-halt',startedAt:iso(),finishedAt:iso(),errors},86400).catch(()=>{});return;}
if(dr.level>=2){await log(env,'SYSTEM','SKIP','Daily risk stop active; existing positions were still managed',{state:'DAILY_RISK_HALT'});await putJSON(env,'bot:lastRun',{state:'daily-risk-stop',startedAt:iso(),finishedAt:iso(),errors},86400).catch(()=>{});return;}
if(await tradeCount(env)>=C.maxTradesPerDay){await log(env,'SYSTEM','SKIP','Maximum daily trade count reached');await putJSON(env,'bot:lastRun',{state:'trade-limit',startedAt:iso(),finishedAt:iso(),errors},86400).catch(()=>{});return;}
if(entriesBlocked){await log(env,'SYSTEM','HALT','New entries remain blocked because broker state is not fully protected',{state:'RECONCILIATION_FAILED',unprotected:rb.unprotected});await putJSON(env,'bot:lastRun',{state:'entries-blocked',startedAt:iso(),finishedAt:iso(),errors},86400).catch(()=>{});return;}
const nowClk=await clock(env);
const scanSymbols=STRATEGY_SYMBOLS.filter(s=>isCrypto(MARKETS[s])||nowClk.is_open);
const closedSymbols=STRATEGY_SYMBOLS.filter(s=>!scanSymbols.includes(s));
for(const s of scanSymbols){const m=MARKETS[s];
if(Date.now()-startedAt>C.maxRunWallMs){errors.push('entry-scan: wall-time budget exceeded; remaining symbols deferred');break;}
try{
if(pos.some(x=>positionKey(x)===s)||reservedSymbols.has(s))continue;
if(open.some(o=>positionKey(o)===s&&['new','accepted','pending_new','partially_filled','calculated'].includes(String(o.status||'').toLowerCase()))){await log(env,s,'SKIP','Open order already exists');continue;}
const a=await analysis(env,m);if(a.spreadPct!=null&&a.spreadPct>C.maxSpread){await log(env,s,'SKIP',`Spread ${(a.spreadPct*100).toFixed(3)}% exceeds configured maximum`,{state:'MARKET_QUALITY_REJECTED'});continue;}
if(!bull(a.macro)){await log(env,s,'SKIP',`Daily macro not in full bull (ema20>${a.macro.ema50?.toFixed(2)}? ema50>${a.macro.ema200?.toFixed(2)}?)`,{state:'MACRO_REJECTED',score:a.signal.score});continue;}
if(a.signal.direction!=='LONG'||a.signal.score<C.minScore){await log(env,s,'HOLD',`No entry: score ${a.signal.score}/100 · ${a.signal.regime}`,{state:'SIGNAL_REJECTED'});continue;}
const cd=await cooldown(env,s);if(cd&&cd.until>Date.now()){await log(env,s,'SKIP',`Cooldown until ${new Date(cd.until).toISOString()}`,{state:'COOLDOWN'});continue;}
if(!a.plan||a.plan.rr<C.minRR){await log(env,s,'SKIP','Invalid risk/reward plan',{state:'RISK_REJECTED'});continue;}
const equity=+acct.equity,bp=+acct.buying_power;const modeledEntry=a.plan.entry*(1+C.slippage);const modeledRisk=Math.max(0.00000001,Math.abs(modeledEntry-a.plan.stop));
let q=qtyRound((equity*C.riskPerTrade)/modeledRisk,m);q=Math.min(q,qtyRound(Math.min(equity*C.maxPositionPct,bp*.98)/a.plan.entry,m));
if(isCrypto(m)){const meta=await assetInfo(env,m);q=qtyForOrder(q,m,meta);}
if(q<=0){await log(env,s,'SKIP','Risk sizing rounds to zero',{state:'RISK_REJECTED'});continue;}
const proposedRisk=q*modeledRisk;if(proposedRisk>equity*C.maxSingleRisk){await log(env,s,'SKIP','Single-trade risk limit reached',{state:'RISK_REJECTED'});continue;}
if(rb.gross+q*a.plan.entry>equity*C.maxGrossExposure){await log(env,s,'SKIP','Gross exposure limit reached',{state:'RISK_REJECTED'});continue;}
const clusterNotional=Object.values(rb.bySymbol).filter(v=>v.cluster===m.cluster).reduce((t,v)=>t+(+v.notional||0),0);if(clusterNotional+q*a.plan.entry>equity*C.maxClusterExposurePct){await log(env,s,'SKIP',`Cluster exposure limit reached: ${m.cluster}`,{state:'RISK_REJECTED'});continue;}
const clusterRiskAfter=proposedClusterRisk(rb,s,m.cluster,proposedRisk,corr);if(clusterRiskAfter>equity*C.maxClusterRisk){await log(env,s,'SKIP',`Cluster risk limit reached: ${m.cluster}`,{state:'RISK_REJECTED',clusterRisk:clusterRiskAfter/equity});continue;}
const portfolioRiskAfter=proposedPortfolioRisk(rb,s,proposedRisk,corr);if(portfolioRiskAfter>equity*C.maxPortfolioRisk){await log(env,s,'SKIP','Portfolio risk limit reached',{state:'RISK_REJECTED',portfolioRisk:portfolioRiskAfter/equity});continue;}
if(dr.level>=1){await log(env,s,'SKIP','Soft daily-loss mode blocks new risk',{state:'RISK_REJECTED'});continue;}
const decisionId=`${day()}-${s.replace(/\W/g,'')}-${Date.now()}`;const decision={decisionId,symbol:s,state:'EXECUTION_PENDING',signal:a.signal,risk:{proposedRisk,portfolioRiskAfter,clusterRiskAfter},plan:a.plan,orderId:null,cryptoProtection:null};await putJSON(env,'decision:'+decisionId,decision,86400);
let r;if(isCrypto(m)){r=await protectedCryptoEntry(env,m,q,a.plan.stop,a.plan.target,{clientOrderId:`eg-${s.replace(/\W/g,'')}-${decisionId}`,expectedEntry:a.plan.entry,atr:a.entry.atr});}else{r=await bracket(env,m,q,a.plan.stop,a.plan.target,`eg-${s.replace(/\W/g,'')}-${decisionId}`);}
if(r.status>=300){await log(env,s,'ERROR',`Protected entry rejected: ${r.body?.message||r.status}`,{decisionId,state:'EXECUTION_REJECTED'});continue;}
decision.state='PROTECTION_VERIFIED';decision.orderId=r.body?.id||null;decision.cryptoProtection=r.body?.protection||null;
if(!isCrypto(m)){const protectionOK=await verifyBracketProtection(env,r.body?.id);if(!protectionOK){await kvPut(env,'reconcile:halt','1',300);await log(env,s,'HALT','Bracket accepted but protective legs could not be verified; new entries blocked',{decisionId,state:'PROTECTION_UNVERIFIED'});continue;}}
await putJSON(env,'decision:'+decisionId,decision,86400);
rb=reserveRiskBook(rb,s,m,q*a.plan.entry,proposedRisk,corr);reservedSymbols.add(s);
await setCooldown(env,s,C.cooldown,'entry');await incTradeCount(env);
await log(env,s,'BUY',`Protected entry ${q} @ ~${a.plan.entry.toFixed(2)} · stop ${a.plan.stop.toFixed(2)} · target ${a.plan.target.toFixed(2)} · risk ${(proposedRisk/equity*100).toFixed(2)}% · cluster ${m.cluster}`,{decisionId,score:a.signal.score,rr:a.plan.rr,state:'PROTECTION_VERIFIED',cluster:m.cluster});
}catch(e){errors.push(`${s}: ${String(e)}`);await log(env,s,'ERROR',String(e),{state:'ENGINE_ERROR'});}
}
await putJSON(env,'bot:lastRun',{state:'complete',startedAt:iso(),finishedAt:iso(),errors,riskUnprotected:rb.unprotected,scanned:scanSymbols,skippedClosed:closedSymbols},86400).catch(()=>{});
}
async function run(env){
if(!env.STRATEGY_LOCK)throw new Error('STRATEGY_LOCK Durable Object binding is required');
const id=env.STRATEGY_LOCK.idFromName('strategy');const stub=env.STRATEGY_LOCK.get(id);
const acquired=await stub.fetch('https://endgame-lock/acquire',{method:'POST'});if(!acquired.ok)return;
const lease=(await acquired.json()).token;const heartbeatMs=Math.max(10000,Math.floor((C.lockSeconds||300)*1000/3));
const timer=setInterval(()=>{stub.fetch('https://endgame-lock/refresh',{method:'POST',headers:{'X-Lock-Token':lease}}).catch(()=>{});},heartbeatMs);
try{await runUnlocked(env);}finally{clearInterval(timer);await stub.fetch('https://endgame-lock/release',{method:'POST',headers:{'X-Lock-Token':lease}}).catch(()=>{});}
}

function journal(orders){
const fills=orders.filter(o=>o.status==='filled'&&finite(o.filled_avg_price)&&finite(o.filled_qty)).map(o=>({symbol:canon(o.symbol)||o.symbol,side:o.side,qty:+o.filled_qty,price:+o.filled_avg_price,time:o.filled_at||o.updated_at||o.submitted_at,stopPrice:+(stopFromOrder(o)||0)||null})).sort((a,b)=>new Date(a.time).getTime()-new Date(b.time).getTime());
const lots={};const tr=[];
for(const f of fills){lots[f.symbol]??=[];if(f.side==='buy'){lots[f.symbol].push({qty:f.qty,price:f.price,time:f.time,stopPrice:f.stopPrice});continue;}
let rem=f.qty;let q=0;let c=0;let start=null;let stopPrice=null;
while(rem>1e-9&&lots[f.symbol].length){const l=lots[f.symbol][0];const take=Math.min(rem,l.qty);q+=take;c+=take*l.price;start??=l.time;stopPrice??=l.stopPrice;l.qty-=take;rem-=take;if(l.qty<=1e-9){lots[f.symbol].shift();}}
if(q){const ep=c/q;tr.push({symbol:f.symbol,qty:q,entryPrice:ep,exitPrice:f.price,entryTime:start,exitTime:f.time,pl:(f.price-ep)*q,plPct:(f.price-ep)/ep,rMultiple:stopPrice&&Math.abs(ep-stopPrice)>0?(f.price-ep)/Math.abs(ep-stopPrice):null,holdingMinutes:start&&f.time?Math.max(0,(new Date(f.time).getTime()-new Date(start).getTime())/60000):null});}}
const wins=tr.filter(x=>x.pl>0);const loss=tr.filter(x=>x.pl<=0);const gw=wins.reduce((s,x)=>s+x.pl,0);const gl=Math.abs(loss.reduce((s,x)=>s+x.pl,0));const net=tr.reduce((s,x)=>s+x.pl,0);
let maxConsecutiveLosses=0,currentLosses=0;for(const x of tr){if(x.pl<=0){currentLosses++;maxConsecutiveLosses=Math.max(maxConsecutiveLosses,currentLosses);}else currentLosses=0;}
return{trades:tr.reverse(),stats:{totalTrades:tr.length,wins:wins.length,losses:loss.length,totalPL:net,winRate:tr.length?wins.length/tr.length:null,avgWin:wins.length?gw/wins.length:0,avgLoss:loss.length?gl/loss.length:0,avgR:tr.filter(x=>finite(x.rMultiple)).length?tr.filter(x=>finite(x.rMultiple)).reduce((s,x)=>s+x.rMultiple,0)/tr.filter(x=>finite(x.rMultiple)).length:null,maxConsecutiveLosses,avgHoldingMinutes:tr.filter(x=>finite(x.holdingMinutes)).length?tr.filter(x=>finite(x.holdingMinutes)).reduce((s,x)=>s+x.holdingMinutes,0)/tr.filter(x=>finite(x.holdingMinutes)).length:null,profitFactor:gl?gw/gl:null,expectancy:tr.length?net/tr.length:0}};
}

async function dashboard(env){
const recon=await reconcile(env);const acct=recon.account,pos=recon.positions,open=recon.openOrders;const clk=await clock(env);const dr=await dayRisk(env,acct);const corr=await correlationMatrix(env);const book=riskBook(pos,open,corr);
const positionsView=pos.map(p=>({...p,protected:!!protectiveStopForPosition(p,open),protected_qty:protectiveStopForPosition(p,open)?.qty||0}));
return{account:acct,positions:positionsView,openOrders:open,clock:clk,dailyReturn:-dr.dd,peakDrawdown:-dr.peakDD,dayStartEquity:dr.start,risk:{...C},riskLevel:dr.level,tradeCount:await tradeCount(env),riskBook:book,reconciliation:recon};
}

export default {
async fetch(req,env){
if(req.method==='OPTIONS'){return new Response(null,{headers:cors});}
if(!env.APP_TOKEN){return json({error:'APP_TOKEN not configured'},500);}
if(req.headers.get('X-App-Token')!==env.APP_TOKEN){return json({error:'Unauthorized'},401);}
const u=new URL(req.url);const p=u.pathname;
try{
if(p==='/api/health'){return json({ok:true,time:iso(),paper:true,version:'v13'});}
if(p==='/api/markets'&&req.method==='GET'){return json({markets:Object.values(MARKETS),config:C});}
if(p==='/api/account'&&req.method==='GET'){return json(await account(env));}
if(p==='/api/positions'&&req.method==='GET'){return json(await positions(env));}
if(p==='/api/orders'&&req.method==='GET'){return json(await orders(env,'all',clamp(+(u.searchParams.get('limit')||100),1,500)));}
if(p==='/api/orders'&&req.method==='POST'){
if((await kvGet(env,'kill','0'))==='1'){return json({error:'Kill switch is active'},403);}
const x=await req.json().catch(()=>({}));const m=market(x.symbol);if(!m){return json({error:'Symbol is outside the Endgame universe'},400);}
if(!['buy','sell'].includes(x.side)){return json({error:'Invalid side'},400);}
if(!finite(x.qty)||+x.qty<=0){return json({error:'Invalid quantity'},400);}
const q=await quote(env,m);const entry=x.type==='limit'?+x.limit_price:(x.side==='buy'?q.ask:q.bid);
if(x.side==='buy'&&(x.stop_loss_price==null||x.take_profit_price==null)){return json({error:'Protected BUY requires stop_loss_price and take_profit_price'},400);}
if(x.side==='buy'){const stop=+x.stop_loss_price;const target=+x.take_profit_price;if(!(stop<entry&&target>entry)){return json({error:'Invalid stop/target'},400);}
const qty=qtyRound(+x.qty,m);const[acct,pos,open,corr]=await Promise.all([account(env),positions(env),orders(env,'open'),correlationMatrix(env)]);
const rb=riskBook(pos,open,corr),proposedRisk=qty*Math.abs(entry-stop),eq=+acct.equity||0;
if((await kvGet(env,'reconcile:halt','0'))==='1')return json({error:'Broker reconciliation is unhealthy; new orders are temporarily blocked'},409);
if(rb.unprotected>0)return json({error:'Existing unprotected position detected; reconcile before adding risk'},409);
if((await tradeCount(env))>=C.maxTradesPerDay)return json({error:'Maximum daily trade count reached'},409);
if(proposedRisk>eq*C.maxSingleRisk)return json({error:'Single-trade risk limit exceeded'},409);
if(rb.gross+qty*entry>eq*C.maxGrossExposure)return json({error:'Gross exposure limit exceeded'},409);
if(qty*entry>eq*C.maxPositionPct)return json({error:'Position size limit exceeded'},409);
const manualDr=await dayRisk(env,acct);if(manualDr.level>=1)return json({error:'Daily de-risk mode blocks new BUY risk'},409);
if(proposedPortfolioRisk(rb,m.symbol,proposedRisk,corr)>eq*C.maxPortfolioRisk)return json({error:'Portfolio risk limit exceeded'},409);
if(proposedClusterRisk(rb,m.symbol,m.cluster,proposedRisk,corr)>eq*C.maxClusterRisk)return json({error:`Cluster risk limit exceeded: ${m.cluster}`},409);
const clusterNotional=pos.filter(p=>marketFor(p.symbol)?.cluster===m.cluster).reduce((v,p)=>v+Math.abs(+p.market_value||0),0);if(clusterNotional+qty*entry>eq*C.maxClusterExposurePct)return json({error:`Cluster exposure limit exceeded: ${m.cluster}`},409);
const clientOrderId=`eg-manual-${m.symbol.replace(/\W/g,'')}-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
const r=isCrypto(m)?await protectedCryptoEntry(env,m,qty,stop,target,{clientOrderId,expectedEntry:entry}):await bracket(env,m,qty,stop,target,clientOrderId);
if(r.status<300&&!isCrypto(m)){const protectionOK=await verifyBracketProtection(env,r.body?.id);if(!protectionOK){await kvPut(env,'reconcile:halt','1',300);await log(env,m.symbol,'HALT','Manual bracket accepted but protective legs could not be verified',{state:'PROTECTION_UNVERIFIED',clientOrderId});return json({error:'Bracket was accepted but protective stop/target could not be verified; new entries are halted'},409);}}
if(r.status<300){await incTradeCount(env);await log(env,m.symbol,'BUY',`Manual protected entry ${qty} @ ~${entry.toFixed(4)} · stop ${stop.toFixed(4)} · target ${target.toFixed(4)}${isCrypto(m)?' · synthetic OCO':''}`,{state:'PROTECTION_VERIFIED',clientOrderId});}
return json(r.body,r.status);}
const sellQty=qtyRound(+x.qty,m);const currentPos=(await positions(env)).find(p=>positionKey(p)===m.symbol);
if(!currentPos||sellQty>Math.abs(+currentPos.qty||0)+1e-9){return json({error:'SELL quantity exceeds the current long position or no position exists'},400);}
const r=await order(env,m,x.side,sellQty,x.type||'market',x.limit_price,{stop_price:x.stop_price,client_order_id:`eg-manual-${m.symbol.replace(/\W/g,'')}-${Date.now()}-${Math.random().toString(36).slice(2,8)}`});
if(r.status<300){await incTradeCount(env);await log(env,m.symbol,'SELL',`Manual exit submitted ${x.qty}`,{state:'EXIT_SUBMITTED'});}
return json(r.body,r.status);
}
if(p==='/api/orders/'&&req.method==='DELETE'){return json({error:'Missing order id'},400);}
if(p.startsWith('/api/orders/')&&req.method==='DELETE'){const id=p.split('/').pop();const r=await alpaca(`${ALPACA_BASE}/v2/orders/${id}`,env,{method:'DELETE'});return json(r.status===204?{ok:true}:r.body,r.status===204?200:r.status);}
if(p==='/api/quote'&&req.method==='GET'){const m=market(u.searchParams.get('symbol'));if(!m){return json({error:'Invalid market'},400);}return json(await quote(env,m));}
if(p==='/api/bars'&&req.method==='GET'){const m=market(u.searchParams.get('symbol'));const tf=u.searchParams.get('timeframe')||'15Min';if(!m){return json({error:'Invalid market'},400);}if(!['1Min','5Min','15Min','30Min','1Hour','1Day'].includes(tf)){return json({error:'Invalid timeframe'},400);}return json({symbol:m.symbol,timeframe:tf,bars:await bars(env,m,tf,clamp(+(u.searchParams.get('limit')||100),10,500))});}
if(p==='/api/analysis'&&req.method==='GET'){const selected=u.searchParams.get('symbol');const keys=selected?[canon(selected)].filter(Boolean):STRATEGY_SYMBOLS;const out=[];for(const k of keys){try{out.push(await analysis(env,MARKETS[k]));}catch(e){out.push({symbol:k,error:String(e)});}}return json({analysis:out});}
if(p==='/api/reconciliation'&&req.method==='GET')return json(await reconcile(env));
if(p==='/api/risk'&&req.method==='GET'){const c=await correlationMatrix(env);return json(await getRiskState(env,c));}
if(p==='/api/dashboard'&&req.method==='GET'){return json(await dashboard(env));}
if(p==='/api/correlation'&&req.method==='GET'){return json(await correlationMatrix(env));}
if(p==='/api/journal'&&req.method==='GET'){const o=await orders(env,'all',C.journalLimit);return json(journal(o));}
if(p==='/api/bot/status'&&req.method==='GET'){
const acct=await account(env);const dr=await dayRisk(env,acct);const corr=await correlationMatrix(env);const[pos,open]=await Promise.all([positions(env),orders(env,'open')]);const rb=riskBook(pos,open,corr);
const cryptoPlans={};for(const p of pos){const m=marketFor(p.symbol);if(isCrypto(m))cryptoPlans[positionKey(p)]=await getJSON(env,cryptoPlanKey(positionKey(p)),null);}
return json({manualHalt:(await kvGet(env,'manualhalt','0'))==='1',killActive:(await kvGet(env,'kill','0'))==='1',reconciliationHalt:(await kvGet(env,'reconcile:halt','0'))==='1',riskBook:rb,cryptoPlans,riskLevel:dr.level,tradeCount:await tradeCount(env),log:await getJSON(env,'bot:log',[]),lastRun:await getJSON(env,'bot:lastRun',null),config:C});
}
if(p==='/api/bot/run'&&req.method==='POST'){await run(env);return json({ok:true});}
if(p==='/api/bot/toggle'&&req.method==='POST'){const x=await req.json().catch(()=>({}));await kvPut(env,'manualhalt',x.halted?'1':'0');await log(env,'SYSTEM',x.halted?'HALT':'SKIP',x.halted?'Autopilot paused':'Autopilot resumed');const corr=await correlationMatrix(env);const[pos,open]=await Promise.all([positions(env),orders(env,'open')]);const rb=riskBook(pos,open,corr);return json({reconciliationHalt:(await kvGet(env,'reconcile:halt','0'))==='1',riskBook:rb,manualHalt:!!x.halted});}
if(p==='/api/kill/status'){return json({active:(await kvGet(env,'kill','0'))==='1'});}
if(p==='/api/kill/activate'&&req.method==='POST'){const x=await req.json().catch(()=>({}));await kvPut(env,'kill','1');let canceled=0;let preservedProtection=0;if(x.cancelOpenOrders){const[openOrders,currentPositions]=await Promise.all([orders(env,'open'),positions(env)]);const protectedIds=new Set();for(const pos of currentPositions){for(const po of activeProtectionOrders(pos,openOrders)){if(po.id)protectedIds.add(po.id);}}for(const o of openOrders){if(protectedIds.has(o.id)){preservedProtection++;continue;}const r=await alpaca(`${ALPACA_BASE}/v2/orders/${o.id}`,env,{method:'DELETE'});if(r.status<300)canceled++;}}await log(env,'SYSTEM','HALT','Kill switch activated',{canceled,preservedProtection});return json({active:true,canceled,preservedProtection});}
if(p==='/api/kill/deactivate'&&req.method==='POST'){await kvPut(env,'kill','0');await log(env,'SYSTEM','SKIP','Kill switch deactivated');return json({active:false});}
return json({error:'Not found'},404);
}catch(e){console.error(e);return json({error:String(e)},500);}
},
async scheduled(event,env){
const startedAt=iso();
try{await putJSON(env,'bot:lastRun',{state:'scheduled-start',startedAt,cron:event?.cron||null},86400);await run(env);}
catch(e){console.error(e);const message=e?.message||String(e);await putJSON(env,'bot:lastRun',{state:'crashed',startedAt,finishedAt:iso(),error:message},86400).catch(()=>{});await log(env,'SYSTEM','ERROR',`Scheduled run crashed before completing: ${message}`,{state:'RUN_CRASHED'}).catch(()=>{});}
}
};

export class StrategyLock {
constructor(state){this.state=state;this.storage=state.storage;}
async fetch(req){
const path=new URL(req.url).pathname;
if(req.method==='POST'&&path==='/acquire'){const now=Date.now();const current=await this.storage.get('lease');if(current?.expiresAt>now){return new Response('locked',{status:409});}const token=crypto.randomUUID();await this.storage.put('lease',{token,expiresAt:now+(C.lockSeconds||300)*1000});return Response.json({token});}
if(req.method==='POST'&&path==='/refresh'){const token=req.headers.get('X-Lock-Token');const current=await this.storage.get('lease');if(current?.token!==token)return new Response('locked',{status:409});await this.storage.put('lease',{token,expiresAt:Date.now()+(C.lockSeconds||300)*1000});return new Response(null,{status:204});}
if(req.method==='POST'&&path==='/release'){const token=req.headers.get('X-Lock-Token');const current=await this.storage.get('lease');if(current?.token===token)await this.storage.delete('lease');return new Response(null,{status:204});}
return new Response('Not found',{status:404});
}
}
