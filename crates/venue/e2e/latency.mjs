// Stage 6: a browser-like WS client on the REAL venue and on the LOCAL sim at the same time for DURATION seconds
// (default 60). Subscribes l2Book BTC, l2Book xyz:TSLA, activeAssetCtx BTC, activeAssetCtx xyz:TSLA and allMids; reports the
// frame count and inter-frame intervals per channel on each side. Read-only on real (subscriptions only).
import { wsConnect, SIM_WS, REAL_WS, sleep } from "./lib.mjs";
const DURATION = Number(process.env.DURATION ?? 60) * 1000;
const subs = [
  { type: "l2Book", coin: "BTC" }, { type: "l2Book", coin: "xyz:TSLA" },
  { type: "activeAssetCtx", coin: "BTC" }, { type: "activeAssetCtx", coin: "xyz:TSLA" }, { type: "allMids" },
];
const key = (m) => (m.data?.coin ? `${m.channel}:${m.data.coin}` : m.channel);
const pct = (a, p) => (a.length ? a[Math.min(a.length - 1, Math.floor((p / 100) * a.length))] : NaN);
function stats(frames) {
  const by = {};
  for (const f of frames) {
    if (!f.msg.channel || f.msg.channel === "subscriptionResponse" || f.msg.channel === "pong") continue;
    (by[key(f.msg)] ??= []).push(f.t);
  }
  const out = {};
  for (const [k, ts] of Object.entries(by)) {
    const gaps = ts.slice(1).map((t, i) => t - ts[i]).sort((a, b) => a - b);
    out[k] = { n: ts.length, perSec: +(ts.length / (DURATION / 1000)).toFixed(2), gapP50: pct(gaps, 50), gapP90: pct(gaps, 90), gapMax: gaps.at(-1) ?? NaN };
  }
  return out;
}
const [real, sim] = await Promise.all([wsConnect(REAL_WS), wsConnect(SIM_WS)]);
for (const s of subs) { real.sub(s); sim.sub(s); }
// browsers ping every 50 s
const pinger = setInterval(() => { real.send({ method: "ping" }); sim.send({ method: "ping" }); }, 30000);
await sleep(DURATION);
clearInterval(pinger);
real.close(); sim.close();
const rs = stats(real.frames), ss = stats(sim.frames);
console.log(`channel`.padEnd(26), "real n  /s   p50  p90  max".padEnd(34), "sim n  /s   p50  p90  max");
for (const k of new Set([...Object.keys(rs), ...Object.keys(ss)])) {
  const f = (x) => (x ? `${String(x.n).padStart(4)} ${String(x.perSec).padStart(5)} ${String(x.gapP50).padStart(5)} ${String(x.gapP90).padStart(5)} ${String(x.gapMax).padStart(5)}` : "   -");
  console.log(k.padEnd(26), f(rs[k]).padEnd(34), f(ss[k]));
}
