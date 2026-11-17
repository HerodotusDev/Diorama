// Stage 1: the sim is ready, every dex serves, every live coin has a fresh ctx, books arrive on demand.
import { simInfo, realInfo, ctrl, Report, sleep, SIM } from "./lib.mjs";
const R = new Report("stage1");
const ready = await fetch(SIM + "/readyz");
R.ok(ready.status === 200, "readyz 200", ready.status);
const sd = (await simInfo({ type: "perpDexs" })).json;
const rd = (await realInfo({ type: "perpDexs" })).json;
R.ok(JSON.stringify(sd.map((d) => d && d.name)) === JSON.stringify(rd.map((d) => d && d.name)), "perpDexs names/order equal real", { sim: sd.map((d) => d && d.name), real: rd.map((d) => d && d.name) });
const sm = (await simInfo({ type: "allPerpMetas" })).json;
const rm = (await realInfo({ type: "allPerpMetas" })).json;
R.ok(sm.length === 11, "11 metas", sm.length);
const dexNames = sd.map((d) => (d ? d.name : ""));
// every coin of every dex: ctx via metaAndAssetCtxs{dex}, fresh via /control metrics
const metrics = (await (await fetch("http://127.0.0.1:8091/metrics")).text());
const stale = {};
for (const m of metrics.matchAll(/venue_sim_stale_ms\{coin="([^"]+)"\} (\d+)/g)) stale[m[1]] = Number(m[2]);
let total = 0, fresh = 0, missing = [], staleList = [];
for (let d = 0; d < sm.length; d++) {
  const dex = dexNames[d];
  const live = sm[d].universe.filter((u) => !u.isDelisted);
  const mc = (await simInfo({ type: "metaAndAssetCtxs", ...(dex ? { dex } : {}) })).json;
  R.ok(Array.isArray(mc) && mc[0].universe.length === sm[d].universe.length, `dex ${dex || "default"} metaAndAssetCtxs universe len`, mc?.[0]?.universe?.length);
  R.ok(mc[1].length === mc[0].universe.length, `dex ${dex || "default"} ctx array len`, mc[1].length);
  console.log(`dex ${dex || "(default)"} idx ${d}: ${live.length} live of ${sm[d].universe.length}; real ${rm[d].universe.length}`);
  R.ok(rm[d].universe.length === sm[d].universe.length, `dex ${dex} universe len equals real`, [sm[d].universe.length, rm[d].universe.length]);
  for (const u of live) {
    total++;
    if (stale[u.name] === undefined) missing.push(u.name);
    else if (stale[u.name] < 30000) fresh++;
    else staleList.push([u.name, stale[u.name]]);
  }
}
console.log(`live coins ${total}, fresh (<30s) ${fresh}, no ctx ${missing.length}, stale ${staleList.length}`);
if (missing.length) console.log("no ctx:", missing.join(","));
if (staleList.length) console.log("stale:", JSON.stringify(staleList));
R.ok(missing.length === 0, "every live coin has a ctx entry", missing);
R.ok(staleList.length === 0, "every live coin ctx is fresh", staleList);

// books on demand
const sample = ["BTC", "HYPE", "ETH", "xyz:TSLA", "flx:COIN", "km:US500", "hyna:BTC", "vntl:SPACEX", "cash:GOLD", "para:BTCD", "mkts:AAPL", "io:BTC", "abcd:BTC"];
const metaCoins = new Set(sm.flatMap((m) => m.universe.map((u) => u.name)));
for (const c of sample.filter((c) => metaCoins.has(c))) {
  const t0 = Date.now();
  const b = (await simInfo({ type: "l2Book", coin: c })).json;
  const ms = Date.now() - t0;
  // a delisted coin has no book on real either (both sides empty); a live one has both
  const delisted = sm.some((m) => m.universe.some((u) => u.name === c && u.isDelisted));
  const ok = delisted
    ? b && b.coin === c && b.levels?.[0]?.length === 0 && b.levels?.[1]?.length === 0
    : b && b.coin === c && b.levels?.[0]?.length > 0 && b.levels?.[1]?.length > 0;
  R.ok(ok, `l2Book ${c} has the venue's sides`, b && { coin: b.coin, bids: b.levels?.[0]?.length, asks: b.levels?.[1]?.length });
  const age = b?.time ? Date.now() - b.time : null;
  console.log(`l2Book ${c}: first read ${ms}ms, bids ${b?.levels?.[0]?.length} asks ${b?.levels?.[1]?.length}, age ${age}ms`);
}
console.log("not in meta (skipped):", sample.filter((c) => !metaCoins.has(c)).join(","));
// builder coins listing
for (let d = 1; d < sm.length; d++) console.log(dexNames[d], sm[d].universe.slice(0, 6).map((u) => u.name).join(","));
R.summary();
