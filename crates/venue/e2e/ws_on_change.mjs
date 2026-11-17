// Local sim only: l2Book/bbo/ctx/allMids push on change. Counts frames and distinct contents over 10 s, then moves the book
// with /control and measures the delay to the next l2Book frame.
import { wsConnect, SIM_WS, sleep, ctrl } from "./lib.mjs";
const c = await wsConnect(SIM_WS);
for (const s of [{ type: "l2Book", coin: "BTC" }, { type: "bbo", coin: "BTC" }, { type: "activeAssetCtx", coin: "BTC" }, { type: "allMids" }]) c.sub(s);
await sleep(10000);
const by = {};
for (const f of c.frames) { const ch = f.msg.channel; if (ch === "subscriptionResponse") continue; (by[ch] ??= []).push(f); }
for (const [ch, fr] of Object.entries(by)) {
  const distinct = new Set(fr.map((f) => JSON.stringify(f.msg.data.levels ?? f.msg.data.bbo ?? f.msg.data.ctx ?? Object.keys(f.msg.data.mids ?? {}).length + ":" + JSON.stringify(f.msg.data.mids?.BTC)))).size;
  console.log(ch.padEnd(16), "frames", fr.length, "distinct", distinct);
}
// control: crash BTC 1%, expect l2Book + ctx frames quickly
const n0 = c.frames.length, t0 = Date.now();
await ctrl("/control/crash", { coin: "BTC", pct: 1 }).catch((e) => console.log("ctrl err", String(e)));
await sleep(1500);
const after = c.frames.slice(n0).filter((f) => f.msg.channel !== "subscriptionResponse");
console.log("after crash:", after.map((f) => `${f.msg.channel}@+${f.t - t0}ms`).join(" "));
await ctrl("/control/crash", { coin: "BTC" }).catch(() => {});
c.close();
