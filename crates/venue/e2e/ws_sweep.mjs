// Subscribe to the same channel on the REAL venue and the LOCAL sim, compare acks and first frames structurally.
import { wsConnect, SIM_WS, REAL_WS, sleep } from "./lib.mjs";
const U = process.env.WS_USER || "0xd77a0944d0039e92f3309f9cf52b6926b7c14b6d";
const subs = [
  { type: "allMids" }, { type: "allMids", dex: "xyz" }, { type: "allMids", dex: "ALL_DEXS" },
  { type: "activeAssetCtx", coin: "BTC" }, { type: "activeAssetCtx", coin: "xyz:TSLA" }, { type: "activeAssetCtx", coin: "NOPE" },
  { type: "l2Book", coin: "BTC" }, { type: "l2Book", coin: "xyz:TSLA" }, { type: "l2Book", coin: "BTC", nSigFigs: 3 }, { type: "l2Book", coin: "NOPE" },
  { type: "bbo", coin: "BTC" }, { type: "trades", coin: "BTC" }, { type: "candle", coin: "BTC", interval: "1m" },
  { type: "webData3", user: U }, { type: "webData2", user: U }, { type: "clearinghouseState", user: U, dex: "" }, { type: "clearinghouseState", user: U, dex: "xyz" },
  { type: "openOrders", user: U, dex: "" }, { type: "openOrders", user: U, dex: "xyz" }, { type: "spotState", user: U },
  { type: "allDexsClearinghouseState", user: U }, { type: "userFills", user: U }, { type: "userFundings", user: U }, { type: "activeAssetData", user: U, coin: "BTC" },
  { type: "userEvents", user: U }, { type: "orderUpdates", user: U }, { type: "userNonFundingLedgerUpdates", user: U }, { type: "notification", user: U },
  { type: "assetCtxs" }, { type: "userTwapSliceFills", user: U }, { type: "userTwapHistory", user: U }, { type: "spotAssetCtxs" },
  { type: "noSuchChannel" }, { type: "l2Book" }, { type: "webData3", user: "0x12" },
];
const sig = (v, d = 0) => {
  if (v === null) return "null";
  if (Array.isArray(v)) return v.length ? `[${sig(v[0], d + 1)}]` : "[]";
  if (typeof v === "object") return d > 3 ? "{..}" : "{" + Object.keys(v).sort().map((k) => `${k}:${sig(v[k], d + 1)}`).join(",") + "}";
  if (typeof v === "string") return /^-?\d+(\.\d+)?$/.test(v) ? (v.includes(".") ? "dec" : "INTSTR") : /^0x[0-9a-f]+$/i.test(v) ? "hex" : "str";
  return typeof v;
};
async function run(url, sub) {
  const c = await wsConnect(url);
  c.sub(sub);
  await sleep(sub.type === "candle" || sub.type === "trades" ? 4500 : 3500);
  c.close();
  return c.frames.slice(0, 6).map((f) => f.msg);
}
const only = process.env.ONLY;
for (const sub of subs) {
  if (only && !only.split(",").includes(sub.type)) continue;
  let real, sim;
  try { real = await run(REAL_WS, sub); } catch (e) { real = [{ error: String(e) }]; }
  await sleep(300);
  try { sim = await run(SIM_WS, sub); } catch (e) { sim = [{ error: String(e) }]; }
  const summarize = (fs) => fs.map((m) => (m.channel ? `${m.channel}${m.data && m.data.isSnapshot !== undefined ? "(snap)" : ""}` : JSON.stringify(m).slice(0, 80)));
  const rd = real.find((m) => m.channel && m.channel !== "subscriptionResponse");
  const sd = sim.find((m) => m.channel && m.channel !== "subscriptionResponse");
  const rack = real.find((m) => m.channel === "subscriptionResponse"), sack = sim.find((m) => m.channel === "subscriptionResponse");
  const ackSame = JSON.stringify(rack) === JSON.stringify(sack);
  const dataSame = rd && sd ? sig(rd.data) === sig(sd.data) && rd.channel === sd.channel : !rd && !sd;
  console.log(`${dataSame && ackSame ? "OK  " : "DIFF"} ${JSON.stringify(sub)}`);
  if (!(dataSame && ackSame)) {
    console.log("   real:", summarize(real).join(" | "), rd && typeof rd.data !== "string" ? "\n        " + sig(rd.data).slice(0, 400) : "");
    console.log("   sim :", summarize(sim).join(" | "), sd && typeof sd.data !== "string" ? "\n        " + sig(sd.data).slice(0, 400) : "");
    if (!ackSame) console.log("   ack real:", JSON.stringify(rack)?.slice(0, 200), "\n   ack sim :", JSON.stringify(sack)?.slice(0, 200));
  }
}
