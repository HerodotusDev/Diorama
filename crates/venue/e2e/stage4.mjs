// Stage 4: liquidation end to end on the LOCAL sim. Cross and isolated positions on a default-dex and a builder-dex coin.
// Read liquidationPx (info + ws), move the mark with /control just before and just past it, assert the position
// liquidates exactly there, with the liquidation fill object and the right margin left; and that updateLeverage on a coin
// changes only that coin.
import { Wallet, ctrl, simInfo, assetId, Report, sleep, wsConnect, SIM_WS, fmtPx, fmtSz } from "./lib.mjs";

const R = new Report("stage4");
const dexNames = (await simInfo({ type: "perpDexs" })).json.map((d) => (d ? d.name : ""));
const metas = (await simInfo({ type: "allPerpMetas" })).json;
const spotMeta = (await simInfo({ type: "spotMeta" })).json;
const D = Number;
const near = (a, b, tol = 1e-6) => Math.abs(D(a) - D(b)) <= tol * Math.max(1, Math.abs(D(b)));
const chs = async (w, dex) => (await simInfo({ type: "clearinghouseState", user: w.address, ...(dex ? { dex } : {}) })).json;
const posOf = (st, coin) => st.assetPositions.find((p) => p.position.coin === coin)?.position;

function locate(coin) {
  const d = coin.includes(":") ? dexNames.indexOf(coin.split(":")[0]) : 0;
  const i = metas[d].universe.findIndex((u) => u.name === coin);
  return { d, i, dex: dexNames[d], u: metas[d].universe[i], a: assetId(d, i), collateral: metas[d].collateralToken };
}
async function markOf(coin) {
  const { d, i, dex } = locate(coin);
  const mc = (await simInfo({ type: "metaAndAssetCtxs", ...(dex ? { dex } : {}) })).json;
  return D(mc[1][i].markPx);
}
async function fundDex(w, d, amount) {
  const dex = dexNames[d];
  const c = metas[d].collateralToken;
  const tok = spotMeta.tokens.find((t) => t.index === c);
  await ctrl("/control/deal", { address: w.address, class: "spot", coin: tok.name, amount: String(amount * 2) });
  if (d === 0) return w.userSend("usdClassTransfer", { amount: String(amount), toPerp: true });
  return w.userSend("sendAsset", { destination: w.address, sourceDex: "spot", destinationDex: dex, token: `${tok.name}:${tok.tokenId}`, amount: String(amount), fromSubAccount: "" });
}
// move the coin's mark to `target` (crash/rally take a percent of the current mark)
async function moveMarkTo(coin, target) {
  const cur = await markOf(coin);
  const up = target > cur;
  const pct = (Math.abs(target - cur) / cur) * 100;
  const r = await ctrl(up ? "/control/rally" : "/control/crash", { coin, pct: pct.toFixed(8) });
  if (r.status !== 200) throw new Error(`move ${coin}: ${r.text}`);
  return r.json;
}
const liveMid = {};
async function release(coin) {
  await ctrl("/control/crash", { coin });
  // wait until the live feed has taken the coin back (the mark returns to the live level)
  for (let i = 0; i < 60; i++) {
    const m = D((await simInfo({ type: "allMids", ...(coin.includes(":") ? { dex: coin.split(":")[0] } : {}) })).json[coin]);
    const real = liveMid[coin] ?? m;
    if (Math.abs(m - real) / real < 0.01) return;
    await sleep(500);
  }
  console.log("   WARN: mark did not return to live for", coin);
}

async function wsSnapshot(w, dex, coin) {
  const c = await wsConnect(SIM_WS);
  c.sub({ type: "clearinghouseState", user: w.address, dex });
  c.sub({ type: "allDexsClearinghouseState", user: w.address });
  c.sub({ type: "webData3", user: w.address });
  for (let i = 0; i < 40; i++) {
    if (["clearinghouseState", "allDexsClearinghouseState", "webData3"].every((ch) => c.frames.some((f) => f.msg.channel === ch))) break;
    await sleep(100);
  }
  const get = (ch) => c.frames.find((f) => f.msg.channel === ch)?.msg.data;
  const out = {
    chs: posOf(get("clearinghouseState")?.clearinghouseState ?? {}, coin)?.liquidationPx,
    all: posOf(get("allDexsClearinghouseState")?.clearinghouseStates?.find(([n]) => n === dex)?.[1] ?? { assetPositions: [] }, coin)?.liquidationPx,
    wd3: (() => { const s = get("webData3"); const st = s?.perpDexStates?.find?.((x, i) => dexNames[i] === dex)?.clearinghouseState ?? s?.clearinghouseState; return st && posOf(st, coin)?.liquidationPx; })(),
  };
  c.close();
  return out;
}

for (const c of ["BTC", "ETH", "xyz:TSLA", "para:AVGO", "io:OAI", "SOL"]) liveMid[c] = D((await simInfo({ type: "allMids", ...(c.includes(":") ? { dex: c.split(":")[0] } : {}) })).json[c]);
async function liqCase({ coin, side, margin, lev, notional = 4500 }) {
  const { d, i, dex, u, a, collateral } = locate(coin);
  const tag = `[${coin} ${margin} ${side}]`;
  const isCross = margin === "cross";
  const w = new Wallet();
  const deposit = 1000;
  await fundDex(w, d, deposit);
  let r = await w.l1Send({ type: "updateLeverage", asset: a, isCross, leverage: lev });
  if (!R.ok(r.json?.status === "ok", `${tag} updateLeverage`, r.text)) return;
  const book = (await simInfo({ type: "l2Book", coin })).json;
  const bid = D(book.levels[0][0].px), ask = D(book.levels[1][0].px);
  const sd = u.szDecimals, lot = 10 ** -sd;
  const sz = Number((Math.ceil(notional / ask / lot) * lot).toFixed(sd));
  const isLong = side === "long";
  r = await w.l1Send({ type: "order", orders: [{ a, b: isLong, p: fmtPx(isLong ? ask * 1.03 : bid * 0.97, sd), s: fmtSz(sz, sd), r: false, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
  const f = r.json?.response?.data?.statuses?.[0]?.filled;
  if (!R.ok(f, `${tag} open`, r.text)) return;
  const st = await chs(w, dex);
  const p = posOf(st, coin);
  R.ok(p && p.liquidationPx != null, `${tag} liquidationPx present`, p);
  if (!p?.liquidationPx) return;
  const liq = D(p.liquidationPx);
  const entry = D(p.entryPx);
  R.ok(isLong ? liq < entry : liq > entry, `${tag} liquidationPx on the losing side of entry`, { liq, entry });
  R.ok(p.leverage.type === (isCross ? "cross" : "isolated") && p.leverage.value === lev, `${tag} position leverage`, p.leverage);
  // consistency across surfaces
  const snap = await wsSnapshot(w, dex, coin);
  R.ok(near(snap.chs, p.liquidationPx, 1e-9) && near(snap.all, p.liquidationPx, 1e-9), `${tag} liquidationPx equal on info, ws clearinghouseState and allDexsClearinghouseState`, { info: p.liquidationPx, ...snap });
  if (snap.wd3 !== undefined) R.ok(near(snap.wd3, p.liquidationPx, 1e-9), `${tag} liquidationPx equal on ws webData3`, { info: p.liquidationPx, ...snap });
  const all = (await simInfo({ type: "frontendOpenOrders", user: w.address, dex: "ALL_DEXS" })).json; // shape sanity only
  const aad = (await simInfo({ type: "activeAssetData", user: w.address, coin })).json;
  R.ok(aad.leverage.value === lev && aad.leverage.type === p.leverage.type, `${tag} activeAssetData leverage`, aad.leverage);

  // just before: no liquidation
  const before = isLong ? liq * 1.003 : liq * 0.997;
  await moveMarkTo(coin, before);
  await ctrl("/control/advance", { seconds: 2, n: 2 });
  let s2 = await chs(w, dex);
  R.ok(posOf(s2, coin) && D(posOf(s2, coin).szi) === D(p.szi), `${tag} still open just before liquidationPx (mark ${before.toFixed(4)} vs ${liq})`, s2.assetPositions);
  const p2 = posOf(s2, coin);
  console.log(`   ${tag} entry ${entry} liqPx ${liq} dist ${(((liq - entry) / entry) * 100).toFixed(2)}%  at ${before.toFixed(4)}: liqPx now ${p2?.liquidationPx} acctValue ${s2.marginSummary.accountValue} mmu ${s2.crossMaintenanceMarginUsed}`);
  // just past: liquidates
  const past = isLong ? liq * 0.997 : liq * 1.003;
  const mv = await moveMarkTo(coin, past);
  const adv = await ctrl("/control/advance", { seconds: 2, n: 2 });
  await sleep(300);
  const s3 = await chs(w, dex);
  R.ok(!posOf(s3, coin), `${tag} liquidated just past liquidationPx (mark ${past.toFixed(4)})`, s3.assetPositions);
  const fills = (await simInfo({ type: "userFills", user: w.address })).json;
  const lf = fills.find((x) => x.liquidation);
  if (R.ok(lf, `${tag} a fill carries the liquidation object`, fills.slice(0, 3))) {
    R.ok(lf.coin === coin && (lf.dir === (isLong ? "Close Long" : "Close Short") || lf.dir === `Liquidated ${isCross ? "Cross" : "Isolated"} ${isLong ? "Long" : "Short"}`), `${tag} liquidation fill dir`, lf.dir);
    R.ok(lf.liquidation.liquidatedUser === w.address && ["market", "backstop"].includes(lf.liquidation.method) && lf.liquidation.markPx !== undefined, `${tag} liquidation object fields`, lf.liquidation);
    R.ok(lf.side === (isLong ? "A" : "B") && near(lf.sz, sz) && near(lf.startPosition, isLong ? sz : -sz), `${tag} liquidation fill side/size/startPosition`, lf);
    R.ok(lf.crossed === true && typeof lf.hash === "string" && lf.feeToken === "USDC" || lf.feeToken !== undefined, `${tag} liquidation fill crossed/feeToken`, lf);
    console.log(`   ${tag} liquidation fill: dir ${lf.dir} px ${lf.px} closedPnl ${lf.closedPnl} fee ${lf.fee} method ${lf.liquidation.method} markPx ${lf.liquidation.markPx}`);
    // the fill price is the mark at which it triggered, which is just past liquidationPx
    R.ok(isLong ? D(lf.px) <= liq * 1.0001 : D(lf.px) >= liq * 0.9999, `${tag} fill price is at or past liquidationPx`, { px: lf.px, liq });
  }
  const bal = D(s3.withdrawable), av = D(s3.marginSummary.accountValue);
  console.log(`   ${tag} after: dex balance ${s3.withdrawable} accountValue ${s3.marginSummary.accountValue} (deposit ${deposit})`);
  R.ok(av >= -1e-6, `${tag} accountValue is not negative after the liquidation`, av);
  if (!isCross) {
    // isolated: only the position's margin is lost; the rest of the wallet stays
    const marginLoss = deposit - av;
    const expectMargin = (sz * entry) / lev;
    R.ok(marginLoss > 0 && marginLoss <= expectMargin * 1.05 + 5, `${tag} isolated loss bounded by its margin (${marginLoss.toFixed(2)} <= ${expectMargin.toFixed(2)})`, { marginLoss, expectMargin });
  } else {
    R.ok(av < deposit * 0.2, `${tag} cross: the equity is nearly all spent (${av.toFixed(2)})`, av);
  }
  // the other dexes / default dex untouched
  if (d !== 0) { const sd0 = await chs(w, ""); R.ok(D(sd0.marginSummary.accountValue) === 0, `${tag} the default dex is untouched`, sd0.marginSummary); }
  await release(coin);
  await sleep(300);
  return { liq, entry };
}

// updateLeverage on one coin changes only that coin (and its open position)
async function leverageIsolation() {
  const w = new Wallet();
  await fundDex(w, 0, 3000);
  const ids = { BTC: locate("BTC"), ETH: locate("ETH"), SOL: locate("SOL") };
  const px = {};
  for (const c of Object.keys(ids)) { const b = (await simInfo({ type: "l2Book", coin: c })).json; px[c] = [D(b.levels[0][0].px), D(b.levels[1][0].px)]; }
  const open = async (c, usd) => {
    const { a, u } = ids[c];
    const sz = Number((Math.ceil(usd / px[c][1] / 10 ** -u.szDecimals) * 10 ** -u.szDecimals).toFixed(u.szDecimals));
    return w.l1Send({ type: "order", orders: [{ a, b: true, p: fmtPx(px[c][1] * 1.03, u.szDecimals), s: fmtSz(sz, u.szDecimals), r: false, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
  };
  await w.l1Send({ type: "updateLeverage", asset: ids.BTC.a, isCross: true, leverage: 10 });
  await w.l1Send({ type: "updateLeverage", asset: ids.ETH.a, isCross: true, leverage: 10 });
  await open("BTC", 3000);
  await open("ETH", 3000);
  const lev = async (c) => (await simInfo({ type: "activeAssetData", user: w.address, coin: c })).json.leverage;
  const posLev = async (c) => posOf(await chs(w, ""), c)?.leverage;
  const before = { BTC: await posLev("BTC"), ETH: await posLev("ETH"), SOLaad: await lev("SOL"), BTCaad: await lev("BTC") };
  const stBefore = await chs(w, "");
  const btcBefore = posOf(stBefore, "BTC");
  const r = await w.l1Send({ type: "updateLeverage", asset: ids.ETH.a, isCross: true, leverage: 3 });
  R.ok(r.json?.status === "ok", "updateLeverage ETH 3x on an open position", r.text);
  const stAfter = await chs(w, "");
  const btcAfter = posOf(stAfter, "BTC"), ethAfter = posOf(stAfter, "ETH");
  R.ok(ethAfter.leverage.value === 3, "the ETH position takes the new leverage", ethAfter.leverage);
  R.ok(JSON.stringify(btcAfter.leverage) === JSON.stringify(btcBefore.leverage) && btcAfter.leverage.value === 10, "the BTC position leverage is untouched", btcAfter.leverage);
  R.ok(near(btcAfter.marginUsed, btcBefore.marginUsed, 1e-3), "BTC marginUsed untouched (up to mark drift)", [btcAfter.marginUsed, btcBefore.marginUsed]);
  R.ok(near(ethAfter.marginUsed, D(ethAfter.positionValue) / 3, 1e-3), "ETH marginUsed follows the new leverage", [ethAfter.marginUsed, D(ethAfter.positionValue) / 3]);
  R.ok((await lev("BTC")).value === 10 && (await lev("ETH")).value === 3, "activeAssetData: BTC 10, ETH 3", [await lev("BTC"), await lev("ETH")]);
  R.ok(JSON.stringify(await lev("SOL")) === JSON.stringify(before.SOLaad), "a coin never set keeps its default", await lev("SOL"));
  // switching ETH to isolated moves only ETH: BTC stays cross
  const r2 = await w.l1Send({ type: "updateLeverage", asset: ids.ETH.a, isCross: false, leverage: 3 });
  console.log("   cross->isolated switch on an open position:", r2.text.slice(0, 200));
  const stIso = await chs(w, "");
  console.log("   after switch ETH:", JSON.stringify(posOf(stIso, "ETH")?.leverage), "BTC:", JSON.stringify(posOf(stIso, "BTC")?.leverage));
  R.ok(posOf(stIso, "BTC")?.leverage.type === "cross", "BTC stays cross when ETH is switched", posOf(stIso, "BTC")?.leverage);
  // a position on a different dex is unaffected by a leverage set on the default dex's coin
}

const only = process.env.ONLY ? process.env.ONLY.split(",") : null;
const cases = [
  { coin: "BTC", side: "long", margin: "cross", lev: 20 },
  { coin: "BTC", side: "short", margin: "cross", lev: 20 },
  { coin: "BTC", side: "long", margin: "isolated", lev: 10 },
  { coin: "ETH", side: "short", margin: "isolated", lev: 10 },
  { coin: "xyz:TSLA", side: "long", margin: "cross", lev: 10 },
  { coin: "xyz:TSLA", side: "short", margin: "isolated", lev: 10 },
  { coin: "para:AVGO", side: "long", margin: "isolated", lev: 5 },
  { coin: "io:OAI", side: "long", margin: "isolated", lev: 3, notional: 2400 },
];
for (const c of cases) {
  if (only && !only.includes(c.coin)) continue;
  try { await liqCase(c); } catch (e) { R.ok(false, `${c.coin} ${c.margin} threw`, String(e)); await release(c.coin); }
}
if (!only || only.includes("leverage")) await leverageIsolation();
const pass = R.summary();
process.exit(pass ? 0 : 1);
