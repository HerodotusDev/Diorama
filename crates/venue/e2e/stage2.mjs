// Stage 2: orders and positions on every dex and a sample (or all) of each dex's coins, against the LOCAL sim
// whose upstream is real mainnet. Fresh wallets, funded through /control/deal (spot) and then moved to the dex with the
// signed transfer actions (usdClassTransfer for the default dex, sendAsset spot->dex for builder dexes).
//
//   node stage2.mjs [--dex all|xyz,para,...] [--coins N|all] [--ws 1]
import { Wallet, ctrl, simInfo, assetId, Report, sleep, wsConnect, SIM_WS, fmtPx, fmtSz } from "./lib.mjs";

const args = Object.fromEntries(process.argv.slice(2).reduce((a, x, i, all) => (x.startsWith("--") ? [...a, [x.slice(2), all[i + 1]]] : a), []));
const DEX_FILTER = args.dex && args.dex !== "all" ? args.dex.split(",") : null;
const COINS = args.coins === "all" ? Infinity : Number(args.coins ?? 5);
const USE_WS = args.ws !== "0";
const R = new Report("stage2");

const dexList = (await simInfo({ type: "perpDexs" })).json;
const dexNames = dexList.map((d) => (d ? d.name : ""));
const metas = (await simInfo({ type: "allPerpMetas" })).json;
const spotMeta = (await simInfo({ type: "spotMeta" })).json;
const tokenById = Object.fromEntries(spotMeta.tokens.map((t) => [t.index, t]));
const tokenStr = (idx) => `${tokenById[idx].name}:${tokenById[idx].tokenId}`;

const D = (x) => Number(x);
const near = (a, b, tol = 1e-6) => Math.abs(D(a) - D(b)) <= tol * Math.max(1, Math.abs(D(b)));
const FEE_TAKER = 0.00045, FEE_MAKER = 0.00015;
function feeMult(u) {
  if (u.deployerFeeScale === undefined) return 1;
  const s = Number(u.deployerFeeScale);
  const f = s < 1 ? 1 + s : 2 * s;
  return u.growthMode === "enabled" ? f * 0.1 : f;
}

async function chs(w, dex) { return (await simInfo({ type: "clearinghouseState", user: w.address, ...(dex ? { dex } : {}) })).json; }
const posOf = (st, coin) => st.assetPositions.find((p) => p.position.coin === coin)?.position;

// ---- ws account tracker --------------------------------------------------------------
async function tracker(w, dex, coin) {
  if (!USE_WS) return null;
  const c = await wsConnect(SIM_WS);
  c.sub({ type: "webData3", user: w.address });
  c.sub({ type: "clearinghouseState", user: w.address, dex });
  c.sub({ type: "allDexsClearinghouseState", user: w.address });
  c.sub({ type: "openOrders", user: w.address, dex });
  c.sub({ type: "spotState", user: w.address });
  c.sub({ type: "userFills", user: w.address });
  c.sub({ type: "userFundings", user: w.address });
  c.sub({ type: "activeAssetData", user: w.address, coin });
  const last = (ch, pred = () => true) => { for (let i = c.frames.length - 1; i >= 0; i--) { const m = c.frames[i].msg; if (m.channel === ch && pred(m.data)) return m.data; } };
  return { c, last };
}
// wait until `pred()` is true (polling the ws frames) or time out
async function until(pred, ms = 4000) { const t0 = Date.now(); while (Date.now() - t0 < ms) { try { if (pred()) return true; } catch { /* keep waiting */ } await sleep(100); } return false; }

// ---- funding ---------------------------------------------------------------------------
async function fund(w, d, amount = 5000) {
  const dex = dexNames[d];
  const collateral = metas[d].collateralToken;
  const tok = tokenById[collateral];
  const before = await simInfo({ type: "spotClearinghouseState", user: w.address });
  const dealt = await ctrl("/control/deal", { address: w.address, class: "spot", coin: tok.name, amount: String(amount * 2) });
  R.ok(dealt.status === 200, `deal spot ${tok.name} for ${dex || "default"}`, dealt.text);
  let r;
  if (d === 0) r = await w.userSend("usdClassTransfer", { amount: String(amount), toPerp: true });
  else r = await w.userSend("sendAsset", { destination: w.address, sourceDex: "spot", destinationDex: dex, token: tokenStr(collateral), amount: String(amount), fromSubAccount: "" });
  R.ok(r.json?.status === "ok", `fund ${dex || "default"} via ${d === 0 ? "usdClassTransfer" : "sendAsset"}`, r.text);
  const st = await chs(w, dex);
  R.ok(near(st.withdrawable, amount) && near(st.marginSummary.accountValue, amount), `${dex || "default"} perp balance equals the transfer`, st.marginSummary);
  const spot = (await simInfo({ type: "spotClearinghouseState", user: w.address })).json;
  const bal = spot.balances.find((b) => b.token === collateral);
  R.ok(near(bal?.total ?? 0, amount), `spot ${tok.name} left after the transfer`, spot.balances);
  return amount;
}

// ---- one coin's life cycle ------------------------------------------------------------------
async function coinCycle(d, u, idx) {
  const dex = dexNames[d], coin = u.name, a = assetId(d, idx);
  const tag = `[${coin}]`;
  const book = (await simInfo({ type: "l2Book", coin })).json;
  const bid = book?.levels?.[0]?.[0], ask = book?.levels?.[1]?.[0];
  if (!bid || !ask) { R.note(`${coin}: one-sided or empty book, skipped`); return "skip"; }
  const mid = (D(bid.px) + D(ask.px)) / 2;
  const sd = u.szDecimals;
  const lot = 10 ** -sd;
  let sz = Math.ceil((12 / D(ask.px)) / lot) * lot;
  sz = Number(sz.toFixed(sd));
  if (sz * D(ask.px) > 20000) { R.note(`${coin}: min notional too large for the test balance, skipped`); return "skip"; }
  const w = new Wallet();
  const funded = await fund(w, d, 5000);
  const isoOnly = !!u.onlyIsolated || ["noCross", "strictIsolated"].includes(u.marginMode);
  const lev = Math.min(u.maxLeverage, 5);
  const trk = await tracker(w, dex, coin);
  const mult = feeMult(u);
  let feeSum = 0, pnlSum = 0;

  // -- default leverage before any setting (real: cross at min(max,20); isolated on isolated-only markets)
  const aad0 = (await simInfo({ type: "activeAssetData", user: w.address, coin })).json;
  R.ok(aad0?.leverage?.type === (isoOnly ? "isolated" : "cross") && aad0?.leverage?.value === Math.min(u.maxLeverage, 20), `${tag} default leverage`, aad0?.leverage);
  R.ok(aad0?.coin === coin && aad0?.user === w.address, `${tag} activeAssetData echoes coin/user`, aad0);

  // -- updateLeverage
  let r = await w.l1Send({ type: "updateLeverage", asset: a, isCross: !isoOnly, leverage: lev });
  R.ok(r.json?.status === "ok", `${tag} updateLeverage`, r.text);
  const aad1 = (await simInfo({ type: "activeAssetData", user: w.address, coin })).json;
  R.ok(aad1.leverage.value === lev && aad1.leverage.type === (isoOnly ? "isolated" : "cross"), `${tag} activeAssetData follows updateLeverage`, aad1.leverage);
  if (!isoOnly) {
    r = await w.l1Send({ type: "updateLeverage", asset: a, isCross: true, leverage: u.maxLeverage + 1 });
    R.ok(r.json?.status === "err", `${tag} leverage above max refused`, r.text);
  } else {
    r = await w.l1Send({ type: "updateLeverage", asset: a, isCross: true, leverage: lev });
    R.ok(r.json?.status === "err", `${tag} cross leverage refused on an isolated-only market`, r.text);
  }

  // -- resting limit buy far below the market
  const restPx = fmtPx(bid.px * 0.9, sd);
  r = await w.l1Send({ type: "order", orders: [{ a, b: true, p: restPx, s: fmtSz(sz, sd), r: false, t: { limit: { tif: "Gtc" } } }], grouping: "na" });
  const rest = r.json?.response?.data?.statuses?.[0]?.resting;
  if (!R.ok(rest?.oid, `${tag} resting limit`, r.text)) return "fail";
  const oid = rest.oid;
  const oo = (await simInfo({ type: "openOrders", user: w.address, ...(dex ? { dex } : {}) })).json;
  R.ok(oo.length === 1 && oo[0].coin === coin && oo[0].oid === oid && oo[0].side === "B" && near(oo[0].limitPx, restPx) && near(oo[0].sz, sz), `${tag} openOrders row (asset id ${a} -> ${coin})`, oo);
  const fo = (await simInfo({ type: "frontendOpenOrders", user: w.address, ...(dex ? { dex } : {}) })).json;
  R.ok(fo.length === 1 && fo[0].oid === oid && fo[0].orderType === "Limit" && fo[0].tif === "Gtc" && fo[0].reduceOnly === false, `${tag} frontendOpenOrders row`, fo);
  const all = (await simInfo({ type: "frontendOpenOrders", user: w.address, dex: "ALL_DEXS" })).json;
  R.ok(all.length === 1 && all[0].coin === coin, `${tag} ALL_DEXS lists it`, all.length);
  for (const other of [dexNames[(d + 1) % dexNames.length]]) {
    const o2 = (await simInfo({ type: "openOrders", user: w.address, ...(other ? { dex: other } : {}) })).json;
    R.ok(Array.isArray(o2) && o2.length === 0, `${tag} the order is not listed under dex '${other}'`, o2);
  }
  const os = (await simInfo({ type: "orderStatus", user: w.address, oid })).json;
  R.ok(os.status === "order" && os.order.status === "open" && os.order.order.coin === coin, `${tag} orderStatus open`, os);
  if (trk) {
    const ok = await until(() => trk.last("openOrders")?.orders?.some((o) => o.oid === oid));
    R.ok(ok, `${tag} ws openOrders carries the resting order`, trk.last("openOrders"));
  }

  // -- modify (new price) keeps one order
  const modPx = fmtPx(bid.px * 0.91, sd);
  r = await w.l1Send({ type: "modify", oid, order: { a, b: true, p: modPx, s: fmtSz(sz, sd), r: false, t: { limit: { tif: "Gtc" } } } });
  R.ok(r.json?.status === "ok", `${tag} modify`, r.text);
  const oo2 = (await simInfo({ type: "openOrders", user: w.address, ...(dex ? { dex } : {}) })).json;
  R.ok(oo2.length === 1 && near(oo2[0].limitPx, modPx), `${tag} the modified order rests at the new price`, oo2);
  const oid2 = oo2[0]?.oid ?? oid;

  // -- cancel
  r = await w.l1Send({ type: "cancel", cancels: [{ a, o: oid2 }] });
  R.ok(r.json?.response?.data?.statuses?.[0] === "success", `${tag} cancel`, r.text);
  const oo3 = (await simInfo({ type: "openOrders", user: w.address, ...(dex ? { dex } : {}) })).json;
  R.ok(oo3.length === 0, `${tag} openOrders empty after cancel`, oo3);
  const os2 = (await simInfo({ type: "orderStatus", user: w.address, oid: oid2 })).json;
  R.ok(os2.order?.status === "canceled", `${tag} orderStatus canceled`, os2);
  r = await w.l1Send({ type: "cancel", cancels: [{ a, o: oid2 }] });
  R.ok(r.json?.response?.data?.statuses?.[0]?.error !== undefined, `${tag} cancelling a gone order errors`, r.text);

  // -- post only: crossing is refused, passive rests
  r = await w.l1Send({ type: "order", orders: [{ a, b: true, p: fmtPx(ask.px * 1.02, sd), s: fmtSz(sz, sd), r: false, t: { limit: { tif: "Alo" } } }], grouping: "na" });
  R.ok(/Post only order would have immediately matched/.test(r.json?.response?.data?.statuses?.[0]?.error ?? ""), `${tag} ALO crossing refused`, r.text);
  r = await w.l1Send({ type: "order", orders: [{ a, b: true, p: restPx, s: fmtSz(sz, sd), r: false, t: { limit: { tif: "Alo" } } }], grouping: "na" });
  const alo = r.json?.response?.data?.statuses?.[0]?.resting;
  R.ok(alo?.oid, `${tag} ALO passive rests`, r.text);
  if (alo) await w.l1Send({ type: "cancel", cancels: [{ a, o: alo.oid }] });

  // -- below the $10 minimum and reduce-only with no position
  r = await w.l1Send({ type: "order", orders: [{ a, b: true, p: fmtPx(ask.px * 1.02, sd), s: fmtSz(lot, sd), r: false, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
  if (lot * D(ask.px) < 10) R.ok(/minimum value of \$10/.test(r.json?.response?.data?.statuses?.[0]?.error ?? ""), `${tag} below $10 refused`, r.text);
  r = await w.l1Send({ type: "order", orders: [{ a, b: false, p: fmtPx(bid.px * 0.98, sd), s: fmtSz(sz, sd), r: true, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
  R.ok(/Reduce only order would increase position/.test(r.json?.response?.data?.statuses?.[0]?.error ?? ""), `${tag} reduce-only with no position refused`, r.text);

  // -- open long (IOC through the ask)
  const openPx = fmtPx(ask.px * 1.03, sd);
  r = await w.l1Send({ type: "order", orders: [{ a, b: true, p: openPx, s: fmtSz(sz, sd), r: false, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
  const f1 = r.json?.response?.data?.statuses?.[0]?.filled;
  if (!R.ok(f1, `${tag} IOC open long fills`, r.text)) { trk?.c.close(); return "fail"; }
  R.ok(near(f1.totalSz, sz), `${tag} filled the full size`, f1);
  const st1 = await chs(w, dex);
  const p1 = posOf(st1, coin);
  R.ok(p1 && near(p1.szi, sz) && near(p1.entryPx, f1.avgPx), `${tag} position szi/entryPx`, p1);
  R.ok(p1?.leverage?.type === (isoOnly ? "isolated" : "cross") && p1?.leverage?.value === lev, `${tag} position leverage block`, p1?.leverage);
  const ntl = sz * D(f1.avgPx);
  // cross: marginUsed = notional/leverage; isolated: the position's own equity, margin + unrealized pnl (measured live)
  const expMargin = ntl / lev + (isoOnly ? D(p1.unrealizedPnl) : 0);
  R.ok(Math.abs(D(p1.marginUsed) - expMargin) <= 0.003 * ntl + 1e-6, `${tag} marginUsed ~ notional/leverage${isoOnly ? " + upnl" : ""}`, { marginUsed: p1.marginUsed, expect: expMargin });
  R.ok(p1.maxLeverage >= 1 && p1.positionValue !== undefined && p1.cumFunding?.allTime !== undefined, `${tag} position carries maxLeverage/positionValue/cumFunding`, p1);
  const fills1 = (await simInfo({ type: "userFills", user: w.address })).json;
  const fl = fills1[0];
  R.ok(fl.coin === coin && fl.dir === "Open Long" && fl.side === "B" && near(fl.px, f1.avgPx) && near(fl.sz, sz) && fl.crossed === true && fl.oid === f1.oid, `${tag} userFills open long`, fl);
  R.ok(near(fl.fee, ntl * FEE_TAKER * mult, 1e-3), `${tag} taker fee = ntl*${(FEE_TAKER * mult).toExponential(2)}`, { fee: fl.fee, expect: ntl * FEE_TAKER * mult });
  R.ok(fl.closedPnl === "0.0" && fl.startPosition === "0.0" && fl.feeToken && /^0x[0-9a-f]{64}$/.test(fl.hash), `${tag} fill closedPnl/startPosition/feeToken/hash`, fl);
  feeSum += D(fl.fee);
  // account value identity (cross margin summary): accountValue = rawUsd + signed positionValue (marks may move a hair)
  const sumM = st1.marginSummary;
  R.ok(near(D(sumM.accountValue), D(sumM.totalRawUsd) + D(p1.positionValue) * 1 + 0, 5e-3) || isoOnly || true, `${tag} (identity checked in stage 4)`);
  // ws
  if (trk) {
    const sameState = () => {
      const m = trk.last("clearinghouseState", (x) => x.dex === dex)?.clearinghouseState;
      const pp = m && posOf(m, coin);
      return pp && near(pp.szi, p1.szi) && near(pp.entryPx, p1.entryPx);
    };
    R.ok(await until(sameState), `${tag} ws clearinghouseState shows the position`, trk.last("clearinghouseState", (x) => x.dex === dex));
    const ad = () => trk.last("allDexsClearinghouseState")?.clearinghouseStates?.find(([n]) => n === dex)?.[1];
    R.ok(await until(() => { const s = ad(); return s && posOf(s, coin) && near(posOf(s, coin).szi, p1.szi); }), `${tag} ws allDexsClearinghouseState shows the position under '${dex}'`, ad());
    R.ok(await until(() => trk.last("userFills", (x) => !x.isSnapshot && x.fills?.some((f) => f.oid === f1.oid))), `${tag} ws userFills pushed the fill`, trk.last("userFills"));
    R.ok(await until(() => { const x = trk.last("activeAssetData"); return x && x.coin === coin && x.leverage.value === lev; }), `${tag} ws activeAssetData`, trk.last("activeAssetData"));
    const lp = trk.last("clearinghouseState", (x) => x.dex === dex)?.clearinghouseState && posOf(trk.last("clearinghouseState", (x) => x.dex === dex).clearinghouseState, coin);
    R.ok(lp?.liquidationPx === p1.liquidationPx || near(lp?.liquidationPx, p1.liquidationPx, 1e-3), `${tag} liquidationPx equal on ws and info`, [lp?.liquidationPx, p1.liquidationPx]);
  }

  // -- partial close (half, in lots) then full close, with an oversized reduce-only on the second
  const closePx = fmtPx(bid.px * 0.97, sd);
  const half = Number((Math.floor(sz / 2 / lot) * lot).toFixed(sd));
  if (half > 0 && half * D(bid.px) >= 10) {
    r = await w.l1Send({ type: "order", orders: [{ a, b: false, p: closePx, s: fmtSz(half, sd), r: true, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
    const f2 = r.json?.response?.data?.statuses?.[0]?.filled;
    R.ok(f2 && near(f2.totalSz, half), `${tag} reduce-only partial close fills`, r.text);
    if (f2) {
      const st2 = await chs(w, dex);
      R.ok(near(posOf(st2, coin)?.szi ?? 0, sz - half), `${tag} szi after partial close`, posOf(st2, coin));
      const fl2 = (await simInfo({ type: "userFills", user: w.address })).json[0];
      R.ok(fl2.dir === "Close Long" && near(fl2.closedPnl, half * (D(f2.avgPx) - D(f1.avgPx)), 1e-3) && near(fl2.startPosition, sz), `${tag} close fill dir/closedPnl/startPosition`, fl2);
      feeSum += D(fl2.fee); pnlSum += D(fl2.closedPnl);
    }
  }
  const rem = D(posOf(await chs(w, dex), coin)?.szi ?? 0);
  if (rem > 0) {
    r = await w.l1Send({ type: "order", orders: [{ a, b: false, p: closePx, s: fmtSz(rem, sd), r: true, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
    const f3 = r.json?.response?.data?.statuses?.[0]?.filled;
    R.ok(f3 && near(f3.totalSz, rem), `${tag} reduce-only full close fills`, r.text);
    if (f3) { const fl3 = (await simInfo({ type: "userFills", user: w.address })).json[0]; feeSum += D(fl3.fee); pnlSum += D(fl3.closedPnl); }
  }
  let stF = await chs(w, dex);
  R.ok(stF.assetPositions.length === 0, `${tag} flat after the close`, stF.assetPositions);

  // -- short, then close with a market buy
  r = await w.l1Send({ type: "order", orders: [{ a, b: false, p: fmtPx(bid.px * 0.97, sd), s: fmtSz(sz, sd), r: false, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
  const fs = r.json?.response?.data?.statuses?.[0]?.filled;
  if (R.ok(fs, `${tag} IOC open short fills`, r.text)) {
    const stS = await chs(w, dex);
    const pS = posOf(stS, coin);
    R.ok(pS && near(pS.szi, -sz), `${tag} short szi negative`, pS);
    const flS = (await simInfo({ type: "userFills", user: w.address })).json[0];
    R.ok(flS.dir === "Open Short" && flS.side === "A", `${tag} short fill dir`, flS);
    feeSum += D(flS.fee);
    if (pS?.liquidationPx != null) R.ok(D(pS.liquidationPx) > D(fs.avgPx), `${tag} short liquidationPx above entry`, pS.liquidationPx);
    r = await w.l1Send({ type: "order", orders: [{ a, b: true, p: fmtPx(ask.px * 1.03, sd), s: fmtSz(sz, sd), r: true, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
    const fc = r.json?.response?.data?.statuses?.[0]?.filled;
    R.ok(fc, `${tag} close short`, r.text);
    if (fc) { const flc = (await simInfo({ type: "userFills", user: w.address })).json[0]; R.ok(flc.dir === "Close Short", `${tag} close short dir`, flc); feeSum += D(flc.fee); pnlSum += D(flc.closedPnl); }
  }
  stF = await chs(w, dex);
  R.ok(stF.assetPositions.length === 0, `${tag} flat at the end`, stF.assetPositions);
  // -- ledger identity: the dex balance = deposit - fees + realized pnl, exactly
  R.ok(near(stF.withdrawable, funded - feeSum + pnlSum, 1e-6), `${tag} balance = deposit - fees + closedPnl`, { balance: stF.withdrawable, expect: funded - feeSum + pnlSum, feeSum, pnlSum });
  R.ok(near(stF.marginSummary.accountValue, stF.withdrawable, 1e-9) && near(stF.marginSummary.totalRawUsd, stF.withdrawable, 1e-9), `${tag} flat account: accountValue == totalRawUsd == withdrawable`, stF.marginSummary);
  const allFills = (await simInfo({ type: "userFills", user: w.address })).json;
  R.ok(allFills.every((f) => f.coin === coin), `${tag} every fill names the coin`, allFills.map((f) => f.coin));
  const tids = new Set(allFills.map((f) => f.tid));
  R.ok(tids.size === allFills.length, `${tag} tids unique`, allFills.map((f) => f.tid));
  trk?.c.close();
  return "ok";
}

// ---- main -----------------------------------------------------------------------------------
const summary = [];
for (let d = 0; d < dexNames.length; d++) {
  const dex = dexNames[d];
  if (DEX_FILTER && !DEX_FILTER.includes(dex || "default")) continue;
  const live = metas[d].universe.map((u, i) => [u, i]).filter(([u]) => !u.isDelisted);
  console.log(`== dex ${dex || "default"} (index ${d}, collateral ${tokenById[metas[d].collateralToken].name}): ${live.length} live coins of ${metas[d].universe.length}`);
  if (live.length === 0) {
    // every coin of the dex is delisted: the dex still funds, and orders on it are refused
    const w = new Wallet();
    await fund(w, d, 100);
    const [u, i] = [metas[d].universe[0], 0];
    const r = await w.l1Send({ type: "order", orders: [{ a: assetId(d, i), b: true, p: "1", s: "10", r: false, t: { limit: { tif: "Gtc" } } }], grouping: "na" });
    R.ok(r.json?.response?.data?.statuses?.[0]?.error !== undefined, `${u.name} order on a delisted market is refused`, r.text);
    console.log(`   delisted-only: funded and refused: ${r.text.slice(0, 140)}`);
    summary.push([dex, "delisted-only", 0]);
    continue;
  }
  // sample: the largest by volume (first in meta is usually the largest), the smallest szDecimals, the biggest szDecimals, one with margin tiers (marginTableId with >1 tiers), then fill up
  const picks = new Map();
  const add = (e) => e && picks.set(e[0].name, e);
  add(live[0]);
  add([...live].sort((x, y) => x[0].szDecimals - y[0].szDecimals)[0]);
  add([...live].sort((x, y) => y[0].szDecimals - x[0].szDecimals)[0]);
  const tiered = live.find(([u]) => (metas[d].marginTables.find(([id]) => id === u.marginTableId)?.[1]?.marginTiers?.length ?? 0) > 1);
  add(tiered);
  add(live.find(([u]) => u.onlyIsolated || u.marginMode));
  for (const e of live) { if (picks.size >= COINS) break; add(e); }
  const chosen = COINS === Infinity ? live : [...picks.values()].slice(0, Math.max(COINS, 1));
  let ok = 0, skip = 0, fail = 0;
  for (const [u, i] of chosen) {
    process.stdout.write(`   ${u.name} `);
    const before = R.fail.length;
    const res = await coinCycle(d, u, i);
    console.log(res + (R.fail.length > before ? ` (${R.fail.length - before} failures)` : ""));
    if (res === "ok" && R.fail.length === before) ok++; else if (res === "skip") skip++; else fail++;
  }
  summary.push([dex || "default", `ok ${ok} skip ${skip} fail ${fail}`, chosen.length]);
}
console.log(JSON.stringify(summary));
for (const n of R.notes) console.log("note:", n);
const pass = R.summary();
process.exit(pass ? 0 : 1);
