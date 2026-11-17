// Stage 3: the transfer matrix over HTTP against the LOCAL sim: spot <-> default perp, spot <-> every builder dex perp,
// dex <-> dex, user -> user (usdSend, spotSend, sendAsset), the activation fee, unified vs standard accounts, error cases.
import { Wallet, ctrl, simInfo, assetId, Report, sleep, wsConnect, SIM_WS } from "./lib.mjs";

const R = new Report("stage3");
const dexList = (await simInfo({ type: "perpDexs" })).json;
const dexNames = dexList.map((d) => (d ? d.name : ""));
const metas = (await simInfo({ type: "allPerpMetas" })).json;
const spotMeta = (await simInfo({ type: "spotMeta" })).json;
const tokenById = Object.fromEntries(spotMeta.tokens.map((t) => [t.index, t]));
const tokenStr = (idx) => `${tokenById[idx].name}:${tokenById[idx].tokenId}`;
const D = Number;
const near = (a, b, tol = 1e-9) => Math.abs(D(a) - D(b)) <= tol * Math.max(1, Math.abs(D(b)));

const chs = async (w, dex) => (await simInfo({ type: "clearinghouseState", user: w.address, ...(dex ? { dex } : {}) })).json;
const spotBal = async (w, token) => {
  const s = (await simInfo({ type: "spotClearinghouseState", user: w.address })).json;
  return D(s.balances.find((b) => b.token === token)?.total ?? 0);
};
async function allDexs(w) {
  const c = await wsConnect(SIM_WS);
  c.sub({ type: "allDexsClearinghouseState", user: w.address });
  for (let i = 0; i < 40; i++) { const f = c.frames.find((x) => x.msg.channel === "allDexsClearinghouseState"); if (f) { c.close(); return f.msg.data; } await sleep(100); }
  c.close();
}
const deal = (w, coin, amount) => ctrl("/control/deal", { address: w.address, class: "spot", coin, amount: String(amount) });
const sendAsset = (w, dest, src, dst, token, amount) => w.userSend("sendAsset", { destination: dest, sourceDex: src, destinationDex: dst, token: tokenStr(token), amount: String(amount), fromSubAccount: "" });
const okRes = (r) => r.json?.status === "ok";
const errText = (r) => (r.json?.status === "err" ? r.json.response : null);

// ---- A: spot <-> default perp, spot <-> each dex perp (every collateral token), dex <-> dex --------------------
{
  const w = new Wallet();
  await deal(w, "USDC", 10000);
  let r = await w.userSend("usdClassTransfer", { amount: "1000", toPerp: true });
  R.ok(okRes(r) && r.json.response?.type === "default", "usdClassTransfer spot->perp answers ok/default", r.text);
  R.ok(near((await chs(w, "")).withdrawable, 1000) && (await spotBal(w, 0)) === 9000, "default perp 1000, spot 9000");
  r = await w.userSend("usdClassTransfer", { amount: "250.5", toPerp: false });
  R.ok(okRes(r) && near((await chs(w, "")).withdrawable, 749.5) && near(await spotBal(w, 0), 9250.5), "usdClassTransfer perp->spot 250.5");
  r = await w.userSend("usdClassTransfer", { amount: "99999", toPerp: true });
  R.ok(errText(r), "usdClassTransfer above the spot balance is refused", r.text);
  console.log("   over-balance usdClassTransfer:", r.text.slice(0, 200));
  r = await w.userSend("usdClassTransfer", { amount: "0", toPerp: true });
  R.ok(errText(r), "usdClassTransfer of 0 is refused", r.text);
  console.log("   zero usdClassTransfer:", r.text.slice(0, 200));

  // every dex, with its collateral token
  const usedCollat = new Set();
  for (let d = 1; d < dexNames.length; d++) {
    const dex = dexNames[d], c = metas[d].collateralToken, name = tokenById[c].name;
    if (!usedCollat.has(c)) { await deal(w, name, 1000); usedCollat.add(c); }
    const before = await spotBal(w, c);
    r = await sendAsset(w, w.address, "spot", dex, c, 100);
    R.ok(okRes(r), `sendAsset spot->${dex} (${name})`, r.text);
    R.ok(near((await chs(w, dex)).withdrawable, 100) && near(await spotBal(w, c), before - 100), `${dex}: perp 100, spot -100`);
    const row = (await allDexs(w))?.clearinghouseStates?.find(([n]) => n === dex);
    R.ok(row && near(row[1].withdrawable, 100) && near(row[1].marginSummary.accountValue, 100), `${dex}: allDexsClearinghouseState row`, row);
    // the wrong token is refused
    const other = c === 0 ? 360 : 0;
    if (tokenById[other]) {
      await deal(w, tokenById[other].name, 50);
      r = await sendAsset(w, w.address, "spot", dex, other, 1);
      R.ok(errText(r), `${dex}: a non-collateral token is refused`, r.text);
    }
    // back
    r = await sendAsset(w, w.address, dex, "spot", c, 40);
    R.ok(okRes(r) && near((await chs(w, dex)).withdrawable, 60) && near(await spotBal(w, c), before - 60), `sendAsset ${dex}->spot 40`, r.text);
  }
  // dex <-> dex: same collateral ok, different refused
  const idx = (n) => dexNames.indexOf(n);
  r = await sendAsset(w, w.address, "xyz", "para", 0, 10);
  R.ok(okRes(r) && near((await chs(w, "xyz")).withdrawable, 50) && near((await chs(w, "para")).withdrawable, 70), "xyz->para (both USDC)", r.text);
  r = await sendAsset(w, w.address, "", "xyz", 0, 10);
  R.ok(okRes(r), "default->xyz (both USDC)", r.text);
  r = await sendAsset(w, w.address, "xyz", "flx", 0, 10);
  R.ok(errText(r), "xyz (USDC)->flx (USDH) refused", r.text);
  console.log("   dex->dex collateral mismatch:", r.text.slice(0, 240));
  r = await sendAsset(w, w.address, "flx", "km", 360, 10);
  R.ok(okRes(r), "flx->km (both USDH)", r.text);
  r = await sendAsset(w, w.address, "hyna", "cash", 235, 10);
  R.ok(errText(r), "hyna (USDE)->cash (USDT0) refused", r.text);
  r = await sendAsset(w, w.address, "nosuchdex", "spot", 0, 1);
  R.ok(errText(r), "unknown source dex refused", r.text);
  console.log("   unknown dex:", r.text.slice(0, 200));
  r = await sendAsset(w, w.address, "xyz", "xyz", 0, 1);
  R.ok(errText(r), "same wallet refused", r.text);
  r = await sendAsset(w, w.address, "xyz", "spot", 0, 100000);
  R.ok(errText(r), "over-balance from a dex refused", r.text);
  console.log("   over-balance from dex:", r.text.slice(0, 220));
  // withdraw3 refused (no bridge)
  r = await w.userSend("withdraw3", { destination: w.address, amount: "1" });
  R.ok(errText(r), "withdraw3 refused with its reason", r.text);
  // transfer ledger rows (userNonFundingLedgerUpdates) exist for the account
  const led = await simInfo({ type: "userNonFundingLedgerUpdates", user: w.address, startTime: 0 });
  console.log("   userNonFundingLedgerUpdates:", led.status, String(led.text).slice(0, 160));
}

// ---- B: user -> user and the activation fee ------------------------------------------------------------------
{
  const A = new Wallet(), B = new Wallet(), C = new Wallet();
  await deal(A, "USDC", 1000);
  await A.userSend("usdClassTransfer", { amount: "500", toPerp: true });
  // A is funded by a deposit (deal): its first outbound transfer to B pays no fee? the rule says: owed set when the account
  // first RECEIVES funds by a transfer. A dealt account does not owe.
  let r = await A.userSend("usdSend", { destination: B.address, amount: "10" });
  R.ok(okRes(r), "usdSend A->B", r.text);
  const a1 = D((await chs(A, "")).withdrawable), b1 = D((await chs(B, "")).withdrawable);
  console.log(`   after usdSend A->B 10: A ${a1} (500-10-fee), B ${b1}`);
  R.ok(near(b1, 10), "B received exactly 10 (fee is on top of the amount, paid by the sender)", b1);
  R.ok(near(a1, 490), "A dealt via a deposit pays no activation fee on its first send", a1);
  // B is a new account created by a transfer: it owes 1 USDC on its first outbound transfer, on top of the amount
  r = await B.userSend("usdSend", { destination: C.address, amount: "5" });
  R.ok(okRes(r), "usdSend B->C", r.text);
  const b2 = D((await chs(B, "")).withdrawable), c2 = D((await chs(C, "")).withdrawable);
  R.ok(near(b2, 10 - 5 - 1), "B pays 5 + 1 USDC activation on its first outbound transfer", b2);
  R.ok(near(c2, 5), "C receives 5", c2);
  r = await B.userSend("usdSend", { destination: C.address, amount: "1" });
  const b3 = D((await chs(B, "")).withdrawable);
  R.ok(okRes(r) && near(b3, b2 - 1), "the fee is charged once", b3);
  r = await B.userSend("usdSend", { destination: C.address, amount: "100" });
  R.ok(errText(r), "usdSend over the balance is refused", r.text);
  console.log("   usdSend over balance:", r.text.slice(0, 200));
  // a send that only covers the amount but not the fee is refused: a fresh D funded 5 by A, first send of 5
  const E = new Wallet(), F = new Wallet();
  await A.userSend("usdSend", { destination: E.address, amount: "5" });
  r = await E.userSend("usdSend", { destination: F.address, amount: "5" });
  R.ok(errText(r) && /activation/i.test(r.json.response), "amount without the fee headroom is refused naming the activation fee", r.text);
  console.log("   amount w/o fee headroom:", r.text.slice(0, 240));

  // spotSend and sendAsset to another user
  await deal(A, "USDC", 100);
  const G = new Wallet();
  r = await A.userSend("spotSend", { destination: G.address, token: "USDC:0x6d1e7cde53ba9467b783cb7c530ce054", amount: "20" });
  R.ok(okRes(r) && near(await spotBal(G, 0), 20), "spotSend A->G 20 USDC", r.text);
  const H = new Wallet();
  r = await sendAsset(A, H.address, "spot", "", 0, 7);
  R.ok(okRes(r) && near((await chs(H, "")).withdrawable, 7), "sendAsset A spot -> H default perp", r.text);
  const I = new Wallet();
  r = await sendAsset(A, I.address, "", "xyz", 0, 3);
  R.ok(okRes(r) && near((await chs(I, "xyz")).withdrawable, 3), "sendAsset A default perp -> I xyz perp", r.text);
  // non-USDC to another user: activation fee comes from spot USDC (G owes 1: it was created by A's transfer)
  await deal(G, "USDH", 30);
  const usdc0 = await spotBal(G, 0);
  r = await G.userSend("spotSend", { destination: H.address, token: tokenStr(360), amount: "10" });
  R.ok(okRes(r), "spotSend USDH G->H", r.text);
  R.ok(near(await spotBal(G, 0), usdc0 - 1) && near(await spotBal(G, 360), 20), "G pays the 1 USDC activation from spot USDC, USDH 10 sent", { usdc: await spotBal(G, 0), usdh: await spotBal(G, 360) });
  // self transfer is not outbound: no fee
  r = await spotSendSelf();
  async function spotSendSelf() { return A.userSend("spotSend", { destination: A.address, token: "USDC:0x6d1e7cde53ba9467b783cb7c530ce054", amount: "1" }); }
  console.log("   spotSend to self:", r.text.slice(0, 200));
  // system address destinations are refused over /exchange
  r = await A.userSend("spotSend", { destination: "0x2000000000000000000000000000000000000000", token: "USDC:0x6d1e7cde53ba9467b783cb7c530ce054", amount: "1" });
  R.ok(errText(r), "spotSend to the USDC system address refused over /exchange", r.text);
  // an ordinary wallet that merely starts with 0x20 is a normal destination
  r = await A.userSend("spotSend", { destination: "0x20a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3", token: "USDC:0x6d1e7cde53ba9467b783cb7c530ce054", amount: "1" });
  R.ok(okRes(r), "spotSend to a wallet starting 0x20 works", r.text);
}

// ---- C: unified vs standard accounts -------------------------------------------------------------------------
{
  const w = new Wallet();
  await deal(w, "USDC", 2000);
  let r = await w.userSend("userSetAbstraction", { user: w.address, abstraction: "unifiedAccount" });
  R.ok(okRes(r), "userSetAbstraction unifiedAccount", r.text);
  const ab = (await simInfo({ type: "userAbstraction", user: w.address })).json;
  R.ok(ab === "unifiedAccount", "userAbstraction reads unifiedAccount", ab);
  const sp = (await simInfo({ type: "spotClearinghouseState", user: w.address })).json;
  console.log("   unified spotClearinghouseState keys:", Object.keys(sp).join(","));
  // trade on default dex from the unified pool: no transfer needed on real; here
  const book = (await simInfo({ type: "l2Book", coin: "BTC" })).json;
  const ask = D(book.levels[1][0].px);
  const a = 0;
  r = await w.l1Send({ type: "order", orders: [{ a, b: true, p: String(Math.round(ask * 1.02)), s: "0.002", r: false, t: { limit: { tif: "Ioc" } } }], grouping: "na" });
  console.log("   unified order on default dex:", r.text.slice(0, 220));
  const st = await chs(w, "");
  console.log("   unified chs:", JSON.stringify(st.marginSummary), "withdrawable", st.withdrawable);
  const spb = await spotBal(w, 0);
  console.log("   unified spot USDC after order:", spb);
  r = await w.userSend("userSetAbstraction", { user: w.address, abstraction: "portfolioMargin" });
  console.log("   portfolioMargin:", r.text.slice(0, 200));
  r = await w.userSend("userSetAbstraction", { user: w.address, abstraction: "nonsense" });
  R.ok(errText(r) || /deserialize/i.test(r.text), "unknown abstraction refused (422 like real)", r.text);
  r = await w.userSend("userSetAbstraction", { user: new Wallet().address, abstraction: "disabled" });
  R.ok(errText(r), "setting another user's abstraction is refused", r.text);
  r = await w.userSend("userSetAbstraction", { user: w.address, abstraction: "disabled" });
  R.ok(okRes(r), "back to disabled (standard)", r.text);
  // standard account: cannot withdraw margin held
  r = await w.userSend("usdClassTransfer", { amount: "1999", toPerp: false });
  console.log("   standard: perp->spot of nearly everything with an open position:", r.text.slice(0, 220));
}

// ---- D: bad signatures, bad shapes ------------------------------------------------------------------------------
{
  const w = new Wallet();
  await deal(w, "USDC", 100);
  const wire = { type: "usdClassTransfer", hyperliquidChain: "Mainnet", signatureChainId: "0x66eee", amount: "1", toPerp: true, nonce: Date.now() };
  const zero = { r: "0x" + "0".repeat(64), s: "0x" + "0".repeat(64), v: 27 };
  let r = await (await import("./lib.mjs")).simExchange({ action: wire, nonce: wire.nonce, signature: zero, vaultAddress: null, expiresAfter: null });
  R.ok(r.json?.status === "err" && r.json.response === "Unable to recover signer.", "zero signature -> Unable to recover signer.", r.text);
  r = await (await import("./lib.mjs")).simExchange("not json");
  R.ok(r.status === 400 && /Failed to parse the request body as JSON/.test(r.text), "non-JSON body: 400", [r.status, r.text]);
  r = await (await import("./lib.mjs")).simExchange({ action: { type: "nopeNotAnAction" }, nonce: 1, signature: zero });
  R.ok(r.status === 422, "unknown action type: 422", [r.status, r.text]);
}
const pass = R.summary();
process.exit(pass ? 0 : 1);
