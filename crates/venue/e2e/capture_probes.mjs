// Capture extra zero-signature /exchange answers from REAL (nothing validly signed) into the fixture.
import fs from "node:fs";
import { sleep } from "./lib.mjs";
const FIX = "../src/api/fixtures/real_exchange.json";
const fx = JSON.parse(fs.readFileSync(FIX, "utf8"));
const zero = { r: "0x" + "0".repeat(64), s: "0x" + "0".repeat(64), v: 27 };
const N = 1794856594300;
const U = "0x0000000000000000000000000000000000000001";
const T = "USDC:0x6d1e7cde53ba9467b783cb7c530ce054";
const ua = (type, extra) => ({ type, hyperliquidChain: "Mainnet", signatureChainId: "0x66eee", ...extra });
const ucs = (extra, drop) => { const a = ua("usdClassTransfer", { amount: "1", toPerp: true, nonce: N, ...extra }); (drop || []).forEach((k) => delete a[k]); return a; };
const ord = (extra = {}) => ({ a: 0, b: true, p: "1", s: "1", r: false, t: { limit: { tif: "Gtc" } }, ...extra });
const cases = [
  ["spotSend_time_not_nonce", { action: ua("spotSend", { destination: U, token: T, amount: "1", time: N }), nonce: N + 5 }],
  ["usdClassTransfer_nonce_not_outer", { action: ucs({}), nonce: N + 5 }],
  ["sendAsset_nonce_not_outer", { action: ua("sendAsset", { destination: U, sourceDex: "spot", destinationDex: "", token: T, amount: "1", fromSubAccount: "", nonce: N }), nonce: N + 5 }],
  ["usdClassTransfer_testnet", { action: ucs({ hyperliquidChain: "Testnet" }), nonce: N }],
  ["usdClassTransfer_chain_lowercase", { action: ucs({ hyperliquidChain: "mainnet" }), nonce: N }],
  ["usdClassTransfer_chain_missing", { action: ucs({}, ["hyperliquidChain"]), nonce: N }],
  ["usdClassTransfer_sigchain_no_prefix", { action: ucs({ signatureChainId: "66eee" }), nonce: N }],
  ["usdClassTransfer_sigchain_empty_hex", { action: ucs({ signatureChainId: "0x" }), nonce: N }],
  ["usdClassTransfer_sigchain_number", { action: ucs({ signatureChainId: 421614 }), nonce: N }],
  ["usdClassTransfer_sigchain_huge", { action: ucs({ signatureChainId: "0xffffffffffffffffffff" }), nonce: N }],
  ["usdClassTransfer_extra_field", { action: ucs({ junk: 1 }), nonce: N }],
  ["usdClassTransfer_amount_number", { action: ucs({ amount: 1 }), nonce: N }],
  ["usdClassTransfer_nonce_string", { action: ucs({ nonce: String(N) }), nonce: N }],
  ["spotSend_destination_not_an_address", { action: ua("spotSend", { destination: "0x12", token: T, amount: "1", time: N }), nonce: N }],
  ["sendAsset_destination_not_an_address", { action: ua("sendAsset", { destination: "junk", sourceDex: "spot", destinationDex: "", token: T, amount: "1", fromSubAccount: "", nonce: N }), nonce: N }],
  ["sendAsset_missing_fromSubAccount", { action: ua("sendAsset", { destination: U, sourceDex: "spot", destinationDex: "", token: T, amount: "1", nonce: N }), nonce: N }],
  ["approveAgent_bad_address", { action: ua("approveAgent", { agentAddress: "0x12", agentName: "x", nonce: N }), nonce: N }],
  ["userSetAbstraction_unknown", { action: ua("userSetAbstraction", { user: U, abstraction: "zzz", nonce: N }), nonce: N }],
  ["order_extra_key_on_action", { action: { type: "order", orders: [ord()], grouping: "na", zz: 1 }, nonce: N }],
  ["order_bad_tif", { action: { type: "order", orders: [ord({ t: { limit: { tif: "Zz" } } })], grouping: "na" }, nonce: N }],
  ["order_bad_grouping", { action: { type: "order", orders: [ord()], grouping: "zz" }, nonce: N }],
  ["updateLeverage_missing_isCross", { action: { type: "updateLeverage", asset: 0, leverage: 5 }, nonce: N }],
  ["updateLeverage_zero", { action: { type: "updateLeverage", asset: 0, isCross: true, leverage: 0 }, nonce: N }],
  ["cancel_extra_key", { action: { type: "cancel", cancels: [{ a: 0, o: 1 }], zz: 1 }, nonce: N }],
  ["noop_extra_key", { action: { type: "noop", zz: 1 }, nonce: N }],
  ["noop_expired", { action: { type: "noop" }, nonce: N, expiresAfter: 1000 }],
];
const have = new Set(fx.probes.map((p) => p.name));
for (const [name, o] of cases) {
  if (have.has(name)) continue;
  const request = JSON.stringify({ ...o, signature: zero });
  const r = await fetch("https://api.hyperliquid.xyz/exchange", { method: "POST", headers: { "content-type": "application/json" }, body: request });
  const body = await r.text();
  fx.probes.push({ name, request, status: r.status, contentType: r.headers.get("content-type"), body });
  console.log(name.padEnd(40), r.status, body.slice(0, 80));
  await sleep(450);
}
fs.writeFileSync(FIX, JSON.stringify(fx, null, 2) + "\n");
