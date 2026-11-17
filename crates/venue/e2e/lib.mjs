// Shared helpers for the venue-sim end-to-end proofs. Real Hyperliquid is only ever read (info + ws);
// every order/transfer/sign goes to the LOCAL sim.
import { privateKeyToAccount, generatePrivateKey } from "viem/accounts";
import { keccak256, hexToBytes, concat } from "viem";
import WebSocket from "ws";

export const SIM = process.env.SIM_INFO || "http://127.0.0.1:8090";
export const CTRL = process.env.SIM_CTRL || "http://127.0.0.1:8091";
export const SIM_WS = SIM.replace(/^http/, "ws") + "/ws";
export const REAL = "https://api.hyperliquid.xyz";
export const REAL_WS = "wss://api.hyperliquid.xyz/ws";
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- read-only real access, paced (budget ~1200 weight/min) --------------
let lastReal = 0;
export async function realInfo(body, minGap = 300) {
  if (body.type === "exchange") throw new Error("never send /exchange to real");
  for (let i = 0; i < 6; i++) {
    const wait = lastReal + minGap - Date.now();
    if (wait > 0) await sleep(wait);
    lastReal = Date.now();
    const r = await fetch(REAL + "/info", { method: "POST", headers: { "content-type": "application/json" }, body: typeof body === "string" ? body : JSON.stringify(body) });
    if (r.status === 429) { await sleep(5000 * (i + 1)); continue; }
    return await wrap(r);
  }
  throw new Error("real 429 persistent");
}
async function wrap(r) {
  const text = await r.text();
  let json;
  try { json = JSON.parse(text); } catch { json = undefined; }
  return { status: r.status, ct: r.headers.get("content-type"), text, json };
}
export async function simInfo(body) {
  const r = await fetch(SIM + "/info", { method: "POST", headers: { "content-type": "application/json" }, body: typeof body === "string" ? body : JSON.stringify(body) });
  return await wrap(r);
}
export async function simExchange(payload) {
  const r = await fetch(SIM + "/exchange", { method: "POST", headers: { "content-type": "application/json" }, body: typeof payload === "string" ? payload : JSON.stringify(payload) });
  return await wrap(r);
}
export async function ctrl(path, body) {
  const r = await fetch(CTRL + path, { method: body === undefined ? "GET" : "POST", headers: { "content-type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body) });
  return await wrap(r);
}

// ---- msgpack (the subset the L1 hash needs; maps keep insertion order) ----
export function msgpack(v) {
  const out = [];
  const w = (...b) => out.push(...b);
  const u = (n, bytes) => { for (let i = bytes - 1; i >= 0; i--) out.push(Number((BigInt(n) >> BigInt(8 * i)) & 0xffn)); };
  const enc = (x) => {
    if (x === null || x === undefined) return w(0xc0);
    if (x === true) return w(0xc3);
    if (x === false) return w(0xc2);
    if (typeof x === "number" || typeof x === "bigint") {
      const n = BigInt(x);
      if (n >= 0n) {
        if (n < 128n) return w(Number(n));
        if (n < 256n) { w(0xcc); return u(n, 1); }
        if (n < 65536n) { w(0xcd); return u(n, 2); }
        if (n < 4294967296n) { w(0xce); return u(n, 4); }
        w(0xcf); return u(n, 8);
      }
      if (n >= -32n) return w(Number(n & 0xffn));
      if (n >= -128n) { w(0xd0); return u(n & 0xffn, 1); }
      if (n >= -32768n) { w(0xd1); return u(n & 0xffffn, 2); }
      if (n >= -2147483648n) { w(0xd2); return u(n & 0xffffffffn, 4); }
      w(0xd3); return u(BigInt.asUintN(64, n), 8);
    }
    if (typeof x === "string") {
      const b = Buffer.from(x, "utf8");
      if (b.length < 32) w(0xa0 | b.length); else if (b.length < 256) { w(0xd9); u(b.length, 1); } else { w(0xda); u(b.length, 2); }
      for (const c of b) out.push(c);
      return;
    }
    if (Array.isArray(x)) {
      if (x.length < 16) w(0x90 | x.length); else { w(0xdc); u(x.length, 2); }
      return x.forEach(enc);
    }
    const keys = Object.keys(x);
    if (keys.length < 16) w(0x80 | keys.length); else { w(0xde); u(keys.length, 2); }
    for (const k of keys) { enc(k); enc(x[k]); }
  };
  enc(v);
  return Uint8Array.from(out);
}

const split = (sig) => ({ r: "0x" + sig.slice(2, 66), s: "0x" + sig.slice(66, 130), v: parseInt(sig.slice(130, 132), 16) });
const T = (name, type) => ({ name, type });
const HC = T("hyperliquidChain", "string");
const specs = {
  usdSend: ["UsdSend", [HC, T("destination", "string"), T("amount", "string"), T("time", "uint64")]],
  spotSend: ["SpotSend", [HC, T("destination", "string"), T("token", "string"), T("amount", "string"), T("time", "uint64")]],
  withdraw3: ["Withdraw", [HC, T("destination", "string"), T("amount", "string"), T("time", "uint64")]],
  usdClassTransfer: ["UsdClassTransfer", [HC, T("amount", "string"), T("toPerp", "bool"), T("nonce", "uint64")]],
  sendAsset: ["SendAsset", [HC, T("destination", "string"), T("sourceDex", "string"), T("destinationDex", "string"), T("token", "string"), T("amount", "string"), T("fromSubAccount", "string"), T("nonce", "uint64")]],
  userSetAbstraction: ["UserSetAbstraction", [HC, T("user", "address"), T("abstraction", "string"), T("nonce", "uint64")]],
  userDexAbstraction: ["UserDexAbstraction", [HC, T("user", "address"), T("enabled", "bool"), T("nonce", "uint64")]],
  approveAgent: ["ApproveAgent", [HC, T("agentAddress", "address"), T("agentName", "string"), T("nonce", "uint64")]],
  approveBuilderFee: ["ApproveBuilderFee", [HC, T("maxFeeRate", "string"), T("builder", "address"), T("nonce", "uint64")]],
};

// ---- wallets ----------------------------------------------------------------
export class Wallet {
  constructor(pk = generatePrivateKey()) {
    this.pk = pk;
    this.account = privateKeyToAccount(pk);
    this.address = this.account.address.toLowerCase();
    this.n = Date.now();
  }
  nonce() { return ++this.n; }
  async l1(action, { vault = null, expiresAfter = null } = {}) {
    const nonce = this.nonce();
    const parts = [msgpack(action), hexToBytes("0x" + BigInt(nonce).toString(16).padStart(16, "0")), vault ? concat([Uint8Array.from([1]), hexToBytes(vault)]) : Uint8Array.from([0])];
    if (expiresAfter != null) parts.push(Uint8Array.from([0]), hexToBytes("0x" + BigInt(expiresAfter).toString(16).padStart(16, "0")));
    const connectionId = keccak256(concat(parts));
    const sig = await this.account.signTypedData({
      domain: { name: "Exchange", version: "1", chainId: 1337, verifyingContract: "0x0000000000000000000000000000000000000000" },
      types: { Agent: [{ name: "source", type: "string" }, { name: "connectionId", type: "bytes32" }] },
      primaryType: "Agent",
      message: { source: "a", connectionId },
    });
    return { action, nonce, signature: split(sig), vaultAddress: vault, expiresAfter };
  }
  async l1Send(action, opts) { return simExchange(await this.l1(action, opts)); }
  // user-signed action sender: w.userSend("sendAsset", {...fields without nonce})
  async userSend(type, msg) {
    const [primary, fields] = specs[type];
    const m = { ...msg };
    if (fields.some((f) => f.name === "nonce") && m.nonce == null) m.nonce = this.nonce();
    if (fields.some((f) => f.name === "time") && m.time == null) m.time = this.nonce();
    const wire = { type, hyperliquidChain: "Mainnet", signatureChainId: "0x66eee", ...m };
    const full = { hyperliquidChain: "Mainnet", ...m };
    const message = {};
    for (const f of fields) message[f.name] = f.type === "uint64" ? BigInt(full[f.name]) : full[f.name];
    const sig = await this.account.signTypedData({
      domain: { name: "HyperliquidSignTransaction", version: "1", chainId: 0x66eee, verifyingContract: "0x0000000000000000000000000000000000000000" },
      types: { [`HyperliquidTransaction:${primary}`]: fields },
      primaryType: `HyperliquidTransaction:${primary}`,
      message,
    });
    return simExchange({ action: wire, nonce: m.nonce ?? m.time, signature: split(sig), vaultAddress: null, expiresAfter: null });
  }
}

// asset id of coin index i in dex index d (0 = default dex)
export const assetId = (d, i) => (d === 0 ? i : 100000 + d * 10000 + i);

// ---- decimals --------------------------------------------------------------
export const approx = (a, b, tol = 1e-6) => Math.abs(Number(a) - Number(b)) <= tol * Math.max(1, Math.abs(Number(b)));

// price formatting per the venue: <=5 significant figures and <= (6 - szDecimals) decimals (perp)
export function fmtPx(px, szDecimals) {
  let p = Number(px);
  if (Number.isInteger(p) && Math.abs(p) >= 1e5) return String(p);
  let s = Number(p.toPrecision(5));
  const maxDec = Math.max(0, 6 - szDecimals);
  s = Number(s.toFixed(maxDec));
  return s.toString();
}
export function fmtSz(sz, szDecimals) { return Number(sz).toFixed(szDecimals).replace(/\.?0+$/, "") || "0"; }

// ---- check recorder ----------------------------------------------------------
export class Report {
  constructor(name) { this.name = name; this.pass = 0; this.fail = []; this.notes = []; }
  ok(cond, label, detail) {
    if (cond) this.pass++;
    else { this.fail.push({ label, detail }); console.log("  FAIL", label, detail === undefined ? "" : JSON.stringify(detail).slice(0, 500)); }
    return cond;
  }
  note(s) { this.notes.push(s); }
  summary() { console.log(`[${this.name}] pass=${this.pass} fail=${this.fail.length}`); return this.fail.length === 0; }
}

// ---- ws client ---------------------------------------------------------------
export function wsConnect(url, onMsg) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const frames = [];
    ws.on("message", (d) => {
      let j;
      try { j = JSON.parse(d.toString()); } catch { j = d.toString(); }
      const f = { t: Date.now(), msg: j };
      frames.push(f);
      onMsg?.(f);
    });
    ws.on("open", () => resolve({
      ws, frames,
      sub: (s) => ws.send(JSON.stringify({ method: "subscribe", subscription: s })),
      unsub: (s) => ws.send(JSON.stringify({ method: "unsubscribe", subscription: s })),
      send: (o) => ws.send(JSON.stringify(o)),
      close: () => ws.close(),
    }));
    ws.on("error", reject);
  });
}
