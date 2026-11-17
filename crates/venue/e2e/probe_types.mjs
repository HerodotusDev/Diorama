// Which info types does the real venue serve (read-only), and does the sim answer them?
import { simInfo, realInfo } from "./lib.mjs";
const U = process.env.REAL_USER || "0xd77a0944d0039e92f3309f9cf52b6926b7c14b6d";
const types = [
  ["userFees", { user: U }], ["historicalOrders", { user: U }], ["userRateLimit", { user: U }], ["userFillsByTime", { user: U, startTime: Date.now() - 86400000 }],
  ["allDexsClearinghouseState", { user: U }], ["allDexsAssetPositions", { user: U }], ["recentTrades", { coin: "BTC" }], ["userRole", { user: U }], ["subAccounts", { user: U }],
  ["referral", { user: U }], ["portfolio", { user: U }], ["userVaultEquities", { user: U }], ["delegations", { user: U }], ["extraAgents", { user: U }], ["twapHistory", { user: U }],
  ["userTwapSliceFills", { user: U }], ["webData2", { user: U }], ["webData3", { user: U }], ["spotMetaAndAssetCtxs", {}], ["metaAndAssetCtxs", {}], ["userDexAbstraction", { user: U }],
  ["perpDexLimits", { dex: "xyz" }], ["perpDexStatus", { dex: "xyz" }], ["delegatorSummary", { user: U }], ["userBorrowLendState", { user: U }], ["activeAssetData", { user: U, coin: "BTC" }],
  ["userOutcome", { user: U }], ["validatorSummaries", {}], ["userToMultiSigSigners", { user: U }], ["legalCheck", { user: U }], ["isVip", { user: U }], ["liquidatable", {}], ["maxMarketOrderNtls", {}],
  ["clearinghouseState", { user: U, dex: "ALL_DEXS" }], ["openOrders", { user: U, dex: "ALL_DEXS" }], ["frontendOpenOrders", { user: U, dex: "ALL_DEXS" }],
];
for (const [type, args] of types) {
  const body = { type, ...args };
  const r = await realInfo(body);
  const s = await simInfo(body);
  console.log(type.padEnd(28), "real", r.status, String(r.text).slice(0, 60).replace(/\s+/g, " "), "| sim", s.status, String(s.text).slice(0, 50));
}
