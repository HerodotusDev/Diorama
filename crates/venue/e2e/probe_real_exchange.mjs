// Re-run the zero-signature probes (nothing validly signed) against REAL and the sim, compare bodies/status.
import fs from "node:fs";
import { Report } from "./lib.mjs";
const R = new Report("probe_real_exchange");
const fx = JSON.parse(fs.readFileSync(process.env.PROBES || "../src/api/fixtures/real_exchange.json", "utf8"));
const pick = new Set((process.env.ONLY || "order_bogus_asset,cancel,updateLeverage,usdClassTransfer,sendAsset,unknown_action,missing_signature,non_json,noop,spotSend,userSetAbstraction").split(","));
const post = async (base, body) => { const r = await fetch(base + "/exchange", { method: "POST", headers: { "content-type": "application/json" }, body }); return { status: r.status, text: await r.text() }; };
for (const p of fx.probes) {
  if (!pick.has(p.name)) continue;
  const body = p.request.replace(/"nonce":\d+/g, `"nonce":${Date.now()}`);
  const real = await post("https://api.hyperliquid.xyz", body);
  await new Promise((r) => setTimeout(r, 400));
  const sim = await post("http://127.0.0.1:8090", body);
  const same = real.status === sim.status && real.text === sim.text;
  R.ok(same, `${p.name}`, { real, sim });
  console.log(p.name.padEnd(22), same ? "same" : "DIFF", real.status, real.text.slice(0, 80));
}
process.exit(R.summary() ? 0 : 1);
