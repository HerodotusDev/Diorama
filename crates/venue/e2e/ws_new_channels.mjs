// New WS channels: bbo / orderUpdates / userEvents on the sim, bbo + acks on real (subscriptions only).
import { wsConnect, SIM_WS, REAL_WS, sleep } from "./lib.mjs";
const zero = "0x0000000000000000000000000000000000000001";
const subs = [{ type: "bbo", coin: "BTC" }, { type: "orderUpdates", user: zero }, { type: "userEvents", user: zero }];
const [real, sim] = await Promise.all([wsConnect(REAL_WS), wsConnect(SIM_WS)]);
for (const s of subs) { real.sub(s); sim.sub(s); }
await sleep(4000);
real.close(); sim.close();
const brief = (frames) => frames.map((f) => JSON.stringify(f.msg).slice(0, 230));
console.log("REAL"); brief(real.frames).slice(0, 6).forEach((l) => console.log("  " + l));
console.log("SIM"); brief(sim.frames).slice(0, 6).forEach((l) => console.log("  " + l));
