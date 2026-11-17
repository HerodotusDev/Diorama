#!/usr/bin/env bash
# End-to-end proof that the venue sim behaves like real Hyperliquid, against a LOCAL sim whose upstream is real mainnet.
# Real Hyperliquid is only ever READ (info + ws subscriptions, paced); every order, transfer and signature goes to the
# local sim. Needs node 20+, `npm install` once in this directory, and a built sim binary:
#   cargo +1.99 build -p diorama-venue-sim --locked
#   bash run_sim.sh fresh          # then wait ~45 s for the first cold market data
#   bash run_all.sh                # stages 1-4 (node stageN.mjs); add `all` for the real-vs-sim sweeps too
# Stage 2: orders/positions on every dex (flags: --dex xyz,para --coins N|all --ws 0|1); stage 3: the transfer matrix;
# stage 4: liquidations + updateLeverage isolation; sweeps: probe_real_exchange.mjs (zero-signature /exchange probes vs
# real), probe_types.mjs (info types), ws_sweep.mjs and ws_new_channels.mjs (ws channels), latency.mjs (60 s frame cadence, DURATION=60).
set -e
cd "$(dirname "$0")"
[ -d node_modules ] || npm install --silent
for s in stage1 stage2 stage3 stage4; do echo "== $s"; node $s.mjs; done
if [ "$1" = "all" ]; then
  for s in probe_real_exchange probe_types ws_sweep ws_new_channels latency; do echo "== $s"; node $s.mjs; done
fi
