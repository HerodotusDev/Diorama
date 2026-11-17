#!/usr/bin/env bash
# Start a LOCAL venue sim whose upstream is real mainnet (see instance.toml.tmpl).
#   run_sim.sh [fresh]      fresh wipes the persisted sim state first
# SIM_BIN overrides the binary (default: target/debug/diorama-venue-sim from `cargo +1.99 build -p diorama-venue-sim --locked`).
set -e
cd "$(dirname "$0")"
ROOT="$(pwd)"
BIN="${SIM_BIN:-$ROOT/../../../target/debug/diorama-venue-sim}"
STATE="$ROOT/.state"
if [ "$1" = "fresh" ]; then rm -rf "$STATE"; fi
mkdir -p "$STATE/state" "$STATE/engstate"
sed "s#@STATE@#$STATE#g" instance.toml.tmpl > "$STATE/instance.toml"
nohup "$BIN" run --config "$STATE/instance.toml" > "$STATE/sim.log" 2>&1 &
echo "sim pid $!"
for i in $(seq 1 90); do
  code=$(curl -s -m2 -o /dev/null -w '%{http_code}' http://127.0.0.1:8090/readyz || true)
  if [ "$code" = "200" ]; then echo "ready after ${i}s"; exit 0; fi
  sleep 1
done
echo "not ready"; exit 1
