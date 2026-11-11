# Diorama staging deployment

How `develop` reaches the `daloc-diorama-stg` namespace on the home k3s
cluster. Staging is the ONLY wired environment: `main` is deliberately not
connected (no prod path exists in this repo).

## Trigger and shape

`.github/workflows/build-and-deploy.yaml` runs on `push` to `develop` and on
`workflow_dispatch` (guarded to `develop`). Two sequential jobs:

1. **apply-anvil** — `kubectl apply -k k8s/stg-anvil` through the
   `HerodotusDev/cicd` `cluster-access` action (WireGuard tunnel +
   deployer kubeconfig, org secrets). This layer owns everything the cicd
   pipeline does not: the namespace, the merged `diorama-config` wiring
   ConfigMap, the three instance ConfigMaps (generated from the committed
   `config/instances/diorama-staging-*`), all PVCs, and the two fork anvils.
2. **call-deploy** — the reusable `HerodotusDev/cicd/build-and-deploy@main`
   workflow: builds the four binaries as images
   (`ghcr.io/herodotusdev/<app>-stg:<version>`, `--build-arg
   APP_NAME=app-<app>`, `docker/Dockerfile.diorama`), pushes them, and
   applies `k8s/stg/<app>-deployment.yaml` into `daloc-diorama-stg` with the
   `<IMAGE>` placeholder substituted.

The jobs are strictly ordered (`needs:`) because the per-repo WireGuard peer
serves one tunnel at a time; the tunnel is a job-local background process, so
it never overlaps.

## Versions and tags

The image tag comes from `workspace.package.version` in `Cargo.toml`
(`k8s/cicd-inputs.yaml`: `version_file`/`version_key`). Bump it for each
deploy-worthy change set; the pipeline creates the matching git tag (plain
`0.2.0` — distinct from the legacy `v0.1.0`). The etcd env lookup
(`/daloc/stg/<app>/.env`) warn-skips: Diorama keeps no config in etcd.

## Epoch safety (why fork_pin)

Both staging anvils pin an explicit `fork_pin` in their committed
specialization (mainnet: `26377000`, venue: `50830000`):

- anvil-t: a restart with the same pin is a **restore** — the engine
  re-forks at the pin and rebuilds from the shadow registry. A re-resolved
  fresh pin would be a new epoch and CLEAR the shadow registry (the
  2026-11-02 incident: every chain-1 project address came back codeless).
- anvil-v: same rule; a moved base cleared 1,974 slots / 109 accounts on
  2026-11-01. The `--state` dump (`--state-interval 300`) additionally
  preserves the chain across a graceful rollout.

`kubectl apply` only replaces a pod when its spec changes, so a routine
push re-applies the anvil layer as a no-op. The instance ConfigMaps are
generated with `disableNameSuffixHash` (the engine manifests reference them
by plain name), so editing an instance file updates the ConfigMap but does
NOT restart anything — restart manually after a config change:

```
kubectl -n daloc-diorama-stg rollout restart deploy/anvil-t deploy/anvil-v
```

Both restarts preserve the epoch (same pin).

## First-boot prerequisites (infra, one-off)

The pipeline applies no secrets. `daloc-diorama-stg` needs:

- `ghcr-pull` — image pull secret for `ghcr.io` (copy from `daloc-prod`).
- `alchemy` — key `ALCHEMY_KEY` (anvil-t fork source + the mainnet engine's
  `RETH_URL`).
- `cctp-relay-key` — key `relay-key`, the bridge's funded relay wallet
  (create with `make cctp-secrets CCTP_RELAY_KEY=0x…` targeted at the
  staging namespace).

## Name map vs the manual per-namespace stack

Staging hosts both mirrors in one namespace, so previously
namespace-scoped names are disambiguated:

| manual (per-namespace)            | staging                              |
| --------------------------------- | ------------------------------------ |
| `engine` (deploy/service)         | `diorama-mainnet-engine` / `diorama-venue-engine` |
| `engine-state` PVC                | `mainnet-engine-state` / `venue-engine-state` |
| `diorama-config` (2 ConfigMaps)   | one merged `diorama-config`          |
| cross-namespace FQDN in `T_RPC_URL` | `http://anvil-t:8545` (same ns)    |

The harness `-nodeport` twins are not carried over: staging has no
loopback-forward harness, and the twins were ClusterIP no-ops anyway. Use
`kubectl port-forward` against the staging Services if a host consumer is
needed.
