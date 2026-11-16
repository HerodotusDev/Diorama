//! The venue bridge: V's Core↔EVM boundary, mirrored.
//!
//! V is a fork of the real HyperEVM, so the *contracts* at the boundary are
//! real (CoreWriter `0x3333…`, the system addresses) but HyperCore is not
//! there to act on them. This bridge is the missing half:
//!
//! - **EVM→Core**: an ERC20 transfer to a token's system address credits the
//!   sender's Core spot balance (from the `Transfer` log — the docs' exact
//!   rule);
//! - **CoreWriter actions**: every call to `0x3333…3333` (top-level *and*
//!   contract-internal, via the callTracer — a contract's action is
//!   attributed to the contract) decodes to a typed action; the sim applies
//!   the ones it models (limit orders, cancels by oid and cloid, USD class
//!   transfer, spot send, sendAsset, add API wallet, approve builder fee, set
//!   abstraction), counts the rest;
//! - **Core→EVM**: a `sendAsset` naming a token's system address is completed
//!   the way the chain does it — a `transfer(recipient, amount)` call on the
//!   linked ERC20 **from** the system address (V runs auto-impersonate).
//!
//! The scan is anchored like the driver's: blocks at or below the fork pin
//! are the *source's* real blocks and are never scanned (their transfers are
//! not the devnet's); a re-pin re-anchors the cursor to the new pin.
pub(crate) mod corewriter;
pub(crate) mod metrics;
pub(crate) mod system;

use crate::error::{Error, Result};
use crate::feed::MarketFeed;
use crate::sim::fees::RowKind;
use crate::sim::order::CancelSpeed;
use crate::sim::transfer::{AssetSend, CoreToEvm, Wallet};
use crate::sim::{Applied, ClassTransfer, Direction};
use crate::sim::{Sim, TokenRef};
use crate::wire::UserAbstraction;
use alloy_consensus::Transaction as _;
use alloy_primitives::{Address, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_rpc_types_trace::geth::{CallFrame, GethTrace, TraceResult};
use corewriter::{CoreAction, CORE_WRITER};
use diorama_anvil::Rpc;
use metrics::BridgeMetrics;
use rust_decimal::Decimal;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use system::{TokenInfo, Transfer};

/// The bridge's declared deployment policy: the scan cadence and the devnet
/// gas stipend for the Core↔EVM system address.
#[derive(Debug, Clone, Copy)]
pub struct BridgePolicy {
    pub poll: Duration,
    pub system_gas_stipend: U256,
}

/// The two dex indices of a CoreWriter `sendAsset` (`u32::MAX` = the spot
/// wallet, otherwise the `perpDexs` position).
#[derive(Debug, Clone, Copy)]
struct DexRoute {
    source: u32,
    destination: u32,
}

impl DexRoute {
    fn new(source: u32, destination: u32) -> Self {
        Self {
            source,
            destination,
        }
    }

    fn spot_to_spot(self) -> bool {
        self.source == corewriter::SPOT_DEX && self.destination == corewriter::SPOT_DEX
    }
}

/// The bridge's own cursor and the persisted state it needs. The cursor lives
/// in the sim's state file (one state root per service).
pub struct Bridge {
    anvil_v: Rpc,
    sim: Arc<Sim>,
    feed: Arc<MarketFeed>,
    /// The scan cadence (the deployment's `bridge_poll_interval_ms`).
    poll: Duration,
    /// The devnet gas stipend for the Core↔EVM system address.
    system_gas_stipend: U256,
    /// The last head whose timestamp was observed (one header read per new
    /// block; the sim's served `time` follows the venue chain's clock).
    observed_head: std::sync::atomic::AtomicU64,
    /// The last scanned V block (0 = not anchored yet); persisted in the
    /// sim's state so a restart does not lose the boundary activity mined
    /// meanwhile.
    cursor: std::sync::atomic::AtomicU64,
    pub metrics: Arc<BridgeMetrics>,
}

impl Bridge {
    pub fn new(
        anvil_v: Rpc,
        sim: Arc<Sim>,
        feed: Arc<MarketFeed>,
        metrics: Arc<BridgeMetrics>,
        policy: BridgePolicy,
    ) -> Result<Self> {
        // A poisoned sim state must not read as "not anchored": that would
        // re-anchor at the head and skip the boundary activity mined while
        // the service was down.
        let cursor = sim.bridge_cursor()?;
        Ok(Self {
            anvil_v,
            sim,
            feed,
            poll: policy.poll,
            system_gas_stipend: policy.system_gas_stipend,
            observed_head: std::sync::atomic::AtomicU64::new(0),
            cursor: std::sync::atomic::AtomicU64::new(cursor),
            metrics,
        })
    }

    /// Run forever: one scan pass per configured interval.
    pub async fn run(self) {
        loop {
            if let Err(e) = self.pass().await {
                tracing::warn!(error = %e, "venue bridge pass failed; will retry");
            }
            tokio::time::sleep(self.poll).await;
        }
    }

    /// One pass: anchor (pin-aware), then scan every new block.
    pub async fn pass(&self) -> Result<()> {
        let head = self.anvil_v.block_number().await?;
        self.observe_head_time(head).await?;
        let pin = self
            .anvil_v
            .fork_config()
            .await?
            .map(|fork| fork.block)
            .unwrap_or(0);
        let mut from = self.cursor.load(Ordering::Relaxed);
        if from == 0 || from < pin || from > head {
            // First pass, a re-pin, or a chain reset under us: anchor at the
            // devnet's own range (never scan the source's blocks).
            let anchor = head.max(pin);
            from = anchor;
            self.cursor.store(anchor, Ordering::Relaxed);
            self.sim.set_bridge_cursor(anchor)?;
            tracing::info!(block = anchor, pin, head, "venue bridge anchored");
        }
        for n in (from + 1)..=head {
            self.scan_block(n).await?;
            self.cursor.store(n, Ordering::Relaxed);
            self.sim.set_bridge_cursor(n)?;
            // One atomic write per block: the cursor and the block's effects
            // land together, so a crash mid-block re-scans instead of
            // half-applying (a duplicate credit is a silent balance lie).
            self.sim.save_if_dirty()?;
        }
        self.metrics
            .set_lag_blocks(head.saturating_sub(self.cursor.load(Ordering::Relaxed)));
        Ok(())
    }

    /// Keep the sim's served `time` on the venue chain's clock: one header
    /// read per new head (their harness jumps every chain forward; a venue
    /// read must not look stale afterwards).
    async fn observe_head_time(&self, head: u64) -> Result<()> {
        use std::sync::atomic::Ordering;
        if self.observed_head.load(Ordering::Relaxed) == head {
            return Ok(());
        }
        let block = self
            .anvil_v
            .block_by_number(head, diorama_anvil::Txs::Hashes)
            .await?;
        self.sim
            .observe_chain_time(block.header.timestamp.saturating_mul(1_000));
        self.observed_head.store(head, Ordering::Relaxed);
        Ok(())
    }

    /// Decode one block's boundary activity and apply it.
    async fn scan_block(&self, n: u64) -> Result<()> {
        let block = self
            .anvil_v
            .block_by_number(n, diorama_anvil::Txs::Full)
            .await?;
        if block.transactions.is_empty() {
            return Ok(());
        }
        let Some(spot) = self.feed.spot_meta() else {
            tracing::warn!(
                block = n,
                "bridge has no spotMeta yet; the block is re-scanned"
            );
            return Err(Error::Warming);
        };
        let tokens = system::token_registry(&spot);

        // The docs' pre-existence rule (interaction timings): the snapshot is
        // taken *before* this block's credits, so an EVM→Core transfer that
        // initializes an account in the same block still cannot act.
        let pre_block = self.sim.account_addresses()?;

        // EVM→Core: the docs' intra-block step 3.
        self.apply_credits(n, &tokens).await?;

        // CoreWriter: top-level txs plus contract-internal calls. The Core
        // sorts a block's actions by category before applying them (docs:
        // non-order actions, then cancels, then actions that send Gtc or Ioc
        // orders; within a category, the proposed order) — the bridge
        // reproduces that order. The consensus batches inside a block are not
        // visible from the EVM side; one batch per block is the usual case.
        let mut calls = self.corewriter_calls(n, &block).await?;
        calls.sort_by_key(|(_, action)| action_rank(action));
        let (calls, refused) = callers_eligible(&pre_block, calls);
        if refused > 0 {
            self.metrics.inc_refusals_by(refused as u64);
            tracing::warn!(
                block = n,
                refused,
                "CoreWriter actions refused: the account must exist on HyperCore before the block (docs: interaction timings)"
            );
        }
        for (caller, action) in calls {
            self.apply_action(caller, action, &tokens).await?;
        }
        Ok(())
    }

    /// The block's EVM→Core side (the docs' intra-block step 3: "EVM -> Core
    /// transfers are processed" before the CoreWriter actions): HYPE's system
    /// contract (`Received`) and every token's system-address transfers,
    /// from the receipts' logs.
    async fn apply_credits(&self, n: u64, tokens: &[TokenInfo]) -> Result<()> {
        let receipts = self.anvil_v.receipts(n).await?;
        for receipt in &receipts {
            // The logs decoded once, with their emitters, so a helper-hop
            // credit can look back at the same transaction's funding leg.
            let logs: Vec<(Address, &alloy_primitives::LogData)> = receipt
                .logs()
                .iter()
                .map(|log| (log.address(), log.data()))
                .collect();
            for (index, log) in receipt.logs().iter().enumerate() {
                // HYPE's boundary is the system contract's `Received` event
                // (docs): a tx value sent to `0x2222…2` credits Core HYPE.
                if log.address() == system::HYPE_SYSTEM {
                    self.apply_hype_credit(log.data(), tokens)?;
                    continue;
                }
                let Some(transfer) = system::transfer_of(log.data()) else {
                    continue;
                };
                self.apply_credit(&transfer, log.address(), &logs[..index], tokens)
                    .await?;
            }
        }
        Ok(())
    }

    /// Every CoreWriter call in the block with its Core account (the calling
    /// address). The callTracer includes the top-level frames, so traces are
    /// the single source when available; a trace fault degrades to top-level
    /// txs only (counted — contract-internal calls are missed that block).
    async fn corewriter_calls(
        &self,
        n: u64,
        block: &alloy_rpc_types_eth::Block,
    ) -> Result<Vec<(Address, CoreAction)>> {
        match self.anvil_v.trace_block_calls(n).await {
            Ok(traces) => {
                let mut out = Vec::new();
                for trace in traces {
                    collect_internal(&trace, &mut out);
                }
                Ok(out)
            }
            Err(e) => {
                self.metrics.inc_trace_faults();
                tracing::warn!(block = n, error = %e, "callTracer unavailable; contract-internal CoreWriter calls are not seen this block");
                let mut out = Vec::new();
                for tx in block.transactions.txns() {
                    if tx.to() == Some(CORE_WRITER) {
                        out.push((tx.inner.signer(), corewriter::decode_calldata(tx.input())));
                    }
                }
                Ok(out)
            }
        }
    }

    // TRUST: value in = value recorded (token-admission-policy). The guards:
    // a Transfer log from the token's linked EVM contract credits its sender;
    // an entry point's own self-transfer is credited to the same-transaction
    // funding depositor; the Core amount is the log's exact value converted by
    // the pinned decimals.
    /// EVM→Core credit from one transfer log (the linked token's own transfer,
    /// or an entry-point self-transfer with a same-transaction funding leg).
    async fn apply_credit(
        &self,
        transfer: &Transfer,
        emitter: Address,
        prior: &[(Address, &alloy_primitives::LogData)],
        tokens: &[TokenInfo],
    ) -> Result<()> {
        // The linked token's own log is the ordinary deposit. When a bridge
        // helper moves the tokens on, the helper's self-transfer is resolved
        // back to the depositor that funded it earlier in the same
        // transaction. Both rules refuse a self-transfer that no funding leg
        // backs; the linked token's own self-transfer is never credited (the
        // measured 2026-10-31 USDC deposit credited the token contract
        // itself).
        let credit = system::credit_of(transfer, tokens, emitter)
            .or_else(|| system::hop_credit_of(transfer, prior, tokens, emitter));
        let Some(credit) = credit else {
            if transfer.from == emitter {
                tracing::warn!(
                    from = %transfer.from,
                    to = %transfer.to,
                    value = %transfer.value,
                    "bridge: EVM→Core transfer originates at its emitter, and no earlier same-transaction transfer into the emitter funds it — refusing to credit"
                );
            }
            return Ok(());
        };
        self.sim.credit_evm_deposit(
            credit.sender,
            TokenRef {
                index: credit.token.index,
                coin: &credit.token.name,
            },
            credit.amount,
            &self.feed,
        )?;
        self.metrics.inc_credits();
        tracing::info!(
            from = %credit.sender,
            token = %credit.token.name,
            amount = %credit.amount,
            "bridge: EVM→Core credit"
        );
        Ok(())
    }

    /// The EVM→Core HYPE credit (the system contract's `Received` event).
    /// The emitter check is the address itself: only the real system contract
    /// can emit a credit, never a look-alike.
    fn apply_hype_credit(
        &self,
        data: &alloy_primitives::LogData,
        tokens: &[TokenInfo],
    ) -> Result<()> {
        let Some(token) = tokens.iter().find(|t| t.name == "HYPE") else {
            return Ok(());
        };
        let Some(credit) = system::hype_received_of(data, token) else {
            return Ok(());
        };
        self.sim.credit_evm_deposit(
            credit.sender,
            TokenRef {
                index: token.index,
                coin: &token.name,
            },
            credit.amount,
            &self.feed,
        )?;
        self.metrics.inc_credits();
        tracing::info!(from = %credit.sender, amount = %credit.amount, "bridge: EVM→Core HYPE credit");
        Ok(())
    }

    /// Apply one decoded action to the sim.
    async fn apply_action(
        &self,
        caller: Address,
        action: CoreAction,
        tokens: &[TokenInfo],
    ) -> Result<()> {
        self.metrics.count_action(action.id());
        match action {
            CoreAction::UsdClassTransfer { ntl, direction } => {
                self.usd_class_transfer(caller, ntl, direction, tokens)?;
            }
            CoreAction::SpotSend {
                destination,
                token,
                wei,
            } => {
                if let Some(info) = self.token_info(tokens, token) {
                    self.spot_send((caller, destination), info, wei).await?;
                }
            }
            CoreAction::SendAsset {
                destination,
                source_dex,
                destination_dex,
                token,
                wei,
                ..
            } => {
                if let Some(info) = self.token_info(tokens, token) {
                    let route = DexRoute::new(source_dex, destination_dex);
                    self.send_asset((caller, destination), info, wei, route)
                        .await?;
                }
            }
            CoreAction::AddApiWallet { wallet, name } => {
                self.add_api_wallet(caller, wallet, &name)?;
            }
            CoreAction::SetAbstraction { user, abstraction } => {
                self.set_abstraction(user, abstraction);
            }
            CoreAction::ApproveBuilderFee {
                max_fee_rate,
                builder,
            } => self.approve_builder_fee(caller, builder, max_fee_rate)?,
            order @ CoreAction::LimitOrder { .. } => self.limit_order(caller, &order).await,
            action @ (CoreAction::CancelByOid { .. } | CoreAction::CancelByCloid { .. }) => {
                self.core_cancel(caller, &action);
            }
            CoreAction::Unmodeled { .. } => {}
            CoreAction::Malformed => self.metrics.inc_malformed(),
        }
        Ok(())
    }

    /// The CoreWriter cancels (ids 10/11): resolve the asset, then the sim's
    /// own cancel — the venue's refusal string rides the log.
    fn core_cancel(&self, caller: Address, action: &CoreAction) {
        let (coin, outcome) = match action {
            CoreAction::CancelByOid { asset, oid } => {
                let Some(coin) = self.coin_of(*asset) else {
                    self.metrics.inc_refusals();
                    tracing::warn!(account = %caller, asset, "bridge: cancel for an unknown asset refused");
                    return;
                };
                (
                    coin.clone(),
                    self.sim.cancel(caller, &coin, *oid, CancelSpeed::Normal),
                )
            }
            CoreAction::CancelByCloid { asset, cloid } => {
                let Some(coin) = self.coin_of(*asset) else {
                    self.metrics.inc_refusals();
                    tracing::warn!(account = %caller, asset, "bridge: cancelByCloid for an unknown asset refused");
                    return;
                };
                (
                    coin.clone(),
                    self.sim.cancel_by_cloid(
                        caller,
                        &coin,
                        alloy_primitives::B128::from(*cloid),
                        CancelSpeed::Normal,
                    ),
                )
            }
            _ => return,
        };
        match outcome {
            Ok(()) => {
                tracing::info!(account = %caller, coin = %coin, "bridge: CoreWriter cancel executed");
            }
            Err(e) => {
                self.metrics.inc_refusals();
                tracing::warn!(account = %caller, coin = %coin, error = %e, "bridge: CoreWriter cancel refused");
            }
        }
    }

    /// The asset index -> coin map the CoreWriter actions address (`None` =
    /// an index the live meta does not carry: a named refusal upstream).
    fn coin_of(&self, asset: u32) -> Option<String> {
        self.feed.coin_of_asset(asset)
    }

    /// The CoreWriter limit order (id 1): prices and sizes arrive as 10^8 ×
    /// the human value and the tif byte is the docs' own (1 `Alo`, 2 `Gtc`,
    /// 3 `Ioc`). The order fills against the real book through the same lane
    /// as `/exchange` and `/control/order` — one lane, three doors. An
    /// `Alo`/`Gtc` order rests in the sim's book; a refused fill (no depth,
    /// no margin, unknown asset) is counted, never retried.
    async fn limit_order(&self, caller: Address, action: &CoreAction) {
        let CoreAction::LimitOrder { asset, tif, .. } = action else {
            return;
        };
        let Some(tif) = crate::sim::order::Tif::from_corewriter(*tif) else {
            self.metrics.inc_malformed();
            tracing::warn!(account = %caller, tif, "bridge: CoreWriter order with an unknown tif byte");
            return;
        };
        let Some(order) = core_order(|a| self.feed.coin_of_asset(a), action, tif) else {
            self.metrics.inc_refusals();
            tracing::warn!(account = %caller, asset, "bridge: CoreWriter order for an unknown asset refused");
            return;
        };
        // An immediate fill walks the coin's book: a coin outside the
        // configured markets gets it on demand.
        let _ = self.feed.ensure_book(&order.coin).await;
        match self.sim.place(&self.feed, caller, &order) {
            Ok(placed) => {
                if let Some(fill) = &placed.fill {
                    tracing::info!(account = %caller, coin = %order.coin, sz = %fill.total_sz, avg_px = %fill.avg_px, "bridge: CoreWriter order filled");
                }
                if let Some(oid) = placed.resting {
                    tracing::info!(account = %caller, coin = %order.coin, oid, "bridge: CoreWriter order resting");
                }
            }
            Err(e) => {
                self.metrics.inc_refusals();
                tracing::warn!(account = %caller, coin = %order.coin, error = %e, "bridge: CoreWriter order refused");
            }
        }
    }

    /// `usdClassTransfer`: USDC between the spot and perp wallets.
    fn usd_class_transfer(
        &self,
        caller: Address,
        ntl: u64,
        direction: Direction,
        tokens: &[TokenInfo],
    ) -> Result<()> {
        let Some(usdc) = system::by_name(tokens, "USDC") else {
            return Err(Error::Warming);
        };
        // Action 7 carries the *perp class's* USD (1e6), not the spot token's
        // core wei: the margin agent's recall reads the perp class's
        // withdrawable precompile (1e6) and converts ×100 only for the
        // spot-class bridge leg (`HLConversions.perpToWei`). Decoding at the
        // spot scale read the recall 1/100 small — the measured
        // 0.15-of-15.0 leg that stranded the recovery sweep.
        let Some(amount) = usd_class_amount(ntl) else {
            self.metrics.inc_refusals();
            tracing::warn!(
                wei = ntl,
                "bridge: usdClassTransfer amount is not representable; refused"
            );
            return Ok(());
        };
        let applied = self.sim.usd_class_transfer(
            caller,
            &ClassTransfer {
                token: TokenRef {
                    index: usdc.index,
                    coin: &usdc.name,
                },
                amount,
                direction,
            },
            &self.feed,
        )?;
        match applied {
            Applied::Applied => {
                tracing::info!(account = %caller, amount = %amount, ?direction, "bridge: USD class transfer");
            }
            Applied::Refused => {
                self.metrics.inc_refusals();
                tracing::warn!(account = %caller, amount = %amount, ?direction, "bridge: USD class transfer refused (insufficient balance)");
            }
        }
        Ok(())
    }

    /// The registry entry for a wire token index; an unknown index is counted
    /// and refused (the bridge never guesses a token).
    fn token_info<'a>(&self, tokens: &'a [TokenInfo], token: u64) -> Option<&'a TokenInfo> {
        if let Some(info) = system::by_index(tokens, u32::try_from(token).ok()?) {
            return Some(info);
        }
        self.metrics.inc_refusals();
        None
    }

    /// `spotSend`: spot→spot inside Core, or Core→EVM when the destination is
    /// the token's system address (the real venue treats both spot sends and
    /// `sendAsset` to a system address as the bridge leg).
    async fn spot_send(
        &self,
        (caller, destination): (Address, Address),
        info: &TokenInfo,
        wei: u64,
    ) -> Result<()> {
        let Some(amount) = system::core_to_human(wei, info.wei_decimals) else {
            self.metrics.inc_refusals();
            tracing::warn!(wei, "bridge: spotSend amount is not representable; refused");
            return Ok(());
        };
        if is_system_destination(destination, info) {
            return self
                .core_to_evm((caller, destination), info, amount, RowKind::SpotTransfer)
                .await;
        }
        let applied = self.sim.spot_send(
            (caller, destination),
            TokenRef {
                index: info.index,
                coin: &info.name,
            },
            amount,
            (RowKind::SpotTransfer, &self.feed),
        )?;
        if matches!(applied, Applied::Refused) {
            self.metrics.inc_refusals();
            tracing::warn!(account = %caller, amount = %amount, "bridge: spot send refused");
        }
        Ok(())
    }

    /// `addApiWallet`: register the agent on the account.
    fn add_api_wallet(&self, caller: Address, wallet: Address, name: &str) -> Result<()> {
        self.sim.note_agent(caller, wallet, name)?;
        tracing::info!(account = %caller, wallet = %wallet, name, "bridge: API wallet registered");
        Ok(())
    }

    /// `approveBuilderFee` (action 12): the caller (the margin agent) approves
    /// `builder` up to `max_fee_rate` tenths of a basis point. The venue sets
    /// the maximum, so a later approval overwrites; orders carrying that
    /// builder on `/exchange` are then accepted up to it.
    fn approve_builder_fee(
        &self,
        caller: Address,
        builder: Address,
        max_fee_rate: u64,
    ) -> Result<()> {
        self.sim
            .approve_builder_fee(caller, builder, max_fee_rate)?;
        tracing::info!(account = %caller, builder = %builder, max_fee_rate, "bridge: builder fee approved");
        Ok(())
    }

    /// `setAbstraction`: 1 = disabled, 2 = unified account; any other code is
    /// counted and never applied. 3 (portfolio margin) reaches the sim, which
    /// refuses it by name — its cross-asset portfolio is the venue's own
    /// accounting — and the refusal is counted here, never a block failure.
    fn set_abstraction(&self, user: Address, abstraction: u8) {
        let mode = match abstraction {
            1 => UserAbstraction::Disabled,
            2 => UserAbstraction::UnifiedAccount,
            3 => UserAbstraction::PortfolioMargin,
            _ => {
                self.metrics.inc_refusals();
                return;
            }
        };
        match self.sim.set_mode(user, mode) {
            Ok(()) => tracing::info!(account = %user, ?mode, "bridge: abstraction set"),
            Err(e) => {
                self.metrics.inc_refusals();
                tracing::warn!(account = %user, error = %e, "bridge: abstraction refused");
            }
        }
    }

    /// `sendAsset`: spot→spot inside Core, Core→EVM when the destination is
    /// the token's system address (completed by a system-address transfer),
    /// and between the spot wallet and the perp dexes' clearinghouses (or two
    /// dexes) otherwise.
    #[allow(clippy::too_many_lines)] // one rendering/transition table: splitting it would hide the field-to-line mapping
    async fn send_asset(
        &self,
        (caller, destination): (Address, Address),
        info: &TokenInfo,
        wei: u64,
        route: DexRoute,
    ) -> Result<()> {
        let Some(amount) = system::core_to_human(wei, info.wei_decimals) else {
            self.metrics.inc_refusals();
            tracing::warn!(
                wei,
                "bridge: sendAsset amount is not representable; refused"
            );
            return Ok(());
        };
        if !route.spot_to_spot() {
            // A perp dex is an endpoint: the move is between ledgers (the
            // token-checked, margin-bounded `sendAsset` of the sim), never to
            // the EVM side.
            if is_system_destination(destination, info) {
                self.metrics.inc_refusals();
                tracing::warn!(account = %caller, "bridge: sendAsset to a system address from a perp dex refused (Core→EVM leaves the spot wallet only)");
                return Ok(());
            }
            return self.send_between_wallets((caller, destination), info, amount, route);
        }
        if is_system_destination(destination, info) {
            return self
                .core_to_evm((caller, destination), info, amount, RowKind::Send)
                .await;
        }
        let applied = self.sim.spot_send(
            (caller, destination),
            TokenRef {
                index: info.index,
                coin: &info.name,
            },
            amount,
            (RowKind::Send, &self.feed),
        )?;
        if matches!(applied, Applied::Refused) {
            self.metrics.inc_refusals();
            tracing::warn!(account = %caller, amount = %amount, "bridge: sendAsset refused");
        }
        Ok(())
    }

    /// Core→EVM: the sim debits the amount, the HyperEVM gas and any owed
    /// activation from the spot wallet (a refusal moves nothing), then the
    /// transfer completes on the EVM side.
    async fn core_to_evm(
        &self,
        (caller, destination): (Address, Address),
        info: &TokenInfo,
        amount: Decimal,
        kind: RowKind,
    ) -> Result<()> {
        let debit = CoreToEvm {
            from: caller,
            destination,
            token: TokenRef {
                index: info.index,
                coin: &info.name,
            },
            amount,
            kind,
        };
        if matches!(
            self.sim.debit_core_to_evm(&debit, &self.feed)?,
            Applied::Refused
        ) {
            self.metrics.inc_refusals();
            tracing::warn!(account = %caller, amount = %amount, "bridge: Core→EVM refused");
            return Ok(());
        }
        self.complete_core_to_evm(&caller, info, amount).await
    }

    /// A `sendAsset` with a perp dex at either end: the sim moves the token
    /// between the two wallets (refusals are counted and logged, never a block
    /// failure).
    fn send_between_wallets(
        &self,
        (caller, destination): (Address, Address),
        info: &TokenInfo,
        amount: Decimal,
        route: DexRoute,
    ) -> Result<()> {
        let (Some(source), Some(target)) = (
            self.wallet_of(route.source),
            self.wallet_of(route.destination),
        ) else {
            self.metrics.inc_refusals();
            tracing::warn!(
                account = %caller,
                source_dex = route.source,
                destination_dex = route.destination,
                "bridge: sendAsset names a dex index the registry does not list; refused"
            );
            return Ok(());
        };
        let send = AssetSend {
            from: caller,
            to: destination,
            token: TokenRef {
                index: info.index,
                coin: &info.name,
            },
            amount,
            source,
            destination: target,
            kind: RowKind::Send,
        };
        match self.sim.send_asset(&send, &self.feed) {
            Ok(()) => {
                tracing::info!(account = %caller, to = %destination, token = %info.name, amount = %amount, "bridge: sendAsset between wallets");
                Ok(())
            }
            Err(Error::BadRequest { reason }) => {
                self.metrics.inc_refusals();
                tracing::warn!(account = %caller, amount = %amount, %reason, "bridge: sendAsset between wallets refused");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// The wallet a CoreWriter dex index names: `u32::MAX` is the spot wallet,
    /// any other index the dex at that `perpDexs` position (`None` = unlisted).
    fn wallet_of(&self, dex: u32) -> Option<Wallet> {
        if dex == corewriter::SPOT_DEX {
            return Some(Wallet::Spot);
        }
        self.feed.dex_name_at(dex).map(Wallet::Perp)
    }

    /// The system address pays gas on V (the real chain's system txs do not):
    /// fund it once, at the deployment's declared stipend, so the pool's
    /// balance check passes, plus the `value` a native transfer moves. A
    /// balance-read fault propagates: it must not be read as "empty" and
    /// funded over a broken RPC.
    async fn fund_system_address(&self, from: Address, value: Option<U256>) -> Result<()> {
        let have = self.anvil_v.balance_at(from, 0).await?;
        let need = self
            .system_gas_stipend
            .saturating_add(value.unwrap_or(U256::ZERO));
        if (value.is_some() && have < need) || have.is_zero() {
            self.anvil_v.anvil_set_balance(from, need.max(have)).await?;
        }
        Ok(())
    }

    /// The completion tx: `transfer(caller, amount)` on the linked ERC20 from
    /// the system address (auto-impersonate), or for HYPE the native value
    /// from `0x2222…2`. It lands with the devnet's next mined block, like the
    /// real chain's system tx.
    async fn complete_core_to_evm(
        &self,
        recipient: &Address,
        info: &TokenInfo,
        amount: Decimal,
    ) -> Result<()> {
        let native = info.name == crate::sim::fees::GAS_TOKEN;
        // HYPE is the native gas token: its transfer carries a value, so the
        // registry's link is only needed for a token's ERC20.
        let evm = match info.evm {
            Some(evm) => evm,
            None if native => system::EvmToken {
                address: system::HYPE_SYSTEM,
                extra_decimals: 0,
            },
            None => return Err(system::no_evm_link(info)),
        };
        let evm_decimals = if native {
            system::HYPE_EVM_DECIMALS
        } else {
            info.wei_decimals as i32 + evm.extra_decimals
        };
        let Some(value) = system::human_to_evm(amount, evm_decimals) else {
            self.metrics.inc_refusals();
            tracing::warn!(amount = %amount, "bridge: Core→EVM amount is not representable in EVM wei; refused");
            return Ok(());
        };
        let from = if native {
            system::HYPE_SYSTEM
        } else {
            system::system_address(u64::from(info.index))
        };
        self.fund_system_address(from, native.then_some(value))
            .await?;
        let tx = if native {
            TransactionRequest::default()
                .from(from)
                .to(*recipient)
                .value(value)
        } else {
            TransactionRequest::default()
                .from(from)
                .to(evm.address)
                .input(system::transfer_calldata(*recipient, value).into())
        };
        let hash = self.anvil_v.send_transaction(tx).await?;
        self.metrics.inc_completions();
        tracing::info!(
            to = %recipient,
            token = %info.name,
            amount = %amount,
            tx = %hash,
            "bridge: Core→EVM completion submitted"
        );
        Ok(())
    }
}

/// Whether `destination` is `info`'s system address: the `0x20…` address
/// carrying the token index, or `0x2222…2` for HYPE.
fn is_system_destination(destination: Address, info: &TokenInfo) -> bool {
    destination == system::system_address(u64::from(info.index))
        || (info.name == crate::sim::fees::GAS_TOKEN && destination == system::HYPE_SYSTEM)
}

/// Action 7's `ntl` as the human decimal the sim keeps: the *perp class's*
/// USD (1e6) — `system::PERP_USD_DECIMALS`, not the spot token's core wei.
/// Pure, so the unit rule is testable without a chain.
fn usd_class_amount(ntl: u64) -> Option<Decimal> {
    system::core_to_human(ntl, system::PERP_USD_DECIMALS)
}

/// The Core's intra-block action order (docs: "Within a block, actions are
/// sorted within each consensus batch as 1. Actions that do not send GTC or
/// IOC orders to any book 2. Cancels 3. Actions that send at least one GTC or
/// IOC … Within each category, actions are sorted in the order they were
/// proposed"). An `Alo` (post-only, tif byte 1) cannot match, so it sorts
/// with the non-order actions; `Gtc`/`Ioc` (bytes 2/3) are the book-sending
/// category. `sort_by_key` is stable, so each category keeps the proposed
/// order.
/// The docs' pre-existence rule (interaction timings): "the account
/// performing the CoreWriter action must exist on HyperCore before the EVM
/// block is built. An EVM -> Core transfer to initialize the account in the
/// same block will still result in the CoreWriter action being rejected."
/// The bridge snapshots the accounts before the block's credits and filters
/// the calls through this — pure, so the rule is testable without a chain.
fn callers_eligible(
    pre_block: &std::collections::HashSet<Address>,
    calls: Vec<(Address, CoreAction)>,
) -> (Vec<(Address, CoreAction)>, usize) {
    let mut refused = 0;
    let eligible = calls
        .into_iter()
        .filter(|(caller, _)| {
            let known = pre_block.contains(caller);
            if !known {
                refused += 1;
            }
            known
        })
        .collect();
    (eligible, refused)
}

fn action_rank(action: &CoreAction) -> u8 {
    match action {
        CoreAction::CancelByOid { .. } | CoreAction::CancelByCloid { .. } => 1,
        CoreAction::LimitOrder { tif, .. } if *tif == 2 || *tif == 3 => 2,
        _ => 0,
    }
}

/// Collect CoreWriter calls from a trace tree (frame order = execution order).
fn collect_internal(trace: &TraceResult, out: &mut Vec<(Address, CoreAction)>) {
    if let Some(GethTrace::CallTracer(frame)) = trace.success() {
        walk_frame(frame, out);
    }
}

fn walk_frame(frame: &CallFrame, out: &mut Vec<(Address, CoreAction)>) {
    if frame.to == Some(CORE_WRITER) && !frame.input.is_empty() {
        out.push((frame.from, corewriter::decode_calldata(&frame.input)));
    }
    for child in &frame.calls {
        walk_frame(child, out);
    }
}

/// The CoreWriter order's units (docs, pinned): prices and sizes are 10^8 ×
/// the human value, and the asset id resolves through the feed's one
/// resolver, `coin_of_asset` (`None` = an id the live dexes do not carry, a
/// named refusal upstream). The tif comes in already mapped
/// (`Tif::from_corewriter`, one home).
fn core_order(
    coin_of_asset: impl Fn(u32) -> Option<String>,
    action: &CoreAction,
    tif: crate::sim::order::Tif,
) -> Option<crate::sim::order::Order> {
    let CoreAction::LimitOrder {
        asset,
        side,
        limit_px,
        sz,
        reduce_only,
        cloid,
        ..
    } = action
    else {
        return None;
    };
    let coin = coin_of_asset(*asset)?;
    let scale = Decimal::from(100_000_000u64);
    Some(crate::sim::order::Order {
        coin,
        is_buy: side.is_buy(),
        size: Decimal::from(*sz) / scale,
        limit_px: Some(Decimal::from(*limit_px) / scale),
        tif,
        reduce_only: *reduce_only,
        // The CoreWriter payload carries the cloid as a uint128 (docs,
        // action 1); zero means "none" (the wire's absent value).
        cloid: (*cloid != 0).then(|| alloy_primitives::B128::from(*cloid)),
        // The CoreWriter has no trigger variant: limit orders only.
        trigger: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usd_transfer_input(ntl: u64) -> Vec<u8> {
        let mut input = vec![0x01, 0x00, 0x00, 0x07];
        input.extend_from_slice(&U256::from(ntl).to_be_bytes::<32>());
        input.extend_from_slice(&U256::from(1u64).to_be_bytes::<32>());
        input
    }

    /// The CoreWriter order's units are the docs' own: 10^8 × human for both
    /// price and size, resolved against the live meta.
    #[test]
    fn corewriter_orders_scale_by_1e8() {
        let mut meta: crate::wire::Meta = serde_json::from_str(r#"{"universe": []}"#).unwrap();
        meta.universe.push(crate::wire::AssetInfo {
            name: "ETH".into(),
            sz_decimals: 4,
            max_leverage: 20,
            ..Default::default()
        });
        let action = CoreAction::LimitOrder {
            asset: 0,
            side: crate::sim::order::Side::Buy,
            limit_px: 10_150_000_000,
            sz: 250_000_000,
            reduce_only: false,
            tif: 3,
            cloid: 0,
        };
        let registry = crate::feed::dex::DexRegistry::of_default(meta);
        let resolve = |asset| registry.coin_of_asset(asset);
        let order =
            core_order(resolve, &action, crate::sim::order::Tif::Ioc).expect("a modeled order");
        assert_eq!(order.coin, "ETH");
        assert!(order.is_buy);
        assert_eq!(order.size, "2.5".parse().unwrap());
        assert_eq!(order.limit_px, Some("101.5".parse().unwrap()));
        assert_eq!(order.tif, crate::sim::order::Tif::Ioc);
        // An index the meta does not carry is a refusal, not a guess.
        let oob = CoreAction::LimitOrder {
            asset: 9,
            side: crate::sim::order::Side::Buy,
            limit_px: 1,
            sz: 1,
            reduce_only: false,
            tif: 3,
            cloid: 0,
        };
        assert!(core_order(resolve, &oob, crate::sim::order::Tif::Ioc).is_none());
        assert!(core_order(|_| None, &action, crate::sim::order::Tif::Ioc).is_none());
    }

    /// Action 7's unit rule: `ntl` is the *perp class's* USD (1e6), not the
    /// spot token's core wei (8). The margin agent's recall sends the clamped
    /// perp-class figure (the `0x0803` withdrawable precompile's own unit) and
    /// converts ×100 only for the spot-class bridge leg — so a 15.0 USDC
    /// recall arrives as `ntl = 15_000_000` and must apply as 15.0. The old
    /// spot-wei reading produced the measured 0.15-of-15.0 leg (R5-02) that
    /// stranded the recovery sweep.
    #[test]
    fn usd_class_amount_is_perp_class_usd_not_spot_wei() {
        assert_eq!(usd_class_amount(15_000_000), Some("15.0".parse().unwrap()));
        assert_eq!(
            usd_class_amount(7_950_068),
            Some("7.950068".parse().unwrap())
        );
        // The defect's reading, pinned as the contrast: the same raw at the
        // spot token's 8-dec scale is 1/100 of the request.
        assert_eq!(
            system::core_to_human(15_000_000, 8),
            Some("0.15".parse().unwrap())
        );
        // Sub-wei dust still converts (the venue floors nothing here — the
        // amount is the caller's exact perp-class figure).
        assert_eq!(usd_class_amount(1), Some("0.000001".parse().unwrap()));
    }

    /// The pre-existence rule is pure: a caller outside the pre-block set is
    /// refused (and counted); a known caller passes.
    #[test]
    fn corewriter_actions_require_a_pre_existing_account() {
        use std::collections::HashSet;
        let known = Address::repeat_byte(0x0a);
        let fresh = Address::repeat_byte(0x0b);
        let pre: HashSet<Address> = [known].into_iter().collect();
        let (eligible, refused) = callers_eligible(
            &pre,
            vec![
                (known, CoreAction::Malformed),
                (fresh, CoreAction::Malformed),
            ],
        );
        assert_eq!(refused, 1, "the same-block initialization still refuses");
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].0, known);
    }

    #[test]
    fn internal_frames_are_walked_in_order() {
        let outer = CallFrame {
            from: Address::repeat_byte(0x01),
            to: Some(Address::repeat_byte(0x02)),
            input: Default::default(),
            calls: vec![
                CallFrame {
                    from: Address::repeat_byte(0x02),
                    to: Some(CORE_WRITER),
                    input: usd_transfer_input(1).into(),
                    calls: vec![],
                    ..Default::default()
                },
                CallFrame {
                    from: Address::repeat_byte(0x02),
                    to: Some(CORE_WRITER),
                    input: usd_transfer_input(2).into(),
                    calls: vec![],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut out = Vec::new();
        walk_frame(&outer, &mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(
            out[0].0,
            Address::repeat_byte(0x02),
            "attributed to the caller"
        );
        assert_eq!(out[0].1.id(), 7);
        assert_eq!(out[1].1.id(), 7);
    }
    /// The Core's intra-block action order (docs): non-order actions, then
    /// cancels, then Gtc/Ioc orders — a stable sort, so each category keeps
    /// the proposed order. An Alo is post-only and sorts with the non-orders.
    #[test]
    fn intra_block_actions_sort_by_category() {
        let cancel = |oid: u64| CoreAction::CancelByOid { asset: 0, oid };
        let order = |tif: u8, cloid: u128| CoreAction::LimitOrder {
            asset: 0,
            side: crate::sim::order::Side::Buy,
            limit_px: 100,
            sz: 1,
            reduce_only: false,
            tif,
            cloid,
        };
        let mut calls = [
            order(2, 1),
            cancel(1),
            CoreAction::UsdClassTransfer {
                ntl: 1,
                direction: Direction::ToPerp,
            },
            order(1, 2),
            order(3, 3),
            cancel(2),
        ];
        calls.sort_by_key(action_rank);
        assert!(
            matches!(calls[0], CoreAction::UsdClassTransfer { .. }),
            "a non-order action leads"
        );
        assert!(
            matches!(calls[1], CoreAction::LimitOrder { tif: 1, .. }),
            "the Alo is post-only: it sorts with the non-orders"
        );
        assert!(matches!(calls[2], CoreAction::CancelByOid { oid: 1, .. }));
        assert!(
            matches!(calls[3], CoreAction::CancelByOid { oid: 2, .. }),
            "cancels keep the proposed order"
        );
        assert!(matches!(calls[4], CoreAction::LimitOrder { tif: 2, .. }));
        assert!(
            matches!(calls[5], CoreAction::LimitOrder { tif: 3, .. }),
            "the book-sending orders close the block, in the proposed order"
        );
    }

    /// A margin agent's `approveBuilderFee` reaches the sim: the CoreWriter
    /// call decodes (selector wrapper, action 12) and applies to the *caller*,
    /// is counted under its id, and a later approval for the same builder
    /// overwrites.
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one call-to-read script: splitting it would hide the path from calldata to the served answer
    async fn a_decoded_approve_builder_fee_is_recorded_on_the_caller() {
        let sim = Arc::new(Sim::memory());
        let bridge = Bridge::new(
            Rpc::new("http://127.0.0.1:1", Duration::from_secs(1)).unwrap(),
            sim.clone(),
            Arc::new(MarketFeed::memory(Vec::new())),
            Arc::new(BridgeMetrics::default()),
            BridgePolicy {
                poll: Duration::from_secs(1),
                system_gas_stipend: U256::ZERO,
            },
        )
        .unwrap();
        let agent = Address::repeat_byte(0x0a);
        let builder = Address::repeat_byte(0x0b);
        let call = |rate: u64| {
            // `CoreWriterLib.approveBuilderFee`: version 1, action 12, then
            // `abi.encode(uint64 maxFeeRate, address builder)`.
            let mut stream = vec![0x01, 0x00, 0x00, 0x0c];
            stream.extend_from_slice(&U256::from(rate).to_be_bytes::<32>());
            stream
                .extend_from_slice(&U256::from_be_bytes(builder.into_word().0).to_be_bytes::<32>());
            use alloy_sol_types::SolCall as _;
            let mut input = corewriter::sendRawActionCall::SELECTOR.to_vec();
            input.extend_from_slice(&U256::from(32u64).to_be_bytes::<32>());
            input.extend_from_slice(&U256::from(stream.len() as u64).to_be_bytes::<32>());
            input.extend_from_slice(&stream);
            input.resize(input.len() + 28, 0);
            corewriter::decode_calldata(&input)
        };

        // The served read: `/info` `maxBuilderFee`, a bare JSON integer.
        let state = Arc::new(
            crate::api::ApiState::new(
                Arc::new(MarketFeed::memory(Vec::new())),
                sim.clone(),
                crate::feed::rest::RestClient::new(&["http://127.0.0.1:1".to_string()]).unwrap(),
                None,
                None,
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .unwrap(),
        );
        let served = |user: Address| {
            let state = state.clone();
            async move {
                let body =
                    format!(r#"{{"type":"maxBuilderFee","user":"{user}","builder":"{builder}"}}"#);
                let response = crate::api::info::info(
                    axum::extract::State(state),
                    axum::body::Bytes::from(body),
                )
                .await
                .unwrap();
                let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()
            }
        };

        assert_eq!(sim.max_builder_fee(agent, builder).unwrap(), None);
        bridge.apply_action(agent, call(1_000), &[]).await.unwrap();
        assert_eq!(sim.max_builder_fee(agent, builder).unwrap(), Some(1_000));
        assert_eq!(served(agent).await, serde_json::json!(1_000));
        bridge.apply_action(agent, call(25), &[]).await.unwrap();
        assert_eq!(
            sim.max_builder_fee(agent, builder).unwrap(),
            Some(25),
            "the venue sets the maximum: the later approval overwrites"
        );
        assert_eq!(served(agent).await, serde_json::json!(25));
        assert!(bridge
            .metrics
            .render()
            .contains("venue_bridge_actions_total{id=\"12\"} 2"));
        // Another account's approval is its own.
        assert_eq!(
            sim.max_builder_fee(Address::repeat_byte(0x0c), builder)
                .unwrap(),
            None
        );
    }

    /// A CoreWriter `sendAsset` with a perp dex at an end moves the token
    /// between the caller's wallets (dex index = `perpDexs` position, spot =
    /// `u32::MAX`), token-checked and margin-bounded; a USDH send into the
    /// default dex and an unlisted dex index are refused and counted.
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one request/response script: splitting it would hide the order of the transfers
    async fn a_corewriter_send_asset_moves_collateral_between_dexes() {
        let sim = Arc::new(Sim::memory());
        let feed = Arc::new(MarketFeed::memory(Vec::new()));
        let now = crate::now_ms();
        feed.put_meta(crate::sim::meta_with("ETH", 10), now);
        feed.put_dex_meta("xyz", crate::sim::meta_of(&[("xyz:TSLA", 10)]), now);
        let mut flx = crate::sim::meta_of(&[("flx:TSLA", 10)]);
        flx.collateral_token = 360;
        feed.put_dex_meta("flx", flx, now);
        let token = |name: &str, index: u32| crate::wire::SpotToken {
            name: name.into(),
            sz_decimals: 8,
            wei_decimals: 8,
            index,
            is_canonical: true,
            evm_contract: None,
            token_id: None,
            full_name: None,
            deployer_trading_fee_share: Decimal::ZERO,
        };
        let spot = crate::wire::SpotMeta {
            tokens: vec![token("USDC", 0), token("USDH", 360)],
            universe: vec![],
        };
        feed.put_spot_meta(spot.clone());
        let tokens = system::token_registry(&spot);
        let bridge = Bridge::new(
            Rpc::new("http://127.0.0.1:1", Duration::from_secs(1)).unwrap(),
            sim.clone(),
            feed.clone(),
            Arc::new(BridgeMetrics::default()),
            BridgePolicy {
                poll: Duration::from_secs(1),
                system_gas_stipend: U256::ZERO,
            },
        )
        .unwrap();
        let who = Address::repeat_byte(0x0a);
        let usdc = TokenRef {
            index: 0,
            coin: "USDC",
        };
        sim.credit_spot(who, usdc, "100".parse().unwrap()).unwrap();
        sim.credit_spot(
            who,
            TokenRef {
                index: 360,
                coin: "USDH",
            },
            "50".parse().unwrap(),
        )
        .unwrap();
        let send = |source: u32, destination: u32, token: u64, human: u64| CoreAction::SendAsset {
            destination: who,
            sub_account: Address::ZERO,
            source_dex: source,
            destination_dex: destination,
            token,
            wei: human * 100_000_000,
        };
        let balance = |dex: &str| sim.account(who).unwrap().unwrap().perp_balance(dex);
        let spot_of = |token: u32| sim.account(who).unwrap().unwrap().spot[&token].total;
        let dec = |t: &str| t.parse::<Decimal>().unwrap();

        // Spot to the xyz dex (position 1), then xyz to the default dex (0).
        bridge
            .apply_action(who, send(corewriter::SPOT_DEX, 1, 0, 40), &tokens)
            .await
            .unwrap();
        assert_eq!((balance("xyz"), spot_of(0)), (dec("40"), dec("60")));
        bridge
            .apply_action(who, send(1, 0, 0, 10), &tokens)
            .await
            .unwrap();
        assert_eq!((balance("xyz"), balance("")), (dec("30"), dec("10")));
        // Back to spot.
        bridge
            .apply_action(who, send(0, corewriter::SPOT_DEX, 0, 10), &tokens)
            .await
            .unwrap();
        assert_eq!((balance(""), spot_of(0)), (Decimal::ZERO, dec("70")));

        // USDH goes to the flx dex (position 2) and nowhere else; refusals
        // are counted and move nothing.
        let refusals = || bridge.metrics.render();
        bridge
            .apply_action(who, send(corewriter::SPOT_DEX, 2, 360, 20), &tokens)
            .await
            .unwrap();
        assert_eq!((balance("flx"), spot_of(360)), (dec("20"), dec("30")));
        bridge
            .apply_action(who, send(corewriter::SPOT_DEX, 0, 360, 5), &tokens)
            .await
            .unwrap();
        bridge
            .apply_action(who, send(corewriter::SPOT_DEX, 9, 0, 5), &tokens)
            .await
            .unwrap();
        assert_eq!(
            (balance(""), spot_of(360), spot_of(0)),
            (Decimal::ZERO, dec("30"), dec("70"))
        );
        assert!(
            refusals().contains("venue_bridge_refusals_total 2"),
            "{}",
            refusals()
        );
    }

    /// A bridge over an in-memory sim and a feed whose spot registry lists
    /// USDC, HYPE (no EVM link, so no completion can run) and a HYPE/USDC
    /// pair at 5; the RPC points nowhere (these tests stop before the chain).
    fn fee_bridge() -> (Bridge, Arc<Sim>, Vec<TokenInfo>) {
        let sim = Arc::new(Sim::memory());
        let feed = Arc::new(MarketFeed::memory(Vec::new()));
        let token = |name: &str, index: u32| crate::wire::SpotToken {
            name: name.into(),
            sz_decimals: 8,
            wei_decimals: 8,
            index,
            is_canonical: true,
            evm_contract: None,
            token_id: None,
            full_name: None,
            deployer_trading_fee_share: Decimal::ZERO,
        };
        let spot = crate::wire::SpotMeta {
            tokens: vec![token("USDC", 0), token("HYPE", 150)],
            universe: vec![crate::wire::SpotPair {
                tokens: [150, 0],
                name: "@107".into(),
                index: 107,
                is_canonical: true,
            }],
        };
        feed.put_spot_meta(spot.clone());
        feed.put_mids(
            std::collections::BTreeMap::from([("@107".to_string(), "5".parse().unwrap())]),
            crate::now_ms(),
        );
        let tokens = system::token_registry(&spot);
        let bridge = Bridge::new(
            Rpc::new("http://127.0.0.1:1", Duration::from_secs(1)).unwrap(),
            sim.clone(),
            feed,
            Arc::new(BridgeMetrics::default()),
            BridgePolicy {
                poll: Duration::from_secs(1),
                system_gas_stipend: U256::ZERO,
            },
        )
        .unwrap();
        (bridge, sim, tokens)
    }

    fn usdc_of(sim: &Sim, who: Address) -> Decimal {
        sim.account(who)
            .unwrap()
            .and_then(|a| a.spot.get(&0).map(|e| e.total))
            .unwrap_or_default()
    }

    /// CoreWriter's `spotSend` and `sendAsset` to an account that does not
    /// exist pay the 1 USDC activation fee like the HTTP transfers do: the
    /// recipient gets the full amount, the second send is free, and a send the
    /// balance cannot cover with the fee is refused (counted) and moves nothing.
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one request script: splitting it would hide the order of the sends
    async fn corewriter_sends_to_a_new_account_pay_the_activation_fee_once() {
        let (bridge, sim, tokens) = fee_bridge();
        let me = Address::repeat_byte(0x0a);
        let new = Address::repeat_byte(0x0b);
        sim.credit_spot(
            me,
            TokenRef {
                index: 0,
                coin: "USDC",
            },
            "10".parse().unwrap(),
        )
        .unwrap();
        let wei = |human: u64| human * 100_000_000;
        let spot_send = |wei| CoreAction::SpotSend {
            destination: new,
            token: 0,
            wei,
        };
        // 10 to a new account needs 11: refused.
        bridge
            .apply_action(me, spot_send(wei(10)), &tokens)
            .await
            .unwrap();
        assert_eq!(
            (usdc_of(&sim, me), usdc_of(&sim, new)),
            ("10".parse().unwrap(), Decimal::ZERO)
        );
        assert!(bridge
            .metrics
            .render()
            .contains("venue_bridge_refusals_total 1"));
        // 4 goes with its fee; the next 4 is free.
        bridge
            .apply_action(me, spot_send(wei(4)), &tokens)
            .await
            .unwrap();
        assert_eq!(
            (usdc_of(&sim, me), usdc_of(&sim, new)),
            ("5".parse().unwrap(), "4".parse().unwrap())
        );
        let send_asset = CoreAction::SendAsset {
            destination: new,
            sub_account: Address::ZERO,
            source_dex: corewriter::SPOT_DEX,
            destination_dex: corewriter::SPOT_DEX,
            token: 0,
            wei: wei(4),
        };
        bridge.apply_action(me, send_asset, &tokens).await.unwrap();
        assert_eq!(
            (usdc_of(&sim, me), usdc_of(&sim, new)),
            ("1".parse().unwrap(), "8".parse().unwrap())
        );
    }

    /// A `spotSend` and a `sendAsset` to a system address are the Core to EVM
    /// leg, never a credit to the system address as if it were an account: HYPE's
    /// `0x2222...` included. A balance that cannot pay the amount and the gas
    /// is refused and nothing moves.
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one request script: splitting it would hide the order of the sends
    async fn sends_to_a_system_address_take_the_core_to_evm_path() {
        let (bridge, sim, tokens) = fee_bridge();
        let me = Address::repeat_byte(0x0a);
        sim.credit_spot(
            me,
            TokenRef {
                index: 0,
                coin: "USDC",
            },
            "10".parse().unwrap(),
        )
        .unwrap();
        sim.credit_spot(
            me,
            TokenRef {
                index: 150,
                coin: "HYPE",
            },
            "1".parse().unwrap(),
        )
        .unwrap();
        let wei = |human: u64| human * 100_000_000;
        // The whole 10 USDC leaves: the gas is paid in HYPE, so the debit
        // lands; HYPE has no EVM link in this registry, so the completion
        // cannot run and the action faults after the debit.
        let to_usdc = CoreAction::SpotSend {
            destination: system::system_address(0),
            token: 0,
            wei: wei(10),
        };
        assert!(bridge.apply_action(me, to_usdc, &tokens).await.is_err());
        assert_eq!(usdc_of(&sim, me), Decimal::ZERO);
        assert!(
            sim.account(system::system_address(0)).unwrap().is_none(),
            "the system address is not credited as an account"
        );
        // HYPE to `0x2222...`: the same path, and no gas fee for HYPE itself.
        let to_hype = CoreAction::SendAsset {
            destination: system::HYPE_SYSTEM,
            sub_account: Address::ZERO,
            source_dex: corewriter::SPOT_DEX,
            destination_dex: corewriter::SPOT_DEX,
            token: 150,
            wei: 50_000_000,
        };
        assert!(bridge.apply_action(me, to_hype, &tokens).await.is_err());
        assert!(sim.account(system::HYPE_SYSTEM).unwrap().is_none());
        // More than the balance: refused (counted), nothing moves.
        let too_much = CoreAction::SpotSend {
            destination: system::system_address(0),
            token: 0,
            wei: wei(5),
        };
        bridge.apply_action(me, too_much, &tokens).await.unwrap();
        assert!(bridge
            .metrics
            .render()
            .contains("venue_bridge_refusals_total 1"));
    }

    /// The system-address test names the token's own address and HYPE's.
    #[test]
    fn the_system_destination_is_the_tokens_own_address() {
        let (_, _, tokens) = fee_bridge();
        let usdc = system::by_name(&tokens, "USDC").unwrap();
        let hype = system::by_name(&tokens, "HYPE").unwrap();
        assert!(is_system_destination(system::system_address(0), usdc));
        assert!(!is_system_destination(system::system_address(150), usdc));
        assert!(is_system_destination(system::HYPE_SYSTEM, hype));
        assert!(is_system_destination(system::system_address(150), hype));
        assert!(!is_system_destination(system::HYPE_SYSTEM, usdc));
    }
}
