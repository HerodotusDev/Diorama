//! The follow step: the venue's counterpart of the mainnet applier.
//!
//! One bounded window per pass: compose the source's window BAL (the spine's
//! generic composer — the HL public API has no native BAL, so it synthesizes
//! one from the envelopes and the declared scope), COW it against the
//! devnet's own writes, land the rest on V, and record the live parity of
//! what was applied. The gap guard *bounds one window's width* — a gap past
//! the policy composes the oldest `limit` blocks and continues next pass, so
//! the follow converges on the live source instead of latching paused
//! (measured: the old latching pause froze V's chain-side state ~9.9k blocks
//! behind while the public HyperEVM moved ~60 blocks/min — only a manual
//! re-pin recovered it; that is not live following). The continuity check
//! names a source reorg.
//!
//! The parts mirror the applier's `apply/`: this module is the step and its
//! guards, [`parity`] is the measurement (the applier's drift-lens
//! counterpart), [`audit`] is the uncovered-log trail (venue-only: reth's
//! native BALs carry every write).
pub(crate) mod audit;
pub(crate) mod parity;

use crate::ctx::Ctx;
use crate::error::Result;
use alloy_rpc_types_eth::Block;
use diorama_devnet::engine::Engine as _;
use diorama_devnet::facts::Fact;
use diorama_devnet::metrics::Registry as _;
use std::time::Instant;

/// One follow window's outcome (what the fact and the gauges report) — the
/// venue's counterpart of the applier's `ApplyOutcome`, returned by [`step`]
/// so a caller that reports per-window lines (the selftest) reads the same
/// numbers the fact carries. The field names are the applier's fact
/// vocabulary (`total`/`applied`/`filtered`), so the two mirrors' facts and
/// logs read the same way.
pub(crate) struct FollowOutcome {
    /// The compose rung's name (`plan.kind.as_str()`: `poll` for the public
    /// API's touches; `bal`/`trace` if a future source serves them).
    pub(crate) source: &'static str,
    /// The composed change count, before COW (the fact's `writes`).
    pub(crate) writes: usize,
    pub(crate) applied: usize,
    pub(crate) filtered: usize,
    pub(crate) skipped_shadowed: usize,
    pub(crate) sampled: usize,
    pub(crate) sampled_ok: usize,
    pub(crate) duration_ms: u64,
}

/// The catch-up state's drift line: the source head ran past the follow
/// cursor by more than one window's policy width, so the follow composes
/// bounded windows until it is current. The log's schema is per producer
/// (`kind` names the anomaly, like the applier's `source` names its diff
/// rung).
#[derive(serde::Serialize)]
struct GapCatchupEntry {
    chain: &'static str,
    kind: &'static str,
    followed: u64,
    source_head: u64,
    window_to: u64,
    gap: u64,
    /// `null` when the policy is unbounded; a zero would read as "no gap
    /// allowed", which is a different statement.
    limit: Option<u64>,
}

/// A source continuity mismatch's drift line: the block after the last
/// composed one carried a different parent hash (a reorg under the follow).
#[derive(serde::Serialize)]
struct SourceReorgEntry {
    chain: &'static str,
    kind: &'static str,
    block: u64,
    expected: String,
    found: String,
}

/// A source that does not have the next block yet is behind, not broken: the
/// public HyperEVM RPC answers `-32001 block not found` (and the typed
/// `BlockMissing` for a null result) while its head has already moved. The
/// follow loop treats that as "wait", never as a pass failure.
fn source_block_unavailable(error: &diorama_anvil::Error) -> bool {
    error.is_block_unavailable()
}

/// Whether a follow pass failed only because the source has not served the
/// next block yet. The loop treats this as a wait (DEBUG), not a pass failure
/// (WARN); the source head/followed gauges still expose the lag.
pub(crate) fn is_source_lag(error: &crate::Error) -> bool {
    match error {
        crate::Error::Rpc(error) => source_block_unavailable(error),
        crate::Error::Devnet(diorama_devnet::Error::Rpc(error)) => source_block_unavailable(error),
        _ => false,
    }
}

/// The next window block, or the fact that the source has not served it yet.
enum NextWindowBlock {
    Ready(Box<Option<Block>>),
    SourceBehind,
}

/// Read the continuity block only when the source has a stored hash to check
/// against; a lagging source's missing block is a wait, never a pass failure.
async fn next_window_block(ctx: &Ctx, followed: u64) -> Result<NextWindowBlock> {
    let Some(_) = ctx.plane.state.source_last_hash()? else {
        return Ok(NextWindowBlock::Ready(Box::new(None)));
    };
    match ctx
        .source_rpc
        .block_by_number(followed + 1, diorama_anvil::Txs::Hashes)
        .await
    {
        Ok(block) => Ok(NextWindowBlock::Ready(Box::new(Some(block)))),
        Err(error) if source_block_unavailable(&error) => {
            tracing::debug!(
                error = %error,
                follow = followed,
                "source block not available yet; waiting for the source head"
            );
            Ok(NextWindowBlock::SourceBehind)
        }
        Err(error) => Err(error.into()),
    }
}

/// Follow the source: compose the window since the last composed source
/// block, land the changed keys on V (COW: devnet-owned keys are skipped),
/// and record the live parity of the applied keys. One window per pass; the
/// gap guard pauses a window that is too wide (a re-pin decision, never a
/// silent partial poll).
///
/// The window's writes are the *source's* state, not the devnet's: they are
/// not journaled (a rewind restores devnet writes, never source history), and
/// a key the devnet owns is skipped until it is handed back.
pub(crate) async fn step(ctx: &Ctx) -> Result<Option<FollowOutcome>> {
    let Some(followed) = ctx.plane.state.source_last_block()? else {
        // Not anchored yet (reconcile anchors at the pin); nothing to do.
        return Ok(None);
    };
    // The opening reads in one body: the head, and — when a stored hash
    // makes the continuity check meaningful — the first window block
    // (`followed + 1`). One body, not two: the follow's binding constraint
    // is the public endpoint's request budget.
    // Read the head first: a source that has not reached `followed + 1` is
    // simply behind, not a fault. Asking for the next block first (the old
    // `head_and_block` shape) made a lagging source return "block not
    // available" and the follow loop logged a WARN every pass.
    let source_head = ctx.source_rpc.block_number().await?;
    ctx.venue_metrics
        .set(&ctx.venue_metrics.source_head, source_head);
    ctx.venue_metrics
        .set(&ctx.venue_metrics.source_followed, followed);
    if source_head <= followed {
        // The symmetric steady line: the mainnet driver logs the same
        // message per pass at its follow head.
        tracing::debug!(
            source_head,
            follow = followed,
            "chain is at the follow head"
        );
        return Ok(None);
    }
    // The continuity check needs the first window block; read it only when
    // the source has one to serve.
    let next_block = match next_window_block(ctx, followed).await? {
        NextWindowBlock::Ready(block) => *block,
        NextWindowBlock::SourceBehind => return Ok(None),
    };
    // One bounded window per pass: the policy's limit caps the window's
    // *width*, never the following itself — a gap past it composes the
    // oldest chunk and continues next pass (the chunked catch-up). Sequential
    // chunks land the same final state a single wide window would, each
    // bounded and logged.
    let limit = ctx.mirror.policies().gap;
    let to = catchup_window(followed, source_head, limit);
    note_gap(ctx, followed, source_head, to, limit)?;

    let started = Instant::now();
    continuity_check(ctx, followed, next_block.as_ref())?;
    let plan = plan_window(ctx, followed, to).await?;
    pre_land_scan(ctx).await?;
    let applied = apply_window(ctx, &plan).await?;
    let (sampled, sampled_ok) = parity::parity_step(ctx, &plan.bal).await?;
    let end = advance_source_cursor(ctx, to).await?;
    mine_window_block(ctx, end).await?;
    let stats = outcome(&plan, applied, (sampled, sampled_ok), started);
    report_applied(ctx, followed, to, source_head, &stats);
    Ok(Some(stats))
}

/// The pass's reported numbers (the fact and the per-window log read the
/// same shape).
fn outcome(
    plan: &diorama_devnet::bal::WindowPlan,
    applied: usize,
    sampled: (usize, usize),
    started: Instant,
) -> FollowOutcome {
    FollowOutcome {
        source: plan.kind.as_str(),
        writes: plan.total,
        applied,
        filtered: plan.writes.len(),
        skipped_shadowed: plan.filter.skipped_shadowed,
        sampled: sampled.0,
        sampled_ok: sampled.1,
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

/// The compose's landing guard: scan V's chain up to its *current* head, so
/// the filter sees the marks of blocks mined while the window composed. The
/// compose's target reads race the producers (measured: a value transfer to
/// a watched account, mined between the pass's scan and the landing, was
/// clobbered by the window's absolute write before the next pass could mark
/// it). Cheap when nothing moved (one head read, no traces); a fault fails
/// the window — re-scanning is cheap and the marks are idempotent, while
/// landing past an unscanned block is the exact COW failure the registry
/// exists to prevent.
async fn pre_land_scan(ctx: &Ctx) -> Result<()> {
    let head = ctx.anvil_v.block_number().await?;
    let first = ctx
        .plane
        .state
        .cursor()?
        .as_ref()
        .map(|c| c.block)
        .unwrap_or(head);
    let walk = diorama_devnet::scan::scan_to(
        &ctx.plane,
        &ctx.anvil_v,
        diorama_devnet::pin::resolved_pin(&ctx.pin).unwrap_or(0),
        head,
        first,
    )
    .await?;
    if walk.walked > 0 {
        ctx.record_scan(&walk);
        diorama_devnet::scan::publish(&ctx.plane, ctx.mirror.label(), &walk);
        ctx.plane.save_shadow()?;
    }
    Ok(())
}

/// The gap guard's journal and log: a bounded window means the source ran
/// ahead (the chunked catch-up); the first live window after it says so.
fn note_gap(ctx: &Ctx, followed: u64, source_head: u64, to: u64, limit: Option<u64>) -> Result<()> {
    if to < source_head {
        if ctx.plane.gap_pause_started() {
            ctx.venue_metrics
                .inc(&ctx.venue_metrics.follow_gap_catchups_total);
            ctx.drift.append(&GapCatchupEntry {
                chain: ctx.mirror.label(),
                kind: "gap_catchup",
                followed,
                source_head,
                window_to: to,
                gap: source_head - followed,
                limit,
            })?;
            tracing::warn!(
                followed,
                source_head,
                to,
                gap = source_head - followed,
                "follow catching up: the source ran ahead; composing bounded windows"
            );
        }
    } else if ctx.plane.gap_pause_cleared() {
        tracing::info!(
            followed,
            source_head,
            "follow caught up; the source is live"
        );
    }
    Ok(())
}

/// One place for the window's gauges, fact, and log.
fn report_applied(ctx: &Ctx, from: u64, to: u64, source_head: u64, window: &FollowOutcome) {
    ctx.venue_metrics
        .inc(&ctx.venue_metrics.follow_windows_total);
    ctx.venue_metrics.add(
        &ctx.venue_metrics.follow_applied_total,
        window.applied as u64,
    );
    ctx.venue_metrics.add(
        &ctx.venue_metrics.follow_sampled_total,
        window.sampled as u64,
    );
    ctx.venue_metrics.add(
        &ctx.venue_metrics.follow_sampled_ok_total,
        window.sampled_ok as u64,
    );
    ctx.venue_metrics.add(
        &ctx.venue_metrics.follow_skipped_shadowed_total,
        window.skipped_shadowed as u64,
    );
    ctx.plane.facts.publish(Fact::BlockApplied {
        chain: ctx.mirror.label(),
        block: to,
        source: window.source,
        writes: window.writes,
        filtered: window.filtered,
        skipped_shadowed: window.skipped_shadowed,
        sampled: window.sampled,
        sampled_ok: window.sampled_ok,
        ms: window.duration_ms,
    });
    tracing::info!(
        from,
        to,
        // The compose rung, named as the mainnet's per-block line names its
        // diff rung (`source = facts.source`): the two lines are mirrors.
        source = window.source,
        // The watched-account denominator, so a quiet window's `writes = 0`
        // reads as "N accounts probed, nothing changed" — never as silence.
        watched = ctx.mirror.scope().watch().accounts.len(),
        source_head,
        lag = source_head.saturating_sub(to),
        writes = window.writes,
        applied = window.applied,
        skipped_shadowed = window.skipped_shadowed,
        sampled_ok = window.sampled_ok,
        sampled = window.sampled,
        ms = window.duration_ms,
        "source window composed onto V"
    );
}

/// The catch-up window's end: one bounded window per pass (the policy's
/// limit caps the *width*, never the following itself). `None` = unbounded.
fn catchup_window(followed: u64, source_head: u64, limit: Option<u64>) -> u64 {
    match limit {
        Some(limit) => source_head.min(followed.saturating_add(limit)),
        None => source_head,
    }
}

/// Continuity: the block after the last composed one must carry the recorded
/// hash as its parent. A mismatch is a source reorg — the window composes from
/// latest-vs-latest reads, so the applied keys are canonical, but keys only
/// the orphaned blocks touched may be stale until a later window touches them:
/// count it, say so, and let the operator re-pin (HL finality is BFT; a
/// visible alarm is the honest guard).
fn continuity_check(ctx: &Ctx, followed: u64, next: Option<&Block>) -> Result<()> {
    let Some(last_hash) = ctx.plane.state.source_last_hash()? else {
        return Ok(());
    };
    let Some(next) = next else {
        return Ok(());
    };
    if next.header.parent_hash != last_hash {
        ctx.venue_metrics.inc(&ctx.venue_metrics.source_reorg_total);
        ctx.drift.append(&SourceReorgEntry {
            chain: ctx.mirror.label(),
            kind: "source_reorg",
            block: followed + 1,
            expected: last_hash.to_string(),
            found: next.header.parent_hash.to_string(),
        })?;
        tracing::error!(
            block = followed + 1,
            expected = %last_hash,
            found = %next.header.parent_hash,
            "source continuity mismatch (reorg): the follow window recomposed from latest"
        );
    }
    Ok(())
}

/// Compose the window and land its writes on V under the COW filter, then
/// record the live parity of what was applied. The composer is the spine's
/// generic one: the venue's source has no native BAL, so it synthesizes.
/// The window's first phase, the applier's `plan_and_check_base` slot: the
/// shared pipeline half (compose → COW → flatten) for the whole window, its
/// gauges recorded and the uncovered log audited.
async fn plan_window(ctx: &Ctx, from: u64, to: u64) -> Result<diorama_devnet::bal::WindowPlan> {
    let plan = diorama_devnet::bal::Composer {
        source: ctx.mirror.follow_source().as_ref(),
        target: ctx.target.as_ref(),
        scope: ctx.mirror.scope(),
    }
    .plan(
        &ctx.plane.shadow,
        diorama_devnet::bal::Window::new(from, to),
    )
    .await?;
    record_window_metrics(&ctx.venue_metrics, &plan.compose);
    audit::record(ctx, &plan.compose.uncovered)?;
    Ok(plan)
}

/// The window's second phase, the applier's `write_and_mine` slot: land the
/// planned writes on V (chunked), counted.
async fn apply_window(ctx: &Ctx, plan: &diorama_devnet::bal::WindowPlan) -> Result<usize> {
    if plan.writes.is_empty() {
        return Ok(0);
    }
    Ok(
        diorama_anvil::apply_writes(&ctx.anvil_v, &plan.writes, ctx.engine.loops.chunk_size)
            .await?,
    )
}

/// The poll's gauges: what the window's composition touched, planned and
/// changed (the uncovered-log trail is [`audit`]'s).
fn record_window_metrics(
    metrics: &crate::metrics::VenueMetrics,
    stats: &diorama_devnet::bal::ComposeStats,
) {
    metrics.set(&metrics.poll_touched, stats.touched as u64);
    metrics.set(&metrics.poll_planned_keys, stats.planned as u64);
    metrics.set(&metrics.poll_changed_keys, stats.changed as u64);
}

/// Advance the source cursor to `source_head` (recording the source's own
/// block hash — the continuity check's anchor) and persist the state. The
/// name says which cursor: the devnet cursor is the spine's `advance_cursor`.
/// Returns the fetched block's timestamp and base fee — the window end's
/// source clock and gas price, which the block-production rule stamps onto
/// V's next block.
async fn advance_source_cursor(ctx: &Ctx, source_head: u64) -> Result<WindowEnd> {
    let head_block = ctx
        .source_rpc
        .block_by_number(source_head, diorama_anvil::Txs::Hashes)
        .await?;
    ctx.plane
        .state
        .set_source_last(source_head, head_block.header.hash)?;
    ctx.plane.save_state()?;
    Ok(WindowEnd {
        timestamp: head_block.header.timestamp,
        base_fee: head_block.header.base_fee_per_gas,
    })
}

/// The source block a window ends at: what V's next block takes from it.
#[derive(Clone, Copy)]
struct WindowEnd {
    timestamp: u64,
    base_fee: Option<u64>,
}

/// Mine one empty block stamped with the window's source-end timestamp: the
/// mirror's idle-liveness rule. Without it V's head (and its clock) advance
/// only when a devnet tx lands, so an idle deployment's freshness consumers
/// — the indexer's head-age verdict, and every on-chain staleness check that
/// reads `block.timestamp` (the RedStone feeds' own) — degrade against a
/// frozen clock (measured live: 2.7 days of degraded health after the last
/// E2E tx). The timestamp is the SOURCE's own clock, clamped monotonic to
/// the fork's last block: a source timestamp behind the fork's clock (a
/// deliberate `evm_increaseTime`, or a pin-era offset) must never produce a
/// non-monotonic block — anvil would refuse the set and mine with its own
/// default instead (the alignment rule, `align_context`'s clamp). Best
/// effort: a failed mine is a warn, never a window failure — the compose
/// already landed, and the next window mines the next block. The block also
/// carries the source's base fee: an idle fork's own fee decays to a few wei
/// (measured 7 wei on staging while HyperEVM's is ~0.1 gwei), and every gas
/// estimate a client makes from V's head (the frontend's Core to EVM bridge
/// reserve) would then undershoot what HyperCore charges.
async fn mine_window_block(ctx: &Ctx, end: WindowEnd) -> Result<()> {
    if !ctx.engine.loops.mine_empty_blocks {
        return Ok(());
    }
    let head = ctx.anvil_v.block_number().await?;
    let last_ts = ctx
        .anvil_v
        .block_by_number(head, diorama_anvil::Txs::Hashes)
        .await?
        .header
        .timestamp;
    let ts = next_block_timestamp(end.timestamp, last_ts);
    if let Err(e) = ctx.anvil_v.anvil_set_next_block_timestamp(ts).await {
        tracing::warn!(error = %e, ts, "window block timestamp set failed; mining with anvil's own clock");
    }
    if let Some(base_fee) = end.base_fee {
        if let Err(e) = ctx.anvil_v.anvil_set_next_block_base_fee(base_fee).await {
            tracing::warn!(error = %e, base_fee, "window block base fee set failed; mining with anvil's own fee");
        }
    }
    if let Err(e) = ctx.anvil_v.anvil_mine(1).await {
        tracing::warn!(error = %e, "window block mine failed; the next window retries");
    }
    Ok(())
}

/// The mined block's timestamp: the source's clock, never behind the fork's
/// own. Pure so the monotonicity rule is pinned by test.
fn next_block_timestamp(source_ts: u64, last_ts: u64) -> u64 {
    source_ts.max(last_ts.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::{catchup_window, next_block_timestamp};

    /// The catch-up window: one bounded window per pass — the policy's limit
    /// caps the width, never the following itself (the latching pause it
    /// replaced froze V's chain-side state for hours; measured live).
    #[test]
    fn the_catchup_window_bounds_one_pass() {
        assert_eq!(catchup_window(0, 100, Some(32)), 32);
        assert_eq!(catchup_window(0, 32, Some(32)), 32);
        assert_eq!(catchup_window(0, 31, Some(32)), 31);
        assert_eq!(catchup_window(100, 132, Some(32)), 132);
        assert_eq!(catchup_window(100, 10_000, Some(300)), 400);
        assert_eq!(catchup_window(100, 10_000, None), 10_000);
    }

    /// The window block's timestamp tracks the source's clock, but never
    /// behind the fork's own: monotonicity beats alignment (anvil refuses a
    /// backwards set and mines with its own default instead, measured).
    #[test]
    fn the_window_block_timestamp_is_the_source_clock_clamped_monotonic() {
        // The steady line: the source is ahead, the fork follows it.
        assert_eq!(next_block_timestamp(2_000, 1_000), 2_000);
        // A pin-era offset (the fork's clock ran ahead of the source's):
        // monotonicity wins, the offset decays one block at a time.
        assert_eq!(next_block_timestamp(2_000, 3_600), 3_601);
        // Exactly equal: still +1, never a duplicate second.
        assert_eq!(next_block_timestamp(2_000, 2_000), 2_001);
        // A source timestamp at u64::MAX cannot overflow the clamp.
        assert_eq!(next_block_timestamp(u64::MAX, u64::MAX), u64::MAX);
    }
}
