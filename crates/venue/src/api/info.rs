//! The client-facing info API (8090): one `POST /info` endpoint, dispatched by
//! the request's `type`.
//!
//! COW rule: a **touched** account is answered from the sim; an **untouched**
//! one is passed through to the real venue (which serves its own empty shapes
//! for addresses it has never seen — the consumer's "unconfigured accounts → empty
//! shape, 200 OK" contract holds either way). Market data is answered from
//! the composed feed, so a crashed mark is the same number the sim priced
//! with.
use super::ApiState;
use crate::error::{Error, Result};
use crate::feed::dex::{dex_of_coin, ALL_DEXS};
use crate::feed::MarketFeed;
use crate::sim::{account, Sim};
use crate::wire::{
    ActiveAssetData, ClearinghouseState, ExchangeStatus, InfoRequest, Leverage, LeverageType,
    MetaAndAssetCtxs, UserAbstraction,
};
use alloy_primitives::Address;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use rust_decimal::Decimal;
use std::sync::Arc;

/// The readiness gate's freshness window (the plan's ≤6 s client gate, with
/// margin for a poll interval).
/// The wire's order-type names (the venue's own vocabulary): one spelling.
const LIMIT: &str = "Limit";
const MARKET: &str = "Market";
const TRIGGER: &str = "Trigger";

/// The wire's order-type name: a trigger rests as `Trigger`.
fn order_type(r: &crate::sim::order::RestingOrder) -> &'static str {
    if r.trigger.is_some() {
        TRIGGER
    } else {
        LIMIT
    }
}

pub async fn healthz(State(state): State<Arc<ApiState>>) -> Response {
    if let Err(refusal) = super::sim_gate(&state) {
        return refusal.into_response();
    }
    (StatusCode::OK, "ok").into_response()
}

/// `GET /health`: the inter-service health contract on the info port.
///
/// The shared `the HTTP client::health_verdict` reads a JSON `status`; the
/// text liveness body (`/healthz`) is for humans and orchestrators, while a
/// the consumer's service probing this port needs the same shape every other service
/// serves. Both answer the sim-poisoned gate identically.
pub async fn health(State(state): State<Arc<ApiState>>) -> Response {
    if let Err(refusal) = super::sim_gate(&state) {
        return refusal.into_response();
    }
    (StatusCode::OK, Json(serde_json::json!({"status":"ok"}))).into_response()
}

pub(crate) async fn readyz(State(state): State<Arc<ApiState>>) -> Response {
    if let Err(refusal) = super::sim_gate(&state) {
        return refusal.into_response();
    }
    if state.ready(crate::feed::MarketFeed::MAX_STALE_MS) {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "market plane warming up").into_response()
    }
}

/// `POST /info`: parse the typed request, dispatch, or pass through.
#[allow(clippy::too_many_lines)] // one dispatch over every info type
pub async fn info(State(state): State<Arc<ApiState>>, body: Bytes) -> Result<Response> {
    // A body the venue cannot deserialize is the caller's fault, answered the
    // way the real API answers it: HTTP 422 with a plain-text reason (measured
    // live). The verifier once measured a 500 here, which reads as "the sim
    // is broken".
    let Ok(req) = serde_json::from_slice::<InfoRequest>(&body) else {
        return Ok(deserialize_failure());
    };
    match req {
        // A type the sim does not serve is a type the real API does not know:
        // the same 422 (never a 200 with a bare string, which a typed client
        // would parse as data).
        InfoRequest::Unrouted => Ok(deserialize_failure()),
        // ---- account-scoped: the sim's, or the venue's -------------------
        InfoRequest::ClearinghouseState { user, dex } => {
            clearinghouse(&state, body, user, dex).await
        }
        InfoRequest::SpotClearinghouseState { user } => spot_state(&state, body, user).await,
        InfoRequest::UserAbstraction { user } => abstraction(&state, body, user).await,
        InfoRequest::OpenOrders { user, dex } => open_orders(&state, body, user, dex).await,
        InfoRequest::FrontendOpenOrders { user, dex } => {
            frontend_open_orders(&state, body, user, dex).await
        }
        InfoRequest::OrderStatus { user, oid } => order_status(&state, body, user, oid).await,
        InfoRequest::PerpsAtOpenInterestCap { dex } => open_interest_caps(&dex, &state),
        InfoRequest::ActiveAssetData { user, coin } => active_asset(&state, body, user, coin).await,
        // ---- market-scoped: the feed's -----------------------------------
        InfoRequest::Meta { dex } => meta(&dex, &state),
        InfoRequest::MetaAndAssetCtxs { dex } => meta_and_ctxs(&dex, &state),
        InfoRequest::PerpDexs => perp_dexs(&state),
        InfoRequest::L2Book {
            coin,
            n_sig_figs,
            mantissa,
        } => l2_book(&coin, n_sig_figs, mantissa, &state).await,
        InfoRequest::MarginTable { id } => margin_table(id, &state),
        InfoRequest::ExchangeStatus => Ok(exchange_status()),
        InfoRequest::SpotMeta => {
            Ok(Json(state.feed.spot_meta().ok_or(Error::Warming)?).into_response())
        }
        InfoRequest::AllMids { dex } => Ok(all_mids(&dex, &state)),
        InfoRequest::AllPerpMetas => all_perp_metas(&state),
        // Market data the sim holds no copy of: the same answer for every
        // consumer, from the venue the marks come from. The annotations and
        // categories describe the builder dexes' coins; the venue's
        // are lists (never a bare string).
        InfoRequest::CandleSnapshot { .. }
        | InfoRequest::PerpConciseAnnotations
        | InfoRequest::PerpCategories
        | InfoRequest::SpotMetaAndAssetCtxs
        | InfoRequest::FundingHistory { .. }
        | InfoRequest::PredictedFundings
        | InfoRequest::PerpDexLimits { .. }
        | InfoRequest::PerpDexStatus { .. }
        | InfoRequest::MaxMarketOrderNtls
        | InfoRequest::VaultSummaries
        | InfoRequest::TokenDetails { .. }
        | InfoRequest::PerpDeployAuctionStatus
        | InfoRequest::SpotPairDeployAuctionStatus
        | InfoRequest::SpotDeployState { .. }
        | InfoRequest::OutcomeMeta
        | InfoRequest::AllBorrowLendReserveStates
        | InfoRequest::BorrowLendReserveState { .. }
        | InfoRequest::LeadingVaults { .. }
        | InfoRequest::VaultDetails { .. } => pass_through(&state, &body).await,
        InfoRequest::UserFills { user, .. } => user_fills(&state, body, user).await,
        InfoRequest::UserFunding {
            user,
            start_time,
            end_time,
        } => user_funding(&state, body, user, start_time, end_time).await,
        InfoRequest::MaxBuilderFee { user, builder } => max_fee(&state, body, user, builder).await,
        InfoRequest::PreTransferCheck { user, .. } => pre_transfer_check(&state, body, user).await,
    }
}

/// The real API's answer for a `dex` it does not know, and for `ALL_DEXS` on
/// the types that do not take it: HTTP 500 with the body `null` (measured on
/// `meta`, `metaAndAssetCtxs`, `clearinghouseState`, `openOrders`,
/// `frontendOpenOrders`, `perpsAtOpenInterestCap`).
fn unknown_dex() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(header::CONTENT_TYPE, diorama_anvil::JSON_CONTENT_TYPE)],
        "null",
    )
        .into_response()
}

/// A plain 200 `null` (the real `allMids` answer for an unknown dex).
fn null_ok() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, diorama_anvil::JSON_CONTENT_TYPE)],
        "null",
    )
        .into_response()
}

/// The dexes a request's `dex` selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DexScope {
    /// One dex (`""` = the default dex).
    One(String),
    /// `ALL_DEXS`.
    All,
}

impl DexScope {
    /// Whether a coin belongs to the selected dexes.
    pub(crate) fn covers(&self, coin: &str) -> bool {
        match self {
            DexScope::One(dex) => dex_of_coin(coin) == dex,
            DexScope::All => true,
        }
    }
}

/// Resolve a `dex` argument: `None` is the default dex, `ALL_DEXS` selects
/// every dex only where `allow_all` (the types that take it), a listed name
/// selects itself, and anything else is unknown (`None`).
pub(crate) fn dex_scope(feed: &MarketFeed, dex: &str, allow_all: bool) -> Option<DexScope> {
    if dex == ALL_DEXS {
        return allow_all.then_some(DexScope::All);
    }
    (dex.is_empty() || feed.has_dex(dex)).then(|| DexScope::One(dex.to_string()))
}

/// `maxBuilderFee`: the maximum fee (tenths of a basis point) a simulated
/// account approved for `builder` through CoreWriter `approveBuilderFee`, as a
/// bare JSON integer (0 when never approved, as the venue answers); the venue's
/// verbatim for an untouched account.
async fn max_fee(
    state: &ApiState,
    body: Bytes,
    user: Address,
    builder: Address,
) -> Result<Response> {
    simulated(state, &body, user, |account| {
        let max = account
            .builder_approvals
            .get(&builder)
            .copied()
            .unwrap_or(0);
        Ok(Json(max).into_response())
    })
    .await
}

/// The most `userFills` serves (the venue's own page: the newest 2000).
const USER_FILLS_PAGE: usize = 2_000;

/// `userFills`: a simulated account's fills, newest first (the venue's
/// order), or the venue's verbatim for an untouched one.
async fn user_fills(state: &ApiState, body: Bytes, user: Address) -> Result<Response> {
    match state.sim.user_fills(user) {
        Some(mut fills) => {
            fills.reverse();
            fills.truncate(USER_FILLS_PAGE);
            Ok(Json(fills).into_response())
        }
        None => pass_through(state, &body).await,
    }
}

/// `userFunding`: a simulated account's funding payments from `startTime`
/// (to `endTime` when given), oldest first, each wrapped as the venue's ledger
/// update; the venue's verbatim for an untouched account.
async fn user_funding(
    state: &ApiState,
    body: Bytes,
    user: Address,
    start_time: u64,
    end_time: Option<u64>,
) -> Result<Response> {
    let Some(fundings) = state.sim.user_fundings(user, start_time) else {
        return pass_through(state, &body).await;
    };
    let rows: Vec<serde_json::Value> = fundings
        .into_iter()
        .filter(|f| end_time.is_none_or(|end| f.time <= end))
        .map(|f| {
            serde_json::json!({
                "time": f.time,
                "hash": ZERO_HASH,
                "delta": {
                    "type": "funding",
                    "coin": f.coin,
                    "usdc": crate::wire::decimal_nano::text(&f.usdc),
                    "szi": crate::wire::decimal_nano::text(&f.szi),
                    "fundingRate": crate::wire::decimal_nano::text(&f.funding_rate),
                    "nSamples": f.n_samples,
                },
            })
        })
        .collect();
    Ok(Json(rows).into_response())
}

/// The hash the venue stamps on funding ledger rows (they have no transaction).
const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

/// The real API's answer to a request it cannot deserialize (an unknown
/// `type`, a missing field): HTTP 422, plain text.
fn deserialize_failure() -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        "Failed to deserialize the JSON body into the target type",
    )
        .into_response()
}

/// `preTransferCheck`: the venue's answer for a simulated account (known to
/// the venue, never sanctioned, no transfer fee), the venue's verbatim for an
/// untouched one.
async fn pre_transfer_check(state: &ApiState, body: Bytes, user: Address) -> Result<Response> {
    simulated(state, &body, user, |_| {
        Ok(Json(serde_json::json!({
            "isSanctioned": false,
            "userExists": true,
            "fee": "0.0",
            "userHasSentTx": true,
        }))
        .into_response())
    })
    .await
}

/// `clearinghouseState`: the sim's one-dex clearinghouse for a touched
/// account, the venue's verbatim otherwise. A dex the venue does not list (and
/// `ALL_DEXS`, which this type does not take) is the venue's 500 `null`.
async fn clearinghouse(
    state: &ApiState,
    body: Bytes,
    user: Address,
    dex: Option<String>,
) -> Result<Response> {
    let name = dex.as_deref().unwrap_or("");
    let Some(DexScope::One(name)) = dex_scope(&state.feed, name, false) else {
        return Ok(unknown_dex());
    };
    match dex_clearinghouse_of(state, user, &name)? {
        Some(served) => Ok(Json(served).into_response()),
        None => pass_through(state, &body).await,
    }
}

/// `spotClearinghouseState`: the unified reconciliation
/// (`available + maintenance == balance`), or the venue's verbatim.
async fn spot_state(state: &ApiState, body: Bytes, user: Address) -> Result<Response> {
    // Pure observation; the 1 Hz tick owns accrual and liquidation.
    simulated(state, &body, user, |account| {
        Ok(Json(crate::wire::SpotClearinghouseState {
            balances: account::spot_balances(account),
            token_to_available_after_maintenance: account::available_after_maintenance(
                account,
                &state.feed,
            )?,
        })
        .into_response())
    })
    .await
}

/// `userAbstraction`: the account's parseable mode, or the venue's verbatim.
async fn abstraction(state: &ApiState, body: Bytes, user: Address) -> Result<Response> {
    simulated(state, &body, user, |account| {
        Ok(Json(account::mode_of(account)).into_response())
    })
    .await
}

/// One `openOrders` row (the docs' shape: coin, limitPx, oid, side, sz,
/// timestamp; a resting order the devnet's book holds).
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenOrder {
    pub coin: String,
    pub limit_px: String,
    pub oid: u64,
    /// "A" (ask) or "B" (bid) — the venue's own letters.
    pub side: &'static str,
    pub sz: String,
    pub timestamp: u64,
    pub orig_sz: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloid: Option<alloy_primitives::B128>,
}

/// One `frontendOpenOrders` row (the docs' fuller shape).
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FrontendOpenOrder {
    pub coin: String,
    pub is_position_tpsl: bool,
    pub is_trigger: bool,
    pub limit_px: String,
    pub oid: u64,
    pub order_type: &'static str,
    pub orig_sz: String,
    pub reduce_only: bool,
    pub side: &'static str,
    pub sz: String,
    pub timestamp: u64,
    /// "N/A" for a plain order, "Triggered"/"Stop …" for triggers (the
    /// venue's own vocabulary; ours names the condition).
    pub trigger_condition: String,
    pub trigger_px: String,
    /// TP/SL children (the sim never creates them; the shape is the venue's).
    pub children: Vec<ChildOrder>,
    /// `null` for a trigger order, the limit's time in force otherwise.
    pub tif: Option<&'static str>,
    pub cloid: Option<alloy_primitives::B128>,
}

pub(crate) fn side_letter(side: crate::sim::order::Side) -> &'static str {
    match side {
        crate::sim::order::Side::Buy => "B",
        crate::sim::order::Side::Sell => "A",
    }
}

fn trigger_condition(order: &crate::sim::order::RestingOrder) -> String {
    match &order.trigger {
        None => "N/A".into(),
        Some(t) => {
            let kind = match t.tpsl {
                crate::sim::order::Tpsl::Tp => "Take Profit",
                crate::sim::order::Tpsl::Sl => "Stop",
            };
            let dir = match order.side {
                crate::sim::order::Side::Buy => MARKET,
                crate::sim::order::Side::Sell => LIMIT,
            };
            format!(
                "{kind} {dir} {}({})",
                if t.is_market { MARKET } else { LIMIT },
                t.trigger_px
            )
        }
    }
}

/// The account's resting orders in the selected dexes, newest first (the
/// venue's order: the sampled lists run from the latest oid down).
pub(crate) fn resting_in(
    state: &ApiState,
    user: Address,
    scope: &DexScope,
) -> Result<Vec<crate::sim::order::RestingOrder>> {
    let mut rows: Vec<_> = state
        .sim
        .resting_orders(Some(user))?
        .into_iter()
        .filter(|r| scope.covers(&r.coin))
        .collect();
    rows.sort_by_key(|r| std::cmp::Reverse(r.oid));
    Ok(rows)
}

/// `openOrders`: the devnet's resting book for a touched account (the venue's
/// own row shape), per dex or `ALL_DEXS`; untouched accounts pass through; an
/// unlisted dex is the venue's 500 `null`.
async fn open_orders(
    state: &ApiState,
    body: Bytes,
    user: Address,
    dex: Option<String>,
) -> Result<Response> {
    let Some(scope) = dex_scope(&state.feed, dex.as_deref().unwrap_or(""), true) else {
        return Ok(unknown_dex());
    };
    simulated(state, &body, user, |_| {
        let rows: Vec<OpenOrder> = resting_in(state, user, &scope)?
            .into_iter()
            .map(|r| OpenOrder {
                coin: r.coin.clone(),
                limit_px: r.px.to_string(),
                oid: r.oid,
                side: side_letter(r.side),
                sz: r.sz.to_string(),
                timestamp: r.placed_ms,
                orig_sz: r.orig_sz.to_string(),
                cloid: r.cloid,
            })
            .collect();
        Ok(Json(rows).into_response())
    })
    .await
}

/// `frontendOpenOrders`: the same rows with the frontend's extra fields.
async fn frontend_open_orders(
    state: &ApiState,
    body: Bytes,
    user: Address,
    dex: Option<String>,
) -> Result<Response> {
    let Some(scope) = dex_scope(&state.feed, dex.as_deref().unwrap_or(""), true) else {
        return Ok(unknown_dex());
    };
    simulated(state, &body, user, |_| {
        let rows: Vec<FrontendOpenOrder> = resting_in(state, user, &scope)?
            .iter()
            .map(frontend_row)
            .collect();
        Ok(Json(rows).into_response())
    })
    .await
}

/// One order's status row inside `orderStatus` (the docs' `order` object).
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatusOrderInner {
    pub coin: String,
    pub side: &'static str,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub trigger_condition: String,
    pub is_trigger: bool,
    pub trigger_px: String,
    /// TP/SL children (the devnet never creates them; the shape is the docs').
    pub children: Vec<ChildOrder>,
    pub is_position_tpsl: bool,
    pub reduce_only: bool,
    pub order_type: &'static str,
    pub orig_sz: String,
    pub tif: &'static str,
    pub cloid: Option<alloy_primitives::B128>,
}

/// A TP/SL child (docs' `children` element shape).
#[derive(Debug, serde::Serialize)]
pub(crate) struct ChildOrder {
    pub order: StatusOrderInner,
    pub r#type: &'static str,
}

/// The `orderStatus` wrapper (docs: `{order, status, statusTimestamp}`).
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatusOrder {
    pub order: StatusOrderInner,
    pub status: &'static str,
    pub status_timestamp: u64,
}

/// The `orderStatus` response: the docs' two shapes.
#[derive(Debug, serde::Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub(crate) enum OrderStatusResponse {
    /// Boxed: the order row dwarfs the unknown marker (clippy's large-variant
    /// rule; the enum rides every orderStatus response).
    Order {
        order: Box<StatusOrder>,
    },
    UnknownOid,
}

fn tif_name(tif: crate::sim::order::Tif) -> &'static str {
    match tif {
        crate::sim::order::Tif::Ioc => "Ioc",
        crate::sim::order::Tif::Gtc => "Gtc",
        crate::sim::order::Tif::Alo => "Alo",
    }
}

/// One resting order's frontend-row fields: the single mapping the two served
/// shapes share (`frontendOpenOrders` uses it whole; `orderStatus` extends it
/// with its own fields).
pub(crate) fn frontend_row(r: &crate::sim::order::RestingOrder) -> FrontendOpenOrder {
    FrontendOpenOrder {
        coin: r.coin.clone(),
        is_position_tpsl: false,
        is_trigger: r.trigger.is_some(),
        limit_px: r.px.to_string(),
        oid: r.oid,
        order_type: order_type(r),
        orig_sz: r.orig_sz.to_string(),
        reduce_only: matches!(r.reduce, crate::sim::order::Reduce::Only),
        side: side_letter(r.side),
        sz: r.sz.to_string(),
        timestamp: r.placed_ms,
        trigger_condition: trigger_condition(r),
        trigger_px: r
            .trigger
            .map(|t| t.trigger_px.to_string())
            .unwrap_or_else(|| "0.0".into()),
        children: Vec::new(),
        tif: if r.trigger.is_some() {
            None
        } else {
            Some(tif_name(r.tif))
        },
        cloid: r.cloid,
    }
}

/// The status row: the frontend row's fields (one mapping for both shapes)
/// plus the status-only extras.
fn status_from_frontend(
    front: FrontendOpenOrder,
    sz: String,
    tif: &'static str,
    cloid: Option<alloy_primitives::B128>,
) -> StatusOrderInner {
    StatusOrderInner {
        coin: front.coin,
        side: front.side,
        limit_px: front.limit_px,
        sz,
        oid: front.oid,
        timestamp: front.timestamp,
        trigger_condition: front.trigger_condition,
        is_trigger: front.is_trigger,
        trigger_px: front.trigger_px,
        children: Vec::new(),
        is_position_tpsl: front.is_position_tpsl,
        reduce_only: front.reduce_only,
        order_type: front.order_type,
        orig_sz: front.orig_sz,
        tif,
        cloid,
    }
}

fn status_inner(r: &crate::sim::order::RestingOrder, sz: String) -> StatusOrderInner {
    status_from_frontend(frontend_row(r), sz, tif_name(r.tif), r.cloid)
}

/// The terminal status label (the docs' orderStatus strings).
fn terminal_status_label(status: crate::sim::order::TerminalStatus) -> &'static str {
    match status {
        crate::sim::order::TerminalStatus::Filled => "filled",
        crate::sim::order::TerminalStatus::Canceled => "canceled",
        crate::sim::order::TerminalStatus::MarginCanceled => "marginCanceled",
    }
}

/// `orderStatus`: open from the book, terminal from the history, else the
/// docs' `unknownOid`.
async fn order_status(
    state: &ApiState,
    body: Bytes,
    user: Address,
    oid: crate::wire::OidOrCloid,
) -> Result<Response> {
    if state.sim.account(user)?.is_none() {
        return pass_through(state, &body).await;
    }
    let found = match oid {
        crate::wire::OidOrCloid::Oid(o) => match state.sim.resting_by_oid(user, o)? {
            Some(r) => Some((status_inner(&r, r.sz.to_string()), "open", r.placed_ms)),
            None => state.sim.terminal_by_oid(user, o)?.map(|t| {
                (
                    status_inner(&t.order, t.order.sz.to_string()),
                    terminal_status_label(t.status),
                    t.status_ms,
                )
            }),
        },
        crate::wire::OidOrCloid::Cloid(c) => match state.sim.resting_by_cloid(user, c)? {
            Some(r) => Some((status_inner(&r, r.sz.to_string()), "open", r.placed_ms)),
            None => state.sim.terminal_by_cloid(user, c)?.map(|t| {
                (
                    status_inner(&t.order, t.order.sz.to_string()),
                    terminal_status_label(t.status),
                    t.status_ms,
                )
            }),
        },
    };
    Ok(match found {
        Some((order, status, status_timestamp)) => Json(OrderStatusResponse::Order {
            order: Box::new(StatusOrder {
                order,
                status,
                status_timestamp,
            }),
        })
        .into_response(),
        None => Json(OrderStatusResponse::UnknownOid).into_response(),
    })
}

/// `activeAssetData`: the sim's derived row, or the venue's verbatim.
async fn active_asset(
    state: &ApiState,
    body: Bytes,
    user: Address,
    coin: String,
) -> Result<Response> {
    // A coin no dex lists is the venue's 500 `null` (on a builder coin as on
    // a default one), whoever asks.
    if state.feed.meta_for_coin(&coin).is_none() {
        return Ok(unknown_dex());
    }
    simulated(state, &body, user, |account| {
        Ok(Json(active_asset_data(state, user, account, &coin)?).into_response())
    })
    .await
}

/// `meta`: one dex's cached metadata (`""` or no `dex` = the default dex). A
/// dex the venue does not list, and `ALL_DEXS`, are the venue's 500 `null`.
fn meta(dex: &Option<String>, state: &ApiState) -> Result<Response> {
    match dex.as_deref().unwrap_or("") {
        "" => Ok(Json(state.feed.meta().ok_or(Error::Warming)?).into_response()),
        name => Ok(state
            .feed
            .meta_of(name)
            .map_or_else(unknown_dex, |meta| Json(meta).into_response())),
    }
}

/// `allPerpMetas`: one metadata entry per perp dex, in `perpDexs` order.
fn all_perp_metas(state: &ApiState) -> Result<Response> {
    let metas = state.feed.metas();
    if metas.is_empty() {
        return Err(Error::Warming);
    }
    Ok(Json(metas).into_response())
}

/// `metaAndAssetCtxs`: one dex's cached metadata and its coins' contexts, the
/// two lists the same length (the venue's positional contract; a dex whose
/// contexts have not all landed is warming, never a short list).
fn meta_and_ctxs(dex: &Option<String>, state: &ApiState) -> Result<Response> {
    let name = dex.as_deref().unwrap_or("");
    let served = state.feed.meta_and_ctxs_of(name);
    if !name.is_empty() && served.is_none() {
        return Ok(unknown_dex());
    }
    let MetaAndAssetCtxs(meta, ctxs) = served.ok_or(Error::Warming)?;
    if ctxs.len() != meta.universe.len() {
        return Err(Error::Warming);
    }
    Ok(Json(MetaAndAssetCtxs(meta, ctxs)).into_response())
}

/// `allMids`: a dex's own keys (the default dex's also carry the spot `@N` and
/// outcome `#N` keys), or every dex's at once for `ALL_DEXS`. An unlisted dex
/// is the venue's 200 `null`.
fn all_mids(dex: &Option<String>, state: &ApiState) -> Response {
    let Some(scope) = dex_scope(&state.feed, dex.as_deref().unwrap_or(""), true) else {
        return null_ok();
    };
    Json(mids_in(&state.feed, &scope)).into_response()
}

/// The `allMids` map of the selected dexes: one dex's own keys (the default
/// dex's also carry the spot `@N` and outcome `#N` keys), or every dex's at
/// once.
pub(crate) fn mids_in(feed: &MarketFeed, scope: &DexScope) -> crate::wire::AllMids {
    match scope {
        DexScope::One(name) => feed.all_mids_of(name),
        DexScope::All => crate::wire::AllMids(
            feed.dex_collateral_tokens()
                .into_iter()
                .map(|(name, _)| name)
                .chain(std::iter::once(String::new()))
                .flat_map(|name| feed.all_mids_of(&name).0)
                .collect(),
        ),
    }
}

/// `perpDexs`: `null` for the default dex, then the venue's own record of each
/// builder dex. A dex that landed without its record (a fixture) is served a
/// bare one rather than a `null`, which would read as the default dex.
fn perp_dexs(state: &ApiState) -> Result<Response> {
    let dexs = state.feed.all_dexs();
    if dexs.is_empty() {
        return Err(Error::Warming);
    }
    let rows: Vec<Option<crate::wire::PerpDex>> = dexs
        .into_iter()
        .map(|d| {
            if d.name.is_empty() {
                return None;
            }
            Some(d.raw.unwrap_or_else(|| crate::wire::PerpDex {
                full_name: d.name.clone(),
                name: d.name,
                deployer: Address::ZERO,
                oracle_updater: None,
                fee_recipient: None,
                asset_to_streaming_oi_cap: Vec::new(),
                asset_to_funding_clamp: Vec::new(),
                asset_to_funding_interest_rate: Vec::new(),
                asset_to_funding_multiplier: Vec::new(),
                sub_deployers: Vec::new(),
            }))
        })
        .collect();
    Ok(Json(rows).into_response())
}

/// `perpsAtOpenInterestCap`: one dex's coins at their cap. An unlisted dex,
/// and `ALL_DEXS`, are the venue's 500 `null`.
fn open_interest_caps(dex: &Option<String>, state: &ApiState) -> Result<Response> {
    let Some(DexScope::One(name)) = dex_scope(&state.feed, dex.as_deref().unwrap_or(""), false)
    else {
        return Ok(unknown_dex());
    };
    let caps = state
        .feed
        .open_interest_caps_of(&name)
        .ok_or(Error::Warming)?;
    Ok(Json(caps).into_response())
}

/// `l2Book`: the cached or scripted book for one coin, aggregated per the
/// request's parameters (docs: `nSigFigs ∈ {2,3,4,5}`, `mantissa ∈ {1,2,5}`
/// only with `nSigFigs = 5`). A parameter outside the documented set is a
/// named 400 — never silently ignored.
async fn l2_book(
    coin: &str,
    n_sig_figs: Option<u8>,
    mantissa: Option<u8>,
    state: &ApiState,
) -> Result<Response> {
    book_params_ok(n_sig_figs, mantissa)?;
    let now = crate::now_ms();
    // A configured market's book is the poller's; any other coin a dex lists
    // (a builder dex's `xyz:TSLA`, a default-dex coin outside `markets`) gets
    // its book on demand and stays wanted while it is asked for.
    let configured = state.feed.markets.iter().any(|m| m == coin);
    let book = if configured {
        state.feed.book(coin, now)
    } else {
        state.feed.ensure_book(coin).await
    };
    let Some(book) = book else {
        // The venue's own absence shape (measured live: `l2Book` for an
        // unknown coin answers HTTP 500 `null`, builder dexes included). A
        // coin the venue lists with no book yet is a different fault: the
        // plane is warming (503).
        return if configured || state.feed.meta_for_coin(coin).is_some() {
            Err(Error::Warming)
        } else {
            Ok(unknown_dex())
        };
    };
    Ok(Json(aggregate_book(book, n_sig_figs, mantissa.unwrap_or(1))).into_response())
}

/// The documented parameter set, refused by name otherwise.
pub(crate) fn book_params_ok(n_sig_figs: Option<u8>, mantissa: Option<u8>) -> Result<()> {
    if let Some(n) = n_sig_figs {
        if !(2..=5).contains(&n) {
            return Err(Error::bad_request(format!(
                "nSigFigs {n} is not in {{2,3,4,5}}"
            )));
        }
    }
    match (n_sig_figs, mantissa) {
        (None, Some(_)) => Err(Error::bad_request("mantissa requires nSigFigs = 5 (docs)")),
        (Some(5), Some(m)) if !matches!(m, 1 | 2 | 5) => Err(Error::bad_request(format!(
            "mantissa {m} is not in {{1,2,5}}"
        ))),
        (Some(n), Some(_)) if n != 5 => {
            Err(Error::bad_request("mantissa requires nSigFigs = 5 (docs)"))
        }
        _ => Ok(()),
    }
}

/// The venue's aggregation: prices round to `n` significant figures (the tick
/// is `mantissa × 10^(integer digits − n)`), levels at the same rounded price
/// merge (sizes and counts sum, saturating — a merge never panics), and each
/// side keeps its order (bids descending, asks ascending).
pub(crate) fn aggregate_book(
    book: crate::wire::L2Book,
    n_sig_figs: Option<u8>,
    mantissa: u8,
) -> crate::wire::L2Book {
    let Some(n) = n_sig_figs else {
        return book;
    };
    let mut out = book;
    for side in 0..2usize {
        let mut merged: std::collections::BTreeMap<Decimal, (Decimal, u64)> =
            std::collections::BTreeMap::new();
        for level in &out.levels[side] {
            let px = round_to_sig_figs(level.px, n, mantissa);
            let entry = merged.entry(px).or_insert((Decimal::ZERO, 0));
            entry.0 = entry.0.saturating_add(level.sz);
            entry.1 = entry.1.saturating_add(level.n);
        }
        let mut levels: Vec<crate::wire::Level> = merged
            .into_iter()
            .map(|(px, (sz, n))| crate::wire::Level { px, sz, n })
            .collect();
        if side == 0 {
            levels.reverse();
        }
        out.levels[side] = levels;
    }
    out
}

/// Round a price to `n` significant figures on a `mantissa`-scaled tick.
fn round_to_sig_figs(px: Decimal, n: u8, mantissa: u8) -> Decimal {
    if px <= Decimal::ZERO {
        return px;
    }
    let norm = px.normalize();
    let int_digits = norm.mantissa().unsigned_abs().ilog10() as i32 + 1 - norm.scale() as i32;
    let step_exp = int_digits - i32::from(n);
    // 10^step_exp by repeated multiply: exact for the magnitudes a book
    // carries; beyond them the price is served unrounded (a bounded,
    // documented degradation rather than a panic).
    let ten = Decimal::from(10u64);
    let mut step = Decimal::ONE;
    for _ in 0..step_exp.abs().min(28) {
        step = if step_exp >= 0 {
            step.saturating_mul(ten)
        } else {
            step.saturating_mul(Decimal::new(1, 1))
        };
    }
    let step = step.saturating_mul(Decimal::from(mantissa));
    if step.is_zero() {
        return px;
    }
    (px / step).round().saturating_mul(step)
}

/// The venue's implicit default table (id 1): the metadata never lists it,
/// but the real API serves it (measured live 2026-10-25:
/// `{"description":"","marginTiers":[{"lowerBound":"0.0","maxLeverage":1}]}`),
/// so the simulator must too.
pub(crate) fn default_margin_table() -> crate::wire::MarginTable {
    crate::wire::MarginTable {
        description: String::new(),
        margin_tiers: vec![crate::wire::MarginTier {
            lower_bound: Decimal::new(0, 1),
            max_leverage: 1,
        }],
    }
}

/// `marginTable`: one table by id, resolved across every dex's tables (the
/// ids are the dexes' own: a builder dex's table 30 sits in its meta), with
/// the venue's implicit default served for id 1. An id no dex lists is the
/// venue's 500 `null`.
fn margin_table(id: u32, state: &ApiState) -> Result<Response> {
    if id == 1 {
        return Ok(Json(default_margin_table()).into_response());
    }
    let metas = state.feed.metas();
    if metas.is_empty() {
        return Err(Error::Warming);
    }
    Ok(metas
        .iter()
        .flat_map(|meta| meta.margin_tables.iter())
        .find(|(tid, _)| *tid == id)
        .map_or_else(unknown_dex, |(_, table)| Json(table).into_response()))
}

/// `exchangeStatus`: the served clock.
fn exchange_status() -> Response {
    Json(ExchangeStatus {
        special_statuses: None,
        time: Sim::served_time(crate::now_ms()),
    })
    .into_response()
}

/// The default dex's served `clearinghouseState` for a touched account (the
/// control plane's read).
pub(crate) fn clearinghouse_of(
    state: &ApiState,
    address: Address,
) -> Result<Option<ClearinghouseState>> {
    dex_clearinghouse_of(state, address, "")
}

/// The served `clearinghouseState` of one dex for a touched account: accrual
/// *first*, then the read (the verifier caught the reverse order: the served figures
/// lagged one accrual window behind the stored state).
pub(crate) fn dex_clearinghouse_of(
    state: &ApiState,
    address: Address,
    dex: &str,
) -> Result<Option<ClearinghouseState>> {
    let now = crate::now_ms();
    let served_time = Sim::served_time(now);
    // A read is an observation, not a step: the 1 Hz account tick owns
    // funding/liquidation. Running the automaton here made a read of an
    // underwater account liquidate it, so two pollers could change the
    // state they were sent to observe.
    let Some(account) = state.sim.account(address)? else {
        return Ok(None);
    };
    // The served `time` is the **oldest live mark** the account is priced
    // from, not the read's clock. A stalled feed keeps its old mark; stamping
    // the read's clock made that stale mark look fresh and passed the
    // client's age gate. Scenario-owned coins are exempt (their stamp is the
    // scenario's present, not a market observation).
    let observation_time = account
        .positions
        .keys()
        .filter(|coin| dex_of_coin(coin) == dex)
        .filter(|coin| !state.feed.market_is_owned(coin))
        .filter_map(|coin| state.feed.last_ctx_at(coin))
        .min()
        .map_or(served_time, |oldest| served_time.min(oldest));
    let served = account::clearinghouse(&account, &state.feed, dex, observation_time)?;
    if !account::identity_holds(&served) {
        state
            .sim
            .metrics
            .identity_violations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(
            address = %address,
            "served clearinghouseState failed the identity re-check; the consumer's gate would refuse it"
        );
    }
    Ok(Some(served))
}

/// The venue's own answer for an untouched account (verbatim passthrough).
/// The per-user read's guard: a simulated account computes the answer; an
/// unconfigured one is passed through to the live venue verbatim (the
/// overlay's discipline — the sim's own state, or the venue's answer, never a
/// guess). One shape for every per-user read.
async fn simulated(
    state: &ApiState,
    body: &Bytes,
    user: Address,
    compute: impl FnOnce(&crate::sim::account::Account) -> Result<Response>,
) -> Result<Response> {
    match state.sim.account(user)? {
        Some(account) => compute(&account),
        None => pass_through(state, body).await,
    }
}

async fn pass_through(state: &ApiState, body: &Bytes) -> Result<Response> {
    // Budget accounting: a pass-through is an outbound venue
    // read, so it spends the conservative heavy weight (most info types weigh
    // 20; clearinghouseState/l2Book/allMids weigh 2) — the poller backs off
    // when test reads and its own polls add up. Not gated: refusing a caller's
    // read would look like a venue outage.
    state
        .feed
        .budget
        .try_spend(crate::feed::budget::weight::HEAVY);
    let resp = state.api.raw_info(body.to_vec()).await?;
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let bytes = resp.bytes().await?;
    Ok((
        status,
        [(header::CONTENT_TYPE, diorama_anvil::JSON_CONTENT_TYPE)],
        bytes,
    )
        .into_response())
}

/// `activeAssetData`: the coin's mark, the user's leverage, and the sizes the
/// withdrawable margin supports at that leverage.
pub(crate) fn active_asset_data(
    state: &ApiState,
    user: Address,
    account: &account::Account,
    coin: &str,
) -> Result<ActiveAssetData> {
    let mark = state.feed.mark(coin).ok_or_else(|| Error::NoMarket {
        coin: coin.to_string(),
    })?;
    let leverage = account
        .positions
        .get(coin)
        .map(|p| Leverage {
            kind: p.leverage,
            value: p.leverage_value,
            raw_usd: None,
        })
        // A no-position coin serves the venue's own default: the live sample
        // measured `leverage: {type: "cross", value: 20}` for an account with
        // no position (2026-10-23, §16.5's probe) — not a fabrication.
        .unwrap_or(Leverage {
            kind: LeverageType::Cross,
            value: 20,
            raw_usd: None,
        });
    // `availableToTrade` is the free margin (value minus used minus the open
    // orders' reservations) — the same figure the placement check reads — not
    // `withdrawable`, which the docs' transfer rule bounds.
    let available = state
        .sim
        .available_for(&state.feed, user, crate::feed::dex::dex_of_coin(coin), None)?
        .ok_or_else(|| Error::NotFound {
            what: format!("{user} is not a simulated account"),
        })?
        .max(Decimal::ZERO);
    let max_sz = account::ratio_or(
        account::mul(available, Decimal::from(leverage.value), "max size")?,
        mark,
        Decimal::ZERO,
    );
    Ok(ActiveAssetData {
        user,
        coin: coin.to_string(),
        leverage,
        max_trade_szs: [max_sz, max_sz],
        available_to_trade: [available, available],
        mark_px: mark,
    })
}

/// `userAbstraction` for a sim account (the control read shares it).
pub(crate) fn abstraction_of(account: &account::Account) -> UserAbstraction {
    account::mode_of(account)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The book aggregation follows the docs: prices round to `n` significant
    /// figures, levels at the same rounded price merge, and out-of-set
    /// parameters refuse by name.
    #[test]
    fn l2_book_aggregation_follows_the_docs() {
        let level = |px: &str, sz: &str, n: u64| crate::wire::Level {
            px: px.parse().unwrap(),
            sz: sz.parse().unwrap(),
            n,
        };
        let book = crate::wire::L2Book {
            coin: "ETH".into(),
            time: 0,
            levels: [
                vec![level("100.4", "1", 2), level("100.9", "2", 1)],
                vec![level("106.0", "1", 1)],
            ],
            spread: None,
        };
        // n=2: 100.4 and 100.9 both round to 100 (merged: sz 3, n 3);
        // 106 rounds to 110 (half away from zero on the step of 10).
        let a = aggregate_book(book.clone(), Some(2), 1);
        assert_eq!(a.levels[0].len(), 1, "same rounded price merges");
        assert_eq!(a.levels[0][0].px, "100".parse().unwrap());
        assert_eq!(a.levels[0][0].sz, "3".parse().unwrap());
        assert_eq!(a.levels[0][0].n, 3);
        assert_eq!(a.levels[1][0].px, "110".parse().unwrap());
        // No params: verbatim, both sides in their own order.
        let c = aggregate_book(book, None, 1);
        assert_eq!(c.levels[0].len(), 2);
        assert_eq!(c.levels[0][0].px, "100.4".parse().unwrap());
        // The documented parameter set, refused by name otherwise.
        assert!(book_params_ok(Some(2), None).is_ok());
        assert!(book_params_ok(Some(5), Some(5)).is_ok());
        assert!(book_params_ok(Some(6), None).is_err());
        assert!(book_params_ok(None, Some(5)).is_err());
        assert!(book_params_ok(Some(4), Some(5)).is_err());
        assert!(book_params_ok(Some(5), Some(3)).is_err());
    }

    /// The implicit default table's served body, pinned against the real
    /// API's measured answer (live 2026-10-25) — the venue serves it for
    /// `marginTable` id 1 although the metadata never lists id 1.
    #[test]
    fn the_default_margin_table_matches_the_venue() {
        let body = serde_json::to_value(default_margin_table()).unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                // The wire serializer trims trailing zeros (nano
                // granularity), so the measured "0.0" is served as "0".
                "description": "",
                "marginTiers": [{"lowerBound": "0", "maxLeverage": 1}]
            })
        );
    }

    /// `activeAssetData` for an account the sim never saw is refused by name:
    /// the old zero available balance fabricated a tradable row for an
    /// unknown user.
    #[test]
    fn active_asset_data_refuses_an_unsimulated_account() {
        let feed = crate::sim::feed_with(&[("ETH", "100", "101")]);
        feed.put_meta(crate::sim::meta_with("ETH", 10), crate::now_ms());
        let api = crate::feed::rest::RestClient::new(&["http://127.0.0.1:1".to_string()]).unwrap();
        let state = ApiState::new(
            Arc::new(feed),
            Arc::new(crate::sim::Sim::memory()),
            api,
            None,
            None,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        let user = Address::repeat_byte(0xaa);
        let err = active_asset_data(&state, user, &account::Account::default(), "ETH").unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }), "{err}");
        assert!(err.to_string().contains("not a simulated account"), "{err}");
    }

    /// `allPerpMetas` is `meta` in a one-element list (the first dex only);
    /// the annotation and category lists are empty arrays, never a string.
    #[test]
    fn all_perp_metas_wraps_the_first_dex_meta() {
        let feed = crate::sim::feed_with(&[("ETH", "100", "101")]);
        feed.put_meta(crate::sim::meta_with("ETH", 10), crate::now_ms());
        let api = crate::feed::rest::RestClient::new(&["http://127.0.0.1:1".to_string()]).unwrap();
        let state = ApiState::new(
            Arc::new(feed),
            Arc::new(crate::sim::Sim::memory()),
            api,
            None,
            None,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        let metas = serde_json::to_value(vec![state.feed.meta().unwrap()]).unwrap();
        let arr = metas.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["universe"][0]["name"], "ETH");
        assert_eq!(arr[0]["collateralToken"], 0);
        assert!(all_perp_metas(&state).is_ok());
    }
}
