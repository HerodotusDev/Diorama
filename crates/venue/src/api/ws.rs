//! The venue's websocket lane (the sim serves "info + WS" on its
//! info port): the channels the venue pushes — `allMids`, `activeAssetCtx`,
//! `l2Book`, and `webData2` — fed from the same composed cache the REST reads
//! serve, so a subscriber never sees a different mark/book/account than the
//! account plane used.
//!
//! Protocol (`feed::ws`'s vocabulary, the client's own): the client subscribes
//! with `{"method":"subscribe","subscription":{"type":…}}`, the server acks
//! with a `subscriptionResponse` frame and pushes `{"channel":…,"data":…}` on
//! each configured account-tick while the subscription is live; `{"method":"ping"}` answers
//! `{"channel":"pong"}`. An unsupported channel (or an unknown coin) is
//! answered with the venue's `error` frame — never silence.
//!
//! `l2Book` pushes the composed book exactly as `/info`'s `l2Book` serves it
//! (`{"coin","time","levels"}`, the live book stamped with its last update,
//! a scripted book with the scenario's present). `webData2` pushes the sim
//! account's served state: `clearinghouseState`, `spotState`, `assetCtxs`
//! (omitted while the meta is warming — never an invented empty list), and
//! `user`.
use super::candles::CandleSub;
use super::ws_codec::{self, FastCtx, PacCtx};
use super::ApiState;
use crate::sim::Sim;
use crate::wire::{
    ActiveAssetData, AllMids, AssetCtx, ClearinghouseState, L2Book, MarginSummary,
    SpotClearinghouseState,
};
use alloy_primitives::Address;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// One subscription the sim serves, in the real venue's vocabulary. The ack
/// echoes the subscription with the venue's defaults filled in (`nSigFigs:
/// null`, `fast: false`, `dex: ""`, ...), exactly as the real venue does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum Subscription {
    #[serde(rename = "allMids")]
    AllMids,
    #[serde(rename = "activeAssetCtx")]
    ActiveAssetCtx { coin: String },
    #[serde(rename = "l2Book")]
    L2Book {
        coin: String,
        #[serde(rename = "nSigFigs", default)]
        n_sig_figs: Option<u8>,
        #[serde(default)]
        mantissa: Option<u8>,
        #[serde(default)]
        fast: bool,
    },
    /// The legacy account channel, kept beside the current ones.
    #[serde(rename = "webData2")]
    WebData2 { user: Address },
    /// The real venue's per-dex account state channel (`{dex, user,
    /// clearinghouseState}`).
    #[serde(rename = "clearinghouseState")]
    ClearinghouseState {
        #[serde(default)]
        dex: String,
        user: Address,
    },
    /// The real venue's open-orders channel (`{dex, user, orders}`).
    #[serde(rename = "openOrders")]
    OpenOrders {
        #[serde(default)]
        dex: String,
        user: Address,
    },
    /// The real venue's spot-state channel (`{user, spotState}`).
    #[serde(rename = "spotState")]
    SpotState {
        user: Address,
        #[serde(rename = "ignorePortfolioMargin", default)]
        ignore_portfolio_margin: bool,
    },
    /// Every perp's mark and mid, deflate-compressed, changed fields after the
    /// first frame.
    #[serde(rename = "fastAssetCtxs")]
    FastAssetCtxs,
    /// Every perp's full context, per dex, deflate-compressed, changed fields
    /// after the first frame.
    #[serde(rename = "pac")]
    Pac,
    /// The order book: a full book first, compressed diffs after it.
    #[serde(rename = "l2")]
    L2 {
        c: String,
        #[serde(default)]
        s: Option<u8>,
        #[serde(default)]
        m: Option<u8>,
    },
    #[serde(rename = "candle")]
    Candle { coin: String, interval: String },
    /// The account's state on every perp dex (`[[dex, state], ...]`).
    #[serde(rename = "allDexsClearinghouseState")]
    AllDexsClearinghouseState { user: Address },
    #[serde(rename = "userFills")]
    UserFills {
        user: Address,
        #[serde(rename = "aggregateByTime", default)]
        aggregate_by_time: bool,
    },
    #[serde(rename = "userFundings")]
    UserFundings { user: Address },
    #[serde(rename = "activeAssetData")]
    ActiveAssetData { user: Address, coin: String },
}

/// The candle intervals the venue accepts.
const CANDLE_INTERVALS: [&str; 14] = [
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "3d", "1w", "1M",
];

/// One client request (the client's `Subscribe`/`Ping` shapes, plus
/// `unsubscribe`: the venue's own method set).
#[derive(Debug, Deserialize)]
#[serde(tag = "method", rename_all = "lowercase")]
pub(crate) enum WsRequest {
    Subscribe { subscription: Subscription },
    Unsubscribe { subscription: Subscription },
    Ping,
}

/// One server frame. The shapes mirror `feed::ws`'s parse types; the
/// round-trip test parses every variant back through the client's own parser.
#[derive(Debug, Serialize)]
#[serde(tag = "channel")]
pub(crate) enum WsPush {
    #[serde(rename = "allMids")]
    AllMids { data: AllMidsData },
    #[serde(rename = "activeAssetCtx")]
    ActiveAssetCtx { data: CtxData },
    #[serde(rename = "l2Book")]
    L2Book { data: L2Book },
    #[serde(rename = "webData2")]
    WebData2 { data: WebData2Data },
    #[serde(rename = "clearinghouseState")]
    ClearinghouseState { data: ClearinghouseStateData },
    #[serde(rename = "openOrders")]
    OpenOrders { data: OpenOrdersData },
    #[serde(rename = "spotState")]
    SpotState { data: SpotStateData },
    /// Base64 of a raw deflate stream (see `ws_codec`).
    #[serde(rename = "fastAssetCtxs")]
    FastAssetCtxs { data: String },
    #[serde(rename = "pac")]
    Pac { data: String },
    /// `{"s": book}` first, `{"c": compressed diff}` after.
    #[serde(rename = "l2")]
    L2 { data: serde_json::Value },
    #[serde(rename = "allDexsClearinghouseState")]
    AllDexsClearinghouseState { data: serde_json::Value },
    #[serde(rename = "userFills")]
    UserFills { data: serde_json::Value },
    #[serde(rename = "userFundings")]
    UserFundings { data: serde_json::Value },
    #[serde(rename = "activeAssetData")]
    ActiveAssetData { data: ActiveAssetData },
    #[serde(rename = "subscriptionResponse")]
    SubscriptionResponse { data: SubscriptionAck },
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "error")]
    Error { data: String },
}

#[derive(Debug, Serialize)]
pub(crate) struct AllMidsData {
    mids: AllMids,
}

#[derive(Debug, Serialize)]
pub(crate) struct CtxData {
    coin: String,
    ctx: AssetCtx,
}

/// The real venue's `webData2` account payload: the served clearinghouse and
/// spot states, the market contexts, and the user. Optional fields the sim
/// cannot serve yet are omitted rather than fabricated (`assetCtxs` while the
/// meta is warming).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WebData2Data {
    user: Address,
    clearinghouse_state: ClearinghouseState,
    spot_state: SpotClearinghouseState,
    #[serde(skip_serializing_if = "Option::is_none")]
    asset_ctxs: Option<Vec<AssetCtx>>,
}

/// The real venue's `clearinghouseState` channel payload.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClearinghouseStateData {
    /// The perp dex; the empty string is the first (default) dex.
    dex: String,
    user: Address,
    clearinghouse_state: ClearinghouseState,
}

/// The real venue's `openOrders` channel payload.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenOrdersData {
    dex: String,
    user: Address,
    orders: Vec<super::info::FrontendOpenOrder>,
}

/// The real venue's `spotState` channel payload.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SpotStateData {
    user: Address,
    spot_state: SpotState,
}

/// The real venue's `spotState` body: balances only (the WS shape omits the
/// REST read's `tokenToAvailableAfterMaintenance`).
#[derive(Debug, Serialize)]
pub(crate) struct SpotState {
    balances: Vec<crate::wire::SpotBalance>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SubscriptionAck {
    method: &'static str,
    subscription: Subscription,
}

/// `GET /ws` (upgrade): one session per subscriber.
pub async fn ws(
    State(state): State<Arc<ApiState>>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| session(state, socket))
}

/// What a live subscription remembers between pushes: the previous state the
/// next diff is computed against, or the relay it listens to.
enum Mem {
    /// A channel that pushes its whole state every tick.
    Whole,
    /// Nothing pushed yet on a diffing channel.
    Fresh,
    Fast(BTreeMap<String, FastCtx>),
    Pac(Vec<Option<PacCtx>>),
    L2(L2Book),
    Candle(CandleSub),
    /// The newest fill id (`tid`) already pushed.
    Fills(Option<u64>),
    /// The newest funding `(time, coin)` already pushed.
    Fundings(Option<(u64, String)>),
}

/// One live subscription and what it remembers.
pub(crate) struct Live {
    sub: Subscription,
    mem: Mem,
}

/// One connection: read requests, push the subscribed channels on the 1 Hz
/// tick (the automaton's own cadence), and push a subscription's first frame
/// right after its ack (the real venue does).
async fn session(state: Arc<ApiState>, mut socket: WebSocket) {
    let mut subs: Vec<Live> = Vec::new();
    let mut tick = tokio::time::interval(state.push_interval);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                // One clock read per tick: every channel in this tick serves
                // the same instant (a scripted book is stamped with it).
                let now = crate::now_ms();
                for live in &mut subs {
                    for frame in pushes(live, &state, now) {
                        if socket.send(Message::Text(frame.into())).await.is_err() {
                            return;
                        }
                    }
                }
            }
            msg = socket.recv() => {
                let Some(Ok(Message::Text(text))) = msg else {
                    if msg.is_none() || matches!(msg, Some(Err(_))) { return; }
                    continue; // binary/ping/pong frames: nothing to answer
                };
                let before = subs.len();
                let reply = request_reply(&text, &mut subs, &state);
                let Ok(reply_text) = serde_json::to_string(&reply) else { continue };
                if socket.send(Message::Text(reply_text.into())).await.is_err() {
                    return;
                }
                if subs.len() > before {
                    if let Some(live) = subs.last_mut() {
                        for frame in pushes(live, &state, crate::now_ms()) {
                            if socket.send(Message::Text(frame.into())).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// The venue's answer to a request it cannot parse: the request text echoed.
fn parse_error(text: &str) -> WsPush {
    WsPush::Error {
        data: format!("Error parsing JSON into valid websocket request: {text}"),
    }
}

/// One request's answer: the ack or the error, or the pong. A subscription is
/// added at most once and removed at most once: the venue answers a repeat of
/// either with an error frame naming the subscription, not an ack.
pub(super) fn request_reply(text: &str, subs: &mut Vec<Live>, state: &ApiState) -> WsPush {
    let Ok(request) = serde_json::from_str::<WsRequest>(text) else {
        return parse_error(text);
    };
    match request {
        WsRequest::Ping => WsPush::Pong,
        WsRequest::Subscribe { subscription } => {
            let named = subscription_json(&subscription);
            if subs.iter().any(|live| live.sub == subscription) {
                return WsPush::Error {
                    data: format!("Already subscribed: {named}"),
                };
            }
            let mem = match admit(&subscription, state) {
                Ok(mem) => mem,
                Err(Refusal::Parse) => return parse_error(text),
                Err(Refusal::Invalid) => {
                    return WsPush::Error {
                        data: format!("Invalid subscription {named}"),
                    };
                }
                Err(Refusal::Named(reason)) => return WsPush::Error { data: reason },
            };
            subs.push(Live {
                sub: subscription.clone(),
                mem,
            });
            WsPush::SubscriptionResponse {
                data: SubscriptionAck {
                    method: "subscribe",
                    subscription,
                },
            }
        }
        WsRequest::Unsubscribe { subscription } => {
            let before = subs.len();
            subs.retain(|live| live.sub != subscription);
            if subs.len() == before {
                return WsPush::Error {
                    data: format!("Already unsubscribed: {}", subscription_json(&subscription)),
                };
            }
            WsPush::SubscriptionResponse {
                data: SubscriptionAck {
                    method: "unsubscribe",
                    subscription,
                },
            }
        }
    }
}

/// A subscription as the venue's error frames print it.
fn subscription_json(sub: &Subscription) -> String {
    serde_json::to_string(sub).unwrap_or_default()
}

/// Why a subscription is refused.
enum Refusal {
    /// The request does not parse as a valid one (the venue's parse error).
    Parse,
    /// A well-formed subscription the venue does not serve (unknown coin).
    Invalid,
    /// A refusal that names its own reason (the sim's own lanes).
    Named(String),
}

/// Admit a subscription: `Ok` with the memory its pushes diff against, or the
/// refusal. A bookless coin is refused at subscribe time, never left silent:
/// the composed book is the same one `/info`'s `l2Book` serves.
#[allow(clippy::too_many_lines)] // one match over the served subscriptions: splitting it would hide which channel refuses what
fn admit(sub: &Subscription, state: &ApiState) -> Result<Mem, Refusal> {
    let feed = &state.feed;
    // The account plane is addressable from the API state: a touched account
    // is computed from the sim, an untouched one is served the venue's empty
    // shape (the same contract `/info`'s passthrough holds). Only a state
    // read fault refuses the subscription.
    let account_ok =
        |user: &Address| {
            state.sim.account(*user).map(|_| ()).map_err(|e| {
                Refusal::Named(format!("account state for `{user}` is unavailable: {e}"))
            })
        };
    let has_book = |coin: &str| {
        if feed.book(coin, crate::now_ms()).is_some() {
            Ok(())
        } else {
            Err(Refusal::Named(format!(
                "coin `{coin}` has no book in the venue's feed"
            )))
        }
    };
    match sub {
        Subscription::AllMids => Ok(Mem::Whole),
        Subscription::ActiveAssetCtx { coin } if feed.mark(coin).is_some() => Ok(Mem::Whole),
        Subscription::ActiveAssetCtx { .. } => Err(Refusal::Invalid),
        Subscription::L2Book {
            coin,
            n_sig_figs,
            mantissa,
            ..
        } => {
            super::info::book_params_ok(*n_sig_figs, *mantissa).map_err(|_| Refusal::Invalid)?;
            has_book(coin)?;
            Ok(Mem::Whole)
        }
        Subscription::L2 { c, s, m } => {
            super::info::book_params_ok(*s, *m).map_err(|_| Refusal::Invalid)?;
            has_book(c)?;
            Ok(Mem::Fresh)
        }
        Subscription::WebData2 { user }
        | Subscription::ClearinghouseState { user, .. }
        | Subscription::OpenOrders { user, .. }
        | Subscription::SpotState { user, .. }
        | Subscription::AllDexsClearinghouseState { user } => {
            account_ok(user)?;
            Ok(Mem::Whole)
        }
        Subscription::ActiveAssetData { user, coin } => {
            account_ok(user)?;
            if feed.mark(coin).is_none() {
                return Err(Refusal::Invalid);
            }
            Ok(Mem::Whole)
        }
        Subscription::FastAssetCtxs | Subscription::Pac => Ok(Mem::Fresh),
        Subscription::Candle { coin, interval } => {
            if !CANDLE_INTERVALS.contains(&interval.as_str()) {
                return Err(Refusal::Parse);
            }
            state
                .candles
                .subscribe(coin, interval)
                .map(Mem::Candle)
                .ok_or_else(|| {
                    Refusal::Named("the venue's candle stream is unavailable".to_string())
                })
        }
        Subscription::UserFills { user, .. } => {
            account_ok(user)?;
            Ok(Mem::Fills(None))
        }
        Subscription::UserFundings { user } => {
            account_ok(user)?;
            Ok(Mem::Fundings(None))
        }
    }
}

/// The frames one live subscription pushes on this tick: none while a diffing
/// channel has nothing new, one for the whole-state channels.
#[allow(clippy::too_many_lines)] // one match over the channels: the diffing state lives in the arms
pub(super) fn pushes(live: &mut Live, state: &ApiState, now_ms: u64) -> Vec<String> {
    let feed = &state.feed;
    match (&live.sub, &mut live.mem) {
        (Subscription::FastAssetCtxs, mem) => {
            let next = ws_codec::fast_snapshot(feed);
            if next.is_empty() {
                // The meta has not landed: nothing to push yet, retry.
                return Vec::new();
            }
            let prev = match mem {
                Mem::Fast(prev) => Some(&*prev),
                _ => None,
            };
            let Some(frame) = ws_codec::fast_frame(prev, &next) else {
                return Vec::new();
            };
            *mem = Mem::Fast(next);
            encode(&WsPush::FastAssetCtxs { data: frame })
        }
        (Subscription::Pac, mem) => {
            let Some(next) = ws_codec::pac_snapshot(feed) else {
                return Vec::new();
            };
            let prev = match mem {
                Mem::Pac(prev) => Some(&*prev),
                _ => None,
            };
            let Some(frame) = ws_codec::pac_frame(prev, &next) else {
                return Vec::new();
            };
            *mem = Mem::Pac(next);
            encode(&WsPush::Pac { data: frame })
        }
        (Subscription::L2 { c, s, m }, mem) => {
            let Some(book) = feed.book(c, now_ms) else {
                return encode(&WsPush::Error {
                    data: format!("coin `{c}` has no book in the venue's feed"),
                });
            };
            let next = match s {
                Some(_) => super::info::aggregate_book(book, *s, m.unwrap_or(1)),
                None => book,
            };
            let data = match mem {
                Mem::L2(prev) => {
                    let Some(diff) = ws_codec::l2_diff(prev, &next) else {
                        return Vec::new();
                    };
                    serde_json::json!({ "c": ws_codec::deflate_b64(&diff) })
                }
                _ => serde_json::json!({ "s": next }),
            };
            *mem = Mem::L2(next);
            encode(&WsPush::L2 { data })
        }
        (Subscription::Candle { .. }, Mem::Candle(candles)) => {
            let mut frames = Vec::new();
            if let Some(first) = candles.take_first() {
                frames.push(first);
            }
            if let Some(latest) = candles.latest() {
                frames.push(latest);
            }
            frames
        }
        (Subscription::UserFills { user, .. }, Mem::Fills(cursor)) => {
            user_fills_push(state, *user, cursor)
        }
        (Subscription::UserFundings { user }, Mem::Fundings(cursor)) => {
            user_fundings_push(state, *user, cursor)
        }
        (sub, _) => push_frame(sub, state, now_ms).into_iter().collect(),
    }
}

fn encode(push: &WsPush) -> Vec<String> {
    match serde_json::to_string(push) {
        Ok(frame) => vec![frame],
        Err(e) => {
            tracing::warn!(error = %e, "ws push serialization failed");
            Vec::new()
        }
    }
}

/// The frame a whole-state subscription pushes, from the composed cache.
/// `None` = the coin has no ctx yet (the feed is warming; the tick retries) or
/// the channel is one of the diffing ones (`pushes` owns those): a
/// serialization fault on these plain shapes is a bug, named here.
fn push_frame(sub: &Subscription, state: &ApiState, now_ms: u64) -> Option<String> {
    let feed = &state.feed;
    let push = match sub {
        Subscription::AllMids => WsPush::AllMids {
            data: AllMidsData {
                mids: feed.all_mids(),
            },
        },
        Subscription::ActiveAssetCtx { coin } => WsPush::ActiveAssetCtx {
            data: CtxData {
                coin: coin.clone(),
                ctx: crate::sim::account::ctx_of(feed, coin)?,
            },
        },
        // The composed book: scripted override over the live remainder, stamped
        // exactly as the REST read stamps it (`feed.book` owns that rule),
        // aggregated per the subscription's parameters.
        Subscription::L2Book {
            coin,
            n_sig_figs,
            mantissa,
            ..
        } => match feed.book(coin, now_ms) {
            Some(book) => WsPush::L2Book {
                data: match n_sig_figs {
                    Some(_) => {
                        super::info::aggregate_book(book, *n_sig_figs, mantissa.unwrap_or(1))
                    }
                    None => book,
                },
            },
            // A book that vanished after the ack is an error frame, not
            // silence (the subscriber must know its channel stopped serving).
            None => WsPush::Error {
                data: format!("coin `{coin}` has no book in the venue's feed"),
            },
        },
        // The account plane's channels share one body; the channel name only
        // selects the frame shape.
        _ => account_push(sub, state)?,
    };
    match serde_json::to_string(&push) {
        Ok(frame) => Some(frame),
        Err(e) => {
            tracing::warn!(error = %e, "ws push serialization failed");
            None
        }
    }
}

/// The account-plane frame for one subscription. `None` only for a non-account
/// variant (the caller's match has already handled the market channels).
#[allow(clippy::too_many_lines)] // one match over the account channels
fn account_push(sub: &Subscription, state: &ApiState) -> Option<WsPush> {
    let push = match sub {
        Subscription::WebData2 { user } => match web_data2(state, *user) {
            Ok(data) => WsPush::WebData2 { data },
            Err(e) => WsPush::Error {
                data: format!("webData2 for `{user}` is unavailable: {e}"),
            },
        },
        Subscription::ClearinghouseState { dex, user } => match dex_state(state, *user, dex) {
            Ok(clearinghouse_state) => WsPush::ClearinghouseState {
                data: ClearinghouseStateData {
                    dex: dex.clone(),
                    user: *user,
                    clearinghouse_state,
                },
            },
            Err(e) => WsPush::Error {
                data: format!("clearinghouseState for `{user}` is unavailable: {e}"),
            },
        },
        Subscription::AllDexsClearinghouseState { user } => match dex_state(state, *user, "") {
            Ok(first) => WsPush::AllDexsClearinghouseState {
                data: serde_json::json!({
                    "user": user,
                    "clearinghouseStates": [["", first]],
                }),
            },
            Err(e) => WsPush::Error {
                data: format!("allDexsClearinghouseState for `{user}` is unavailable: {e}"),
            },
        },
        Subscription::OpenOrders { dex, user } => open_orders_push(*user, dex, state),
        Subscription::SpotState { user, .. } => spot_state_push(*user, state),
        Subscription::ActiveAssetData { user, coin } => match state.sim.account(*user) {
            Ok(Some(account)) => {
                match super::info::active_asset_data(state, *user, &account, coin) {
                    Ok(data) => WsPush::ActiveAssetData { data },
                    Err(e) => WsPush::Error {
                        data: format!("activeAssetData for `{user}` is unavailable: {e}"),
                    },
                }
            }
            Ok(None) => WsPush::Error {
                data: format!(
                    "activeAssetData for `{user}` is unavailable: not a simulated account"
                ),
            },
            Err(e) => WsPush::Error {
                data: format!("activeAssetData for `{user}` is unavailable: {e}"),
            },
        },
        _ => return None,
    };
    Some(push)
}

/// The account's perp state on one dex: the served read for the first dex, the
/// empty shape for any other (the sim serves the first dex only).
fn dex_state(
    state: &ApiState,
    user: Address,
    dex: &str,
) -> crate::error::Result<ClearinghouseState> {
    if !dex.is_empty() {
        return Ok(empty_clearinghouse(Sim::served_time(crate::now_ms())));
    }
    Ok(account_states(state, user)?.0)
}

/// The `openOrders` channel's frame: the sim's resting rows in the venue's
/// `{dex, user, orders}` shape (the same rows `frontendOpenOrders` serves).
fn open_orders_push(user: Address, dex: &str, state: &ApiState) -> WsPush {
    if !dex.is_empty() {
        return WsPush::OpenOrders {
            data: OpenOrdersData {
                dex: dex.to_string(),
                user,
                orders: Vec::new(),
            },
        };
    }
    match state.sim.resting_orders(Some(user)) {
        Ok(rows) => WsPush::OpenOrders {
            data: OpenOrdersData {
                dex: String::new(),
                user,
                orders: rows.iter().map(super::info::frontend_row).collect(),
            },
        },
        Err(e) => WsPush::Error {
            data: format!("openOrders for `{user}` is unavailable: {e}"),
        },
    }
}

/// The most a `userFills` or `userFundings` snapshot carries.
const SNAPSHOT_PAGE: usize = 2_000;

/// The `userFills` frames: the account's recent fills as a snapshot on the
/// first push, each new fill after it. The ledger is the sim's own; an
/// address the sim never touched has none.
fn user_fills_push(state: &ApiState, user: Address, cursor: &mut Option<u64>) -> Vec<String> {
    let fills = state.sim.user_fills(user).unwrap_or_default();
    let (fresh, snapshot): (Vec<_>, bool) = match *cursor {
        None => {
            let skip = fills.len().saturating_sub(SNAPSHOT_PAGE);
            (fills.into_iter().skip(skip).collect(), true)
        }
        Some(last) => (fills.into_iter().filter(|f| f.tid > last).collect(), false),
    };
    if !snapshot && fresh.is_empty() {
        return Vec::new();
    }
    if let Some(newest) = fresh.last() {
        *cursor = Some(newest.tid);
    } else {
        // An empty snapshot still counts as pushed.
        *cursor = Some(0);
    }
    let mut data = serde_json::json!({ "user": user, "fills": fresh });
    if snapshot {
        data["isSnapshot"] = serde_json::Value::Bool(true);
    }
    encode(&WsPush::UserFills { data })
}

/// The `userFundings` frames: same contract as the fills, for funding payments
/// (served flat, without the info route's ledger wrapper).
fn user_fundings_push(
    state: &ApiState,
    user: Address,
    cursor: &mut Option<(u64, String)>,
) -> Vec<String> {
    let all = state.sim.user_fundings(user, 0).unwrap_or_default();
    let (fresh, snapshot): (Vec<_>, bool) = match cursor.as_ref() {
        None => {
            let skip = all.len().saturating_sub(SNAPSHOT_PAGE);
            (all.into_iter().skip(skip).collect(), true)
        }
        Some((time, coin)) => (
            all.into_iter()
                .filter(|f| (f.time, f.coin.as_str()) > (*time, coin.as_str()))
                .collect(),
            false,
        ),
    };
    if !snapshot && fresh.is_empty() {
        return Vec::new();
    }
    *cursor = Some(
        fresh
            .last()
            .map_or((0, String::new()), |f| (f.time, f.coin.clone())),
    );
    let mut data = serde_json::json!({ "user": user, "fundings": fresh });
    if snapshot {
        data["isSnapshot"] = serde_json::Value::Bool(true);
    }
    encode(&WsPush::UserFundings { data })
}

/// The `spotState` channel's frame: the same balances `/info` serves, in the
/// WS shape (which carries no `tokenToAvailableAfterMaintenance`).
fn spot_state_push(user: Address, state: &ApiState) -> WsPush {
    match account_states(state, user) {
        Ok((_, spot_state)) => WsPush::SpotState {
            data: SpotStateData {
                user,
                spot_state: SpotState {
                    balances: spot_state.balances,
                },
            },
        },
        Err(e) => WsPush::Error {
            data: format!("spotState for `{user}` is unavailable: {e}"),
        },
    }
}

/// The account's served `webData2` payload: the same clearinghouse read
/// `/info` serves (the observation-time rule included), the spot state, and
/// the market contexts when the meta has landed.
fn web_data2(state: &ApiState, user: Address) -> crate::error::Result<WebData2Data> {
    let (clearinghouse_state, spot_state) = account_states(state, user)?;
    let asset_ctxs = state
        .feed
        .meta_and_ctxs()
        .map(|crate::wire::MetaAndAssetCtxs(_, ctxs)| ctxs);
    Ok(WebData2Data {
        user,
        clearinghouse_state,
        spot_state,
        asset_ctxs,
    })
}

/// The account's served clearinghouse/spot states — the one body both the legacy
/// `webData2` payload and the current `clearinghouseState`/`spotState` channels
/// publish. An untouched address is served the venue's empty shape (the `/info`
/// passthrough's documented contract); the WS lane cannot pass through, so it
/// serves that same shape rather than refusing.
fn account_states(
    state: &ApiState,
    user: Address,
) -> crate::error::Result<(ClearinghouseState, SpotClearinghouseState)> {
    match state.sim.account(user)? {
        Some(account) => {
            let clearinghouse_state =
                super::info::clearinghouse_of(state, user)?.ok_or_else(|| {
                    crate::error::Error::NotFound {
                        what: format!("simulated account `{user}`"),
                    }
                })?;
            let meta = state.feed.meta();
            let spot_state = SpotClearinghouseState {
                balances: crate::sim::account::spot_balances(&account),
                token_to_available_after_maintenance:
                    crate::sim::account::available_after_maintenance(
                        &account,
                        &state.feed,
                        meta.as_ref(),
                    )?,
            };
            Ok((clearinghouse_state, spot_state))
        }
        None => Ok((
            empty_clearinghouse(Sim::served_time(crate::now_ms())),
            SpotClearinghouseState {
                balances: Vec::new(),
                token_to_available_after_maintenance: Vec::new(),
            },
        )),
    }
}

/// The venue's empty `clearinghouseState`: zero summaries, no positions, the
/// served clock. This is the shape the live venue returns for an untouched
/// address — not a fabricated position, and not a refusal.
fn empty_clearinghouse(time: u64) -> ClearinghouseState {
    let zero = MarginSummary {
        account_value: Decimal::ZERO,
        total_ntl_pos: Decimal::ZERO,
        total_raw_usd: Decimal::ZERO,
        total_margin_used: Decimal::ZERO,
    };
    ClearinghouseState {
        margin_summary: zero.clone(),
        cross_margin_summary: zero,
        cross_maintenance_margin_used: Decimal::ZERO,
        withdrawable: Decimal::ZERO,
        asset_positions: Vec::new(),
        time,
        control_owned: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::ws::WsMessage;
    use crate::feed::MarketFeed;
    use crate::sim::{Deal, DealClass};
    use crate::wire::{test_ctx, Level, SpotMeta, SpotToken};
    use rust_decimal::Decimal;
    use std::time::Duration;

    fn api_state(feed: Arc<MarketFeed>) -> Arc<ApiState> {
        let api = crate::feed::rest::RestClient::new(&["http://127.0.0.1:1".to_string()]).unwrap();
        Arc::new(
            ApiState::new(
                feed,
                Arc::new(crate::sim::Sim::memory()),
                api,
                None,
                None,
                Duration::from_secs(45),
                Duration::from_secs(1),
            )
            .unwrap(),
        )
    }

    fn book_sub(coin: &str) -> Subscription {
        Subscription::L2Book {
            coin: coin.into(),
            n_sig_figs: None,
            mantissa: None,
            fast: false,
        }
    }

    /// One two-sided book (the same shape `/info`'s `l2Book` serves).
    fn test_book(coin: &str, px: &str) -> L2Book {
        let bid: Decimal = px.parse().unwrap();
        L2Book {
            coin: coin.into(),
            time: 1,
            levels: [
                vec![Level {
                    px: bid,
                    sz: Decimal::ONE,
                    n: 1,
                }],
                vec![Level {
                    px: bid + Decimal::ONE,
                    sz: Decimal::ONE,
                    n: 1,
                }],
            ],
            spread: None,
        }
    }

    fn fund_perp(state: &ApiState, feed: &MarketFeed, user: Address) {
        state
            .sim
            .deal(
                feed,
                &Deal {
                    address: user,
                    class: DealClass::Perp,
                    coin: None,
                    amount: Decimal::from(1_000),
                },
            )
            .unwrap();
    }

    fn put_usdc(feed: &MarketFeed) {
        feed.put_spot_meta(SpotMeta {
            tokens: vec![SpotToken {
                name: "USDC".into(),
                sz_decimals: 8,
                wei_decimals: 8,
                index: 0,
                is_canonical: true,
                evm_contract: None,
                token_id: None,
                full_name: None,
                deployer_trading_fee_share: Decimal::ZERO,
            }],
            universe: vec![],
        });
    }

    /// Every frame the server emits parses through the *client's own* parser:
    /// the two sides of the venue's websocket vocabulary in one test — a shape
    /// change on either side fails here, not on a live subscriber. The new
    /// channels' payloads are additionally deserialized through the same wire
    /// types `/info` serves.
    #[test]
    #[allow(clippy::too_many_lines)] // one frame-by-frame round-trip table: splitting it would hide the shape mapping
    fn pushes_round_trip_through_the_client_parser() {
        let feed = Arc::new(MarketFeed::memory(vec!["BTC".into()]));
        feed.put_meta(crate::sim::meta_with("BTC", 10), 1);
        feed.put_ctx("BTC".into(), test_ctx("100", "101"), 1);
        feed.put_book(test_book("BTC", "100"), 1_000);
        put_usdc(&feed);
        let state = api_state(feed.clone());
        let user = Address::repeat_byte(0xaa);
        fund_perp(&state, &feed, user);
        state
            .sim
            .deal(
                &feed,
                &Deal {
                    address: user,
                    class: DealClass::Spot,
                    coin: Some("USDC".into()),
                    amount: Decimal::from(25),
                },
            )
            .unwrap();

        let frames = vec![
            push_frame(&Subscription::AllMids, &state, 2_000).unwrap(),
            push_frame(
                &Subscription::ActiveAssetCtx { coin: "BTC".into() },
                &state,
                2_000,
            )
            .unwrap(),
            push_frame(&book_sub("BTC"), &state, 2_000).unwrap(),
            push_frame(&Subscription::WebData2 { user }, &state, 2_000).unwrap(),
            push_frame(
                &Subscription::ClearinghouseState {
                    dex: String::new(),
                    user,
                },
                &state,
                2_000,
            )
            .unwrap(),
            push_frame(
                &Subscription::OpenOrders {
                    dex: String::new(),
                    user,
                },
                &state,
                2_000,
            )
            .unwrap(),
            push_frame(
                &Subscription::SpotState {
                    user,
                    ignore_portfolio_margin: false,
                },
                &state,
                2_000,
            )
            .unwrap(),
            serde_json::to_string(&WsPush::Pong).unwrap(),
            serde_json::to_string(&WsPush::Error { data: "x".into() }).unwrap(),
        ];
        for text in frames {
            let parsed: WsMessage = serde_json::from_str(&text).unwrap();
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            match value["channel"].as_str().unwrap() {
                "allMids" => assert!(matches!(parsed, WsMessage::AllMids { .. })),
                "activeAssetCtx" => {
                    assert!(matches!(parsed, WsMessage::ActiveAssetCtx { .. }));
                }
                "l2Book" => {
                    assert!(
                        !matches!(parsed, WsMessage::Error { .. }),
                        "l2Book parsed as an error: {text}"
                    );
                    let data: L2Book = serde_json::from_value(value["data"].clone()).unwrap();
                    assert_eq!(data.coin, "BTC");
                    assert_eq!(data.time, 1_000, "the live book keeps its update stamp");
                }
                "clearinghouseState" => {
                    let data = &value["data"];
                    assert_eq!(data["user"], serde_json::json!(user));
                    let clearinghouse: ClearinghouseState =
                        serde_json::from_value(data["clearinghouseState"].clone()).unwrap();
                    assert_eq!(
                        clearinghouse.margin_summary.account_value,
                        Decimal::from(1_000)
                    );
                }
                "openOrders" => {
                    assert_eq!(value["data"]["orders"], serde_json::json!([]));
                }
                "spotState" => {
                    let spot: SpotClearinghouseState =
                        serde_json::from_value(value["data"]["spotState"].clone()).unwrap();
                    assert!(spot.balances.iter().any(|b| b.coin == "USDC"));
                }
                "webData2" => {
                    assert!(
                        !matches!(parsed, WsMessage::Error { .. }),
                        "webData2 parsed as an error: {text}"
                    );
                    let data = &value["data"];
                    assert_eq!(data["user"], serde_json::json!(user));
                    let clearinghouse: ClearinghouseState =
                        serde_json::from_value(data["clearinghouseState"].clone()).unwrap();
                    assert_eq!(
                        clearinghouse.margin_summary.account_value,
                        Decimal::from(1_000)
                    );
                    let spot: SpotClearinghouseState =
                        serde_json::from_value(data["spotState"].clone()).unwrap();
                    assert!(spot
                        .balances
                        .iter()
                        .any(|b| b.coin == "USDC" && b.total == Decimal::from(25)));
                    let ctxs: Vec<AssetCtx> =
                        serde_json::from_value(data["assetCtxs"].clone()).unwrap();
                    assert_eq!(ctxs.len(), 1);
                    assert_eq!(ctxs[0].mark_px, Decimal::from(100));
                }
                "pong" => assert!(matches!(parsed, WsMessage::Pong)),
                "error" => assert!(matches!(parsed, WsMessage::Error { .. })),
                other => panic!("unexpected channel {other}"),
            }
        }
    }

    /// The subscription lifecycle: ack, at-most-once, unsubscribe, and the
    /// error frame for what the sim does not serve (never silence).
    #[test]
    #[allow(clippy::too_many_lines)] // one subscription-lifecycle table: splitting it would hide the ack/error ordering
    fn the_ws_acks_unsubscribes_and_answers_unserved_frames_with_an_error() {
        let feed = Arc::new(MarketFeed::memory(vec!["BTC".into()]));
        feed.put_ctx("BTC".into(), test_ctx("100", "101"), 1);
        feed.put_book(test_book("BTC", "100"), 1_000);
        let state = api_state(feed.clone());
        let user = Address::repeat_byte(0xaa);
        fund_perp(&state, &feed, user);
        let mut subs: Vec<Live> = Vec::new();

        let ack = request_reply(
            r#"{"method":"subscribe","subscription":{"type":"allMids"}}"#,
            &mut subs,
            &state,
        );
        assert!(matches!(ack, WsPush::SubscriptionResponse { .. }));
        let repeat = request_reply(
            r#"{"method":"subscribe","subscription":{"type":"allMids"}}"#,
            &mut subs,
            &state,
        );
        assert_eq!(subs.len(), 1, "a duplicate subscribe is not queued twice");
        assert!(
            matches!(&repeat, WsPush::Error { data } if data == r#"Already subscribed: {"type":"allMids"}"#),
            "{repeat:?}"
        );

        let ack = request_reply(
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#,
            &mut subs,
            &state,
        );
        assert!(matches!(ack, WsPush::SubscriptionResponse { .. }));
        assert_eq!(subs.len(), 2, "l2Book is a served channel");
        let error = request_reply(
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"NOPE"}}"#,
            &mut subs,
            &state,
        );
        assert!(
            matches!(error, WsPush::Error { .. }),
            "a bookless coin is refused, never silent"
        );

        let ack = request_reply(
            &format!(
                r#"{{"method":"subscribe","subscription":{{"type":"webData2","user":"{user}"}}}}"#
            ),
            &mut subs,
            &state,
        );
        assert!(matches!(ack, WsPush::SubscriptionResponse { .. }));
        assert_eq!(subs.len(), 3, "webData2 is a served channel");
        let unknown = Address::repeat_byte(0xff);
        let ack = request_reply(
            &format!(
                r#"{{"method":"subscribe","subscription":{{"type":"webData2","user":"{unknown}"}}}}"#
            ),
            &mut subs,
            &state,
        );
        assert!(
            matches!(ack, WsPush::SubscriptionResponse { .. }),
            "an untouched account is served the venue's empty shape, not refused"
        );
        let frame = push_frame(&Subscription::WebData2 { user: unknown }, &state, 2_000).unwrap();
        let value: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(value["channel"], "webData2");
        let clearinghouse: ClearinghouseState =
            serde_json::from_value(value["data"]["clearinghouseState"].clone()).unwrap();
        assert_eq!(clearinghouse.margin_summary.account_value, Decimal::ZERO);
        assert!(clearinghouse.asset_positions.is_empty());

        let error = request_reply(
            r#"{"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"NOPE"}}"#,
            &mut subs,
            &state,
        );
        assert!(
            matches!(error, WsPush::Error { .. }),
            "an unknown coin is refused"
        );

        request_reply(
            r#"{"method":"unsubscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#,
            &mut subs,
            &state,
        );
        assert_eq!(subs.len(), 3);
        // Unsubscribing something that is not live is the venue's own error.
        let error = request_reply(
            r#"{"method":"unsubscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#,
            &mut subs,
            &state,
        );
        assert!(
            matches!(&error, WsPush::Error { data } if data.starts_with("Already unsubscribed: ")),
            "{error:?}"
        );
        request_reply(
            r#"{"method":"unsubscribe","subscription":{"type":"allMids"}}"#,
            &mut subs,
            &state,
        );
        assert_eq!(subs.len(), 2);
        assert!(matches!(
            request_reply(r#"{"method":"ping"}"#, &mut subs, &state),
            WsPush::Pong
        ));
        assert!(matches!(
            request_reply("not json", &mut subs, &state),
            WsPush::Error { .. }
        ));
    }

    /// `l2Book` pushes the composed book: a live book keeps its update stamp,
    /// a scripted book is the scenario's present, and the order lane's
    /// consumption shows in the served size (the same read `/info` serves).
    #[test]
    fn l2_book_push_is_the_composed_book() {
        let feed = Arc::new(MarketFeed::memory(vec!["BTC".into()]));
        feed.put_book(test_book("BTC", "100"), 1_000);
        let state = api_state(feed.clone());
        let sub = book_sub("BTC");
        let live = push_frame(&sub, &state, 5_000).unwrap();
        let value: serde_json::Value = serde_json::from_str(&live).unwrap();
        assert_eq!(value["channel"], "l2Book");
        let book: L2Book = serde_json::from_value(value["data"].clone()).unwrap();
        assert_eq!(book.time, 1_000, "the live book's stamp is its update");
        assert_eq!(book.levels[0][0].px, Decimal::from(100));

        feed.own_book(&test_book("BTC", "99")).unwrap();
        feed.consume(crate::feed::Consumption {
            coin: "BTC".into(),
            side: 0,
            price: Decimal::from(99),
            size: Decimal::new(4, 1),
        })
        .unwrap();
        let scripted = push_frame(&sub, &state, 5_000).unwrap();
        let value: serde_json::Value = serde_json::from_str(&scripted).unwrap();
        let book: L2Book = serde_json::from_value(value["data"].clone()).unwrap();
        assert_eq!(book.time, 5_000, "a scripted book is stamped now");
        assert_eq!(book.levels[0][0].px, Decimal::from(99));
        assert_eq!(
            book.levels[0][0].sz,
            Decimal::new(6, 1),
            "scripted − consumed"
        );

        let missing = push_frame(&book_sub("NOPE"), &state, 5_000).unwrap();
        let value: serde_json::Value = serde_json::from_str(&missing).unwrap();
        assert_eq!(value["channel"], "error");
    }

    /// `webData2` carries the served account state, and omits `assetCtxs`
    /// while the meta is warming instead of inventing an empty market list.
    #[test]
    fn web_data2_omits_asset_ctxs_while_the_meta_is_warming() {
        let feed = Arc::new(MarketFeed::memory(vec!["BTC".into()]));
        let state = api_state(feed.clone());
        let user = Address::repeat_byte(0xaa);
        fund_perp(&state, &feed, user);

        let frame = push_frame(&Subscription::WebData2 { user }, &state, 2_000).unwrap();
        let value: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(value["channel"], "webData2");
        let data = value["data"].as_object().unwrap();
        assert_eq!(data.len(), 3, "only the served account fields are present");
        for key in ["user", "clearinghouseState", "spotState"] {
            assert!(data.contains_key(key), "missing `{key}`: {data:?}");
        }
        assert!(
            data.get("assetCtxs").is_none(),
            "no invented empty ctx list"
        );
        let clearinghouse: ClearinghouseState =
            serde_json::from_value(value["data"]["clearinghouseState"].clone()).unwrap();
        assert_eq!(
            clearinghouse.margin_summary.account_value,
            Decimal::from(1_000)
        );
        let spot: SpotClearinghouseState =
            serde_json::from_value(value["data"]["spotState"].clone()).unwrap();
        assert!(spot.balances.is_empty());
    }

    /// The whole lane end-to-end, in-process: the sim's info router on a
    /// random port and the crate's own `WsClient` subscribing — the live
    /// subscriber path, not just the frame shapes. The client's cache must
    /// fill from the server's pushes (mark from `activeAssetCtx`).
    #[tokio::test]
    async fn ws_client_fills_its_cache_from_the_server() {
        let server_feed = Arc::new(MarketFeed::memory(vec!["BTC".into()]));
        server_feed.put_ctx("BTC".into(), test_ctx("100", "101"), 1);
        let state = api_state(server_feed);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, super::super::info_router(state))
                .await
                .unwrap();
        });

        let client_feed = Arc::new(MarketFeed::memory(vec!["BTC".into()]));
        let ws_pool = diorama_anvil::EndpointPool::new(
            &[format!("ws://{addr}/ws")],
            diorama_anvil::Scheme::Ws,
            diorama_anvil::PoolPolicy::default(),
        )
        .unwrap();
        let client = crate::feed::ws::WsClient::new(client_feed.clone(), ws_pool);
        let task = tokio::spawn(client.run());
        // 20 s: the server pushes on a 1 Hz tick; the budget only has to
        // outlast a loaded parallel test run (the same flake class as the
        // bridge's deadlock guard).
        let filled = 'wait: {
            for _ in 0..200 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if client_feed.mark("BTC") == Some(rust_decimal::Decimal::from(100)) {
                    break 'wait true;
                }
            }
            false
        };
        assert!(
            filled,
            "the client's cache must fill from the server's pushes"
        );
        task.abort();
        server.abort();
    }

    /// The session lane itself (not just the frame builders): a raw subscriber
    /// acks both new channels and receives their pushes, each parsed through
    /// the client's own parser.
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one raw-socket session: the subscribe/ack/push sequence is the test
    async fn the_session_pushes_l2_book_and_web_data2() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message as ClientMessage;

        let feed = Arc::new(MarketFeed::memory(vec!["BTC".into()]));
        feed.put_meta(crate::sim::meta_with("BTC", 10), 1);
        feed.put_ctx("BTC".into(), test_ctx("100", "101"), 1);
        feed.put_book(test_book("BTC", "100"), 1_000);
        let state = api_state(feed.clone());
        let user = Address::repeat_byte(0xaa);
        fund_perp(&state, &feed, user);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, super::super::info_router(state))
                .await
                .unwrap();
        });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
            .await
            .unwrap();
        socket
            .send(ClientMessage::Text(
                r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#.into(),
            ))
            .await
            .unwrap();
        socket
            .send(ClientMessage::Text(
                format!(
                    r#"{{"method":"subscribe","subscription":{{"type":"webData2","user":"{user}"}}}}"#
                )
                .into(),
            ))
            .await
            .unwrap();

        let mut acks = 0;
        let mut book_seen = false;
        let mut web_seen = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while (!book_seen || !web_seen || acks < 2) && tokio::time::Instant::now() < deadline {
            let Some(Ok(ClientMessage::Text(text))) =
                tokio::time::timeout(Duration::from_secs(5), socket.next())
                    .await
                    .ok()
                    .flatten()
            else {
                continue;
            };
            let parsed: WsMessage = serde_json::from_str(&text).unwrap();
            assert!(
                !matches!(parsed, WsMessage::Error { .. }),
                "unexpected error frame: {text}"
            );
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            match value["channel"].as_str().unwrap_or("") {
                "subscriptionResponse" => acks += 1,
                "l2Book" => {
                    let data: L2Book = serde_json::from_value(value["data"].clone()).unwrap();
                    assert_eq!(data.coin, "BTC");
                    book_seen = true;
                }
                "webData2" => {
                    assert_eq!(value["data"]["user"], serde_json::json!(user));
                    web_seen = true;
                }
                _ => {}
            }
        }
        assert!(book_seen, "the session must push l2Book");
        assert!(web_seen, "the session must push webData2");
        assert_eq!(acks, 2, "both subscriptions are acked");
        socket.close(None).await.ok();
        server.abort();
    }
}
