//! The simulator's HTTP surface: the consumer-facing info API (8090) and the
//! control API (8091).
//!
//! Two routers, one state ([`ApiState`]): the info router answers the venue's
//! info types (simulated for touched accounts, passed through to the real
//! venue for untouched ones), the control router drives scenarios. The error
//! mapping keeps the taxonomy's meaning on the wire: control refusals are
//! 4xx, a warming market plane is 503, a venue fault is 502.
pub(crate) mod candles;
pub(crate) mod control;
pub(crate) mod cors;
pub(crate) mod exchange;
pub(crate) mod info;
#[cfg(test)]
mod liquidation_parity;
#[cfg(test)]
mod parity;
pub(crate) mod signing;
mod user_actions;
pub(crate) mod ws;
pub(crate) mod ws_codec;

use crate::error::Result;
use crate::feed::rest::RestClient;
use crate::feed::MarketFeed;
use crate::sim::Sim;
use axum::Router;
use std::sync::Arc;

/// The market channels' default push cadence (about the real venue's).
const DEFAULT_MARKET_PUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Everything both routers read. Built once by the bin.
pub struct ApiState {
    pub feed: Arc<MarketFeed>,
    pub sim: Arc<Sim>,
    /// The venue's public API (passthrough reads).
    pub api: RestClient,
    /// The venue engine's admin base (`/admin/epoch`), for `epoch_new`.
    pub engine_admin: Option<String>,
    /// Plain client for the engine admin call.
    pub http: reqwest::Client,
    /// The bridge's counters (rendered with the sim's metrics), when the
    /// bridge is configured.
    pub bridge: Option<Arc<crate::bridge::metrics::BridgeMetrics>>,
    /// The market websocket lane's shared pool, when the service runs one.
    /// Kept optional so the API can be built without a live plane in tests;
    /// `/metrics` renders it when present.
    pub ws_pool: Option<diorama_anvil::EndpointPool>,
    /// The venue websocket's push cadence (the account automaton's configured
    /// tick, not a second hardcoded second).
    pub push_interval: std::time::Duration,
    /// The market-data channels' push cadence (`l2Book`, `l2`,
    /// `activeAssetCtx`, `allMids`, `fastAssetCtxs`, `pac`): the real venue
    /// pushes these about twice a second, faster than the account automaton's
    /// tick, so they have their own interval.
    pub market_push_interval: std::time::Duration,
    /// The relayed candle streams (the venue serves candles; the sim has no
    /// trade tape to build them from).
    pub candles: Arc<candles::CandleHub>,
}

impl ApiState {
    #[expect(
        clippy::too_many_arguments,
        reason = "the state's inputs are the deployment's own boundaries; grouping them would \
                  hide which caller supplied each"
    )]
    pub fn new(
        feed: Arc<MarketFeed>,
        sim: Arc<Sim>,
        api: RestClient,
        engine_admin: Option<String>,
        bridge: Option<Arc<crate::bridge::metrics::BridgeMetrics>>,
        admin_timeout: std::time::Duration,
        push_interval: std::time::Duration,
    ) -> Result<Self> {
        Ok(Self {
            feed,
            sim,
            api,
            engine_admin,
            // The engine's re-pin forks from a public RPC: the deployment
            // sets this call's own budget.
            http: reqwest::Client::builder().timeout(admin_timeout).build()?,
            bridge,
            ws_pool: None,
            push_interval,
            market_push_interval: push_interval.min(DEFAULT_MARKET_PUSH_INTERVAL),
            candles: Arc::new(candles::CandleHub::default()),
        })
    }

    /// Set the market-data channels' push cadence.
    #[must_use]
    pub fn with_market_push_interval(mut self, interval: std::time::Duration) -> Self {
        self.market_push_interval = interval;
        self
    }

    /// Attach the market websocket lane's pool so `/metrics` can expose its
    /// per-endpoint health. One pool per lane, not a second policy.
    #[must_use]
    pub fn with_ws_pool(mut self, pool: diorama_anvil::EndpointPool) -> Self {
        self.candles.set_pool(pool.clone());
        self.ws_pool = Some(pool);
        self
    }

    /// Readiness: the market plane has the default dex's meta, the dex
    /// registry and the spot token registry, and the default dex's data is
    /// fresher than `max_stale_ms` (the plan's freshness gate: the contexts
    /// of its largest coins by volume). No coin list is configured: every
    /// coin of every dex is a market, and none is singled out. The builder
    /// dexes' own contexts are deliberately not part of it: a slow dex must
    /// not take the pod out of rotation. A coin's book is read on demand and
    /// is not a readiness fact.
    pub fn ready(&self, max_stale_ms: u64) -> bool {
        if self.feed.meta().is_none() || !self.feed.registry_loaded() {
            return false;
        }
        // The spot token registry is part of the served contract (spot deals,
        // `spotClearinghouseState`'s token mapping). The poller fetches it in
        // the same pass as the meta, one HEAVY call later: readiness that
        // passed between the two left the first spot deal hitting the gap
        // (found by the local run).
        if !self.feed.spot_registry_loaded() {
            return false;
        }
        self.feed.default_dex_fresh(crate::now_ms(), max_stale_ms)
    }
}

/// The account plane's poison answer (503): one spelling for the three
/// health routes that raise it.
pub(crate) const SIM_POISONED: &str = "sim state poisoned";

/// The sim-poisoned gate's refusal: the 503 every health surface returns
/// while the account plane is poisoned (the reason is the shared constant).
pub(crate) struct SimPoisoned;

impl axum::response::IntoResponse for SimPoisoned {
    fn into_response(self) -> axum::response::Response {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, SIM_POISONED).into_response()
    }
}

/// The sim-poisoned gate every health surface shares: a poisoned account
/// plane refuses every read, so a 200 would hide an outage from the
/// orchestrator (the verifier's readiness-gap find). `Err` = the refusal.
pub(crate) fn sim_gate(state: &ApiState) -> Result<(), SimPoisoned> {
    if state.sim.is_poisoned() {
        Err(SimPoisoned)
    } else {
        Ok(())
    }
}

/// The info router (8090): the venue's `POST /info`, liveness, readiness.
pub fn info_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route(
            diorama_devnet::handlers::HEALTHZ,
            axum::routing::get(info::healthz),
        )
        // The info port's liveness alias. The shared inter-service health probe
        // (`the HTTP client::health_verdict`) reads a JSON `status`; the control
        // port's text body is for humans and orchestrators.
        .route("/health", axum::routing::get(info::health))
        .route("/readyz", axum::routing::get(info::readyz))
        .route("/info", axum::routing::post(info::info))
        // The matching lane's named refusal: their client posts
        // actions here; the envelope is HL's own err shape, never a 404.
        .route("/exchange", axum::routing::post(exchange::exchange))
        // The venue's websocket lane ("info + WS" on this port).
        .route("/ws", axum::routing::get(ws::ws))
        // Browsers call this port straight from the page (a JSON POST, so a
        // preflight first): the info surface answers any origin, like the
        // venue it mirrors. The control router stays closed to browsers.
        .layer(axum::middleware::from_fn(cors::cors))
        .with_state(state)
}

/// The control router (8091): scenario mutations, the state read, metrics.
pub fn control_router(state: Arc<ApiState>) -> Router {
    use axum::routing::{get, post};
    Router::new()
        .route("/health", get(control::health))
        .route(diorama_devnet::handlers::METRICS, get(control::metrics))
        .route("/control/deal", post(control::deal))
        .route("/control/order", post(control::order))
        .route("/control/advance", post(control::advance))
        .route("/control/crash", post(control::crash))
        .route("/control/rally", post(control::rally))
        .route("/control/stall", post(control::stall))
        .route("/control/set_book", post(control::set_book))
        .route("/control/set_funding", post(control::set_funding))
        .route("/control/set_account", post(control::set_account))
        .route(
            "/control/clear_account/{address}",
            post(control::clear_account),
        )
        .route(
            "/control/clear_fixture/{address}",
            post(control::clear_fixture),
        )
        .route(
            "/control/state/{address}",
            get(control::get_state).post(control::post_state),
        )
        .route("/control/reset", post(control::reset))
        .route("/control/epoch_new", post(control::epoch_new))
        .with_state(state)
}

/// Serve one router on one listener (the bin's two ports): the spine's
/// shared boundary — one bind fault, one listening line, one serve fault.
pub async fn serve(addr: String, router: Router) -> Result<()> {
    diorama_devnet::engine::serve_router("venue sim api", addr, router).await
}

// The HTTP mapping of the error taxonomy lives in the engine's `http` module
// (one impl for both surfaces: the engine's admin routes and the simulator's
// info/control routers).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::test_ctx;
    use std::time::Duration;

    fn state(feed: Arc<MarketFeed>) -> ApiState {
        let api = RestClient::new(&["http://127.0.0.1:1".to_string()]).unwrap();
        ApiState::new(
            feed,
            Arc::new(Sim::memory()),
            api,
            None,
            None,
            Duration::from_secs(45),
            Duration::from_secs(1),
        )
        .unwrap()
    }

    /// Readiness has no market list: it needs the default dex's meta, the dex
    /// registry, the spot registry and fresh default-dex data (its largest
    /// coins, whichever they are). A builder dex that has no data (or a slow
    /// one) never holds the pod out of rotation.
    #[test]
    fn readiness_follows_the_registries_and_the_default_dex_data() {
        let feed = Arc::new(MarketFeed::memory());
        let api = state(feed.clone());
        let max = MarketFeed::MAX_STALE_MS;
        assert!(!api.ready(max), "nothing has landed");

        let now = crate::now_ms();
        let metas = vec![
            crate::sim::meta_of(&[("BTC", 40), ("HYPE", 10)]),
            crate::sim::meta_of(&[("xyz:TSLA", 10)]),
        ];
        let dexs = vec![(String::new(), None), ("xyz".to_string(), None)];
        assert!(feed.put_perp_dexs(dexs, metas, now));
        feed.put_spot_meta(crate::wire::SpotMeta {
            tokens: vec![],
            universe: vec![],
        });
        assert!(!api.ready(max), "no default-dex data yet");

        feed.put_ctx("BTC".into(), test_ctx("100", "100"), now);
        feed.put_ctx("HYPE".into(), test_ctx("10", "10"), now);
        assert!(
            api.ready(max),
            "the builder dex has no data and does not matter"
        );

        feed.put_ctx("HYPE".into(), test_ctx("10", "10"), now - 2 * max);
        assert!(!api.ready(max), "a stale default-dex coin is not ready");
    }

    #[test]
    fn the_market_push_interval_defaults_to_the_venues_cadence() {
        let api = state(Arc::new(MarketFeed::memory()));
        assert_eq!(api.push_interval, Duration::from_secs(1));
        assert_eq!(api.market_push_interval, Duration::from_millis(500));
        let api = api.with_market_push_interval(Duration::from_millis(250));
        assert_eq!(api.market_push_interval, Duration::from_millis(250));
    }
}
