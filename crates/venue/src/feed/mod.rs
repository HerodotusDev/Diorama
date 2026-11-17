//! The market plane: a live cache of the venue's market data with scenario
//! overrides on top.
//!
//! Two layers, one owner each:
//!
//! - [`MarketFeed`] holds the *cache* — the latest `meta`/`metaAndAssetCtxs`/
//!   `l2Book`/`allMids` snapshots from the REST poller and the WS client —
//!   with per-coin update stamps (the staleness metrics);
//! - [`Overrides`] holds the scenario's owned values (`crash` prices,
//!   `set_funding`, `set_book`) *and* the order lane's level consumptions
//!   ([`Consumption`] — the COW analogue: a devnet order owns the
//!   sizes it took, per level, and the served book shows the remainder while
//!   the live feed keeps refreshing every other level), persisted to
//!   `feed.json` so a sim restart mid-scenario does not lose either.
//!
//! Every read goes through the composed view ([`MarketFeed::mark`] and
//! friends): a crash owns both mark and oracle, and `metaAndAssetCtxs` serves
//! the same owned numbers the account plane computes with — the consumer must never
//! read a different mark than the sim used for margin.
pub(crate) mod budget;
pub(crate) mod dex;
pub(crate) mod metrics;
pub(crate) mod relay;
pub(crate) mod rest;
pub(crate) mod ws;

use crate::error::Result;
use crate::wire::{AllMids, AssetCtx, InfoRequest, L2Book, Meta, PerpDex};
use budget::Budget;
use dex::{dex_of_coin, DexRegistry, PerpDexInfo};
use diorama_devnet::store;
use metrics::FeedMetrics;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// The scenario's owned market values (persisted).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct Overrides {
    /// `crash`: owned mark + oracle per coin.
    #[serde(default)]
    pub prices: BTreeMap<String, PriceOverride>,
    /// `set_funding`: owned hourly funding rate per coin.
    #[serde(default)]
    pub funding: BTreeMap<String, Decimal>,
    /// Absolute epoch-millisecond deadlines for the funding overrides. A
    /// missing entry is a legacy override with no deadline; a present entry
    /// past `now` is inert (the rate falls back to the live ctx) until the
    /// next write clears it.
    #[serde(default)]
    pub funding_expires_ms: BTreeMap<String, u64>,
    /// `set_book`: scripted books per coin.
    #[serde(default)]
    pub books: BTreeMap<String, L2Book>,
    /// The order lane's consumed levels (devnet-owned sizes).
    #[serde(default)]
    pub consumptions: Vec<Consumption>,
}

/// One consumed book level (per-level ownership + consumption
/// deltas). Level-scoped: a live update that no longer carries the price
/// prunes it (I6 — pruned, never re-owned); the *mark is never moved* (I3).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct Consumption {
    pub coin: String,
    /// The wire side index: 0 = bids, 1 = asks (the order in `L2Book.levels`).
    pub side: u8,
    pub price: Decimal,
    /// The size the devnet took at this level (summed across orders).
    pub size: Decimal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub(crate) struct PriceOverride {
    pub mark: Decimal,
    pub oracle: Decimal,
}

/// The prices the account plane prices a coin from (the live context's mark
/// and oracle, overrides applied).
pub(crate) struct Prices {
    pub mark: Decimal,
    pub oracle: Decimal,
}

/// The book's staleness for the readiness gate: a scripted book's present is
/// never stale; a coin whose live base has not landed yet is warming (a
/// different fault — absence, not staleness); a live book is fresh within the
/// budget or stale past it, carrying its age.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BookAge {
    Scripted,
    Warming,
    Fresh,
    Stale(u64),
}

/// The live cache (never persisted: it is a cache, and a stale file would
/// masquerade as fresh data).
///
/// Per-dex state is keyed by the dex name (`""` = the default dex); ctxs,
/// books and overrides are keyed by the coin name, which is already unique
/// across dexes (`xyz:TSLA`).
#[derive(Default)]
struct Cache {
    /// The dexes in `perpDexs` order, each with its universe.
    registry: DexRegistry,
    /// Whether `perpDexs` has landed (the registry is the venue's, not just
    /// the default dex a fixture or the first `metaAndAssetCtxs` put there).
    registry_loaded: bool,
    /// The default dex meta's last update.
    meta_at: Option<u64>,
    spot_meta: Option<crate::wire::SpotMeta>,
    ctxs: BTreeMap<String, AssetCtx>,
    ctx_at: BTreeMap<String, u64>,
    books: BTreeMap<String, L2Book>,
    /// The instant of the book's last **update** (what the served `time`
    /// carries).
    book_at: BTreeMap<String, u64>,
    /// The instant the book was last known current: its last update, or the
    /// last moment a live relay stream confirmed it unchanged (the venue
    /// pushes a book on change only, so a quiet coin's book is silent while
    /// being exactly right). The freshness gates read this, not `book_at`.
    book_live_at: BTreeMap<String, u64>,
    /// `allMids` per dex (the default dex's also carries the spot `@N` and
    /// outcome `#N` keys, exactly as the venue's default `allMids` does).
    mids: BTreeMap<String, BTreeMap<String, Decimal>>,
    mids_at: BTreeMap<String, u64>,
    /// `perpsAtOpenInterestCap`'s last answer per dex (the venue's cap list).
    /// An absent dex = the market plane has not answered for it yet: the
    /// order lane refuses rather than treating an unknown cap set as "no
    /// caps".
    oi_caps: BTreeMap<String, Vec<String>>,
}

/// A context pushed by the socket this recently is not overwritten by a REST
/// snapshot (the snapshot may be older than the push by a poll's latency).
const WS_CTX_PRIORITY_MS: u64 = 3_000;

/// USDC's token id on the venue (spot token 0), as the transfer actions write
/// it (`USDC:0x6d1e...`).
const USDC_TOKEN_ID: &str = "0x6d1e7cde53ba9467b783cb7c530ce054";

/// How long [`MarketFeed::ensure_book`] waits for the venue before the caller
/// carries on without the book.
const ENSURE_BOOK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

pub struct MarketFeed {
    cache: RwLock<Cache>,
    overrides: RwLock<Overrides>,
    /// The rate budget meter (shared with the passthrough reads).
    pub(crate) budget: Budget,
    pub(crate) metrics: FeedMetrics,
    /// Which coins the upstream socket carries a book for (demand-driven):
    /// see [`relay`].
    pub(crate) relay: Arc<relay::Relay>,
    /// The overrides' state file; `None` keeps them in memory only.
    path: Option<PathBuf>,
    /// Serializes persists (snapshot + write): a concurrent pair must not let
    /// an older snapshot's rename land last.
    persist_lock: std::sync::Mutex<()>,
    /// The staleness drill: live writes are dropped until this deadline
    /// (0 = not stalled). The scripted plane is never gated.
    stalled_until_ms: AtomicU64,
    /// The venue API client the on-demand book read uses (attached by the
    /// service; absent in fixtures, where [`Self::ensure_book`] only serves
    /// what is cached).
    rest: std::sync::OnceLock<rest::RestClient>,
    /// A monotonic version bumped by every write that can change what a market
    /// channel serves (a live ctx, book or mids write, a scripted override, a
    /// consumed level). A downstream websocket wakes on it, composes its
    /// channels and pushes only what changed. A relay confirmation (the book
    /// is unchanged) does not bump it.
    changes: tokio::sync::watch::Sender<u64>,
}

/// A live context with the scenario's overrides applied: the one composition
/// `metaAndAssetCtxs` and the per-coin lookups share.
fn compose_ctx(
    live: &AssetCtx,
    coin: &str,
    overrides: &Overrides,
    stalled: bool,
    now: u64,
) -> AssetCtx {
    let mut ctx = live.clone();
    // `control_owned` is the PRICE ownership bit: the mark/oracle
    // (`crash`/`rally`), the book (`set_book`) or the whole live plane
    // (`stall`). Funding moves no mark and is reported separately below;
    // conflating the two froze the mirror's WHYPE oracle on a funding-only
    // scenario.
    let price_owned =
        overrides.prices.contains_key(coin) || overrides.books.contains_key(coin) || stalled;
    let funding_owned = overrides.funding.contains_key(coin)
        && overrides
            .funding_expires_ms
            .get(coin)
            .is_none_or(|deadline| *deadline > now);
    if let Some(p) = overrides.prices.get(coin) {
        ctx.mark_px = p.mark;
        ctx.oracle_px = p.oracle;
    }
    if funding_owned {
        if let Some(rate) = overrides.funding.get(coin) {
            ctx.funding = *rate;
        }
    }
    ctx.control_owned = price_owned;
    ctx.funding_owned = funding_owned;
    ctx
}

impl MarketFeed {
    /// Load the persisted overrides beside the live cache.
    pub fn load(path: PathBuf) -> Result<Self> {
        let overrides = store::load::<Overrides>(&path, "feed overrides")?.unwrap_or_default();
        Ok(Self {
            cache: RwLock::new(Cache::default()),
            overrides: RwLock::new(overrides),
            budget: Budget::default(),
            metrics: FeedMetrics::default(),
            relay: Arc::default(),
            path: Some(path),
            persist_lock: std::sync::Mutex::new(()),
            stalled_until_ms: AtomicU64::new(0),
            rest: std::sync::OnceLock::new(),
            changes: tokio::sync::watch::channel(0).0,
        })
    }

    /// Signal that the market content may have changed (see `changes`).
    fn notify(&self) {
        self.changes.send_modify(|version| *version += 1);
    }

    /// The change signal a websocket session waits on.
    pub(crate) fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changes.subscribe()
    }

    /// An in-memory feed (tests only: no production path builds one).
    #[cfg(test)]
    pub fn memory() -> Self {
        Self {
            cache: RwLock::new(Cache::default()),
            overrides: RwLock::new(Overrides::default()),
            budget: Budget::default(),
            metrics: FeedMetrics::default(),
            relay: Arc::default(),
            path: None,
            persist_lock: std::sync::Mutex::new(()),
            stalled_until_ms: AtomicU64::new(0),
            rest: std::sync::OnceLock::new(),
            changes: tokio::sync::watch::channel(0).0,
        }
    }

    /// The staleness window every consumer shares (`readyz`, the order
    /// lane): live data older than this is stale — and stale is not tradable.
    pub const MAX_STALE_MS: u64 = 30_000;

    /// `stall`: hold the live market plane for `seconds` (0 clears) — the
    /// staleness drill. Live writes are dropped while stalled (a real stall
    /// serves nothing new); the control plane's scripted writes are never
    /// gated. Returns the deadline in ms since the epoch (0 = not stalled).
    pub(crate) fn stall(&self, seconds: u64) -> u64 {
        let until = if seconds == 0 {
            0
        } else {
            crate::now_ms().saturating_add(seconds.saturating_mul(1_000))
        };
        self.stalled_until_ms.store(until, Ordering::Relaxed);
        self.notify();
        until
    }

    /// Whether the live plane is currently held (the staleness drill).
    pub(crate) fn stalled(&self) -> bool {
        let until = self.stalled_until_ms.load(Ordering::Relaxed);
        until != 0 && crate::now_ms() < until
    }

    /// The served book's age when it is live-sourced and older than `max_ms`:
    /// `None` for a scripted book (the scenario's present is never stale) and
    /// for a coin whose live base has not landed yet (that read is `Warming`,
    /// a different fault — absence, not staleness).
    pub fn book_age(&self, coin: &str, now_ms: u64, max_ms: u64) -> BookAge {
        if self.read_overrides().books.contains_key(coin) {
            return BookAge::Scripted;
        }
        // The book's own stamp, not the context's: a REST-only book stall
        // leaves `ctx_at` advancing on the WS lane while `l2Book` freezes.
        // Gating on the context is what let a stale book pass readiness.
        // The stamp is the last update or the last relay confirmation.
        match self.read().book_live_at.get(coin).copied() {
            None => BookAge::Warming,
            Some(at) => {
                let age = now_ms.saturating_sub(at);
                if age > max_ms {
                    BookAge::Stale(age)
                } else {
                    BookAge::Fresh
                }
            }
        }
    }

    /// Whether the served book has a scenario-owned base (the fills then walk
    /// the scripted depth plus the live remainder — the COW composition).
    pub fn scripted_book(&self, coin: &str) -> bool {
        self.read_overrides().books.contains_key(coin)
    }

    /// Whether a **price input** the account plane prices this coin from is
    /// scenario-owned: the mark/oracle (`crash`/`rally`), the book
    /// (`set_book`), or the whole live plane (`stall`). A `controlOwned`
    /// account is a simulation, not an observation; the operator's HF fold and
    /// the gateway's draw capacity must refuse it, and the PM advisory must not
    /// be built on it.
    ///
    /// Funding is deliberately excluded: `set_funding` moves the published
    /// rate, not the mark/oracle, so a funding-only scenario is still a real
    /// price observation. Treating it as price ownership froze the mirror's
    /// WHYPE oracle: `oracle-liveness` refuses `controlOwned` contexts, and a
    /// forgotten funding override therefore stopped every re-stamp until the
    /// proof gate expired. Funding ownership is [`Self::funding_is_owned`].
    pub fn market_is_owned(&self, coin: &str) -> bool {
        let o = self.read_overrides();
        o.prices.contains_key(coin) || o.books.contains_key(coin) || self.stalled()
    }

    /// Whether this coin's hourly funding rate is scenario-owned and still
    /// inside its declared deadline. The price inputs are unaffected.
    pub fn funding_is_owned(&self, coin: &str) -> bool {
        let o = self.read_overrides();
        if !o.funding.contains_key(coin) {
            return false;
        }
        o.funding_expires_ms
            .get(coin)
            .is_none_or(|deadline| *deadline > crate::now_ms())
    }

    /// The instant of the coin's last **live** context update. The account
    /// plane stamps its served `time` with the oldest position coin's stamp,
    /// so a stalled feed cannot serve a fresh `time` over stale marks.
    pub fn last_ctx_at(&self, coin: &str) -> Option<u64> {
        self.read().ctx_at.get(coin).copied()
    }

    /// Milliseconds since the last live `allMids` update, or `None` before the
    /// first one. The REST poller uses this to skip a mids read the socket has
    /// already served within the poll interval.
    pub(crate) fn mids_age_ms(&self, now_ms: u64) -> Option<u64> {
        self.read()
            .mids_at
            .get("")
            .map(|at| now_ms.saturating_sub(*at))
    }

    /// Milliseconds since the OLDEST builder dex's mids update (`None` while
    /// any builder dex has none yet, and `None` with no builder dex). The
    /// socket only pushes the default dex's `allMids`, so the builder dexes'
    /// freshness is the REST lane's alone.
    pub(crate) fn builder_mids_age_ms(&self, now_ms: u64) -> Option<u64> {
        let c = self.read();
        let mut oldest = None;
        for dex in c.registry.dexs().iter().filter(|d| !d.name.is_empty()) {
            let at = *c.mids_at.get(&dex.name)?;
            let age = now_ms.saturating_sub(at);
            oldest = Some(oldest.map_or(age, |o: u64| o.max(age)));
        }
        oldest
    }

    // ---- books on demand --------------------------------------------------

    /// Attach the venue API client the on-demand book read uses (the service
    /// does this once at boot; a second call is ignored).
    pub(crate) fn attach_rest(&self, rest: rest::RestClient) {
        let _ = self.rest.set(rest);
    }

    /// Ask for `coin`'s book to be kept live (an order on it, a position or
    /// resting order in it, an `l2Book` read): the upstream socket carries its
    /// `l2Book` stream until the grace window after the last such ask has run
    /// out. `true` when some dex lists the coin; anything else is `false` (an
    /// unlisted coin is never subscribed: the venue closes the socket on one).
    pub(crate) fn want_book(&self, coin: &str) -> bool {
        self.want_book_at(coin, crate::now_ms())
    }

    fn want_book_at(&self, coin: &str, now_ms: u64) -> bool {
        if self.meta_for_coin(coin).is_none() {
            return false;
        }
        self.relay.touch(coin, now_ms);
        true
    }

    /// [`Self::want_book`] for every coin of a held position or resting order
    /// (the tick calls this, so a coin that is held never idles out).
    pub(crate) fn want_books<'a>(&self, coins: impl IntoIterator<Item = &'a str>) {
        let now = crate::now_ms();
        for coin in coins {
            self.want_book_at(coin, now);
        }
    }

    /// Hold `coin` wanted for as long as the lease lives (a downstream
    /// websocket subscription on it). `None` for a coin no dex lists.
    pub(crate) fn lease_coin(&self, coin: &str) -> Option<relay::Lease> {
        self.meta_for_coin(coin)?;
        Some(self.relay.lease(coin, crate::now_ms()))
    }

    /// The coins the poller backfills books for: every wanted coin (the ones
    /// the relay carries a stream for), most important first. A coin whose
    /// grace ran out is dropped here with its cached book.
    pub(crate) fn wanted_coins(&self, now_ms: u64) -> Vec<String> {
        let (wanted, expired) = self.relay.sweep(now_ms);
        for coin in &expired {
            self.evict_book(coin);
        }
        wanted
    }

    /// The coins whose context stream is permanent: every live (not delisted)
    /// coin of every dex. A delisted coin's mark is a settlement price that
    /// barely moves; it is refreshed by the REST sweep, or streamed while it
    /// is wanted.
    pub(crate) fn permanent_ctx_coins(&self) -> Vec<String> {
        let c = self.read();
        c.registry
            .dexs()
            .iter()
            .flat_map(|d| d.meta.universe.iter())
            .filter(|a| !a.is_delisted.unwrap_or(false))
            .map(|a| a.name.clone())
            .collect()
    }

    /// The streams the upstream socket should carry now, and the books whose
    /// grace has run out (evicted from the cache).
    pub(crate) fn desired_streams(&self, now_ms: u64) -> std::collections::BTreeSet<relay::Stream> {
        let wanted = self.wanted_coins(now_ms);
        let permanent = self.permanent_ctx_coins();
        self.relay.desired(&permanent, &wanted, now_ms)
    }

    /// A relay stream is live for these coins' books: the venue pushes a book
    /// on change only, so a quiet book is current as long as its stream is
    /// healthy. The websocket lane calls this while the socket is delivering.
    pub(crate) fn confirm_books<'a>(&self, coins: impl IntoIterator<Item = &'a str>, now_ms: u64) {
        if self.stalled() {
            return;
        }
        let mut c = self.write();
        for coin in coins {
            if c.books.contains_key(coin) {
                c.book_live_at.insert(coin.to_string(), now_ms);
            }
        }
    }

    /// Forget a cached live book (a scripted one is the scenario's and stays).
    fn evict_book(&self, coin: &str) {
        let mut c = self.write();
        c.books.remove(coin);
        c.book_at.remove(coin);
        c.book_live_at.remove(coin);
        drop(c);
        self.notify();
    }

    /// The coin's book, fetching it from the venue once when it is not cached
    /// (or is older than the freshness window), and keeping it wanted from
    /// then on. `None` for a coin no dex lists, a feed with no venue client,
    /// a spent request budget, or a venue that did not answer in time: the
    /// caller carries on as it did without the book.
    pub(crate) async fn ensure_book(&self, coin: &str) -> Option<L2Book> {
        let now = crate::now_ms();
        let known = self.want_book_at(coin, now);
        if matches!(
            self.book_age(coin, now, Self::MAX_STALE_MS),
            BookAge::Scripted | BookAge::Fresh
        ) {
            return self.book(coin, now);
        }
        if !known {
            return None;
        }
        let rest = self.rest.get()?;
        if self.stalled() || !self.budget.try_spend(budget::weight::LIGHT) {
            return self.book(coin, now);
        }
        let req = InfoRequest::L2Book {
            coin: coin.to_string(),
            n_sig_figs: None,
            mantissa: None,
        };
        match tokio::time::timeout(ENSURE_BOOK_TIMEOUT, rest.info::<L2Book>(&req)).await {
            Ok(Ok(book)) if book.coin == coin => {
                let now = crate::now_ms();
                self.put_book(book, now);
                self.book(coin, now)
            }
            Ok(Ok(_)) => None,
            Ok(Err(e)) => {
                self.metrics.inc_rest_errors();
                tracing::warn!(coin, error = %e, "on-demand l2Book read failed");
                None
            }
            Err(_) => {
                self.metrics.inc_rest_errors();
                tracing::warn!(coin, "on-demand l2Book read timed out");
                None
            }
        }
    }

    /// [`Self::ensure_book`] for the synchronous websocket admission: `true`
    /// when the coin's book is cached or on its way, `false` for a coin no dex
    /// lists (or, on a multi-threaded runtime, a read that did not land). On a
    /// multi-threaded runtime the read is awaited (bounded by the read's own
    /// timeout) so the subscription's first frame carries the book; on any
    /// other runtime it runs in the background and the book arrives with a
    /// later push.
    pub(crate) fn warm_book(self: &std::sync::Arc<Self>, coin: &str) -> bool {
        if !self.want_book(coin) {
            return false;
        }
        if self.book(coin, crate::now_ms()).is_some() {
            return true;
        }
        // Without a venue client or a runtime there is nothing to fetch with.
        let (Some(_), Ok(runtime)) = (self.rest.get(), tokio::runtime::Handle::try_current())
        else {
            return false;
        };
        if runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
            return tokio::task::block_in_place(|| runtime.block_on(self.ensure_book(coin)))
                .is_some();
        }
        let feed = self.clone();
        let coin = coin.to_string();
        runtime.spawn(async move {
            let _ = feed.ensure_book(&coin).await;
        });
        true
    }

    // ---- writes (the poller / WS client / control plane) ------------------

    /// The default dex's meta.
    pub(crate) fn put_meta(&self, meta: Meta, now_ms: u64) {
        if self.stalled() {
            tracing::debug!("live meta dropped: the feed is stalled (the staleness drill)");
            return;
        }
        let mut c = self.write();
        c.registry.put_meta("", meta);
        c.meta_at = Some(now_ms);
        drop(c);
        self.notify();
    }

    /// One dex's meta (the default dex is `""`); a dex the registry does not
    /// know yet is appended after the others. The poller rebuilds the whole
    /// registry through [`Self::put_perp_dexs`]; this is the fixtures' door
    /// for a second dex.
    #[cfg(test)]
    pub(crate) fn put_dex_meta(&self, dex: &str, meta: Meta, now_ms: u64) {
        if dex.is_empty() {
            self.put_meta(meta, now_ms);
            return;
        }
        self.write().registry.put_meta(dex, meta);
        self.notify();
    }

    /// The venue's dex list with every dex's meta (`perpDexs` +
    /// `allPerpMetas`, index-aligned): the registry. A misaligned pair keeps
    /// the previous registry and returns `false`.
    pub(crate) fn put_perp_dexs(
        &self,
        dexs: Vec<(String, Option<PerpDex>)>,
        metas: Vec<Meta>,
        now_ms: u64,
    ) -> bool {
        if self.stalled() {
            tracing::debug!("live perpDexs dropped: the feed is stalled (the staleness drill)");
            return false;
        }
        let Some(registry) = DexRegistry::from_api(dexs, metas) else {
            return false;
        };
        let mut c = self.write();
        c.registry = registry;
        c.registry_loaded = true;
        c.meta_at = Some(now_ms);
        drop(c);
        self.notify();
        true
    }

    /// One builder dex's contexts, aligned to its universe by index, and its
    /// fresh meta when the dex is in the registry.
    pub(crate) fn put_dex_ctxs(&self, dex: &str, meta: Meta, ctxs: Vec<AssetCtx>, now_ms: u64) {
        if self.stalled() {
            tracing::debug!(dex = %dex, "live dex ctxs dropped: the feed is stalled (the staleness drill)");
            return;
        }
        if ctxs.len() != meta.universe.len() {
            tracing::warn!(
                dex = %dex,
                universe = meta.universe.len(),
                ctxs = ctxs.len(),
                "dex ctxs are not aligned with the universe; folding the aligned prefix"
            );
        }
        let mut c = self.write();
        for (asset, ctx) in meta.universe.iter().zip(ctxs) {
            // The socket's fresher push wins over this snapshot.
            if Self::ws_ctx_is_newer(&c, &asset.name, now_ms) {
                continue;
            }
            c.ctx_at.insert(asset.name.clone(), now_ms);
            c.ctxs.insert(asset.name.clone(), ctx);
        }
        c.registry.refresh_meta(dex, meta);
        drop(c);
        self.notify();
    }

    pub(crate) fn put_spot_meta(&self, spot: crate::wire::SpotMeta) {
        self.write().spot_meta = Some(spot);
    }

    /// `perpsAtOpenInterestCap`: the coins at their cap (the venue's own
    /// list; the order lane refuses orders that would increase their OI).
    pub(crate) fn put_oi_caps(&self, caps: Vec<String>) {
        self.put_oi_caps_of("", caps);
    }

    /// One dex's cap list (the default dex is `""`).
    pub(crate) fn put_oi_caps_of(&self, dex: &str, caps: Vec<String>) {
        if self.stalled() {
            tracing::debug!("live OI caps dropped: the feed is stalled (the staleness drill)");
            return;
        }
        self.write().oi_caps.insert(dex.to_string(), caps);
    }

    /// The default dex's cached cap list. `None` until the venue has
    /// answered: an unknown cap set is refused by the order lane, never read
    /// as "nothing is capped".
    #[cfg(test)]
    pub fn open_interest_caps(&self) -> Option<Vec<String>> {
        self.open_interest_caps_of("")
    }

    /// One dex's cached cap list (`None` = not answered yet).
    pub fn open_interest_caps_of(&self, dex: &str) -> Option<Vec<String>> {
        self.read().oi_caps.get(dex).cloned()
    }

    /// Whether the coin's context was updated so recently that a REST
    /// snapshot taken now must not overwrite it (the socket's push may be
    /// newer than the snapshot's data).
    fn ws_ctx_is_newer(c: &Cache, coin: &str, now_ms: u64) -> bool {
        c.ctx_at
            .get(coin)
            .is_some_and(|at| now_ms.saturating_sub(*at) < WS_CTX_PRIORITY_MS)
    }

    /// A context from a REST snapshot: the gap reconciliation. A coin the
    /// socket pushed a moment ago keeps the push.
    pub(crate) fn put_ctx_backfill(&self, coin: String, ctx: AssetCtx, now_ms: u64) {
        if self.stalled() {
            tracing::debug!(coin = %coin, "live ctx dropped: the feed is stalled (the staleness drill)");
            return;
        }
        let mut c = self.write();
        if Self::ws_ctx_is_newer(&c, &coin, now_ms) {
            return;
        }
        c.ctx_at.insert(coin.clone(), now_ms);
        let changed = c.ctxs.get(&coin) != Some(&ctx);
        c.ctxs.insert(coin, ctx);
        drop(c);
        if changed {
            self.notify();
        }
    }

    pub(crate) fn put_ctx(&self, coin: String, ctx: AssetCtx, now_ms: u64) {
        if self.stalled() {
            tracing::debug!(coin = %coin, "live ctx dropped: the feed is stalled (the staleness drill)");
            return;
        }
        let mut c = self.write();
        c.ctx_at.insert(coin.clone(), now_ms);
        let changed = c.ctxs.get(&coin) != Some(&ctx);
        c.ctxs.insert(coin, ctx);
        drop(c);
        if changed {
            self.notify();
        }
    }

    pub(crate) fn put_book(&self, book: L2Book, now_ms: u64) {
        if self.stalled() {
            tracing::debug!(coin = %book.coin, "live book dropped: the feed is stalled (the staleness drill)");
            return;
        }
        // I2/I6 are scoped to the *served* base: a live update refreshes
        // (and can prune against) the book only while live is what the coin
        // serves. A scripted book owns the coin until its hand-back — the
        // live feed neither refreshes nor prunes it (measured: an unscoped
        // prune dropped a scripted level's consumption on the next live
        // update, and the level reappeared).
        if !self.read_overrides().books.contains_key(&book.coin) {
            let (skipped, pruned) = self.prune_consumptions(&book);
            if skipped > 0 {
                self.metrics.add_owned_skips(skipped as u64);
            }
            if pruned > 0 {
                self.metrics.add_owned_pruned(pruned as u64);
                self.metrics.set_owned_levels(self.owned_levels());
            }
        }
        let mut c = self.write();
        c.book_at.insert(book.coin.clone(), now_ms);
        c.book_live_at.insert(book.coin.clone(), now_ms);
        let changed = c
            .books
            .get(&book.coin)
            .is_none_or(|old| old.levels != book.levels);
        c.books.insert(book.coin.clone(), book);
        drop(c);
        if changed {
            self.notify();
        }
    }

    /// Prune the coin's consumptions against the base it will serve (a live
    /// update, or a freshly scripted book): records whose price the base
    /// carries stay, the rest are pruned; returns (owned levels the base met,
    /// pruned records).
    fn prune_consumptions(&self, book: &L2Book) -> (usize, usize) {
        let mut o = self.write_overrides();
        let before = o.consumptions.len();
        let mut met = 0usize;
        o.consumptions.retain(|c| {
            if c.coin != book.coin {
                return true;
            }
            // Only 0 and 1 are wire sides. A persisted record outside them is
            // corrupt: it cannot match a level, so it is pruned (fail closed)
            // rather than clamped onto the ask side.
            let Some(levels) = book.levels.get(usize::from(c.side)) else {
                tracing::warn!(coin = %c.coin, side = c.side, "consumption has an invalid side; pruning it");
                return false;
            };
            let live = levels.iter().any(|l| l.px == c.price);
            if live {
                met += 1;
            }
            live
        });
        (met, before - o.consumptions.len())
    }

    /// The default dex's `allMids` (the socket's `allMids` push and the
    /// default REST read).
    pub fn put_mids(&self, mids: BTreeMap<String, Decimal>, now_ms: u64) {
        if self.stalled() {
            tracing::debug!("live mids dropped: the feed is stalled (the staleness drill)");
            return;
        }
        let mut c = self.write();
        let changed = c.mids.get("") != Some(&mids);
        c.mids.insert(String::new(), mids);
        c.mids_at.insert(String::new(), now_ms);
        drop(c);
        if changed {
            self.notify();
        }
    }

    /// `allMids {dex:"ALL_DEXS"}`: every dex's coins in one answer, split by
    /// the coin's dex prefix. A key without a prefix (a default perp, a spot
    /// `@N`, an outcome `#N`) belongs to the default dex, which is how the
    /// venue's own default `allMids` reads. Every registry dex is stamped
    /// (one with no keys serves an empty map, not a stale one).
    pub(crate) fn put_all_dexs_mids(&self, mids: BTreeMap<String, Decimal>, now_ms: u64) {
        if self.stalled() {
            tracing::debug!("live mids dropped: the feed is stalled (the staleness drill)");
            return;
        }
        let mut c = self.write();
        let mut by_dex: BTreeMap<String, BTreeMap<String, Decimal>> = c
            .registry
            .dexs()
            .iter()
            .map(|d| (d.name.clone(), BTreeMap::new()))
            .collect();
        by_dex.entry(String::new()).or_default();
        for (coin, mid) in mids {
            by_dex
                .entry(dex_of_coin(&coin).to_string())
                .or_default()
                .insert(coin, mid);
        }
        let mut changed = false;
        for (dex, mids) in by_dex {
            c.mids_at.insert(dex.clone(), now_ms);
            changed |= c.mids.get(&dex) != Some(&mids);
            c.mids.insert(dex, mids);
        }
        drop(c);
        if changed {
            self.notify();
        }
    }

    /// `crash`: own this coin's mark and oracle (live updates stop mattering
    /// until [`Self::clear_price`]).
    pub(crate) fn own_price(&self, coin: &str, price: PriceOverride) -> Result<()> {
        self.write_overrides()
            .prices
            .insert(coin.to_string(), price);
        self.notify();
        self.persist()
    }

    pub fn clear_price(&self, coin: &str) -> Result<()> {
        self.write_overrides().prices.remove(coin);
        self.notify();
        self.persist()
    }

    pub fn own_funding(&self, coin: &str, rate: Decimal, expires_ms: Option<u64>) -> Result<()> {
        {
            let mut o = self.write_overrides();
            o.funding.insert(coin.to_string(), rate);
            match expires_ms {
                Some(deadline) => {
                    o.funding_expires_ms.insert(coin.to_string(), deadline);
                }
                None => {
                    o.funding_expires_ms.remove(coin);
                }
            }
        }
        self.notify();
        self.persist()
    }

    pub fn clear_funding(&self, coin: &str) -> Result<()> {
        {
            let mut o = self.write_overrides();
            o.funding.remove(coin);
            o.funding_expires_ms.remove(coin);
        }
        self.notify();
        self.persist()
    }

    /// Record one consumed level (the order lane's ownership write): sizes at
    /// the same (coin, side, price) accumulate, exactly as a second order
    /// would take more of the same level.
    pub(crate) fn consume(&self, c: Consumption) -> Result<()> {
        {
            let mut o = self.write_overrides();
            match o
                .consumptions
                .iter_mut()
                .find(|x| x.coin == c.coin && x.side == c.side && x.price == c.price)
            {
                Some(x) => {
                    x.size = x
                        .size
                        .checked_add(c.size)
                        .ok_or(crate::error::Error::Overflow {
                            what: "consumed level size",
                        })?;
                }
                None => o.consumptions.push(c),
            }
        }
        self.metrics.set_owned_levels(self.owned_levels());
        self.notify();
        self.persist()
    }

    /// The devnet-owned book level count (the `venue_sim_owned_levels` gauge).
    pub fn owned_levels(&self) -> u64 {
        self.read_overrides().consumptions.len() as u64
    }

    pub(crate) fn own_book(&self, book: &L2Book) -> Result<()> {
        {
            let mut o = self.write_overrides();
            o.books.insert(book.coin.clone(), book.clone());
        }
        // A new scripted base re-scopes the coin's consumptions: the deltas
        // for prices it still carries stay, the rest are pruned (I6 — pruned,
        // never re-owned).
        let (_, pruned) = self.prune_consumptions(book);
        if pruned > 0 {
            self.metrics.add_owned_pruned(pruned as u64);
        }
        self.metrics.set_owned_levels(self.owned_levels());
        self.notify();
        self.persist()
    }

    pub fn clear_book(&self, coin: &str) -> Result<()> {
        {
            let mut o = self.write_overrides();
            o.books.remove(coin);
            // I4: the hand-back restores the live book — the coin's consumed
            // levels are part of what the devnet owned, so they drop with it.
            o.consumptions.retain(|c| c.coin != coin);
        }
        self.metrics.set_owned_levels(self.owned_levels());
        self.notify();
        self.persist()
    }

    /// `reset`: drop every override (the live cache stays — it is the venue's,
    /// not the scenario's).
    pub fn clear_overrides(&self) -> Result<()> {
        *self.write_overrides() = Overrides::default();
        self.metrics.set_owned_levels(0);
        self.notify();
        self.persist()
    }

    /// The number of scenario-owned feed values (`crash` prices + `set_funding`
    /// rates + `set_book` books) — the `venue_feed_overrides` gauge, computed
    /// from the overrides themselves so it cannot drift.
    pub fn owned_count(&self) -> u64 {
        let o = self.read_overrides();
        let now = crate::now_ms();
        let active_funding = o
            .funding
            .keys()
            .filter(|coin| {
                o.funding_expires_ms
                    .get(*coin)
                    .is_none_or(|deadline| *deadline > now)
            })
            .count();
        (o.prices.len() + active_funding + o.books.len()) as u64
    }

    // ---- composed reads ---------------------------------------------------

    /// The coin's mark: the owned price wins, else the live context.
    pub fn mark(&self, coin: &str) -> Option<Decimal> {
        if let Some(p) = self.read_overrides().prices.get(coin) {
            return Some(p.mark);
        }
        self.read().ctxs.get(coin).map(|c| c.mark_px)
    }

    /// The coin's mark, or `fallback` when no mark has landed (the caller's
    /// standing price): the order lane's one read.
    pub fn mark_or(&self, coin: &str, fallback: Decimal) -> Decimal {
        self.mark(coin).unwrap_or(fallback)
    }

    /// The coin's oracle price (funding and liquidations price from it).
    pub fn oracle(&self, coin: &str) -> Option<Decimal> {
        if let Some(p) = self.read_overrides().prices.get(coin) {
            return Some(p.oracle);
        }
        self.read().ctxs.get(coin).map(|c| c.oracle_px)
    }

    /// The coin's published hourly funding rate. A live, non-expired
    /// `set_funding` override wins; an expired one is inert and the live ctx
    /// answers (the override row stays until the next control write clears it).
    pub fn funding(&self, coin: &str) -> Option<Decimal> {
        if self.funding_is_owned(coin) {
            if let Some(rate) = self.read_overrides().funding.get(coin) {
                return Some(*rate);
            }
        }
        self.read().ctxs.get(coin).map(|c| c.funding)
    }

    /// The coin's book: the scripted book wins; a scripted book is stamped
    /// `now` at serve time (it is the scenario's present, not a snapshot).
    pub(crate) fn book(&self, coin: &str, now_ms: u64) -> Option<L2Book> {
        let overrides = self.read_overrides();
        let base = if let Some(b) = overrides.books.get(coin) {
            let mut b = b.clone();
            b.time = now_ms;
            b
        } else {
            let c = self.read();
            let mut b = c.books.get(coin)?.clone();
            // The live book's served instant is its **last update**, not
            // the read's clock. A stalled book that stamped `now` on every
            // read looked fresh to every consumer's age gate; the indexer's
            // `venue_observed_at` can only bound what the venue honestly
            // stamps.
            if let Some(at) = c.book_at.get(coin) {
                b.time = *at;
            }
            b
        };
        Some(apply_consumptions(base, &overrides.consumptions))
    }

    /// The DEFAULT dex's perp universe + margin tables (the live meta; `None`
    /// until the first poll lands, a read that needs it is a 503, never a
    /// guess). Other dexes: [`Self::meta_of`], [`Self::meta_for_coin`].
    pub(crate) fn meta(&self) -> Option<Meta> {
        self.meta_of("")
    }

    /// One dex's meta (`""` = the default dex; `None` = the dex is unknown or
    /// has not landed).
    pub(crate) fn meta_of(&self, dex: &str) -> Option<Meta> {
        self.read().registry.dex(dex).map(|d| d.meta.clone())
    }

    /// Every dex's meta in `perpDexs` order (what `allPerpMetas` serves).
    #[allow(dead_code)] // the REST info handlers adopt it with the multi-dex serving phase
    pub(crate) fn metas(&self) -> Vec<Meta> {
        self.read()
            .registry
            .dexs()
            .iter()
            .map(|d| d.meta.clone())
            .collect()
    }

    /// The meta of the dex that lists `coin` (`None` = no dex lists it).
    pub(crate) fn meta_for_coin(&self, coin: &str) -> Option<Meta> {
        self.read().registry.meta_for_coin(coin).cloned()
    }

    /// The meta a per-coin lookup (margin tiers, leverage bounds, tick/lot,
    /// delisting) resolves `coin` against: its dex's meta whether or not the
    /// dex lists the coin (so an unknown coin of a known dex reads as
    /// not-found, never as a warming plane), the default dex's meta when the
    /// coin's dex is not in the registry (an unknown prefix is not-found
    /// there too). `None` only while no meta has landed.
    pub(crate) fn meta_for_lookup(&self, coin: &str) -> Option<Meta> {
        let c = self.read();
        let registry = &c.registry;
        registry
            .dex(dex_of_coin(coin))
            .or_else(|| registry.dex(""))
            .map(|d| d.meta.clone())
    }

    /// The collateral token a dex margins against (`0` = USDC). A dex the
    /// registry does not know (a seeded position on a dex that has not landed
    /// yet) reads as USDC: the same reading the default dex has.
    pub(crate) fn collateral_token_of(&self, dex: &str) -> u32 {
        self.read()
            .registry
            .dex(dex)
            .map_or(0, |d| d.collateral_token)
    }

    /// Every dex name with its collateral token, `perpDexs` order (cheap: no
    /// universes are copied).
    pub(crate) fn dex_collateral_tokens(&self) -> Vec<(String, u32)> {
        self.read()
            .registry
            .dexs()
            .iter()
            .map(|d| (d.name.clone(), d.collateral_token))
            .collect()
    }

    /// Every dex with its coin names in universe order, `perpDexs` order (the
    /// push lanes' per-tick read: only the names are copied).
    pub(crate) fn dex_coins(&self) -> Vec<(String, Vec<String>)> {
        self.read()
            .registry
            .dexs()
            .iter()
            .map(|d| {
                (
                    d.name.clone(),
                    d.meta.universe.iter().map(|a| a.name.clone()).collect(),
                )
            })
            .collect()
    }

    /// Whether the registry lists the dex (`""` is the default dex): the
    /// names an info request or a subscription may select.
    pub(crate) fn has_dex(&self, name: &str) -> bool {
        self.read().registry.dex(name).is_some()
    }

    /// The name of spot token `index` (the venue's spot meta). Token 0 is
    /// `USDC` even before the registry lands; any other index needs the
    /// registry (`None` = unknown).
    pub(crate) fn token_name(&self, index: u32) -> Option<String> {
        let c = self.read();
        let named = c
            .spot_meta
            .as_ref()
            .and_then(|s| s.tokens.iter().find(|t| t.index == index))
            .map(|t| t.name.clone());
        named.or_else(|| (index == 0).then(|| "USDC".to_string()))
    }

    /// The token a fill on `coin` pays its fee in: the dex's collateral token
    /// name (`USDC`, `USDH`, ...). A token the registry cannot name falls back
    /// to its index, never to a USDC that would be wrong for the dex.
    pub(crate) fn fee_token_of_coin(&self, coin: &str) -> String {
        let token = self.collateral_token_of(dex_of_coin(coin));
        self.token_name(token).unwrap_or_else(|| token.to_string())
    }

    /// Whether `token` is the collateral token of some perp dex (USDC, the
    /// default dex's, always counts): a balance of it backs margin, so a
    /// transfer out of the spot wallet is bounded by the margin its positions
    /// hold.
    pub(crate) fn is_collateral_token(&self, token: u32) -> bool {
        token == 0
            || self
                .read()
                .registry
                .dexs()
                .iter()
                .any(|d| d.collateral_token == token)
    }

    /// The name of the dex at `perpDexs` position `index` (`""` for the
    /// default dex): how CoreWriter's `sendAsset` names a perp dex. `None` for
    /// a position the registry does not list.
    pub(crate) fn dex_name_at(&self, index: u32) -> Option<String> {
        self.read()
            .registry
            .dexs()
            .get(usize::try_from(index).ok()?)
            .map(|d| d.name.clone())
    }

    /// A spot token named the way the `/exchange` transfers name it: `NAME`
    /// or `NAME:tokenId` (the venue's token string). The id, when given, must
    /// be the registry's for the name. `None` for a token the registry does
    /// not know. USDC resolves before the registry lands.
    pub(crate) fn resolve_token(&self, spec: &str) -> Option<(u32, String)> {
        let (name, id) = match spec.split_once(':') {
            Some((name, id)) => (name, Some(id)),
            None => (spec, None),
        };
        let c = self.read();
        let found = c
            .spot_meta
            .as_ref()
            .and_then(|s| s.tokens.iter().find(|t| t.name == name));
        match found {
            Some(t) => match (id, &t.token_id) {
                (Some(want), Some(have)) if !want.eq_ignore_ascii_case(have) => None,
                _ => Some((t.index, t.name.clone())),
            },
            None => (name == "USDC" && id.is_none_or(|id| id.eq_ignore_ascii_case(USDC_TOKEN_ID)))
                .then(|| (0, "USDC".to_string())),
        }
    }

    /// Every delisted coin of every dex (`isDelisted` in its dex's meta): the
    /// delisting settlement's work list. Scans under the lock, copying only
    /// the names (the registry holds every universe).
    pub(crate) fn delisted_coins(&self) -> Vec<String> {
        let c = self.read();
        c.registry
            .dexs()
            .iter()
            .flat_map(|d| d.meta.universe.iter())
            .filter(|a| a.is_delisted.unwrap_or(false))
            .map(|a| a.name.clone())
            .collect()
    }

    /// Every coin the sim may hold a mark for: each dex's universe plus any
    /// coin a context has landed for (a fixture feed has contexts and no
    /// registry).
    pub(crate) fn all_coins(&self) -> Vec<String> {
        let c = self.read();
        let mut coins: Vec<String> = c.ctxs.keys().cloned().collect();
        for dex in c.registry.dexs() {
            coins.extend(dex.meta.universe.iter().map(|a| a.name.clone()));
        }
        drop(c);
        coins.sort_unstable();
        coins.dedup();
        coins
    }

    /// Every dex (`perpDexs` order): name, index, collateral token, asset-id
    /// base, the raw `perpDexs` record and the universe.
    pub(crate) fn all_dexs(&self) -> Vec<PerpDexInfo> {
        self.read().registry.dexs().to_vec()
    }

    /// Whether `perpDexs` has landed: the readiness gate's registry fact.
    pub fn registry_loaded(&self) -> bool {
        self.read().registry_loaded
    }

    /// The coin's asset id (its dex's base plus its index in that dex's
    /// universe; `None` = no dex lists the coin).
    #[allow(dead_code)] // the activeAssetData / marginTable serving adopts it with the multi-dex serving phase
    pub(crate) fn asset_id(&self, coin: &str) -> Option<u32> {
        self.read().registry.asset_id(coin)
    }

    /// The one asset-id resolver (see [`DexRegistry::coin_of_asset`]): the
    /// default dex's plain index or `100000 + dex * 10000 + index` for a
    /// builder dex; `None` = unknown id.
    pub(crate) fn coin_of_asset(&self, asset: u32) -> Option<String> {
        self.read().registry.coin_of_asset(asset)
    }

    /// The spot token registry (`None` until the first poll lands).
    pub(crate) fn spot_meta(&self) -> Option<crate::wire::SpotMeta> {
        self.read().spot_meta.clone()
    }

    /// The spot token index for a coin name (`None` = not tracked).
    pub fn token_of(&self, coin: &str) -> Option<u32> {
        self.read()
            .spot_meta
            .as_ref()?
            .tokens
            .iter()
            .find(|t| t.name == coin)
            .map(|t| t.index)
    }

    /// The spot token registry's presence: the readiness gate's third fact
    /// and the spot surface's precondition (spot deals and
    /// `spotClearinghouseState`'s token mapping refuse without it).
    pub fn spot_registry_loaded(&self) -> bool {
        self.read().spot_meta.is_some()
    }

    /// `metaAndAssetCtxs` with the overrides applied: the served contexts are
    /// exactly what the account plane prices from.
    #[cfg(test)]
    pub(crate) fn meta_and_ctxs(&self) -> Option<crate::wire::MetaAndAssetCtxs> {
        self.meta_and_ctxs_of("")
    }

    /// [`Self::meta_and_ctxs`] for one dex (`""` = the default dex).
    pub(crate) fn meta_and_ctxs_of(&self, dex: &str) -> Option<crate::wire::MetaAndAssetCtxs> {
        let meta = self.meta_of(dex)?;
        let overrides = self.read_overrides().clone();
        let stalled = self.stalled();
        let now = crate::now_ms();
        let c = self.read();
        let ctxs = meta
            .universe
            .iter()
            .filter_map(|asset| {
                let ctx = c.ctxs.get(&asset.name)?;
                Some(compose_ctx(ctx, &asset.name, &overrides, stalled, now))
            })
            .collect();
        Some(crate::wire::MetaAndAssetCtxs(meta, ctxs))
    }

    /// One coin's context with the overrides applied (the row
    /// `metaAndAssetCtxs` serves for it), looked up directly by the coin's
    /// name (builder coins included); `None` while no live context has landed
    /// for the coin.
    pub(crate) fn ctx(&self, coin: &str) -> Option<AssetCtx> {
        let stalled = self.stalled();
        let now = crate::now_ms();
        let overrides = self.read_overrides();
        let c = self.read();
        let ctx = c.ctxs.get(coin)?;
        Some(compose_ctx(ctx, coin, &overrides, stalled, now))
    }

    /// `allMids` with the overrides applied.
    pub(crate) fn all_mids(&self) -> AllMids {
        self.all_mids_of("")
    }

    /// `allMids` for one dex (`""` = the default dex): the dex's own keys,
    /// with an owned price winning for the coins of that dex.
    pub(crate) fn all_mids_of(&self, dex: &str) -> AllMids {
        let overrides = self.read_overrides().clone();
        let c = self.read();
        let mut mids = c.mids.get(dex).cloned().unwrap_or_default();
        // The context stream carries each coin's mid (`midPx`) about once a
        // second, far fresher than the `allMids` snapshot for a builder dex
        // (the venue's socket never pushes those). A context newer than the
        // dex's mids snapshot wins for its coin.
        let snapshot_at = c.mids_at.get(dex).copied().unwrap_or(0);
        if let Some(info) = c.registry.dex(dex) {
            for asset in &info.meta.universe {
                let fresher = c
                    .ctx_at
                    .get(&asset.name)
                    .is_some_and(|at| *at > snapshot_at);
                if let (true, Some(mid)) = (fresher, c.ctxs.get(&asset.name).and_then(|x| x.mid_px))
                {
                    mids.insert(asset.name.clone(), mid);
                }
            }
        }
        for (coin, p) in &overrides.prices {
            if dex_of_coin(coin) == dex {
                mids.insert(coin.clone(), p.mark);
            }
        }
        AllMids(mids)
    }

    /// The prices the account plane prices a coin from (mark, oracle). A coin
    /// with no live context and no override is a fault at the read that needs
    /// it, never a zero (a zero mark would liquidate every position).
    pub fn required_prices(&self, coin: &str) -> Option<Prices> {
        Some(Prices {
            mark: self.mark(coin)?,
            oracle: self.oracle(coin)?,
        })
    }

    /// Milliseconds since the coin's last live context update (`None` = never
    /// seen). The readiness gate and the staleness metrics read this.
    pub fn stale_ms(&self, coin: &str, now_ms: u64) -> Option<u64> {
        let c = self.read();
        c.ctx_at.get(coin).map(|at| now_ms.saturating_sub(*at))
    }

    /// Publish the staleness gauges + budget use (the poller calls this per
    /// pass; the metrics endpoint renders the rest). One staleness series per
    /// live coin that has a context.
    pub fn publish_metrics(&self, now_ms: u64) {
        self.metrics.set_budget_used(self.budget.used());
        let stale: Vec<(String, u64)> = {
            let c = self.read();
            c.registry
                .dexs()
                .iter()
                .flat_map(|d| d.meta.universe.iter())
                .filter(|a| !a.is_delisted.unwrap_or(false))
                .filter_map(|a| {
                    let at = c.ctx_at.get(&a.name)?;
                    Some((a.name.clone(), now_ms.saturating_sub(*at)))
                })
                .collect()
        };
        self.metrics.set_stale_all(stale);
    }

    /// Whether the socket keeps `dex`'s contexts fresh: every live (not
    /// delisted) coin of the dex has one younger than `max_ms`. A dex with no
    /// live coin, or one not in the registry, is not carried.
    pub(crate) fn dex_ctxs_fresh(&self, dex: &str, now_ms: u64, max_ms: u64) -> bool {
        let c = self.read();
        let Some(info) = c.registry.dex(dex) else {
            return false;
        };
        let mut any = false;
        for asset in &info.meta.universe {
            // A delisted coin streams nothing, so its (static) context only
            // ever comes from the REST read: until one landed, the dex is not
            // carried (`metaAndAssetCtxs` serves the whole universe, so a
            // dex missing a delisted coin's row would stay unserved forever).
            if asset.is_delisted.unwrap_or(false) {
                if !c.ctxs.contains_key(&asset.name) {
                    return false;
                }
                continue;
            }
            any = true;
            if !c
                .ctx_at
                .get(&asset.name)
                .is_some_and(|at| now_ms.saturating_sub(*at) <= max_ms)
            {
                return false;
            }
        }
        any
    }

    /// The coin-count fact of the heartbeat line: live coins with a context
    /// fresher than `max_ms`, and live coins in all.
    pub(crate) fn ctx_freshness(&self, now_ms: u64, max_ms: u64) -> (usize, usize) {
        let c = self.read();
        let mut fresh = 0usize;
        let mut total = 0usize;
        for asset in c
            .registry
            .dexs()
            .iter()
            .flat_map(|d| d.meta.universe.iter())
            .filter(|a| !a.is_delisted.unwrap_or(false))
        {
            total += 1;
            if c.ctx_at
                .get(&asset.name)
                .is_some_and(|at| now_ms.saturating_sub(*at) <= max_ms)
            {
                fresh += 1;
            }
        }
        (fresh, total)
    }

    /// Whether the default dex's data is fresh: the contexts of its largest
    /// coins (by `dayNtlVlm`) are all newer than `max_ms`. A fixture or boot
    /// state with no contexts is not fresh. Built for readiness: it does not
    /// depend on any one coin, and it never looks at a builder dex.
    pub(crate) fn default_dex_fresh(&self, now_ms: u64, max_ms: u64) -> bool {
        const LARGEST: usize = 3;
        let c = self.read();
        let Some(info) = c.registry.dex("") else {
            return false;
        };
        let mut by_volume: Vec<(&str, Decimal)> = info
            .meta
            .universe
            .iter()
            .filter(|a| !a.is_delisted.unwrap_or(false))
            .filter_map(|a| {
                c.ctxs
                    .get(&a.name)
                    .map(|ctx| (a.name.as_str(), ctx.day_ntl_vlm))
            })
            .collect();
        if by_volume.is_empty() {
            return false;
        }
        by_volume.sort_by_key(|a| std::cmp::Reverse(a.1));
        by_volume.iter().take(LARGEST).all(|(coin, _)| {
            c.ctx_at
                .get(*coin)
                .is_some_and(|at| now_ms.saturating_sub(*at) <= max_ms)
        })
    }

    fn persist(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        // The snapshot and the write under one lock: concurrent persists must
        // not let an older snapshot's rename land last (a restart would then
        // reload the staler overrides).
        let _serial = self
            .persist_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let overrides = self.read_overrides().clone();
        store::save(path, &overrides, "feed overrides")?;
        Ok(())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Cache> {
        self.cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Cache> {
        self.cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_overrides(&self) -> std::sync::RwLockReadGuard<'_, Overrides> {
        self.overrides
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_overrides(&self) -> std::sync::RwLockWriteGuard<'_, Overrides> {
        self.overrides
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The served book: the base book minus the consumed sizes — level-scoped, so
/// a level the devnet never touched serves the live size verbatim. A fully
/// consumed level leaves the book (the wire never carries a zero size); the
/// order count `n` stays the live level's (a consumption takes size, not
/// orders).
fn apply_consumptions(mut book: L2Book, consumptions: &[Consumption]) -> L2Book {
    for side in 0..2usize {
        for level in book.levels[side].iter_mut() {
            let taken: Decimal = consumptions
                .iter()
                .filter(|c| {
                    c.coin == book.coin && usize::from(c.side) == side && c.price == level.px
                })
                .map(|c| c.size)
                .sum();
            if taken > Decimal::ZERO {
                level.sz = (level.sz - taken).max(Decimal::ZERO);
            }
        }
        book.levels[side].retain(|l| l.sz > Decimal::ZERO);
    }
    book
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::wire::{test_ctx, Level};

    /// A scripted book owns the coin until its hand-back: live updates are
    /// not the served base, so they neither refresh nor prune its consumed
    /// levels (the measured bug: an unscoped prune dropped a scripted level's
    /// consumption on the next live update and the level reappeared).
    /// The standing-price read: a mark when one has landed, the caller's
    /// fallback otherwise (the order lane's rule, named once).
    #[test]
    fn mark_or_answers_the_mark_then_the_fallback() {
        let feed = MarketFeed::memory();
        assert_eq!(feed.mark_or("BTC", Decimal::from(42)), Decimal::from(42));
        feed.own_price(
            "BTC",
            PriceOverride {
                mark: Decimal::from(7),
                oracle: Decimal::from(7),
            },
        )
        .unwrap();
        assert_eq!(feed.mark_or("BTC", Decimal::from(42)), Decimal::from(7));
    }

    #[test]
    fn a_consumption_with_an_invalid_side_is_pruned_not_clamped() {
        let feed = MarketFeed::memory();
        let book = L2Book {
            coin: "BTC".into(),
            time: 1,
            levels: [
                vec![Level {
                    px: Decimal::from(100),
                    sz: Decimal::ONE,
                    n: 1,
                }],
                vec![],
            ],
            spread: None,
        };
        feed.consume(Consumption {
            coin: "BTC".into(),
            side: 2,
            price: Decimal::from(100),
            size: Decimal::ONE,
        })
        .unwrap();
        assert_eq!(feed.owned_levels(), 1);
        // The live book carries the price on the bid side only; an invalid
        // side must not be clamped onto the ask side and kept.
        feed.put_book(book, 2_000);
        assert_eq!(feed.owned_levels(), 0, "the corrupt record was pruned");
    }

    #[test]
    fn live_updates_leave_a_scripted_books_consumptions_alone() {
        let feed = MarketFeed::memory();
        let scripted = |px: &str| L2Book {
            coin: "BTC".into(),
            time: 1,
            levels: [
                vec![],
                vec![Level {
                    px: px.parse().unwrap(),
                    sz: Decimal::ONE,
                    n: 1,
                }],
            ],
            spread: None,
        };
        feed.own_book(&scripted("100")).unwrap();
        feed.consume(Consumption {
            coin: "BTC".into(),
            side: 1,
            price: "100".parse().unwrap(),
            size: "0.5".parse().unwrap(),
        })
        .unwrap();
        // A live update that cannot carry the scripted price: no prune.
        feed.put_book(scripted("50"), 2);
        assert_eq!(feed.owned_levels(), 1, "the scripted consumption survives");
        assert_eq!(
            feed.book("BTC", 0).unwrap().levels[1][0].sz,
            "0.5".parse().unwrap(),
            "served = scripted − consumed"
        );
        // Re-scripting the book without the price re-scopes: pruned.
        feed.own_book(&scripted("50")).unwrap();
        assert_eq!(feed.owned_levels(), 0);
        assert_eq!(
            feed.book("BTC", 0).unwrap().levels[1][0].sz,
            "1".parse().unwrap()
        );
    }

    #[test]
    fn overrides_compose_over_the_cache() {
        let feed = MarketFeed::memory();
        feed.put_ctx("BTC".into(), test_ctx("100", "101"), 1_000);
        assert_eq!(feed.mark("BTC"), Some(Decimal::from(100)));
        assert_eq!(feed.oracle("BTC"), Some(Decimal::from(101)));

        feed.own_price(
            "BTC",
            PriceOverride {
                mark: "90".parse().unwrap(),
                oracle: "91".parse().unwrap(),
            },
        )
        .unwrap();
        assert_eq!(feed.mark("BTC"), Some(Decimal::from(90)));
        assert_eq!(feed.oracle("BTC"), Some(Decimal::from(91)));
        // A live update no longer changes the owned read.
        feed.put_ctx("BTC".into(), test_ctx("200", "201"), 2_000);
        assert_eq!(feed.mark("BTC"), Some(Decimal::from(90)));

        feed.clear_price("BTC").unwrap();
        assert_eq!(feed.mark("BTC"), Some(Decimal::from(200)));
    }

    #[test]
    fn scripted_book_wins_and_is_stamped_now() {
        let feed = MarketFeed::memory();
        let book = |px: &str| L2Book {
            coin: "BTC".into(),
            time: 1,
            levels: [
                vec![Level {
                    px: px.parse().unwrap(),
                    sz: Decimal::ONE,
                    n: 1,
                }],
                vec![],
            ],
            spread: None,
        };
        feed.put_book(book("100"), 1_000);
        assert_eq!(
            feed.book("BTC", 5_000).unwrap().time,
            1_000,
            "live book keeps its stamp"
        );
        feed.own_book(&book("99")).unwrap();
        let served = feed.book("BTC", 5_000).unwrap();
        assert_eq!(
            served.time, 5_000,
            "scripted book is the scenario's present"
        );
        assert_eq!(served.levels[0][0].px, Decimal::from(99));
        feed.clear_book("BTC").unwrap();
        assert_eq!(
            feed.book("BTC", 6_000).unwrap().levels[0][0].px,
            Decimal::from(100)
        );
    }

    /// The stall holds the live plane (scripted writes still land) and the
    /// served-book freshness is live-only: a scripted book is never stale.
    #[test]
    #[allow(clippy::too_many_lines)] // one rendering/transition table: splitting it would hide the field-to-line mapping
    fn a_stall_drops_live_updates_and_book_freshness_is_live_only() {
        let feed = MarketFeed::memory();
        let now = crate::now_ms();
        let book = |px: &str| L2Book {
            coin: "BTC".into(),
            time: 1,
            levels: [
                vec![Level {
                    px: px.parse().unwrap(),
                    sz: Decimal::ONE,
                    n: 1,
                }],
                vec![],
            ],
            spread: None,
        };
        feed.put_ctx("BTC".into(), test_ctx("100", "101"), now);
        feed.put_book(book("100"), now);
        assert_eq!(
            feed.book_age("BTC", now, MarketFeed::MAX_STALE_MS),
            BookAge::Fresh
        );
        assert_eq!(
            feed.book_age(
                "BTC",
                now + MarketFeed::MAX_STALE_MS + 1,
                MarketFeed::MAX_STALE_MS
            ),
            BookAge::Stale(MarketFeed::MAX_STALE_MS + 1),
            "an aged live book is stale"
        );
        // The stall: live writes drop, scripted writes land.
        // The deadline is computed from the stall's own clock read: compare in
        // a window, not to the millisecond (the two reads may straddle one).
        let until = feed.stall(30);
        assert!(
            until >= now + 30_000 && until <= now + 31_000,
            "the stall's deadline is ~30 s out: {until} vs {now}"
        );
        assert!(feed.stalled());
        feed.put_ctx("BTC".into(), test_ctx("999", "999"), now);
        feed.put_book(book("999"), now);
        assert_eq!(
            feed.mark("BTC"),
            Some(Decimal::from(100)),
            "the live ctx was dropped"
        );
        assert_eq!(
            feed.book("BTC", now).unwrap().levels[0][0].px,
            Decimal::from(100)
        );
        feed.own_book(&book("77")).unwrap();
        assert_eq!(
            feed.book("BTC", now).unwrap().levels[0][0].px,
            Decimal::from(77),
            "the scripted plane is never gated"
        );
        assert_eq!(
            feed.book_age("BTC", now + 60_000, MarketFeed::MAX_STALE_MS),
            BookAge::Scripted
        );
        // Clearing the stall lets the live plane back in.
        feed.stall(0);
        assert!(!feed.stalled());
        feed.put_ctx("BTC".into(), test_ctx("999", "999"), now);
        assert_eq!(feed.mark("BTC"), Some(Decimal::from(999)));
    }

    #[test]
    fn staleness_tracks_ctx_updates() {
        let feed = MarketFeed::memory();
        assert_eq!(feed.stale_ms("BTC", 10_000), None);
        feed.put_ctx("BTC".into(), test_ctx("100", "101"), 8_000);
        assert_eq!(feed.stale_ms("BTC", 10_000), Some(2_000));
        assert_eq!(feed.stale_ms("ETH", 10_000), None);
    }

    /// A funding-only scenario owns the published rate, not the mark/oracle.
    /// The mirror's `oracle-liveness` writer refuses a `controlOwned` context;
    /// conflating funding with price ownership froze WHYPE for an hour and
    /// expired its proof gate. The two bits must stay separate.
    #[test]
    fn funding_ownership_does_not_own_the_mark() {
        let feed = MarketFeed::memory();
        feed.put_meta(
            crate::wire::Meta {
                universe: vec![crate::wire::AssetInfo {
                    name: "BTC".into(),
                    sz_decimals: 5,
                    max_leverage: 20,
                    margin_table_id: None,
                    margin_mode: None,
                    only_isolated: None,
                    is_delisted: None,
                    ..Default::default()
                }],
                margin_tables: Vec::new(),
                collateral_token: 0,
            },
            1_000,
        );
        feed.put_ctx("BTC".into(), test_ctx("100", "101"), 1_000);
        feed.own_funding("BTC", Decimal::new(-4, 2), None).unwrap();
        assert!(feed.funding_is_owned("BTC"));
        assert!(
            !feed.market_is_owned("BTC"),
            "funding is not price ownership"
        );
        let served = feed.meta_and_ctxs().expect("ctx");
        let btc = served
            .1
            .iter()
            .find(|c| c.mark_px == Decimal::from(100))
            .expect("BTC ctx");
        assert!(!btc.control_owned);
        assert!(btc.funding_owned);
        assert_eq!(btc.funding, Decimal::new(-4, 2));
        // The live oracle is still what a re-stamping consumer reads.
        assert_eq!(feed.oracle("BTC"), Some(Decimal::from(101)));
    }

    /// A deadline bounds the override: past it the live rate answers and the
    /// ownership bit clears, without a control write.
    #[test]
    fn an_expired_funding_override_is_inert() {
        let feed = MarketFeed::memory();
        feed.put_ctx("BTC".into(), test_ctx("100", "101"), 1_000);
        feed.own_funding("BTC", Decimal::new(5, 2), Some(1_000))
            .unwrap();
        // `now_ms()` is wall time; a deadline in the past is inert immediately.
        assert!(!feed.funding_is_owned("BTC"));
        assert_eq!(feed.owned_count(), 0);
        assert_eq!(feed.funding("BTC"), Some(Decimal::ZERO));
    }

    /// The registry is the venue's `perpDexs` order, one meta per dex, with
    /// the default dex readable through the old single-dex accessor.
    #[test]
    fn the_feed_serves_every_dex_and_the_default_through_meta() {
        let feed = crate::sim::feed_with_dexs(
            &[],
            &[
                ("", &[("BTC", 40)]),
                ("xyz", &[("xyz:XYZ100", 20), ("xyz:TSLA", 10)]),
            ],
        );
        assert_eq!(feed.meta().unwrap().universe[0].name, "BTC");
        assert_eq!(feed.meta_of("xyz").unwrap().universe.len(), 2);
        assert!(feed.meta_of("nope").is_none());
        let names: Vec<String> = feed.all_dexs().into_iter().map(|d| d.name).collect();
        assert_eq!(names, ["", "xyz"]);
        assert_eq!(feed.metas().len(), 2);
        assert_eq!(
            feed.meta_for_coin("xyz:TSLA").unwrap().universe[1].name,
            "xyz:TSLA"
        );
        assert!(feed.meta_for_coin("xyz:NOPE").is_none());
        // The one resolver, both ways.
        assert_eq!(feed.coin_of_asset(0).as_deref(), Some("BTC"));
        assert_eq!(feed.coin_of_asset(110_001).as_deref(), Some("xyz:TSLA"));
        assert_eq!(feed.coin_of_asset(5), None);
        assert_eq!(feed.asset_id("xyz:TSLA"), Some(110_001));
        // A fixture feed has no venue `perpDexs` yet: not registry-ready.
        assert!(!feed.registry_loaded());
    }

    /// `put_perp_dexs` is the venue's registry: it flips the readiness fact,
    /// keeps `perpDexs` order, and a misaligned `allPerpMetas` is refused (the
    /// previous registry stays).
    #[test]
    fn the_perp_dexs_registry_lands_aligned_or_not_at_all() {
        let feed = MarketFeed::memory();
        let metas = vec![
            crate::sim::meta_of(&[("BTC", 40)]),
            crate::sim::meta_of(&[("xyz:TSLA", 10)]),
        ];
        let dexs = vec![(String::new(), None), ("xyz".to_string(), None)];
        assert!(!feed.put_perp_dexs(dexs.clone(), metas[..1].to_vec(), 1));
        assert!(!feed.registry_loaded());
        assert!(feed.put_perp_dexs(dexs, metas, 1));
        assert!(feed.registry_loaded());
        assert_eq!(feed.coin_of_asset(110_000).as_deref(), Some("xyz:TSLA"));
    }

    /// A builder dex's contexts align to its universe by index, are quoted by
    /// coin name (`mark`, `meta_and_ctxs_of`) and a slot without a context is
    /// skipped, not shifted onto its neighbour.
    #[test]
    fn a_builder_dexs_contexts_are_quoted_by_coin_and_aligned_by_index() {
        let feed = crate::sim::feed_with_dexs(
            &[("BTC", "100", "100")],
            &[
                ("", &[("BTC", 40)]),
                ("xyz", &[("xyz:A", 10), ("xyz:B", 10), ("xyz:C", 10)]),
            ],
        );
        // Two contexts for three assets: A and B are folded, C is not.
        feed.put_dex_ctxs(
            "xyz",
            crate::sim::meta_of(&[("xyz:A", 10), ("xyz:B", 10), ("xyz:C", 10)]),
            vec![test_ctx("1", "1"), test_ctx("2", "2")],
            5,
        );
        assert_eq!(feed.mark("xyz:A"), Some(Decimal::from(1)));
        assert_eq!(feed.mark("xyz:B"), Some(Decimal::from(2)));
        assert_eq!(feed.mark("xyz:C"), None);
        assert_eq!(feed.stale_ms("xyz:B", 15), Some(10));
        let served = feed.meta_and_ctxs_of("xyz").unwrap();
        assert_eq!(served.1.len(), 2);
        assert_eq!(feed.mark("BTC"), Some(Decimal::from(100)));
        // The default dex's served contexts do not carry the builder coins.
        assert_eq!(feed.meta_and_ctxs().unwrap().1.len(), 1);
    }

    /// `ALL_DEXS` mids split by the coin's dex; an unprefixed key (a default
    /// perp, a spot `@N`, an outcome `#N`) is the default dex's, exactly as
    /// the venue's default `allMids` reads; an owned price wins in its dex.
    #[test]
    fn all_dexs_mids_split_by_the_coins_dex() {
        let feed = crate::sim::feed_with_dexs(
            &[],
            &[
                ("", &[("BTC", 40)]),
                ("xyz", &[("xyz:TSLA", 10)]),
                ("flx", &[]),
            ],
        );
        let mids: BTreeMap<String, Decimal> = [
            ("BTC", "100"),
            ("@107", "5"),
            ("#10", "0.5"),
            ("xyz:TSLA", "300"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.parse().unwrap()))
        .collect();
        assert_eq!(feed.builder_mids_age_ms(10), None, "none landed yet");
        feed.put_all_dexs_mids(mids, 4);
        let default = feed.all_mids().0;
        assert_eq!(default.len(), 3);
        assert!(default.contains_key("@107") && default.contains_key("#10"));
        let xyz = feed.all_mids_of("xyz").0;
        assert_eq!(xyz.len(), 1);
        assert_eq!(xyz["xyz:TSLA"], Decimal::from(300));
        assert!(feed.all_mids_of("flx").0.is_empty(), "stamped, empty");
        assert_eq!(feed.builder_mids_age_ms(10), Some(6));
        assert_eq!(feed.mids_age_ms(10), Some(6));
        feed.own_price(
            "xyz:TSLA",
            PriceOverride {
                mark: Decimal::from(7),
                oracle: Decimal::from(7),
            },
        )
        .unwrap();
        assert_eq!(feed.all_mids_of("xyz").0["xyz:TSLA"], Decimal::from(7));
        assert!(
            !feed.all_mids().0.contains_key("xyz:TSLA"),
            "an owned builder price is not a default-dex mid"
        );
    }

    /// Caps are per dex; a dex that has not answered is unknown, never
    /// "nothing capped".
    #[test]
    fn open_interest_caps_are_per_dex() {
        let feed = MarketFeed::memory();
        assert_eq!(feed.open_interest_caps_of("xyz"), None);
        feed.put_oi_caps_of("xyz", vec!["xyz:TSLA".into()]);
        feed.put_oi_caps(vec!["BTC".into()]);
        assert_eq!(
            feed.open_interest_caps_of("xyz"),
            Some(vec!["xyz:TSLA".into()])
        );
        assert_eq!(feed.open_interest_caps(), Some(vec!["BTC".into()]));
        assert_eq!(feed.open_interest_caps_of("flx"), None);
    }

    /// The wanted set: every coin some dex lists (default or builder, no coin
    /// singled out) is wanted from the moment it is asked for until it has
    /// idled past the grace window, then dropped with its cached book; a coin
    /// no dex lists is never wanted.
    #[test]
    fn a_wanted_book_idles_out_with_its_cache() {
        let feed = MarketFeed::memory();
        feed.put_meta(crate::sim::meta_of(&[("BTC", 40), ("SOL", 20)]), 1);
        feed.put_dex_meta("xyz", crate::sim::meta_of(&[("xyz:TSLA", 10)]), 1);
        assert!(feed.want_book("BTC"), "a default coin is a market");
        assert!(feed.want_book("SOL"));
        assert!(feed.want_book("xyz:TSLA"), "so is a builder coin");
        assert!(!feed.want_book("NOPE"), "no dex lists it");
        assert!(!feed.want_book("xyz:NOPE"), "the dex does not list it");
        let now = crate::now_ms();
        let mut wanted = feed.wanted_coins(now);
        wanted.sort();
        assert_eq!(wanted, ["BTC", "SOL", "xyz:TSLA"]);

        let level = |px: &str| Level {
            px: px.parse().unwrap(),
            sz: Decimal::ONE,
            n: 1,
        };
        feed.put_book(
            L2Book {
                coin: "SOL".into(),
                time: 0,
                levels: [vec![level("99")], vec![level("101")]],
                spread: None,
            },
            now,
        );
        assert!(feed.book("SOL", now).is_some());
        // Held coins keep themselves wanted: SOL is touched a second on, the
        // rest are not.
        assert!(feed.want_book_at("SOL", now + 1_000));
        let later = now + relay::GRACE_MS + 1;
        assert_eq!(
            feed.wanted_coins(later),
            ["SOL"],
            "the coin touched just now stays, the idle ones are dropped"
        );
        let much_later = later + 1_000 + relay::GRACE_MS;
        assert!(feed.wanted_coins(much_later).is_empty());
        assert!(
            feed.book("SOL", much_later).is_none(),
            "an idle coin's cached book goes with it"
        );
    }

    /// A quiet book (the venue pushes on change only) is current while its
    /// stream is confirmed: the freshness gate reads the confirmation, the
    /// served `time` stays the last update's.
    #[test]
    fn a_confirmed_quiet_book_stays_fresh_without_moving_its_time() {
        let feed = MarketFeed::memory();
        let level = |px: &str| Level {
            px: px.parse().unwrap(),
            sz: Decimal::ONE,
            n: 1,
        };
        feed.put_book(
            L2Book {
                coin: "BTC".into(),
                time: 0,
                levels: [vec![level("99")], vec![level("101")]],
                spread: None,
            },
            1_000,
        );
        let window = MarketFeed::MAX_STALE_MS;
        assert_eq!(feed.book_age("BTC", 1_000 + window, window), BookAge::Fresh);
        assert!(matches!(
            feed.book_age("BTC", 2_000 + window, window),
            BookAge::Stale(_)
        ));
        feed.confirm_books(["BTC", "ETH"], 2_000 + window);
        assert_eq!(
            feed.book_age("BTC", 2_000 + window, window),
            BookAge::Fresh,
            "the confirmation keeps it current"
        );
        assert_eq!(
            feed.book_age("ETH", 2_000 + window, window),
            BookAge::Warming,
            "a coin with no book is not conjured by a confirmation"
        );
        assert_eq!(
            feed.book("BTC", 9_999).unwrap().time,
            1_000,
            "the served time is the last real update"
        );
    }

    /// Every coin of every dex has a permanent context stream except the
    /// delisted ones, which are streamed only while wanted; a wanted coin
    /// also gets its book stream, a coin no dex lists gets nothing.
    #[test]
    fn the_desired_streams_cover_every_live_coin_and_the_wanted_books() {
        use relay::Stream;
        let feed = MarketFeed::memory();
        let mut default = crate::sim::meta_of(&[("BTC", 40), ("OLD", 10)]);
        default.universe[1].is_delisted = Some(true);
        feed.put_meta(default, 1);
        feed.put_dex_meta("xyz", crate::sim::meta_of(&[("xyz:TSLA", 10)]), 1);
        let now = crate::now_ms();
        let streams = feed.desired_streams(now);
        assert!(streams.contains(&Stream::Ctx("BTC".into())));
        assert!(streams.contains(&Stream::Ctx("xyz:TSLA".into())));
        assert!(
            !streams.contains(&Stream::Ctx("OLD".into())),
            "a delisted coin is not streamed unasked"
        );
        assert!(!streams.iter().any(|s| matches!(s, Stream::Book(_))));

        assert!(feed.want_book("OLD"));
        assert!(feed.want_book("xyz:TSLA"));
        assert!(!feed.want_book("NOPE"));
        let lease = feed.lease_coin("BTC").expect("listed");
        assert!(feed.lease_coin("NOPE").is_none());
        let streams = feed.desired_streams(now);
        for coin in ["OLD", "xyz:TSLA", "BTC"] {
            assert!(streams.contains(&Stream::Book(coin.into())), "{coin} book");
        }
        assert!(
            streams.contains(&Stream::Ctx("OLD".into())),
            "a wanted delisted coin is streamed"
        );
        assert!(!streams.contains(&Stream::Book("NOPE".into())));
        drop(lease);
    }

    /// Readiness reads the default dex's largest coins, whichever they are,
    /// and nothing from a builder dex.
    #[test]
    fn default_dex_freshness_follows_the_largest_coins_not_a_fixed_list() {
        let feed = MarketFeed::memory();
        assert!(!feed.default_dex_fresh(10, 100), "nothing landed");
        feed.put_meta(crate::sim::meta_of(&[("AAA", 10), ("BBB", 10)]), 1);
        feed.put_dex_meta("xyz", crate::sim::meta_of(&[("xyz:TSLA", 10)]), 1);
        let mut big = test_ctx("1", "1");
        big.day_ntl_vlm = Decimal::from(1_000);
        feed.put_ctx("AAA".into(), big, 50);
        feed.put_ctx("BBB".into(), test_ctx("1", "1"), 5);
        assert!(feed.default_dex_fresh(100, 100), "both inside the window");
        assert!(
            !feed.default_dex_fresh(150, 100),
            "the stale small coin counts: the top three are all read"
        );
        // A builder coin's age never matters.
        feed.put_ctx("xyz:TSLA".into(), test_ctx("1", "1"), 1);
        assert!(feed.default_dex_fresh(100, 100));
    }

    /// The socket's push is not overwritten by a REST snapshot taken within
    /// its priority window; a stale coin is backfilled; a builder dex whose
    /// coins are all fresh is carried by the socket.
    #[test]
    fn a_rest_snapshot_backfills_but_never_overwrites_a_fresher_push() {
        let feed = MarketFeed::memory();
        feed.put_dex_meta("xyz", crate::sim::meta_of(&[("xyz:A", 10)]), 1);
        feed.put_ctx("xyz:A".into(), test_ctx("5", "5"), 10_000);
        feed.put_ctx_backfill("xyz:A".into(), test_ctx("4", "4"), 10_500);
        assert_eq!(feed.mark("xyz:A"), Some(Decimal::from(5)), "the push wins");
        feed.put_ctx_backfill(
            "xyz:A".into(),
            test_ctx("4", "4"),
            10_000 + WS_CTX_PRIORITY_MS,
        );
        assert_eq!(feed.mark("xyz:A"), Some(Decimal::from(4)), "backfilled");
        assert!(feed.dex_ctxs_fresh("xyz", 12_000, 15_000));
        assert!(!feed.dex_ctxs_fresh("xyz", 40_000, 15_000));
        assert!(!feed.dex_ctxs_fresh("nope", 12_000, 15_000));
    }

    /// A dex with delisted coins is carried by the socket only once the REST
    /// read has landed the delisted coins' static contexts (the socket never
    /// streams them): otherwise `metaAndAssetCtxs {dex}` would stay a 503.
    #[test]
    fn a_dex_with_delisted_coins_needs_their_rest_context_once() {
        let feed = MarketFeed::memory();
        let mut meta = crate::sim::meta_of(&[("xyz:A", 10), ("xyz:OLD", 10)]);
        meta.universe[1].is_delisted = Some(true);
        feed.put_dex_meta("xyz", meta.clone(), 1);
        feed.put_ctx("xyz:A".into(), test_ctx("5", "5"), 10_000);
        assert!(
            !feed.dex_ctxs_fresh("xyz", 12_000, 15_000),
            "the delisted coin has no context yet: the REST read is still owed"
        );
        feed.put_dex_ctxs(
            "xyz",
            meta,
            vec![test_ctx("5", "5"), test_ctx("2", "2")],
            11_000,
        );
        assert!(feed.dex_ctxs_fresh("xyz", 12_000, 15_000));
        assert!(feed.meta_and_ctxs_of("xyz").is_some_and(|m| m.1.len() == 2));
    }

    /// `allMids` for a dex takes the context's mid when the context is newer
    /// than the dex's mids snapshot (the builder dexes' mids are otherwise
    /// the REST sweep's), and keeps the snapshot's spot keys.
    #[test]
    fn a_newer_context_mid_wins_over_the_mids_snapshot() {
        let feed =
            crate::sim::feed_with_dexs(&[], &[("", &[("BTC", 40)]), ("xyz", &[("xyz:A", 10)])]);
        let mids: BTreeMap<String, Decimal> = [("BTC", "100"), ("@107", "5"), ("xyz:A", "7")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.parse().unwrap()))
            .collect();
        feed.put_all_dexs_mids(mids, 1_000);
        let mut ctx = test_ctx("9", "9");
        ctx.mid_px = Some(Decimal::from(9));
        feed.put_ctx("xyz:A".into(), ctx.clone(), 2_000);
        assert_eq!(feed.all_mids_of("xyz").0["xyz:A"], Decimal::from(9));
        ctx.mid_px = Some(Decimal::from(101));
        feed.put_ctx("BTC".into(), ctx, 2_000);
        let default = feed.all_mids().0;
        assert_eq!(default["BTC"], Decimal::from(101));
        assert_eq!(default["@107"], Decimal::from(5), "spot keys stay");
        // A snapshot newer than the context wins back.
        feed.put_all_dexs_mids(
            [("xyz:A".to_string(), Decimal::from(8))]
                .into_iter()
                .collect(),
            3_000,
        );
        assert_eq!(feed.all_mids_of("xyz").0["xyz:A"], Decimal::from(8));
    }

    /// A venue stand-in answering `l2Book` for any coin, counting the reads.
    pub(crate) async fn l2_server(reads: std::sync::Arc<std::sync::atomic::AtomicUsize>) -> String {
        use axum::extract::State;
        use axum::routing::post;
        async fn info(
            State(reads): State<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
            axum::Json(req): axum::Json<serde_json::Value>,
        ) -> axum::Json<serde_json::Value> {
            reads.fetch_add(1, Ordering::Relaxed);
            let coin = req["coin"].as_str().unwrap_or_default().to_string();
            axum::Json(serde_json::json!({
                "coin": coin, "time": 1,
                "levels": [[{"px": "199", "sz": "5", "n": 1}], [{"px": "200", "sz": "5", "n": 1}]]
            }))
        }
        let app = axum::Router::new()
            .route("/info", post(info))
            .with_state(reads);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    /// `ensure_book` fetches a listed coin's book once (the venue's `l2Book`),
    /// caches it and keeps the coin wanted; a coin no dex lists never reaches
    /// the venue; a cached fresh book is served without a read.
    #[tokio::test]
    async fn ensure_book_fetches_once_and_keeps_the_coin_wanted() {
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let url = l2_server(reads.clone()).await;
        let feed = std::sync::Arc::new(MarketFeed::memory());
        feed.put_meta(crate::sim::meta_of(&[("BTC", 40)]), 1);
        feed.put_dex_meta("xyz", crate::sim::meta_of(&[("xyz:TSLA", 10)]), 1);
        // No client attached: nothing to fetch with.
        assert!(feed.ensure_book("xyz:TSLA").await.is_none());
        feed.attach_rest(rest::RestClient::new(&[url]).unwrap());

        let book = feed.ensure_book("xyz:TSLA").await.expect("fetched");
        assert_eq!(book.coin, "xyz:TSLA");
        assert_eq!(book.levels[1][0].px, Decimal::from(200));
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert!(feed
            .wanted_coins(crate::now_ms())
            .contains(&"xyz:TSLA".to_string()));
        assert!(feed.ensure_book("xyz:TSLA").await.is_some());
        assert_eq!(reads.load(Ordering::Relaxed), 1, "the cached book is fresh");

        assert!(feed.ensure_book("NOPE").await.is_none());
        assert_eq!(
            reads.load(Ordering::Relaxed),
            1,
            "an unknown coin never reaches the venue"
        );
        // The websocket admission wants a listed coin and refuses an unlisted one.
        assert!(feed.warm_book("xyz:TSLA"));
        assert!(!feed.warm_book("NOPE"));
    }

    /// A transfer's token string is `NAME` or `NAME:tokenId`: the id must be
    /// the registry's for the name, an unknown name is nothing, and USDC
    /// resolves (with its published id) before the registry lands.
    #[test]
    fn a_token_resolves_by_name_and_id() {
        let feed = MarketFeed::memory();
        let usdc_id = "USDC:0x6d1e7cde53ba9467b783cb7c530ce054";
        assert_eq!(feed.resolve_token("USDC"), Some((0, "USDC".to_string())));
        assert_eq!(feed.resolve_token(usdc_id), Some((0, "USDC".to_string())));
        assert_eq!(feed.resolve_token("USDC:0xdead"), None);
        assert_eq!(
            feed.resolve_token("USDH"),
            None,
            "unknown before the registry"
        );
        let usdh = crate::wire::SpotToken {
            name: "USDH".into(),
            sz_decimals: 2,
            wei_decimals: 8,
            index: 360,
            is_canonical: true,
            evm_contract: None,
            token_id: Some("0x54e00a5988577cb0b0c9ab0cb6ef7f4b".into()),
            full_name: None,
            deployer_trading_fee_share: Decimal::ZERO,
        };
        feed.put_spot_meta(crate::wire::SpotMeta {
            tokens: vec![usdh],
            universe: vec![],
        });
        assert_eq!(
            feed.resolve_token("USDH:0x54E00A5988577CB0B0C9AB0CB6EF7F4B"),
            Some((360, "USDH".to_string()))
        );
        assert_eq!(feed.resolve_token("USDH:0x1234"), None);
        assert_eq!(feed.resolve_token("USDH"), Some((360, "USDH".to_string())));
    }
}
