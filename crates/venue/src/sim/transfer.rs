//! Collateral transfers between an account's wallets: the spot wallet and the
//! perp dexes' clearinghouses (`sendAsset`, HTTP and CoreWriter action 13).
//!
//! The real venue keeps one clearinghouse per (user, perp dex) and a spot
//! wallet. Facts (the official SDK's `send_asset` and the docs): dex `""` is
//! the default perp dex, `"spot"` the spot wallet, any other name a builder
//! dex; and the token sent must match the collateral token of a perp-dex
//! endpoint, so USDC moves to and from the default dex and the dexes that
//! collateralise in USDC, USDH to a USDH dex, and a USDH transfer into the
//! default dex is refused.
//!
//! Bounds: a perp wallet gives up what its dex lets it withdraw (the served
//! `withdrawable`: margin in use stays, and a standard account never sends more
//! than the wallet's cash), a unified account's spot wallet what is left after
//! the maintenance its positions hold on every dex margining against the token.
use super::account::{self, Account};
use super::{stamp_funding_baseline, Sim, TokenRef};
use crate::error::{Error, Result};
use crate::feed::MarketFeed;
use alloy_primitives::Address;
use rust_decimal::Decimal;

/// One end of a transfer: the spot wallet or one perp dex's clearinghouse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Wallet {
    Spot,
    /// A perp dex by name (`""` = the default dex).
    Perp(String),
}

/// The `dex` string of the venue's `sendAsset` for the spot wallet.
pub(crate) const SPOT_DEX_NAME: &str = "spot";

impl Wallet {
    /// The wallet the venue's `sourceDex` / `destinationDex` string names.
    pub(crate) fn of_dex_name(name: &str) -> Self {
        if name == SPOT_DEX_NAME {
            Wallet::Spot
        } else {
            Wallet::Perp(name.to_string())
        }
    }

    fn describe(&self) -> String {
        match self {
            Wallet::Spot => "the spot wallet".to_string(),
            Wallet::Perp(dex) if dex.is_empty() => "the default perp dex".to_string(),
            Wallet::Perp(dex) => format!("perp dex `{dex}`"),
        }
    }
}

/// One `sendAsset` applied: `amount` of `token` from `from`'s `source` wallet
/// to `to`'s `destination` wallet.
pub(crate) struct AssetSend<'a> {
    pub from: Address,
    pub to: Address,
    pub token: TokenRef<'a>,
    pub amount: Decimal,
    pub source: Wallet,
    pub destination: Wallet,
}

/// A refusal in the sim's own voice (the transfer actions' texts are not
/// captured from the real venue).
fn refuse(reason: impl Into<String>) -> Error {
    Error::bad_request(format!("venue-sim: {}", reason.into()))
}

/// The shape rules that need no account: a positive amount, known dexes, and
/// the token matching every perp endpoint's collateral.
fn check_shape(send: &AssetSend<'_>, feed: &MarketFeed) -> Result<()> {
    if send.amount <= Decimal::ZERO {
        return Err(refuse("the transfer amount must be positive"));
    }
    for wallet in [&send.source, &send.destination] {
        let Wallet::Perp(dex) = wallet else {
            continue;
        };
        if feed.meta_of(dex).is_none() {
            return Err(refuse(format!(
                "perp dex `{dex}` is not in the venue's dex registry"
            )));
        }
        let collateral = feed.collateral_token_of(dex);
        if collateral != send.token.index {
            let named = feed
                .token_name(collateral)
                .unwrap_or_else(|| collateral.to_string());
            return Err(refuse(format!(
                "token {} is not the collateral token of {} ({named}); the token sent must \
                 match the collateral token of a perp dex endpoint",
                send.token.coin,
                wallet.describe()
            )));
        }
    }
    if send.from == send.to && send.source == send.destination {
        return Err(refuse("the source and the destination are the same wallet"));
    }
    Ok(())
}

/// What the source wallet may give up now (see the module docs).
fn source_bound(
    account: &Account,
    feed: &MarketFeed,
    wallet: &Wallet,
    token: u32,
) -> Result<Decimal> {
    match wallet {
        Wallet::Spot => {
            let held = account.spot.get(&token).map_or(Decimal::ZERO, |e| e.total);
            Ok(match account::spot_bridge_bound(account, feed, token)? {
                Some(free) => held.min(free),
                None => held,
            })
        }
        Wallet::Perp(dex) => account::perp_transfer_bound(account, feed, dex).ok_or_else(|| {
            refuse("the marks behind the source dex's positions are not served yet")
        }),
    }
}

/// The destination can take `amount` without overflowing its decimal range
/// (checked before anything moves, so a refusal never half applies).
fn check_headroom(
    account: Option<&Account>,
    wallet: &Wallet,
    token: u32,
    amount: Decimal,
) -> Result<()> {
    let current = match (account, wallet) {
        (None, _) => Decimal::ZERO,
        (Some(a), Wallet::Spot) => a.spot.get(&token).map_or(Decimal::ZERO, |e| e.total),
        (Some(a), Wallet::Perp(dex)) => a.perp_balance(dex),
    };
    account::add(current, amount, "asset send")?;
    Ok(())
}

impl Sim {
    /// Apply one `sendAsset` (module docs). `Err(BadRequest)` is a refusal
    /// carrying its reason (nothing moved); any other error is a fault.
    pub(crate) fn send_asset(&self, send: &AssetSend<'_>, feed: &MarketFeed) -> Result<()> {
        check_shape(send, feed)?;
        let clock = self.clock(crate::now_ms())?;
        self.with_state(|state| {
            let source = state.accounts.get(&send.from).ok_or_else(|| {
                refuse(format!("account {} is not a venue-sim account", send.from))
            })?;
            let bound = source_bound(source, feed, &send.source, send.token.index)?;
            if send.amount > bound {
                return Err(refuse(format!(
                    "the transfer of {} {} exceeds what {} may send now ({bound})",
                    send.amount,
                    send.token.coin,
                    send.source.describe()
                )));
            }
            check_headroom(
                state.accounts.get(&send.to),
                &send.destination,
                send.token.index,
                send.amount,
            )?;
            // The debit.
            let from = state.accounts.entry(send.from).or_default();
            match &send.source {
                Wallet::Spot => {
                    let entry = account::spot_entry(from, send.token);
                    entry.total = account::sub(entry.total, send.amount, "asset send")?;
                }
                Wallet::Perp(dex) => {
                    from.credit_perp(dex, -send.amount, "asset send")?;
                }
            }
            stamp_funding_baseline(from, clock);
            // The credit.
            let to = state.accounts.entry(send.to).or_default();
            match &send.destination {
                Wallet::Spot => {
                    account::credit_entry(to, send.token, send.amount, "asset send")?;
                }
                Wallet::Perp(dex) => {
                    to.credit_perp(dex, send.amount, "asset send")?;
                }
            }
            stamp_funding_baseline(to, clock);
            Ok(())
        })?;
        self.touch();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::order::{Order, Tif};
    use crate::sim::{meta_of, meta_with, Deal, DealClass};
    use crate::wire::{L2Book, Level, SpotMeta, SpotToken, UserAbstraction};

    fn dec(text: &str) -> Decimal {
        text.parse().unwrap()
    }

    fn token(name: &str, index: u32) -> SpotToken {
        SpotToken {
            name: name.into(),
            sz_decimals: 8,
            wei_decimals: 8,
            index,
            is_canonical: true,
            evm_contract: None,
            token_id: None,
            full_name: None,
            deployer_trading_fee_share: Decimal::ZERO,
        }
    }

    fn book(coin: &str, px: &str) -> L2Book {
        let level = |p: &str| Level {
            px: dec(p),
            sz: dec("50"),
            n: 1,
        };
        L2Book {
            coin: coin.into(),
            time: 0,
            levels: [vec![level(px)], vec![level(px)]],
            spread: None,
        }
    }

    /// The default dex (`ETH`), `xyz` (USDC collateral, `xyz:TSLA`) and `flx`
    /// (USDH collateral, `flx:TSLA`); both tokens are in the spot registry.
    fn feed() -> MarketFeed {
        let feed = MarketFeed::memory(vec![
            "ETH".to_string(),
            "xyz:TSLA".to_string(),
            "flx:TSLA".to_string(),
        ]);
        let now = crate::now_ms();
        feed.put_meta(meta_with("ETH", 10), now);
        feed.put_dex_meta("xyz", meta_of(&[("xyz:TSLA", 10)]), now);
        let mut flx = meta_of(&[("flx:TSLA", 10)]);
        flx.collateral_token = 360;
        feed.put_dex_meta("flx", flx, now);
        for (coin, px) in [("ETH", "100"), ("xyz:TSLA", "200"), ("flx:TSLA", "200")] {
            feed.put_ctx(coin.into(), crate::wire::test_ctx(px, px), now);
            feed.put_book(book(coin, px), now);
        }
        feed.put_oi_caps(Vec::new());
        feed.put_oi_caps_of("xyz", Vec::new());
        feed.put_oi_caps_of("flx", Vec::new());
        feed.put_spot_meta(SpotMeta {
            tokens: vec![token("USDC", 0), token("USDH", 360)],
            universe: vec![],
        });
        feed
    }

    const USDC: TokenRef<'static> = TokenRef {
        index: 0,
        coin: "USDC",
    };
    const USDH: TokenRef<'static> = TokenRef {
        index: 360,
        coin: "USDH",
    };

    fn spot(sim: &Sim, who: Address, token: TokenRef<'_>, amount: &str) {
        sim.credit_spot(who, token, dec(amount)).unwrap();
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "a test helper: each call names its two wallets and amount inline"
    )]
    fn send(
        sim: &Sim,
        feed: &MarketFeed,
        token: TokenRef<'_>,
        amount: &str,
        source: Wallet,
        destination: Wallet,
    ) -> Result<()> {
        let me = Address::repeat_byte(0xa1);
        sim.send_asset(
            &AssetSend {
                from: me,
                to: me,
                token,
                amount: dec(amount),
                source,
                destination,
            },
            feed,
        )
    }

    fn perp(dex: &str) -> Wallet {
        Wallet::Perp(dex.to_string())
    }

    fn account(sim: &Sim) -> Account {
        sim.account(Address::repeat_byte(0xa1)).unwrap().unwrap()
    }

    fn refusal(result: Result<()>) -> String {
        match result {
            Err(Error::BadRequest { reason }) => reason,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// Spot USDC moves to a USDC-collateral builder dex's ledger and back;
    /// each dex keeps its own balance.
    #[test]
    fn spot_to_a_usdc_dex_and_back() {
        let feed = feed();
        let sim = Sim::memory();
        spot(&sim, Address::repeat_byte(0xa1), USDC, "100");

        send(&sim, &feed, USDC, "40", Wallet::Spot, perp("xyz")).unwrap();
        let a = account(&sim);
        assert_eq!(a.perp_balance("xyz"), dec("40"));
        assert_eq!(
            a.perp_balance(""),
            Decimal::ZERO,
            "the default dex is untouched"
        );
        assert_eq!(a.spot[&0].total, dec("60"));

        send(&sim, &feed, USDC, "15", perp("xyz"), Wallet::Spot).unwrap();
        let a = account(&sim);
        assert_eq!(a.perp_balance("xyz"), dec("25"));
        assert_eq!(a.spot[&0].total, dec("75"));

        // The default dex is the dex named "".
        send(&sim, &feed, USDC, "5", Wallet::Spot, perp("")).unwrap();
        assert_eq!(account(&sim).perp_balance(""), dec("5"));
    }

    /// The token must be the collateral of every perp endpoint: USDH does not
    /// go to a USDC dex (or the default dex), USDC does not go to the USDH dex.
    #[test]
    fn a_token_that_is_not_the_dexs_collateral_is_refused() {
        let feed = feed();
        let sim = Sim::memory();
        spot(&sim, Address::repeat_byte(0xa1), USDC, "100");
        spot(&sim, Address::repeat_byte(0xa1), USDH, "100");

        let reason = refusal(send(&sim, &feed, USDC, "10", Wallet::Spot, perp("flx")));
        assert!(
            reason.contains("collateral token of perp dex `flx`"),
            "{reason}"
        );
        let reason = refusal(send(&sim, &feed, USDH, "10", Wallet::Spot, perp("xyz")));
        assert!(reason.contains("(USDC)"), "{reason}");
        refusal(send(&sim, &feed, USDH, "10", Wallet::Spot, perp("")));
        // The matching token works.
        send(&sim, &feed, USDH, "10", Wallet::Spot, perp("flx")).unwrap();
        assert_eq!(account(&sim).perp_balance("flx"), dec("10"));
        // Nothing half applied by the refusals.
        assert_eq!(account(&sim).spot[&360].total, dec("90"));
        assert_eq!(account(&sim).spot[&0].total, dec("100"));
    }

    /// Dex to dex moves a token between two clearinghouses that share its
    /// collateral; a USDC dex to a USDH dex is a token mismatch.
    #[test]
    fn dex_to_dex_needs_a_shared_collateral() {
        let feed = feed();
        let sim = Sim::memory();
        let me = Address::repeat_byte(0xa1);
        sim.deal(
            &feed,
            &Deal {
                address: me,
                class: DealClass::Perp,
                coin: None,
                dex: Some("xyz".into()),
                amount: dec("50"),
            },
        )
        .unwrap();
        send(&sim, &feed, USDC, "20", perp("xyz"), perp("")).unwrap();
        let a = account(&sim);
        assert_eq!(a.perp_balance("xyz"), dec("30"));
        assert_eq!(a.perp_balance(""), dec("20"));
        refusal(send(&sim, &feed, USDC, "5", perp("xyz"), perp("flx")));
        refusal(send(&sim, &feed, USDC, "5", perp("xyz"), perp("xyz")));
        assert_eq!(account(&sim).perp_balance("xyz"), dec("30"));
    }

    /// A dex that is not in the registry, and a non-positive amount, are
    /// refused.
    #[test]
    fn an_unknown_dex_or_a_zero_amount_is_refused() {
        let feed = feed();
        let sim = Sim::memory();
        spot(&sim, Address::repeat_byte(0xa1), USDC, "100");
        let reason = refusal(send(&sim, &feed, USDC, "1", Wallet::Spot, perp("nope")));
        assert!(
            reason.contains("not in the venue's dex registry"),
            "{reason}"
        );
        refusal(send(&sim, &feed, USDC, "0", Wallet::Spot, perp("xyz")));
    }

    /// The source dex keeps the margin its positions hold: with a position on
    /// `xyz` only the dex's withdrawable can leave, and a standard account
    /// never sends more than the wallet's cash.
    #[test]
    fn the_margin_in_use_cannot_be_transferred_out() {
        let feed = feed();
        let sim = Sim::memory();
        let me = Address::repeat_byte(0xa1);
        sim.deal(
            &feed,
            &Deal {
                address: me,
                class: DealClass::Perp,
                coin: None,
                dex: Some("xyz".into()),
                amount: dec("100"),
            },
        )
        .unwrap();
        // 1 xyz:TSLA at 200, 10x: 20 initial margin, notional 200.
        let order = Order {
            coin: "xyz:TSLA".into(),
            is_buy: true,
            size: dec("1"),
            limit_px: None,
            tif: Tif::Ioc,
            reduce_only: false,
            cloid: None,
            trigger: None,
        };
        sim.order(&feed, me, &order).unwrap();

        // The transfer floor is max(initial margin 20, 10% of 200 = 20): about
        // 80 of the ~100 value can move, never the whole balance.
        let bound = account::perp_transfer_bound(&account(&sim), &feed, "xyz").unwrap();
        assert!(bound < dec("81") && bound > dec("79"), "{bound}");
        let reason = refusal(send(&sim, &feed, USDC, "95", perp("xyz"), Wallet::Spot));
        assert!(
            reason.contains("exceeds what perp dex `xyz` may send"),
            "{reason}"
        );
        send(&sim, &feed, USDC, "70", perp("xyz"), Wallet::Spot).unwrap();
        assert_eq!(account(&sim).spot[&0].total, dec("70"));
    }

    /// A unified account's spot wallet keeps the maintenance its positions
    /// hold on the dexes margining against the token.
    #[test]
    fn a_unified_spot_wallet_keeps_its_pooled_maintenance() {
        let feed = feed();
        let sim = Sim::memory();
        let me = Address::repeat_byte(0xa1);
        sim.set_mode(me, UserAbstraction::UnifiedAccount).unwrap();
        spot(&sim, me, USDC, "100");
        // A 1 ETH long at 100 (10x): maintenance 5 on the default dex.
        let order = Order {
            coin: "ETH".into(),
            is_buy: true,
            size: dec("1"),
            limit_px: None,
            tif: Tif::Ioc,
            reduce_only: false,
            cloid: None,
            trigger: None,
        };
        sim.order(&feed, me, &order).unwrap();
        let free = account::spot_bridge_bound(&account(&sim), &feed, 0)
            .unwrap()
            .expect("a unified account has a bound");
        assert!(free < dec("100"), "{free}");
        let reason = refusal(send(&sim, &feed, USDC, "100", Wallet::Spot, perp("xyz")));
        assert!(reason.contains("the spot wallet may send"), "{reason}");
        send(
            &sim,
            &feed,
            USDC,
            &free.to_string(),
            Wallet::Spot,
            perp("xyz"),
        )
        .unwrap();
    }

    /// A transfer to another account credits that account's wallet; an
    /// unknown sender is refused.
    #[test]
    fn a_transfer_can_credit_another_account() {
        let feed = feed();
        let sim = Sim::memory();
        let me = Address::repeat_byte(0xa1);
        let other = Address::repeat_byte(0xb2);
        spot(&sim, me, USDC, "10");
        sim.send_asset(
            &AssetSend {
                from: me,
                to: other,
                token: USDC,
                amount: dec("4"),
                source: Wallet::Spot,
                destination: perp("xyz"),
            },
            &feed,
        )
        .unwrap();
        assert_eq!(
            sim.account(other).unwrap().unwrap().perp_balance("xyz"),
            dec("4")
        );
        let stranger = Address::repeat_byte(0xc3);
        let result = sim.send_asset(
            &AssetSend {
                from: stranger,
                to: me,
                token: USDC,
                amount: dec("1"),
                source: Wallet::Spot,
                destination: Wallet::Spot,
            },
            &feed,
        );
        refusal(result);
    }
}
