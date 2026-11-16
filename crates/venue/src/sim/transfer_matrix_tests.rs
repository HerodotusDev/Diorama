//! The transfer matrix: every direction a balance can move (spot, the default
//! perp dex, builder dexes, another account, the EVM side), with the balances,
//! holds, fees and refusals each one carries. The fee rules are researched in
//! [`super::fees`]: a send to a new account costs the sender 1 USDC once, a
//! Core to EVM transfer costs HyperEVM gas, a deposited account owes its
//! activation on its first outbound transfer.
use super::account::Account;
use super::fees::{RowKind, ACTIVATION_FEE};
use super::order::{Order, Tif};
use super::transfer::{AssetSend, CoreToEvm, Wallet};
use super::{meta_of, meta_with, Applied, Deal, DealClass, Sim, SimState, TokenRef};
use crate::error::Error;
use crate::feed::MarketFeed;
use crate::wire::{L2Book, Level, SpotMeta, SpotPair, SpotToken, UserAbstraction};
use alloy_primitives::Address;
use rust_decimal::Decimal;
use std::collections::BTreeMap;

const USDC: TokenRef<'static> = TokenRef {
    index: 0,
    coin: "USDC",
};
const USDH: TokenRef<'static> = TokenRef {
    index: 360,
    coin: "USDH",
};
const HYPE: TokenRef<'static> = TokenRef {
    index: 150,
    coin: "HYPE",
};
const PURR: TokenRef<'static> = TokenRef {
    index: 1,
    coin: "PURR",
};

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

/// The default dex (`ETH`), two USDC builder dexes (`xyz`, `abc`) and a USDH
/// builder dex (`flx`); the spot registry lists USDC, PURR, HYPE and USDH, and
/// HYPE trades at 5 USDC (pair `@107`).
fn feed() -> MarketFeed {
    let feed = MarketFeed::memory(vec![
        "ETH".to_string(),
        "xyz:TSLA".to_string(),
        "abc:GOLD".to_string(),
        "flx:TSLA".to_string(),
    ]);
    let now = crate::now_ms();
    feed.put_meta(meta_with("ETH", 10), now);
    feed.put_dex_meta("xyz", meta_of(&[("xyz:TSLA", 10)]), now);
    feed.put_dex_meta("abc", meta_of(&[("abc:GOLD", 10)]), now);
    let mut flx = meta_of(&[("flx:TSLA", 10)]);
    flx.collateral_token = 360;
    feed.put_dex_meta("flx", flx, now);
    for coin in ["ETH", "xyz:TSLA", "abc:GOLD", "flx:TSLA"] {
        feed.put_ctx(coin.into(), crate::wire::test_ctx("100", "100"), now);
        feed.put_book(book(coin, "100"), now);
    }
    feed.put_oi_caps(Vec::new());
    for dex in ["xyz", "abc", "flx"] {
        feed.put_oi_caps_of(dex, Vec::new());
    }
    feed.put_spot_meta(SpotMeta {
        tokens: vec![
            token("USDC", 0),
            token("PURR", 1),
            token("HYPE", 150),
            token("USDH", 360),
        ],
        universe: vec![SpotPair {
            tokens: [150, 0],
            name: "@107".into(),
            index: 107,
            is_canonical: true,
        }],
    });
    feed.put_mids(BTreeMap::from([("@107".to_string(), dec("5"))]), now);
    feed
}

const ME: Address = Address::repeat_byte(0xa1);
const YOU: Address = Address::repeat_byte(0xb2);
const NEWCOMER: Address = Address::repeat_byte(0xc3);

fn spot(sim: &Sim, who: Address, token: TokenRef<'_>, amount: &str) {
    sim.credit_spot(who, token, dec(amount)).unwrap();
}

fn deal_perp(sim: &Sim, feed: &MarketFeed, who: Address, dex: Option<&str>, amount: &str) {
    sim.deal(
        feed,
        &Deal {
            address: who,
            class: DealClass::Perp,
            coin: None,
            dex: dex.map(str::to_string),
            amount: dec(amount),
        },
    )
    .unwrap();
}

fn perp(dex: &str) -> Wallet {
    Wallet::Perp(dex.to_string())
}

fn send(
    sim: &Sim,
    feed: &MarketFeed,
    (from, to): (Address, Address),
    (token, amount): (TokenRef<'_>, &str),
    (source, destination): (Wallet, Wallet),
) -> Result<(), Error> {
    sim.send_asset(
        &AssetSend {
            from,
            to,
            token,
            amount: dec(amount),
            source,
            destination,
            kind: RowKind::Send,
        },
        feed,
    )
}

fn refusal(result: Result<(), Error>) -> String {
    match result {
        Err(Error::BadRequest { reason }) => reason,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

fn account(sim: &Sim, who: Address) -> Account {
    sim.account(who).unwrap().unwrap()
}

fn spot_of(sim: &Sim, who: Address, token: TokenRef<'_>) -> Decimal {
    sim.account(who)
        .unwrap()
        .and_then(|a| a.spot.get(&token.index).map(|e| e.total))
        .unwrap_or_default()
}

fn perp_of(sim: &Sim, who: Address, dex: &str) -> Decimal {
    sim.account(who)
        .unwrap()
        .map_or(Decimal::ZERO, |a| a.perp_balance(dex))
}

/// The `delta` objects of an account's transfer ledger, oldest first.
fn deltas(sim: &Sim, who: Address) -> Vec<serde_json::Value> {
    sim.ledger_updates(who, (0, None))
        .unwrap_or_default()
        .into_iter()
        .map(|row| row.delta)
        .collect()
}

fn buy(coin: &str, size: &str) -> Order {
    Order {
        coin: coin.into(),
        is_buy: true,
        size: dec(size),
        limit_px: None,
        tif: Tif::Ioc,
        reduce_only: false,
        cloid: None,
        trigger: None,
    }
}

// ---- spot, the default dex and every builder dex ------------------------------

/// Every wallet pair a USDC balance can move between, in a ring: spot to the
/// default dex to `xyz` to `abc` and back to spot. The total never changes
/// (no fee: it is the account's own money) and each wallet keeps its own ledger.
#[test]
fn usdc_moves_through_every_usdc_wallet_and_the_total_is_conserved() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "100");
    let total = || {
        let a = account(&sim, ME);
        a.spot[&0].total + a.perp_balance("") + a.perp_balance("xyz") + a.perp_balance("abc")
    };
    let hop = |amount: &str, from: Wallet, to: Wallet| {
        send(&sim, &feed, (ME, ME), (USDC, amount), (from, to)).unwrap();
        assert_eq!(total(), dec("100"), "an own transfer never costs anything");
    };
    hop("60", Wallet::Spot, perp(""));
    hop("50", perp(""), perp("xyz"));
    hop("40", perp("xyz"), perp("abc"));
    hop("30", perp("abc"), Wallet::Spot);
    hop("5", perp("xyz"), perp(""));
    hop("5", Wallet::Spot, perp("abc"));
    let a = account(&sim, ME);
    assert_eq!(
        (
            a.spot[&0].total,
            a.perp_balance(""),
            a.perp_balance("xyz"),
            a.perp_balance("abc")
        ),
        (dec("65"), dec("15"), dec("5"), dec("15"))
    );
    assert!(account(&sim, ME).spot.len() == 1, "only USDC moved");
    // None of those moves is a fee row: the ledger holds six fee-free sends.
    let rows = deltas(&sim, ME);
    assert_eq!(rows.len(), 6);
    assert!(rows
        .iter()
        .all(|r| r["fee"] == "0.0" && r["feeToken"] == ""));
    assert_eq!(rows[0]["sourceDex"], "spot");
    assert_eq!(rows[0]["destinationDex"], "");
    assert_eq!(rows[1]["sourceDex"], "");
    assert_eq!(rows[1]["destinationDex"], "xyz");
}

/// USDH lives on the `flx` dex only: spot to `flx` and back work, and every
/// pairing of a USDH transfer with a USDC dex, or a USDC transfer with the USDH
/// dex, is a token mismatch that moves nothing.
#[test]
fn a_usdh_dex_takes_usdh_only_in_both_directions() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "100");
    spot(&sim, ME, USDH, "100");
    send(
        &sim,
        &feed,
        (ME, ME),
        (USDH, "40"),
        (Wallet::Spot, perp("flx")),
    )
    .unwrap();
    send(
        &sim,
        &feed,
        (ME, ME),
        (USDH, "10"),
        (perp("flx"), Wallet::Spot),
    )
    .unwrap();
    assert_eq!(
        (perp_of(&sim, ME, "flx"), spot_of(&sim, ME, USDH)),
        (dec("30"), dec("70"))
    );
    let before = account(&sim, ME);
    for (token, source, destination) in [
        (USDC, Wallet::Spot, perp("flx")),
        (USDC, perp("flx"), Wallet::Spot),
        (USDH, Wallet::Spot, perp("")),
        (USDH, Wallet::Spot, perp("xyz")),
        (USDH, perp("flx"), perp("xyz")),
        (USDC, perp("xyz"), perp("flx")),
    ] {
        let reason = refusal(send(
            &sim,
            &feed,
            (ME, ME),
            (token, "1"),
            (source, destination),
        ));
        assert!(reason.contains("collateral token"), "{reason}");
    }
    assert_eq!(account(&sim, ME), before, "a mismatch moves nothing");
}

/// A perp wallet sends no more than its dex lets it withdraw: with a position
/// open on each of two USDC dexes, each keeps its own margin, and the other's
/// balance never backs it.
#[test]
fn each_dex_keeps_its_own_margin_and_never_the_others() {
    let feed = feed();
    let sim = Sim::memory();
    deal_perp(&sim, &feed, ME, Some("xyz"), "100");
    deal_perp(&sim, &feed, ME, Some("abc"), "100");
    sim.order(&feed, ME, &buy("xyz:TSLA", "1")).unwrap();
    let bound = super::account::perp_transfer_bound(&account(&sim, ME), &feed, "xyz").unwrap();
    assert!(bound < dec("95") && bound > dec("85"), "{bound}");
    // Asking for the whole of xyz is refused although abc holds plenty.
    let reason = refusal(send(
        &sim,
        &feed,
        (ME, ME),
        (USDC, "99"),
        (perp("xyz"), Wallet::Spot),
    ));
    assert!(reason.contains("exceeds what perp dex `xyz`"), "{reason}");
    // abc has no position: its whole balance can leave.
    send(
        &sim,
        &feed,
        (ME, ME),
        (USDC, "100"),
        (perp("abc"), perp("")),
    )
    .unwrap();
    assert_eq!(perp_of(&sim, ME, ""), dec("100"));
}

/// A resting order's hold stays in the spot wallet: only what is free moves.
#[test]
fn what_a_spot_hold_covers_cannot_be_sent() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "100");
    sim.with_account(ME, |a| {
        a.spot.get_mut(&0).unwrap().hold = dec("30");
        Ok(())
    })
    .unwrap();
    let reason = refusal(send(
        &sim,
        &feed,
        (ME, YOU),
        (USDC, "80"),
        (Wallet::Spot, Wallet::Spot),
    ));
    assert!(reason.contains("may send now (70)"), "{reason}");
    // 69 plus the 1 USDC activation fee is exactly the free 70.
    send(
        &sim,
        &feed,
        (ME, YOU),
        (USDC, "69"),
        (Wallet::Spot, Wallet::Spot),
    )
    .unwrap();
    assert_eq!(spot_of(&sim, ME, USDC), dec("30"), "the hold is untouched");
}

/// A unified account's spot USDC is one pool behind the positions of every dex
/// that margins against it: a position on `xyz` and one on the default dex
/// both lower what spot can send. A standard account's spot is free of its
/// perp margin (its perp wallets are separate ledgers).
#[test]
fn a_unified_spot_pool_keeps_the_maintenance_of_every_dex_sharing_the_token() {
    let feed = feed();
    let unified = Sim::memory();
    unified
        .set_mode(ME, UserAbstraction::UnifiedAccount)
        .unwrap();
    spot(&unified, ME, USDC, "100");
    unified.order(&feed, ME, &buy("ETH", "1")).unwrap();
    let one_dex = super::transfer::spot_free(&account(&unified, ME), &feed, 0).unwrap();
    unified.order(&feed, ME, &buy("xyz:TSLA", "1")).unwrap();
    let two_dexes = super::transfer::spot_free(&account(&unified, ME), &feed, 0).unwrap();
    assert!(
        one_dex < dec("100") && two_dexes < one_dex,
        "{one_dex} {two_dexes}"
    );
    let reason = refusal(send(
        &unified,
        &feed,
        (ME, ME),
        (USDC, "100"),
        (Wallet::Spot, perp("abc")),
    ));
    assert!(reason.contains("the spot wallet may send"), "{reason}");

    // Standard: the same 100 in spot is not margin for the perp dex.
    let standard = Sim::memory();
    spot(&standard, ME, USDC, "100");
    deal_perp(&standard, &feed, ME, None, "50");
    standard.order(&feed, ME, &buy("ETH", "1")).unwrap();
    assert_eq!(
        super::transfer::spot_free(&account(&standard, ME), &feed, 0).unwrap(),
        dec("100")
    );
}

/// A zero, a negative and a malformed send, and a sender the sim never held,
/// are refused and move nothing.
#[test]
fn degenerate_sends_are_refused() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "10");
    for amount in ["0", "-1"] {
        refusal(send(
            &sim,
            &feed,
            (ME, YOU),
            (USDC, amount),
            (Wallet::Spot, Wallet::Spot),
        ));
    }
    let reason = refusal(send(
        &sim,
        &feed,
        (NEWCOMER, ME),
        (USDC, "1"),
        (Wallet::Spot, Wallet::Spot),
    ));
    assert!(reason.contains("not a venue-sim account"), "{reason}");
    assert_eq!(spot_of(&sim, ME, USDC), dec("10"));
    assert!(sim.account(YOU).unwrap().is_none());
}

// ---- the activation fee --------------------------------------------------------

/// A spot send to an account that does not exist costs the sender 1 USDC on
/// top: the recipient gets the full amount, both ledgers carry the fee, and the
/// second send to the same account is free.
#[test]
fn the_first_send_to_a_new_account_pays_the_activation_fee_once() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "100");
    assert!(!sim.address_exists(YOU).unwrap());
    send(
        &sim,
        &feed,
        (ME, YOU),
        (USDC, "10"),
        (Wallet::Spot, Wallet::Spot),
    )
    .unwrap();
    assert_eq!(
        (spot_of(&sim, ME, USDC), spot_of(&sim, YOU, USDC)),
        (dec("89"), dec("10"))
    );
    assert!(sim.address_exists(YOU).unwrap());
    send(
        &sim,
        &feed,
        (ME, YOU),
        (USDC, "5"),
        (Wallet::Spot, Wallet::Spot),
    )
    .unwrap();
    assert_eq!(
        (spot_of(&sim, ME, USDC), spot_of(&sim, YOU, USDC)),
        (dec("84"), dec("15")),
        "the second send is free"
    );
    for who in [ME, YOU] {
        let rows = deltas(&sim, who);
        assert_eq!(rows.len(), 2, "both parties see both rows");
        assert_eq!(rows[0]["type"], "send");
        assert_eq!(rows[0]["fee"], "1.0");
        assert_eq!(rows[0]["feeToken"], "USDC");
        assert_eq!(rows[0]["nativeTokenFee"], "0.0");
        assert_eq!(rows[0]["amount"], "10.0");
        assert_eq!(rows[1]["fee"], "0.0");
        assert_eq!(rows[1]["feeToken"], "");
    }
}

/// `spotSend` writes a `spotTransfer` row and `usdSend` (default dex to
/// default dex) a `send` row with empty dexes; both charge a new destination.
#[test]
fn spot_send_and_usd_send_charge_a_new_destination_too() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "100");
    deal_perp(&sim, &feed, ME, None, "50");
    assert_eq!(
        sim.spot_send((ME, YOU), USDC, dec("10"), (RowKind::SpotTransfer, &feed))
            .unwrap(),
        Applied::Applied
    );
    assert_eq!(spot_of(&sim, ME, USDC), dec("89"));
    assert_eq!(deltas(&sim, YOU)[0]["type"], "spotTransfer");
    // usdSend: the default dex of one account to the default dex of another.
    send(
        &sim,
        &feed,
        (ME, NEWCOMER),
        (USDC, "20"),
        (perp(""), perp("")),
    )
    .unwrap();
    assert_eq!(
        (perp_of(&sim, ME, ""), perp_of(&sim, NEWCOMER, "")),
        (dec("29"), dec("20")),
        "the fee came out of the sending dex"
    );
    let row = &deltas(&sim, NEWCOMER)[0];
    assert_eq!(
        (
            row["type"].clone(),
            row["sourceDex"].clone(),
            row["fee"].clone()
        ),
        ("send".into(), "".into(), "1.0".into())
    );
}

/// The fee is 1 USDC whatever is sent: a PURR send to a new account takes the
/// USDC from the spot wallet and delivers the PURR in full.
#[test]
fn a_non_usdc_send_pays_the_activation_fee_from_spot_usdc() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, PURR, "50");
    // No USDC at all: refused by name, nothing moves, nothing is created.
    let reason = refusal(send(
        &sim,
        &feed,
        (ME, YOU),
        (PURR, "5"),
        (Wallet::Spot, Wallet::Spot),
    ));
    assert!(reason.contains("activation fee"), "{reason}");
    assert_eq!(spot_of(&sim, ME, PURR), dec("50"));
    assert!(sim.account(YOU).unwrap().is_none());
    assert!(
        !sim.address_exists(YOU).unwrap(),
        "a refusal activates nothing"
    );
    spot(&sim, ME, USDC, "3");
    send(
        &sim,
        &feed,
        (ME, YOU),
        (PURR, "5"),
        (Wallet::Spot, Wallet::Spot),
    )
    .unwrap();
    assert_eq!(
        (
            spot_of(&sim, ME, PURR),
            spot_of(&sim, ME, USDC),
            spot_of(&sim, YOU, PURR),
            spot_of(&sim, YOU, USDC)
        ),
        (dec("45"), dec("2"), dec("5"), Decimal::ZERO)
    );
    let row = &deltas(&sim, ME)[0];
    assert_eq!(
        (
            row["token"].clone(),
            row["fee"].clone(),
            row["feeToken"].clone()
        ),
        ("PURR".into(), "1.0".into(), "USDC".into())
    );
}

/// The amount and the fee must both fit: 10 USDC to a new account from a
/// balance of exactly 10 is refused (nothing moves), 9 goes through.
#[test]
fn the_amount_plus_the_fee_must_fit_the_balance() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "10");
    let reason = refusal(send(
        &sim,
        &feed,
        (ME, YOU),
        (USDC, "10"),
        (Wallet::Spot, Wallet::Spot),
    ));
    assert!(
        reason.contains("plus the 1 USDC activation fee"),
        "{reason}"
    );
    assert_eq!(spot_of(&sim, ME, USDC), dec("10"));
    assert!(!sim.address_exists(YOU).unwrap());
    // To an account that exists, the whole 10 goes.
    spot(&sim, YOU, USDC, "1");
    send(
        &sim,
        &feed,
        (ME, YOU),
        (USDC, "10"),
        (Wallet::Spot, Wallet::Spot),
    )
    .unwrap();
    assert_eq!(spot_of(&sim, ME, USDC), Decimal::ZERO);
}

/// A send to oneself, between one's own wallets, is never an activation.
#[test]
fn an_own_transfer_never_pays_an_activation() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "10");
    send(
        &sim,
        &feed,
        (ME, ME),
        (USDC, "10"),
        (Wallet::Spot, perp("xyz")),
    )
    .unwrap();
    assert_eq!(perp_of(&sim, ME, "xyz"), dec("10"));
}

/// A send to a builder dex of a new account: the fee is debited from the
/// sender, the recipient's dex ledger gets the whole amount, and the account
/// exists afterwards.
#[test]
fn a_cross_account_send_into_a_builder_dex_activates_the_recipient() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "20");
    send(
        &sim,
        &feed,
        (ME, YOU),
        (USDC, "10"),
        (Wallet::Spot, perp("xyz")),
    )
    .unwrap();
    assert_eq!(
        (spot_of(&sim, ME, USDC), perp_of(&sim, YOU, "xyz")),
        (dec("9"), dec("10"))
    );
    assert_eq!(deltas(&sim, YOU)[0]["destinationDex"], "xyz");
    assert!(sim.address_exists(YOU).unwrap());
}

/// An account that exists only because a saved state carries it is not billed:
/// an older file (no `activated` set) loads with every account existing, and a
/// funded account exists without being in the set.
#[test]
fn every_account_in_a_saved_state_exists() {
    let sim = Sim::memory();
    spot(&sim, YOU, USDC, "1");
    let state = sim.with_state(|s| Ok(s.clone())).unwrap();
    let mut json = serde_json::to_value(&state).unwrap();
    for key in [
        "activated",
        "activation_owed",
        "transfer_ledger",
        "next_ledger_id",
    ] {
        json.as_object_mut().unwrap().remove(key);
    }
    let mut old: SimState = serde_json::from_value(json).unwrap();
    assert!(old.activated.is_empty(), "the field defaults empty");
    old.migrate();
    assert!(old.activated.contains(&YOU));
    assert!(old.exists(YOU));
    assert!(!old.exists(NEWCOMER));
    assert_eq!(old.activation_due(ME, YOU), Decimal::ZERO);
    assert_eq!(old.activation_due(ME, NEWCOMER), ACTIVATION_FEE);
    // The new fields round-trip through the file.
    let again: SimState = serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
    assert_eq!(again, state);
}

// ---- Core to EVM and EVM to Core ------------------------------------------------

fn core_to_evm(sim: &Sim, feed: &MarketFeed, token: TokenRef<'_>, amount: &str) -> Applied {
    let destination = if token.coin == "HYPE" {
        crate::bridge::system::HYPE_SYSTEM
    } else {
        crate::bridge::system::system_address(u64::from(token.index))
    };
    sim.debit_core_to_evm(
        &CoreToEvm {
            from: ME,
            destination,
            token,
            amount: dec(amount),
            kind: RowKind::Send,
        },
        feed,
    )
    .unwrap()
}

/// Core to EVM costs 200k gas at 0.1 gwei = 0.00002 HYPE. A sender with HYPE
/// pays it in HYPE (`nativeTokenFee`), the token arrives whole.
#[test]
fn core_to_evm_gas_is_paid_in_hype_when_the_sender_holds_it() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "100");
    spot(&sim, ME, HYPE, "1");
    assert_eq!(core_to_evm(&sim, &feed, USDC, "100"), Applied::Applied);
    assert_eq!(
        spot_of(&sim, ME, USDC),
        Decimal::ZERO,
        "the whole amount left"
    );
    assert_eq!(spot_of(&sim, ME, HYPE), dec("0.99998"));
    let row = &deltas(&sim, ME)[0];
    assert_eq!(
        row["destination"],
        "0x2000000000000000000000000000000000000000"
    );
    assert_eq!(row["nativeTokenFee"], "0.00002");
    assert_eq!(row["fee"], "0.0");
    assert_eq!(row["feeToken"], "");
}

/// Without HYPE the gas is taken in the token sent, valued at the HYPE price
/// (0.00002 HYPE x 5 = 0.0001 USDC), so the amount plus the fee must fit.
#[test]
fn core_to_evm_gas_without_hype_is_taken_in_the_token() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "100");
    // The whole balance does not cover the amount and the gas.
    assert_eq!(core_to_evm(&sim, &feed, USDC, "100"), Applied::Refused);
    assert_eq!(
        spot_of(&sim, ME, USDC),
        dec("100"),
        "a refusal moves nothing"
    );
    assert!(deltas(&sim, ME).is_empty());
    assert_eq!(core_to_evm(&sim, &feed, USDC, "99"), Applied::Applied);
    assert_eq!(spot_of(&sim, ME, USDC), dec("0.9999"));
    let row = &deltas(&sim, ME)[0];
    assert_eq!(
        (
            row["fee"].clone(),
            row["feeToken"].clone(),
            row["nativeTokenFee"].clone()
        ),
        ("0.0001".into(), "USDC".into(), "0.0".into())
    );
}

/// HYPE itself leaves with no fee (every real HYPE row carries none), to its
/// own system address `0x2222...`.
#[test]
fn hype_leaves_core_with_no_gas_fee() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, HYPE, "2");
    assert_eq!(core_to_evm(&sim, &feed, HYPE, "2"), Applied::Applied);
    assert_eq!(spot_of(&sim, ME, HYPE), Decimal::ZERO);
    let row = &deltas(&sim, ME)[0];
    assert_eq!(
        row["destination"],
        "0x2222222222222222222222222222222222222222"
    );
    assert_eq!(row["fee"], "0.0");
    assert_eq!(row["nativeTokenFee"], "0.0");
}

/// An account a deposit created owes its activation: its first Core to EVM
/// transfer pays 1 USDC more, the second does not, and an account that
/// existed already never owes it.
#[test]
fn a_deposited_account_pays_its_activation_on_its_first_outbound_transfer() {
    let feed = feed();
    let sim = Sim::memory();
    sim.credit_evm_deposit(ME, USDC, dec("10"), &feed).unwrap();
    // The deposit is credited whole, and the system address's row is its record.
    assert_eq!(spot_of(&sim, ME, USDC), dec("10"));
    let row = &deltas(&sim, ME)[0];
    assert_eq!(row["type"], "spotTransfer");
    assert_eq!(row["user"], "0x2000000000000000000000000000000000000000");
    assert_eq!(row["destination"], format!("{ME:#x}"));
    assert_eq!(row["fee"], "0.0");
    // 10 is not enough for 9.5 + 1 + gas, and it moves nothing.
    assert_eq!(core_to_evm(&sim, &feed, USDC, "9.5"), Applied::Refused);
    assert_eq!(spot_of(&sim, ME, USDC), dec("10"));
    assert_eq!(core_to_evm(&sim, &feed, USDC, "5"), Applied::Applied);
    assert_eq!(
        spot_of(&sim, ME, USDC),
        dec("3.9999"),
        "5 + 1 + 0.0001 left"
    );
    let row = &deltas(&sim, ME)[1];
    assert_eq!(row["fee"], "1.0001");
    // The second one owes only gas.
    assert_eq!(core_to_evm(&sim, &feed, USDC, "1"), Applied::Applied);
    assert_eq!(deltas(&sim, ME)[2]["fee"], "0.0001");

    // A deposit into an account that already existed owes nothing.
    let existing = Sim::memory();
    spot(&existing, ME, USDC, "5");
    existing
        .credit_evm_deposit(ME, USDC, dec("5"), &feed)
        .unwrap();
    assert!(existing
        .with_state(|s| Ok(s.activation_owed.is_empty()))
        .unwrap());
}

/// A deposited account that sends to another account pays its owed activation
/// and the new destination's: they are independent charges.
#[test]
fn an_owed_activation_and_a_new_destination_are_two_fees() {
    let feed = feed();
    let sim = Sim::memory();
    sim.credit_evm_deposit(ME, USDC, dec("10"), &feed).unwrap();
    send(
        &sim,
        &feed,
        (ME, YOU),
        (USDC, "5"),
        (Wallet::Spot, Wallet::Spot),
    )
    .unwrap();
    assert_eq!(spot_of(&sim, ME, USDC), dec("3"));
    assert_eq!(deltas(&sim, ME)[1]["fee"], "2.0");
    send(
        &sim,
        &feed,
        (ME, YOU),
        (USDC, "1"),
        (Wallet::Spot, Wallet::Spot),
    )
    .unwrap();
    assert_eq!(
        spot_of(&sim, ME, USDC),
        dec("2"),
        "nothing is owed any more"
    );
}

/// A HYPE deposit is recorded from `0x2222...`; a unified account's Core to EVM
/// transfer is bounded by what its positions leave free.
#[test]
fn hype_deposits_come_from_the_hype_system_address_and_unified_bounds_apply() {
    let feed = feed();
    let sim = Sim::memory();
    sim.credit_evm_deposit(YOU, HYPE, dec("3"), &feed).unwrap();
    assert_eq!(
        deltas(&sim, YOU)[0]["user"],
        "0x2222222222222222222222222222222222222222"
    );
    sim.set_mode(ME, UserAbstraction::UnifiedAccount).unwrap();
    spot(&sim, ME, USDC, "100");
    spot(&sim, ME, HYPE, "1");
    sim.order(&feed, ME, &buy("ETH", "1")).unwrap();
    let free = super::transfer::spot_free(&account(&sim, ME), &feed, 0).unwrap();
    assert!(free < dec("100"), "{free}");
    assert_eq!(core_to_evm(&sim, &feed, USDC, "100"), Applied::Refused);
    assert_eq!(
        core_to_evm(&sim, &feed, USDC, &free.floor().to_string()),
        Applied::Applied
    );
}

// ---- the ledger ----------------------------------------------------------------

/// A usdClassTransfer writes an `accountClassTransfer` row, and the window and
/// end bound of `userNonFundingLedgerUpdates` apply; an address the sim never
/// held is `None` (the API passes the read through).
#[test]
fn the_ledger_serves_class_transfers_in_the_requested_window() {
    let feed = feed();
    let sim = Sim::memory();
    spot(&sim, ME, USDC, "10");
    sim.usd_class_transfer(
        ME,
        &super::ClassTransfer {
            token: USDC,
            amount: dec("4"),
            direction: super::Direction::ToPerp,
        },
        &feed,
    )
    .unwrap();
    let rows = sim.ledger_updates(ME, (0, None)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].delta["type"], "accountClassTransfer");
    assert_eq!(rows[0].delta["usdc"], "4.0");
    assert_eq!(rows[0].delta["toPerp"], true);
    let time = rows[0].time;
    assert_eq!(sim.ledger_updates(ME, (time + 1, None)).unwrap().len(), 0);
    assert_eq!(
        sim.ledger_updates(ME, (0, Some(time - 1))).unwrap().len(),
        0
    );
    assert!(sim.ledger_updates(NEWCOMER, (0, None)).is_none());
}
