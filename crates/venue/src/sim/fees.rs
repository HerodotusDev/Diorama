//! What a transfer costs on the venue, and the ledger rows that record it.
//!
//! Researched rules (the docs, and the real `userNonFundingLedgerUpdates` rows
//! and `preTransferCheck` answers read on 2026-11-17):
//!
//! * **Activation fee.** "New HyperCore accounts require 1 quote token (e.g.,
//!   1 USDC, 1 USDT) of fees for the first transaction which has the new
//!   account as destination address. This applies regardless of the asset
//!   being transfered to the new account." (docs, "Activation gas fee"). On
//!   the real wire the sender pays it on top of the amount: the row carries
//!   `fee: "1.0"`, `feeToken: "USDC"`, the recipient gets the full amount, and
//!   `preTransferCheck` answers `userExists: false, fee: "1.0"` for such a
//!   destination. Circle's CCTP guide adds that a deposit credited to a new
//!   account is credited in full and the fee is "charged on the user's first
//!   outbound action"; the sim models that as an *owed* activation
//!   ([`SimState::activation_owed`]).
//! * **Core to EVM gas.** "A transfer from HyperCore to HyperEVM costs 200k
//!   gas at the base gas price of the next HyperEVM block." The real rows show
//!   it as `nativeTokenFee` (HYPE, `0.00002` = 200k gas at 0.1 gwei) when the
//!   sender holds HYPE, otherwise as `fee` in the token sent, valued at the
//!   HYPE price. HYPE itself is sent with no fee in every row read. The
//!   EVM to Core direction costs EVM gas only (paid on the EVM side).
//! * `withdraw3` costs 1 USDC (docs, "$1 fee"; real `withdraw` rows carry
//!   `fee: "1.0"`); the devnet does not run that bridge, so it stays refused.
//!
//! Where the sources are silent the sim follows its owner's rule: both fees
//! apply on outbound transfers, the activation fee once per destination
//! account (the account then exists).
use super::account::Account;
use super::{SimState, TokenRef};
use crate::feed::MarketFeed;
use alloy_primitives::Address;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// The activation fee: one unit of the quote token.
pub(crate) const ACTIVATION_FEE: Decimal = Decimal::ONE;

/// The quote token the activation fee is charged in (spot token 0, USDC).
pub(crate) const QUOTE_TOKEN: TokenRef<'static> = TokenRef {
    index: 0,
    coin: "USDC",
};

/// HyperEVM gas a Core to EVM transfer costs (docs).
const CORE_TO_EVM_GAS: i128 = 200_000;

/// HyperEVM's base gas price in wei: the 0.1 gwei floor every real
/// `nativeTokenFee: "0.00002"` row implies (200k gas x 0.1 gwei = 0.00002 HYPE).
const BASE_GAS_PRICE_WEI: i128 = 100_000_000;

/// The most ledger rows kept per account (the oldest drop first).
const LEDGER_BOUND: usize = 10_000;

/// The native gas token's name on Core.
pub(crate) const GAS_TOKEN: &str = "HYPE";

/// The HYPE a Core to EVM transfer costs at the base gas price.
pub(crate) fn core_to_evm_gas_hype() -> Decimal {
    Decimal::from_i128_with_scale(CORE_TO_EVM_GAS * BASE_GAS_PRICE_WEI, 18)
}

/// The USD value of one `token`: USDC is 1, a token with a USDC spot pair its
/// mid, any other perp collateral token (a stable) 1; `None` when unpriced.
pub(crate) fn token_usd(feed: &MarketFeed, token: u32) -> Option<Decimal> {
    if token == QUOTE_TOKEN.index {
        return Some(Decimal::ONE);
    }
    let pair = feed
        .spot_meta()?
        .universe
        .into_iter()
        .find(|p| p.tokens == [token, QUOTE_TOKEN.index]);
    if let Some(mid) = pair.and_then(|p| feed.all_mids().0.get(&p.name).copied()) {
        return Some(mid);
    }
    feed.is_collateral_token(token).then_some(Decimal::ONE)
}

/// The venue's decimal text on ledger rows: trimmed, with a `.0` on a whole
/// number (`"1.0"`, `"13.76"`).
pub(crate) fn real_text(value: Decimal) -> String {
    let text = value.round_dp(9).normalize().to_string();
    if text.contains('.') {
        text
    } else {
        format!("{text}.0")
    }
}

/// Whether the account holds or ever held anything: a balance row, a
/// clearinghouse, a position or a fill.
fn holds_anything(account: &Account) -> bool {
    !account.spot.is_empty()
        || !account.perps.is_empty()
        || !account.positions.is_empty()
        || !account.fills.is_empty()
}

impl SimState {
    /// Whether the address exists on the venue: it was activated (received or
    /// held something) or the sim holds a funded account for it.
    pub(crate) fn exists(&self, who: Address) -> bool {
        self.activated.contains(&who) || self.accounts.get(&who).is_some_and(holds_anything)
    }

    /// The activation fee a send from `from` to `to` pays now: one unit when
    /// `to` is another account that does not exist yet, one more when `from`
    /// is a deposited account that still owes its own. A send to oneself
    /// pays none.
    pub(crate) fn activation_due(&self, from: Address, to: Address) -> Decimal {
        if from == to {
            return Decimal::ZERO;
        }
        let new_destination = !self.exists(to);
        let owed = self.activation_owed.contains(&from);
        ACTIVATION_FEE * Decimal::from(u8::from(new_destination) + u8::from(owed))
    }

    /// Book a send's activation: the destination now exists and the sender's
    /// owed fee, when it was part of `due`, is paid.
    pub(crate) fn settle_activation(&mut self, from: Address, to: Address) {
        if from != to {
            self.activation_owed.remove(&from);
        }
        self.activated.insert(to);
        self.activated.insert(from);
    }

    /// Append `delta` to the ledger of every party (once each), stamped `time`.
    pub(crate) fn record_ledger(
        &mut self,
        parties: &[Address],
        time: u64,
        delta: &serde_json::Value,
    ) {
        self.next_ledger_id = self.next_ledger_id.saturating_add(1);
        let row = LedgerUpdate {
            time,
            hash: format!("0x{:064x}", self.next_ledger_id),
            delta: delta.clone(),
        };
        let mut seen: Vec<Address> = Vec::new();
        for party in parties {
            if seen.contains(party) {
                continue;
            }
            seen.push(*party);
            let list = self.transfer_ledger.entry(*party).or_default();
            list.push_back(row.clone());
            while list.len() > LEDGER_BOUND {
                list.pop_front();
            }
        }
    }
}

/// One `userNonFundingLedgerUpdates` row: `{time, hash, delta}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct LedgerUpdate {
    pub time: u64,
    pub hash: String,
    pub delta: serde_json::Value,
}

/// Which action's row a transfer writes: `spotSend` is a `spotTransfer`,
/// `sendAsset` and `usdSend` a `send`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowKind {
    SpotTransfer,
    Send,
}

/// What a transfer paid on top of its amount: the activation or gas in the
/// token's own terms (`fee`, in `fee_token`) and the HYPE gas (`native`).
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Paid {
    pub fee: Decimal,
    pub fee_token: String,
    pub native: Decimal,
}

/// One transfer's facts for its ledger row.
pub(crate) struct TransferRow<'a> {
    pub kind: RowKind,
    pub user: Address,
    pub destination: Address,
    /// `sourceDex` / `destinationDex` (`""` default dex, `"spot"`).
    pub dexes: (&'a str, &'a str),
    pub token: &'a str,
    pub amount: Decimal,
    pub usdc_value: Decimal,
    pub paid: &'a Paid,
    pub nonce: u64,
}

impl TransferRow<'_> {
    /// The row's `delta`, with the real venue's field names.
    pub(crate) fn delta(&self) -> serde_json::Value {
        let mut delta = serde_json::json!({
            "type": match self.kind { RowKind::SpotTransfer => "spotTransfer", RowKind::Send => "send" },
            "token": self.token,
            "amount": real_text(self.amount),
            "usdcValue": real_text(self.usdc_value),
            "user": format!("{:#x}", self.user),
            "destination": format!("{:#x}", self.destination),
            "fee": real_text(self.paid.fee),
            "nativeTokenFee": real_text(self.paid.native),
            "nonce": self.nonce,
            "feeToken": self.paid.fee_token,
        });
        if self.kind == RowKind::Send {
            let (source, destination) = self.dexes;
            delta["sourceDex"] = source.into();
            delta["destinationDex"] = destination.into();
        }
        delta
    }
}

/// The ledger of `user`: rows from `start_ms` (and up to `end_ms`), oldest
/// first. `None` when the sim never held an account for the address.
pub(crate) fn rows_of(
    state: &SimState,
    user: Address,
    (start_ms, end_ms): (u64, Option<u64>),
) -> Option<Vec<LedgerUpdate>> {
    if !state.accounts.contains_key(&user) && !state.transfer_ledger.contains_key(&user) {
        return None;
    }
    let empty = VecDeque::new();
    let list = state.transfer_ledger.get(&user).unwrap_or(&empty);
    Some(
        list.iter()
            .filter(|r| r.time >= start_ms && end_ms.is_none_or(|end| r.time <= end))
            .cloned()
            .collect(),
    )
}

impl super::Sim {
    /// Whether the address exists on the sim's venue (`preTransferCheck`).
    pub(crate) fn address_exists(&self, who: Address) -> crate::error::Result<bool> {
        self.with_state(|state| Ok(state.exists(who)))
    }

    /// The account's transfer rows for `userNonFundingLedgerUpdates`, oldest
    /// first; `None` when the sim never held the address (the caller passes
    /// the read through to the real venue). A poisoned lock also answers
    /// `None`.
    pub(crate) fn ledger_updates(
        &self,
        user: Address,
        window: (u64, Option<u64>),
    ) -> Option<Vec<LedgerUpdate>> {
        let state = self.state.lock().ok()?;
        rows_of(&state, user, window)
    }
}
