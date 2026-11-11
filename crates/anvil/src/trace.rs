//! prestateTracer diff flattening (`debug_traceBlockByNumber(..., diffMode)`) —
//! the fallback when a BAL is unavailable. The wire shapes are alloy's
//! (`alloy_rpc_types_trace::geth`); the flattening is one small function per
//! account field, so a tracer shape change lands as a compile error, never as
//! a silently skipped write.
//!
//! Measured on anvil 1.8.6 (the mirror's pin): the frame lists only *changed*
//! accounts in `post` (created accounts absent from `pre`, selfdestructed
//! absent from `post`), `balance`/`code`/`storage` as hex strings, `nonce` as
//! a JSON number, and storage keys/values as **full-width** 32-byte hex —
//! exactly what alloy's `AccountState` models, so the parse is the official
//! type's, not ours.
use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rpc_types_trace::geth::{AccountState, GethTrace, PreStateFrame, TraceResult};
use std::collections::{BTreeMap, HashMap};

/// One state write a trace diff reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Write {
    Storage {
        address: Address,
        slot: U256,
        value: U256,
    },
    Balance {
        address: Address,
        value: U256,
    },
    Nonce {
        address: Address,
        value: u64,
    },
    Code {
        address: Address,
        value: Bytes,
    },
}

impl Write {
    pub fn address(&self) -> Address {
        match self {
            Write::Storage { address, .. }
            | Write::Balance { address, .. }
            | Write::Nonce { address, .. }
            | Write::Code { address, .. } => *address,
        }
    }

    /// The write's key half: what it writes, without the value. This is the
    /// vocabulary of the watch set, the shadow registry, the journal and the
    /// venue's declared closures — one key space for both mirrors.
    pub fn key(&self) -> Key {
        match self {
            Write::Storage { address, slot, .. } => Key::Storage {
                address: *address,
                slot: *slot,
            },
            Write::Balance { address, .. } => Key::Balance(*address),
            Write::Nonce { address, .. } => Key::Nonce(*address),
            Write::Code { address, .. } => Key::Code(*address),
        }
    }

    /// The storage key of a storage write (`None` for account-field writes:
    /// only storage carries a slot — the parity lens samples exactly these).
    pub fn storage_key(&self) -> Option<Key> {
        match self {
            Write::Storage { address, slot, .. } => Some(Key::Storage {
                address: *address,
                slot: *slot,
            }),
            _ => None,
        }
    }
}

/// A state key: `Write` without its value. `Key` is what a read asks for
/// (the read plan asks keys) and what a mark names (the shadow
/// registry's marks are keyed by it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Key {
    Storage { address: Address, slot: U256 },
    Balance(Address),
    Nonce(Address),
    Code(Address),
}

impl Key {
    pub fn address(&self) -> Address {
        match self {
            Key::Storage { address, .. }
            | Key::Balance(address)
            | Key::Nonce(address)
            | Key::Code(address) => *address,
        }
    }
}

/// A state key's value, as a read returns it. `Write` is key + value; `Value`
/// is what the read seam answers, so the composer can read a
/// plan of keys without matching on four separate methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Storage(U256),
    Balance(U256),
    Nonce(u64),
    Code(Bytes),
}

impl Write {
    /// Pair a key with its value. `None` when they do not match (a read plan
    /// bug): the caller keeps the key and treats the value as unusable, never
    /// writes a mistyped word.
    pub fn new(key: Key, value: Value) -> Option<Write> {
        match (key, value) {
            (Key::Storage { address, slot }, Value::Storage(value)) => Some(Write::Storage {
                address,
                slot,
                value,
            }),
            (Key::Balance(address), Value::Balance(value)) => {
                Some(Write::Balance { address, value })
            }
            (Key::Nonce(address), Value::Nonce(value)) => Some(Write::Nonce { address, value }),
            (Key::Code(address), Value::Code(value)) => Some(Write::Code { address, value }),
            _ => None,
        }
    }
}

/// Flatten one transaction's `prestateTracer{diffMode}` frame into writes:
/// the `pre` side lists the *changed* fields (measured: a read-only or no-op
/// touch appears nowhere), the `post` side omits fields that changed to zero
/// (measured on anvil 1.8.6: a drained storage slot is present in `pre` with
/// its old value and absent from `post`). A slot in `pre` but not in `post`
/// is therefore a write to zero — without this the shadow registry records a
/// stale non-zero value, and a same-pin restore resurrects it (measured: a
/// drained balance came back after a reset).
pub(crate) fn flatten_trace_diff(frame: &PreStateFrame) -> Vec<Write> {
    flatten_trace_diff_with_pre(frame).0
}

/// The diff flattening plus the trace's own `pre` side: for every changed
/// key, the value it held at the block's start (the first transaction's
/// `pre` wins for a key several transactions touch — later pres are
/// mid-block states, never the block's before-value). A key whose `pre`
/// side is absent reads as zero (measured: a 0→v write lives in `post`
/// alone). This is the journal's before-value document — the applier
/// journals it without a single read.
pub(crate) fn flatten_trace_diff_with_pre(
    frame: &PreStateFrame,
) -> (Vec<Write>, Vec<(Key, Value)>) {
    let Some(diff) = frame.as_diff() else {
        return (Vec::new(), Vec::new());
    };
    let mut out = Vec::new();
    let mut pres = Vec::new();
    // The union of both sides: an account can appear in `post` only (anvil
    // omits an unchanged-zero pre-state's storage, so a 0→v write lives in
    // `post.storage` alone) and in `pre` only (a v→0 write, which `post`
    // omits — both measured).
    let mut addresses: std::collections::BTreeSet<Address> = diff.pre.keys().copied().collect();
    addresses.extend(diff.post.keys().copied());
    for address in addresses {
        let pre = diff.pre.get(&address);
        let post = diff.post.get(&address);
        push_storage(&mut out, &mut pres, address, pre, post);
        if let Some(post) = post {
            push_balance(&mut out, &mut pres, address, pre, post);
            push_nonce(&mut out, &mut pres, address, pre, post);
            push_code(&mut out, &mut pres, address, pre, post);
        }
    }
    (out, pres)
}

fn push_balance(
    out: &mut Vec<Write>,
    pres: &mut Vec<(Key, Value)>,
    address: Address,
    pre: Option<&AccountState>,
    post: &AccountState,
) {
    if let Some(value) = post.balance {
        if pre.and_then(|p| p.balance) != Some(value) {
            out.push(Write::Balance { address, value });
            let before = pre.and_then(|p| p.balance).unwrap_or(U256::ZERO);
            pres.push((Key::Balance(address), Value::Balance(before)));
        }
    }
}

fn push_nonce(
    out: &mut Vec<Write>,
    pres: &mut Vec<(Key, Value)>,
    address: Address,
    pre: Option<&AccountState>,
    post: &AccountState,
) {
    if let Some(value) = post.nonce {
        if pre.and_then(|p| p.nonce) != Some(value) {
            out.push(Write::Nonce { address, value });
            let before = pre.and_then(|p| p.nonce).unwrap_or(0);
            pres.push((Key::Nonce(address), Value::Nonce(before)));
        }
    }
}

fn push_code(
    out: &mut Vec<Write>,
    pres: &mut Vec<(Key, Value)>,
    address: Address,
    pre: Option<&AccountState>,
    post: &AccountState,
) {
    if let Some(value) = &post.code {
        if pre.and_then(|p| p.code.as_ref()) != Some(value) {
            out.push(Write::Code {
                address,
                value: value.clone(),
            });
            let before = pre
                .and_then(|p| p.code.as_ref())
                .cloned()
                .unwrap_or_else(Bytes::new);
            pres.push((Key::Code(address), Value::Code(before)));
        }
    }
}

/// Storage writes: the union of the `pre` and `post` storage maps. Measured:
/// reth's `pre.storage` lists the changed slots and `post` omits slots that
/// changed to zero; anvil's `post.storage` lists them and `pre` omits slots
/// whose pre-value was zero. Either side alone misses a 0→v or v→0 write, so
/// a slot present on one side only reads the missing side as zero.
fn push_storage(
    out: &mut Vec<Write>,
    pres: &mut Vec<(Key, Value)>,
    address: Address,
    pre: Option<&AccountState>,
    post: Option<&AccountState>,
) {
    let empty = BTreeMap::new();
    let pre_storage = pre.map(|p| &p.storage).unwrap_or(&empty);
    let post_storage = post.map(|p| &p.storage).unwrap_or(&empty);
    let mut slots: std::collections::BTreeSet<B256> = pre_storage.keys().copied().collect();
    slots.extend(post_storage.keys().copied());
    for slot in slots {
        let pre_value = pre_storage
            .get(&slot)
            .map(|v| U256::from_be_bytes(v.0))
            .unwrap_or(U256::ZERO);
        let post_value = post_storage
            .get(&slot)
            .map(|v| U256::from_be_bytes(v.0))
            .unwrap_or(U256::ZERO);
        if pre_value != post_value {
            let slot = U256::from_be_bytes(slot.0);
            out.push(Write::Storage {
                address,
                slot,
                value: post_value,
            });
            pres.push((Key::Storage { address, slot }, Value::Storage(pre_value)));
        }
    }
}

/// Flatten one transaction's trace answer: the prestate frame's writes, or
/// none when the tracer answered with another frame (we always ask for the
/// prestate tracer, so another frame means nothing to mark).
pub fn flatten_trace(trace: &GethTrace) -> Vec<Write> {
    match trace {
        GethTrace::PreStateTracer(frame) => flatten_trace_diff(frame),
        _ => Vec::new(),
    }
}

/// [`Self::flatten_trace`] plus the frame's `pre` side (the same first-wins
/// rule at single-transaction scope).
pub fn flatten_trace_with_pre(trace: &GethTrace) -> (Vec<Write>, Vec<(Key, Value)>) {
    match trace {
        GethTrace::PreStateTracer(frame) => flatten_trace_diff_with_pre(frame),
        _ => (Vec::new(), Vec::new()),
    }
}

/// Flatten a `debug_traceBlockByNumber` answer (`[{txHash, result}]`):
/// the prestate frames of every successfully traced transaction, in order.
/// A transaction the tracer could not process contributes no writes — the
/// caller's next parent BAL is the backstop for a partial flattening.
pub fn flatten_trace_block(traces: &[TraceResult]) -> Vec<Write> {
    traces
        .iter()
        .filter_map(TraceResult::success)
        .flat_map(flatten_trace)
        .collect()
}

/// [`Self::flatten_trace_block`] plus the block's before-values: per key,
/// the *first* touching transaction's `pre` (in tx order) — the block's own
/// starting state, never a mid-block value.
pub fn flatten_trace_block_with_pre(
    traces: &[TraceResult],
) -> (Vec<Write>, HashMap<Key, Value>) {
    let mut writes = Vec::new();
    let mut pres: HashMap<Key, Value> = HashMap::new();
    for trace in traces.iter().filter_map(TraceResult::success) {
        let (tx_writes, tx_pres) = flatten_trace_with_pre(trace);
        writes.extend(tx_writes);
        for (key, value) in tx_pres {
            pres.entry(key).or_insert(value);
        }
    }
    (writes, pres)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A genuine excerpt of anvil 1.8.6 `prestateTracer` diffMode output
    /// (measured 2026-10-22 on the dry-run fork): storage keys/values are
    /// full-width 32-byte hex; `nonce` is a JSON number; `post` carries only
    /// the fields that changed, `pre` the touched account's prior state.
    const MEASURED_FRAME: &str = r#"{
      "post": {
        "0x5875db54cd9ae2b2a875e09bb731772297ae9d92": {"balance": "0xbdc602038b410e5", "nonce": 172449},
        "0x0000000aa232009084bd71a5797d089aa4edfad4": {
          "storage": {
            "0x0000000000000000000000000000000000000000000000000000000000000003": "0x000000004be6a279be925c8183c6ab0b5b60554bc7adbacf0000000001908b7a",
            "0xc00849b9357c3b9274369e6c5b3b60d0742a7ddad7c3c48b10e9b9109914640a": "0x0000000000000000000000000000000000000000000000000000001cfc5bd1cb"
          }
        }
      },
      "pre": {
        "0x5875db54cd9ae2b2a875e09bb731772297ae9d92": {"balance": "0xbdc860812f71543", "nonce": 172448},
        "0x0000000aa232009084bd71a5797d089aa4edfad4": {
          "balance": "0x0",
          "nonce": 8,
          "storage": {
            "0x0000000000000000000000000000000000000000000000000000000000000003": "0x000000004be6a279be925c8183c6ab0b5b60554bc7adbacf0000000001908b79",
            "0xc00849b9357c3b9274369e6c5b3b60d0742a7ddad7c3c48b10e9b9109914640a": "0x0000000000000000000000000000000000000000000000000000001cf99154b7"
          }
        }
      }
    }"#;

    #[test]
    fn trace_diff_flattening() {
        let frame: PreStateFrame = serde_json::from_str(MEASURED_FRAME).expect("measured frame");
        let writes = flatten_trace_diff(&frame);
        assert_eq!(
            writes,
            vec![
                Write::Storage {
                    address: "0x0000000aa232009084bd71a5797d089aa4edfad4"
                        .parse()
                        .unwrap(),
                    slot: U256::from(3u64),
                    value: U256::from_str_radix(
                        "4be6a279be925c8183c6ab0b5b60554bc7adbacf0000000001908b7a",
                        16
                    )
                    .unwrap(),
                },
                Write::Storage {
                    address: "0x0000000aa232009084bd71a5797d089aa4edfad4"
                        .parse()
                        .unwrap(),
                    slot: U256::from_str_radix(
                        "c00849b9357c3b9274369e6c5b3b60d0742a7ddad7c3c48b10e9b9109914640a",
                        16
                    )
                    .unwrap(),
                    value: U256::from_str_radix("1cfc5bd1cb", 16).unwrap(),
                },
                Write::Balance {
                    address: "0x5875db54cd9ae2b2a875e09bb731772297ae9d92"
                        .parse()
                        .unwrap(),
                    value: U256::from(0x0bdc_6020_38b4_10e5_u64),
                },
                Write::Nonce {
                    address: "0x5875db54cd9ae2b2a875e09bb731772297ae9d92"
                        .parse()
                        .unwrap(),
                    value: 172_449,
                },
            ]
        );
    }

    #[test]
    fn unchanged_fields_are_not_writes() {
        // The same value in `pre` and `post` is not a write (the scanner must
        // not mark untouched state as devnet-owned).
        let frame: PreStateFrame = serde_json::from_str(
            r#"{"pre":{"0x0000000000000000000000000000000000000001":{"balance":"0x1"}},
                "post":{"0x0000000000000000000000000000000000000001":{"balance":"0x1"}}}"#,
        )
        .expect("frame");
        assert!(flatten_trace_diff(&frame).is_empty());
    }

    /// A 0→v storage write whose pre-state side omits the slot (measured
    /// 2026-10-24 on anvil: `pre` carries only balance/code for an account
    /// whose slot was zero; the change lives in `post.storage` alone). The
    /// union rule must record it — the pre-driven rule silently missed it.
    #[test]
    fn a_post_only_storage_change_is_a_write() {
        let frame: PreStateFrame = serde_json::from_str(
            r#"{"post":{"0x000000000000000000000000000000000000c0de":{
                    "storage":{"0x0000000000000000000000000000000000000000000000000000000000000000":"0x0000000000000000000000000000000000000000000000000000000000000001"}}},
                "pre":{"0x000000000000000000000000000000000000c0de":{"balance":"0x0","code":"0x60016000556000"}}}"#,
        )
        .expect("frame");
        let writes = flatten_trace_diff(&frame);
        assert_eq!(writes.len(), 1, "{writes:?}");
        assert!(matches!(
            &writes[0],
            Write::Storage { slot, value, .. } if *slot == U256::ZERO && *value == U256::from(1)
        ));
    }

    /// A genuine excerpt of a zeroing transfer (measured 2026-10-22 on the
    /// scratch V): the drained slot sits in `pre` with its old value and is
    /// ABSENT from `post.storage` — anvil omits zeroed slots. Missing this
    /// made the shadow registry keep the stale non-zero value, and a same-pin
    /// restore resurrected a drained balance.
    #[test]
    fn a_slot_changed_to_zero_is_a_write() {
        let frame: PreStateFrame = serde_json::from_str(
            r#"{
              "pre": {
                "0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e": {
                  "storage": {
                    "0x986d56fe2954bacde776e5a83b13aa605cf4db18d945bbea664eb3dc8135d4c1": "0x0000000000000000000000000000000000000000000000000000000000000064",
                    "0x4c9e99a9b0f9f4f7d2b6cf12a3b7e6c1a1e5b7d9e3f2a4c6b8d0e2f4a6c8e0f2": "0x0000000000000000000000000000000000000000000000000000000000000009"
                  }
                }
              },
              "post": {
                "0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e": {
                  "storage": {
                    "0x4c9e99a9b0f9f4f7d2b6cf12a3b7e6c1a1e5b7d9e3f2a4c6b8d0e2f4a6c8e0f2": "0x000000000000000000000000000000000000000000000000000000000000006d"
                  }
                }
              }
            }"#,
        )
        .expect("measured frame");
        let writes = flatten_trace_diff(&frame);
        assert_eq!(writes.len(), 2, "{writes:?}");
        // BTreeMap order: the changed slot (4c9e…) precedes the zeroed one.
        // The changed slot keeps its post value.
        assert_eq!(
            writes[0],
            Write::Storage {
                address: "0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e"
                    .parse()
                    .unwrap(),
                slot: U256::from_str_radix(
                    "4c9e99a9b0f9f4f7d2b6cf12a3b7e6c1a1e5b7d9e3f2a4c6b8d0e2f4a6c8e0f2",
                    16
                )
                .unwrap(),
                value: U256::from(0x6d_u64),
            }
        );
        // The zeroed slot (pre-present, post-absent) is recorded as ZERO.
        assert_eq!(
            writes[1],
            Write::Storage {
                address: "0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e"
                    .parse()
                    .unwrap(),
                slot: U256::from_str_radix(
                    "986d56fe2954bacde776e5a83b13aa605cf4db18d945bbea664eb3dc8135d4c1",
                    16
                )
                .unwrap(),
                value: U256::ZERO,
            }
        );
    }

    /// The `pre` side is the journal's before-value document: every changed
    /// key carries the value it held at the block's start. A `post`-only key
    /// (measured: a 0→v write) reads as zero; a field that did not change
    /// carries no pre at all.
    #[test]
    fn the_pre_side_is_the_before_value_document() {
        let frame: PreStateFrame = serde_json::from_str(MEASURED_FRAME).expect("measured frame");
        let (writes, pres) = flatten_trace_diff_with_pre(&frame);
        let pres: HashMap<Key, Value> = pres.into_iter().collect();
        let before = |w: &Write| pres.get(&w.key()).expect("every write has a pre");
        // Storage: pre slot 3 = ...8b79 (post ...8b7a); pre c008... = ...54b7.
        assert_eq!(
            before(&writes[0]),
            &Value::Storage(U256::from_str_radix(
                "4be6a279be925c8183c6ab0b5b60554bc7adbacf0000000001908b79",
                16
            )
            .unwrap())
        );
        assert_eq!(
            before(&writes[1]),
            &Value::Storage(U256::from_str_radix("1cf99154b7", 16).unwrap())
        );
        // Balance: pre 0xbdc860812f71543; nonce: pre 172448.
        assert_eq!(
            before(&writes[2]),
            &Value::Balance(U256::from(0x0bdc_8608_12f7_1543_u64))
        );
        assert_eq!(before(&writes[3]), &Value::Nonce(172_448));
    }

    /// A `post`-only write's before-value is zero: the account had no
    /// non-zero pre-state for it (measured shape — the slot was absent from
    /// `pre` because it was zero).
    #[test]
    fn a_post_only_write_reads_a_zero_before() {
        let frame: PreStateFrame = serde_json::from_str(
            r#"{"post":{"0x000000000000000000000000000000000000c0de":{
                    "storage":{"0x0000000000000000000000000000000000000000000000000000000000000000":"0x0000000000000000000000000000000000000000000000000000000000000001"}}},
                "pre":{"0x000000000000000000000000000000000000c0de":{"balance":"0x0","code":"0x60016000556000"}}}"#,
        )
        .expect("frame");
        let (writes, pres) = flatten_trace_diff_with_pre(&frame);
        let pres: HashMap<Key, Value> = pres.into_iter().collect();
        assert_eq!(writes.len(), 1);
        assert_eq!(pres.get(&writes[0].key()), Some(&Value::Storage(U256::ZERO)));
    }

    /// A key two transactions touch carries the FIRST transaction's `pre` —
    /// the block's starting state, never a mid-block value.
    #[test]
    fn block_pres_keep_the_first_touching_txs_pre() {
        let frame = |pre: &str, post: &str| {
            serde_json::from_str::<PreStateFrame>(&format!(
                r#"{{"pre":{{"0x00000000000000000000000000000000000000a1":{{"balance":"{pre}"}}}},
                    "post":{{"0x00000000000000000000000000000000000000a1":{{"balance":"{post}"}}}}}}"#
            ))
            .expect("frame")
        };
        let traces = vec![
            TraceResult::Success {
                result: GethTrace::PreStateTracer(
                    frame("0x64", "0xc8"), // tx1: 100 -> 200 (pre = 100)
                ),
                tx_hash: None,
            },
            TraceResult::Success {
                result: GethTrace::PreStateTracer(
                    frame("0xc8", "0x12c"), // tx2: 200 -> 300 (pre = 200, mid-block)
                ),
                tx_hash: None,
            },
        ];
        let (writes, pres) = flatten_trace_block_with_pre(&traces);
        // Both writes ride (the applier lands both, in order).
        assert_eq!(writes.len(), 2);
        let a1: Address = "0x00000000000000000000000000000000000000a1".parse().unwrap();
        assert_eq!(pres.get(&Key::Balance(a1)), Some(&Value::Balance(U256::from(100))));
    }
}
